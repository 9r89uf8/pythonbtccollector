"""One explicit PostgreSQL smoke check; never targets the production database.

Apply schema.sql first to a disposable database named settlement_validation_*.
Run this script as the local postgres OS user, with DATABASE_NAME as its sole
argument and this checkout on PYTHONPATH. Uses existing asyncpg, not pytest.
The caller owns scratch database creation and removal. No role changes or feeds.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, localcontext
import json
import sys

import asyncpg

from price_collector.settlement_store import SettlementStore, DAY_MS


def decision(start, identity, offset, prices, *, observation_window_s=30):
    end = start + 300_000
    wall = (end - 29_000 + offset) * 1_000_000
    with localcontext() as arithmetic:
        arithmetic.prec = 80
        signals = {}
        for name, value in zip(('ghost', 'twap', 'spot'), prices):
            price = Decimal(value)
            lead = (price - Decimal(100)) * 100
            signals[name] = dict(price=value, side='up' if lead > 0 else 'down' if lead < 0 else 'tie',
                                 lead_bps=str(lead), qualifies=abs(lead) >= 2)
    frozen = dict(run_id='postgres-smoke', decision_id=identity, schema_version=1, kind='settlement',
        rule_version='settlement-first-2bp-v1', threshold_bps='2', evaluation_start_ms=start,
        market_id=start // 300_000, market_start_ms=start, market_end_ms=end,
        target_source_timestamp_ms=end, decision_wall_ns=str(wall), decision_monotonic_ns=str(1_000_000 + offset * 1_000_000),
        valid_until_wall_ns=str(wall + 2_000_000_000), status='available', quality='healthy',
        reference=dict(price_to_beat='100', condition_id='c', up_token_id='u', down_token_id='d'),
        reasons=[], signals=signals, slots=[], slot_inputs=[])
    if observation_window_s == 60:
        frozen.update(schema_version=3, rule_version='historical-settlement-v2',
                      observation_window_s=60, sampling_interval_ms=2000)
        frozen.pop('threshold_bps')
    state = dict(publication=dict(status='acknowledged', attempt_wall_ns=str(wall + 1),
        ack_wall_ns=str(wall + 2), eligible_before_close=True), target=dict(status='pending', first_event=None))
    wire = {key: value for key, value in frozen.items() if key not in ('slots', 'slot_inputs')}
    wire.update(publication_state='attempted', publication_attempt_wall_ns=str(wall + 1))
    state['publication']['attempted_payload'] = json.dumps(wire)
    return dict(run_id=frozen['run_id'], decision_id=identity, decision_wall_ns=wall,
                created_ms=wall // 1_000_000, frozen_json=json.dumps(frozen), state_json=json.dumps(state),
                version=1, terminal=False)


async def rejected(connection, sql, *parameters, contains):
    try:
        await connection.execute(sql, *parameters)
    except asyncpg.PostgresError as error:
        assert contains in str(error), str(error)
    else:
        raise AssertionError('PostgreSQL accepted a forbidden mutation')


async def main(name):
    if not name.startswith('settlement_validation_') or not name.replace('_', '').isalnum():
        raise ValueError('requires explicitly provisioned settlement_validation_* database')
    connection = await asyncpg.connect(database=name, user='postgres', host='/var/run/postgresql', command_timeout=5)
    pool = None
    try:
        assert await connection.fetchval('SELECT current_database()') == name
        assert await connection.fetchval('SELECT count(*) FROM settlement_audit') == 0, 'scratch database must be empty'
        now = await connection.fetchval('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint')
        start = now // DAY_MS * DAY_MS - 3 * DAY_MS
        async def writer(client):
            await client.execute('SET ROLE price_writer')
        pool = await asyncpg.create_pool(database=name, user='postgres', host='/var/run/postgresql',
                                        min_size=1, max_size=1, command_timeout=5, setup=writer)
        store = SettlementStore(pool)
        a = decision(start, '1', 0, ('100.03', '100.01', '99.97'))
        b = decision(start, '2', 500, ('99.97', '100.03', '99.97'))
        assert await store.persist(a) == 'inserted'
        assert await store.persist(a) == 'unchanged'
        assert await store.persist(b) == 'inserted'
        history = await store.maintain(now)
        body = json.loads(await connection.fetchval('SELECT body_json FROM settlement_history_markets'))
        assert body['buckets']['25-30']['order'][2] == '1'
        assert body['buckets']['25-30']['signals']['ghost']['price'] == '100.03'
        assert body['buckets']['25-30']['signals']['twap']['price'] == '100.01'
        assert await connection.fetchval('SELECT count(*) FROM settlement_market_evaluation') == 0
        assert await connection.fetchval('SELECT count(*) FROM settlement_evaluation_reports') == 0
        assert await connection.fetchval('SELECT bool_and(history_folded_version=version) FROM settlement_audit')

        await connection.execute('SET ROLE price_writer')
        await rejected(connection, "UPDATE settlement_audit SET run_id='changed' WHERE decision_id='1'",
                       contains='immutable')
        changed = json.loads(a['state_json'])
        changed['publication']['ack_wall_ns'] = str(int(changed['publication']['ack_wall_ns']) + 1)
        await rejected(connection, "UPDATE settlement_audit SET state_json=$1,version=2 WHERE decision_id='1'",
                       json.dumps(changed), contains='clocks are immutable')
        await rejected(connection, "DELETE FROM settlement_audit WHERE decision_id='1'", contains='seven days')
        await rejected(connection, "DELETE FROM settlement_history_markets", contains='seven days')
        await rejected(connection, "UPDATE settlement_history_daily SET body_json='{}'", contains='immutable')
        await rejected(connection, "DELETE FROM settlement_history_daily", contains='ninety days')
        await connection.execute('RESET ROLE')

        for record in (a, b):
            record.update(version=2, terminal=True)
            assert await store.persist(record) == 'updated'
        repeated = await store.maintain(now)
        assert history['cells'] == repeated['cells'], 'retries never count a second market'
        assert history['status'] == 'available'
        assert sum(cell['unknown'] for cell in history['cells']) == 3
        assert sum(cell['frozen_unknown'] for cell in history['cells']) == 3
        assert await connection.fetchval('SELECT count(*) FROM settlement_audit WHERE frozen_json IS NULL') == 2
        assert await store.persist(a) == 'unchanged', 'compaction must not break idempotent recovery'
        await connection.execute('SET ROLE price_reader')
        assert await connection.fetchval('SELECT count(*) FROM settlement_audit') == 2
        await rejected(connection, "UPDATE settlement_audit SET terminal=true", contains='permission denied')
        await connection.execute('RESET ROLE')
        guard = await store.guard()
        assert guard['capacity_ok'] and guard['row_count'] == 2
        minute = decision(start, 'minute', -31_000, ('100.03', '100.01', '99.97'), observation_window_s=60)
        assert await store.persist(minute) == 'inserted', 'new contract must admit the final-60s boundary'
        assert await store.persist(minute) == 'unchanged'
        mixed = await store.maintain(now)
        by_selection = {cohort['selection_version']: cohort['id'] for cohort in mixed['cohorts']}
        assert set(by_selection) == {'first-ack-5s-v1', 'first-ack-5s-v2'}
        legacy_coverage = [row for row in mixed['coverage'] if row['cohort'] == by_selection['first-ack-5s-v1']]
        minute_coverage = [row for row in mixed['coverage'] if row['cohort'] == by_selection['first-ack-5s-v2']]
        assert len(legacy_coverage) == 6 and len(minute_coverage) == 12
        assert next(row for row in minute_coverage if row['time_bucket'] == '55-60')['selected_markets'] == 1
        assert await connection.fetchval("""SELECT count(*) FROM pg_constraint
            WHERE conrelid='settlement_audit'::regclass
            AND conname='settlement_audit_observation_window_check'""") == 1
        print(json.dumps(dict(database=name, server=await connection.fetchval('SHOW server_version'),
            result='passed', checks=['writer persistence and idempotency', 'first ACK per time bucket',
                'immutable audit and publication clocks', 'seven-day individual deletion guard',
                'daily freeze and ninety-day aggregate guard', 'terminal compaction',
                'bounded fold and outcome join SQL', 'reader/writer privilege split', 'capacity query',
                'versioned sixty-second admission', 'separate legacy and minute history buckets'],
                relation_bytes=guard['relation_bytes'])))
    finally:
        if pool is not None:
            await pool.close()
        await connection.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: settlement_postgres_smoke.py settlement_validation_DATABASE')
    asyncio.run(main(sys.argv[1]))
