"""One explicit PostgreSQL smoke check; never targets the production database.

Apply schema.sql first to a disposable database named settlement_validation_*.
Run this script as the local postgres OS user, with DATABASE_NAME as its sole
argument and this checkout on PYTHONPATH. Uses existing asyncpg, not pytest.
The caller owns scratch database creation and removal. This exercises the old
named schedule constraint's migration in the disposable database, including
an idempotent second application. No role definitions or feeds are changed.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, localcontext
import json
from pathlib import Path
import sys

import asyncpg

from price_collector.settlement_store import SettlementStore, DAY_MS
from price_collector.ghost_twap_store import validate_record
from price_collector.settlement_history import cohort_key, retrospective_cohort, MARKET_CONDITION_SELECTION_VERSION

SMOKE_POLICY = dict(enabled=True, source_max_age_ms=5000, receipt_max_age_ms=3000,
    max_carry_ms=10000, history_ms=120000, max_events=1024, spot_reconnect_max_gap_ms=10000)


AUDIT_INSERT_SQL = """INSERT INTO settlement_audit
    (run_id,decision_id,evaluation_start_ms,market_id,market_start_ms,market_end_ms,
     decision_wall_ns,created_ms,frozen_json,frozen_sha256,compact_json,state_json,version,terminal)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"""


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
        model_version='chainlink-60s-offset3-settlement-v1', policy=dict(SMOKE_POLICY),
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


def condition_observation(start, identity, offset=0, prices=('100.01', '99.97')):
    """An observed-condition audit, with no projection or publication credit."""
    end, now = start + 300_000, start + offset
    wall, mono = now * 1_000_000, (300_000 + offset) * 1_000_000
    signals = {}
    events = {}
    with localcontext() as arithmetic:
        arithmetic.prec = 80
        for sequence, (name, value) in enumerate(zip(('twap', 'spot'), prices), 1):
            price = Decimal(value)
            lead = price - Decimal(100)
            signals[name] = dict(price=value, side='up' if lead > 0 else 'down' if lead < 0 else 'tie',
                signed_lead_usd=str(lead), lead_bps=str(lead * 100))
            events['current_' + name] = dict(feed=name, value=value,
                source_timestamp_ms=now // 1000 * 1000 - 1000,
                received_wall_ns=str(wall - 500_000_000),
                received_monotonic_ns=str(mono - 500_000_000), received_ms=now - 500,
                sequence=sequence, event_id=f'{identity}-{name}', window_s=60 if name == 'twap' else None)
    frozen = dict(run_id='postgres-smoke', decision_id=identity, schema_version=4, kind='market_conditions',
        model_version='market-conditions-v1', rule_version='historical-market-conditions-v1',
        observation_window_s=300, sampling_interval_ms=5000,
        market_id=start // 300_000, market_start_ms=start, market_end_ms=end,
        target_source_timestamp_ms=end, decision_wall_ns=str(wall), decision_monotonic_ns=str(mono),
        decision_time_ms=now, remaining_ms=end-now, valid_until_ms=min(now + 2000, end),
        valid_until_wall_ns=str(min(wall + 2_000_000_000, end * 1_000_000)),
        status='available', quality='healthy', reasons=[], signals=signals, **events,
        reference=dict(price_to_beat='100', condition_id='c', up_token_id='u', down_token_id='d'),
        policy=dict(SMOKE_POLICY))
    frozen['history_cohort'] = cohort_key(frozen)
    state = dict(publication=dict(status='not_published', eligible_before_close=False),
        observation=dict(status='recorded', decision_wall_ns=str(wall), decision_monotonic_ns=str(mono)),
        target=dict(status='not_applicable', first_event=None))
    return dict(run_id=frozen['run_id'], decision_id=identity, decision_wall_ns=wall,
                created_ms=now, frozen_json=json.dumps(frozen), state_json=json.dumps(state),
                version=1, terminal=False)


def insert_parameters(record):
    """Bypass store schedule validation to exercise the database constraint."""
    record = validate_record(record)
    frozen = json.loads(record['frozen_json'])
    compact = {key: value for key, value in frozen.items() if key not in ('slots', 'slot_inputs')}
    return (record['run_id'], record['decision_id'], frozen.get('evaluation_start_ms', 0),
            frozen['market_id'], frozen['market_start_ms'], frozen['market_end_ms'],
            record['decision_wall_ns'], record['created_ms'], record['frozen_json'],
            record['frozen_sha256'], json.dumps(compact), record['state_json'],
            record['version'], record['terminal'])


async def migrate_old_schedule_check(connection):
    """Reproduce the installed old named check, then migrate existing rows."""
    sql = (Path(__file__).resolve().parents[1] / 'schema.sql').read_text(encoding='utf-8')
    start = sql.index('-- Replace the original anonymous final-30s schedule check')
    end = sql.index('CREATE INDEX IF NOT EXISTS settlement_audit_expiry_idx', start)
    migration = sql[start:end]
    async with connection.transaction():
        await connection.execute('ALTER TABLE settlement_audit DROP CONSTRAINT settlement_audit_observation_window_check')
        await connection.execute("""ALTER TABLE settlement_audit
            ADD CONSTRAINT settlement_audit_observation_window_check CHECK (
              decision_wall_ns >= (market_end_ms - CASE
                WHEN compact_json::jsonb->'schema_version'='3'::jsonb
                  AND compact_json::jsonb->>'rule_version'='historical-settlement-v2'
                  AND compact_json::jsonb->'observation_window_s'='60'::jsonb
                  AND compact_json::jsonb->'sampling_interval_ms'='2000'::jsonb THEN 60000
                WHEN NOT (compact_json::jsonb ? 'observation_window_s')
                  AND compact_json::jsonb->'schema_version' IN ('1'::jsonb,'2'::jsonb)
                  AND compact_json::jsonb->>'rule_version'<>'historical-settlement-v2' THEN 30000
                ELSE 0 END) * 1000000
              AND decision_wall_ns < market_end_ms * 1000000
              AND created_ms = decision_wall_ns / 1000000)""")
        for _ in range(2):
            await connection.execute(migration)
        definition = await connection.fetchval("""SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE conrelid='settlement_audit'::regclass
              AND conname='settlement_audit_observation_window_check'""")
        assert 'historical-market-conditions-v1' in definition
        assert 'historical-settlement-v2' in definition
    assert await connection.fetchval("""SELECT convalidated FROM pg_constraint
        WHERE conrelid='settlement_audit'::regclass
          AND conname='settlement_audit_observation_window_check'""") is False


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
        legacy_days = {row['cohort']: row['body_json'] for row in await connection.fetch(
            'SELECT cohort,body_json FROM settlement_history_daily')}
        await migrate_old_schedule_check(connection)
        conditions = SettlementStore(pool, market_conditions=True)
        first = condition_observation(start, 'conditions-first')
        later = condition_observation(start, 'conditions-later', 5000, ('99.8', '99.7'))
        # Persistence arrival order cannot select a later observation or a more
        # favorable margin. Both decisions belong to the 285-300s time bucket.
        assert await conditions.persist(later) == 'inserted'
        assert await conditions.persist(first) == 'inserted', 'schema 4 admits the full-300s boundary'
        assert await conditions.persist(first) == 'unchanged'
        combined = await conditions.maintain(now)
        assert combined['schema_version'] == 2 and combined['status'] == 'available'
        assert combined['selection_version'] == MARKET_CONDITION_SELECTION_VERSION
        assert len(combined['cohorts']) == 1 and combined['cohorts'][0]['observation_window_s'] == 300
        cohort = combined['cohorts'][0]['id']
        body = json.loads(await connection.fetchval(
            'SELECT body_json FROM settlement_history_markets WHERE cohort=$1', cohort))
        assert body['buckets']['285-300']['order'][2] == 'conditions-first'
        assert body['buckets']['285-300']['signals']['twap']['price'] == '100.01'
        assert set(body['buckets']['285-300']['signals']) == {'twap', 'spot'}
        assert len(combined['cells']) == 1 and combined['cells'][0]['signal'] == 'combined'
        cell = combined['cells'][0]
        assert (cell['time_bucket'], cell['twap_margin_bucket'], cell['spot_margin_bucket'], cell['spot_alignment']) == (
            '285-300', '1-2', '2-4', 'opposes')
        assert (cell['resolved'], cell['unknown'], cell['frozen_unknown']) == (0, 1, 1)
        assert cell['win_rate_pct'] is None and cell['interval95_pct'] is None
        assert next(row for row in combined['coverage'] if row['time_bucket'] == '285-300')['selected_markets'] == 1

        await connection.execute('SET ROLE price_writer')
        for field in ('decision_wall_ns', 'decision_monotonic_ns', 'status'):
            changed = json.loads(first['state_json'])
            value = changed['observation'][field]
            changed['observation'][field] = 'unavailable' if field == 'status' else str(int(value) + 1)
            await rejected(connection, "UPDATE settlement_audit SET state_json=$1,version=2 WHERE decision_id='conditions-first'",
                           json.dumps(changed), contains='recorded market observation is immutable')
        removed = json.loads(first['state_json'])
        removed.pop('observation')
        await rejected(connection, "UPDATE settlement_audit SET state_json=$1,version=2 WHERE decision_id='conditions-first'",
                       json.dumps(removed), contains='recorded market observation is immutable')

        for field, invalid in (('schema_version', 3), ('kind', 'settlement'),
                ('model_version', 'unsupported'), ('rule_version', 'historical-settlement-v2'),
                ('observation_window_s', 60), ('sampling_interval_ms', 2000)):
            rejected_record = condition_observation(start, 'invalid-' + field)
            frozen = json.loads(rejected_record['frozen_json'])
            frozen[field] = invalid
            rejected_record['frozen_json'] = json.dumps(frozen)
            await rejected(connection, AUDIT_INSERT_SQL, *insert_parameters(rejected_record),
                           contains='settlement_audit_observation_window_check')
        for offset in (-1, 300_000):
            outside = condition_observation(start, f'outside-{offset}', offset)
            await rejected(connection, AUDIT_INSERT_SQL, *insert_parameters(outside),
                           contains='settlement_audit_observation_window_check')
        for window in (30, 60):
            old_early = decision(start, f'old-contract-{window}-early', -271_000,
                ('100.03', '100.01', '99.97'), observation_window_s=window)
            await rejected(connection, AUDIT_INSERT_SQL, *insert_parameters(old_early),
                           contains='settlement_audit_observation_window_check')
        await connection.execute('RESET ROLE')

        # The metadata transaction has committed. Validate retained rows using
        # its weaker lock, independently of schema installation, while a writer
        # can still update the table. Invalid inserts above were already blocked.
        async with connection.transaction():
            validation_sql = (Path(__file__).resolve().parents[1] / 'deployment' /
                              'validate_market_conditions.sql').read_text(encoding='utf-8')
            await connection.execute(validation_sql)
            locks = await connection.fetch("""SELECT mode FROM pg_locks WHERE pid=pg_backend_pid()
                AND relation='settlement_audit'::regclass AND granted""")
            modes = {row['mode'] for row in locks}
            assert 'ShareUpdateExclusiveLock' in modes and 'AccessExclusiveLock' not in modes
            async with pool.acquire(timeout=5) as concurrent_writer:
                await concurrent_writer.execute("""UPDATE settlement_audit
                    SET history_folded_version=history_folded_version WHERE decision_id='conditions-first'""")
        assert await connection.fetchval("""SELECT convalidated FROM pg_constraint
            WHERE conrelid='settlement_audit'::regclass
              AND conname='settlement_audit_observation_window_check'""") is True

        for record in (first, later):
            record.update(version=2, terminal=True)
            assert await conditions.persist(record) == 'updated'
        repeated_combined = await conditions.maintain(now)
        assert repeated_combined['cells'] == combined['cells'], 'condition retries never count a second market'
        assert await conditions.persist(first) == 'unchanged', 'condition compaction preserves recovery'
        saved_days = {row['cohort']: row['body_json'] for row in await connection.fetch(
            'SELECT cohort,body_json FROM settlement_history_daily')}
        assert len(saved_days) == 3 and all(saved_days[key] == value for key, value in legacy_days.items())
        assert await connection.fetchval('SELECT bool_and(history_folded_version=version) FROM settlement_audit')
        assert await connection.fetchval("SELECT count(*) FROM settlement_audit WHERE decision_id LIKE 'invalid-%'") == 0
        guard = await conditions.guard()
        assert guard['capacity_ok'] and guard['row_count'] == 5
        original_days = dict(saved_days)
        original_markets = {(row['cohort'], row['market_id']): row['body_json'] for row in await connection.fetch(
            'SELECT cohort,market_id,body_json FROM settlement_history_markets')}
        configured = SettlementStore(pool, market_conditions=True, history_cohort=cohort,
                                     history_policy=SMOKE_POLICY)
        previous_count = len(original_days)
        for _ in range(2):
            combined_retained = await configured.maintain(now)
            assert combined_retained['schema_version'] == 3
            assert combined_retained['retrospective_backfill']['processed_groups'] == 1
            current_count = await connection.fetchval('SELECT count(*) FROM settlement_history_daily')
            assert current_count == previous_count + 1, 'one separate derived day per pass'
            previous_count = current_count
        repeated_retained = await configured.maintain(now)
        assert repeated_retained['retrospective_backfill']['status'] == 'complete'
        assert repeated_retained['cells'] == combined_retained['cells']
        assert len(repeated_retained['cohorts']) == 3
        assert len(repeated_retained['cells']) == 3 and sum(cell[7] for cell in repeated_retained['cells']) == 3
        preserved = {row['cohort']: row['body_json'] for row in await connection.fetch(
            'SELECT cohort,body_json FROM settlement_history_daily')}
        assert len(preserved) == 5 and all(preserved[key] == value for key, value in original_days.items())
        assert {(row['cohort'], row['market_id']): row['body_json'] for row in await connection.fetch(
            'SELECT cohort,market_id,body_json FROM settlement_history_markets')} == original_markets
        assert await connection.fetchval('SELECT count(*) FROM settlement_audit') == 5
        await connection.execute('SET ROLE price_writer')
        for original_cohort in legacy_days:
            derived_cohort = retrospective_cohort(original_cohort)
            derived = json.loads(preserved[derived_cohort])
            assert derived['final'] and derived['source_final']
            assert derived['description']['source_cohort'] == original_cohort
            assert derived['outcome_freeze_ms'] == json.loads(legacy_days[original_cohort])['outcome_freeze_ms']
            await rejected(connection, 'UPDATE settlement_history_daily SET updated_ms=updated_ms+1 WHERE cohort=$1',
                           derived_cohort, contains='frozen history day is immutable')
        await connection.execute('RESET ROLE')
        print(json.dumps(dict(database=name, server=await connection.fetchval('SHOW server_version'),
            result='passed', checks=['writer persistence and idempotency', 'first ACK per time bucket',
                'immutable audit and publication clocks', 'seven-day individual deletion guard',
                'daily freeze and ninety-day aggregate guard', 'terminal compaction',
                'bounded fold and outcome join SQL', 'reader/writer privilege split', 'capacity query',
                'versioned sixty-second admission', 'separate legacy and minute history buckets',
                'old named schedule constraint migration and repeat application',
                'committed NOT VALID check still enforces new rows',
                'separate constraint validation allows concurrent writer',
                'full-market observed-condition boundary admission', 'immutable recorded observations',
                'exact schema-4 contract and half-open schedule rejection',
                'combined condition summary and first observation selection',
                'condition compaction and retry idempotency', 'legacy daily totals preserved',
                'policy-enabled retained-market backfill emits schema 3',
                'one separate compact retrospective day per pass',
                'retrospective idempotency and immutable final rows',
                'original daily, market and audit evidence unchanged'],
                relation_bytes=guard['relation_bytes'])))
    finally:
        if pool is not None:
            await pool.close()
        await connection.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: settlement_postgres_smoke.py settlement_validation_DATABASE')
    asyncio.run(main(sys.argv[1]))
