"""Durable historical observations do not depend on forecast publication."""
import asyncio
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from price_collector.ghost_twap_spool import GhostSpool
from price_collector.ghost_twap_store import GhostAuditConflict
from price_collector.settlement_runtime import MarketConditionsRuntime
from price_collector.settlement_store import SettlementStore, history_eligible
from test_settlement import decision, END, NS
from test_settlement_runtime import setup, Store
from test_settlement_store import Pool, HistoryConnection, START as HISTORY_START


def observation_runtime(tmp_path, snapshot=None):
    legacy, snapshot, clock = setup(tmp_path, snapshot or decision(horizon=182, remaining=180_000))
    runtime = MarketConditionsRuntime(legacy.parent, legacy.settings, legacy.store, legacy.spool)
    runtime.context, runtime.context_wall, runtime.context_mono = (
        legacy.context, legacy.context_wall, legacy.context_mono)
    runtime.guard, runtime.guard_mono = legacy.guard, legacy.guard_mono
    return runtime, snapshot, clock


def test_full_market_observation_is_durable_without_a_redis_forecast(tmp_path):
    async def run():
        runtime, snapshot, clock = observation_runtime(tmp_path)
        runtime.offer(snapshot, 0)
        row = runtime.records['1']
        assert row['projection']['remaining_ms'] == 180_000
        assert not history_eligible(row['projection'], row['state'])
        assert 'projected_price' not in row['projection']
        assert set(row['projection']['signals']) == {'twap', 'spot'}
        original = runtime._save
        calls = []
        async def save(record):
            calls.append(deepcopy(record['state']))
            result = await original(record)
            # Delayed disk acknowledgement must not relabel the earlier
            # observed inputs as a fresh live prediction.
            clock['wall'] += 5000 * NS
            clock['mono'] += 5000 * NS
            return result
        runtime._save = save
        await runtime.publish_one(row)
        assert calls[0]['observation']['status'] == 'pending'
        assert calls[1]['observation']['status'] == 'recorded'
        assert history_eligible(row['projection'], row['state'])
        assert not runtime.redis.publications
        assert not runtime.store.rows
        runtime.store.failure = True
        with pytest.raises(OSError):
            await runtime.flush()
        assert runtime.spool.read_all()[0]['frozen_json'] == row['frozen_json']
        runtime.store.failure = False
        await runtime.flush()
        assert runtime.store.rows[-1]['frozen_json'] == row['frozen_json']
        await runtime.close()
    asyncio.run(run())


def test_sampling_is_bounded_without_reopening_an_admitted_five_second_interval(tmp_path):
    async def run():
        runtime, snapshot, clock = observation_runtime(tmp_path)
        runtime.guard['capacity_ok'] = False
        runtime.offer(snapshot, 0)
        assert not runtime.records
        runtime.guard['capacity_ok'] = True
        runtime.offer(snapshot, 0)
        runtime.offer(replace(snapshot, decision_id='2',
            decision_wall_ns=clock['wall'] + 2000 * NS,
            decision_monotonic_ns=clock['mono'] + 2000 * NS), 0)
        assert list(runtime.records) == ['1']
        later = replace(decision(horizon=177, remaining=175_000), decision_id='3')
        clock.update(wall=later.decision_wall_ns, mono=later.decision_monotonic_ns)
        runtime.parent.guard['monotonic_ns'] = clock['mono']
        runtime.offer(later, 0)
        assert list(runtime.records) == ['1', '3']
        runtime.offer(replace(snapshot, decision_id='4'), 0)
        assert runtime.counters['sampling_skipped'] == 2
        await runtime.close()
    asyncio.run(run())


def test_recorded_observation_survives_restart_without_publication(tmp_path):
    async def run():
        runtime, snapshot, _ = observation_runtime(tmp_path)
        runtime.offer(snapshot, 0)
        await runtime.publish_one(runtime.records['1'])
        before = runtime.spool.read_all()[0]
        runtime.spool.close()
        resumed = MarketConditionsRuntime(runtime.parent, runtime.settings, Store(),
            GhostSpool(tmp_path / 'settlement'))
        await resumed.start()
        recovered = resumed.store.rows[0]
        frozen, state = json.loads(recovered['frozen_json']), json.loads(recovered['state_json'])
        assert recovered['frozen_json'] == before['frozen_json']
        assert history_eligible(frozen, state)
        assert recovered['terminal'] and not runtime.redis.publications
        await resumed.close()
    asyncio.run(run())


def test_recorded_condition_identity_is_immutable_in_store(tmp_path):
    async def run():
        runtime, snapshot, _ = observation_runtime(tmp_path)
        runtime.offer(snapshot, 0)
        row = runtime.records['1']
        await runtime.publish_one(row)
        store = SettlementStore(Pool(), market_conditions=True)
        record = runtime.record(row)
        assert await store.persist(record) == 'inserted'
        assert await store.persist(record) == 'unchanged'
        changed = deepcopy(record)
        state = json.loads(changed['state_json'])
        state['observation']['status'] = 'pending'
        changed.update(state_json=json.dumps(state), version=record['version'] + 1)
        with pytest.raises(GhostAuditConflict, match='observation is immutable'):
            await store.persist(changed)
        state = json.loads(record['state_json'])
        state['observation']['decision_wall_ns'] = str(int(snapshot.decision_wall_ns) + 1)
        assert not history_eligible(row['projection'], state)
        await runtime.close()
    asyncio.run(run())


def test_current_market_does_not_block_closed_market_history_from_the_same_day(tmp_path):
    async def run():
        runtime, snapshot, _ = observation_runtime(tmp_path)
        runtime.offer(snapshot, 0)
        await runtime.publish_one(runtime.records['1'])
        prototype = runtime.record(runtime.records['1'])
        pool = Pool()
        pool.connection = HistoryConnection()
        store = SettlementStore(pool, market_conditions=True)
        def moved(identifier, start):
            record = deepcopy(prototype)
            frozen, state = json.loads(record['frozen_json']), json.loads(record['state_json'])
            delta_ms = start - frozen['market_start_ms']
            frozen.update(decision_id=identifier, market_id=start // 300_000,
                market_start_ms=start, market_end_ms=start + 300_000,
                target_source_timestamp_ms=start + 300_000)
            frozen['decision_wall_ns'] = str(int(frozen['decision_wall_ns']) + delta_ms * NS)
            frozen['valid_until_wall_ns'] = str(int(frozen['valid_until_wall_ns']) + delta_ms * NS)
            frozen['decision_time_ms'] += delta_ms
            frozen['reference'].update(condition_id='c', up_token_id='u', down_token_id='d')
            state['observation']['decision_wall_ns'] = frozen['decision_wall_ns']
            record.update(decision_id=identifier, created_ms=frozen['decision_time_ms'],
                decision_wall_ns=int(frozen['decision_wall_ns']), frozen_json=json.dumps(frozen),
                state_json=json.dumps(state))
            return record
        await store.persist(moved('closed', HISTORY_START))
        await store.persist(moved('open', HISTORY_START + 300_000))
        summary = await store.maintain(HISTORY_START + 450_000)
        assert summary['schema_version'] == 2 and summary['status'] == 'available'
        assert summary['cohorts'][0]['observed_markets'] == 1
        assert sum(cell['resolved'] for cell in summary['cells']) == 1
        assert pool.connection.audit[(snapshot.run_id, 'open')].get('history_folded_version', -1) == -1
        await runtime.close()
    asyncio.run(run())
