"""Shutdown retains accepted evidence across slow I/O and caller cancellation."""
import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import threading
from types import SimpleNamespace

import pytest

import price_collector.ghost_twap_runtime as module
import price_collector.polymarket_chainlink_collector as collector
from price_collector.ghost_twap import NS_PER_SECOND
from price_collector.ghost_twap_spool import GhostSpool
from tests.test_ghost_twap_runtime_faults import BASE, issue, runtime


@pytest.fixture(autouse=True)
def budgets(monkeypatch):
    monkeypatch.setattr(module, 'SHUTDOWN_QUIESCE_SECONDS', .1)
    monkeypatch.setattr(module, 'SHUTDOWN_SPOOL_SECONDS', 10)
    monkeypatch.setattr(module, 'SHUTDOWN_DRAIN_SECONDS', 10)
    monkeypatch.setattr(module, 'SHUTDOWN_RESOURCE_SECONDS', .2)
    monkeypatch.setattr(module, 'SHUTDOWN_RETRY_SECONDS', .005)


def add_tail(value, count):
    first = issue(value)
    for number in range(2, count + 1):
        decision = replace(first.decision, decision_id=str(number))
        frozen = json.loads(first.frozen_json)
        frozen['decision_id'] = str(number)
        row = module.PendingDecision(decision, json.dumps(frozen), deepcopy(first.state))
        value.records[str(number)] = row
        value._changed(row)
    return first


def test_real_spool_243_row_tail_drains_beyond_old_scaled_outer_budget(tmp_path):
    async def scenario():
        value, _, _, store, _ = runtime()
        value.spool = GhostSpool(tmp_path)
        await value._spool(value.spool.open)
        add_tail(value, 243)
        original = store.persist
        async def delayed(record):
            await asyncio.sleep(.001)
            return await original(record)
        store.persist = delayed
        started = asyncio.get_running_loop().time()
        await value.close()
        assert asyncio.get_running_loop().time() - started > .05
        assert len(store.persisted) == 243
        assert all(record['terminal'] for record in store.persisted)
        assert all(t['status'] == 'restart_unmatched' for record in store.persisted
                   for t in json.loads(record['state_json'])['targets'].values())
        assert not list(tmp_path.glob('*.row'))
        assert value.spool._lock is None
        assert value.shutdown_summary['drained_records'] == 243
        assert value.shutdown_summary['drain_complete'] is True
        assert not value.records and not value._dirty
    asyncio.run(scenario())


def test_database_failure_retains_all_final_versions_not_only_first_batch(monkeypatch, tmp_path):
    monkeypatch.setattr(module, 'SHUTDOWN_DRAIN_SECONDS', .04)
    async def scenario():
        value, _, _, store, _ = runtime()
        value.spool = GhostSpool(tmp_path)
        await value._spool(value.spool.open)
        add_tail(value, 40)
        store.failure = OSError('database unavailable')
        await value.close()
        disk = value.spool.read_all()
        assert len(disk) == 40
        assert all(r['terminal'] for r in disk)
        assert {r['decision_id']: r for r in disk} == {k: r.record() for k, r in value.records.items()}
        assert value.shutdown_summary['retained_records'] == 40
        assert value.shutdown_summary['final_outbox_pass_complete'] is True
        assert value.shutdown_summary['drain_complete'] is False
        assert value.spool._lock is None
    asyncio.run(scenario())


def test_inflight_ack_and_queued_target_survive_shutdown_without_next_publication():
    async def scenario():
        value, clock, _, store, redis = runtime()
        row = issue(value)
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed(*args):
            redis.calls.append(args)
            entered.set()
            await release.wait()
            return 1
        redis.eval = delayed
        publisher = asyncio.create_task(value.publish(row))
        value._tasks = [publisher]
        await entered.wait()
        clock.advance(NS_PER_SECOND)
        value.offer_price('twap', row.decision.current_twap.value,
                          BASE + 101000, clock.wall, clock.mono, 'accepted-at-close', 60)
        assert value.queue
        closing = asyncio.create_task(value.close())
        await asyncio.sleep(.01)
        assert value._closed
        release.set()
        await closing
        record = next(r for r in reversed(store.persisted) if r['decision_id'] == row.decision.decision_id)
        state = json.loads(record['state_json'])
        assert state['publication']['status'] == 'acknowledged'
        assert state['publication']['payload_json'].encode() == redis.calls[0][-2]
        assert state['targets']['1']['status'] == 'matched'
        assert state['targets']['1']['first_event']['event_id'] == 'accepted-at-close'
        assert state['targets']['5']['status'] == 'restart_unmatched'
        assert not value.queue and len(redis.calls) == 1
        assert value.issue() is None
    asyncio.run(scenario())


def test_forced_cancel_during_redis_attempt_records_uncertain(monkeypatch):
    monkeypatch.setattr(module, 'SHUTDOWN_QUIESCE_SECONDS', .01)
    async def scenario():
        value, _, _, store, redis = runtime()
        row = issue(value)
        entered = asyncio.Event()
        async def blocked(*args):
            entered.set()
            await asyncio.Event().wait()
        redis.eval = blocked
        publisher = asyncio.create_task(value.publish(row))
        value._tasks = [publisher]
        await entered.wait()
        await value.close()
        state = json.loads(store.persisted[-1]['state_json'])
        assert state['publication']['status'] == 'uncertain'
        assert 'ack_monotonic_ns' not in state['publication']
        assert state['publication']['payload_json']
    asyncio.run(scenario())


def test_caller_cancellation_keeps_filesystem_and_spool_owned_until_finished(monkeypatch):
    monkeypatch.setattr(module, 'SHUTDOWN_SPOOL_SECONDS', .02)
    async def scenario():
        value, _, spool, _, _ = runtime()
        issue(value)
        spool.open()
        entered, release = threading.Event(), threading.Event()
        first = True
        def block_once():
            nonlocal first
            if first:
                first = False
                entered.set()
                assert release.wait(3)
        spool.before_write = block_once
        closing = asyncio.create_task(value.close())
        while not entered.is_set():
            await asyncio.sleep(.001)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        await asyncio.sleep(.04)
        assert spool.opened and not value._close_task.done()
        release.set()
        await value.close()
        assert not spool.opened
        assert value.shutdown_summary['drain_complete'] is True
    asyncio.run(scenario())


def test_quiesce_timeout_cannot_race_worker_fsync_with_terminal_snapshot(monkeypatch):
    monkeypatch.setattr(module, 'SHUTDOWN_QUIESCE_SECONDS', .01)
    async def scenario():
        value, _, spool, store, redis = runtime()
        row = issue(value)
        spool.open()
        entered, release = threading.Event(), threading.Event()
        first = True
        def block_once():
            nonlocal first
            if first:
                first = False
                entered.set()
                assert release.wait(3)
        spool.before_write = block_once
        publisher = asyncio.create_task(value.publish(row))
        value._tasks = [publisher]
        while not entered.is_set():
            await asyncio.sleep(.001)
        closing = asyncio.create_task(value.close())
        await asyncio.sleep(.03)
        assert not closing.done() and spool.opened
        assert not row.terminal and not store.persisted
        release.set()
        await closing
        assert publisher.cancelled()
        assert len(store.persisted) == 1 and store.persisted[0] == row.record()
        assert row.terminal and row.state['publication']['status'] == 'intent'
        assert not redis.calls and not spool.opened
    asyncio.run(scenario())


def test_transient_database_failure_retries_without_changing_final_evidence():
    async def scenario():
        value, _, spool, store, _ = runtime()
        row = issue(value)
        original = store.persist
        attempts = []
        async def retry(record):
            attempts.append(deepcopy(record))
            if len(attempts) == 1:
                raise OSError('transient database fault')
            return await original(record)
        store.persist = retry
        await value.close()
        assert len(attempts) == 2 and attempts[0] == attempts[1] == row.record()
        assert value.shutdown_summary['drain_complete'] and not spool.rows
    asyncio.run(scenario())


def test_late_target_failure_is_reported_as_pending_not_durable(monkeypatch):
    monkeypatch.setattr(module, 'SHUTDOWN_DRAIN_SECONDS', .02)
    async def scenario():
        value, clock, _, store, _ = runtime()
        value.offer_price('twap', Decimal('100'), BASE + 100000,
                          clock.wall, clock.mono, 'late-only', 60)
        async def fail(*args, **kwargs):
            raise OSError('database offline')
        store.note_late_target = fail
        await value.close()
        assert not value.records
        assert value.shutdown_summary['pending_late_events'] == 1
        assert value.shutdown_summary['drain_complete'] is False
    asyncio.run(scenario())


def test_late_events_are_drained_even_with_no_dirty_decisions():
    async def scenario():
        value, clock, _, store, _ = runtime()
        value.offer_price('twap', Decimal('100'), BASE + 100000,
                          clock.wall, clock.mono, 'late-only', 60)
        value.drain_inputs()
        assert value.late_events and not value.records
        await value.close()
        assert store.late_calls and not value.late_events
        assert value.shutdown_summary['drain_complete']
    asyncio.run(scenario())


def test_outer_sink_timeout_detaches_owned_cleanup_without_cancelling(monkeypatch, caplog):
    monkeypatch.setattr(collector, 'GHOST_SHUTDOWN_TIMEOUT_SECONDS', .02)
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        class Runtime:
            async def close(self):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    calls.append('cancelled')
                    raise
                calls.append('closed')
        instance = Runtime()
        async def factory(settings):
            return instance
        monkeypatch.setattr(collector, 'start_ghost_runtime', factory)
        sink = collector._OptionalGhostSink(None)
        await sink.start_task
        await sink.close()
        assert entered.is_set() and calls == []
        assert sink._close_task in collector._GHOST_CLEANUP_TASKS
        assert 'cleanup_still_owned' in caplog.text
        release.set()
        await sink.close()
        assert calls == ['closed']
    asyncio.run(scenario())


@pytest.mark.parametrize('startup', [False, True])
def test_factory_cancellation_does_not_close_clients_before_runtime(monkeypatch, tmp_path, startup):
    import asyncpg
    import redis.asyncio as redis_async
    from price_collector.config import Settings
    async def scenario():
        calls = []
        entered, release, started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        class Runtime:
            def __init__(self, *args):
                pass
            async def start(self):
                started.set()
                if startup:
                    await asyncio.Event().wait()
            async def close(self):
                entered.set()
                await release.wait()
                calls.append('runtime')
        class Pool:
            async def close(self):
                calls.append('pool')
        class Redis:
            def __init__(self, **kwargs):
                pass
            async def aclose(self):
                calls.append('redis')
        async def create_pool(**kwargs):
            return Pool()
        monkeypatch.setattr(module, 'GhostSettings', lambda: SimpleNamespace(
            enabled=True, state_directory=tmp_path, audit_max_records=2, record_max_bytes=1024))
        monkeypatch.setattr(module, 'GhostRuntime', Runtime)
        monkeypatch.setattr(asyncpg, 'create_pool', create_pool)
        monkeypatch.setattr(redis_async, 'Redis', Redis)
        factory = asyncio.create_task(module.start_ghost_runtime(Settings()))
        await started.wait()
        if startup:
            task = factory
            task.cancel()
        else:
            value = await factory
            task = asyncio.create_task(value.close())
        await entered.wait()
        task.cancel()
        await asyncio.sleep(.01)
        assert calls == []
        release.set()
        if startup:
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
            await value.close()
        assert calls == ['runtime', 'redis', 'pool']
    asyncio.run(scenario())
