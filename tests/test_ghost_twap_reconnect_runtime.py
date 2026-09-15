"""Reconnect admission races and immutable audit evidence, without external IO."""
import asyncio
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json

import pytest

from price_collector.ghost_twap import NS_PER_MS, NS_PER_SECOND
from price_collector.ghost_twap_runtime import GhostRuntime, RESERVE_BYTES
from price_collector.ghost_twap_spool import GhostSpool
from price_collector.ghost_twap_store import (
    encode_export_row, iter_export_proofs, validate_record, verify_export_file,
)
from test_ghost_twap_runtime_faults import (
    BASE, Redis, Store, issue, run, runtime, target,
    same_loop_for_runtime_construction_and_io,
)


def reconnect_record(row):
    return json.loads(row.frozen_json)['spot_reconnect']


def offer_current(value, clock, second, *, feed='spot', price='100'):
    value.offer_price(feed, Decimal(price), BASE + second * 1000,
                      clock.wall, clock.mono, f'{feed}-reconnect-{second}',
                      60 if feed == 'twap' else None)


def take_pending(value, row):
    """Mirror the publication loop's dequeue before invoking publish directly."""
    assert value._pending_publication == row.decision.decision_id
    value._pending_publication = None


def block_spool(value, spool):
    entered, release = asyncio.Event(), asyncio.Event()
    original = value._spool

    async def blocked(method, *args):
        if method == spool.write:
            entered.set()
            await release.wait()
        return await original(method, *args)

    value._spool = blocked
    return entered, release


@pytest.mark.parametrize('feed', ['spot', 'twap'])
def test_gap_fences_reserved_publication_at_admission_before_queue_drain(feed):
    value, _, _, _, redis = runtime()
    old = issue(value)
    old_epoch, frozen = value._publication_epoch, old.frozen_json
    value.offer_gap(feed, 'connection_end')
    assert value._publication_epoch == old_epoch + 1
    assert value.queue, 'the admission fence must not depend on engine processing'
    assert not value._publishable(old)
    take_pending(value, old)
    run(value.publish(old))
    assert not redis.calls
    assert old.state['publication']['status'] == 'expired_or_target_received'
    assert old.frozen_json == frozen


@pytest.mark.parametrize('clock_name', ['observed_wall_ns', 'observed_monotonic_ns'])
@pytest.mark.parametrize('bad_clock', [True, -1, 1.5])
def test_invalid_partial_gap_clock_is_rejected_before_mutating_engine_evidence(clock_name, bad_clock):
    value, clock, _, _, _ = runtime()
    original = issue(value)
    frozen = original.decision.to_audit_json()
    with pytest.raises((TypeError, ValueError)):
        value.engine.record_gap('spot', 'connection_end', **{clock_name: bad_clock})
    after = value.engine.snapshot(original.decision.decision_id, clock.wall, clock.mono)
    assert after.to_audit_json() == frozen
    assert value.engine.spot_reconnect is None


def test_input_overflow_fences_old_publication_before_implicit_gap_drain():
    value, clock, _, _, redis = runtime(input_queue_max=16)
    old = issue(value)
    epoch = value._publication_epoch
    for _ in range(16):
        offer_current(value, clock, 100)
    assert value._publication_epoch == epoch
    offer_current(value, clock, 100)
    assert value._publication_epoch == epoch + 1
    assert len(value.queue) <= 16 and value.counters['input_drops'] == 16
    assert not value._publishable(old)
    take_pending(value, old)
    run(value.publish(old))
    assert not redis.calls


@pytest.mark.parametrize('recover_before_write_finishes', [False, True])
def test_gap_during_spool_keeps_old_row_fenced_even_after_reconnect(recover_before_write_finishes):
    async def scenario():
        value, clock, spool, _, redis = runtime()
        old = issue(value)
        take_pending(value, old)
        frozen = old.frozen_json
        entered, release = block_spool(value, spool)
        publication = asyncio.create_task(value.publish(old))
        await entered.wait()
        clock.advance(100 * NS_PER_MS)
        value.offer_gap('spot', 'connection_end')
        assert not value._publishable(old)
        newer = None
        if recover_before_write_finishes:
            clock.advance(900 * NS_PER_MS)
            offer_current(value, clock, 101)
            offer_current(value, clock, 101, feed='twap')
            newer = value.issue()
            assert newer is not None
            assert reconnect_record(newer)['status'] == 'retained'
            assert all(f.price == Decimal('100') for f in newer.decision.forecasts)
            assert not value._publishable(old), 'recovery must not revive an older epoch'
        release.set()
        await publication
        assert old.state['publication']['status'] == 'preempted_after_spool'
        assert not redis.calls
        assert old.frozen_json == frozen
        saved = spool.rows[(old.decision.run_id, old.decision.decision_id)]
        assert saved['frozen_json'] == frozen
        assert json.loads(saved['state_json'])['publication']['status'] == 'intent'
        if newer is not None:
            take_pending(value, newer)
            await value.publish(newer)
            assert newer.state['publication']['status'] == 'acknowledged'
            assert len(redis.calls) == 1
            payload = json.loads(redis.calls[0][-2])
            assert payload['decision_id'] == newer.decision.decision_id
            assert payload['spot_reconnect'] == reconnect_record(newer)
    run(scenario())


def test_gap_during_redis_await_preserves_actual_attempt_ack_and_target_order():
    async def scenario():
        value, clock, _, _, redis = runtime()
        old = issue(value)
        take_pending(value, old)
        frozen = old.frozen_json
        entered, release = asyncio.Event(), asyncio.Event()

        async def delayed_eval(*args):
            redis.calls.append(args)
            entered.set()
            await release.wait()
            return 1

        redis.eval = delayed_eval
        publication = asyncio.create_task(value.publish(old))
        await entered.wait()
        attempted = old.state['publication']['payload_json']
        attempt_mono = old.state['publication']['attempt_monotonic_ns']
        clock.advance(NS_PER_SECOND)
        value.offer_gap('spot', 'connection_end')
        offer_current(value, clock, 101)
        offer_current(value, clock, 101, feed='twap')
        value.drain_inputs()
        assert old.state['targets']['1']['status'] == 'matched'
        clock.advance(NS_PER_MS)
        release.set()
        await publication
        assert len(redis.calls) == 1
        assert old.state['publication']['status'] == 'acknowledged'
        assert old.state['publication']['attempt_monotonic_ns'] == attempt_mono
        assert old.state['publication']['payload_json'] == attempted
        assert redis.calls[0][-2].decode() == attempted
        assert old.frozen_json == frozen
        assert old.state['targets']['1']['confirmed_redis_lead_ns'] is None
        clock.advance(999 * NS_PER_MS)
        value.observe_target(target(value, old, clock, horizon=2))
        assert old.state['targets']['2']['confirmed_redis_lead_ns'] == 999 * NS_PER_MS
        assert not value._publishable(old)
    run(scenario())


def test_queued_gap_uses_original_nanosecond_clocks_and_waiting_cannot_forecast():
    value, clock, _, _, redis = runtime()
    original = issue(value)
    frozen = original.frozen_json
    clock.advance(123 * NS_PER_MS + 456)
    gap_wall, gap_mono = clock.wall, clock.mono
    value.offer_gap('spot', 'connection_end')
    clock.advance(100 * NS_PER_MS)
    waiting = value.issue()
    assert waiting is not None
    before_reconnect = waiting.frozen_json
    waiting_meta = reconnect_record(waiting)
    assert waiting_meta['status'] == 'waiting'
    assert waiting_meta['gap_wall_ns'] == str(gap_wall)
    assert waiting_meta['gap_monotonic_ns'] == str(gap_mono)
    assert waiting_meta['previous_spot']['event_id'] == 'spot-100'
    assert waiting_meta['first_post_gap_spot'] is None
    assert all(f.price is None for f in waiting.decision.forecasts)
    take_pending(value, waiting)
    run(value.publish(waiting))
    assert not redis.calls
    clock.advance(900 * NS_PER_MS)
    offer_current(value, clock, 101)
    restored = value.issue()
    assert restored is not None
    meta = reconnect_record(restored)
    assert meta['status'] == 'retained'
    assert meta['gap_ordinal'] == waiting_meta['gap_ordinal']
    assert meta['gap_wall_ns'] == str(gap_wall)
    assert meta['gap_monotonic_ns'] == str(gap_mono)
    assert meta['first_post_gap_spot']['received_wall_ns'] == str(clock.wall)
    assert meta['first_post_gap_spot']['received_monotonic_ns'] == str(clock.mono)
    assert all(f.price == Decimal('100') for f in restored.decision.forecasts)
    gaps = json.loads(restored.frozen_json)['operational_gaps']
    assert gaps[-1]['observed_wall_ns'] == str(gap_wall)
    assert gaps[-1]['observed_monotonic_ns'] == str(gap_mono)
    assert original.frozen_json == frozen and waiting.frozen_json == before_reconnect
    assert value.counters['spot_reconnect_waits'] == 1
    assert value.counters['spot_reconnect_retained'] == 1
    assert value.counters['resets'] == 0


def test_hard_gap_after_recovery_clears_coverage_without_rewriting_retained_audit():
    value, clock, _, _, _ = runtime()
    issue(value)
    clock.advance(100 * NS_PER_MS)
    value.offer_gap('spot', 'connection_end')
    clock.advance(900 * NS_PER_MS)
    offer_current(value, clock, 101)
    restored = value.issue()
    assert restored is not None
    frozen = restored.frozen_json
    assert reconnect_record(restored)['status'] == 'retained'
    value.offer_gap('spot', 'input_overflow')
    clock.advance(NS_PER_SECOND)
    offer_current(value, clock, 102)
    offer_current(value, clock, 102, feed='twap')
    cleared = value.issue()
    assert cleared is not None
    assert reconnect_record(cleared)['status'] == 'cleared'
    assert reconnect_record(cleared)['reason'] == 'hard_gap:input_overflow'
    assert all(f.price is None and 'missing_slots' in f.reasons for f in cleared.decision.forecasts)
    assert all(event['source_timestamp_ms'] >= BASE + 102000
               for event in json.loads(cleared.frozen_json)['slot_inputs'])
    assert restored.frozen_json == frozen
    assert value.counters['resets'] == 1


def test_timeout_clear_counter_is_recorded_once_in_frozen_runtime_evidence():
    value, clock, _, _, _ = runtime()
    issue(value)
    clock.advance(100 * NS_PER_MS)
    value.offer_gap('spot', 'connection_end')
    value.drain_inputs()
    clock.advance(10 * NS_PER_SECOND)
    run(value.refresh_guard())
    timed_out = value.issue()
    assert timed_out is not None
    assert reconnect_record(timed_out)['status'] == 'cleared'
    assert reconnect_record(timed_out)['reason'] == 'reconnect_timeout'
    assert all(f.price is None for f in timed_out.decision.forecasts)
    assert value.counters['spot_reconnect_cleared'] == 1
    assert json.loads(timed_out.frozen_json)['runtime_counters']['spot_reconnect_cleared'] == 1
    value.issue()
    assert value.counters['spot_reconnect_cleared'] == 1


@pytest.mark.parametrize('reason', ['connection_end', 'input_overflow'])
def test_gap_cancelled_waiting_recovery_is_counted_once(reason):
    value, clock, _, _, _ = runtime()
    issue(value)
    clock.advance(100 * NS_PER_MS)
    value.offer_gap('spot', 'connection_end')
    value.drain_inputs()
    assert value.counters['spot_reconnect_waits'] == 1
    clock.advance(100 * NS_PER_MS)
    value.offer_gap('spot', reason)
    cancelled = value.issue()
    assert cancelled is not None
    assert reconnect_record(cancelled)['status'] == 'cleared'
    assert all(f.price is None for f in cancelled.decision.forecasts)
    assert value.counters['spot_reconnect_cleared'] == 1
    assert json.loads(cancelled.frozen_json)['runtime_counters']['spot_reconnect_cleared'] == 1
    value.offer_gap('spot', 'input_overflow')
    value.drain_inputs()
    assert value.counters['spot_reconnect_cleared'] == 1


@pytest.mark.parametrize('durable_ack', [False, True])
def test_reconnect_metadata_survives_real_spool_export_and_restart_without_republishing(tmp_path, durable_ack):
    async def scenario():
        value, clock, _, _, redis = runtime()
        issue(value)
        clock.advance(100 * NS_PER_MS)
        value.offer_gap('spot', 'connection_end')
        clock.advance(900 * NS_PER_MS)
        offer_current(value, clock, 101)
        offer_current(value, clock, 101, feed='twap')
        row = value.issue()
        assert row is not None
        take_pending(value, row)
        frozen, metadata = row.frozen_json, deepcopy(reconnect_record(row))
        assert metadata['status'] == 'retained'
        assert json.loads(frozen)['runtime_version'] == 'ghost-canary-v6'
        assert json.loads(frozen)['contract_version'] == 4
        assert any(event['source_timestamp_ms'] < BASE + 100000
                   for event in json.loads(frozen)['slot_inputs'])
        real = GhostSpool(tmp_path/'outbox')
        real.open()
        value.spool = real
        try:
            await value.publish(row)
            assert row.state['publication']['status'] == 'acknowledged'
            if durable_ack:
                clock.advance(2 * NS_PER_SECOND)
                value.observe_target(target(value, row, clock, horizon=2))
                real.write(row.record())
        finally:
            real.close()
        reopened = GhostSpool(tmp_path/'outbox')
        reopened.open()
        try:
            saved, = reopened.read_all()
        finally:
            reopened.close()
        assert saved['frozen_json'] == frozen
        before_state = json.loads(saved['state_json'])
        recovered_store, recovered_redis = Store(), Redis()
        recovered = GhostRuntime(value.settings, recovered_store, recovered_redis, reopened,
                                 wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono,
                                 disk_free=lambda: 2 * RESERVE_BYTES)
        await recovered._recover(saved)
        record = validate_record(recovered_store.persisted[-1])
        state = json.loads(record['state_json'])
        assert record['terminal'] and record['frozen_json'] == frozen
        assert json.loads(record['frozen_json'])['spot_reconnect'] == metadata
        assert state['publication']['payload_json'] == before_state['publication']['payload_json']
        assert not recovered_redis.calls
        assert recovered.engine.history_size == 0, 'audit recovery must not seed a new live run'
        if durable_ack:
            assert state['publication']['status'] == 'acknowledged'
            assert state['targets']['2']['confirmed_redis_lead_ns'] == str(2 * NS_PER_SECOND)
            assert state['publication']['payload_json'].encode() == redis.calls[0][-2]
        else:
            assert state['publication']['status'] == 'intent'
            assert state['publication']['restart_outcome'] == 'unconfirmed_after_restart'
            assert 'ack_monotonic_ns' not in state['publication']
            assert all(t['confirmed_redis_lead_ns'] is None for t in state['targets'].values())
        raw = encode_export_row(record)
        export = tmp_path/'reconnect.jsonl'
        export.write_bytes(raw)
        digest = sha256(raw).hexdigest()
        assert verify_export_file(export, expected_sha256=digest, expected_rows=1)['row_count'] == 1
        assert json.loads(raw)['frozen_json'] == frozen
        proofs = list(iter_export_proofs(export, expected_sha256=digest, expected_rows=1))
        assert proofs == [{key: record[key] for key in
                           ('run_id', 'decision_id', 'version', 'frozen_sha256', 'state_sha256')}]
    run(scenario())
