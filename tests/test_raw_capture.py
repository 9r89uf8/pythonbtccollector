import asyncio
import inspect
import time
from collections import deque
from dataclasses import replace
from decimal import Decimal
from uuid import UUID

import pytest

import price_collector.raw_capture as capture


CONNECTION_1 = UUID("11111111-1111-1111-1111-111111111111")
CONNECTION_2 = UUID("22222222-2222-2222-2222-222222222222")


def trade_observation(
    *,
    connection_id=CONNECTION_1,
    received_ms=1_783_459_500_010,
    received_monotonic_ns=10,
    trade_time_ms=1_783_459_500_001,
    event_time_ms=1_783_459_500_002,
    price="100",
    agg_trade_id=1,
):
    return capture.FuturesTradeObservation(
        connection_id=connection_id,
        received_wall_ns=received_ms * 1_000_000,
        received_monotonic_ns=received_monotonic_ns,
        trade_time_ms=trade_time_ms,
        event_time_ms=event_time_ms,
        price=Decimal(price),
        agg_trade_id=agg_trade_id,
    )


def trace_record(*, bucket_start_ms=1_783_459_500_000, connection_id=CONNECTION_1):
    coalescer = capture.FuturesPriceTraceCoalescer()
    coalescer.add_trade(
        trade_observation(
            connection_id=connection_id,
            received_ms=bucket_start_ms + 1,
        )
    )
    return coalescer.finish()


def chainlink_record(*, receive_sequence=1, connection_id=CONNECTION_1):
    return capture.ChainlinkPriceEvent(
        received_wall_ns=1_783_459_500_010_000_000,
        received_monotonic_ns=100,
        connection_id=connection_id,
        receive_sequence=receive_sequence,
        provider_event_ms=1_783_459_500_001,
        provider_message_ms=1_783_459_500_002,
        price=Decimal("62001.123456789012345678"),
    )


def session_record(*, connection_id=CONNECTION_1):
    session = capture.FeedSession(
        source="binance_futures_agg_trade",
        connection_id=connection_id,
        connected_wall_ns=1_000,
        connected_monotonic_ns=100,
    )
    session.mark_ready(ready_wall_ns=1_010, ready_monotonic_ns=110)
    return session.opened_record()


class FakeBackend:
    def __init__(self, *, maintenance_results=None, fail_futures=False):
        self.maintenance_results = deque(maintenance_results or [True])
        self.fail_futures = fail_futures
        self.maintenance_calls = 0
        self.futures_calls = 0
        self.chainlink_calls = 0
        self.session_calls = 0
        self.futures_batches = []
        self.chainlink_batches = []
        self.session_batches = []
        self.closed = 0
        self.current_partition = "p1783459200000"
        self.raw_table_bytes = 0
        self.maintained = asyncio.Event()
        self.written = asyncio.Event()

    async def maintain(self):
        self.maintenance_calls += 1
        self.maintained.set()
        if len(self.maintenance_results) > 1:
            return self.maintenance_results.popleft()
        return self.maintenance_results[0]

    async def copy_futures_traces(self, records):
        self.futures_calls += 1
        self.futures_batches.append(list(records))
        self.written.set()
        if self.fail_futures:
            raise RuntimeError("copy failed")

    async def copy_chainlink_events(self, records):
        self.chainlink_calls += 1
        self.chainlink_batches.append(list(records))
        self.written.set()

    async def upsert_feed_sessions(self, records):
        self.session_calls += 1
        self.session_batches.append(list(records))
        self.written.set()

    async def close(self):
        self.closed += 1


async def wait_until(predicate, *, timeout_seconds=0.5):
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not reached before timeout")
        await asyncio.sleep(0.001)


def test_futures_coalescer_builds_receive_time_ohlc_and_flushes_boundary():
    counters = capture.CaptureCounters()
    coalescer = capture.FuturesPriceTraceCoalescer(counters=counters)

    observations = [
        trade_observation(
            received_ms=1_783_459_500_001,
            received_monotonic_ns=10,
            trade_time_ms=500,
            event_time_ms=600,
            price="100.00",
            agg_trade_id=10,
        ),
        trade_observation(
            received_ms=1_783_459_500_025,
            received_monotonic_ns=20,
            trade_time_ms=400,
            event_time_ms=700,
            price="105.00",
            agg_trade_id=12,
        ),
        trade_observation(
            received_ms=1_783_459_500_099,
            received_monotonic_ns=30,
            trade_time_ms=450,
            event_time_ms=650,
            price="95.00",
            agg_trade_id=11,
        ),
    ]
    for observation in observations:
        assert coalescer.add_trade(observation) is None

    completed = coalescer.add_trade(
        trade_observation(
            received_ms=1_783_459_500_100,
            received_monotonic_ns=40,
            price="102.00",
            agg_trade_id=13,
        )
    )

    assert completed.bucket_start_ms == 1_783_459_500_000
    assert completed.open_price == Decimal("100.00")
    assert completed.high_price == Decimal("105.00")
    assert completed.low_price == Decimal("95.00")
    assert completed.close_price == Decimal("95.00")
    assert completed.event_count == 3
    assert completed.first_trade_time_ms == 500
    assert completed.last_trade_time_ms == 450
    assert completed.first_event_time_ms == 600
    assert completed.last_event_time_ms == 650
    assert completed.first_agg_trade_id == 10
    assert completed.last_agg_trade_id == 11
    assert completed.first_received_wall_ns == observations[0].received_wall_ns
    assert completed.last_received_wall_ns == observations[-1].received_wall_ns
    assert isinstance(completed.open_price, Decimal)
    assert counters.records_coalesced_total == 2

    final = coalescer.finish()
    assert final.bucket_start_ms == 1_783_459_500_100
    assert final.close_price == Decimal("102.00")
    assert coalescer.finish() is None
    assert coalescer.pending_event_count == 0


def test_futures_coalescer_splits_connections_inside_same_bucket():
    coalescer = capture.FuturesPriceTraceCoalescer()
    assert coalescer.add_trade(trade_observation(connection_id=CONNECTION_1)) is None

    first = coalescer.add_trade(
        trade_observation(
            connection_id=CONNECTION_2,
            received_ms=1_783_459_500_011,
            price="101",
        )
    )
    second = coalescer.finish()

    assert first.connection_id == CONNECTION_1
    assert second.connection_id == CONNECTION_2
    assert first.bucket_start_ms == second.bucket_start_ms


def test_futures_coalescer_drops_wall_rollback_without_duplicate_bucket():
    counters = capture.CaptureCounters()
    coalescer = capture.FuturesPriceTraceCoalescer(counters=counters)
    current_ms = 1_783_459_500_110

    coalescer.add_trade(trade_observation(received_ms=current_ms, price="100"))
    assert (
        coalescer.add_trade(
            trade_observation(received_ms=current_ms - 100, price="99")
        )
        is None
    )
    assert coalescer.pending_event_count == 1
    assert counters.records_dropped_total == 1

    coalescer.add_trade(
        trade_observation(
            received_ms=current_ms + 1,
            received_monotonic_ns=11,
            price="101",
        )
    )
    first = coalescer.add_trade(
        trade_observation(
            received_ms=current_ms + 100,
            received_monotonic_ns=12,
            price="102",
        )
    )
    second = coalescer.finish()

    keys = [(row.connection_id, row.bucket_start_ms) for row in (first, second)]
    assert len(keys) == len(set(keys))
    assert first.event_count == 2


def test_futures_coalescer_drops_revisit_after_bucket_is_finished():
    counters = capture.CaptureCounters()
    coalescer = capture.FuturesPriceTraceCoalescer(counters=counters)
    current_ms = 1_783_459_500_110
    observation = trade_observation(received_ms=current_ms)

    coalescer.add_trade(observation)
    sealed = coalescer.finish()
    assert coalescer.add_trade(observation) is None
    assert (
        coalescer.add_trade(trade_observation(received_ms=current_ms - 100))
        is None
    )

    assert sealed.bucket_start_ms == 1_783_459_500_100
    assert coalescer.pending_event_count == 0
    assert counters.records_dropped_total == 2


def test_futures_coalescer_bounds_sealed_connection_history():
    coalescer = capture.FuturesPriceTraceCoalescer()
    for connection_number in range(capture.SEALED_CONNECTION_HISTORY_MAX + 1):
        coalescer.add_trade(
            trade_observation(
                connection_id=UUID(int=connection_number + 1),
                received_ms=1_783_459_500_001 + (connection_number * 100),
            )
        )
        coalescer.finish()

    assert coalescer.sealed_connection_count == capture.SEALED_CONNECTION_HISTORY_MAX


def test_idle_seal_uses_monotonic_fallback_during_wall_clock_rollback():
    coalescer = capture.FuturesPriceTraceCoalescer()
    coalescer.add_trade(
        trade_observation(
            received_ms=1_783_459_500_050,
            received_monotonic_ns=1_000_000_000,
        )
    )

    sealed = coalescer.finish_if_elapsed(
        now_wall_ns=1_783_459_499_000_000_000,
        now_monotonic_ns=1_100_000_000,
    )

    assert sealed.bucket_start_ms == 1_783_459_500_000
    assert coalescer.pending_event_count == 0


def test_trace_wall_timestamps_must_fall_inside_its_bucket():
    record = trace_record()

    with pytest.raises(ValueError, match="inside trace bucket"):
        replace(
            record,
            first_received_wall_ns=(record.bucket_start_ms - 1) * 1_000_000,
        )


def test_phase_one_coalescer_rejects_non_100ms_bucket_and_float_price():
    with pytest.raises(ValueError, match="100 ms"):
        capture.FuturesPriceTraceCoalescer(bucket_ms=50)

    with pytest.raises(TypeError, match="Decimal"):
        capture.FuturesTradeObservation(
            connection_id=CONNECTION_1,
            received_wall_ns=1_000_000,
            received_monotonic_ns=1,
            trade_time_ms=1,
            event_time_ms=1,
            price=100.0,
            agg_trade_id=1,
        )


def test_coalescer_idle_seal_and_monotonic_guard():
    counters = capture.CaptureCounters()
    coalescer = capture.FuturesPriceTraceCoalescer(counters=counters)
    observation = trade_observation(
        received_ms=1_783_459_500_001,
        received_monotonic_ns=20,
    )
    coalescer.add_trade(observation)

    assert (
        coalescer.finish_if_elapsed(now_wall_ns=1_783_459_500_099_999_999)
        is None
    )
    sealed = coalescer.finish_if_elapsed(
        now_wall_ns=1_783_459_500_100_000_000
    )
    assert sealed.event_count == 1

    assert coalescer.add_trade(observation) is None
    assert counters.records_dropped_total == 1

    pending = capture.FuturesPriceTraceCoalescer(counters=counters)
    pending.add_trade(observation)
    assert (
        pending.add_trade(
            trade_observation(
                received_ms=1_783_459_500_002,
                received_monotonic_ns=19,
            )
        )
        is None
    )
    assert pending.pending_event_count == 1
    assert counters.records_dropped_total == 2


def test_feed_session_sequences_messages_and_builds_open_and_closed_records():
    counters = capture.CaptureCounters()
    session = capture.FeedSession(
        source="polymarket_chainlink_rtds",
        connection_id=CONNECTION_1,
        connected_wall_ns=1_000,
        connected_monotonic_ns=100,
        counters=counters,
    )

    open_record = session.opened_record()
    assert open_record.ready_wall_ns is None
    assert open_record.disconnected_wall_ns is None

    session.mark_ready(ready_wall_ns=1_010, ready_monotonic_ns=110)
    first = session.next_receive_stamp(
        received_wall_ns=1_020,
        received_monotonic_ns=120,
    )
    second = session.next_receive_stamp(
        received_wall_ns=1_020,
        received_monotonic_ns=120,
    )
    session.mark_accepted()
    session.mark_parse_error()
    session.mark_record_dropped()
    closed = session.finish(
        close_reason="remote_close",
        disconnected_wall_ns=1_100,
        disconnected_monotonic_ns=200,
    )

    assert first.receive_sequence == 1
    assert first.received_ms == 0
    assert second.receive_sequence == 2
    assert first.received_wall_ns == second.received_wall_ns
    assert closed.ready_wall_ns == 1_010
    assert closed.close_reason == "remote_close"
    assert closed.messages_received_total == 2
    assert closed.messages_accepted_total == 1
    assert closed.parse_errors_total == 1
    assert closed.records_dropped_total == 1
    assert closed.last_receive_sequence == 2
    assert session.finish(close_reason="shutdown") is closed
    with pytest.raises(RuntimeError, match="closed"):
        session.mark_accepted()
    with pytest.raises(RuntimeError, match="closed"):
        session.opened_record()
    assert counters.messages_received_total == 2
    assert counters.messages_accepted_total == 1
    assert counters.parse_errors_total == 1
    assert not hasattr(closed, "__dict__")


def test_chainlink_same_nanosecond_events_remain_distinct_by_sequence():
    first = chainlink_record(receive_sequence=1)
    second = chainlink_record(receive_sequence=2)

    assert first.received_wall_ns == second.received_wall_ns
    assert first.receive_sequence == 1
    assert second.receive_sequence == 2
    assert first != second
    assert isinstance(first.price, Decimal)


def test_records_and_sessions_reject_invalid_copy_state():
    with pytest.raises(ValueError, match="source"):
        capture.FeedSession(
            source="unknown",
            connected_wall_ns=1,
            connected_monotonic_ns=1,
        )
    with pytest.raises(ValueError, match="finite and positive"):
        capture.ChainlinkPriceEvent(
            received_wall_ns=1,
            received_monotonic_ns=1,
            connection_id=CONNECTION_1,
            receive_sequence=1,
            provider_event_ms=1,
            provider_message_ms=None,
            price=Decimal("NaN"),
        )

    session = capture.FeedSession(
        source="polymarket_chainlink_rtds",
        connected_wall_ns=10,
        connected_monotonic_ns=10,
    )
    with pytest.raises(ValueError, match="exceed received"):
        session.mark_accepted()
    with pytest.raises(ValueError, match="precedes connection"):
        session.mark_ready(ready_wall_ns=11, ready_monotonic_ns=9)


def test_copy_records_enforce_postgresql_numeric_and_integer_bounds():
    valid_price = Decimal("99999999999999999999.999999999999999999")
    assert replace(chainlink_record(), price=valid_price).price == valid_price
    assert (
        replace(
            chainlink_record(),
            price=Decimal("1.0000000000000000000"),
        ).price
        == Decimal("1.0000000000000000000")
    )

    with pytest.raises(ValueError, match=r"NUMERIC\(38,18\)"):
        replace(chainlink_record(), price=Decimal("1e20"))
    with pytest.raises(ValueError, match="scale"):
        replace(
            chainlink_record(),
            price=Decimal("1.0000000000000000001"),
        )
    with pytest.raises(ValueError, match="BIGINT"):
        replace(
            chainlink_record(),
            provider_event_ms=capture.POSTGRES_BIGINT_MAX + 1,
        )
    with pytest.raises(ValueError, match="INTEGER"):
        replace(trace_record(), event_count=capture.POSTGRES_INTEGER_MAX + 1)


def test_ready_timestamp_is_single_assignment_and_zero_storage_bytes_is_valid():
    session = capture.FeedSession(
        source="polymarket_chainlink_rtds",
        connected_wall_ns=10,
        connected_monotonic_ns=10,
    )
    session.mark_ready(ready_wall_ns=11, ready_monotonic_ns=11)
    with pytest.raises(RuntimeError, match="already ready"):
        session.mark_ready(ready_wall_ns=12, ready_monotonic_ns=12)

    counters = capture.CaptureCounters()
    counters.record_storage_state(current_partition="p0", raw_table_bytes=0)
    snapshot = counters.snapshot(queue_depth=0)
    assert snapshot.raw_table_bytes == 0


def test_drop_oldest_buffer_preserves_newest_and_tracks_high_water():
    counters = capture.CaptureCounters()
    buffer = capture.DropOldestCaptureBuffer(max_events=2, counters=counters)

    assert buffer.offer_nowait("first").dropped_oldest is False
    assert buffer.offer_nowait("second").queue_high_water == 2
    result = buffer.offer_nowait("third")

    assert result.accepted is True
    assert result.dropped_oldest is True
    assert result.queue_depth == 2
    assert buffer.qsize() == 2
    assert buffer.drain_nowait(10) == ["second", "third"]
    assert counters.records_enqueued_total == 3
    assert counters.records_dropped_total == 1
    assert counters.queue_high_water == 2


def test_buffer_drop_warning_hook_is_rate_limited(monkeypatch):
    warnings = []
    clock_values = iter([1_000_000_000, 1_500_000_000, 3_100_000_000])
    monkeypatch.setattr(capture.time, "monotonic_ns", lambda: next(clock_values))
    buffer = capture.DropOldestCaptureBuffer(
        max_events=1,
        drop_warning_interval_seconds=2,
        drop_warning_hook=lambda depth, maximum: warnings.append((depth, maximum)),
    )

    buffer.offer_nowait("one")
    buffer.offer_nowait("two")
    buffer.offer_nowait("three")
    buffer.offer_nowait("four")

    assert warnings == [(1, 1), (1, 1)]


def test_reentrant_drop_warning_hook_cannot_exceed_buffer_bound():
    warnings = []
    buffer = None

    def warning_hook(depth, maximum):
        warnings.append((depth, maximum))
        buffer.offer_nowait("from-hook")

    buffer = capture.DropOldestCaptureBuffer(
        max_events=1,
        drop_warning_hook=warning_hook,
    )
    buffer.offer_nowait("first")
    result = buffer.offer_nowait("outer")

    assert result.dropped_record == "first"
    assert buffer.qsize() == 1
    assert buffer.drain_nowait(1) == ["from-hook"]
    assert warnings == [(1, 1)]


def test_buffer_eviction_exposes_dropped_record_connection():
    buffer = capture.DropOldestCaptureBuffer(max_events=1)
    first = trace_record(connection_id=CONNECTION_1)
    second = trace_record(connection_id=CONNECTION_2)

    buffer.offer_nowait(first)
    result = buffer.offer_nowait(second)

    assert result.dropped_oldest is True
    assert result.dropped_record is first
    assert result.dropped_record.connection_id == CONNECTION_1


def test_runtime_full_batch_groups_records_and_constructs_backend_in_background():
    async def scenario():
        backend = FakeBackend()
        factory_calls = 0

        def backend_factory():
            nonlocal factory_calls
            factory_calls += 1
            return backend

        runtime = capture.create_raw_capture_runtime(
            futures_enabled=True,
            chainlink_enabled=True,
            backend_factory=backend_factory,
            queue_max_events=10,
            batch_max_rows=3,
            flush_ms=1_000,
            maintenance_interval_seconds=60,
        )
        assert runtime is not None
        assert factory_calls == 0
        runtime.offer_nowait(trace_record())
        runtime.offer_nowait(chainlink_record())
        runtime.offer_nowait(session_record())
        assert factory_calls == 0

        runtime.start()
        await asyncio.wait_for(backend.written.wait(), timeout=0.5)
        await wait_until(lambda: runtime.counters.records_persisted_total == 3)
        await runtime.close()

        assert factory_calls == 1
        assert backend.maintenance_calls >= 1
        assert backend.futures_calls == 1
        assert backend.chainlink_calls == 1
        assert backend.session_calls == 1
        assert backend.futures_batches == [[trace_record()]]
        assert backend.chainlink_batches == [[chainlink_record()]]
        assert backend.session_batches == [[session_record()]]
        assert runtime.counters.last_batch_rows == 3
        assert runtime.counters.batches_failed_total == 0
        assert backend.closed == 1

    asyncio.run(scenario())


def test_runtime_partial_batch_waits_for_flush_deadline():
    async def scenario():
        backend = FakeBackend()
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=10,
            batch_max_rows=3,
            flush_ms=40,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=500,
        )
        runtime.start()
        await asyncio.wait_for(backend.maintained.wait(), timeout=0.5)
        runtime.offer_nowait(trace_record())
        await asyncio.sleep(0)
        assert backend.futures_calls == 0

        await asyncio.wait_for(backend.written.wait(), timeout=0.5)
        await wait_until(lambda: runtime.counters.records_persisted_total == 1)
        await runtime.close()

        assert backend.futures_calls == 1
        assert runtime.counters.last_batch_rows == 1

    asyncio.run(scenario())


def test_runtime_discards_failed_copy_once_without_retrying_batch():
    async def scenario():
        backend = FakeBackend(fail_futures=True)
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=10,
            batch_max_rows=2,
            flush_ms=1_000,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=500,
        )
        runtime.offer_nowait(trace_record(bucket_start_ms=1_783_459_500_000))
        runtime.offer_nowait(trace_record(bucket_start_ms=1_783_459_500_100))
        runtime.start()

        await asyncio.wait_for(backend.written.wait(), timeout=0.5)
        await wait_until(lambda: runtime.counters.batches_failed_total == 1)
        await runtime.close()

        assert backend.futures_calls == 1
        assert runtime.counters.records_persisted_total == 0
        assert runtime.counters.records_dropped_total == 2
        assert runtime.counters.last_batch_rows == 2

    asyncio.run(scenario())


def test_mixed_batch_cancellation_preserves_completed_group_accounting():
    async def scenario():
        class MixedBackend(FakeBackend):
            def __init__(self):
                super().__init__()
                self.chainlink_started = asyncio.Event()

            async def copy_chainlink_events(self, records):
                self.chainlink_calls += 1
                self.chainlink_started.set()
                await asyncio.Event().wait()

        backend = MixedBackend()
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=2,
            batch_max_rows=2,
            flush_ms=10,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=20,
        )
        runtime.offer_nowait(trace_record())
        runtime.offer_nowait(chainlink_record())
        runtime.start()
        await asyncio.wait_for(backend.chainlink_started.wait(), timeout=0.5)

        await runtime.close()
        await wait_until(lambda: runtime.counters.batches_failed_total == 1)

        assert backend.futures_calls == 1
        assert backend.chainlink_calls == 1
        assert runtime.counters.records_persisted_total == 1
        assert runtime.counters.records_dropped_total == 1
        assert runtime.counters.last_batch_rows == 2

    asyncio.run(scenario())


def test_cancellation_resistant_backend_can_finish_without_double_accounting():
    async def scenario():
        class ResistantBackend(FakeBackend):
            def __init__(self):
                super().__init__()
                self.copy_started = asyncio.Event()
                self.release = asyncio.Event()
                self.cancellations = 0

            async def copy_futures_traces(self, records):
                self.futures_calls += 1
                self.copy_started.set()
                while not self.release.is_set():
                    try:
                        await self.release.wait()
                    except asyncio.CancelledError:
                        self.cancellations += 1
                self.futures_batches.append(list(records))

        backend = ResistantBackend()
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=2,
            batch_max_rows=1,
            flush_ms=10,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=10,
        )
        runtime.offer_nowait(trace_record())
        runtime.start()
        await asyncio.wait_for(backend.copy_started.wait(), timeout=0.5)
        runtime.offer_nowait(
            trace_record(bucket_start_ms=1_783_459_500_100)
        )

        await runtime.close()
        assert backend.cancellations == 1
        assert runtime.counters.records_persisted_total == 0
        assert runtime.counters.records_dropped_total == 1

        backend.release.set()
        await wait_until(lambda: runtime.counters.records_persisted_total == 1)
        await runtime.close()

        assert runtime.counters.records_persisted_total == 1
        assert runtime.counters.records_dropped_total == 1

    asyncio.run(scenario())


def test_repeated_close_retries_cancellation_of_resistant_backend():
    async def scenario():
        class TwiceResistantBackend(FakeBackend):
            def __init__(self):
                super().__init__()
                self.copy_started = asyncio.Event()
                self.cancellations = 0

            async def copy_futures_traces(self, records):
                self.copy_started.set()
                while True:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        self.cancellations += 1
                        if self.cancellations >= 2:
                            raise

        backend = TwiceResistantBackend()
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=1,
            batch_max_rows=1,
            flush_ms=10,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=10,
        )
        runtime.offer_nowait(trace_record())
        runtime.start()
        await asyncio.wait_for(backend.copy_started.wait(), timeout=0.5)

        await runtime.close()
        assert backend.cancellations == 1
        await runtime.close()
        await wait_until(lambda: runtime.counters.batches_failed_total == 1)

        assert backend.cancellations == 2
        assert runtime.counters.records_persisted_total == 0
        assert runtime.counters.records_dropped_total == 1

    asyncio.run(scenario())


def test_cancellation_during_post_take_maintenance_accounts_drained_batch():
    async def scenario():
        class BlockingMaintenanceBackend(FakeBackend):
            def __init__(self):
                super().__init__()
                self.second_maintenance_started = asyncio.Event()

            async def maintain(self):
                self.maintenance_calls += 1
                self.maintained.set()
                if self.maintenance_calls == 1:
                    return True
                self.second_maintenance_started.set()
                await asyncio.Event().wait()

        backend = BlockingMaintenanceBackend()
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=2,
            batch_max_rows=2,
            flush_ms=20,
            maintenance_interval_seconds=0.01,
            shutdown_timeout_ms=20,
        )
        runtime.start()
        await asyncio.wait_for(backend.maintained.wait(), timeout=0.5)
        runtime.offer_nowait(trace_record())
        await asyncio.wait_for(
            backend.second_maintenance_started.wait(),
            timeout=0.5,
        )

        await runtime.close()

        assert runtime.counters.records_persisted_total == 0
        assert runtime.counters.records_dropped_total == 1

    asyncio.run(scenario())


def test_maintenance_false_suspends_offers_then_idle_run_reenables_them():
    async def scenario():
        backend = FakeBackend(maintenance_results=[False, True])
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=10,
            batch_max_rows=1,
            flush_ms=10,
            maintenance_interval_seconds=0.02,
            shutdown_timeout_ms=500,
        )
        runtime.start()
        await wait_until(lambda: backend.maintenance_calls >= 1)
        assert runtime.storage_permitted is False

        rejected = runtime.offer_nowait(trace_record())
        assert rejected.accepted is False
        assert runtime.counters.records_dropped_total == 1

        await wait_until(lambda: backend.maintenance_calls >= 2)
        assert runtime.storage_permitted is True
        accepted = runtime.offer_nowait(trace_record())
        assert accepted.accepted is True
        await asyncio.wait_for(backend.written.wait(), timeout=0.5)
        await wait_until(lambda: runtime.counters.records_persisted_total == 1)
        await runtime.close()

        assert runtime.counters.maintenance_runs_total >= 2
        assert runtime.counters.maintenance_failures_total == 0

    asyncio.run(scenario())


def test_suspended_runtime_retries_maintenance_on_short_cadence(monkeypatch):
    async def scenario():
        backend = FakeBackend(maintenance_results=[False, False, True])
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=1,
            batch_max_rows=1,
            flush_ms=10,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=500,
        )
        runtime.start()
        await wait_until(lambda: backend.maintenance_calls >= 3)

        assert runtime.storage_permitted is True
        snapshot = runtime.counters.snapshot(queue_depth=runtime.buffer.qsize())
        assert snapshot.current_partition == "p1783459200000"
        assert snapshot.raw_table_bytes == 0
        await runtime.close()

    monkeypatch.setattr(capture, "SUSPENDED_MAINTENANCE_RETRY_SECONDS", 0.01)
    asyncio.run(scenario())


def test_disabled_factory_returns_none_without_touching_backend_factory():
    factory_calls = 0

    def backend_factory():
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("disabled capture must not construct a backend")

    runtime = capture.create_raw_capture_runtime(
        futures_enabled=False,
        chainlink_enabled=False,
        backend_factory=backend_factory,
    )

    assert runtime is None
    assert factory_calls == 0


def test_buffer_and_runtime_construct_outside_loop_after_asyncio_run():
    asyncio.run(asyncio.sleep(0))

    buffer = capture.DropOldestCaptureBuffer(max_events=1)
    runtime = capture.RawCaptureRuntime(
        backend_factory=lambda: None,
        queue_max_events=1,
        batch_max_rows=1,
        flush_ms=1,
        maintenance_interval_seconds=60,
        shutdown_timeout_ms=1,
    )

    assert buffer.offer_nowait("value").accepted is True
    assert runtime.started is False


def test_factory_maintenance_default_is_sixty_seconds():
    parameter = inspect.signature(
        capture.create_raw_capture_runtime
    ).parameters["maintenance_interval_seconds"]

    assert parameter.default == 60


def test_runtime_rejects_non_records_and_batch_larger_than_queue():
    with pytest.raises(ValueError, match="cannot exceed"):
        capture.RawCaptureRuntime(
            backend_factory=lambda: None,
            queue_max_events=1,
            batch_max_rows=2,
            flush_ms=1,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=1,
        )

    runtime = capture.RawCaptureRuntime(
        backend_factory=lambda: None,
        queue_max_events=1,
        batch_max_rows=1,
        flush_ms=1,
        maintenance_interval_seconds=60,
        shutdown_timeout_ms=1,
    )
    with pytest.raises(TypeError, match="CaptureRecord"):
        runtime.offer_nowait(object())


def test_runtime_shutdown_is_bounded_when_backend_copy_hangs():
    async def scenario():
        class HangingBackend(FakeBackend):
            def __init__(self):
                super().__init__()
                self.copy_started = asyncio.Event()

            async def copy_futures_traces(self, records):
                self.futures_calls += 1
                self.copy_started.set()
                await asyncio.Event().wait()

        backend = HangingBackend()
        runtime = capture.RawCaptureRuntime(
            backend_factory=lambda: backend,
            queue_max_events=2,
            batch_max_rows=1,
            flush_ms=10,
            maintenance_interval_seconds=60,
            shutdown_timeout_ms=20,
        )
        runtime.offer_nowait(trace_record())
        runtime.start()
        await asyncio.wait_for(backend.copy_started.wait(), timeout=0.5)

        started = time.monotonic()
        await runtime.close()
        elapsed = time.monotonic() - started

        assert elapsed < 0.2
        assert runtime.counters.records_dropped_total >= 1

    asyncio.run(scenario())
