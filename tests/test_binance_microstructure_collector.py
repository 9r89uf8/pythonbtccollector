import asyncio
import heapq
import re
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import price_collector.binance_microstructure_collector as runtime
from price_collector.binance_futures_streams import (
    parse_binance_futures_agg_trade_payload,
)


ROOT = Path(__file__).resolve().parents[1]
from price_collector.binance_microstructure import (
    CollectorState,
    MicrostructureEventSink,
    QueuedEvent,
    finalize_boundary,
)


def agg_trade_payload(**overrides):
    payload = {
        "e": "aggTrade",
        "E": 1_783_459_501_990,
        "a": 10,
        "s": "BTCUSDT",
        "p": "100.00",
        "q": "2.000",
        "f": 100,
        "l": 101,
        "T": 1_783_459_501_980,
        "m": False,
    }
    payload.update(overrides)
    return payload


class RecordingSink:
    def __init__(self):
        self.calls = []

    def offer_spot_trade(self, trade, received_ms):
        self.calls.append(("spot_trade", trade, received_ms))
        return True

    def offer_spot_depth(self, depth, received_ms):
        self.calls.append(("spot_depth", depth, received_ms))
        return True

    def offer_futures_depth(self, depth, received_ms):
        self.calls.append(("futures_depth", depth, received_ms))
        return True

    def offer_liquidation(self, liquidation, received_ms):
        self.calls.append(("liquidation", liquidation, received_ms))
        return True


def test_unwraps_combined_and_raw_frames_without_changing_payload():
    data = {"e": "aggTrade"}
    assert runtime.unwrap_stream_payload(
        {"stream": "btcusdt@aggTrade", "data": data}
    ) == ("btcusdt@aggTrade", data)
    assert runtime.unwrap_stream_payload(
        data,
        raw_stream="btcusdt@forceOrder",
    ) == ("btcusdt@forceOrder", data)

    with pytest.raises(runtime.MicrostructureMessageError, match="missing a string"):
        runtime.unwrap_stream_payload({"data": data})


def test_spot_dispatch_requires_exact_lowercase_stream_and_keeps_decimal():
    sink = RecordingSink()
    runtime.dispatch_spot_message(
        {
            "stream": "btcusdt@aggTrade",
            "data": agg_trade_payload(),
        },
        sink=sink,
        expected_symbol="BTCUSDT",
        received_ms=1_783_459_501_999,
    )

    kind, trade, received_ms = sink.calls[0]
    assert kind == "spot_trade"
    assert trade.price == Decimal("100.00")
    assert not isinstance(trade.price, float)
    assert received_ms == 1_783_459_501_999

    with pytest.raises(runtime.MicrostructureMessageError, match="unexpected spot"):
        runtime.dispatch_spot_message(
            {
                "stream": "BTCUSDT@aggTrade",
                "data": agg_trade_payload(),
            },
            sink=sink,
            expected_symbol="BTCUSDT",
            received_ms=1_783_459_502_000,
        )


def test_raw_futures_depth_and_liquidation_dispatch_to_distinct_sinks(monkeypatch):
    sink = RecordingSink()
    parsed_depth = object()
    parsed_liquidation = object()
    monkeypatch.setattr(
        runtime,
        "parse_futures_depth_payload",
        lambda payload, *, expected_symbol: parsed_depth,
    )
    monkeypatch.setattr(
        runtime,
        "parse_liquidation_payload",
        lambda payload, *, expected_symbol: parsed_liquidation,
    )

    runtime.dispatch_futures_depth_message(
        {"e": "depthUpdate", "s": "BTCUSDT"},
        sink=sink,
        expected_symbol="BTCUSDT",
        received_ms=2_001,
    )
    runtime.dispatch_liquidation_message(
        {"e": "forceOrder", "o": {"s": "BTCUSDT"}},
        sink=sink,
        expected_symbol="BTCUSDT",
        received_ms=2_002,
    )

    assert sink.calls == [
        ("futures_depth", parsed_depth, 2_001),
        ("liquidation", parsed_liquidation, 2_002),
    ]

    with pytest.raises(
        runtime.MicrostructureMessageError,
        match="unexpected futures depth",
    ):
        runtime.dispatch_futures_depth_message(
            {
                "stream": "btcusdt@depth10@100ms",
                "data": {"e": "depthUpdate", "s": "BTCUSDT"},
            },
            sink=sink,
            expected_symbol="BTCUSDT",
            received_ms=2_003,
        )


def test_reader_helpers_use_the_configured_routing_and_dispatch(monkeypatch):
    calls = []

    async def fake_reader(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(runtime, "_websocket_reader_loop", fake_reader)
    settings = SimpleNamespace(
        BINANCE_FUTURES_SYMBOL="BTCUSDT",
        BINANCE_MICROSTRUCTURE_SPOT_WS_URL="spot-url",
        BINANCE_MICROSTRUCTURE_FUTURES_DEPTH_WS_URL="public-url",
        BINANCE_MICROSTRUCTURE_FUTURES_LIQUIDATION_WS_URL="market-url",
    )
    sink = RecordingSink()

    async def exercise():
        await asyncio.gather(
            runtime.spot_microstructure_reader_loop(settings, sink),
            runtime.futures_depth_microstructure_reader_loop(settings, sink),
            runtime.futures_liquidation_microstructure_reader_loop(settings, sink),
        )

    asyncio.run(exercise())

    assert [(call["source"], call["url"], call["dispatch"]) for call in calls] == [
        (runtime.SPOT_SOURCE, "spot-url", runtime.dispatch_spot_message),
        (
            runtime.FUTURES_DEPTH_SOURCE,
            "public-url",
            runtime.dispatch_futures_depth_message,
        ),
        (
            runtime.FUTURES_LIQUIDATION_SOURCE,
            "market-url",
            runtime.dispatch_liquidation_message,
        ),
    ]


class FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.connection.transaction_entered = True
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.connection.transaction_exited = True
        return False


class FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeConnection:
    def __init__(self):
        self.calls = []
        self.transaction_entered = False
        self.transaction_exited = False

    def transaction(self):
        return FakeTransaction(self)

    async def execute(self, query, *args):
        self.calls.append((query, args))
        return "INSERT 0 1"


class FakePool:
    def __init__(self):
        self.connection = FakeConnection()

    def acquire(self):
        return FakeAcquire(self.connection)


def test_upsert_ensures_market_window_atomically_and_latest_receipt_wins():
    pool = FakePool()
    marker = Decimal("62000.123456789012345678")
    row = {column: None for column in runtime.MICROSTRUCTURE_VALUE_COLUMNS}
    row.update(
        {
            "sample_second_ms": 1_783_459_500_000,
            "spot_mid": marker,
        }
    )

    asyncio.run(
        runtime.upsert_binance_microstructure_1s(
            pool,
            symbol="BTCUSDT",
            row=row,
            received_ms=1_783_459_501_250,
        )
    )

    connection = pool.connection
    assert connection.transaction_entered is True
    assert connection.transaction_exited is True
    assert len(connection.calls) == 2
    market_query, market_args = connection.calls[0]
    upsert_query, upsert_args = connection.calls[1]
    assert "INSERT INTO market_windows" in market_query
    assert market_args[:3] == (
        5_944_865,
        1_783_459_500_000,
        1_783_459_800_000,
    )
    assert "ON CONFLICT (symbol, sample_second_ms)" in upsert_query
    assert "<= EXCLUDED.received_ms" in " ".join(upsert_query.split())
    marker_index = 4 + runtime.MICROSTRUCTURE_VALUE_COLUMNS.index("spot_mid")
    assert upsert_args[marker_index] is marker
    assert upsert_args[-1] == 1_783_459_501_250


def test_runtime_insert_columns_match_postgresql_schema_in_order():
    schema = (ROOT / "schema.sql").read_text()
    block = schema.split(
        "CREATE TABLE IF NOT EXISTS binance_microstructure_1s (", 1
    )[1].split("\n);", 1)[0]
    schema_columns = []
    for line in block.splitlines():
        match = re.match(r"\s*([a-z][a-z0-9_]*)\s+", line)
        if match and match.group(1) not in {"primary", "check"}:
            schema_columns.append(match.group(1))

    assert schema_columns == [
        "symbol",
        "market_id",
        "sample_second_ms",
        "sample_second_at",
        *runtime.MICROSTRUCTURE_VALUE_COLUMNS,
        "received_ms",
        "created_at",
    ]


def test_write_gate_pauses_at_cap_and_resumes_only_below_warning():
    gate = runtime.MicrostructureWriteGate(
        warning_bytes=100,
        maximum_bytes=150,
    )

    assert gate.observe(149) == "warning"
    assert gate.paused is False
    assert gate.observe(150) == "paused"
    assert gate.paused is True
    assert gate.observe(120) == "warning"
    assert gate.paused is True
    assert gate.observe(99) == "resumed"
    assert gate.paused is False


def test_retention_delete_uses_symbol_leading_primary_key():
    pool = FakePool()
    asyncio.run(
        runtime.delete_expired_microstructure_rows(
            pool,
            symbol="BTCUSDT",
            now_ms=10 * runtime.MILLISECONDS_PER_DAY,
            retention_days=3,
        )
    )

    query, args = pool.connection.calls[0]
    assert "WHERE symbol = $1" in query
    assert "sample_second_ms < $2" in query
    assert args == ("BTCUSDT", 7 * runtime.MILLISECONDS_PER_DAY)


def test_microstructure_live_cache_write_is_decimal_safe_and_nonfatal(caplog):
    row = {
        "sample_second_ms": 1_783_459_500_000,
        "spot_mid": Decimal("62000.123456789012345678"),
    }
    calls = []

    class RecordingLiveCache:
        async def set_microstructure_snapshot(self, key, **kwargs):
            calls.append((key, kwargs))

    assert asyncio.run(
        runtime.update_microstructure_live_cache(
            RecordingLiveCache(),
            row=row,
            received_ms=1_783_459_501_250,
        )
    )
    assert calls == [
        (
            runtime.MICROSTRUCTURE_LIVE_KEY,
            {
                "row": row,
                "received_ms": 1_783_459_501_250,
            },
        )
    ]

    class FailingLiveCache:
        async def set_microstructure_snapshot(self, key, **kwargs):
            raise OSError("redis unavailable")

    with caplog.at_level("ERROR"):
        assert not asyncio.run(
            runtime.update_microstructure_live_cache(
                FailingLiveCache(),
                row=row,
                received_ms=1_783_459_501_250,
            )
        )
    assert "microstructure_live_cache_write_failed" in caplog.text


def test_finalize_boundary_holds_event_received_at_boundary_for_next_row():
    first_trade = parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=1, f=1, l=1, p="100", q="1"),
        expected_symbol="BTCUSDT",
    )
    boundary_trade = parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=2, f=2, l=2, p="101", q="2"),
        expected_symbol="BTCUSDT",
    )

    async def queued_events():
        events = asyncio.Queue()
        sink = MicrostructureEventSink(events)
        assert sink.offer_spot_trade(first_trade, 1_999) is True
        assert sink.offer_spot_trade(boundary_trade, 2_000) is True
        return [events.get_nowait(), events.get_nowait()]

    pending = asyncio.run(queued_events())
    heapq.heapify(pending)
    state = CollectorState(symbol="BTCUSDT")

    first = finalize_boundary(
        state,
        pending,
        2_000,
        sample_jitter_ms=250,
    )
    assert first["sample_second_ms"] == 1_000
    assert first["spot_buy_usdt"] == Decimal("100")
    assert len(pending) == 1

    second = finalize_boundary(
        state,
        pending,
        3_000,
        sample_jitter_ms=250,
    )
    assert second["sample_second_ms"] == 2_000
    assert second["spot_buy_usdt"] == Decimal("202")
    assert pending == []


def test_reader_parse_rejection_enqueues_causal_error(monkeypatch):
    class InvalidThenIdleWebSocket:
        def __init__(self):
            self.calls = 0

        async def recv(self):
            self.calls += 1
            if self.calls == 1:
                return "not-json"
            await asyncio.Future()

    class WebSocketContext:
        async def __aenter__(self):
            return InvalidThenIdleWebSocket()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def scenario():
        events = asyncio.Queue()
        sink = MicrostructureEventSink(events)
        monkeypatch.setattr(
            runtime.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(),
        )
        task = asyncio.create_task(
            runtime._websocket_reader_loop(
                name="spot",
                source=runtime.SPOT_SOURCE,
                url="wss://example.test/spot",
                sink=sink,
                dispatch=runtime.dispatch_spot_message,
                expected_symbol="BTCUSDT",
            )
        )
        opened = await asyncio.wait_for(events.get(), timeout=1)
        error = await asyncio.wait_for(events.get(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert opened.kind == "connection_opened"
        assert error.kind == "error"
        assert error.payload == runtime.SPOT_SOURCE
        assert error.received_ms > 0

    asyncio.run(scenario())


def test_runtime_rebase_discards_queue_and_unfinished_interval(monkeypatch):
    async def scenario():
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=10,
            BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=5,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
        )
        collector_runtime = runtime.MicrostructureRuntime(settings, object())
        collector_runtime.state.spot_trades.observe(
            price=Decimal("100"),
            quantity=Decimal("1"),
            buyer_is_maker=False,
            actual_trades=1,
            received_ms=1_900,
            event_time_ms=1_850,
        )
        collector_runtime.state.spot_book.bbo_ofi_quantity = Decimal("4")
        collector_runtime.state.spot_book.snapshot_count = 3
        collector_runtime.state.connections.update(
            {
                "spot": True,
                "futures_trade": True,
                "futures_depth": True,
                "futures_liquidation": True,
            }
        )
        collector_runtime.events.put_nowait(QueuedEvent(1_950, 0, "error", "old"))
        finalized = runtime.FinalizedMicrostructureRow(
            row={"sample_second_ms": 1_000},
            received_ms=2_250,
        )
        collector_runtime.persistence_rows.put_nowait(finalized)
        monkeypatch.setattr(runtime, "current_utc_epoch_ms", lambda: 2_000)

        reset_ms, discarded = collector_runtime._rebase_after_worker_failure()

        assert (reset_ms, discarded) == (2_000, 1)
        assert collector_runtime.events.empty()
        assert collector_runtime.persistence_rows.get_nowait() is finalized
        assert collector_runtime.state.spot_trades.buy_quote == Decimal("0")
        assert collector_runtime.state.spot_trades.last_received_ms == 1_900
        assert collector_runtime.state.spot_book.bbo_ofi_quantity == Decimal("0")
        assert collector_runtime.state.spot_book.snapshot_count == 0
        assert collector_runtime.state.connections["futures_trade"] is True
        assert collector_runtime.state.connections["spot"] is False
        assert collector_runtime.state.connections["futures_depth"] is False
        assert collector_runtime.state.connections["futures_liquidation"] is False
        assert collector_runtime.state.connection_errors == 1
        assert collector_runtime.state.unhealthy_until_ms == 7_000

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "event_kind,initial_state,expected_state",
    [
        ("connection_opened", False, True),
        ("connection_closed", True, False),
    ],
)
def test_runtime_rebase_replays_latest_core_connection_transition(
    monkeypatch,
    event_kind,
    initial_state,
    expected_state,
):
    async def scenario():
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=10,
            BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=5,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
        )
        collector_runtime = runtime.MicrostructureRuntime(settings, object())
        collector_runtime.state.connections["futures_trade"] = initial_state
        collector_runtime.events.put_nowait(
            QueuedEvent(1_900, 0, event_kind, "futures_trade")
        )
        monkeypatch.setattr(runtime, "current_utc_epoch_ms", lambda: 2_000)

        reset_ms, discarded = collector_runtime._rebase_after_worker_failure()

        assert (reset_ms, discarded) == (2_000, 1)
        assert (
            collector_runtime.state.connections["futures_trade"]
            is expected_state
        )
        assert collector_runtime.state.unhealthy_until_ms == 7_000

    asyncio.run(scenario())


def test_slow_postgres_does_not_delay_later_redis_seconds(monkeypatch):
    async def scenario():
        clock = {"now_ms": 1_950}
        events = asyncio.Queue()
        persistence_rows = asyncio.Queue(maxsize=10)
        state = CollectorState(symbol="BTCUSDT")
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS=250,
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        published_samples = []
        three_published = asyncio.Event()
        postgres_started = asyncio.Event()

        class RecordingLiveCache:
            async def set_microstructure_snapshot(self, key, **kwargs):
                assert key == runtime.MICROSTRUCTURE_LIVE_KEY
                published_samples.append(
                    kwargs["row"]["sample_second_ms"]
                )
                if len(published_samples) == 3:
                    three_published.set()

        async def fake_retention(*args, **kwargs):
            return "DELETE 0"

        async def fake_relation_size(pool):
            return 0

        async def blocked_upsert(pool, *, symbol, row, received_ms):
            del pool, symbol, row, received_ms
            postgres_started.set()
            await asyncio.Future()

        events.put_nowait(QueuedEvent(1_999, 0, "error", "first"))
        monkeypatch.setattr(
            runtime,
            "current_utc_epoch_ms",
            lambda: clock["now_ms"],
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            blocked_upsert,
        )
        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )

        aggregate_task = asyncio.create_task(
            runtime.microstructure_aggregate_loop(
                settings=settings,
                state=state,
                events=events,
                persistence_rows=persistence_rows,
                live_cache=RecordingLiveCache(),
            )
        )
        persistence_task = asyncio.create_task(
            runtime.microstructure_persistence_loop(
                pool=object(),
                settings=settings,
                persistence_rows=persistence_rows,
            )
        )
        await asyncio.sleep(0.01)

        clock["now_ms"] = 2_250
        events.put_nowait(QueuedEvent(2_000, 1, "error", "second"))
        await asyncio.wait_for(postgres_started.wait(), timeout=1)

        clock["now_ms"] = 3_250
        events.put_nowait(QueuedEvent(3_000, 2, "error", "third"))
        await asyncio.sleep(0)
        clock["now_ms"] = 4_250
        events.put_nowait(QueuedEvent(4_000, 3, "error", "fourth"))
        await asyncio.wait_for(three_published.wait(), timeout=1)

        assert published_samples == [1_000, 2_000, 3_000]
        assert persistence_rows.qsize() == 2

        aggregate_task.cancel()
        persistence_task.cancel()
        results = await asyncio.gather(
            aggregate_task,
            persistence_task,
            return_exceptions=True,
        )
        assert all(
            isinstance(result, asyncio.CancelledError)
            for result in results
        )

    asyncio.run(scenario())


def test_redis_failure_still_enqueues_and_persists_row(monkeypatch):
    async def scenario():
        clock = {"now_ms": 1_950}
        events = asyncio.Queue()
        persistence_rows = asyncio.Queue(maxsize=10)
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS=250,
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        persisted = asyncio.Event()
        persisted_rows = []

        class FailingLiveCache:
            async def set_microstructure_snapshot(self, key, **kwargs):
                raise OSError("redis unavailable")

        async def fake_retention(*args, **kwargs):
            return "DELETE 0"

        async def fake_relation_size(pool):
            return 0

        async def fake_upsert(pool, *, symbol, row, received_ms):
            persisted_rows.append((row["sample_second_ms"], received_ms))
            persisted.set()

        monkeypatch.setattr(
            runtime,
            "current_utc_epoch_ms",
            lambda: clock["now_ms"],
        )
        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            fake_upsert,
        )

        aggregate_task = asyncio.create_task(
            runtime.microstructure_aggregate_loop(
                settings=settings,
                state=CollectorState(symbol="BTCUSDT"),
                events=events,
                persistence_rows=persistence_rows,
                live_cache=FailingLiveCache(),
            )
        )
        persistence_task = asyncio.create_task(
            runtime.microstructure_persistence_loop(
                pool=object(),
                settings=settings,
                persistence_rows=persistence_rows,
            )
        )
        await asyncio.sleep(0)
        clock["now_ms"] = 2_250
        events.put_nowait(QueuedEvent(2_000, 0, "error", "wake"))
        await asyncio.wait_for(persisted.wait(), timeout=1)

        assert persisted_rows == [(1_000, 2_250)]

        aggregate_task.cancel()
        persistence_task.cancel()
        await asyncio.gather(
            aggregate_task,
            persistence_task,
            return_exceptions=True,
        )

    asyncio.run(scenario())


def test_persistence_worker_pauses_at_cap_then_resumes(monkeypatch):
    async def scenario():
        clock = {"now_ms": 10 * runtime.MILLISECONDS_PER_DAY}
        persistence_rows = asyncio.Queue(maxsize=10)
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        calls = []
        relation_sizes = iter((150, 99))
        gate = runtime.MicrostructureWriteGate(
            warning_bytes=100,
            maximum_bytes=150,
        )

        async def fake_retention(pool, *, symbol, now_ms, retention_days):
            calls.append(("retention", now_ms))
            return "DELETE 0"

        async def fake_relation_size(pool):
            size = next(relation_sizes)
            calls.append(("size", size))
            return size

        async def fake_upsert(pool, *, symbol, row, received_ms):
            calls.append(
                ("postgres", row["sample_second_ms"], received_ms)
            )

        monkeypatch.setattr(
            runtime,
            "current_utc_epoch_ms",
            lambda: clock["now_ms"],
        )
        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            fake_upsert,
        )

        persistence_rows.put_nowait(
            runtime.FinalizedMicrostructureRow(
                row={"sample_second_ms": 1_000},
                received_ms=2_250,
            )
        )
        task = asyncio.create_task(
            runtime.microstructure_persistence_loop(
                pool=object(),
                settings=settings,
                persistence_rows=persistence_rows,
                write_gate=gate,
            )
        )
        await asyncio.wait_for(persistence_rows.join(), timeout=1)
        assert gate.paused is True
        assert not any(call[0] == "postgres" for call in calls)

        # Maintenance cadence follows writer wall time, not the queued row's
        # old received_ms. Advancing one minute permits a fresh size check.
        clock["now_ms"] += 60_000
        persistence_rows.put_nowait(
            runtime.FinalizedMicrostructureRow(
                row={"sample_second_ms": 2_000},
                received_ms=3_250,
            )
        )
        await asyncio.wait_for(persistence_rows.join(), timeout=1)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert calls == [
            ("retention", 10 * runtime.MILLISECONDS_PER_DAY),
            ("size", 150),
            ("size", 99),
            ("postgres", 2_000, 3_250),
        ]
        assert gate.paused is False

    asyncio.run(scenario())


def test_persistence_failure_does_not_stop_newer_rows(monkeypatch):
    async def scenario():
        persistence_rows = asyncio.Queue(maxsize=10)
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        attempted = []

        async def fake_retention(*args, **kwargs):
            return "DELETE 0"

        async def fake_relation_size(pool):
            return 0

        async def flaky_upsert(pool, *, symbol, row, received_ms):
            attempted.append(row["sample_second_ms"])
            if len(attempted) == 1:
                raise OSError("postgres unavailable")

        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            flaky_upsert,
        )

        for sample_second_ms in (1_000, 2_000):
            persistence_rows.put_nowait(
                runtime.FinalizedMicrostructureRow(
                    row={"sample_second_ms": sample_second_ms},
                    received_ms=sample_second_ms + 1_250,
                )
            )
        task = asyncio.create_task(
            runtime.microstructure_persistence_loop(
                pool=object(),
                settings=settings,
                persistence_rows=persistence_rows,
            )
        )
        await asyncio.wait_for(persistence_rows.join(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert attempted == [1_000, 2_000]

    asyncio.run(scenario())


def test_persistence_queue_overflow_keeps_newest_rows(caplog):
    async def scenario():
        queue = asyncio.Queue(maxsize=2)
        runtime._LAST_OFFER_LOG_AT.pop("persistence_queue_overflow", None)
        first_row = {"sample_second_ms": 1_000}

        assert runtime.enqueue_finalized_microstructure_row(
            queue,
            row=first_row,
            received_ms=2_250,
        ) is None
        first_row["sample_second_ms"] = 999_000
        assert runtime.enqueue_finalized_microstructure_row(
            queue,
            row={"sample_second_ms": 2_000},
            received_ms=3_250,
        ) is None
        with caplog.at_level("ERROR"):
            dropped = runtime.enqueue_finalized_microstructure_row(
                queue,
                row={"sample_second_ms": 3_000},
                received_ms=4_250,
            )

        assert dropped.row["sample_second_ms"] == 1_000
        assert [
            queue.get_nowait().row["sample_second_ms"],
            queue.get_nowait().row["sample_second_ms"],
        ] == [2_000, 3_000]

    asyncio.run(scenario())
    assert "binance_microstructure_persistence_queue_overflow" in caplog.text


def test_persistence_shutdown_drains_before_stopping_writer(monkeypatch):
    async def scenario():
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=10,
            BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=5,
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        collector_runtime = runtime.MicrostructureRuntime(settings, object())
        persisted = []

        async def fake_retention(*args, **kwargs):
            return "DELETE 0"

        async def fake_relation_size(pool):
            return 0

        async def fake_upsert(pool, *, symbol, row, received_ms):
            persisted.append((row["sample_second_ms"], received_ms))

        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            fake_upsert,
        )

        for sample_second_ms in (1_000, 2_000):
            collector_runtime.persistence_rows.put_nowait(
                runtime.FinalizedMicrostructureRow(
                    row={"sample_second_ms": sample_second_ms},
                    received_ms=sample_second_ms + 1_250,
                )
            )
        persistence_task = asyncio.create_task(
            collector_runtime._persistence_supervisor()
        )

        discarded = await collector_runtime._stop_persistence(
            persistence_task
        )

        assert discarded == 0
        assert persisted == [(1_000, 2_250), (2_000, 3_250)]
        assert collector_runtime.persistence_rows.empty()
        await asyncio.wait_for(
            collector_runtime.persistence_rows.join(),
            timeout=0.1,
        )

    asyncio.run(scenario())


def test_persistence_shutdown_timeout_cancels_and_accounts_rows(
    monkeypatch,
    caplog,
):
    async def scenario():
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=10,
            BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=5,
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        collector_runtime = runtime.MicrostructureRuntime(settings, object())
        started = asyncio.Event()

        async def fake_retention(*args, **kwargs):
            return "DELETE 0"

        async def fake_relation_size(pool):
            return 0

        async def blocked_upsert(*args, **kwargs):
            started.set()
            await asyncio.Future()

        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            blocked_upsert,
        )
        monkeypatch.setattr(
            runtime,
            "MICROSTRUCTURE_PERSIST_DRAIN_TIMEOUT_SECONDS",
            0.01,
        )

        for sample_second_ms in (1_000, 2_000):
            collector_runtime.persistence_rows.put_nowait(
                runtime.FinalizedMicrostructureRow(
                    row={"sample_second_ms": sample_second_ms},
                    received_ms=sample_second_ms + 1_250,
                )
            )
        persistence_task = asyncio.create_task(
            collector_runtime._persistence_supervisor()
        )
        await asyncio.wait_for(started.wait(), timeout=1)

        with caplog.at_level("ERROR"):
            discarded = await collector_runtime._stop_persistence(
                persistence_task
            )

        assert discarded == 2
        assert collector_runtime.persistence_rows.empty()
        await asyncio.wait_for(
            collector_runtime.persistence_rows.join(),
            timeout=0.1,
        )

    asyncio.run(scenario())
    assert "binance_microstructure_persistence_drain_timed_out" in caplog.text
    assert "binance_microstructure_persistence_rows_discarded" in caplog.text
    discard_record = next(
        record
        for record in caplog.records
        if record.message
        == "binance_microstructure_persistence_rows_discarded"
    )
    assert discard_record.discarded_row_count == 2
    assert discard_record.oldest_sample_second_ms == 1_000
    assert discard_record.newest_sample_second_ms == 2_000


def test_persistence_supervisor_survives_producer_restart(monkeypatch):
    async def scenario():
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=10,
            BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=5,
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        collector_runtime = runtime.MicrostructureRuntime(settings, object())
        producer_round = {"value": 0}
        second_round_started = asyncio.Event()
        persistence_starts = []

        def producer_coroutines():
            producer_round["value"] += 1
            current_round = producer_round["value"]

            async def producer():
                if current_round == 1:
                    raise RuntimeError("restart producers")
                second_round_started.set()
                await asyncio.Future()

            return [producer()]

        async def fake_persistence_loop(**kwargs):
            persistence_starts.append(kwargs["persistence_rows"])
            await asyncio.Future()

        collector_runtime._producer_coroutines = producer_coroutines
        monkeypatch.setattr(
            runtime,
            "microstructure_persistence_loop",
            fake_persistence_loop,
        )
        monkeypatch.setattr(
            runtime,
            "reconnect_delay_seconds",
            lambda attempt: 0,
        )

        task = asyncio.create_task(collector_runtime.run())
        await asyncio.wait_for(second_round_started.wait(), timeout=1)
        assert persistence_starts == [collector_runtime.persistence_rows]

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_cancelled_persistence_requeues_inflight_row_when_safe(monkeypatch):
    async def scenario():
        queue = asyncio.Queue(maxsize=2)
        settings = SimpleNamespace(
            BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30,
            BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=100,
            BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=200,
            BINANCE_FUTURES_SYMBOL="BTCUSDT",
        )
        started = asyncio.Event()
        finalized = runtime.FinalizedMicrostructureRow(
            row={"sample_second_ms": 1_000},
            received_ms=2_250,
        )
        queue.put_nowait(finalized)

        async def fake_retention(*args, **kwargs):
            return "DELETE 0"

        async def fake_relation_size(pool):
            return 0

        async def blocked_upsert(*args, **kwargs):
            started.set()
            await asyncio.Future()

        monkeypatch.setattr(
            runtime,
            "delete_expired_microstructure_rows",
            fake_retention,
        )
        monkeypatch.setattr(
            runtime,
            "fetch_microstructure_relation_size_bytes",
            fake_relation_size,
        )
        monkeypatch.setattr(
            runtime,
            "upsert_binance_microstructure_1s",
            blocked_upsert,
        )

        task = asyncio.create_task(
            runtime.microstructure_persistence_loop(
                pool=object(),
                settings=settings,
                persistence_rows=queue,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert queue.get_nowait() is finalized

    asyncio.run(scenario())
