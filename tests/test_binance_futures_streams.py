import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

import price_collector.binance_futures_streams as streams
import price_collector.raw_capture as raw_capture


def agg_trade_payload(**overrides):
    payload = {
        "e": "aggTrade",
        "E": 1_783_459_500_125,
        "a": 10,
        "s": "BTCUSDT",
        "p": "100.00",
        "q": "2.000",
        "f": 100,
        "l": 102,
        "T": 1_783_459_500_100,
        "m": False,
    }
    payload.update(overrides)
    return payload


def book_ticker_payload(**overrides):
    payload = {
        "e": "bookTicker",
        "E": 1_783_459_500_125,
        "T": 1_783_459_500_120,
        "u": 123456,
        "s": "BTCUSDT",
        "b": "100.00",
        "B": "3.000",
        "a": "102.00",
        "A": "1.000",
    }
    payload.update(overrides)
    return payload


def test_parse_agg_trade_uses_decimal_fields_and_taker_side():
    trade = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(m=False),
        expected_symbol="BTCUSDT",
    )

    assert trade.symbol == "BTCUSDT"
    assert trade.price == Decimal("100.00")
    assert trade.quantity == Decimal("2.000")
    assert trade.quote_notional == Decimal("200.00000")
    assert trade.trade_count == 3
    assert trade.buyer_is_maker is False
    assert not isinstance(trade.price, float)


def test_parse_agg_trade_rejects_unexpected_symbol():
    with pytest.raises(streams.FuturesStreamParseError, match="unexpected aggTrade symbol"):
        streams.parse_binance_futures_agg_trade_payload(
            agg_trade_payload(s="ETHUSDT"),
            expected_symbol="BTCUSDT",
        )


def test_flow_aggregator_flushes_one_second_buy_sell_totals():
    aggregator = streams.FlowAggregator(
        venue="binance_usdm_perp",
        symbol="BTCUSDT",
    )
    buy = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=10, p="100.00", q="2.0", f=100, l=102, m=False),
        expected_symbol="BTCUSDT",
    )
    sell = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=11, p="110.00", q="1.0", f=103, l=103, m=True),
        expected_symbol="BTCUSDT",
    )

    assert aggregator.add_trade(buy, received_ms=1_783_459_500_200) is True
    assert aggregator.add_trade(sell, received_ms=1_783_459_500_300) is True
    samples = aggregator.flush_ready(
        now_ms=1_783_459_502_000,
        flush_delay_ms=1_500,
    )

    assert len(samples) == 1
    sample = samples[0]
    assert sample.sample_second_ms == 1_783_459_500_000
    assert sample.window.market_start_ms == 1_783_459_500_000
    assert sample.buy_quote == Decimal("200.000")
    assert sample.sell_quote == Decimal("110.000")
    assert sample.delta_quote == Decimal("90.000")
    assert sample.total_quote == Decimal("310.000")
    assert sample.taker_imbalance == Decimal("90.000") / Decimal("310.000")
    assert sample.cvd_quote == Decimal("90.000")
    assert sample.cvd_10s == Decimal("90.000")
    assert sample.agg_trade_count == 2
    assert sample.trade_count == 4
    assert sample.first_agg_trade_id == 10
    assert sample.last_agg_trade_id == 11
    assert sample.max_trade_quote == Decimal("200.000")


def test_flow_aggregator_fills_missing_seconds_between_trade_buckets():
    aggregator = streams.FlowAggregator(
        venue="binance_usdm_perp",
        symbol="BTCUSDT",
    )
    first = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(
            a=10,
            p="100.00",
            q="1",
            T=1_783_459_500_100,
            E=1_783_459_500_125,
            m=False,
        ),
        expected_symbol="BTCUSDT",
    )
    third = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(
            a=11,
            p="50.00",
            q="1",
            T=1_783_459_502_100,
            E=1_783_459_502_125,
            f=103,
            l=103,
            m=True,
        ),
        expected_symbol="BTCUSDT",
    )

    aggregator.add_trade(first, received_ms=1_783_459_500_200)
    aggregator.add_trade(third, received_ms=1_783_459_502_200)
    samples = aggregator.flush_ready(
        now_ms=1_783_459_504_000,
        flush_delay_ms=1_500,
    )

    assert [sample.sample_second_ms for sample in samples] == [
        1_783_459_500_000,
        1_783_459_501_000,
        1_783_459_502_000,
    ]
    assert [sample.delta_quote for sample in samples] == [
        Decimal("100.00"),
        Decimal("0"),
        Decimal("-50.00"),
    ]
    assert samples[1].agg_trade_count == 0
    assert samples[2].cvd_quote == Decimal("50.00")
    assert samples[2].cvd_10s == Decimal("50.00")


def test_parse_book_ticker_and_builds_derived_values():
    ticker = streams.parse_binance_futures_book_ticker_payload(
        book_ticker_payload(),
        expected_symbol="BTCUSDT",
    )
    sample = streams.build_book_sample(
        venue="binance_usdm_perp",
        ticker=ticker,
        sample_second_ms=1_783_459_500_000,
        received_ms=1_783_459_500_200,
    )

    assert sample.bid == Decimal("100.00")
    assert sample.ask == Decimal("102.00")
    assert sample.mid == Decimal("101.00")
    assert sample.spread == Decimal("2.00")
    assert sample.spread_bps == Decimal("2.00") / Decimal("101.00") * Decimal("10000")
    assert sample.book_imbalance == Decimal("2.000") / Decimal("4.000")
    assert sample.microprice == Decimal("406.00000") / Decimal("4.000")
    assert sample.event_time_ms == 1_783_459_500_125
    assert sample.transaction_time_ms == 1_783_459_500_120


def test_book_ticker_aggregator_keeps_latest_snapshot_per_second():
    aggregator = streams.BookTickerAggregator(
        venue="binance_usdm_perp",
        symbol="BTCUSDT",
    )
    older = streams.parse_binance_futures_book_ticker_payload(
        book_ticker_payload(E=1_783_459_500_100, b="100.00", a="102.00"),
        expected_symbol="BTCUSDT",
    )
    newer = streams.parse_binance_futures_book_ticker_payload(
        book_ticker_payload(E=1_783_459_500_900, b="101.00", a="103.00"),
        expected_symbol="BTCUSDT",
    )

    assert aggregator.update(older, received_ms=1_783_459_500_110) is True
    assert aggregator.update(newer, received_ms=1_783_459_500_910) is True
    samples = aggregator.flush_ready(
        now_ms=1_783_459_502_500,
        flush_delay_ms=1_500,
    )

    assert len(samples) == 1
    assert samples[0].sample_second_ms == 1_783_459_500_000
    assert samples[0].bid == Decimal("101.00")
    assert samples[0].ask == Decimal("103.00")


def test_book_ticker_rejects_crossed_book():
    with pytest.raises(streams.FuturesStreamParseError, match="ask"):
        streams.parse_binance_futures_book_ticker_payload(
            book_ticker_payload(b="103.00", a="102.00"),
            expected_symbol="BTCUSDT",
        )


class ScriptedWebSocket:
    def __init__(self, messages, *, events=None):
        self.messages = list(messages)
        self.events = events

    async def recv(self):
        if self.messages:
            if self.events is not None:
                self.events.append("recv")
            return self.messages.pop(0)
        await asyncio.Future()


class WebSocketContext:
    def __init__(self, websocket):
        self.websocket = websocket

    async def __aenter__(self):
        return self.websocket

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class RecordingFlowStore:
    def __init__(self, target_count=1):
        self.target_count = target_count
        self.calls = []
        self.reached_target = None

    async def add_trade(self, trade, *, received_ms):
        self.calls.append((trade, received_ms))
        if (
            self.reached_target is not None
            and len(self.calls) >= self.target_count
        ):
            self.reached_target.set()
        await asyncio.sleep(0)
        return True


class RecordingRawCapture:
    def __init__(self, *, fail_traces=False):
        self.counters = raw_capture.CaptureCounters()
        self.records = []
        self.fail_traces = fail_traces

    def offer_nowait(self, record):
        if self.fail_traces and isinstance(
            record,
            raw_capture.BinanceFuturesPriceTrace,
        ):
            raise RuntimeError("trace sink failed")
        self.records.append(record)
        return raw_capture.OfferResult(
            accepted=True,
            dropped_oldest=False,
            dropped_record=None,
            queue_depth=len(self.records),
            queue_high_water=len(self.records),
        )


def stream_settings():
    return SimpleNamespace(
        BINANCE_FUTURES_AGG_TRADE_WS_URL="wss://example.test/aggTrade",
        BINANCE_FUTURES_SYMBOL="BTCUSDT",
        RAW_FUTURES_BUCKET_MS=100,
    )


async def run_reader_until_flow(
    monkeypatch,
    *,
    websocket,
    flow_store,
    raw=None,
    shadow=None,
):
    flow_store.reached_target = asyncio.Event()
    monkeypatch.setattr(
        streams.websockets,
        "connect",
        lambda *args, **kwargs: WebSocketContext(websocket),
    )
    task = asyncio.create_task(
        streams.futures_agg_trade_reader_loop(
            stream_settings(),
            flow_store,
            raw_capture=raw,
            shadow_monitor=shadow,
        )
    )
    await asyncio.wait_for(flow_store.reached_target.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def install_capture_clocks(monkeypatch):
    wall_ns = [1_783_459_500_000_000_000]
    monotonic_ns = [1_000_000_000]

    def next_wall_ns():
        wall_ns[0] += 10_000_000
        return wall_ns[0]

    def next_monotonic_ns():
        monotonic_ns[0] += 10_000_000
        return monotonic_ns[0]

    monkeypatch.setattr(streams.time, "time_ns", next_wall_ns)
    monkeypatch.setattr(streams.time, "monotonic_ns", next_monotonic_ns)


def test_futures_shadow_monitor_tracks_decimal_comparison_gaps_and_clock_skew():
    monitor = streams.FuturesShadowMonitor()
    first_connection = UUID("11111111-1111-1111-1111-111111111111")
    second_connection = UUID("22222222-2222-2222-2222-222222222222")
    monitor.observe_rest(Decimal("99"), 1_783_459_500_150, 1_783_459_500_160)
    monitor.connection_opened(first_connection)
    first = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=10, p="100", T=1_783_459_500_100),
        expected_symbol="BTCUSDT",
    )
    monitor.update_ws(
        first,
        raw_capture.ReceiveStamp(
            connection_id=first_connection,
            receive_sequence=1,
            received_wall_ns=1_783_459_500_120_000_000,
            received_monotonic_ns=100,
        ),
    )
    monitor.observe_rest(Decimal("99"), 1_783_459_500_150, 1_783_459_500_160)
    monitor.connection_closed(first_connection)
    monitor.observe_rest(Decimal("99"), 1_783_459_500_175, 1_783_459_500_180)
    assert (
        monitor.telemetry_fields(1_783_459_500_180)[
            "shadow_ws_current_for_connection"
        ]
        is False
    )
    monitor.connection_opened(second_connection)
    gap = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=12, p="103", T=1_783_459_500_200),
        expected_symbol="BTCUSDT",
    )
    gap_stamp = raw_capture.ReceiveStamp(
        connection_id=second_connection,
        receive_sequence=1,
        received_wall_ns=1_783_459_500_220_000_000,
        received_monotonic_ns=200,
    )
    monitor.update_ws(gap, gap_stamp)
    monitor.update_ws(gap, gap_stamp)
    monitor.update_ws(
        streams.parse_binance_futures_agg_trade_payload(
            agg_trade_payload(a=11, p="200"),
            expected_symbol="BTCUSDT",
        ),
        raw_capture.ReceiveStamp(
            connection_id=second_connection,
            receive_sequence=2,
            received_wall_ns=1_783_459_500_230_000_000,
            received_monotonic_ns=210,
        ),
    )

    fields = monitor.telemetry_fields(now_ms=1_783_459_500_190)

    assert fields["shadow_reconnects_total"] == 1
    assert fields["shadow_ws_id_gap_events_total"] == 1
    assert fields["shadow_ws_missing_agg_trades_total"] == 1
    assert fields["shadow_ws_duplicate_ids_total"] == 1
    assert fields["shadow_ws_regressions_total"] == 1
    assert fields["shadow_rest_observations_total"] == 3
    assert fields["shadow_rest_missing_ws_total"] == 2
    assert fields["shadow_rest_comparisons_total"] == 1
    assert fields["shadow_ws_current_for_connection"] is True
    assert fields["shadow_ws_price"] == Decimal("103")
    assert fields["shadow_ws_minus_rest_price"] == Decimal("4")
    assert fields["shadow_ws_minus_rest_bps"] == Decimal("4") / Decimal("99") * Decimal("10000")
    assert isinstance(fields["shadow_ws_minus_rest_price"], Decimal)
    assert fields["shadow_ws_source_age_ms"] == -10
    assert fields["shadow_ws_minus_rest_source_time_ms"] == 25


def test_disabled_reader_stamps_wall_before_parse_without_raw_uuid_or_monotonic(
    monkeypatch,
):
    events = []
    websocket = ScriptedWebSocket([json.dumps(agg_trade_payload())], events=events)
    flow_store = RecordingFlowStore()
    original_loads = json.loads

    def parse_after_stamp(value):
        events.append("parse")
        return original_loads(value)

    def wall_stamp():
        events.append("wall")
        return 1_783_459_500_222_000_000

    monkeypatch.setattr(streams.json, "loads", parse_after_stamp)
    monkeypatch.setattr(streams.time, "time_ns", wall_stamp)
    monkeypatch.setattr(
        streams.time,
        "monotonic_ns",
        lambda: (_ for _ in ()).throw(AssertionError("disabled monotonic_ns")),
    )
    monkeypatch.setattr(
        streams,
        "uuid4",
        lambda: (_ for _ in ()).throw(AssertionError("disabled uuid4")),
    )

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
        )
    )

    receive_index = events.index("recv")
    assert events[receive_index : receive_index + 3] == ["recv", "wall", "parse"]
    assert flow_store.calls[0][1] == 1_783_459_500_222


def test_raw_capture_coalesces_and_finalizes_session_on_cancellation(monkeypatch):
    install_capture_clocks(monkeypatch)
    websocket = ScriptedWebSocket(
        [
            json.dumps(agg_trade_payload(a=10, p="100")),
            json.dumps(agg_trade_payload(a=11, p="103", f=103, l=103)),
        ]
    )
    flow_store = RecordingFlowStore(target_count=2)
    raw = RecordingRawCapture()

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            raw=raw,
            shadow=streams.FuturesShadowMonitor(),
        )
    )

    assert len(flow_store.calls) == 2
    assert [type(record) for record in raw.records] == [
        raw_capture.FeedSessionRecord,
        raw_capture.BinanceFuturesPriceTrace,
        raw_capture.FeedSessionRecord,
    ]
    trace = raw.records[1]
    assert trace.event_count == 2
    assert trace.open_price == Decimal("100")
    assert trace.high_price == Decimal("103")
    assert trace.close_price == Decimal("103")
    closed = raw.records[2]
    assert closed.close_reason == "cancelled"
    assert closed.messages_received_total == 2
    assert closed.messages_accepted_total == 2


def test_raw_receive_clocks_are_captured_before_json_parsing(monkeypatch):
    events = []
    websocket = ScriptedWebSocket(
        [json.dumps(agg_trade_payload())],
        events=events,
    )
    flow_store = RecordingFlowStore()
    raw = RecordingRawCapture()
    original_loads = json.loads
    wall_ns = [1_783_459_500_000_000_000]
    monotonic_ns = [1_000_000_000]

    def next_wall_ns():
        events.append("wall")
        wall_ns[0] += 10_000_000
        return wall_ns[0]

    def next_monotonic_ns():
        events.append("monotonic")
        monotonic_ns[0] += 10_000_000
        return monotonic_ns[0]

    def parse_after_clocks(value):
        events.append("parse")
        return original_loads(value)

    monkeypatch.setattr(streams.time, "time_ns", next_wall_ns)
    monkeypatch.setattr(streams.time, "monotonic_ns", next_monotonic_ns)
    monkeypatch.setattr(streams.json, "loads", parse_after_clocks)

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            raw=raw,
        )
    )

    receive_index = events.index("recv")
    assert events[receive_index : receive_index + 4] == [
        "recv",
        "wall",
        "monotonic",
        "parse",
    ]


def test_idle_timeout_seals_pending_trace_before_connection_closes(monkeypatch):
    raw = RecordingRawCapture()
    flow_store = RecordingFlowStore()
    wall_values = iter(
        [
            1_783_459_500_001_000_000,
            1_783_459_500_002_000_000,
            1_783_459_500_010_000_000,
            1_783_459_500_120_000_000,
            1_783_459_500_130_000_000,
        ]
    )
    monotonic_values = iter(
        [
            1_001_000_000,
            1_002_000_000,
            1_010_000_000,
            1_120_000_000,
            1_130_000_000,
        ]
    )

    class IdleThenCancelWebSocket:
        def __init__(self):
            self.calls = 0

        async def recv(self):
            self.calls += 1
            if self.calls == 1:
                return json.dumps(agg_trade_payload())
            if self.calls == 2:
                await asyncio.Future()
            assert any(
                isinstance(record, raw_capture.BinanceFuturesPriceTrace)
                for record in raw.records
            )
            raise asyncio.CancelledError

    websocket = IdleThenCancelWebSocket()
    monkeypatch.setattr(streams.time, "time_ns", lambda: next(wall_values))
    monkeypatch.setattr(
        streams.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        streams.websockets,
        "connect",
        lambda *args, **kwargs: WebSocketContext(websocket),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            streams.futures_agg_trade_reader_loop(
                stream_settings(),
                flow_store,
                raw_capture=raw,
            )
        )

    traces = [
        record
        for record in raw.records
        if isinstance(record, raw_capture.BinanceFuturesPriceTrace)
    ]
    assert len(traces) == 1
    assert traces[0].event_count == 1


def test_malformed_message_updates_parse_counters_and_valid_trade_still_flows(
    monkeypatch,
):
    install_capture_clocks(monkeypatch)
    websocket = ScriptedWebSocket(["[]", json.dumps(agg_trade_payload())])
    flow_store = RecordingFlowStore()
    raw = RecordingRawCapture()

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            raw=raw,
        )
    )

    closed = [
        record
        for record in raw.records
        if isinstance(record, raw_capture.FeedSessionRecord)
        and record.close_reason is not None
    ][0]
    assert closed.messages_received_total == 2
    assert closed.messages_accepted_total == 1
    assert closed.parse_errors_total == 1
    assert raw.counters.parse_errors_total == 1
    assert len(flow_store.calls) == 1


def test_raw_offer_failure_does_not_block_shadow_or_flow(monkeypatch):
    wall_ns = [1_783_459_500_000_000_000]
    monotonic_ns = [1_000_000_000]

    def next_wall_ns():
        wall_ns[0] += 110_000_000
        return wall_ns[0]

    def next_monotonic_ns():
        monotonic_ns[0] += 110_000_000
        return monotonic_ns[0]

    monkeypatch.setattr(streams.time, "time_ns", next_wall_ns)
    monkeypatch.setattr(streams.time, "monotonic_ns", next_monotonic_ns)
    websocket = ScriptedWebSocket(
        [
            json.dumps(agg_trade_payload(a=10, p="101")),
            json.dumps(agg_trade_payload(a=11, p="102", f=103, l=103)),
        ]
    )
    flow_store = RecordingFlowStore(target_count=2)
    raw = RecordingRawCapture(fail_traces=True)
    shadow = streams.FuturesShadowMonitor()

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            raw=raw,
            shadow=shadow,
        )
    )

    assert len(flow_store.calls) == 2
    assert shadow.telemetry_fields(1_783_459_501_000)["shadow_ws_price"] == Decimal("102")
    assert raw.counters.records_dropped_total == 2


def test_raw_capture_telemetry_logs_required_summary_fields(
    monkeypatch,
    caplog,
):
    raw = RecordingRawCapture()
    raw.buffer = SimpleNamespace(qsize=lambda: 0)
    raw.counters.message_received()
    raw.counters.message_accepted()
    monitor = streams.FuturesShadowMonitor()
    connection_id = UUID("33333333-3333-3333-3333-333333333333")
    monitor.connection_opened(connection_id)
    sleep_calls = 0

    async def one_interval_then_cancel(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(streams.asyncio, "sleep", one_interval_then_cancel)
    caplog.set_level("INFO", logger="price_collector.binance_futures_streams")

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            streams.futures_raw_capture_telemetry_loop(
                raw_capture=raw,
                shadow_monitor=monitor,
                interval_seconds=60,
            )
        )

    summary = next(
        record
        for record in caplog.records
        if getattr(record, "event", None) == "raw_capture_summary"
    )
    assert summary.source == "binance_futures_agg_trade"
    assert summary.messages_received_total == 1
    assert summary.messages_accepted_total == 1
    assert summary.connection_id == connection_id
    for field_name in (
        "parse_errors_total",
        "records_coalesced_total",
        "records_enqueued_total",
        "records_persisted_total",
        "records_dropped_total",
        "batches_failed_total",
        "queue_depth",
        "queue_high_water",
        "last_batch_rows",
        "last_batch_duration_ms",
        "current_partition",
        "raw_table_bytes",
    ):
        assert hasattr(summary, field_name)
