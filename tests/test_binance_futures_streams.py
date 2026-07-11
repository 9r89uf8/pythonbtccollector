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
    trade_state=None,
):
    trade_state = trade_state or streams.FuturesTradeState()
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
            trade_state=trade_state,
            raw_capture=raw,
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


def test_futures_trade_state_tracks_gaps_duplicates_regressions_and_freshness():
    state = streams.FuturesTradeState()
    first_connection = UUID("11111111-1111-1111-1111-111111111111")
    second_connection = UUID("22222222-2222-2222-2222-222222222222")
    state.connection_opened(first_connection)
    first = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=10, p="100", T=1_783_459_500_100),
        expected_symbol="BTCUSDT",
    )
    first_item = state.update_ws(
        first,
        raw_capture.ReceiveStamp(
            connection_id=first_connection,
            receive_sequence=1,
            received_wall_ns=1_783_459_500_120_000_000,
            received_monotonic_ns=1_000_000_000,
        ),
    )
    assert first_item is not None
    assert first_item.sequence == 1
    assert state.fresh_current(
        now_monotonic_ns=11_000_000_000,
        stale_after_ms=10_000,
    ) == first_item
    assert state.fresh_current(
        now_monotonic_ns=11_001_000_000,
        stale_after_ms=10_000,
    ) is None

    state.connection_closed(first_connection)
    assert state.fresh_current(
        now_monotonic_ns=1_000_000_001,
        stale_after_ms=10_000,
    ) is None

    state.connection_opened(second_connection)
    assert state.fresh_current(
        now_monotonic_ns=1_000_000_001,
        stale_after_ms=10_000,
    ) is None

    gap = streams.parse_binance_futures_agg_trade_payload(
        agg_trade_payload(a=12, p="103", T=1_783_459_500_200),
        expected_symbol="BTCUSDT",
    )
    gap_stamp = raw_capture.ReceiveStamp(
        connection_id=second_connection,
        receive_sequence=1,
        received_wall_ns=1_783_459_500_220_000_000,
        received_monotonic_ns=1_100_000_000,
    )
    gap_item = state.update_ws(gap, gap_stamp)
    assert gap_item is not None
    assert gap_item.sequence == 2
    assert state.update_ws(gap, gap_stamp) is None
    assert state.update_ws(
        streams.parse_binance_futures_agg_trade_payload(
            agg_trade_payload(a=11, p="200", T=1_783_459_500_190),
            expected_symbol="BTCUSDT",
        ),
        raw_capture.ReceiveStamp(
            connection_id=second_connection,
            receive_sequence=2,
            received_wall_ns=1_783_459_500_230_000_000,
            received_monotonic_ns=1_110_000_000,
        ),
    ) is None

    current = state.fresh_current(
        now_monotonic_ns=1_100_000_001,
        stale_after_ms=10_000,
    )
    assert current == gap_item
    assert current.trade.price == Decimal("103")
    fields = state.telemetry_fields(now_ms=1_783_459_500_190)
    assert fields["shadow_reconnects_total"] == 1
    assert fields["shadow_ws_id_gap_events_total"] == 1
    assert fields["shadow_ws_missing_agg_trades_total"] == 1
    assert fields["shadow_ws_duplicate_ids_total"] == 1
    assert fields["shadow_ws_regressions_total"] == 1
    assert fields["shadow_ws_current_for_connection"] is True
    assert fields["shadow_ws_price"] == Decimal("103")
    assert fields["futures_live_delivery_sequence"] == 2


def test_futures_trade_state_live_attempt_barrier_advances_on_failure():
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("33333333-3333-3333-3333-333333333333")
        state.connection_opened(connection_id)
        item = state.update_ws(
            streams.parse_binance_futures_agg_trade_payload(
                agg_trade_payload(a=10),
                expected_symbol="BTCUSDT",
            ),
            raw_capture.ReceiveStamp(
                connection_id=connection_id,
                receive_sequence=1,
                received_wall_ns=1_783_459_500_220_000_000,
                received_monotonic_ns=1_100_000_000,
            ),
        )
        assert item is not None

        waiter = asyncio.create_task(state.wait_until_live_attempted(item.sequence))
        await asyncio.sleep(0)
        assert not waiter.done()
        state.mark_live_attempted(
            item,
            succeeded=False,
            attempted_ms=1_783_459_500_300,
        )
        assert await asyncio.wait_for(waiter, timeout=0.5) == item

        fields = state.telemetry_fields(now_ms=1_783_459_500_400)
        assert fields["futures_live_attempted_sequence"] == item.sequence
        assert fields["futures_live_attempts_total"] == 1
        assert fields["futures_live_successes_total"] == 0
        assert fields["futures_live_failures_total"] == 1

    asyncio.run(scenario())


def test_futures_trade_state_wait_for_latest_collapses_to_newest_version():
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("44444444-4444-4444-4444-444444444444")
        state.connection_opened(connection_id)
        for receive_sequence, trade_id, price in (
            (1, 10, "100"),
            (2, 11, "101"),
        ):
            state.update_ws(
                streams.parse_binance_futures_agg_trade_payload(
                    agg_trade_payload(
                        a=trade_id,
                        p=price,
                        f=100 + receive_sequence,
                        l=100 + receive_sequence,
                        T=1_783_459_500_100 + receive_sequence,
                    ),
                    expected_symbol="BTCUSDT",
                ),
                raw_capture.ReceiveStamp(
                    connection_id=connection_id,
                    receive_sequence=receive_sequence,
                    received_wall_ns=(
                        1_783_459_500_220_000_000 + receive_sequence
                    ),
                    received_monotonic_ns=1_100_000_000 + receive_sequence,
                ),
            )

        latest = await state.wait_for_latest_after(0)
        assert latest.sequence == 2
        assert latest.trade.price == Decimal("101")

    asyncio.run(scenario())


def test_futures_trade_state_barrier_returns_newer_attempted_version():
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("55555555-5555-5555-5555-555555555555")
        state.connection_opened(connection_id)
        first = state.update_ws(
            streams.parse_binance_futures_agg_trade_payload(
                agg_trade_payload(a=10, T=1_783_459_500_100),
                expected_symbol="BTCUSDT",
            ),
            raw_capture.ReceiveStamp(
                connection_id=connection_id,
                receive_sequence=1,
                received_wall_ns=1_783_459_500_120_000_000,
                received_monotonic_ns=1_000_000_000,
            ),
        )
        second = state.update_ws(
            streams.parse_binance_futures_agg_trade_payload(
                agg_trade_payload(a=11, p="101", T=1_783_459_500_200),
                expected_symbol="BTCUSDT",
            ),
            raw_capture.ReceiveStamp(
                connection_id=connection_id,
                receive_sequence=2,
                received_wall_ns=1_783_459_500_220_000_000,
                received_monotonic_ns=1_100_000_000,
            ),
        )
        assert first is not None and second is not None
        state.mark_live_attempted(
            second,
            succeeded=True,
            attempted_ms=1_783_459_500_300,
        )

        assert await state.wait_until_live_attempted(first.sequence) == second

    asyncio.run(scenario())


def test_reader_stamps_receive_clocks_before_parse_and_updates_state_without_raw(
    monkeypatch,
):
    events = []
    websocket = ScriptedWebSocket([json.dumps(agg_trade_payload())], events=events)
    state = streams.FuturesTradeState()
    original_loads = json.loads
    connection_id = UUID("55555555-5555-5555-5555-555555555555")

    class StateCheckingFlowStore(RecordingFlowStore):
        async def add_trade(self, trade, *, received_ms):
            fields = state.telemetry_fields(now_ms=received_ms)
            assert fields["futures_live_delivery_sequence"] == 1
            assert fields["shadow_ws_price"] == trade.price
            events.append("flow")
            return await super().add_trade(trade, received_ms=received_ms)

    flow_store = StateCheckingFlowStore()

    def parse_after_stamp(value):
        events.append("parse")
        return original_loads(value)

    def wall_stamp():
        events.append("wall")
        return 1_783_459_500_222_000_000

    def monotonic_stamp():
        events.append("monotonic")
        return 1_100_000_000

    monkeypatch.setattr(streams.json, "loads", parse_after_stamp)
    monkeypatch.setattr(streams.time, "time_ns", wall_stamp)
    monkeypatch.setattr(streams.time, "monotonic_ns", monotonic_stamp)
    monkeypatch.setattr(streams, "uuid4", lambda: connection_id)

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            trade_state=state,
        )
    )

    receive_index = events.index("recv")
    assert events[receive_index : receive_index + 4] == [
        "recv",
        "wall",
        "monotonic",
        "parse",
    ]
    assert events.index("parse") < events.index("flow")
    assert flow_store.calls[0][1] == 1_783_459_500_222
    fields = state.telemetry_fields(now_ms=1_783_459_500_223)
    assert fields["shadow_ws_price"] == Decimal("100.00")
    assert fields["shadow_ws_trade_time_ms"] == 1_783_459_500_100
    assert fields["shadow_ws_received_ms"] == 1_783_459_500_222


def test_reader_rejects_non_current_connection_state_after_shutdown(monkeypatch):
    state = streams.FuturesTradeState()
    websocket = ScriptedWebSocket([json.dumps(agg_trade_payload())])
    flow_store = RecordingFlowStore()
    install_capture_clocks(monkeypatch)

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            trade_state=state,
        )
    )

    assert state.current_connection_id is None
    assert state.fresh_current(
        now_monotonic_ns=1_020_000_001,
        stale_after_ms=10_000,
    ) is None


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
            trade_state=streams.FuturesTradeState(),
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
                trade_state=streams.FuturesTradeState(),
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
    trade_state = streams.FuturesTradeState()

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            raw=raw,
            trade_state=trade_state,
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
    assert (
        trade_state.telemetry_fields(1_783_459_501_000)[
            "futures_live_delivery_sequence"
        ]
        == 1
    )


def test_raw_offer_failure_does_not_block_trade_state_or_flow(monkeypatch):
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
    trade_state = streams.FuturesTradeState()

    asyncio.run(
        run_reader_until_flow(
            monkeypatch,
            websocket=websocket,
            flow_store=flow_store,
            raw=raw,
            trade_state=trade_state,
        )
    )

    assert len(flow_store.calls) == 2
    assert (
        trade_state.telemetry_fields(1_783_459_501_000)["shadow_ws_price"]
        == Decimal("102")
    )
    assert raw.counters.records_dropped_total == 2


def test_raw_capture_telemetry_logs_required_summary_fields(
    monkeypatch,
    caplog,
):
    raw = RecordingRawCapture()
    raw.buffer = SimpleNamespace(qsize=lambda: 0)
    raw.counters.message_received()
    raw.counters.message_accepted()
    trade_state = streams.FuturesTradeState()
    connection_id = UUID("33333333-3333-3333-3333-333333333333")
    trade_state.connection_opened(connection_id)
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
                trade_state=trade_state,
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
        "futures_live_delivery_sequence",
        "futures_live_attempted_sequence",
        "futures_live_attempts_total",
        "futures_live_successes_total",
        "futures_live_failures_total",
        "futures_live_last_attempt_ms",
    ):
        assert hasattr(summary, field_name)
