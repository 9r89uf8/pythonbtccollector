import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from price_collector.binance_microstructure import (
    MICROSTRUCTURE_COLUMNS,
    MICROSTRUCTURE_VALUE_COLUMNS,
    CollectorState,
    LiquidationAccumulator,
    MicrostructureEventSink,
    MicrostructureParseError,
    QueuedEvent,
    TradeAccumulator,
    finalize_boundary,
    parse_futures_depth_payload,
    parse_liquidation_payload,
    parse_spot_depth_payload,
)


D = Decimal


def depth_levels(best: int, *, bids: bool, first_quantity: str = "1") -> list[list[str]]:
    direction = -1 if bids else 1
    return [
        [str(best + direction * index), first_quantity if index == 0 else str(index + 1)]
        for index in range(10)
    ]


def spot_depth(*, bid_quantity: str = "4", symbol: str = "BTCUSDT") -> dict:
    return {
        "stream": f"{symbol.lower()}@depth10@100ms",
        "data": {
            "s": symbol,
            "E": 9_850,
            "bids": depth_levels(99, bids=True, first_quantity=bid_quantity),
            "asks": depth_levels(101, bids=False, first_quantity="2"),
        },
    }


def futures_depth(*, subtype=1, symbol: str = "BTCUSDT") -> dict:
    return {
        "stream": f"{symbol.lower()}@depth10@500ms",
        "data": {
            "e": "depthUpdate",
            "s": symbol,
            "st": subtype,
            "E": 9_850,
            "T": 9_840,
            "b": depth_levels(100, bids=True, first_quantity="3"),
            "a": depth_levels(102, bids=False, first_quantity="1"),
        },
    }


def force_order(**order_overrides) -> dict:
    order = {
        "s": "BTCUSDT",
        "S": "SELL",
        "ap": "0",
        "p": "100.5",
        "l": "2",
        "z": "999",
        "q": "1000",
        "T": 9_850,
    }
    order.update(order_overrides)
    return {
        "stream": "btcusdt@forceOrder",
        "data": {"e": "forceOrder", "st": 1, "o": order},
    }


def test_strict_depth_and_liquidation_parsers_keep_decimal_values() -> None:
    spot = parse_spot_depth_payload(spot_depth())
    future = parse_futures_depth_payload(futures_depth())
    fill = parse_liquidation_payload(force_order())

    assert spot.bids[0] == (D("99"), D("4"))
    assert future.asks[0] == (D("102"), D("1"))
    assert future.event_time_ms == 9_840
    assert fill.price == D("100.5")  # ap=0 falls back to order price p
    assert fill.quantity == D("2")  # never q or cumulative z
    assert fill.side == "SELL"


def test_depth_parser_rejects_truncated_top_ten_snapshot() -> None:
    payload = spot_depth()
    payload["data"]["bids"] = payload["data"]["bids"][:1]

    with pytest.raises(MicrostructureParseError, match="ten bid and ask"):
        parse_spot_depth_payload(payload)


@pytest.mark.parametrize(
    "payload,parser",
    [
        (spot_depth(symbol="ETHUSDT"), parse_spot_depth_payload),
        (futures_depth(subtype=2), parse_futures_depth_payload),
        (force_order(s="ETHUSDT"), parse_liquidation_payload),
        (force_order(l=float("nan")), parse_liquidation_payload),
        (force_order(l=True), parse_liquidation_payload),
    ],
)
def test_strict_parsers_reject_wrong_product_and_lossy_numbers(payload, parser) -> None:
    with pytest.raises(MicrostructureParseError):
        parser(payload)


def test_trade_accumulator_is_decimal_only_and_resets_bucket_not_age() -> None:
    trades = TradeAccumulator()
    trades.observe(
        price=D("100"),
        quantity=D("1.5"),
        normal_quantity=D("1"),
        buyer_is_maker=True,
        actual_trades=2,
        received_ms=9_900,
        event_time_ms=9_850,
    )
    trades.observe(
        price=D("102"),
        quantity=D("0.5"),
        normal_quantity=D("0.5"),
        buyer_is_maker=False,
        actual_trades=1,
        received_ms=9_950,
        event_time_ms=9_850,
    )

    row = trades.snapshot_and_reset("fut", 10_000)
    assert row["fut_sell_usdt"] == D("150.0")
    assert row["fut_buy_usdt"] == D("51.0")
    assert row["fut_rpi_sell_usdt"] == D("50.0")
    assert row["fut_rpi_buy_usdt"] == D("0.0")
    assert row["fut_trade_id_span"] == 3
    assert row["fut_aggtrade_count"] == 2
    assert row["fut_max_aggtrade_usdt"] == D("150.0")
    assert row["fut_vwap"] == D("100.5")
    assert row["fut_trade_high"] == D("102")
    assert row["fut_trade_low"] == D("100")
    assert row["fut_last_trade"] == D("102")
    assert row["fut_trade_age_ms"] == 50
    assert row["fut_trade_lag_mean_ms"] == D("75")
    assert row["fut_trade_lag_max_ms"] == 100
    empty = trades.snapshot_and_reset("fut", 11_000)
    assert empty["fut_buy_usdt"] == D("0")
    assert empty["fut_max_aggtrade_usdt"] is None
    assert empty["fut_vwap"] is None
    assert empty["fut_trade_age_ms"] == 1_050
    with pytest.raises(MicrostructureParseError):
        trades.observe(
            price=100.0,
            quantity=D("1"),
            buyer_is_maker=False,
            actual_trades=1,
            received_ms=11_000,
            event_time_ms=10_999,
        )


def test_futures_rpi_is_unknown_when_any_trade_omits_normal_quantity() -> None:
    trades = TradeAccumulator()
    trades.observe(
        price=D("100"),
        quantity=D("1"),
        normal_quantity=None,
        buyer_is_maker=False,
        actual_trades=1,
        received_ms=9_900,
        event_time_ms=9_850,
    )

    row = trades.snapshot_and_reset("fut", 10_000)

    assert row["fut_buy_usdt"] == D("100")
    assert row["fut_rpi_buy_usdt"] is None
    assert row["fut_rpi_sell_usdt"] is None
    empty = trades.snapshot_and_reset("fut", 11_000)
    assert empty["fut_rpi_buy_usdt"] == D("0")
    assert empty["fut_rpi_sell_usdt"] == D("0")


def test_liquidation_accumulator_uses_executed_last_fill_and_correct_side() -> None:
    accumulator = LiquidationAccumulator()
    accumulator.observe(parse_liquidation_payload(force_order()), received_ms=9_900)
    accumulator.observe(
        parse_liquidation_payload(force_order(S="BUY", ap="101", l="3")),
        received_ms=9_950,
    )
    row = accumulator.snapshot_and_reset()
    assert row["long_liq_usdt"] == D("201.0")
    assert row["short_liq_usdt"] == D("303")
    assert row["liq_snapshot_count"] == 2
    assert row["liq_lag_mean_ms"] == D("75")


def test_finalize_boundary_orders_receive_time_and_builds_complete_decimal_row() -> None:
    state = CollectorState()
    state.connections.update({source: True for source in state.connections})
    context = SimpleNamespace(
        symbol="BTCUSDT",
        received_ms=9_900,
        request_started_ms=9_800,
        mark_price=D("101.2"),
        index_price=D("101"),
        last_funding_rate=D("0.0001"),
        next_funding_time_ms=20_000,
        premium_index_time_ms=9_850,
        open_interest=D("1234.5"),
        open_interest_time_ms=9_850,
    )
    pending = [
        QueuedEvent(9_900, 12, "context", context),
        QueuedEvent(10_000, 13, "spot_trade", {
            "s": "BTCUSDT", "e": "aggTrade", "p": "103", "q": "2", "m": False,
            "a": 3, "f": 3, "l": 3, "T": 9_999,
        }),
        QueuedEvent(9_900, 10, "liquidation", parse_liquidation_payload(force_order())),
        QueuedEvent(9_910, 5, "spot_depth", parse_spot_depth_payload(spot_depth(bid_quantity="5"))),
        QueuedEvent(9_900, 4, "spot_depth", parse_spot_depth_payload(spot_depth())),
        QueuedEvent(9_900, 6, "futures_depth", parse_futures_depth_payload(futures_depth())),
        QueuedEvent(9_900, 7, "spot_trade", {
            "s": "BTCUSDT", "e": "aggTrade", "p": "101", "q": "2", "m": False,
            "a": 1, "f": 5, "l": 6, "T": 9_880,
        }),
        QueuedEvent(9_900, 8, "futures_trade", {
            "s": "BTCUSDT", "st": 1, "e": "aggTrade", "p": "100", "q": "1.5",
            "nq": "1", "m": True, "a": 2, "f": 10, "l": 10, "T": 9_870,
        }),
    ]
    row = finalize_boundary(state, pending, 10_000, sample_jitter_ms=12)

    assert list(row) == list(MICROSTRUCTURE_COLUMNS)
    assert tuple(row)[1:] == MICROSTRUCTURE_VALUE_COLUMNS
    assert row["sample_second_ms"] == 9_000
    assert row["schema_version"] == 1
    assert row["spot_mid"] == D("100")
    assert row["fut_mid"] == D("101")
    assert row["spot_buy_usdt"] == D("202")
    assert row["fut_sell_usdt"] == D("150.0")
    assert row["fut_rpi_sell_usdt"] == D("50.0")
    assert row["spot_snapshot_bbo_ofi_usdt"] == D("100")
    assert row["spot_imbalance_10"] is not None
    assert row["spot_bid_depth_usdt_10"] == sum(
        (price * quantity for price, quantity in state.spot_book.bids), D("0")
    )
    assert row["long_liq_usdt"] == D("201.0")
    assert row["perp_spot_basis_bps"] == D("100")
    assert row["mark_index_basis_bps"] == (D("101.2") / D("101") - D("1")) * D("10000")
    assert row["open_interest_usdt"] == D("1234.5") * D("101")
    assert row["oi_http_lag_ms"] == 100
    assert row["sample_span_ms"] is None
    assert row["collector_healthy"] is False
    assert len(pending) == 1
    assert pending[0].received_ms == 10_000

    second = finalize_boundary(state, pending, 11_000)
    assert second["sample_second_ms"] == 10_000
    assert second["spot_buy_usdt"] == D("206")
    assert second["sample_span_ms"] == 1_000
    assert second["collector_healthy"] is True


def test_connection_close_marks_gap_and_unhealthy() -> None:
    state = CollectorState()
    for source in state.connections:
        state.connection_opened(source, 1_000)
    state.connection_closed("futures_depth", 5_000)
    assert state.connection_errors == 1
    assert state.unhealthy_until_ms == 10_000
    assert state.connections["futures_depth"] is False


def test_connection_open_cools_down_partial_post_connect_intervals() -> None:
    state = CollectorState()

    state.connection_opened("spot", 2_500)

    assert state.connections["spot"] is True
    assert state.connection_errors == 0
    assert state.unhealthy_until_ms == 7_500


def test_book_ofi_does_not_compare_across_disconnect() -> None:
    state = CollectorState()
    state.connection_opened("spot", 9_000)
    state.handle_spot_depth(
        parse_spot_depth_payload(spot_depth(bid_quantity="4")),
        received_ms=9_100,
    )
    state.spot_book.snapshot("spot", 9_200)

    state.connection_closed("spot", 9_300)
    state.connection_opened("spot", 9_400)
    state.handle_spot_depth(
        parse_spot_depth_payload(spot_depth(bid_quantity="9")),
        received_ms=9_500,
    )

    row = state.spot_book.snapshot("spot", 10_000)
    assert row["spot_snapshot_bbo_ofi_usdt"] == D("0")
    assert row["spot_book_snapshot_count"] == 1


def test_partial_context_does_not_refresh_complete_mark_pair_age() -> None:
    state = CollectorState()
    state.update_context(
        SimpleNamespace(
            symbol="BTCUSDT",
            received_ms=9_000,
            mark_price=D("101"),
            index_price=D("100"),
            last_funding_rate=D("0.0001"),
            next_funding_time_ms=20_000,
            premium_index_time_ms=8_950,
            open_interest=D("10"),
            open_interest_time_ms=8_950,
            request_started_ms=8_900,
        )
    )
    state.update_context(
        SimpleNamespace(
            symbol="BTCUSDT",
            received_ms=9_900,
            mark_price=None,
            index_price=D("100"),
            last_funding_rate=D("0.0002"),
            next_funding_time_ms=21_000,
            premium_index_time_ms=9_850,
            open_interest=None,
            open_interest_time_ms=None,
            request_started_ms=9_800,
        )
    )

    assert state.mark_price == D("101")
    assert state.index_price == D("100")
    assert state.mark_received_ms == 9_000
    assert state.mark_lag_ms == 50


def test_finalize_boundary_drops_events_before_interval_and_marks_gap() -> None:
    state = CollectorState()
    pending = [
        QueuedEvent(
            1_000,
            0,
            "spot_trade",
            {
                "s": "BTCUSDT",
                "e": "aggTrade",
                "p": "100",
                "q": "2",
                "m": False,
                "a": 1,
                "f": 1,
                "l": 1,
                "T": 999,
            },
        )
    ]

    row = finalize_boundary(state, pending, 3_000)

    assert row["sample_second_ms"] == 2_000
    assert row["spot_buy_usdt"] == D("0")
    assert row["connection_errors"] == 1
    assert row["collector_healthy"] is False
    assert pending == []


def test_stale_connection_marker_preserves_current_socket_state() -> None:
    state = CollectorState()
    pending = [QueuedEvent(1_000, 0, "connection_opened", "spot")]

    row = finalize_boundary(state, pending, 3_000)

    assert state.connections["spot"] is True
    assert row["connection_errors"] == 1
    assert row["collector_healthy"] is False


def test_event_sink_is_nonblocking_and_assigns_stable_sequence() -> None:
    async def scenario() -> None:
        queue = asyncio.Queue(maxsize=2)
        sink = MicrostructureEventSink(queue)
        assert sink.connection_opened("spot", 1_000)
        assert sink.offer_spot_depth(spot_depth(), 1_001)
        assert not sink.record_error("spot", 1_002)
        assert sink.dropped_events == 1
        first = queue.get_nowait()
        second = queue.get_nowait()
        assert (first.sequence, second.sequence) == (0, 1)
        assert first.kind == "connection_opened"
        assert second.kind == "spot_depth"
        # Once consumers make room, the loss marker is causally inserted before
        # the next ordinary event so CollectorState cannot report false health.
        assert sink.connection_opened("futures_trade", 1_003)
        overflow_marker = queue.get_nowait()
        recovered_event = queue.get_nowait()
        assert overflow_marker.kind == "error"
        assert overflow_marker.received_ms == 1_002
        assert recovered_event.kind == "connection_opened"
        state = CollectorState()
        state.apply_event(overflow_marker)
        assert state.connection_errors == 1
        assert state.unhealthy_until_ms == 6_002

    asyncio.run(scenario())
