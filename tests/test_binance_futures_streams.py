from decimal import Decimal

import pytest

import price_collector.binance_futures_streams as streams


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
