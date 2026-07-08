import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from price_collector.collector import (
    LatestPrice,
    TickerParseError,
    build_pending_sample,
    is_latest_price_stale,
    parse_binance_ticker_payload,
    require_collector_database_url,
    sample_second_ms_for_now,
    update_binance_live_cache,
)
from price_collector.live_cache import BINANCE_SPOT_LIVE_KEY


def test_parse_binance_ticker_payload_uses_decimal_price_and_event_time():
    ticker = parse_binance_ticker_payload(
        {
            "s": "BTCUSDT",
            "c": "12345.67000000",
            "E": 1783459199876,
        },
        expected_symbol="BTCUSDT",
    )

    assert ticker.symbol == "BTCUSDT"
    assert ticker.price == Decimal("12345.67000000")
    assert ticker.provider_event_ms == 1783459199876


@pytest.mark.parametrize(
    "payload",
    [
        {"s": "BTCUSDT", "E": 1783459199876},
        {"s": "BTCUSDT", "c": "not-a-number", "E": 1783459199876},
        {"s": "BTCUSDT", "c": "0", "E": 1783459199876},
        {"s": "BTCUSDT", "c": 12345, "E": 1783459199876},
        {"s": "BTCUSDT", "c": "NaN", "E": 1783459199876},
        {"s": "BTCUSDT", "c": "Infinity", "E": 1783459199876},
        {"s": "BTCUSDT", "c": "-Infinity", "E": 1783459199876},
        {"s": "BTCUSDT", "c": "-1", "E": 1783459199876},
    ],
)
def test_parse_binance_ticker_payload_invalid_price_fails_cleanly(payload):
    with pytest.raises(TickerParseError):
        parse_binance_ticker_payload(payload, expected_symbol="BTCUSDT")


def test_parse_binance_ticker_payload_rejects_unexpected_symbol():
    with pytest.raises(TickerParseError, match="unexpected Binance ticker symbol"):
        parse_binance_ticker_payload(
            {
                "s": "ETHUSDT",
                "c": "12345.67000000",
                "E": 1783459199876,
            },
            expected_symbol="BTCUSDT",
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"s": "BTCUSDT", "c": "12345.67", "E": 0},
        {"s": "BTCUSDT", "c": "12345.67", "E": -1},
        {"s": "BTCUSDT", "c": "12345.67", "E": "not-an-int"},
    ],
)
def test_parse_binance_ticker_payload_invalid_event_time_fails_cleanly(payload):
    with pytest.raises(TickerParseError):
        parse_binance_ticker_payload(payload, expected_symbol="BTCUSDT")


def test_stale_price_decision_uses_strict_greater_than_threshold():
    latest = LatestPrice(
        symbol="BTCUSDT",
        price=Decimal("12345.67000000"),
        provider_event_ms=1000,
        received_ms=1_000,
    )

    assert not is_latest_price_stale(latest, now_ms=11_000, stale_price_ms=10_000)
    assert is_latest_price_stale(latest, now_ms=11_001, stale_price_ms=10_000)


def test_sample_second_ms_is_floored_to_whole_utc_second():
    assert sample_second_ms_for_now(1_783_459_199_876) == 1_783_459_199_000


def test_build_pending_sample_skips_missing_latest_price():
    assert build_pending_sample(latest=None, now_ms=2_000, stale_price_ms=10_000) is None


def test_build_pending_sample_skips_stale_latest_price():
    latest = LatestPrice(
        symbol="BTCUSDT",
        price=Decimal("12345.67000000"),
        provider_event_ms=1_000,
        received_ms=1_000,
    )

    assert build_pending_sample(
        latest=latest,
        now_ms=11_001,
        stale_price_ms=10_000,
    ) is None


def test_build_pending_sample_uses_market_helper_for_fresh_price():
    latest = LatestPrice(
        symbol="BTCUSDT",
        price=Decimal("12345.67000000"),
        provider_event_ms=1_783_459_199_876,
        received_ms=1_783_459_199_900,
    )

    sample = build_pending_sample(
        latest=latest,
        now_ms=1_783_459_200_123,
        stale_price_ms=10_000,
    )

    assert sample is not None
    assert sample.sample_second_ms == 1_783_459_200_000
    assert sample.price == Decimal("12345.67000000")
    assert sample.window.market_start_ms == 1_783_459_200_000


def test_binance_websocket_update_writes_redis_live_cache():
    calls = []

    class FakeLiveCache:
        async def set_price(
            self,
            key,
            *,
            value,
            source_timestamp_ms,
            received_ms,
        ):
            calls.append(
                {
                    "key": key,
                    "value": value,
                    "source_timestamp_ms": source_timestamp_ms,
                    "received_ms": received_ms,
                }
            )

    latest = LatestPrice(
        symbol="BTCUSDT",
        price=Decimal("62067.89000000"),
        provider_event_ms=1_783_459_250_123,
        received_ms=1_783_459_250_150,
    )

    written = asyncio.run(update_binance_live_cache(FakeLiveCache(), latest))

    assert written is True
    assert calls == [
        {
            "key": BINANCE_SPOT_LIVE_KEY,
            "value": Decimal("62067.89000000"),
            "source_timestamp_ms": 1_783_459_250_123,
            "received_ms": 1_783_459_250_150,
        }
    ]


def test_collector_requires_database_url_before_startup_connects():
    settings = SimpleNamespace(DATABASE_URL=None)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set for the collector"):
        require_collector_database_url(settings)
