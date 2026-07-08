import asyncio
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import price_collector.polymarket_chainlink_collector as collector


def polymarket_settings():
    return SimpleNamespace(
        POLYMARKET_CHAINLINK_TOPIC="crypto_prices_chainlink",
        POLYMARKET_CHAINLINK_RTD_SYMBOL="btc/usd",
    )


def valid_message(**overrides):
    message = {
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": 1_783_459_200_456,
        "payload": {
            "symbol": "btc/usd",
            "value": Decimal("123456.780000000000000000"),
            "timestamp": 1_783_459_200_123,
        },
    }
    message.update(overrides)
    return message


def test_polymarket_subscription_uses_chainlink_topic_and_btc_usd_filter():
    subscription = collector.build_polymarket_chainlink_subscription(polymarket_settings())

    assert subscription["action"] == "subscribe"
    rtds_subscription = subscription["subscriptions"][0]
    assert rtds_subscription["topic"] == "crypto_prices_chainlink"
    assert rtds_subscription["topic"] != "crypto_prices"
    assert rtds_subscription["type"] == "*"
    assert rtds_subscription["filters"] == '{"symbol":"btc/usd"}'
    assert json.loads(rtds_subscription["filters"]) == {"symbol": "btc/usd"}
    assert "btcusdt" not in rtds_subscription["filters"]


def test_parse_polymarket_chainlink_message_uses_decimal_value_and_payload_timestamp():
    raw_message = """
    {
      "topic": "crypto_prices_chainlink",
      "type": "update",
      "timestamp": 1783459200456,
      "payload": {
        "symbol": "btc/usd",
        "value": 123456.780000000000000000,
        "timestamp": 1783459200123
      }
    }
    """
    message = json.loads(raw_message, parse_float=Decimal)

    tick = collector.parse_polymarket_chainlink_message(message)

    assert isinstance(message["payload"]["value"], Decimal)
    assert tick.symbol == "BTCUSD"
    assert tick.price == Decimal("123456.780000000000000000")
    assert tick.provider_event_ms == 1_783_459_200_123
    assert tick.provider_message_ms == 1_783_459_200_456


def test_parse_polymarket_chainlink_message_rejects_binance_sourced_rtds_topic():
    with pytest.raises(collector.RtdsParseError, match="unexpected RTDS topic"):
        collector.parse_polymarket_chainlink_message(valid_message(topic="crypto_prices"))


def test_parse_polymarket_chainlink_message_rejects_btcusdt_symbol():
    message = valid_message(
        payload={
            "symbol": "btcusdt",
            "value": Decimal("123456.78"),
            "timestamp": 1_783_459_200_123,
        }
    )

    with pytest.raises(collector.RtdsParseError, match="unexpected Chainlink symbol"):
        collector.parse_polymarket_chainlink_message(message)


def test_sample_second_uses_payload_timestamp_floored_to_second():
    assert collector.sample_second_ms_for_provider_event(1_783_459_200_999) == 1_783_459_200_000


def test_exact_five_minute_boundary_belongs_to_new_market_from_payload_timestamp():
    tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("123456.78"),
        provider_event_ms=1_783_459_500_000,
        provider_message_ms=1_783_459_500_010,
    )

    sample = collector.build_polymarket_chainlink_sample(
        tick,
        received_ms=1_783_459_500_020,
    )

    assert sample.sample_second_ms == 1_783_459_500_000
    assert sample.window.market_start_ms == 1_783_459_500_000
    assert sample.window.market_end_ms == 1_783_459_800_000


def test_duplicate_ticks_in_same_source_second_use_same_upsert_key(monkeypatch):
    calls = []

    async def fake_upsert_price_sample(pool, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(collector, "upsert_price_sample", fake_upsert_price_sample)

    first_tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("123456.78"),
        provider_event_ms=1_783_459_200_123,
        provider_message_ms=1_783_459_200_456,
    )
    second_tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("123457.01"),
        provider_event_ms=1_783_459_200_999,
        provider_message_ms=1_783_459_201_050,
    )

    asyncio.run(
        collector.handle_tick(
            "pool",
            42,
            first_tick,
            received_ms=1_783_459_200_500,
        )
    )
    asyncio.run(
        collector.handle_tick(
            "pool",
            42,
            second_tick,
            received_ms=1_783_459_201_100,
        )
    )

    assert calls[0]["instrument_id"] == 42
    assert calls[0]["sample_second_ms"] == 1_783_459_200_000
    assert calls[1]["sample_second_ms"] == 1_783_459_200_000
    assert calls[0]["source_price_field"] == "payload.value"
    assert calls[0]["source_topic"] == "crypto_prices_chainlink"
    assert calls[1]["price"] == Decimal("123457.01")


def test_rtds_ping_loop_sends_text_ping_every_five_seconds(monkeypatch):
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 1:
            raise asyncio.CancelledError

    class FakeWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, message):
            self.sent.append(message)

    fake_websocket = FakeWebSocket()
    monkeypatch.setattr(collector.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(collector.rtds_ping_loop(fake_websocket))

    assert sleeps == [5.0, 5.0]
    assert fake_websocket.sent == ["PING"]


def test_collector_module_does_not_connect_to_direct_chainlink_websocket():
    source = Path(collector.__file__).read_text()

    assert "wss://ws.dataengine.chain.link" not in source
    assert "ws.dataengine.chain.link" not in source
