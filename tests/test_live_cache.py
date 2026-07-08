import asyncio
from decimal import Decimal

from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LiveCache,
    build_current_live_payload,
)
from price_collector.market import MarketWindow


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.mget_calls = []

    async def set(self, key, value):
        self.data[key] = value

    async def get(self, key):
        return self.data.get(key)

    async def mget(self, keys):
        self.mget_calls.append(list(keys))
        return [self.data.get(key) for key in keys]


def test_live_cache_set_price_stores_requested_json_shape():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)

    asyncio.run(
        cache.set_price(
            BINANCE_SPOT_LIVE_KEY,
            value=Decimal("62067.89000000"),
            source_timestamp_ms=123,
            received_ms=456,
        )
    )

    assert redis.data[BINANCE_SPOT_LIVE_KEY] == (
        '{"value":"62067.89000000","source_timestamp_ms":123,"received_ms":456}'
    )


def test_build_current_live_payload_calculates_freshness_ages():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    window = MarketWindow(
        market_id=5_944_864,
        market_start_ms=1_783_459_200_000,
        market_end_ms=1_783_459_500_000,
    )

    async def run():
        await cache.set_price(
            BINANCE_SPOT_LIVE_KEY,
            value=Decimal("62067.89"),
            source_timestamp_ms=1_783_459_250_000,
            received_ms=1_783_459_250_050,
        )
        await cache.set_price(
            CHAINLINK_LIVE_KEY,
            value=Decimal("62066.12"),
            source_timestamp_ms=1_783_459_249_900,
            received_ms=1_783_459_250_075,
        )
        await cache.set_price(
            FUTURES_LIVE_KEY,
            value=Decimal("62070.11"),
            source_timestamp_ms=1_783_459_249_950,
            received_ms=1_783_459_250_090,
        )
        return await build_current_live_payload(
            cache,
            window=window,
            server_time_ms=1_783_459_250_123,
        )

    payload = asyncio.run(run())

    assert redis.mget_calls == [
        [BINANCE_SPOT_LIVE_KEY, CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]
    ]
    assert payload["prices"]["binance_spot"]["value"] == "62067.89"
    assert payload["prices"]["binance_spot"]["source_age_ms"] == 123
    assert payload["prices"]["binance_spot"]["received_age_ms"] == 73
    assert payload["prices"]["chainlink"]["source_age_ms"] == 223
    assert payload["prices"]["chainlink"]["received_age_ms"] == 48
    assert payload["futures"]["last"]["source_age_ms"] == 173
    assert payload["futures"]["last"]["received_age_ms"] == 33
    assert payload["futures"]["last"]["time_ms"] == 1_783_459_249_950
