import asyncio
import json
from decimal import Decimal

import pytest

import price_collector.live_cache as live_cache_module
from price_collector.binance_microstructure import (
    MICROSTRUCTURE_COLUMNS,
    CollectorState,
)
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    MICROSTRUCTURE_LIVE_KEY,
    LiveCache,
    LiveCachePayloadError,
    LivePrice,
    build_current_live_payload,
    decode_microstructure_snapshot,
    decode_live_price,
    encode_microstructure_snapshot,
)
from price_collector.market import MarketWindow


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.mget_calls = []
        self.set_calls = []
        self.closed = False

    async def set(self, key, value, **options):
        self.set_calls.append((key, value, options))
        self.data[key] = value

    async def get(self, key):
        return self.data.get(key)

    async def mget(self, keys):
        self.mget_calls.append(list(keys))
        return [self.data.get(key) for key in keys]

    async def aclose(self):
        self.closed = True


def microstructure_cache_row(**updates):
    row = CollectorState().snapshot(
        1_783_459_500_000,
        now_ms=1_783_459_501_000,
        sample_jitter_ms=250,
    )
    row.update(updates)
    return row


def test_live_cache_set_price_stores_exact_source_price_shape():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)

    asyncio.run(
        cache.set_price(
            CHAINLINK_LIVE_KEY,
            value=Decimal("62067.89000000"),
            source_timestamp_ms=123,
            received_ms=456,
        )
    )

    assert redis.data[CHAINLINK_LIVE_KEY] == (
        '{"value":"62067.89000000","source_timestamp_ms":123,'
        '"received_ms":456}'
    )
    assert json.loads(redis.data[CHAINLINK_LIVE_KEY]) == {
        "value": "62067.89000000",
        "source_timestamp_ms": 123,
        "received_ms": 456,
    }
    assert asyncio.run(cache.get_price(CHAINLINK_LIVE_KEY)) == LivePrice(
        value="62067.89000000",
        source_timestamp_ms=123,
        received_ms=456,
    )


def test_live_price_decoder_accepts_null_source_timestamp():
    assert decode_live_price(
        '{"value":"1","source_timestamp_ms":null,"received_ms":2}'
    ) == LivePrice(value="1", source_timestamp_ms=None, received_ms=2)
    assert decode_live_price(None) is None


def test_microstructure_snapshot_preserves_decimal_strings_nulls_and_types():
    row = microstructure_cache_row(
        spot_mid=Decimal("62000.123456789012345678"),
        fut_rpi_buy_usdt=None,
        spot_book_age_ms=125,
    )

    encoded = encode_microstructure_snapshot(
        row=row,
        received_ms=1_783_459_501_250,
    )

    encoded_payload = json.loads(encoded)
    assert set(encoded_payload) == {
        *MICROSTRUCTURE_COLUMNS,
        "received_ms",
    }
    assert encoded_payload["sample_second_ms"] == 1_783_459_500_000
    assert encoded_payload["schema_version"] == 1
    assert encoded_payload["collector_healthy"] is False
    assert encoded_payload["spot_mid"] == "62000.123456789012345678"
    assert encoded_payload["spot_buy_usdt"] == "0"
    assert encoded_payload["fut_rpi_buy_usdt"] is None
    assert encoded_payload["spot_book_age_ms"] == 125
    assert encoded_payload["received_ms"] == 1_783_459_501_250

    decoded = decode_microstructure_snapshot(encoded)
    assert set(decoded) == {*MICROSTRUCTURE_COLUMNS, "received_ms"}
    assert decoded["spot_mid"] == "62000.123456789012345678"
    assert decoded["spot_buy_usdt"] == "0"
    assert decoded["fut_rpi_buy_usdt"] is None
    assert decoded["spot_book_age_ms"] == 125
    assert decode_microstructure_snapshot(None) is None


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    (
        (
            "spot_mid",
            1.25,
            "numeric values must be integers or strings",
        ),
        (
            "spot_mid",
            "nope",
            "must contain a decimal string",
        ),
        (
            "spot_mid",
            1,
            "must be a decimal string",
        ),
        (
            "spot_book_age_ms",
            "125",
            "must be an integer or null",
        ),
        (
            "spot_book_age_ms",
            True,
            "must be an integer or null",
        ),
        (
            "spot_book_snapshot_count",
            None,
            "must be an integer",
        ),
        (
            "spot_buy_usdt",
            None,
            "must be a decimal string",
        ),
        (
            "collector_healthy",
            1,
            "must be a boolean",
        ),
        (
            "schema_version",
            2,
            "schema_version is unsupported",
        ),
        (
            "sample_second_ms",
            1_783_459_500_001,
            "must be UTC-second aligned",
        ),
    ),
)
def test_microstructure_snapshot_decoder_rejects_unsafe_payloads(
    field_name,
    value,
    message,
):
    payload = json.loads(
        encode_microstructure_snapshot(
            row=microstructure_cache_row(),
            received_ms=1_783_459_501_250,
        )
    )
    payload[field_name] = value

    with pytest.raises(LiveCachePayloadError, match=message):
        decode_microstructure_snapshot(json.dumps(payload))


@pytest.mark.parametrize("mutation", ("missing", "unknown"))
def test_microstructure_snapshot_decoder_requires_exact_fields(mutation):
    payload = json.loads(
        encode_microstructure_snapshot(
            row=microstructure_cache_row(),
            received_ms=1_783_459_501_250,
        )
    )
    if mutation == "missing":
        payload.pop("spot_mid")
    else:
        payload["unexpected"] = None

    with pytest.raises(LiveCachePayloadError, match="fields do not match"):
        decode_microstructure_snapshot(json.dumps(payload))


def test_microstructure_snapshot_decoder_rejects_duplicate_fields():
    encoded = encode_microstructure_snapshot(
        row=microstructure_cache_row(),
        received_ms=1_783_459_501_250,
    )
    duplicated = encoded[:-1] + ',"received_ms":1783459501251}'

    with pytest.raises(LiveCachePayloadError, match="is duplicated"):
        decode_microstructure_snapshot(duplicated)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    (
        ("spot_mid", "62000.10", "must be a Decimal"),
        ("spot_mid", 62_000, "must be a Decimal"),
        ("spot_buy_usdt", None, "must be a Decimal"),
        ("spot_book_age_ms", Decimal("125"), "must be an integer or null"),
        ("spot_book_snapshot_count", None, "must be an integer"),
        ("collector_healthy", 1, "must be a boolean"),
        ("schema_version", 2, "schema_version is unsupported"),
        (
            "sample_second_ms",
            1_783_459_500_001,
            "must be UTC-second aligned",
        ),
    ),
)
def test_microstructure_snapshot_encoder_enforces_field_types(
    field_name,
    value,
    message,
):
    row = microstructure_cache_row(**{field_name: value})

    with pytest.raises(LiveCachePayloadError, match=message):
        encode_microstructure_snapshot(
            row=row,
            received_ms=1_783_459_501_250,
        )


@pytest.mark.parametrize("mutation", ("missing", "unknown"))
def test_microstructure_snapshot_encoder_requires_exact_fields(mutation):
    row = microstructure_cache_row()
    if mutation == "missing":
        row.pop("spot_mid")
    else:
        row["unexpected"] = None

    with pytest.raises(LiveCachePayloadError, match="fields do not match"):
        encode_microstructure_snapshot(
            row=row,
            received_ms=1_783_459_501_250,
        )


@pytest.mark.parametrize(
    ("raw", "message"),
    (
        ('{"value":12,"received_ms":2}', "value must be a string"),
        (
            '{"value":"not-a-decimal","received_ms":2}',
            "value must be a finite decimal string",
        ),
        (
            '{"value":"NaN","received_ms":2}',
            "value must be a finite decimal string",
        ),
        ('{"value":"1"}', "received_ms is required"),
        (
            '{"value":"1","source_timestamp_ms":true,"received_ms":2}',
            "source_timestamp_ms must be an integer timestamp",
        ),
        ("{not-json", "not valid JSON"),
        (b"\xff", "not valid UTF-8"),
    ),
)
def test_live_price_decoder_rejects_malformed_payloads(raw, message):
    with pytest.raises(LiveCachePayloadError, match=message):
        decode_live_price(raw)


@pytest.mark.parametrize("value", ("not-decimal", Decimal("NaN")))
def test_live_price_encoder_rejects_non_decimal_or_non_finite_values(value):
    with pytest.raises(LiveCachePayloadError, match="finite Decimal"):
        live_cache_module.encode_live_price(
            value=value,
            source_timestamp_ms=1,
            received_ms=2,
        )


def test_get_prices_uses_one_ordered_mget():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)

    async def run():
        await cache.set_price(
            CHAINLINK_LIVE_KEY,
            value=Decimal("101.25"),
            source_timestamp_ms=123,
            received_ms=456,
        )
        return await cache.get_prices(
            [BINANCE_SPOT_LIVE_KEY, CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]
        )

    prices = asyncio.run(run())

    assert redis.mget_calls == [
        [BINANCE_SPOT_LIVE_KEY, CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]
    ]
    assert prices[BINANCE_SPOT_LIVE_KEY] is None
    assert prices[CHAINLINK_LIVE_KEY].value == "101.25"
    assert prices[FUTURES_LIVE_KEY] is None


def test_get_prices_with_microstructure_uses_one_ordered_mget():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)

    async def run():
        await cache.set_price(
            BINANCE_SPOT_LIVE_KEY,
            value=Decimal("62000.10"),
            source_timestamp_ms=1_001,
            received_ms=1_002,
        )
        await cache.set_microstructure_snapshot(
            MICROSTRUCTURE_LIVE_KEY,
            row=microstructure_cache_row(
                sample_second_ms=1_000,
                collector_healthy=True,
                spot_mid=Decimal("62000.05"),
            ),
            received_ms=2_250,
        )
        return await cache.get_prices_with_microstructure(
            [BINANCE_SPOT_LIVE_KEY, CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]
        )

    prices, microstructure = asyncio.run(run())

    assert redis.mget_calls == [
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            FUTURES_LIVE_KEY,
            MICROSTRUCTURE_LIVE_KEY,
        ]
    ]
    assert prices[BINANCE_SPOT_LIVE_KEY].value == "62000.10"
    assert prices[CHAINLINK_LIVE_KEY] is None
    assert prices[FUTURES_LIVE_KEY] is None
    assert set(microstructure) == {*MICROSTRUCTURE_COLUMNS, "received_ms"}
    assert microstructure["sample_second_ms"] == 1_000
    assert microstructure["schema_version"] == 1
    assert microstructure["collector_healthy"] is True
    assert microstructure["spot_mid"] == "62000.05"
    assert microstructure["received_ms"] == 2_250


def test_get_prices_with_microstructure_rejects_non_decimal_price_payload():
    redis = FakeRedis()
    redis.data[BINANCE_SPOT_LIVE_KEY] = (
        '{"value":"not-a-decimal","source_timestamp_ms":1,"received_ms":2}'
    )
    cache = LiveCache(redis_client=redis)

    with pytest.raises(LiveCachePayloadError, match="finite decimal string"):
        asyncio.run(
            cache.get_prices_with_microstructure(
                [
                    BINANCE_SPOT_LIVE_KEY,
                    CHAINLINK_LIVE_KEY,
                    FUTURES_LIVE_KEY,
                ]
            )
        )


def test_build_current_live_payload_returns_only_source_prices_and_freshness():
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
    assert set(payload) == {
        "server_time_ms",
        "market_id",
        "market_start_ms",
        "market_end_ms",
        "prices",
        "futures",
    }
    assert payload["prices"]["binance_spot"] == {
        "value": "62067.89",
        "source_timestamp_ms": 1_783_459_250_000,
        "received_ms": 1_783_459_250_050,
        "source_age_ms": 123,
        "received_age_ms": 73,
        "provider_event_ms": 1_783_459_250_000,
    }
    assert payload["prices"]["chainlink"]["source_age_ms"] == 223
    assert payload["prices"]["chainlink"]["received_age_ms"] == 48
    assert payload["futures"]["last"]["source_age_ms"] == 173
    assert payload["futures"]["last"]["received_age_ms"] == 33
    assert payload["futures"]["last"]["time_ms"] == 1_783_459_249_950


def test_build_current_live_payload_serializes_missing_sources_as_nulls():
    payload = asyncio.run(
        build_current_live_payload(
            LiveCache(redis_client=FakeRedis()),
            window=MarketWindow(
                market_id=5_944_864,
                market_start_ms=1_783_459_200_000,
                market_end_ms=1_783_459_500_000,
            ),
            server_time_ms=1_783_459_250_123,
        )
    )

    assert payload["prices"]["chainlink"] == {
        "value": None,
        "source_timestamp_ms": None,
        "received_ms": None,
        "source_age_ms": None,
        "received_age_ms": None,
        "provider_event_ms": None,
    }
    assert payload["futures"]["last"]["value"] is None


def test_get_prices_rejects_short_mget_response():
    class ShortRedis(FakeRedis):
        async def mget(self, keys):
            values = await super().mget(keys)
            return values[:-1]

    cache = LiveCache(redis_client=ShortRedis())

    with pytest.raises(LiveCachePayloadError, match="unexpected value count"):
        asyncio.run(
            cache.get_prices(
                [BINANCE_SPOT_LIVE_KEY, CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]
            )
        )


def test_get_prices_propagates_redis_read_failure():
    class FailingRedis(FakeRedis):
        async def mget(self, keys):
            raise OSError("redis unavailable")

    cache = LiveCache(redis_client=FailingRedis())

    with pytest.raises(OSError, match="redis unavailable"):
        asyncio.run(cache.get_prices([CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]))


def test_default_redis_client_keeps_bytes_for_strict_decoding(monkeypatch):
    captured = {}
    redis_client = FakeRedis()

    def fake_redis(**kwargs):
        captured.update(kwargs)
        return redis_client

    monkeypatch.setattr(live_cache_module.redis, "Redis", fake_redis)

    cache = LiveCache()

    assert cache.redis is redis_client
    assert captured["decode_responses"] is False


def test_live_cache_closes_redis_client():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)

    asyncio.run(cache.close())

    assert redis.closed is True
