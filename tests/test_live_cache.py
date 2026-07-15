import asyncio
import json
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal

import pytest

import price_collector.live_cache as live_cache_module
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    CHAINLINK_SHADOW_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LiveCache,
    LiveCachePayloadError,
    LiveShadowSignal,
    build_current_live_payload,
    decode_shadow_signal,
    encode_shadow_signal,
    serialize_shadow_signal,
)
from price_collector.market import MarketWindow


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.mget_calls = []
        self.set_calls = []
        self.delete_calls = []

    async def set(self, key, value, **options):
        self.set_calls.append((key, value, options))
        self.data[key] = value

    async def get(self, key):
        return self.data.get(key)

    async def mget(self, keys):
        self.mget_calls.append(list(keys))
        return [self.data.get(key) for key in keys]

    async def delete(self, key):
        self.delete_calls.append(key)
        self.data.pop(key, None)


def valid_shadow_signal(**overrides):
    values = {
        "schema_version": 1,
        "mode": "shadow",
        "selection_schema_version": 2,
        "selection_policy_version": "chronological_holdout_v2",
        "selection_fingerprint_sha256": "a" * 64,
        "selection_artifact_sha256": "b" * 64,
        "selection_evidence_end_ms": 1_783_400_000_000,
        "model_version": "catchup_ratio_l3500_b100",
        "beta": Decimal("1"),
        "generated_ms": 1_783_459_495_125,
        "valid": True,
        "status": "valid",
        "invalid_reasons": (),
        "state": "anchored",
        "horizon_ms": 3_500,
        "estimated_lag_ms": 3_500,
        "current_chainlink": Decimal("100.00"),
        "projected_chainlink": Decimal("101.00"),
        "pending_move": Decimal("1.00"),
        "pending_move_bps": Decimal("100"),
        "direction": "up",
        "futures_now": Decimal("101.00"),
        "futures_reference": Decimal("100.00"),
        "chainlink_now_source_timestamp_ms": 1_783_459_495_000,
        "chainlink_now_received_ms": 1_783_459_495_070,
        "anchor_chainlink_source_timestamp_ms": 1_783_459_495_000,
        "anchor_chainlink_received_ms": 1_783_459_495_070,
        "futures_now_source_timestamp_ms": 1_783_459_495_000,
        "futures_now_received_ms": 1_783_459_495_102,
        "futures_reference_source_timestamp_ms": 1_783_459_491_500,
        "futures_reference_received_ms": 1_783_459_491_550,
        "futures_reference_target_ms": 1_783_459_491_570,
        "futures_reference_gap_ms": 20,
        "futures_received_age_ms": 23,
        "chainlink_received_age_ms": 55,
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "ms_to_market_end": 4_875,
        "full_horizon_before_market_end": True,
    }
    values.update(overrides)
    return LiveShadowSignal(**values)


def invalid_shadow_signal(**overrides):
    values = {
        "valid": False,
        "status": "futures_stale",
        "invalid_reasons": ("futures_stale",),
        "projected_chainlink": None,
        "pending_move": None,
        "pending_move_bps": None,
        "direction": None,
        "futures_reference": None,
        "anchor_chainlink_source_timestamp_ms": None,
        "anchor_chainlink_received_ms": None,
        "futures_reference_source_timestamp_ms": None,
        "futures_reference_received_ms": None,
        "futures_reference_target_ms": None,
        "futures_reference_gap_ms": None,
    }
    values.update(overrides)
    return replace(valid_shadow_signal(), **values)


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


def test_chainlink_live_price_sequence_metadata_round_trips_and_is_optional():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"

    asyncio.run(
        cache.set_price(
            CHAINLINK_LIVE_KEY,
            value=Decimal("62067.89000000"),
            source_timestamp_ms=123,
            received_ms=456,
            publisher_epoch=publisher_epoch,
            accepted_event_sequence=17,
        )
    )

    encoded = redis.data[CHAINLINK_LIVE_KEY]
    assert json.loads(encoded) == {
        "value": "62067.89000000",
        "source_timestamp_ms": 123,
        "received_ms": 456,
        "publisher_epoch": publisher_epoch,
        "accepted_event_sequence": 17,
    }
    decoded = asyncio.run(cache.get_price(CHAINLINK_LIVE_KEY))
    assert decoded is not None
    assert decoded.publisher_epoch == publisher_epoch
    assert decoded.accepted_event_sequence == 17

    legacy = live_cache_module.decode_live_price(
        '{"value":"1","source_timestamp_ms":null,"received_ms":2}'
    )
    assert legacy is not None
    assert legacy.publisher_epoch is None
    assert legacy.accepted_event_sequence is None


@pytest.mark.parametrize(
    "overrides",
    (
        {"publisher_epoch": "8b3f42da-8927-48f8-9c90-4f2ce84100d8"},
        {"accepted_event_sequence": 1},
        {
            "publisher_epoch": "not-a-uuid",
            "accepted_event_sequence": 1,
        },
        {
            "publisher_epoch": "8b3f42da-8927-48f8-9c90-4f2ce84100d8",
            "accepted_event_sequence": 0,
        },
    ),
)
def test_live_price_decoder_rejects_non_atomic_or_invalid_sequence_metadata(
    overrides,
):
    payload = {
        "value": "1",
        "source_timestamp_ms": 1,
        "received_ms": 2,
        **overrides,
    }
    with pytest.raises(LiveCachePayloadError):
        live_cache_module.decode_live_price(json.dumps(payload))


def test_shadow_signal_round_trip_is_typed_and_uses_decimal_strings():
    signal = valid_shadow_signal()

    encoded = encode_shadow_signal(signal)
    payload = json.loads(encoded)

    assert decode_shadow_signal(encoded) == signal
    assert payload["schema_version"] == 1
    assert payload["mode"] == "shadow"
    assert payload["selection_fingerprint_sha256"] == "a" * 64
    assert payload["selection_artifact_sha256"] == "b" * 64
    assert payload["model_version"] == "catchup_ratio_l3500_b100"
    for field_name in (
        "beta",
        "current_chainlink",
        "projected_chainlink",
        "pending_move",
        "pending_move_bps",
        "futures_now",
        "futures_reference",
    ):
        assert isinstance(payload[field_name], str)
    assert payload["current_chainlink"] == "100.00"
    assert payload["pending_move_bps"] == "100"
    assert decode_shadow_signal(None) is None

    with pytest.raises(FrozenInstanceError):
        signal.status = "futures_stale"


def test_invalid_shadow_signal_nulls_projection_and_anchor_but_keeps_inputs():
    signal = invalid_shadow_signal()

    payload = json.loads(encode_shadow_signal(signal))

    assert decode_shadow_signal(json.dumps(payload)) == signal
    assert payload["valid"] is False
    assert payload["status"] == "futures_stale"
    assert payload["invalid_reasons"] == ["futures_stale"]
    for field_name in (
        "projected_chainlink",
        "pending_move",
        "pending_move_bps",
        "direction",
        "futures_reference",
        "anchor_chainlink_source_timestamp_ms",
        "anchor_chainlink_received_ms",
        "futures_reference_source_timestamp_ms",
        "futures_reference_received_ms",
        "futures_reference_target_ms",
        "futures_reference_gap_ms",
    ):
        assert payload[field_name] is None
    assert payload["current_chainlink"] == "100.00"
    assert payload["chainlink_now_received_ms"] == 1_783_459_495_070
    assert payload["futures_now"] == "101.00"
    assert payload["futures_received_age_ms"] == 23


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda item: item.update(schema_version=2), "schema_version"),
        (lambda item: item.update(mode="live"), "mode must be shadow"),
        (lambda item: item.update(beta=1), "decimal string"),
        (lambda item: item.update(valid="true"), "valid must be a boolean"),
        (
            lambda item: item.update(selection_artifact_sha256="A" * 64),
            "lowercase SHA-256",
        ),
        (
            lambda item: item.update(projected_chainlink="102"),
            "pending_move is inconsistent",
        ),
        (
            lambda item: item.update(futures_received_age_ms=24),
            "received age is inconsistent",
        ),
        (
            lambda item: item.update(ms_to_market_end=4_874),
            "ms_to_market_end is inconsistent",
        ),
    ],
)
def test_shadow_signal_decoder_rejects_inconsistent_payloads(mutator, message):
    payload = json.loads(encode_shadow_signal(valid_shadow_signal()))
    mutator(payload)

    with pytest.raises(LiveCachePayloadError, match=message):
        decode_shadow_signal(json.dumps(payload))


def test_shadow_signal_decoder_rejects_missing_extra_duplicate_and_float_fields():
    encoded = encode_shadow_signal(valid_shadow_signal())
    payload = json.loads(encoded)

    missing = dict(payload)
    missing.pop("status")
    extra = dict(payload, unexpected=True)
    duplicate = '{"schema_version":1,' + encoded[1:]
    numeric_decimal = encoded.replace('"beta":"1"', '"beta":1.0')

    for raw in (
        json.dumps(missing),
        json.dumps(extra),
        duplicate,
        numeric_decimal,
    ):
        with pytest.raises(LiveCachePayloadError):
            decode_shadow_signal(raw)


def test_live_cache_shadow_methods_use_fixed_key_and_atomic_ttl():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    signal = valid_shadow_signal()

    async def run():
        await cache.set_shadow_signal(signal, 2_000)
        stored = await cache.get_shadow_signal()
        await cache.delete_shadow_signal()
        return stored

    stored = asyncio.run(run())

    assert stored == signal
    assert redis.set_calls[-1] == (
        CHAINLINK_SHADOW_LIVE_KEY,
        encode_shadow_signal(signal),
        {"px": 2_000},
    )
    assert redis.delete_calls == [CHAINLINK_SHADOW_LIVE_KEY]
    assert CHAINLINK_SHADOW_LIVE_KEY not in redis.data


@pytest.mark.parametrize("ttl_ms", [0, -1])
def test_shadow_signal_ttl_must_be_positive(ttl_ms):
    cache = LiveCache(redis_client=FakeRedis())

    with pytest.raises(ValueError, match="ttl_ms must be positive"):
        asyncio.run(cache.set_shadow_signal(valid_shadow_signal(), ttl_ms))


@pytest.mark.parametrize("ttl_ms", [True, "2000", Decimal("2000")])
def test_shadow_signal_ttl_must_be_an_integer(ttl_ms):
    cache = LiveCache(redis_client=FakeRedis())

    with pytest.raises(TypeError, match="ttl_ms must be an integer"):
        asyncio.run(cache.set_shadow_signal(valid_shadow_signal(), ttl_ms))


def test_get_prices_independent_isolates_malformed_live_price_payloads():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)

    async def run():
        await cache.set_price(
            FUTURES_LIVE_KEY,
            value=Decimal("101.25"),
            source_timestamp_ms=123,
            received_ms=456,
        )
        redis.data[CHAINLINK_LIVE_KEY] = '{"value":12}'
        return await cache.get_prices_independent(
            [FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY, BINANCE_SPOT_LIVE_KEY]
        )

    prices, errors = asyncio.run(run())

    assert prices[FUTURES_LIVE_KEY].value == "101.25"
    assert prices[CHAINLINK_LIVE_KEY] is None
    assert prices[BINANCE_SPOT_LIVE_KEY] is None
    assert set(errors) == {CHAINLINK_LIVE_KEY}
    assert isinstance(errors[CHAINLINK_LIVE_KEY], LiveCachePayloadError)


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
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            FUTURES_LIVE_KEY,
            CHAINLINK_SHADOW_LIVE_KEY,
        ]
    ]
    assert payload["prices"]["binance_spot"]["value"] == "62067.89"
    assert payload["prices"]["binance_spot"]["source_age_ms"] == 123
    assert payload["prices"]["binance_spot"]["received_age_ms"] == 73
    assert payload["prices"]["chainlink"]["source_age_ms"] == 223
    assert payload["prices"]["chainlink"]["received_age_ms"] == 48
    assert payload["futures"]["last"]["source_age_ms"] == 173
    assert payload["futures"]["last"]["received_age_ms"] == 33
    assert payload["futures"]["last"]["time_ms"] == 1_783_459_249_950
    assert payload["signals"] == {"chainlink_catchup": None}


def test_build_current_live_payload_serializes_shadow_in_same_mget():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    signal = valid_shadow_signal()
    window = MarketWindow(
        market_id=5_944_864,
        market_start_ms=1_783_459_200_000,
        market_end_ms=1_783_459_500_000,
    )

    async def run():
        await cache.set_shadow_signal(signal, 2_000)
        return await build_current_live_payload(
            cache,
            window=window,
            server_time_ms=1_783_459_495_200,
        )

    payload = asyncio.run(run())
    serialized = payload["signals"]["chainlink_catchup"]

    assert redis.mget_calls == [
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            FUTURES_LIVE_KEY,
            CHAINLINK_SHADOW_LIVE_KEY,
        ]
    ]
    assert set(serialized) == {
        *signal.__dataclass_fields__,
        "signal_age_ms",
    }
    assert serialized["signal_age_ms"] == 75
    assert serialized["invalid_reasons"] == []
    for field_name in (
        "beta",
        "current_chainlink",
        "projected_chainlink",
        "pending_move",
        "pending_move_bps",
        "futures_now",
        "futures_reference",
    ):
        assert isinstance(serialized[field_name], str)


def test_shadow_api_serialization_keeps_invalid_signal_and_clamps_age():
    serialized = serialize_shadow_signal(
        invalid_shadow_signal(),
        server_time_ms=0,
    )

    assert serialized["signal_age_ms"] == 0
    assert serialized["valid"] is False
    assert serialized["status"] == "futures_stale"
    assert serialized["invalid_reasons"] == ["futures_stale"]
    assert serialized["projected_chainlink"] is None
    assert serialized["pending_move"] is None


def test_malformed_shadow_is_logged_and_does_not_hide_prices(caplog):
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
            source_timestamp_ms=1_783_459_495_000,
            received_ms=1_783_459_495_050,
        )
        redis.data[CHAINLINK_SHADOW_LIVE_KEY] = "{not-json"
        return await build_current_live_payload(
            cache,
            window=window,
            server_time_ms=1_783_459_495_200,
        )

    payload = asyncio.run(run())

    assert payload["prices"]["binance_spot"]["value"] == "62067.89"
    assert payload["signals"]["chainlink_catchup"] is None
    assert "shadow_signal_live_cache_payload_invalid" in caplog.text
    assert "{not-json" not in caplog.text


def test_shadow_with_unencodable_text_is_isolated_from_prices(caplog):
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    payload = json.loads(encode_shadow_signal(valid_shadow_signal()))
    payload["model_version"] = "\ud800"
    redis.data[BINANCE_SPOT_LIVE_KEY] = (
        '{"value":"62067.89","source_timestamp_ms":1783459495000,'
        '"received_ms":1783459495050}'
    )
    redis.data[CHAINLINK_SHADOW_LIVE_KEY] = json.dumps(payload)

    prices, shadow_signal = asyncio.run(
        cache.get_prices_and_shadow_signal(
            [
                BINANCE_SPOT_LIVE_KEY,
                CHAINLINK_LIVE_KEY,
                FUTURES_LIVE_KEY,
            ]
        )
    )

    assert prices[BINANCE_SPOT_LIVE_KEY].value == "62067.89"
    assert shadow_signal is None
    assert "shadow_signal_live_cache_payload_invalid" in caplog.text
    assert "\\ud800" not in caplog.text


def test_non_utf8_shadow_bytes_are_isolated_per_mget_slot(caplog):
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    redis.data[BINANCE_SPOT_LIVE_KEY] = (
        b'{"value":"62067.89","source_timestamp_ms":1783459495000,'
        b'"received_ms":1783459495050}'
    )
    redis.data[CHAINLINK_SHADOW_LIVE_KEY] = b"\xff"

    prices, shadow_signal = asyncio.run(
        cache.get_prices_and_shadow_signal(
            [
                BINANCE_SPOT_LIVE_KEY,
                CHAINLINK_LIVE_KEY,
                FUTURES_LIVE_KEY,
            ]
        )
    )

    assert prices[BINANCE_SPOT_LIVE_KEY].value == "62067.89"
    assert shadow_signal is None
    assert "shadow_signal_live_cache_payload_invalid" in caplog.text


def test_shadow_payload_field_names_are_not_copied_to_logs(caplog):
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    redis.data[CHAINLINK_SHADOW_LIVE_KEY] = '{"secret-shadow-field":1}'

    _prices, shadow_signal = asyncio.run(
        cache.get_prices_and_shadow_signal(
            [
                BINANCE_SPOT_LIVE_KEY,
                CHAINLINK_LIVE_KEY,
                FUTURES_LIVE_KEY,
            ]
        )
    )

    assert shadow_signal is None
    assert "shadow_signal_live_cache_payload_invalid" in caplog.text
    assert "secret-shadow-field" not in caplog.text


def test_malformed_actual_price_still_fails_with_valid_shadow():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    redis.data[BINANCE_SPOT_LIVE_KEY] = '{"value":12}'
    redis.data[CHAINLINK_SHADOW_LIVE_KEY] = encode_shadow_signal(
        valid_shadow_signal()
    )

    with pytest.raises(LiveCachePayloadError, match="value must be a string"):
        asyncio.run(
            cache.get_prices_and_shadow_signal(
                [
                    BINANCE_SPOT_LIVE_KEY,
                    CHAINLINK_LIVE_KEY,
                    FUTURES_LIVE_KEY,
                ]
            )
        )


def test_non_utf8_actual_price_still_fails_closed():
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    redis.data[BINANCE_SPOT_LIVE_KEY] = b"\xff"
    redis.data[CHAINLINK_SHADOW_LIVE_KEY] = encode_shadow_signal(
        valid_shadow_signal()
    )

    with pytest.raises(LiveCachePayloadError, match="not valid UTF-8"):
        asyncio.run(
            cache.get_prices_and_shadow_signal(
                [
                    BINANCE_SPOT_LIVE_KEY,
                    CHAINLINK_LIVE_KEY,
                    FUTURES_LIVE_KEY,
                ]
            )
        )


def test_default_redis_client_keeps_bytes_for_per_slot_decoding(monkeypatch):
    captured = {}
    redis_client = FakeRedis()

    def fake_redis(**kwargs):
        captured.update(kwargs)
        return redis_client

    monkeypatch.setattr(live_cache_module.redis, "Redis", fake_redis)

    cache = LiveCache()

    assert cache.redis is redis_client
    assert captured["decode_responses"] is False


def test_live_snapshot_rejects_short_mget_response():
    class ShortRedis(FakeRedis):
        async def mget(self, keys):
            values = await super().mget(keys)
            return values[:-1]

    cache = LiveCache(redis_client=ShortRedis())

    with pytest.raises(LiveCachePayloadError, match="unexpected value count"):
        asyncio.run(
            cache.get_prices_and_shadow_signal(
                [
                    BINANCE_SPOT_LIVE_KEY,
                    CHAINLINK_LIVE_KEY,
                    FUTURES_LIVE_KEY,
                ]
            )
        )


def test_live_snapshot_propagates_redis_read_failure():
    class FailingRedis(FakeRedis):
        async def mget(self, keys):
            raise OSError("redis unavailable")

    cache = LiveCache(redis_client=FailingRedis())

    with pytest.raises(OSError, match="redis unavailable"):
        asyncio.run(
            cache.get_prices_and_shadow_signal(
                [
                    BINANCE_SPOT_LIVE_KEY,
                    CHAINLINK_LIVE_KEY,
                    FUTURES_LIVE_KEY,
                ]
            )
        )


def test_shadow_decoder_normalizes_size_depth_and_integer_limits():
    encoded = encode_shadow_signal(valid_shadow_signal())
    oversized = "x" * 65_537
    deeply_nested = "[" * 2_000 + "0" + "]" * 2_000
    huge_integer = encoded.replace(
        '"generated_ms":1783459495125',
        '"generated_ms":999999999999999999999999999999999999',
    )

    for raw in (oversized, deeply_nested, huge_integer):
        with pytest.raises(LiveCachePayloadError):
            decode_shadow_signal(raw)


def test_shadow_contract_rejects_oversized_strings_and_misaligned_market():
    payload = json.loads(encode_shadow_signal(valid_shadow_signal()))
    payload["model_version"] = "x" * 257
    with pytest.raises(LiveCachePayloadError, match="model_version is too long"):
        decode_shadow_signal(json.dumps(payload))

    payload = json.loads(encode_shadow_signal(invalid_shadow_signal()))
    payload["invalid_reasons"] = ["\ud800"]
    with pytest.raises(LiveCachePayloadError, match="valid Unicode"):
        decode_shadow_signal(json.dumps(payload))

    with pytest.raises(LiveCachePayloadError, match="must align"):
        replace(
            valid_shadow_signal(),
            market_start_ms=1_783_459_200_001,
            market_end_ms=1_783_459_500_001,
        )


def test_shadow_contract_clamps_input_age_after_local_clock_regression():
    signal = valid_shadow_signal()

    clock_regression = replace(
        signal,
        futures_now_received_ms=signal.generated_ms + 1,
        futures_received_age_ms=0,
    )

    assert decode_shadow_signal(encode_shadow_signal(clock_regression)) == (
        clock_regression
    )
