import asyncio
import json
import logging
from dataclasses import replace
from decimal import Decimal, localcontext
from types import SimpleNamespace

import pytest
from redis.exceptions import RedisError

import price_collector.shadow_signal_2s_live as live_module
from price_collector.shadow_signal_2s_live import (
    SHADOW_SIGNAL_2S_BETA,
    SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
    SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
    SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
    SHADOW_SIGNAL_2S_LIVE_KEY,
    SHADOW_SIGNAL_2S_MODE,
    SHADOW_SIGNAL_2S_MODEL_VERSION,
    SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
    SHADOW_SIGNAL_2S_SCHEMA_VERSION,
    LiveShadowSignal2s,
    PayloadError,
    ShadowSignal2sStore,
    create_shadow_signal_2s_store,
    decode_shadow_signal_2s,
    encode_shadow_signal_2s,
    serialize_shadow_signal_2s,
)


MARKET_START_MS = 1_800_000_000_000
GENERATED_MS = MARKET_START_MS + 100_000


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.set_calls = []
        self.get_calls = []
        self.delete_calls = []
        self.closed = False

    async def set(self, key, value, **options):
        self.set_calls.append((key, value, options))
        self.data[key] = value

    async def get(self, key):
        self.get_calls.append(key)
        return self.data.get(key)

    async def delete(self, key):
        self.delete_calls.append(key)
        self.data.pop(key, None)

    async def aclose(self):
        self.closed = True


def valid_signal(**overrides):
    values = {
        "schema_version": SHADOW_SIGNAL_2S_SCHEMA_VERSION,
        "mode": SHADOW_SIGNAL_2S_MODE,
        "publication_role": SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
        "experiment_version": SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
        "model_version": SHADOW_SIGNAL_2S_MODEL_VERSION,
        "beta": SHADOW_SIGNAL_2S_BETA,
        "futures_lookback_ms": SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
        "forecast_horizon_ms": SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
        "generated_ms": GENERATED_MS,
        "target_ms": GENERATED_MS + SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
        "valid": True,
        "status": "valid",
        "invalid_reasons": (),
        "state": "anchored",
        "current_chainlink": Decimal("50000.00"),
        "projected_chainlink": Decimal("50050.00000"),
        "pending_move": Decimal("50.00000"),
        "pending_move_bps": Decimal("10.000"),
        "direction": "up",
        "futures_now": Decimal("60060.00"),
        "futures_reference": Decimal("60000.00"),
        "chainlink_now_source_timestamp_ms": GENERATED_MS - 600,
        "chainlink_now_received_ms": GENERATED_MS - 500,
        "anchor_chainlink_source_timestamp_ms": GENERATED_MS - 600,
        "anchor_chainlink_received_ms": GENERATED_MS - 500,
        "futures_now_source_timestamp_ms": GENERATED_MS - 30,
        "futures_now_received_ms": GENERATED_MS - 20,
        "futures_reference_source_timestamp_ms": GENERATED_MS - 2_525,
        "futures_reference_received_ms": GENERATED_MS - 2_520,
        "futures_reference_target_ms": GENERATED_MS - 2_500,
        "futures_reference_gap_ms": 20,
        "futures_received_age_ms": 20,
        "chainlink_received_age_ms": 500,
        "market_id": MARKET_START_MS // 300_000,
        "market_start_ms": MARKET_START_MS,
        "market_end_ms": MARKET_START_MS + 300_000,
        "ms_to_market_end": 200_000,
        "full_horizon_before_market_end": True,
    }
    values.update(overrides)
    return LiveShadowSignal2s(**values)


def invalid_signal(**overrides):
    values = {
        "valid": False,
        "status": "reference_unavailable",
        "invalid_reasons": ("reference_unavailable",),
        "state": "warming",
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
    return replace(valid_signal(), **values)


def test_valid_signal_round_trips_with_decimal_strings_and_fixed_identity():
    signal = valid_signal()

    encoded = encode_shadow_signal_2s(signal)
    wire = json.loads(encoded)

    assert wire["schema_version"] == 1
    assert wire["mode"] == "shadow_candidate"
    assert wire["publication_role"] == "challenger"
    assert wire["experiment_version"] == "prospective_catchup_2s_basis_v2"
    assert wire["model_version"] == (
        "catchup_v2_l2000_h2000_b100_basis5m"
    )
    assert wire["beta"] == "1"
    assert wire["current_chainlink"] == "50000.00"
    assert wire["projected_chainlink"] == "50050.00000"
    assert wire["futures_lookback_ms"] == 2_000
    assert wire["forecast_horizon_ms"] == 2_000
    assert decode_shadow_signal_2s(encoded) == signal
    assert decode_shadow_signal_2s(encoded.encode("utf-8")) == signal
    assert decode_shadow_signal_2s(None) is None


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("schema_version", 2),
        ("mode", "shadow"),
        ("publication_role", "primary"),
        ("experiment_version", "prospective_catchup_2s_v2"),
        ("model_version", "catchup_v1_l3000_h3000_b100"),
        ("beta", Decimal("0.9")),
        ("futures_lookback_ms", 3_000),
        ("forecast_horizon_ms", 3_000),
    ],
)
def test_frozen_experiment_fields_cannot_be_changed(field_name, bad_value):
    with pytest.raises(PayloadError, match="frozen experiment"):
        valid_signal(**{field_name: bad_value})


def test_target_reference_and_market_invariants_are_enforced():
    with pytest.raises(PayloadError, match="target_ms is inconsistent"):
        valid_signal(target_ms=GENERATED_MS + 2_001)
    with pytest.raises(PayloadError, match="reference target is inconsistent"):
        valid_signal(futures_reference_target_ms=GENERATED_MS - 2_499)
    with pytest.raises(PayloadError, match="reference gap is inconsistent"):
        valid_signal(futures_reference_gap_ms=19)
    with pytest.raises(PayloadError, match="full_horizon"):
        valid_signal(full_horizon_before_market_end=False)


def test_projection_move_bps_direction_and_anchor_invariants_are_enforced():
    adjusted = valid_signal(
        projected_chainlink=Decimal("50049"),
        pending_move=Decimal("49"),
        pending_move_bps=Decimal("9.8"),
    )
    assert adjusted.projected_chainlink == Decimal("50049")

    with pytest.raises(PayloadError, match="pending_move is inconsistent"):
        valid_signal(projected_chainlink=Decimal("50049"))
    with pytest.raises(PayloadError, match="pending_move is inconsistent"):
        valid_signal(pending_move=Decimal("49"))
    with pytest.raises(PayloadError, match="pending_move_bps is inconsistent"):
        valid_signal(pending_move_bps=Decimal("9"))
    with pytest.raises(PayloadError, match="direction is inconsistent"):
        valid_signal(direction="down")
    with pytest.raises(PayloadError, match="active anchor"):
        valid_signal(anchor_chainlink_received_ms=GENERATED_MS - 501)


def test_validation_is_independent_of_the_ambient_decimal_context():
    with localcontext() as context:
        context.prec = 8
        signal = valid_signal()
        assert decode_shadow_signal_2s(encode_shadow_signal_2s(signal)) == signal


def test_current_input_age_is_clamped_and_consistent():
    future_received = GENERATED_MS + 5
    signal = valid_signal(
        futures_now_received_ms=future_received,
        futures_received_age_ms=0,
    )
    assert signal.futures_received_age_ms == 0
    with pytest.raises(PayloadError, match="received age is inconsistent"):
        valid_signal(futures_received_age_ms=21)


def test_invalid_signal_keeps_current_inputs_but_clears_projection_and_anchor():
    signal = invalid_signal()
    assert signal.current_chainlink == Decimal("50000.00")
    assert signal.futures_now == Decimal("60060.00")
    assert decode_shadow_signal_2s(encode_shadow_signal_2s(signal)) == signal

    with pytest.raises(PayloadError, match="projection and anchor fields"):
        invalid_signal(projected_chainlink=Decimal("50000"))
    with pytest.raises(PayloadError, match="non-valid status"):
        invalid_signal(invalid_reasons=())
    with pytest.raises(PayloadError, match="non-valid status"):
        invalid_signal(status="valid")


def test_decoder_requires_exact_duplicate_free_schema_and_decimal_strings():
    encoded = encode_shadow_signal_2s(valid_signal())
    wire = json.loads(encoded)

    missing = dict(wire)
    missing.pop("state")
    with pytest.raises(PayloadError, match="fields differ"):
        decode_shadow_signal_2s(json.dumps(missing))

    extra = dict(wire, secret="must-not-pass")
    with pytest.raises(PayloadError, match="fields differ"):
        decode_shadow_signal_2s(json.dumps(extra))

    duplicate = encoded.replace(
        '{"schema_version":1,',
        '{"schema_version":1,"schema_version":1,',
        1,
    )
    with pytest.raises(PayloadError, match="duplicate field"):
        decode_shadow_signal_2s(duplicate)

    numeric_beta = encoded.replace('"beta":"1"', '"beta":1.0', 1)
    with pytest.raises(PayloadError, match="floating-point"):
        decode_shadow_signal_2s(numeric_beta)

    numeric_price = encoded.replace(
        '"current_chainlink":"50000.00"',
        '"current_chainlink":50000',
        1,
    )
    with pytest.raises(PayloadError, match="fixed-point decimal string"):
        decode_shadow_signal_2s(numeric_price)

    nonfinite = encoded.replace('"beta":"1"', '"beta":NaN', 1)
    with pytest.raises(PayloadError, match="non-finite"):
        decode_shadow_signal_2s(nonfinite)


def test_decoder_revalidates_fixed_values_not_just_field_names():
    wire = json.loads(encode_shadow_signal_2s(valid_signal()))
    wire["publication_role"] = "primary"

    with pytest.raises(PayloadError, match="frozen experiment"):
        decode_shadow_signal_2s(json.dumps(wire, separators=(",", ":")))


def test_serialize_adds_clamped_age_without_changing_redis_schema():
    signal = valid_signal()

    payload = serialize_shadow_signal_2s(
        signal,
        server_time_ms=signal.generated_ms - 1,
    )

    assert payload is not None
    assert payload["signal_age_ms"] == 0
    assert "signal_age_ms" not in json.loads(encode_shadow_signal_2s(signal))
    assert serialize_shadow_signal_2s(None, server_time_ms=0) is None


def test_store_uses_separate_key_and_atomic_set_with_px_ttl():
    fake = FakeRedis()
    store = ShadowSignal2sStore(redis_client=fake)
    signal = valid_signal()

    asyncio.run(store.set_signal(signal, ttl_ms=1_750))

    assert fake.set_calls == [
        (
            SHADOW_SIGNAL_2S_LIVE_KEY,
            encode_shadow_signal_2s(signal),
            {"px": 1_750},
        )
    ]
    assert asyncio.run(store.get_signal()) == signal
    asyncio.run(store.delete_signal())
    assert fake.delete_calls == [SHADOW_SIGNAL_2S_LIVE_KEY]
    assert asyncio.run(store.get_signal()) is None
    asyncio.run(store.close())
    assert fake.closed is True


@pytest.mark.parametrize("ttl_ms", [True, "2000", 0, -1])
def test_store_rejects_invalid_ttl(ttl_ms):
    store = ShadowSignal2sStore(redis_client=FakeRedis())
    error = TypeError if isinstance(ttl_ms, (bool, str)) else ValueError
    with pytest.raises(error):
        asyncio.run(store.set_signal(valid_signal(), ttl_ms=ttl_ms))


def test_store_isolates_malformed_payload_and_rate_limits_safe_logs(caplog):
    fake = FakeRedis()
    secret_raw = '{"secret-shadow-field":"do-not-log"}'
    fake.data[SHADOW_SIGNAL_2S_LIVE_KEY] = secret_raw
    store = ShadowSignal2sStore(redis_client=fake)

    async def read_repeatedly():
        return [await store.get_signal() for _ in range(100)]

    with caplog.at_level(
        logging.WARNING,
        logger="price_collector.shadow_signal_2s_live",
    ):
        results = asyncio.run(read_repeatedly())

    matching = [
        record
        for record in caplog.records
        if record.message == "shadow_signal_2s_live_cache_payload_invalid"
    ]
    assert results == [None] * 100
    assert len(matching) == 2
    assert [record.occurrence for record in matching] == [1, 100]
    assert secret_raw not in caplog.text
    assert "secret-shadow-field" not in caplog.text


def test_store_does_not_swallow_transport_errors():
    class FailingRedis:
        async def get(self, key):
            raise RedisError("redis unavailable")

    store = ShadowSignal2sStore(redis_client=FailingRedis())
    with pytest.raises(RedisError, match="redis unavailable"):
        asyncio.run(store.get_signal())


def test_factory_uses_shared_redis_settings(monkeypatch):
    captured = {}
    fake = FakeRedis()

    def make_redis(**kwargs):
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(live_module.redis, "Redis", make_redis)
    settings = SimpleNamespace(
        REDIS_HOST="127.0.0.9",
        REDIS_PORT=6380,
        REDIS_DB=2,
        REDIS_SOCKET_TIMEOUT_SECONDS=0.5,
    )

    store = create_shadow_signal_2s_store(settings)

    assert isinstance(store, ShadowSignal2sStore)
    assert store.redis is fake
    assert captured == {
        "host": "127.0.0.9",
        "port": 6380,
        "db": 2,
        "decode_responses": False,
        "socket_connect_timeout": 0.5,
        "socket_timeout": 0.5,
    }
