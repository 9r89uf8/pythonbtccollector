import asyncio
from decimal import Decimal, localcontext
from pathlib import Path

import pytest
from pydantic import ValidationError

import price_collector.shadow_signal_2s_collector as collector
from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LiveCache,
    encode_live_price,
)
from price_collector.shadow_signal_2s_live import (
    SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
    SHADOW_SIGNAL_2S_LIVE_KEY,
    SHADOW_SIGNAL_2S_MODE,
    SHADOW_SIGNAL_2S_MODEL_VERSION,
    SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
    project_chainlink_2s,
)


BASE_MS = 1_800_000_000_000


class FakeRedis:
    def __init__(self, *, mget_error=None, on_mget=None):
        self.data = {}
        self.mget_calls = []
        self.mget_error = mget_error
        self.on_mget = on_mget
        self.closed = False

    async def mget(self, keys):
        self.mget_calls.append(list(keys))
        if self.mget_error is not None:
            raise self.mget_error
        if self.on_mget is not None:
            self.on_mget()
        return [self.data.get(key) for key in keys]

    async def aclose(self):
        self.closed = True


class FakeSignalStore:
    def __init__(self, *, set_error=None):
        self.set_error = set_error
        self.set_calls = []
        self.closed = False

    async def set_signal(self, signal, *, ttl_ms):
        self.set_calls.append((signal, ttl_ms))
        if self.set_error is not None:
            raise self.set_error

    async def close(self):
        self.closed = True


def set_price(
    redis,
    key,
    value,
    *,
    received_ms,
    source_timestamp_ms=None,
):
    redis.data[key] = encode_live_price(
        value=Decimal(value),
        source_timestamp_ms=(
            received_ms
            if source_timestamp_ms is None
            else source_timestamp_ms
        ),
        received_ms=received_ms,
    )


def make_worker(*, redis=None, store=None, now_ms=None, sleep=None):
    redis = redis or FakeRedis()
    store = store or FakeSignalStore()
    arguments = {
        "live_cache": LiveCache(redis_client=redis),
        "signal_store": store,
    }
    if now_ms is not None:
        arguments["now_ms"] = now_ms
    if sleep is not None:
        arguments["sleep"] = sleep
    return collector.ShadowSignal2sWorker(**arguments), redis, store


def run_once(worker, now_ms):
    return asyncio.run(worker.run_once(now_ms=now_ms))


def test_settings_are_disabled_by_default_and_reject_runtime_drift():
    settings = collector.ShadowSignal2sSettings(_env_file=None)

    assert settings.SHADOW_SIGNAL_2S_ENABLED is False
    assert settings.SHADOW_SIGNAL_2S_POLL_MS == 100
    assert settings.SHADOW_SIGNAL_2S_TTL_MS == 2_000
    assert settings.REDIS_HOST == "127.0.0.1"
    assert "DATABASE_URL" not in settings.__class__.model_fields
    assert "READ_DATABASE_URL" not in settings.__class__.model_fields

    with pytest.raises(ValidationError):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            SHADOW_SIGNAL_2S_POLL_MS=200,
        )
    with pytest.raises(ValidationError):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            SHADOW_SIGNAL_2S_TTL_MS=1_999,
        )


@pytest.mark.parametrize(
    ("now_ms", "expected_ms"),
    ((1_000, 100), (1_001, 99), (1_099, 1)),
)
def test_poll_boundary_is_strictly_future_and_epoch_aligned(
    now_ms,
    expected_ms,
):
    delay_ms = collector.milliseconds_until_next_poll_boundary(now_ms, 100)

    assert delay_ms == expected_ms
    assert (now_ms + delay_ms) % 100 == 0


def test_frozen_model_and_engine_assumptions_are_exact():
    worker, _, _ = make_worker()

    assert collector.SHADOW_SIGNAL_2S_MODEL.version == (
        "catchup_v1_l2000_h2000_b100"
    )
    assert collector.SHADOW_SIGNAL_2S_MODEL.lag_ms == 2_000
    assert collector.SHADOW_SIGNAL_2S_MODEL.beta == Decimal("1")
    assert worker.poll_ms == 100
    assert worker.ttl_ms == 2_000
    assert worker.engine.futures_stale_ms == 3_000
    assert worker.engine.chainlink_stale_ms == 2_500
    assert worker.engine.reference_max_gap_ms == 3_000
    assert worker.engine.history_retention_ms == 10_000
    assert worker.engine.max_future_skew_ms == 250

    with pytest.raises(ValueError, match="poll interval is frozen"):
        collector.ShadowSignal2sWorker(
            live_cache=worker.live_cache,
            signal_store=worker.signal_store,
            poll_ms=200,
        )
    with pytest.raises(ValueError, match="TTL is frozen"):
        collector.ShadowSignal2sWorker(
            live_cache=worker.live_cache,
            signal_store=worker.signal_store,
            ttl_ms=1_999,
        )


def test_tick_uses_one_ordered_mget_and_publishes_challenger_metadata():
    worker, redis, store = make_worker()

    payload = run_once(worker, BASE_MS)

    assert redis.mget_calls == [[FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]]
    assert len(store.set_calls) == 1
    assert store.set_calls[0] == (payload, 2_000)
    assert SHADOW_SIGNAL_2S_LIVE_KEY == "btc:live:chainlink_shadow_2s"
    assert payload.schema_version == 1
    assert payload.mode == SHADOW_SIGNAL_2S_MODE
    assert payload.publication_role == SHADOW_SIGNAL_2S_PUBLICATION_ROLE
    assert payload.experiment_version == SHADOW_SIGNAL_2S_EXPERIMENT_VERSION
    assert payload.model_version == SHADOW_SIGNAL_2S_MODEL_VERSION
    assert payload.futures_lookback_ms == 2_000
    assert payload.forecast_horizon_ms == 2_000
    assert payload.generated_ms == BASE_MS
    assert payload.target_ms == BASE_MS + 2_000
    assert payload.valid is False
    assert payload.projected_chainlink is None


def test_generated_time_is_stamped_after_redis_inputs_are_available():
    clock = [BASE_MS]
    redis = FakeRedis(on_mget=lambda: clock.__setitem__(0, BASE_MS + 7))
    worker, _, _ = make_worker(redis=redis, now_ms=lambda: clock[0])

    payload = asyncio.run(worker.run_once())

    assert payload.generated_ms == BASE_MS + 7
    assert payload.target_ms == BASE_MS + 2_007


def test_worker_warms_then_publishes_valid_and_overwrites_when_stale():
    worker, redis, store = make_worker()

    set_price(
        redis,
        FUTURES_LIVE_KEY,
        "60000",
        received_ms=BASE_MS - 2_000,
    )
    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=BASE_MS,
    )
    warming = run_once(worker, BASE_MS)

    set_price(
        redis,
        FUTURES_LIVE_KEY,
        "60060",
        received_ms=BASE_MS + 1_000,
    )
    waiting = run_once(worker, BASE_MS + 1_000)

    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=BASE_MS + 1_100,
    )
    valid = run_once(worker, BASE_MS + 1_100)

    assert warming.valid is False
    assert warming.state == "waiting_for_new_chainlink_anchor"
    assert waiting.valid is False
    assert waiting.state == "waiting_for_new_chainlink_anchor"
    assert valid.valid is True
    assert valid.status == "valid"
    assert valid.state == "anchored"
    assert valid.current_chainlink == Decimal("50000")
    assert valid.futures_reference == Decimal("60000")
    assert valid.futures_now == Decimal("60060")
    assert valid.projected_chainlink == Decimal("50050.000")
    assert valid.pending_move == Decimal("50.000")
    assert valid.target_ms == BASE_MS + 3_100

    stale = run_once(worker, BASE_MS + 4_101)

    assert stale.valid is False
    assert stale.projected_chainlink is None
    assert store.set_calls[-2][0] == valid
    assert store.set_calls[-1][0] == stale
    assert all(ttl_ms == 2_000 for _, ttl_ms in store.set_calls)


def test_projection_uses_the_frozen_28_digit_decimal_calculation():
    worker, redis, _ = make_worker()
    set_price(
        redis,
        FUTURES_LIVE_KEY,
        "60000",
        received_ms=BASE_MS - 2_000,
    )
    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=BASE_MS,
    )

    with localcontext() as context:
        context.prec = 8
        run_once(worker, BASE_MS)
        set_price(
            redis,
            FUTURES_LIVE_KEY,
            "60001",
            received_ms=BASE_MS + 1_000,
        )
        run_once(worker, BASE_MS + 1_000)
        set_price(
            redis,
            CHAINLINK_LIVE_KEY,
            "50000",
            received_ms=BASE_MS + 1_100,
        )
        payload = run_once(worker, BASE_MS + 1_100)

    expected = project_chainlink_2s(
        current_chainlink=Decimal("50000"),
        futures_now=Decimal("60001"),
        futures_reference=Decimal("60000"),
    )
    assert payload.valid is True
    assert payload.projected_chainlink == expected


def test_malformed_input_still_overwrites_with_an_invalid_payload():
    worker, redis, store = make_worker()
    set_price(
        redis,
        FUTURES_LIVE_KEY,
        "60000",
        received_ms=BASE_MS,
    )
    redis.data[CHAINLINK_LIVE_KEY] = (
        '{"value":"not-a-price","source_timestamp_ms":1800000000000,'
        '"received_ms":1800000000000}'
    )

    payload = run_once(worker, BASE_MS)

    assert payload.valid is False
    assert payload.status == "chainlink_unavailable"
    assert payload.current_chainlink is None
    assert payload.futures_now == Decimal("60000")
    assert store.set_calls == [(payload, 2_000)]


def test_redis_read_failure_skips_refresh_and_relies_on_key_expiry():
    worker, redis, store = make_worker(
        redis=FakeRedis(mget_error=OSError("redis unavailable"))
    )

    payload = run_once(worker, BASE_MS)

    assert payload is None
    assert redis.mget_calls == [[FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]]
    assert store.set_calls == []


def test_redis_write_failure_does_not_abort_the_tick():
    store = FakeSignalStore(set_error=OSError("redis unavailable"))
    worker, _, _ = make_worker(store=store)

    payload = run_once(worker, BASE_MS)

    assert payload is not None
    assert payload.valid is False
    assert store.set_calls == [(payload, 2_000)]


def test_disabled_collector_touches_no_runtime_dependency(monkeypatch):
    settings = collector.ShadowSignal2sSettings(_env_file=None)

    def forbidden(*args, **kwargs):
        raise AssertionError("disabled challenger touched Redis")

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=forbidden,
            signal_store_factory=forbidden,
        )
    )


def test_enabled_collector_closes_both_redis_clients(monkeypatch):
    settings = collector.ShadowSignal2sSettings(
        _env_file=None,
        SHADOW_SIGNAL_2S_ENABLED=True,
    )
    redis = FakeRedis()
    live_cache = LiveCache(redis_client=redis)
    store = FakeSignalStore()
    live_factory_calls = []
    store_factory_calls = []

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=lambda value: (
                live_factory_calls.append(value) or live_cache
            ),
            signal_store_factory=lambda value: (
                store_factory_calls.append(value) or store
            ),
            max_iterations=0,
        )
    )

    assert live_factory_calls == [settings]
    assert store_factory_calls == [settings]
    assert redis.closed is True
    assert store.closed is True


def test_runtime_has_no_artifact_database_or_accepted_key_dependency():
    source = Path(collector.__file__).read_text()

    assert "shadow_signal_artifact" not in source
    assert "price_collector.db" not in source
    assert "DATABASE_URL" not in source
    assert "READ_DATABASE_URL" not in source
    assert "CHAINLINK_SHADOW_LIVE_KEY" not in source
