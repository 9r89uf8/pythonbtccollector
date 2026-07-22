import asyncio
from decimal import Decimal, localcontext
import hashlib
import json
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


class FakeEvaluationScheduler:
    def __init__(
        self,
        *,
        matured=(),
        error=None,
        on_observe=None,
        **configuration,
    ):
        self.configuration = configuration
        self.matured = tuple(matured)
        self.error = error
        self.on_observe = on_observe
        self.calls = []
        self.observation_gap_count = 0
        self.chainlink_sequence_gap_count = 0
        self.chainlink_sequence_regression_count = 0
        self.chainlink_sequence_identity_mismatch_count = 0
        self.chainlink_publisher_epoch_change_count = 0
        self.chainlink_sequence_metadata_loss_count = 0
        self.chainlink_sequence_confirmation_timeout_count = 0

    def observe(self, observation, *, chainlink):
        self.calls.append({"observation": observation, "chainlink": chainlink})
        if self.on_observe is not None:
            self.on_observe()
        if self.error is not None:
            raise self.error
        return self.matured


class FakeEvaluationWriter:
    def __init__(self, *, offer_error=None, **configuration):
        self.configuration = configuration
        self.offer_error = offer_error
        self.offers = []
        self.started = 0
        self.closed = 0

    def start(self):
        self.started += 1

    def offer_cohort_nowait(self, records):
        self.offers.append(tuple(records))
        if self.offer_error is not None:
            raise self.offer_error

    async def close(self):
        self.closed += 1


def set_price(
    redis,
    key,
    value,
    *,
    received_ms,
    source_timestamp_ms=None,
    publisher_epoch=None,
    accepted_event_sequence=None,
):
    redis.data[key] = encode_live_price(
        value=Decimal(value),
        source_timestamp_ms=(
            received_ms
            if source_timestamp_ms is None
            else source_timestamp_ms
        ),
        received_ms=received_ms,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=accepted_event_sequence,
    )


def make_worker(
    *,
    redis=None,
    store=None,
    scheduler=None,
    writer=None,
    now_ms=None,
    sleep=None,
):
    redis = redis or FakeRedis()
    store = store or FakeSignalStore()
    arguments = {
        "live_cache": LiveCache(redis_client=redis),
        "signal_store": store,
        "evaluation_scheduler": scheduler,
        "evaluation_writer": writer,
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
    assert settings.SHADOW_SIGNAL_2S_EVALUATION_ENABLED is False
    assert settings.SHADOW_SIGNAL_2S_EVALUATION_INTERVAL_MS == 500
    assert settings.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_HOURS == 168
    assert settings.REDIS_HOST == "127.0.0.1"
    assert settings.DATABASE_URL is None
    assert settings.READ_DATABASE_URL is None

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
    with pytest.raises(ValidationError):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            SHADOW_SIGNAL_2S_EVALUATION_RETENTION_HOURS=167,
        )


def test_evaluation_requires_enabled_worker_writer_url_and_no_reader_url():
    with pytest.raises(ValidationError, match="requires.*ENABLED=true"):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            SHADOW_SIGNAL_2S_EVALUATION_ENABLED=True,
            DATABASE_URL="postgresql://writer/db",
        )
    with pytest.raises(ValidationError, match="requires DATABASE_URL"):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            SHADOW_SIGNAL_2S_ENABLED=True,
            SHADOW_SIGNAL_2S_EVALUATION_ENABLED=True,
        )
    with pytest.raises(ValidationError, match="must not receive"):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            READ_DATABASE_URL="postgresql://reader/db",
        )


def test_shared_retention_cleanup_capacity_covers_five_candidates():
    with pytest.raises(ValidationError, match="cover five candidates"):
        collector.ShadowSignal2sSettings(
            _env_file=None,
            SHADOW_SIGNAL_2S_EVALUATION_RETENTION_BATCH_ROWS=2_999,
        )


def test_prospective_registration_is_explicit_and_digest_reproducible():
    registration = collector.load_shadow_signal_2s_registration()
    artifact_bytes = registration.path.read_bytes()
    payload = json.loads(artifact_bytes)

    assert payload["selected"] is False
    assert payload["schema_version"] == 4
    assert payload["publication_role"] == "challenger"
    assert payload["policy_version"] == "prospective_fixed_challenger_v1"
    assert payload["evidence_end_ms"] == 1_784_686_800_000
    assert payload["evidence"] == {
        "first_market_id": 5_948_856,
        "last_market_id": 5_948_955,
        "market_count": 100,
        "session_end_ms": 1_784_686_800_000,
        "session_start_ms": 1_784_656_800_000,
        "source": (
            "received_time_lead_lag_and_fixed_lookback_forecast_proxy"
        ),
        "source_artifact_filename": (
            "chainlink_futures_lead_lag_bundle_20260722-094106044.zip"
        ),
        "source_artifact_sha256": (
            "c763c6ac2811488f2e3ec5ae9992c9a174d40e2915efb51648c3a02e9d3de524"
        ),
    }
    assert payload["model"]["model_version"] == (
        "catchup_v1_l2000_h2000_b100"
    )
    assert payload["evaluation"] == {
        "cadence_ms": 500,
        "causal_policy_version": "sequenced_cache_causal_v3",
        "retention_hours": 168,
    }
    evidence_bundle_path = (
        Path(__file__).resolve().parents[1]
        / payload["evidence"]["source_artifact_filename"]
    )
    assert hashlib.sha256(evidence_bundle_path.read_bytes()).hexdigest() == (
        payload["evidence"]["source_artifact_sha256"]
    )
    assert registration.artifact_sha256 == hashlib.sha256(
        artifact_bytes
    ).hexdigest()
    assert registration.fingerprint_sha256 == hashlib.sha256(
        collector._registration_fingerprint_bytes(payload)
    ).hexdigest()
    assert registration.provenance.selection_artifact_sha256 == (
        registration.artifact_sha256
    )
    assert registration.artifact_sha256 != registration.fingerprint_sha256
    evidence_only_change = dict(payload)
    evidence_only_change["evidence_end_ms"] += 1
    assert collector._registration_fingerprint_bytes(
        evidence_only_change
    ) == collector._registration_fingerprint_bytes(payload)
    attributes = Path(".gitattributes").read_text().splitlines()
    assert (
        "price_collector/shadow_signal_2s_registration.json text eol=lf"
        in attributes
    )


@pytest.mark.parametrize(
    "contents",
    (
        '{"selected":false,"selected":true}',
        '{"selected":true}',
        "not-json",
    ),
)
def test_prospective_registration_rejects_duplicates_tampering_and_invalid_json(
    tmp_path,
    contents,
):
    path = tmp_path / "registration.json"
    path.write_text(contents)

    with pytest.raises(RuntimeError, match="registration"):
        collector.load_shadow_signal_2s_registration(path)


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


def test_worker_requires_evaluation_scheduler_and_writer_as_one_pair():
    worker, _, _ = make_worker()

    with pytest.raises(ValueError, match="must be provided together"):
        collector.ShadowSignal2sWorker(
            live_cache=worker.live_cache,
            signal_store=worker.signal_store,
            evaluation_scheduler=FakeEvaluationScheduler(),
        )


def test_matured_evaluation_is_offered_after_live_publication():
    record = object()
    store = FakeSignalStore()
    writes_before_evaluation = []
    scheduler = FakeEvaluationScheduler(
        matured=(record,),
        on_observe=lambda: writes_before_evaluation.append(
            len(store.set_calls)
        ),
    )
    writer = FakeEvaluationWriter()
    worker, _, _ = make_worker(
        store=store,
        scheduler=scheduler,
        writer=writer,
    )

    payload = run_once(worker, BASE_MS)

    assert store.set_calls == [(payload, 2_000)]
    assert writes_before_evaluation == [1]
    assert len(scheduler.calls) == 1
    assert writer.offers == [(record,)]


def test_malformed_chainlink_reaches_evaluator_as_absent():
    scheduler = FakeEvaluationScheduler()
    writer = FakeEvaluationWriter()
    worker, redis, store = make_worker(
        scheduler=scheduler,
        writer=writer,
    )
    redis.data[CHAINLINK_LIVE_KEY] = (
        '{"value":"not-a-price","source_timestamp_ms":1,'
        f'"received_ms":{BASE_MS}}}'
    )

    payload = run_once(worker, BASE_MS)

    assert store.set_calls == [(payload, 2_000)]
    assert scheduler.calls[0]["chainlink"] is None


@pytest.mark.parametrize("failure_owner", ("scheduler", "writer"))
def test_evaluation_failure_does_not_interrupt_live_publication(failure_owner):
    record = object()
    scheduler = FakeEvaluationScheduler(
        matured=(record,),
        error=(
            RuntimeError("scheduler failed")
            if failure_owner == "scheduler"
            else None
        ),
    )
    writer = FakeEvaluationWriter(
        offer_error=(
            RuntimeError("writer failed")
            if failure_owner == "writer"
            else None
        )
    )
    worker, _, store = make_worker(scheduler=scheduler, writer=writer)

    payload = run_once(worker, BASE_MS)

    assert payload is not None
    assert store.set_calls == [(payload, 2_000)]


def test_real_scheduler_matures_two_second_rows_causally():
    registration = collector.load_shadow_signal_2s_registration()
    scheduler = collector.ShadowEvaluationScheduler(
        models=(collector.SHADOW_SIGNAL_2S_MODEL,),
        provenance=registration.provenance,
        cadence_ms=500,
        max_observation_gap_ms=200,
    )
    writer = FakeEvaluationWriter()
    worker, redis, _ = make_worker(scheduler=scheduler, writer=writer)

    set_price(
        redis,
        FUTURES_LIVE_KEY,
        "60000",
        received_ms=BASE_MS - 2_000,
    )
    run_once(worker, BASE_MS - 2_000)
    for offset_ms in range(0, 2_001, 100):
        set_price(
            redis,
            FUTURES_LIVE_KEY,
            str(60_000 + offset_ms // 100),
            received_ms=BASE_MS + offset_ms,
        )
        if offset_ms == 0:
            set_price(
                redis,
                CHAINLINK_LIVE_KEY,
                "50000",
                received_ms=BASE_MS,
                publisher_epoch="00000000-0000-4000-8000-000000000001",
                accepted_event_sequence=1,
            )
        elif offset_ms == 2_000:
            set_price(
                redis,
                CHAINLINK_LIVE_KEY,
                "50010",
                received_ms=BASE_MS + 2_000,
                publisher_epoch="00000000-0000-4000-8000-000000000001",
                accepted_event_sequence=2,
            )
        run_once(worker, BASE_MS + offset_ms)

    records = [record for cohort in writer.offers for record in cohort]
    first = next(record for record in records if record.generated_ms == BASE_MS)
    assert first.model_version == "catchup_v1_l2000_h2000_b100"
    assert first.horizon_ms == 2_000
    assert first.target_ms == BASE_MS + 2_000
    assert first.matured_ms == BASE_MS + 2_000
    assert first.actual_chainlink == Decimal("50010")
    assert first.actual_chainlink_received_ms <= first.target_ms
    assert first.selection_policy_version == (
        "prospective_fixed_challenger_v1"
    )


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


def test_enabled_collector_validates_registration_before_runtime_dependencies(
    monkeypatch,
):
    settings = collector.ShadowSignal2sSettings(
        _env_file=None,
        SHADOW_SIGNAL_2S_ENABLED=True,
    )
    runtime_calls = []

    def invalid_registration():
        raise RuntimeError("registration tampered")

    def runtime_factory(*args, **kwargs):
        runtime_calls.append((args, kwargs))
        raise AssertionError("runtime dependency opened before registration")

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(
        collector,
        "load_shadow_signal_2s_registration",
        invalid_registration,
    )

    with pytest.raises(RuntimeError, match="registration tampered"):
        asyncio.run(
            collector.run_collector(
                settings,
                live_cache_factory=runtime_factory,
                signal_store_factory=runtime_factory,
            )
        )
    assert runtime_calls == []


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


def test_enabled_evaluation_runtime_is_wired_started_and_closed(monkeypatch):
    settings = collector.ShadowSignal2sSettings(
        _env_file=None,
        SHADOW_SIGNAL_2S_ENABLED=True,
        SHADOW_SIGNAL_2S_EVALUATION_ENABLED=True,
        DATABASE_URL=(
            "postgresql://price_writer:secret@127.0.0.1/price_collector"
        ),
        SHADOW_SIGNAL_2S_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS=4.0,
        SHADOW_SIGNAL_2S_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS=3.0,
    )
    redis = FakeRedis()
    live_cache = LiveCache(redis_client=redis)
    store = FakeSignalStore()
    schedulers = []
    writers = []
    backend_calls = []

    def scheduler_factory(**configuration):
        scheduler = FakeEvaluationScheduler(**configuration)
        schedulers.append(scheduler)
        return scheduler

    def writer_factory(**configuration):
        writer = FakeEvaluationWriter(**configuration)
        writers.append(writer)
        return writer

    async def backend_factory(database_url, **configuration):
        backend_calls.append((database_url, configuration))
        return object()

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(
        collector,
        "create_shadow_evaluation_backend",
        backend_factory,
    )

    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=lambda _settings: live_cache,
            signal_store_factory=lambda _settings: store,
            evaluation_scheduler_factory=scheduler_factory,
            evaluation_writer_factory=writer_factory,
            max_iterations=0,
        )
    )

    assert len(schedulers) == 1
    scheduler_configuration = schedulers[0].configuration
    assert scheduler_configuration["models"] == (
        collector.SHADOW_SIGNAL_2S_MODEL,
    )
    assert scheduler_configuration["cadence_ms"] == 500
    assert scheduler_configuration["max_observation_gap_ms"] == 200
    provenance = scheduler_configuration["provenance"]
    registration = collector.load_shadow_signal_2s_registration()
    assert provenance == registration.provenance
    assert provenance.policy_version == "prospective_fixed_challenger_v1"
    assert provenance.selection_schema_version == 4
    assert provenance.evidence_end_ms == 1_784_686_800_000

    assert len(writers) == 1
    writer = writers[0]
    assert writer.started == 1
    assert writer.closed == 1
    assert writer.configuration["candidate_model_versions"] == (
        "catchup_v1_l2000_h2000_b100",
    )
    assert writer.configuration["queue_max_records"] == 5_000
    assert writer.configuration["batch_max_rows"] == 500
    assert writer.configuration["flush_ms"] == 1_000
    assert writer.configuration["retry_ms"] == 5_000
    assert writer.configuration["shutdown_timeout_ms"] == 10_000
    assert writer.configuration["retention_ms"] == 604_800_000
    assert writer.configuration["cleanup_interval_ms"] == 300_000
    assert writer.configuration["cleanup_batch_rows"] == 5_000
    asyncio.run(writer.configuration["backend_factory"]())
    assert backend_calls == [
        (
            settings.DATABASE_URL,
            {
                "model_versions": (
                    "catchup_v1_l2000_h2000_b100",
                ),
                "connect_timeout_seconds": 4.0,
                "command_timeout_seconds": 3.0,
            },
        )
    ]
    assert redis.closed is True
    assert store.closed is True


def test_disabled_evaluation_never_constructs_database_runtime(monkeypatch):
    settings = collector.ShadowSignal2sSettings(
        _env_file=None,
        SHADOW_SIGNAL_2S_ENABLED=True,
        SHADOW_SIGNAL_2S_EVALUATION_ENABLED=False,
    )
    redis = FakeRedis()
    live_cache = LiveCache(redis_client=redis)
    store = FakeSignalStore()

    def forbidden(*args, **kwargs):
        raise AssertionError("disabled evaluation touched PostgreSQL")

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(
        collector,
        "create_shadow_evaluation_backend",
        forbidden,
    )
    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=lambda _settings: live_cache,
            signal_store_factory=lambda _settings: store,
            evaluation_backend_factory=forbidden,
            evaluation_scheduler_factory=forbidden,
            evaluation_writer_factory=forbidden,
            max_iterations=0,
        )
    )

    assert redis.closed is True
    assert store.closed is True


def test_runtime_has_no_accepted_artifact_or_key_dependency():
    source = Path(collector.__file__).read_text()

    assert "shadow_signal_artifact" not in source
    assert "CHAINLINK_SHADOW_LIVE_KEY" not in source
    assert "prospective_fixed_challenger_v1" in source
    provenance = collector.load_shadow_signal_2s_registration().provenance
    assert provenance.selection_schema_version == 4
    assert provenance.evidence_end_ms == 1_784_686_800_000
    assert (
        provenance.policy_version
        == "prospective_fixed_challenger_v1"
    )
