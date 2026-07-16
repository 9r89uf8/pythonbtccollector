import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

import price_collector.shadow_signal_collector as collector
from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    CHAINLINK_SHADOW_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LiveCache,
    decode_shadow_signal,
    encode_live_price,
)
from price_collector.shadow_signal import CatchupModel
from price_collector.shadow_signal_artifact import (
    ActivatedShadowSelection,
    ShadowSignalArtifactError,
)


BASE_MS = 1_800_000_000_000
MODELS = (
    CatchupModel(
        version="catchup_ratio_l3000_b100",
        lag_ms=3_000,
        beta=Decimal("1"),
    ),
    CatchupModel(
        version="catchup_ratio_l3500_b100",
        lag_ms=3_500,
        beta=Decimal("1"),
    ),
    CatchupModel(
        version="catchup_ratio_l4000_b100",
        lag_ms=4_000,
        beta=Decimal("1"),
    ),
)


class FakeRedis:
    def __init__(self, *, mget_error=None, set_error=None, on_mget=None):
        self.data = {}
        self.mget_calls = []
        self.set_calls = []
        self.mget_error = mget_error
        self.set_error = set_error
        self.on_mget = on_mget
        self.closed = False

    async def mget(self, keys):
        self.mget_calls.append(list(keys))
        if self.mget_error is not None:
            raise self.mget_error
        if self.on_mget is not None:
            self.on_mget()
        return [self.data.get(key) for key in keys]

    async def set(self, key, value, **options):
        self.set_calls.append((key, value, options))
        if self.set_error is not None:
            raise self.set_error
        self.data[key] = value

    async def aclose(self):
        self.closed = True


def activated_selection(*, primary_index=0, poll_ms=100):
    return ActivatedShadowSelection(
        selection_schema_version=2,
        primary_model=MODELS[primary_index],
        models=MODELS,
        poll_ms=poll_ms,
        futures_stale_ms=3_000,
        chainlink_stale_ms=2_500,
        reference_max_gap_ms=3_000,
        history_retention_ms=10_000,
        max_future_skew_ms=250,
        policy_version="chronological_holdout_v2",
        selection_fingerprint_sha256="a" * 64,
        selection_artifact_sha256="b" * 64,
        evidence_end_ms=BASE_MS - 86_400_000,
    )


def make_worker(*, redis=None, primary_index=0, now_ms=None, sleep=None):
    redis = redis or FakeRedis()
    arguments = {
        "live_cache": LiveCache(redis_client=redis),
        "activated": activated_selection(primary_index=primary_index),
        "ttl_ms": 2_000,
    }
    if now_ms is not None:
        arguments["now_ms"] = now_ms
    if sleep is not None:
        arguments["sleep"] = sleep
    return collector.ShadowSignalWorker(**arguments), redis


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


def run_once(worker, now_ms):
    return asyncio.run(worker.run_once(now_ms=now_ms))


@pytest.mark.parametrize(
    ("now_ms", "poll_ms", "expected_ms"),
    [
        (1_000, 100, 100),
        (1_001, 100, 99),
        (1_099, 100, 1),
        (1_100, 250, 150),
    ],
)
def test_next_poll_boundary_is_strictly_future_and_epoch_aligned(
    now_ms,
    poll_ms,
    expected_ms,
):
    delay_ms = collector.milliseconds_until_next_poll_boundary(
        now_ms,
        poll_ms,
    )

    assert delay_ms == expected_ms
    assert (now_ms + delay_ms) % poll_ms == 0
    assert delay_ms > 0


@pytest.mark.parametrize(
    ("now_ms", "poll_ms", "error"),
    [
        (True, 100, TypeError),
        (-1, 100, ValueError),
        (1_000, True, TypeError),
        (1_000, 0, ValueError),
    ],
)
def test_next_poll_boundary_rejects_invalid_inputs(now_ms, poll_ms, error):
    with pytest.raises(error):
        collector.milliseconds_until_next_poll_boundary(now_ms, poll_ms)


def test_each_tick_uses_one_ordered_mget_and_one_atomic_ttl_set():
    worker, redis = make_worker()

    payload = run_once(worker, BASE_MS)

    assert redis.mget_calls == [[FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]]
    assert len(redis.set_calls) == 1
    key, encoded, options = redis.set_calls[0]
    assert key == CHAINLINK_SHADOW_LIVE_KEY
    assert options == {"px": 2_000}
    assert decode_shadow_signal(encoded) == payload
    assert payload.valid is False
    assert payload.projected_chainlink is None


def test_generated_time_is_stamped_after_redis_inputs_are_available():
    clock = [BASE_MS]
    redis = FakeRedis(on_mget=lambda: clock.__setitem__(0, BASE_MS + 7))
    worker, _ = make_worker(redis=redis, now_ms=lambda: clock[0])

    payload = asyncio.run(worker.run_once())

    assert payload.generated_ms == BASE_MS + 7


def test_worker_warms_then_publishes_valid_and_overwrites_it_when_stale():
    worker, redis = make_worker()

    # A fresh cached futures value may predate worker startup, but the first
    # cached Chainlink event must not be backfilled into an anchor.
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

    assert warming.state == "warming_up_futures_history"
    assert warming.valid is False
    assert waiting.state == "waiting_for_new_chainlink_anchor"
    assert waiting.valid is False
    assert valid.valid is True
    assert valid.status == "valid"
    assert valid.state == "anchored"
    assert valid.model_version == "catchup_ratio_l3000_b100"
    assert valid.current_chainlink == Decimal("50000")
    assert valid.futures_reference == Decimal("60000")
    assert valid.futures_now == Decimal("60060")
    assert valid.projected_chainlink == Decimal("50050.000")
    assert valid.pending_move == Decimal("50.000")
    assert valid.pending_move_bps == Decimal("10.0000")

    # A new, fresh Chainlink event makes the stale futures input the sole
    # freshness failure. The invalid payload must replace, not retain, valid
    # projection or anchor fields.
    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50050",
        received_ms=BASE_MS + 4_101,
    )
    stale = run_once(worker, BASE_MS + 4_101)
    stored = decode_shadow_signal(redis.data[CHAINLINK_SHADOW_LIVE_KEY])

    assert stale.valid is False
    assert stale.status == "futures_stale"
    assert stale.invalid_reasons == ("futures_stale",)
    assert stale.current_chainlink == Decimal("50050")
    assert stale.futures_now == Decimal("60060")
    assert stale.projected_chainlink is None
    assert stale.pending_move is None
    assert stale.pending_move_bps is None
    assert stale.direction is None
    assert stale.futures_reference is None
    assert stale.anchor_chainlink_received_ms is None
    assert stored == stale
    assert len(redis.set_calls) == 4
    assert all(call[2] == {"px": 2_000} for call in redis.set_calls)


def test_all_candidates_run_but_only_the_frozen_primary_is_published():
    worker, redis = make_worker(primary_index=1)
    initial_chainlink_received_ms = BASE_MS

    set_price(redis, FUTURES_LIVE_KEY, "48000", received_ms=BASE_MS)
    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=initial_chainlink_received_ms,
    )
    run_once(worker, BASE_MS)

    for offset_ms, value in (
        (500, "50000"),
        (1_000, "60000"),
        (3_900, "60060"),
    ):
        set_price(
            redis,
            FUTURES_LIVE_KEY,
            value,
            received_ms=BASE_MS + offset_ms,
        )
        run_once(worker, BASE_MS + offset_ms)

    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=BASE_MS + 4_000,
    )
    published = run_once(worker, BASE_MS + 4_000)

    expected_versions = {model.version for model in MODELS}
    assert {model.version for model in worker.engine.models} == expected_versions
    assert set(worker.engine._anchors) == expected_versions
    assert published.model_version == "catchup_ratio_l3500_b100"
    assert published.horizon_ms == 3_500
    assert published.futures_reference == Decimal("50000")
    assert published.projected_chainlink == Decimal("60060")
    assert decode_shadow_signal(
        redis.data[CHAINLINK_SHADOW_LIVE_KEY]
    ) == published


@pytest.mark.parametrize(
    ("bad_key", "bad_payload", "expected_status", "surviving_value"),
    [
        (
            FUTURES_LIVE_KEY,
            "{not-json",
            "futures_unavailable",
            Decimal("50000"),
        ),
        (
            CHAINLINK_LIVE_KEY,
            '{"value":"not-a-price","source_timestamp_ms":1,'
            '"received_ms":1800000000000}',
            "chainlink_unavailable",
            Decimal("60000"),
        ),
    ],
)
def test_malformed_source_is_isolated_and_published_as_invalid(
    bad_key,
    bad_payload,
    expected_status,
    surviving_value,
):
    worker, redis = make_worker()
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    set_price(redis, CHAINLINK_LIVE_KEY, "50000", received_ms=BASE_MS)
    redis.data[bad_key] = bad_payload

    payload = run_once(worker, BASE_MS)

    assert payload.valid is False
    assert payload.status == expected_status
    assert payload.projected_chainlink is None
    if bad_key == FUTURES_LIVE_KEY:
        assert payload.futures_now is None
        assert payload.current_chainlink == surviving_value
    else:
        assert payload.current_chainlink is None
        assert payload.futures_now == surviving_value
    assert len(redis.mget_calls) == 1
    assert len(redis.set_calls) == 1


def test_non_finite_price_is_rejected_without_poisoning_the_other_source():
    worker, redis = make_worker()
    redis.data[FUTURES_LIVE_KEY] = (
        '{"value":"NaN","source_timestamp_ms":1800000000000,'
        '"received_ms":1800000000000}'
    )
    set_price(redis, CHAINLINK_LIVE_KEY, "50000", received_ms=BASE_MS)

    payload = run_once(worker, BASE_MS)

    assert payload.status == "futures_unavailable"
    assert payload.futures_now is None
    assert payload.current_chainlink == Decimal("50000")
    assert payload.projected_chainlink is None


def test_redis_read_error_skips_refresh_and_leaves_expiry_as_fail_safe():
    redis = FakeRedis(mget_error=OSError("redis unavailable"))
    worker, _ = make_worker(redis=redis)

    payload = run_once(worker, BASE_MS)

    assert payload is None
    assert redis.mget_calls == [[FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]]
    assert redis.set_calls == []


def test_redis_write_error_does_not_abort_the_tick():
    redis = FakeRedis(set_error=TimeoutError("redis write timed out"))
    worker, _ = make_worker(redis=redis)

    payload = run_once(worker, BASE_MS)

    assert payload is not None
    assert payload.valid is False
    assert len(redis.mget_calls) == 1
    assert len(redis.set_calls) == 1
    assert redis.set_calls[0][2] == {"px": 2_000}


def test_delayed_loop_skips_missed_boundaries_instead_of_bursting():
    # Each iteration reads the clock once for scheduling and once for the tick.
    # The second schedule read simulates a 250 ms first-tick overrun.
    clock_values = iter(
        (BASE_MS, BASE_MS + 100, BASE_MS + 350, BASE_MS + 400)
    )
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    worker, redis = make_worker(
        now_ms=lambda: next(clock_values),
        sleep=fake_sleep,
    )

    asyncio.run(worker.run(max_iterations=2))

    assert sleep_calls == [0.1, 0.05]
    assert len(redis.mget_calls) == 2
    assert len(redis.set_calls) == 2
    generated = [
        decode_shadow_signal(call[1]).generated_ms
        for call in redis.set_calls
    ]
    assert generated == [BASE_MS + 100, BASE_MS + 400]


def test_disabled_collector_loads_neither_artifact_nor_redis(monkeypatch):
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        SHADOW_SIGNAL_ENABLED=False,
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("disabled worker touched a runtime dependency")

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(collector, "load_activated_selection", forbidden)

    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=forbidden,
        )
    )


def test_invalid_artifact_fails_before_redis_is_created(monkeypatch):
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        SHADOW_SIGNAL_ENABLED=True,
        SHADOW_SIGNAL_SELECTION_PATH="/trusted/selection.json",
        SHADOW_SIGNAL_SELECTION_SHA256="c" * 64,
        SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH="/trusted/replay.json",
        SHADOW_SIGNAL_TRUSTED_DECISION_DIR="/trusted",
    )
    factory_calls = []

    def reject_artifact(*args, **kwargs):
        raise ShadowSignalArtifactError("untrusted artifact")

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(
        collector,
        "load_activated_selection",
        reject_artifact,
    )

    with pytest.raises(ShadowSignalArtifactError, match="untrusted artifact"):
        asyncio.run(
            collector.run_collector(
                settings,
                live_cache_factory=lambda value: factory_calls.append(value),
            )
        )

    assert factory_calls == []


def test_replay_poll_mismatch_fails_before_redis_is_created(monkeypatch):
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        SHADOW_SIGNAL_ENABLED=True,
        SHADOW_SIGNAL_SELECTION_PATH="/trusted/selection.json",
        SHADOW_SIGNAL_SELECTION_SHA256="c" * 64,
        SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH="/trusted/replay.json",
        SHADOW_SIGNAL_TRUSTED_DECISION_DIR="/trusted",
        SHADOW_SIGNAL_POLL_MS=200,
    )
    factory_calls = []

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(
        collector,
        "load_activated_selection",
        lambda *args, **kwargs: activated_selection(),
    )

    with pytest.raises(RuntimeError, match="poll interval"):
        asyncio.run(
            collector.run_collector(
                settings,
                live_cache_factory=lambda value: factory_calls.append(value),
            )
        )

    assert factory_calls == []


def test_enabled_collector_loads_decision_once_then_closes_cache(monkeypatch):
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        APP_ENV="production",
        SHADOW_SIGNAL_ENABLED=True,
        SHADOW_SIGNAL_SELECTION_PATH="/trusted/selection.json",
        SHADOW_SIGNAL_SELECTION_SHA256="c" * 64,
        SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH="/trusted/replay.json",
        SHADOW_SIGNAL_TRUSTED_DECISION_DIR="/trusted",
        SHADOW_SIGNAL_POLL_MS=100,
        SHADOW_SIGNAL_TTL_MS=2_000,
    )
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
    load_calls = []
    factory_calls = []

    def load_once(*args, **kwargs):
        load_calls.append((args, kwargs))
        return activated_selection()

    def create_once(value):
        factory_calls.append(value)
        return cache

    monkeypatch.setattr(collector, "setup_logging", lambda level: None)
    monkeypatch.setattr(collector, "load_activated_selection", load_once)

    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=create_once,
            max_iterations=0,
        )
    )

    assert len(load_calls) == 1
    assert str(load_calls[0][0][0]).replace("\\", "/").endswith(
        "/trusted/selection.json"
    )
    assert load_calls[0][1]["expected_selection_sha256"] == "c" * 64
    assert factory_calls == [settings]
    assert redis.closed is True


class FakeEvaluationScheduler:
    def __init__(self, *, matured=(), error=None, redis=None, **configuration):
        self.matured = tuple(matured)
        self.error = error
        self.redis = redis
        self.configuration = configuration
        self.calls = []
        self.observation_gap_count = 0
        self.chainlink_sequence_gap_count = 0
        self.chainlink_sequence_regression_count = 0
        self.chainlink_sequence_identity_mismatch_count = 0
        self.chainlink_publisher_epoch_change_count = 0
        self.chainlink_sequence_metadata_loss_count = 0
        self.chainlink_sequence_confirmation_timeout_count = 0

    def observe(self, observation, *, chainlink):
        self.calls.append(
            {
                "observation": observation,
                "chainlink": chainlink,
                "redis_writes_before_evaluation": (
                    None if self.redis is None else len(self.redis.set_calls)
                ),
            }
        )
        if self.error is not None:
            raise self.error
        return self.matured


class FakeEvaluationWriter:
    def __init__(self, **configuration):
        self.configuration = configuration
        self.records = []
        self.record_calls = []
        self.cohort_calls = []
        self.started = 0
        self.closed = 0

    def start(self):
        self.started += 1

    def offer_nowait(self, record):
        self.record_calls.append(record)
        self.records.append(record)

    def offer_cohort_nowait(self, records):
        cohort = tuple(records)
        self.cohort_calls.append(cohort)
        self.records.extend(cohort)

    async def close(self):
        self.closed += 1


def test_worker_enqueues_matured_evaluations_after_live_publication():
    redis = FakeRedis()
    records = (object(), object(), object())
    scheduler = FakeEvaluationScheduler(matured=records, redis=redis)
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=BASE_MS,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=17,
    )

    payload = run_once(worker, BASE_MS)

    assert payload is not None
    assert scheduler.calls[0]["observation"].generated_ms == BASE_MS
    assert scheduler.calls[0]["chainlink"].value == Decimal("50000")
    assert scheduler.calls[0]["chainlink"].publisher_epoch == publisher_epoch
    assert scheduler.calls[0]["chainlink"].accepted_event_sequence == 17
    assert scheduler.calls[0]["redis_writes_before_evaluation"] == 1
    assert writer.records == list(records)


def test_worker_offers_multiple_matured_cohorts_separately_and_atomically():
    redis = FakeRedis()
    first_cohort = tuple(
        (BASE_MS - 4_000, model.version) for model in MODELS
    )
    second_cohort = tuple(
        (BASE_MS - 3_500, model.version) for model in MODELS
    )
    scheduler = FakeEvaluationScheduler(
        matured=(*first_cohort, *second_cohort),
        redis=redis,
    )
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    set_price(
        redis,
        CHAINLINK_LIVE_KEY,
        "50000",
        received_ms=BASE_MS,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=17,
    )

    payload = run_once(worker, BASE_MS)

    assert payload is not None
    assert len(scheduler.calls) == 1
    assert writer.record_calls == []
    assert writer.cohort_calls == [first_cohort, second_cohort]
    assert writer.records == [*first_cohort, *second_cohort]


def test_worker_logs_sequence_discontinuity_counter_without_payload(caplog):
    class SequenceGapScheduler(FakeEvaluationScheduler):
        def observe(self, observation, *, chainlink):
            matured = super().observe(observation, chainlink=chainlink)
            self.chainlink_sequence_gap_count += 1
            return matured

    redis = FakeRedis()
    scheduler = SequenceGapScheduler()
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    set_price(redis, CHAINLINK_LIVE_KEY, "54321.98765", received_ms=BASE_MS)

    caplog.set_level("WARNING", logger=collector.LOGGER.name)
    run_once(worker, BASE_MS)

    assert "shadow_signal_evaluation_chainlink_sequence_gap" in caplog.text
    assert "54321.98765" not in caplog.text


def test_worker_logs_sequence_identity_mismatch_without_payload(caplog):
    class SequenceIdentityMismatchScheduler(FakeEvaluationScheduler):
        def observe(self, observation, *, chainlink):
            matured = super().observe(observation, chainlink=chainlink)
            self.chainlink_sequence_identity_mismatch_count += 1
            return matured

    redis = FakeRedis()
    scheduler = SequenceIdentityMismatchScheduler()
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    set_price(redis, CHAINLINK_LIVE_KEY, "54321.98765", received_ms=BASE_MS)

    caplog.set_level("WARNING", logger=collector.LOGGER.name)
    run_once(worker, BASE_MS)

    assert (
        "shadow_signal_evaluation_chainlink_sequence_identity_mismatch"
        in caplog.text
    )
    assert "54321.98765" not in caplog.text


def test_worker_logs_sequence_confirmation_timeout_without_payload(caplog):
    class SequenceConfirmationTimeoutScheduler(FakeEvaluationScheduler):
        def observe(self, observation, *, chainlink):
            matured = super().observe(observation, chainlink=chainlink)
            self.chainlink_sequence_confirmation_timeout_count += 1
            return matured

    redis = FakeRedis()
    scheduler = SequenceConfirmationTimeoutScheduler()
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    set_price(redis, CHAINLINK_LIVE_KEY, "54321.98765", received_ms=BASE_MS)

    caplog.set_level("WARNING", logger=collector.LOGGER.name)
    run_once(worker, BASE_MS)

    assert (
        "shadow_signal_evaluation_chainlink_sequence_confirmation_timeout"
        in caplog.text
    )
    assert "54321.98765" not in caplog.text


@pytest.mark.parametrize(
    "cached_chainlink",
    (
        None,
        '{"value":"not-a-price","source_timestamp_ms":1,'
        '"received_ms":1800000000000}',
    ),
)
def test_missing_or_malformed_chainlink_reaches_evaluator_as_absent(
    cached_chainlink,
):
    redis = FakeRedis()
    scheduler = FakeEvaluationScheduler(redis=redis)
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )
    set_price(redis, FUTURES_LIVE_KEY, "60000", received_ms=BASE_MS)
    if cached_chainlink is not None:
        redis.data[CHAINLINK_LIVE_KEY] = cached_chainlink

    payload = run_once(worker, BASE_MS)

    assert payload.status == "chainlink_unavailable"
    assert scheduler.calls[0]["chainlink"] is None
    assert scheduler.calls[0]["redis_writes_before_evaluation"] == 1


def test_evaluation_failure_does_not_interrupt_live_publication():
    redis = FakeRedis()
    scheduler = FakeEvaluationScheduler(
        error=RuntimeError("evaluation defect"),
    )
    writer = FakeEvaluationWriter()
    worker = collector.ShadowSignalWorker(
        live_cache=LiveCache(redis_client=redis),
        activated=activated_selection(),
        ttl_ms=2_000,
        evaluation_scheduler=scheduler,
        evaluation_writer=writer,
    )

    payload = run_once(worker, BASE_MS)

    assert payload is not None
    assert len(redis.set_calls) == 1
    assert writer.records == []


def test_worker_requires_scheduler_and_writer_as_one_optional_pair():
    redis = FakeRedis()

    with pytest.raises(ValueError, match="must be provided together"):
        collector.ShadowSignalWorker(
            live_cache=LiveCache(redis_client=redis),
            activated=activated_selection(),
            ttl_ms=2_000,
            evaluation_scheduler=FakeEvaluationScheduler(),
        )


def test_enabled_evaluation_runtime_is_wired_and_closed(monkeypatch):
    settings = SimpleNamespace(
        LOG_LEVEL="INFO",
        APP_ENV="production",
        SHADOW_SIGNAL_ENABLED=True,
        SHADOW_SIGNAL_SELECTION_PATH="/trusted/selection.json",
        SHADOW_SIGNAL_SELECTION_SHA256="c" * 64,
        SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH="/trusted/replay.json",
        SHADOW_SIGNAL_TRUSTED_DECISION_DIR="/trusted",
        SHADOW_SIGNAL_POLL_MS=100,
        SHADOW_SIGNAL_TTL_MS=2_000,
        SHADOW_SIGNAL_EVALUATION_ENABLED=True,
        SHADOW_SIGNAL_EVALUATION_INTERVAL_MS=500,
        SHADOW_SIGNAL_EVALUATION_QUEUE_MAX=5_000,
        SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS=500,
        SHADOW_SIGNAL_EVALUATION_FLUSH_MS=1_000,
        SHADOW_SIGNAL_EVALUATION_RETRY_MS=5_000,
        SHADOW_SIGNAL_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS=10.0,
        SHADOW_SIGNAL_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS=4.0,
        SHADOW_SIGNAL_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS=3.0,
        SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS=168,
        SHADOW_SIGNAL_EVALUATION_RETENTION_CHECK_SECONDS=300,
        SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS=5_000,
        DATABASE_URL=(
            "postgresql://price_writer:secret@127.0.0.1/price_collector"
        ),
    )
    redis = FakeRedis()
    cache = LiveCache(redis_client=redis)
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
        "load_activated_selection",
        lambda *args, **kwargs: activated_selection(),
    )
    monkeypatch.setattr(
        collector,
        "create_shadow_evaluation_backend",
        backend_factory,
    )

    asyncio.run(
        collector.run_collector(
            settings,
            live_cache_factory=lambda _settings: cache,
            evaluation_scheduler_factory=scheduler_factory,
            evaluation_writer_factory=writer_factory,
            max_iterations=0,
        )
    )

    assert len(schedulers) == 1
    assert schedulers[0].configuration["cadence_ms"] == 500
    assert schedulers[0].configuration["max_observation_gap_ms"] == 200
    provenance = schedulers[0].configuration["provenance"]
    assert provenance.policy_version == "chronological_holdout_v2"
    assert provenance.selection_artifact_sha256 == "b" * 64
    assert len(writers) == 1
    writer = writers[0]
    assert writer.started == 1
    assert writer.closed == 1
    assert writer.configuration["queue_max_records"] == 5_000
    assert writer.configuration["batch_max_rows"] == 500
    assert writer.configuration["retry_ms"] == 5_000
    assert writer.configuration["retention_ms"] == 604_800_000
    assert writer.configuration["cleanup_interval_ms"] == 300_000
    assert writer.configuration["cleanup_batch_rows"] == 5_000
    asyncio.run(writer.configuration["backend_factory"]())
    assert backend_calls == [
        (
            settings.DATABASE_URL,
            {
                "connect_timeout_seconds": 4.0,
                "command_timeout_seconds": 3.0,
            },
        )
    ]
    assert redis.closed is True
