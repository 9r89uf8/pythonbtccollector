import asyncio
import time
from dataclasses import replace
from decimal import Decimal

import pytest

from price_collector.market import MarketWindow
from price_collector.shadow_signal import (
    ANCHORED,
    VALID,
    CatchupModel,
    EngineObservation,
    ModelAnchor,
    ModelSignal,
    ObservedPrice,
    Projection,
)
from price_collector.shadow_signal_evaluation import (
    FORECAST_INPUT_AFTER_GENERATED,
    ShadowEvaluationProvenance,
    ShadowEvaluationScheduler,
    ShadowEvaluationWriterRuntime,
)


PROVENANCE = ShadowEvaluationProvenance(
    selection_schema_version=2,
    policy_version="chronological_holdout_v2",
    selection_fingerprint_sha256="a" * 64,
    selection_artifact_sha256="b" * 64,
    evidence_end_ms=1,
)


def price(value, received_ms, source_timestamp_ms=None):
    return ObservedPrice(
        value=Decimal(value),
        source_timestamp_ms=source_timestamp_ms,
        received_ms=received_ms,
    )


def models(*lags):
    return tuple(
        CatchupModel(
            version=f"candidate_{lag}",
            lag_ms=lag,
            beta=Decimal("1"),
        )
        for lag in lags
    )


def observation(
    generated_ms,
    candidate_models,
    *,
    chainlink=None,
    futures_now=None,
    invalid_versions=(),
    projected_by_version=None,
):
    chainlink = chainlink or price("100", generated_ms, generated_ms)
    futures_now = futures_now or price("201", generated_ms, generated_ms)
    reference = price("200", max(0, generated_ms - 100), max(0, generated_ms - 100))
    projected_by_version = projected_by_version or {}
    signals = []
    for candidate in candidate_models:
        invalid = candidate.version in invalid_versions
        projected = Decimal(
            projected_by_version.get(candidate.version, "100.5")
        )
        projection = None
        if not invalid:
            projection = Projection(
                model_version=candidate.version,
                horizon_ms=candidate.lag_ms,
                projected_chainlink=projected,
                pending_move=projected - chainlink.value,
                pending_move_bps=(
                    (projected - chainlink.value)
                    / chainlink.value
                    * Decimal("10000")
                ),
                direction="up" if projected > chainlink.value else "flat",
            )
        signals.append(
            ModelSignal(
                model_version=candidate.version,
                horizon_ms=candidate.lag_ms,
                generated_ms=generated_ms,
                valid=not invalid,
                status=VALID if not invalid else "futures_stale",
                invalid_reasons=() if not invalid else ("futures_stale",),
                state=ANCHORED,
                projection=projection,
                anchor=ModelAnchor(
                    chainlink=chainlink,
                    futures_reference=reference,
                ),
                futures_now=futures_now,
                chainlink_now=chainlink,
                futures_received_age_ms=max(
                    0,
                    generated_ms - futures_now.received_ms,
                ),
                chainlink_received_age_ms=max(
                    0,
                    generated_ms - chainlink.received_ms,
                ),
                futures_reference_target_ms=max(0, generated_ms - candidate.lag_ms),
                futures_reference_gap_ms=10,
                full_horizon_before_market_end=True,
            )
        )
    market_start = (generated_ms // 300_000) * 300_000
    market = MarketWindow(
        market_id=market_start // 300_000,
        market_start_ms=market_start,
        market_end_ms=market_start + 300_000,
    )
    return EngineObservation(
        generated_ms=generated_ms,
        market=market,
        ms_to_market_end=market.market_end_ms - generated_ms,
        signals=tuple(signals),
        timestamp_regression_sources=(),
        futures_history_size=10,
    )


def scheduler(
    candidate_models,
    *,
    cadence_ms=500,
    max_observation_gap_ms=None,
):
    return ShadowEvaluationScheduler(
        models=candidate_models,
        provenance=PROVENANCE,
        cadence_ms=cadence_ms,
        max_observation_gap_ms=max_observation_gap_ms,
    )


def test_cadence_schedules_once_per_entered_bucket_without_backfill():
    candidate_models = models(10_000, 10_500, 11_000)
    evaluator = scheduler(candidate_models)

    evaluator.observe(
        observation(101, candidate_models),
        chainlink=price("100", 100),
    )
    evaluator.observe(
        observation(499, candidate_models),
        chainlink=price("100", 100),
    )
    assert evaluator.pending_count == 3

    evaluator.observe(
        observation(1_601, candidate_models),
        chainlink=price("100", 1_600),
    )
    assert evaluator.pending_count == 6


def test_all_candidates_and_invalid_signals_are_scheduled_for_coverage():
    candidate_models = models(300, 400, 500)
    invalid = candidate_models[1].version
    evaluator = scheduler(candidate_models)
    forecast_chainlink = price("100", 0, 0)

    evaluator.observe(
        observation(
            0,
            candidate_models,
            chainlink=forecast_chainlink,
            invalid_versions=(invalid,),
        ),
        chainlink=forecast_chainlink,
    )
    matured = evaluator.observe(
        observation(500, candidate_models, chainlink=price("101", 250, 240)),
        chainlink=price("101", 250, 240),
    )

    assert [record.model_version for record in matured] == [
        model.version for model in candidate_models
    ]
    invalid_record = next(
        record for record in matured if record.model_version == invalid
    )
    assert invalid_record.valid is False
    assert invalid_record.projected_chainlink is None
    assert invalid_record.forecast_error is None
    assert invalid_record.baseline_error == Decimal("-1")


def test_post_generated_forecast_inputs_are_fail_closed_for_efficacy():
    (candidate,) = models(300)
    evaluator = scheduler((candidate,))
    future_input = price("100", 107, 100)

    evaluator.observe(
        observation(
            100,
            (candidate,),
            chainlink=future_input,
            futures_now=price("201", 106, 100),
        ),
        chainlink=future_input,
    )
    matured = evaluator.observe(
        observation(400, (candidate,), chainlink=price("101", 350)),
        chainlink=price("101", 350),
    )

    record = next(record for record in matured if record.generated_ms == 100)
    assert record.valid is False
    assert record.status == FORECAST_INPUT_AFTER_GENERATED
    assert record.invalid_reasons == (FORECAST_INPUT_AFTER_GENERATED,)
    assert record.chainlink_at_forecast_received_ms == 107
    assert record.futures_now_received_ms == 106
    assert record.projected_chainlink is None
    assert record.forecast_error is None


def test_rejected_candidate_set_does_not_consume_or_partially_fill_bucket():
    candidate_models = models(300, 400)
    evaluator = scheduler(candidate_models)
    valid_observation = observation(100, candidate_models)
    malformed_signal = replace(
        valid_observation.signals[1],
        horizon_ms=401,
    )
    malformed_observation = replace(
        valid_observation,
        signals=(valid_observation.signals[0], malformed_signal),
    )

    with pytest.raises(ValueError, match="signal horizon"):
        evaluator.observe(
            malformed_observation,
            chainlink=price("100", 100),
        )

    assert evaluator.pending_count == 0
    evaluator.observe(
        valid_observation,
        chainlink=price("100", 100),
    )
    assert evaluator.pending_count == 2


def test_pending_heap_matures_distinct_horizons_in_target_order():
    long_model, short_model = models(900, 300)
    evaluator = scheduler((long_model, short_model))
    evaluator.observe(
        observation(0, (long_model, short_model)),
        chainlink=price("100", 0),
    )

    first = evaluator.observe(
        observation(300, (long_model, short_model)),
        chainlink=price("101", 250),
    )
    second = evaluator.observe(
        observation(900, (long_model, short_model)),
        chainlink=price("102", 850),
    )

    assert [record.model_version for record in first] == [short_model.version]
    assert [record.model_version for record in second if record.generated_ms == 0] == [
        long_model.version
    ]


def test_actual_is_latest_causal_value_and_excludes_after_target_update():
    (candidate,) = models(1_000)
    evaluator = scheduler((candidate,))
    evaluator.observe(
        observation(0, (candidate,), chainlink=price("100", 0)),
        chainlink=price("100", 0),
    )
    evaluator.observe(
        observation(900, (candidate,), chainlink=price("101", 900, 890)),
        chainlink=price("101", 900, 890),
    )
    matured = evaluator.observe(
        observation(1_005, (candidate,), chainlink=price("102", 1_001, 990)),
        chainlink=price("102", 1_001, 990),
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink == Decimal("101")
    assert record.actual_chainlink_received_ms == 900
    assert record.actual_chainlink_age_at_target_ms == 100


def test_same_price_refresh_is_a_distinct_actual_identity():
    (candidate,) = models(1_000)
    evaluator = scheduler((candidate,))
    evaluator.observe(
        observation(0, (candidate,), chainlink=price("100", 0, 0)),
        chainlink=price("100", 0, 0),
    )
    evaluator.observe(
        observation(900, (candidate,), chainlink=price("100", 900, 800)),
        chainlink=price("100", 900, 800),
    )
    matured = evaluator.observe(
        observation(1_000, (candidate,), chainlink=price("100", 900, 800)),
        chainlink=price("100", 900, 800),
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink == Decimal("100")
    assert record.actual_chainlink_received_ms == 900
    assert record.actual_chainlink_source_timestamp_ms == 800


def test_no_causal_actual_leaves_actual_and_errors_null():
    (candidate,) = models(1_000)
    evaluator = scheduler((candidate,))
    forecast = price("100", 0)
    evaluator.observe(
        observation(0, (candidate,), chainlink=forecast),
        chainlink=None,
    )
    matured = evaluator.observe(
        observation(1_001, (candidate,), chainlink=price("101", 1_001)),
        chainlink=price("101", 1_001),
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.actual_chainlink_received_ms is None
    assert record.actual_chainlink_age_at_target_ms is None
    assert record.forecast_error is None
    assert record.baseline_error is None


def test_received_time_regression_invalidates_outstanding_history_epoch():
    (candidate,) = models(1_000)
    evaluator = scheduler((candidate,))
    evaluator.observe(
        observation(0, (candidate,), chainlink=price("100", 0)),
        chainlink=price("100", 0),
    )
    evaluator.observe(
        observation(500, (candidate,), chainlink=price("101", 400)),
        chainlink=price("101", 400),
    )
    evaluator.observe(
        observation(600, (candidate,), chainlink=price("99", 300)),
        chainlink=price("99", 300),
    )
    matured = evaluator.observe(
        observation(1_000, (candidate,), chainlink=price("102", 1_000)),
        chainlink=price("102", 1_000),
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert evaluator.regression_count == 1
    assert record.actual_chainlink is None
    assert record.forecast_error is None


def test_engine_clock_regression_invalidates_outstanding_history_epoch():
    (candidate,) = models(1_000)
    evaluator = scheduler((candidate,))
    evaluator.observe(
        observation(1_000, (candidate,), chainlink=price("100", 1_000)),
        chainlink=price("100", 1_000),
    )

    assert evaluator.observe(
        observation(900, (candidate,), chainlink=price("99", 900)),
        chainlink=price("99", 900),
    ) == ()
    matured = evaluator.observe(
        observation(2_000, (candidate,), chainlink=price("102", 2_000)),
        chainlink=price("102", 2_000),
    )

    record = next(record for record in matured if record.generated_ms == 1_000)
    assert evaluator.regression_count == 1
    assert record.actual_chainlink is None
    assert record.forecast_error is None


def test_live_observation_gap_fails_closed_for_outstanding_actuals():
    (candidate,) = models(1_000)
    evaluator = scheduler(
        (candidate,),
        max_observation_gap_ms=200,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=price("100", 0)),
        chainlink=price("100", 0),
    )
    evaluator.observe(
        observation(100, (candidate,), chainlink=price("100", 0)),
        chainlink=price("100", 0),
    )
    evaluator.observe(
        observation(400, (candidate,), chainlink=price("101", 350)),
        chainlink=price("101", 350),
    )
    for tick_ms in (600, 800, 1_000):
        matured = evaluator.observe(
            observation(
                tick_ms,
                (candidate,),
                chainlink=price("101", 350),
            ),
            chainlink=price("101", 350),
        )

    record = next(record for record in matured if record.generated_ms == 0)
    assert evaluator.observation_gap_count == 1
    assert record.actual_chainlink is None
    assert record.forecast_error is None


def test_signed_decimal_error_math_and_provenance_are_exact():
    (candidate,) = models(1_000)
    evaluator = scheduler((candidate,))
    forecast = price("100.1", 0, 0)
    evaluator.observe(
        observation(
            0,
            (candidate,),
            chainlink=forecast,
            projected_by_version={candidate.version: "101.25"},
        ),
        chainlink=forecast,
    )
    matured = evaluator.observe(
        observation(1_000, (candidate,), chainlink=price("100.75", 900, 800)),
        chainlink=price("100.75", 900, 800),
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.forecast_error == Decimal("0.50")
    assert record.baseline_error == Decimal("-0.65")
    assert isinstance(record.forecast_error, Decimal)
    assert record.selection_policy_version == "chronological_holdout_v2"
    assert record.selection_artifact_sha256 == "b" * 64
    assert record.target_ms == 1_000
    assert record.matured_ms == 1_000


def mature_record(*, generated_ms=0):
    (candidate,) = models(100)
    evaluator = scheduler((candidate,), cadence_ms=50)
    initial = price("100", generated_ms, generated_ms)
    evaluator.observe(
        observation(generated_ms, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    actual = price("101", generated_ms + 90, generated_ms + 80)
    matured = evaluator.observe(
        observation(generated_ms + 100, (candidate,), chainlink=actual),
        chainlink=actual,
    )
    return next(record for record in matured if record.generated_ms == generated_ms)


class Backend:
    def __init__(self, *, fail=False, hang=False, cleanup_error=False):
        self.fail = fail
        self.hang = hang
        self.cleanup_error = cleanup_error
        self.batches = []
        self.cleanup_calls = []
        self.closed = False
        self.entered = asyncio.Event()

    async def write_evaluations(self, records):
        self.entered.set()
        if self.hang:
            await asyncio.Event().wait()
        if self.fail:
            raise RuntimeError("write failed")
        self.batches.append(list(records))

    async def delete_expired(self, *, cutoff_generated_ms, limit):
        self.cleanup_calls.append((cutoff_generated_ms, limit))
        if self.cleanup_error:
            raise RuntimeError("cleanup failed")
        return 2

    async def close(self):
        self.closed = True


async def wait_until(predicate, *, timeout=1):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition did not become true")
        await asyncio.sleep(0.001)


def writer(backend_factory, **overrides):
    arguments = {
        "backend_factory": backend_factory,
        "queue_max_records": 10,
        "batch_max_rows": 3,
        "flush_ms": 10,
        "retry_ms": 1,
        "shutdown_timeout_ms": 200,
    }
    arguments.update(overrides)
    return ShadowEvaluationWriterRuntime(**arguments)


def test_writer_queue_overflow_drops_oldest_without_starting_backend(caplog):
    async def scenario():
        backend = Backend()
        runtime = writer(
            lambda: backend,
            queue_max_records=2,
            batch_max_rows=2,
        )
        first = mature_record(generated_ms=0)
        second = mature_record(generated_ms=1_000)
        third = mature_record(generated_ms=2_000)

        runtime.offer_nowait(first)
        runtime.offer_nowait(second)
        result = runtime.offer_nowait(third)

        assert result.dropped_oldest is True
        assert result.dropped_record == first
        assert result.queue_depth == 2
        assert runtime.counters.records_dropped_total == 1
        assert runtime.counters.queue_high_water == 2
        assert backend.batches == []
        await runtime.close()

    asyncio.run(scenario())
    assert "shadow_signal_evaluation_queue_drop" in caplog.text


def test_writer_batches_records_and_flushes_partial_batch():
    async def scenario():
        backend = Backend()
        runtime = writer(lambda: backend, batch_max_rows=3, flush_ms=5)
        runtime.start()
        records = [mature_record(generated_ms=index * 1_000) for index in range(4)]
        for record in records:
            runtime.offer_nowait(record)

        await wait_until(lambda: runtime.counters.records_persisted_total == 4)
        await runtime.close()

        assert [len(batch) for batch in backend.batches] == [3, 1]
        assert backend.batches[0] == records[:3]
        assert backend.batches[1] == records[3:]
        assert runtime.counters.batches_succeeded_total == 2
        assert backend.closed is True

    asyncio.run(scenario())


def test_writer_recreates_backend_after_write_and_creation_failures():
    async def scenario():
        failed_backend = Backend(fail=True)
        good_backend = Backend()
        factory_calls = 0

        def factory():
            nonlocal factory_calls
            factory_calls += 1
            if factory_calls == 1:
                raise RuntimeError("database unavailable")
            if factory_calls == 2:
                return failed_backend
            return good_backend

        runtime = writer(
            factory,
            batch_max_rows=1,
            flush_ms=2,
        )
        runtime.start()
        runtime.offer_nowait(mature_record(generated_ms=0))
        await wait_until(lambda: runtime.counters.batches_failed_total == 1)
        runtime.offer_nowait(mature_record(generated_ms=1_000))
        await wait_until(lambda: runtime.counters.batches_failed_total == 2)
        runtime.offer_nowait(mature_record(generated_ms=2_000))
        await wait_until(lambda: runtime.counters.records_persisted_total == 3)
        await runtime.close()

        assert factory_calls == 3
        assert runtime.counters.backend_creation_failures_total == 1
        assert runtime.counters.records_dropped_total == 0
        assert failed_backend.closed is True
        assert [batch[0].generated_ms for batch in good_backend.batches] == [
            0,
            1_000,
            2_000,
        ]

    asyncio.run(scenario())


def test_writer_retention_cleanup_is_bounded_interval_and_nonfatal():
    async def scenario():
        now = [100_000]
        backend = Backend(cleanup_error=True)
        runtime = writer(
            lambda: backend,
            batch_max_rows=1,
            retention_ms=10_000,
            cleanup_interval_ms=5_000,
            cleanup_batch_rows=7,
            now_ms=lambda: now[0],
        )
        runtime.start()
        runtime.offer_nowait(mature_record(generated_ms=0))
        await wait_until(lambda: runtime.counters.cleanup_failures_total == 1)
        backend.cleanup_error = False
        runtime.offer_nowait(mature_record(generated_ms=1_000))
        await wait_until(lambda: runtime.counters.records_persisted_total == 2)
        assert len(backend.cleanup_calls) == 1

        now[0] += 5_000
        runtime.offer_nowait(mature_record(generated_ms=2_000))
        await wait_until(lambda: runtime.counters.cleanup_runs_total == 2)
        await runtime.close()

        assert backend.cleanup_calls == [(90_000, 7), (95_000, 7)]
        assert runtime.counters.records_deleted_total == 2
        assert runtime.counters.records_persisted_total == 3

    asyncio.run(scenario())


def test_writer_shutdown_is_bounded_when_backend_hangs():
    async def scenario():
        backend = Backend(hang=True)
        runtime = writer(
            lambda: backend,
            batch_max_rows=1,
            shutdown_timeout_ms=20,
        )
        runtime.start()
        runtime.offer_nowait(mature_record())
        await asyncio.wait_for(backend.entered.wait(), timeout=1)

        started = time.monotonic()
        await runtime.close()
        elapsed = time.monotonic() - started

        assert elapsed < 0.3
        assert runtime.counters.records_dropped_total == 1

    asyncio.run(scenario())


def test_writer_rejects_bad_configuration_and_offers_after_close():
    record = mature_record()

    with pytest.raises(ValueError, match="cannot exceed"):
        writer(lambda: Backend(), queue_max_records=1, batch_max_rows=2)

    async def scenario():
        runtime = writer(lambda: Backend())
        await runtime.close()
        result = runtime.offer_nowait(record)
        assert result.accepted is False
        assert runtime.counters.records_dropped_total == 1

    asyncio.run(scenario())
