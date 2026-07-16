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
    OUTCOME_REASON_CHAINLINK_OBSERVATION_GAP,
    OUTCOME_REASON_CHAINLINK_PUBLISHER_EPOCH_CHANGE,
    OUTCOME_REASON_CHAINLINK_RECEIVED_TIME_REGRESSION,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_CONFIRMATION_TIMEOUT,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_LOSS,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_RECOVERY,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED,
    OUTCOME_REASON_CHAINLINK_SEQUENCE_REGRESSION,
    OUTCOME_REASON_CHAINLINK_STARTUP_LEGACY_TO_SEQUENCED,
    OUTCOME_REASON_ENGINE_CLOCK_REGRESSION,
    OUTCOME_STATUS_AVAILABLE,
    OUTCOME_STATUS_INTEGRITY_INVALID,
    OUTCOME_STATUS_UNAVAILABLE,
    ShadowEvaluationCohort,
    ShadowEvaluationCohortWriteResult,
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


def price(
    value,
    received_ms,
    source_timestamp_ms=None,
    *,
    publisher_epoch=None,
    accepted_event_sequence=None,
):
    return ObservedPrice(
        value=Decimal(value),
        source_timestamp_ms=source_timestamp_ms,
        received_ms=received_ms,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=accepted_event_sequence,
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


def v3_scheduler(
    candidate_models,
    *,
    cadence_ms=500,
    max_observation_gap_ms=200,
):
    return ShadowEvaluationScheduler(
        models=candidate_models,
        provenance=replace(
            PROVENANCE,
            selection_schema_version=3,
            policy_version="chronological_holdout_v3",
        ),
        cadence_ms=cadence_ms,
        max_observation_gap_ms=max_observation_gap_ms,
    )


def matured_evaluation_record(*, invalid=False):
    (candidate,) = models(300)
    evaluator = scheduler((candidate,))
    forecast_chainlink = price("100", 0, 0)
    evaluator.observe(
        observation(
            0,
            (candidate,),
            chainlink=forecast_chainlink,
            invalid_versions=(candidate.version,) if invalid else (),
        ),
        chainlink=None,
    )
    matured = evaluator.observe(
        observation(301, (candidate,), chainlink=price("101", 301, 300)),
        chainlink=price("101", 301, 300),
    )
    return next(record for record in matured if record.generated_ms == 0)


@pytest.mark.parametrize(
    "field_name",
    (
        "chainlink_at_forecast",
        "futures_now",
        "futures_reference",
        "futures_reference_target_ms",
        "futures_reference_gap_ms",
        "projected_chainlink",
        "pending_move",
        "pending_move_bps",
        "direction",
    ),
)
def test_valid_evaluation_requires_complete_projection_contract(field_name):
    record = matured_evaluation_record()

    with pytest.raises(
        ValueError,
        match="valid evaluation requires complete projection inputs and output",
    ):
        replace(record, **{field_name: None})


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("projected_chainlink", Decimal("100.5")),
        ("pending_move", Decimal("0.5")),
        ("pending_move_bps", Decimal("50")),
        ("direction", "up"),
    ),
)
def test_invalid_evaluation_rejects_any_projection_output(field_name, value):
    record = matured_evaluation_record(invalid=True)

    with pytest.raises(
        ValueError,
        match="invalid evaluation must not contain projection output",
    ):
        replace(record, **{field_name: value})


def test_invalid_evaluation_preserves_legitimately_partial_input_tuples():
    record = matured_evaluation_record(invalid=True)

    partial = replace(
        record,
        futures_now=None,
        futures_now_source_timestamp_ms=None,
        futures_now_received_ms=None,
        futures_reference=None,
        futures_reference_source_timestamp_ms=None,
        futures_reference_received_ms=None,
        futures_reference_gap_ms=None,
    )

    assert partial.valid is False
    assert partial.chainlink_at_forecast == Decimal("100")
    assert partial.futures_reference_target_ms == 0
    assert partial.futures_reference_gap_ms is None


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"pending_move": Decimal("0.4")}, "pending_move is inconsistent"),
        (
            {"pending_move_bps": Decimal("49")},
            "pending_move_bps is inconsistent",
        ),
        ({"direction": "down"}, "direction is inconsistent with pending_move"),
    ),
)
def test_valid_evaluation_rejects_inconsistent_projection_math(
    overrides,
    message,
):
    record = matured_evaluation_record()

    with pytest.raises(ValueError, match=message):
        replace(record, **overrides)


def test_valid_evaluation_accepts_exact_flat_projection_contract():
    record = matured_evaluation_record()

    flat = replace(
        record,
        projected_chainlink=record.chainlink_at_forecast,
        pending_move=Decimal("0"),
        pending_move_bps=Decimal("0"),
        direction="flat",
    )

    assert flat.pending_move == Decimal("0")
    assert flat.direction == "flat"


def test_outcome_state_contract_distinguishes_availability_from_integrity():
    unavailable = matured_evaluation_record()
    assert unavailable.outcome_status == OUTCOME_STATUS_UNAVAILABLE
    assert unavailable.outcome_invalid_reasons == ()

    with pytest.raises(ValueError, match="available outcome requires"):
        replace(unavailable, outcome_status=OUTCOME_STATUS_AVAILABLE)
    with pytest.raises(ValueError, match="requires an explicit reason"):
        replace(
            unavailable,
            outcome_status=OUTCOME_STATUS_INTEGRITY_INVALID,
        )
    with pytest.raises(ValueError, match="must not contain invalid reasons"):
        replace(
            unavailable,
            outcome_invalid_reasons=(OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,),
        )

    integrity_invalid = replace(
        unavailable,
        outcome_status=OUTCOME_STATUS_INTEGRITY_INVALID,
        outcome_invalid_reasons=(OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,),
    )
    assert integrity_invalid.actual_chainlink is None

    with pytest.raises(ValueError, match="must not contain duplicates"):
        replace(
            integrity_invalid,
            outcome_invalid_reasons=(
                OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,
                OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,
            ),
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
    assert invalid_record.outcome_status == OUTCOME_STATUS_AVAILABLE
    assert invalid_record.outcome_invalid_reasons == ()


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


def test_pending_heap_stages_distinct_horizons_until_cohort_is_complete():
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

    assert first == ()
    assert [record.model_version for record in second if record.generated_ms == 0] == [
        long_model.version,
        short_model.version,
    ]
    generated_zero = [
        record for record in second if record.generated_ms == 0
    ]
    assert generated_zero[0].matured_ms == 900
    assert generated_zero[1].matured_ms == 900


def test_complete_cohort_resolves_each_target_from_retained_causal_history():
    short_model, long_model = models(100, 200)
    evaluator = scheduler((short_model, long_model))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )
    short_actual = price(
        "101",
        90,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(90, (short_model, long_model), chainlink=short_actual),
        chainlink=short_actual,
    )
    assert evaluator.observe(
        observation(100, (short_model, long_model), chainlink=short_actual),
        chainlink=short_actual,
    ) == ()
    long_actual = price(
        "102",
        150,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=3,
    )
    evaluator.observe(
        observation(150, (short_model, long_model), chainlink=long_actual),
        chainlink=long_actual,
    )
    matured = evaluator.observe(
        observation(200, (short_model, long_model), chainlink=long_actual),
        chainlink=long_actual,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.actual_chainlink for record in cohort] == [
        Decimal("101"),
        Decimal("102"),
    ]
    assert [record.actual_chainlink_received_ms for record in cohort] == [
        90,
        150,
    ]
    assert all(record.matured_ms == 200 for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_AVAILABLE
        for record in cohort
    )
    assert all(record.outcome_invalid_reasons == () for record in cohort)


def test_sequence_gap_after_short_target_invalidates_entire_staged_cohort():
    short_model, long_model = models(100, 200)
    evaluator = ShadowEvaluationScheduler(
        models=(short_model, long_model),
        provenance=replace(
            PROVENANCE,
            selection_schema_version=3,
            policy_version="chronological_holdout_v3",
        ),
        cadence_ms=500,
        max_observation_gap_ms=200,
    )
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )

    assert evaluator.observe(
        observation(100, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    ) == ()

    after_gap = price(
        "102",
        150,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=3,
    )
    assert evaluator.observe(
        observation(150, (short_model, long_model), chainlink=after_gap),
        chainlink=after_gap,
    ) == ()
    matured = evaluator.observe(
        observation(200, (short_model, long_model), chainlink=after_gap),
        chainlink=after_gap,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.model_version for record in cohort] == [
        short_model.version,
        long_model.version,
    ]
    assert evaluator.chainlink_sequence_gap_count == 1
    assert all(record.actual_chainlink is None for record in cohort)
    assert all(
        record.actual_chainlink_source_timestamp_ms is None
        for record in cohort
    )
    assert all(record.actual_chainlink_received_ms is None for record in cohort)
    assert all(
        record.actual_chainlink_age_at_target_ms is None
        for record in cohort
    )
    assert all(record.forecast_error is None for record in cohort)
    assert all(record.baseline_error is None for record in cohort)
    assert all(record.matured_ms == 200 for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
        for record in cohort
    )
    assert all(
        record.outcome_invalid_reasons
        == (OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,)
        for record in cohort
    )


def test_v3_missing_chainlink_at_targets_never_scores_cached_history():
    short_model, long_model = models(100, 200)
    evaluator = ShadowEvaluationScheduler(
        models=(short_model, long_model),
        provenance=replace(
            PROVENANCE,
            selection_schema_version=3,
            policy_version="chronological_holdout_v3",
        ),
        cadence_ms=500,
        max_observation_gap_ms=200,
    )
    initial = price(
        "100",
        0,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )

    assert evaluator.observe(
        observation(100, (short_model, long_model)),
        chainlink=None,
    ) == ()
    assert evaluator.observe(
        observation(200, (short_model, long_model)),
        chainlink=None,
    ) == ()
    assert evaluator.observe(
        observation(399, (short_model, long_model)),
        chainlink=None,
    ) == ()
    assert evaluator.pending_count == 2

    matured = evaluator.observe(
        observation(400, (short_model, long_model)),
        chainlink=None,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.model_version for record in cohort] == [
        short_model.version,
        long_model.version,
    ]
    assert all(record.actual_chainlink is None for record in cohort)
    assert all(
        record.actual_chainlink_source_timestamp_ms is None
        for record in cohort
    )
    assert all(record.actual_chainlink_received_ms is None for record in cohort)
    assert all(record.forecast_error is None for record in cohort)
    assert all(record.baseline_error is None for record in cohort)
    assert all(record.matured_ms == 400 for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
        for record in cohort
    )
    assert all(
        record.outcome_invalid_reasons
        == (OUTCOME_REASON_CHAINLINK_SEQUENCE_CONFIRMATION_TIMEOUT,)
        for record in cohort
    )
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 1
    assert evaluator.pending_count == 0


@pytest.mark.parametrize("confirmation_ms", (200, 300, 400))
def test_v3_sequenced_confirmation_at_or_before_deadline_scores_cohort(
    confirmation_ms,
):
    short_model, long_model = models(100, 200)
    evaluator = ShadowEvaluationScheduler(
        models=(short_model, long_model),
        provenance=replace(
            PROVENANCE,
            selection_schema_version=3,
            policy_version="chronological_holdout_v3",
        ),
        cadence_ms=500,
        max_observation_gap_ms=200,
    )
    initial = price(
        "100",
        0,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )
    evaluator.observe(
        observation(100, (short_model, long_model)),
        chainlink=None,
    )
    if confirmation_ms > 200:
        assert evaluator.observe(
            observation(200, (short_model, long_model)),
            chainlink=None,
        ) == ()

    matured = evaluator.observe(
        observation(
            confirmation_ms,
            (short_model, long_model),
            chainlink=initial,
        ),
        chainlink=initial,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.actual_chainlink for record in cohort] == [
        Decimal("100"),
        Decimal("100"),
    ]
    assert all(record.matured_ms == confirmation_ms for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_AVAILABLE
        for record in cohort
    )
    assert all(record.outcome_invalid_reasons == () for record in cohort)
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 0


def test_v3_sequenced_confirmation_after_deadline_cannot_score_cohort():
    short_model, long_model = models(100, 200)
    evaluator = ShadowEvaluationScheduler(
        models=(short_model, long_model),
        provenance=replace(
            PROVENANCE,
            selection_schema_version=3,
            policy_version="chronological_holdout_v3",
        ),
        cadence_ms=500,
        max_observation_gap_ms=200,
    )
    initial = price(
        "100",
        0,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )
    assert evaluator.observe(
        observation(200, (short_model, long_model)),
        chainlink=None,
    ) == ()

    matured = evaluator.observe(
        observation(401, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert len(cohort) == 2
    assert all(record.actual_chainlink is None for record in cohort)
    assert all(record.forecast_error is None for record in cohort)
    assert all(record.baseline_error is None for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
        for record in cohort
    )
    assert all(
        record.outcome_invalid_reasons
        == (OUTCOME_REASON_CHAINLINK_SEQUENCE_CONFIRMATION_TIMEOUT,)
        for record in cohort
    )
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 1


def test_v2_missing_chainlink_at_targets_retains_legacy_maturity_behavior():
    short_model, long_model = models(100, 200)
    evaluator = ShadowEvaluationScheduler(
        models=(short_model, long_model),
        provenance=PROVENANCE,
        cadence_ms=500,
        max_observation_gap_ms=200,
    )
    initial = price(
        "100",
        0,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )
    evaluator.observe(
        observation(100, (short_model, long_model)),
        chainlink=None,
    )
    matured = evaluator.observe(
        observation(200, (short_model, long_model)),
        chainlink=None,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.actual_chainlink for record in cohort] == [
        Decimal("100"),
        Decimal("100"),
    ]
    assert all(
        record.outcome_status == OUTCOME_STATUS_AVAILABLE
        for record in cohort
    )
    assert all(record.outcome_invalid_reasons == () for record in cohort)


def test_v3_scheduler_requires_bounded_sequence_confirmation_timeout():
    (candidate,) = models(100)

    with pytest.raises(ValueError, match="requires a bounded observation gap"):
        ShadowEvaluationScheduler(
            models=(candidate,),
            provenance=replace(
                PROVENANCE,
                selection_schema_version=3,
                policy_version="chronological_holdout_v3",
            ),
            cadence_ms=500,
        )


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
    assert record.outcome_status == OUTCOME_STATUS_AVAILABLE
    assert record.outcome_invalid_reasons == ()


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
    assert record.outcome_status == OUTCOME_STATUS_UNAVAILABLE
    assert record.outcome_invalid_reasons == ()


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
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_RECEIVED_TIME_REGRESSION,
    )


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
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_ENGINE_CLOCK_REGRESSION,
    )


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
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_OBSERVATION_GAP,
    )


def test_sequenced_cache_repetition_and_next_event_survive_long_poll_gap():
    (candidate,) = models(1_000)
    evaluator = scheduler(
        (candidate,),
        max_observation_gap_ms=200,
    )
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=5,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    evaluator.observe(
        observation(350, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    updated = price(
        "101",
        900,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=6,
    )
    matured = evaluator.observe(
        observation(1_000, (candidate,), chainlink=updated),
        chainlink=updated,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink == Decimal("101")
    assert evaluator.observation_gap_count == 0
    assert evaluator.chainlink_sequence_gap_count == 0
    assert evaluator.chainlink_sequence_regression_count == 0
    assert evaluator.chainlink_publisher_epoch_change_count == 0


@pytest.mark.parametrize("next_sequence", (1, 2))
def test_identical_event_identity_is_allowed_for_same_or_next_sequence(
    next_sequence,
):
    (candidate,) = models(100)
    evaluator = v3_scheduler((candidate,))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    repeated = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=next_sequence,
    )

    matured = evaluator.observe(
        observation(100, (candidate,), chainlink=repeated),
        chainlink=repeated,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink == Decimal("100")
    assert record.outcome_status == OUTCOME_STATUS_AVAILABLE
    assert record.outcome_invalid_reasons == ()
    assert evaluator.chainlink_sequence_identity_mismatch_count == 0
    assert evaluator.history_size == 1


@pytest.mark.parametrize(
    ("conflicting_value", "conflicting_source_ms", "conflicting_received_ms"),
    (
        ("100.25", 80, 90),
        ("100", 81, 90),
        ("100", 80, 91),
    ),
)
def test_same_sequence_identity_mismatch_invalidates_complete_v3_cohort(
    conflicting_value,
    conflicting_source_ms,
    conflicting_received_ms,
):
    short_model, long_model = models(100, 200)
    evaluator = v3_scheduler((short_model, long_model))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (short_model, long_model), chainlink=initial),
        chainlink=initial,
    )
    accepted = price(
        "100",
        90,
        80,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(90, (short_model, long_model), chainlink=accepted),
        chainlink=accepted,
    )
    conflicting = price(
        conflicting_value,
        conflicting_received_ms,
        conflicting_source_ms,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    assert evaluator.observe(
        observation(150, (short_model, long_model), chainlink=conflicting),
        chainlink=conflicting,
    ) == ()

    matured = evaluator.observe(
        observation(200, (short_model, long_model), chainlink=conflicting),
        chainlink=conflicting,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.model_version for record in cohort] == [
        short_model.version,
        long_model.version,
    ]
    assert all(record.actual_chainlink is None for record in cohort)
    assert all(
        record.actual_chainlink_source_timestamp_ms is None
        for record in cohort
    )
    assert all(record.actual_chainlink_received_ms is None for record in cohort)
    assert all(
        record.actual_chainlink_age_at_target_ms is None
        for record in cohort
    )
    assert all(record.forecast_error is None for record in cohort)
    assert all(record.baseline_error is None for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
        for record in cohort
    )
    assert all(
        record.outcome_invalid_reasons
        == (OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH,)
        for record in cohort
    )
    assert evaluator.chainlink_sequence_identity_mismatch_count == 1
    assert evaluator.chainlink_sequence_gap_count == 0
    assert evaluator.chainlink_sequence_regression_count == 0
    assert evaluator.chainlink_publisher_epoch_change_count == 0
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 0
    assert evaluator.history_size == 0


def test_same_sequence_identity_mismatch_at_max_target_cannot_confirm():
    (candidate,) = models(100)
    evaluator = v3_scheduler((candidate,))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    conflicting = price(
        "100.25",
        100,
        100,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )

    matured = evaluator.observe(
        observation(100, (candidate,), chainlink=conflicting),
        chainlink=conflicting,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.forecast_error is None
    assert record.baseline_error is None
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH,
    )
    assert evaluator.chainlink_sequence_identity_mismatch_count == 1
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 0


def test_sequence_identity_mismatch_quarantines_until_newer_sequence():
    (candidate,) = models(100)
    evaluator = v3_scheduler((candidate,), cadence_ms=100)
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    conflicting = price(
        "100",
        50,
        1,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(50, (candidate,), chainlink=conflicting),
        chainlink=conflicting,
    )
    conflict_matured = evaluator.observe(
        observation(100, (candidate,), chainlink=conflicting),
        chainlink=conflicting,
    )

    pre_conflict_record = next(
        record for record in conflict_matured if record.generated_ms == 0
    )
    assert pre_conflict_record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH,
    )
    assert evaluator.chainlink_sequence_identity_mismatch_count == 1
    assert evaluator.history_size == 0

    # Reverting to the other disputed identity cannot end quarantine.
    assert evaluator.observe(
        observation(150, (candidate,), chainlink=initial),
        chainlink=initial,
    ) == ()
    assert evaluator.chainlink_sequence_identity_mismatch_count == 1
    assert evaluator.history_size == 0

    recovered = price(
        "100",
        200,
        200,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    recovery_matured = evaluator.observe(
        observation(200, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )

    quarantined_record = next(
        record for record in recovery_matured if record.generated_ms == 100
    )
    assert quarantined_record.actual_chainlink is None
    assert quarantined_record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert quarantined_record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH,
    )
    assert evaluator.history_size == 1

    matured = evaluator.observe(
        observation(300, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )
    recovered_record = next(
        record for record in matured if record.generated_ms == 200
    )
    assert recovered_record.actual_chainlink == Decimal("100")
    assert recovered_record.outcome_status == OUTCOME_STATUS_AVAILABLE
    assert recovered_record.outcome_invalid_reasons == ()


def test_metadata_loss_cannot_escape_sequence_identity_quarantine():
    (candidate,) = models(1_000)
    evaluator = v3_scheduler((candidate,))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    conflicting = price(
        "100",
        100,
        1,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(100, (candidate,), chainlink=conflicting),
        chainlink=conflicting,
    )
    legacy = price("100", 200, 200)
    evaluator.observe(
        observation(200, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )

    evaluator.observe(
        observation(300, (candidate,), chainlink=initial),
        chainlink=initial,
    )

    assert evaluator.chainlink_sequence_identity_mismatch_count == 1
    assert evaluator.chainlink_sequence_metadata_loss_count == 1
    assert evaluator.history_size == 0

    recovered = price(
        "100",
        400,
        400,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(400, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )

    assert evaluator.history_size == 1


def test_metadata_loss_preserves_identity_for_first_mismatch_detection():
    (candidate,) = models(1_000)
    evaluator = v3_scheduler((candidate,))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    legacy = price("100", 100, 100)
    evaluator.observe(
        observation(100, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )
    conflicting = price(
        "101",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(200, (candidate,), chainlink=conflicting),
        chainlink=conflicting,
    )
    evaluator.observe(
        observation(300, (candidate,), chainlink=initial),
        chainlink=initial,
    )

    assert evaluator.chainlink_sequence_identity_mismatch_count == 1
    assert evaluator.chainlink_sequence_metadata_loss_count == 1
    assert evaluator.history_size == 0

    recovered = price(
        "102",
        400,
        400,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(400, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )

    assert evaluator.history_size == 1


def test_metadata_loss_allows_exact_sequence_identity_recovery():
    (candidate,) = models(1_000)
    evaluator = v3_scheduler((candidate,))
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    legacy = price("100", 100, 100)
    evaluator.observe(
        observation(100, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )
    evaluator.observe(
        observation(200, (candidate,), chainlink=initial),
        chainlink=initial,
    )

    assert evaluator.chainlink_sequence_identity_mismatch_count == 0
    assert evaluator.chainlink_sequence_metadata_loss_count == 1
    assert evaluator.history_size == 1


def test_same_sequence_identity_mismatch_is_enforced_for_v2():
    (candidate,) = models(100)
    evaluator = scheduler((candidate,), max_observation_gap_ms=200)
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    initial = price(
        "100",
        0,
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    conflicting = price(
        "100.25",
        100,
        100,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )

    matured = evaluator.observe(
        observation(100, (candidate,), chainlink=conflicting),
        chainlink=conflicting,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_IDENTITY_MISMATCH,
    )
    assert evaluator.chainlink_sequence_identity_mismatch_count == 1


@pytest.mark.parametrize(
    (
        "first_epoch",
        "first_sequence",
        "next_epoch",
        "next_sequence",
        "counter_name",
        "outcome_reason",
    ),
    (
        (
            "8b3f42da-8927-48f8-9c90-4f2ce84100d8",
            1,
            "8b3f42da-8927-48f8-9c90-4f2ce84100d8",
            3,
            "chainlink_sequence_gap_count",
            OUTCOME_REASON_CHAINLINK_SEQUENCE_GAP,
        ),
        (
            "8b3f42da-8927-48f8-9c90-4f2ce84100d8",
            3,
            "8b3f42da-8927-48f8-9c90-4f2ce84100d8",
            2,
            "chainlink_sequence_regression_count",
            OUTCOME_REASON_CHAINLINK_SEQUENCE_REGRESSION,
        ),
        (
            "8b3f42da-8927-48f8-9c90-4f2ce84100d8",
            3,
            "43cddae5-cf07-4e7c-948f-bfb4f0c945b7",
            1,
            "chainlink_publisher_epoch_change_count",
            OUTCOME_REASON_CHAINLINK_PUBLISHER_EPOCH_CHANGE,
        ),
    ),
)
def test_chainlink_delivery_discontinuity_invalidates_old_actual_history(
    first_epoch,
    first_sequence,
    next_epoch,
    next_sequence,
    counter_name,
    outcome_reason,
):
    (candidate,) = models(1_000)
    evaluator = scheduler(
        (candidate,),
        max_observation_gap_ms=200,
    )
    initial = price(
        "100",
        0,
        publisher_epoch=first_epoch,
        accepted_event_sequence=first_sequence,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    restarted = price(
        "101",
        500,
        publisher_epoch=next_epoch,
        accepted_event_sequence=next_sequence,
    )
    evaluator.observe(
        observation(500, (candidate,), chainlink=restarted),
        chainlink=restarted,
    )
    matured = evaluator.observe(
        observation(1_000, (candidate,), chainlink=restarted),
        chainlink=restarted,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.forecast_error is None
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (outcome_reason,)
    assert evaluator.history_size == 1
    assert getattr(evaluator, counter_name) == 1
    assert evaluator.chainlink_sequence_identity_mismatch_count == 0
    assert evaluator.observation_gap_count == 0


def test_sequence_metadata_downgrade_and_recovery_each_fail_closed():
    (candidate,) = models(300)
    evaluator = scheduler(
        (candidate,),
        max_observation_gap_ms=200,
    )
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    sequenced = price(
        "100",
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=sequenced),
        chainlink=sequenced,
    )
    legacy = price("101", 100)
    evaluator.observe(
        observation(100, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )
    evaluator.observe(
        observation(200, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )
    matured = evaluator.observe(
        observation(300, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_LOSS,
    )
    assert evaluator.chainlink_sequence_metadata_loss_count == 1
    assert evaluator.observation_gap_count == 0

    recovered = price(
        "102",
        400,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(400, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )
    assert evaluator.history_size == 1


def test_sequence_metadata_loss_stays_fail_closed_until_recovery():
    (candidate,) = models(300)
    evaluator = scheduler((candidate,), max_observation_gap_ms=200)
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    sequenced = price(
        "100",
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=sequenced),
        chainlink=sequenced,
    )
    legacy = price("101", 100)
    evaluator.observe(
        observation(100, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )

    evaluator.observe(
        observation(500, (candidate,), chainlink=price("100", 500)),
        chainlink=price("100", 500),
    )
    matured = evaluator.observe(
        observation(800, (candidate,), chainlink=price("100", 790)),
        chainlink=price("100", 790),
    )

    record = next(record for record in matured if record.generated_ms == 500)
    assert record.actual_chainlink is None
    assert record.forecast_error is None
    assert record.outcome_status == OUTCOME_STATUS_UNAVAILABLE
    assert record.outcome_invalid_reasons == ()
    assert evaluator.history_size == 0
    assert evaluator.chainlink_sequence_metadata_loss_count == 1

    recovered = price(
        "104",
        900,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(900, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )
    assert evaluator.history_size == 1


def test_sequence_metadata_recovery_invalidates_cohort_started_during_loss():
    (candidate,) = models(300)
    evaluator = scheduler(
        (candidate,),
        cadence_ms=100,
        max_observation_gap_ms=200,
    )
    publisher_epoch = "8b3f42da-8927-48f8-9c90-4f2ce84100d8"
    sequenced = price(
        "100",
        0,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=1,
    )
    evaluator.observe(
        observation(0, (candidate,), chainlink=sequenced),
        chainlink=sequenced,
    )
    legacy = price("101", 100)
    evaluator.observe(
        observation(
            100,
            (candidate,),
            chainlink=legacy,
            projected_by_version={candidate.version: "101.5"},
        ),
        chainlink=legacy,
    )
    recovered = price(
        "102",
        200,
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=2,
    )
    evaluator.observe(
        observation(200, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )
    matured = evaluator.observe(
        observation(400, (candidate,), chainlink=recovered),
        chainlink=recovered,
    )

    record = next(record for record in matured if record.generated_ms == 100)
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_METADATA_RECOVERY,
    )
    assert record.actual_chainlink is None
    assert record.forecast_error is None


def test_startup_legacy_to_sequenced_transition_resets_actual_history():
    (candidate,) = models(300)
    evaluator = scheduler((candidate,), max_observation_gap_ms=200)
    legacy = price("100", 0)
    evaluator.observe(
        observation(0, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )
    sequenced = price(
        "101",
        100,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=10,
    )
    evaluator.observe(
        observation(100, (candidate,), chainlink=sequenced),
        chainlink=sequenced,
    )
    matured = evaluator.observe(
        observation(300, (candidate,), chainlink=sequenced),
        chainlink=sequenced,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.forecast_error is None
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_STARTUP_LEGACY_TO_SEQUENCED,
    )
    assert evaluator.history_size == 1
    assert evaluator.chainlink_sequence_metadata_loss_count == 0


def test_v3_startup_legacy_is_not_ingested_or_scoreable():
    short_model, long_model = models(100, 200)
    evaluator = v3_scheduler((short_model, long_model))
    legacy = price("100", 0)

    assert evaluator.observe(
        observation(0, (short_model, long_model), chainlink=legacy),
        chainlink=legacy,
    ) == ()
    assert evaluator.history_size == 0
    assert evaluator.pending_count == 2
    assert evaluator.observe(
        observation(100, (short_model, long_model), chainlink=legacy),
        chainlink=legacy,
    ) == ()

    matured = evaluator.observe(
        observation(200, (short_model, long_model), chainlink=legacy),
        chainlink=legacy,
    )

    cohort = [record for record in matured if record.generated_ms == 0]
    assert [record.model_version for record in cohort] == [
        short_model.version,
        long_model.version,
    ]
    assert all(record.actual_chainlink is None for record in cohort)
    assert all(
        record.actual_chainlink_source_timestamp_ms is None
        for record in cohort
    )
    assert all(record.actual_chainlink_received_ms is None for record in cohort)
    assert all(
        record.actual_chainlink_age_at_target_ms is None
        for record in cohort
    )
    assert all(record.forecast_error is None for record in cohort)
    assert all(record.baseline_error is None for record in cohort)
    assert all(
        record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
        for record in cohort
    )
    assert all(
        record.outcome_invalid_reasons
        == (OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED,)
        for record in cohort
    )
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 0
    assert evaluator.history_size == 0
    assert evaluator.pending_count == 0


def test_v3_first_sequence_cannot_retroactively_score_prior_cohort():
    (candidate,) = models(200)
    evaluator = v3_scheduler((candidate,))

    evaluator.observe(
        observation(0, (candidate,)),
        chainlink=None,
    )
    first_sequenced = price(
        "101",
        100,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )
    assert evaluator.observe(
        observation(100, (candidate,), chainlink=first_sequenced),
        chainlink=first_sequenced,
    ) == ()
    matured = evaluator.observe(
        observation(200, (candidate,), chainlink=first_sequenced),
        chainlink=first_sequenced,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink is None
    assert record.forecast_error is None
    assert record.baseline_error is None
    assert record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED,
    )
    assert evaluator.chainlink_sequence_confirmation_timeout_count == 0
    assert evaluator.history_size == 1
    assert {item.generated_ms for item in matured} == {0}
    assert evaluator.pending_count == 0


def test_v3_first_sequenced_bucket_starts_clean_scoring():
    (candidate,) = models(100)
    evaluator = v3_scheduler((candidate,), cadence_ms=100)
    legacy = price("100", 0)
    evaluator.observe(
        observation(0, (candidate,), chainlink=legacy),
        chainlink=legacy,
    )
    assert evaluator.history_size == 0

    first_sequenced = price(
        "100",
        100,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )
    first_matured = evaluator.observe(
        observation(100, (candidate,), chainlink=first_sequenced),
        chainlink=first_sequenced,
    )

    pre_sequence_record = next(
        record for record in first_matured if record.generated_ms == 0
    )
    assert pre_sequence_record.outcome_status == OUTCOME_STATUS_INTEGRITY_INVALID
    assert pre_sequence_record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED,
        OUTCOME_REASON_CHAINLINK_STARTUP_LEGACY_TO_SEQUENCED,
    )
    assert pre_sequence_record.actual_chainlink is None
    assert evaluator.history_size == 1
    assert evaluator.pending_count == 1

    matured = evaluator.observe(
        observation(200, (candidate,), chainlink=first_sequenced),
        chainlink=first_sequenced,
    )

    first_scoreable_record = next(
        record for record in matured if record.generated_ms == 100
    )
    assert first_scoreable_record.actual_chainlink == Decimal("100")
    assert first_scoreable_record.outcome_status == OUTCOME_STATUS_AVAILABLE
    assert first_scoreable_record.outcome_invalid_reasons == ()


def test_v3_first_sequence_after_long_startup_is_not_an_observation_gap():
    (candidate,) = models(100)
    evaluator = v3_scheduler((candidate,))
    evaluator.observe(
        observation(0, (candidate,)),
        chainlink=None,
    )
    first_sequenced = price(
        "101",
        500,
        publisher_epoch="8b3f42da-8927-48f8-9c90-4f2ce84100d8",
        accepted_event_sequence=1,
    )

    matured = evaluator.observe(
        observation(500, (candidate,), chainlink=first_sequenced),
        chainlink=first_sequenced,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.outcome_invalid_reasons == (
        OUTCOME_REASON_CHAINLINK_SEQUENCE_NOT_ESTABLISHED,
    )
    assert evaluator.observation_gap_count == 0
    assert evaluator.history_size == 1
    assert evaluator.pending_count == 1


def test_v2_legacy_only_startup_keeps_gap_fallback_and_causal_actuals():
    (candidate,) = models(300)
    evaluator = scheduler((candidate,), max_observation_gap_ms=200)
    initial = price("100", 0)
    evaluator.observe(
        observation(0, (candidate,), chainlink=initial),
        chainlink=initial,
    )
    actual = price("101", 200)
    evaluator.observe(
        observation(200, (candidate,), chainlink=actual),
        chainlink=actual,
    )
    matured = evaluator.observe(
        observation(300, (candidate,), chainlink=actual),
        chainlink=actual,
    )

    record = next(record for record in matured if record.generated_ms == 0)
    assert record.actual_chainlink == Decimal("101")
    assert record.actual_chainlink_received_ms == 200


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


def mature_cohort(*, generated_ms=0):
    candidate_models = models(100, 150, 200)
    evaluator = scheduler(candidate_models, cadence_ms=50)
    initial = price("100", generated_ms, generated_ms)
    evaluator.observe(
        observation(generated_ms, candidate_models, chainlink=initial),
        chainlink=initial,
    )
    actual = price("101", generated_ms + 190, generated_ms + 180)
    matured = evaluator.observe(
        observation(
            generated_ms + 200,
            candidate_models,
            chainlink=actual,
        ),
        chainlink=actual,
    )
    return tuple(
        record for record in matured if record.generated_ms == generated_ms
    )


class Backend:
    def __init__(
        self,
        *,
        fail=False,
        hang=False,
        cleanup_error=False,
        persisted_cohort_count=None,
    ):
        self.fail = fail
        self.hang = hang
        self.cleanup_error = cleanup_error
        self.persisted_cohort_count = persisted_cohort_count
        self.batches = []
        self.cleanup_calls = []
        self.closed = False
        self.entered = asyncio.Event()

    async def write_evaluation_cohorts(self, cohorts):
        self.entered.set()
        if self.hang:
            await asyncio.Event().wait()
        if self.fail:
            raise RuntimeError("write failed")
        self.batches.append(list(cohorts))
        persisted_count = (
            len(cohorts)
            if self.persisted_cohort_count is None
            else self.persisted_cohort_count
        )
        return ShadowEvaluationCohortWriteResult(
            persisted_cohort_ids=frozenset(
                cohort.identity for cohort in cohorts[:persisted_count]
            ),
            rejected_cohort_ids=frozenset(
                cohort.identity for cohort in cohorts[persisted_count:]
            ),
            deferred_cohort_ids=frozenset(),
        )

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
        "candidate_model_versions": ("candidate_100",),
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


def test_writer_queue_overflow_drops_one_complete_candidate_cohort(caplog):
    async def scenario():
        backend = Backend()
        first = mature_cohort(generated_ms=0)
        second = mature_cohort(generated_ms=1_000)
        third = mature_cohort(generated_ms=2_000)
        runtime = writer(
            lambda: backend,
            candidate_model_versions=tuple(
                record.model_version for record in first
            ),
            queue_max_records=6,
            batch_max_rows=6,
        )

        runtime.offer_cohort_nowait(first)
        runtime.offer_cohort_nowait(second)
        result = runtime.offer_cohort_nowait(third)

        assert result.dropped_oldest is True
        assert result.dropped_records == first
        assert result.queue_depth == 6
        assert result.queue_cohort_depth == 2
        assert runtime.counters.records_dropped_total == 3
        assert runtime.counters.records_enqueued_total == 9
        assert runtime.counters.cohorts_offered_total == 3
        assert runtime.counters.cohorts_enqueued_total == 3
        assert runtime.counters.cohorts_dropped_total == 1
        assert runtime.counters.queue_cohort_high_water == 2
        assert backend.batches == []
        await runtime.close()

    asyncio.run(scenario())
    assert "shadow_signal_evaluation_queue_drop" in caplog.text


def test_writer_rejects_incomplete_candidate_cohort_before_queueing():
    cohort = mature_cohort()
    runtime = writer(
        lambda: Backend(),
        candidate_model_versions=tuple(
            record.model_version for record in cohort
        ),
        queue_max_records=3,
        batch_max_rows=3,
    )

    with pytest.raises(ValueError, match="model set or order"):
        runtime.offer_cohort_nowait(cohort[:2])

    assert runtime.queue_depth == 0
    assert runtime.counters.records_offered_total == 0


def test_candidate_cohort_rejects_mixed_generation_market_fields():
    cohort = mature_cohort()
    malformed = replace(
        cohort[1],
        market_id=1,
        market_start_ms=300_000,
        market_end_ms=600_000,
        ms_to_market_end=600_000 - cohort[1].generated_ms,
    )

    with pytest.raises(ValueError, match="generation-market fields"):
        ShadowEvaluationCohort((cohort[0], malformed, cohort[2]))


def test_writer_batch_row_limit_never_splits_candidate_cohort():
    async def scenario():
        backend = Backend()
        first = mature_cohort(generated_ms=0)
        second = mature_cohort(generated_ms=1_000)
        runtime = writer(
            lambda: backend,
            candidate_model_versions=tuple(
                record.model_version for record in first
            ),
            queue_max_records=6,
            batch_max_rows=4,
            flush_ms=2,
        )
        runtime.start()
        runtime.offer_cohort_nowait(first)
        runtime.offer_cohort_nowait(second)

        await wait_until(lambda: runtime.counters.records_persisted_total == 6)
        await runtime.close()

        assert backend.batches == [
            [ShadowEvaluationCohort(first)],
            [ShadowEvaluationCohort(second)],
        ]
        assert [
            sum(cohort.row_count for cohort in batch)
            for batch in backend.batches
        ] == [3, 3]
        assert runtime.counters.cohorts_persisted_total == 2

    asyncio.run(scenario())


def test_writer_flushes_duplicate_cohort_identities_in_separate_batches():
    async def scenario():
        backend = Backend()
        cohort = mature_cohort(generated_ms=0)
        runtime = writer(
            lambda: backend,
            candidate_model_versions=tuple(
                record.model_version for record in cohort
            ),
            queue_max_records=6,
            batch_max_rows=6,
            flush_ms=2,
        )
        runtime.start()
        runtime.offer_cohort_nowait(cohort)
        runtime.offer_cohort_nowait(cohort)

        await wait_until(lambda: runtime.counters.records_persisted_total == 6)
        await runtime.close()

        expected = [ShadowEvaluationCohort(cohort)]
        assert backend.batches == [expected, expected]
        assert runtime.counters.cohorts_persisted_total == 2
        assert runtime.counters.batches_succeeded_total == 2
        assert runtime.counters.batches_failed_total == 0

    asyncio.run(scenario())


def test_writer_counts_exact_mixed_multirow_cohort_dispositions():
    class MixedDispositionBackend(Backend):
        async def write_evaluation_cohorts(self, cohorts):
            self.entered.set()
            self.batches.append(list(cohorts))
            return ShadowEvaluationCohortWriteResult(
                persisted_cohort_ids=frozenset((cohorts[0].identity,)),
                rejected_cohort_ids=frozenset((cohorts[1].identity,)),
                deferred_cohort_ids=frozenset(),
            )

    async def scenario():
        backend = MixedDispositionBackend()
        first = mature_cohort(generated_ms=0)
        second = mature_cohort(generated_ms=1_000)
        runtime = writer(
            lambda: backend,
            candidate_model_versions=tuple(
                record.model_version for record in first
            ),
            queue_max_records=6,
            batch_max_rows=6,
            flush_ms=2,
        )
        runtime.start()
        runtime.offer_cohort_nowait(first)
        runtime.offer_cohort_nowait(second)

        await wait_until(lambda: runtime.counters.batches_succeeded_total == 1)
        await runtime.close()

        assert runtime.counters.records_persisted_total == 3
        assert runtime.counters.records_rejected_total == 3
        assert runtime.counters.records_dropped_total == 3
        assert runtime.counters.cohorts_persisted_total == 1
        assert runtime.counters.cohorts_rejected_total == 1
        assert runtime.counters.cohorts_dropped_total == 1
        assert runtime.counters.batches_failed_total == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("invalid_partition", ("overlap", "unknown", "omitted"))
def test_writer_retries_batch_for_inexact_backend_cohort_id_partition(
    invalid_partition,
):
    class SplitResultBackend(Backend):
        async def write_evaluation_cohorts(self, cohorts):
            self.entered.set()
            self.batches.append(list(cohorts))
            first_identity, second_identity = (
                cohort.identity for cohort in cohorts
            )
            if invalid_partition == "overlap":
                return ShadowEvaluationCohortWriteResult(
                    persisted_cohort_ids=frozenset((first_identity,)),
                    rejected_cohort_ids=frozenset((first_identity,)),
                    deferred_cohort_ids=frozenset((second_identity,)),
                )
            if invalid_partition == "unknown":
                unknown_identity = replace(
                    second_identity,
                    generated_ms=second_identity.generated_ms + 500,
                )
                return ShadowEvaluationCohortWriteResult(
                    persisted_cohort_ids=frozenset((first_identity,)),
                    rejected_cohort_ids=frozenset((second_identity,)),
                    deferred_cohort_ids=frozenset((unknown_identity,)),
                )
            return ShadowEvaluationCohortWriteResult(
                persisted_cohort_ids=frozenset((first_identity,)),
                rejected_cohort_ids=frozenset(),
                deferred_cohort_ids=frozenset(),
            )

    async def scenario():
        backend = SplitResultBackend()
        first = mature_cohort(generated_ms=0)
        second = mature_cohort(generated_ms=1_000)
        runtime = writer(
            lambda: backend,
            candidate_model_versions=tuple(
                record.model_version for record in first
            ),
            queue_max_records=6,
            batch_max_rows=6,
            flush_ms=2,
            retry_ms=50,
        )
        runtime.start()
        runtime.offer_cohort_nowait(first)
        runtime.offer_cohort_nowait(second)

        await wait_until(lambda: runtime.counters.batches_failed_total == 1)
        assert backend.batches == [
            [ShadowEvaluationCohort(first), ShadowEvaluationCohort(second)]
        ]
        assert runtime.queue_depth == 6
        assert runtime.queue_cohort_depth == 2
        assert runtime.counters.records_persisted_total == 0
        assert runtime.counters.records_rejected_total == 0
        assert runtime.counters.records_deferred_total == 0
        assert runtime.counters.cohorts_persisted_total == 0
        assert runtime.counters.cohorts_rejected_total == 0
        assert runtime.counters.cohorts_deferred_total == 0
        assert runtime.counters.batches_succeeded_total == 0
        await runtime.close()

        assert runtime.counters.records_dropped_total == 6
        assert runtime.counters.cohorts_dropped_total == 2

    asyncio.run(scenario())


def test_writer_shutdown_summary_exposes_cohort_coverage(caplog):
    async def scenario():
        cohort = mature_cohort()
        runtime = writer(
            lambda: Backend(),
            candidate_model_versions=tuple(
                record.model_version for record in cohort
            ),
            queue_max_records=3,
            batch_max_rows=3,
        )
        runtime.offer_cohort_nowait(cohort)
        await runtime.close()

    caplog.set_level(
        "INFO",
        logger="price_collector.shadow_signal_evaluation",
    )
    asyncio.run(scenario())

    summary = next(
        record
        for record in caplog.records
        if getattr(record, "event", None)
        == "shadow_signal_evaluation_writer_closed"
    )
    assert summary.records_offered_total == 3
    assert summary.records_dropped_total == 3
    assert summary.cohorts_offered_total == 1
    assert summary.cohorts_enqueued_total == 1
    assert summary.cohorts_persisted_total == 0
    assert summary.cohorts_rejected_total == 0
    assert summary.cohorts_deferred_total == 0
    assert summary.cohorts_dropped_total == 1
    assert summary.queue_cohort_high_water == 1


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
        assert [
            cohort.records[0] for cohort in backend.batches[0]
        ] == records[:3]
        assert [
            cohort.records[0] for cohort in backend.batches[1]
        ] == records[3:]
        assert runtime.counters.batches_succeeded_total == 2
        assert backend.closed is True

    asyncio.run(scenario())


def test_writer_counts_permanently_rejected_records_without_requeueing():
    async def scenario():
        backend = Backend(persisted_cohort_count=2)
        runtime = writer(lambda: backend, batch_max_rows=3, flush_ms=5)
        runtime.start()
        records = [
            mature_record(generated_ms=index * 1_000)
            for index in range(3)
        ]
        for record in records:
            runtime.offer_nowait(record)

        await wait_until(lambda: runtime.counters.batches_succeeded_total == 1)
        await runtime.close()

        assert runtime.counters.records_persisted_total == 2
        assert runtime.counters.records_rejected_total == 1
        assert runtime.counters.records_dropped_total == 1
        assert runtime.counters.batches_failed_total == 0
        assert runtime.queue_depth == 0
        assert [
            cohort.records[0] for cohort in backend.batches[0]
        ] == records

    asyncio.run(scenario())


def test_writer_requeues_only_deferred_cohorts():
    class DeferredOnceBackend(Backend):
        async def write_evaluation_cohorts(self, cohorts):
            self.entered.set()
            self.batches.append(list(cohorts))
            cohort_ids = frozenset(cohort.identity for cohort in cohorts)
            if len(self.batches) == 1:
                return ShadowEvaluationCohortWriteResult(
                    persisted_cohort_ids=frozenset(),
                    rejected_cohort_ids=frozenset(),
                    deferred_cohort_ids=cohort_ids,
                )
            return ShadowEvaluationCohortWriteResult(
                persisted_cohort_ids=cohort_ids,
                rejected_cohort_ids=frozenset(),
                deferred_cohort_ids=frozenset(),
            )

    async def scenario():
        backend = DeferredOnceBackend()
        runtime = writer(
            lambda: backend,
            batch_max_rows=1,
            flush_ms=2,
        )
        runtime.start()
        record = mature_record()
        runtime.offer_nowait(record)

        await wait_until(lambda: runtime.counters.records_persisted_total == 1)
        await runtime.close()

        cohort = ShadowEvaluationCohort((record,))
        assert backend.batches == [[cohort], [cohort]]
        assert runtime.counters.records_deferred_total == 1
        assert runtime.counters.records_rejected_total == 0
        assert runtime.counters.records_dropped_total == 0
        assert runtime.counters.batches_succeeded_total == 2

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "invalid_result",
    (
        None,
        True,
        -1,
        2,
        ShadowEvaluationCohortWriteResult(
            persisted_cohort_ids=frozenset(),
            rejected_cohort_ids=frozenset(),
            deferred_cohort_ids=frozenset(),
        ),
    ),
)
def test_writer_retries_invalid_backend_results(invalid_result):
    class InvalidCountBackend(Backend):
        async def write_evaluation_cohorts(self, cohorts):
            self.entered.set()
            self.batches.append(list(cohorts))
            return invalid_result

    async def scenario():
        backend = InvalidCountBackend()
        runtime = writer(
            lambda: backend,
            batch_max_rows=1,
            flush_ms=2,
            retry_ms=50,
        )
        runtime.start()
        runtime.offer_nowait(mature_record())

        await wait_until(lambda: runtime.counters.batches_failed_total == 1)
        assert runtime.counters.records_persisted_total == 0
        assert runtime.counters.records_rejected_total == 0
        assert runtime.queue_depth == 1
        await runtime.close()

        assert runtime.counters.records_dropped_total == 1

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
        assert [
            batch[0].identity.generated_ms for batch in good_backend.batches
        ] == [
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

    cohort = mature_cohort()
    with pytest.raises(ValueError, match="cleanup_batch_rows must fit"):
        writer(
            lambda: Backend(),
            candidate_model_versions=tuple(
                candidate.model_version for candidate in cohort
            ),
            queue_max_records=3,
            batch_max_rows=3,
            retention_ms=10_000,
            cleanup_batch_rows=2,
        )

    async def scenario():
        runtime = writer(lambda: Backend())
        await runtime.close()
        result = runtime.offer_nowait(record)
        assert result.accepted is False
        assert runtime.counters.records_dropped_total == 1

    asyncio.run(scenario())
