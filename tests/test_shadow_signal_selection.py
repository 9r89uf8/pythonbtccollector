import json
from copy import deepcopy
from decimal import Decimal, localcontext

import pytest

import price_collector.shadow_signal_selection as selection_module
from price_collector.shadow_signal_selection import (
    EXPECTED_CANDIDATES,
    SelectionInputError,
    encode_selection_artifact,
    select_provisional_primary,
    write_selection_artifact,
)


WINDOW_MS = 6_000_000
MODEL_3000 = "catchup_ratio_l3000_b100"
MODEL_3500 = "catchup_ratio_l3500_b100"
MODEL_4000 = "catchup_ratio_l4000_b100"


CONFIGURATION = {
    "poll_ms": 100,
    "evaluation_interval_ms": 500,
    "lags_ms": [3_000, 3_500, 4_000],
    "beta": "1",
    "futures_stale_ms": 1_000,
    "chainlink_stale_ms": 5_000,
    "reference_max_gap_ms": 250,
    "history_retention_ms": 10_000,
    "max_future_skew_ms": 250,
    "neutral_band_bps": "1",
    "move_size_thresholds_bps": ["1", "3"],
    "volatility_thresholds_bps": ["0.5", "1.5"],
    "volatility_lookback_ms": 10_000,
    "volatility_measure": "rms_of_consecutive_raw_bucket_returns",
    "near_expiry_ms": 10_000,
    "near_reconnect_ms": 10_000,
    "session_boundary_measure": (
        "time_since_common_segment_start_and_until_segment_end"
    ),
    "quantile_sample_max": 10_000,
    "exclude_parse_error_sessions": False,
}


def metric_payload(
    *,
    count=10_000,
    model_mae="40",
    baseline_mae="100",
    model_rmse=None,
    baseline_rmse=None,
    wins=None,
    ties=None,
    losses=None,
):
    model_mae = Decimal(model_mae)
    baseline_mae = Decimal(baseline_mae)
    model_rmse = Decimal(model_rmse or model_mae)
    baseline_rmse = Decimal(baseline_rmse or baseline_mae)
    if wins is None:
        wins = count * 6 // 10
    if ties is None:
        ties = count // 10
    if losses is None:
        losses = count - wins - ties
    count_decimal = Decimal(count)
    model_sum = model_mae * count_decimal
    baseline_sum = baseline_mae * count_decimal
    model_squared_sum = model_rmse * model_rmse * count_decimal
    baseline_squared_sum = baseline_rmse * baseline_rmse * count_decimal
    advantage_sum = baseline_sum - model_sum
    return {
        "count": count,
        "model_mean_absolute_error_usd": str(model_mae),
        "baseline_mean_absolute_error_usd": str(baseline_mae),
        "model_rmse_usd": str(model_rmse),
        "baseline_rmse_usd": str(baseline_rmse),
        "mean_absolute_advantage_usd": str(baseline_mae - model_mae),
        "mae_skill_vs_no_change": str(
            Decimal("1") - model_mae / baseline_mae
        ),
        "wins": wins,
        "ties": ties,
        "losses": losses,
        "directional_eligible": count,
        "directional_correct": wins,
        "directional_action": wins + losses,
        "sufficient_statistics": {
            "model_absolute_error_sum_usd": str(model_sum),
            "baseline_absolute_error_sum_usd": str(baseline_sum),
            "model_squared_error_sum_usd2": str(model_squared_sum),
            "baseline_squared_error_sum_usd2": str(baseline_squared_sum),
            "absolute_advantage_sum_usd": str(advantage_sum),
        },
    }


def common_slices(metrics):
    return {
        "actual_direction": {"up": deepcopy(metrics)},
        "actual_move_size": {"medium": deepcopy(metrics)},
        "raw_bucket_return_rms_regime": {"medium": deepcopy(metrics)},
        "market_expiry": {"regular": deepcopy(metrics)},
        "session_boundary_proximity": {"stable_segment": deepcopy(metrics)},
    }


def candidate_payload(model_version, horizon_ms, metrics):
    count = metrics["count"]
    return {
        "model_version": model_version,
        "horizon_ms": horizon_ms,
        "beta": "1",
        "scheduled": count,
        "scored": count,
        "common_cohort": {
            "definition": (
                "same_generated_ms_max_horizon_eligible_all_models_valid"
            ),
            "target_eligible": count,
            "valid_generated": count,
            "scored": count,
            "metrics": deepcopy(metrics),
            "slices": common_slices(metrics),
        },
    }


def default_model_metrics(*, count=10_000):
    return {
        MODEL_3000: metric_payload(
            count=count,
            model_mae="40",
            baseline_mae="50",
        ),
        MODEL_3500: metric_payload(
            count=count,
            model_mae="90",
            baseline_mae="150",
        ),
        MODEL_4000: metric_payload(
            count=count,
            model_mae="70",
            baseline_mae="100",
        ),
    }


def replay_payload(start_ms, end_ms, *, model_metrics=None):
    if model_metrics is None:
        model_metrics = default_model_metrics()
    candidates = [
        candidate_payload(version, horizon, model_metrics[version])
        for version, horizon in EXPECTED_CANDIDATES
    ]
    return {
        "schema_version": 2,
        "mode": "shadow_raw_replay",
        "status": "ok",
        "selection_performed": False,
        "comparison_cohort": {
            "definition": (
                "same_generated_ms_max_horizon_eligible_all_models_valid"
            ),
            "metrics_location": "candidates[].common_cohort.metrics",
            "required_for_status_ok": True,
        },
        "range": {
            "start_ms": start_ms,
            "end_ms": end_ms,
            "boundary": "[start_ms,end_ms)",
        },
        "configuration": deepcopy(CONFIGURATION),
        "data_quality": {
            "session_policy": "completed_clean_integrity_checked",
            "conservative_reset_at_common_session_boundary": True,
        },
        "candidates": candidates,
    }


def write_report(path, payload):
    path.write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def candidate_from_artifact(artifact, model_version):
    return next(
        candidate
        for candidate in artifact.to_dict()["candidates"]
        if candidate["model_version"] == model_version
    )


def test_selects_by_normalized_calibration_skill_then_passes_holdout(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )
    payload = artifact.to_dict()

    assert artifact.status == "selected"
    assert payload["selection_performed"] is True
    assert payload["policy"]["version"] == "chronological_holdout_v2"
    assert payload["policy"]["supersedes"] == "chronological_holdout_v1"
    assert payload["policy"][
        "previously_inspected_holdouts_must_be_calibration"
    ] is True
    assert payload["policy"][
        "new_later_holdout_required_after_revision"
    ] is True
    assert "paired_wins_minus_losses" not in payload["policy"][
        "efficacy_gates"
    ]
    assert payload["decision"]["frozen_calibration_winner"] == MODEL_3500
    assert payload["decision"]["provisional_primary_model"] == {
        "model_version": MODEL_3500,
        "horizon_ms": 3_500,
        "beta": Decimal("1"),
        "evidence_end_ms": 2 * WINDOW_MS,
    }
    assert candidate_from_artifact(artifact, MODEL_3500)[
        "calibration_rank"
    ] == 1
    selected_candidate = candidate_from_artifact(artifact, MODEL_3500)
    assert selected_candidate["calibration"]["slices"]["actual_direction"][
        "up"
    ]["count"] == 10_000
    assert {
        (warning["dimension"], warning["category"], warning["warning"])
        for warning in selected_candidate["slice_warnings"]
    } >= {
        ("actual_direction", "down", "sparse_slice"),
        ("actual_move_size", "small", "sparse_slice"),
        ("market_expiry", "near_market_end", "sparse_slice"),
        (
            "session_boundary_proximity",
            "post_segment_start",
            "sparse_slice",
        ),
    }
    assert selected_candidate["calibration"]["slices"]["actual_direction"][
        "down"
    ]["count"] == 0
    assert payload["dynamic_switching"] is False
    assert [
        report["role"] for report in payload["provenance"]["reports"]
    ] == ["calibration", "holdout"]
    assert all(
        len(report["sha256"]) == 64
        for report in payload["provenance"]["reports"]
    )
    assert '"path"' not in encode_selection_artifact(artifact)


def test_input_order_does_not_change_artifact_bytes(tmp_path):
    first = write_report(
        tmp_path / "calibration-1.json",
        replay_payload(0, WINDOW_MS),
    )
    second = write_report(
        tmp_path / "calibration-2.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(2 * WINDOW_MS, 3 * WINDOW_MS),
    )

    forward = select_provisional_primary(
        calibration_report_paths=[first, second],
        holdout_report_paths=[holdout],
    )
    reverse = select_provisional_primary(
        calibration_report_paths=[second, first],
        holdout_report_paths=[holdout],
    )

    assert encode_selection_artifact(forward) == encode_selection_artifact(
        reverse
    )


def test_pools_sufficient_statistics_instead_of_averaging_report_rates(tmp_path):
    first_metrics = default_model_metrics(count=10_000)
    first_metrics[MODEL_3500] = metric_payload(
        count=10_000,
        model_mae="10",
        baseline_mae="20",
    )
    second_metrics = default_model_metrics(count=20_000)
    second_metrics[MODEL_3500] = metric_payload(
        count=20_000,
        model_mae="30",
        baseline_mae="40",
        wins=12_000,
        ties=2_000,
        losses=6_000,
    )
    first = write_report(
        tmp_path / "calibration-1.json",
        replay_payload(0, WINDOW_MS, model_metrics=first_metrics),
    )
    second = write_report(
        tmp_path / "calibration-2.json",
        replay_payload(
            WINDOW_MS,
            3 * WINDOW_MS,
            model_metrics=second_metrics,
        ),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(3 * WINDOW_MS, 4 * WINDOW_MS),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[first, second],
        holdout_report_paths=[holdout],
    )
    metrics = candidate_from_artifact(artifact, MODEL_3500)["calibration"][
        "metrics"
    ]

    assert metrics["count"] == 30_000
    with localcontext() as context:
        context.prec = 50
        assert metrics["model_mean_absolute_error_usd"] == (
            Decimal("70") / Decimal("3")
        )
        assert metrics["baseline_mean_absolute_error_usd"] == (
            Decimal("100") / Decimal("3")
        )


def test_holdout_failure_abstains_without_falling_back(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    holdout_metrics = default_model_metrics()
    holdout_metrics[MODEL_3500] = metric_payload(
        model_mae="160",
        baseline_mae="150",
        model_rmse="170",
        baseline_rmse="150",
        wins=3_000,
        ties=1_000,
        losses=6_000,
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(
            WINDOW_MS,
            2 * WINDOW_MS,
            model_metrics=holdout_metrics,
        ),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )
    decision = artifact.to_dict()["decision"]

    assert artifact.status == "holdout_failed"
    assert decision["frozen_calibration_winner"] == MODEL_3500
    assert decision["provisional_primary_model"] is None
    assert decision["holdout_reranking_performed"] is False
    assert decision["fallback_after_holdout_failure_performed"] is False


def test_paired_losses_are_diagnostic_and_do_not_block_selection(tmp_path):
    calibration_metrics = default_model_metrics()
    holdout_metrics = default_model_metrics()
    for metrics_by_model in (calibration_metrics, holdout_metrics):
        updated = deepcopy(metrics_by_model[MODEL_3500])
        updated.update(
            wins=3_000,
            ties=1_000,
            losses=6_000,
            directional_correct=3_000,
            directional_action=9_000,
        )
        metrics_by_model[MODEL_3500] = updated
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS, model_metrics=calibration_metrics),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(
            WINDOW_MS,
            2 * WINDOW_MS,
            model_metrics=holdout_metrics,
        ),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )
    selected = candidate_from_artifact(artifact, MODEL_3500)

    assert artifact.status == "selected"
    assert artifact.to_dict()["decision"]["provisional_primary_model"][
        "model_version"
    ] == MODEL_3500
    assert set(selected["calibration"]["gates"]) == {
        "mae_skill_positive",
        "rmse_skill_positive",
    }
    diagnostic = selected["calibration"]["paired_frequency_diagnostic"]
    assert diagnostic["hard_gate"] is False
    assert diagnostic["affects_eligibility"] is False
    assert diagnostic["affects_ranking"] is False
    assert diagnostic["observed_wins_minus_losses"] == -3_000
    assert diagnostic["warning"] == "paired_wins_do_not_exceed_losses"
    holdout_diagnostic = selected["holdout"]["paired_frequency_diagnostic"]
    assert holdout_diagnostic["hard_gate"] is False
    assert holdout_diagnostic["warning"] == (
        "paired_wins_do_not_exceed_losses"
    )
    assert candidate_from_artifact(artifact, MODEL_4000)[
        "calibration_rank"
    ] > selected["calibration_rank"]

    output = tmp_path / "selected-with-paired-warning.json"
    assert selection_module.main(
        [
            "--calibration-report",
            str(calibration),
            "--holdout-report",
            str(holdout),
            "--output",
            str(output),
        ]
    ) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == (
        "selected"
    )


def test_insufficient_common_evidence_abstains(tmp_path):
    metrics = default_model_metrics(count=9_999)
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS, model_metrics=metrics),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(
            WINDOW_MS,
            2 * WINDOW_MS,
            model_metrics=metrics,
        ),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )

    assert artifact.status == "insufficient_evidence"
    assert artifact.to_dict()["selection_performed"] is False


def test_no_calibration_candidate_beats_baseline(tmp_path):
    metrics = {
        version: metric_payload(
            model_mae="100",
            baseline_mae="100",
            wins=3_000,
            ties=4_000,
            losses=3_000,
        )
        for version, _horizon in EXPECTED_CANDIDATES
    }
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS, model_metrics=metrics),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )

    assert artifact.status == "calibration_error_gate_failed"
    assert artifact.to_dict()["decision"]["reason"] == (
        "no calibration candidate passed both MAE and RMSE improvement gates"
    )
    assert artifact.to_dict()["decision"]["frozen_calibration_winner"] is None


def test_exact_calibration_efficacy_tie_abstains(tmp_path):
    metrics = default_model_metrics()
    metrics[MODEL_3000] = metric_payload(
        model_mae="40",
        baseline_mae="100",
        wins=7_000,
        ties=1_000,
        losses=2_000,
    )
    metrics[MODEL_3500] = metric_payload(
        model_mae="40",
        baseline_mae="100",
        wins=3_000,
        ties=1_000,
        losses=6_000,
    )
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS, model_metrics=metrics),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )

    assert artifact.status == "calibration_tie"
    assert artifact.to_dict()["decision"]["reason"] == (
        "the leading calibration candidates are exactly tied on MAE and "
        "RMSE skill"
    )
    assert artifact.to_dict()["decision"]["provisional_primary_model"] is None


def test_policy_v2_uses_inspected_windows_as_calibration_only(tmp_path):
    original_calibration = write_report(
        tmp_path / "original-calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    inspected_v1_holdout = write_report(
        tmp_path / "inspected-v1-holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )
    new_future_holdout = write_report(
        tmp_path / "new-future-holdout.json",
        replay_payload(2 * WINDOW_MS, 3 * WINDOW_MS),
    )

    artifact = select_provisional_primary(
        calibration_report_paths=[
            inspected_v1_holdout,
            original_calibration,
        ],
        holdout_report_paths=[new_future_holdout],
    )

    assert artifact.status == "selected"
    assert [
        report["role"]
        for report in artifact.to_dict()["provenance"]["reports"]
    ] == ["calibration", "calibration", "holdout"]


def test_policy_v2_rejects_multiple_holdout_reports(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    first_holdout = write_report(
        tmp_path / "holdout-1.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )
    second_holdout = write_report(
        tmp_path / "holdout-2.json",
        replay_payload(2 * WINDOW_MS, 3 * WINDOW_MS),
    )

    with pytest.raises(SelectionInputError, match="exactly one"):
        select_provisional_primary(
            calibration_report_paths=[calibration],
            holdout_report_paths=[first_holdout, second_holdout],
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.update(schema_version=1),
        lambda payload: payload.update(status="partial_candidate_evidence"),
        lambda payload: payload.update(selection_performed=True),
        lambda payload: payload["configuration"].update(lags_ms=[3_000]),
        lambda payload: payload["configuration"].pop("futures_stale_ms"),
        lambda payload: payload["data_quality"].update(
            conservative_reset_at_common_session_boundary=False
        ),
        lambda payload: payload["candidates"].pop(),
        lambda payload: payload["candidates"].append(
            deepcopy(payload["candidates"][0])
        ),
        lambda payload: payload["candidates"][0]["common_cohort"].update(
            scored=9_999
        ),
        lambda payload: payload["candidates"][0]["common_cohort"][
            "metrics"
        ].update(count=9_999),
        lambda payload: payload["candidates"][0]["common_cohort"][
            "metrics"
        ]["sufficient_statistics"].update(
            model_absolute_error_sum_usd="NaN"
        ),
        lambda payload: payload["candidates"][0]["common_cohort"][
            "metrics"
        ]["sufficient_statistics"].update(
            model_squared_error_sum_usd2="0"
        ),
        lambda payload: payload["candidates"][0]["common_cohort"][
            "metrics"
        ].update(model_mean_absolute_error_usd="999"),
        lambda payload: payload["candidates"][0].update(scheduled=True),
    ],
)
def test_incompatible_report_contract_fails_closed(tmp_path, mutator):
    payload = replay_payload(0, WINDOW_MS)
    mutator(payload)
    calibration = write_report(tmp_path / "calibration.json", payload)
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )

    with pytest.raises(SelectionInputError):
        select_provisional_primary(
            calibration_report_paths=[calibration],
            holdout_report_paths=[holdout],
        )


def test_rejects_overlapping_or_nonchronological_roles(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    overlap = write_report(
        tmp_path / "holdout-overlap.json",
        replay_payload(WINDOW_MS - 1, 2 * WINDOW_MS),
    )
    earlier = write_report(
        tmp_path / "holdout-earlier.json",
        replay_payload(0, WINDOW_MS),
    )

    for holdout in (overlap, earlier):
        with pytest.raises(SelectionInputError):
            select_provisional_primary(
                calibration_report_paths=[calibration],
                holdout_report_paths=[holdout],
            )


def test_rejects_configuration_changes_between_roles(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    changed = replay_payload(WINDOW_MS, 2 * WINDOW_MS)
    changed["configuration"]["chainlink_stale_ms"] = 6_000
    holdout = write_report(tmp_path / "holdout.json", changed)

    with pytest.raises(SelectionInputError, match="configurations differ"):
        select_provisional_primary(
            calibration_report_paths=[calibration],
            holdout_report_paths=[holdout],
        )


def test_rejects_duplicate_keys_and_json_floats(tmp_path):
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text(
        '{"schema_version":2,"schema_version":2}',
        encoding="utf-8",
    )
    float_report = tmp_path / "float.json"
    float_report.write_text(
        json.dumps(replay_payload(0, WINDOW_MS)).replace(
            '"poll_ms": 100',
            '"poll_ms": 100.0',
        ),
        encoding="utf-8",
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )

    for calibration in (duplicate, float_report):
        with pytest.raises(SelectionInputError):
            select_provisional_primary(
                calibration_report_paths=[calibration],
                holdout_report_paths=[holdout],
            )


def test_rejects_oversized_report_before_decoding(tmp_path):
    oversized = tmp_path / "oversized.json"
    with oversized.open("wb") as stream:
        stream.seek(selection_module.MAX_REPORT_BYTES)
        stream.write(b"x")
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )

    with pytest.raises(SelectionInputError, match="exceeds"):
        select_provisional_primary(
            calibration_report_paths=[oversized],
            holdout_report_paths=[holdout],
        )


def test_artifact_is_decimal_string_json_and_atomic(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )
    artifact = select_provisional_primary(
        calibration_report_paths=[calibration],
        holdout_report_paths=[holdout],
    )
    output = tmp_path / "selection.json"

    write_selection_artifact(output, artifact)
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["schema_version"] == 2
    assert payload["mode"] == "shadow_primary_selection"
    assert payload["decision"]["provisional_primary_model"]["beta"] == "1"
    assert list(tmp_path.glob("*.tmp")) == []

    write_selection_artifact(output, artifact)
    original = output.read_bytes()
    changed_payload = dict(artifact.to_dict())
    changed_payload["status"] = "holdout_failed"
    changed = selection_module.SelectionArtifact(payload=changed_payload)
    with pytest.raises(SelectionInputError, match="already exists"):
        write_selection_artifact(output, changed)
    assert output.read_bytes() == original


def test_cli_exit_codes_for_selection_and_valid_abstention(tmp_path):
    calibration = write_report(
        tmp_path / "calibration.json",
        replay_payload(0, WINDOW_MS),
    )
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )
    selected_output = tmp_path / "selected.json"

    selected_exit = selection_module.main(
        [
            "--calibration-report",
            str(calibration),
            "--holdout-report",
            str(holdout),
            "--output",
            str(selected_output),
        ]
    )

    assert selected_exit == 0
    assert json.loads(selected_output.read_text(encoding="utf-8"))[
        "status"
    ] == "selected"

    insufficient_metrics = default_model_metrics(count=9_999)
    insufficient = write_report(
        tmp_path / "insufficient.json",
        replay_payload(
            2 * WINDOW_MS,
            3 * WINDOW_MS,
            model_metrics=insufficient_metrics,
        ),
    )
    abstained_output = tmp_path / "abstained.json"
    abstained_exit = selection_module.main(
        [
            "--calibration-report",
            str(calibration),
            "--holdout-report",
            str(insufficient),
            "--output",
            str(abstained_output),
        ]
    )

    assert abstained_exit == 2
    assert json.loads(abstained_output.read_text(encoding="utf-8"))[
        "status"
    ] == "insufficient_evidence"


def test_cli_returns_one_and_writes_nothing_for_invalid_input(tmp_path):
    invalid = write_report(
        tmp_path / "invalid.json",
        replay_payload(0, WINDOW_MS),
    )
    payload = json.loads(invalid.read_text(encoding="utf-8"))
    payload["schema_version"] = 1
    write_report(invalid, payload)
    holdout = write_report(
        tmp_path / "holdout.json",
        replay_payload(WINDOW_MS, 2 * WINDOW_MS),
    )
    output = tmp_path / "selection.json"

    exit_code = selection_module.main(
        [
            "--calibration-report",
            str(invalid),
            "--holdout-report",
            str(holdout),
            "--output",
            str(output),
        ]
    )

    assert exit_code == 1
    assert not output.exists()

    extreme_payload = replay_payload(0, WINDOW_MS)
    extreme_payload["candidates"][0]["common_cohort"]["metrics"][
        "sufficient_statistics"
    ]["model_absolute_error_sum_usd"] = "1e999999999"
    extreme = write_report(tmp_path / "extreme.json", extreme_payload)
    extreme_output = tmp_path / "extreme-selection.json"
    extreme_exit = selection_module.main(
        [
            "--calibration-report",
            str(extreme),
            "--holdout-report",
            str(holdout),
            "--output",
            str(extreme_output),
        ]
    )

    assert extreme_exit == 1
    assert not extreme_output.exists()
