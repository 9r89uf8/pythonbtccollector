import json
from decimal import Decimal

import pytest

from price_collector.chainlink_futures_lead_lag import (
    DEFAULT_MODEL_VERSION,
    LeadLagAnalysisError,
    PriceObservation,
    analysis_payload,
    analyze_lead_lag,
    load_observation_tape,
)


IDENTITY = {
    "schema_version": 2,
    "policy_version": "chronological_holdout_v2",
    "evidence_end_ms": 100_000,
    "fingerprint_sha256": "a" * 64,
    "artifact_sha256": "b" * 64,
}


def observation(value, timestamp_ms, block_id=0):
    return PriceObservation(
        value=Decimal(value),
        source_timestamp_ms=timestamp_ms,
        received_ms=timestamp_ms,
        block_id=block_id,
    )


def point(generated_ms, chainlink_value, chainlink_received_ms, futures_value, futures_received_ms):
    return {
        "selection_schema_version": IDENTITY["schema_version"],
        "selection_policy_version": IDENTITY["policy_version"],
        "selection_evidence_end_ms": IDENTITY["evidence_end_ms"],
        "selection_fingerprint_sha256": IDENTITY["fingerprint_sha256"],
        "selection_artifact_sha256": IDENTITY["artifact_sha256"],
        "model_version": DEFAULT_MODEL_VERSION,
        "beta": "1",
        "generated_ms": generated_ms,
        "target_ms": generated_ms + 3_000,
        "horizon_ms": 3_000,
        "valid": True,
        "chainlink_at_forecast": chainlink_value,
        "chainlink_at_forecast_source_timestamp_ms": chainlink_received_ms - 100,
        "chainlink_at_forecast_received_ms": chainlink_received_ms,
        "futures_at_forecast": futures_value,
        "futures_at_forecast_source_timestamp_ms": futures_received_ms - 50,
        "futures_at_forecast_received_ms": futures_received_ms,
    }


def report(points):
    return {
        "schema_version": 2,
        "market": {
            "market_id": 1,
            "market_start_ms": 300_000,
            "market_end_ms": 600_000,
        },
        "coverage": {
            "market_window_elapsed": True,
            "attempts": len(points),
            "valid_forecasts": len(points),
            "scored": len(points),
            "invalid": 0,
        },
        "model": {
            "model_version": DEFAULT_MODEL_VERSION,
            "horizon_ms": 3_000,
            "beta": "1",
            "evaluation_cadence_ms": 500,
            "selection_identities": [IDENTITY],
        },
        "points": points,
    }


def write_report(tmp_path, payload):
    path = tmp_path / (
        "btc_5m_market_1_shadow_evaluations_"
        "catchup_ratio_l3000_b100.json"
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_known_three_second_futures_lead_is_recovered_with_constant_price_basis():
    futures_values = (
        "100",
        "101",
        "100.5",
        "103",
        "102.25",
        "106",
        "104",
        "108.5",
        "107",
        "111",
        "109.5",
        "113",
        "112.25",
        "116",
        "114",
        "118.5",
        "117",
        "121",
        "119.25",
        "123",
    )
    futures = [
        observation(value, index * 1_000)
        for index, value in enumerate(futures_values)
    ]
    basis_multiplier = Decimal("0.998")
    chainlink = [
        observation(Decimal(value) * basis_multiplier, (index + 3) * 1_000)
        for index, value in enumerate(futures_values[:-3])
    ]

    result = analyze_lead_lag(
        chainlink,
        futures,
        candidate_lags_ms=(-1_000, 0, 1_000, 2_000, 3_000, 4_000),
        max_chainlink_gap_ms=2_000,
        max_futures_asof_age_ms=0,
        minimum_market_intervals=1,
    )

    assert result.best_score.lag_ms == 3_000
    assert result.best_score.mae_bps == 0
    assert result.best_score.correlation == 1


def test_asof_lookup_never_selects_a_futures_observation_after_boundary():
    chainlink = [observation("100", 2_000), observation("101", 3_000)]
    futures = [
        observation("100", 1_000),
        observation("150", 2_500),
        observation("151", 3_500),
    ]

    result = analyze_lead_lag(
        chainlink,
        futures,
        candidate_lags_ms=(500,),
        max_chainlink_gap_ms=2_000,
        max_futures_asof_age_ms=1_000,
        minimum_market_intervals=1,
    )
    sample = result.samples_by_lag[500][0]

    assert sample.previous_futures_time_ms == 1_000
    assert sample.current_futures_time_ms == 2_500


def test_interval_ending_at_market_boundary_belongs_to_new_market():
    chainlink = [observation("100", 299_000), observation("101", 300_000)]
    futures = [observation("100", 299_000), observation("101", 300_000)]

    result = analyze_lead_lag(
        chainlink,
        futures,
        candidate_lags_ms=(0,),
        max_chainlink_gap_ms=2_000,
        max_futures_asof_age_ms=0,
        minimum_market_intervals=1,
    )

    assert result.samples_by_lag[0][0].market_id == 1


def test_loader_deduplicates_repeated_forecast_cache_observation(tmp_path):
    payload = report(
        [
            point(300_000, "100.00", 299_900, "101.00", 299_950),
            point(300_500, "100.00", 299_900, "101.25", 300_450),
        ]
    )
    write_report(tmp_path, payload)

    tape = load_observation_tape(tmp_path)

    assert tape.valid_snapshot_rows == 2
    assert len(tape.chainlink) == 1
    assert len(tape.futures) == 2
    assert tape.chainlink[0].value == Decimal("100.00")


def test_loader_rejects_conflicting_values_at_same_receive_time(tmp_path):
    payload = report(
        [
            point(300_000, "100.00", 299_900, "101.00", 299_950),
            point(300_500, "100.50", 299_900, "101.25", 300_450),
        ]
    )
    write_report(tmp_path, payload)

    with pytest.raises(LeadLagAnalysisError, match="conflicting observations"):
        load_observation_tape(tmp_path)


def test_loader_rejects_rounded_export(tmp_path):
    payload = report(
        [point(300_000, "100.00", 299_900, "101.00", 299_950)]
    )
    payload["export"] = {"variant": "rounded_download"}
    write_report(tmp_path, payload)

    with pytest.raises(LeadLagAnalysisError, match="rounded export"):
        load_observation_tape(tmp_path)


def test_loader_rejects_source_timestamp_regression(tmp_path):
    first = point(300_000, "100.00", 299_900, "101.00", 299_950)
    second = point(300_500, "100.10", 300_400, "101.10", 300_450)
    second["chainlink_at_forecast_source_timestamp_ms"] = 299_700
    write_report(tmp_path, report([first, second]))

    with pytest.raises(LeadLagAnalysisError, match="source timestamp regressed"):
        load_observation_tape(tmp_path)


def test_loader_rejects_mixed_point_model_metadata(tmp_path):
    mixed = point(300_000, "100.00", 299_900, "101.00", 299_950)
    mixed["model_version"] = "different_model"
    write_report(tmp_path, report([mixed]))

    with pytest.raises(LeadLagAnalysisError, match="different model_version"):
        load_observation_tape(tmp_path)


def test_summary_serializes_decimal_metrics_as_strings(tmp_path):
    payload = report(
        [
            point(300_000, "100.00", 299_900, "101.00", 299_850),
            point(300_500, "100.10", 300_400, "101.10", 300_350),
        ]
    )
    write_report(tmp_path, payload)
    tape = load_observation_tape(tmp_path)
    analysis = analyze_lead_lag(
        tape.chainlink,
        tape.futures,
        candidate_lags_ms=(0,),
        max_chainlink_gap_ms=2_000,
        max_futures_asof_age_ms=1_000,
        minimum_market_intervals=1,
    )

    payload = analysis_payload(tape, analysis, {})

    assert isinstance(payload["pooled_best"]["mae_bps"], str)
    assert isinstance(payload["lag_scores"][0]["rmse_bps"], str)
