import asyncio
import json
from decimal import Decimal

import pytest

from price_collector.shadow_signal_lag_test import (
    CALIBRATION_LAGS_MS,
    _calibration_decision,
    _holdout_decision,
    encode_lag_test_result,
    run_lag_test,
    write_lag_test_result,
)


START_MS = 1_000_000
DAY_MS = 86_400_000
CALIBRATION_END_MS = START_MS + DAY_MS
HOLDOUT_START_MS = CALIBRATION_END_MS + DAY_MS
HOLDOUT_END_MS = HOLDOUT_START_MS + DAY_MS


class FakeReplayReport:
    def __init__(self, status, candidate_summaries):
        self.status = status
        self.candidate_summaries = tuple(candidate_summaries)

    def to_dict(self):
        return {
            "schema_version": 3,
            "status": self.status,
            "candidates": list(self.candidate_summaries),
        }


def candidate(lag_ms, skill):
    return {
        "model_version": f"catchup_ratio_l{lag_ms}_b100",
        "horizon_ms": lag_ms,
        "common_cohort": {
            "target_eligible": 10_000,
            "valid_generated": 10_000,
            "scored": 10_000,
            "scored_coverage": Decimal("1"),
            "maturation_coverage": Decimal("1"),
            "metrics": {
                "count": 10_000,
                "model_mean_absolute_error_usd": Decimal("1.2"),
                "baseline_mean_absolute_error_usd": Decimal("1.5"),
                "mae_skill_vs_no_change": skill,
                "wins": 50,
                "ties": 10,
                "losses": 30,
            },
        },
    }


def calibration_report(**skills):
    return FakeReplayReport(
        "ok",
        [
            candidate(lag_ms, skills.get(str(lag_ms), Decimal("0.1")))
            for lag_ms in CALIBRATION_LAGS_MS
        ],
    )


def holdout_report(challenger_lag_ms, challenger_skill, reference_skill):
    return FakeReplayReport(
        "ok",
        [
            candidate(challenger_lag_ms, challenger_skill),
            candidate(3_000, reference_skill),
        ],
    )


def test_simple_lag_test_freezes_shorter_winner_before_holdout():
    reports = [
        calibration_report(
            **{
                "1500": Decimal("0.1"),
                "2000": Decimal("0.3"),
                "2500": Decimal("0.2"),
                "3000": Decimal("0.8"),
                "3500": Decimal("0.9"),
            }
        ),
        holdout_report(2_000, Decimal("0.25"), Decimal("0.2")),
    ]
    seen_configs = []

    async def replay(**kwargs):
        seen_configs.append(kwargs["config"])
        return reports[len(seen_configs) - 1]

    result = asyncio.run(
        run_lag_test(
            database_url="postgresql://reader@example/test",
            calibration_start_ms=START_MS,
            calibration_end_ms=CALIBRATION_END_MS,
            holdout_start_ms=HOLDOUT_START_MS,
            holdout_end_ms=HOLDOUT_END_MS,
            replay=replay,
        )
    )

    assert seen_configs[0].lags_ms == (1_500, 2_000, 2_500, 3_000, 3_500)
    assert seen_configs[1].lags_ms == (2_000, 3_000)
    assert seen_configs[0].exclude_parse_error_sessions is True
    assert seen_configs[0].allowed_chainlink_parse_error_totals == (0,)
    assert seen_configs[0].futures_availability_delay_ms == 100
    assert seen_configs[0].chainlink_availability_delay_ms == 100
    assert result["calibration"]["winner_lag_ms"] == 2_000
    assert result["holdout"]["challenger_lag_ms"] == 2_000
    assert result["status"] == "observed_shorter_better"
    assert "allowed_chainlink_parse_error_totals" not in (
        result["fixed_replay_settings"]
    )


def test_calibration_exact_skill_tie_chooses_smaller_lag():
    report = calibration_report(
        **{
            "1500": Decimal("0.2"),
            "2000": Decimal("0.2"),
            "2500": Decimal("0.1"),
        }
    )

    decision = _calibration_decision(report)

    assert decision["winner_lag_ms"] == 1_500
    assert [item["lag_ms"] for item in decision["ranking"]] == [
        1_500,
        2_000,
        2_500,
    ]


@pytest.mark.parametrize(
    ("target_eligible", "valid_generated", "scored"),
    [
        (10_000, 10_000, 9_999),
        (20_001, 10_000, 10_000),
        (10_102, 10_102, 10_000),
    ],
)
def test_calibration_requires_minimum_common_evidence(
    target_eligible,
    valid_generated,
    scored,
):
    report = calibration_report()
    common = report.candidate_summaries[0]["common_cohort"]
    common["target_eligible"] = target_eligible
    common["valid_generated"] = valid_generated
    common["scored"] = scored

    decision = _calibration_decision(report)

    assert decision["status"] == "insufficient_evidence"
    assert decision["reason"] == "calibration_evidence_gate_failed_lag_1500"


@pytest.mark.parametrize(
    ("challenger_skill", "reference_skill"),
    [
        (Decimal("0.2"), Decimal("0.2")),
        (Decimal("0"), Decimal("-0.1")),
    ],
)
def test_holdout_requires_positive_and_strictly_better_skill(
    challenger_skill,
    reference_skill,
):
    decision = _holdout_decision(
        holdout_report(2_000, challenger_skill, reference_skill),
        challenger_lag_ms=2_000,
    )

    assert decision["status"] == "retain_3000_reference"


@pytest.mark.parametrize(
    "report",
    [
        FakeReplayReport("no_scored_forecasts", ()),
        calibration_report(**{"2000": None}),
    ],
)
def test_incomplete_calibration_stops_before_holdout(report):
    calls = 0

    async def replay(**_kwargs):
        nonlocal calls
        calls += 1
        return report

    result = asyncio.run(
        run_lag_test(
            database_url="postgresql://reader@example/test",
            calibration_start_ms=START_MS,
            calibration_end_ms=CALIBRATION_END_MS,
            holdout_start_ms=HOLDOUT_START_MS,
            holdout_end_ms=HOLDOUT_END_MS,
            replay=replay,
        )
    )

    assert calls == 1
    assert result["status"] == "insufficient_evidence"
    assert result["holdout"] is None
    assert result["holdout_report"] is None


def test_nonpositive_calibration_stops_without_reading_holdout():
    report = calibration_report(
        **{
            "1500": Decimal("0"),
            "2000": Decimal("-0.1"),
            "2500": Decimal("-0.2"),
        }
    )
    calls = 0

    async def replay(**_kwargs):
        nonlocal calls
        calls += 1
        return report

    result = asyncio.run(
        run_lag_test(
            database_url="postgresql://reader@example/test",
            calibration_start_ms=START_MS,
            calibration_end_ms=CALIBRATION_END_MS,
            holdout_start_ms=HOLDOUT_START_MS,
            holdout_end_ms=HOLDOUT_END_MS,
            replay=replay,
        )
    )

    assert calls == 1
    assert result["status"] == "retain_3000_reference"
    assert result["calibration"]["winner_lag_ms"] is None


def test_lag_test_rejects_overlapping_or_non_day_ranges():
    async def replay(**_kwargs):
        raise AssertionError("invalid ranges must fail before replay")

    with pytest.raises(ValueError, match="must not overlap"):
        asyncio.run(
            run_lag_test(
                database_url="postgresql://reader@example/test",
                calibration_start_ms=START_MS,
                calibration_end_ms=CALIBRATION_END_MS,
                holdout_start_ms=CALIBRATION_END_MS - 1,
                holdout_end_ms=CALIBRATION_END_MS - 1 + DAY_MS,
                replay=replay,
            )
        )
    with pytest.raises(ValueError, match="calibration range must be exactly"):
        asyncio.run(
            run_lag_test(
                database_url="postgresql://reader@example/test",
                calibration_start_ms=START_MS,
                calibration_end_ms=START_MS,
                holdout_start_ms=HOLDOUT_START_MS,
                holdout_end_ms=HOLDOUT_END_MS,
                replay=replay,
            )
        )


def test_lag_test_json_keeps_decimal_strings_and_writes_one_document(tmp_path):
    result = {
        "status": "observed_shorter_better",
        "mae_skill_difference": Decimal("0.0500"),
    }
    path = tmp_path / "lag-test.json"

    write_lag_test_result(path, result)

    assert json.loads(path.read_text(encoding="utf-8")) == {
        "mae_skill_difference": "0.0500",
        "status": "observed_shorter_better",
    }
    assert encode_lag_test_result(result).count("observed_shorter_better") == 1
    assert list(tmp_path.iterdir()) == [path]
