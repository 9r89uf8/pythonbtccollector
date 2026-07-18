from __future__ import annotations

"""Small, read-only calibration/holdout test for shorter catch-up lags."""

import argparse
import asyncio
import json
import os
import sys
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

import asyncpg

from price_collector.shadow_signal_replay import (
    DEFAULT_DATABASE_CHUNK_MS,
    ReplayConfig,
    ReplayDataError,
    ReplayReport,
    replay_from_database,
)


CALIBRATION_LAGS_MS = (1_500, 2_000, 2_500, 3_000, 3_500)
SHORTER_LAGS_MS = (1_500, 2_000, 2_500)
REFERENCE_LAG_MS = 3_000
TEST_WINDOW_MS = 86_400_000
MIN_COMMON_SCORED = 10_000
MIN_COMMON_VALID_COVERAGE = Decimal("0.50")
MIN_COMMON_MATURATION_COVERAGE = Decimal("0.99")

ReplayRunner = Callable[..., Awaitable[ReplayReport]]


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def encode_lag_test_result(result: Mapping[str, Any]) -> str:
    return json.dumps(_json_ready(result), indent=2, sort_keys=True)


def write_lag_test_result(path: Path, result: Mapping[str, Any]) -> None:
    if not isinstance(path, Path):
        raise TypeError("path must be pathlib.Path")
    temporary_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary_path.write_text(
            encode_lag_test_result(result) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _candidate_by_lag(report: ReplayReport) -> dict[int, Mapping[str, Any]]:
    candidates: dict[int, Mapping[str, Any]] = {}
    for summary in report.candidate_summaries:
        lag_ms = summary.get("horizon_ms")
        if isinstance(lag_ms, bool) or not isinstance(lag_ms, int):
            raise ReplayDataError("replay candidate has an invalid horizon_ms")
        if lag_ms in candidates:
            raise ReplayDataError("replay candidate horizons are not unique")
        candidates[lag_ms] = summary
    return candidates


def _common_skill(summary: Mapping[str, Any]) -> Optional[Decimal]:
    try:
        value = summary["common_cohort"]["metrics"][
            "mae_skill_vs_no_change"
        ]
    except (KeyError, TypeError) as exc:
        raise ReplayDataError(
            "replay candidate is missing common-cohort MAE skill"
        ) from exc
    if value is None:
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ReplayDataError(
            "common-cohort MAE skill must be a finite Decimal or null"
        )
    return value


def _evidence_gate(summary: Mapping[str, Any]) -> dict[str, Any]:
    common = summary["common_cohort"]
    target_eligible = common["target_eligible"]
    valid_generated = common["valid_generated"]
    scored = common["scored"]
    valid_coverage = (
        Decimal(valid_generated) / Decimal(target_eligible)
        if target_eligible
        else Decimal("0")
    )
    maturation_coverage = (
        Decimal(scored) / Decimal(valid_generated)
        if valid_generated
        else Decimal("0")
    )
    passed = (
        scored >= MIN_COMMON_SCORED
        and valid_coverage >= MIN_COMMON_VALID_COVERAGE
        and maturation_coverage >= MIN_COMMON_MATURATION_COVERAGE
    )
    return {
        "passed": passed,
        "valid_coverage": valid_coverage,
        "maturation_coverage": maturation_coverage,
    }


def _compact_candidate(summary: Mapping[str, Any]) -> dict[str, Any]:
    common = summary["common_cohort"]
    metrics = common["metrics"]
    evidence_gate = _evidence_gate(summary)
    return {
        "model_version": summary["model_version"],
        "lag_ms": summary["horizon_ms"],
        "target_eligible": common["target_eligible"],
        "valid_generated": common["valid_generated"],
        "scored": common["scored"],
        "scored_coverage": common["scored_coverage"],
        "maturation_coverage": common["maturation_coverage"],
        "valid_coverage": evidence_gate["valid_coverage"],
        "evidence_gate_passed": evidence_gate["passed"],
        "model_mae_usd": metrics["model_mean_absolute_error_usd"],
        "baseline_mae_usd": metrics["baseline_mean_absolute_error_usd"],
        "mae_skill_vs_no_change": metrics["mae_skill_vs_no_change"],
        "wins": metrics["wins"],
        "ties": metrics["ties"],
        "losses": metrics["losses"],
    }


def _calibration_decision(report: ReplayReport) -> dict[str, Any]:
    if report.status != "ok":
        return {
            "status": "insufficient_evidence",
            "reason": f"calibration_replay_{report.status}",
            "winner_lag_ms": None,
            "ranking": [],
        }
    candidates = _candidate_by_lag(report)
    if set(candidates) != set(CALIBRATION_LAGS_MS):
        raise ReplayDataError("calibration replay returned the wrong lag family")

    ranked: list[tuple[Decimal, int, Mapping[str, Any]]] = []
    for lag_ms in SHORTER_LAGS_MS:
        summary = candidates[lag_ms]
        if not _evidence_gate(summary)["passed"]:
            return {
                "status": "insufficient_evidence",
                "reason": f"calibration_evidence_gate_failed_lag_{lag_ms}",
                "winner_lag_ms": None,
                "ranking": [],
            }
        skill = _common_skill(summary)
        if skill is None:
            return {
                "status": "insufficient_evidence",
                "reason": f"calibration_skill_missing_lag_{lag_ms}",
                "winner_lag_ms": None,
                "ranking": [],
            }
        ranked.append((skill, lag_ms, summary))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    ranking = [_compact_candidate(item[2]) for item in ranked]
    best_skill, winner_lag_ms, _summary = ranked[0]
    if best_skill <= 0:
        return {
            "status": "retain_3000_reference",
            "reason": "no_shorter_lag_had_positive_calibration_skill",
            "winner_lag_ms": None,
            "ranking": ranking,
        }
    return {
        "status": "challenger_selected",
        "reason": "highest_positive_shorter_common_cohort_mae_skill",
        "winner_lag_ms": winner_lag_ms,
        "ranking": ranking,
    }


def _holdout_decision(
    report: ReplayReport,
    *,
    challenger_lag_ms: int,
) -> dict[str, Any]:
    if report.status != "ok":
        return {
            "status": "insufficient_evidence",
            "reason": f"holdout_replay_{report.status}",
            "challenger_lag_ms": challenger_lag_ms,
        }
    candidates = _candidate_by_lag(report)
    if set(candidates) != {challenger_lag_ms, REFERENCE_LAG_MS}:
        raise ReplayDataError("holdout replay returned the wrong lag pair")
    challenger = candidates[challenger_lag_ms]
    reference = candidates[REFERENCE_LAG_MS]
    if not _evidence_gate(challenger)["passed"]:
        return {
            "status": "insufficient_evidence",
            "reason": "holdout_challenger_evidence_gate_failed",
            "challenger_lag_ms": challenger_lag_ms,
        }
    if not _evidence_gate(reference)["passed"]:
        return {
            "status": "insufficient_evidence",
            "reason": "holdout_reference_evidence_gate_failed",
            "challenger_lag_ms": challenger_lag_ms,
        }
    challenger_skill = _common_skill(challenger)
    reference_skill = _common_skill(reference)
    if challenger_skill is None or reference_skill is None:
        return {
            "status": "insufficient_evidence",
            "reason": "holdout_skill_missing",
            "challenger_lag_ms": challenger_lag_ms,
        }
    skill_difference = challenger_skill - reference_skill
    shorter_better = challenger_skill > 0 and skill_difference > 0
    return {
        "status": (
            "observed_shorter_better"
            if shorter_better
            else "retain_3000_reference"
        ),
        "reason": (
            "challenger_positive_and_strictly_better_than_3000_reference"
            if shorter_better
            else "challenger_not_positive_or_not_better_than_3000_reference"
        ),
        "challenger_lag_ms": challenger_lag_ms,
        "challenger": _compact_candidate(challenger),
        "reference": _compact_candidate(reference),
        "mae_skill_difference": skill_difference,
    }


def _replay_config(
    *,
    start_ms: int,
    end_ms: int,
    lags_ms: tuple[int, ...],
    futures_stale_ms: int,
    chainlink_stale_ms: int,
    history_retention_ms: int,
) -> ReplayConfig:
    return ReplayConfig(
        start_ms=start_ms,
        end_ms=end_ms,
        lags_ms=lags_ms,
        beta=Decimal("1"),
        poll_ms=100,
        evaluation_interval_ms=500,
        futures_stale_ms=futures_stale_ms,
        chainlink_stale_ms=chainlink_stale_ms,
        reference_max_gap_ms=250,
        history_retention_ms=history_retention_ms,
        max_future_skew_ms=0,
        futures_availability_delay_ms=100,
        chainlink_availability_delay_ms=100,
        evaluation_phase_offset_ms=0,
        exclude_parse_error_sessions=True,
    )


async def run_lag_test(
    *,
    database_url: str,
    calibration_start_ms: int,
    calibration_end_ms: int,
    holdout_start_ms: int,
    holdout_end_ms: int,
    futures_stale_ms: int = 1_000,
    chainlink_stale_ms: int = 5_000,
    history_retention_ms: int = 10_000,
    chunk_ms: int = DEFAULT_DATABASE_CHUNK_MS,
    replay: Optional[ReplayRunner] = None,
) -> dict[str, Any]:
    if not isinstance(database_url, str) or not database_url:
        raise ValueError("DATABASE_URL is required")
    if calibration_end_ms - calibration_start_ms != TEST_WINDOW_MS:
        raise ValueError("calibration range must be exactly 24 hours")
    if holdout_end_ms - holdout_start_ms != TEST_WINDOW_MS:
        raise ValueError("holdout range must be exactly 24 hours")
    if calibration_end_ms > holdout_start_ms:
        raise ValueError("calibration and holdout ranges must not overlap")
    replay_runner = replay or replay_from_database
    calibration_config = _replay_config(
        start_ms=calibration_start_ms,
        end_ms=calibration_end_ms,
        lags_ms=CALIBRATION_LAGS_MS,
        futures_stale_ms=futures_stale_ms,
        chainlink_stale_ms=chainlink_stale_ms,
        history_retention_ms=history_retention_ms,
    )
    holdout_range_config = _replay_config(
        start_ms=holdout_start_ms,
        end_ms=holdout_end_ms,
        lags_ms=(SHORTER_LAGS_MS[0], REFERENCE_LAG_MS),
        futures_stale_ms=futures_stale_ms,
        chainlink_stale_ms=chainlink_stale_ms,
        history_retention_ms=history_retention_ms,
    )
    calibration_report = await replay_runner(
        database_url=database_url,
        config=calibration_config,
        chunk_ms=chunk_ms,
    )
    calibration = _calibration_decision(calibration_report)
    result: dict[str, Any] = {
        "schema_version": 1,
        "mode": "simple_shadow_lag_test",
        "status": calibration["status"],
        "calibration_range": {
            "start_ms": calibration_start_ms,
            "end_ms": calibration_end_ms,
            "boundary": "[start_ms,end_ms)",
        },
        "holdout_range": {
            "start_ms": holdout_start_ms,
            "end_ms": holdout_end_ms,
            "boundary": "[start_ms,end_ms)",
        },
        "fixed_replay_settings": {
            "calibration_lags_ms": list(CALIBRATION_LAGS_MS),
            "shorter_lags_ms": list(SHORTER_LAGS_MS),
            "reference_lag_ms": REFERENCE_LAG_MS,
            "beta": Decimal("1"),
            "poll_ms": 100,
            "evaluation_interval_ms": 500,
            "futures_stale_ms": futures_stale_ms,
            "chainlink_stale_ms": chainlink_stale_ms,
            "reference_max_gap_ms": 250,
            "history_retention_ms": history_retention_ms,
            "max_future_skew_ms": 0,
            "futures_availability_delay_ms": 100,
            "chainlink_availability_delay_ms": 100,
            "evaluation_phase_offset_ms": 0,
            "exclude_parse_error_sessions": True,
            "minimum_common_scored": MIN_COMMON_SCORED,
            "minimum_common_valid_coverage": MIN_COMMON_VALID_COVERAGE,
            "minimum_common_maturation_coverage": (
                MIN_COMMON_MATURATION_COVERAGE
            ),
        },
        "selection_rule": {
            "metric": "common_cohort_mae_skill_vs_no_change",
            "calibration": "highest positive shorter-lag skill",
            "exact_tie_break": "smaller_lag_ms",
            "holdout": (
                "challenger skill must be positive and strictly exceed the "
                "3000 ms fixed-settings reference"
            ),
            "holdout_reranking": False,
            "production_promotion": False,
        },
        "calibration": calibration,
        "holdout": None,
        "calibration_report": calibration_report.to_dict(),
        "holdout_report": None,
    }
    winner_lag_ms = calibration["winner_lag_ms"]
    if winner_lag_ms is None:
        return result

    holdout_config = replace(
        holdout_range_config,
        lags_ms=(winner_lag_ms, REFERENCE_LAG_MS),
    )
    holdout_report = await replay_runner(
        database_url=database_url,
        config=holdout_config,
        chunk_ms=chunk_ms,
    )
    holdout = _holdout_decision(
        holdout_report,
        challenger_lag_ms=winner_lag_ms,
    )
    result["status"] = holdout["status"]
    result["holdout"] = holdout
    result["holdout_report"] = holdout_report.to_dict()
    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one simple shorter-lag calibration and holdout test"
    )
    parser.add_argument("--calibration-start-ms", type=int, required=True)
    parser.add_argument("--calibration-end-ms", type=int, required=True)
    parser.add_argument("--holdout-start-ms", type=int, required=True)
    parser.add_argument("--holdout-end-ms", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--futures-stale-ms", type=int, default=1_000)
    parser.add_argument("--chainlink-stale-ms", type=int, default=5_000)
    parser.add_argument("--history-retention-ms", type=int, default=10_000)
    parser.add_argument("--chunk-ms", type=int, default=DEFAULT_DATABASE_CHUNK_MS)
    return parser


async def _run_from_arguments(arguments: argparse.Namespace) -> dict[str, Any]:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required in the environment")
    return await run_lag_test(
        database_url=database_url,
        calibration_start_ms=arguments.calibration_start_ms,
        calibration_end_ms=arguments.calibration_end_ms,
        holdout_start_ms=arguments.holdout_start_ms,
        holdout_end_ms=arguments.holdout_end_ms,
        futures_stale_ms=arguments.futures_stale_ms,
        chainlink_stale_ms=arguments.chainlink_stale_ms,
        history_retention_ms=arguments.history_retention_ms,
        chunk_ms=arguments.chunk_ms,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        result = asyncio.run(_run_from_arguments(arguments))
        write_lag_test_result(arguments.output, result)
    except (
        OSError,
        RuntimeError,
        ValueError,
        ReplayDataError,
        asyncpg.PostgresError,
    ) as exc:
        print(f"shadow lag test failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
