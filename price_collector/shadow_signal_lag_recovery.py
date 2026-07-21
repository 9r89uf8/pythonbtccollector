from __future__ import annotations

"""One-time, descriptive recovery for the July 2026 shorter-lag test."""

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import asyncpg

from price_collector.shadow_signal_lag_test import (
    CALIBRATION_LAGS_MS,
    MIN_COMMON_MATURATION_COVERAGE,
    MIN_COMMON_SCORED,
    MIN_COMMON_VALID_COVERAGE,
    REFERENCE_LAG_MS,
    SHORTER_LAGS_MS,
    ReplayRunner,
    encode_lag_test_result,
    run_lag_test,
)
from price_collector.shadow_signal_replay import (
    CHAINLINK_SESSION_SOURCE,
    DEFAULT_DATABASE_CHUNK_MS,
    FUTURES_SESSION_SOURCE,
    ReplayDataError,
)


ORIGINAL_RESULT_BASENAME = "shadow-lag-test-20260719-20260721.json"
RECOVERY_RESULT_BASENAME = (
    "shadow-lag-test-20260719-20260721-posthoc-descriptive-recovery.json"
)
ORIGINAL_RESULT_SHA256 = (
    "2e715151b011dc051f0064490ad1c5a29c319f6aa054bc71edbee7cdf4251f5a"
)
OPERATOR_RECORDED_SOURCE_GIT_COMMIT = (
    "ab30ab67fd66b96199b1526c29e897dad7a4ea0e"
)
CALIBRATION_START_MS = 1_784_419_200_000
CALIBRATION_END_MS = 1_784_505_600_000
HOLDOUT_START_MS = 1_784_505_600_000
HOLDOUT_END_MS = 1_784_592_000_000
MAX_ORIGINAL_RESULT_BYTES = 1_000_000
RECOVERY_CHAINLINK_PARSE_ERROR_TOTALS = (0, 2)
EXPECTED_RECOVERY_CHANGED_PATHS = (
    "CHAINLINK_SHORTER_LAG_TEST_RUNBOOK.md",
    "OPERATIONS.md",
    "README.md",
    "price_collector/polymarket_chainlink_collector.py",
    "price_collector/shadow_signal_lag_recovery.py",
    "price_collector/shadow_signal_lag_test.py",
    "price_collector/shadow_signal_replay.py",
    "tests/test_polymarket_chainlink_collector.py",
    "tests/test_shadow_signal_lag_recovery.py",
    "tests/test_shadow_signal_lag_test.py",
    "tests/test_shadow_signal_replay.py",
)
RECOVERY_SESSION_CENSUS_SQL = """
SELECT source, parse_errors_total, count(*) AS sessions
FROM raw_capture.feed_sessions
WHERE connected_wall_ns < $2
  AND COALESCE(disconnected_wall_ns, $2) > $1
  AND source IN (
      'binance_futures_agg_trade',
      'polymarket_chainlink_rtds'
  )
GROUP BY source, parse_errors_total
ORDER BY source, parse_errors_total
"""

EXPECTED_CALIBRATION_RANGE = {
    "start_ms": CALIBRATION_START_MS,
    "end_ms": CALIBRATION_END_MS,
    "boundary": "[start_ms,end_ms)",
}
EXPECTED_HOLDOUT_RANGE = {
    "start_ms": HOLDOUT_START_MS,
    "end_ms": HOLDOUT_END_MS,
    "boundary": "[start_ms,end_ms)",
}
EXPECTED_ORIGINAL_FIXED_REPLAY_SETTINGS = {
    "calibration_lags_ms": list(CALIBRATION_LAGS_MS),
    "shorter_lags_ms": list(SHORTER_LAGS_MS),
    "reference_lag_ms": REFERENCE_LAG_MS,
    "beta": "1",
    "poll_ms": 100,
    "evaluation_interval_ms": 500,
    "futures_stale_ms": 1_000,
    "chainlink_stale_ms": 5_000,
    "reference_max_gap_ms": 250,
    "history_retention_ms": 10_000,
    "max_future_skew_ms": 0,
    "futures_availability_delay_ms": 100,
    "chainlink_availability_delay_ms": 100,
    "evaluation_phase_offset_ms": 0,
    "exclude_parse_error_sessions": True,
    "minimum_common_scored": MIN_COMMON_SCORED,
    "minimum_common_valid_coverage": format(
        MIN_COMMON_VALID_COVERAGE,
        "f",
    ),
    "minimum_common_maturation_coverage": format(
        MIN_COMMON_MATURATION_COVERAGE,
        "f",
    ),
}
EXPECTED_SELECTION_RULE = {
    "metric": "common_cohort_mae_skill_vs_no_change",
    "calibration": "highest positive shorter-lag skill",
    "exact_tie_break": "smaller_lag_ms",
    "holdout": (
        "challenger skill must be positive and strictly exceed the "
        "3000 ms fixed-settings reference"
    ),
    "holdout_reranking": False,
    "production_promotion": False,
}


class RecoveryValidationError(ValueError):
    """The frozen incident evidence does not match the recovery contract."""


@dataclass(frozen=True)
class OriginalRecoveryArtifact:
    path: Path
    sha256: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class RecoverySourceProvenance:
    git_commit: str
    parent_git_commit: str
    changed_paths_from_original: tuple[str, ...]


def _reject_duplicate_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RecoveryValidationError(
                f"original result contains duplicate JSON key {key!r}"
            )
        result[key] = value
    return result


def _reject_nonstandard_json_constant(value: str) -> None:
    raise RecoveryValidationError(
        f"original result contains non-standard JSON constant {value}"
    )


def _require_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RecoveryValidationError(f"{field_name} must be an object")
    return value


def _require_equal(actual: Any, expected: Any, field_name: str) -> None:
    if actual != expected:
        raise RecoveryValidationError(
            f"{field_name} does not match the frozen incident contract"
        )


def load_original_recovery_artifact(path: Path) -> OriginalRecoveryArtifact:
    if not isinstance(path, Path):
        raise TypeError("path must be pathlib.Path")
    if path.name != ORIGINAL_RESULT_BASENAME:
        raise RecoveryValidationError(
            f"original result basename must be {ORIGINAL_RESULT_BASENAME}"
        )
    raw = path.read_bytes()
    if not raw:
        raise RecoveryValidationError("original result is empty")
    if len(raw) > MAX_ORIGINAL_RESULT_BYTES:
        raise RecoveryValidationError("original result exceeds the size limit")
    sha256 = hashlib.sha256(raw).hexdigest()
    if sha256 != ORIGINAL_RESULT_SHA256:
        raise RecoveryValidationError(
            "original result SHA-256 does not match the frozen failed artifact"
        )
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_object_keys,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryValidationError(
            "original result is not valid UTF-8 JSON"
        ) from exc
    payload = _require_mapping(payload, "original result")

    _require_equal(payload.get("schema_version"), 1, "schema_version")
    _require_equal(payload.get("mode"), "simple_shadow_lag_test", "mode")
    _require_equal(payload.get("status"), "insufficient_evidence", "status")
    _require_equal(
        payload.get("calibration_range"),
        EXPECTED_CALIBRATION_RANGE,
        "calibration_range",
    )
    _require_equal(
        payload.get("holdout_range"),
        EXPECTED_HOLDOUT_RANGE,
        "holdout_range",
    )
    _require_equal(
        payload.get("fixed_replay_settings"),
        EXPECTED_ORIGINAL_FIXED_REPLAY_SETTINGS,
        "fixed_replay_settings",
    )
    _require_equal(payload.get("holdout"), None, "holdout")
    _require_equal(payload.get("holdout_report"), None, "holdout_report")

    selection_rule = _require_mapping(
        payload.get("selection_rule"),
        "selection_rule",
    )
    _require_equal(
        selection_rule,
        EXPECTED_SELECTION_RULE,
        "selection_rule",
    )
    calibration = _require_mapping(payload.get("calibration"), "calibration")
    _require_equal(
        calibration.get("status"),
        "insufficient_evidence",
        "calibration.status",
    )
    _require_equal(
        calibration.get("reason"),
        "calibration_replay_no_eligible_segments",
        "calibration.reason",
    )
    _require_equal(
        calibration.get("winner_lag_ms"),
        None,
        "calibration.winner_lag_ms",
    )

    calibration_report = _require_mapping(
        payload.get("calibration_report"),
        "calibration_report",
    )
    _require_equal(
        calibration_report.get("status"),
        "no_eligible_segments",
        "calibration_report.status",
    )
    _require_equal(
        calibration_report.get("range"),
        EXPECTED_CALIBRATION_RANGE,
        "calibration_report.range",
    )
    data_quality = _require_mapping(
        calibration_report.get("data_quality"),
        "calibration_report.data_quality",
    )
    expected_quality_fields = {
        "sessions_total_by_source": {
            FUTURES_SESSION_SOURCE: 2,
            CHAINLINK_SESSION_SOURCE: 15,
        },
        "sessions_eligible_by_source": {FUTURES_SESSION_SOURCE: 2},
        "sessions_excluded_by_reason": {"parse_errors": 15},
        "excluded_integrity_scope_raw_rows": 94_664,
        "common_healthy_segments": 0,
        "input_events": 0,
        "ignored_events": 0,
        "polls_processed": 0,
    }
    for field_name, expected in expected_quality_fields.items():
        _require_equal(
            data_quality.get(field_name),
            expected,
            f"calibration_report.data_quality.{field_name}",
        )

    return OriginalRecoveryArtifact(path=path, sha256=sha256, payload=payload)


def validate_recovery_output_path(
    *,
    original_path: Path,
    output_path: Path,
) -> None:
    if not isinstance(original_path, Path) or not isinstance(output_path, Path):
        raise TypeError("original_path and output_path must be pathlib.Path values")
    if output_path.name != RECOVERY_RESULT_BASENAME:
        raise RecoveryValidationError(
            f"recovery output basename must be {RECOVERY_RESULT_BASENAME}"
        )
    if original_path.resolve() == output_path.resolve():
        raise RecoveryValidationError(
            "recovery output must differ from original result"
        )
    if output_path.exists():
        raise RecoveryValidationError(
            "recovery output already exists; refusing overwrite"
        )
    if not output_path.parent.is_dir():
        raise RecoveryValidationError("recovery output directory does not exist")


def _git_stdout(repository_path: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository_path), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RecoveryValidationError("git provenance check could not run") from exc
    if completed.returncode != 0:
        raise RecoveryValidationError(
            f"git provenance check failed: {' '.join(arguments)}"
        )
    return completed.stdout.strip()


def load_recovery_source_provenance(
    repository_path: Path,
) -> RecoverySourceProvenance:
    if not isinstance(repository_path, Path):
        raise TypeError("repository_path must be pathlib.Path")
    git_commit = _git_stdout(repository_path, "rev-parse", "HEAD")
    if len(git_commit) != 40 or any(
        character not in "0123456789abcdef" for character in git_commit
    ):
        raise RecoveryValidationError("recovery Git commit is not a full SHA-1")

    parents = _git_stdout(
        repository_path,
        "rev-list",
        "--parents",
        "-n",
        "1",
        "HEAD",
    ).split()
    if parents != [git_commit, OPERATOR_RECORDED_SOURCE_GIT_COMMIT]:
        raise RecoveryValidationError(
            "recovery commit must be the single direct child of the original "
            "operator-recorded commit"
        )

    status = _git_stdout(
        repository_path,
        "status",
        "--porcelain",
        "--untracked-files=all",
    )
    if status:
        raise RecoveryValidationError(
            "recovery repository must have a completely clean worktree"
        )

    changed_paths = tuple(
        sorted(
            line
            for line in _git_stdout(
                repository_path,
                "diff",
                "--name-only",
                f"{OPERATOR_RECORDED_SOURCE_GIT_COMMIT}..HEAD",
            ).splitlines()
            if line
        )
    )
    if changed_paths != EXPECTED_RECOVERY_CHANGED_PATHS:
        raise RecoveryValidationError(
            "recovery commit changed files outside the audited checkpoint"
        )
    return RecoverySourceProvenance(
        git_commit=git_commit,
        parent_git_commit=OPERATOR_RECORDED_SOURCE_GIT_COMMIT,
        changed_paths_from_original=changed_paths,
    )


def validate_recovery_session_census(
    census: Mapping[str, Any],
) -> dict[str, dict[str, int]]:
    census = _require_mapping(census, "full-range session census")
    expected_sources = {FUTURES_SESSION_SOURCE, CHAINLINK_SESSION_SOURCE}
    if set(census) != expected_sources:
        raise RecoveryValidationError(
            "full-range session census must contain exactly both replay sources"
        )

    normalized: dict[str, dict[str, int]] = {}
    for source in (FUTURES_SESSION_SOURCE, CHAINLINK_SESSION_SOURCE):
        source_counts = _require_mapping(
            census.get(source),
            f"full-range session census for {source}",
        )
        normalized_counts: dict[str, int] = {}
        for parse_errors_total, session_count in source_counts.items():
            if not isinstance(parse_errors_total, str):
                raise RecoveryValidationError(
                    "session-census parse totals must be decimal strings"
                )
            if isinstance(session_count, bool) or not isinstance(session_count, int):
                raise RecoveryValidationError(
                    "session-census counts must be integers"
                )
            if session_count <= 0:
                raise RecoveryValidationError(
                    "session-census counts must be positive"
                )
            normalized_counts[parse_errors_total] = session_count
        normalized[source] = normalized_counts

    futures_counts = normalized[FUTURES_SESSION_SOURCE]
    chainlink_counts = normalized[CHAINLINK_SESSION_SOURCE]
    if set(futures_counts) != {"0"}:
        raise RecoveryValidationError(
            "full-range Futures sessions must all have parse-error total zero"
        )
    if chainlink_counts != {"0": 1, "2": 29}:
        raise RecoveryValidationError(
            "full-range Chainlink census must match the frozen 0:1, 2:29 "
            "incident population"
        )
    return normalized


async def load_recovery_session_census(
    database_url: str,
) -> dict[str, dict[str, int]]:
    if not isinstance(database_url, str) or not database_url:
        raise ValueError("DATABASE_URL is required")
    connection = await asyncpg.connect(
        dsn=database_url,
        server_settings={
            "application_name": "price_collector_shadow_lag_recovery_preflight",
            "statement_timeout": "1500",
            "lock_timeout": "1000",
            "default_transaction_read_only": "on",
        },
    )
    try:
        rows = await connection.fetch(
            RECOVERY_SESSION_CENSUS_SQL,
            CALIBRATION_START_MS * 1_000_000,
            HOLDOUT_END_MS * 1_000_000,
        )
    finally:
        await connection.close()

    census: dict[str, dict[str, int]] = {
        FUTURES_SESSION_SOURCE: {},
        CHAINLINK_SESSION_SOURCE: {},
    }
    for row in rows:
        source = row["source"]
        parse_errors_total = row["parse_errors_total"]
        session_count = row["sessions"]
        if source not in census:
            raise RecoveryValidationError(
                "session census returned an unexpected source"
            )
        census[source][str(parse_errors_total)] = session_count
    return validate_recovery_session_census(census)


def _validate_recovery_report(report: Mapping[str, Any], phase: str) -> None:
    configuration = _require_mapping(
        report.get("configuration"),
        f"{phase}_report.configuration",
    )
    _require_equal(
        configuration.get("exclude_parse_error_sessions"),
        True,
        f"{phase}_report.configuration.exclude_parse_error_sessions",
    )
    _require_equal(
        configuration.get("allowed_chainlink_parse_error_totals"),
        list(RECOVERY_CHAINLINK_PARSE_ERROR_TOTALS),
        f"{phase}_report.configuration.allowed_chainlink_parse_error_totals",
    )
    data_quality = _require_mapping(
        report.get("data_quality"),
        f"{phase}_report.data_quality",
    )
    _require_equal(
        data_quality.get("session_policy"),
        "completed_integrity_checked_with_exact_chainlink_parse_error_allowlist",
        f"{phase}_report.data_quality.session_policy",
    )
    excluded = _require_mapping(
        data_quality.get("sessions_excluded_by_reason"),
        f"{phase}_report.data_quality.sessions_excluded_by_reason",
    )
    if excluded.get("parse_errors", 0) != 0:
        raise RecoveryValidationError(
            f"{phase} contains a session outside the exact recovery parse policy"
        )

    census = _require_mapping(
        data_quality.get("parse_error_totals_by_source"),
        f"{phase}_report.data_quality.parse_error_totals_by_source",
    )
    futures_counts = _require_mapping(
        census.get(FUTURES_SESSION_SOURCE),
        f"{phase} futures parse-error census",
    )
    chainlink_counts = _require_mapping(
        census.get(CHAINLINK_SESSION_SOURCE),
        f"{phase} Chainlink parse-error census",
    )
    if set(futures_counts) - {"0"}:
        raise RecoveryValidationError(
            f"{phase} contains a Futures session with parse errors"
        )
    if set(chainlink_counts) - {"0", "2"}:
        raise RecoveryValidationError(
            f"{phase} contains a Chainlink session outside parse totals 0 or 2"
        )
    exception_count = chainlink_counts.get("2", 0)
    if isinstance(exception_count, bool) or not isinstance(exception_count, int):
        raise RecoveryValidationError(
            f"{phase} Chainlink exact-two census count must be an integer"
        )
    if phase == "calibration" and exception_count <= 0:
        raise RecoveryValidationError(
            "calibration did not exercise the incident-specific exception"
        )
    applied = _require_mapping(
        data_quality.get("parse_error_exception_applied_by_source"),
        f"{phase}_report.data_quality.parse_error_exception_applied_by_source",
    )
    if set(applied) - {CHAINLINK_SESSION_SOURCE}:
        raise RecoveryValidationError(
            f"{phase} applied a non-Chainlink parse-error exception"
        )
    _require_equal(
        applied.get(CHAINLINK_SESSION_SOURCE, 0),
        exception_count,
        f"{phase} applied Chainlink exact-two session count",
    )


def _validate_recovered_analysis(
    recovered: Mapping[str, Any],
    original: OriginalRecoveryArtifact,
) -> None:
    expected_settings = dict(EXPECTED_ORIGINAL_FIXED_REPLAY_SETTINGS)
    expected_settings["allowed_chainlink_parse_error_totals"] = list(
        RECOVERY_CHAINLINK_PARSE_ERROR_TOTALS
    )
    normalized_settings = json.loads(
        encode_lag_test_result(
            {"settings": recovered.get("fixed_replay_settings")}
        )
    )["settings"]
    _require_equal(
        normalized_settings,
        expected_settings,
        "recovered_analysis.fixed_replay_settings",
    )
    _require_equal(
        recovered.get("calibration_range"),
        original.payload.get("calibration_range"),
        "recovered_analysis.calibration_range",
    )
    _require_equal(
        recovered.get("holdout_range"),
        original.payload.get("holdout_range"),
        "recovered_analysis.holdout_range",
    )
    selection_rule = _require_mapping(
        recovered.get("selection_rule"),
        "recovered_analysis.selection_rule",
    )
    _require_equal(
        selection_rule,
        original.payload.get("selection_rule"),
        "recovered_analysis.selection_rule",
    )
    calibration_report = _require_mapping(
        recovered.get("calibration_report"),
        "recovered_analysis.calibration_report",
    )
    _validate_recovery_report(calibration_report, "calibration")
    holdout_report = recovered.get("holdout_report")
    if holdout_report is not None:
        _validate_recovery_report(
            _require_mapping(
                holdout_report,
                "recovered_analysis.holdout_report",
            ),
            "holdout",
        )


async def run_posthoc_lag_recovery(
    *,
    database_url: str,
    original: OriginalRecoveryArtifact,
    recovery_source: RecoverySourceProvenance,
    chunk_ms: int = DEFAULT_DATABASE_CHUNK_MS,
    parse_error_census: Optional[Mapping[str, Any]] = None,
    replay: Optional[ReplayRunner] = None,
) -> dict[str, Any]:
    if not isinstance(recovery_source, RecoverySourceProvenance):
        raise TypeError("recovery_source must be RecoverySourceProvenance")
    if parse_error_census is None:
        parse_error_census = await load_recovery_session_census(database_url)
    validated_census = validate_recovery_session_census(parse_error_census)
    recovered = await run_lag_test(
        database_url=database_url,
        calibration_start_ms=CALIBRATION_START_MS,
        calibration_end_ms=CALIBRATION_END_MS,
        holdout_start_ms=HOLDOUT_START_MS,
        holdout_end_ms=HOLDOUT_END_MS,
        futures_stale_ms=1_000,
        chainlink_stale_ms=5_000,
        history_retention_ms=10_000,
        chunk_ms=chunk_ms,
        allowed_chainlink_parse_error_totals=(0, 2),
        replay=replay,
    )
    _validate_recovered_analysis(recovered, original)

    underlying_status = recovered.get("status")
    if underlying_status == "insufficient_evidence":
        outer_status = "insufficient_evidence"
    else:
        outer_status = "recovery_complete"
    decision = recovered.get("holdout") or recovered.get("calibration")
    decision = _require_mapping(decision, "recovered analysis decision")

    return {
        "schema_version": 1,
        "mode": "posthoc_shadow_lag_recovery",
        "status": outer_status,
        "evidence_class": "descriptive_only",
        "eligible_for_production_promotion": False,
        "original_result_preserved": True,
        "conclusion": {
            "recovered_analysis_status": underlying_status,
            "reason": decision.get("reason"),
            "winner_lag_ms": recovered["calibration"].get("winner_lag_ms"),
            "requires_future_clean_calibration_and_untouched_holdout": True,
        },
        "provenance": {
            "original_artifact": {
                "basename": ORIGINAL_RESULT_BASENAME,
                "sha256": original.sha256,
                "operator_recorded_source_git_commit": (
                    OPERATOR_RECORDED_SOURCE_GIT_COMMIT
                ),
                "source_commit_was_not_embedded_in_original_json": True,
                "status": original.payload["status"],
                "reason": original.payload["calibration"]["reason"],
                "calibration_range": original.payload["calibration_range"],
                "holdout_range": original.payload["holdout_range"],
            },
            "configuration_delta_from_original": {
                "only_changed_frozen_replay_setting": (
                    "allowed_chainlink_parse_error_totals"
                ),
                "original_effective_value": [0],
                "recovery_value": [0, 2],
                "all_model_timing_evidence_and_decision_settings_unchanged": True,
            },
            "recovery_implementation": {
                "git_commit": recovery_source.git_commit,
                "parent_git_commit": recovery_source.parent_git_commit,
                "single_direct_child_of_original_commit": True,
                "worktree_clean_at_start": True,
                "changed_paths_from_original": list(
                    recovery_source.changed_paths_from_original
                ),
            },
            "recovery_policy": {
                "futures_allowed_parse_error_totals": [0],
                "chainlink_allowed_parse_error_totals": [0, 2],
                "all_other_session_and_integrity_gates_unchanged": True,
                "classification_basis": "persisted_session_counters_only",
                "operator_observed_chainlink_48h_session_counter_distribution": {
                    "0": 1,
                    "2": 29,
                },
                "database_preflight_parse_error_totals_by_source": (
                    validated_census
                ),
                "operator_observed_journal_error_line_counts": {
                    "empty_json_frame": 6,
                    "unexpected_crypto_prices_history_topic": 6,
                },
                "limitation": (
                    "Rejected frame bodies were not persisted, and retained "
                    "journals do not prove both startup causes for every exact-two "
                    "session. This recovery is therefore post-hoc and descriptive."
                ),
            },
        },
        "recovered_analysis": recovered,
    }


def write_recovery_result_exclusive(
    path: Path,
    result: Mapping[str, Any],
) -> None:
    if not isinstance(path, Path):
        raise TypeError("path must be pathlib.Path")
    encoded = (encode_lag_test_result(result) + "\n").encode("utf-8")
    temporary_path = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    descriptor: Optional[int] = None
    try:
        descriptor = os.open(
            temporary_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o640,
        )
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
            if hasattr(os, "fchmod"):
                os.fchmod(stream.fileno(), 0o640)
        os.link(temporary_path, path)
        if os.name == "posix":
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            directory_descriptor = os.open(path.parent, directory_flags)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the pinned, post-hoc descriptive recovery for the failed "
            "July 2026 shorter-lag artifact"
        )
    )
    parser.add_argument("--original-result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--chunk-ms", type=int, default=DEFAULT_DATABASE_CHUNK_MS)
    return parser


async def _run_from_arguments(
    arguments: argparse.Namespace,
    original: OriginalRecoveryArtifact,
    recovery_source: RecoverySourceProvenance,
) -> dict[str, Any]:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required in the environment")
    return await run_posthoc_lag_recovery(
        database_url=database_url,
        original=original,
        recovery_source=recovery_source,
        chunk_ms=arguments.chunk_ms,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        original = load_original_recovery_artifact(arguments.original_result)
        validate_recovery_output_path(
            original_path=arguments.original_result,
            output_path=arguments.output,
        )
        recovery_source = load_recovery_source_provenance(
            Path(__file__).resolve().parents[1]
        )
        result = asyncio.run(
            _run_from_arguments(arguments, original, recovery_source)
        )
        original_after_replay = load_original_recovery_artifact(
            arguments.original_result
        )
        if original_after_replay.sha256 != original.sha256:
            raise RecoveryValidationError(
                "original result changed while the recovery was running"
            )
        write_recovery_result_exclusive(arguments.output, result)
    except (
        OSError,
        RuntimeError,
        ValueError,
        ReplayDataError,
        asyncpg.PostgresError,
    ) as exc:
        print(f"shadow lag recovery failed: {exc}", file=sys.stderr)
        return 1
    print(f"post-hoc descriptive recovery written: {arguments.output}")
    print(f"status: {result['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
