from __future__ import annotations

"""Fail-closed activation of an immutable shadow-signal selection artifact."""

import hashlib
import hmac
import json
import os
import re
import stat
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from price_collector.shadow_signal import CatchupModel


SELECTION_SCHEMA_VERSION = 2
SELECTION_MODE = "shadow_primary_selection"
SELECTION_POLICY_VERSION = "chronological_holdout_v2"
REPLAY_SCHEMA_VERSION = 2
SELECTION_SCHEMA_VERSION_V3 = 3
SELECTION_POLICY_VERSION_V3 = "chronological_holdout_v3"
REPLAY_SCHEMA_VERSION_V3 = 3
REPLAY_MODE = "shadow_raw_replay"
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_DECIMAL_CHARACTERS = 256

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_EXPECTED_MODEL_SPECS = (
    ("catchup_ratio_l3000_b100", 3_000, Decimal("1")),
    ("catchup_ratio_l3500_b100", 3_500, Decimal("1")),
    ("catchup_ratio_l4000_b100", 4_000, Decimal("1")),
)
_EXPECTED_POLICY_V2 = {
    "version": SELECTION_POLICY_VERSION,
    "supersedes": "chronological_holdout_v1",
    "revision_reason": (
        "paired win/loss frequency is diagnostic on autocorrelated "
        "500 ms rows; MAE and RMSE remain efficacy gates"
    ),
    "previously_inspected_holdouts_must_be_calibration": True,
    "new_later_holdout_required_after_revision": True,
    "candidate_set": [version for version, _lag, _beta in _EXPECTED_MODEL_SPECS],
    "calibration_and_holdout_are_explicit": True,
    "ranking_source": "common_cohort_only",
    "ranking_order": [
        "higher_calibration_mae_skill_vs_no_change",
        "higher_calibration_rmse_skill_vs_no_change",
    ],
    "exact_efficacy_tie_abstains": True,
    "holdout_reranking": False,
    "fallback_after_holdout_failure": False,
    "evidence_thresholds": {
        "minimum_calibration_reports": 1,
        "required_holdout_reports": 1,
        "minimum_common_scored_per_report": 10_000,
        "minimum_common_valid_coverage": "0.50",
        "minimum_common_maturation_coverage": "0.99",
        "minimum_slice_scored_for_warning": 500,
    },
    "efficacy_gates": {
        "mae_skill_vs_no_change": {
            "operator": ">",
            "threshold": "0",
        },
        "rmse_skill_vs_no_change": {
            "operator": ">",
            "threshold": "0",
        },
    },
    "diagnostics": {
        "paired_win_loss_frequency": {
            "hard_gate": False,
            "affects_eligibility": False,
            "affects_ranking": False,
            "warning_when": "wins_do_not_exceed_losses",
            "reason": "500_ms_rows_are_autocorrelated",
        }
    },
    "threshold_provenance": (
        "project_policy_v2_not_statistical_significance_and_not_engine_md"
    ),
}

# Keep the v2 names above as compatibility aliases: the currently promoted
# production decision is immutable v2 evidence.  V3 is a separate trust
# profile and cannot be mixed with either half of a v2 decision pair.
_EXPECTED_POLICY = _EXPECTED_POLICY_V2
_EXPECTED_POLICY_V3 = {
    "version": SELECTION_POLICY_VERSION_V3,
    "supersedes": "chronological_holdout_v2",
    "revision_reason": (
        "directional diagnostics use a full three-class confusion matrix "
        "that includes actions on neutral outcomes; MAE and RMSE remain "
        "efficacy gates; fixed replay visibility delays and evaluation "
        "phase are frozen sensitivity assumptions, with zero future skew"
    ),
    "previously_inspected_holdouts_must_be_calibration": True,
    "new_later_holdout_required_after_revision": True,
    "candidate_set": [version for version, _lag, _beta in _EXPECTED_MODEL_SPECS],
    "calibration_and_holdout_are_explicit": True,
    "ranking_source": "common_cohort_only",
    "ranking_order": [
        "higher_calibration_mae_skill_vs_no_change",
        "higher_calibration_rmse_skill_vs_no_change",
    ],
    "exact_efficacy_tie_abstains": True,
    "holdout_reranking": False,
    "fallback_after_holdout_failure": False,
    "evidence_thresholds": {
        "minimum_calibration_reports": 1,
        "required_holdout_reports": 1,
        "minimum_common_scored_per_report": 10_000,
        "minimum_common_valid_coverage": "0.50",
        "minimum_common_maturation_coverage": "0.99",
        "minimum_slice_scored_for_warning": 500,
    },
    "efficacy_gates": {
        "mae_skill_vs_no_change": {
            "operator": ">",
            "threshold": "0",
        },
        "rmse_skill_vs_no_change": {
            "operator": ">",
            "threshold": "0",
        },
    },
    "diagnostics": {
        "paired_win_loss_frequency": {
            "hard_gate": False,
            "affects_eligibility": False,
            "affects_ranking": False,
            "warning_when": "wins_do_not_exceed_losses",
            "reason": "500_ms_rows_are_autocorrelated",
        }
    },
    "threshold_provenance": (
        "project_policy_v3_not_statistical_significance_and_not_engine_md"
    ),
}


@dataclass(frozen=True)
class _ArtifactFormat:
    selection_schema_version: int
    selection_policy_version: str
    replay_schema_version: int
    expected_policy: Mapping[str, Any]
    require_zero_future_skew: bool
    require_visibility_assumptions: bool


_ARTIFACT_FORMATS = {
    SELECTION_SCHEMA_VERSION: _ArtifactFormat(
        selection_schema_version=SELECTION_SCHEMA_VERSION,
        selection_policy_version=SELECTION_POLICY_VERSION,
        replay_schema_version=REPLAY_SCHEMA_VERSION,
        expected_policy=_EXPECTED_POLICY_V2,
        require_zero_future_skew=False,
        require_visibility_assumptions=False,
    ),
    SELECTION_SCHEMA_VERSION_V3: _ArtifactFormat(
        selection_schema_version=SELECTION_SCHEMA_VERSION_V3,
        selection_policy_version=SELECTION_POLICY_VERSION_V3,
        replay_schema_version=REPLAY_SCHEMA_VERSION_V3,
        expected_policy=_EXPECTED_POLICY_V3,
        require_zero_future_skew=True,
        require_visibility_assumptions=True,
    ),
}


class ShadowSignalArtifactError(ValueError):
    """The selected model cannot be activated safely."""


@dataclass(frozen=True)
class ActivatedShadowSelection:
    selection_schema_version: int
    primary_model: CatchupModel
    models: tuple[CatchupModel, ...]
    poll_ms: int
    futures_stale_ms: int
    chainlink_stale_ms: int
    reference_max_gap_ms: int
    history_retention_ms: int
    max_future_skew_ms: int
    policy_version: str
    selection_fingerprint_sha256: str
    selection_artifact_sha256: str
    evidence_end_ms: int


@dataclass(frozen=True)
class _ProvenanceReport:
    role: str
    start_ms: int
    end_ms: int
    sha256: str


@dataclass(frozen=True)
class _RuntimeConfiguration:
    poll_ms: int
    futures_stale_ms: int
    chainlink_stale_ms: int
    reference_max_gap_ms: int
    history_retention_ms: int
    max_future_skew_ms: int
    lags_ms: tuple[int, ...]
    beta: Decimal


def _fail(message: str) -> ShadowSignalArtifactError:
    return ShadowSignalArtifactError(message)


def _validate_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise _fail(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _as_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise _fail(f"{field_name} must be an object")
    return value


def _as_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise _fail(f"{field_name} must be an array")
    return value


def _as_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise _fail(f"{field_name} must be a non-empty string")
    return value


def _as_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise _fail(f"{field_name} must be a boolean")
    return value


def _as_int(
    value: Any,
    field_name: str,
    *,
    minimum: Optional[int] = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise _fail(f"{field_name} must be at least {minimum}")
    return value


def _as_decimal(value: Any, field_name: str) -> Decimal:
    if not isinstance(value, str):
        raise _fail(f"{field_name} must be a JSON decimal string")
    if (
        len(value) > MAX_DECIMAL_CHARACTERS
        or _DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise _fail(f"{field_name} must be a bounded fixed-point decimal")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise _fail(f"{field_name} is not a decimal") from exc
    if not parsed.is_finite():
        raise _fail(f"{field_name} must be finite")
    return parsed


def _require_value(value: Any, expected: Any, field_name: str) -> None:
    if isinstance(expected, Mapping):
        actual = _as_mapping(value, field_name)
        if set(actual) != set(expected):
            raise _fail(f"{field_name} has unsupported fields")
        for key, expected_item in expected.items():
            _require_value(actual[key], expected_item, f"{field_name}.{key}")
        return
    if isinstance(expected, list):
        actual = _as_list(value, field_name)
        if len(actual) != len(expected):
            raise _fail(f"{field_name} has an unexpected length")
        for index, (actual_item, expected_item) in enumerate(
            zip(actual, expected)
        ):
            _require_value(
                actual_item,
                expected_item,
                f"{field_name}[{index}]",
            )
        return
    if isinstance(expected, bool):
        if not isinstance(value, bool) or value is not expected:
            raise _fail(f"{field_name} has an unsupported value")
        return
    if isinstance(expected, int):
        if isinstance(value, bool) or not isinstance(value, int) or value != expected:
            raise _fail(f"{field_name} has an unsupported value")
        return
    if not isinstance(value, type(expected)) or value != expected:
        raise _fail(f"{field_name} has an unsupported value")


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: Sequence[str],
    field_name: str,
) -> None:
    if set(value) != set(expected):
        raise _fail(f"{field_name} has unsupported or missing fields")


def _reject_float(raw_value: str) -> None:
    raise _fail(f"JSON floating-point value is forbidden: {raw_value}")


def _reject_constant(raw_value: str) -> None:
    raise _fail(f"non-finite JSON value is forbidden: {raw_value}")


def _parse_json_int(raw_value: str) -> int:
    if len(raw_value.lstrip("-")) > 20:
        raise _fail("JSON integer exceeds 20 digits")
    return int(raw_value)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise _fail(f"duplicate JSON key: {key}")
        payload[key] = value
    return payload


def _decode_json(raw: bytes, field_name: str) -> Mapping[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _fail(f"{field_name} is not UTF-8") from exc
    try:
        payload = json.loads(
            text,
            parse_float=_reject_float,
            parse_int=_parse_json_int,
            parse_constant=_reject_constant,
            object_pairs_hook=_unique_object,
        )
    except json.JSONDecodeError as exc:
        raise _fail(f"{field_name} is not valid JSON") from exc
    return _as_mapping(payload, field_name)


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _validate_posix_trusted_directory(
    metadata: Any,
    field_name: str,
) -> None:
    if metadata.st_uid != 0:
        raise _fail(f"{field_name} must be root-owned")
    if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise _fail(f"{field_name} must not be group- or world-writable")


def _validate_posix_decision_file(metadata: Any, field_name: str) -> None:
    if metadata.st_uid != 0:
        raise _fail(f"{field_name} must be root-owned")
    if metadata.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise _fail(f"{field_name} must have no owner, group, or world write bit")


def _validate_trusted_directory(
    trusted_directory: Path,
    *,
    enforce_posix_permissions: bool,
) -> Path:
    if not isinstance(trusted_directory, Path):
        raise TypeError("trusted_directory must be pathlib.Path or None")
    if not trusted_directory.is_absolute():
        raise _fail("trusted_directory must be absolute")
    try:
        metadata = os.lstat(trusted_directory)
    except OSError as exc:
        raise _fail("trusted_directory is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise _fail("trusted_directory must be a non-symlink directory")
    if os.name == "posix" and enforce_posix_permissions:
        _validate_posix_trusted_directory(metadata, "trusted_directory")
    return Path(os.path.abspath(trusted_directory))


def _read_bounded_regular_file(
    path: Path,
    *,
    field_name: str,
    trusted_directory: Optional[Path],
    enforce_posix_permissions: bool,
) -> bytes:
    if not isinstance(path, Path):
        raise TypeError(f"{field_name} must be pathlib.Path")
    if not path.is_absolute():
        raise _fail(f"{field_name} must be absolute")
    absolute_path = Path(os.path.abspath(path))
    if trusted_directory is not None and absolute_path.parent != trusted_directory:
        raise _fail(f"{field_name} must be a direct child of trusted_directory")

    try:
        before = os.lstat(absolute_path)
    except OSError as exc:
        raise _fail(f"{field_name} is unavailable") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise _fail(f"{field_name} must be a regular non-symlink file")
    if before.st_size > MAX_ARTIFACT_BYTES:
        raise _fail(f"{field_name} exceeds the bounded file-size limit")
    if os.name == "posix" and enforce_posix_permissions:
        _validate_posix_decision_file(before, field_name)

    flags = os.O_RDONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(absolute_path, flags)
    except OSError as exc:
        raise _fail(f"{field_name} could not be opened safely") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise _fail(f"{field_name} changed while opening")
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise _fail(f"{field_name} changed while opening")
        if os.name == "posix" and enforce_posix_permissions:
            _validate_posix_decision_file(opened, field_name)
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(MAX_ARTIFACT_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > MAX_ARTIFACT_BYTES:
        raise _fail(f"{field_name} exceeds the bounded file-size limit")
    return raw


def _decimal_ratio(numerator: int, denominator: int) -> Optional[Decimal]:
    if denominator == 0:
        return None
    with localcontext() as context:
        context.prec = 50
        return Decimal(numerator) / Decimal(denominator)


def _validate_coverage(value: Any, field_name: str) -> None:
    coverage = _as_mapping(value, field_name)
    _require_exact_keys(
        coverage,
        (
            "target_eligible",
            "valid_generated",
            "scored",
            "valid_coverage",
            "maturation_coverage",
            "gates",
            "passed",
        ),
        field_name,
    )
    target_eligible = _as_int(
        coverage["target_eligible"],
        f"{field_name}.target_eligible",
        minimum=0,
    )
    valid_generated = _as_int(
        coverage["valid_generated"],
        f"{field_name}.valid_generated",
        minimum=0,
    )
    scored = _as_int(
        coverage["scored"],
        f"{field_name}.scored",
        minimum=0,
    )
    if not target_eligible >= valid_generated >= scored:
        raise _fail(f"{field_name} counts are inconsistent")
    valid_coverage = _decimal_ratio(valid_generated, target_eligible)
    maturation_coverage = _decimal_ratio(scored, valid_generated)
    if valid_coverage is None or maturation_coverage is None:
        raise _fail(f"{field_name} cannot prove selected evidence coverage")
    if _as_decimal(
        coverage["valid_coverage"], f"{field_name}.valid_coverage"
    ) != valid_coverage:
        raise _fail(f"{field_name}.valid_coverage is inconsistent")
    if _as_decimal(
        coverage["maturation_coverage"],
        f"{field_name}.maturation_coverage",
    ) != maturation_coverage:
        raise _fail(f"{field_name}.maturation_coverage is inconsistent")

    expected_gates = {
        "minimum_common_scored": {
            "observed": scored,
            "threshold_inclusive": 10_000,
            "passed": scored >= 10_000,
        },
        "minimum_common_valid_coverage": {
            "observed": format(valid_coverage, "f"),
            "threshold_inclusive": "0.50",
            "passed": valid_coverage >= Decimal("0.50"),
        },
        "minimum_common_maturation_coverage": {
            "observed": format(maturation_coverage, "f"),
            "threshold_inclusive": "0.99",
            "passed": maturation_coverage >= Decimal("0.99"),
        },
    }
    _require_value(coverage["gates"], expected_gates, f"{field_name}.gates")
    if not all(gate["passed"] for gate in expected_gates.values()):
        raise _fail(f"{field_name} failed a selection evidence gate")
    if _as_bool(coverage["passed"], f"{field_name}.passed") is not True:
        raise _fail(f"{field_name}.passed must be true")


def _validate_provenance(
    value: Any,
) -> tuple[str, str, tuple[_ProvenanceReport, ...]]:
    provenance = _as_mapping(value, "selection.provenance")
    _require_exact_keys(
        provenance,
        ("configuration_sha256", "selection_fingerprint_sha256", "reports"),
        "selection.provenance",
    )
    configuration_sha256 = _validate_sha256(
        provenance["configuration_sha256"],
        "selection.provenance.configuration_sha256",
    )
    fingerprint_sha256 = _validate_sha256(
        provenance["selection_fingerprint_sha256"],
        "selection.provenance.selection_fingerprint_sha256",
    )
    report_payloads = _as_list(
        provenance["reports"], "selection.provenance.reports"
    )
    if len(report_payloads) < 2:
        raise _fail("selection.provenance.reports requires calibration and holdout")

    reports: list[_ProvenanceReport] = []
    previous_end_ms: Optional[int] = None
    holdout_count = 0
    seen_holdout = False
    seen_digests: set[str] = set()
    for index, raw_report in enumerate(report_payloads):
        field_name = f"selection.provenance.reports[{index}]"
        report = _as_mapping(raw_report, field_name)
        _require_exact_keys(
            report,
            (
                "role",
                "start_ms",
                "end_ms",
                "boundary",
                "gap_from_previous_ms",
                "sha256",
                "coverage",
            ),
            field_name,
        )
        role = _as_string(report["role"], f"{field_name}.role")
        if role not in {"calibration", "holdout"}:
            raise _fail(f"{field_name}.role is unsupported")
        if role == "holdout":
            seen_holdout = True
            holdout_count += 1
        elif seen_holdout:
            raise _fail("calibration evidence cannot follow holdout evidence")
        start_ms = _as_int(report["start_ms"], f"{field_name}.start_ms", minimum=0)
        end_ms = _as_int(report["end_ms"], f"{field_name}.end_ms", minimum=1)
        if end_ms <= start_ms:
            raise _fail(f"{field_name} must have a positive range")
        if report["boundary"] != "[start_ms,end_ms)":
            raise _fail(f"{field_name}.boundary is unsupported")
        expected_gap = None if previous_end_ms is None else start_ms - previous_end_ms
        if report["gap_from_previous_ms"] != expected_gap:
            raise _fail(f"{field_name}.gap_from_previous_ms is inconsistent")
        if expected_gap is not None and expected_gap < 0:
            raise _fail("selection provenance reports overlap")
        digest = _validate_sha256(report["sha256"], f"{field_name}.sha256")
        if digest in seen_digests:
            raise _fail("selection provenance repeats a report digest")
        seen_digests.add(digest)
        _validate_coverage(report["coverage"], f"{field_name}.coverage")
        reports.append(_ProvenanceReport(role, start_ms, end_ms, digest))
        previous_end_ms = end_ms

    if reports[0].role != "calibration" or holdout_count != 1:
        raise _fail("selection provenance requires calibration then one holdout")
    if reports[-1].role != "holdout":
        raise _fail("the untouched holdout must be the final evidence report")
    return configuration_sha256, fingerprint_sha256, tuple(reports)


def _validate_efficacy_gates(value: Any, field_name: str) -> None:
    gates = _as_mapping(value, field_name)
    _require_exact_keys(
        gates,
        ("mae_skill_positive", "rmse_skill_positive"),
        field_name,
    )
    for gate_name in ("mae_skill_positive", "rmse_skill_positive"):
        gate_field = f"{field_name}.{gate_name}"
        gate = _as_mapping(gates[gate_name], gate_field)
        _require_exact_keys(
            gate,
            ("observed", "threshold_exclusive", "passed"),
            gate_field,
        )
        observed = _as_decimal(gate["observed"], f"{gate_field}.observed")
        threshold = _as_decimal(
            gate["threshold_exclusive"],
            f"{gate_field}.threshold_exclusive",
        )
        if threshold != 0 or observed <= threshold:
            raise _fail(f"{gate_field} does not prove positive skill")
        if _as_bool(gate["passed"], f"{gate_field}.passed") is not True:
            raise _fail(f"{gate_field}.passed must be true")


def _validate_candidate_evidence(
    value: Any,
    field_name: str,
    *,
    require_passed_gates: bool,
) -> None:
    evidence = _as_mapping(value, field_name)
    _require_exact_keys(
        evidence,
        ("metrics", "gates", "paired_frequency_diagnostic", "slices"),
        field_name,
    )
    _as_mapping(evidence["metrics"], f"{field_name}.metrics")
    _as_mapping(evidence["slices"], f"{field_name}.slices")
    paired = _as_mapping(
        evidence["paired_frequency_diagnostic"],
        f"{field_name}.paired_frequency_diagnostic",
    )
    for key in ("hard_gate", "affects_eligibility", "affects_ranking"):
        if _as_bool(
            paired.get(key),
            f"{field_name}.paired_frequency_diagnostic.{key}",
        ) is not False:
            raise _fail(
                f"{field_name}.paired_frequency_diagnostic.{key} must be false"
            )
    if require_passed_gates:
        _validate_efficacy_gates(evidence["gates"], f"{field_name}.gates")
    else:
        _as_mapping(evidence["gates"], f"{field_name}.gates")


def _validate_selection_candidates(
    value: Any,
    primary_payload: Mapping[str, Any],
    *,
    artifact_format: _ArtifactFormat,
) -> tuple[tuple[CatchupModel, ...], CatchupModel]:
    candidates = _as_list(value, "selection.candidates")
    if len(candidates) != len(_EXPECTED_MODEL_SPECS):
        raise _fail(
            "selection.candidates is not the "
            f"{artifact_format.selection_policy_version} candidate set"
        )
    expected_by_version = {
        version: (lag_ms, beta)
        for version, lag_ms, beta in _EXPECTED_MODEL_SPECS
    }
    models_by_version: dict[str, CatchupModel] = {}
    ranks: set[int] = set()
    primary_version = _as_string(
        primary_payload.get("model_version"),
        "selection.decision.provisional_primary_model.model_version",
    )
    for index, raw_candidate in enumerate(candidates):
        field_name = f"selection.candidates[{index}]"
        candidate = _as_mapping(raw_candidate, field_name)
        _require_exact_keys(
            candidate,
            (
                "calibration_rank",
                "model_version",
                "horizon_ms",
                "beta",
                "calibration_eligible",
                "calibration",
                "holdout",
                "slice_warnings",
            ),
            field_name,
        )
        rank = _as_int(
            candidate["calibration_rank"],
            f"{field_name}.calibration_rank",
            minimum=1,
        )
        if rank in ranks:
            raise _fail("selection candidate ranks must be unique")
        ranks.add(rank)
        version = _as_string(candidate["model_version"], f"{field_name}.model_version")
        if version in models_by_version or version not in expected_by_version:
            raise _fail("selection candidate model versions are unsupported")
        horizon_ms = _as_int(
            candidate["horizon_ms"],
            f"{field_name}.horizon_ms",
            minimum=1,
        )
        beta = _as_decimal(candidate["beta"], f"{field_name}.beta")
        if (horizon_ms, beta) != expected_by_version[version]:
            raise _fail(f"{field_name} is inconsistent with the policy candidate")
        calibration_eligible = _as_bool(
            candidate["calibration_eligible"],
            f"{field_name}.calibration_eligible",
        )
        is_primary = version == primary_version
        if is_primary and (rank != 1 or not calibration_eligible):
            raise _fail("the frozen primary must be calibration rank one and eligible")
        _validate_candidate_evidence(
            candidate["calibration"],
            f"{field_name}.calibration",
            require_passed_gates=is_primary,
        )
        _validate_candidate_evidence(
            candidate["holdout"],
            f"{field_name}.holdout",
            require_passed_gates=is_primary,
        )
        _as_list(candidate["slice_warnings"], f"{field_name}.slice_warnings")
        models_by_version[version] = CatchupModel(version, horizon_ms, beta)

    if ranks != set(range(1, len(candidates) + 1)):
        raise _fail("selection candidate ranks must be consecutive")
    if set(models_by_version) != set(expected_by_version):
        raise _fail("selection candidate set is incomplete")
    if primary_version not in models_by_version:
        raise _fail("the selected primary is absent from selection candidates")
    primary = models_by_version[primary_version]
    if (
        _as_int(
            primary_payload.get("horizon_ms"),
            "selection.decision.provisional_primary_model.horizon_ms",
            minimum=1,
        )
        != primary.lag_ms
        or _as_decimal(
            primary_payload.get("beta"),
            "selection.decision.provisional_primary_model.beta",
        )
        != primary.beta
    ):
        raise _fail("the selected primary is inconsistent with its candidate")
    models = tuple(
        models_by_version[version]
        for version, _lag_ms, _beta in _EXPECTED_MODEL_SPECS
    )
    return models, primary


def _validate_selection(
    payload: Mapping[str, Any],
) -> tuple[
    _ArtifactFormat,
    tuple[CatchupModel, ...],
    CatchupModel,
    int,
    str,
    str,
    tuple[_ProvenanceReport, ...],
]:
    _require_exact_keys(
        payload,
        (
            "schema_version",
            "mode",
            "status",
            "selection_performed",
            "provisional",
            "dynamic_switching",
            "prediction_target",
            "policy",
            "provenance",
            "decision",
            "candidates",
            "limitations",
        ),
        "selection",
    )
    selection_schema_version = _as_int(
        payload["schema_version"],
        "selection.schema_version",
    )
    artifact_format = _ARTIFACT_FORMATS.get(selection_schema_version)
    if artifact_format is None:
        raise _fail("selection.schema_version is unsupported")
    if payload["mode"] != SELECTION_MODE or payload["status"] != "selected":
        raise _fail("selection artifact is not an accepted primary selection")
    if _as_bool(
        payload["selection_performed"],
        "selection.selection_performed",
    ) is not True:
        raise _fail("selection.selection_performed must be true")
    if _as_bool(payload["provisional"], "selection.provisional") is not True:
        raise _fail("selection.provisional must be true")
    if _as_bool(
        payload["dynamic_switching"],
        "selection.dynamic_switching",
    ) is not False:
        raise _fail("selection.dynamic_switching must be false")
    if payload["prediction_target"] != (
        "latest_chainlink_value_known_at_generated_ms_plus_horizon_ms"
    ):
        raise _fail("selection.prediction_target is unsupported")
    _require_value(
        payload["policy"],
        artifact_format.expected_policy,
        "selection.policy",
    )

    configuration_sha256, fingerprint_sha256, reports = _validate_provenance(
        payload["provenance"]
    )
    fingerprint_payload = {
        "policy": payload["policy"],
        "configuration_sha256": configuration_sha256,
        "reports": [
            {
                "role": report.role,
                "start_ms": report.start_ms,
                "end_ms": report.end_ms,
                "sha256": report.sha256,
            }
            for report in reports
        ],
    }
    calculated_fingerprint = _sha256(_canonical_json_bytes(fingerprint_payload))
    if not hmac.compare_digest(calculated_fingerprint, fingerprint_sha256):
        raise _fail("selection provenance fingerprint is inconsistent")

    decision = _as_mapping(payload["decision"], "selection.decision")
    _require_exact_keys(
        decision,
        (
            "reason",
            "frozen_calibration_winner",
            "provisional_primary_model",
            "holdout_reranking_performed",
            "fallback_after_holdout_failure_performed",
        ),
        "selection.decision",
    )
    _as_string(decision["reason"], "selection.decision.reason")
    if _as_bool(
        decision["holdout_reranking_performed"],
        "selection.decision.holdout_reranking_performed",
    ) is not False:
        raise _fail("selection decision performed holdout reranking")
    if _as_bool(
        decision["fallback_after_holdout_failure_performed"],
        "selection.decision.fallback_after_holdout_failure_performed",
    ) is not False:
        raise _fail("selection decision performed a holdout fallback")
    primary_payload = _as_mapping(
        decision["provisional_primary_model"],
        "selection.decision.provisional_primary_model",
    )
    _require_exact_keys(
        primary_payload,
        ("model_version", "horizon_ms", "beta", "evidence_end_ms"),
        "selection.decision.provisional_primary_model",
    )
    frozen_winner = _as_string(
        decision["frozen_calibration_winner"],
        "selection.decision.frozen_calibration_winner",
    )
    if frozen_winner != primary_payload["model_version"]:
        raise _fail("the frozen calibration winner differs from the primary")
    evidence_end_ms = _as_int(
        primary_payload["evidence_end_ms"],
        "selection.decision.provisional_primary_model.evidence_end_ms",
        minimum=1,
    )
    if evidence_end_ms != reports[-1].end_ms:
        raise _fail("primary evidence_end_ms differs from the holdout end")
    models, primary = _validate_selection_candidates(
        payload["candidates"],
        primary_payload,
        artifact_format=artifact_format,
    )
    limitations = _as_list(payload["limitations"], "selection.limitations")
    if not limitations or not all(
        isinstance(item, str) and item for item in limitations
    ):
        raise _fail("selection.limitations must contain non-empty strings")
    return (
        artifact_format,
        models,
        primary,
        evidence_end_ms,
        configuration_sha256,
        fingerprint_sha256,
        reports,
    )


def _validate_replay_configuration(
    payload: Mapping[str, Any],
    *,
    artifact_format: _ArtifactFormat,
    replay_sha256: str,
    configuration_sha256: str,
    provenance_reports: Sequence[_ProvenanceReport],
    selection_models: Sequence[CatchupModel],
) -> _RuntimeConfiguration:
    if (
        _as_int(payload.get("schema_version"), "replay.schema_version")
        != artifact_format.replay_schema_version
    ):
        raise _fail("replay.schema_version is unsupported")
    if payload.get("mode") != REPLAY_MODE or payload.get("status") != "ok":
        raise _fail("replay configuration report is not successful raw replay")
    if _as_bool(payload.get("selection_performed"), "replay.selection_performed"):
        raise _fail("replay configuration report already performed selection")
    matching_records = [
        record for record in provenance_reports if record.sha256 == replay_sha256
    ]
    if len(matching_records) != 1:
        raise _fail("replay configuration report is absent from selection provenance")
    matching_record = matching_records[0]
    replay_range = _as_mapping(payload.get("range"), "replay.range")
    if (
        _as_int(replay_range.get("start_ms"), "replay.range.start_ms", minimum=0)
        != matching_record.start_ms
        or _as_int(replay_range.get("end_ms"), "replay.range.end_ms", minimum=1)
        != matching_record.end_ms
        or replay_range.get("boundary") != "[start_ms,end_ms)"
    ):
        raise _fail("replay range differs from its selection provenance record")

    configuration = _as_mapping(payload.get("configuration"), "replay.configuration")
    calculated_configuration_sha256 = _sha256(
        _canonical_json_bytes(configuration)
    )
    if not hmac.compare_digest(
        calculated_configuration_sha256,
        configuration_sha256,
    ):
        raise _fail("replay configuration digest differs from selection provenance")

    poll_ms = _as_int(
        configuration.get("poll_ms"), "replay.configuration.poll_ms", minimum=1
    )
    if poll_ms != 100:
        raise _fail(
            "replay.configuration.poll_ms must equal "
            f"{artifact_format.selection_policy_version} 100 ms"
        )
    evaluation_interval_ms = _as_int(
        configuration.get("evaluation_interval_ms"),
        "replay.configuration.evaluation_interval_ms",
        minimum=1,
    )
    if evaluation_interval_ms != 500:
        raise _fail(
            "replay evaluation cadence must equal "
            f"{artifact_format.selection_policy_version} 500 ms"
        )
    lag_values = _as_list(configuration.get("lags_ms"), "replay.configuration.lags_ms")
    lags_ms = tuple(
        _as_int(value, f"replay.configuration.lags_ms[{index}]", minimum=1)
        for index, value in enumerate(lag_values)
    )
    expected_lags = tuple(lag for _version, lag, _beta in _EXPECTED_MODEL_SPECS)
    if lags_ms != expected_lags:
        raise _fail(
            "replay.configuration.lags_ms is not the "
            f"{artifact_format.selection_policy_version} candidate set"
        )
    beta = _as_decimal(configuration.get("beta"), "replay.configuration.beta")
    if beta != Decimal("1"):
        raise _fail(
            "replay.configuration.beta must equal "
            f"{artifact_format.selection_policy_version} beta 1"
        )
    futures_stale_ms = _as_int(
        configuration.get("futures_stale_ms"),
        "replay.configuration.futures_stale_ms",
        minimum=1,
    )
    chainlink_stale_ms = _as_int(
        configuration.get("chainlink_stale_ms"),
        "replay.configuration.chainlink_stale_ms",
        minimum=1,
    )
    reference_max_gap_ms = _as_int(
        configuration.get("reference_max_gap_ms"),
        "replay.configuration.reference_max_gap_ms",
        minimum=0,
    )
    history_retention_ms = _as_int(
        configuration.get("history_retention_ms"),
        "replay.configuration.history_retention_ms",
        minimum=1,
    )
    max_future_skew_ms = _as_int(
        configuration.get("max_future_skew_ms"),
        "replay.configuration.max_future_skew_ms",
        minimum=0,
    )
    if artifact_format.require_zero_future_skew and max_future_skew_ms != 0:
        raise _fail(
            "replay.configuration.max_future_skew_ms must equal "
            f"{artifact_format.selection_policy_version} zero"
        )
    if artifact_format.require_visibility_assumptions:
        _as_int(
            configuration.get("futures_availability_delay_ms"),
            "replay.configuration.futures_availability_delay_ms",
            minimum=0,
        )
        _as_int(
            configuration.get("chainlink_availability_delay_ms"),
            "replay.configuration.chainlink_availability_delay_ms",
            minimum=0,
        )
        evaluation_phase_offset_ms = _as_int(
            configuration.get("evaluation_phase_offset_ms"),
            "replay.configuration.evaluation_phase_offset_ms",
            minimum=0,
        )
        if evaluation_phase_offset_ms >= evaluation_interval_ms:
            raise _fail(
                "replay.configuration.evaluation_phase_offset_ms must be less "
                "than evaluation_interval_ms"
            )
        if evaluation_phase_offset_ms % poll_ms != 0:
            raise _fail(
                "replay.configuration.evaluation_phase_offset_ms must be a "
                "multiple of poll_ms"
            )
    minimum_retention_ms = (
        max(lags_ms) + chainlink_stale_ms + reference_max_gap_ms
    )
    if history_retention_ms < minimum_retention_ms:
        raise _fail("replay history retention cannot support its model configuration")

    replay_candidates = _as_list(payload.get("candidates"), "replay.candidates")
    replay_specs: set[tuple[str, int, Decimal]] = set()
    for index, raw_candidate in enumerate(replay_candidates):
        field_name = f"replay.candidates[{index}]"
        candidate = _as_mapping(raw_candidate, field_name)
        replay_specs.add(
            (
                _as_string(
                    candidate.get("model_version"),
                    f"{field_name}.model_version",
                ),
                _as_int(
                    candidate.get("horizon_ms"),
                    f"{field_name}.horizon_ms",
                    minimum=1,
                ),
                _as_decimal(candidate.get("beta"), f"{field_name}.beta"),
            )
        )
    selection_specs = {
        (model.version, model.lag_ms, model.beta) for model in selection_models
    }
    if len(replay_specs) != len(replay_candidates) or replay_specs != selection_specs:
        raise _fail("replay candidates differ from the selected candidate set")

    return _RuntimeConfiguration(
        poll_ms=poll_ms,
        futures_stale_ms=futures_stale_ms,
        chainlink_stale_ms=chainlink_stale_ms,
        reference_max_gap_ms=reference_max_gap_ms,
        history_retention_ms=history_retention_ms,
        max_future_skew_ms=max_future_skew_ms,
        lags_ms=lags_ms,
        beta=beta,
    )


def load_activated_selection(
    selection_path: Path,
    expected_selection_sha256: str,
    replay_configuration_report_path: Path,
    trusted_directory: Optional[Path] = None,
    enforce_posix_permissions: bool = True,
) -> ActivatedShadowSelection:
    """Load and cryptographically bind a fixed primary to its replay settings.

    The returned object is an in-memory activation snapshot. Callers must load it
    once at process startup; this function deliberately provides no fallback or
    hot-reload behavior.
    """

    expected_digest = _validate_sha256(
        expected_selection_sha256,
        "expected_selection_sha256",
    )
    if not isinstance(enforce_posix_permissions, bool):
        raise TypeError("enforce_posix_permissions must be bool")
    if (
        os.name == "posix"
        and enforce_posix_permissions
        and trusted_directory is None
    ):
        raise _fail(
            "trusted_directory is required when POSIX permission enforcement "
            "is enabled"
        )
    validated_directory = (
        _validate_trusted_directory(
            trusted_directory,
            enforce_posix_permissions=enforce_posix_permissions,
        )
        if trusted_directory is not None
        else None
    )
    selection_raw = _read_bounded_regular_file(
        selection_path,
        field_name="selection_path",
        trusted_directory=validated_directory,
        enforce_posix_permissions=enforce_posix_permissions,
    )
    selection_digest = _sha256(selection_raw)
    if not hmac.compare_digest(selection_digest, expected_digest):
        raise _fail("selection artifact SHA-256 does not match the trusted digest")
    selection = _decode_json(selection_raw, "selection")
    (
        artifact_format,
        selection_models,
        primary_model,
        evidence_end_ms,
        configuration_sha256,
        fingerprint_sha256,
        provenance_reports,
    ) = _validate_selection(selection)

    replay_raw = _read_bounded_regular_file(
        replay_configuration_report_path,
        field_name="replay_configuration_report_path",
        trusted_directory=validated_directory,
        enforce_posix_permissions=enforce_posix_permissions,
    )
    replay_digest = _sha256(replay_raw)
    replay = _decode_json(replay_raw, "replay")
    runtime = _validate_replay_configuration(
        replay,
        artifact_format=artifact_format,
        replay_sha256=replay_digest,
        configuration_sha256=configuration_sha256,
        provenance_reports=provenance_reports,
        selection_models=selection_models,
    )
    models_by_lag = {model.lag_ms: model for model in selection_models}
    ordered_models = tuple(models_by_lag[lag_ms] for lag_ms in runtime.lags_ms)

    return ActivatedShadowSelection(
        selection_schema_version=artifact_format.selection_schema_version,
        primary_model=primary_model,
        models=ordered_models,
        poll_ms=runtime.poll_ms,
        futures_stale_ms=runtime.futures_stale_ms,
        chainlink_stale_ms=runtime.chainlink_stale_ms,
        reference_max_gap_ms=runtime.reference_max_gap_ms,
        history_retention_ms=runtime.history_retention_ms,
        max_future_skew_ms=runtime.max_future_skew_ms,
        policy_version=artifact_format.selection_policy_version,
        selection_fingerprint_sha256=fingerprint_sha256,
        selection_artifact_sha256=selection_digest,
        evidence_end_ms=evidence_end_ms,
    )
