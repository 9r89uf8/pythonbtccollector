"""Frozen contracts for the offline Chainlink shorter-lag experiment.

This module deliberately contains no producer, Redis, database, or model-
selection side effects.  It defines the identities that later replay and
evidence checkpoints must bind before any efficacy-bearing holdout is run.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence, Union
from uuid import RFC_4122, UUID


POLICY_VERSION = "chronological_holdout_v4_shorter_challenger_24h"
EXPERIMENT_SCHEMA_VERSION = 1
PREREGISTRATION_ARTIFACT_TYPE = "chainlink_v4_holdout_preregistration"
TERMINAL_RESULT_ARTIFACT_TYPE = "chainlink_v4_terminal_result"

DAY_MS = 86_400_000
POLL_MS = 100
GENERATION_INTERVAL_MS = 500
FINALIZATION_ALLOWANCE_MS = 200
REFERENCE_MAX_GAP_MS = 250
MAX_FUTURE_SKEW_MS = 0
CALIBRATION_ATTEMPT_FREEZE_LEAD_MS = 3_600_000
MINIMUM_PREREGISTRATION_LEAD_MS = DAY_MS
PREREGISTRATION_PUBLICATION_ALLOWANCE_MS = 3_600_000
MAX_CANDIDATE_DAYS_PER_SELECTION = 7
MAX_QUALITY_ONLY_SUCCESSORS_PER_STAGE = 1

COMPARISON_LAGS_MS = (1_500, 2_000, 2_500, 3_000, 3_500)
PROMOTION_ELIGIBLE_LAGS_MS = (1_500, 2_000, 2_500)
INCUMBENT_LAG_MS = 3_000
GUARDRAIL_LAG_MS = 3_500

ANCHOR_RULE = "new_visible_chainlink_event_reanchors"
FUTURES_REFERENCE_RULE = (
    "newest_already_visible_futures_with_received_ms_lte_"
    "chainlink_received_ms_minus_lag_ms"
)
SAME_POLL_REFERENCE_RULE = (
    "newly_visible_futures_may_anchor_only_when_prior_history_existed"
)
PROJECTION_RULE = "chainlink_anchor_times_one_plus_beta_times_futures_return"
FORECAST_VALIDITY_RULE = (
    "all_inputs_visible_fresh_causal_and_reference_gap_lte_limit"
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_MODEL_VERSION_PATTERN = re.compile(r"catchup_ratio_l([0-9]+)_b100\Z")
_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_GIT_OBJECT_ID_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_MAX_JSON_BYTES = 64 * 1024 * 1024


class ExperimentValidationError(ValueError):
    """An experiment contract or artifact failed closed validation."""


class IncumbentProvenanceError(ExperimentValidationError):
    """The active incumbent cannot be used as an offline replacement control."""


def _require_int(
    value: object,
    field_name: str,
    *,
    minimum: Optional[int] = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExperimentValidationError(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise ExperimentValidationError(
            f"{field_name} must be at least {minimum}"
        )
    return value


def _require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ExperimentValidationError(f"{field_name} must be a boolean")
    return value


def _require_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ExperimentValidationError(
            f"{field_name} must be a non-empty string"
        )
    return value


def _require_sha256(value: object, field_name: str) -> str:
    text = _require_string(value, field_name)
    if _SHA256_PATTERN.fullmatch(text) is None:
        raise ExperimentValidationError(
            f"{field_name} must be a lowercase SHA-256"
        )
    return text


def _require_git_object_id(value: object, field_name: str) -> str:
    text = _require_string(value, field_name)
    if _GIT_OBJECT_ID_PATTERN.fullmatch(text) is None:
        raise ExperimentValidationError(
            f"{field_name} must be a lowercase Git object ID"
        )
    return text


def _require_uuid4(value: object, field_name: str) -> UUID:
    if isinstance(value, UUID):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = UUID(value)
        except ValueError as exc:
            raise ExperimentValidationError(
                f"{field_name} must be a canonical UUID4"
            ) from exc
        if str(parsed) != value:
            raise ExperimentValidationError(
                f"{field_name} must be a canonical UUID4"
            )
    else:
        raise ExperimentValidationError(f"{field_name} must be a UUID4")
    if parsed.version != 4 or parsed.variant != RFC_4122:
        raise ExperimentValidationError(f"{field_name} must be a UUID4")
    return parsed


def _require_decimal(value: object, field_name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise ExperimentValidationError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise ExperimentValidationError(f"{field_name} must be finite")
    return value


def _parse_decimal_string(value: object, field_name: str) -> Decimal:
    if not isinstance(value, str) or _DECIMAL_PATTERN.fullmatch(value) is None:
        raise ExperimentValidationError(
            f"{field_name} must be a fixed-point decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ExperimentValidationError(
            f"{field_name} must be a fixed-point decimal string"
        ) from exc
    if not parsed.is_finite():
        raise ExperimentValidationError(f"{field_name} must be finite")
    if value != _decimal_string(parsed):
        raise ExperimentValidationError(
            f"{field_name} must use canonical fixed-point Decimal spelling"
        )
    return parsed


def _decimal_string(value: Decimal) -> str:
    _require_decimal(value, "decimal")
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _require_exact_keys(
    payload: Mapping[str, Any],
    expected: Sequence[str],
    field_name: str,
) -> None:
    if set(payload) != set(expected):
        raise ExperimentValidationError(f"{field_name} has unsupported fields")


def _as_mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ExperimentValidationError(f"{field_name} must be an object")
    return value


def _as_list(value: object, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ExperimentValidationError(f"{field_name} must be an array")
    return value


def _as_sequence(value: object, field_name: str) -> Sequence[Any]:
    if not isinstance(value, (list, tuple)):
        raise ExperimentValidationError(f"{field_name} must be an array")
    return value


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_string(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        normalized = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ExperimentValidationError(
                    "canonical JSON object keys must be strings"
                )
            normalized[key] = _json_ready(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, float):
        raise ExperimentValidationError(
            "floating-point values are prohibited in experiment artifacts"
        )
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise ExperimentValidationError(
        f"unsupported canonical JSON value: {type(value).__name__}"
    )


def _freeze_json(value: Any) -> Any:
    """Recursively detach and freeze JSON-shaped contract material."""

    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ExperimentValidationError(
                "frozen JSON object keys must be strings"
            )
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if isinstance(value, float):
        raise ExperimentValidationError(
            "floating-point values are prohibited in experiment artifacts"
        )
    if value is None or isinstance(value, (str, int, bool, Decimal)):
        return value
    raise ExperimentValidationError(
        f"unsupported frozen JSON value: {type(value).__name__}"
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Return compact, deterministic UTF-8 JSON bytes without a trailing LF."""

    return json.dumps(
        _json_ready(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_artifact_bytes(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def artifact_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_artifact_bytes(value)).hexdigest()


def _reject_float(_raw: str) -> object:
    raise ExperimentValidationError("JSON floating-point numbers are prohibited")


def _reject_constant(_raw: str) -> object:
    raise ExperimentValidationError("non-finite JSON constants are prohibited")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ExperimentValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def decode_strict_json(raw: Union[bytes, str]) -> Mapping[str, Any]:
    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    elif isinstance(raw, bytes):
        raw_bytes = raw
    else:
        raise TypeError("raw must be bytes or str")
    if len(raw_bytes) > _MAX_JSON_BYTES:
        raise ExperimentValidationError("experiment artifact is too large")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExperimentValidationError("experiment artifact is not UTF-8") from exc
    try:
        value = json.loads(
            text,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
            object_pairs_hook=_unique_object,
        )
    except json.JSONDecodeError as exc:
        raise ExperimentValidationError(
            "experiment artifact is not valid JSON"
        ) from exc
    return _as_mapping(value, "artifact")


@dataclass(frozen=True)
class TimingCell:
    cell_id: str
    futures_delay_ms: int
    chainlink_delay_ms: int
    phase_offset_ms: int
    role: str

    def __post_init__(self) -> None:
        _require_string(self.cell_id, "cell_id")
        _require_int(self.futures_delay_ms, "futures_delay_ms", minimum=0)
        _require_int(self.chainlink_delay_ms, "chainlink_delay_ms", minimum=0)
        _require_int(self.phase_offset_ms, "phase_offset_ms", minimum=0)
        if self.phase_offset_ms >= GENERATION_INTERVAL_MS:
            raise ExperimentValidationError(
                "phase_offset_ms must be less than the generation interval"
            )
        if self.phase_offset_ms % POLL_MS:
            raise ExperimentValidationError(
                "phase_offset_ms must be aligned to the poll interval"
            )
        if self.role not in ("canonical", "robustness"):
            raise ExperimentValidationError("timing-cell role is unsupported")

    def to_dict(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "futures_delay_ms": self.futures_delay_ms,
            "chainlink_delay_ms": self.chainlink_delay_ms,
            "phase_offset_ms": self.phase_offset_ms,
            "role": self.role,
        }


V4_TIMING_CELLS = (
    TimingCell("canonical_p0", 100, 100, 0, "canonical"),
    TimingCell("canonical_p100", 100, 100, 100, "robustness"),
    TimingCell("canonical_p200", 100, 100, 200, "robustness"),
    TimingCell("canonical_p300", 100, 100, 300, "robustness"),
    TimingCell("canonical_p400", 100, 100, 400, "robustness"),
    TimingCell("futures_slower_p0", 200, 100, 0, "robustness"),
    TimingCell("chainlink_slower_p0", 100, 200, 0, "robustness"),
)


@dataclass(frozen=True)
class ForecastConfig:
    lag_ms: int
    horizon_ms: int
    beta: Decimal
    futures_stale_ms: int
    chainlink_stale_ms: int
    reference_max_gap_ms: int
    history_retention_ms: int
    max_future_skew_ms: int
    anchor_rule: str = ANCHOR_RULE
    futures_reference_rule: str = FUTURES_REFERENCE_RULE
    same_poll_reference_rule: str = SAME_POLL_REFERENCE_RULE
    projection_rule: str = PROJECTION_RULE
    forecast_validity_rule: str = FORECAST_VALIDITY_RULE

    def __post_init__(self) -> None:
        for field_name in (
            "lag_ms",
            "horizon_ms",
            "futures_stale_ms",
            "chainlink_stale_ms",
            "history_retention_ms",
        ):
            _require_int(getattr(self, field_name), field_name, minimum=1)
        _require_int(
            self.reference_max_gap_ms,
            "reference_max_gap_ms",
            minimum=0,
        )
        _require_int(self.max_future_skew_ms, "max_future_skew_ms", minimum=0)
        beta = _require_decimal(self.beta, "beta")
        if beta < 0:
            raise ExperimentValidationError("beta must be non-negative")
        for field_name in (
            "anchor_rule",
            "futures_reference_rule",
            "same_poll_reference_rule",
            "projection_rule",
            "forecast_validity_rule",
        ):
            _require_string(getattr(self, field_name), field_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "lag_ms": self.lag_ms,
            "horizon_ms": self.horizon_ms,
            "beta": self.beta,
            "futures_stale_ms": self.futures_stale_ms,
            "chainlink_stale_ms": self.chainlink_stale_ms,
            "reference_max_gap_ms": self.reference_max_gap_ms,
            "history_retention_ms": self.history_retention_ms,
            "max_future_skew_ms": self.max_future_skew_ms,
            "anchor_rule": self.anchor_rule,
            "futures_reference_rule": self.futures_reference_rule,
            "same_poll_reference_rule": self.same_poll_reference_rule,
            "projection_rule": self.projection_rule,
            "forecast_validity_rule": self.forecast_validity_rule,
        }

    @classmethod
    def from_dict(cls, value: object, field_name: str) -> ForecastConfig:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(payload, cls.__dataclass_fields__, field_name)
        return cls(
            lag_ms=_require_int(payload["lag_ms"], f"{field_name}.lag_ms", minimum=1),
            horizon_ms=_require_int(
                payload["horizon_ms"], f"{field_name}.horizon_ms", minimum=1
            ),
            beta=_parse_decimal_string(payload["beta"], f"{field_name}.beta"),
            futures_stale_ms=_require_int(
                payload["futures_stale_ms"],
                f"{field_name}.futures_stale_ms",
                minimum=1,
            ),
            chainlink_stale_ms=_require_int(
                payload["chainlink_stale_ms"],
                f"{field_name}.chainlink_stale_ms",
                minimum=1,
            ),
            reference_max_gap_ms=_require_int(
                payload["reference_max_gap_ms"],
                f"{field_name}.reference_max_gap_ms",
                minimum=0,
            ),
            history_retention_ms=_require_int(
                payload["history_retention_ms"],
                f"{field_name}.history_retention_ms",
                minimum=1,
            ),
            max_future_skew_ms=_require_int(
                payload["max_future_skew_ms"],
                f"{field_name}.max_future_skew_ms",
                minimum=0,
            ),
            anchor_rule=_require_string(
                payload["anchor_rule"], f"{field_name}.anchor_rule"
            ),
            futures_reference_rule=_require_string(
                payload["futures_reference_rule"],
                f"{field_name}.futures_reference_rule",
            ),
            same_poll_reference_rule=_require_string(
                payload["same_poll_reference_rule"],
                f"{field_name}.same_poll_reference_rule",
            ),
            projection_rule=_require_string(
                payload["projection_rule"], f"{field_name}.projection_rule"
            ),
            forecast_validity_rule=_require_string(
                payload["forecast_validity_rule"],
                f"{field_name}.forecast_validity_rule",
            ),
        )


def forecast_config_payload(config: ForecastConfig) -> dict[str, Any]:
    if not isinstance(config, ForecastConfig):
        raise TypeError("config must be ForecastConfig")
    return config.to_dict()


def forecast_config_digest(config: ForecastConfig) -> str:
    return canonical_sha256(forecast_config_payload(config))


def non_lag_forecast_config_digest(config: ForecastConfig) -> str:
    payload = forecast_config_payload(config)
    del payload["lag_ms"]
    del payload["horizon_ms"]
    return canonical_sha256(payload)


@dataclass(frozen=True)
class V4ForecastSettings:
    """Explicit non-lag settings frozen by a particular v4 lineage.

    The plan intentionally does not prescribe the three staleness/retention
    values.  Callers must state them explicitly; they are then cryptographically
    bound rather than silently inherited from a deployed or legacy default.
    """

    futures_stale_ms: int
    chainlink_stale_ms: int
    history_retention_ms: int
    beta: Decimal = Decimal("1")
    reference_max_gap_ms: int = REFERENCE_MAX_GAP_MS
    max_future_skew_ms: int = MAX_FUTURE_SKEW_MS
    anchor_rule: str = ANCHOR_RULE
    futures_reference_rule: str = FUTURES_REFERENCE_RULE
    same_poll_reference_rule: str = SAME_POLL_REFERENCE_RULE
    projection_rule: str = PROJECTION_RULE
    forecast_validity_rule: str = FORECAST_VALIDITY_RULE

    def __post_init__(self) -> None:
        for field_name in (
            "futures_stale_ms",
            "chainlink_stale_ms",
            "history_retention_ms",
        ):
            _require_int(getattr(self, field_name), field_name, minimum=1)
        _require_decimal(self.beta, "beta")
        _require_int(
            self.reference_max_gap_ms,
            "reference_max_gap_ms",
            minimum=0,
        )
        _require_int(
            self.max_future_skew_ms,
            "max_future_skew_ms",
            minimum=0,
        )
        if self.beta != Decimal("1"):
            raise ExperimentValidationError("v4 beta must equal 1")
        if self.reference_max_gap_ms != REFERENCE_MAX_GAP_MS:
            raise ExperimentValidationError(
                "v4 reference_max_gap_ms must equal 250"
            )
        if self.max_future_skew_ms != MAX_FUTURE_SKEW_MS:
            raise ExperimentValidationError("v4 max_future_skew_ms must equal 0")
        expected_rules = {
            "anchor_rule": ANCHOR_RULE,
            "futures_reference_rule": FUTURES_REFERENCE_RULE,
            "same_poll_reference_rule": SAME_POLL_REFERENCE_RULE,
            "projection_rule": PROJECTION_RULE,
            "forecast_validity_rule": FORECAST_VALIDITY_RULE,
        }
        for field_name, expected in expected_rules.items():
            if getattr(self, field_name) != expected:
                raise ExperimentValidationError(
                    f"v4 {field_name} differs from the frozen rule"
                )
        minimum_retention = (
            max(COMPARISON_LAGS_MS)
            + self.chainlink_stale_ms
            + self.reference_max_gap_ms
        )
        if self.history_retention_ms < minimum_retention:
            raise ExperimentValidationError(
                "v4 history retention cannot support the comparison family"
            )

    def config_for_lag(self, lag_ms: int) -> ForecastConfig:
        if lag_ms not in COMPARISON_LAGS_MS:
            raise ExperimentValidationError("lag is not in the v4 family")
        return ForecastConfig(
            lag_ms=lag_ms,
            horizon_ms=lag_ms,
            beta=self.beta,
            futures_stale_ms=self.futures_stale_ms,
            chainlink_stale_ms=self.chainlink_stale_ms,
            reference_max_gap_ms=self.reference_max_gap_ms,
            history_retention_ms=self.history_retention_ms,
            max_future_skew_ms=self.max_future_skew_ms,
            anchor_rule=self.anchor_rule,
            futures_reference_rule=self.futures_reference_rule,
            same_poll_reference_rule=self.same_poll_reference_rule,
            projection_rule=self.projection_rule,
            forecast_validity_rule=self.forecast_validity_rule,
        )

    @property
    def candidate_configs(self) -> tuple[ForecastConfig, ...]:
        return tuple(self.config_for_lag(lag) for lag in COMPARISON_LAGS_MS)


@dataclass(frozen=True)
class ForecastCodeManifest:
    anchor_formation_sha256: str
    futures_reference_selection_sha256: str
    projection_sha256: str
    forecast_validity_sha256: str
    component_digest_scheme: str = "sha256_raw_component_bytes_v1"

    def __post_init__(self) -> None:
        for field_name in (
            "anchor_formation_sha256",
            "futures_reference_selection_sha256",
            "projection_sha256",
            "forecast_validity_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        if self.component_digest_scheme != "sha256_raw_component_bytes_v1":
            raise ExperimentValidationError(
                "forecast-code digest scheme is unsupported"
            )

    def component_payload(self) -> dict[str, str]:
        """Return only the four forecast-rule identities covered by the digest."""

        return {
            "anchor_formation_sha256": self.anchor_formation_sha256,
            "futures_reference_selection_sha256": (
                self.futures_reference_selection_sha256
            ),
            "projection_sha256": self.projection_sha256,
            "forecast_validity_sha256": self.forecast_validity_sha256,
        }

    def to_dict(self) -> dict[str, str]:
        return {
            **self.component_payload(),
            "component_digest_scheme": self.component_digest_scheme,
        }

    def to_artifact_dict(self, artifact_type: str) -> dict[str, Any]:
        """Return the canonical self-describing manifest artifact envelope."""

        if artifact_type not in (
            "active_forecast_code_manifest",
            "v4_forecast_code_manifest",
        ):
            raise ExperimentValidationError(
                "forecast-code manifest artifact type is unsupported"
            )
        return {
            "artifact_type": artifact_type,
            "schema_version": EXPERIMENT_SCHEMA_VERSION,
            "component_digest_scheme": self.component_digest_scheme,
            "forecast_code_digest_scheme": (
                "sha256_canonical_component_identity_json_v1"
            ),
            "forecast_code_digest": self.digest,
            "components": self.component_payload(),
        }

    @property
    def digest(self) -> str:
        return canonical_sha256(self.component_payload())

    @classmethod
    def from_component_bytes(
        cls,
        *,
        anchor_formation: bytes,
        futures_reference_selection: bytes,
        projection: bytes,
        forecast_validity: bytes,
    ) -> ForecastCodeManifest:
        components = {
            "anchor_formation_sha256": anchor_formation,
            "futures_reference_selection_sha256": (
                futures_reference_selection
            ),
            "projection_sha256": projection,
            "forecast_validity_sha256": forecast_validity,
        }
        digests = {}
        for field_name, content in components.items():
            if not isinstance(content, bytes):
                raise TypeError(f"{field_name} content must be bytes")
            if not content:
                raise ExperimentValidationError(
                    f"{field_name} content must not be empty"
                )
            digests[field_name] = hashlib.sha256(content).hexdigest()
        return cls(**digests)

    @classmethod
    def from_dict(cls, value: object, field_name: str) -> ForecastCodeManifest:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(payload, cls.__dataclass_fields__, field_name)
        return cls(
            anchor_formation_sha256=_require_sha256(
                payload["anchor_formation_sha256"],
                f"{field_name}.anchor_formation_sha256",
            ),
            futures_reference_selection_sha256=_require_sha256(
                payload["futures_reference_selection_sha256"],
                f"{field_name}.futures_reference_selection_sha256",
            ),
            projection_sha256=_require_sha256(
                payload["projection_sha256"],
                f"{field_name}.projection_sha256",
            ),
            forecast_validity_sha256=_require_sha256(
                payload["forecast_validity_sha256"],
                f"{field_name}.forecast_validity_sha256",
            ),
            component_digest_scheme=_require_string(
                payload["component_digest_scheme"],
                f"{field_name}.component_digest_scheme",
            ),
        )

    @classmethod
    def from_artifact_dict(
        cls,
        value: object,
        field_name: str,
        *,
        expected_artifact_type: str,
    ) -> ForecastCodeManifest:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(
            payload,
            (
                "artifact_type",
                "schema_version",
                "component_digest_scheme",
                "forecast_code_digest_scheme",
                "forecast_code_digest",
                "components",
            ),
            field_name,
        )
        if payload["artifact_type"] != expected_artifact_type:
            raise ExperimentValidationError(
                f"{field_name} has the wrong artifact type"
            )
        if _require_int(
            payload["schema_version"], f"{field_name}.schema_version"
        ) != EXPERIMENT_SCHEMA_VERSION:
            raise ExperimentValidationError(
                f"{field_name} has an unsupported schema"
            )
        if payload["forecast_code_digest_scheme"] != (
            "sha256_canonical_component_identity_json_v1"
        ):
            raise ExperimentValidationError(
                f"{field_name} has an unsupported forecast-code digest scheme"
            )
        components = _as_mapping(payload["components"], f"{field_name}.components")
        expected_component_keys = (
            "anchor_formation_sha256",
            "futures_reference_selection_sha256",
            "projection_sha256",
            "forecast_validity_sha256",
        )
        _require_exact_keys(
            components,
            expected_component_keys,
            f"{field_name}.components",
        )
        manifest = cls(
            anchor_formation_sha256=_require_sha256(
                components["anchor_formation_sha256"],
                f"{field_name}.components.anchor_formation_sha256",
            ),
            futures_reference_selection_sha256=_require_sha256(
                components["futures_reference_selection_sha256"],
                f"{field_name}.components.futures_reference_selection_sha256",
            ),
            projection_sha256=_require_sha256(
                components["projection_sha256"],
                f"{field_name}.components.projection_sha256",
            ),
            forecast_validity_sha256=_require_sha256(
                components["forecast_validity_sha256"],
                f"{field_name}.components.forecast_validity_sha256",
            ),
            component_digest_scheme=_require_string(
                payload["component_digest_scheme"],
                f"{field_name}.component_digest_scheme",
            ),
        )
        if payload["forecast_code_digest"] != manifest.digest:
            raise ExperimentValidationError(
                f"{field_name} forecast-code digest is inconsistent"
            )
        return manifest


@dataclass(frozen=True)
class ArtifactBinding:
    artifact_type: str
    schema_version: int
    sha256: str

    def __post_init__(self) -> None:
        _require_string(self.artifact_type, "artifact_type")
        _require_int(self.schema_version, "schema_version", minimum=1)
        _require_sha256(self.sha256, "sha256")

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "schema_version": self.schema_version,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object, field_name: str) -> ArtifactBinding:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(
            payload,
            ("artifact_type", "schema_version", "sha256"),
            field_name,
        )
        return cls(
            artifact_type=_require_string(
                payload["artifact_type"], f"{field_name}.artifact_type"
            ),
            schema_version=_require_int(
                payload["schema_version"],
                f"{field_name}.schema_version",
                minimum=1,
            ),
            sha256=_require_sha256(payload["sha256"], f"{field_name}.sha256"),
        )


@dataclass(frozen=True)
class InspectedEvidenceBinding:
    """Authoritative scope metadata for one previously inspected artifact."""

    artifact: ArtifactBinding
    source_lineage_id: str
    source_experiment_id: str
    window_start_ms: int
    window_end_ms: int
    inspection_role: str
    evidence_scope: str

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, ArtifactBinding):
            raise TypeError("artifact must be ArtifactBinding")
        _require_string(self.source_lineage_id, "source_lineage_id")
        _require_string(self.source_experiment_id, "source_experiment_id")
        _require_int(self.window_start_ms, "window_start_ms", minimum=0)
        _require_int(self.window_end_ms, "window_end_ms", minimum=1)
        if self.window_end_ms <= self.window_start_ms:
            raise ExperimentValidationError(
                "inspected-evidence window must be non-empty"
            )
        if self.inspection_role not in (
            "historical_selection",
            "historical_replay",
            "historical_holdout",
            "prior_attempt_quality_only",
        ):
            raise ExperimentValidationError(
                "inspected-evidence role is unsupported"
            )
        if self.evidence_scope not in (
            "calibration_only",
            "calibration_quality_only",
            "holdout_quality_only",
        ):
            raise ExperimentValidationError(
                "inspected-evidence scope is unsupported"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact": self.artifact.to_dict(),
            "source_lineage_id": self.source_lineage_id,
            "source_experiment_id": self.source_experiment_id,
            "window": {
                "start_ms": self.window_start_ms,
                "end_ms": self.window_end_ms,
                "boundary": "[start_ms,end_ms)",
            },
            "inspection_role": self.inspection_role,
            "evidence_scope": self.evidence_scope,
        }

    @classmethod
    def from_dict(
        cls, value: object, field_name: str
    ) -> InspectedEvidenceBinding:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(
            payload,
            (
                "artifact",
                "source_lineage_id",
                "source_experiment_id",
                "window",
                "inspection_role",
                "evidence_scope",
            ),
            field_name,
        )
        window = _as_mapping(payload["window"], f"{field_name}.window")
        _require_exact_keys(
            window,
            ("start_ms", "end_ms", "boundary"),
            f"{field_name}.window",
        )
        if window["boundary"] != "[start_ms,end_ms)":
            raise ExperimentValidationError(
                f"{field_name}.window boundary is invalid"
            )
        return cls(
            artifact=ArtifactBinding.from_dict(
                payload["artifact"], f"{field_name}.artifact"
            ),
            source_lineage_id=_require_string(
                payload["source_lineage_id"],
                f"{field_name}.source_lineage_id",
            ),
            source_experiment_id=_require_string(
                payload["source_experiment_id"],
                f"{field_name}.source_experiment_id",
            ),
            window_start_ms=_require_int(
                window["start_ms"], f"{field_name}.window.start_ms", minimum=0
            ),
            window_end_ms=_require_int(
                window["end_ms"], f"{field_name}.window.end_ms", minimum=1
            ),
            inspection_role=_require_string(
                payload["inspection_role"], f"{field_name}.inspection_role"
            ),
            evidence_scope=_require_string(
                payload["evidence_scope"], f"{field_name}.evidence_scope"
            ),
        )


def _require_binding_identity(
    binding: Optional[ArtifactBinding],
    expected_type: str,
    field_name: str,
) -> ArtifactBinding:
    if not isinstance(binding, ArtifactBinding):
        raise ExperimentValidationError(f"{field_name} is required")
    if (
        binding.artifact_type != expected_type
        or binding.schema_version != EXPERIMENT_SCHEMA_VERSION
    ):
        raise ExperimentValidationError(
            f"{field_name} has an unsupported artifact identity"
        )
    return binding


@dataclass(frozen=True)
class ActiveIncumbentFreeze:
    selection_sha256: str
    replay_config_sha256: str
    primary_model_version: str
    forecast_config: ForecastConfig
    forecast_code: ForecastCodeManifest
    loaded_runtime_identity_sha256: str
    installed_runtime_identity_sha256: str
    invocation_start: ArtifactBinding
    selection_artifact: ArtifactBinding
    replay_config_artifact: ArtifactBinding
    forecast_code_manifest_artifact: ArtifactBinding
    reconstruction_report: ArtifactBinding

    def __post_init__(self) -> None:
        _require_sha256(self.selection_sha256, "selection_sha256")
        _require_sha256(self.replay_config_sha256, "replay_config_sha256")
        _require_string(self.primary_model_version, "primary_model_version")
        if not isinstance(self.forecast_config, ForecastConfig):
            raise TypeError("forecast_config must be ForecastConfig")
        if not isinstance(self.forecast_code, ForecastCodeManifest):
            raise TypeError("forecast_code must be ForecastCodeManifest")
        _require_sha256(
            self.loaded_runtime_identity_sha256,
            "loaded_runtime_identity_sha256",
        )
        _require_sha256(
            self.installed_runtime_identity_sha256,
            "installed_runtime_identity_sha256",
        )
        if (
            self.loaded_runtime_identity_sha256
            != self.installed_runtime_identity_sha256
        ):
            raise IncumbentProvenanceError(
                "active loaded and installed runtime identities differ"
            )
        if not isinstance(self.invocation_start, ArtifactBinding):
            raise TypeError("invocation_start must be ArtifactBinding")
        required_bindings = (
            (
                self.invocation_start,
                "active_invocation_start_record",
                "invocation_start",
            ),
            (
                self.selection_artifact,
                "active_incumbent_selection",
                "selection_artifact",
            ),
            (
                self.replay_config_artifact,
                "active_incumbent_replay_configuration",
                "replay_config_artifact",
            ),
            (
                self.forecast_code_manifest_artifact,
                "active_forecast_code_manifest",
                "forecast_code_manifest_artifact",
            ),
            (
                self.reconstruction_report,
                "active_forecast_reconstruction_report",
                "reconstruction_report",
            ),
        )
        for artifact, expected_type, field_name in required_bindings:
            if not isinstance(artifact, ArtifactBinding):
                raise TypeError(f"{field_name} must be ArtifactBinding")
            if (
                artifact.artifact_type != expected_type
                or artifact.schema_version != EXPERIMENT_SCHEMA_VERSION
            ):
                raise IncumbentProvenanceError(
                    f"{field_name} has an unsupported artifact identity"
                )
        if self.selection_artifact.sha256 != self.selection_sha256:
            raise IncumbentProvenanceError(
                "selection artifact hash differs from the active freeze"
            )
        if self.replay_config_artifact.sha256 != self.replay_config_sha256:
            raise IncumbentProvenanceError(
                "replay configuration hash differs from the active freeze"
            )
        if self.forecast_code_manifest_artifact.sha256 != artifact_sha256(
            self.forecast_code.to_artifact_dict(
                "active_forecast_code_manifest"
            )
        ):
            raise IncumbentProvenanceError(
                "forecast-code manifest artifact does not bind the supplied code"
            )
        if (
            self.forecast_config.lag_ms != INCUMBENT_LAG_MS
            or self.forecast_config.horizon_ms != INCUMBENT_LAG_MS
            or self.forecast_config.beta != Decimal("1")
        ):
            raise IncumbentProvenanceError(
                "active primary must remain lag_ms=3000, horizon_ms=3000, beta=1"
            )
        match = _MODEL_VERSION_PATTERN.fullmatch(self.primary_model_version)
        if match is None or int(match.group(1)) != INCUMBENT_LAG_MS:
            raise IncumbentProvenanceError(
                "active primary model version is not the frozen 3000 ms rule"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "selection_sha256": self.selection_sha256,
            "replay_config_sha256": self.replay_config_sha256,
            "primary_model_version": self.primary_model_version,
            "forecast_config": self.forecast_config.to_dict(),
            "forecast_code": self.forecast_code.to_dict(),
            "loaded_runtime_identity_sha256": (
                self.loaded_runtime_identity_sha256
            ),
            "installed_runtime_identity_sha256": (
                self.installed_runtime_identity_sha256
            ),
            "invocation_start": self.invocation_start.to_dict(),
            "selection_artifact": self.selection_artifact.to_dict(),
            "replay_config_artifact": self.replay_config_artifact.to_dict(),
            "forecast_code_manifest_artifact": (
                self.forecast_code_manifest_artifact.to_dict()
            ),
            "reconstruction_report": self.reconstruction_report.to_dict(),
        }


class ControlMode(str, Enum):
    V4_3000_ALIAS = "v4_3000_candidate"
    DISTINCT_OPERATIONAL_CONTROL = "distinct_operational_control"


@dataclass(frozen=True)
class ReplacementControlResolution:
    mode: ControlMode
    active_full_config_digest: str
    active_non_lag_config_digest: str
    v4_3000_full_config_digest: str
    v4_non_lag_config_digest: str
    active_code_digest: str
    v4_code_digest: str
    decision_scope: str

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ControlMode):
            raise TypeError("mode must be ControlMode")
        for field_name in (
            "active_full_config_digest",
            "active_non_lag_config_digest",
            "v4_3000_full_config_digest",
            "v4_non_lag_config_digest",
            "active_code_digest",
            "v4_code_digest",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        expected_scope = (
            "lag_only"
            if self.mode is ControlMode.V4_3000_ALIAS
            else "complete_v4_challenger_configuration"
        )
        if self.decision_scope != expected_scope:
            raise ExperimentValidationError(
                "replacement-control decision scope is inconsistent"
            )

    @property
    def control_model_role(self) -> str:
        return "offline_replay_replacement_control"

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "control_model_role": self.control_model_role,
            "active_full_config_digest": self.active_full_config_digest,
            "active_non_lag_config_digest": self.active_non_lag_config_digest,
            "v4_3000_full_config_digest": self.v4_3000_full_config_digest,
            "v4_non_lag_config_digest": self.v4_non_lag_config_digest,
            "active_code_digest": self.active_code_digest,
            "v4_code_digest": self.v4_code_digest,
            "decision_scope": self.decision_scope,
        }


def resolve_replacement_control(
    *,
    active_incumbent: ActiveIncumbentFreeze,
    v4_3000_config: ForecastConfig,
    v4_forecast_code: ForecastCodeManifest,
) -> ReplacementControlResolution:
    if not isinstance(active_incumbent, ActiveIncumbentFreeze):
        raise TypeError("active_incumbent must be ActiveIncumbentFreeze")
    if not isinstance(v4_3000_config, ForecastConfig):
        raise TypeError("v4_3000_config must be ForecastConfig")
    if (
        v4_3000_config.lag_ms != INCUMBENT_LAG_MS
        or v4_3000_config.horizon_ms != INCUMBENT_LAG_MS
        or v4_3000_config.beta != Decimal("1")
    ):
        raise ExperimentValidationError(
            "v4 replacement comparator must be the 3000 ms beta=1 candidate"
        )
    active_full = forecast_config_digest(active_incumbent.forecast_config)
    active_non_lag = non_lag_forecast_config_digest(
        active_incumbent.forecast_config
    )
    v4_full = forecast_config_digest(v4_3000_config)
    v4_non_lag = non_lag_forecast_config_digest(v4_3000_config)
    active_code = active_incumbent.forecast_code.digest
    v4_code = v4_forecast_code.digest
    matches = active_full == v4_full and active_code == v4_code
    return ReplacementControlResolution(
        mode=(
            ControlMode.V4_3000_ALIAS
            if matches
            else ControlMode.DISTINCT_OPERATIONAL_CONTROL
        ),
        active_full_config_digest=active_full,
        active_non_lag_config_digest=active_non_lag,
        v4_3000_full_config_digest=v4_full,
        v4_non_lag_config_digest=v4_non_lag,
        active_code_digest=active_code,
        v4_code_digest=v4_code,
        decision_scope=(
            "lag_only"
            if matches
            else "complete_v4_challenger_configuration"
        ),
    )


def _model_version(lag_ms: int) -> str:
    return f"catchup_ratio_l{lag_ms}_b100"


def _evaluation_policy_payload(
    *,
    candidate_configs: Sequence[ForecastConfig],
    control: ReplacementControlResolution,
    active_control_config_digest: str,
) -> dict[str, Any]:
    return {
        "policy_version": POLICY_VERSION,
        "comparison_family": [
            {
                "model_role": "v4_candidate",
                "model_version": _model_version(config.lag_ms),
                "lag_ms": config.lag_ms,
                "horizon_ms": config.horizon_ms,
                "forecast_config_digest": forecast_config_digest(config),
            }
            for config in candidate_configs
        ],
        "promotion_eligible_lags_ms": list(PROMOTION_ELIGIBLE_LAGS_MS),
        "incumbent_comparison_lag_ms": INCUMBENT_LAG_MS,
        "guardrail_lag_ms": GUARDRAIL_LAG_MS,
        "offline_replacement_control": {
            "model_role": control.control_model_role,
            "mode": control.mode.value,
            "forecast_config_digest": active_control_config_digest,
            "distinct_identity_from_v4_3000_candidate": True,
            "control_role_participates_in_family_ranking": False,
            "v4_3000_family_member_participates_in_family_ranking": True,
            "promotion_eligible": False,
        },
        "baseline_contract": (
            "horizon_matched_no_change_at_each_forecast_chainlink_anchor"
        ),
        "baseline_pairing": (
            "candidate_and_control_each_pair_with_own_same_origin_baseline"
        ),
        "common_cohort_contract": (
            "target_eligible_all_five_valid_all_five_causal_actuals_"
            "no_integrity_reset_through_maximum_target_finalization"
        ),
        "decision_cohort_contract": (
            "common_scored_and_replacement_control_valid_with_causal_actual"
        ),
        "target_resolution_contract": (
            "newest_chainlink_visible_by_target_with_full_received_wall_ns_"
            "lte_target"
        ),
        "visibility_contract": {
            "available_wall_ns_formula": (
                "received_wall_ns+source_delay_ms*1000000"
            ),
            "visible_tick_index_formula": (
                "(available_wall_ns+poll_ns-1)//poll_ns"
            ),
            "visible_ms_formula": "visible_tick_index*poll_ms",
            "every_input_rule": "input_visible_ms<=generated_ms",
            "actual_rule": "actual_visible_ms<=target_ms",
            "maturation_rule": "first_poll_at_or_after_target",
            "v4_targets_must_be_poll_aligned": True,
        },
        "scheduling_contract": {
            "poll_ms": POLL_MS,
            "generation_interval_ms": GENERATION_INTERVAL_MS,
            "origin": "global_epoch_aligned",
            "target_eligible_tail_rule": (
                "generated_ms_plus_3500_strictly_before_scoring_end_ms"
            ),
        },
        "poll_and_tie_order_contract": {
            "poll_order": (
                "apply_available_events_then_observe_then_mature_then_generate"
            ),
            "raw_order": [
                "received_wall_ns",
                "source_kind_order_futures_before_chainlink",
                "received_monotonic_ns",
                "source_sequence",
                "connection_id",
            ],
            "visibility_order": ["available_wall_ns", "raw_order"],
            "exact_poll_and_target_ties_are_eligible": True,
        },
        "missing_origin_contract": (
            "retain_target_index_position_with_explicit_reason_no_shift_"
            "compaction_imputation_or_zero_fill"
        ),
        "finalization_and_continuity_contract": {
            "allowance_ms": FINALIZATION_ALLOWANCE_MS,
            "allowance_extends_horizon": False,
            "source_timestamp_regression_resets_forecast_state": True,
            "source_timestamp_regression_alone_invalidates_pending_cohort": False,
        },
        "timing_cells": [cell.to_dict() for cell in V4_TIMING_CELLS],
        "timing_cell_use_contract": {
            "pool_cells": False,
            "ranking_estimation_bootstrap_and_decision_cell": "canonical_p0",
            "robustness_cells_are_rejection_only": [
                cell.cell_id for cell in V4_TIMING_CELLS[1:]
            ],
            "robustness_cells_use_own_cohorts": True,
        },
        "calibration_ranking_contract": {
            "metric": "mae_skill_vs_horizon_matched_no_change_baseline",
            "direction": "highest_first",
            "cohort": "canonical_p0_control_paired_decision_eligible",
            "ordered_runner_up_and_lead_required": True,
            "rmse_can_rank_or_break_tie": False,
            "unclear_or_tied_winner_action": "retain_incumbent",
            "shorter_runner_up_fallback": False,
        },
        "raw_delivery_metadata_contract": {
            "futures_source_sequence": "last_agg_trade_id",
            "chainlink_source_sequence": "receive_sequence",
            "publisher_epoch": "not_captured",
            "accepted_event_sequence": "not_captured",
            "delay_semantics": "fixed_sensitivity_assumption_not_measurement",
            "observation_identity_fields": [
                "value",
                "received_wall_ns",
                "received_monotonic_ns",
                "visible_ms",
                "source_timestamp_ms",
                "connection_id",
                "source_sequence",
                "publisher_epoch",
                "accepted_event_sequence",
            ],
        },
    }


@dataclass(frozen=True)
class ModelIdentity:
    model_role: str
    model_version: str
    forecast_config_digest: str
    offline_evaluation_policy_digest: str

    def __post_init__(self) -> None:
        if self.model_role not in (
            "v4_candidate",
            "offline_replay_replacement_control",
        ):
            raise ExperimentValidationError("model_role is unsupported")
        _require_string(self.model_version, "model_version")
        _require_sha256(self.forecast_config_digest, "forecast_config_digest")
        _require_sha256(
            self.offline_evaluation_policy_digest,
            "offline_evaluation_policy_digest",
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "model_role": self.model_role,
            "model_version": self.model_version,
            "forecast_config_digest": self.forecast_config_digest,
            "offline_evaluation_policy_digest": (
                self.offline_evaluation_policy_digest
            ),
        }

    @classmethod
    def from_dict(cls, value: object, field_name: str) -> ModelIdentity:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(payload, cls.__dataclass_fields__, field_name)
        return cls(
            model_role=_require_string(
                payload["model_role"], f"{field_name}.model_role"
            ),
            model_version=_require_string(
                payload["model_version"], f"{field_name}.model_version"
            ),
            forecast_config_digest=_require_sha256(
                payload["forecast_config_digest"],
                f"{field_name}.forecast_config_digest",
            ),
            offline_evaluation_policy_digest=_require_sha256(
                payload["offline_evaluation_policy_digest"],
                f"{field_name}.offline_evaluation_policy_digest",
            ),
        )


@dataclass(frozen=True)
class V4ExperimentContract:
    forecast_settings: V4ForecastSettings
    v4_forecast_code: ForecastCodeManifest
    v4_forecast_code_manifest_artifact: ArtifactBinding
    active_incumbent: ActiveIncumbentFreeze
    replacement_control: ReplacementControlResolution = field(init=False)
    offline_evaluation_policy: Mapping[str, Any] = field(init=False)
    offline_evaluation_policy_digest: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.forecast_settings, V4ForecastSettings):
            raise TypeError("forecast_settings must be V4ForecastSettings")
        if not isinstance(self.v4_forecast_code, ForecastCodeManifest):
            raise TypeError("v4_forecast_code must be ForecastCodeManifest")
        _require_binding_identity(
            self.v4_forecast_code_manifest_artifact,
            "v4_forecast_code_manifest",
            "v4_forecast_code_manifest_artifact",
        )
        if self.v4_forecast_code_manifest_artifact.sha256 != artifact_sha256(
            self.v4_forecast_code.to_artifact_dict(
                "v4_forecast_code_manifest"
            )
        ):
            raise ExperimentValidationError(
                "v4 forecast-code manifest does not bind the supplied code"
            )
        if not isinstance(self.active_incumbent, ActiveIncumbentFreeze):
            raise TypeError("active_incumbent must be ActiveIncumbentFreeze")
        candidates = self.forecast_settings.candidate_configs
        if tuple(config.lag_ms for config in candidates) != COMPARISON_LAGS_MS:
            raise ExperimentValidationError("v4 comparison family is not exact")
        if any(
            config.horizon_ms != config.lag_ms
            or config.beta != Decimal("1")
            for config in candidates
        ):
            raise ExperimentValidationError("v4 candidates violate the model rule")
        control = resolve_replacement_control(
            active_incumbent=self.active_incumbent,
            v4_3000_config=self.forecast_settings.config_for_lag(
                INCUMBENT_LAG_MS
            ),
            v4_forecast_code=self.v4_forecast_code,
        )
        policy = _evaluation_policy_payload(
            candidate_configs=candidates,
            control=control,
            active_control_config_digest=forecast_config_digest(
                self.active_incumbent.forecast_config
            ),
        )
        object.__setattr__(self, "replacement_control", control)
        object.__setattr__(
            self,
            "offline_evaluation_policy",
            _freeze_json(policy),
        )
        object.__setattr__(
            self,
            "offline_evaluation_policy_digest",
            canonical_sha256(policy),
        )

    @property
    def candidate_configs(self) -> tuple[ForecastConfig, ...]:
        return self.forecast_settings.candidate_configs

    def candidate_identity(self, lag_ms: int) -> ModelIdentity:
        config = self.forecast_settings.config_for_lag(lag_ms)
        return ModelIdentity(
            model_role="v4_candidate",
            model_version=_model_version(lag_ms),
            forecast_config_digest=forecast_config_digest(config),
            offline_evaluation_policy_digest=(
                self.offline_evaluation_policy_digest
            ),
        )

    @property
    def control_identity(self) -> ModelIdentity:
        return ModelIdentity(
            model_role="offline_replay_replacement_control",
            model_version=self.active_incumbent.primary_model_version,
            forecast_config_digest=forecast_config_digest(
                self.active_incumbent.forecast_config
            ),
            offline_evaluation_policy_digest=(
                self.offline_evaluation_policy_digest
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        candidate_configs = self.candidate_configs
        v4_3000 = self.forecast_settings.config_for_lag(INCUMBENT_LAG_MS)
        return {
            "policy_version": POLICY_VERSION,
            "experiment_policy": _experiment_policy_payload(),
            "comparison_family": [
                {
                    "model_version": _model_version(config.lag_ms),
                    "forecast_config": config.to_dict(),
                    "forecast_config_digest": forecast_config_digest(config),
                }
                for config in candidate_configs
            ],
            "v4_forecast_code": self.v4_forecast_code.to_dict(),
            "v4_forecast_code_manifest_artifact": (
                self.v4_forecast_code_manifest_artifact.to_dict()
            ),
            "active_incumbent": self.active_incumbent.to_dict(),
            "replacement_control": self.replacement_control.to_dict(),
            "digests": {
                "active_incumbent_selection_sha256": (
                    self.active_incumbent.selection_sha256
                ),
                "active_incumbent_replay_config_sha256": (
                    self.active_incumbent.replay_config_sha256
                ),
                "active_incumbent_primary_model_version": (
                    self.active_incumbent.primary_model_version
                ),
                "active_incumbent_forecast_config_digest": (
                    forecast_config_digest(self.active_incumbent.forecast_config)
                ),
                "active_incumbent_non_lag_forecast_config_digest": (
                    non_lag_forecast_config_digest(
                        self.active_incumbent.forecast_config
                    )
                ),
                "v4_3000_forecast_config_digest": (
                    forecast_config_digest(v4_3000)
                ),
                "v4_non_lag_forecast_config_digest": (
                    non_lag_forecast_config_digest(v4_3000)
                ),
                "active_incumbent_forecast_code_digest": (
                    self.active_incumbent.forecast_code.digest
                ),
                "v4_forecast_code_digest": self.v4_forecast_code.digest,
                "offline_evaluation_policy_digest": (
                    self.offline_evaluation_policy_digest
                ),
            },
            "offline_evaluation_policy": _json_ready(
                self.offline_evaluation_policy
            ),
            "candidate_identities": [
                self.candidate_identity(lag).to_dict()
                for lag in COMPARISON_LAGS_MS
            ],
            "control_identity": self.control_identity.to_dict(),
        }

    @property
    def digest(self) -> str:
        return canonical_sha256(self.to_dict())

    def validate_payload(self, value: object, field_name: str = "contract") -> None:
        payload = _as_mapping(value, field_name)
        if canonical_json_bytes(payload) != canonical_json_bytes(self.to_dict()):
            raise ExperimentValidationError(
                f"{field_name} differs from the frozen v4 experiment contract"
            )


@dataclass(frozen=True)
class AttemptIdentity:
    calibration_lineage_id: UUID
    experiment_id: UUID
    calibration_attempt_index: int
    holdout_attempt_index: Optional[int]

    def __post_init__(self) -> None:
        _require_uuid4(self.calibration_lineage_id, "calibration_lineage_id")
        _require_uuid4(self.experiment_id, "experiment_id")
        if self.calibration_lineage_id == self.experiment_id:
            raise ExperimentValidationError(
                "lineage and experiment identifiers must be distinct"
            )
        if self.calibration_attempt_index not in (0, 1):
            raise ExperimentValidationError(
                "calibration_attempt_index must be zero or one"
            )
        if self.holdout_attempt_index not in (None, 0, 1):
            raise ExperimentValidationError(
                "holdout_attempt_index must be null, zero, or one"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "calibration_lineage_id": str(self.calibration_lineage_id),
            "experiment_id": str(self.experiment_id),
            "calibration_attempt_index": self.calibration_attempt_index,
            "holdout_attempt_index": self.holdout_attempt_index,
        }

    @classmethod
    def from_dict(cls, value: object, field_name: str) -> AttemptIdentity:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(payload, cls.__dataclass_fields__, field_name)
        return cls(
            calibration_lineage_id=_require_uuid4(
                payload["calibration_lineage_id"],
                f"{field_name}.calibration_lineage_id",
            ),
            experiment_id=_require_uuid4(
                payload["experiment_id"], f"{field_name}.experiment_id"
            ),
            calibration_attempt_index=_require_int(
                payload["calibration_attempt_index"],
                f"{field_name}.calibration_attempt_index",
                minimum=0,
            ),
            holdout_attempt_index=(
                None
                if payload["holdout_attempt_index"] is None
                else _require_int(
                    payload["holdout_attempt_index"],
                    f"{field_name}.holdout_attempt_index",
                    minimum=0,
                )
            ),
        )


def _validate_artifact_bindings(
    value: object,
    field_name: str,
) -> tuple[ArtifactBinding, ...]:
    items = _as_sequence(value, field_name)
    bindings = []
    for index, item in enumerate(items):
        if isinstance(item, ArtifactBinding):
            binding = item
        else:
            binding = ArtifactBinding.from_dict(
                item, f"{field_name}[{index}]"
            )
        bindings.append(binding)
    normalized = tuple(bindings)
    hashes = [binding.sha256 for binding in normalized]
    if len(hashes) != len(set(hashes)):
        raise ExperimentValidationError(f"{field_name} contains duplicates")
    return normalized


def _validate_inspected_evidence_bindings(
    value: object,
    field_name: str,
) -> tuple[InspectedEvidenceBinding, ...]:
    items = _as_sequence(value, field_name)
    inspected = []
    for index, item in enumerate(items):
        if isinstance(item, InspectedEvidenceBinding):
            binding = item
        else:
            binding = InspectedEvidenceBinding.from_dict(
                item, f"{field_name}[{index}]"
            )
        inspected.append(binding)
    normalized = tuple(inspected)
    hashes = [item.artifact.sha256 for item in normalized]
    if len(hashes) != len(set(hashes)):
        raise ExperimentValidationError(f"{field_name} contains duplicates")
    return normalized


def _require_nonempty_mapping(value: object, field_name: str) -> Mapping[str, Any]:
    payload = _as_mapping(value, field_name)
    if not payload:
        raise ExperimentValidationError(f"{field_name} must not be empty")
    canonical_json_bytes(payload)
    return payload


def _validate_hashes_by_cell(value: object, field_name: str) -> Mapping[str, str]:
    payload = _as_mapping(value, field_name)
    expected_ids = tuple(cell.cell_id for cell in V4_TIMING_CELLS)
    _require_exact_keys(payload, expected_ids, field_name)
    return {
        cell_id: _require_sha256(payload[cell_id], f"{field_name}.{cell_id}")
        for cell_id in expected_ids
    }


def _validate_origin_contract(value: object) -> Mapping[str, Any]:
    field_name = "preregistration.origin_contract"
    payload = _as_mapping(value, field_name)
    expected_keys = (
        "origin_formula",
        "scheduled_count_per_cell",
        "target_eligible_count_per_cell",
        "scheduled_vector_sha256_by_cell",
        "target_eligible_mask_sha256_by_cell",
        "target_eligible_vector_sha256_by_cell",
        "observed_mask_index",
        "observed_mask_schemas",
        "missing_origin_treatment",
        "coverage_thresholds",
    )
    _require_exact_keys(payload, expected_keys, field_name)
    if payload["origin_formula"] != (
        "generated_ms=cell.phase_offset_ms+500*k_restricted_to_half_open_window"
    ):
        raise ExperimentValidationError("origin formula differs from v4")
    if _require_int(
        payload["scheduled_count_per_cell"],
        f"{field_name}.scheduled_count_per_cell",
    ) != 172_800:
        raise ExperimentValidationError("scheduled origin count must be 172800")
    if _require_int(
        payload["target_eligible_count_per_cell"],
        f"{field_name}.target_eligible_count_per_cell",
    ) != 172_793:
        raise ExperimentValidationError(
            "target-eligible origin count must be 172793"
        )
    for key in (
        "scheduled_vector_sha256_by_cell",
        "target_eligible_mask_sha256_by_cell",
        "target_eligible_vector_sha256_by_cell",
    ):
        _validate_hashes_by_cell(payload[key], f"{field_name}.{key}")
    if payload["observed_mask_index"] != "target_eligible_origin_vector":
        raise ExperimentValidationError("observed masks use the wrong index")
    expected_schemas = (
        "generation_eligible_mask",
        "common_scored_mask",
        "decision_eligible_mask",
        "per_origin_missing_reasons",
    )
    observed_mask_schemas = _as_sequence(
        payload["observed_mask_schemas"],
        f"{field_name}.observed_mask_schemas",
    )
    if tuple(observed_mask_schemas) != expected_schemas:
        raise ExperimentValidationError("observed-mask schemas differ from v4")
    if payload["missing_origin_treatment"] != (
        "retain_position_no_shift_compaction_imputation_or_zero_fill"
    ):
        raise ExperimentValidationError("missing-origin treatment differs from v4")
    expected_coverage = {
        "canonical_common_scored_minimum": 164_154,
        "robustness_common_scored_minimum": 155_514,
        "canonical_decision_eligible_minimum": 164_154,
        "robustness_decision_eligible_minimum": 155_514,
    }
    if canonical_json_bytes(payload["coverage_thresholds"]) != (
        canonical_json_bytes(expected_coverage)
    ):
        raise ExperimentValidationError(
            "origin coverage thresholds differ from v4"
        )
    return payload


_ARCHIVE_HEALTH_CONTRACT_KEYS = (
    "archive_boundary_contract_sha256",
    "partition_pair_contract_sha256",
    "capture_maintenance_contract_sha256",
    "headroom_projection_contract_sha256",
    "seal_feasibility_contract_sha256",
    "checkpoint_schedule_sha256",
    "failure_rules_sha256",
)

_PROVENANCE_CONTRACT_KEYS = (
    "precalibration_provenance_freeze_sha256",
    "continuity_ledger_contract_sha256",
    "checkpoint_schedule_sha256",
    "experiment_environment_sha256",
    "producer_identity_set_sha256",
    "current_provenance_continuity_root",
)


def _validate_sha_contract(
    value: object,
    field_name: str,
    expected_keys: Sequence[str],
) -> Mapping[str, str]:
    payload = _as_mapping(value, field_name)
    _require_exact_keys(payload, expected_keys, field_name)
    return {
        key: _require_sha256(payload[key], f"{field_name}.{key}")
        for key in expected_keys
    }


_CALIBRATION_CORE_ARTIFACT_TYPES = (
    "calibration_candidate_day_ledger",
    "calibration_attempt_freeze",
    "calibration_archive_checkpoint_manifest",
    "calibration_raw_manifest",
    *tuple(
        f"calibration_quality_report:{cell.cell_id}"
        for cell in V4_TIMING_CELLS
    ),
    "calibration_pre_efficacy_provenance_gate",
    "calibration_efficacy_started",
    "calibration_efficacy_ledger",
    "calibration_efficacy_report",
    "calibration_efficacy_completed",
    "final_analysis_checkpoint",
    "holdout_selection_authorization",
)


def _validate_calibration_artifact_set(
    artifacts: Sequence[ArtifactBinding],
    *,
    calibration_attempt_index: int,
    start_marker_sha256: str,
    completion_marker_sha256: str,
) -> None:
    types = tuple(artifact.artifact_type for artifact in artifacts)
    if calibration_attempt_index == 1:
        expected_types = (
            "calibration_retry_eligibility",
            _CALIBRATION_CORE_ARTIFACT_TYPES[0],
            "calibration_successor_authorization",
            *_CALIBRATION_CORE_ARTIFACT_TYPES[1:],
        )
    else:
        expected_types = _CALIBRATION_CORE_ARTIFACT_TYPES
    if types != expected_types:
        raise ExperimentValidationError(
            "calibration_artifacts are not in the complete frozen order"
        )
    if any(artifact.schema_version != 1 for artifact in artifacts):
        raise ExperimentValidationError(
            "calibration artifact schema version is unsupported"
        )
    by_type = {artifact.artifact_type: artifact for artifact in artifacts}
    if by_type["calibration_efficacy_started"].sha256 != start_marker_sha256:
        raise ExperimentValidationError(
            "calibration start-marker hash is inconsistent"
        )
    if by_type["calibration_efficacy_completed"].sha256 != (
        completion_marker_sha256
    ):
        raise ExperimentValidationError(
            "calibration completion-marker hash is inconsistent"
        )


FROZEN_BOOTSTRAP_CONTRACT = _freeze_json({
    "seed": "sha256_of_canonical_preregistration_bytes_big_endian_integer",
    "rng": "python_random_Random",
    "grid": "canonical_target_eligible_172793_position_vector",
    "block_length": 1_800,
    "draws_per_replicate": 96,
    "block_sampling": "circular_wraparound",
    "truncate_last_block_to": 1_793,
    "truncate_before_observed_mask_skip": True,
    "replicates": 10_000,
    "lower_bound": "500th_one_indexed_sorted_statistic",
    "statistic": "challenger_mae_skill_minus_control_mae_skill",
    "paired_series": [
        "challenger_absolute_loss",
        "challenger_baseline_absolute_loss",
        "control_absolute_loss",
        "control_baseline_absolute_loss",
    ],
    "paired_series_sampled_synchronously": True,
    "undefined_or_zero_baseline_action": "fail_inference",
    "operation_order": (
        "truncate_draw_then_apply_paired_mask_then_sum_losses_then_compute_"
        "maes_then_challenger_skill_then_control_skill_then_difference"
    ),
    "implementation": "exact_circular_block_sufficient_statistics",
    "sufficient_statistics": {
        "zero_loss_exponent": 0,
        "shared_exponent": "minimum_nonzero_frozen_loss_exponent",
        "accumulator_precision": "max(50,Dmax+len(str(2*172793)))",
        "decimal_traps": ["Inexact", "Rounded"],
        "block_lengths": [1_800, 1_793],
        "full_blocks_per_replicate": 95,
        "partial_block_draw_index": 96,
        "exponent_bounds_must_validate": True,
    },
    "decimal_precision": 50,
    "decimal_rounding": "ROUND_HALF_EVEN",
})

FROZEN_HOLDOUT_GATES = _freeze_json({
    "challenger_canonical_mae_skill_minimum": Decimal("0.05"),
    "mae_skill_improvement_vs_control_minimum": Decimal("0.02"),
    "mae_skill_improvement_bootstrap_lower_bound_strictly_positive": True,
    "challenger_canonical_rmse_skill_strictly_positive": True,
    "rmse_skill_improvement_vs_control_minimum": Decimal("0"),
    "challenger_mae_skill_each_robustness_cell_strictly_positive": True,
    "robustness_mae_skill_improvement_vs_control_minimum": Decimal("-0.01"),
    "rerank_or_runner_up_fallback": False,
})

FROZEN_CALIBRATION_GATES = _freeze_json({
    "duration_ms": DAY_MS,
    "scheduled_count_per_cell": 172_800,
    "target_eligible_count_per_cell": 172_793,
    "canonical_common_scored_minimum": 164_154,
    "robustness_common_scored_minimum": 155_514,
    "canonical_decision_eligible_minimum": 164_154,
    "robustness_decision_eligible_minimum": 155_514,
    "winner_canonical_mae_skill_minimum": Decimal("0.05"),
    "winner_canonical_rmse_skill_strictly_positive": True,
    "mae_skill_lead_over_runner_up_minimum": Decimal("0.01"),
    "relative_robustness_maximum_deficit": Decimal("0.01"),
    "rmse_can_rank_or_break_tie": False,
    "promotion_eligible_lags_ms": PROMOTION_ELIGIBLE_LAGS_MS,
})

FROZEN_QUALITY_GATES = _freeze_json({
    "duration_ms": DAY_MS,
    "timing_cell_count": 7,
    "scheduled_count_per_cell": 172_800,
    "target_eligible_count_per_cell": 172_793,
    "canonical_common_scored_minimum": 164_154,
    "robustness_common_scored_minimum": 155_514,
    "canonical_decision_eligible_minimum": 164_154,
    "robustness_decision_eligible_minimum": 155_514,
    "cohort_classification_percent": Decimal("1"),
    "causal_violations": 0,
    "archive_health_and_provenance_required": True,
})


def _experiment_policy_payload() -> dict[str, Any]:
    return {
        "policy_version": POLICY_VERSION,
        "result_domain": [
            "insufficient_evidence",
            "retain_incumbent",
            "promotion_eligible",
        ],
        "calibration_window": "one_exact_midnight_aligned_utc_day",
        "holdout_window": "one_exact_future_midnight_aligned_utc_day",
        "calibration_attempt_freeze_lead_ms": (
            CALIBRATION_ATTEMPT_FREEZE_LEAD_MS
        ),
        "minimum_preregistration_lead_ms": MINIMUM_PREREGISTRATION_LEAD_MS,
        "preregistration_publication_allowance_ms": (
            PREREGISTRATION_PUBLICATION_ALLOWANCE_MS
        ),
        "max_candidate_days_per_selection": MAX_CANDIDATE_DAYS_PER_SELECTION,
        "max_quality_only_successors_per_stage": (
            MAX_QUALITY_ONLY_SUCCESSORS_PER_STAGE
        ),
        "calibration_gates": _json_ready(FROZEN_CALIBRATION_GATES),
        "calibration_quality_gates": _json_ready(FROZEN_QUALITY_GATES),
        "holdout_quality_gates": _json_ready(FROZEN_QUALITY_GATES),
        "holdout_performance_gates": _json_ready(FROZEN_HOLDOUT_GATES),
        "bootstrap_contract": _json_ready(FROZEN_BOOTSTRAP_CONTRACT),
        "promotion_is_activation": False,
        "rerank_fallback_or_dynamic_switching": False,
    }


@dataclass(frozen=True)
class SelectionAnchorProvenance:
    """Artifact-backed source of the immutable holdout-selection anchor."""

    mode: str
    source_artifact: ArtifactBinding
    timestamp_field: str
    timestamp_ms: int
    authorization_artifact: ArtifactBinding

    def __post_init__(self) -> None:
        if self.mode == "calibration_completion":
            expected_source_type = "calibration_efficacy_completed"
            expected_timestamp_field = "completed_at_ms"
            expected_authorization_type = "holdout_selection_authorization"
        elif self.mode == "retry_eligibility":
            expected_source_type = "holdout_retry_eligibility"
            expected_timestamp_field = "created_at_ms"
            expected_authorization_type = "holdout_successor_authorization"
        else:
            raise ExperimentValidationError(
                "selection-anchor provenance mode is unsupported"
            )
        for binding, expected_type, field_name in (
            (
                self.source_artifact,
                expected_source_type,
                "selection_anchor_provenance.source_artifact",
            ),
            (
                self.authorization_artifact,
                expected_authorization_type,
                "selection_anchor_provenance.authorization_artifact",
            ),
        ):
            if not isinstance(binding, ArtifactBinding):
                raise TypeError(f"{field_name} must be ArtifactBinding")
            if (
                binding.artifact_type != expected_type
                or binding.schema_version != EXPERIMENT_SCHEMA_VERSION
            ):
                raise ExperimentValidationError(
                    f"{field_name} has an unsupported artifact identity"
                )
        if self.timestamp_field != expected_timestamp_field:
            raise ExperimentValidationError(
                "selection-anchor timestamp field is inconsistent"
            )
        _require_int(self.timestamp_ms, "selection_anchor_provenance.timestamp_ms", minimum=0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "source_artifact": self.source_artifact.to_dict(),
            "timestamp_field": self.timestamp_field,
            "timestamp_ms": self.timestamp_ms,
            "authorization_artifact": self.authorization_artifact.to_dict(),
        }

    @classmethod
    def from_dict(
        cls, value: object, field_name: str
    ) -> SelectionAnchorProvenance:
        payload = _as_mapping(value, field_name)
        _require_exact_keys(payload, cls.__dataclass_fields__, field_name)
        return cls(
            mode=_require_string(payload["mode"], f"{field_name}.mode"),
            source_artifact=ArtifactBinding.from_dict(
                payload["source_artifact"], f"{field_name}.source_artifact"
            ),
            timestamp_field=_require_string(
                payload["timestamp_field"], f"{field_name}.timestamp_field"
            ),
            timestamp_ms=_require_int(
                payload["timestamp_ms"], f"{field_name}.timestamp_ms", minimum=0
            ),
            authorization_artifact=ArtifactBinding.from_dict(
                payload["authorization_artifact"],
                f"{field_name}.authorization_artifact",
            ),
        )


@dataclass(frozen=True)
class V4Preregistration:
    attempt: AttemptIdentity
    experiment_contract: V4ExperimentContract
    selection_anchor_provenance: SelectionAnchorProvenance
    frozen_challenger: ModelIdentity
    calibration_artifacts: tuple[ArtifactBinding, ...]
    calibration_efficacy_started_sha256: str
    calibration_efficacy_completed_sha256: str
    holdout_start_ms: int
    holdout_end_ms: int
    archive_input_start_ms: int
    archive_input_end_ms: int
    seal_timeout_ms: int
    seal_deadline_ms: int
    candidate_day_ledger_root: str
    provenance_continuity_root: str
    preregistration_publication_deadline_ms: int
    pushed_receipt_deadline_ms: int
    authoritative_remote_ref: str
    authoritative_remote_url_sha256: str
    origin_contract: Mapping[str, Any]
    archive_health_contract: Mapping[str, Any]
    provenance_contract: Mapping[str, Any]
    prior_evidence: Mapping[str, Any]
    created_at_ms: int

    @property
    def selection_anchor_ms(self) -> int:
        return self.selection_anchor_provenance.timestamp_ms

    def __post_init__(self) -> None:
        try:
            calibration_artifacts = tuple(self.calibration_artifacts)
        except TypeError as exc:
            raise ExperimentValidationError(
                "calibration_artifacts must be an array"
            ) from exc
        if not all(
            isinstance(item, ArtifactBinding) for item in calibration_artifacts
        ):
            raise ExperimentValidationError(
                "calibration_artifacts must contain ArtifactBinding values"
            )
        object.__setattr__(
            self,
            "calibration_artifacts",
            calibration_artifacts,
        )
        if not isinstance(self.attempt, AttemptIdentity):
            raise TypeError("attempt must be AttemptIdentity")
        if self.attempt.holdout_attempt_index not in (0, 1):
            raise ExperimentValidationError(
                "a holdout preregistration requires a holdout attempt index"
            )
        if not isinstance(self.experiment_contract, V4ExperimentContract):
            raise TypeError("experiment_contract must be V4ExperimentContract")
        if not isinstance(
            self.selection_anchor_provenance, SelectionAnchorProvenance
        ):
            raise TypeError(
                "selection_anchor_provenance must be SelectionAnchorProvenance"
            )
        if not isinstance(self.frozen_challenger, ModelIdentity):
            raise TypeError("frozen_challenger must be ModelIdentity")
        eligible_identities = {
            canonical_json_bytes(
                self.experiment_contract.candidate_identity(lag).to_dict()
            )
            for lag in PROMOTION_ELIGIBLE_LAGS_MS
        }
        if canonical_json_bytes(self.frozen_challenger.to_dict()) not in (
            eligible_identities
        ):
            raise ExperimentValidationError(
                "frozen challenger is not an eligible shorter v4 candidate"
            )
        if not self.calibration_artifacts:
            raise ExperimentValidationError(
                "calibration_artifacts must not be empty"
            )
        _validate_artifact_bindings(
            [item.to_dict() for item in self.calibration_artifacts],
            "calibration_artifacts",
        )
        _require_sha256(
            self.calibration_efficacy_started_sha256,
            "calibration_efficacy_started_sha256",
        )
        _require_sha256(
            self.calibration_efficacy_completed_sha256,
            "calibration_efficacy_completed_sha256",
        )
        if (
            self.calibration_efficacy_started_sha256
            == self.calibration_efficacy_completed_sha256
        ):
            raise ExperimentValidationError(
                "calibration start and completion markers must be distinct"
            )
        _validate_calibration_artifact_set(
            self.calibration_artifacts,
            calibration_attempt_index=self.attempt.calibration_attempt_index,
            start_marker_sha256=self.calibration_efficacy_started_sha256,
            completion_marker_sha256=(
                self.calibration_efficacy_completed_sha256
            ),
        )
        by_calibration_type = {
            artifact.artifact_type: artifact
            for artifact in self.calibration_artifacts
        }
        anchor = self.selection_anchor_provenance
        if self.attempt.holdout_attempt_index == 0:
            if anchor.mode != "calibration_completion":
                raise ExperimentValidationError(
                    "initial holdout anchor must come from calibration completion"
                )
            if anchor.source_artifact != by_calibration_type[
                "calibration_efficacy_completed"
            ]:
                raise ExperimentValidationError(
                    "selection anchor does not bind calibration completion"
                )
            if anchor.authorization_artifact != by_calibration_type[
                "holdout_selection_authorization"
            ]:
                raise ExperimentValidationError(
                    "selection anchor does not bind holdout authorization"
                )
        for field_name in (
            "holdout_start_ms",
            "holdout_end_ms",
            "archive_input_start_ms",
            "archive_input_end_ms",
            "seal_timeout_ms",
            "seal_deadline_ms",
            "preregistration_publication_deadline_ms",
            "pushed_receipt_deadline_ms",
            "created_at_ms",
        ):
            _require_int(getattr(self, field_name), field_name, minimum=0)
        if self.holdout_start_ms % DAY_MS:
            raise ExperimentValidationError(
                "holdout must start at midnight UTC"
            )
        if self.holdout_end_ms - self.holdout_start_ms != DAY_MS:
            raise ExperimentValidationError(
                "holdout must be one exact UTC day"
            )
        required_warmup_ms = max(
            self.experiment_contract.forecast_settings.history_retention_ms,
            self.experiment_contract.active_incumbent.forecast_config.history_retention_ms,
        ) + POLL_MS
        if self.archive_input_start_ms > (
            self.holdout_start_ms - required_warmup_ms
        ):
            raise ExperimentValidationError(
                "archive input does not cover the required warmup"
            )
        if (
            self.archive_input_end_ms
            < self.holdout_end_ms
            + max(COMPARISON_LAGS_MS)
            + FINALIZATION_ALLOWANCE_MS
        ):
            raise ExperimentValidationError(
                "archive input does not cover the finalization tail"
            )
        if self.seal_deadline_ms != self.holdout_end_ms + self.seal_timeout_ms:
            raise ExperimentValidationError("seal deadline is inconsistent")
        if self.seal_deadline_ms < self.archive_input_end_ms:
            raise ExperimentValidationError(
                "seal deadline precedes the required archive tail"
            )
        _require_sha256(
            self.candidate_day_ledger_root, "candidate_day_ledger_root"
        )
        _require_sha256(
            self.provenance_continuity_root, "provenance_continuity_root"
        )
        expected_publication_deadline = (
            self.archive_input_start_ms
            - MINIMUM_PREREGISTRATION_LEAD_MS
            - PREREGISTRATION_PUBLICATION_ALLOWANCE_MS
        )
        if self.archive_input_start_ms < (
            self.selection_anchor_ms
            + MINIMUM_PREREGISTRATION_LEAD_MS
            + PREREGISTRATION_PUBLICATION_ALLOWANCE_MS
        ):
            raise ExperimentValidationError(
                "holdout archive starts before the frozen selection lead"
            )
        if self.preregistration_publication_deadline_ms != (
            expected_publication_deadline
        ):
            raise ExperimentValidationError(
                "preregistration publication deadline is inconsistent"
            )
        if self.pushed_receipt_deadline_ms != (
            self.archive_input_start_ms - MINIMUM_PREREGISTRATION_LEAD_MS
        ):
            raise ExperimentValidationError(
                "pushed-receipt deadline is inconsistent"
            )
        if not self.authoritative_remote_ref.startswith("refs/heads/"):
            raise ExperimentValidationError(
                "authoritative remote ref must be a full branch ref"
            )
        _require_string(
            self.authoritative_remote_ref, "authoritative_remote_ref"
        )
        _require_sha256(
            self.authoritative_remote_url_sha256,
            "authoritative_remote_url_sha256",
        )
        if self.created_at_ms < self.selection_anchor_ms:
            raise ExperimentValidationError(
                "preregistration predates its selection anchor"
            )
        if self.created_at_ms > self.preregistration_publication_deadline_ms:
            raise ExperimentValidationError("preregistration was published late")
        _validate_origin_contract(dict(self.origin_contract))
        _validate_sha_contract(
            self.archive_health_contract,
            "archive_health_contract",
            _ARCHIVE_HEALTH_CONTRACT_KEYS,
        )
        provenance_contract = _validate_sha_contract(
            self.provenance_contract,
            "provenance_contract",
            _PROVENANCE_CONTRACT_KEYS,
        )
        if provenance_contract["current_provenance_continuity_root"] != (
            self.provenance_continuity_root
        ):
            raise ExperimentValidationError(
                "provenance contract root differs from the preregistration"
            )
        self._validate_prior_evidence()
        if (
            self.attempt.calibration_attempt_index == 1
            and self.attempt.holdout_attempt_index == 0
        ):
            prior = _as_mapping(self.prior_evidence, "prior_evidence")
            if (
                ArtifactBinding.from_dict(
                    prior["retry_eligibility"],
                    "prior_evidence.retry_eligibility",
                )
                != by_calibration_type["calibration_retry_eligibility"]
                or ArtifactBinding.from_dict(
                    prior["successor_authorization"],
                    "prior_evidence.successor_authorization",
                )
                != by_calibration_type[
                    "calibration_successor_authorization"
                ]
            ):
                raise ExperimentValidationError(
                    "calibration successor ancestry differs from its "
                    "calibration artifact chain"
                )
        if self.attempt.holdout_attempt_index == 1:
            prior = _as_mapping(self.prior_evidence, "prior_evidence")
            retry_eligibility = ArtifactBinding.from_dict(
                prior["retry_eligibility"],
                "prior_evidence.retry_eligibility",
            )
            successor_authorization = ArtifactBinding.from_dict(
                prior["successor_authorization"],
                "prior_evidence.successor_authorization",
            )
            if (
                self.selection_anchor_provenance.mode != "retry_eligibility"
                or self.selection_anchor_provenance.source_artifact
                != retry_eligibility
                or self.selection_anchor_provenance.authorization_artifact
                != successor_authorization
            ):
                raise ExperimentValidationError(
                    "successor selection anchor is not bound to its authorization"
                )
        object.__setattr__(self, "origin_contract", _freeze_json(self.origin_contract))
        object.__setattr__(
            self,
            "archive_health_contract",
            _freeze_json(self.archive_health_contract),
        )
        object.__setattr__(
            self,
            "provenance_contract",
            _freeze_json(self.provenance_contract),
        )
        object.__setattr__(self, "prior_evidence", _freeze_json(self.prior_evidence))

    def _validate_prior_evidence(self) -> None:
        payload = _as_mapping(dict(self.prior_evidence), "prior_evidence")
        if self.attempt.holdout_attempt_index == 1:
            expected = (
                "mode",
                "parent_result",
                "retry_eligibility",
                "successor_authorization",
                "inherited_ancestry",
                "prior_attempt_was_loss_free_quality_only",
                "no_holdout_efficacy_generated_or_exposed",
                "inspected_artifacts",
            )
            _require_exact_keys(payload, expected, "prior_evidence")
            if payload["mode"] != "holdout_quality_only_successor":
                raise ExperimentValidationError("prior-evidence mode is invalid")
            expected_retry_type = "holdout_retry_eligibility"
            expected_authorization_type = "holdout_successor_authorization"
            no_efficacy_key = "no_holdout_efficacy_generated_or_exposed"
            successor_stage = "holdout"
        elif self.attempt.calibration_attempt_index == 1:
            expected = (
                "mode",
                "parent_result",
                "retry_eligibility",
                "successor_authorization",
                "prior_attempt_was_loss_free_quality_only",
                "no_calibration_efficacy_generated_or_exposed",
                "inspected_artifacts",
            )
            _require_exact_keys(payload, expected, "prior_evidence")
            if payload["mode"] != "calibration_quality_only_successor":
                raise ExperimentValidationError("prior-evidence mode is invalid")
            expected_retry_type = "calibration_retry_eligibility"
            expected_authorization_type = "calibration_successor_authorization"
            no_efficacy_key = "no_calibration_efficacy_generated_or_exposed"
            successor_stage = "calibration"
        else:
            expected = (
                "mode",
                "all_previously_inspected_evidence_is_calibration_only",
                "inspected_artifacts",
            )
            _require_exact_keys(payload, expected, "prior_evidence")
            if payload["mode"] != "initial_holdout":
                raise ExperimentValidationError("prior-evidence mode is invalid")
            if not _require_bool(
                payload["all_previously_inspected_evidence_is_calibration_only"],
                "prior_evidence.all_previously_inspected_evidence_is_calibration_only",
            ):
                raise ExperimentValidationError(
                    "initial holdout must declare calibration-only prior evidence"
                )
            successor_stage = None
        if successor_stage is not None:
            parent_result = ArtifactBinding.from_dict(
                payload["parent_result"], "prior_evidence.parent_result"
            )
            retry_eligibility = ArtifactBinding.from_dict(
                payload["retry_eligibility"],
                "prior_evidence.retry_eligibility",
            )
            successor_authorization = ArtifactBinding.from_dict(
                payload["successor_authorization"],
                "prior_evidence.successor_authorization",
            )
            expected_types = (
                (parent_result, TERMINAL_RESULT_ARTIFACT_TYPE),
                (retry_eligibility, expected_retry_type),
                (successor_authorization, expected_authorization_type),
            )
            if any(
                artifact.artifact_type != expected_type
                or artifact.schema_version != 1
                for artifact, expected_type in expected_types
            ):
                raise ExperimentValidationError(
                    f"{successor_stage} successor bindings have unsupported "
                    "identities"
                )
            for key in (
                "prior_attempt_was_loss_free_quality_only",
                no_efficacy_key,
            ):
                if not _require_bool(payload[key], f"prior_evidence.{key}"):
                    raise ExperimentValidationError(
                        f"{successor_stage} successor requires loss-free "
                        "prior evidence"
                    )
            if successor_stage == "holdout":
                inherited_ancestry = _validate_artifact_bindings(
                    payload["inherited_ancestry"],
                    "prior_evidence.inherited_ancestry",
                )
                expected_inherited_types = (
                    (
                        TERMINAL_RESULT_ARTIFACT_TYPE,
                        "calibration_retry_eligibility",
                        "calibration_successor_authorization",
                    )
                    if self.attempt.calibration_attempt_index == 1
                    else ()
                )
                if tuple(
                    artifact.artifact_type
                    for artifact in inherited_ancestry
                ) != expected_inherited_types or any(
                    artifact.schema_version != EXPERIMENT_SCHEMA_VERSION
                    for artifact in inherited_ancestry
                ):
                    raise ExperimentValidationError(
                        "holdout successor inherited ancestry is incomplete"
                    )
                if inherited_ancestry:
                    by_calibration_type = {
                        artifact.artifact_type: artifact
                        for artifact in self.calibration_artifacts
                    }
                    if inherited_ancestry[-2:] != (
                        by_calibration_type["calibration_retry_eligibility"],
                        by_calibration_type[
                            "calibration_successor_authorization"
                        ],
                    ):
                        raise ExperimentValidationError(
                            "holdout successor inherited ancestry differs "
                            "from the calibration artifact chain"
                        )
        inspected = _validate_inspected_evidence_bindings(
            payload["inspected_artifacts"], "prior_evidence.inspected_artifacts"
        )
        if not inspected:
            raise ExperimentValidationError(
                "prior evidence must inventory every inspected artifact"
            )
        holdout_efficacy_types = {
            "holdout_efficacy_started",
            "holdout_efficacy_ledger",
            "holdout_bootstrap_report",
            "holdout_efficacy_report",
            "holdout_efficacy_completed",
        }
        if any(
            item.artifact.artifact_type in holdout_efficacy_types
            for item in inspected
        ):
            raise ExperimentValidationError(
                "prior evidence cannot expose holdout efficacy artifacts"
            )
        expected_scope = {
            None: "calibration_only",
            "calibration": "calibration_quality_only",
            "holdout": "holdout_quality_only",
        }[successor_stage]
        if any(item.evidence_scope != expected_scope for item in inspected):
            raise ExperimentValidationError(
                "prior evidence scope is inconsistent with the attempt lineage"
            )
        if successor_stage is None:
            if any(
                item.inspection_role == "prior_attempt_quality_only"
                for item in inspected
            ):
                raise ExperimentValidationError(
                    "initial holdout cannot relabel prior-attempt quality evidence"
                )
        elif any(
            item.inspection_role != "prior_attempt_quality_only"
            for item in inspected
        ):
            raise ExperimentValidationError(
                "successor prior evidence must be classified as prior-attempt "
                "quality-only evidence"
            )
        ordered = tuple(
            sorted(
                inspected,
                key=lambda item: (
                    item.artifact.artifact_type,
                    item.artifact.schema_version,
                    item.artifact.sha256,
                    item.source_lineage_id,
                    item.source_experiment_id,
                    item.window_start_ms,
                    item.window_end_ms,
                    item.inspection_role,
                    item.evidence_scope,
                ),
            )
        )
        if inspected != ordered:
            raise ExperimentValidationError(
                "prior evidence inventory is not in canonical order"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": PREREGISTRATION_ARTIFACT_TYPE,
            "schema_version": EXPERIMENT_SCHEMA_VERSION,
            "policy_version": POLICY_VERSION,
            "attempt": self.attempt.to_dict(),
            "experiment_contract": self.experiment_contract.to_dict(),
            "experiment_contract_digest": self.experiment_contract.digest,
            "selection_anchor_ms": self.selection_anchor_ms,
            "selection_anchor_provenance": (
                self.selection_anchor_provenance.to_dict()
            ),
            "frozen_challenger": self.frozen_challenger.to_dict(),
            "calibration_artifacts": [
                binding.to_dict() for binding in self.calibration_artifacts
            ],
            "calibration_efficacy_started_sha256": (
                self.calibration_efficacy_started_sha256
            ),
            "calibration_efficacy_completed_sha256": (
                self.calibration_efficacy_completed_sha256
            ),
            "holdout_window": {
                "start_ms": self.holdout_start_ms,
                "end_ms": self.holdout_end_ms,
                "boundary": "[start_ms,end_ms)",
            },
            "archive": {
                "input_start_ms": self.archive_input_start_ms,
                "input_end_ms": self.archive_input_end_ms,
                "seal_timeout_ms": self.seal_timeout_ms,
                "seal_deadline_ms": self.seal_deadline_ms,
            },
            "candidate_day_ledger_root": self.candidate_day_ledger_root,
            "provenance_continuity_root": self.provenance_continuity_root,
            "minimum_preregistration_lead_ms": MINIMUM_PREREGISTRATION_LEAD_MS,
            "preregistration_publication_allowance_ms": (
                PREREGISTRATION_PUBLICATION_ALLOWANCE_MS
            ),
            "preregistration_publication_deadline_ms": (
                self.preregistration_publication_deadline_ms
            ),
            "pushed_receipt_deadline_ms": self.pushed_receipt_deadline_ms,
            "authoritative_remote_ref": self.authoritative_remote_ref,
            "authoritative_remote_url_sha256": (
                self.authoritative_remote_url_sha256
            ),
            "timing_cells": [cell.to_dict() for cell in V4_TIMING_CELLS],
            "origin_contract": _json_ready(self.origin_contract),
            "bootstrap_contract": _json_ready(FROZEN_BOOTSTRAP_CONTRACT),
            "quality_gates": _json_ready(FROZEN_QUALITY_GATES),
            "holdout_performance_gates": _json_ready(FROZEN_HOLDOUT_GATES),
            "archive_health_contract": _json_ready(
                self.archive_health_contract
            ),
            "provenance_contract": _json_ready(self.provenance_contract),
            "prior_evidence": _json_ready(self.prior_evidence),
            "decimal_context": {
                "precision": 50,
                "rounding": "ROUND_HALF_EVEN",
                "financial_json_type": "decimal_string",
            },
            "prohibitions": {
                "rerank": True,
                "runner_up_fallback": True,
                "dynamic_switching": True,
                "more_than_one_efficacy_bearing_holdout": True,
                "production_activation": True,
            },
            "created_at_ms": self.created_at_ms,
        }


def _coerce_payload(value: object, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, (bytes, str)):
        raw = value.encode("utf-8") if isinstance(value, str) else value
        payload = decode_strict_json(raw)
        if raw != canonical_artifact_bytes(payload):
            raise ExperimentValidationError(
                f"{field_name} is not canonical JSON with one trailing LF"
            )
        return payload
    payload = _as_mapping(value, field_name)
    return decode_strict_json(canonical_json_bytes(payload))


def _validate_bound_artifact_payload(
    value: object,
    *,
    binding: ArtifactBinding,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, (bytes, str)):
        raise ExperimentValidationError(
            f"{field_name} must be canonical raw artifact bytes"
        )
    payload = _coerce_payload(value, field_name)
    if artifact_sha256(payload) != binding.sha256:
        raise ExperimentValidationError(
            f"{field_name} bytes differ from the bound SHA-256"
        )
    if payload.get("artifact_type") != binding.artifact_type:
        raise ExperimentValidationError(
            f"{field_name} artifact type differs from its binding"
        )
    if _require_int(
        payload.get("schema_version"), f"{field_name}.schema_version"
    ) != binding.schema_version:
        raise ExperimentValidationError(
            f"{field_name} schema differs from its binding"
        )
    return payload


def _validate_loss_free_successor_parent(
    value: object,
    *,
    binding: ArtifactBinding,
    expected_contract: V4ExperimentContract,
    successor_attempt: AttemptIdentity,
    successor_stage: str,
) -> Mapping[str, Any]:
    payload = _validate_bound_artifact_payload(
        value,
        binding=binding,
        field_name="successor_parent_result_artifact",
    )
    required_fields = {
        "artifact_type",
        "schema_version",
        "policy_version",
        "attempt",
        "experiment_contract",
        "experiment_contract_digest",
        "terminal_stage",
        "decision",
        "failure_stage",
        "failure_reasons",
        "parent_result",
        "ancestry",
        "retry_state",
        "selection_anchor_provenance",
        "candidate_day_ledger_root",
        "provenance_continuity_root",
        "preregistration_binding",
        "preregistration_publication_deadline_ms",
        "receipt_deadline_check",
        "pushed_receipt",
        "holdout_attempted",
        "calibration_efficacy_started",
        "calibration_efficacy_completed",
        "calibration_start_marker",
        "calibration_completion_marker",
        "holdout_efficacy_started",
        "holdout_efficacy_completed",
        "holdout_start_marker",
        "holdout_completion_marker",
        "terminal_efficacy_completed_at_ms",
        "efficacy_attempt_consumed",
        "evidence_artifacts",
        "provenance_checkpoint",
        "quality_evidence",
        "frozen_challenger",
        "created_at_ms",
    }
    _require_exact_keys(
        payload, tuple(required_fields), "successor_parent_result_artifact"
    )
    if payload.get("policy_version") != POLICY_VERSION:
        raise ExperimentValidationError(
            "successor parent uses a different experiment policy"
        )
    expected_contract.validate_payload(payload.get("experiment_contract"))
    if payload.get("experiment_contract_digest") != expected_contract.digest:
        raise ExperimentValidationError(
            "successor parent uses a different experiment contract"
        )
    parent_attempt = AttemptIdentity.from_dict(
        payload.get("attempt"), "successor_parent_result_artifact.attempt"
    )
    if (
        parent_attempt.calibration_lineage_id
        != successor_attempt.calibration_lineage_id
        or parent_attempt.experiment_id == successor_attempt.experiment_id
    ):
        raise ExperimentValidationError(
            "successor parent identity differs from its allocated lineage"
        )
    if successor_stage == "calibration":
        expected_indexes = (
            successor_attempt.calibration_attempt_index - 1,
            None,
        )
        started = payload.get("calibration_efficacy_started")
        completed = payload.get("calibration_efficacy_completed")
        start_marker = payload.get("calibration_start_marker")
        completion_marker = payload.get("calibration_completion_marker")
        remaining_key = "calibration_successors_remaining"
        retryable_failure_stages = {
            "calibration_window_selection",
            "calibration_attempt_freeze",
            "calibration_archive",
            "calibration_quality",
        }
    elif successor_stage == "holdout":
        expected_indexes = (
            successor_attempt.calibration_attempt_index,
            successor_attempt.holdout_attempt_index - 1,
        )
        started = payload.get("holdout_efficacy_started")
        completed = payload.get("holdout_efficacy_completed")
        start_marker = payload.get("holdout_start_marker")
        completion_marker = payload.get("holdout_completion_marker")
        remaining_key = "holdout_successors_remaining"
        retryable_failure_stages = {
            "holdout_window_selection",
            "preregistration_lead",
            "holdout_archive",
            "holdout_quality",
        }
    else:
        raise ExperimentValidationError("successor stage is unsupported")
    if (
        parent_attempt.calibration_attempt_index,
        parent_attempt.holdout_attempt_index,
    ) != expected_indexes:
        raise ExperimentValidationError(
            "successor parent indexes do not immediately precede allocation"
        )
    reasons = _as_sequence(
        payload.get("failure_reasons"),
        "successor_parent_result_artifact.failure_reasons",
    )
    retry_state = _as_mapping(
        payload.get("retry_state"),
        "successor_parent_result_artifact.retry_state",
    )
    if not (
        payload.get("terminal_stage") == successor_stage
        and payload.get("decision") == "insufficient_evidence"
        and payload.get("failure_stage") in retryable_failure_stages
        and "structural_gate_infeasibility" not in reasons
        and started is False
        and completed is False
        and start_marker is None
        and completion_marker is None
        and payload.get("terminal_efficacy_completed_at_ms") is None
        and payload.get("efficacy_attempt_consumed") is False
        and retry_state.get("successor_allowed") is True
        and retry_state.get("lineage_closed") is False
        and retry_state.get("retries_exhausted") is False
        and retry_state.get(remaining_key) == 1
    ):
        raise ExperimentValidationError(
            "successor parent is not a loss-free retryable quality-only result"
        )
    reconstructed_parent = _reconstruct_terminal_result_for_parent(
        payload, expected_contract
    )
    if canonical_json_bytes(payload) != canonical_json_bytes(
        reconstructed_parent.to_dict()
    ):
        raise ExperimentValidationError(
            "successor parent is not a canonical strict terminal result"
        )
    return payload


def _calibration_stage_attempt(attempt: AttemptIdentity) -> AttemptIdentity:
    return AttemptIdentity(
        calibration_lineage_id=attempt.calibration_lineage_id,
        experiment_id=attempt.experiment_id,
        calibration_attempt_index=attempt.calibration_attempt_index,
        holdout_attempt_index=None,
    )


def _validate_authorized_successor_chain(
    *,
    attempt: AttemptIdentity,
    expected_contract: V4ExperimentContract,
    successor_stage: str,
    parent_binding: ArtifactBinding,
    retry_binding: ArtifactBinding,
    authorization_binding: ArtifactBinding,
    parent_artifact: Optional[object],
    retry_artifact: Optional[object],
    authorization_artifact: Optional[object],
    expected_parent_binding: Optional[ArtifactBinding],
    expected_restoration_evidence: Optional[Sequence[ArtifactBinding]],
    expected_candidate_day_ledger_root: str,
    expected_provenance_continuity_root: Optional[str],
    selected_window_binding: Optional[ArtifactBinding],
    published_at_ms: int,
) -> None:
    if expected_parent_binding is None:
        raise ExperimentValidationError(
            "successor validation requires its authoritative parent binding"
        )
    if not isinstance(expected_parent_binding, ArtifactBinding):
        raise ExperimentValidationError(
            "expected_successor_parent_result must be an ArtifactBinding"
        )
    if parent_binding != expected_parent_binding:
        raise ExperimentValidationError(
            "successor parent differs from the authoritative lineage"
        )
    if parent_artifact is None:
        raise ExperimentValidationError(
            "successor validation requires its canonical raw parent"
        )
    parent_payload = _validate_loss_free_successor_parent(
        parent_artifact,
        binding=parent_binding,
        expected_contract=expected_contract,
        successor_attempt=attempt,
        successor_stage=successor_stage,
    )
    if retry_artifact is None or authorization_artifact is None:
        raise ExperimentValidationError(
            "successor validation requires canonical retry eligibility and "
            "authorization artifacts"
        )
    retry_payload = _validate_bound_artifact_payload(
        retry_artifact,
        binding=retry_binding,
        field_name=f"{successor_stage}_retry_eligibility_artifact",
    )
    _require_exact_keys(
        retry_payload,
        (
            "artifact_type",
            "schema_version",
            "parent_result",
            "allocated_attempt",
            "failure_stage",
            "created_at_ms",
            "restoration_evidence",
            "successors_remaining_after_allocation",
        ),
        f"{successor_stage}_retry_eligibility_artifact",
    )
    restoration_evidence = _validate_artifact_bindings(
        retry_payload["restoration_evidence"],
        f"{successor_stage}_retry_eligibility_artifact.restoration_evidence",
    )
    if not restoration_evidence:
        raise ExperimentValidationError(
            "retry eligibility requires objective restoration evidence"
        )
    if expected_restoration_evidence is None:
        raise ExperimentValidationError(
            "successor validation requires authoritative restoration evidence"
        )
    try:
        authoritative_restoration = tuple(expected_restoration_evidence)
    except TypeError as exc:
        raise ExperimentValidationError(
            "expected_retry_restoration_evidence must be an array"
        ) from exc
    if not all(
        isinstance(item, ArtifactBinding) for item in authoritative_restoration
    ):
        raise ExperimentValidationError(
            "expected_retry_restoration_evidence must contain ArtifactBinding "
            "values"
        )
    if restoration_evidence != authoritative_restoration:
        raise ExperimentValidationError(
            "retry restoration evidence differs from the authoritative "
            "eligibility record"
        )
    required_retry = {
        "parent_result": parent_binding.to_dict(),
        "allocated_attempt": attempt.to_dict(),
        "failure_stage": parent_payload["failure_stage"],
        "successors_remaining_after_allocation": 0,
    }
    for field_name, expected_value in required_retry.items():
        if canonical_json_bytes(retry_payload.get(field_name)) != (
            canonical_json_bytes(expected_value)
        ):
            raise ExperimentValidationError(
                "retry eligibility does not derive from its loss-free parent"
            )
    retry_created_at_ms = _require_int(
        retry_payload["created_at_ms"],
        f"{successor_stage}_retry_eligibility_artifact.created_at_ms",
        minimum=0,
    )
    parent_created_at_ms = _require_int(
        parent_payload.get("created_at_ms"),
        "successor_parent_result_artifact.created_at_ms",
        minimum=0,
    )
    if retry_created_at_ms < parent_created_at_ms:
        raise ExperimentValidationError(
            "retry eligibility predates its terminal parent"
        )
    if retry_created_at_ms > published_at_ms:
        raise ExperimentValidationError(
            "retry eligibility follows the successor artifact"
        )
    authorization_payload = _validate_bound_artifact_payload(
        authorization_artifact,
        binding=authorization_binding,
        field_name=f"{successor_stage}_successor_authorization_artifact",
    )
    if successor_stage == "calibration":
        _require_exact_keys(
            authorization_payload,
            (
                "artifact_type",
                "schema_version",
                "attempt",
                "parent_result",
                "retry_eligibility_sha256",
                "experiment_contract_digest",
                "candidate_day_ledger_root",
                "selected_window_artifact",
                "provenance_continuity_root",
                "successors_remaining_after_allocation",
            ),
            "calibration_successor_authorization_artifact",
        )
    required_authorization = {
        "attempt": attempt.to_dict(),
        "parent_result": parent_binding.to_dict(),
        "retry_eligibility_sha256": retry_binding.sha256,
        "experiment_contract_digest": expected_contract.digest,
        "candidate_day_ledger_root": _require_sha256(
            expected_candidate_day_ledger_root,
            "expected_candidate_day_ledger_root",
        ),
        "successors_remaining_after_allocation": 0,
    }
    if successor_stage == "calibration":
        _require_binding_identity(
            selected_window_binding,
            "calibration_attempt_freeze",
            "selected_window_binding",
        )
        required_authorization["selected_window_artifact"] = (
            selected_window_binding.to_dict()
        )
    if expected_provenance_continuity_root is None:
        raise ExperimentValidationError(
            "successor validation requires its authoritative authorization "
            "provenance root"
        )
    authorization_provenance_root = _require_sha256(
        authorization_payload.get("provenance_continuity_root"),
        "successor_authorization.provenance_continuity_root",
    )
    if authorization_provenance_root != _require_sha256(
        expected_provenance_continuity_root,
        "expected_provenance_continuity_root",
    ):
        raise ExperimentValidationError(
            "successor authorization does not bind the expected provenance root"
        )
    for field_name, expected_value in required_authorization.items():
        if canonical_json_bytes(authorization_payload.get(field_name)) != (
            canonical_json_bytes(expected_value)
        ):
            raise ExperimentValidationError(
                "successor authorization does not bind its exact parent, "
                "eligibility, and contract"
            )


def _validate_holdout_selection_anchor_artifacts(
    *,
    attempt: AttemptIdentity,
    expected_contract: V4ExperimentContract,
    anchor: SelectionAnchorProvenance,
    challenger: ModelIdentity,
    source_artifact: Optional[object],
    authorization_artifact: Optional[object],
    calibration_report_artifact: Optional[object],
    expected_calibration_report: Optional[ArtifactBinding],
    expected_calibration_completion: Optional[ArtifactBinding],
    expected_candidate_day_ledger_root: str,
    expected_provenance_continuity_root: Optional[str],
) -> None:
    if source_artifact is None or authorization_artifact is None:
        raise ExperimentValidationError(
            "holdout result requires canonical raw selection-anchor source "
            "and authorization artifacts"
        )
    source_payload = _validate_bound_artifact_payload(
        source_artifact,
        binding=anchor.source_artifact,
        field_name="selection_anchor_source_artifact",
    )
    source_timestamp = _require_int(
        source_payload.get(anchor.timestamp_field),
        f"selection_anchor_source_artifact.{anchor.timestamp_field}",
        minimum=0,
    )
    if source_timestamp != anchor.timestamp_ms:
        raise ExperimentValidationError(
            "selection anchor differs from its hashed source artifact"
        )
    authorization_payload = _validate_bound_artifact_payload(
        authorization_artifact,
        binding=anchor.authorization_artifact,
        field_name="selection_anchor_authorization_artifact",
    )
    authorization_keys = [
        "artifact_type",
        "schema_version",
        "attempt",
        "selection_anchor_source_sha256",
        "selection_anchor_ms",
        "frozen_challenger",
        "experiment_contract_digest",
        "calibration_efficacy_report_sha256",
        "calibration_efficacy_completed_sha256",
    ]
    if attempt.holdout_attempt_index == 1:
        authorization_keys.extend(
            (
                "parent_result",
                "retry_eligibility_sha256",
                "holdout_window",
                "candidate_day_ledger_root",
                "provenance_continuity_root",
                "successors_remaining_after_allocation",
            )
        )
    _require_exact_keys(
        authorization_payload,
        authorization_keys,
        "selection_anchor_authorization_artifact",
    )
    required_authorization = {
        "attempt": attempt.to_dict(),
        "selection_anchor_source_sha256": anchor.source_artifact.sha256,
        "selection_anchor_ms": anchor.timestamp_ms,
        "frozen_challenger": challenger.to_dict(),
        "experiment_contract_digest": expected_contract.digest,
    }
    if attempt.holdout_attempt_index == 1:
        required_authorization.update(
            candidate_day_ledger_root=_require_sha256(
                expected_candidate_day_ledger_root,
                "expected_candidate_day_ledger_root",
            ),
            provenance_continuity_root=_require_sha256(
                expected_provenance_continuity_root,
                "expected_provenance_continuity_root",
            ),
            successors_remaining_after_allocation=0,
        )
    for field_name, expected_value in required_authorization.items():
        if canonical_json_bytes(authorization_payload.get(field_name)) != (
            canonical_json_bytes(expected_value)
        ):
            raise ExperimentValidationError(
                "selection authorization does not bind the frozen holdout"
            )
    if (
        expected_calibration_report is None
        or expected_calibration_completion is None
    ):
        raise ExperimentValidationError(
            "holdout anchor validation requires authoritative calibration "
            "report and completion bindings"
        )
    if not isinstance(
        expected_calibration_report, ArtifactBinding
    ) or not isinstance(expected_calibration_completion, ArtifactBinding):
        raise ExperimentValidationError(
            "authoritative calibration identities must be ArtifactBinding "
            "values"
        )
    if authorization_payload.get(
        "calibration_efficacy_report_sha256"
    ) != expected_calibration_report.sha256 or authorization_payload.get(
        "calibration_efficacy_completed_sha256"
    ) != expected_calibration_completion.sha256:
        raise ExperimentValidationError(
            "selection authorization does not bind authoritative calibration"
        )
    if anchor.mode != "calibration_completion":
        return
    if anchor.source_artifact != expected_calibration_completion:
        raise ExperimentValidationError(
            "initial holdout anchor differs from authoritative completion"
        )
    if calibration_report_artifact is None:
        raise ExperimentValidationError(
            "initial holdout anchor requires its canonical calibration report"
        )
    report_payload = _validate_bound_artifact_payload(
        calibration_report_artifact,
        binding=expected_calibration_report,
        field_name="calibration_efficacy_report_artifact",
    )
    efficacy_ledger = ArtifactBinding.from_dict(
        report_payload.get("efficacy_ledger"),
        "calibration_efficacy_report_artifact.efficacy_ledger",
    )
    calibration_stage_attempt = _calibration_stage_attempt(attempt)
    expected_report_payload = calibration_selection_report_payload(
        attempt=calibration_stage_attempt,
        experiment_contract_digest=expected_contract.digest,
        frozen_challenger=challenger,
        efficacy_ledger=efficacy_ledger,
        efficacy_evidence=report_payload.get("efficacy_evidence"),
    )
    if canonical_json_bytes(report_payload) != canonical_json_bytes(
        expected_report_payload
    ):
        raise ExperimentValidationError(
            "initial holdout calibration report is not canonical"
        )
    start_marker = ArtifactBinding.from_dict(
        source_payload.get("efficacy_start_marker"),
        "selection_anchor_source_artifact.efficacy_start_marker",
    )
    immutable_inventory = _validate_artifact_bindings(
        source_payload.get("immutable_efficacy_artifacts"),
        "selection_anchor_source_artifact.immutable_efficacy_artifacts",
    )
    expected_completion_payload = efficacy_completion_marker_payload(
        attempt=calibration_stage_attempt,
        experiment_contract_digest=expected_contract.digest,
        terminal_stage="calibration",
        efficacy_start_marker=start_marker,
        prerequisite_artifacts=_validate_artifact_bindings(
            source_payload.get("prerequisite_artifacts"),
            "selection_anchor_source_artifact.prerequisite_artifacts",
        ),
        efficacy_report=expected_calibration_report,
        immutable_efficacy_artifacts=immutable_inventory,
        completed_at_ms=source_timestamp,
    )
    if canonical_json_bytes(source_payload) != canonical_json_bytes(
        expected_completion_payload
    ):
        raise ExperimentValidationError(
            "initial holdout calibration completion marker is not canonical"
        )


def validate_preregistration(
    value: object,
    *,
    expected_contract: V4ExperimentContract,
    selection_anchor_source_artifact: object,
    selection_anchor_authorization_artifact: object,
    calibration_efficacy_report_artifact: Optional[object] = None,
    calibration_completion_marker_artifact: Optional[object] = None,
    expected_calibration_efficacy_report: Optional[ArtifactBinding] = None,
    expected_calibration_completion_marker: Optional[ArtifactBinding] = None,
    successor_parent_result_artifact: Optional[object] = None,
    calibration_retry_eligibility_artifact: Optional[object] = None,
    calibration_successor_authorization_artifact: Optional[object] = None,
    calibration_parent_result_artifact: Optional[object] = None,
    expected_prior_evidence_artifacts: Optional[
        Sequence[InspectedEvidenceBinding]
    ] = None,
    expected_successor_parent_result: Optional[ArtifactBinding] = None,
    expected_retry_restoration_evidence: Optional[
        Sequence[ArtifactBinding]
    ] = None,
    expected_calibration_parent_result: Optional[ArtifactBinding] = None,
    expected_calibration_retry_restoration_evidence: Optional[
        Sequence[ArtifactBinding]
    ] = None,
    expected_calibration_authorization_provenance_root: Optional[str] = None,
    expected: Optional[V4Preregistration] = None,
) -> Mapping[str, Any]:
    payload = _coerce_payload(value, "preregistration")
    expected_keys = (
        "artifact_type",
        "schema_version",
        "policy_version",
        "attempt",
        "experiment_contract",
        "experiment_contract_digest",
        "selection_anchor_ms",
        "selection_anchor_provenance",
        "frozen_challenger",
        "calibration_artifacts",
        "calibration_efficacy_started_sha256",
        "calibration_efficacy_completed_sha256",
        "holdout_window",
        "archive",
        "candidate_day_ledger_root",
        "provenance_continuity_root",
        "minimum_preregistration_lead_ms",
        "preregistration_publication_allowance_ms",
        "preregistration_publication_deadline_ms",
        "pushed_receipt_deadline_ms",
        "authoritative_remote_ref",
        "authoritative_remote_url_sha256",
        "timing_cells",
        "origin_contract",
        "bootstrap_contract",
        "quality_gates",
        "holdout_performance_gates",
        "archive_health_contract",
        "provenance_contract",
        "prior_evidence",
        "decimal_context",
        "prohibitions",
        "created_at_ms",
    )
    _require_exact_keys(payload, expected_keys, "preregistration")
    if payload["artifact_type"] != PREREGISTRATION_ARTIFACT_TYPE:
        raise ExperimentValidationError("preregistration artifact type is invalid")
    if _require_int(
        payload["schema_version"], "preregistration.schema_version"
    ) != EXPERIMENT_SCHEMA_VERSION:
        raise ExperimentValidationError("preregistration schema is unsupported")
    if payload["policy_version"] != POLICY_VERSION:
        raise ExperimentValidationError("preregistration policy is unsupported")
    expected_contract.validate_payload(payload["experiment_contract"])
    if payload["experiment_contract_digest"] != expected_contract.digest:
        raise ExperimentValidationError("experiment contract digest differs")
    attempt = AttemptIdentity.from_dict(payload["attempt"], "attempt")
    challenger = ModelIdentity.from_dict(
        payload["frozen_challenger"], "frozen_challenger"
    )
    selection_anchor_provenance = SelectionAnchorProvenance.from_dict(
        payload["selection_anchor_provenance"],
        "selection_anchor_provenance",
    )
    if payload["selection_anchor_ms"] != selection_anchor_provenance.timestamp_ms:
        raise ExperimentValidationError(
            "selection_anchor_ms is not derived from its provenance"
        )
    source_payload = _validate_bound_artifact_payload(
        selection_anchor_source_artifact,
        binding=selection_anchor_provenance.source_artifact,
        field_name="selection_anchor_source_artifact",
    )
    source_timestamp = _require_int(
        source_payload.get(selection_anchor_provenance.timestamp_field),
        (
            "selection_anchor_source_artifact."
            f"{selection_anchor_provenance.timestamp_field}"
        ),
        minimum=0,
    )
    if source_timestamp != selection_anchor_provenance.timestamp_ms:
        raise ExperimentValidationError(
            "selection anchor differs from its hashed source artifact"
        )
    authorization_payload = _validate_bound_artifact_payload(
        selection_anchor_authorization_artifact,
        binding=selection_anchor_provenance.authorization_artifact,
        field_name="selection_anchor_authorization_artifact",
    )
    authorization_keys = [
        "artifact_type",
        "schema_version",
        "attempt",
        "selection_anchor_source_sha256",
        "selection_anchor_ms",
        "frozen_challenger",
        "experiment_contract_digest",
        "calibration_efficacy_report_sha256",
        "calibration_efficacy_completed_sha256",
    ]
    if attempt.holdout_attempt_index == 1:
        authorization_keys.extend(
            (
                "parent_result",
                "retry_eligibility_sha256",
                "holdout_window",
                "candidate_day_ledger_root",
                "provenance_continuity_root",
                "successors_remaining_after_allocation",
            )
        )
    _require_exact_keys(
        authorization_payload,
        authorization_keys,
        "selection_anchor_authorization_artifact",
    )
    required_authorization_fields = {
        "attempt": attempt.to_dict(),
        "selection_anchor_source_sha256": (
            selection_anchor_provenance.source_artifact.sha256
        ),
        "selection_anchor_ms": selection_anchor_provenance.timestamp_ms,
        "frozen_challenger": challenger.to_dict(),
    }
    if attempt.holdout_attempt_index == 1:
        required_authorization_fields.update(
            candidate_day_ledger_root=payload["candidate_day_ledger_root"],
            provenance_continuity_root=payload["provenance_continuity_root"],
            successors_remaining_after_allocation=0,
        )
    for field, expected_value in required_authorization_fields.items():
        if canonical_json_bytes(authorization_payload.get(field)) != (
            canonical_json_bytes(expected_value)
        ):
            raise ExperimentValidationError(
                "selection authorization does not bind the frozen holdout"
            )
    if attempt.holdout_attempt_index == 1:
        expected_window = {
            "start_ms": _require_int(
                _as_mapping(payload["holdout_window"], "holdout_window")[
                    "start_ms"
                ],
                "holdout_window.start_ms",
                minimum=0,
            ),
            "end_ms": _require_int(
                _as_mapping(payload["holdout_window"], "holdout_window")[
                    "end_ms"
                ],
                "holdout_window.end_ms",
                minimum=1,
            ),
            "boundary": "[start_ms,end_ms)",
        }
        if canonical_json_bytes(
            authorization_payload.get("holdout_window")
        ) != canonical_json_bytes(expected_window):
            raise ExperimentValidationError(
                "successor authorization does not bind the selected holdout"
            )
    calibration_artifacts = _validate_artifact_bindings(
        payload["calibration_artifacts"], "calibration_artifacts"
    )
    _validate_calibration_artifact_set(
        calibration_artifacts,
        calibration_attempt_index=attempt.calibration_attempt_index,
        start_marker_sha256=_require_sha256(
            payload["calibration_efficacy_started_sha256"],
            "calibration_efficacy_started_sha256",
        ),
        completion_marker_sha256=_require_sha256(
            payload["calibration_efficacy_completed_sha256"],
            "calibration_efficacy_completed_sha256",
        ),
    )
    by_calibration_type = {
        artifact.artifact_type: artifact
        for artifact in calibration_artifacts
    }
    if (
        calibration_efficacy_report_artifact is None
        or calibration_completion_marker_artifact is None
    ):
        raise ExperimentValidationError(
            "preregistration requires canonical raw calibration report and "
            "completion artifacts"
        )
    calibration_report_binding = by_calibration_type[
        "calibration_efficacy_report"
    ]
    if expected_calibration_efficacy_report is None or (
        expected_calibration_completion_marker is None
    ):
        raise ExperimentValidationError(
            "preregistration validation requires authoritative calibration "
            "report and completion bindings"
        )
    if not isinstance(
        expected_calibration_efficacy_report, ArtifactBinding
    ) or not isinstance(
        expected_calibration_completion_marker, ArtifactBinding
    ):
        raise ExperimentValidationError(
            "authoritative calibration identities must be ArtifactBinding "
            "values"
        )
    if calibration_report_binding != expected_calibration_efficacy_report:
        raise ExperimentValidationError(
            "calibration report differs from the authoritative result"
        )
    calibration_report_payload = _validate_bound_artifact_payload(
        calibration_efficacy_report_artifact,
        binding=calibration_report_binding,
        field_name="calibration_efficacy_report_artifact",
    )
    calibration_stage_attempt = _calibration_stage_attempt(attempt)
    if attempt.holdout_attempt_index == 1:
        prior_for_calibration_scope = _as_mapping(
            payload["prior_evidence"], "prior_evidence"
        )
        parent_for_calibration_scope = ArtifactBinding.from_dict(
            prior_for_calibration_scope.get("parent_result"),
            "prior_evidence.parent_result",
        )
        if successor_parent_result_artifact is None:
            raise ExperimentValidationError(
                "holdout successor requires its canonical raw parent before "
                "calibration inheritance can be validated"
            )
        parent_scope_payload = _validate_bound_artifact_payload(
            successor_parent_result_artifact,
            binding=parent_for_calibration_scope,
            field_name="successor_parent_result_artifact",
        )
        calibration_stage_attempt = _calibration_stage_attempt(
            AttemptIdentity.from_dict(
                parent_scope_payload.get("attempt"),
                "successor_parent_result_artifact.attempt",
            )
        )
    expected_calibration_report = calibration_selection_report_payload(
        attempt=calibration_stage_attempt,
        experiment_contract_digest=expected_contract.digest,
        frozen_challenger=challenger,
        efficacy_ledger=by_calibration_type[
            "calibration_efficacy_ledger"
        ],
        efficacy_evidence=calibration_report_payload.get(
            "efficacy_evidence"
        ),
    )
    if canonical_json_bytes(calibration_report_payload) != (
        canonical_json_bytes(expected_calibration_report)
    ):
        raise ExperimentValidationError(
            "frozen challenger differs from the immutable calibration report"
        )
    calibration_completion_binding = by_calibration_type[
        "calibration_efficacy_completed"
    ]
    if calibration_completion_binding != (
        expected_calibration_completion_marker
    ):
        raise ExperimentValidationError(
            "calibration completion differs from the authoritative result"
        )
    calibration_completion_payload = _validate_bound_artifact_payload(
        calibration_completion_marker_artifact,
        binding=calibration_completion_binding,
        field_name="calibration_completion_marker_artifact",
    )
    calibration_completed_at_ms = _require_int(
        calibration_completion_payload.get("completed_at_ms"),
        "calibration_completion_marker_artifact.completed_at_ms",
        minimum=0,
    )
    expected_calibration_completion = efficacy_completion_marker_payload(
        attempt=calibration_stage_attempt,
        experiment_contract_digest=expected_contract.digest,
        terminal_stage="calibration",
        efficacy_start_marker=by_calibration_type[
            "calibration_efficacy_started"
        ],
        prerequisite_artifacts=(
            by_calibration_type["calibration_attempt_freeze"],
            by_calibration_type["calibration_raw_manifest"],
            by_calibration_type["calibration_pre_efficacy_provenance_gate"],
        ),
        efficacy_report=calibration_report_binding,
        immutable_efficacy_artifacts=(
            by_calibration_type["calibration_efficacy_started"],
            by_calibration_type["calibration_efficacy_ledger"],
            calibration_report_binding,
        ),
        completed_at_ms=calibration_completed_at_ms,
    )
    if canonical_json_bytes(calibration_completion_payload) != (
        canonical_json_bytes(expected_calibration_completion)
    ):
        raise ExperimentValidationError(
            "calibration completion marker differs from its immutable report"
        )
    for field_name, expected_value in {
        "experiment_contract_digest": expected_contract.digest,
        "calibration_efficacy_report_sha256": (
            calibration_report_binding.sha256
        ),
        "calibration_efficacy_completed_sha256": (
            calibration_completion_binding.sha256
        ),
    }.items():
        if authorization_payload.get(field_name) != expected_value:
            raise ExperimentValidationError(
                "selection authorization does not bind immutable calibration"
            )
    successor_stage = (
        "holdout"
        if attempt.holdout_attempt_index == 1
        else (
            "calibration"
            if attempt.calibration_attempt_index == 1
            else None
        )
    )
    prior_evidence = _as_mapping(
        payload["prior_evidence"], "prior_evidence"
    )
    if expected_prior_evidence_artifacts is None:
        raise ExperimentValidationError(
            "preregistration validation requires the authoritative prior-"
            "evidence inventory"
        )
    try:
        authoritative_prior_evidence = tuple(
            expected_prior_evidence_artifacts
        )
    except TypeError as exc:
        raise ExperimentValidationError(
            "expected_prior_evidence_artifacts must be an array"
        ) from exc
    if not all(
        isinstance(item, InspectedEvidenceBinding)
        for item in authoritative_prior_evidence
    ):
        raise ExperimentValidationError(
            "expected_prior_evidence_artifacts must contain "
            "InspectedEvidenceBinding "
            "values"
        )
    inspected_prior_evidence = _validate_inspected_evidence_bindings(
        prior_evidence.get("inspected_artifacts"),
        "prior_evidence.inspected_artifacts",
    )
    if inspected_prior_evidence != authoritative_prior_evidence:
        raise ExperimentValidationError(
            "prior-evidence inventory is incomplete or differs from the "
            "authoritative inventory"
        )
    if successor_stage is not None:
        parent_binding = ArtifactBinding.from_dict(
            prior_evidence.get("parent_result"),
            "prior_evidence.parent_result",
        )
        retry_binding = ArtifactBinding.from_dict(
            prior_evidence.get("retry_eligibility"),
            "prior_evidence.retry_eligibility",
        )
        authorization_binding = ArtifactBinding.from_dict(
            prior_evidence.get("successor_authorization"),
            "prior_evidence.successor_authorization",
        )
        if successor_stage == "holdout":
            retry_artifact = selection_anchor_source_artifact
            successor_authorization_artifact = (
                selection_anchor_authorization_artifact
            )
        else:
            retry_artifact = calibration_retry_eligibility_artifact
            successor_authorization_artifact = (
                calibration_successor_authorization_artifact
            )
        _validate_authorized_successor_chain(
            attempt=(
                _calibration_stage_attempt(attempt)
                if successor_stage == "calibration"
                else attempt
            ),
            expected_contract=expected_contract,
            successor_stage=successor_stage,
            parent_binding=parent_binding,
            retry_binding=retry_binding,
            authorization_binding=authorization_binding,
            parent_artifact=successor_parent_result_artifact,
            retry_artifact=retry_artifact,
            authorization_artifact=successor_authorization_artifact,
            expected_parent_binding=expected_successor_parent_result,
            expected_restoration_evidence=(
                expected_retry_restoration_evidence
            ),
            expected_candidate_day_ledger_root=(
                payload["candidate_day_ledger_root"]
                if successor_stage == "holdout"
                else by_calibration_type[
                    "calibration_candidate_day_ledger"
                ].sha256
            ),
            expected_provenance_continuity_root=(
                payload["provenance_continuity_root"]
                if successor_stage == "holdout"
                else expected_calibration_authorization_provenance_root
            ),
            selected_window_binding=(
                by_calibration_type["calibration_attempt_freeze"]
                if successor_stage == "calibration"
                else None
            ),
            published_at_ms=_require_int(
                payload["created_at_ms"], "created_at_ms", minimum=0
            ),
        )
        if successor_stage == "holdout":
            for field_name, expected_value in {
                "calibration_efficacy_report_sha256": (
                    calibration_report_binding.sha256
                ),
                "calibration_efficacy_completed_sha256": (
                    calibration_completion_binding.sha256
                ),
            }.items():
                if authorization_payload.get(field_name) != expected_value:
                    raise ExperimentValidationError(
                        "holdout successor authorization does not bind the "
                        "immutable calibration"
                    )
    if (
        attempt.calibration_attempt_index == 1
        and attempt.holdout_attempt_index == 1
    ):
        inherited_ancestry = _validate_artifact_bindings(
            prior_evidence.get("inherited_ancestry"),
            "prior_evidence.inherited_ancestry",
        )
        if successor_parent_result_artifact is None:
            raise ExperimentValidationError(
                "combined successor requires its holdout parent"
            )
        holdout_parent_payload = _coerce_payload(
            successor_parent_result_artifact,
            "successor_parent_result_artifact",
        )
        inherited_attempt = AttemptIdentity.from_dict(
            holdout_parent_payload.get("attempt"),
            "successor_parent_result_artifact.attempt",
        )
        _validate_authorized_successor_chain(
            attempt=_calibration_stage_attempt(inherited_attempt),
            expected_contract=expected_contract,
            successor_stage="calibration",
            parent_binding=inherited_ancestry[0],
            retry_binding=inherited_ancestry[1],
            authorization_binding=inherited_ancestry[2],
            parent_artifact=calibration_parent_result_artifact,
            retry_artifact=calibration_retry_eligibility_artifact,
            authorization_artifact=calibration_successor_authorization_artifact,
            expected_parent_binding=expected_calibration_parent_result,
            expected_restoration_evidence=(
                expected_calibration_retry_restoration_evidence
            ),
            expected_candidate_day_ledger_root=by_calibration_type[
                "calibration_candidate_day_ledger"
            ].sha256,
            expected_provenance_continuity_root=(
                expected_calibration_authorization_provenance_root
            ),
            selected_window_binding=by_calibration_type[
                "calibration_attempt_freeze"
            ],
            published_at_ms=_require_int(
                payload["created_at_ms"], "created_at_ms", minimum=0
            ),
        )
    elif any(
        value is not None
        for value in (
            calibration_parent_result_artifact,
            expected_calibration_parent_result,
            expected_calibration_retry_restoration_evidence,
        )
    ):
        raise ExperimentValidationError(
            "preregistration cannot receive unbound inherited calibration "
            "successor evidence"
        )
    if successor_stage is None and any(
        value is not None
        for value in (
            successor_parent_result_artifact,
            calibration_retry_eligibility_artifact,
            calibration_successor_authorization_artifact,
            expected_successor_parent_result,
            expected_retry_restoration_evidence,
        )
    ):
        raise ExperimentValidationError(
            "initial preregistration cannot receive unbound successor evidence"
        )
    if (
        attempt.calibration_attempt_index == 0
        and expected_calibration_authorization_provenance_root is not None
    ):
        raise ExperimentValidationError(
            "preregistration cannot receive an unbound calibration "
            "authorization provenance root"
        )
    holdout = _as_mapping(payload["holdout_window"], "holdout_window")
    _require_exact_keys(
        holdout, ("start_ms", "end_ms", "boundary"), "holdout_window"
    )
    if holdout["boundary"] != "[start_ms,end_ms)":
        raise ExperimentValidationError("holdout boundary is invalid")
    archive = _as_mapping(payload["archive"], "archive")
    _require_exact_keys(
        archive,
        ("input_start_ms", "input_end_ms", "seal_timeout_ms", "seal_deadline_ms"),
        "archive",
    )
    if payload["minimum_preregistration_lead_ms"] != (
        MINIMUM_PREREGISTRATION_LEAD_MS
    ):
        raise ExperimentValidationError("minimum preregistration lead changed")
    if payload["preregistration_publication_allowance_ms"] != (
        PREREGISTRATION_PUBLICATION_ALLOWANCE_MS
    ):
        raise ExperimentValidationError("publication allowance changed")
    if payload["timing_cells"] != [
        cell.to_dict() for cell in V4_TIMING_CELLS
    ]:
        raise ExperimentValidationError("timing-cell set or order changed")
    _validate_origin_contract(payload["origin_contract"])
    if canonical_json_bytes(payload["bootstrap_contract"]) != (
        canonical_json_bytes(FROZEN_BOOTSTRAP_CONTRACT)
    ):
        raise ExperimentValidationError("bootstrap contract changed")
    if canonical_json_bytes(payload["quality_gates"]) != canonical_json_bytes(
        FROZEN_QUALITY_GATES
    ):
        raise ExperimentValidationError("quality gates changed")
    if canonical_json_bytes(payload["holdout_performance_gates"]) != (
        canonical_json_bytes(FROZEN_HOLDOUT_GATES)
    ):
        raise ExperimentValidationError("holdout performance gates changed")
    if payload["decimal_context"] != {
        "precision": 50,
        "rounding": "ROUND_HALF_EVEN",
        "financial_json_type": "decimal_string",
    }:
        raise ExperimentValidationError("Decimal context changed")
    if payload["prohibitions"] != {
        "rerank": True,
        "runner_up_fallback": True,
        "dynamic_switching": True,
        "more_than_one_efficacy_bearing_holdout": True,
        "production_activation": True,
    }:
        raise ExperimentValidationError("experiment prohibitions changed")
    reconstructed = V4Preregistration(
        attempt=attempt,
        experiment_contract=expected_contract,
        selection_anchor_provenance=selection_anchor_provenance,
        frozen_challenger=challenger,
        calibration_artifacts=calibration_artifacts,
        calibration_efficacy_started_sha256=_require_sha256(
            payload["calibration_efficacy_started_sha256"],
            "calibration_efficacy_started_sha256",
        ),
        calibration_efficacy_completed_sha256=_require_sha256(
            payload["calibration_efficacy_completed_sha256"],
            "calibration_efficacy_completed_sha256",
        ),
        holdout_start_ms=_require_int(
            holdout["start_ms"], "holdout_window.start_ms", minimum=0
        ),
        holdout_end_ms=_require_int(
            holdout["end_ms"], "holdout_window.end_ms", minimum=1
        ),
        archive_input_start_ms=_require_int(
            archive["input_start_ms"], "archive.input_start_ms", minimum=0
        ),
        archive_input_end_ms=_require_int(
            archive["input_end_ms"], "archive.input_end_ms", minimum=1
        ),
        seal_timeout_ms=_require_int(
            archive["seal_timeout_ms"], "archive.seal_timeout_ms", minimum=1
        ),
        seal_deadline_ms=_require_int(
            archive["seal_deadline_ms"], "archive.seal_deadline_ms", minimum=1
        ),
        candidate_day_ledger_root=_require_sha256(
            payload["candidate_day_ledger_root"], "candidate_day_ledger_root"
        ),
        provenance_continuity_root=_require_sha256(
            payload["provenance_continuity_root"],
            "provenance_continuity_root",
        ),
        preregistration_publication_deadline_ms=_require_int(
            payload["preregistration_publication_deadline_ms"],
            "preregistration_publication_deadline_ms",
            minimum=0,
        ),
        pushed_receipt_deadline_ms=_require_int(
            payload["pushed_receipt_deadline_ms"],
            "pushed_receipt_deadline_ms",
            minimum=0,
        ),
        authoritative_remote_ref=_require_string(
            payload["authoritative_remote_ref"],
            "authoritative_remote_ref",
        ),
        authoritative_remote_url_sha256=_require_sha256(
            payload["authoritative_remote_url_sha256"],
            "authoritative_remote_url_sha256",
        ),
        origin_contract=_as_mapping(payload["origin_contract"], "origin_contract"),
        archive_health_contract=_require_nonempty_mapping(
            payload["archive_health_contract"], "archive_health_contract"
        ),
        provenance_contract=_require_nonempty_mapping(
            payload["provenance_contract"], "provenance_contract"
        ),
        prior_evidence=_as_mapping(payload["prior_evidence"], "prior_evidence"),
        created_at_ms=_require_int(
            payload["created_at_ms"], "created_at_ms", minimum=0
        ),
    )
    if canonical_json_bytes(payload) != canonical_json_bytes(reconstructed.to_dict()):
        raise ExperimentValidationError(
            "preregistration is not the canonical strict v4 schema"
        )
    if expected is not None and canonical_json_bytes(payload) != canonical_json_bytes(
        expected.to_dict()
    ):
        raise ExperimentValidationError(
            "preregistration differs from the expected frozen artifact"
        )
    return payload


def _marker_binding(
    *,
    present: bool,
    binding: Optional[ArtifactBinding],
    field_name: str,
    expected_type: str,
) -> None:
    if present != (binding is not None):
        raise ExperimentValidationError(
            f"{field_name} presence and artifact binding disagree"
        )
    if binding is not None and (
        binding.artifact_type != expected_type
        or binding.schema_version != EXPERIMENT_SCHEMA_VERSION
    ):
        raise ExperimentValidationError(
            f"{field_name} has an unsupported artifact identity"
        )


_CALIBRATION_FAILURE_STATES = {
    "calibration_window_selection": ((False, False),),
    "calibration_attempt_freeze": ((False, False),),
    "calibration_archive": ((False, False),),
    "calibration_quality": ((False, False),),
    "calibration_pre_efficacy_provenance": ((False, False),),
    "calibration_efficacy_execution": ((True, False),),
    "calibration_efficacy_artifact_integrity": ((True, True),),
    "calibration_post_start_provenance": ((True, False), (True, True)),
}

_HOLDOUT_FAILURE_STATES = {
    "holdout_window_selection": ((False, False),),
    "preregistration_lead": ((False, False),),
    "holdout_archive": ((False, False),),
    "holdout_quality": ((False, False),),
    "holdout_pre_efficacy_provenance": ((False, False),),
    "holdout_efficacy_execution": ((True, False),),
    "holdout_efficacy_artifact_integrity": ((True, True),),
    "holdout_post_start_provenance": ((True, False), (True, True)),
}

_FAILURE_REASON_CODES = {
    "calibration_window_selection": {"candidate_days_exhausted"},
    "calibration_attempt_freeze": {"attempt_freeze_deadline_missed"},
    "calibration_archive": {
        "archive_unsealable",
        "archive_integrity_failure",
    },
    "calibration_quality": {
        "quality_gate_failure",
        "causal_integrity_failure",
        "structural_gate_infeasibility",
    },
    "calibration_pre_efficacy_provenance": {"provenance_failure"},
    "calibration_efficacy_execution": {"efficacy_execution_failure"},
    "calibration_efficacy_artifact_integrity": {
        "efficacy_artifact_integrity_failure"
    },
    "calibration_post_start_provenance": {
        "relevant_provenance_transition"
    },
    "holdout_window_selection": {"candidate_days_exhausted"},
    "preregistration_lead": {
        "preregistration_deadline_missed",
        "pushed_receipt_missing_or_late",
    },
    "holdout_archive": {
        "archive_unsealable",
        "archive_integrity_failure",
    },
    "holdout_quality": {
        "quality_gate_failure",
        "causal_integrity_failure",
        "structural_gate_infeasibility",
    },
    "holdout_pre_efficacy_provenance": {"provenance_failure"},
    "holdout_efficacy_execution": {"efficacy_execution_failure"},
    "holdout_efficacy_artifact_integrity": {
        "efficacy_artifact_integrity_failure"
    },
    "holdout_post_start_provenance": {"relevant_provenance_transition"},
}

_QUALITY_FAILURE_CODE_ORDER = (
    "archive_or_provenance_gate_failed",
    "causal_violation",
    "cohort_classification_incomplete",
    "common_scored_coverage_below_minimum",
    "decision_eligible_coverage_below_minimum",
    "quality_stage_incomplete",
    "structural_gate_infeasibility",
)
_QUALITY_FAILURE_CODES = set(_QUALITY_FAILURE_CODE_ORDER)


def _validate_quality_evidence(
    value: object,
    *,
    terminal_stage: str,
    decision: str,
    failure_stage: Optional[str],
) -> Mapping[str, Any]:
    payload = _as_mapping(value, "quality_evidence")
    if decision != "insufficient_evidence":
        expected_status = "passed"
    elif failure_stage in (
        "calibration_quality",
        "holdout_quality",
    ):
        expected_status = "failed"
    elif failure_stage in (
        "calibration_pre_efficacy_provenance",
        "calibration_efficacy_execution",
        "calibration_efficacy_artifact_integrity",
        "calibration_post_start_provenance",
        "holdout_pre_efficacy_provenance",
        "holdout_efficacy_execution",
        "holdout_efficacy_artifact_integrity",
        "holdout_post_start_provenance",
    ):
        expected_status = "passed"
    else:
        expected_status = "not_reached"
    if expected_status == "not_reached":
        _require_exact_keys(payload, ("status",), "quality_evidence")
        if payload["status"] != expected_status:
            raise ExperimentValidationError(
                "quality-evidence status is inconsistent with the result stage"
            )
        return payload
    expected_keys = (
        "status",
        "stage",
        "cells",
        "archive_health_passed",
        "provenance_passed",
        "structural_gate_infeasibility_report_binding",
        "failure_codes",
        "all_quality_gates_passed",
    )
    _require_exact_keys(payload, expected_keys, "quality_evidence")
    if payload["status"] != expected_status or payload["stage"] != terminal_stage:
        raise ExperimentValidationError(
            "quality-evidence status or stage is inconsistent"
        )
    cells = _as_sequence(payload["cells"], "quality_evidence.cells")
    all_cells = tuple(cell.cell_id for cell in V4_TIMING_CELLS)
    completed = []
    cell_gate_results = []
    common_gate_results = []
    decision_gate_results = []
    classification_results = []
    causal_results = []
    binding_fields = (
        "scheduled_vector_binding",
        "target_eligible_mask_binding",
        "target_eligible_vector_binding",
        "generation_eligible_mask_binding",
        "common_scored_mask_binding",
        "decision_eligible_mask_binding",
        "missing_reasons_binding",
        "quality_report_binding",
    )
    cell_keys = (
        "cell_id",
        "scheduled_count",
        "target_eligible_count",
        "generation_eligible_count",
        "common_scored_count",
        "decision_eligible_count",
        "cohort_classified_count",
        "causal_violation_count",
        *binding_fields,
        "common_scored_gate_passed",
        "decision_eligible_gate_passed",
        "cell_quality_passed",
    )
    binding_type_prefixes = {
        "scheduled_vector_binding": "scheduled_vector",
        "target_eligible_mask_binding": "target_eligible_mask",
        "target_eligible_vector_binding": "target_eligible_vector",
        "generation_eligible_mask_binding": "generation_eligible_mask",
        "common_scored_mask_binding": "common_scored_mask",
        "decision_eligible_mask_binding": "decision_eligible_mask",
        "missing_reasons_binding": "missing_reasons",
        "quality_report_binding": "quality_report",
    }
    for index, raw_cell in enumerate(cells):
        cell_field = f"quality_evidence.cells[{index}]"
        cell = _as_mapping(raw_cell, cell_field)
        _require_exact_keys(cell, cell_keys, cell_field)
        cell_id = _require_string(cell["cell_id"], f"{cell_field}.cell_id")
        if index >= len(all_cells) or cell_id != all_cells[index]:
            raise ExperimentValidationError(
                "completed quality cells are not a canonical prefix"
            )
        completed.append(cell_id)
        scheduled = _require_int(
            cell["scheduled_count"], f"{cell_field}.scheduled_count", minimum=0
        )
        target_eligible = _require_int(
            cell["target_eligible_count"],
            f"{cell_field}.target_eligible_count",
            minimum=0,
        )
        generation_eligible = _require_int(
            cell["generation_eligible_count"],
            f"{cell_field}.generation_eligible_count",
            minimum=0,
        )
        common_scored = _require_int(
            cell["common_scored_count"],
            f"{cell_field}.common_scored_count",
            minimum=0,
        )
        decision_eligible = _require_int(
            cell["decision_eligible_count"],
            f"{cell_field}.decision_eligible_count",
            minimum=0,
        )
        classified = _require_int(
            cell["cohort_classified_count"],
            f"{cell_field}.cohort_classified_count",
            minimum=0,
        )
        causal_violations = _require_int(
            cell["causal_violation_count"],
            f"{cell_field}.causal_violation_count",
            minimum=0,
        )
        if scheduled != 172_800 or target_eligible != 172_793:
            raise ExperimentValidationError(
                "quality-cell origin counts differ from the frozen grid"
            )
        if not (
            0 <= decision_eligible <= common_scored <= generation_eligible
            <= target_eligible
            and 0 <= classified <= target_eligible
            and 0 <= causal_violations <= target_eligible
        ):
            raise ExperimentValidationError(
                "quality-cell cohort counts are inconsistent"
            )
        for binding_field in binding_fields:
            _require_binding_identity(
                ArtifactBinding.from_dict(
                    cell[binding_field], f"{cell_field}.{binding_field}"
                ),
                (
                    f"{terminal_stage}_"
                    f"{binding_type_prefixes[binding_field]}:{cell_id}"
                ),
                f"{cell_field}.{binding_field}",
            )
        minimum = 164_154 if index == 0 else 155_514
        common_passed = common_scored >= minimum
        decision_passed = decision_eligible >= minimum
        cell_passed = (
            common_passed
            and decision_passed
            and classified == target_eligible
            and causal_violations == 0
        )
        expected_bools = (
            common_passed,
            decision_passed,
            cell_passed,
        )
        observed_bools = (
            _require_bool(
                cell["common_scored_gate_passed"],
                f"{cell_field}.common_scored_gate_passed",
            ),
            _require_bool(
                cell["decision_eligible_gate_passed"],
                f"{cell_field}.decision_eligible_gate_passed",
            ),
            _require_bool(
                cell["cell_quality_passed"],
                f"{cell_field}.cell_quality_passed",
            ),
        )
        if observed_bools != expected_bools:
            raise ExperimentValidationError(
                "quality-cell gate booleans are not derived from counts"
            )
        cell_gate_results.append(cell_passed)
        common_gate_results.append(common_passed)
        decision_gate_results.append(decision_passed)
        classification_results.append(classified == target_eligible)
        causal_results.append(causal_violations == 0)
    completed_tuple = tuple(completed)
    if completed_tuple != all_cells[: len(completed_tuple)]:
        raise ExperimentValidationError(
            "completed quality cells are not a canonical prefix"
        )
    failure_codes = tuple(
        _require_string(item, f"quality_evidence.failure_codes[{index}]")
        for index, item in enumerate(
            _as_sequence(
                payload["failure_codes"], "quality_evidence.failure_codes"
            )
        )
    )
    if len(failure_codes) != len(set(failure_codes)) or not set(
        failure_codes
    ).issubset(_QUALITY_FAILURE_CODES):
        raise ExperimentValidationError(
            "quality_evidence contains unsupported failure codes"
        )
    archive_health_passed = _require_bool(
        payload["archive_health_passed"],
        "quality_evidence.archive_health_passed",
    )
    provenance_passed = _require_bool(
        payload["provenance_passed"],
        "quality_evidence.provenance_passed",
    )
    structural_report_raw = payload[
        "structural_gate_infeasibility_report_binding"
    ]
    if structural_report_raw is not None:
        raise ExperimentValidationError(
            "structural gate infeasibility is not accepted without an "
            "independently derived feasibility proof"
        )
    structural_infeasibility = False
    derived_all_passed = (
        completed_tuple == all_cells
        and all(cell_gate_results)
        and archive_health_passed
        and provenance_passed
    )
    derived_failure_codes = []
    failure_conditions = {
        "archive_or_provenance_gate_failed": not (
            archive_health_passed and provenance_passed
        ),
        "causal_violation": not all(causal_results),
        "cohort_classification_incomplete": not all(classification_results),
        "common_scored_coverage_below_minimum": not all(common_gate_results),
        "decision_eligible_coverage_below_minimum": not all(
            decision_gate_results
        ),
        "quality_stage_incomplete": completed_tuple != all_cells,
        "structural_gate_infeasibility": structural_infeasibility,
    }
    for code in _QUALITY_FAILURE_CODE_ORDER:
        if failure_conditions[code]:
            derived_failure_codes.append(code)
    if failure_codes != tuple(derived_failure_codes):
        raise ExperimentValidationError(
            "quality failure codes are not derived from the evidence"
        )
    all_passed = _require_bool(
        payload["all_quality_gates_passed"],
        "quality_evidence.all_quality_gates_passed",
    )
    if all_passed != derived_all_passed:
        raise ExperimentValidationError(
            "all_quality_gates_passed is not derived from quality evidence"
        )
    if expected_status == "passed":
        if completed_tuple != all_cells or failure_codes or not all_passed:
            raise ExperimentValidationError(
                "passed quality evidence is incomplete"
            )
    elif not failure_codes or all_passed:
        raise ExperimentValidationError(
            "failed quality evidence requires exact failure codes"
        )
    return payload


def _metric_decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return _require_decimal(value, field_name)
    return _parse_decimal_string(value, field_name)


_ROBUSTNESS_CELL_IDS = tuple(cell.cell_id for cell in V4_TIMING_CELLS[1:])
_HOLDOUT_GATE_KEYS = (
    "challenger_canonical_mae_skill",
    "mae_skill_improvement_vs_control",
    "improvement_bootstrap_lower_bound",
    "challenger_canonical_rmse_skill",
    "rmse_skill_improvement_vs_control",
    "challenger_mae_skill_all_robustness_cells",
    "robustness_improvement_all_cells",
    "no_rerank_or_runner_up_fallback",
)


def _validate_decimal_by_robustness_cell(
    value: object, field_name: str
) -> Mapping[str, Decimal]:
    payload = _as_mapping(value, field_name)
    _require_exact_keys(payload, _ROBUSTNESS_CELL_IDS, field_name)
    return {
        cell_id: _metric_decimal(payload[cell_id], f"{field_name}.{cell_id}")
        for cell_id in _ROBUSTNESS_CELL_IDS
    }


def _validate_holdout_efficacy_evidence(
    value: object, *, decision: str, preregistration_sha256: str
) -> Mapping[str, Any]:
    field_name = "efficacy_evidence"
    payload = _as_mapping(value, field_name)
    expected_keys = (
        "challenger_canonical_mae_skill",
        "control_canonical_mae_skill",
        "mae_skill_improvement_vs_control",
        "improvement_bootstrap_lower_bound",
        "challenger_canonical_rmse_skill",
        "control_canonical_rmse_skill",
        "rmse_skill_improvement_vs_control",
        "challenger_mae_skill_by_robustness_cell",
        "control_mae_skill_by_robustness_cell",
        "mae_skill_improvement_vs_control_by_robustness_cell",
        "bootstrap_seed_sha256",
        "bootstrap_seed_int",
        "bootstrap_contract_digest",
        "bootstrap_replicate_count",
        "bootstrap_defined_replicate_count",
        "gate_results",
        "all_gates_passed",
    )
    _require_exact_keys(payload, expected_keys, field_name)
    challenger_mae = _metric_decimal(
        payload["challenger_canonical_mae_skill"],
        f"{field_name}.challenger_canonical_mae_skill",
    )
    control_mae = _metric_decimal(
        payload["control_canonical_mae_skill"],
        f"{field_name}.control_canonical_mae_skill",
    )
    mae_improvement = _metric_decimal(
        payload["mae_skill_improvement_vs_control"],
        f"{field_name}.mae_skill_improvement_vs_control",
    )
    bootstrap_lower = _metric_decimal(
        payload["improvement_bootstrap_lower_bound"],
        f"{field_name}.improvement_bootstrap_lower_bound",
    )
    challenger_rmse = _metric_decimal(
        payload["challenger_canonical_rmse_skill"],
        f"{field_name}.challenger_canonical_rmse_skill",
    )
    control_rmse = _metric_decimal(
        payload["control_canonical_rmse_skill"],
        f"{field_name}.control_canonical_rmse_skill",
    )
    rmse_improvement = _metric_decimal(
        payload["rmse_skill_improvement_vs_control"],
        f"{field_name}.rmse_skill_improvement_vs_control",
    )
    robustness_skills = _validate_decimal_by_robustness_cell(
        payload["challenger_mae_skill_by_robustness_cell"],
        f"{field_name}.challenger_mae_skill_by_robustness_cell",
    )
    control_robustness_skills = _validate_decimal_by_robustness_cell(
        payload["control_mae_skill_by_robustness_cell"],
        f"{field_name}.control_mae_skill_by_robustness_cell",
    )
    robustness_improvements = _validate_decimal_by_robustness_cell(
        payload["mae_skill_improvement_vs_control_by_robustness_cell"],
        f"{field_name}.mae_skill_improvement_vs_control_by_robustness_cell",
    )
    if mae_improvement != challenger_mae - control_mae:
        raise ExperimentValidationError(
            "canonical MAE-skill improvement is not derived from both models"
        )
    if rmse_improvement != challenger_rmse - control_rmse:
        raise ExperimentValidationError(
            "canonical RMSE-skill improvement is not derived from both models"
        )
    if any(
        robustness_improvements[cell_id]
        != robustness_skills[cell_id] - control_robustness_skills[cell_id]
        for cell_id in _ROBUSTNESS_CELL_IDS
    ):
        raise ExperimentValidationError(
            "robustness improvements are not derived from both models"
        )
    bootstrap_seed_sha256 = _require_sha256(
        payload["bootstrap_seed_sha256"],
        f"{field_name}.bootstrap_seed_sha256",
    )
    if bootstrap_seed_sha256 != preregistration_sha256:
        raise ExperimentValidationError(
            "bootstrap seed does not derive from the bound preregistration"
        )
    bootstrap_seed_int = _require_string(
        payload["bootstrap_seed_int"], f"{field_name}.bootstrap_seed_int"
    )
    if (
        not bootstrap_seed_int.isdigit()
        or str(int(bootstrap_seed_int)) != bootstrap_seed_int
        or int(bootstrap_seed_int) != int(bootstrap_seed_sha256, 16)
    ):
        raise ExperimentValidationError(
            "bootstrap seed integer is not derived from its SHA-256"
        )
    expected_bootstrap_digest = canonical_sha256(FROZEN_BOOTSTRAP_CONTRACT)
    if payload["bootstrap_contract_digest"] != expected_bootstrap_digest:
        raise ExperimentValidationError("bootstrap contract digest differs")
    for count_field in (
        "bootstrap_replicate_count",
        "bootstrap_defined_replicate_count",
    ):
        if _require_int(
            payload[count_field], f"{field_name}.{count_field}", minimum=0
        ) != 10_000:
            raise ExperimentValidationError(
                "bootstrap replicate counts must both equal 10000"
            )
    expected_gate_results = {
        "challenger_canonical_mae_skill": challenger_mae >= Decimal("0.05"),
        "mae_skill_improvement_vs_control": (
            mae_improvement >= Decimal("0.02")
        ),
        "improvement_bootstrap_lower_bound": bootstrap_lower > 0,
        "challenger_canonical_rmse_skill": challenger_rmse > 0,
        "rmse_skill_improvement_vs_control": rmse_improvement >= 0,
        "challenger_mae_skill_all_robustness_cells": all(
            value > 0 for value in robustness_skills.values()
        ),
        "robustness_improvement_all_cells": all(
            value >= Decimal("-0.01")
            for value in robustness_improvements.values()
        ),
        "no_rerank_or_runner_up_fallback": True,
    }
    gate_results = _as_mapping(payload["gate_results"], f"{field_name}.gate_results")
    _require_exact_keys(gate_results, _HOLDOUT_GATE_KEYS, f"{field_name}.gate_results")
    for gate_name, expected_value in expected_gate_results.items():
        if _require_bool(
            gate_results[gate_name],
            f"{field_name}.gate_results.{gate_name}",
        ) != expected_value:
            raise ExperimentValidationError(
                f"holdout gate {gate_name} is not derived from its inputs"
            )
    all_passed = all(expected_gate_results.values())
    if _require_bool(
        payload["all_gates_passed"], f"{field_name}.all_gates_passed"
    ) != all_passed:
        raise ExperimentValidationError(
            "all_gates_passed is not derived from the holdout gates"
        )
    if (decision == "promotion_eligible") != all_passed:
        raise ExperimentValidationError(
            "holdout decision is inconsistent with its conjunctive gates"
        )
    return payload


_CALIBRATION_GATE_KEYS = (
    "winner_canonical_mae_skill",
    "winner_canonical_rmse_skill",
    "mae_skill_lead_over_runner_up",
    "relative_robustness_all_cells",
    "winner_promotion_eligible",
    "unique_best",
)


_COMPARISON_LAG_KEYS = tuple(str(lag) for lag in COMPARISON_LAGS_MS)


def _validate_decimal_by_lag(
    value: object, field_name: str
) -> Mapping[int, Decimal]:
    payload = _as_mapping(value, field_name)
    _require_exact_keys(payload, _COMPARISON_LAG_KEYS, field_name)
    return {
        int(lag): _metric_decimal(payload[lag], f"{field_name}.{lag}")
        for lag in _COMPARISON_LAG_KEYS
    }


def _validate_calibration_efficacy_evidence(
    value: object, *, require_qualifying_shorter: bool = False
) -> Mapping[str, Any]:
    field_name = "efficacy_evidence"
    payload = _as_mapping(value, field_name)
    expected_keys = (
        "ranking_metric",
        "candidate_canonical_mae_skill_by_lag",
        "candidate_canonical_rmse_skill_by_lag",
        "candidate_mae_skill_by_robustness_cell",
        "ordered_candidate_lags_ms",
        "winner_lag_ms",
        "runner_up_lag_ms",
        "winner_canonical_mae_skill",
        "winner_canonical_rmse_skill",
        "mae_skill_lead_over_runner_up",
        "winner_relative_deficit_by_robustness_cell",
        "winner_promotion_eligible",
        "boundary_winner",
        "unique_best",
        "gate_results",
        "all_gates_passed",
    )
    _require_exact_keys(payload, expected_keys, field_name)
    if payload["ranking_metric"] != (
        "mae_skill_vs_horizon_matched_no_change_baseline"
    ):
        raise ExperimentValidationError("calibration ranking metric changed")
    candidate_mae = _validate_decimal_by_lag(
        payload["candidate_canonical_mae_skill_by_lag"],
        f"{field_name}.candidate_canonical_mae_skill_by_lag",
    )
    candidate_rmse = _validate_decimal_by_lag(
        payload["candidate_canonical_rmse_skill_by_lag"],
        f"{field_name}.candidate_canonical_rmse_skill_by_lag",
    )
    robustness_payload = _as_mapping(
        payload["candidate_mae_skill_by_robustness_cell"],
        f"{field_name}.candidate_mae_skill_by_robustness_cell",
    )
    _require_exact_keys(
        robustness_payload,
        _ROBUSTNESS_CELL_IDS,
        f"{field_name}.candidate_mae_skill_by_robustness_cell",
    )
    candidate_robustness = {
        cell_id: _validate_decimal_by_lag(
            robustness_payload[cell_id],
            f"{field_name}.candidate_mae_skill_by_robustness_cell.{cell_id}",
        )
        for cell_id in _ROBUSTNESS_CELL_IDS
    }
    ordered_lags = tuple(
        _require_int(item, f"{field_name}.ordered_candidate_lags_ms[{index}]")
        for index, item in enumerate(
            _as_sequence(
                payload["ordered_candidate_lags_ms"],
                f"{field_name}.ordered_candidate_lags_ms",
            )
        )
    )
    expected_order = tuple(
        sorted(
            COMPARISON_LAGS_MS,
            key=lambda lag: (-candidate_mae[lag], COMPARISON_LAGS_MS.index(lag)),
        )
    )
    if ordered_lags != expected_order:
        raise ExperimentValidationError(
            "calibration ranking is not derived from canonical MAE skill"
        )
    winner_lag = _require_int(payload["winner_lag_ms"], f"{field_name}.winner_lag_ms")
    runner_up_lag = _require_int(
        payload["runner_up_lag_ms"], f"{field_name}.runner_up_lag_ms"
    )
    if ordered_lags[:2] != (winner_lag, runner_up_lag):
        raise ExperimentValidationError(
            "calibration winner and runner-up do not match the ranking"
        )
    winner_mae = _metric_decimal(
        payload["winner_canonical_mae_skill"],
        f"{field_name}.winner_canonical_mae_skill",
    )
    winner_rmse = _metric_decimal(
        payload["winner_canonical_rmse_skill"],
        f"{field_name}.winner_canonical_rmse_skill",
    )
    lead = _metric_decimal(
        payload["mae_skill_lead_over_runner_up"],
        f"{field_name}.mae_skill_lead_over_runner_up",
    )
    robustness_deficits = _validate_decimal_by_robustness_cell(
        payload["winner_relative_deficit_by_robustness_cell"],
        f"{field_name}.winner_relative_deficit_by_robustness_cell",
    )
    expected_lead = candidate_mae[winner_lag] - candidate_mae[runner_up_lag]
    expected_deficits = {
        cell_id: max(values.values()) - values[winner_lag]
        for cell_id, values in candidate_robustness.items()
    }
    if (
        winner_mae != candidate_mae[winner_lag]
        or winner_rmse != candidate_rmse[winner_lag]
        or lead != expected_lead
        or canonical_json_bytes(robustness_deficits)
        != canonical_json_bytes(expected_deficits)
    ):
        raise ExperimentValidationError(
            "calibration winner summaries are not derived from family metrics"
        )
    promotion_eligible = winner_lag in PROMOTION_ELIGIBLE_LAGS_MS
    boundary_winner = winner_lag == min(PROMOTION_ELIGIBLE_LAGS_MS)
    unique_best = expected_lead > 0
    if _require_bool(
        payload["winner_promotion_eligible"],
        f"{field_name}.winner_promotion_eligible",
    ) != promotion_eligible:
        raise ExperimentValidationError("winner promotion eligibility is inconsistent")
    if _require_bool(
        payload["boundary_winner"], f"{field_name}.boundary_winner"
    ) != boundary_winner:
        raise ExperimentValidationError("boundary_winner is inconsistent")
    if _require_bool(payload["unique_best"], f"{field_name}.unique_best") != unique_best:
        raise ExperimentValidationError("unique_best is inconsistent")
    expected_gate_results = {
        "winner_canonical_mae_skill": winner_mae >= Decimal("0.05"),
        "winner_canonical_rmse_skill": winner_rmse > 0,
        "mae_skill_lead_over_runner_up": lead >= Decimal("0.01"),
        "relative_robustness_all_cells": all(
            value <= Decimal("0.01") for value in robustness_deficits.values()
        ),
        "winner_promotion_eligible": promotion_eligible,
        "unique_best": unique_best,
    }
    gate_results = _as_mapping(payload["gate_results"], f"{field_name}.gate_results")
    _require_exact_keys(
        gate_results, _CALIBRATION_GATE_KEYS, f"{field_name}.gate_results"
    )
    for gate_name, expected_value in expected_gate_results.items():
        if _require_bool(
            gate_results[gate_name],
            f"{field_name}.gate_results.{gate_name}",
        ) != expected_value:
            raise ExperimentValidationError(
                f"calibration gate {gate_name} is not derived from its inputs"
            )
    all_passed = all(expected_gate_results.values())
    if _require_bool(
        payload["all_gates_passed"], f"{field_name}.all_gates_passed"
    ) != all_passed:
        raise ExperimentValidationError(
            "all_gates_passed is not derived from the calibration gates"
        )
    if require_qualifying_shorter and not all_passed:
        raise ExperimentValidationError(
            "calibration selection report requires one qualifying shorter "
            "winner"
        )
    if all_passed and not require_qualifying_shorter:
        raise ExperimentValidationError(
            "a qualifying shorter calibration winner must continue to holdout"
        )
    return payload


_RETRY_STATE_KEYS = (
    "calibration_successors_used",
    "holdout_successors_used",
    "calibration_successors_remaining",
    "holdout_successors_remaining",
    "successor_allowed",
    "retries_exhausted",
    "lineage_closed",
)


def _validate_retry_state(
    value: object,
    *,
    attempt: AttemptIdentity,
    terminal_stage: str,
    decision: str,
    failure_stage: Optional[str],
    structural_gate_infeasibility: bool,
    stage_started: bool,
) -> Mapping[str, Any]:
    payload = _as_mapping(value, "retry_state")
    _require_exact_keys(payload, _RETRY_STATE_KEYS, "retry_state")
    calibration_used = _require_int(
        payload["calibration_successors_used"],
        "retry_state.calibration_successors_used",
        minimum=0,
    )
    holdout_used = _require_int(
        payload["holdout_successors_used"],
        "retry_state.holdout_successors_used",
        minimum=0,
    )
    calibration_remaining = _require_int(
        payload["calibration_successors_remaining"],
        "retry_state.calibration_successors_remaining",
        minimum=0,
    )
    holdout_remaining = _require_int(
        payload["holdout_successors_remaining"],
        "retry_state.holdout_successors_remaining",
        minimum=0,
    )
    successor_allowed = _require_bool(
        payload["successor_allowed"], "retry_state.successor_allowed"
    )
    retries_exhausted = _require_bool(
        payload["retries_exhausted"], "retry_state.retries_exhausted"
    )
    lineage_closed = _require_bool(
        payload["lineage_closed"], "retry_state.lineage_closed"
    )
    expected_calibration_used = attempt.calibration_attempt_index
    expected_holdout_used = attempt.holdout_attempt_index or 0
    if (
        calibration_used != expected_calibration_used
        or holdout_used != expected_holdout_used
    ):
        raise ExperimentValidationError(
            "retry-state usage does not match the attempt indexes"
        )
    retryable_failure_stages = {
        "calibration": {
            "calibration_window_selection",
            "calibration_attempt_freeze",
            "calibration_archive",
            "calibration_quality",
        },
        "holdout": {
            "holdout_window_selection",
            "preregistration_lead",
            "holdout_archive",
            "holdout_quality",
        },
    }
    retryable = (
        decision == "insufficient_evidence"
        and not stage_started
        and failure_stage in retryable_failure_stages[terminal_stage]
        and not structural_gate_infeasibility
    )
    if retryable:
        current_index = (
            attempt.calibration_attempt_index
            if terminal_stage == "calibration"
            else expected_holdout_used
        )
        successor_available = current_index == 0
    else:
        successor_available = False
    expected_calibration_remaining = (
        1
        if successor_available and terminal_stage == "calibration"
        else 0
    )
    expected_holdout_remaining = (
        1 if successor_available and terminal_stage == "holdout" else 0
    )
    expected_exhausted = retryable and not successor_available
    expected_closed = not successor_available
    expected = (
        expected_calibration_remaining,
        expected_holdout_remaining,
        successor_available,
        expected_exhausted,
        expected_closed,
    )
    observed = (
        calibration_remaining,
        holdout_remaining,
        successor_allowed,
        retries_exhausted,
        lineage_closed,
    )
    if observed != expected:
        raise ExperimentValidationError(
            "retry_state is inconsistent with the terminal state"
        )
    return payload


def _validate_result_ancestry(
    *,
    attempt: AttemptIdentity,
    parent_result: Optional[ArtifactBinding],
    ancestry: Sequence[ArtifactBinding],
    selection_anchor_provenance: Optional[SelectionAnchorProvenance],
) -> None:
    expected_types = []
    if attempt.calibration_attempt_index == 1:
        expected_types.extend(
            (
                TERMINAL_RESULT_ARTIFACT_TYPE,
                "calibration_retry_eligibility",
                "calibration_successor_authorization",
            )
        )
    if attempt.holdout_attempt_index == 1:
        expected_types.extend(
            (
                TERMINAL_RESULT_ARTIFACT_TYPE,
                "holdout_retry_eligibility",
                "holdout_successor_authorization",
            )
        )
    if not expected_types:
        if parent_result is not None or ancestry:
            raise ExperimentValidationError(
                "an initial attempt cannot claim successor ancestry"
            )
        return
    parent = _require_binding_identity(
        parent_result, TERMINAL_RESULT_ARTIFACT_TYPE, "parent_result"
    )
    if tuple(item.artifact_type for item in ancestry) != tuple(expected_types):
        raise ExperimentValidationError(
            "successor ancestry does not bind its complete parent, "
            "eligibility, and authorization chain"
        )
    if any(item.schema_version != EXPERIMENT_SCHEMA_VERSION for item in ancestry):
        raise ExperimentValidationError("successor ancestry schema is unsupported")
    if len({item.sha256 for item in ancestry}) != len(ancestry):
        raise ExperimentValidationError("successor ancestry contains duplicates")
    current_parent_index = -3
    if ancestry[current_parent_index] != parent:
        raise ExperimentValidationError(
            "parent_result differs from the ancestry parent"
        )
    if attempt.holdout_attempt_index == 1 and (
        not isinstance(selection_anchor_provenance, SelectionAnchorProvenance)
        or ancestry[-2] != selection_anchor_provenance.source_artifact
        or ancestry[-1] != selection_anchor_provenance.authorization_artifact
    ):
        raise ExperimentValidationError(
            "holdout successor ancestry differs from its selection anchor"
        )


def _require_inventory_binding(
    inventory: Sequence[ArtifactBinding],
    binding: Optional[ArtifactBinding],
    field_name: str,
) -> None:
    if binding is not None and binding not in inventory:
        raise ExperimentValidationError(
            f"{field_name} is absent from evidence_artifacts"
        )


def terminal_efficacy_report_payload(
    *,
    attempt: AttemptIdentity,
    experiment_contract_digest: str,
    terminal_stage: str,
    decision: str,
    efficacy_evidence: Mapping[str, Any],
    efficacy_ledger: ArtifactBinding,
    bootstrap_report: Optional[ArtifactBinding] = None,
) -> dict[str, Any]:
    """Build the canonical report body whose hash a performance result binds."""

    if terminal_stage not in ("calibration", "holdout"):
        raise ExperimentValidationError("efficacy report stage is unsupported")
    if decision not in ("retain_incumbent", "promotion_eligible"):
        raise ExperimentValidationError(
            "efficacy report requires a performance decision"
        )
    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    _require_sha256(
        experiment_contract_digest, "experiment_contract_digest"
    )
    evidence = _require_nonempty_mapping(
        efficacy_evidence, "efficacy_evidence"
    )
    _require_binding_identity(
        efficacy_ledger,
        f"{terminal_stage}_efficacy_ledger",
        "efficacy_ledger",
    )
    supporting_artifacts = {
        "efficacy_ledger_sha256": efficacy_ledger.sha256,
    }
    if terminal_stage == "holdout":
        _require_binding_identity(
            bootstrap_report,
            "holdout_bootstrap_report",
            "bootstrap_report",
        )
        supporting_artifacts["bootstrap_report_sha256"] = (
            bootstrap_report.sha256
        )
    elif bootstrap_report is not None:
        raise ExperimentValidationError(
            "calibration efficacy report cannot bind a bootstrap report"
        )
    payload = {
        "artifact_type": f"{terminal_stage}_efficacy_report",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "report_kind": "terminal_decision",
        "attempt": attempt.to_dict(),
        "experiment_contract_digest": experiment_contract_digest,
        "terminal_stage": terminal_stage,
        "decision": decision,
        "supporting_artifacts": supporting_artifacts,
        "efficacy_evidence": _json_ready(evidence),
    }
    canonical_json_bytes(payload)
    return payload


def _stage_attempt_scope(
    attempt: AttemptIdentity, terminal_stage: str
) -> dict[str, Any]:
    if terminal_stage == "calibration":
        stage_attempt_index = attempt.calibration_attempt_index
    elif terminal_stage == "holdout":
        if attempt.holdout_attempt_index not in (0, 1):
            raise ExperimentValidationError(
                "holdout artifact scope requires a holdout attempt index"
            )
        stage_attempt_index = attempt.holdout_attempt_index
    else:
        raise ExperimentValidationError("artifact stage is unsupported")
    return {
        **attempt.to_dict(),
        "stage_attempt_index": stage_attempt_index,
    }


def _preregistration_sidecar_sha256(preregistration_sha256: str) -> str:
    digest_value = _require_sha256(
        preregistration_sha256, "preregistration_sha256"
    )
    return hashlib.sha256(f"{digest_value}\n".encode("ascii")).hexdigest()


def pushed_preregistration_receipt_payload(
    *,
    attempt: AttemptIdentity,
    preregistration: ArtifactBinding,
    authoritative_remote_url_sha256: str,
    pushed_commit_id: str,
    observed_remote_ref: str,
    observed_remote_commit_id: str,
    verified_at_ms: int,
) -> dict[str, Any]:
    """Build the canonical create-once receipt for the pushed preregistration."""

    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    if attempt.holdout_attempt_index not in (0, 1):
        raise ExperimentValidationError(
            "pushed receipt requires a holdout attempt"
        )
    _require_binding_identity(
        preregistration,
        PREREGISTRATION_ARTIFACT_TYPE,
        "preregistration",
    )
    remote_ref = _require_string(observed_remote_ref, "observed_remote_ref")
    if not remote_ref.startswith("refs/heads/"):
        raise ExperimentValidationError(
            "observed remote ref must be a full branch ref"
        )
    payload = {
        "artifact_type": "holdout_pushed_preregistration_receipt",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "attempt": attempt.to_dict(),
        "preregistration_sha256": preregistration.sha256,
        "preregistration_sidecar_sha256": (
            _preregistration_sidecar_sha256(preregistration.sha256)
        ),
        "authoritative_remote_url_sha256": _require_sha256(
            authoritative_remote_url_sha256,
            "authoritative_remote_url_sha256",
        ),
        "pushed_commit_id": _require_git_object_id(
            pushed_commit_id, "pushed_commit_id"
        ),
        "observed_remote_ref": remote_ref,
        "observed_remote_commit_id": _require_git_object_id(
            observed_remote_commit_id, "observed_remote_commit_id"
        ),
        "verified_at_ms": _require_int(
            verified_at_ms, "verified_at_ms", minimum=0
        ),
    }
    canonical_json_bytes(payload)
    return payload


def receipt_deadline_check_payload(
    *,
    attempt: AttemptIdentity,
    preregistration: ArtifactBinding,
    authoritative_remote_url_sha256: str,
    expected_remote_ref: str,
    pushed_receipt_deadline_ms: int,
    checked_at_ms: int,
    pushed_receipt: Optional[ArtifactBinding],
    pushed_receipt_payload_value: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build the canonical receipt-presence and timeliness observation."""

    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    _require_binding_identity(
        preregistration,
        PREREGISTRATION_ARTIFACT_TYPE,
        "preregistration",
    )
    expected_ref = _require_string(expected_remote_ref, "expected_remote_ref")
    if not expected_ref.startswith("refs/heads/"):
        raise ExperimentValidationError(
            "expected remote ref must be a full branch ref"
        )
    deadline_ms = _require_int(
        pushed_receipt_deadline_ms,
        "pushed_receipt_deadline_ms",
        minimum=0,
    )
    check_ms = _require_int(checked_at_ms, "checked_at_ms", minimum=0)
    if pushed_receipt is None:
        if pushed_receipt_payload_value is not None:
            raise ExperimentValidationError(
                "receipt check cannot receive unbound receipt contents"
            )
        observed_ref = None
        expected_commit = None
        observed_commit = None
        remote_commit_matches_expected = False
        receipt_verified_at_ms = None
        receipt_present = False
        receipt_timely = False
        bound_receipt = None
        if check_ms <= deadline_ms:
            raise ExperimentValidationError(
                "missing receipt cannot be concluded before its deadline"
            )
    else:
        _require_binding_identity(
            pushed_receipt,
            "holdout_pushed_preregistration_receipt",
            "pushed_receipt",
        )
        receipt_payload = _as_mapping(
            pushed_receipt_payload_value,
            "pushed_receipt_payload_value",
        )
        observed_ref = _require_string(
            receipt_payload.get("observed_remote_ref"),
            "pushed_receipt.observed_remote_ref",
        )
        expected_commit = _require_git_object_id(
            receipt_payload.get("pushed_commit_id"),
            "pushed_receipt.pushed_commit_id",
        )
        observed_commit = _require_git_object_id(
            receipt_payload.get("observed_remote_commit_id"),
            "pushed_receipt.observed_remote_commit_id",
        )
        receipt_verified_at_ms = _require_int(
            receipt_payload.get("verified_at_ms"),
            "pushed_receipt.verified_at_ms",
            minimum=0,
        )
        if check_ms < receipt_verified_at_ms:
            raise ExperimentValidationError(
                "receipt deadline check predates receipt verification"
            )
        receipt_present = True
        remote_commit_matches_expected = observed_commit == expected_commit
        receipt_timely = (
            receipt_verified_at_ms <= deadline_ms
            and observed_ref == expected_ref
            and remote_commit_matches_expected
        )
        bound_receipt = pushed_receipt.to_dict()
    payload = {
        "artifact_type": "holdout_receipt_deadline_check",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "attempt": attempt.to_dict(),
        "preregistration_sha256": preregistration.sha256,
        "authoritative_remote_url_sha256": _require_sha256(
            authoritative_remote_url_sha256,
            "authoritative_remote_url_sha256",
        ),
        "expected_remote_ref": expected_ref,
        "observed_remote_ref": observed_ref,
        "expected_remote_commit_id": expected_commit,
        "observed_remote_commit_id": observed_commit,
        "remote_commit_matches_expected": remote_commit_matches_expected,
        "pushed_receipt": bound_receipt,
        "receipt_verified_at_ms": receipt_verified_at_ms,
        "pushed_receipt_deadline_ms": deadline_ms,
        "checked_at_ms": check_ms,
        "receipt_present": receipt_present,
        "receipt_timely": receipt_timely,
    }
    canonical_json_bytes(payload)
    return payload


def preregistration_deadline_check_payload(
    *,
    attempt: AttemptIdentity,
    preregistration_publication_deadline_ms: int,
    checked_at_ms: int,
) -> dict[str, Any]:
    """Build the explicit absence check for a missed preregistration publish."""

    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    deadline_ms = _require_int(
        preregistration_publication_deadline_ms,
        "preregistration_publication_deadline_ms",
        minimum=0,
    )
    check_ms = _require_int(checked_at_ms, "checked_at_ms", minimum=0)
    if check_ms <= deadline_ms:
        raise ExperimentValidationError(
            "preregistration absence cannot be concluded before its deadline"
        )
    payload = {
        "artifact_type": "holdout_preregistration_deadline_check",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "attempt": attempt.to_dict(),
        "preregistration_publication_deadline_ms": deadline_ms,
        "checked_at_ms": check_ms,
        "preregistration_present": False,
    }
    canonical_json_bytes(payload)
    return payload


def calibration_selection_report_payload(
    *,
    attempt: AttemptIdentity,
    experiment_contract_digest: str,
    frozen_challenger: ModelIdentity,
    efficacy_ledger: ArtifactBinding,
    efficacy_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the successful-calibration report that freezes one challenger."""

    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    _require_sha256(
        experiment_contract_digest, "experiment_contract_digest"
    )
    if not isinstance(frozen_challenger, ModelIdentity):
        raise TypeError("frozen_challenger must be ModelIdentity")
    version_match = _MODEL_VERSION_PATTERN.fullmatch(
        frozen_challenger.model_version
    )
    winner_lag_ms = (
        int(version_match.group(1)) if version_match is not None else None
    )
    if frozen_challenger.model_role != "v4_candidate" or (
        winner_lag_ms not in PROMOTION_ELIGIBLE_LAGS_MS
    ):
        raise ExperimentValidationError(
            "calibration selection report requires an eligible shorter winner"
        )
    _require_binding_identity(
        efficacy_ledger,
        "calibration_efficacy_ledger",
        "efficacy_ledger",
    )
    validated_evidence = _validate_calibration_efficacy_evidence(
        efficacy_evidence,
        require_qualifying_shorter=True,
    )
    evidence_winner_lag_ms = _require_int(
        validated_evidence.get("winner_lag_ms"),
        "efficacy_evidence.winner_lag_ms",
    )
    if evidence_winner_lag_ms != winner_lag_ms:
        raise ExperimentValidationError(
            "frozen challenger differs from the derived calibration winner"
        )
    payload = {
        "artifact_type": "calibration_efficacy_report",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "report_kind": "eligible_challenger_selection",
        "attempt_scope": _stage_attempt_scope(attempt, "calibration"),
        "experiment_contract_digest": experiment_contract_digest,
        "efficacy_ledger": efficacy_ledger.to_dict(),
        "frozen_challenger": frozen_challenger.to_dict(),
        "winner_lag_ms": winner_lag_ms,
        "winner_promotion_eligible": True,
        "all_selection_gates_passed": True,
        "selection_gate_contract_digest": canonical_sha256(
            FROZEN_CALIBRATION_GATES
        ),
        "efficacy_evidence": _json_ready(validated_evidence),
    }
    canonical_json_bytes(payload)
    return payload


def efficacy_completion_marker_payload(
    *,
    attempt: AttemptIdentity,
    experiment_contract_digest: str,
    terminal_stage: str,
    efficacy_start_marker: ArtifactBinding,
    prerequisite_artifacts: Sequence[ArtifactBinding],
    efficacy_report: ArtifactBinding,
    immutable_efficacy_artifacts: Sequence[ArtifactBinding],
    completed_at_ms: int,
) -> dict[str, Any]:
    """Build a completion marker bound to its immutable efficacy report."""

    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    _require_sha256(
        experiment_contract_digest, "experiment_contract_digest"
    )
    _require_binding_identity(
        efficacy_start_marker,
        f"{terminal_stage}_efficacy_started",
        "efficacy_start_marker",
    )
    _require_binding_identity(
        efficacy_report,
        f"{terminal_stage}_efficacy_report",
        "efficacy_report",
    )
    try:
        prerequisite_inventory = tuple(prerequisite_artifacts)
    except TypeError as exc:
        raise ExperimentValidationError(
            "prerequisite_artifacts must be an array"
        ) from exc
    expected_prerequisite_types = (
        (
            "calibration_attempt_freeze",
            "calibration_raw_manifest",
            "calibration_pre_efficacy_provenance_gate",
        )
        if terminal_stage == "calibration"
        else (
            PREREGISTRATION_ARTIFACT_TYPE,
            "holdout_pushed_preregistration_receipt",
            "holdout_receipt_deadline_check",
            "holdout_raw_manifest",
            "holdout_pre_efficacy_provenance_gate",
        )
    )
    if (
        len(prerequisite_inventory) != len(expected_prerequisite_types)
        or not all(
            isinstance(artifact, ArtifactBinding)
            for artifact in prerequisite_inventory
        )
        or tuple(
            artifact.artifact_type for artifact in prerequisite_inventory
        )
        != expected_prerequisite_types
        or any(
            artifact.schema_version != EXPERIMENT_SCHEMA_VERSION
            for artifact in prerequisite_inventory
        )
    ):
        raise ExperimentValidationError(
            "completion marker prerequisite inventory is incomplete or out "
            "of order"
        )
    try:
        immutable_inventory = tuple(immutable_efficacy_artifacts)
    except TypeError as exc:
        raise ExperimentValidationError(
            "immutable_efficacy_artifacts must be an array"
        ) from exc
    expected_types = (
        (
            "calibration_efficacy_started",
            "calibration_efficacy_ledger",
            "calibration_efficacy_report",
        )
        if terminal_stage == "calibration"
        else (
            "holdout_efficacy_started",
            "holdout_efficacy_ledger",
            "holdout_bootstrap_report",
            "holdout_efficacy_report",
        )
    )
    if (
        tuple(
            artifact.artifact_type
            for artifact in immutable_inventory
            if isinstance(artifact, ArtifactBinding)
        )
        != expected_types
        or len(immutable_inventory) != len(expected_types)
        or any(
            artifact.schema_version != EXPERIMENT_SCHEMA_VERSION
            for artifact in immutable_inventory
            if isinstance(artifact, ArtifactBinding)
        )
        or immutable_inventory[0] != efficacy_start_marker
        or immutable_inventory[-1] != efficacy_report
    ):
        raise ExperimentValidationError(
            "completion marker immutable efficacy inventory is incomplete or "
            "out of order"
        )
    _require_int(completed_at_ms, "completed_at_ms", minimum=0)
    payload = {
        "artifact_type": f"{terminal_stage}_efficacy_completed",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "attempt_scope": _stage_attempt_scope(attempt, terminal_stage),
        "experiment_contract_digest": experiment_contract_digest,
        "terminal_stage": terminal_stage,
        "efficacy_start_marker": efficacy_start_marker.to_dict(),
        "prerequisite_artifacts": [
            artifact.to_dict() for artifact in prerequisite_inventory
        ],
        "immutable_efficacy_artifacts": [
            artifact.to_dict() for artifact in immutable_inventory
        ],
        "efficacy_report": efficacy_report.to_dict(),
        "completed_at_ms": completed_at_ms,
    }
    canonical_json_bytes(payload)
    return payload


def _structural_gate_infeasibility_report_payload(
    *,
    attempt: AttemptIdentity,
    experiment_contract_digest: str,
    terminal_stage: str,
    quality_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the canonical proof record that makes a quality failure terminal."""

    if terminal_stage not in ("calibration", "holdout"):
        raise ExperimentValidationError(
            "structural infeasibility report stage is unsupported"
        )
    if not isinstance(attempt, AttemptIdentity):
        raise TypeError("attempt must be AttemptIdentity")
    _require_sha256(
        experiment_contract_digest, "experiment_contract_digest"
    )
    quality = _as_mapping(quality_evidence, "quality_evidence")
    cells = _as_sequence(quality.get("cells"), "quality_evidence.cells")
    quality_report_bindings = []
    failed_frozen_coverage_gates = []
    for index, expected_cell in enumerate(V4_TIMING_CELLS):
        if index >= len(cells):
            raise ExperimentValidationError(
                "structural infeasibility report requires all seven quality cells"
            )
        cell = _as_mapping(
            cells[index], f"quality_evidence.cells[{index}]"
        )
        if cell.get("cell_id") != expected_cell.cell_id:
            raise ExperimentValidationError(
                "structural infeasibility quality cells are not canonical"
            )
        report_binding = ArtifactBinding.from_dict(
            cell.get("quality_report_binding"),
            f"quality_evidence.cells[{index}].quality_report_binding",
        )
        _require_binding_identity(
            report_binding,
            f"{terminal_stage}_quality_report:{expected_cell.cell_id}",
            f"quality_evidence.cells[{index}].quality_report_binding",
        )
        quality_report_bindings.append(report_binding.to_dict())
        if cell.get("common_scored_gate_passed") is False:
            failed_frozen_coverage_gates.append(
                f"{expected_cell.cell_id}:common_scored_coverage"
            )
        if cell.get("decision_eligible_gate_passed") is False:
            failed_frozen_coverage_gates.append(
                f"{expected_cell.cell_id}:decision_eligible_coverage"
            )
    if len(cells) != len(V4_TIMING_CELLS):
        raise ExperimentValidationError(
            "structural infeasibility report requires all seven quality cells"
        )
    if not failed_frozen_coverage_gates:
        raise ExperimentValidationError(
            "structural infeasibility report requires a failed frozen "
            "coverage gate"
        )
    payload = {
        "artifact_type": (
            f"{terminal_stage}_structural_gate_infeasibility_report"
        ),
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "attempt": attempt.to_dict(),
        "experiment_contract_digest": experiment_contract_digest,
        "terminal_stage": terminal_stage,
        "frozen_gate_contract": {
            "quality_gates_digest": canonical_sha256(
                FROZEN_QUALITY_GATES
            ),
            "reference_max_gap_ms": REFERENCE_MAX_GAP_MS,
            "max_future_skew_ms": MAX_FUTURE_SKEW_MS,
        },
        "quality_report_bindings": quality_report_bindings,
        "failed_frozen_coverage_gates": failed_frozen_coverage_gates,
        "conclusion": "frozen_gate_structurally_infeasible",
        "retry_disposition": "new_policy_and_lineage_required",
    }
    canonical_json_bytes(payload)
    return payload


def _validate_terminal_artifact_inventory(result: V4TerminalResult) -> None:
    inventory = result.evidence_artifacts
    if not inventory:
        raise ExperimentValidationError("evidence_artifacts must not be empty")
    if any(item.schema_version != EXPERIMENT_SCHEMA_VERSION for item in inventory):
        raise ExperimentValidationError(
            "evidence_artifacts contain an unsupported schema"
        )
    artifact_types = tuple(item.artifact_type for item in inventory)
    if len(artifact_types) != len(set(artifact_types)):
        raise ExperimentValidationError(
            "evidence_artifacts contain duplicate artifact types"
        )
    _require_inventory_binding(
        inventory, result.provenance_checkpoint, "provenance_checkpoint"
    )
    for field_name in (
        "calibration_start_marker",
        "calibration_completion_marker",
        "holdout_start_marker",
        "holdout_completion_marker",
        "preregistration_binding",
        "receipt_deadline_check",
        "pushed_receipt",
    ):
        _require_inventory_binding(
            inventory, getattr(result, field_name), field_name
        )
    expected_candidate_ledger = f"{result.terminal_stage}_candidate_day_ledger"
    if expected_candidate_ledger not in artifact_types:
        raise ExperimentValidationError(
            "result does not bind its candidate-day ledger"
        )
    candidate_binding = next(
        item for item in inventory if item.artifact_type == expected_candidate_ledger
    )
    if candidate_binding.sha256 != result.candidate_day_ledger_root:
        raise ExperimentValidationError(
            "candidate-day ledger binding differs from its root"
        )
    calibration_quality_types = {
        f"calibration_quality_report:{cell.cell_id}"
        for cell in V4_TIMING_CELLS
    }
    holdout_quality_types = {
        f"holdout_quality_report:{cell.cell_id}"
        for cell in V4_TIMING_CELLS
    }
    allowed_types = {
        "calibration_candidate_day_ledger",
        "calibration_retry_eligibility",
        "calibration_successor_authorization",
        "calibration_attempt_freeze_deadline_check",
        "calibration_attempt_freeze",
        "calibration_archive_checkpoint_manifest",
        "calibration_archive_failure_report",
            "calibration_raw_manifest",
            "calibration_structural_gate_infeasibility_report",
            "calibration_pre_efficacy_provenance_gate",
        "calibration_efficacy_started",
        "calibration_efficacy_ledger",
        "calibration_efficacy_report",
        "calibration_efficacy_completed",
        "holdout_selection_authorization",
        "holdout_retry_eligibility",
        "holdout_candidate_day_ledger",
        "holdout_successor_authorization",
        "holdout_preregistration_deadline_check",
        PREREGISTRATION_ARTIFACT_TYPE,
        "holdout_pushed_preregistration_receipt",
        "holdout_receipt_deadline_check",
        "holdout_archive_checkpoint_manifest",
        "holdout_archive_failure_report",
            "holdout_raw_manifest",
            "holdout_structural_gate_infeasibility_report",
            "holdout_pre_efficacy_provenance_gate",
        "holdout_efficacy_started",
        "holdout_efficacy_ledger",
        "holdout_bootstrap_report",
        "holdout_efficacy_report",
        "holdout_efficacy_completed",
        "stage_terminal_checkpoint",
        "final_analysis_checkpoint",
        *calibration_quality_types,
        *holdout_quality_types,
    }
    unsupported = set(artifact_types) - allowed_types
    if unsupported:
        raise ExperimentValidationError(
            "evidence_artifacts contain unsupported artifact types"
        )
    checkpoint_type = result.provenance_checkpoint.artifact_type
    if result.terminal_stage == "calibration":
        base_allowed = {
            "calibration_candidate_day_ledger",
            checkpoint_type,
        }
        stage_allowed = set(base_allowed)
        if result.failure_stage == "calibration_attempt_freeze":
            stage_allowed.add("calibration_attempt_freeze_deadline_check")
        elif result.failure_stage != "calibration_window_selection":
            stage_allowed.add("calibration_attempt_freeze")
            if result.failure_stage == "calibration_archive":
                stage_allowed.update(
                        {
                            "calibration_archive_checkpoint_manifest",
                            "calibration_archive_failure_report",
                            "calibration_raw_manifest",
                    }
                )
            else:
                stage_allowed.update(
                    {
                        "calibration_archive_checkpoint_manifest",
                        "calibration_raw_manifest",
                    }
                )
            if result.quality_evidence["status"] in ("failed", "passed"):
                stage_allowed.update(calibration_quality_types)
            if result.quality_evidence.get(
                "structural_gate_infeasibility_report_binding"
            ) is not None:
                stage_allowed.add(
                    "calibration_structural_gate_infeasibility_report"
                )
        if result.quality_evidence["status"] == "passed":
            stage_allowed.add("calibration_pre_efficacy_provenance_gate")
        if result.calibration_efficacy_started:
            stage_allowed.update(
                {
                    "calibration_efficacy_started",
                    "calibration_efficacy_ledger",
                    "calibration_efficacy_report",
                }
            )
        if result.calibration_efficacy_completed:
            stage_allowed.add("calibration_efficacy_completed")
    else:
        stage_allowed = {
            "calibration_efficacy_started",
            "calibration_efficacy_completed",
            "holdout_candidate_day_ledger",
            checkpoint_type,
        }
        if result.attempt.holdout_attempt_index == 0:
            stage_allowed.add("holdout_selection_authorization")
        else:
            stage_allowed.update(
                {
                    "holdout_retry_eligibility",
                    "holdout_successor_authorization",
                }
            )
        if result.preregistration_binding is not None:
            stage_allowed.add(PREREGISTRATION_ARTIFACT_TYPE)
        if result.receipt_deadline_check is not None:
            stage_allowed.add(result.receipt_deadline_check.artifact_type)
        if result.pushed_receipt is not None:
            stage_allowed.add("holdout_pushed_preregistration_receipt")
        if result.failure_stage == "holdout_archive":
            stage_allowed.update(
                {
                    "holdout_archive_checkpoint_manifest",
                    "holdout_archive_failure_report",
                    "holdout_raw_manifest",
                }
            )
        elif result.failure_stage not in (
            "holdout_window_selection",
            "preregistration_lead",
        ):
            stage_allowed.update(
                {
                    "holdout_archive_checkpoint_manifest",
                    "holdout_raw_manifest",
                }
            )
            if result.quality_evidence["status"] in ("failed", "passed"):
                stage_allowed.update(holdout_quality_types)
            if result.quality_evidence.get(
                "structural_gate_infeasibility_report_binding"
            ) is not None:
                stage_allowed.add(
                    "holdout_structural_gate_infeasibility_report"
                )
        if result.quality_evidence["status"] == "passed":
            stage_allowed.add("holdout_pre_efficacy_provenance_gate")
        if result.holdout_efficacy_started:
            stage_allowed.update(
                {
                    "holdout_efficacy_started",
                    "holdout_efficacy_ledger",
                    "holdout_bootstrap_report",
                    "holdout_efficacy_report",
                }
            )
        if result.holdout_efficacy_completed:
            stage_allowed.add("holdout_efficacy_completed")
    if result.attempt.calibration_attempt_index == 1:
        stage_allowed.update(
            {
                "calibration_retry_eligibility",
                "calibration_successor_authorization",
            }
        )
    unavailable = set(artifact_types) - stage_allowed
    if unavailable:
        raise ExperimentValidationError(
            "evidence_artifacts contain stage-unavailable artifacts"
        )
    canonical_order = (
        "calibration_retry_eligibility",
        "calibration_candidate_day_ledger",
        "calibration_successor_authorization",
        "calibration_attempt_freeze_deadline_check",
        "calibration_attempt_freeze",
        "calibration_archive_checkpoint_manifest",
        "calibration_archive_failure_report",
        "calibration_raw_manifest",
        *tuple(
            f"calibration_quality_report:{cell.cell_id}"
            for cell in V4_TIMING_CELLS
        ),
        "calibration_structural_gate_infeasibility_report",
        "calibration_pre_efficacy_provenance_gate",
        "calibration_efficacy_started",
        "calibration_efficacy_ledger",
        "calibration_efficacy_report",
        "calibration_efficacy_completed",
        "holdout_selection_authorization",
        "holdout_retry_eligibility",
        "holdout_candidate_day_ledger",
        "holdout_successor_authorization",
        "holdout_preregistration_deadline_check",
        PREREGISTRATION_ARTIFACT_TYPE,
        "holdout_pushed_preregistration_receipt",
        "holdout_receipt_deadline_check",
        "holdout_archive_checkpoint_manifest",
        "holdout_archive_failure_report",
        "holdout_raw_manifest",
        *tuple(
            f"holdout_quality_report:{cell.cell_id}"
            for cell in V4_TIMING_CELLS
        ),
        "holdout_structural_gate_infeasibility_report",
        "holdout_pre_efficacy_provenance_gate",
        "holdout_efficacy_started",
        "holdout_efficacy_ledger",
        "holdout_bootstrap_report",
        "holdout_efficacy_report",
        "holdout_efficacy_completed",
        "stage_terminal_checkpoint",
        "final_analysis_checkpoint",
    )
    order_index = {
        artifact_type: index
        for index, artifact_type in enumerate(canonical_order)
    }
    if tuple(sorted(artifact_types, key=order_index.__getitem__)) != artifact_types:
        raise ExperimentValidationError(
            "evidence_artifacts are not in canonical stage order"
        )
    if result.attempt.calibration_attempt_index == 1:
        for index, binding in enumerate(result.ancestry[1:3]):
            _require_inventory_binding(
                inventory,
                binding,
                f"calibration_successor_ancestry[{index}]",
            )
    if result.attempt.holdout_attempt_index == 1:
        for index, binding in enumerate(result.ancestry[-2:]):
            _require_inventory_binding(
                inventory,
                binding,
                f"holdout_successor_ancestry[{index}]",
            )
    quality_status = result.quality_evidence["status"]
    stage_quality_types = tuple(
        f"{result.terminal_stage}_quality_report:{cell.cell_id}"
        for cell in V4_TIMING_CELLS
    )
    present_quality_types = tuple(
        item for item in artifact_types if item in stage_quality_types
    )
    if quality_status == "passed" and present_quality_types != stage_quality_types:
        raise ExperimentValidationError(
            "passed quality evidence requires all seven quality reports"
        )
    if quality_status == "failed":
        completed = tuple(
            cell["cell_id"] for cell in result.quality_evidence["cells"]
        )
        if present_quality_types != stage_quality_types[: len(completed)]:
            raise ExperimentValidationError(
                "failed quality inventory differs from completed cells"
            )
    if quality_status == "not_reached" and present_quality_types:
        raise ExperimentValidationError(
            "quality reports exist before the quality stage was reached"
        )
    calibration_freeze_required = (
        result.terminal_stage == "calibration"
        and (
            result.decision != "insufficient_evidence"
            or result.failure_stage
            not in ("calibration_window_selection", "calibration_attempt_freeze")
        )
    )
    if calibration_freeze_required and (
        "calibration_attempt_freeze" not in artifact_types
    ):
        raise ExperimentValidationError(
            "post-freeze calibration result omits the attempt freeze"
        )
    if (
        result.terminal_stage == "calibration"
        and result.failure_stage == "calibration_attempt_freeze"
        and "calibration_attempt_freeze_deadline_check" not in artifact_types
    ):
        raise ExperimentValidationError(
            "attempt-freeze failure omits its deadline check"
        )
    if quality_status in ("failed", "passed"):
        by_type = {item.artifact_type: item for item in inventory}
        for cell in result.quality_evidence["cells"]:
            quality_binding = ArtifactBinding.from_dict(
                cell["quality_report_binding"],
                "quality_evidence.cells.quality_report_binding",
            )
            if by_type.get(quality_binding.artifact_type) != quality_binding:
                raise ExperimentValidationError(
                    "quality report inventory differs from quality evidence"
                )
    structural_report_raw = result.quality_evidence.get(
        "structural_gate_infeasibility_report_binding"
    )
    if structural_report_raw is not None:
        structural_report = ArtifactBinding.from_dict(
            structural_report_raw,
            (
                "quality_evidence."
                "structural_gate_infeasibility_report_binding"
            ),
        )
        _require_inventory_binding(
            inventory,
            structural_report,
            "structural_gate_infeasibility_report_binding",
        )
        expected_structural_report = (
            _structural_gate_infeasibility_report_payload(
                attempt=result.attempt,
                experiment_contract_digest=(
                    result.experiment_contract.digest
                ),
                terminal_stage=result.terminal_stage,
                quality_evidence=result.quality_evidence,
            )
        )
        if structural_report.sha256 != artifact_sha256(
            expected_structural_report
        ):
            raise ExperimentValidationError(
                "structural infeasibility report differs from the "
                "validated quality evidence"
            )
    if quality_status in ("failed", "passed") and (
        f"{result.terminal_stage}_raw_manifest" not in artifact_types
    ):
        raise ExperimentValidationError(
            "completed quality work requires the sealed raw manifest"
        )
    if quality_status in ("failed", "passed") and (
        f"{result.terminal_stage}_archive_checkpoint_manifest"
        not in artifact_types
    ):
        raise ExperimentValidationError(
            "completed quality work requires the archive checkpoint manifest"
        )
    if result.failure_stage in ("calibration_archive", "holdout_archive") and (
        f"{result.terminal_stage}_archive_failure_report"
        not in artifact_types
    ):
        raise ExperimentValidationError(
            "archive failure omits its failure report"
        )
    if quality_status == "passed" and (
        f"{result.terminal_stage}_pre_efficacy_provenance_gate"
        not in artifact_types
    ):
        raise ExperimentValidationError(
            "passed quality evidence requires the pre-efficacy provenance gate"
        )
    if result.calibration_efficacy_started and (
        "calibration_efficacy_started" not in artifact_types
    ):
        raise ExperimentValidationError("calibration start marker is not inventoried")
    if result.calibration_efficacy_completed and (
        "calibration_efficacy_completed" not in artifact_types
    ):
        raise ExperimentValidationError(
            "calibration completion marker is not inventoried"
        )
    if result.holdout_efficacy_started and (
        "holdout_efficacy_started" not in artifact_types
    ):
        raise ExperimentValidationError("holdout start marker is not inventoried")
    if result.holdout_efficacy_completed and (
        "holdout_efficacy_completed" not in artifact_types
    ):
        raise ExperimentValidationError("holdout completion marker is not inventoried")
    if (
        result.terminal_stage == "calibration"
        and result.calibration_efficacy_completed
        and not {
            "calibration_efficacy_ledger",
            "calibration_efficacy_report",
        }.issubset(artifact_types)
    ):
        raise ExperimentValidationError(
            "completed calibration omits immutable efficacy artifacts"
        )
    if result.holdout_efficacy_completed and not {
        "holdout_efficacy_ledger",
        "holdout_bootstrap_report",
        "holdout_efficacy_report",
    }.issubset(artifact_types):
        raise ExperimentValidationError(
            "completed holdout omits immutable efficacy artifacts"
        )
    terminal_completed = (
        result.calibration_efficacy_completed
        if result.terminal_stage == "calibration"
        else result.holdout_efficacy_completed
    )
    if terminal_completed:
        by_type = {item.artifact_type: item for item in inventory}
        efficacy_report = by_type[
            f"{result.terminal_stage}_efficacy_report"
        ]
        completion_marker = (
            result.calibration_completion_marker
            if result.terminal_stage == "calibration"
            else result.holdout_completion_marker
        )
        expected_completion_marker = efficacy_completion_marker_payload(
            attempt=result.attempt,
            experiment_contract_digest=result.experiment_contract.digest,
            terminal_stage=result.terminal_stage,
            efficacy_start_marker=by_type[
                f"{result.terminal_stage}_efficacy_started"
            ],
            prerequisite_artifacts=tuple(
                by_type[artifact_type]
                for artifact_type in (
                    (
                        "calibration_attempt_freeze",
                        "calibration_raw_manifest",
                        "calibration_pre_efficacy_provenance_gate",
                    )
                    if result.terminal_stage == "calibration"
                    else (
                        PREREGISTRATION_ARTIFACT_TYPE,
                        "holdout_pushed_preregistration_receipt",
                        "holdout_receipt_deadline_check",
                        "holdout_raw_manifest",
                        "holdout_pre_efficacy_provenance_gate",
                    )
                )
            ),
            efficacy_report=efficacy_report,
            immutable_efficacy_artifacts=tuple(
                by_type[artifact_type]
                for artifact_type in (
                    (
                        "calibration_efficacy_started",
                        "calibration_efficacy_ledger",
                        "calibration_efficacy_report",
                    )
                    if result.terminal_stage == "calibration"
                    else (
                        "holdout_efficacy_started",
                        "holdout_efficacy_ledger",
                        "holdout_bootstrap_report",
                        "holdout_efficacy_report",
                    )
                )
            ),
            completed_at_ms=result.terminal_efficacy_completed_at_ms,
        )
        if completion_marker.sha256 != artifact_sha256(
            expected_completion_marker
        ):
            raise ExperimentValidationError(
                "efficacy completion marker differs from its immutable report"
            )
    if result.decision != "insufficient_evidence":
        required_efficacy_types = (
            f"{result.terminal_stage}_efficacy_ledger",
            f"{result.terminal_stage}_efficacy_report",
        )
        if not set(required_efficacy_types).issubset(artifact_types):
            raise ExperimentValidationError(
                "performance result omits required efficacy artifacts"
            )
        if result.terminal_stage == "holdout" and (
            "holdout_bootstrap_report" not in artifact_types
        ):
            raise ExperimentValidationError(
                "holdout performance result omits its bootstrap report"
            )
        by_type = {item.artifact_type: item for item in inventory}
        report_binding = by_type[
            f"{result.terminal_stage}_efficacy_report"
        ]
        report_payload = terminal_efficacy_report_payload(
            attempt=result.attempt,
            experiment_contract_digest=result.experiment_contract.digest,
            terminal_stage=result.terminal_stage,
            decision=result.decision,
            efficacy_evidence=result.efficacy_evidence,
            efficacy_ledger=by_type[
                f"{result.terminal_stage}_efficacy_ledger"
            ],
            bootstrap_report=(
                by_type["holdout_bootstrap_report"]
                if result.terminal_stage == "holdout"
                else None
            ),
        )
        if report_binding.sha256 != artifact_sha256(report_payload):
            raise ExperimentValidationError(
                "terminal efficacy metrics differ from the immutable report"
            )
    if result.selection_anchor_provenance is not None:
        _require_inventory_binding(
            inventory,
            result.selection_anchor_provenance.source_artifact,
            "selection_anchor_provenance.source_artifact",
        )
        _require_inventory_binding(
            inventory,
            result.selection_anchor_provenance.authorization_artifact,
            "selection_anchor_provenance.authorization_artifact",
        )


@dataclass(frozen=True)
class V4TerminalResult:
    attempt: AttemptIdentity
    experiment_contract: V4ExperimentContract
    terminal_stage: str
    decision: str
    failure_stage: Optional[str]
    failure_reasons: tuple[str, ...]
    parent_result: Optional[ArtifactBinding]
    ancestry: tuple[ArtifactBinding, ...]
    retry_state: Mapping[str, Any]
    selection_anchor_provenance: Optional[SelectionAnchorProvenance]
    candidate_day_ledger_root: str
    provenance_continuity_root: str
    preregistration_binding: Optional[ArtifactBinding]
    preregistration_publication_deadline_ms: Optional[int]
    receipt_deadline_check: Optional[ArtifactBinding]
    pushed_receipt: Optional[ArtifactBinding]
    holdout_attempted: bool
    calibration_efficacy_started: bool
    calibration_efficacy_completed: bool
    calibration_start_marker: Optional[ArtifactBinding]
    calibration_completion_marker: Optional[ArtifactBinding]
    holdout_efficacy_started: bool
    holdout_efficacy_completed: bool
    holdout_start_marker: Optional[ArtifactBinding]
    holdout_completion_marker: Optional[ArtifactBinding]
    terminal_efficacy_completed_at_ms: Optional[int]
    efficacy_attempt_consumed: bool
    evidence_artifacts: tuple[ArtifactBinding, ...]
    provenance_checkpoint: ArtifactBinding
    quality_evidence: Mapping[str, Any]
    efficacy_evidence: Optional[Mapping[str, Any]]
    frozen_challenger: Optional[ModelIdentity]
    created_at_ms: int

    def __post_init__(self) -> None:
        try:
            failure_reasons = tuple(self.failure_reasons)
            ancestry = tuple(self.ancestry)
            evidence_artifacts = tuple(self.evidence_artifacts)
        except TypeError as exc:
            raise ExperimentValidationError(
                "result sequence fields must be arrays"
            ) from exc
        if not all(isinstance(item, str) and item for item in failure_reasons):
            raise ExperimentValidationError(
                "failure_reasons must contain non-empty strings"
            )
        if len(failure_reasons) != len(set(failure_reasons)):
            raise ExperimentValidationError("failure_reasons contain duplicates")
        if not all(isinstance(item, ArtifactBinding) for item in ancestry):
            raise ExperimentValidationError(
                "ancestry must contain ArtifactBinding values"
            )
        if not all(
            isinstance(item, ArtifactBinding) for item in evidence_artifacts
        ):
            raise ExperimentValidationError(
                "evidence_artifacts must contain ArtifactBinding values"
            )
        object.__setattr__(self, "failure_reasons", failure_reasons)
        object.__setattr__(self, "ancestry", ancestry)
        object.__setattr__(self, "evidence_artifacts", evidence_artifacts)
        if not isinstance(self.attempt, AttemptIdentity):
            raise TypeError("attempt must be AttemptIdentity")
        if not isinstance(self.experiment_contract, V4ExperimentContract):
            raise TypeError("experiment_contract must be V4ExperimentContract")
        if self.terminal_stage not in ("calibration", "holdout"):
            raise ExperimentValidationError("terminal_stage is unsupported")
        if self.decision not in (
            "insufficient_evidence",
            "retain_incumbent",
            "promotion_eligible",
        ):
            raise ExperimentValidationError("terminal decision is unsupported")
        _require_sha256(
            self.candidate_day_ledger_root, "candidate_day_ledger_root"
        )
        _require_sha256(
            self.provenance_continuity_root, "provenance_continuity_root"
        )
        _require_int(self.created_at_ms, "created_at_ms", minimum=0)
        _require_bool(self.holdout_attempted, "holdout_attempted")
        for field_name in (
            "calibration_efficacy_started",
            "calibration_efficacy_completed",
            "holdout_efficacy_started",
            "holdout_efficacy_completed",
            "efficacy_attempt_consumed",
        ):
            _require_bool(getattr(self, field_name), field_name)
        if self.calibration_efficacy_completed and not (
            self.calibration_efficacy_started
        ):
            raise ExperimentValidationError(
                "calibration completion requires a start marker"
            )
        if self.holdout_efficacy_completed and not self.holdout_efficacy_started:
            raise ExperimentValidationError(
                "holdout completion requires a start marker"
            )
        terminal_completed = (
            self.calibration_efficacy_completed
            if self.terminal_stage == "calibration"
            else self.holdout_efficacy_completed
        )
        if terminal_completed != (
            self.terminal_efficacy_completed_at_ms is not None
        ):
            raise ExperimentValidationError(
                "terminal completion time presence differs from its marker"
            )
        if self.terminal_efficacy_completed_at_ms is not None:
            _require_int(
                self.terminal_efficacy_completed_at_ms,
                "terminal_efficacy_completed_at_ms",
                minimum=0,
            )
            if self.terminal_efficacy_completed_at_ms > self.created_at_ms:
                raise ExperimentValidationError(
                    "terminal completion time follows result creation"
                )
        for present, binding, field_name, expected_type in (
            (
                self.calibration_efficacy_started,
                self.calibration_start_marker,
                "calibration start marker",
                "calibration_efficacy_started",
            ),
            (
                self.calibration_efficacy_completed,
                self.calibration_completion_marker,
                "calibration completion marker",
                "calibration_efficacy_completed",
            ),
            (
                self.holdout_efficacy_started,
                self.holdout_start_marker,
                "holdout start marker",
                "holdout_efficacy_started",
            ),
            (
                self.holdout_efficacy_completed,
                self.holdout_completion_marker,
                "holdout completion marker",
                "holdout_efficacy_completed",
            ),
        ):
            _marker_binding(
                present=present,
                binding=binding,
                field_name=field_name,
                expected_type=expected_type,
            )
        if self.terminal_stage == "calibration":
            if self.attempt.holdout_attempt_index is not None:
                raise ExperimentValidationError(
                    "terminal calibration result requires null holdout index"
                )
            if self.holdout_attempted or self.holdout_efficacy_started or (
                self.holdout_efficacy_completed
            ):
                raise ExperimentValidationError(
                    "terminal calibration result cannot claim a holdout"
                )
            if self.efficacy_attempt_consumed:
                raise ExperimentValidationError(
                    "calibration result cannot consume holdout efficacy"
                )
            if self.decision == "promotion_eligible":
                raise ExperimentValidationError(
                    "promotion_eligible is a holdout-only result"
                )
            if self.frozen_challenger is not None:
                raise ExperimentValidationError(
                    "terminal calibration result cannot freeze a challenger"
                )
            if self.selection_anchor_provenance is not None or any(
                value is not None
                for value in (
                    self.preregistration_binding,
                    self.preregistration_publication_deadline_ms,
                    self.receipt_deadline_check,
                    self.pushed_receipt,
                )
            ):
                raise ExperimentValidationError(
                    "calibration result cannot contain holdout bindings"
                )
        else:
            if self.attempt.holdout_attempt_index not in (0, 1):
                raise ExperimentValidationError(
                    "holdout result requires a holdout attempt index"
                )
            if not self.holdout_attempted:
                raise ExperimentValidationError(
                    "a holdout-stage result must record the allocated attempt"
                )
            if not (
                self.calibration_efficacy_started
                and self.calibration_efficacy_completed
            ):
                raise ExperimentValidationError(
                    "a holdout result requires completed calibration efficacy"
                )
            if self.efficacy_attempt_consumed != self.holdout_efficacy_started:
                raise ExperimentValidationError(
                    "holdout efficacy consumption must derive from its start marker"
                )
            if not isinstance(
                self.selection_anchor_provenance, SelectionAnchorProvenance
            ):
                raise ExperimentValidationError(
                    "holdout result requires selection-anchor provenance"
                )
            expected_anchor_mode = (
                "calibration_completion"
                if self.attempt.holdout_attempt_index == 0
                else "retry_eligibility"
            )
            if self.selection_anchor_provenance.mode != expected_anchor_mode:
                raise ExperimentValidationError(
                    "holdout anchor mode is inconsistent with its attempt index"
                )
            if self.created_at_ms < self.selection_anchor_provenance.timestamp_ms:
                raise ExperimentValidationError(
                    "holdout result predates its selection anchor"
                )
            if self.frozen_challenger is None:
                raise ExperimentValidationError(
                    "a holdout-stage result requires the frozen challenger"
                )
            eligible = {
                canonical_json_bytes(
                    self.experiment_contract.candidate_identity(lag).to_dict()
                )
                for lag in PROMOTION_ELIGIBLE_LAGS_MS
            }
            if canonical_json_bytes(self.frozen_challenger.to_dict()) not in eligible:
                raise ExperimentValidationError(
                    "holdout challenger is not eligible under v4"
                )
            window_selection_failed = (
                self.decision == "insufficient_evidence"
                and self.failure_stage == "holdout_window_selection"
            )
            preregistration_publication_missed = (
                self.decision == "insufficient_evidence"
                and self.failure_stage == "preregistration_lead"
                and self.failure_reasons
                == ("preregistration_deadline_missed",)
            )
            if window_selection_failed:
                if any(
                    value is not None
                    for value in (
                        self.preregistration_binding,
                        self.preregistration_publication_deadline_ms,
                        self.receipt_deadline_check,
                        self.pushed_receipt,
                    )
                ):
                    raise ExperimentValidationError(
                        "window-selection failure cannot bind preregistration artifacts"
                    )
            elif preregistration_publication_missed:
                if (
                    self.preregistration_binding is not None
                    or self.pushed_receipt is not None
                ):
                    raise ExperimentValidationError(
                        "missed preregistration publication cannot bind a "
                        "preregistration or pushed receipt"
                    )
                _require_binding_identity(
                    self.receipt_deadline_check,
                    "holdout_preregistration_deadline_check",
                    "receipt_deadline_check",
                )
                if self.preregistration_publication_deadline_ms is None:
                    raise ExperimentValidationError(
                        "missed preregistration requires its frozen deadline"
                    )
                _require_int(
                    self.preregistration_publication_deadline_ms,
                    "preregistration_publication_deadline_ms",
                    minimum=0,
                )
                if self.created_at_ms <= (
                    self.preregistration_publication_deadline_ms
                ):
                    raise ExperimentValidationError(
                        "preregistration-missed result does not follow its deadline"
                    )
            else:
                _require_binding_identity(
                    self.preregistration_binding,
                    PREREGISTRATION_ARTIFACT_TYPE,
                    "preregistration_binding",
                )
                _require_binding_identity(
                    self.receipt_deadline_check,
                    "holdout_receipt_deadline_check",
                    "receipt_deadline_check",
                )
                if self.preregistration_publication_deadline_ms is None:
                    raise ExperimentValidationError(
                        "preregistered result requires its publication deadline"
                    )
                _require_int(
                    self.preregistration_publication_deadline_ms,
                    "preregistration_publication_deadline_ms",
                    minimum=0,
                )
                receipt_may_be_absent = (
                    self.decision == "insufficient_evidence"
                    and self.failure_stage == "preregistration_lead"
                )
                if self.pushed_receipt is not None:
                    _require_binding_identity(
                        self.pushed_receipt,
                        "holdout_pushed_preregistration_receipt",
                        "pushed_receipt",
                    )
                elif not receipt_may_be_absent:
                    raise ExperimentValidationError(
                        "post-lead holdout result requires the pushed receipt"
                    )
        if self.decision == "insufficient_evidence":
            failure_states = (
                _CALIBRATION_FAILURE_STATES
                if self.terminal_stage == "calibration"
                else _HOLDOUT_FAILURE_STATES
            )
            if self.failure_stage not in failure_states:
                raise ExperimentValidationError(
                    "failure_stage is unsupported for the terminal stage"
                )
            if not self.failure_reasons or not set(self.failure_reasons).issubset(
                _FAILURE_REASON_CODES[self.failure_stage]
            ):
                raise ExperimentValidationError(
                    "failure_reasons are unsupported for the failure stage"
                )
            if (
                self.failure_stage == "preregistration_lead"
                and len(self.failure_reasons) != 1
            ):
                raise ExperimentValidationError(
                    "preregistration-lead failures require one exact reason"
                )
            marker_state = (
                (
                    self.calibration_efficacy_started,
                    self.calibration_efficacy_completed,
                )
                if self.terminal_stage == "calibration"
                else (
                    self.holdout_efficacy_started,
                    self.holdout_efficacy_completed,
                )
            )
            if marker_state not in failure_states[self.failure_stage]:
                raise ExperimentValidationError(
                    "efficacy marker state is inconsistent with failure_stage"
                )
            if self.efficacy_evidence is not None:
                raise ExperimentValidationError(
                    "insufficient evidence cannot expose efficacy values"
                )
        else:
            if self.failure_stage is not None or self.failure_reasons:
                raise ExperimentValidationError(
                    "performance decisions cannot contain failure fields"
                )
            if not (
                self.calibration_efficacy_started
                and self.calibration_efficacy_completed
            ):
                raise ExperimentValidationError(
                    "a performance decision requires completed calibration efficacy"
                )
            if self.terminal_stage == "holdout" and not (
                self.holdout_efficacy_started
                and self.holdout_efficacy_completed
                and self.efficacy_attempt_consumed
            ):
                raise ExperimentValidationError(
                    "a holdout performance decision requires completed efficacy"
                )
            if self.efficacy_evidence is None:
                raise ExperimentValidationError(
                    "a performance decision requires efficacy evidence"
                )
        quality_evidence = _validate_quality_evidence(
            self.quality_evidence,
            terminal_stage=self.terminal_stage,
            decision=self.decision,
            failure_stage=self.failure_stage,
        )
        structural_gate_infeasibility = (
            quality_evidence.get(
                "structural_gate_infeasibility_report_binding"
            )
            is not None
        )
        if self.failure_stage in (
            "calibration_quality",
            "holdout_quality",
        ):
            quality_failure_codes = set(quality_evidence["failure_codes"])
            if structural_gate_infeasibility:
                expected_failure_reasons = (
                    "structural_gate_infeasibility",
                )
            else:
                derived_reasons = []
                if "causal_violation" in quality_failure_codes:
                    derived_reasons.append("causal_integrity_failure")
                if quality_failure_codes - {"causal_violation"}:
                    derived_reasons.append("quality_gate_failure")
                expected_failure_reasons = tuple(derived_reasons)
            if self.failure_reasons != expected_failure_reasons:
                raise ExperimentValidationError(
                    "quality failure_reasons are not derived from quality "
                    "evidence"
                )
        efficacy_evidence = self.efficacy_evidence
        if efficacy_evidence is not None:
            if self.terminal_stage == "holdout":
                efficacy_evidence = _validate_holdout_efficacy_evidence(
                    efficacy_evidence,
                    decision=self.decision,
                    preregistration_sha256=self.preregistration_binding.sha256,
                )
            else:
                if self.decision != "retain_incumbent":
                    raise ExperimentValidationError(
                        "calibration efficacy can only support retain_incumbent"
                    )
                efficacy_evidence = _validate_calibration_efficacy_evidence(
                    efficacy_evidence
                )
        retry_state = _validate_retry_state(
            self.retry_state,
            attempt=self.attempt,
            terminal_stage=self.terminal_stage,
            decision=self.decision,
            failure_stage=self.failure_stage,
            structural_gate_infeasibility=structural_gate_infeasibility,
            stage_started=(
                self.calibration_efficacy_started
                if self.terminal_stage == "calibration"
                else self.holdout_efficacy_started
            ),
        )
        _validate_result_ancestry(
            attempt=self.attempt,
            parent_result=self.parent_result,
            ancestry=self.ancestry,
            selection_anchor_provenance=self.selection_anchor_provenance,
        )
        expected_checkpoint_type = (
            "final_analysis_checkpoint"
            if self.decision != "insufficient_evidence"
            or self.failure_stage in (
                "calibration_post_start_provenance",
                "holdout_post_start_provenance",
            )
            else "stage_terminal_checkpoint"
        )
        _require_binding_identity(
            self.provenance_checkpoint,
            expected_checkpoint_type,
            "provenance_checkpoint",
        )
        if self.provenance_continuity_root != self.provenance_checkpoint.sha256:
            raise ExperimentValidationError(
                "provenance continuity root differs from the immediately "
                "preceding checkpoint"
            )
        hashes = [binding.sha256 for binding in self.evidence_artifacts]
        if len(hashes) != len(set(hashes)):
            raise ExperimentValidationError(
                "evidence_artifacts contain duplicate hashes"
            )
        object.__setattr__(self, "quality_evidence", quality_evidence)
        _validate_terminal_artifact_inventory(self)
        object.__setattr__(self, "retry_state", _freeze_json(retry_state))
        object.__setattr__(
            self, "quality_evidence", _freeze_json(quality_evidence)
        )
        if efficacy_evidence is not None:
            object.__setattr__(
                self,
                "efficacy_evidence",
                _freeze_json(efficacy_evidence),
            )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "artifact_type": TERMINAL_RESULT_ARTIFACT_TYPE,
            "schema_version": EXPERIMENT_SCHEMA_VERSION,
            "policy_version": POLICY_VERSION,
            "attempt": self.attempt.to_dict(),
            "experiment_contract": self.experiment_contract.to_dict(),
            "experiment_contract_digest": self.experiment_contract.digest,
            "terminal_stage": self.terminal_stage,
            "decision": self.decision,
            "failure_stage": self.failure_stage,
            "failure_reasons": list(self.failure_reasons),
            "parent_result": (
                self.parent_result.to_dict()
                if self.parent_result is not None
                else None
            ),
            "ancestry": [binding.to_dict() for binding in self.ancestry],
            "retry_state": _json_ready(self.retry_state),
            "selection_anchor_provenance": (
                self.selection_anchor_provenance.to_dict()
                if self.selection_anchor_provenance is not None
                else None
            ),
            "candidate_day_ledger_root": self.candidate_day_ledger_root,
            "provenance_continuity_root": self.provenance_continuity_root,
            "preregistration_binding": (
                self.preregistration_binding.to_dict()
                if self.preregistration_binding is not None
                else None
            ),
            "preregistration_publication_deadline_ms": (
                self.preregistration_publication_deadline_ms
            ),
            "receipt_deadline_check": (
                self.receipt_deadline_check.to_dict()
                if self.receipt_deadline_check is not None
                else None
            ),
            "pushed_receipt": (
                self.pushed_receipt.to_dict()
                if self.pushed_receipt is not None
                else None
            ),
            "holdout_attempted": self.holdout_attempted,
            "calibration_efficacy_started": (
                self.calibration_efficacy_started
            ),
            "calibration_efficacy_completed": (
                self.calibration_efficacy_completed
            ),
            "calibration_start_marker": (
                self.calibration_start_marker.to_dict()
                if self.calibration_start_marker is not None
                else None
            ),
            "calibration_completion_marker": (
                self.calibration_completion_marker.to_dict()
                if self.calibration_completion_marker is not None
                else None
            ),
            "holdout_efficacy_started": self.holdout_efficacy_started,
            "holdout_efficacy_completed": self.holdout_efficacy_completed,
            "holdout_start_marker": (
                self.holdout_start_marker.to_dict()
                if self.holdout_start_marker is not None
                else None
            ),
            "holdout_completion_marker": (
                self.holdout_completion_marker.to_dict()
                if self.holdout_completion_marker is not None
                else None
            ),
            "terminal_efficacy_completed_at_ms": (
                self.terminal_efficacy_completed_at_ms
            ),
            "efficacy_attempt_consumed": self.efficacy_attempt_consumed,
            "evidence_artifacts": [
                binding.to_dict() for binding in self.evidence_artifacts
            ],
            "provenance_checkpoint": self.provenance_checkpoint.to_dict(),
            "quality_evidence": _json_ready(self.quality_evidence),
            "frozen_challenger": (
                self.frozen_challenger.to_dict()
                if self.frozen_challenger is not None
                else None
            ),
            "created_at_ms": self.created_at_ms,
        }
        if self.efficacy_evidence is not None:
            payload["efficacy_evidence"] = _json_ready(self.efficacy_evidence)
        return payload


def _optional_binding(value: object, field_name: str) -> Optional[ArtifactBinding]:
    if value is None:
        return None
    return ArtifactBinding.from_dict(value, field_name)


def _reconstruct_terminal_result_for_parent(
    payload: Mapping[str, Any], expected_contract: V4ExperimentContract
) -> V4TerminalResult:
    """Apply all intrinsic terminal-result invariants to a raw parent.

    External preregistration/authorization trust is validated separately by
    the successor chain. This reconstruction prevents a correctly hashed but
    internally malformed quality result from allocating a retry.
    """

    attempt = AttemptIdentity.from_dict(
        payload.get("attempt"), "successor_parent_result_artifact.attempt"
    )
    failure_reasons = tuple(
        _require_string(
            item,
            f"successor_parent_result_artifact.failure_reasons[{index}]",
        )
        for index, item in enumerate(
            _as_sequence(
                payload.get("failure_reasons"),
                "successor_parent_result_artifact.failure_reasons",
            )
        )
    )
    ancestry = _validate_artifact_bindings(
        payload.get("ancestry"),
        "successor_parent_result_artifact.ancestry",
    )
    evidence = _validate_artifact_bindings(
        payload.get("evidence_artifacts"),
        "successor_parent_result_artifact.evidence_artifacts",
    )
    challenger_raw = payload.get("frozen_challenger")
    return V4TerminalResult(
        attempt=attempt,
        experiment_contract=expected_contract,
        terminal_stage=_require_string(
            payload.get("terminal_stage"),
            "successor_parent_result_artifact.terminal_stage",
        ),
        decision=_require_string(
            payload.get("decision"),
            "successor_parent_result_artifact.decision",
        ),
        failure_stage=(
            None
            if payload.get("failure_stage") is None
            else _require_string(
                payload.get("failure_stage"),
                "successor_parent_result_artifact.failure_stage",
            )
        ),
        failure_reasons=failure_reasons,
        parent_result=_optional_binding(
            payload.get("parent_result"),
            "successor_parent_result_artifact.parent_result",
        ),
        ancestry=ancestry,
        retry_state=_require_nonempty_mapping(
            payload.get("retry_state"),
            "successor_parent_result_artifact.retry_state",
        ),
        selection_anchor_provenance=(
            None
            if payload.get("selection_anchor_provenance") is None
            else SelectionAnchorProvenance.from_dict(
                payload.get("selection_anchor_provenance"),
                "successor_parent_result_artifact.selection_anchor_provenance",
            )
        ),
        candidate_day_ledger_root=_require_sha256(
            payload.get("candidate_day_ledger_root"),
            "successor_parent_result_artifact.candidate_day_ledger_root",
        ),
        provenance_continuity_root=_require_sha256(
            payload.get("provenance_continuity_root"),
            "successor_parent_result_artifact.provenance_continuity_root",
        ),
        preregistration_binding=_optional_binding(
            payload.get("preregistration_binding"),
            "successor_parent_result_artifact.preregistration_binding",
        ),
        preregistration_publication_deadline_ms=(
            None
            if payload.get("preregistration_publication_deadline_ms") is None
            else _require_int(
                payload.get("preregistration_publication_deadline_ms"),
                (
                    "successor_parent_result_artifact."
                    "preregistration_publication_deadline_ms"
                ),
                minimum=0,
            )
        ),
        receipt_deadline_check=_optional_binding(
            payload.get("receipt_deadline_check"),
            "successor_parent_result_artifact.receipt_deadline_check",
        ),
        pushed_receipt=_optional_binding(
            payload.get("pushed_receipt"),
            "successor_parent_result_artifact.pushed_receipt",
        ),
        holdout_attempted=_require_bool(
            payload.get("holdout_attempted"),
            "successor_parent_result_artifact.holdout_attempted",
        ),
        calibration_efficacy_started=_require_bool(
            payload.get("calibration_efficacy_started"),
            "successor_parent_result_artifact.calibration_efficacy_started",
        ),
        calibration_efficacy_completed=_require_bool(
            payload.get("calibration_efficacy_completed"),
            "successor_parent_result_artifact.calibration_efficacy_completed",
        ),
        calibration_start_marker=_optional_binding(
            payload.get("calibration_start_marker"),
            "successor_parent_result_artifact.calibration_start_marker",
        ),
        calibration_completion_marker=_optional_binding(
            payload.get("calibration_completion_marker"),
            "successor_parent_result_artifact.calibration_completion_marker",
        ),
        holdout_efficacy_started=_require_bool(
            payload.get("holdout_efficacy_started"),
            "successor_parent_result_artifact.holdout_efficacy_started",
        ),
        holdout_efficacy_completed=_require_bool(
            payload.get("holdout_efficacy_completed"),
            "successor_parent_result_artifact.holdout_efficacy_completed",
        ),
        holdout_start_marker=_optional_binding(
            payload.get("holdout_start_marker"),
            "successor_parent_result_artifact.holdout_start_marker",
        ),
        holdout_completion_marker=_optional_binding(
            payload.get("holdout_completion_marker"),
            "successor_parent_result_artifact.holdout_completion_marker",
        ),
        terminal_efficacy_completed_at_ms=(
            None
            if payload.get("terminal_efficacy_completed_at_ms") is None
            else _require_int(
                payload.get("terminal_efficacy_completed_at_ms"),
                (
                    "successor_parent_result_artifact."
                    "terminal_efficacy_completed_at_ms"
                ),
                minimum=0,
            )
        ),
        efficacy_attempt_consumed=_require_bool(
            payload.get("efficacy_attempt_consumed"),
            "successor_parent_result_artifact.efficacy_attempt_consumed",
        ),
        evidence_artifacts=evidence,
        provenance_checkpoint=ArtifactBinding.from_dict(
            payload.get("provenance_checkpoint"),
            "successor_parent_result_artifact.provenance_checkpoint",
        ),
        quality_evidence=_require_nonempty_mapping(
            payload.get("quality_evidence"),
            "successor_parent_result_artifact.quality_evidence",
        ),
        efficacy_evidence=None,
        frozen_challenger=(
            None
            if challenger_raw is None
            else ModelIdentity.from_dict(
                challenger_raw,
                "successor_parent_result_artifact.frozen_challenger",
            )
        ),
        created_at_ms=_require_int(
            payload.get("created_at_ms"),
            "successor_parent_result_artifact.created_at_ms",
            minimum=0,
        ),
    )


def _validate_terminal_receipt_artifacts(
    *,
    result: V4TerminalResult,
    preregistration_context: Optional[Mapping[str, Any]],
    pushed_receipt_artifact: Optional[object],
    receipt_deadline_check_artifact: Optional[object],
) -> None:
    check_binding = result.receipt_deadline_check
    receipt_binding = result.pushed_receipt
    if check_binding is None:
        if (
            pushed_receipt_artifact is not None
            or receipt_deadline_check_artifact is not None
        ):
            raise ExperimentValidationError(
                "result cannot receive unbound receipt evidence"
            )
        return
    if receipt_deadline_check_artifact is None:
        raise ExperimentValidationError(
            "result requires its canonical raw receipt deadline check"
        )
    check_payload = _validate_bound_artifact_payload(
        receipt_deadline_check_artifact,
        binding=check_binding,
        field_name="receipt_deadline_check_artifact",
    )
    checked_at_ms = _require_int(
        check_payload.get("checked_at_ms"),
        "receipt_deadline_check_artifact.checked_at_ms",
        minimum=0,
    )
    if check_binding.artifact_type == "holdout_preregistration_deadline_check":
        if receipt_binding is not None or pushed_receipt_artifact is not None:
            raise ExperimentValidationError(
                "preregistration publication miss cannot bind a pushed receipt"
            )
        expected_check = preregistration_deadline_check_payload(
            attempt=result.attempt,
            preregistration_publication_deadline_ms=(
                result.preregistration_publication_deadline_ms
            ),
            checked_at_ms=checked_at_ms,
        )
        if canonical_json_bytes(check_payload) != canonical_json_bytes(
            expected_check
        ):
            raise ExperimentValidationError(
                "preregistration deadline check is not canonical"
            )
        return
    _require_binding_identity(
        check_binding,
        "holdout_receipt_deadline_check",
        "receipt_deadline_check",
    )
    if preregistration_context is None or result.preregistration_binding is None:
        raise ExperimentValidationError(
            "receipt deadline check requires the bound preregistration"
        )
    receipt_payload = None
    if receipt_binding is None:
        if pushed_receipt_artifact is not None:
            raise ExperimentValidationError(
                "result cannot receive unbound pushed-receipt contents"
            )
    else:
        if pushed_receipt_artifact is None:
            raise ExperimentValidationError(
                "result requires its canonical raw pushed receipt"
            )
        receipt_payload = _validate_bound_artifact_payload(
            pushed_receipt_artifact,
            binding=receipt_binding,
            field_name="pushed_receipt_artifact",
        )
        expected_receipt = pushed_preregistration_receipt_payload(
            attempt=result.attempt,
            preregistration=result.preregistration_binding,
            authoritative_remote_url_sha256=_require_sha256(
                preregistration_context.get(
                    "authoritative_remote_url_sha256"
                ),
                "preregistration_artifact.authoritative_remote_url_sha256",
            ),
            pushed_commit_id=receipt_payload.get("pushed_commit_id"),
            observed_remote_ref=receipt_payload.get("observed_remote_ref"),
            observed_remote_commit_id=receipt_payload.get(
                "observed_remote_commit_id"
            ),
            verified_at_ms=_require_int(
                receipt_payload.get("verified_at_ms"),
                "pushed_receipt_artifact.verified_at_ms",
                minimum=0,
            ),
        )
        if canonical_json_bytes(receipt_payload) != canonical_json_bytes(
            expected_receipt
        ):
            raise ExperimentValidationError(
                "pushed preregistration receipt is not canonical"
            )
    expected_check = receipt_deadline_check_payload(
        attempt=result.attempt,
        preregistration=result.preregistration_binding,
        authoritative_remote_url_sha256=_require_sha256(
            preregistration_context.get("authoritative_remote_url_sha256"),
            "preregistration_artifact.authoritative_remote_url_sha256",
        ),
        expected_remote_ref=_require_string(
            preregistration_context.get("authoritative_remote_ref"),
            "preregistration_artifact.authoritative_remote_ref",
        ),
        pushed_receipt_deadline_ms=_require_int(
            preregistration_context.get("pushed_receipt_deadline_ms"),
            "preregistration_artifact.pushed_receipt_deadline_ms",
            minimum=0,
        ),
        checked_at_ms=checked_at_ms,
        pushed_receipt=receipt_binding,
        pushed_receipt_payload_value=receipt_payload,
    )
    if canonical_json_bytes(check_payload) != canonical_json_bytes(
        expected_check
    ):
        raise ExperimentValidationError(
            "receipt deadline check is not canonical or transitively bound"
        )
    if result.failure_stage == "preregistration_lead":
        if check_payload.get("receipt_timely") is True:
            raise ExperimentValidationError(
                "preregistration-lead failure contradicts a timely receipt"
            )
    elif check_payload.get("receipt_timely") is not True:
        raise ExperimentValidationError(
            "post-lead holdout result requires a timely pushed receipt"
        )


def validate_terminal_result(
    value: object,
    *,
    expected_contract: V4ExperimentContract,
    preregistration_artifact: Optional[object] = None,
    selection_anchor_source_artifact: Optional[object] = None,
    selection_anchor_authorization_artifact: Optional[object] = None,
    calibration_efficacy_report_artifact: Optional[object] = None,
    calibration_completion_marker_artifact: Optional[object] = None,
    expected_calibration_efficacy_report: Optional[ArtifactBinding] = None,
    expected_calibration_completion_marker: Optional[ArtifactBinding] = None,
    successor_parent_result_artifact: Optional[object] = None,
    calibration_retry_eligibility_artifact: Optional[object] = None,
    calibration_successor_authorization_artifact: Optional[object] = None,
    calibration_parent_result_artifact: Optional[object] = None,
    pushed_receipt_artifact: Optional[object] = None,
    receipt_deadline_check_artifact: Optional[object] = None,
    expected_prior_evidence_artifacts: Optional[
        Sequence[InspectedEvidenceBinding]
    ] = None,
    expected_successor_parent_result: Optional[ArtifactBinding] = None,
    expected_retry_restoration_evidence: Optional[
        Sequence[ArtifactBinding]
    ] = None,
    expected_calibration_parent_result: Optional[ArtifactBinding] = None,
    expected_calibration_retry_restoration_evidence: Optional[
        Sequence[ArtifactBinding]
    ] = None,
    expected_calibration_authorization_provenance_root: Optional[str] = None,
    structural_infeasibility_report_artifact: Optional[object] = None,
    existing_terminal_experiment_ids: Sequence[Union[UUID, str]] = (),
    expected: Optional[V4TerminalResult] = None,
) -> Mapping[str, Any]:
    payload = _coerce_payload(value, "result")
    base_keys = {
        "artifact_type",
        "schema_version",
        "policy_version",
        "attempt",
        "experiment_contract",
        "experiment_contract_digest",
        "terminal_stage",
        "decision",
        "failure_stage",
        "failure_reasons",
        "parent_result",
        "ancestry",
        "retry_state",
        "selection_anchor_provenance",
        "candidate_day_ledger_root",
        "provenance_continuity_root",
        "preregistration_binding",
        "preregistration_publication_deadline_ms",
        "receipt_deadline_check",
        "pushed_receipt",
        "holdout_attempted",
        "calibration_efficacy_started",
        "calibration_efficacy_completed",
        "calibration_start_marker",
        "calibration_completion_marker",
        "holdout_efficacy_started",
        "holdout_efficacy_completed",
        "holdout_start_marker",
        "holdout_completion_marker",
        "terminal_efficacy_completed_at_ms",
        "efficacy_attempt_consumed",
        "evidence_artifacts",
        "provenance_checkpoint",
        "quality_evidence",
        "frozen_challenger",
        "created_at_ms",
    }
    allowed_keys = set(base_keys)
    if payload.get("decision") != "insufficient_evidence":
        allowed_keys.add("efficacy_evidence")
    if set(payload) != allowed_keys:
        raise ExperimentValidationError("result has unsupported fields")
    if payload["artifact_type"] != TERMINAL_RESULT_ARTIFACT_TYPE:
        raise ExperimentValidationError("result artifact type is invalid")
    if _require_int(
        payload["schema_version"], "result.schema_version"
    ) != EXPERIMENT_SCHEMA_VERSION:
        raise ExperimentValidationError("result schema is unsupported")
    if payload["policy_version"] != POLICY_VERSION:
        raise ExperimentValidationError("result policy is unsupported")
    expected_contract.validate_payload(payload["experiment_contract"])
    if payload["experiment_contract_digest"] != expected_contract.digest:
        raise ExperimentValidationError("result contract digest differs")
    attempt = AttemptIdentity.from_dict(payload["attempt"], "attempt")
    existing = {
        str(_require_uuid4(item, "existing_terminal_experiment_id"))
        for item in existing_terminal_experiment_ids
    }
    if str(attempt.experiment_id) in existing:
        raise ExperimentValidationError(
            "experiment_id already has a terminal result"
        )
    failure_reasons_raw = _as_list(payload["failure_reasons"], "failure_reasons")
    failure_reasons = tuple(
        _require_string(item, f"failure_reasons[{index}]")
        for index, item in enumerate(failure_reasons_raw)
    )
    ancestry = _validate_artifact_bindings(payload["ancestry"], "ancestry")
    evidence = _validate_artifact_bindings(
        payload["evidence_artifacts"], "evidence_artifacts"
    )
    challenger = (
        None
        if payload["frozen_challenger"] is None
        else ModelIdentity.from_dict(
            payload["frozen_challenger"], "frozen_challenger"
        )
    )
    result = V4TerminalResult(
        attempt=attempt,
        experiment_contract=expected_contract,
        terminal_stage=_require_string(
            payload["terminal_stage"], "terminal_stage"
        ),
        decision=_require_string(payload["decision"], "decision"),
        failure_stage=(
            None
            if payload["failure_stage"] is None
            else _require_string(payload["failure_stage"], "failure_stage")
        ),
        failure_reasons=failure_reasons,
        parent_result=_optional_binding(payload["parent_result"], "parent_result"),
        ancestry=ancestry,
        retry_state=_require_nonempty_mapping(payload["retry_state"], "retry_state"),
        selection_anchor_provenance=(
            None
            if payload["selection_anchor_provenance"] is None
            else SelectionAnchorProvenance.from_dict(
                payload["selection_anchor_provenance"],
                "selection_anchor_provenance",
            )
        ),
        candidate_day_ledger_root=_require_sha256(
            payload["candidate_day_ledger_root"], "candidate_day_ledger_root"
        ),
        provenance_continuity_root=_require_sha256(
            payload["provenance_continuity_root"],
            "provenance_continuity_root",
        ),
        preregistration_binding=_optional_binding(
            payload["preregistration_binding"], "preregistration_binding"
        ),
        preregistration_publication_deadline_ms=(
            None
            if payload["preregistration_publication_deadline_ms"] is None
            else _require_int(
                payload["preregistration_publication_deadline_ms"],
                "preregistration_publication_deadline_ms",
                minimum=0,
            )
        ),
        receipt_deadline_check=_optional_binding(
            payload["receipt_deadline_check"], "receipt_deadline_check"
        ),
        pushed_receipt=_optional_binding(
            payload["pushed_receipt"], "pushed_receipt"
        ),
        holdout_attempted=_require_bool(
            payload["holdout_attempted"], "holdout_attempted"
        ),
        calibration_efficacy_started=_require_bool(
            payload["calibration_efficacy_started"],
            "calibration_efficacy_started",
        ),
        calibration_efficacy_completed=_require_bool(
            payload["calibration_efficacy_completed"],
            "calibration_efficacy_completed",
        ),
        calibration_start_marker=_optional_binding(
            payload["calibration_start_marker"], "calibration_start_marker"
        ),
        calibration_completion_marker=_optional_binding(
            payload["calibration_completion_marker"],
            "calibration_completion_marker",
        ),
        holdout_efficacy_started=_require_bool(
            payload["holdout_efficacy_started"], "holdout_efficacy_started"
        ),
        holdout_efficacy_completed=_require_bool(
            payload["holdout_efficacy_completed"],
            "holdout_efficacy_completed",
        ),
        holdout_start_marker=_optional_binding(
            payload["holdout_start_marker"], "holdout_start_marker"
        ),
        holdout_completion_marker=_optional_binding(
            payload["holdout_completion_marker"], "holdout_completion_marker"
        ),
        terminal_efficacy_completed_at_ms=(
            None
            if payload["terminal_efficacy_completed_at_ms"] is None
            else _require_int(
                payload["terminal_efficacy_completed_at_ms"],
                "terminal_efficacy_completed_at_ms",
                minimum=0,
            )
        ),
        efficacy_attempt_consumed=_require_bool(
            payload["efficacy_attempt_consumed"],
            "efficacy_attempt_consumed",
        ),
        evidence_artifacts=evidence,
        provenance_checkpoint=ArtifactBinding.from_dict(
            payload["provenance_checkpoint"], "provenance_checkpoint"
        ),
        quality_evidence=_require_nonempty_mapping(
            payload["quality_evidence"], "quality_evidence"
        ),
        efficacy_evidence=(
            None
            if "efficacy_evidence" not in payload
            else _require_nonempty_mapping(
                payload["efficacy_evidence"], "efficacy_evidence"
            )
        ),
        frozen_challenger=challenger,
        created_at_ms=_require_int(
            payload["created_at_ms"], "created_at_ms", minimum=0
        ),
    )
    preregistration_context = (
        _coerce_payload(preregistration_artifact, "preregistration_artifact")
        if preregistration_artifact is not None
        else None
    )
    anchor_candidate_day_ledger_root = result.candidate_day_ledger_root
    anchor_provenance_continuity_root = result.provenance_continuity_root
    if preregistration_context is not None:
        anchor_candidate_day_ledger_root = _require_sha256(
            preregistration_context.get("candidate_day_ledger_root"),
            "preregistration_artifact.candidate_day_ledger_root",
        )
        anchor_provenance_continuity_root = _require_sha256(
            preregistration_context.get("provenance_continuity_root"),
            "preregistration_artifact.provenance_continuity_root",
        )
    _validate_terminal_receipt_artifacts(
        result=result,
        preregistration_context=preregistration_context,
        pushed_receipt_artifact=pushed_receipt_artifact,
        receipt_deadline_check_artifact=receipt_deadline_check_artifact,
    )
    if result.terminal_stage == "holdout":
        _validate_holdout_selection_anchor_artifacts(
            attempt=result.attempt,
            expected_contract=expected_contract,
            anchor=result.selection_anchor_provenance,
            challenger=result.frozen_challenger,
            source_artifact=selection_anchor_source_artifact,
            authorization_artifact=selection_anchor_authorization_artifact,
            calibration_report_artifact=calibration_efficacy_report_artifact,
            expected_calibration_report=expected_calibration_efficacy_report,
            expected_calibration_completion=(
                expected_calibration_completion_marker
            ),
            expected_candidate_day_ledger_root=(
                anchor_candidate_day_ledger_root
            ),
            expected_provenance_continuity_root=(
                anchor_provenance_continuity_root
            ),
        )
    if result.attempt.calibration_attempt_index == 1:
        calibration_ancestry = result.ancestry[:3]
        combined_successor = result.attempt.holdout_attempt_index == 1
        calibration_scope_bindings = {
            artifact.artifact_type: artifact
            for artifact in result.evidence_artifacts
        }
        if (
            result.terminal_stage == "holdout"
            and preregistration_context is not None
        ):
            calibration_scope_bindings.update(
                {
                    artifact.artifact_type: artifact
                    for artifact in _validate_artifact_bindings(
                        preregistration_context.get("calibration_artifacts"),
                        "preregistration_artifact.calibration_artifacts",
                    )
                }
            )
        _validate_authorized_successor_chain(
            attempt=_calibration_stage_attempt(
                AttemptIdentity.from_dict(
                    _coerce_payload(
                        successor_parent_result_artifact,
                        "successor_parent_result_artifact",
                    ).get("attempt"),
                    "successor_parent_result_artifact.attempt",
                )
                if combined_successor
                else result.attempt
            ),
            expected_contract=expected_contract,
            successor_stage="calibration",
            parent_binding=calibration_ancestry[0],
            retry_binding=calibration_ancestry[1],
            authorization_binding=calibration_ancestry[2],
            parent_artifact=(
                calibration_parent_result_artifact
                if combined_successor
                else successor_parent_result_artifact
            ),
            retry_artifact=calibration_retry_eligibility_artifact,
            authorization_artifact=(
                calibration_successor_authorization_artifact
            ),
            expected_parent_binding=(
                expected_calibration_parent_result
                if combined_successor
                else expected_successor_parent_result
            ),
            expected_restoration_evidence=(
                expected_calibration_retry_restoration_evidence
                if combined_successor
                else expected_retry_restoration_evidence
            ),
            expected_candidate_day_ledger_root=calibration_scope_bindings[
                "calibration_candidate_day_ledger"
            ].sha256,
            expected_provenance_continuity_root=(
                expected_calibration_authorization_provenance_root
            ),
            selected_window_binding=calibration_scope_bindings[
                "calibration_attempt_freeze"
            ],
            published_at_ms=result.created_at_ms,
        )
    if result.attempt.holdout_attempt_index == 1:
        holdout_ancestry = result.ancestry[-3:]
        _validate_authorized_successor_chain(
            attempt=result.attempt,
            expected_contract=expected_contract,
            successor_stage="holdout",
            parent_binding=holdout_ancestry[0],
            retry_binding=holdout_ancestry[1],
            authorization_binding=holdout_ancestry[2],
            parent_artifact=successor_parent_result_artifact,
            retry_artifact=selection_anchor_source_artifact,
            authorization_artifact=(
                selection_anchor_authorization_artifact
            ),
            expected_parent_binding=expected_successor_parent_result,
            expected_restoration_evidence=(
                expected_retry_restoration_evidence
            ),
            expected_candidate_day_ledger_root=(
                anchor_candidate_day_ledger_root
            ),
            expected_provenance_continuity_root=(
                anchor_provenance_continuity_root
            ),
            selected_window_binding=None,
            published_at_ms=result.created_at_ms,
        )
    if structural_infeasibility_report_artifact is not None:
        raise ExperimentValidationError(
            "structural infeasibility is not accepted without an independently "
            "derived feasibility proof"
        )
    if result.terminal_stage == "holdout" and (
        result.preregistration_binding is not None
    ):
        if preregistration_artifact is None:
            raise ExperimentValidationError(
                "holdout result requires its bound preregistration artifact"
            )
        if (
            selection_anchor_source_artifact is None
            or selection_anchor_authorization_artifact is None
        ):
            raise ExperimentValidationError(
                "holdout result requires the preregistration's bound "
                "selection-anchor artifacts"
            )
        preregistration_payload = _validate_bound_artifact_payload(
            preregistration_artifact,
            binding=result.preregistration_binding,
            field_name="preregistration_artifact",
        )
        validate_preregistration(
            preregistration_artifact,
            expected_contract=expected_contract,
            selection_anchor_source_artifact=(
                selection_anchor_source_artifact
            ),
            selection_anchor_authorization_artifact=(
                selection_anchor_authorization_artifact
            ),
            calibration_efficacy_report_artifact=(
                calibration_efficacy_report_artifact
            ),
            calibration_completion_marker_artifact=(
                calibration_completion_marker_artifact
            ),
            expected_calibration_efficacy_report=(
                expected_calibration_efficacy_report
            ),
            expected_calibration_completion_marker=(
                expected_calibration_completion_marker
            ),
            successor_parent_result_artifact=(
                successor_parent_result_artifact
            ),
            calibration_retry_eligibility_artifact=(
                calibration_retry_eligibility_artifact
            ),
            calibration_successor_authorization_artifact=(
                calibration_successor_authorization_artifact
            ),
            calibration_parent_result_artifact=(
                calibration_parent_result_artifact
            ),
            expected_prior_evidence_artifacts=(
                expected_prior_evidence_artifacts
            ),
            expected_successor_parent_result=(
                expected_successor_parent_result
            ),
            expected_retry_restoration_evidence=(
                expected_retry_restoration_evidence
            ),
            expected_calibration_parent_result=(
                expected_calibration_parent_result
            ),
            expected_calibration_retry_restoration_evidence=(
                expected_calibration_retry_restoration_evidence
            ),
            expected_calibration_authorization_provenance_root=(
                expected_calibration_authorization_provenance_root
            ),
        )
        if result.failure_stage == "preregistration_lead":
            minimum_result_time_ms = _require_int(
                preregistration_payload.get("pushed_receipt_deadline_ms"),
                "preregistration_artifact.pushed_receipt_deadline_ms",
                minimum=0,
            )
        else:
            archive_window = _as_mapping(
                preregistration_payload.get("archive"),
                "preregistration_artifact.archive",
            )
            minimum_result_time_ms = _require_int(
                archive_window.get("input_end_ms"),
                "preregistration_artifact.archive.input_end_ms",
                minimum=1,
            )
            if (
                result.holdout_efficacy_completed
                and result.terminal_efficacy_completed_at_ms
                < minimum_result_time_ms
            ):
                raise ExperimentValidationError(
                    "holdout efficacy completion predates the frozen archive "
                    "input tail"
                )
        if result.created_at_ms < minimum_result_time_ms:
            raise ExperimentValidationError(
                "holdout result predates its causally necessary boundary"
            )
        required_preregistration_values = {
            "attempt": result.attempt.to_dict(),
            "experiment_contract_digest": result.experiment_contract.digest,
            "selection_anchor_provenance": (
                result.selection_anchor_provenance.to_dict()
            ),
            "frozen_challenger": result.frozen_challenger.to_dict(),
            "candidate_day_ledger_root": result.candidate_day_ledger_root,
            "preregistration_publication_deadline_ms": (
                result.preregistration_publication_deadline_ms
            ),
        }
        for field_name, expected_value in required_preregistration_values.items():
            if canonical_json_bytes(preregistration_payload.get(field_name)) != (
                canonical_json_bytes(expected_value)
            ):
                raise ExperimentValidationError(
                    "terminal result differs from its bound preregistration"
                )
        if (
            result.calibration_start_marker is None
            or result.calibration_completion_marker is None
        ):
            raise ExperimentValidationError(
                "holdout result omits frozen calibration markers"
            )
        calibration_marker_hashes = {
            "calibration_efficacy_started_sha256": (
                result.calibration_start_marker.sha256
            ),
            "calibration_efficacy_completed_sha256": (
                result.calibration_completion_marker.sha256
            ),
        }
        for field_name, expected_hash in calibration_marker_hashes.items():
            if preregistration_payload.get(field_name) != expected_hash:
                raise ExperimentValidationError(
                    "terminal calibration markers differ from the bound "
                    "preregistration"
                )
        if (
            result.attempt.holdout_attempt_index == 1
            or result.attempt.calibration_attempt_index == 1
        ):
            prior_evidence = _as_mapping(
                preregistration_payload.get("prior_evidence"),
                "preregistration_artifact.prior_evidence",
            )
            expected_successor_ancestry = tuple(
                ArtifactBinding.from_dict(
                    prior_evidence.get(field_name),
                    f"preregistration_artifact.prior_evidence.{field_name}",
                )
                for field_name in (
                    "parent_result",
                    "retry_eligibility",
                    "successor_authorization",
                )
            )
            inherited_ancestry = (
                _validate_artifact_bindings(
                    prior_evidence.get("inherited_ancestry"),
                    "preregistration_artifact.prior_evidence.inherited_ancestry",
                )
                if result.attempt.holdout_attempt_index == 1
                else ()
            )
            expected_complete_ancestry = (
                *inherited_ancestry,
                *expected_successor_ancestry,
            )
            if tuple(result.ancestry) != expected_complete_ancestry:
                raise ExperimentValidationError(
                    "terminal successor ancestry differs from the bound "
                    "preregistration"
                )
            successor_bindings_differ = (
                result.parent_result != expected_successor_ancestry[0]
            )
            if result.attempt.holdout_attempt_index == 1:
                successor_bindings_differ = successor_bindings_differ or (
                    result.selection_anchor_provenance.source_artifact
                    != expected_successor_ancestry[1]
                    or result.selection_anchor_provenance.authorization_artifact
                    != expected_successor_ancestry[2]
                )
            if successor_bindings_differ:
                raise ExperimentValidationError(
                    "terminal successor bindings differ from the bound "
                    "preregistration"
                )
        origin_contract = _as_mapping(
            preregistration_payload.get("origin_contract"),
            "preregistration_artifact.origin_contract",
        )
        origin_binding_fields = {
            "scheduled_vector_binding": (
                "scheduled_vector_sha256_by_cell"
            ),
            "target_eligible_mask_binding": (
                "target_eligible_mask_sha256_by_cell"
            ),
            "target_eligible_vector_binding": (
                "target_eligible_vector_sha256_by_cell"
            ),
        }
        quality_cells = result.quality_evidence.get("cells", ())
        for cell_index, raw_cell in enumerate(quality_cells):
            cell = _as_mapping(
                raw_cell,
                f"quality_evidence.cells[{cell_index}]",
            )
            cell_id = _require_string(
                cell.get("cell_id"),
                f"quality_evidence.cells[{cell_index}].cell_id",
            )
            for binding_field, origin_hash_field in (
                origin_binding_fields.items()
            ):
                origin_hashes = _as_mapping(
                    origin_contract.get(origin_hash_field),
                    (
                        "preregistration_artifact.origin_contract."
                        f"{origin_hash_field}"
                    ),
                )
                binding_payload = _as_mapping(
                    cell.get(binding_field),
                    (
                        f"quality_evidence.cells[{cell_index}]."
                        f"{binding_field}"
                    ),
                )
                if binding_payload.get("sha256") != origin_hashes.get(cell_id):
                    raise ExperimentValidationError(
                        "terminal quality origins differ from the bound "
                        "preregistration"
                    )
    elif any(
        artifact is not None
        for artifact in (
            preregistration_artifact,
            calibration_completion_marker_artifact,
            expected_prior_evidence_artifacts,
        )
    ):
        raise ExperimentValidationError(
            "result cannot receive an unbound preregistration artifact"
        )
    if (
        result.terminal_stage == "calibration"
        and
        result.attempt.calibration_attempt_index == 0
        and result.attempt.holdout_attempt_index != 1
        and any(
            artifact is not None
            for artifact in (
                selection_anchor_source_artifact,
                selection_anchor_authorization_artifact,
                successor_parent_result_artifact,
                calibration_retry_eligibility_artifact,
                calibration_successor_authorization_artifact,
                calibration_parent_result_artifact,
                expected_successor_parent_result,
                expected_retry_restoration_evidence,
                expected_calibration_parent_result,
                expected_calibration_retry_restoration_evidence,
                expected_calibration_authorization_provenance_root,
            )
        )
    ):
        raise ExperimentValidationError(
            "initial result cannot receive unbound successor evidence"
        )
    if canonical_json_bytes(payload) != canonical_json_bytes(result.to_dict()):
        raise ExperimentValidationError(
            "result is not the canonical strict v4 schema"
        )
    if expected is not None and canonical_json_bytes(payload) != canonical_json_bytes(
        expected.to_dict()
    ):
        raise ExperimentValidationError(
            "result differs from the expected terminal artifact"
        )
    return payload


__all__ = [
    "ActiveIncumbentFreeze",
    "ArtifactBinding",
    "AttemptIdentity",
    "CALIBRATION_ATTEMPT_FREEZE_LEAD_MS",
    "COMPARISON_LAGS_MS",
    "ControlMode",
    "ExperimentValidationError",
    "FINALIZATION_ALLOWANCE_MS",
    "FROZEN_BOOTSTRAP_CONTRACT",
    "FROZEN_CALIBRATION_GATES",
    "FROZEN_HOLDOUT_GATES",
    "FROZEN_QUALITY_GATES",
    "ForecastCodeManifest",
    "ForecastConfig",
    "GUARDRAIL_LAG_MS",
    "IncumbentProvenanceError",
    "InspectedEvidenceBinding",
    "MINIMUM_PREREGISTRATION_LEAD_MS",
    "ModelIdentity",
    "POLICY_VERSION",
    "PREREGISTRATION_PUBLICATION_ALLOWANCE_MS",
    "PROMOTION_ELIGIBLE_LAGS_MS",
    "ReplacementControlResolution",
    "SelectionAnchorProvenance",
    "TimingCell",
    "V4ExperimentContract",
    "V4ForecastSettings",
    "V4Preregistration",
    "V4TerminalResult",
    "V4_TIMING_CELLS",
    "artifact_sha256",
    "calibration_selection_report_payload",
    "canonical_artifact_bytes",
    "canonical_json_bytes",
    "canonical_sha256",
    "decode_strict_json",
    "efficacy_completion_marker_payload",
    "forecast_config_digest",
    "forecast_config_payload",
    "non_lag_forecast_config_digest",
    "preregistration_deadline_check_payload",
    "pushed_preregistration_receipt_payload",
    "receipt_deadline_check_payload",
    "resolve_replacement_control",
    "terminal_efficacy_report_payload",
    "validate_preregistration",
    "validate_terminal_result",
]
