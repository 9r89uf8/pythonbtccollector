from dataclasses import dataclass
from decimal import Decimal, localcontext
from enum import Enum
import re
from typing import Any, Mapping, Optional, Sequence

from price_collector.market import MARKET_MS, MarketWindow, market_for_sample_second


SHADOW_EVALUATION_REPORT_SCHEMA_VERSION = 2
SHADOW_EVALUATION_CADENCE_MS = 500
SHADOW_EVALUATION_SCORED_INPUT_MAX_FUTURE_SKEW_MS = 0
SHADOW_EVALUATION_MAX_POINTS = 1_000
SHADOW_EVALUATION_QUERY_LIMIT = SHADOW_EVALUATION_MAX_POINTS + 1
POSTGRES_BIGINT_MAX = (1 << 63) - 1
MAX_MARKET_ID = (POSTGRES_BIGINT_MAX - MARKET_MS) // MARKET_MS
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
DECIMAL_TOLERANCE = Decimal("0.000000000000000002")
PENDING_BPS_TOLERANCE = Decimal("0.000000000000000010")
REPORT_DECIMAL_PRECISION = 80


class ShadowEvaluationModelVersion(str, Enum):
    CATCHUP_RATIO_L3000_B100 = "catchup_ratio_l3000_b100"
    CATCHUP_RATIO_L3500_B100 = "catchup_ratio_l3500_b100"
    CATCHUP_RATIO_L4000_B100 = "catchup_ratio_l4000_b100"


@dataclass(frozen=True)
class ShadowEvaluationModelSpec:
    model_version: str
    horizon_ms: int
    beta: Decimal


SHADOW_EVALUATION_MODEL_SPECS = {
    version.value: ShadowEvaluationModelSpec(
        model_version=version.value,
        horizon_ms=horizon_ms,
        beta=Decimal("1"),
    )
    for version, horizon_ms in (
        (ShadowEvaluationModelVersion.CATCHUP_RATIO_L3000_B100, 3_000),
        (ShadowEvaluationModelVersion.CATCHUP_RATIO_L3500_B100, 3_500),
        (ShadowEvaluationModelVersion.CATCHUP_RATIO_L4000_B100, 4_000),
    )
}


SHADOW_EVALUATION_MARKET_EXISTS_SQL = """
    SELECT EXISTS (
        SELECT 1
        FROM public.market_windows
        WHERE market_id = $1::BIGINT
    )
"""


SHADOW_EVALUATION_POINTS_SQL = """
    SELECT
        selection_schema_version,
        selection_policy_version,
        selection_evidence_end_ms,
        selection_fingerprint_sha256,
        selection_artifact_sha256,
        model_version,
        beta,
        generated_ms,
        target_ms,
        matured_ms,
        horizon_ms,
        valid,
        status,
        invalid_reasons,
        state,
        outcome_status,
        outcome_invalid_reasons,
        forecast_market_id,
        full_horizon_before_forecast_market_end,
        chainlink_at_forecast,
        chainlink_at_forecast_source_timestamp_ms,
        chainlink_at_forecast_received_ms,
        futures_at_forecast,
        futures_at_forecast_source_timestamp_ms,
        futures_at_forecast_received_ms,
        projected_chainlink,
        actual_chainlink,
        actual_chainlink_source_timestamp_ms,
        actual_chainlink_received_ms,
        actual_chainlink_age_at_target_ms,
        pending_move,
        pending_move_bps,
        direction,
        forecast_error,
        baseline_error
    FROM public.shadow_signal_evaluation_chart_points
    WHERE model_version = $2::TEXT
      AND forecast_market_id IN ($1::BIGINT, $1::BIGINT - 1)
      AND target_ms >= $3::BIGINT
      AND target_ms < $4::BIGINT
    ORDER BY target_ms ASC, generated_ms ASC, horizon_ms ASC
    LIMIT $5::INTEGER
"""


@dataclass(frozen=True)
class ShadowEvaluationFetchResult:
    market_exists: bool
    rows: tuple[Mapping[str, Any], ...]


class ShadowEvaluationReportingError(RuntimeError):
    """The reporting result cannot be represented by the public contract."""


def shadow_evaluation_market_window(market_id: int) -> MarketWindow:
    if isinstance(market_id, bool) or not isinstance(market_id, int):
        raise TypeError("market_id must be an integer")
    if market_id < 0 or market_id > MAX_MARKET_ID:
        raise ValueError("market_id is outside the PostgreSQL BIGINT range")
    return market_for_sample_second(market_id * MARKET_MS)


def shadow_evaluation_model_spec(
    model_version: str,
) -> ShadowEvaluationModelSpec:
    try:
        return SHADOW_EVALUATION_MODEL_SPECS[model_version]
    except KeyError as exc:
        raise ValueError("unsupported shadow evaluation model_version") from exc


async def fetch_shadow_evaluation_chart_points(
    pool: Any,
    *,
    window: MarketWindow,
    model_version: str,
) -> ShadowEvaluationFetchResult:
    shadow_evaluation_model_spec(model_version)

    async with pool.acquire() as connection:
        market_exists = await connection.fetchval(
            SHADOW_EVALUATION_MARKET_EXISTS_SQL,
            window.market_id,
        )
        rows = await connection.fetch(
            SHADOW_EVALUATION_POINTS_SQL,
            window.market_id,
            model_version,
            window.market_start_ms,
            window.market_end_ms,
            SHADOW_EVALUATION_QUERY_LIMIT,
        )

    return ShadowEvaluationFetchResult(
        market_exists=bool(market_exists),
        rows=tuple(dict(row) for row in rows),
    )


def _require_int(row: Mapping[str, Any], field: str) -> int:
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ShadowEvaluationReportingError(f"{field} must be an integer")
    return value


def _require_bool(row: Mapping[str, Any], field: str) -> bool:
    value = row[field]
    if not isinstance(value, bool):
        raise ShadowEvaluationReportingError(f"{field} must be a boolean")
    return value


def _require_string(row: Mapping[str, Any], field: str) -> str:
    value = row[field]
    if not isinstance(value, str) or not value.strip():
        raise ShadowEvaluationReportingError(f"{field} must be non-empty text")
    return value


def _decimal_value(
    row: Mapping[str, Any],
    field: str,
    *,
    required: bool = False,
) -> Optional[Decimal]:
    value = row[field]
    if value is None:
        if required:
            raise ShadowEvaluationReportingError(f"{field} must not be null")
        return None
    if not isinstance(value, Decimal):
        try:
            value = Decimal(str(value))
        except Exception as exc:
            raise ShadowEvaluationReportingError(
                f"{field} must be a decimal"
            ) from exc
    if not value.is_finite():
        raise ShadowEvaluationReportingError(f"{field} must be finite")
    return value


def _decimal_string(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else format(value, "f")


def _optional_non_negative_int(
    row: Mapping[str, Any],
    field: str,
) -> Optional[int]:
    value = row[field]
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ShadowEvaluationReportingError(
            f"{field} must be a non-negative integer or null"
        )
    return value


def _forecast_observation(
    row: Mapping[str, Any],
    *,
    value_field: str,
    source_timestamp_field: str,
    received_field: str,
    generated_ms: int,
    required: bool,
) -> tuple[Optional[Decimal], Optional[int], Optional[int]]:
    value = _decimal_value(row, value_field)
    source_timestamp_ms = _optional_non_negative_int(
        row,
        source_timestamp_field,
    )
    received_ms = _optional_non_negative_int(row, received_field)

    if value is None:
        if source_timestamp_ms is not None or received_ms is not None:
            raise ShadowEvaluationReportingError(
                f"null {value_field} has timing metadata"
            )
        if required:
            raise ShadowEvaluationReportingError(
                f"valid row has null {value_field}"
            )
        return None, None, None

    if value <= 0:
        raise ShadowEvaluationReportingError(f"{value_field} must be positive")
    if received_ms is None:
        raise ShadowEvaluationReportingError(
            f"{value_field} has no received timestamp"
        )
    if required and received_ms > generated_ms:
        raise ShadowEvaluationReportingError(
            f"{value_field} received time is after generated_ms"
        )
    return value, source_timestamp_ms, received_ms


def _reason_list(row: Mapping[str, Any], field: str) -> list[str]:
    value = row[field]
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ShadowEvaluationReportingError(
            f"{field} must be a sequence"
        )
    reasons = list(value)
    if any(not isinstance(reason, str) or not reason for reason in reasons):
        raise ShadowEvaluationReportingError(
            f"{field} must contain non-empty text"
        )
    return reasons


def _require_sha256(row: Mapping[str, Any], field: str) -> str:
    value = _require_string(row, field)
    if HEX_SHA256.fullmatch(value) is None:
        raise ShadowEvaluationReportingError(f"{field} must be lowercase SHA-256")
    return value


def _validate_error(
    *,
    field: str,
    observed: Optional[Decimal],
    left: Optional[Decimal],
    actual: Optional[Decimal],
) -> None:
    if left is None or actual is None:
        if observed is not None:
            raise ShadowEvaluationReportingError(
                f"{field} must be null without both inputs"
            )
        return
    if observed is None:
        raise ShadowEvaluationReportingError(f"{field} is inconsistent")
    with localcontext() as context:
        context.prec = REPORT_DECIMAL_PRECISION
        inconsistent = abs(observed - (left - actual)) > DECIMAL_TOLERANCE
    if inconsistent:
        raise ShadowEvaluationReportingError(f"{field} is inconsistent")


def _serialize_shadow_evaluation_point(
    row: Mapping[str, Any],
    *,
    window: MarketWindow,
    spec: ShadowEvaluationModelSpec,
) -> dict[str, Any]:
    selection_schema_version = _require_int(row, "selection_schema_version")
    selection_policy_version = _require_string(
        row,
        "selection_policy_version",
    )
    selection_evidence_end_ms = _require_int(
        row,
        "selection_evidence_end_ms",
    )
    fingerprint = _require_sha256(row, "selection_fingerprint_sha256")
    artifact = _require_sha256(row, "selection_artifact_sha256")
    model_version = _require_string(row, "model_version")
    beta = _decimal_value(row, "beta", required=True)
    generated_ms = _require_int(row, "generated_ms")
    target_ms = _require_int(row, "target_ms")
    matured_ms = _require_int(row, "matured_ms")
    horizon_ms = _require_int(row, "horizon_ms")
    valid = _require_bool(row, "valid")
    status = _require_string(row, "status")
    invalid_reasons = _reason_list(row, "invalid_reasons")
    state = _require_string(row, "state")
    outcome_status = _require_string(row, "outcome_status")
    outcome_invalid_reasons = _reason_list(
        row,
        "outcome_invalid_reasons",
    )
    forecast_market_id = _require_int(row, "forecast_market_id")
    full_horizon = _require_bool(
        row,
        "full_horizon_before_forecast_market_end",
    )

    if selection_schema_version <= 0:
        raise ShadowEvaluationReportingError(
            "selection_schema_version must be positive"
        )
    if (
        selection_evidence_end_ms < 0
        or selection_evidence_end_ms > generated_ms
    ):
        raise ShadowEvaluationReportingError(
            "selection_evidence_end_ms is inconsistent"
        )
    if model_version != spec.model_version:
        raise ShadowEvaluationReportingError("model_version does not match request")
    if horizon_ms != spec.horizon_ms:
        raise ShadowEvaluationReportingError("horizon_ms does not match model")
    if beta != spec.beta:
        raise ShadowEvaluationReportingError("beta does not match model")
    if generated_ms < 0:
        raise ShadowEvaluationReportingError("generated_ms must be non-negative")
    if target_ms != generated_ms + horizon_ms:
        raise ShadowEvaluationReportingError("target_ms is inconsistent")
    if matured_ms < target_ms:
        raise ShadowEvaluationReportingError("matured_ms precedes target_ms")
    if not window.market_start_ms <= target_ms < window.market_end_ms:
        raise ShadowEvaluationReportingError("target_ms is outside the market")
    if forecast_market_id not in {window.market_id, window.market_id - 1}:
        raise ShadowEvaluationReportingError("forecast_market_id is outside query scope")
    if generated_ms // MARKET_MS != forecast_market_id:
        raise ShadowEvaluationReportingError("forecast_market_id is inconsistent")
    expected_full_horizon = target_ms <= (forecast_market_id + 1) * MARKET_MS
    if full_horizon is not expected_full_horizon:
        raise ShadowEvaluationReportingError(
            "full_horizon_before_forecast_market_end is inconsistent"
        )
    if valid != (status == "valid"):
        raise ShadowEvaluationReportingError("valid and status disagree")
    if valid and invalid_reasons:
        raise ShadowEvaluationReportingError("valid row has invalid reasons")
    if not valid and not invalid_reasons:
        raise ShadowEvaluationReportingError("invalid row has no reason")

    (
        chainlink_at_forecast,
        chainlink_at_forecast_source_ms,
        chainlink_at_forecast_received_ms,
    ) = _forecast_observation(
        row,
        value_field="chainlink_at_forecast",
        source_timestamp_field="chainlink_at_forecast_source_timestamp_ms",
        received_field="chainlink_at_forecast_received_ms",
        generated_ms=generated_ms,
        required=valid,
    )
    (
        futures_at_forecast,
        futures_at_forecast_source_ms,
        futures_at_forecast_received_ms,
    ) = _forecast_observation(
        row,
        value_field="futures_at_forecast",
        source_timestamp_field="futures_at_forecast_source_timestamp_ms",
        received_field="futures_at_forecast_received_ms",
        generated_ms=generated_ms,
        required=valid,
    )
    projected_chainlink = _decimal_value(row, "projected_chainlink")
    actual_chainlink = _decimal_value(row, "actual_chainlink")
    pending_move = _decimal_value(row, "pending_move")
    pending_move_bps = _decimal_value(row, "pending_move_bps")
    direction = row["direction"]
    forecast_error = _decimal_value(row, "forecast_error")
    baseline_error = _decimal_value(row, "baseline_error")

    if actual_chainlink is not None and actual_chainlink <= 0:
        raise ShadowEvaluationReportingError("actual_chainlink must be positive")

    if valid:
        for field, value in (
            ("projected_chainlink", projected_chainlink),
            ("pending_move", pending_move),
            ("pending_move_bps", pending_move_bps),
        ):
            if value is None:
                raise ShadowEvaluationReportingError(
                    f"valid row has null {field}"
                )
        if chainlink_at_forecast <= 0 or projected_chainlink <= 0:
            raise ShadowEvaluationReportingError(
                "valid projection prices must be positive"
            )
        if not isinstance(direction, str) or direction not in {
            "up",
            "down",
            "flat",
        }:
            raise ShadowEvaluationReportingError("valid row has invalid direction")
        with localcontext() as context:
            context.prec = REPORT_DECIMAL_PRECISION
            pending_move_inconsistent = (
                abs(
                    pending_move
                    - (projected_chainlink - chainlink_at_forecast)
                )
                > DECIMAL_TOLERANCE
            )
            pending_bps_inconsistent = (
                abs(
                    pending_move_bps
                    - (
                        pending_move
                        * Decimal("10000")
                        / chainlink_at_forecast
                    )
                )
                > PENDING_BPS_TOLERANCE
            )
        if pending_move_inconsistent:
            raise ShadowEvaluationReportingError("pending_move is inconsistent")
        if pending_bps_inconsistent:
            raise ShadowEvaluationReportingError("pending_move_bps is inconsistent")
        expected_direction = (
            "up" if pending_move > 0 else "down" if pending_move < 0 else "flat"
        )
        if direction != expected_direction:
            raise ShadowEvaluationReportingError("direction is inconsistent")
    elif any(
        value is not None
        for value in (
            projected_chainlink,
            pending_move,
            pending_move_bps,
            direction,
        )
    ):
        raise ShadowEvaluationReportingError(
            "invalid row contains projection output"
        )

    actual_source_ms = row["actual_chainlink_source_timestamp_ms"]
    actual_received_ms = row["actual_chainlink_received_ms"]
    actual_age_ms = row["actual_chainlink_age_at_target_ms"]
    if actual_chainlink is None:
        if any(
            value is not None
            for value in (actual_source_ms, actual_received_ms, actual_age_ms)
        ):
            raise ShadowEvaluationReportingError(
                "null actual has timing metadata"
            )
    else:
        if actual_source_ms is not None and (
            isinstance(actual_source_ms, bool)
            or not isinstance(actual_source_ms, int)
            or actual_source_ms < 0
        ):
            raise ShadowEvaluationReportingError("actual source time is invalid")
        if (
            isinstance(actual_received_ms, bool)
            or not isinstance(actual_received_ms, int)
            or actual_received_ms < 0
            or actual_received_ms > target_ms
        ):
            raise ShadowEvaluationReportingError("actual received time is invalid")
        if (
            isinstance(actual_age_ms, bool)
            or not isinstance(actual_age_ms, int)
            or actual_age_ms != target_ms - actual_received_ms
        ):
            raise ShadowEvaluationReportingError("actual age is inconsistent")

    if outcome_status == "available":
        if actual_chainlink is None:
            raise ShadowEvaluationReportingError(
                "available outcome has no actual"
            )
        if outcome_invalid_reasons:
            raise ShadowEvaluationReportingError(
                "available outcome has invalid reasons"
            )
    elif outcome_status == "unavailable":
        if actual_chainlink is not None:
            raise ShadowEvaluationReportingError(
                "unavailable outcome has an actual"
            )
        if outcome_invalid_reasons:
            raise ShadowEvaluationReportingError(
                "unavailable outcome has invalid reasons"
            )
    elif outcome_status == "integrity_invalid":
        if actual_chainlink is not None:
            raise ShadowEvaluationReportingError(
                "integrity-invalid outcome has an actual"
            )
        if not outcome_invalid_reasons:
            raise ShadowEvaluationReportingError(
                "integrity-invalid outcome has no reason"
            )
    else:
        raise ShadowEvaluationReportingError("outcome_status is unsupported")

    _validate_error(
        field="forecast_error",
        observed=forecast_error,
        left=projected_chainlink,
        actual=actual_chainlink,
    )
    _validate_error(
        field="baseline_error",
        observed=baseline_error,
        left=chainlink_at_forecast,
        actual=actual_chainlink,
    )

    return {
        "selection_schema_version": selection_schema_version,
        "selection_policy_version": selection_policy_version,
        "selection_evidence_end_ms": selection_evidence_end_ms,
        "selection_fingerprint_sha256": fingerprint,
        "selection_artifact_sha256": artifact,
        "model_version": model_version,
        "beta": _decimal_string(beta),
        "generated_ms": generated_ms,
        "target_ms": target_ms,
        "matured_ms": matured_ms,
        "horizon_ms": horizon_ms,
        "valid": valid,
        "status": status,
        "invalid_reasons": invalid_reasons,
        "state": state,
        "outcome_status": outcome_status,
        "outcome_invalid_reasons": outcome_invalid_reasons,
        "forecast_market_id": forecast_market_id,
        "full_horizon_before_forecast_market_end": full_horizon,
        "chainlink_at_forecast": _decimal_string(chainlink_at_forecast),
        "chainlink_at_forecast_source_timestamp_ms": (
            chainlink_at_forecast_source_ms
        ),
        "chainlink_at_forecast_received_ms": chainlink_at_forecast_received_ms,
        "futures_at_forecast": _decimal_string(futures_at_forecast),
        "futures_at_forecast_source_timestamp_ms": (
            futures_at_forecast_source_ms
        ),
        "futures_at_forecast_received_ms": futures_at_forecast_received_ms,
        "projected_chainlink": _decimal_string(projected_chainlink),
        "actual_chainlink": _decimal_string(actual_chainlink),
        "actual_chainlink_source_timestamp_ms": actual_source_ms,
        "actual_chainlink_received_ms": actual_received_ms,
        "actual_chainlink_age_at_target_ms": actual_age_ms,
        "pending_move": _decimal_string(pending_move),
        "pending_move_bps": _decimal_string(pending_move_bps),
        "direction": direction,
        "forecast_error": _decimal_string(forecast_error),
        "baseline_error": _decimal_string(baseline_error),
    }


def _median(values: Sequence[Decimal]) -> Decimal:
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / Decimal("2")


def _nearest_rank_p95(values: Sequence[Decimal]) -> Decimal:
    rank = (95 * len(values) + 99) // 100
    return values[rank - 1]


def _build_shadow_evaluation_performance_cohort(
    *,
    selection_schema_version: int,
    selection_policy_version: str,
    selection_evidence_end_ms: int,
    selection_fingerprint_sha256: str,
    selection_artifact_sha256: str,
    observations: Sequence[tuple[Decimal, Decimal]],
) -> dict[str, Any]:
    scored_points = len(observations)
    if scored_points == 0:
        return {
            "selection_identity": {
                "schema_version": selection_schema_version,
                "policy_version": selection_policy_version,
                "evidence_end_ms": selection_evidence_end_ms,
                "fingerprint_sha256": selection_fingerprint_sha256,
                "artifact_sha256": selection_artifact_sha256,
            },
            "scored_points": 0,
            "forecast": {
                "mean_absolute_error_usd": None,
                "median_absolute_error_usd": None,
                "p95_absolute_error_usd": None,
                "maximum_absolute_error_usd": None,
                "root_mean_squared_error_usd": None,
                "mean_signed_error_usd": None,
            },
            "no_change_baseline": {
                "mean_absolute_error_usd": None,
                "root_mean_squared_error_usd": None,
            },
            "mean_absolute_advantage_usd": None,
            "mae_skill_vs_no_change": None,
            "rmse_skill_vs_no_change": None,
            "paired_comparison": {
                "wins": 0,
                "ties": 0,
                "losses": 0,
                "win_rate": None,
                "tie_rate": None,
                "loss_rate": None,
            },
        }

    with localcontext() as context:
        context.prec = REPORT_DECIMAL_PRECISION
        count = Decimal(scored_points)
        forecast_errors = [forecast for forecast, _baseline in observations]
        baseline_errors = [baseline for _forecast, baseline in observations]
        forecast_absolute_errors = sorted(abs(error) for error in forecast_errors)
        baseline_absolute_errors = sorted(abs(error) for error in baseline_errors)

        forecast_mae = sum(
            forecast_absolute_errors,
            start=Decimal("0"),
        ) / count
        baseline_mae = sum(
            baseline_absolute_errors,
            start=Decimal("0"),
        ) / count
        forecast_rmse = (
            sum(
                (error * error for error in forecast_errors),
                start=Decimal("0"),
            )
            / count
        ).sqrt()
        baseline_rmse = (
            sum(
                (error * error for error in baseline_errors),
                start=Decimal("0"),
            )
            / count
        ).sqrt()
        forecast_mean_signed_error = sum(
            forecast_errors,
            start=Decimal("0"),
        ) / count
        forecast_median = _median(forecast_absolute_errors)
        forecast_p95 = _nearest_rank_p95(forecast_absolute_errors)
        mean_absolute_advantage = baseline_mae - forecast_mae
        mae_skill = (
            None
            if baseline_mae == 0
            else Decimal("1") - (forecast_mae / baseline_mae)
        )
        rmse_skill = (
            None
            if baseline_rmse == 0
            else Decimal("1") - (forecast_rmse / baseline_rmse)
        )

        wins = sum(
            abs(forecast) < abs(baseline)
            for forecast, baseline in observations
        )
        ties = sum(
            abs(forecast) == abs(baseline)
            for forecast, baseline in observations
        )
        losses = scored_points - wins - ties

        win_rate = Decimal(wins) / count
        tie_rate = Decimal(ties) / count
        loss_rate = Decimal(losses) / count

    return {
        "selection_identity": {
            "schema_version": selection_schema_version,
            "policy_version": selection_policy_version,
            "evidence_end_ms": selection_evidence_end_ms,
            "fingerprint_sha256": selection_fingerprint_sha256,
            "artifact_sha256": selection_artifact_sha256,
        },
        "scored_points": scored_points,
        "forecast": {
            "mean_absolute_error_usd": _decimal_string(forecast_mae),
            "median_absolute_error_usd": _decimal_string(forecast_median),
            "p95_absolute_error_usd": _decimal_string(forecast_p95),
            "maximum_absolute_error_usd": _decimal_string(
                forecast_absolute_errors[-1]
            ),
            "root_mean_squared_error_usd": _decimal_string(forecast_rmse),
            "mean_signed_error_usd": _decimal_string(
                forecast_mean_signed_error
            ),
        },
        "no_change_baseline": {
            "mean_absolute_error_usd": _decimal_string(baseline_mae),
            "root_mean_squared_error_usd": _decimal_string(baseline_rmse),
        },
        "mean_absolute_advantage_usd": _decimal_string(
            mean_absolute_advantage
        ),
        "mae_skill_vs_no_change": _decimal_string(mae_skill),
        "rmse_skill_vs_no_change": _decimal_string(rmse_skill),
        "paired_comparison": {
            "wins": wins,
            "ties": ties,
            "losses": losses,
            "win_rate": _decimal_string(win_rate),
            "tie_rate": _decimal_string(tie_rate),
            "loss_rate": _decimal_string(loss_rate),
        },
    }


def _build_shadow_evaluation_performance(
    points: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    observations_by_identity: dict[
        tuple[int, str, int, str, str],
        list[tuple[Decimal, Decimal]],
    ] = {}

    for point in points:
        identity = (
            point["selection_schema_version"],
            point["selection_policy_version"],
            point["selection_evidence_end_ms"],
            point["selection_fingerprint_sha256"],
            point["selection_artifact_sha256"],
        )
        observations = observations_by_identity.setdefault(identity, [])
        if not point["valid"] or point["outcome_status"] != "available":
            continue

        forecast_error = point["forecast_error"]
        baseline_error = point["baseline_error"]
        if forecast_error is None or baseline_error is None:
            raise ShadowEvaluationReportingError(
                "scored point has null error fields"
            )
        observations.append(
            (Decimal(forecast_error), Decimal(baseline_error))
        )

    return {
        "cohorts": [
            _build_shadow_evaluation_performance_cohort(
                selection_schema_version=schema_version,
                selection_policy_version=policy_version,
                selection_evidence_end_ms=evidence_end_ms,
                selection_fingerprint_sha256=fingerprint,
                selection_artifact_sha256=artifact,
                observations=observations_by_identity[
                    (
                        schema_version,
                        policy_version,
                        evidence_end_ms,
                        fingerprint,
                        artifact,
                    )
                ],
            )
            for (
                schema_version,
                policy_version,
                evidence_end_ms,
                fingerprint,
                artifact,
            ) in sorted(observations_by_identity)
        ]
    }


def build_shadow_evaluation_report(
    *,
    window: MarketWindow,
    server_time_ms: int,
    model_version: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if isinstance(server_time_ms, bool) or not isinstance(server_time_ms, int):
        raise TypeError("server_time_ms must be an integer")
    if server_time_ms < 0:
        raise ValueError("server_time_ms must be non-negative")
    if len(rows) > SHADOW_EVALUATION_MAX_POINTS:
        raise ShadowEvaluationReportingError(
            "shadow evaluation result exceeds 1000 rows"
        )

    spec = shadow_evaluation_model_spec(model_version)
    points = [
        _serialize_shadow_evaluation_point(row, window=window, spec=spec)
        for row in rows
    ]
    points.sort(
        key=lambda point: (
            point["target_ms"],
            point["generated_ms"],
            point["horizon_ms"],
        )
    )

    bucket_ids = {
        point["generated_ms"] // SHADOW_EVALUATION_CADENCE_MS
        for point in points
    }
    window_buckets = MARKET_MS // SHADOW_EVALUATION_CADENCE_MS
    if len(bucket_ids) > window_buckets:
        raise ShadowEvaluationReportingError(
            "shadow evaluation result exceeds the market bucket count"
        )

    valid_forecasts = sum(point["valid"] for point in points)
    invalid = len(points) - valid_forecasts
    scored = sum(
        point["valid"] and point["outcome_status"] == "available"
        for point in points
    )
    valid_without_actual = valid_forecasts - scored
    market_window_elapsed = server_time_ms >= window.market_end_ms

    selection_identities = sorted(
        {
            (
                point["selection_schema_version"],
                point["selection_policy_version"],
                point["selection_evidence_end_ms"],
                point["selection_fingerprint_sha256"],
                point["selection_artifact_sha256"],
            )
            for point in points
        }
    )
    performance = _build_shadow_evaluation_performance(points)
    performance_scored = sum(
        cohort["scored_points"] for cohort in performance["cohorts"]
    )
    if performance_scored != scored:
        raise ShadowEvaluationReportingError(
            "performance scored count is inconsistent"
        )

    return {
        "schema_version": SHADOW_EVALUATION_REPORT_SCHEMA_VERSION,
        "server_time_ms": server_time_ms,
        "market": {
            "market_id": window.market_id,
            "market_start_ms": window.market_start_ms,
            "market_end_ms": window.market_end_ms,
            "boundary": "[start_ms,end_ms)",
        },
        "evaluation_semantics": {
            "scored_input_max_future_skew_ms": (
                SHADOW_EVALUATION_SCORED_INPUT_MAX_FUTURE_SKEW_MS
            ),
        },
        "model": {
            "model_version": spec.model_version,
            "horizon_ms": spec.horizon_ms,
            "beta": _decimal_string(spec.beta),
            "evaluation_cadence_ms": SHADOW_EVALUATION_CADENCE_MS,
            "selection_identities": [
                {
                    "schema_version": schema_version,
                    "policy_version": policy_version,
                    "evidence_end_ms": evidence_end_ms,
                    "fingerprint_sha256": fingerprint,
                    "artifact_sha256": artifact,
                }
                for (
                    schema_version,
                    policy_version,
                    evidence_end_ms,
                    fingerprint,
                    artifact,
                ) in selection_identities
            ],
        },
        "coverage": {
            "window_buckets": window_buckets,
            "market_window_elapsed": market_window_elapsed,
            "observed_buckets": len(bucket_ids),
            "unobserved_buckets_as_of_response": (
                window_buckets - len(bucket_ids)
                if market_window_elapsed
                else None
            ),
            "attempts": len(points),
            "valid_forecasts": valid_forecasts,
            "scored": scored,
            "invalid": invalid,
            "valid_without_actual": valid_without_actual,
        },
        "performance": performance,
        "points": points,
    }
