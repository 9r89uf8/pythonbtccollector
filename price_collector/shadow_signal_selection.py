from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


SELECTION_SCHEMA_VERSION = 3
SUPPORTED_REPLAY_SCHEMA_VERSION = 3
SELECTION_POLICY_VERSION = "chronological_holdout_v3"
COMMON_COHORT_DEFINITION = (
    "same_generated_ms_max_horizon_eligible_all_models_valid"
)
EXPECTED_CANDIDATES = (
    ("catchup_ratio_l3000_b100", 3_000),
    ("catchup_ratio_l3500_b100", 3_500),
    ("catchup_ratio_l4000_b100", 4_000),
)
EXPECTED_LAGS_MS = tuple(horizon for _version, horizon in EXPECTED_CANDIDATES)
EXPECTED_BETA = Decimal("1")
DIRECTION_LABELS = ("up", "neutral", "down")
DIRECTIONAL_COUNT_KEYS = (
    "three_class_correct",
    "predicted_actions",
    "actual_moves",
    "actual_neutral",
    "correct_actions",
    "false_actions_on_neutral",
    "opposite_direction_actions",
)
DIRECTIONAL_RATE_KEYS = (
    "three_class_accuracy",
    "action_precision",
    "move_recall",
    "false_action_rate_on_neutral",
    "opposite_direction_rate_on_actual_moves",
    "predicted_action_frequency",
)
DirectionalMatrix = tuple[tuple[int, int, int], tuple[int, int, int], tuple[int, int, int]]

# These are project policy-v3 thresholds, not claims of statistical
# significance and not values supplied by engine.md. Changing any one requires
# a new policy version so a holdout cannot be tuned after its results are seen.
MIN_COMMON_SCORED_PER_REPORT = 10_000
MIN_COMMON_VALID_COVERAGE = Decimal("0.50")
MIN_COMMON_MATURATION_COVERAGE = Decimal("0.99")
MIN_SLICE_SCORED_FOR_WARNING = 500
MIN_MAE_SKILL_EXCLUSIVE = Decimal("0")
MIN_RMSE_SKILL_EXCLUSIVE = Decimal("0")
MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_DECIMAL_CHARACTERS = 256
DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")

SLICE_DIMENSIONS = (
    "actual_direction",
    "actual_move_size",
    "raw_bucket_return_rms_regime",
    "market_expiry",
    "session_boundary_proximity",
)
SLICE_CATEGORIES = {
    "actual_direction": ("down", "neutral", "up"),
    "actual_move_size": ("small", "medium", "large"),
    "raw_bucket_return_rms_regime": ("unknown", "low", "medium", "high"),
    "market_expiry": (
        "regular",
        "near_market_end",
        "horizon_crosses_market_end",
    ),
    "session_boundary_proximity": (
        "stable_segment",
        "post_segment_start",
        "pre_segment_end",
        "near_both_segment_boundaries",
    ),
}
POSITIVE_CONFIGURATION_INTS = (
    "poll_ms",
    "evaluation_interval_ms",
    "futures_stale_ms",
    "chainlink_stale_ms",
    "history_retention_ms",
    "volatility_lookback_ms",
    "near_expiry_ms",
    "near_reconnect_ms",
    "quantile_sample_max",
)
NON_NEGATIVE_CONFIGURATION_INTS = (
    "reference_max_gap_ms",
    "max_future_skew_ms",
    "futures_availability_delay_ms",
    "chainlink_availability_delay_ms",
    "evaluation_phase_offset_ms",
)


class SelectionInputError(ValueError):
    pass


def _as_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise SelectionInputError(f"{field_name} must be an object")
    return value


def _require_exact_keys(
    payload: Mapping[str, Any],
    expected: Sequence[str],
    field_name: str,
) -> None:
    if set(payload) != set(expected):
        raise SelectionInputError(f"{field_name} has unsupported fields")


def _as_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise SelectionInputError(f"{field_name} must be an array")
    return value


def _as_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise SelectionInputError(f"{field_name} must be a non-empty string")
    return value


def _as_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise SelectionInputError(f"{field_name} must be a boolean")
    return value


def _as_int(
    value: Any,
    field_name: str,
    *,
    minimum: Optional[int] = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SelectionInputError(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise SelectionInputError(
            f"{field_name} must be at least {minimum}"
        )
    return value


def _as_decimal(value: Any, field_name: str) -> Decimal:
    if not isinstance(value, str):
        raise SelectionInputError(
            f"{field_name} must be a JSON string containing a decimal"
        )
    if (
        len(value) > MAX_DECIMAL_CHARACTERS
        or DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise SelectionInputError(
            f"{field_name} must be a bounded fixed-point decimal"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise SelectionInputError(f"{field_name} is not a decimal") from exc
    if not parsed.is_finite():
        raise SelectionInputError(f"{field_name} must be finite")
    return parsed


def _decimal_sqrt(value: Decimal) -> Decimal:
    if value < 0:
        raise SelectionInputError("cannot take the square root of a negative")
    with localcontext() as context:
        context.prec = 50
        return value.sqrt()


def _replay_mean(total: Decimal, count: int) -> Decimal:
    with localcontext() as context:
        context.prec = 28
        return total / Decimal(count)


def _replay_rmse(total_squared_error: Decimal, count: int) -> Decimal:
    with localcontext() as context:
        context.prec = 28
        mean_square = total_squared_error / Decimal(count)
    return _decimal_sqrt(mean_square)


def _replay_mae_skill(
    model_mae: Decimal,
    baseline_mae: Decimal,
) -> Optional[Decimal]:
    if baseline_mae == 0:
        return None
    with localcontext() as context:
        context.prec = 28
        return Decimal("1") - model_mae / baseline_mae


def _require_reported_decimal(
    payload: Mapping[str, Any],
    key: str,
    expected: Optional[Decimal],
    field_name: str,
) -> None:
    raw_value = payload.get(key)
    if expected is None:
        if raw_value is not None:
            raise SelectionInputError(f"{field_name}.{key} must be null")
        return
    actual = _as_decimal(raw_value, f"{field_name}.{key}")
    if actual != expected:
        raise SelectionInputError(
            f"{field_name}.{key} is inconsistent with sufficient statistics"
        )


def _decimal_ratio(
    numerator: Decimal | int,
    denominator: Decimal | int,
) -> Optional[Decimal]:
    if denominator == 0:
        return None
    with localcontext() as context:
        context.prec = 50
        return Decimal(numerator) / Decimal(denominator)


def _directional_summary(
    matrix: DirectionalMatrix,
    *,
    count: int,
) -> dict[str, Any]:
    cells = {
        (actual, predicted): matrix[actual_index][predicted_index]
        for actual_index, actual in enumerate(DIRECTION_LABELS)
        for predicted_index, predicted in enumerate(DIRECTION_LABELS)
    }
    matrix_count = sum(cells.values())
    if matrix_count != count:
        raise SelectionInputError(
            "directional confusion matrix count differs from metric count"
        )

    correct_actions = cells[("up", "up")] + cells[("down", "down")]
    three_class_correct = correct_actions + cells[("neutral", "neutral")]
    predicted_actions = sum(
        cells[(actual, predicted)]
        for actual in DIRECTION_LABELS
        for predicted in ("up", "down")
    )
    actual_moves = sum(
        cells[(actual, predicted)]
        for actual in ("up", "down")
        for predicted in DIRECTION_LABELS
    )
    actual_neutral = sum(
        cells[("neutral", predicted)] for predicted in DIRECTION_LABELS
    )
    false_actions_on_neutral = (
        cells[("neutral", "up")] + cells[("neutral", "down")]
    )
    opposite_direction_actions = (
        cells[("up", "down")] + cells[("down", "up")]
    )
    return {
        "confusion_matrix": {
            f"actual_{actual}": {
                f"predicted_{predicted}": cells[(actual, predicted)]
                for predicted in DIRECTION_LABELS
            }
            for actual in DIRECTION_LABELS
        },
        "counts": {
            "three_class_correct": three_class_correct,
            "predicted_actions": predicted_actions,
            "actual_moves": actual_moves,
            "actual_neutral": actual_neutral,
            "correct_actions": correct_actions,
            "false_actions_on_neutral": false_actions_on_neutral,
            "opposite_direction_actions": opposite_direction_actions,
        },
        "rates": {
            "three_class_accuracy": _decimal_ratio(three_class_correct, count),
            "action_precision": _decimal_ratio(
                correct_actions,
                predicted_actions,
            ),
            "move_recall": _decimal_ratio(correct_actions, actual_moves),
            "false_action_rate_on_neutral": _decimal_ratio(
                false_actions_on_neutral,
                actual_neutral,
            ),
            "opposite_direction_rate_on_actual_moves": _decimal_ratio(
                opposite_direction_actions,
                actual_moves,
            ),
            "predicted_action_frequency": _decimal_ratio(
                predicted_actions,
                count,
            ),
        },
    }


def _parse_directional(
    value: Any,
    field_name: str,
    *,
    count: int,
) -> DirectionalMatrix:
    directional = _as_mapping(value, field_name)
    _require_exact_keys(
        directional,
        ("confusion_matrix", "counts", "rates"),
        field_name,
    )
    confusion = _as_mapping(
        directional["confusion_matrix"],
        f"{field_name}.confusion_matrix",
    )
    expected_rows = tuple(f"actual_{label}" for label in DIRECTION_LABELS)
    _require_exact_keys(
        confusion,
        expected_rows,
        f"{field_name}.confusion_matrix",
    )
    parsed_rows: list[tuple[int, int, int]] = []
    for actual in DIRECTION_LABELS:
        row_name = f"actual_{actual}"
        row = _as_mapping(
            confusion[row_name],
            f"{field_name}.confusion_matrix.{row_name}",
        )
        expected_columns = tuple(
            f"predicted_{label}" for label in DIRECTION_LABELS
        )
        _require_exact_keys(
            row,
            expected_columns,
            f"{field_name}.confusion_matrix.{row_name}",
        )
        parsed_rows.append(
            tuple(
                _as_int(
                    row[f"predicted_{predicted}"],
                    (
                        f"{field_name}.confusion_matrix.{row_name}."
                        f"predicted_{predicted}"
                    ),
                    minimum=0,
                )
                for predicted in DIRECTION_LABELS
            )
        )
    matrix: DirectionalMatrix = tuple(parsed_rows)  # type: ignore[assignment]
    expected = _directional_summary(matrix, count=count)

    reported_counts = _as_mapping(
        directional["counts"],
        f"{field_name}.counts",
    )
    _require_exact_keys(
        reported_counts,
        DIRECTIONAL_COUNT_KEYS,
        f"{field_name}.counts",
    )
    for key in DIRECTIONAL_COUNT_KEYS:
        reported = _as_int(
            reported_counts[key],
            f"{field_name}.counts.{key}",
            minimum=0,
        )
        if reported != expected["counts"][key]:
            raise SelectionInputError(
                f"{field_name}.counts.{key} is inconsistent with confusion matrix"
            )

    reported_rates = _as_mapping(
        directional["rates"],
        f"{field_name}.rates",
    )
    _require_exact_keys(
        reported_rates,
        DIRECTIONAL_RATE_KEYS,
        f"{field_name}.rates",
    )
    for key in DIRECTIONAL_RATE_KEYS:
        _require_reported_decimal(
            reported_rates,
            key,
            expected["rates"][key],
            f"{field_name}.rates",
        )
    return matrix


def _reject_float(raw_value: str) -> None:
    raise SelectionInputError(
        f"JSON floating-point value is forbidden: {raw_value}"
    )


def _reject_constant(raw_value: str) -> None:
    raise SelectionInputError(f"non-finite JSON value is forbidden: {raw_value}")


def _parse_json_int(raw_value: str) -> int:
    if len(raw_value.lstrip("-")) > 20:
        raise SelectionInputError("JSON integer exceeds 20 digits")
    return int(raw_value)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise SelectionInputError(f"duplicate JSON key: {key}")
        payload[key] = value
    return payload


def _decode_report(raw: bytes, field_name: str) -> Mapping[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SelectionInputError(f"{field_name} is not UTF-8") from exc
    try:
        payload = json.loads(
            text,
            parse_float=_reject_float,
            parse_int=_parse_json_int,
            parse_constant=_reject_constant,
            object_pairs_hook=_unique_object,
        )
    except json.JSONDecodeError as exc:
        raise SelectionInputError(f"{field_name} is not valid JSON") from exc
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


@dataclass(frozen=True)
class MetricEvidence:
    count: int
    model_absolute_error_sum: Decimal
    baseline_absolute_error_sum: Decimal
    model_squared_error_sum: Decimal
    baseline_squared_error_sum: Decimal
    absolute_advantage_sum: Decimal
    wins: int
    ties: int
    losses: int
    directional_matrix: DirectionalMatrix

    @classmethod
    def from_payload(
        cls,
        payload: Any,
        field_name: str,
    ) -> MetricEvidence:
        metrics = _as_mapping(payload, field_name)
        count = _as_int(metrics.get("count"), f"{field_name}.count", minimum=1)
        statistics = _as_mapping(
            metrics.get("sufficient_statistics"),
            f"{field_name}.sufficient_statistics",
        )
        model_absolute_error_sum = _as_decimal(
            statistics.get("model_absolute_error_sum_usd"),
            f"{field_name}.sufficient_statistics.model_absolute_error_sum_usd",
        )
        baseline_absolute_error_sum = _as_decimal(
            statistics.get("baseline_absolute_error_sum_usd"),
            f"{field_name}.sufficient_statistics.baseline_absolute_error_sum_usd",
        )
        model_squared_error_sum = _as_decimal(
            statistics.get("model_squared_error_sum_usd2"),
            f"{field_name}.sufficient_statistics.model_squared_error_sum_usd2",
        )
        baseline_squared_error_sum = _as_decimal(
            statistics.get("baseline_squared_error_sum_usd2"),
            f"{field_name}.sufficient_statistics.baseline_squared_error_sum_usd2",
        )
        absolute_advantage_sum = _as_decimal(
            statistics.get("absolute_advantage_sum_usd"),
            f"{field_name}.sufficient_statistics.absolute_advantage_sum_usd",
        )
        for name, value in (
            ("model_absolute_error_sum_usd", model_absolute_error_sum),
            ("baseline_absolute_error_sum_usd", baseline_absolute_error_sum),
            ("model_squared_error_sum_usd2", model_squared_error_sum),
            ("baseline_squared_error_sum_usd2", baseline_squared_error_sum),
        ):
            if value < 0:
                raise SelectionInputError(
                    f"{field_name}.sufficient_statistics.{name} must be non-negative"
                )
        if absolute_advantage_sum != (
            baseline_absolute_error_sum - model_absolute_error_sum
        ):
            raise SelectionInputError(
                f"{field_name} absolute advantage sum is inconsistent"
            )
        if model_absolute_error_sum > 0 and model_squared_error_sum == 0:
            raise SelectionInputError(
                f"{field_name} model squared-error sum is impossible"
            )
        if baseline_absolute_error_sum > 0 and baseline_squared_error_sum == 0:
            raise SelectionInputError(
                f"{field_name} baseline squared-error sum is impossible"
            )
        with localcontext() as context:
            context.prec = 50
            for label, absolute_sum, squared_sum in (
                (
                    "model",
                    model_absolute_error_sum,
                    model_squared_error_sum,
                ),
                (
                    "baseline",
                    baseline_absolute_error_sum,
                    baseline_squared_error_sum,
                ),
            ):
                left = squared_sum * Decimal(count)
                right = absolute_sum * absolute_sum
                tolerance = max(abs(left), abs(right), Decimal("1")) * (
                    Decimal("1e-24")
                )
                if left + tolerance < right:
                    raise SelectionInputError(
                        f"{field_name} {label} error statistics are infeasible"
                    )

        model_mae = _replay_mean(model_absolute_error_sum, count)
        baseline_mae = _replay_mean(baseline_absolute_error_sum, count)
        model_rmse = _replay_rmse(model_squared_error_sum, count)
        baseline_rmse = _replay_rmse(baseline_squared_error_sum, count)
        _require_reported_decimal(
            metrics,
            "model_mean_absolute_error_usd",
            model_mae,
            field_name,
        )
        _require_reported_decimal(
            metrics,
            "baseline_mean_absolute_error_usd",
            baseline_mae,
            field_name,
        )
        _require_reported_decimal(
            metrics,
            "model_rmse_usd",
            model_rmse,
            field_name,
        )
        _require_reported_decimal(
            metrics,
            "baseline_rmse_usd",
            baseline_rmse,
            field_name,
        )
        _require_reported_decimal(
            metrics,
            "mean_absolute_advantage_usd",
            _replay_mean(absolute_advantage_sum, count),
            field_name,
        )
        _require_reported_decimal(
            metrics,
            "mae_skill_vs_no_change",
            _replay_mae_skill(model_mae, baseline_mae),
            field_name,
        )

        wins = _as_int(metrics.get("wins"), f"{field_name}.wins", minimum=0)
        ties = _as_int(metrics.get("ties"), f"{field_name}.ties", minimum=0)
        losses = _as_int(
            metrics.get("losses"),
            f"{field_name}.losses",
            minimum=0,
        )
        if wins + ties + losses != count:
            raise SelectionInputError(
                f"{field_name} wins, ties, and losses do not equal count"
            )
        directional_matrix = _parse_directional(
            metrics.get("directional"),
            f"{field_name}.directional",
            count=count,
        )
        return cls(
            count=count,
            model_absolute_error_sum=model_absolute_error_sum,
            baseline_absolute_error_sum=baseline_absolute_error_sum,
            model_squared_error_sum=model_squared_error_sum,
            baseline_squared_error_sum=baseline_squared_error_sum,
            absolute_advantage_sum=absolute_advantage_sum,
            wins=wins,
            ties=ties,
            losses=losses,
            directional_matrix=directional_matrix,
        )


class MetricPool:
    def __init__(self) -> None:
        self.count = 0
        self.model_absolute_error_sum = Decimal("0")
        self.baseline_absolute_error_sum = Decimal("0")
        self.model_squared_error_sum = Decimal("0")
        self.baseline_squared_error_sum = Decimal("0")
        self.absolute_advantage_sum = Decimal("0")
        self.wins = 0
        self.ties = 0
        self.losses = 0
        self.directional_matrix = [
            [0 for _predicted in DIRECTION_LABELS]
            for _actual in DIRECTION_LABELS
        ]

    def add(self, evidence: MetricEvidence) -> None:
        self.count += evidence.count
        with localcontext() as context:
            context.prec = 50
            self.model_absolute_error_sum += evidence.model_absolute_error_sum
            self.baseline_absolute_error_sum += (
                evidence.baseline_absolute_error_sum
            )
            self.model_squared_error_sum += evidence.model_squared_error_sum
            self.baseline_squared_error_sum += evidence.baseline_squared_error_sum
            self.absolute_advantage_sum += evidence.absolute_advantage_sum
        self.wins += evidence.wins
        self.ties += evidence.ties
        self.losses += evidence.losses
        for actual_index, row in enumerate(evidence.directional_matrix):
            for predicted_index, value in enumerate(row):
                self.directional_matrix[actual_index][predicted_index] += value

    @property
    def model_mae(self) -> Optional[Decimal]:
        return _decimal_ratio(self.model_absolute_error_sum, self.count)

    @property
    def baseline_mae(self) -> Optional[Decimal]:
        return _decimal_ratio(self.baseline_absolute_error_sum, self.count)

    @property
    def model_rmse(self) -> Optional[Decimal]:
        mean_square = _decimal_ratio(self.model_squared_error_sum, self.count)
        return None if mean_square is None else _decimal_sqrt(mean_square)

    @property
    def baseline_rmse(self) -> Optional[Decimal]:
        mean_square = _decimal_ratio(self.baseline_squared_error_sum, self.count)
        return None if mean_square is None else _decimal_sqrt(mean_square)

    @property
    def mae_skill(self) -> Optional[Decimal]:
        if self.baseline_absolute_error_sum == 0:
            return None
        with localcontext() as context:
            context.prec = 50
            return Decimal("1") - (
                self.model_absolute_error_sum
                / self.baseline_absolute_error_sum
            )

    @property
    def rmse_skill(self) -> Optional[Decimal]:
        model_rmse = self.model_rmse
        baseline_rmse = self.baseline_rmse
        if model_rmse is None or baseline_rmse in (None, Decimal("0")):
            return None
        with localcontext() as context:
            context.prec = 50
            return Decimal("1") - model_rmse / baseline_rmse

    @property
    def paired_net_win_rate(self) -> Optional[Decimal]:
        return _decimal_ratio(self.wins - self.losses, self.count)

    def summary(self) -> dict[str, Any]:
        directional_matrix: DirectionalMatrix = tuple(
            tuple(row) for row in self.directional_matrix
        )  # type: ignore[assignment]
        return {
            "count": self.count,
            "model_mean_absolute_error_usd": self.model_mae,
            "baseline_mean_absolute_error_usd": self.baseline_mae,
            "model_rmse_usd": self.model_rmse,
            "baseline_rmse_usd": self.baseline_rmse,
            "mean_absolute_advantage_usd": _decimal_ratio(
                self.absolute_advantage_sum,
                self.count,
            ),
            "mae_skill_vs_no_change": self.mae_skill,
            "rmse_skill_vs_no_change": self.rmse_skill,
            "wins": self.wins,
            "ties": self.ties,
            "losses": self.losses,
            "win_rate": _decimal_ratio(self.wins, self.count),
            "tie_rate": _decimal_ratio(self.ties, self.count),
            "loss_rate": _decimal_ratio(self.losses, self.count),
            "paired_net_win_rate": self.paired_net_win_rate,
            "directional": _directional_summary(
                directional_matrix,
                count=self.count,
            ),
            "sufficient_statistics": {
                "model_absolute_error_sum_usd": self.model_absolute_error_sum,
                "baseline_absolute_error_sum_usd": (
                    self.baseline_absolute_error_sum
                ),
                "model_squared_error_sum_usd2": self.model_squared_error_sum,
                "baseline_squared_error_sum_usd2": (
                    self.baseline_squared_error_sum
                ),
                "absolute_advantage_sum_usd": self.absolute_advantage_sum,
            },
        }


@dataclass(frozen=True)
class CandidateEvidence:
    model_version: str
    horizon_ms: int
    beta: Decimal
    scheduled: int
    top_level_scored: int
    common_target_eligible: int
    common_valid_generated: int
    common_scored: int
    metrics: MetricEvidence
    slices: Mapping[str, Mapping[str, MetricEvidence]]


@dataclass(frozen=True)
class ReplayEvidence:
    digest: str
    start_ms: int
    end_ms: int
    configuration: Mapping[str, Any]
    candidates: Mapping[str, CandidateEvidence]

    @property
    def configuration_digest(self) -> str:
        return _sha256(_canonical_json_bytes(self.configuration))

    @property
    def common_counts(self) -> tuple[int, int, int]:
        first = self.candidates[EXPECTED_CANDIDATES[0][0]]
        return (
            first.common_target_eligible,
            first.common_valid_generated,
            first.common_scored,
        )


def _parse_slices(
    payload: Any,
    field_name: str,
    *,
    expected_count: int,
) -> Mapping[str, Mapping[str, MetricEvidence]]:
    slices = _as_mapping(payload, field_name)
    if set(slices) != set(SLICE_DIMENSIONS):
        raise SelectionInputError(
            f"{field_name} must contain the required slice dimensions"
        )
    parsed: dict[str, dict[str, MetricEvidence]] = {}
    for dimension in SLICE_DIMENSIONS:
        categories = _as_mapping(
            slices[dimension],
            f"{field_name}.{dimension}",
        )
        if not categories:
            raise SelectionInputError(
                f"{field_name}.{dimension} must contain a category"
            )
        unknown_categories = set(categories) - set(SLICE_CATEGORIES[dimension])
        if unknown_categories:
            raise SelectionInputError(
                f"{field_name}.{dimension} contains unsupported categories"
            )
        parsed_categories = {
            category: MetricEvidence.from_payload(
                metrics,
                f"{field_name}.{dimension}.{category}",
            )
            for category, metrics in categories.items()
        }
        if sum(item.count for item in parsed_categories.values()) != expected_count:
            raise SelectionInputError(
                f"{field_name}.{dimension} counts do not equal cohort count"
            )
        parsed[dimension] = parsed_categories
    return parsed


def _parse_candidate(payload: Any, field_name: str) -> CandidateEvidence:
    candidate = _as_mapping(payload, field_name)
    model_version = _as_string(
        candidate.get("model_version"),
        f"{field_name}.model_version",
    )
    horizon_ms = _as_int(
        candidate.get("horizon_ms"),
        f"{field_name}.horizon_ms",
        minimum=1,
    )
    beta = _as_decimal(candidate.get("beta"), f"{field_name}.beta")
    scheduled = _as_int(
        candidate.get("scheduled"),
        f"{field_name}.scheduled",
        minimum=0,
    )
    top_level_scored = _as_int(
        candidate.get("scored"),
        f"{field_name}.scored",
        minimum=0,
    )
    common = _as_mapping(
        candidate.get("common_cohort"),
        f"{field_name}.common_cohort",
    )
    if common.get("definition") != COMMON_COHORT_DEFINITION:
        raise SelectionInputError(
            f"{field_name}.common_cohort.definition is unsupported"
        )
    common_target_eligible = _as_int(
        common.get("target_eligible"),
        f"{field_name}.common_cohort.target_eligible",
        minimum=0,
    )
    common_valid_generated = _as_int(
        common.get("valid_generated"),
        f"{field_name}.common_cohort.valid_generated",
        minimum=0,
    )
    common_scored = _as_int(
        common.get("scored"),
        f"{field_name}.common_cohort.scored",
        minimum=0,
    )
    if not (
        scheduled >= top_level_scored >= common_scored
        and scheduled >= common_target_eligible >= common_valid_generated
        and common_valid_generated >= common_scored
    ):
        raise SelectionInputError(f"{field_name} cohort counts are inconsistent")
    metrics = MetricEvidence.from_payload(
        common.get("metrics"),
        f"{field_name}.common_cohort.metrics",
    )
    if metrics.count != common_scored:
        raise SelectionInputError(
            f"{field_name}.common_cohort metrics count does not equal scored"
        )
    slices = _parse_slices(
        common.get("slices"),
        f"{field_name}.common_cohort.slices",
        expected_count=common_scored,
    )
    return CandidateEvidence(
        model_version=model_version,
        horizon_ms=horizon_ms,
        beta=beta,
        scheduled=scheduled,
        top_level_scored=top_level_scored,
        common_target_eligible=common_target_eligible,
        common_valid_generated=common_valid_generated,
        common_scored=common_scored,
        metrics=metrics,
        slices=slices,
    )


def _validate_configuration(configuration: Mapping[str, Any]) -> None:
    lags = _as_list(configuration.get("lags_ms"), "configuration.lags_ms")
    if tuple(lags) != EXPECTED_LAGS_MS:
        raise SelectionInputError(
            "configuration.lags_ms is not the provisional V0 candidate set"
        )
    if _as_decimal(configuration.get("beta"), "configuration.beta") != (
        EXPECTED_BETA
    ):
        raise SelectionInputError("configuration.beta must equal 1")
    poll_ms = _as_int(configuration.get("poll_ms"), "configuration.poll_ms")
    if poll_ms != 100:
        raise SelectionInputError("configuration.poll_ms must equal 100")
    evaluation_interval_ms = _as_int(
        configuration.get("evaluation_interval_ms"),
        "configuration.evaluation_interval_ms",
    )
    if evaluation_interval_ms != 500:
        raise SelectionInputError(
            "configuration.evaluation_interval_ms must equal 500"
        )
    for field_name in POSITIVE_CONFIGURATION_INTS:
        _as_int(
            configuration.get(field_name),
            f"configuration.{field_name}",
            minimum=1,
        )
    non_negative_values = {}
    for field_name in NON_NEGATIVE_CONFIGURATION_INTS:
        non_negative_values[field_name] = _as_int(
            configuration.get(field_name),
            f"configuration.{field_name}",
            minimum=0,
        )
    if non_negative_values["max_future_skew_ms"] != 0:
        raise SelectionInputError(
            "configuration.max_future_skew_ms must equal policy-v3 zero"
        )
    evaluation_phase_offset_ms = non_negative_values[
        "evaluation_phase_offset_ms"
    ]
    if evaluation_phase_offset_ms >= evaluation_interval_ms:
        raise SelectionInputError(
            "configuration.evaluation_phase_offset_ms must be less than "
            "evaluation_interval_ms"
        )
    if evaluation_phase_offset_ms % poll_ms != 0:
        raise SelectionInputError(
            "configuration.evaluation_phase_offset_ms must be a multiple "
            "of poll_ms"
        )
    for field_name in (
        "neutral_band_bps",
    ):
        if _as_decimal(
            configuration.get(field_name),
            f"configuration.{field_name}",
        ) < 0:
            raise SelectionInputError(
                f"configuration.{field_name} must be non-negative"
            )
    for field_name in (
        "move_size_thresholds_bps",
        "volatility_thresholds_bps",
    ):
        thresholds = _as_list(
            configuration.get(field_name),
            f"configuration.{field_name}",
        )
        if len(thresholds) != 2:
            raise SelectionInputError(
                f"configuration.{field_name} must contain two values"
            )
        parsed = [
            _as_decimal(value, f"configuration.{field_name}[{index}]")
            for index, value in enumerate(thresholds)
        ]
        if parsed[0] < 0 or parsed[0] >= parsed[1]:
            raise SelectionInputError(
                f"configuration.{field_name} must be non-negative and increasing"
            )
    if configuration.get("volatility_measure") != (
        "rms_of_consecutive_raw_bucket_returns"
    ):
        raise SelectionInputError("unsupported configuration.volatility_measure")
    if configuration.get("volatility_time_basis") != (
        "worker_poll_visibility_ms"
    ):
        raise SelectionInputError(
            "unsupported configuration.volatility_time_basis"
        )
    if configuration.get("session_boundary_measure") != (
        "time_since_common_segment_start_and_until_segment_end"
    ):
        raise SelectionInputError(
            "unsupported configuration.session_boundary_measure"
        )
    _as_bool(
        configuration.get("exclude_parse_error_sessions"),
        "configuration.exclude_parse_error_sessions",
    )


def _parse_report(path: Path) -> ReplayEvidence:
    if not isinstance(path, Path):
        raise TypeError("report paths must be pathlib.Path values")
    if path.stat().st_size > MAX_REPORT_BYTES:
        raise SelectionInputError(
            f"{path} exceeds the {MAX_REPORT_BYTES}-byte report limit"
        )
    with path.open("rb") as stream:
        raw = stream.read(MAX_REPORT_BYTES + 1)
    if len(raw) > MAX_REPORT_BYTES:
        raise SelectionInputError(
            f"{path} exceeds the {MAX_REPORT_BYTES}-byte report limit"
        )
    payload = _decode_report(raw, str(path))
    if _as_int(payload.get("schema_version"), "schema_version") != (
        SUPPORTED_REPLAY_SCHEMA_VERSION
    ):
        raise SelectionInputError("unsupported replay schema_version")
    if payload.get("mode") != "shadow_raw_replay":
        raise SelectionInputError("report mode must be shadow_raw_replay")
    if payload.get("status") != "ok":
        raise SelectionInputError("replay report status must be ok")
    if _as_bool(payload.get("selection_performed"), "selection_performed"):
        raise SelectionInputError("replay report already performed selection")

    comparison = _as_mapping(
        payload.get("comparison_cohort"),
        "comparison_cohort",
    )
    if comparison.get("definition") != COMMON_COHORT_DEFINITION:
        raise SelectionInputError("unsupported comparison cohort definition")
    if comparison.get("metrics_location") != (
        "candidates[].common_cohort.metrics"
    ):
        raise SelectionInputError("unsupported comparison metrics location")
    if not _as_bool(
        comparison.get("required_for_status_ok"),
        "comparison_cohort.required_for_status_ok",
    ):
        raise SelectionInputError("common comparison cohort must be required")

    report_range = _as_mapping(payload.get("range"), "range")
    start_ms = _as_int(report_range.get("start_ms"), "range.start_ms", minimum=0)
    end_ms = _as_int(report_range.get("end_ms"), "range.end_ms", minimum=1)
    if end_ms <= start_ms:
        raise SelectionInputError("report range must have positive duration")
    if report_range.get("boundary") != "[start_ms,end_ms)":
        raise SelectionInputError("unsupported report range boundary")

    configuration = _as_mapping(payload.get("configuration"), "configuration")
    _validate_configuration(configuration)
    data_quality = _as_mapping(payload.get("data_quality"), "data_quality")
    if data_quality.get("session_policy") != (
        "completed_clean_integrity_checked"
    ):
        raise SelectionInputError("unsupported data-quality session policy")
    if not _as_bool(
        data_quality.get("conservative_reset_at_common_session_boundary"),
        "data_quality.conservative_reset_at_common_session_boundary",
    ):
        raise SelectionInputError("replay must reset at common session boundaries")
    candidate_payloads = _as_list(payload.get("candidates"), "candidates")
    parsed_candidates: dict[str, CandidateEvidence] = {}
    for index, candidate_payload in enumerate(candidate_payloads):
        candidate = _parse_candidate(
            candidate_payload,
            f"candidates[{index}]",
        )
        if candidate.model_version in parsed_candidates:
            raise SelectionInputError("duplicate candidate model_version")
        parsed_candidates[candidate.model_version] = candidate
    expected_versions = {version for version, _horizon in EXPECTED_CANDIDATES}
    if set(parsed_candidates) != expected_versions:
        raise SelectionInputError("report candidate set is not provisional V0")
    for expected_version, expected_horizon in EXPECTED_CANDIDATES:
        candidate = parsed_candidates[expected_version]
        if candidate.horizon_ms != expected_horizon:
            raise SelectionInputError(
                f"{expected_version} has an inconsistent horizon"
            )
        if candidate.beta != EXPECTED_BETA:
            raise SelectionInputError(f"{expected_version} beta must equal 1")
    common_counts = {
        (
            candidate.common_target_eligible,
            candidate.common_valid_generated,
            candidate.common_scored,
        )
        for candidate in parsed_candidates.values()
    }
    if len(common_counts) != 1:
        raise SelectionInputError(
            "common cohort counts differ across candidates"
        )
    scheduled_counts = {
        candidate.scheduled for candidate in parsed_candidates.values()
    }
    if len(scheduled_counts) != 1:
        raise SelectionInputError("scheduled counts differ across candidates")
    maximum_ticks = (end_ms - start_ms + 499) // 500 + 1
    if next(iter(scheduled_counts)) > maximum_ticks:
        raise SelectionInputError("scheduled count exceeds the report cadence")
    return ReplayEvidence(
        digest=_sha256(raw),
        start_ms=start_ms,
        end_ms=end_ms,
        configuration=configuration,
        candidates=parsed_candidates,
    )


def _load_role_reports(
    paths: Sequence[Path],
    role: str,
) -> list[ReplayEvidence]:
    if not paths:
        raise SelectionInputError(f"at least one {role} report is required")
    reports = sorted(
        (_parse_report(path) for path in paths),
        key=lambda report: (report.start_ms, report.end_ms, report.digest),
    )
    for previous, current in zip(reports, reports[1:]):
        if current.start_ms < previous.end_ms:
            raise SelectionInputError(f"{role} report ranges overlap")
    return reports


def _validate_report_set(
    calibration: Sequence[ReplayEvidence],
    holdout: Sequence[ReplayEvidence],
) -> None:
    all_reports = list(calibration) + list(holdout)
    digests = [report.digest for report in all_reports]
    if len(set(digests)) != len(digests):
        raise SelectionInputError("the same replay report was supplied twice")
    configuration_digests = {
        report.configuration_digest for report in all_reports
    }
    if len(configuration_digests) != 1:
        raise SelectionInputError("replay report configurations differ")
    if calibration[-1].end_ms > holdout[0].start_ms:
        raise SelectionInputError(
            "every holdout range must be later than calibration ranges"
        )
    chronological = sorted(
        all_reports,
        key=lambda report: (report.start_ms, report.end_ms, report.digest),
    )
    for previous, current in zip(chronological, chronological[1:]):
        if current.start_ms < previous.end_ms:
            raise SelectionInputError("calibration and holdout ranges overlap")


def _pool_candidate(
    reports: Sequence[ReplayEvidence],
    model_version: str,
) -> tuple[MetricPool, dict[str, dict[str, MetricPool]]]:
    overall = MetricPool()
    slices: dict[str, dict[str, MetricPool]] = {
        dimension: {} for dimension in SLICE_DIMENSIONS
    }
    for report in reports:
        candidate = report.candidates[model_version]
        overall.add(candidate.metrics)
        for dimension, categories in candidate.slices.items():
            for category, metrics in categories.items():
                slices[dimension].setdefault(category, MetricPool()).add(metrics)
    return overall, slices


def _slice_summary(
    slices: Mapping[str, Mapping[str, MetricPool]],
) -> dict[str, dict[str, Any]]:
    return {
        dimension: {
            category: categories.get(category, MetricPool()).summary()
            for category in SLICE_CATEGORIES[dimension]
        }
        for dimension, categories in slices.items()
    }


def _slice_warnings(
    role: str,
    slices: Mapping[str, Mapping[str, MetricPool]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for dimension, categories in slices.items():
        for category in SLICE_CATEGORIES[dimension]:
            pool = categories.get(category, MetricPool())
            if pool.count < MIN_SLICE_SCORED_FOR_WARNING:
                warnings.append(
                    {
                        "role": role,
                        "dimension": dimension,
                        "category": category,
                        "warning": "sparse_slice",
                        "count": pool.count,
                        "threshold": MIN_SLICE_SCORED_FOR_WARNING,
                    }
                )
                continue
            if pool.mae_skill is None or pool.mae_skill <= 0:
                warnings.append(
                    {
                        "role": role,
                        "dimension": dimension,
                        "category": category,
                        "warning": "no_mae_improvement_vs_no_change",
                        "count": pool.count,
                        "mae_skill_vs_no_change": pool.mae_skill,
                    }
                )
    return warnings


def _efficacy_gates(pool: MetricPool) -> dict[str, dict[str, Any]]:
    mae_skill = pool.mae_skill
    rmse_skill = pool.rmse_skill
    return {
        "mae_skill_positive": {
            "observed": mae_skill,
            "threshold_exclusive": MIN_MAE_SKILL_EXCLUSIVE,
            "passed": (
                mae_skill is not None
                and mae_skill > MIN_MAE_SKILL_EXCLUSIVE
            ),
        },
        "rmse_skill_positive": {
            "observed": rmse_skill,
            "threshold_exclusive": MIN_RMSE_SKILL_EXCLUSIVE,
            "passed": (
                rmse_skill is not None
                and rmse_skill > MIN_RMSE_SKILL_EXCLUSIVE
            ),
        },
    }


def _paired_frequency_diagnostic(pool: MetricPool) -> dict[str, Any]:
    wins_minus_losses = pool.wins - pool.losses
    return {
        "hard_gate": False,
        "affects_eligibility": False,
        "affects_ranking": False,
        "wins": pool.wins,
        "ties": pool.ties,
        "losses": pool.losses,
        "observed_wins_minus_losses": wins_minus_losses,
        "paired_net_win_rate": pool.paired_net_win_rate,
        "warning": (
            "paired_wins_do_not_exceed_losses"
            if wins_minus_losses <= 0
            else None
        ),
        "interpretation": (
            "frequency diagnostic over autocorrelated 500 ms rows; "
            "not an efficacy gate"
        ),
    }


def _all_gates_pass(gates: Mapping[str, Mapping[str, Any]]) -> bool:
    return all(_as_bool(gate["passed"], "gate.passed") for gate in gates.values())


def _coverage_for_report(report: ReplayEvidence) -> dict[str, Any]:
    target_eligible, valid_generated, scored = report.common_counts
    valid_coverage = _decimal_ratio(valid_generated, target_eligible)
    maturation_coverage = _decimal_ratio(scored, valid_generated)
    gates = {
        "minimum_common_scored": {
            "observed": scored,
            "threshold_inclusive": MIN_COMMON_SCORED_PER_REPORT,
            "passed": scored >= MIN_COMMON_SCORED_PER_REPORT,
        },
        "minimum_common_valid_coverage": {
            "observed": valid_coverage,
            "threshold_inclusive": MIN_COMMON_VALID_COVERAGE,
            "passed": (
                valid_coverage is not None
                and valid_coverage >= MIN_COMMON_VALID_COVERAGE
            ),
        },
        "minimum_common_maturation_coverage": {
            "observed": maturation_coverage,
            "threshold_inclusive": MIN_COMMON_MATURATION_COVERAGE,
            "passed": (
                maturation_coverage is not None
                and maturation_coverage >= MIN_COMMON_MATURATION_COVERAGE
            ),
        },
    }
    return {
        "target_eligible": target_eligible,
        "valid_generated": valid_generated,
        "scored": scored,
        "valid_coverage": valid_coverage,
        "maturation_coverage": maturation_coverage,
        "gates": gates,
        "passed": _all_gates_pass(gates),
    }


def _report_records(
    calibration: Sequence[ReplayEvidence],
    holdout: Sequence[ReplayEvidence],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    previous_end_ms: Optional[int] = None
    for role, reports in (("calibration", calibration), ("holdout", holdout)):
        for report in reports:
            records.append(
                {
                    "role": role,
                    "start_ms": report.start_ms,
                    "end_ms": report.end_ms,
                    "boundary": "[start_ms,end_ms)",
                    "gap_from_previous_ms": (
                        None
                        if previous_end_ms is None
                        else report.start_ms - previous_end_ms
                    ),
                    "sha256": report.digest,
                    "coverage": _coverage_for_report(report),
                }
            )
            previous_end_ms = report.end_ms
    return records


def _policy_payload() -> dict[str, Any]:
    return {
        "version": SELECTION_POLICY_VERSION,
        "supersedes": "chronological_holdout_v2",
        "revision_reason": (
            "directional diagnostics use a full three-class confusion matrix "
            "that includes actions on neutral outcomes; MAE and RMSE remain "
            "efficacy gates; fixed replay visibility delays and evaluation "
            "phase are frozen sensitivity assumptions, with zero future skew"
        ),
        "previously_inspected_holdouts_must_be_calibration": True,
        "new_later_holdout_required_after_revision": True,
        "candidate_set": [version for version, _horizon in EXPECTED_CANDIDATES],
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
            "minimum_common_scored_per_report": (
                MIN_COMMON_SCORED_PER_REPORT
            ),
            "minimum_common_valid_coverage": MIN_COMMON_VALID_COVERAGE,
            "minimum_common_maturation_coverage": (
                MIN_COMMON_MATURATION_COVERAGE
            ),
            "minimum_slice_scored_for_warning": (
                MIN_SLICE_SCORED_FOR_WARNING
            ),
        },
        "efficacy_gates": {
            "mae_skill_vs_no_change": {
                "operator": ">",
                "threshold": MIN_MAE_SKILL_EXCLUSIVE,
            },
            "rmse_skill_vs_no_change": {
                "operator": ">",
                "threshold": MIN_RMSE_SKILL_EXCLUSIVE,
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


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("selection artifact contains a non-finite Decimal")
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


@dataclass(frozen=True)
class SelectionArtifact:
    payload: Mapping[str, Any]

    @property
    def status(self) -> str:
        return str(self.payload["status"])

    @property
    def selected(self) -> bool:
        return self.status == "selected"

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


def select_provisional_primary(
    *,
    calibration_report_paths: Sequence[Path],
    holdout_report_paths: Sequence[Path],
) -> SelectionArtifact:
    if len(holdout_report_paths) != 1:
        raise SelectionInputError(
            "policy v3 requires exactly one untouched holdout report"
        )
    calibration = _load_role_reports(
        calibration_report_paths,
        "calibration",
    )
    holdout = _load_role_reports(holdout_report_paths, "holdout")
    _validate_report_set(calibration, holdout)
    report_records = _report_records(calibration, holdout)
    evidence_passed = all(
        record["coverage"]["passed"] for record in report_records
    )

    candidate_work: list[dict[str, Any]] = []
    for model_version, horizon_ms in EXPECTED_CANDIDATES:
        calibration_pool, calibration_slices = _pool_candidate(
            calibration,
            model_version,
        )
        holdout_pool, holdout_slices = _pool_candidate(
            holdout,
            model_version,
        )
        calibration_gates = _efficacy_gates(calibration_pool)
        holdout_gates = _efficacy_gates(holdout_pool)
        candidate_work.append(
            {
                "model_version": model_version,
                "horizon_ms": horizon_ms,
                "beta": EXPECTED_BETA,
                "calibration_pool": calibration_pool,
                "holdout_pool": holdout_pool,
                "calibration_slices": calibration_slices,
                "holdout_slices": holdout_slices,
                "calibration_gates": calibration_gates,
                "holdout_gates": holdout_gates,
                "calibration_paired_frequency": (
                    _paired_frequency_diagnostic(calibration_pool)
                ),
                "holdout_paired_frequency": (
                    _paired_frequency_diagnostic(holdout_pool)
                ),
                "calibration_eligible": _all_gates_pass(calibration_gates),
            }
        )

    def ranking_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
        pool: MetricPool = item["calibration_pool"]
        mae_skill = pool.mae_skill
        rmse_skill = pool.rmse_skill
        return (
            not bool(item["calibration_eligible"]),
            mae_skill is None,
            -(mae_skill or Decimal("0")),
            rmse_skill is None,
            -(rmse_skill or Decimal("0")),
            int(item["horizon_ms"]),
            str(item["model_version"]),
        )

    ranked = sorted(candidate_work, key=ranking_key)
    for rank, item in enumerate(ranked, start=1):
        item["calibration_rank"] = rank

    eligible = [item for item in ranked if item["calibration_eligible"]]
    frozen_winner: Optional[dict[str, Any]] = None
    status: str
    reason: str
    if not evidence_passed:
        status = "insufficient_evidence"
        reason = "one or more reports failed common-cohort evidence gates"
    elif not eligible:
        status = "calibration_error_gate_failed"
        reason = (
            "no calibration candidate passed both MAE and RMSE "
            "improvement gates"
        )
    else:
        first = eligible[0]
        if len(eligible) > 1:
            second = eligible[1]
            first_pool: MetricPool = first["calibration_pool"]
            second_pool: MetricPool = second["calibration_pool"]
            first_efficacy = (
                first_pool.mae_skill,
                first_pool.rmse_skill,
            )
            second_efficacy = (
                second_pool.mae_skill,
                second_pool.rmse_skill,
            )
        else:
            first_efficacy = None
            second_efficacy = None
        if first_efficacy is not None and first_efficacy == second_efficacy:
            status = "calibration_tie"
            reason = (
                "the leading calibration candidates are exactly tied on "
                "MAE and RMSE skill"
            )
        else:
            frozen_winner = first
            if _all_gates_pass(first["holdout_gates"]):
                status = "selected"
                reason = (
                    "the frozen calibration winner passed positive MAE- and "
                    "RMSE-skill holdout gates; paired wins/losses are "
                    "diagnostic only"
                )
            else:
                status = "holdout_failed"
                reason = (
                    "the frozen calibration winner failed MAE and/or RMSE "
                    "holdout gates; no fallback candidate was considered"
                )

    candidate_payloads: list[dict[str, Any]] = []
    for item in sorted(
        ranked,
        key=lambda candidate: int(candidate["calibration_rank"]),
    ):
        calibration_pool = item["calibration_pool"]
        holdout_pool = item["holdout_pool"]
        warnings = _slice_warnings(
            "calibration",
            item["calibration_slices"],
        ) + _slice_warnings("holdout", item["holdout_slices"])
        candidate_payloads.append(
            {
                "calibration_rank": item["calibration_rank"],
                "model_version": item["model_version"],
                "horizon_ms": item["horizon_ms"],
                "beta": item["beta"],
                "calibration_eligible": item["calibration_eligible"],
                "calibration": {
                    "metrics": calibration_pool.summary(),
                    "gates": item["calibration_gates"],
                    "paired_frequency_diagnostic": (
                        item["calibration_paired_frequency"]
                    ),
                    "slices": _slice_summary(item["calibration_slices"]),
                },
                "holdout": {
                    "metrics": holdout_pool.summary(),
                    "gates": item["holdout_gates"],
                    "paired_frequency_diagnostic": (
                        item["holdout_paired_frequency"]
                    ),
                    "slices": _slice_summary(item["holdout_slices"]),
                },
                "slice_warnings": warnings,
            }
        )

    policy = _policy_payload()
    configuration_digest = calibration[0].configuration_digest
    fingerprint_payload = {
        "policy": _json_ready(policy),
        "configuration_sha256": configuration_digest,
        "reports": [
            {
                "role": record["role"],
                "start_ms": record["start_ms"],
                "end_ms": record["end_ms"],
                "sha256": record["sha256"],
            }
            for record in report_records
        ],
    }
    selected_model = None
    if status == "selected" and frozen_winner is not None:
        selected_model = {
            "model_version": frozen_winner["model_version"],
            "horizon_ms": frozen_winner["horizon_ms"],
            "beta": frozen_winner["beta"],
            "evidence_end_ms": holdout[-1].end_ms,
        }
    payload = {
        "schema_version": SELECTION_SCHEMA_VERSION,
        "mode": "shadow_primary_selection",
        "status": status,
        "selection_performed": status == "selected",
        "provisional": True,
        "dynamic_switching": False,
        "prediction_target": (
            "latest_chainlink_value_known_at_generated_ms_plus_horizon_ms"
        ),
        "policy": policy,
        "provenance": {
            "configuration_sha256": configuration_digest,
            "selection_fingerprint_sha256": _sha256(
                _canonical_json_bytes(fingerprint_payload)
            ),
            "reports": report_records,
        },
        "decision": {
            "reason": reason,
            "frozen_calibration_winner": (
                None
                if frozen_winner is None
                else frozen_winner["model_version"]
            ),
            "provisional_primary_model": selected_model,
            "holdout_reranking_performed": False,
            "fallback_after_holdout_failure_performed": False,
        },
        "candidates": candidate_payloads,
        "limitations": [
            "This is a provisional price-only Chainlink catch-up model.",
            "It is not a market-close, settlement, probability, or execution forecast.",
            "Horizon-crossing forecasts do not claim arrival before market expiry.",
            "Per-report bounded medians are not pooled or used for selection.",
            "Paired win/loss frequency is diagnostic because 500 ms rows are autocorrelated.",
            "Slice warnings are descriptive guardrails, not significance tests.",
            "All candidates must continue silent evaluation; do not switch dynamically.",
            "Phase 4 raw partition and retention risks remain unproven.",
        ],
    }
    return SelectionArtifact(payload=payload)


def encode_selection_artifact(artifact: SelectionArtifact) -> str:
    if not isinstance(artifact, SelectionArtifact):
        raise TypeError("artifact must be SelectionArtifact")
    return json.dumps(
        _json_ready(artifact.to_dict()),
        indent=2,
        sort_keys=True,
    )


def write_selection_artifact(path: Path, artifact: SelectionArtifact) -> None:
    if not isinstance(path, Path):
        raise TypeError("path must be pathlib.Path")
    encoded = (encode_selection_artifact(artifact) + "\n").encode("utf-8")
    temporary_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary_path.write_bytes(encoded)
        try:
            os.link(temporary_path, path)
        except FileExistsError as exc:
            if path.read_bytes() != encoded:
                raise SelectionInputError(
                    "selection artifact already exists with different content"
                ) from exc
    finally:
        temporary_path.unlink(missing_ok=True)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze a provisional shadow model using chronological calibration "
            "and holdout replay reports"
        )
    )
    parser.add_argument(
        "--calibration-report",
        action="append",
        type=Path,
        required=True,
        help="Older Phase 2 report; repeat for additional calibration windows",
    )
    parser.add_argument(
        "--holdout-report",
        action="append",
        type=Path,
        required=True,
        help="Exactly one later untouched Phase 2 report",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        artifact = select_provisional_primary(
            calibration_report_paths=arguments.calibration_report,
            holdout_report_paths=arguments.holdout_report,
        )
        write_selection_artifact(arguments.output, artifact)
        if not artifact.selected:
            print(
                f"shadow selection abstained: {artifact.status}",
                file=sys.stderr,
            )
            return 2
    except (
        ArithmeticError,
        OSError,
        SelectionInputError,
        TypeError,
        ValueError,
    ) as exc:
        print(f"shadow selection failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
