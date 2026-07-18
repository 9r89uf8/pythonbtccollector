from __future__ import annotations

import argparse
import asyncio
import heapq
import json
import os
import sys
from collections import Counter, deque
from dataclasses import dataclass, replace
from decimal import Decimal, localcontext
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Deque,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
)
from uuid import UUID

import asyncpg

from price_collector.shadow_signal import (
    ANCHOR_HISTORY_MISSING,
    ANCHOR_REFERENCE_GAP,
    ANCHORED,
    BASIS_POINTS,
    CHAINLINK_STALE,
    CHAINLINK_UNAVAILABLE,
    CatchupModel,
    EngineObservation,
    FUTURES_STALE,
    FUTURES_UNAVAILABLE,
    MODEL_ERROR,
    ModelSignal,
    ModelAnchor,
    ObservedPrice,
    ShadowSignalEngine,
    TIMESTAMP_REGRESSION,
    VALID,
    WAITING_FOR_NEW_CHAINLINK_ANCHOR,
    WARMING_UP,
    WARMING_UP_FUTURES_HISTORY,
    no_change_projection,
    project_from_anchor,
)
from price_collector.shadow_signal_experiment import (
    COMPARISON_LAGS_MS,
    DAY_MS,
    FINALIZATION_ALLOWANCE_MS,
    GENERATION_INTERVAL_MS as V4_GENERATION_INTERVAL_MS,
    POLL_MS as V4_POLL_MS,
    ControlMode,
    ForecastConfig,
    ModelIdentity,
    TimingCell,
    V4ExperimentContract,
    V4_TIMING_CELLS,
    forecast_config_digest,
)


FUTURES_EVENT = "futures"
CHAINLINK_EVENT = "chainlink"
FUTURES_SESSION_SOURCE = "binance_futures_agg_trade"
CHAINLINK_SESSION_SOURCE = "polymarket_chainlink_rtds"
REPLAY_APPLICATION_NAME = "price_collector_shadow_signal_replay"
NS_PER_MS = 1_000_000
REPORT_SCHEMA_VERSION = 3
MAX_REPLAY_WINDOW_MS = 24 * 60 * 60 * 1000
DEFAULT_QUANTILE_SAMPLE_MAX = 10_000
DEFAULT_DATABASE_CHUNK_MS = 5 * 60 * 1000
MIN_POLL_MS = 100
MIN_EVALUATION_INTERVAL_MS = 500
MAX_LAG_CANDIDATES = 5
MAX_LAG_MS = 10_000
MAX_HISTORY_RETENTION_MS = 30_000
MAX_VOLATILITY_LOOKBACK_MS = 30_000
MAX_QUANTILE_SAMPLE_MAX = 50_000
MAX_CANDIDATE_QUANTILE_BUDGET = 150_000

DEFAULT_LAGS_MS = (3_000, 3_500, 4_000)
DEFAULT_BETA = Decimal("1")
DIRECTION_LABELS = ("up", "neutral", "down")

V4_CAUSAL_REPLAY_MODE = "chainlink_v4_causal_raw_replay"
V4_REPLAY_SCHEMA_VERSION = 1
V4_NOT_CAPTURED = "not_captured"


class ReplayDataError(ValueError):
    pass


class _BoundedSample:
    """Deterministic reservoir used only for distribution summaries."""

    def __init__(self, max_size: int, *, seed: int) -> None:
        _require_positive_int(max_size, "max_size")
        self.max_size = max_size
        self.population_size = 0
        self.values: list[Any] = []
        self.maximum: Optional[Any] = None
        self._state = seed & ((1 << 64) - 1)

    def add(self, value: Any) -> None:
        self.population_size += 1
        if self.maximum is None or value > self.maximum:
            self.maximum = value
        if len(self.values) < self.max_size:
            self.values.append(value)
            return
        self._state = (
            self._state * 6_364_136_223_846_793_005
            + 1_442_695_040_888_963_407
        ) & ((1 << 64) - 1)
        replacement_index = self._state % self.population_size
        if replacement_index < self.max_size:
            self.values[replacement_index] = value

    def snapshot(self) -> list[Any]:
        return self.values.copy()

    @property
    def sampled(self) -> bool:
        return self.population_size > len(self.values)

    def metadata(self) -> dict[str, Any]:
        return {
            "population_size": self.population_size,
            "sample_size": len(self.values),
            "bounded_reservoir": self.sampled,
        }


def _require_non_negative_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_int(value: object, field_name: str) -> None:
    _require_non_negative_int(value, field_name)
    if value == 0:
        raise ValueError(f"{field_name} must be positive")


def _require_finite_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")


def _decimal_rate(numerator: int, denominator: int) -> Optional[Decimal]:
    if denominator == 0:
        return None
    with localcontext() as context:
        context.prec = 50
        return Decimal(numerator) / Decimal(denominator)


def _directional_summary(
    confusion: Mapping[tuple[str, str], int],
    *,
    count: int,
) -> dict[str, Any]:
    matrix = {
        f"actual_{actual}": {
            f"predicted_{predicted}": confusion.get((actual, predicted), 0)
            for predicted in DIRECTION_LABELS
        }
        for actual in DIRECTION_LABELS
    }
    matrix_count = sum(
        matrix[f"actual_{actual}"][f"predicted_{predicted}"]
        for actual in DIRECTION_LABELS
        for predicted in DIRECTION_LABELS
    )
    if matrix_count != count:
        raise ReplayDataError(
            "directional confusion matrix count differs from metric count"
        )

    correct_actions = (
        matrix["actual_up"]["predicted_up"]
        + matrix["actual_down"]["predicted_down"]
    )
    three_class_correct = (
        correct_actions + matrix["actual_neutral"]["predicted_neutral"]
    )
    predicted_actions = sum(
        matrix[f"actual_{actual}"][f"predicted_{predicted}"]
        for actual in DIRECTION_LABELS
        for predicted in ("up", "down")
    )
    actual_moves = sum(
        matrix[f"actual_{actual}"][f"predicted_{predicted}"]
        for actual in ("up", "down")
        for predicted in DIRECTION_LABELS
    )
    actual_neutral = sum(matrix["actual_neutral"].values())
    false_actions_on_neutral = (
        matrix["actual_neutral"]["predicted_up"]
        + matrix["actual_neutral"]["predicted_down"]
    )
    opposite_direction_actions = (
        matrix["actual_up"]["predicted_down"]
        + matrix["actual_down"]["predicted_up"]
    )
    return {
        "confusion_matrix": matrix,
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
            "three_class_accuracy": _decimal_rate(three_class_correct, count),
            "action_precision": _decimal_rate(
                correct_actions,
                predicted_actions,
            ),
            "move_recall": _decimal_rate(correct_actions, actual_moves),
            "false_action_rate_on_neutral": _decimal_rate(
                false_actions_on_neutral,
                actual_neutral,
            ),
            "opposite_direction_rate_on_actual_moves": _decimal_rate(
                opposite_direction_actions,
                actual_moves,
            ),
            "predicted_action_frequency": _decimal_rate(
                predicted_actions,
                count,
            ),
        },
    }


def _decimal_mean(total: Decimal, count: int) -> Optional[Decimal]:
    if count == 0:
        return None
    return total / Decimal(count)


def _decimal_sqrt(value: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return value.sqrt()


def _median(values: list[Decimal]) -> Optional[Decimal]:
    if not values:
        return None
    values.sort()
    midpoint = len(values) // 2
    if len(values) % 2:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / Decimal("2")


def _nearest_rank(values: list[int], percentile: Decimal) -> Optional[int]:
    if not values:
        return None
    values.sort()
    rank = int((percentile * Decimal(len(values))).to_integral_value(
        rounding="ROUND_CEILING"
    ))
    return values[max(0, rank - 1)]


def _ceil_to_multiple(value: int, multiple: int) -> int:
    return ((value + multiple - 1) // multiple) * multiple


def _beta_version(beta: Decimal) -> str:
    scaled = beta * Decimal("100")
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise ValueError("beta must have at most two decimal places")
    return f"b{int(integral):03d}"


@dataclass(frozen=True)
class ReplayConfig:
    start_ms: int
    end_ms: int
    lags_ms: tuple[int, ...] = DEFAULT_LAGS_MS
    beta: Decimal = DEFAULT_BETA
    poll_ms: int = 100
    evaluation_interval_ms: int = 500
    futures_stale_ms: int = 1_000
    chainlink_stale_ms: int = 5_000
    reference_max_gap_ms: int = 250
    history_retention_ms: int = 10_000
    max_future_skew_ms: int = 0
    futures_availability_delay_ms: int = 0
    chainlink_availability_delay_ms: int = 0
    evaluation_phase_offset_ms: int = 0
    neutral_band_bps: Decimal = Decimal("1")
    move_size_thresholds_bps: tuple[Decimal, Decimal] = (
        Decimal("1"),
        Decimal("3"),
    )
    volatility_thresholds_bps: tuple[Decimal, Decimal] = (
        Decimal("0.5"),
        Decimal("1.5"),
    )
    volatility_lookback_ms: int = 10_000
    near_expiry_ms: int = 10_000
    near_reconnect_ms: int = 10_000
    quantile_sample_max: int = DEFAULT_QUANTILE_SAMPLE_MAX
    exclude_parse_error_sessions: bool = False

    def __post_init__(self) -> None:
        _require_non_negative_int(self.start_ms, "start_ms")
        _require_positive_int(self.end_ms, "end_ms")
        if self.end_ms <= self.start_ms:
            raise ValueError("end_ms must be greater than start_ms")
        if self.end_ms - self.start_ms > MAX_REPLAY_WINDOW_MS:
            raise ValueError("replay range cannot exceed 24 hours")
        if not self.lags_ms:
            raise ValueError("at least one lag is required")
        if len(self.lags_ms) > MAX_LAG_CANDIDATES:
            raise ValueError(
                f"at most {MAX_LAG_CANDIDATES} lag candidates are allowed"
            )
        for lag_ms in self.lags_ms:
            _require_positive_int(lag_ms, "lag_ms")
            if lag_ms > MAX_LAG_MS:
                raise ValueError(f"lag_ms cannot exceed {MAX_LAG_MS}")
        if len(set(self.lags_ms)) != len(self.lags_ms):
            raise ValueError("lag candidates must be unique")
        if self.end_ms - self.start_ms <= max(self.lags_ms):
            raise ValueError("replay range must exceed the maximum lag")

        _require_finite_decimal(self.beta, "beta")
        if self.beta < 0:
            raise ValueError("beta must be non-negative")
        _beta_version(self.beta)

        for field_name in (
            "poll_ms",
            "evaluation_interval_ms",
            "futures_stale_ms",
            "chainlink_stale_ms",
            "history_retention_ms",
            "volatility_lookback_ms",
            "near_expiry_ms",
            "near_reconnect_ms",
            "quantile_sample_max",
        ):
            _require_positive_int(getattr(self, field_name), field_name)
        _require_non_negative_int(
            self.reference_max_gap_ms,
            "reference_max_gap_ms",
        )
        _require_non_negative_int(self.max_future_skew_ms, "max_future_skew_ms")
        _require_non_negative_int(
            self.futures_availability_delay_ms,
            "futures_availability_delay_ms",
        )
        _require_non_negative_int(
            self.chainlink_availability_delay_ms,
            "chainlink_availability_delay_ms",
        )
        _require_non_negative_int(
            self.evaluation_phase_offset_ms,
            "evaluation_phase_offset_ms",
        )
        if self.poll_ms < MIN_POLL_MS or self.poll_ms % MIN_POLL_MS != 0:
            raise ValueError("poll_ms must be a multiple of 100 and at least 100")
        if self.evaluation_interval_ms < MIN_EVALUATION_INTERVAL_MS:
            raise ValueError("evaluation_interval_ms must be at least 500")
        if self.evaluation_interval_ms % self.poll_ms != 0:
            raise ValueError("poll_ms must divide evaluation_interval_ms")
        if self.evaluation_phase_offset_ms >= self.evaluation_interval_ms:
            raise ValueError(
                "evaluation_phase_offset_ms must be less than "
                "evaluation_interval_ms"
            )
        if self.evaluation_phase_offset_ms % self.poll_ms != 0:
            raise ValueError(
                "evaluation_phase_offset_ms must be a multiple of poll_ms"
            )
        if self.quantile_sample_max > MAX_QUANTILE_SAMPLE_MAX:
            raise ValueError(
                "quantile_sample_max cannot exceed "
                f"{MAX_QUANTILE_SAMPLE_MAX}"
            )
        if len(self.lags_ms) * self.quantile_sample_max > (
            MAX_CANDIDATE_QUANTILE_BUDGET
        ):
            raise ValueError(
                "lag candidate count times quantile_sample_max cannot exceed "
                f"{MAX_CANDIDATE_QUANTILE_BUDGET}"
            )
        if self.history_retention_ms > MAX_HISTORY_RETENTION_MS:
            raise ValueError(
                "history_retention_ms cannot exceed "
                f"{MAX_HISTORY_RETENTION_MS}"
            )
        if self.volatility_lookback_ms > MAX_VOLATILITY_LOOKBACK_MS:
            raise ValueError(
                "volatility_lookback_ms cannot exceed "
                f"{MAX_VOLATILITY_LOOKBACK_MS}"
            )

        minimum_retention_ms = (
            max(self.lags_ms)
            + self.chainlink_stale_ms
            + self.reference_max_gap_ms
        )
        if self.history_retention_ms < minimum_retention_ms:
            raise ValueError(
                "history_retention_ms must cover maximum lag, Chainlink "
                "freshness, and reference gap"
            )

        _require_finite_decimal(self.neutral_band_bps, "neutral_band_bps")
        if self.neutral_band_bps < 0:
            raise ValueError("neutral_band_bps must be non-negative")
        self._validate_threshold_pair(
            self.move_size_thresholds_bps,
            "move_size_thresholds_bps",
        )
        self._validate_threshold_pair(
            self.volatility_thresholds_bps,
            "volatility_thresholds_bps",
        )
        if not isinstance(self.exclude_parse_error_sessions, bool):
            raise TypeError("exclude_parse_error_sessions must be bool")

    @staticmethod
    def _validate_threshold_pair(
        values: tuple[Decimal, Decimal],
        field_name: str,
    ) -> None:
        if not isinstance(values, tuple) or len(values) != 2:
            raise TypeError(f"{field_name} must be a two-item tuple")
        for value in values:
            _require_finite_decimal(value, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
        if values[0] >= values[1]:
            raise ValueError(f"{field_name} values must be increasing")

    @property
    def models(self) -> tuple[CatchupModel, ...]:
        beta_version = _beta_version(self.beta)
        return tuple(
            CatchupModel(
                version=f"catchup_ratio_l{lag_ms}_{beta_version}",
                lag_ms=lag_ms,
                beta=self.beta,
            )
            for lag_ms in self.lags_ms
        )


@dataclass(frozen=True)
class ReplayEvent:
    kind: str
    received_wall_ns: int
    received_monotonic_ns: int
    connection_id: UUID
    sequence: int
    source_timestamp_ms: int
    value: Decimal
    event_count: int = 1

    def __post_init__(self) -> None:
        if self.kind not in (FUTURES_EVENT, CHAINLINK_EVENT):
            raise ValueError("invalid replay event kind")
        _require_positive_int(self.received_wall_ns, "received_wall_ns")
        _require_positive_int(
            self.received_monotonic_ns,
            "received_monotonic_ns",
        )
        if not isinstance(self.connection_id, UUID):
            raise TypeError("connection_id must be UUID")
        _require_non_negative_int(self.sequence, "sequence")
        _require_positive_int(self.source_timestamp_ms, "source_timestamp_ms")
        _require_finite_decimal(self.value, "value")
        if self.value <= 0:
            raise ValueError("value must be positive")
        _require_positive_int(self.event_count, "event_count")

    @property
    def received_ms(self) -> int:
        return self.received_wall_ns // NS_PER_MS

    @property
    def observed_price(self) -> ObservedPrice:
        return ObservedPrice(
            value=self.value,
            source_timestamp_ms=self.source_timestamp_ms,
            received_ms=self.received_ms,
        )

    @property
    def sort_key(self) -> tuple[int, int, int, int, str]:
        kind_order = 0 if self.kind == FUTURES_EVENT else 1
        return (
            self.received_wall_ns,
            kind_order,
            self.received_monotonic_ns,
            self.sequence,
            str(self.connection_id),
        )


@dataclass(frozen=True)
class V4CausalReplayConfig:
    """One frozen timing cell and scoring window for the isolated v4 replay.

    Unlike :class:`ReplayConfig`, every forecast setting and timing assumption
    comes from the already-validated experiment contract.  Keeping this as a
    separate type prevents a five-lag legacy replay from being mistaken for v4
    evidence.
    """

    scoring_start_ms: int
    scoring_end_ms: int
    contract: V4ExperimentContract
    timing_cell: TimingCell

    def __post_init__(self) -> None:
        _require_non_negative_int(self.scoring_start_ms, "scoring_start_ms")
        _require_positive_int(self.scoring_end_ms, "scoring_end_ms")
        if self.scoring_end_ms <= self.scoring_start_ms:
            raise ValueError("scoring_end_ms must be greater than scoring_start_ms")
        if self.scoring_end_ms - self.scoring_start_ms > DAY_MS:
            raise ValueError("v4 scoring range cannot exceed one UTC day")
        if not isinstance(self.contract, V4ExperimentContract):
            raise TypeError("contract must be V4ExperimentContract")
        if not isinstance(self.timing_cell, TimingCell):
            raise TypeError("timing_cell must be TimingCell")
        if self.timing_cell not in V4_TIMING_CELLS:
            raise ValueError("timing_cell is not one of the frozen v4 cells")

        candidates = self.contract.candidate_configs
        if tuple(item.lag_ms for item in candidates) != COMPARISON_LAGS_MS:
            raise ValueError("v4 replay contract has the wrong comparison family")
        configurations = (*candidates, self.control_config)
        if any(item.lag_ms != item.horizon_ms for item in configurations):
            raise ValueError("v4 replay requires lag-aligned horizons")
        if any(item.horizon_ms % V4_POLL_MS for item in configurations):
            raise ValueError("every v4 replay target must be poll aligned")
        if self.timing_cell.phase_offset_ms % V4_POLL_MS:
            raise ValueError("v4 timing-cell phase must be poll aligned")

    @property
    def candidate_configs(self) -> tuple[ForecastConfig, ...]:
        return self.contract.candidate_configs

    @property
    def control_config(self) -> ForecastConfig:
        return self.contract.active_incumbent.forecast_config

    @property
    def maximum_horizon_ms(self) -> int:
        return max(
            *(item.horizon_ms for item in self.candidate_configs),
            self.control_config.horizon_ms,
        )

    @property
    def maximum_history_retention_ms(self) -> int:
        return max(
            *(item.history_retention_ms for item in self.candidate_configs),
            self.control_config.history_retention_ms,
        )

    @property
    def archive_input_start_ms(self) -> int:
        return max(
            0,
            self.scoring_start_ms
            - self.maximum_history_retention_ms
            - V4_POLL_MS,
        )

    @property
    def archive_input_end_ms(self) -> int:
        return (
            self.scoring_end_ms
            + self.maximum_horizon_ms
            + FINALIZATION_ALLOWANCE_MS
        )


@dataclass(frozen=True)
class V4VisibleObservation:
    """A raw observation plus its simulated worker-visibility identity."""

    kind: str
    value: Decimal
    received_wall_ns: int
    received_monotonic_ns: int
    available_wall_ns: int
    visible_ms: int
    source_timestamp_ms: int
    connection_id: UUID
    source_sequence: int
    publisher_epoch: None = None
    accepted_event_sequence: None = None

    def __post_init__(self) -> None:
        if self.kind not in (FUTURES_EVENT, CHAINLINK_EVENT):
            raise ValueError("invalid v4 observation kind")
        _require_finite_decimal(self.value, "value")
        if self.value <= 0:
            raise ValueError("value must be positive")
        _require_positive_int(self.received_wall_ns, "received_wall_ns")
        _require_positive_int(
            self.received_monotonic_ns,
            "received_monotonic_ns",
        )
        _require_positive_int(self.available_wall_ns, "available_wall_ns")
        _require_non_negative_int(self.visible_ms, "visible_ms")
        _require_positive_int(self.source_timestamp_ms, "source_timestamp_ms")
        if not isinstance(self.connection_id, UUID):
            raise TypeError("connection_id must be UUID")
        _require_non_negative_int(self.source_sequence, "source_sequence")
        if self.available_wall_ns < self.received_wall_ns:
            raise ValueError("available_wall_ns cannot precede raw receipt")
        poll_ns = V4_POLL_MS * NS_PER_MS
        expected_visible_ms = (
            (self.available_wall_ns + poll_ns - 1) // poll_ns
        ) * V4_POLL_MS
        if self.visible_ms != expected_visible_ms:
            raise ValueError("visible_ms does not match the frozen ceiling rule")
        if self.publisher_epoch is not None:
            raise ValueError("raw v4 replay did not capture publisher_epoch")
        if self.accepted_event_sequence is not None:
            raise ValueError(
                "raw v4 replay did not capture accepted_event_sequence"
            )

    @property
    def received_ms(self) -> int:
        return self.received_wall_ns // NS_PER_MS

    @property
    def raw_order(self) -> tuple[int, int, int, int, str]:
        kind_order = 0 if self.kind == FUTURES_EVENT else 1
        return (
            self.received_wall_ns,
            kind_order,
            self.received_monotonic_ns,
            self.source_sequence,
            str(self.connection_id),
        )

    @property
    def identity(self) -> tuple[Any, ...]:
        return (
            self.kind,
            self.value,
            self.received_wall_ns,
            self.received_monotonic_ns,
            self.available_wall_ns,
            self.visible_ms,
            self.source_timestamp_ms,
            self.connection_id,
            self.source_sequence,
            self.publisher_epoch,
            self.accepted_event_sequence,
        )

    @property
    def observed_price(self) -> ObservedPrice:
        return ObservedPrice(
            value=self.value,
            source_timestamp_ms=self.source_timestamp_ms,
            received_ms=self.received_ms,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "value": self.value,
            "received_wall_ns": self.received_wall_ns,
            "received_ms": self.received_ms,
            "received_monotonic_ns": self.received_monotonic_ns,
            "available_wall_ns": self.available_wall_ns,
            "visible_ms": self.visible_ms,
            "source_timestamp_ms": self.source_timestamp_ms,
            "connection_id": str(self.connection_id),
            "source_sequence": self.source_sequence,
            "publisher_epoch": None,
            "accepted_event_sequence": None,
            "publisher_epoch_capture": V4_NOT_CAPTURED,
            "accepted_event_sequence_capture": V4_NOT_CAPTURED,
        }


@dataclass(frozen=True)
class V4ForecastAttempt:
    identity: ModelIdentity
    lag_ms: int
    horizon_ms: int
    beta: Decimal
    generated_ms: int
    target_ms: int
    valid: bool
    status: str
    invalid_reasons: tuple[str, ...]
    chainlink_anchor: Optional[V4VisibleObservation]
    futures_now: Optional[V4VisibleObservation]
    futures_reference: Optional[V4VisibleObservation]
    futures_reference_target_ms: Optional[int]
    futures_reference_gap_ms: Optional[int]
    projected_chainlink: Optional[Decimal]
    matched_no_change_prediction: Optional[Decimal]
    actual_chainlink: Optional[V4VisibleObservation] = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ModelIdentity):
            raise TypeError("identity must be ModelIdentity")
        _require_positive_int(self.lag_ms, "lag_ms")
        _require_positive_int(self.horizon_ms, "horizon_ms")
        _require_finite_decimal(self.beta, "beta")
        _require_non_negative_int(self.generated_ms, "generated_ms")
        if self.target_ms != self.generated_ms + self.horizon_ms:
            raise ValueError("target_ms must equal generated_ms plus horizon_ms")
        if not isinstance(self.valid, bool):
            raise TypeError("valid must be bool")
        if not isinstance(self.status, str) or not self.status:
            raise ValueError("status must be a non-empty string")
        if not isinstance(self.invalid_reasons, tuple) or not all(
            isinstance(item, str) and item for item in self.invalid_reasons
        ):
            raise TypeError("invalid_reasons must be a tuple of strings")
        for field_name in (
            "chainlink_anchor",
            "futures_now",
            "futures_reference",
            "actual_chainlink",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, V4VisibleObservation):
                raise TypeError(f"{field_name} must be V4VisibleObservation or None")
        if self.valid:
            if self.status != VALID or self.invalid_reasons:
                raise ValueError("valid v4 forecast has inconsistent status")
            required = (
                self.chainlink_anchor,
                self.futures_now,
                self.futures_reference,
                self.futures_reference_target_ms,
                self.futures_reference_gap_ms,
                self.projected_chainlink,
                self.matched_no_change_prediction,
            )
            if any(item is None for item in required):
                raise ValueError("valid v4 forecast is missing forecast inputs")
        else:
            if self.status == VALID or not self.invalid_reasons:
                raise ValueError("invalid v4 forecast has inconsistent status")
            if self.projected_chainlink is not None:
                raise ValueError("invalid v4 forecast cannot expose a projection")
            if self.matched_no_change_prediction is not None:
                raise ValueError("invalid v4 forecast cannot expose a baseline")
        for field_name in ("projected_chainlink", "matched_no_change_prediction"):
            value = getattr(self, field_name)
            if value is not None:
                _require_finite_decimal(value, field_name)
        if self.futures_reference_gap_ms is not None:
            _require_non_negative_int(
                self.futures_reference_gap_ms,
                "futures_reference_gap_ms",
            )
        for observation in (
            self.chainlink_anchor,
            self.futures_now,
            self.futures_reference,
        ):
            if observation is not None and observation.visible_ms > self.generated_ms:
                raise ValueError("forecast input was not visible by generation")
        if self.actual_chainlink is not None:
            if self.actual_chainlink.kind != CHAINLINK_EVENT:
                raise ValueError("forecast actual must be a Chainlink observation")
            if self.actual_chainlink.visible_ms > self.target_ms:
                raise ValueError("forecast actual was not visible by its target")
            if self.actual_chainlink.received_wall_ns > self.target_ms * NS_PER_MS:
                raise ValueError("forecast actual arrived after its exact target")

    def with_actual(
        self,
        actual: Optional[V4VisibleObservation],
    ) -> V4ForecastAttempt:
        return replace(self, actual_chainlink=actual)

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "lag_ms": self.lag_ms,
            "horizon_ms": self.horizon_ms,
            "beta": self.beta,
            "generated_ms": self.generated_ms,
            "target_ms": self.target_ms,
            "valid": self.valid,
            "status": self.status,
            "invalid_reasons": list(self.invalid_reasons),
            "chainlink_anchor": (
                None
                if self.chainlink_anchor is None
                else self.chainlink_anchor.to_dict()
            ),
            "futures_now": (
                None if self.futures_now is None else self.futures_now.to_dict()
            ),
            "futures_reference": (
                None
                if self.futures_reference is None
                else self.futures_reference.to_dict()
            ),
            "futures_reference_target_ms": self.futures_reference_target_ms,
            "futures_reference_gap_ms": self.futures_reference_gap_ms,
            "projected_chainlink": self.projected_chainlink,
            "matched_no_change_prediction": self.matched_no_change_prediction,
            "actual_chainlink": (
                None
                if self.actual_chainlink is None
                else self.actual_chainlink.to_dict()
            ),
        }


@dataclass(frozen=True)
class V4OriginCohort:
    cell_id: str
    generated_ms: int
    finalization_ms: int
    target_eligible: bool
    generation_eligible: bool
    common_scored: bool
    decision_eligible: bool
    candidate_attempts: tuple[V4ForecastAttempt, ...]
    control_attempt: V4ForecastAttempt
    integrity_epoch_at_generation: int
    integrity_epoch_at_finalization: int
    integrity_reset_before_finalization: bool
    structural_exclusion_reason: Optional[str]
    missing_reasons: tuple[str, ...]
    causal_violation_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.cell_id, str) or not self.cell_id:
            raise ValueError("cell_id must be a non-empty string")
        _require_non_negative_int(self.generated_ms, "generated_ms")
        _require_non_negative_int(self.finalization_ms, "finalization_ms")
        if self.finalization_ms < self.generated_ms:
            raise ValueError("finalization_ms cannot precede generation")
        for field_name in (
            "target_eligible",
            "generation_eligible",
            "common_scored",
            "decision_eligible",
            "integrity_reset_before_finalization",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be bool")
        if self.generation_eligible and not self.target_eligible:
            raise ValueError("generation eligibility requires target eligibility")
        if self.common_scored and not self.generation_eligible:
            raise ValueError("common scoring requires generation eligibility")
        if self.decision_eligible and not self.common_scored:
            raise ValueError("decision eligibility requires common scoring")
        if self.common_scored and self.integrity_reset_before_finalization:
            raise ValueError("an integrity-reset cohort cannot be common scored")
        if len(self.candidate_attempts) != len(COMPARISON_LAGS_MS):
            raise ValueError("v4 cohort must contain all five candidates")
        if not all(
            isinstance(item, V4ForecastAttempt)
            for item in self.candidate_attempts
        ):
            raise TypeError("candidate_attempts must contain v4 forecasts")
        if not isinstance(self.control_attempt, V4ForecastAttempt):
            raise TypeError("control_attempt must be V4ForecastAttempt")
        if self.common_scored and any(
            not item.valid or item.actual_chainlink is None
            for item in self.candidate_attempts
        ):
            raise ValueError("common-scored cohort is missing candidate evidence")
        if self.decision_eligible and (
            not self.control_attempt.valid
            or self.control_attempt.actual_chainlink is None
        ):
            raise ValueError("decision-eligible cohort is missing control evidence")
        _require_non_negative_int(
            self.integrity_epoch_at_generation,
            "integrity_epoch_at_generation",
        )
        _require_non_negative_int(
            self.integrity_epoch_at_finalization,
            "integrity_epoch_at_finalization",
        )
        if self.integrity_reset_before_finalization != (
            self.integrity_epoch_at_generation
            != self.integrity_epoch_at_finalization
        ):
            raise ValueError("integrity reset flag differs from the epoch change")
        if self.structural_exclusion_reason is not None and self.target_eligible:
            raise ValueError("target-eligible origin cannot be structurally excluded")
        if not isinstance(self.missing_reasons, tuple) or tuple(
            sorted(set(self.missing_reasons))
        ) != self.missing_reasons:
            raise ValueError("missing_reasons must be sorted and unique")
        _require_non_negative_int(
            self.causal_violation_count,
            "causal_violation_count",
        )
        if self.causal_violation_count:
            raise ValueError("v4 causal replay cannot publish a causal violation")

    def to_dict(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "generated_ms": self.generated_ms,
            "finalization_ms": self.finalization_ms,
            "target_eligible": self.target_eligible,
            "generation_eligible": self.generation_eligible,
            "common_scored": self.common_scored,
            "decision_eligible": self.decision_eligible,
            "candidate_attempts": [
                item.to_dict() for item in self.candidate_attempts
            ],
            "control_attempt": self.control_attempt.to_dict(),
            "integrity_epoch_at_generation": self.integrity_epoch_at_generation,
            "integrity_epoch_at_finalization": self.integrity_epoch_at_finalization,
            "integrity_reset_before_finalization": (
                self.integrity_reset_before_finalization
            ),
            "structural_exclusion_reason": self.structural_exclusion_reason,
            "missing_reasons": list(self.missing_reasons),
            "causal_violation_count": self.causal_violation_count,
        }


@dataclass(frozen=True)
class ReplaySession:
    connection_id: UUID
    source: str
    connected_wall_ns: int
    ready_wall_ns: Optional[int]
    disconnected_wall_ns: Optional[int]
    messages_accepted_total: int
    parse_errors_total: int
    records_dropped_total: int
    raw_row_count: Optional[int] = None
    raw_accepted_total: Optional[int] = None
    duplicate_key_count: Optional[int] = None
    monotonic_regression_count: Optional[int] = None
    wall_regression_count: Optional[int] = None
    out_of_session_count: Optional[int] = None
    integrity_checked: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.connection_id, UUID):
            raise TypeError("connection_id must be UUID")
        if self.source not in (
            FUTURES_SESSION_SOURCE,
            CHAINLINK_SESSION_SOURCE,
        ):
            raise ValueError("invalid replay session source")
        _require_positive_int(self.connected_wall_ns, "connected_wall_ns")
        for field_name in ("ready_wall_ns", "disconnected_wall_ns"):
            value = getattr(self, field_name)
            if value is not None:
                _require_positive_int(value, field_name)
        if (
            self.ready_wall_ns is not None
            and self.ready_wall_ns < self.connected_wall_ns
        ):
            raise ValueError("session ready time precedes connection")
        if (
            self.disconnected_wall_ns is not None
            and self.disconnected_wall_ns < self.connected_wall_ns
        ):
            raise ValueError("session disconnect time precedes connection")
        if (
            self.ready_wall_ns is not None
            and self.disconnected_wall_ns is not None
            and self.disconnected_wall_ns <= self.ready_wall_ns
        ):
            raise ValueError("session disconnect must follow ready time")
        for field_name in (
            "messages_accepted_total",
            "parse_errors_total",
            "records_dropped_total",
        ):
            _require_non_negative_int(getattr(self, field_name), field_name)
        for field_name in (
            "raw_row_count",
            "raw_accepted_total",
            "duplicate_key_count",
            "monotonic_regression_count",
            "wall_regression_count",
            "out_of_session_count",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_non_negative_int(value, field_name)
        if not isinstance(self.integrity_checked, bool):
            raise TypeError("integrity_checked must be bool")

    def exclusion_reasons(self, config: ReplayConfig) -> tuple[str, ...]:
        reasons = []
        if self.ready_wall_ns is None:
            reasons.append("never_ready")
        if self.disconnected_wall_ns is None:
            reasons.append("open_unverified")
        if self.records_dropped_total:
            reasons.append("records_dropped")
        if config.exclude_parse_error_sessions and self.parse_errors_total:
            reasons.append("parse_errors")
        if not self.integrity_checked:
            reasons.append("integrity_unverified")
        else:
            if self.raw_accepted_total != self.messages_accepted_total:
                reasons.append("accepted_count_mismatch_or_retention_truncated")
            if self.duplicate_key_count:
                reasons.append("duplicate_raw_keys")
            if self.monotonic_regression_count:
                reasons.append("monotonic_regression")
            if self.wall_regression_count:
                reasons.append("wall_clock_regression")
            if self.out_of_session_count:
                reasons.append("raw_rows_outside_session")
        return tuple(reasons)


@dataclass(frozen=True)
class ReplaySegment:
    start_wall_ns: int
    end_wall_ns: int
    futures_session_id: UUID
    chainlink_session_id: UUID

    def __post_init__(self) -> None:
        _require_positive_int(self.start_wall_ns, "start_wall_ns")
        _require_positive_int(self.end_wall_ns, "end_wall_ns")
        if self.end_wall_ns <= self.start_wall_ns:
            raise ValueError("replay segment must have positive duration")


@dataclass(frozen=True)
class SessionSelection:
    segments: tuple[ReplaySegment, ...]
    eligible_session_ids: frozenset[UUID]
    total_by_source: Mapping[str, int]
    eligible_by_source: Mapping[str, int]
    excluded_by_reason: Mapping[str, int]
    excluded_integrity_scope_raw_rows: int


def select_replay_sessions(
    sessions: Sequence[ReplaySession],
    config: ReplayConfig,
) -> SessionSelection:
    total_by_source: Counter[str] = Counter()
    eligible_by_source: Counter[str] = Counter()
    excluded_by_reason: Counter[str] = Counter()
    excluded_raw_rows = 0
    eligible: dict[str, list[ReplaySession]] = {
        FUTURES_SESSION_SOURCE: [],
        CHAINLINK_SESSION_SOURCE: [],
    }

    for session in sessions:
        total_by_source[session.source] += 1
        reasons = session.exclusion_reasons(config)
        if reasons:
            excluded_by_reason.update(reasons)
            excluded_raw_rows += session.raw_row_count or 0
            continue
        eligible[session.source].append(session)
        eligible_by_source[session.source] += 1

    for source_sessions in eligible.values():
        source_sessions.sort(key=lambda session: session.ready_wall_ns or 0)
        for previous, current in zip(source_sessions, source_sessions[1:]):
            if previous.disconnected_wall_ns > current.ready_wall_ns:
                raise ReplayDataError("eligible sessions overlap for one source")

    segments: list[ReplaySegment] = []
    futures_sessions = eligible[FUTURES_SESSION_SOURCE]
    chainlink_sessions = eligible[CHAINLINK_SESSION_SOURCE]
    futures_index = 0
    chainlink_index = 0
    while (
        futures_index < len(futures_sessions)
        and chainlink_index < len(chainlink_sessions)
    ):
        futures_session = futures_sessions[futures_index]
        chainlink_session = chainlink_sessions[chainlink_index]
        start_wall_ns = max(
            futures_session.ready_wall_ns,
            chainlink_session.ready_wall_ns,
        )
        end_wall_ns = min(
            futures_session.disconnected_wall_ns,
            chainlink_session.disconnected_wall_ns,
        )
        if start_wall_ns < end_wall_ns:
            segments.append(
                ReplaySegment(
                    start_wall_ns=start_wall_ns,
                    end_wall_ns=end_wall_ns,
                    futures_session_id=futures_session.connection_id,
                    chainlink_session_id=chainlink_session.connection_id,
                )
            )
        if (
            futures_session.disconnected_wall_ns
            <= chainlink_session.disconnected_wall_ns
        ):
            futures_index += 1
        else:
            chainlink_index += 1

    report_start_ns = config.start_ms * NS_PER_MS
    report_end_ns = config.end_ms * NS_PER_MS
    segments = [
        segment
        for segment in segments
        if segment.start_wall_ns < report_end_ns
        and segment.end_wall_ns > report_start_ns
    ]
    eligible_session_ids = frozenset(
        connection_id
        for segment in segments
        for connection_id in (
            segment.futures_session_id,
            segment.chainlink_session_id,
        )
    )
    return SessionSelection(
        segments=tuple(segments),
        eligible_session_ids=eligible_session_ids,
        total_by_source=dict(total_by_source),
        eligible_by_source=dict(eligible_by_source),
        excluded_by_reason=dict(excluded_by_reason),
        excluded_integrity_scope_raw_rows=excluded_raw_rows,
    )


@dataclass(frozen=True)
class MaturedForecast:
    model_version: str
    generated_ms: int
    target_ms: int
    horizon_ms: int
    chainlink_at_forecast: Decimal
    projected_chainlink: Decimal
    baseline_chainlink: Decimal
    predicted_move_bps: Decimal
    market_id: int
    ms_to_market_end: int
    full_horizon_before_market_end: bool
    volatility_bps: Optional[Decimal]
    ms_since_segment_start: int
    ms_until_segment_end: int
    common_cohort_member: bool


@dataclass(frozen=True)
class ForecastOutcome:
    forecast: MaturedForecast
    actual_chainlink: Decimal
    actual_received_ms: int
    actual_age_ms: int
    model_error: Decimal
    baseline_error: Decimal
    model_error_bps: Decimal
    baseline_error_bps: Decimal
    actual_move_bps: Decimal
    absolute_advantage: Decimal


class _MetricAggregate:
    def __init__(
        self,
        *,
        keep_medians: bool,
        sample_max: int,
        seed: int,
    ) -> None:
        self.count = 0
        self.model_absolute_error_sum = Decimal("0")
        self.baseline_absolute_error_sum = Decimal("0")
        self.model_squared_error_sum = Decimal("0")
        self.baseline_squared_error_sum = Decimal("0")
        self.wins = 0
        self.ties = 0
        self.losses = 0
        self.directional_confusion: Counter[tuple[str, str]] = Counter()
        self._keep_medians = keep_medians
        self._model_absolute_errors = _BoundedSample(
            sample_max,
            seed=seed + 1,
        )
        self._model_absolute_error_bps = _BoundedSample(
            sample_max,
            seed=seed + 2,
        )
        self._baseline_absolute_errors = _BoundedSample(
            sample_max,
            seed=seed + 3,
        )
        self._baseline_absolute_error_bps = _BoundedSample(
            sample_max,
            seed=seed + 4,
        )
        self._advantages = _BoundedSample(sample_max, seed=seed + 5)

    def add(self, outcome: ForecastOutcome, *, neutral_band_bps: Decimal) -> None:
        model_absolute_error = abs(outcome.model_error)
        baseline_absolute_error = abs(outcome.baseline_error)
        model_absolute_error_bps = abs(outcome.model_error_bps)
        baseline_absolute_error_bps = abs(outcome.baseline_error_bps)
        self.count += 1
        self.model_absolute_error_sum += model_absolute_error
        self.baseline_absolute_error_sum += baseline_absolute_error
        self.model_squared_error_sum += outcome.model_error * outcome.model_error
        self.baseline_squared_error_sum += (
            outcome.baseline_error * outcome.baseline_error
        )
        if model_absolute_error < baseline_absolute_error:
            self.wins += 1
        elif model_absolute_error > baseline_absolute_error:
            self.losses += 1
        else:
            self.ties += 1

        actual_direction = _direction_with_band(
            outcome.actual_move_bps,
            neutral_band_bps,
        )
        predicted_direction = _direction_with_band(
            outcome.forecast.predicted_move_bps,
            neutral_band_bps,
        )
        self.directional_confusion[(actual_direction, predicted_direction)] += 1

        if self._keep_medians:
            self._model_absolute_errors.add(model_absolute_error)
            self._model_absolute_error_bps.add(model_absolute_error_bps)
            self._baseline_absolute_errors.add(baseline_absolute_error)
            self._baseline_absolute_error_bps.add(baseline_absolute_error_bps)
            self._advantages.add(outcome.absolute_advantage)

    def summary(self) -> dict[str, Any]:
        absolute_advantage_sum = (
            self.baseline_absolute_error_sum
            - self.model_absolute_error_sum
        )
        model_mae = _decimal_mean(self.model_absolute_error_sum, self.count)
        baseline_mae = _decimal_mean(self.baseline_absolute_error_sum, self.count)
        model_rmse = (
            _decimal_sqrt(self.model_squared_error_sum / Decimal(self.count))
            if self.count
            else None
        )
        baseline_rmse = (
            _decimal_sqrt(self.baseline_squared_error_sum / Decimal(self.count))
            if self.count
            else None
        )
        mae_skill = None
        if baseline_mae not in (None, Decimal("0")):
            mae_skill = Decimal("1") - model_mae / baseline_mae
        payload = {
            "count": self.count,
            "model_mean_absolute_error_usd": model_mae,
            "baseline_mean_absolute_error_usd": baseline_mae,
            "model_rmse_usd": model_rmse,
            "baseline_rmse_usd": baseline_rmse,
            "mean_absolute_advantage_usd": _decimal_mean(
                absolute_advantage_sum,
                self.count,
            ),
            "mae_skill_vs_no_change": mae_skill,
            "wins": self.wins,
            "ties": self.ties,
            "losses": self.losses,
            "win_rate": _decimal_rate(self.wins, self.count),
            "tie_rate": _decimal_rate(self.ties, self.count),
            "loss_rate": _decimal_rate(self.losses, self.count),
            "directional": _directional_summary(
                self.directional_confusion,
                count=self.count,
            ),
            "sufficient_statistics": {
                "model_absolute_error_sum_usd": (
                    self.model_absolute_error_sum
                ),
                "baseline_absolute_error_sum_usd": (
                    self.baseline_absolute_error_sum
                ),
                "model_squared_error_sum_usd2": (
                    self.model_squared_error_sum
                ),
                "baseline_squared_error_sum_usd2": (
                    self.baseline_squared_error_sum
                ),
                "absolute_advantage_sum_usd": absolute_advantage_sum,
            },
        }
        if self._keep_medians:
            payload.update(
                {
                    "model_median_absolute_error_usd": _median(
                        self._model_absolute_errors.snapshot()
                    ),
                    "model_median_absolute_error_bps": _median(
                        self._model_absolute_error_bps.snapshot()
                    ),
                    "baseline_median_absolute_error_usd": _median(
                        self._baseline_absolute_errors.snapshot()
                    ),
                    "baseline_median_absolute_error_bps": _median(
                        self._baseline_absolute_error_bps.snapshot()
                    ),
                    "median_absolute_advantage_usd": _median(
                        self._advantages.snapshot()
                    ),
                    "quantile_sampling": self._model_absolute_errors.metadata(),
                }
            )
        return payload


def _direction_with_band(value: Decimal, band: Decimal) -> str:
    if value > band:
        return "up"
    if value < -band:
        return "down"
    return "neutral"


class _CoverageAggregate:
    def __init__(self) -> None:
        self.scheduled = 0
        self.valid_generated = 0
        self.target_eligible = 0
        self.valid_target_eligible = 0

    def add(self, *, valid: bool, target_eligible: bool) -> None:
        self.scheduled += 1
        if valid:
            self.valid_generated += 1
        if target_eligible:
            self.target_eligible += 1
            if valid:
                self.valid_target_eligible += 1

    def summary(self) -> dict[str, Any]:
        return {
            "scheduled": self.scheduled,
            "valid_generated": self.valid_generated,
            "generation_coverage": _decimal_rate(
                self.valid_generated,
                self.scheduled,
            ),
            "target_eligible": self.target_eligible,
            "valid_target_eligible": self.valid_target_eligible,
        }


class _CandidateAccumulator:
    def __init__(self, model: CatchupModel, config: ReplayConfig) -> None:
        self.model = model
        self.config = config
        self.scheduled = 0
        self.valid_generated = 0
        self.target_eligible = 0
        self.valid_target_eligible = 0
        self.target_censored = 0
        self.scored = 0
        self.missing_actual = 0
        self.common_target_eligible = 0
        self.common_valid_generated = 0
        self.common_scored = 0
        self.invalid_statuses: Counter[str] = Counter()
        self.invalid_reasons: Counter[str] = Counter()
        seed = model.lag_ms + int(model.beta * Decimal("100"))
        self.reference_gaps_ms = _BoundedSample(
            config.quantile_sample_max,
            seed=seed + 10,
        )
        self.actual_ages_ms = _BoundedSample(
            config.quantile_sample_max,
            seed=seed + 20,
        )
        self.overall = _MetricAggregate(
            keep_medians=True,
            sample_max=config.quantile_sample_max,
            seed=seed + 30,
        )
        self.common_metrics = _MetricAggregate(
            keep_medians=True,
            sample_max=config.quantile_sample_max,
            seed=seed + 40,
        )
        self.slices: dict[str, dict[str, _MetricAggregate]] = {
            "actual_direction": {},
            "actual_move_size": {},
            "raw_bucket_return_rms_regime": {},
            "market_expiry": {},
            "session_boundary_proximity": {},
        }
        self.common_slices: dict[str, dict[str, _MetricAggregate]] = {
            dimension: {} for dimension in self.slices
        }
        self.coverage_slices: dict[str, dict[str, _CoverageAggregate]] = {
            "market_expiry": {},
            "session_boundary_proximity": {},
        }

    def attempt(
        self,
        signal: ModelSignal,
        *,
        target_eligible: bool,
        common_target_eligible: bool,
        common_cohort_member: bool,
        coverage_categories: Mapping[str, str],
    ) -> None:
        self.scheduled += 1
        if target_eligible:
            self.target_eligible += 1
        else:
            self.target_censored += 1
        if common_target_eligible:
            self.common_target_eligible += 1
        if common_cohort_member:
            self.common_valid_generated += 1
        for dimension, category in coverage_categories.items():
            self.coverage_slices[dimension].setdefault(
                category,
                _CoverageAggregate(),
            ).add(valid=signal.valid, target_eligible=target_eligible)
        if signal.valid:
            self.valid_generated += 1
            if target_eligible:
                self.valid_target_eligible += 1
            if signal.futures_reference_gap_ms is not None:
                self.reference_gaps_ms.add(signal.futures_reference_gap_ms)
            return
        self.invalid_statuses[signal.status] += 1
        self.invalid_reasons.update(signal.invalid_reasons)

    def score(self, outcome: ForecastOutcome) -> None:
        self.scored += 1
        self.actual_ages_ms.add(outcome.actual_age_ms)
        self.overall.add(
            outcome,
            neutral_band_bps=self.config.neutral_band_bps,
        )
        if outcome.forecast.common_cohort_member:
            self.common_scored += 1
            self.common_metrics.add(
                outcome,
                neutral_band_bps=self.config.neutral_band_bps,
            )
        categories = {
            "actual_direction": _direction_with_band(
                outcome.actual_move_bps,
                self.config.neutral_band_bps,
            ),
            "actual_move_size": _three_way_bucket(
                abs(outcome.actual_move_bps),
                self.config.move_size_thresholds_bps,
                labels=("small", "medium", "large"),
            ),
            "raw_bucket_return_rms_regime": (
                "unknown"
                if outcome.forecast.volatility_bps is None
                else _three_way_bucket(
                    outcome.forecast.volatility_bps,
                    self.config.volatility_thresholds_bps,
                    labels=("low", "medium", "high"),
                )
            ),
            "market_expiry": _expiry_bucket(outcome.forecast, self.config),
            "session_boundary_proximity": _session_boundary_bucket(
                outcome.forecast,
                self.config,
            ),
        }
        for dimension, category in categories.items():
            aggregate = self.slices[dimension].setdefault(
                category,
                _MetricAggregate(
                    keep_medians=False,
                    sample_max=self.config.quantile_sample_max,
                    seed=(
                        self.model.lag_ms
                        + sum(ord(character) for character in dimension + category)
                    ),
                ),
            )
            aggregate.add(
                outcome,
                neutral_band_bps=self.config.neutral_band_bps,
            )
            if outcome.forecast.common_cohort_member:
                common_aggregate = self.common_slices[dimension].setdefault(
                    category,
                    _MetricAggregate(
                        keep_medians=False,
                        sample_max=self.config.quantile_sample_max,
                        seed=(
                            self.model.lag_ms
                            + 10_000
                            + sum(
                                ord(character)
                                for character in dimension + category
                            )
                        ),
                    ),
                )
                common_aggregate.add(
                    outcome,
                    neutral_band_bps=self.config.neutral_band_bps,
                )

    def summary(self) -> dict[str, Any]:
        return {
            "model_version": self.model.version,
            "horizon_ms": self.model.lag_ms,
            "beta": self.model.beta,
            "scheduled": self.scheduled,
            "valid_generated": self.valid_generated,
            "generation_coverage": _decimal_rate(
                self.valid_generated,
                self.scheduled,
            ),
            "scored": self.scored,
            "target_eligible": self.target_eligible,
            "valid_target_eligible": self.valid_target_eligible,
            "target_censored": self.target_censored,
            "target_eligible_rate": _decimal_rate(
                self.target_eligible,
                self.scheduled,
            ),
            "scored_coverage": _decimal_rate(
                self.scored,
                self.target_eligible,
            ),
            "maturation_coverage": _decimal_rate(
                self.scored,
                self.valid_target_eligible,
            ),
            "missing_actual": self.missing_actual,
            "common_cohort": {
                "definition": (
                    "same_generated_ms_max_horizon_eligible_"
                    "all_models_valid"
                ),
                "target_eligible": self.common_target_eligible,
                "valid_generated": self.common_valid_generated,
                "scored": self.common_scored,
                "scored_coverage": _decimal_rate(
                    self.common_scored,
                    self.common_target_eligible,
                ),
                "maturation_coverage": _decimal_rate(
                    self.common_scored,
                    self.common_valid_generated,
                ),
                "metrics": self.common_metrics.summary(),
                "slices": {
                    dimension: {
                        category: aggregate.summary()
                        for category, aggregate in sorted(categories.items())
                    }
                    for dimension, categories in self.common_slices.items()
                },
            },
            "invalid_statuses": dict(sorted(self.invalid_statuses.items())),
            "invalid_reasons": dict(sorted(self.invalid_reasons.items())),
            "reference_gap_ms": _integer_distribution(self.reference_gaps_ms),
            "actual_chainlink_age_at_target_ms": _integer_distribution(
                self.actual_ages_ms
            ),
            "metrics": self.overall.summary(),
            "slices": {
                dimension: {
                    category: aggregate.summary()
                    for category, aggregate in sorted(categories.items())
                }
                for dimension, categories in self.slices.items()
            },
            "coverage_slices": {
                dimension: {
                    category: aggregate.summary()
                    for category, aggregate in sorted(categories.items())
                }
                for dimension, categories in self.coverage_slices.items()
            },
        }


def _three_way_bucket(
    value: Decimal,
    thresholds: tuple[Decimal, Decimal],
    *,
    labels: tuple[str, str, str],
) -> str:
    if value < thresholds[0]:
        return labels[0]
    if value < thresholds[1]:
        return labels[1]
    return labels[2]


def _expiry_bucket(forecast: MaturedForecast, config: ReplayConfig) -> str:
    return _expiry_category(
        full_horizon_before_market_end=(
            forecast.full_horizon_before_market_end
        ),
        ms_to_market_end=forecast.ms_to_market_end,
        config=config,
    )


def _expiry_category(
    *,
    full_horizon_before_market_end: bool,
    ms_to_market_end: int,
    config: ReplayConfig,
) -> str:
    if not full_horizon_before_market_end:
        return "horizon_crosses_market_end"
    if ms_to_market_end <= config.near_expiry_ms:
        return "near_market_end"
    return "regular"


def _session_boundary_bucket(
    forecast: MaturedForecast,
    config: ReplayConfig,
) -> str:
    return _session_boundary_category(
        ms_since_segment_start=forecast.ms_since_segment_start,
        ms_until_segment_end=forecast.ms_until_segment_end,
        config=config,
    )


def _session_boundary_category(
    *,
    ms_since_segment_start: int,
    ms_until_segment_end: int,
    config: ReplayConfig,
) -> str:
    near_start = ms_since_segment_start < config.near_reconnect_ms
    near_end = ms_until_segment_end <= config.near_reconnect_ms
    if near_start and near_end:
        return "near_both_segment_boundaries"
    if near_start:
        return "post_segment_start"
    if near_end:
        return "pre_segment_end"
    return "stable_segment"


def _integer_distribution(sample: _BoundedSample) -> dict[str, Any]:
    values = sample.snapshot()
    if not values:
        return {
            "count": 0,
            "p50": None,
            "p99": None,
            "max": None,
            "sampling": sample.metadata(),
        }
    return {
        "count": sample.population_size,
        "p50": _nearest_rank(values.copy(), Decimal("0.50")),
        "p99": _nearest_rank(values.copy(), Decimal("0.99")),
        "max": sample.maximum,
        "sampling": sample.metadata(),
    }


class _EventDiagnostics:
    def __init__(self, *, sample_max: int) -> None:
        self.counts: Counter[str] = Counter()
        self.interarrival_ns: dict[str, _BoundedSample] = {
            FUTURES_EVENT: _BoundedSample(sample_max, seed=101),
            CHAINLINK_EVENT: _BoundedSample(sample_max, seed=102),
        }
        self.receive_minus_source_ms: dict[str, _BoundedSample] = {
            FUTURES_EVENT: _BoundedSample(sample_max, seed=201),
            CHAINLINK_EVENT: _BoundedSample(sample_max, seed=202),
        }
        self._last_received_ns: dict[str, int] = {}

    def reset_segment(self) -> None:
        self._last_received_ns.clear()

    def observe(self, event: ReplayEvent) -> None:
        self.counts[event.kind] += 1
        previous_ns = self._last_received_ns.get(event.kind)
        if previous_ns is not None:
            self.interarrival_ns[event.kind].add(
                event.received_wall_ns - previous_ns
            )
        self._last_received_ns[event.kind] = event.received_wall_ns
        self.receive_minus_source_ms[event.kind].add(
            event.received_ms - event.source_timestamp_ms
        )

    def summary(self) -> dict[str, Any]:
        payload = {}
        for kind in (FUTURES_EVENT, CHAINLINK_EVENT):
            interarrival = self.interarrival_ns[kind]
            p50_ns = _nearest_rank(
                interarrival.snapshot(),
                Decimal("0.50"),
            )
            p99_ns = _nearest_rank(
                interarrival.snapshot(),
                Decimal("0.99"),
            )
            source_lags = self.receive_minus_source_ms[kind]
            payload[kind] = {
                "events": self.counts[kind],
                "interarrival_ms_p50": (
                    Decimal(p50_ns) / Decimal(NS_PER_MS)
                    if p50_ns is not None
                    else None
                ),
                "interarrival_ms_p99": (
                    Decimal(p99_ns) / Decimal(NS_PER_MS)
                    if p99_ns is not None
                    else None
                ),
                "receive_minus_source_ms_p50": _nearest_rank(
                    source_lags.snapshot(),
                    Decimal("0.50"),
                ),
                "receive_minus_source_ms_p99": _nearest_rank(
                    source_lags.snapshot(),
                    Decimal("0.99"),
                ),
                "interarrival_sampling": interarrival.metadata(),
                "transport_lag_sampling": source_lags.metadata(),
            }
        return payload


@dataclass(frozen=True)
class ReplayReport:
    config: ReplayConfig
    session_selection: SessionSelection
    candidate_summaries: tuple[Mapping[str, Any], ...]
    event_diagnostics: Mapping[str, Any]
    polls_processed: int
    input_events: int
    ignored_events: int

    @property
    def status(self) -> str:
        if not self.session_selection.segments:
            return "no_eligible_segments"
        overall_scores = [
            int(summary["scored"]) for summary in self.candidate_summaries
        ]
        if not any(overall_scores):
            return "no_scored_forecasts"
        common_scores = [
            int(summary["common_cohort"]["scored"])
            for summary in self.candidate_summaries
        ]
        if any(score == 0 for score in overall_scores + common_scores):
            return "partial_candidate_evidence"
        return "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "mode": "shadow_raw_replay",
            "status": self.status,
            "selection_performed": False,
            "comparison_cohort": {
                "definition": (
                    "same_generated_ms_max_horizon_eligible_"
                    "all_models_valid"
                ),
                "metrics_location": "candidates[].common_cohort.metrics",
                "required_for_status_ok": True,
            },
            "range": {
                "start_ms": self.config.start_ms,
                "end_ms": self.config.end_ms,
                "boundary": "[start_ms,end_ms)",
            },
            "configuration": {
                "poll_ms": self.config.poll_ms,
                "evaluation_interval_ms": self.config.evaluation_interval_ms,
                "lags_ms": list(self.config.lags_ms),
                "beta": self.config.beta,
                "futures_stale_ms": self.config.futures_stale_ms,
                "chainlink_stale_ms": self.config.chainlink_stale_ms,
                "reference_max_gap_ms": self.config.reference_max_gap_ms,
                "history_retention_ms": self.config.history_retention_ms,
                "max_future_skew_ms": self.config.max_future_skew_ms,
                "futures_availability_delay_ms": (
                    self.config.futures_availability_delay_ms
                ),
                "chainlink_availability_delay_ms": (
                    self.config.chainlink_availability_delay_ms
                ),
                "evaluation_phase_offset_ms": (
                    self.config.evaluation_phase_offset_ms
                ),
                "neutral_band_bps": self.config.neutral_band_bps,
                "move_size_thresholds_bps": list(
                    self.config.move_size_thresholds_bps
                ),
                "volatility_thresholds_bps": list(
                    self.config.volatility_thresholds_bps
                ),
                "volatility_lookback_ms": self.config.volatility_lookback_ms,
                "volatility_measure": "rms_of_consecutive_raw_bucket_returns",
                "volatility_time_basis": "worker_poll_visibility_ms",
                "near_expiry_ms": self.config.near_expiry_ms,
                "near_reconnect_ms": self.config.near_reconnect_ms,
                "session_boundary_measure": (
                    "time_since_common_segment_start_and_until_segment_end"
                ),
                "quantile_sample_max": self.config.quantile_sample_max,
                "exclude_parse_error_sessions": (
                    self.config.exclude_parse_error_sessions
                ),
            },
            "data_quality": {
                "session_policy": "completed_clean_integrity_checked",
                "conservative_reset_at_common_session_boundary": True,
                "sessions_total_by_source": dict(
                    self.session_selection.total_by_source
                ),
                "sessions_eligible_by_source": dict(
                    self.session_selection.eligible_by_source
                ),
                "sessions_excluded_by_reason": dict(
                    self.session_selection.excluded_by_reason
                ),
                "excluded_integrity_scope_raw_rows": (
                    self.session_selection.excluded_integrity_scope_raw_rows
                ),
                "common_healthy_segments": len(
                    self.session_selection.segments
                ),
                "input_events": self.input_events,
                "ignored_events": self.ignored_events,
                "polls_processed": self.polls_processed,
                "event_diagnostics": self.event_diagnostics,
                "limitations": [
                    "100 ms futures OHLC exposes only each bucket close at its last receive time.",
                    "Raw receive time precedes parsing and Redis publication latency.",
                    "Configured source visibility delays and evaluation phase are fixed sensitivity assumptions, not measured Redis publication-completion timing.",
                    "Volatility returns enter diagnostic slices at the worker poll where each delayed futures event becomes visible, and the lookback uses that poll time.",
                    "Open sessions are excluded because their persisted counters are not final.",
                    "Cross-model comparisons must use the common-cohort "
                    "metrics; top-level metrics include horizon-specific "
                    "edge rows.",
                    "Phase 4 raw-retention behavior remains unproven in production.",
                ],
            },
            "candidates": list(self.candidate_summaries),
        }


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def encode_replay_report(report: ReplayReport) -> str:
    return json.dumps(
        _json_ready(report.to_dict()),
        indent=2,
        sort_keys=True,
    )


def write_replay_report(path: Path, report: ReplayReport) -> None:
    if not isinstance(path, Path):
        raise TypeError("path must be pathlib.Path")
    temporary_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary_path.write_text(
            encode_replay_report(report) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


class _SegmentRuntime:
    def __init__(
        self,
        *,
        segment: ReplaySegment,
        config: ReplayConfig,
        accumulators: Mapping[str, _CandidateAccumulator],
        diagnostics: _EventDiagnostics,
        initial_futures: Optional[ReplayEvent] = None,
        initial_chainlink: Optional[ReplayEvent] = None,
    ) -> None:
        self.segment = segment
        self.effective_end_wall_ns = min(
            segment.end_wall_ns,
            config.end_ms * NS_PER_MS,
        )
        self.config = config
        self.accumulators = accumulators
        self.diagnostics = diagnostics
        warmup_start_ms = max(
            0,
            config.start_ms
            - config.history_retention_ms
            - config.poll_ms,
        )
        simulation_start_ns = max(
            segment.start_wall_ns,
            warmup_start_ms * NS_PER_MS,
        )
        self.simulation_start_wall_ns = simulation_start_ns
        simulation_start_ms = _ceil_to_multiple(
            (simulation_start_ns + NS_PER_MS - 1) // NS_PER_MS,
            config.poll_ms,
        )
        self.next_poll_ms = simulation_start_ms
        self.engine = self._new_engine()
        self.latest_futures: Optional[ReplayEvent] = None
        self.latest_chainlink: Optional[ReplayEvent] = None
        self._visibility_pending: list[
            tuple[int, tuple[int, int, int, int, str], ReplayEvent]
        ] = []
        if initial_futures is not None:
            self._queue_for_visibility(initial_futures)
        if initial_chainlink is not None:
            self._queue_for_visibility(initial_chainlink)
        self._chainlink_history: Deque[ReplayEvent] = deque()
        self._pending: list[tuple[int, int, MaturedForecast]] = []
        self._pending_sequence = 0
        self.polls_processed = 0
        self.events_processed = 0
        self._last_futures_for_volatility: Optional[ReplayEvent] = None
        self._volatility_returns: Deque[tuple[int, Decimal]] = deque()
        self._volatility_square_sum = Decimal("0")
        diagnostics.reset_segment()

    def _new_engine(self) -> ShadowSignalEngine:
        return ShadowSignalEngine(
            models=self.config.models,
            futures_stale_ms=self.config.futures_stale_ms,
            chainlink_stale_ms=self.config.chainlink_stale_ms,
            reference_max_gap_ms=self.config.reference_max_gap_ms,
            history_retention_ms=self.config.history_retention_ms,
            max_future_skew_ms=self.config.max_future_skew_ms,
        )

    def consume(self, event: ReplayEvent) -> None:
        self._advance_polls_before(event.received_wall_ns)
        self.events_processed += 1
        self.diagnostics.observe(event)
        self._queue_for_visibility(event)

    def _queue_for_visibility(self, event: ReplayEvent) -> None:
        delay_ms = (
            self.config.futures_availability_delay_ms
            if event.kind == FUTURES_EVENT
            else self.config.chainlink_availability_delay_ms
        )
        available_wall_ns = event.received_wall_ns + delay_ms * NS_PER_MS
        heapq.heappush(
            self._visibility_pending,
            (available_wall_ns, event.sort_key, event),
        )

    def _apply_visible_events(self, tick_ms: int) -> None:
        tick_wall_ns = tick_ms * NS_PER_MS
        while (
            self._visibility_pending
            and self._visibility_pending[0][0] <= tick_wall_ns
        ):
            _available_wall_ns, _sort_key, event = heapq.heappop(
                self._visibility_pending
            )
            if event.kind == FUTURES_EVENT:
                self.latest_futures = event
                self._observe_volatility_event(event, visible_ms=tick_ms)
            else:
                self.latest_chainlink = event
                self._chainlink_history.append(event)
                cutoff_ms = (
                    event.received_ms - self.config.history_retention_ms
                )
                while (
                    len(self._chainlink_history) > 1
                    and self._chainlink_history[1].received_ms < cutoff_ms
                ):
                    self._chainlink_history.popleft()

    def finish(self) -> None:
        self._advance_polls_before(self.effective_end_wall_ns)
        final_target_ms = (self.effective_end_wall_ns - 1) // NS_PER_MS
        self._mature(final_target_ms)
        if self._pending:
            raise ReplayDataError("pending forecasts remain at segment boundary")

    def _advance_polls_before(self, exclusive_wall_ns: int) -> None:
        while self.next_poll_ms * NS_PER_MS < exclusive_wall_ns:
            self._poll(self.next_poll_ms)
            self.next_poll_ms += self.config.poll_ms

    def _poll(self, tick_ms: int) -> None:
        self.polls_processed += 1
        self._apply_visible_events(tick_ms)
        observation = self.engine.observe(
            futures=(
                self.latest_futures.observed_price
                if self.latest_futures is not None
                else None
            ),
            chainlink=(
                self.latest_chainlink.observed_price
                if self.latest_chainlink is not None
                else None
            ),
            now_ms=tick_ms,
        )
        self._mature(tick_ms)
        if self._is_generation_tick(tick_ms):
            self._generate(observation)

    def _is_generation_tick(self, tick_ms: int) -> bool:
        if (
            tick_ms % self.config.evaluation_interval_ms
            != self.config.evaluation_phase_offset_ms
        ):
            return False
        if tick_ms < self.config.start_ms:
            return False
        return tick_ms * NS_PER_MS < self.effective_end_wall_ns

    def _generate(self, observation: EngineObservation) -> None:
        expected_versions = tuple(model.version for model in self.config.models)
        observed_versions = tuple(
            signal.model_version for signal in observation.signals
        )
        if observed_versions != expected_versions:
            raise ReplayDataError(
                "engine observation does not contain the configured models"
            )
        common_target_ns = (
            observation.generated_ms + max(self.config.lags_ms)
        ) * NS_PER_MS
        common_target_eligible = (
            common_target_ns < self.config.end_ms * NS_PER_MS
            and common_target_ns < self.segment.end_wall_ns
        )
        common_cohort_member = common_target_eligible and all(
            signal.valid for signal in observation.signals
        )
        volatility_bps = self._current_volatility(observation.generated_ms)
        segment_start_ms = (
            self.segment.start_wall_ns + NS_PER_MS - 1
        ) // NS_PER_MS
        segment_end_ms = self.segment.end_wall_ns // NS_PER_MS
        for signal in observation.signals:
            accumulator = self.accumulators[signal.model_version]
            target_ms = signal.generated_ms + signal.horizon_ms
            target_ns = target_ms * NS_PER_MS
            target_eligible = (
                target_ns < self.config.end_ms * NS_PER_MS
                and target_ns < self.segment.end_wall_ns
            )
            ms_since_segment_start = max(
                0,
                signal.generated_ms - segment_start_ms,
            )
            ms_until_segment_end = max(
                0,
                segment_end_ms - signal.generated_ms,
            )
            accumulator.attempt(
                signal,
                target_eligible=target_eligible,
                common_target_eligible=common_target_eligible,
                common_cohort_member=common_cohort_member,
                coverage_categories={
                    "market_expiry": _expiry_category(
                        full_horizon_before_market_end=(
                            signal.full_horizon_before_market_end
                        ),
                        ms_to_market_end=observation.ms_to_market_end,
                        config=self.config,
                    ),
                    "session_boundary_proximity": (
                        _session_boundary_category(
                            ms_since_segment_start=ms_since_segment_start,
                            ms_until_segment_end=ms_until_segment_end,
                            config=self.config,
                        )
                    ),
                },
            )
            if not signal.valid or not target_eligible:
                continue
            if (
                signal.projection is None
                or signal.chainlink_now is None
                or signal.anchor is None
            ):
                raise ReplayDataError("valid signal is missing projection inputs")
            baseline = no_change_projection(
                chainlink=signal.chainlink_now,
                horizon_ms=signal.horizon_ms,
            )
            forecast = MaturedForecast(
                model_version=signal.model_version,
                generated_ms=signal.generated_ms,
                target_ms=target_ms,
                horizon_ms=signal.horizon_ms,
                chainlink_at_forecast=signal.chainlink_now.value,
                projected_chainlink=signal.projection.projected_chainlink,
                baseline_chainlink=baseline.projected_chainlink,
                predicted_move_bps=signal.projection.pending_move_bps,
                market_id=observation.market.market_id,
                ms_to_market_end=observation.ms_to_market_end,
                full_horizon_before_market_end=(
                    signal.full_horizon_before_market_end
                ),
                volatility_bps=volatility_bps,
                ms_since_segment_start=ms_since_segment_start,
                ms_until_segment_end=ms_until_segment_end,
                common_cohort_member=common_cohort_member,
            )
            self._pending_sequence += 1
            heapq.heappush(
                self._pending,
                (forecast.target_ms, self._pending_sequence, forecast),
            )

    def _mature(self, tick_ms: int) -> None:
        while self._pending and self._pending[0][0] <= tick_ms:
            _target_ms, _sequence, forecast = heapq.heappop(self._pending)
            accumulator = self.accumulators[forecast.model_version]
            actual_event = self._actual_chainlink_at(forecast.target_ms)
            if actual_event is None:
                accumulator.missing_actual += 1
                continue
            outcome = _build_outcome(forecast, actual_event)
            accumulator.score(outcome)

    def _actual_chainlink_at(self, target_ms: int) -> Optional[ReplayEvent]:
        target_ns = target_ms * NS_PER_MS
        for event in reversed(self._chainlink_history):
            if event.received_wall_ns <= target_ns:
                return event
        return None

    def _observe_volatility_event(
        self,
        event: ReplayEvent,
        *,
        visible_ms: int,
    ) -> None:
        previous = self._last_futures_for_volatility
        if previous is not None:
            return_bps = (
                event.value / previous.value - Decimal("1")
            ) * BASIS_POINTS
            squared = return_bps * return_bps
            self._volatility_returns.append((visible_ms, squared))
            with localcontext() as context:
                context.prec = 50
                self._volatility_square_sum += squared
        self._last_futures_for_volatility = event

    def _current_volatility(self, now_ms: int) -> Optional[Decimal]:
        cutoff_ms = now_ms - self.config.volatility_lookback_ms
        while (
            self._volatility_returns
            and self._volatility_returns[0][0] < cutoff_ms
        ):
            _visible_ms, squared = self._volatility_returns.popleft()
            with localcontext() as context:
                context.prec = 50
                self._volatility_square_sum -= squared
        if not self._volatility_returns:
            self._volatility_square_sum = Decimal("0")
            return None
        if self._volatility_square_sum < 0:
            with localcontext() as context:
                context.prec = 50
                self._volatility_square_sum = sum(
                    (
                        squared
                        for _visible_ms, squared in self._volatility_returns
                    ),
                    Decimal("0"),
                )
        if self._volatility_square_sum < 0:
            raise ReplayDataError("volatility square sum cannot be negative")
        with localcontext() as context:
            context.prec = 50
            mean_square = self._volatility_square_sum / Decimal(
                len(self._volatility_returns)
            )
        return _decimal_sqrt(mean_square)


def _build_outcome(
    forecast: MaturedForecast,
    actual_event: ReplayEvent,
) -> ForecastOutcome:
    actual = actual_event.value
    model_error = forecast.projected_chainlink - actual
    baseline_error = forecast.baseline_chainlink - actual
    model_error_bps = (
        model_error / forecast.chainlink_at_forecast * BASIS_POINTS
    )
    baseline_error_bps = (
        baseline_error / forecast.chainlink_at_forecast * BASIS_POINTS
    )
    actual_move_bps = (
        (actual - forecast.chainlink_at_forecast)
        / forecast.chainlink_at_forecast
        * BASIS_POINTS
    )
    return ForecastOutcome(
        forecast=forecast,
        actual_chainlink=actual,
        actual_received_ms=actual_event.received_ms,
        actual_age_ms=max(0, forecast.target_ms - actual_event.received_ms),
        model_error=model_error,
        baseline_error=baseline_error,
        model_error_bps=model_error_bps,
        baseline_error_bps=baseline_error_bps,
        actual_move_bps=actual_move_bps,
        absolute_advantage=abs(baseline_error) - abs(model_error),
    )


class ShadowReplayRunner:
    def __init__(
        self,
        *,
        config: ReplayConfig,
        sessions: Sequence[ReplaySession],
    ) -> None:
        self.config = config
        self.session_selection = select_replay_sessions(sessions, config)
        self.accumulators = {
            model.version: _CandidateAccumulator(model, config)
            for model in config.models
        }
        self.diagnostics = _EventDiagnostics(
            sample_max=config.quantile_sample_max
        )
        self._segments = self.session_selection.segments
        self._segment_index = 0
        self._runtime: Optional[_SegmentRuntime] = None
        self._last_sort_key: Optional[tuple[int, int, int, int, str]] = None
        self.input_events = 0
        self.ignored_events = 0
        self.polls_processed = 0
        self._latest_by_connection: dict[tuple[str, UUID], ReplayEvent] = {}
        self._start_next_segment()

    def consume(self, event: ReplayEvent) -> None:
        if not isinstance(event, ReplayEvent):
            raise TypeError("event must be ReplayEvent")
        if self._last_sort_key is not None and event.sort_key <= self._last_sort_key:
            raise ReplayDataError(
                "replay events are duplicated or not in chronological order"
            )
        self._last_sort_key = event.sort_key
        self.input_events += 1
        self._latest_by_connection[(event.kind, event.connection_id)] = event

        while self._runtime is not None:
            segment = self._runtime.segment
            if event.received_wall_ns >= self._runtime.effective_end_wall_ns:
                self._finish_current_segment()
                self._start_next_segment()
                continue
            break
        if self._runtime is None:
            self.ignored_events += 1
            return

        segment = self._runtime.segment
        if event.received_wall_ns < self._runtime.simulation_start_wall_ns:
            self.ignored_events += 1
            return
        expected_connection_id = (
            segment.futures_session_id
            if event.kind == FUTURES_EVENT
            else segment.chainlink_session_id
        )
        if (
            event.received_wall_ns < segment.start_wall_ns
            or event.connection_id != expected_connection_id
        ):
            self.ignored_events += 1
            return
        self._runtime.consume(event)

    def finish(self) -> ReplayReport:
        while self._runtime is not None:
            self._finish_current_segment()
            self._start_next_segment()
        candidate_summaries = tuple(
            self.accumulators[model.version].summary()
            for model in self.config.models
        )
        common_counts = {
            (
                int(summary["common_cohort"]["target_eligible"]),
                int(summary["common_cohort"]["valid_generated"]),
                int(summary["common_cohort"]["scored"]),
            )
            for summary in candidate_summaries
        }
        if len(common_counts) != 1:
            raise ReplayDataError(
                "common comparison cohort differs across model candidates"
            )
        return ReplayReport(
            config=self.config,
            session_selection=self.session_selection,
            candidate_summaries=candidate_summaries,
            event_diagnostics=self.diagnostics.summary(),
            polls_processed=self.polls_processed,
            input_events=self.input_events,
            ignored_events=self.ignored_events,
        )

    def _start_next_segment(self) -> None:
        if self._segment_index >= len(self._segments):
            self._runtime = None
            return
        segment = self._segments[self._segment_index]
        self._segment_index += 1
        initial_futures = self._latest_before_segment(
            FUTURES_EVENT,
            segment.futures_session_id,
            segment.start_wall_ns,
        )
        initial_chainlink = self._latest_before_segment(
            CHAINLINK_EVENT,
            segment.chainlink_session_id,
            segment.start_wall_ns,
        )
        self._runtime = _SegmentRuntime(
            segment=segment,
            config=self.config,
            accumulators=self.accumulators,
            diagnostics=self.diagnostics,
            initial_futures=initial_futures,
            initial_chainlink=initial_chainlink,
        )

    def _latest_before_segment(
        self,
        kind: str,
        connection_id: UUID,
        segment_start_ns: int,
    ) -> Optional[ReplayEvent]:
        event = self._latest_by_connection.get((kind, connection_id))
        if event is not None and event.received_wall_ns < segment_start_ns:
            return event
        return None

    def _finish_current_segment(self) -> None:
        if self._runtime is None:
            return
        self._runtime.finish()
        self.polls_processed += self._runtime.polls_processed
        self._runtime = None


def replay_shadow_signals(
    *,
    events: Iterable[ReplayEvent],
    sessions: Sequence[ReplaySession],
    config: ReplayConfig,
) -> ReplayReport:
    runner = ShadowReplayRunner(config=config, sessions=sessions)
    for event in events:
        runner.consume(event)
    return runner.finish()


@dataclass(frozen=True)
class _V4Anchor:
    chainlink: V4VisibleObservation
    futures_reference: V4VisibleObservation


class _V4ForecastMachine:
    """Event-complete v4 forecast state for one compatible config family."""

    def __init__(
        self,
        models: Sequence[tuple[ForecastConfig, ModelIdentity]],
    ) -> None:
        self.models = tuple(models)
        if not self.models:
            raise ValueError("v4 forecast machine requires at least one model")
        for config, identity in self.models:
            if not isinstance(config, ForecastConfig):
                raise TypeError("v4 model config must be ForecastConfig")
            if not isinstance(identity, ModelIdentity):
                raise TypeError("v4 model identity must be ModelIdentity")
            if identity.model_version != f"catchup_ratio_l{config.lag_ms}_b100":
                raise ReplayDataError("v4 model identity differs from its config")
            if identity.forecast_config_digest != forecast_config_digest(config):
                raise ReplayDataError(
                    "v4 model identity has the wrong forecast-config digest"
                )
        if len({identity for _config, identity in self.models}) != len(
            self.models
        ):
            raise ReplayDataError("v4 forecast machine identities must be unique")
        policy_digests = {
            identity.offline_evaluation_policy_digest
            for _config, identity in self.models
        }
        if len(policy_digests) != 1:
            raise ReplayDataError(
                "v4 forecast machine identities mix evaluation policies"
            )
        non_lag_settings = {
            (
                item.futures_stale_ms,
                item.chainlink_stale_ms,
                item.reference_max_gap_ms,
                item.history_retention_ms,
                item.max_future_skew_ms,
                item.anchor_rule,
                item.futures_reference_rule,
                item.same_poll_reference_rule,
                item.projection_rule,
                item.forecast_validity_rule,
            )
            for item, _identity in self.models
        }
        if len(non_lag_settings) != 1:
            raise ReplayDataError(
                "one v4 forecast machine cannot mix non-lag settings"
            )
        first = self.models[0][0]
        self.futures_stale_ms = first.futures_stale_ms
        self.chainlink_stale_ms = first.chainlink_stale_ms
        self.reference_max_gap_ms = first.reference_max_gap_ms
        self.history_retention_ms = first.history_retention_ms
        self.max_future_skew_ms = first.max_future_skew_ms
        minimum_retention_ms = (
            max(config.lag_ms for config, _identity in self.models)
            + self.chainlink_stale_ms
            + self.reference_max_gap_ms
        )
        if self.history_retention_ms < minimum_retention_ms:
            raise ReplayDataError(
                "history_retention_ms cannot support this v4 forecast machine"
            )
        self.reset()

    def reset(self) -> None:
        self._futures_history: Deque[V4VisibleObservation] = deque()
        self._last_futures_identity: Optional[tuple[Any, ...]] = None
        self._last_chainlink_identity: Optional[tuple[Any, ...]] = None
        self._last_futures_received_ms: Optional[int] = None
        self._last_chainlink_received_ms: Optional[int] = None
        self._futures_source_timestamp_watermark: Optional[int] = None
        self._chainlink_source_timestamp_watermark: Optional[int] = None
        self._anchors: dict[ModelIdentity, _V4Anchor] = {}
        self._anchor_targets: dict[ModelIdentity, int] = {}
        self._anchor_gaps: dict[ModelIdentity, int] = {}
        self._anchor_failures: dict[ModelIdentity, str] = {}
        self._poll_regression_sources: tuple[str, ...] = ()

    def apply_poll(
        self,
        *,
        futures_events: Sequence[V4VisibleObservation],
        chainlink_events: Sequence[V4VisibleObservation],
        now_ms: int,
    ) -> None:
        _require_non_negative_int(now_ms, "now_ms")
        if any(item.kind != FUTURES_EVENT for item in futures_events):
            raise ReplayDataError("futures poll batch contains another source")
        if any(item.kind != CHAINLINK_EVENT for item in chainlink_events):
            raise ReplayDataError("Chainlink poll batch contains another source")
        for item in (*futures_events, *chainlink_events):
            if item.visible_ms > now_ms:
                raise ReplayDataError("v4 poll applied an event before visibility")

        history_existed_before_poll = bool(self._futures_history)
        regression_sources: list[str] = []
        futures_regressed = False
        for observation in futures_events:
            if observation.identity == self._last_futures_identity:
                continue
            if self._timestamps_regress(
                observation,
                last_received_ms=self._last_futures_received_ms,
                source_timestamp_watermark=(
                    self._futures_source_timestamp_watermark
                ),
            ):
                self._reset_for_futures_regression(observation)
                futures_regressed = True
                if FUTURES_EVENT not in regression_sources:
                    regression_sources.append(FUTURES_EVENT)
                continue
            self._ingest_futures(observation, now_ms=now_ms)
        self._prune_futures(now_ms)

        chainlink_regressed = False
        for observation in chainlink_events:
            if observation.identity == self._last_chainlink_identity:
                continue
            if self._timestamps_regress(
                observation,
                last_received_ms=self._last_chainlink_received_ms,
                source_timestamp_watermark=(
                    self._chainlink_source_timestamp_watermark
                ),
            ):
                self._reset_for_chainlink_regression(observation)
                chainlink_regressed = True
                if CHAINLINK_EVENT not in regression_sources:
                    regression_sources.append(CHAINLINK_EVENT)
                continue
            if futures_regressed or chainlink_regressed:
                self._quarantine_chainlink(observation)
                continue
            self._ingest_chainlink(
                observation,
                now_ms=now_ms,
                history_existed_before_poll=history_existed_before_poll,
            )
        self._poll_regression_sources = tuple(regression_sources)

    @staticmethod
    def _timestamps_regress(
        observation: V4VisibleObservation,
        *,
        last_received_ms: Optional[int],
        source_timestamp_watermark: Optional[int],
    ) -> bool:
        if (
            last_received_ms is not None
            and observation.received_ms < last_received_ms
        ):
            return True
        return (
            source_timestamp_watermark is not None
            and observation.source_timestamp_ms < source_timestamp_watermark
        )

    def _ingest_futures(
        self,
        observation: V4VisibleObservation,
        *,
        now_ms: int,
    ) -> None:
        self._last_futures_identity = observation.identity
        self._last_futures_received_ms = observation.received_ms
        self._futures_source_timestamp_watermark = max(
            observation.source_timestamp_ms,
            self._futures_source_timestamp_watermark
            if self._futures_source_timestamp_watermark is not None
            else observation.source_timestamp_ms,
        )
        if self._received_age_ms(observation, now_ms) <= self.futures_stale_ms:
            self._futures_history.append(observation)

    def _ingest_chainlink(
        self,
        observation: V4VisibleObservation,
        *,
        now_ms: int,
        history_existed_before_poll: bool,
    ) -> None:
        self._last_chainlink_identity = observation.identity
        self._last_chainlink_received_ms = observation.received_ms
        self._chainlink_source_timestamp_watermark = max(
            observation.source_timestamp_ms,
            self._chainlink_source_timestamp_watermark
            if self._chainlink_source_timestamp_watermark is not None
            else observation.source_timestamp_ms,
        )
        is_fresh = (
            self._received_age_ms(observation, now_ms)
            <= self.chainlink_stale_ms
        )
        for config, identity in self.models:
            self._clear_anchor(identity)
            if not is_fresh:
                continue
            target_ms = observation.received_ms - config.lag_ms
            self._anchor_targets[identity] = target_ms
            if not history_existed_before_poll:
                self._anchor_failures[identity] = ANCHOR_HISTORY_MISSING
                continue
            reference, gap_ms = self._find_reference(target_ms)
            if reference is None:
                self._anchor_failures[identity] = ANCHOR_HISTORY_MISSING
                continue
            self._anchor_gaps[identity] = gap_ms
            if gap_ms > config.reference_max_gap_ms:
                self._anchor_failures[identity] = ANCHOR_REFERENCE_GAP
                continue
            self._anchors[identity] = _V4Anchor(
                chainlink=observation,
                futures_reference=reference,
            )
            self._anchor_failures.pop(identity, None)

    def _quarantine_chainlink(self, observation: V4VisibleObservation) -> None:
        self._last_chainlink_identity = observation.identity
        self._last_chainlink_received_ms = observation.received_ms
        self._chainlink_source_timestamp_watermark = max(
            observation.source_timestamp_ms,
            self._chainlink_source_timestamp_watermark
            if self._chainlink_source_timestamp_watermark is not None
            else observation.source_timestamp_ms,
        )

    def _reset_for_futures_regression(
        self,
        observation: V4VisibleObservation,
    ) -> None:
        self._futures_history.clear()
        self._clear_all_anchors()
        self._last_futures_identity = observation.identity
        self._last_futures_received_ms = observation.received_ms
        self._futures_source_timestamp_watermark = (
            observation.source_timestamp_ms
        )

    def _reset_for_chainlink_regression(
        self,
        observation: V4VisibleObservation,
    ) -> None:
        self._clear_all_anchors()
        self._last_chainlink_identity = observation.identity
        self._last_chainlink_received_ms = observation.received_ms
        self._chainlink_source_timestamp_watermark = (
            observation.source_timestamp_ms
        )

    def _find_reference(
        self,
        target_ms: int,
    ) -> tuple[Optional[V4VisibleObservation], int]:
        for observation in reversed(self._futures_history):
            if observation.received_ms <= target_ms:
                return observation, target_ms - observation.received_ms
        return None, 0

    def _history_is_ready(self, config: ForecastConfig, now_ms: int) -> bool:
        reference, gap_ms = self._find_reference(now_ms - config.lag_ms)
        return reference is not None and gap_ms <= config.reference_max_gap_ms

    def _prune_futures(self, now_ms: int) -> None:
        cutoff_ms = now_ms - self.history_retention_ms
        while (
            self._futures_history
            and self._futures_history[0].received_ms < cutoff_ms
        ):
            self._futures_history.popleft()

    def _clear_anchor(self, identity: ModelIdentity) -> None:
        self._anchors.pop(identity, None)
        self._anchor_targets.pop(identity, None)
        self._anchor_gaps.pop(identity, None)
        self._anchor_failures.pop(identity, None)

    def _clear_all_anchors(self) -> None:
        self._anchors.clear()
        self._anchor_targets.clear()
        self._anchor_gaps.clear()
        self._anchor_failures.clear()

    @staticmethod
    def _received_age_ms(
        observation: V4VisibleObservation,
        now_ms: int,
    ) -> int:
        return max(0, now_ms - observation.received_ms)

    def forecast_attempts(
        self,
        *,
        latest_futures: Optional[V4VisibleObservation],
        latest_chainlink: Optional[V4VisibleObservation],
        generated_ms: int,
    ) -> tuple[V4ForecastAttempt, ...]:
        return tuple(
            self._forecast_attempt(
                config=config,
                identity=identity,
                latest_futures=latest_futures,
                latest_chainlink=latest_chainlink,
                generated_ms=generated_ms,
            )
            for config, identity in self.models
        )

    def _forecast_attempt(
        self,
        *,
        config: ForecastConfig,
        identity: ModelIdentity,
        latest_futures: Optional[V4VisibleObservation],
        latest_chainlink: Optional[V4VisibleObservation],
        generated_ms: int,
    ) -> V4ForecastAttempt:
        anchor = self._anchors.get(identity)
        state = (
            ANCHORED
            if anchor is not None
            else (
                WAITING_FOR_NEW_CHAINLINK_ANCHOR
                if self._history_is_ready(config, generated_ms)
                else WARMING_UP_FUTURES_HISTORY
            )
        )
        regression_sources = list(self._poll_regression_sources)
        for kind, observation in (
            (FUTURES_EVENT, latest_futures),
            (CHAINLINK_EVENT, latest_chainlink),
        ):
            if (
                observation is not None
                and observation.received_ms - generated_ms
                > config.max_future_skew_ms
                and kind not in regression_sources
            ):
                regression_sources.append(kind)
        if regression_sources:
            return self._invalid_attempt(
                config=config,
                identity=identity,
                generated_ms=generated_ms,
                status=TIMESTAMP_REGRESSION,
                reasons=(TIMESTAMP_REGRESSION,),
                anchor=anchor,
                latest_futures=latest_futures,
            )

        reasons: list[str] = []
        if latest_chainlink is None:
            reasons.append(CHAINLINK_UNAVAILABLE)
        elif (
            self._received_age_ms(latest_chainlink, generated_ms)
            > config.chainlink_stale_ms
        ):
            reasons.append(CHAINLINK_STALE)
        if latest_futures is None:
            reasons.append(FUTURES_UNAVAILABLE)
        elif (
            self._received_age_ms(latest_futures, generated_ms)
            > config.futures_stale_ms
        ):
            reasons.append(FUTURES_STALE)
        if anchor is None:
            anchor_reason = self._anchor_failures.get(identity, state)
            if anchor_reason not in reasons:
                reasons.append(anchor_reason)
        if reasons:
            return self._invalid_attempt(
                config=config,
                identity=identity,
                generated_ms=generated_ms,
                status=self._status_for_reasons(reasons),
                reasons=tuple(reasons),
                anchor=anchor,
                latest_futures=latest_futures,
            )
        if latest_futures is None or anchor is None:
            raise ReplayDataError("valid v4 state is missing forecast inputs")
        try:
            projection = project_from_anchor(
                model=CatchupModel(
                    version=identity.model_version,
                    lag_ms=config.lag_ms,
                    beta=config.beta,
                ),
                anchor=ModelAnchor(
                    chainlink=anchor.chainlink.observed_price,
                    futures_reference=anchor.futures_reference.observed_price,
                ),
                futures_now=latest_futures.observed_price,
            )
        except (ArithmeticError, ValueError):
            return self._invalid_attempt(
                config=config,
                identity=identity,
                generated_ms=generated_ms,
                status=MODEL_ERROR,
                reasons=(MODEL_ERROR,),
                anchor=anchor,
                latest_futures=latest_futures,
            )
        return V4ForecastAttempt(
            identity=identity,
            lag_ms=config.lag_ms,
            horizon_ms=config.horizon_ms,
            beta=config.beta,
            generated_ms=generated_ms,
            target_ms=generated_ms + config.horizon_ms,
            valid=True,
            status=VALID,
            invalid_reasons=(),
            chainlink_anchor=anchor.chainlink,
            futures_now=latest_futures,
            futures_reference=anchor.futures_reference,
            futures_reference_target_ms=self._anchor_targets[identity],
            futures_reference_gap_ms=self._anchor_gaps[identity],
            projected_chainlink=projection.projected_chainlink,
            matched_no_change_prediction=anchor.chainlink.value,
        )

    def _invalid_attempt(
        self,
        *,
        config: ForecastConfig,
        identity: ModelIdentity,
        generated_ms: int,
        status: str,
        reasons: tuple[str, ...],
        anchor: Optional[_V4Anchor],
        latest_futures: Optional[V4VisibleObservation],
    ) -> V4ForecastAttempt:
        return V4ForecastAttempt(
            identity=identity,
            lag_ms=config.lag_ms,
            horizon_ms=config.horizon_ms,
            beta=config.beta,
            generated_ms=generated_ms,
            target_ms=generated_ms + config.horizon_ms,
            valid=False,
            status=status,
            invalid_reasons=reasons,
            chainlink_anchor=None if anchor is None else anchor.chainlink,
            futures_now=latest_futures,
            futures_reference=(
                None if anchor is None else anchor.futures_reference
            ),
            futures_reference_target_ms=self._anchor_targets.get(identity),
            futures_reference_gap_ms=self._anchor_gaps.get(identity),
            projected_chainlink=None,
            matched_no_change_prediction=None,
        )

    @staticmethod
    def _status_for_reasons(reasons: Sequence[str]) -> str:
        for status in (
            MODEL_ERROR,
            CHAINLINK_UNAVAILABLE,
            FUTURES_UNAVAILABLE,
            CHAINLINK_STALE,
            FUTURES_STALE,
            ANCHOR_HISTORY_MISSING,
            ANCHOR_REFERENCE_GAP,
        ):
            if status in reasons:
                return status
        return WARMING_UP


def _select_v4_replay_sessions(
    sessions: Sequence[ReplaySession],
    config: V4CausalReplayConfig,
) -> SessionSelection:
    total_by_source: Counter[str] = Counter()
    eligible_by_source: Counter[str] = Counter()
    excluded_by_reason: Counter[str] = Counter()
    excluded_raw_rows = 0
    eligible: dict[str, list[ReplaySession]] = {
        FUTURES_SESSION_SOURCE: [],
        CHAINLINK_SESSION_SOURCE: [],
    }

    class _StrictSessionPolicy:
        exclude_parse_error_sessions = True

    input_start_ns = config.archive_input_start_ms * NS_PER_MS
    input_end_ns = config.archive_input_end_ms * NS_PER_MS
    for session in sessions:
        if not isinstance(session, ReplaySession):
            raise TypeError("sessions must contain ReplaySession values")
        total_by_source[session.source] += 1
        potential_start_ns = (
            session.ready_wall_ns
            if session.ready_wall_ns is not None
            else session.connected_wall_ns
        )
        potential_end_ns = (
            session.disconnected_wall_ns
            if session.disconnected_wall_ns is not None
            else input_end_ns
        )
        if (
            potential_start_ns >= input_end_ns
            or potential_end_ns <= input_start_ns
        ):
            continue
        reasons = session.exclusion_reasons(
            _StrictSessionPolicy()  # type: ignore[arg-type]
        )
        if reasons:
            excluded_by_reason.update(reasons)
            excluded_raw_rows += session.raw_row_count or 0
            raise ReplayDataError(
                "v4 overlapping session failed loss-free archive quality: "
                + ",".join(sorted(reasons))
            )
        eligible[session.source].append(session)
        eligible_by_source[session.source] += 1

    for source_sessions in eligible.values():
        source_sessions.sort(key=lambda item: item.ready_wall_ns or 0)
        for previous, current in zip(source_sessions, source_sessions[1:]):
            if previous.disconnected_wall_ns > current.ready_wall_ns:
                raise ReplayDataError("eligible sessions overlap for one source")

    segments: list[ReplaySegment] = []
    futures_sessions = eligible[FUTURES_SESSION_SOURCE]
    chainlink_sessions = eligible[CHAINLINK_SESSION_SOURCE]
    futures_index = 0
    chainlink_index = 0
    while (
        futures_index < len(futures_sessions)
        and chainlink_index < len(chainlink_sessions)
    ):
        futures_session = futures_sessions[futures_index]
        chainlink_session = chainlink_sessions[chainlink_index]
        start_wall_ns = max(
            futures_session.ready_wall_ns,
            chainlink_session.ready_wall_ns,
            input_start_ns,
        )
        end_wall_ns = min(
            futures_session.disconnected_wall_ns,
            chainlink_session.disconnected_wall_ns,
            input_end_ns,
        )
        if start_wall_ns < end_wall_ns:
            segments.append(
                ReplaySegment(
                    start_wall_ns=start_wall_ns,
                    end_wall_ns=end_wall_ns,
                    futures_session_id=futures_session.connection_id,
                    chainlink_session_id=chainlink_session.connection_id,
                )
            )
        if (
            futures_session.disconnected_wall_ns
            <= chainlink_session.disconnected_wall_ns
        ):
            futures_index += 1
        else:
            chainlink_index += 1

    eligible_session_ids = frozenset(
        connection_id
        for segment in segments
        for connection_id in (
            segment.futures_session_id,
            segment.chainlink_session_id,
        )
    )
    return SessionSelection(
        segments=tuple(segments),
        eligible_session_ids=eligible_session_ids,
        total_by_source=dict(total_by_source),
        eligible_by_source=dict(eligible_by_source),
        excluded_by_reason=dict(excluded_by_reason),
        excluded_integrity_scope_raw_rows=excluded_raw_rows,
    )


@dataclass(frozen=True)
class _PendingV4Origin:
    generated_ms: int
    finalization_ms: int
    target_eligible: bool
    candidate_attempts: tuple[V4ForecastAttempt, ...]
    control_attempt: V4ForecastAttempt
    generation_eligible: bool
    integrity_epoch: int
    session_available_at_generation: bool


class V4CausalReplayRunner:
    """Streaming, loss-free causal replay for exactly one frozen v4 cell."""

    def __init__(
        self,
        *,
        config: V4CausalReplayConfig,
        sessions: Sequence[ReplaySession],
    ) -> None:
        if not isinstance(config, V4CausalReplayConfig):
            raise TypeError("config must be V4CausalReplayConfig")
        self.config = config
        self.session_selection = _select_v4_replay_sessions(sessions, config)
        self._segments = self.session_selection.segments
        candidate_models = tuple(
            (
                forecast_config,
                config.contract.candidate_identity(forecast_config.lag_ms),
            )
            for forecast_config in config.candidate_configs
        )
        self._candidate_machine = _V4ForecastMachine(candidate_models)
        self._control_alias = (
            config.contract.replacement_control.mode
            is ControlMode.V4_3000_ALIAS
        )
        v4_3000_config = config.candidate_configs[
            COMPARISON_LAGS_MS.index(3_000)
        ]
        supported_control_rules = (
            "anchor_rule",
            "futures_reference_rule",
            "same_poll_reference_rule",
            "projection_rule",
            "forecast_validity_rule",
        )
        if (
            not self._control_alias
            and (
                config.contract.replacement_control.active_code_digest
                != config.contract.replacement_control.v4_code_digest
                or any(
                    getattr(config.control_config, field_name)
                    != getattr(v4_3000_config, field_name)
                    for field_name in supported_control_rules
                )
            )
        ):
            raise ReplayDataError(
                "the distinct operational control has unsupported forecast "
                "code or rules; "
                "a manifest-verified reconstructed executor is required"
            )
        self._control_machine = (
            None
            if self._control_alias
            else _V4ForecastMachine(
                ((config.control_config, config.contract.control_identity),)
            )
        )
        self._active_segment: Optional[ReplaySegment] = None
        self._poll_segment_index = 0
        self._raw_segment_index = 0
        self._integrity_epoch = 0
        self._visibility_pending: list[
            tuple[int, tuple[int, int, int, int, str], ReplayEvent]
        ] = []
        self._latest_futures: Optional[V4VisibleObservation] = None
        self._latest_chainlink: Optional[V4VisibleObservation] = None
        self._chainlink_actual_history: Deque[V4VisibleObservation] = deque()
        self._pending_origins: list[tuple[int, int, _PendingV4Origin]] = []
        self._pending_sequence = 0
        self._last_sort_key: Optional[tuple[int, int, int, int, str]] = None
        self.next_poll_ms = _ceil_to_multiple(
            config.archive_input_start_ms,
            V4_POLL_MS,
        )
        self.input_events = 0
        self.ignored_events = 0
        self.polls_processed = 0
        self.origins_finalized = 0

    def consume(self, event: ReplayEvent) -> Iterator[V4OriginCohort]:
        if not isinstance(event, ReplayEvent):
            raise TypeError("event must be ReplayEvent")
        if self._last_sort_key is not None and event.sort_key <= self._last_sort_key:
            raise ReplayDataError(
                "v4 replay events are duplicated or not in raw chronological order"
            )
        self._last_sort_key = event.sort_key
        self.input_events += 1
        yield from self._iter_polls_before(event.received_wall_ns)

        input_start_ns = self.config.archive_input_start_ms * NS_PER_MS
        input_end_ns = self.config.archive_input_end_ms * NS_PER_MS
        if not input_start_ns <= event.received_wall_ns < input_end_ns:
            self.ignored_events += 1
            return
        segment = self._segment_for_raw_event(event.received_wall_ns)
        if segment is None or event.connection_id != self._connection_for_kind(
            segment,
            event.kind,
        ):
            self.ignored_events += 1
            return
        delay_ms = (
            self.config.timing_cell.futures_delay_ms
            if event.kind == FUTURES_EVENT
            else self.config.timing_cell.chainlink_delay_ms
        )
        available_wall_ns = event.received_wall_ns + delay_ms * NS_PER_MS
        heapq.heappush(
            self._visibility_pending,
            (available_wall_ns, event.sort_key, event),
        )

    def finish(self) -> Iterator[V4OriginCohort]:
        yield from self._iter_polls_before(
            self.config.archive_input_end_ms * NS_PER_MS,
        )
        if self._pending_origins:
            raise ReplayDataError(
                "v4 pending cohorts remain after the archive finalization tail"
            )

    def _iter_polls_before(
        self,
        exclusive_wall_ns: int,
    ) -> Iterator[V4OriginCohort]:
        input_end_ns = self.config.archive_input_end_ms * NS_PER_MS
        while (
            self.next_poll_ms * NS_PER_MS < exclusive_wall_ns
            and self.next_poll_ms * NS_PER_MS < input_end_ns
        ):
            finalized = self._poll(self.next_poll_ms)
            self.next_poll_ms += V4_POLL_MS
            yield from finalized

    def _poll(self, tick_ms: int) -> tuple[V4OriginCohort, ...]:
        self.polls_processed += 1
        self._transition_segment(tick_ms * NS_PER_MS)
        futures_events, chainlink_events = self._apply_visible_events(tick_ms)
        self._candidate_machine.apply_poll(
            futures_events=futures_events,
            chainlink_events=chainlink_events,
            now_ms=tick_ms,
        )
        if self._control_machine is not None:
            self._control_machine.apply_poll(
                futures_events=futures_events,
                chainlink_events=chainlink_events,
                now_ms=tick_ms,
            )
        finalized = self._finalize_due(tick_ms)
        if self._is_generation_tick(tick_ms):
            self._generate(tick_ms)
        self._prune_actual_history(tick_ms)
        return finalized

    def _transition_segment(self, tick_wall_ns: int) -> None:
        while (
            self._poll_segment_index < len(self._segments)
            and self._segments[self._poll_segment_index].end_wall_ns
            <= tick_wall_ns
        ):
            self._poll_segment_index += 1
        next_segment = None
        if self._poll_segment_index < len(self._segments):
            candidate = self._segments[self._poll_segment_index]
            if candidate.start_wall_ns <= tick_wall_ns < candidate.end_wall_ns:
                next_segment = candidate
        if next_segment == self._active_segment:
            return
        self._active_segment = next_segment
        self._integrity_epoch += 1
        self._candidate_machine.reset()
        if self._control_machine is not None:
            self._control_machine.reset()
        self._latest_futures = None
        self._latest_chainlink = None
        self._chainlink_actual_history.clear()
        if next_segment is None:
            self.ignored_events += len(self._visibility_pending)
            self._visibility_pending.clear()
            return
        retained_pending = [
            item
            for item in self._visibility_pending
            if (
                next_segment.start_wall_ns
                <= item[2].received_wall_ns
                < next_segment.end_wall_ns
                and item[2].connection_id
                == self._connection_for_kind(next_segment, item[2].kind)
            )
        ]
        self.ignored_events += len(self._visibility_pending) - len(retained_pending)
        self._visibility_pending = retained_pending
        heapq.heapify(self._visibility_pending)

    def _apply_visible_events(
        self,
        tick_ms: int,
    ) -> tuple[tuple[V4VisibleObservation, ...], tuple[V4VisibleObservation, ...]]:
        tick_wall_ns = tick_ms * NS_PER_MS
        futures_events: list[V4VisibleObservation] = []
        chainlink_events: list[V4VisibleObservation] = []
        while (
            self._visibility_pending
            and self._visibility_pending[0][0] <= tick_wall_ns
        ):
            available_wall_ns, _sort_key, event = heapq.heappop(
                self._visibility_pending
            )
            if self._active_segment is None or (
                event.connection_id
                != self._connection_for_kind(self._active_segment, event.kind)
            ):
                self.ignored_events += 1
                continue
            visible_tick_index = (
                available_wall_ns + V4_POLL_MS * NS_PER_MS - 1
            ) // (V4_POLL_MS * NS_PER_MS)
            observation = V4VisibleObservation(
                kind=event.kind,
                value=event.value,
                received_wall_ns=event.received_wall_ns,
                received_monotonic_ns=event.received_monotonic_ns,
                available_wall_ns=available_wall_ns,
                visible_ms=visible_tick_index * V4_POLL_MS,
                source_timestamp_ms=event.source_timestamp_ms,
                connection_id=event.connection_id,
                source_sequence=event.sequence,
            )
            if observation.visible_ms != tick_ms:
                raise ReplayDataError("v4 visibility queue escaped its poll tick")
            if event.kind == FUTURES_EVENT:
                self._latest_futures = observation
                futures_events.append(observation)
            else:
                self._latest_chainlink = observation
                self._chainlink_actual_history.append(observation)
                chainlink_events.append(observation)
        return tuple(futures_events), tuple(chainlink_events)

    def _is_generation_tick(self, tick_ms: int) -> bool:
        if not (
            self.config.scoring_start_ms
            <= tick_ms
            < self.config.scoring_end_ms
        ):
            return False
        return (
            tick_ms % V4_GENERATION_INTERVAL_MS
            == self.config.timing_cell.phase_offset_ms
        )

    def _generate(self, generated_ms: int) -> None:
        candidate_attempts = self._candidate_machine.forecast_attempts(
            latest_futures=self._latest_futures,
            latest_chainlink=self._latest_chainlink,
            generated_ms=generated_ms,
        )
        expected_identities = tuple(
            self.config.contract.candidate_identity(lag_ms)
            for lag_ms in COMPARISON_LAGS_MS
        )
        if tuple(item.identity for item in candidate_attempts) != expected_identities:
            raise ReplayDataError("v4 candidate attempts have the wrong identities")
        if self._control_alias:
            candidate_3000 = candidate_attempts[
                COMPARISON_LAGS_MS.index(self.config.control_config.lag_ms)
            ]
            control_attempt = replace(
                candidate_3000,
                identity=self.config.contract.control_identity,
            )
        else:
            if self._control_machine is None:
                raise ReplayDataError("distinct v4 control machine is missing")
            (control_attempt,) = self._control_machine.forecast_attempts(
                latest_futures=self._latest_futures,
                latest_chainlink=self._latest_chainlink,
                generated_ms=generated_ms,
            )
        if control_attempt.identity != self.config.contract.control_identity:
            raise ReplayDataError("v4 control attempt has the wrong identity")

        target_eligible = (
            generated_ms + max(COMPARISON_LAGS_MS)
            < self.config.scoring_end_ms
        )
        generation_eligible = target_eligible and all(
            item.valid for item in candidate_attempts
        )
        finalization_ms = (
            generated_ms
            + max(COMPARISON_LAGS_MS)
            + FINALIZATION_ALLOWANCE_MS
        )
        pending = _PendingV4Origin(
            generated_ms=generated_ms,
            finalization_ms=finalization_ms,
            target_eligible=target_eligible,
            candidate_attempts=candidate_attempts,
            control_attempt=control_attempt,
            generation_eligible=generation_eligible,
            integrity_epoch=self._integrity_epoch,
            session_available_at_generation=self._active_segment is not None,
        )
        self._pending_sequence += 1
        heapq.heappush(
            self._pending_origins,
            (finalization_ms, self._pending_sequence, pending),
        )

    def _finalize_due(self, tick_ms: int) -> tuple[V4OriginCohort, ...]:
        finalized: list[V4OriginCohort] = []
        while self._pending_origins and self._pending_origins[0][0] <= tick_ms:
            _finalization_ms, _sequence, pending = heapq.heappop(
                self._pending_origins
            )
            finalized.append(self._finalize_origin(pending))
            self.origins_finalized += 1
        return tuple(finalized)

    def _finalize_origin(self, pending: _PendingV4Origin) -> V4OriginCohort:
        integrity_reset = pending.integrity_epoch != self._integrity_epoch
        if pending.target_eligible and not integrity_reset:
            candidate_attempts = tuple(
                item.with_actual(self._actual_chainlink_at(item.target_ms))
                for item in pending.candidate_attempts
            )
            control_attempt = pending.control_attempt.with_actual(
                self._actual_chainlink_at(pending.control_attempt.target_ms)
            )
        else:
            candidate_attempts = pending.candidate_attempts
            control_attempt = pending.control_attempt

        common_scored = (
            pending.generation_eligible
            and not integrity_reset
            and all(item.actual_chainlink is not None for item in candidate_attempts)
        )
        decision_eligible = (
            common_scored
            and control_attempt.valid
            and control_attempt.actual_chainlink is not None
        )
        missing_reasons: list[str] = []
        structural_reason = None
        if not pending.target_eligible:
            structural_reason = "maximum_horizon_tail"
        else:
            if not pending.session_available_at_generation:
                missing_reasons.append("session_unavailable_at_generation")
            for item in candidate_attempts:
                if not item.valid:
                    missing_reasons.append(
                        "candidate_forecast_invalid:"
                        f"{item.identity.model_version}:{item.status}"
                    )
            if integrity_reset:
                missing_reasons.append("integrity_reset_before_finalization")
            else:
                for item in candidate_attempts:
                    if item.valid and item.actual_chainlink is None:
                        missing_reasons.append(
                            "candidate_actual_missing:"
                            f"{item.identity.model_version}"
                        )
            if not control_attempt.valid:
                missing_reasons.append(
                    f"control_forecast_invalid:{control_attempt.status}"
                )
            elif not integrity_reset and control_attempt.actual_chainlink is None:
                missing_reasons.append("control_actual_missing")

        return V4OriginCohort(
            cell_id=self.config.timing_cell.cell_id,
            generated_ms=pending.generated_ms,
            finalization_ms=pending.finalization_ms,
            target_eligible=pending.target_eligible,
            generation_eligible=pending.generation_eligible,
            common_scored=common_scored,
            decision_eligible=decision_eligible,
            candidate_attempts=candidate_attempts,
            control_attempt=control_attempt,
            integrity_epoch_at_generation=pending.integrity_epoch,
            integrity_epoch_at_finalization=self._integrity_epoch,
            integrity_reset_before_finalization=integrity_reset,
            structural_exclusion_reason=structural_reason,
            missing_reasons=tuple(sorted(set(missing_reasons))),
        )

    def _actual_chainlink_at(
        self,
        target_ms: int,
    ) -> Optional[V4VisibleObservation]:
        target_wall_ns = target_ms * NS_PER_MS
        for observation in reversed(self._chainlink_actual_history):
            if (
                observation.visible_ms <= target_ms
                and observation.received_wall_ns <= target_wall_ns
            ):
                return observation
        return None

    def _prune_actual_history(self, now_ms: int) -> None:
        cutoff_visible_ms = (
            now_ms
            - max(COMPARISON_LAGS_MS)
            - FINALIZATION_ALLOWANCE_MS
            - V4_POLL_MS
        )
        while (
            len(self._chainlink_actual_history) > 1
            and self._chainlink_actual_history[1].visible_ms < cutoff_visible_ms
        ):
            self._chainlink_actual_history.popleft()

    def _segment_for_raw_event(
        self,
        received_wall_ns: int,
    ) -> Optional[ReplaySegment]:
        while (
            self._raw_segment_index < len(self._segments)
            and self._segments[self._raw_segment_index].end_wall_ns
            <= received_wall_ns
        ):
            self._raw_segment_index += 1
        if self._raw_segment_index >= len(self._segments):
            return None
        segment = self._segments[self._raw_segment_index]
        if segment.start_wall_ns <= received_wall_ns < segment.end_wall_ns:
            return segment
        return None

    @staticmethod
    def _connection_for_kind(segment: ReplaySegment, kind: str) -> UUID:
        if kind == FUTURES_EVENT:
            return segment.futures_session_id
        if kind == CHAINLINK_EVENT:
            return segment.chainlink_session_id
        raise ReplayDataError("invalid v4 event kind")


@dataclass(frozen=True)
class V4CausalReplayResult:
    config: V4CausalReplayConfig
    session_selection: SessionSelection
    origins: tuple[V4OriginCohort, ...]
    polls_processed: int
    input_events: int
    ignored_events: int

    def __post_init__(self) -> None:
        if not isinstance(self.config, V4CausalReplayConfig):
            raise TypeError("config must be V4CausalReplayConfig")
        if not isinstance(self.session_selection, SessionSelection):
            raise TypeError("session_selection must be SessionSelection")
        if not isinstance(self.origins, tuple) or not all(
            isinstance(item, V4OriginCohort) for item in self.origins
        ):
            raise TypeError("origins must be a tuple of V4OriginCohort values")
        generated = tuple(item.generated_ms for item in self.origins)
        if generated != tuple(sorted(generated)) or len(set(generated)) != len(
            generated
        ):
            raise ValueError("v4 origins must be unique and generation ordered")
        for field_name in ("polls_processed", "input_events", "ignored_events"):
            _require_non_negative_int(getattr(self, field_name), field_name)

    @property
    def scheduled_origin_vector(self) -> tuple[int, ...]:
        return tuple(item.generated_ms for item in self.origins)

    @property
    def target_eligible_mask(self) -> tuple[bool, ...]:
        return tuple(item.target_eligible for item in self.origins)

    @property
    def target_eligible_origin_vector(self) -> tuple[int, ...]:
        return tuple(
            item.generated_ms for item in self.origins if item.target_eligible
        )

    @property
    def generation_eligible_mask(self) -> tuple[bool, ...]:
        return tuple(
            item.generation_eligible
            for item in self.origins
            if item.target_eligible
        )

    @property
    def common_scored_mask(self) -> tuple[bool, ...]:
        return tuple(
            item.common_scored for item in self.origins if item.target_eligible
        )

    @property
    def decision_eligible_mask(self) -> tuple[bool, ...]:
        return tuple(
            item.decision_eligible for item in self.origins if item.target_eligible
        )

    def to_dict(self) -> dict[str, Any]:
        target_origins = [item for item in self.origins if item.target_eligible]
        return {
            "schema_version": V4_REPLAY_SCHEMA_VERSION,
            "mode": V4_CAUSAL_REPLAY_MODE,
            "selection_performed": False,
            "losses_materialized": False,
            "cell": self.config.timing_cell.to_dict(),
            "contract_digest": self.config.contract.digest,
            "offline_evaluation_policy_digest": (
                self.config.contract.offline_evaluation_policy_digest
            ),
            "scoring_range": {
                "start_ms": self.config.scoring_start_ms,
                "end_ms": self.config.scoring_end_ms,
                "boundary": "[start_ms,end_ms)",
            },
            "archive_input_range": {
                "start_ms": self.config.archive_input_start_ms,
                "end_ms": self.config.archive_input_end_ms,
                "boundary": "[start_ms,end_ms)",
            },
            "scheduled_origin_vector": list(self.scheduled_origin_vector),
            "target_eligible_mask": list(self.target_eligible_mask),
            "target_eligible_origin_vector": list(
                self.target_eligible_origin_vector
            ),
            "generation_eligible_mask": list(self.generation_eligible_mask),
            "common_scored_mask": list(self.common_scored_mask),
            "decision_eligible_mask": list(self.decision_eligible_mask),
            "counts": {
                "scheduled": len(self.origins),
                "target_eligible": len(target_origins),
                "generation_eligible": sum(
                    item.generation_eligible for item in target_origins
                ),
                "common_scored": sum(item.common_scored for item in target_origins),
                "decision_eligible": sum(
                    item.decision_eligible for item in target_origins
                ),
                "cohort_classified": len(target_origins),
                "causal_violations": sum(
                    item.causal_violation_count for item in target_origins
                ),
            },
            "missing_reasons_by_origin": {
                str(item.generated_ms): list(item.missing_reasons)
                for item in target_origins
                if item.missing_reasons
            },
            "session_quality": {
                "policy": "completed_clean_integrity_checked_parse_strict",
                "common_healthy_segments": len(
                    self.session_selection.segments
                ),
                "sessions_total_by_source": dict(
                    self.session_selection.total_by_source
                ),
                "sessions_eligible_by_source": dict(
                    self.session_selection.eligible_by_source
                ),
                "sessions_excluded_by_reason": dict(
                    self.session_selection.excluded_by_reason
                ),
            },
            "polls_processed": self.polls_processed,
            "input_events": self.input_events,
            "ignored_events": self.ignored_events,
            "origins": [item.to_dict() for item in self.origins],
        }


def iter_v4_causal_origins(
    *,
    events: Iterable[ReplayEvent],
    sessions: Sequence[ReplaySession],
    config: V4CausalReplayConfig,
) -> Iterable[V4OriginCohort]:
    """Yield finalized v4 origins without retaining a full-day rich ledger."""

    runner = V4CausalReplayRunner(config=config, sessions=sessions)
    for event in events:
        yield from runner.consume(event)
    yield from runner.finish()


def replay_v4_causal_signals(
    *,
    events: Iterable[ReplayEvent],
    sessions: Sequence[ReplaySession],
    config: V4CausalReplayConfig,
) -> V4CausalReplayResult:
    """Collect a bounded v4 replay; use the iterator for full-day ledgers."""

    runner = V4CausalReplayRunner(config=config, sessions=sessions)
    origins: list[V4OriginCohort] = []
    for event in events:
        origins.extend(runner.consume(event))
    origins.extend(runner.finish())
    return V4CausalReplayResult(
        config=config,
        session_selection=runner.session_selection,
        origins=tuple(origins),
        polls_processed=runner.polls_processed,
        input_events=runner.input_events,
        ignored_events=runner.ignored_events,
    )


SESSION_SQL = """
SELECT
    connection_id,
    source,
    connected_wall_ns,
    ready_wall_ns,
    disconnected_wall_ns,
    messages_accepted_total,
    parse_errors_total,
    records_dropped_total
FROM raw_capture.feed_sessions
WHERE connected_wall_ns < $2
  AND COALESCE(disconnected_wall_ns, $2) > $1
  AND (ready_wall_ns IS NULL OR ready_wall_ns < $2)
  AND source IN (
      'binance_futures_agg_trade',
      'polymarket_chainlink_rtds'
  )
ORDER BY connected_wall_ns, source, connection_id
"""


FUTURES_INTEGRITY_SQL = """
WITH scoped AS (
    SELECT
        traces.*,
        traces.first_received_wall_ns >= sessions.ready_wall_ns
            AND traces.last_received_wall_ns < sessions.disconnected_wall_ns
            AS in_session
    FROM raw_capture.binance_futures_price_trace_100ms traces
    JOIN raw_capture.feed_sessions sessions
      ON sessions.connection_id = traces.connection_id
     AND sessions.source = 'binance_futures_agg_trade'
    WHERE traces.bucket_start_ms >= $1
      AND traces.bucket_start_ms < $2
      AND traces.last_received_wall_ns >= $3
      AND traces.last_received_wall_ns < $4
      AND traces.connection_id = ANY($5::uuid[])
), ordered AS (
    SELECT
        connection_id,
        bucket_start_ms,
        event_count,
        last_received_wall_ns,
        last_received_monotonic_ns,
        lag(last_received_monotonic_ns) OVER (
            PARTITION BY connection_id
            ORDER BY
                bucket_start_ms,
                last_received_monotonic_ns,
                last_agg_trade_id
        ) AS previous_monotonic_ns,
        lag(last_received_wall_ns) OVER (
            PARTITION BY connection_id
            ORDER BY
                bucket_start_ms,
                last_received_monotonic_ns,
                last_agg_trade_id
        ) AS previous_wall_ns
    FROM scoped
    WHERE in_session
), aggregated AS (
SELECT
    connection_id,
    count(*) AS raw_row_count,
    COALESCE(sum(event_count), 0) AS raw_accepted_total,
    count(*) - count(DISTINCT bucket_start_ms) AS duplicate_key_count,
    count(*) FILTER (
        WHERE previous_monotonic_ns IS NOT NULL
          AND last_received_monotonic_ns < previous_monotonic_ns
    ) AS monotonic_regression_count,
    count(*) FILTER (
        WHERE previous_wall_ns IS NOT NULL
          AND last_received_wall_ns < previous_wall_ns
    ) AS wall_regression_count,
    (array_agg(bucket_start_ms ORDER BY bucket_start_ms))[1]
        AS first_logical_key,
    (array_agg(bucket_start_ms ORDER BY bucket_start_ms DESC))[1]
        AS last_logical_key,
    (array_agg(last_received_monotonic_ns ORDER BY bucket_start_ms))[1]
        AS first_monotonic_ns,
    (array_agg(last_received_monotonic_ns ORDER BY bucket_start_ms DESC))[1]
        AS last_monotonic_ns,
    (array_agg(last_received_wall_ns ORDER BY bucket_start_ms))[1]
        AS first_wall_ns,
    (array_agg(last_received_wall_ns ORDER BY bucket_start_ms DESC))[1]
        AS last_wall_ns
FROM ordered
GROUP BY connection_id
), outside AS (
    SELECT
        connection_id,
        count(*) FILTER (WHERE NOT in_session) AS out_of_session_count
    FROM scoped
    GROUP BY connection_id
)
SELECT
    outside.connection_id,
    COALESCE(aggregated.raw_row_count, 0) AS raw_row_count,
    COALESCE(aggregated.raw_accepted_total, 0) AS raw_accepted_total,
    COALESCE(aggregated.duplicate_key_count, 0) AS duplicate_key_count,
    COALESCE(aggregated.monotonic_regression_count, 0)
        AS monotonic_regression_count,
    COALESCE(aggregated.wall_regression_count, 0) AS wall_regression_count,
    aggregated.first_logical_key,
    aggregated.last_logical_key,
    aggregated.first_monotonic_ns,
    aggregated.last_monotonic_ns,
    aggregated.first_wall_ns,
    aggregated.last_wall_ns,
    COALESCE(outside.out_of_session_count, 0) AS out_of_session_count
FROM outside
LEFT JOIN aggregated USING (connection_id)
"""


CHAINLINK_INTEGRITY_SQL = """
WITH scoped AS (
    SELECT
        events.*,
        events.received_wall_ns >= sessions.ready_wall_ns
            AND events.received_wall_ns < sessions.disconnected_wall_ns
            AS in_session
    FROM raw_capture.chainlink_price_events events
    JOIN raw_capture.feed_sessions sessions
      ON sessions.connection_id = events.connection_id
     AND sessions.source = 'polymarket_chainlink_rtds'
    WHERE events.received_wall_ns >= $1
      AND events.received_wall_ns < $2
      AND events.connection_id = ANY($3::uuid[])
), ordered AS (
    SELECT
        connection_id,
        receive_sequence,
        received_wall_ns,
        received_monotonic_ns,
        lag(received_monotonic_ns) OVER (
            PARTITION BY connection_id
            ORDER BY receive_sequence
        ) AS previous_monotonic_ns,
        lag(received_wall_ns) OVER (
            PARTITION BY connection_id
            ORDER BY receive_sequence
        ) AS previous_wall_ns
    FROM scoped
    WHERE in_session
), aggregated AS (
SELECT
    connection_id,
    count(*) AS raw_row_count,
    count(*) AS raw_accepted_total,
    count(*) - count(DISTINCT receive_sequence) AS duplicate_key_count,
    count(*) FILTER (
        WHERE previous_monotonic_ns IS NOT NULL
          AND received_monotonic_ns < previous_monotonic_ns
    ) AS monotonic_regression_count,
    count(*) FILTER (
        WHERE previous_wall_ns IS NOT NULL
          AND received_wall_ns < previous_wall_ns
    ) AS wall_regression_count,
    (array_agg(receive_sequence ORDER BY receive_sequence))[1]
        AS first_logical_key,
    (array_agg(receive_sequence ORDER BY receive_sequence DESC))[1]
        AS last_logical_key,
    (array_agg(received_monotonic_ns ORDER BY receive_sequence))[1]
        AS first_monotonic_ns,
    (array_agg(received_monotonic_ns ORDER BY receive_sequence DESC))[1]
        AS last_monotonic_ns,
    (array_agg(received_wall_ns ORDER BY receive_sequence))[1]
        AS first_wall_ns,
    (array_agg(received_wall_ns ORDER BY receive_sequence DESC))[1]
        AS last_wall_ns
FROM ordered
GROUP BY connection_id
), outside AS (
    SELECT
        connection_id,
        count(*) FILTER (WHERE NOT in_session) AS out_of_session_count
    FROM scoped
    GROUP BY connection_id
)
SELECT
    outside.connection_id,
    COALESCE(aggregated.raw_row_count, 0) AS raw_row_count,
    COALESCE(aggregated.raw_accepted_total, 0) AS raw_accepted_total,
    COALESCE(aggregated.duplicate_key_count, 0) AS duplicate_key_count,
    COALESCE(aggregated.monotonic_regression_count, 0)
        AS monotonic_regression_count,
    COALESCE(aggregated.wall_regression_count, 0) AS wall_regression_count,
    aggregated.first_logical_key,
    aggregated.last_logical_key,
    aggregated.first_monotonic_ns,
    aggregated.last_monotonic_ns,
    aggregated.first_wall_ns,
    aggregated.last_wall_ns,
    COALESCE(outside.out_of_session_count, 0) AS out_of_session_count
FROM outside
LEFT JOIN aggregated USING (connection_id)
"""


REPLAY_EVENTS_SQL = """
SELECT
    event_kind,
    received_wall_ns,
    received_monotonic_ns,
    connection_id,
    sequence,
    source_timestamp_ms,
    value,
    event_count
FROM (
    SELECT
        0 AS kind_order,
        'futures'::text AS event_kind,
        traces.last_received_wall_ns AS received_wall_ns,
        traces.last_received_monotonic_ns AS received_monotonic_ns,
        traces.connection_id,
        traces.last_agg_trade_id AS sequence,
        traces.last_trade_time_ms AS source_timestamp_ms,
        traces.close_price AS value,
        traces.event_count
    FROM raw_capture.binance_futures_price_trace_100ms traces
    JOIN raw_capture.feed_sessions futures_sessions
      ON futures_sessions.connection_id = traces.connection_id
     AND futures_sessions.source = 'binance_futures_agg_trade'
    WHERE traces.bucket_start_ms >= $1
      AND traces.bucket_start_ms < $2
      AND traces.last_received_wall_ns >= $3
      AND traces.last_received_wall_ns < $4
      AND traces.connection_id = ANY($5::uuid[])

    UNION ALL

    SELECT
        1 AS kind_order,
        'chainlink'::text AS event_kind,
        events.received_wall_ns,
        events.received_monotonic_ns,
        events.connection_id,
        events.receive_sequence AS sequence,
        events.provider_event_ms AS source_timestamp_ms,
        events.price AS value,
        1 AS event_count
    FROM raw_capture.chainlink_price_events events
    JOIN raw_capture.feed_sessions chainlink_sessions
      ON chainlink_sessions.connection_id = events.connection_id
     AND chainlink_sessions.source = 'polymarket_chainlink_rtds'
    WHERE events.received_wall_ns >= $3
      AND events.received_wall_ns < $4
      AND events.connection_id = ANY($5::uuid[])
) events
ORDER BY
    received_wall_ns,
    kind_order,
    received_monotonic_ns,
    sequence,
    connection_id
"""


PARTITION_MANIFEST_SQL = """
SELECT
    parent.relname AS parent_table,
    child.relname AS partition_name,
    pg_get_expr(child.relpartbound, child.oid) AS partition_bound
FROM pg_inherits inheritance
JOIN pg_class parent ON parent.oid = inheritance.inhparent
JOIN pg_class child ON child.oid = inheritance.inhrelid
JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
WHERE namespace.nspname = 'raw_capture'
  AND parent.relname IN (
      'binance_futures_price_trace_100ms',
      'chainlink_price_events'
  )
ORDER BY parent.relname, child.relname
"""


ORPHAN_CONNECTIONS_SQL = """
SELECT event_kind, count(DISTINCT connection_id) AS orphan_connections
FROM (
    SELECT
        'futures'::text AS event_kind,
        traces.connection_id
    FROM raw_capture.binance_futures_price_trace_100ms traces
    LEFT JOIN raw_capture.feed_sessions sessions
      ON sessions.connection_id = traces.connection_id
     AND sessions.source = 'binance_futures_agg_trade'
    WHERE traces.bucket_start_ms >= $1
      AND traces.bucket_start_ms < $2
      AND traces.last_received_wall_ns >= $3
      AND traces.last_received_wall_ns < $4
      AND sessions.connection_id IS NULL

    UNION ALL

    SELECT
        'chainlink'::text AS event_kind,
        events.connection_id
    FROM raw_capture.chainlink_price_events events
    LEFT JOIN raw_capture.feed_sessions sessions
      ON sessions.connection_id = events.connection_id
     AND sessions.source = 'polymarket_chainlink_rtds'
    WHERE events.received_wall_ns >= $3
      AND events.received_wall_ns < $4
      AND sessions.connection_id IS NULL
) orphaned
GROUP BY event_kind
"""


def _session_from_row(row: Mapping[str, Any]) -> ReplaySession:
    return ReplaySession(
        connection_id=row["connection_id"],
        source=row["source"],
        connected_wall_ns=row["connected_wall_ns"],
        ready_wall_ns=row["ready_wall_ns"],
        disconnected_wall_ns=row["disconnected_wall_ns"],
        messages_accepted_total=row["messages_accepted_total"],
        parse_errors_total=row["parse_errors_total"],
        records_dropped_total=row["records_dropped_total"],
    )


class _IntegrityAccumulator:
    def __init__(self) -> None:
        self.raw_row_count = 0
        self.raw_accepted_total = 0
        self.duplicate_key_count = 0
        self.monotonic_regression_count = 0
        self.wall_regression_count = 0
        self.out_of_session_count = 0
        self.last_logical_key: Optional[int] = None
        self.last_monotonic_ns: Optional[int] = None
        self.last_wall_ns: Optional[int] = None

    def add(self, row: Mapping[str, Any]) -> None:
        self.raw_row_count += int(row["raw_row_count"])
        self.raw_accepted_total += int(row["raw_accepted_total"])
        self.duplicate_key_count += int(row["duplicate_key_count"])
        self.monotonic_regression_count += int(
            row["monotonic_regression_count"]
        )
        self.wall_regression_count += int(row["wall_regression_count"])
        self.out_of_session_count += int(row["out_of_session_count"])
        if row["first_logical_key"] is None:
            return
        first_logical_key = int(row["first_logical_key"])
        first_monotonic_ns = int(row["first_monotonic_ns"])
        first_wall_ns = int(row["first_wall_ns"])
        if self.last_logical_key is not None:
            if first_logical_key == self.last_logical_key:
                self.duplicate_key_count += 1
            elif first_logical_key < self.last_logical_key:
                self.wall_regression_count += 1
            if first_monotonic_ns < self.last_monotonic_ns:
                self.monotonic_regression_count += 1
            if first_wall_ns < self.last_wall_ns:
                self.wall_regression_count += 1
        self.last_logical_key = int(row["last_logical_key"])
        self.last_monotonic_ns = int(row["last_monotonic_ns"])
        self.last_wall_ns = int(row["last_wall_ns"])

    def to_fields(self) -> dict[str, int]:
        return {
            "raw_row_count": self.raw_row_count,
            "raw_accepted_total": self.raw_accepted_total,
            "duplicate_key_count": self.duplicate_key_count,
            "monotonic_regression_count": self.monotonic_regression_count,
            "wall_regression_count": self.wall_regression_count,
            "out_of_session_count": self.out_of_session_count,
        }


async def _load_sessions_with_integrity(
    connection: asyncpg.Connection,
    *,
    config: ReplayConfig,
    chunk_ms: int,
) -> tuple[list[ReplaySession], int, int]:
    warmup_start_ns = max(
        0,
        config.start_ms - config.history_retention_ms - config.poll_ms,
    ) * NS_PER_MS
    report_end_ns = config.end_ms * NS_PER_MS
    rows = await connection.fetch(SESSION_SQL, warmup_start_ns, report_end_ns)
    sessions = [_session_from_row(row) for row in rows]
    completed_ready = [
        session
        for session in sessions
        if session.ready_wall_ns is not None
        and session.disconnected_wall_ns is not None
    ]
    if not completed_ready:
        return sessions, warmup_start_ns, report_end_ns

    data_start_ns = min(session.ready_wall_ns for session in completed_ready)
    data_end_ns = max(
        session.disconnected_wall_ns for session in completed_ready
    )
    futures_ids = [
        session.connection_id
        for session in completed_ready
        if session.source == FUTURES_SESSION_SOURCE
    ]
    chainlink_ids = [
        session.connection_id
        for session in completed_ready
        if session.source == CHAINLINK_SESSION_SOURCE
    ]
    integrity_by_id: dict[UUID, _IntegrityAccumulator] = {}
    chunk_ns = chunk_ms * NS_PER_MS
    chunk_start_ns = data_start_ns
    while chunk_start_ns < data_end_ns:
        chunk_end_ns = min(data_end_ns, chunk_start_ns + chunk_ns)
        chunk_start_ms = chunk_start_ns // NS_PER_MS
        chunk_end_ms = (chunk_end_ns + NS_PER_MS - 1) // NS_PER_MS
        if futures_ids:
            integrity_rows = await connection.fetch(
                FUTURES_INTEGRITY_SQL,
                (chunk_start_ms // 100) * 100,
                _ceil_to_multiple(chunk_end_ms, 100),
                chunk_start_ns,
                chunk_end_ns,
                futures_ids,
            )
            for row in integrity_rows:
                integrity_by_id.setdefault(
                    row["connection_id"],
                    _IntegrityAccumulator(),
                ).add(row)
        if chainlink_ids:
            integrity_rows = await connection.fetch(
                CHAINLINK_INTEGRITY_SQL,
                chunk_start_ns,
                chunk_end_ns,
                chainlink_ids,
            )
            for row in integrity_rows:
                integrity_by_id.setdefault(
                    row["connection_id"],
                    _IntegrityAccumulator(),
                ).add(row)
        chunk_start_ns = chunk_end_ns

    updated_sessions = []
    for session in sessions:
        accumulator = integrity_by_id.get(session.connection_id)
        if accumulator is None:
            updated_sessions.append(session)
            continue
        updated_sessions.append(
            replace(
                session,
                integrity_checked=True,
                **accumulator.to_fields(),
            )
        )
    return updated_sessions, data_start_ns, data_end_ns


def _event_from_row(row: Mapping[str, Any]) -> ReplayEvent:
    return ReplayEvent(
        kind=row["event_kind"],
        received_wall_ns=row["received_wall_ns"],
        received_monotonic_ns=row["received_monotonic_ns"],
        connection_id=row["connection_id"],
        sequence=row["sequence"],
        source_timestamp_ms=row["source_timestamp_ms"],
        value=row["value"],
        event_count=row["event_count"],
    )


async def _iter_database_events(
    connection: asyncpg.Connection,
    *,
    start_ns: int,
    end_ns: int,
    connection_ids: Sequence[UUID],
    chunk_ms: int,
) -> AsyncIterator[ReplayEvent]:
    _require_positive_int(chunk_ms, "chunk_ms")
    if chunk_ms > DEFAULT_DATABASE_CHUNK_MS:
        raise ValueError("chunk_ms cannot exceed five minutes")
    chunk_ns = chunk_ms * NS_PER_MS
    chunk_start_ns = start_ns
    while chunk_start_ns < end_ns:
        chunk_end_ns = min(end_ns, chunk_start_ns + chunk_ns)
        start_ms = chunk_start_ns // NS_PER_MS
        end_ms = (chunk_end_ns + NS_PER_MS - 1) // NS_PER_MS
        futures_bucket_start_ms = (start_ms // 100) * 100
        futures_bucket_end_ms = _ceil_to_multiple(end_ms, 100)
        rows = await connection.fetch(
            REPLAY_EVENTS_SQL,
            futures_bucket_start_ms,
            futures_bucket_end_ms,
            chunk_start_ns,
            chunk_end_ns,
            list(connection_ids),
        )
        for row in rows:
            yield _event_from_row(row)
        chunk_start_ns = chunk_end_ns


async def _audit_orphans_in_chunks(
    connection: asyncpg.Connection,
    *,
    start_ns: int,
    end_ns: int,
    chunk_ms: int,
) -> None:
    chunk_ns = chunk_ms * NS_PER_MS
    chunk_start_ns = start_ns
    while chunk_start_ns < end_ns:
        chunk_end_ns = min(end_ns, chunk_start_ns + chunk_ns)
        start_ms = chunk_start_ns // NS_PER_MS
        end_ms = (chunk_end_ns + NS_PER_MS - 1) // NS_PER_MS
        rows = await connection.fetch(
            ORPHAN_CONNECTIONS_SQL,
            (start_ms // 100) * 100,
            _ceil_to_multiple(end_ms, 100),
            chunk_start_ns,
            chunk_end_ns,
        )
        if any(int(row["orphan_connections"]) for row in rows):
            raise ReplayDataError(
                "raw capture contains connections without sessions"
            )
        chunk_start_ns = chunk_end_ns


async def replay_from_database(
    *,
    database_url: str,
    config: ReplayConfig,
    chunk_ms: int = DEFAULT_DATABASE_CHUNK_MS,
) -> ReplayReport:
    if not isinstance(database_url, str) or not database_url:
        raise ValueError("DATABASE_URL is required")
    _require_positive_int(chunk_ms, "chunk_ms")
    if chunk_ms > DEFAULT_DATABASE_CHUNK_MS:
        raise ValueError("chunk_ms cannot exceed five minutes")
    connection = await asyncpg.connect(
        dsn=database_url,
        server_settings={
            "application_name": REPLAY_APPLICATION_NAME,
            "statement_timeout": "1500",
            "lock_timeout": "1000",
            "default_transaction_read_only": "on",
        },
    )
    try:
        manifest_before = [tuple(row) for row in await connection.fetch(
            PARTITION_MANIFEST_SQL
        )]
        sessions, _integrity_start_ns, _integrity_end_ns = (
            await _load_sessions_with_integrity(
                connection,
                config=config,
                chunk_ms=chunk_ms,
            )
        )
        runner = ShadowReplayRunner(config=config, sessions=sessions)
        segments = runner.session_selection.segments
        audit_start_ns = max(
            0,
            config.start_ms
            - config.history_retention_ms
            - config.poll_ms,
        ) * NS_PER_MS
        audit_end_ns = config.end_ms * NS_PER_MS
        await _audit_orphans_in_chunks(
            connection,
            start_ns=audit_start_ns,
            end_ns=audit_end_ns,
            chunk_ms=chunk_ms,
        )
        if segments:
            warmup_start_ns = max(
                min(segment.start_wall_ns for segment in segments),
                max(
                    0,
                    config.start_ms
                    - config.history_retention_ms
                    - config.poll_ms,
                ) * NS_PER_MS,
            )
            replay_end_ns = min(
                config.end_ms * NS_PER_MS,
                max(segment.end_wall_ns for segment in segments),
            )
            async for event in _iter_database_events(
                connection,
                start_ns=warmup_start_ns,
                end_ns=replay_end_ns,
                connection_ids=tuple(
                    runner.session_selection.eligible_session_ids
                ),
                chunk_ms=chunk_ms,
            ):
                runner.consume(event)
        report = runner.finish()
        manifest_after = [tuple(row) for row in await connection.fetch(
            PARTITION_MANIFEST_SQL
        )]
        if manifest_after != manifest_before:
            raise ReplayDataError(
                "raw partition manifest changed during replay; rerun the window"
            )
        return report
    finally:
        await connection.close()


def _parse_csv_ints(raw: str, field_name: str) -> tuple[int, ...]:
    try:
        values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{field_name} must be comma-separated integers"
        ) from exc
    if not values:
        raise argparse.ArgumentTypeError(f"{field_name} must not be empty")
    return values


def _parse_decimal(raw: str, field_name: str) -> Decimal:
    try:
        value = Decimal(raw)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"{field_name} must be decimal") from exc
    if not value.is_finite():
        raise argparse.ArgumentTypeError(f"{field_name} must be finite")
    return value


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay raw futures and Chainlink capture into shadow-signal metrics"
        )
    )
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--lags-ms", default="3000,3500,4000")
    parser.add_argument("--beta", default="1")
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--evaluation-interval-ms", type=int, default=500)
    parser.add_argument("--futures-stale-ms", type=int, default=1_000)
    parser.add_argument("--chainlink-stale-ms", type=int, default=5_000)
    parser.add_argument("--reference-max-gap-ms", type=int, default=250)
    parser.add_argument("--history-retention-ms", type=int, default=10_000)
    parser.add_argument("--max-future-skew-ms", type=int, default=0)
    parser.add_argument("--futures-availability-delay-ms", type=int, default=0)
    parser.add_argument("--chainlink-availability-delay-ms", type=int, default=0)
    parser.add_argument("--evaluation-phase-offset-ms", type=int, default=0)
    parser.add_argument("--neutral-band-bps", default="1")
    parser.add_argument("--chunk-ms", type=int, default=DEFAULT_DATABASE_CHUNK_MS)
    parser.add_argument(
        "--quantile-sample-max",
        type=int,
        default=DEFAULT_QUANTILE_SAMPLE_MAX,
    )
    parser.add_argument(
        "--strict-parse-error-sessions",
        action="store_true",
        help="Exclude otherwise complete sessions that contain parse errors",
    )
    return parser


def config_from_arguments(arguments: argparse.Namespace) -> ReplayConfig:
    return ReplayConfig(
        start_ms=arguments.start_ms,
        end_ms=arguments.end_ms,
        lags_ms=_parse_csv_ints(arguments.lags_ms, "lags_ms"),
        beta=_parse_decimal(arguments.beta, "beta"),
        poll_ms=arguments.poll_ms,
        evaluation_interval_ms=arguments.evaluation_interval_ms,
        futures_stale_ms=arguments.futures_stale_ms,
        chainlink_stale_ms=arguments.chainlink_stale_ms,
        reference_max_gap_ms=arguments.reference_max_gap_ms,
        history_retention_ms=arguments.history_retention_ms,
        max_future_skew_ms=arguments.max_future_skew_ms,
        futures_availability_delay_ms=(
            arguments.futures_availability_delay_ms
        ),
        chainlink_availability_delay_ms=(
            arguments.chainlink_availability_delay_ms
        ),
        evaluation_phase_offset_ms=arguments.evaluation_phase_offset_ms,
        neutral_band_bps=_parse_decimal(
            arguments.neutral_band_bps,
            "neutral_band_bps",
        ),
        quantile_sample_max=arguments.quantile_sample_max,
        exclude_parse_error_sessions=arguments.strict_parse_error_sessions,
    )


async def _run_cli(arguments: argparse.Namespace) -> ReplayReport:
    config = config_from_arguments(arguments)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required in the environment")
    return await replay_from_database(
        database_url=database_url,
        config=config,
        chunk_ms=arguments.chunk_ms,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)
    try:
        report = asyncio.run(_run_cli(arguments))
        if arguments.output is None:
            sys.stdout.write(encode_replay_report(report) + "\n")
        else:
            write_replay_report(arguments.output, report)
        if report.status != "ok":
            print(f"shadow replay incomplete: {report.status}", file=sys.stderr)
            return 2
    except (
        OSError,
        RuntimeError,
        ValueError,
        ReplayDataError,
        asyncpg.PostgresError,
    ) as exc:
        print(f"shadow replay failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
