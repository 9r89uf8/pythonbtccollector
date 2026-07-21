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
from typing import Any, AsyncIterator, Deque, Iterable, Mapping, Optional, Sequence
from uuid import UUID

import asyncpg

from price_collector.shadow_signal import (
    BASIS_POINTS,
    CatchupModel,
    EngineObservation,
    ModelSignal,
    ObservedPrice,
    ShadowSignalEngine,
    no_change_projection,
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
    allowed_chainlink_parse_error_totals: tuple[int, ...] = (0,)

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
        if not isinstance(self.allowed_chainlink_parse_error_totals, tuple):
            raise TypeError("allowed_chainlink_parse_error_totals must be tuple")
        if not self.allowed_chainlink_parse_error_totals:
            raise ValueError(
                "allowed_chainlink_parse_error_totals must not be empty"
            )
        for parse_errors_total in self.allowed_chainlink_parse_error_totals:
            _require_non_negative_int(
                parse_errors_total,
                "allowed_chainlink_parse_error_totals",
            )
        if self.allowed_chainlink_parse_error_totals not in ((0,), (0, 2)):
            raise ValueError(
                "allowed_chainlink_parse_error_totals must be (0,) or the "
                "incident-specific recovery policy (0, 2)"
            )
        if (
            self.allowed_chainlink_parse_error_totals != (0,)
            and not self.exclude_parse_error_sessions
        ):
            raise ValueError(
                "a Chainlink parse-error allowance requires strict session filtering"
            )

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
            is_allowed_chainlink_total = (
                self.source == CHAINLINK_SESSION_SOURCE
                and self.parse_errors_total
                in config.allowed_chainlink_parse_error_totals
            )
            if not is_allowed_chainlink_total:
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
    parse_error_totals_by_source: Mapping[str, Mapping[str, int]]
    parse_error_exception_applied_by_source: Mapping[str, int]
    excluded_session_ids_by_reason: Mapping[str, tuple[str, ...]]


def select_replay_sessions(
    sessions: Sequence[ReplaySession],
    config: ReplayConfig,
) -> SessionSelection:
    total_by_source: Counter[str] = Counter()
    eligible_by_source: Counter[str] = Counter()
    excluded_by_reason: Counter[str] = Counter()
    parse_error_totals_by_source: dict[str, Counter[str]] = {
        FUTURES_SESSION_SOURCE: Counter(),
        CHAINLINK_SESSION_SOURCE: Counter(),
    }
    parse_error_exception_applied_by_source: Counter[str] = Counter()
    excluded_session_ids_by_reason: dict[str, list[str]] = {}
    excluded_raw_rows = 0
    eligible: dict[str, list[ReplaySession]] = {
        FUTURES_SESSION_SOURCE: [],
        CHAINLINK_SESSION_SOURCE: [],
    }

    for session in sessions:
        total_by_source[session.source] += 1
        parse_error_totals_by_source[session.source][
            str(session.parse_errors_total)
        ] += 1
        reasons = session.exclusion_reasons(config)
        if (
            config.allowed_chainlink_parse_error_totals == (0, 2)
            and session.source == CHAINLINK_SESSION_SOURCE
            and session.parse_errors_total == 2
            and "parse_errors" not in reasons
        ):
            parse_error_exception_applied_by_source[session.source] += 1
        if reasons:
            excluded_by_reason.update(reasons)
            for reason in reasons:
                excluded_session_ids_by_reason.setdefault(reason, []).append(
                    str(session.connection_id)
                )
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
        parse_error_totals_by_source={
            source: dict(sorted(counts.items()))
            for source, counts in parse_error_totals_by_source.items()
        },
        parse_error_exception_applied_by_source=dict(
            parse_error_exception_applied_by_source
        ),
        excluded_session_ids_by_reason={
            reason: tuple(sorted(connection_ids))
            for reason, connection_ids in sorted(
                excluded_session_ids_by_reason.items()
            )
        },
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
                **(
                    {
                        "allowed_chainlink_parse_error_totals": list(
                            self.config.allowed_chainlink_parse_error_totals
                        )
                    }
                    if self.config.allowed_chainlink_parse_error_totals != (0,)
                    else {}
                ),
            },
            "data_quality": {
                "session_policy": (
                    "completed_clean_integrity_checked"
                    if self.config.allowed_chainlink_parse_error_totals == (0,)
                    else (
                        "completed_integrity_checked_with_exact_chainlink_"
                        "parse_error_allowlist"
                    )
                ),
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
                **(
                    {
                        "parse_error_totals_by_source": dict(
                            self.session_selection.parse_error_totals_by_source
                        ),
                        "parse_error_exception_applied_by_source": dict(
                            self.session_selection
                            .parse_error_exception_applied_by_source
                        ),
                        "excluded_session_ids_by_reason": {
                            reason: list(connection_ids)
                            for reason, connection_ids in (
                                self.session_selection
                                .excluded_session_ids_by_reason.items()
                            )
                        },
                    }
                    if self.config.allowed_chainlink_parse_error_totals == (0, 2)
                    else {}
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
                    *(
                        [
                            "The incident-specific Chainlink parse-error exception "
                            "is based on persisted per-session counters; rejected "
                            "frame bodies were not stored for per-session verification."
                        ]
                        if self.config.allowed_chainlink_parse_error_totals
                        == (0, 2)
                        else []
                    ),
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
