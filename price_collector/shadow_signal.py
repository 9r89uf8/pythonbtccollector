from __future__ import annotations

"""Pure state and math for Chainlink catch-up shadow models.

The no-change baseline is intentionally paired with each catch-up model at that
model's horizon by replay/evaluation code; it is not a separate stateful model.
"""

from collections import deque
from dataclasses import dataclass
from decimal import Decimal, DecimalException
from typing import Deque, Optional, Sequence
from uuid import UUID

from price_collector.market import MarketWindow, market_for_sample_second


ONE = Decimal("1")
BASIS_POINTS = Decimal("10000")

VALID = "valid"
WARMING_UP = "warming_up"
CHAINLINK_UNAVAILABLE = "chainlink_unavailable"
FUTURES_UNAVAILABLE = "futures_unavailable"
CHAINLINK_STALE = "chainlink_stale"
FUTURES_STALE = "futures_stale"
ANCHOR_HISTORY_MISSING = "anchor_history_missing"
ANCHOR_REFERENCE_GAP = "anchor_reference_gap"
TIMESTAMP_REGRESSION = "timestamp_regression"
MODEL_ERROR = "model_error"

WARMING_UP_FUTURES_HISTORY = "warming_up_futures_history"
WAITING_FOR_NEW_CHAINLINK_ANCHOR = "waiting_for_new_chainlink_anchor"
ANCHORED = "anchored"


def _require_non_negative_int(value: object, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_positive_int(value: object, field_name: str) -> None:
    _require_non_negative_int(value, field_name)
    if value == 0:
        raise ValueError(f"{field_name} must be positive")


def _require_price(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be finite and positive")


@dataclass(frozen=True)
class ObservedPrice:
    value: Decimal
    source_timestamp_ms: Optional[int]
    received_ms: int
    publisher_epoch: Optional[str] = None
    accepted_event_sequence: Optional[int] = None

    def __post_init__(self) -> None:
        _require_price(self.value, "value")
        if self.source_timestamp_ms is not None:
            _require_non_negative_int(
                self.source_timestamp_ms,
                "source_timestamp_ms",
            )
        _require_non_negative_int(self.received_ms, "received_ms")
        metadata_present = (
            self.publisher_epoch is not None,
            self.accepted_event_sequence is not None,
        )
        if metadata_present[0] != metadata_present[1]:
            raise ValueError(
                "publisher_epoch and accepted_event_sequence must be provided together"
            )
        if self.publisher_epoch is not None:
            if not isinstance(self.publisher_epoch, str):
                raise TypeError("publisher_epoch must be a string")
            try:
                parsed_epoch = UUID(self.publisher_epoch)
            except (ValueError, AttributeError) as exc:
                raise ValueError("publisher_epoch must be a canonical UUID") from exc
            if str(parsed_epoch) != self.publisher_epoch:
                raise ValueError("publisher_epoch must be a canonical UUID")
            _require_positive_int(
                self.accepted_event_sequence,
                "accepted_event_sequence",
            )

    @property
    def identity(self) -> tuple[Optional[int], int, Decimal]:
        return (self.source_timestamp_ms, self.received_ms, self.value)


@dataclass(frozen=True)
class CatchupModel:
    version: str
    lag_ms: int
    beta: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.version, str):
            raise TypeError("version must be a string")
        if not self.version.strip():
            raise ValueError("version must not be empty")
        _require_positive_int(self.lag_ms, "lag_ms")
        if not isinstance(self.beta, Decimal):
            raise TypeError("beta must be Decimal")
        if not self.beta.is_finite() or self.beta < 0:
            raise ValueError("beta must be finite and non-negative")


@dataclass(frozen=True)
class ModelAnchor:
    chainlink: ObservedPrice
    futures_reference: ObservedPrice


@dataclass(frozen=True)
class Projection:
    model_version: str
    horizon_ms: int
    projected_chainlink: Decimal
    pending_move: Decimal
    pending_move_bps: Decimal
    direction: str


@dataclass(frozen=True)
class ModelSignal:
    model_version: str
    horizon_ms: int
    generated_ms: int
    valid: bool
    status: str
    invalid_reasons: tuple[str, ...]
    state: str
    projection: Optional[Projection]
    anchor: Optional[ModelAnchor]
    futures_now: Optional[ObservedPrice]
    chainlink_now: Optional[ObservedPrice]
    futures_received_age_ms: Optional[int]
    chainlink_received_age_ms: Optional[int]
    futures_reference_target_ms: Optional[int]
    futures_reference_gap_ms: Optional[int]
    full_horizon_before_market_end: bool


@dataclass(frozen=True)
class EngineObservation:
    generated_ms: int
    market: MarketWindow
    ms_to_market_end: int
    signals: tuple[ModelSignal, ...]
    timestamp_regression_sources: tuple[str, ...]
    futures_history_size: int

    def signal_for(self, model_version: str) -> ModelSignal:
        for signal in self.signals:
            if signal.model_version == model_version:
                return signal
        raise KeyError(model_version)


def project_from_anchor(
    *,
    model: CatchupModel,
    anchor: ModelAnchor,
    futures_now: ObservedPrice,
) -> Projection:
    futures_return = (
        futures_now.value / anchor.futures_reference.value
    ) - ONE
    projected = anchor.chainlink.value * (
        ONE + model.beta * futures_return
    )
    if not projected.is_finite() or projected <= 0:
        raise ValueError("projected Chainlink price must be finite and positive")

    pending = projected - anchor.chainlink.value
    if not pending.is_finite():
        raise ValueError("pending move must be finite")
    pending_bps = pending / anchor.chainlink.value * BASIS_POINTS
    if not pending_bps.is_finite():
        raise ValueError("pending move basis points must be finite")
    if pending > 0:
        direction = "up"
    elif pending < 0:
        direction = "down"
    else:
        direction = "flat"

    return Projection(
        model_version=model.version,
        horizon_ms=model.lag_ms,
        projected_chainlink=projected,
        pending_move=pending,
        pending_move_bps=pending_bps,
        direction=direction,
    )


def no_change_projection(
    *,
    chainlink: ObservedPrice,
    horizon_ms: int,
    model_version: str = "baseline_no_change",
) -> Projection:
    _require_positive_int(horizon_ms, "horizon_ms")
    if not isinstance(model_version, str):
        raise TypeError("model_version must be a string")
    if not model_version.strip():
        raise ValueError("model_version must not be empty")
    return Projection(
        model_version=model_version,
        horizon_ms=horizon_ms,
        projected_chainlink=chainlink.value,
        pending_move=Decimal("0"),
        pending_move_bps=Decimal("0"),
        direction="flat",
    )


class ShadowSignalEngine:
    def __init__(
        self,
        *,
        models: Sequence[CatchupModel],
        futures_stale_ms: int,
        chainlink_stale_ms: int,
        reference_max_gap_ms: int,
        history_retention_ms: int,
        max_future_skew_ms: int = 250,
    ) -> None:
        self.models = tuple(models)
        if not self.models:
            raise ValueError("at least one catch-up model is required")
        if not all(isinstance(model, CatchupModel) for model in self.models):
            raise TypeError("models must contain only CatchupModel values")
        versions = [model.version for model in self.models]
        if len(set(versions)) != len(versions):
            raise ValueError("model versions must be unique")

        _require_positive_int(futures_stale_ms, "futures_stale_ms")
        _require_positive_int(chainlink_stale_ms, "chainlink_stale_ms")
        _require_non_negative_int(reference_max_gap_ms, "reference_max_gap_ms")
        _require_positive_int(history_retention_ms, "history_retention_ms")
        _require_non_negative_int(max_future_skew_ms, "max_future_skew_ms")

        minimum_retention_ms = (
            max(model.lag_ms for model in self.models)
            + chainlink_stale_ms
            + reference_max_gap_ms
        )
        if history_retention_ms < minimum_retention_ms:
            raise ValueError(
                "history_retention_ms must cover the maximum model lag, "
                "Chainlink freshness allowance, and reference gap"
            )

        self.futures_stale_ms = futures_stale_ms
        self.chainlink_stale_ms = chainlink_stale_ms
        self.reference_max_gap_ms = reference_max_gap_ms
        self.history_retention_ms = history_retention_ms
        self.max_future_skew_ms = max_future_skew_ms

        self._futures_history: Deque[ObservedPrice] = deque()
        self._last_futures_identity: Optional[
            tuple[Optional[int], int, Decimal]
        ] = None
        self._last_chainlink_identity: Optional[
            tuple[Optional[int], int, Decimal]
        ] = None
        self._last_futures_received_ms: Optional[int] = None
        self._last_chainlink_received_ms: Optional[int] = None
        self._futures_source_timestamp_watermark: Optional[int] = None
        self._chainlink_source_timestamp_watermark: Optional[int] = None
        self._last_now_ms: Optional[int] = None

        self._anchors: dict[str, ModelAnchor] = {}
        self._anchor_targets: dict[str, int] = {}
        self._anchor_gaps: dict[str, int] = {}
        self._anchor_failures: dict[str, str] = {}

    def observe(
        self,
        *,
        futures: Optional[ObservedPrice],
        chainlink: Optional[ObservedPrice],
        now_ms: int,
    ) -> EngineObservation:
        _require_non_negative_int(now_ms, "now_ms")
        if futures is not None and not isinstance(futures, ObservedPrice):
            raise TypeError("futures must be ObservedPrice or None")
        if chainlink is not None and not isinstance(chainlink, ObservedPrice):
            raise TypeError("chainlink must be ObservedPrice or None")

        if self._last_now_ms is not None and now_ms < self._last_now_ms:
            self._reset_all(futures=futures, chainlink=chainlink)
            self._last_now_ms = now_ms
            return self._build_observation(
                futures=futures,
                chainlink=chainlink,
                now_ms=now_ms,
                regression_sources=("engine_clock",),
            )
        self._last_now_ms = now_ms

        excessive_future_sources = tuple(
            source
            for source, price in (
                ("futures", futures),
                ("chainlink", chainlink),
            )
            if price is not None
            and price.received_ms - now_ms > self.max_future_skew_ms
        )
        if excessive_future_sources:
            return self._build_observation(
                futures=futures,
                chainlink=chainlink,
                now_ms=now_ms,
                regression_sources=excessive_future_sources,
            )

        futures_regressed = self._is_futures_regression(futures)
        chainlink_regressed = self._is_chainlink_regression(chainlink)
        if futures_regressed:
            self._reset_for_futures_regression(futures)
            regression_sources = ["futures"]
            if chainlink_regressed:
                self._reset_for_chainlink_regression(chainlink)
                regression_sources.append("chainlink")
            else:
                self._quarantine_chainlink_if_new(chainlink)
            return self._build_observation(
                futures=futures,
                chainlink=chainlink,
                now_ms=now_ms,
                regression_sources=tuple(regression_sources),
            )

        history_existed_before_poll = bool(self._futures_history)
        self._ingest_futures(futures, now_ms=now_ms)
        self._prune_futures(now_ms)

        if chainlink_regressed:
            self._reset_for_chainlink_regression(chainlink)
            return self._build_observation(
                futures=futures,
                chainlink=chainlink,
                now_ms=now_ms,
                regression_sources=("chainlink",),
            )

        self._ingest_chainlink(
            chainlink,
            now_ms=now_ms,
            history_existed_before_poll=history_existed_before_poll,
        )
        return self._build_observation(
            futures=futures,
            chainlink=chainlink,
            now_ms=now_ms,
            regression_sources=(),
        )

    def _is_futures_regression(
        self,
        futures: Optional[ObservedPrice],
    ) -> bool:
        if futures is None or futures.identity == self._last_futures_identity:
            return False
        return self._timestamps_regress(
            futures,
            last_received_ms=self._last_futures_received_ms,
            source_timestamp_watermark=self._futures_source_timestamp_watermark,
        )

    def _is_chainlink_regression(
        self,
        chainlink: Optional[ObservedPrice],
    ) -> bool:
        if chainlink is None or chainlink.identity == self._last_chainlink_identity:
            return False
        return self._timestamps_regress(
            chainlink,
            last_received_ms=self._last_chainlink_received_ms,
            source_timestamp_watermark=self._chainlink_source_timestamp_watermark,
        )

    @staticmethod
    def _timestamps_regress(
        price: ObservedPrice,
        *,
        last_received_ms: Optional[int],
        source_timestamp_watermark: Optional[int],
    ) -> bool:
        if last_received_ms is not None and price.received_ms < last_received_ms:
            return True
        return (
            price.source_timestamp_ms is not None
            and source_timestamp_watermark is not None
            and price.source_timestamp_ms < source_timestamp_watermark
        )

    def _ingest_futures(
        self,
        futures: Optional[ObservedPrice],
        *,
        now_ms: int,
    ) -> None:
        if futures is None or futures.identity == self._last_futures_identity:
            return
        self._last_futures_identity = futures.identity
        self._last_futures_received_ms = futures.received_ms
        if futures.source_timestamp_ms is not None:
            self._futures_source_timestamp_watermark = max(
                futures.source_timestamp_ms,
                self._futures_source_timestamp_watermark
                if self._futures_source_timestamp_watermark is not None
                else futures.source_timestamp_ms,
            )
        if self._received_age_ms(futures, now_ms) > self.futures_stale_ms:
            return
        self._futures_history.append(futures)

    def _ingest_chainlink(
        self,
        chainlink: Optional[ObservedPrice],
        *,
        now_ms: int,
        history_existed_before_poll: bool,
    ) -> None:
        if chainlink is None or chainlink.identity == self._last_chainlink_identity:
            return

        self._last_chainlink_identity = chainlink.identity
        self._last_chainlink_received_ms = chainlink.received_ms
        if chainlink.source_timestamp_ms is not None:
            self._chainlink_source_timestamp_watermark = max(
                chainlink.source_timestamp_ms,
                self._chainlink_source_timestamp_watermark
                if self._chainlink_source_timestamp_watermark is not None
                else chainlink.source_timestamp_ms,
            )

        chainlink_is_fresh = (
            self._received_age_ms(chainlink, now_ms) <= self.chainlink_stale_ms
        )
        for model in self.models:
            self._clear_anchor(model.version)
            if not chainlink_is_fresh:
                continue

            target_ms = chainlink.received_ms - model.lag_ms
            self._anchor_targets[model.version] = target_ms
            if not history_existed_before_poll:
                self._anchor_failures[model.version] = ANCHOR_HISTORY_MISSING
                continue

            reference, gap_ms = self._find_reference(target_ms)
            if reference is None:
                self._anchor_failures[model.version] = ANCHOR_HISTORY_MISSING
                continue

            self._anchor_gaps[model.version] = gap_ms
            if gap_ms > self.reference_max_gap_ms:
                self._anchor_failures[model.version] = ANCHOR_REFERENCE_GAP
                continue

            self._anchors[model.version] = ModelAnchor(
                chainlink=chainlink,
                futures_reference=reference,
            )
            self._anchor_failures.pop(model.version, None)

    def _find_reference(
        self,
        target_ms: int,
    ) -> tuple[Optional[ObservedPrice], int]:
        for observation in reversed(self._futures_history):
            if observation.received_ms <= target_ms:
                return observation, target_ms - observation.received_ms
        return None, 0

    def _history_is_ready(self, model: CatchupModel, now_ms: int) -> bool:
        target_ms = now_ms - model.lag_ms
        reference, gap_ms = self._find_reference(target_ms)
        return reference is not None and gap_ms <= self.reference_max_gap_ms

    def _prune_futures(self, now_ms: int) -> None:
        cutoff_ms = now_ms - self.history_retention_ms
        while (
            self._futures_history
            and self._futures_history[0].received_ms < cutoff_ms
        ):
            self._futures_history.popleft()

    def _clear_anchor(self, model_version: str) -> None:
        self._anchors.pop(model_version, None)
        self._anchor_targets.pop(model_version, None)
        self._anchor_gaps.pop(model_version, None)
        self._anchor_failures.pop(model_version, None)

    def _clear_all_anchors(self) -> None:
        self._anchors.clear()
        self._anchor_targets.clear()
        self._anchor_gaps.clear()
        self._anchor_failures.clear()

    def _reset_for_futures_regression(self, futures: ObservedPrice) -> None:
        self._futures_history.clear()
        self._clear_all_anchors()
        self._last_futures_identity = futures.identity
        self._last_futures_received_ms = futures.received_ms
        self._futures_source_timestamp_watermark = futures.source_timestamp_ms

    def _reset_for_chainlink_regression(self, chainlink: ObservedPrice) -> None:
        self._clear_all_anchors()
        self._last_chainlink_identity = chainlink.identity
        self._last_chainlink_received_ms = chainlink.received_ms
        self._chainlink_source_timestamp_watermark = chainlink.source_timestamp_ms

    def _quarantine_chainlink_if_new(
        self,
        chainlink: Optional[ObservedPrice],
    ) -> None:
        if chainlink is None or chainlink.identity == self._last_chainlink_identity:
            return
        self._last_chainlink_identity = chainlink.identity
        self._last_chainlink_received_ms = chainlink.received_ms
        if chainlink.source_timestamp_ms is not None:
            self._chainlink_source_timestamp_watermark = max(
                chainlink.source_timestamp_ms,
                self._chainlink_source_timestamp_watermark
                if self._chainlink_source_timestamp_watermark is not None
                else chainlink.source_timestamp_ms,
            )

    def _reset_all(
        self,
        *,
        futures: Optional[ObservedPrice],
        chainlink: Optional[ObservedPrice],
    ) -> None:
        self._futures_history.clear()
        self._clear_all_anchors()

        self._last_futures_identity = futures.identity if futures is not None else None
        self._last_futures_received_ms = (
            futures.received_ms if futures is not None else None
        )
        self._futures_source_timestamp_watermark = (
            futures.source_timestamp_ms if futures is not None else None
        )

        self._last_chainlink_identity = (
            chainlink.identity if chainlink is not None else None
        )
        self._last_chainlink_received_ms = (
            chainlink.received_ms if chainlink is not None else None
        )
        self._chainlink_source_timestamp_watermark = (
            chainlink.source_timestamp_ms if chainlink is not None else None
        )

    def _build_observation(
        self,
        *,
        futures: Optional[ObservedPrice],
        chainlink: Optional[ObservedPrice],
        now_ms: int,
        regression_sources: tuple[str, ...],
    ) -> EngineObservation:
        sample_second_ms = (now_ms // 1000) * 1000
        market = market_for_sample_second(sample_second_ms)
        ms_to_market_end = max(0, market.market_end_ms - now_ms)
        signals = tuple(
            self._build_model_signal(
                model=model,
                futures=futures,
                chainlink=chainlink,
                now_ms=now_ms,
                market=market,
                regression_sources=regression_sources,
            )
            for model in self.models
        )
        return EngineObservation(
            generated_ms=now_ms,
            market=market,
            ms_to_market_end=ms_to_market_end,
            signals=signals,
            timestamp_regression_sources=regression_sources,
            futures_history_size=len(self._futures_history),
        )

    def _build_model_signal(
        self,
        *,
        model: CatchupModel,
        futures: Optional[ObservedPrice],
        chainlink: Optional[ObservedPrice],
        now_ms: int,
        market: MarketWindow,
        regression_sources: tuple[str, ...],
    ) -> ModelSignal:
        anchor = self._anchors.get(model.version)
        state = self._model_state(model, now_ms=now_ms, anchor=anchor)
        futures_age_ms = (
            self._received_age_ms(futures, now_ms)
            if futures is not None
            else None
        )
        chainlink_age_ms = (
            self._received_age_ms(chainlink, now_ms)
            if chainlink is not None
            else None
        )
        full_horizon = now_ms + model.lag_ms <= market.market_end_ms

        if regression_sources:
            return self._invalid_model_signal(
                model=model,
                now_ms=now_ms,
                status=TIMESTAMP_REGRESSION,
                reasons=(TIMESTAMP_REGRESSION,),
                state=state,
                anchor=anchor,
                futures=futures,
                chainlink=chainlink,
                futures_age_ms=futures_age_ms,
                chainlink_age_ms=chainlink_age_ms,
                full_horizon=full_horizon,
            )

        reasons: list[str] = []
        if chainlink is None:
            reasons.append(CHAINLINK_UNAVAILABLE)
        elif chainlink_age_ms is not None and chainlink_age_ms > self.chainlink_stale_ms:
            reasons.append(CHAINLINK_STALE)

        if futures is None:
            reasons.append(FUTURES_UNAVAILABLE)
        elif futures_age_ms is not None and futures_age_ms > self.futures_stale_ms:
            reasons.append(FUTURES_STALE)

        if anchor is None:
            anchor_reason = self._anchor_reason(model, state=state)
            if anchor_reason not in reasons:
                reasons.append(anchor_reason)

        if reasons:
            status = self._status_for_reasons(reasons)
            return self._invalid_model_signal(
                model=model,
                now_ms=now_ms,
                status=status,
                reasons=tuple(reasons),
                state=state,
                anchor=anchor,
                futures=futures,
                chainlink=chainlink,
                futures_age_ms=futures_age_ms,
                chainlink_age_ms=chainlink_age_ms,
                full_horizon=full_horizon,
            )

        try:
            projection = project_from_anchor(
                model=model,
                anchor=anchor,
                futures_now=futures,
            )
        except (ArithmeticError, DecimalException, ValueError):
            return self._invalid_model_signal(
                model=model,
                now_ms=now_ms,
                status=MODEL_ERROR,
                reasons=(MODEL_ERROR,),
                state=state,
                anchor=anchor,
                futures=futures,
                chainlink=chainlink,
                futures_age_ms=futures_age_ms,
                chainlink_age_ms=chainlink_age_ms,
                full_horizon=full_horizon,
            )

        return ModelSignal(
            model_version=model.version,
            horizon_ms=model.lag_ms,
            generated_ms=now_ms,
            valid=True,
            status=VALID,
            invalid_reasons=(),
            state=state,
            projection=projection,
            anchor=anchor,
            futures_now=futures,
            chainlink_now=chainlink,
            futures_received_age_ms=futures_age_ms,
            chainlink_received_age_ms=chainlink_age_ms,
            futures_reference_target_ms=self._anchor_targets.get(model.version),
            futures_reference_gap_ms=self._anchor_gaps.get(model.version),
            full_horizon_before_market_end=full_horizon,
        )

    def _model_state(
        self,
        model: CatchupModel,
        *,
        now_ms: int,
        anchor: Optional[ModelAnchor],
    ) -> str:
        if anchor is not None:
            return ANCHORED
        if self._history_is_ready(model, now_ms):
            return WAITING_FOR_NEW_CHAINLINK_ANCHOR
        return WARMING_UP_FUTURES_HISTORY

    def _anchor_reason(self, model: CatchupModel, *, state: str) -> str:
        failure = self._anchor_failures.get(model.version)
        if failure is not None:
            return failure
        return state

    @staticmethod
    def _status_for_reasons(reasons: Sequence[str]) -> str:
        priority = (
            MODEL_ERROR,
            CHAINLINK_UNAVAILABLE,
            FUTURES_UNAVAILABLE,
            CHAINLINK_STALE,
            FUTURES_STALE,
            ANCHOR_HISTORY_MISSING,
            ANCHOR_REFERENCE_GAP,
        )
        for status in priority:
            if status in reasons:
                return status
        return WARMING_UP

    def _invalid_model_signal(
        self,
        *,
        model: CatchupModel,
        now_ms: int,
        status: str,
        reasons: tuple[str, ...],
        state: str,
        anchor: Optional[ModelAnchor],
        futures: Optional[ObservedPrice],
        chainlink: Optional[ObservedPrice],
        futures_age_ms: Optional[int],
        chainlink_age_ms: Optional[int],
        full_horizon: bool,
    ) -> ModelSignal:
        return ModelSignal(
            model_version=model.version,
            horizon_ms=model.lag_ms,
            generated_ms=now_ms,
            valid=False,
            status=status,
            invalid_reasons=reasons,
            state=state,
            projection=None,
            anchor=anchor,
            futures_now=futures,
            chainlink_now=chainlink,
            futures_received_age_ms=futures_age_ms,
            chainlink_received_age_ms=chainlink_age_ms,
            futures_reference_target_ms=self._anchor_targets.get(model.version),
            futures_reference_gap_ms=self._anchor_gaps.get(model.version),
            full_horizon_before_market_end=full_horizon,
        )

    @staticmethod
    def _received_age_ms(price: ObservedPrice, now_ms: int) -> int:
        # The worker may capture now immediately before its Redis MGET, so an
        # observation written during that read can be a few milliseconds newer.
        return max(0, now_ms - price.received_ms)
