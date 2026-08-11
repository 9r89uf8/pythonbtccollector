"""Causal, Decimal-only shadow forecasts for the Chainlink 30-second TWAP.

The model is intentionally independent of databases, Redis, asyncio, and wall
clock access.  Callers supply both source and local receive timestamps, which
lets the same core be used live and in receive-order backtests without making
post-cutoff observations visible to an earlier forecast.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from types import MappingProxyType
from typing import Mapping, Optional, Sequence

from price_collector.market import market_for_sample_second


SCHEMA_VERSION = 1
MODEL_VERSION = 1

# These constants, the three-source consensus rule, and the runtime's fixed
# 250 ms input polling / 60-sample warm-up define model version 1. Changing any
# of them requires incrementing MODEL_VERSION so retained forecasts remain
# comparable.

SOURCE_FUTURES = "futures"
SOURCE_CHAINLINK_SPOT = "chainlink_spot"
SOURCE_BINANCE_SPOT = "binance_spot"

WINDOW_MS = 30_000
BASIS_WINDOW_MS = 30 * 60 * 1_000
RECENT_ERROR_WINDOW_MS = BASIS_WINDOW_MS
EXPECTED_ACTUAL_RECEIVE_DELAY_MS = 1_800
HORIZONS_SECONDS = (1, 3, 5, 10)
MIN_CONSENSUS_SOURCES = 2
DEFAULT_STALE_AFTER_MS = 10_000
DEFAULT_MIN_BASIS_SAMPLES = 1

PRICE_QUANTUM = Decimal("0.000000000000000001")
BPS_QUANTUM = Decimal("0.00000001")
FRACTION_QUANTUM = Decimal("0.00000001")
TEN_THOUSAND = Decimal("10000")
NUMERIC_38_18_LIMIT = Decimal("1e20")
NUMERIC_20_8_LIMIT = Decimal("1e12")
_DECIMAL_PRECISION = 80
_POSTGRES_BIGINT_MAX = (2**63) - 1
_FIXED_QUALITY_FLAG = "futures_proxy_polled_latest_wins"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    endpoint_delay_ms: int


SOURCE_SPECS = (
    SourceSpec(SOURCE_FUTURES, 2_800),
    SourceSpec(SOURCE_CHAINLINK_SPOT, 1_700),
    SourceSpec(SOURCE_BINANCE_SPOT, 2_500),
)
SOURCE_NAMES = tuple(spec.name for spec in SOURCE_SPECS)
SOURCE_ENDPOINT_DELAYS_MS: Mapping[str, int] = MappingProxyType(
    {spec.name: spec.endpoint_delay_ms for spec in SOURCE_SPECS}
)
SOURCE_OBSERVATION_RETENTION_MS = (
    BASIS_WINDOW_MS + WINDOW_MS + max(SOURCE_ENDPOINT_DELAYS_MS.values())
)


def _require_int(value: object, field_name: str, *, nonnegative: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if nonnegative and value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    if value > _POSTGRES_BIGINT_MAX:
        raise ValueError(f"{field_name} exceeds PostgreSQL BIGINT")
    return value


def _require_price(value: object, field_name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    if value >= NUMERIC_38_18_LIMIT:
        raise ValueError(f"{field_name} exceeds NUMERIC(38,18)")
    return value


def _require_source(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("source must be a string")
    if value not in SOURCE_ENDPOINT_DELAYS_MS:
        raise ValueError(f"unsupported shadow source: {value!r}")
    return value


def _quantize_price(value: Decimal) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_PRECISION
            quantized = value.quantize(PRICE_QUANTUM, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError("price cannot be represented as NUMERIC(38,18)") from exc
    if not quantized.is_finite() or quantized <= 0:
        raise ValueError("price must remain finite and positive")
    if quantized >= NUMERIC_38_18_LIMIT:
        raise ValueError("price cannot be represented as NUMERIC(38,18)")
    return quantized


def _quantize_bps(value: Decimal) -> Decimal:
    try:
        with localcontext() as context:
            context.prec = _DECIMAL_PRECISION
            quantized = value.quantize(BPS_QUANTUM, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError("basis cannot be represented as NUMERIC(20,8)") from exc
    if not quantized.is_finite() or abs(quantized) >= NUMERIC_20_8_LIMIT:
        raise ValueError("basis cannot be represented as NUMERIC(20,8)")
    return quantized


def _quantize_fraction(value: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        return value.quantize(FRACTION_QUANTUM, rounding=ROUND_HALF_UP)


def _median(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise ValueError("median requires at least one value")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    with localcontext() as context:
        context.prec = _DECIMAL_PRECISION
        return (ordered[middle - 1] + ordered[middle]) / Decimal(2)


def _nearest_rank_p90(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise ValueError("p90 requires at least one value")
    ordered = sorted(values)
    rank = (9 * len(ordered) + 9) // 10
    return ordered[rank - 1]


def _decimal_text(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else format(value, "f")


def _flags(*groups: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({flag for group in groups for flag in group}))


@dataclass(frozen=True)
class SourceObservation:
    source: str
    price: Decimal
    source_ms: int
    received_ms: int
    sequence: int = 0

    def __post_init__(self) -> None:
        _require_source(self.source)
        _require_price(self.price, "price")
        _require_int(self.source_ms, "source_ms")
        _require_int(self.received_ms, "received_ms")
        _require_int(self.sequence, "sequence")


@dataclass(frozen=True)
class ActualTwapObservation:
    price: Decimal
    provider_event_ms: int
    received_ms: int
    sequence: int = 0

    def __post_init__(self) -> None:
        _require_price(self.price, "price")
        event_ms = _require_int(self.provider_event_ms, "provider_event_ms")
        if event_ms % 1_000:
            raise ValueError("provider_event_ms must be aligned to a UTC second")
        _require_int(self.received_ms, "received_ms")
        _require_int(self.sequence, "sequence")


@dataclass(frozen=True)
class SourceBiasState:
    source: str
    median_bps: Optional[Decimal]
    sample_count: int


@dataclass(frozen=True)
class SourceForecast:
    source: str
    endpoint_delay_ms: int
    target_second_ms: int
    window_start_ms: int
    window_end_ms: int
    raw_twap: Optional[Decimal]
    bias_bps: Optional[Decimal]
    adjusted_twap: Optional[Decimal]
    known_fraction: Decimal
    basis_sample_count: int
    latest_source_ms: Optional[int]
    latest_received_ms: Optional[int]
    source_age_ms: Optional[int]
    received_age_ms: Optional[int]
    ready: bool
    status: str
    quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class HorizonForecast:
    horizon_seconds: int
    target_second_ms: int
    target_market_id: int
    expected_actual_received_ms: int
    value: Optional[Decimal]
    known_fraction: Optional[Decimal]
    source_count: int
    source_spread_bps: Optional[Decimal]
    estimated_error_bps: Optional[Decimal]
    status: str
    quality_flags: tuple[str, ...]
    sources: tuple[SourceForecast, ...]


@dataclass(frozen=True)
class RecentErrorMetadata:
    window_seconds: int
    sample_count: int
    oldest_provider_second_ms: Optional[int]
    newest_provider_second_ms: Optional[int]
    p90_abs_error_bps: Optional[Decimal]


@dataclass(frozen=True)
class ForecastBatch:
    schema_version: int
    model_version: int
    origin_second_ms: int
    generated_ms: int
    basis_window_seconds: int
    status: str
    quality_flags: tuple[str, ...]
    source_basis: tuple[SourceBiasState, ...]
    recent_error: RecentErrorMetadata
    forecasts: tuple[HorizonForecast, ...]

    @property
    def source_bias_bps(self) -> Mapping[str, Optional[Decimal]]:
        return MappingProxyType(
            {item.source: item.median_bps for item in self.source_basis}
        )

    @property
    def basis_sample_counts(self) -> Mapping[str, int]:
        return MappingProxyType(
            {item.source: item.sample_count for item in self.source_basis}
        )

    def to_live_payload(self) -> dict[str, object]:
        """Return the strict live-cache representation for the latest batch."""

        predictions: dict[str, object] = {}
        for forecast in self.forecasts:
            predictions[f"h{forecast.horizon_seconds}"] = {
                "horizon_seconds": forecast.horizon_seconds,
                "target_second_ms": forecast.target_second_ms,
                "target_market_id": forecast.target_market_id,
                "expected_actual_received_ms": forecast.expected_actual_received_ms,
                "value": _decimal_text(forecast.value),
                "known_fraction": _decimal_text(forecast.known_fraction),
                "source_count": forecast.source_count,
                "source_spread_bps": _decimal_text(forecast.source_spread_bps),
                "estimated_error_bps": _decimal_text(forecast.estimated_error_bps),
            }

        return {
            "schema_version": self.schema_version,
            "model_version": self.model_version,
            "origin_second_ms": self.origin_second_ms,
            "generated_ms": self.generated_ms,
            "basis_window_seconds": self.basis_window_seconds,
            "status": self.status,
            "quality_flags": list(self.quality_flags),
            "source_bias_bps": {
                source: _decimal_text(self.source_bias_bps[source])
                for source in SOURCE_NAMES
            },
            "basis_sample_counts": {
                source: self.basis_sample_counts[source] for source in SOURCE_NAMES
            },
            "recent_p90_abs_error_bps": _decimal_text(
                self.recent_error.p90_abs_error_bps
            ),
            "predictions": predictions,
        }


@dataclass(frozen=True)
class SourceResidualUpdate:
    source: str
    raw_twap: Optional[Decimal]
    residual_bps: Optional[Decimal]
    stored: bool
    quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class ActualUpdate:
    provider_second_ms: int
    received_ms: int
    ensemble_nowcast: Optional[Decimal]
    ensemble_source_count: int
    abs_nowcast_error_bps: Optional[Decimal]
    source_residuals: tuple[SourceResidualUpdate, ...]
    quality_flags: tuple[str, ...]


@dataclass(frozen=True)
class _StoredObservation:
    observation: SourceObservation
    arrival_order: int


@dataclass(frozen=True)
class _BasisSample:
    provider_second_ms: int
    received_ms: int
    residual_bps: Decimal


@dataclass(frozen=True)
class _ErrorSample:
    provider_second_ms: int
    received_ms: int
    abs_error_bps: Decimal


@dataclass(frozen=True)
class _RawWindow:
    value: Optional[Decimal]
    known_fraction: Decimal
    seed_source_ms: Optional[int]


class TwapShadowModel:
    """Stateful causal consensus model with immutable forecast outputs."""

    def __init__(
        self,
        *,
        stale_after_ms: int = DEFAULT_STALE_AFTER_MS,
        min_basis_samples: int = DEFAULT_MIN_BASIS_SAMPLES,
    ) -> None:
        self.stale_after_ms = _require_int(stale_after_ms, "stale_after_ms")
        self.min_basis_samples = _require_int(
            min_basis_samples, "min_basis_samples"
        )
        if self.stale_after_ms <= 0:
            raise ValueError("stale_after_ms must be positive")
        if self.min_basis_samples <= 0:
            raise ValueError("min_basis_samples must be positive")

        self._observations: dict[str, list[_StoredObservation]] = {
            source: [] for source in SOURCE_NAMES
        }
        self._basis: dict[str, dict[int, _BasisSample]] = {
            source: {} for source in SOURCE_NAMES
        }
        self._errors: dict[int, _ErrorSample] = {}
        self._arrival_order = 0
        self._watermark_ms: Optional[int] = None
        self._last_batch: Optional[ForecastBatch] = None

    def observe_source(self, observation: SourceObservation) -> None:
        if not isinstance(observation, SourceObservation):
            raise TypeError("observation must be SourceObservation")
        if (
            self._watermark_ms is not None
            and observation.source_ms
            < self._watermark_ms - SOURCE_OBSERVATION_RETENTION_MS
        ):
            # A revision this old cannot contribute to any retained actual
            # basis key.  Ignoring it also prevents a late packet from
            # resurrecting history already removed at the causal watermark.
            return
        self._arrival_order += 1
        self._observations[observation.source].append(
            _StoredObservation(observation, self._arrival_order)
        )

    def observe_actual(
        self,
        observation: ActualTwapObservation,
        *,
        record_nowcast_error: bool = True,
    ) -> ActualUpdate:
        if not isinstance(observation, ActualTwapObservation):
            raise TypeError("observation must be ActualTwapObservation")
        if not isinstance(record_nowcast_error, bool):
            raise TypeError("record_nowcast_error must be a boolean")
        provider_ms = observation.provider_event_ms
        if (
            self._watermark_ms is not None
            and provider_ms <= self._watermark_ms - BASIS_WINDOW_MS
        ):
            raise ValueError("actual observation is older than the retained basis window")

        self._evict(provider_ms)
        for source in SOURCE_NAMES:
            self._basis[source].pop(provider_ms, None)
        self._errors.pop(provider_ms, None)

        raw_by_source: dict[str, _RawWindow] = {}
        fresh_by_source: dict[str, bool] = {}
        latest_by_source: dict[str, Optional[_StoredObservation]] = {}
        for source in SOURCE_NAMES:
            delay_ms = SOURCE_ENDPOINT_DELAYS_MS[source]
            end_ms = provider_ms - delay_ms
            start_ms = end_ms - WINDOW_MS
            raw, latest = self._source_window(
                source=source,
                start_ms=start_ms,
                end_ms=end_ms,
                source_cutoff_ms=provider_ms,
                received_cutoff_ms=observation.received_ms,
                known_at_ms=provider_ms,
            )
            raw_by_source[source] = raw
            latest_by_source[source] = latest
            fresh_by_source[source] = self._is_fresh(
                latest,
                source_reference_ms=provider_ms,
                received_reference_ms=observation.received_ms,
            )

        nowcast_values: list[Decimal] = []
        if record_nowcast_error:
            for source in SOURCE_NAMES:
                raw_value = raw_by_source[source].value
                bias, count = self._basis_stats(
                    source=source,
                    provider_cutoff_ms=provider_ms,
                    received_cutoff_ms=observation.received_ms,
                    include_cutoff=False,
                )
                if (
                    raw_value is None
                    or not fresh_by_source[source]
                    or bias is None
                    or count < self.min_basis_samples
                ):
                    continue
                adjusted = self._adjusted_price(raw_value, bias)
                if adjusted is not None:
                    nowcast_values.append(adjusted)

        ensemble_nowcast: Optional[Decimal] = None
        abs_error_bps: Optional[Decimal] = None
        if len(nowcast_values) >= MIN_CONSENSUS_SOURCES:
            ensemble_nowcast = _quantize_price(_median(nowcast_values))
            with localcontext() as context:
                context.prec = _DECIMAL_PRECISION
                abs_error_bps = _quantize_bps(
                    abs(
                        (ensemble_nowcast / observation.price - Decimal(1))
                        * TEN_THOUSAND
                    )
                )
            self._errors[provider_ms] = _ErrorSample(
                provider_second_ms=provider_ms,
                received_ms=observation.received_ms,
                abs_error_bps=abs_error_bps,
            )

        residual_updates: list[SourceResidualUpdate] = []
        for source in SOURCE_NAMES:
            raw_value = raw_by_source[source].value
            update_flags: list[str] = []
            residual: Optional[Decimal] = None
            if raw_value is None:
                update_flags.append("missing_seed")
            elif not fresh_by_source[source]:
                update_flags.append("source_stale")
            else:
                try:
                    with localcontext() as context:
                        context.prec = _DECIMAL_PRECISION
                        residual = _quantize_bps(
                            (observation.price / raw_value - Decimal(1))
                            * TEN_THOUSAND
                        )
                except ValueError:
                    update_flags.append("numeric_overflow")
                else:
                    self._basis[source][provider_ms] = _BasisSample(
                        provider_second_ms=provider_ms,
                        received_ms=observation.received_ms,
                        residual_bps=residual,
                    )
            residual_updates.append(
                SourceResidualUpdate(
                    source=source,
                    raw_twap=raw_value,
                    residual_bps=residual,
                    stored=residual is not None,
                    quality_flags=_flags(update_flags),
                )
            )

        self._watermark_ms = (
            provider_ms
            if self._watermark_ms is None
            else max(self._watermark_ms, provider_ms)
        )
        update_flags = () if ensemble_nowcast is not None else ("nowcast_unavailable",)
        return ActualUpdate(
            provider_second_ms=provider_ms,
            received_ms=observation.received_ms,
            ensemble_nowcast=ensemble_nowcast,
            ensemble_source_count=len(nowcast_values),
            abs_nowcast_error_bps=abs_error_bps,
            source_residuals=tuple(residual_updates),
            quality_flags=update_flags,
        )

    def forecast(self, *, origin_ms: int, issued_ms: int) -> ForecastBatch:
        origin = _require_int(origin_ms, "origin_ms")
        generated = _require_int(issued_ms, "issued_ms")
        if origin % 1_000:
            raise ValueError("origin_ms must be aligned to a UTC second")
        if generated < origin or generated >= origin + 1_000:
            raise ValueError("issued_ms must fall within the origin UTC second")

        if self._last_batch is not None:
            if origin < self._last_batch.origin_second_ms:
                raise ValueError("forecast origins must not regress")
            if origin == self._last_batch.origin_second_ms:
                return self._last_batch

        self._evict(origin)
        error_metadata = self._recent_error_metadata(
            provider_cutoff_ms=origin,
            received_cutoff_ms=generated,
        )
        source_basis = tuple(
            SourceBiasState(
                source=source,
                median_bps=bias,
                sample_count=count,
            )
            for source in SOURCE_NAMES
            for bias, count in (
                self._basis_stats(
                    source=source,
                    provider_cutoff_ms=origin,
                    received_cutoff_ms=generated,
                    include_cutoff=True,
                ),
            )
        )
        bias_by_source = {item.source: item for item in source_basis}

        forecasts = tuple(
            self._forecast_horizon(
                origin_ms=origin,
                issued_ms=generated,
                horizon_seconds=horizon_seconds,
                bias_by_source=bias_by_source,
                estimated_error_bps=error_metadata.p90_abs_error_bps,
            )
            for horizon_seconds in HORIZONS_SECONDS
        )

        available = sum(item.value is not None for item in forecasts)
        if available == len(forecasts):
            status = (
                "ready"
                if all(item.status == "ready" for item in forecasts)
                else "degraded"
            )
        elif available:
            status = "degraded"
        elif any(item.status == "warming_up" for item in forecasts):
            status = "warming_up"
        else:
            status = "unavailable"

        batch_flags: list[str] = [_FIXED_QUALITY_FLAG]
        if error_metadata.p90_abs_error_bps is None:
            batch_flags.append("recent_error_unavailable")
        if any(item.source_count == MIN_CONSENSUS_SOURCES for item in forecasts):
            batch_flags.append("partial_consensus")
        if any(item.source_count < MIN_CONSENSUS_SOURCES for item in forecasts):
            batch_flags.append("insufficient_sources")
        if any("flat_hold_future" in item.quality_flags for item in forecasts):
            batch_flags.append("flat_hold_future")

        batch = ForecastBatch(
            schema_version=SCHEMA_VERSION,
            model_version=MODEL_VERSION,
            origin_second_ms=origin,
            generated_ms=generated,
            basis_window_seconds=BASIS_WINDOW_MS // 1_000,
            status=status,
            quality_flags=_flags(batch_flags),
            source_basis=source_basis,
            recent_error=error_metadata,
            forecasts=forecasts,
        )
        self._last_batch = batch
        self._watermark_ms = (
            origin if self._watermark_ms is None else max(self._watermark_ms, origin)
        )
        return batch

    def _forecast_horizon(
        self,
        *,
        origin_ms: int,
        issued_ms: int,
        horizon_seconds: int,
        bias_by_source: Mapping[str, SourceBiasState],
        estimated_error_bps: Optional[Decimal],
    ) -> HorizonForecast:
        target_ms = origin_ms + horizon_seconds * 1_000
        source_forecasts: list[SourceForecast] = []
        adjusted_values: list[Decimal] = []
        adjusted_known_fractions: list[Decimal] = []

        for source in SOURCE_NAMES:
            delay_ms = SOURCE_ENDPOINT_DELAYS_MS[source]
            end_ms = target_ms - delay_ms
            start_ms = end_ms - WINDOW_MS
            raw, latest = self._source_window(
                source=source,
                start_ms=start_ms,
                end_ms=end_ms,
                source_cutoff_ms=origin_ms,
                received_cutoff_ms=issued_ms,
                known_at_ms=origin_ms,
            )
            is_fresh = self._is_fresh(
                latest,
                source_reference_ms=origin_ms,
                received_reference_ms=issued_ms,
            )
            basis_state = bias_by_source[source]
            source_flags: list[str] = []
            if raw.known_fraction < Decimal(1):
                source_flags.append("flat_hold_future")
            if raw.value is None:
                source_flags.append("missing_seed")
            if not is_fresh:
                source_flags.append("source_stale")
            if (
                basis_state.median_bps is None
                or basis_state.sample_count < self.min_basis_samples
            ):
                source_flags.append("basis_warming_up")

            adjusted: Optional[Decimal] = None
            if (
                raw.value is not None
                and is_fresh
                and basis_state.median_bps is not None
                and basis_state.sample_count >= self.min_basis_samples
            ):
                adjusted = self._adjusted_price(raw.value, basis_state.median_bps)
                if adjusted is None:
                    source_flags.append("numeric_overflow")

            ready = adjusted is not None
            if ready:
                source_status = "ready"
                adjusted_values.append(adjusted)
                adjusted_known_fractions.append(raw.known_fraction)
            elif raw.value is not None and is_fresh:
                source_status = "warming_up"
            elif not is_fresh:
                source_status = "stale"
            else:
                source_status = "unavailable"

            latest_observation = None if latest is None else latest.observation
            source_forecasts.append(
                SourceForecast(
                    source=source,
                    endpoint_delay_ms=delay_ms,
                    target_second_ms=target_ms,
                    window_start_ms=start_ms,
                    window_end_ms=end_ms,
                    raw_twap=raw.value,
                    bias_bps=basis_state.median_bps,
                    adjusted_twap=adjusted,
                    known_fraction=raw.known_fraction,
                    basis_sample_count=basis_state.sample_count,
                    latest_source_ms=(
                        None
                        if latest_observation is None
                        else latest_observation.source_ms
                    ),
                    latest_received_ms=(
                        None
                        if latest_observation is None
                        else latest_observation.received_ms
                    ),
                    source_age_ms=(
                        None
                        if latest_observation is None
                        else origin_ms - latest_observation.source_ms
                    ),
                    received_age_ms=(
                        None
                        if latest_observation is None
                        else issued_ms - latest_observation.received_ms
                    ),
                    ready=ready,
                    status=source_status,
                    quality_flags=_flags(source_flags),
                )
            )

        consensus: Optional[Decimal] = None
        known_fraction: Optional[Decimal] = None
        spread_bps: Optional[Decimal] = None
        source_count = len(adjusted_values)
        horizon_flags: list[str] = []
        if source_count >= MIN_CONSENSUS_SOURCES:
            consensus = _quantize_price(_median(adjusted_values))
            known_fraction = min(adjusted_known_fractions)
            with localcontext() as context:
                context.prec = _DECIMAL_PRECISION
                spread_bps = _quantize_bps(
                    (max(adjusted_values) - min(adjusted_values))
                    / consensus
                    * TEN_THOUSAND
                )
            if source_count == MIN_CONSENSUS_SOURCES:
                status = "degraded"
                horizon_flags.append("partial_consensus")
            else:
                status = "ready"
        else:
            horizon_flags.append("insufficient_sources")
            raw_fresh_count = sum(
                item.raw_twap is not None and item.status == "warming_up"
                for item in source_forecasts
            )
            status = (
                "warming_up"
                if raw_fresh_count >= MIN_CONSENSUS_SOURCES
                else "unavailable"
            )
        if any("flat_hold_future" in item.quality_flags for item in source_forecasts):
            horizon_flags.append("flat_hold_future")

        return HorizonForecast(
            horizon_seconds=horizon_seconds,
            target_second_ms=target_ms,
            target_market_id=market_for_sample_second(target_ms).market_id,
            expected_actual_received_ms=(
                target_ms + EXPECTED_ACTUAL_RECEIVE_DELAY_MS
            ),
            value=consensus,
            known_fraction=known_fraction,
            source_count=source_count,
            source_spread_bps=spread_bps,
            estimated_error_bps=(
                estimated_error_bps if consensus is not None else None
            ),
            status=status,
            quality_flags=_flags(horizon_flags),
            sources=tuple(source_forecasts),
        )

    def _source_window(
        self,
        *,
        source: str,
        start_ms: int,
        end_ms: int,
        source_cutoff_ms: int,
        received_cutoff_ms: int,
        known_at_ms: int,
    ) -> tuple[_RawWindow, Optional[_StoredObservation]]:
        order_key = lambda stored: (  # noqa: E731 - shared deterministic key
            stored.observation.source_ms,
            stored.observation.received_ms,
            stored.observation.sequence,
            stored.arrival_order,
        )
        seed: Optional[_StoredObservation] = None
        latest: Optional[_StoredObservation] = None
        by_source_ms: dict[int, _StoredObservation] = {}
        for stored in self._observations[source]:
            observation = stored.observation
            if (
                observation.source_ms > source_cutoff_ms
                or observation.received_ms > received_cutoff_ms
            ):
                continue
            if latest is None or order_key(stored) > order_key(latest):
                latest = stored
            if observation.source_ms >= end_ms:
                continue
            if observation.source_ms <= start_ms:
                if seed is None or order_key(stored) > order_key(seed):
                    seed = stored
                continue
            existing = by_source_ms.get(observation.source_ms)
            if existing is None or order_key(stored) > order_key(existing):
                by_source_ms[observation.source_ms] = stored
        ordered = [by_source_ms[key] for key in sorted(by_source_ms)]

        known_end_ms = min(end_ms, known_at_ms)
        known_ms = min(WINDOW_MS, max(0, known_end_ms - start_ms))
        with localcontext() as context:
            context.prec = _DECIMAL_PRECISION
            known_fraction = _quantize_fraction(
                Decimal(known_ms) / Decimal(WINDOW_MS)
            )

        if seed is None:
            return (
                _RawWindow(
                    value=None,
                    known_fraction=known_fraction,
                    seed_source_ms=None,
                ),
                latest,
            )

        cursor_ms = start_ms
        current_price = seed.observation.price
        with localcontext() as context:
            context.prec = _DECIMAL_PRECISION
            area = Decimal(0)
            for stored in ordered:
                event_ms = stored.observation.source_ms
                if event_ms <= start_ms:
                    continue
                area += current_price * Decimal(event_ms - cursor_ms)
                current_price = stored.observation.price
                cursor_ms = event_ms
            area += current_price * Decimal(end_ms - cursor_ms)
            value = _quantize_price(area / Decimal(WINDOW_MS))

        return (
            _RawWindow(
                value=value,
                known_fraction=known_fraction,
                seed_source_ms=seed.observation.source_ms,
            ),
            latest,
        )

    def _is_fresh(
        self,
        observation: Optional[_StoredObservation],
        *,
        source_reference_ms: int,
        received_reference_ms: int,
    ) -> bool:
        if observation is None:
            return False
        source_age_ms = source_reference_ms - observation.observation.source_ms
        received_age_ms = received_reference_ms - observation.observation.received_ms
        return (
            0 <= source_age_ms <= self.stale_after_ms
            and 0 <= received_age_ms <= self.stale_after_ms
        )

    def _basis_stats(
        self,
        *,
        source: str,
        provider_cutoff_ms: int,
        received_cutoff_ms: int,
        include_cutoff: bool,
    ) -> tuple[Optional[Decimal], int]:
        lower_bound_ms = provider_cutoff_ms - BASIS_WINDOW_MS
        samples = [
            sample
            for provider_ms, sample in self._basis[source].items()
            if provider_ms > lower_bound_ms
            and (
                provider_ms <= provider_cutoff_ms
                if include_cutoff
                else provider_ms < provider_cutoff_ms
            )
            and sample.received_ms <= received_cutoff_ms
        ]
        if not samples:
            return None, 0
        median = _quantize_bps(_median([sample.residual_bps for sample in samples]))
        return median, len(samples)

    def _recent_error_metadata(
        self,
        *,
        provider_cutoff_ms: int,
        received_cutoff_ms: int,
    ) -> RecentErrorMetadata:
        lower_bound_ms = provider_cutoff_ms - RECENT_ERROR_WINDOW_MS
        samples = [
            sample
            for provider_ms, sample in self._errors.items()
            if provider_ms > lower_bound_ms
            and provider_ms <= provider_cutoff_ms
            and sample.received_ms <= received_cutoff_ms
        ]
        samples.sort(key=lambda sample: sample.provider_second_ms)
        p90 = (
            None
            if not samples
            else _quantize_bps(
                _nearest_rank_p90([sample.abs_error_bps for sample in samples])
            )
        )
        return RecentErrorMetadata(
            window_seconds=RECENT_ERROR_WINDOW_MS // 1_000,
            sample_count=len(samples),
            oldest_provider_second_ms=(
                None if not samples else samples[0].provider_second_ms
            ),
            newest_provider_second_ms=(
                None if not samples else samples[-1].provider_second_ms
            ),
            p90_abs_error_bps=p90,
        )

    @staticmethod
    def _adjusted_price(raw_twap: Decimal, basis_bps: Decimal) -> Optional[Decimal]:
        try:
            with localcontext() as context:
                context.prec = _DECIMAL_PRECISION
                adjusted = raw_twap * (
                    Decimal(1) + basis_bps / TEN_THOUSAND
                )
            return _quantize_price(adjusted)
        except ValueError:
            return None

    def _evict(self, provider_reference_ms: int) -> None:
        watermark_ms = (
            provider_reference_ms
            if self._watermark_ms is None
            else max(self._watermark_ms, provider_reference_ms)
        )
        basis_cutoff_ms = watermark_ms - BASIS_WINDOW_MS
        for source in SOURCE_NAMES:
            self._basis[source] = {
                provider_ms: sample
                for provider_ms, sample in self._basis[source].items()
                if provider_ms > basis_cutoff_ms
            }
        self._errors = {
            provider_ms: sample
            for provider_ms, sample in self._errors.items()
            if provider_ms > watermark_ms - RECENT_ERROR_WINDOW_MS
        }
        self._prune_observations(watermark_ms)

    def _prune_observations(self, watermark_ms: int) -> None:
        cutoff_ms = watermark_ms - SOURCE_OBSERVATION_RETENTION_MS
        for source in SOURCE_NAMES:
            observations = self._observations[source]
            older = [
                stored
                for stored in observations
                if stored.observation.source_ms < cutoff_ms
            ]
            recent = [
                stored
                for stored in observations
                if stored.observation.source_ms >= cutoff_ms
            ]
            if older:
                seed = max(
                    older,
                    key=lambda stored: (
                        stored.observation.source_ms,
                        stored.observation.received_ms,
                        stored.observation.sequence,
                        stored.arrival_order,
                    ),
                )
                recent.append(seed)
            self._observations[source] = recent


__all__ = [
    "ActualTwapObservation",
    "ActualUpdate",
    "BASIS_WINDOW_MS",
    "BPS_QUANTUM",
    "DEFAULT_MIN_BASIS_SAMPLES",
    "DEFAULT_STALE_AFTER_MS",
    "EXPECTED_ACTUAL_RECEIVE_DELAY_MS",
    "FRACTION_QUANTUM",
    "ForecastBatch",
    "HORIZONS_SECONDS",
    "HorizonForecast",
    "MIN_CONSENSUS_SOURCES",
    "MODEL_VERSION",
    "PRICE_QUANTUM",
    "RecentErrorMetadata",
    "SCHEMA_VERSION",
    "SOURCE_BINANCE_SPOT",
    "SOURCE_CHAINLINK_SPOT",
    "SOURCE_ENDPOINT_DELAYS_MS",
    "SOURCE_FUTURES",
    "SOURCE_NAMES",
    "SOURCE_OBSERVATION_RETENTION_MS",
    "SOURCE_SPECS",
    "SourceBiasState",
    "SourceForecast",
    "SourceObservation",
    "SourceResidualUpdate",
    "TwapShadowModel",
    "WINDOW_MS",
]
