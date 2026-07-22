"""Pure rolling basis-band math for the two-second challenger.

The component samples the dollar futures-minus-Chainlink basis once per
epoch-aligned 500 ms bucket.  Statistics returned for a bucket are calculated
strictly from prior buckets, so the current observation can never influence
the band used to adjust its own projection.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal, DecimalException, localcontext
from typing import Deque, Optional


BASIS_HISTORY_MS = 300_000
BASIS_SAMPLE_CADENCE_MS = 500
BASIS_MIN_SAMPLES = BASIS_HISTORY_MS // BASIS_SAMPLE_CADENCE_MS
BASIS_DECIMAL_PRECISION = 28
BASIS_VOLATILITY_MULTIPLIER = Decimal("0.75")
BASIS_MIN_HALF_WIDTH = Decimal("1")
BASIS_CORRECTION_STRENGTH = Decimal("0.5")


def _require_non_negative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _require_decimal(
    value: object,
    field_name: str,
    *,
    positive: bool = False,
) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if positive and value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


@dataclass(frozen=True)
class BasisBandStats:
    """Strictly-prior rolling statistics for one 500 ms bucket."""

    sample_count: int
    normal_basis: Optional[Decimal]
    population_sd: Optional[Decimal]
    half_width: Optional[Decimal]
    lower_basis: Optional[Decimal]
    upper_basis: Optional[Decimal]
    ready: bool

    def __post_init__(self) -> None:
        sample_count = _require_non_negative_int(
            self.sample_count,
            "sample_count",
        )
        if not isinstance(self.ready, bool):
            raise TypeError("ready must be a boolean")

        values = (
            self.normal_basis,
            self.population_sd,
            self.half_width,
            self.lower_basis,
            self.upper_basis,
        )
        if not self.ready:
            if sample_count >= BASIS_MIN_SAMPLES:
                raise ValueError("a complete basis window must be ready")
            if any(value is not None for value in values):
                raise ValueError("unready basis statistics must be null")
            return

        if sample_count < BASIS_MIN_SAMPLES:
            raise ValueError("ready basis statistics require a complete window")
        if any(value is None for value in values):
            raise ValueError("ready basis statistics must be complete")

        normal = _require_decimal(self.normal_basis, "normal_basis")
        population_sd = _require_decimal(
            self.population_sd,
            "population_sd",
        )
        half_width = _require_decimal(self.half_width, "half_width")
        lower = _require_decimal(self.lower_basis, "lower_basis")
        upper = _require_decimal(self.upper_basis, "upper_basis")
        if population_sd < 0:
            raise ValueError("population_sd must be non-negative")
        if half_width < BASIS_MIN_HALF_WIDTH:
            raise ValueError("half_width is below the frozen minimum")

        with localcontext() as context:
            context.prec = BASIS_DECIMAL_PRECISION
            expected_half_width = max(
                BASIS_MIN_HALF_WIDTH,
                population_sd * BASIS_VOLATILITY_MULTIPLIER,
            )
            expected_lower = normal - half_width
            expected_upper = normal + half_width
        if half_width != expected_half_width:
            raise ValueError("half_width is inconsistent")
        if lower != expected_lower or upper != expected_upper:
            raise ValueError("basis bounds are inconsistent")


@dataclass(frozen=True)
class BasisBandResult:
    """A raw or softly basis-adjusted two-second projection."""

    projected_chainlink: Decimal
    adjustment: Decimal
    projected_basis_before: Decimal
    projected_basis_after: Decimal
    applied: bool

    def __post_init__(self) -> None:
        projected = _require_decimal(
            self.projected_chainlink,
            "projected_chainlink",
            positive=True,
        )
        adjustment = _require_decimal(self.adjustment, "adjustment")
        before = _require_decimal(
            self.projected_basis_before,
            "projected_basis_before",
        )
        after = _require_decimal(
            self.projected_basis_after,
            "projected_basis_after",
        )
        if not isinstance(self.applied, bool):
            raise TypeError("applied must be a boolean")
        if self.applied != (adjustment != 0):
            raise ValueError("applied is inconsistent with adjustment")

        with localcontext() as context:
            context.prec = BASIS_DECIMAL_PRECISION
            raw_projected = projected - adjustment
            expected_after = before - adjustment
            rounding_tolerance = max(
                Decimal("1e-27"),
                abs(projected) * Decimal("1e-27"),
            )
        if raw_projected <= 0:
            raise ValueError("raw projected Chainlink price must be positive")
        if abs(after - expected_after) > rounding_tolerance:
            raise ValueError("projected_basis_after is inconsistent")


@dataclass(frozen=True)
class _BasisSample:
    bucket_ms: int
    value: Decimal


class RollingBasisBand:
    """Maintain the frozen five-minute, 500 ms rolling basis window."""

    def __init__(self) -> None:
        self._samples: Deque[_BasisSample] = deque()
        self._current_bucket_ms: Optional[int] = None
        self._current_bucket_stats: Optional[BasisBandStats] = None
        self._current_bucket_sampled = False
        self._last_generated_ms: Optional[int] = None

    def reset(self) -> None:
        """Discard history after a clock or source-timestamp discontinuity."""

        self._samples.clear()
        self._current_bucket_ms = None
        self._current_bucket_stats = None
        self._current_bucket_sampled = False
        self._last_generated_ms = None

    def observe(
        self,
        *,
        generated_ms: int,
        basis: Optional[Decimal],
    ) -> BasisBandStats:
        """Return prior-bucket stats and sample this bucket at most once.

        ``basis=None`` records no sample.  A later valid observation in the
        same bucket may still supply that bucket's sample without changing the
        already-cached prior statistics.
        """

        now_ms = _require_non_negative_int(generated_ms, "generated_ms")
        if basis is not None:
            basis = _require_decimal(basis, "basis")

        if self._last_generated_ms is not None and now_ms < self._last_generated_ms:
            self.reset()
        self._last_generated_ms = now_ms

        bucket_ms = (
            now_ms // BASIS_SAMPLE_CADENCE_MS * BASIS_SAMPLE_CADENCE_MS
        )
        if self._current_bucket_ms != bucket_ms:
            self._start_bucket(bucket_ms)

        stats = self._current_bucket_stats
        assert stats is not None
        if basis is not None and not self._current_bucket_sampled:
            self._samples.append(_BasisSample(bucket_ms=bucket_ms, value=basis))
            self._current_bucket_sampled = True
        return stats

    def _start_bucket(self, bucket_ms: int) -> None:
        cutoff_ms = bucket_ms - BASIS_HISTORY_MS
        while self._samples and self._samples[0].bucket_ms < cutoff_ms:
            self._samples.popleft()

        self._current_bucket_ms = bucket_ms
        self._current_bucket_stats = self._calculate_stats()
        self._current_bucket_sampled = False

    def _calculate_stats(self) -> BasisBandStats:
        sample_count = len(self._samples)
        if sample_count < BASIS_MIN_SAMPLES:
            return BasisBandStats(
                sample_count=sample_count,
                normal_basis=None,
                population_sd=None,
                half_width=None,
                lower_basis=None,
                upper_basis=None,
                ready=False,
            )

        try:
            with localcontext() as context:
                context.prec = BASIS_DECIMAL_PRECISION
                count = Decimal(sample_count)
                total = sum(
                    (sample.value for sample in self._samples),
                    Decimal("0"),
                )
                normal = total / count
                squared_deviation_total = sum(
                    (
                        (sample.value - normal) * (sample.value - normal)
                        for sample in self._samples
                    ),
                    Decimal("0"),
                )
                variance = squared_deviation_total / count
                population_sd = variance.sqrt()
                half_width = max(
                    BASIS_MIN_HALF_WIDTH,
                    population_sd * BASIS_VOLATILITY_MULTIPLIER,
                )
                lower = normal - half_width
                upper = normal + half_width
        except (ArithmeticError, DecimalException) as exc:
            raise ValueError("basis statistics arithmetic failed") from exc

        return BasisBandStats(
            sample_count=sample_count,
            normal_basis=normal,
            population_sd=population_sd,
            half_width=half_width,
            lower_basis=lower,
            upper_basis=upper,
            ready=True,
        )


def futures_chainlink_basis(
    *,
    futures_now: Decimal,
    chainlink_now: Decimal,
) -> Decimal:
    """Return the current futures-minus-Chainlink dollar basis."""

    futures = _require_decimal(futures_now, "futures_now", positive=True)
    chainlink = _require_decimal(
        chainlink_now,
        "chainlink_now",
        positive=True,
    )
    with localcontext() as context:
        context.prec = BASIS_DECIMAL_PRECISION
        return futures - chainlink


def apply_basis_band(
    *,
    raw_projected_chainlink: Decimal,
    futures_now: Decimal,
    stats: BasisBandStats,
) -> BasisBandResult:
    """Softly move an out-of-band projected basis toward the nearest edge.

    An incomplete history returns the raw projection.  If an extreme input
    would produce a nonpositive adjusted price, the raw projection is also
    retained as a safe fallback.
    """

    raw_projected = _require_decimal(
        raw_projected_chainlink,
        "raw_projected_chainlink",
        positive=True,
    )
    futures = _require_decimal(futures_now, "futures_now", positive=True)
    if not isinstance(stats, BasisBandStats):
        raise TypeError("stats must be BasisBandStats")

    try:
        with localcontext() as context:
            context.prec = BASIS_DECIMAL_PRECISION
            projected_basis_before = futures - raw_projected
            adjustment = Decimal("0")
            if stats.ready:
                lower = stats.lower_basis
                upper = stats.upper_basis
                assert lower is not None and upper is not None
                if projected_basis_before < lower:
                    adjustment = -BASIS_CORRECTION_STRENGTH * (
                        lower - projected_basis_before
                    )
                elif projected_basis_before > upper:
                    adjustment = BASIS_CORRECTION_STRENGTH * (
                        projected_basis_before - upper
                    )

            projected = raw_projected + adjustment
            if not projected.is_finite() or projected <= 0:
                projected = raw_projected
                adjustment = Decimal("0")
            projected_basis_after = futures - projected
    except (ArithmeticError, DecimalException) as exc:
        raise ValueError("basis adjustment arithmetic failed") from exc

    return BasisBandResult(
        projected_chainlink=projected,
        adjustment=adjustment,
        projected_basis_before=projected_basis_before,
        projected_basis_after=projected_basis_after,
        applied=adjustment != 0,
    )
