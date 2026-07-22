from decimal import Decimal, localcontext

import pytest

from price_collector.shadow_signal_2s_basis import (
    BASIS_CORRECTION_STRENGTH,
    BASIS_HISTORY_MS,
    BASIS_MIN_HALF_WIDTH,
    BASIS_MIN_SAMPLES,
    BASIS_SAMPLE_CADENCE_MS,
    BASIS_VOLATILITY_MULTIPLIER,
    BasisBandStats,
    RollingBasisBand,
    apply_basis_band,
    futures_chainlink_basis,
)


def populate_window(band, values):
    for index, value in enumerate(values):
        band.observe(
            generated_ms=index * BASIS_SAMPLE_CADENCE_MS,
            basis=Decimal(value),
        )


def ready_stats(
    *,
    normal="20",
    population_sd="0",
    half_width="1",
):
    normal_value = Decimal(normal)
    sd_value = Decimal(population_sd)
    width_value = Decimal(half_width)
    return BasisBandStats(
        sample_count=BASIS_MIN_SAMPLES,
        normal_basis=normal_value,
        population_sd=sd_value,
        half_width=width_value,
        lower_basis=normal_value - width_value,
        upper_basis=normal_value + width_value,
        ready=True,
    )


def test_frozen_basis_configuration_is_exact():
    assert BASIS_HISTORY_MS == 300_000
    assert BASIS_SAMPLE_CADENCE_MS == 500
    assert BASIS_MIN_SAMPLES == 600
    assert BASIS_VOLATILITY_MULTIPLIER == Decimal("0.75")
    assert BASIS_MIN_HALF_WIDTH == Decimal("1")
    assert BASIS_CORRECTION_STRENGTH == Decimal("0.5")


def test_current_bucket_is_excluded_and_same_bucket_cannot_replace_sample():
    band = RollingBasisBand()
    populate_window(band, ["20"] * BASIS_MIN_SAMPLES)

    stats = band.observe(generated_ms=300_000, basis=Decimal("100"))
    repeated = band.observe(generated_ms=300_400, basis=Decimal("-100"))

    assert stats.ready is True
    assert stats.sample_count == 600
    assert stats.normal_basis == Decimal("20")
    assert stats.population_sd == Decimal("0")
    assert repeated == stats

    next_stats = band.observe(generated_ms=300_500, basis=None)
    with localcontext() as context:
        context.prec = 28
        expected_mean = (
            Decimal("20") * Decimal("599") + Decimal("100")
        ) / Decimal("600")
    assert next_stats.normal_basis == expected_mean


def test_later_valid_read_can_fill_an_unsampled_current_bucket_once():
    band = RollingBasisBand()

    first = band.observe(generated_ms=0, basis=None)
    later = band.observe(generated_ms=100, basis=Decimal("20"))
    repeated = band.observe(generated_ms=400, basis=Decimal("999"))

    assert first == later == repeated
    for index in range(1, BASIS_MIN_SAMPLES):
        band.observe(
            generated_ms=index * BASIS_SAMPLE_CADENCE_MS,
            basis=Decimal("20"),
        )
    stats = band.observe(generated_ms=300_000, basis=None)
    assert stats.ready is True
    assert stats.normal_basis == Decimal("20")


def test_missing_bucket_is_not_backfilled_and_disables_complete_window():
    band = RollingBasisBand()
    populate_window(band, ["20"] * BASIS_MIN_SAMPLES)

    ready = band.observe(generated_ms=300_000, basis=None)
    after_missing = band.observe(generated_ms=300_500, basis=Decimal("20"))

    assert ready.ready is True
    assert after_missing.ready is False
    assert after_missing.sample_count == 599
    assert after_missing.normal_basis is None


def test_population_standard_deviation_and_dynamic_band_are_exact():
    band = RollingBasisBand()
    populate_window(band, ["18", "22"] * 300)

    stats = band.observe(generated_ms=300_000, basis=None)

    assert stats.normal_basis == Decimal("20")
    assert stats.population_sd == Decimal("2")
    assert stats.half_width == Decimal("1.50")
    assert stats.lower_basis == Decimal("18.50")
    assert stats.upper_basis == Decimal("21.50")


def test_one_dollar_floor_applies_when_basis_volatility_is_zero():
    band = RollingBasisBand()
    populate_window(band, ["20"] * BASIS_MIN_SAMPLES)

    stats = band.observe(generated_ms=300_000, basis=None)

    assert stats.population_sd == Decimal("0")
    assert stats.half_width == Decimal("1")
    assert stats.lower_basis == Decimal("19")
    assert stats.upper_basis == Decimal("21")


def test_soft_correction_is_symmetric_and_inside_band_is_unchanged():
    stats = ready_stats()

    below = apply_basis_band(
        raw_projected_chainlink=Decimal("90"),
        futures_now=Decimal("100"),
        stats=stats,
    )
    inside = apply_basis_band(
        raw_projected_chainlink=Decimal("80"),
        futures_now=Decimal("100"),
        stats=stats,
    )
    above = apply_basis_band(
        raw_projected_chainlink=Decimal("70"),
        futures_now=Decimal("100"),
        stats=stats,
    )

    assert below.projected_basis_before == Decimal("10")
    assert below.adjustment == Decimal("-4.5")
    assert below.projected_chainlink == Decimal("85.5")
    assert below.projected_basis_after == Decimal("14.5")
    assert below.applied is True

    assert inside.projected_chainlink == Decimal("80")
    assert inside.adjustment == Decimal("0")
    assert inside.projected_basis_before == Decimal("20")
    assert inside.projected_basis_after == Decimal("20")
    assert inside.applied is False

    assert above.projected_basis_before == Decimal("30")
    assert above.adjustment == Decimal("4.5")
    assert above.projected_chainlink == Decimal("74.5")
    assert above.projected_basis_after == Decimal("25.5")
    assert above.applied is True


def test_after_basis_uses_the_adjusted_price_despite_decimal_rounding_order():
    result = apply_basis_band(
        raw_projected_chainlink=Decimal(
            "50017.99760095961615353858457"
        ),
        futures_now=Decimal("50030"),
        stats=ready_stats(),
    )

    with localcontext() as context:
        context.prec = 28
        expected_after = Decimal("50030") - result.projected_chainlink
    assert result.projected_basis_after == expected_after
    assert result.applied is True


def test_warmup_and_nonpositive_adjustment_fall_back_to_raw_projection():
    warming = BasisBandStats(
        sample_count=599,
        normal_basis=None,
        population_sd=None,
        half_width=None,
        lower_basis=None,
        upper_basis=None,
        ready=False,
    )
    raw = apply_basis_band(
        raw_projected_chainlink=Decimal("90"),
        futures_now=Decimal("100"),
        stats=warming,
    )
    extreme = apply_basis_band(
        raw_projected_chainlink=Decimal("1"),
        futures_now=Decimal("1"),
        stats=ready_stats(normal="101"),
    )

    assert raw.projected_chainlink == Decimal("90")
    assert raw.adjustment == Decimal("0")
    assert raw.applied is False
    assert extreme.projected_chainlink == Decimal("1")
    assert extreme.adjustment == Decimal("0")
    assert extreme.applied is False


def test_clock_regression_and_explicit_reset_discard_prior_history():
    band = RollingBasisBand()
    band.observe(generated_ms=1_000, basis=Decimal("10"))
    before_regression = band.observe(
        generated_ms=1_500,
        basis=Decimal("11"),
    )
    after_regression = band.observe(
        generated_ms=1_400,
        basis=Decimal("12"),
    )
    next_bucket = band.observe(generated_ms=1_500, basis=None)

    assert before_regression.sample_count == 1
    assert after_regression.sample_count == 0
    assert next_bucket.sample_count == 1

    band.reset()
    after_reset = band.observe(generated_ms=2_000, basis=None)
    assert after_reset.sample_count == 0


def test_math_is_independent_of_ambient_decimal_precision():
    values = ["19", "20", "24"] * 200

    with localcontext() as context:
        context.prec = 6
        low_precision_band = RollingBasisBand()
        populate_window(low_precision_band, values)
        low_stats = low_precision_band.observe(
            generated_ms=300_000,
            basis=None,
        )
        low_result = apply_basis_band(
            raw_projected_chainlink=Decimal("90.123456789"),
            futures_now=Decimal("100.987654321"),
            stats=low_stats,
        )

    with localcontext() as context:
        context.prec = 50
        high_precision_band = RollingBasisBand()
        populate_window(high_precision_band, values)
        high_stats = high_precision_band.observe(
            generated_ms=300_000,
            basis=None,
        )
        high_result = apply_basis_band(
            raw_projected_chainlink=Decimal("90.123456789"),
            futures_now=Decimal("100.987654321"),
            stats=high_stats,
        )

    assert low_stats == high_stats
    assert low_result == high_result


def test_current_basis_math_is_decimal_only_and_context_independent():
    with localcontext() as context:
        context.prec = 5
        low_precision = futures_chainlink_basis(
            futures_now=Decimal("60020.123456789"),
            chainlink_now=Decimal("60000.000000001"),
        )
    with localcontext() as context:
        context.prec = 50
        high_precision = futures_chainlink_basis(
            futures_now=Decimal("60020.123456789"),
            chainlink_now=Decimal("60000.000000001"),
        )

    assert low_precision == high_precision == Decimal("20.123456788")
    assert isinstance(low_precision, Decimal)


@pytest.mark.parametrize(
    ("generated_ms", "basis", "error"),
    (
        (True, None, TypeError),
        (-1, None, ValueError),
        (0, "20", TypeError),
        (0, Decimal("NaN"), ValueError),
    ),
)
def test_observe_rejects_invalid_inputs(generated_ms, basis, error):
    with pytest.raises(error):
        RollingBasisBand().observe(generated_ms=generated_ms, basis=basis)


def test_projection_and_stats_types_are_validated():
    stats = ready_stats()
    with pytest.raises(TypeError, match="raw_projected_chainlink"):
        apply_basis_band(
            raw_projected_chainlink=90.0,
            futures_now=Decimal("100"),
            stats=stats,
        )
    with pytest.raises(ValueError, match="positive"):
        apply_basis_band(
            raw_projected_chainlink=Decimal("0"),
            futures_now=Decimal("100"),
            stats=stats,
        )
    with pytest.raises(TypeError, match="stats"):
        apply_basis_band(
            raw_projected_chainlink=Decimal("90"),
            futures_now=Decimal("100"),
            stats=None,
        )
