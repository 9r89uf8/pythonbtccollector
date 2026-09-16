from decimal import Decimal

from research.spot_twap_response.reliability_canary.browser_cohorts import coverage, integer_ceil, spans
from research.spot_twap_response.reliability_canary.independent_browser_check import distribution


def test_missing_bins_stay_unknown_and_uncalibrated_masks_do_not_count_as_usable():
    probes = {0: dict(calibrated=True, usable_horizons=[1, 5]),
              1: dict(calibrated=False, usable_horizons=[]),
              4: dict(calibrated=True, usable_horizons=[5])}
    result = coverage(probes, range(6))
    assert result['planned_bins'] == 6
    assert result['observed_bins'] == 3
    assert result['missing_bins'] == 3
    assert result['maximum_missing_streak_bins'] == 2
    assert [(r['first_index'], r['last_index']) for r in result['missing_intervals']] == [(2, 3), (5, 5)]
    first, five = result['by_horizon'][0], result['by_horizon'][3]
    assert (first['usable_bins'], first['not_usable_bins'], first['unknown_bins']) == (1, 1, 4)
    assert (five['usable_bins'], five['not_usable_bins'], five['unknown_bins']) == (2, 0, 4)
    assert five['usable_fraction'] == Decimal(2)/6


def test_fractional_first_receipt_cutoff_rounds_forward_at_both_boundaries():
    exact = Decimal('65000.000000001')
    integer_ms = integer_ceil(exact)
    assert integer_ms == 65001
    first_bin = (integer_ms+99)//100
    assert first_bin == 651
    assert (first_bin-1)*100 < exact <= first_bin*100


def test_noncontiguous_campaign_panel_does_not_fill_outside_or_missing_bins():
    result = coverage({1: dict(calibrated=True, usable_horizons=[30])}, [1, 3])
    assert result['planned_bins'] == 2 and result['missing_bins'] == 1
    assert result['by_horizon'][-1]['usable_fraction'] == Decimal('.5')
    assert spans([]) == []


def test_independent_quantiles_preserve_decimal_prices_and_interpolate_without_float():
    values = [Decimal('100000.000000000000000003'),Decimal('100000.000000000000000001')]
    result = distribution(values)
    assert result['median'] == Decimal('100000.000000000000000002')
    assert result['p90'] == Decimal('100000.0000000000000000028')
    assert result['maximum'] == values[0]
    assert distribution([])['median'] is None
