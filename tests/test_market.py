import pytest

from price_collector.market import market_for_sample_second


def test_market_boundary_exact_boundary_belongs_to_new_market():
    window = market_for_sample_second(300_000)

    assert window.market_id == 1
    assert window.market_start_ms == 300_000
    assert window.market_end_ms == 600_000


def test_market_boundary_one_second_before_belongs_to_previous_market():
    window = market_for_sample_second(299_000)

    assert window.market_id == 0
    assert window.market_start_ms == 0
    assert window.market_end_ms == 300_000


def test_market_rejects_non_second_aligned_sample():
    with pytest.raises(ValueError, match="sample_second_ms must be floored"):
        market_for_sample_second(300_001)

