from decimal import Decimal

import pytest

from research.spot_twap_response.reliability_review_corrections.feed_gap_review import (
    deadline_components, sequence_bridge, storage_arithmetic, utc_ns,
)


def test_utc_preserves_nanoseconds_without_float():
    assert utc_ns(1789513080195834567) == '2026-09-15T22:58:00.195834567Z'


def test_receipt_and_source_expiry_are_distinguished():
    event = {'source_timestamp_ms': 1000, 'received_wall_ns': '2000000000', 'received_monotonic_ns': '1000000000'}
    payload = {'decision_wall_ns': '2500000000', 'decision_monotonic_ns': '1500000000',
               'policy': {'source_max_age_ms': 5000, 'receipt_max_age_ms': 3000},
               'current_spot': event, 'current_twap': event, 'reasons': [], 'valid_until_wall_ns': '5000000000'}
    result = deadline_components(payload)
    assert result['limiting_clocks'] == ['spot_receipt_wall', 'spot_receipt_monotonic', 'twap_receipt_wall', 'twap_receipt_monotonic']
    payload['current_twap'] = dict(event, source_timestamp_ms=-1000)
    payload['valid_until_wall_ns'] = '4000000000'
    assert deadline_components(payload)['limiting_clocks'] == ['twap_source']


def test_incomplete_sequence_bridge_rejects_silence_claim():
    with pytest.raises(ValueError, match='incomplete'):
        sequence_bridge({1: {}, 3: {}}, 1, 3)


def test_capacity_extrapolation_does_not_replace_one_hour_cap():
    result = storage_arithmetic(218710016, 309420032, 1610612736)
    assert result['one_canary_relation_growth_bytes'] == 90710016
    assert result['remaining_relation_bytes'] == 1301192704
    assert Decimal('14.34') < Decimal(result['hypothetical_additional_hours_at_same_growth']) < Decimal('14.35')
    assert result['observed_canary_hours'] == 1
