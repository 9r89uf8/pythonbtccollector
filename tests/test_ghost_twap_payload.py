"""Independent delivery validation; engine-generated fixtures live only in tests."""
from dataclasses import FrozenInstanceError
from decimal import Decimal
import json

import pytest

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent
from price_collector.ghost_twap_payload import (
    MAX_PAYLOAD_BYTES, InvalidGhostPayload, bind_read_clock, parse_ghost_payload,
)

BASE_MS = 1_800_000_000_000
NS = 1_000_000
PRODUCER_MONO_NS = 900_000_000_000
DECISION_WALL_NS = (BASE_MS + 1000) * NS
DECISION_MONO_NS = PRODUCER_MONO_NS + 1000 * NS
READ_WALL_NS = DECISION_WALL_NS + 100 * NS
API_MONO_NS = 17  # Deliberately unrelated to the producer's monotonic origin.


def wire_payload(*, no_inputs=False, missing_history=False, reconnect=False):
    engine = GhostTwapEngine('wire-run', GhostPolicy(enabled=True))
    sequence = 0
    if not no_inputs:
        for second in ([0] if missing_history else range(-65, 1)):
            sequence += 1
            source = BASE_MS + second * 1000
            engine.accept(PriceEvent('spot', Decimal('100.000000000000000001'), source,
                source * NS, PRODUCER_MONO_NS + second * 1000 * NS, sequence, 'spot-' + str(sequence)))
        sequence += 1
        engine.accept(PriceEvent('twap', Decimal('100'), BASE_MS, (BASE_MS + 500) * NS,
            PRODUCER_MONO_NS + 500 * NS, sequence, 'anchor-event', 60))
        if reconnect:
            engine.record_gap('spot', 'connection_end', observed_wall_ns=(BASE_MS+600)*NS,
                              observed_monotonic_ns=PRODUCER_MONO_NS+600*NS)
            sequence += 1
            engine.accept(PriceEvent('spot', Decimal('101'), BASE_MS+1000, DECISION_WALL_NS,
                DECISION_MONO_NS, sequence, 'resumed-spot'))
    decision = engine.snapshot('1', DECISION_WALL_NS, DECISION_MONO_NS)
    body = json.loads(decision.to_live_json())
    for f in body['forecasts']:
        start = f['slot_start_index']
        chosen = [] if start is None else decision.slots[start:start+60]
        f['max_interior_carry_ms'] = max((s.carry_age_ms for s in chosen if s.category == 'carried'), default=0)
    eligible = [f['horizon_s'] for f in body['forecasts'] if f['price'] is not None]
    body.update(runtime_version='ghost-canary-v6', publication_sequence=1, publication_state='attempted',
        audit_state='durable_outbox_postgres_pending',
        computation_completed_wall_ns=str(DECISION_WALL_NS+10),
        computation_completed_monotonic_ns=str(DECISION_MONO_NS+10),
        publication_intent_wall_ns=str(DECISION_WALL_NS+20),
        publication_intent_monotonic_ns=str(DECISION_MONO_NS+20),
        publication_attempt_wall_ns=str(DECISION_WALL_NS+30),
        publication_attempt_monotonic_ns=str(DECISION_MONO_NS+30),
        publication_eligibility=dict(version=1, checked_wall_ns=str(DECISION_WALL_NS+30),
            checked_monotonic_ns=str(DECISION_MONO_NS+30), eligible_horizons=eligible,
            excluded_horizons={str(f['horizon_s']): f['reasons'] for f in body['forecasts'] if f['price'] is None}))
    return body


def raw_payload(body=None, **kwargs):
    return json.dumps(wire_payload(**kwargs) if body is None else body,
                      sort_keys=True, separators=(',', ':')).encode()


def test_actual_v6_wire_is_immutable_and_returns_original_bytes():
    raw = raw_payload()
    parsed = parse_ghost_payload(raw)
    assert parsed.raw is raw
    assert parsed.run_id == 'wire-run' and parsed.decision_id == 1
    assert parsed.anchor_value == '100.000000000000000000'
    assert parsed.anchor_source_timestamp_ms == BASE_MS
    assert parsed.anchor_received_wall_ns == (BASE_MS+500)*NS
    assert parsed.current_twap.event_id == 'anchor-event'
    assert parsed.current_spot.value == '100.000000000000000001'
    assert parsed.eligible_horizons == (1, 2, 3, 5, 10, 30)
    assert parsed.forecasts[0].price == '100.000000000000000001'
    with pytest.raises(FrozenInstanceError):
        parsed.run_id = 'changed'
    with pytest.raises(FrozenInstanceError):
        parsed.forecasts[0].counts.observed = 0
    with pytest.raises(FrozenInstanceError):
        parsed.current_twap.value = '0'


def test_api_monotonic_origin_is_independent_and_deadline_does_not_refresh():
    parsed = parse_ghost_payload(raw_payload())
    read = bind_read_clock(parsed, wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS)
    assert read.payload is parsed
    assert read.api_deadline_monotonic_ns == API_MONO_NS + parsed.valid_until_wall_ns - READ_WALL_NS
    assert read.is_fresh(wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS)
    assert read.remaining_ns(wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS) == 1900*NS
    assert not read.is_fresh(wall_ns=READ_WALL_NS, monotonic_ns=read.api_deadline_monotonic_ns)
    assert not read.is_fresh(wall_ns=parsed.valid_until_wall_ns, monotonic_ns=API_MONO_NS)
    assert read.is_fresh(wall_ns=parsed.valid_until_wall_ns-1, monotonic_ns=read.api_deadline_monotonic_ns-1)
    assert not read.is_fresh(wall_ns=READ_WALL_NS-1, monotonic_ns=API_MONO_NS)
    assert not read.is_fresh(wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS-1)


def test_api_read_rejects_future_producer_but_accepts_expired_as_unavailable():
    parsed = parse_ghost_payload(raw_payload())
    with pytest.raises(InvalidGhostPayload, match='future'):
        bind_read_clock(parsed, wall_ns=parsed.publication_attempt_wall_ns-1, monotonic_ns=0)
    read = bind_read_clock(parsed, wall_ns=parsed.valid_until_wall_ns+1, monotonic_ns=0)
    assert read.api_deadline_monotonic_ns == 0
    assert not read.is_fresh(wall_ns=parsed.valid_until_wall_ns+1, monotonic_ns=0)


def test_all_null_health_payloads_allow_missing_inputs_and_preserve_counts():
    parsed = parse_ghost_payload(raw_payload(no_inputs=True))
    assert not parsed.has_eligible_prices and parsed.current_spot is parsed.current_twap is None
    assert all(f.counts.missing == 60 and f.price is None for f in parsed.forecasts)
    read = bind_read_clock(parsed, wall_ns=READ_WALL_NS, monotonic_ns=0)
    assert not read.is_fresh(wall_ns=READ_WALL_NS, monotonic_ns=0)
    warming = parse_ghost_payload(raw_payload(missing_history=True))
    assert not warming.has_eligible_prices
    assert warming.current_twap is not None and warming.current_spot is not None


def test_reconnect_payload_accepts_bounded_saved_evidence_without_price_recalculation():
    body = wire_payload(reconnect=True)
    assert body['spot_reconnect']['status'] == 'retained'
    assert parse_ghost_payload(raw_payload(body)).current_spot.event_id == 'resumed-spot'


@pytest.mark.parametrize('value', [100, 100.0, 'NaN', 'Infinity', '0.000000000000000000',
    '-1.000000000000000000', '100', '1e2', '100.0000000000000000001'])
def test_price_format_is_strict_e18(value):
    body = wire_payload()
    body['forecasts'][0]['price'] = value
    with pytest.raises(InvalidGhostPayload):
        parse_ghost_payload(raw_payload(body))


@pytest.mark.parametrize('mutation', [
    lambda p: p.update(runtime_version='ghost-canary-v5'),
    lambda p: p.update(contract_version=3),
    lambda p: p.update(contract_version=4.0),
    lambda p: p.update(decision_id='01'),
    lambda p: p.update(decision_id=1),
    lambda p: p.update(run_id='run\r\npoison'),
    lambda p: p.update(publication_sequence=2),
    lambda p: p.update(publication_state='intent'),
    lambda p: p['forecasts'][0].update(horizon_s=True),
    lambda p: p['forecasts'][0].update(horizon_s=2),
    lambda p: p['forecasts'][0].update(slot_start_index=False),
    lambda p: p['forecasts'][0]['counts'].update(observed=True),
    lambda p: p['forecasts'][0]['counts'].update(observed=59),
    lambda p: p['forecasts'][0].update(quality='degraded'),
    lambda p: p['forecasts'][0].update(target_source_timestamp_ms=BASE_MS+2000),
    lambda p: p['forecasts'][0].update(estimated_arrival_wall_ns='0'),
    lambda p: p['forecasts'][0].update(estimated_remaining_ns='0'),
    lambda p: p['publication_eligibility']['eligible_horizons'].append(1),
    lambda p: p['publication_eligibility'].update(excluded_horizons={'1': ['masked']}),
    lambda p: p['publication_eligibility'].update(checked_monotonic_ns=str(DECISION_MONO_NS+29)),
    lambda p: p.update(publication_intent_monotonic_ns=str(DECISION_MONO_NS-1)),
    lambda p: p['current_spot'].update(source_timestamp_ms=BASE_MS+2000),
    lambda p: p['current_spot'].update(received_wall_ns=str(DECISION_WALL_NS+1)),
    lambda p: p['current_spot'].update(received_monotonic_ns=str(DECISION_MONO_NS+1)),
    lambda p: p['current_twap'].update(window_s=30),
    lambda p: p['policy'].update(receipt_max_age_ms=3001),
])
def test_malformed_identity_clocks_membership_and_quality_fail_closed(mutation):
    body = wire_payload()
    mutation(body)
    with pytest.raises(InvalidGhostPayload):
        parse_ghost_payload(raw_payload(body))


def test_partial_membership_nulls_only_excluded_horizon():
    body = wire_payload()
    body['forecasts'][0].update(price=None, quality='unavailable', reasons=['target_received_before_publication'])
    body['publication_eligibility']['eligible_horizons'].remove(1)
    body['publication_eligibility']['excluded_horizons']['1'] = ['target_received_before_publication']
    parsed = parse_ghost_payload(raw_payload(body))
    assert parsed.eligible_horizons == (2, 3, 5, 10, 30)
    assert parsed.forecasts[0].price is None
    body['forecasts'][0]['price'] = '100.000000000000000000'
    with pytest.raises(InvalidGhostPayload):
        parse_ghost_payload(raw_payload(body))


def test_expiry_cannot_exceed_input_deadlines_but_can_be_shorter():
    body = wire_payload()
    original = int(body['valid_until_wall_ns'])
    body['valid_until_wall_ns'] = str(original+1)
    body['valid_until_ms'] = (original+1)//NS
    with pytest.raises(InvalidGhostPayload, match='deadline'):
        parse_ghost_payload(raw_payload(body))
    body['valid_until_wall_ns'] = str(original-1)
    body['valid_until_ms'] = (original-1)//NS
    assert parse_ghost_payload(raw_payload(body)).valid_until_wall_ns == original-1


def test_producer_monotonic_elapsed_deadline_is_checked_without_api_clock_comparison():
    body = wire_payload()
    body['current_spot']['received_monotonic_ns'] = str(PRODUCER_MONO_NS-500*NS)
    # Producer's elapsed receive age gives an earlier deadline than wall receipt.
    with pytest.raises(InvalidGhostPayload, match='deadline'):
        parse_ghost_payload(raw_payload(body))
    body['valid_until_wall_ns'] = str((BASE_MS+2500)*NS)
    body['valid_until_ms'] = BASE_MS+2500
    parsed = parse_ghost_payload(raw_payload(body))
    read = bind_read_clock(parsed, wall_ns=READ_WALL_NS, monotonic_ns=1)
    assert read.api_deadline_monotonic_ns == 1 + 1400*NS


def test_producer_elapsed_before_attempt_shortens_api_deadline_without_rewriting_wire():
    body = wire_payload()
    # Wall moved 30 ns, while the producer's monotonic clock moved 500 ms.
    body['publication_attempt_monotonic_ns'] = str(DECISION_MONO_NS+500*NS)
    body['publication_eligibility']['checked_monotonic_ns'] = body['publication_attempt_monotonic_ns']
    raw = raw_payload(body)
    parsed = parse_ghost_payload(raw)
    read = bind_read_clock(parsed, wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS)
    assert parsed.raw is raw
    expected = 1400*NS+30
    assert read.remaining_ns(wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS) == expected
    assert not read.is_fresh(wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS+expected)
    body['publication_attempt_monotonic_ns'] = str(DECISION_MONO_NS+2000*NS)
    body['publication_eligibility']['checked_monotonic_ns'] = body['publication_attempt_monotonic_ns']
    with pytest.raises(InvalidGhostPayload, match='already expired'):
        parse_ghost_payload(raw_payload(body))


def test_json_size_duplicate_keys_and_depth_are_bounded():
    with pytest.raises(InvalidGhostPayload):
        parse_ghost_payload(b'x'*(MAX_PAYLOAD_BYTES+1))
    with pytest.raises(InvalidGhostPayload, match='duplicate'):
        parse_ghost_payload(b'{"run_id":"a","run_id":"b"}')
    with pytest.raises(InvalidGhostPayload):
        parse_ghost_payload(b'['*1500+b'0'+b']'*1500)
    with pytest.raises(InvalidGhostPayload):
        parse_ghost_payload(bytearray(raw_payload()))
    raw = raw_payload()
    padded = b' '*(MAX_PAYLOAD_BYTES-len(raw))+raw
    assert len(padded) == MAX_PAYLOAD_BYTES
    assert parse_ghost_payload(padded).raw is padded


def test_no_forecast_calculation_or_mutable_decoded_object_is_exposed():
    body = wire_payload()
    body['forecasts'][0]['price'] = '99.000000000000000000'
    parsed = parse_ghost_payload(raw_payload(body))
    # Structural delivery validation deliberately cannot verify a 60-slot mean
    # because its constituent evidence is absent from the live wire contract.
    assert parsed.forecasts[0].price == '99.000000000000000000'
    assert not hasattr(parsed, 'decoded')
