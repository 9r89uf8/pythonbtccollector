"""Native v6 synthetic evidence, independent arithmetic and corruption cases."""
import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import runpy

import pytest

from research.spot_twap_response.reliability_canary import analyze as scorer
from research.spot_twap_response.reliability_canary.join_observer import target_at_read_end, validate_entry

ROOT = Path(__file__).resolve().parents[3]
HELPERS = runpy.run_path(str(ROOT / 'tests/test_ghost_twap_runtime_faults.py'))


def encode(row, frozen=None, state=None):
    row = deepcopy(row)
    for name, value in (('frozen', frozen), ('state', state)):
        if value is not None:
            row[name + '_json'] = json.dumps(value, separators=(',', ':'), sort_keys=True)
        row[name + '_sha256'] = sha256(row[name + '_json'].encode()).hexdigest()
    return row


def make_row(filtered=False, tiny=False):
    async def build():
        value, clock, _, _, _ = HELPERS['runtime']()
        if tiny:
            offer = value.offer_price
            def priced(feed, amount, *args, **kwargs):
                return offer(feed, Decimal('100.000000000000000001') if feed == 'spot' else amount,
                             *args, **kwargs)
            value.offer_price = priced
        HELPERS['warm'](value)
        row = value.issue()
        start = clock.mono
        if filtered:
            clock.advance(scorer.SECOND)
            value.observe_target(replace(HELPERS['target'](value, row, clock, horizon=1), sequence=1001))
        await value.publish(row)
        for h in scorer.HORIZONS:
            if filtered and h == 1:
                continue
            clock.advance(start + h * scorer.SECOND - clock.mono)
            value.observe_target(replace(HELPERS['target'](value, row, clock, horizon=h,
                price='100' if tiny else '101', identity='target-' + str(h)), sequence=1000+h))
        clock.advance(start + 120 * scorer.SECOND - clock.mono)
        value.finalize_due()
        return encode(row.record())
    return asyncio.run(build())


def run(tmp_path, rows, *, stop=None, digest=None):
    path = tmp_path / 'audit.jsonl'
    raw = b''.join((json.dumps(row, separators=(',', ':')) + '\n').encode() for row in rows)
    path.write_bytes(raw)
    frozen = json.loads(rows[0]['frozen_json'])
    return scorer.analyze(path, run_id=rows[0]['run_id'], campaign_start_ms=HELPERS['BASE'],
        stop_request_wall_ns=stop or int(frozen['decision_wall_ns']) + 120 * scorer.SECOND,
        expected_sha256=digest or sha256(raw).hexdigest(), expected_rows=len(rows))


def test_native_v6_filtered_membership_denominators_and_exact_complete_cutoff(tmp_path):
    row = make_row(filtered=True)
    summary, index = run(tmp_path, [row])
    assert summary['contract_version'] == 4
    h1, h2 = summary['panels']['complete_120s_window']['horizons'][:2]
    assert h1['errors']['calculated_matched_valid']['n'] == 1
    assert h1['errors']['acknowledged_matched_valid']['n'] == 0
    assert h2['counts']['confirmed_early'] == 1
    assert h2['confirmed_lead_ms']['p50'] == '1000.0'
    assert summary['panels']['tail_under_120s']['counts'].get('selected_rows', 0) == 0
    entry, = index['payloads'].values()
    assert 1 not in entry['eligible_horizons'] and entry['forecasts']['1']['price'] is not None
    decision = int(json.loads(row['frozen_json'])['decision_wall_ns'])
    tail, _ = run(tmp_path, [row], stop=decision + 120 * scorer.SECOND - 1)
    assert tail['panels']['complete_120s_window']['counts'].get('selected_rows', 0) == 0
    assert tail['panels']['tail_under_120s']['counts']['selected_rows'] == 1


def test_e18_error_and_basis_points_are_not_float_rounded(tmp_path):
    summary, _ = run(tmp_path, [make_row(tiny=True)])
    for h in summary['panels']['all_decisions']['horizons']:
        error = h['errors']['acknowledged_matched_valid']
        assert Decimal(error['ghost_absolute_error']['p50']) == Decimal('1e-18')
        assert Decimal(error['ghost_absolute_error_bps']['p50']) == Decimal('1e-16')
        assert Decimal(error['persistence_absolute_error']['p50']) == 0


@pytest.mark.parametrize('field', ['price', 'target_source_timestamp_ms', 'quality'])
def test_attempt_membership_requires_exact_original_forecast(tmp_path, field):
    row = make_row()
    state = json.loads(row['state_json'])
    payload = json.loads(state['publication']['payload_json'])
    payload['forecasts'][0][field] = {'price': '99.000000000000000000',
        'target_source_timestamp_ms': 0, 'quality': 'unavailable'}[field]
    state['publication']['payload_json'] = json.dumps(payload)
    with pytest.raises(ValueError, match='Eligible forecast differs'):
        run(tmp_path, [encode(row, state=state)])


def test_target_at_ack_is_matched_but_not_confirmed_early(tmp_path):
    row = make_row()
    state = json.loads(row['state_json'])
    first = state['targets']['1']['first_event']
    state['publication']['ack_wall_ns'] = first['received_wall_ns']
    state['publication']['ack_monotonic_ns'] = first['received_monotonic_ns']
    for target in state['targets'].values():
        lead = int(target['first_event']['received_monotonic_ns']) - int(first['received_monotonic_ns'])
        target['confirmed_redis_lead_ns'] = str(lead) if lead > 0 else None
    result, _ = run(tmp_path, [encode(row, state=state)])
    h = result['panels']['all_decisions']['horizons'][0]
    assert h['errors']['acknowledged_matched_valid']['n'] == 1
    assert h['errors']['confirmed_early_matched_valid']['n'] == 0


def test_export_integrity_and_invalid_reconnect_are_rejected(tmp_path):
    row = make_row()
    with pytest.raises(ValueError, match='SHA mismatch'):
        run(tmp_path, [row], digest='0' * 64)
    with pytest.raises(ValueError, match='Duplicate exported'):
        run(tmp_path, [row, row])
    frozen = json.loads(row['frozen_json'])
    frozen['spot_reconnect'] = {'status': 'retained'}
    with pytest.raises(ValueError, match='Reconnect field contract'):
        run(tmp_path, [encode(row, frozen=frozen)])


@pytest.mark.parametrize('clock', ['wall', 'monotonic'])
def test_actual_publication_must_have_at_least_one_ms_on_both_deadlines(clock):
    row = make_row()
    frozen, state = json.loads(row['frozen_json']), json.loads(row['state_json'])
    publication = state['publication']
    payload = json.loads(publication['payload_json'])
    deadline = int(frozen['valid_until_wall_ns'])
    if clock == 'monotonic':
        deadline += int(frozen['decision_monotonic_ns']) - int(frozen['decision_wall_ns'])
    too_late = str(deadline - scorer.MS + 1)
    publication['attempt_' + clock + '_ns'] = too_late
    publication['ack_' + clock + '_ns'] = too_late
    payload['publication_attempt_' + clock + '_ns'] = too_late
    payload['publication_eligibility']['checked_' + clock + '_ns'] = too_late
    publication['payload_json'] = json.dumps(payload)
    with pytest.raises(ValueError, match='after usable expiry'):
        scorer.evidence.verify_publication(frozen, state)


def test_observer_read_end_requires_later_exact_target_not_just_fresh_payload():
    first = {'received_wall_ns': '200', 'received_monotonic_ns': '200'}
    forecast = {'target_source_timestamp_ms': 1000, 'first_event': first,
                'target_status': 'matched', 'target_conflicted': False, 'target_clock_anomaly': False}
    entry = {'causality_invalid': False, 'forecasts': {'5': forecast}}
    known = {'1000': {'earliest': first, 'distinct_values': 1}}
    assert target_at_read_end(entry, 5, known, 199, 199) == 'recorded_first_target_later_than_read_end'
    assert target_at_read_end(entry, 5, known, 200, 200) == 'already_received_by_read_end'
    assert target_at_read_end(entry, 5, known, 199, 200) == 'clock_order_ambiguous'
    known['1000']['earliest'] = {'received_wall_ns': '150', 'received_monotonic_ns': '150'}
    assert target_at_read_end(entry, 5, known, 180, 180) == 'already_received_by_read_end'
    known['1000']['distinct_values'] = 2
    assert target_at_read_end(entry, 5, known, 100, 100) == 'invalid_target_evidence'
    forecast['first_event'] = None
    assert target_at_read_end(entry, 5, {}, 100, 100) == 'target_receipt_unknown'


def test_observer_exact_attempt_mask_cannot_be_replaced_by_calculated_availability(tmp_path):
    row = make_row(filtered=True)
    _, index = run(tmp_path, [row])
    entry, = index['payloads'].values()
    raw = json.loads(row['state_json'])['publication']['payload_json'].encode()
    validate_entry(raw, entry)
    altered = deepcopy(entry)
    altered['eligible_horizons'] = [1, 2, 3, 5, 10, 30]
    with pytest.raises(ValueError, match='mask mismatch'):
        validate_entry(raw, altered)
