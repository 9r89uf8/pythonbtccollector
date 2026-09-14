from dataclasses import asdict, replace
from decimal import Decimal, localcontext
from hashlib import sha256
import json

import pytest

from research.spot_twap_response.freshness_expiry.review import (
    MATCH_NS, M, PriceEvent, current_ok, exact_mean, inventory,
    reference_slots, rows, target_result,
)

D = Decimal


def event(sequence=1, feed='spot', source=100000, received=102000*M,
          value='100.000000000000000001'):
    return PriceEvent(feed, D(value), source, received, received, sequence,
                      f'event-{sequence}', 60 if feed == 'twap' else None)


def test_source_and_both_receipt_clocks_have_separate_inclusive_bounds():
    spot = event()
    wall = mono = 105000*M
    assert current_ok(spot, wall, mono, 5000)
    assert not current_ok(spot, wall, mono, 3000)
    assert not current_ok(spot, wall+1, mono, 5000)
    assert not current_ok(spot, wall, mono+1, 5000)
    future = replace(spot, source_timestamp_ms=103000)
    assert not current_ok(future, wall, mono, 5000)


def test_future_slots_use_saved_current_and_past_slots_never_use_future_source():
    spot = event(source=100000)
    anchor = event(sequence=2, feed='twap', source=103000, received=104000*M)
    inputs = {100000: spot, 106000: event(sequence=3, source=106000, received=106000*M)}
    # The independent lookup receives an admitted prefix. Supply its causal
    # subset here; a later source is not admitted at a 105s decision.
    slots = reference_slots({100000: inputs[100000]}, spot, anchor, 105000*M, 105000*M, 5000)
    by_stamp = {s['slot_timestamp_ms']: s for s in slots}
    assert by_stamp[99000]['value'] is None
    assert by_stamp[100000]['category'] == 'observed'
    assert by_stamp[105000]['category'] == 'pending'
    assert by_stamp[106000]['category'] == 'future'
    assert by_stamp[106000]['input_sequence'] == spot.sequence
    original = reference_slots({100000: spot}, spot, anchor, 105000*M, 105000*M, 3000)
    assert next(s for s in original if s['slot_timestamp_ms'] == 106000)['value'] is None


def test_past_carry_includes_exact_ten_seconds_but_never_more():
    spot = event(source=100000)
    current = event(sequence=2, source=112000, received=112000*M)
    anchor = event(sequence=3, feed='twap', source=110000, received=112000*M)
    slots = reference_slots({100000: spot, 112000: current}, current, anchor,
                            112000*M, 112000*M, 5000)
    by_stamp = {s['slot_timestamp_ms']: s for s in slots}
    assert by_stamp[110000]['value'] == spot.value
    assert by_stamp[110000]['category'] == 'carried'
    assert by_stamp[111000]['value'] is None
    assert by_stamp[111000]['carry_age_ms'] == 11000


def test_exact_mean_preserves_e18_and_rounds_half_even_independently_of_outer_context():
    with localcontext() as context:
        context.prec = 8
        values = [D('76822.000000000000000001')]*30 + [D('76822.000000000000000002')]*30
        assert exact_mean(values) == D('76822.000000000000000002')
    with pytest.raises(ValueError, match='60'):
        exact_mean(values[:-1])


def test_first_target_receipt_is_inclusive_at_decision_and_exclusive_at_120_seconds():
    decision = 102000*M
    first = event(sequence=2, feed='twap', received=decision)
    assert target_result({100000: [first]}, 100000, 1, decision, decision,
                         decision+MATCH_NS)[0] == 'clean_matched'
    late = replace(first, received_wall_ns=decision+MATCH_NS,
                   received_monotonic_ns=decision+MATCH_NS)
    assert target_result({100000: [late]}, 100000, 1, decision, decision,
                         decision+MATCH_NS)[0] == 'missing_target'


def test_incomplete_future_window_is_excluded_even_when_a_first_report_is_known():
    decision = 102000*M
    first = event(sequence=2, feed='twap', received=decision+1)
    assert target_result({100000: [first]}, 100000, 1, decision, decision,
                         decision+MATCH_NS-1)[0] == 'incomplete_target_window'


def test_already_received_and_conflicting_revisions_are_excluded():
    decision = 102000*M
    first = event(sequence=2, feed='twap', received=decision+1)
    conflict = event(sequence=3, feed='twap', received=decision+2, value='101')
    duplicate = replace(conflict, value=first.value)
    args = (100000, 1, decision, decision, decision+MATCH_NS)
    assert target_result({100000: [first, conflict]}, *args)[0] == 'conflicting_target'
    assert target_result({100000: [first, duplicate]}, *args)[0] == 'clean_matched'
    assert target_result({100000: [first]}, 100000, 2, decision, decision,
                         decision+MATCH_NS)[0] == 'already_received'
    after_window = replace(conflict, received_wall_ns=decision+MATCH_NS,
                           received_monotonic_ns=decision+MATCH_NS)
    assert target_result({100000: [first, after_window]}, *args)[0] == 'clean_matched'


def test_future_source_target_is_not_scored():
    decision = 102000*M
    first = event(sequence=2, feed='twap', source=103000, received=decision+1)
    assert target_result({103000: [first]}, 103000, 1, decision, decision,
                         decision+MATCH_NS)[0] == 'target_clock_anomaly'


def make_export(path, events, included):
    frozen = dict(contract_version=2,
                  policy=dict(enabled=True, current_max_age_ms=3000, max_carry_ms=10000,
                              history_ms=120000, max_events=1024),
                  gap_count=0, included_sequence=included, current_spot=None,
                  slot_inputs=[asdict(e) for e in events])
    frozen_text = json.dumps(frozen, default=str)
    state_text = '{}'
    row = dict(terminal=True, run_id='test', decision_id='1', frozen_json=frozen_text,
               state_json=state_text, frozen_sha256=sha256(frozen_text.encode()).hexdigest(),
               state_sha256=sha256(state_text.encode()).hexdigest())
    raw = (json.dumps(row)+'\n').encode()
    path.write_bytes(raw)
    return sha256(raw).hexdigest()


def test_inventory_reports_postdecision_holes_without_inventing_history(tmp_path):
    path = tmp_path/'export.jsonl'
    checksum = make_export(path, [event(1), event(2), event(4)], 2)
    _, info, _ = inventory(path, checksum)
    assert info['complete_sequence_prefix_end'] == 2
    assert info['missing_sequences'] == [3]


def test_inventory_rejects_incomplete_decision_prefix_and_hash_corruption(tmp_path):
    path = tmp_path/'export.jsonl'
    checksum = make_export(path, [event(1), event(3)], 3)
    with pytest.raises(ValueError, match='incomplete accepted input prefix'):
        inventory(path, checksum)
    with pytest.raises(ValueError, match='SHA-256'):
        inventory(path, '0'*64)


def test_inventory_rejects_contradictory_event_versions(tmp_path):
    path = tmp_path/'export.jsonl'
    checksum = make_export(path, [event(1), event(1, value='101')], 1)
    with pytest.raises(ValueError, match='conflicting accepted event sequence'):
        inventory(path, checksum)


def test_numeric_replay_order_is_independent_of_text_identity_export_order(tmp_path):
    path = tmp_path/'export.jsonl'
    make_export(path, [event(1)], 1)
    row = json.loads(path.read_text())
    raw = b''.join((json.dumps(dict(row, decision_id=key))+'\n').encode()
                   for key in sorted(map(str, range(1, 11))))
    path.write_bytes(raw)
    _, _, offsets = inventory(path, sha256(raw).hexdigest())
    assert [r['decision_id'] for r, _, _ in rows(path, offsets)] == list(map(str, range(1, 11)))
