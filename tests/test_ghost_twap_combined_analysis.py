"""Focused offline evidence checks; fixture generation may use fake runtime I/O.

The analyzer itself imports neither the runtime nor its arithmetic helpers.
"""
import asyncio
from copy import deepcopy
from decimal import Decimal, localcontext
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import runpy

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('combined_analysis', ROOT / 'research/spot_twap_response/combined_canary/analyze.py')
analysis = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(analysis)
HELPERS = runpy.run_path(str(ROOT / 'tests/test_ghost_twap_runtime_faults.py'))


def encode(row, frozen=None, state=None):
    row = deepcopy(row)
    if frozen is not None:
        row['frozen_json'] = json.dumps(frozen, sort_keys=True, separators=(',', ':'))
    if state is not None:
        row['state_json'] = json.dumps(state, sort_keys=True, separators=(',', ':'))
    for name in ('frozen', 'state'):
        row[name + '_sha256'] = sha256(row[name + '_json'].encode()).hexdigest()
    return row


def make_row(*, filtered=False, uncertain=False, unavailable=False, tiny=False):
    async def scenario():
        value, clock, spool, store, redis = HELPERS['runtime']()
        if tiny:
            offer = value.offer_price
            def priced(feed, amount, *args, **kwargs):
                return offer(feed, Decimal('100.000000000000000001') if feed == 'spot' else amount, *args, **kwargs)
            value.offer_price = priced
        if unavailable:
            value.offer_price('spot', Decimal('100'), HELPERS['BASE'] + 100000, clock.wall, clock.mono, 'only-spot')
            value.offer_price('twap', Decimal('100'), HELPERS['BASE'] + 100000, clock.wall, clock.mono, 'anchor', 60)
        else:
            HELPERS['warm'](value)
        row = value.issue()
        start = clock.mono
        if filtered:
            clock.advance(analysis.SECOND)
            value.observe_target(HELPERS['target'](value, row, clock, horizon=1))
        if uncertain:
            redis.failure = TimeoutError('reply lost')
        await value.publish(row)
        if not unavailable:
            for h in analysis.HORIZONS:
                if filtered and h == 1:
                    continue
                clock.advance(start + h * analysis.SECOND - clock.mono)
                value.observe_target(HELPERS['target'](value, row, clock, horizon=h,
                    price='100' if tiny else '101', identity='target-' + str(h)))
        clock.advance(start + 120 * analysis.SECOND - clock.mono)
        value.finalize_due()
        return encode(row.record())
    return asyncio.run(scenario())


def export(tmp_path, rows):
    raw = b''.join((json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n').encode() for row in rows)
    path = tmp_path / 'audit.jsonl'
    path.write_bytes(raw)
    return path, sha256(raw).hexdigest(), len(rows)


def audit(tmp_path, rows):
    path, digest, count = export(tmp_path, rows)
    return analysis.analyze(path, HELPERS['BASE'], digest, count)


def test_filtered_published_and_early_denominators_are_distinct(tmp_path):
    result = audit(tmp_path, [make_row(filtered=True)])
    h1, h2 = result['horizons'][:2]
    assert h1['counts']['calculated'] == 1
    assert h1['counts']['acknowledged_eligible'] == 0
    assert h1['errors']['calculated_matched_valid']['n'] == 1
    assert h1['errors']['acknowledged_matched_valid']['n'] == 0
    assert h2['counts']['acknowledged_eligible'] == 1
    assert h2['counts']['confirmed_early'] == 1
    assert h2['confirmed_lead_ms']['p50'] == '1000.0'
    assert result['withheld_forecast_reasons'] == {'target_received_before_publication': 1}


def test_other_campaign_is_excluded_only_after_envelope_hash_validation(tmp_path):
    selected = make_row()
    old = deepcopy(selected)
    frozen = json.loads(old['frozen_json'])
    frozen['runtime_policy']['canary_start_ms'] -= analysis.HOUR_MS
    frozen['runtime_version'], frozen['contract_version'] = 'ghost-canary-v3', 2
    old['run_id'] = frozen['run_id'] = 'other-run'
    old = encode(old, frozen=frozen)
    result = audit(tmp_path, [old, selected])
    assert result['counts']['selected_rows'] == 1
    assert result['counts']['other_campaign_rows_excluded'] == 1
    old['state_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='Per-row state hash'):
        audit(tmp_path, [old, selected])


def test_multiple_run_ids_and_text_id_order_do_not_merge_rows(tmp_path):
    first, second = make_row(), make_row()
    assert first['decision_id'] == second['decision_id']
    result = audit(tmp_path, [second, first])
    assert len(result['run_rows']) == 2 and result['counts']['selected_rows'] == 2
    with pytest.raises(ValueError, match='Duplicate exported decision'):
        audit(tmp_path, [first, first])


@pytest.mark.parametrize('mutation', ['missing', 'version', 'eligible_id', 'duplicate', 'unpartitioned', 'price', 'target', 'clock', 'quality'])
def test_bad_v5_masks_fail_closed(tmp_path, mutation):
    row = make_row()
    state = json.loads(row['state_json'])
    payload = json.loads(state['publication']['payload_json'])
    mask = payload['publication_eligibility']
    if mutation == 'missing':
        payload.pop('publication_eligibility')
    elif mutation == 'version':
        mask['version'] = 99
    elif mutation == 'eligible_id':
        mask['eligible_horizons'][0] = True
    elif mutation == 'duplicate':
        mask['eligible_horizons'].append(1)
    elif mutation == 'unpartitioned':
        mask['eligible_horizons'].remove(1)
    elif mutation == 'price':
        payload['forecasts'][0]['price'] = '99.000000000000000000'
    elif mutation == 'target':
        payload['forecasts'][0]['target_source_timestamp_ms'] += 1000
    elif mutation == 'clock':
        mask['checked_monotonic_ns'] = str(int(mask['checked_monotonic_ns']) + 1)
    elif mutation == 'quality':
        payload['forecasts'][0]['quality'] = 'unavailable'
    state['publication']['payload_json'] = json.dumps(payload)
    with pytest.raises(ValueError):
        audit(tmp_path, [encode(row, state=state)])


def test_false_withheld_credit_is_rejected(tmp_path):
    row = make_row(filtered=True)
    state = json.loads(row['state_json'])
    state['targets']['1']['confirmed_redis_lead_ns'] = '1'
    with pytest.raises(ValueError, match='confirmed lead credit'):
        audit(tmp_path, [encode(row, state=state)])


def test_uncertain_and_all_null_ack_are_not_published_calculated_forecasts(tmp_path):
    uncertain = audit(tmp_path, [make_row(uncertain=True)])
    assert uncertain['publication_statuses'] == {'uncertain': 1}
    assert all(h['counts']['uncertain_eligible'] == 1 and h['counts']['acknowledged_eligible'] == 0 for h in uncertain['horizons'])
    health = audit(tmp_path, [make_row(unavailable=True)])
    assert health['publication_statuses'] == {'acknowledged': 1}
    assert all(h['counts']['calculated'] == h['counts']['acknowledged_eligible'] == 0 for h in health['horizons'])


def test_target_between_attempt_and_ack_stays_in_accuracy_but_not_early(tmp_path):
    row = make_row()
    state = json.loads(row['state_json'])
    state['publication']['ack_monotonic_ns'] = state['targets']['1']['first_event']['received_monotonic_ns']
    state['publication']['ack_wall_ns'] = state['targets']['1']['first_event']['received_wall_ns']
    for target in state['targets'].values():
        lead = int(target['first_event']['received_monotonic_ns']) - int(state['publication']['ack_monotonic_ns'])
        target['confirmed_redis_lead_ns'] = str(lead) if lead > 0 else None
    result = audit(tmp_path, [encode(row, state=state)])
    first = result['horizons'][0]
    assert first['errors']['acknowledged_matched_valid']['n'] == 1
    assert first['errors']['confirmed_early_matched_valid']['n'] == 0


def test_intent_only_restart_is_unconfirmed_without_inferred_wire_membership(tmp_path):
    async def scenario():
        value, _, spool, _, _ = HELPERS['runtime']()
        row = HELPERS['issue'](value)
        await value.publish(row)
        intent = next(iter(spool.rows.values()))
        recovered, _, _, store, redis = HELPERS['runtime']()
        await recovered._recover(intent)
        assert not redis.calls
        return encode(store.persisted[-1])
    result = audit(tmp_path, [asyncio.run(scenario())])
    assert result['publication_statuses'] == {'intent': 1}
    assert result['counts']['restart_reconciled_rows'] == 1
    assert all(h['counts']['target_restart_unmatched'] == 1 and
               h['counts']['acknowledged_eligible'] == 0 for h in result['horizons'])


@pytest.mark.parametrize('fault', ['missing_anchor', 'twap_regression'])
def test_global_reasons_keep_engine_immediate_expiry(tmp_path, fault):
    async def scenario():
        value, clock, _, _, _ = HELPERS['runtime']()
        if fault == 'missing_anchor':
            value.offer_price('spot', Decimal('100'), HELPERS['BASE'] + 100000,
                              clock.wall, clock.mono, 'spot')
        else:
            HELPERS['warm'](value)
            clock.advance(analysis.SECOND)
            value.offer_price('twap', Decimal('100'), HELPERS['BASE'] + 99000,
                              clock.wall, clock.mono, 'regressed-anchor', 60)
        row = value.issue()
        assert row.decision.valid_until_wall_ns == row.decision.decision_wall_ns
        await value.publish(row)
        clock.advance(120 * analysis.SECOND)
        value.finalize_due()
        return encode(row.record())
    result = audit(tmp_path, [asyncio.run(scenario())])
    assert result['publication_statuses'] == {'expired_or_target_received': 1}
    assert all(h['counts']['calculated'] == 0 for h in result['horizons'])


@pytest.mark.parametrize('fault', ['conflicted', 'causality_invalid'])
def test_invalid_matches_keep_recorded_errors_but_leave_valid_paired_cohort(tmp_path, fault):
    row = make_row()
    state = json.loads(row['state_json'])
    if fault == 'conflicted':
        state['targets']['1']['conflicted'] = True
        state['targets']['1']['confirmed_redis_lead_ns'] = None
    else:
        state['causality_invalid'] = True
        for target in state['targets'].values():
            target['confirmed_redis_lead_ns'] = None
    result = audit(tmp_path, [encode(row, state=state)])
    first = result['horizons'][0]
    assert first['counts']['acknowledged_eligible'] == 1
    assert first['counts']['target_matched'] == 1
    assert first['errors']['acknowledged_matched_valid']['n'] == 0


def test_actual_future_clock_target_is_counted_but_not_scored(tmp_path):
    row = make_row()
    state = json.loads(row['state_json'])
    target = state['targets']['1']
    event = target['first_event']
    event['received_wall_ns'] = str(event['source_timestamp_ms'] * analysis.MS - 1)
    event['received_ms'] = int(event['received_wall_ns']) // analysis.MS
    target['clock_anomaly'] = True
    target['confirmed_redis_lead_ns'] = None
    forecast = json.loads(row['frozen_json'])['forecasts'][0]
    target['eta_error_ns'] = str(int(event['received_wall_ns']) - int(forecast['estimated_arrival_wall_ns']))
    result = audit(tmp_path, [encode(row, state=state)])
    first = result['horizons'][0]
    assert first['counts']['clock_anomaly'] == 1
    assert first['errors']['acknowledged_matched_valid']['n'] == 0


@pytest.mark.parametrize('fault', ['global_reasons', 'negative_intent_latency', 'unsubstantiated_nonpending'])
def test_unsupported_fault_and_clock_evidence_is_rejected(tmp_path, fault):
    row = make_row(filtered=True)
    frozen, state = json.loads(row['frozen_json']), json.loads(row['state_json'])
    payload = json.loads(state['publication']['payload_json'])
    if fault == 'global_reasons':
        frozen['reasons'] = ['twap_regression']
        frozen['valid_until_wall_ns'] = frozen['decision_wall_ns']
    elif fault == 'negative_intent_latency':
        early = str(int(state['computation_completed_monotonic_ns']) - 1)
        state['publication']['intent_monotonic_ns'] = early
        payload['publication_intent_monotonic_ns'] = early
    else:
        reasons = ['target_not_pending_before_publication']
        payload['publication_eligibility']['excluded_horizons']['1'] = reasons
        payload['forecasts'][0]['reasons'] = reasons
    state['publication']['payload_json'] = json.dumps(payload)
    with pytest.raises(ValueError):
        audit(tmp_path, [encode(row, frozen, state)])


@pytest.mark.parametrize('fault', ['false_conflict_flag', 'wrong_conflict_source', 'policy'])
def test_secondary_evidence_and_frozen_policy_cannot_silently_change(tmp_path, fault):
    row = make_row()
    frozen, state = json.loads(row['frozen_json']), json.loads(row['state_json'])
    if fault == 'policy':
        frozen['policy']['max_carry_ms'] += 1
    else:
        target = state['targets']['1']
        conflict = deepcopy(target['first_event'])
        conflict['value'] = '102'
        target['first_conflicting_event'] = conflict
        if fault == 'wrong_conflict_source':
            conflict['source_timestamp_ms'] += 1000
            target['conflicted'] = True
            target['confirmed_redis_lead_ns'] = None
    with pytest.raises(ValueError):
        audit(tmp_path, [encode(row, frozen, state)])


def test_exact_decimal_arithmetic_ignores_ambient_precision(tmp_path):
    row = make_row(tiny=True)
    with localcontext() as context:
        context.prec = 6
        result = audit(tmp_path, [row])
    errors = result['horizons'][0]['errors']['acknowledged_matched_valid']
    assert Decimal(errors['ghost_absolute_error']['p50']) == Decimal('0.000000000000000001')
    assert Decimal(errors['persistence_absolute_error']['p50']) == 0
    assert errors['worse'] == 1


@pytest.mark.parametrize('mutation', ['mean', 'count', 'slot_reference', 'expiry', 'outside', 'nonterminal', 'target_error'])
def test_snapshot_integrity_failures(tmp_path, mutation):
    row = make_row()
    frozen, state = json.loads(row['frozen_json']), json.loads(row['state_json'])
    if mutation == 'mean':
        frozen['forecasts'][0]['price'] = '99.000000000000000000'
    elif mutation == 'count':
        frozen['forecasts'][0]['counts']['observed'] -= 1
    elif mutation == 'slot_reference':
        frozen['slots'][0]['value'] = '99'
    elif mutation == 'expiry':
        frozen['valid_until_wall_ns'] = str(int(frozen['valid_until_wall_ns']) + 1)
    elif mutation == 'outside':
        frozen['runtime_policy']['canary_start_ms'] = HELPERS['BASE'] - analysis.HOUR_MS
    elif mutation == 'nonterminal':
        row['terminal'] = False
    elif mutation == 'target_error':
        state['targets']['1']['error'] = '0.000000000000000000'
    with pytest.raises(ValueError):
        audit(tmp_path, [encode(row, frozen, state)])


def test_expected_whole_export_hash_count_and_no_overwrite(tmp_path):
    row = make_row()
    path, digest, count = export(tmp_path, [row])
    with pytest.raises(ValueError, match='SHA-256'):
        analysis.analyze(path, HELPERS['BASE'], '0' * 64, count)
    with pytest.raises(ValueError, match='row count'):
        analysis.analyze(path, HELPERS['BASE'], digest, count + 1)
    output = tmp_path / 'results'
    analysis.write_results(path, HELPERS['BASE'], digest, count, output)
    manifest = json.loads((output / 'manifest.json').read_text())
    for name, expected in manifest['artifacts_sha256'].items():
        assert sha256((output / name).read_bytes()).hexdigest() == expected
    assert manifest['code_sha256'] == sha256(Path(analysis.__file__).read_bytes()).hexdigest()
    index = json.loads((output / 'payload_index.json').read_text())
    payload = json.loads(row['state_json'])['publication']['payload_json']
    entry = index[sha256(payload.encode()).hexdigest()]
    assert entry['eligible_horizons'] == list(analysis.HORIZONS) and entry['status'] == 'acknowledged'
    with pytest.raises(ValueError, match='overwrite'):
        analysis.write_results(path, HELPERS['BASE'], digest, count, output)


def test_truncated_and_duplicate_key_exports_are_rejected(tmp_path):
    row = make_row()
    path, digest, count = export(tmp_path, [row])
    path.write_bytes(path.read_bytes().rstrip(b'\n'))
    with pytest.raises(ValueError, match='truncated'):
        analysis.analyze(path, HELPERS['BASE'], sha256(path.read_bytes()).hexdigest(), count)
    with pytest.raises(ValueError, match='Duplicate JSON key'):
        analysis.read_json('{"version":1,"version":2}')
