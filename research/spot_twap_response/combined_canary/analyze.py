"""Independent v5 campaign audit; no production imports or service access.

Verifies a complete exported table before accepting the selected one-hour
campaign. Selected snapshots prove their saved arithmetic, not completeness of
the feed or reconstruction of rejected events. No observer/browser inference.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
from hashlib import sha256
import json
from pathlib import Path
import re

HORIZONS = (1, 2, 3, 5, 10, 30)
CATEGORIES = ('observed', 'carried', 'pending', 'future', 'missing')
CONTEXT = Context(prec=80, rounding=ROUND_HALF_EVEN)
QUANTUM = Decimal('0.000000000000000001')
MS, SECOND, HOUR_MS = 1_000_000, 1_000_000_000, 3_600_000
FIELDS = {'run_id', 'decision_id', 'decision_wall_ns', 'created_ms', 'frozen_json',
          'state_json', 'version', 'terminal', 'frozen_sha256', 'state_sha256'}
POLICY = 'per-horizon-unreceived-v1'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reject_number(value):
    raise ValueError('Floating/nonfinite JSON number: ' + value)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'Duplicate JSON key: ' + key)
        result[key] = value
    return result


def read_json(raw):
    return json.loads(raw, parse_float=reject_number, parse_constant=reject_number,
                      object_pairs_hook=unique_object)


def integer(value):
    require(type(value) is int or isinstance(value, str) and
            re.fullmatch(r'-?[0-9]+', value) is not None, 'Expected exact integer')
    return int(value)


def decimal(value, positive=False):
    require(isinstance(value, str), 'Financial value must be a decimal string')
    result = Decimal(value)
    require(result.is_finite() and abs(result) < Decimal('1e20'), 'Invalid financial value')
    require(not positive or result > 0, 'Price must be positive')
    require(result.quantize(QUANTUM) == result, 'Financial value exceeds E18')
    return result


def e18(value):
    return format(value.quantize(QUANTUM), '.18f')


def distribution(values, divisor=1):
    values = sorted(Decimal(v) / Decimal(divisor) for v in values)
    if not values:
        return {'n': 0, 'negative_n': 0, 'p50': None, 'p90': None, 'p99': None, 'min': None, 'max': None}
    def quantile(q):
        position = Decimal(len(values) - 1) * Decimal(q)
        lower = int(position)
        upper = min(lower + 1, len(values) - 1)
        return str(values[lower] + (values[upper] - values[lower]) * (position - lower))
    return dict(n=len(values), negative_n=sum(v < 0 for v in values),
                p50=quantile('.5'), p90=quantile('.9'), p99=quantile('.99'),
                min=str(values[0]), max=str(values[-1]))


def paired_summary(pairs):
    ghost = [abs(a) for a, _, _ in pairs]
    persistence = [abs(b) for _, b, _ in pairs]
    return dict(n=len(pairs), better=sum(abs(a) < abs(b) for a, b, _ in pairs),
                equal=sum(abs(a) == abs(b) for a, b, _ in pairs),
                worse=sum(abs(a) > abs(b) for a, b, _ in pairs),
                ghost_absolute_error=distribution(ghost),
                persistence_absolute_error=distribution(persistence),
                ghost_absolute_error_bps=distribution([abs(a) / actual * 10000 for a, _, actual in pairs]),
                persistence_absolute_error_bps=distribution([abs(b) / actual * 10000 for _, b, actual in pairs]))


def event_check(event, feed=None):
    require(isinstance(event, dict), 'Event must be an object')
    require(event['feed'] in ('spot', 'twap') and (feed is None or event['feed'] == feed), 'Event feed mismatch')
    require(event['window_s'] == (60 if event['feed'] == 'twap' else None), 'Event window mismatch')
    for field in ('source_timestamp_ms', 'received_wall_ns', 'received_monotonic_ns', 'sequence'):
        require(integer(event[field]) >= 0, 'Negative event clock/sequence')
    require(type(event['source_timestamp_ms']) is int and event['source_timestamp_ms'] % 1000 == 0, 'Source second identity')
    require(type(event['sequence']) is int and event['sequence'] > 0, 'Sequence identity')
    require(isinstance(event['event_id'], str) and bool(event['event_id']), 'Missing event identity')
    require(integer(event['received_wall_ns']) // MS == event['received_ms'], 'Event receive millisecond mismatch')
    decimal(event['value'], True)


def verify_calculation(frozen, state):
    wall, mono = integer(frozen['decision_wall_ns']), integer(frozen['decision_monotonic_ns'])
    require(frozen['contract_version'] == 3 and frozen['runtime_version'] == 'ghost-canary-v5'
            and frozen['publication_eligibility_policy'] == POLICY, 'Selected campaign has wrong version/policy')
    require(type(frozen['contract_version']) is int, 'Contract version must be an integer')
    for field in ('causality_invalid', 'restart_reconciled'):
        require(field not in state or type(state[field]) is bool, 'Invalid state boolean: ' + field)
    require((frozen['context_precision'], frozen['price_precision'], frozen['rounding']) ==
            (80, 18, 'ROUND_HALF_EVEN'), 'Arithmetic contract mismatch')
    policy = frozen['policy']
    require(policy['source_max_age_ms'] == 5000 and policy['receipt_max_age_ms'] == 3000,
            'Combined campaign requires source 5000 ms / receipt 3000 ms')
    require((policy['max_carry_ms'], policy['history_ms'], policy['max_events']) == (10000, 120000, 1024),
            'Combined campaign carry/history/event policy mismatch')
    for field in ('source_max_age_ms', 'receipt_max_age_ms', 'max_carry_ms', 'history_ms', 'max_events'):
        require(type(policy[field]) is int and policy[field] > 0, 'Invalid engine policy')
    require(policy['enabled'] is True and frozen['runtime_policy']['enabled'] is True, 'Disabled campaign policy')
    for field in ('source_max_age_ms', 'receipt_max_age_ms'):
        require(frozen['runtime_policy'][field] == policy[field], 'Runtime/engine policy mismatch')
    spot, anchor = frozen['current_spot'], frozen['current_twap']
    events = {}
    for current, feed in ((spot, 'spot'), (anchor, 'twap')):
        if current is not None:
            event_check(current, feed)
            require(integer(current['received_wall_ns']) <= wall and integer(current['received_monotonic_ns']) <= mono,
                    'Current input received after decision')
            events[current['sequence']] = current
    # Global input/clock/regression faults expire immediately. Missing slots
    # alone are horizon-level reasons and do not change this deadline rule.
    deadlines = [wall] if frozen['reasons'] else []
    for event in (spot, anchor):
        if event is not None:
            deadlines.extend(((event['source_timestamp_ms'] + policy['source_max_age_ms']) * MS,
                              integer(event['received_wall_ns']) + policy['receipt_max_age_ms'] * MS,
                              wall + policy['receipt_max_age_ms'] * MS -
                              (mono - integer(event['received_monotonic_ns']))))
    require(integer(frozen['valid_until_wall_ns']) == min(deadlines, default=wall), 'Frozen expiry deadline mismatch')
    inputs = {}
    for event in frozen['slot_inputs']:
        event_check(event, 'spot')
        sequence = event['sequence']
        require(sequence not in inputs, 'Duplicate slot input sequence')
        require(integer(event['received_wall_ns']) <= wall and integer(event['received_monotonic_ns']) <= mono,
                'Slot input received after decision')
        require(sequence not in events or events[sequence] == event, 'Conflicting sequence evidence')
        inputs[sequence] = events[sequence] = event
    if events:
        require(integer(frozen['included_sequence']) >= max(events), 'Input exceeds decision sequence')
    slots = frozen['slots']
    require(isinstance(slots, list) and len(slots) == (89 if anchor else 0), 'Slot array length')
    for i, slot in enumerate(slots):
        stamp, category = slot['slot_timestamp_ms'], slot['category']
        require(stamp == anchor['source_timestamp_ms'] + (i - 61) * 1000, 'Slot grid boundary')
        require(category in CATEGORIES, 'Unknown slot category')
        if category == 'missing':
            require(slot['value'] is None and slot['input_sequence'] is None, 'Missing slot has price/reference')
            continue
        event = inputs.get(slot['input_sequence'])
        require(event is not None and slot['value'] == event['value'], 'Slot reference/value mismatch')
        if category == 'future':
            require(stamp * MS > wall and event == spot and slot['carry_age_ms'] is None, 'Future slot is not current spot')
        else:
            age = stamp - event['source_timestamp_ms']
            require(stamp * MS <= wall and event['source_timestamp_ms'] * MS <= integer(event['received_wall_ns']),
                    'Historical slot uses future information')
            require(slot['carry_age_ms'] == age and 0 <= age <= policy['max_carry_ms'], 'Carry age invalid')
            require((category == 'observed') == (age == 0), 'Observed/carry classification')
    forecasts = frozen['forecasts']
    require([f['horizon_s'] for f in forecasts] == list(HORIZONS) and
            all(type(f['horizon_s']) is int for f in forecasts), 'Forecast horizons must be exact and unique')
    require(set(state['targets']) == {str(h) for h in HORIZONS}, 'Target horizons mismatch')
    for h, forecast in zip(HORIZONS, forecasts):
        require(isinstance(forecast['reasons'], list) and all(isinstance(r, str) for r in forecast['reasons'])
                and all(reason in forecast['reasons'] for reason in frozen['reasons']), 'Forecast omits global reasons')
        target = None if anchor is None else anchor['source_timestamp_ms'] + h * 1000
        start = None if anchor is None else h - 1
        chosen = [] if start is None else slots[start:start + 60]
        counts = {category: sum(s['category'] == category for s in chosen) for category in CATEGORIES}
        if not chosen:
            counts['missing'] = 60
        require(forecast['target_source_timestamp_ms'] == target and forecast['slot_start_index'] == start, 'Forecast window identity')
        require(forecast['counts'] == counts and all(type(n) is int for n in forecast['counts'].values())
                and sum(counts.values()) == 60, 'Forecast category counts')
        require(forecast['max_interior_carry_ms'] == max((s['carry_age_ms'] for s in chosen if s['category'] == 'carried'), default=0),
                'Interior carry maximum mismatch')
        eta = None if anchor is None else integer(anchor['received_wall_ns']) + h * SECOND
        require((None if forecast['estimated_arrival_wall_ns'] is None else integer(forecast['estimated_arrival_wall_ns'])) == eta,
                'Estimated arrival mismatch')
        require((None if forecast['estimated_remaining_ns'] is None else integer(forecast['estimated_remaining_ns'])) ==
                (None if eta is None else eta - wall), 'Remaining estimate mismatch')
        require(forecast['estimate_overdue'] is (eta is not None and eta < wall), 'Overdue flag mismatch')
        if target is not None:
            market = target // 300000 * 300000
            require(forecast['market'] == dict(market_id=market // 300000, market_start_ms=market, market_end_ms=market + 300000),
                    'Forecast market mismatch')
            require(len(chosen) == 60 and chosen[0]['slot_timestamp_ms'] == target - 62000
                    and chosen[-1]['slot_timestamp_ms'] == target - 3000, '60-slot endpoints')
        if forecast['price'] is None:
            require(forecast['quality'] == 'unavailable' and isinstance(forecast['reasons'], list) and bool(forecast['reasons']),
                    'Unavailable forecast missing reasons')
        else:
            require(not frozen['reasons'] and not forecast['reasons'] and not counts['missing'], 'Available forecast has missing/reasons')
            require(forecast['quality'] == ('degraded' if counts['carried'] else 'healthy'), 'Available forecast quality')
            expected = e18(sum((decimal(s['value'], True) for s in chosen), Decimal(0)) / Decimal(60))
            require(forecast['price'] == expected, 'Independent 60-slot mean mismatch')
            for event in (spot, anchor):
                require(event is not None, 'Available forecast missing current input')
                source, receipt = event['source_timestamp_ms'] * MS, integer(event['received_wall_ns'])
                ages = (wall - source, wall - receipt, mono - integer(event['received_monotonic_ns']))
                require(source <= receipt and min(ages) >= 0 and ages[0] <= policy['source_max_age_ms'] * MS
                        and max(ages[1:]) <= policy['receipt_max_age_ms'] * MS, 'Available current freshness mismatch')
    return events


def verify_publication(frozen, state):
    pub = state['publication']
    require(isinstance(pub, dict) and isinstance(pub.get('status'), str), 'Publication status missing')
    if pub.get('payload_json') is None:
        require(pub['status'] not in ('acknowledged', 'uncertain') and pub.get('attempt_monotonic_ns') is None,
                'Attempt/ack without payload')
        return set(), None
    payload = read_json(pub['payload_json'])
    require(isinstance(payload, dict), 'Payload must be an object')
    for field in ('run_id', 'decision_id', 'decision_wall_ns', 'decision_monotonic_ns', 'current_spot', 'current_twap',
                  'contract_version', 'policy', 'valid_until_wall_ns'):
        require(payload[field] == frozen[field], 'Payload/frozen mismatch: ' + field)
    require(payload['runtime_version'] == 'ghost-canary-v5', 'Wrong wire runtime version')
    require([f['horizon_s'] for f in payload['forecasts']] == list(HORIZONS), 'Wire horizons must be exact and unique')
    require(all(type(f['horizon_s']) is int for f in payload['forecasts']), 'Wire horizons must be integer IDs')
    selection = payload.get('publication_eligibility')
    require(isinstance(selection, dict) and type(selection.get('version')) is int and selection['version'] == 1,
            'Unknown or missing v5 publication mask')
    eligible, excluded = selection.get('eligible_horizons'), selection.get('excluded_horizons')
    require(isinstance(eligible, list) and all(type(h) is int for h in eligible) and len(set(eligible)) == len(eligible)
            and eligible == [h for h in HORIZONS if h in eligible], 'Invalid eligible horizon IDs')
    require(isinstance(excluded, dict) and set(excluded) == {str(h) for h in HORIZONS if h not in eligible}, 'Mask does not partition horizons')
    checked_wall, checked_mono = integer(selection['checked_wall_ns']), integer(selection['checked_monotonic_ns'])
    require(checked_mono >= integer(pub['intent_monotonic_ns']), 'Selection predates intent')
    require(integer(pub['intent_monotonic_ns']) >= integer(state['computation_completed_monotonic_ns']),
            'Publication intent predates completed calculation')
    require(integer(payload['publication_intent_wall_ns']) == integer(pub['intent_wall_ns'])
            and integer(payload['publication_intent_monotonic_ns']) == integer(pub['intent_monotonic_ns']), 'Intent clocks mismatch')
    attempted = pub.get('attempt_monotonic_ns') is not None
    require(payload['publication_state'] == ('attempted' if attempted else 'intent'), 'Payload attempt state mismatch')
    if attempted:
        require(checked_wall == integer(pub['attempt_wall_ns']) and checked_mono == integer(pub['attempt_monotonic_ns']), 'Final selection/attempt clocks mismatch')
        for suffix in ('wall_ns', 'monotonic_ns'):
            require(integer(payload['publication_attempt_' + suffix]) == integer(pub['attempt_' + suffix]), 'Wire attempt clock mismatch')
    if pub['status'] in ('acknowledged', 'uncertain'):
        require(attempted, 'Published result without attempt')
    if pub['status'] == 'acknowledged':
        require(integer(pub['ack_monotonic_ns']) >= checked_mono, 'ACK before attempt')
    for original, wire in zip(frozen['forecasts'], payload['forecasts']):
        h = original['horizon_s']
        result = state['targets'][str(h)]
        if h in eligible:
            require(original['price'] is not None and wire == original, 'Eligible forecast differs from frozen price/quality/evidence')
            first = result.get('first_event')
            if first is not None:
                # Equal timestamps do not establish callback order; lead still
                # requires ACK strictly before receipt.
                require(integer(first['received_monotonic_ns']) >= checked_mono, 'Eligible target recorded before selection')
        elif original['price'] is None:
            require(wire == original and excluded[str(h)] == original['reasons'], 'Unavailable forecast was rewritten')
        else:
            reasons = excluded[str(h)]
            require(isinstance(reasons, list) and reasons in
                    [original['reasons'] + ['target_received_before_publication'],
                     original['reasons'] + ['target_not_pending_before_publication']], 'Invalid exclusion reasons')
            expected = dict(original, price=None, quality='unavailable', reasons=reasons)
            require(wire == expected, 'Withheld wire forecast mismatch')
            if reasons[-1] == 'target_received_before_publication':
                require(result.get('first_event') is not None and
                        integer(result['first_event']['received_monotonic_ns']) <= checked_mono, 'Exclusion has no prior recorded receipt')
            else:
                require(result.get('first_event') is None and
                        (result['status'] != 'pending' or result.get('conflicted') is True),
                        'Nonpending exclusion has no supporting target state')
                if result['status'] == 'missing':
                    require(checked_mono >= integer(frozen['decision_monotonic_ns']) + 120 * SECOND,
                            'Missing-target exclusion predates matching deadline')
    return (set(eligible) if attempted else set()), payload


class Audit:
    def __init__(self, campaign_start_ms):
        self.start = campaign_start_ms
        self.counts, self.publication, self.runs = Counter(), Counter(), Counter()
        self.horizons = {h: Counter() for h in HORIZONS}
        self.pairs = {h: defaultdict(list) for h in HORIZONS}
        self.leads = {h: [] for h in HORIZONS}
        self.timing, self.withheld = defaultdict(list), Counter()
        self.first_wall, self.last_wall = None, None
        self.payload_index, self.run_details, self.gap_hashes = {}, {}, defaultdict(set)

    def row(self, row, frozen, state):
        events = verify_calculation(frozen, state)
        eligible, payload = verify_publication(frozen, state)
        wall, mono = integer(frozen['decision_wall_ns']), integer(frozen['decision_monotonic_ns'])
        require(row['terminal'] is True, 'Selected campaign has incomplete rows')
        require(self.start * MS <= wall < (self.start + HOUR_MS) * MS, 'Decision outside fixed one-hour campaign')
        self.first_wall = wall if self.first_wall is None else min(self.first_wall, wall)
        self.last_wall = wall if self.last_wall is None else max(self.last_wall, wall)
        self.counts['selected_rows'] += 1
        self.runs[row['run_id']] += 1
        detail = self.run_details.setdefault(row['run_id'], dict(first_decision_wall_ns=str(wall),
            last_decision_wall_ns=str(wall), first_decision_monotonic_ns=str(mono), last_decision_monotonic_ns=str(mono),
            runtime_counter_max={}, max_gap_count=0, runtime_suspension_history_latest={}))
        newest = wall >= integer(detail['last_decision_wall_ns'])
        for prefix, select in (('first', min), ('last', max)):
            detail[prefix + '_decision_wall_ns'] = str(select(integer(detail[prefix + '_decision_wall_ns']), wall))
            detail[prefix + '_decision_monotonic_ns'] = str(select(integer(detail[prefix + '_decision_monotonic_ns']), mono))
        for name, count in frozen.get('runtime_counters', {}).items():
            require(type(count) is int and count >= 0, 'Invalid frozen runtime counter')
            detail['runtime_counter_max'][name] = max(detail['runtime_counter_max'].get(name, 0), count)
        detail['max_gap_count'] = max(detail['max_gap_count'], integer(frozen['gap_count']))
        if newest:
            detail['runtime_suspension_history_latest'] = frozen.get('runtime_suspension_history', {})
        for gap in frozen.get('operational_gaps', []):
            self.gap_hashes[row['run_id']].add(sha256(json.dumps(gap, sort_keys=True).encode()).hexdigest())
        detail['unique_saved_operational_gap_records'] = len(self.gap_hashes[row['run_id']])
        pub = state['publication']
        self.publication[pub['status']] += 1
        self.counts['restart_reconciled_rows'] += bool(state.get('restart_reconciled'))
        self.counts['causality_invalid_rows'] += bool(state.get('causality_invalid'))
        self.counts['any_calculated_rows'] += any(f['price'] is not None for f in frozen['forecasts'])
        self.counts['acknowledged_eligible_rows'] += pub['status'] == 'acknowledged' and bool(eligible)
        if payload is not None and pub.get('attempt_monotonic_ns') is not None:
            payload_hash = sha256(pub['payload_json'].encode()).hexdigest()
            require(payload_hash not in self.payload_index, 'Duplicate attempted payload identity')
            self.payload_index[payload_hash] = dict(run_id=row['run_id'], decision_id=row['decision_id'],
                status=pub['status'], eligible_horizons=sorted(eligible),
                attempt_wall_ns=str(integer(pub['attempt_wall_ns'])),
                attempt_monotonic_ns=str(integer(pub['attempt_monotonic_ns'])))
        completed = integer(state['computation_completed_monotonic_ns'])
        require(completed >= mono, 'Calculation completion predates decision')
        self.timing['decision_to_calculation_completed_ms'].append(completed - mono)
        included = events.get(frozen['included_sequence'])
        if included is not None:
            self.timing['included_event_receive_to_decision_ms'].append(mono - integer(included['received_monotonic_ns']))
        else:
            self.counts['included_event_receive_clock_not_in_snapshot'] += 1
        stages = [('calculation_completed_to_intent_ms', completed, pub.get('intent_monotonic_ns')),
                  ('intent_to_attempt_ms', pub.get('intent_monotonic_ns'), pub.get('attempt_monotonic_ns')),
                  ('attempt_to_ack_ms', pub.get('attempt_monotonic_ns'), pub.get('ack_monotonic_ns')),
                  ('decision_to_ack_ms', mono, pub.get('ack_monotonic_ns')),
                  ('included_event_receive_to_ack_ms', None if included is None else included['received_monotonic_ns'], pub.get('ack_monotonic_ns'))]
        for name, start, end in stages:
            if start is not None and end is not None:
                self.timing[name].append(integer(end) - integer(start))
        for forecast in frozen['forecasts']:
            h, price_text = forecast['horizon_s'], forecast['price']
            counts, target = self.horizons[h], state['targets'][str(h)]
            require(type(target['conflicted']) is bool, 'Invalid conflicted flag')
            for flag in ('clock_anomaly', 'late_missing'):
                require(flag not in target or type(target[flag]) is bool, 'Invalid target boolean: ' + flag)
            require(target['horizon'] == str(h) and target['target_source_timestamp_ms'] == forecast['target_source_timestamp_ms'], 'Target identity mismatch')
            require(target['status'] in ('matched', 'missing', 'not_forecast', 'restart_unmatched'), 'Unexpected nonterminal target status')
            require((target['status'] == 'not_forecast') == (price_text is None), 'Target/calculation availability mismatch')
            counts['rows'] += 1
            counts['calculated'] += price_text is not None
            counts['calculation_unavailable'] += price_text is None
            counts['target_' + target['status']] += 1
            counts['conflicted'] += target.get('conflicted') is True
            counts['clock_anomaly'] += target.get('clock_anomaly') is True
            counts['late_missing'] += bool(target.get('late_missing') or target.get('first_late_event'))
            acked = pub['status'] == 'acknowledged' and h in eligible
            counts['recorded_attempt_payload_eligible'] += h in eligible
            counts['acknowledged_eligible'] += acked
            counts['uncertain_eligible'] += pub['status'] == 'uncertain' and h in eligible
            if acked:
                counts['acknowledged_target_' + target['status']] += 1
            if payload and price_text is not None and str(h) in payload['publication_eligibility']['excluded_horizons']:
                for reason in payload['publication_eligibility']['excluded_horizons'][str(h)]:
                    self.withheld[reason] += 1
                    counts['withheld_' + reason] += 1
            first, expected_lead, pair = target.get('first_event'), None, None
            for field in ('first_late_event', 'first_conflicting_event'):
                evidence = target.get(field)
                if evidence is not None:
                    event_check(evidence, 'twap')
                    require(evidence['source_timestamp_ms'] == forecast['target_source_timestamp_ms'], 'Secondary target evidence identity mismatch')
                    if field == 'first_conflicting_event':
                        require(first is not None and target['conflicted'] is True and
                                decimal(evidence['value'], True) != decimal(first['value'], True),
                                'Conflicting target evidence contradicts conflict flag/first price')
                    else:
                        require(first is None and target['status'] in ('missing', 'restart_unmatched'),
                                'Late target evidence contradicts target status')
            if first is not None:
                event_check(first, 'twap')
                require(target['status'] == 'matched' and price_text is not None, 'First target is not a calculated match')
                require(first['source_timestamp_ms'] == forecast['target_source_timestamp_ms'], 'Wrong first target source')
                received = integer(first['received_monotonic_ns'])
                require(mono <= received < mono + 120 * SECOND, 'First target outside matching window')
                require(integer(first['received_wall_ns']) >= wall, 'First target wall receipt predates decision')
                anomaly = first['source_timestamp_ms'] * MS > integer(first['received_wall_ns'])
                require(target.get('clock_anomaly') is anomaly, 'Target clock anomaly flag mismatch')
                actual = decimal(first['value'], True)
                ghost_error = decimal(price_text, True) - actual
                persistence_error = decimal(frozen['current_twap']['value'], True) - actual
                require(target['error'] == e18(ghost_error) and target['persistence_error'] == e18(persistence_error), 'Stored target error mismatch')
                require(integer(target['eta_error_ns']) == integer(first['received_wall_ns']) - integer(forecast['estimated_arrival_wall_ns']), 'Stored ETA error mismatch')
                valid = not (target.get('conflicted') or anomaly or state.get('causality_invalid'))
                if valid:
                    pair = (ghost_error, persistence_error, actual)
                    self.pairs[h]['calculated_matched_valid'].append(pair)
                    if acked:
                        self.pairs[h]['acknowledged_matched_valid'].append(pair)
                        ack = integer(pub['ack_monotonic_ns'])
                        if ack < received:
                            expected_lead = received - ack
            else:
                require(target['status'] != 'matched', 'Matched target lacks first event')
                require(target.get('error') is None and target.get('persistence_error') is None, 'Error without first target')
            recorded = target.get('confirmed_redis_lead_ns')
            require((None if recorded is None else integer(recorded)) == expected_lead, 'False or missing confirmed lead credit')
            if expected_lead is not None:
                counts['confirmed_early'] += 1
                self.leads[h].append(expected_lead)
                self.pairs[h]['confirmed_early_matched_valid'].append(pair)

    def summary(self):
        return dict(status='accepted', campaign_start_ms=self.start, campaign_end_ms_exclusive=self.start + HOUR_MS,
                    runtime_version='ghost-canary-v5', contract_version=3, publication_eligibility_policy=POLICY,
                    counts=dict(self.counts), publication_statuses=dict(self.publication), run_rows=dict(self.runs),
                    run_details=self.run_details,
                    first_decision_wall_ns=None if self.first_wall is None else str(self.first_wall),
                    last_decision_wall_ns=None if self.last_wall is None else str(self.last_wall),
                    withheld_forecast_reasons=dict(self.withheld),
                    latency_ms={k: distribution(v, MS) for k, v in sorted(self.timing.items())},
                    horizons=[dict(horizon_s=h, counts=dict(self.horizons[h]), confirmed_lead_ms=distribution(self.leads[h], MS),
                        errors={cohort: paired_summary(self.pairs[h][cohort]) for cohort in
                            ('calculated_matched_valid', 'acknowledged_matched_valid', 'confirmed_early_matched_valid')}) for h in HORIZONS],
                    limitations=[
                        'Saved selected inputs validate saved arithmetic, not full input/rejected-event history or callback completeness.',
                        'Calculated means are distinct from attempted, acknowledged and confirmed-early forecasts.',
                        'Valid matched error cohorts exclude missing, restart-unmatched, conflicted, future-clock and causality-invalid targets.',
                        'Intent-only recovery cannot establish the exact transmitted subset; uncertainty receives no confirmed credit.',
                        'Recorded attempt payloads include any expired-before-attempt row; their final publication status distinguishes suppression from Redis acknowledgement.',
                        'Latency is local monotonic timing. Included-event receipt is reported only when its exact sequence exists in the saved snapshot.',
                        'Intent-to-attempt includes scheduling and durable-outbox work; it does not isolate fsync.',
                        'Confirmed early means Redis acknowledgement before the recorded first official target receipt, not client/browser delivery.',
                        'No continuous-time cache-coverage percentage is inferred from decision rows; use the separate observer.',
                        'Error basis points divide by the official target value; descriptive quantiles use linear interpolation.',
                        'Campaign row count is observed, not a promised timer rate; full export count is checked against its supplied verified manifest.'])


def analyze(input_path, campaign_start_ms, expected_sha256, expected_rows, *, include_payload_index=False):
    input_path = Path(input_path)
    require(not input_path.name.endswith('.part'), 'Only completed exports are accepted')
    require(type(campaign_start_ms) is int and campaign_start_ms >= 0, 'Invalid campaign start')
    require(type(expected_rows) is int and 0 < expected_rows <= 600000, 'Invalid expected export rows')
    require(isinstance(expected_sha256, str) and re.fullmatch('[0-9a-f]{64}', expected_sha256), 'Invalid expected SHA-256')
    audit, digest, seen = Audit(campaign_start_ms), sha256(), set()
    with localcontext(CONTEXT), input_path.open('rb') as source:
        while True:
            raw = source.readline(1024 * 1024 + 1)
            if not raw:
                break
            require(len(raw) <= 1024 * 1024 and raw.endswith(b'\n'), 'Oversized/truncated export line')
            digest.update(raw)
            row = read_json(raw)
            require(isinstance(row, dict) and set(row) == FIELDS, 'Export field contract')
            for field in ('run_id', 'decision_id', 'frozen_json', 'state_json'):
                require(isinstance(row[field], str) and bool(row[field]), 'Invalid export text field')
            require(type(row['version']) is int and row['version'] > 0 and type(row['terminal']) is bool, 'Invalid row version/terminal')
            require(type(row['created_ms']) is int and row['created_ms'] >= 0, 'Invalid row created_ms')
            identity = row['run_id'], row['decision_id']
            require(identity not in seen, 'Duplicate exported decision identity: ' + repr(identity))
            seen.add(identity)
            require(len(seen) <= expected_rows, 'Export exceeds expected row count')
            for field in ('frozen', 'state'):
                require(sha256(row[field + '_json'].encode()).hexdigest() == row[field + '_sha256'], 'Per-row ' + field + ' hash mismatch')
            frozen, state = read_json(row['frozen_json']), read_json(row['state_json'])
            require(isinstance(frozen, dict) and isinstance(state, dict), 'Nested audit JSON must be objects')
            require((frozen['run_id'], frozen['decision_id']) == identity, 'Frozen/export identity mismatch')
            wall = integer(frozen['decision_wall_ns'])
            require(wall == integer(row['decision_wall_ns']) and wall // MS == row['created_ms'], 'Export decision clock mismatch')
            start = frozen['runtime_policy']['canary_start_ms']
            require(type(start) is int and start >= 0, 'Invalid frozen campaign start')
            audit.counts['full_export_rows'] += 1
            if start != campaign_start_ms:
                audit.counts['other_campaign_rows_excluded'] += 1
                continue
            try:
                audit.row(row, frozen, state)
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError('Campaign row ' + repr(identity) + ': ' + str(exc)) from exc
        require(len(seen) == expected_rows, 'Export row count mismatch')
        require(digest.hexdigest() == expected_sha256, 'Full export SHA-256 mismatch')
        require(audit.counts['selected_rows'] > 0, 'No rows for requested campaign')
        summary = audit.summary()
        return (summary, audit.payload_index) if include_payload_index else summary


def report(summary):
    counts = summary['counts']
    lines = ['# Combined one-hour ghost canary', '',
             f"Campaign: `{summary['campaign_start_ms']}` to `{summary['campaign_end_ms_exclusive']}` (exclusive).",
             f"Verified {counts['full_export_rows']} exported rows; selected {counts['selected_rows']} terminal decisions across {len(summary['run_rows'])} run IDs.", '',
             '| Horizon | Calculated | ACK eligible | Confirmed early | Valid ACK pairs | Ghost median abs bp | Persistence median abs bp |',
             '|---:|---:|---:|---:|---:|---:|---:|']
    for row in summary['horizons']:
        c, errors = row['counts'], row['errors']['acknowledged_matched_valid']
        lines.append(f"| {row['horizon_s']}s | {c.get('calculated', 0)} | {c.get('acknowledged_eligible', 0)} | {c.get('confirmed_early', 0)} | {errors['n']} | {errors['ghost_absolute_error_bps']['p50']} | {errors['persistence_absolute_error_bps']['p50']} |")
    lines.extend(['', 'Publication outcomes: `' + json.dumps(summary['publication_statuses'], sort_keys=True) + '`.', '',
                  'Denominators, excluded targets, withheld reasons and stage-specific latency sample sizes are in summary.json.', ''])
    lines.extend('- ' + item for item in summary['limitations'])
    return '\n'.join(lines) + '\n'


def write_results(input_path, campaign_start_ms, expected_sha256, expected_rows, output):
    output = Path(output)
    require(not output.exists(), 'Output already exists; refusing overwrite')
    started_utc = datetime.now(timezone.utc).isoformat()
    code_hash = sha256(Path(__file__).read_bytes()).hexdigest()
    summary, payload_index = analyze(input_path, campaign_start_ms, expected_sha256, expected_rows, include_payload_index=True)
    require(sha256(Path(__file__).read_bytes()).hexdigest() == code_hash, 'Analyzer changed during verification')
    output.mkdir(parents=True, exist_ok=False)
    artifacts = {'summary.json': (json.dumps(summary, indent=2, sort_keys=True) + '\n').encode(),
                 'report.md': report(summary).encode(),
                 'payload_index.json': (json.dumps(payload_index, indent=2, sort_keys=True) + '\n').encode()}
    for name, raw in artifacts.items():
        with (output / name).open('xb') as handle:
            handle.write(raw)
    manifest = dict(status='accepted', started_utc=started_utc, created_utc=datetime.now(timezone.utc).isoformat(),
                    input_path=str(Path(input_path).resolve()), input_sha256=expected_sha256,
                    expected_export_rows=expected_rows, campaign_start_ms=campaign_start_ms,
                    campaign_end_ms_exclusive=campaign_start_ms + HOUR_MS,
                    selected_rows=summary['counts']['selected_rows'],
                    code_path=str(Path(__file__).resolve()), code_sha256=code_hash,
                    artifacts_sha256={name: sha256(raw).hexdigest() for name, raw in artifacts.items()})
    with (output / 'manifest.json').open('x', encoding='utf-8', newline='\n') as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write('\n')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--campaign-start-ms', type=int, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--expected-rows', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    write_results(args.input, args.campaign_start_ms, args.expected_sha256, args.expected_rows, args.output)
    print('Accepted campaign audit:', args.output)


if __name__ == '__main__':
    main()
