"""Compact ghost evidence and deterministic bounded accuracy aggregation.

Production-only standard-library module. No database, network or research imports.
Original hashes identify pre-compaction bytes; compact_revision identifies later
compact-only annotations. Individual bps use Decimal80; additive totals use
Decimal256 so retained 90-day bounded counters merge without rounding dependence.
"""
from __future__ import annotations

from bisect import bisect_left
from copy import deepcopy
from decimal import Decimal, localcontext
from hashlib import sha256
import json

HORIZONS = (1, 2, 3, 5, 10, 30)
CATEGORIES = ('observed', 'carried', 'pending', 'future', 'missing')
NS = 1000000000
HOUR_MS = 3600000
E18 = Decimal('0.000000000000000001')
EVENT_FIELDS = ('value', 'source_timestamp_ms', 'received_wall_ns',
                'received_monotonic_ns', 'sequence', 'event_id', 'window_s')
STAGES = ('intent', 'attempt', 'ack')

def require(value, message):
    if not value:
        raise ValueError(message)


def reject(value):
    raise ValueError('JSON floating/nonfinite value: ' + value)


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'Duplicate JSON key')
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, parse_float=reject, parse_constant=reject, object_pairs_hook=object_pairs)


def canonical_bytes(value):
    # Round-trip with strict decoding also rejects accidental Python floats.
    raw = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    decode(raw)
    return raw


def integer(value, *, nullable=False):
    if value is None and nullable:
        return None
    require(type(value) is int or isinstance(value, str) and value.lstrip('-').isdigit(), 'Expected integer')
    return int(value)


def money(value, *, positive=False, nullable=False):
    if value is None and nullable:
        return None
    require(isinstance(value, str), 'Financial value must be a string')
    result = Decimal(value)
    with localcontext() as context:
        context.prec = 80
        require(result.is_finite() and abs(result) < Decimal('1e20') and result.quantize(E18) == result,
                'Financial value does not fit NUMERIC(38,18)')
    require(not positive or result > 0, 'Price must be positive')
    # A fixed spelling makes NUMERIC(38,18) round-trips canonical. Original
    # source spelling remains identified by the original evidence hashes.
    return format(abs(result) if result.is_zero() else result, '.18f')


def event_record(event, feed):
    if event is None:
        return None
    require(event['feed'] == feed and event['window_s'] == (60 if feed == 'twap' else None), 'Event identity')
    result = {key:event[key] for key in EVENT_FIELDS}
    result['value'] = money(result['value'], positive=True)
    for name in ('source_timestamp_ms', 'received_wall_ns', 'received_monotonic_ns', 'sequence'):
        result[name] = integer(result[name])
        require(result[name] >= 0, 'Negative event clock/sequence')
    require(result['source_timestamp_ms'] % 1000 == 0 and result['sequence'] > 0, 'Invalid event stamp/sequence')
    require(isinstance(result['event_id'], str) and bool(result['event_id']), 'Missing event ID')
    return result


def compact_record(row, *, finalized_as_of_wall_ns):
    """Compact only an already-terminal recorded version after decision+120s.

    The as-of wall time is an offline age gate, not invented terminal evidence.
    Shutdown/restart-unmatched records remain censored even after this gate.
    """
    with localcontext() as context:
        context.prec = 80
        require(row['terminal'] is True, 'Cannot compact nonterminal record')
        for kind in ('frozen', 'state'):
            require(sha256(row[kind+'_json'].encode()).hexdigest() == row[kind+'_sha256'], 'Original hash mismatch')
        frozen, state = decode(row['frozen_json']), decode(row['state_json'])
        identity = row['run_id'], row['decision_id']
        require((frozen['run_id'], frozen['decision_id']) == identity, 'Frozen identity mismatch')
        require(frozen['runtime_version'] in ('ghost-canary-v6','ghost-continuous-v1') and frozen['contract_version'] == 4,
                'Unsupported runtime/contract')
        wall, mono = integer(frozen['decision_wall_ns']), integer(frozen['decision_monotonic_ns'])
        require(integer(row['decision_wall_ns']) == wall and row['created_ms'] == wall//1000000,
                'Decision row clock mismatch')
        require(integer(finalized_as_of_wall_ns) >= wall+120*NS, 'Matching window has not aged 120 seconds')
        pub = state['publication']
        attempted = pub.get('attempt_monotonic_ns') is not None
        wire = None if pub.get('payload_json') is None else decode(pub['payload_json'])
        if attempted:
            require(wire is not None and wire['publication_state'] == 'attempted', 'Attempt without exact payload')
        require(pub['status'] not in ('acknowledged', 'uncertain') or attempted, 'Publication without attempt')
        eligible, excluded, checked = set(), {}, {'wall_ns':None, 'monotonic_ns':None}
        if wire is not None:
            require((wire['run_id'], wire['decision_id']) == identity, 'Wire identity mismatch')
            for key in ('decision_wall_ns','decision_monotonic_ns','current_spot','current_twap',
                        'contract_version','policy','valid_until_wall_ns'):
                require(wire[key] == frozen[key], 'Wire/frozen shared metadata mismatch')
            selection = wire['publication_eligibility']
            require(selection['version'] == 1, 'Unsupported eligibility mask')
            ids = selection['eligible_horizons']
            require(all(type(h) is int for h in ids) and ids == [h for h in HORIZONS if h in ids], 'Bad eligibility IDs')
            require(set(selection['excluded_horizons']) == {str(h) for h in HORIZONS if h not in ids}, 'Mask partition')
            checked = {k:integer(selection['checked_'+k]) for k in checked}
            if attempted:
                eligible = set(ids)
                require(all(checked[k] == integer(pub['attempt_'+k]) for k in checked), 'Selection/attempt clocks')
            excluded = selection['excluded_horizons']
            require([f['horizon_s'] for f in wire['forecasts']] == list(HORIZONS), 'Wire horizon domain')
        decision = dict(run_id=row['run_id'], decision_id=row['decision_id'], created_ms=row['created_ms'],
            decision_wall_ns=wall, decision_monotonic_ns=mono, record_version=integer(row['version']),
            frozen_sha256=row['frozen_sha256'], state_sha256=row['state_sha256'],
            attempted_payload_sha256=sha256(pub['payload_json'].encode()).hexdigest() if attempted else None,
            model_version=frozen['model_version'], runtime_version=frozen['runtime_version'],
            contract_version=frozen['contract_version'], policy=frozen['policy'],
            publication_eligibility_policy=frozen['publication_eligibility_policy'],
            campaign_start_ms=frozen.get('campaign_start_ms',frozen['runtime_policy']['canary_start_ms']),
            current_spot=event_record(frozen['current_spot'], 'spot'), current_twap=event_record(frozen['current_twap'], 'twap'),
            included_sequence=integer(frozen['included_sequence']),
            computation_completed_wall_ns=integer(state['computation_completed_wall_ns']),
            computation_completed_monotonic_ns=integer(state['computation_completed_monotonic_ns']),
            valid_until_wall_ns=integer(frozen['valid_until_wall_ns']), publication_status=pub['status'],
            eligibility_checked_wall_ns=checked['wall_ns'], eligibility_checked_monotonic_ns=checked['monotonic_ns'],
            global_reasons=frozen['reasons'], causality_invalid=bool(state.get('causality_invalid')),
            shutdown=state.get('shutdown'), restart_reconciled=bool(state.get('restart_reconciled')),
            publication_restart_outcome=pub.get('restart_outcome'),
            gap_count=frozen.get('gap_count'), spot_reconnect_status=(frozen.get('spot_reconnect') or {}).get('status'),
            spot_reconnect_reason=(frozen.get('spot_reconnect') or {}).get('reason'))
        for stage in STAGES:
            for clock in ('wall_ns', 'monotonic_ns'):
                decision[stage+'_'+clock] = integer(pub.get(stage+'_'+clock), nullable=True)
        require(decision['computation_completed_monotonic_ns'] >= mono, 'Calculation clock regression')
        if attempted:
            require(mono <= decision['computation_completed_monotonic_ns'] <= decision['intent_monotonic_ns'] <=
                    decision['attempt_monotonic_ns'], 'Publication clock regression')
        if pub['status'] in ('acknowledged', 'uncertain'):
            left = min(decision['valid_until_wall_ns']-decision['attempt_wall_ns'],
                       decision['valid_until_wall_ns']-wall-(decision['attempt_monotonic_ns']-mono))
            require(left >= 1000000, 'Actual attempt expired')
        if pub['status'] == 'acknowledged':
            require(decision['ack_monotonic_ns'] >= decision['attempt_monotonic_ns'], 'ACK precedes attempt')
        require([f['horizon_s'] for f in frozen['forecasts']] == list(HORIZONS), 'Frozen horizon domain')
        require(set(state['targets']) == {str(h) for h in HORIZONS}, 'Target horizon domain')
        horizons = []
        for index, forecast in enumerate(frozen['forecasts']):
            h = forecast['horizon_s']; target = state['targets'][str(h)]
            require(type(target['conflicted']) is bool, 'Invalid conflict flag')
            require(target['target_source_timestamp_ms'] == forecast['target_source_timestamp_ms'], 'Target stamp mismatch')
            require(target['status'] in ('matched','missing','not_forecast','restart_unmatched'), 'Nonterminal target')
            counts = forecast['counts']
            require(set(counts) == set(CATEGORIES) and all(type(n) is int and n >= 0 for n in counts.values()) and
                    sum(counts.values()) == 60, 'Invalid slot counts')
            if h in eligible:
                require(wire['forecasts'][index] == forecast and forecast['price'] is not None, 'Attempted forecast differs')
            first = event_record(target.get('first_event'), 'twap')
            late = event_record(target.get('first_late_event'), 'twap')
            conflict = event_record(target.get('first_conflicting_event'), 'twap')
            for event in (first, late, conflict):
                require(event is None or event['source_timestamp_ms'] == target['target_source_timestamp_ms'], 'Event target mismatch')
            entry = dict(horizon_s=h, target_source_timestamp_ms=forecast['target_source_timestamp_ms'],
                forecast_price=money(forecast['price'], positive=True, nullable=True), quality=forecast['quality'],
                counts=counts, max_interior_carry_ms=forecast['max_interior_carry_ms'], reasons=forecast['reasons'],
                attempted_eligible=h in eligible, exclusion_reasons=excluded.get(str(h), []),
                estimated_arrival_wall_ns=integer(forecast['estimated_arrival_wall_ns'], nullable=True),
                estimated_remaining_ns=integer(forecast['estimated_remaining_ns'], nullable=True),
                target_status=target['status'], first_event=first, first_late_event=late, first_conflicting_event=conflict,
                conflicted=target['conflicted'], clock_anomaly=bool(target.get('clock_anomaly')),
                late_missing=bool(target.get('late_missing')), error=money(target.get('error'), nullable=True),
                persistence_error=money(target.get('persistence_error'), nullable=True),
                eta_error_ns=integer(target.get('eta_error_ns'), nullable=True),
                confirmed_redis_lead_ns=integer(target.get('confirmed_redis_lead_ns'), nullable=True))
            if first is not None:
                require(entry['target_status'] == 'matched' and entry['forecast_price'] is not None, 'Invalid matched target')
                require(mono <= first['received_monotonic_ns'] < mono+120*NS and first['received_wall_ns'] >= wall,
                        'Target outside matching window')
                require(entry['clock_anomaly'] == (first['source_timestamp_ms']*1000000 > first['received_wall_ns']), 'Clock anomaly flag')
                actual = Decimal(first['value'])
                require(Decimal(entry['error']) == Decimal(entry['forecast_price'])-actual and
                        Decimal(entry['persistence_error']) == Decimal(decision['current_twap']['value'])-actual, 'Error mismatch')
                if h in eligible:
                    require(first['received_monotonic_ns'] >= decision['attempt_monotonic_ns'], 'Target preceded eligibility')
            else:
                require(entry['target_status'] != 'matched' and entry['error'] is None and entry['persistence_error'] is None,
                        'Error without target')
            if conflict is not None:
                require(first is not None and entry['conflicted'] is True and Decimal(conflict['value']) != Decimal(first['value']),
                        'Unflagged conflict')
            ack_valid = (pub['status'] == 'acknowledged' and h in eligible and first is not None and
                         not (entry['conflicted'] or entry['clock_anomaly'] or decision['causality_invalid']))
            lead = first['received_monotonic_ns']-decision['ack_monotonic_ns'] if ack_valid and decision['ack_monotonic_ns'] < first['received_monotonic_ns'] else None
            require(entry['confirmed_redis_lead_ns'] == lead, 'Confirmed lead mismatch')
            horizons.append(entry)
        result = dict(schema_version=1, decision=decision, horizons=horizons)
        result['compact_sha256'] = sha256(canonical_bytes(result)).hexdigest()
        return result


def verify_compact(record):
    require(set(record) == {'schema_version','decision','horizons','compact_sha256'} and record['schema_version'] == 1,
            'Compact record contract')
    body = {key:value for key,value in record.items() if key != 'compact_sha256'}
    require(sha256(canonical_bytes(body)).hexdigest() == record['compact_sha256'], 'Compact hash mismatch')
    require([h['horizon_s'] for h in record['horizons']] == list(HORIZONS), 'Compact horizon domain')
    return record


def annotate_target(record, event):
    """Return a rehashed copy for first late/conflicting evidence, else None.

    Never promote a late event to the first match. Original state/version hashes
    remain source lineage; compact_revision counts post-compaction annotations.
    """
    verify_compact(record)
    observed = event_record(event, 'twap')
    require(observed is not None and observed['source_timestamp_ms']*1000000 <= observed['received_wall_ns'],
            'Future or absent late target')
    result = deepcopy(record)
    changed = False
    for target in result['horizons']:
        if target['target_source_timestamp_ms'] != observed['source_timestamp_ms']:
            continue
        first = target['first_event']
        if first is not None and target['target_status'] == 'matched':
            if Decimal(first['value']) != Decimal(observed['value']) and target['first_conflicting_event'] is None:
                target.update(conflicted=True, first_conflicting_event=observed, confirmed_redis_lead_ns=None)
                changed = True
        elif target['target_status'] in ('missing', 'restart_unmatched') and target['first_late_event'] is None:
            target.update(first_late_event=observed, late_missing=True)
            changed = True
    if not changed:
        return None
    result['decision']['compact_revision'] = result['decision'].get('compact_revision', 0)+1
    result.pop('compact_sha256')
    result['compact_sha256'] = sha256(canonical_bytes(result)).hexdigest()
    return result


# Each bucket is <= its named bound; final bucket is >1000bp. These are
# descriptive quantile brackets, never exact percentiles or statistical CIs.
HISTOGRAM_BOUNDS_BPS = ('0', '0.0001', '0.0003', '0.001', '0.003', '0.01',
                       '0.03', '0.1', '0.3', '1', '3', '10', '30', '100', '300', '1000')
_BOUNDS = tuple(Decimal(x) for x in HISTOGRAM_BOUNDS_BPS)
_SUMS = ('ghost_absolute_usd', 'held_absolute_usd', 'ghost_signed_usd',
         'ghost_absolute_bps', 'held_absolute_bps', 'ghost_signed_bps', 'paired_excess_bps',
         'confirmed_lead_ns', 'spot_source_age_ns', 'spot_wall_receipt_age_ns', 'spot_mono_receipt_age_ns',
         'twap_source_age_ns', 'twap_wall_receipt_age_ns', 'twap_mono_receipt_age_ns')
_COUNTS = ('issued', 'calculated', 'unavailable', 'attempted_eligible', 'published_eligible',
           'unpublished', 'uncertain', 'withheld', 'missing', 'restart_unmatched', 'conflicted',
           'clock_invalid', 'late_missing', 'late_publication', 'early', 'scored', 'better', 'equal', 'worse',
           'overpredicting', 'underpredicting', 'exact', 'quality_healthy', 'quality_degraded',
           'quality_unavailable', 'observed_slots', 'carried_slots', 'pending_slots', 'future_slots',
           'missing_slots', 'interior_carry_rows', 'spot_age_n', 'twap_age_n',
           'spot_missing', 'twap_missing', 'spot_future_clock', 'twap_future_clock',
           'partial_campaign_hour_rows')


def _empty_metrics():
    return dict(counts={name:0 for name in _COUNTS}, sums={name:'0' for name in _SUMS},
                reasons={}, ghost_histogram=[0]*(len(_BOUNDS)+1), held_histogram=[0]*(len(_BOUNDS)+1))


def _group_identity(decision, horizon, cohort):
    identity = {k:decision[k] for k in ('model_version','runtime_version','contract_version','policy')}
    policy_key = sha256(canonical_bytes(identity)).hexdigest()
    return policy_key+'|'+str(horizon)+'|'+cohort, dict(**identity, policy_key=policy_key, horizon_s=horizon, cohort=cohort)


def _decimal_text(value):
    if value == 0:
        return '0'
    text = format(value,'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def _sum(metric, key, value):
    metric['sums'][key] = _decimal_text(Decimal(metric['sums'][key])+Decimal(value))


def contribution(record):
    """One decision's reversible hourly contribution, including missingness.

    fixed_clock_window is the first UTC second of each minute; it can contain
    multiple decisions. It is a phase-selected diagnostic, not one independent
    observation per minute. Early/published panels always use identical pairs
    for ghost versus held-TWAP comparisons.
    """
    verify_compact(record)
    decision = record['decision']
    sampled = (decision['decision_wall_ns']//NS) % 60 == 0
    output = dict(schema_version=1, hour_start_ms=decision['created_ms']//HOUR_MS*HOUR_MS, groups={})
    campaign_start = decision['campaign_start_ms']
    partial_campaign_hour = (campaign_start % HOUR_MS != 0 and
        campaign_start//HOUR_MS*HOUR_MS == output['hour_start_ms'])
    for target in record['horizons']:
        h = target['horizon_s']; first = target['first_event']
        acked = decision['publication_status'] == 'acknowledged' and target['attempted_eligible']
        valid = (acked and first is not None and target['target_status'] == 'matched' and
                 not (target['conflicted'] or target['clock_anomaly'] or decision['causality_invalid']))
        early = valid and decision['ack_monotonic_ns'] < first['received_monotonic_ns']
        # Individual ratios have an explicit Decimal80 boundary. Summing their
        # finite representations below uses a larger exact accumulator context.
        pair = None
        if valid:
            with localcontext() as context:
                context.prec = 80
                actual = Decimal(first['value'])
                error = Decimal(target['forecast_price'])-actual
                held = Decimal(decision['current_twap']['value'])-actual
                pair = (error, held, error/actual*10000, held/actual*10000)
        for cohort in ('early','published'):
            key, identity = _group_identity(decision,h,cohort)
            group = dict(identity=identity, metrics=_empty_metrics(), fixed_clock_window=_empty_metrics())
            output['groups'][key] = group
            metrics = [group['metrics']]+([group['fixed_clock_window']] if sampled else [])
            for metric in metrics:
                counts = metric['counts']
                counts.update(issued=1, calculated=int(target['forecast_price'] is not None),
                    unavailable=int(target['forecast_price'] is None), attempted_eligible=int(target['attempted_eligible']),
                    published_eligible=int(acked), unpublished=int(decision['publication_status'] != 'acknowledged'),
                    uncertain=int(decision['publication_status'] == 'uncertain'),
                    withheld=int(target['forecast_price'] is not None and not target['attempted_eligible']),
                    missing=int(target['target_status'] == 'missing'), restart_unmatched=int(target['target_status'] == 'restart_unmatched'),
                    conflicted=int(target['conflicted']), clock_invalid=int(target['clock_anomaly'] or decision['causality_invalid']),
                    late_missing=int(target['late_missing'] or target['first_late_event'] is not None),
                    late_publication=int(valid and not early), early=int(early),
                    interior_carry_rows=int(target['max_interior_carry_ms'] > 0),
                    partial_campaign_hour_rows=int(partial_campaign_hour))
                require(target['quality'] in ('healthy','degraded','unavailable'), 'Unknown compact quality')
                counts['quality_'+target['quality']] = 1
                for category in CATEGORIES:
                    counts[category+'_slots'] = target['counts'][category]
                for reason in set(target['reasons']+target['exclusion_reasons']):
                    metric['reasons'][reason] = 1
                with localcontext() as context:
                    context.prec = 256
                    for feed in ('spot','twap'):
                        event = decision['current_'+feed]
                        if event is None:
                            counts[feed+'_missing'] = 1
                            continue
                        ages = (decision['decision_wall_ns']-event['source_timestamp_ms']*1000000,
                                decision['decision_wall_ns']-event['received_wall_ns'],
                                decision['decision_monotonic_ns']-event['received_monotonic_ns'])
                        if min(ages) < 0:
                            counts[feed+'_future_clock'] = 1
                        else:
                            counts[feed+'_age_n'] = 1
                            for suffix, age in zip(('source_age_ns','wall_receipt_age_ns','mono_receipt_age_ns'),ages):
                                _sum(metric,feed+'_'+suffix,age)
                    if early:
                        _sum(metric,'confirmed_lead_ns',first['received_monotonic_ns']-decision['ack_monotonic_ns'])
                    if pair is None or cohort == 'early' and not early:
                        continue
                    error, held, error_bps, held_bps = pair
                    counts['scored'] = 1
                    counts['better' if abs(error)<abs(held) else 'equal' if abs(error)==abs(held) else 'worse'] = 1
                    counts['overpredicting' if error>0 else 'underpredicting' if error<0 else 'exact'] = 1
                    for name,value in (('ghost_absolute_usd',abs(error)),('held_absolute_usd',abs(held)),
                        ('ghost_signed_usd',error),('ghost_absolute_bps',abs(error_bps)),('held_absolute_bps',abs(held_bps)),
                        ('ghost_signed_bps',error_bps),('paired_excess_bps',abs(error_bps)-abs(held_bps))):
                        _sum(metric,name,value)
                    metric['ghost_histogram'][bisect_left(_BOUNDS,abs(error_bps))] = 1
                    metric['held_histogram'][bisect_left(_BOUNDS,abs(held_bps))] = 1
    return output


def _merge_metric(target, source, sign):
    require(set(source['counts']) == set(_COUNTS) and set(source['sums']) == set(_SUMS), 'Aggregate metric contract')
    for name in ('counts','reasons'):
        for key,value in source[name].items():
            require(type(value) is int and value >= 0, 'Invalid aggregate count')
            updated = target[name].get(key,0)+sign*value
            require(updated >= 0, 'Aggregate subtraction underflow')
            if name == 'reasons' and updated == 0:
                target[name].pop(key,None)
            else:
                target[name][key] = updated
    with localcontext() as context:
        context.prec = 256
        for key,value in source['sums'].items():
            require(isinstance(value,str), 'Aggregate financial sums must remain decimal strings')
            number = Decimal(value)
            require(number.is_finite(), 'Invalid aggregate sum')
            result = Decimal(target['sums'][key])+sign*number
            target['sums'][key] = _decimal_text(result)
    for name in ('ghost_histogram','held_histogram'):
        require(len(source[name]) == len(_BOUNDS)+1, 'Histogram contract')
        for index,value in enumerate(source[name]):
            require(type(value) is int and value >= 0, 'Invalid histogram count')
            target[name][index] += sign*value
            require(target[name][index] >= 0, 'Histogram subtraction underflow')
        require(sum(target[name]) == target['counts']['scored'], 'Histogram/scored mismatch')


def merge(aggregate, delta, sign=1):
    """Pure associative add/subtract; caller commits replacement atomically."""
    require(type(sign) is int and sign in (-1,1), 'Merge sign')
    require(delta['schema_version'] == 1, 'Aggregate version')
    if aggregate is None:
        aggregate = dict(schema_version=1,hour_start_ms=delta['hour_start_ms'],groups={})
    require(aggregate['schema_version'] == 1 and aggregate['hour_start_ms'] == delta['hour_start_ms'], 'Cross-hour merge')
    result = deepcopy(aggregate)
    for key,source in delta['groups'].items():
        target = result['groups'].setdefault(key,dict(identity=deepcopy(source['identity']),metrics=_empty_metrics(),fixed_clock_window=_empty_metrics()))
        require(target['identity'] == source['identity'], 'Group identity collision')
        for name in ('metrics','fixed_clock_window'):
            _merge_metric(target[name],source[name],sign)
        if target['metrics']['counts']['issued'] == 0:
            require(all(Decimal(x)==0 for x in target['metrics']['sums'].values()), 'Nonzero empty aggregate')
            result['groups'].pop(key)
    return result


def _quantile_bracket(histogram, numerator, denominator):
    n = sum(histogram)
    if not n:
        return None
    rank = (n*numerator+denominator-1)//denominator
    total = 0
    for index,count in enumerate(histogram):
        total += count
        if total >= rank:
            return dict(lower_bps='0' if index == 0 else HISTOGRAM_BOUNDS_BPS[index-1],
                        upper_bps=HISTOGRAM_BOUNDS_BPS[index] if index<len(_BOUNDS) else None,
                        lower_inclusive=index == 0, upper_inclusive=True)


def _describe(metric):
    counts, sums = metric['counts'],metric['sums']
    with localcontext() as context:
        context.prec = 80
        def mean(name,n):
            return None if not n else format(Decimal(sums[name])/n,'f')
        n = counts['scored']
        held = Decimal(sums['held_absolute_bps'])
        return dict(counts=deepcopy(counts), unavailable_reasons=deepcopy(metric['reasons']),
            ghost_mae_bps=mean('ghost_absolute_bps',n), held_mae_bps=mean('held_absolute_bps',n),
            ghost_signed_mean_bps=mean('ghost_signed_bps',n), ghost_signed_mean_usd=mean('ghost_signed_usd',n),
            ghost_mae_usd=mean('ghost_absolute_usd',n), held_mae_usd=mean('held_absolute_usd',n),
            paired_excess_mae_bps=mean('paired_excess_bps',n),
            ghost_to_held_mae_ratio=None if not held else format(Decimal(sums['ghost_absolute_bps'])/held,'f'),
            confirmed_lead_mean_ns=mean('confirmed_lead_ns',counts['early']),
            scored_fraction_of_published_eligible=None if not counts['published_eligible'] else format(Decimal(n)/counts['published_eligible'],'f'),
            mean_input_ages_ns={feed:{suffix:mean(feed+'_'+suffix,counts[feed+'_age_n']) for suffix in
                ('source_age_ns','wall_receipt_age_ns','mono_receipt_age_ns')} for feed in ('spot','twap')},
            ghost_median_bracket_bps=_quantile_bracket(metric['ghost_histogram'],1,2),
            ghost_p90_bracket_bps=_quantile_bracket(metric['ghost_histogram'],9,10),
            held_median_bracket_bps=_quantile_bracket(metric['held_histogram'],1,2),
            held_p90_bracket_bps=_quantile_bracket(metric['held_histogram'],9,10))


def _pool(rows, key, start, end):
    result = None; hours = []
    for row in rows:
        if start <= row['hour_start_ms'] < end and key in row['groups']:
            group = row['groups'][key]
            shifted = dict(schema_version=1,hour_start_ms=0,groups={key:group})
            result = merge(result,shifted)
            if group['metrics']['counts']['issued'] and not group['metrics']['counts']['partial_campaign_hour_rows']:
                hours.append(row['hour_start_ms'])
    return None if result is None else result['groups'][key],sorted(set(hours))


def _operational_warnings(counts):
    warnings = []
    issued = counts['issued']
    if issued and counts['unpublished']*100 > issued:
        warnings.append('unpublished_above_1_percent')
    if issued and counts['unavailable']*20 > issued:
        warnings.append('unavailable_above_5_percent')
    if counts['calculated'] and counts['published_eligible']*10 < counts['calculated']*9:
        warnings.append('published_coverage_below_90_percent_of_calculated')
    if counts['published_eligible'] and counts['scored']*10 < counts['published_eligible']*9:
        warnings.append('scoring_coverage_below_90_percent_of_published')
    if counts['conflicted'] or counts['clock_invalid']:
        warnings.append('conflict_or_invalid_clock_evidence')
    return warnings


def compose_status(hourlyrows, now_ms, runtime_health):
    """Compose cached status with create-only frozen-baseline candidates.

    Caller persists/pops _baseline_candidate and _warning_state before publishing.
    The first qualifying three full UTC days available in this bounded snapshot
    establish a per-policy reference; no sliding replacement or earliest-ever
    claim after a long monitoring outage.
    Warnings are explicit engineering heuristics, not calibrated significance.
    """
    require(type(now_ms) is int, 'Status clock')
    rows = []
    for item in hourlyrows:
        row = item.get('aggregate',item)
        if isinstance(row,str):
            row = decode(row)
        require(row['schema_version'] == 1 and row['hour_start_ms'] % HOUR_MS == 0, 'Hourly aggregate contract')
        rows.append(row)
    require(len(rows) <= 24*91 and len({r['hour_start_ms'] for r in rows}) == len(rows), 'Unbounded/duplicate hourly rows')
    rows.sort(key=lambda r:r['hour_start_ms'])
    watermark = runtime_health.get('persistence_watermark_ms')
    end = ((now_ms-120000)//HOUR_MS)*HOUR_MS
    if watermark is not None:
        end = min(end,(integer(watermark)//HOUR_MS)*HOUR_MS)
    keys = sorted({key for row in rows if end-7*24*HOUR_MS <= row['hour_start_ms'] < end for key in row['groups']})
    baselines = runtime_health.get('accuracy_baseline') or dict(schema_version=1,groups={})
    old_warnings = runtime_health.get('accuracy_warning_state') or dict(schema_version=1,groups={})
    candidates = dict(schema_version=1,groups={}); warnings = deepcopy(old_warnings)
    panels = {name:[] for name in ('1h','24h','7d')}; monitor = []
    for key in keys:
        for name,hours in (('1h',1),('24h',24),('7d',168)):
            pooled,present = _pool(rows,key,end-hours*HOUR_MS,end)
            if pooled is not None:
                panels[name].append(dict(group_key=key,identity=pooled['identity'],hours_observed=len(present),expected_hours=hours,
                    metrics=_describe(pooled['metrics']),fixed_clock_window=_describe(pooled['fixed_clock_window'])))
        baseline = baselines['groups'].get(key)
        current,present = _pool(rows,key,end-24*HOUR_MS,end)
        operational = [] if current is None else _operational_warnings(current['metrics']['counts'])
        if baseline is None:
            available = [r['hour_start_ms'] for r in rows if r['hour_start_ms'] < end and key in r['groups']]
            for first in available:
                if first % (24*HOUR_MS):
                    continue
                last = first+72*HOUR_MS
                if last > end:
                    break
                pooled,present = _pool(rows,key,first,last)
                if len(present) == 72 and present == list(range(first,last,HOUR_MS)):
                    counts = pooled['metrics']['counts']
                    if counts['scored'] >= 3000 and counts['published_eligible'] and not _operational_warnings(counts):
                        candidates['groups'][key] = dict(schema_version=1,start_ms=first,end_ms=last,
                            captured_at_ms=now_ms,group=pooled,policy='first-available-3-complete-UTC-days-min3000-coverage90pct-v1')
                        break
            complete = [r['hour_start_ms'] for r in rows if r['hour_start_ms'] < end and key in r['groups']
                and r['groups'][key]['metrics']['counts']['issued']
                and not r['groups'][key]['metrics']['counts']['partial_campaign_hour_rows']]
            monitor.append(dict(group_key=key,state='collecting_baseline',complete_hours=len(set(complete)),
                                operational_warnings=operational))
            continue
        if current is None:
            continue
        previous = baseline['group']; c = current['metrics']['counts']
        adequate = (len(present) == 24 and c['scored'] >= 1000 and c['published_eligible'] and
                    not operational and end-24*HOUR_MS >= baseline['end_ms'])
        prior = deepcopy(old_warnings['groups'].get(key,dict(last_evaluated_end_ms=None,bad_hours=0,good_hours=0,active=False)))
        description = _describe(current['metrics']); reference = _describe(previous['metrics'])
        bad = False; recovered = False
        if adequate:
            with localcontext() as context:
                context.prec = 80
                mae, base = Decimal(description['ghost_mae_bps']),Decimal(reference['ghost_mae_bps'])
                excess, base_excess = Decimal(description['paired_excess_mae_bps']),Decimal(reference['paired_excess_mae_bps'])
                bad = mae > base*Decimal('1.25') and excess > base_excess+Decimal('0.01')
                recovered = mae <= base*Decimal('1.10') or excess <= base_excess+Decimal('0.005')
        if prior['last_evaluated_end_ms'] != end:
            consecutive = prior['last_evaluated_end_ms'] == end-HOUR_MS
            prior['bad_hours'] = (prior['bad_hours'] if consecutive else 0)+1 if adequate and bad else 0
            prior['good_hours'] = (prior['good_hours'] if consecutive else 0)+1 if adequate and recovered else 0
            if prior['bad_hours'] >= 3: prior['active'] = True
            if prior['good_hours'] >= 3: prior['active'] = False
            prior['last_evaluated_end_ms'] = end
        warnings['groups'][key] = prior
        monitor.append(dict(group_key=key,state='insufficient_current_coverage' if not adequate else
            'degraded' if prior['active'] else 'watch' if bad else 'within_thresholds',warning_active=prior['active'],
            baseline_start_ms=baseline['start_ms'],baseline_end_ms=baseline['end_ms'],operational_warnings=operational,**prior))
    health = {key:value for key,value in runtime_health.items() if key not in ('accuracy_baseline','accuracy_warning_state')}
    return dict(schema_version=1,generated_at_ms=now_ms,completed_window_end_ms=end,panels=panels,monitor=monitor,
        runtime_health=health, _baseline_candidate=candidates,_warning_state=warnings,
        method=dict(primary='early',secondary='published',matching_window_seconds=120,
            histogram_bounds_bps=list(HISTOGRAM_BOUNDS_BPS),quantiles='bounded nearest-rank histogram brackets',
            fixed_clock_window='all decisions within the first UTC second of each minute; not one independent sample/minute',
            warning='frozen72h reference;24hMAE >125% baseline AND paired excess >baseline+0.01bp;3 consecutive completed hours',
            recovery='24hMAE <=110% baseline OR paired excess <=baseline+0.005bp;3 consecutive completed hours',
            baseline_minimum='first qualifying3 full UTC days available in bounded snapshot,72 consecutive complete hours,3000pairs,90%publishedeligiblecoverage; partial campaign first hours excluded',
            current_minimum='24completehours,1000pairs,90%publishedeligiblecoverage,afterbaseline',
            operational_thresholds='unpublished>1%,unavailable>5%,published/calculated<90%,scored/published<90%,anyconflict/invalidclock; blocks accuracy warning/recovery decisions',
            limitations=['Engineering thresholds are not a statistical significance test or universal calibration.',
                        'Overlapping forecasts are correlated; feed gaps and missing targets are not ordinary prediction errors.',
                        'Compact evidence cannot re-verify deleted slot arithmetic or reconstruct all feed receipt gaps.',
                        'Confirmed Redis lead is not browser receipt lead.']))
