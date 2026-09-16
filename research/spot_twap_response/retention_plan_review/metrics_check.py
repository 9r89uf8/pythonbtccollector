"""Independent Decimal metrics from the immutable reliability-canary audit.

No production or historical scorer imports. This checks selection/arithmetic,
not a second reconstruction of every original sixty-slot forecast.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path

RUN = 'fe2dd06da5754c50b696cb9419ca7251'
SHA = 'f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954'
BYTES, ROWS = 1078545445, 23411
START = 1789512998000 * 1000000
STOP = 1789516598428617010
SECOND = 1000000000
HORIZONS = (1, 2, 3, 5, 10, 30)
PANELS = ('all_decisions', 'complete_120s_window', 'after_65s_complete_120s_window')
COHORTS = ('acknowledged_matched_valid', 'confirmed_early_matched_valid')


def require(value, message):
    if not value:
        raise ValueError(message)


def reject(value):
    raise ValueError('Unexpected JSON float/nonfinite: ' + value)


def decode(raw):
    return json.loads(raw, parse_float=reject, parse_constant=reject)


def price(value):
    require(isinstance(value, str), 'Price is not a string')
    result = Decimal(value)
    require(result.is_finite() and result > 0, 'Invalid price')
    return result


def summary(pairs):
    # error, baseline error, actual, target stamp
    n = len(pairs)
    if not n:
        return {'n': 0}
    errors = [x[0] for x in pairs]
    ghost = [abs(x[0]) / x[2] * 10000 for x in pairs]
    held = [abs(x[1]) / x[2] * 10000 for x in pairs]
    total_g, total_h = sum(ghost), sum(held)
    better = sum(abs(x[0]) < abs(x[1]) for x in pairs)
    equal = sum(abs(x[0]) == abs(x[1]) for x in pairs)
    result = dict(n=n, distinct_target_stamps=len({x[3] for x in pairs}),
        overpredicting_n=sum(x > 0 for x in errors), exact_n=sum(x == 0 for x in errors),
        better_n=better, equal_n=equal, worse_n=n-better-equal,
        signed_mean_usd=str(sum(errors) / n),
        overpredicting_fraction=str(Decimal(sum(x > 0 for x in errors)) / n),
        ghost_mae_usd=str(sum(abs(x[0]) for x in pairs) / n),
        held_mae_usd=str(sum(abs(x[1]) for x in pairs) / n),
        ghost_mae_bps=str(total_g / n), held_mae_bps=str(total_h / n),
        ratio_bps=None if not total_h else str(total_g / total_h),
        better_fraction=str(Decimal(better) / n),
        paired_excess_mae_bps=str((total_g-total_h) / n))
    return result


def analyze(source, reference):
    digest, count, size, chosen = sha256(), 0, 0, 0
    seen, publication = set(), Counter()
    buckets = defaultdict(list)
    exclusions = Counter()
    with localcontext() as context:
        context.prec = 80
        with source.open('rb') as stream:
            while True:
                raw = stream.readline(1048577)
                if not raw:
                    break
                require(len(raw) <= 1048576 and raw.endswith(b'\n'), 'Invalid line')
                digest.update(raw); size += len(raw); count += 1
                row = decode(raw)
                identity = row['run_id'], row['decision_id']
                require(identity not in seen, 'Duplicate decision identity')
                seen.add(identity)
                for field in ('frozen', 'state'):
                    require(sha256(row[field+'_json'].encode()).hexdigest() == row[field+'_sha256'],
                            'Row hash mismatch')
                if row['run_id'] != RUN:
                    continue
                chosen += 1
                frozen, state = decode(row['frozen_json']), decode(row['state_json'])
                require(row['terminal'] is True, 'Nonterminal canary record')
                require((frozen['run_id'], frozen['decision_id']) == identity, 'Frozen identity')
                require(frozen['runtime_version'] == 'ghost-canary-v6' and frozen['contract_version'] == 4,
                        'Unexpected runtime/contract')
                require(frozen['runtime_policy']['canary_start_ms'] * 1000000 == START, 'Campaign start')
                wall, mono = int(frozen['decision_wall_ns']), int(frozen['decision_monotonic_ns'])
                require(START <= wall < STOP, 'Decision outside campaign')
                panels = ['all_decisions']
                if wall + 120*SECOND <= STOP:
                    panels.append('complete_120s_window')
                    if wall >= START + 65*SECOND:
                        panels.append('after_65s_complete_120s_window')
                quarter = min(3, (wall-START)//(900*SECOND))
                pub = state['publication']; publication[pub['status']] += 1
                if pub['status'] != 'acknowledged':
                    continue
                wire = decode(pub['payload_json'])
                require((wire['run_id'], wire['decision_id']) == identity, 'Wire identity')
                selection = wire['publication_eligibility']
                require(selection['version'] == 1, 'Unknown membership policy')
                eligible = selection['eligible_horizons']
                require(len(set(eligible)) == len(eligible) and set(eligible) <= set(HORIZONS), 'Invalid mask')
                require(set(selection['excluded_horizons']) == {str(h) for h in HORIZONS if h not in eligible},
                        'Mask partition')
                attempt = int(pub['attempt_monotonic_ns']); ack = int(pub['ack_monotonic_ns'])
                require(int(selection['checked_monotonic_ns']) == attempt and
                        int(selection['checked_wall_ns']) == int(pub['attempt_wall_ns']), 'Selection clocks')
                require(mono <= int(state['computation_completed_monotonic_ns']) <=
                        int(pub['intent_monotonic_ns']) <= attempt <= ack, 'Publication clock order')
                deadline = int(frozen['valid_until_wall_ns'])
                require(min(deadline-int(pub['attempt_wall_ns']), deadline-wall-(attempt-mono)) >= 1000000,
                        'Attempt outside usable expiry')
                require(int(pub['attempt_wall_ns']) < START+3600*SECOND, 'Attempt after planned end')
                require([f['horizon_s'] for f in frozen['forecasts']] == list(HORIZONS) and
                        [f['horizon_s'] for f in wire['forecasts']] == list(HORIZONS), 'Horizon domain')
                for forecast, sent in zip(frozen['forecasts'], wire['forecasts']):
                    h = forecast['horizon_s']; target = state['targets'][str(h)]
                    if h not in eligible:
                        exclusions['ineligible'] += 1
                        continue
                    require(sent == forecast and forecast['price'] is not None and
                            forecast['quality'] in ('healthy', 'degraded'), 'Unissued forecast credit')
                    first = target.get('first_event')
                    if first is None:
                        exclusions['no_first_event_'+target['status']] += 1
                        continue
                    require(target['status'] == 'matched' and first['feed'] == 'twap' and first['window_s'] == 60,
                            'Target identity')
                    require(first['source_timestamp_ms'] == forecast['target_source_timestamp_ms'] ==
                            target['target_source_timestamp_ms'], 'Exact target stamp')
                    received, received_wall = int(first['received_monotonic_ns']), int(first['received_wall_ns'])
                    require(mono <= received < mono+120*SECOND and received_wall >= wall and
                            attempt <= received, 'Target outside matching/selection window')
                    anomaly = first['source_timestamp_ms']*1000000 > received_wall
                    require(target.get('clock_anomaly') is anomaly, 'Target clock flag')
                    valid = not (target['conflicted'] or anomaly or state.get('causality_invalid'))
                    if not valid:
                        exclusions['invalid_target'] += 1
                        continue
                    actual, predicted, baseline = price(first['value']), price(forecast['price']), price(frozen['current_twap']['value'])
                    error, held_error = predicted-actual, baseline-actual
                    require(Decimal(target['error']) == error and Decimal(target['persistence_error']) == held_error,
                            'Stored error mismatch')
                    expected_lead = received-ack if ack < received else None
                    require((None if target['confirmed_redis_lead_ns'] is None else int(target['confirmed_redis_lead_ns'])) == expected_lead,
                            'Recorded lead mismatch')
                    cohorts = ['acknowledged_matched_valid']
                    if expected_lead is not None:
                        cohorts.append('confirmed_early_matched_valid')
                    pair = error, held_error, actual, first['source_timestamp_ms']
                    for panel in panels:
                        for cohort in cohorts:
                            buckets[panel,cohort,h,'all'].append(pair)
                            buckets[panel,cohort,h,quarter].append(pair)
        require((digest.hexdigest(), count, size) == (SHA, ROWS, BYTES), 'Immutable export mismatch')
        require(chosen == 7082, 'Wrong selected count')
        output = {}
        checks = []
        for panel in PANELS:
            output[panel] = {}
            for cohort in COHORTS:
                items = []
                for h in HORIZONS:
                    value = summary(buckets[panel,cohort,h,'all'])
                    original = next(x for x in reference['panels'][panel]['horizons'] if x['horizon_s'] == h)['errors'][cohort]
                    for key, other in [('n','n'),('better_n','better'),('equal_n','equal'),('worse_n','worse')]:
                        require(value[key] == original[other], 'Reference cohort mismatch')
                    checks.append({'panel':panel,'cohort':cohort,'horizon_s':h,'n':value['n'],'counts_match_reference':True})
                    items.append({'horizon_s':h, **value, 'quarter_hours':[
                        {'quarter':q+1,'elapsed_start_s':q*900,'elapsed_end_s':(q+1)*900,
                         **summary(buckets[panel,cohort,h,q])} for q in range(4)]})
                output[panel][cohort] = items
        return {'status':'accepted','source':{'path':str(source.resolve()),'sha256':digest.hexdigest(),
                'rows':count,'bytes':size},'run_id':RUN,'selected_decisions':chosen,
            'publication_statuses':dict(publication),'acknowledged_horizon_exclusions':dict(exclusions),
            'panels':output,'reference_count_checks':checks,
            'method':{'error':'forecast minus exact first official target; baseline holds original current TWAP',
                'bps':'each absolute dollar error / its actual target price * 10000; average those bps',
                'ratio':'sum absolute forecast error in bps / sum paired baseline absolute error in bps',
                'overpredicting':'strictly positive signed error / all matched pairs; exact zeros remain denominator',
                'better':'strictly smaller absolute error than baseline; ties remain denominator',
                'quarter_hours':'decision time in four fixed 900-second blocks beginning at declared campaign start',
                'cohorts':'separate full-run, complete120s and after65s+complete120s; each with ACK valid and strict-early valid pairs',
                'independence':'stdlib Decimal precision80; no scorer/runtime imports; no repeated slot arithmetic validation',
                'limits':['One hour cannot establish persistent absence of bias or calibrate hourly versus daily alarm reliability.',
                    'Quarter-hour records overlap and share target stamps; sample counts are not independent trials.',
                    'Unreceived targets are not proof that the provider never published them.']}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Refusing overwrite')
    code_hash = sha256(Path(__file__).read_bytes()).hexdigest()
    reference_bytes = args.reference.read_bytes()
    result = analyze(args.input, decode(reference_bytes))
    require(code_hash == sha256(Path(__file__).read_bytes()).hexdigest(), 'Code changed during run')
    result['code_sha256'] = code_hash
    result['reference_sha256'] = sha256(reference_bytes).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({'status':result['status'],'selected_decisions':result['selected_decisions'],
                      'output':str(args.output)}))


if __name__ == '__main__':
    main()
