"""Bounded recorded incident regression; one streaming pass of the local audit.

Outputs a raw fixture outside Git and small manifest/results at unused paths.
Replays received input prefixes at original decision clocks, with no publication
simulation. Policy zero must reproduce original frozen prices and slot counts.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent, MATH_CONTEXT


VERSION = 'recorded-spot-reconnect-v1'
RUN = '9b3cf7d21f9640de989062ddd4001d5b'
CENTER_MS = 1789426057919  # Recorded reconnect-scheduled journal timestamp, floored.
START_MS, END_MS = CENTER_MS-120_000, CENTER_MS+90_000
WARMUP_MS = 65_000


def event_record(value):
    if value is None:
        return None
    price = Decimal(value['value'])
    if not price.is_finite() or price <= 0:
        raise ValueError('invalid event price')
    result = {key: value[key] for key in ('feed', 'event_id')}
    result.update(value=format(price, '.18f'), window_s=value.get('window_s'))
    for key in ('source_timestamp_ms', 'received_wall_ns', 'received_monotonic_ns', 'sequence'):
        result[key] = int(value[key])
    if value.get('received_ms', result['received_wall_ns']//1_000_000) != result['received_wall_ns']//1_000_000:
        raise ValueError('inconsistent receive milliseconds')
    return result


def gap_record(value):
    return dict(feed=value['feed'], reason=value['reason'], after_sequence=int(value['after_sequence']),
                observed_wall_ns=int(value['observed_wall_ns']), observed_monotonic_ns=int(value['observed_monotonic_ns']))


def gap_key(value):
    return (value['after_sequence'], value['observed_monotonic_ns'], value['feed'], value['reason'])


def write_new(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def extract(path, expected_sha256, expected_rows):
    events, gaps, decisions, policies = {}, {}, [], []
    digest, row_count = sha256(), 0
    def retain(value):
        event = event_record(value)
        if event is None or not START_MS*1_000_000 <= event['received_wall_ns'] <= END_MS*1_000_000:
            return
        previous = events.setdefault(event['sequence'], event)
        if previous != event:
            raise ValueError('conflicting observations for one global sequence')
    with path.open('rb') as stream:
        for raw in stream:
            digest.update(raw)
            row_count += 1
            row = json.loads(raw)
            if row['run_id'] != RUN:
                continue
            frozen, state = json.loads(row['frozen_json']), json.loads(row['state_json'])
            retain(frozen.get('current_spot'))
            retain(frozen.get('current_twap'))
            for event in frozen['slot_inputs']:
                retain(event)
            for target in state['targets'].values():
                for field in ('first_event', 'first_conflicting_event', 'first_late_event'):
                    retain(target.get(field))
            observed_gaps = []
            for raw_gap in frozen.get('operational_gaps', []):
                gap = gap_record(raw_gap)
                if START_MS*1_000_000 <= gap['observed_wall_ns'] <= END_MS*1_000_000:
                    key = gap_key(gap)
                    if gaps.setdefault(key, gap) != gap:
                        raise ValueError('conflicting gap evidence')
                    observed_gaps.append(gap)
            if START_MS <= row['created_ms'] <= END_MS:
                decisions.append(dict(decision_id=frozen['decision_id'],
                    decision_wall_ns=int(frozen['decision_wall_ns']),
                    decision_monotonic_ns=int(frozen['decision_monotonic_ns']),
                    included_sequence=frozen['included_sequence'], forecasts=frozen['forecasts'],
                    operational_gaps=observed_gaps))
                policies.append(frozen['policy'])
    if digest.hexdigest() != expected_sha256 or row_count != expected_rows:
        raise ValueError('full source audit hash/count mismatch')
    if not events or not decisions or any(policy != policies[0] for policy in policies):
        raise ValueError('missing or mixed-policy incident evidence')
    decisions.sort(key=lambda row: (row['decision_monotonic_ns'], int(row['decision_id'])))
    ordered = [events[key] for key in sorted(events)]
    missing = [seq for seq in range(ordered[0]['sequence'], ordered[-1]['sequence']+1) if seq not in events]
    return dict(version=VERSION, source_audit_sha256=digest.hexdigest(), source_rows=row_count,
        run_id=RUN, receipt_start_ms=START_MS, receipt_end_ms=END_MS, warmup_ms=WARMUP_MS,
        policy=policies[0], events=ordered, gaps=sorted(gaps.values(), key=gap_key), decisions=decisions,
        missing_sequences=missing)


def price_event(value):
    return PriceEvent(feed=value['feed'], value=Decimal(value['value']),
        source_timestamp_ms=value['source_timestamp_ms'], received_wall_ns=value['received_wall_ns'],
        received_monotonic_ns=value['received_monotonic_ns'], sequence=value['sequence'],
        event_id=value['event_id'], window_s=value['window_s'])


def replay(fixture):
    if fixture['missing_sequences']:
        return dict(status='blocked', reason='noncontiguous_input_sequence', missing_sequences=fixture['missing_sequences'])
    policy = dict(fixture['policy'])
    policy.pop('spot_reconnect_max_gap_ms', None)
    engines = {limit: GhostTwapEngine(RUN, GhostPolicy(**policy, spot_reconnect_max_gap_ms=limit))
               for limit in (0, 10000)}
    events = fixture['events']
    by_source = defaultdict(list)
    for event in events:
        if event['feed'] == 'twap':
            by_source[event['source_timestamp_ms']].append(event)
    position, applied, measured, parity = 0, set(), [], 0
    for row in fixture['decisions']:
        included = row['included_sequence']
        if included is None:
            continue
        pending = sorted((gap for gap in row['operational_gaps'] if gap_key(gap) not in applied), key=gap_key)
        def apply(gap):
            if gap['observed_wall_ns'] > row['decision_wall_ns'] or gap['observed_monotonic_ns'] > row['decision_monotonic_ns']:
                raise ValueError('gap appears before observation')
            for engine in engines.values():
                engine.record_gap(gap['feed'], gap['reason'], observed_wall_ns=gap['observed_wall_ns'],
                                  observed_monotonic_ns=gap['observed_monotonic_ns'])
            applied.add(gap_key(gap))
        while position < len(events) and events[position]['sequence'] <= included:
            while pending and pending[0]['after_sequence'] < events[position]['sequence']:
                apply(pending.pop(0))
            event = price_event(events[position])
            for engine in engines.values():
                engine.accept(event)
            position += 1
        for gap in pending:
            if gap['after_sequence'] > included:
                raise ValueError('gap ordering exceeds included prefix')
            apply(gap)
        snapshots = {limit: engine.snapshot(row['decision_id'], row['decision_wall_ns'], row['decision_monotonic_ns'])
                     for limit, engine in engines.items()}
        if row['decision_wall_ns'] < (START_MS+WARMUP_MS)*1_000_000:
            continue
        if position == 0 or events[position-1]['sequence'] != included:
            raise ValueError('measured cutoff lacks exact included prefix')
        for expected, old in zip(row['forecasts'], snapshots[0].forecasts):
            expected_price = None if expected['price'] is None else Decimal(expected['price'])
            if (expected['horizon_s'] != old.horizon_s or expected_price != old.price
                    or expected['counts'] != asdict(old.counts) or expected['quality'] != old.quality
                    or expected['target_source_timestamp_ms'] != old.target_source_timestamp_ms):
                raise ValueError('policy-zero parity failure at decision '+row['decision_id']+' horizon '+str(old.horizon_s))
            parity += 1
        measured.append((row, snapshots))
    summary = {}
    spot_gaps = [gap for gap in fixture['gaps'] if gap['feed'] == 'spot' and gap['reason'] == 'connection_end']
    if len(spot_gaps) != 1:
        raise ValueError('expected exactly one recorded spot reconnect incident')
    gap_wall = spot_gaps[0]['observed_wall_ns']
    for horizon in (1, 2, 3, 5, 10, 30):
        counts = dict(decisions=len(measured), old_available=0, new_available=0, regained=0,
                      lost=0, both_available_changed_price=0, regained_matched=0,
                      regained_missing_target=0, regained_conflicted_target=0, regained_target_clock_invalid=0)
        examples, errors, dollar_errors, first_available = [], [], [], {}
        for row, snapshots in measured:
            old = next(f for f in snapshots[0].forecasts if f.horizon_s == horizon)
            new = next(f for f in snapshots[10000].forecasts if f.horizon_s == horizon)
            counts['old_available'] += old.price is not None
            counts['new_available'] += new.price is not None
            counts['lost'] += old.price is not None and new.price is None
            counts['both_available_changed_price'] += old.price is not None and new.price is not None and old.price != new.price
            if row['decision_wall_ns'] >= gap_wall:
                for label, forecast in (('old', old), ('new', new)):
                    if forecast.price is not None and label not in first_available:
                        first_available[label] = {key: row[key] for key in
                            ('decision_id', 'decision_wall_ns', 'decision_monotonic_ns')}
            if old.price is not None or new.price is None:
                continue
            counts['regained'] += 1
            targets = by_source.get(new.target_source_timestamp_ms, [])
            example = dict(decision_id=row['decision_id'], decision_wall_ns=row['decision_wall_ns'],
                target_source_timestamp_ms=new.target_source_timestamp_ms, price=format(new.price, '.18f'),
                quality=new.quality, counts=asdict(new.counts))
            if not targets:
                counts['regained_missing_target'] += 1
            elif len({Decimal(target['value']) for target in targets}) != 1:
                counts['regained_conflicted_target'] += 1
            else:
                first = targets[0]
                if (first['received_monotonic_ns'] < row['decision_monotonic_ns']
                        or first['source_timestamp_ms']*1_000_000 > first['received_wall_ns']):
                    counts['regained_target_clock_invalid'] += 1
                else:
                    counts['regained_matched'] += 1
                    with localcontext(MATH_CONTEXT):
                        dollar_error = new.price-Decimal(first['value'])
                        error_bps = dollar_error/Decimal(first['value'])*Decimal(10000)
                    errors.append(error_bps.copy_abs())
                    dollar_errors.append(dollar_error.copy_abs())
                    example.update(recorded_target=first, error_bps=str(error_bps))
            if len(examples) < 3:
                examples.append(example)
        with localcontext(MATH_CONTEXT):
            counts['regained_mean_absolute_error_bps'] = str(sum(errors, Decimal(0))/Decimal(len(errors))) if errors else None
            counts['regained_max_absolute_error_bps'] = str(max(errors)) if errors else None
            ordered = sorted(dollar_errors)
            n = len(ordered)
            median = (ordered[n//2] if n % 2 else (ordered[n//2-1]+ordered[n//2])/Decimal(2)) if n else None
            p90 = ordered[(9*n+9)//10-1] if n else None
            counts['regained_median_absolute_error_usd'] = str(median) if median is not None else None
            counts['regained_p90_absolute_error_usd'] = str(p90) if p90 is not None else None
            if 'old' in first_available and 'new' in first_available:
                counts['first_availability_cutoff_gain_wall_ms'] = str(Decimal(first_available['old']['decision_wall_ns']-first_available['new']['decision_wall_ns'])/Decimal(1_000_000))
                counts['first_availability_cutoff_gain_monotonic_ms'] = str(Decimal(first_available['old']['decision_monotonic_ns']-first_available['new']['decision_monotonic_ns'])/Decimal(1_000_000))
        counts['first_available_cutoffs_after_gap'] = first_available
        counts['examples'] = examples
        summary[str(horizon)] = counts
    return dict(status='completed', version=VERSION, measured_decisions=len(measured),
        exact_policy_zero_forecast_checks=parity, first_measured_decision=measured[0][0]['decision_id'],
        last_measured_decision=measured[-1][0]['decision_id'], horizons=summary,
        recorded_spot_gap=spot_gaps[0], error_quantiles='Median averages the two center values when even; p90 is nearest rank ceil(0.9*n). Decimal context precision80.',
        limitations=['One recorded reconnect incident; not a study-wide coverage estimate.',
            'Original cutoffs and received input prefixes; no Redis scheduling, acknowledgement or lead simulation.',
            'Newly available price errors are conditional on recorded earliest TWAP source matches inside the bounded receipt interval.',
            'Missing/conflicting/clock-invalid targets stay ungraded; later conflicts beyond the interval are outside this fixture.',
            'Error rows are decision-horizon observations; repeated source targets are correlated, not independent trials.',
            'Earlier first availability compares calculation cutoffs only, not actual publication or cache coverage.',
            'Global input sequences are preserved; first 65 seconds are warmup, and old-policy price/count parity is required.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--input', type=Path)
    source.add_argument('--fixture-input', type=Path)
    parser.add_argument('--expected-sha256')
    parser.add_argument('--expected-rows', type=int)
    parser.add_argument('--expected-fixture-sha256')
    parser.add_argument('--fixture-output', type=Path)
    parser.add_argument('--result-output', type=Path, required=True)
    parser.add_argument('--manifest-output', type=Path, required=True)
    args = parser.parse_args()
    if args.input and (not args.expected_sha256 or args.expected_rows is None or args.fixture_output is None):
        parser.error('audit input requires expected hash/count and a new fixture output')
    if args.fixture_input and (not args.expected_fixture_sha256 or args.fixture_output is not None):
        parser.error('fixture input requires its expected hash and no fixture output')
    paths = tuple(p for p in (args.fixture_output, args.result_output, args.manifest_output) if p is not None)
    if len({p.resolve() for p in paths}) != len(paths) or any(p.exists() for p in paths):
        raise FileExistsError('all outputs must be distinct unused paths')
    if args.input:
        fixture = extract(args.input, args.expected_sha256, args.expected_rows)
        write_new(args.fixture_output, fixture)
        fixture_path = args.fixture_output
    else:
        raw = args.fixture_input.read_bytes()
        if sha256(raw).hexdigest() != args.expected_fixture_sha256:
            raise ValueError('saved fixture hash mismatch')
        fixture = json.loads(raw)
        if fixture['version'] != VERSION or fixture['run_id'] != RUN:
            raise ValueError('unexpected recorded fixture identity')
        fixture_path = args.fixture_input
    result = replay(fixture)
    write_new(args.result_output, result)
    manifest = dict(version=VERSION, source_audit_sha256=fixture['source_audit_sha256'], source_rows=fixture['source_rows'],
        run_id=RUN, receipt_start_ms=START_MS, receipt_end_ms=END_MS, warmup_ms=WARMUP_MS,
        events=len(fixture['events']), decisions=len(fixture['decisions']), gaps=fixture['gaps'],
        first_sequence=fixture['events'][0]['sequence'], last_sequence=fixture['events'][-1]['sequence'],
        missing_sequences=fixture['missing_sequences'], status=result['status'],
        files={str(p): dict(sha256=sha256(p.read_bytes()).hexdigest(), bytes=p.stat().st_size)
               for p in (fixture_path, args.result_output, Path(__file__))})
    write_new(args.manifest_output, manifest)
    print(json.dumps(dict(status=result['status'], events=manifest['events'], missing=fixture['missing_sequences'],
                         decisions=len(fixture['decisions']), measured=result.get('measured_decisions'),
                         horizons=result.get('horizons')), sort_keys=True))


if __name__ == '__main__':
    main()
