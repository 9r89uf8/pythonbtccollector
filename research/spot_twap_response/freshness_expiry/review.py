"""Replay two freshness policies on verified saved decisions; never run live I/O.

Accepted sequences must be complete through every decision. Primary target
grading additionally requires a complete 120-second accepted-event window.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import asdict
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from hashlib import sha256
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent

D = Decimal
M = 1_000_000
MATCH_NS = 120_000_000_000
QUANTUM = D('0.000000000000000001')
EXPECTED_SHA = 'd7536bcafcf62170abbf9329f6b720d7df35fb710b9f79f64ec96ca2bda05b80'
EVENT_FIELDS = ('feed', 'value', 'source_timestamp_ms', 'received_wall_ns',
                'received_monotonic_ns', 'sequence', 'event_id', 'window_s')
CATEGORIES = ('observed', 'carried', 'pending', 'future', 'missing')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with path.open('rb') as stream:
        return sha256(stream.read()).hexdigest()


def event_record(value):
    record = {key: value[key] for key in EVENT_FIELDS}
    for key in ('received_wall_ns', 'received_monotonic_ns'):
        record[key] = int(record[key])
    record['value'] = D(record['value'])
    return PriceEvent(**record)


def nested_events(value):
    if isinstance(value, dict):
        if all(key in value for key in EVENT_FIELDS):
            yield event_record(value)
        else:
            for child in value.values():
                yield from nested_events(child)
    elif isinstance(value, list):
        for child in value:
            yield from nested_events(child)


def rows(path, offsets=None):
    with path.open('rb') as stream:
        def lines():
            if offsets is not None:
                for offset in offsets:
                    stream.seek(offset)
                    yield offset, stream.readline()
            else:
                while True:
                    offset = stream.tell()
                    raw = stream.readline()
                    if not raw:
                        return
                    yield offset, raw
        for offset, raw in lines():
            row = json.loads(raw)
            for kind in ('frozen', 'state'):
                require(sha256(row[kind+'_json'].encode()).hexdigest() == row[kind+'_sha256'],
                        'export row '+kind+' digest mismatch')
            row['_byte_offset'] = offset
            yield row, json.loads(row['frozen_json']), json.loads(row['state_json'])


def inventory(path, expected_sha):
    require(digest(path) == expected_sha, 'full export SHA-256 mismatch')
    events, runs, ids, offsets = {}, set(), set(), {}
    max_included = 0
    n = 0
    for row, frozen, state in rows(path):
        n += 1
        runs.add(row['run_id'])
        ids.add(int(row['decision_id']))
        offsets[int(row['decision_id'])] = row['_byte_offset']
        require(row['terminal'], 'nonterminal audit row')
        require(frozen['contract_version'] == 2, 'expected frozen contract 2')
        require(frozen['policy'] == dict(enabled=True, current_max_age_ms=3000,
                max_carry_ms=10000, history_ms=120000, max_events=1024), 'unexpected old policy')
        require(not frozen['gap_count'] and not frozen.get('operational_gaps')
                and not state.get('causality_invalid'), 'unsupported gap/causality fault')
        max_included = max(max_included, frozen['included_sequence'] or 0)
        for event in nested_events([frozen, state]):
            require(event.sequence not in events or events[event.sequence] == event,
                    'conflicting accepted event sequence')
            events[event.sequence] = event
    require(len(runs) == 1 and len(ids) == n and ids == set(range(1, n+1)),
            'expected one run and complete unique decision IDs')
    prefix = 0
    while prefix+1 in events:
        prefix += 1
    require(prefix >= max_included, 'incomplete accepted input prefix; replay cannot certify inputs')
    ordered = [events[k] for k in sorted(events)]
    require(all(a.received_wall_ns <= b.received_wall_ns
                and a.received_monotonic_ns <= b.received_monotonic_ns
                for a, b in zip(ordered, ordered[1:])), 'accepted receipt order mismatch')
    info = dict(rows=n, run_id=next(iter(runs)), unique_events=len(events),
                events_by_feed=dict(Counter(e.feed for e in events.values())),
                last_decision_included_sequence=max_included,
                complete_sequence_prefix_end=prefix,
                complete_prefix_received_monotonic_ns=events[prefix].received_monotonic_ns,
                complete_prefix_received_wall_ns=events[prefix].received_wall_ns,
                max_exported_sequence=max(events),
                missing_sequences=[k for k in range(1, max(events)+1) if k not in events],
                contradictory_sequence_versions=0)
    return events, info, [offsets[key] for key in sorted(offsets)]


def current_ok(event, wall, mono, source_ms, receipt_ms=3000):
    if event is None:
        return False
    ages = (wall-event.source_timestamp_ms*M, wall-event.received_wall_ns,
            mono-event.received_monotonic_ns)
    return (min(ages) >= 0 and event.source_timestamp_ms*M <= event.received_wall_ns
            and ages[0] <= source_ms*M and max(ages[1:]) <= receipt_ms*M)


def reference_slots(latest_spot_by_source, current_spot, anchor, wall, mono, source_ms):
    """Independent source-key lookup over admitted prefix, with the same bounds."""
    if anchor is None:
        return []
    cutoff = wall//M - 120000
    sources = sorted(stamp for stamp, event in latest_spot_by_source.items()
                     if stamp*M <= event.received_wall_ns and stamp >= cutoff-10000)
    before = [stamp for stamp in sources if stamp < cutoff]
    sources = ([before[-1]] if before else []) + [stamp for stamp in sources if stamp >= cutoff]
    output = []
    for stamp in range(anchor.source_timestamp_ms-61000, anchor.source_timestamp_ms+28000, 1000):
        if stamp*M > wall:
            event = current_spot if current_ok(current_spot, wall, mono, source_ms) else None
            output.append(dict(slot_timestamp_ms=stamp, value=None if event is None else event.value,
                               input_sequence=None if event is None else event.sequence,
                               category='missing' if event is None else 'future', carry_age_ms=None))
            continue
        index = bisect_right(sources, stamp)-1
        event = latest_spot_by_source[sources[index]] if index >= 0 else None
        age = None if event is None else stamp-event.source_timestamp_ms
        if event is None or age > 10000:
            output.append(dict(slot_timestamp_ms=stamp, value=None, input_sequence=None,
                               category='missing', carry_age_ms=age))
        else:
            output.append(dict(slot_timestamp_ms=stamp, value=event.value,
                               input_sequence=event.sequence,
                               category='observed' if age == 0 else 'pending' if stamp > sources[-1] else 'carried',
                               carry_age_ms=age))
    return output


def slot_record(slot):
    return dict(slot_timestamp_ms=slot.slot_timestamp_ms, value=slot.value,
                input_sequence=None if slot.input is None else slot.input.sequence,
                category=slot.category, carry_age_ms=slot.carry_age_ms)


def saved_slots(frozen):
    return [dict(slot, value=None if slot['value'] is None else D(slot['value']))
            for slot in frozen['slots']]


def exact_mean(values):
    require(len(values) == 60 and all(v is not None for v in values), 'mean requires 60 observed/estimated values')
    with localcontext() as context:
        context.prec = 80
        return (sum(values, D(0))/60).quantize(QUANTUM, rounding=ROUND_HALF_EVEN)


def target_result(events_by_stamp, stamp, included_sequence, wall, mono, prefix_end_mono):
    """Grade first report only after proving a complete matching/conflict window."""
    events = events_by_stamp.get(stamp, [])
    if any(e.sequence <= included_sequence for e in events):
        return 'already_received', None
    if mono+MATCH_NS > prefix_end_mono:
        return 'incomplete_target_window', None
    eligible = [e for e in events if mono <= e.received_monotonic_ns < mono+MATCH_NS]
    if not eligible:
        return 'missing_target', None
    first = eligible[0]
    if first.received_wall_ns < wall or first.source_timestamp_ms*M > first.received_wall_ns:
        return 'target_clock_anomaly', None
    if any(e.value != first.value for e in eligible):
        return 'conflicting_target', None
    return 'clean_matched', first


def distribution(values):
    if not values:
        return dict(n=0)
    values = sorted(values)
    def q(p):
        index = (len(values)-1)*D(p)
        lo = int(index)
        return values[lo]+(index-lo)*(values[min(lo+1, len(values)-1)]-values[lo])
    return dict(n=len(values), minimum=values[0], p10=q('.1'), median=q('.5'), p90=q('.9'), p99=q('.99'),
                maximum=values[-1], mean=sum(values, D(0))/len(values))


def json_text(value):
    return json.dumps(value, indent=2, default=lambda x: format(x, 'f'))+'\n'


def run(path, output, expected_sha=EXPECTED_SHA):
    require(not output.exists(), 'output directory already exists')
    engine_path = ROOT/'price_collector/ghost_twap.py'
    engine_hash = digest(engine_path)
    events, info, offsets = inventory(path, expected_sha)
    by_stamp = defaultdict(list)
    for event in sorted(events.values(), key=lambda e: e.sequence):
        if event.feed == 'twap':
            by_stamp[event.source_timestamp_ms].append(event)
    old = GhostTwapEngine(info['run_id'], GhostPolicy(enabled=True, source_max_age_ms=3000, receipt_max_age_ms=3000))
    new = GhostTwapEngine(info['run_id'], GhostPolicy(enabled=True, source_max_age_ms=5000, receipt_max_age_ms=3000))
    latest_spots, current = {}, {}
    accepted = 0
    checks = Counter()
    groups = defaultdict(lambda: dict(counts=Counter(), errors=defaultdict(list), targets=set()))
    populations = {seconds: Counter() for seconds in (0, 60, 65)}
    campaign_start = None
    # Server export identity ordering is lexical for text decision IDs. Replay
    # the verified numeric acceptance order without loading the 312MiB export.
    for row, frozen, state in rows(path, offsets):
        if campaign_start is None:
            campaign_start = frozen['runtime_policy']['canary_start_ms']
        included = frozen['included_sequence'] or 0
        wall, mono = int(frozen['decision_wall_ns']), int(frozen['decision_monotonic_ns'])
        require(included >= accepted, 'included prefix regressed')
        for sequence in range(accepted+1, included+1):
            event = events[sequence]
            require(event.received_wall_ns <= wall and event.received_monotonic_ns <= mono,
                    'future receipt in admitted input prefix')
            old.accept(event)
            new.accept(event)
            current[event.feed] = event
            if event.feed == 'spot':
                latest_spots[event.source_timestamp_ms] = event
        accepted = included
        old_decision = old.snapshot(row['decision_id'], wall, mono)
        candidate = new.snapshot(row['decision_id'], wall, mono)
        for feed in ('spot', 'twap'):
            saved = frozen['current_'+feed]
            require(current.get(feed) == (None if saved is None else event_record(saved)), 'current input parity')
        original_slots = saved_slots(frozen)
        require([slot_record(s) for s in old_decision.slots] == original_slots, 'old complete slot parity')
        require(old_decision.valid_until_wall_ns == int(frozen['valid_until_wall_ns']), 'old expiry parity')
        direct = reference_slots(latest_spots, current.get('spot'), current.get('twap'), wall, mono, 5000)
        require([slot_record(s) for s in candidate.slots] == direct, 'candidate independent source-slot parity')
        checks['decisions_replayed'] += 1
        checks['original_slots_equal'] += len(original_slots)
        checks['candidate_independent_slots_equal'] += len(direct)
        old_available, new_available = [], []
        for saved, baseline, forecast in zip(frozen['forecasts'], old_decision.forecasts, candidate.forecasts):
            h = baseline.horizon_s
            original_price = None if saved['price'] is None else D(saved['price'])
            require(baseline.price == original_price and asdict(baseline.counts) == saved['counts']
                    and baseline.quality == saved['quality'] and list(baseline.reasons) == saved['reasons']
                    and baseline.target_source_timestamp_ms == saved['target_source_timestamp_ms']
                    and baseline.slot_start_index == saved['slot_start_index'], 'old forecast/count/reason/price parity')
            checks['original_horizons_equal'] += 1
            idx = forecast.slot_start_index
            selected = [] if idx is None else direct[idx:idx+60]
            expected_counts = {name: sum(s['category'] == name for s in selected) for name in CATEGORIES}
            if not selected:
                expected_counts['missing'] = 60
            require(asdict(forecast.counts) == expected_counts, 'candidate independent category counts')
            # Preserve every non-freshness policy rejection, while independently
            # recomputing current gates and constituent completeness.
            other_issues = set(saved['reasons'])-{'stale_spot', 'stale_twap', 'missing_slots'}
            reference_available = (not other_issues and len(selected) == 60
                    and all(s['value'] is not None for s in selected)
                    and all(current_ok(current.get(feed), wall, mono, 5000) for feed in ('spot', 'twap')))
            require(reference_available == (forecast.price is not None), 'candidate independent availability')
            if forecast.price is not None:
                require(exact_mean([s['value'] for s in selected]) == forecast.price, 'candidate independent E18 mean')
                checks['candidate_independent_means_equal'] += 1
            old_available.append(baseline.price is not None)
            new_available.append(forecast.price is not None)
            require(baseline.price is None or forecast.price == baseline.price, 'candidate changed existing price')
            scopes = ['all_saved_decisions']
            if row['created_ms'] >= campaign_start+65000:
                scopes.append('after_65_seconds')
            if forecast.price is None:
                for scope in scopes:
                    groups[scope, h, 'still_unavailable']['counts']['decisions'] += 1
                continue
            cohort = 'newly_eligible' if baseline.price is None else 'existing_intersection'
            status, target = target_result(by_stamp, forecast.target_source_timestamp_ms, included,
                    wall, mono, info['complete_prefix_received_monotonic_ns'])
            for scope in scopes:
                group = groups[scope, h, cohort]
                group['counts']['decisions'] += 1
                group['counts']['old_publication_'+state['publication']['status']] += 1
                group['counts'][status] += 1
            if target is None:
                continue
            actual = target.value
            error = forecast.price-actual
            persistence = current['twap'].value-actual
            metrics = dict(ghost_abs_usd=abs(error), persistence_abs_usd=abs(persistence),
                           ghost_abs_bp=abs(error)/actual*10000,
                           persistence_abs_bp=abs(persistence)/actual*10000,
                           ghost_signed_bp=error/actual*10000,
                           target_receipt_after_decision_s=D(target.received_monotonic_ns-mono)/1_000_000_000,
                           target_source_minus_decision_s=D(target.source_timestamp_ms*M-wall)/1_000_000_000)
            for scope in scopes:
                group = groups[scope, h, cohort]
                for key, value in metrics.items():
                    group['errors'][key].append(value)
                group['counts']['ghost_strictly_better'] += abs(error) < abs(persistence)
                group['counts']['equal_absolute_error'] += abs(error) == abs(persistence)
                group['counts']['target_source_at_or_before_decision'] += target.source_timestamp_ms*M <= wall
                group['counts']['target_receipt_strictly_after_decision'] += target.received_monotonic_ns > mono
                group['targets'].add(forecast.target_source_timestamp_ms)
            if baseline.price is not None:
                saved_target = state['targets'][str(h)]
                require(saved_target['status'] == 'matched'
                        and event_record(saved_target['first_event']) == target
                        and D(saved_target['error']) == error
                        and D(saved_target['persistence_error']) == persistence,
                        'old target/persisted error parity')
                checks['original_persisted_target_errors_equal'] += 1
        for seconds, counts in populations.items():
            if row['created_ms'] >= campaign_start+seconds*1000:
                counts['decisions'] += 1
                counts['original_any_unavailable'] += not all(old_available)
                counts['candidate_any_unavailable'] += not all(new_available)
                counts['recovered_all_horizons'] += not all(old_available) and all(new_available)
    require(digest(engine_path) == engine_hash, 'engine changed during replay; rerun against frozen code')
    tables = []
    for (scope, h, cohort), group in sorted(groups.items()):
        n = group['counts']['clean_matched']
        tables.append(dict(scope=scope, horizon_s=h, cohort=cohort, counts=dict(group['counts']),
                           distinct_scored_target_stamps=len(group['targets']),
                           ghost_better_percent=None if not n else D(group['counts']['ghost_strictly_better'])*100/n,
                           metrics={key: distribution(values) for key, values in group['errors'].items()}))
    result = dict(status='passed', input_path=str(path.resolve()), input_sha256=expected_sha,
                  input_bytes=path.stat().st_size, reconstruction=info,
                  policies=dict(original=asdict(old.policy), candidate=asdict(new.policy)),
                  checks=dict(checks), mismatch_count=0, populations=populations, cohorts=tables,
                  provenance=dict(script_sha256=digest(Path(__file__)), engine_sha256=engine_hash),
                  target_grading='First exact source report in [decision, decision+120s); entire accepted-event window must be inside complete prefix; no prior target or conflicting value in that window.',
                  limitations=[
                      'Fixed saved decision times; candidate expiry scheduling, publications and live coverage are not simulated.',
                      'Candidate forecasts were not published and receive no publication or lead credit.',
                      'Accepted sequence coverage proves completeness only within this recorded collector acceptance sequence, not upstream completeness or clock truth.',
                      'Postdecision missing accepted sequences prevent complete target-window grading for the end of the run; these cases remain explicit exclusions.',
                      'Existing and newly eligible strata occur in different conditions; their error distributions are descriptive, not a randomized policy comparison.',
                      'Overlapping forecasts reuse targets; no independence, significance, profitability or browser claim.',
                      'The full 120-second window is a conservative common evidence rule; it is stricter than merely observing a clean first target in stored runtime state.'])
    output.mkdir(parents=True)
    (output/'summary.json').write_text(json_text(result), encoding='utf-8')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('export', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--expected-sha256', default=EXPECTED_SHA)
    args = parser.parse_args()
    with localcontext() as context:
        context.prec = 80
        context.rounding = ROUND_HALF_EVEN
        result = run(args.export, args.output, args.expected_sha256)
    print(json_text({key: result[key] for key in ('status', 'reconstruction', 'checks', 'populations')}))


if __name__ == '__main__':
    main()
