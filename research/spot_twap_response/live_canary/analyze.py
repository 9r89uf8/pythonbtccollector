"""Offline Decimal summary of a verified ghost canary export; no production I/O."""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from hashlib import sha256
import json
from pathlib import Path

D = Decimal
START_MS = 1789346720328
END_MS = START_MS + 3600000
HORIZONS = (1, 2, 3, 5, 10, 30)

def quantile(values, p):
    values = sorted(D(v) for v in values)
    if not values:
        return None
    index = (len(values) - 1) * D(p)
    lo = int(index)
    fraction = index - lo
    return values[lo] + fraction * (values[min(lo+1, len(values)-1)]-values[lo])

def stats(values):
    if not values:
        return {'n': 0}
    return dict(n=len(values), min=min(values), p10=quantile(values, '.1'),
                p50=quantile(values, '.5'), p90=quantile(values, '.9'),
                p99=quantile(values, '.99'), max=max(values),
                mean=sum(map(D, values), D(0))/len(values))

def serial(value):
    if isinstance(value, D):
        return format(value, 'f')
    raise TypeError(type(value).__name__)

def analyze(path):
    digest = sha256()
    pub_counts = Counter()
    horizon = {h: dict(quality_issued=Counter(), quality_published=Counter(),
                       targets_issued=Counter(), targets_published=Counter(),
                       reasons=Counter(), slots=defaultdict(list), carry_max_ms=[],
                       cohorts=defaultdict(lambda: defaultdict(list)),
                       distinct_targets=defaultdict(set), first_available_ms=None,
                       first_published_available_ms=None, boundary_crossing_paired=0,
                       target_flags=Counter()) for h in HORIZONS}
    timings = defaultdict(list)
    timing_invalid = Counter()
    runtime_counter_max = Counter()
    all_gaps = set()
    all_suspensions = set()
    rows, terminal, run_ids = 0, 0, set()
    decision_ids = set()
    min_created, max_created = None, None
    events = {}
    issued_sequences = []
    body_sizes = []
    serialized_sizes = []
    available_ack_count = 0
    for raw in path.open('rb'):
        digest.update(raw)
        row = json.loads(raw)
        f, s = json.loads(row['frozen_json']), json.loads(row['state_json'])
        rows += 1
        terminal += row['terminal']
        run_ids.add(row['run_id'])
        decision_ids.add(int(row['decision_id']))
        created = row['created_ms']
        min_created = created if min_created is None else min(min_created, created)
        max_created = created if max_created is None else max(max_created, created)
        serialized_sizes.append(len(raw))
        pub = s['publication']
        status = pub['status']
        pub_counts[status] += 1
        acknowledged = status == 'acknowledged'
        dm = int(f['decision_monotonic_ns'])
        dw = int(f['decision_wall_ns'])
        seq = f['included_sequence']
        issued_sequences.append((dm, seq))
        for k, v in f.get('runtime_counters', {}).items():
            runtime_counter_max[k] = max(runtime_counter_max[k], v)
        for gap in f.get('operational_gaps', []):
            all_gaps.add(json.dumps(gap, sort_keys=True))
        if f.get('runtime_suspension_history'):
            all_suspensions.add(json.dumps(f['runtime_suspension_history'], sort_keys=True))
        inputs = [e for e in [f['current_spot'], f['current_twap']] if e]
        for event in inputs + f['slot_inputs']:
            events[event['sequence']] = event
        if acknowledged:
            body_sizes.append(len(pub['payload_json'].encode()))
            available_ack_count += any(x['price'] is not None for x in f['forecasts'])
            selected = [e for e in inputs if e['sequence'] == seq]
            if len(selected) != 1:
                timing_invalid['latest_included_event_not_current_input'] += 1
            else:
                rm = int(selected[0]['received_monotonic_ns'])
                cm = int(s['computation_completed_monotonic_ns'])
                pm = int(pub['attempt_monotonic_ns'])
                am = int(pub['ack_monotonic_ns'])
                stages = dict(receipt_to_decision=dm-rm, calculation=cm-dm,
                              completion_to_attempt=pm-cm, redis_attempt_to_ack=am-pm,
                              decision_to_ack=am-dm, receipt_to_ack=am-rm)
                for name, ns in stages.items():
                    if ns < 0:
                        timing_invalid[name] += 1
                    else:
                        timings[name+'_ms'].append(D(ns)/1000000)
        for forecast in f['forecasts']:
            h = forecast['horizon_s']
            item = horizon[h]
            t = s['targets'][str(h)]
            item['quality_issued'][forecast['quality']] += 1
            item['targets_issued'][t['status']] += 1
            item['reasons'].update(forecast['reasons'])
            if forecast['price'] is not None:
                item['first_available_ms'] = min(item['first_available_ms'] or created, created)
            if acknowledged:
                item['quality_published'][forecast['quality']] += 1
                item['targets_published'][t['status']] += 1
                if forecast['price'] is not None:
                    item['first_published_available_ms'] = min(item['first_published_available_ms'] or created, created)
                    for category, count in forecast['counts'].items():
                        item['slots'][category].append(count)
                    item['carry_max_ms'].append(forecast['max_interior_carry_ms'])
            for flag in ('conflicted', 'clock_anomaly', 'first_late_event', 'first_conflicting_event'):
                if t.get(flag): item['target_flags'][flag] += 1
            if s.get('causality_invalid'): item['target_flags']['causality_invalid'] += 1
            event = t.get('first_event')
            if event:
                events[event['sequence']] = event
            clean = (forecast['price'] is not None and t['status']=='matched'
                     and event is not None and not t.get('conflicted')
                     and not t.get('clock_anomaly') and not s.get('causality_invalid')
                     and event['source_timestamp_ms']==forecast['target_source_timestamp_ms']
                     and dm <= int(event['received_monotonic_ns']) < dm+120000000000)
            if not clean:
                continue
            actual = D(event['value'])
            error = D(forecast['price'])-actual
            persistence = D(f['current_twap']['value'])-actual
            assert error == D(t['error']) and persistence == D(t['persistence_error'])
            early = (acknowledged and int(pub['ack_monotonic_ns']) < int(event['received_monotonic_ns']))
            panels = ['all_available_clean_matched']
            if acknowledged:
                panels.append('published_clean_matched')
                if forecast['market']['market_id'] != f['current_twap']['source_timestamp_ms']//300000:
                    item['boundary_crossing_paired'] += 1
            if early:
                panels.append('confirmed_early')
            for panel in panels:
                metrics = item['cohorts'][panel]
                metrics['ghost_abs_error_bp'].append(abs(error)/actual*10000)
                metrics['ghost_signed_error_bp'].append(error/actual*10000)
                metrics['persistence_abs_error_bp'].append(abs(persistence)/actual*10000)
                metrics['ghost_abs_error_usd'].append(abs(error))
                metrics['ghost_better'].append(int(abs(error)<abs(persistence)))
                metrics['target_after_decision_s'].append(D(int(event['received_monotonic_ns'])-dm)/1000000000)
                metrics['eta_error_s'].append(D(t['eta_error_ns'])/1000000000)
                metrics['eta_abs_error_s'].append(abs(D(t['eta_error_ns']))/1000000000)
                if early:
                    metrics['redis_lead_s'].append(D(t['confirmed_redis_lead_ns'])/1000000000)
                item['distinct_targets'][panel].add(event['source_timestamp_ms'])
    assert rows == len(decision_ids) and decision_ids == set(range(1, rows+1))
    assert terminal == rows
    feed_metrics = {}
    for feed in ('spot', 'twap'):
        selected = sorted((e for e in events.values() if e['feed']==feed), key=lambda e:e['sequence'])
        feed_metrics[feed] = dict(unique_observed_events=len(selected),
            source_to_receipt_ms=stats([D(int(e['received_wall_ns'])-e['source_timestamp_ms']*1000000)/1000000 for e in selected]),
            consecutive_observed_receipt_gap_ms=stats([D(int(b['received_monotonic_ns'])-int(a['received_monotonic_ns']))/1000000 for a,b in zip(selected,selected[1:])]))
    for h,item in horizon.items():
        item['cohorts'] = {name:{key:stats(values) for key,values in metrics.items()} for name,metrics in item['cohorts'].items()}
        item['distinct_targets'] = {name:len(values) for name,values in item['distinct_targets'].items()}
        item['slots'] = {k:stats(v) for k,v in item['slots'].items()}
        item['carry_max_ms'] = stats(item['carry_max_ms'])
        for key in ('first_available_ms','first_published_available_ms'):
            item[key.replace('_ms','_after_start_s')] = (D(item[key]-START_MS)/1000) if item[key] is not None else None
    issued_sequences.sort()
    return dict(export_sha256=digest.hexdigest(), export_bytes=path.stat().st_size,
                run_ids=sorted(run_ids), start_ms=START_MS, end_ms=END_MS,
                issued_decisions=rows, terminal_decisions=terminal, earliest_created_ms=min_created,
                latest_created_ms=max_created, publication_statuses=pub_counts,
                acknowledged_with_any_available=available_ack_count,
                repeated_included_sequence_adjacent_decisions=sum(a[1]==b[1] for a,b in zip(issued_sequences, issued_sequences[1:])),
                runtime_counter_max_observed=runtime_counter_max,
                distinct_operational_gaps=[json.loads(s) for s in sorted(all_gaps)],
                suspension_history_snapshots=[json.loads(s) for s in sorted(all_suspensions)],
                latency={k:stats(v) for k,v in timings.items()}, timing_invalid=timing_invalid,
                receipt_to_ack_within_10_ms=sum(x<=10 for x in timings['receipt_to_ack_ms']),
                serialized_export_row_bytes=stats(serialized_sizes),live_payload_bytes=stats(body_sizes),
                observed_feed_events=feed_metrics, horizons=horizon,
                notes=["Prices/errors and interpolated quantiles use Decimal precision 80; no binary financial arithmetic.",
                       "Basis points use the actual matched target price as denominator, matching part 6.",
                       "Issued and published counts are decision counts, not wall-clock availability or independent samples.",
                       "Observed feed events are only those retained in audit snapshots/target records; this is not an independent complete feed ledger.",
                       "Runtime counter maxima are frozen observations, not guaranteed shutdown counter totals.",
                       "Timing is latest included event receipt to acknowledged Redis publication, not browser latency.",
                       "The recorded input-slot arithmetic is independently checked by independent_check.py."])

def main():
    p=argparse.ArgumentParser()
    p.add_argument('export',type=Path)
    p.add_argument('output',type=Path)
    args=p.parse_args()
    with localcontext() as ctx:
        ctx.prec=80
        ctx.rounding=ROUND_HALF_EVEN
        result=analyze(args.export)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,default=serial,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('issued_decisions','publication_statuses','latency','receipt_to_ack_within_10_ms')},default=serial,indent=2))
    for h,item in result['horizons'].items():
        print(json.dumps({'horizon_s':h,'published':item['cohorts'].get('published_clean_matched'),'counts':item['targets_published']},default=serial))

if __name__=='__main__':main()
