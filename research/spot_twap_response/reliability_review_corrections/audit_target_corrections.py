"""Read-only correction evidence from the exact frozen reliability-canary export."""
from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

EXPECTED_SHA256 = 'f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954'
EXPECTED_BYTES = 1078545445
EXPECTED_ROWS = 23411
RUN_ID = 'fe2dd06da5754c50b696cb9419ca7251'
HORIZONS = (1, 2, 3, 5, 10, 30)


def utc(ms: int) -> str:
    return datetime.fromtimestamp(ms // 1000, timezone.utc).isoformat()


def analyze(path: Path) -> dict:
    whole = hashlib.sha256()
    total_bytes = total_rows = selected_rows = 0
    unpublished, unmatched, aged_missing = [], [], []
    sources, sequences = set(), {}
    statuses = Counter()
    last_received = None

    def remember(event):
        nonlocal last_received
        if event is None:
            return
        if event['feed'] != 'twap' or event['window_s'] != 60:
            raise ValueError('unexpected TWAP event identity')
        signature = (event['source_timestamp_ms'], event['received_wall_ns'],
                     event['received_monotonic_ns'], event['event_id'], event['value'])
        sequence = event['sequence']
        if sequence in sequences and sequences[sequence] != signature:
            raise ValueError('conflicting retained TWAP sequence')
        sequences[sequence] = signature
        sources.add(event['source_timestamp_ms'])
        if last_received is None or int(event['received_monotonic_ns']) > int(last_received['received_monotonic_ns']):
            last_received = {k:event[k] for k in ('sequence','event_id','source_timestamp_ms',
                                                  'received_wall_ns','received_monotonic_ns')}

    with path.open('rb') as stream:
        while True:
            raw = stream.readline(1048577)
            if not raw:
                break
            if len(raw) > 1048576 or not raw.endswith(b'\n'):
                raise ValueError('oversized or truncated audit row')
            whole.update(raw); total_bytes += len(raw); total_rows += 1
            row = json.loads(raw, parse_float=Decimal)
            if row['run_id'] != RUN_ID:
                continue
            selected_rows += 1
            for field in ('frozen','state'):
                if hashlib.sha256(row[field+'_json'].encode()).hexdigest() != row[field+'_sha256']:
                    raise ValueError('per-row audit hash mismatch')
            frozen = json.loads(row['frozen_json'], parse_float=Decimal)
            state = json.loads(row['state_json'], parse_float=Decimal)
            remember(frozen['current_twap'])
            status = state['publication']['status']
            statuses[status] += 1
            if status != 'acknowledged':
                clocks = {}
                for name in ('current_spot','current_twap'):
                    event = frozen[name]
                    clocks[name] = None if event is None else {
                        'source_age_ns':int(frozen['decision_wall_ns'])-event['source_timestamp_ms']*1000000,
                        'wall_receipt_age_ns':int(frozen['decision_wall_ns'])-int(event['received_wall_ns']),
                        'mono_receipt_age_ns':int(frozen['decision_monotonic_ns'])-int(event['received_monotonic_ns'])}
                unpublished.append({'decision_id':row['decision_id'],
                    'decision_wall_ns':str(frozen['decision_wall_ns']), 'publication_status':status,
                    'all_forecasts_null':all(f['price'] is None for f in frozen['forecasts']),
                    'global_reasons':frozen['reasons'],
                    'forecast_reason_sets':sorted({tuple(f['reasons']) for f in frozen['forecasts']}),
                    'current_input_ages':clocks})
            for horizon, target in state['targets'].items():
                for field in ('first_event','first_late_event','first_conflicting_event'):
                    remember(target.get(field))
                if target['status'] == 'restart_unmatched':
                    if target.get('first_event') is not None:
                        raise ValueError('restart-unmatched target contains a first receipt')
                    unmatched.append({'decision_id':row['decision_id'],'horizon_s':int(horizon),
                        'target_source_timestamp_ms':target['target_source_timestamp_ms'],
                        'first_late_event_present':target.get('first_late_event') is not None,
                        'shutdown_label':state.get('shutdown')})
                elif target['status'] == 'missing':
                    aged_missing.append({'decision_id':row['decision_id'],'horizon_s':int(horizon),
                        'target_source_timestamp_ms':target['target_source_timestamp_ms'],
                        'first_late_event_present':target.get('first_late_event') is not None})
    if (total_bytes,total_rows,whole.hexdigest()) != (EXPECTED_BYTES,EXPECTED_ROWS,EXPECTED_SHA256):
        raise ValueError('immutable input does not match the declared export')
    ordered = sorted(sources)
    if not ordered:
        raise ValueError('no retained TWAP observations')
    maximum = ordered[-1]
    split = {h:Counter() for h in HORIZONS}
    earlier = Counter()
    earlier_horizons = {}
    after = Counter()
    for item in unmatched:
        stamp = item['target_source_timestamp_ms']
        if stamp in sources:
            split[item['horizon_s']]['exact_stamp_observed_elsewhere'] += 1
        elif stamp < maximum:
            split[item['horizon_s']]['earlier_unobserved_source_stamp'] += 1
            earlier[stamp] += 1
            earlier_horizons.setdefault(stamp,Counter())[item['horizon_s']] += 1
        else:
            split[item['horizon_s']]['beyond_last_observed_source_stamp'] += 1
            after[stamp] += 1
    holes = []
    for stamp in sorted(earlier):
        index = bisect_left(ordered,stamp)
        if not 0 < index < len(ordered):
            raise ValueError('claimed earlier hole is not bracketed by retained observations')
        holes.append({'target_source_timestamp_ms':stamp,'utc':utc(stamp),
            'unmatched_forecasts':earlier[stamp],
            'horizon_counts':dict(sorted(earlier_horizons[stamp].items())),
            'preceding_observed_source_ms':ordered[index-1],
            'following_observed_source_ms':ordered[index],
            'observed_bracket_span_ms':ordered[index]-ordered[index-1]})
    aged_stamps = Counter(item['target_source_timestamp_ms'] for item in aged_missing)
    aged_brackets = []
    for stamp in sorted(aged_stamps):
        index = bisect_left(ordered,stamp)
        aged_brackets.append({'target_source_timestamp_ms':stamp,'utc':utc(stamp),
            'missing_forecasts':aged_stamps[stamp],'exact_stamp_observed':stamp in sources,
            'preceding_observed_source_ms':ordered[index-1] if index else None,
            'following_observed_source_ms':ordered[index] if index<len(ordered) else None})
    all_source_holes = set(range(ordered[0],maximum+1000,1000))-sources
    aged_all_bracketed = all(not item['exact_stamp_observed']
        and item['preceding_observed_source_ms'] is not None and item['following_observed_source_ms'] is not None
        for item in aged_brackets)
    expected_split = {1:(8,0),2:(10,2),3:(12,3),5:(15,7),10:(20,16),30:(21,55)}
    actual = {h:(split[h]['earlier_unobserved_source_stamp'],split[h]['beyond_last_observed_source_stamp']) for h in HORIZONS}
    reason_counts = Counter(reason for row in unpublished for reason in row['global_reasons'])
    claims = {
        'seven_unpublished_all_noncalculable':len(unpublished)==7 and all(r['all_forecasts_null'] for r in unpublished),
        'one_missing_spot_six_stale_twap':reason_counts['missing_spot']==1 and reason_counts['stale_twap']==6,
        'restart_unmatched_169_split_86_83':len(unmatched)==169 and actual==expected_split,
        '46_distinct_fully_aged_missing_stamps_are_observed_holes':len(aged_stamps)==46 and aged_all_bracketed}
    verified = (selected_rows==7082 and all(claims.values())
        and not any(s['exact_stamp_observed_elsewhere'] for s in split.values()))
    return {'claims_verified':verified,'claim_checks':claims,'input':{'path':str(path),'sha256':whole.hexdigest(),
            'bytes':total_bytes,'rows':total_rows}, 'run_id':RUN_ID,'selected_rows':selected_rows,
        'publication_status_counts':dict(statuses),'unpublished_rows':len(unpublished),
        'unpublished_global_reason_counts':dict(reason_counts),'unpublished_details':unpublished,
        'restart_unmatched_forecasts':len(unmatched),
        'restart_unmatched_shutdown_labels':dict(Counter(item['shutdown_label'] for item in unmatched)),
        'restart_unmatched_with_late_receipt':sum(item['first_late_event_present'] for item in unmatched),
        'by_horizon':[{'horizon_s':h,'earlier_unobserved_source_stamp':actual[h][0],
            'beyond_last_observed_source_stamp':actual[h][1],
            'exact_stamp_observed_elsewhere':split[h]['exact_stamp_observed_elsewhere']} for h in HORIZONS],
        'earlier_unobserved_forecasts':sum(earlier.values()),'earlier_unique_unobserved_stamps':len(earlier),
        'beyond_last_observed_forecasts':sum(after.values()),'beyond_last_observed_unique_stamps':len(after),
        'fully_aged_missing_forecasts':len(aged_missing),'fully_aged_missing_unique_stamps':len(aged_stamps),
        'fully_aged_missing_with_late_receipt':sum(item['first_late_event_present'] for item in aged_missing),
        'fully_aged_missing_by_horizon':dict(sorted(Counter(item['horizon_s'] for item in aged_missing).items())),
        'fully_aged_missing_hole_brackets':aged_brackets,
        'all_observed_source_holes_between_first_and_last':len(all_source_holes),
        'aged_and_shutdown_earlier_hole_overlap':len(set(aged_stamps)&set(earlier)),
        'retained_observed_twap':{'unique_sequences':len(sequences),'unique_source_stamps':len(ordered),
            'first_source_ms':ordered[0],'last_source_ms':maximum,'last_source_utc':utc(maximum),
            'last_received_event':last_received},'earlier_hole_brackets':holes,
        'method':'One stream of the frozen export. Retained TWAP observations are the union of current_twap and first/late/conflicting target events for this exact run. An earlier hole is an absent exact source stamp bracketed by retained observed stamps; the other group lies beyond the maximum retained source stamp. The 46-stamp claim concerns fully-aged target status missing; restart_unmatched is a separate shutdown cohort.',
        'limitations':['These are observed source-stamp holes, not proof the provider never published or that every accepted input event was retained.',
            'Beyond the last observed source stamp means unobserved continuation at shutdown; it need not be future wall-clock time.',
            'restart_unmatched is a shutdown status, not a cause. Earlier missing receipts could still have arrived late if observation had continued.',
            'No forecasts were recalculated and no previous performance metrics or artifacts were changed.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():
        raise SystemExit('refusing to overwrite correction evidence')
    result=analyze(args.audit)
    result['code_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as handle:
        json.dump(result,handle,indent=2);handle.write('\n')
    print(json.dumps({k:result[k] for k in ('claims_verified','selected_rows','publication_status_counts',
        'unpublished_rows','unpublished_global_reason_counts','restart_unmatched_forecasts',
        'by_horizon','earlier_unobserved_forecasts','earlier_unique_unobserved_stamps',
        'beyond_last_observed_forecasts','retained_observed_twap')},indent=2))
    if not result['claims_verified']:
        raise SystemExit(1)


if __name__=='__main__':
    main()
