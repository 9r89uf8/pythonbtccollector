"""Offline reliability-canary cohorts; no browser, network or production access.

Reuses the frozen C parser/pairing arithmetic and adds explicit capture checks,
fixed-grid accounting and retrospective server/browser clock-model intervals.
An optional successful exact-byte audit check upgrades run membership only;
it never supplies browser receipts or target anchors.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PARSER_PATH = ROOT/'research/spot_twap_response/checkpoint_c/analyze_browser.py'
SPEC = importlib.util.spec_from_file_location('frozen_c_browser_analysis', PARSER_PATH)
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)
HORIZONS = (1, 2, 3, 5, 10, 30)
NS_MS = Decimal(1_000_000)


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b''):
            result.update(chunk)
    return result.hexdigest()


def integer_ceil(value):
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def spans(indices):
    answer = []
    for index in sorted(indices):
        if answer and answer[-1]['last_index']+1 == index:
            answer[-1]['last_index'] = index
        else:
            answer.append(dict(first_index=index, last_index=index))
    for span in answer:
        span.update(count=span['last_index']-span['first_index']+1,
                    start_elapsed_ms=span['first_index']*100,
                    end_elapsed_ms=(span['last_index']+1)*100)
    return answer


def coverage(probes, indices):
    indices = set(indices)
    present = indices & set(probes)
    missing = sorted(indices-present)
    calibrated = {i for i in present if probes[i]['calibrated']}
    intervals = spans(missing)
    return dict(planned_bins=len(indices), observed_bins=len(present), missing_bins=len(missing),
        uncalibrated_bins=len(present-calibrated), missing_intervals=intervals,
        maximum_missing_streak_bins=max((s['count'] for s in intervals), default=0),
        by_horizon=[dict(horizon_s=h, usable_bins=sum(h in probes[i]['usable_horizons'] for i in calibrated),
            not_usable_bins=sum(h not in probes[i]['usable_horizons'] for i in calibrated),
            unknown_bins=len(indices-calibrated),
            usable_fraction=Decimal(sum(h in probes[i]['usable_horizons'] for i in calibrated))/len(indices)
                if indices else None) for h in HORIZONS])


def analyze(browser, campaign_path, launch_path, stop_path, run_id, audit_check=None):
    metadata = C.decode(campaign_path.read_text(encoding='utf-8'))
    launch = C.decode(launch_path.read_text(encoding='utf-8'))
    stop = C.decode(stop_path.read_text(encoding='utf-8'))
    stop = stop.get('stop_record', stop)
    campaign = metadata['campaign']
    start_server = int(launch['campaign_start_ms'])*1_000_000
    end_server = int(launch['campaign_end_ms'])*1_000_000
    require(campaign['start_ms'] == launch['campaign_start_ms'] and campaign['end_ms'] == launch['campaign_end_ms'],
            'browser/operator campaign metadata mismatch')
    require(campaign['run_id'] in (None, run_id), 'browser campaign names another run')
    require(end_server-start_server == 3_600_000_000_000, 'expected fixed one-hour campaign')
    require(stop['state_directory'] == launch['state_directory'], 'stop belongs to another campaign')
    require(stop['status'] == 'stopped', 'operator stop did not complete')
    stop_server = int(stop['requested_wall_ns'])
    source_hash = digest(browser)
    membership = 'provisional'
    if audit_check is not None:
        joined = C.decode(audit_check.read_text(encoding='utf-8'))
        require(joined['status'] == 'passed' and not joined['errors'] and joined['run_id'] == run_id,
                'exact-byte audit join failed or names another run')
        require(joined['browser']['sha256'] == source_hash and
                joined['browser']['exact_attempted_byte_matches'] == joined['browser']['unique_run_payloads'],
                'audit join does not cover every observed run payload in this capture')
        membership = 'exact_attempted_bytes_verified'
    probes, rows, anchors, identities = {}, [], {}, {}
    kinds, errors = Counter(), Counter()
    brackets, snapshots, failure_details = [], [], []
    foreign_runs, rounding = Counter(), []
    start = end = previous = first_same_run = None
    start_wall = end_wall = None
    count = byte_count = 0
    observation = drain = None
    opener = gzip.open if browser.suffix == '.gz' else open
    before = browser.stat()
    with opener(browser, 'rb') as handle:
        while True:
            raw = handle.readline(262_145)
            if not raw:
                break
            require(len(raw) <= 262_144 and raw.endswith(b'\n'), 'oversized/truncated browser record')
            require(end is None, 'record follows final end marker')
            count += 1
            require(count <= 100_000 and byte_count+len(raw) <= 128*1024**2, 'capture exceeds frozen cap')
            record = C.decode(raw.decode('utf-8'))
            kind = record['kind']
            now = Decimal(record['browser_ms'])
            require(previous is None or now >= previous, 'browser record clocks regress')
            previous = now
            kinds[kind] += 1
            if kind == 'start':
                require(start is None and count == 1, 'start must be first and unique')
                start, start_wall = now, record['wall_ms']
                observation, drain = record['observation_ms'], record['drain_ms']
                require((observation, drain) == (3_600_000, 120_000), 'unexpected capture timing')
            else:
                require(start is not None, 'missing start')
                elapsed = now-start
                if kind == 'end':
                    require(record.get('complete') is True and record.get('reason') == 'completed', 'incomplete capture')
                    require(elapsed >= observation+drain, 'early completion')
                    require(record['records_before_end'] == count-1 and record['bytes_before_end'] == byte_count,
                            'end-marker record/byte counts disagree')
                    end, end_wall = now, record['wall_ms']
                    require(record == metadata['end'], 'sidecar/end marker mismatch')
                elif kind == 'probe':
                    index = record['planned_index']
                    require(type(index) is int and 0 <= index < (observation+drain)//100, 'invalid planned index')
                    require(index not in probes, 'duplicate recorded grid index')
                    computed = int(elapsed//100)
                    if computed != index:
                        require(abs(elapsed-index*100) <= Decimal('.001'), 'planned index disagrees with clock')
                        rounding.append(dict(index=index, elapsed_ms=elapsed, computed=computed))
                    require(record['phase'] == ('observation' if elapsed < observation else 'follow_through'), 'wrong phase')
                    require(type(record['calibrated']) is bool, 'invalid calibration flag')
                    require(record['usable_horizons'] == [h for h in HORIZONS if h in record['usable_horizons']], 'bad horizon list')
                    require(record['calibrated'] or not record['usable_horizons'], 'uncalibrated sample claims availability')
                    probes[index] = record
                elif kind == 'snapshot':
                    request_start = Decimal(record['start_ms'])
                    request_end = Decimal(record['end_ms'])
                    require(start <= request_start <= request_end == now, 'invalid GET request clocks')
                    snapshots.append(dict(start_elapsed_ms=request_start-start, end_elapsed_ms=elapsed,
                                          status=record['status'], duration_ms=request_end-request_start))
                    if record['status'] == 200 and record['server_time_ns'] is not None:
                        server = Decimal(record['server_time_ns'])
                        # Expand by one ns for decimal-text vs JS-number rounding.
                        brackets.append(dict(lower=server-request_end*NS_MS-1,
                                             upper=server-request_start*NS_MS+1, elapsed_ms=elapsed))
                elif kind == 'error':
                    errors[record['reason']] += 1
                    if record['reason'] == 'snapshot_error':
                        request_start = record.get('start_ms')
                        failure_details.append(dict(elapsed_ms=elapsed, start_ms=request_start,
                            duration_ms=Decimal(record['end_ms'])-Decimal(request_start) if request_start is not None else None,
                            timer_fired_ms=record.get('abort_timer_fired_ms'), stage=record.get('stage')))
                elif kind == 'ghost':
                    api, identity, anchor, forecasts = C.wire(record['data'])
                    if identity is not None:
                        run, decision, signature = identity
                        if run != run_id:
                            foreign_runs[run] += 1
                        else:
                            payload = C.decode(record['data'])['ghost']
                            require(start_server <= int(payload['decision_wall_ns']) < end_server, 'run decision outside campaign')
                            if first_same_run is None:
                                first_same_run = now
                            if anchor is not None:
                                entry = anchors.setdefault(anchor[0],dict(first_ms=elapsed,values=set()))
                                entry['values'].add(anchor[1])
                            if decision in identities:
                                require(identities[decision] == signature, 'conflicting producer identity')
                            else:
                                identities[decision] = signature
                                if elapsed < observation:
                                    rows.extend(dict(f,run_id=run,decision_id=decision,elapsed_ms=elapsed,resync=api['resync']) for f in forecasts)
                else:
                    require(kind == 'open', 'unexpected record kind')
            byte_count += len(raw)
    after = browser.stat()
    require((before.st_size,before.st_mtime_ns) == (after.st_size,after.st_mtime_ns), 'browser file changed')
    require(end is not None and first_same_run is not None, 'complete capture and campaign object required')
    require(metadata['records'] == count and metadata['bytes'] == byte_count, 'export counts disagree')
    first_declared = metadata['first_campaign_object']
    require(first_declared is not None and first_declared['run_id'] == run_id and
            Decimal(first_declared['browser_ms']) == first_same_run, 'first campaign object mismatch')
    exact_warmup = first_same_run-start+65_000
    warmup = integer_ceil(exact_warmup)
    require(warmup < observation, 'no post-first-object warmup window')
    warm_start_index = (warmup+99)//100
    full = coverage(probes,range(observation//100))
    post = coverage(probes,range(warm_start_index,observation//100))
    follow = coverage(probes,range(observation//100,(observation+drain)//100))
    model = dict(status='unavailable', successful_GET_brackets=len(brackets),
                 assumption='Constant server-wall minus browser-performance offset over the capture; not a live freshness certificate.')
    overlap = comparable = None
    if brackets:
        lower, upper = max(b['lower'] for b in brackets), min(b['upper'] for b in brackets)
        model.update(offset_lower_ns=lower,offset_upper_ns=upper,
            first_calibration_elapsed_ms=brackets[0]['elapsed_ms'],last_calibration_elapsed_ms=brackets[-1]['elapsed_ms'])
        if lower <= upper:
            model['status'] = 'not_contradicted_by_successful_GETs'
            inside, boundary, outside = [], [], []
            for i in range(observation//100):
                tick = (start+i*100)*NS_MS
                target = inside if tick+lower >= start_server and tick+upper < end_server else (
                    outside if tick+upper < start_server or tick+lower >= end_server else boundary)
                target.append(i)
            overlap = dict(guaranteed_inside_clock_model=coverage(probes,inside),
                           boundary_uncertain_bins=len(boundary),outside_bins=len(outside))
            accuracy_lower = max(Decimal(0),(Decimal(start_server)-lower)/NS_MS-start)
            accuracy_upper = min(Decimal(observation),(Decimal(stop_server)-120_000_000_000-upper)/NS_MS-start)
            comparable = dict(lower_elapsed_ms=accuracy_lower,upper_elapsed_ms=accuracy_upper,
                stop_requested_wall_ns=str(stop_server),margin_ns=120_000_000_000,
                condition='Receipt interval wholly after campaign start and at least120s before actual stop request, under clock model.',
                by_horizon=C._panel(rows,anchors,accuracy_lower,accuracy_upper))
        else:
            model['status'] = 'incompatible_brackets_no_overlap_or_comparable_claim'
    return dict(version='reliability-browser-cohorts-v1',status='checked',run_id=run_id,
        membership=membership,source_sha256=source_hash,record_count=count,decompressed_bytes=byte_count,
        record_kinds=dict(kinds),errors=dict(errors),foreign_run_envelopes=dict(foreign_runs),
        capture_start_browser_ms=start,capture_end_browser_ms=end,
        local_wall_minus_performance_change_ms=Decimal(end_wall-start_wall)-(end-start),
        first_same_campaign_browser_ms=first_same_run,exact_warmup_offset_ms=exact_warmup,
        analyzer_warmup_ms=warmup,warmup_rounding='Ceiling to integer ms; first included grid bin then ceilings to100ms.',
        full_browser=full,after_first_object_plus65s=post,follow_through=follow,
        retrospective_clock_model=model,campaign_overlap=overlap,comparable_accuracy=comparable,
        all_hour_accuracy=C._panel(rows,anchors,Decimal(0),Decimal(observation)),
        enriched_GET_failures=failure_details,completed_GETs=snapshots,index_rounding_cases=rounding,
        limitations=['Missing browser bins remain unknown; no interpolation or replacement by Redis/audit observations.',
          'Browser price pairing uses only this run\'s exact later anchors in this same capture.',
          'Clock-model panels are retrospective and cannot certify clocks during an unobserved pause.',
          'Recorded browser usability masks are not independently rederived by this helper.',
          'Financial pairing reuses the frozen C parser/panel; this is cohort accounting, not an independent price estimator.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('browser','campaign','launch','stop','output'):
        parser.add_argument('--'+name,required=True,type=Path)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--audit-check',type=Path)
    args = parser.parse_args()
    require(not args.output.exists(),'refuse overwrite')
    with localcontext() as context:
        context.prec = 80
        result = analyze(args.browser,args.campaign,args.launch,args.stop,args.run_id,args.audit_check)
        result['input_sha256'] = {str(p):digest(p) for p in
            [args.browser,args.campaign,args.launch,args.stop]+([args.audit_check] if args.audit_check else [])}
        result['code_sha256'] = {str(p):digest(p) for p in [Path(__file__),PARSER_PATH]}
        result = C._jsonable(result)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as handle:
        json.dump(result,handle,indent=2,sort_keys=True,allow_nan=False)
        handle.write('\n')
    print(json.dumps({'status':'checked','membership':result['membership'],
        'warmup_ms':result['analyzer_warmup_ms'],'browser_planned_bins':result['full_browser']['planned_bins'],
        'browser_missing_bins':result['full_browser']['missing_bins']}))


if __name__ == '__main__':
    main()
