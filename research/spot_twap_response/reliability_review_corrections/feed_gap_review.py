"""Fixed-canary local evidence review; no service/database access or model fitting."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path

NS_MS = 1_000_000
RUN_ID = 'fe2dd06da5754c50b696cb9419ca7251'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def utc_ns(value):
    seconds, remainder = divmod(int(value), 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + f'.{remainder:09d}Z'


def event_summary(event):
    return {**event, 'source_utc': utc_ns(event['source_timestamp_ms'] * NS_MS),
            'receipt_utc': utc_ns(event['received_wall_ns'])}


def deadline_components(payload):
    limits = {}
    wall, mono = int(payload['decision_wall_ns']), int(payload['decision_monotonic_ns'])
    policy = payload['policy']
    for feed in ('spot', 'twap'):
        event = payload['current_' + feed]
        limits[feed + '_source'] = (event['source_timestamp_ms'] + policy['source_max_age_ms']) * NS_MS
        limits[feed + '_receipt_wall'] = int(event['received_wall_ns']) + policy['receipt_max_age_ms'] * NS_MS
        limits[feed + '_receipt_monotonic'] = wall + policy['receipt_max_age_ms'] * NS_MS - (mono - int(event['received_monotonic_ns']))
    if payload['reasons']:
        limits['global_unavailability'] = wall
    earliest = min(limits.values())
    require(earliest == int(payload['valid_until_wall_ns']), 'Payload expiry does not match inputs')
    return {'expiry_wall_ns': str(earliest), 'expiry_utc': utc_ns(earliest),
            'limiting_clocks': [key for key, value in limits.items() if value == earliest],
            'components_wall_ns': {key: str(value) for key, value in limits.items()}}


def sequence_bridge(events, first, last):
    missing = sorted(set(range(first, last + 1)) - events.keys())
    require(not missing, 'Boundary sequence evidence is incomplete')
    ordered = [events[n] for n in range(first, last + 1)]
    require(all(int(a['received_monotonic_ns']) <= int(b['received_monotonic_ns'])
                and int(a['received_wall_ns']) <= int(b['received_wall_ns'])
                for a, b in zip(ordered, ordered[1:])), 'Boundary receipt order conflict')
    return [event_summary(e) for e in ordered]


def storage_arithmetic(before, after, stop):
    growth = after - before
    require(0 < growth and after < stop, 'Unexpected relation measurements')
    with localcontext() as context:
        context.prec = 80
        return {'before_relation_bytes': before, 'after_relation_bytes': after,
                'one_canary_relation_growth_bytes': growth, 'relation_stop_bytes': stop,
                'remaining_relation_bytes': stop - after,
                'hypothetical_additional_hours_at_same_growth': str(Decimal(stop-after) / Decimal(growth)),
                'observed_canary_hours': 1,
                'interpretation': 'Linear extrapolation of one one-hour canary relation delta, not a guaranteed capacity, throughput rate, retention validation, or permission to run longer. The separate runtime campaign deadline remains one hour.'}


def analyze(*, results, observer, audit_index):
    results, observer, audit_index = Path(results), Path(observer), Path(audit_index)
    provenance = {}

    def checked(path, expected=None):
        raw = path.read_bytes()
        digest = sha256(raw).hexdigest()
        require(expected is None or expected == digest, 'Input hash mismatch: ' + str(path))
        provenance[str(path)] = {'sha256': digest, 'bytes': len(raw)}
        return raw

    manifest = json.loads(checked(results/'audit_analysis/manifest.json'))
    require(manifest['status'] == 'accepted' and manifest['run_id'] == RUN_ID, 'Wrong audit manifest')
    index = json.loads(checked(audit_index, manifest['index_sha256']))
    require(index['run_id'] == RUN_ID, 'Wrong index run')
    observer_manifest = json.loads(checked(observer/'manifest.json'))
    old_gaps = json.loads(checked(results/'OBSERVER_GAPS.json'))
    require(old_gaps['status'] == 'accepted' and old_gaps['provenance']['observer_manifest_sha256'] ==
            provenance[str(observer/'manifest.json')]['sha256'], 'Wrong observer gap evidence')
    ledger, events = {}, {}

    def remember(event):
        if event is None:
            return
        sequence = event['sequence']
        require(type(sequence) is int and sequence > 0, 'Bad event sequence')
        require(sequence not in events or events[sequence] == event, 'Conflicting event sequence')
        events[sequence] = event

    for raw in checked(observer/'payloads.jsonl', observer_manifest['files']['payloads.jsonl']['sha256']).splitlines():
        record = json.loads(raw)
        body = base64.b64decode(record['raw_base64'], validate=True)
        key = sha256(body).hexdigest()
        require(key == record['sha256'] and len(body) == record['bytes'], 'Bad payload ledger bytes')
        payload = json.loads(body)
        require(payload['run_id'] == RUN_ID and key in index['payloads'], 'Payload not in accepted audit index')
        require(index['payloads'][key]['status'] == 'acknowledged', 'Unconfirmed observed payload')
        require(payload['decision_id'] == index['payloads'][key]['decision_id'], 'Payload identity mismatch')
        ledger[key] = payload
        for field in ('current_spot', 'current_twap'):
            remember(payload[field])
    for item in index['target_events'].values():
        remember(item['earliest'])
    samples = []
    for raw in checked(observer/'samples.jsonl', observer_manifest['files']['samples.jsonl']['sha256']).splitlines():
        record = json.loads(raw)
        require(record['index'] == len(samples) and record['count'] == 1, 'Unexpected fixed-run grid')
        samples.append(record)
    require(len(samples) == observer_manifest['planned_bins'] == 36000, 'Incomplete observer grid')
    is_absent = lambda row: (row['status'] == 'absent' and not row.get('clock_anomaly')
        and not row.get('read_crosses_bin_end') and not row.get('read_crosses_campaign_end'))
    runs, opened = [], None
    for i in range(650, len(samples)+1):
        if i < len(samples) and is_absent(samples[i]):
            if opened is None:
                opened = i
        elif opened is not None:
            runs.append((opened, i-1))
            opened = None
    require(len(runs) == 3 and sum(b-a+1 for a,b in runs) == 151, 'Unexpected post65 absence runs')
    require(sorted(runs) == sorted((r['first_bin'], r['last_bin_inclusive']) for r in
        old_gaps['panels']['post_65_seconds']['key_absent_known']['longest_runs']), 'Gap evidence changed')

    def probe(row):
        fields = ('index', 'status', 'read_start_wall_ns', 'read_end_wall_ns',
                  'read_start_monotonic_ns', 'read_end_monotonic_ns', 'ttl_ms', 'payload_sha256')
        return {**{f:row[f] for f in fields if f in row},
                'read_end_utc': utc_ns(row['read_end_wall_ns'])}

    reviews = []
    for first, last in runs:
        before, after = samples[first-1], samples[last+1]
        require(before['status'] == after['status'] == 'present', 'Unbracketed cache absence')
        previous, following = ledger[before['payload_sha256']], ledger[after['payload_sha256']]
        expiry = deadline_components(previous)
        feeds = {}
        for feed in ('spot', 'twap'):
            a, b = previous['current_'+feed], following['current_'+feed]
            require(a['sequence'] < b['sequence'] and a['source_timestamp_ms'] < b['source_timestamp_ms'], 'Nonadvancing restoration')
            between = [e for e in events.values() if e['feed'] == feed and a['sequence'] < e['sequence'] < b['sequence']]
            require(not between, 'Intervening saved feed input')
            missing_stamps = list(range(a['source_timestamp_ms']+1000, b['source_timestamp_ms'], 1000))
            feeds[feed] = {'before': event_summary(a), 'after': event_summary(b),
                'source_gap_ms': b['source_timestamp_ms']-a['source_timestamp_ms'],
                'receipt_gap_wall_ns': str(int(b['received_wall_ns'])-int(a['received_wall_ns'])),
                'receipt_gap_monotonic_ns': str(int(b['received_monotonic_ns'])-int(a['received_monotonic_ns'])),
                'interior_second_stamps': missing_stamps,
                'interior_second_stamps_utc': [utc_ns(s*NS_MS) for s in missing_stamps],
                'saved_twap_target_evidence_at_interior_stamps': [s for s in missing_stamps if str(s) in index['target_events']] if feed=='twap' else None}
        bridge = sequence_bridge(events, min(previous['current_'+f]['sequence'] for f in feeds),
                                 max(following['current_'+f]['sequence'] for f in feeds))
        reviews.append({'first_absent_bin': first, 'last_absent_bin': last, 'absent_bins': last-first+1,
            'sampled_bin_footprint_ms': (last-first+1)*100,
            'first_to_last_planned_probe_span_ms': (last-first)*100,
            'previous_present': probe(before), 'first_absent': probe(samples[first]),
            'last_absent': probe(samples[last]), 'next_present': probe(after),
            'previous_payload_decision_id': previous['decision_id'], 'next_payload_decision_id': following['decision_id'],
            'previous_payload_expiry': expiry,
            'expiry_between_previous_present_end_and_first_absent_start':
                int(before['read_end_wall_ns']) <= int(expiry['expiry_wall_ns']) <= int(samples[first]['read_start_wall_ns']),
            'feeds': feeds, 'contiguous_boundary_event_sequences': bridge,
            'explicit_gap_count_before_after': [previous['gap_count'], following['gap_count']],
            'reconnect_metadata_before_after': [previous['spot_reconnect'], following['spot_reconnect']]})
    pre = json.loads(checked(results/'PREFLIGHT.json'))
    post = json.loads(checked(results/'STOP_VERIFICATION.json'))
    summary = json.loads(checked(results/'audit_analysis/summary.json', manifest['artifacts_sha256']['summary.json']))
    source = Path(__file__)
    provenance[str(source)] = {'sha256': sha256(source.read_bytes()).hexdigest()}
    return {'status': 'accepted', 'run_id': RUN_ID, 'gaps': reviews,
        'storage': storage_arithmetic(pre['audit_status']['measurement']['relation_bytes'],
                    post['audit_status']['measurement']['relation_bytes'], pre['expected']['relation_stop_bytes']),
        'recorded_stop_reason': post['campaign']['stop_reason'],
        'recorded_campaign_duration_ms': manifest['campaign_end_ms'] - manifest['campaign_start_ms'],
        'saved_event_union_count': len(events), 'source_export_sha256': manifest['input_sha256'],
        'interpretation': [
            'All three post65 cache absences align with a pause in locally offered spot and TWAP inputs, expiry of the last observed payload, and restoration after both fresh inputs are received.',
            'Complete sequence bridges prove no omitted offer_price sequence between these boundary inputs under this run\'s saved runtime contract. They do not prove capture completeness outside those bridges or absence of rejected/unoffered source frames.',
            'The first two expiries are receipt-age limited; the third is TWAP source-age limited. This is stronger than a source-stamp-only correlation.',
            'No explicit gap/reconnect metadata is recorded around these pauses. Their upstream/network/collector origin and whether the provider published the missing stamps are not established.',
            'Three episodes describe this one observed hour; they are not an estimated general occurrence rate.',
            'Cache absence is sampled at 100ms; endpoints bracket observations, not exact uninterrupted outage duration.',
            'The laptop closure affects browser evidence; these observer reads and producer receipts were recorded on the server.'],
        'provenance': provenance}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--observer', type=Path, required=True)
    parser.add_argument('--audit-index', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = vars(parser.parse_args())
    output = args.pop('output')
    require(not output.exists(), 'Refusing to overwrite review evidence')
    result = analyze(**args)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({'status': result['status'], 'gaps': len(result['gaps']), 'storage': result['storage']}))


if __name__ == '__main__':
    main()
