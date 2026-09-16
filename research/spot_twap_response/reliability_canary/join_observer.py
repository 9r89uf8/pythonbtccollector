"""Exact observer/audit join and conservative read-end target-receipt check."""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from decimal import localcontext
from hashlib import sha256
import json
from pathlib import Path

from price_collector.ghost_twap_observer import analyze as validate_observer, classify_payload
from research.spot_twap_response.combined_canary.join_observer import checked_lines
from research.spot_twap_response.reliability_canary import _audit_v6 as evidence

require, decode = evidence.require, evidence.read_json


def validate_entry(raw, entry):
    p = decode(raw)
    require(p['run_id'] == entry['run_id'] and p['decision_id'] == entry['decision_id'], 'Observer identity mismatch')
    require(entry['status'] in ('acknowledged', 'uncertain'), 'Observed payload is not an audited write')
    require(p['publication_state'] == 'attempted', 'Observed payload is intent-only')
    require(p['publication_eligibility']['eligible_horizons'] == entry['eligible_horizons'], 'Observer mask mismatch')
    for clock in ('wall', 'monotonic'):
        require(str(p['publication_attempt_' + clock + '_ns']) == entry['attempt_' + clock + '_ns'],
                'Observer attempt clock mismatch')
    for f in p['forecasts']:
        if f['horizon_s'] in entry['eligible_horizons']:
            original = entry['forecasts'][str(f['horizon_s'])]
            require(f['quality'] in ('healthy', 'degraded') and f['price'] is not None and
                    all(f[k] == original[k] for k in ('price', 'quality', 'target_source_timestamp_ms')),
                    'Observer forecast membership mismatch')
    return p


def target_at_read_end(entry, horizon, target_events, wall, mono):
    """Do not infer unreceived from a missing target or from a price-only match."""
    f = entry['forecasts'][str(horizon)]
    if entry['causality_invalid'] or f['target_conflicted'] or f['target_clock_anomaly']:
        return 'invalid_target_evidence'
    first = f.get('first_event')
    known = target_events.get(str(f['target_source_timestamp_ms']))
    if known is not None and known['distinct_values'] > 1:
        return 'invalid_target_evidence'
    earliest = None if known is None else known['earliest']
    if earliest is not None:
        ew, em = int(earliest['received_wall_ns']), int(earliest['received_monotonic_ns'])
        if ew <= wall and em <= mono:
            return 'already_received_by_read_end'
        if (ew <= wall) != (em <= mono):
            return 'clock_order_ambiguous'
    if first is None or f['target_status'] != 'matched':
        return 'target_receipt_unknown'
    fw, fm = int(first['received_wall_ns']), int(first['received_monotonic_ns'])
    if fw <= wall and fm <= mono:
        return 'already_received_by_read_end'
    if fw > wall and fm > mono:
        return 'recorded_first_target_later_than_read_end'
    return 'clock_order_ambiguous'


def join(observer_directory, analysis_directory, launch_path):
    observer_directory, analysis_directory, launch_path = map(Path, (observer_directory, analysis_directory, launch_path))
    audit_manifest_raw = (analysis_directory/'manifest.json').read_bytes()
    audit_manifest = decode(audit_manifest_raw)
    require(audit_manifest['status'] == 'accepted', 'Audit was not accepted')
    index_path = Path(audit_manifest['index_path'])
    require(index_path.stat().st_size <= 64 * 1024**2, 'Index too large')
    index_raw = index_path.read_bytes()
    require(sha256(index_raw).hexdigest() == audit_manifest['index_sha256'], 'Audit index hash mismatch')
    index = decode(index_raw)
    require(index['run_id'] == audit_manifest['run_id'], 'Audit index run mismatch')
    observer_manifest_raw = (observer_directory/'manifest.json').read_bytes()
    manifest = decode(observer_manifest_raw)
    launch_raw = launch_path.read_bytes()
    launch = decode(launch_raw)
    require(manifest['status'] == 'complete' and manifest['start_ms'] == audit_manifest['campaign_start_ms']
            and manifest['end_ms'] == audit_manifest['campaign_end_ms'], 'Observer interval mismatch')
    require(manifest['boot_id'] == launch['observer_ready']['boot_id'] and
            launch['campaign_start_ms'] == manifest['start_ms'], 'Observer launch/boot mismatch')
    # Revalidates exact ledger bytes, all saved classification fields, clocks and grid.
    coverage = validate_observer(observer_directory)
    raw_payloads, parsed, statuses = {}, {}, Counter()
    qualified = set(coverage['observed_qualified_payload_sha256'])
    for row in checked_lines(observer_directory/'payloads.jsonl', 100000, manifest['files']['payloads.jsonl']):
        digest = row['sha256']
        raw = base64.b64decode(row['raw_base64'], validate=True)
        require(sha256(raw).hexdigest() == digest and digest not in raw_payloads, 'Duplicate/corrupt observer ledger')
        raw_payloads[digest] = raw
        if digest in qualified:
            require(digest in index['payloads'], 'Observed bytes absent from audit attempt index')
            entry = index['payloads'][digest]
            parsed[digest] = validate_entry(raw, entry)
            statuses[entry['status']] += 1
    require(qualified <= parsed.keys(), 'Qualified payload missing from ledger')
    counts = {h: Counter() for h in evidence.HORIZONS}
    unique = {h: set() for h in evidence.HORIZONS}
    errors = {h: [] for h in evidence.HORIZONS}
    leads = {h: [] for h in evidence.HORIZONS}
    sample_statuses = Counter()
    with localcontext(evidence.CONTEXT):
        for row in checked_lines(observer_directory/'samples.jsonl', 16384, manifest['files']['samples.jsonl']):
            if row.get('status') != 'present' or row.get('payload_sha256') not in qualified:
                continue
            digest = row['payload_sha256']
            entry, payload = index['payloads'][digest], parsed[digest]
            classification = classify_payload(raw_payloads[digest], row['ttl_ms'], start_ms=manifest['start_ms'],
                end_ms=manifest['end_ms'], **{k:int(row[k]) for k in (
                    'read_start_wall_ns','read_end_wall_ns','read_start_monotonic_ns','read_end_monotonic_ns')})
            require(classification['payload_valid'] and classification['campaign_qualified'], 'Previously qualified payload is invalid')
            sample_statuses[entry['status']] += 1
            usable = [] if row.get('read_crosses_bin_end') or row.get('clock_step') else classification['usable_horizons']
            for h in usable:
                counts[h]['fresh_at_read_end_bins'] += 1
                result = target_at_read_end(entry, h, index['target_events'],
                    int(row['read_end_wall_ns']), int(row['read_end_monotonic_ns']))
                counts[h][result] += 1
                if result != 'recorded_first_target_later_than_read_end':
                    continue
                identity = (entry['run_id'], entry['decision_id'])
                if identity in unique[h]:
                    continue
                unique[h].add(identity)
                f = entry['forecasts'][str(h)]
                actual = evidence.decimal(f['first_event']['value'], True)
                forecast = evidence.decimal(f['price'], True)
                held = evidence.decimal(payload['current_twap']['value'], True)
                errors[h].append((forecast-actual, held-actual, actual))
                leads[h].append(int(f['first_event']['received_monotonic_ns']) - int(row['read_end_monotonic_ns']))
        result = dict(status='accepted', run_id=index['run_id'], planned_bins=manifest['planned_bins'],
            qualified_unique_payloads=len(qualified), unique_payloads_by_audit_status=dict(statuses),
            qualified_sample_bins_by_audit_status=dict(sample_statuses),
            horizons=[dict(horizon_s=h, counts=dict(counts[h]),
                unique_forecasts_with_recorded_target_later=len(unique[h]),
                unique_forecast_errors=evidence.paired_summary(errors[h]),
                first_usable_read_end_to_target_receipt_ms=evidence.distribution(leads[h], evidence.MS))
                for h in evidence.HORIZONS],
            limitations=[
                'These are sampled100ms read bins, not continuous-time coverage.',
                'Target-unreceived classification requires a clean exact matched first target received later on both recorded clocks; missing target evidence remains unknown.',
                'Any earlier saved TWAP receipt at the same source stamp overrides a later per-decision receipt; saved conflicting target prices are excluded.',
                'Shared monotonic-clock comparability relies on the frozen same-host launch/observer boot provenance; the decision export itself has no independent boot-id field.',
                'Known target evidence is the saved union, not a claim that every source event or earlier duplicate was durably captured.',
                'Exact observed bytes prove cache presence. Uncertain publication status, if any, remains uncertain and gains no ACK credit.',
                'Read-end freshness and target-arrival comparison do not prove browser delivery or execution readiness.'],
            provenance={'audit_manifest_sha256':sha256(audit_manifest_raw).hexdigest(),
                'audit_export_sha256':audit_manifest['input_sha256'],'audit_index_sha256':audit_manifest['index_sha256'],
                'observer_manifest_sha256':sha256(observer_manifest_raw).hexdigest(),
                'launch_sha256':sha256(launch_raw).hexdigest(),'code_sha256':sha256(Path(__file__).read_bytes()).hexdigest()})
    require((observer_directory/'manifest.json').read_bytes() == observer_manifest_raw, 'Observer manifest changed')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observer-directory', type=Path, required=True)
    parser.add_argument('--analysis-directory', type=Path, required=True)
    parser.add_argument('--launch', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Refusing to overwrite join')
    result = join(args.observer_directory, args.analysis_directory, args.launch)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({'status':result['status'],'qualified_payloads':result['qualified_unique_payloads']}))


if __name__ == '__main__':
    main()
