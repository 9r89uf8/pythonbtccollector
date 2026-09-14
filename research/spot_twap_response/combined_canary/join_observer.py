"""Join sampled Redis bytes to the independently verified campaign audit.

An observed uncertain write proves cache presence, not acknowledged lead. This
join never updates the audit or promotes its publication status.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path

from price_collector.ghost_twap_observer import analyze as analyze_observer


def require(ok, message):
    if not ok:
        raise ValueError(message)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'Duplicate JSON key')
        result[key] = value
    return result


def reject(value):
    raise ValueError('Non-integral JSON number: ' + value)


def decode(raw):
    return json.loads(raw, object_pairs_hook=pairs, parse_float=reject, parse_constant=reject)


def read(path, limit):
    require(path.stat().st_size <= limit, 'Artifact exceeds size limit')
    return path.read_bytes()


def checked_lines(path, maximum_line, expected):
    require(path.stat().st_size <= 128 * 1024**2, 'Observer artifact exceeds cap')
    digest, size = sha256(), 0
    with path.open('rb') as stream:
        while True:
            line = stream.readline(maximum_line + 1)
            if not line:
                break
            size += len(line)
            require(size <= 128 * 1024**2 and len(line) <= maximum_line and line.endswith(b'\n'),
                    'Truncated or oversized observer record')
            digest.update(line)
            yield decode(line)
    require(dict(bytes=size, sha256=digest.hexdigest()) == expected, 'Observer artifact changed during join')


def join_records(coverage, index, payloads):
    qualified = coverage['observed_qualified_payload_sha256']
    require(len(set(qualified)) == len(qualified), 'Duplicate observed hash')
    statuses, runs = Counter(), set()
    for digest in qualified:
        require(digest in index, 'Observed payload absent from verified campaign audit: ' + digest)
        require(digest in payloads, 'Missing exact observed bytes')
        raw = payloads[digest]
        require(sha256(raw).hexdigest() == digest, 'Observed bytes hash mismatch')
        payload, entry = decode(raw), index[digest]
        require(entry['status'] in ('acknowledged', 'uncertain'), 'Observed payload has a non-write audit outcome')
        require(payload['run_id'] == entry['run_id'] and payload['decision_id'] == entry['decision_id'],
                'Observed/audit identity mismatch')
        require(sorted(payload['publication_eligibility']['eligible_horizons']) == entry['eligible_horizons'],
                'Observed/audit membership mismatch')
        for clock in ('wall', 'monotonic'):
            require(str(payload['publication_attempt_' + clock + '_ns']) == entry['attempt_' + clock + '_ns'],
                    'Observed/audit attempt clock mismatch')
        statuses[entry['status']] += 1
        runs.add(entry['run_id'])
    return dict(qualified_unique_payloads=len(qualified), verified_unique_payloads=len(qualified),
                unique_payloads_by_final_audit_status=dict(statuses), verified_run_ids=sorted(runs),
                interpretation='Exact cache bytes matched a verified attempt; final audit status and lead credit are unchanged.')


def build(observer_directory, analysis_directory):
    observer_directory, analysis_directory = Path(observer_directory), Path(analysis_directory)
    manifest_raw = read(analysis_directory / 'manifest.json', 65536)
    manifest = decode(manifest_raw)
    require(manifest['status'] == 'accepted', 'Audit analysis was not accepted')
    artifacts = {}
    for name, limit in (('summary.json', 16 * 1024**2), ('payload_index.json', 16 * 1024**2)):
        raw = read(analysis_directory / name, limit)
        require(sha256(raw).hexdigest() == manifest['artifacts_sha256'][name], 'Audit analysis hash mismatch')
        artifacts[name] = decode(raw)
    observer_manifest_raw = read(observer_directory / 'manifest.json', 65536)
    observer_manifest = decode(observer_manifest_raw)
    coverage = analyze_observer(observer_directory)
    require(coverage['start_ms'] == manifest['campaign_start_ms']
            and coverage['end_ms'] == manifest['campaign_end_ms_exclusive'], 'Campaign interval mismatch')
    payloads = {}
    for row in checked_lines(observer_directory / 'payloads.jsonl', 100000, observer_manifest['files']['payloads.jsonl']):
        payloads[row['sha256']] = base64.b64decode(row['raw_base64'], validate=True)
    membership = join_records(coverage, artifacts['payload_index.json'], payloads)
    sample_statuses = Counter()
    for row in checked_lines(observer_directory / 'samples.jsonl', 16384, observer_manifest['files']['samples.jsonl']):
        digest = row.get('payload_sha256')
        if row.get('payload_valid') and row.get('campaign_qualified'):
            require(digest in artifacts['payload_index.json'], 'Observed sample absent from campaign audit')
            sample_statuses[artifacts['payload_index.json'][digest]['status']] += 1
    require(read(observer_directory / 'manifest.json', 65536) == observer_manifest_raw, 'Observer manifest changed during join')
    membership['qualified_sample_bins_by_final_audit_status'] = dict(sample_statuses)
    coverage.pop('observed_qualified_payload_sha256')
    return dict(coverage=coverage, membership=membership,
                provenance=dict(audit_manifest_sha256=sha256(manifest_raw).hexdigest(),
                    audit_export_sha256=manifest['input_sha256'],
                    observer_manifest_sha256=sha256(observer_manifest_raw).hexdigest(),
                    code_sha256=sha256(Path(__file__).read_bytes()).hexdigest()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observer-directory', type=Path, required=True)
    parser.add_argument('--analysis-directory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Output already exists')
    result = build(args.observer_directory, args.analysis_directory)
    with args.output.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')


if __name__ == '__main__':
    main()
