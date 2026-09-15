"""Streaming offline audit metadata and exact browser/publication-byte check.

No runtime, collector, database, or existing analyzer imports. This verifies
recorded evidence, not forecast means, omitted inputs, or DB export ACKs.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

RUN = 'd31ac97c83fb4635bc64fef79fedafa9'
HORIZONS = ('1', '2', '3', '5', '10', '30')
VERSION = 'checkpoint-c-audit-check-v1'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def invalid_constant(value):
    raise ValueError('nonfinite JSON constant')


DECODER = json.JSONDecoder(parse_float=Decimal, parse_constant=invalid_constant, object_pairs_hook=pairs)


def decode(value):
    return DECODER.decode(value.decode('utf-8') if isinstance(value, bytes) else value)


def sha(value):
    return hashlib.sha256(value).hexdigest()


def file_hash(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(1_048_576), b''):
            result.update(part)
    return result.hexdigest()


def exact_producer_json(data):
    """Recover the literal producer slice inserted by the API SSE framer."""
    envelope = decode(data)
    require(isinstance(envelope, dict) and list(envelope) == ['api', 'ghost'], 'unexpected SSE envelope layout')
    offset = 0
    while data[offset].isspace():
        offset += 1
    require(data[offset] == '{', 'SSE object required')
    offset += 1
    for expected_key in ('api', 'ghost'):
        while data[offset].isspace():
            offset += 1
        key, offset = DECODER.raw_decode(data, offset)
        require(key == expected_key, 'unexpected SSE key order')
        while data[offset].isspace():
            offset += 1
        require(data[offset] == ':', 'SSE colon required')
        begin = offset+1
        offset = begin
        while data[offset].isspace():
            offset += 1
        _, offset = DECODER.raw_decode(data, offset)
        while data[offset].isspace():
            offset += 1
        if expected_key == 'ghost':
            require(data[offset] == '}' and not data[offset+1:].strip(), 'SSE closing object required')
            return envelope, data[begin:offset].encode('utf-8')
        require(data[offset] == ',', 'SSE separator required')
        offset += 1
    raise AssertionError('unreachable')


def browser_payloads(path, run):
    expected, records, duplicates, foreign = {}, 0, 0, 0
    start = end = None
    observation = None
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', newline='') as stream:
        for line in stream:
            records += 1
            require(len(line) <= 262_144 and records <= 100_000, 'browser input exceeds bound')
            record = decode(line)
            if record['kind'] == 'start':
                require(start is None, 'duplicate start')
                start = Decimal(str(record['browser_ms']))
                observation = int(record['observation_ms'])
            elif record['kind'] == 'end':
                end = Decimal(str(record['browser_ms']))
            elif record['kind'] == 'ghost':
                envelope, raw = exact_producer_json(record['data'])
                payload = envelope['ghost']
                if payload is None:
                    continue
                if payload['run_id'] != run:
                    foreign += 1
                    continue
                require(start is not None, 'browser start missing')
                key = payload['decision_id']
                fingerprint = sha(raw)
                eligible = payload['publication_eligibility']['eligible_horizons']
                require(eligible == [int(h) for h in HORIZONS if int(h) in eligible], 'invalid browser membership')
                for forecast in payload['forecasts']:
                    h = forecast['horizon_s']
                    require((forecast['price'] is not None) == (h in eligible), 'browser price/membership mismatch')
                if key in expected:
                    require(expected[key]['sha256'] == fingerprint, 'browser identity has differing producer bytes')
                    duplicates += 1
                    continue
                elapsed = Decimal(str(record['browser_ms']))-start
                expected[key] = dict(sha256=fingerprint, eligible=tuple(str(h) for h in eligible),
                                     admitted=0 <= elapsed < observation)
    require(start is not None and end is not None and end-start >= observation+120_000, 'complete browser capture required')
    return expected, dict(path=str(path.resolve()), sha256=file_hash(path), bytes=path.stat().st_size,
        records=records, unique_run_payloads=len(expected), duplicate_run_payloads=duplicates,
        other_run_envelopes=foreign, admission_unique_payloads=sum(item['admitted'] for item in expected.values()))


def summarize(audit, manifest_path, browser, run):
    expected, browser_info = browser_payloads(browser, run)
    manifest = decode(manifest_path.read_bytes())
    before = audit.stat()
    digest = hashlib.sha256()
    seen, selected_ids, matched_ids = set(), [], set()
    statuses, terminals, versions, shutdowns = Counter(), Counter(), Counter(), Counter()
    target_statuses = {h: Counter() for h in HORIZONS}
    target_flags = {h: Counter() for h in HORIZONS}
    counters, reconnect, reasons = {}, Counter(), Counter()
    unique_gaps, operational_gaps, suspension_history = {}, {}, {}
    joins, admission_joins = Counter(), Counter()
    eligible_joins, admission_eligible_joins = Counter(), Counter()
    errors = Counter()
    examples = []
    created, restart_ids, restart_target_ids = [], [], []
    gap_rows = max_gap = total = selected = 0
    runtime_versions, contract_versions, campaign_starts = set(), set(), set()
    latest = None
    with audit.open('rb') as stream:
        while True:
            raw = stream.readline(1_048_577)
            if not raw:
                break
            require(len(raw) <= 1_048_576 and raw.endswith(b'\n'), 'audit row bound/truncation')
            digest.update(raw)
            row = decode(raw)
            total += 1
            identity = (row['run_id'], row['decision_id'])
            require(identity not in seen, 'duplicate audit identity')
            seen.add(identity)
            for field in ('frozen', 'state'):
                if sha(row[field+'_json'].encode('utf-8')) != row[field+'_sha256']:
                    errors[field+'_text_hash_mismatch'] += 1
            if row['run_id'] != run:
                continue
            selected += 1
            decision = int(row['decision_id'])
            selected_ids.append(decision)
            frozen, state = decode(row['frozen_json']), decode(row['state_json'])
            require(frozen['run_id'] == run and frozen['decision_id'] == row['decision_id']
                    and int(frozen['decision_wall_ns']) == row['decision_wall_ns'], 'frozen/outer identity mismatch')
            require(type(row['terminal']) is bool, 'terminal must be bool')
            terminals[str(row['terminal']).lower()] += 1
            versions[str(row['version'])] += 1
            created.append(row['created_ms'])
            runtime_versions.add(frozen['runtime_version'])
            contract_versions.add(frozen['contract_version'])
            campaign_starts.add(frozen['runtime_policy']['canary_start_ms'])
            pub = state['publication']
            statuses[pub['status']] += 1
            if state.get('restart_reconciled'):
                restart_ids.append(decision)
            if state.get('shutdown'):
                shutdowns[state['shutdown']] += 1
            row_restart_target = False
            require(set(state['targets']) == set(HORIZONS), 'target horizon set differs')
            for h, target in state['targets'].items():
                target_statuses[h][target['status']] += 1
                row_restart_target |= target['status'] == 'restart_unmatched'
                for flag in ('conflicted', 'clock_anomaly', 'causality_invalid'):
                    target_flags[h][flag] += int(target.get(flag) is True)
                target_flags[h]['first_event_present'] += int(target.get('first_event') is not None)
                target_flags[h]['recorded_confirmed_redis_lead_present'] += int(target.get('confirmed_redis_lead_ns') is not None)
            if row_restart_target:
                restart_target_ids.append(decision)
            for name, value in frozen.get('runtime_counters', {}).items():
                require(type(value) is int and value >= 0, 'invalid frozen runtime counter')
                counters[name] = max(counters.get(name, 0), value)
            gap_count = frozen.get('gap_count', 0)
            max_gap = max(max_gap, gap_count)
            gap_rows += int(gap_count > 0)
            reasons.update(frozen.get('reasons', []))
            recovery = frozen.get('spot_reconnect')
            reconnect['none' if recovery is None else recovery['status']] += 1
            for name, bucket in (('last_gaps', unique_gaps), ('operational_gaps', operational_gaps),
                                 ('runtime_suspension_history', suspension_history)):
                for event in frozen.get(name, []):
                    bucket[json.dumps(event, sort_keys=True, separators=(',', ':'))] = event
            if latest is None or decision > latest['decision_id']:
                latest = dict(decision_id=decision, created_ms=row['created_ms'], runtime_counters=frozen.get('runtime_counters', {}),
                              horizon_recovery=frozen.get('horizon_recovery', {}))
            if row['decision_id'] in expected:
                delivered = expected[row['decision_id']]
                attempted = pub.get('payload_json')
                if attempted is None:
                    errors['browser_delivery_without_attempted_bytes'] += 1
                elif sha(attempted.encode('utf-8')) != delivered['sha256']:
                    errors['browser_attempted_bytes_mismatch'] += 1
                else:
                    matched_ids.add(row['decision_id'])
                    joins[pub['status']] += 1
                    eligible_joins.update(delivered['eligible'])
                    if delivered['admitted']:
                        admission_joins[pub['status']] += 1
                        admission_eligible_joins.update(delivered['eligible'])
                    continue
                if len(examples) < 30:
                    examples.append(row['decision_id'])
    after = audit.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), 'audit changed during scan')
    if digest.hexdigest() != manifest['sha256']:
        errors['export_sha256_mismatch'] += 1
    if total != manifest['row_count']:
        errors['export_row_count_mismatch'] += 1
    missing = sorted(set(expected)-{str(item) for item in selected_ids}, key=int)
    if missing:
        errors['browser_payload_missing_from_audit'] = len(missing)
    require(selected > 0, 'requested run absent')
    ordered = sorted(selected_ids)
    return dict(version=VERSION, status='passed' if not errors else 'failed', run_id=run,
        audit=dict(path=str(audit.resolve()), bytes=after.st_size, sha256=digest.hexdigest(),
                   manifest_path=str(manifest_path.resolve()), manifest_sha256=file_hash(manifest_path),
                   manifest_expected_sha256=manifest['sha256'], total_rows=total, old_or_other_run_rows=total-selected,
                   frozen_and_state_text_hashes_checked=2*total),
        run=dict(rows=selected, first_decision_id=ordered[0], last_decision_id=ordered[-1],
            decision_ids_contiguous=ordered == list(range(ordered[0], ordered[-1]+1)),
            first_created_ms=min(created), last_created_ms=max(created),
            runtime_versions=sorted(runtime_versions), contract_versions=sorted(contract_versions),
            campaign_start_ms=sorted(campaign_starts), terminal_counts=dict(terminals), publication_statuses=dict(statuses),
            persistence_versions=dict(versions), target_statuses={h: dict(v) for h, v in target_statuses.items()},
            target_flags={h: dict(v) for h, v in target_flags.items()},
            restart_reconciled_rows=len(restart_ids), restart_reconciled_decision_ids=sorted(restart_ids),
            rows_with_restart_unmatched_targets=len(restart_target_ids), restart_unmatched_decision_ids=sorted(restart_target_ids),
            shutdown_markers=dict(shutdowns), frozen_runtime_counter_maxima=counters, latest_frozen=latest,
            rows_with_nonzero_gap_count=gap_rows, maximum_gap_count=max_gap, distinct_last_gaps=list(unique_gaps.values()),
            distinct_operational_gaps=list(operational_gaps.values()), distinct_runtime_suspensions=list(suspension_history.values()),
            frozen_spot_reconnect_state_counts=dict(reconnect), frozen_global_reason_counts=dict(reasons)),
        browser=dict(browser_info, exact_attempted_byte_matches=len(matched_ids),
            matched_audited_publication_statuses=dict(joins), matched_eligible_horizons=dict(eligible_joins),
            admission_matched_publication_statuses=dict(admission_joins), admission_matched_eligible_horizons=dict(admission_eligible_joins),
            missing_audit_decision_ids=missing, byte_mismatch_examples=examples),
        errors=dict(errors), code_sha256=file_hash(Path(__file__)),
        limitations=[
            'Browser byte equality proves the delivered object matches recorded attempted bytes. It does not time or prove a Redis acknowledgement.',
            'Publication status and target flags are summarized as recorded; this check does not independently recompute forecast prices or confirmed lead.',
            'restart_unmatched denotes unobserved target tails after shutdown/recovery, not forecast losses.',
            'Frozen counters are snapshots, commonly one decision behind; maxima do not replace final process counters or logs.',
            'Absent gap/reconnect records in these snapshots do not establish complete feed history.',
            'The offline export checksum/row verification does not establish completion of external database export acknowledgements.'])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--browser', type=Path, required=True)
    parser.add_argument('--run-id', default=RUN)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    require(not args.output.exists(), 'output exists; refusing overwrite')
    result = summarize(args.audit, args.manifest, args.browser, args.run_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    print(json.dumps(dict(status=result['status'], rows=result['run']['rows'],
                         browser_matches=result['browser']['exact_attempted_byte_matches'], errors=result['errors'])))
    if result['status'] != 'passed':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
