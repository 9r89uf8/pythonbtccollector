"""Offline v6/contract4 reliability-canary scoring; never opens a service.

The versioned verifier is adapted from the earlier independent combined audit.
Prices remain Decimal(80); descriptive quantiles interpolate at (n-1)*q.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import localcontext
from hashlib import sha256
import json
from pathlib import Path
import re

from research.spot_twap_response.reliability_canary import _audit_v6 as evidence

HORIZONS = evidence.HORIZONS
MS, SECOND = evidence.MS, evidence.SECOND
require, read_json, integer = evidence.require, evidence.read_json, evidence.integer
PANELS = ('all_decisions', 'complete_120s_window', 'after_65s_complete_120s_window', 'tail_under_120s')


def analyze(input_path, *, run_id, campaign_start_ms, stop_request_wall_ns,
            expected_sha256, expected_rows):
    path = Path(input_path)
    require(not path.name.endswith('.part'), 'Only a completed export may be scored')
    require(re.fullmatch('[0-9a-f]{64}', expected_sha256) is not None, 'Invalid expected export hash')
    require(type(expected_rows) is int and 0 < expected_rows <= 600000, 'Invalid expected count')
    require(type(campaign_start_ms) is int and type(stop_request_wall_ns) is int and
            campaign_start_ms * MS < stop_request_wall_ns, 'Invalid campaign/stop clocks')
    audits = {name: evidence.Audit(campaign_start_ms) for name in PANELS}
    complete_end = stop_request_wall_ns - 120 * SECOND
    digest, seen, count = sha256(), set(), 0
    target_events, target_sequences = {}, {}

    def remember(event):
        if event is None:
            return
        evidence.event_check(event, 'twap')
        seq = event['sequence']
        require(seq not in target_sequences or target_sequences[seq] == event,
                'Conflicting same-run TWAP sequence evidence')
        target_sequences[seq] = event
        stamp = str(event['source_timestamp_ms'])
        item = target_events.setdefault(stamp, {'earliest': event, 'values': set()})
        item['values'].add(event['value'])
        if integer(event['received_monotonic_ns']) < integer(item['earliest']['received_monotonic_ns']):
            item['earliest'] = event

    with localcontext(evidence.CONTEXT), path.open('rb') as source:
        while True:
            raw = source.readline(1024 * 1024 + 1)
            if not raw:
                break
            require(len(raw) <= 1024 * 1024 and raw.endswith(b'\n'), 'Oversized/truncated export row')
            digest.update(raw)
            row = read_json(raw)
            require(isinstance(row, dict) and set(row) == evidence.FIELDS, 'Export field contract')
            identity = row['run_id'], row['decision_id']
            require(all(isinstance(x, str) and x for x in identity), 'Invalid export identity')
            require(identity not in seen, 'Duplicate exported decision')
            seen.add(identity)
            count += 1
            require(count <= expected_rows, 'Too many exported rows')
            require(type(row['version']) is int and row['version'] > 0 and type(row['terminal']) is bool,
                    'Invalid export version/terminal')
            for name in ('frozen', 'state'):
                require(isinstance(row[name + '_json'], str) and
                        sha256(row[name + '_json'].encode()).hexdigest() == row[name + '_sha256'],
                        'Per-row ' + name + ' hash mismatch')
            frozen = read_json(row['frozen_json'])
            require((frozen['run_id'], frozen['decision_id']) == identity, 'Export/frozen identity mismatch')
            wall = integer(frozen['decision_wall_ns'])
            require(wall == integer(row['decision_wall_ns']) and type(row['created_ms']) is int
                    and wall // MS == row['created_ms'], 'Export decision clock mismatch')
            if row['run_id'] != run_id:
                continue
            require(frozen['runtime_policy']['canary_start_ms'] == campaign_start_ms,
                    'Selected run has another campaign start')
            require(wall < stop_request_wall_ns, 'Decision is after actual stop request')
            state = read_json(row['state_json'])
            chosen = ['all_decisions']
            if wall <= complete_end:
                chosen.append('complete_120s_window')
                if wall >= (campaign_start_ms + 65000) * MS:
                    chosen.append('after_65s_complete_120s_window')
            else:
                chosen.append('tail_under_120s')
            try:
                verified = None
                for name in chosen:
                    verified = audits[name].row(row, frozen, state, verified=verified)
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError('Selected row ' + repr(identity) + ': ' + str(exc)) from exc
            remember(frozen['current_twap'])
            for target in state['targets'].values():
                for field in ('first_event', 'first_late_event', 'first_conflicting_event'):
                    remember(target.get(field))
            publication = state['publication']
            if publication.get('attempt_monotonic_ns') is not None and publication.get('payload_json'):
                key = sha256(publication['payload_json'].encode()).hexdigest()
                entry = audits['all_decisions'].payload_index[key]
                entry['decision_wall_ns'] = str(wall)
                entry['decision_monotonic_ns'] = str(integer(frozen['decision_monotonic_ns']))
                entry['causality_invalid'] = bool(state.get('causality_invalid'))
                entry['forecasts'] = {str(f['horizon_s']): {
                    'target_source_timestamp_ms': f['target_source_timestamp_ms'],
                    'price': f['price'], 'quality': f['quality'],
                    'target_status': state['targets'][str(f['horizon_s'])]['status'],
                    'target_conflicted': state['targets'][str(f['horizon_s'])]['conflicted'],
                    'target_clock_anomaly': bool(state['targets'][str(f['horizon_s'])].get('clock_anomaly')),
                    'first_event': state['targets'][str(f['horizon_s'])].get('first_event'),
                } for f in frozen['forecasts']}
        require(count == expected_rows and digest.hexdigest() == expected_sha256,
                'Completed export count/SHA mismatch')
        require(audits['all_decisions'].counts['selected_rows'] > 0, 'Requested run absent')
        panels = {name: audit.summary() for name, audit in audits.items()}
    for item in target_events.values():
        item['distinct_values'] = len(item.pop('values'))
    all_panel = panels['all_decisions']
    result = dict(status='accepted', run_id=run_id, full_export_rows=count,
        other_run_rows_excluded=count - all_panel['counts']['selected_rows'],
        campaign_start_ms=campaign_start_ms, planned_end_ms=campaign_start_ms + evidence.HOUR_MS,
        actual_stop_request_wall_ns=str(stop_request_wall_ns), complete_window_last_decision_wall_ns=str(complete_end),
        runtime_version='ghost-canary-v6', contract_version=4, panels=panels,
        target_evidence_summary={'unique_saved_twap_sequences': len(target_sequences),
            'unique_source_timestamps': len(target_events),
            'timestamps_with_multiple_saved_prices': sum(v['distinct_values'] > 1 for v in target_events.values())},
        method={
            'primary_panel': 'complete_120s_window',
            'complete_window_rule': 'decision_wall_ns +120s <= actual stop-request wall ns; all decisions retained in separate panel',
            'warmup_sensitivity': 'after_65s_complete_120s_window starts at declared campaign start +65s; not a claim about actual startup duration',
            'all_matched': 'calculated_matched_valid: available forecast with exact first target, excluding conflict/future-clock/causality-invalid evidence',
            'acknowledged': 'exact eligible horizon/stamp/price/quality in the immutable attempted payload and acknowledged publication',
            'early': 'acknowledgement monotonic time strictly precedes recorded first target receipt',
            'error': 'forecast or held current-TWAP anchor minus the exact official target; basis points divide by target price',
            'quantile': 'linear interpolation at (n-1)*q, Decimal precision80/ROUND_HALF_EVEN',
            'latency': 'local recorded monotonic stage clocks; intent-to-attempt includes scheduling/outbox work, not pure fsync',
            'counter_limit': 'Frozen counter maxima can lag final shutdown; lack of a saved source/Redis gap is not proof no connection changed.',
            'redis_reconnect': 'Not inferable from decision audit. Requires separate subscriber logs or live connection instrumentation.',
            'causal_evidence_limit': 'Saved selected slots/inputs prove arithmetic and recorded receipt causality, not full input capture completeness.'})
    index = dict(run_id=run_id, payloads=audits['all_decisions'].payload_index, target_events=target_events)
    return result, index


def report(result):
    lines = ['# Reliability canary: offline audit', '',
             f"Run `{result['run_id']}`; {result['full_export_rows']} full-export rows verified.",
             f"Complete-window cutoff: decision ≤ `{result['complete_window_last_decision_wall_ns']}` wall ns.", '']
    for name in ('complete_120s_window', 'all_decisions', 'after_65s_complete_120s_window'):
        panel = result['panels'][name]
        lines += [f'## {name}', '',
            '| h | Calculated | Valid matched | ACK eligible | ACK valid pairs | Early | Ghost median / p90 $ | Hold median / p90 $ | Ghost median / p90 bp | Hold median / p90 bp | Early lead median ms |',
            '|---:|---:|---:|---:|---:|---:|---|---|---|---|---:|']
        for row in panel['horizons']:
            c, e = row['counts'], row['errors']['acknowledged_matched_valid']
            show = lambda x: f"{x['p50']} / {x['p90']}"
            lines.append(f"| {row['horizon_s']} | {c.get('calculated',0)} | {row['errors']['calculated_matched_valid']['n']} | {c.get('acknowledged_eligible',0)} | {e['n']} | {c.get('confirmed_early',0)} | {show(e['ghost_absolute_error'])} | {show(e['persistence_absolute_error'])} | {show(e['ghost_absolute_error_bps'])} | {show(e['persistence_absolute_error_bps'])} | {row['confirmed_lead_ms']['p50']} |")
        lines += ['', 'Error columns use valid acknowledged pairs; full JSON also reports all valid calculated pairs and strict early pairs.', '']
    lines += ['## Publication latency: all decisions', '', '| Stage | n | Median ms | p90 ms | p99 ms |', '|---|---:|---:|---:|---:|']
    for name, values in result['panels']['all_decisions']['latency_ms'].items():
        lines.append(f"| {name} | {values['n']} | {values['p50']} | {values['p90']} | {values['p99']} |")
    lines += ['', 'Saved counter maxima, unavailable reasons, exact reconnect episodes and tail target missingness are in summary.json.',
              'This run does not establish profitability, execution fills or comparability to a different market hour.', '']
    return '\n'.join(lines)


def write_results(input_path, *, output, index_path, **kwargs):
    output, index_path = Path(output), Path(index_path)
    require(not output.exists() and not index_path.exists(), 'Refusing to overwrite analysis')
    code_paths = (Path(__file__), Path(evidence.__file__))
    hashes = {p.name: sha256(p.read_bytes()).hexdigest() for p in code_paths}
    result, index = analyze(input_path, **kwargs)
    require(hashes == {p.name: sha256(p.read_bytes()).hexdigest() for p in code_paths}, 'Scorer changed while running')
    index_path.parent.mkdir(parents=True, exist_ok=True)
    raw_index = (json.dumps(index, separators=(',', ':'), sort_keys=True) + '\n').encode()
    with index_path.open('xb') as stream:
        stream.write(raw_index)
    output.mkdir(parents=True, exist_ok=False)
    artifacts = {'summary.json': (json.dumps(result, indent=2, sort_keys=True) + '\n').encode(),
                 'tables.md': report(result).encode()}
    for name, raw in artifacts.items():
        with (output / name).open('xb') as stream:
            stream.write(raw)
    manifest = dict(status='accepted', created_utc=datetime.now(timezone.utc).isoformat(),
        input_path=str(Path(input_path).resolve()), input_sha256=kwargs['expected_sha256'],
        expected_export_rows=kwargs['expected_rows'], run_id=kwargs['run_id'],
        campaign_start_ms=kwargs['campaign_start_ms'], campaign_end_ms=kwargs['campaign_start_ms'] + evidence.HOUR_MS,
        actual_stop_request_wall_ns=str(kwargs['stop_request_wall_ns']), code_sha256=hashes,
        index_path=str(index_path.resolve()), index_sha256=sha256(raw_index).hexdigest(),
        artifacts_sha256={name: sha256(raw).hexdigest() for name, raw in artifacts.items()})
    with (output / 'manifest.json').open('x', encoding='utf-8') as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write('\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--campaign-start-ms', type=int, required=True)
    parser.add_argument('--stop-request-wall-ns', type=int, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--expected-rows', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--index-path', type=Path, required=True)
    args = vars(parser.parse_args())
    args['input_path'] = args.pop('input')
    result = write_results(**args)
    print(json.dumps({'status': result['status'], 'selected_rows': result['panels']['all_decisions']['counts']['selected_rows']}))


if __name__ == '__main__':
    main()
