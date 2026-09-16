"""Consecutive sampled-bin gaps, keeping unknown probes separate from absence."""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

from price_collector.ghost_twap_observer import analyze as validate_observer
from research.spot_twap_response.combined_canary.join_observer import checked_lines

HORIZONS = (1, 2, 3, 5, 10, 30)


def summarize_runs(flags, first, stop, start_ms, interval_ms):
    runs, opened = [], None
    for i in range(first, stop + 1):
        active = i < stop and flags[i]
        if active and opened is None:
            opened = i
        elif not active and opened is not None:
            runs.append({'first_bin': opened, 'last_bin_inclusive': i - 1,
                'consecutive_bins': i-opened,
                'planned_bin_footprint_ms': (i-opened)*interval_ms,
                'first_planned_ms': start_ms + opened*interval_ms,
                'last_planned_ms': start_ms + (i-1)*interval_ms,
                'first_to_last_planned_probe_span_ms': (i-opened-1)*interval_ms,
                'clipped_at_panel_start': opened == first and first > 0 and flags[first-1],
                'touches_panel_start': opened == first, 'touches_panel_end': i == stop})
            opened = None
    ranked = sorted(runs, key=lambda r: (-r['consecutive_bins'], r['first_bin']))
    return {'bins': sum(bool(x) for x in flags[first:stop]), 'runs': len(runs),
            'longest_consecutive_bins': ranked[0]['consecutive_bins'] if ranked else 0,
            'longest_planned_bin_footprint_ms': ranked[0]['planned_bin_footprint_ms'] if ranked else 0,
            'longest_runs': ranked[:5]}


def analyze(directory):
    directory = Path(directory)
    manifest_raw = (directory/'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    validated = validate_observer(directory)
    states = []
    for row in checked_lines(directory/'samples.jsonl', 16384, manifest['files']['samples.jsonl']):
        if row['index'] != len(states):
            raise ValueError('Observer grid changed')
        unknown = (row['status'] in ('error', 'missed', 'unrecorded')
            or bool(row.get('clock_anomaly')) or bool(row.get('read_crosses_campaign_end'))
            or bool(row.get('read_crosses_bin_end'))
            or 'expiry_boundary_ambiguous' in row.get('freshness_reasons', []))
        states.extend([dict(unknown=unknown, absent=row['status']=='absent',
            eligible=set(row.get('eligible_horizons', [])), usable=set(row.get('usable_horizons', [])))] * row['count'])
    states.extend([dict(unknown=True, absent=False, eligible=set(), usable=set())]
                  * (manifest['planned_bins']-len(states)))
    panels = {}
    for name, first in (('full_hour', 0), ('post_65_seconds', 650)):
        stop = len(states)
        summarize = lambda flags: summarize_runs(flags, first, stop, manifest['start_ms'], manifest['interval_ms'])
        panel = dict(planned_bins=stop-first,
            key_absent_known=summarize([s['absent'] and not s['unknown'] for s in states]),
            unknown=summarize([s['unknown'] for s in states]), horizons={})
        if panel['unknown']['bins'] != validated[name]['unknown_bins']:
            raise ValueError('Unknown denominator differs from validated observer')
        for h in HORIZONS:
            panel['horizons'][str(h)] = {
                'usable_bins': sum(h in s['usable'] for s in states[first:]),
                'known_not_eligible': summarize([not s['unknown'] and h not in s['eligible'] for s in states]),
                'known_eligible_but_not_fresh': summarize([not s['unknown'] and h in s['eligible'] and h not in s['usable'] for s in states]),
                'known_not_usable': summarize([not s['unknown'] and h not in s['usable'] for s in states])}
            if panel['horizons'][str(h)]['usable_bins'] != validated[name]['usable'][str(h)]:
                raise ValueError('Usable denominator differs from validated observer')
            if panel['horizons'][str(h)]['usable_bins'] + panel['horizons'][str(h)]['known_not_usable']['bins'] + panel['unknown']['bins'] != stop-first:
                raise ValueError('Known/usable/unknown bins do not conserve denominator')
        panels[name] = panel
    if (directory/'manifest.json').read_bytes() != manifest_raw:
        raise ValueError('Observer manifest changed')
    return dict(status='accepted', start_ms=manifest['start_ms'], end_ms=manifest['end_ms'],
        interval_ms=manifest['interval_ms'], panels=panels,
        definitions={
            'key_absent_known': 'Successful GET returned no key with valid in-bin clocks. Unknown bins break absence runs.',
            'unknown': 'Error, missed/unrecorded, clock anomaly, read crossing its bin/campaign end, or ambiguous expiry boundary.',
            'known_not_eligible': 'Known bin with no publication-eligible price for this horizon, including absent keys.',
            'known_eligible_but_not_fresh': 'Known bin with an eligible forecast that fails conservative read-end freshness.',
            'known_not_usable': 'Known bin with no usable forecast, including absence, ineligibility and stale payloads. Unknown bins break these runs.',
            'duration': 'Run duration is the count of consecutive100ms planned bins. It is a sampled-bin footprint, not proof the cache was continuously absent/unusable between reads. A single observed probe spans zero time between probe points.',
            'target_condition': 'Usability here is publication membership plus read-end freshness. Exact target-still-unreceived conditions are in observer_join.json.',
            'post65': 'The post65 panel clips any run crossing the scheduled start+65s boundary; clipping is flagged.'},
        provenance={'observer_manifest_sha256': sha256(manifest_raw).hexdigest(),
            'samples': manifest['files']['samples.jsonl'], 'payloads': manifest['files']['payloads.jsonl'],
            'code_sha256': sha256(Path(__file__).read_bytes()).hexdigest()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observer-directory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('Refusing to overwrite gap evidence')
    result = analyze(args.observer_directory)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps({'status':result['status'],'output':str(args.output)}))


if __name__ == '__main__':
    main()
