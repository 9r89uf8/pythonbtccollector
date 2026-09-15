"""Reproduce the combined-canary raw recount and observer_gap_breakdown.json.

Run from the repository root with the development Python, for example:

  python research/spot_twap_response/combined_canary/observer_results_check.py \
    --observer-directory /path/to/verified/observer --output /unused/gap.json

The observer analyzer verifies hashes, raw payload classifications and clocks.
The independent sample recount is printed to stdout; --recount-output optionally
retains it at another unused path. The gap JSON uses the same calculation and
serialization as the original review's shell-invoked Python. Existing artifacts
are never overwritten. This script has no network, database or runtime effects.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import sys

# Permit direct invocation without installing research packages in production.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from price_collector.ghost_twap_observer import analyze


HORIZONS = (1, 2, 3, 5, 10, 30)


def utc(ms: int) -> str:
    return (datetime.fromtimestamp(ms // 1000, timezone.utc)
            + timedelta(milliseconds=ms % 1000)).isoformat(timespec='milliseconds')


def _write_new(path: Path, value: dict) -> None:
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write('\n')


def _panel(items: list, expected: dict) -> dict:
    status = Counter(row['status'] for row in items)
    eligible = {str(h): sum(h in row.get('eligible_horizons', []) for row in items)
                for h in HORIZONS}
    usable = {str(h): sum(h in row.get('usable_horizons', []) for row in items)
              for h in HORIZONS}
    unknown = sum(row['status'] in ('error', 'missed', 'unrecorded')
                  or bool(row.get('clock_anomaly')) or bool(row.get('read_crosses_bin_end'))
                  or bool(row.get('read_crosses_campaign_end'))
                  or 'expiry_boundary_ambiguous' in row.get('freshness_reasons', [])
                  for row in items)
    if (len(items) != expected['planned_bins'] or dict(status) != dict(expected['statuses'])
            or unknown != expected['unknown_bins']
            or eligible != {str(h): expected['eligible'].get(str(h), 0) for h in HORIZONS}
            or usable != {str(h): expected['usable'].get(str(h), 0) for h in HORIZONS}):
        raise ValueError('independent recount disagrees with observer analyzer')
    return dict(n=len(items), statuses=status, eligible=eligible, usable=usable, unknown=unknown,
                usable_pct={h: str((Decimal(v)*100/Decimal(len(items))).quantize(Decimal('.001')))
                            for h, v in usable.items()},
                present_pct=str((Decimal(status['present'])*100/Decimal(len(items))).quantize(Decimal('.001'))))


def _streaks(rows: list, predicate, start_ms: int) -> list:
    streaks = []
    for row in rows:
        if predicate(row):
            if streaks and streaks[-1]['last_index'] == row['index']-1:
                streaks[-1]['last_index'] = row['index']
                streaks[-1]['probes'] += 1
            else:
                streaks.append(dict(first_index=row['index'], last_index=row['index'], probes=1))
    for streak in streaks:
        streak.update(first_planned_utc=utc(start_ms+streak['first_index']*100),
                      last_planned_utc=utc(start_ms+streak['last_index']*100))
    return sorted(streaks, key=lambda item: item['probes'], reverse=True)[:5]


def calculate(rows: list, ledger: dict, analysis: dict) -> tuple[dict, dict]:
    """Pure reproduction of the original raw recount and gap calculations."""
    if len(rows) != 36000 or not all(row['index'] == i and row['count'] == 1
                                     for i, row in enumerate(rows)):
        raise ValueError('this completed-canary review requires the recorded 36,000-row grid')
    start_ms = analysis['start_ms']
    post = rows[650:]
    full_panel, post_panel = _panel(rows, analysis['full_hour']), _panel(post, analysis['post_65_seconds'])
    flags = Counter()
    episodes = []
    for row in rows:
        for name in ('clock_anomaly', 'read_crosses_bin_end', 'read_crosses_campaign_end'):
            if row.get(name):
                flags[name] += 1
        flags.update(row.get('freshness_reasons', []))
        if row['status'] == 'absent':
            if episodes and episodes[-1]['last_index'] == row['index']-1:
                episode = episodes[-1]
                episode['last_index'] = row['index']
                episode['probes'] += 1
                episode['last_read_end_ns'] = row['read_end_wall_ns']
            else:
                episodes.append(dict(first_index=row['index'], last_index=row['index'], probes=1,
                                     first_read_start_ns=row['read_start_wall_ns'],
                                     last_read_end_ns=row['read_end_wall_ns']))
    for episode in episodes:
        episode['first_planned_utc'] = utc(start_ms+episode['first_index']*100)
        episode['last_planned_utc'] = utc(start_ms+episode['last_index']*100)
    worst = sorted(enumerate(analysis['minute_bins']),
                   key=lambda pair: (pair[1]['statuses'].get('absent', 0), pair[1]['unknown_bins']),
                   reverse=True)[:10]
    recount = dict(start_utc=utc(start_ms), end_utc=utc(analysis['end_ms']),
                   full=full_panel, post65=post_panel,
                   read_latency_ms={k: str((Decimal(v)/Decimal(1000000)).quantize(Decimal('.000001')))
                                    for k, v in analysis['read_latency'].items()},
                   flags=flags, invalid_payloads=analysis['full_hour']['invalid_payloads'],
                   reason_counts=analysis['full_hour']['reasons'], absence_episode_count=len(episodes),
                   longest_absence_episodes=sorted(episodes, key=lambda item: item['probes'], reverse=True)[:10],
                   worst_minutes=[dict(index=i, start_utc=utc(start_ms+i*60000), **bucket)
                                  for i, bucket in worst],
                   run_ids=analysis['observed_run_ids'],
                   qualified_payload_hashes=len(analysis['observed_qualified_payload_sha256']))
    output = dict(interval='post65seconds', planned_bins=len(post),
                  present_bins=post_panel['statuses']['present'], absent_bins=post_panel['statuses']['absent'],
                  horizons={})
    for h in (5, 10, 30):
        ineligible, examples = [], []
        sets, reasons, counts, gapmarkers = Counter(), Counter(), Counter(), Counter()
        for row in post:
            if row['status'] != 'present' or h in row.get('usable_horizons', []):
                continue
            payload = ledger[row['payload_sha256']]
            forecast = next(f for f in payload['forecasts'] if f['horizon_s'] == h)
            if forecast['price'] is not None:
                raise ValueError('this review expected null prices for present unusable horizons')
            ineligible.append(row)
            key = ' + '.join(sorted(forecast['reasons'])) or '(no reason)'
            sets[key] += 1
            reasons.update(forecast['reasons'])
            counts[str(forecast['counts']['missing'])] += 1
            gapmarkers.update(str(g) for g in payload.get('last_gaps', []))
            if len(examples) < 5 and (not examples or examples[-1]['reason_set'] != key):
                examples.append(dict(index=row['index'], utc=utc(start_ms+row['index']*100),
                                     reason_set=key, forecast=forecast, current_spot=payload['current_spot'],
                                     current_twap=payload['current_twap'], last_gaps=payload.get('last_gaps'),
                                     decision_id=payload['decision_id'], payload_sha256=row['payload_sha256']))
        output['horizons'][str(h)] = dict(
            usable_bins=sum(h in row.get('usable_horizons', []) for row in post),
            present_ineligible_bins=len(ineligible), reason_sets=sets, reason_counts=reasons,
            missing_slot_count_distribution=counts, last_gap_marker_counts=gapmarkers,
            max_unusable_streaks_including_absence=_streaks(post, lambda row: h not in row.get('usable_horizons', []), start_ms),
            max_present_ineligible_streaks=_streaks(post, lambda row: row['status'] == 'present' and h not in row.get('usable_horizons', []), start_ms),
            examples=examples)
    return output, recount


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observer-directory', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--recount-output', type=Path)
    args = parser.parse_args()
    targets = [args.output] + ([args.recount_output] if args.recount_output else [])
    if len({path.resolve() for path in targets}) != len(targets) or any(path.exists() for path in targets):
        raise FileExistsError('choose distinct unused output paths')
    analysis = analyze(args.observer_directory)
    ledger = {}
    for line in (args.observer_directory/'payloads.jsonl').read_bytes().splitlines():
        entry = json.loads(line)
        raw = base64.b64decode(entry['raw_base64'], validate=True)
        if sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('payload hash changed after analysis')
        ledger[entry['sha256']] = json.loads(raw)
    rows = [json.loads(line) for line in (args.observer_directory/'samples.jsonl').read_bytes().splitlines()]
    # A copied artifact must remain stable throughout the local calculation.
    manifest = json.loads((args.observer_directory/'manifest.json').read_bytes())
    for name in ('samples.jsonl', 'payloads.jsonl'):
        raw = (args.observer_directory/name).read_bytes()
        if sha256(raw).hexdigest() != manifest['files'][name]['sha256'] or len(raw) != manifest['files'][name]['bytes']:
            raise ValueError('observer file changed after analysis')
    output, recount = calculate(rows, ledger, analysis)
    _write_new(args.output, output)
    if args.recount_output:
        _write_new(args.recount_output, recount)
    print(json.dumps(recount, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
