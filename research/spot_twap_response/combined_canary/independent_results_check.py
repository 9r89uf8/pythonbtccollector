"""Independent, offline check of the completed combined-canary result metrics.

No scorer, runtime, engine, or service imports. This checks denominators and
recomputes target errors from saved forecasts, not the forecast model itself.
All financial arithmetic and linear quantiles use Decimal precision 80.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from hashlib import sha256
import json
from pathlib import Path

CAMPAIGN_START_MS = 1789425244002
EXPECTED_RUN = '9b3cf7d21f9640de989062ddd4001d5b'
EXPECTED_ALL, EXPECTED_SELECTED = 14346, 7054
HORIZONS = (1, 2, 3, 5, 10, 30)
REPORT_HORIZONS = (5, 10, 30)
MS, SECOND = 1_000_000, 1_000_000_000
E18 = Decimal('0.000000000000000001')


def check(ok, message):
    if not ok:
        raise ValueError(message)


def no_float(value):
    raise ValueError('Forbidden floating/nonfinite JSON number: ' + value)


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        check(key not in result, 'Duplicate JSON key: ' + key)
        result[key] = value
    return result


def read_json(raw):
    return json.loads(raw, parse_float=no_float, parse_constant=no_float,
                      object_pairs_hook=object_pairs)


def number(text):
    check(isinstance(text, str), 'Financial value is not a string')
    value = Decimal(text)
    check(value.is_finite() and value > 0 and value == value.quantize(E18), 'Invalid positive E18 price')
    return value


def quantiles(values):
    ordered = sorted(values)
    def percentile(numerator):
        if not ordered:
            return None
        scaled = (len(ordered) - 1) * numerator
        index, remainder = divmod(scaled, 100)
        low, high = ordered[index], ordered[min(index + 1, len(ordered) - 1)]
        return str((low * (100 - remainder) + high * remainder) / Decimal(100))
    return dict(n=len(ordered), median=percentile(50), p90=percentile(90))


def errors(pairs):
    return dict(n=len(pairs),
        ghost_absolute_dollars=quantiles([abs(g) for g, _, _ in pairs]),
        persistence_absolute_dollars=quantiles([abs(p) for _, p, _ in pairs]),
        ghost_absolute_bps=quantiles([abs(g) * Decimal(10000) / actual for g, _, actual in pairs]),
        persistence_absolute_bps=quantiles([abs(p) * Decimal(10000) / actual for _, p, actual in pairs]),
        ghost_better=sum(abs(g) < abs(p) for g, p, _ in pairs),
        equal=sum(abs(g) == abs(p) for g, p, _ in pairs),
        ghost_worse=sum(abs(g) > abs(p) for g, p, _ in pairs))


def membership(frozen, state):
    pub = state['publication']
    if not pub.get('payload_json'):
        check(pub['status'] not in ('acknowledged', 'uncertain'), 'Publication lacks attempted payload')
        return set()
    body = read_json(pub['payload_json'])
    check(body['run_id'] == frozen['run_id'] and body['decision_id'] == frozen['decision_id'], 'Wire identity mismatch')
    check(body['runtime_version'] == 'ghost-canary-v5' and body['contract_version'] == 3, 'Wrong wire contract')
    mask = body.get('publication_eligibility')
    check(isinstance(mask, dict) and type(mask.get('version')) is int and mask['version'] == 1, 'Invalid publication mask')
    selected, omitted = mask['eligible_horizons'], mask['excluded_horizons']
    check(isinstance(selected, list) and all(type(h) is int for h in selected) and
          selected == [h for h in HORIZONS if h in selected], 'Invalid/duplicate mask horizons')
    check(set(omitted) == {str(h) for h in HORIZONS if h not in selected}, 'Mask does not partition all horizons')
    check([f['horizon_s'] for f in body['forecasts']] == list(HORIZONS), 'Wire forecast identities differ')
    attempted = pub.get('attempt_monotonic_ns') is not None
    check(body['publication_state'] == ('attempted' if attempted else 'intent'), 'Publication attempt state mismatch')
    if attempted:
        for clock in ('wall_ns', 'monotonic_ns'):
            check(int(mask['checked_' + clock]) == int(pub['attempt_' + clock]) ==
                  int(body['publication_attempt_' + clock]), 'Attempt/mask clock mismatch')
    else:
        check(pub['status'] not in ('acknowledged', 'uncertain'), 'ACK/uncertainty without an attempt')
    for original, transmitted in zip(frozen['forecasts'], body['forecasts']):
        h = original['horizon_s']
        target = state['targets'][str(h)]
        if h in selected:
            check(original['price'] is not None and transmitted == original and
                  transmitted['quality'] in ('healthy', 'degraded'), 'Selected price/target/quality does not match frozen evidence')
            if target.get('first_event') is not None:
                check(int(target['first_event']['received_monotonic_ns']) >= int(mask['checked_monotonic_ns']),
                      'Selected target was recorded before selection')
        elif original['price'] is None:
            check(transmitted == original and omitted[str(h)] == original['reasons'], 'Originally unavailable forecast changed')
        else:
            reason = omitted[str(h)]
            check(reason in (['target_received_before_publication'], ['target_not_pending_before_publication']), 'Unknown withholding reason')
            check(transmitted == dict(original, price=None, quality='unavailable', reasons=reason), 'Withheld payload mismatch')
            if reason == ['target_received_before_publication']:
                check(target.get('first_event') is not None and
                      int(target['first_event']['received_monotonic_ns']) <= int(mask['checked_monotonic_ns']),
                      'Withholding lacks prior target receipt')
    return set(selected) if attempted else set()


def verify(input_path, manifest_path):
    input_path, manifest_path = Path(input_path), Path(manifest_path)
    check(not input_path.name.endswith('.part') and not manifest_path.name.endswith('.part'), 'Completed files required')
    manifest_bytes = manifest_path.read_bytes()
    manifest = read_json(manifest_bytes)
    check(manifest['row_count'] == EXPECTED_ALL, 'Unexpected full export row count')
    digest, seen, statuses, counts = sha256(), set(), Counter(), Counter()
    by_h = {h: Counter() for h in HORIZONS}
    acknowledged = {h: [] for h in REPORT_HORIZONS}
    early = {h: [] for h in REPORT_HORIZONS}
    leads = {h: [] for h in REPORT_HORIZONS}
    first_wall, last_wall = None, None
    with localcontext() as context, input_path.open('rb') as source:
        context.prec, context.rounding = 80, ROUND_HALF_EVEN
        # Independent interpolation checks, including an even sample and tie.
        check(quantiles([Decimal(0), Decimal(2)]) == dict(n=2, median='1', p90='1.8'), 'Quantile self-check failed')
        check(quantiles([Decimal(3)] * 3)['median'] == '3', 'Tie quantile self-check failed')
        while True:
            line = source.readline(1024 * 1024 + 1)
            if not line:
                break
            check(len(line) <= 1024 * 1024 and line.endswith(b'\n'), 'Truncated/oversized export line')
            digest.update(line)
            row = read_json(line)
            counts['full_export_rows'] += 1
            check(counts['full_export_rows'] <= EXPECTED_ALL, 'Too many export rows')
            identity = row['run_id'], row['decision_id']
            check(identity not in seen, 'Duplicate decision identity')
            seen.add(identity)
            for part in ('frozen', 'state'):
                check(sha256(row[part + '_json'].encode()).hexdigest() == row[part + '_sha256'], 'Per-row hash mismatch')
            frozen, state = read_json(row['frozen_json']), read_json(row['state_json'])
            check((frozen['run_id'], frozen['decision_id']) == identity, 'Export/frozen identity mismatch')
            if frozen['runtime_policy']['canary_start_ms'] != CAMPAIGN_START_MS:
                counts['old_campaign_rows_excluded'] += 1
                continue
            check(row['run_id'] == EXPECTED_RUN and row['terminal'] is True, 'Unexpected run/incomplete campaign row')
            check(frozen['runtime_version'] == 'ghost-canary-v5' and frozen['contract_version'] == 3, 'Campaign contract mismatch')
            check([f['horizon_s'] for f in frozen['forecasts']] == list(HORIZONS), 'Frozen horizon mismatch')
            wall, mono = int(frozen['decision_wall_ns']), int(frozen['decision_monotonic_ns'])
            check(wall == int(row['decision_wall_ns']) and wall // MS == row['created_ms'], 'Export/decision time mismatch')
            check(CAMPAIGN_START_MS * MS <= wall < (CAMPAIGN_START_MS + 3600000) * MS, 'Out-of-campaign decision')
            first_wall = wall if first_wall is None else min(first_wall, wall)
            last_wall = wall if last_wall is None else max(last_wall, wall)
            counts['selected_rows'] += 1
            pub = state['publication']
            statuses[pub['status']] += 1
            chosen = membership(frozen, state)
            ack = int(pub['ack_monotonic_ns']) if pub['status'] == 'acknowledged' else None
            if ack is not None:
                check(ack >= int(pub['attempt_monotonic_ns']), 'ACK predates attempt')
            available = {f['horizon_s'] for f in frozen['forecasts'] if f['price'] is not None}
            counts['calculated_any_rows'] += bool(available)
            counts['acknowledged_any_eligible_rows'] += ack is not None and bool(chosen)
            counts['partial_acknowledged_rows'] += ack is not None and bool(chosen) and bool(available - chosen)
            counts['causality_invalid_rows'] += bool(state.get('causality_invalid'))
            for f in frozen['forecasts']:
                h, tally = f['horizon_s'], by_h[f['horizon_s']]
                target = state['targets'][str(h)]
                check(target['horizon'] == str(h) and target['target_source_timestamp_ms'] == f['target_source_timestamp_ms'], 'Target identity mismatch')
                tally['rows'] += 1
                tally['calculated'] += f['price'] is not None
                tally['recorded_attempt_eligible'] += h in chosen
                published = ack is not None and h in chosen
                tally['acknowledged_eligible'] += published
                tally['uncertain_eligible'] += pub['status'] == 'uncertain' and h in chosen
                tally['target_' + target['status']] += 1
                tally['conflicted'] += bool(target.get('conflicted'))
                tally['clock_anomaly'] += bool(target.get('clock_anomaly'))
                first = target.get('first_event')
                if target.get('first_conflicting_event'):
                    check(target.get('conflicted') is True, 'Conflict evidence hidden by flag')
                lead = None
                if first is not None:
                    check(target['status'] == 'matched' and f['price'] is not None, 'First event on nonforecast/missing target')
                    check(first['feed'] == 'twap' and first['window_s'] == 60 and
                          first['source_timestamp_ms'] == f['target_source_timestamp_ms'], 'Wrong target event')
                    receipt = int(first['received_monotonic_ns'])
                    check(mono <= receipt < mono + 120 * SECOND, 'Target outside scoring window')
                    check(int(first['received_wall_ns']) >= wall, 'Target wall clock predates decision')
                    actual = number(first['value'])
                    ghost = number(f['price']) - actual
                    baseline = number(frozen['current_twap']['value']) - actual
                    check(target['error'] == format(ghost.quantize(E18), '.18f') and
                          target['persistence_error'] == format(baseline.quantize(E18), '.18f'), 'Stored error arithmetic mismatch')
                    anomalous = first['source_timestamp_ms'] * MS > int(first['received_wall_ns'])
                    check(target.get('clock_anomaly') is anomalous, 'Incorrect target clock flag')
                    valid = not (anomalous or target.get('conflicted') or state.get('causality_invalid'))
                    tally['matched_valid'] += valid
                    if valid and published:
                        tally['acknowledged_matched_valid'] += 1
                        if h in REPORT_HORIZONS:
                            acknowledged[h].append((ghost, baseline, actual))
                        if ack < receipt:
                            lead = receipt - ack
                            tally['confirmed_early'] += 1
                            if h in REPORT_HORIZONS:
                                early[h].append((ghost, baseline, actual))
                                leads[h].append(Decimal(lead) / Decimal(MS))
                recorded = target.get('confirmed_redis_lead_ns')
                check((None if recorded is None else int(recorded)) == lead, 'Confirmed lead differs from independent membership/clock test')
        check(counts['full_export_rows'] == EXPECTED_ALL and counts['selected_rows'] == EXPECTED_SELECTED
              and counts['old_campaign_rows_excluded'] == EXPECTED_ALL - EXPECTED_SELECTED, 'Campaign counts differ from expected export')
        check(digest.hexdigest() == manifest['sha256'], 'Full export SHA-256 mismatch')
        return dict(status='passed', campaign_start_ms=CAMPAIGN_START_MS, campaign_end_ms_exclusive=CAMPAIGN_START_MS + 3600000,
            run_id=EXPECTED_RUN, counts=dict(counts), publication_statuses=dict(statuses),
            observed_decision_wall_ns=dict(first=str(first_wall), last=str(last_wall)),
            per_horizon_counts={str(h): dict(by_h[h]) for h in HORIZONS},
            diagnostics={str(h): dict(acknowledged_matched_valid=errors(acknowledged[h]),
                confirmed_early_matched_valid=errors(early[h]), confirmed_early_lead_ms=quantiles(leads[h])) for h in REPORT_HORIZONS},
            input_sha256=digest.hexdigest(), input_manifest_sha256=sha256(manifest_bytes).hexdigest(),
            code_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
            definitions=[
                'Acknowledged eligible requires exact v5 attempted horizon/target/price/quality and eligibility-mask membership.',
                'Valid pairs use the same recorded first official target for ghost and raw-TWAP persistence; missing/conflicted/future-clock/causality-invalid targets are excluded.',
                'Confirmed early further requires Redis ACK strictly before first target receipt on the local monotonic clock.',
                'Basis points divide absolute price error by that official target value; p50/p90 are Decimal linear-interpolated quantiles.',
                'One-hour decision-weighted descriptions; repeated targets are not independent samples and different canaries are not a controlled accuracy comparison.',
                'No scorer/runtime/model imports; this metric check does not rerun frozen-slot arithmetic or infer browser delivery.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    check(not args.output.exists(), 'Refusing to overwrite result')
    result = verify(args.input, args.manifest)
    result['verified_utc'] = datetime.now(timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8', newline='\n') as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
        handle.write('\n')
    print(json.dumps({'status': result['status'], 'counts': result['counts'],
                      'publication_statuses': result['publication_statuses']}, sort_keys=True))


if __name__ == '__main__':
    main()
