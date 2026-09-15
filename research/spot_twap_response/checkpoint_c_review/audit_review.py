"""Local review of frozen Checkpoint C recovery, audit metrics, and coverage.

Streams the export and retains only this run's compact scoring/publication
fields. Financial calculations and persisted fractions use Decimal precision80.
No production imports, queries, or mutations. Coverage is reconstructed from
recorded ACK/TTL and current-target evidence, not measured continuous Redis reads.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP, localcontext
import gzip
import hashlib
import json
from pathlib import Path
import tarfile

RUN = 'd31ac97c83fb4635bc64fef79fedafa9'
HS = (1, 2, 3, 5, 10, 30)
NS = 1_000_000
SEC = 1_000_000_000


def load(value):
    return json.loads(value, parse_float=Decimal)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def filehash(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1_048_576), b''):
            h.update(chunk)
    return h.hexdigest()


def js_ns(value):
    return int((Decimal(str(value))*NS).to_integral_value(rounding=ROUND_HALF_UP))


def quantile(values, p):
    if not values:
        return None
    a = sorted(values)
    i = (len(a)-1)*Decimal(p)
    lo = int(i)
    return a[lo]+(a[min(lo+1, len(a)-1)]-a[lo])*(i-lo)


def stats(values):
    return dict(n=len(values), median=quantile(values, '.5'), p90=quantile(values, '.9'))


def serial(value):
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, dict):
        return {str(k): serial(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [serial(v) for v in value]
    return value


def intervals_coverage(intervals, lo, hi):
    assert hi > lo
    pieces = sorted((max(lo, a), min(hi, b)) for a, b in intervals if b > lo and a < hi and b > a)
    merged = []
    for a, b in pieces:
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(b, merged[-1][1])
        else:
            merged.append([a, b])
    gap_start = lo
    gaps = []
    for a, b in merged:
        if a > gap_start:
            gaps.append([gap_start, a])
        gap_start = b
    if gap_start < hi:
        gaps.append([gap_start, hi])
    covered = sum(b-a for a, b in merged)
    return dict(window_start_wall_ns=str(lo), window_end_wall_ns=str(hi), duration_s=Decimal(hi-lo)/SEC,
        covered_s=Decimal(covered)/SEC, unavailable_s=Decimal(hi-lo-covered)/SEC,
        covered_percent=Decimal(covered)*100/(hi-lo),
        longest_unavailable_s=Decimal(max((b-a for a, b in gaps), default=0))/SEC,
        unavailable_intervals=[dict(start_wall_ns=str(a), end_wall_ns=str(b), seconds=Decimal(b-a)/SEC)
                               for a, b in gaps if b-a >= 100_000_000])


def read_browser(path):
    probes, bodies, first, anchors = [], {}, {}, {}
    start = None
    offsets_lo, offsets_hi = [], []
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            r = load(line)
            if r['kind'] == 'start':
                start = js_ns(r['browser_ms'])
            elif r['kind'] == 'ghost':
                e = load(r['data']); p = e['ghost']
                if p is None or p['run_id'] != RUN:
                    continue
                key = p['decision_id']
                bodies[key] = p
                first.setdefault(key, js_ns(r['browser_ms'])-start)
                a = p['current_twap']
                if a:
                    seen = anchors.setdefault(a['source_timestamp_ms'], dict(at=js_ns(r['browser_ms'])-start, values=set()))
                    seen['values'].add(Decimal(a['value']))
            elif r['kind'] == 'probe':
                r['elapsed_ns'] = js_ns(r['browser_ms'])-start
                probes.append(r)
                if r['calibrated']:
                    offsets_lo.append(int(r['offset_lower_ns'])); offsets_hi.append(int(r['offset_upper_ns']))
    assert start is not None and offsets_lo and max(offsets_lo) <= min(offsets_hi)
    eligible_first = {h: min(first[k] for k, p in bodies.items()
                            if h in p['publication_eligibility']['eligible_horizons']) for h in HS}
    return dict(start=start, probes=probes, bodies=bodies, first=first, anchors=anchors,
                offset_lo=max(offsets_lo), offset_hi=min(offsets_hi), eligible_first=eligible_first)


def probe_panel(browser, h, cutoff_ns):
    first_slot = (cutoff_ns+100*NS-1)//(100*NS)
    buckets = defaultdict(list)
    for p in browser['probes']:
        slot = p['elapsed_ns']//(100*NS)
        if first_slot <= slot < 9000:
            buckets[slot].append(p)
    counts = Counter()
    first_usable = None
    classifier_disagreements = 0
    initial_prefix = 0
    for slot in range(first_slot, 9000):
        group = buckets.get(slot)
        if not group:
            category = 'missing_probe'
        else:
            categories = []
            for p in group:
                body = browser['bodies'].get(p['selected_decision_id'])
                if not p['calibrated']:
                    why = 'uncalibrated'
                elif body is None:
                    why = 'no_current_payload'
                elif js_ns(p['browser_ms']) >= int(p['effective_deadline_browser_ns']):
                    why = 'expired_at_probe'
                elif h not in body['publication_eligibility']['eligible_horizons']:
                    why = 'horizon_not_eligible'
                else:
                    target = next(f['target_source_timestamp_ms'] for f in body['forecasts'] if f['horizon_s'] == h)
                    why = 'target_already_passed' if p['latest_anchor_source_ms'] is not None and p['latest_anchor_source_ms'] >= target else 'usable'
                classifier_disagreements += int((why == 'usable') != (h in p['usable_horizons']))
                categories.append(why)
            category = next((c for c in categories if c != 'usable'), 'usable')
        counts[category] += 1
        if category == 'usable' and first_usable is None:
            first_usable = slot
        elif first_usable is None:
            initial_prefix += 1
    planned = 9000-first_slot
    return dict(cutoff_elapsed_ms=Decimal(cutoff_ns)/NS, first_included_grid_ms=first_slot*100,
        planned_slots=planned, usable_slots=counts['usable'], usable_percent=Decimal(counts['usable'])*100/planned,
        classification=dict(counts), classifier_disagreements=classifier_disagreements,
        initial_prefix_unusable_slots=initial_prefix, initial_prefix_s=Decimal(initial_prefix)/10,
        first_usable_grid_ms=None if first_usable is None else first_usable*100,
        unavailable_after_initial_prefix_slots=planned-counts['usable']-initial_prefix)


def read_prestate(evidence):
    plan = load((evidence/'recovery_plan.json').read_bytes())
    planned = {r['decision_id']: r for r in plan['records']}
    prior, pending = {}, Counter()
    with tarfile.open(evidence/'outbox_before_recovery.tar.gz', 'r:gz') as t:
        for m in t.getmembers():
            if not m.name.endswith('.row'):
                continue
            assert m.size <= 131_072
            raw = t.extractfile(m).read()
            assert sha(raw) == plan['outbox_sha256'][m.name]
            r = load(raw); p = planned[r['decision_id']]
            assert r['run_id'] == RUN and r['version'] == p['version']
            assert sha(r['frozen_json'].encode()) == p['frozen_sha256']
            assert sha(r['state_json'].encode()) == p['state_sha256']
            state = load(r['state_json'])
            prior[r['decision_id']] = dict(record=r, state=state)
            pending.update(h for h, target in state['targets'].items() if target['status'] == 'pending')
    assert set(prior) == set(planned)
    return prior, pending


def read_audit(path, prior):
    digest = hashlib.sha256(); rows = []; total = 0; transitions = Counter(); existing = Counter()
    old_targets_unchanged = old_publication_unchanged = 0
    with path.open('rb') as f:
        while True:
            raw = f.readline(1_048_577)
            if not raw:
                break
            assert len(raw) <= 1_048_576 and raw.endswith(b'\n')
            digest.update(raw); total += 1
            if RUN.encode() not in raw:
                continue
            r = load(raw)
            if r['run_id'] != RUN:
                continue
            for k in ('frozen', 'state'):
                assert sha(r[k+'_json'].encode()) == r[k+'_sha256']
            frozen, state = load(r['frozen_json']), load(r['state_json'])
            if r['decision_id'] in prior:
                old = prior[r['decision_id']]
                assert r['frozen_json'] == old['record']['frozen_json']
                for h, before in old['state']['targets'].items():
                    after = state['targets'][h]
                    if before['status'] == 'pending':
                        assert after['status'] == 'restart_unmatched'
                        assert {k:v for k,v in after.items() if k != 'status'} == {k:v for k,v in before.items() if k != 'status'}
                        transitions[h] += 1
                    else:
                        assert before == after
                        old_targets_unchanged += 1
                if old['state']['publication']['status'] == 'acknowledged':
                    assert old['state']['publication'] == state['publication']
                    old_publication_unchanged += 1
            existing.update(h for h, t in state['targets'].items() if t['status'] == 'restart_unmatched')
            pub = state['publication']; payload = load(pub['payload_json']) if pub.get('payload_json') else None
            rows.append(dict(id=r['decision_id'], dw=int(frozen['decision_wall_ns']), dm=int(frozen['decision_monotonic_ns']),
                valid=int(frozen['valid_until_wall_ns']), included=frozen['included_sequence'], pub=pub, payload=payload,
                targets=state['targets'], current_twap=frozen['current_twap'], source=frozen['runtime_policy']['canary_start_ms']))
    manifest = load(Path(str(path)+'.manifest.json').read_bytes())
    assert digest.hexdigest() == manifest['sha256'] and total == manifest['row_count']
    return rows, dict(export_sha256=digest.hexdigest(), total_rows=total, run_rows=len(rows)), dict(
        exact_highest_prestate_rows=len(prior), newly_restart_unmatched_by_horizon=dict(transitions),
        newly_restart_unmatched=sum(transitions.values()), final_restart_unmatched_by_horizon=dict(existing),
        final_restart_unmatched=sum(existing.values()), already_classified_before_recovery=sum(existing.values())-sum(transitions.values()),
        preexisting_nonpending_targets_unchanged=old_targets_unchanged, preexisting_acknowledged_publications_unchanged=old_publication_unchanged)


def metrics(rows, browser):
    output = {}
    first_decision = min(row['dw'] for row in rows)
    for cohort in ('all_run', 'after_configured_start65', 'after_first_decision65', 'browser_admission_decisions'):
        grouped = []
        for h in (5, 10, 30):
            errors, leads, counts = [], [], Counter()
            for row in rows:
                if cohort == 'after_configured_start65' and row['dw'] < row['source']*NS+65*SEC:
                    continue
                if cohort == 'after_first_decision65' and row['dw'] < first_decision+65*SEC:
                    continue
                if cohort == 'browser_admission_decisions' and not 0 <= browser['first'].get(row['id'], 10**30) < 900*SEC:
                    continue
                counts['decisions'] += 1
                pub, p, target = row['pub'], row['payload'], row['targets'][str(h)]
                if pub['status'] != 'acknowledged' or not p:
                    counts['not_acknowledged'] += 1; continue
                if h not in p['publication_eligibility']['eligible_horizons']:
                    counts['not_in_attempted_membership'] += 1; continue
                aw, am, kw, km = (int(pub[k]) for k in ('attempt_wall_ns', 'attempt_monotonic_ns', 'ack_wall_ns', 'ack_monotonic_ns'))
                assert row['dw'] <= aw <= kw and row['dm'] <= am <= km
                if min(row['valid']-kw, row['valid']-row['dw']-(km-row['dm'])) <= 0:
                    counts['expired_at_ack'] += 1; continue
                counts['acknowledged_eligible_valid'] += 1
                if target['status'] != 'matched' or any(target.get(k) for k in ('conflicted', 'clock_anomaly', 'causality_invalid')):
                    counts['unmatched_or_flagged'] += 1; continue
                event = target['first_event']; forecast = next(f for f in p['forecasts'] if f['horizon_s'] == h)
                ew, em = int(event['received_wall_ns']), int(event['received_monotonic_ns'])
                assert event['feed'] == 'twap' and event['window_s'] == 60
                assert event['source_timestamp_ms'] == forecast['target_source_timestamp_ms'] == target['target_source_timestamp_ms']
                assert event['source_timestamp_ms']*NS <= ew and event['sequence'] > row['included']
                assert row['dw'] <= ew and row['dm'] <= em < row['dm']+120*SEC
                if not kw < ew or not km < em:
                    counts['not_strictly_early'] += 1; continue
                assert forecast['quality'] in ('healthy', 'degraded') and not forecast['reasons']
                error = Decimal(forecast['price'])-Decimal(event['value'])
                assert error == Decimal(target['error'])
                assert int(target['confirmed_redis_lead_ns']) == em-km
                errors.append(abs(error)); leads.append(Decimal(em-km)/SEC)
            grouped.append(dict(horizon_s=h, counts=dict(counts), absolute_error_usd=stats(errors), confirmed_redis_lead_s=stats(leads)))
        output[cohort] = grouped
    return output


def publication_intervals(rows):
    pubs = []
    for row in rows:
        pub, payload = row['pub'], row['payload']
        if pub['status'] != 'acknowledged' or not payload:
            continue
        aw, am, kw, km = (int(pub[k]) for k in ('attempt_wall_ns', 'attempt_monotonic_ns', 'ack_wall_ns', 'ack_monotonic_ns'))
        ttl_ms = min((row['valid']-aw)//NS, (row['valid']-row['dw']-(am-row['dm']))//NS)
        end = min(aw+ttl_ms*NS, row['valid'], kw+row['valid']-row['dw']-(km-row['dm']))
        pubs.append(dict(row=row, attempt=aw, ack=kw, end=end, eligible=set(payload['publication_eligibility']['eligible_horizons'])))
    pubs.sort(key=lambda p: p['ack'])
    intervals = {'key': [], **{str(h): [] for h in (5, 10, 30)}}
    for i, p in enumerate(pubs):
        later = pubs[i+1] if i+1 < len(pubs) else None
        key_end = min(p['end'], later['ack']) if later else p['end']
        if key_end > p['ack']:
            intervals['key'].append((p['ack'], key_end))
        for h in (5, 10, 30):
            if h not in p['eligible']:
                continue
            end = p['end']
            target = p['row']['targets'][str(h)]
            if target.get('first_event'):
                end = min(end, int(target['first_event']['received_wall_ns']))
            if later:
                # If both old/new forecasts are eligible and live through ACK,
                # either version is usable throughout the unknown SET instant.
                new_live = later['end'] > later['ack'] and h in later['eligible']
                target_next = later['row']['targets'][str(h)].get('first_event')
                new_live &= target_next is None or int(target_next['received_wall_ns']) > later['ack']
                replacement = later['ack'] if new_live and end >= later['ack'] else later['attempt']
                end = min(end, replacement)
            if end > p['ack']:
                intervals[str(h)].append((p['ack'], end))
    return intervals


def run(audit, evidence):
    browser_path = evidence/'browser.jsonl.gz'
    browser = read_browser(browser_path)
    prior, pending = read_prestate(evidence)
    rows, provenance, recovery = read_audit(audit, prior)
    assert dict(pending) == recovery['newly_restart_unmatched_by_horizon']
    start = rows[0]['source']*NS
    assert all(r['source']*NS == start for r in rows)
    first_decision = min(r['dw'] for r in rows); last_decision = max(r['dw'] for r in rows)
    bounds = (browser['offset_lo'], browser['offset_hi'])
    bp = browser['start']; intervals = publication_intervals(rows)
    audit_cov = {}
    windows = {
        'first_decision_plus65_to_last_decision': [(first_decision+65*SEC, last_decision)],
        'configured_start_plus65_to_configured_stop': [(start+65*SEC, start+1020*SEC)],
        'same_browser_post65_window_offset_endpoints': [(bp+65*SEC+o, bp+900*SEC+o) for o in bounds],
        'configured_start_plus65_to_browser_admission_end_offset_endpoints': [(start+65*SEC, bp+900*SEC+o) for o in bounds],
    }
    for name, pairs in windows.items():
        audit_cov[name] = [{k: intervals_coverage(v, a, b) for k, v in intervals.items()} for a, b in pairs]
    producer_cutoffs = [(start+65*SEC-o)-bp for o in reversed(bounds)]
    first_d_cutoffs = [(first_decision+65*SEC-o)-bp for o in reversed(bounds)]
    probe_cov = []
    for h in (5, 10, 30):
        primary = probe_panel(browser, h, 65*SEC)
        first_eligible = browser['eligible_first'][h]
        probe_cov.append(dict(horizon_s=h, first_eligible_sse_elapsed_ms=Decimal(first_eligible)/NS,
            primary_after_browser65=primary,
            before_first_eligible_inside_primary_s=Decimal(max(0, first_eligible-65*SEC))/SEC,
            after_configured_start65_offset_endpoint_panels=[probe_panel(browser, h, c) for c in producer_cutoffs],
            after_first_audit_decision65_offset_endpoint_panels=[probe_panel(browser, h, c) for c in first_d_cutoffs],
            after_first_eligible_sse=probe_panel(browser, h, first_eligible)))
    return dict(status='passed', run_id=RUN, audit=provenance, recovery=recovery, audit_metrics=metrics(rows, browser),
        audit_interval_coverage=audit_cov, browser_probe_coverage=probe_cov,
        clocks=dict(common_offset_lower_ns=str(bounds[0]), common_offset_upper_ns=str(bounds[1]),
            common_offset_width_ms=Decimal(bounds[1]-bounds[0])/NS,
            configured_start_browser_elapsed_ms_bounds=[Decimal(start-o-bp)/NS for o in reversed(bounds)],
            first_audit_decision_browser_elapsed_ms_bounds=[Decimal(first_decision-o-bp)/NS for o in reversed(bounds)],
            configured_start_plus65_browser_elapsed_ms_bounds=[Decimal(c)/NS for c in producer_cutoffs]),
        methods=[
            'Audit errors/lead require recorded ACK, exact attempted horizon membership, valid source/receipt expiry at ACK, matched unflagged exact target within120s, and strict ACK before first target in both producer clocks.',
            'Coverage uses recorded acknowledged publications and earliest TTL expiry derived from the actual runtime PX formula, bounded by frozen wall/producer-monotonic validity. New publications replace old ones; an unavailable horizon cannot inherit an older price.',
            'Intervals start at ACK; the SET instant inside attempt→ACK is not measured. Transition coverage is allowed only when both eligible versions are live through ACK; other transitions end at the next attempt. Exact known target receipts end horizon usability.',
            'Browser/host alignment uses the final intersection of recorded timed-GET offset brackets retrospectively, assuming no clock discontinuity. Endpoint scenarios are sensitivities, not a newly measured exact clock offset.',
            'Browser probe classification is checked against saved selected-payload membership, recorded effective deadline, calibration and latest observed target stamp. Missing probe slots remain unavailable in all denominators.',
            'The original browser+65s panel remains primary. Later start definitions and first-eligible cutoffs are labeled descriptive diagnostics, not replacements.',
            'Coverage is not a direct continuous Redis observation. Missing exact target evidence and unobserved intermediate inputs limit reconstruction. Audit ACK/TTL coverage cannot replace actual browser usability.'
        ], sources={str(p):filehash(p) for p in (browser_path, evidence/'recovery_plan.json', evidence/'outbox_before_recovery.tar.gz', evidence/'start.json')},
        code_sha256=filehash(Path(__file__)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    a = parser.parse_args()
    assert not a.output.exists(), 'refusing overwrite'
    with localcontext() as c:
        c.prec = 80
        assert intervals_coverage([(0, 10), (8, 20)], 0, 30)['covered_s'] == Decimal(20)/SEC
        result = serial(run(a.audit, a.evidence))
    a.output.parent.mkdir(parents=True, exist_ok=True)
    with a.output.open('x', encoding='utf-8') as f:
        json.dump(result, f, indent=2, sort_keys=True); f.write('\n')
    print(json.dumps(dict(status=result['status'], recovery_new=result['recovery']['newly_restart_unmatched'], audit_metrics=result['audit_metrics']['all_run'])))


if __name__ == '__main__':
    main()
