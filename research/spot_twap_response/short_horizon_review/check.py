"""Offline short-source-horizon diagnostic from already exported anchor clocks."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    root = Path(__file__).resolve().parent
    parent = root.parent
    source = parent/'receipt_ghost_review/anchor_targets.csv'
    audit_path = parent/'receipt_ghost_review/audit_manifest.json'
    boundary_path = parent/'receipt_ghost_review/boundary_side_summary.json'
    part6b_path = root.parents[2]/'results/spot_twap_response/2026-09-13-pilot/pilot_part6b_output.txt'
    output = root/'summary.json'
    if output.exists():
        raise SystemExit('Refusing to overwrite the short-horizon review')
    source_hash = digest(source)
    audit = json.loads(audit_path.read_text(encoding='utf-8'))
    if audit['status'] != 'accepted' or audit['files_sha256']['anchor_targets.csv'] != source_hash:
        raise ValueError('Source export does not match its accepted audit')
    anchors, horizons_seen, row_count = {}, {}, 0
    with source.open(encoding='utf-8',newline='') as handle:
        for row in csv.DictReader(handle):
            tau = int(row['tau'])
            pair = (int(row['w']) if row['w'] else None,
                    int(row['spot_last_source_ms']) if row['spot_last_source_ms'] else None)
            if tau in anchors and anchors[tau] != pair:
                raise ValueError('Clock inputs differ across original horizon rows')
            anchors[tau] = pair
            h = int(row['h'])
            if h in horizons_seen.setdefault(tau,set()):
                raise ValueError('Duplicate original anchor/horizon')
            horizons_seen[tau].add(h)
            row_count += 1
    if row_count != 30243 or len(anchors) != 10081 or any(v != {5,10,30} for v in horizons_seen.values()):
        raise ValueError('Expected complete original three-horizon anchor grid')
    eligible = [(tau,w,s) for tau,(w,s) in sorted(anchors.items()) if w is not None and s is not None]
    results = []
    examples = []
    for h in (1,2,3):
        values = []
        for tau,w,s in eligible:
            numerator = w+h*1000-3000-s
            if numerator % 1000:
                raise ValueError('Tail formula requires exact source-second clocks')
            tail = max(0,numerator//1000)
            target = w+h*1000
            values.append((tau,target,tail))
            if tail:
                examples.append({'tau_ms':tau,'baseline_source_ms':w,'latest_retained_spot_source_ms':s,
                                 'h_s':h,'target_source_ms':target,'tail_slots':tail,
                                 'target_source_at_or_before_tau':target<=tau})
        results.append({'h_s':h,'eligible_anchor_n':len(values),
                        'tail_positive_n':sum(tail>0 for tau,target,tail in values),
                        'tail_slots_total':sum(tail for tau,target,tail in values),
                        'maximum_tail_slots':max((tail for tau,target,tail in values),default=None),
                        'tail_exceeds_60_slots_n':sum(tail>60 for tau,target,tail in values),
                        'target_source_at_or_before_tau_n':sum(target<=tau for tau,target,tail in values),
                        'target_source_strictly_before_tau_n':sum(target<tau for tau,target,tail in values),
                        'target_source_exactly_tau_n':sum(target==tau for tau,target,tail in values),
                        'tail_positive_and_target_source_at_or_before_tau_n':sum(tail>0 and target<=tau for tau,target,tail in values)})
    boundary = json.loads(boundary_path.read_text(encoding='utf-8'))
    if boundary['status'] != 'accepted' or boundary['source_sha256'] != source_hash:
        raise ValueError('Boundary summary uses a different source')
    known = {(r['h_s'],r['baseline_target_cross_market']):(r['n'],r['side_changes']) for r in boundary['rows']}
    supplied = []
    for line in part6b_path.read_text(encoding='utf-8').splitlines():
        if re.match(r'^(5|10|30)\|[tf]\|',line):
            cells = line.split('|')
            h,cross,n,changes = int(cells[0]),cells[1]=='t',int(cells[2]),int(cells[3])
            if known.get((h,cross)) != (n,changes):
                raise ValueError('Part6b denominator/actual-side-change count differs')
            supplied.append({'h_s':h,'baseline_target_cross_market':cross,'n':n,'side_changes':changes})
    if len(supplied) != 6:
        raise ValueError('Expected six supplied boundary groups')
    result = {
        'status':'accepted','created_utc':datetime.now(timezone.utc).isoformat(),
        'offline_only':True,'database_queries':0,'source_rows':row_count,'unique_tau':len(anchors),
        'anchors_missing_w_or_spot_last':len(anchors)-len(eligible),
        'source_path':str(source),'source_sha256':source_hash,'code_sha256':digest(Path(__file__)),
        'source_audit_sha256':digest(audit_path),'source_snapshot_ms':audit['snapshot_ms'],
        'tail_formula':'max(0,(w+h*1000-3000-spot_last_source_ms)/1000), exact integer-second clocks',
        'tail_scope':'Lower-bound diagnostic for this sample (no value exceeds60); omits internal carried slots and missing earlier history',
        'source_time_comparison':'U=w+h*1000; U<=tau does not imply that the target report was received by tau',
        'results':results,'tail_positive_examples':examples,
        'part6b_reconciliation':{'status':'matched','verified_fields':['n','side_changes'],
                                'all_six_groups':supplied,'boundary_summary_sha256':digest(boundary_path),
                                'supplied_output_sha256':digest(part6b_path),
                                'not_recomputed':['ghost predictions','ghost errors','ghost new-side detections','false alarms']},
        'limitations':[
            'The original exported target prices/receipts cover only h5,10,30; no h1,2,3 accuracy or arrival result is measured here',
            'spot_last_source_ms is the greatest retained eligible source in the original combined slot grid; this is not a new per-slot availability replay',
            'Zero tail slots do not establish zero assumptions: internal absent source stamps may be carried forward',
            'The latest retained spot may not reconstruct an earlier overwritten same-second value',
        ]}
    output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'status':'accepted','unique_tau':len(anchors),'results':results,
                      'part6b_n_and_side_changes_match_all_six_groups':True},indent=2))


if __name__=='__main__':
    main()
