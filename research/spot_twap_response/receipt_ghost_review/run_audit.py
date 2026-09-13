"""Read-only baseline/target diagnostics, without recomputing ghost windows."""
from __future__ import annotations
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import subprocess
import time


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    root=Path(__file__).resolve().parent
    for name in ('audit_stdout.txt','audit_stderr.txt','anchor_targets.csv','audit_manifest.json'):
        if (root/name).exists(): raise SystemExit('Refusing to overwrite audit outputs')
    query=(root/'audit.sql').read_text(encoding='utf-8')
    remote="set -eu\nsudo -u postgres psql -X -A -t -q -v ON_ERROR_STOP=1 -d price_collector <<'H3_GHOST_AUDIT_SQL'\n"+query+"\nH3_GHOST_AUDIT_SQL\n"
    started_utc=datetime.now(timezone.utc).isoformat(); started=time.monotonic()
    run=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@152.42.247.86','bash -s'],
                       input=remote.encode('utf-8'),capture_output=True,timeout=45)
    elapsed=time.monotonic()-started
    (root/'audit_stdout.txt').write_bytes(run.stdout); (root/'audit_stderr.txt').write_bytes(run.stderr)
    text=run.stdout.decode('utf-8'); lines=text.splitlines(); errors=[]; rows=[]; summaries=[]
    metadata=next((line.split('|') for line in lines if line.startswith('H3_AUDIT_META|')),None)
    if run.returncode: errors.append(f'SSH/psql exit {run.returncode}')
    if metadata is None or metadata[2:]!=['on','repeatable read','20s']: errors.append('Invalid transaction metadata')
    if not errors:
        csv_text=text[text.index('tau,h,w,'):]
        (root/'anchor_targets.csv').write_text(csv_text,encoding='utf-8',newline='\n')
        rows=list(csv.DictReader(io.StringIO(csv_text)))
        if len(rows)!=30243: errors.append('Expected10081anchors times3horizons')
        for h in ('5','10','30'):
            candidates=[r for r in rows if r['h']==h]
            eligible=[r for r in candidates if r['w'] and r['target_min_price'] and r['resolution_status']=='resolved']
            n=len(eligible)
            d={'h':int(h),'candidate_anchors':len(candidates),'no_baseline':sum(not r['w'] for r in candidates),
               'baseline_but_no_target':sum(bool(r['w']) and not r['target_min_price'] for r in candidates),
               'no_resolved_target_market':sum(bool(r['w']) and r['resolution_status']!='resolved' for r in candidates),
               'baseline_target_eligible_n':n,'missing_target_k':sum(not r['target_k'] for r in eligible),
               'duplicate_target_stamps':sum(int(r['target_events'])>1 for r in eligible),
               'conflicting_target_prices':sum(Decimal(r['target_min_price'])!=Decimal(r['target_max_price']) for r in eligible),
               'min_price_differs_from_earliest_price':sum(Decimal(r['target_min_price'])!=Decimal(r['target_earliest_price']) for r in eligible),
               'unexpected_target_identity_rows':sum(int(r['unexpected_identity_events'])>0 for r in eligible),
               'baseline_receipt_tie_rows':sum(int(r['baseline_receipt_ties'])>1 for r in eligible),
               'target_arrived_before_tau':sum(int(r['target_arrival_ns'])<0 for r in eligible),
               'target_arrived_exactly_tau':sum(int(r['target_arrival_ns'])==0 for r in eligible),
               'target_arrival_truncated_ms_zero':sum(int(r['original_target_arrival_ms'])==0 for r in eligible),
               'minimum_target_arrival_ns':min((int(r['target_arrival_ns']) for r in eligible),default=None),
               'baseline_target_cross_market':sum(r['baseline_target_cross_market']=='t' for r in eligible),
               'target_exact_market_boundary':sum(r['target_exact_market_boundary']=='t' for r in eligible),
               'decision_target_different_market':sum(r['decision_target_different_market']=='t' for r in eligible),
               'latest_spot_after_baseline_receipt':sum(bool(r['spot_last_received_ms']) and int(r['spot_last_received_ms'])*1000000>int(r['baseline_received_ns']) for r in eligible),
               'spot_last_source_after_tau':sum(bool(r['spot_last_source_ms']) and int(r['spot_last_source_ms'])>int(r['tau']) for r in eligible)}
            summaries.append(d)
    original=json.loads((root/'attempt1_manifest.json').read_text(encoding='utf-8'))
    expected={int(line.split('|')[0]):int(line.split('|')[1]) for line in original['original_result_lines']}
    if summaries and any(d['baseline_target_eligible_n']!=expected[d['h']] for d in summaries):
        errors.append('Baseline/target eligible counts differ from original full-window cohorts')
    manifest={'status':'accepted' if not errors else 'failed','started_utc':started_utc,'elapsed_seconds':round(elapsed,3),
              'ssh_psql_exit_code':run.returncode,'validation_errors':errors,'snapshot_ms':metadata[1] if metadata else None,
              'read_only':True,'isolation':'repeatable read','statement_timeout':'20s','rows':len(rows),
              'summary':summaries,'query_sha256':digest(root/'audit.sql'),'code_sha256':digest(Path(__file__)),
              'files_sha256':{name:digest(root/name) for name in ('audit_stdout.txt','audit_stderr.txt','anchor_targets.csv') if (root/name).exists()},
              'scope':'Original baseline/target selection only; no per-slot forecast rewrite or actual historical-carry recount',
              'snapshot_note':'Separate read-only snapshot from exact original reproduction; matching horizon cohorts checked'}
    (root/'audit_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'status':manifest['status'],'elapsed_seconds':manifest['elapsed_seconds'],'errors':errors,'summary':summaries},indent=2))
    if errors: raise SystemExit(1)


if __name__=='__main__': main()
