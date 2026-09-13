"""Reproduce only original section 19 under a bounded read-only transaction."""
from __future__ import annotations
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if re.match(r'^(5|10|30)\|',line.strip())]


def main() -> None:
    root=Path(__file__).resolve().parent
    source=root.parent/'pilot_part6.sql'
    supplied=root.parents[2]/'results/spot_twap_response/2026-09-13-pilot/pilot_part6_output.txt'
    names=('section19_original.sql','supplied_output.txt','attempt1_query.sql','attempt1_stdout.txt',
           'attempt1_stderr.txt','attempt1_manifest.json','summary.csv')
    if any((root/name).exists() for name in names): raise SystemExit('Refusing to overwrite prior receipt-ghost artifacts')
    original=source.read_text(encoding='utf-8')
    section=original[original.index(r'\echo === 19.'):]
    supplied_text=supplied.read_text(encoding='utf-8')
    expected=lines(supplied_text)
    if len(expected)!=3 or section.count('COMMIT;')!=1: raise SystemExit('Unexpected supplied section19 structure')
    (root/'section19_original.sql').write_text(section,encoding='utf-8',newline='\n')
    (root/'supplied_output.txt').write_text(supplied_text,encoding='utf-8',newline='\n')
    preamble=r"""\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='2s';
SET LOCAL TIME ZONE 'UTC';
SELECT 'H3_AUDIT_META',(extract(epoch FROM transaction_timestamp())*1000)::bigint,
  current_setting('transaction_read_only'),current_setting('transaction_isolation'),current_setting('statement_timeout');
"""
    query=preamble+'\n'+section
    (root/'attempt1_query.sql').write_text(query,encoding='utf-8',newline='\n')
    remote="set -eu\nsudo -u postgres psql -X -A -t -q -v ON_ERROR_STOP=1 -d price_collector <<'H3_RECEIPT_GHOST_SQL'\n"+query+"\nH3_RECEIPT_GHOST_SQL\n"
    started_utc=datetime.now(timezone.utc).isoformat(); started=time.monotonic()
    run=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@152.42.247.86','bash -s'],
                       input=remote.encode('utf-8'),capture_output=True,timeout=45)
    elapsed=time.monotonic()-started
    (root/'attempt1_stdout.txt').write_bytes(run.stdout); (root/'attempt1_stderr.txt').write_bytes(run.stderr)
    text=run.stdout.decode('utf-8'); actual=lines(text)
    metadata=[line.split('|') for line in text.splitlines() if line.startswith('H3_AUDIT_META|')]
    errors=[]
    if run.returncode: errors.append(f'SSH/psql exit {run.returncode}')
    if actual!=expected: errors.append('Exact original rows differ from supplied output')
    if len(metadata)!=1 or metadata[0][2:]!=['on','repeatable read','20s']: errors.append('Invalid transaction metadata')
    if actual:
        header=next(line for line in supplied_text.splitlines() if line.startswith('h_s|'))
        with (root/'summary.csv').open('x',encoding='utf-8',newline='') as handle:
            writer=csv.writer(handle); writer.writerow(header.split('|')); writer.writerows(line.split('|') for line in actual)
    manifest={'status':'accepted' if not errors else 'failed','started_utc':started_utc,
              'elapsed_seconds':round(elapsed,3),'ssh_psql_exit_code':run.returncode,'validation_errors':errors,
              'snapshot_ms':metadata[0][1] if metadata else None,'read_only':True,'isolation':'repeatable read',
              'statement_timeout':'20s','original_rows_match':actual==expected,'original_result_lines':actual,
              'source_sha256':digest(source),'supplied_output_sha256':digest(supplied),'code_sha256':digest(Path(__file__)),
              'files_sha256':{name:digest(root/name) for name in names if name!='attempt1_manifest.json' and (root/name).exists()},
              'scope':'Only fixed-week section19 unchanged; no other sections or production changes',
              'numeric_note':'Original NUMERIC arithmetic and percentile_cont approximation on finalized dimensionless bps/arrivalms preserved'}
    (root/'attempt1_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'status':manifest['status'],'elapsed_seconds':manifest['elapsed_seconds'],'errors':errors,'rows':actual},indent=2))
    if errors: raise SystemExit(1)


if __name__=='__main__': main()
