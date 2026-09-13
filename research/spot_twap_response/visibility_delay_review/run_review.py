"""Run the bounded read-only section-14 review once, preserving supplied artifacts."""
from __future__ import annotations
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    root = Path(__file__).resolve().parent
    source = root.parent/'pilot_part4.sql'
    supplied = root.parents[2]/'results/spot_twap_response/2026-09-13-pilot/pilot_part4_output.txt'
    names = ('section14_original.sql','supplied_section14.txt','stdout.jsonl','stderr.txt','negative_pairs.csv','manifest.json')
    if any((root/name).exists() for name in names):
        raise SystemExit('Refusing to overwrite visibility review outputs')
    code = source.read_text(encoding='utf-8')
    section = code[code.index(r'\echo === 14.'):code.index(r'\echo === 15.')]
    report = supplied.read_text(encoding='utf-8')
    report = report[report.index('=== 14.'):report.index('=== 15.')]
    expected = next(line.strip() for line in report.splitlines() if line[:1].isdigit())
    (root/'section14_original.sql').write_text(section,encoding='utf-8',newline='\n')
    (root/'supplied_section14.txt').write_text(report,encoding='utf-8',newline='\n')
    sql = (root/'query.sql').read_text(encoding='utf-8')
    remote = "set -eu\nsudo -u postgres psql -X -A -t -q -v ON_ERROR_STOP=1 -d price_collector <<'H3_VISIBILITY_SQL'\n"+sql+"\nH3_VISIBILITY_SQL\n"
    started_utc = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    run = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@152.42.247.86','bash -s'],
                         input=remote.encode('utf-8'),capture_output=True,timeout=45)
    elapsed = time.monotonic()-started
    (root/'stdout.jsonl').write_bytes(run.stdout)
    (root/'stderr.txt').write_bytes(run.stderr)
    errors, result = [], None
    if run.returncode:
        errors.append(f'SSH/psql exit {run.returncode}')
    else:
        result = json.loads(run.stdout)
        if (result['read_only'],result['isolation'],result['statement_timeout']) != ('on','repeatable read','20s'):
            errors.append('Unexpected transaction safety metadata')
        if result['original_result_line'] != expected:
            errors.append('Original section14 result differs from supplied output')
        if len(result['negative_pairs']) != result['stats']['negative_pairs']:
            errors.append('Detailed negative count differs from summary')
        if result['stats']['n_pairs']+result['coverage']['spot_without_exact_plus3_twap'] != result['coverage']['retained_spot_seconds']:
            errors.append('Coverage counts do not reconcile')
        with (root/'negative_pairs.csv').open('x',encoding='utf-8',newline='') as handle:
            writer=csv.DictWriter(handle,fieldnames=list(result['negative_pairs'][0]))
            writer.writeheader(); writer.writerows(result['negative_pairs'])
    manifest={'status':'accepted' if not errors else 'failed','started_utc':started_utc,
              'elapsed_seconds':round(elapsed,3),'ssh_psql_exit_code':run.returncode,'validation_errors':errors,
              'read_only':True,'isolation':'repeatable read','statement_timeout':'20s',
              'original_result_line_expected':expected,'source_sha256':digest(source),
              'supplied_output_sha256':digest(supplied),'code_sha256':digest(Path(__file__)),
              'query_sha256':digest(root/'query.sql'),
              'scope':'Only inclusive Sept1--Sept8 source range, +10s TWAP margin; no sections15--17',
              'rounding':'Original earliest ns receipt divided by1000000 using integer division; original percentile_cont on millisecond offsets preserved',
              'bounded_index_predicate':'Redundant market_id bounds follow schema source-second/market constraints; original window_s-only identity scope preserved',
              'files_sha256':{name:digest(root/name) for name in names if name!='manifest.json' and (root/name).exists()},
              'result':result}
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'status':manifest['status'],'elapsed_seconds':manifest['elapsed_seconds'],
                      'errors':errors,'stats':result['stats'] if result else None,
                      'coverage':result['coverage'] if result else None},indent=2))
    if errors: raise SystemExit(1)


if __name__=='__main__': main()
