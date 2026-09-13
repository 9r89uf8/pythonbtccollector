"""Bounded exact section-18 reproduction; preserves a failed first attempt."""
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


def data_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if re.match(r'^(5|10|30)\|',line.strip())]


def main() -> None:
    root=Path(__file__).resolve().parent
    source=root.parent/'pilot_part5.sql'
    supplied=root.parents[2]/'results/spot_twap_response/2026-09-13-pilot/pilot_part5_output.txt'
    names=('section18_original.sql','supplied_output.txt','attempt1_query.sql','attempt1_stdout.txt',
           'attempt1_stderr.txt','attempt1_manifest.json','summary.csv')
    if any((root/name).exists() for name in names):
        raise SystemExit('Refusing to overwrite prior ghost review artifacts')
    original=source.read_text(encoding='utf-8')
    section=original[original.index(r'\echo === 18.'):]
    supplied_text=supplied.read_text(encoding='utf-8')
    expected=data_lines(supplied_text)
    if len(expected)!=3 or section.count('COMMIT;')!=1:
        raise SystemExit('Unexpected original section18 structure')
    (root/'section18_original.sql').write_text(section,encoding='utf-8',newline='\n')
    (root/'supplied_output.txt').write_text(supplied_text,encoding='utf-8',newline='\n')
    preamble=r"""\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='2s';
SET LOCAL TIME ZONE 'UTC';
SELECT 'H3_AUDIT_META',
  (extract(epoch FROM transaction_timestamp())*1000)::bigint,
  current_setting('transaction_read_only'),current_setting('transaction_isolation'),
  current_setting('statement_timeout');
WITH anchors AS (
  SELECT generate_series(1788220800000,1788825600000,60000) w
), horizons AS (SELECT unnest(ARRAY[5,10,30]) h), counts AS (
  SELECT h, count(*) candidate_anchors,
    count(*) FILTER (WHERE tn.price IS NULL) missing_exact_twap_now,
    count(*) FILTER (WHERE tf.price IS NULL) missing_exact_twap_future,
    count(*) FILTER (WHERE r.market_id IS NULL) missing_target_resolution_record,
    count(*) FILTER (WHERE r.market_id IS NOT NULL AND r.resolution_status<>'resolved') target_resolution_not_resolved,
    count(*) FILTER (WHERE r.market_id IS NOT NULL AND r.chainlink_open_price IS NULL) missing_target_opening_price,
    count(*) FILTER (WHERE tn.price IS NOT NULL AND tf.price IS NOT NULL AND r.resolution_status='resolved') exact_targets_with_resolved_market,
    count(*) FILTER (WHERE w=1788825600000) end_boundary_anchor
  FROM anchors CROSS JOIN horizons
  LEFT JOIN price_samples tn ON tn.instrument_id=4 AND tn.sample_second_ms=w
  LEFT JOIN price_samples tf ON tf.instrument_id=4 AND tf.sample_second_ms=w+h*1000
  LEFT JOIN polymarket_btc_5m_resolutions r ON r.market_id=(w+h*1000)/300000
  GROUP BY h
)
SELECT 'H3_COVERAGE_META',jsonb_agg(to_jsonb(counts) ORDER BY h) FROM counts;
"""
    query=preamble+'\n'+section
    (root/'attempt1_query.sql').write_text(query,encoding='utf-8',newline='\n')
    remote="set -eu\nsudo -u postgres psql -X -A -t -q -v ON_ERROR_STOP=1 -d price_collector <<'H3_GHOST_SQL'\n"+query+"\nH3_GHOST_SQL\n"
    started_utc=datetime.now(timezone.utc).isoformat()
    started=time.monotonic()
    run=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@152.42.247.86','bash -s'],
                       input=remote.encode('utf-8'),capture_output=True,timeout=55)
    elapsed=time.monotonic()-started
    (root/'attempt1_stdout.txt').write_bytes(run.stdout)
    (root/'attempt1_stderr.txt').write_bytes(run.stderr)
    text=run.stdout.decode('utf-8')
    actual=data_lines(text)
    metadata=[line.split('|') for line in text.splitlines() if line.startswith('H3_AUDIT_META|')]
    coverage_lines=[line.split('|',1)[1] for line in text.splitlines() if line.startswith('H3_COVERAGE_META|')]
    coverage=json.loads(coverage_lines[0]) if len(coverage_lines)==1 else None
    errors=[]
    if run.returncode: errors.append(f'SSH/psql exit {run.returncode}')
    if actual!=expected: errors.append('Exact section18 rows do not match supplied output')
    if len(metadata)!=1 or metadata[0][2:]!=['on','repeatable read','20s']:
        errors.append('Unexpected transaction safety metadata')
    if not coverage or any(row['candidate_anchors']!=10081 for row in coverage):
        errors.append('Expected10081candidate anchors for each horizon')
    if actual:
        header=next(line.strip() for line in supplied_text.splitlines() if line.startswith('h_s|'))
        with (root/'summary.csv').open('x',encoding='utf-8',newline='') as handle:
            writer=csv.writer(handle);writer.writerow(header.split('|'));writer.writerows(line.split('|') for line in actual)
    manifest={'status':'accepted' if not errors else 'failed','started_utc':started_utc,
              'elapsed_seconds':round(elapsed,3),'ssh_psql_exit_code':run.returncode,'validation_errors':errors,
              'snapshot_ms':metadata[0][1] if metadata else None,'read_only':True,
              'isolation':'repeatable read','statement_timeout':'20s','original_rows_match':actual==expected,
              'original_result_lines':actual,'coverage':coverage,
              'source_sha256':digest(source),'supplied_output_sha256':digest(supplied),
              'code_sha256':digest(Path(__file__)),
              'files_sha256':{name:digest(root/name) for name in names if name!='attempt1_manifest.json' and (root/name).exists()},
              'scope':'Only original fixed-week section18 plus point-lookup target coverage; no other sections',
              'numeric_note':'Original NUMERIC financial arithmetic and percentile_cont approximation on finalized dimensionless bps preserved; original3decimalrounding',
              'limitations':['Source-clock retained-history replay, not causal receipt-clock forecast',
                             'Inclusive final anchor adds the September8 midnight anchor and forecasts beyond the nominal week',
                             'Side correctness is relative to future TWAP and reconciled target-market strike, not official final winner']}
    (root/'attempt1_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'status':manifest['status'],'elapsed_seconds':manifest['elapsed_seconds'],
                      'errors':errors,'result_lines':actual,'coverage':coverage},indent=2))
    if errors: raise SystemExit(1)


if __name__=='__main__': main()
