"""Reproduce only supplied per-market section 13 with a 20-second query limit."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time


HEADER = "tsec|n|projff_err_p50_bps|projff_err_p90_bps|twap_err_p50_bps|projff_wrong|twap_wrong|spot_wrong|both_ok|projff_only_ok|twap_only_ok|both_wrong|rows_using_carried_value"
MARKER = "=== 13. Projection pilot"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table(text: str) -> list[str]:
    section = text[text.index(MARKER):]
    return [line.strip() for line in section.splitlines()
            if re.match(r"^(60|30|15|10|5|3)\|", line.strip())]


def main() -> int:
    directory = Path(__file__).resolve().parent
    repo = directory.parents[2]
    source = directory.parent / "pilot_part3b.sql"
    supplied = repo / "results/spot_twap_response/2026-09-13-pilot/pilot_part3b_output.txt"
    names = ("section13_original.sql", "query.sql", "stdout.txt", "stderr.txt",
             "results.csv", "manifest.json", "supplied_output.txt")
    if any((directory / name).exists() for name in names):
        raise SystemExit("Refusing to overwrite existing carry-forward audit artifacts")
    source_hash, supplied_hash = sha256(source), sha256(supplied)
    original = source.read_text(encoding="utf-8")
    source_marker = r"\echo " + MARKER
    if original.count(source_marker) != 1:
        raise SystemExit("Could not uniquely identify section 13")
    section = original[original.index(source_marker):]
    if section.count("COMMIT;") != 1 or not section.rstrip().endswith("COMMIT;"):
        raise SystemExit("Unexpected section-13 transaction boundary")
    supplied_text = supplied.read_text(encoding="utf-8")
    expected = table(supplied_text)
    if len(expected) != 6:
        raise SystemExit("Expected six supplied section-13 rows")
    (directory / "section13_original.sql").write_text(section, encoding="utf-8", newline="\n")
    (directory / "supplied_output.txt").write_text(supplied_text, encoding="utf-8", newline="\n")
    safety_source = directory.parent / "projection_pilot_review/query.sql"
    safety = safety_source.read_text(encoding="utf-8").split("\nCOPY (", 1)[0]
    safety = safety[safety.index(r"\set ON_ERROR_STOP on"):]
    safety = safety.replace("SET LOCAL statement_timeout = '30s';", "SET LOCAL statement_timeout = '20s';")
    if "SET LOCAL statement_timeout = '20s';" not in safety or "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;" not in safety:
        raise SystemExit("Unexpected bounded read-only preamble")
    metadata = """
SELECT 'H3_AUDIT_META',
  (extract(epoch FROM transaction_timestamp())*1000)::bigint,
  current_setting('transaction_read_only'),
  current_setting('transaction_isolation'),
  current_setting('statement_timeout');
SELECT 'H3_COHORT_META', count(*),
  count(*) FILTER (WHERE r.chainlink_open_price IS NULL),
  count(*) FILTER (WHERE r.chainlink_close_price IS NULL),
  count(*) FILTER (WHERE r.winner IS NULL)
FROM polymarket_btc_5m_resolutions r JOIN market_windows mw USING (market_id)
WHERE r.resolution_status='resolved'
  AND mw.market_start_ms BETWEEN 1788220800000 AND 1788825600000-300000;
"""
    query = "-- Exact pilot_part3b.sql section 13 only; retrospective source-clock reproduction.\n" + safety + "\n" + metadata + "\n" + section
    query_path = directory / "query.sql"
    query_path.write_text(query, encoding="utf-8", newline="\n")
    remote = (
        "set -eu\n"
        "sudo -u postgres psql -X -A -t -q -v ON_ERROR_STOP=1 -d price_collector <<'H3_SECTION13_SQL'\n"
        + query + "\nH3_SECTION13_SQL\n"
    )
    started_utc = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
         "root@152.42.247.86", "bash -s"],
        input=remote.encode("utf-8"), capture_output=True, timeout=50,
    )
    elapsed = time.monotonic()-started
    (directory / "stdout.txt").write_bytes(result.stdout)
    (directory / "stderr.txt").write_bytes(result.stderr)
    text = result.stdout.decode("utf-8")
    errors, actual = [], []
    snapshot, cohort = None, None
    if result.returncode:
        errors.append(f"SSH/psql exit {result.returncode}")
    else:
        actual = table(text)
        metadata_lines = [line.split("|") for line in text.splitlines() if line.startswith("H3_AUDIT_META|")]
        cohort_lines = [line.split("|") for line in text.splitlines() if line.startswith("H3_COHORT_META|")]
        if len(metadata_lines) != 1 or metadata_lines[0][2:] != ["on", "repeatable read", "20s"]:
            errors.append("Unexpected transaction metadata")
        else:
            snapshot = metadata_lines[0][1]
        if len(cohort_lines) != 1:
            errors.append("Missing cohort audit metadata")
        else:
            keys = ("resolved_markets", "missing_open", "missing_close", "missing_winner")
            cohort = dict(zip(keys, map(int, cohort_lines[0][1:])))
        if actual != expected:
            errors.append("Reproduced rows differ from supplied section-13 output")
        if [row.split("|", 1)[0] for row in actual] != ["60", "30", "15", "10", "5", "3"]:
            errors.append("Expected six ordered checkpoint rows")
        for line in actual:
            row = dict(zip(HEADER.split("|"), line.split("|")))
            if sum(int(row[key]) for key in ("both_ok", "projff_only_ok", "twap_only_ok", "both_wrong")) != int(row["n"]):
                errors.append("Paired outcome counts do not sum to denominator")
            if cohort and int(row["n"]) != cohort["resolved_markets"]:
                errors.append("Selected count differs from all resolved markets")
    if sha256(source) != source_hash or sha256(supplied) != supplied_hash:
        errors.append("Supplied source or output changed during reproduction")
    with (directory / "results.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER.split("|"))
        writer.writerows(row.split("|") for row in actual)
    manifest = {
        "status": "accepted" if not errors else "failed",
        "audit_type": "Exact fixed-week per-market section-13 source-clock reproduction; not receipt-causal",
        "started_utc": started_utc, "elapsed_seconds": round(elapsed, 3),
        "ssh_psql_exit_code": result.returncode, "validation_errors": errors,
        "supplied_rows_match_exactly": actual == expected,
        "rows": len(actual), "snapshot_ms": snapshot, "cohort_audit": cohort,
        "host": "152.42.247.86", "database": "price_collector",
        "read_only": True, "isolation": "repeatable read", "statement_timeout": "20s",
        "cohort_start_ms": 1788220800000, "cohort_end_ms": 1788825600000,
        "source_path": str(source), "source_sha256": source_hash,
        "supplied_output_path": str(supplied), "supplied_output_sha256": supplied_hash,
        "preamble_source_sha256": sha256(safety_source),
        "artifacts_sha256": {name: sha256(directory/name) for name in names if name != "manifest.json"},
        "code_sha256": sha256(Path(__file__)),
        "quantile_note": "Preserves original percentile_cont approximation on finalized dimensionless bps; financial projection arithmetic remains PostgreSQL NUMERIC.",
        "scope_note": "Only per-market section 13 executed; no pilot_part3.sql per-second queries. Original external files and prior reviews remain unchanged.",
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8", newline="\n")
    print(json.dumps({"status": manifest["status"], "elapsed_seconds": manifest["elapsed_seconds"],
                      "rows": len(actual), "supplied_rows_match_exactly": actual == expected,
                      "cohort_audit": cohort, "errors": errors}))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
