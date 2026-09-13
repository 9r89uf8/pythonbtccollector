"""Run only the two bounded Binance-clock checks after the DB queue is idle."""

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


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def csv_block(text, prefix):
    return text.split(prefix + "_BEGIN\n", 1)[1].split(prefix + "_END", 1)[0].strip() + "\n"


def main():
    root = Path(__file__).resolve().parent
    repo = root.parents[2]
    names = ("stdout.txt", "stderr.txt", "clock_profile.csv", "pairing.csv", "manifest.json")
    if any((root / name).exists() for name in names):
        raise SystemExit("Refusing to overwrite an existing Binance delay review")
    source = repo / "research/spot_twap_response/pilot_part4.sql"
    supplied = repo / "results/spot_twap_response/2026-09-13-pilot/pilot_part4_output.txt"
    collector = repo / "price_collector/collector.py"
    source_hash, supplied_hash = sha256(source), sha256(supplied)
    original_output = supplied.read_text(encoding="utf-8")
    section = original_output.split("=== 15.", 1)[1].split("=== 16.", 1)[0]
    expected = [line.strip().split("|") for line in section.splitlines() if line.startswith(("3|", "4|"))]
    if len(expected) != 2:
        raise SystemExit("Could not identify exactly two supplied section-15 rows")
    query = root / "query.sql"
    sql = query.read_text(encoding="utf-8")
    query_hash = sha256(query)
    script = "set -euo pipefail\nsudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -d price_collector <<'BINANCE_CLOCK_AUDIT_SQL'\n" + sql + "\nBINANCE_CLOCK_AUDIT_SQL\n"
    started = datetime.now(timezone.utc).isoformat()
    start_clock = time.monotonic()
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "root@152.42.247.86", "bash -s"],
        input=script.encode("utf-8"), capture_output=True, timeout=65,
    )
    elapsed = time.monotonic() - start_clock
    (root / "stdout.txt").write_bytes(result.stdout)
    (root / "stderr.txt").write_bytes(result.stderr)
    text = result.stdout.decode("utf-8").replace("\r\n", "\n")
    errors, profiles, pairs = [], [], []
    if result.returncode:
        errors.append(f"SSH/psql exit {result.returncode}")
    if "BINANCE_REVIEW_DONE" not in text:
        errors.append("No successful final transaction marker")
    if not errors:
        profile_text, pair_text = csv_block(text, "BINANCE_PROFILE"), csv_block(text, "BINANCE_PAIRING")
        (root / "clock_profile.csv").write_text(profile_text, encoding="utf-8")
        (root / "pairing.csv").write_text(pair_text, encoding="utf-8")
        profiles = list(csv.DictReader(io.StringIO(profile_text)))
        pairs = list(csv.DictReader(io.StringIO(pair_text)))
        if len(profiles) != 1 or len(pairs) != 2:
            errors.append("Unexpected diagnostic row counts")
        else:
            profile = profiles[0]
            if [profile["transaction_read_only"], profile["transaction_isolation"], profile["statement_timeout"]] != ["on", "repeatable read", "20s"]:
                errors.append("Unexpected transaction settings")
            fields = ("twap_stamp_offset_s", "n_pairs", "p10_ms", "p50_ms", "p90_ms", "p99_ms")
            if any([Decimal(row[field]) for field in fields] != [Decimal(value) for value in expected_row]
                   for row, expected_row in zip(pairs, expected)):
                errors.append("Section-15 timing numbers differ from supplied output")
    if (sha256(source), sha256(supplied), sha256(query)) != (source_hash, supplied_hash, query_hash):
        errors.append("A source artifact changed during the diagnostic")
    manifest = {
        "status": "accepted" if not errors else "failed",
        "scope": "Fixed-week Binance sampler-clock profile and section-15 receipt-pairing reproduction only; no economic propagation estimate",
        "started_utc": started,
        "elapsed_seconds": round(elapsed, 3),
        "ssh_psql_exit_code": result.returncode,
        "validation_errors": errors,
        "sample_cohort_start_ms": 1788220800000,
        "sample_cohort_end_ms_inclusive": 1788825600000,
        "source_sql_sha256": source_hash,
        "supplied_output_sha256": supplied_hash,
        "collector_code_sha256": sha256(collector),
        "profile": profiles[0] if len(profiles) == 1 else None,
        "section15_pairs": pairs,
        "section15_supplied_values_reproduced": not errors,
        "artifacts_sha256": {path.name: sha256(path) for path in (query, Path(__file__), *(root / name for name in names[:-1])) if path.exists()},
        "notes": [
            "Queries execute sequentially within one read-only repeatable-read transaction; each statement timeout is20s",
            "Expected BTC/USD topic filters are added to the original window-only TWAP restriction to use its source-time index",
            "Sample endpoints intentionally preserve the supplied SQL's inclusive BETWEEN convention",
            "Quantiles concern timestamp differences only; financial prices are used solely to detect repeated cached ticker states",
        ],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
