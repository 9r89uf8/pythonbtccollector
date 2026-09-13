"""One bounded read-only scheduled-close audit, run only while the droplet is idle."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    directory = Path(__file__).resolve().parent
    query_path = directory / "closing_boundary_audit.sql"
    output_path = directory / "closing_boundary_audit_output.csv"
    error_path = directory / "closing_boundary_audit_stderr.txt"
    manifest_path = directory / "closing_boundary_audit_manifest.json"
    if any(path.exists() for path in (output_path, error_path, manifest_path)):
        raise SystemExit("Refusing to overwrite closing-boundary audit artifacts")
    sql = query_path.read_text(encoding="utf-8")
    sql_hash = digest(query_path)
    script = (
        "set -euo pipefail\n"
        "sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -d price_collector <<'CLOSING_BOUNDARY_AUDIT_SQL'\n"
        + sql + "\nCLOSING_BOUNDARY_AUDIT_SQL\n"
    )
    started = datetime.now(timezone.utc).isoformat()
    clock = time.monotonic()
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
         "root@152.42.247.86", "bash -s"],
        input=script.encode("utf-8"), capture_output=True, timeout=45,
    )
    elapsed = time.monotonic() - clock
    output_path.write_bytes(result.stdout)
    error_path.write_bytes(result.stderr)
    rows = list(csv.DictReader(io.StringIO(result.stdout.decode("utf-8")))) if result.returncode == 0 else []
    errors = []
    if result.returncode:
        errors.append(f"SSH/psql exit {result.returncode}")
    if len(rows) != 1:
        errors.append("Expected one summary row")
    else:
        row = rows[0]
        if [row["transaction_read_only"], row["transaction_isolation"], row["statement_timeout"]] != ["on", "repeatable read", "20s"]:
            errors.append("Unexpected transaction safety settings")
        if int(row["calendar_markets"]) != 2016:
            errors.append("Expected all 2,016 calendar markets")
        if sum(int(row[key]) for key in ("exact_end_missing_markets", "exact_end_unique_markets", "exact_end_conflicting_markets")) != 2016:
            errors.append("Boundary availability counts do not conserve the cohort")
        if int(row["exact_price_matches"]) + int(row["price_mismatches"]) != int(row["comparable_markets"]):
            errors.append("Price comparison counts do not conserve the comparable cohort")
    if digest(query_path) != sql_hash:
        errors.append("SQL changed while the audit ran")
    manifest = {
        "status": "accepted" if not errors else "failed",
        "purpose": "Grading-only check of scheduled close C=E against official final price; no input selection or future-window fitting",
        "cohort_start_ms": 1788220800000,
        "cohort_end_ms_exclusive": 1788825600000,
        "host": "152.42.247.86",
        "database": "price_collector",
        "started_utc": started,
        "elapsed_seconds": round(elapsed, 3),
        "ssh_psql_exit_code": result.returncode,
        "validation_errors": errors,
        "result": rows[0] if len(rows) == 1 else None,
        "artifacts_sha256": {
            path.name: digest(path) for path in (query_path, Path(__file__), output_path, error_path)
        },
        "execution_scope": "One fixed-week indexed per-market lateral query, 20-second statement timeout, read-only repeatable-read; run after receipt export completed",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
