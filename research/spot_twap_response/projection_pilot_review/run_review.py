"""Bounded section-7 reproduction; writes audit artifacts only beside this file."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    root = Path(__file__).resolve().parent
    outputs = [root / name for name in ("results.csv", "stderr.txt", "manifest.json", "section7_original.sql")]
    if any(path.exists() for path in outputs):
        raise SystemExit("Refusing to overwrite existing projection audit outputs")
    pilot = root.parent / "pilot.sql"
    original = pilot.read_text(encoding="utf-8")
    marker = r"\echo === 7. Projection pilot"
    if original.count(marker) != 1:
        raise SystemExit("Could not uniquely identify original section 7")
    (root / "section7_original.sql").write_text(original[original.index(marker):], encoding="utf-8", newline="\n")
    query = root / "query.sql"
    sql = query.read_text(encoding="utf-8")
    remote_script = (
        "set -eu\n"
        "sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -d price_collector <<'H3_PROJECTION_SQL'\n"
        + sql + "\nH3_PROJECTION_SQL\n"
    )
    started_utc = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    # Match .h3-verification/run_remote.py: authenticated root SSH, fixed host,
    # BatchMode, 15-second connection timeout, and script over stdin to bash -s.
    completed = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
         "root@152.42.247.86", "bash -s"],
        input=remote_script.encode("utf-8"), capture_output=True, timeout=55,
    )
    elapsed = time.monotonic() - started
    (root / "results.csv").write_bytes(completed.stdout)
    (root / "stderr.txt").write_bytes(completed.stderr)
    errors = []
    rows = []
    if completed.returncode != 0:
        errors.append(f"SSH/psql exit code {completed.returncode}")
    else:
        rows = list(csv.DictReader(io.StringIO(completed.stdout.decode("utf-8"))))
        if [int(row["tsec"]) for row in rows] != [60,30,15,10,5,3]:
            errors.append("Expected exactly the six original checkpoint rows")
        for row in rows:
            if row["transaction_read_only"] != "on" or row["transaction_isolation"] != "repeatable read" or row["statement_timeout"] != "30s":
                errors.append("Unexpected transaction safety metadata")
            n = int(row["n"])
            if any(int(row[f"{kind}_correct_n"])+int(row[f"{kind}_pilot_error_n"]) != n for kind in ("proj", "twap", "spot")):
                errors.append("Correct/error counts do not sum to the pilot denominator")
            if int(row["resolved_cohort_n"]) != n+int(row["excluded_from_pilot_n"]):
                errors.append("Cohort and selected counts do not reconcile")
    manifest = {
        "status": "accepted" if not errors else "failed",
        "audit_type": "retrospective source-clock reproduction of original pilot section 7; not receipt-causal",
        "utc_started": started_utc, "elapsed_seconds": round(elapsed, 3),
        "ssh_psql_exit_code": completed.returncode, "validation_errors": errors,
        "host": "152.42.247.86", "database": "price_collector",
        "read_only": True, "isolation": "repeatable read", "statement_timeout": "30s",
        "requested_start_ms": 1788220800000, "requested_end_ms": 1788825600000,
        "snapshot_ms": rows[0]["snapshot_ms"] if rows else None,
        "returned_rows": len(rows),
        "source_pilot_path": str(pilot), "source_pilot_sha256": sha256(pilot),
        "artifacts_sha256": {path.name: sha256(path) for path in (root / "section7_original.sql", query, Path(__file__), root / "results.csv", root / "stderr.txt")},
        "preserved_pilot_conventions": [
            "Hard-coded source IDs verified against provider, symbol and stream identity",
            "Source-second exact lookup; no receipt-time cutoffs",
            "Reconciled opening price and inferred close>=open label",
            "At most three missing past slots filled with retained-constituent mean",
            "Original percentile_cont double-precision approximation on finalized dimensionless bps",
            "Original complete selected cohort shared by projection, raw TWAP and raw spot",
        ],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8", newline="\n")
    print(json.dumps({"status": manifest["status"], "elapsed_seconds": manifest["elapsed_seconds"], "rows": len(rows), "errors": errors}))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
