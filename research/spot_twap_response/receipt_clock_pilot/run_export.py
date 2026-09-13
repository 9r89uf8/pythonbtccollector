"""Run sequential bounded statements in one SSH/psql read-only snapshot."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import subprocess
import threading

import pilot


def main():
    root = Path(__file__).resolve().parent
    outputs = [root / name for name in ("observations.csv", "extraction.json", "psql.stderr.txt", "early_t30.json")]
    if any(path.exists() for path in outputs):
        raise ValueError("Extraction outputs already exist; preserve them and choose a new run deliberately")
    guard = (root / "guard.sql").read_text(encoding="utf-8")
    query = (root / "extract.sql").read_text(encoding="utf-8")
    pieces = [guard]
    for t in (30, 60, 15, 10, 5, 3):
        for day in range(7):
            start = pilot.START_MS + day * 86400000
            pieces.append(f"\\echo PILOT_BEGIN_{t}_{day}\n\\set slice_start_ms {start}\n\\set slice_end_ms {start + 86400000}\n\\set t_sec {t}\n{query}\n\\echo PILOT_END_{t}_{day}\n")
        pieces.append(f"\\echo PILOT_CHECKPOINT_DONE_{t}\n")
    pieces.append("COMMIT;\n\\echo PILOT_ALL_DONE\n")
    script = "set -euo pipefail\nsudo -u postgres psql -X -q -A -t -v ON_ERROR_STOP=1 -d price_collector <<'RECEIPT_PILOT_SQL'\n" + "\n".join(pieces) + "\nRECEIPT_PILOT_SQL\n"
    started = datetime.now(timezone.utc).isoformat()
    record = {"started_at_utc": started, "cohort_start_ms": pilot.START_MS, "cohort_end_ms_exclusive": pilot.END_MS,
              "read_only": True, "isolation": "repeatable read", "statement_timeout": "20s",
              "lock_timeout": "2s", "slice_markets": 288, "sequential_statements": 42,
              "checkpoint_order": [30, 60, 15, 10, 5, 3], "guard_sql_sha256": pilot.sha256(root / "guard.sql"),
              "extract_sql_sha256": pilot.sha256(root / "extract.sql"), "runner_sha256": pilot.sha256(__file__)}
    header = None
    slice_rows = total_rows = complete_slices = 0
    checkpoint_lines = []
    expecting_header = guards_passed = all_done = False
    with (root / "psql.stderr.txt").open("wb") as error_file, (root / "observations.csv").open("w", encoding="utf-8", newline="") as output:
        process = subprocess.Popen(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
                                    "root@152.42.247.86", "bash -s"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=error_file)
        writer_error = []
        def send_script():
            try:
                process.stdin.write(script.encode("utf-8"))
                process.stdin.close()
            except Exception as exc:
                writer_error.append(repr(exc))
        writer = threading.Thread(target=send_script, daemon=True)
        writer.start()
        try:
            for content in iter(process.stdout.readline, b""):
                line = content.decode("utf-8").rstrip("\r\n")
                if not line:
                    continue
                if line.startswith("PILOT_CLOCK_PROFILE "):
                    record["clock_profile"] = json.loads(line.removeprefix("PILOT_CLOCK_PROFILE "))
                elif line == "PILOT_GUARDS_PASSED":
                    guards_passed = True
                    print(line, flush=True)
                elif line.startswith("PILOT_BEGIN_"):
                    expecting_header = True
                    slice_rows = 0
                elif line.startswith("PILOT_END_"):
                    if slice_rows != 288:
                        raise ValueError("Incomplete market slice")
                    complete_slices += 1
                    output.flush()
                    print(f"{line}: {slice_rows} rows", flush=True)
                elif line.startswith("PILOT_CHECKPOINT_DONE_"):
                    t = int(line.rsplit("_", 1)[1])
                    if len(checkpoint_lines) != 2016:
                        raise ValueError("Incomplete checkpoint")
                    raw_rows = list(csv.DictReader(io.StringIO(header + "\n" + "\n".join(checkpoint_lines))))
                    assessed = [pilot.assess_row(row) for row in raw_rows]
                    expected_keys = {(start // 300000, t) for start in range(pilot.START_MS, pilot.END_MS, 300000)}
                    if {(row["market_id"], row["t_sec"]) for row in assessed} != expected_keys or len({row["snapshot_ms"] for row in assessed}) != 1:
                        raise ValueError("Early checkpoint key/snapshot validation failed")
                    summaries = pilot.summarize_rows(assessed)
                    if t == 30:
                        (root / "early_t30.json").write_text(json.dumps(pilot.jsonable({"status": "provisional_until_complete_psql_exit", "summaries": summaries}), indent=2) + "\n", encoding="utf-8")
                    print("CHECKPOINT_SUMMARY " + json.dumps(pilot.jsonable(summaries)), flush=True)
                    checkpoint_lines = []
                elif line == "PILOT_ALL_DONE":
                    all_done = True
                elif expecting_header:
                    if header is None:
                        header = line
                        output.write(line + "\n")
                    elif line != header:
                        raise ValueError("Slice CSV headers differ")
                    expecting_header = False
                else:
                    if header is None or not guards_passed:
                        raise ValueError("Unexpected psql output before validated data")
                    output.write(line + "\n")
                    checkpoint_lines.append(line)
                    slice_rows += 1
                    total_rows += 1
            returncode = process.wait(timeout=30)
            writer.join(timeout=1)
            if writer_error or returncode or not guards_passed or not all_done or complete_slices != 42 or total_rows != 12096:
                raise ValueError(f"Incomplete extraction: exit={returncode}, slices={complete_slices}, rows={total_rows}, writer={writer_error}")
            record.update(psql_exit_code=returncode, status="accepted", guards_passed=True,
                          complete_slices=complete_slices, rows=total_rows)
        except Exception as exc:
            process.terminate()
            record.update(psql_exit_code=process.wait(timeout=30), status="failed", error=repr(exc),
                          guards_passed=guards_passed, complete_slices=complete_slices, rows=total_rows)
            raise
        finally:
            record["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
            output.flush()
            record["observations_sha256"] = pilot.sha256(root / "observations.csv")
            record["stderr_sha256"] = pilot.sha256(root / "psql.stderr.txt")
            (root / "extraction.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print("EXTRACTION_ACCEPTED " + json.dumps(record), flush=True)


if __name__ == "__main__":
    main()
