"""Run the supplied local third implementation and assert its printed checks."""
from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SCRIPT = ROOT / "research/spot_twap_response/checkpoint_a_replay/third_implementation_check.py"
BASE = ROOT / "tests/fixtures/ghost_twap"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    stdout_path = OUT / "third_review_stdout.txt"
    stderr_path = OUT / "third_review_stderr.txt"
    manifest_path = OUT / "third_review_manifest.json"
    if any(path.exists() for path in (stdout_path, stderr_path, manifest_path)):
        raise FileExistsError("preserve the existing third review artifacts")
    inputs = [SCRIPT, ROOT / "price_collector/ghost_twap.py", ROOT / "price_collector/market.py",
              BASE / "manifest.json", BASE / "recorded_hour_expected.csv.gz",
              BASE / "pending_v2/manifest.json"]
    inputs.extend(BASE / "pending_v2" / name for name in (
        "recorded_hour_events.csv.gz", "recorded_hour_decisions.csv.gz", "recorded_hour_expected.csv.gz"))
    before = {str(path.relative_to(ROOT)).replace("\\", "/"): digest(path) for path in inputs}
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    started = time.time_ns()
    monotonic_start = time.perf_counter_ns()
    result = subprocess.run([sys.executable, str(SCRIPT)], cwd=ROOT, env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    elapsed_ns = time.perf_counter_ns() - monotonic_start
    stdout_path.write_bytes(result.stdout)
    stderr_path.write_bytes(result.stderr)
    text = result.stdout.decode("utf-8")
    errors = []
    if result.returncode != 0:
        errors.append(f"script exit code {result.returncode}")
    baseline = re.search(r"contract 1 vs 2 on (\d+) rows: price/availability/ETA differences = (\d+); carried split mismatches = (\d+)", text)
    sampled = re.search(r"third implementation: checked (\d+) sampled forecasts; mismatches vs expected = (\d+); vs engine = (\d+)", text)
    if baseline is None or tuple(map(int, baseline.groups())) != (21600, 0, 0):
        errors.append("expected 21,600 unchanged baseline rows and zero carry split mismatches")
    if sampled is None or tuple(map(int, sampled.groups())) != (3090, 0, 0):
        errors.append("expected 3,090 sampled forecasts and zero mismatches against both references")
    if "MISMATCH" in text:
        errors.append("script printed a mismatch")
    manifest_v2 = json.loads((BASE / "pending_v2/manifest.json").read_text(encoding="utf-8"))
    carry_rows = []
    pattern = r"h=\s*(\d+): (\{[^\n]*\})  -> healthy \(no carry\) (\d+)/(\d+); carry <= 3 s (\d+)/(\d+)"
    for match in re.finditer(pattern, text):
        horizon, raw_distribution, healthy, total, within_three, second_total = match.groups()
        h, healthy, total, within_three, second_total = map(int, (horizon, healthy, total, within_three, second_total))
        distribution = ast.literal_eval(raw_distribution)
        if (not isinstance(distribution, dict)
                or any(type(age) is not int or age < 0 or type(count) is not int or count < 0
                       for age, count in distribution.items())):
            errors.append(f"invalid carry distribution at horizon {h}")
            continue
        counts = manifest_v2["quality_and_reason_counts"]
        if not (sum(distribution.values()) == total == second_total == 3585
                and distribution.get(0, 0) == healthy == counts[f"h{h}_healthy"]
                and sum(count for age, count in distribution.items() if age <= 3) == within_three
                and total - healthy == counts[f"h{h}_degraded"]):
            errors.append(f"carry table does not reconcile at horizon {h}")
        carry_rows.append(dict(horizon_s=h, maximum_interior_carry_seconds=distribution,
                               no_interior_carry=healthy, available=total,
                               maximum_interior_carry_at_most_three_seconds=within_three))
    if [row["horizon_s"] for row in carry_rows] != [1, 2, 3, 5, 10, 30]:
        errors.append("carry table horizon domain mismatch")
    after = {str(path.relative_to(ROOT)).replace("\\", "/"): digest(path) for path in inputs}
    if before != after:
        errors.append("an input changed during this local run")
    manifest = dict(status="accepted" if not errors else "rejected", errors=errors,
                    started_wall_ns=started, elapsed_ns=elapsed_ns,
                    python_executable=sys.executable, python_version=sys.version,
                    script_exit_code=result.returncode, statement="Local files only; no remote or database access",
                    script_printed_assertions_checked=True,
                    baseline_rows=21600 if baseline is not None else None,
                    sampled_forecasts=3090 if sampled is not None else None,
                    carry_table=carry_rows, inputs_sha256=before,
                    input_hashes_unchanged_during_run=before == after,
                    runner_sha256=digest(Path(__file__)),
                    outputs_sha256={stdout_path.name: digest(stdout_path), stderr_path.name: digest(stderr_path)})
    with manifest_path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps(manifest, indent=2))
    if errors:
        raise AssertionError("; ".join(errors))


if __name__ == "__main__":
    main()
