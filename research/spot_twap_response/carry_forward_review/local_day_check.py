"""Offline Sept-11 carry-forward audit from the existing source export only."""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict, deque
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path
import sys
import time

START_MS = 1789084800000
END_MS = 1789171200000
PRECISION = 80
LOOKBACK_MS = 600000


@dataclass(frozen=True)
class Window:
    filled_sum: Decimal
    retained_sum: Decimal
    retained_n: int
    missing_seed_n: int
    expired_seed_n: int
    carried_n: int


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def quantile(values: list[Decimal], p: Decimal) -> Decimal | None:
    if not values:
        return None
    with localcontext() as context:
        context.prec = PRECISION
        position = (len(values)-1)*p
        left = int(position)
        fraction = position-left
        return values[left] if not fraction else values[left]+fraction*(values[left+1]-values[left])


def build_windows(spot: dict[int, Decimal], first: int, last: int) -> tuple[dict[int, Window], dict]:
    """Window keys are newest constituent seconds; 60 slots, strict <600s LOCF."""
    if first % 1000 or last % 1000 or first > last:
        raise ValueError("Expected an inclusive UTC-second grid")
    stamps = sorted(spot)
    if any(s % 1000 or not spot[s].is_finite() or spot[s] <= 0 for s in stamps):
        raise ValueError("Expected positive finite prices at unique source seconds")
    index = bisect_right(stamps, first)-1
    queue, windows = deque(), {}
    filled_sum = retained_sum = Decimal(0)
    retained_n = missing_n = expired_n = carried_n = 0
    coverage = {"grid_first_ms": first, "grid_last_ms_inclusive": last,
                "grid_slots_n": (last-first)//1000+1, "retained_grid_slots_n": 0,
                "missing_retained_grid_slots_n": 0, "carried_grid_slots_n": 0,
                "unavailable_seed_grid_slots_n": 0, "expired_seed_grid_slots_n": 0,
                "maximum_valid_carry_age_ms": 0}
    missing_runs, run = [], 0
    with localcontext() as context:
        context.prec = PRECISION
        for second in range(first, last+1, 1000):
            while index+1 < len(stamps) and stamps[index+1] <= second:
                index += 1
            exact = spot.get(second)
            if exact is None:
                coverage["missing_retained_grid_slots_n"] += 1
                run += 1
            else:
                coverage["retained_grid_slots_n"] += 1
                if run:
                    missing_runs.append(run)
                    run = 0
            missing = int(index < 0)
            age = second-stamps[index] if index >= 0 else None
            expired = int(age is not None and age >= LOOKBACK_MS)
            filled = spot[stamps[index]] if not missing and not expired else None
            carried = int(filled is not None and age != 0)
            coverage["unavailable_seed_grid_slots_n"] += missing
            coverage["expired_seed_grid_slots_n"] += expired
            coverage["carried_grid_slots_n"] += carried
            if carried:
                coverage["maximum_valid_carry_age_ms"] = max(coverage["maximum_valid_carry_age_ms"], age)
            entry = (exact, filled, missing, expired, carried)
            queue.append(entry)
            if exact is not None:
                retained_sum += exact
                retained_n += 1
            if filled is not None:
                filled_sum += filled
            missing_n += missing
            expired_n += expired
            carried_n += carried
            if len(queue) > 60:
                old_exact, old_filled, old_missing, old_expired, old_carried = queue.popleft()
                if old_exact is not None:
                    retained_sum -= old_exact
                    retained_n -= 1
                if old_filled is not None:
                    filled_sum -= old_filled
                missing_n -= old_missing
                expired_n -= old_expired
                carried_n -= old_carried
            if len(queue) == 60:
                windows[second] = Window(filled_sum, retained_sum, retained_n, missing_n, expired_n, carried_n)
    if run:
        missing_runs.append(run)
    coverage.update({"missing_retained_runs_n": len(missing_runs),
                     "single_second_missing_runs_n": sum(length == 1 for length in missing_runs),
                     "missing_runs_at_least_10_seconds_n": sum(length >= 10 for length in missing_runs),
                     "maximum_missing_run_seconds": max(missing_runs, default=0),
                     "missing_run_touches_grid_left_edge": first not in spot,
                     "missing_run_touches_grid_right_edge": last not in spot})
    return windows, coverage


def self_check() -> None:
    dense = {i*1000: Decimal(i+1) for i in range(61)}
    windows, _ = build_windows(dense, 0, 60000)
    assert windows[59000].filled_sum == Decimal(1830)
    assert windows[60000].filled_sum == Decimal(1890)
    step = {i*1000: Decimal(100 if i < 0 else 160) for i in range(-60, 61)}
    windows, _ = build_windows(step, -59000, 59000)
    assert windows[0].filled_sum/60 == Decimal(101)   # W source stamp +3 seconds
    assert windows[29000].filled_sum/60 == Decimal(130)
    assert windows[59000].filled_sum/60 == Decimal(160)
    gap = {i*1000: Decimal(100 if i < 30 else 160) for i in range(60) if i not in range(1, 11)}
    windows, _ = build_windows(gap, 0, 59000)
    frame = windows[59000]
    assert frame.retained_n == 50 and frame.carried_n == 10
    assert frame.filled_sum/60 == Decimal(130)
    assert frame.retained_sum/frame.retained_n == Decimal(136)
    no_seed, _ = build_windows({i*1000: Decimal(100) for i in range(1, 60)}, 0, 59000)
    assert no_seed[59000].missing_seed_n == 1
    exact_limit, _ = build_windows({-600000: Decimal(100)}, 0, 59000)
    inside_limit, _ = build_windows({-599000: Decimal(100)}, 0, 59000)
    assert exact_limit[59000].expired_seed_n == 60
    assert inside_limit[59000].expired_seed_n == 59
    assert inside_limit[59000].filled_sum == Decimal(100)
    assert quantile([Decimal(1), Decimal(2), Decimal(3), Decimal(4)], Decimal("0.5")) == Decimal("2.5")
    assert quantile([Decimal(1), Decimal(2), Decimal(3), Decimal(4)], Decimal("0.99")) == Decimal("3.97")
    assert quantile([], Decimal("0.5")) is None
    with localcontext() as context:
        context.prec = 6
        exact = {i*1000: Decimal("10000.000000000000000001") for i in range(60)}
        windows, _ = build_windows(exact, 0, 59000)
        assert windows[59000].filled_sum == Decimal("600000.000000000000000060")


def jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def main() -> None:
    self_check()
    if sys.argv[1:] == ["--self-check"]:
        print("Focused known-step, gap, seed, strict-lookback, window-edge and Decimal checks passed")
        return
    if sys.argv[1:]:
        raise SystemExit("Usage: local_day_check.py [--self-check]")
    directory = Path(__file__).resolve().parent
    source_directory = directory.parent / "pilot_review"
    source = source_directory / "sources.csv"
    extraction_path = source_directory / "extraction.json"
    output = directory / "local_day_summary.csv"
    manifest_path = directory / "local_day_manifest.json"
    if output.exists() or manifest_path.exists():
        raise SystemExit("Refusing to overwrite existing local-day audit results")
    started = time.monotonic()
    started_utc = datetime.now(timezone.utc).isoformat()
    extraction = json.loads(extraction_path.read_text(encoding="utf-8"))
    if (extraction["psql_exit_code"] != 0 or extraction["read_only"] is not True
            or extraction["isolation"] != "repeatable read"
            or extraction["sources_sha256"] != sha256(source)
            or extraction["sql_sha256"] != sha256(source_directory/"extract_sources.sql")):
        raise ValueError("Existing export provenance validation failed")
    spot, twap_values = {}, defaultdict(set)
    twap_rows = 0
    with source.open(encoding="utf-8", newline="") as handle, localcontext() as context:
        context.prec = PRECISION
        for row in csv.DictReader(handle):
            stamp, sample = int(row["source_ms"]), int(row["sample_second_ms"])
            price = Decimal(row["price"])
            if stamp != sample or stamp % 1000 or not price.is_finite() or price <= 0:
                raise ValueError("Invalid exact-source-second price")
            if row["kind"] == "spot":
                if not START_MS-63000 <= stamp < END_MS+4000 or stamp in spot:
                    raise ValueError("Spot source range/uniqueness failure")
                spot[stamp] = price
            elif row["kind"] == "twap":
                if not START_MS <= stamp < END_MS or Decimal(row["price_e18"]) != price*Decimal(10)**18:
                    raise ValueError("TWAP range/E18 equality failure")
                twap_values[stamp].add(price)
                twap_rows += 1
            else:
                raise ValueError("Unexpected source kind")
    conflicts = {stamp for stamp, prices in twap_values.items() if len(prices) != 1}
    twap = {stamp: next(iter(prices)) for stamp, prices in twap_values.items() if len(prices) == 1}
    windows, grid_coverage = build_windows(spot, START_MS-62000, END_MS-4000)
    cohorts = {name: [] for name in ("complete", "incomplete", "all_paired")}
    exclusions = {"windows_missing_export_seed_n": 0, "windows_expired_seed_n": 0,
                  "windows_unavailable_seed_union_n": 0, "valid_locf_zero_retained_n": 0}
    with localcontext() as context:
        context.prec = PRECISION
        for stamp, actual in sorted(twap.items()):
            frame = windows[stamp-3000]
            exclusions["windows_missing_export_seed_n"] += bool(frame.missing_seed_n)
            exclusions["windows_expired_seed_n"] += bool(frame.expired_seed_n)
            if frame.missing_seed_n or frame.expired_seed_n:
                exclusions["windows_unavailable_seed_union_n"] += 1
                continue
            if not frame.retained_n:
                exclusions["valid_locf_zero_retained_n"] += 1
                continue
            filled_error = Decimal(10000)*abs(frame.filled_sum/Decimal(60)-actual)/actual
            retained_error = Decimal(10000)*abs(frame.retained_sum/Decimal(frame.retained_n)-actual)/actual
            record = (stamp, filled_error, retained_error, frame.retained_n)
            cohorts["complete" if frame.retained_n == 60 else "incomplete"].append(record)
            cohorts["all_paired"].append(record)
    records = []
    for name, rows in cohorts.items():
        record = {"cohort": name, "n": len(rows),
                  "minimum_retained_n": min((row[3] for row in rows), default=None),
                  "maximum_retained_n": max((row[3] for row in rows), default=None),
                  "locf_lower_absolute_error_n": sum(row[1] < row[2] for row in rows),
                  "equal_absolute_error_n": sum(row[1] == row[2] for row in rows),
                  "locf_higher_absolute_error_n": sum(row[1] > row[2] for row in rows)}
        for label, position in (("locf", 1), ("retained", 2)):
            errors = sorted(row[position] for row in rows)
            record[f"{label}_median_absolute_bps"] = quantile(errors, Decimal("0.5"))
            record[f"{label}_p99_absolute_bps"] = quantile(errors, Decimal("0.99"))
            record[f"{label}_maximum_absolute_bps"] = errors[-1] if errors else None
            record[f"{label}_maximum_first_twap_source_ms"] = min((row[0] for row in rows if row[position] == errors[-1]), default=None)
        records.append(record)
    complete = cohorts["complete"]
    if any(row[1] != row[2] for row in complete):
        raise ValueError("Complete-window methods must coincide exactly")
    if len(cohorts["all_paired"])+exclusions["windows_unavailable_seed_union_n"]+exclusions["valid_locf_zero_retained_n"] != len(twap):
        raise ValueError("Candidate and excluded window counts do not reconcile")
    with output.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    manifest = {
        "status": "accepted", "offline_only": True, "new_database_queries": 0,
        "started_utc": started_utc, "elapsed_seconds": round(time.monotonic()-started, 3),
        "target_start_ms": START_MS, "target_end_ms_exclusive": END_MS,
        "shift_seconds_a": -3, "formula": "60 source-second slots [u-62000,u-3000]; latest retained spot <= slot and > slot-600000",
        "seed_policy": "Use only exported observations; exclude every window containing an unavailable/expired seed. Do not invent pre-export values.",
        "retained_baseline": "Arithmetic mean of actual retained observations within the same 60 slots; undefined at count zero",
        "target_source": "Durable TWAP events, unique exact source seconds; identical duplicate values collapsed, conflicting stamps excluded",
        "residual": "10000*abs(reconstructed_mean-TWAP)/TWAP",
        "decimal_precision": PRECISION, "quantiles": "Exact Decimal sorted residuals; linear interpolation at (n-1)*p",
        "spot_export_rows": len(spot), "spot_export_first_ms": min(spot), "spot_export_last_ms": max(spot),
        "twap_event_rows": twap_rows, "twap_unique_source_stamps": len(twap_values),
        "twap_conflicting_stamps_excluded": len(conflicts),
        "twap_duplicate_rows_beyond_unique_stamps": twap_rows-len(twap_values),
        "target_calendar_seconds": (END_MS-START_MS)//1000,
        "missing_twap_source_seconds": (END_MS-START_MS)//1000-len(twap_values),
        "grid_coverage": grid_coverage, "window_exclusions": exclusions,
        "focused_checks": "passed: known persistent step; gap filling versus retained mean; missing seed; exact 600s rejection/599s acceptance; inclusive 60-slot edges; linear median/p99; 18-place sums under low ambient precision",
        "limitations": [
            "Independent day from the Sept1-8 pilot week, but Sept11 was already inspected for alignment; not held-out validation",
            "Source-time retained-history reconstruction only; receipt availability, frontend delay and settlement forecasts not tested",
            "A missing retained stamp is not proof of connection loss; run lengths describe the exported source-second grid",
            "Export starts day-63s, not day-662s; any required earlier unknown seed is explicitly excluded",
            "Same-second spot upserts can erase earlier states; no outside CSV observations are used",
        ],
        "input_extraction": extraction,
        "files_sha256": {str(path.relative_to(directory.parent)): sha256(path) for path in (
            source, extraction_path, source_directory/"extract_sources.sql", Path(__file__), output)},
        "results": records,
    }
    manifest_path.write_text(json.dumps(jsonable(manifest), indent=2)+"\n", encoding="utf-8", newline="\n")
    print(json.dumps(jsonable({"status": "accepted", "window_exclusions": exclusions,
                              "grid_coverage": grid_coverage, "results": records}), indent=2))


if __name__ == "__main__":
    main()
