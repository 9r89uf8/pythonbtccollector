"""One-day, source-aligned, Decimal-only spot/TWAP diagnostic; no database writes."""

from __future__ import annotations

from collections import defaultdict, deque
import csv
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path


START_MS = 1789084800000
END_MS = 1789171200000
SHIFTS = tuple(range(-4, 5))
PRECISION = 80


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def complete_window_sums(spot: dict[int, Decimal]) -> dict[int, Decimal]:
    """Return exact sums only for all 60 distinct consecutive source seconds."""
    window = deque()
    sums = {}
    total = Decimal(0)
    with localcontext() as context:
        context.prec = PRECISION
        for stamp, price in sorted(spot.items()):
            if stamp % 1000 or not price.is_finite() or price <= 0:
                raise ValueError("Expected positive prices at exact source seconds")
            window.append((stamp, price))
            total += price
            while window and window[0][0] < stamp - 59000:
                total -= window.popleft()[1]
            if len(window) == 60 and window[0][0] == stamp - 59000:
                sums[stamp] = total
    return sums


def quantile(sorted_values: list[Decimal], probability: Decimal) -> Decimal | None:
    """Linear interpolation at (n-1)*p, entirely in Decimal arithmetic."""
    if not sorted_values:
        return None
    with localcontext() as context:
        context.prec = PRECISION
        position = (len(sorted_values) - 1) * probability
        left = int(position)
        fraction = position - left
        return (sorted_values[left] if fraction == 0 else sorted_values[left]
                + fraction * (sorted_values[left + 1] - sorted_values[left]))


def summary(values: list[Decimal]) -> dict:
    ordered = sorted(values)
    return {"n": len(ordered), "median_absolute_bps": quantile(ordered, Decimal("0.5")),
            "p99_absolute_bps": quantile(ordered, Decimal("0.99")),
            "maximum_absolute_bps": ordered[-1] if ordered else None}


def self_check() -> None:
    complete = {second * 1000: Decimal(second + 1) for second in range(61)}
    assert complete_window_sums(complete) == {59000: Decimal(1830), 60000: Decimal(1890)}
    del complete[30000]
    assert complete_window_sums(complete) == {}
    assert quantile([Decimal(1), Decimal(2), Decimal(3), Decimal(4)], Decimal("0.5")) == Decimal("2.5")
    assert quantile([Decimal(1), Decimal(2), Decimal(3), Decimal(4)], Decimal("0.99")) == Decimal("3.97")
    assert summary([])["median_absolute_bps"] is None
    with localcontext() as context:
        context.prec = 6
        values = {second * 1000: Decimal("10000.000000000000000001") for second in range(60)}
        assert complete_window_sums(values)[59000] == Decimal("600000.000000000000000060")


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
    root = Path(__file__).resolve().parent
    extraction = json.loads((root / "extraction.json").read_text(encoding="utf-8"))
    if (type(extraction["psql_exit_code"]) is not int or extraction["psql_exit_code"] != 0
            or extraction["read_only"] is not True or extraction["isolation"] != "repeatable read"
            or extraction["sql_sha256"] != sha256(root / "extract_sources.sql")
            or extraction["sources_sha256"] != sha256(root / "sources.csv")):
        raise ValueError("Source export must match a successful extraction")
    spot = {}
    twap_values = defaultdict(set)
    twap_rows = 0
    with (root / "sources.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            source = int(row["source_ms"])
            sample = int(row["sample_second_ms"])
            price = Decimal(row["price"])
            if not price.is_finite() or price <= 0 or source != sample or source % 1000:
                raise ValueError("Nonpositive/nonfinite price or non-exact source-second alignment")
            if row["kind"] == "spot":
                if not START_MS - 63000 <= source < END_MS + 4000 or source in spot:
                    raise ValueError("Spot range or unique-second invariant failed")
                spot[source] = price
            elif row["kind"] == "twap":
                with localcontext() as context:
                    context.prec = PRECISION
                    if Decimal(row["price_e18"]) != price * Decimal(10) ** 18:
                        raise ValueError("TWAP E18 value disagrees with NUMERIC price")
                if not START_MS <= source < END_MS:
                    raise ValueError("TWAP source timestamp outside the one-day target")
                twap_values[source].add(price)
                twap_rows += 1
            else:
                raise ValueError("Unknown source kind")
    conflicts = {stamp for stamp, values in twap_values.items() if len(values) != 1}
    twap = {stamp: next(iter(values)) for stamp, values in twap_values.items() if len(values) == 1}
    sums = complete_window_sums(spot)
    common = {stamp for stamp in twap if all(stamp + shift * 1000 in sums for shift in SHIFTS)}
    records = []
    with localcontext() as context:
        context.prec = PRECISION
        for shift in SHIFTS:
            residuals = []
            shared_residuals = []
            for stamp, w in twap.items():
                total = sums.get(stamp + shift * 1000)
                if total is None:
                    continue
                error = Decimal(10000) * abs(total / Decimal(60) - w) / w
                residuals.append(error)
                if stamp in common:
                    shared_residuals.append(error)
            records.append({"shift_seconds_a": shift, **summary(residuals),
                            **{f"common_{key}": value for key, value in summary(shared_residuals).items()}})
    crosscheck_run = json.loads((root / "crosscheck_execution.json").read_text(encoding="utf-8"))
    if (crosscheck_run["psql_exit_code"] != 0
            or crosscheck_run["sql_sha256"] != sha256(root / "crosscheck.sql")
            or crosscheck_run["output_sha256"] != sha256(root / "crosscheck.stdout.jsonl")):
        raise ValueError("Cross-check output must match its successful SQL execution")
    crosscheck = [json.loads(line) for line in (root / "crosscheck.stdout.jsonl").read_text(encoding="utf-8").splitlines()
                  if line.strip()]
    identity = crosscheck[0]
    if (identity["durable_source_timestamps"] != len(twap_values)
            or identity["materialized_seconds"] != len(twap_values)
            or any(identity[name] for name in ("durable_only_timestamps", "materialized_only_seconds",
                                               "matched_prices_different", "durable_conflict_groups",
                                               "materialized_source_sample_mismatches"))):
        raise ValueError("The materialized/durable TWAP equality cross-check failed")
    if {row["shift_seconds_a"] for row in crosscheck[1:]} != {-4, -3, -2, -1}:
        raise ValueError("Independent SQL cross-check must cover four negative shifts")
    maximum_difference = Decimal(0)
    with localcontext() as context:
        context.prec = PRECISION
        for sql_row in crosscheck[1:]:
            local = next(row for row in records if row["shift_seconds_a"] == sql_row["shift_seconds_a"])
            if local["n"] != sql_row["n"]:
                raise ValueError("Independent SQL sample count disagrees")
            for field in ("median_absolute_bps", "p99_absolute_bps", "maximum_absolute_bps"):
                difference = abs(local[field] - Decimal(sql_row[field]))
                maximum_difference = max(maximum_difference, difference)
                if difference > Decimal("1e-60"):
                    raise ValueError("Independent SQL residual summaries disagree")
    output = root / "shift_summary.csv"
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    manifest = {
        "target_start_utc": "2026-09-11T00:00:00Z", "target_end_utc_exclusive": "2026-09-12T00:00:00Z",
        "formula": "mean_a(u) = sum_{j=0..59} S(u + 1000*a - 1000*j) / 60; residual = 10000*abs(mean_a(u)-W(u))/W(u)",
        "shift_convention": "Newest spot source timestamp is u+a seconds; positive a uses spot timestamps later than W(u)",
        "window_requirement": "All 60 unique consecutive spot seconds; no filling, interpolation, or weighting of missing seconds",
        "twap_source": "polymarket_twap_events; btc/usd; crypto_prices_twap_sixty; window_s=60; exact provider timestamp",
        "spot_source": "price_samples instrument_id=2; retained Chainlink source-second price; provider_event_ms=sample_second_ms verified",
        "decimal_precision": PRECISION,
        "quantile_convention": "Sort exact Decimal absolute-bps residuals; interpolate linearly at (n-1)*p (p=0.5 and p=0.99)",
        "spot_rows_with_margins": len(spot), "spot_rows_in_target_day": sum(START_MS <= stamp < END_MS for stamp in spot),
        "target_calendar_seconds": (END_MS - START_MS) // 1000,
        "twap_event_rows": twap_rows, "twap_unique_source_timestamps": len(twap_values),
        "twap_conflicting_source_timestamps_excluded": len(conflicts),
        "twap_duplicate_rows_beyond_unique_timestamps": twap_rows - len(twap_values),
        "common_shift_cohort_n": len(common),
        "self_check": "passed: exact 60-second sums; incomplete-window rejection; linear median/p99; low-ambient-precision 18-place sums",
        "independent_sql_crosscheck": {"result": "passed", "target": "price_samples instrument_id=4",
                                        "materialized_durable_equality": identity,
                                        "maximum_summary_difference_bps": maximum_difference,
                                        "summary_difference_tolerance_bps": "1e-60",
                                        "execution": crosscheck_run},
        "extraction": extraction,
        "limitations": [
            "One day only; not reproduction of the external week or unspecified 29-day cohort",
            "Ex-post source-time alignment; no receipt-time decision replay or settlement projection was tested",
            "Spot same-second upserts erase earlier within-second values/conflicts; optional raw spot table had no Sept11 rows",
            "The local target uses durable TWAP events; a bounded SQL cross-check found the materialized instrument_id=4 target identical on this day",
            "No best offset was prespecified or validated on fresh data; all nine shifts are reported",
        ],
        "files_sha256": {name: sha256(root / name) for name in (
            "inventory.sql", "inventory.stdout.jsonl", "profile.sql", "profile.stdout.jsonl",
            "extract_sources.sql", "sources.csv", "extraction.json", "calculate.py", "shift_summary.csv",
            "crosscheck.sql", "crosscheck.stdout.jsonl", "crosscheck_execution.json")},
        "results": records,
    }
    (root / "manifest.json").write_text(json.dumps(jsonable(manifest), indent=2) + "\n", encoding="utf-8")
    print(json.dumps(jsonable({"common_shift_cohort_n": len(common), "results": records}), indent=2))


if __name__ == "__main__":
    main()
