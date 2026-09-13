"""Decimal-only, descriptive leader-risk tables from the frozen SQL export.

Run with: python research/leader_risk/tabulate.py observations.csv --output RUN_DIR
No production imports, database connection, model fitting, or trading inference.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from pathlib import Path

CHECKPOINTS = (120, 90, 60, 30, 15, 10, 5, 3)
X_LABELS = ("[0,1)", "[1,2)", "[2,4)", "[4,8)", "[8,infinity)")
Y_LABELS = ("<-2", "[-2,0)", "[0,2)", ">=2")
REASONS = (
    "invalid_market_rule", "boundary_missing_or_late", "boundary_conflict",
    "twap_no_recent_receipt", "spot_no_recent_receipt",
    "twap_bad_source_age", "spot_bad_source_age",
)
PRECISION = 60
Z = Decimal("1.959963984540054")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DEFAULT_MARKER = "2026-09-10T02:30:00Z"


def utc_text(ms: int) -> str:
    return (EPOCH + timedelta(milliseconds=ms)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("Use an explicitly UTC timestamp")
    delta = parsed - EPOCH
    if delta.microseconds % 1000:
        raise ValueError("Timestamp must be an exact millisecond")
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def boolean(value: str) -> bool:
    if value not in ("t", "f"):
        raise ValueError(f"Expected PostgreSQL boolean, got {value!r}")
    return value == "t"


def number(value: str) -> Decimal | None:
    if value == "":
        return None
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError("Non-finite financial value")
    return parsed


def wilson(losses: int, resolved: int) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if not 0 <= losses <= resolved:
        raise ValueError("Require 0 <= losses <= resolved")
    if resolved == 0:
        return None, None, None
    with localcontext() as context:
        context.prec = PRECISION
        n = Decimal(resolved)
        p = Decimal(losses) / n
        a = 1 + Z * Z / n
        center = (p + Z * Z / (2 * n)) / a
        half = Z * (p * (1 - p) / n + Z * Z / (4 * n * n)).sqrt() / a
        return p, max(Decimal(0), center - half), min(Decimal(1), center + half)


def bin_coordinates(k: Decimal, w: Decimal, s: Decimal) -> tuple[str, Decimal, Decimal, int, int]:
    if min(k, w, s) <= 0 or w == k:
        raise ValueError("Coordinates require positive prices and a non-tied leader")
    with localcontext() as context:
        context.prec = PRECISION
        direction = Decimal(1) if w > k else Decimal(-1)
        x = 10000 * direction * (w - k) / k
        y = 10000 * direction * (s - w) / k
        xi = sum(x >= edge for edge in (1, 2, 4, 8))
        yi = sum(y >= edge for edge in (-2, 0, 2))
        return ("Up" if direction == 1 else "Down"), x, y, xi, yi


@dataclass
class Counts:
    N: int = 0
    U: int = 0
    L: int = 0

    def add(self, leader: str, winner: str) -> None:
        self.N += 1
        if not winner:
            self.U += 1
        elif winner != leader:
            self.L += 1

    def record(self) -> dict:
        n = self.N - self.U
        rate, low, high = wilson(self.L, n)
        return {"N": self.N, "U": self.U, "L": self.L, "resolved": n,
                "loss_rate": rate, "wilson_95_low": low, "wilson_95_high": high}


def validate_rows(rows: list[dict[str, str]]) -> dict:
    if not rows:
        raise ValueError("Empty export")
    metadata_names = ("requested_cohort_start_ms", "cohort_start_ms", "cohort_end_ms",
                      "outcome_cutoff_ms", "cohort_market_count")
    metadata = {name: int(rows[0][name]) for name in metadata_names}
    start, end = metadata["cohort_start_ms"], metadata["cohort_end_ms"]
    if (start != metadata["requested_cohort_start_ms"] or start % 300000
            or end % 300000 or end <= start or end > metadata["outcome_cutoff_ms"]):
        raise ValueError("Invalid, incomplete, or silently clamped cohort bounds")
    keys = set()
    market_checkpoints = defaultdict(set)
    market_identities = {}
    identity_fields = ("start_ms", "end_ms", "rule_valid", "resolution_status", "resolution_type",
                       "reconciled_settlement_rule_version", "official_winner", "official_k_audit_only")
    for row in rows:
        if any(int(row[name]) != metadata[name] for name in metadata_names):
            raise ValueError("Inconsistent extraction metadata")
        market_id, t = int(row["market_id"]), int(row["t_sec"])
        market_start, market_end, cut = int(row["start_ms"]), int(row["end_ms"]), int(row["cut_ms"])
        if (market_start != market_id * 300000 or market_end != market_start + 300000
                or not start <= market_start < end or market_end > end
                or t not in CHECKPOINTS or cut != market_end - t * 1000):
            raise ValueError(f"Invalid market/checkpoint boundary: {market_id}/{t}")
        if (market_id, t) in keys:
            raise ValueError(f"Duplicate market/checkpoint: {market_id}/{t}")
        keys.add((market_id, t))
        market_checkpoints[market_id].add(t)
        identity = tuple(row[name] for name in identity_fields)
        if market_id in market_identities and market_identities[market_id] != identity:
            raise ValueError("Market identity/outcome changes between checkpoints")
        market_identities[market_id] = identity
        k, w, s = (number(row[name]) for name in ("k", "w", "s"))
        variants = int(row["boundary_variants"])
        if variants < 0 or (variants == 1) != (k is not None):
            raise ValueError("Boundary variant count disagrees with K availability")
        if variants > 0 and (not row["k_received_ns"] or int(row["k_received_ns"]) > cut * 1000000):
            raise ValueError("K was not received by the checkpoint")
        w_received = int(row["w_received_ns"]) if row["w_received_ns"] else None
        s_received = int(row["s_received_ms"]) if row["s_received_ms"] else None
        if (w is None) != (w_received is None) or (s is None) != (s_received is None):
            raise ValueError("Selected price and receipt clock disagree")
        if w_received is not None and not (cut - 3000) * 1000000 <= w_received <= cut * 1000000:
            raise ValueError("TWAP receipt is outside the inclusive as-of interval")
        if s_received is not None and not cut - 3000 <= s_received <= cut:
            raise ValueError("Spot receipt is outside the inclusive as-of interval")
        if w is not None:
            with localcontext() as context:
                context.prec = PRECISION
                if number(row["w_e18"]) != w * Decimal(10) ** 18:
                    raise ValueError("TWAP price disagrees with exact E18 value")
        w_fresh = bool(row["w_source_ms"]) and 0 <= cut - int(row["w_source_ms"]) <= 3000
        s_fresh = bool(row["s_source_ms"]) and 0 <= cut - int(row["s_source_ms"]) <= 3000
        rule_valid = boolean(row["rule_valid"])
        available = rule_valid and all(p is not None and p > 0 for p in (k, w, s)) and w_fresh and s_fresh
        if boolean(row["inputs_available"]) != available:
            raise ValueError("SQL eligibility disagrees with prices/source clocks")
        if boolean(row["current_tie"]) != (k is not None and w is not None and k == w):
            raise ValueError("SQL tie flag disagrees with prices")
        expected_reasons = (not rule_valid, variants == 0, variants > 1, w_received is None,
                            s_received is None, w_received is not None and not w_fresh,
                            s_received is not None and not s_fresh)
        if any(boolean(row[name]) != expected for name, expected in zip(REASONS, expected_reasons)):
            raise ValueError("SQL exclusion flags disagree with underlying observations")
        winner = row["official_winner"]
        verified = (row["resolution_status"] == "resolved" and row["resolution_type"] == "winner"
                    and row["reconciled_settlement_rule_version"] == "btc-5m-twap-60")
        if winner not in ("", "Up", "Down") or (winner and not verified):
            raise ValueError("Unverified official winner")
    if any(ts != set(CHECKPOINTS) for ts in market_checkpoints.values()):
        raise ValueError("Require exactly eight checkpoints per market")
    if len(market_checkpoints) != metadata["cohort_market_count"]:
        raise ValueError("Exported market count does not match snapshot count; possible truncation")
    missing = sorted(set(range(start // 300000, end // 300000)) - market_checkpoints.keys())
    metadata.update({"calendar_market_slots": (end - start) // 300000,
                     "missing_metadata_market_ids": missing, "rows": len(rows),
                     "markets": len(market_checkpoints),
                     "first_stored_market_start_utc": utc_text(min(market_checkpoints) * 300000),
                     "last_stored_market_end_utc": utc_text((max(market_checkpoints) + 1) * 300000)})
    return metadata


def analyze(rows: list[dict[str, str]], comparison_ms: int) -> dict:
    metadata = validate_rows(rows)
    if comparison_ms % 300000:
        raise ValueError("Comparison marker must be a UTC five-minute boundary")
    cells = defaultdict(Counts)
    direction_cells = defaultdict(Counts)
    day_cells = defaultdict(Counts)
    period_cells = defaultdict(Counts)
    totals = defaultdict(Counts)
    period_totals = defaultdict(Counts)
    direction_totals = defaultdict(Counts)
    day_totals = defaultdict(Counts)
    days = sorted({utc_text(int(row["start_ms"]))[:10] for row in rows})
    for t in CHECKPOINTS:
        for direction in ("Up", "Down"):
            direction_totals[(t, direction)]
        for period in ("pre", "post"):
            period_totals[(t, period)]
        for day in days:
            day_totals[(t, day)]
    coverage = {t: Counter() for t in CHECKPOINTS}
    reasons = {t: Counter() for t in CHECKPOINTS}
    outcomes = Counter()
    for row in rows:
        t = int(row["t_sec"])
        coverage[t]["total"] += 1
        available, tied = boolean(row["inputs_available"]), boolean(row["current_tie"])
        status = "input_unavailable" if not available else "available_tie" if tied else "eligible"
        coverage[t][status] += 1
        outcomes[(t, status, row["resolution_status"], row["resolution_type"],
                  row["reconciled_settlement_rule_version"], row["official_winner"])] += 1
        if not available:
            for name in REASONS:
                reasons[t][name] += boolean(row[name])
            reasons[t]["nonpositive_price"] += any(number(row[name]) is not None and number(row[name]) <= 0
                                                     for name in ("k", "w", "s"))
            continue
        if tied:
            continue
        leader, _, _, xi, yi = bin_coordinates(*(number(row[name]) for name in ("k", "w", "s")))
        winner = row["official_winner"]
        day = utc_text(int(row["start_ms"]))[:10]
        period = "pre" if int(row["start_ms"]) < comparison_ms else "post"
        for counter in (cells[(t, xi, yi)], direction_cells[(t, leader, xi, yi)],
                        day_cells[(t, day, xi, yi)], period_cells[(t, period, xi, yi)],
                        totals[t], period_totals[(t, period)], direction_totals[(t, leader)],
                        day_totals[(t, day)]):
            counter.add(leader, winner)
        coverage[t]["eligible_unknown_outcome"] += not bool(winner)
        cut = int(row["cut_ms"])
        coverage[t]["eligible_twap_source_age_3000"] += cut - int(row["w_source_ms"]) == 3000
        coverage[t]["eligible_spot_source_age_3000"] += cut - int(row["s_source_ms"]) == 3000
        coverage[t]["eligible_either_source_age_3000"] += (cut - int(row["w_source_ms"]) == 3000
                                                          or cut - int(row["s_source_ms"]) == 3000)
    for t in CHECKPOINTS:
        c = coverage[t]
        if c["total"] != c["input_unavailable"] + c["available_tie"] + c["eligible"]:
            raise ValueError("Coverage conservation failed")
        for name in ("N", "U", "L"):
            if sum(getattr(cells[(t, xi, yi)], name) for xi in range(5) for yi in range(4)) != getattr(totals[t], name):
                raise ValueError("Cell count conservation failed")
        if totals[t].N != c["eligible"] or totals[t].U != c["eligible_unknown_outcome"]:
            raise ValueError("Eligibility conservation failed")
    return {"metadata": metadata, "cells": cells, "direction_cells": direction_cells,
            "day_cells": day_cells, "period_cells": period_cells, "totals": totals,
            "period_totals": period_totals, "direction_totals": direction_totals,
            "day_totals": day_totals, "coverage": coverage, "reasons": reasons,
            "outcomes": outcomes, "comparison_ms": comparison_ms}


def write_csv(path: Path, records: list[dict]) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def percentage(value: Decimal | None) -> str:
    if value is None:
        return "unavailable"
    with localcontext() as context:
        context.prec = PRECISION
        return f"{value * 100:.2f}%"


def display(counts: Counts) -> str:
    r = counts.record()
    interval = (f"[{percentage(r['wilson_95_low'])}, {percentage(r['wilson_95_high'])}]"
                if r["resolved"] else "[unavailable]")
    return f"{counts.L}/{r['resolved']} = {percentage(r['loss_rate'])} {interval}; U={counts.U}"


def render_report(result: dict) -> str:
    meta, totals = result["metadata"], result["totals"]
    lines = ["# TWAP leader-loss risk: full available history", "",
             f"Requested cohort: **{utc_text(meta['cohort_start_ms'])}** through "
             f"**{utc_text(meta['cohort_end_ms'])}** (exclusive). Official outcomes read at "
             f"**{utc_text(meta['outcome_cutoff_ms'])}**.", "",
             f"The export contains **{meta['markets']:,} markets / {meta['rows']:,} checkpoints**. "
             f"Of {meta['calendar_market_slots']:,} calendar slots, "
             f"{len(meta['missing_metadata_market_ids']):,} lack stored market metadata; they are outside the observed sample. "
             f"The first stored market starts {meta['first_stored_market_start_utc']}.", "",
             "Each cell shows **L/resolved = loss rate [pointwise 95% Wilson interval]; U=unknown outcomes**. "
             "N = resolved + U. A loss means the checkpoint's TWAP leader differs from the verified official winner. "
             "Unknown outcomes remain counted; rates use resolved observations only. Empty or all-unknown cells have unavailable rates.", "",
             "X is TWAP distance from the stream-observed Price to Beat in the leader's direction, in basis points. "
             "Y is Chainlink spot minus TWAP in that direction, using the same Price to Beat denominator. "
             "Negative Y means spot trails TWAP; it does not alone mean spot crossed the Price to Beat.", "",
             "## Coverage and overall loss rates", "",
             "| Seconds remaining | Rows | Inputs unavailable | Available ties | Eligible N | Losses / resolved; 95% interval; U |",
             "|---:|---:|---:|---:|---:|:---|"]
    for t in CHECKPOINTS:
        c = result["coverage"][t]
        lines.append(f"| {t} | {c['total']} | {c['input_unavailable']} | {c['available_tie']} | {c['eligible']} | {display(totals[t])} |")
    lines += ["", "Unavailable-reason counts can overlap; see [coverage.csv](coverage.csv) and "
              "[exclusions.csv](exclusions.csv). [Outcome accounting](outcome_accounting.csv) separates eligible unknowns "
              "from unavailable inputs and ties. Missing calendar IDs and source-market guard evidence are in [manifest.json](manifest.json).", ""]
    for t in CHECKPOINTS:
        lines += [f"## {t} seconds remaining", "", "| X (bp) \\ Y (bp) | " + " | ".join(Y_LABELS) + " |",
                  "|:---|:---|:---|:---|:---|"]
        for xi, label in enumerate(X_LABELS):
            lines.append("| " + label + " | " + " | ".join(display(result["cells"][(t, xi, yi)]) for yi in range(4)) + " |")
        lines.append("")
    marker_description = ("the first full market after deployment of `ace8d19`"
                          if result["comparison_ms"] == utc_ms(DEFAULT_MARKER) else "the requested descriptive split")
    lines += ["## One date comparison", "",
              f"The marker is **{utc_text(result['comparison_ms'])}**, {marker_description}. "
              "The September 10 deployment `ace8d19` did not modify these spot/TWAP writers or official-resolution parsing, and the TWAP "
              "connection spanned deployment. Earlier valid history remains in every primary grid. This split is descriptive: "
              "overlapping intervals do not establish equal risk, and different X/Y composition can change overall rates.", "",
              "| Seconds remaining | Before marker: losses / resolved; 95% interval; U | At/after marker: losses / resolved; 95% interval; U |",
              "|---:|:---|:---|"]
    for t in CHECKPOINTS:
        lines.append(f"| {t} | {display(result['period_totals'][(t, 'pre')])} | {display(result['period_totals'][(t, 'post')])} |")
    lines += ["", "The same split by X/Y cell appears once in [comparison_cells.csv](comparison_cells.csv). "
              "[Direction totals](direction_totals.csv), [direction cells](direction_cells.csv), "
              "[daily totals](daily_totals.csv), and [daily cells](daily_cells.csv) are descriptive breakdowns; "
              "days use UTC market-start dates. The complete primary table is [grids.csv](grids.csv).", "",
              "## Interpretation limits", "",
              "These are sampled-history conditional loss rates, not fill probabilities, profitability, or evidence that a cell "
              "will stay below 1%. The opening reference must have an exact boundary TWAP event received by the checkpoint; "
              "missing or conflicting references exclude the checkpoint. Spot is retained one-second history with same-second "
              "upserts, not a complete replay. Selected prices must pass both source and receipt age, inclusively 0–3,000 ms; "
              "the number using exactly 3,000 ms source age is recorded in coverage.csv. A later fresh tick cannot repair an earlier missing decision.", "",
              "Wilson intervals are pointwise descriptions under independent comparable-market assumptions. They do not "
              "adjust for temporal dependence, searching 160 cells, or future regime changes. Repeated checkpoints in one "
              "market are never pooled into a larger independent sample. Low observed losses and zero-loss cells can still "
              "have wide intervals. Any optional future validation requires its own frozen condition and fresh observations.", ""]
    return "\n".join(lines)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_results(result: dict, output: Path, input_path: Path, sql_path: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    names = ("grids.csv", "comparison_cells.csv", "direction_cells.csv", "daily_cells.csv",
             "direction_totals.csv", "daily_totals.csv", "comparison_totals.csv", "totals.csv",
             "coverage.csv", "exclusions.csv", "outcome_accounting.csv", "manifest.json", "report.md")
    if any((output / name).exists() for name in names):
        raise ValueError("Output artifacts already exist; choose a new directory")
    for filename, source, dimension, values in (
        ("grids.csv", "cells", None, (None,)),
        ("comparison_cells.csv", "period_cells", "period", ("pre", "post")),
        ("direction_cells.csv", "direction_cells", "leader", ("Up", "Down")),
        ("daily_cells.csv", "day_cells", "utc_market_start_date", sorted({key[1] for key in result["day_totals"]})),
    ):
        records = []
        for t in CHECKPOINTS:
            for value in values:
                for xi in range(5):
                    for yi in range(4):
                        key = (t, value, xi, yi) if dimension else (t, xi, yi)
                        label = {"t_sec": t, **({dimension: value} if dimension else {}), "x_bin": X_LABELS[xi], "y_bin": Y_LABELS[yi]}
                        records.append({**label, **result[source][key].record()})
        write_csv(output / filename, records)
    for filename, source, dimension in (("totals.csv", "totals", None),
                                        ("comparison_totals.csv", "period_totals", "period"),
                                        ("direction_totals.csv", "direction_totals", "leader"),
                                        ("daily_totals.csv", "day_totals", "utc_market_start_date")):
        records = []
        for key, counts in sorted(result[source].items()):
            label = {"t_sec": key[0], dimension: key[1]} if dimension else {"t_sec": key}
            records.append({**label, **counts.record()})
        write_csv(output / filename, records)
    coverage_names = ("total", "input_unavailable", "available_tie", "eligible", "eligible_unknown_outcome",
                      "eligible_twap_source_age_3000", "eligible_spot_source_age_3000", "eligible_either_source_age_3000")
    write_csv(output / "coverage.csv", [{"t_sec": t, **{name: result["coverage"][t][name] for name in coverage_names}}
                                        for t in CHECKPOINTS])
    write_csv(output / "exclusions.csv", [{"t_sec": t, **{name: result["reasons"][t][name] for name in (*REASONS, "nonpositive_price")}}
                                          for t in CHECKPOINTS])
    write_csv(output / "outcome_accounting.csv", [
        {"t_sec": key[0], "input_status": key[1], "resolution_status": key[2], "resolution_type": key[3],
         "reconciled_rule": key[4], "official_winner": key[5], "rows": count}
        for key, count in sorted(result["outcomes"].items())])
    (output / "report.md").write_text(render_report(result), encoding="utf-8")
    git_result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2],
                                capture_output=True, text=True, check=False)
    manifest = {"study": "H3 TWAP leader risk; fresh descriptive study", **result["metadata"],
                "cohort_start_utc": utc_text(result["metadata"]["cohort_start_ms"]),
                "cohort_end_utc_exclusive": utc_text(result["metadata"]["cohort_end_ms"]),
                "outcome_cutoff_utc": utc_text(result["metadata"]["outcome_cutoff_ms"]),
                "comparison_marker_utc": utc_text(result["comparison_ms"]),
                "decimal_precision": PRECISION, "wilson_z": str(Z),
                "checkpoint_seconds": CHECKPOINTS, "x_bins_bp": X_LABELS, "y_bins_bp": Y_LABELS,
                "git_head_context_only": git_result.stdout.strip() if git_result.returncode == 0 else None,
                "version_note": "File hashes identify the actual working-tree code; Git HEAD alone does not.",
                "observations_sha256": sha256(input_path), "extract_sql_sha256": sha256(sql_path),
                "tabulate_py_sha256": sha256(Path(__file__)),
                "source_market_guard": {
                    "result": ("passed before COPY in the extraction's repeatable-read read-only transaction"
                               if result.get("extraction", {}).get("psql_exit_code") == 0 else "not attested by an extraction record"),
                    "sufficient_received_ms_minus_sample_second_ms_bounds_inclusive": [-2999, 177000],
                    "scope": "all expected TWAP60 and Chainlink spot rows, before source-freshness filtering",
                    "market_identity_basis": "source table CHECK constraints bind sample_second_ms to market_id",
                    "failure_behavior": "abort with ON_ERROR_STOP; no CSV accepted",
                    "note": "Pass attested by successful psql exit; extrema are not exported."},
                "verification": {"eight_unique_checkpoints_per_market": True, "snapshot_market_count_matches": True,
                                 "explicit_bounds_honored": True, "decimal_e18_and_clock_checks": True,
                                 "counts_conserved": True, "primary_cells_including_empty": 160},
                "artifacts_sha256": {name: sha256(output / name) for name in names if name != "manifest.json"}}
    if "guard_audit" in result:
        manifest["source_market_guard"] = result["guard_audit"]
    if "extraction" in result:
        manifest["extraction_process"] = result["extraction"]
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observations", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sql", type=Path, default=Path(__file__).with_name("extract.sql"))
    parser.add_argument("--comparison-marker", default=DEFAULT_MARKER)
    parser.add_argument("--expected-start", required=True, help="Requested extraction UTC start")
    parser.add_argument("--expected-end", required=True, help="Requested exclusive UTC end")
    parser.add_argument("--expected-sql-sha256", required=True, help="SQL hash recorded before extraction")
    args = parser.parse_args()
    if sha256(args.sql) != args.expected_sql_sha256.lower():
        raise ValueError("SQL changed after extraction; use the actual extraction SQL")
    extraction = json.loads((args.output / "extraction.json").read_text(encoding="utf-8-sig"))
    if (extraction.get("psql_exit_code") != 0 or extraction.get("read_only") is not True
            or extraction.get("isolation") != "repeatable read"
            or extraction.get("sql_sha256") != args.expected_sql_sha256.lower()
            or utc_ms(extraction["start_utc"]) != utc_ms(args.expected_start)
            or utc_ms(extraction["end_utc_exclusive"]) != utc_ms(args.expected_end)):
        raise ValueError("Extraction process record is unsuccessful or does not match this run")
    with args.observations.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    result = analyze(rows, utc_ms(args.comparison_marker))
    result["extraction"] = extraction
    if (result["metadata"]["cohort_start_ms"] != utc_ms(args.expected_start)
            or result["metadata"]["cohort_end_ms"] != utc_ms(args.expected_end)):
        raise ValueError("Export bounds do not match requested run")
    write_results(result, args.output, args.observations, args.sql)
    print(f"Wrote 8 grids / 160 cells for {result['metadata']['markets']:,} markets to {args.output}")


if __name__ == "__main__":
    main()
