"""Add retained market quotes to the frozen, descriptive leader-risk study.

Run with python -m research.leader_risk.market_prices --help.
This companion uses Decimal throughout and never estimates fees or profitability.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

from research.leader_risk import tabulate
from research.leader_risk.tabulate import (
    CHECKPOINTS, PRECISION, X_LABELS, Y_LABELS, Counts, bin_coordinates,
    boolean, number, percentage, utc_ms, utc_text, validate_rows, wilson,
)

ASK_BANDS = ("ask=0", "(0,0.90)", "[0.90,0.95)", "[0.95,0.98)", "[0.98,1)", "ask=1")
QUOTE_REASONS = (
    "no_quote_row", "token_mismatch", "quote_sample_stale", "quote_received_stale",
    "ask_price_missing", "ask_price_invalid", "ask_source_clock_missing_or_invalid",
    "ask_source_age", "ask_received_clock_missing_or_invalid", "ask_received_age",
    "bid_price_missing", "bid_price_invalid", "bid_source_clock_missing_or_invalid",
    "bid_source_age", "bid_received_clock_missing_or_invalid", "bid_received_age",
    "crossed_quote",
)
AUDIT_FIELDS = (
    "ask_available_N", "bid_available_N", "ask_only_N", "bid_only_N",
    "paired_quote_N", "crossed_quote_N", "neither_available_N",
    "ask_available_eq_0_N", "ask_available_eq_1_N",
)
QUOTE_METADATA = (
    "requested_cohort_start_ms", "cohort_start_ms", "cohort_end_ms",
    "cohort_market_count", "quote_extraction_ms",
)


def median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    if any(not isinstance(value, Decimal) or not value.is_finite() for value in values):
        raise ValueError("Median requires finite Decimal values")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    with localcontext() as context:
        context.prec = PRECISION
        return (ordered[middle - 1] + ordered[middle]) / 2


def price_band(ask: Decimal) -> str:
    if not isinstance(ask, Decimal) or not ask.is_finite() or not 0 <= ask <= 1:
        raise ValueError("Ask band requires a finite Decimal price in [0, 1]")
    if ask == 0:
        return ASK_BANDS[0]
    if ask < Decimal("0.90"):
        return ASK_BANDS[1]
    if ask < Decimal("0.95"):
        return ASK_BANDS[2]
    if ask < Decimal("0.98"):
        return ASK_BANDS[3]
    return ASK_BANDS[4] if ask < 1 else ASK_BANDS[5]


def _optional_int(value: str | None) -> int | None:
    return None if value in (None, "") else int(value)


def _price(value: str | None, side: str, reasons: list[str]) -> Decimal | None:
    if value in (None, ""):
        reasons.append(f"{side}_price_missing")
        return None
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        result = None
    if result is None or not result.is_finite() or not 0 <= result <= 1:
        reasons.append(f"{side}_price_invalid")
        return None
    return result


def _clock_fresh(value: str | None, name: str, cut_ms: int, max_age: int,
                 reasons: list[str]) -> bool:
    try:
        clock = _optional_int(value)
    except (ValueError, TypeError):
        clock = None
    if clock is None:
        reasons.append(f"{name}_clock_missing_or_invalid")
        return False
    if not 0 <= cut_ms - clock <= max_age:
        reasons.append(f"{name}_age")
        return False
    return True


@dataclass(frozen=True)
class QuoteAssessment:
    bid: Decimal | None
    ask: Decimal | None
    bid_available: bool
    ask_available: bool
    matched: bool
    reasons: tuple[str, ...]


def assess_quote(row: dict[str, str], leader: str, cut_ms: int,
                 quote_max_age_ms: int = 3000) -> QuoteAssessment:
    if leader not in ("Up", "Down"):
        raise ValueError("Quote selection requires an Up or Down leader")
    if isinstance(quote_max_age_ms, bool) or not isinstance(quote_max_age_ms, int) or quote_max_age_ms < 0:
        raise ValueError("Quote maximum age must be a nonnegative integer")
    reasons: list[str] = []
    sample = _optional_int(row["quote_sample_ms"])
    received = _optional_int(row["quote_received_ms"])
    if (sample is None) != (received is None):
        raise ValueError("Selected quote sample and row receipt disagree")
    if sample is not None and (sample > cut_ms or received > cut_ms):
        raise ValueError("Selected quote sample or receipt is after the checkpoint")
    if row["tokens_match"] not in ("", "t", "f"):
        raise ValueError("Invalid token-match flag")
    if sample is None:
        reasons.append("no_quote_row")
    elif not 0 <= cut_ms - sample <= quote_max_age_ms:
        reasons.append("quote_sample_stale")
    if received is not None and not 0 <= cut_ms - received <= quote_max_age_ms:
        reasons.append("quote_received_stale")
    if row["tokens_match"] != "t":
        reasons.append("token_mismatch")
    common_valid = not reasons
    prefix = leader.lower()
    prices: dict[str, Decimal | None] = {}
    available: dict[str, bool] = {}
    for side in ("bid", "ask"):
        prices[side] = _price(row[f"{prefix}_{side}"], side, reasons)
        source_fresh = _clock_fresh(row[f"{prefix}_{side}_source_ms"], f"{side}_source",
                                    cut_ms, quote_max_age_ms, reasons)
        receipt_fresh = _clock_fresh(row[f"{prefix}_{side}_received_ms"], f"{side}_received",
                                     cut_ms, quote_max_age_ms, reasons)
        available[side] = common_valid and prices[side] is not None and source_fresh and receipt_fresh
    paired = available["bid"] and available["ask"]
    crossed = paired and prices["bid"] > prices["ask"]
    if crossed:
        reasons.append("crossed_quote")
    return QuoteAssessment(prices["bid"], prices["ask"], available["bid"], available["ask"],
                           paired and not crossed, tuple(reasons))


def validate_quote_rows(observations: list[dict[str, str]], quotes: list[dict[str, str]],
                        study_metadata: dict | None = None) -> dict:
    base = study_metadata if study_metadata is not None else validate_rows(observations)
    if not quotes:
        raise ValueError("Empty quote export")
    metadata = {name: int(quotes[0][name]) for name in QUOTE_METADATA}
    for name in QUOTE_METADATA[:-1]:
        if metadata[name] != base[name]:
            raise ValueError(f"Quote cohort metadata differs from frozen study: {name}")
    if metadata["quote_extraction_ms"] < metadata["cohort_end_ms"]:
        raise ValueError("Quote extraction precedes the completed cohort")
    base_by_key = {(int(row["market_id"]), int(row["t_sec"])): row for row in observations}
    seen = set()
    for row in quotes:
        if any(int(row[name]) != metadata[name] for name in QUOTE_METADATA):
            raise ValueError("Inconsistent quote extraction metadata")
        key = (int(row["market_id"]), int(row["t_sec"]))
        if key in seen:
            raise ValueError("Duplicate quote market/checkpoint key")
        seen.add(key)
        if key not in base_by_key or int(row["cut_ms"]) != int(base_by_key[key]["cut_ms"]):
            raise ValueError("Quote key or checkpoint differs from frozen study")
        sample, receipt = (_optional_int(row[name]) for name in ("quote_sample_ms", "quote_received_ms"))
        if (sample is None) != (receipt is None):
            raise ValueError("Selected quote sample and row receipt disagree")
        if sample is not None and (sample > int(row["cut_ms"]) or receipt > int(row["cut_ms"])):
            raise ValueError("Quote row contains a post-checkpoint sample or receipt")
        if row["tokens_match"] not in ("", "t", "f"):
            raise ValueError("Invalid token-match flag")
    if seen != set(base_by_key):
        raise ValueError("Quote export does not contain exactly the frozen study keys; possible truncation")
    return metadata


@dataclass
class PriceCounts:
    counts: Counts = field(default_factory=Counts)
    bids: list[Decimal] = field(default_factory=list)
    asks: list[Decimal] = field(default_factory=list)

    def add(self, leader: str, winner: str, bid: Decimal, ask: Decimal) -> None:
        self.counts.add(leader, winner)
        self.bids.append(bid)
        self.asks.append(ask)

    def record(self) -> dict:
        return {**self.counts.record(), "median_bid": median_decimal(self.bids),
                "median_ask": median_decimal(self.asks),
                "ask_eq_0_N": self.asks.count(Decimal(0)), "ask_eq_1_N": self.asks.count(Decimal(1))}


@dataclass
class CellCounts:
    full: Counts = field(default_factory=Counts)
    matched: PriceCounts = field(default_factory=PriceCounts)
    audit: Counter = field(default_factory=Counter)

    def add(self, leader: str, winner: str, quote: QuoteAssessment) -> None:
        self.full.add(leader, winner)
        bid, ask = quote.bid_available, quote.ask_available
        paired = bid and ask
        crossed = paired and not quote.matched
        flags = (ask, bid, ask and not bid, bid and not ask, paired, crossed, not (ask or bid),
                 ask and quote.ask == 0, ask and quote.ask == 1)
        for name, value in zip(AUDIT_FIELDS, flags):
            self.audit[name] += value
        for reason in quote.reasons:
            self.audit[f"excluded_{reason}_N"] += 1
        if quote.matched:
            self.matched.add(leader, winner, quote.bid, quote.ask)

    def record(self) -> dict:
        priced = self.matched.record()
        result = {**{f"full_{key}": value for key, value in self.full.record().items()},
                  **{f"matched_{key}": value for key, value in priced.items()
                     if key not in ("median_bid", "median_ask")},
                  "median_bid": priced["median_bid"], "median_ask": priced["median_ask"],
                  **{name: self.audit[name] for name in AUDIT_FIELDS},
                  **{f"excluded_{name}_N": self.audit[f"excluded_{name}_N"] for name in QUOTE_REASONS}}
        if result["full_N"] != sum(result[name] for name in (
                "matched_N", "ask_only_N", "bid_only_N", "crossed_quote_N", "neither_available_N")):
            raise ValueError("Quote availability counts do not conserve full-study eligibility")
        return result


def analyze(observations: list[dict[str, str]], quotes: list[dict[str, str]], *,
            quote_max_age_ms: int = 3000) -> dict:
    if isinstance(quote_max_age_ms, bool) or not isinstance(quote_max_age_ms, int) or quote_max_age_ms < 0:
        raise ValueError("Quote maximum age must be a nonnegative integer")
    metadata = validate_rows(observations)
    quote_metadata = validate_quote_rows(observations, quotes, metadata)
    quote_lookup = {(int(row["market_id"]), int(row["t_sec"])): row for row in quotes}
    cells = defaultdict(CellCounts)
    bands = defaultdict(PriceCounts)
    totals = {t: CellCounts() for t in CHECKPOINTS}
    coverage = {t: Counter() for t in CHECKPOINTS}
    for row in observations:
        t = int(row["t_sec"])
        coverage[t]["rows"] += 1
        if not boolean(row["inputs_available"]):
            coverage[t]["study_input_unavailable_N"] += 1
            continue
        if boolean(row["current_tie"]):
            coverage[t]["study_tie_N"] += 1
            continue
        leader, _, _, xi, yi = bin_coordinates(*(number(row[name]) for name in ("k", "w", "s")))
        quote = assess_quote(quote_lookup[(int(row["market_id"]), t)], leader, int(row["cut_ms"]), quote_max_age_ms)
        winner = row["official_winner"]
        cells[(t, xi, yi)].add(leader, winner, quote)
        totals[t].add(leader, winner, quote)
        if quote.matched:
            bands[(t, xi, yi, price_band(quote.ask))].add(leader, winner, quote.bid, quote.ask)
    records, band_records, coverage_records = [], [], []
    for t in CHECKPOINTS:
        for xi, x_label in enumerate(X_LABELS):
            for yi, y_label in enumerate(Y_LABELS):
                labels = {"t_sec": t, "x_bin": x_label, "y_bin": y_label}
                cell = cells[(t, xi, yi)].record()
                records.append({**labels, **cell})
                children = [bands[(t, xi, yi, band)].record() for band in ASK_BANDS]
                for attribute in ("N", "U", "L", "resolved"):
                    if sum(child[attribute] for child in children) != cell[f"matched_{attribute}"]:
                        raise ValueError("Ask bands do not conserve matched-quote counts")
                band_records.extend({**labels, "ask_band": band, **child} for band, child in zip(ASK_BANDS, children))
        total = totals[t].record()
        c = coverage[t]
        if c["rows"] != c["study_input_unavailable_N"] + c["study_tie_N"] + total["full_N"]:
            raise ValueError("Study coverage counts do not conserve rows")
        for prefix in ("full", "matched"):
            for attribute in ("N", "U", "L", "resolved"):
                if sum(row[f"{prefix}_{attribute}"] for row in records if row["t_sec"] == t) != total[f"{prefix}_{attribute}"]:
                    raise ValueError("Cell counts do not conserve checkpoint totals")
        coverage_records.append({"t_sec": t, "rows": c["rows"],
                                 "study_input_unavailable_N": c["study_input_unavailable_N"],
                                 "study_tie_N": c["study_tie_N"], **total})
    return {"metadata": {**metadata, "quote_extraction_ms": quote_metadata["quote_extraction_ms"],
                         "quote_max_age_ms": quote_max_age_ms},
            "cells": records, "bands": band_records, "coverage": coverage_records}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_extraction(record: dict, metadata: dict, sql_path: Path) -> None:
    if type(record.get("psql_exit_code")) is not int or record["psql_exit_code"] != 0:
        raise ValueError("Quote extraction must have a recorded successful psql exit")
    if record.get("read_only") is not True or record.get("isolation") != "repeatable read":
        raise ValueError("Quote extraction must use a read-only repeatable-read transaction")
    if not isinstance(record.get("sql_sha256"), str) or record["sql_sha256"].lower() != _sha256(sql_path):
        raise ValueError("Quote SQL hash does not match the extraction record")
    if (utc_ms(record["start_utc"]) != metadata["cohort_start_ms"]
            or utc_ms(record["end_utc_exclusive"]) != metadata["cohort_end_ms"]):
        raise ValueError("Quote extraction record and frozen cohort bounds differ")


def _money(value: Decimal | None) -> str:
    if value is None:
        return "unavailable"
    whole, _, fraction = format(Decimal(0) if value == 0 else value, "f").partition(".")
    return f"${whole}.{fraction.rstrip('0').ljust(2, '0')}"


def _risk(row: dict, prefix: str = "matched_") -> str:
    return (f"{row[prefix + 'L']}/{row[prefix + 'resolved']} ({percentage(row[prefix + 'loss_rate'])}; "
            f"95% {percentage(row[prefix + 'wilson_95_low'])}–{percentage(row[prefix + 'wilson_95_high'])}); "
            f"U={row[prefix + 'U']}")


def render_report(result: dict) -> str:
    m = result["metadata"]
    lines = ["# Market prices beside TWAP leader risk", "",
             f"Frozen study: **{utc_text(m['cohort_start_ms'])}** through **{utc_text(m['cohort_end_ms'])}** "
             f"(exclusive), **{m['markets']:,} markets / {m['rows']:,} checkpoint rows**. "
             f"Official outcomes remain frozen at **{utc_text(m['outcome_cutoff_ms'])}**. "
             f"Retained quote history was extracted at **{utc_text(m['quote_extraction_ms'])}**.", "",
             "The quoted token is the recorded TWAP leader: Up or Down. The full study and quote-matched subset "
             "have separate loss counts; an affordable ask band is scored on its own observations.", "",
             f"A matched quote requires correct token identity, a retained sample and row receipt no more than "
             f"{m['quote_max_age_ms']:,} ms old, independently fresh leader bid and ask source/receipt clocks, and bid <= ask. "
             "All clocks must be at or before the checkpoint; the freshness limit is inclusive. "
             "Fresh ask-only and bid-only counts remain separate. Opposite-outcome quote freshness does not refresh or invalidate the leader's sides.", "",
             "**These are retained sampled quotes, not executable prices or fills.** The one-second sampler can skip "
             "missing-side states, so a later withdrawal may be absent while an older retained quote still passes the age rule. "
             "Boundary prices of exactly $0 and $1 remain reported and counted; neither establishes orderability. "
             "No fees, slippage, execution delay, profit or trading edge are estimated here.", "",
             "## Coverage and matched prices", "",
             "| T (seconds) | Full-study N | Matched N | Matched losses/resolved; interval; U | Median bid | Median ask | Matched ask=0 / ask=1 |",
             "|---:|---:|---:|:---|---:|---:|---:|"]
    for row in result["coverage"]:
        lines.append(f"| {row['t_sec']} | {row['full_N']} | {row['matched_N']} | {_risk(row)} | "
                     f"{_money(row['median_bid'])} | {_money(row['median_ask'])} | "
                     f"{row['matched_ask_eq_0_N']} / {row['matched_ask_eq_1_N']} |")
    lines += ["", "[coverage.csv](coverage.csv) retains ask/bid availability separately and overlapping exclusion reasons. "
              "`ask_only_N` and `bid_only_N` mean exactly one fresh side; `paired_quote_N` means both sides are fresh, "
              "including any crossed pair. `matched_N` additionally rejects crossed pairs. "
              "Loss rates and Wilson intervals use resolved observations only; U remains explicit.", ""]
    by_key = {(row["t_sec"], row["x_bin"], row["y_bin"]): row for row in result["cells"]}
    for t in CHECKPOINTS:
        lines += [f"## {t} seconds remaining", "", "Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.", "",
                  "| X (bp) \\ Y (bp) | " + " | ".join(Y_LABELS) + " |", "|:---|:---|:---|:---|:---|"]
        for x in X_LABELS:
            rendered = []
            for y in Y_LABELS:
                row = by_key[(t, x, y)]
                rendered.append(f"ask {_money(row['median_ask'])}; bid {_money(row['median_bid'])}<br>"
                                f"{_risk(row)}<br>N={row['matched_N']}/{row['full_N']}; ask=1: {row['matched_ask_eq_1_N']}")
            lines.append("| " + x + " | " + " | ".join(rendered) + " |")
        lines.append("")
    lines += ["## Price-band companion", "",
              "[market_price_cells.csv](market_price_cells.csv) contains all 160 cells with full-study and matched-subset "
              "counts, intervals and price medians. [price_bands.csv](price_bands.csv) contains six disjoint ask bands "
              "for every cell: exactly 0, (0,0.90), [0.90,0.95), [0.95,0.98), [0.98,1), and exactly 1. "
              "Band rates are recomputed within the same matched bid/ask subset; empty bands have unavailable rates.", "",
              "These are descriptive historical associations. Medians do not show every opportunity, a quote is not a fill, "
              "and a low full-region loss rate cannot be assigned automatically to a cheaper subset. "
              "Pointwise Wilson intervals assume independent, comparable markets and do not correct for searching cells "
              "or temporal dependence. Repeated checkpoints are never pooled into an independent-market claim. "
              "[manifest.json](manifest.json) records both snapshots, code and input hashes.", ""]
    return "\n".join(lines)


def _jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_results(result: dict, output: Path, observations_path: Path, quotes_path: Path,
                  quote_extraction_path: Path, sql_path: Path) -> None:
    record = json.loads(quote_extraction_path.read_text(encoding="utf-8-sig"), parse_float=Decimal)
    validate_extraction(record, result["metadata"], sql_path)
    names = ("market_price_cells.csv", "price_bands.csv", "coverage.csv", "report.md", "manifest.json", "explorer_data.json")
    if any((output / name).exists() for name in names):
        raise ValueError("Output artifacts already exist; choose a new directory")
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / names[0], result["cells"])
    _write_csv(output / names[1], result["bands"])
    _write_csv(output / names[2], result["coverage"])
    (output / "report.md").write_text(render_report(result), encoding="utf-8")
    explorer = {**result, "cells": [{**row, "median_bid_display": _money(row["median_bid"]),
                                    "median_ask_display": _money(row["median_ask"]),
                                    "matched_loss_rate_display": percentage(row["matched_loss_rate"])}
                                   for row in result["cells"]]}
    (output / "explorer_data.json").write_text(json.dumps(_jsonable(explorer), indent=2) + "\n", encoding="utf-8")
    manifest = {"study": "Market quotes added to the frozen descriptive TWAP leader-risk study",
                **result["metadata"], "study_outcome_cutoff_utc": utc_text(result["metadata"]["outcome_cutoff_ms"]),
                "quote_extraction_utc": utc_text(result["metadata"]["quote_extraction_ms"]),
                "decimal_precision": PRECISION, "wilson_z": str(tabulate.Z), "ask_bands": ASK_BANDS,
                "primary_cell_count": len(result["cells"]), "price_band_count": len(result["bands"]),
                "eligible_checkpoint_observations": sum(row["full_N"] for row in result["coverage"]),
                "matched_checkpoint_observations": sum(row["matched_N"] for row in result["coverage"]),
                "observations_path": str(observations_path.resolve()), "observations_sha256": _sha256(observations_path),
                "quotes_path": str(quotes_path.resolve()), "quotes_sha256": _sha256(quotes_path),
                "quote_extraction_record_sha256": _sha256(quote_extraction_path),
                "extract_quotes_sql_sha256": _sha256(sql_path), "market_prices_py_sha256": _sha256(Path(__file__)),
                "tabulate_helper_sha256": _sha256(Path(tabulate.__file__)),
                "quote_extraction_process": record,
                "verification": {"identical_frozen_market_checkpoint_keys": True, "identical_checkpoint_clocks": True,
                                 "eight_rows_per_market": True, "snapshot_counts_match": True,
                                 "full_matched_and_band_counts_conserved": True},
                "quote_interpretation": "Retained sampled quotes may omit later withdrawals; prices at 0 or 1 do not prove orderability. No fee or profitability inference.",
                "artifacts_sha256": {name: _sha256(output / name) for name in names if name != "manifest.json"}}
    (output / "manifest.json").write_text(json.dumps(_jsonable(manifest), indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", required=True, type=Path)
    parser.add_argument("--quotes", required=True, type=Path)
    parser.add_argument("--quote-extraction", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--quote-max-age-ms", type=int, default=3000)
    parser.add_argument("--sql", type=Path, default=Path(__file__).with_name("extract_quotes.sql"))
    args = parser.parse_args()
    with args.observations.open(encoding="utf-8-sig", newline="") as handle:
        observations = list(csv.DictReader(handle))
    with args.quotes.open(encoding="utf-8-sig", newline="") as handle:
        quotes = list(csv.DictReader(handle))
    result = analyze(observations, quotes, quote_max_age_ms=args.quote_max_age_ms)
    write_results(result, args.output, args.observations, args.quotes, args.quote_extraction, args.sql)
    print(f"Wrote {len(result['cells'])} market-price cells and {len(result['bands'])} price bands to {args.output}")


if __name__ == "__main__":
    main()
