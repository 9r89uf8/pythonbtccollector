"""Audit realized payout-minus-recorded-ask comparisons on frozen H3 data.

This is a hypothetical one-share, gross historical calculation, not expected
value, executable returns, fills, fees, or a trading recommendation. Run with
python -m research.leader_risk.quote_return_audit --help.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import dataclass, field
from decimal import Decimal, localcontext
import hashlib
import io
import json
from pathlib import Path

from research.leader_risk import market_prices, tabulate
from research.leader_risk.market_prices import (
    ASK_BANDS, QUOTE_REASONS, QuoteAssessment, assess_quote, price_band,
    validate_quote_rows,
)
from research.leader_risk.tabulate import (
    CHECKPOINTS, PRECISION, X_LABELS, Y_LABELS, Counts, bin_coordinates,
    boolean, number, utc_text, validate_rows,
)


QUOTE_MAX_AGE_MS = 3000
STATUSES = ("matched", "ask_only", "bid_only", "crossed_quote", "neither_available")
COMMON_REASONS = frozenset((
    "no_quote_row", "token_mismatch", "quote_sample_stale", "quote_received_stale",
))
DIAGNOSTIC_REASONS = (
    "bid_source_old", "ask_source_old", "bid_source_future", "ask_source_future",
    "bid_received_stale", "ask_received_stale", "bid_received_future", "ask_received_future",
    "quote_row_stale", "both_sources_old_common_fresh",
    "only_bid_unavailable_with_fresh_ask", "only_bid_source_old_with_fresh_ask",
    "only_bid_received_stale_with_fresh_ask",
)
REASONS = QUOTE_REASONS + DIAGNOSTIC_REASONS
INTERPRETATION = (
    "Hypothetical one share per resolved matched observation: official payout "
    "minus retained recorded ask, before any fees or execution costs. This is a "
    "realized gross historical comparison, not expected value, a fill, ROI, or "
    "evidence of executable profit. Boundary asks 0 and 1 do not establish orderability."
)
SCREEN_INTERPRETATION = (
    "The fixed-mean-ask Wilson screen is 1 - resolved_mean_ask - Wilson upper "
    "loss bound. It holds the realized mean ask fixed solely to reproduce the "
    "review's descriptive screen; it is NOT a validated confidence interval for "
    "expected return. It does not cover joint price/outcome variation, temporal "
    "dependence, repeated checkpoints, or searching many groups."
)


def _counts_record(counts: Counts) -> dict:
    result = counts.record()
    return {"N": result["N"], "U": result["U"], "L": result["L"],
            "n": result["resolved"], "loss_rate": result["loss_rate"],
            "wilson_95_low": result["wilson_95_low"], "wilson_95_high": result["wilson_95_high"]}


@dataclass
class GrossCounts:
    counts: Counts = field(default_factory=Counts)
    ask_sum: Decimal = Decimal(0)
    payout_sum: Decimal = Decimal(0)

    def add(self, leader: str, winner: str, ask: Decimal) -> None:
        self.counts.add(leader, winner)
        if not winner:
            return
        with localcontext() as context:
            context.prec = PRECISION
            self.ask_sum += ask
            self.payout_sum += Decimal(1) if winner == leader else Decimal(0)

    def record(self) -> dict:
        result = _counts_record(self.counts)
        with localcontext() as context:
            context.prec = PRECISION
            n = result["n"]
            gross_sum = self.payout_sum - self.ask_sum
            mean_ask = self.ask_sum / n if n else None
            gross_mean = gross_sum / n if n else None
            diagnostic = (Decimal(1) - mean_ask - result["wilson_95_high"]
                          if n else None)
        return {**result, "resolved_mean_ask": mean_ask,
                "gross_payout_sum": self.payout_sum, "gross_ask_sum": self.ask_sum,
                "gross_pnl_sum": gross_sum, "gross_pnl_mean": gross_mean,
                "fixed_mean_ask_wilson_screen_lower": diagnostic}


def _age(row: dict[str, str], key: str, cut_ms: int) -> int | None:
    try:
        value = row.get(key)
        return None if value in (None, "") else cut_ms - int(value)
    except (ValueError, TypeError):
        return None


def _status(quote: QuoteAssessment) -> str:
    if quote.matched:
        return "matched"
    if quote.bid_available and quote.ask_available:
        return "crossed_quote"
    if quote.ask_available:
        return "ask_only"
    return "bid_only" if quote.bid_available else "neither_available"


def _diagnostics(row: dict[str, str], leader: str, cut_ms: int,
                 quote: QuoteAssessment) -> tuple[str, ...]:
    flags = set(quote.reasons)
    for side in ("bid", "ask"):
        for clock in ("source", "received"):
            age = _age(row, f"{leader.lower()}_{side}_{clock}_ms", cut_ms)
            if age is not None and age > QUOTE_MAX_AGE_MS:
                suffix = "old" if clock == "source" else "stale"
                flags.add(f"{side}_{clock}_{suffix}")
            if age is not None and age < 0:
                flags.add(f"{side}_{clock}_future")
    if {"quote_sample_stale", "quote_received_stale"}.intersection(quote.reasons):
        flags.add("quote_row_stale")
    if (not COMMON_REASONS.intersection(quote.reasons)
            and {"bid_source_old", "ask_source_old"}.issubset(flags)):
        flags.add("both_sources_old_common_fresh")
    if quote.ask_available and not quote.bid_available:
        flags.add("only_bid_unavailable_with_fresh_ask")
        if set(quote.reasons) == {"bid_source_age"} and "bid_source_old" in flags:
            flags.add("only_bid_source_old_with_fresh_ask")
        if set(quote.reasons) == {"bid_received_age"} and "bid_received_stale" in flags:
            flags.add("only_bid_received_stale_with_fresh_ask")
    return tuple(reason for reason in REASONS if reason in flags)


def _screen_counts(rows: list[dict], grouping: str, minimum_basis: str) -> dict:
    admitted = [row for row in rows if row[minimum_basis] >= 100]
    return {"grouping": grouping, "minimum_basis": minimum_basis, "minimum_count": 100,
            "groups_considered": len(rows), "groups_meeting_minimum": len(admitted),
            "positive_gross_mean_groups": sum(
                row["gross_pnl_mean"] is not None and row["gross_pnl_mean"] > 0 for row in admitted),
            "positive_fixed_mean_ask_wilson_screen_groups": sum(
                row["fixed_mean_ask_wilson_screen_lower"] is not None
                and row["fixed_mean_ask_wilson_screen_lower"] > 0 for row in admitted)}


def build_audit(observations: list[dict[str, str]], quotes: list[dict[str, str]]) -> dict:
    metadata = validate_rows(observations)
    quote_metadata = validate_quote_rows(observations, quotes, metadata)
    quote_lookup = {(int(row["market_id"]), int(row["t_sec"])): row for row in quotes}
    full, excluded = defaultdict(Counts), defaultdict(Counts)
    totals, checkpoint_bands, cell_bands = (defaultdict(GrossCounts) for _ in range(3))
    status_counts, reason_counts = defaultdict(Counts), defaultdict(Counts)
    coverage_counts = {t: Counter() for t in CHECKPOINTS}
    for row in observations:
        t, cut = int(row["t_sec"]), int(row["cut_ms"])
        coverage_counts[t]["rows"] += 1
        if not boolean(row["inputs_available"]):
            coverage_counts[t]["study_input_unavailable_N"] += 1
            continue
        if boolean(row["current_tie"]):
            coverage_counts[t]["study_tie_N"] += 1
            continue
        leader, _, _, xi, yi = bin_coordinates(*(number(row[name]) for name in ("k", "w", "s")))
        winner = row["official_winner"]
        quote_row = quote_lookup[(int(row["market_id"]), t)]
        quote = assess_quote(quote_row, leader, cut, QUOTE_MAX_AGE_MS)
        full[t].add(leader, winner)
        status_counts[(t, _status(quote))].add(leader, winner)
        for reason in _diagnostics(quote_row, leader, cut, quote):
            reason_counts[(t, reason)].add(leader, winner)
        if not quote.matched:
            excluded[t].add(leader, winner)
            continue
        band = price_band(quote.ask)
        totals[t].add(leader, winner, quote.ask)
        checkpoint_bands[(t, band)].add(leader, winner, quote.ask)
        cell_bands[(t, xi, yi, band)].add(leader, winner, quote.ask)
    total_records, checkpoint_records, cell_records = [], [], []
    coverage_records, status_records, reason_records = [], [], []
    for t in CHECKPOINTS:
        total = totals[t].record()
        total_records.append({"t_sec": t, **total})
        checkpoint_records.extend({"t_sec": t, "ask_band": band, **checkpoint_bands[(t, band)].record()}
                                  for band in ASK_BANDS)
        for xi, x_bin in enumerate(X_LABELS):
            for yi, y_bin in enumerate(Y_LABELS):
                cell_records.extend({"t_sec": t, "x_bin": x_bin, "y_bin": y_bin,
                                     "ask_band": band, **cell_bands[(t, xi, yi, band)].record()}
                                    for band in ASK_BANDS)
        full_record, excluded_record = _counts_record(full[t]), _counts_record(excluded[t])
        for name in ("N", "U", "L", "n"):
            if full_record[name] != total[name] + excluded_record[name]:
                raise ValueError("Full/matched/excluded counts do not conserve eligible observations")
            if sum(_counts_record(status_counts[(t, status)])[name] for status in STATUSES) != full_record[name]:
                raise ValueError("Disjoint quote statuses do not conserve eligible observations")
            for records in (checkpoint_records, cell_records):
                if sum(record[name] for record in records if record["t_sec"] == t) != total[name]:
                    raise ValueError("Ask bands do not conserve matched observations")
        c = coverage_counts[t]
        if c["rows"] != c["study_input_unavailable_N"] + c["study_tie_N"] + full_record["N"]:
            raise ValueError("Study eligibility accounting does not conserve checkpoint rows")
        coverage_records.append({
            "t_sec": t, "rows": c["rows"], "study_input_unavailable_N": c["study_input_unavailable_N"],
            "study_tie_N": c["study_tie_N"],
            **{f"full_{key}": value for key, value in full_record.items()},
            **{f"matched_{key}": value for key, value in total.items()},
            **{f"excluded_{key}": value for key, value in excluded_record.items()},
            **{f"{reason}_N": reason_counts[(t, reason)].N for reason in DIAGNOSTIC_REASONS},
        })
        status_records.extend({"t_sec": t, "status": status, **_counts_record(status_counts[(t, status)])}
                              for status in STATUSES)
        reason_records.extend({"t_sec": t, "reason": reason, **_counts_record(reason_counts[(t, reason)])}
                              for reason in REASONS)
    screens = [_screen_counts(records, grouping, basis)
               for grouping, records in (("checkpoint_bands", checkpoint_records), ("cell_bands", cell_records))
               for basis in ("n", "N")]
    return {
        "metadata": {**metadata, "quote_extraction_ms": quote_metadata["quote_extraction_ms"],
                     "quote_max_age_ms": QUOTE_MAX_AGE_MS, "decimal_precision": PRECISION,
                     "wilson_z": str(tabulate.Z), "ask_bands": ASK_BANDS,
                     "interpretation": INTERPRETATION, "screen_interpretation": SCREEN_INTERPRETATION,
                     "reason_interpretation": "Reasons overlap; statuses are disjoint. Source-old means age >3000 ms, never future-dated. No checkpoints are pooled as independent markets."},
        "checkpoint_totals": total_records, "checkpoint_bands": checkpoint_records,
        "cell_bands": cell_records, "coverage": coverage_records, "statuses": status_records,
        "reasons": reason_records, "screens": screens,
    }


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _read_csv(path: Path) -> tuple[list[dict[str, str]], str]:
    content = path.read_bytes()
    return list(csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""))), _sha256_bytes(content)


def _validate_price_manifest(record: dict, metadata: dict, observations_hash: str, quotes_hash: str) -> None:
    for name, expected in (("observations_sha256", observations_hash), ("quotes_sha256", quotes_hash),
                           ("market_prices_py_sha256", _sha256(Path(market_prices.__file__))),
                           ("tabulate_helper_sha256", _sha256(Path(tabulate.__file__)))):
        if record.get(name) != expected:
            raise ValueError(f"Price manifest hash mismatch: {name}")
    for name in ("requested_cohort_start_ms", "cohort_start_ms", "cohort_end_ms", "cohort_market_count",
                 "outcome_cutoff_ms", "quote_extraction_ms", "quote_max_age_ms", "rows", "markets"):
        if record.get(name) != metadata[name]:
            raise ValueError(f"Price manifest metadata mismatch: {name}")
    market_prices.validate_extraction(record.get("quote_extraction_process", {}), metadata,
                                     Path(__file__).with_name("extract_quotes.sql"))


def run_audit(*, observations: Path, quotes: Path, price_manifest: Path, output: Path) -> dict:
    observations, quotes, price_manifest, output = map(Path, (observations, quotes, price_manifest, output))
    if output.exists():
        raise ValueError("Output directory already exists; choose a new directory")
    observation_rows, observations_hash = _read_csv(observations)
    quote_rows, quotes_hash = _read_csv(quotes)
    price_manifest_bytes = price_manifest.read_bytes()
    prior = json.loads(price_manifest_bytes.decode("utf-8-sig"), parse_float=Decimal)
    result = build_audit(observation_rows, quote_rows)
    _validate_price_manifest(prior, result["metadata"], observations_hash, quotes_hash)
    output.mkdir(parents=True, exist_ok=False)
    artifacts = []
    for name in ("checkpoint_totals", "checkpoint_bands", "cell_bands", "coverage", "statuses", "reasons", "screens"):
        filename = f"{name}.csv"
        market_prices._write_csv(output / filename, result[name])
        artifacts.append(filename)
    with (output / "audit.json").open("x", encoding="utf-8") as handle:
        json.dump(market_prices._jsonable(result), handle, indent=2)
        handle.write("\n")
    artifacts.append("audit.json")
    manifest = {
        "study": "Frozen H3 realized gross quote-return and exclusion audit",
        **result["metadata"], "outcome_cutoff_utc": utc_text(result["metadata"]["outcome_cutoff_ms"]),
        "quote_extraction_utc": utc_text(result["metadata"]["quote_extraction_ms"]),
        "observations_path": str(observations.resolve()), "observations_sha256": observations_hash,
        "quotes_path": str(quotes.resolve()), "quotes_sha256": quotes_hash,
        "price_manifest_path": str(price_manifest.resolve()),
        "price_manifest_sha256": _sha256_bytes(price_manifest_bytes),
        "quote_return_audit_py_sha256": _sha256(Path(__file__)),
        "market_prices_py_sha256": _sha256(Path(market_prices.__file__)),
        "tabulate_helper_sha256": _sha256(Path(tabulate.__file__)),
        "extract_quotes_sql_sha256": _sha256(Path(__file__).with_name("extract_quotes.sql")),
        "quote_extraction_process": prior["quote_extraction_process"],
        "verification": {"canonical_input_and_helper_hashes_match": True,
                         "frozen_cohort_and_checkpoint_keys_match": True,
                         "resolved_prices_and_payouts_use_identical_observations": True,
                         "full_matched_excluded_status_and_band_counts_conserve": True},
        "output_row_counts": {name: len(result[name]) for name in result if name != "metadata"},
        "artifacts_sha256": {name: _sha256(output / name) for name in artifacts},
    }
    with (output / "manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(market_prices._jsonable(manifest), handle, indent=2)
        handle.write("\n")
    return {**result, "manifest": manifest}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--quotes", type=Path, required=True)
    parser.add_argument("--price-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_audit(observations=args.observations, quotes=args.quotes,
                       price_manifest=args.price_manifest, output=args.output)
    print(f"Wrote {len(result['checkpoint_bands'])} checkpoint/ask bands and "
          f"{len(result['cell_bands'])} cell/ask bands to {args.output}")


if __name__ == "__main__":
    main()
