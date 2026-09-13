"""Fixed-week receipt-clock pilot; causal retained-history estimates only."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

START_MS = 1788220800000
END_MS = 1788825600000
CHECKPOINTS = (60, 30, 15, 10, 5, 3)
LOOKBACK_MS = 600000
PRECISION = 80


def decimal_price(value) -> Decimal:
    result = Decimal(value)
    if not result.is_finite() or result <= 0:
        raise ValueError("Require positive finite Decimal prices")
    return result


def select_current(events, D, kind="spot"):
    def receipt(event):
        return int(event["received_ms"]) * 1000000 if kind == "spot" else int(event["received_wall_ns"])
    if kind not in ("spot", "twap"):
        raise ValueError("Unknown feed")
    eligible = [event for event in events if (D - LOOKBACK_MS) * 1000000 < receipt(event) <= D * 1000000]
    if not eligible:
        return {"price": None, "source_ms": None, "received_ns": None, "variants": 0,
                "tied_rows": 0, "source_age_ms": None, "receipt_age_ns": None,
                "available": False, "fresh_3s": False, "reasons": ("missing_current",)}
    latest = max(receipt(event) for event in eligible)
    latest_events = [event for event in eligible if receipt(event) == latest]
    variants = {(int(event["source_ms"]), decimal_price(event["price"])) for event in latest_events}
    source, price = next(iter(variants)) if len(variants) == 1 else (None, None)
    source_age = D - source if source is not None else None
    receipt_age = D * 1000000 - latest
    reasons = []
    if len(variants) != 1:
        reasons.append("receipt_ambiguity")
    elif source_age < 0:
        reasons.append("future_source")
    elif source_age >= LOOKBACK_MS:
        reasons.append("expired_source")
    available = not reasons
    return {"price": price, "source_ms": source, "received_ns": latest, "variants": len(variants),
            "tied_rows": len(latest_events), "source_age_ms": source_age, "receipt_age_ns": receipt_age,
            "available": available, "fresh_3s": available and source_age <= 3000 and receipt_age <= 3000000000,
            "reasons": tuple(reasons)}


def opening_reference(events, start, D):
    eligible = [event for event in events if int(event["source_ms"]) == start
                and int(event["received_wall_ns"]) <= D * 1000000]
    values = {decimal_price(event["price"]) for event in eligible}
    return {"price": next(iter(values)) if len(values) == 1 else None, "variants": len(values),
            "event_count": len(eligible),
            "first_received_ns": min((int(event["received_wall_ns"]) for event in eligible), default=None),
            "available": len(values) == 1}


def project_slots(events, E, D, current):
    visible = [event for event in events if int(event["received_ms"]) <= D]
    history_sum = Decimal(0)
    exact = carried = missing = future = history = 0
    carry_ages, receive_ages = [], []
    with localcontext() as context:
        context.prec = PRECISION
        for j in range(60):
            slot = E - 62000 + j * 1000
            if slot > D:
                future += 1
                continue
            history += 1
            candidates = [event for event in visible if slot - LOOKBACK_MS < int(event["source_ms"]) <= slot]
            if not candidates:
                missing += 1
                continue
            selected = max(candidates, key=lambda event: (int(event["source_ms"]), int(event["received_ms"])))
            age = slot - int(selected["source_ms"])
            history_sum += decimal_price(selected["price"])
            exact += age == 0
            carried += age > 0
            carry_ages.append(age)
            receive_ages.append(D - int(selected["received_ms"]))
        future_filled = future if current["available"] else 0
        future_missing = future - future_filled
        available = not missing and not future_missing
        projection = ((history_sum + future * current["price"]) / 60
                      if available and future else history_sum / 60 if available else None)
    return {"projected_price": projection, "projection_available": available, "history_sum": history_sum,
            "historical_requested_slots": history, "exact_observed_slots": exact, "carried_slots": carried,
            "historical_missing_slots": missing, "future_requested_slots": future,
            "future_extrapolated_slots": future_filled, "future_missing_slots": future_missing,
            "max_carry_age_ms": max(carry_ages, default=None),
            "max_history_receive_age_ms": max(receive_ages, default=None)}


def _int(value):
    return None if value in (None, "") else int(value)


def _decimal(value):
    return None if value in (None, "") else decimal_price(value)


def assess_row(raw):
    row = dict(raw)
    for name in ("market_id", "start_ms", "end_ms", "t_sec", "cut_ms", "snapshot_ms", "k_variants",
                 "k_event_count", "k_first_received_ns", "s_variants", "s_tied_rows", "s_source_ms",
                 "s_received_ms", "s_source_key_errors", "w_variants", "w_tied_rows", "w_source_ms",
                 "w_received_ns", "history_requested_slots", "history_exact_slots", "history_carried_slots",
                 "history_missing_slots", "history_source_key_errors", "history_max_carry_age_ms",
                 "history_max_receive_age_ms", "history_min_source_ms", "history_max_source_ms",
                 "history_latest_received_ms"):
        row[name] = _int(raw.get(name))
    D, E = row["cut_ms"], row["end_ms"]
    variants, k_value, k_receipt = row["k_variants"], _decimal(raw.get("k_value")), row["k_first_received_ns"]
    if (variants is None or variants < 0 or row["k_event_count"] is None or row["k_event_count"] < variants
            or (variants == 0) != (k_value is None) or (variants == 0) != (k_receipt is None)
            or (k_receipt is not None and k_receipt > D * 1000000)):
        raise ValueError("Opening reference provenance mismatch")
    requested, missing = row["history_requested_slots"], row["history_missing_slots"]
    if requested is None or missing is None or not 0 <= missing <= requested:
        raise ValueError("Historical count provenance mismatch")
    present = requested - missing
    required_history = (row["history_min_source_ms"], row["history_max_source_ms"],
                        row["history_latest_received_ms"], row["history_max_carry_age_ms"],
                        row["history_max_receive_age_ms"])
    history_sum = Decimal(raw["history_sum"] or "0")
    if (not history_sum.is_finite() or history_sum < 0
            or (present == 0 and (history_sum != 0 or any(value is not None for value in required_history)))
            or (present > 0 and (history_sum <= 0 or any(value is None for value in required_history)))):
        raise ValueError("Historical value/clock provenance mismatch")
    if present and (row["history_latest_received_ms"] > D
                    or not E - 62000 - LOOKBACK_MS < row["history_min_source_ms"] <= row["history_max_source_ms"] <= min(D, E - 3000)
                    or not 0 <= row["history_max_carry_age_ms"] < LOOKBACK_MS
                    or row["history_max_receive_age_ms"] < 0):
        raise ValueError("Historical source/receipt eligibility mismatch")
    row["rule_valid"] = raw["rule_valid"] == "t"
    row["k"] = _decimal(raw.get("k_value")) if row["k_variants"] == 1 else None
    row["k_available"] = row["rule_valid"] and row["k"] is not None
    for feed in ("s", "w"):
        source = row[f"{feed}_source_ms"]
        received_ns = row["s_received_ms"] * 1000000 if feed == "s" and row["s_received_ms"] is not None else row.get("w_received_ns") if feed == "w" else None
        source_age = D - source if source is not None else None
        receipt_age = D * 1000000 - received_ns if received_ns is not None else None
        key_errors = row["s_source_key_errors"] if feed == "s" else 0
        row[feed] = _decimal(raw.get(f"{feed}_value")) if row[f"{feed}_variants"] == 1 else None
        row[f"{feed}_source_age_ms"] = source_age
        row[f"{feed}_receipt_age_ns"] = receipt_age
        row[f"{feed}_future_source"] = source_age is not None and source_age < 0
        row[f"{feed}_expired_source"] = source_age is not None and source_age >= LOOKBACK_MS
        row[f"{feed}_receipt_ambiguity"] = row[f"{feed}_variants"] > 1
        row[f"{feed}_available"] = (row[feed] is not None and source_age is not None and 0 <= source_age < LOOKBACK_MS
                                    and receipt_age is not None and 0 <= receipt_age < LOOKBACK_MS * 1000000 and not key_errors)
        row[f"{feed}_fresh_3s"] = (row[f"{feed}_available"] and source_age <= 3000 and receipt_age <= 3000000000)
    future = 60 - row["history_requested_slots"]
    row["historical_requested_slots"] = row["history_requested_slots"]
    row["exact_observed_slots"] = row["history_exact_slots"]
    row["carried_slots"] = row["history_carried_slots"]
    row["historical_missing_slots"] = row["history_missing_slots"]
    row["future_requested_slots"] = future
    row["future_extrapolated_slots"] = future if row["s_available"] else 0
    row["future_missing_slots"] = future - row["future_extrapolated_slots"]
    row["projection_available"] = (row["history_missing_slots"] == 0 and not row["history_source_key_errors"]
                                    and (future == 0 or row["s_available"]))
    with localcontext() as context:
        context.prec = PRECISION
        row["history_sum"] = Decimal(raw["history_sum"] or "0")
        row["projected_price"] = ((row["history_sum"] + (future * row["s"] if future else 0)) / 60
                                  if row["projection_available"] else None)
        official_k = _decimal(raw.get("official_k_audit_only"))
        row["official_k_minus_stream_k"] = official_k - row["k"] if official_k is not None and row["k"] is not None else None
    row["primary_paired_available"] = (row["rule_valid"] and row["k_available"] and row["s_available"]
                                       and row["w_available"] and row["projection_available"])
    row["fresh_3s_paired_available"] = (row["primary_paired_available"] and row["s_fresh_3s"] and row["w_fresh_3s"])
    winner = raw.get("official_winner", "")
    if winner not in ("", "Up", "Down"):
        raise ValueError("Invalid official winner")
    row["official_winner"] = winner
    row["official_outcome_available"] = bool(winner)
    final = _decimal(raw.get("official_final_audit_only"))
    row["official_price_rule_winner_audit"] = ("Up" if final >= official_k else "Down") if final is not None and official_k is not None else ""
    row["official_price_rule_winner_mismatch"] = bool(winner and row["official_price_rule_winner_audit"] and winner != row["official_price_rule_winner_audit"])
    for label, value, available in (("projected", row["projected_price"], row["projection_available"]),
                                    ("spot", row["s"], row["s_available"]), ("twap", row["w"], row["w_available"])):
        usable = row["k_available"] and available
        row[f"{label}_tie"] = bool(usable and value == row["k"])
        row[f"{label}_prediction"] = ("Up" if value >= row["k"] else "Down") if usable else ""
        row[f"{label}_correct"] = row[f"{label}_prediction"] == winner if usable and winner else None
        with localcontext() as context:
            context.prec = PRECISION
            row[f"{label}_absolute_final_error_bps_audit"] = 10000 * abs(value - final) / final if usable and final is not None else None
    expected_history = sum(E - 62000 + j * 1000 <= D for j in range(60))
    if (row["history_requested_slots"] != expected_history
            or row["history_exact_slots"] + row["history_carried_slots"] + row["history_missing_slots"] != expected_history):
        raise ValueError("Slot accounting mismatch")
    return row


def jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def summarize_rows(rows):
    summaries = []
    for t in CHECKPOINTS:
        all_t = [row for row in rows if row["t_sec"] == t]
        if not all_t:
            continue
        for subset, flag in (("primary_600s", "primary_paired_available"), ("fresh_3s", "fresh_3s_paired_available")):
            paired = [row for row in all_t if row[flag]]
            resolved = [row for row in paired if row["official_winner"]]
            result = {"t_sec": t, "subset": subset, "all_market_rows": len(all_t), "N": len(paired),
                      "U": len(paired) - len(resolved), "n": len(resolved)}
            with localcontext() as context:
                context.prec = PRECISION
                for label in ("projected", "twap", "spot"):
                    correct = sum(row[f"{label}_correct"] for row in resolved)
                    result[f"{label}_correct"] = correct
                    result[f"{label}_errors"] = len(resolved) - correct
                    result[f"{label}_accuracy"] = Decimal(correct) / len(resolved) if resolved else None
                    result[f"{label}_ties"] = sum(row[f"{label}_tie"] for row in paired)
                result["both_correct"] = sum(row["projected_correct"] and row["twap_correct"] for row in resolved)
                result["both_wrong"] = sum(not row["projected_correct"] and not row["twap_correct"] for row in resolved)
                result["projected_improves_twap_count"] = sum(row["projected_correct"] and not row["twap_correct"] for row in resolved)
                result["projected_worsens_twap_count"] = sum(not row["projected_correct"] and row["twap_correct"] for row in resolved)
            summaries.append(result)
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observations", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    extraction_path = args.observations.with_name("extraction.json")
    extraction = json.loads(extraction_path.read_text(encoding="utf-8"))
    if (extraction.get("status") != "accepted" or type(extraction.get("psql_exit_code")) is not int
            or extraction["psql_exit_code"] != 0 or extraction.get("guards_passed") is not True
            or extraction.get("read_only") is not True or extraction.get("isolation") != "repeatable read"
            or extraction.get("observations_sha256") != sha256(args.observations)
            or extraction.get("extract_sql_sha256") != sha256(root / "extract.sql")
            or extraction.get("guard_sql_sha256") != sha256(root / "guard.sql")):
        raise ValueError("Require matching successful guarded extraction")
    with args.observations.open(encoding="utf-8", newline="") as handle:
        rows = [assess_row(row) for row in csv.DictReader(handle)]
    expected = {(start // 300000, t) for start in range(START_MS, END_MS, 300000) for t in CHECKPOINTS}
    if len(rows) != len(expected) or {(row["market_id"], row["t_sec"]) for row in rows} != expected:
        raise ValueError("Require all 2016 markets and six unique checkpoints")
    if len({row["snapshot_ms"] for row in rows}) != 1:
        raise ValueError("Require one repeatable-read snapshot")
    for row in rows:
        if row["start_ms"] != row["market_id"] * 300000 or row["end_ms"] != row["start_ms"] + 300000 or row["cut_ms"] != row["end_ms"] - row["t_sec"] * 1000:
            raise ValueError("Market/checkpoint boundary mismatch")
    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / "rows.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summaries = summarize_rows(rows)
    with (args.output / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    daily = [{"utc_market_start_date": f"2026-09-{day + 1:02d}", **summary}
             for day in range(7)
             for summary in summarize_rows([row for row in rows if (row["start_ms"] - START_MS) // 86400000 == day])]
    with (args.output / "daily_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(daily[0]))
        writer.writeheader()
        writer.writerows(daily)
    manifest = {"cohort_start_ms": START_MS, "cohort_end_ms_exclusive": END_MS, "markets": 2016,
                "checkpoint_seconds": CHECKPOINTS, "rows": len(rows), "snapshot_ms": rows[0]["snapshot_ms"],
                "shift_seconds_a": -3, "final_slots": "[E-62s,E-3s] inclusive, 60 exact source slots",
                "historical_rule": "greatest retained source<=slot, received<=D, source>slot-600s; carried values are estimates",
                "current_rule": "greatest receipt in(D-600s,D] before source check; source age in[0,600s); receipt ties with different source/value unavailable",
                "opening_rule": "one distinct exact-start stream E18 value received byD; official opening audit-only",
                "freshness_diagnostic": "both selected current spot/TWAP source and receipt ages <=3000ms inclusive",
                "decimal_precision": PRECISION, "source_sha256": sha256(args.observations),
                "code_sha256": sha256(__file__), "extract_sql_sha256": sha256(root / "extract.sql"),
                "guard_sql_sha256": sha256(root / "guard.sql"),
                "extraction_record_sha256": sha256(extraction_path), "extraction": extraction,
                "status": "accepted", "summaries": summaries,
                "artifacts_sha256": {name: sha256(args.output / name) for name in ("rows.csv", "summary.csv", "daily_summary.csv")},
                "limitations": ["Retained spot upserts may erase values actually available at an earlier decision; receipt filtering cannot reconstruct them",
                                "Historical carry and flat future slots are model estimates, not observed unchanged values",
                                "Comparisons are exploratory on a previously examined fixed week; repeated checkpoints are not independent markets"]}
    (args.output / "manifest.json").write_text(json.dumps(jsonable(manifest), indent=2) + "\n", encoding="utf-8")
    print(json.dumps(jsonable(summaries), indent=2))


if __name__ == "__main__":
    main()
