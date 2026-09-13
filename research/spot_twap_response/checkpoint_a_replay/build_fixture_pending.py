"""Freeze contract-v2 pending labels against the immutable contract-v1 fixture.

Local files only. This module does not import the production ghost engine.
The synthetic replay clock is deliberately not represented as measured timing.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter
import csv
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
import gzip
import hashlib
import io
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE_DIR = ROOT / "research/spot_twap_response/pilot_review"
BASELINE = ROOT / "tests/fixtures/ghost_twap"
OUTPUT = BASELINE / "pending_v2"
START_MS = 1789088400000
END_MS = 1789092000000
CONTEXT_START_MS = START_MS - 120_000
CONTEXT_END_MS = END_MS + 120_000
HORIZONS = (1, 2, 3, 5, 10, 30)
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000
PRICE_QUANTUM = Decimal("0.000000000000000001")
ORIGINAL_FIELDS = ("kind", "source_ms", "sample_second_ms", "price", "price_e18",
                   "received_ms", "received_wall_ns")
EVENT_FIELDS = (*ORIGINAL_FIELDS, "source_export_row", "event_id", "replay_order",
                "replay_received_wall_ns", "replay_monotonic_ns", "window_s")
DECISION_FIELDS = ("decision_id", "decision_wall_ns", "decision_monotonic_ns")
EXPECTED_FIELDS = ("decision_id", "decision_wall_ns", "horizon_s",
                   "current_spot_event_id", "current_twap_event_id", "included_sequence",
                   "target_source_timestamp_ms", "price", "quality", "reasons",
                   "exact", "carried", "pending", "future", "missing",
                   "estimated_arrival_wall_ns", "estimated_remaining_ns", "estimate_overdue")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare_baseline(expected: list[dict]) -> dict:
    """Reject changes to every frozen field except the declared label split."""
    baseline_manifest = json.loads((BASELINE / "manifest.json").read_text(encoding="utf-8"))
    original_generator = ROOT / baseline_manifest["generator_path"]
    if sha256(original_generator) != baseline_manifest["generator_sha256"]:
        raise ValueError("original generator changed")
    for name, digest in baseline_manifest["artifacts_sha256"].items():
        if sha256(BASELINE / name) != digest:
            raise ValueError(f"original artifact changed: {name}")
    with gzip.open(BASELINE / "recorded_hour_expected.csv.gz", "rt", encoding="utf-8", newline="") as stream:
        old_rows = list(csv.DictReader(stream))
    if len(old_rows) != len(expected):
        raise ValueError("forecast domain changed")
    tables = {h: dict(horizon_s=h, old_healthy=0, old_degraded=0, old_unavailable=0,
                      new_healthy=0, new_degraded=0, new_unavailable=0,
                      degraded_to_healthy=0, forecasts_with_pending=0,
                      available_forecasts_with_pending=0, total_pending_slots=0)
              for h in HORIZONS}
    checked_columns = [field for field in old_rows[0] if field not in ("quality", "carried")]
    changed_prices = 0
    for old, new in zip(old_rows, expected):
        clue = old["decision_id"], old["horizon_s"]
        for field in checked_columns:
            value = "" if new[field] is None else str(new[field])
            if old[field] != value:
                raise ValueError(f"frozen field changed at {clue}: {field}")
        changed_prices += old["price"] != new["price"]
        if int(old["carried"]) != new["carried"] + new["pending"]:
            raise ValueError(f"carry partition changed at {clue}")
        if sum(new[key] for key in ("exact", "carried", "pending", "future", "missing")) != 60:
            raise ValueError(f"slot count changed at {clue}")
        if (old["quality"] == "unavailable") != (new["quality"] == "unavailable"):
            raise ValueError(f"eligibility changed at {clue}")
        if old["quality"] != new["quality"] and (old["quality"], new["quality"]) != ("degraded", "healthy"):
            raise ValueError(f"unexpected quality transition at {clue}")
        table = tables[new["horizon_s"]]
        table["old_" + old["quality"]] += 1
        table["new_" + new["quality"]] += 1
        table["degraded_to_healthy"] += old["quality"] == "degraded" and new["quality"] == "healthy"
        table["forecasts_with_pending"] += new["pending"] > 0
        table["available_forecasts_with_pending"] += new["pending"] > 0 and new["quality"] != "unavailable"
        table["total_pending_slots"] += new["pending"]
    return dict(status="passed", forecasts_checked=len(expected), changed_prices=changed_prices,
                unchanged_baseline_columns=checked_columns,
                old_carried_equals_new_carried_plus_pending=True,
                five_categories_sum_to_sixty=True,
                eligibility_and_reason_codes_unchanged=True,
                current_event_ids_and_included_sequence_unchanged=True,
                target_stamps_and_signed_eta_unchanged=True,
                labels_by_horizon=[tables[h] for h in HORIZONS],
                baseline_manifest_sha256=sha256(BASELINE / "manifest.json"),
                baseline_generator_sha256=sha256(original_generator),
                baseline_expected_sha256=sha256(BASELINE / "recorded_hour_expected.csv.gz"))


def write_gzip_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    # Empty gzip filename and fixed mtime make rebuilding byte-for-byte repeatable.
    with path.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)


def load_events() -> tuple[list[dict], dict]:
    source = SOURCE_DIR / "sources.csv"
    record_path = SOURCE_DIR / "extraction.json"
    extraction = json.loads(record_path.read_text(encoding="utf-8"))
    if (extraction["psql_exit_code"] != 0 or extraction["read_only"] is not True
            or extraction["isolation"] != "repeatable read"):
        raise ValueError("original source extraction was not accepted read-only evidence")
    if sha256(source) != extraction["sources_sha256"]:
        raise ValueError("source export hash does not match its original extraction record")
    if sha256(SOURCE_DIR / "extract_sources.sql") != extraction["sql_sha256"]:
        raise ValueError("original extraction SQL hash mismatch")
    events = []
    with source.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != ORIGINAL_FIELDS:
            raise ValueError("unexpected source fields")
        for ordinal, original in enumerate(reader, 1):
            if original["kind"] not in ("spot", "twap"):
                raise ValueError("unexpected source kind")
            receipt_ns = (int(original["received_ms"]) * NS_PER_MS
                          if original["kind"] == "spot" else int(original["received_wall_ns"]))
            if not CONTEXT_START_MS * NS_PER_MS <= receipt_ns < CONTEXT_END_MS * NS_PER_MS:
                continue
            source_ms = int(original["source_ms"])
            if source_ms % 1000 or source_ms != int(original["sample_second_ms"]):
                raise ValueError("fixture requires the recorded whole-second source grid")
            value = Decimal(original["price"])
            with localcontext() as context:
                context.prec = 80
                if (not value.is_finite() or value <= 0
                        or value != value.quantize(PRICE_QUANTUM)):
                    raise ValueError("invalid exact retained price")
                if original["kind"] == "twap" and value * Decimal(10**18) != Decimal(original["price_e18"]):
                    raise ValueError("TWAP price and E18 disagree")
            events.append(dict(original, source_export_row=ordinal,
                               event_id=f"retained:{original['kind']}:{ordinal}",
                               replay_received_wall_ns=receipt_ns,
                               window_s=60 if original["kind"] == "twap" else ""))
    events.sort(key=lambda item: (item["replay_received_wall_ns"], item["source_export_row"]))
    if not events:
        raise ValueError("empty context")
    origin_ns = events[0]["replay_received_wall_ns"]
    high_water: dict[str, int] = {}
    source_values: dict[tuple[str, int], str] = {}
    audit: Counter = Counter()
    for order, event in enumerate(events, 1):
        event["replay_order"] = order
        event["replay_monotonic_ns"] = event["replay_received_wall_ns"] - origin_ns
        feed, stamp = event["kind"], int(event["source_ms"])
        audit[f"{feed}_events"] += 1
        audit[f"{feed}_future_at_receipt"] += stamp * NS_PER_MS > event["replay_received_wall_ns"]
        audit[f"{feed}_source_regressions"] += stamp < high_water.get(feed, stamp)
        high_water[feed] = max(stamp, high_water.get(feed, stamp))
        key = feed, stamp
        if key in source_values:
            audit[f"{feed}_duplicate_source_stamps"] += 1
            audit[f"{feed}_conflicting_source_prices"] += Decimal(source_values[key]) != Decimal(event["price"])
        source_values[key] = event["price"]
    audit["tied_normalized_receipts"] = len(events) - len({e["replay_received_wall_ns"] for e in events})
    # This recorded fixture has none of these cases; adversarial engine tests own
    # the separate regression/conflict latch behavior. Never silently ignore one.
    if audit["twap_source_regressions"] or audit["twap_conflicting_source_prices"]:
        raise ValueError("recorded fixture needs an explicit oracle for TWAP regression/conflict recovery")
    return events, {"source_sha256": sha256(source), "extraction_sha256": sha256(record_path),
                    "extract_sql_sha256": extraction["sql_sha256"], "audit": dict(audit),
                    "synthetic_monotonic_origin_wall_ns": origin_ns,
                    "first_event_receipt_ns": origin_ns,
                    "last_event_receipt_ns": events[-1]["replay_received_wall_ns"]}


def current_issues(event: dict | None, feed: str, decision_ns: int) -> list[str]:
    if event is None:
        return [f"missing_{feed}"]
    source_ns = int(event["source_ms"]) * NS_PER_MS
    receipt_ns = event["replay_received_wall_ns"]
    # Inputs stamped after their own receipt remain invalid, even later in replay.
    if source_ns > receipt_ns or min(decision_ns - source_ns, decision_ns - receipt_ns) < 0:
        return [f"future_{feed}"]
    if max(decision_ns - source_ns, decision_ns - receipt_ns) > 3000 * NS_PER_MS:
        return [f"stale_{feed}"]
    return []


def reference_rows(events: list[dict], decisions: list[dict]) -> tuple[list[dict], dict]:
    """Evaluate each 60-slot window directly from the retained causal prefix.

    No sliding mean or engine import is used. Source keys select historical
    constituents; receipt ordering selects revisions and the two current values.
    """
    latest: dict[str, dict] = {}
    revisions: dict[tuple[str, int], dict] = {}
    next_event = 0
    output: list[dict] = []
    summary: Counter = Counter()
    maximum_history = 0
    for decision in decisions:
        decision_ns = decision["decision_wall_ns"]
        decision_ms = decision_ns // NS_PER_MS
        while next_event < len(events) and events[next_event]["replay_received_wall_ns"] <= decision_ns:
            event = events[next_event]
            latest[event["kind"]] = event
            revisions[event["kind"], int(event["source_ms"])] = event
            next_event += 1
        # At a given cutoff the relevant bounded source history is the last120s
        # plus the newest preceding spot seed within the10s carry allowance.
        cutoff = decision_ms - 120_000
        seeds = [stamp for feed, stamp in revisions if feed == "spot" and cutoff - 10_000 <= stamp < cutoff]
        seed = max(seeds, default=None)
        history = {key: event for key, event in revisions.items()
                   if key[1] >= cutoff or key == ("spot", seed)}
        maximum_history = max(maximum_history, len(history))
        if len(history) > 1024:
            raise ValueError("recorded prefix exceeds the declared engine capacity")
        valid_spots = {stamp: event for (feed, stamp), event in history.items()
                       if feed == "spot" and stamp * NS_PER_MS <= event["replay_received_wall_ns"]}
        source_keys = sorted(valid_spots)
        frontier = source_keys[-1] if source_keys else None
        spot, twap = latest.get("spot"), latest.get("twap")
        spot_issues = current_issues(spot, "spot", decision_ns)
        common_issues = spot_issues + current_issues(twap, "twap", decision_ns)
        for horizon in HORIZONS:
            target = None if twap is None else int(twap["source_ms"]) + horizon * 1000
            counts = Counter(exact=0, carried=0, pending=0, future=0, missing=0)
            values: list[Decimal] = []
            if target is None:
                counts["missing"] = 60
            else:
                # Inclusive endpoints U-62s and U-3s: exactly60 one-second slots.
                for slot_ms in range(target - 62_000, target - 2000, 1000):
                    if slot_ms * NS_PER_MS > decision_ns:
                        if spot_issues:
                            counts["missing"] += 1
                        else:
                            counts["future"] += 1
                            values.append(Decimal(spot["price"]))
                        continue
                    index = bisect_right(source_keys, slot_ms) - 1
                    stamp = source_keys[index] if index >= 0 else None
                    if stamp is None or slot_ms - stamp > 10_000:
                        counts["missing"] += 1
                    else:
                        if stamp == slot_ms:
                            category = "exact"
                        elif frontier is not None and slot_ms > frontier:
                            category = "pending"
                        else:
                            category = "carried"
                        counts[category] += 1
                        values.append(Decimal(valid_spots[stamp]["price"]))
            issues = list(common_issues)
            if counts["missing"]:
                issues.append("missing_slots")
            if target is not None and ("twap", target) in history:
                issues.append("target_already_received")
            assert sum(counts.values()) == 60
            price = ""
            if not issues:
                assert len(values) == 60
                with localcontext() as context:
                    context.prec = 80
                    context.rounding = ROUND_HALF_EVEN
                    price = format((sum(values, Decimal(0)) / Decimal(60)).quantize(PRICE_QUANTUM), ".18f")
            quality = "unavailable" if issues else "degraded" if counts["carried"] else "healthy"
            eta = None if twap is None else twap["replay_received_wall_ns"] + horizon * NS_PER_SECOND
            remaining = None if eta is None else eta - decision_ns
            row = dict(decision_id=decision["decision_id"], decision_wall_ns=decision_ns,
                       horizon_s=horizon, current_spot_event_id="" if spot is None else spot["event_id"],
                       current_twap_event_id="" if twap is None else twap["event_id"],
                       included_sequence=next_event, target_source_timestamp_ms=target,
                       price=price, quality=quality, reasons="|".join(issues), **counts,
                       estimated_arrival_wall_ns=eta, estimated_remaining_ns=remaining,
                       estimate_overdue=remaining is not None and remaining < 0)
            output.append(row)
            summary[f"h{horizon}_{quality}"] += 1
            for reason in issues:
                summary[f"h{horizon}_reason_{reason}"] += 1
    return output, {"quality_and_reason_counts": dict(summary),
                    "maximum_retained_source_history_at_decisions": maximum_history}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--review-output", type=Path, default=Path(__file__).with_name("pending_review.json"))
    args = parser.parse_args()
    output_dir = args.output.resolve()
    review_path = args.review_output.resolve()
    names = ("recorded_hour_events.csv.gz", "recorded_hour_decisions.csv.gz",
             "recorded_hour_expected.csv.gz", "manifest.json")
    if review_path.exists() or any((output_dir / name).exists() for name in names):
        raise FileExistsError("fixture output already exists; use a new directory")
    events, provenance = load_events()
    origin = provenance["synthetic_monotonic_origin_wall_ns"]
    decisions = [dict(decision_id=f"retained-hour:{stamp}", decision_wall_ns=stamp * NS_PER_MS,
                      decision_monotonic_ns=stamp * NS_PER_MS - origin)
                 for stamp in range(START_MS, END_MS, 1000)]
    expected, evaluation = reference_rows(events, decisions)
    assert len(decisions) == 3600 and len(expected) == 21600
    comparison = compare_baseline(expected)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_gzip_csv(output_dir / names[0], EVENT_FIELDS, events)
    write_gzip_csv(output_dir / names[1], DECISION_FIELDS, decisions)
    write_gzip_csv(output_dir / names[2], EXPECTED_FIELDS, expected)
    for name in names[:2]:
        if (output_dir / name).read_bytes() != (BASELINE / name).read_bytes():
            raise ValueError(f"source or decision bytes changed: {name}")
    manifest = dict(
        status="accepted", schema_version=2, contract_version=2,
        selection="First UTC-aligned hour with at least 120s of both-feed export coverage before and after; no quality/error selection",
        decision_start_utc="2026-09-11T01:00:00Z", decision_end_utc_exclusive="2026-09-11T02:00:00Z",
        decision_start_ms=START_MS, decision_end_ms_exclusive=END_MS,
        receipt_context_start_ms=CONTEXT_START_MS, receipt_context_end_ms_exclusive=CONTEXT_END_MS,
        context_selection="Normalized recorded receipt in [00:58,02:02); no source-time prefilter",
        decisions=len(decisions), event_rows=len(events), expected_rows=len(expected), horizons_s=list(HORIZONS),
        policy=dict(enabled=True, current_max_age_ms=3000, max_carry_ms=10_000, history_ms=120_000, max_events=1024),
        numeric=dict(context_precision=80, rounding="ROUND_HALF_EVEN", price_decimal_places=18),
        expected_mean_window="target_source_timestamp_ms-62000 through target_source_timestamp_ms-3000 inclusive",
        generator_path=str(Path(__file__).relative_to(ROOT)).replace("\\", "/"),
        generator_sha256=sha256(Path(__file__)),
        source_path="research/spot_twap_response/pilot_review/sources.csv",
        **provenance, **evaluation, baseline_comparison=comparison,
        pending_definition="Usable nonexact historical slot beyond the maximum admissible source timestamp in the received causal spot prefix",
        carried_definition="Usable nonexact historical slot below that maximum; this identifies an interior absent source stamp, not a proven silent feed",
        quality_definition="Unavailable for unchanged reason codes; otherwise degraded for interior carry only; pending and future assumptions are shown separately",
        artifacts_sha256={name: sha256(output_dir / name) for name in names[:3]},
        limitations=[
            "Spot rows are retained same-second upserts, not all accepted arrivals; overwritten earlier values cannot be recovered.",
            "TWAP rows came from durable events, but this export omits session, event identity, accepted sequence, monotonic clocks and gap records.",
            "Spot receipt milliseconds are multiplied by 1000000; this does not create submillisecond measurement precision.",
            "Replay sequence is receipt then one-based data-row ordinal in the unsorted original CSV; it is not the original cross-feed acceptance order.",
            "Synthetic monotonic=normalized receipt minus first fixture receipt makes deterministic replay possible; it cannot test wall-clock corrections or measured monotonic timing.",
            "No connection gaps or first-arrival completeness are inferred; no unavailable input is fabricated.",
            "Expected rows validate calculation and causal-prefix behavior against retained evidence, not future predictive accuracy, frontend delivery or production latency.",
        ],
    )
    with (output_dir / "manifest.json").open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review = dict(comparison, generator_path=manifest["generator_path"],
                  generator_sha256=manifest["generator_sha256"],
                  pending_manifest_sha256=sha256(output_dir / "manifest.json"),
                  pending_artifacts_sha256=manifest["artifacts_sha256"],
                  source_and_decision_compressed_bytes_identical=True,
                  limitations=["Pending is an assumption label, not a promise that a source event will arrive.",
                               "An interior absent stamp does not prove silence, loss, or first-arrival completeness.",
                               "All retained/upserted-source and synthetic-clock limits of v1 remain."])
    with review_path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(review, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"output": str(output_dir), "events": len(events), "decisions": len(decisions),
                      "expected_rows": len(expected), **evaluation, "labels_by_horizon": comparison["labels_by_horizon"]}, indent=2))


if __name__ == "__main__":
    main()
