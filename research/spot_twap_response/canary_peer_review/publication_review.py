"""Offline publication-timing review; does not infer unrecorded fsync timestamps.

The next intent in a run bounds the earlier publication worker's completed
eligibility check. Candidate causes inside that interval are evidence, not an
invented exact post-spool timestamp. No engine or runtime code is imported.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path


MILLION = Decimal(1_000_000)
ROOT = Path(__file__).resolve().parents[3]


def distribution(values):
    if not values:
        return {"n": 0}
    ordered = sorted(Decimal(value) for value in values)

    def percentile(p):
        index = Decimal(len(ordered) - 1) * Decimal(p) / Decimal(100)
        lo = int(index)
        hi = min(lo + 1, len(ordered) - 1)
        return ordered[lo] + (ordered[hi] - ordered[lo]) * (index - lo)

    return dict(n=len(ordered), min_ms=str(ordered[0] / MILLION),
                p50_ms=str(percentile(50) / MILLION), p90_ms=str(percentile(90) / MILLION),
                p99_ms=str(percentile(99) / MILLION), max_ms=str(ordered[-1] / MILLION),
                mean_ms=str(sum(ordered) / Decimal(len(ordered)) / MILLION),
                negative=sum(value < 0 for value in ordered),
                at_most_10_ms=sum(value <= 10_000_000 for value in ordered))


def review(source_path, manifest_path):
    if source_path.name.endswith(".part"):
        raise ValueError("A completed export is required")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    digest = sha256()
    statuses, totals = Counter(), Counter()
    timings = {"acknowledged_all": defaultdict(list), "acknowledged_any_available": defaultdict(list)}
    published_intents = defaultdict(list)
    preempted = []
    previous = None
    with source_path.open("rb") as source:
        for raw in source:
            if not raw.endswith(b"\n") or len(raw) > 1024 * 1024:
                raise ValueError("Truncated/oversized row")
            digest.update(raw)
            row = json.loads(raw)
            identity = (row["run_id"], row["decision_id"])
            if previous is not None and identity <= previous:
                raise ValueError("Unordered/duplicate row")
            previous = identity
            for field in ("frozen", "state"):
                if sha256(row[field + "_json"].encode()).hexdigest() != row[field + "_sha256"]:
                    raise ValueError("Row hash mismatch")
            f, s = json.loads(row["frozen_json"]), json.loads(row["state_json"])
            pub = s["publication"]
            totals["rows"] += 1
            statuses[pub["status"]] += 1
            if pub.get("intent_monotonic_ns") is not None:
                published_intents[row["run_id"]].append((int(pub["intent_monotonic_ns"]),
                    int(pub["intent_wall_ns"]), row["decision_id"]))
            if pub["status"] == "acknowledged":
                available = any(forecast["price"] is not None for forecast in f["forecasts"])
                totals["acknowledged_any_available" if available else "acknowledged_all_unavailable"] += 1
                selected = [event for event in [f["current_spot"], f["current_twap"], *f["slot_inputs"]]
                            if event is not None and event["sequence"] == f["included_sequence"]]
                receipts = {int(event["received_monotonic_ns"]) for event in selected}
                if len(receipts) != 1:
                    totals["acknowledged_missing_or_ambiguous_included_receipt"] += 1
                    continue
                receipt = receipts.pop()
                decision, completion = int(f["decision_monotonic_ns"]), int(s["computation_completed_monotonic_ns"])
                intent, attempt, ack = (int(pub[name + "_monotonic_ns"]) for name in ("intent", "attempt", "ack"))
                spans = dict(receipt_to_decision=decision-receipt,
                    decision_to_computation=completion-decision,
                    computation_to_intent=intent-completion,
                    computation_to_attempt=attempt-completion,
                    intent_to_attempt=attempt-intent,
                    attempt_to_ack=ack-attempt,
                    decision_to_ack=ack-decision,
                    receipt_to_ack=ack-receipt)
                for group in (["acknowledged_all", "acknowledged_any_available"] if available else ["acknowledged_all"]):
                    for name, span in spans.items():
                        timings[group][name].append(span)
                    timings[group]["intent_span_exceeds_half_full"].append(int(2 * (attempt-intent) > ack-receipt))
            if pub["status"] == "preempted_after_spool":
                firsts = [target["first_event"] for target in s["targets"].values() if target.get("first_event")]
                preempted.append(dict(run_id=row["run_id"], decision_id=row["decision_id"],
                    intent_mono=int(pub["intent_monotonic_ns"]), intent_wall=int(pub["intent_wall_ns"]),
                    decision_mono=int(f["decision_monotonic_ns"]), decision_wall=int(f["decision_wall_ns"]),
                    valid_until=int(f["valid_until_wall_ns"]),
                    first_target_mono=min((int(event["received_monotonic_ns"]) for event in firsts), default=None),
                    target_receipts={h: int(target["first_event"]["received_monotonic_ns"])
                        for h, target in s["targets"].items() if target.get("first_event")},
                    available_horizon_keys=[str(x["horizon_s"]) for x in f["forecasts"] if x["price"] is not None],
                    target_count=len(firsts), available_horizons=sum(x["price"] is not None for x in f["forecasts"]),
                    stop=s.get("runtime_stop"), suspensions=s.get("runtime_suspensions", {}),
                    canary_end_wall=(f["runtime_policy"]["canary_start_ms"] + 3_600_000) * 1_000_000))
    if digest.hexdigest() != manifest["sha256"] or totals["rows"] != manifest["row_count"]:
        raise ValueError("Completed export does not match manifest")

    classifications, diagnostics = Counter(), Counter()
    target_only_horizons, withheld_later_horizons = Counter(), Counter()
    bounds, target_deltas, expiry_deltas = [], [], []
    examples = defaultdict(list)
    for values in published_intents.values():
        values.sort()
    for item in preempted:
        intents = published_intents[item["run_id"]]
        position = bisect_right(intents, (item["intent_mono"], 10 ** 30, ""))
        next_intent = intents[position] if position < len(intents) else None
        expiry_mono = item["decision_mono"] + item["valid_until"] - item["decision_wall"]
        expiry_deltas.append(expiry_mono - item["intent_mono"])
        target = item["first_target_mono"]
        if target is None:
            diagnostics["no_first_target_recorded"] += 1
        else:
            target_deltas.append(target - item["intent_mono"])
            diagnostics["target_before_or_at_intent"] += int(target <= item["intent_mono"])
            diagnostics["first_target_precedes_expiry"] += int(target < expiry_mono)
            diagnostics["first_target_at_or_after_expiry"] += int(target >= expiry_mono)
        diagnostics["all_horizons_unavailable"] += int(item["available_horizons"] == 0)
        if next_intent is None:
            label = "no_later_intent_upper_bound"
            triggers = []
            if item["stop"]:
                diagnostics["unbounded_case_with_stop_" + item["stop"]["reason"]] += 1
        else:
            upper_mono, upper_wall, upper_id = next_intent
            bounds.append(upper_mono - item["intent_mono"])
            triggers = []
            if target is not None and target <= upper_mono:
                triggers.append("target_receipt")
            if expiry_mono <= upper_mono or item["valid_until"] <= upper_wall:
                triggers.append("validity_expiry")
            if any(int(status["started_monotonic_ns"]) <= upper_mono
                   and (status.get("resumed_wall_ns") is None
                        or int(status.get("resumed_monotonic_ns", 10 ** 30)) >= item["intent_mono"])
                   for status in item["suspensions"].values()):
                triggers.append("suspension")
            if item["stop"] and int(item["stop"]["observed_monotonic_ns"]) <= upper_mono:
                triggers.append("stop")
            if item["decision_mono"] + 120_000_000_000 <= upper_mono:
                triggers.append("matching_deadline")
            if item["canary_end_wall"] <= upper_wall:
                triggers.append("canary_deadline")
            label = " + ".join(triggers) if triggers else "no_recorded_trigger_within_bound"
            if label == "target_receipt":
                first_horizons = sorted((h for h, receipt in item["target_receipts"].items()
                                        if receipt == target), key=int)
                target_only_horizons["+".join(first_horizons)] += 1
                for h in ("5", "10", "30"):
                    if h in item["available_horizon_keys"] and item["target_receipts"].get(h, 10 ** 30) > upper_mono:
                        withheld_later_horizons[h] += 1
        classifications[label] += 1
        if len(examples[label]) < 3:
            examples[label].append(dict(run_id=item["run_id"], decision_id=item["decision_id"],
                intent_to_first_target_ms=None if target is None else str(Decimal(target-item["intent_mono"]) / MILLION),
                intent_to_expiry_ms=str(Decimal(expiry_mono-item["intent_mono"]) / MILLION),
                check_completion_upper_bound_ms=None if next_intent is None else str(Decimal(next_intent[0]-item["intent_mono"]) / MILLION),
                next_intent_decision_id=None if next_intent is None else next_intent[2],
                recorded_stop_reason=None if item["stop"] is None else item["stop"]["reason"],
                intent_to_recorded_stop_ms=None if item["stop"] is None else
                    str(Decimal(int(item["stop"]["observed_monotonic_ns"])-item["intent_mono"]) / MILLION)))
    timing_report = {}
    for group, stages in timings.items():
        shares = stages.pop("intent_span_exceeds_half_full", [])
        timing_report[group] = {name: distribution(values) for name, values in stages.items()}
        total = sum(stages.get("receipt_to_ack", []))
        timing_report[group]["intent_to_attempt_share_of_total_recorded_latency"] = (
            None if total == 0 else str(Decimal(sum(stages["intent_to_attempt"])) / Decimal(total)))
        completion_to_attempt = sum(stages.get("computation_to_attempt", []))
        timing_report[group]["intent_to_attempt_share_of_computation_to_attempt"] = (
            None if completion_to_attempt == 0 else str(Decimal(sum(stages["intent_to_attempt"])) / Decimal(completion_to_attempt)))
        timing_report[group]["decisions_intent_to_attempt_exceeds_half_receipt_to_ack"] = sum(shares)
    return dict(status="completed", checked_utc=datetime.now(timezone.utc).isoformat(),
        input_path=str(source_path.resolve()), input_sha256=digest.hexdigest(),
        manifest_sha256=sha256(manifest_bytes).hexdigest(),
        code_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
        runtime_sha256=sha256((ROOT / "price_collector/ghost_twap_runtime.py").read_bytes()).hexdigest(),
        spool_sha256=sha256((ROOT / "price_collector/ghost_twap_spool.py").read_bytes()).hexdigest(),
        totals=dict(totals), publication_statuses=dict(statuses), timing=timing_report,
        preempted=dict(n=len(preempted), recorded_trigger_sets_within_upper_bound=dict(classifications),
            target_only_first_receipt_horizon_sets=dict(target_only_horizons),
            target_only_available_later_horizons_unreceived_through_upper_bound=dict(withheld_later_horizons),
            diagnostics=dict(diagnostics), intent_to_check_completion_upper_bound=distribution(bounds),
            intent_to_first_target=distribution(target_deltas), intent_to_validity_expiry=distribution(expiry_deltas),
            examples=dict(examples)),
        interpretation=[
            "preempted_after_spool is an eligibility rejection after a completed durable write; coalesced_before_publication is a separate status.",
            "The next same-run publication intent bounds the prior worker's check completion. It is not the missing exact check time.",
            "Trigger sets describe recorded candidates before that bound; multiple candidates remain ambiguous. No transient unrecorded clock jump is inferred.",
            "Intent-to-attempt includes serialization, async-lock wait, executor scheduling, file checks/writes, file fsync, rename, directory fsync and post-write recheck.",
            "No recorded timestamps isolate individual fsync time. Fsync is already offloaded to an executor while durable completion gates publication.",
            "Timing distributions are per acknowledged decision from latest included receipt, not all feed arrivals, target lead or browser delivery.",
            "Quantiles use linear interpolation at (n-1)*p; financial values are neither converted nor used in timing arithmetic."])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Refusing to overwrite a review")
    with localcontext() as context:
        context.prec = 80
        result = review(args.input, args.input.with_name(args.input.name + ".manifest.json"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(dict(publication_statuses=result["publication_statuses"],
        preempted=result["preempted"], timing=result["timing"])))


if __name__ == "__main__":
    main()
