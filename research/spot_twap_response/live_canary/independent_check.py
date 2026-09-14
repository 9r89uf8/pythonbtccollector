"""Independent, streaming check of completed ghost-canary exports.

No collector/engine imports, I/O to services, or reconstructed unused history.
Financial calculations use precision 80 and E18 half-even rounding throughout.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
from hashlib import sha256
import json
from pathlib import Path


HORIZONS = (1, 2, 3, 5, 10, 30)
CATEGORIES = ("observed", "carried", "pending", "future", "missing")
CONTEXT = Context(prec=80, rounding=ROUND_HALF_EVEN)
QUANTUM = Decimal("0.000000000000000001")
MS = 1_000_000
SECOND = 1_000_000_000
FIELDS = {"run_id", "decision_id", "decision_wall_ns", "created_ms", "frozen_json",
          "state_json", "version", "terminal", "frozen_sha256", "state_sha256"}


def reject_number(value):
    raise ValueError("JSON floating/nonfinite numbers are forbidden: " + value)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


def read_json(raw):
    return json.loads(raw, parse_float=reject_number, parse_constant=reject_number,
                      object_pairs_hook=unique_object)


def integer(value):
    if type(value) is int:
        return value
    if isinstance(value, str) and value and (value.isdecimal() or
            value.startswith("-") and value[1:].isdecimal()):
        return int(value)
    raise ValueError("expected exact integer clock")


def price(value):
    if not isinstance(value, str):
        raise ValueError("price must be a Decimal string")
    result = Decimal(value)
    if not result.is_finite() or result <= 0 or result >= Decimal(10) ** 20:
        raise ValueError("invalid positive E18 price")
    if result.quantize(QUANTUM) != result:
        raise ValueError("price exceeds E18 precision")
    return result


def e18(value):
    return format(value.quantize(QUANTUM), ".18f")


class Checker:
    def __init__(self):
        self.counts = Counter()
        self.failures = Counter()
        self.examples = []
        self.identity = None
        self.horizon_counts = {str(h): Counter() for h in HORIZONS}

    def check(self, condition, code, detail=None):
        self.counts["assertions"] += 1
        if not condition:
            self.failures[code] += 1
            if len(self.examples) < 100:
                self.examples.append(dict(identity=self.identity, check=code, detail=detail))

    def row(self, row):
        self.identity = [row.get("run_id"), row.get("decision_id")]
        self.check(set(row) == FIELDS, "export_fields")
        self.check(row["terminal"] is True, "terminal_export")
        self.check(sha256(row["frozen_json"].encode()).hexdigest() == row["frozen_sha256"], "frozen_hash")
        self.check(sha256(row["state_json"].encode()).hexdigest() == row["state_sha256"], "state_hash")
        frozen, state = read_json(row["frozen_json"]), read_json(row["state_json"])
        wall, mono = integer(frozen["decision_wall_ns"]), integer(frozen["decision_monotonic_ns"])
        self.check(frozen["run_id"] == row["run_id"] and frozen["decision_id"] == row["decision_id"], "row_identity")
        self.check(wall == integer(row["decision_wall_ns"]) and wall // MS == row["created_ms"], "decision_clock")
        self.check(frozen["contract_version"] == 2 and frozen["runtime_version"] == "ghost-canary-v3", "contract_version")
        self.check(frozen["rounding"] == "ROUND_HALF_EVEN" and frozen["context_precision"] == 80
                   and frozen["price_precision"] == 18, "precision_contract")
        spot, anchor = frozen["current_spot"], frozen["current_twap"]
        inputs = {}
        for event in frozen["slot_inputs"]:
            sequence = event["sequence"]
            self.check(sequence not in inputs, "duplicate_slot_input")
            inputs[sequence] = event
            self.check(event["feed"] == "spot" and event["window_s"] is None, "slot_input_feed")
            self.check(integer(event["received_wall_ns"]) <= wall
                       and integer(event["received_monotonic_ns"]) <= mono, "slot_input_causality")
            price(event["value"])
        slots = frozen["slots"]
        self.check(len(slots) == (89 if anchor else 0), "slot_array_length")
        for i, slot in enumerate(slots):
            self.counts["slots_checked"] += 1
            stamp, category = slot["slot_timestamp_ms"], slot["category"]
            self.check(stamp == anchor["source_timestamp_ms"] + (i - 61) * 1000, "slot_stamp")
            self.check(category in CATEGORIES, "slot_category")
            if category == "missing":
                self.check(slot["value"] is None and slot["input_sequence"] is None, "missing_slot_reference")
                continue
            event = inputs.get(slot["input_sequence"])
            self.check(event is not None, "slot_reference_exists")
            if event is None:
                continue
            self.check(slot["value"] == event["value"], "slot_reference_price")
            price(slot["value"])
            if category == "future":
                self.check(stamp * MS > wall and spot is not None and event == spot
                           and slot["carry_age_ms"] is None, "future_slot_reference")
            else:
                age = stamp - event["source_timestamp_ms"]
                self.check(stamp * MS <= wall and event["source_timestamp_ms"] * MS
                           <= integer(event["received_wall_ns"]), "historical_slot_causality")
                self.check(slot["carry_age_ms"] == age and 0 <= age <= frozen["policy"]["max_carry_ms"], "carry_age")
                self.check((category == "observed") == (age == 0), "exact_slot_category")

        forecasts = frozen["forecasts"]
        self.check([f["horizon_s"] for f in forecasts] == list(HORIZONS), "six_horizons")
        self.check(set(state["targets"]) == {str(h) for h in HORIZONS}, "six_target_states")
        pub = state["publication"]
        self.counts["publication_" + pub["status"]] += 1
        payload = read_json(pub["payload_json"]) if pub.get("payload_json") is not None else None
        if payload is not None:
            self.check(payload["run_id"] == row["run_id"] and payload["decision_id"] == row["decision_id"], "payload_identity")
            self.check(payload["current_spot"] == spot and payload["current_twap"] == anchor, "payload_current_inputs")
            self.check(len(payload["forecasts"]) == 6, "payload_six_horizons")
            self.counts["payloads_checked"] += 1
        completed = integer(state["computation_completed_monotonic_ns"])
        self.check(completed >= mono, "computation_order")
        for h, forecast in zip(HORIZONS, forecasts):
            self.horizon(h, forecast, frozen, state, inputs, payload, wall, mono)

    def horizon(self, h, forecast, frozen, state, inputs, payload, wall, mono):
        counts = self.horizon_counts[str(h)]
        counts["forecasts"] += 1
        anchor, spot = frozen["current_twap"], frozen["current_spot"]
        stamp = None if anchor is None else anchor["source_timestamp_ms"] + h * 1000
        start = None if anchor is None else h - 1
        selected = [] if start is None else frozen["slots"][start:start + 60]
        actual_counts = {name: sum(s["category"] == name for s in selected) for name in CATEGORIES}
        if not selected:
            actual_counts["missing"] = 60
        self.check(forecast["target_source_timestamp_ms"] == stamp and forecast["slot_start_index"] == start, "forecast_window_identity", h)
        self.check(forecast["counts"] == actual_counts and sum(actual_counts.values()) == 60, "forecast_category_counts", h)
        if anchor:
            self.check(len(selected) == 60 and selected[0]["slot_timestamp_ms"] == stamp - 62000
                       and selected[-1]["slot_timestamp_ms"] == stamp - 3000, "sixty_slot_bounds", h)
            market_start = stamp // 300000 * 300000
            self.check(forecast["market"] == dict(market_id=market_start // 300000,
                market_start_ms=market_start, market_end_ms=market_start + 300000), "target_market", h)
        eta = None if anchor is None else integer(anchor["received_wall_ns"]) + h * SECOND
        self.check(forecast["estimated_arrival_wall_ns"] is None if eta is None else
                   integer(forecast["estimated_arrival_wall_ns"]) == eta, "eta_value", h)
        self.check(forecast["estimated_remaining_ns"] is None if eta is None else
                   integer(forecast["estimated_remaining_ns"]) == eta - wall, "eta_remaining", h)
        self.check(forecast["estimate_overdue"] == (eta is not None and eta < wall), "eta_overdue", h)
        max_carry = max((s["carry_age_ms"] for s in selected if s["category"] == "carried"), default=0)
        self.check(forecast["max_interior_carry_ms"] == max_carry, "max_interior_carry", h)
        if forecast["price"] is not None:
            counts["available"] += 1
            self.counts["available_forecasts_recomputed"] += 1
            self.check(not forecast["reasons"] and actual_counts["missing"] == 0, "available_reasons", h)
            if len(selected) == 60 and all(s["value"] is not None for s in selected):
                expected = e18(sum((price(s["value"]) for s in selected), Decimal(0)) / Decimal(60))
                self.check(forecast["price"] == expected, "sixty_slot_mean", h)
            self.check(forecast["quality"] == ("degraded" if actual_counts["carried"] else "healthy"), "available_quality", h)
            for event in (spot, anchor):
                self.check(event is not None, "available_current_input", h)
                if event:
                    source_wall = event["source_timestamp_ms"] * MS
                    received_wall = integer(event["received_wall_ns"])
                    ages = (wall-source_wall, wall-received_wall, mono-integer(event["received_monotonic_ns"]))
                    self.check(source_wall <= received_wall and all(0 <= age <= 3000 * MS for age in ages), "available_current_freshness", h)
        else:
            counts["unavailable"] += 1
            self.check(forecast["quality"] == "unavailable" and bool(forecast["reasons"]), "unavailable_quality", h)
        if payload is not None:
            matches = [f for f in payload["forecasts"] if f["horizon_s"] == h]
            self.check(len(matches) == 1 and matches[0] == forecast, "payload_frozen_forecast", h)
        target = state["targets"][str(h)]
        self.check(target["target_source_timestamp_ms"] == stamp and target["horizon"] == str(h), "target_identity", h)
        counts["target_" + target["status"]] += 1
        first = target.get("first_event")
        ack = state["publication"].get("ack_monotonic_ns")
        expected_lead = None
        if first is not None:
            counts["first_events"] += 1
            received = integer(first["received_monotonic_ns"])
            self.check(target["status"] == "matched" and forecast["price"] is not None, "first_event_status", h)
            self.check(first["feed"] == "twap" and first["window_s"] == 60 and first["source_timestamp_ms"] == stamp, "first_event_identity", h)
            self.check(mono <= received < mono + 120 * SECOND, "target_receipt_window", h)
            actual = price(first["value"])
            if forecast["price"] is not None:
                self.check(target["error"] == e18(price(forecast["price"]) - actual), "stored_ghost_error", h)
                self.check(target["persistence_error"] == e18(price(anchor["value"]) - actual), "stored_persistence_error", h)
                self.counts["matched_error_pairs_checked"] += 1
            self.check(integer(target["eta_error_ns"]) == integer(first["received_wall_ns"]) - eta, "stored_eta_error", h)
            self.check(target.get("clock_anomaly") is (stamp * MS > integer(first["received_wall_ns"])), "target_clock_flag", h)
            if (ack is not None and state["publication"]["status"] == "acknowledged"
                    and not target.get("conflicted") and not target.get("clock_anomaly")
                    and not state.get("causality_invalid") and integer(ack) < received):
                expected_lead = received - integer(ack)
        else:
            self.check(target["status"] != "matched", "matched_without_first_event", h)
        recorded_lead = target.get("confirmed_redis_lead_ns")
        self.check((None if recorded_lead is None else integer(recorded_lead)) == expected_lead, "confirmed_lead_eligibility", h)
        if expected_lead is not None:
            counts["confirmed_leads_checked"] += 1
            self.counts["confirmed_leads_checked"] += 1


def audit(input_path: Path, manifest_path: Path):
    if input_path.name.endswith(".part") or manifest_path.name.endswith(".part"):
        raise ValueError("Only a completed export and manifest may be verified")
    manifest_bytes = manifest_path.read_bytes()
    manifest = read_json(manifest_bytes)
    expected_hash, expected_rows = manifest["sha256"], manifest["row_count"]
    if type(expected_rows) is not int or not 0 <= expected_rows <= 600000:
        raise ValueError("invalid manifest row count")
    checker = Checker()
    digest, previous = sha256(), None
    with localcontext(CONTEXT), input_path.open("rb") as source:
        while True:
            raw = source.readline(1024 * 1024 + 1)
            if not raw:
                break
            digest.update(raw)
            checker.counts["rows"] += 1
            if len(raw) > 1024 * 1024 or not raw.endswith(b"\n"):
                raise ValueError("oversized or truncated export row")
            if checker.counts["rows"] > 600000:
                raise ValueError("export exceeds bounded canary")
            try:
                row = read_json(raw)
                identity = (row["run_id"], row["decision_id"])
                checker.identity = identity
                checker.check(previous is None or identity > previous, "ordered_unique_identity")
                previous = identity
                checker.row(row)
            except (KeyError, ValueError, TypeError, ArithmeticError, IndexError) as exc:
                checker.check(False, "row_parse_or_check_exception", type(exc).__name__ + ": " + str(exc)[:180])
    checker.identity = None
    checker.check(checker.counts["rows"] == expected_rows, "manifest_row_count")
    checker.check(digest.hexdigest() == expected_hash, "manifest_sha256")
    return dict(status="passed" if not checker.failures else "failed",
        checked_utc=datetime.now(timezone.utc).isoformat(),
        input_path=str(input_path.resolve()), input_sha256=digest.hexdigest(),
        manifest_path=str(manifest_path.resolve()), manifest_sha256=sha256(manifest_bytes).hexdigest(),
        code_path=str(Path(__file__).resolve()), code_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
        counts=dict(checker.counts), horizons={h: dict(c) for h, c in checker.horizon_counts.items()},
        mismatch_count=sum(checker.failures.values()), mismatches_by_check=dict(checker.failures),
        mismatch_examples=checker.examples,
        limitations=["Checks selected frozen snapshots, not all historical or rejected/unused input events.",
            "Slot reference and age checks do not independently prove complete input selection or the unseen history frontier.",
            "Hash verification proves exported byte integrity, not independent truth of the recorded receipt clocks.",
            "Redis acknowledgement evidence does not establish browser receipt or execution profitability."])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Refusing to overwrite an existing result")
    manifest = args.manifest or args.input.with_name(args.input.name + ".manifest.json")
    result = audit(args.input, manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    print(json.dumps({key: result[key] for key in ("status", "counts", "mismatch_count", "input_sha256")}))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
