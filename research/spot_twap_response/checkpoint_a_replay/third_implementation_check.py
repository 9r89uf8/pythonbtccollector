"""Independent third implementation of the ghost TWAP rules (contract 2), for review only.

Written from the rules as stated in GHOST_TWAP_LIVE_PLAN.md and the pending_v2 fixture README,
without reading the engine or the fixture builder for the calculation itself. It replays the
recorded hour, recomputes every forecast on a sample of decisions from the causal event prefix,
and compares against (1) the fixture's independent expected table and (2) the engine's output.
It also reports the interior-carry age distribution behind the degraded label and checks that the
contract 1 and contract 2 expected tables agree on price, availability and ETA for every row.

Run from the repository root:  python research/spot_twap_response/checkpoint_a_replay/third_implementation_check.py
Research-only; imports the engine solely to compare against it. Not used by any service.
"""
from __future__ import annotations

import csv
import gzip
import sys
from collections import Counter
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "ghost_twap"
HORIZONS = (1, 2, 3, 5, 10, 30)
MAX_AGE_NS = 3_000 * 1_000_000
MAX_CARRY_MS = 10_000
NS = 1_000_000
SAMPLE_EVERY = 7


def rows(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def my_ghost(decision_ns: int, prefix: list[dict]) -> dict:
    """Forecasts from the events received by decision_ns, in receipt order."""
    spots = [r for r in prefix if r["kind"] == "spot"]
    twaps = [r for r in prefix if r["kind"] == "twap"]
    cur_spot = spots[-1] if spots else None
    cur_twap = twaps[-1] if twaps else None
    reasons = []
    for name, event in (("spot", cur_spot), ("twap", cur_twap)):
        if event is None:
            reasons.append("missing_" + name)
            continue
        if event["src"] * NS > event["recv"] or decision_ns < event["recv"] or decision_ns < event["src"] * NS:
            reasons.append("future_" + name)
            continue
        if max(decision_ns - event["src"] * NS, decision_ns - event["recv"]) > MAX_AGE_NS:
            reasons.append("stale_" + name)
    if cur_twap is None:
        return {h: (None, None, reasons, 0) for h in HORIZONS}
    latest_by_src = {}
    for r in spots:  # later receipt overwrites: latest accepted revision per source stamp
        if r["src"] * NS <= r["recv"]:
            latest_by_src[r["src"]] = r
    srcs = sorted(latest_by_src)
    frontier = srcs[-1] if srcs else None
    seen_twap = {r["src"] for r in twaps}
    anchor = cur_twap["src"]
    out = {}
    for h in HORIZONS:
        target = anchor + h * 1000
        issues = list(reasons)
        counts = Counter()
        values = []
        max_interior_carry = 0
        for u in range(target - 62_000, target - 3_000 + 1, 1000):
            if u * NS > decision_ns:
                if cur_spot is None or any(x.endswith("spot") for x in reasons):
                    counts["missing"] += 1
                else:
                    counts["future"] += 1
                    values.append(cur_spot["p"])
                continue
            lo, hi = 0, len(srcs)
            while lo < hi:  # greatest source stamp <= u
                mid = (lo + hi) // 2
                if srcs[mid] <= u:
                    lo = mid + 1
                else:
                    hi = mid
            if lo == 0:
                counts["missing"] += 1
                continue
            s = srcs[lo - 1]
            age = u - s
            if age > MAX_CARRY_MS:
                counts["missing"] += 1
                continue
            if age == 0:
                counts["exact"] += 1
            elif u > frontier:
                counts["pending"] += 1
            else:
                counts["carried"] += 1
                max_interior_carry = max(max_interior_carry, age)
            values.append(latest_by_src[s]["p"])
        if counts["missing"]:
            issues.append("missing_slots")
        if target in seen_twap:
            issues.append("target_already_received")
        price = None
        if not issues:
            with localcontext() as ctx:
                ctx.prec = 80
                ctx.rounding = ROUND_HALF_EVEN
                price = (sum(values, Decimal(0)) / Decimal(60)).quantize(Decimal("1e-18"))
        out[h] = (price, counts, issues, max_interior_carry)
    return out


def main() -> None:
    v2 = FIXTURE / "pending_v2"
    events = rows(v2 / "recorded_hour_events.csv.gz")
    decisions = rows(v2 / "recorded_hour_decisions.csv.gz")
    expected = rows(v2 / "recorded_hour_expected.csv.gz")
    expected_v1 = rows(FIXTURE / "recorded_hour_expected.csv.gz")
    key = lambda r: (r["decision_id"], int(r["horizon_s"]))
    exp = {key(r): r for r in expected}
    exp_v1 = {key(r): r for r in expected_v1}

    # Contract 1 vs contract 2: prices, availability and ETA must be identical; carried_v1 == carried + pending.
    diff = sum(1 for k, r in exp.items() if (r["price"], r["estimated_arrival_wall_ns"], r["quality"] == "unavailable")
               != (exp_v1[k]["price"], exp_v1[k]["estimated_arrival_wall_ns"], exp_v1[k]["quality"] == "unavailable"))
    split = sum(1 for k, r in exp.items() if int(exp_v1[k]["carried"]) != int(r["carried"]) + int(r["pending"]))
    print(f"contract 1 vs 2 on {len(exp)} rows: price/availability/ETA differences = {diff}; carried split mismatches = {split}")

    ev = sorted(events, key=lambda r: int(r["replay_order"]))
    for r in ev:
        r["recv"] = int(r["replay_received_wall_ns"])
        r["src"] = int(r["source_ms"])
        r["p"] = Decimal(r["price"])
    engine = GhostTwapEngine("third", GhostPolicy(enabled=True))
    nxt = checked = bad_expected = bad_engine = 0
    carry_ages = {h: Counter() for h in HORIZONS}
    for i, d in enumerate(decisions):
        cutoff = int(d["decision_wall_ns"])
        while nxt < len(ev) and ev[nxt]["recv"] <= cutoff:
            r = ev[nxt]
            engine.accept(PriceEvent(r["kind"], r["p"], r["src"], r["recv"], int(r["replay_monotonic_ns"]),
                                     int(r["replay_order"]), r["event_id"], 60 if r["kind"] == "twap" else None))
            nxt += 1
        decision = engine.snapshot(d["decision_id"], cutoff, int(d["decision_monotonic_ns"]))
        mine = my_ghost(cutoff, ev[:nxt])
        for forecast in decision.forecasts:
            price, counts, issues, max_carry = mine[forecast.horizon_s]
            if price is not None:
                carry_ages[forecast.horizon_s][max_carry // 1000] += 1
            if i % SAMPLE_EVERY:
                continue
            checked += 1
            e = exp[(d["decision_id"], forecast.horizon_s)]
            exp_price = Decimal(e["price"]) if e["price"] else None
            exp_counts = tuple(int(e[k]) for k in ("exact", "carried", "pending", "future", "missing"))
            my_counts = tuple(counts[k] for k in ("exact", "carried", "pending", "future", "missing")) if counts else None
            my_quality = "unavailable" if price is None else "degraded" if counts["carried"] else "healthy"
            if price != exp_price or (counts and my_counts != exp_counts) or my_quality != e["quality"]:
                bad_expected += 1
                if bad_expected <= 5:
                    print("MISMATCH vs expected", d["decision_id"], forecast.horizon_s, price, exp_price, my_counts, exp_counts, my_quality, e["quality"])
            eng_counts = (forecast.counts.observed, forecast.counts.carried, forecast.counts.pending,
                          forecast.counts.future, forecast.counts.missing)
            if price != forecast.price or (counts and my_counts != eng_counts) or my_quality != forecast.quality:
                bad_engine += 1
                if bad_engine <= 5:
                    print("MISMATCH vs engine", d["decision_id"], forecast.horizon_s, price, forecast.price, my_counts, eng_counts, my_quality, forecast.quality)
    print(f"third implementation: checked {checked} sampled forecasts; mismatches vs expected = {bad_expected}; vs engine = {bad_engine}")
    print("available forecasts by maximum interior carry age (seconds), all 3,600 decisions:")
    for h in HORIZONS:
        total = sum(carry_ages[h].values())
        dist = dict(sorted(carry_ages[h].items()))
        healthy = carry_ages[h][0]
        within3 = sum(v for k, v in carry_ages[h].items() if k <= 3)
        print(f"  h={h:>2}: {dist}  -> healthy (no carry) {healthy}/{total}; carry <= 3 s {within3}/{total}")


if __name__ == "__main__":
    main()
