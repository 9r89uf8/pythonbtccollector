"""Fixed-bin descriptive history; no database, feed, or forecast dependencies."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from decimal import Decimal, localcontext
import hashlib
import json
from typing import Mapping

DAY_MS = 86_400_000
LEGACY_SELECTION_VERSION = "first-ack-5s-v1"
SELECTION_VERSION = "first-ack-5s-v2"
TIME_BUCKETS = tuple(f"{lower}-{lower + 5}" for lower in range(0, 60, 5))
MARGIN_BUCKETS = ("0-1", "1-2", "2-4", "4-8", "8+")
SIGNALS = ("ghost", "twap", "spot")
MIN_PERCENT_COUNT = 30


def observation_window_s(projection: Mapping) -> int:
    """Keep legacy evidence scoped to 30s; only contract 3 admits 60s."""
    window = projection.get("observation_window_s")
    if "observation_window_s" not in projection:
        if projection.get("schema_version") == 3 or projection.get("rule_version") == "historical-settlement-v2":
            raise ValueError("missing settlement observation window")
        return 30
    if (type(window) is not int or window != 60 or projection.get("schema_version") != 3
            or projection.get("rule_version") != "historical-settlement-v2"
            or type(projection.get("sampling_interval_ms")) is not int
            or projection["sampling_interval_ms"] != 2000):
        raise ValueError("unsupported settlement observation window")
    return 60


def _buckets(description: Mapping) -> tuple[str, ...]:
    window = description.get("observation_window_s", 30)
    if type(window) is not int or window not in (30, 60):
        raise ValueError("unsupported history observation window")
    return TIME_BUCKETS[:window // 5]


def cohort_description(projection: Mapping) -> dict:
    """Price identity, arithmetic and input rules, never the retired 2-bp gate.

    Missing opening observations still belong to the configured cohort's
    coverage denominator. Actual usable references are validated by the producer.
    Market-specific condition/token IDs are checked against each outcome instead.
    """
    window = observation_window_s(projection)
    description = {
        "model_version": projection.get("model_version"),
        "policy": deepcopy(projection.get("policy", {})),
        "settlement_rule_version": "btc-5m-twap-60",
        "reference_version": "causal-observed-website-opening-v1",
        "selection_version": LEGACY_SELECTION_VERSION if window == 30 else SELECTION_VERSION,
    }
    if window == 60:
        description["observation_window_s"] = 60
        description["sampling_interval_ms"] = 2000
    return description


def cohort_key(projection: Mapping) -> str:
    encoded = json.dumps(cohort_description(projection), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def time_bucket(remaining_ns: int, *, window_s: int = 30) -> str | None:
    buckets = _buckets({"observation_window_s": window_s})
    if not 0 < remaining_ns <= window_s * 1_000_000_000:
        return None
    return buckets[min(len(buckets) - 1, remaining_ns // 5_000_000_000)]


def margin_bucket(margin: Decimal) -> str:
    if not margin.is_finite():
        raise ValueError("nonfinite margin")
    absolute = abs(margin)
    for upper, label in zip((1, 2, 4, 8), MARGIN_BUCKETS):
        if absolute < upper:
            return label
    return MARGIN_BUCKETS[-1]


def observe(body: dict, projection: Mapping, state: Mapping, *, eligible: bool) -> dict:
    """Select the first ACK before inspecting its margin, including ties.

    Retrying or visiting an older audit version cannot count a second market.
    All three signals use the same acknowledged publication opportunity.
    """
    result = deepcopy(body) if body else {
        "cohort": cohort_key(projection), "description": cohort_description(projection),
        "market_id": int(projection["market_id"]),
        "market_start_ms": int(projection["market_start_ms"]),
        "market_end_ms": int(projection["market_end_ms"]), "buckets": {},
        "official_outcome": None, "outcome_checked_ms": 0,
    }
    if result["cohort"] != cohort_key(projection):
        raise ValueError("incompatible history cohort")
    if not eligible:
        return result
    ack = int(state["publication"]["ack_wall_ns"])
    bucket = time_bucket(result["market_end_ms"] * 1_000_000 - ack,
                         window_s=observation_window_s(projection))
    if bucket is None:
        return result
    order = [str(ack), str(projection["run_id"]), str(projection["decision_id"])]
    previous = result["buckets"].get(bucket)
    if previous and (int(previous["order"][0]), *previous["order"][1:]) <= (ack, *order[1:]):
        return result
    reference = projection["reference"]
    price_to_beat = Decimal(reference["price_to_beat"])
    if not price_to_beat.is_finite() or price_to_beat <= 0:
        raise ValueError("invalid historical reference")
    signals = {}
    with localcontext() as context:
        context.prec = 80
        for name in SIGNALS:
            price = Decimal(projection["signals"][name]["price"])
            if not price.is_finite():
                raise ValueError("invalid historical signal")
            margin = (price - price_to_beat) / price_to_beat * 10_000
            signals[name] = {"price": str(price), "margin_bps": str(margin),
                             "side": "up" if margin > 0 else "down" if margin < 0 else "tie"}
    result["buckets"][bucket] = {
        "order": order, "remaining_ns": str(result["market_end_ms"] * 1_000_000 - ack),
        "price_to_beat": str(price_to_beat), "signals": signals,
        "identity": {key: reference.get(key) for key in ("condition_id", "up_token_id", "down_token_id")},
    }
    return result


def percentage(wins: int, resolved: int) -> tuple[str | None, list[str] | None]:
    """Wilson interval is a descriptive independent-market approximation."""
    if resolved < MIN_PERCENT_COUNT:
        return None, None
    with localcontext() as context:
        context.prec = 50
        n, z = Decimal(resolved), Decimal("1.959963984540054")
        rate = Decimal(wins) / n
        denominator = 1 + z * z / n
        center = (rate + z * z / (2 * n)) / denominator
        radius = z * (rate * (1 - rate) / n + z * z / (4 * n * n)).sqrt() / denominator
        render = lambda value: format((value * 100).quantize(Decimal("0.01")), ".2f")
        return render(rate), [render(max(Decimal(0), center - radius)), render(min(Decimal(1), center + radius))]


def daily_summary(markets: list[dict], *, day_ms: int, now_ms: int, final: bool) -> dict:
    if not markets:
        raise ValueError("empty daily history")
    cohort = markets[0]["cohort"]
    if len({item["market_id"] for item in markets}) != len(markets):
        raise ValueError("duplicate historical market")
    if any(item["cohort"] != cohort or item["market_start_ms"] // DAY_MS * DAY_MS != day_ms for item in markets):
        raise ValueError("mixed daily cohort")
    closed = [item for item in markets if item["market_end_ms"] <= now_ms]
    buckets = _buckets(markets[0]["description"])
    counts, selected, ties = {}, Counter(), {bucket: Counter() for bucket in buckets}
    for market in closed:
        for bucket, observation in market["buckets"].items():
            if bucket not in buckets:
                raise ValueError("observation outside historical cohort window")
            selected[bucket] += 1
            outcome = market.get("official_outcome")
            if outcome and any(outcome.get(key) != value for key, value in observation["identity"].items()):
                outcome = None
            for name in SIGNALS:
                signal = observation["signals"][name]
                if signal["side"] == "tie":
                    ties[bucket][name] += 1
                    continue
                key = (name, bucket, margin_bucket(Decimal(signal["margin_bps"])))
                cell = counts.setdefault(key, Counter())
                if not outcome:
                    cell["unknown"] += 1
                    cell["frozen_unknown" if final else "pending"] += 1
                else:
                    cell["wins" if signal["side"] == outcome["winner"] else "losses"] += 1
    cells = [{"signal": key[0], "time_bucket": key[1], "margin_bucket": key[2],
              **{name: values[name] for name in ("wins", "losses", "unknown", "pending", "frozen_unknown")}}
             for key, values in sorted(counts.items())]
    return {"cohort": cohort, "description": markets[0]["description"], "day_ms": day_ms,
            "generated_at_ms": now_ms, "final": final, "outcome_freeze_ms": now_ms if final else None,
            "covered_start_ms": min((item["market_start_ms"] for item in closed), default=None),
            "covered_end_ms": max((item["market_end_ms"] for item in closed), default=None),
            "observed_markets": len(closed), "cells": cells,
            "coverage": [{"time_bucket": bucket, "selected_markets": selected[bucket],
                          "ties": {name: ties[bucket][name] for name in SIGNALS}} for bucket in buckets]}


def history_summary(days: list[dict], now_ms: int, *, complete: bool) -> dict:
    """Combine replaceable daily totals; never individual rows plus old totals."""
    grouped = {}
    for day in days:
        if day["day_ms"] <= (now_ms - 90 * DAY_MS) // DAY_MS * DAY_MS:
            continue
        if not day["observed_markets"]:
            continue
        grouped.setdefault(day["cohort"], []).append(day)
    cells, coverage, cohorts = [], [], []
    for cohort, records in sorted(grouped.items()):
        buckets = _buckets(records[-1]["description"])
        starts = [item["covered_start_ms"] for item in records if item["covered_start_ms"] is not None]
        ends = [item["covered_end_ms"] for item in records if item["covered_end_ms"] is not None]
        start, end = min(starts, default=None), max(ends, default=None)
        observed = sum(item["observed_markets"] for item in records)
        expected = (end - start) // 300_000 if start is not None else 0
        cohorts.append({"id": cohort, **records[-1]["description"], "covered_start_ms": start,
                        "covered_end_ms": end, "observed_markets": observed,
                        "expected_markets": expected, "no_observation_markets": max(0, expected - observed),
                        "incomplete_frozen_days": sum(item.get("final", False) and not item.get("persistence_complete", True)
                                                      for item in records)})
        totals, selected = {}, Counter()
        tie_counts = {bucket: Counter() for bucket in buckets}
        for record in records:
            for cell in record["cells"]:
                key = (cell["signal"], cell["time_bucket"], cell["margin_bucket"])
                totals.setdefault(key, Counter()).update({name: cell.get(name, 0) for name in
                    ("wins", "losses", "unknown", "pending", "frozen_unknown")})
            for item in record["coverage"]:
                selected[item["time_bucket"]] += item["selected_markets"]
                tie_counts[item["time_bucket"]].update(item["ties"])
        for bucket in buckets:
            coverage.append({"cohort": cohort, "time_bucket": bucket, "observed_markets": observed,
                             "selected_markets": selected[bucket],
                             "no_eligible_publication_markets": observed - selected[bucket],
                             "no_observation_markets": max(0, expected - observed),
                             "ties": {name: tie_counts[bucket][name] for name in SIGNALS}})
            for signal in SIGNALS:
                for margin in MARGIN_BUCKETS:
                    values = totals.get((signal, bucket, margin), Counter())
                    resolved = values["wins"] + values["losses"]
                    rate, interval = percentage(values["wins"], resolved)
                    cells.append({"cohort": cohort, "signal": signal, "time_bucket": bucket,
                                  "margin_bucket": margin, **{name: values[name] for name in
                                  ("wins", "losses", "unknown", "pending", "frozen_unknown")},
                                  "resolved": resolved, "win_rate_pct": rate, "interval95_pct": interval})
    starts = [item["covered_start_ms"] for item in cohorts if item["covered_start_ms"] is not None]
    ends = [item["covered_end_ms"] for item in cohorts if item["covered_end_ms"] is not None]
    return {"schema_version": 1, "status": "available" if starts and complete else "warming_up",
            "generated_at_ms": now_ms, "history_days": 90, "individual_days": 7,
            "selection_version": SELECTION_VERSION, "direction_pooling": "leading_side",
            "minimum_percentage_count": MIN_PERCENT_COUNT,
            "covered_start_ms": min(starts, default=None), "covered_end_ms": max(ends, default=None),
            "persistence_complete": complete, "backfill_pending": not complete,
            "cohorts": cohorts, "cells": cells, "coverage": coverage,
            "limitations": ["Descriptive historical frequency, not an individual-market probability",
                            "Up and Down leading sides are pooled; markets may be correlated",
                            "Intervals assume independent comparable markets",
                            "TWAP and spot share the eligible ghost publication cohort",
                            "Daily outcomes freeze after the following UTC day; unknowns remain unknown",
                            "Coverage starts at the first retained observation; earlier missing history is not inferred"]}
