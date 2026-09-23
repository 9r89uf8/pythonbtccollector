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
MARKET_CONDITION_SELECTION_VERSION = "first-observation-time-bucket-v1"
MARKET_CONDITION_RULE = "historical-market-conditions-v1"
MARKET_CONDITION_MODEL = "market-conditions-v1"
MARKET_CONDITION_CELL_ENCODING = "market-condition-indexed-v1"
SPOT_ALIGNMENTS = ("agrees", "opposes", "tie")
TIME_BUCKETS = tuple(f"{lower}-{lower + 5}" for lower in range(0, 60, 5))
MARKET_CONDITION_TIME_BUCKETS = TIME_BUCKETS + tuple(f"{lower}-{lower + 15}" for lower in range(60, 300, 15))
MARGIN_BUCKETS = ("0-1", "1-2", "2-4", "4-8", "8+")
SIGNALS = ("ghost", "twap", "spot")
MIN_PERCENT_COUNT = 30


def observation_window_s(projection: Mapping) -> int:
    """Validate each observation contract without expanding legacy evidence."""
    window = projection.get("observation_window_s")
    if (projection.get("schema_version") == 4 or projection.get("kind") == "market_conditions"
            or projection.get("rule_version") == MARKET_CONDITION_RULE):
        if (type(window) is not int or window != 300 or projection.get("schema_version") != 4
                or projection.get("kind") != "market_conditions"
                or projection.get("rule_version") != MARKET_CONDITION_RULE
                or projection.get("model_version") != MARKET_CONDITION_MODEL
                or type(projection.get("sampling_interval_ms")) is not int
                or projection["sampling_interval_ms"] != 5000):
            raise ValueError("unsupported market condition observation window")
        return 300
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
    if type(window) is int and window == 300:
        return MARKET_CONDITION_TIME_BUCKETS
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
    elif window == 300:
        description.update(schema_version=4, kind="market_conditions", rule_version=MARKET_CONDITION_RULE,
                           observation_window_s=300, sampling_interval_ms=5000,
                           selection_version=MARKET_CONDITION_SELECTION_VERSION)
    return description


def cohort_key(projection: Mapping) -> str:
    encoded = json.dumps(cohort_description(projection), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def time_bucket(remaining_ns: int, *, window_s: int = 30) -> str | None:
    buckets = _buckets({"observation_window_s": window_s})
    if not 0 < remaining_ns <= window_s * 1_000_000_000:
        return None
    if window_s == 300 and remaining_ns >= 60_000_000_000:
        index = len(TIME_BUCKETS) + (remaining_ns - 60_000_000_000) // 15_000_000_000
        return buckets[min(len(buckets) - 1, index)]
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
    """Select an observation before inspecting its margins, including ties.

    Retrying or visiting an older audit version cannot count a second market.
    Legacy signals share an ACK; market conditions use the decision clock and
    require no publication. The caller validates the observation's eligibility.
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
    window = observation_window_s(projection)
    clock = int(projection["decision_wall_ns"] if window == 300 else state["publication"]["ack_wall_ns"])
    if window == 300 and any(result[key] != int(projection[key]) for key in
                             ("market_id", "market_start_ms", "market_end_ms")):
        raise ValueError("incompatible history market")
    bucket = time_bucket(result["market_end_ms"] * 1_000_000 - clock, window_s=window)
    if bucket is None:
        return result
    order = [str(clock), str(projection["run_id"]), str(projection["decision_id"])]
    previous = result["buckets"].get(bucket)
    if previous and (int(previous["order"][0]), *previous["order"][1:]) <= (clock, *order[1:]):
        return result
    reference = projection["reference"]
    price_to_beat = Decimal(reference["price_to_beat"])
    if not price_to_beat.is_finite() or price_to_beat <= 0:
        raise ValueError("invalid historical reference")
    signals = {}
    with localcontext() as context:
        context.prec = 80
        for name in (("twap", "spot") if window == 300 else SIGNALS):
            price = Decimal(projection["signals"][name]["price"])
            if not price.is_finite() or (window == 300 and price <= 0):
                raise ValueError("invalid historical signal")
            margin = (price - price_to_beat) / price_to_beat * 10_000
            signals[name] = {"price": str(price), "margin_bps": str(margin),
                             "side": "up" if margin > 0 else "down" if margin < 0 else "tie"}
    result["buckets"][bucket] = {
        "order": order, "remaining_ns": str(result["market_end_ms"] * 1_000_000 - clock),
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
    if markets[0]["description"].get("selection_version") == MARKET_CONDITION_SELECTION_VERSION:
        return _market_condition_daily_summary(markets, day_ms=day_ms, now_ms=now_ms, final=final)
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
        if (day["description"].get("schema_version") == 4
                or day["description"].get("selection_version") == MARKET_CONDITION_SELECTION_VERSION):
            continue
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


def _market_condition_daily_summary(markets: list[dict], *, day_ms: int, now_ms: int, final: bool) -> dict:
    """Count the TWAP-leading side once for each selected joint condition."""
    description = markets[0]["description"]
    observation_window_s(description)
    if any(item["description"] != description for item in markets):
        raise ValueError("mixed market condition descriptions")
    closed = [item for item in markets if item["market_end_ms"] <= now_ms]
    selected, counts = Counter(), {}
    ties = {bucket: Counter() for bucket in MARKET_CONDITION_TIME_BUCKETS}
    for market in closed:
        for bucket, observation in market["buckets"].items():
            if bucket not in MARKET_CONDITION_TIME_BUCKETS:
                raise ValueError("observation outside historical cohort window")
            selected[bucket] += 1
            twap, spot = (observation["signals"][name] for name in ("twap", "spot"))
            for name, signal in (("twap", twap), ("spot", spot)):
                if signal["side"] == "tie":
                    ties[bucket][name] += 1
            if twap["side"] == "tie":
                continue
            alignment = "tie" if spot["side"] == "tie" else "agrees" if spot["side"] == twap["side"] else "opposes"
            key = (bucket, margin_bucket(Decimal(twap["margin_bps"])),
                   margin_bucket(Decimal(spot["margin_bps"])), alignment)
            cell = counts.setdefault(key, Counter())
            outcome = market.get("official_outcome")
            if outcome and (outcome.get("winner") not in ("up", "down") or
                            any(outcome.get(name) != value for name, value in observation["identity"].items())):
                outcome = None
            if not outcome:
                cell["unknown"] += 1
                cell["frozen_unknown" if final else "pending"] += 1
            else:
                cell["wins" if twap["side"] == outcome["winner"] else "losses"] += 1
    # The versioned axes make these integer tuples unambiguous while keeping
    # every possible daily cell below the existing 64-KiB database row cap.
    cells = [[MARKET_CONDITION_TIME_BUCKETS.index(key[0]), MARGIN_BUCKETS.index(key[1]),
              MARGIN_BUCKETS.index(key[2]), SPOT_ALIGNMENTS.index(key[3]),
              *[values[name] for name in ("wins", "losses", "unknown", "pending", "frozen_unknown")]]
             for key, values in sorted(counts.items())]
    return {"cohort": markets[0]["cohort"], "description": deepcopy(description), "day_ms": day_ms,
            "generated_at_ms": now_ms, "final": final, "outcome_freeze_ms": now_ms if final else None,
            "covered_start_ms": min((item["market_start_ms"] for item in closed), default=None),
            "covered_end_ms": max((item["market_end_ms"] for item in closed), default=None),
            "observed_markets": len(closed), "cell_encoding": MARKET_CONDITION_CELL_ENCODING, "cells": cells,
            "coverage": [{"time_bucket": bucket, "selected_markets": selected[bucket],
                          "ties": {name: ties[bucket][name] for name in ("twap", "spot")}}
                         for bucket in MARKET_CONDITION_TIME_BUCKETS]}


def _market_condition_cells(day: Mapping) -> list[dict]:
    """Decode only the declared, bounded daily count representation."""
    if day.get("cell_encoding") != MARKET_CONDITION_CELL_ENCODING:
        raise ValueError("unsupported market condition cell encoding")
    rows = day["cells"]
    if not isinstance(rows, list) or len(rows) > len(MARKET_CONDITION_TIME_BUCKETS) * 55:
        raise ValueError("market condition daily cell capacity exceeded")
    seen, result = set(), []
    for row in rows:
        if (not isinstance(row, list) or len(row) != 9
                or any(type(value) is not int or value < 0 for value in row)
                or row[0] >= len(MARKET_CONDITION_TIME_BUCKETS)
                or row[1] >= len(MARGIN_BUCKETS) or row[2] >= len(MARGIN_BUCKETS)
                or row[3] >= len(SPOT_ALIGNMENTS) or any(value > 288 for value in row[4:])
                or row[6] != row[7] + row[8] or sum(row[4:7]) > 288
                or (row[3] == SPOT_ALIGNMENTS.index("tie") and row[2] != 0)):
            raise ValueError("invalid market condition daily cell")
        key = tuple(row[:4])
        if key in seen:
            raise ValueError("duplicate market condition daily cell")
        seen.add(key)
        result.append({"signal": "combined", "time_bucket": MARKET_CONDITION_TIME_BUCKETS[row[0]],
                       "twap_margin_bucket": MARGIN_BUCKETS[row[1]], "spot_margin_bucket": MARGIN_BUCKETS[row[2]],
                       "spot_alignment": SPOT_ALIGNMENTS[row[3]],
                       **dict(zip(("wins", "losses", "unknown", "pending", "frozen_unknown"), row[4:]))})
    return result


def freeze_daily_unknowns(body: dict) -> dict:
    """Freeze unknown outcomes for either daily format without changing totals."""
    result = deepcopy(body)
    if result.get("cell_encoding") == MARKET_CONDITION_CELL_ENCODING:
        _market_condition_cells(result)
        for cell in result["cells"]:
            cell[8] += cell[7]
            cell[7] = 0
    elif "cell_encoding" in result:
        raise ValueError("unsupported market condition cell encoding")
    else:
        for cell in result["cells"]:
            cell["frozen_unknown"] = cell.get("frozen_unknown", 0) + cell.get("pending", 0)
            cell["pending"] = 0
    return result


def market_condition_summary(days: list[dict], now_ms: int, *, complete: bool,
                             cohort_id: str | None = None) -> dict:
    """Expose only joint current-condition cohorts, without legacy forecasts.

    Daily totals remain replaceable and are never combined with individual
    rows. Cells are sparse: an unobserved combination is not an estimated rate.
    """
    grouped, identities = {}, set()
    for day in days:
        if cohort_id is not None and day["cohort"] != cohort_id:
            continue
        description = day["description"]
        if description.get("selection_version") != MARKET_CONDITION_SELECTION_VERSION:
            continue
        observation_window_s(description)
        if day["day_ms"] <= (now_ms - 90 * DAY_MS) // DAY_MS * DAY_MS or not day["observed_markets"]:
            continue
        identity = day["cohort"], day["day_ms"]
        if identity in identities:
            raise ValueError("duplicate market condition daily summary")
        identities.add(identity)
        grouped.setdefault(day["cohort"], []).append(day)
    cells, coverage, cohorts = [], [], []
    for cohort, records in sorted(grouped.items()):
        description = records[0]["description"]
        if any(record["description"] != description for record in records):
            raise ValueError("mixed market condition descriptions")
        starts = [item["covered_start_ms"] for item in records if item["covered_start_ms"] is not None]
        ends = [item["covered_end_ms"] for item in records if item["covered_end_ms"] is not None]
        start, end = min(starts, default=None), max(ends, default=None)
        observed = sum(item["observed_markets"] for item in records)
        expected = (end - start) // 300_000 if start is not None else 0
        cohort_index = len(cohorts)
        cohorts.append({"id": cohort, **deepcopy(description), "covered_start_ms": start,
                        "covered_end_ms": end, "observed_markets": observed,
                        "expected_markets": expected, "no_observation_markets": max(0, expected - observed),
                        "incomplete_frozen_days": sum(item.get("final", False) and not item.get("persistence_complete", True)
                                                      for item in records)})
        totals, selected = {}, Counter()
        ties = {bucket: Counter() for bucket in MARKET_CONDITION_TIME_BUCKETS}
        for record in records:
            for cell in _market_condition_cells(record):
                key = (cell["time_bucket"], cell["twap_margin_bucket"], cell["spot_margin_bucket"], cell["spot_alignment"])
                if (cell["signal"] != "combined" or key[0] not in MARKET_CONDITION_TIME_BUCKETS
                        or key[1] not in MARGIN_BUCKETS or key[2] not in MARGIN_BUCKETS
                        or key[3] not in ("agrees", "opposes", "tie")):
                    raise ValueError("invalid market condition cell")
                totals.setdefault(key, Counter()).update({name: cell.get(name, 0) for name in
                    ("wins", "losses", "unknown", "pending", "frozen_unknown")})
            for item in record["coverage"]:
                bucket = item["time_bucket"]
                if bucket not in MARKET_CONDITION_TIME_BUCKETS:
                    raise ValueError("invalid market condition coverage")
                selected[bucket] += item["selected_markets"]
                ties[bucket].update(item["ties"])
        for bucket in MARKET_CONDITION_TIME_BUCKETS:
            coverage.append({"cohort": cohort_index, "time_bucket": bucket, "observed_markets": observed,
                             "selected_markets": selected[bucket],
                             "classified_markets": selected[bucket] - ties[bucket]["twap"],
                             "no_eligible_observation_markets": observed - selected[bucket],
                             "no_observation_markets": max(0, expected - observed),
                             "ties": {name: ties[bucket][name] for name in ("twap", "spot")}})
        for key, values in sorted(totals.items()):
            resolved = values["wins"] + values["losses"]
            rate, interval = percentage(values["wins"], resolved)
            cells.append({"cohort": cohort_index, "signal": "combined", "time_bucket": key[0],
                          "twap_margin_bucket": key[1], "spot_margin_bucket": key[2], "spot_alignment": key[3],
                          **{name: values[name] for name in ("wins", "losses", "unknown", "pending", "frozen_unknown")},
                          "resolved": resolved, "win_rate_pct": rate, "interval95_pct": interval})
    starts = [item["covered_start_ms"] for item in cohorts if item["covered_start_ms"] is not None]
    ends = [item["covered_end_ms"] for item in cohorts if item["covered_end_ms"] is not None]
    return {"schema_version": 2, "status": "available" if starts and complete else "warming_up",
            "generated_at_ms": now_ms, "history_days": 90, "individual_days": 7,
            "selection_version": MARKET_CONDITION_SELECTION_VERSION, "direction_pooling": "twap_leading_side",
            "minimum_percentage_count": MIN_PERCENT_COUNT,
            "time_buckets": list(MARKET_CONDITION_TIME_BUCKETS), "margin_buckets": list(MARGIN_BUCKETS),
            "covered_start_ms": min(starts, default=None), "covered_end_ms": max(ends, default=None),
            "persistence_complete": complete, "backfill_pending": not complete,
            "cohorts": cohorts, "cells": cells, "coverage": coverage,
            "limitations": ["Descriptive historical frequency, not an individual-market probability",
                            "Wins follow the current TWAP-leading side; Up and Down are pooled",
                            "Spot position is classified relative to the same opening reference and TWAP side",
                            "Intervals assume independent comparable markets; markets may be correlated",
                            "TWAP ties have no leading side and are excluded from rate cells",
                            "The first eligible observation in each time bucket is selected before price classification",
                            "Daily outcomes freeze after the following UTC day; unknowns remain unknown",
                            "Coverage starts at the first retained observation; earlier missing history is not inferred"]}
