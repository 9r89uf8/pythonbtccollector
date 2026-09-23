"""Causal observed market conditions for descriptive, full-market history.

This uses the already accepted spot and official TWAP events, never a Ghost
projection or reconstructed price history. It performs no I/O.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal, InvalidOperation, localcontext

from price_collector.ghost_twap import Decision, MATH_CONTEXT, PRICE_QUANTUM, PriceEvent
from price_collector.market import market_for_sample_second
from price_collector.settlement import NS_MS, _event, _safe, _validate_context

MODEL_VERSION = "market-conditions-v1"
RULE_VERSION = "historical-market-conditions-v1"
OBSERVATION_WINDOW_S = 300
SAMPLING_INTERVAL_MS = 5000


def build_observation(decision: Decision, context: dict | None,
                      context_available_wall_ns: int, context_available_monotonic_ns: int) -> dict:
    """Freeze current prices, opening reference and time in the active market.

    Decision-level reasons describe feed/clock integrity and remain binding. Slot
    and forecast availability do not: no historical spot warmup is needed here.
    Each input's original source and both receipt clocks bound this observation.
    """
    wall, mono = decision.decision_wall_ns, decision.decision_monotonic_ns
    now_ms = wall // NS_MS
    market = market_for_sample_second(now_ms // 1000 * 1000)
    target = market.market_end_ms
    reasons = list(decision.reasons)
    deadlines = [target * NS_MS]
    policy = decision.policy
    if (not policy.enabled or policy.source_max_age_ms > 5000
            or policy.receipt_max_age_ms > 3000):
        reasons.append("unsupported_input_policy")

    for name, event in (("spot", decision.current_spot), ("twap", decision.current_twap)):
        if event is None:
            reasons.append("missing_" + name)
            continue
        if (not isinstance(event, PriceEvent) or event.feed != name
                or event.window_s != (60 if name == "twap" else None)):
            reasons.append("invalid_" + name + "_identity")
            continue
        source = event.source_timestamp_ms * NS_MS
        ages = (wall - source, wall - event.received_wall_ns,
                mono - event.received_monotonic_ns)
        source_limit = min(policy.source_max_age_ms, 5000) * NS_MS
        receipt_limit = min(policy.receipt_max_age_ms, 3000) * NS_MS
        if (min(ages) < 0 or source > event.received_wall_ns
                or ages[0] > source_limit or max(ages[1:]) > receipt_limit):
            reasons.append("invalid_" + name + "_freshness")
        deadlines.extend((source + source_limit, event.received_wall_ns + receipt_limit,
                          wall + receipt_limit - ages[2]))

    reference = None
    try:
        if (type(context_available_wall_ns) is not int or type(context_available_monotonic_ns) is not int
                or not 0 < context_available_wall_ns <= wall
                or not 0 <= context_available_monotonic_ns <= mono
                or context is None or context.get("status") != "available"):
            raise ValueError("reference not available at decision")
        if (int(context["received_wall_ns"]) > context_available_wall_ns
                or int(context["received_monotonic_ns"]) > context_available_monotonic_ns):
            raise ValueError("reference received after availability")
        reference = _validate_context(context, market, wall, mono)
    except (KeyError, ValueError, TypeError, InvalidOperation):
        reasons.append("reference_unavailable_or_noncausal")

    expiry = min(deadlines)
    if expiry <= wall:
        reasons.append("inputs_expired")
    reasons = list(dict.fromkeys(reasons))
    if reasons:
        expiry = min(expiry, wall)

    signals = {}
    with localcontext(MATH_CONTEXT):
        for name, event in (("twap", decision.current_twap), ("spot", decision.current_spot)):
            value = None if event is None else event.value
            lead = None if value is None or reference is None else value - reference
            bps = None if lead is None else lead / reference * Decimal(10000)
            signals[name] = dict(
                price=None if value is None else format(value, ".18f"),
                side=None if lead is None else "up" if lead > 0 else "down" if lead < 0 else "tie",
                signed_lead_usd=None if lead is None else format(lead, ".18f"),
                lead_bps=None if bps is None else format(bps.quantize(PRICE_QUANTUM), ".18f"),
            )

    frozen_reference = deepcopy(context) if context is not None else {}
    frozen_reference.update(available_wall_ns=context_available_wall_ns,
                            available_monotonic_ns=context_available_monotonic_ns)
    observation = _safe(dict(
        schema_version=4, kind="market_conditions", model_version=MODEL_VERSION,
        rule_version=RULE_VERSION, observation_window_s=OBSERVATION_WINDOW_S,
        sampling_interval_ms=SAMPLING_INTERVAL_MS,
        run_id=decision.run_id, decision_id=decision.decision_id,
        market_id=market.market_id, market_start_ms=market.market_start_ms, market_end_ms=target,
        target_source_timestamp_ms=target, decision_time_ms=now_ms,
        decision_wall_ns=wall, decision_monotonic_ns=mono,
        remaining_ms=target - now_ms, valid_until_wall_ns=expiry, valid_until_ms=expiry // NS_MS,
        status="unavailable" if reasons else "available",
        quality="unavailable" if reasons else "healthy", reasons=reasons,
        current_twap=_event(decision.current_twap), current_spot=_event(decision.current_spot),
        reference=frozen_reference, policy=asdict(policy), signals=signals,
    ))
    from price_collector.settlement_history import cohort_key
    observation["history_cohort"] = cohort_key(observation)
    return observation
