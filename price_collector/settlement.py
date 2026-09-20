"""Opt-in, causal closing-stamp projection; independent of ghost contract 4."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal, InvalidOperation, localcontext
import json
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

from price_collector.dashboard_api import opening_reference
from price_collector.ghost_twap import Decision, MATH_CONTEXT, PRICE_QUANTUM, Slot
from price_collector.market import market_for_sample_second

SETTLEMENT_KEY = "btc:live:ghost_settlement"
SETTLEMENT_CHANNEL = SETTLEMENT_KEY + ":updates"
CONTEXT_KEY = "btc:live:ghost_settlement_context"
CONTEXT_MAX_BYTES = 8192
RULE_VERSION = "historical-settlement-v2"
OBSERVATION_WINDOW_S = 60
DAY_MS = 86_400_000
NS_MS = 1_000_000
CATEGORIES = ("observed", "carried", "pending", "future", "missing")


class SettlementSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SETTLEMENT_", case_sensitive=False)
    enabled: bool = False


def _safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: str(item) if key.endswith("_ns") and type(item) is int else _safe(item)
                for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_safe(item) for item in value]
    return value


def _money(value: Any) -> Decimal:
    if not isinstance(value, (str, Decimal)):
        raise ValueError("reference price must be an exact decimal")
    number = Decimal(value)
    with localcontext(MATH_CONTEXT):
        if not number.is_finite() or not Decimal(0) < number < Decimal("1e20"):
            raise ValueError("reference price outside allowed range")
        if number.quantize(PRICE_QUANTUM) != number:
            raise ValueError("reference price exceeds E18 precision")
    return number


def _canonical_price(value: Any) -> str:
    text = format(_money(value), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _event(event: Any) -> dict | None:
    if event is None:
        return None
    record = asdict(event)
    record.update(value=format(event.value, ".18f"), received_ms=event.received_ms)
    return _safe(record)


def make_context(market: Any, payload: dict, *, status: str, http_status: int | None,
                 requested_wall_ns: int, requested_monotonic_ns: int,
                 received_wall_ns: int, received_monotonic_ns: int,
                 identity_validated: bool, response_sha256: str | None = None) -> dict:
    """Wrap an existing evidence response; it does not fetch or infer a strike."""
    window = market.window
    context = dict(
        schema_version=1, market_id=window.market_id,
        market_start_ms=window.market_start_ms, market_end_ms=window.market_end_ms,
        condition_id=market.condition_id, up_token_id=market.up_token_id,
        down_token_id=market.down_token_id, settlement_reference=market.settlement_reference,
        settlement_window_s=market.settlement_window_s,
        settlement_source_url=market.settlement_source_url,
        settlement_rule_version=market.settlement_rule_version,
        source_url=payload.get("source_url"), request_params=deepcopy(payload.get("request_params")),
        price_to_beat=payload.get("price_to_beat"), completed=payload.get("completed"),
        incomplete=payload.get("incomplete"), observation_status=status, http_status=http_status,
        requested_wall_ns=requested_wall_ns, requested_monotonic_ns=requested_monotonic_ns,
        received_wall_ns=received_wall_ns, received_monotonic_ns=received_monotonic_ns,
        identity_validated=identity_validated, response_sha256=response_sha256,
        status="unavailable", reason="opening_reference_unavailable", conflicted=False,
    )
    context = _safe(context)
    try:
        now_ms = (received_wall_ns + NS_MS - 1) // NS_MS
        _validate_context(context, window, now_ms * NS_MS, received_monotonic_ns)
        context["price_to_beat"] = _canonical_price(context["price_to_beat"])
        context.update(status="available", reason=None)
    except (KeyError, ValueError, TypeError, InvalidOperation):
        context["price_to_beat"] = None
    return context


def _validate_context(context: dict, window: Any, wall_ns: int, mono_ns: int) -> Decimal:
    if not isinstance(context, dict) or context.get("schema_version") != 1:
        raise ValueError("reference context invalid")
    if (context.get("market_id") != window.market_id
            or context.get("market_start_ms") != window.market_start_ms
            or context.get("market_end_ms") != window.market_end_ms
            or context.get("identity_validated") is not True or context.get("conflicted") is not False):
        raise ValueError("reference identity or conflict")
    request_wall, receipt_wall = int(context["requested_wall_ns"]), int(context["received_wall_ns"])
    request_mono, receipt_mono = int(context["requested_monotonic_ns"]), int(context["received_monotonic_ns"])
    if not (0 <= request_wall <= receipt_wall <= wall_ns and 0 <= request_mono <= receipt_mono <= mono_ns):
        raise ValueError("reference clocks are noncausal")
    row = dict(context, status=context.get("observation_status"), payload={
        "source_url": context.get("source_url"), "request_params": context.get("request_params"),
        "price_to_beat": context.get("price_to_beat"), "completed": context.get("completed"),
    })
    result = opening_reference([row], window, wall_ns // NS_MS)
    if result["reference_status"] != "official":
        raise ValueError("reference observation invalid")
    return _money(result["price_to_beat"])


def latch_context(previous: dict | None, current: dict) -> dict:
    """A conflicting website opening stays unavailable until this market ends.

    The metadata worker reloads the bounded Redis context before updating it,
    preserving this latch across its own restart. There is no stream fallback.
    """
    result = deepcopy(current)
    same = isinstance(previous, dict) and previous.get("market_id") == result["market_id"]
    old = previous.get("first_price_to_beat") if same else None
    new = result.get("price_to_beat") if result.get("status") == "available" else None
    first = old if old is not None else result.get("first_price_to_beat", new)
    conflicted = bool(result.get("conflicted") or same and previous.get("conflicted"))
    if old is not None and new is not None and _money(old) != _money(new):
        conflicted = True
    result["first_price_to_beat"] = first
    result["conflicted"] = conflicted
    if conflicted:
        result.update(status="unavailable", reason="conflicting_website_openings", price_to_beat=None)
    return result


def decode_context(raw: bytes | str) -> dict:
    def reject(value):
        raise ValueError("context must not contain JSON floats or nonfinite values")
    if not isinstance(raw, (bytes, str)) or not 0 < len(raw) <= CONTEXT_MAX_BYTES:
        raise ValueError("context payload exceeds limit")
    context = json.loads(raw, parse_float=reject, parse_constant=reject)
    if not isinstance(context, dict) or context.get("schema_version") != 1:
        raise ValueError("context payload invalid")
    return context


def build_projection(decision: Decision, context: dict | None,
                     context_available_wall_ns: int, context_available_monotonic_ns: int) -> dict:
    """Project the ending market from this decision's already-frozen inputs."""
    wall, mono = decision.decision_wall_ns, decision.decision_monotonic_ns
    now_ms = wall // NS_MS
    market = market_for_sample_second(now_ms // 1000 * 1000)
    target = market.market_end_ms
    expiry = min(decision.valid_until_wall_ns, target * NS_MS)
    remaining = target - now_ms
    reasons = list(decision.reasons)
    if not 0 < target * NS_MS - wall <= OBSERVATION_WINDOW_S * 1000 * NS_MS:
        reasons.append("outside_final_60_seconds")
    if expiry <= wall:
        reasons.append("inputs_expired")
    if (not decision.policy.enabled or decision.policy.source_max_age_ms > 5000
            or decision.policy.receipt_max_age_ms > 3000 or decision.policy.max_carry_ms > 10000):
        reasons.append("unsupported_input_policy")
    for name, event in (("spot", decision.current_spot), ("twap", decision.current_twap)):
        if event is None:
            reasons.append("missing_" + name)
            continue
        ages = (wall - event.source_timestamp_ms * NS_MS, wall - event.received_wall_ns,
                mono - event.received_monotonic_ns)
        if (min(ages) < 0 or ages[0] > decision.policy.source_max_age_ms * NS_MS
                or max(ages[1:]) > decision.policy.receipt_max_age_ms * NS_MS):
            reasons.append("invalid_" + name + "_freshness")
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
    anchor = decision.current_twap
    horizon = None if anchor is None else (target - anchor.source_timestamp_ms) // 1000
    if horizon is None or not 1 <= horizon <= OBSERVATION_WINDOW_S + 5:
        reasons.append("unsupported_closing_horizon")
    by_stamp = {slot.slot_timestamp_ms: slot for slot in decision.slots}
    selected = []
    for stamp in range(target - 62_000, target - 2_000, 1000):
        slot = by_stamp.get(stamp)
        if slot is None and anchor is not None and decision.current_spot is not None:
            # Only extend the future tail. Missing past inputs must remain
            # missing; they cannot be reconstructed with today's spot.
            if (stamp > decision.slots[-1].slot_timestamp_ms and stamp * NS_MS > wall):
                slot = Slot(stamp, decision.current_spot.value, decision.current_spot, "future", None)
        if slot is None:
            slot = Slot(stamp, None, None, "missing", None)
        selected.append(slot)
    counts = {name: sum(slot.category == name for slot in selected) for name in CATEGORIES}
    if counts["missing"] or sum(counts.values()) != 60:
        reasons.append("missing_slots")
    for slot in selected:
        if slot.input is not None and (slot.input.received_wall_ns > wall or slot.input.received_monotonic_ns > mono):
            reasons.append("slot_input_after_decision")
        if slot.category in ("carried", "pending") and (
                slot.carry_age_ms is None or slot.carry_age_ms > decision.policy.max_carry_ms):
            reasons.append("carry_limit")
    maximum_carry = max((slot.carry_age_ms or 0 for slot in selected if slot.category == "carried"), default=0)
    reasons = list(dict.fromkeys(reasons))
    projected = None
    signals = {}
    with localcontext(MATH_CONTEXT):
        if not reasons:
            projected = (sum((slot.value for slot in selected), Decimal(0)) / Decimal(60)).quantize(PRICE_QUANTUM)
        for name, value in (("ghost", projected), ("twap", None if anchor is None else anchor.value),
                            ("spot", None if decision.current_spot is None else decision.current_spot.value)):
            lead = None if value is None or reference is None else value - reference
            bps = None if lead is None else lead / reference * Decimal(10000)
            signals[name] = dict(price=None if value is None else format(value, ".18f"),
                side=None if lead is None else "up" if lead > 0 else "down" if lead < 0 else "tie",
                signed_lead_usd=None if lead is None else format(lead, ".18f"),
                lead_bps=None if bps is None else format(bps.quantize(PRICE_QUANTUM), ".18f"))
    inputs = {slot.input.sequence: slot.input for slot in selected if slot.input is not None}
    frozen_reference = deepcopy(context) if context is not None else {}
    frozen_reference.update(available_wall_ns=context_available_wall_ns,
                            available_monotonic_ns=context_available_monotonic_ns)
    projection = _safe(dict(
        schema_version=3, kind="settlement", model_version="chainlink-60s-offset3-settlement-v1",
        observation_window_s=OBSERVATION_WINDOW_S,
        sampling_interval_ms=2000,
        rule_version=RULE_VERSION, run_id=decision.run_id, decision_id=decision.decision_id,
        market_id=market.market_id, market_start_ms=market.market_start_ms, market_end_ms=target,
        target_source_timestamp_ms=target, decision_time_ms=now_ms,
        decision_wall_ns=wall, decision_monotonic_ns=mono, source_horizon_s=horizon,
        remaining_ms=remaining, valid_until_wall_ns=expiry, valid_until_ms=expiry // NS_MS,
        status="unavailable" if reasons else "available",
        quality="unavailable" if reasons else "degraded" if counts["carried"] else "healthy", reasons=reasons,
        projected_price=None if projected is None else format(projected, ".18f"),
        current_twap=_event(anchor), current_spot=_event(decision.current_spot),
        reference=frozen_reference, policy=asdict(decision.policy), counts=counts,
        max_interior_carry_ms=maximum_carry, signals=signals,
        slots=[dict(slot_timestamp_ms=slot.slot_timestamp_ms, value=slot.value,
                    input_sequence=None if slot.input is None else slot.input.sequence,
                    category=slot.category, carry_age_ms=slot.carry_age_ms) for slot in selected],
        slot_inputs=[_event(inputs[key]) for key in sorted(inputs)],
    ))
    from price_collector.settlement_history import cohort_key
    projection["history_cohort"] = cohort_key(projection)
    return projection


def public_payload(projection: dict) -> dict:
    return deepcopy({key: value for key, value in projection.items() if key not in ("slots", "slot_inputs")})
