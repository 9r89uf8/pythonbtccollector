"""Pure, opt-in ghost TWAP calculation. No clocks, feeds, storage or network I/O.

Inputs are already accepted canonical Chainlink spot / sixty-second TWAP events.
The caller supplies one acceptance sequence across both streams and all clocks.
The optional collector worker supplies events; this engine has no I/O.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import asdict, dataclass, replace
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
import json

from price_collector.market import MarketWindow, market_for_sample_second

HORIZONS = (1, 2, 3, 5, 10, 30)
MODEL_VERSION = "chainlink-60s-offset3-v1"
CONTRACT_VERSION = 4
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000
PRICE_QUANTUM = Decimal("0.000000000000000001")
PRICE_LIMIT = Decimal("100000000000000000000")
MATH_CONTEXT = Context(prec=80, rounding=ROUND_HALF_EVEN)


def _integer(value: int, name: str, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError(f"{name} must be a nonempty string of at most 256 characters")


def _price(value: Decimal) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or not 0 < value < PRICE_LIMIT:
        raise ValueError("price must be a positive finite Decimal within NUMERIC(38,18)")
    with localcontext(MATH_CONTEXT):
        if value.quantize(PRICE_QUANTUM) != value:
            raise ValueError("price has precision beyond E18")


def _price_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, ".18f")


@dataclass(frozen=True)
class GhostPolicy:
    enabled: bool = False
    source_max_age_ms: int = 5_000
    receipt_max_age_ms: int = 3_000
    max_carry_ms: int = 10_000
    history_ms: int = 120_000
    max_events: int = 1_024
    # Zero preserves the old disconnect-reset policy for historical replay.
    spot_reconnect_max_gap_ms: int = 10_000

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("enabled must be bool")
        for name in ("source_max_age_ms", "receipt_max_age_ms", "max_carry_ms", "history_ms", "max_events"):
            _integer(getattr(self, name), name, 1)
        _integer(self.spot_reconnect_max_gap_ms, "spot_reconnect_max_gap_ms")
        if self.spot_reconnect_max_gap_ms > 10_000:
            raise ValueError("spot reconnect allowance must not exceed 10000 ms")
        if self.history_ms < 62_000 + self.source_max_age_ms:
            raise ValueError("history must cover the oldest requested slot")


@dataclass(frozen=True)
class PriceEvent:
    feed: str
    value: Decimal
    source_timestamp_ms: int
    received_wall_ns: int
    received_monotonic_ns: int
    sequence: int
    event_id: str
    window_s: int | None = None

    def __post_init__(self) -> None:
        if self.feed not in ("spot", "twap"):
            raise ValueError("unsupported feed")
        if self.feed == "twap" and (type(self.window_s) is not int or self.window_s != 60):
            raise ValueError("only canonical sixty-second TWAP is supported")
        if self.feed == "spot" and self.window_s is not None:
            raise ValueError("spot has no TWAP window")
        _price(self.value)
        for name in ("source_timestamp_ms", "received_wall_ns", "received_monotonic_ns", "sequence"):
            _integer(getattr(self, name), name)
        if self.source_timestamp_ms % 1000:
            raise ValueError("source timestamp must be second-aligned")
        _text(self.event_id, "event_id")

    @property
    def received_ms(self) -> int:
        return self.received_wall_ns // NS_PER_MS


@dataclass(frozen=True)
class Slot:
    slot_timestamp_ms: int
    value: Decimal | None
    input: PriceEvent | None
    category: str
    carry_age_ms: int | None


@dataclass(frozen=True)
class SlotCounts:
    observed: int = 0
    carried: int = 0
    pending: int = 0
    future: int = 0
    missing: int = 0


@dataclass(frozen=True)
class GapMarker:
    feed: str
    reason: str
    ordinal: int
    after_sequence: int | None


@dataclass(frozen=True)
class SpotReconnect:
    """Bounded evidence for the latest transport gap; never proves continuity."""
    gap_ordinal: int
    gap_wall_ns: int | None
    gap_monotonic_ns: int | None
    previous_spot: PriceEvent | None
    first_post_gap_spot: PriceEvent | None
    status: str
    reason: str


@dataclass(frozen=True)
class Forecast:
    horizon_s: int
    target_source_timestamp_ms: int | None
    market: MarketWindow | None
    slot_start_index: int | None
    price: Decimal | None
    quality: str
    reasons: tuple[str, ...]
    counts: SlotCounts
    estimated_arrival_wall_ns: int | None
    estimated_remaining_ns: int | None
    estimate_overdue: bool


def _event_record(event: PriceEvent | None) -> dict | None:
    if event is None:
        return None
    result = asdict(event)
    result["value"] = _price_text(event.value)
    result["received_ms"] = event.received_ms
    return result


def _json_bytes(record: dict) -> bytes:
    # Epoch/monotonic nanoseconds exceed JavaScript's exact integer range.
    # Preserve them as strings; UTC millisecond fields and counts remain ints.
    def safe(value):
        if isinstance(value, dict):
            return {key: str(item) if key.endswith("_ns") and type(item) is int
                    else safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [safe(item) for item in value]
        return value
    return json.dumps(safe(record), sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class Decision:
    run_id: str
    decision_id: str
    decision_wall_ns: int
    decision_monotonic_ns: int
    included_sequence: int | None
    policy: GhostPolicy
    current_spot: PriceEvent | None
    current_twap: PriceEvent | None
    slots: tuple[Slot, ...]
    forecasts: tuple[Forecast, ...]
    reasons: tuple[str, ...]
    gap_count: int
    last_gaps: tuple[GapMarker, ...]
    valid_until_wall_ns: int
    spot_reconnect: SpotReconnect | None

    def _record(self) -> dict:
        reconnect = None
        if self.spot_reconnect is not None:
            reconnect = asdict(self.spot_reconnect)
            reconnect["previous_spot"] = _event_record(self.spot_reconnect.previous_spot)
            reconnect["first_post_gap_spot"] = _event_record(self.spot_reconnect.first_post_gap_spot)
        forecasts = []
        for forecast in self.forecasts:
            record = asdict(forecast)
            record["price"] = _price_text(forecast.price)
            forecasts.append(record)
        return dict(
            contract_version=CONTRACT_VERSION, model_version=MODEL_VERSION,
            run_id=self.run_id, decision_id=self.decision_id,
            decision_wall_ns=self.decision_wall_ns,
            decision_time_ms=self.decision_wall_ns // NS_PER_MS,
            decision_monotonic_ns=self.decision_monotonic_ns,
            included_sequence=self.included_sequence, policy=asdict(self.policy),
            current_spot=_event_record(self.current_spot),
            current_twap=_event_record(self.current_twap),
            forecasts=forecasts, reasons=self.reasons, gap_count=self.gap_count,
            last_gaps=[asdict(gap) for gap in self.last_gaps],
            spot_reconnect=reconnect,
            valid_until_wall_ns=self.valid_until_wall_ns,
            valid_until_ms=self.valid_until_wall_ns // NS_PER_MS,
            publication_state="not_published",
            price_precision=18, rounding="ROUND_HALF_EVEN", context_precision=80,
        )

    def to_live_json(self) -> bytes:
        """Calculation contract only; B must add real publication clocks/status."""
        return _json_bytes(self._record())

    def to_audit_json(self) -> bytes:
        record = self._record()
        inputs = {slot.input.sequence: slot.input for slot in self.slots if slot.input is not None}
        record["slot_inputs"] = [_event_record(inputs[key]) for key in sorted(inputs)]
        record["slots"] = [dict(
            slot_timestamp_ms=slot.slot_timestamp_ms, value=_price_text(slot.value),
            input_sequence=None if slot.input is None else slot.input.sequence,
            category=slot.category, carry_age_ms=slot.carry_age_ms,
        ) for slot in self.slots]
        return _json_bytes(record)


class GhostTwapEngine:
    """Single-owner state machine. A snapshot cannot be backdated.

    Bad acceptance clock/sequence ordering latches a fault: rebuild a new run to
    recover. Hard loss discards coverage and rewarms. A bounded spot transport
    reconnect may preserve observed history, with unobserved seconds estimated.
    Source disorder is retained; a TWAP regression needs a strict new high-water
    stamp before forecasts resume. Same-source spot revisions replace history,
    while already frozen decisions keep their original immutable objects.
    """

    def __init__(self, run_id: str, policy: GhostPolicy | None = None) -> None:
        _text(run_id, "run_id")
        self.run_id = run_id
        self._policy = GhostPolicy() if policy is None else policy
        if not isinstance(self._policy, GhostPolicy):
            raise TypeError("policy must be GhostPolicy")
        self._history: dict[tuple[str, int], PriceEvent] = {}
        self._current: dict[str, PriceEvent] = {}
        self._conflicts: set[int] = set()
        self._twap_high_water: int | None = None
        self._twap_regressed = False
        self._fault: str | None = None
        self._last_event: PriceEvent | None = None
        self._last_decision: tuple[int, int] | None = None
        self._gap_count = 0
        self._last_gaps: dict[str, GapMarker] = {}
        self._spot_reconnect: SpotReconnect | None = None

    @property
    def policy(self) -> GhostPolicy:
        return self._policy

    @property
    def history_size(self) -> int:
        return len(self._history)

    @property
    def spot_reconnect(self) -> SpotReconnect | None:
        return self._spot_reconnect

    def record_gap(self, feed: str, reason: str, *, observed_wall_ns: int | None = None,
                   observed_monotonic_ns: int | None = None) -> None:
        if feed not in ("spot", "twap"):
            raise ValueError("unsupported feed")
        _text(reason, "gap reason")
        for value, name in ((observed_wall_ns, "gap wall clock"),
                            (observed_monotonic_ns, "gap monotonic clock")):
            if value is not None:
                _integer(value, name)
        self._gap_count += 1
        self._last_gaps[feed] = GapMarker(feed, reason, self._gap_count,
                                         None if self._last_event is None else self._last_event.sequence)
        retain = False
        if feed == "spot" and reason == "connection_end":
            previous = self._current.get("spot")
            exclusion = "policy_disabled"
            if self.policy.enabled and self.policy.spot_reconnect_max_gap_ms and not self._fault:
                exclusion = "missing_gap_clocks"
                if observed_wall_ns is not None and observed_monotonic_ns is not None:
                    boundaries = [] if self._last_decision is None else [self._last_decision]
                    if self._last_event is not None:
                        boundaries.append((self._last_event.received_wall_ns, self._last_event.received_monotonic_ns))
                    if any(observed_wall_ns < wall or observed_monotonic_ns < mono for wall, mono in boundaries):
                        self._fault = "gap_order"
                        raise ValueError("gap clocks predate accepted data or an issued decision")
                    invalid = self._current_reasons(previous, "spot", observed_wall_ns, observed_monotonic_ns)
                    exclusion = invalid[0] if invalid else "pre_gap_source_regression"
                    frontier = max((source for (kind, source), event in self._history.items()
                                    if kind == "spot" and source * NS_PER_MS <= event.received_wall_ns), default=None)
                    retain = not invalid and previous.source_timestamp_ms == frontier
                    if retain:
                        exclusion = "awaiting_fresh_advancing_spot"
            self._spot_reconnect = SpotReconnect(
                self._gap_count, observed_wall_ns, observed_monotonic_ns,
                previous, None, "waiting" if retain else "cleared", exclusion)
        elif feed == "spot" and self._spot_reconnect is not None:
            self._spot_reconnect = replace(self._spot_reconnect, status="cleared", reason=("hard_gap:" + reason)[:256])
        if not retain:
            self._history = {key: value for key, value in self._history.items() if key[0] != feed}
        self._current.pop(feed, None)
        if feed == "twap":
            self._conflicts.clear()
            self._twap_regressed = self._twap_high_water is not None

    def _reconnect_limit_ns(self) -> int:
        return min(self.policy.spot_reconnect_max_gap_ms, self.policy.max_carry_ms) * NS_PER_MS

    def _clear_spot_reconnect(self, reason: str, event: PriceEvent | None = None) -> None:
        self._history = {key: value for key, value in self._history.items() if key[0] != "spot"}
        self._current.pop("spot", None)
        self._spot_reconnect = replace(self._spot_reconnect, status="cleared", reason=reason,
                                       first_post_gap_spot=event)

    def _resolve_spot_reconnect(self, event: PriceEvent) -> None:
        recovery = self._spot_reconnect
        if event.feed != "spot" or recovery is None or recovery.status != "waiting":
            return
        previous = recovery.previous_spot
        if (event.received_wall_ns < recovery.gap_wall_ns
                or event.received_monotonic_ns < recovery.gap_monotonic_ns):
            self._fault = "gap_order"
            raise ValueError("post-gap event predates its gap")
        invalid = self._current_reasons(event, "spot", event.received_wall_ns, event.received_monotonic_ns)
        source_delta = (event.source_timestamp_ms - previous.source_timestamp_ms) * NS_PER_MS
        wall_delta = event.received_wall_ns - previous.received_wall_ns
        mono_delta = event.received_monotonic_ns - previous.received_monotonic_ns
        if invalid:
            self._clear_spot_reconnect("post_gap:" + invalid[0], event)
        elif source_delta <= 0:
            self._clear_spot_reconnect("source_not_advanced", event)
        elif max(source_delta, wall_delta, mono_delta) > self._reconnect_limit_ns():
            self._clear_spot_reconnect("reconnect_limit_exceeded", event)
        else:
            self._spot_reconnect = replace(recovery, first_post_gap_spot=event,
                                           status="retained", reason="bounded_carry_estimates")

    def accept(self, event: PriceEvent) -> bool:
        if not isinstance(event, PriceEvent):
            raise TypeError("accept requires a PriceEvent")
        if not self.policy.enabled:
            return False
        previous = self._last_event
        if previous is not None and (
            event.sequence <= previous.sequence
            or event.received_wall_ns < previous.received_wall_ns
            or event.received_monotonic_ns < previous.received_monotonic_ns
        ):
            self._fault = "acceptance_order"
            raise ValueError("event receipt clocks/sequence regressed; create a new run")
        if self._last_decision is not None and (
            event.received_wall_ns < self._last_decision[0]
            or event.received_monotonic_ns < self._last_decision[1]
        ):
            self._fault = "backlogged_event"
            raise ValueError("event predates an issued decision; create a new run")
        if previous is not None and event.sequence > previous.sequence + 1:
            self.record_gap("spot", "sequence_gap")
            self.record_gap("twap", "sequence_gap")
        self._resolve_spot_reconnect(event)
        key = (event.feed, event.source_timestamp_ms)
        old = self._history.get(key)
        if event.feed == "twap":
            if old is not None and old.value != event.value:
                self._conflicts.add(event.source_timestamp_ms)
            # A future-dated input cannot poison the legitimate source high-water.
            if event.source_timestamp_ms * NS_PER_MS <= event.received_wall_ns:
                if self._twap_high_water is None or event.source_timestamp_ms > self._twap_high_water:
                    self._twap_high_water = event.source_timestamp_ms
                    self._twap_regressed = False
                elif event.source_timestamp_ms < self._twap_high_water:
                    self._twap_regressed = True
        self._history[key] = event
        self._current[event.feed] = event
        self._last_event = event
        self._prune(event.received_ms)
        if len(self._history) > self.policy.max_events:
            # Hard bounded loss: never silently evict a required constituent.
            self.record_gap("spot", "capacity")
            self.record_gap("twap", "capacity")
            self._history[key] = event
            self._current[event.feed] = event
        return True

    def _prune(self, wall_ms: int) -> None:
        cutoff = wall_ms - self.policy.history_ms
        predecessors = [event for (feed, source), event in self._history.items()
                        if feed == "spot" and cutoff - self.policy.max_carry_ms <= source < cutoff]
        seed = max(predecessors, key=lambda item: item.source_timestamp_ms, default=None)
        self._history = {key: event for key, event in self._history.items()
                         if key[1] >= cutoff or event is seed}
        self._conflicts.intersection_update(source for feed, source in self._history if feed == "twap")

    def _current_reasons(self, event: PriceEvent | None, feed: str,
                         wall_ns: int, mono_ns: int) -> list[str]:
        if event is None:
            return [f"missing_{feed}"]
        ages = (wall_ns - event.source_timestamp_ms * NS_PER_MS,
                wall_ns - event.received_wall_ns, mono_ns - event.received_monotonic_ns)
        if min(ages) < 0 or event.source_timestamp_ms * NS_PER_MS > event.received_wall_ns:
            return [f"future_{feed}"]
        if (ages[0] > self.policy.source_max_age_ms * NS_PER_MS
                or max(ages[1:]) > self.policy.receipt_max_age_ms * NS_PER_MS):
            return [f"stale_{feed}"]
        return []

    def snapshot(self, decision_id: str, decision_wall_ns: int,
                 decision_monotonic_ns: int) -> Decision:
        _text(decision_id, "decision_id")
        _integer(decision_wall_ns, "decision_wall_ns")
        _integer(decision_monotonic_ns, "decision_monotonic_ns")
        boundaries = [] if self._last_decision is None else [self._last_decision]
        if self._last_event is not None:
            boundaries.append((self._last_event.received_wall_ns, self._last_event.received_monotonic_ns))
        if any(decision_wall_ns < wall or decision_monotonic_ns < mono for wall, mono in boundaries):
            raise ValueError("snapshot cutoff predates already accepted data or an issued decision")
        self._last_decision = (decision_wall_ns, decision_monotonic_ns)
        recovery = self._spot_reconnect
        if recovery is not None and recovery.status == "waiting":
            if (decision_wall_ns < recovery.gap_wall_ns or decision_monotonic_ns < recovery.gap_monotonic_ns):
                raise ValueError("snapshot cutoff predates a connection gap")
            if max(decision_wall_ns - recovery.previous_spot.received_wall_ns,
                   decision_monotonic_ns - recovery.previous_spot.received_monotonic_ns) > self._reconnect_limit_ns():
                self._clear_spot_reconnect("reconnect_timeout")
        self._prune(decision_wall_ns // NS_PER_MS)
        spot, anchor = self._current.get("spot"), self._current.get("twap")
        reasons: list[str] = []
        if not self.policy.enabled:
            reasons.append("disabled")
        if self._fault is not None:
            reasons.append(self._fault)
        reasons.extend(self._current_reasons(spot, "spot", decision_wall_ns, decision_monotonic_ns))
        reasons.extend(self._current_reasons(anchor, "twap", decision_wall_ns, decision_monotonic_ns))
        if self._twap_regressed:
            reasons.append("twap_regression")
        if anchor is not None and anchor.source_timestamp_ms in self._conflicts:
            reasons.append("twap_conflict")
        reason_tuple = tuple(reasons)
        slots: list[Slot] = []
        if anchor is not None:
            spots = sorted((event for (feed, _), event in self._history.items() if feed == "spot"
                            and event.source_timestamp_ms * NS_PER_MS <= event.received_wall_ns),
                           key=lambda event: event.source_timestamp_ms)
            sources = [event.source_timestamp_ms for event in spots]
            for second in range(-61, 28):
                stamp = anchor.source_timestamp_ms + second * 1000
                if stamp * NS_PER_MS > decision_wall_ns:
                    if spot is None or self._current_reasons(spot, "spot", decision_wall_ns, decision_monotonic_ns):
                        slots.append(Slot(stamp, None, None, "missing", None))
                    else:
                        slots.append(Slot(stamp, spot.value, spot, "future", None))
                    continue
                index = bisect_right(sources, stamp) - 1
                event = spots[index] if index >= 0 else None
                age = None if event is None else stamp - event.source_timestamp_ms
                if event is None or age > self.policy.max_carry_ms:
                    slots.append(Slot(stamp, None, None, "missing", age))
                else:
                    # A tail beyond the newest admissible source has no later
                    # received observation yet. Keep the same selected price;
                    # latest-receipt current spot may have an older source.
                    # Pending does not promise that this report will arrive.
                    category = "observed" if age == 0 else "pending" if stamp > sources[-1] else "carried"
                    slots.append(Slot(stamp, event.value, event, category, age))
        forecasts: list[Forecast] = []
        for horizon in HORIZONS:
            target = None if anchor is None else anchor.source_timestamp_ms + horizon * 1000
            selected = slots[horizon - 1:horizon + 59] if anchor is not None else []
            counts = SlotCounts(**{category: sum(slot.category == category for slot in selected)
                                   for category in ("observed", "carried", "pending", "future", "missing")}) if selected else SlotCounts(missing=60)
            issues = list(reasons)
            if counts.missing:
                issues.append("missing_slots")
            if target is not None and ("twap", target) in self._history:
                issues.append("target_already_received")
            price = None
            if not issues:
                with localcontext(MATH_CONTEXT):
                    price = (sum((slot.value for slot in selected), Decimal(0)) / Decimal(60)).quantize(PRICE_QUANTUM)
            eta = None if anchor is None else anchor.received_wall_ns + horizon * NS_PER_SECOND
            remaining = None if eta is None else eta - decision_wall_ns
            forecasts.append(Forecast(
                horizon, target, None if target is None else market_for_sample_second(target),
                None if anchor is None else horizon - 1, price,
                "unavailable" if issues else "degraded" if counts.carried else "healthy",
                tuple(issues), counts, eta, remaining, remaining is not None and remaining < 0,
            ))
        deadlines = [decision_wall_ns] if reason_tuple else []
        for event in (spot, anchor):
            if event is not None:
                deadlines.extend((event.received_wall_ns + self.policy.receipt_max_age_ms * NS_PER_MS,
                                  (event.source_timestamp_ms + self.policy.source_max_age_ms) * NS_PER_MS,
                                  decision_wall_ns + self.policy.receipt_max_age_ms * NS_PER_MS
                                  - (decision_monotonic_ns - event.received_monotonic_ns)))
        return Decision(self.run_id, decision_id, decision_wall_ns, decision_monotonic_ns,
                        None if self._last_event is None else self._last_event.sequence,
                        self.policy, spot, anchor, tuple(slots), tuple(forecasts), reason_tuple,
                        self._gap_count, tuple(self._last_gaps[feed] for feed in sorted(self._last_gaps)),
                        min(deadlines, default=decision_wall_ns), self._spot_reconnect)
