"""Pending means beyond the received source frontier, not known unchanged."""

from decimal import Decimal
import json

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent


BASE = 1_788_220_800_000
NS = 1_000_000
MONO_ORIGIN = 1_000_000_000_000


class Feed:
    def __init__(self, **policy):
        self.engine = GhostTwapEngine(
            run_id="pending-tests", policy=GhostPolicy(enabled=True, **policy)
        )
        self.sequence = 0

    def add(self, kind, source_offset, receipt_offset, value="100"):
        self.sequence += 1
        event = PriceEvent(
            feed=kind,
            value=Decimal(value),
            source_timestamp_ms=BASE + source_offset,
            received_wall_ns=(BASE + receipt_offset) * NS,
            received_monotonic_ns=MONO_ORIGIN + receipt_offset * NS,
            sequence=self.sequence,
            event_id=f"pending:{self.sequence}",
            window_s=60 if kind == "twap" else None,
        )
        self.engine.accept(event)
        return event

    def snapshot(self, offset=2000, name="decision"):
        return self.engine.snapshot(
            decision_id=name,
            decision_wall_ns=(BASE + offset) * NS,
            decision_monotonic_ns=MONO_ORIGIN + offset * NS,
        )


def primed(*, omit=(), **policy):
    feed = Feed(**policy)
    for second in range(-90, 1):
        if second not in omit:
            feed.add("spot", second * 1000, second * 1000 + 1000)
    feed.add("twap", 0, 1500)
    return feed


def slot_at(decision, offset):
    return next(s for s in decision.slots if s.slot_timestamp_ms == BASE + offset)


def horizon(decision, seconds):
    return next(f for f in decision.forecasts if f.horizon_s == seconds)


def test_pending_and_future_are_separate_and_do_not_degrade_complete_history():
    decision = primed().snapshot()
    for offset in (1000, 2000):
        slot = slot_at(decision, offset)
        assert slot.category == "pending"
        assert slot.value == Decimal("100")
        assert slot.input.source_timestamp_ms == BASE
        assert slot.carry_age_ms == offset
    assert slot_at(decision, 3000).category == "future"
    short = horizon(decision, 5)
    assert (short.counts.observed, short.counts.carried, short.counts.pending,
            short.counts.future, short.counts.missing) == (58, 0, 2, 0, 0)
    long = horizon(decision, 30)
    assert (long.counts.observed, long.counts.carried, long.counts.pending,
            long.counts.future, long.counts.missing) == (33, 0, 2, 25, 0)
    assert all(f.price == Decimal("100") and f.quality == "healthy"
               for f in decision.forecasts)
    assert all(f.counts.observed + f.counts.carried + f.counts.pending
               + f.counts.future + f.counts.missing == 60
               for f in decision.forecasts)


def test_interior_missing_second_is_carried_even_when_recent_tail_is_pending():
    decision = primed(omit={-1}).snapshot()
    assert slot_at(decision, -1000).category == "carried"
    assert slot_at(decision, 1000).category == "pending"
    assert horizon(decision, 1).quality == "healthy"  # Window ends before the hole.
    assert horizon(decision, 2).quality == "degraded"
    assert horizon(decision, 5).quality == "degraded"
    assert horizon(decision, 5).counts.pending == 2


def test_latest_receipt_source_regression_does_not_move_frontier_or_change_pending_input():
    feed = primed()
    latest = feed.add("spot", -1000, 1800, "200")
    decision = feed.snapshot()
    assert decision.current_spot == latest
    for offset in (1000, 2000):
        pending = slot_at(decision, offset)
        assert pending.category == "pending"
        assert pending.input.source_timestamp_ms == BASE
        assert pending.value == Decimal("100")
    future = slot_at(decision, 3000)
    assert future.category == "future"
    assert future.input == latest
    assert future.value == Decimal("200")
    # The legitimate revision at -1 s changes one observed slot by +100;
    # neither pending slot may also inherit the latest-receipt price of 200.
    assert horizon(decision, 5).price == Decimal("101.666666666666666667")
    assert horizon(decision, 5).quality == "healthy"


def test_future_at_receipt_input_never_advances_admissible_frontier():
    feed = primed()
    feed.add("spot", 20_000, 1800, "999")
    feed.add("spot", -1000, 1900)
    decision = feed.snapshot()
    assert slot_at(decision, 1000).category == "pending"
    assert slot_at(decision, 2000).category == "pending"
    assert all(f.price == Decimal("100") for f in decision.forecasts)


def test_later_source_makes_a_pending_hole_interior_without_rewriting_old_decision():
    feed = primed()
    old = feed.snapshot()
    old_bytes = old.to_audit_json()
    feed.add("spot", 2000, 2500, "160")
    new = feed.snapshot(2600, "later-source")
    assert slot_at(old, 1000).category == "pending"
    assert slot_at(new, 1000).category == "carried"
    assert slot_at(new, 1000).input == slot_at(old, 1000).input
    assert slot_at(new, 1000).value == slot_at(old, 1000).value
    assert slot_at(new, 2000).category == "observed"
    assert horizon(old, 5).quality == "healthy"
    assert horizon(new, 5).quality == "degraded"
    assert horizon(new, 5).price == Decimal("101")
    assert old.to_audit_json() == old_bytes


def test_arriving_pending_slot_becomes_observed_and_preserves_old_snapshot():
    feed = primed()
    old = feed.snapshot()
    feed.add("spot", 1000, 2300, "160")
    new = feed.snapshot(2500, "pending-arrival")
    assert slot_at(old, 1000).category == "pending"
    assert slot_at(old, 1000).value == Decimal("100")
    assert slot_at(new, 1000).category == "observed"
    assert slot_at(new, 2000).category == "pending"
    assert slot_at(new, 2000).value == Decimal("160")
    assert horizon(new, 5).quality == "healthy"
    assert horizon(new, 5).price == Decimal("102")


def test_pending_label_cannot_mask_stale_current_inputs():
    decision = primed(source_max_age_ms=3000).snapshot(3001)
    assert slot_at(decision, 1000).category == "pending"
    assert all(f.price is None and f.quality == "unavailable" for f in decision.forecasts)


def test_pending_age_still_obeys_inclusive_carry_limit():
    # Relax only current freshness to isolate the unchanged ten-second slot guard.
    decision = primed(source_max_age_ms=20_000, receipt_max_age_ms=20_000).snapshot(11_000)
    edge = slot_at(decision, 10_000)
    assert edge.category == "pending"
    assert edge.carry_age_ms == 10_000
    assert slot_at(decision, 11_000).category == "missing"
    assert horizon(decision, 10).price == Decimal("100")
    assert horizon(decision, 30).price is None


def test_absent_seed_remains_missing_not_pending():
    feed = Feed()
    feed.add("spot", 0, 1000)
    feed.add("twap", 0, 1500)
    decision = feed.snapshot()
    assert slot_at(decision, -1000).category == "missing"
    assert slot_at(decision, 1000).category == "pending"
    assert all(f.price is None for f in decision.forecasts)


def test_spot_gap_recovery_depends_on_horizon_not_a_fixed_62_second_wait():
    feed = primed()
    feed.engine.record_gap(feed="spot", reason="lost-history")
    for second in range(3, 36):
        feed.add("spot", second * 1000, second * 1000 + 1000)
    feed.add("twap", 35_000, 36_500)
    decision = feed.snapshot(37_000)
    # h30 begins at w−32 = +3 s, the first retained post-gap source.
    assert horizon(decision, 30).price == Decimal("100")
    assert horizon(decision, 30).quality == "healthy"
    assert horizon(decision, 30).counts.pending == 2
    assert all(horizon(decision, h).price is None for h in (1, 2, 3, 5, 10))


def test_contract_v3_preserves_pending_counts_and_audit_categories():
    decision = primed().snapshot()
    live = json.loads(decision.to_live_json())
    audit = json.loads(decision.to_audit_json())
    assert live["contract_version"] == audit["contract_version"] == 3
    assert all("pending" in forecast["counts"] for forecast in live["forecasts"])
    assert any(slot["category"] == "pending" for slot in audit["slots"])
    assert isinstance(live["decision_wall_ns"], str)
    assert isinstance(live["forecasts"][0]["price"], str)
