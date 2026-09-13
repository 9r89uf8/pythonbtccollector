"""Adversarial contract tests for the optional, pure ghost-TWAP engine."""

from dataclasses import FrozenInstanceError
from decimal import Decimal
import json

import pytest

from price_collector.ghost_twap import (
    HORIZONS,
    GhostPolicy,
    GhostTwapEngine,
    PriceEvent,
)


BASE = 1_788_220_800_000
NS_PER_MS = 1_000_000
MONO_ORIGIN = 1_000_000_000_000


def mono_for_wall(wall_ns):
    return MONO_ORIGIN + wall_ns - BASE * NS_PER_MS


class Events:
    def __init__(self, engine):
        self.engine = engine
        self.sequence = 0

    def add(self, feed, source_ms, receipt_ms, value="100"):
        self.sequence += 1
        wall_ns = receipt_ms * NS_PER_MS
        event = PriceEvent(
            feed=feed,
            value=Decimal(value),
            source_timestamp_ms=source_ms,
            received_wall_ns=wall_ns,
            received_monotonic_ns=mono_for_wall(wall_ns),
            sequence=self.sequence,
            event_id=f"edge:{self.sequence}",
            window_s=60 if feed == "twap" else None,
        )
        self.engine.accept(event)
        return event


def engine_and_events(**policy_overrides):
    engine = GhostTwapEngine(
        run_id="edge-run", policy=GhostPolicy(enabled=True, **policy_overrides)
    )
    return engine, Events(engine)


def prime(*, omitted_offsets=(), **policy_overrides):
    engine, events = engine_and_events(**policy_overrides)
    for offset in range(-90, 1):
        if offset not in omitted_offsets:
            source_ms = BASE + offset * 1000
            events.add("spot", source_ms, source_ms + 1000)
    events.add("twap", BASE, BASE + 1500)
    return engine, events


def snapshot(engine, offset_ms=2000, decision_id="decision"):
    wall_ns = (BASE + offset_ms) * NS_PER_MS
    return engine.snapshot(
        decision_id=decision_id,
        decision_wall_ns=wall_ns,
        decision_monotonic_ns=mono_for_wall(wall_ns),
    )


def by_horizon(decision):
    return {forecast.horizon_s: forecast for forecast in decision.forecasts}


def assert_unavailable(decision):
    assert len(decision.forecasts) == 6
    assert all(f.price is None and f.quality == "unavailable" for f in decision.forecasts)


def test_union_is_89_slots_and_each_source_horizon_slices_exactly_60():
    engine, _ = prime()
    decision = snapshot(engine)
    assert tuple(HORIZONS) == (1, 2, 3, 5, 10, 30)
    assert isinstance(decision.slots, tuple)
    assert len(decision.slots) == 89
    assert decision.slots[0].slot_timestamp_ms == BASE - 61_000
    assert decision.slots[-1].slot_timestamp_ms == BASE + 27_000
    expected_counts = {
        1: (60, 0, 0, 0, 0),
        2: (60, 0, 0, 0, 0),
        3: (60, 0, 0, 0, 0),
        5: (58, 0, 2, 0, 0),
        10: (53, 0, 2, 5, 0),
        30: (33, 0, 2, 25, 0),
    }
    for forecast in decision.forecasts:
        assert forecast.slot_start_index == forecast.horizon_s - 1
        slots = decision.slots[forecast.slot_start_index : forecast.slot_start_index + 60]
        assert len(slots) == 60
        target = BASE + forecast.horizon_s * 1000
        assert forecast.target_source_timestamp_ms == target
        assert slots[0].slot_timestamp_ms == target - 62_000
        assert slots[-1].slot_timestamp_ms == target - 3000
        counts = forecast.counts
        assert (counts.observed, counts.carried, counts.pending, counts.future, counts.missing) == expected_counts[forecast.horizon_s]
        assert forecast.price == Decimal("100")


def test_source_past_unreceived_target_remains_valid_even_when_eta_overdue():
    engine, _ = prime()
    forecast = by_horizon(snapshot(engine, 2700))[1]
    assert forecast.target_source_timestamp_ms < BASE + 2700
    assert forecast.price == Decimal("100")
    assert forecast.estimated_arrival_wall_ns == (BASE + 2500) * NS_PER_MS
    assert forecast.estimated_remaining_ns == -200 * NS_PER_MS
    assert forecast.estimate_overdue is True


def test_current_source_age_limit_is_inclusive():
    engine, _ = prime()
    assert all(f.price is not None for f in snapshot(engine, 3000).forecasts)
    assert_unavailable(snapshot(engine, 3001, "expired"))


def test_validity_deadline_clamps_to_remaining_monotonic_freshness():
    engine, _ = prime()
    wall_ns = (BASE + 2500) * NS_PER_MS
    # Latest spot receipt was at +1000 ms: monotonic age is now 2900 ms,
    # although the source/wall deadlines still have 500 ms remaining.
    decision = engine.snapshot(
        decision_id="different-clock-elapsed-times",
        decision_wall_ns=wall_ns,
        decision_monotonic_ns=mono_for_wall((BASE + 3900) * NS_PER_MS),
    )
    assert all(f.price is not None for f in decision.forecasts)
    assert decision.valid_until_wall_ns == (BASE + 2600) * NS_PER_MS


def test_historical_carry_at_ten_seconds_is_usable_but_eleven_is_missing():
    engine, _ = prime(omitted_offsets=set(range(-39, -29)))
    decision = snapshot(engine)
    slot = next(s for s in decision.slots if s.slot_timestamp_ms == BASE - 30_000)
    assert slot.category == "carried"
    assert slot.carry_age_ms == 10_000
    assert by_horizon(decision)[1].price == Decimal("100")
    assert by_horizon(decision)[1].quality == "degraded"

    engine, _ = prime(omitted_offsets=set(range(-39, -28)))
    decision = snapshot(engine)
    slot = next(s for s in decision.slots if s.slot_timestamp_ms == BASE - 29_000)
    assert slot.category == "missing"
    assert slot.value is None
    assert by_horizon(decision)[1].price is None
    assert by_horizon(decision)[1].counts.missing == 1


@pytest.mark.parametrize("source_offset", [-5000, 10_000])
def test_newer_invalid_current_spot_cannot_fall_back_to_older_fresh_one(source_offset):
    engine, events = prime()
    events.add("spot", BASE + source_offset, BASE + 1800, "200")
    assert_unavailable(snapshot(engine))
    events.add("spot", BASE + 1000, BASE + 2200, "300")
    recovered = snapshot(engine, 2300, "recovered")
    assert all(f.price is not None for f in recovered.forecasts)
    assert all(s.value == Decimal("300") for s in recovered.slots if s.category == "future")


def test_twap_regression_requires_strict_advance_and_never_scores_seen_target():
    engine, events = prime()
    events.add("twap", BASE - 2000, BASE + 1800)
    regressed = snapshot(engine)
    assert_unavailable(regressed)
    # The two-second target of that regressed anchor was already received.
    assert by_horizon(regressed)[2].target_source_timestamp_ms == BASE
    events.add("twap", BASE, BASE + 2100)
    assert_unavailable(snapshot(engine, 2200, "equal-high-water"))
    events.add("twap", BASE + 1000, BASE + 2400)
    assert all(f.price is not None for f in snapshot(engine, 2500, "advanced").forecasts)


def test_conflicting_anchor_is_invalid_until_new_clean_advancing_stamp():
    engine, events = prime()
    events.add("twap", BASE, BASE + 1800, "101")
    assert_unavailable(snapshot(engine))
    events.add("twap", BASE + 1000, BASE + 2100)
    assert all(f.price is not None for f in snapshot(engine, 2200, "clean").forecasts)


def test_late_revision_changes_only_new_snapshot_not_frozen_prior_evidence():
    engine, events = prime()
    old = snapshot(engine)
    old_audit = old.to_audit_json()
    old_live = old.to_live_json()
    events.add("spot", BASE - 2000, BASE + 2300, "160")
    events.add("spot", BASE + 1000, BASE + 2400)
    new = snapshot(engine, 2500, "after-revision")
    assert by_horizon(old)[1].price == Decimal("100")
    assert by_horizon(new)[1].price == Decimal("101")
    assert old.to_audit_json() == old_audit
    assert old.to_live_json() == old_live
    assert isinstance(old_audit, bytes) and isinstance(old_live, bytes)
    json.loads(old_audit)
    json.loads(old_live)
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        old.slots[0].value = Decimal("999")
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        old.slots[0].input.value = Decimal("999")


def test_equal_receipt_clocks_use_sequence_to_order_a_spot_revision():
    engine, events = prime()
    events.add("spot", BASE - 2000, BASE + 1800, "160")
    events.add("spot", BASE - 2000, BASE + 1800, "220")
    events.add("spot", BASE + 1000, BASE + 1900)
    decision = snapshot(engine)
    assert by_horizon(decision)[1].price == Decimal("102")


@pytest.mark.parametrize("bad_field", ["sequence", "wall", "monotonic"])
def test_invalid_acceptance_order_raises_and_latches_fault(bad_field):
    engine, events = prime()
    latest_wall = (BASE + 1500) * NS_PER_MS
    next_wall = (BASE + 1800) * NS_PER_MS
    bad = PriceEvent(
        feed="spot", value=Decimal("100"), source_timestamp_ms=BASE + 1000,
        received_wall_ns=latest_wall - 1 if bad_field == "wall" else next_wall,
        received_monotonic_ns=mono_for_wall(latest_wall) - 1 if bad_field == "monotonic" else mono_for_wall(next_wall),
        sequence=events.sequence if bad_field == "sequence" else events.sequence + 1,
        event_id="bad-order",
    )
    with pytest.raises(ValueError):
        engine.accept(bad)
    # Either an explicit snapshot error or an unavailable result is fail-closed.
    try:
        decision = snapshot(engine)
    except ValueError:
        pass
    else:
        assert_unavailable(decision)
    events.sequence += 2
    try:
        events.add("spot", BASE + 1000, BASE + 2200)
    except ValueError:
        pass
    try:
        decision = snapshot(engine, 2300, "still-faulted")
    except ValueError:
        pass
    else:
        assert_unavailable(decision)


@pytest.mark.parametrize("bad_clock", ["wall", "monotonic"])
def test_snapshot_cannot_precede_last_accepted_receipt_on_either_clock(bad_clock):
    engine, _ = prime()
    latest_wall = (BASE + 1500) * NS_PER_MS
    decision_wall = (BASE + 2000) * NS_PER_MS
    with pytest.raises(ValueError):
        engine.snapshot(
            decision_id="retrospective",
            decision_wall_ns=latest_wall - 1 if bad_clock == "wall" else decision_wall,
            decision_monotonic_ns=mono_for_wall(latest_wall) - 1 if bad_clock == "monotonic" else mono_for_wall(decision_wall),
        )


def test_snapshot_cannot_move_back_before_prior_decision():
    engine, _ = prime()
    snapshot(engine, 2200, "later")
    with pytest.raises(ValueError):
        snapshot(engine, 2100, "earlier")


def test_spot_gap_discards_coverage_and_rewarms_from_new_events():
    engine, events = prime()
    engine.record_gap(feed="spot", reason="test-input-loss")
    assert_unavailable(snapshot(engine))
    for offset in range(3, 71):
        source_ms = BASE + offset * 1000
        events.add("spot", source_ms, source_ms + 1000)
    events.add("twap", BASE + 70_000, BASE + 71_500)
    assert all(f.price is not None for f in snapshot(engine, 72_000, "rewarmed").forecasts)


def test_twap_gap_requires_new_advancing_anchor():
    engine, events = prime()
    engine.record_gap(feed="twap", reason="test-reconnect")
    assert_unavailable(snapshot(engine))
    events.add("twap", BASE, BASE + 2100)
    assert_unavailable(snapshot(engine, 2200, "same-anchor"))
    events.add("twap", BASE + 1000, BASE + 2400)
    assert all(f.price is not None for f in snapshot(engine, 2500, "new-anchor").forecasts)


def test_last_gap_metadata_is_bounded_and_prior_snapshots_remain_immutable():
    engine, events = prime()
    engine.record_gap(feed="spot", reason="first-spot-loss")
    old = snapshot(engine)
    old_bytes = old.to_audit_json()
    assert isinstance(old.last_gaps, tuple)
    assert len(old.last_gaps) == 1
    first = old.last_gaps[0]
    assert (first.feed, first.reason, first.ordinal, first.after_sequence) == (
        "spot", "first-spot-loss", 1, events.sequence
    )
    engine.record_gap(feed="twap", reason="twap-reconnect")
    engine.record_gap(feed="spot", reason="second-spot-loss")
    new = snapshot(engine, 2100, "later-gaps")
    assert len(new.last_gaps) == 2
    markers = {marker.feed: marker for marker in new.last_gaps}
    assert markers["spot"].reason == "second-spot-loss"
    assert markers["spot"].ordinal == 3
    assert markers["twap"].reason == "twap-reconnect"
    assert markers["twap"].ordinal == 2
    assert all(marker.after_sequence == events.sequence for marker in new.last_gaps)
    assert old.to_audit_json() == old_bytes
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        first.reason = "rewritten"


def test_capacity_loss_clears_coverage_but_can_rewarm_without_a_new_engine():
    engine, events = engine_and_events(max_events=70)
    for offset in range(71):
        source_ms = BASE + offset * 1000
        events.add("spot", source_ms, source_ms + 1000)
        assert engine.history_size <= 70
    assert_unavailable(snapshot(engine, 71_500, "capacity-loss"))
    for offset in range(71, 139):
        source_ms = BASE + offset * 1000
        events.add("spot", source_ms, source_ms + 1000)
        assert engine.history_size <= 70
    events.add("twap", BASE + 138_000, BASE + 139_500)
    recovered = snapshot(engine, 140_000, "capacity-recovery")
    assert engine.history_size <= 70
    assert all(f.price is not None for f in recovered.forecasts)


def test_missing_global_input_sequence_invalidates_both_feeds_then_rewarms():
    engine, events = prime()
    before = snapshot(engine, 1600, "before-sequence-loss")
    before_bytes = before.to_audit_json()
    last_sequence = events.sequence
    events.sequence += 1  # One canonical accepted ghost input never arrived.
    events.add("spot", BASE + 1000, BASE + 1800)
    lost = snapshot(engine, 2000, "sequence-loss")
    assert_unavailable(lost)
    assert lost.current_twap is None
    assert engine.history_size == 1
    markers = {marker.feed: marker for marker in lost.last_gaps}
    assert set(markers) == {"spot", "twap"}
    assert all(marker.reason == "sequence_gap" for marker in markers.values())
    assert all(marker.after_sequence == last_sequence for marker in markers.values())
    assert before.to_audit_json() == before_bytes
    for offset in range(2, 71):
        source_ms = BASE + offset * 1000
        events.add("spot", source_ms, source_ms + 1000)
    events.add("twap", BASE + 70_000, BASE + 71_500)
    assert all(f.price is not None for f in snapshot(engine, 72_000, "sequence-recovery").forecasts)


def test_history_remains_bounded_under_long_running_input():
    engine, events = engine_and_events(max_events=128)
    for offset in range(301):
        source_ms = BASE + offset * 1000
        events.add("spot", source_ms, source_ms + 1000)
        assert engine.history_size <= 128
    events.add("twap", BASE + 300_000, BASE + 301_500)
    assert all(f.price is not None for f in snapshot(engine, 302_000, "bounded").forecasts)


def test_default_disabled_engine_never_produces_forecasts():
    engine = GhostTwapEngine(run_id="disabled", policy=GhostPolicy())
    events = Events(engine)
    events.add("spot", BASE, BASE + 1000)
    events.add("twap", BASE, BASE + 1500)
    assert_unavailable(snapshot(engine))


def test_engine_rejects_a_non_policy_object_at_construction():
    with pytest.raises((TypeError, ValueError)):
        GhostTwapEngine(run_id="wrong-policy", policy=object())
