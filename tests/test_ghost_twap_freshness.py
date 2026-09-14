"""Separate source/receipt deadlines without changing the forecast model."""
from dataclasses import asdict
from decimal import Decimal
import json

import pytest

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent


MS = 1_000_000
NOW_MS = 1_800_000_100_000
WALL = NOW_MS * MS
MONO = 1_000_000_000_000


def make_engine(feed, *, source_age_ms, wall_age_ns, mono_age_ns, policy=None):
    engine = GhostTwapEngine("freshness", policy or GhostPolicy(enabled=True))
    sequence = 0

    def add(kind, source_age, wall_age, mono_age, value="100"):
        nonlocal sequence
        sequence += 1
        event = PriceEvent(kind, Decimal(value), NOW_MS-source_age,
            WALL-wall_age, MONO-mono_age, sequence, f"freshness:{sequence}",
            60 if kind == "twap" else None)
        engine.accept(event)
        return event

    for age in range(90_000, 3999, -1000):
        add("spot", age, age * MS, age * MS)
    selected = add(feed, source_age_ms, wall_age_ns, mono_age_ns)
    other = add("twap" if feed == "spot" else "spot", 1000, 500 * MS, 500 * MS)
    return engine, selected, other


def snapshot(engine, *, wall_delta=0, mono_delta=0, name="cut"):
    return engine.snapshot(name, WALL+wall_delta, MONO+mono_delta)


@pytest.mark.parametrize("feed", ["spot", "twap"])
@pytest.mark.parametrize("clock", ["source", "wall_receipt", "monotonic_receipt"])
@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_each_feed_and_clock_has_an_inclusive_nanosecond_boundary(feed, clock, delta):
    age = dict(source_age_ms=4000, wall_age_ns=1000 * MS, mono_age_ns=1000 * MS)
    if clock == "source":
        age["source_age_ms"] = 5000
    elif clock == "wall_receipt":
        age["wall_age_ns"] = 3000 * MS
    else:
        age["mono_age_ns"] = 3000 * MS
    engine, selected, _ = make_engine(feed, **age)
    decision = snapshot(engine, wall_delta=delta if clock != "monotonic_receipt" else 0,
                        mono_delta=delta if clock == "monotonic_receipt" else 0)
    assert getattr(decision, "current_" + feed) == selected
    if delta <= 0:
        assert all(f.price == Decimal("100") for f in decision.forecasts)
        assert not decision.reasons
    else:
        assert decision.reasons == ("stale_" + feed,)
        assert all(f.price is None for f in decision.forecasts)
        assert decision.valid_until_wall_ns <= decision.decision_wall_ns


@pytest.mark.parametrize("feed", ["spot", "twap"])
@pytest.mark.parametrize("clock,wall_age,mono_age,remaining_ms", [
    ("source", 500, 500, 1000),
    ("wall_receipt", 2900, 500, 100),
    ("monotonic_receipt", 500, 2900, 100),
])
def test_valid_until_uses_the_clock_that_expires_first(feed, clock, wall_age, mono_age, remaining_ms):
    engine, _, _ = make_engine(feed, source_age_ms=4000,
                              wall_age_ns=wall_age * MS, mono_age_ns=mono_age * MS)
    decision = snapshot(engine)
    assert all(f.price is not None for f in decision.forecasts)
    assert decision.valid_until_wall_ns == WALL + remaining_ms * MS
    # Repeated snapshots cannot grant a fresh duration to unchanged input clocks.
    later = snapshot(engine, wall_delta=50 * MS, mono_delta=50 * MS, name="later")
    assert later.valid_until_wall_ns == decision.valid_until_wall_ns
    stale = snapshot(engine, wall_delta=remaining_ms * MS + 1,
                     mono_delta=remaining_ms * MS + 1, name="stale")
    assert stale.valid_until_wall_ns == decision.valid_until_wall_ns
    assert all(f.price is None for f in stale.forecasts)


@pytest.mark.parametrize("feed", ["spot", "twap"])
def test_source_relaxation_does_not_relax_either_receipt_age(feed):
    old = GhostPolicy(enabled=True, source_max_age_ms=3000, receipt_max_age_ms=3000)
    new = GhostPolicy(enabled=True)
    kwargs = dict(source_age_ms=4000, wall_age_ns=1000 * MS, mono_age_ns=1000 * MS)
    old_engine, _, _ = make_engine(feed, policy=old, **kwargs)
    new_engine, _, _ = make_engine(feed, policy=new, **kwargs)
    assert all(f.price is None for f in snapshot(old_engine).forecasts)
    assert all(f.price == Decimal("100") for f in snapshot(new_engine).forecasts)
    for clock in ("wall_age_ns", "mono_age_ns"):
        stale = dict(kwargs, **{clock: 3000 * MS + 1})
        engine, _, _ = make_engine(feed, policy=new, **stale)
        assert all(f.price is None for f in snapshot(engine).forecasts)


@pytest.mark.parametrize("feed", ["spot", "twap"])
def test_future_at_original_receipt_never_becomes_valid_by_waiting(feed):
    # Source now lies in the past, but it was 1 ns ahead at its own receipt.
    engine, selected, _ = make_engine(feed, source_age_ms=1000,
        wall_age_ns=1000 * MS + 1, mono_age_ns=1000 * MS)
    decision = snapshot(engine)
    assert "future_" + feed in decision.reasons
    assert all(f.price is None for f in decision.forecasts)
    assert getattr(decision, "current_" + feed) == selected
    assert decision.valid_until_wall_ns <= WALL
    if feed == "spot":
        assert all(slot.input != selected for slot in decision.slots if slot.category != "missing")


@pytest.mark.parametrize("feed", ["spot", "twap"])
@pytest.mark.parametrize("clock", ["received_wall_ns", "received_monotonic_ns"])
def test_future_receipt_or_backdated_cutoff_is_rejected(feed, clock):
    engine, _, _ = make_engine(feed, source_age_ms=4000,
                              wall_age_ns=1000 * MS, mono_age_ns=1000 * MS)
    # A new accepted frame is 1 ns beyond the proposed cutoff in one clock.
    event = PriceEvent(feed, Decimal("100"), NOW_MS,
        WALL + (1 if clock == "received_wall_ns" else 0),
        MONO + (1 if clock == "received_monotonic_ns" else 0), 90, "future-cutoff",
        60 if feed == "twap" else None)
    # Keep acceptance sequence contiguous so the test isolates clock rejection.
    event = PriceEvent(event.feed, event.value, event.source_timestamp_ms,
                       event.received_wall_ns, event.received_monotonic_ns,
                       engine._last_event.sequence + 1, event.event_id, event.window_s)
    engine.accept(event)
    with pytest.raises(ValueError, match="cutoff predates"):
        snapshot(engine)


def test_source_cap_does_not_disable_twap_regression_or_seen_target_protection():
    engine, _, _ = make_engine("twap", source_age_ms=1000,
                              wall_age_ns=1000 * MS, mono_age_ns=1000 * MS)
    event = PriceEvent("twap", Decimal("100"), NOW_MS-2000, WALL-100 * MS,
                       MONO-100 * MS, engine._last_event.sequence + 1, "regression", 60)
    engine.accept(event)
    result = snapshot(engine)
    assert "twap_regression" in result.reasons
    assert "target_already_received" in result.forecasts[0].reasons
    assert all(f.price is None for f in result.forecasts)


@pytest.mark.parametrize("field", ["source_max_age_ms", "receipt_max_age_ms"])
@pytest.mark.parametrize("value", [0, -1, True, "3000", 3.5])
def test_freshness_settings_require_positive_integers(field, value):
    with pytest.raises(ValueError, match=field):
        GhostPolicy(**{field: value})


def test_history_budget_covers_source_age_and_current_policy_is_explicit():
    with pytest.raises(ValueError, match="oldest requested slot"):
        GhostPolicy(source_max_age_ms=5000, history_ms=66_999)
    policy = GhostPolicy(enabled=True, source_max_age_ms=5000,
                         receipt_max_age_ms=3000, history_ms=67_000)
    # Receipt allowance does not move the oldest source-window boundary.
    assert GhostPolicy(source_max_age_ms=5000, receipt_max_age_ms=20_000,
                       history_ms=67_000).history_ms == 67_000
    engine, _, _ = make_engine("spot", source_age_ms=4000,
        wall_age_ns=1000 * MS, mono_age_ns=1000 * MS, policy=policy)
    decision = snapshot(engine)
    assert all(f.price == Decimal("100") for f in decision.forecasts)
    live, audit = json.loads(decision.to_live_json()), json.loads(decision.to_audit_json())
    assert live["contract_version"] == audit["contract_version"] == 3
    assert live["policy"] == audit["policy"] == asdict(policy)
    assert live["policy"]["max_carry_ms"] == 10_000
    assert "current_max_age_ms" not in live["policy"]
    assert live["model_version"] == "chainlink-60s-offset3-v1"
