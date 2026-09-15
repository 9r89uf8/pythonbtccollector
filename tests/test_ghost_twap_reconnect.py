"""Pure observable tests of bounded spot connection-end recovery."""
from decimal import Decimal
import json

import pytest

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent


BASE = 1_788_220_800_000
NS = 1_000_000
MONO = 1_000_000_000_000


class Feed:
    def __init__(self, **policy):
        self.engine = GhostTwapEngine('reconnect-tests', GhostPolicy(enabled=True, **policy))
        self.sequence = 0

    def add(self, feed, source_ms, receipt_ms, value='100', *, mono_ms=None,
            wall_extra_ns=0, mono_extra_ns=0):
        self.sequence += 1
        event = PriceEvent(feed, Decimal(value), BASE+source_ms,
            (BASE+receipt_ms)*NS+wall_extra_ns,
            MONO+(receipt_ms if mono_ms is None else mono_ms)*NS+mono_extra_ns,
            self.sequence, f'reconnect:{self.sequence}', 60 if feed == 'twap' else None)
        self.engine.accept(event)
        return event

    def snapshot(self, wall_ms=2000, *, mono_ms=None, wall_extra_ns=0, mono_extra_ns=0,
                 name='decision'):
        return self.engine.snapshot(name, (BASE+wall_ms)*NS+wall_extra_ns,
            MONO+(wall_ms if mono_ms is None else mono_ms)*NS+mono_extra_ns)

    def gap(self, wall_ms=2000, *, mono_ms=None, reason='connection_end'):
        self.engine.record_gap('spot', reason, observed_wall_ns=(BASE+wall_ms)*NS,
            observed_monotonic_ns=MONO+(wall_ms if mono_ms is None else mono_ms)*NS)


def primed(**policy):
    feed = Feed(**policy)
    for second in range(-90, 1):
        feed.add('spot', second*1000, second*1000+1000)
    feed.add('twap', 0, 1500)
    return feed


def metadata(decision):
    return json.loads(decision.to_audit_json())['spot_reconnect']


def horizon(decision, seconds=5):
    return next(f for f in decision.forecasts if f.horizon_s == seconds)


def old_history_visible(decision):
    return any(s.input is not None and s.input.source_timestamp_ms < BASE
               and s.input.received_wall_ns <= (BASE+1500)*NS for s in decision.slots)


def advance_anchor(feed, source_ms=4000, receipt_ms=5500, decision_ms=6000):
    feed.add('twap', source_ms, receipt_ms)
    return feed.snapshot(decision_ms, name='after-reconnect')


def test_short_reconnect_preserves_exact_history_but_waits_for_new_spot():
    feed = primed()
    before = feed.snapshot(name='before')
    frozen_before = before.to_audit_json()
    feed.gap()
    waiting = feed.snapshot(name='waiting')
    frozen_waiting = waiting.to_audit_json()
    assert metadata(waiting)['status'] == 'waiting'
    assert waiting.current_spot is None and old_history_visible(waiting)
    assert all(f.price is None for f in waiting.forecasts)
    post = feed.add('spot', 4000, 5000, '160')
    recovered = advance_anchor(feed)
    state = metadata(recovered)
    assert state['status'] == 'retained'
    assert state['previous_spot']['source_timestamp_ms'] == BASE
    assert state['first_post_gap_spot']['sequence'] == post.sequence
    assert old_history_visible(recovered)
    f = horizon(recovered)
    assert f.price == Decimal('103.000000000000000000')
    assert f.quality == 'degraded'
    assert (f.counts.observed, f.counts.carried, f.counts.pending,
            f.counts.future, f.counts.missing) == (55, 3, 2, 0, 0)
    bridge = [s for s in recovered.slots if BASE+1000 <= s.slot_timestamp_ms <= BASE+3000]
    assert len(bridge) == 3
    assert all(s.category == 'carried' and s.value == Decimal('100')
               and s.input.source_timestamp_ms == BASE for s in bridge)
    assert [s.carry_age_ms for s in bridge] == [1000, 2000, 3000]
    assert before.to_audit_json() == frozen_before
    assert waiting.to_audit_json() == frozen_waiting
    assert metadata(waiting)['first_post_gap_spot'] is None


@pytest.mark.parametrize('source_ms,wall_extra,mono_extra,retained', [
    (10000, 0, 0, True),
    (11000, 0, 0, False),
    (10000, 1, 0, False),
    (10000, 0, 1, False),
])
def test_source_wall_and_monotonic_bounds_are_independently_inclusive(source_ms, wall_extra, mono_extra, retained):
    feed = primed()
    feed.gap()
    feed.add('spot', source_ms, 11000, wall_extra_ns=wall_extra, mono_extra_ns=mono_extra)
    decision = advance_anchor(feed, source_ms=10000, receipt_ms=11500, decision_ms=12000)
    assert metadata(decision)['status'] == ('retained' if retained else 'cleared')
    assert old_history_visible(decision) is retained
    assert (horizon(decision).price is not None) is retained


@pytest.mark.parametrize('source_ms,receipt_ms,retained', [(3000, 4000, True), (4000, 5000, False)])
def test_smaller_carry_bound_also_limits_reconnect(source_ms, receipt_ms, retained):
    feed = primed(max_carry_ms=3000, spot_reconnect_max_gap_ms=10000)
    feed.gap()
    feed.add('spot', source_ms, receipt_ms)
    decision = advance_anchor(feed, source_ms=source_ms, receipt_ms=receipt_ms+500, decision_ms=receipt_ms+1000)
    assert metadata(decision)['status'] == ('retained' if retained else 'cleared')
    assert old_history_visible(decision) is retained


@pytest.mark.parametrize('source_ms,receipt_ms', [
    (0, 2500),       # Duplicate source stamp, despite a fresh new receipt.
    (-1000, 2500),   # Source regression.
    (6000, 5000),    # Future source at its own receipt.
    (1000, 7000),    # Stale at first receipt, despite a short receipt gap.
])
def test_first_inadmissible_spot_cancels_retention_and_later_good_tick_cannot_revive_it(source_ms, receipt_ms):
    feed = primed()
    feed.gap()
    rejected = feed.add('spot', source_ms, receipt_ms)
    feed.add('spot', 7000, 8000)
    decision = advance_anchor(feed, source_ms=7000, receipt_ms=8500, decision_ms=9000)
    assert metadata(decision)['status'] == 'cleared'
    assert metadata(decision)['first_post_gap_spot']['sequence'] == rejected.sequence
    assert not old_history_visible(decision)
    assert all(f.price is None and f.counts.missing > 0 for f in decision.forecasts)


@pytest.mark.parametrize('clock', ['wall', 'monotonic'])
def test_waiting_expires_one_nanosecond_after_either_receipt_bound(clock):
    feed = primed()
    feed.gap()
    at_limit = feed.snapshot(11000, name='inclusive-limit')
    assert metadata(at_limit)['status'] == 'waiting' and old_history_visible(at_limit)
    expired = feed.snapshot(11000, wall_extra_ns=int(clock == 'wall'),
        mono_extra_ns=int(clock == 'monotonic'), name='expired')
    assert metadata(expired)['status'] == 'cleared'
    assert not old_history_visible(expired)
    assert metadata(at_limit)['status'] == 'waiting'


@pytest.mark.parametrize('kind', ['disabled', 'no_clocks', 'wall_only', 'mono_only'])
def test_disabled_or_unclocked_gap_uses_existing_hard_clear(kind):
    feed = primed(**({'spot_reconnect_max_gap_ms': 0} if kind == 'disabled' else {}))
    clocks = {}
    if kind in ('disabled', 'wall_only'):
        clocks['observed_wall_ns'] = (BASE+2000)*NS
    if kind in ('disabled', 'mono_only'):
        clocks['observed_monotonic_ns'] = MONO+2000*NS
    feed.engine.record_gap('spot', 'connection_end', **clocks)
    decision = feed.snapshot()
    assert not old_history_visible(decision)
    assert all(f.price is None for f in decision.forecasts)


@pytest.mark.parametrize('bad', [-1, 10001, True, 1.5, '10000'])
def test_reconnect_policy_is_a_bounded_integer(bad):
    with pytest.raises((TypeError, ValueError)):
        GhostPolicy(spot_reconnect_max_gap_ms=bad)


def test_source_regressed_pre_gap_current_is_not_a_valid_retention_boundary():
    feed = primed()
    feed.add('spot', -1000, 1800, '160')
    feed.gap()
    feed.add('spot', 4000, 5000)
    decision = advance_anchor(feed)
    assert not old_history_visible(decision)
    assert all(f.price is None for f in decision.forecasts)


def test_future_history_cannot_poison_admissible_frontier():
    feed = primed()
    feed.add('spot', 50000, 1800, '999')
    feed.add('spot', 1000, 1900)
    feed.gap()
    feed.add('spot', 4000, 5000)
    decision = advance_anchor(feed)
    assert metadata(decision)['status'] == 'retained'
    assert metadata(decision)['previous_spot']['source_timestamp_ms'] == BASE+1000
    assert horizon(decision).price == Decimal('100')
    assert all(s.value != Decimal('999') for s in decision.slots)


@pytest.mark.parametrize('reason', ['connection_end', 'input_overflow', 'invalid_ghost_input'])
def test_repeated_connection_end_or_hard_gap_cancels_pending_retention(reason):
    feed = primed()
    feed.gap()
    waiting = feed.snapshot()
    feed.gap(2500, reason=reason)
    feed.add('spot', 4000, 5000)
    decision = advance_anchor(feed)
    assert not old_history_visible(decision)
    assert all(f.price is None for f in decision.forecasts)
    assert metadata(waiting)['status'] == 'waiting'


def test_sequence_loss_cannot_restore_retained_pre_gap_prices():
    feed = primed()
    feed.gap()
    feed.sequence += 1
    feed.add('spot', 4000, 5000)
    decision = advance_anchor(feed)
    assert not old_history_visible(decision)
    assert all(f.price is None for f in decision.forecasts)
    assert any(marker.reason == 'sequence_gap' for marker in decision.last_gaps)


def test_backlogged_first_arrival_remains_a_latched_causality_fault():
    feed = primed()
    feed.gap()
    feed.snapshot()
    with pytest.raises(ValueError):
        feed.add('spot', 1000, 1999)
    feed.add('spot', 4000, 5000)
    decision = advance_anchor(feed)
    assert all(f.price is None for f in decision.forecasts)
    assert 'backlogged_event' in decision.reasons


def test_recovery_does_not_refresh_a_stale_twap_anchor():
    feed = primed()
    feed.gap()
    feed.add('spot', 4000, 5000)
    decision = feed.snapshot(6000)
    assert metadata(decision)['status'] == 'retained'
    assert old_history_visible(decision)
    assert 'stale_twap' in decision.reasons
    assert all(f.price is None for f in decision.forecasts)


@pytest.mark.parametrize('gap_ms,retained', [(4000, True), (4001, False)])
def test_pre_gap_current_receipt_freshness_is_inclusive(gap_ms, retained):
    feed = primed()
    feed.gap(gap_ms)
    waiting = feed.snapshot(gap_ms)
    assert old_history_visible(waiting) is retained
    assert metadata(waiting)['status'] == ('waiting' if retained else 'cleared')
    assert waiting.current_spot is None


@pytest.mark.parametrize('wall_ms,mono_ms', [(1499, 2000), (2000, 1499)])
def test_gap_clock_regression_is_a_latched_fault(wall_ms, mono_ms):
    feed = primed()
    with pytest.raises(ValueError):
        feed.gap(wall_ms, mono_ms=mono_ms)
    feed.add('spot', 4000, 5000)
    decision = advance_anchor(feed)
    assert 'gap_order' in decision.reasons
    assert all(f.price is None for f in decision.forecasts)


def test_waiting_snapshot_cannot_precede_explicit_gap_clocks():
    feed = primed()
    feed.gap(2500)
    with pytest.raises(ValueError):
        feed.snapshot(2400)
    valid = feed.snapshot(2600)
    assert metadata(valid)['status'] == 'waiting'
    assert all(f.price is None for f in valid.forecasts)
