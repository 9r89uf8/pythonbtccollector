"""Independent clock, admission, publication and recovery fault regressions."""
import asyncio
from copy import deepcopy
from decimal import Decimal
import json
import threading

import pytest

from price_collector.ghost_twap import NS_PER_MS, NS_PER_SECOND, PriceEvent
from price_collector.ghost_twap_runtime import (
    CANARY_MS, MATCH_NS, MAX_ROWS, RESERVE_BYTES, STOP_BYTES,
    GhostRuntime, GhostSettings,
)
from price_collector.ghost_twap_spool import GhostSpool

BASE = 1800000000000


@pytest.fixture(autouse=True)
def same_loop_for_runtime_construction_and_io():
    # Python 3.9 binds asyncio primitives during construction, unlike newer
    # interpreters. Exercise the runtime on the single loop it is designed for.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.run_until_complete(loop.shutdown_default_executor())
    loop.close()
    asyncio.set_event_loop(None)


def run(coroutine):
    return asyncio.get_event_loop().run_until_complete(coroutine)


class Clock:
    def __init__(self):
        self.wall = (BASE + 100000) * NS_PER_MS
        self.mono = 100 * NS_PER_SECOND
    def advance(self, ns):
        self.wall += ns
        self.mono += ns


class Spool:
    def __init__(self):
        self.rows = {}
        self.saved_campaign = None
        self.failure = None
        self.before_write = None
        self.opened = False
    def open(self):
        self.opened = True
    def close(self):
        self.opened = False
    def campaign(self, start):
        if self.saved_campaign is None:
            self.saved_campaign = dict(start_ms=start, last_wall_ms=start, stop_reason=None)
        return deepcopy(self.saved_campaign)
    def save_campaign(self, state):
        self.saved_campaign = deepcopy(state)
    def write(self, record):
        if self.before_write:
            self.before_write()
        if self.failure:
            raise self.failure
        self.rows[(record["run_id"], record["decision_id"])] = deepcopy(record)
    def read_all(self):
        return deepcopy(list(self.rows.values()))
    def remove(self, record):
        self.rows.pop((record["run_id"], record["decision_id"]), None)


class Store:
    def __init__(self):
        self.persisted = []
        self.incomplete = []
        self.late_calls = []
        self.measurement = dict(relation_bytes=0, row_count=0,
                                tablespaces=["pg_default"], db_data_directory=None)
        self.failure = None
    async def initialize(self):
        return {"row_count": self.measurement["row_count"]}
    async def list_incomplete(self, *, limit, after=None):
        return deepcopy(self.incomplete if after is None else [])
    async def measure(self):
        if self.failure:
            raise self.failure
        return deepcopy(self.measurement)
    async def persist(self, record):
        if self.failure:
            raise self.failure
        self.persisted.append(deepcopy(record))
        return {"version": record["version"], "outcome": "inserted"}
    async def get_record(self, run_id, decision_id):
        for record in reversed(self.persisted):
            if (record["run_id"], record["decision_id"]) == (run_id, decision_id):
                return deepcopy(record)
        return None
    async def note_late_target(self, event, *, after=None, limit=100):
        self.late_calls.append((deepcopy(event), after))
        return {"next_after": None}


class Redis:
    def __init__(self):
        self.calls = []
        self.hook = None
        self.failure = None
    async def eval(self, *args):
        self.calls.append(args)
        if self.hook:
            self.hook()
        if self.failure:
            raise self.failure
        return 1


def runtime(**settings):
    clock, spool, store, redis = Clock(), Spool(), Store(), Redis()
    config = GhostSettings(enabled=True, canary_start_ms=BASE, **settings)
    value = GhostRuntime(config, store, redis, spool, wall_ns=lambda: clock.wall,
                         mono_ns=lambda: clock.mono, disk_free=lambda: 2 * RESERVE_BYTES)
    value.campaign = spool.campaign(BASE)
    value.guard = dict(store.measurement, free_bytes=2 * RESERVE_BYTES, monotonic_ns=clock.mono)
    value._end_mono = clock.mono + CANARY_MS * NS_PER_MS
    return value, clock, spool, store, redis


def warm(value):
    for second in range(39, 101):
        value.offer_price("spot", Decimal("100"), BASE + second * 1000,
                          (BASE + second * 1000) * NS_PER_MS, second * NS_PER_SECOND,
                          "spot-" + str(second))
        value.drain_inputs()
    value.offer_price("twap", Decimal("100"), BASE + 100000,
                      (BASE + 100000) * NS_PER_MS, 100 * NS_PER_SECOND, "anchor", 60)


def issue(value):
    warm(value)
    row = value.issue()
    assert row is not None
    assert all(f.price == Decimal("100") for f in row.decision.forecasts)
    return row


def target(value, row, clock, horizon=1, price="101", identity="target"):
    return PriceEvent("twap", Decimal(price),
                      row.state["targets"][str(horizon)]["target_source_timestamp_ms"],
                      clock.wall, clock.mono, value._sequence + 1, identity, 60)


def test_issue_drains_all_constituents_before_clocked_snapshot():
    value, clock, _, _, _ = runtime()
    warm(value)
    value.offer_price("spot", Decimal("160"), BASE + 100000,
                      clock.wall, clock.mono, "last-revision")
    row = value.issue()
    assert not value.queue
    assert row.decision.included_sequence == value._sequence
    assert row.decision.current_spot.event_id == "last-revision"
    assert row.decision.forecasts[3].price == Decimal("103")


def test_backlogged_admission_after_decision_stops_new_work():
    value, clock, _, _, _ = runtime()
    issue(value)
    value.offer_price("spot", Decimal("100"), BASE + 99000,
                      clock.wall - 1, clock.mono - 1, "backlogged")
    assert value.issue() is None
    assert value.stop_reason == "receipt_order_fault"


def test_backlogged_target_offer_blocks_publication_before_next_drain():
    value, clock, _, _, redis = runtime()
    row = issue(value)
    event = target(value, row, clock)
    value.offer_price(event.feed, event.value, event.source_timestamp_ms,
                      clock.wall - 1, clock.mono - 1, event.event_id, 60)
    run(value.publish(row))
    assert redis.calls == []
    assert value.stop_reason == "receipt_order_fault"


def test_constituent_overflow_is_bounded_and_invalidates_coverage():
    value, clock, _, _, _ = runtime(input_queue_max=16)
    issue(value)
    for index in range(17):
        value.offer_price("spot", Decimal("100"), BASE + 100000,
                          clock.wall, clock.mono, "burst-" + str(index))
    assert len(value.queue) <= 16
    assert value.counters["input_drops"] == 16
    row = value.issue()
    assert row is not None
    assert all(f.price is None for f in row.decision.forecasts)
    assert row.decision.gap_count >= 2


def test_target_matching_survives_full_constituent_queue():
    value, clock, _, _, _ = runtime(input_queue_max=16)
    row = issue(value)
    for index in range(16):
        value.offer_price("spot", Decimal("100"), BASE + 100000,
                          clock.wall, clock.mono, "burst-" + str(index))
    clock.advance(NS_PER_SECOND)
    event = target(value, row, clock)
    value.offer_price(event.feed, event.value, event.source_timestamp_ms,
                      event.received_wall_ns, event.received_monotonic_ns, event.event_id, 60)
    assert row.state["targets"]["1"]["first_event"]["event_id"] == "target"
    assert len(value.queue) <= 16


def test_first_target_and_frozen_price_remain_immutable_after_conflict():
    value, clock, _, _, _ = runtime()
    row = issue(value)
    frozen = row.frozen_json
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock))
    first = deepcopy(row.state["targets"]["1"]["first_event"])
    clock.advance(1)
    value.observe_target(target(value, row, clock, price="102", identity="conflict"))
    result = row.state["targets"]["1"]
    assert result["first_event"] == first
    assert result["error"] == "-1.000000000000000000"
    assert result["conflicted"]
    assert result["confirmed_redis_lead_ns"] is None
    assert row.frozen_json == frozen


@pytest.mark.parametrize("delay", [0, 1])
def test_target_admitted_at_or_after_cutoff_preempts_publication(delay):
    value, clock, _, _, redis = runtime()
    row = issue(value)
    clock.advance(delay)
    value.observe_target(target(value, row, clock))
    run(value.publish(row))
    assert redis.calls == []
    assert row.state["targets"]["1"]["confirmed_redis_lead_ns"] is None


def test_late_target_is_not_scored_when_expiry_timer_has_not_run():
    value, clock, _, _, _ = runtime()
    row = issue(value)
    clock.advance(MATCH_NS + 1)
    value.observe_target(target(value, row, clock))
    result = row.state["targets"]["1"]
    assert result["status"] == "missing"
    assert result["first_event"] is None
    assert result["first_late_event"]["event_id"] == "target"
    assert result.get("error") is None


def test_exact_matching_deadline_is_an_explicit_boundary():
    value, clock, _, _, _ = runtime()
    row = issue(value)
    clock.advance(MATCH_NS - 1)
    value.finalize_due()
    assert not row.terminal
    clock.advance(1)
    value.finalize_due()
    assert row.terminal
    assert all(t["status"] == "missing" for t in row.state["targets"].values())


@pytest.mark.parametrize("timer_first", [False, True])
def test_matching_deadline_does_not_depend_on_callback_order(timer_first):
    value, clock, _, _, _ = runtime()
    row = issue(value)
    clock.advance(MATCH_NS)
    if timer_first:
        value.finalize_due()
    value.observe_target(target(value, row, clock))
    result = row.state["targets"]["1"]
    assert result["status"] == "missing"
    assert result["first_event"] is None
    assert result["first_late_event"]["event_id"] == "target"


def test_outbox_is_durable_before_redis_and_payload_preserves_decimal():
    value, clock, spool, _, redis = runtime()
    row = issue(value)
    def check_intent():
        saved = spool.rows[(row.decision.run_id, row.decision.decision_id)]
        assert saved["frozen_json"] == row.frozen_json
        assert json.loads(saved["state_json"])["publication"]["status"] == "intent"
    redis.hook = check_intent
    run(value.publish(row))
    assert row.state["publication"]["status"] == "acknowledged"
    args = redis.calls[0]
    body = json.loads(args[-2])
    assert body["forecasts"][0]["price"] == "100.000000000000000000"
    assert args[-1] == 3000
    assert isinstance(body["decision_wall_ns"], str)


def test_outbox_failure_never_touches_redis():
    value, _, spool, _, redis = runtime()
    row = issue(value)
    spool.failure = OSError("disk full")
    with pytest.raises(OSError):
        run(value.publish(row))
    assert redis.calls == []


def test_expiry_during_fsync_suppresses_old_publication():
    value, clock, spool, _, redis = runtime()
    row = issue(value)
    spool.before_write = lambda: clock.advance(3 * NS_PER_SECOND)
    run(value.publish(row))
    assert redis.calls == []
    assert row.state["publication"]["status"] == "preempted_after_spool"


def test_monotonic_ttl_keeps_original_input_expiry_despite_slow_wall_clock():
    value, clock, _, _, redis = runtime()
    warm(value)
    clock.advance(2 * NS_PER_SECOND)
    row = value.issue()
    assert row.decision.valid_until_wall_ns - row.decision.decision_wall_ns == NS_PER_SECOND
    # Only 100ms of wall time passed, but 900ms of the original 1s allowance
    # passed monotonically. A fresh 3s allowance at publication would be wrong.
    clock.wall += 100 * NS_PER_MS
    clock.mono += 900 * NS_PER_MS
    run(value.publish(row))
    assert redis.calls[0][-1] == 100


def test_guard_becoming_stale_during_fsync_prevents_publication():
    value, clock, spool, _, redis = runtime()
    row = issue(value)
    value.guard["monotonic_ns"] = clock.mono - 2900 * NS_PER_MS
    spool.before_write = lambda: clock.advance(101 * NS_PER_MS)
    run(value.publish(row))
    assert redis.calls == []
    assert value.stop_reason == "stale_guard"


def test_target_during_fsync_suppresses_publication():
    value, clock, spool, _, redis = runtime()
    row = issue(value)
    def receive():
        clock.advance(1)
        value.observe_target(target(value, row, clock))
    spool.before_write = receive
    run(value.publish(row))
    assert redis.calls == []


@pytest.mark.parametrize("advance_after_target", [0, 1])
def test_target_between_attempt_and_ack_or_tied_with_ack_never_earns_lead(advance_after_target):
    value, clock, _, _, redis = runtime()
    row = issue(value)
    def receive():
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock))
        clock.advance(advance_after_target)
    redis.hook = receive
    run(value.publish(row))
    assert row.state["publication"]["status"] == "acknowledged"
    assert row.state["targets"]["1"]["confirmed_redis_lead_ns"] is None


def test_only_ack_strictly_before_receipt_earns_exact_lead():
    value, clock, _, _, _ = runtime()
    row = issue(value)
    run(value.publish(row))
    clock.advance(NS_PER_SECOND + 7)
    value.observe_target(target(value, row, clock))
    assert row.state["targets"]["1"]["confirmed_redis_lead_ns"] == NS_PER_SECOND + 7


def test_future_source_target_is_preserved_without_confirmed_lead():
    value, clock, _, _, _ = runtime()
    row = issue(value)
    run(value.publish(row))
    clock.advance(500 * NS_PER_MS)
    event = target(value, row, clock)  # target stamp is still 500ms in the future
    value.offer_price(event.feed, event.value, event.source_timestamp_ms,
                      event.received_wall_ns, event.received_monotonic_ns, event.event_id, 60)
    result = row.state["targets"]["1"]
    assert result["first_event"]["event_id"] == event.event_id
    assert result["confirmed_redis_lead_ns"] is None


@pytest.mark.parametrize("earlier_stop", [None, "audit_persistence_failure"])
def test_admission_fault_cannot_be_repaired_by_a_later_target_for_lead_scoring(earlier_stop):
    value, clock, _, _, _ = runtime()
    row = issue(value)
    run(value.publish(row))
    if earlier_stop:
        value.stop(earlier_stop)
    value.offer_price("spot", Decimal("100"), BASE + 99000,
                      clock.wall - 1, clock.mono - 1, "lost-before-decision")
    assert value.stop_reason == (earlier_stop or "receipt_order_fault")
    clock.advance(NS_PER_SECOND)
    event = target(value, row, clock)
    value.offer_price(event.feed, event.value, event.source_timestamp_ms,
                      event.received_wall_ns, event.received_monotonic_ns, event.event_id, 60)
    assert row.state["targets"]["1"]["first_event"]["event_id"] == "target"
    assert row.state["targets"]["1"]["confirmed_redis_lead_ns"] is None


@pytest.mark.parametrize("field,bad", [("event_id", "event-\u2603"),
                                      ("received_wall_ns", 2 ** 63),
                                      ("received_mono_ns", 2 ** 63)])
def test_invalid_identity_or_bigint_clock_marks_lost_coverage(field, bad):
    value, clock, _, _, _ = runtime()
    issue(value)
    arguments = dict(feed="spot", value=Decimal("100"), source_ms=BASE + 100000,
                     received_wall_ns=clock.wall, received_mono_ns=clock.mono,
                     event_id="canonical-event")
    arguments[field] = bad
    value.offer_price(**arguments)
    assert value.counters["invalid_inputs"] == 1
    row = value.issue()
    assert row is not None
    assert all(f.price is None for f in row.decision.forecasts)
    assert row.decision.gap_count >= 1


def test_redis_uncertain_outcome_does_not_earn_lead_or_drop_audit():
    value, clock, spool, _, redis = runtime()
    row = issue(value)
    redis.failure = TimeoutError("ack lost after possible publication")
    run(value.publish(row))
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock))
    assert row.state["publication"]["status"] == "uncertain"
    assert row.state["targets"]["1"]["confirmed_redis_lead_ns"] is None
    assert spool.rows


def test_new_decision_coalesces_publication_but_keeps_previous_audit():
    value, clock, _, _, _ = runtime()
    first = issue(value)
    clock.advance(1)
    second = value.issue()
    assert len(value.records) == 2
    assert first.state["publication"]["status"] == "coalesced_before_publication"
    assert value._pending_publication == second.decision.decision_id
    assert first.decision.decision_id in value._dirty


def test_audit_reservations_remain_bounded_until_targets_terminalize():
    value, clock, _, _, _ = runtime(audit_max_records=8)
    issue(value)
    for _ in range(7):
        clock.advance(1)
        assert value.issue() is not None
    assert value.issue() is None
    assert len(value.records) == 8
    assert value.counters["audit_admission_pauses"] == 1
    assert value.stop_reason is None


def test_late_event_queue_never_grows_past_its_cap_when_audit_is_stalled():
    value, clock, _, _, _ = runtime(input_queue_max=16)
    for index in range(30):
        value.offer_price("twap", Decimal("100"), BASE + 100000,
                          clock.wall, clock.mono, "event-" + str(index), 60)
    assert len(value.late_events) == 16
    assert len(value.queue) <= 16
    assert value.stop_reason == "late_target_queue_full"
    assert value.counters["late_audit_drops"] == 14


def test_oversized_frozen_record_stops_admission_before_publication():
    value, clock, _, _, redis = runtime(record_max_bytes=32768)
    warm(value)
    # The operational evidence ring is bounded, but a maximally verbose ring
    # can exceed a smaller configured per-record budget. Fail closed, retaining
    # the explicit stop, rather than stripping evidence to make it fit.
    for index in range(128):
        value.gaps.append(dict(feed="spot", reason="x" * 256,
                               after_sequence=index, observed_wall_ns=clock.wall,
                               observed_monotonic_ns=clock.mono))
    assert value.issue() is None
    assert value.stop_reason == "record_size_cap"
    assert len(value.gaps) == 128
    assert not value.records
    assert not redis.calls


@pytest.mark.parametrize("fault", ["stale", "rows", "relation", "free", "error"])
def test_guard_failures_block_only_optional_admission(fault):
    value, clock, _, store, _ = runtime()
    if fault == "stale":
        value.guard["monotonic_ns"] = clock.mono - 3 * NS_PER_SECOND - 1
    else:
        if fault == "rows":
            store.measurement["row_count"] = MAX_ROWS
        elif fault == "relation":
            store.measurement["relation_bytes"] = STOP_BYTES
        elif fault == "free":
            value.disk_free = lambda: RESERVE_BYTES - 1
        else:
            store.failure = RuntimeError("database guard unavailable")
        run(value.refresh_guard())
    assert value.issue() is None
    assert value.stop_reason is not None
    value.offer_price("spot", Decimal("100"), BASE + 100000, clock.wall, clock.mono, "still-accepted")
    assert value.queue, "optional pause does not turn a feed offer into blocking I/O"


def test_guard_freshness_boundary_is_inclusive():
    value, clock, _, _, _ = runtime()
    value.guard["monotonic_ns"] = clock.mono - 3 * NS_PER_SECOND
    assert value._can_issue(clock.wall, clock.mono)


def test_progress_clock_is_persisted_for_restart_regression_detection():
    value, clock, spool, _, _ = runtime()
    assert value._can_issue(clock.wall, clock.mono)
    run(value.flush_audit_once())
    assert spool.saved_campaign["last_wall_ms"] == clock.wall // NS_PER_MS


def test_stop_during_campaign_fsync_cannot_clear_the_new_dirty_state():
    value, clock, spool, _, _ = runtime()
    assert value._can_issue(clock.wall, clock.mono)
    original = spool.save_campaign
    def save_then_stop(state):
        original(state)
        value.stop("audit_size_cap")
    spool.save_campaign = save_then_stop
    run(value.flush_audit_once())
    assert value._campaign_dirty, "the saved snapshot predated this stop"
    run(value.flush_audit_once())
    assert spool.saved_campaign["stop_reason"] == "audit_size_cap"
    assert not value._campaign_dirty


def test_canary_end_and_stop_survive_restart_without_new_72h_allowance():
    async def scenario():
        value, clock, spool, _, _ = runtime()
        clock.wall = (BASE + CANARY_MS - 3600000) * NS_PER_MS
        await value.start()
        assert value._end_mono - clock.mono == 3600000 * NS_PER_MS
        value.stop("audit_size_cap")
        await value.close()
        second = GhostRuntime(value.settings, Store(), Redis(), spool,
                              wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono,
                              disk_free=lambda: 2 * RESERVE_BYTES)
        try:
            await second.start()
            assert second.stop_reason == "audit_size_cap"
            assert second.issue() is None
        finally:
            await second.close()
    run(scenario())


def test_restart_reconciles_intent_without_republishing_or_claiming_lead():
    async def scenario():
        value, clock, spool, _, _ = runtime()
        row = issue(value)
        spool.write(row.record())
        other_store, other_redis = Store(), Redis()
        other = GhostRuntime(value.settings, other_store, other_redis, spool,
                             wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono,
                             disk_free=lambda: 2 * RESERVE_BYTES)
        try:
            await other.start()
            saved = other_store.persisted[0]
            assert saved["terminal"]
            state = json.loads(saved["state_json"])
            assert all(t["status"] == "restart_unmatched" for t in state["targets"].values())
            assert all(t["confirmed_redis_lead_ns"] is None for t in state["targets"].values())
            assert other_redis.calls == []
            assert other.engine.history_size == 0
        finally:
            await other.close()
    run(scenario())


def test_restart_preserves_already_durable_matched_lead_and_first_price():
    async def scenario():
        value, clock, _, store, _ = runtime()
        row = issue(value)
        await value.publish(row)
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock))
        await value.flush_audit_once()
        persisted = store.persisted[-1]
        other, _, _, other_store, _ = runtime()
        await other._recover(persisted)
        result = json.loads(other_store.persisted[-1]["state_json"])
        matched = result["targets"]["1"]
        assert matched["first_event"]["event_id"] == "target"
        assert matched["error"] == "-1.000000000000000000"
        assert int(matched["confirmed_redis_lead_ns"]) == NS_PER_SECOND
        assert result["targets"]["30"]["status"] == "restart_unmatched"
    run(scenario())


@pytest.mark.parametrize("database_already_terminal", [False, True])
def test_newer_terminal_outbox_state_is_not_discarded_during_recovery(database_already_terminal):
    async def scenario():
        value, clock, _, store, _ = runtime()
        row = issue(value)
        await value.publish(row)
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock))
        if database_already_terminal:
            row.terminal = True
        old = row.record()
        await store.persist(old)
        clock.advance(1)
        value.observe_target(target(value, row, clock, price="102", identity="later-conflict"))
        row.terminal = True
        incoming = row.record()
        assert incoming["version"] > old["version"]
        await value._recover(incoming)
        stored = await store.get_record(row.decision.run_id, row.decision.decision_id)
        assert stored["version"] >= incoming["version"]
        assert stored["terminal"]
        result = json.loads(stored["state_json"])["targets"]["1"]
        assert result["first_event"]["event_id"] == "target"
        assert result["conflicted"]
        assert result["first_conflicting_event"]["event_id"] == "later-conflict"
    run(scenario())


def test_publication_reserves_room_for_all_bounded_target_result_updates(tmp_path):
    async def scenario():
        value, clock, _, store, redis = runtime(record_max_bytes=45000)
        real = GhostSpool(tmp_path / "bounded-outbox", max_records=512, max_record_bytes=45000)
        real.open()
        value.spool = real
        try:
            warm(value)
            row = value.issue()
            if row is None:  # Rejecting admission before publishing is safe.
                assert value.stop_reason is not None
                assert not redis.calls
                return
            await value.publish(row)
            if not redis.calls:
                assert value.stop_reason is not None
                return
            for horizon in (1, 2, 3, 5, 10, 30):
                desired = row.decision.decision_monotonic_ns + horizon * NS_PER_SECOND
                clock.advance(desired - clock.mono)
                value.observe_target(target(value, row, clock, horizon=horizon,
                                            identity="first-" + "x" * 250))
                clock.advance(1)
                value.observe_target(target(value, row, clock, horizon=horizon,
                                            price="102", identity="other-" + "y" * 250))
            # Published inputs being durable is necessary, but bounded first
            # matches/conflicts must also fit the capacity reserved beforehand.
            await value.flush_audit_once()
            saved = json.loads(store.persisted[-1]["state_json"])
            assert all(t["conflicted"] for t in saved["targets"].values())
        finally:
            real.close()
    run(scenario())


def test_maximum_bounded_event_metadata_and_all_result_records_fit_default_spool(tmp_path):
    async def scenario():
        value, clock, _, store, redis = runtime()
        real = GhostSpool(tmp_path / "full-metadata", max_records=512, max_record_bytes=131072)
        real.open()
        value.spool = real
        large_price = "99999999999999999999.999999999999999999"
        try:
            for second in range(39, 101):
                value.offer_price("spot", Decimal(large_price), BASE + second * 1000,
                                  (BASE + second * 1000) * NS_PER_MS, second * NS_PER_SECOND,
                                  ("spot-" + str(second)).ljust(256, "x"))
                value.drain_inputs()
            value.offer_price("twap", Decimal(large_price), BASE + 100000,
                              clock.wall, clock.mono, "anchor".ljust(256, "x"), 60)
            row = value.issue()
            assert row is not None
            await value.publish(row)
            assert redis.calls
            for horizon in (1, 2, 3, 5, 10, 30):
                clock.advance(row.decision.decision_monotonic_ns + horizon * NS_PER_SECOND - clock.mono)
                value.observe_target(target(value, row, clock, horizon=horizon, price=large_price,
                                            identity="first".ljust(256, "x")))
                clock.advance(1)
                value.observe_target(target(value, row, clock, horizon=horizon,
                                            price="1.000000000000000001", identity="conflict".ljust(256, "y")))
            await value.flush_audit_once()
            assert max(path.stat().st_size for path in real.directory.glob("*.row")) <= 131072
            saved = json.loads(store.persisted[-1]["state_json"])
            assert all(t["conflicted"] for t in saved["targets"].values())
        finally:
            real.close()
    run(scenario())


def test_row_count_guard_uses_initial_plus_issued_when_measurement_omits_count():
    value, clock, _, store, _ = runtime()
    value.initial_rows = MAX_ROWS - 1
    store.measurement["row_count"] = None
    run(value.refresh_guard())
    assert value.guard["row_count"] == MAX_ROWS - 1
    row = issue(value)
    assert row is not None
    # No DB insertion or updated measurement is needed to reserve the final row.
    assert value.issue() is None
    assert value.stop_reason == "audit_row_reserve"
    assert value._decisions == 1


def test_stopped_runtime_state_stays_bounded_with_ongoing_feed_offers():
    value, clock, _, _, _ = runtime(input_queue_max=16, audit_max_records=8)
    issue(value)
    value.stop("audit_persistence_failure")
    records = len(value.records)
    for second in range(101, 301):
        clock.advance(NS_PER_SECOND)
        for feed in ("spot", "twap"):
            value.offer_price(feed, Decimal("100"), BASE + second * 1000,
                              clock.wall, clock.mono, feed + str(second),
                              60 if feed == "twap" else None)
        assert value.issue() is None
        value.finalize_due()
    assert len(value.records) == records
    assert len(value.queue) <= 16
    assert len(value.late_events) <= 16
    assert len(value._dirty) <= records
    assert len(value.gaps) <= 128
    assert value.engine.history_size <= value.engine.policy.max_events


def test_cancellation_keeps_spool_exclusive_until_the_filesystem_call_finishes():
    async def scenario():
        value, _, _, _, _ = runtime()
        entered, release, second_entered = threading.Event(), threading.Event(), threading.Event()
        def blocked_filesystem_call():
            entered.set()
            if not release.wait(2):
                raise TimeoutError("test did not release filesystem worker")
        first = asyncio.create_task(value._spool(blocked_filesystem_call))
        assert await asyncio.to_thread(entered.wait, 1)
        first.cancel()
        await asyncio.sleep(0)
        second = asyncio.create_task(value._spool(second_entered.set))
        try:
            overlapped = await asyncio.to_thread(second_entered.wait, 0.1)
        finally:
            release.set()
            results = await asyncio.gather(first, second, return_exceptions=True)
        assert not overlapped, "cancellation must not release ownership while a write thread remains active"
        assert isinstance(results[0], asyncio.CancelledError)
        assert second_entered.is_set()
    run(scenario())


def test_persistence_failure_keeps_complete_outbox_and_in_memory_reservation():
    value, _, spool, store, _ = runtime()
    row = issue(value)
    store.failure = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        run(value.flush_audit_once())
    assert spool.rows[(row.decision.run_id, row.decision.decision_id)]["frozen_json"] == row.frozen_json
    assert row.decision.decision_id in value.records
    assert row.decision.decision_id in value._dirty


def test_late_target_pagination_is_fully_drained_before_event_is_discarded():
    value, clock, _, store, _ = runtime()
    row = issue(value)
    event = target(value, row, clock)
    value.late_events.clear()  # isolate the target from the warming anchor
    value.late_events.append({"event_id": event.event_id})
    async def paged(event, *, after=None, limit=100):
        store.late_calls.append((event, after))
        return {"next_after": ("run", "100") if after is None else None}
    store.note_late_target = paged
    run(value.flush_audit_once())
    assert [after for _, after in store.late_calls] == [None, ("run", "100")]
    assert not value.late_events
