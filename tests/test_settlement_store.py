import asyncio
from copy import deepcopy
from decimal import Decimal, localcontext
import json
from pathlib import Path

import pytest

from price_collector.ghost_twap_store import GhostAuditConflict
from price_collector.settlement_store import (
    DAY_MS, RULE, SOURCE, SettlementStore, build_report, capture_outcome,
    official_outcome, publication_eligible, update_market,
)


START = 20_000 * DAY_MS


def signal(price, *, qualifies=None):
    with localcontext() as ctx:
        ctx.prec = 80
        lead = (Decimal(price) - Decimal(100)) * 100
    return dict(price=price, side="up" if lead > 0 else "down" if lead < 0 else "tie",
                lead_bps=str(lead), qualifies=abs(lead) >= 2 if qualifies is None else qualifies)


def decision(identifier="d1", *, offset_ms=0, prices=("101", "99", "100.01"), quality="healthy"):
    end = START + 300_000
    clock = (end - 29_000 + offset_ms) * 1_000_000
    frozen = dict(run_id="run", decision_id=identifier, schema_version=1, kind="settlement",
        rule_version="settlement-first-2bp-v1", threshold_bps="2",
        market_id=START // 300_000, market_start_ms=START, market_end_ms=end,
        target_source_timestamp_ms=end, decision_wall_ns=str(clock), decision_monotonic_ns="1000000",
        valid_until_wall_ns=str(clock + 2_000_000_000), status="available", quality=quality,
        reference=dict(price_to_beat="100", condition_id="c", up_token_id="u", down_token_id="d"),
        reasons=[], signals=dict(zip(("ghost", "twap", "spot"), map(signal, prices))), slots=[], slot_inputs=[])
    state = dict(publication=dict(status="acknowledged", attempt_wall_ns=str(clock + 1),
                                 ack_wall_ns=str(clock + 2), eligible_before_close=True),
                 target=dict(status="pending", first_event=None))
    seal(frozen, state)
    return frozen, state


def seal(frozen, state):
    wire = {key: value for key, value in frozen.items() if key not in ("slots", "slot_inputs")}
    wire.update(publication_state="attempted", publication_attempt_wall_ns=state["publication"]["attempt_wall_ns"])
    state["publication"]["attempted_payload"] = json.dumps(wire)


def record(frozen, state, *, version=0, terminal=False):
    return dict(run_id=frozen["run_id"], decision_id=frozen["decision_id"],
                decision_wall_ns=int(frozen["decision_wall_ns"]),
                created_ms=int(frozen["decision_wall_ns"]) // 1_000_000,
                frozen_json=json.dumps(frozen), state_json=json.dumps(state), version=version, terminal=terminal)


def resolution(**changes):
    return dict(dict(condition_id="c", up_token_id="u", down_token_id="d", resolution_status="resolved",
        resolution_type="winner", winner="Up", winning_token_id="u", up_payout=Decimal(1),
        down_payout=Decimal(0), settlement_reference="chainlink_twap", settlement_window_s=60,
        settlement_source_url=SOURCE, settlement_rule_version=RULE, reconciled_settlement_rule_version=RULE,
        resolution_source="polymarket_clob_rest", last_checked_ms=START + 310_000), **changes)


@pytest.mark.parametrize("fault", ("deadline", "close", "no_ack", "not_eligible", "attempt_after_ack", "monotonic_regression"))
def test_only_actual_acknowledged_fresh_preclose_publications_count(fault):
    frozen, state = decision()
    assert publication_eligible(frozen, state)
    pub = state["publication"]
    if fault == "deadline": pub["ack_wall_ns"] = frozen["valid_until_wall_ns"]
    if fault == "close":
        frozen["valid_until_wall_ns"] = str((frozen["market_end_ms"] + 1000) * 1_000_000)
        pub["ack_wall_ns"] = str(frozen["market_end_ms"] * 1_000_000)
    if fault == "no_ack": pub["status"] = "attempting"
    if fault == "not_eligible": pub["eligible_before_close"] = False
    if fault == "attempt_after_ack": pub["attempt_wall_ns"] = str(int(pub["ack_wall_ns"]) + 1)
    if fault == "monotonic_regression": pub.update(attempt_monotonic_ns="999999", ack_monotonic_ns="1000001")
    assert not publication_eligible(frozen, state)
    assert update_market({}, frozen, state, inserted=True)["first_calls"] == {}


def test_signal_owns_first_call_and_paired_baseline_keeps_ghost_instant():
    frozen, state = decision(prices=("100.01", "101", "99"))
    body = update_market({}, frozen, state, inserted=True)
    assert set(body["first_calls"]) == {"twap", "spot"}
    later, state = decision("d2", offset_ms=500, prices=("101", "100.01", "99"))
    body = update_market(body, later, state, inserted=True)
    assert body["first_calls"]["ghost"]["frozen"]["signals"]["twap"]["price"] == "100.01"
    assert body["first_calls"]["twap"]["signal"]["price"] == "101"
    assert body["revocations"]["twap"] == 1
    newest, state = decision("d3", offset_ms=900, prices=("100.01", "100.01", "99"))
    body = update_market(body, newest, state, inserted=True)
    assert body["revocations"] == {"twap": 1, "ghost": 1}
    newest["decision_id"] = "d4"; newest["decision_wall_ns"] = str(int(newest["decision_wall_ns"]) + 1)
    body = update_market(body, newest, state, inserted=True)
    assert body["revocations"] == {"twap": 1, "ghost": 1}
    assert set(body["first_calls"]) == {"ghost", "twap", "spot"}


def test_recovery_selects_earliest_ack_with_deterministic_tie_without_rewriting_call():
    later, later_state = decision("z", offset_ms=500)
    body = update_market({}, later, later_state, inserted=True)
    earlier, earlier_state = decision("a", offset_ms=100)
    body = update_market(body, earlier, earlier_state, inserted=True)
    assert body["first_calls"]["ghost"]["frozen"]["decision_id"] == "a"
    same, same_state = decision("b", offset_ms=100)
    body = update_market(body, same, same_state, inserted=True)
    assert body["first_calls"]["ghost"]["frozen"]["decision_id"] == "a"
    assert body["revocations"] == {}


def test_rounded_display_lead_cannot_admit_a_below_threshold_price():
    frozen, state = decision(prices=("100.019999999999999999", "100.02", "99.98"))
    frozen["signals"]["ghost"].update(qualifies=True, lead_bps="2.000000000000000000")
    seal(frozen, state)
    body = update_market({}, frozen, state, inserted=True)
    assert set(body["first_calls"]) == {"twap", "spot"}


def test_publication_credit_requires_the_actual_attempted_values():
    frozen, state = decision()
    body = json.loads(state["publication"]["attempted_payload"])
    body["signals"]["ghost"]["price"] = "102"
    state["publication"]["attempted_payload"] = json.dumps(body)
    assert not publication_eligible(frozen, state)


def test_status_updates_are_idempotent_and_ack_timing_is_from_actual_publication():
    frozen, ack = decision()
    reserved = dict(publication=dict(status="reserved"))
    body = update_market({}, frozen, reserved, inserted=True)
    body = update_market(body, frozen, ack, inserted=False, previous_state=reserved)
    body = update_market(body, frozen, ack, inserted=False, previous_state=ack)
    assert body["publication_status_counts"] == {"reserved": 0, "acknowledged": 1}
    assert body["eligible_publications"] == 1
    report = build_report([market_row(body)], START, START + DAY_MS, final=False, persistence_complete=True)
    assert report["signals"]["ghost"]["call_timing"]["median_remaining_ns"] == "28999999998"
    assert report["signals"]["spot"]["abstention_reasons"]["below_threshold"] == 1


@pytest.mark.parametrize("change", [dict(winner="Down"), dict(resolution_type="split"),
    dict(up_payout=Decimal("0.99")), dict(settlement_window_s=30),
    dict(reconciled_settlement_rule_version="btc-5m-twap-30"),
    dict(resolution_source="inferred"), dict(up_token_id="d"), dict(condition_id=None),
    dict(last_checked_ms=START + 400_001)])
def test_official_outcome_requires_payout_identity_and_known_by_cutoff(change):
    assert official_outcome(resolution(), START + 400_000)["winner"] == "up"
    assert official_outcome(resolution(**change), START + 400_000) is None


def test_pre_cutoff_revision_can_remove_outcome_but_late_resolution_cannot_backdate():
    body = capture_outcome({}, resolution(), START + 400_000)
    assert body["official_outcome"]["winner"] == "up"
    assert capture_outcome(body, resolution(last_checked_ms=START + 500_000, winner="Down"), START + 400_000) == body
    revised = capture_outcome(body, resolution(last_checked_ms=START + 350_000, resolution_status="pending"), START + 400_000)
    assert revised["official_outcome"] is None


def market_row(body):
    return dict(market_id=START // 300_000, market_start_ms=START,
                market_end_ms=START + 300_000, body_json=json.dumps(body))


def test_report_separates_coverage_unknown_abstention_and_paired_baselines():
    frozen, state = decision(quality="degraded")
    body = update_market({}, frozen, state, inserted=True)
    body = capture_outcome(body, resolution(), START + 400_000)
    report = build_report([market_row(body)], START, START + DAY_MS, final=False, persistence_complete=True)
    assert (report["scheduled_markets"], report["observed_markets"], report["no_observation_markets"]) == (288, 1, 287)
    assert "TWAP and spot baseline calls are evaluated only at eligible acknowledged ghost settlement publications; ghost-specific unavailability also removes baseline opportunities" in report["limitations"]
    ghost, twap, spot = [report["signals"][x] for x in ("ghost", "twap", "spot")]
    assert (ghost["calls"], ghost["losses"], ghost["abstentions"]) == (1, 0, 287)
    assert (twap["calls"], twap["losses"]) == (1, 1)
    assert spot["calls"] == 0
    assert ghost["by_quality"]["degraded"]["resolved"] == 1
    assert report["paired_at_ghost_first"]["twap"]["ghost_only_correct"] == 1
    assert report["paired_at_ghost_first"]["spot"]["both_correct"] == 1  # paired baseline need not qualify
    body["official_outcome"]["condition_id"] = "different-market"
    report = build_report([market_row(body)], START, START + DAY_MS, final=False, persistence_complete=True)
    assert report["signals"]["ghost"]["unknown"] == 1
    assert report["signals"]["ghost"]["loss_rate"] is None
    assert report["paired_at_ghost_first"]["twap"]["unknown_outcome"] == 1


class Context:
    def __init__(self, item): self.item = item
    async def __aenter__(self): return self.item
    async def __aexit__(self, *_): return False


class Connection:
    def __init__(self):
        self.audit, self.markets, self.reports, self.sql = {}, {}, {}, []
    def transaction(self): return Context(self)
    async def execute(self, sql, *args):
        self.sql.append((sql, args))
        if sql.startswith("UPDATE settlement_audit"):
            self.audit[args[:2]].update(state_json=args[2], version=args[3], terminal=args[4])
        elif "INSERT INTO settlement_market_evaluation" in sql:
            self.markets.setdefault(args[:2], dict(evaluation_start_ms=args[0], market_id=args[1],
                market_start_ms=args[2], market_end_ms=args[3], body_json="{}"))
        elif sql.startswith("UPDATE settlement_market_evaluation"):
            self.markets[args[:2]]["body_json"] = args[2]
    async def fetchval(self, sql, *args):
        self.sql.append((sql, args))
        if "INSERT INTO settlement_audit" in sql:
            if args[:2] in self.audit: return None
            fields = ("run_id", "decision_id", "evaluation_start_ms", "market_id", "market_start_ms", "market_end_ms",
                "decision_wall_ns", "created_ms", "frozen_json", "frozen_sha256", "compact_json", "state_json", "version", "terminal")
            self.audit[args[:2]] = dict(zip(fields, args))
            return True
        if "INSERT INTO settlement_evaluation_reports" in sql:
            previous = self.reports.get(args[0])
            if previous and previous["final"]: return None
            self.reports[args[0]] = dict(final=args[2], body_json=args[3])
            return args[3]
        if "FROM settlement_evaluation_reports" in sql: return self.reports[args[0]]["body_json"]
        return self.markets[args[:2]]["body_json"]
    async def fetchrow(self, sql, *args):
        self.sql.append((sql, args))
        if "FROM settlement_audit" in sql: return deepcopy(self.audit.get(args[:2]))
        return deepcopy(self.reports.get(args[0]))
    async def fetch(self, sql, *args):
        self.sql.append((sql, args))
        rows = [deepcopy(row) for (evaluation, market), row in sorted(self.markets.items())
                if evaluation == args[0] and market > args[1]][:100]
        if "LEFT JOIN" in sql:
            for row in rows: row.update(resolution())
        return rows


class Pool:
    def __init__(self): self.connection = Connection()
    def acquire(self, *, timeout):
        assert timeout == 5
        return Context(self.connection)


def test_store_versions_compaction_retry_first_match_and_frozen_report():
    async def scenario():
        pool = Pool(); store = SettlementStore(pool, START)
        frozen, state = decision()
        first = record(frozen, state)
        assert await store.persist(first) == "inserted"
        assert await store.persist(first) == "unchanged"
        bad = deepcopy(frozen); bad["signals"]["ghost"]["price"] = "200"
        with pytest.raises(GhostAuditConflict, match="frozen"):
            await store.persist(record(bad, state, version=1))
        state["target"] = dict(status="matched", first_event=dict(value="100", source_timestamp_ms=frozen["market_end_ms"]))
        final = record(frozen, state, version=2, terminal=True)
        assert await store.persist(final) == "updated"
        pool.connection.audit[("run", "d1")]["frozen_json"] = None
        assert await store.persist(final) == "unchanged"  # retry cannot re-inflate compact evidence
        assert await store.persist(first) == "stale"
        state["target"]["first_event"]["value"] = "101"
        with pytest.raises(GhostAuditConflict, match="target"):
            await store.persist(record(frozen, state, version=3, terminal=True))
        result = await store.maintain(store.report_due_ms, persistence_complete=False)
        assert result["final"] and result["status"] == "final_incomplete"
        assert result["scheduled_markets"] == 1440
        assert result["signals"]["ghost"]["resolved"] == 1
        assert await store.report(store.report_due_ms + DAY_MS, persistence_complete=True) == result
        assert sum("INSERT INTO settlement_evaluation_reports" in sql for sql, _ in pool.connection.sql) == 1
    asyncio.run(scenario())


def test_report_freezes_after_outcome_cutoff_when_complete_and_marks_missed_due():
    async def scenario():
        pool = Pool(); store = SettlementStore(pool, START)
        early = await store.maintain(store.cutoff_ms - 1)
        assert not early["final"]
        result = await store.maintain(store.cutoff_ms)
        assert result["final"] and result["status"] == "final"
        late = build_report([], START, store.report_due_ms + 1, final=True, persistence_complete=True)
        assert late["status"] == "final_incomplete" and late["report_deadline_missed"]
    asyncio.run(scenario())


def test_unarmed_and_outside_window_live_audit_does_not_enter_evaluation():
    async def scenario():
        pool = Pool(); store = SettlementStore(pool, 0)
        frozen, state = decision()
        assert await store.persist(record(frozen, state)) == "inserted"
        assert not pool.connection.markets
        assert (await store.report(START))["status"] == "unarmed"
        assert (await SettlementStore(pool, START + DAY_MS).report(START))["status"] == "scheduled"
        assert (await SettlementStore(pool, START).report(START + 90 * DAY_MS))["status"] == "expired"
        assert not pool.connection.reports
    asyncio.run(scenario())


def test_recovery_uses_frozen_evaluation_association_after_configuration_changes():
    async def scenario():
        pool = Pool()
        frozen, state = decision("unarmed")
        frozen["evaluation_start_ms"] = 0
        seal(frozen, state)
        saved = record(frozen, state)
        assert await SettlementStore(pool, 0).persist(saved) == "inserted"
        assert await SettlementStore(pool, START).persist(saved) == "unchanged"
        assert pool.connection.audit[("run", "unarmed")]["evaluation_start_ms"] == 0
        assert not pool.connection.markets
        frozen, state = decision("prior")
        frozen["evaluation_start_ms"] = START
        seal(frozen, state)
        saved = record(frozen, state)
        assert await SettlementStore(pool, START + 7 * DAY_MS).persist(saved) == "inserted"
        assert (START, frozen["market_id"]) in pool.connection.markets
        assert (START + 7 * DAY_MS, frozen["market_id"]) not in pool.connection.markets
        state["target"] = dict(status="restart_unmatched", first_event=None)
        assert await SettlementStore(pool, 0).persist(record(frozen, state, version=1, terminal=True)) == "updated"
    asyncio.run(scenario())


def test_schema_has_separate_indexed_retention_and_immutable_final_guards():
    sql = Path("schema.sql").read_text(encoding="utf-8").split("-- Settlement evaluation:", 1)[1].split("-- End settlement evaluation schema.")[0]
    assert "PRIMARY KEY(run_id,decision_id)" in sql
    assert "PRIMARY KEY(evaluation_start_ms,market_id)" in sql
    assert "settlement_audit_expiry_idx" in sql and "settlement_audit_compact_idx" in sql
    assert "expired settlement cannot be reinserted" in sql
    assert "settlement first target is immutable" in sql
    assert "final settlement report is immutable" in sql
    assert "518400000" in sql  # finalization never precedes five days + the 24-hour outcome cutoff
