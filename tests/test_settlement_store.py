import asyncio
from copy import deepcopy
from decimal import Decimal, localcontext
import json
from pathlib import Path

import pytest

from price_collector.ghost_twap_store import GhostAuditConflict
from price_collector.settlement_store import (
    DAY_MS, RULE, SOURCE, SettlementStore, capture_outcome,
    official_outcome, publication_eligible,
)
from price_collector.settlement_history import (
    cohort_key, observe, time_bucket, margin_bucket, daily_summary, history_summary,
)

START = 20_000 * DAY_MS


def signal(price, *, qualifies=None):
    with localcontext() as ctx:
        ctx.prec = 80
        lead = (Decimal(price) - Decimal(100)) * 100
    return dict(price=price, side="up" if lead > 0 else "down" if lead < 0 else "tie",
                lead_bps=str(lead), qualifies=abs(lead) >= 2 if qualifies is None else qualifies)


def decision(identifier="d1", *, offset_ms=0, prices=("101", "99", "100.01"), quality="healthy", market_start_ms=START):
    end = market_start_ms + 300_000
    clock = (end - 29_000 + offset_ms) * 1_000_000
    frozen = dict(run_id="run", decision_id=identifier, schema_version=1, kind="settlement",
        rule_version="settlement-first-2bp-v1", threshold_bps="2",
        market_id=market_start_ms // 300_000, market_start_ms=market_start_ms, market_end_ms=end,
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


def minute_contract(frozen, state):
    frozen.update(schema_version=3, rule_version="historical-settlement-v2",
                  observation_window_s=60, sampling_interval_ms=2000)
    frozen.pop("threshold_bps", None)
    seal(frozen, state)
    return frozen, state


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
        if sql.startswith("UPDATE settlement_audit SET state_json"):
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
        return None
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


def test_publication_credit_requires_the_actual_attempted_values():
    frozen, state = decision()
    body = json.loads(state["publication"]["attempted_payload"])
    body["signals"]["ghost"]["price"] = "102"
    state["publication"]["attempted_payload"] = json.dumps(body)
    assert not publication_eligible(frozen, state)


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


def test_store_keeps_version_and_frozen_target_guards_without_writing_study():
    async def scenario():
        pool = Pool(); store = SettlementStore(pool)
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
        assert await store.persist(final) == "unchanged"
        assert await store.persist(first) == "stale"
        state["target"]["first_event"]["value"] = "101"
        with pytest.raises(GhostAuditConflict, match="target"):
            await store.persist(record(frozen, state, version=3, terminal=True))
        assert not pool.connection.markets and not pool.connection.reports
        assert not any("INSERT INTO settlement_market_evaluation" in sql for sql, _ in pool.connection.sql)
        assert not hasattr(store, "report")
    asyncio.run(scenario())


def test_old_frozen_study_association_survives_recovery_without_rearming():
    async def scenario():
        pool = Pool(); frozen, state = decision()
        frozen["evaluation_start_ms"] = START
        seal(frozen, state)
        assert await SettlementStore(pool).persist(record(frozen, state)) == "inserted"
        assert await SettlementStore(pool).persist(record(frozen, state)) == "unchanged"
        assert pool.connection.audit[("run", "d1")]["evaluation_start_ms"] == START
        assert not pool.connection.markets
    asyncio.run(scenario())


def test_cohort_ignores_old_call_rule_but_changes_with_model_and_policy():
    frozen, _ = decision()
    old = dict(frozen, model_version="model-1", policy={"source_max_age_ms": 5000})
    new = dict(old, schema_version=2, rule_version="historical-settlement-v1")
    new.pop("threshold_bps")
    assert cohort_key(old) == cohort_key(new)
    assert cohort_key(new) != cohort_key(dict(new, model_version="model-2"))
    assert cohort_key(new) != cohort_key(dict(new, policy={"source_max_age_ms": 3000}))
    assert cohort_key(old) == "acf0f5763a79d9fd72e455caa58ac3f070da186f99f57bb7b1ad17cd0bb1e628"


def test_time_and_margin_boundaries_are_exact():
    assert time_bucket(0) is None and time_bucket(30_000_000_001) is None
    assert time_bucket(4_999_999_999) == "0-5"
    assert time_bucket(5_000_000_000) == "5-10"
    assert time_bucket(30_000_000_000) == "25-30"
    assert time_bucket(29_999_999_999, window_s=60) == "25-30"
    assert time_bucket(30_000_000_000, window_s=60) == "30-35"
    assert time_bucket(55_000_000_000, window_s=60) == "55-60"
    assert time_bucket(60_000_000_000, window_s=60) == "55-60"
    assert time_bucket(60_000_000_001, window_s=60) is None
    assert [margin_bucket(Decimal(x)) for x in ("0.999999999999999999", "1", "-2", "4", "-8")] == [
        "0-1", "1-2", "2-4", "4-8", "8+"]


def test_minute_contract_admits_earlier_decisions_without_expanding_legacy():
    async def scenario():
        pool = Pool(); store = SettlementStore(pool)
        frozen, state = decision("minute", offset_ms=-31_000)
        with pytest.raises(ValueError, match="schedule"):
            await store.persist(record(frozen, state))
        minute_contract(frozen, state)
        for change in (dict(schema_version=2), dict(rule_version="historical-settlement-v1"),
                       dict(observation_window_s=120), dict(sampling_interval_ms=500)):
            bad = dict(frozen, **change)
            with pytest.raises(ValueError, match="observation window"):
                await store.persist(record(bad, state))
        assert await store.persist(record(frozen, state)) == "inserted"
        assert await store.persist(record(frozen, state)) == "unchanged"
        assert not pool.connection.markets and not pool.connection.reports
        too_early, early_state = minute_contract(*decision("early", offset_ms=-31_001))
        with pytest.raises(ValueError, match="schedule"):
            await store.persist(record(too_early, early_state))
    asyncio.run(scenario())


def test_mixed_history_keeps_legacy_six_buckets_and_new_twelve_distinct():
    legacy, old_state = decision(prices=("100.01", "100", "99.99"))
    minute, state = minute_contract(*decision("minute", offset_ms=-30_000,
        prices=("100.01", "100", "99.99"), market_start_ms=START + 300_000))
    legacy_body = observe({}, legacy, old_state, eligible=True)
    minute_body = observe({}, minute, state, eligible=True)
    assert set(minute_body["buckets"]) == {"55-60"}
    assert legacy_body["cohort"] != minute_body["cohort"]
    assert minute_body["description"]["sampling_interval_ms"] == 2000
    days = [daily_summary([body], day_ms=START, now_ms=START + DAY_MS, final=False)
            for body in (legacy_body, minute_body)]
    history = history_summary(days, START + DAY_MS, complete=True)
    old_coverage = [row for row in history["coverage"] if row["cohort"] == legacy_body["cohort"]]
    new_coverage = [row for row in history["coverage"] if row["cohort"] == minute_body["cohort"]]
    assert len(old_coverage) == 6 and len(new_coverage) == 12
    assert "55-60" not in {row["time_bucket"] for row in old_coverage}
    assert next(row for row in new_coverage if row["time_bucket"] == "55-60")["selected_markets"] == 1
    assert len([row for row in history["cells"] if row["cohort"] == legacy_body["cohort"]]) == 90
    assert len([row for row in history["cells"] if row["cohort"] == minute_body["cohort"]]) == 180


def test_first_ack_selected_before_binning_and_no_replacement_with_later_better_margin():
    later, later_state = decision("late", offset_ms=500, prices=("101", "99", "100.01"))
    earlier, earlier_state = decision("early", prices=("100.01", "100", "99.99"))
    body = observe({}, later, later_state, eligible=True)
    body = observe(body, earlier, earlier_state, eligible=True)
    assert body["buckets"]["25-30"]["order"][2] == "early"
    assert body["buckets"]["25-30"]["signals"]["ghost"]["margin_bps"] == "1.0000"
    assert observe(body, later, later_state, eligible=True) == body
    assert observe(body, earlier, earlier_state, eligible=True) == body
    # Decision stamp is 25.5s out; its ACK crosses into the 20-25s bucket.
    boundary, state = decision("boundary", offset_ms=3500)
    state["publication"]["ack_wall_ns"] = str((boundary["market_end_ms"] - 24_900) * 1_000_000)
    body = observe(body, boundary, state, eligible=True)
    assert "20-25" in body["buckets"]


def test_counts_use_one_market_and_keep_losses_unknown_ties_and_missing_separate():
    frozen, state = decision(prices=("100.01", "100", "99.99"))
    winner = capture_outcome(observe({}, frozen, state, eligible=True), resolution(), START + 400_000)
    second, state2 = decision("second", market_start_ms=START + 300_000, prices=("100.01", "100", "99.99"))
    unknown = observe({}, second, state2, eligible=True)
    third, state3 = decision("third", market_start_ms=START + 900_000)
    unavailable = observe({}, third, state3, eligible=False)
    day = daily_summary([winner, unknown, unavailable], day_ms=START, now_ms=START + DAY_MS, final=False)
    history = history_summary([day], START + DAY_MS, complete=True)
    ghost = next(c for c in history["cells"] if (c["signal"], c["time_bucket"], c["margin_bucket"]) == ("ghost", "25-30", "1-2"))
    spot = next(c for c in history["cells"] if (c["signal"], c["time_bucket"], c["margin_bucket"]) == ("spot", "25-30", "1-2"))
    assert (ghost["wins"], ghost["losses"], ghost["unknown"], ghost["pending"]) == (1, 0, 1, 1)
    assert (spot["wins"], spot["losses"], spot["unknown"]) == (0, 1, 1)
    assert ghost["win_rate_pct"] is None and ghost["interval95_pct"] is None
    coverage = next(c for c in history["coverage"] if c["time_bucket"] == "25-30")
    assert coverage["no_observation_markets"] == 1
    assert coverage["no_eligible_publication_markets"] == 1
    assert coverage["ties"]["twap"] == 2
    unknown["official_outcome"] = dict(winner["official_outcome"], condition_id="other-market")
    final = daily_summary([winner, unknown], day_ms=START, now_ms=START + 2 * DAY_MS, final=True)
    unknown_cell = next(c for c in final["cells"] if c["signal"] == "ghost")
    assert unknown_cell["unknown"] == 1 and unknown_cell["frozen_unknown"] == 1 and unknown_cell["pending"] == 0
    # Aggregate losses survive beyond the individual seven-day expiry.
    retained = history_summary([final], START + 8 * DAY_MS, complete=True)
    assert sum(c["losses"] for c in retained["cells"] if c["signal"] == "spot") == 1
    assert not history_summary([final], START + 90 * DAY_MS, complete=True)["cells"]


def test_percent_and_interval_only_use_resolved_markets():
    from price_collector.settlement_history import percentage
    assert percentage(29, 29) == (None, None)
    rate, interval = percentage(57, 60)
    assert rate == "95.00" and Decimal(interval[0]) < 95 < Decimal(interval[1])


def test_schema_retains_evidence_and_adds_guarded_history_marker_and_finite_expiry():
    sql = Path("schema.sql").read_text(encoding="utf-8")
    assert "history_folded_version < version" in sql
    assert "DEFAULT -1" in sql
    assert "PRIMARY KEY(cohort,market_id)" in sql and "PRIMARY KEY(cohort,day_ms)" in sql
    assert "frozen history day is immutable" in sql
    assert "history individual retention is seven days" in sql
    assert "history daily counts retain ninety days" in sql
    assert "final settlement report is immutable" in sql


class HistoryConnection(Connection):
    def __init__(self):
        super().__init__()
        self.history = {}
        self.daily = {}

    async def fetchval(self, sql, *args):
        if "SELECT final FROM settlement_history_daily" in sql:
            return self.daily.get(args[:2], {}).get("final")
        if "SELECT body_json FROM settlement_history_markets" in sql:
            return self.history.get(args[:2])
        if "SELECT EXISTS" in sql:
            rows = (row for row in self.audit.values()
                    if row.get("history_folded_version", -1) < row["version"] and row["created_ms"] > args[0])
            if "market_start_ms >=" in sql:
                return any(args[1] <= row["market_start_ms"] < args[2] for row in rows)
            return any(row["market_end_ms"] + 120_000 <= args[1] for row in rows)
        return await super().fetchval(sql, *args)

    async def fetch(self, sql, *args):
        if "FROM settlement_audit WHERE history_folded_version" in sql:
            return [deepcopy(row) for row in self.audit.values()
                    if row.get("history_folded_version", -1) < row["version"] and row["created_ms"] > args[0]
                    and row["market_end_ms"] + 120_000 <= args[1]][:100]
        if "SELECT DISTINCT h.cohort" in sql:
            return [dict(cohort=cohort, day_ms=START) for cohort in sorted({key[0] for key in self.history})
                    if not self.daily.get((cohort, START), {}).get("final")]
        if "FROM settlement_history_markets h" in sql:
            return [dict(market_id=market, body_json=body, **resolution())
                    for (cohort, market), body in self.history.items() if cohort == args[0]]
        if "SELECT body_json FROM settlement_history_daily" in sql:
            return [deepcopy(row) for row in self.daily.values()]
        return []

    async def fetchrow(self, sql, *args):
        if "FROM settlement_history_daily" in sql:
            return deepcopy(self.daily.get(args[:2]))
        return await super().fetchrow(sql, *args)

    async def execute(self, sql, *args):
        if "INSERT INTO settlement_history_markets" in sql:
            self.history[args[:2]] = args[5]
        elif "UPDATE settlement_history_markets" in sql:
            self.history[args[:2]] = args[2]
        elif "SET history_folded_version" in sql:
            self.audit[args[:2]]["history_folded_version"] = args[2]
        elif "INSERT INTO settlement_history_daily" in sql:
            self.daily[args[:2]] = dict(final=args[3], body_json=args[4])
        elif "UPDATE settlement_history_daily" in sql:
            self.daily[args[:2]] = dict(final=True, body_json=args[3])
        else:
            await super().execute(sql, *args)


def test_background_fold_retries_and_later_ack_are_idempotent():
    async def scenario():
        pool = Pool(); pool.connection = HistoryConnection()
        frozen, acknowledged = decision(prices=("100.01", "99.99", "100.01"))
        reserved = dict(publication={"status": "reserved"}, target={"status": "pending", "first_event": None})
        store = SettlementStore(pool)
        await store.persist(record(frozen, reserved))
        before = await store.maintain(START + 430_000)
        assert sum(cell["resolved"] for cell in before["cells"]) == 0
        await store.persist(record(frozen, acknowledged, version=1))
        after = await store.maintain(START + 430_000)
        assert sum(cell["wins"] for cell in after["cells"]) == 2
        assert sum(cell["losses"] for cell in after["cells"]) == 1
        retry = await SettlementStore(pool).maintain(START + 430_000)
        assert retry["cells"] == after["cells"]
        final = await store.maintain(START + 2 * DAY_MS)
        assert next(iter(pool.connection.daily.values()))["final"] is True
        pool.connection.history.clear()  # individual records have expired
        retained = await store.maintain(START + 8 * DAY_MS)
        assert retained["cells"] == final["cells"]
    asyncio.run(scenario())


@pytest.mark.parametrize("age_ms", (6 * DAY_MS, 7 * DAY_MS + 60_000))
def test_old_day_catchup_finishes_every_page_before_freezing(age_ms):
    async def scenario():
        pool = Pool(); pool.connection = HistoryConnection()
        store = SettlementStore(pool)
        frozen, state = decision("initial", prices=("100.03", "100.01", "99.97"))
        await store.persist(record(frozen, state))
        await store.maintain(START + DAY_MS)
        prior = next(iter(pool.connection.daily.values()))
        assert not prior["final"] and json.loads(prior["body_json"])["observed_markets"] == 1

        # Three ticks in each of 288 markets need multiple 400-record passes.
        # Even just after day+7 midnight, every input here remains retained.
        for market in range(288):
            for tick in range(3):
                frozen, state = decision(f"{market}-{tick}", offset_ms=tick * 100,
                    prices=("100.03", "100.01", "99.97"), market_start_ms=START + market * 300_000)
                await store.persist(record(frozen, state))
        now = START + age_ms
        for _ in range(2):
            report = await store.maintain(now)
            assert report["backfill_pending"] is True
            assert not next(iter(pool.connection.daily.values()))["final"]
        report = await store.maintain(now)
        final = next(iter(pool.connection.daily.values()))
        assert report["status"] == "available" and final["final"] is True
        assert json.loads(final["body_json"])["observed_markets"] == 288
        assert sum(cell["wins"] for cell in report["cells"] if cell["signal"] == "ghost") == 288
        assert sum(cell["losses"] for cell in report["cells"] if cell["signal"] == "spot") == 288
        assert all(row["history_folded_version"] == row["version"] for row in pool.connection.audit.values())
        assert (await store.maintain(now))["cells"] == report["cells"]
    asyncio.run(scenario())
