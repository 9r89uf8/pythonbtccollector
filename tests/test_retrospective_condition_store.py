import asyncio
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json

import pytest

from price_collector.ghost_twap import GhostPolicy
from price_collector.settlement_history import daily_summary, observe, retrospective_cohort
from price_collector.settlement_store import DAY_MS, STOP_BYTES, SettlementStore, capture_outcome
from test_settlement_store import Context, START, decision, resolution

POLICY = asdict(GhostPolicy(enabled=True))
NOW = START + 3 * DAY_MS


def source(*, start=START, final=True):
    frozen, state = decision(market_start_ms=start, prices=("100.03", "100.01", "99.97"))
    frozen.update(model_version="chainlink-60s-offset3-settlement-v1", policy=POLICY)
    body = observe({}, frozen, state, eligible=True)
    body = capture_outcome(body, resolution(last_checked_ms=start + 310_000), NOW)
    day = daily_summary([body], day_ms=start, now_ms=NOW, final=final)
    day["persistence_complete"] = True
    return body, day


class Connection:
    def __init__(self):
        self.days, self.markets, self.sql = {}, {}, []
        self.market_updated = {}
        self.bytes = 519_946_240
        self.writes = 0
    def add(self, body, day):
        self.days[(day["cohort"], day["day_ms"])] = dict(cohort=day["cohort"], day_ms=day["day_ms"],
            final=day["final"], body_json=json.dumps(day), updated_ms=NOW)
        self.markets[(body["cohort"], body["market_id"])] = deepcopy(body)
        self.market_updated[(body["cohort"], body["market_id"])] = NOW
    def transaction(self): return Context(self)
    async def execute(self, sql, *args):
        self.sql.append((sql, args))
        if "INSERT INTO settlement_history_daily" in sql:
            previous = self.days.get(args[:2])
            assert not previous or not previous["final"]
            self.days[args[:2]] = dict(cohort=args[0], day_ms=args[1], updated_ms=args[2],
                                      final=args[3], body_json=args[4])
            self.writes += 1
    async def fetchrow(self, sql, *args):
        self.sql.append((sql, args))
        if "WITH sources AS" in sql:
            candidates = []
            for row in self.days.values():
                body = json.loads(row["body_json"])
                if not args[0] <= row["day_ms"] <= args[1]: continue
                if body["description"]["selection_version"] not in ("first-ack-5s-v1", "first-ack-5s-v2"): continue
                if body["description"].get("model_version") != "chainlink-60s-offset3-settlement-v1": continue
                if body["description"].get("policy", {}) != json.loads(args[3]): continue
                retained = [(key, m) for key, m in self.markets.items() if m["cohort"] == row["cohort"]
                    and row["day_ms"] <= m["market_start_ms"] < row["day_ms"] + DAY_MS
                    and m["market_end_ms"] > args[4] and m["market_end_ms"] + 120000 <= args[6]][:289]
                if not retained: continue
                stamp = max(self.market_updated.get(key, NOW) for key, _ in retained)
                digest = hashlib.sha256(json.dumps({key: value for key, value in body.items()
                    if key != "generated_at_ms"}, sort_keys=True).encode() + f'|{len(retained)}|{stamp}'.encode()).hexdigest()
                if f"{row['cohort']}:{row['day_ms']}:{digest}" in args[5]: continue
                prior = self.days.get((retrospective_cohort(row["cohort"]), row["day_ms"]))
                if prior and (prior["final"] or json.loads(prior["body_json"]).get("source_summary_fingerprint") == digest): continue
                candidates.append((prior is not None, row["day_ms"], row["cohort"], dict(row,
                    source_fingerprint=digest, retained_count=len(retained), retained_updated_ms=stamp)))
            return deepcopy(min(candidates)[3]) if candidates else None
        if "pg_total_relation_size" in sql:
            return dict(relation_bytes=self.bytes, row_count=100)
        return deepcopy(self.days.get(args[:2]))
    async def fetch(self, sql, *args):
        self.sql.append((sql, args))
        assert "LIMIT 289 FOR SHARE" in sql and "JOIN" not in sql
        return [dict(body_json=json.dumps(body), updated_ms=self.market_updated.get(key, NOW))
            for key, body in self.markets.items()
            if body["cohort"] == args[0] and args[1] <= body["market_start_ms"] < args[2]
            and body["market_end_ms"] > args[3] and body["market_end_ms"] + 120_000 <= args[4]][:289]


class Pool:
    def __init__(self): self.connection = Connection()
    def acquire(self, *, timeout): return Context(self.connection)


def store(pool):
    return SettlementStore(pool, market_conditions=True, history_policy=POLICY)


def test_backfill_writes_only_one_separate_compact_day_and_is_idempotent():
    async def run():
        pool = Pool()
        body, day = source()
        pool.connection.add(body, day)
        original = deepcopy(pool.connection.days)
        original_markets = deepcopy(pool.connection.markets)
        instance = store(pool)
        result = await instance._backfill_legacy_conditions(NOW)
        assert result["processed_groups"] == 1
        assert len(pool.connection.days) == 2
        derived = pool.connection.days[(retrospective_cohort(day["cohort"]), START)]
        saved = json.loads(derived["body_json"])
        assert derived["final"] and saved["final"]
        assert len(derived["body_json"].encode()) < 65536
        assert len(saved["cells"]) == 1
        assert saved["cells"][0][4:7] == [1, 0, 0]
        assert pool.connection.markets == original_markets
        assert all(pool.connection.days[key] == value for key, value in original.items())
        again = await instance._backfill_legacy_conditions(NOW)
        assert again["status"] == "complete" and pool.connection.writes == 1
        assert not any("settlement_audit" in sql and "INSERT" in sql for sql, _ in pool.connection.sql)
    asyncio.run(run())


def test_backfill_prioritizes_unprocessed_days_and_never_replaces_final_rows():
    async def run():
        pool = Pool()
        body, day = source(final=False)
        pool.connection.add(body, day)
        instance = store(pool)
        await instance._backfill_legacy_conditions(NOW)
        original_key = day["cohort"], START
        changed = json.loads(pool.connection.days[original_key]["body_json"])
        changed["persistence_complete"] = False
        pool.connection.days[original_key]["body_json"] = json.dumps(changed)
        pool.connection.add(*source(start=START + DAY_MS))
        result = await instance._backfill_legacy_conditions(NOW)
        assert result["last_day_ms"] == START + DAY_MS
        assert result["processed_groups"] == 1 and pool.connection.writes == 2
        final_key = retrospective_cohort(day["cohort"]), START + DAY_MS
        original_final = deepcopy(pool.connection.days[final_key])
        await instance._backfill_legacy_conditions(NOW)
        assert pool.connection.days[final_key] == original_final
    asyncio.run(run())


@pytest.mark.parametrize("used, expected", [(519_946_240, "pending"),
    (STOP_BYTES - 16 * 1024 * 1024 - 16_384, "capacity_paused"), (STOP_BYTES, "capacity_paused")])
def test_capacity_reserves_actual_compact_bytes_and_preserves_existing_limits(used, expected):
    async def run():
        pool = Pool()
        pool.connection.add(*source())
        pool.connection.bytes = used
        result = await store(pool)._backfill_legacy_conditions(NOW)
        assert result["status"] == expected
        assert pool.connection.writes == (1 if expected == "pending" else 0)
        if expected == "capacity_paused":
            assert result["pending"] and result["reason"] == "history_capacity_reserve"
    asyncio.run(run())


def test_generation_timestamp_refresh_does_not_rewrite_mutable_derived_day():
    async def run():
        pool = Pool()
        body, day = source(final=False)
        pool.connection.add(body, day)
        instance = store(pool)
        await instance._backfill_legacy_conditions(NOW)
        original = pool.connection.days[(day["cohort"], START)]
        refreshed = json.loads(original["body_json"])
        refreshed["generated_at_ms"] += 30_000
        original.update(updated_ms=NOW + 30_000, body_json=json.dumps(refreshed))
        result = await instance._backfill_legacy_conditions(NOW + 30_000)
        assert result["status"] == "complete" and pool.connection.writes == 1
    asyncio.run(run())


def test_joint_pair_changes_are_detected_even_when_daily_marginals_are_unchanged():
    async def run():
        pool = Pool()
        first, _ = source(final=False)
        frozen, state = decision(market_start_ms=START + 300_000, prices=("100.03", "100.03", "99.99"))
        frozen.update(model_version="chainlink-60s-offset3-settlement-v1", policy=POLICY)
        second = capture_outcome(observe({}, frozen, state, eligible=True), resolution(), NOW)
        day = daily_summary([first, second], day_ms=START, now_ms=NOW, final=False)
        day["persistence_complete"] = True
        pool.connection.add(first, day)
        pool.connection.add(second, day)
        instance = store(pool)
        await instance._backfill_legacy_conditions(NOW)
        key = retrospective_cohort(day["cohort"]), START
        before = json.loads(pool.connection.days[key]["body_json"])["cells"]
        first_key, second_key = (first["cohort"], first["market_id"]), (second["cohort"], second["market_id"])
        a, b = (pool.connection.markets[item]["buckets"]["25-30"]["signals"] for item in (first_key, second_key))
        a["spot"], b["spot"] = b["spot"], a["spot"]
        pool.connection.market_updated[first_key] += 30_000
        pool.connection.market_updated[second_key] += 30_000
        await instance._backfill_legacy_conditions(NOW + 30_000)
        after = json.loads(pool.connection.days[key]["body_json"])["cells"]
        assert after != before and pool.connection.writes == 2
        assert (await instance._backfill_legacy_conditions(NOW + 30_000))["status"] == "complete"
    asyncio.run(run())


def test_semantically_identical_market_revision_is_not_written_repeatedly():
    async def run():
        pool = Pool()
        body, day = source(final=False)
        pool.connection.add(body, day)
        instance = store(pool)
        await instance._backfill_legacy_conditions(NOW)
        pool.connection.market_updated[(body["cohort"], body["market_id"])] += 30_000
        result = await instance._backfill_legacy_conditions(NOW + 30_000)
        assert result["reason"] == "unchanged_conditions"
        assert (await instance._backfill_legacy_conditions(NOW + 30_000))["status"] == "complete"
        assert pool.connection.writes == 1
    asyncio.run(run())


def test_source_expiry_cannot_recreate_retained_markets_or_count_expired_rows():
    async def run():
        pool = Pool()
        pool.connection.add(*source())
        result = await store(pool)._backfill_legacy_conditions(START + 7 * DAY_MS + 300_000)
        assert result["status"] == "complete" and pool.connection.writes == 0
        assert len(pool.connection.markets) == 1
    asyncio.run(run())


def test_partial_retention_preserves_source_denominator_and_records_missing_markets():
    async def run():
        pool = Pool()
        first, _ = source()
        frozen, state = decision(market_start_ms=START + 600_000, prices=("100.03", "100.01", "99.97"))
        frozen.update(model_version="chainlink-60s-offset3-settlement-v1", policy=POLICY)
        second = capture_outcome(observe({}, frozen, state, eligible=True), resolution(), NOW)
        day = daily_summary([first, second], day_ms=START, now_ms=NOW, final=True)
        day["persistence_complete"] = True
        pool.connection.add(first, day)
        pool.connection.add(second, day)
        await store(pool)._backfill_legacy_conditions(START + 7 * DAY_MS + 300_000)
        saved = json.loads(pool.connection.days[(retrospective_cohort(day["cohort"]), START)]["body_json"])
        assert saved["observed_markets"] == 2 and saved["retained_markets"] == 1
        assert saved["unavailable_retained_markets"] == 1
        assert saved["outcome_freeze_ms"] == day["outcome_freeze_ms"]
        assert not saved["reconstruction_complete"] and not saved["persistence_complete"]
        assert len(pool.connection.markets) == 2
    asyncio.run(run())


def test_new_market_rows_wait_for_the_original_daily_summary_to_catch_up():
    async def run():
        pool = Pool()
        body, day = source(final=False)
        pool.connection.add(body, day)
        extra = deepcopy(body)
        extra.update(market_id=body["market_id"] + 1,
                     market_start_ms=body["market_start_ms"] + 300_000,
                     market_end_ms=body["market_end_ms"] + 300_000)
        pool.connection.markets[(extra["cohort"], extra["market_id"])] = extra
        result = await store(pool)._backfill_legacy_conditions(NOW)
        assert result["pending"] and result["reason"] == "source_summary_pending"
        assert pool.connection.writes == 0
    asyncio.run(run())


def test_more_than_one_calendar_day_of_markets_fails_before_any_write():
    async def run():
        pool = Pool()
        body, day = source()
        pool.connection.add(body, day)
        for index in range(1, 289):
            extra = deepcopy(body)
            extra["market_id"] += index
            pool.connection.markets[(extra["cohort"], extra["market_id"])] = extra
        with pytest.raises(ValueError, match="fixed calendar"):
            await store(pool)._backfill_legacy_conditions(NOW)
        assert pool.connection.writes == 0
    asyncio.run(run())
