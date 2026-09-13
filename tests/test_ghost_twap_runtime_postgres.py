"""Opt-in full ghost-runtime audit path in a disposable PostgreSQL database.

Real engine/runtime, real fsynced local outbox and real PostgreSQL; Redis and
receipt clocks are controlled test doubles. These are not latency or accuracy
measurements from a live feed. No environment opt-in means all tests skip.
"""
import asyncio
from copy import deepcopy
from decimal import Decimal, localcontext
from hashlib import sha256
import json
import os
from pathlib import Path

import asyncpg
import pytest

from price_collector.ghost_twap import HORIZONS, MATH_CONTEXT, NS_PER_MS, NS_PER_SECOND
from price_collector.ghost_twap_runtime import (
    CANARY_MS, GHOST_CHANNEL, GHOST_KEY, MATCH_NS, PUBLISH_LUA, RESERVE_BYTES,
    GhostRuntime, GhostSettings,
)
from price_collector.ghost_twap_spool import GhostSpool
from price_collector.ghost_twap_store import (
    GhostAuditConflict, GhostAuditStore, encode_export_row, iter_export_proofs,
)

DSN = os.environ.get("GHOST_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="requires explicitly provisioned disposable PostgreSQL")
ROOT = Path(__file__).resolve().parents[1]


async def prepare():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3, command_timeout=5)
    try:
        async with pool.acquire() as connection:
            name = await connection.fetchval("SELECT current_database()")
            prefix = "ghost_checkpoint_b_validation_"
            if not name.startswith(prefix) or len(name) <= len(prefix):
                raise RuntimeError("refusing non-disposable validation database")
            await connection.execute("DROP TABLE IF EXISTS public.ghost_twap_audit CASCADE")
            sql = (ROOT / "schema.sql").read_text(encoding="utf-8").split(
                "CREATE TABLE IF NOT EXISTS providers", 1)[0]
            await connection.execute(sql)
            now = await connection.fetchval("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
        # Synthetic decisions are old enough to exercise real 96h expiry, while
        # their injected runtime clocks still lie inside their frozen campaign.
        base = (now // 1000) * 1000 - 98 * 60 * 60 * 1000
        return pool, base
    except BaseException:
        await pool.close()
        raise


class Clock:
    def __init__(self, base):
        self.base = base
        self.wall = base * NS_PER_MS
        self.mono = 0

    def at(self, second, offset_ms=0):
        mono = second * NS_PER_SECOND + offset_ms * NS_PER_MS
        assert mono >= self.mono, "test receipt clock must never regress"
        self.mono = mono
        self.wall = self.base * NS_PER_MS + mono

    def advance(self, ns):
        self.wall += ns
        self.mono += ns


def price(second, feed="spot"):
    fraction = int(sha256(f"{feed}:{second}".encode()).hexdigest()[:15], 16) % 10**18
    with localcontext(MATH_CONTEXT):
        return Decimal(60000 + second % 7) + Decimal(fraction) / Decimal(10**18)


def exact_hash(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class PublishedRedis:
    """Inspect the actual fsynced intent before acknowledging a fake publish."""
    def __init__(self, clock):
        self.clock = clock
        self.runtime = None
        self.calls = []
        self.fail_on = set()
        self.max_spool_intent_bytes = 0

    async def eval(self, script, key_count, key, channel, body, ttl_ms):
        assert (script, key_count, key, channel) == (PUBLISH_LUA, 2, GHOST_KEY, GHOST_CHANNEL)
        assert isinstance(body, bytes) and 0 < ttl_ms <= 3000
        live = json.loads(body)
        row = self.runtime.records[live["decision_id"]]
        path = self.runtime.spool._path(row.record())
        raw = await asyncio.to_thread(path.read_bytes)
        self.max_spool_intent_bytes = max(self.max_spool_intent_bytes, len(raw))
        durable = json.loads(raw)
        intent = json.loads(durable["state_json"])["publication"]
        assert intent["status"] == "intent"
        assert durable["frozen_json"] == row.frozen_json
        assert "ack_wall_ns" not in intent and "ack_monotonic_ns" not in intent
        assert live["publication_state"] == "attempted"
        assert live["audit_state"] == "durable_outbox_postgres_pending"
        assert "slots" not in live and "slot_inputs" not in live
        assert isinstance(live["decision_wall_ns"], str)
        assert all(isinstance(f["price"], str) for f in live["forecasts"])
        self.calls.append(dict(run_id=live["run_id"], decision_id=live["decision_id"],
                               payload=body, ttl_ms=ttl_ms))
        self.clock.advance(2 * NS_PER_MS)
        if len(self.calls) in self.fail_on:
            raise TimeoutError("test: publication may have occurred but its acknowledgement was lost")
        return 1


async def make_runtime(directory, store, base, *, clock=None):
    clock = Clock(base) if clock is None else clock
    settings = GhostSettings(enabled=True, canary_start_ms=base, state_directory=directory)
    spool = GhostSpool(directory, settings.audit_max_records, settings.record_max_bytes)
    redis = PublishedRedis(clock)
    runtime = GhostRuntime(settings, store, redis, spool, wall_ns=lambda: clock.wall,
                           mono_ns=lambda: clock.mono, disk_free=lambda: 2 * RESERVE_BYTES)
    redis.runtime = runtime
    await runtime._spool(spool.open)
    runtime.campaign = await runtime._spool(spool.campaign, base)
    runtime._end_mono = clock.mono + CANARY_MS * NS_PER_MS
    runtime.initial_rows = (await store.initialize())["row_count"]
    await runtime.refresh_guard()
    return runtime, clock, redis


def offer(runtime, clock, second, feed="spot", *, offset_ms=0, value=None, event_id=None):
    clock.at(second, offset_ms)
    runtime.offer_price(feed, price(second, feed) if value is None else value,
                        clock.base + second * 1000, clock.wall, clock.mono,
                        event_id or f"{feed}:{second}:{offset_ms}", 60 if feed == "twap" else None)


def warm(runtime, clock):
    for second in range(62):
        offer(runtime, clock, second)
        runtime.drain_inputs()
    offer(runtime, clock, 61, "twap", offset_ms=100)
    runtime.drain_inputs()


async def issue_and_publish(runtime):
    await runtime.refresh_guard()
    row = runtime.issue()
    assert row is not None, runtime.stop_reason
    assert len(row.decision.slots) == 89
    assert all(f.price is not None for f in row.decision.forecasts)
    # This is the exact handoff performed by _publication_loop; leaving its
    # pending marker set would falsely coalesce a publication already sent.
    assert runtime._pending_publication == row.decision.decision_id
    runtime._pending_publication = None
    await runtime.publish(row)
    return row


async def flush(runtime):
    for _ in range(32):
        await runtime.flush_audit_once()
        if not runtime._dirty and not runtime.late_events and not runtime._campaign_dirty:
            return
    raise AssertionError("bounded audit flush did not drain")


async def all_rows(store):
    rows, after = [], None
    while True:
        page = await store.export_page(after=after)
        if not page:
            return rows
        rows.extend(page)
        after = (page[-1]["run_id"], page[-1]["decision_id"])


def test_postgres_real_runtime_rows_spool_publication_targets_and_storage(tmp_path):
    async def scenario():
        pool, base = await prepare()
        runtime = None
        try:
            store = GhostAuditStore(pool)
            runtime, clock, redis = await make_runtime(tmp_path / "runtime-outbox", store, base)
            baseline = await store.measure(include_count=True)
            warm(runtime, clock)
            redis.fail_on = {5}
            issued = []
            max_frozen = max_state = max_combined = max_outbox = max_live = 0

            def sizes(row):
                nonlocal max_frozen, max_state, max_combined, max_outbox, max_live
                record = row.record()
                frozen_bytes, state_bytes = (len(record[field].encode()) for field in ("frozen_json", "state_json"))
                max_frozen, max_state = max(max_frozen, frozen_bytes), max(max_state, state_bytes)
                max_combined = max(max_combined, frozen_bytes + state_bytes)
                max_outbox = max(max_outbox, len(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()))
                payload = row.state["publication"].get("payload_json", "")
                max_live = max(max_live, len(payload.encode()))

            # Two event-triggered decisions per second: spot first, then the
            # same source-second TWAP 100ms later. This includes unseen targets
            # whose source stamp is already at the local decision time.
            for second in range(62, 94):
                offer(runtime, clock, second)
                row = await issue_and_publish(runtime)
                issued.append(row)
                sizes(row)
                await flush(runtime)
                offer(runtime, clock, second, "twap", offset_ms=100)
                row = await issue_and_publish(runtime)
                issued.append(row)
                sizes(row)
                await flush(runtime)
            after_publications = await store.measure(include_count=True)
            assert len(issued) == len(redis.calls) == after_publications["row_count"] == 64
            first_frozen_hashes = {row.decision.decision_id: sha256(row.frozen_json.encode()).hexdigest() for row in issued}
            for second in range(94, 124):
                offer(runtime, clock, second)
                offer(runtime, clock, second, "twap", offset_ms=100)
                runtime.drain_inputs()
                await flush(runtime)
            assert all(all(t["status"] == "matched" for t in row.state["targets"].values()) for row in issued)

            # The first already-received target never changes when a later
            # conflicting price arrives; conflict invalidates scoring eligibility.
            first = issued[0]
            matched = first.state["targets"]["1"]
            first_event_hash = exact_hash(matched["first_event"])
            raw_error = matched["error"]
            clock.advance(100 * NS_PER_MS)
            runtime.offer_price("twap", Decimal(matched["first_event"]["value"]) + Decimal(1),
                                matched["target_source_timestamp_ms"], clock.wall, clock.mono,
                                "later-conflict", 60)
            runtime.drain_inputs()
            await flush(runtime)
            assert exact_hash(matched["first_event"]) == first_event_hash
            assert matched["error"] == raw_error and matched["conflicted"] is True
            assert matched["confirmed_redis_lead_ns"] is None

            sent = {call["decision_id"]: call["payload"].decode() for call in redis.calls}
            for row in issued:
                sizes(row)
                frozen = json.loads(row.frozen_json)
                assert set(("runtime_policy", "runtime_counters", "operational_gaps", "horizon_recovery")) <= set(frozen)
                assert all("max_interior_carry_ms" in f for f in frozen["forecasts"])
                assert row.state["publication"]["payload_json"] == sent[row.decision.decision_id]
                for target in row.state["targets"].values():
                    if row.state["publication"]["status"] == "uncertain":
                        assert target["confirmed_redis_lead_ns"] is None
                    elif not target["conflicted"]:
                        assert target["confirmed_redis_lead_ns"] > 0
                        assert target["confirmed_redis_lead_ns"] == (
                            int(target["first_event"]["received_monotonic_ns"])
                            - row.state["publication"]["ack_monotonic_ns"])
            after_targets = await store.measure(include_count=True)
            deadline = issued[-1].decision.decision_monotonic_ns + MATCH_NS
            clock.advance(deadline - clock.mono)
            runtime.finalize_due()
            for row in issued:
                assert row.terminal
                sizes(row)
            await flush(runtime)
            assert not runtime.records and not runtime.spool.read_all()
            after_terminal = await store.measure(include_count=True)
            rows = await all_rows(store)
            assert len(rows) == 64 and all(row["terminal"] for row in rows)
            assert all(row["frozen_sha256"] == first_frozen_hashes[row["decision_id"]] for row in rows)
            assert sum(len(json.loads(row["state_json"])["targets"]) for row in rows) == 384
            assert runtime.stop_reason is None

            exported = b"".join(encode_export_row(row) for row in rows)
            path = tmp_path / "runtime-export.jsonl"
            path.write_bytes(exported)
            digest = sha256(exported).hexdigest()
            proofs = list(iter_export_proofs(path, expected_sha256=digest, expected_rows=64))
            result = await store.mark_verified_export(proofs, export_sha256=digest,
                                                     external_location="test-owner-computer:/runtime-export.jsonl")
            assert len(result["verified"]) == 64
            expired = 0
            while True:
                batch = await store.expire_verified(limit=20)
                if not batch:
                    break
                expired += len(batch)
            assert expired == 64
            after_delete = await store.measure(include_count=True)
            async with pool.acquire() as connection:
                await connection.execute("VACUUM (ANALYZE) public.ghost_twap_audit", timeout=20)
            after_vacuum = await store.measure(include_count=True)
            report = dict(sample_rows=64, horizons_per_row=6, slots_per_row=89,
                          actual_runtime_frozen_metadata=True, actual_live_payload_in_state=True,
                          actual_fsynced_spool=True, simulated_receipt_clocks=True, fake_redis=True,
                          publication_attempts=len(redis.calls), acknowledged=runtime.counters["redis_acknowledged"],
                          uncertain=runtime.counters["redis_uncertain"], matched_targets=384,
                          max_frozen_json_bytes=max_frozen, max_state_json_bytes=max_state,
                          max_complete_json_bytes=max_combined, max_serialized_outbox_bytes=max_outbox,
                          max_durable_intent_bytes=redis.max_spool_intent_bytes, max_live_payload_bytes=max_live,
                          baseline=baseline, after_publications=after_publications,
                          after_all_targets=after_targets, after_terminal=after_terminal,
                          after_delete=after_delete, after_vacuum=after_vacuum,
                          export_bytes=len(exported), export_sha256=digest, expired_rows=expired,
                          limitation="64 synthetic two-per-second runtime decisions; not live Redis latency, full-cap load or 72h canary evidence.")
            report_path = Path(os.environ.get("GHOST_TEST_RUNTIME_STORAGE_REPORT", tmp_path / "runtime_storage.json"))
            report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(report, sort_keys=True))
        finally:
            if runtime is not None:
                await runtime._spool(runtime.spool.close)
            await pool.close()
    asyncio.run(scenario())


async def receive_horizons(runtime, clock, row, horizons=HORIZONS):
    for horizon in horizons:
        target = row.state["targets"][str(horizon)]
        second = (target["target_source_timestamp_ms"] - clock.base) // 1000
        offer(runtime, clock, second, "twap", offset_ms=100, event_id=f"first:{horizon}")
        runtime.drain_inputs()


async def restart_without_workers(runtime, clock, store):
    await runtime._spool(runtime.spool.close)
    restarted, _, redis = await make_runtime(runtime.spool.directory, store, clock.base, clock=clock)
    assert restarted.engine.history_size == 0
    for saved in await restarted._spool(restarted.spool.read_all):
        await restarted._recover(saved)
        await restarted._spool(restarted.spool.remove, saved)
    assert not redis.calls
    return restarted


def test_postgres_runtime_restart_prefers_newest_evidence_and_preserves_first_hash(tmp_path):
    async def scenario():
        pool, base = await prepare()
        open_runtimes = []
        try:
            store = GhostAuditStore(pool)
            runtime, clock, _ = await make_runtime(tmp_path / "newer-spool", store, base)
            open_runtimes.append(runtime)
            warm(runtime, clock)
            row = await issue_and_publish(runtime)
            await flush(runtime)
            older_db = await store.get_record(row.decision.run_id, row.decision.decision_id)
            await receive_horizons(runtime, clock, row)
            clock.advance(row.decision.decision_monotonic_ns + MATCH_NS - clock.mono)
            runtime.finalize_due()
            terminal_spool = row.record()
            assert terminal_spool["terminal"] and terminal_spool["version"] > older_db["version"]
            # The durable contract encodes nanosecond clocks as strings. Compare
            # the exact serialized evidence on both sides, not in-memory ints
            # against their deliberate JSON representation.
            terminal_state = json.loads(terminal_spool["state_json"])
            expected_firsts = {h: exact_hash(t["first_event"]) for h, t in terminal_state["targets"].items()}
            await runtime._spool(runtime.spool.write, terminal_spool)
            # Simulate crash after terminal outbox fsync, before its PostgreSQL commit.
            restarted = await restart_without_workers(runtime, clock, store)
            open_runtimes.append(restarted)
            stored = await store.get_record(row.decision.run_id, row.decision.decision_id)
            assert stored["version"] >= terminal_spool["version"] and stored["terminal"]
            assert stored["state_json"] == terminal_spool["state_json"]
            state = json.loads(stored["state_json"])
            assert {h: exact_hash(t["first_event"]) for h, t in state["targets"].items()} == expected_firsts
            assert all(t["status"] == "matched" for t in state["targets"].values())
            assert stored["frozen_sha256"] == sha256(row.frozen_json.encode()).hexdigest()

            # PostgreSQL may subsequently be ahead of a retained old outbox due
            # to terminal late-target annotations. Recovery preserves that version.
            await restarted._spool(restarted.spool.write, terminal_spool)
            target = state["targets"]["1"]
            late = dict(target["first_event"], event_id="terminal-conflict", value="61000.000000000000000000",
                        received_wall_ns=str(clock.wall), received_monotonic_ns=str(clock.mono))
            assert (await store.note_late_target(late))["updated"] == 1
            higher = await store.get_record(row.decision.run_id, row.decision.decision_id)
            await restarted._recover(terminal_spool)
            unchanged = await store.get_record(row.decision.run_id, row.decision.decision_id)
            assert unchanged["version"] == higher["version"] and unchanged["state_sha256"] == higher["state_sha256"]
            await restarted._spool(restarted.spool.remove, terminal_spool)

            # A newer nonterminal DB record already has a durable first match;
            # restart fills only unresolved targets and retains its exact score.
            other, other_clock, _ = await make_runtime(tmp_path / "newer-database", store, base)
            open_runtimes.append(other)
            warm(other, other_clock)
            other_row = await issue_and_publish(other)
            await flush(other)
            old_outbox = (await other._spool(other.spool.read_all))[0]
            await receive_horizons(other, other_clock, other_row, (1,))
            await store.persist(other_row.record())
            higher = await store.get_record(other_row.decision.run_id, other_row.decision.decision_id)
            expected_match = deepcopy(json.loads(higher["state_json"])["targets"]["1"])
            assert higher["version"] > old_outbox["version"] and not higher["terminal"]
            recovered = await restart_without_workers(other, other_clock, store)
            open_runtimes.append(recovered)
            saved = await store.get_record(other_row.decision.run_id, other_row.decision.decision_id)
            saved_state = json.loads(saved["state_json"])
            assert saved["terminal"] and saved["version"] > higher["version"]
            assert saved_state["targets"]["1"] == expected_match
            assert all(t["status"] == "restart_unmatched" for h, t in saved_state["targets"].items() if h != "1")

            # Equal-version divergent status is an error, not a favorable merge;
            # disk evidence must remain available when startup reconciliation fails.
            divergent = {key: saved[key] for key in ("run_id", "decision_id", "decision_wall_ns", "created_ms",
                                                     "frozen_json", "state_json", "version", "terminal")}
            divergent_state = json.loads(divergent["state_json"])
            divergent_state["unexpected_same_version"] = True
            divergent["state_json"] = json.dumps(divergent_state)
            await recovered._spool(recovered.spool.write, divergent)
            with pytest.raises(ValueError, match="same-version"):
                await recovered._recover(divergent)
            assert len(await recovered._spool(recovered.spool.read_all)) == 1
            assert (await store.get_record(saved["run_id"], saved["decision_id"]))["state_sha256"] == saved["state_sha256"]
            changed = deepcopy(saved_state)
            changed["targets"]["1"]["first_event"]["value"] = "1.000000000000000000"
            with pytest.raises(GhostAuditConflict, match="first target"):
                await store.persist(dict(saved, state_json=json.dumps(changed), version=saved["version"] + 1))
        finally:
            for runtime in open_runtimes:
                await runtime._spool(runtime.spool.close)
            await pool.close()
    asyncio.run(scenario())


def test_postgres_blocked_audit_commit_does_not_block_fsynced_redis_publication(tmp_path):
    async def scenario():
        pool, base = await prepare()
        runtime, pending_flush = None, None
        release = asyncio.Event()
        try:
            real_store = GhostAuditStore(pool)
            entered = asyncio.Event()

            class BlockFirstPersist:
                def __init__(self):
                    self.block = True

                def __getattr__(self, name):
                    return getattr(real_store, name)

                async def persist(self, record):
                    if self.block:
                        self.block = False
                        entered.set()
                        await release.wait()
                    return await real_store.persist(record)

            runtime, clock, redis = await make_runtime(tmp_path / "blocked-pg", BlockFirstPersist(), base)
            warm(runtime, clock)
            await runtime.refresh_guard()
            row = runtime.issue()
            assert row is not None
            runtime._pending_publication = None
            pending_flush = asyncio.create_task(runtime.flush_audit_once())
            await asyncio.wait_for(entered.wait(), timeout=2)
            await asyncio.wait_for(runtime.publish(row), timeout=1.5)
            assert len(redis.calls) == 1 and row.state["publication"]["status"] == "acknowledged"
            assert not release.is_set() and not pending_flush.done()
            assert await real_store.get_record(row.decision.run_id, row.decision.decision_id) is None
            assert (await runtime._spool(runtime.spool.read_all))[0]["frozen_json"] == row.frozen_json
            release.set()
            await pending_flush
            await flush(runtime)
            saved = await real_store.get_record(row.decision.run_id, row.decision.decision_id)
            assert json.loads(saved["state_json"])["publication"]["payload_json"] == redis.calls[0]["payload"].decode()
            assert row.decision.decision_id in runtime.records, "reservation remains until terminal commit"
            clock.advance(MATCH_NS)
            runtime.finalize_due()
            await flush(runtime)
            assert not runtime.records and not runtime.spool.read_all()
            assert (await real_store.get_record(row.decision.run_id, row.decision.decision_id))["terminal"]
        finally:
            release.set()
            if pending_flush is not None:
                await asyncio.gather(pending_flush, return_exceptions=True)
            if runtime is not None:
                await runtime._spool(runtime.spool.close)
            await pool.close()
    asyncio.run(scenario())
