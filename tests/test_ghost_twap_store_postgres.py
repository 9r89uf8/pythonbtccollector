"""Opt-in destructive tests ONLY in a separately provisioned disposable database.

Set GHOST_TEST_POSTGRES_DSN to a database whose name begins exactly with
ghost_checkpoint_b_validation_. These tests never create/drop a database and
refuse the production database regardless of the supplied role. They replace
only the ghost table/function in that disposable database. No opt-in means skip.
"""
import asyncio
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path

import asyncpg
import pytest

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, HORIZONS, PriceEvent
from price_collector.ghost_twap_store import (
    GhostAuditConflict, GhostAuditStore, encode_export_row, iter_export_proofs,
)

DSN = os.environ.get("GHOST_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="requires an explicitly provisioned disposable PostgreSQL database")
SCHEMA = Path(__file__).resolve().parents[1] / "schema.sql"


async def prepare():
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3, command_timeout=5)
    try:
        async with pool.acquire() as connection:
            name = await connection.fetchval("SELECT current_database()")
            if not name.startswith("ghost_checkpoint_b_validation_") or len(name) <= len("ghost_checkpoint_b_validation_"):
                raise RuntimeError("refusing any database except the named disposable validation database")
            await connection.execute("DROP TABLE IF EXISTS public.ghost_twap_audit CASCADE")
            schema = SCHEMA.read_text(encoding="utf-8")
            sql = schema.split("CREATE TABLE IF NOT EXISTS providers", 1)[0]
            # Only this disposable table receives the real production ACL.
            # price_writer must already exist; never create/alter a cluster role.
            sql += schema.split("-- Ghost audit privilege boundary.", 1)[1].split(
                "-- End ghost audit privilege boundary.", 1)[0]
            await connection.execute(sql)
            await connection.execute(sql)  # Migration is repeatable on its own table.
        return pool
    except BaseException:
        await pool.close()
        raise


def row(created_ms, decision_id="d1", *, terminal=False):
    frozen = {"run_id": "integration", "decision_id": decision_id,
              "decision_wall_ns": str(created_ms * 1_000_000),
              "forecasts": [{"horizon_s": 1, "target_source_timestamp_ms": created_ms + 1000}],
              "price": "60000.123456789012345678"}
    state = {"targets": {"1": {"target_source_timestamp_ms": created_ms + 1000,
                              "status": "missing" if terminal else "pending", "first_event": None}}}
    return dict(run_id="integration", decision_id=decision_id, created_ms=created_ms,
                decision_wall_ns=created_ms * 1_000_000, frozen_json=json.dumps(frozen),
                state_json=json.dumps(state), version=0, terminal=terminal)


def event(stamp, *, value="60001.123456789012345678", event_id="e1"):
    return dict(feed="twap", event_id=event_id, value=value, source_timestamp_ms=stamp,
                received_wall_ns=str((stamp + 1000) * 1_000_000),
                received_monotonic_ns="1000000000", window_s=60)


async def stored(pool, decision_id="d1"):
    async with pool.acquire() as connection:
        return dict(await connection.fetchrow("SELECT * FROM ghost_twap_audit WHERE run_id='integration' AND decision_id=$1",
                                               decision_id))


def test_postgres_guards_recovery_late_updates_export_and_atomic_expiry(tmp_path):
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            assert (await store.initialize())["row_count"] == 0
            async with pool.acquire() as connection:
                now_ms = await connection.fetchval("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            old_ms = now_ms - 98 * 60 * 60 * 1000
            original = row(old_ms)
            # Identical simultaneous insert retries converge to exactly one row.
            outcomes = await asyncio.gather(store.persist(original), store.persist(original))
            assert {x["outcome"] for x in outcomes} == {"inserted", "unchanged"}
            assert (await store.measure(include_count=True))["row_count"] == 1
            assert (await stored(pool))["frozen_sha256"] == sha256(original["frozen_json"].encode()).hexdigest()
            assert (await store.initialize())["incomplete_count"] == 1
            assert len(await store.list_incomplete(limit=1)) == 1
            async with pool.acquire() as connection:
                with pytest.raises(asyncpg.PostgresError, match="immutable"):
                    await connection.execute("UPDATE ghost_twap_audit SET frozen_json='{}' WHERE decision_id='d1'")
                with pytest.raises(asyncpg.PostgresError, match="new version"):
                    await connection.execute("UPDATE ghost_twap_audit SET state_json='{}' WHERE decision_id='d1'")
                with pytest.raises(asyncpg.PostgresError, match="age96h"):
                    await connection.execute("DELETE FROM ghost_twap_audit WHERE decision_id='d1'")
            restored = dict(original, version=1, terminal=True)
            state = json.loads(restored["state_json"])
            state["restart"] = True
            state["targets"]["1"]["status"] = "restart_unmatched"
            restored["state_json"] = json.dumps(state)
            await store.persist(restored)
            assert (await store.initialize())["incomplete_count"] == 0
            assert (await store.persist(original))["outcome"] == "stale"
            with pytest.raises(GhostAuditConflict):
                await store.persist(dict(restored, state_json='{"changed":true}'))

            async def verify_and_mark():
                rows = await store.export_page()
                content = b"".join(encode_export_row(item) for item in rows)
                export_path = tmp_path / "verified.jsonl"
                export_path.write_bytes(content)
                digest = sha256(content).hexdigest()
                proofs = list(iter_export_proofs(export_path, expected_sha256=digest, expected_rows=len(rows)))
                marked = await store.mark_verified_export(proofs, export_sha256=digest,
                                                          external_location="test-owner-computer:/verified.jsonl")
                return proofs, marked

            proofs, marked = await verify_and_mark()
            assert marked["verified"] == [("integration", "d1")]
            # Actual subsequent report only annotates a previously missing target.
            late = event(old_ms + 1000)
            assert (await store.note_late_target(late))["updated"] == 1
            changed = await stored(pool)
            assert changed["verified_version"] is None and changed["version"] == 2
            assert json.loads(changed["state_json"])["targets"]["1"]["status"] == "restart_unmatched"
            assert (await store.note_late_target(late))["updated"] == 0
            stale = await store.mark_verified_export(proofs, export_sha256="a" * 64,
                                                     external_location="test-owner-computer:/old.jsonl")
            assert stale["stale_or_ineligible"] == 1
            assert await store.expire_verified() == []
            await verify_and_mark()
            # Another transaction holds the row: bounded expiry skips it.
            async with pool.acquire() as locked:
                async with locked.transaction():
                    await locked.fetchrow("SELECT 1 FROM ghost_twap_audit WHERE decision_id='d1' FOR UPDATE")
                    assert await store.expire_verified(limit=1) == []
            assert await store.expire_verified(limit=1) == [{"run_id": "integration", "decision_id": "d1"}]

            # A fully exported recent row still cannot be deleted before 96h.
            recent = row(now_ms, "recent", terminal=True)
            await store.persist(recent)
            await verify_and_mark()
            assert await store.expire_verified() == []
            async with pool.acquire() as connection:
                with pytest.raises(asyncpg.PostgresError, match="age96h"):
                    await connection.execute("DELETE FROM ghost_twap_audit WHERE decision_id='recent'")

            # Both direct SQL and the backend preserve a first matched report.
            matched = row(old_ms, "matched", terminal=True)
            state = json.loads(matched["state_json"])
            state["targets"]["1"].update(status="matched", first_event=late, error_bps="0.001")
            matched["state_json"] = json.dumps(state)
            await store.persist(matched)
            replacement = deepcopy(state)
            replacement["targets"]["1"]["first_event"]["value"] = "1"
            async with pool.acquire() as connection:
                with pytest.raises(asyncpg.PostgresError, match="first target"):
                    await connection.execute("UPDATE ghost_twap_audit SET version=1,state_json=$1 WHERE decision_id='matched'",
                                             json.dumps(replacement))
            conflict = event(old_ms + 1000, value="61000", event_id="e2")
            assert (await store.note_late_target(conflict))["updated"] == 1
            matched_state = json.loads((await stored(pool, "matched"))["state_json"])["targets"]["1"]
            assert matched_state["first_event"] == late and matched_state["error_bps"] == "0.001"
            assert matched_state["conflicted"] is True
            assert (await store.measure())["row_count"] is None
        finally:
            await pool.close()
    asyncio.run(scenario())


def test_postgres_bounded_representative_storage_probe(tmp_path):
    """128 actual 89-slot engine rows, each with six committed result revisions.

    This measures bounded sample costs, not a 600k-row or 72-hour canary. Optional
    GHOST_TEST_STORAGE_REPORT preserves the measured counts/sizes outside the DB.
    """
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            await store.initialize()
            baseline = await store.measure(include_count=True)
            async with pool.acquire() as connection:
                now_ms = await connection.fetchval("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            base = (now_ms // 1000) * 1000 - 98 * 60 * 60 * 1000
            engine = GhostTwapEngine("probe", GhostPolicy(enabled=True))
            sequence = 0

            def accept(feed, second):
                nonlocal sequence
                sequence += 1
                stamp = base + second * 1000
                fraction = int(sha256(f"{feed}:{second}".encode()).hexdigest()[:15], 16) % 10**18
                price = Decimal(60000) + Decimal(fraction) / Decimal(10**18)
                engine.accept(PriceEvent(feed, price, stamp, stamp * 1_000_000,
                                        second * 1_000_000_000, sequence, f"probe:{sequence}",
                                        60 if feed == "twap" else None))

            for second in range(62):
                accept("spot", second)
            records = []
            max_frozen = max_complete = 0
            for index in range(128):
                second = 62 + index
                accept("spot", second)
                accept("twap", second)
                decision = engine.snapshot(f"d{index:03}", (base + second * 1000) * 1_000_000,
                                           second * 1_000_000_000)
                assert len(decision.slots) == 89
                frozen = decision.to_audit_json().decode()
                state = {"publication": {"status": "acknowledged", "ack_wall_ns": str(decision.decision_wall_ns)},
                         "targets": {str(f.horizon_s): {"target_source_timestamp_ms": f.target_source_timestamp_ms,
                                     "status": "pending", "first_event": None} for f in decision.forecasts}}
                item = dict(run_id="probe", decision_id=decision.decision_id, created_ms=decision.decision_wall_ns // 1_000_000,
                            decision_wall_ns=decision.decision_wall_ns, frozen_json=frozen,
                            state_json=json.dumps(state), version=0, terminal=False)
                await store.persist(item)
                records.append((item, state))
                max_frozen = max(max_frozen, len(frozen.encode()))
            inserted = await store.measure(include_count=True)
            for horizon in HORIZONS:
                for item, state in records:
                    target = state["targets"][str(horizon)]
                    target.update(status="matched", first_event=event(target["target_source_timestamp_ms"],
                                  event_id=f"{item['decision_id']}:{horizon}"), error_bps="0.000000000000000001")
                    item.update(state_json=json.dumps(state), version=item["version"] + 1,
                                terminal=horizon == HORIZONS[-1])
                    max_complete = max(max_complete, len((item["frozen_json"] + item["state_json"]).encode()))
                    await store.persist(item)
            updated = await store.measure(include_count=True)
            assert updated["row_count"] == 128 and updated["tablespaces"] == ["pg_default"]
            rows = []
            after = None
            while True:
                page = await store.export_page(after=after)
                if not page:
                    break
                rows.extend(page)
                after = (page[-1]["run_id"], page[-1]["decision_id"])
            content = b"".join(encode_export_row(item) for item in rows)
            path = tmp_path / "probe.jsonl"
            path.write_bytes(content)
            digest = sha256(content).hexdigest()
            proofs = list(iter_export_proofs(path, expected_sha256=digest, expected_rows=128))
            for offset in range(0, len(proofs), 100):
                assert (await store.mark_verified_export(proofs[offset:offset+100], export_sha256=digest,
                    external_location="test-owner-computer:/probe.jsonl"))["stale_or_ineligible"] == 0
            expired = 0
            while True:
                removed = await store.expire_verified(limit=100)
                if not removed:
                    break
                expired += len(removed)
            assert expired == 128
            after_delete = await store.measure(include_count=True)
            async with pool.acquire() as connection:
                await connection.execute("VACUUM (ANALYZE) public.ghost_twap_audit", timeout=20)
            after_vacuum = await store.measure(include_count=True)
            report = dict(sample_rows=128, slots_per_row=89, committed_result_revisions_per_row=6,
                          max_frozen_json_bytes=max_frozen, max_complete_json_bytes=max_complete,
                          baseline=baseline, after_insert=inserted, after_six_updates=updated,
                          after_delete=after_delete, after_vacuum=after_vacuum,
                          export_bytes=len(content), export_sha256=digest, expired_rows=expired,
                          limitation="Bounded synthetic engine-shaped sample; not full-canary retention/cap validation.")
            report_path = Path(os.environ.get("GHOST_TEST_STORAGE_REPORT", tmp_path / "storage_probe.json"))
            report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(report, sort_keys=True))
        finally:
            await pool.close()
    asyncio.run(scenario())


def test_postgres_writer_can_persist_and_annotate_but_cannot_attest_or_delete(tmp_path):
    async def scenario():
        pool = await prepare()
        writer_pool = None
        try:
            async def writer_role(connection):
                await connection.execute("SET ROLE price_writer")

            writer_pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1,
                                                    command_timeout=5, setup=writer_role)
            writer = GhostAuditStore(writer_pool)
            admin = GhostAuditStore(pool)
            await writer.initialize()
            async with pool.acquire() as connection:
                now_ms = await connection.fetchval("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
            created = now_ms - 98 * 60 * 60 * 1000
            original = row(created)
            assert (await writer.persist(original))["outcome"] == "inserted"
            state = json.loads(original["state_json"])
            state["targets"]["1"]["status"] = "missing"
            terminal = dict(original, state_json=json.dumps(state), version=1, terminal=True)
            assert (await writer.persist(terminal))["outcome"] == "updated"
            # Exercises the same SELECT FOR UPDATE path under column UPDATE grants.
            assert (await writer.note_late_target(event(created + 1000)))["updated"] == 1
            assert (await writer.note_late_target(event(created + 1000)))["updated"] == 0
            current = await stored(pool)
            content = encode_export_row(current)
            path = tmp_path / "writer_boundary_verified.jsonl"
            path.write_bytes(content)
            digest = sha256(content).hexdigest()
            proofs = list(iter_export_proofs(path, expected_sha256=digest, expected_rows=1))
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await writer.mark_verified_export(proofs, export_sha256=digest,
                                                   external_location="owner-computer:/verified.jsonl")
            assert (await admin.mark_verified_export(proofs, export_sha256=digest,
                    external_location="owner-computer:/verified.jsonl"))["verified"] == [("integration", "d1")]
            async with writer_pool.acquire() as connection:
                for sql in (
                    "UPDATE ghost_twap_audit SET frozen_json='{}'",
                    "UPDATE ghost_twap_audit SET verified_export_sha256=repeat('a',64)",
                    "INSERT INTO ghost_twap_audit (verified_export_sha256) VALUES (repeat('a',64))",
                    "DELETE FROM ghost_twap_audit",
                    "TRUNCATE ghost_twap_audit",
                    "ALTER TABLE ghost_twap_audit DISABLE TRIGGER ghost_twap_audit_guard_trigger",
                ):
                    with pytest.raises(asyncpg.InsufficientPrivilegeError):
                        await connection.execute(sql)
            assert (await admin.expire_verified(limit=1)) == [{"run_id": "integration", "decision_id": "d1"}]
        finally:
            if writer_pool is not None:
                await writer_pool.close()
            await pool.close()
    asyncio.run(scenario())


def test_postgres_live_record_exclusion_prevents_same_version_collision():
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            original = row(1000000, terminal=True)
            await store.persist(original)
            late = event(1001000)
            live_state = json.loads(original["state_json"])
            live_state["targets"]["1"]["first_late_event"] = late
            live = dict(original, version=1, state_json=json.dumps(live_state))
            # Deterministic interleaving of the former two version owners.
            assert (await store.note_late_target(late))["updated"] == 1
            with pytest.raises(GhostAuditConflict, match="same state version"):
                await store.persist(live)
            safe = row(1000000, "safe", terminal=True)
            await store.persist(safe)
            unowned = row(1000000, "z_unowned", terminal=True)
            await store.persist(unowned)
            result = await store.note_late_target(late, after=("integration", "d1"), limit=1,
                                                  exclude=[("integration", "safe")])
            assert result == dict(examined=1, updated=1, next_after=("integration", "z_unowned"))
            safe_live = dict(safe, version=1, state_json=json.dumps(live_state))
            assert (await store.persist(safe_live))["outcome"] == "updated"
            assert (await store.note_late_target(late))["updated"] == 0
        finally:
            await pool.close()
    asyncio.run(scenario())


def test_postgres_first_clocks_and_target_results_are_immutable_even_to_direct_sql():
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            original = row(1000000, terminal=True)
            state = json.loads(original["state_json"])
            state["computation_completed_wall_ns"] = "1000"
            state["publication"] = dict(status="acknowledged", attempt_monotonic_ns="1000",
                                        ack_wall_ns="1200", payload_json='{"price":"1"}')
            state["targets"]["1"].update(status="matched", first_event=event(1001000),
                first_conflicting_event=event(1001000, value="62000"), conflicted=True,
                error="1.000000000000000001", error_bps="0.01", persistence_error="2",
                eta_error_ns="3", confirmed_redis_lead_ns="1000")
            original["state_json"] = json.dumps(state)
            await store.persist(original)
            changes = [(("computation_completed_wall_ns",), "2"),
                       (("publication", "ack_wall_ns"), "2"),
                       (("publication", "status"), "failed"),
                       (("publication", "payload_json"), "{}"),
                       (("targets", "1", "target_source_timestamp_ms"), 1002000),
                       (("targets", "1", "error"), "2"),
                       (("targets", "1", "error_bps"), "2"),
                       (("targets", "1", "persistence_error"), "3"),
                       (("targets", "1", "eta_error_ns"), "4"),
                       (("targets", "1", "first_conflicting_event"), event(1001000, value="63000")),
                       (("targets", "1", "conflicted"), False)]
            async with pool.acquire() as connection:
                for path, value in changes:
                    changed = deepcopy(state)
                    target = changed
                    for key in path[:-1]:
                        target = target[key]
                    target[path[-1]] = value
                    with pytest.raises(asyncpg.PostgresError):
                        await connection.execute("UPDATE ghost_twap_audit SET state_json=$1,version=1 WHERE decision_id='d1'",
                                                 json.dumps(changed))
                assert json.loads(await connection.fetchval("SELECT state_json FROM ghost_twap_audit")) == state
                # Revoking a positive lead after an observed conflict remains allowed.
                changed = deepcopy(state)
                changed["targets"]["1"]["confirmed_redis_lead_ns"] = None
                await connection.execute("UPDATE ghost_twap_audit SET state_json=$1,version=1", json.dumps(changed))
        finally:
            await pool.close()
    asyncio.run(scenario())
