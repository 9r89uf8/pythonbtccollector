"""One opt-in lifecycle smoke check; only a pre-provisioned disposable DB.

The owner applies schema.sql first. No role/schema/database creation occurs here.
The sole guard bypass seeds an eight-day-old fixture inside one owner transaction
in a name-checked Unix-socket database; every lifecycle operation uses writer role.
"""
import asyncio
from copy import deepcopy
import json
import os
from unittest.mock import patch

import asyncpg
import pytest

from price_collector import ghost_twap_accuracy as accuracy
from price_collector.ghost_twap import HORIZONS, NS_PER_MS, NS_PER_SECOND
from price_collector.ghost_twap_health import GhostFeedHealth
from price_collector.ghost_twap_retention import GhostRetentionStore
from price_collector.ghost_twap_store import GhostAuditConflict, RECORD_FIELDS, validate_record
import test_ghost_twap_runtime_faults as helpers

DSN = os.environ.get("GHOST_TEST_POSTGRES_DSN")
DAY_MS = 86_400_000


async def terminal_record(created_ms):
    """Real engine calculation/publication/matching; only I/O and clocks are fake."""
    with patch.object(helpers, "BASE", created_ms - 100_000):
        runtime, clock, _, _, redis = helpers.runtime(continuous=True)
        runtime.guard["capacity_ok"] = True
        row = helpers.issue(runtime)
        runtime._pending_publication = None
        clock.advance(NS_PER_MS)
        await runtime.publish(row)
        assert len(redis.calls) == 1 and row.state["publication"]["status"] == "acknowledged"
        for index, horizon in enumerate(HORIZONS):
            clock.advance(row.decision.decision_monotonic_ns + (horizon+1)*NS_PER_SECOND - clock.mono)
            event = helpers.target(runtime, row, clock, horizon, identity=f"target-{index}")
            runtime.observe_target(event)
        clock.advance(row.decision.decision_monotonic_ns + 120*NS_PER_SECOND - clock.mono)
        runtime.finalize_due()
        assert row.terminal and all(t["status"] == "matched" for t in row.state["targets"].values())
        return validate_record(row.record())


async def read_json(pool, table, run_id=None, decision_id=None, hour=None):
    assert table in ("ghost_twap_compact", "ghost_twap_accuracy_hourly")
    async with pool.acquire() as connection:
        if table == "ghost_twap_compact":
            body = await connection.fetchval(f"SELECT body_json FROM public.{table} WHERE run_id=$1 AND decision_id=$2",
                                             run_id, decision_id)
        else:
            body = await connection.fetchval(f"SELECT body_json FROM public.{table} WHERE hour_start_ms=$1", hour)
    return None if body is None else json.loads(body)


@pytest.mark.skipif(not DSN, reason="requires an explicitly provisioned disposable PostgreSQL database")
def test_writer_compact_annotate_metadata_and_seven_day_expiry():
    async def scenario():
        owner = await asyncpg.create_pool(DSN, min_size=1, max_size=1, command_timeout=5)
        writer = None
        try:
            # Fail before any mutation, including SET ROLE, on the wrong server.
            async with owner.acquire() as connection:
                identity = await connection.fetchrow("SELECT current_database() AS db,session_user AS role,inet_server_addr() AS address")
                assert identity["db"].startswith("ghost_checkpoint_b_validation_") and identity["db"] != "ghost_checkpoint_b_validation_"
                assert identity["role"] == "postgres" and identity["address"] is None, "requires local postgres Unix socket"
                assert await connection.fetchval("SELECT count(*) FROM public.ghost_twap_compact") == 0
                assert await connection.fetchval("SELECT count(*) FROM public.ghost_twap_accuracy_hourly") == 0
                now_ms = await connection.fetchval("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")

            async def writer_role(connection):
                await connection.execute("SET ROLE price_writer")

            writer = await asyncpg.create_pool(DSN, min_size=1, max_size=1, command_timeout=5, setup=writer_role)
            store = GhostRetentionStore(writer)
            initial = await store.initialize()
            healthy = await terminal_record((now_ms//1000)*1000 - 180_000)
            poison = await terminal_record(healthy["created_ms"] - 1000)
            state = json.loads(poison["state_json"])
            del state["computation_completed_wall_ns"]
            poison = validate_record(dict(poison, state_json=json.dumps(state)))
            for record in (poison, healthy):
                assert (await store.persist(record))["outcome"] == "inserted"
            assert (await store.persist(healthy))["outcome"] == "unchanged"
            assert (await store.measure())["row_count"] == initial["row_count"] + 2
            async with writer.acquire() as connection:
                async with connection.transaction():
                    # Index eligibility only: this tiny fixture is no benchmark.
                    await connection.execute("SET LOCAL enable_seqscan=off")
                    plan = await connection.fetchval("EXPLAIN (FORMAT JSON) SELECT min(created_ms) "
                        "FROM public.ghost_twap_audit WHERE "
                        "(frozen_json::jsonb #> '{runtime_policy,continuous}')='true'::jsonb")
                    assert "ghost_twap_audit_continuous_watermark_idx" in str(plan)

            expected = accuracy.compact_record(healthy, finalized_as_of_wall_ns=now_ms*NS_PER_MS)
            result = await store.maintenance(now_ms, limit=10)
            assert result["compacted"] == 1, "an older poison row must not starve the healthy row"
            assert result["failed"] == 1 and result["failed_rows_tracked"] == 1
            retry = await store.maintenance(now_ms, limit=10)
            assert retry["compacted"] == 0 and retry["failed"] == 0 and retry["deferred"] >= 1
            assert await store.get_record(healthy["run_id"], healthy["decision_id"]) is None
            assert await store.get_record(poison["run_id"], poison["decision_id"]) is not None
            assert await read_json(writer, "ghost_twap_compact", healthy["run_id"], healthy["decision_id"]) == expected
            hour = accuracy.contribution(expected)["hour_start_ms"]
            assert await read_json(writer, "ghost_twap_accuracy_hourly", hour=hour) == accuracy.contribution(expected)
            assert await store.reconcile_compacted(healthy)
            assert (await store.persist(healthy))["outcome"] == "compacted"
            async with writer.acquire() as connection:
                assert await connection.fetchval("SELECT current_user") == "price_writer"
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute("DELETE FROM public.ghost_twap_compact WHERE run_id=$1", healthy["run_id"])
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute("UPDATE public.ghost_twap_accuracy_hourly SET body_json=body_json")
            async with owner.acquire() as connection:
                with pytest.raises(asyncpg.PostgresError, match="seven-day"):
                    await connection.execute("DELETE FROM public.ghost_twap_compact WHERE run_id=$1", healthy["run_id"])

            selected = next(h for h in expected["horizons"] if h["horizon_s"] == 5)
            conflict = dict(selected["first_event"], feed="twap", value="102", event_id="late-conflict")
            conflict["received_wall_ns"] += NS_PER_SECOND
            conflict["received_monotonic_ns"] += NS_PER_SECOND
            conflict["sequence"] += 100
            assert (await store.note_late_target(conflict))["updated"] == 1
            annotated = accuracy.annotate_target(expected, conflict)
            assert await read_json(writer, "ghost_twap_compact", healthy["run_id"], healthy["decision_id"]) == annotated
            summary = await read_json(writer, "ghost_twap_accuracy_hourly", hour=hour)
            assert summary == accuracy.contribution(annotated)
            for group in summary["groups"].values():
                assert group["metrics"]["counts"]["scored"] == (0 if group["identity"]["horizon_s"] == 5 else 1)
            assert (await store.note_late_target(conflict))["updated"] == 0

            key, group = next(iter(summary["groups"].items()))
            baseline = {"schema_version":1,"groups":{key:{"schema_version":1,"start_ms":hour,"end_ms":hour+3_600_000,
                        "captured_at_ms":now_ms,"group":group,"policy":"disposable-smoke-only"}}}
            assert (await store.save_baseline(baseline))["groups"][key] == baseline["groups"][key]
            changed = deepcopy(baseline)
            changed["groups"][key]["captured_at_ms"] += 1
            assert await store.save_baseline(changed) == baseline
            warning = {"schema_version":1,"groups":{key:{"last_evaluated_end_ms":hour+3_600_000,
                       "bad_hours":1,"good_hours":0,"active":False}}}
            assert await store.save_warning_state(warning) == warning
            stale = deepcopy(warning)
            stale["groups"][key]["last_evaluated_end_ms"] -= 3_600_000
            assert await store.save_warning_state(stale) == warning
            health = GhostFeedHealth(healthy["run_id"], now_ms-2000)
            feed = health.snapshot(now_ms-1000, 1_000_000_000)
            await store.record_feed_health(feed)
            await store.record_feed_health(feed)
            latest_feed = health.snapshot(now_ms, 2_000_000_000)
            await store.record_feed_health(latest_feed)
            async with writer.acquire() as connection:
                bodies = await connection.fetch("SELECT body_json FROM public.ghost_twap_feed_health WHERE run_id=$1 ORDER BY hour_start_ms",
                                                 healthy["run_id"])
            assert [json.loads(r["body_json"]) for r in bodies] == [
                dict(h, run_id=healthy["run_id"], dropped_hours=0) for h in latest_feed["hours"]]
            snapshot = await store.monitoring_snapshot(now_ms)
            assert snapshot["accuracy_baseline"] == baseline and snapshot["accuracy_warning_state"] == warning
            assert snapshot["persistence_watermark_ms"] == poison["created_ms"]

            old = await terminal_record(healthy["created_ms"] - 8*DAY_MS)
            with pytest.raises(asyncpg.PostgresError, match="expired"):
                await store.persist(old)
            # The real insert guard prevents retrospective fixtures. Temporarily
            # bypass ONLY that guard as owner, atomically, in this disposable DB.
            async with owner.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("ALTER TABLE public.ghost_twap_audit DISABLE TRIGGER ghost_twap_retention_insert_trigger")
                    await connection.execute("INSERT INTO public.ghost_twap_audit "
                        "(run_id,decision_id,decision_wall_ns,created_ms,frozen_json,state_json,version,terminal,target_source_timestamps_ms) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                        *(old[name] for name in RECORD_FIELDS), old["target_source_timestamps_ms"])
                    await connection.execute("ALTER TABLE public.ghost_twap_audit ENABLE TRIGGER ghost_twap_retention_insert_trigger")
            result = await store.maintenance(now_ms, limit=10)
            assert result["compacted"] == 1 and result["expired"] == 0
            # Expiry deliberately runs before compaction, so a newly compacted
            # already-old record becomes eligible on the next bounded cycle.
            result = await store.maintenance(now_ms, limit=10)
            assert result["compacted"] == 0 and result["expired"] == 1 and result["deferred"] >= 1
            assert await read_json(writer, "ghost_twap_compact", old["run_id"], old["decision_id"]) is None
            assert await store.get_record(old["run_id"], old["decision_id"]) is None
            old_hour = accuracy.contribution(accuracy.compact_record(old, finalized_as_of_wall_ns=now_ms*NS_PER_MS))["hour_start_ms"]
            assert await read_json(writer, "ghost_twap_accuracy_hourly", hour=old_hour) is not None
            with pytest.raises(GhostAuditConflict, match="expired unreconciled"):
                await store.reconcile_compacted(old)
            async with writer.acquire() as connection:
                expired = await connection.fetchrow("SELECT * FROM public.ghost_twap_retention_expire($1,10)", now_ms+8*DAY_MS)
                assert expired["expired"] == 0, "caller future clock must not bypass server seven-day age"
            assert await read_json(writer, "ghost_twap_compact", healthy["run_id"], healthy["decision_id"]) == annotated
            assert (await store.measure())["row_count"] == initial["row_count"]+2
            assert (await store.maintenance(now_ms, limit=10))["compacted"] == 0
        finally:
            if writer is not None:
                await writer.close()
            await owner.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=45))
