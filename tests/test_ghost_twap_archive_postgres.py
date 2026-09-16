"""Opt-in archive selection/CAS tests in the existing disposable PostgreSQL DB.

The shared prepare() refuses every database except ghost_checkpoint_b_validation_*
before replacing the ghost table. No opt-in means no connections. Run serially:
these tests intentionally share the disposable schema with the other PG tests.
"""
import asyncio
from hashlib import sha256
import json

import pytest

from price_collector.ghost_twap_store import (
    GhostAuditStore, encode_export_row, iter_export_proofs,
)
from test_ghost_twap_store_postgres import DSN, event, prepare, row


requires_postgres = pytest.mark.skipif(
    not DSN, reason="requires an explicitly provisioned disposable PostgreSQL database"
)


def record(created_ms, decision_id, *, run_id="integration", terminal=True):
    result = row(created_ms, decision_id, terminal=terminal)
    frozen = json.loads(result["frozen_json"])
    frozen["run_id"] = run_id
    result.update(run_id=run_id, frozen_json=json.dumps(frozen))
    return result


def identities(records):
    return [(item["run_id"], item["decision_id"]) for item in records]


def archive_proofs(tmp_path, snapshot, name):
    # Selection prioritizes age; the canonical export verifier requires identity
    # order. Preserve the complete selected row versions while sorting the file.
    ordered = sorted(snapshot, key=lambda item: (item["run_id"], item["decision_id"]))
    raw = b"".join(encode_export_row(item) for item in ordered)
    path = tmp_path / name
    path.write_bytes(raw)
    digest = sha256(raw).hexdigest()
    proofs = list(iter_export_proofs(path, expected_sha256=digest, expected_rows=len(ordered)))
    return digest, proofs


async def acknowledge(store, tmp_path, snapshot, name):
    digest, proofs = archive_proofs(tmp_path, snapshot, name)
    return await store.mark_verified_export(
        proofs, export_sha256=digest, external_location="test-owner-computer:/" + name
    )


async def verification(pool, decision_id):
    async with pool.acquire() as connection:
        result = await connection.fetchrow(
            "SELECT version, verified_version FROM public.ghost_twap_audit "
            "WHERE run_id='integration' AND decision_id=$1", decision_id
        )
        return dict(result)


@requires_postgres
def test_archive_candidates_allow_live_pending_and_prioritize_oldest_unverified(tmp_path):
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            async with pool.acquire() as connection:
                index = await connection.fetchrow("""
                    SELECT i.indisvalid, i.indisready,
                           pg_get_expr(i.indpred, i.indrelid) AS predicate,
                           ARRAY(SELECT pg_get_indexdef(i.indexrelid, n, true)
                                 FROM generate_series(1, i.indnkeyatts) AS n) AS keys
                    FROM pg_index i
                    WHERE i.indexrelid = 'public.ghost_twap_audit_archive_idx'::regclass
                      AND i.indrelid = 'public.ghost_twap_audit'::regclass
                    """)
            assert index is not None and index["indisvalid"] and index["indisready"]
            assert index["keys"] == ["created_ms", "run_id", "decision_id"]
            predicate = "".join(index["predicate"].lower().split()).replace("(", "").replace(")", "")
            assert predicate == "terminalandverified_versionisnull"
            inputs = [
                record(5000, "a-new"),
                record(2000, "z-tie", run_id="a-run"),
                record(2000, "a-tie", run_id="z-run"),
                record(2000, "a-tie", run_id="a-run"),
                record(1000, "z-oldest"),
                record(100, "live-pending", terminal=False),
                record(500, "known-verified"),
            ]
            for item in inputs:
                await store.persist(item)
            verified = await store.get_record("integration", "known-verified")
            marked = await acknowledge(store, tmp_path, [verified], "known.jsonl")
            assert marked == {"verified": [("integration", "known-verified")], "stale_or_ineligible": 0}
            expected = [("integration", "z-oldest"), ("a-run", "a-tie"),
                        ("a-run", "z-tie"), ("z-run", "a-tie"), ("integration", "a-new")]
            assert identities(await store.archive_candidates()) == expected
            assert identities(await store.archive_candidates(limit=1)) == expected[:1]
            assert identities(await store.archive_candidates(limit=3)) == expected[:3]
            assert identities(await store.archive_candidates(limit=100)) == expected
            assert (await store.initialize())["incomplete_count"] == 1
            assert identities(await store.list_incomplete()) == [("integration", "live-pending")]
        finally:
            await pool.close()
    asyncio.run(scenario())


@requires_postgres
def test_verified_mutation_and_old_pending_transition_reenter_without_cursor(tmp_path):
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            pending = record(500, "0-pending", terminal=False)
            for item in (pending, record(1000, "a-old"), record(3000, "z-later")):
                await store.persist(item)
            snapshot = await store.archive_candidates()
            assert identities(snapshot) == [("integration", "a-old"), ("integration", "z-later")]
            assert (await acknowledge(store, tmp_path, snapshot, "initial.jsonl"))["stale_or_ineligible"] == 0
            assert await store.archive_candidates() == []

            # A real late-result update retains terminal status but clears the
            # trigger's exact-version verification, even behind the prior batch.
            assert (await store.note_late_target(event(2000)))["updated"] == 1
            changed = await store.get_record("integration", "a-old")
            assert changed["terminal"] and changed["version"] == 1
            assert await verification(pool, "a-old") == {"version": 1, "verified_version": None}
            assert identities(await store.archive_candidates()) == [("integration", "a-old")]

            state = json.loads(pending["state_json"])
            state["targets"]["1"]["status"] = "missing"
            await store.persist(dict(pending, terminal=True, version=1, state_json=json.dumps(state)))
            assert identities(await store.archive_candidates(limit=1)) == [("integration", "0-pending")]
            assert identities(await store.archive_candidates()) == [("integration", "0-pending"), ("integration", "a-old")]
            untouched = await verification(pool, "z-later")
            assert untouched["verified_version"] == untouched["version"] == 0
        finally:
            await pool.close()
    asyncio.run(scenario())


@requires_postgres
def test_archive_upload_snapshot_race_partially_acknowledges_and_retries_changed_row(tmp_path):
    async def scenario():
        pool = await prepare()
        try:
            store = GhostAuditStore(pool)
            for item in (record(1000, "a-changes"), record(3000, "z-stable"),
                         record(4000, "live-pending", terminal=False)):
                await store.persist(item)
            snapshot = await store.archive_candidates()
            digest, proofs = archive_proofs(tmp_path, snapshot, "upload-in-progress.jsonl")
            old_state = snapshot[0]["state_json"]

            # The returned snapshot owns no row locks while an external upload
            # runs. Another transaction can commit a late-result annotation.
            assert (await store.note_late_target(event(2000, event_id="during-upload")))["updated"] == 1
            assert snapshot[0]["version"] == 0 and snapshot[0]["state_json"] == old_state
            marked = await store.mark_verified_export(
                proofs, export_sha256=digest, external_location="test-owner-computer:/upload-in-progress.jsonl"
            )
            assert marked == {"verified": [("integration", "z-stable")], "stale_or_ineligible": 1}
            retry = await store.archive_candidates()
            assert identities(retry) == [("integration", "a-changes")]
            assert retry[0]["version"] == 1 and retry[0]["state_json"] != old_state
            assert retry[0]["frozen_sha256"] == snapshot[0]["frozen_sha256"]
            assert retry[0]["state_sha256"] != snapshot[0]["state_sha256"]
            assert json.loads(retry[0]["state_json"])["targets"]["1"]["first_late_event"]["event_id"] == "during-upload"
            assert await acknowledge(store, tmp_path, retry, "retry-current-version.jsonl") == {
                "verified": [("integration", "a-changes")], "stale_or_ineligible": 0
            }
            assert await store.archive_candidates() == []
            assert (await store.initialize())["incomplete_count"] == 1
        finally:
            await pool.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("limit", [0, -1, 101, True, False, 1.5, "1", None])
def test_archive_candidate_limit_validation_precedes_database_access(limit):
    with pytest.raises(ValueError):
        asyncio.run(GhostAuditStore(None).archive_candidates(limit=limit))
