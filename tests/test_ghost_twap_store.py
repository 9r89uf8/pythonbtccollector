import asyncio
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

import pytest

from price_collector.ghost_twap_store import (
    GhostAuditConflict, GhostAuditStore, MAX_RECORD_BYTES, RETENTION_MS,
    _transition, encode_export_row, iter_export_proofs, validate_record,
    verify_export_file,
)


def record(*, version=0, terminal=False, decision_id="d1", state=None):
    frozen = {"run_id": "run", "decision_id": decision_id, "decision_wall_ns": "1000000000000",
              "price": "61234.123456789012345678", "forecasts": [
                  {"horizon_s": 1, "target_source_timestamp_ms": 1001000}]}
    state = state if state is not None else {"targets": {"1": {
        "target_source_timestamp_ms": 1001000, "status": "pending", "first_event": None}}}
    return dict(run_id="run", decision_id=decision_id, decision_wall_ns=1000000000000,
                created_ms=1000000, frozen_json=json.dumps(frozen), state_json=json.dumps(state),
                version=version, terminal=terminal)


def late_event(value="61000.123456789012345678"):
    return dict(feed="twap", event_id="late1", source_timestamp_ms=1001000, value=value,
                received_wall_ns="1002000000000", received_monotonic_ns="2000000000", window_s=60)


class Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class Connection:
    def __init__(self):
        self.rows = {}
        self.calls = []
        self.failure = None

    def transaction(self):
        return Context(self)

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        if sql.startswith("UPDATE"):
            row = self.rows[args[:2]]
            row["state_json"], row["version"] = args[2:4]
            row["state_sha256"] = sha256(row["state_json"].encode()).hexdigest()
            if len(args) == 5:
                row["terminal"] = args[4]
            row["verified"] = False

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if self.failure:
            raise self.failure
        if "INSERT INTO" in sql:
            identity = args[:2]
            if identity in self.rows:
                return None
            fields = ("run_id", "decision_id", "decision_wall_ns", "created_ms", "frozen_json",
                      "state_json", "version", "terminal", "target_source_timestamps_ms")
            self.rows[identity] = validate_record(dict(zip(fields, args)))
            return args[6]
        if "pg_trigger" in sql:
            return True
        if "SHOW data_directory" in sql:
            return "/db/data"
        raise AssertionError(sql)

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if sql.lstrip().startswith("UPDATE"):
            row = self.rows.get(args[:2])
            if row is None or not row["terminal"] or tuple(row[k] for k in
                    ("version", "frozen_sha256", "state_sha256")) != args[2:5]:
                return None
            row["verified"] = True
            return {k: row[k] for k in ("run_id", "decision_id")}
        if "FOR UPDATE" in sql:
            return deepcopy(self.rows.get(args[:2]))
        if "pg_total_relation_size" in sql:
            return dict(relation_bytes=8192, row_count=None if "NULL::bigint" in sql else len(self.rows), tablespaces=["pg_default"])
        return dict(row_count=len(self.rows), earliest_created_ms=1000000 if self.rows else None,
                    latest_created_ms=1000000 if self.rows else None,
                    incomplete_count=sum(not x["terminal"] for x in self.rows.values()))

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        if "target_source_timestamps_ms @>" in sql:
            stamp, run, decision, limit, excluded_runs, excluded_decisions = args
            excluded = set(zip(excluded_runs, excluded_decisions))
            return [deepcopy(row) for key, row in sorted(self.rows.items()) if
                    key not in excluded and key > (run, decision) and row["terminal"]
                    and stamp in row["target_source_timestamps_ms"]][:limit]
        if "DELETE FROM" in sql:
            return []
        run, decision, limit = args
        return [deepcopy(row) for key, row in sorted(self.rows.items()) if key > (run, decision)
                and ("NOT terminal" not in sql or not row["terminal"])][:limit]


class Pool:
    def __init__(self):
        self.connection = Connection()

    def acquire(self, *, timeout):
        assert timeout == 5
        return Context(self.connection)


def test_persistence_retries_versions_and_immutable_inputs():
    async def scenario():
        pool = Pool()
        store = GhostAuditStore(pool)
        first = record()
        assert await store.persist(first) == {"version": 0, "outcome": "inserted"}
        assert (await store.persist(first))["outcome"] == "unchanged"
        changed = dict(first, frozen_json=first["frozen_json"].replace("61234", "61235"), version=1)
        with pytest.raises(GhostAuditConflict, match="immutable"):
            await store.persist(changed)
        changed = dict(first, state_json='{"targets":{},"failure":"lost"}')
        with pytest.raises(GhostAuditConflict, match="same state version"):
            await store.persist(changed)
        newer = dict(first, state_json=json.dumps(dict(json.loads(first["state_json"]), failure="lost")),
                     version=4, terminal=True)
        assert (await store.persist(newer))["outcome"] == "updated"
        assert await store.persist(first) == {"version": 4, "outcome": "stale"}
        with pytest.raises(GhostAuditConflict, match="terminal"):
            await store.persist(dict(newer, version=5, terminal=False))
        assert pool.connection.rows[("run", "d1")]["state_json"] == newer["state_json"]
        assert any("FOR UPDATE" in sql for sql, _ in pool.connection.calls)
    asyncio.run(scenario())


def test_first_match_cannot_be_replaced_or_regraded_as_missing():
    first = late_event()
    state = {"targets": {"1": {"target_source_timestamp_ms": 1001000,
                              "status": "matched", "first_event": first}}}
    old = validate_record(record(state=state, terminal=True))
    for mutate in (lambda target: target.update(first_event=late_event("62000")),
                   lambda target: target.update(status="missing")):
        changed = deepcopy(state)
        mutate(changed["targets"]["1"])
        with pytest.raises(GhostAuditConflict, match="first target"):
            _transition(old, validate_record(record(state=changed, terminal=True, version=1)))


@pytest.mark.parametrize("change", [
    {"version": True}, {"decision_wall_ns": -1}, {"terminal": 1},
    {"state_json": '{"x":1.5}'}, {"state_json": '{"x":NaN}'},
    {"state_json": '{"x":1,"x":2}'}, {"state_json": '[]'},
    {"state_json": '{"targets":[]}'}, {"run_id": "different"},
    {"state_json": json.dumps({"huge": "x" * MAX_RECORD_BYTES})},
    {"state_json": '{"targets":{"1":{"target_source_timestamp_ms":9}}}'},
])
def test_invalid_records_fail_before_database_acquisition(change):
    with pytest.raises(ValueError):
        asyncio.run(GhostAuditStore(None).persist(dict(record(), **change)))


def test_ambiguous_commit_failure_is_not_retried_in_store():
    pool = Pool()
    pool.connection.failure = RuntimeError("lost connection")
    with pytest.raises(RuntimeError, match="lost connection"):
        asyncio.run(GhostAuditStore(pool).persist(record()))
    assert sum("INSERT INTO" in sql for sql, _ in pool.connection.calls) == 1


def test_startup_is_read_only_and_recovery_paginates():
    async def scenario():
        pool = Pool()
        store = GhostAuditStore(pool)
        await store.persist(record(decision_id="d1"))
        await store.persist(record(decision_id="d2", terminal=True))
        await store.persist(record(decision_id="d3"))
        pool.connection.calls.clear()
        assert await store.initialize() == dict(row_count=3, earliest_created_ms=1000000,
                                               latest_created_ms=1000000, incomplete_count=2)
        first = await store.list_incomplete(limit=1)
        assert first[0]["decision_id"] == "d1"
        assert [x["decision_id"] for x in await store.list_incomplete(after=("run", "d1"))] == ["d3"]
        assert not any("UPDATE " in sql or "INSERT " in sql for sql, _ in pool.connection.calls)
        assert (await store.measure())["tablespaces"] == ["pg_default"]
    asyncio.run(scenario())


def test_export_verification_rejects_tampering_truncation_and_wrong_manifest(tmp_path):
    row = validate_record(record(terminal=True))
    content = encode_export_row(row)
    digest = sha256(content).hexdigest()
    path = tmp_path / "export.jsonl"
    path.write_bytes(content)
    verified = verify_export_file(path, expected_sha256=digest, expected_rows=1)
    assert verified["row_count"] == 1
    assert list(iter_export_proofs(path, expected_sha256=digest, expected_rows=1))[0]["state_sha256"] == row["state_sha256"]
    assert b"61234.123456789012345678" in content
    for bad in (content[:-1], content.replace(b"61234", b"61235"), content + content):
        path.write_bytes(bad)
        with pytest.raises(ValueError):
            next(iter_export_proofs(path, expected_sha256=digest, expected_rows=1))
    path.write_bytes(content)
    with pytest.raises(ValueError, match="manifest"):
        verify_export_file(path, expected_sha256="0" * 64, expected_rows=1)


def test_verified_proofs_remain_bound_to_verified_read_after_file_replaced(tmp_path):
    rows = [validate_record(record(terminal=True, decision_id=f"d{x}")) for x in (1, 2)]
    content = b"".join(encode_export_row(row) for row in rows)
    path = tmp_path / "export.jsonl"
    path.write_bytes(content)
    proofs = iter_export_proofs(path, expected_sha256=sha256(content).hexdigest(), expected_rows=2)
    assert next(proofs)["decision_id"] == "d1"
    path.write_bytes(b"replaced")
    assert next(proofs)["decision_id"] == "d2"
    with pytest.raises(StopIteration):
        next(proofs)


def test_export_acknowledgement_compare_and_swap_and_invalidation():
    async def scenario():
        pool = Pool()
        store = GhostAuditStore(pool)
        row = validate_record(record(terminal=True))
        await store.persist(row)
        proof = {key: row[key] for key in ("run_id", "decision_id", "version", "frozen_sha256", "state_sha256")}
        result = await store.mark_verified_export([proof], export_sha256="a" * 64,
                                                  external_location="owner-computer:/exports/canary.jsonl")
        assert result["verified"] == [("run", "d1")]
        await store.persist(dict(row, version=1, state_json=json.dumps(dict(json.loads(row["state_json"]), restart=True))))
        assert pool.connection.rows[("run", "d1")]["verified"] is False
        assert (await store.mark_verified_export([proof], export_sha256="a" * 64,
                external_location="owner-computer:/exports/canary.jsonl"))["stale_or_ineligible"] == 1
        await store.expire_verified(limit=1)
        sql, args = pool.connection.calls[-1]
        assert "FOR UPDATE SKIP LOCKED" in sql and args == (RETENTION_MS, 1)
        assert "verified_state_sha256=state_sha256" in sql
    asyncio.run(scenario())


def test_terminal_late_flags_are_idempotent_keep_first_match_and_paginate():
    async def scenario():
        pool = Pool()
        store = GhostAuditStore(pool)
        first = late_event("60000")
        matched = {"targets": {"1": {"target_source_timestamp_ms": 1001000, "status": "matched",
                                      "first_event": first, "error_bps": "0.000001",
                                      "confirmed_redis_lead_ns": "1000000"}}}
        missing = {"targets": {"1": {"target_source_timestamp_ms": 1001000, "status": "missing",
                                      "first_event": None}}}
        await store.persist(record(terminal=True, state=matched))
        await store.persist(record(terminal=True, decision_id="d2", state=missing))
        page = await store.note_late_target(late_event(), limit=1)
        assert page == dict(examined=1, updated=1, next_after=("run", "d1"))
        await store.note_late_target(late_event(), after=page["next_after"])
        state = json.loads(pool.connection.rows[("run", "d1")]["state_json"])["targets"]["1"]
        assert state["first_event"] == first and state["status"] == "matched"
        assert state["error_bps"] == "0.000001" and state["conflicted"] is True
        assert state["confirmed_redis_lead_ns"] is None
        state = json.loads(pool.connection.rows[("run", "d2")]["state_json"])["targets"]["1"]
        assert state["status"] == "missing" and state["first_event"] is None and state["late_missing"] is True
        assert (await store.note_late_target(late_event()))["updated"] == 0
    asyncio.run(scenario())


@pytest.mark.parametrize("change", [{"received_wall_ns": "1"}, {"received_monotonic_ns": -1},
                                    {"window_s": 30}, {"value": 60000.0}, {"value": "NaN"}])
def test_invalid_late_event_fails_before_database(change):
    with pytest.raises(ValueError):
        asyncio.run(GhostAuditStore(None).note_late_target(dict(late_event(), **change)))


def test_live_ownership_excludes_rows_before_database_version_allocation_and_limit():
    async def scenario():
        pool = Pool()
        store = GhostAuditStore(pool)
        state = {"targets": {"1": {"target_source_timestamp_ms": 1001000,
                                  "status": "missing", "first_event": None}}}
        original = record(terminal=True, state=state)
        await store.persist(original)
        # Reproduce two owners independently allocating version 1.
        live_state = deepcopy(state)
        live_state["targets"]["1"]["first_late_event"] = late_event()
        live = dict(original, version=1, state_json=json.dumps(live_state))
        assert (await store.note_late_target(late_event()))["updated"] == 1
        with pytest.raises(GhostAuditConflict, match="same state version"):
            await store.persist(live)

        pool = Pool()
        store = GhostAuditStore(pool)
        await store.persist(original)
        await store.persist(record(terminal=True, decision_id="d2", state=state))
        result = await store.note_late_target(late_event(), limit=1,
                                              exclude=[("run", "d1"), ("run", "d1")])
        assert result == dict(examined=1, updated=1, next_after=("run", "d2"))
        sql, args = next((sql, args) for sql, args in pool.connection.calls if "unnest" in sql)
        assert sql.index("NOT EXISTS") < sql.index("LIMIT") < sql.index("FOR UPDATE")
        assert args[-2:] == (["run"], ["d1"])
        assert (await store.persist(live))["outcome"] == "updated"
        # After committed handoff, the same event is already known and stays idempotent.
        assert (await store.note_late_target(late_event()))["updated"] == 0
    asyncio.run(scenario())


@pytest.mark.parametrize("exclude", [None, "run", [("run",)], [["run", "d1"]],
                                    [("", "d1")], [("run", "d1")] * 513])
def test_invalid_live_ownership_fails_before_database(exclude):
    with pytest.raises(ValueError):
        asyncio.run(GhostAuditStore(None).note_late_target(late_event(), exclude=exclude))


def test_server_directory_is_cached_but_counts_remain_fresh():
    async def scenario():
        pool = Pool()
        store = GhostAuditStore(pool)
        assert (await store.initialize())["row_count"] == 0
        await store.persist(record())
        assert (await store.initialize())["row_count"] == 1
        for _ in range(3):
            before = len(pool.connection.calls)
            assert (await store.measure())["db_data_directory"] == "/db/data"
            calls = pool.connection.calls[before:]
            assert sum("SET LOCAL statement_timeout" in sql for sql, _ in calls) == 1
        assert sum("SHOW data_directory" in sql for sql, _ in pool.connection.calls) == 1
    asyncio.run(scenario())


def test_server_directory_permission_denial_is_cached_and_transient_failure_retried():
    class Denied(RuntimeError):
        sqlstate = "42501"

    async def scenario():
        for failure, cached in ((Denied("denied"), True), (RuntimeError("offline"), False)):
            pool = Pool()
            store = GhostAuditStore(pool)
            fetchval = pool.connection.fetchval
            attempts = []

            async def show(sql, *args):
                if "SHOW data_directory" in sql:
                    attempts.append(sql)
                    if len(attempts) == 1:
                        raise failure
                return await fetchval(sql, *args)

            pool.connection.fetchval = show
            if cached:
                assert (await store.measure())["db_data_directory"] is None
            else:
                with pytest.raises(RuntimeError, match="offline"):
                    await store.measure()
            result = await store.measure()
            assert result["db_data_directory"] == (None if cached else "/db/data")
            assert len(attempts) == (1 if cached else 2)
    asyncio.run(scenario())


def evidence_state():
    return {"computation_completed_wall_ns": "1000", "computation_completed_monotonic_ns": "900",
            "publication": {"status": "acknowledged", "attempt_wall_ns": "1100",
                            "attempt_monotonic_ns": "1000", "ack_wall_ns": "1200",
                            "ack_monotonic_ns": "1100", "payload_json": "{\"price\":\"1\"}"},
            "targets": {"1": {"horizon": 1, "target_source_timestamp_ms": 1001000,
                              "status": "matched", "first_event": late_event(),
                              "first_conflicting_event": late_event("62000"), "conflicted": True,
                              "error": "1.000000000000000001", "error_bps": "0.001",
                              "persistence_error": "2", "eta_error_ns": "3", "clock_anomaly": True,
                              "confirmed_redis_lead_ns": "1000000"}}}


@pytest.mark.parametrize("path,value", [
    (("computation_completed_wall_ns",), "2"),
    (("publication", "ack_wall_ns"), "2"),
    (("publication", "status"), "failed"),
    (("publication", "payload_json"), "{}"),
    (("targets", "1", "horizon"), 2),
    (("targets", "1", "error"), "2"),
    (("targets", "1", "error_bps"), "2"),
    (("targets", "1", "persistence_error"), "3"),
    (("targets", "1", "eta_error_ns"), "4"),
    (("targets", "1", "clock_anomaly"), False),
    (("targets", "1", "first_conflicting_event"), late_event("63000")),
    (("targets", "1", "conflicted"), False),
])
def test_first_evidence_and_observed_flags_cannot_be_rewritten(path, value):
    state = evidence_state()
    changed = deepcopy(state)
    target = changed
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(GhostAuditConflict):
        _transition(validate_record(record(terminal=True, state=state)),
                    validate_record(record(terminal=True, state=changed, version=1)))


def test_missing_status_and_first_late_evidence_stay_fixed_but_lead_can_be_invalidated():
    state = evidence_state()
    changed = deepcopy(state)
    changed["targets"]["1"]["confirmed_redis_lead_ns"] = None
    assert _transition(validate_record(record(terminal=True, state=state)),
                       validate_record(record(terminal=True, state=changed, version=1))) == "updated"
    state = {"targets": {"1": {"target_source_timestamp_ms": 1001000, "status": "missing",
                              "first_event": None, "first_late_event": late_event(), "late_missing": True}}}
    for patch in ({"status": "matched"}, {"first_late_event": late_event("63000")}, {"late_missing": False}):
        changed = deepcopy(state)
        changed["targets"]["1"].update(patch)
        with pytest.raises(GhostAuditConflict):
            _transition(validate_record(record(terminal=True, state=state)),
                        validate_record(record(terminal=True, state=changed, version=1)))


def test_ghost_schema_is_single_table_with_hash_and_expiry_protection():
    sql = Path("schema.sql").read_text(encoding="utf-8").split("CREATE TABLE IF NOT EXISTS providers", 1)[0]
    assert sql.count("CREATE TABLE IF NOT EXISTS") == 1
    assert "PRIMARY KEY (run_id, decision_id)" in sql
    assert "sha256(convert_to(NEW.frozen_json, 'UTF8'))" in sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in sql
    assert "NEW.verified_version := NULL" in sql
    assert "345600000" in sql and "OLD.verified_state_sha256 IS DISTINCT FROM OLD.state_sha256" in sql
    assert "ghost first target match/status is immutable" in sql
    assert "USING GIN (target_source_timestamps_ms) WHERE terminal" in sql
