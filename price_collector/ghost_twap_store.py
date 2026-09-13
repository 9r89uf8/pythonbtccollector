"""Bounded PostgreSQL audit operations, independent of feed/publication workers.

The caller owns its durable outbox, reservations, retries and target lifecycle.
This module never starts a task, reads a feed, or publishes Redis values. Export
acknowledgement is an operator action AFTER verification on an external machine.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any, Iterator, Mapping

MAX_BIGINT = 9_223_372_036_854_775_807
MAX_RECORD_BYTES = 128 * 1024
MAX_BATCH = 100
RETENTION_MS = 96 * 60 * 60 * 1000
SQL_TIMEOUT_SECONDS = 5
TABLE = "public.ghost_twap_audit"
RECORD_FIELDS = (
    "run_id", "decision_id", "decision_wall_ns", "created_ms", "frozen_json",
    "state_json", "version", "terminal",
)
EXPORT_FIELDS = RECORD_FIELDS + ("frozen_sha256", "state_sha256")
_SELECT = ", ".join(EXPORT_FIELDS)


class GhostAuditConflict(ValueError):
    """An identity/version was reused with incompatible evidence."""


def _integer(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= MAX_BIGINT:
        raise ValueError(f"{name} must be an integer in [{minimum}, {MAX_BIGINT}]")
    return value


def _text(value: Any, name: str, maximum: int = 128) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= maximum or "\x00" in value:
        raise ValueError(f"invalid {name}")
    return value


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _hash_text(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("invalid SHA-256")
    return value


def _no_float(value: str) -> None:
    raise ValueError("audit JSON must not contain floats/nonfinite numbers")


def _unique_object(pairs: list) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate audit JSON key")
        value[key] = item
    return value


def _json_object(value: Any) -> dict:
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_RECORD_BYTES or "\x00" in value:
        raise ValueError("audit JSON must be bounded text")
    try:
        result = json.loads(value, parse_float=_no_float, parse_constant=_no_float,
                            object_pairs_hook=_unique_object)
    except (RecursionError, UnicodeError) as exc:
        raise ValueError("invalid audit JSON") from exc
    if not isinstance(result, dict):
        raise ValueError("audit JSON must be an object")
    # Escaped NULs also cannot be converted to PostgreSQL jsonb.
    if "\\u0000" in value.lower():
        raise ValueError("audit JSON contains an unsupported NUL escape")
    return result


def _canonical(value: Mapping) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def validate_record(record: Mapping) -> dict:
    """Copy/validate before awaiting, so mutable caller state cannot race I/O."""
    result = {key: record[key] for key in RECORD_FIELDS}
    for key in ("run_id", "decision_id"):
        _text(result[key], key)
    for key in ("decision_wall_ns", "created_ms", "version"):
        _integer(result[key], key)
    if type(result["terminal"]) is not bool:
        raise ValueError("terminal must be boolean")
    frozen = _json_object(result["frozen_json"])
    state = _json_object(result["state_json"])
    if len((result["frozen_json"] + result["state_json"]).encode("utf-8")) > MAX_RECORD_BYTES:
        raise ValueError("audit record exceeds 128 KiB")
    for key in ("run_id", "decision_id"):
        if key in frozen and frozen[key] != result[key]:
            raise ValueError(f"frozen {key} disagrees with row identity")
    if "decision_wall_ns" in frozen and str(frozen["decision_wall_ns"]) != str(result["decision_wall_ns"]):
        raise ValueError("frozen decision clock disagrees with row")
    forecasts = frozen.get("forecasts", [])
    if not isinstance(forecasts, list) or len(forecasts) > 6:
        raise ValueError("expected at most six frozen forecasts")
    targets = set()
    for forecast in forecasts:
        if not isinstance(forecast, dict):
            raise ValueError("invalid frozen forecast")
        stamp = forecast.get("target_source_timestamp_ms")
        if stamp is not None:
            targets.add(_integer(stamp, "target source timestamp"))
    result["target_source_timestamps_ms"] = sorted(targets)
    state_targets = state.get("targets", {})
    if not isinstance(state_targets, dict) or len(state_targets) > 6:
        raise ValueError("expected at most six target states")
    for target in state_targets.values():
        if not isinstance(target, dict):
            raise ValueError("invalid target state")
        if target.get("target_source_timestamp_ms") is not None and target["target_source_timestamp_ms"] not in targets:
            raise ValueError("state target disagrees with frozen forecasts")
        if target.get("first_event") is not None and not isinstance(target["first_event"], dict):
            raise ValueError("invalid first target event")
    result["frozen_sha256"] = _hash(result["frozen_json"])
    result["state_sha256"] = _hash(result["state_json"])
    return result


def _transition(old: Mapping, new: Mapping) -> str:
    for key in ("run_id", "decision_id", "decision_wall_ns", "created_ms", "frozen_json"):
        if old[key] != new[key]:
            raise GhostAuditConflict("immutable decision evidence differs")
    if new["version"] < old["version"]:
        return "stale"
    if new["version"] == old["version"]:
        if old["state_json"] != new["state_json"] or old["terminal"] != new["terminal"]:
            raise GhostAuditConflict("same state version has different evidence")
        return "unchanged"
    if old["terminal"] and not new["terminal"]:
        raise GhostAuditConflict("terminal state cannot regress")
    old_targets = _json_object(old["state_json"]).get("targets", {})
    new_targets = _json_object(new["state_json"]).get("targets", {})
    for horizon, target in old_targets.items():
        first = target.get("first_event")
        if first is not None:
            replacement = new_targets.get(horizon, {})
            if replacement.get("first_event") != first or replacement.get("status") != target.get("status"):
                raise GhostAuditConflict("first target match/status is immutable")
    return "updated"


def _batch(limit: int) -> int:
    _integer(limit, "batch limit", 1)
    if limit > MAX_BATCH:
        raise ValueError("batch limit exceeds 100")
    return limit


def _cursor(after: tuple | None) -> tuple:
    if after is None:
        return ("", "")
    if not isinstance(after, tuple) or len(after) != 2:
        raise ValueError("cursor must be (run_id, decision_id)")
    return tuple(_text(value, "cursor") for value in after)


class GhostAuditStore:
    def __init__(self, pool: Any):
        self.pool = pool

    @asynccontextmanager
    async def _connection(self):
        async with self.pool.acquire(timeout=SQL_TIMEOUT_SECONDS) as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL statement_timeout = '5s'")
                await connection.execute("SET LOCAL lock_timeout = '1s'")
                yield connection

    async def initialize(self) -> dict:
        """Check only: reconcile the caller's durable spool before recovery writes."""
        async with self._connection() as connection:
            protected = await connection.fetchval("""
                SELECT EXISTS (SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'public.ghost_twap_audit'::regclass
                  AND tgname = 'ghost_twap_audit_guard_trigger' AND tgenabled = 'O')
                """)
            if not protected:
                raise RuntimeError("ghost audit schema protection is missing/disabled")
            row = await connection.fetchrow(f"""
                SELECT count(*) AS row_count, min(created_ms) AS earliest_created_ms,
                       count(*) FILTER (WHERE NOT terminal) AS incomplete_count
                FROM {TABLE}
                """)
        return dict(row)

    async def persist(self, record: Mapping) -> dict:
        incoming = validate_record(record)
        async with self._connection() as connection:
            inserted = await connection.fetchval(f"""
                INSERT INTO {TABLE}
                (run_id, decision_id, decision_wall_ns, created_ms, frozen_json,
                 state_json, version, terminal, target_source_timestamps_ms)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (run_id, decision_id) DO NOTHING RETURNING version
                """, *(incoming[key] for key in RECORD_FIELDS), incoming["target_source_timestamps_ms"])
            if inserted is not None:
                return {"version": inserted, "outcome": "inserted"}
            row = await connection.fetchrow(f"SELECT {_SELECT} FROM {TABLE} "
                                            "WHERE run_id=$1 AND decision_id=$2 FOR UPDATE",
                                            incoming["run_id"], incoming["decision_id"])
            if row is None:
                # Concurrent expiry can remove a very old exported row. Never
                # recreate it using an old retry inside this transaction.
                raise GhostAuditConflict("audit row disappeared during persistence")
            outcome = _transition(row, incoming)
            if outcome == "updated":
                await connection.execute(f"UPDATE {TABLE} SET state_json=$3, version=$4, terminal=$5 "
                                         "WHERE run_id=$1 AND decision_id=$2",
                                         incoming["run_id"], incoming["decision_id"],
                                         incoming["state_json"], incoming["version"], incoming["terminal"])
            return {"version": incoming["version"] if outcome == "updated" else row["version"],
                    "outcome": outcome}

    async def export_page(self, *, after: tuple | None = None, limit: int = MAX_BATCH,
                          incomplete_only: bool = False) -> list:
        after = _cursor(after)
        _batch(limit)
        condition = "AND NOT terminal" if incomplete_only else ""
        async with self._connection() as connection:
            rows = await connection.fetch(f"SELECT {_SELECT} FROM {TABLE} "
                                          f"WHERE (run_id, decision_id) > ($1,$2) {condition} "
                                          "ORDER BY run_id, decision_id LIMIT $3", *after, limit)
        return [dict(row) for row in rows]

    async def list_incomplete(self, *, after: tuple | None = None, limit: int = MAX_BATCH) -> list:
        return await self.export_page(after=after, limit=limit, incomplete_only=True)

    async def get_record(self, run_id: str, decision_id: str) -> dict | None:
        """Read the latest committed version before reconciling a restarted spool."""
        _text(run_id, "run_id")
        _text(decision_id, "decision_id")
        async with self._connection() as connection:
            row = await connection.fetchrow(f"SELECT {_SELECT} FROM {TABLE} WHERE run_id=$1 AND decision_id=$2",
                                            run_id, decision_id)
        return None if row is None else dict(row)

    async def measure(self, *, include_count: bool = False) -> dict:
        """Size is cheap; request an exact count only for bounded reconciliation.

        During publication the admission guard uses initialize().row_count plus
        all newly admitted decisions, including uncertain/uncommitted inserts.
        It must not interpret row_count=None as zero.
        """
        if type(include_count) is not bool:
            raise ValueError("include_count must be boolean")
        count_sql = f"(SELECT count(*) FROM {TABLE})" if include_count else "NULL::bigint"
        async with self._connection() as connection:
            row = await connection.fetchrow(f"""
                SELECT pg_total_relation_size('{TABLE}'::regclass) AS relation_bytes,
                    {count_sql} AS row_count,
                    ARRAY(SELECT DISTINCT t.spcname
                          FROM pg_class c CROSS JOIN pg_database d
                          JOIN pg_tablespace t ON true
                          WHERE d.datname = current_database()
                            AND t.oid = CASE WHEN c.reltablespace = 0 THEN d.dattablespace ELSE c.reltablespace END
                            AND (c.oid = '{TABLE}'::regclass
                                 OR c.oid = (SELECT reltoastrelid FROM pg_class WHERE oid = '{TABLE}'::regclass)
                                 OR c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid IN
                                    ('{TABLE}'::regclass, (SELECT reltoastrelid FROM pg_class WHERE oid = '{TABLE}'::regclass)))))
                    AS tablespaces
                """)
        result = dict(row)
        _integer(result["relation_bytes"], "relation_bytes")
        if include_count:
            _integer(result["row_count"], "row_count")
        result["db_data_directory"] = None
        try:
            async with self._connection() as connection:
                result["db_data_directory"] = await connection.fetchval("SHOW data_directory")
        except Exception as exc:
            if getattr(exc, "sqlstate", None) != "42501":
                raise
        return result

    async def mark_verified_export(self, proofs: list, *, export_sha256: str,
                                   external_location: str) -> dict:
        """CAS acknowledgement after external verification, never a local backup.

        The caller supplies proof records yielded by iter_export_proofs() on the
        external machine. The DB cannot establish where that machine is: CLI
        orchestration/operator attestation must enforce that trust boundary.
        """
        _batch(len(proofs))
        _hash_text(export_sha256)
        _text(external_location, "external export location", 2048)
        validated = []
        for proof in proofs:
            validated.append((_text(proof["run_id"], "run_id"), _text(proof["decision_id"], "decision_id"),
                              _integer(proof["version"], "version"), _hash_text(proof["frozen_sha256"]),
                              _hash_text(proof["state_sha256"])))
        if len({item[:2] for item in validated}) != len(validated):
            raise ValueError("duplicate export proof")
        matched = []
        async with self._connection() as connection:
            for run_id, decision_id, version, frozen_hash, state_hash in sorted(validated):
                row = await connection.fetchrow(f"""
                    UPDATE {TABLE} SET verified_export_sha256=$6, verified_external_location=$7,
                        verified_version=version, verified_frozen_sha256=frozen_sha256,
                        verified_state_sha256=state_sha256, verified_at=clock_timestamp()
                    WHERE run_id=$1 AND decision_id=$2 AND terminal AND version=$3
                        AND frozen_sha256=$4 AND state_sha256=$5
                    RETURNING run_id, decision_id
                    """, run_id, decision_id, version, frozen_hash, state_hash, export_sha256, external_location)
                if row is not None:
                    matched.append((row["run_id"], row["decision_id"]))
        return {"verified": matched, "stale_or_ineligible": len(validated) - len(matched)}

    async def expire_verified(self, *, limit: int = MAX_BATCH) -> list:
        """One bounded atomic batch; concurrent state changes invalidate eligibility."""
        _batch(limit)
        async with self._connection() as connection:
            rows = await connection.fetch(f"""
                WITH eligible AS (
                    SELECT run_id, decision_id FROM {TABLE}
                    WHERE terminal AND verified_version=version
                      AND verified_frozen_sha256=frozen_sha256 AND verified_state_sha256=state_sha256
                      AND created_ms <= (extract(epoch FROM clock_timestamp()) * 1000)::bigint - $1
                    ORDER BY created_ms, run_id, decision_id LIMIT $2 FOR UPDATE SKIP LOCKED
                ) DELETE FROM {TABLE} a USING eligible e
                  WHERE a.run_id=e.run_id AND a.decision_id=e.decision_id
                  RETURNING a.run_id, a.decision_id
                """, RETENTION_MS, limit)
        return [dict(row) for row in rows]

    async def note_late_target(self, event: Mapping, *, after: tuple | None = None,
                               limit: int = MAX_BATCH) -> dict:
        """Flag a terminal match once; never replace first match or its score.

        Call again with next_after until null. Repeated events are no-ops once
        the first corresponding late/conflict flag is retained (not a total
        later-event census). This remains an asynchronous audit-worker action.
        """
        event = _json_object(_canonical(event))
        stamp = _integer(event.get("source_timestamp_ms"), "source_timestamp_ms")
        _text(event.get("event_id"), "event_id")
        for clock in ("received_wall_ns", "received_monotonic_ns"):
            clock_value = event.get(clock)
            if isinstance(clock_value, str) and clock_value.isascii() and clock_value.isdigit():
                clock_value = int(clock_value)
            _integer(clock_value, clock)
        if event.get("window_s") != 60 or event.get("feed") != "twap":
            raise ValueError("late target must be a 60-second TWAP event")
        if stamp * 1_000_000 > int(event["received_wall_ns"]):
            raise ValueError("late target source is future-dated at receipt")
        if not isinstance(event.get("value"), str):
            raise ValueError("late target price must remain a Decimal string")
        from decimal import Decimal, InvalidOperation
        try:
            value = Decimal(event["value"])
            if not value.is_finite() or value <= 0:
                raise ValueError("invalid late target price")
        except InvalidOperation as exc:
            raise ValueError("invalid late target price") from exc
        _batch(limit)
        after = _cursor(after)
        changed = 0
        async with self._connection() as connection:
            rows = await connection.fetch(f"SELECT {_SELECT} FROM {TABLE} "
                "WHERE terminal AND target_source_timestamps_ms @> ARRAY[$1]::bigint[] "
                "AND (run_id, decision_id) > ($2,$3) ORDER BY run_id, decision_id LIMIT $4 FOR UPDATE",
                stamp, *after, limit)
            for row in rows:
                state = _json_object(row["state_json"])
                dirty = False
                for target in state.get("targets", {}).values():
                    if target.get("target_source_timestamp_ms") != stamp:
                        continue
                    first = target.get("first_event")
                    if first is not None and target.get("status") == "matched":
                        if Decimal(first["value"]) != value and not target.get("first_conflicting_event"):
                            target["conflicted"] = True
                            target["first_conflicting_event"] = event
                            target["conflict_count"] = max(1, target.get("conflict_count", 0))
                            target["confirmed_redis_lead_ns"] = None
                            dirty = True
                    elif target.get("status") in ("missing", "restart_unmatched") and not target.get("first_late_event"):
                        target["first_late_event"] = event
                        target["late_event_count"] = max(1, target.get("late_event_count", 0))
                        target["late_missing"] = True
                        dirty = True
                if dirty:
                    update = dict(row, state_json=_canonical(state), version=row["version"] + 1)
                    validate_record(update)
                    _transition(row, update)
                    await connection.execute(f"UPDATE {TABLE} SET state_json=$3, version=$4 "
                        "WHERE run_id=$1 AND decision_id=$2", row["run_id"], row["decision_id"],
                        update["state_json"], update["version"])
                    changed += 1
        cursor = (rows[-1]["run_id"], rows[-1]["decision_id"]) if len(rows) == limit else None
        return {"examined": len(rows), "updated": changed, "next_after": cursor}


def encode_export_row(row: Mapping) -> bytes:
    validated = validate_record(row)
    for field in ("frozen_sha256", "state_sha256"):
        if row.get(field) != validated[field]:
            raise ValueError("export row hash mismatch")
    return (_canonical({field: validated[field] for field in EXPORT_FIELDS}) + "\n").encode("utf-8")


def _scan_export(path: str | Path, expected_sha256: str, expected_rows: int,
                 proof_handle: Any = None) -> dict:
    """Offline full-file verification, bounded memory; no acknowledgement I/O.

    Expected count/hash must come from the completed export stream manifest,
    not from this verifier. This proves transferred bytes/rows, not that a
    changing database had no additional rows; stop admissions and reconcile
    pagination/counts separately. Changed versions fail the later DB CAS.
    """
    _hash_text(expected_sha256)
    _integer(expected_rows, "expected_rows")
    if expected_rows > 600_000:
        raise ValueError("export exceeds first-canary row cap")
    digest = sha256()
    count = 0
    previous = None
    with Path(path).open("rb") as handle:
        while True:
            line = handle.readline(MAX_RECORD_BYTES * 6 + 8192)
            if not line:
                break
            if not line.endswith(b"\n"):
                raise ValueError("truncated/oversized export line")
            row = json.loads(line, parse_float=_no_float, parse_constant=_no_float,
                             object_pairs_hook=_unique_object)
            if not isinstance(row, dict) or set(row) != set(EXPORT_FIELDS):
                raise ValueError("unexpected export fields")
            if encode_export_row(row) != line:
                raise ValueError("noncanonical or corrupt export row")
            identity = (row["run_id"], row["decision_id"])
            if previous is not None and identity <= previous:
                raise ValueError("export identities must be unique and sorted")
            previous = identity
            digest.update(line)
            count += 1
            if count > expected_rows:
                raise ValueError("export row count exceeds manifest")
            if proof_handle is not None:
                proof = {field: row[field] for field in
                         ("run_id", "decision_id", "version", "frozen_sha256", "state_sha256")}
                proof_handle.write((_canonical(proof) + "\n").encode("utf-8"))
    if digest.hexdigest() != expected_sha256 or count != expected_rows:
        raise ValueError("export manifest hash/count mismatch")
    return {"sha256": digest.hexdigest(), "row_count": count, "external_path": str(Path(path).resolve())}


def verify_export_file(path: str | Path, *, expected_sha256: str,
                       expected_rows: int) -> dict:
    """Verify completed external export bytes, exact financial text and row hashes."""
    return _scan_export(path, expected_sha256, expected_rows)


def iter_export_proofs(path: str | Path, *, expected_sha256: str,
                       expected_rows: int) -> Iterator[dict]:
    """Verify the entire file before yielding any bounded per-row proof.

    Proofs are spooled from the same verified read, with at most 1 MiB in memory.
    The temporary proof file is deleted on close. No reread/replace race can
    manufacture proofs for different contents after verification completes.
    """
    with tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b") as proof_handle:
        _scan_export(path, expected_sha256, expected_rows, proof_handle)
        proof_handle.seek(0)
        for line in proof_handle:
            yield json.loads(line)
