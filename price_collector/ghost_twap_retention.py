"""Continuous ghost audit retention; never used on the read-only API path.

Only decisions explicitly frozen with ``runtime_policy.continuous=true`` may
be compacted. PostgreSQL owns deletion permission and the durable replay floor.
The legacy store/export contracts remain available on the inherited interface.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import json
import logging
import time
from typing import Mapping

from . import ghost_twap_accuracy as accuracy
from .ghost_twap_store import (
    GhostAuditConflict, GhostAuditStore, MAX_ACTIVE_IDENTITIES, MAX_BATCH,
    RECORD_FIELDS, TABLE, _SELECT, _batch, _canonical, _cursor, _integer,
    _json_object, _text, _transition, validate_record,
)

COMPACT = "public.ghost_twap_compact"
HOURLY = "public.ghost_twap_accuracy_hourly"
STATE = "public.ghost_twap_retention_state"
FEED = "public.ghost_twap_feed_health"
TABLES = (TABLE, COMPACT, HOURLY, STATE, FEED)
DAY_MS = 86_400_000
COMPACT_RETENTION_MS = 7 * DAY_MS
SUMMARY_RETENTION_MS = 90 * DAY_MS
FINALIZE_NS = 120_000_000_000
COMPACTION_SECONDS = 3
RETRY_SECONDS = 60
MAX_FAILED_IDENTITIES = 128
LOGGER = logging.getLogger(__name__)


def _body(value: Mapping, maximum: int = 2 * 1024 * 1024) -> str:
    raw = accuracy.canonical_bytes(value)
    if len(raw) > maximum:
        raise ValueError("retention JSON exceeds bounded storage size")
    return raw.decode("utf-8")


def _continuous(record: Mapping) -> bool:
    return _json_object(record["frozen_json"]).get("runtime_policy", {}).get("continuous") is True


async def _lock_identity(connection, run_id: str, decision_id: str) -> None:
    await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 917))",
                             run_id + "\x1f" + decision_id)


async def _hour(connection, hour_start_ms: int) -> tuple:
    await connection.execute("SELECT pg_advisory_xact_lock($1::bigint)", -hour_start_ms - 918)
    row = await connection.fetchrow(f"SELECT body_json, body_sha256 FROM {HOURLY} "
                                    "WHERE hour_start_ms=$1", hour_start_ms)
    return (None, None) if row is None else (json.loads(row["body_json"]), row["body_sha256"])


class GhostRetentionStore(GhostAuditStore):
    """Same audit store interface, with bounded continuous-only maintenance."""

    def __init__(self, pool):
        super().__init__(pool)
        self._compact_after = (-1, '', '')
        self._failed_compactions = OrderedDict()
        self._failure_evictions = 0

    async def _compact_one(self, identity: Mapping, now_ms: int) -> bool:
        async with self._connection() as connection:
            await _lock_identity(connection, identity['run_id'], identity['decision_id'])
            row = await connection.fetchrow(f"SELECT {_SELECT} FROM {TABLE} "
                "WHERE run_id=$1 AND decision_id=$2 FOR UPDATE", identity['run_id'], identity['decision_id'])
            if row is None or not row['terminal'] or not _continuous(row):
                return False
            compact = accuracy.compact_record(dict(row), finalized_as_of_wall_ns=now_ms * 1_000_000)
            delta = accuracy.contribution(compact)
            previous, previous_hash = await _hour(connection, delta['hour_start_ms'])
            combined = accuracy.merge(previous, delta)
            accepted = await connection.fetchval("SELECT public.ghost_twap_compact_commit($1,$2,$3,$4,$5,$6,$7,$8)",
                row['run_id'], row['decision_id'], row['version'], row['frozen_sha256'], row['state_sha256'],
                _body(compact, 128 * 1024), _body(combined), previous_hash)
            if not accepted:
                raise GhostAuditConflict('compact/summary version changed during atomic commit')
            return True

    def compaction_health(self) -> dict:
        failures = list(self._failed_compactions.values())
        return dict(failed_rows_tracked=len(failures), failure_tracking_evictions=self._failure_evictions,
                    failures=[{key:item[key] for key in ('run_id','decision_id','error_type','attempts')}
                              for item in failures[:10]])

    async def initialize(self) -> dict:
        async with self._connection() as connection:
            protected = await connection.fetchval("""
                SELECT count(*)=5 FROM pg_trigger WHERE tgenabled='O' AND NOT tgisinternal
                AND (tgrelid,tgname) IN (
                  ('public.ghost_twap_audit'::regclass,'ghost_twap_audit_guard_trigger'),
                  ('public.ghost_twap_audit'::regclass,'ghost_twap_retention_insert_trigger'),
                  ('public.ghost_twap_audit'::regclass,'ghost_twap_retention_count_trigger'),
                  ('public.ghost_twap_compact'::regclass,'ghost_twap_compact_delete_trigger'),
                  ('public.ghost_twap_compact'::regclass,'ghost_twap_retention_count_trigger'))
                """)
            if not protected:
                raise RuntimeError("continuous ghost retention schema protection is missing/disabled")
            row = await connection.fetchrow(f"""
                SELECT active_rows+compact_rows AS row_count, incomplete_rows AS incomplete_count,
                  (SELECT created_ms FROM {TABLE} ORDER BY created_ms LIMIT 1) AS earliest_created_ms,
                  greatest((SELECT created_ms FROM {TABLE} ORDER BY created_ms DESC LIMIT 1),
                           (SELECT created_ms FROM {COMPACT} ORDER BY created_ms DESC LIMIT 1)) AS latest_created_ms
                FROM {STATE} WHERE singleton
                """)
            if row is None:
                raise RuntimeError("continuous ghost retention state is missing")
        await self._cache_data_directory()
        return dict(row)

    async def _reconciled(self, connection, incoming: Mapping) -> bool:
        row = await connection.fetchrow(f"SELECT version,frozen_sha256,state_sha256 FROM {COMPACT} "
                                        "WHERE run_id=$1 AND decision_id=$2",
                                        incoming["run_id"], incoming["decision_id"])
        if row is not None:
            if (row["frozen_sha256"] != incoming["frozen_sha256"] or row["version"] < incoming["version"]
                    or row["version"] == incoming["version"] and row["state_sha256"] != incoming["state_sha256"]):
                raise GhostAuditConflict("compacted identity has incompatible evidence/version")
            return True
        return False

    async def reconcile_compacted(self, record: Mapping) -> bool:
        incoming = validate_record(record)
        expired = False
        async with self._connection() as connection:
            await _lock_identity(connection, incoming["run_id"], incoming["decision_id"])
            if await self._reconciled(connection, incoming):
                return True
            if _continuous(incoming):
                expired = await connection.fetchval(f"""
                    SELECT $3 <= greatest(expired_before_ms,
                      (extract(epoch FROM clock_timestamp())*1000)::bigint-$4)
                    AND NOT EXISTS(SELECT 1 FROM {TABLE} WHERE run_id=$1 AND decision_id=$2)
                    FROM {STATE} WHERE singleton
                    """, incoming["run_id"], incoming["decision_id"], incoming["created_ms"],
                    COMPACT_RETENTION_MS)
                if expired:
                    await connection.execute("SELECT public.ghost_twap_retention_expired_recovery()")
        # Commit the health counter before surfacing this fail-closed recovery fault.
        if expired:
            raise GhostAuditConflict("expired unreconciled continuous outbox; evidence cannot be reconstructed")
        return False

    async def persist(self, record: Mapping) -> dict:
        incoming = validate_record(record)
        async with self._connection() as connection:
            await _lock_identity(connection, incoming["run_id"], incoming["decision_id"])
            if await self._reconciled(connection, incoming):
                return {"version": incoming["version"], "outcome": "compacted"}
            row = await connection.fetchrow(f"SELECT {_SELECT} FROM {TABLE} "
                "WHERE run_id=$1 AND decision_id=$2 FOR UPDATE", incoming["run_id"], incoming["decision_id"])
            if row is None:
                await connection.execute(f"INSERT INTO {TABLE} "
                    "(run_id,decision_id,decision_wall_ns,created_ms,frozen_json,state_json,version,terminal,"
                    "target_source_timestamps_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    *(incoming[key] for key in RECORD_FIELDS), incoming["target_source_timestamps_ms"])
                return {"version": incoming["version"], "outcome": "inserted"}
            outcome = _transition(row, incoming)
            if outcome == "updated":
                await connection.execute(f"UPDATE {TABLE} SET state_json=$3,version=$4,terminal=$5 "
                    "WHERE run_id=$1 AND decision_id=$2", incoming["run_id"], incoming["decision_id"],
                    incoming["state_json"], incoming["version"], incoming["terminal"])
            return {"version": incoming["version"] if outcome == "updated" else row["version"], "outcome": outcome}

    async def maintenance(self, now_ms: int, *, limit: int = MAX_BATCH, exclude: tuple | list = ()) -> dict:
        _integer(now_ms, "now_ms")
        _batch(limit)
        if not isinstance(exclude,(tuple,list)) or len(exclude)>MAX_ACTIVE_IDENTITIES:
            raise ValueError("active identity exclusion must contain at most 512 pairs")
        excluded = [_cursor(identity) for identity in exclude]
        # Expiry must run even when an individual verbose row cannot compact.
        async with self._connection() as connection:
            expired = await connection.fetchrow("SELECT * FROM public.ghost_twap_retention_expire($1,$2)", now_ms, limit)
        deferred = [key for key,value in self._failed_compactions.items()
                    if value['retry_after'] > time.monotonic()]
        async with self._connection() as connection:
            # Candidate reads do not lock a batch across independent hour updates.
            rows = await connection.fetch(f"SELECT created_ms,run_id,decision_id FROM {TABLE} "
                "WHERE terminal AND (frozen_json::jsonb #> '{runtime_policy,continuous}')='true'::jsonb "
                "AND decision_wall_ns <= ($1::bigint-120000)*1000000 "
                "AND NOT EXISTS(SELECT 1 FROM unnest($3::text[],$4::text[]) e(r,d) "
                f"WHERE e.r={TABLE}.run_id AND e.d={TABLE}.decision_id) "
                "AND (created_ms,run_id,decision_id)>($5,$6,$7) "
                "AND NOT EXISTS(SELECT 1 FROM unnest($8::text[],$9::text[]) e(r,d) "
                f"WHERE e.r={TABLE}.run_id AND e.d={TABLE}.decision_id) "
                "ORDER BY created_ms,run_id,decision_id LIMIT $2", now_ms, limit,
                [x[0] for x in excluded],[x[1] for x in excluded],*self._compact_after,
                [x[0] for x in deferred],[x[1] for x in deferred])
        compacted = failed = examined = 0
        deadline = time.monotonic() + COMPACTION_SECONDS
        for identity in rows:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                break
            self._compact_after = (identity['created_ms'],identity['run_id'],identity['decision_id'])
            key = identity['run_id'],identity['decision_id']
            examined += 1
            try:
                compacted += int(await asyncio.wait_for(self._compact_one(identity, now_ms), timeout=remaining))
                self._failed_compactions.pop(key, None)
            except Exception as exc:
                # Connection, schema and cancellation/time-budget failures are
                # global errors, not evidence that this particular row is bad.
                code = getattr(exc, 'sqlstate', '') or ''
                if not (isinstance(exc, (ValueError, KeyError, TypeError, ArithmeticError))
                        or code == 'P0001' or code.startswith(('22','23'))):
                    raise
                old = self._failed_compactions.pop(key, {})
                self._failed_compactions[key] = dict(run_id=key[0],decision_id=key[1],
                    error_type=type(exc).__name__,attempts=old.get('attempts',0)+1,
                    retry_after=time.monotonic()+RETRY_SECONDS)
                if len(self._failed_compactions) > MAX_FAILED_IDENTITIES:
                    self._failed_compactions.popitem(last=False)
                    self._failure_evictions += 1
                failed += 1
                LOGGER.warning('ghost_compaction_row_deferred run_id=%s decision_id=%s error_type=%s',
                               *key, type(exc).__name__)
        if examined == len(rows) and len(rows) < limit:
            self._compact_after = (-1, '', '')
        return dict(compacted=compacted, failed=failed, deferred=len(deferred),
                    **dict(expired), **self.compaction_health())

    async def note_late_target(self, event: Mapping, *, after: tuple | None = None,
                               limit: int = MAX_BATCH, exclude: tuple | list = ()) -> dict:
        # Validate identically to the legacy path before selecting either domain.
        from decimal import Decimal, InvalidOperation
        event = _json_object(_canonical(event))
        stamp = _integer(event.get("source_timestamp_ms"), "source_timestamp_ms")
        _text(event.get("event_id"), "event_id")
        for name in ("received_wall_ns", "received_monotonic_ns"):
            value = event.get(name)
            if isinstance(value, str) and value.isascii() and value.isdigit():
                value = int(value)
            _integer(value, name)
        if event.get("feed") != "twap" or event.get("window_s") != 60 or stamp*1_000_000 > int(event["received_wall_ns"]):
            raise ValueError("late target must be a causal 60-second TWAP event")
        try:
            if not isinstance(event.get("value"), str):
                raise ValueError("late target price must be a Decimal string")
            price = Decimal(event["value"])
            if not price.is_finite() or price <= 0:
                raise ValueError("invalid late target price")
        except InvalidOperation as exc:
            raise ValueError("invalid late target price") from exc
        _batch(limit)
        after = _cursor(after)
        if not isinstance(exclude, (tuple, list)) or len(exclude) > MAX_ACTIVE_IDENTITIES:
            raise ValueError("active identity exclusion must contain at most 512 pairs")
        excluded = [_cursor(identity) for identity in exclude]
        async with self._connection() as connection:
            rows = await connection.fetch(f"""
                SELECT run_id,decision_id FROM (
                  SELECT run_id,decision_id FROM {TABLE} WHERE terminal AND target_source_timestamps_ms @> ARRAY[$1]::bigint[]
                  UNION SELECT run_id,decision_id FROM {COMPACT} WHERE target_source_timestamps_ms @> ARRAY[$1]::bigint[]
                ) candidates WHERE (run_id,decision_id)>($2,$3)
                  AND NOT EXISTS(SELECT 1 FROM unnest($5::text[],$6::text[]) e(r,d)
                    WHERE e.r=candidates.run_id AND e.d=candidates.decision_id)
                ORDER BY run_id,decision_id LIMIT $4
                """, stamp, *after, limit, [x[0] for x in excluded], [x[1] for x in excluded])
        updated = 0
        for identity in rows:
            async with self._connection() as connection:
                await _lock_identity(connection, identity["run_id"], identity["decision_id"])
                compact_row = await connection.fetchrow(f"SELECT body_json,body_sha256 FROM {COMPACT} "
                    "WHERE run_id=$1 AND decision_id=$2", identity["run_id"], identity["decision_id"])
                if compact_row is not None:
                    old = accuracy.verify_compact(json.loads(compact_row["body_json"]))
                    new = accuracy.annotate_target(old, event)
                    if new is None:
                        continue
                    old_delta, new_delta = accuracy.contribution(old), accuracy.contribution(new)
                    aggregate, aggregate_hash = await _hour(connection, old_delta["hour_start_ms"])
                    if aggregate is None:
                        raise GhostAuditConflict("retained compact has no committed hourly summary")
                    replacement = accuracy.merge(accuracy.merge(aggregate, old_delta, sign=-1), new_delta)
                    accepted = await connection.fetchval("SELECT public.ghost_twap_compact_annotate($1,$2,$3,$4,$5,$6)",
                        identity["run_id"], identity["decision_id"], compact_row["body_sha256"],
                        _body(new, 128*1024), _body(replacement), aggregate_hash)
                    if not accepted:
                        raise GhostAuditConflict("compact annotation CAS failed")
                    updated += 1
                    continue
                row = await connection.fetchrow(f"SELECT {_SELECT} FROM {TABLE} "
                    "WHERE run_id=$1 AND decision_id=$2 AND terminal FOR UPDATE", identity["run_id"], identity["decision_id"])
                if row is None:
                    continue
                state = _json_object(row["state_json"])
                dirty = False
                for target in state.get("targets", {}).values():
                    if target.get("target_source_timestamp_ms") != stamp:
                        continue
                    first = target.get("first_event")
                    if first is not None and target.get("status") == "matched":
                        if Decimal(first["value"]) != price and not target.get("first_conflicting_event"):
                            target.update(conflicted=True,first_conflicting_event=event,
                                conflict_count=max(1,target.get("conflict_count",0)),confirmed_redis_lead_ns=None)
                            dirty = True
                    elif target.get("status") in ("missing","restart_unmatched") and not target.get("first_late_event"):
                        target.update(first_late_event=event,late_event_count=max(1,target.get("late_event_count",0)),late_missing=True)
                        dirty = True
                if dirty:
                    new_row = dict(row,state_json=_canonical(state),version=row["version"]+1)
                    validate_record(new_row)
                    _transition(row,new_row)
                    await connection.execute(f"UPDATE {TABLE} SET state_json=$3,version=$4 WHERE run_id=$1 AND decision_id=$2",
                        row["run_id"],row["decision_id"],new_row["state_json"],new_row["version"])
                    updated += 1
        cursor = (rows[-1]["run_id"],rows[-1]["decision_id"]) if len(rows)==limit else None
        return {"examined":len(rows),"updated":updated,"next_after":cursor}

    async def measure(self, *, include_count: bool = False) -> dict:
        if type(include_count) is not bool:
            raise ValueError("include_count must be boolean")
        async with self._connection() as connection:
            row = await connection.fetchrow(f"""
                WITH owned AS (SELECT unnest($1::regclass[]) AS oid),
                placed AS (SELECT oid FROM owned UNION SELECT c.reltoastrelid FROM pg_class c JOIN owned o ON c.oid=o.oid WHERE c.reltoastrelid<>0),
                all_rel AS (SELECT oid FROM placed UNION SELECT indexrelid FROM pg_index WHERE indrelid IN(SELECT oid FROM placed))
                SELECT (SELECT sum(pg_total_relation_size(oid))::bigint FROM owned) AS relation_bytes,
                  active_rows+compact_rows AS row_count,active_rows,compact_rows,hourly_rows,feed_rows,
                  ARRAY(SELECT DISTINCT t.spcname FROM pg_class c JOIN all_rel r ON c.oid=r.oid
                    CROSS JOIN pg_database d JOIN pg_tablespace t ON t.oid=CASE WHEN c.reltablespace=0 THEN d.dattablespace ELSE c.reltablespace END
                    WHERE d.datname=current_database()) AS tablespaces
                FROM {STATE} WHERE singleton
                """, list(TABLES))
        if row is None:
            raise RuntimeError("continuous ghost retention state is missing")
        await self._cache_data_directory()
        return {**dict(row),"db_data_directory":self._db_data_directory}

    async def monitoring_snapshot(self, now_ms: int) -> dict:
        _integer(now_ms,"now_ms")
        async with self._connection() as connection:
            state = await connection.fetchrow(f"SELECT * FROM {STATE} WHERE singleton")
            persistence_watermark_ms = await connection.fetchval(f"SELECT min(created_ms) FROM {TABLE} "
                "WHERE (frozen_json::jsonb #> '{runtime_policy,continuous}')='true'::jsonb")
            # Retain ninety days on disk, but never decode it all for a minute
            # status update. Initial baseline evidence plus recent panels only.
            hourly = await connection.fetch(f"""
                WITH selected AS (
                  (SELECT hour_start_ms FROM {HOURLY} WHERE hour_start_ms+3600000<=$1 ORDER BY hour_start_ms LIMIT 96)
                  UNION SELECT hour_start_ms FROM {HOURLY} WHERE hour_start_ms >= $2 AND hour_start_ms <= $1
                ), sized AS (SELECT h.hour_start_ms,h.body_json,sum(octet_length(h.body_json)) OVER() AS total_bytes
                  FROM {HOURLY} h JOIN selected s USING(hour_start_ms))
                SELECT CASE WHEN total_bytes<=33554432 THEN body_json ELSE NULL END AS body_json,total_bytes
                  FROM sized ORDER BY hour_start_ms
                """, now_ms,max(0,(now_ms//3_600_000)*3_600_000-7*DAY_MS))
        if state is None:
            raise RuntimeError("continuous ghost retention state is missing")
        total_bytes = (hourly[0]["total_bytes"] if hourly else 0) + sum(
            len((state[key] or "").encode("utf-8")) for key in ("baseline_json","warning_json"))
        if total_bytes > 32*1024*1024:
            raise RuntimeError("accuracy snapshot exceeds bounded 32 MiB body budget")
        measured = await self.measure()
        return {"hourly":[json.loads(x["body_json"]) for x in hourly],
                "persistence_watermark_ms":persistence_watermark_ms,
                "health":{**{k:state[k] for k in ("active_rows","compact_rows","hourly_rows","feed_rows","expired_before_ms","expired_recoveries")},
                          "relation_bytes":measured["relation_bytes"],"snapshot_body_bytes":total_bytes},
                "accuracy_baseline":None if state["baseline_json"] is None else json.loads(state["baseline_json"]),
                "accuracy_warning_state":None if state["warning_json"] is None else json.loads(state["warning_json"])}

    async def load_baseline(self) -> dict | None:
        async with self._connection() as connection:
            body = await connection.fetchval(f"SELECT baseline_json FROM {STATE} WHERE singleton")
        return None if body is None else json.loads(body)

    async def save_baseline(self, candidate: Mapping) -> dict:
        async with self._connection() as connection:
            body = await connection.fetchval("SELECT public.ghost_twap_retention_metadata('baseline',$1)", _body(candidate))
        return json.loads(body)

    async def save_warning_state(self, candidate: Mapping) -> dict:
        async with self._connection() as connection:
            body = await connection.fetchval("SELECT public.ghost_twap_retention_metadata('warning',$1)", _body(candidate))
        return json.loads(body)

    async def record_feed_health(self, snapshot: Mapping) -> None:
        run_id = _text(snapshot.get("run_id"),"run_id")
        hours = snapshot.get("hours")
        if not isinstance(hours,list) or len(hours)>96:
            raise ValueError("feed health must contain at most 96 hours")
        dropped = _integer(snapshot.get("dropped_hours"),"dropped_hours")
        seen = set()
        copied = []
        for item in hours:
            hour = _integer(item.get("hour_start_ms"),"hour_start_ms")
            revision = _integer(item.get("revision"),"revision")
            if hour%3_600_000 or hour in seen or type(item.get("complete")) is not bool:
                raise ValueError("invalid/duplicate feed health hour")
            seen.add(hour)
            copied.append((hour,revision,_body({**item,"run_id":run_id,"dropped_hours":dropped},128*1024)))
        async with self._connection() as connection:
            for hour,revision,body in sorted(copied):
                await connection.execute("SELECT public.ghost_twap_feed_health_commit($1,$2,$3,$4)",run_id,hour,revision,body)
