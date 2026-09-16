"""Bounded, operator-owned expiry of non-ghost collector history.

Run separately from collectors, using PostgreSQL peer authentication. Ordinary
collector/API roles gain no permissions. The default CLI only inspects; --apply
is the explicit destructive mode used by the installed maintenance timer.
"""
import argparse
import asyncio
from dataclasses import dataclass
import json
import time

import asyncpg

from price_collector.retention_policy import HISTORY_RETENTION_DAYS, retained_market_floor_ms

DAY_MS = 86_400_000
LOCK_ID = 740_101_610
PARENT_TABLES = frozenset((
    "public.polymarket_btc_5m_flip_evaluations",
    "public.polymarket_btc_5m_markets", "public.market_windows",
    "public.polymarket_twap_sessions", "public.polymarket_evidence_payloads",
    "raw_capture.feed_sessions",
))

# Names are a fixed allowlist, never supplied by a caller or read from SQL text.
# Optional retired tables are only expired if present, never recreated or read
# into the live collector. Child tables precede their parent metadata.
MARKET_TABLES = (
    ("polymarket_btc_5m_flip_cutoffs", "market_id", True),
    ("polymarket_btc_5m_flip_events", "market_id", True),
    ("binance_microstructure_1s_flip_archive", "market_id", True),
    ("chainlink_twap_shadow_predictions", "market_id", True),
    ("price_samples", "market_id", False),
    ("polymarket_probability_samples", "market_id", False),
    ("binance_futures_snapshots", "market_id", False),
    ("binance_flow_1s", "market_id", False),
    ("binance_book_1s", "market_id", False),
    ("binance_microstructure_1s", "market_id", False),
    ("binance_futures_oi_5m_summaries", "effective_market_id", False),
    ("polymarket_twap_events", "market_id", False),
    ("polymarket_quote_observations", "market_id", False),
    ("polymarket_market_observations", "market_id", False),
    ("polymarket_btc_5m_flip_evaluations", "market_id", True),
    ("polymarket_btc_5m_resolutions", "market_id", False),
    ("polymarket_btc_5m_markets", "market_id", False),
    ("market_windows", "market_id", False),
)


@dataclass(frozen=True)
class Expiry:
    table: str
    predicate: str
    order: str
    cutoff: int

    def exists_sql(self):
        if self.table.startswith("public.") and self.predicate == f"{self.order} < $1":
            # These public time/market columns have leading B-tree indexes.
            # Reading the minimum stays cheap immediately after bulk expiry,
            # before ANALYZE has corrected the old selectivity estimates.
            return (f"SELECT COALESCE((SELECT {self.order} FROM {self.table} t "
                    f"ORDER BY {self.order} LIMIT 1) < $1, FALSE)")
        return f"SELECT EXISTS(SELECT 1 FROM {self.table} t WHERE {self.predicate})"

    def delete_sql(self):
        # tableoid matters for partitioned raw tables: ctids repeat in children.
        return f"""WITH expired AS (
            SELECT t.tableoid AS oid, t.ctid AS tid FROM {self.table} t
            WHERE {self.predicate} ORDER BY {self.order}
            LIMIT $2 FOR UPDATE SKIP LOCKED
        ) DELETE FROM {self.table} t USING expired e
          WHERE t.tableoid=e.oid AND t.ctid=e.tid"""


def _unreferenced(table, column, parent_column):
    return f"NOT EXISTS(SELECT 1 FROM {table} c WHERE c.{column}=t.{parent_column})"


async def expiry_plan(connection, now_ms):
    cutoff_ms = max(0, now_ms - HISTORY_RETENTION_DAYS * DAY_MS)
    floor_id = retained_market_floor_ms(now_ms) // 300_000
    present = set()
    for table, _, optional in MARKET_TABLES:
        if await connection.fetchval("SELECT to_regclass($1)::text", "public." + table):
            present.add(table)
        elif not optional:
            raise RuntimeError(f"required retention table missing: {table}")

    plan = []
    for table, column, _ in MARKET_TABLES:
        if table not in present:
            continue
        predicate = f"t.{column} < $1"
        if table == "polymarket_btc_5m_flip_evaluations":
            children = ((name, "market_id") for name in (
                "polymarket_btc_5m_flip_events", "polymarket_btc_5m_flip_cutoffs") if name in present)
        elif table == "polymarket_btc_5m_markets":
            children = ((name, "market_id") for name in (
                "polymarket_btc_5m_resolutions", "polymarket_btc_5m_flip_evaluations") if name in present)
        elif table == "market_windows":
            children = ((name, col) for name, col, _ in MARKET_TABLES
                        if name in present and name != table)
        else:
            children = ()
        for child, child_column in children:
            predicate += " AND " + _unreferenced("public." + child, child_column, "market_id")
        # OI summaries label the following market; their actual source window
        # starts one market earlier. Expire on that source window's age.
        table_cutoff = floor_id + 1 if table == "binance_futures_oi_5m_summaries" else floor_id
        plan.append(Expiry("public." + table, predicate, f"t.{column}", table_cutoff))

    plan.extend((
        Expiry("public.polymarket_twap_gaps", "t.detected_wall_ns < $1",
               "t.detected_wall_ns", cutoff_ms * 1_000_000),
        Expiry("public.polymarket_twap_sessions",
               "COALESCE(t.disconnected_wall_ns,t.connected_wall_ns) < $1 AND "
               + _unreferenced("public.polymarket_twap_events", "connection_id", "connection_id")
               + " AND " + _unreferenced("public.polymarket_twap_gaps", "connection_id", "connection_id"),
               "t.connected_wall_ns", cutoff_ms * 1_000_000),
        Expiry("public.polymarket_evidence_payloads",
               "t.created_at < TIMESTAMPTZ 'epoch' + $1::bigint * INTERVAL '1 millisecond' AND "
               + _unreferenced("public.polymarket_market_observations", "payload_hash", "payload_hash"),
               "t.created_at", cutoff_ms),
        Expiry("raw_capture.binance_futures_price_trace_100ms", "t.bucket_start_ms < $1",
               "t.bucket_start_ms", cutoff_ms),
        Expiry("raw_capture.chainlink_price_events", "t.received_wall_ns < $1",
               "t.received_wall_ns", cutoff_ms * 1_000_000),
        Expiry("raw_capture.feed_sessions",
               "COALESCE(t.disconnected_wall_ns,t.connected_wall_ns) < $1 AND "
               + _unreferenced("raw_capture.binance_futures_price_trace_100ms", "connection_id", "connection_id")
               + " AND " + _unreferenced("raw_capture.chainlink_price_events", "connection_id", "connection_id"),
               "t.connected_wall_ns", cutoff_ms * 1_000_000),
    ))
    return plan


async def inspect_history(connection, now_ms):
    """Read-only, index-backed existence checks; no full-table COUNT scan."""
    result = {"cutoff_ms": max(0, now_ms - HISTORY_RETENTION_DAYS * DAY_MS),
              "market_floor_ms": retained_market_floor_ms(now_ms), "remaining": {}}
    for expiry in await expiry_plan(connection, now_ms):
        result["remaining"][expiry.table] = await connection.fetchval(expiry.exists_sql(), expiry.cutoff)
    return result


async def expire_history(connection, now_ms, *, max_seconds=45, batch_size=2000):
    """Commit small batches fairly across tables. One error cannot starve others.

    A fixed cutoff is used throughout a pass. Retained sessions/payloads that
    still support current history are metadata, not expired market samples.
    """
    if not 0 < max_seconds <= 3600 or not 0 < batch_size <= 10_000:
        raise ValueError("invalid retention work bound")
    plan = await expiry_plan(connection, now_ms)
    result = {"cutoff_ms": max(0, now_ms - HISTORY_RETENTION_DAYS * DAY_MS),
              "market_floor_ms": retained_market_floor_ms(now_ms),
              "deleted": {p.table: 0 for p in plan}, "errors": {},
              "remaining": {p.table: None for p in plan}}
    if not await connection.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_ID):
        result["skipped"] = "another_retention_run"
        return result
    deadline = time.monotonic() + max_seconds
    pending_leaves = {p.table for p in plan if p.table not in PARENT_TABLES}
    completed_leaves = set()
    try:
        while time.monotonic() < deadline:
            progressed = False
            for expiry in plan:
                if time.monotonic() >= deadline:
                    break
                if expiry.table in result["errors"] or expiry.table in completed_leaves:
                    continue
                # During initial catch-up, parent anti-joins would repeatedly
                # scan thousands of markets whose children are still queued.
                # Drain the finite, fixed-cutoff leaf set before those checks.
                if expiry.table in PARENT_TABLES and pending_leaves:
                    continue
                try:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL lock_timeout = '500ms'")
                        await connection.execute("SET LOCAL statement_timeout = '3s'")
                        status = await connection.execute(expiry.delete_sql(), expiry.cutoff, batch_size)
                    count = int(status.split()[-1])
                    result["deleted"][expiry.table] += count
                    progressed |= count > 0
                    if count < batch_size and expiry.table in pending_leaves:
                        pending_leaves.remove(expiry.table)
                        completed_leaves.add(expiry.table)
                        progressed = True  # Parent phase may need another lap.
                except (asyncpg.PostgresError, asyncio.TimeoutError, TimeoutError) as error:
                    # A FK race or lock timeout rolls back this batch only. No
                    # broad CASCADE, disabled constraints, or guessed deletion.
                    result["errors"][expiry.table] = type(error).__name__
            if not progressed:
                break
            await asyncio.sleep(0.02)
        # Report backlog independently of whether it fitted in this pass.
        for expiry in plan:
            if time.monotonic() >= deadline:
                break  # Unknown is explicit; never report a timed-out scan clean.
            try:
                result["remaining"][expiry.table] = await connection.fetchval(
                    expiry.exists_sql(), expiry.cutoff, timeout=3)
            except (asyncpg.PostgresError, asyncio.TimeoutError, TimeoutError) as error:
                result["errors"][expiry.table] = type(error).__name__
                result["remaining"][expiry.table] = None
    finally:
        await connection.execute("SELECT pg_advisory_unlock($1)", LOCK_ID)
    return result


async def _run(args):
    connection = await asyncpg.connect(database=args.database, host=args.host,
                                       user=args.user, command_timeout=5,
                                       server_settings={"application_name": "collector-retention"})
    try:
        now_ms = await connection.fetchval(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
        if args.apply:
            result = await expire_history(connection, now_ms, max_seconds=args.max_seconds,
                                          batch_size=args.batch_size)
        else:
            result = await inspect_history(connection, now_ms)
        print(json.dumps(result, sort_keys=True), flush=True)
        return 1 if result.get("errors") else 0
    finally:
        await connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="delete history older than the fixed 10-day policy")
    parser.add_argument("--max-seconds", type=int, default=45)
    parser.add_argument("--batch-size", type=int, default=2000,
                        help="rows per transaction (maximum 10000); default 2000")
    parser.add_argument("--database", default="price_collector")
    parser.add_argument("--host", default="/var/run/postgresql")
    parser.add_argument("--user", default="postgres")
    raise SystemExit(asyncio.run(_run(parser.parse_args())))


if __name__ == "__main__":
    main()
