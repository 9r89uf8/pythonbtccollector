import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from math import isfinite
from typing import Any, Mapping, Optional, Sequence

import asyncpg

from price_collector.market import MarketWindow


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RAW_CAPTURE_SCHEMA = "raw_capture"
RAW_CAPTURE_PARTITION_WIDTH_MS = 6 * 60 * 60 * 1000
RAW_CAPTURE_MAINTENANCE_LOCK_ID = 0x5241574341505455
RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS = 5
SHADOW_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS = 5
SHADOW_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS = 5

RAW_FUTURES_TRACE_COLUMNS = (
    "bucket_start_ms",
    "connection_id",
    "first_received_wall_ns",
    "last_received_wall_ns",
    "first_received_monotonic_ns",
    "last_received_monotonic_ns",
    "first_trade_time_ms",
    "last_trade_time_ms",
    "first_event_time_ms",
    "last_event_time_ms",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "event_count",
    "first_agg_trade_id",
    "last_agg_trade_id",
)

RAW_CHAINLINK_EVENT_COLUMNS = (
    "received_wall_ns",
    "received_monotonic_ns",
    "connection_id",
    "receive_sequence",
    "provider_event_ms",
    "provider_message_ms",
    "price",
)

SHADOW_SIGNAL_EVALUATION_COLUMNS = (
    "selection_schema_version",
    "selection_policy_version",
    "selection_fingerprint_sha256",
    "selection_artifact_sha256",
    "selection_evidence_end_ms",
    "model_version",
    "beta",
    "generated_ms",
    "target_ms",
    "matured_ms",
    "horizon_ms",
    "valid",
    "status",
    "invalid_reasons",
    "state",
    "market_id",
    "market_start_ms",
    "market_end_ms",
    "ms_to_market_end",
    "full_horizon_before_market_end",
    "chainlink_at_forecast",
    "chainlink_at_forecast_source_timestamp_ms",
    "chainlink_at_forecast_received_ms",
    "projected_chainlink",
    "pending_move",
    "pending_move_bps",
    "direction",
    "futures_now",
    "futures_now_source_timestamp_ms",
    "futures_now_received_ms",
    "futures_reference",
    "futures_reference_source_timestamp_ms",
    "futures_reference_received_ms",
    "futures_reference_target_ms",
    "futures_reference_gap_ms",
    "futures_received_age_ms",
    "chainlink_received_age_ms",
    "actual_chainlink",
    "actual_chainlink_source_timestamp_ms",
    "actual_chainlink_received_ms",
    "actual_chainlink_age_at_target_ms",
    "forecast_error",
    "baseline_error",
)

SHADOW_SIGNAL_EVALUATION_INSERT_SQL = f"""
    INSERT INTO shadow_signal_evaluations (
        {", ".join(SHADOW_SIGNAL_EVALUATION_COLUMNS)}
    )
    VALUES ({", ".join(f"${index}" for index in range(1, len(SHADOW_SIGNAL_EVALUATION_COLUMNS) + 1))})
    ON CONFLICT (model_version, generated_ms, horizon_ms) DO NOTHING
"""


@dataclass(frozen=True)
class RawCaptureMaintenanceStatus:
    storage_permitted: bool
    current_partition: str
    current_partition_start_ms: int
    raw_table_bytes: int


def epoch_ms_to_utc_datetime(epoch_ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=epoch_ms)


def utc_datetime_to_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def decimal_fixed_or_none(
    value: Optional[Decimal],
    places: str,
) -> Optional[str]:
    if value is None:
        return None

    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    quantum = Decimal(places)
    return format(value.quantize(quantum, rounding=ROUND_HALF_UP), "f")


def decimal_string_or_none(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return format(value, "f")


def decimal_compact_or_none(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def decimal_2dp_or_none(value: Optional[Decimal]) -> Optional[str]:
    return decimal_fixed_or_none(value, "0.01")


def money_2dp(value: Optional[Decimal]) -> Optional[str]:
    return decimal_fixed_or_none(value, "0.01")


def oi_3dp(value: Optional[Decimal]) -> Optional[str]:
    return decimal_fixed_or_none(value, "0.001")


def age_ms(server_time_ms: int, timestamp_ms: Optional[int]) -> Optional[int]:
    if timestamp_ms is None:
        return None
    return max(0, server_time_ms - int(timestamp_ms))


def freshness_meta(
    *,
    server_time_ms: int,
    source_time_ms: Optional[int],
    received_ms: Optional[int],
) -> dict[str, Optional[int]]:
    return {
        "source_age_ms": age_ms(server_time_ms, source_time_ms),
        "received_age_ms": age_ms(server_time_ms, received_ms),
        "transport_lag_ms": (
            None
            if source_time_ms is None or received_ms is None
            else max(0, int(received_ms) - int(source_time_ms))
        ),
    }


def freshness_age_only_meta(
    *,
    server_time_ms: int,
    source_time_ms: Optional[int],
    received_ms: Optional[int],
) -> dict[str, Optional[int]]:
    return {
        "source_age_ms": age_ms(server_time_ms, source_time_ms),
        "received_age_ms": age_ms(server_time_ms, received_ms),
    }


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )


async def create_raw_capture_pool(database_url: str) -> asyncpg.Pool:
    """Create a lazy, single-connection pool used only by raw capture."""
    return await asyncpg.create_pool(
        database_url,
        min_size=0,
        max_size=1,
        timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS,
        command_timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS,
        server_settings={"application_name": "price_collector_raw_capture"},
    )


def _validated_database_timeout(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    if value <= 0 or (isinstance(value, float) and not isfinite(value)):
        raise ValueError(f"{field_name} must be positive and finite")
    return value


async def create_shadow_evaluation_pool(
    database_url: str,
    *,
    connect_timeout_seconds: float = (
        SHADOW_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS
    ),
    command_timeout_seconds: float = (
        SHADOW_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS
    ),
) -> asyncpg.Pool:
    """Create the lazy single-connection pool for noncritical evaluations."""
    connect_timeout_seconds = _validated_database_timeout(
        connect_timeout_seconds,
        "connect_timeout_seconds",
    )
    command_timeout_seconds = _validated_database_timeout(
        command_timeout_seconds,
        "command_timeout_seconds",
    )
    return await asyncpg.create_pool(
        database_url,
        min_size=0,
        max_size=1,
        timeout=connect_timeout_seconds,
        command_timeout=command_timeout_seconds,
        server_settings={
            "application_name": "price_collector_shadow_evaluation"
        },
    )


async def create_read_pool(settings: Any) -> asyncpg.Pool:
    database_url = settings.READ_DATABASE_URL or settings.DATABASE_URL
    if not database_url:
        raise RuntimeError("READ_DATABASE_URL or DATABASE_URL must be set for the API")
    return await create_pool(database_url)


def raw_capture_partition_start_ms(received_ms: int) -> int:
    if received_ms < 0:
        raise ValueError("received_ms must be non-negative")
    return (
        received_ms // RAW_CAPTURE_PARTITION_WIDTH_MS
    ) * RAW_CAPTURE_PARTITION_WIDTH_MS


def raw_capture_partition_name(parent_table: str, partition_start_ms: int) -> str:
    if parent_table not in {
        "binance_futures_price_trace_100ms",
        "chainlink_price_events",
    }:
        raise ValueError("unsupported raw capture parent table")
    if (
        partition_start_ms < 0
        or partition_start_ms % RAW_CAPTURE_PARTITION_WIDTH_MS != 0
    ):
        raise ValueError("raw capture partition start must be six-hour aligned")
    return f"{parent_table}_p{partition_start_ms}"


async def ensure_raw_capture_partitions(
    connection: asyncpg.Connection,
    *,
    now_ms: int,
) -> None:
    current_start_ms = raw_capture_partition_start_ms(now_ms)
    partition_specs = []
    for partition_start_ms in (
        current_start_ms,
        current_start_ms + RAW_CAPTURE_PARTITION_WIDTH_MS,
    ):
        partition_end_ms = partition_start_ms + RAW_CAPTURE_PARTITION_WIDTH_MS
        futures_partition = raw_capture_partition_name(
            "binance_futures_price_trace_100ms",
            partition_start_ms,
        )
        chainlink_partition = raw_capture_partition_name(
            "chainlink_price_events",
            partition_start_ms,
        )
        partition_specs.extend(
            (
                (
                    futures_partition,
                    "binance_futures_price_trace_100ms",
                    partition_start_ms,
                    partition_end_ms,
                ),
                (
                    chainlink_partition,
                    "chainlink_price_events",
                    partition_start_ms * 1_000_000,
                    partition_end_ms * 1_000_000,
                ),
            )
        )

    qualified_names = [
        f"{RAW_CAPTURE_SCHEMA}.{partition_name}"
        for partition_name, _parent, _start, _end in partition_specs
    ]
    existing_rows = await connection.fetch(
        """
        SELECT candidate.qualified_name
        FROM unnest($1::TEXT[]) AS candidate(qualified_name)
        WHERE to_regclass(candidate.qualified_name) IS NOT NULL
        """,
        qualified_names,
    )
    existing_names = {str(row["qualified_name"]) for row in existing_rows}

    for partition_name, parent_table, partition_start, partition_end in partition_specs:
        qualified_name = f"{RAW_CAPTURE_SCHEMA}.{partition_name}"
        if qualified_name in existing_names:
            continue
        await connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_CAPTURE_SCHEMA}."{partition_name}"
            PARTITION OF {RAW_CAPTURE_SCHEMA}.{parent_table}
            FOR VALUES FROM ({partition_start}) TO ({partition_end})
            """
        )


async def fetch_raw_capture_partitions(
    connection: asyncpg.Connection,
) -> list:
    return await connection.fetch(
        """
        SELECT
            parent.relname AS parent_table,
            child.relname AS partition_name,
            substring(child.relname FROM '_p([0-9]+)$')::BIGINT
                AS partition_start_ms,
            pg_total_relation_size(child.oid)::BIGINT AS total_bytes
        FROM pg_inherits inheritance
        JOIN pg_class parent ON parent.oid = inheritance.inhparent
        JOIN pg_class child ON child.oid = inheritance.inhrelid
        JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
        JOIN pg_namespace parent_namespace
          ON parent_namespace.oid = parent.relnamespace
        WHERE namespace.nspname = 'raw_capture'
          AND parent_namespace.nspname = 'raw_capture'
          AND parent.relname IN (
              'binance_futures_price_trace_100ms',
              'chainlink_price_events'
          )
          AND child.relname ~ '_p[0-9]+$'
          AND NOT EXISTS (
              SELECT 1
              FROM pg_inherits descendants
              WHERE descendants.inhparent = child.oid
          )
        ORDER BY partition_start_ms ASC, parent_table ASC
        """
    )


async def drop_raw_capture_interval(
    connection: asyncpg.Connection,
    *,
    partition_start_ms: int,
) -> None:
    futures_partition = raw_capture_partition_name(
        "binance_futures_price_trace_100ms",
        partition_start_ms,
    )
    chainlink_partition = raw_capture_partition_name(
        "chainlink_price_events",
        partition_start_ms,
    )
    await connection.execute(
        f"""
        DROP TABLE IF EXISTS
            {RAW_CAPTURE_SCHEMA}."{futures_partition}",
            {RAW_CAPTURE_SCHEMA}."{chainlink_partition}"
        """
    )


async def maintain_raw_capture_partitions(
    pool: asyncpg.Pool,
    *,
    retention_hours: int,
    max_relation_mb: int,
    now_ms: Optional[int] = None,
) -> Optional[RawCaptureMaintenanceStatus]:
    """Return maintenance telemetry, or None when another process owns it."""
    if retention_hours < 6:
        raise ValueError("retention_hours must be at least six")
    if max_relation_mb <= 0:
        raise ValueError("max_relation_mb must be positive")

    async with pool.acquire(timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS) as connection:
        async with connection.transaction():
            lock_acquired = await connection.fetchval(
                "SELECT pg_try_advisory_xact_lock($1)",
                RAW_CAPTURE_MAINTENANCE_LOCK_ID,
            )
            if not lock_acquired:
                return None

            await connection.execute("SET LOCAL lock_timeout = '2s'")
            await connection.execute("SET LOCAL statement_timeout = '5s'")
            if now_ms is None:
                now_ms = int(
                    await connection.fetchval(
                        """
                        SELECT floor(
                            extract(epoch FROM clock_timestamp()) * 1000
                        )::BIGINT
                        """
                    )
                )

            await ensure_raw_capture_partitions(connection, now_ms=now_ms)
            partitions = await fetch_raw_capture_partitions(connection)

            grouped = {}
            for row in partitions:
                partition_start_ms = int(row["partition_start_ms"])
                grouped.setdefault(partition_start_ms, 0)
                grouped[partition_start_ms] += int(row["total_bytes"])

            current_start_ms = raw_capture_partition_start_ms(now_ms)
            retention_cutoff_ms = now_ms - retention_hours * 60 * 60 * 1000
            dropped_starts = set()
            total_bytes = sum(grouped.values())

            for partition_start_ms in sorted(grouped):
                partition_end_ms = (
                    partition_start_ms + RAW_CAPTURE_PARTITION_WIDTH_MS
                )
                if partition_end_ms <= retention_cutoff_ms:
                    await drop_raw_capture_interval(
                        connection,
                        partition_start_ms=partition_start_ms,
                    )
                    total_bytes -= grouped[partition_start_ms]
                    dropped_starts.add(partition_start_ms)

            relation_budget_bytes = max_relation_mb * 1024 * 1024
            for partition_start_ms in sorted(grouped):
                if total_bytes <= relation_budget_bytes:
                    break
                if partition_start_ms in dropped_starts:
                    continue
                partition_end_ms = (
                    partition_start_ms + RAW_CAPTURE_PARTITION_WIDTH_MS
                )
                if partition_end_ms > current_start_ms:
                    continue
                await drop_raw_capture_interval(
                    connection,
                    partition_start_ms=partition_start_ms,
                )
                total_bytes -= grouped[partition_start_ms]
                dropped_starts.add(partition_start_ms)

            retained_starts = [
                partition_start_ms
                for partition_start_ms in grouped
                if partition_start_ms not in dropped_starts
            ]
            oldest_retained_start_ms = min(
                retained_starts,
                default=current_start_ms,
            )
            await connection.execute(
                """
                DELETE FROM raw_capture.feed_sessions
                WHERE disconnected_wall_ns IS NOT NULL
                  AND disconnected_wall_ns < $1
                """,
                oldest_retained_start_ms * 1_000_000,
            )

            return RawCaptureMaintenanceStatus(
                storage_permitted=total_bytes <= relation_budget_bytes,
                current_partition=f"p{current_start_ms}",
                current_partition_start_ms=current_start_ms,
                raw_table_bytes=total_bytes,
            )


async def copy_binance_futures_price_traces(
    connection: asyncpg.Connection,
    records: Sequence[Any],
) -> None:
    if not records:
        return
    await connection.copy_records_to_table(
        "binance_futures_price_trace_100ms",
        schema_name=RAW_CAPTURE_SCHEMA,
        columns=RAW_FUTURES_TRACE_COLUMNS,
        records=records,
        timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS,
    )


async def copy_chainlink_price_events(
    connection: asyncpg.Connection,
    records: Sequence[Any],
) -> None:
    if not records:
        return
    await connection.copy_records_to_table(
        "chainlink_price_events",
        schema_name=RAW_CAPTURE_SCHEMA,
        columns=RAW_CHAINLINK_EVENT_COLUMNS,
        records=records,
        timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS,
    )


class AsyncpgRawCaptureBackend:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        retention_hours: int,
        max_relation_mb: int,
    ) -> None:
        self._pool = pool
        self._retention_hours = retention_hours
        self._max_relation_mb = max_relation_mb
        self._maintenance_status: Optional[RawCaptureMaintenanceStatus] = None

    @property
    def current_partition(self) -> Optional[str]:
        status = self._maintenance_status
        return None if status is None else status.current_partition

    @property
    def raw_table_bytes(self) -> Optional[int]:
        status = self._maintenance_status
        return None if status is None else status.raw_table_bytes

    async def copy_futures_traces(self, records: Sequence[Any]) -> None:
        rows = [
            (
                record.bucket_start_ms,
                record.connection_id,
                record.first_received_wall_ns,
                record.last_received_wall_ns,
                record.first_received_monotonic_ns,
                record.last_received_monotonic_ns,
                record.first_trade_time_ms,
                record.last_trade_time_ms,
                record.first_event_time_ms,
                record.last_event_time_ms,
                record.open_price,
                record.high_price,
                record.low_price,
                record.close_price,
                record.event_count,
                record.first_agg_trade_id,
                record.last_agg_trade_id,
            )
            for record in records
        ]
        if not rows:
            return
        async with self._pool.acquire(
            timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS
        ) as connection:
            await copy_binance_futures_price_traces(connection, rows)

    async def copy_chainlink_events(self, records: Sequence[Any]) -> None:
        rows = [
            (
                record.received_wall_ns,
                record.received_monotonic_ns,
                record.connection_id,
                record.receive_sequence,
                record.provider_event_ms,
                record.provider_message_ms,
                record.price,
            )
            for record in records
        ]
        if not rows:
            return
        async with self._pool.acquire(
            timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS
        ) as connection:
            await copy_chainlink_price_events(connection, rows)

    async def upsert_feed_sessions(self, records: Sequence[Any]) -> None:
        if not records:
            return
        rows = [
            (
                record.connection_id,
                record.source,
                record.connected_wall_ns,
                record.connected_monotonic_ns,
                record.ready_wall_ns,
                record.ready_monotonic_ns,
                record.disconnected_wall_ns,
                record.disconnected_monotonic_ns,
                record.close_reason,
                record.messages_received_total,
                record.messages_accepted_total,
                record.parse_errors_total,
                record.records_dropped_total,
                record.last_receive_sequence,
            )
            for record in records
        ]
        async with self._pool.acquire(
            timeout=RAW_CAPTURE_DATABASE_TIMEOUT_SECONDS
        ) as connection:
            await connection.executemany(
                """
                INSERT INTO raw_capture.feed_sessions AS existing (
                    connection_id,
                    source,
                    connected_wall_ns,
                    connected_monotonic_ns,
                    ready_wall_ns,
                    ready_monotonic_ns,
                    disconnected_wall_ns,
                    disconnected_monotonic_ns,
                    close_reason,
                    messages_received_total,
                    messages_accepted_total,
                    parse_errors_total,
                    records_dropped_total,
                    last_receive_sequence
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14
                )
                ON CONFLICT (connection_id)
                DO UPDATE SET
                    ready_wall_ns = COALESCE(
                        existing.ready_wall_ns,
                        EXCLUDED.ready_wall_ns
                    ),
                    ready_monotonic_ns = COALESCE(
                        existing.ready_monotonic_ns,
                        EXCLUDED.ready_monotonic_ns
                    ),
                    disconnected_wall_ns = COALESCE(
                        existing.disconnected_wall_ns,
                        EXCLUDED.disconnected_wall_ns
                    ),
                    disconnected_monotonic_ns = COALESCE(
                        existing.disconnected_monotonic_ns,
                        EXCLUDED.disconnected_monotonic_ns
                    ),
                    close_reason = COALESCE(
                        existing.close_reason,
                        EXCLUDED.close_reason
                    ),
                    messages_received_total = GREATEST(
                        existing.messages_received_total,
                        EXCLUDED.messages_received_total
                    ),
                    messages_accepted_total = GREATEST(
                        existing.messages_accepted_total,
                        EXCLUDED.messages_accepted_total
                    ),
                    parse_errors_total = GREATEST(
                        existing.parse_errors_total,
                        EXCLUDED.parse_errors_total
                    ),
                    records_dropped_total = GREATEST(
                        existing.records_dropped_total,
                        EXCLUDED.records_dropped_total
                    ),
                    last_receive_sequence = GREATEST(
                        existing.last_receive_sequence,
                        EXCLUDED.last_receive_sequence
                    )
                """,
                rows,
            )

    async def maintain(self) -> bool:
        result = await maintain_raw_capture_partitions(
            self._pool,
            retention_hours=self._retention_hours,
            max_relation_mb=self._max_relation_mb,
        )
        if result is not None:
            self._maintenance_status = result
        status = self._maintenance_status
        return True if status is None else status.storage_permitted

    async def close(self) -> None:
        await self._pool.close()


async def create_raw_capture_backend(
    database_url: str,
    *,
    retention_hours: int,
    max_relation_mb: int,
) -> AsyncpgRawCaptureBackend:
    pool = await create_raw_capture_pool(database_url)
    return AsyncpgRawCaptureBackend(
        pool,
        retention_hours=retention_hours,
        max_relation_mb=max_relation_mb,
    )


def shadow_signal_evaluation_row(record: Any) -> tuple[Any, ...]:
    """Map a typed matured evaluation to the fixed PostgreSQL row contract."""
    decimal_fields = (
        "beta",
        "chainlink_at_forecast",
        "projected_chainlink",
        "pending_move",
        "pending_move_bps",
        "futures_now",
        "futures_reference",
        "actual_chainlink",
        "forecast_error",
        "baseline_error",
    )
    for field_name in decimal_fields:
        value = getattr(record, field_name)
        if value is not None and not isinstance(value, Decimal):
            raise TypeError(f"{field_name} must be Decimal or None")

    return (
        record.selection_schema_version,
        record.selection_policy_version,
        record.selection_fingerprint_sha256,
        record.selection_artifact_sha256,
        record.selection_evidence_end_ms,
        record.model_version,
        record.beta,
        record.generated_ms,
        record.target_ms,
        record.matured_ms,
        record.horizon_ms,
        record.valid,
        record.status,
        tuple(record.invalid_reasons),
        record.state,
        record.market_id,
        record.market_start_ms,
        record.market_end_ms,
        record.ms_to_market_end,
        record.full_horizon_before_market_end,
        record.chainlink_at_forecast,
        record.chainlink_at_forecast_source_timestamp_ms,
        record.chainlink_at_forecast_received_ms,
        record.projected_chainlink,
        record.pending_move,
        record.pending_move_bps,
        record.direction,
        record.futures_now,
        record.futures_now_source_timestamp_ms,
        record.futures_now_received_ms,
        record.futures_reference,
        record.futures_reference_source_timestamp_ms,
        record.futures_reference_received_ms,
        record.futures_reference_target_ms,
        record.futures_reference_gap_ms,
        record.futures_received_age_ms,
        record.chainlink_received_age_ms,
        record.actual_chainlink,
        record.actual_chainlink_source_timestamp_ms,
        record.actual_chainlink_received_ms,
        record.actual_chainlink_age_at_target_ms,
        record.forecast_error,
        record.baseline_error,
    )


async def insert_shadow_signal_evaluations(
    connection: asyncpg.Connection,
    rows: Sequence[tuple[Any, ...]],
) -> None:
    """Insert one batch idempotently without retrying ambiguous failures."""
    if not rows:
        return
    await connection.executemany(
        SHADOW_SIGNAL_EVALUATION_INSERT_SQL,
        rows,
    )


class AsyncpgShadowEvaluationBackend:
    """Single-pool persistence boundary used by the bounded async writer."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        connect_timeout_seconds: float = (
            SHADOW_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS
        ),
    ) -> None:
        self._pool = pool
        self._connect_timeout_seconds = _validated_database_timeout(
            connect_timeout_seconds,
            "connect_timeout_seconds",
        )

    async def write_evaluations(self, records: Sequence[Any]) -> None:
        rows = [shadow_signal_evaluation_row(record) for record in records]
        if not rows:
            return
        async with self._pool.acquire(
            timeout=self._connect_timeout_seconds
        ) as connection:
            await insert_shadow_signal_evaluations(connection, rows)

    async def write(self, records: Sequence[Any]) -> None:
        """Compatibility alias for generic bounded batch writers."""
        await self.write_evaluations(records)

    async def delete_expired(
        self,
        *,
        cutoff_generated_ms: int,
        limit: int = 10_000,
    ) -> int:
        if (
            isinstance(cutoff_generated_ms, bool)
            or not isinstance(cutoff_generated_ms, int)
        ):
            raise TypeError("cutoff_generated_ms must be an integer")
        if cutoff_generated_ms < 0:
            raise ValueError("cutoff_generated_ms must be non-negative")
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("limit must be an integer")
        if limit <= 0:
            raise ValueError("limit must be positive")

        async with self._pool.acquire(
            timeout=self._connect_timeout_seconds
        ) as connection:
            status = await connection.execute(
                """
                WITH expired AS (
                    SELECT ctid
                    FROM shadow_signal_evaluations
                    WHERE generated_ms < $1
                    ORDER BY generated_ms
                    LIMIT $2
                )
                DELETE FROM shadow_signal_evaluations AS evaluations
                USING expired
                WHERE evaluations.ctid = expired.ctid
                """,
                cutoff_generated_ms,
                limit,
            )

        command, separator, count = status.partition(" ")
        if command != "DELETE" or not separator or not count.isdigit():
            raise RuntimeError(
                f"unexpected shadow evaluation delete status: {status!r}"
            )
        return int(count)

    async def close(self) -> None:
        await self._pool.close()


async def create_shadow_evaluation_backend(
    database_url: str,
    *,
    connect_timeout_seconds: float = (
        SHADOW_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS
    ),
    command_timeout_seconds: float = (
        SHADOW_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS
    ),
) -> AsyncpgShadowEvaluationBackend:
    pool = await create_shadow_evaluation_pool(
        database_url,
        connect_timeout_seconds=connect_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )
    return AsyncpgShadowEvaluationBackend(
        pool,
        connect_timeout_seconds=connect_timeout_seconds,
    )


async def get_instrument_id(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
) -> int:
    async with pool.acquire() as connection:
        instrument_id = await connection.fetchval(
            """
            SELECT i.instrument_id
            FROM instruments i
            JOIN providers p ON p.provider_id = i.provider_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
            """,
            provider_code,
            symbol,
        )

    if instrument_id is None:
        raise LookupError(
            f"instrument not found for provider_code={provider_code!r}, symbol={symbol!r}"
        )

    return int(instrument_id)


async def _ensure_market_window(connection: asyncpg.Connection, window: MarketWindow) -> None:
    await connection.execute(
        """
        INSERT INTO market_windows (
            market_id,
            market_start_ms,
            market_end_ms,
            market_start_at,
            market_end_at
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (market_id) DO NOTHING
        """,
        window.market_id,
        window.market_start_ms,
        window.market_end_ms,
        epoch_ms_to_utc_datetime(window.market_start_ms),
        epoch_ms_to_utc_datetime(window.market_end_ms),
    )


async def ensure_market_window(pool: asyncpg.Pool, window: MarketWindow) -> None:
    async with pool.acquire() as connection:
        await _ensure_market_window(connection, window)


async def upsert_price_sample(
    pool: asyncpg.Pool,
    *,
    instrument_id: int,
    sample_second_ms: int,
    window: MarketWindow,
    price: Decimal,
    provider_event_ms: Optional[int],
    received_ms: int,
    source_price_field: str = "c",
    provider_message_ms: Optional[int] = None,
    source_topic: Optional[str] = None,
) -> None:
    if not isinstance(price, Decimal):
        raise TypeError("price must be Decimal")

    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO price_samples (
                    instrument_id,
                    sample_second_ms,
                    sample_second_at,
                    market_id,
                    price,
                    provider_event_ms,
                    received_ms,
                    source_price_field,
                    provider_message_ms,
                    source_topic
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (instrument_id, sample_second_ms)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    provider_event_ms = EXCLUDED.provider_event_ms,
                    received_ms = EXCLUDED.received_ms,
                    source_price_field = EXCLUDED.source_price_field,
                    provider_message_ms = EXCLUDED.provider_message_ms,
                    source_topic = EXCLUDED.source_topic
                """,
                instrument_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                window.market_id,
                price,
                provider_event_ms,
                received_ms,
                source_price_field,
                provider_message_ms,
                source_topic,
            )


async def upsert_polymarket_btc_5m_market(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    slug: str,
    gamma_event_id: Optional[str],
    gamma_market_id: Optional[str],
    condition_id: Optional[str],
    question: Optional[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    up_token_id: str,
    down_token_id: str,
    up_outcome: str,
    down_outcome: str,
    active: Optional[bool],
    closed: Optional[bool],
    archived: Optional[bool],
    raw_gamma: Mapping[str, Any],
    seen_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO polymarket_btc_5m_markets (
                    market_id,
                    slug,
                    gamma_event_id,
                    gamma_market_id,
                    condition_id,
                    question,
                    start_ms,
                    end_ms,
                    start_at,
                    end_at,
                    up_token_id,
                    down_token_id,
                    up_outcome,
                    down_outcome,
                    active,
                    closed,
                    archived,
                    raw_gamma,
                    first_seen_ms,
                    last_seen_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, $16, $17,
                    $18::jsonb, $19, $20
                )
                ON CONFLICT (market_id)
                DO UPDATE SET
                    slug = EXCLUDED.slug,
                    gamma_event_id = EXCLUDED.gamma_event_id,
                    gamma_market_id = EXCLUDED.gamma_market_id,
                    condition_id = EXCLUDED.condition_id,
                    question = EXCLUDED.question,
                    start_ms = EXCLUDED.start_ms,
                    end_ms = EXCLUDED.end_ms,
                    start_at = EXCLUDED.start_at,
                    end_at = EXCLUDED.end_at,
                    up_token_id = EXCLUDED.up_token_id,
                    down_token_id = EXCLUDED.down_token_id,
                    up_outcome = EXCLUDED.up_outcome,
                    down_outcome = EXCLUDED.down_outcome,
                    active = EXCLUDED.active,
                    closed = EXCLUDED.closed,
                    archived = EXCLUDED.archived,
                    raw_gamma = EXCLUDED.raw_gamma,
                    last_seen_ms = EXCLUDED.last_seen_ms,
                    updated_at = now()
                """,
                window.market_id,
                slug,
                gamma_event_id,
                gamma_market_id,
                condition_id,
                question,
                start_ms,
                end_ms,
                epoch_ms_to_utc_datetime(start_ms) if start_ms is not None else None,
                epoch_ms_to_utc_datetime(end_ms) if end_ms is not None else None,
                up_token_id,
                down_token_id,
                up_outcome,
                down_outcome,
                active,
                closed,
                archived,
                json.dumps(raw_gamma, default=str),
                seen_ms,
                seen_ms,
            )


async def fetch_due_polymarket_resolutions(
    pool: asyncpg.Pool,
    *,
    now_ms: int,
    limit: int,
) -> list[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                pm.market_id,
                pm.slug,
                pm.gamma_event_id,
                pm.gamma_market_id,
                pm.condition_id,
                pm.up_token_id,
                pm.down_token_id,
                pm.up_outcome,
                pm.down_outcome,
                mw.market_end_ms,
                COALESCE(resolution.resolution_status, 'pending')
                    AS resolution_status,
                COALESCE(resolution.resolution_attempts, 0)
                    AS resolution_attempts
            FROM polymarket_btc_5m_markets pm
            JOIN market_windows mw ON mw.market_id = pm.market_id
            LEFT JOIN polymarket_btc_5m_resolutions resolution
              ON resolution.market_id = pm.market_id
            WHERE mw.market_end_ms <= $1
              AND (
                resolution.market_id IS NULL
                OR resolution.resolution_status = 'pending'
                OR resolution.chainlink_open_price IS NULL
                OR resolution.chainlink_close_price IS NULL
              )
              AND (
                resolution.next_check_ms IS NULL
                OR resolution.next_check_ms <= $1
              )
            ORDER BY
                COALESCE(resolution.next_check_ms, 0) ASC,
                mw.market_end_ms DESC
            LIMIT $2
            """,
            now_ms,
            max(1, int(limit)),
        )

    return [dict(row) for row in rows]


async def upsert_polymarket_btc_5m_resolution(
    pool: asyncpg.Pool,
    *,
    market_id: int,
    resolution_status: str,
    resolution_type: Optional[str],
    chainlink_open_price: Optional[Decimal],
    chainlink_close_price: Optional[Decimal],
    chainlink_source: Optional[str],
    winner: Optional[str],
    winning_token_id: Optional[str],
    up_payout: Optional[Decimal],
    down_payout: Optional[Decimal],
    resolved_at_ms: Optional[int],
    resolution_source: Optional[str],
    raw_resolution: Optional[Mapping[str, Any]],
    checked_ms: int,
    next_check_ms: Optional[int],
    resolution_attempts: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                """
                INSERT INTO polymarket_btc_5m_resolutions (
                    market_id,
                    resolution_status,
                    resolution_type,
                    chainlink_open_price,
                    chainlink_close_price,
                    chainlink_source,
                    winner,
                    winning_token_id,
                    up_payout,
                    down_payout,
                    resolved_at_ms,
                    resolution_source,
                    raw_resolution,
                    first_checked_ms,
                    last_checked_ms,
                    next_check_ms,
                    resolution_attempts
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13::jsonb, $14, $15, $16, $17
                )
                ON CONFLICT (market_id)
                DO UPDATE SET
                    resolution_status = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.resolution_status
                        ELSE EXCLUDED.resolution_status
                    END,
                    resolution_type = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.resolution_type
                        ELSE EXCLUDED.resolution_type
                    END,
                    chainlink_open_price = COALESCE(
                        EXCLUDED.chainlink_open_price,
                        polymarket_btc_5m_resolutions.chainlink_open_price
                    ),
                    chainlink_close_price = COALESCE(
                        EXCLUDED.chainlink_close_price,
                        polymarket_btc_5m_resolutions.chainlink_close_price
                    ),
                    chainlink_source = COALESCE(
                        EXCLUDED.chainlink_source,
                        polymarket_btc_5m_resolutions.chainlink_source
                    ),
                    winner = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.winner
                        ELSE EXCLUDED.winner
                    END,
                    winning_token_id = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.winning_token_id
                        ELSE EXCLUDED.winning_token_id
                    END,
                    up_payout = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.up_payout
                        ELSE EXCLUDED.up_payout
                    END,
                    down_payout = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.down_payout
                        ELSE EXCLUDED.down_payout
                    END,
                    resolved_at_ms = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.resolved_at_ms
                        ELSE EXCLUDED.resolved_at_ms
                    END,
                    resolution_source = CASE
                        WHEN polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                        THEN polymarket_btc_5m_resolutions.resolution_source
                        ELSE EXCLUDED.resolution_source
                    END,
                    raw_resolution = COALESCE(
                        polymarket_btc_5m_resolutions.raw_resolution,
                        '{}'::jsonb
                    ) || COALESCE(EXCLUDED.raw_resolution, '{}'::jsonb),
                    last_checked_ms = GREATEST(
                        polymarket_btc_5m_resolutions.last_checked_ms,
                        EXCLUDED.last_checked_ms
                    ),
                    next_check_ms = CASE
                        WHEN (
                            polymarket_btc_5m_resolutions.resolution_status
                                = 'resolved'
                            OR EXCLUDED.resolution_status = 'resolved'
                        )
                        AND COALESCE(
                            EXCLUDED.chainlink_open_price,
                            polymarket_btc_5m_resolutions.chainlink_open_price
                        ) IS NOT NULL
                        AND COALESCE(
                            EXCLUDED.chainlink_close_price,
                            polymarket_btc_5m_resolutions.chainlink_close_price
                        ) IS NOT NULL
                        THEN NULL
                        WHEN EXCLUDED.last_checked_ms
                                < polymarket_btc_5m_resolutions.last_checked_ms
                        THEN polymarket_btc_5m_resolutions.next_check_ms
                        WHEN EXCLUDED.next_check_ms IS NULL
                        THEN NULL
                        ELSE GREATEST(
                            EXCLUDED.next_check_ms,
                            polymarket_btc_5m_resolutions.last_checked_ms,
                            EXCLUDED.last_checked_ms
                        )
                    END,
                    resolution_attempts = GREATEST(
                        polymarket_btc_5m_resolutions.resolution_attempts,
                        EXCLUDED.resolution_attempts
                    ),
                    updated_at = now()
                """,
                market_id,
                resolution_status,
                resolution_type,
                chainlink_open_price,
                chainlink_close_price,
                chainlink_source,
                winner,
                winning_token_id,
                up_payout,
                down_payout,
                resolved_at_ms,
                resolution_source,
                (
                    json.dumps(raw_resolution, default=str)
                    if raw_resolution is not None
                    else None
                ),
                checked_ms,
                checked_ms,
                next_check_ms,
                max(0, int(resolution_attempts)),
            )

            if resolution_status == "resolved":
                await connection.execute(
                    """
                    UPDATE polymarket_btc_5m_markets
                    SET closed = TRUE,
                        updated_at = now()
                    WHERE market_id = $1
                    """,
                    market_id,
                )


async def schedule_polymarket_resolution_retry(
    pool: asyncpg.Pool,
    *,
    market_id: int,
    checked_ms: int,
    next_check_ms: int,
    resolution_attempts: int,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO polymarket_btc_5m_resolutions (
                market_id,
                resolution_status,
                first_checked_ms,
                last_checked_ms,
                next_check_ms,
                resolution_attempts
            )
            VALUES ($1, 'pending', $2, $2, $3, $4)
            ON CONFLICT (market_id)
            DO UPDATE SET
                last_checked_ms = GREATEST(
                    polymarket_btc_5m_resolutions.last_checked_ms,
                    EXCLUDED.last_checked_ms
                ),
                next_check_ms = CASE
                    WHEN polymarket_btc_5m_resolutions.resolution_status
                            = 'resolved'
                         AND polymarket_btc_5m_resolutions.chainlink_open_price
                            IS NOT NULL
                         AND polymarket_btc_5m_resolutions.chainlink_close_price
                            IS NOT NULL
                    THEN NULL
                    WHEN EXCLUDED.last_checked_ms
                            < polymarket_btc_5m_resolutions.last_checked_ms
                    THEN polymarket_btc_5m_resolutions.next_check_ms
                    ELSE GREATEST(
                        EXCLUDED.next_check_ms,
                        polymarket_btc_5m_resolutions.last_checked_ms,
                        EXCLUDED.last_checked_ms
                    )
                END,
                resolution_attempts = GREATEST(
                    polymarket_btc_5m_resolutions.resolution_attempts,
                    EXCLUDED.resolution_attempts
                ),
                updated_at = now()
            """,
            market_id,
            checked_ms,
            next_check_ms,
            max(0, int(resolution_attempts)),
        )


async def upsert_polymarket_probability_sample(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    source: str,
    sample_second_ms: int,
    up_token_id: str,
    down_token_id: str,
    up_bid: Optional[Decimal],
    up_ask: Optional[Decimal],
    up_mid: Optional[Decimal],
    down_bid: Optional[Decimal],
    down_ask: Optional[Decimal],
    down_mid: Optional[Decimal],
    up_prob_norm: Optional[Decimal],
    down_prob_norm: Optional[Decimal],
    provider_event_ms: Optional[int],
    received_ms: int,
    raw: Optional[Mapping[str, Any]],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO polymarket_probability_samples (
                    market_id,
                    source,
                    sample_second_ms,
                    sample_second_at,
                    up_token_id,
                    down_token_id,
                    up_bid,
                    up_ask,
                    up_mid,
                    down_bid,
                    down_ask,
                    down_mid,
                    up_prob_norm,
                    down_prob_norm,
                    provider_event_ms,
                    received_ms,
                    raw
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14,
                    $15, $16,
                    $17::jsonb
                )
                ON CONFLICT (market_id, source, sample_second_ms)
                DO UPDATE SET
                    up_token_id = EXCLUDED.up_token_id,
                    down_token_id = EXCLUDED.down_token_id,
                    up_bid = EXCLUDED.up_bid,
                    up_ask = EXCLUDED.up_ask,
                    up_mid = EXCLUDED.up_mid,
                    down_bid = EXCLUDED.down_bid,
                    down_ask = EXCLUDED.down_ask,
                    down_mid = EXCLUDED.down_mid,
                    up_prob_norm = EXCLUDED.up_prob_norm,
                    down_prob_norm = EXCLUDED.down_prob_norm,
                    provider_event_ms = EXCLUDED.provider_event_ms,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                window.market_id,
                source,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                up_token_id,
                down_token_id,
                up_bid,
                up_ask,
                up_mid,
                down_bid,
                down_ask,
                down_mid,
                up_prob_norm,
                down_prob_norm,
                provider_event_ms,
                received_ms,
                json.dumps(raw, default=str) if raw is not None else None,
            )


async def upsert_binance_futures_snapshot(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    futures_last_price: Optional[Decimal],
    futures_last_price_time_ms: Optional[int],
    mark_price: Optional[Decimal],
    index_price: Optional[Decimal],
    last_funding_rate: Optional[Decimal],
    next_funding_time_ms: Optional[int],
    premium_index_time_ms: Optional[int],
    open_interest: Optional[Decimal],
    open_interest_time_ms: Optional[int],
    oi_notional_usdt: Optional[Decimal],
    premium_bps: Optional[Decimal],
    received_ms: int,
    raw: Optional[Mapping[str, Any]],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_futures_snapshots (
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    futures_last_price,
                    futures_last_price_time_ms,
                    mark_price,
                    index_price,
                    last_funding_rate,
                    next_funding_time_ms,
                    premium_index_time_ms,
                    open_interest,
                    open_interest_time_ms,
                    oi_notional_usdt,
                    premium_bps,
                    received_ms,
                    raw
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9, $10, $11,
                    $12, $13,
                    $14, $15,
                    $16,
                    $17::jsonb
                )
                ON CONFLICT (symbol, sample_second_ms)
                DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    sample_second_at = EXCLUDED.sample_second_at,
                    futures_last_price = EXCLUDED.futures_last_price,
                    futures_last_price_time_ms = EXCLUDED.futures_last_price_time_ms,
                    mark_price = EXCLUDED.mark_price,
                    index_price = EXCLUDED.index_price,
                    last_funding_rate = EXCLUDED.last_funding_rate,
                    next_funding_time_ms = EXCLUDED.next_funding_time_ms,
                    premium_index_time_ms = EXCLUDED.premium_index_time_ms,
                    open_interest = EXCLUDED.open_interest,
                    open_interest_time_ms = EXCLUDED.open_interest_time_ms,
                    oi_notional_usdt = EXCLUDED.oi_notional_usdt,
                    premium_bps = EXCLUDED.premium_bps,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                futures_last_price,
                futures_last_price_time_ms,
                mark_price,
                index_price,
                last_funding_rate,
                next_funding_time_ms,
                premium_index_time_ms,
                open_interest,
                open_interest_time_ms,
                oi_notional_usdt,
                premium_bps,
                received_ms,
                json.dumps(raw, default=str) if raw is not None else None,
            )


async def upsert_binance_futures_oi_5m_summary(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    source_window_start_ms: int,
    source_window_end_ms: int,
    effective_window: MarketWindow,
    binance_timestamp_ms: int,
    sum_open_interest: Optional[Decimal],
    sum_open_interest_value: Optional[Decimal],
    received_ms: int,
    raw: Optional[Mapping[str, Any]],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, effective_window)
            await connection.execute(
                """
                INSERT INTO binance_futures_oi_5m_summaries (
                    symbol,
                    source_window_start_ms,
                    source_window_end_ms,
                    effective_market_id,
                    binance_timestamp_ms,
                    sum_open_interest,
                    sum_open_interest_value,
                    received_ms,
                    raw
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                ON CONFLICT (symbol, source_window_start_ms, source_window_end_ms)
                DO UPDATE SET
                    effective_market_id = EXCLUDED.effective_market_id,
                    binance_timestamp_ms = EXCLUDED.binance_timestamp_ms,
                    sum_open_interest = EXCLUDED.sum_open_interest,
                    sum_open_interest_value = EXCLUDED.sum_open_interest_value,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                symbol,
                source_window_start_ms,
                source_window_end_ms,
                effective_window.market_id,
                binance_timestamp_ms,
                sum_open_interest,
                sum_open_interest_value,
                received_ms,
                json.dumps(raw, default=str) if raw is not None else None,
            )


async def upsert_binance_flow_1s(
    pool: asyncpg.Pool,
    *,
    venue: str,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    buy_base: Decimal,
    sell_base: Decimal,
    buy_quote: Decimal,
    sell_quote: Decimal,
    delta_quote: Decimal,
    total_quote: Decimal,
    taker_imbalance: Optional[Decimal],
    cvd_quote: Decimal,
    cvd_10s: Decimal,
    cvd_30s: Decimal,
    imbalance_10s: Optional[Decimal],
    imbalance_30s: Optional[Decimal],
    agg_trade_count: int,
    trade_count: int,
    max_trade_quote: Optional[Decimal],
    first_agg_trade_id: Optional[int],
    last_agg_trade_id: Optional[int],
    last_trade_time_ms: Optional[int],
    last_event_time_ms: Optional[int],
    received_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_flow_1s (
                    venue,
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    buy_base,
                    sell_base,
                    buy_quote,
                    sell_quote,
                    delta_quote,
                    total_quote,
                    taker_imbalance,
                    cvd_quote,
                    cvd_10s,
                    cvd_30s,
                    imbalance_10s,
                    imbalance_30s,
                    agg_trade_count,
                    trade_count,
                    max_trade_quote,
                    first_agg_trade_id,
                    last_agg_trade_id,
                    last_trade_time_ms,
                    last_event_time_ms,
                    received_ms
                )
                VALUES (
                    $1, $2,
                    $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12,
                    $13, $14, $15, $16, $17,
                    $18, $19, $20,
                    $21, $22, $23, $24,
                    $25
                )
                ON CONFLICT (venue, symbol, sample_second_ms)
                DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    sample_second_at = EXCLUDED.sample_second_at,
                    buy_base = EXCLUDED.buy_base,
                    sell_base = EXCLUDED.sell_base,
                    buy_quote = EXCLUDED.buy_quote,
                    sell_quote = EXCLUDED.sell_quote,
                    delta_quote = EXCLUDED.delta_quote,
                    total_quote = EXCLUDED.total_quote,
                    taker_imbalance = EXCLUDED.taker_imbalance,
                    cvd_quote = EXCLUDED.cvd_quote,
                    cvd_10s = EXCLUDED.cvd_10s,
                    cvd_30s = EXCLUDED.cvd_30s,
                    imbalance_10s = EXCLUDED.imbalance_10s,
                    imbalance_30s = EXCLUDED.imbalance_30s,
                    agg_trade_count = EXCLUDED.agg_trade_count,
                    trade_count = EXCLUDED.trade_count,
                    max_trade_quote = EXCLUDED.max_trade_quote,
                    first_agg_trade_id = EXCLUDED.first_agg_trade_id,
                    last_agg_trade_id = EXCLUDED.last_agg_trade_id,
                    last_trade_time_ms = EXCLUDED.last_trade_time_ms,
                    last_event_time_ms = EXCLUDED.last_event_time_ms,
                    received_ms = EXCLUDED.received_ms
                """,
                venue,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                buy_base,
                sell_base,
                buy_quote,
                sell_quote,
                delta_quote,
                total_quote,
                taker_imbalance,
                cvd_quote,
                cvd_10s,
                cvd_30s,
                imbalance_10s,
                imbalance_30s,
                agg_trade_count,
                trade_count,
                max_trade_quote,
                first_agg_trade_id,
                last_agg_trade_id,
                last_trade_time_ms,
                last_event_time_ms,
                received_ms,
            )


async def upsert_binance_book_1s(
    pool: asyncpg.Pool,
    *,
    venue: str,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    bid: Decimal,
    ask: Decimal,
    bid_qty: Decimal,
    ask_qty: Decimal,
    mid: Decimal,
    spread: Decimal,
    spread_bps: Decimal,
    book_imbalance: Optional[Decimal],
    microprice: Optional[Decimal],
    update_id: Optional[int],
    event_time_ms: Optional[int],
    transaction_time_ms: Optional[int],
    received_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_book_1s (
                    venue,
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    bid,
                    ask,
                    bid_qty,
                    ask_qty,
                    mid,
                    spread,
                    spread_bps,
                    book_imbalance,
                    microprice,
                    update_id,
                    event_time_ms,
                    transaction_time_ms,
                    received_ms
                )
                VALUES (
                    $1, $2,
                    $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13, $14,
                    $15, $16, $17,
                    $18
                )
                ON CONFLICT (venue, symbol, sample_second_ms)
                DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    sample_second_at = EXCLUDED.sample_second_at,
                    bid = EXCLUDED.bid,
                    ask = EXCLUDED.ask,
                    bid_qty = EXCLUDED.bid_qty,
                    ask_qty = EXCLUDED.ask_qty,
                    mid = EXCLUDED.mid,
                    spread = EXCLUDED.spread,
                    spread_bps = EXCLUDED.spread_bps,
                    book_imbalance = EXCLUDED.book_imbalance,
                    microprice = EXCLUDED.microprice,
                    update_id = EXCLUDED.update_id,
                    event_time_ms = EXCLUDED.event_time_ms,
                    transaction_time_ms = EXCLUDED.transaction_time_ms,
                    received_ms = EXCLUDED.received_ms
                """,
                venue,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                bid,
                ask,
                bid_qty,
                ask_qty,
                mid,
                spread,
                spread_bps,
                book_imbalance,
                microprice,
                update_id,
                event_time_ms,
                transaction_time_ms,
                received_ms,
            )


async def health_check(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.fetchval("SELECT 1")


async def fetch_latest_price(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
) -> Optional[Mapping[str, Any]]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                ps.price AS price,
                ps.sample_second_ms AS sample_second_ms,
                ps.sample_second_at AS sample_second_at,
                ps.provider_event_ms AS provider_event_ms,
                ps.received_ms AS received_ms,
                ps.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
            ORDER BY ps.sample_second_ms DESC
            LIMIT 1
            """,
            provider_code,
            symbol,
        )

    return dict(row) if row is not None else None


async def fetch_latest_market_id(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
) -> Optional[int]:
    async with pool.acquire() as connection:
        market_id = await connection.fetchval(
            """
            SELECT max(ps.market_id)
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
            """,
            provider_code,
            symbol,
        )

    return int(market_id) if market_id is not None else None


async def fetch_recent_market_windows(
    pool: asyncpg.Pool,
    *,
    server_time_ms: int,
    include_current: bool,
    before_market_id: Optional[int],
    limit: int,
) -> list[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            WITH instrument_ids AS (
                SELECT
                    max(i.instrument_id) FILTER (
                        WHERE p.provider_code = 'binance_spot'
                          AND i.symbol = 'BTCUSDT'
                    ) AS binance_id,
                    max(i.instrument_id) FILTER (
                        WHERE p.provider_code = 'polymarket_chainlink_rtds'
                          AND i.symbol = 'BTCUSD'
                    ) AS chainlink_id
                FROM instruments i
                JOIN providers p ON p.provider_id = i.provider_id
            ),
            candidates AS MATERIALIZED (
                SELECT
                    mw.market_id,
                    mw.market_start_ms,
                    mw.market_end_ms,
                    mw.market_start_at,
                    mw.market_end_at
                FROM market_windows mw
                CROSS JOIN instrument_ids ids
                WHERE mw.market_start_ms <= $1::BIGINT
                  AND ($2::BOOLEAN OR mw.market_end_ms <= $1::BIGINT)
                  AND ($3::BIGINT IS NULL OR mw.market_id < $3::BIGINT)
                  AND (
                    EXISTS (
                        SELECT 1
                        FROM price_samples ps
                        WHERE ps.market_id = mw.market_id
                          AND ps.instrument_id IN (ids.binance_id, ids.chainlink_id)
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM binance_futures_snapshots f
                        WHERE f.market_id = mw.market_id
                          AND f.symbol = 'BTCUSDT'
                          AND (
                            f.futures_last_price IS NOT NULL
                            OR f.open_interest IS NOT NULL
                          )
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM binance_flow_1s flow
                        WHERE flow.market_id = mw.market_id
                          AND flow.venue = 'binance_usdm_perp'
                          AND flow.symbol = 'BTCUSDT'
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM binance_book_1s book
                        WHERE book.market_id = mw.market_id
                          AND book.venue = 'binance_usdm_perp'
                          AND book.symbol = 'BTCUSDT'
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM polymarket_probability_samples probabilities
                        WHERE probabilities.market_id = mw.market_id
                          AND probabilities.source = 'polymarket_clob'
                          AND probabilities.up_ask IS NOT NULL
                          AND probabilities.down_ask IS NOT NULL
                    )
                  )
                ORDER BY mw.market_id DESC
                LIMIT $4::INTEGER
            )
            SELECT
                candidates.market_id,
                candidates.market_start_ms,
                candidates.market_end_ms,
                candidates.market_start_at,
                candidates.market_end_at,
                candidates.market_end_ms <= $1::BIGINT AS is_complete,
                COALESCE(price_counts.binance_count, 0)::INTEGER
                    AS binance_sample_count,
                COALESCE(price_counts.chainlink_count, 0)::INTEGER
                    AS chainlink_sample_count,
                COALESCE(futures_counts.futures_count, 0)::INTEGER
                    AS futures_sample_count,
                COALESCE(futures_counts.open_interest_count, 0)::INTEGER
                    AS open_interest_sample_count,
                COALESCE(flow_counts.sample_count, 0)::INTEGER
                    AS flow_sample_count,
                COALESCE(book_counts.sample_count, 0)::INTEGER
                    AS book_sample_count,
                COALESCE(probability_counts.sample_count, 0)::INTEGER
                    AS probability_sample_count
            FROM candidates
            CROSS JOIN instrument_ids ids
            LEFT JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE ps.instrument_id = ids.binance_id
                    ) AS binance_count,
                    count(*) FILTER (
                        WHERE ps.instrument_id = ids.chainlink_id
                    ) AS chainlink_count
                FROM price_samples ps
                WHERE ps.market_id = candidates.market_id
                  AND ps.instrument_id IN (ids.binance_id, ids.chainlink_id)
            ) price_counts ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    count(*) FILTER (
                        WHERE futures_last_price IS NOT NULL
                    ) AS futures_count,
                    count(*) FILTER (
                        WHERE open_interest IS NOT NULL
                    ) AS open_interest_count
                FROM binance_futures_snapshots
                WHERE market_id = candidates.market_id
                  AND symbol = 'BTCUSDT'
            ) futures_counts ON TRUE
            LEFT JOIN LATERAL (
                SELECT count(*) AS sample_count
                FROM binance_flow_1s
                WHERE market_id = candidates.market_id
                  AND venue = 'binance_usdm_perp'
                  AND symbol = 'BTCUSDT'
            ) flow_counts ON TRUE
            LEFT JOIN LATERAL (
                SELECT count(*) AS sample_count
                FROM binance_book_1s
                WHERE market_id = candidates.market_id
                  AND venue = 'binance_usdm_perp'
                  AND symbol = 'BTCUSDT'
            ) book_counts ON TRUE
            LEFT JOIN LATERAL (
                SELECT count(*) AS sample_count
                FROM polymarket_probability_samples
                WHERE market_id = candidates.market_id
                  AND source = 'polymarket_clob'
                  AND up_ask IS NOT NULL
                  AND down_ask IS NOT NULL
            ) probability_counts ON TRUE
            ORDER BY candidates.market_id DESC
            """,
            server_time_ms,
            include_current,
            before_market_id,
            max(1, int(limit)),
        )

    return [dict(row) for row in rows]


async def fetch_market_summary(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
    market_id: int,
) -> Optional[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                mw.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms,
                mw.market_start_at AS market_start_at,
                mw.market_end_at AS market_end_at,
                ps.sample_second_ms AS sample_second_ms,
                ps.sample_second_at AS sample_second_at,
                ps.price AS price
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
              AND ps.market_id = $3
            ORDER BY ps.sample_second_ms ASC
            """,
            provider_code,
            symbol,
            market_id,
        )

    if not rows:
        return None

    samples = [
        {
            "sample_second_ms": row["sample_second_ms"],
            "sample_second_at": row["sample_second_at"],
            "price": row["price"],
        }
        for row in rows
    ]
    prices = [sample["price"] for sample in samples]
    first = rows[0]

    return {
        "provider": first["provider"],
        "symbol": first["symbol"],
        "market_id": first["market_id"],
        "market_start_ms": first["market_start_ms"],
        "market_end_ms": first["market_end_ms"],
        "market_start_at": first["market_start_at"],
        "market_end_at": first["market_end_at"],
        "sample_count": len(samples),
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "samples": samples,
    }


def build_market_sources_summary(
    rows: list[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    if not rows:
        return None

    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = (row["provider"], row["symbol"])
        grouped.setdefault(key, []).append(row)

    first = rows[0]
    sources = []
    for source_rows in grouped.values():
        prices = [row["price"] for row in source_rows]
        source_first = source_rows[0]
        source_latest = source_rows[-1]
        sources.append(
            {
                "provider": source_first["provider"],
                "symbol": source_first["symbol"],
                "quote_asset": source_first["quote_asset"],
                "sample_count": len(source_rows),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "latest_sample_second_ms": source_latest["sample_second_ms"],
                "latest_provider_event_ms": source_latest["provider_event_ms"],
                "latest_received_ms": source_latest["received_ms"],
            }
        )

    return {
        "market_id": first["market_id"],
        "market_start_ms": first["market_start_ms"],
        "market_end_ms": first["market_end_ms"],
        "market_start_at": first["market_start_at"],
        "market_end_at": first["market_end_at"],
        "sources": sources,
    }


async def fetch_market_summaries_for_btc_sources(
    pool: asyncpg.Pool,
    market_id: int,
) -> Optional[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                i.quote_asset AS quote_asset,
                mw.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms,
                mw.market_start_at AS market_start_at,
                mw.market_end_at AS market_end_at,
                ps.sample_second_ms AS sample_second_ms,
                ps.price AS price,
                ps.provider_event_ms AS provider_event_ms,
                ps.received_ms AS received_ms
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE ps.market_id = $1
              AND (
                (p.provider_code = 'binance_spot' AND i.symbol = 'BTCUSDT')
                OR
                (p.provider_code = 'polymarket_chainlink_rtds' AND i.symbol = 'BTCUSD')
              )
            ORDER BY
                CASE p.provider_code
                    WHEN 'binance_spot' THEN 0
                    WHEN 'polymarket_chainlink_rtds' THEN 1
                    ELSE 2
                END,
                i.symbol ASC,
                ps.sample_second_ms ASC
            """,
            market_id,
        )

    return build_market_sources_summary([dict(row) for row in rows])


def build_market_download_payload(
    rows: list[Mapping[str, Any]],
    *,
    server_time_ms: int,
    include_probabilities: bool,
    include_futures: bool,
    include_oi: bool,
    include_flow: bool = False,
    include_book: bool = False,
) -> Optional[dict[str, Any]]:
    if not rows:
        return None

    first = rows[0]
    market_start_ms = int(first["market_start_ms"])
    market_end_ms = int(first["market_end_ms"])

    series = []
    for row in rows:
        sample_second_ms = int(row["sample_second_ms"])
        binance_source_ms = row.get("binance_provider_event_ms")
        binance_received_ms = row.get("binance_received_ms")
        chainlink_source_ms = row.get("chainlink_provider_event_ms")
        chainlink_message_ms = row.get("chainlink_provider_message_ms")
        chainlink_received_ms = row.get("chainlink_received_ms")
        chainlink_sample_second_ms = row.get("chainlink_sample_second_ms")
        futures_last_price_time_ms = row.get("futures_last_price_time_ms")
        premium_index_time_ms = row.get("premium_index_time_ms")
        open_interest_time_ms = row.get("open_interest_time_ms")
        futures_received_ms = row.get("futures_received_ms")
        flow_last_trade_time_ms = row.get("flow_last_trade_time_ms")
        flow_last_event_time_ms = row.get("flow_last_event_time_ms")
        flow_received_ms = row.get("flow_received_ms")
        book_event_time_ms = row.get("book_event_time_ms")
        book_transaction_time_ms = row.get("book_transaction_time_ms")
        book_received_ms = row.get("book_received_ms")
        book_source_time_ms = book_event_time_ms or book_transaction_time_ms
        item = {
            "t": (sample_second_ms - market_start_ms) // 1000,
            "timestamp_ms": sample_second_ms,
            "timestamp_at": utc_datetime_to_z(epoch_ms_to_utc_datetime(sample_second_ms)),
            "prices": {
                "binance": decimal_2dp_or_none(row["binance_price"]),
                "chainlink": decimal_2dp_or_none(row["chainlink_price"]),
            },
            "freshness": {
                "binance": {
                    "source_ms": binance_source_ms,
                    "received_ms": binance_received_ms,
                    **freshness_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=binance_source_ms,
                        received_ms=binance_received_ms,
                    ),
                },
                "chainlink": {
                    "source_ms": chainlink_source_ms,
                    "message_ms": chainlink_message_ms,
                    "received_ms": chainlink_received_ms,
                    "is_carried_forward": (
                        chainlink_sample_second_ms is not None
                        and int(chainlink_sample_second_ms) != sample_second_ms
                    ),
                    **freshness_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=chainlink_source_ms,
                        received_ms=chainlink_received_ms,
                    ),
                },
                "futures_last": {
                    "source_ms": futures_last_price_time_ms,
                    "received_ms": futures_received_ms,
                    **freshness_age_only_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=futures_last_price_time_ms,
                        received_ms=futures_received_ms,
                    ),
                },
                "open_interest": {
                    "source_ms": open_interest_time_ms,
                    "received_ms": futures_received_ms,
                    **freshness_age_only_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=open_interest_time_ms,
                        received_ms=futures_received_ms,
                    ),
                },
            },
        }

        if include_flow:
            item["flow"] = {
                "buy_base": decimal_string_or_none(row.get("flow_buy_base")),
                "sell_base": decimal_string_or_none(row.get("flow_sell_base")),
                "buy_quote": decimal_string_or_none(row.get("flow_buy_quote")),
                "sell_quote": decimal_string_or_none(row.get("flow_sell_quote")),
                "delta_quote": decimal_string_or_none(row.get("flow_delta_quote")),
                "total_quote": decimal_string_or_none(row.get("flow_total_quote")),
                "taker_imbalance": decimal_string_or_none(
                    row.get("flow_taker_imbalance")
                ),
                "cvd_quote": decimal_string_or_none(row.get("flow_cvd_quote")),
                "cvd_10s": decimal_string_or_none(row.get("flow_cvd_10s")),
                "cvd_30s": decimal_string_or_none(row.get("flow_cvd_30s")),
                "imbalance_10s": decimal_string_or_none(
                    row.get("flow_imbalance_10s")
                ),
                "imbalance_30s": decimal_string_or_none(
                    row.get("flow_imbalance_30s")
                ),
                "agg_trade_count": row.get("flow_agg_trade_count"),
                "trade_count": row.get("flow_trade_count"),
                "max_trade_quote": decimal_string_or_none(
                    row.get("flow_max_trade_quote")
                ),
                "first_agg_trade_id": row.get("flow_first_agg_trade_id"),
                "last_agg_trade_id": row.get("flow_last_agg_trade_id"),
            }
            item["freshness"]["futures_flow"] = {
                "source_ms": flow_last_trade_time_ms,
                "event_ms": flow_last_event_time_ms,
                "received_ms": flow_received_ms,
                **freshness_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=flow_last_trade_time_ms,
                    received_ms=flow_received_ms,
                ),
            }

        if include_book:
            item["book"] = {
                "bid": decimal_string_or_none(row.get("book_bid")),
                "ask": decimal_string_or_none(row.get("book_ask")),
                "bid_qty": decimal_string_or_none(row.get("book_bid_qty")),
                "ask_qty": decimal_string_or_none(row.get("book_ask_qty")),
                "mid": decimal_string_or_none(row.get("book_mid")),
                "spread": decimal_string_or_none(row.get("book_spread")),
                "spread_bps": decimal_string_or_none(row.get("book_spread_bps")),
                "book_imbalance": decimal_string_or_none(
                    row.get("book_book_imbalance")
                ),
                "microprice": decimal_string_or_none(row.get("book_microprice")),
                "update_id": row.get("book_update_id"),
            }
            item["freshness"]["futures_book"] = {
                "source_ms": book_source_time_ms,
                "event_ms": book_event_time_ms,
                "transaction_ms": book_transaction_time_ms,
                "received_ms": book_received_ms,
                **freshness_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=book_source_time_ms,
                    received_ms=book_received_ms,
                ),
            }

        if include_probabilities:
            item["probabilities"] = {
                "up": {
                    "ask": decimal_2dp_or_none(row["up_ask"]),
                },
                "down": {
                    "ask": decimal_2dp_or_none(row["down_ask"]),
                },
            }

        if include_futures:
            item["futures"] = {
                "last": money_2dp(row["futures_last_price"]),
                "mark": money_2dp(row["mark_price"]),
                "index": money_2dp(row["index_price"]),
                "premium_bps": money_2dp(row["premium_bps"]),
            }

        if include_oi:
            item["open_interest"] = {
                "contracts": oi_3dp(row["open_interest"]),
                "notional_usdt": money_2dp(row["oi_notional_usdt"]),
                "delta_30s": oi_3dp(row["oi_delta_30s"]),
                "delta_60s": oi_3dp(row["oi_delta_60s"]),
                "delta_300s": oi_3dp(row["oi_delta_300s"]),
            }

        series.append(item)

    resolution_status = str(first.get("resolution_status") or "pending")
    chainlink_open = decimal_compact_or_none(
        first.get("resolution_chainlink_open_price")
    )
    chainlink_close = decimal_compact_or_none(
        first.get("resolution_chainlink_close_price")
    )
    if chainlink_open is not None and chainlink_close is not None:
        chainlink_status = "official"
    else:
        chainlink_status = "pending"

    payload = {
        "schema_version": 2,
        "server_time_ms": server_time_ms,
        "market": {
            "market_id": int(first["market_id"]),
            "market_start_ms": market_start_ms,
            "market_end_ms": market_end_ms,
            "market_start_at": utc_datetime_to_z(first["market_start_at"]),
            "market_end_at": utc_datetime_to_z(first["market_end_at"]),
            "seconds_expected": (market_end_ms - market_start_ms) // 1000,
            "chainlink_resolution": {
                "open": chainlink_open,
                "close": chainlink_close,
                "status": chainlink_status,
                "source": first.get("resolution_chainlink_source"),
            },
            "resolution": {
                "status": resolution_status,
                "resolution_type": first.get("resolution_type"),
                "winner": first.get("resolution_winner"),
                "winning_token_id": first.get("resolution_winning_token_id"),
                "resolved_at_ms": first.get("resolution_resolved_at_ms"),
                "official_payouts": {
                    "up": decimal_compact_or_none(
                        first.get("resolution_up_payout")
                    ),
                    "down": decimal_compact_or_none(
                        first.get("resolution_down_payout")
                    ),
                },
                "source": first.get("resolution_source"),
            },
        },
        "series": series,
    }

    if include_oi and first.get("prev_oi_source_window_start_ms") is not None:
        payload["previous_5m_oi_summary"] = {
            "source_window_start_ms": first["prev_oi_source_window_start_ms"],
            "source_window_end_ms": first["prev_oi_source_window_end_ms"],
            "effective_market_id": int(first["market_id"]),
            "sum_open_interest": oi_3dp(first["prev_oi_sum_open_interest"]),
            "sum_open_interest_value": money_2dp(
                first["prev_oi_sum_open_interest_value"]
            ),
        }

    return payload


async def fetch_market_download_payload(
    pool: asyncpg.Pool,
    *,
    market_id: int,
    server_time_ms: int,
    include_probabilities: bool,
    include_futures: bool,
    include_oi: bool,
    include_flow: bool = False,
    include_book: bool = False,
    fill_display: bool = False,
    max_carry_forward_ms: int = 10_000,
) -> Optional[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            WITH mw AS (
                SELECT *
                FROM market_windows
                WHERE market_id = $1
            ),
            seconds AS (
                SELECT generate_series(
                    (SELECT market_start_ms FROM mw),
                    (SELECT market_end_ms FROM mw) - 1000,
                    1000::BIGINT
                )::BIGINT AS sample_second_ms
            ),
            binance AS (
                SELECT
                    ps.sample_second_ms,
                    ps.price,
                    ps.provider_event_ms AS binance_provider_event_ms,
                    ps.received_ms AS binance_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'binance_spot'
                  AND i.symbol = 'BTCUSDT'
            ),
            chainlink AS (
                SELECT
                    ps.sample_second_ms,
                    ps.price,
                    ps.provider_event_ms AS chainlink_provider_event_ms,
                    ps.provider_message_ms AS chainlink_provider_message_ms,
                    ps.received_ms AS chainlink_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'polymarket_chainlink_rtds'
                  AND i.symbol = 'BTCUSD'
            ),
            probs AS (
                SELECT *
                FROM polymarket_probability_samples
                WHERE market_id = $1
                  AND source = 'polymarket_clob'
            ),
            futures AS (
                SELECT *
                FROM binance_futures_snapshots
                WHERE market_id = $1
                  AND symbol = 'BTCUSDT'
            ),
            flow AS (
                SELECT *
                FROM binance_flow_1s
                WHERE market_id = $1
                  AND venue = 'binance_usdm_perp'
                  AND symbol = 'BTCUSDT'
            ),
            book AS (
                SELECT *
                FROM binance_book_1s
                WHERE market_id = $1
                  AND venue = 'binance_usdm_perp'
                  AND symbol = 'BTCUSDT'
            ),
            oi_prev AS (
                SELECT *
                FROM binance_futures_oi_5m_summaries
                WHERE effective_market_id = $1
                  AND symbol = 'BTCUSDT'
                ORDER BY source_window_end_ms DESC
                LIMIT 1
            ),
            oi_30 AS (
                SELECT
                    f.sample_second_ms,
                    prev.open_interest AS open_interest_30s_ago
                FROM futures f
                LEFT JOIN binance_futures_snapshots prev
                  ON prev.symbol = f.symbol
                 AND prev.sample_second_ms = f.sample_second_ms - 30000
            ),
            oi_60 AS (
                SELECT
                    f.sample_second_ms,
                    prev.open_interest AS open_interest_60s_ago
                FROM futures f
                LEFT JOIN binance_futures_snapshots prev
                  ON prev.symbol = f.symbol
                 AND prev.sample_second_ms = f.sample_second_ms - 60000
            ),
            oi_300 AS (
                SELECT
                    f.sample_second_ms,
                    prev.open_interest AS open_interest_300s_ago
                FROM futures f
                LEFT JOIN binance_futures_snapshots prev
                  ON prev.symbol = f.symbol
                 AND prev.sample_second_ms = f.sample_second_ms - 300000
            ),
            pm AS (
                SELECT *
                FROM polymarket_btc_5m_markets
                WHERE market_id = $1
            ),
            resolution AS (
                SELECT *
                FROM polymarket_btc_5m_resolutions
                WHERE market_id = $1
            )
            SELECT
                mw.market_id,
                mw.market_start_ms,
                mw.market_end_ms,
                mw.market_start_at,
                mw.market_end_at,

                pm.slug,
                pm.question,
                pm.condition_id,
                pm.up_token_id,
                pm.down_token_id,

                resolution.resolution_status,
                resolution.resolution_type,
                resolution.chainlink_open_price
                    AS resolution_chainlink_open_price,
                resolution.chainlink_close_price
                    AS resolution_chainlink_close_price,
                resolution.chainlink_source
                    AS resolution_chainlink_source,
                resolution.winner AS resolution_winner,
                resolution.winning_token_id AS resolution_winning_token_id,
                resolution.up_payout AS resolution_up_payout,
                resolution.down_payout AS resolution_down_payout,
                resolution.resolved_at_ms AS resolution_resolved_at_ms,
                resolution.resolution_source,

                s.sample_second_ms,

                b.price AS binance_price,
                b.binance_provider_event_ms,
                b.binance_received_ms,

                c.sample_second_ms AS chainlink_sample_second_ms,
                c.price AS chainlink_price,
                c.chainlink_provider_event_ms,
                c.chainlink_provider_message_ms,
                c.chainlink_received_ms,

                probs.up_bid,
                probs.up_ask,
                probs.up_mid,
                probs.down_bid,
                probs.down_ask,
                probs.down_mid,
                probs.up_prob_norm,
                probs.down_prob_norm,

                f.futures_last_price,
                f.mark_price,
                f.index_price,
                f.last_funding_rate,
                f.next_funding_time_ms,
                f.futures_last_price_time_ms,
                f.premium_index_time_ms,
                f.open_interest,
                f.open_interest_time_ms,
                f.oi_notional_usdt,
                f.premium_bps,
                f.received_ms AS futures_received_ms,

                (f.open_interest - oi_30.open_interest_30s_ago) AS oi_delta_30s,
                (f.open_interest - oi_60.open_interest_60s_ago) AS oi_delta_60s,
                (f.open_interest - oi_300.open_interest_300s_ago) AS oi_delta_300s,

                flow.buy_base AS flow_buy_base,
                flow.sell_base AS flow_sell_base,
                flow.buy_quote AS flow_buy_quote,
                flow.sell_quote AS flow_sell_quote,
                flow.delta_quote AS flow_delta_quote,
                flow.total_quote AS flow_total_quote,
                flow.taker_imbalance AS flow_taker_imbalance,
                flow.cvd_quote AS flow_cvd_quote,
                flow.cvd_10s AS flow_cvd_10s,
                flow.cvd_30s AS flow_cvd_30s,
                flow.imbalance_10s AS flow_imbalance_10s,
                flow.imbalance_30s AS flow_imbalance_30s,
                flow.agg_trade_count AS flow_agg_trade_count,
                flow.trade_count AS flow_trade_count,
                flow.max_trade_quote AS flow_max_trade_quote,
                flow.first_agg_trade_id AS flow_first_agg_trade_id,
                flow.last_agg_trade_id AS flow_last_agg_trade_id,
                flow.last_trade_time_ms AS flow_last_trade_time_ms,
                flow.last_event_time_ms AS flow_last_event_time_ms,
                flow.received_ms AS flow_received_ms,

                book.bid AS book_bid,
                book.ask AS book_ask,
                book.bid_qty AS book_bid_qty,
                book.ask_qty AS book_ask_qty,
                book.mid AS book_mid,
                book.spread AS book_spread,
                book.spread_bps AS book_spread_bps,
                book.book_imbalance AS book_book_imbalance,
                book.microprice AS book_microprice,
                book.update_id AS book_update_id,
                book.event_time_ms AS book_event_time_ms,
                book.transaction_time_ms AS book_transaction_time_ms,
                book.received_ms AS book_received_ms,

                oi_prev.source_window_start_ms AS prev_oi_source_window_start_ms,
                oi_prev.source_window_end_ms AS prev_oi_source_window_end_ms,
                oi_prev.sum_open_interest AS prev_oi_sum_open_interest,
                oi_prev.sum_open_interest_value AS prev_oi_sum_open_interest_value
            FROM seconds s
            CROSS JOIN mw
            LEFT JOIN pm ON pm.market_id = mw.market_id
            LEFT JOIN resolution ON resolution.market_id = mw.market_id
            LEFT JOIN binance b ON b.sample_second_ms = s.sample_second_ms
            LEFT JOIN LATERAL (
                SELECT *
                FROM chainlink cl
                WHERE (
                    $2::BOOLEAN
                    AND cl.sample_second_ms <= s.sample_second_ms
                    AND cl.sample_second_ms >= s.sample_second_ms - $3::BIGINT
                )
                OR (
                    NOT $2::BOOLEAN
                    AND cl.sample_second_ms = s.sample_second_ms
                )
                ORDER BY cl.sample_second_ms DESC
                LIMIT 1
            ) c ON TRUE
            LEFT JOIN probs ON probs.sample_second_ms = s.sample_second_ms
            LEFT JOIN futures f ON f.sample_second_ms = s.sample_second_ms
            LEFT JOIN flow ON flow.sample_second_ms = s.sample_second_ms
            LEFT JOIN book ON book.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_30 ON oi_30.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_60 ON oi_60.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_300 ON oi_300.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_prev ON TRUE
            ORDER BY s.sample_second_ms ASC
            """,
            market_id,
            fill_display,
            max(0, max_carry_forward_ms),
        )

    return build_market_download_payload(
        [dict(row) for row in rows],
        server_time_ms=server_time_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
        include_flow=include_flow,
        include_book=include_book,
    )


async def fetch_current_live_payload(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    current_sample_second_ms: int,
    server_time_ms: int,
    max_chainlink_carry_forward_ms: int = 10_000,
) -> dict[str, Any]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            WITH binance AS (
                SELECT
                    ps.sample_second_ms AS binance_sample_second_ms,
                    ps.price AS binance_price,
                    ps.provider_event_ms AS binance_provider_event_ms,
                    ps.received_ms AS binance_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'binance_spot'
                  AND i.symbol = 'BTCUSDT'
                ORDER BY ps.sample_second_ms DESC
                LIMIT 1
            ),
            chainlink AS (
                SELECT
                    ps.sample_second_ms AS chainlink_sample_second_ms,
                    ps.price AS chainlink_price,
                    ps.provider_event_ms AS chainlink_provider_event_ms,
                    ps.provider_message_ms AS chainlink_provider_message_ms,
                    ps.received_ms AS chainlink_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'polymarket_chainlink_rtds'
                  AND i.symbol = 'BTCUSD'
                  AND ps.sample_second_ms <= $2
                  AND ps.sample_second_ms >= $2 - $3::BIGINT
                ORDER BY ps.sample_second_ms DESC
                LIMIT 1
            ),
            futures_price AS (
                SELECT
                    sample_second_ms AS futures_sample_second_ms,
                    futures_last_price,
                    futures_last_price_time_ms,
                    mark_price,
                    index_price,
                    premium_index_time_ms,
                    received_ms AS futures_received_ms
                FROM binance_futures_snapshots
                WHERE market_id = $1
                  AND symbol = 'BTCUSDT'
                  AND (
                    futures_last_price IS NOT NULL
                    OR mark_price IS NOT NULL
                    OR index_price IS NOT NULL
                  )
                ORDER BY COALESCE(
                    futures_last_price_time_ms,
                    premium_index_time_ms,
                    sample_second_ms
                ) DESC
                LIMIT 1
            ),
            oi AS (
                SELECT
                    sample_second_ms AS oi_sample_second_ms,
                    open_interest,
                    open_interest_time_ms,
                    received_ms AS oi_received_ms
                FROM binance_futures_snapshots
                WHERE market_id = $1
                  AND symbol = 'BTCUSDT'
                  AND open_interest IS NOT NULL
                ORDER BY COALESCE(open_interest_time_ms, sample_second_ms) DESC
                LIMIT 1
            )
            SELECT
                binance.binance_sample_second_ms,
                binance.binance_price,
                binance.binance_provider_event_ms,
                binance.binance_received_ms,

                chainlink.chainlink_sample_second_ms,
                chainlink.chainlink_price,
                chainlink.chainlink_provider_event_ms,
                chainlink.chainlink_provider_message_ms,
                chainlink.chainlink_received_ms,

                futures_price.futures_sample_second_ms,
                futures_price.futures_last_price,
                futures_price.futures_last_price_time_ms,
                futures_price.mark_price,
                futures_price.index_price,
                futures_price.premium_index_time_ms,
                futures_price.futures_received_ms,

                oi.oi_sample_second_ms,
                oi.open_interest,
                oi.open_interest_time_ms,
                oi.oi_received_ms
            FROM (SELECT 1) seed
            LEFT JOIN binance ON TRUE
            LEFT JOIN chainlink ON TRUE
            LEFT JOIN futures_price ON TRUE
            LEFT JOIN oi ON TRUE
            """,
            window.market_id,
            current_sample_second_ms,
            max(0, max_chainlink_carry_forward_ms),
        )

    data = dict(row) if row is not None else {}

    binance_source_ms = data.get("binance_provider_event_ms")
    binance_received_ms = data.get("binance_received_ms")
    chainlink_source_ms = data.get("chainlink_provider_event_ms")
    chainlink_received_ms = data.get("chainlink_received_ms")
    futures_last_time_ms = data.get("futures_last_price_time_ms")
    premium_index_time_ms = data.get("premium_index_time_ms")
    futures_received_ms = data.get("futures_received_ms")
    open_interest_time_ms = data.get("open_interest_time_ms")
    oi_received_ms = data.get("oi_received_ms")

    return {
        "server_time_ms": server_time_ms,
        "market_id": window.market_id,
        "market_start_ms": window.market_start_ms,
        "market_end_ms": window.market_end_ms,
        "prices": {
            "binance_spot": {
                "value": decimal_2dp_or_none(data.get("binance_price")),
                "sample_second_ms": data.get("binance_sample_second_ms"),
                "provider_event_ms": binance_source_ms,
                "received_ms": binance_received_ms,
                **freshness_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=binance_source_ms,
                    received_ms=binance_received_ms,
                ),
            },
            "chainlink": {
                "value": decimal_2dp_or_none(data.get("chainlink_price")),
                "sample_second_ms": data.get("chainlink_sample_second_ms"),
                "provider_event_ms": chainlink_source_ms,
                "provider_message_ms": data.get("chainlink_provider_message_ms"),
                "received_ms": chainlink_received_ms,
                "is_carried_forward_for_display": (
                    data.get("chainlink_sample_second_ms") is not None
                    and int(data["chainlink_sample_second_ms"]) < current_sample_second_ms
                ),
                **freshness_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=chainlink_source_ms,
                    received_ms=chainlink_received_ms,
                ),
            },
        },
        "futures": {
            "last": {
                "value": money_2dp(data.get("futures_last_price")),
                "time_ms": futures_last_time_ms,
                "received_ms": futures_received_ms,
                **freshness_age_only_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=futures_last_time_ms,
                    received_ms=futures_received_ms,
                ),
            },
            "mark": {
                "value": money_2dp(data.get("mark_price")),
                "time_ms": premium_index_time_ms,
                "received_ms": futures_received_ms,
                **freshness_age_only_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=premium_index_time_ms,
                    received_ms=futures_received_ms,
                ),
            },
            "index": {
                "value": money_2dp(data.get("index_price")),
                "time_ms": premium_index_time_ms,
                "received_ms": futures_received_ms,
                **freshness_age_only_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=premium_index_time_ms,
                    received_ms=futures_received_ms,
                ),
            },
        },
        "open_interest": {
            "contracts": oi_3dp(data.get("open_interest")),
            "time_ms": open_interest_time_ms,
            "received_ms": oi_received_ms,
            **freshness_age_only_meta(
                server_time_ms=server_time_ms,
                source_time_ms=open_interest_time_ms,
                received_ms=oi_received_ms,
            ),
        },
    }
