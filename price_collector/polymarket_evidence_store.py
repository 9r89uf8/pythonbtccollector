"""Compact, append-only evidence for causal Polymarket observations.

The metadata and sampled-quote writers are independent of each other and of
the collector's socket reader.  Bounded queues never evict accepted records.
When new records cannot be accepted, bounded loss summaries make that coverage
gap explicit.  There is deliberately no automatic deletion of study evidence.
An in-memory queue is not a crash-safe spool: unfinished durable sessions are
recovered as unclean gaps on the next start.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import logging
import time
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence
from uuid import UUID, uuid4, uuid5

from price_collector.market import MARKET_MS, market_for_sample_second


LOGGER = logging.getLogger("price_collector.polymarket_evidence_store")
EVIDENCE_KINDS = frozenset({
    "price_to_beat", "gamma_market", "clob_market", "clob_order_rules", "tick_size",
    "session_start", "session_end", "gap",
})
EVIDENCE_STATUSES = frozenset({
    "ok", "missing", "invalid", "http_error", "transport_error",
})
CONTROL_KINDS = frozenset({"session_start", "session_end", "gap"})
SQL_TIMEOUT_SECONDS = 5.0
SIZE_CHECK_SECONDS = 60.0
LOSS_FLUSH_SECONDS = 60.0
MAX_LOSS_SCOPES = 64
CONTROL_QUEUE_MAX = 64
MAX_BIGINT = 9_223_372_036_854_775_807
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or value < minimum or value > MAX_BIGINT):
        raise ValueError(f"{name} must be an integer between {minimum} and {MAX_BIGINT}")
    return value


def _optional_integer(value: Any, name: str, *, minimum: int = 0) -> None:
    if value is not None:
        _integer(value, name, minimum=minimum)


def _plain_payload(value: Any) -> Any:
    """Copy JSON-compatible data without a binary-float financial boundary."""
    if isinstance(value, float):
        raise TypeError("evidence payloads must not contain floats")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("evidence Decimal values must be finite")
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("evidence payload keys must be strings")
        return {key: _plain_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_payload(item) for item in value]
    raise TypeError(f"unsupported evidence payload type: {type(value).__name__}")


def _freeze_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_payload(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_payload(item) for item in value)
    return value


@dataclass(frozen=True)
class EvidenceRecord:
    record_id: UUID
    market_id: int
    kind: str
    received_wall_ns: int
    received_monotonic_ns: int
    payload: Mapping[str, Any]
    requested_wall_ns: Optional[int] = None
    requested_monotonic_ns: Optional[int] = None
    http_status: Optional[int] = None
    status: str = "ok"
    connection_id: Optional[UUID] = None
    provider_event_ms: Optional[int] = None
    response_sha256: Optional[str] = None
    response_date: Optional[str] = None
    response_age_seconds: Optional[int] = None
    payload_json: str = field(init=False, repr=False, compare=False)
    payload_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.record_id, UUID):
            raise TypeError("record_id must be a UUID")
        _integer(self.market_id, "market_id")
        _integer(self.received_wall_ns, "received_wall_ns", minimum=1)
        _integer(self.received_monotonic_ns, "received_monotonic_ns", minimum=1)
        if self.kind not in EVIDENCE_KINDS or self.status not in EVIDENCE_STATUSES:
            raise ValueError("invalid evidence kind or status")
        if self.connection_id is not None and not isinstance(self.connection_id, UUID):
            raise TypeError("connection_id must be a UUID")
        if self.kind in {"session_start", "session_end"} and self.connection_id is None:
            raise ValueError("session records require a connection_id")
        if (self.requested_wall_ns is None) != (self.requested_monotonic_ns is None):
            raise ValueError("request clocks must be supplied together")
        _optional_integer(self.requested_wall_ns, "requested_wall_ns", minimum=1)
        _optional_integer(self.requested_monotonic_ns, "requested_monotonic_ns", minimum=1)
        if (self.requested_monotonic_ns is not None
                and self.requested_monotonic_ns > self.received_monotonic_ns):
            raise ValueError("response monotonic clock precedes request")
        _optional_integer(self.http_status, "http_status", minimum=100)
        if self.http_status is not None and self.http_status > 599:
            raise ValueError("http_status must be between 100 and 599")
        _optional_integer(self.provider_event_ms, "provider_event_ms")
        if self.response_sha256 is not None and (
            not isinstance(self.response_sha256, str) or len(self.response_sha256) != 64
            or any(char not in "0123456789abcdef" for char in self.response_sha256)
        ):
            raise ValueError("response_sha256 must be a lowercase SHA256 hex digest")
        if self.response_date is not None and not isinstance(self.response_date, str):
            raise TypeError("response_date must be the response header string or None")
        _optional_integer(self.response_age_seconds, "response_age_seconds")
        if not isinstance(self.payload, Mapping):
            raise TypeError("evidence payload must be a mapping")
        plain = _plain_payload(self.payload)
        encoded = json.dumps(plain, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False)
        object.__setattr__(self, "payload", _freeze_payload(plain))
        object.__setattr__(self, "payload_json", encoded)
        object.__setattr__(self, "payload_hash", hashlib.sha256(encoded.encode("utf-8")).hexdigest())


@dataclass(frozen=True)
class QuoteObservation:
    connection_id: UUID
    market_id: int
    observed_wall_ns: int
    observed_monotonic_ns: int
    receive_sequence: int
    received_wall_ns: Optional[int]
    received_monotonic_ns: Optional[int]
    up_bid: Optional[Decimal]
    up_ask: Optional[Decimal]
    down_bid: Optional[Decimal]
    down_ask: Optional[Decimal]
    up_bid_provider_event_ms: Optional[int]
    up_ask_provider_event_ms: Optional[int]
    down_bid_provider_event_ms: Optional[int]
    down_ask_provider_event_ms: Optional[int]
    up_bid_received_ms: Optional[int]
    up_ask_received_ms: Optional[int]
    down_bid_received_ms: Optional[int]
    down_ask_received_ms: Optional[int]
    event_type: Optional[str]
    resolved: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.connection_id, UUID):
            raise TypeError("connection_id must be a UUID")
        for name in ("market_id", "receive_sequence"):
            _integer(getattr(self, name), name)
        for name in ("observed_wall_ns", "observed_monotonic_ns"):
            _integer(getattr(self, name), name, minimum=1)
        if (self.received_wall_ns is None) != (self.received_monotonic_ns is None):
            raise ValueError("quote receipt clocks must be supplied together")
        _optional_integer(self.received_wall_ns, "received_wall_ns", minimum=1)
        _optional_integer(self.received_monotonic_ns, "received_monotonic_ns", minimum=1)
        if (self.received_monotonic_ns is not None
                and self.received_monotonic_ns > self.observed_monotonic_ns):
            raise ValueError("quote cannot be observed before its monotonic receipt")
        for name in ("up_bid", "up_ask", "down_bid", "down_ask"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite()
                                      or not Decimal("0") <= value <= Decimal("1")):
                raise ValueError(f"{name} must be a finite Decimal between 0 and 1")
            if value is not None and value != value.quantize(Decimal("0.000000000000000001")):
                raise ValueError(f"{name} has nonzero precision beyond 18 decimal places")
        for name in QUOTE_COMPONENT_CLOCKS:
            _optional_integer(getattr(self, name), name)
        if self.event_type is not None and not isinstance(self.event_type, str):
            raise TypeError("event_type must be a string or None")
        if not isinstance(self.resolved, bool):
            raise TypeError("resolved must be a bool")


QUOTE_COMPONENT_CLOCKS = (
    "up_bid_provider_event_ms", "up_ask_provider_event_ms",
    "down_bid_provider_event_ms", "down_ask_provider_event_ms",
    "up_bid_received_ms", "up_ask_received_ms", "down_bid_received_ms", "down_ask_received_ms",
)
QUOTE_COLUMNS = (
    "connection_id", "market_id", "observed_wall_ns", "observed_monotonic_ns",
    "receive_sequence", "received_wall_ns", "received_monotonic_ns",
    "up_bid", "up_ask", "down_bid", "down_ask",
) + QUOTE_COMPONENT_CLOCKS + ("event_type", "resolved")

# SQL identifiers are static: input values are always query parameters.
_MARKET_SQL = """
INSERT INTO market_windows (
    market_id, market_start_ms, market_end_ms, market_start_at, market_end_at
) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (market_id) DO NOTHING
"""
_PAYLOAD_SQL = """
INSERT INTO polymarket_evidence_payloads (payload_hash, payload)
VALUES ($1, $2::jsonb) ON CONFLICT (payload_hash) DO NOTHING
"""
_OBSERVATION_SQL = """
INSERT INTO polymarket_market_observations (
    record_id, market_id, kind, received_wall_ns, received_monotonic_ns,
    requested_wall_ns, requested_monotonic_ns, http_status, status,
    connection_id, provider_event_ms, payload_hash,
    response_sha256, response_date, response_age_seconds
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT DO NOTHING
"""
_QUOTE_SQL = """
INSERT INTO polymarket_quote_observations (
    connection_id, market_id, observed_wall_ns, observed_monotonic_ns,
    receive_sequence, received_wall_ns, received_monotonic_ns,
    up_bid, up_ask, down_bid, down_ask,
    up_bid_provider_event_ms, up_ask_provider_event_ms,
    down_bid_provider_event_ms, down_ask_provider_event_ms,
    up_bid_received_ms, up_ask_received_ms, down_bid_received_ms, down_ask_received_ms,
    event_type, resolved
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
          $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
ON CONFLICT (connection_id, observed_wall_ns) DO NOTHING
"""
_SIZE_SQL = """
SELECT pg_total_relation_size('polymarket_evidence_payloads'::regclass)
     + pg_total_relation_size('polymarket_market_observations'::regclass)
     + pg_total_relation_size('polymarket_quote_observations'::regclass)
"""
_RECOVERY_SQL = """
SELECT opened.connection_id, opened.market_id,
       opened.received_wall_ns AS session_start_wall_ns,
       latest.observed_wall_ns AS last_durable_observed_wall_ns,
       latest.received_wall_ns AS last_durable_received_wall_ns,
       latest.receive_sequence AS last_durable_receive_sequence
FROM polymarket_market_observations opened
LEFT JOIN LATERAL (
    SELECT observed_wall_ns, received_wall_ns, receive_sequence
    FROM polymarket_quote_observations quote
    WHERE quote.connection_id = opened.connection_id
    ORDER BY observed_wall_ns DESC LIMIT 1
) latest ON TRUE
WHERE opened.kind = 'session_start'
  AND NOT EXISTS (
    SELECT 1 FROM polymarket_market_observations ended
    WHERE ended.connection_id = opened.connection_id AND ended.kind = 'session_end'
  )
ORDER BY opened.received_wall_ns
LIMIT 128
"""
_ORPHAN_SCAN_RESULT_SQL = """
SELECT candidate.connection_id, latest.market_id,
       NULL::bigint AS session_start_wall_ns,
       latest.observed_wall_ns AS last_durable_observed_wall_ns,
       latest.received_wall_ns AS last_durable_received_wall_ns,
       latest.receive_sequence AS last_durable_receive_sequence,
       EXISTS (
           SELECT 1 FROM polymarket_market_observations session
           WHERE session.connection_id = candidate.connection_id
             AND session.kind IN ('session_start', 'session_end')
       ) AS session_recorded
FROM candidates candidate
JOIN LATERAL (
    SELECT market_id, observed_wall_ns, received_wall_ns, receive_sequence
    FROM polymarket_quote_observations quote
    WHERE quote.connection_id = candidate.connection_id
    ORDER BY observed_wall_ns DESC LIMIT 1
) latest ON TRUE
ORDER BY candidate.connection_id
"""
_ORPHAN_SCAN_FIRST_SQL = """
WITH candidates AS (
    SELECT DISTINCT connection_id FROM polymarket_quote_observations
    ORDER BY connection_id LIMIT 128
)
""" + _ORPHAN_SCAN_RESULT_SQL
_ORPHAN_SCAN_NEXT_SQL = """
WITH candidates AS (
    SELECT DISTINCT connection_id FROM polymarket_quote_observations
    WHERE connection_id > $1::uuid
    ORDER BY connection_id LIMIT 128
)
""" + _ORPHAN_SCAN_RESULT_SQL


def _market_arguments(market_ids: Sequence[int]) -> list[tuple[Any, ...]]:
    arguments = []
    for market_id in sorted(set(market_ids)):
        window = market_for_sample_second(market_id * MARKET_MS)
        arguments.append((
            window.market_id, window.market_start_ms, window.market_end_ms,
            _EPOCH + timedelta(milliseconds=window.market_start_ms),
            _EPOCH + timedelta(milliseconds=window.market_end_ms),
        ))
    return arguments


@dataclass
class _LossRange:
    market_id: int
    connection_id: Optional[UUID]
    channel: str
    reason: str
    first_wall_ns: int
    last_wall_ns: int
    first_monotonic_ns: int
    last_monotonic_ns: int
    count: int = 1
    first_sequence: Optional[int] = None
    last_sequence: Optional[int] = None
    first_market_id: Optional[int] = None
    last_market_id: Optional[int] = None
    scope_overflow: bool = False
    record_id: UUID = field(default_factory=uuid4)

    def extend(self, *, market_id: int, wall_ns: int, monotonic_ns: int,
               sequence: Optional[int]) -> None:
        self.last_wall_ns = wall_ns
        self.last_monotonic_ns = monotonic_ns
        self.last_sequence = sequence
        self.last_market_id = market_id
        self.count += 1

    def as_record(self) -> EvidenceRecord:
        return EvidenceRecord(
            record_id=self.record_id, market_id=self.market_id, kind="gap", status="missing",
            received_wall_ns=time.time_ns(), received_monotonic_ns=time.monotonic_ns(),
            connection_id=self.connection_id,
            payload={
                "reason": self.reason, "channel": self.channel,
                "lost_observations": self.count,
                "first_lost_wall_ns": self.first_wall_ns,
                "last_lost_wall_ns": self.last_wall_ns,
                "first_lost_monotonic_ns": self.first_monotonic_ns,
                "last_lost_monotonic_ns": self.last_monotonic_ns,
                "first_lost_receive_sequence": self.first_sequence,
                "last_lost_receive_sequence": self.last_sequence,
                "first_market_id": self.first_market_id,
                "last_market_id": self.last_market_id,
                "scope_overflow": self.scope_overflow,
                "scope_note": ("multiple market/connection scopes; market_id anchors this summary"
                               if self.scope_overflow else "single market and connection"),
                "replay_available": False,
            },
        )


class EvidenceWriter:
    """Bounded independent queues, idempotent persistence, and explicit losses."""

    def __init__(self, pool: Any, queue_max: int = 5000, quote_queue_max: int = 2000,
                 batch_size: int = 250, flush_seconds: float = 0.5,
                 max_relation_mb: int = 6144, warn_relation_mb: int = 4096) -> None:
        for name, value in (("queue_max", queue_max), ("quote_queue_max", quote_queue_max),
                            ("batch_size", batch_size), ("max_relation_mb", max_relation_mb),
                            ("warn_relation_mb", warn_relation_mb)):
            _integer(value, name, minimum=1)
        if not 0 < flush_seconds <= 60:
            raise ValueError("flush_seconds must be between 0 and 60")
        if warn_relation_mb > max_relation_mb:
            raise ValueError("warn_relation_mb must not exceed max_relation_mb")
        self.pool = pool
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.max_relation_bytes = max_relation_mb * 1024 * 1024
        self.warn_relation_bytes = warn_relation_mb * 1024 * 1024
        self.relation_bytes: Optional[int] = None
        self.quote_writes_paused = True
        self._pause_reason = "relation_size_unchecked"
        self._metadata_queue: asyncio.Queue[EvidenceRecord] = asyncio.Queue(maxsize=queue_max)
        self._control_queue: asyncio.Queue[EvidenceRecord] = asyncio.Queue(maxsize=CONTROL_QUEUE_MAX)
        self._quote_queue: asyncio.Queue[QuoteObservation] = asyncio.Queue(maxsize=quote_queue_max)
        self._losses: OrderedDict[tuple[Any, ...], _LossRange] = OrderedDict()
        self._overflow_loss: Optional[_LossRange] = None
        self._next_loss_flush_at = 0.0
        self._deferred_ends: dict[UUID, EvidenceRecord] = {}
        self._pending_quotes: dict[UUID, int] = {}
        self._pending_metadata: dict[UUID, int] = {}
        self._live_connections: OrderedDict[UUID, None] = OrderedDict()
        self._metadata_wake = asyncio.Event()
        self._quote_wake = asyncio.Event()
        self._closing_event = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._started = False
        self._closed = False
        self._inflight_metadata = 0
        self._inflight_quotes = 0

    async def start(self) -> EvidenceWriter:
        if self._closed:
            raise RuntimeError("an evidence writer cannot restart after close")
        if not self._started:
            self._started = True
            self._tasks = [
                asyncio.create_task(self._metadata_worker(), name="polymarket-evidence-metadata"),
                asyncio.create_task(self._quote_worker(), name="polymarket-evidence-quotes"),
                asyncio.create_task(self._size_worker(), name="polymarket-evidence-size"),
            ]
        return self

    def offer(self, record: EvidenceRecord) -> bool:
        if not isinstance(record, EvidenceRecord):
            raise TypeError("offer requires an EvidenceRecord")
        if self._closed or self._closing_event.is_set():
            return False
        self._track_live_connection(record.connection_id)
        queue = self._control_queue if record.kind in CONTROL_KINDS else self._metadata_queue
        try:
            queue.put_nowait(record)
        except asyncio.QueueFull:
            self._record_loss(record.market_id, record.connection_id, "metadata", "queue_full",
                              record.received_wall_ns, record.received_monotonic_ns)
            return False
        if record.connection_id is not None and record.kind not in CONTROL_KINDS:
            self._pending_metadata[record.connection_id] = self._pending_metadata.get(record.connection_id, 0) + 1
        self._metadata_wake.set()
        return True

    def offer_quote(self, quote: QuoteObservation) -> bool:
        if not isinstance(quote, QuoteObservation):
            raise TypeError("offer_quote requires a QuoteObservation")
        if self._closed or self._closing_event.is_set():
            return False
        self._track_live_connection(quote.connection_id)
        if self.quote_writes_paused:
            self._record_loss(quote.market_id, quote.connection_id, "quotes", self._pause_reason,
                              quote.observed_wall_ns, quote.observed_monotonic_ns, quote.receive_sequence)
            return False
        try:
            self._quote_queue.put_nowait(quote)
        except asyncio.QueueFull:
            self._record_loss(quote.market_id, quote.connection_id, "quotes", "queue_full",
                              quote.observed_wall_ns, quote.observed_monotonic_ns, quote.receive_sequence)
            return False
        self._pending_quotes[quote.connection_id] = self._pending_quotes.get(quote.connection_id, 0) + 1
        self._quote_wake.set()
        return True

    def _track_live_connection(self, connection_id: Optional[UUID]) -> None:
        if connection_id is None:
            return
        self._live_connections[connection_id] = None
        self._live_connections.move_to_end(connection_id)
        if len(self._live_connections) > CONTROL_QUEUE_MAX:
            self._live_connections.popitem(last=False)

    def _connection_pending(self, connection_id: Optional[UUID]) -> bool:
        return bool(self._pending_quotes.get(connection_id) or self._pending_metadata.get(connection_id))

    def _record_loss(self, market_id: int, connection_id: Optional[UUID], channel: str,
                     reason: str, wall_ns: int, monotonic_ns: int,
                     sequence: Optional[int] = None) -> None:
        key = (market_id, connection_id, channel, reason)
        existing = self._losses.get(key)
        if existing is not None:
            existing.extend(market_id=market_id, wall_ns=wall_ns, monotonic_ns=monotonic_ns, sequence=sequence)
        elif len(self._losses) < MAX_LOSS_SCOPES:
            self._losses[key] = _LossRange(
                market_id, connection_id, channel, reason, wall_ns, wall_ns, monotonic_ns, monotonic_ns,
                first_sequence=sequence, last_sequence=sequence,
                first_market_id=market_id, last_market_id=market_id,
            )
            LOGGER.warning("polymarket_evidence_capture_gap", extra={
                "event": "polymarket_evidence_capture_gap", "market_id": market_id,
                "connection_id": str(connection_id), "channel": channel, "reason": reason,
            })
        elif self._overflow_loss is None:
            self._overflow_loss = _LossRange(
                market_id, None, "multiple", "loss_scope_capacity_exceeded",
                wall_ns, wall_ns, monotonic_ns, monotonic_ns,
                first_market_id=market_id, last_market_id=market_id, scope_overflow=True,
            )
        else:
            self._overflow_loss.extend(market_id=market_id, wall_ns=wall_ns,
                                       monotonic_ns=monotonic_ns, sequence=None)
        self._metadata_wake.set()

    async def _transaction(self, operation: Any) -> Any:
        async def run() -> Any:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout = '4000ms'")
                    return await operation(connection)
        return await asyncio.wait_for(run(), timeout=SQL_TIMEOUT_SECONDS)

    async def _persist_metadata(self, records: Sequence[EvidenceRecord]) -> None:
        if not records:
            return
        async def write(connection: Any) -> None:
            await connection.executemany(_MARKET_SQL, _market_arguments([row.market_id for row in records]))
            payloads = {row.payload_hash: row.payload_json for row in records}
            await connection.executemany(_PAYLOAD_SQL, list(payloads.items()))
            await connection.executemany(_OBSERVATION_SQL, [(
                row.record_id, row.market_id, row.kind, row.received_wall_ns, row.received_monotonic_ns,
                row.requested_wall_ns, row.requested_monotonic_ns, row.http_status, row.status,
                row.connection_id, row.provider_event_ms, row.payload_hash,
                row.response_sha256, row.response_date, row.response_age_seconds,
            ) for row in records])
        await self._transaction(write)

    async def _persist_quotes(self, records: Sequence[QuoteObservation]) -> None:
        if not records:
            return
        async def write(connection: Any) -> None:
            await connection.executemany(_MARKET_SQL, _market_arguments([row.market_id for row in records]))
            await connection.executemany(_QUOTE_SQL, [tuple(getattr(row, name) for name in QUOTE_COLUMNS)
                                                       for row in records])
        await self._transaction(write)

    async def _retry(self, operation: Any, *, channel: str) -> Any:
        attempt = 0
        while True:
            try:
                return await operation()
            except asyncio.CancelledError:
                raise
            except Exception:
                attempt += 1
                LOGGER.exception("polymarket_evidence_persistence_retry", extra={
                    "event": "polymarket_evidence_persistence_retry", "channel": channel,
                    "attempt": attempt,
                })
                await asyncio.sleep(min(0.25 * (2 ** min(attempt - 1, 6)), 10.0))

    async def _recover_sessions(self) -> None:
        while True:
            async def fetch(connection: Any) -> Any:
                return await connection.fetch(_RECOVERY_SQL)
            rows = await self._transaction(fetch)
            if not rows:
                break
            await self._finish_recovered_sessions(rows, missing_start_record=False)
        # Quote inserts deliberately do not wait for session metadata. Recover
        # orphan connections too, paging the existing connection/time PK so a
        # growing history does not require one unbounded aggregate query.
        cursor: Optional[UUID] = None
        while True:
            async def scan(connection: Any) -> Any:
                if cursor is None:
                    return await connection.fetch(_ORPHAN_SCAN_FIRST_SQL)
                return await connection.fetch(_ORPHAN_SCAN_NEXT_SQL, cursor)
            rows = await self._transaction(scan)
            if not rows:
                return
            orphaned = [row for row in rows if not row["session_recorded"]
                        and row["connection_id"] not in self._live_connections]
            if orphaned:
                await self._finish_recovered_sessions(orphaned, missing_start_record=True)
            cursor = rows[-1]["connection_id"]

    async def _finish_recovered_sessions(self, rows: Sequence[Mapping[str, Any]],
                                        *, missing_start_record: bool) -> None:
        recovered = []
        detected_wall_ns = time.time_ns()
        detected_monotonic_ns = time.monotonic_ns()
        for row in rows:
            connection_id = row["connection_id"]
            payload = {
                "reason": "unclean_shutdown", "disconnect_time_known": False,
                "missing_start_record": missing_start_record,
                "detected_wall_ns": detected_wall_ns,
                "session_start_wall_ns": row["session_start_wall_ns"],
                "last_durable_observed_wall_ns": row["last_durable_observed_wall_ns"],
                "last_durable_received_wall_ns": row["last_durable_received_wall_ns"],
                "last_durable_receive_sequence": row["last_durable_receive_sequence"],
                "replay_available": False,
            }
            for kind in ("gap", "session_end"):
                recovered.append(EvidenceRecord(
                    record_id=uuid5(connection_id, f"polymarket-evidence-unclean-{kind}"),
                    market_id=row["market_id"], kind=kind, status="missing",
                    received_wall_ns=detected_wall_ns, received_monotonic_ns=detected_monotonic_ns,
                    connection_id=connection_id, payload=payload,
                ))
        await self._persist_metadata(recovered)
        LOGGER.warning("polymarket_evidence_unclean_sessions_recovered", extra={
            "event": "polymarket_evidence_unclean_sessions_recovered", "session_count": len(rows),
            "missing_start_record": missing_start_record,
        })

    async def _wait_for_records(self, event: asyncio.Event) -> None:
        if not self._closing_event.is_set():
            await event.wait()
            try:
                await asyncio.wait_for(self._closing_event.wait(), timeout=self.flush_seconds)
            except asyncio.TimeoutError:
                pass
        event.clear()

    def _metadata_batch(self) -> tuple[list[EvidenceRecord], list[asyncio.Queue[EvidenceRecord]]]:
        records: list[EvidenceRecord] = []
        completed_queues: list[asyncio.Queue[EvidenceRecord]] = []
        for connection_id, record in list(self._deferred_ends.items()):
            if not self._connection_pending(connection_id):
                records.append(record)
                del self._deferred_ends[connection_id]
                if len(records) >= self.batch_size:
                    break
        # Reserve at least half of a batch for ordinary metadata under sustained gaps.
        control_limit = max(1, self.batch_size // 2)
        while len(records) < control_limit:
            try:
                record = self._control_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if record.kind == "session_end" and self._connection_pending(record.connection_id):
                self._deferred_ends.setdefault(record.connection_id, record)
                self._control_queue.task_done()
                continue
            records.append(record)
            completed_queues.append(self._control_queue)
        while len(records) < self.batch_size:
            try:
                records.append(self._metadata_queue.get_nowait())
                completed_queues.append(self._metadata_queue)
            except asyncio.QueueEmpty:
                break
        # A size pause must not itself create a high-rate stream of gap rows.
        # Flush aggregated loss clocks at most once a minute, on shutdown, or
        # atomically with the affected session end. A first loss is immediate.
        ending_connections = {record.connection_id for record in records if record.kind == "session_end"}
        loss_due = self._closing_event.is_set() or time.monotonic() >= self._next_loss_flush_at
        flushed_loss = False
        for key, loss in list(self._losses.items()):
            # Associated gaps and an end must commit atomically even if ordinary
            # rows filled the nominal batch. This adds at most MAX_LOSS_SCOPES
            # records; it cannot grow with an unbounded number of connections.
            closing_scope = loss.connection_id in ending_connections
            if closing_scope or (loss_due and len(records) < self.batch_size):
                records.append(loss.as_record())
                del self._losses[key]
                flushed_loss = True
        if (self._overflow_loss is not None
                and (ending_connections or (loss_due and len(records) < self.batch_size))):
            records.append(self._overflow_loss.as_record())
            self._overflow_loss = None
            flushed_loss = True
        if flushed_loss:
            self._next_loss_flush_at = time.monotonic() + LOSS_FLUSH_SECONDS
        return records, completed_queues

    def _metadata_pending(self) -> bool:
        return bool(not self._metadata_queue.empty() or not self._control_queue.empty()
                    or self._losses or self._overflow_loss or self._deferred_ends)

    async def _metadata_worker(self) -> None:
        await self._retry(self._recover_sessions, channel="session_recovery")
        while True:
            if self._closing_event.is_set() and not self._metadata_pending():
                return
            # A deferred end waits for its quote worker without blocking other metadata.
            if self._metadata_pending():
                self._metadata_wake.set()
            await self._wait_for_records(self._metadata_wake)
            records, completed_queues = self._metadata_batch()
            if not records:
                if self._deferred_ends:
                    await asyncio.sleep(min(self.flush_seconds, 0.05))
                continue
            self._inflight_metadata = len(records)
            await self._retry(lambda: self._persist_metadata(records), channel="metadata")
            self._inflight_metadata = 0
            for record in records:
                if record.connection_id is not None and record.kind not in CONTROL_KINDS:
                    remaining = self._pending_metadata[record.connection_id] - 1
                    if remaining:
                        self._pending_metadata[record.connection_id] = remaining
                    else:
                        del self._pending_metadata[record.connection_id]
            for queue in completed_queues:
                queue.task_done()

    async def _quote_worker(self) -> None:
        while True:
            if self._closing_event.is_set() and self._quote_queue.empty():
                return
            if not self._quote_queue.empty():
                self._quote_wake.set()
            await self._wait_for_records(self._quote_wake)
            records = []
            while len(records) < self.batch_size:
                try:
                    records.append(self._quote_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if not records:
                continue
            self._inflight_quotes = len(records)
            await self._retry(lambda: self._persist_quotes(records), channel="quotes")
            self._inflight_quotes = 0
            for record in records:
                remaining = self._pending_quotes[record.connection_id] - 1
                if remaining:
                    self._pending_quotes[record.connection_id] = remaining
                else:
                    del self._pending_quotes[record.connection_id]
                self._quote_queue.task_done()
            self._metadata_wake.set()

    async def _check_size(self) -> None:
        async def measure(connection: Any) -> Any:
            return await connection.fetchval(_SIZE_SQL)
        measured = await self._transaction(measure)
        if isinstance(measured, bool) or not isinstance(measured, int) or measured < 0:
            raise ValueError("evidence relation size was unavailable")
        previous_pause = self.quote_writes_paused
        previous_reason = self._pause_reason
        previous_bytes = self.relation_bytes
        self.relation_bytes = measured
        at_cap = measured >= self.max_relation_bytes
        hold_pause = previous_reason == "relation_size_cap" and measured >= self.warn_relation_bytes
        self.quote_writes_paused = at_cap or hold_pause
        self._pause_reason = "relation_size_cap" if self.quote_writes_paused else ""
        warning_crossed = measured >= self.warn_relation_bytes and (
            previous_bytes is None or previous_bytes < self.warn_relation_bytes)
        if previous_pause != self.quote_writes_paused or previous_reason != self._pause_reason or warning_crossed:
            LOGGER.log(logging.WARNING if measured >= self.warn_relation_bytes else logging.INFO,
                       "polymarket_evidence_relation_size", extra={
                           "event": "polymarket_evidence_relation_size", "relation_bytes": measured,
                           "quote_writes_paused": self.quote_writes_paused,
                           "max_relation_bytes": self.max_relation_bytes,
                       })

    async def _size_worker(self) -> None:
        while not self._closing_event.is_set():
            try:
                await self._check_size()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.quote_writes_paused = True
                self._pause_reason = "relation_size_check_failed"
                LOGGER.exception("polymarket_evidence_size_check_failed", extra={
                    "event": "polymarket_evidence_size_check_failed",
                })
            try:
                await asyncio.wait_for(self._closing_event.wait(), timeout=SIZE_CHECK_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def close(self, timeout_seconds: float = 10) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self._closed:
            return
        if not self._started:
            await self.start()
        self._closing_event.set()
        self._metadata_wake.set()
        self._quote_wake.set()
        try:
            _done, pending = await asyncio.wait(self._tasks, timeout=timeout_seconds)
            if pending:
                LOGGER.error("polymarket_evidence_shutdown_incomplete", extra={
                    "event": "polymarket_evidence_shutdown_incomplete",
                    "queued_metadata": self._metadata_queue.qsize(),
                    "queued_controls": self._control_queue.qsize(),
                    "queued_quotes": self._quote_queue.qsize(),
                    "inflight_metadata": self._inflight_metadata,
                    "inflight_quotes": self._inflight_quotes,
                    "pending_loss_scopes": len(self._losses) + int(self._overflow_loss is not None),
                    "deferred_session_ends": len(self._deferred_ends),
                })
        finally:
            for task in self._tasks:
                task.cancel()
            # wait_for(gather(...)) can exceed its timeout while a database
            # driver finishes cancellation. Bound cancellation separately too.
            done, still_running = await asyncio.wait(self._tasks, timeout=min(timeout_seconds, 0.1))
            for task in done:
                if not task.cancelled():
                    error = task.exception()
                    if error is not None:
                        LOGGER.error("polymarket_evidence_worker_stopped", extra={
                            "event": "polymarket_evidence_worker_stopped", "error": repr(error),
                        })
            if still_running:
                LOGGER.error("polymarket_evidence_shutdown_tasks_still_running", extra={
                    "event": "polymarket_evidence_shutdown_tasks_still_running",
                    "task_count": len(still_running),
                })
            self._closed = True
