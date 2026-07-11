from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from decimal import Decimal
from typing import (
    Any,
    Awaitable,
    Callable,
    Deque,
    Generic,
    List,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)
from uuid import UUID, uuid4


LOGGER = logging.getLogger("price_collector.raw_capture")

FUTURES_BUCKET_MS = 100
FUTURES_BUCKET_NS = FUTURES_BUCKET_MS * 1_000_000
POSTGRES_BIGINT_MAX = (2**63) - 1
POSTGRES_INTEGER_MAX = (2**31) - 1
POSTGRES_NUMERIC_38_18_LIMIT = Decimal("1e20")
SUSPENDED_MAINTENANCE_RETRY_SECONDS = 5.0
SEALED_CONNECTION_HISTORY_MAX = 1_024
FEED_SESSION_SOURCES = frozenset(
    {
        "binance_futures_agg_trade",
        "polymarket_chainlink_rtds",
    }
)
FEED_SESSION_CLOSE_REASONS = frozenset(
    {
        "remote_close",
        "error",
        "proactive_reconnect",
        "cancelled",
        "shutdown",
    }
)


def _require_uuid(value: Any, field_name: str) -> None:
    if not isinstance(value, UUID):
        raise TypeError(f"{field_name} must be UUID")


def _require_int(value: Any, field_name: str, *, positive: bool) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if positive and value <= 0:
        raise ValueError(f"{field_name} must be positive")
    if not positive and value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    if value > POSTGRES_BIGINT_MAX:
        raise ValueError(f"{field_name} exceeds PostgreSQL BIGINT")


def _require_optional_positive_int(value: Any, field_name: str) -> None:
    if value is not None:
        _require_int(value, field_name, positive=True)


def _require_price(value: Any, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    if value >= POSTGRES_NUMERIC_38_18_LIMIT:
        raise ValueError(f"{field_name} exceeds NUMERIC(38,18)")
    _sign, digits, exponent = value.as_tuple()
    if exponent < -18:
        extra_places = -18 - exponent
        if extra_places > len(digits) or any(digits[-extra_places:]):
            raise ValueError(f"{field_name} exceeds NUMERIC(38,18) scale")


def _require_positive_count(value: Any, field_name: str) -> None:
    _require_int(value, field_name, positive=True)


@dataclass(frozen=True)
class ReceiveStamp:
    __slots__ = (
        "connection_id",
        "receive_sequence",
        "received_wall_ns",
        "received_monotonic_ns",
    )

    connection_id: UUID
    receive_sequence: int
    received_wall_ns: int
    received_monotonic_ns: int

    def __post_init__(self) -> None:
        _require_uuid(self.connection_id, "connection_id")
        _require_int(self.receive_sequence, "receive_sequence", positive=True)
        _require_int(self.received_wall_ns, "received_wall_ns", positive=True)
        _require_int(
            self.received_monotonic_ns,
            "received_monotonic_ns",
            positive=True,
        )

    @property
    def received_ms(self) -> int:
        return self.received_wall_ns // 1_000_000


@dataclass(frozen=True)
class FuturesTradeObservation:
    __slots__ = (
        "connection_id",
        "received_wall_ns",
        "received_monotonic_ns",
        "trade_time_ms",
        "event_time_ms",
        "price",
        "agg_trade_id",
    )

    connection_id: UUID
    received_wall_ns: int
    received_monotonic_ns: int
    trade_time_ms: int
    event_time_ms: int
    price: Decimal
    agg_trade_id: int

    def __post_init__(self) -> None:
        _require_uuid(self.connection_id, "connection_id")
        _require_int(self.received_wall_ns, "received_wall_ns", positive=True)
        _require_int(
            self.received_monotonic_ns,
            "received_monotonic_ns",
            positive=True,
        )
        _require_int(self.trade_time_ms, "trade_time_ms", positive=True)
        _require_int(self.event_time_ms, "event_time_ms", positive=True)
        _require_price(self.price, "price")
        _require_int(self.agg_trade_id, "agg_trade_id", positive=False)


@dataclass(frozen=True)
class BinanceFuturesPriceTrace:
    __slots__ = (
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

    bucket_start_ms: int
    connection_id: UUID
    first_received_wall_ns: int
    last_received_wall_ns: int
    first_received_monotonic_ns: int
    last_received_monotonic_ns: int
    first_trade_time_ms: int
    last_trade_time_ms: int
    first_event_time_ms: int
    last_event_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    event_count: int
    first_agg_trade_id: int
    last_agg_trade_id: int

    def __post_init__(self) -> None:
        _require_int(self.bucket_start_ms, "bucket_start_ms", positive=True)
        if self.bucket_start_ms % FUTURES_BUCKET_MS != 0:
            raise ValueError("bucket_start_ms must be aligned to 100 ms")
        _require_uuid(self.connection_id, "connection_id")
        for field_name in (
            "first_received_wall_ns",
            "last_received_wall_ns",
            "first_received_monotonic_ns",
            "last_received_monotonic_ns",
            "first_trade_time_ms",
            "last_trade_time_ms",
            "first_event_time_ms",
            "last_event_time_ms",
        ):
            _require_int(getattr(self, field_name), field_name, positive=True)
        if self.last_received_monotonic_ns < self.first_received_monotonic_ns:
            raise ValueError("last received monotonic time precedes first")
        bucket_start_ns = self.bucket_start_ms * 1_000_000
        bucket_end_ns = bucket_start_ns + FUTURES_BUCKET_NS
        if not (
            bucket_start_ns <= self.first_received_wall_ns < bucket_end_ns
            and bucket_start_ns <= self.last_received_wall_ns < bucket_end_ns
        ):
            raise ValueError("received wall timestamps must fall inside trace bucket")
        for field_name in ("open_price", "high_price", "low_price", "close_price"):
            _require_price(getattr(self, field_name), field_name)
        if self.high_price < max(self.open_price, self.low_price, self.close_price):
            raise ValueError("high_price is inconsistent with OHLC values")
        if self.low_price > min(self.open_price, self.high_price, self.close_price):
            raise ValueError("low_price is inconsistent with OHLC values")
        _require_int(self.event_count, "event_count", positive=True)
        if self.event_count > POSTGRES_INTEGER_MAX:
            raise ValueError("event_count exceeds PostgreSQL INTEGER")
        _require_int(self.first_agg_trade_id, "first_agg_trade_id", positive=False)
        _require_int(self.last_agg_trade_id, "last_agg_trade_id", positive=False)


@dataclass(frozen=True)
class ChainlinkPriceEvent:
    __slots__ = (
        "received_wall_ns",
        "received_monotonic_ns",
        "connection_id",
        "receive_sequence",
        "provider_event_ms",
        "provider_message_ms",
        "price",
    )

    received_wall_ns: int
    received_monotonic_ns: int
    connection_id: UUID
    receive_sequence: int
    provider_event_ms: int
    provider_message_ms: Optional[int]
    price: Decimal

    def __post_init__(self) -> None:
        _require_int(self.received_wall_ns, "received_wall_ns", positive=True)
        _require_int(
            self.received_monotonic_ns,
            "received_monotonic_ns",
            positive=True,
        )
        _require_uuid(self.connection_id, "connection_id")
        _require_int(self.receive_sequence, "receive_sequence", positive=True)
        _require_int(self.provider_event_ms, "provider_event_ms", positive=True)
        _require_optional_positive_int(self.provider_message_ms, "provider_message_ms")
        _require_price(self.price, "price")


@dataclass(frozen=True)
class FeedSessionRecord:
    __slots__ = (
        "source",
        "connection_id",
        "connected_wall_ns",
        "connected_monotonic_ns",
        "ready_wall_ns",
        "ready_monotonic_ns",
        "disconnected_wall_ns",
        "disconnected_monotonic_ns",
        "close_reason",
        "messages_received_total",
        "messages_accepted_total",
        "parse_errors_total",
        "records_dropped_total",
        "last_receive_sequence",
    )

    source: str
    connection_id: UUID
    connected_wall_ns: int
    connected_monotonic_ns: int
    ready_wall_ns: Optional[int]
    ready_monotonic_ns: Optional[int]
    disconnected_wall_ns: Optional[int]
    disconnected_monotonic_ns: Optional[int]
    close_reason: Optional[str]
    messages_received_total: int
    messages_accepted_total: int
    parse_errors_total: int
    records_dropped_total: int
    last_receive_sequence: int

    def __post_init__(self) -> None:
        if self.source not in FEED_SESSION_SOURCES:
            raise ValueError("invalid feed session source")
        _require_uuid(self.connection_id, "connection_id")
        _require_int(self.connected_wall_ns, "connected_wall_ns", positive=True)
        _require_int(
            self.connected_monotonic_ns,
            "connected_monotonic_ns",
            positive=True,
        )
        ready_values = (self.ready_wall_ns, self.ready_monotonic_ns)
        if (ready_values[0] is None) != (ready_values[1] is None):
            raise ValueError("ready wall and monotonic timestamps must appear together")
        _require_optional_positive_int(self.ready_wall_ns, "ready_wall_ns")
        _require_optional_positive_int(self.ready_monotonic_ns, "ready_monotonic_ns")
        if (
            self.ready_monotonic_ns is not None
            and self.ready_monotonic_ns < self.connected_monotonic_ns
        ):
            raise ValueError("ready monotonic time precedes connection")

        disconnected_values = (
            self.disconnected_wall_ns,
            self.disconnected_monotonic_ns,
        )
        if (disconnected_values[0] is None) != (disconnected_values[1] is None):
            raise ValueError(
                "disconnected wall and monotonic timestamps must appear together"
            )
        _require_optional_positive_int(
            self.disconnected_wall_ns,
            "disconnected_wall_ns",
        )
        _require_optional_positive_int(
            self.disconnected_monotonic_ns,
            "disconnected_monotonic_ns",
        )
        if self.disconnected_monotonic_ns is None:
            if self.close_reason is not None:
                raise ValueError("an open feed session cannot have a close reason")
        else:
            if self.close_reason not in FEED_SESSION_CLOSE_REASONS:
                raise ValueError("invalid feed session close reason")
            if self.disconnected_monotonic_ns < self.connected_monotonic_ns:
                raise ValueError("disconnect monotonic time precedes connection")
            if (
                self.ready_monotonic_ns is not None
                and self.disconnected_monotonic_ns < self.ready_monotonic_ns
            ):
                raise ValueError("disconnect monotonic time precedes ready time")

        for field_name in (
            "messages_received_total",
            "messages_accepted_total",
            "parse_errors_total",
            "records_dropped_total",
            "last_receive_sequence",
        ):
            _require_int(getattr(self, field_name), field_name, positive=False)
        if self.messages_accepted_total + self.parse_errors_total > self.messages_received_total:
            raise ValueError("accepted plus parse-error messages exceed received messages")
        if self.last_receive_sequence != self.messages_received_total:
            raise ValueError("last receive sequence must equal received message count")


@dataclass(frozen=True)
class CaptureCounterSnapshot:
    __slots__ = (
        "messages_received_total",
        "messages_accepted_total",
        "parse_errors_total",
        "records_coalesced_total",
        "records_enqueued_total",
        "records_persisted_total",
        "records_dropped_total",
        "batches_failed_total",
        "maintenance_runs_total",
        "maintenance_failures_total",
        "capture_suspended",
        "queue_depth",
        "queue_high_water",
        "last_batch_rows",
        "last_batch_duration_ms",
        "current_partition",
        "raw_table_bytes",
        "connection_id",
    )

    messages_received_total: int
    messages_accepted_total: int
    parse_errors_total: int
    records_coalesced_total: int
    records_enqueued_total: int
    records_persisted_total: int
    records_dropped_total: int
    batches_failed_total: int
    maintenance_runs_total: int
    maintenance_failures_total: int
    capture_suspended: bool
    queue_depth: int
    queue_high_water: int
    last_batch_rows: int
    last_batch_duration_ms: float
    current_partition: Optional[str]
    raw_table_bytes: Optional[int]
    connection_id: Optional[UUID]

    def __post_init__(self) -> None:
        for field_name in (
            "messages_received_total",
            "messages_accepted_total",
            "parse_errors_total",
            "records_coalesced_total",
            "records_enqueued_total",
            "records_persisted_total",
            "records_dropped_total",
            "batches_failed_total",
            "maintenance_runs_total",
            "maintenance_failures_total",
            "queue_depth",
            "queue_high_water",
            "last_batch_rows",
        ):
            _require_int(getattr(self, field_name), field_name, positive=False)
        if self.queue_depth > self.queue_high_water:
            raise ValueError("queue depth exceeds queue high-water mark")
        if self.last_batch_duration_ms < 0:
            raise ValueError("last batch duration must be non-negative")
        if self.raw_table_bytes is not None:
            _require_int(self.raw_table_bytes, "raw_table_bytes", positive=False)
        if self.connection_id is not None:
            _require_uuid(self.connection_id, "connection_id")


class CaptureCounters:
    __slots__ = (
        "messages_received_total",
        "messages_accepted_total",
        "parse_errors_total",
        "records_coalesced_total",
        "records_enqueued_total",
        "records_persisted_total",
        "records_dropped_total",
        "batches_failed_total",
        "maintenance_runs_total",
        "maintenance_failures_total",
        "capture_suspended",
        "queue_high_water",
        "last_batch_rows",
        "last_batch_duration_ms",
        "current_partition",
        "raw_table_bytes",
    )

    def __init__(self) -> None:
        self.messages_received_total = 0
        self.messages_accepted_total = 0
        self.parse_errors_total = 0
        self.records_coalesced_total = 0
        self.records_enqueued_total = 0
        self.records_persisted_total = 0
        self.records_dropped_total = 0
        self.batches_failed_total = 0
        self.maintenance_runs_total = 0
        self.maintenance_failures_total = 0
        self.capture_suspended = False
        self.queue_high_water = 0
        self.last_batch_rows = 0
        self.last_batch_duration_ms = 0.0
        self.current_partition: Optional[str] = None
        self.raw_table_bytes: Optional[int] = None

    def message_received(self, count: int = 1) -> None:
        _require_positive_count(count, "count")
        self.messages_received_total += count

    def message_accepted(self, count: int = 1) -> None:
        _require_positive_count(count, "count")
        self.messages_accepted_total += count

    def parse_error(self, count: int = 1) -> None:
        _require_positive_count(count, "count")
        self.parse_errors_total += count

    def record_coalesced(self, count: int = 1) -> None:
        _require_positive_count(count, "count")
        self.records_coalesced_total += count

    def record_enqueued(self, count: int = 1) -> None:
        _require_positive_count(count, "count")
        self.records_enqueued_total += count

    def record_persisted(self, count: int) -> None:
        if count == 0:
            return
        _require_positive_count(count, "count")
        self.records_persisted_total += count

    def record_dropped(self, count: int = 1) -> None:
        if count == 0:
            return
        _require_positive_count(count, "count")
        self.records_dropped_total += count

    def observe_queue_depth(self, depth: int) -> None:
        _require_int(depth, "depth", positive=False)
        self.queue_high_water = max(self.queue_high_water, depth)

    def record_batch_result(
        self,
        *,
        rows: int,
        persisted: int,
        dropped: int,
        failed: bool,
        duration_ms: float,
    ) -> None:
        for value, field_name in (
            (rows, "rows"),
            (persisted, "persisted"),
            (dropped, "dropped"),
        ):
            _require_int(value, field_name, positive=False)
        if persisted + dropped != rows:
            raise ValueError("persisted plus dropped rows must equal batch rows")
        if duration_ms < 0:
            raise ValueError("batch duration must be non-negative")
        self.last_batch_rows = rows
        self.last_batch_duration_ms = duration_ms
        self.record_persisted(persisted)
        self.record_dropped(dropped)
        if failed:
            self.batches_failed_total += 1

    def record_maintenance(self, *, succeeded: bool, permitted: bool) -> None:
        self.maintenance_runs_total += 1
        if not succeeded:
            self.maintenance_failures_total += 1
        self.capture_suspended = not permitted

    def record_storage_state(
        self,
        *,
        current_partition: Optional[str],
        raw_table_bytes: Optional[int],
    ) -> None:
        self.current_partition = current_partition
        self.raw_table_bytes = raw_table_bytes

    def snapshot(
        self,
        *,
        queue_depth: int,
        connection_id: Optional[UUID] = None,
    ) -> CaptureCounterSnapshot:
        _require_int(queue_depth, "queue_depth", positive=False)
        return CaptureCounterSnapshot(
            messages_received_total=self.messages_received_total,
            messages_accepted_total=self.messages_accepted_total,
            parse_errors_total=self.parse_errors_total,
            records_coalesced_total=self.records_coalesced_total,
            records_enqueued_total=self.records_enqueued_total,
            records_persisted_total=self.records_persisted_total,
            records_dropped_total=self.records_dropped_total,
            batches_failed_total=self.batches_failed_total,
            maintenance_runs_total=self.maintenance_runs_total,
            maintenance_failures_total=self.maintenance_failures_total,
            capture_suspended=self.capture_suspended,
            queue_depth=queue_depth,
            queue_high_water=self.queue_high_water,
            last_batch_rows=self.last_batch_rows,
            last_batch_duration_ms=self.last_batch_duration_ms,
            current_partition=self.current_partition,
            raw_table_bytes=self.raw_table_bytes,
            connection_id=connection_id,
        )


class FeedSession:
    __slots__ = (
        "source",
        "connection_id",
        "connected_wall_ns",
        "connected_monotonic_ns",
        "_ready_wall_ns",
        "_ready_monotonic_ns",
        "_messages_received_total",
        "_messages_accepted_total",
        "_parse_errors_total",
        "_records_dropped_total",
        "_receive_sequence",
        "_closed_record",
        "_counters",
    )

    def __init__(
        self,
        *,
        source: str,
        connection_id: Optional[UUID] = None,
        connected_wall_ns: Optional[int] = None,
        connected_monotonic_ns: Optional[int] = None,
        counters: Optional[CaptureCounters] = None,
    ) -> None:
        if source not in FEED_SESSION_SOURCES:
            raise ValueError("invalid feed session source")
        self.source = source
        self.connection_id = connection_id or uuid4()
        self.connected_wall_ns = (
            time.time_ns() if connected_wall_ns is None else connected_wall_ns
        )
        self.connected_monotonic_ns = (
            time.monotonic_ns()
            if connected_monotonic_ns is None
            else connected_monotonic_ns
        )
        _require_uuid(self.connection_id, "connection_id")
        _require_int(self.connected_wall_ns, "connected_wall_ns", positive=True)
        _require_int(
            self.connected_monotonic_ns,
            "connected_monotonic_ns",
            positive=True,
        )
        self._ready_wall_ns: Optional[int] = None
        self._ready_monotonic_ns: Optional[int] = None
        self._messages_received_total = 0
        self._messages_accepted_total = 0
        self._parse_errors_total = 0
        self._records_dropped_total = 0
        self._receive_sequence = 0
        self._closed_record: Optional[FeedSessionRecord] = None
        self._counters = counters

    def mark_ready(
        self,
        *,
        ready_wall_ns: Optional[int] = None,
        ready_monotonic_ns: Optional[int] = None,
    ) -> None:
        if self._closed_record is not None:
            raise RuntimeError("feed session is closed")
        if self._ready_wall_ns is not None:
            raise RuntimeError("feed session is already ready")
        wall_ns = time.time_ns() if ready_wall_ns is None else ready_wall_ns
        monotonic_ns = (
            time.monotonic_ns()
            if ready_monotonic_ns is None
            else ready_monotonic_ns
        )
        _require_int(wall_ns, "ready_wall_ns", positive=True)
        _require_int(monotonic_ns, "ready_monotonic_ns", positive=True)
        if monotonic_ns < self.connected_monotonic_ns:
            raise ValueError("ready monotonic time precedes connection")
        self._ready_wall_ns = wall_ns
        self._ready_monotonic_ns = monotonic_ns

    def next_receive_stamp(
        self,
        *,
        received_wall_ns: Optional[int] = None,
        received_monotonic_ns: Optional[int] = None,
    ) -> ReceiveStamp:
        if self._closed_record is not None:
            raise RuntimeError("feed session is closed")
        next_sequence = self._receive_sequence + 1
        stamp = ReceiveStamp(
            connection_id=self.connection_id,
            receive_sequence=next_sequence,
            received_wall_ns=(
                time.time_ns() if received_wall_ns is None else received_wall_ns
            ),
            received_monotonic_ns=(
                time.monotonic_ns()
                if received_monotonic_ns is None
                else received_monotonic_ns
            ),
        )
        self._receive_sequence = next_sequence
        self._messages_received_total += 1
        if self._counters is not None:
            self._counters.message_received()
        return stamp

    def mark_accepted(self, count: int = 1) -> None:
        if self._closed_record is not None:
            raise RuntimeError("feed session is closed")
        _require_positive_count(count, "count")
        if (
            self._messages_accepted_total
            + self._parse_errors_total
            + count
            > self._messages_received_total
        ):
            raise ValueError("accepted plus parse-error messages exceed received messages")
        self._messages_accepted_total += count
        if self._counters is not None:
            self._counters.message_accepted(count)

    def mark_parse_error(self, count: int = 1) -> None:
        if self._closed_record is not None:
            raise RuntimeError("feed session is closed")
        _require_positive_count(count, "count")
        if (
            self._messages_accepted_total
            + self._parse_errors_total
            + count
            > self._messages_received_total
        ):
            raise ValueError("accepted plus parse-error messages exceed received messages")
        self._parse_errors_total += count
        if self._counters is not None:
            self._counters.parse_error(count)

    def mark_record_dropped(self, count: int = 1) -> None:
        if self._closed_record is not None:
            raise RuntimeError("feed session is closed")
        _require_positive_count(count, "count")
        self._records_dropped_total += count

    def opened_record(self) -> FeedSessionRecord:
        if self._closed_record is not None:
            raise RuntimeError("feed session is closed")
        return self._record(
            disconnected_wall_ns=None,
            disconnected_monotonic_ns=None,
            close_reason=None,
        )

    def finish(
        self,
        *,
        close_reason: str,
        disconnected_wall_ns: Optional[int] = None,
        disconnected_monotonic_ns: Optional[int] = None,
    ) -> FeedSessionRecord:
        if close_reason not in FEED_SESSION_CLOSE_REASONS:
            raise ValueError("invalid feed session close reason")
        if self._closed_record is not None:
            return self._closed_record

        wall_ns = time.time_ns() if disconnected_wall_ns is None else disconnected_wall_ns
        monotonic_ns = (
            time.monotonic_ns()
            if disconnected_monotonic_ns is None
            else disconnected_monotonic_ns
        )
        if monotonic_ns < self.connected_monotonic_ns:
            raise ValueError("disconnect monotonic time precedes connection")
        if self._ready_monotonic_ns is not None and monotonic_ns < self._ready_monotonic_ns:
            raise ValueError("disconnect monotonic time precedes ready time")

        self._closed_record = self._record(
            disconnected_wall_ns=wall_ns,
            disconnected_monotonic_ns=monotonic_ns,
            close_reason=close_reason,
        )
        return self._closed_record

    def _record(
        self,
        *,
        disconnected_wall_ns: Optional[int],
        disconnected_monotonic_ns: Optional[int],
        close_reason: Optional[str],
    ) -> FeedSessionRecord:
        return FeedSessionRecord(
            source=self.source,
            connection_id=self.connection_id,
            connected_wall_ns=self.connected_wall_ns,
            connected_monotonic_ns=self.connected_monotonic_ns,
            ready_wall_ns=self._ready_wall_ns,
            ready_monotonic_ns=self._ready_monotonic_ns,
            disconnected_wall_ns=disconnected_wall_ns,
            disconnected_monotonic_ns=disconnected_monotonic_ns,
            close_reason=close_reason,
            messages_received_total=self._messages_received_total,
            messages_accepted_total=self._messages_accepted_total,
            parse_errors_total=self._parse_errors_total,
            records_dropped_total=self._records_dropped_total,
            last_receive_sequence=self._receive_sequence,
        )


class _PendingFuturesBucket:
    __slots__ = (
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

    def __init__(self, observation: FuturesTradeObservation) -> None:
        self.bucket_start_ms = (
            observation.received_wall_ns // FUTURES_BUCKET_NS
        ) * FUTURES_BUCKET_MS
        self.connection_id = observation.connection_id
        self.first_received_wall_ns = observation.received_wall_ns
        self.last_received_wall_ns = observation.received_wall_ns
        self.first_received_monotonic_ns = observation.received_monotonic_ns
        self.last_received_monotonic_ns = observation.received_monotonic_ns
        self.first_trade_time_ms = observation.trade_time_ms
        self.last_trade_time_ms = observation.trade_time_ms
        self.first_event_time_ms = observation.event_time_ms
        self.last_event_time_ms = observation.event_time_ms
        self.open_price = observation.price
        self.high_price = observation.price
        self.low_price = observation.price
        self.close_price = observation.price
        self.event_count = 1
        self.first_agg_trade_id = observation.agg_trade_id
        self.last_agg_trade_id = observation.agg_trade_id

    def add(self, observation: FuturesTradeObservation) -> None:
        self.last_received_wall_ns = observation.received_wall_ns
        self.last_received_monotonic_ns = observation.received_monotonic_ns
        self.last_trade_time_ms = observation.trade_time_ms
        self.last_event_time_ms = observation.event_time_ms
        self.high_price = max(self.high_price, observation.price)
        self.low_price = min(self.low_price, observation.price)
        self.close_price = observation.price
        self.event_count += 1
        self.last_agg_trade_id = observation.agg_trade_id

    def finish(self) -> BinanceFuturesPriceTrace:
        return BinanceFuturesPriceTrace(
            bucket_start_ms=self.bucket_start_ms,
            connection_id=self.connection_id,
            first_received_wall_ns=self.first_received_wall_ns,
            last_received_wall_ns=self.last_received_wall_ns,
            first_received_monotonic_ns=self.first_received_monotonic_ns,
            last_received_monotonic_ns=self.last_received_monotonic_ns,
            first_trade_time_ms=self.first_trade_time_ms,
            last_trade_time_ms=self.last_trade_time_ms,
            first_event_time_ms=self.first_event_time_ms,
            last_event_time_ms=self.last_event_time_ms,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            event_count=self.event_count,
            first_agg_trade_id=self.first_agg_trade_id,
            last_agg_trade_id=self.last_agg_trade_id,
        )


class FuturesPriceTraceCoalescer:
    __slots__ = (
        "bucket_ms",
        "_pending",
        "_counters",
        "_last_sealed_bucket_by_connection",
        "_last_sequence_warning_ns",
    )

    def __init__(
        self,
        *,
        bucket_ms: int = FUTURES_BUCKET_MS,
        counters: Optional[CaptureCounters] = None,
    ) -> None:
        if bucket_ms != FUTURES_BUCKET_MS:
            raise ValueError("futures capture requires 100 ms buckets")
        self.bucket_ms = bucket_ms
        self._pending: Optional[_PendingFuturesBucket] = None
        self._counters = counters
        self._last_sealed_bucket_by_connection: "OrderedDict[UUID, int]" = OrderedDict()
        self._last_sequence_warning_ns: Optional[int] = None

    @property
    def pending_event_count(self) -> int:
        return 0 if self._pending is None else self._pending.event_count

    @property
    def sealed_connection_count(self) -> int:
        return len(self._last_sealed_bucket_by_connection)

    def _drop_out_of_sequence(self, reason: str) -> None:
        if self._counters is not None:
            self._counters.record_dropped()
        now_ns = time.monotonic_ns()
        if (
            self._last_sequence_warning_ns is not None
            and now_ns - self._last_sequence_warning_ns < 60_000_000_000
        ):
            return
        self._last_sequence_warning_ns = now_ns
        LOGGER.warning(
            "raw_capture_futures_observation_dropped",
            extra={
                "event": "raw_capture_futures_observation_dropped",
                "reason": reason,
            },
        )

    def add_trade(
        self,
        observation: FuturesTradeObservation,
    ) -> Optional[BinanceFuturesPriceTrace]:
        if not isinstance(observation.price, Decimal):
            raise TypeError("futures capture price must be Decimal")

        bucket_start_ms = (
            observation.received_wall_ns // FUTURES_BUCKET_NS
        ) * FUTURES_BUCKET_MS
        last_sealed_bucket = self._last_sealed_bucket_by_connection.get(
            observation.connection_id
        )
        if last_sealed_bucket is not None and bucket_start_ms <= last_sealed_bucket:
            self._drop_out_of_sequence("receive_bucket_already_sealed")
            return None
        if self._pending is None:
            self._pending = _PendingFuturesBucket(observation)
            return None

        if (
            self._pending.connection_id == observation.connection_id
            and self._pending.bucket_start_ms == bucket_start_ms
        ):
            if (
                observation.received_monotonic_ns
                < self._pending.last_received_monotonic_ns
            ):
                self._drop_out_of_sequence("monotonic_time_moved_backwards")
                return None
            self._pending.add(observation)
            if self._counters is not None:
                self._counters.record_coalesced()
            return None

        if (
            self._pending.connection_id == observation.connection_id
            and bucket_start_ms < self._pending.bucket_start_ms
        ):
            self._drop_out_of_sequence("receive_bucket_moved_backwards")
            return None

        finished = self._seal_pending()
        self._pending = _PendingFuturesBucket(observation)
        return finished

    def finish(self) -> Optional[BinanceFuturesPriceTrace]:
        if self._pending is None:
            return None
        finished = self._seal_pending()
        self._pending = None
        return finished

    def _seal_pending(self) -> BinanceFuturesPriceTrace:
        if self._pending is None:
            raise RuntimeError("no futures bucket is pending")
        finished = self._pending.finish()
        previous = self._last_sealed_bucket_by_connection.get(finished.connection_id)
        if previous is None or finished.bucket_start_ms > previous:
            if previous is None and len(self._last_sealed_bucket_by_connection) >= SEALED_CONNECTION_HISTORY_MAX:
                self._last_sealed_bucket_by_connection.popitem(last=False)
            self._last_sealed_bucket_by_connection[
                finished.connection_id
            ] = finished.bucket_start_ms
            self._last_sealed_bucket_by_connection.move_to_end(finished.connection_id)
        return finished

    def finish_if_elapsed(
        self,
        *,
        now_wall_ns: int,
        now_monotonic_ns: Optional[int] = None,
    ) -> Optional[BinanceFuturesPriceTrace]:
        _require_int(now_wall_ns, "now_wall_ns", positive=True)
        if self._pending is None:
            return None
        if now_monotonic_ns is not None:
            _require_int(now_monotonic_ns, "now_monotonic_ns", positive=True)
            if now_monotonic_ns < self._pending.last_received_monotonic_ns:
                raise ValueError("now monotonic time precedes pending observation")
        bucket_end_ns = (
            self._pending.bucket_start_ms + FUTURES_BUCKET_MS
        ) * 1_000_000
        wall_elapsed = now_wall_ns >= bucket_end_ns
        monotonic_elapsed = (
            now_monotonic_ns is not None
            and now_monotonic_ns - self._pending.last_received_monotonic_ns
            >= FUTURES_BUCKET_NS
        )
        if not wall_elapsed and not monotonic_elapsed:
            return None
        return self.finish()


@dataclass(frozen=True)
class OfferResult:
    __slots__ = (
        "accepted",
        "dropped_oldest",
        "dropped_record",
        "queue_depth",
        "queue_high_water",
    )

    accepted: bool
    dropped_oldest: bool
    dropped_record: Optional[Any]
    queue_depth: int
    queue_high_water: int

    def __post_init__(self) -> None:
        _require_int(self.queue_depth, "queue_depth", positive=False)
        _require_int(self.queue_high_water, "queue_high_water", positive=False)
        if self.queue_depth > self.queue_high_water:
            raise ValueError("queue depth exceeds queue high-water mark")


T = TypeVar("T")


class DropOldestCaptureBuffer(Generic[T]):
    """A bounded, single-event-loop buffer with a synchronous producer path."""

    __slots__ = (
        "_items",
        "_max_events",
        "_available",
        "_high_water",
        "_counters",
        "_drop_warning_interval_ns",
        "_last_drop_warning_ns",
        "_drop_warning_hook",
        "_drop_warning_in_progress",
    )

    def __init__(
        self,
        *,
        max_events: int,
        counters: Optional[CaptureCounters] = None,
        drop_warning_interval_seconds: float = 60.0,
        drop_warning_hook: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        if max_events <= 0:
            raise ValueError("capture buffer max_events must be positive")
        if drop_warning_interval_seconds <= 0:
            raise ValueError("drop warning interval must be positive")
        self._items: Deque[T] = deque()
        self._max_events = max_events
        self._available: Optional[asyncio.Event] = None
        self._high_water = 0
        self._counters = counters
        self._drop_warning_interval_ns = int(
            drop_warning_interval_seconds * 1_000_000_000
        )
        self._last_drop_warning_ns: Optional[int] = None
        self._drop_warning_hook = drop_warning_hook
        self._drop_warning_in_progress = False

    @property
    def maxsize(self) -> int:
        return self._max_events

    @property
    def high_water(self) -> int:
        return self._high_water

    def qsize(self) -> int:
        return len(self._items)

    def empty(self) -> bool:
        return not self._items

    def offer_nowait(self, item: T) -> OfferResult:
        dropped = False
        dropped_record: Optional[T] = None
        if len(self._items) >= self._max_events:
            dropped_record = self._items.popleft()
            dropped = True
            if self._counters is not None:
                self._counters.record_dropped()

        self._items.append(item)
        depth = len(self._items)
        self._high_water = max(self._high_water, depth)
        if self._counters is not None:
            self._counters.record_enqueued()
            self._counters.observe_queue_depth(depth)
        if self._available is not None:
            self._available.set()
        if dropped:
            self._warn_drop()
        return OfferResult(
            accepted=True,
            dropped_oldest=dropped,
            dropped_record=dropped_record,
            queue_depth=depth,
            queue_high_water=self._high_water,
        )

    def _warn_drop(self) -> None:
        if self._drop_warning_in_progress:
            return
        now_ns = time.monotonic_ns()
        if (
            self._last_drop_warning_ns is not None
            and now_ns - self._last_drop_warning_ns < self._drop_warning_interval_ns
        ):
            return
        self._last_drop_warning_ns = now_ns
        self._drop_warning_in_progress = True
        try:
            if self._drop_warning_hook is not None:
                try:
                    self._drop_warning_hook(len(self._items), self._max_events)
                except Exception:
                    LOGGER.exception("raw_capture_drop_warning_hook_failed")
                return
            LOGGER.warning(
                "raw_capture_queue_oldest_dropped",
                extra={
                    "event": "raw_capture_queue_oldest_dropped",
                    "queue_depth": len(self._items),
                    "queue_max_events": self._max_events,
                },
            )
        finally:
            self._drop_warning_in_progress = False

    def drain_nowait(self, limit: int) -> List[T]:
        if limit <= 0:
            raise ValueError("capture buffer drain limit must be positive")
        drained: List[T] = []
        while self._items and len(drained) < limit:
            drained.append(self._items.popleft())
        if not self._items and self._available is not None:
            self._available.clear()
        return drained

    def discard_all(self) -> int:
        count = len(self._items)
        self._items.clear()
        if self._available is not None:
            self._available.clear()
        return count

    async def wait_for_item(self, timeout_seconds: float) -> bool:
        if self._items:
            return True
        if self._available is None:
            self._available = asyncio.Event()
        self._available.clear()
        try:
            await asyncio.wait_for(self._available.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return False
        return bool(self._items)

    def wake(self) -> None:
        if self._available is not None:
            self._available.set()


CaptureRecord = Union[
    BinanceFuturesPriceTrace,
    ChainlinkPriceEvent,
    FeedSessionRecord,
]


class CaptureSink(Protocol):
    def offer_nowait(self, record: CaptureRecord) -> OfferResult:
        ...


class RawCaptureBackend(Protocol):
    async def maintain(self) -> bool:
        ...

    async def copy_futures_traces(
        self,
        records: Sequence[BinanceFuturesPriceTrace],
    ) -> None:
        ...

    async def copy_chainlink_events(
        self,
        records: Sequence[ChainlinkPriceEvent],
    ) -> None:
        ...

    async def upsert_feed_sessions(
        self,
        records: Sequence[FeedSessionRecord],
    ) -> None:
        ...

    async def close(self) -> None:
        ...


BackendFactoryResult = Union[RawCaptureBackend, Awaitable[RawCaptureBackend]]
BackendFactory = Callable[[], BackendFactoryResult]


def _consume_task_result(task: "asyncio.Task[Any]") -> None:
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        LOGGER.exception("raw_capture_background_task_failed")


class RawCaptureRuntime:
    __slots__ = (
        "counters",
        "buffer",
        "_backend_factory",
        "_backend",
        "_batch_max_rows",
        "_flush_seconds",
        "_maintenance_interval_seconds",
        "_shutdown_timeout_seconds",
        "_writer_task",
        "_stop_requested",
        "_accepting_offers",
        "_storage_permitted",
        "_active_batch",
        "_shutdown_buffer_accounted",
    )

    def __init__(
        self,
        *,
        backend_factory: BackendFactory,
        queue_max_events: int,
        batch_max_rows: int,
        flush_ms: int,
        maintenance_interval_seconds: float,
        shutdown_timeout_ms: int,
        counters: Optional[CaptureCounters] = None,
        bucket_ms: int = FUTURES_BUCKET_MS,
    ) -> None:
        if bucket_ms != FUTURES_BUCKET_MS:
            raise ValueError("futures capture requires 100 ms buckets")
        _require_int(queue_max_events, "queue_max_events", positive=True)
        _require_int(batch_max_rows, "batch_max_rows", positive=True)
        if batch_max_rows > queue_max_events:
            raise ValueError("raw capture batch_max_rows cannot exceed queue capacity")
        if flush_ms <= 0:
            raise ValueError("raw capture flush_ms must be positive")
        if maintenance_interval_seconds <= 0:
            raise ValueError("raw capture maintenance interval must be positive")
        if shutdown_timeout_ms <= 0:
            raise ValueError("raw capture shutdown timeout must be positive")

        self.counters = counters or CaptureCounters()
        self.buffer: DropOldestCaptureBuffer[CaptureRecord] = DropOldestCaptureBuffer(
            max_events=queue_max_events,
            counters=self.counters,
        )
        self._backend_factory = backend_factory
        self._backend: Optional[RawCaptureBackend] = None
        self._batch_max_rows = batch_max_rows
        self._flush_seconds = flush_ms / 1000.0
        self._maintenance_interval_seconds = maintenance_interval_seconds
        self._shutdown_timeout_seconds = shutdown_timeout_ms / 1000.0
        self._writer_task: Optional["asyncio.Task[None]"] = None
        self._stop_requested = False
        self._accepting_offers = True
        self._storage_permitted = True
        self._active_batch: Optional[List[CaptureRecord]] = None
        self._shutdown_buffer_accounted = False

    @property
    def started(self) -> bool:
        return self._writer_task is not None

    @property
    def storage_permitted(self) -> bool:
        return self._storage_permitted

    def start(self) -> "asyncio.Task[None]":
        if self._stop_requested:
            raise RuntimeError("raw capture runtime is closed")
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(self._writer_loop())
            self._writer_task.add_done_callback(self._writer_done)
        return self._writer_task

    def _writer_done(self, task: "asyncio.Task[Any]") -> None:
        _consume_task_result(task)
        if self._stop_requested:
            self._discard_shutdown_buffer_once()

    def _discard_shutdown_buffer_once(self) -> None:
        if self._shutdown_buffer_accounted:
            return
        discarded = self.buffer.discard_all()
        self.counters.record_dropped(discarded)
        self._shutdown_buffer_accounted = True

    def offer_nowait(self, record: CaptureRecord) -> OfferResult:
        if not isinstance(
            record,
            (BinanceFuturesPriceTrace, ChainlinkPriceEvent, FeedSessionRecord),
        ):
            raise TypeError("raw capture runtime only accepts CaptureRecord values")
        if not self._accepting_offers or not self._storage_permitted:
            self.counters.record_dropped()
            return OfferResult(
                accepted=False,
                dropped_oldest=False,
                dropped_record=None,
                queue_depth=self.buffer.qsize(),
                queue_high_water=self.buffer.high_water,
            )
        return self.buffer.offer_nowait(record)

    async def close(self, *, timeout_ms: Optional[int] = None) -> None:
        if not self._stop_requested:
            self._accepting_offers = False
            self._stop_requested = True
            self.buffer.wake()

        if self._writer_task is None:
            self._discard_shutdown_buffer_once()
            return
        if self._writer_task.done():
            self._discard_shutdown_buffer_once()
            return

        timeout_seconds = (
            self._shutdown_timeout_seconds
            if timeout_ms is None
            else max(0.001, timeout_ms / 1000.0)
        )
        done, _pending = await asyncio.wait(
            {self._writer_task},
            timeout=timeout_seconds,
        )
        if self._writer_task not in done:
            self._writer_task.cancel()
            cancelled_done, _cancelled_pending = await asyncio.wait(
                {self._writer_task},
                timeout=min(0.1, max(0.01, timeout_seconds)),
            )
            if self._writer_task not in cancelled_done:
                self._discard_shutdown_buffer_once()
                LOGGER.warning(
                    "raw_capture_shutdown_task_still_running",
                    extra={
                        "event": "raw_capture_shutdown_task_still_running",
                        "queue_depth": self.buffer.qsize(),
                        "active_batch_rows": (
                            0
                            if self._active_batch is None
                            else len(self._active_batch)
                        ),
                    },
                )
        if self._writer_task.done():
            self._discard_shutdown_buffer_once()

    async def _ensure_backend(self) -> RawCaptureBackend:
        if self._backend is not None:
            return self._backend
        result = self._backend_factory()
        if inspect.isawaitable(result):
            result = await result
        self._backend = result
        return result

    async def _run_maintenance(self) -> None:
        was_permitted = self._storage_permitted
        succeeded = False
        permitted = False
        try:
            backend = await self._ensure_backend()
            permitted = bool(await backend.maintain())
            succeeded = True
            self.counters.record_storage_state(
                current_partition=getattr(backend, "current_partition", None),
                raw_table_bytes=getattr(backend, "raw_table_bytes", None),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("raw_capture_maintenance_failed")
        self._storage_permitted = permitted
        self.counters.record_maintenance(succeeded=succeeded, permitted=permitted)
        if not permitted:
            if succeeded:
                LOGGER.warning(
                    "raw_capture_suspended_by_storage_budget",
                    extra={
                        "event": "raw_capture_suspended_by_storage_budget",
                        "queue_depth": self.buffer.qsize(),
                    },
                )
            discarded = self.buffer.discard_all()
            self.counters.record_dropped(discarded)
        elif not was_permitted:
            LOGGER.info(
                "raw_capture_storage_resumed",
                extra={"event": "raw_capture_storage_resumed"},
            )

    async def _take_batch(self, first_wait_seconds: float) -> List[CaptureRecord]:
        available = await self.buffer.wait_for_item(max(0.001, first_wait_seconds))
        if not available:
            return []

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._flush_seconds
        batch: List[CaptureRecord] = []
        try:
            while len(batch) < self._batch_max_rows:
                batch.extend(
                    self.buffer.drain_nowait(self._batch_max_rows - len(batch))
                )
                if len(batch) >= self._batch_max_rows or self._stop_requested:
                    break
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                available = await self.buffer.wait_for_item(remaining)
                if not available:
                    break
        except asyncio.CancelledError:
            self.counters.record_dropped(len(batch))
            raise
        return batch

    async def _write_batch(self, batch: Sequence[CaptureRecord]) -> None:
        started_ns = time.monotonic_ns()
        persisted = 0
        dropped = 0
        failed = False
        try:
            try:
                backend = await self._ensure_backend()
            except Exception:
                LOGGER.exception("raw_capture_backend_creation_failed")
                failed = True
                dropped = len(batch)
            else:
                groups = (
                    (
                        [
                            record
                            for record in batch
                            if isinstance(record, BinanceFuturesPriceTrace)
                        ],
                        backend.copy_futures_traces,
                    ),
                    (
                        [
                            record
                            for record in batch
                            if isinstance(record, ChainlinkPriceEvent)
                        ],
                        backend.copy_chainlink_events,
                    ),
                    (
                        [
                            record
                            for record in batch
                            if isinstance(record, FeedSessionRecord)
                        ],
                        backend.upsert_feed_sessions,
                    ),
                )
                for records, write in groups:
                    if not records:
                        continue
                    try:
                        await write(records)
                    except Exception:
                        LOGGER.exception("raw_capture_batch_group_failed")
                        failed = True
                        dropped += len(records)
                    else:
                        persisted += len(records)
        except asyncio.CancelledError:
            dropped += len(batch) - persisted - dropped
            duration_ms = (time.monotonic_ns() - started_ns) / 1_000_000.0
            self.counters.record_batch_result(
                rows=len(batch),
                persisted=persisted,
                dropped=dropped,
                failed=True,
                duration_ms=duration_ms,
            )
            raise

        duration_ms = (time.monotonic_ns() - started_ns) / 1_000_000.0
        self.counters.record_batch_result(
            rows=len(batch),
            persisted=persisted,
            dropped=dropped,
            failed=failed,
            duration_ms=duration_ms,
        )

    async def _writer_loop(self) -> None:
        loop = asyncio.get_running_loop()
        next_maintenance = loop.time()
        try:
            while True:
                now = loop.time()
                if now >= next_maintenance:
                    await self._run_maintenance()
                    maintenance_delay = (
                        self._maintenance_interval_seconds
                        if self._storage_permitted
                        else min(
                            SUSPENDED_MAINTENANCE_RETRY_SECONDS,
                            self._maintenance_interval_seconds,
                        )
                    )
                    next_maintenance = loop.time() + maintenance_delay

                if self._stop_requested and self.buffer.empty():
                    break

                until_maintenance = max(0.001, next_maintenance - loop.time())
                if not self._storage_permitted:
                    await self.buffer.wait_for_item(until_maintenance)
                    if not self.buffer.empty():
                        discarded = self.buffer.discard_all()
                        self.counters.record_dropped(discarded)
                    continue

                batch = await self._take_batch(until_maintenance)
                if not batch:
                    continue

                self._active_batch = batch
                try:
                    if loop.time() >= next_maintenance:
                        try:
                            await self._run_maintenance()
                        except asyncio.CancelledError:
                            self.counters.record_dropped(len(batch))
                            raise
                        maintenance_delay = (
                            self._maintenance_interval_seconds
                            if self._storage_permitted
                            else min(
                                SUSPENDED_MAINTENANCE_RETRY_SECONDS,
                                self._maintenance_interval_seconds,
                            )
                        )
                        next_maintenance = loop.time() + maintenance_delay
                    if not self._storage_permitted:
                        self.counters.record_dropped(len(batch))
                        continue
                    await self._write_batch(batch)
                finally:
                    self._active_batch = None
        finally:
            backend = self._backend
            self._backend = None
            if backend is not None:
                try:
                    await asyncio.wait_for(
                        backend.close(),
                        timeout=min(1.0, self._shutdown_timeout_seconds),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception("raw_capture_backend_close_failed")


def create_raw_capture_runtime(
    *,
    futures_enabled: bool,
    chainlink_enabled: bool,
    backend_factory: Optional[BackendFactory] = None,
    queue_max_events: int = 5_000,
    batch_max_rows: int = 500,
    flush_ms: int = 1_000,
    maintenance_interval_seconds: float = 60,
    shutdown_timeout_ms: int = 2_000,
    bucket_ms: int = FUTURES_BUCKET_MS,
    counters: Optional[CaptureCounters] = None,
) -> Optional[RawCaptureRuntime]:
    if not futures_enabled and not chainlink_enabled:
        return None
    if backend_factory is None:
        raise ValueError("enabled raw capture requires a backend factory")
    return RawCaptureRuntime(
        backend_factory=backend_factory,
        queue_max_events=queue_max_events,
        batch_max_rows=batch_max_rows,
        flush_ms=flush_ms,
        maintenance_interval_seconds=maintenance_interval_seconds,
        shutdown_timeout_ms=shutdown_timeout_ms,
        counters=counters,
        bucket_ms=bucket_ms,
    )
