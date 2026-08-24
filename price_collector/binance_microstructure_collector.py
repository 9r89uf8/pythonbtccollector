"""Noncritical Binance microstructure readers and one-second persistence.

The existing futures collector owns the futures aggTrade and REST context
feeds.  This module adds the remaining public streams and folds all events
offered through :class:`MicrostructureEventSink` into receipt-time-causal
one-second PostgreSQL rows.
"""

from __future__ import annotations

import asyncio
import heapq
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping, Optional

import websockets

from price_collector.binance_futures_streams import (
    parse_binance_futures_agg_trade_payload,
)
from price_collector.binance_microstructure import (
    MICROSTRUCTURE_VALUE_COLUMNS,
    CollectorState,
    MicrostructureEventSink,
    finalize_boundary,
    parse_futures_depth_payload,
    parse_liquidation_payload,
    parse_spot_depth_payload,
)
from price_collector.collector import (
    BINANCE_RECONNECT_SECONDS,
    current_utc_epoch_ms,
    reconnect_delay_seconds,
)
from price_collector.db import epoch_ms_to_utc_datetime
from price_collector.live_cache import MICROSTRUCTURE_LIVE_KEY
from price_collector.market import MarketWindow, market_for_sample_second


LOGGER = logging.getLogger("price_collector.binance_microstructure_collector")
MILLISECONDS_PER_DAY = 86_400_000
BYTES_PER_MEBIBYTE = 1024 * 1024

SPOT_SOURCE = "spot"
FUTURES_DEPTH_SOURCE = "futures_depth"
FUTURES_LIQUIDATION_SOURCE = "futures_liquidation"
_OFFER_LOG_INTERVAL_SECONDS = 60.0
_LAST_OFFER_LOG_AT: dict[str, float] = {}
MICROSTRUCTURE_PERSIST_DRAIN_TIMEOUT_SECONDS = 5.0


class MicrostructureMessageError(ValueError):
    """A frame does not belong to the exact stream handled by its reader."""


class BinanceServerShutdown(ConnectionError):
    """Binance requested a reconnect instead of ordinary frame skipping."""


def _stream_name(symbol: str, suffix: str) -> str:
    return f"{symbol.lower()}@{suffix}"


def unwrap_stream_payload(
    message: Mapping[str, Any],
    *,
    raw_stream: Optional[str] = None,
) -> tuple[str, Mapping[str, Any]]:
    """Return ``(stream, data)`` for Binance combined or raw frames.

    A combined frame must contain both a string ``stream`` and an object
    ``data``.  A raw frame has no wrapper and is assigned the caller's known
    endpoint stream, when one is available.
    """

    if not isinstance(message, Mapping):
        raise MicrostructureMessageError("Binance WebSocket frame must be an object")

    has_wrapper_field = "stream" in message or "data" in message
    if has_wrapper_field:
        stream = message.get("stream")
        data = message.get("data")
        if not isinstance(stream, str) or not stream:
            raise MicrostructureMessageError(
                "combined Binance frame is missing a string stream name"
            )
        if not isinstance(data, Mapping):
            raise MicrostructureMessageError(
                "combined Binance frame data must be an object"
            )
        return stream, data

    return raw_stream or "", message


def _raise_for_server_shutdown(payload: Mapping[str, Any]) -> None:
    if payload.get("e") == "serverShutdown":
        raise BinanceServerShutdown("Binance announced serverShutdown")


def _offer(sink: MicrostructureEventSink, method_name: str, *args: Any) -> bool:
    """Call a nonblocking sink method without taking a reader down."""

    try:
        accepted = bool(getattr(sink, method_name)(*args))
    except Exception:
        if _should_log_offer_failure(f"exception:{method_name}"):
            LOGGER.exception(
                "binance_microstructure_sink_offer_failed",
                extra={
                    "event": "binance_microstructure_sink_offer_failed",
                    "method": method_name,
                },
            )
        return False

    if not accepted and _should_log_offer_failure(f"full:{method_name}"):
        LOGGER.warning(
            "binance_microstructure_queue_full",
            extra={
                "event": "binance_microstructure_queue_full",
                "method": method_name,
            },
        )
    return accepted


def _should_log_offer_failure(key: str) -> bool:
    """Rate-limit hot-path sink failures while retaining an operational signal."""

    now = time.monotonic()
    previous = _LAST_OFFER_LOG_AT.get(key)
    if previous is not None and now - previous < _OFFER_LOG_INTERVAL_SECONDS:
        return False
    _LAST_OFFER_LOG_AT[key] = now
    return True


def dispatch_spot_message(
    message: Mapping[str, Any],
    *,
    sink: MicrostructureEventSink,
    expected_symbol: str,
    received_ms: int,
) -> bool:
    """Validate and offer one spot aggTrade or top-10 depth frame."""

    stream, payload = unwrap_stream_payload(message)
    _raise_for_server_shutdown(payload)
    agg_trade_stream = _stream_name(expected_symbol, "aggTrade")
    depth_stream = _stream_name(expected_symbol, "depth10")

    # The configured spot endpoint is combined.  Raw support is retained for
    # tests and operator-supplied /ws URLs, where the payload identifies which
    # of the two exact subscriptions it represents.
    if not stream:
        if payload.get("e") == "aggTrade":
            stream = agg_trade_stream
        elif "lastUpdateId" in payload and "bids" in payload and "asks" in payload:
            stream = depth_stream
        else:
            raise MicrostructureMessageError("unrecognized raw spot frame")

    if stream == agg_trade_stream:
        trade = parse_binance_futures_agg_trade_payload(
            payload,
            expected_symbol=expected_symbol,
        )
        return _offer(sink, "offer_spot_trade", trade, received_ms)
    if stream == depth_stream:
        depth = parse_spot_depth_payload(
            payload,
            expected_symbol=expected_symbol,
        )
        return _offer(sink, "offer_spot_depth", depth, received_ms)

    raise MicrostructureMessageError(
        f"unexpected spot stream: expected {agg_trade_stream!r} or "
        f"{depth_stream!r}, got {stream!r}"
    )


def dispatch_futures_depth_message(
    message: Mapping[str, Any],
    *,
    sink: MicrostructureEventSink,
    expected_symbol: str,
    received_ms: int,
) -> bool:
    """Validate and offer one USD-M public top-10 depth frame."""

    expected_stream = _stream_name(expected_symbol, "depth10@500ms")
    stream, payload = unwrap_stream_payload(message, raw_stream=expected_stream)
    _raise_for_server_shutdown(payload)
    if stream != expected_stream:
        raise MicrostructureMessageError(
            f"unexpected futures depth stream: expected {expected_stream!r}, "
            f"got {stream!r}"
        )
    depth = parse_futures_depth_payload(payload, expected_symbol=expected_symbol)
    return _offer(sink, "offer_futures_depth", depth, received_ms)


def dispatch_liquidation_message(
    message: Mapping[str, Any],
    *,
    sink: MicrostructureEventSink,
    expected_symbol: str,
    received_ms: int,
) -> bool:
    """Validate and offer one USD-M market forced-order frame."""

    expected_stream = _stream_name(expected_symbol, "forceOrder")
    stream, payload = unwrap_stream_payload(message, raw_stream=expected_stream)
    _raise_for_server_shutdown(payload)
    if stream != expected_stream:
        raise MicrostructureMessageError(
            f"unexpected futures liquidation stream: expected {expected_stream!r}, "
            f"got {stream!r}"
        )
    liquidation = parse_liquidation_payload(payload, expected_symbol=expected_symbol)
    return _offer(sink, "offer_liquidation", liquidation, received_ms)


def _loads_frame(raw: Any) -> Mapping[str, Any]:
    try:
        payload = json.loads(raw, parse_float=Decimal)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
        raise MicrostructureMessageError("invalid Binance WebSocket JSON") from exc
    if not isinstance(payload, Mapping):
        raise MicrostructureMessageError("Binance WebSocket frame must be an object")
    return payload


async def _websocket_reader_loop(
    *,
    name: str,
    source: str,
    url: str,
    sink: MicrostructureEventSink,
    dispatch: Callable[..., bool],
    expected_symbol: str,
) -> None:
    attempt = 0

    while True:
        connected = False
        connection_error: Optional[Exception] = None
        try:
            LOGGER.info(
                "binance_microstructure_websocket_connecting",
                extra={
                    "event": "binance_microstructure_websocket_connecting",
                    "reader": name,
                    "source": source,
                    "url": url,
                },
            )
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                open_timeout=15,
                max_queue=4096,
            ) as websocket:
                attempt = 0
                connected = True
                connected_at = time.monotonic()
                _offer(
                    sink,
                    "connection_opened",
                    source,
                    current_utc_epoch_ms(),
                )
                LOGGER.info(
                    "binance_microstructure_websocket_connected",
                    extra={
                        "event": "binance_microstructure_websocket_connected",
                        "reader": name,
                        "source": source,
                        "url": url,
                    },
                )

                while True:
                    connected_seconds = time.monotonic() - connected_at
                    remaining_seconds = BINANCE_RECONNECT_SECONDS - connected_seconds
                    if remaining_seconds <= 0:
                        LOGGER.info(
                            "binance_microstructure_websocket_proactive_reconnect",
                            extra={
                                "event": (
                                    "binance_microstructure_websocket_proactive_reconnect"
                                ),
                                "source": source,
                                "connected_seconds": int(connected_seconds),
                            },
                        )
                        break

                    try:
                        raw = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=min(30.0, remaining_seconds),
                        )
                        # Causality is keyed to local receipt.  Keep this wall
                        # timestamp immediately after recv and before parsing.
                        received_ms = time.time_ns() // 1_000_000
                    except asyncio.TimeoutError:
                        continue

                    try:
                        message = _loads_frame(raw)
                        dispatch(
                            message,
                            sink=sink,
                            expected_symbol=expected_symbol,
                            received_ms=received_ms,
                        )
                    except BinanceServerShutdown:
                        raise
                    except (MicrostructureMessageError, TypeError, ValueError) as exc:
                        # A rejected frame can undercount trade flow or OFI.
                        # Put a causal loss marker into the same ordered queue
                        # so the affected row cannot report false health.
                        _offer(
                            sink,
                            "record_error",
                            source,
                            received_ms,
                            exc,
                        )
                        if _should_log_offer_failure(f"parse:{source}"):
                            LOGGER.warning(
                                "binance_microstructure_message_skipped",
                                extra={
                                    "event": "binance_microstructure_message_skipped",
                                    "source": source,
                                    "error": str(exc),
                                },
                            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            connection_error = exc
            if not connected:
                # A failed connect is still an observable feed gap.  Offering
                # it through the same ordered queue keeps health accounting
                # causal with respect to every other source event.
                _offer(
                    sink,
                    "connection_closed",
                    source,
                    current_utc_epoch_ms(),
                )
        finally:
            if connected:
                _offer(
                    sink,
                    "connection_closed",
                    source,
                    current_utc_epoch_ms(),
                )

        if connection_error is not None:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "binance_microstructure_websocket_reconnect_scheduled",
                extra={
                    "event": "binance_microstructure_websocket_reconnect_scheduled",
                    "source": source,
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(connection_error),
                },
            )
            await asyncio.sleep(delay)


async def spot_microstructure_reader_loop(
    settings: Any,
    sink: MicrostructureEventSink,
) -> None:
    await _websocket_reader_loop(
        name="spot",
        source=SPOT_SOURCE,
        url=settings.BINANCE_MICROSTRUCTURE_SPOT_WS_URL,
        sink=sink,
        dispatch=dispatch_spot_message,
        expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
    )


async def futures_depth_microstructure_reader_loop(
    settings: Any,
    sink: MicrostructureEventSink,
) -> None:
    await _websocket_reader_loop(
        name="futures-depth",
        source=FUTURES_DEPTH_SOURCE,
        url=settings.BINANCE_MICROSTRUCTURE_FUTURES_DEPTH_WS_URL,
        sink=sink,
        dispatch=dispatch_futures_depth_message,
        expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
    )


async def futures_liquidation_microstructure_reader_loop(
    settings: Any,
    sink: MicrostructureEventSink,
) -> None:
    await _websocket_reader_loop(
        name="futures-liquidation",
        source=FUTURES_LIQUIDATION_SOURCE,
        url=settings.BINANCE_MICROSTRUCTURE_FUTURES_LIQUIDATION_WS_URL,
        sink=sink,
        dispatch=dispatch_liquidation_message,
        expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
    )


_ROW_PREFIX_COLUMNS = (
    "symbol",
    "market_id",
    "sample_second_ms",
    "sample_second_at",
)
_ROW_SUFFIX_COLUMNS = ("received_ms",)
_ALL_INSERT_COLUMNS = (
    *_ROW_PREFIX_COLUMNS,
    *MICROSTRUCTURE_VALUE_COLUMNS,
    *_ROW_SUFFIX_COLUMNS,
)


def _validate_column_names() -> None:
    for column in _ALL_INSERT_COLUMNS:
        if not isinstance(column, str) or not column.replace("_", "").isalnum():
            raise RuntimeError(f"unsafe microstructure column name: {column!r}")


_validate_column_names()


def _insert_sql() -> str:
    columns = ",\n                    ".join(_ALL_INSERT_COLUMNS)
    placeholders = ", ".join(f"${index}" for index in range(1, len(_ALL_INSERT_COLUMNS) + 1))
    update_columns = (
        "market_id",
        "sample_second_at",
        *MICROSTRUCTURE_VALUE_COLUMNS,
        "received_ms",
    )
    assignments = ",\n                    ".join(
        f"{column} = EXCLUDED.{column}" for column in update_columns
    )
    return f"""
                INSERT INTO binance_microstructure_1s (
                    {columns}
                )
                VALUES ({placeholders})
                ON CONFLICT (symbol, sample_second_ms)
                DO UPDATE SET
                    {assignments}
                WHERE binance_microstructure_1s.received_ms
                    <= EXCLUDED.received_ms
                """


MICROSTRUCTURE_UPSERT_SQL = _insert_sql()


async def _ensure_market_window(
    connection: Any,
    window: MarketWindow,
) -> None:
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


async def upsert_binance_microstructure_1s(
    pool: Any,
    *,
    symbol: str,
    row: Mapping[str, Any],
    received_ms: int,
) -> None:
    """Upsert a finalized row and its five-minute window atomically."""

    sample_second_ms = row.get("sample_second_ms")
    if not isinstance(sample_second_ms, int) or isinstance(sample_second_ms, bool):
        raise TypeError("sample_second_ms must be an integer")
    missing = [column for column in MICROSTRUCTURE_VALUE_COLUMNS if column not in row]
    if missing:
        raise ValueError(f"microstructure row is missing columns: {missing!r}")

    window = market_for_sample_second(sample_second_ms)
    arguments = (
        symbol,
        window.market_id,
        sample_second_ms,
        epoch_ms_to_utc_datetime(sample_second_ms),
        *(row[column] for column in MICROSTRUCTURE_VALUE_COLUMNS),
        received_ms,
    )
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(MICROSTRUCTURE_UPSERT_SQL, *arguments)


async def delete_expired_microstructure_rows(
    pool: Any,
    *,
    symbol: str,
    now_ms: int,
    retention_days: int,
) -> Any:
    cutoff_ms = now_ms - retention_days * MILLISECONDS_PER_DAY
    async with pool.acquire() as connection:
        return await connection.execute(
            """
            DELETE FROM binance_microstructure_1s
            WHERE symbol = $1
              AND sample_second_ms < $2
            """,
            symbol,
            cutoff_ms,
        )


async def fetch_microstructure_relation_size_bytes(pool: Any) -> int:
    async with pool.acquire() as connection:
        size = await connection.fetchval(
            """
            SELECT pg_total_relation_size(
                'binance_microstructure_1s'::regclass
            )
            """
        )
    return int(size)


async def update_microstructure_live_cache(
    live_cache: Any,
    *,
    row: Mapping[str, Any],
    received_ms: int,
) -> bool:
    """Publish a finalized second without coupling it to PostgreSQL storage."""

    if live_cache is None:
        return False
    try:
        await live_cache.set_microstructure_snapshot(
            MICROSTRUCTURE_LIVE_KEY,
            row=row,
            received_ms=received_ms,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.exception(
            "microstructure_live_cache_write_failed",
            extra={
                "event": "microstructure_live_cache_write_failed",
                "sample_second_ms": row.get("sample_second_ms"),
            },
        )
        return False
    return True


@dataclass(frozen=True)
class FinalizedMicrostructureRow:
    """One immutable queue envelope for independent historical persistence."""

    row: Mapping[str, Any]
    received_ms: int


def enqueue_finalized_microstructure_row(
    persistence_rows: asyncio.Queue[FinalizedMicrostructureRow],
    *,
    row: Mapping[str, Any],
    received_ms: int,
) -> Optional[FinalizedMicrostructureRow]:
    """Offer without blocking, dropping the oldest queued history at capacity."""

    finalized = FinalizedMicrostructureRow(
        row=dict(row),
        received_ms=received_ms,
    )
    try:
        persistence_rows.put_nowait(finalized)
        return None
    except asyncio.QueueFull:
        dropped = persistence_rows.get_nowait()
        persistence_rows.task_done()
        persistence_rows.put_nowait(finalized)

    LOGGER.error(
        "binance_microstructure_persistence_queue_overflow",
        extra={
            "event": "binance_microstructure_persistence_queue_overflow",
            "dropped_row_count": 1,
            "dropped_sample_second_ms": dropped.row.get("sample_second_ms"),
            "retained_sample_second_ms": row.get("sample_second_ms"),
            "queued_rows": persistence_rows.qsize(),
            "queue_max_rows": persistence_rows.maxsize,
        },
    )
    return dropped


def _requeue_inflight_microstructure_row(
    persistence_rows: asyncio.Queue[FinalizedMicrostructureRow],
    finalized: FinalizedMicrostructureRow,
) -> bool:
    """Preserve a cancelled in-flight row when doing so loses no newer row."""

    try:
        persistence_rows.put_nowait(finalized)
    except asyncio.QueueFull:
        LOGGER.error(
            "binance_microstructure_inflight_row_not_requeued",
            extra={
                "event": "binance_microstructure_inflight_row_not_requeued",
                "sample_second_ms": finalized.row.get("sample_second_ms"),
                "queue_max_rows": persistence_rows.maxsize,
            },
        )
        return False
    return True


@dataclass
class MicrostructureWriteGate:
    """Hysteretic relation-size cap for only the optional research rows."""

    warning_bytes: int
    maximum_bytes: int
    paused: bool = False
    relation_bytes: Optional[int] = None

    def observe(self, relation_bytes: int) -> str:
        self.relation_bytes = relation_bytes
        if relation_bytes >= self.maximum_bytes:
            was_paused = self.paused
            self.paused = True
            return "paused" if not was_paused else "still_paused"
        if self.paused and relation_bytes < self.warning_bytes:
            self.paused = False
            return "resumed"
        if relation_bytes >= self.warning_bytes:
            return "warning"
        return "ok"


async def microstructure_persistence_loop(
    *,
    pool: Any,
    settings: Any,
    persistence_rows: asyncio.Queue[FinalizedMicrostructureRow],
    write_gate: Optional[MicrostructureWriteGate] = None,
) -> None:
    """Drain finalized rows without delaying aggregation or the Redis path."""

    gate = write_gate or MicrostructureWriteGate(
        warning_bytes=(
            settings.BINANCE_MICROSTRUCTURE_WARN_RELATION_MB
            * BYTES_PER_MEBIBYTE
        ),
        maximum_bytes=(
            settings.BINANCE_MICROSTRUCTURE_MAX_RELATION_MB
            * BYTES_PER_MEBIBYTE
        ),
    )
    last_retention_day: Optional[int] = None
    last_retention_attempt_minute: Optional[int] = None
    last_size_check_minute: Optional[int] = None

    while True:
        finalized = await persistence_rows.get()
        finalization_ms = finalized.received_ms
        row = finalized.row
        sample_second_ms = row.get("sample_second_ms")
        try:
            maintenance_ms = current_utc_epoch_ms()
            utc_day = maintenance_ms // MILLISECONDS_PER_DAY
            utc_minute = maintenance_ms // 60_000
            if (
                utc_day != last_retention_day
                and utc_minute != last_retention_attempt_minute
            ):
                last_retention_attempt_minute = utc_minute
                try:
                    result = await delete_expired_microstructure_rows(
                        pool,
                        symbol=settings.BINANCE_FUTURES_SYMBOL,
                        now_ms=maintenance_ms,
                        retention_days=(
                            settings.BINANCE_MICROSTRUCTURE_RETENTION_DAYS
                        ),
                    )
                    LOGGER.info(
                        "binance_microstructure_retention_completed",
                        extra={
                            "event": (
                                "binance_microstructure_retention_completed"
                            ),
                            "result": result,
                            "retention_days": (
                                settings.BINANCE_MICROSTRUCTURE_RETENTION_DAYS
                            ),
                        },
                    )
                    last_retention_day = utc_day
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception(
                        "binance_microstructure_retention_failed",
                        extra={
                            "event": (
                                "binance_microstructure_retention_failed"
                            )
                        },
                    )

            if utc_minute != last_size_check_minute:
                try:
                    relation_bytes = (
                        await fetch_microstructure_relation_size_bytes(pool)
                    )
                    gate_status = gate.observe(relation_bytes)
                    log = (
                        LOGGER.warning
                        if gate_status in {"paused", "warning"}
                        else LOGGER.info
                    )
                    if gate_status != "ok":
                        log(
                            "binance_microstructure_relation_size_status",
                            extra={
                                "event": (
                                    "binance_microstructure_relation_size_status"
                                ),
                                "status": gate_status,
                                "relation_bytes": relation_bytes,
                                "warning_bytes": gate.warning_bytes,
                                "maximum_bytes": gate.maximum_bytes,
                                "writes_paused": gate.paused,
                            },
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A failed size check pauses only this optional historical
                    # sink. Aggregation, Redis, and critical futures paths keep
                    # running while a later successful check can release it.
                    gate.paused = True
                    LOGGER.exception(
                        "binance_microstructure_relation_size_check_failed",
                        extra={
                            "event": (
                                "binance_microstructure_relation_size_check_failed"
                            )
                        },
                    )
                last_size_check_minute = utc_minute

            if gate.paused:
                LOGGER.debug(
                    "binance_microstructure_write_skipped_size_cap",
                    extra={
                        "event": (
                            "binance_microstructure_write_skipped_size_cap"
                        ),
                        "sample_second_ms": sample_second_ms,
                        "relation_bytes": gate.relation_bytes,
                    },
                )
            else:
                try:
                    await upsert_binance_microstructure_1s(
                        pool,
                        symbol=settings.BINANCE_FUTURES_SYMBOL,
                        row=row,
                        received_ms=finalization_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if _should_log_offer_failure("write"):
                        LOGGER.exception(
                            "binance_microstructure_write_failed",
                            extra={
                                "event": "binance_microstructure_write_failed",
                                "sample_second_ms": sample_second_ms,
                            },
                        )
        except asyncio.CancelledError:
            persistence_rows.task_done()
            _requeue_inflight_microstructure_row(
                persistence_rows,
                finalized,
            )
            raise
        except Exception:
            # An invalid internal envelope must not take down the live path or
            # prevent newer, independently finalized rows from being drained.
            LOGGER.exception(
                "binance_microstructure_persistence_row_failed",
                extra={
                    "event": "binance_microstructure_persistence_row_failed",
                    "sample_second_ms": sample_second_ms,
                },
            )
            persistence_rows.task_done()
        else:
            persistence_rows.task_done()


def _event_received_ms(event: Any) -> int:
    try:
        return int(event.received_ms)
    except AttributeError as exc:
        raise TypeError("queued microstructure event has no received_ms") from exc


def _record_aggregate_gap(state: CollectorState, received_ms: int) -> None:
    method = getattr(state, "record_gap", None)
    if callable(method):
        try:
            method(received_ms)
        except Exception:
            LOGGER.exception(
                "binance_microstructure_gap_record_failed",
                extra={"event": "binance_microstructure_gap_record_failed"},
            )


def _stage_pending_event(
    *,
    event: Any,
    pending: list[Any],
    state: CollectorState,
    last_finalized_boundary_ms: Optional[int],
    detected_ms: int,
) -> None:
    event_received_ms = _event_received_ms(event)
    if (
        last_finalized_boundary_ms is not None
        and event_received_ms < last_finalized_boundary_ms
    ):
        _record_aggregate_gap(state, detected_ms)
        if _should_log_offer_failure("late_event"):
            LOGGER.warning(
                "binance_microstructure_late_event_dropped",
                extra={
                    "event": "binance_microstructure_late_event_dropped",
                    "received_ms": event_received_ms,
                    "last_finalized_boundary_ms": last_finalized_boundary_ms,
                },
            )
        return
    heapq.heappush(pending, event)


def _drain_events_into_pending(
    *,
    events: asyncio.Queue[Any],
    pending: list[Any],
    state: CollectorState,
    last_finalized_boundary_ms: Optional[int],
    detected_ms: int,
) -> None:
    while True:
        try:
            event = events.get_nowait()
        except asyncio.QueueEmpty:
            return
        _stage_pending_event(
            event=event,
            pending=pending,
            state=state,
            last_finalized_boundary_ms=last_finalized_boundary_ms,
            detected_ms=detected_ms,
        )


async def microstructure_aggregate_loop(
    *,
    settings: Any,
    state: CollectorState,
    events: asyncio.Queue[Any],
    persistence_rows: asyncio.Queue[FinalizedMicrostructureRow],
    live_cache: Any = None,
) -> None:
    """Finalize causally, publish Redis, and enqueue history without blocking."""

    pending: list[Any] = []
    now_ms = current_utc_epoch_ms()
    next_boundary_ms = (now_ms // 1000 + 1) * 1000
    last_finalized_boundary_ms: Optional[int] = None

    while True:
        deadline_ms = next_boundary_ms + settings.BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS
        timeout_seconds = max(
            0.0,
            (deadline_ms - current_utc_epoch_ms()) / 1000.0,
        )
        try:
            event = await asyncio.wait_for(events.get(), timeout=timeout_seconds)
            _stage_pending_event(
                event=event,
                pending=pending,
                state=state,
                last_finalized_boundary_ms=last_finalized_boundary_ms,
                detected_ms=current_utc_epoch_ms(),
            )
        except asyncio.TimeoutError:
            pass

        now_ms = current_utc_epoch_ms()
        if now_ms < deadline_ms:
            continue

        while (
            next_boundary_ms + settings.BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS
            <= now_ms
        ):
            # A Redis attempt or scheduler delay can cross another boundary.
            # Re-drain before *every* catch-up row so events received during
            # those awaits are included in their own causal interval.
            _drain_events_into_pending(
                events=events,
                pending=pending,
                state=state,
                last_finalized_boundary_ms=last_finalized_boundary_ms,
                detected_ms=now_ms,
            )
            finalization_ms = current_utc_epoch_ms()
            sample_jitter_ms = max(0, finalization_ms - next_boundary_ms)
            row = finalize_boundary(
                state,
                pending,
                next_boundary_ms,
                sample_jitter_ms=sample_jitter_ms,
            )
            expected_sample_second_ms = next_boundary_ms - 1000
            if row.get("sample_second_ms") != expected_sample_second_ms:
                raise RuntimeError(
                    "microstructure state returned the wrong sample key: "
                    f"expected {expected_sample_second_ms}, "
                    f"got {row.get('sample_second_ms')!r}"
                )
            last_finalized_boundary_ms = next_boundary_ms
            await update_microstructure_live_cache(
                live_cache,
                row=row,
                received_ms=finalization_ms,
            )
            enqueue_finalized_microstructure_row(
                persistence_rows,
                row=row,
                received_ms=finalization_ms,
            )

            next_boundary_ms += 1000
            now_ms = current_utc_epoch_ms()


class MicrostructureRuntime:
    """Own the optional microstructure readers, queue, state, and writer."""

    def __init__(
        self,
        settings: Any,
        pool: Any,
        *,
        live_cache: Any = None,
    ) -> None:
        self.settings = settings
        self.pool = pool
        self.live_cache = live_cache
        self.events: asyncio.Queue[Any] = asyncio.Queue(
            maxsize=settings.BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS
        )
        self.persistence_rows: asyncio.Queue[FinalizedMicrostructureRow] = (
            asyncio.Queue(
                maxsize=(
                    settings.BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS
                )
            )
        )
        self.sink = MicrostructureEventSink(
            self.events,
            expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
        )
        self.state = CollectorState(symbol=settings.BINANCE_FUTURES_SYMBOL)
        self.write_gate = MicrostructureWriteGate(
            warning_bytes=(
                settings.BINANCE_MICROSTRUCTURE_WARN_RELATION_MB
                * BYTES_PER_MEBIBYTE
            ),
            maximum_bytes=(
                settings.BINANCE_MICROSTRUCTURE_MAX_RELATION_MB
                * BYTES_PER_MEBIBYTE
            ),
        )

    def _producer_coroutines(self) -> list[Any]:
        return [
            spot_microstructure_reader_loop(self.settings, self.sink),
            futures_depth_microstructure_reader_loop(self.settings, self.sink),
            futures_liquidation_microstructure_reader_loop(
                self.settings,
                self.sink,
            ),
            microstructure_aggregate_loop(
                settings=self.settings,
                state=self.state,
                events=self.events,
                persistence_rows=self.persistence_rows,
                live_cache=self.live_cache,
            ),
        ]

    async def _persistence_supervisor(self) -> None:
        """Keep one queue consumer alive across producer-group restarts."""

        attempt = 0
        while True:
            try:
                await microstructure_persistence_loop(
                    pool=self.pool,
                    settings=self.settings,
                    persistence_rows=self.persistence_rows,
                    write_gate=self.write_gate,
                )
                raise RuntimeError(
                    "Binance microstructure persistence stopped unexpectedly"
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                delay = reconnect_delay_seconds(attempt)
                LOGGER.exception(
                    "binance_microstructure_persistence_restarting",
                    extra={
                        "event": (
                            "binance_microstructure_persistence_restarting"
                        ),
                        "attempt": attempt,
                        "delay_seconds": round(delay, 3),
                        "error": repr(exc),
                        "queued_rows": self.persistence_rows.qsize(),
                    },
                )
                await asyncio.sleep(delay)

    def _discard_queued_persistence_rows(self, *, reason: str) -> int:
        discarded_samples: list[Any] = []
        while True:
            try:
                finalized = self.persistence_rows.get_nowait()
            except asyncio.QueueEmpty:
                break
            discarded_samples.append(
                finalized.row.get("sample_second_ms")
            )
            self.persistence_rows.task_done()

        discarded_count = len(discarded_samples)
        if discarded_count:
            integer_samples = [
                sample
                for sample in discarded_samples
                if isinstance(sample, int) and not isinstance(sample, bool)
            ]
            LOGGER.error(
                "binance_microstructure_persistence_rows_discarded",
                extra={
                    "event": (
                        "binance_microstructure_persistence_rows_discarded"
                    ),
                    "reason": reason,
                    "discarded_row_count": discarded_count,
                    "oldest_sample_second_ms": (
                        min(integer_samples) if integer_samples else None
                    ),
                    "newest_sample_second_ms": (
                        max(integer_samples) if integer_samples else None
                    ),
                },
            )
        return discarded_count

    async def _stop_persistence(
        self,
        persistence_task: asyncio.Task[Any],
    ) -> int:
        """Drain briefly after producers stop, then cancel and account loss."""

        timed_out = False
        try:
            await asyncio.wait_for(
                self.persistence_rows.join(),
                timeout=MICROSTRUCTURE_PERSIST_DRAIN_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            timed_out = True
            LOGGER.error(
                "binance_microstructure_persistence_drain_timed_out",
                extra={
                    "event": (
                        "binance_microstructure_persistence_drain_timed_out"
                    ),
                    "timeout_seconds": (
                        MICROSTRUCTURE_PERSIST_DRAIN_TIMEOUT_SECONDS
                    ),
                    "queued_rows": self.persistence_rows.qsize(),
                },
            )
        finally:
            if not persistence_task.done():
                persistence_task.cancel()
            await asyncio.gather(persistence_task, return_exceptions=True)

        return self._discard_queued_persistence_rows(
            reason="shutdown_timeout" if timed_out else "shutdown",
        )

    def _rebase_after_worker_failure(self) -> tuple[int, int]:
        """Discard unfinished inputs while preserving finalized history rows."""

        discarded_events = 0
        latest_futures_trade_transition: Any = None
        while True:
            try:
                event = self.events.get_nowait()
            except asyncio.QueueEmpty:
                break
            discarded_events += 1
            if (
                getattr(event, "kind", None)
                in ("connection_opened", "connection_closed")
                and getattr(event, "payload", None) == "futures_trade"
                and (
                    latest_futures_trade_transition is None
                    or event > latest_futures_trade_transition
                )
            ):
                latest_futures_trade_transition = event
        reset_ms = current_utc_epoch_ms()
        self.state.reset_after_runtime_gap(reset_ms)
        # The core aggTrade reader is independent of these optional workers and
        # is not restarted here. Reconcile its latest queued transition so a
        # drained `opened` marker cannot leave health false until the socket's
        # next proactive reconnect (about 23h50), and a `closed` marker cannot
        # leave a stale true state.
        if latest_futures_trade_transition is not None:
            self.state.apply_event(latest_futures_trade_transition)
        self.sink.reset_after_runtime_gap()
        return reset_ms, discarded_events

    async def run(self) -> None:
        """Supervise optional workers without propagating failures to core feeds."""

        restart_attempt = 0
        persistence_task = asyncio.create_task(
            self._persistence_supervisor()
        )
        try:
            while True:
                tasks = [
                    asyncio.create_task(producer)
                    for producer in self._producer_coroutines()
                ]
                restarting = False
                try:
                    done, _ = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    failure: Optional[Exception] = None
                    for task in done:
                        if task.cancelled():
                            continue
                        exception = task.exception()
                        if isinstance(exception, Exception):
                            failure = exception
                            break
                    if failure is None:
                        failure = RuntimeError(
                            "a Binance microstructure producer stopped "
                            "unexpectedly"
                        )
                    raise failure
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    restarting = True
                    restart_attempt += 1
                    delay = reconnect_delay_seconds(restart_attempt)
                    LOGGER.exception(
                        "binance_microstructure_runtime_restarting",
                        extra={
                            "event": (
                                "binance_microstructure_runtime_restarting"
                            ),
                            "attempt": restart_attempt,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                    )
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                    if restarting:
                        # The aggregate worker's local heap disappears with
                        # its task. Drain unfinished inputs and reset bucket
                        # totals without touching independently finalized rows.
                        reset_ms, discarded_events = (
                            self._rebase_after_worker_failure()
                        )
                        LOGGER.warning(
                            "binance_microstructure_runtime_state_rebased",
                            extra={
                                "event": (
                                    "binance_microstructure_runtime_state_rebased"
                                ),
                                "received_ms": reset_ms,
                                "discarded_events": discarded_events,
                                "queued_persistence_rows": (
                                    self.persistence_rows.qsize()
                                ),
                            },
                        )

                await asyncio.sleep(delay)
        finally:
            await self._stop_persistence(persistence_task)


def create_microstructure_runtime(
    settings: Any,
    pool: Any,
    *,
    live_cache: Any = None,
) -> MicrostructureRuntime:
    return MicrostructureRuntime(
        settings,
        pool,
        live_cache=live_cache,
    )
