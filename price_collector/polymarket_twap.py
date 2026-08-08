import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Union
from uuid import UUID, uuid4

import websockets

from price_collector import db
from price_collector.collector import reconnect_delay_seconds
from price_collector.market import MarketWindow, market_for_sample_second
from price_collector.live_cache import LIVE_CACHE_WRITE_ERRORS, TWAP_LIVE_KEY
from price_collector.raw_capture import (
    POSTGRES_BIGINT_MAX,
    POSTGRES_NUMERIC_38_18_LIMIT,
)


LOGGER = logging.getLogger("price_collector.polymarket_twap")
TWAP_PROVIDER_CODE = "polymarket_chainlink_twap_rtds"
TWAP_INSTRUMENT_SYMBOL = "BTCUSD_TWAP_30S"
TWAP_TOPIC = "crypto_prices_twap_thirty"
TWAP_SYMBOL = "btc/usd"
TWAP_WINDOW_SECONDS = 30
TWAP_E18_DECIMAL_PLACES = 18
TWAP_PING_SECONDS = 5.0
TWAP_MAX_SOURCE_AGE_MS = 10_000
TWAP_MAX_FUTURE_SKEW_MS = 2_000
TWAP_FINAL_CONTROL_RESERVE = 3
TWAP_MAX_PROVIDER_EVENT_MS = (
    ((POSTGRES_BIGINT_MAX - 300_000) // 300_000) * 300_000
    + 299_999
)


class RtdsTwapParseError(ValueError):
    pass


class RtdsTwapAcceptedEventIdleTimeout(TimeoutError):
    pass


class RtdsTwapPersistenceBackpressure(RuntimeError):
    pass


class RtdsTwapRemoteClose(ConnectionError):
    pass


@dataclass(frozen=True)
class PolymarketTwapTick:
    symbol: str
    window_s: int
    price_e18: int
    price: Decimal
    provider_event_ms: int
    provider_message_ms: Optional[int]


@dataclass(frozen=True)
class PolymarketTwapEvent:
    instrument_id: int
    connection_id: UUID
    receive_sequence: int
    topic: str
    symbol: str
    window_s: int
    provider_event_ms: int
    provider_message_ms: Optional[int]
    received_wall_ns: int
    received_monotonic_ns: int
    price_e18: int
    price: Decimal
    sample_second_ms: int
    window: MarketWindow


@dataclass(frozen=True)
class PolymarketTwapSessionStart:
    connection_id: UUID
    topic: str
    symbol: str
    window_s: int
    connected_wall_ns: int
    connected_monotonic_ns: int
    subscribed_wall_ns: int
    subscribed_monotonic_ns: int
    persisted: Optional[asyncio.Event] = None


@dataclass(frozen=True)
class PolymarketTwapSessionFinish:
    connection_id: UUID
    disconnected_wall_ns: int
    disconnected_monotonic_ns: int
    close_reason: str
    messages_received_total: int
    messages_accepted_total: int
    parse_errors_total: int
    last_receive_sequence: int
    last_accepted_received_ms: Optional[int]
    last_provider_event_ms: Optional[int]


@dataclass(frozen=True)
class PolymarketTwapGap:
    connection_id: UUID
    detected_wall_ns: int
    detected_monotonic_ns: int
    reason: str
    idle_timeout_ms: Optional[int]
    last_accepted_received_ms: Optional[int]
    last_provider_event_ms: Optional[int]
    messages_received_total: int
    messages_accepted_total: int
    parse_errors_total: int


PolymarketTwapPersistenceRecord = Union[
    PolymarketTwapEvent,
    PolymarketTwapSessionStart,
    PolymarketTwapSessionFinish,
    PolymarketTwapGap,
]


def validate_twap_runtime_identity(settings: Any) -> None:
    expected = {
        "POLYMARKET_TWAP_PROVIDER_CODE": TWAP_PROVIDER_CODE,
        "POLYMARKET_TWAP_SYMBOL": TWAP_INSTRUMENT_SYMBOL,
        "POLYMARKET_TWAP_RTD_SYMBOL": TWAP_SYMBOL,
        "POLYMARKET_TWAP_TOPIC": TWAP_TOPIC,
        "POLYMARKET_TWAP_WINDOW_SECONDS": TWAP_WINDOW_SECONDS,
    }
    for field_name, expected_value in expected.items():
        actual_value = getattr(settings, field_name, None)
        if actual_value != expected_value:
            raise ValueError(
                f"{field_name} must be {expected_value!r} for the enabled "
                "BTC five-minute TWAP collector"
            )


def build_polymarket_twap_subscription(settings: Any) -> dict[str, Any]:
    return {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": settings.POLYMARKET_TWAP_TOPIC,
                "type": "update",
                "filters": json.dumps(
                    {"symbol": settings.POLYMARKET_TWAP_RTD_SYMBOL},
                    separators=(",", ":"),
                ),
            }
        ],
    }


def _parse_positive_int(raw_value: Any, field_name: str) -> int:
    if isinstance(raw_value, bool):
        raise RtdsTwapParseError(f"{field_name} must be a positive integer")
    if isinstance(raw_value, int):
        value = raw_value
    elif isinstance(raw_value, Decimal):
        if raw_value != raw_value.to_integral_value():
            raise RtdsTwapParseError(
                f"{field_name} must be a positive integer"
            )
        value = int(raw_value)
    elif isinstance(raw_value, str) and raw_value.isdecimal():
        value = int(raw_value)
    else:
        raise RtdsTwapParseError(f"{field_name} must be a positive integer")
    if value <= 0:
        raise RtdsTwapParseError(f"{field_name} must be positive")
    return value


def _parse_timestamp_ms(raw_value: Any, field_name: str) -> int:
    value = _parse_positive_int(raw_value, field_name)
    if value > POSTGRES_BIGINT_MAX:
        raise RtdsTwapParseError(f"{field_name} exceeds PostgreSQL BIGINT")
    return value


def _decimal_from_e18(price_e18: int) -> Decimal:
    """Build an exact Decimal without applying the ambient Decimal context."""

    digits = str(price_e18)
    if len(digits) <= TWAP_E18_DECIMAL_PLACES:
        text = "0." + digits.zfill(TWAP_E18_DECIMAL_PLACES)
    else:
        text = (
            digits[:-TWAP_E18_DECIMAL_PLACES]
            + "."
            + digits[-TWAP_E18_DECIMAL_PLACES:]
        )
    try:
        return Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover - defensive
        raise RtdsTwapParseError(
            "invalid RTDS payload.full_accuracy_value"
        ) from exc


def _validate_twap_price_for_storage(price: Decimal) -> None:
    if not price.is_finite() or price <= 0:
        raise RtdsTwapParseError("RTDS TWAP price must be finite and positive")
    if price >= POSTGRES_NUMERIC_38_18_LIMIT:
        raise RtdsTwapParseError(
            "RTDS TWAP price exceeds PostgreSQL NUMERIC(38,18)"
        )
    _sign, digits, exponent = price.as_tuple()
    if exponent < -TWAP_E18_DECIMAL_PLACES:
        extra_places = -TWAP_E18_DECIMAL_PLACES - exponent
        if extra_places > len(digits) or any(digits[-extra_places:]):
            raise RtdsTwapParseError(
                "RTDS TWAP price exceeds PostgreSQL NUMERIC(38,18) scale"
            )


def parse_polymarket_twap_message(
    message: Mapping[str, Any],
    *,
    expected_topic: str = TWAP_TOPIC,
    expected_symbol: str = TWAP_SYMBOL,
    expected_window_s: int = TWAP_WINDOW_SECONDS,
) -> PolymarketTwapTick:
    topic = message.get("topic")
    if topic != expected_topic:
        raise RtdsTwapParseError(
            f"unexpected TWAP topic: expected {expected_topic!r}, got {topic!r}"
        )
    message_type = message.get("type")
    if message_type != "update":
        raise RtdsTwapParseError(f"non-update TWAP message: {message_type!r}")

    payload = message.get("payload")
    if not isinstance(payload, Mapping):
        raise RtdsTwapParseError("RTDS TWAP payload must be an object")

    symbol = payload.get("symbol")
    if symbol != expected_symbol:
        raise RtdsTwapParseError(
            f"unexpected TWAP symbol: expected {expected_symbol!r}, got {symbol!r}"
        )

    window_s = _parse_positive_int(payload.get("window_s"), "payload.window_s")
    if window_s != expected_window_s:
        raise RtdsTwapParseError(
            f"unexpected TWAP window_s: expected {expected_window_s}, got {window_s}"
        )

    raw_e18 = payload.get("full_accuracy_value")
    if raw_e18 is None:
        raise RtdsTwapParseError(
            "RTDS TWAP payload missing payload.full_accuracy_value"
        )
    price_e18 = _parse_positive_int(
        raw_e18,
        "payload.full_accuracy_value",
    )
    price = _decimal_from_e18(price_e18)
    _validate_twap_price_for_storage(price)

    provider_event_ms = _parse_timestamp_ms(
        payload.get("timestamp"),
        "payload.timestamp",
    )
    if provider_event_ms > TWAP_MAX_PROVIDER_EVENT_MS:
        raise RtdsTwapParseError(
            "RTDS TWAP payload.timestamp exceeds the PostgreSQL market-window range"
        )
    provider_window = market_for_sample_second(
        (provider_event_ms // 1_000) * 1_000
    )
    try:
        db.epoch_ms_to_utc_datetime(provider_window.market_end_ms)
    except OverflowError as exc:
        raise RtdsTwapParseError(
            "RTDS TWAP payload.timestamp exceeds the application datetime range"
        ) from exc

    provider_message_ms = None
    raw_message_ms = message.get("timestamp")
    if raw_message_ms is not None:
        try:
            provider_message_ms = _parse_timestamp_ms(
                raw_message_ms,
                "message.timestamp",
            )
        except RtdsTwapParseError:
            # Publisher time is diagnostic. A malformed value must not discard
            # an otherwise exact Chainlink observation.
            provider_message_ms = None

    return PolymarketTwapTick(
        symbol=expected_symbol,
        window_s=window_s,
        price_e18=price_e18,
        price=price,
        provider_event_ms=provider_event_ms,
        provider_message_ms=provider_message_ms,
    )


def validate_twap_timestamp_plausibility(
    tick: PolymarketTwapTick,
    *,
    received_ms: int,
    accepted_event_idle_timeout_ms: int,
) -> None:
    if (
        not isinstance(received_ms, int)
        or isinstance(received_ms, bool)
        or received_ms <= 0
    ):
        raise ValueError("received_ms must be a positive integer")
    if (
        not isinstance(accepted_event_idle_timeout_ms, int)
        or isinstance(accepted_event_idle_timeout_ms, bool)
        or accepted_event_idle_timeout_ms <= 0
    ):
        raise ValueError(
            "accepted_event_idle_timeout_ms must be a positive integer"
        )

    source_age_ms = received_ms - tick.provider_event_ms
    maximum_age_ms = min(
        accepted_event_idle_timeout_ms,
        TWAP_MAX_SOURCE_AGE_MS,
    )
    if source_age_ms > maximum_age_ms:
        raise RtdsTwapParseError(
            "RTDS TWAP payload.timestamp is stale: "
            f"source_age_ms={source_age_ms}, maximum_age_ms={maximum_age_ms}"
        )
    if source_age_ms < -TWAP_MAX_FUTURE_SKEW_MS:
        raise RtdsTwapParseError(
            "RTDS TWAP payload.timestamp is materially future-dated: "
            f"future_skew_ms={-source_age_ms}, "
            f"maximum_future_skew_ms={TWAP_MAX_FUTURE_SKEW_MS}"
        )


def build_polymarket_twap_event(
    tick: PolymarketTwapTick,
    *,
    instrument_id: int,
    connection_id: UUID,
    receive_sequence: int,
    topic: str,
    received_wall_ns: int,
    received_monotonic_ns: int,
) -> PolymarketTwapEvent:
    sample_second_ms = (tick.provider_event_ms // 1_000) * 1_000
    return PolymarketTwapEvent(
        instrument_id=instrument_id,
        connection_id=connection_id,
        receive_sequence=receive_sequence,
        topic=topic,
        symbol=tick.symbol,
        window_s=tick.window_s,
        provider_event_ms=tick.provider_event_ms,
        provider_message_ms=tick.provider_message_ms,
        received_wall_ns=received_wall_ns,
        received_monotonic_ns=received_monotonic_ns,
        price_e18=tick.price_e18,
        price=tick.price,
        sample_second_ms=sample_second_ms,
        window=market_for_sample_second(sample_second_ms),
    )


async def update_twap_live_cache(
    live_cache: Any,
    event: PolymarketTwapEvent,
) -> bool:
    if live_cache is None:
        return False
    try:
        await live_cache.set_price(
            TWAP_LIVE_KEY,
            value=event.price,
            source_timestamp_ms=event.provider_event_ms,
            received_ms=event.received_wall_ns // 1_000_000,
        )
    except asyncio.CancelledError:
        raise
    except LIVE_CACHE_WRITE_ERRORS as exc:
        LOGGER.warning(
            "polymarket_twap_live_cache_write_failed",
            extra={
                "event": "polymarket_twap_live_cache_write_failed",
                "key": TWAP_LIVE_KEY,
                "provider_event_ms": event.provider_event_ms,
                "error": repr(exc),
            },
        )
        return False
    except Exception as exc:
        # A cache implementation failure is non-authoritative and must not
        # suppress the durable event that follows it.
        LOGGER.exception(
            "polymarket_twap_live_cache_write_failed",
            extra={
                "event": "polymarket_twap_live_cache_write_failed",
                "key": TWAP_LIVE_KEY,
                "provider_event_ms": event.provider_event_ms,
                "error": repr(exc),
            },
        )
        return False
    return True


async def _persist_twap_record(pool: Any, record: PolymarketTwapPersistenceRecord) -> None:
    if isinstance(record, PolymarketTwapEvent):
        await db.record_polymarket_twap_event(
            pool,
            instrument_id=record.instrument_id,
            connection_id=record.connection_id,
            receive_sequence=record.receive_sequence,
            topic=record.topic,
            symbol=record.symbol,
            window_s=record.window_s,
            provider_event_ms=record.provider_event_ms,
            provider_message_ms=record.provider_message_ms,
            received_wall_ns=record.received_wall_ns,
            received_monotonic_ns=record.received_monotonic_ns,
            price_e18=record.price_e18,
            price=record.price,
            sample_second_ms=record.sample_second_ms,
            window=record.window,
        )
        return
    if isinstance(record, PolymarketTwapSessionStart):
        await db.start_polymarket_twap_session(
            pool,
            connection_id=record.connection_id,
            topic=record.topic,
            symbol=record.symbol,
            window_s=record.window_s,
            connected_wall_ns=record.connected_wall_ns,
            connected_monotonic_ns=record.connected_monotonic_ns,
            subscribed_wall_ns=record.subscribed_wall_ns,
            subscribed_monotonic_ns=record.subscribed_monotonic_ns,
        )
        return
    if isinstance(record, PolymarketTwapSessionFinish):
        await db.finish_polymarket_twap_session(
            pool,
            connection_id=record.connection_id,
            disconnected_wall_ns=record.disconnected_wall_ns,
            disconnected_monotonic_ns=record.disconnected_monotonic_ns,
            close_reason=record.close_reason,
            messages_received_total=record.messages_received_total,
            messages_accepted_total=record.messages_accepted_total,
            parse_errors_total=record.parse_errors_total,
            last_receive_sequence=record.last_receive_sequence,
            last_accepted_received_ms=record.last_accepted_received_ms,
            last_provider_event_ms=record.last_provider_event_ms,
        )
        return
    if isinstance(record, PolymarketTwapGap):
        await db.record_polymarket_twap_gap(
            pool,
            connection_id=record.connection_id,
            detected_wall_ns=record.detected_wall_ns,
            detected_monotonic_ns=record.detected_monotonic_ns,
            reason=record.reason,
            idle_timeout_ms=record.idle_timeout_ms,
            last_accepted_received_ms=record.last_accepted_received_ms,
            last_provider_event_ms=record.last_provider_event_ms,
            messages_received_total=record.messages_received_total,
            messages_accepted_total=record.messages_accepted_total,
            parse_errors_total=record.parse_errors_total,
        )
        return
    raise TypeError(f"unsupported TWAP persistence record: {type(record)!r}")


async def polymarket_twap_persistence_worker(
    *,
    pool: Any,
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
) -> None:
    """Persist records in receive order without blocking the socket reader."""

    while True:
        record = await records.get()
        attempt = 0
        try:
            while True:
                try:
                    await _persist_twap_record(pool, record)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    attempt += 1
                    delay = reconnect_delay_seconds(attempt)
                    LOGGER.exception(
                        "polymarket_twap_persistence_retry_scheduled",
                        extra={
                            "event": "polymarket_twap_persistence_retry_scheduled",
                            "record_type": type(record).__name__,
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "queued_records": records.qsize(),
                            "error": repr(exc),
                        },
                    )
                    await asyncio.sleep(delay)
                else:
                    if (
                        isinstance(record, PolymarketTwapSessionStart)
                        and record.persisted is not None
                    ):
                        record.persisted.set()
                    break
        finally:
            records.task_done()


async def polymarket_twap_ping_loop(websocket: Any) -> None:
    while True:
        await asyncio.sleep(TWAP_PING_SECONDS)
        await websocket.send("PING")


async def _close_for_backpressure(websocket: Any) -> None:
    close = getattr(websocket, "close", None)
    if close is None:
        return
    try:
        await close(code=1013, reason="TWAP persistence queue saturated")
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.exception(
            "polymarket_twap_backpressure_close_failed",
            extra={"event": "polymarket_twap_backpressure_close_failed"},
        )


async def _enqueue_twap_event(
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
    event: PolymarketTwapEvent,
    *,
    websocket: Any,
    event_capacity: Optional[int] = None,
) -> None:
    capacity = records.maxsize if event_capacity is None else event_capacity
    if capacity <= 0:
        raise ValueError("TWAP event queue capacity must be positive")
    if records.qsize() < capacity:
        records.put_nowait(event)
        return

    LOGGER.error(
        "polymarket_twap_persistence_queue_saturated",
        extra={
            "event": "polymarket_twap_persistence_queue_saturated",
            "connection_id": event.connection_id,
            "receive_sequence": event.receive_sequence,
            "provider_event_ms": event.provider_event_ms,
            "queued_records": records.qsize(),
            "event_capacity": capacity,
            "queue_max_records": records.maxsize,
            "coverage_gap": True,
        },
    )

    # Runtime queues reserve three positions for this already-received event,
    # its gap, and its session finish. Store the event before awaiting socket
    # close so cancellation cannot lose it.
    try:
        records.put_nowait(event)
    except asyncio.QueueFull as exc:
        LOGGER.critical(
            "polymarket_twap_persistence_reserve_exhausted",
            extra={
                "event": "polymarket_twap_persistence_reserve_exhausted",
                "connection_id": event.connection_id,
                "receive_sequence": event.receive_sequence,
                "queued_records": records.qsize(),
                "queue_max_records": records.maxsize,
                "coverage_gap": True,
            },
        )
        raise RtdsTwapPersistenceBackpressure(
            "TWAP persistence final-control reserve was exhausted"
        ) from exc
    await _close_for_backpressure(websocket)
    raise RtdsTwapPersistenceBackpressure(
        "TWAP persistence queue reached its configured capacity"
    )


async def _deliver_and_enqueue_twap_event(
    *,
    live_cache: Any,
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
    event: PolymarketTwapEvent,
    websocket: Any,
    event_capacity: int,
) -> None:
    """Keep the Redis-attempt -> durable-queue handoff cancellation-safe."""

    live_attempt = asyncio.create_task(
        update_twap_live_cache(live_cache, event)
    )
    try:
        await asyncio.shield(live_attempt)
    except asyncio.CancelledError as cancellation:
        # Shield keeps the already-started Redis attempt alive. Redis has a
        # configured socket timeout, so wait for that bounded attempt before
        # exposing the event to the PostgreSQL writer, then preserve the
        # original planned cancellation.
        await asyncio.gather(live_attempt, return_exceptions=True)
        try:
            await _enqueue_twap_event(
                records,
                event,
                websocket=websocket,
                event_capacity=event_capacity,
            )
        except RtdsTwapPersistenceBackpressure:
            # The event was placed in the reserved slot before this exception.
            # Planned cancellation still owns the session close reason.
            pass
        raise cancellation

    await _enqueue_twap_event(
        records,
        event,
        websocket=websocket,
        event_capacity=event_capacity,
    )


async def _enqueue_twap_control_record(
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
    record: PolymarketTwapPersistenceRecord,
) -> None:
    """Never discard session or gap evidence at a full event queue."""

    await records.put(record)


def _offer_twap_final_control_record(
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
    record: PolymarketTwapPersistenceRecord,
) -> bool:
    try:
        records.put_nowait(record)
    except asyncio.QueueFull:
        LOGGER.critical(
            "polymarket_twap_final_control_not_enqueued",
            extra={
                "event": "polymarket_twap_final_control_not_enqueued",
                "record_type": type(record).__name__,
                "queued_records": records.qsize(),
                "queue_max_records": records.maxsize,
                "coverage_gap": True,
            },
        )
        return False
    return True


def _offer_twap_session_final_records(
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
    *,
    connection_id: UUID,
    close_reason: str,
    gap_reason: Optional[str],
    gap_idle_timeout_ms: Optional[int],
    messages_received_total: int,
    messages_accepted_total: int,
    parse_errors_total: int,
    receive_sequence: int,
    last_accepted_received_ms: Optional[int],
    last_provider_event_ms: Optional[int],
) -> bool:
    detected_wall_ns = time.time_ns()
    detected_monotonic_ns = time.monotonic_ns()
    if gap_reason is not None:
        gap_offered = _offer_twap_final_control_record(
            records,
            PolymarketTwapGap(
                connection_id=connection_id,
                detected_wall_ns=detected_wall_ns,
                detected_monotonic_ns=detected_monotonic_ns,
                reason=gap_reason,
                idle_timeout_ms=gap_idle_timeout_ms,
                last_accepted_received_ms=last_accepted_received_ms,
                last_provider_event_ms=last_provider_event_ms,
                messages_received_total=messages_received_total,
                messages_accepted_total=messages_accepted_total,
                parse_errors_total=parse_errors_total,
            ),
        )
        if not gap_offered:
            # Leave the durable session open. Startup recovery will create an
            # orphan gap before closing it; a finish without its gap would
            # incorrectly hide the coverage interval.
            return False
    return _offer_twap_final_control_record(
        records,
        PolymarketTwapSessionFinish(
            connection_id=connection_id,
            disconnected_wall_ns=detected_wall_ns,
            disconnected_monotonic_ns=detected_monotonic_ns,
            close_reason=close_reason,
            messages_received_total=messages_received_total,
            messages_accepted_total=messages_accepted_total,
            parse_errors_total=parse_errors_total,
            last_receive_sequence=receive_sequence,
            last_accepted_received_ms=last_accepted_received_ms,
            last_provider_event_ms=last_provider_event_ms,
        ),
    )


def _connection_error_reason(exc: Exception) -> tuple[str, str, Optional[int]]:
    if isinstance(exc, RtdsTwapAcceptedEventIdleTimeout):
        return (
            "accepted_event_idle_timeout",
            "accepted_event_idle_timeout",
            getattr(exc, "idle_timeout_ms", None),
        )
    if isinstance(exc, RtdsTwapPersistenceBackpressure):
        return "persistence_backpressure", "persistence_backpressure", None
    if isinstance(exc, RtdsTwapRemoteClose):
        return "remote_close", "remote_close", None
    return "error", "connection_error", None


async def recover_orphaned_polymarket_twap_sessions(pool: Any) -> int:
    """Turn hard-crashed open sessions into explicit no-replay gaps."""

    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                session.connection_id,
                session.subscribed_monotonic_ns,
                GREATEST(
                    session.messages_received_total,
                    COALESCE(event_summary.last_receive_sequence, 0)
                )::BIGINT AS messages_received_total,
                GREATEST(
                    session.messages_accepted_total,
                    COALESCE(event_summary.event_count, 0)
                )::BIGINT AS messages_accepted_total,
                session.parse_errors_total,
                GREATEST(
                    session.last_receive_sequence,
                    COALESCE(event_summary.last_receive_sequence, 0)
                )::BIGINT AS last_receive_sequence,
                COALESCE(
                    session.last_accepted_received_ms,
                    (latest_event.received_wall_ns / 1000000)::BIGINT
                ) AS last_accepted_received_ms,
                COALESCE(
                    session.last_provider_event_ms,
                    latest_event.provider_event_ms
                ) AS last_provider_event_ms
            FROM polymarket_twap_sessions session
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*)::BIGINT AS event_count,
                    MAX(event.receive_sequence)::BIGINT
                        AS last_receive_sequence
                FROM polymarket_twap_events event
                WHERE event.connection_id = session.connection_id
            ) event_summary ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    event.received_wall_ns,
                    event.provider_event_ms
                FROM polymarket_twap_events event
                WHERE event.connection_id = session.connection_id
                ORDER BY event.receive_sequence DESC
                LIMIT 1
            ) latest_event ON TRUE
            WHERE session.topic = 'crypto_prices_twap_thirty'
              AND session.symbol = 'btc/usd'
              AND session.window_s = 30
              AND session.disconnected_wall_ns IS NULL
            ORDER BY session.connected_wall_ns ASC, session.connection_id ASC
            """
        )

    recovered = 0
    for row in rows:
        connection_id = row["connection_id"]
        detected_wall_ns = time.time_ns()
        # Monotonic clocks restart at boot and cannot be compared across host
        # reboots. Preserve the causal schema invariant without pretending the
        # cross-boot delta is meaningful; wall time and the explicit orphan
        # reason define the recoverable coverage interval.
        detected_monotonic_ns = max(
            time.monotonic_ns(),
            int(row["subscribed_monotonic_ns"]),
        )
        messages_received_total = int(row["messages_received_total"] or 0)
        messages_accepted_total = int(row["messages_accepted_total"] or 0)
        parse_errors_total = int(row["parse_errors_total"] or 0)
        last_receive_sequence = int(row["last_receive_sequence"] or 0)
        last_accepted_received_ms = row["last_accepted_received_ms"]
        last_provider_event_ms = row["last_provider_event_ms"]

        # Gap first, finish second. If either write fails, runtime startup
        # fails and retries; it never starts a new socket over ambiguous state.
        await db.record_polymarket_twap_gap(
            pool,
            connection_id=connection_id,
            detected_wall_ns=detected_wall_ns,
            detected_monotonic_ns=detected_monotonic_ns,
            reason="orphaned_session_recovered",
            idle_timeout_ms=None,
            last_accepted_received_ms=last_accepted_received_ms,
            last_provider_event_ms=last_provider_event_ms,
            messages_received_total=messages_received_total,
            messages_accepted_total=messages_accepted_total,
            parse_errors_total=parse_errors_total,
        )
        await db.finish_polymarket_twap_session(
            pool,
            connection_id=connection_id,
            disconnected_wall_ns=detected_wall_ns,
            disconnected_monotonic_ns=detected_monotonic_ns,
            close_reason="orphaned_session_recovered",
            messages_received_total=messages_received_total,
            messages_accepted_total=messages_accepted_total,
            parse_errors_total=parse_errors_total,
            last_receive_sequence=last_receive_sequence,
            last_accepted_received_ms=last_accepted_received_ms,
            last_provider_event_ms=last_provider_event_ms,
        )
        recovered += 1
        LOGGER.warning(
            "polymarket_twap_orphaned_session_recovered",
            extra={
                "event": "polymarket_twap_orphaned_session_recovered",
                "connection_id": connection_id,
                "messages_received_total": messages_received_total,
                "messages_accepted_total": messages_accepted_total,
                "last_provider_event_ms": last_provider_event_ms,
                "coverage_gap": True,
            },
        )
    return recovered


async def polymarket_twap_reader_loop(
    settings: Any,
    *,
    instrument_id: int,
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]",
    live_cache: Any = None,
) -> None:
    validate_twap_runtime_identity(settings)
    attempt = 0
    connection_sequence = 0
    idle_reconnects_total = 0

    while True:
        connection_id: Optional[UUID] = None
        session_started = False
        session_finalized = False
        connected_wall_ns: Optional[int] = None
        connected_monotonic_ns: Optional[int] = None
        messages_received_total = 0
        messages_accepted_total = 0
        parse_errors_total = 0
        receive_sequence = 0
        last_accepted_received_ms: Optional[int] = None
        last_provider_event_ms: Optional[int] = None
        close_reason = "error"
        gap_reason: Optional[str] = None
        gap_idle_timeout_ms: Optional[int] = None
        reconnect_error: Optional[Exception] = None

        try:
            LOGGER.info(
                "polymarket_twap_connecting",
                extra={
                    "event": "polymarket_twap_connecting",
                    "url": settings.POLYMARKET_RTDS_WS_URL,
                    "topic": settings.POLYMARKET_TWAP_TOPIC,
                    "rtd_symbol": settings.POLYMARKET_TWAP_RTD_SYMBOL,
                },
            )
            async with websockets.connect(
                settings.POLYMARKET_RTDS_WS_URL,
                ping_interval=None,
                close_timeout=10,
                max_queue=4_096,
            ) as websocket:
                connection_sequence += 1
                connection_id = uuid4()
                connected_wall_ns = time.time_ns()
                connected_monotonic_ns = time.monotonic_ns()

                subscription = build_polymarket_twap_subscription(settings)
                await websocket.send(json.dumps(subscription, separators=(",", ":")))
                subscribed_wall_ns = time.time_ns()
                subscribed_monotonic_ns = time.monotonic_ns()
                session_persisted = asyncio.Event()
                await _enqueue_twap_control_record(
                    records,
                    PolymarketTwapSessionStart(
                        connection_id=connection_id,
                        topic=settings.POLYMARKET_TWAP_TOPIC,
                        symbol=settings.POLYMARKET_TWAP_RTD_SYMBOL,
                        window_s=settings.POLYMARKET_TWAP_WINDOW_SECONDS,
                        connected_wall_ns=connected_wall_ns,
                        connected_monotonic_ns=connected_monotonic_ns,
                        subscribed_wall_ns=subscribed_wall_ns,
                        subscribed_monotonic_ns=subscribed_monotonic_ns,
                        persisted=session_persisted,
                    ),
                )
                # Do not call recv until the session row is committed. A hard
                # process kill after this barrier therefore leaves a durable
                # open session that startup recovery can turn into a gap.
                await session_persisted.wait()
                session_started = True

                idle_timeout_ms = (
                    settings.POLYMARKET_TWAP_ACCEPTED_EVENT_IDLE_TIMEOUT_MS
                )
                last_accepted_monotonic_ns = subscribed_monotonic_ns
                LOGGER.info(
                    "polymarket_twap_subscribed",
                    extra={
                        "event": "polymarket_twap_subscribed",
                        "topic": settings.POLYMARKET_TWAP_TOPIC,
                        "rtd_symbol": settings.POLYMARKET_TWAP_RTD_SYMBOL,
                        "window_s": settings.POLYMARKET_TWAP_WINDOW_SECONDS,
                        "connection_id": connection_id,
                        "connection_sequence": connection_sequence,
                        "accepted_event_idle_timeout_ms": idle_timeout_ms,
                    },
                )

                ping_task = asyncio.create_task(
                    polymarket_twap_ping_loop(websocket)
                )
                try:
                    iterator = websocket.__aiter__()
                    while True:
                        remaining_seconds = (
                            last_accepted_monotonic_ns
                            + idle_timeout_ms * 1_000_000
                            - time.monotonic_ns()
                        ) / 1_000_000_000
                        try:
                            if remaining_seconds <= 0:
                                raise asyncio.TimeoutError
                            raw_message = await asyncio.wait_for(
                                iterator.__anext__(),
                                timeout=remaining_seconds,
                            )
                        except StopAsyncIteration as exc:
                            raise RtdsTwapRemoteClose(
                                "Polymarket TWAP RTDS socket closed"
                            ) from exc
                        except asyncio.TimeoutError as exc:
                            timeout_monotonic_ns = time.monotonic_ns()
                            accepted_idle_ms = max(
                                0,
                                (
                                    timeout_monotonic_ns
                                    - last_accepted_monotonic_ns
                                )
                                // 1_000_000,
                            )
                            idle_reconnects_total += 1
                            LOGGER.warning(
                                "polymarket_twap_idle_reconnect_triggered",
                                extra={
                                    "event": "polymarket_twap_idle_reconnect_triggered",
                                    "connection_id": connection_id,
                                    "connection_sequence": connection_sequence,
                                    "idle_basis": "accepted_twap_tick",
                                    "idle_timeout_ms": idle_timeout_ms,
                                    "accepted_tick_idle_ms": accepted_idle_ms,
                                    "last_accepted_received_ms": last_accepted_received_ms,
                                    "last_provider_event_ms": last_provider_event_ms,
                                    "messages_received_total": messages_received_total,
                                    "messages_accepted_total": messages_accepted_total,
                                    "parse_errors_total": parse_errors_total,
                                    "idle_reconnects_total": idle_reconnects_total,
                                },
                            )
                            timeout_error = RtdsTwapAcceptedEventIdleTimeout(
                                "no accepted TWAP event for "
                                f"{accepted_idle_ms} ms"
                            )
                            timeout_error.idle_timeout_ms = idle_timeout_ms
                            raise timeout_error from exc

                        # These two stamps deliberately occur immediately after
                        # recv completes and before decoding or validation.
                        received_wall_ns = time.time_ns()
                        received_monotonic_ns = time.monotonic_ns()
                        receive_sequence += 1
                        messages_received_total += 1

                        if raw_message in (
                            "",
                            b"",
                            "PING",
                            "PONG",
                            b"PING",
                            b"PONG",
                        ):
                            continue

                        try:
                            message = json.loads(
                                raw_message,
                                parse_float=Decimal,
                            )
                            if not isinstance(message, Mapping):
                                raise RtdsTwapParseError(
                                    "RTDS TWAP message must be an object"
                                )
                            tick = parse_polymarket_twap_message(
                                message,
                                expected_topic=settings.POLYMARKET_TWAP_TOPIC,
                                expected_symbol=settings.POLYMARKET_TWAP_RTD_SYMBOL,
                                expected_window_s=(
                                    settings.POLYMARKET_TWAP_WINDOW_SECONDS
                                ),
                            )
                            validate_twap_timestamp_plausibility(
                                tick,
                                received_ms=received_wall_ns // 1_000_000,
                                accepted_event_idle_timeout_ms=idle_timeout_ms,
                            )
                        except (
                            UnicodeDecodeError,
                            json.JSONDecodeError,
                            RtdsTwapParseError,
                            TypeError,
                        ) as exc:
                            parse_errors_total += 1
                            LOGGER.warning(
                                "polymarket_twap_message_skipped",
                                extra={
                                    "event": "polymarket_twap_message_skipped",
                                    "connection_id": connection_id,
                                    "receive_sequence": receive_sequence,
                                    "error": str(exc),
                                },
                            )
                            continue

                        messages_accepted_total += 1
                        last_accepted_monotonic_ns = received_monotonic_ns
                        last_accepted_received_ms = received_wall_ns // 1_000_000
                        last_provider_event_ms = tick.provider_event_ms
                        attempt = 0

                        event = build_polymarket_twap_event(
                            tick,
                            instrument_id=instrument_id,
                            connection_id=connection_id,
                            receive_sequence=receive_sequence,
                            topic=settings.POLYMARKET_TWAP_TOPIC,
                            received_wall_ns=received_wall_ns,
                            received_monotonic_ns=received_monotonic_ns,
                        )
                        # The live attempt deliberately precedes PostgreSQL
                        # queue visibility. The handoff preserves this event
                        # even when a planned restart cancels the reader while
                        # Redis is in flight.
                        await _deliver_and_enqueue_twap_event(
                            live_cache=live_cache,
                            records=records,
                            event=event,
                            websocket=websocket,
                            event_capacity=(
                                settings.POLYMARKET_TWAP_PERSIST_QUEUE_MAX_EVENTS
                            ),
                        )
                except asyncio.CancelledError:
                    close_reason = "planned_restart"
                    gap_reason = "planned_restart"
                    if connection_id is not None and session_started:
                        session_finalized = _offer_twap_session_final_records(
                            records,
                            connection_id=connection_id,
                            close_reason=close_reason,
                            gap_reason=gap_reason,
                            gap_idle_timeout_ms=None,
                            messages_received_total=messages_received_total,
                            messages_accepted_total=messages_accepted_total,
                            parse_errors_total=parse_errors_total,
                            receive_sequence=receive_sequence,
                            last_accepted_received_ms=(
                                last_accepted_received_ms
                            ),
                            last_provider_event_ms=last_provider_event_ms,
                        )
                    raise
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except asyncio.CancelledError:
            close_reason = "planned_restart"
            gap_reason = "planned_restart"
            raise
        except Exception as exc:
            reconnect_error = exc
            close_reason, gap_reason, gap_idle_timeout_ms = (
                _connection_error_reason(exc)
            )
        finally:
            if (
                connection_id is not None
                and session_started
                and not session_finalized
            ):
                session_finalized = _offer_twap_session_final_records(
                    records,
                    connection_id=connection_id,
                    close_reason=close_reason,
                    gap_reason=gap_reason,
                    gap_idle_timeout_ms=gap_idle_timeout_ms,
                    messages_received_total=messages_received_total,
                    messages_accepted_total=messages_accepted_total,
                    parse_errors_total=parse_errors_total,
                    receive_sequence=receive_sequence,
                    last_accepted_received_ms=last_accepted_received_ms,
                    last_provider_event_ms=last_provider_event_ms,
                )

        if reconnect_error is None:
            continue
        attempt += 1
        delay = reconnect_delay_seconds(attempt)
        LOGGER.warning(
            "polymarket_twap_reconnect_scheduled",
            extra={
                "event": "polymarket_twap_reconnect_scheduled",
                "attempt": attempt,
                "delay_seconds": round(delay, 3),
                "error": repr(reconnect_error),
                "connection_id": connection_id,
            },
        )
        await asyncio.sleep(delay)


def _consume_detached_task_result(task: "asyncio.Task[Any]") -> None:
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        LOGGER.exception(
            "polymarket_twap_detached_task_failed",
            extra={"event": "polymarket_twap_detached_task_failed"},
        )


async def _cancel_twap_task_bounded(
    task: "asyncio.Task[Any]",
    *,
    timeout_seconds: float,
) -> bool:
    if task.done():
        await asyncio.gather(task, return_exceptions=True)
        return True
    task.cancel()
    done, _pending = await asyncio.wait(
        {task},
        timeout=max(0.001, timeout_seconds),
    )
    if task in done:
        await asyncio.gather(task, return_exceptions=True)
        return True

    # Interrupt a cancellation-resistant websocket close/finalizer once more.
    # Final coverage controls are non-blocking and are offered before normal
    # websocket context teardown.
    task.cancel()
    done, _pending = await asyncio.wait({task}, timeout=0.1)
    if task in done:
        await asyncio.gather(task, return_exceptions=True)
        return True
    task.add_done_callback(_consume_detached_task_result)
    return False


async def run_polymarket_twap_runtime(
    settings: Any,
    pool: Any,
    *,
    live_cache: Any = None,
) -> None:
    validate_twap_runtime_identity(settings)
    await recover_orphaned_polymarket_twap_sessions(pool)
    instrument_id = await db.get_instrument_id(
        pool,
        provider_code=settings.POLYMARKET_TWAP_PROVIDER_CODE,
        symbol=settings.POLYMARKET_TWAP_SYMBOL,
    )
    event_capacity = settings.POLYMARKET_TWAP_PERSIST_QUEUE_MAX_EVENTS
    records: "asyncio.Queue[PolymarketTwapPersistenceRecord]" = asyncio.Queue(
        maxsize=event_capacity + TWAP_FINAL_CONTROL_RESERVE
    )
    writer = asyncio.create_task(
        polymarket_twap_persistence_worker(pool=pool, records=records)
    )
    reader = asyncio.create_task(
        polymarket_twap_reader_loop(
            settings,
            instrument_id=instrument_id,
            records=records,
            live_cache=live_cache,
        )
    )
    try:
        done, _pending = await asyncio.wait(
            {reader, writer},
            return_when=asyncio.FIRST_COMPLETED,
        )
        completed = next(iter(done))
        if completed.cancelled():
            raise asyncio.CancelledError
        exception = completed.exception()
        if exception is not None:
            raise exception
        raise RuntimeError("a critical Polymarket TWAP runtime task stopped")
    finally:
        shutdown_timeout_seconds = float(
            settings.POLYMARKET_TWAP_PERSIST_SHUTDOWN_TIMEOUT_SECONDS
        )
        shutdown_deadline = (
            asyncio.get_running_loop().time() + shutdown_timeout_seconds
        )
        reader_stopped = await _cancel_twap_task_bounded(
            reader,
            timeout_seconds=min(2.0, shutdown_timeout_seconds),
        )
        if not reader_stopped:
            LOGGER.error(
                "polymarket_twap_reader_shutdown_incomplete",
                extra={
                    "event": "polymarket_twap_reader_shutdown_incomplete",
                    "shutdown_timeout_seconds": shutdown_timeout_seconds,
                },
            )

        remaining_seconds = max(
            0.001,
            shutdown_deadline - asyncio.get_running_loop().time(),
        )
        try:
            await asyncio.wait_for(
                records.join(),
                timeout=remaining_seconds,
            )
        except asyncio.TimeoutError:
            LOGGER.error(
                "polymarket_twap_persistence_shutdown_incomplete",
                extra={
                    "event": "polymarket_twap_persistence_shutdown_incomplete",
                    "queued_records": records.qsize(),
                    "event_capacity": event_capacity,
                    "queue_max_records": records.maxsize,
                },
            )
        writer_stopped = await _cancel_twap_task_bounded(
            writer,
            timeout_seconds=0.1,
        )
        if not writer_stopped:
            LOGGER.error(
                "polymarket_twap_writer_shutdown_incomplete",
                extra={
                    "event": "polymarket_twap_writer_shutdown_incomplete",
                    "queued_records": records.qsize(),
                },
            )


async def run_polymarket_twap_noncritical(
    settings: Any,
    pool: Any,
    *,
    live_cache: Any = None,
) -> None:
    """Keep TWAP failures from stopping the standard Chainlink collector."""

    attempt = 0
    while True:
        try:
            await run_polymarket_twap_runtime(
                settings,
                pool,
                live_cache=live_cache,
            )
            raise RuntimeError("Polymarket TWAP runtime stopped unexpectedly")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.exception(
                "polymarket_twap_runtime_restarting",
                extra={
                    "event": "polymarket_twap_runtime_restarting",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay)
