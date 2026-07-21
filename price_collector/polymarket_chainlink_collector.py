import asyncio
import json
import logging
import signal
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Optional
from uuid import UUID, uuid4

import websockets

from price_collector.collector import (
    current_utc_epoch_ms,
    reconnect_delay_seconds,
    require_collector_database_url,
    setup_logging,
)
from price_collector.config import Settings
from price_collector.db import (
    create_pool,
    create_raw_capture_backend,
    epoch_ms_to_utc_datetime,
    get_instrument_id,
    upsert_price_sample,
)
from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    LIVE_CACHE_WRITE_ERRORS,
    create_live_cache,
)
from price_collector.market import MarketWindow, market_for_sample_second
from price_collector.raw_capture import (
    ChainlinkPriceEvent,
    FeedSession,
    FeedSessionRecord,
    POSTGRES_BIGINT_MAX,
    POSTGRES_NUMERIC_38_18_LIMIT,
    ReceiveStamp,
    create_raw_capture_runtime,
)


LOGGER = logging.getLogger("price_collector.polymarket_chainlink_collector")
RTDS_PING_SECONDS = 5.0
RTDS_CRYPTO_PRICES_HISTORY_TOPIC = "crypto_prices"
CHAINLINK_HISTORY_PENDING_MAX_SECONDS = 5_000
CHAINLINK_HISTORY_SETTLE_MS = 1_000
CHAINLINK_HISTORY_POLL_SECONDS = 0.25
CHAINLINK_DELIVERY_SHUTDOWN_TIMEOUT_SECONDS = 5.0
CHAINLINK_READER_SHUTDOWN_TIMEOUT_SECONDS = 12.0
CHAINLINK_CANCEL_CONFIRM_TIMEOUT_SECONDS = 0.1
CHAINLINK_DELIVERY_DROP_WARNING_INTERVAL_NS = 60_000_000_000
CHAINLINK_MAX_PROVIDER_EVENT_MS = (
    ((POSTGRES_BIGINT_MAX - 300_000) // 300_000) * 300_000
    + 299_999
)


class RtdsParseError(ValueError):
    pass


class RtdsAcceptedEventIdleTimeout(TimeoutError):
    pass


@dataclass(frozen=True)
class PolymarketChainlinkTick:
    symbol: str
    price: Decimal
    provider_event_ms: int
    provider_message_ms: Optional[int]


@dataclass(frozen=True)
class PolymarketChainlinkSample:
    symbol: str
    price: Decimal
    provider_event_ms: int
    provider_message_ms: Optional[int]
    received_ms: int
    sample_second_ms: int
    window: MarketWindow


@dataclass(frozen=True)
class SequencedChainlinkSample:
    sequence: int
    sample: PolymarketChainlinkSample


class ChainlinkDeliveryState:
    """Single-event-loop state for the critical Redis and history workers."""

    def __init__(
        self,
        *,
        history_capacity_seconds: int = CHAINLINK_HISTORY_PENDING_MAX_SECONDS,
        history_settle_ms: int = CHAINLINK_HISTORY_SETTLE_MS,
    ) -> None:
        if history_capacity_seconds <= 0:
            raise ValueError("Chainlink history capacity must be positive")
        if history_settle_ms < 0:
            raise ValueError("Chainlink history settle delay must be non-negative")

        self.history_capacity_seconds = history_capacity_seconds
        self.history_settle_ms = history_settle_ms
        self._accepting = True
        self._publisher_epoch = str(uuid4())
        self._sequence = 0
        self._latest: Optional[SequencedChainlinkSample] = None
        self._latest_event: Optional[asyncio.Event] = None
        self._live_attempted_sequence = 0
        self._live_attempted_event: Optional[asyncio.Event] = None
        self._history_pending: "OrderedDict[int, SequencedChainlinkSample]" = (
            OrderedDict()
        )
        self._history_inflight: Optional[SequencedChainlinkSample] = None
        self._history_event: Optional[asyncio.Event] = None
        self._closed_event: Optional[asyncio.Event] = None
        self.live_attempts_total = 0
        self.live_successes_total = 0
        self.live_failures_total = 0
        self.history_collapsed_total = 0
        self.history_persisted_total = 0
        self.history_failures_total = 0
        self.history_pending_dropped_total = 0
        self.history_pending_high_water = 0
        self.last_live_attempt_ms: Optional[int] = None
        self.last_history_write_ms: Optional[int] = None
        self._last_history_drop_warning_ns: Optional[int] = None
        self._current_connection_id: Optional[UUID] = None
        self._connections_opened_total = 0
        self._reconnects_total = 0
        self._latest_raw_event: Optional[ChainlinkPriceEvent] = None
        self._last_raw_monotonic_ns: Optional[int] = None
        self._raw_interarrival_ns: Optional[int] = None
        self._raw_max_interarrival_ns: Optional[int] = None

    @property
    def accepting(self) -> bool:
        return self._accepting

    @property
    def current_connection_id(self) -> Optional[UUID]:
        return self._current_connection_id

    @property
    def publisher_epoch(self) -> str:
        return self._publisher_epoch

    @property
    def history_pending_count(self) -> int:
        return len(self._history_pending)

    @property
    def history_drained(self) -> bool:
        return not self._history_pending and self._history_inflight is None

    def update_latest(
        self,
        sample: PolymarketChainlinkSample,
    ) -> SequencedChainlinkSample:
        if not self._accepting:
            raise RuntimeError("Chainlink delivery state is closed")
        self._sequence += 1
        item = SequencedChainlinkSample(sequence=self._sequence, sample=sample)
        self._latest = item
        if self._latest_event is not None:
            self._latest_event.set()
        return item

    def offer_history(self, item: SequencedChainlinkSample) -> None:
        if not self._accepting:
            raise RuntimeError("Chainlink delivery state is closed")
        sample_second_ms = item.sample.sample_second_ms
        if sample_second_ms in self._history_pending:
            self.history_collapsed_total += 1
            self._history_pending[sample_second_ms] = item
            self._history_pending.move_to_end(sample_second_ms)
        else:
            if not self._make_history_capacity(sample_second_ms):
                return
            self._history_pending[sample_second_ms] = item
        self.history_pending_high_water = max(
            self.history_pending_high_water,
            len(self._history_pending),
        )
        if self._history_event is not None:
            self._history_event.set()

    def _make_history_capacity(self, incoming_second_ms: int) -> bool:
        if len(self._history_pending) < self.history_capacity_seconds:
            return True
        inflight_second = (
            None
            if self._history_inflight is None
            else self._history_inflight.sample.sample_second_ms
        )
        drop_second = next(
            (
                sample_second_ms
                for sample_second_ms in self._history_pending
                if sample_second_ms != inflight_second
            ),
            None,
        )
        if drop_second is None:
            self._record_history_pending_drop(incoming_second_ms)
            return False
        self._history_pending.pop(drop_second)
        self._record_history_pending_drop(drop_second)
        return True

    def _record_history_pending_drop(self, sample_second_ms: int) -> None:
        self.history_pending_dropped_total += 1
        now_ns = time.monotonic_ns()
        if (
            self._last_history_drop_warning_ns is None
            or now_ns - self._last_history_drop_warning_ns
            >= CHAINLINK_DELIVERY_DROP_WARNING_INTERVAL_NS
        ):
            self._last_history_drop_warning_ns = now_ns
            LOGGER.error(
                "polymarket_chainlink_history_pending_dropped",
                extra={
                    "event": "polymarket_chainlink_history_pending_dropped",
                    "sample_second_ms": sample_second_ms,
                    "history_capacity_seconds": self.history_capacity_seconds,
                },
            )

    async def wait_for_latest_after(
        self,
        sequence: int,
    ) -> Optional[SequencedChainlinkSample]:
        while True:
            latest = self._latest
            if latest is not None and latest.sequence > sequence:
                return latest
            if not self._accepting:
                return None
            if self._latest_event is None:
                self._latest_event = asyncio.Event()
            self._latest_event.clear()
            latest = self._latest
            if latest is not None and latest.sequence > sequence:
                continue
            await self._latest_event.wait()

    def mark_live_attempted(
        self,
        item: SequencedChainlinkSample,
        *,
        succeeded: bool,
        attempted_ms: int,
    ) -> None:
        self.live_attempts_total += 1
        if succeeded:
            self.live_successes_total += 1
        else:
            self.live_failures_total += 1
        self.last_live_attempt_ms = attempted_ms
        self._live_attempted_sequence = max(
            self._live_attempted_sequence,
            item.sequence,
        )
        if self._live_attempted_event is not None:
            self._live_attempted_event.set()

    async def wait_until_live_attempted(self, sequence: int) -> None:
        while self._live_attempted_sequence < sequence:
            if self._live_attempted_event is None:
                self._live_attempted_event = asyncio.Event()
            self._live_attempted_event.clear()
            if self._live_attempted_sequence >= sequence:
                continue
            await self._live_attempted_event.wait()

    def next_history_ready(
        self,
        *,
        now_ms: int,
    ) -> Optional[SequencedChainlinkSample]:
        if self._history_inflight is not None or not self._history_pending:
            return None
        newest_second_ms = max(self._history_pending)
        for sample_second_ms in sorted(self._history_pending):
            item = self._history_pending[sample_second_ms]
            settled = (
                sample_second_ms < newest_second_ms
                or now_ms - item.sample.received_ms >= self.history_settle_ms
            )
            if self._accepting and not settled:
                continue
            self._history_inflight = item
            return item
        return None

    def mark_history_succeeded(
        self,
        item: SequencedChainlinkSample,
        *,
        written_ms: int,
    ) -> None:
        current = self._history_pending.get(item.sample.sample_second_ms)
        if current is not None and current.sequence == item.sequence:
            self._history_pending.pop(item.sample.sample_second_ms)
        self._history_inflight = None
        self.history_persisted_total += 1
        self.last_history_write_ms = written_ms
        if self._history_pending:
            if self._history_event is not None:
                self._history_event.set()

    def mark_history_failed(self, item: SequencedChainlinkSample) -> None:
        if self._history_inflight == item:
            self._history_inflight = None
        self.history_failures_total += 1
        if self._history_event is not None:
            self._history_event.set()

    def release_history_inflight(self, item: SequencedChainlinkSample) -> None:
        if self._history_inflight == item:
            self._history_inflight = None
        if self._history_event is not None:
            self._history_event.set()

    async def wait_for_history_activity(self, timeout_seconds: float) -> None:
        if self._history_event is None:
            self._history_event = asyncio.Event()
        self._history_event.clear()
        if self._history_pending and not self._accepting:
            return
        try:
            await asyncio.wait_for(
                self._history_event.wait(),
                timeout=max(0.001, timeout_seconds),
            )
        except asyncio.TimeoutError:
            return

    async def wait_for_history_retry(self, timeout_seconds: float) -> None:
        if not self._accepting:
            return
        if self._closed_event is None:
            self._closed_event = asyncio.Event()
        try:
            await asyncio.wait_for(
                self._closed_event.wait(),
                timeout=max(0.001, timeout_seconds),
            )
        except asyncio.TimeoutError:
            return

    def raw_connection_opened(self, connection_id: UUID) -> None:
        if self._current_connection_id == connection_id:
            return
        if self._connections_opened_total:
            self._reconnects_total += 1
        self._connections_opened_total += 1
        self._current_connection_id = connection_id

    def raw_connection_closed(self, connection_id: UUID) -> None:
        if self._current_connection_id == connection_id:
            self._current_connection_id = None

    def observe_raw_event(self, event: ChainlinkPriceEvent) -> None:
        if self._last_raw_monotonic_ns is not None:
            interarrival_ns = (
                event.received_monotonic_ns - self._last_raw_monotonic_ns
            )
            self._raw_interarrival_ns = interarrival_ns
            if interarrival_ns >= 0:
                self._raw_max_interarrival_ns = (
                    interarrival_ns
                    if self._raw_max_interarrival_ns is None
                    else max(self._raw_max_interarrival_ns, interarrival_ns)
                )
        self._last_raw_monotonic_ns = event.received_monotonic_ns
        self._latest_raw_event = event

    def close(self) -> None:
        self._accepting = False
        if self._latest_event is not None:
            self._latest_event.set()
        if self._live_attempted_event is not None:
            self._live_attempted_event.set()
        if self._history_event is not None:
            self._history_event.set()
        if self._closed_event is not None:
            self._closed_event.set()

    def telemetry_fields(self, *, now_ms: int) -> dict[str, Any]:
        latest_sample = None if self._latest is None else self._latest.sample
        raw_event = self._latest_raw_event
        return {
            "delivery_sequence": self._sequence,
            "delivery_live_attempted_sequence": self._live_attempted_sequence,
            "delivery_live_attempts_total": self.live_attempts_total,
            "delivery_live_successes_total": self.live_successes_total,
            "delivery_live_failures_total": self.live_failures_total,
            "delivery_history_collapsed_total": self.history_collapsed_total,
            "delivery_history_persisted_total": self.history_persisted_total,
            "delivery_history_failures_total": self.history_failures_total,
            "delivery_history_pending_dropped_total": (
                self.history_pending_dropped_total
            ),
            "delivery_history_pending_seconds": len(self._history_pending),
            "delivery_history_pending_high_water": self.history_pending_high_water,
            "delivery_last_live_attempt_ms": self.last_live_attempt_ms,
            "delivery_last_history_write_ms": self.last_history_write_ms,
            "chainlink_connections_opened_total": self._connections_opened_total,
            "chainlink_reconnects_total": self._reconnects_total,
            "chainlink_latest_price": (
                None if latest_sample is None else latest_sample.price
            ),
            "chainlink_provider_event_ms": (
                None if latest_sample is None else latest_sample.provider_event_ms
            ),
            "chainlink_provider_message_ms": (
                None if latest_sample is None else latest_sample.provider_message_ms
            ),
            "chainlink_received_ms": (
                None if latest_sample is None else latest_sample.received_ms
            ),
            "chainlink_provider_event_to_receive_ms": (
                None
                if latest_sample is None
                else latest_sample.received_ms - latest_sample.provider_event_ms
            ),
            "chainlink_provider_message_to_receive_ms": (
                None
                if latest_sample is None
                or latest_sample.provider_message_ms is None
                else latest_sample.received_ms - latest_sample.provider_message_ms
            ),
            "chainlink_provider_message_minus_event_ms": (
                None
                if latest_sample is None
                or latest_sample.provider_message_ms is None
                else latest_sample.provider_message_ms
                - latest_sample.provider_event_ms
            ),
            "chainlink_provider_event_age_ms": (
                None
                if latest_sample is None
                else now_ms - latest_sample.provider_event_ms
            ),
            "chainlink_received_age_ms": (
                None
                if latest_sample is None
                else now_ms - latest_sample.received_ms
            ),
            "chainlink_latest_receive_sequence": (
                None if raw_event is None else raw_event.receive_sequence
            ),
            "chainlink_latest_connection_id": (
                None if raw_event is None else raw_event.connection_id
            ),
            "chainlink_raw_interarrival_ns": self._raw_interarrival_ns,
            "chainlink_raw_max_interarrival_ns": self._raw_max_interarrival_ns,
        }


def build_polymarket_chainlink_subscription(settings: Settings) -> dict[str, Any]:
    return {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": settings.POLYMARKET_CHAINLINK_TOPIC,
                "type": "*",
                "filters": json.dumps(
                    {"symbol": settings.POLYMARKET_CHAINLINK_RTD_SYMBOL},
                    separators=(",", ":"),
                ),
            }
        ],
    }


def _parse_positive_int_ms(raw_value: Any, field_name: str) -> int:
    if isinstance(raw_value, bool):
        raise RtdsParseError(f"{field_name} must be a positive integer millisecond timestamp")

    if isinstance(raw_value, int):
        value = raw_value
    elif isinstance(raw_value, Decimal):
        if raw_value != raw_value.to_integral_value():
            raise RtdsParseError(f"{field_name} must be a positive integer millisecond timestamp")
        value = int(raw_value)
    elif isinstance(raw_value, str):
        if not raw_value.isdecimal():
            raise RtdsParseError(f"{field_name} must be a positive integer millisecond timestamp")
        value = int(raw_value)
    else:
        raise RtdsParseError(f"{field_name} must be a positive integer millisecond timestamp")

    if value <= 0:
        raise RtdsParseError(f"{field_name} must be positive")
    if value > POSTGRES_BIGINT_MAX:
        raise RtdsParseError(f"{field_name} exceeds PostgreSQL BIGINT")

    return value


def _validate_chainlink_price_for_storage(price: Decimal) -> None:
    if price >= POSTGRES_NUMERIC_38_18_LIMIT:
        raise RtdsParseError("RTDS price exceeds PostgreSQL NUMERIC(38,18)")
    _sign, digits, exponent = price.as_tuple()
    if exponent < -18:
        extra_places = -18 - exponent
        if extra_places > len(digits) or any(digits[-extra_places:]):
            raise RtdsParseError(
                "RTDS price exceeds PostgreSQL NUMERIC(38,18) scale"
            )


def parse_polymarket_chainlink_message(
    message: Mapping[str, Any],
    *,
    expected_symbol: str = "btc/usd",
    db_symbol: str = "BTCUSD",
    expected_topic: str = "crypto_prices_chainlink",
) -> PolymarketChainlinkTick:
    topic = message.get("topic")
    if topic != expected_topic:
        raise RtdsParseError(f"unexpected RTDS topic: expected {expected_topic!r}, got {topic!r}")

    message_type = message.get("type")
    if message_type != "update":
        raise RtdsParseError(f"non-update RTDS message: {message_type!r}")

    payload = message.get("payload")
    if not isinstance(payload, Mapping):
        raise RtdsParseError("RTDS message payload must be an object")

    symbol = payload.get("symbol")
    if symbol != expected_symbol:
        raise RtdsParseError(
            f"unexpected Chainlink symbol: expected {expected_symbol!r}, got {symbol!r}"
        )

    raw_value = payload.get("value")
    if raw_value is None:
        raise RtdsParseError("RTDS payload missing price field payload.value")

    try:
        price = raw_value if isinstance(raw_value, Decimal) else Decimal(str(raw_value))
    except (InvalidOperation, ValueError) as exc:
        raise RtdsParseError("invalid RTDS price field payload.value") from exc

    if not price.is_finite() or price <= 0:
        raise RtdsParseError("RTDS price must be finite and positive")
    _validate_chainlink_price_for_storage(price)

    provider_event_ms = _parse_positive_int_ms(
        payload.get("timestamp"),
        "RTDS payload.timestamp",
    )
    if provider_event_ms > CHAINLINK_MAX_PROVIDER_EVENT_MS:
        raise RtdsParseError(
            "RTDS payload.timestamp exceeds the PostgreSQL market-window range"
        )
    provider_window = market_for_sample_second(
        (provider_event_ms // 1000) * 1000
    )
    try:
        epoch_ms_to_utc_datetime(provider_window.market_end_ms)
    except OverflowError as exc:
        raise RtdsParseError(
            "RTDS payload.timestamp exceeds the application datetime range"
        ) from exc

    provider_message_ms = None
    raw_message_ms = message.get("timestamp")
    if raw_message_ms is not None:
        try:
            provider_message_ms = _parse_positive_int_ms(
                raw_message_ms,
                "RTDS message.timestamp",
            )
        except RtdsParseError:
            provider_message_ms = None

    return PolymarketChainlinkTick(
        symbol=db_symbol,
        price=price,
        provider_event_ms=provider_event_ms,
        provider_message_ms=provider_message_ms,
    )


def _is_expected_chainlink_startup_snapshot(
    message: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> bool:
    """Identify the RTDS historical dump sent before live Chainlink updates."""

    if message.get("topic") != RTDS_CRYPTO_PRICES_HISTORY_TOPIC:
        return False
    if message.get("type") != "subscribe":
        return False

    payload = message.get("payload")
    return (
        isinstance(payload, Mapping)
        and payload.get("symbol") == expected_symbol
        and isinstance(payload.get("data"), list)
    )


def sample_second_ms_for_provider_event(provider_event_ms: int) -> int:
    return (provider_event_ms // 1000) * 1000


def build_polymarket_chainlink_sample(
    tick: PolymarketChainlinkTick,
    *,
    received_ms: int,
) -> PolymarketChainlinkSample:
    sample_second_ms = sample_second_ms_for_provider_event(tick.provider_event_ms)
    window = market_for_sample_second(sample_second_ms)

    return PolymarketChainlinkSample(
        symbol=tick.symbol,
        price=tick.price,
        provider_event_ms=tick.provider_event_ms,
        provider_message_ms=tick.provider_message_ms,
        received_ms=received_ms,
        sample_second_ms=sample_second_ms,
        window=window,
    )


async def update_chainlink_live_cache(
    live_cache: Any,
    sample: PolymarketChainlinkSample,
    *,
    publisher_epoch: Optional[str] = None,
    accepted_event_sequence: Optional[int] = None,
) -> bool:
    if live_cache is None:
        return False

    try:
        live_fields: dict[str, Any] = {
            "value": sample.price,
            "source_timestamp_ms": sample.provider_event_ms,
            "received_ms": sample.received_ms,
        }
        if publisher_epoch is not None or accepted_event_sequence is not None:
            live_fields.update(
                publisher_epoch=publisher_epoch,
                accepted_event_sequence=accepted_event_sequence,
            )
        await live_cache.set_price(CHAINLINK_LIVE_KEY, **live_fields)
    except LIVE_CACHE_WRITE_ERRORS as exc:
        LOGGER.warning(
            "live_cache_write_failed",
            extra={
                "event": "live_cache_write_failed",
                "source": "chainlink",
                "key": CHAINLINK_LIVE_KEY,
                "error": repr(exc),
            },
        )
        return False

    return True


async def handle_tick(
    pool: Any,
    instrument_id: int,
    tick: PolymarketChainlinkTick,
    *,
    source_topic: str = "crypto_prices_chainlink",
    received_ms: Optional[int] = None,
    live_cache: Any = None,
) -> None:
    sample = build_polymarket_chainlink_sample(
        tick,
        received_ms=current_utc_epoch_ms() if received_ms is None else received_ms,
    )

    await update_chainlink_live_cache(live_cache, sample)

    await write_chainlink_sample(
        pool,
        instrument_id,
        sample,
        source_topic=source_topic,
    )


async def write_chainlink_sample(
    pool: Any,
    instrument_id: int,
    sample: PolymarketChainlinkSample,
    *,
    source_topic: str = "crypto_prices_chainlink",
) -> None:

    await upsert_price_sample(
        pool,
        instrument_id=instrument_id,
        sample_second_ms=sample.sample_second_ms,
        window=sample.window,
        price=sample.price,
        provider_event_ms=sample.provider_event_ms,
        received_ms=sample.received_ms,
        source_price_field="payload.value",
        provider_message_ms=sample.provider_message_ms,
        source_topic=source_topic,
    )

    LOGGER.info(
        "polymarket_chainlink_sample_written",
        extra={
            "event": "polymarket_chainlink_sample_written",
            "symbol": sample.symbol,
            "sample_second_ms": sample.sample_second_ms,
            "market_id": sample.window.market_id,
            "provider_event_ms": sample.provider_event_ms,
            "provider_message_ms": sample.provider_message_ms,
            "received_ms": sample.received_ms,
            "source_topic": source_topic,
        },
    )


async def chainlink_live_worker(
    *,
    delivery_state: ChainlinkDeliveryState,
    live_cache: Any,
) -> None:
    processed_sequence = 0
    while True:
        item = await delivery_state.wait_for_latest_after(processed_sequence)
        if item is None:
            return
        succeeded = False
        try:
            succeeded = await update_chainlink_live_cache(
                live_cache,
                item.sample,
                publisher_epoch=delivery_state.publisher_epoch,
                accepted_event_sequence=item.sequence,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "polymarket_chainlink_live_worker_failed",
                extra={"event": "polymarket_chainlink_live_worker_failed"},
            )
        delivery_state.mark_live_attempted(
            item,
            succeeded=succeeded,
            attempted_ms=current_utc_epoch_ms(),
        )
        processed_sequence = item.sequence


async def chainlink_history_worker(
    *,
    delivery_state: ChainlinkDeliveryState,
    pool: Any,
    instrument_id: int,
    source_topic: str,
) -> None:
    failure_attempt = 0
    while True:
        item = delivery_state.next_history_ready(
            now_ms=current_utc_epoch_ms(),
        )
        if item is None:
            if not delivery_state.accepting and delivery_state.history_drained:
                return
            await delivery_state.wait_for_history_activity(
                CHAINLINK_HISTORY_POLL_SECONDS
            )
            continue

        await delivery_state.wait_until_live_attempted(item.sequence)
        try:
            await write_chainlink_sample(
                pool,
                instrument_id,
                item.sample,
                source_topic=source_topic,
            )
        except asyncio.CancelledError:
            delivery_state.release_history_inflight(item)
            raise
        except Exception as exc:
            delivery_state.mark_history_failed(item)
            failure_attempt += 1
            delay = reconnect_delay_seconds(failure_attempt)
            LOGGER.warning(
                "polymarket_chainlink_history_write_retry_scheduled",
                extra={
                    "event": (
                        "polymarket_chainlink_history_write_retry_scheduled"
                    ),
                    "sample_second_ms": item.sample.sample_second_ms,
                    "attempt": failure_attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await delivery_state.wait_for_history_retry(delay)
        else:
            delivery_state.mark_history_succeeded(
                item,
                written_ms=current_utc_epoch_ms(),
            )
            failure_attempt = 0


async def rtds_ping_loop(websocket: Any) -> None:
    while True:
        await asyncio.sleep(RTDS_PING_SECONDS)
        await websocket.send("PING")


def _record_chainlink_raw_drop(
    raw_capture: Any,
    session: Optional[FeedSession],
    *,
    increment_global: bool,
) -> None:
    if increment_global:
        try:
            raw_capture.counters.record_dropped()
        except Exception:
            LOGGER.exception(
                "polymarket_chainlink_raw_drop_counter_failed",
                extra={"event": "polymarket_chainlink_raw_drop_counter_failed"},
            )
    if session is not None:
        try:
            session.mark_record_dropped()
        except Exception:
            LOGGER.exception(
                "polymarket_chainlink_session_drop_counter_failed",
                extra={
                    "event": "polymarket_chainlink_session_drop_counter_failed"
                },
            )


def _offer_chainlink_raw_record(
    raw_capture: Any,
    record: Any,
    *,
    session: Optional[FeedSession],
) -> bool:
    try:
        result = raw_capture.offer_nowait(record)
        if not result.accepted:
            _record_chainlink_raw_drop(
                raw_capture,
                session,
                increment_global=False,
            )
            return False

        dropped_record = (
            result.dropped_record if result.dropped_oldest else None
        )
        if (
            session is not None
            and isinstance(
                dropped_record,
                (ChainlinkPriceEvent, FeedSessionRecord),
            )
            and dropped_record.connection_id == session.connection_id
        ):
            _record_chainlink_raw_drop(
                raw_capture,
                session,
                increment_global=False,
            )
        return True
    except Exception:
        _record_chainlink_raw_drop(
            raw_capture,
            session,
            increment_global=True,
        )
        LOGGER.exception(
            "polymarket_chainlink_raw_offer_failed",
            extra={"event": "polymarket_chainlink_raw_offer_failed"},
        )
        return False


def _mark_chainlink_session(
    session: Optional[FeedSession],
    method_name: str,
) -> None:
    if session is None:
        return
    try:
        getattr(session, method_name)()
    except Exception:
        LOGGER.exception(
            "polymarket_chainlink_session_counter_failed",
            extra={
                "event": "polymarket_chainlink_session_counter_failed",
                "counter": method_name,
            },
        )


def _capture_chainlink_tick(
    *,
    raw_capture: Any,
    session: Optional[FeedSession],
    stamp: ReceiveStamp,
    tick: PolymarketChainlinkTick,
    delivery_state: ChainlinkDeliveryState,
) -> None:
    try:
        event = ChainlinkPriceEvent(
            received_wall_ns=stamp.received_wall_ns,
            received_monotonic_ns=stamp.received_monotonic_ns,
            connection_id=stamp.connection_id,
            receive_sequence=stamp.receive_sequence,
            provider_event_ms=tick.provider_event_ms,
            provider_message_ms=tick.provider_message_ms,
            price=tick.price,
        )
    except Exception:
        _record_chainlink_raw_drop(
            raw_capture,
            session,
            increment_global=True,
        )
        LOGGER.exception(
            "polymarket_chainlink_raw_record_failed",
            extra={"event": "polymarket_chainlink_raw_record_failed"},
        )
        return

    try:
        delivery_state.observe_raw_event(event)
    except Exception:
        LOGGER.exception(
            "polymarket_chainlink_raw_monitor_failed",
            extra={"event": "polymarket_chainlink_raw_monitor_failed"},
        )
    _offer_chainlink_raw_record(
        raw_capture,
        event,
        session=session,
    )


def _finish_chainlink_session(
    *,
    raw_capture: Any,
    session: FeedSession,
    close_reason: str,
) -> None:
    try:
        record = session.finish(close_reason=close_reason)
    except Exception:
        LOGGER.exception(
            "polymarket_chainlink_session_finish_failed",
            extra={"event": "polymarket_chainlink_session_finish_failed"},
        )
        return
    _offer_chainlink_raw_record(raw_capture, record, session=None)


async def chainlink_raw_capture_telemetry_loop(
    *,
    raw_capture: Any,
    delivery_state: ChainlinkDeliveryState,
    interval_seconds: float = 60.0,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("raw capture telemetry interval must be positive")
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            snapshot = raw_capture.counters.snapshot(
                queue_depth=raw_capture.buffer.qsize(),
                connection_id=delivery_state.current_connection_id,
            )
            fields = {
                "event": "raw_capture_summary",
                "source": "polymarket_chainlink_rtds",
                "messages_received_total": snapshot.messages_received_total,
                "messages_accepted_total": snapshot.messages_accepted_total,
                "parse_errors_total": snapshot.parse_errors_total,
                "records_coalesced_total": snapshot.records_coalesced_total,
                "records_enqueued_total": snapshot.records_enqueued_total,
                "records_persisted_total": snapshot.records_persisted_total,
                "records_dropped_total": snapshot.records_dropped_total,
                "batches_failed_total": snapshot.batches_failed_total,
                "maintenance_runs_total": snapshot.maintenance_runs_total,
                "maintenance_failures_total": snapshot.maintenance_failures_total,
                "capture_suspended": snapshot.capture_suspended,
                "queue_depth": snapshot.queue_depth,
                "queue_high_water": snapshot.queue_high_water,
                "last_batch_rows": snapshot.last_batch_rows,
                "last_batch_duration_ms": snapshot.last_batch_duration_ms,
                "current_partition": snapshot.current_partition,
                "raw_table_bytes": snapshot.raw_table_bytes,
                "connection_id": snapshot.connection_id,
            }
            fields.update(
                delivery_state.telemetry_fields(now_ms=current_utc_epoch_ms())
            )
            LOGGER.info("raw_capture_summary", extra=fields)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "raw_capture_summary_failed",
                extra={
                    "event": "raw_capture_summary_failed",
                    "source": "polymarket_chainlink_rtds",
                },
            )


async def polymarket_chainlink_reader_loop(
    settings: Settings,
    delivery_state: ChainlinkDeliveryState,
    *,
    raw_capture: Any = None,
) -> None:
    attempt = 0
    connection_sequence = 0
    idle_reconnects_total = 0
    consecutive_idle_reconnects = 0

    while delivery_state.accepting:
        connection_id: Optional[UUID] = None
        capture_session: Optional[FeedSession] = None
        local_receive_sequence = 0
        close_reason = "remote_close"
        reconnect_error: Optional[Exception] = None
        try:
            LOGGER.info(
                "polymarket_rtds_connecting",
                extra={
                    "event": "polymarket_rtds_connecting",
                    "url": settings.POLYMARKET_RTDS_WS_URL,
                    "topic": settings.POLYMARKET_CHAINLINK_TOPIC,
                    "rtd_symbol": settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
                },
            )
            async with websockets.connect(
                settings.POLYMARKET_RTDS_WS_URL,
                ping_interval=None,
                close_timeout=10,
            ) as websocket:
                connection_sequence += 1
                loop = asyncio.get_running_loop()
                connected_monotonic = loop.time()
                last_frame_monotonic: Optional[float] = None
                last_frame_received_ms: Optional[int] = None
                last_accepted_received_ms: Optional[int] = None
                last_provider_event_ms: Optional[int] = None
                connection_messages_received_total = 0
                connection_messages_accepted_total = 0
                connection_parse_errors_total = 0
                idle_timeout_ms = (
                    settings.POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS
                )
                if raw_capture is not None:
                    try:
                        connection_id = uuid4()
                    except Exception:
                        _record_chainlink_raw_drop(
                            raw_capture,
                            None,
                            increment_global=True,
                        )
                        LOGGER.exception(
                            "polymarket_chainlink_session_start_failed",
                            extra={
                                "event": "polymarket_chainlink_session_start_failed",
                                "connection_id": connection_id,
                            },
                        )
                    if connection_id is not None:
                        try:
                            delivery_state.raw_connection_opened(connection_id)
                        except Exception:
                            LOGGER.exception(
                                "polymarket_chainlink_raw_monitor_failed",
                                extra={
                                    "event": (
                                        "polymarket_chainlink_raw_monitor_failed"
                                    )
                                },
                            )
                        try:
                            capture_session = FeedSession(
                                source="polymarket_chainlink_rtds",
                                connection_id=connection_id,
                                connected_wall_ns=time.time_ns(),
                                connected_monotonic_ns=time.monotonic_ns(),
                                counters=raw_capture.counters,
                            )
                        except Exception:
                            capture_session = None
                            _record_chainlink_raw_drop(
                                raw_capture,
                                None,
                                increment_global=True,
                            )
                            LOGGER.exception(
                                "polymarket_chainlink_session_start_failed",
                                extra={
                                    "event": (
                                        "polymarket_chainlink_session_start_failed"
                                    ),
                                    "connection_id": connection_id,
                                },
                            )

                subscription = build_polymarket_chainlink_subscription(settings)
                await websocket.send(json.dumps(subscription))
                last_accepted_monotonic = loop.time()
                if capture_session is not None:
                    try:
                        capture_session.mark_ready(
                            ready_wall_ns=time.time_ns(),
                            ready_monotonic_ns=time.monotonic_ns(),
                        )
                        _offer_chainlink_raw_record(
                            raw_capture,
                            capture_session.opened_record(),
                            session=capture_session,
                        )
                    except Exception:
                        _record_chainlink_raw_drop(
                            raw_capture,
                            capture_session,
                            increment_global=True,
                        )
                        LOGGER.exception(
                            "polymarket_chainlink_session_ready_failed",
                            extra={
                                "event": "polymarket_chainlink_session_ready_failed",
                                "connection_id": connection_id,
                            },
                        )
                LOGGER.info(
                    "polymarket_rtds_subscribed",
                    extra={
                        "event": "polymarket_rtds_subscribed",
                        "topic": settings.POLYMARKET_CHAINLINK_TOPIC,
                        "rtd_symbol": settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
                        "connection_id": connection_id,
                        "connection_sequence": connection_sequence,
                        "accepted_event_idle_timeout_ms": idle_timeout_ms,
                    },
                )

                ping_task = asyncio.create_task(rtds_ping_loop(websocket))
                try:
                    message_iterator = websocket.__aiter__()
                    while True:
                        remaining_seconds = (
                            last_accepted_monotonic
                            + idle_timeout_ms / 1_000
                            - loop.time()
                        )
                        try:
                            if remaining_seconds <= 0:
                                raise asyncio.TimeoutError
                            raw_message = await asyncio.wait_for(
                                message_iterator.__anext__(),
                                timeout=remaining_seconds,
                            )
                        except StopAsyncIteration:
                            break
                        except asyncio.TimeoutError as exc:
                            timeout_monotonic = loop.time()
                            accepted_tick_idle_ms = max(
                                0,
                                int(
                                    (
                                        timeout_monotonic
                                        - last_accepted_monotonic
                                    )
                                    * 1_000
                                ),
                            )
                            frame_idle_ms = max(
                                0,
                                int(
                                    (
                                        timeout_monotonic
                                        - (
                                            last_frame_monotonic
                                            if last_frame_monotonic is not None
                                            else connected_monotonic
                                        )
                                    )
                                    * 1_000
                                ),
                            )
                            idle_reconnects_total += 1
                            consecutive_idle_reconnects += 1
                            LOGGER.warning(
                                "polymarket_rtds_idle_reconnect_triggered",
                                extra={
                                    "event": (
                                        "polymarket_rtds_idle_reconnect_triggered"
                                    ),
                                    "source": "polymarket_chainlink_rtds",
                                    "topic": settings.POLYMARKET_CHAINLINK_TOPIC,
                                    "rtd_symbol": (
                                        settings.POLYMARKET_CHAINLINK_RTD_SYMBOL
                                    ),
                                    "connection_id": connection_id,
                                    "connection_sequence": connection_sequence,
                                    "idle_basis": "accepted_chainlink_tick",
                                    "idle_timeout_ms": idle_timeout_ms,
                                    "accepted_tick_idle_ms": (
                                        accepted_tick_idle_ms
                                    ),
                                    "frame_idle_ms": frame_idle_ms,
                                    "connected_elapsed_ms": max(
                                        0,
                                        int(
                                            (
                                                timeout_monotonic
                                                - connected_monotonic
                                            )
                                            * 1_000
                                        ),
                                    ),
                                    "last_frame_received_ms": (
                                        last_frame_received_ms
                                    ),
                                    "last_accepted_received_ms": (
                                        last_accepted_received_ms
                                    ),
                                    "last_provider_event_ms": (
                                        last_provider_event_ms
                                    ),
                                    "connection_messages_received_total": (
                                        connection_messages_received_total
                                    ),
                                    "connection_messages_accepted_total": (
                                        connection_messages_accepted_total
                                    ),
                                    "connection_parse_errors_total": (
                                        connection_parse_errors_total
                                    ),
                                    "idle_reconnects_total": (
                                        idle_reconnects_total
                                    ),
                                    "consecutive_idle_reconnects": (
                                        consecutive_idle_reconnects
                                    ),
                                },
                            )
                            raise RtdsAcceptedEventIdleTimeout(
                                "no accepted Chainlink price event for "
                                f"{accepted_tick_idle_ms} ms"
                            ) from exc

                        received_wall_ns = time.time_ns()
                        last_frame_monotonic = loop.time()
                        last_frame_received_ms = (
                            received_wall_ns // 1_000_000
                        )
                        connection_messages_received_total += 1
                        stamp: Optional[ReceiveStamp] = None
                        if connection_id is not None:
                            try:
                                received_monotonic_ns = time.monotonic_ns()
                                if capture_session is not None:
                                    try:
                                        stamp = (
                                            capture_session.next_receive_stamp(
                                                received_wall_ns=(
                                                    received_wall_ns
                                                ),
                                                received_monotonic_ns=(
                                                    received_monotonic_ns
                                                ),
                                            )
                                        )
                                        local_receive_sequence = (
                                            stamp.receive_sequence
                                        )
                                    except Exception:
                                        local_receive_sequence += 1
                                        stamp = ReceiveStamp(
                                            connection_id=connection_id,
                                            receive_sequence=(
                                                local_receive_sequence
                                            ),
                                            received_wall_ns=received_wall_ns,
                                            received_monotonic_ns=(
                                                received_monotonic_ns
                                            ),
                                        )
                                        _record_chainlink_raw_drop(
                                            raw_capture,
                                            capture_session,
                                            increment_global=True,
                                        )
                                        LOGGER.exception(
                                            "polymarket_chainlink_receive_stamp_failed",
                                            extra={
                                                "event": (
                                                    "polymarket_chainlink_receive_stamp_failed"
                                                ),
                                                "connection_id": connection_id,
                                            },
                                        )
                                else:
                                    local_receive_sequence += 1
                                    stamp = ReceiveStamp(
                                        connection_id=connection_id,
                                        receive_sequence=local_receive_sequence,
                                        received_wall_ns=received_wall_ns,
                                        received_monotonic_ns=(
                                            received_monotonic_ns
                                        ),
                                    )
                            except Exception:
                                stamp = None
                                _record_chainlink_raw_drop(
                                    raw_capture,
                                    capture_session,
                                    increment_global=True,
                                )
                                LOGGER.exception(
                                    "polymarket_chainlink_receive_stamp_failed",
                                    extra={
                                        "event": (
                                            "polymarket_chainlink_receive_stamp_failed"
                                        ),
                                        "connection_id": connection_id,
                                    },
                                )

                        if raw_message in ("PONG", "PING", b"PONG", b"PING"):
                            continue

                        before_first_accepted_tick = (
                            connection_messages_accepted_total == 0
                        )
                        if before_first_accepted_tick and raw_message in (
                            "",
                            b"",
                        ):
                            continue

                        try:
                            message = json.loads(raw_message, parse_float=Decimal)
                            if not isinstance(message, Mapping):
                                raise RtdsParseError(
                                    "RTDS message must be an object"
                                )
                            if (
                                before_first_accepted_tick
                                and _is_expected_chainlink_startup_snapshot(
                                    message,
                                    expected_symbol=(
                                        settings.POLYMARKET_CHAINLINK_RTD_SYMBOL
                                    ),
                                )
                            ):
                                continue
                            tick = parse_polymarket_chainlink_message(
                                message,
                                expected_symbol=settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
                                db_symbol=settings.POLYMARKET_CHAINLINK_SYMBOL,
                                expected_topic=settings.POLYMARKET_CHAINLINK_TOPIC,
                            )
                        except (
                            UnicodeDecodeError,
                            json.JSONDecodeError,
                            RtdsParseError,
                            TypeError,
                        ) as exc:
                            connection_parse_errors_total += 1
                            _mark_chainlink_session(
                                capture_session,
                                "mark_parse_error",
                            )
                            LOGGER.warning(
                                "polymarket_rtds_message_skipped",
                                extra={
                                    "event": "polymarket_rtds_message_skipped",
                                    "error": str(exc),
                                },
                            )
                            continue

                        _mark_chainlink_session(
                            capture_session,
                            "mark_accepted",
                        )
                        sample = build_polymarket_chainlink_sample(
                            tick,
                            received_ms=received_wall_ns // 1_000_000,
                        )
                        item = delivery_state.update_latest(sample)
                        delivery_state.offer_history(item)
                        connection_messages_accepted_total += 1
                        last_accepted_monotonic = loop.time()
                        last_accepted_received_ms = sample.received_ms
                        last_provider_event_ms = sample.provider_event_ms
                        consecutive_idle_reconnects = 0
                        attempt = 0

                        if (
                            raw_capture is not None
                            and stamp is not None
                        ):
                            _capture_chainlink_tick(
                                raw_capture=raw_capture,
                                session=capture_session,
                                stamp=stamp,
                                tick=tick,
                                delivery_state=delivery_state,
                            )
                finally:
                    ping_task.cancel()
                    ping_result = (
                        await asyncio.gather(
                            ping_task,
                            return_exceptions=True,
                        )
                    )[0]
                    if isinstance(ping_result, Exception) and not isinstance(
                        ping_result,
                        asyncio.CancelledError,
                    ):
                        LOGGER.warning(
                            "polymarket_rtds_ping_task_failed",
                            extra={
                                "event": "polymarket_rtds_ping_task_failed",
                                "error": repr(ping_result),
                            },
                        )
        except asyncio.CancelledError:
            close_reason = "cancelled"
            raise
        except RtdsAcceptedEventIdleTimeout as exc:
            close_reason = "proactive_reconnect"
            reconnect_error = exc
        except Exception as exc:
            close_reason = "error"
            reconnect_error = exc
        finally:
            if raw_capture is not None and capture_session is not None:
                _finish_chainlink_session(
                    raw_capture=raw_capture,
                    session=capture_session,
                    close_reason=close_reason,
                )
            if connection_id is not None:
                try:
                    delivery_state.raw_connection_closed(connection_id)
                except Exception:
                    LOGGER.exception(
                        "polymarket_chainlink_raw_monitor_failed",
                        extra={
                            "event": "polymarket_chainlink_raw_monitor_failed"
                        },
                    )

        if not delivery_state.accepting:
            return
        if reconnect_error is not None:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "polymarket_rtds_reconnect_scheduled",
                extra={
                    "event": "polymarket_rtds_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(reconnect_error),
                    "connection_id": connection_id,
                },
            )
            await asyncio.sleep(delay)


def _install_sigterm_cancellation() -> Optional[Callable[[], None]]:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if task is None:
        return None
    try:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
    except (AttributeError, NotImplementedError, RuntimeError, ValueError):
        return None

    def remove_handler() -> None:
        loop.remove_signal_handler(signal.SIGTERM)

    return remove_handler


async def _run_chainlink_telemetry_noncritical(
    *,
    raw_capture: Any,
    delivery_state: ChainlinkDeliveryState,
) -> None:
    try:
        await chainlink_raw_capture_telemetry_loop(
            raw_capture=raw_capture,
            delivery_state=delivery_state,
            interval_seconds=60.0,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.exception(
            "polymarket_chainlink_raw_telemetry_failed",
            extra={"event": "polymarket_chainlink_raw_telemetry_failed"},
        )


def _consume_detached_task_result(task: "asyncio.Task[Any]") -> None:
    if task.cancelled():
        return
    try:
        task.result()
    except Exception:
        LOGGER.exception(
            "polymarket_chainlink_detached_task_failed",
            extra={"event": "polymarket_chainlink_detached_task_failed"},
        )


async def _cancel_and_wait(
    task: Optional["asyncio.Task[Any]"],
    *,
    timeout_seconds: Optional[float] = None,
) -> bool:
    if task is None:
        return True
    if not task.done():
        task.cancel()
    if timeout_seconds is not None:
        done, _pending = await asyncio.wait(
            {task},
            timeout=max(0.001, timeout_seconds),
        )
        if task not in done:
            task.add_done_callback(_consume_detached_task_result)
            return False
    await asyncio.gather(task, return_exceptions=True)
    return True


async def run_collector(settings: Settings) -> None:
    setup_logging(settings.LOG_LEVEL)
    LOGGER.info(
        "polymarket_chainlink_collector_starting",
        extra={
            "event": "polymarket_chainlink_collector_starting",
            "app_env": settings.APP_ENV,
            "provider_code": settings.POLYMARKET_CHAINLINK_PROVIDER_CODE,
            "symbol": settings.POLYMARKET_CHAINLINK_SYMBOL,
            "rtd_symbol": settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
            "topic": settings.POLYMARKET_CHAINLINK_TOPIC,
            "raw_chainlink_events_enabled": settings.RAW_CHAINLINK_EVENTS_ENABLED,
            "accepted_event_idle_timeout_ms": (
                settings.POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS
            ),
        },
    )

    database_url = require_collector_database_url(settings)
    pool = await create_pool(database_url)
    live_cache = None
    raw_capture = None
    delivery_state = ChainlinkDeliveryState()
    reader_task: Optional["asyncio.Task[Any]"] = None
    live_task: Optional["asyncio.Task[Any]"] = None
    history_task: Optional["asyncio.Task[Any]"] = None
    telemetry_task: Optional["asyncio.Task[Any]"] = None
    remove_sigterm_handler = _install_sigterm_cancellation()
    try:
        live_cache = create_live_cache(settings)
        instrument_id = await get_instrument_id(
            pool,
            provider_code=settings.POLYMARKET_CHAINLINK_PROVIDER_CODE,
            symbol=settings.POLYMARKET_CHAINLINK_SYMBOL,
        )

        if settings.RAW_CHAINLINK_EVENTS_ENABLED:
            async def raw_backend_factory() -> Any:
                return await create_raw_capture_backend(
                    database_url,
                    retention_hours=settings.RAW_CAPTURE_RETENTION_HOURS,
                    max_relation_mb=settings.RAW_CAPTURE_MAX_RELATION_MB,
                )

            raw_capture = create_raw_capture_runtime(
                futures_enabled=False,
                chainlink_enabled=True,
                backend_factory=raw_backend_factory,
                queue_max_events=settings.RAW_CAPTURE_QUEUE_MAX_EVENTS,
                batch_max_rows=settings.RAW_CAPTURE_BATCH_MAX_ROWS,
                flush_ms=settings.RAW_CAPTURE_FLUSH_MS,
                maintenance_interval_seconds=(
                    settings.RAW_CAPTURE_RETENTION_CHECK_SECONDS
                ),
                bucket_ms=settings.RAW_FUTURES_BUCKET_MS,
            )
            if raw_capture is None:
                raise RuntimeError("Chainlink raw capture runtime was not created")
            raw_capture.start()

        live_task = asyncio.create_task(
            chainlink_live_worker(
                delivery_state=delivery_state,
                live_cache=live_cache,
            )
        )
        history_task = asyncio.create_task(
            chainlink_history_worker(
                delivery_state=delivery_state,
                pool=pool,
                instrument_id=instrument_id,
                source_topic=settings.POLYMARKET_CHAINLINK_TOPIC,
            )
        )
        reader_task = asyncio.create_task(
            polymarket_chainlink_reader_loop(
                settings,
                delivery_state,
                raw_capture=raw_capture,
            )
        )
        if raw_capture is not None:
            telemetry_task = asyncio.create_task(
                _run_chainlink_telemetry_noncritical(
                    raw_capture=raw_capture,
                    delivery_state=delivery_state,
                )
            )

        done, _pending = await asyncio.wait(
            {reader_task, live_task, history_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        completed = next(iter(done))
        if completed.cancelled():
            raise asyncio.CancelledError()
        exception = completed.exception()
        if exception is not None:
            raise exception
        raise RuntimeError("a critical Chainlink collector task stopped")
    finally:
        if remove_sigterm_handler is not None:
            remove_sigterm_handler()

        reader_stopped = await _cancel_and_wait(
            reader_task,
            timeout_seconds=CHAINLINK_READER_SHUTDOWN_TIMEOUT_SECONDS,
        )
        if not reader_stopped:
            LOGGER.error(
                "polymarket_chainlink_reader_shutdown_incomplete",
                extra={
                    "event": "polymarket_chainlink_reader_shutdown_incomplete",
                    "timeout_seconds": (
                        CHAINLINK_READER_SHUTDOWN_TIMEOUT_SECONDS
                    ),
                },
            )
        delivery_state.close()

        delivery_workers = {
            task
            for task in (live_task, history_task)
            if task is not None
        }
        if delivery_workers:
            done, pending = await asyncio.wait(
                delivery_workers,
                timeout=CHAINLINK_DELIVERY_SHUTDOWN_TIMEOUT_SECONDS,
            )
            if pending:
                LOGGER.error(
                    "polymarket_chainlink_delivery_shutdown_incomplete",
                    extra={
                        "event": (
                            "polymarket_chainlink_delivery_shutdown_incomplete"
                        ),
                        "pending_history_seconds": (
                            delivery_state.history_pending_count
                        ),
                        "pending_tasks": len(pending),
                    },
                )
                for task in pending:
                    task.cancel()
            await asyncio.gather(*done, return_exceptions=True)
            if pending:
                cancelled_done, still_pending = await asyncio.wait(
                    pending,
                    timeout=CHAINLINK_CANCEL_CONFIRM_TIMEOUT_SECONDS,
                )
                await asyncio.gather(
                    *cancelled_done,
                    return_exceptions=True,
                )
                for task in still_pending:
                    task.add_done_callback(_consume_detached_task_result)

        await _cancel_and_wait(telemetry_task)
        try:
            if raw_capture is not None:
                await raw_capture.close()
        finally:
            try:
                if live_cache is not None:
                    await live_cache.close()
            finally:
                await pool.close()


def main() -> None:
    settings = Settings()
    try:
        asyncio.run(run_collector(settings))
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
