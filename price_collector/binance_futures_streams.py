import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Deque, Mapping, Optional
from uuid import UUID, uuid4

import websockets

from price_collector.collector import (
    BINANCE_RECONNECT_SECONDS,
    current_utc_epoch_ms,
    reconnect_delay_seconds,
)
from price_collector.config import Settings
from price_collector.db import upsert_binance_book_1s, upsert_binance_flow_1s
from price_collector.market import MarketWindow, market_for_sample_second
from price_collector.raw_capture import (
    BinanceFuturesPriceTrace,
    FeedSession,
    FeedSessionRecord,
    FuturesPriceTraceCoalescer,
    FuturesTradeObservation,
    ReceiveStamp,
)


LOGGER = logging.getLogger("price_collector.binance_futures_streams")
ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
TEN_THOUSAND = Decimal("10000")
MICROSTRUCTURE_SINK_LOG_INTERVAL_SECONDS = 60.0
_MICROSTRUCTURE_SINK_LAST_LOG_AT: dict[str, float] = {}


class FuturesStreamParseError(ValueError):
    pass


@dataclass(frozen=True)
class BinanceAggTrade:
    symbol: str
    agg_trade_id: int
    price: Decimal
    quantity: Decimal
    first_trade_id: int
    last_trade_id: int
    trade_time_ms: int
    event_time_ms: int
    buyer_is_maker: bool
    normal_quantity: Optional[Decimal] = None

    @property
    def quote_notional(self) -> Decimal:
        return self.price * self.quantity

    @property
    def trade_count(self) -> int:
        return self.last_trade_id - self.first_trade_id + 1


@dataclass(frozen=True)
class SequencedFuturesTrade:
    sequence: int
    trade: BinanceAggTrade
    stamp: ReceiveStamp


class FuturesTradeState:
    """Single-event-loop state for the validated futures last-price feed."""

    def __init__(self) -> None:
        self._current_connection_id: Optional[UUID] = None
        self._connections_opened_total = 0
        self._reconnects_total = 0
        self._ws_messages_total = 0
        self._ws_id_gap_events_total = 0
        self._ws_missing_agg_trades_total = 0
        self._ws_duplicate_ids_total = 0
        self._ws_regressions_total = 0
        self._ws_interarrival_ns: Optional[int] = None
        self._ws_max_interarrival_ns: Optional[int] = None
        self._ws_max_source_age_ms: Optional[int] = None
        self._ws_max_received_age_ms: Optional[int] = None
        self._last_observed_monotonic_ns: Optional[int] = None
        self._latest_ws_trade: Optional[BinanceAggTrade] = None
        self._latest_ws_stamp: Optional[ReceiveStamp] = None
        self._sequence = 0
        self._latest: Optional[SequencedFuturesTrade] = None
        self._latest_event: Optional[asyncio.Event] = None
        self._live_attempted_sequence = 0
        self._live_attempted_item: Optional[SequencedFuturesTrade] = None
        self._live_attempted_event: Optional[asyncio.Event] = None
        self._live_attempts_total = 0
        self._live_successes_total = 0
        self._live_failures_total = 0
        self._last_live_attempt_ms: Optional[int] = None

    @property
    def current_connection_id(self) -> Optional[UUID]:
        return self._current_connection_id

    def connection_opened(self, connection_id: UUID) -> None:
        if not isinstance(connection_id, UUID):
            raise TypeError("connection_id must be UUID")
        if self._current_connection_id == connection_id:
            return
        if self._connections_opened_total:
            self._reconnects_total += 1
        self._connections_opened_total += 1
        self._current_connection_id = connection_id

    def connection_closed(self, connection_id: UUID) -> None:
        if not isinstance(connection_id, UUID):
            raise TypeError("connection_id must be UUID")
        if self._current_connection_id == connection_id:
            self._current_connection_id = None

    def _has_current_ws_trade(self) -> bool:
        return (
            self._current_connection_id is not None
            and self._latest_ws_stamp is not None
            and self._latest_ws_stamp.connection_id == self._current_connection_id
        )

    def update_ws(
        self,
        trade: BinanceAggTrade,
        stamp: ReceiveStamp,
    ) -> Optional[SequencedFuturesTrade]:
        if not isinstance(trade, BinanceAggTrade):
            raise TypeError("trade must be BinanceAggTrade")
        if not isinstance(stamp, ReceiveStamp):
            raise TypeError("stamp must be ReceiveStamp")
        if self._current_connection_id != stamp.connection_id:
            self.connection_opened(stamp.connection_id)

        self._ws_messages_total += 1
        previous_monotonic_ns = self._last_observed_monotonic_ns
        if previous_monotonic_ns is not None:
            interarrival_ns = stamp.received_monotonic_ns - previous_monotonic_ns
            if interarrival_ns >= 0:
                self._ws_interarrival_ns = interarrival_ns
                self._ws_max_interarrival_ns = (
                    interarrival_ns
                    if self._ws_max_interarrival_ns is None
                    else max(self._ws_max_interarrival_ns, interarrival_ns)
                )
        self._last_observed_monotonic_ns = stamp.received_monotonic_ns

        previous_trade = self._latest_ws_trade
        if previous_trade is not None:
            id_delta = trade.agg_trade_id - previous_trade.agg_trade_id
            if id_delta == 0:
                self._ws_duplicate_ids_total += 1
                return None
            if id_delta < 0 or trade.trade_time_ms < previous_trade.trade_time_ms:
                self._ws_regressions_total += 1
                return None
            if id_delta > 1:
                self._ws_id_gap_events_total += 1
                self._ws_missing_agg_trades_total += id_delta - 1

        self._latest_ws_trade = trade
        self._latest_ws_stamp = stamp
        self._sequence += 1
        item = SequencedFuturesTrade(
            sequence=self._sequence,
            trade=trade,
            stamp=stamp,
        )
        self._latest = item
        if self._latest_event is not None:
            self._latest_event.set()
        return item

    def fresh_current(
        self,
        *,
        now_monotonic_ns: int,
        stale_after_ms: int,
    ) -> Optional[SequencedFuturesTrade]:
        item = self._latest
        if item is None or not self.is_fresh_current_item(
            item,
            now_monotonic_ns=now_monotonic_ns,
            stale_after_ms=stale_after_ms,
        ):
            return None
        return item

    def is_fresh_current_item(
        self,
        item: SequencedFuturesTrade,
        *,
        now_monotonic_ns: int,
        stale_after_ms: int,
    ) -> bool:
        if not isinstance(item, SequencedFuturesTrade):
            raise TypeError("item must be SequencedFuturesTrade")
        if isinstance(now_monotonic_ns, bool) or not isinstance(
            now_monotonic_ns,
            int,
        ):
            raise TypeError("now_monotonic_ns must be an integer")
        if isinstance(stale_after_ms, bool) or not isinstance(stale_after_ms, int):
            raise TypeError("stale_after_ms must be an integer")
        if stale_after_ms <= 0:
            raise ValueError("stale_after_ms must be positive")

        if (
            self._current_connection_id is None
            or item.stamp.connection_id != self._current_connection_id
        ):
            return False

        received_age_ns = now_monotonic_ns - item.stamp.received_monotonic_ns
        if received_age_ns < 0 or received_age_ns > stale_after_ms * 1_000_000:
            return False
        return True

    async def wait_for_latest_after(
        self,
        sequence: int,
    ) -> SequencedFuturesTrade:
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise TypeError("sequence must be an integer")
        while True:
            latest = self._latest
            if latest is not None and latest.sequence > sequence:
                return latest
            if self._latest_event is None:
                self._latest_event = asyncio.Event()
            self._latest_event.clear()
            latest = self._latest
            if latest is not None and latest.sequence > sequence:
                continue
            await self._latest_event.wait()

    def mark_live_attempted(
        self,
        item: SequencedFuturesTrade,
        *,
        succeeded: bool,
        attempted_ms: int,
    ) -> None:
        if not isinstance(item, SequencedFuturesTrade):
            raise TypeError("item must be SequencedFuturesTrade")
        if not isinstance(succeeded, bool):
            raise TypeError("succeeded must be boolean")
        if isinstance(attempted_ms, bool) or not isinstance(attempted_ms, int):
            raise TypeError("attempted_ms must be an integer")
        self._live_attempts_total += 1
        if succeeded:
            self._live_successes_total += 1
        else:
            self._live_failures_total += 1
        self._last_live_attempt_ms = attempted_ms
        if item.sequence >= self._live_attempted_sequence:
            self._live_attempted_sequence = item.sequence
            self._live_attempted_item = item
        if self._live_attempted_event is not None:
            self._live_attempted_event.set()

    async def wait_until_live_attempted(
        self,
        sequence: int,
    ) -> SequencedFuturesTrade:
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise TypeError("sequence must be an integer")
        while self._live_attempted_sequence < sequence:
            if self._live_attempted_event is None:
                self._live_attempted_event = asyncio.Event()
            self._live_attempted_event.clear()
            if self._live_attempted_sequence >= sequence:
                continue
            await self._live_attempted_event.wait()
        if self._live_attempted_item is None:
            raise RuntimeError("live attempt state is missing its trade")
        return self._live_attempted_item

    def telemetry_fields(self, now_ms: int) -> dict:
        if isinstance(now_ms, bool) or not isinstance(now_ms, int):
            raise TypeError("now_ms must be an integer")
        trade = self._latest_ws_trade
        stamp = self._latest_ws_stamp
        ws_source_age_ms = None if trade is None else now_ms - trade.trade_time_ms
        ws_received_age_ms = None if stamp is None else now_ms - stamp.received_ms
        if ws_source_age_ms is not None:
            self._ws_max_source_age_ms = (
                ws_source_age_ms
                if self._ws_max_source_age_ms is None
                else max(self._ws_max_source_age_ms, ws_source_age_ms)
            )
        if ws_received_age_ms is not None:
            self._ws_max_received_age_ms = (
                ws_received_age_ms
                if self._ws_max_received_age_ms is None
                else max(self._ws_max_received_age_ms, ws_received_age_ms)
            )
        return {
            "futures_live_delivery_sequence": self._sequence,
            "futures_live_attempted_sequence": self._live_attempted_sequence,
            "futures_live_attempts_total": self._live_attempts_total,
            "futures_live_successes_total": self._live_successes_total,
            "futures_live_failures_total": self._live_failures_total,
            "futures_live_last_attempt_ms": self._last_live_attempt_ms,
            "shadow_current_connection_id": self._current_connection_id,
            "shadow_connections_opened_total": self._connections_opened_total,
            "shadow_reconnects_total": self._reconnects_total,
            "shadow_ws_current_for_connection": self._has_current_ws_trade(),
            "shadow_ws_messages_total": self._ws_messages_total,
            "shadow_ws_id_gap_events_total": self._ws_id_gap_events_total,
            "shadow_ws_missing_agg_trades_total": self._ws_missing_agg_trades_total,
            "shadow_ws_duplicate_ids_total": self._ws_duplicate_ids_total,
            "shadow_ws_regressions_total": self._ws_regressions_total,
            "shadow_ws_interarrival_ns": self._ws_interarrival_ns,
            "shadow_ws_max_interarrival_ns": self._ws_max_interarrival_ns,
            "shadow_ws_max_source_age_ms": self._ws_max_source_age_ms,
            "shadow_ws_max_received_age_ms": self._ws_max_received_age_ms,
            "shadow_ws_connection_id": (
                None if stamp is None else stamp.connection_id
            ),
            "shadow_ws_receive_sequence": (
                None if stamp is None else stamp.receive_sequence
            ),
            "shadow_ws_price": None if trade is None else trade.price,
            "shadow_ws_agg_trade_id": (
                None if trade is None else trade.agg_trade_id
            ),
            "shadow_ws_trade_time_ms": (
                None if trade is None else trade.trade_time_ms
            ),
            "shadow_ws_event_time_ms": (
                None if trade is None else trade.event_time_ms
            ),
            "shadow_ws_source_age_ms": ws_source_age_ms,
            "shadow_ws_received_ms": None if stamp is None else stamp.received_ms,
            "shadow_ws_received_age_ms": ws_received_age_ms,
        }


@dataclass(frozen=True)
class BinanceBookTicker:
    symbol: str
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    update_id: Optional[int]
    event_time_ms: Optional[int]
    transaction_time_ms: Optional[int]

    @property
    def source_time_ms(self) -> Optional[int]:
        return self.event_time_ms or self.transaction_time_ms


@dataclass
class FlowBucket:
    sample_second_ms: int
    window: MarketWindow
    received_ms: int
    buy_base: Decimal = ZERO
    sell_base: Decimal = ZERO
    buy_quote: Decimal = ZERO
    sell_quote: Decimal = ZERO
    agg_trade_count: int = 0
    trade_count: int = 0
    max_trade_quote: Optional[Decimal] = None
    first_agg_trade_id: Optional[int] = None
    last_agg_trade_id: Optional[int] = None
    last_trade_time_ms: Optional[int] = None
    last_event_time_ms: Optional[int] = None

    def add_trade(self, trade: BinanceAggTrade, *, received_ms: int) -> None:
        quote_notional = trade.quote_notional

        if trade.buyer_is_maker:
            self.sell_base += trade.quantity
            self.sell_quote += quote_notional
        else:
            self.buy_base += trade.quantity
            self.buy_quote += quote_notional

        self.agg_trade_count += 1
        self.trade_count += trade.trade_count
        self.max_trade_quote = (
            quote_notional
            if self.max_trade_quote is None
            else max(self.max_trade_quote, quote_notional)
        )
        self.first_agg_trade_id = (
            trade.agg_trade_id
            if self.first_agg_trade_id is None
            else min(self.first_agg_trade_id, trade.agg_trade_id)
        )
        self.last_agg_trade_id = (
            trade.agg_trade_id
            if self.last_agg_trade_id is None
            else max(self.last_agg_trade_id, trade.agg_trade_id)
        )
        self.last_trade_time_ms = (
            trade.trade_time_ms
            if self.last_trade_time_ms is None
            else max(self.last_trade_time_ms, trade.trade_time_ms)
        )
        self.last_event_time_ms = (
            trade.event_time_ms
            if self.last_event_time_ms is None
            else max(self.last_event_time_ms, trade.event_time_ms)
        )
        self.received_ms = max(self.received_ms, received_ms)


@dataclass(frozen=True)
class BinanceFlowSample:
    venue: str
    symbol: str
    window: MarketWindow
    sample_second_ms: int
    buy_base: Decimal
    sell_base: Decimal
    buy_quote: Decimal
    sell_quote: Decimal
    delta_quote: Decimal
    total_quote: Decimal
    taker_imbalance: Optional[Decimal]
    cvd_quote: Decimal
    cvd_10s: Decimal
    cvd_30s: Decimal
    imbalance_10s: Optional[Decimal]
    imbalance_30s: Optional[Decimal]
    agg_trade_count: int
    trade_count: int
    max_trade_quote: Optional[Decimal]
    first_agg_trade_id: Optional[int]
    last_agg_trade_id: Optional[int]
    last_trade_time_ms: Optional[int]
    last_event_time_ms: Optional[int]
    received_ms: int


@dataclass(frozen=True)
class BinanceBookSample:
    venue: str
    symbol: str
    window: MarketWindow
    sample_second_ms: int
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    mid: Decimal
    spread: Decimal
    spread_bps: Decimal
    book_imbalance: Optional[Decimal]
    microprice: Optional[Decimal]
    update_id: Optional[int]
    event_time_ms: Optional[int]
    transaction_time_ms: Optional[int]
    received_ms: int

    @property
    def ordering_time_ms(self) -> int:
        return self.event_time_ms or self.transaction_time_ms or self.received_ms


def _decimal_field(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    positive: bool,
) -> Decimal:
    value = payload.get(field_name)
    if value is None:
        raise FuturesStreamParseError(f"payload missing decimal field {field_name!r}")
    if isinstance(value, bool) or isinstance(value, float):
        raise FuturesStreamParseError(f"decimal field {field_name!r} must be a string")

    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise FuturesStreamParseError(
            f"decimal field {field_name!r} is invalid"
        ) from exc

    if not parsed.is_finite():
        raise FuturesStreamParseError(f"decimal field {field_name!r} must be finite")
    if positive and parsed <= 0:
        raise FuturesStreamParseError(f"decimal field {field_name!r} must be positive")
    if not positive and parsed < 0:
        raise FuturesStreamParseError(
            f"decimal field {field_name!r} must be non-negative"
        )

    return parsed


def _required_int(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    positive: bool,
) -> int:
    value = payload.get(field_name)
    if value is None:
        raise FuturesStreamParseError(f"payload missing integer field {field_name!r}")
    if isinstance(value, bool):
        raise FuturesStreamParseError(f"integer field {field_name!r} is invalid")

    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise FuturesStreamParseError(
            f"integer field {field_name!r} is invalid"
        ) from exc

    if positive and parsed <= 0:
        raise FuturesStreamParseError(f"integer field {field_name!r} must be positive")
    if not positive and parsed < 0:
        raise FuturesStreamParseError(
            f"integer field {field_name!r} must be non-negative"
        )

    return parsed


def _optional_positive_int(
    payload: Mapping[str, Any],
    field_name: str,
) -> Optional[int]:
    value = payload.get(field_name)
    if value is None or value == "":
        return None
    return _required_int(payload, field_name, positive=True)


def _required_bool(payload: Mapping[str, Any], field_name: str) -> bool:
    value = payload.get(field_name)
    if not isinstance(value, bool):
        raise FuturesStreamParseError(f"boolean field {field_name!r} is invalid")
    return value


def _optional_nonnegative_decimal(
    payload: Mapping[str, Any],
    field_name: str,
) -> Optional[Decimal]:
    if field_name not in payload:
        return None

    value = payload[field_name]
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise FuturesStreamParseError(
            f"decimal field {field_name!r} must be a string, integer, or Decimal"
        )

    try:
        parsed = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise FuturesStreamParseError(
            f"decimal field {field_name!r} is invalid"
        ) from exc

    if not parsed.is_finite():
        raise FuturesStreamParseError(f"decimal field {field_name!r} must be finite")
    if parsed < 0:
        raise FuturesStreamParseError(
            f"decimal field {field_name!r} must be non-negative"
        )

    return parsed


def _validate_symbol(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
    stream_name: str,
) -> str:
    symbol = payload.get("s")
    if symbol != expected_symbol:
        raise FuturesStreamParseError(
            f"unexpected {stream_name} symbol: expected {expected_symbol!r}, got {symbol!r}"
        )
    return str(symbol)


def _validate_usdm_stream_type(payload: Mapping[str, Any]) -> None:
    stream_type = payload.get("st")
    if stream_type is None:
        return
    is_usdm = False
    if isinstance(stream_type, int) and not isinstance(stream_type, bool):
        is_usdm = stream_type == 1
    elif isinstance(stream_type, Decimal):
        is_usdm = (
            stream_type.is_finite()
            and stream_type == stream_type.to_integral_value()
            and stream_type == Decimal("1")
        )
    elif isinstance(stream_type, str):
        # Reject permissive coercions such as "1.0", whitespace, and signs.
        is_usdm = stream_type == "1"
    if not is_usdm:
        raise FuturesStreamParseError(
            f"stream type 'st' must be USD-M (1), got {stream_type!r}"
        )


def parse_binance_futures_agg_trade_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
    strict_normal_quantity: bool = True,
) -> BinanceAggTrade:
    _validate_usdm_stream_type(payload)
    symbol = _validate_symbol(
        payload,
        expected_symbol=expected_symbol,
        stream_name="aggTrade",
    )
    first_trade_id = _required_int(payload, "f", positive=False)
    last_trade_id = _required_int(payload, "l", positive=False)
    if last_trade_id < first_trade_id:
        raise FuturesStreamParseError("aggTrade last trade id is before first trade id")

    quantity = _decimal_field(payload, "q", positive=True)
    try:
        normal_quantity = _optional_nonnegative_decimal(payload, "nq")
    except FuturesStreamParseError:
        if strict_normal_quantity:
            raise
        normal_quantity = None
    if normal_quantity is not None and normal_quantity > quantity:
        if strict_normal_quantity:
            raise FuturesStreamParseError(
                "aggTrade normal quantity must not exceed total quantity"
            )
        normal_quantity = None

    return BinanceAggTrade(
        symbol=symbol,
        agg_trade_id=_required_int(payload, "a", positive=False),
        price=_decimal_field(payload, "p", positive=True),
        quantity=quantity,
        first_trade_id=first_trade_id,
        last_trade_id=last_trade_id,
        trade_time_ms=_required_int(payload, "T", positive=True),
        event_time_ms=_required_int(payload, "E", positive=True),
        buyer_is_maker=_required_bool(payload, "m"),
        normal_quantity=normal_quantity,
    )


def parse_binance_futures_book_ticker_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> BinanceBookTicker:
    _validate_usdm_stream_type(payload)
    symbol = _validate_symbol(
        payload,
        expected_symbol=expected_symbol,
        stream_name="bookTicker",
    )
    bid = _decimal_field(payload, "b", positive=True)
    ask = _decimal_field(payload, "a", positive=True)
    if ask < bid:
        raise FuturesStreamParseError("bookTicker ask must be greater than or equal to bid")

    return BinanceBookTicker(
        symbol=symbol,
        bid=bid,
        ask=ask,
        bid_qty=_decimal_field(payload, "B", positive=False),
        ask_qty=_decimal_field(payload, "A", positive=False),
        update_id=_optional_positive_int(payload, "u"),
        event_time_ms=_optional_positive_int(payload, "E"),
        transaction_time_ms=_optional_positive_int(payload, "T"),
    )


def sample_second_ms_for_source_time(source_time_ms: int) -> int:
    return (source_time_ms // 1000) * 1000


def decimal_ratio(numerator: Decimal, denominator: Decimal) -> Optional[Decimal]:
    if denominator <= 0:
        return None
    return numerator / denominator


def _rolling_cutoff(sample_second_ms: int, window_seconds: int) -> int:
    return sample_second_ms - ((window_seconds - 1) * 1000)


class FlowAggregator:
    def __init__(self, *, venue: str, symbol: str) -> None:
        self.venue = venue
        self.symbol = symbol
        self._buckets: dict[int, FlowBucket] = {}
        self._last_flushed_sample_second_ms: Optional[int] = None
        self._cvd_quote = ZERO
        self._rolling_10s: Deque[tuple[int, Decimal, Decimal]] = deque()
        self._rolling_30s: Deque[tuple[int, Decimal, Decimal]] = deque()

    def add_trade(self, trade: BinanceAggTrade, *, received_ms: int) -> bool:
        sample_second_ms = sample_second_ms_for_source_time(trade.trade_time_ms)
        if (
            self._last_flushed_sample_second_ms is not None
            and sample_second_ms <= self._last_flushed_sample_second_ms
        ):
            return False

        bucket = self._buckets.get(sample_second_ms)
        if bucket is None:
            bucket = FlowBucket(
                sample_second_ms=sample_second_ms,
                window=market_for_sample_second(sample_second_ms),
                received_ms=received_ms,
            )
            self._buckets[sample_second_ms] = bucket

        bucket.add_trade(trade, received_ms=received_ms)
        return True

    def flush_ready(
        self,
        *,
        now_ms: int,
        flush_delay_ms: int,
    ) -> list[BinanceFlowSample]:
        cutoff_ms = sample_second_ms_for_source_time(now_ms - max(0, flush_delay_ms))
        ready_existing = [
            sample_second_ms
            for sample_second_ms in self._buckets
            if sample_second_ms <= cutoff_ms
        ]
        if not ready_existing:
            return []

        max_ready_second_ms = max(ready_existing)
        if (
            self._last_flushed_sample_second_ms is not None
            and max_ready_second_ms <= self._last_flushed_sample_second_ms
        ):
            return []

        if self._last_flushed_sample_second_ms is None:
            start_second_ms = min(ready_existing)
        else:
            start_second_ms = self._last_flushed_sample_second_ms + 1000

        samples = []
        sample_second_ms = start_second_ms
        while sample_second_ms <= max_ready_second_ms:
            bucket = self._buckets.pop(sample_second_ms, None)
            if bucket is None:
                bucket = FlowBucket(
                    sample_second_ms=sample_second_ms,
                    window=market_for_sample_second(sample_second_ms),
                    received_ms=now_ms,
                )
            samples.append(self._build_sample(bucket))
            self._last_flushed_sample_second_ms = sample_second_ms
            sample_second_ms += 1000

        return samples

    def _build_sample(self, bucket: FlowBucket) -> BinanceFlowSample:
        delta_quote = bucket.buy_quote - bucket.sell_quote
        total_quote = bucket.buy_quote + bucket.sell_quote
        taker_imbalance = decimal_ratio(delta_quote, total_quote)

        self._cvd_quote += delta_quote
        self._rolling_10s.append((bucket.sample_second_ms, delta_quote, total_quote))
        self._rolling_30s.append((bucket.sample_second_ms, delta_quote, total_quote))
        self._trim_rolling(
            self._rolling_10s,
            cutoff_ms=_rolling_cutoff(bucket.sample_second_ms, 10),
        )
        self._trim_rolling(
            self._rolling_30s,
            cutoff_ms=_rolling_cutoff(bucket.sample_second_ms, 30),
        )

        cvd_10s, imbalance_10s = self._rolling_values(self._rolling_10s)
        cvd_30s, imbalance_30s = self._rolling_values(self._rolling_30s)

        return BinanceFlowSample(
            venue=self.venue,
            symbol=self.symbol,
            window=bucket.window,
            sample_second_ms=bucket.sample_second_ms,
            buy_base=bucket.buy_base,
            sell_base=bucket.sell_base,
            buy_quote=bucket.buy_quote,
            sell_quote=bucket.sell_quote,
            delta_quote=delta_quote,
            total_quote=total_quote,
            taker_imbalance=taker_imbalance,
            cvd_quote=self._cvd_quote,
            cvd_10s=cvd_10s,
            cvd_30s=cvd_30s,
            imbalance_10s=imbalance_10s,
            imbalance_30s=imbalance_30s,
            agg_trade_count=bucket.agg_trade_count,
            trade_count=bucket.trade_count,
            max_trade_quote=bucket.max_trade_quote,
            first_agg_trade_id=bucket.first_agg_trade_id,
            last_agg_trade_id=bucket.last_agg_trade_id,
            last_trade_time_ms=bucket.last_trade_time_ms,
            last_event_time_ms=bucket.last_event_time_ms,
            received_ms=bucket.received_ms,
        )

    @staticmethod
    def _trim_rolling(
        rolling: Deque[tuple[int, Decimal, Decimal]],
        *,
        cutoff_ms: int,
    ) -> None:
        while rolling and rolling[0][0] < cutoff_ms:
            rolling.popleft()

    @staticmethod
    def _rolling_values(
        rolling: Deque[tuple[int, Decimal, Decimal]]
    ) -> tuple[Decimal, Optional[Decimal]]:
        delta_sum = sum((item[1] for item in rolling), ZERO)
        total_sum = sum((item[2] for item in rolling), ZERO)
        return delta_sum, decimal_ratio(delta_sum, total_sum)


class BookTickerAggregator:
    def __init__(self, *, venue: str, symbol: str) -> None:
        self.venue = venue
        self.symbol = symbol
        self._snapshots: dict[int, BinanceBookSample] = {}
        self._last_flushed_sample_second_ms: Optional[int] = None

    def update(self, ticker: BinanceBookTicker, *, received_ms: int) -> bool:
        source_time_ms = ticker.source_time_ms or received_ms
        sample_second_ms = sample_second_ms_for_source_time(source_time_ms)
        if (
            self._last_flushed_sample_second_ms is not None
            and sample_second_ms <= self._last_flushed_sample_second_ms
        ):
            return False

        sample = build_book_sample(
            venue=self.venue,
            ticker=ticker,
            sample_second_ms=sample_second_ms,
            received_ms=received_ms,
        )
        existing = self._snapshots.get(sample_second_ms)
        if existing is None or sample.ordering_time_ms >= existing.ordering_time_ms:
            self._snapshots[sample_second_ms] = sample

        return True

    def flush_ready(
        self,
        *,
        now_ms: int,
        flush_delay_ms: int,
    ) -> list[BinanceBookSample]:
        cutoff_ms = sample_second_ms_for_source_time(now_ms - max(0, flush_delay_ms))
        ready_seconds = sorted(
            sample_second_ms
            for sample_second_ms in self._snapshots
            if sample_second_ms <= cutoff_ms
        )
        samples = []
        for sample_second_ms in ready_seconds:
            sample = self._snapshots.pop(sample_second_ms)
            samples.append(sample)
            self._last_flushed_sample_second_ms = sample_second_ms

        return samples


class AsyncFlowAggregator:
    def __init__(self, *, venue: str, symbol: str) -> None:
        self._aggregator = FlowAggregator(venue=venue, symbol=symbol)
        self._lock = asyncio.Lock()

    async def add_trade(self, trade: BinanceAggTrade, *, received_ms: int) -> bool:
        async with self._lock:
            return self._aggregator.add_trade(trade, received_ms=received_ms)

    async def flush_ready(
        self,
        *,
        now_ms: int,
        flush_delay_ms: int,
    ) -> list[BinanceFlowSample]:
        async with self._lock:
            return self._aggregator.flush_ready(
                now_ms=now_ms,
                flush_delay_ms=flush_delay_ms,
            )


class AsyncBookTickerAggregator:
    def __init__(self, *, venue: str, symbol: str) -> None:
        self._aggregator = BookTickerAggregator(venue=venue, symbol=symbol)
        self._lock = asyncio.Lock()

    async def update(self, ticker: BinanceBookTicker, *, received_ms: int) -> bool:
        async with self._lock:
            return self._aggregator.update(ticker, received_ms=received_ms)

    async def flush_ready(
        self,
        *,
        now_ms: int,
        flush_delay_ms: int,
    ) -> list[BinanceBookSample]:
        async with self._lock:
            return self._aggregator.flush_ready(
                now_ms=now_ms,
                flush_delay_ms=flush_delay_ms,
            )


def build_book_sample(
    *,
    venue: str,
    ticker: BinanceBookTicker,
    sample_second_ms: int,
    received_ms: int,
) -> BinanceBookSample:
    mid = (ticker.bid + ticker.ask) / TWO
    spread = ticker.ask - ticker.bid
    spread_bps = spread / mid * TEN_THOUSAND
    total_qty = ticker.bid_qty + ticker.ask_qty
    book_imbalance = decimal_ratio(ticker.bid_qty - ticker.ask_qty, total_qty)
    microprice = (
        None
        if total_qty <= 0
        else (ticker.ask * ticker.bid_qty + ticker.bid * ticker.ask_qty) / total_qty
    )

    return BinanceBookSample(
        venue=venue,
        symbol=ticker.symbol,
        window=market_for_sample_second(sample_second_ms),
        sample_second_ms=sample_second_ms,
        bid=ticker.bid,
        ask=ticker.ask,
        bid_qty=ticker.bid_qty,
        ask_qty=ticker.ask_qty,
        mid=mid,
        spread=spread,
        spread_bps=spread_bps,
        book_imbalance=book_imbalance,
        microprice=microprice,
        update_id=ticker.update_id,
        event_time_ms=ticker.event_time_ms,
        transaction_time_ms=ticker.transaction_time_ms,
        received_ms=received_ms,
    )


def _record_raw_capture_drop(
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
                "binance_futures_raw_capture_drop_counter_failed",
                extra={"event": "binance_futures_raw_capture_drop_counter_failed"},
            )
    if session is not None:
        try:
            session.mark_record_dropped()
        except Exception:
            LOGGER.exception(
                "binance_futures_raw_capture_session_drop_counter_failed",
                extra={
                    "event": "binance_futures_raw_capture_session_drop_counter_failed"
                },
            )


def _offer_raw_capture_record(
    raw_capture: Any,
    record: Any,
    *,
    session: Optional[FeedSession],
) -> bool:
    try:
        result = raw_capture.offer_nowait(record)
    except Exception:
        _record_raw_capture_drop(
            raw_capture,
            session,
            increment_global=True,
        )
        LOGGER.exception(
            "binance_futures_raw_capture_offer_failed",
            extra={"event": "binance_futures_raw_capture_offer_failed"},
        )
        return False

    if not result.accepted:
        _record_raw_capture_drop(
            raw_capture,
            session,
            increment_global=False,
        )
        return False

    dropped_record = result.dropped_record if result.dropped_oldest else None
    if (
        session is not None
        and isinstance(
            dropped_record,
            (BinanceFuturesPriceTrace, FeedSessionRecord),
        )
        and dropped_record.connection_id == session.connection_id
    ):
        _record_raw_capture_drop(
            raw_capture,
            session,
            increment_global=False,
        )
    return True


def _mark_capture_message(
    session: Optional[FeedSession],
    method_name: str,
) -> None:
    if session is None:
        return
    try:
        getattr(session, method_name)()
    except Exception:
        LOGGER.exception(
            "binance_futures_raw_capture_session_counter_failed",
            extra={
                "event": "binance_futures_raw_capture_session_counter_failed",
                "counter": method_name,
            },
        )


def _capture_futures_trade(
    *,
    raw_capture: Any,
    session: FeedSession,
    coalescer: FuturesPriceTraceCoalescer,
    trade: BinanceAggTrade,
    stamp: ReceiveStamp,
) -> None:
    try:
        completed = coalescer.add_trade(
            FuturesTradeObservation(
                connection_id=stamp.connection_id,
                received_wall_ns=stamp.received_wall_ns,
                received_monotonic_ns=stamp.received_monotonic_ns,
                trade_time_ms=trade.trade_time_ms,
                event_time_ms=trade.event_time_ms,
                price=trade.price,
                agg_trade_id=trade.agg_trade_id,
            )
        )
    except Exception:
        _record_raw_capture_drop(
            raw_capture,
            session,
            increment_global=True,
        )
        LOGGER.exception(
            "binance_futures_raw_capture_coalesce_failed",
            extra={"event": "binance_futures_raw_capture_coalesce_failed"},
        )
        return

    if completed is not None:
        _offer_raw_capture_record(
            raw_capture,
            completed,
            session=session,
        )


def _seal_futures_capture(
    *,
    raw_capture: Any,
    session: FeedSession,
    coalescer: FuturesPriceTraceCoalescer,
    idle: bool,
) -> None:
    try:
        if idle:
            completed = coalescer.finish_if_elapsed(
                now_wall_ns=time.time_ns(),
                now_monotonic_ns=time.monotonic_ns(),
            )
        else:
            completed = coalescer.finish()
    except Exception:
        _record_raw_capture_drop(
            raw_capture,
            session,
            increment_global=True,
        )
        LOGGER.exception(
            "binance_futures_raw_capture_seal_failed",
            extra={"event": "binance_futures_raw_capture_seal_failed"},
        )
        return

    if completed is not None:
        _offer_raw_capture_record(
            raw_capture,
            completed,
            session=session,
        )


def _finish_feed_session(
    *,
    raw_capture: Any,
    session: FeedSession,
    close_reason: str,
) -> None:
    try:
        record = session.finish(close_reason=close_reason)
    except Exception:
        LOGGER.exception(
            "binance_futures_raw_capture_session_finish_failed",
            extra={"event": "binance_futures_raw_capture_session_finish_failed"},
        )
        return
    _offer_raw_capture_record(raw_capture, record, session=None)


async def futures_raw_capture_telemetry_loop(
    *,
    raw_capture: Any,
    trade_state: FuturesTradeState,
    interval_seconds: float = 60.0,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("raw capture telemetry interval must be positive")

    while True:
        await asyncio.sleep(interval_seconds)
        try:
            snapshot = raw_capture.counters.snapshot(
                queue_depth=raw_capture.buffer.qsize(),
                connection_id=trade_state.current_connection_id,
            )
            fields = {
                "event": "raw_capture_summary",
                "source": "binance_futures_agg_trade",
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
                trade_state.telemetry_fields(now_ms=current_utc_epoch_ms())
            )
            LOGGER.info("raw_capture_summary", extra=fields)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "raw_capture_summary_failed",
                extra={
                    "event": "raw_capture_summary_failed",
                    "source": "binance_futures_agg_trade",
                },
            )


def _call_microstructure_sink(
    microstructure_sink: Any,
    method_name: str,
    *args: Any,
) -> None:
    if microstructure_sink is None:
        return

    try:
        callback = getattr(microstructure_sink, method_name)
        callback(*args)
    except Exception:
        now = time.monotonic()
        previous = _MICROSTRUCTURE_SINK_LAST_LOG_AT.get(method_name)
        if (
            previous is None
            or now - previous >= MICROSTRUCTURE_SINK_LOG_INTERVAL_SECONDS
        ):
            _MICROSTRUCTURE_SINK_LAST_LOG_AT[method_name] = now
            LOGGER.exception(
                "binance_futures_microstructure_sink_failed",
                extra={
                    "event": "binance_futures_microstructure_sink_failed",
                    "method": method_name,
                },
            )


async def futures_agg_trade_reader_loop(
    settings: Settings,
    flow_store: AsyncFlowAggregator,
    *,
    trade_state: FuturesTradeState,
    raw_capture: Any = None,
    microstructure_sink: Any = None,
) -> None:
    attempt = 0

    while True:
        connection_id: Optional[UUID] = None
        capture_session: Optional[FeedSession] = None
        capture_coalescer: Optional[FuturesPriceTraceCoalescer] = None
        close_reason = "error"
        reconnect_error: Optional[Exception] = None
        local_receive_sequence = 0
        try:
            LOGGER.info(
                "binance_futures_agg_trade_websocket_connecting",
                extra={
                    "event": "binance_futures_agg_trade_websocket_connecting",
                    "url": settings.BINANCE_FUTURES_AGG_TRADE_WS_URL,
                },
            )
            async with websockets.connect(
                settings.BINANCE_FUTURES_AGG_TRADE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as websocket:
                attempt = 0
                connected_at = time.monotonic()
                connection_id = uuid4()
                trade_state.connection_opened(connection_id)
                if microstructure_sink is not None:
                    _call_microstructure_sink(
                        microstructure_sink,
                        "connection_opened",
                        "futures_trade",
                        current_utc_epoch_ms(),
                    )

                if raw_capture is not None and connection_id is not None:
                    try:
                        capture_session = FeedSession(
                            source="binance_futures_agg_trade",
                            connection_id=connection_id,
                            connected_wall_ns=time.time_ns(),
                            connected_monotonic_ns=time.monotonic_ns(),
                            counters=raw_capture.counters,
                        )
                        capture_session.mark_ready(
                            ready_wall_ns=time.time_ns(),
                            ready_monotonic_ns=time.monotonic_ns(),
                        )
                        capture_coalescer = FuturesPriceTraceCoalescer(
                            bucket_ms=settings.RAW_FUTURES_BUCKET_MS,
                            counters=raw_capture.counters,
                        )
                        _offer_raw_capture_record(
                            raw_capture,
                            capture_session.opened_record(),
                            session=capture_session,
                        )
                    except Exception:
                        capture_session = None
                        capture_coalescer = None
                        try:
                            raw_capture.counters.record_dropped()
                        except Exception:
                            pass
                        LOGGER.exception(
                            "binance_futures_raw_capture_session_start_failed",
                            extra={
                                "event": (
                                    "binance_futures_raw_capture_session_start_failed"
                                )
                            },
                        )

                LOGGER.info(
                    "binance_futures_agg_trade_websocket_connected",
                    extra={
                        "event": "binance_futures_agg_trade_websocket_connected",
                        "url": settings.BINANCE_FUTURES_AGG_TRADE_WS_URL,
                        "symbol": settings.BINANCE_FUTURES_SYMBOL,
                        "connection_id": connection_id,
                    },
                )

                while True:
                    connected_seconds = time.monotonic() - connected_at
                    remaining_seconds = BINANCE_RECONNECT_SECONDS - connected_seconds
                    if remaining_seconds <= 0:
                        LOGGER.info(
                            "binance_futures_agg_trade_proactive_reconnect",
                            extra={
                                "event": "binance_futures_agg_trade_proactive_reconnect",
                                "connected_seconds": int(connected_seconds),
                                "connection_id": connection_id,
                            },
                        )
                        close_reason = "proactive_reconnect"
                        break

                    receive_timeout = min(30.0, remaining_seconds)
                    if (
                        capture_coalescer is not None
                        and capture_coalescer.pending_event_count
                    ):
                        receive_timeout = min(0.1, remaining_seconds)
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=receive_timeout,
                        )
                    except asyncio.TimeoutError:
                        if (
                            raw_capture is not None
                            and capture_session is not None
                            and capture_coalescer is not None
                        ):
                            _seal_futures_capture(
                                raw_capture=raw_capture,
                                session=capture_session,
                                coalescer=capture_coalescer,
                                idle=True,
                            )
                        continue

                    received_wall_ns = time.time_ns()
                    stamp: Optional[ReceiveStamp] = None
                    if capture_session is not None:
                        received_monotonic_ns = time.monotonic_ns()
                        try:
                            stamp = capture_session.next_receive_stamp(
                                received_wall_ns=received_wall_ns,
                                received_monotonic_ns=received_monotonic_ns,
                            )
                            local_receive_sequence = stamp.receive_sequence
                        except Exception:
                            local_receive_sequence += 1
                            stamp = ReceiveStamp(
                                connection_id=connection_id,
                                receive_sequence=local_receive_sequence,
                                received_wall_ns=received_wall_ns,
                                received_monotonic_ns=received_monotonic_ns,
                            )
                            _record_raw_capture_drop(
                                raw_capture,
                                capture_session,
                                increment_global=True,
                            )
                            LOGGER.exception(
                                "binance_futures_raw_capture_receive_stamp_failed",
                                extra={
                                    "event": (
                                        "binance_futures_raw_capture_receive_stamp_failed"
                                    )
                                },
                            )
                    elif connection_id is not None:
                        received_monotonic_ns = time.monotonic_ns()
                        local_receive_sequence += 1
                        stamp = ReceiveStamp(
                            connection_id=connection_id,
                            receive_sequence=local_receive_sequence,
                            received_wall_ns=received_wall_ns,
                            received_monotonic_ns=received_monotonic_ns,
                        )

                    try:
                        payload = json.loads(message)
                        if not isinstance(payload, Mapping):
                            raise FuturesStreamParseError(
                                "aggTrade payload must be an object"
                            )
                        trade = parse_binance_futures_agg_trade_payload(
                            payload,
                            expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
                            # `nq` is optional RPI research context. A malformed
                            # optional field must not suppress the critical
                            # Phase 5 last-price and normal flow paths.
                            strict_normal_quantity=False,
                        )
                    except (
                        json.JSONDecodeError,
                        FuturesStreamParseError,
                        TypeError,
                    ) as exc:
                        _mark_capture_message(capture_session, "mark_parse_error")
                        _call_microstructure_sink(
                            microstructure_sink,
                            "record_error",
                            "futures_trade",
                            received_wall_ns // 1_000_000,
                            exc,
                        )
                        LOGGER.warning(
                            "binance_futures_agg_trade_message_skipped",
                            extra={
                                "event": "binance_futures_agg_trade_message_skipped",
                                "error": str(exc),
                            },
                        )
                        continue

                    _mark_capture_message(capture_session, "mark_accepted")
                    accepted_current_trade = None
                    if stamp is not None:
                        accepted_current_trade = trade_state.update_ws(trade, stamp)

                    received_ms = received_wall_ns // 1_000_000
                    if accepted_current_trade is not None:
                        _call_microstructure_sink(
                            microstructure_sink,
                            "offer_futures_trade",
                            trade,
                            received_ms,
                        )

                    if (
                        raw_capture is not None
                        and capture_session is not None
                        and capture_coalescer is not None
                        and stamp is not None
                    ):
                        _capture_futures_trade(
                            raw_capture=raw_capture,
                            session=capture_session,
                            coalescer=capture_coalescer,
                            trade=trade,
                            stamp=stamp,
                        )

                    accepted = await flow_store.add_trade(trade, received_ms=received_ms)
                    if not accepted:
                        LOGGER.debug(
                            "binance_futures_agg_trade_late_message_skipped",
                            extra={
                                "event": "binance_futures_agg_trade_late_message_skipped",
                                "trade_time_ms": trade.trade_time_ms,
                                "received_ms": received_ms,
                            },
                        )
        except asyncio.CancelledError:
            close_reason = "cancelled"
            raise
        except Exception as exc:
            reconnect_error = exc
            clean_close_type = getattr(websockets, "ConnectionClosedOK", None)
            if (
                clean_close_type is not None
                and isinstance(exc, clean_close_type)
            ) or exc.__class__.__name__ == "ConnectionClosedOK":
                close_reason = "remote_close"
        finally:
            if (
                raw_capture is not None
                and capture_session is not None
                and capture_coalescer is not None
            ):
                _seal_futures_capture(
                    raw_capture=raw_capture,
                    session=capture_session,
                    coalescer=capture_coalescer,
                    idle=False,
                )
                _finish_feed_session(
                    raw_capture=raw_capture,
                    session=capture_session,
                    close_reason=close_reason,
                )
            if connection_id is not None:
                trade_state.connection_closed(connection_id)
                if close_reason != "cancelled" and microstructure_sink is not None:
                    _call_microstructure_sink(
                        microstructure_sink,
                        "connection_closed",
                        "futures_trade",
                        current_utc_epoch_ms(),
                    )

        if reconnect_error is not None:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "binance_futures_agg_trade_websocket_reconnect_scheduled",
                extra={
                    "event": "binance_futures_agg_trade_websocket_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(reconnect_error),
                    "connection_id": connection_id,
                },
            )
            await asyncio.sleep(delay)


async def futures_book_ticker_reader_loop(
    settings: Settings,
    book_store: AsyncBookTickerAggregator,
) -> None:
    attempt = 0

    while True:
        try:
            LOGGER.info(
                "binance_futures_book_ticker_websocket_connecting",
                extra={
                    "event": "binance_futures_book_ticker_websocket_connecting",
                    "url": settings.BINANCE_FUTURES_BOOK_TICKER_WS_URL,
                },
            )
            async with websockets.connect(
                settings.BINANCE_FUTURES_BOOK_TICKER_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as websocket:
                attempt = 0
                connected_at = time.monotonic()
                LOGGER.info(
                    "binance_futures_book_ticker_websocket_connected",
                    extra={
                        "event": "binance_futures_book_ticker_websocket_connected",
                        "url": settings.BINANCE_FUTURES_BOOK_TICKER_WS_URL,
                        "symbol": settings.BINANCE_FUTURES_SYMBOL,
                    },
                )

                while True:
                    connected_seconds = time.monotonic() - connected_at
                    remaining_seconds = BINANCE_RECONNECT_SECONDS - connected_seconds
                    if remaining_seconds <= 0:
                        LOGGER.info(
                            "binance_futures_book_ticker_proactive_reconnect",
                            extra={
                                "event": "binance_futures_book_ticker_proactive_reconnect",
                                "connected_seconds": int(connected_seconds),
                            },
                        )
                        break

                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=min(30.0, remaining_seconds),
                        )
                    except asyncio.TimeoutError:
                        continue

                    try:
                        payload = json.loads(message)
                        ticker = parse_binance_futures_book_ticker_payload(
                            payload,
                            expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
                        )
                    except (json.JSONDecodeError, FuturesStreamParseError) as exc:
                        LOGGER.warning(
                            "binance_futures_book_ticker_message_skipped",
                            extra={
                                "event": "binance_futures_book_ticker_message_skipped",
                                "error": str(exc),
                            },
                        )
                        continue

                    received_ms = current_utc_epoch_ms()
                    accepted = await book_store.update(ticker, received_ms=received_ms)
                    if not accepted:
                        LOGGER.debug(
                            "binance_futures_book_ticker_late_message_skipped",
                            extra={
                                "event": "binance_futures_book_ticker_late_message_skipped",
                                "source_time_ms": ticker.source_time_ms,
                                "received_ms": received_ms,
                            },
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "binance_futures_book_ticker_websocket_reconnect_scheduled",
                extra={
                    "event": "binance_futures_book_ticker_websocket_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay)


async def futures_flow_flush_loop(
    *,
    pool: Any,
    settings: Settings,
    flow_store: AsyncFlowAggregator,
) -> None:
    sleep_seconds = max(0.05, settings.BINANCE_FUTURES_STREAM_FLUSH_SECONDS)
    while True:
        await asyncio.sleep(sleep_seconds)
        try:
            samples = await flow_store.flush_ready(
                now_ms=current_utc_epoch_ms(),
                flush_delay_ms=settings.BINANCE_FUTURES_FLOW_FLUSH_DELAY_MS,
            )
            for sample in samples:
                await upsert_binance_flow_1s(
                    pool,
                    venue=sample.venue,
                    symbol=sample.symbol,
                    window=sample.window,
                    sample_second_ms=sample.sample_second_ms,
                    buy_base=sample.buy_base,
                    sell_base=sample.sell_base,
                    buy_quote=sample.buy_quote,
                    sell_quote=sample.sell_quote,
                    delta_quote=sample.delta_quote,
                    total_quote=sample.total_quote,
                    taker_imbalance=sample.taker_imbalance,
                    cvd_quote=sample.cvd_quote,
                    cvd_10s=sample.cvd_10s,
                    cvd_30s=sample.cvd_30s,
                    imbalance_10s=sample.imbalance_10s,
                    imbalance_30s=sample.imbalance_30s,
                    agg_trade_count=sample.agg_trade_count,
                    trade_count=sample.trade_count,
                    max_trade_quote=sample.max_trade_quote,
                    first_agg_trade_id=sample.first_agg_trade_id,
                    last_agg_trade_id=sample.last_agg_trade_id,
                    last_trade_time_ms=sample.last_trade_time_ms,
                    last_event_time_ms=sample.last_event_time_ms,
                    received_ms=sample.received_ms,
                )

            if samples:
                LOGGER.info(
                    "binance_futures_flow_rows_written",
                    extra={
                        "event": "binance_futures_flow_rows_written",
                        "count": len(samples),
                        "last_sample_second_ms": samples[-1].sample_second_ms,
                    },
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "binance_futures_flow_flush_failed",
                extra={"event": "binance_futures_flow_flush_failed"},
            )


async def futures_book_flush_loop(
    *,
    pool: Any,
    settings: Settings,
    book_store: AsyncBookTickerAggregator,
) -> None:
    sleep_seconds = max(0.05, settings.BINANCE_FUTURES_STREAM_FLUSH_SECONDS)
    while True:
        await asyncio.sleep(sleep_seconds)
        try:
            samples = await book_store.flush_ready(
                now_ms=current_utc_epoch_ms(),
                flush_delay_ms=settings.BINANCE_FUTURES_BOOK_FLUSH_DELAY_MS,
            )
            for sample in samples:
                await upsert_binance_book_1s(
                    pool,
                    venue=sample.venue,
                    symbol=sample.symbol,
                    window=sample.window,
                    sample_second_ms=sample.sample_second_ms,
                    bid=sample.bid,
                    ask=sample.ask,
                    bid_qty=sample.bid_qty,
                    ask_qty=sample.ask_qty,
                    mid=sample.mid,
                    spread=sample.spread,
                    spread_bps=sample.spread_bps,
                    book_imbalance=sample.book_imbalance,
                    microprice=sample.microprice,
                    update_id=sample.update_id,
                    event_time_ms=sample.event_time_ms,
                    transaction_time_ms=sample.transaction_time_ms,
                    received_ms=sample.received_ms,
                )

            if samples:
                LOGGER.info(
                    "binance_futures_book_rows_written",
                    extra={
                        "event": "binance_futures_book_rows_written",
                        "count": len(samples),
                        "last_sample_second_ms": samples[-1].sample_second_ms,
                    },
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "binance_futures_book_flush_failed",
                extra={"event": "binance_futures_book_flush_failed"},
            )
