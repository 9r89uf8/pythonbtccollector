import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Deque, Mapping, Optional

import websockets

from price_collector.collector import (
    BINANCE_RECONNECT_SECONDS,
    current_utc_epoch_ms,
    reconnect_delay_seconds,
)
from price_collector.config import Settings
from price_collector.db import upsert_binance_book_1s, upsert_binance_flow_1s
from price_collector.market import MarketWindow, market_for_sample_second


LOGGER = logging.getLogger("price_collector.binance_futures_streams")
ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
TEN_THOUSAND = Decimal("10000")


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

    @property
    def quote_notional(self) -> Decimal:
        return self.price * self.quantity

    @property
    def trade_count(self) -> int:
        return self.last_trade_id - self.first_trade_id + 1


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


def parse_binance_futures_agg_trade_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> BinanceAggTrade:
    symbol = _validate_symbol(
        payload,
        expected_symbol=expected_symbol,
        stream_name="aggTrade",
    )
    first_trade_id = _required_int(payload, "f", positive=False)
    last_trade_id = _required_int(payload, "l", positive=False)
    if last_trade_id < first_trade_id:
        raise FuturesStreamParseError("aggTrade last trade id is before first trade id")

    return BinanceAggTrade(
        symbol=symbol,
        agg_trade_id=_required_int(payload, "a", positive=False),
        price=_decimal_field(payload, "p", positive=True),
        quantity=_decimal_field(payload, "q", positive=True),
        first_trade_id=first_trade_id,
        last_trade_id=last_trade_id,
        trade_time_ms=_required_int(payload, "T", positive=True),
        event_time_ms=_required_int(payload, "E", positive=True),
        buyer_is_maker=_required_bool(payload, "m"),
    )


def parse_binance_futures_book_ticker_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> BinanceBookTicker:
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


async def futures_agg_trade_reader_loop(
    settings: Settings,
    flow_store: AsyncFlowAggregator,
) -> None:
    attempt = 0

    while True:
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
                LOGGER.info(
                    "binance_futures_agg_trade_websocket_connected",
                    extra={
                        "event": "binance_futures_agg_trade_websocket_connected",
                        "url": settings.BINANCE_FUTURES_AGG_TRADE_WS_URL,
                        "symbol": settings.BINANCE_FUTURES_SYMBOL,
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
                        trade = parse_binance_futures_agg_trade_payload(
                            payload,
                            expected_symbol=settings.BINANCE_FUTURES_SYMBOL,
                        )
                    except (json.JSONDecodeError, FuturesStreamParseError) as exc:
                        LOGGER.warning(
                            "binance_futures_agg_trade_message_skipped",
                            extra={
                                "event": "binance_futures_agg_trade_message_skipped",
                                "error": str(exc),
                            },
                        )
                        continue

                    received_ms = current_utc_epoch_ms()
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
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "binance_futures_agg_trade_websocket_reconnect_scheduled",
                extra={
                    "event": "binance_futures_agg_trade_websocket_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
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
