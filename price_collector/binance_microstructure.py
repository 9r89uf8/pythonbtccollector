"""Decimal-only causal state for one-second Binance microstructure rows.

This module deliberately contains no network or database code.  Readers put
validated observations into :class:`MicrostructureEventSink`; a single
consumer orders them by wall receive time and calls :func:`finalize_boundary`.
Financial values remain ``Decimal`` from parsing through row construction.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Mapping, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:  # Imports are documentation-only and cannot create cycles.
    from price_collector.binance_futures_collector import BinanceFuturesSnapshot
    from price_collector.binance_futures_streams import BinanceAggTrade


SCHEMA_VERSION = 1
ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
TEN_THOUSAND = Decimal("10000")
MAX_SAMPLE_JITTER_MS = 500
REQUIRED_CONNECTION_SOURCES = frozenset(
    {"spot", "futures_trade", "futures_depth", "futures_liquidation"}
)


class MicrostructureParseError(ValueError):
    """A Binance microstructure payload violated the strict wire contract."""


@dataclass(frozen=True, order=True)
class QueuedEvent:
    """An event ordered first by receive time and then by local sequence."""

    received_ms: int
    sequence: int
    kind: str = field(compare=False)
    payload: Any = field(compare=False)


@dataclass(frozen=True)
class Top10BookSnapshot:
    symbol: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    event_time_ms: Optional[int]


@dataclass(frozen=True)
class LiquidationFill:
    symbol: str
    side: str
    price: Decimal
    quantity: Decimal
    event_time_ms: int


@dataclass(frozen=True)
class AggTradeObservation:
    symbol: str
    price: Decimal
    quantity: Decimal
    buyer_is_maker: bool
    trade_count: int
    event_time_ms: int
    normal_quantity: Optional[Decimal] = None


def _required_ms(value: Any, name: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or isinstance(value, float):
        raise MicrostructureParseError(f"{name} must be an integer")
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise MicrostructureParseError(f"{name} must be an integer")
        result = int(value)
    elif isinstance(value, int):
        result = value
    elif isinstance(value, str):
        if not value or value.strip() != value:
            raise MicrostructureParseError(f"{name} must be an integer")
        try:
            result = int(value)
        except ValueError as exc:
            raise MicrostructureParseError(f"{name} must be an integer") from exc
    else:
        raise MicrostructureParseError(f"{name} must be an integer")
    if positive and result <= 0:
        raise MicrostructureParseError(f"{name} must be positive")
    return result


def _optional_ms(value: Any, name: str) -> Optional[int]:
    if value is None or value == "":
        return None
    return _required_ms(value, name)


def _decimal(
    value: Any,
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    # A float has already rounded the provider's decimal text, so accepting one
    # here would break the collector's Decimal-only guarantee.
    if isinstance(value, (bool, float)):
        raise MicrostructureParseError(f"{name} must be decimal text or Decimal")
    try:
        if isinstance(value, Decimal):
            result = value
        elif isinstance(value, str):
            if not value or value.strip() != value:
                raise InvalidOperation
            result = Decimal(value)
        elif isinstance(value, int):
            result = Decimal(value)
        else:
            raise InvalidOperation
    except (InvalidOperation, ValueError) as exc:
        raise MicrostructureParseError(f"invalid {name}: {value!r}") from exc
    if not result.is_finite():
        raise MicrostructureParseError(f"{name} must be finite")
    if positive and result <= ZERO:
        raise MicrostructureParseError(f"{name} must be positive")
    if nonnegative and result < ZERO:
        raise MicrostructureParseError(f"{name} must be non-negative")
    return result


def _optional_decimal(
    value: Any,
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    return _decimal(
        value,
        name,
        positive=positive,
        nonnegative=nonnegative,
    )


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MicrostructureParseError(f"{name} must be an object")
    return value


def _unwrap_payload(payload: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    outer = _mapping(payload, "payload")
    data = outer.get("data")
    if data is None:
        return str(outer.get("stream", "")), outer
    return str(outer.get("stream", "")), _mapping(data, "payload.data")


def _validate_symbol(
    stream: str,
    data: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> str:
    expected = expected_symbol.upper()
    payload_symbol = data.get("s", data.get("symbol"))
    if payload_symbol is not None:
        if not isinstance(payload_symbol, str) or payload_symbol.upper() != expected:
            raise MicrostructureParseError(
                f"unexpected symbol: expected {expected!r}, got {payload_symbol!r}"
            )
    if "@" in stream:
        stream_symbol = stream.split("@", 1)[0].upper()
        if stream_symbol and not stream_symbol.startswith("!") and stream_symbol != expected:
            raise MicrostructureParseError(
                f"unexpected stream symbol: expected {expected!r}, got {stream_symbol!r}"
            )
    return expected


def _validate_usdm(data: Mapping[str, Any]) -> None:
    subtype = data.get("st")
    if subtype is None:
        return
    if isinstance(subtype, bool) or isinstance(subtype, float):
        raise MicrostructureParseError("futures st must identify USD-M")
    if subtype not in (1, "1"):
        raise MicrostructureParseError("coin-margined futures payload is not accepted")


def _parse_levels(value: Any, name: str) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise MicrostructureParseError(f"{name} must be an array of price levels")
    if not value:
        raise MicrostructureParseError(f"{name} must not be empty")
    levels: list[tuple[Decimal, Decimal]] = []
    for index, level in enumerate(value[:10]):
        if not isinstance(level, Sequence) or isinstance(level, (str, bytes, bytearray)):
            raise MicrostructureParseError(f"{name}[{index}] must be a price/quantity pair")
        if len(level) < 2:
            raise MicrostructureParseError(f"{name}[{index}] must be a price/quantity pair")
        levels.append(
            (
                _decimal(level[0], f"{name}[{index}].price", positive=True),
                _decimal(level[1], f"{name}[{index}].quantity", positive=True),
            )
        )
    return tuple(levels)


def _parse_depth_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
    futures: bool,
) -> Top10BookSnapshot:
    stream, data = _unwrap_payload(payload)
    symbol = _validate_symbol(stream, data, expected_symbol=expected_symbol)
    if futures:
        _validate_usdm(data)
    bid_data = data.get("b" if futures else "bids")
    ask_data = data.get("a" if futures else "asks")
    if bid_data is None:
        bid_data = data.get("bids" if futures else "b")
    if ask_data is None:
        ask_data = data.get("asks" if futures else "a")
    bids = tuple(sorted(_parse_levels(bid_data, "bids"), reverse=True))
    asks = tuple(sorted(_parse_levels(ask_data, "asks")))
    if len(bids) < 10 or len(asks) < 10:
        raise MicrostructureParseError(
            "top-10 book snapshot must contain ten bid and ask levels"
        )
    if bids[0][0] > asks[0][0]:
        raise MicrostructureParseError("best bid must not exceed best ask")
    raw_event_ms = data.get("T") if futures and data.get("T") is not None else data.get("E")
    event_time_ms = _optional_ms(raw_event_ms, "depth event time")
    return Top10BookSnapshot(
        symbol=symbol,
        bids=bids,
        asks=asks,
        event_time_ms=event_time_ms,
    )


def parse_spot_depth_payload(
    payload: Mapping[str, Any], *, expected_symbol: str = "BTCUSDT"
) -> Top10BookSnapshot:
    return _parse_depth_payload(payload, expected_symbol=expected_symbol, futures=False)


def parse_futures_depth_payload(
    payload: Mapping[str, Any], *, expected_symbol: str = "BTCUSDT"
) -> Top10BookSnapshot:
    return _parse_depth_payload(payload, expected_symbol=expected_symbol, futures=True)


# Descriptive aliases retained for callers that name the actual depth shape.
parse_spot_top10_snapshot = parse_spot_depth_payload
parse_futures_top10_snapshot = parse_futures_depth_payload


def parse_liquidation_payload(
    payload: Mapping[str, Any], *, expected_symbol: str = "BTCUSDT"
) -> LiquidationFill:
    stream, data = _unwrap_payload(payload)
    _validate_usdm(data)
    if data.get("e") not in (None, "forceOrder") and "forceOrder" not in stream:
        raise MicrostructureParseError("payload is not a forceOrder event")
    order = _mapping(data.get("o"), "forceOrder.o")
    symbol = _validate_symbol(stream, order, expected_symbol=expected_symbol)
    side = order.get("S")
    if not isinstance(side, str) or side.upper() not in ("BUY", "SELL"):
        raise MicrostructureParseError("forceOrder side must be BUY or SELL")

    average_value = order.get("ap")
    if average_value in (None, "", "0", 0, ZERO):
        price = _decimal(order.get("p"), "forceOrder price", positive=True)
    else:
        price = _decimal(average_value, "forceOrder average price", positive=True)
    # Only `l` is an incremental executed fill.  Neither original quantity `q`
    # nor cumulative quantity `z` is a safe fallback.
    quantity = _decimal(order.get("l"), "forceOrder last-filled quantity", positive=True)
    event_time_ms = _required_ms(order.get("T"), "forceOrder trade time", positive=True)
    return LiquidationFill(
        symbol=symbol,
        side=side.upper(),
        price=price,
        quantity=quantity,
        event_time_ms=event_time_ms,
    )


parse_futures_force_order = parse_liquidation_payload


def _coerce_trade(
    trade: Any,
    *,
    expected_symbol: str,
    futures: bool,
) -> AggTradeObservation:
    if isinstance(trade, Mapping):
        stream, data = _unwrap_payload(trade)
        symbol = _validate_symbol(stream, data, expected_symbol=expected_symbol)
        if futures:
            _validate_usdm(data)
        event_name = data.get("e")
        if event_name not in (None, "aggTrade") and "aggTrade" not in stream:
            raise MicrostructureParseError("payload is not an aggTrade")
        maker = data.get("m")
        if not isinstance(maker, bool):
            raise MicrostructureParseError("aggTrade m must be boolean")
        first_id = _required_ms(data.get("f", data.get("a")), "aggTrade first id")
        last_id = _required_ms(data.get("l", data.get("a")), "aggTrade last id")
        if last_id < first_id:
            raise MicrostructureParseError("aggTrade last id precedes first id")
        normal = (
            _optional_decimal(data.get("nq"), "aggTrade normal quantity", nonnegative=True)
            if futures
            else None
        )
        return AggTradeObservation(
            symbol=symbol,
            price=_decimal(data.get("p"), "aggTrade price", positive=True),
            quantity=_decimal(data.get("q"), "aggTrade quantity", positive=True),
            buyer_is_maker=maker,
            trade_count=last_id - first_id + 1,
            event_time_ms=_required_ms(
                data.get("T", data.get("E")), "aggTrade event time", positive=True
            ),
            normal_quantity=normal,
        )

    symbol_value = getattr(trade, "symbol", None)
    if not isinstance(symbol_value, str) or symbol_value.upper() != expected_symbol.upper():
        raise MicrostructureParseError(
            f"unexpected aggTrade symbol: expected {expected_symbol!r}, got {symbol_value!r}"
        )
    maker = getattr(trade, "buyer_is_maker", None)
    if not isinstance(maker, bool):
        raise MicrostructureParseError("aggTrade buyer_is_maker must be boolean")
    trade_count_value = getattr(trade, "trade_count", None)
    if trade_count_value is None:
        first_id = _required_ms(getattr(trade, "first_trade_id", None), "first trade id")
        last_id = _required_ms(getattr(trade, "last_trade_id", None), "last trade id")
        trade_count_value = last_id - first_id + 1
    trade_count = _required_ms(trade_count_value, "trade count", positive=True)
    normal_raw = None
    if futures:
        for attribute in ("normal_quantity", "normal_qty", "nq"):
            if hasattr(trade, attribute):
                normal_raw = getattr(trade, attribute)
                break
    normal = _optional_decimal(normal_raw, "aggTrade normal quantity", nonnegative=True)
    event_value = getattr(trade, "trade_time_ms", None)
    if event_value is None:
        event_value = getattr(trade, "event_time_ms", None)
    return AggTradeObservation(
        symbol=symbol_value.upper(),
        price=_decimal(getattr(trade, "price", None), "aggTrade price", positive=True),
        quantity=_decimal(getattr(trade, "quantity", None), "aggTrade quantity", positive=True),
        buyer_is_maker=maker,
        trade_count=trade_count,
        event_time_ms=_required_ms(event_value, "aggTrade event time", positive=True),
        normal_quantity=normal,
    )


def _age(now_ms: int, then_ms: Optional[int]) -> Optional[int]:
    return None if then_ms is None else now_ms - then_ms


def _lag(received_ms: int, event_ms: Optional[int]) -> Optional[int]:
    return None if event_ms is None else received_ms - event_ms


@dataclass
class TradeAccumulator:
    buy_quote: Decimal = ZERO
    sell_quote: Decimal = ZERO
    rpi_buy_quote: Decimal = ZERO
    rpi_sell_quote: Decimal = ZERO
    price_quantity: Decimal = ZERO
    quantity: Decimal = ZERO
    trade_count: int = 0
    aggtrade_count: int = 0
    max_aggtrade_quote: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    last: Optional[Decimal] = None
    last_received_ms: Optional[int] = None
    lag_sum_ms: int = 0
    lag_count: int = 0
    lag_max_ms: Optional[int] = None
    rpi_complete: bool = True

    def observe(
        self,
        *,
        price: Any,
        quantity: Any,
        buyer_is_maker: bool,
        actual_trades: int,
        received_ms: int,
        event_time_ms: Any,
        normal_quantity: Any = None,
    ) -> None:
        parsed_price = _decimal(price, "trade price", positive=True)
        parsed_quantity = _decimal(quantity, "trade quantity", positive=True)
        if not isinstance(buyer_is_maker, bool):
            raise MicrostructureParseError("buyer_is_maker must be boolean")
        parsed_count = _required_ms(actual_trades, "actual trade count", positive=True)
        parsed_received = _required_ms(received_ms, "received_ms")
        parsed_event = _required_ms(event_time_ms, "event_time_ms", positive=True)
        parsed_normal = _optional_decimal(
            normal_quantity, "normal quantity", nonnegative=True
        )
        if parsed_normal is not None and parsed_normal > parsed_quantity:
            raise MicrostructureParseError(
                "normal quantity must not exceed total trade quantity"
            )
        notional = parsed_price * parsed_quantity
        if parsed_normal is None:
            self.rpi_complete = False
            rpi_quantity = ZERO
        else:
            rpi_quantity = parsed_quantity - parsed_normal
        if buyer_is_maker:
            self.sell_quote += notional
            self.rpi_sell_quote += parsed_price * rpi_quantity
        else:
            self.buy_quote += notional
            self.rpi_buy_quote += parsed_price * rpi_quantity
        self.price_quantity += notional
        self.quantity += parsed_quantity
        self.trade_count += parsed_count
        self.aggtrade_count += 1
        self.max_aggtrade_quote = (
            notional
            if self.max_aggtrade_quote is None
            else max(self.max_aggtrade_quote, notional)
        )
        self.high = parsed_price if self.high is None else max(self.high, parsed_price)
        self.low = parsed_price if self.low is None else min(self.low, parsed_price)
        self.last = parsed_price
        self.last_received_ms = parsed_received
        lag = parsed_received - parsed_event
        self.lag_sum_ms += lag
        self.lag_count += 1
        self.lag_max_ms = lag if self.lag_max_ms is None else max(self.lag_max_ms, lag)

    def snapshot_and_reset(self, prefix: str, now_ms: int) -> dict[str, Any]:
        result: dict[str, Any] = {
            f"{prefix}_buy_usdt": self.buy_quote,
            f"{prefix}_sell_usdt": self.sell_quote,
            f"{prefix}_trade_id_span": self.trade_count,
            f"{prefix}_aggtrade_count": self.aggtrade_count,
            f"{prefix}_max_aggtrade_usdt": self.max_aggtrade_quote,
            f"{prefix}_vwap": (
                self.price_quantity / self.quantity if self.quantity > ZERO else None
            ),
            f"{prefix}_trade_high": self.high,
            f"{prefix}_trade_low": self.low,
            f"{prefix}_last_trade": self.last,
            f"{prefix}_trade_age_ms": _age(now_ms, self.last_received_ms),
            f"{prefix}_trade_lag_mean_ms": (
                Decimal(self.lag_sum_ms) / Decimal(self.lag_count)
                if self.lag_count
                else None
            ),
            f"{prefix}_trade_lag_max_ms": self.lag_max_ms,
        }
        if prefix == "fut":
            rpi_known = self.aggtrade_count == 0 or self.rpi_complete
            result["fut_rpi_buy_usdt"] = self.rpi_buy_quote if rpi_known else None
            result["fut_rpi_sell_usdt"] = self.rpi_sell_quote if rpi_known else None
        self.reset_interval()
        return result

    def reset_interval(self) -> None:
        """Discard bucket totals while retaining last-trade age diagnostics."""

        last_received_ms = self.last_received_ms
        self.__dict__.update(TradeAccumulator().__dict__)
        # Age describes the most recently received trade even in an empty bucket.
        self.last_received_ms = last_received_ms


@dataclass
class BookState:
    bids: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    asks: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    last_received_ms: Optional[int] = None
    last_lag_ms: Optional[int] = None
    bbo_ofi_quantity: Decimal = ZERO
    snapshot_count: int = 0
    comparison_ready: bool = False

    def update(self, snapshot: Top10BookSnapshot, *, received_ms: int) -> None:
        if not isinstance(snapshot, Top10BookSnapshot):
            raise TypeError("snapshot must be Top10BookSnapshot")
        parsed_received = _required_ms(received_ms, "received_ms")
        new_bids = list(snapshot.bids[:10])
        new_asks = list(snapshot.asks[:10])
        if not new_bids or not new_asks:
            raise MicrostructureParseError("book snapshot must contain bids and asks")
        if self.comparison_ready and self.bids and self.asks:
            old_bid, old_bid_quantity = self.bids[0]
            old_ask, old_ask_quantity = self.asks[0]
            new_bid, new_bid_quantity = new_bids[0]
            new_ask, new_ask_quantity = new_asks[0]
            if new_bid >= old_bid:
                self.bbo_ofi_quantity += new_bid_quantity
            if new_bid <= old_bid:
                self.bbo_ofi_quantity -= old_bid_quantity
            if new_ask <= old_ask:
                self.bbo_ofi_quantity -= new_ask_quantity
            if new_ask >= old_ask:
                self.bbo_ofi_quantity += old_ask_quantity
        self.bids = new_bids
        self.asks = new_asks
        self.last_received_ms = parsed_received
        self.last_lag_ms = _lag(parsed_received, snapshot.event_time_ms)
        self.snapshot_count += 1
        self.comparison_ready = True

    def invalidate_comparison(self) -> None:
        """Prevent the next BBO from computing OFI across a feed gap."""

        self.comparison_ready = False

    def reset_interval_after_gap(self) -> None:
        """Discard unfinished interval totals but retain quote-age diagnostics."""

        self.bbo_ofi_quantity = ZERO
        self.snapshot_count = 0
        self.invalidate_comparison()

    @staticmethod
    def _imbalance(
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
        levels: int,
    ) -> Optional[Decimal]:
        if len(bids) < levels or len(asks) < levels:
            return None
        bid_quantity = sum((quantity for _, quantity in bids[:levels]), ZERO)
        ask_quantity = sum((quantity for _, quantity in asks[:levels]), ZERO)
        total = bid_quantity + ask_quantity
        return (bid_quantity - ask_quantity) / total if total > ZERO else None

    def snapshot(self, prefix: str, now_ms: int) -> dict[str, Any]:
        if not self.bids or not self.asks:
            result = {
                f"{prefix}_mid": None,
                f"{prefix}_bid": None,
                f"{prefix}_ask": None,
                f"{prefix}_spread_bps": None,
                f"{prefix}_weighted_mid_offset_bps": None,
                f"{prefix}_imbalance_1": None,
                f"{prefix}_imbalance_5": None,
                f"{prefix}_imbalance_10": None,
                f"{prefix}_bid_depth_usdt_10": None,
                f"{prefix}_ask_depth_usdt_10": None,
                f"{prefix}_book_age_ms": None,
                f"{prefix}_book_lag_ms": None,
                f"{prefix}_snapshot_bbo_ofi_usdt": None,
                f"{prefix}_book_snapshot_count": self.snapshot_count,
            }
        else:
            bid, bid_quantity = self.bids[0]
            ask, ask_quantity = self.asks[0]
            mid = (bid + ask) / TWO
            top_quantity = bid_quantity + ask_quantity
            weighted_mid = (
                (ask * bid_quantity + bid * ask_quantity) / top_quantity
                if top_quantity > ZERO
                else mid
            )
            has_ten = len(self.bids) >= 10 and len(self.asks) >= 10
            result = {
                f"{prefix}_mid": mid,
                f"{prefix}_bid": bid,
                f"{prefix}_ask": ask,
                f"{prefix}_spread_bps": (ask - bid) / mid * TEN_THOUSAND,
                f"{prefix}_weighted_mid_offset_bps": (
                    (weighted_mid - mid) / mid * TEN_THOUSAND
                ),
                f"{prefix}_imbalance_1": self._imbalance(self.bids, self.asks, 1),
                f"{prefix}_imbalance_5": self._imbalance(self.bids, self.asks, 5),
                f"{prefix}_imbalance_10": self._imbalance(self.bids, self.asks, 10),
                f"{prefix}_bid_depth_usdt_10": (
                    sum((price * quantity for price, quantity in self.bids[:10]), ZERO)
                    if has_ten
                    else None
                ),
                f"{prefix}_ask_depth_usdt_10": (
                    sum((price * quantity for price, quantity in self.asks[:10]), ZERO)
                    if has_ten
                    else None
                ),
                f"{prefix}_book_age_ms": _age(now_ms, self.last_received_ms),
                f"{prefix}_book_lag_ms": self.last_lag_ms,
                f"{prefix}_snapshot_bbo_ofi_usdt": self.bbo_ofi_quantity * mid,
                f"{prefix}_book_snapshot_count": self.snapshot_count,
            }
        self.bbo_ofi_quantity = ZERO
        self.snapshot_count = 0
        return result


@dataclass
class LiquidationAccumulator:
    long_quote: Decimal = ZERO
    short_quote: Decimal = ZERO
    count: int = 0
    lag_sum_ms: int = 0
    lag_count: int = 0

    def observe(self, fill: LiquidationFill, *, received_ms: int) -> None:
        if not isinstance(fill, LiquidationFill):
            raise TypeError("fill must be LiquidationFill")
        parsed_received = _required_ms(received_ms, "received_ms")
        notional = fill.price * fill.quantity
        if fill.side == "SELL":
            self.long_quote += notional
        elif fill.side == "BUY":
            self.short_quote += notional
        else:  # Defensive: parser/dataclass normally makes this unreachable.
            raise MicrostructureParseError("liquidation side must be BUY or SELL")
        self.count += 1
        self.lag_sum_ms += parsed_received - fill.event_time_ms
        self.lag_count += 1

    def snapshot_and_reset(self) -> dict[str, Any]:
        result = {
            "long_liq_usdt": self.long_quote,
            "short_liq_usdt": self.short_quote,
            "liq_snapshot_count": self.count,
            "liq_lag_mean_ms": (
                Decimal(self.lag_sum_ms) / Decimal(self.lag_count)
                if self.lag_count
                else None
            ),
        }
        self.reset_interval()
        return result

    def reset_interval(self) -> None:
        self.__dict__.update(LiquidationAccumulator().__dict__)


MICROSTRUCTURE_VALUE_COLUMNS = (
    "schema_version",
    "sample_span_ms",
    "sample_jitter_ms",
    "collector_healthy",
    "spot_mid",
    "spot_bid",
    "spot_ask",
    "spot_spread_bps",
    "spot_weighted_mid_offset_bps",
    "spot_imbalance_1",
    "spot_imbalance_5",
    "spot_imbalance_10",
    "spot_bid_depth_usdt_10",
    "spot_ask_depth_usdt_10",
    "spot_book_age_ms",
    "spot_book_lag_ms",
    "spot_snapshot_bbo_ofi_usdt",
    "spot_book_snapshot_count",
    "spot_buy_usdt",
    "spot_sell_usdt",
    "spot_trade_id_span",
    "spot_aggtrade_count",
    "spot_max_aggtrade_usdt",
    "spot_vwap",
    "spot_trade_high",
    "spot_trade_low",
    "spot_last_trade",
    "spot_trade_age_ms",
    "spot_trade_lag_mean_ms",
    "spot_trade_lag_max_ms",
    "fut_mid",
    "fut_bid",
    "fut_ask",
    "fut_spread_bps",
    "fut_weighted_mid_offset_bps",
    "fut_imbalance_1",
    "fut_imbalance_5",
    "fut_imbalance_10",
    "fut_bid_depth_usdt_10",
    "fut_ask_depth_usdt_10",
    "fut_book_age_ms",
    "fut_book_lag_ms",
    "fut_snapshot_bbo_ofi_usdt",
    "fut_book_snapshot_count",
    "fut_buy_usdt",
    "fut_sell_usdt",
    "fut_rpi_buy_usdt",
    "fut_rpi_sell_usdt",
    "fut_trade_id_span",
    "fut_aggtrade_count",
    "fut_max_aggtrade_usdt",
    "fut_vwap",
    "fut_trade_high",
    "fut_trade_low",
    "fut_last_trade",
    "fut_trade_age_ms",
    "fut_trade_lag_mean_ms",
    "fut_trade_lag_max_ms",
    "perp_spot_basis_bps",
    "spot_fut_book_skew_ms",
    "mark_price",
    "index_price",
    "mark_index_basis_bps",
    "funding_rate",
    "seconds_to_funding",
    "mark_age_ms",
    "mark_lag_ms",
    "open_interest_btc",
    "open_interest_usdt",
    "oi_age_ms",
    "oi_exchange_age_ms",
    "oi_http_lag_ms",
    "long_liq_usdt",
    "short_liq_usdt",
    "liq_snapshot_count",
    "liq_lag_mean_ms",
    "connection_errors",
)
MICROSTRUCTURE_COLUMNS = ("sample_second_ms",) + MICROSTRUCTURE_VALUE_COLUMNS


@dataclass
class CollectorState:
    symbol: str = "BTCUSDT"
    spot_book: BookState = field(default_factory=BookState)
    fut_book: BookState = field(default_factory=BookState)
    spot_trades: TradeAccumulator = field(default_factory=TradeAccumulator)
    fut_trades: TradeAccumulator = field(default_factory=TradeAccumulator)
    liquidations: LiquidationAccumulator = field(default_factory=LiquidationAccumulator)
    mark_price: Optional[Decimal] = None
    index_price: Optional[Decimal] = None
    funding_rate: Optional[Decimal] = None
    next_funding_ms: Optional[int] = None
    mark_received_ms: Optional[int] = None
    mark_lag_ms: Optional[int] = None
    open_interest_btc: Optional[Decimal] = None
    oi_received_ms: Optional[int] = None
    oi_exchange_ms: Optional[int] = None
    oi_http_lag_ms: Optional[int] = None
    connection_errors: int = 0
    unhealthy_until_ms: int = 0
    last_sample_second_ms: Optional[int] = None
    connections: dict[str, bool] = field(
        default_factory=lambda: {source: False for source in REQUIRED_CONNECTION_SOURCES}
    )

    def __post_init__(self) -> None:
        self.symbol = self.symbol.upper()

    def connection_opened(self, source: str, received_ms: int) -> None:
        self._validate_connection_source(source)
        parsed_received = _required_ms(received_ms, "received_ms")
        if not self.connections[source]:
            # The interval containing an initial connect or reconnect is only
            # partially observed. Keep a cooldown from the *open* time as well
            # as any earlier close; long backoff must not let the first fresh
            # post-connect frame immediately produce a healthy partial row.
            self.unhealthy_until_ms = max(
                self.unhealthy_until_ms,
                parsed_received + 5_000,
            )
        self._invalidate_book_comparison(source)
        self.connections[source] = True

    def connection_closed(self, source: str, received_ms: int) -> None:
        self._validate_connection_source(source)
        self.connections[source] = False
        self._invalidate_book_comparison(source)
        self.record_gap(received_ms)

    def _invalidate_book_comparison(self, source: str) -> None:
        if source == "spot":
            self.spot_book.invalidate_comparison()
        elif source == "futures_depth":
            self.fut_book.invalidate_comparison()

    def _validate_connection_source(self, source: str) -> None:
        if source not in REQUIRED_CONNECTION_SOURCES:
            raise ValueError(f"unknown microstructure connection source: {source!r}")

    def record_gap(self, received_ms: int) -> None:
        parsed_received = _required_ms(received_ms, "received_ms")
        self.connection_errors += 1
        self.unhealthy_until_ms = max(self.unhealthy_until_ms, parsed_received + 5_000)

    def record_error(self, received_ms: int) -> None:
        self.record_gap(received_ms)

    def reset_after_runtime_gap(self, received_ms: int) -> None:
        """Rebase unfinished interval state after optional-worker supervision.

        Quotes and context remain cached so their ages expose the outage.  Flow,
        liquidation, and OFI totals that cannot be assigned causally after a
        worker restart are discarded, and independently managed sockets must
        prove that they reconnected.
        """

        self.record_gap(received_ms)
        self.spot_trades.reset_interval()
        self.fut_trades.reset_interval()
        self.liquidations.reset_interval()
        self.spot_book.reset_interval_after_gap()
        self.fut_book.reset_interval_after_gap()
        for source in ("spot", "futures_depth", "futures_liquidation"):
            self.connections[source] = False

    def handle_spot_trade(self, trade: Any, *, received_ms: int) -> None:
        observation = _coerce_trade(trade, expected_symbol=self.symbol, futures=False)
        self.spot_trades.observe(
            price=observation.price,
            quantity=observation.quantity,
            buyer_is_maker=observation.buyer_is_maker,
            actual_trades=observation.trade_count,
            received_ms=received_ms,
            event_time_ms=observation.event_time_ms,
        )

    def handle_futures_trade(self, trade: Any, *, received_ms: int) -> None:
        observation = _coerce_trade(trade, expected_symbol=self.symbol, futures=True)
        self.fut_trades.observe(
            price=observation.price,
            quantity=observation.quantity,
            buyer_is_maker=observation.buyer_is_maker,
            actual_trades=observation.trade_count,
            received_ms=received_ms,
            event_time_ms=observation.event_time_ms,
            normal_quantity=observation.normal_quantity,
        )

    def handle_spot_depth(self, snapshot: Any, *, received_ms: int) -> None:
        parsed = (
            snapshot
            if isinstance(snapshot, Top10BookSnapshot)
            else parse_spot_depth_payload(snapshot, expected_symbol=self.symbol)
        )
        if parsed.symbol != self.symbol:
            raise MicrostructureParseError("unexpected spot depth symbol")
        self.spot_book.update(parsed, received_ms=received_ms)

    def handle_futures_depth(self, snapshot: Any, *, received_ms: int) -> None:
        parsed = (
            snapshot
            if isinstance(snapshot, Top10BookSnapshot)
            else parse_futures_depth_payload(snapshot, expected_symbol=self.symbol)
        )
        if parsed.symbol != self.symbol:
            raise MicrostructureParseError("unexpected futures depth symbol")
        self.fut_book.update(parsed, received_ms=received_ms)

    def handle_liquidation(self, fill: Any, *, received_ms: int) -> None:
        parsed = (
            fill
            if isinstance(fill, LiquidationFill)
            else parse_liquidation_payload(fill, expected_symbol=self.symbol)
        )
        if parsed.symbol != self.symbol:
            raise MicrostructureParseError("unexpected liquidation symbol")
        self.liquidations.observe(parsed, received_ms=received_ms)

    def update_context(self, snapshot: Any) -> None:
        snapshot_symbol = getattr(snapshot, "symbol", None)
        if not isinstance(snapshot_symbol, str) or snapshot_symbol.upper() != self.symbol:
            raise MicrostructureParseError(
                f"unexpected context symbol: expected {self.symbol!r}, got {snapshot_symbol!r}"
            )
        received_ms = _required_ms(getattr(snapshot, "received_ms", None), "context received_ms")
        mark_price = _optional_decimal(
            getattr(snapshot, "mark_price", None), "mark price", positive=True
        )
        index_price = _optional_decimal(
            getattr(snapshot, "index_price", None), "index price", positive=True
        )
        self.funding_rate = _optional_decimal(
            getattr(snapshot, "last_funding_rate", None), "funding rate"
        )
        self.next_funding_ms = _optional_ms(
            getattr(snapshot, "next_funding_time_ms", None), "next funding time"
        )
        premium_time_ms = _optional_ms(
            getattr(snapshot, "premium_index_time_ms", None), "premium index time"
        )
        # A partial REST response must not make stale mark/index values look
        # newly observed. Retain the last complete pair so its age exposes the
        # missing context instead.
        if mark_price is not None and index_price is not None:
            self.mark_price = mark_price
            self.index_price = index_price
            self.mark_received_ms = received_ms
            self.mark_lag_ms = _lag(received_ms, premium_time_ms)
        open_interest = _optional_decimal(
            getattr(snapshot, "open_interest", None),
            "open interest",
            nonnegative=True,
        )
        if open_interest is not None:
            self.open_interest_btc = open_interest
            self.oi_received_ms = received_ms
            self.oi_exchange_ms = _optional_ms(
                getattr(snapshot, "open_interest_time_ms", None), "open interest time"
            )
            request_started_ms = _optional_ms(
                getattr(snapshot, "request_started_ms", None), "request started time"
            )
            self.oi_http_lag_ms = (
                None if request_started_ms is None else received_ms - request_started_ms
            )

    def apply_event(self, event: QueuedEvent) -> None:
        received_ms = event.received_ms
        kind = event.kind
        payload = event.payload
        if kind == "spot_trade":
            self.handle_spot_trade(payload, received_ms=received_ms)
        elif kind == "futures_trade":
            self.handle_futures_trade(payload, received_ms=received_ms)
        elif kind == "spot_depth":
            self.handle_spot_depth(payload, received_ms=received_ms)
        elif kind == "futures_depth":
            self.handle_futures_depth(payload, received_ms=received_ms)
        elif kind == "liquidation":
            self.handle_liquidation(payload, received_ms=received_ms)
        elif kind == "context":
            self.update_context(payload)
        elif kind == "connection_opened":
            self.connection_opened(str(payload), received_ms)
        elif kind == "connection_closed":
            self.connection_closed(str(payload), received_ms)
        elif kind == "error":
            self.record_error(received_ms)
        else:
            raise ValueError(f"unknown microstructure event kind: {kind!r}")

    def snapshot(
        self,
        sample_second_ms: int,
        *,
        now_ms: Optional[int] = None,
        sample_jitter_ms: int = 0,
    ) -> dict[str, Any]:
        sample_second = _required_ms(sample_second_ms, "sample_second_ms")
        snapshot_now = sample_second + 1_000 if now_ms is None else _required_ms(now_ms, "now_ms")
        jitter = _required_ms(sample_jitter_ms, "sample_jitter_ms")
        if sample_second % 1_000:
            raise ValueError("sample_second_ms must be aligned to a UTC second")
        row: dict[str, Any] = {
            "sample_second_ms": sample_second,
            "schema_version": SCHEMA_VERSION,
            "sample_span_ms": (
                None
                if self.last_sample_second_ms is None
                else sample_second - self.last_sample_second_ms
            ),
            "sample_jitter_ms": jitter,
            "connection_errors": self.connection_errors,
        }
        self.last_sample_second_ms = sample_second
        row.update(self.spot_book.snapshot("spot", snapshot_now))
        row.update(self.spot_trades.snapshot_and_reset("spot", snapshot_now))
        row.update(self.fut_book.snapshot("fut", snapshot_now))
        row.update(self.fut_trades.snapshot_and_reset("fut", snapshot_now))
        row.update(self.liquidations.snapshot_and_reset())

        spot_mid = row["spot_mid"]
        fut_mid = row["fut_mid"]
        row["perp_spot_basis_bps"] = (
            (fut_mid / spot_mid - ONE) * TEN_THOUSAND
            if spot_mid is not None and fut_mid is not None and spot_mid > ZERO
            else None
        )
        row["spot_fut_book_skew_ms"] = (
            abs(self.spot_book.last_received_ms - self.fut_book.last_received_ms)
            if self.spot_book.last_received_ms is not None
            and self.fut_book.last_received_ms is not None
            else None
        )
        row.update(
            {
                "mark_price": self.mark_price,
                "index_price": self.index_price,
                "mark_index_basis_bps": (
                    (self.mark_price / self.index_price - ONE) * TEN_THOUSAND
                    if self.mark_price is not None
                    and self.index_price is not None
                    and self.index_price > ZERO
                    else None
                ),
                "funding_rate": self.funding_rate,
                "seconds_to_funding": (
                    max(0, (self.next_funding_ms - snapshot_now) // 1_000)
                    if self.next_funding_ms is not None
                    else None
                ),
                "mark_age_ms": _age(snapshot_now, self.mark_received_ms),
                "mark_lag_ms": self.mark_lag_ms,
                "open_interest_btc": self.open_interest_btc,
                "open_interest_usdt": (
                    self.open_interest_btc * fut_mid
                    if self.open_interest_btc is not None and fut_mid is not None
                    else None
                ),
                "oi_age_ms": _age(snapshot_now, self.oi_received_ms),
                "oi_exchange_age_ms": _age(snapshot_now, self.oi_exchange_ms),
                "oi_http_lag_ms": self.oi_http_lag_ms,
            }
        )

        required_ages = (
            row["spot_book_age_ms"],
            row["fut_book_age_ms"],
            row["spot_trade_age_ms"],
            row["fut_trade_age_ms"],
            row["mark_age_ms"],
            row["oi_age_ms"],
        )
        age_ok = all(value is not None for value in required_ages)
        if age_ok:
            age_ok = bool(
                0 <= row["spot_book_age_ms"] <= 3_000
                and 0 <= row["fut_book_age_ms"] <= 2_000
                and 0 <= row["spot_trade_age_ms"] <= 3_000
                and 0 <= row["fut_trade_age_ms"] <= 3_000
                and 0 <= row["mark_age_ms"] <= 4_000
                and 0 <= row["oi_age_ms"] <= 15_000
                and row["oi_exchange_age_ms"] is not None
                and 0 <= row["oi_exchange_age_ms"] <= 15_000
            )
        lag_ok = all(
            value is not None and -1_000 <= value <= 2_000
            for value in (row["fut_book_lag_ms"], row["mark_lag_ms"])
        )
        for optional_trade_lag in (
            row["spot_trade_lag_max_ms"],
            row["fut_trade_lag_max_ms"],
        ):
            if optional_trade_lag is not None:
                lag_ok = lag_ok and -1_000 <= optional_trade_lag <= 2_000
        if row["liq_snapshot_count"]:
            liquidation_lag = row["liq_lag_mean_ms"]
            lag_ok = bool(
                lag_ok
                and liquidation_lag is not None
                and Decimal("-1000") <= liquidation_lag <= Decimal("2000")
            )
        # A new runtime begins inside an already-open UTC second, so its first
        # bucket cannot prove that the whole receipt interval was observed.
        # Retain that partial row for diagnostics but never label it healthy.
        span_ok = (
            row["sample_span_ms"] is not None
            and 800 <= row["sample_span_ms"] <= 1_200
        )
        skew_ok = (
            row["spot_fut_book_skew_ms"] is not None
            and row["spot_fut_book_skew_ms"] <= 1_500
        )
        # The runtime intentionally waits up to 250 ms after the UTC boundary
        # so already-received events reach this queue.  Allow an additional
        # 250 ms of scheduler overhead before declaring the sample unhealthy.
        jitter_ok = 0 <= jitter <= MAX_SAMPLE_JITTER_MS
        gap_ok = snapshot_now > self.unhealthy_until_ms
        connections_ok = all(self.connections.values())
        context_ok = (
            row["mark_price"] is not None
            and row["index_price"] is not None
            and row["mark_index_basis_bps"] is not None
            and row["funding_rate"] is not None
            and row["seconds_to_funding"] is not None
        )
        rpi_ok = (
            row["fut_rpi_buy_usdt"] is not None
            and row["fut_rpi_sell_usdt"] is not None
        )
        depth_ok = all(
            row[name] is not None
            for name in (
                "spot_imbalance_10",
                "spot_bid_depth_usdt_10",
                "spot_ask_depth_usdt_10",
                "fut_imbalance_10",
                "fut_bid_depth_usdt_10",
                "fut_ask_depth_usdt_10",
            )
        )
        row["collector_healthy"] = bool(
            age_ok
            and lag_ok
            and span_ok
            and skew_ok
            and jitter_ok
            and gap_ok
            and connections_ok
            and context_ok
            and rpi_ok
            and depth_ok
        )

        missing = set(MICROSTRUCTURE_COLUMNS).difference(row)
        extra = set(row).difference(MICROSTRUCTURE_COLUMNS)
        if missing or extra:
            raise RuntimeError(
                f"microstructure schema mismatch; missing={sorted(missing)}, extra={sorted(extra)}"
            )
        return {name: row[name] for name in MICROSTRUCTURE_COLUMNS}


class MicrostructureEventSink:
    """Synchronous, nonblocking adapter used by independent async readers."""

    def __init__(
        self,
        queue: "asyncio.Queue[QueuedEvent]",
        *,
        sequence: Optional[Iterator[int]] = None,
        expected_symbol: str = "BTCUSDT",
    ) -> None:
        self.queue = queue
        self._sequence = sequence if sequence is not None else itertools.count()
        self.expected_symbol = expected_symbol.upper()
        self.dropped_events = 0
        self._overflow_error_received_ms: Optional[int] = None

    def _flush_deferred_overflow_error(self) -> bool:
        """Publish one loss marker before any later ordinary event.

        A full ``asyncio.Queue`` cannot accept its own error marker.  Remember
        the first lost event's receive time and insert the marker on the first
        subsequent offer for which capacity is available.
        """

        received_ms = self._overflow_error_received_ms
        if received_ms is None:
            return True
        marker = QueuedEvent(received_ms, next(self._sequence), "error", "queue_overflow")
        try:
            self.queue.put_nowait(marker)
        except asyncio.QueueFull:
            return False
        self._overflow_error_received_ms = None
        return True

    def _offer(self, kind: str, payload: Any, received_ms: int) -> bool:
        parsed_received = _required_ms(received_ms, "received_ms")
        if not self._flush_deferred_overflow_error():
            self.dropped_events += 1
            return False
        event = QueuedEvent(parsed_received, next(self._sequence), kind, payload)
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped_events += 1
            if self._overflow_error_received_ms is None:
                self._overflow_error_received_ms = parsed_received
            return False
        return True

    def offer_futures_trade(self, trade: Any, received_ms: int) -> bool:
        return self._offer("futures_trade", trade, received_ms)

    def offer_context(self, snapshot: Any) -> bool:
        return self._offer("context", snapshot, getattr(snapshot, "received_ms", None))

    def offer_spot_trade(self, trade: Any, received_ms: int) -> bool:
        return self._offer("spot_trade", trade, received_ms)

    def offer_spot_depth(self, snapshot: Any, received_ms: int) -> bool:
        parsed = (
            snapshot
            if isinstance(snapshot, Top10BookSnapshot)
            else parse_spot_depth_payload(snapshot, expected_symbol=self.expected_symbol)
        )
        return self._offer("spot_depth", parsed, received_ms)

    def offer_futures_depth(self, snapshot: Any, received_ms: int) -> bool:
        parsed = (
            snapshot
            if isinstance(snapshot, Top10BookSnapshot)
            else parse_futures_depth_payload(snapshot, expected_symbol=self.expected_symbol)
        )
        return self._offer("futures_depth", parsed, received_ms)

    def offer_liquidation(self, fill: Any, received_ms: int) -> bool:
        parsed = (
            fill
            if isinstance(fill, LiquidationFill)
            else parse_liquidation_payload(fill, expected_symbol=self.expected_symbol)
        )
        return self._offer("liquidation", parsed, received_ms)

    def connection_opened(self, source: str, received_ms: int) -> bool:
        if source not in REQUIRED_CONNECTION_SOURCES:
            raise ValueError(f"unknown microstructure connection source: {source!r}")
        return self._offer("connection_opened", source, received_ms)

    def connection_closed(self, source: str, received_ms: int) -> bool:
        if source not in REQUIRED_CONNECTION_SOURCES:
            raise ValueError(f"unknown microstructure connection source: {source!r}")
        return self._offer("connection_closed", source, received_ms)

    def record_error(
        self,
        source: str,
        received_ms: Optional[int] = None,
        error: Any = None,
    ) -> bool:
        del error  # Error text belongs in logs; the row retains a bounded count.
        if not isinstance(source, str) or not source:
            raise ValueError("error source must be a non-empty string")
        timestamp_ms = time.time_ns() // 1_000_000 if received_ms is None else received_ms
        return self._offer("error", source, timestamp_ms)

    def reset_after_runtime_gap(self) -> None:
        """Forget a deferred overflow marker already covered by state health."""

        self._overflow_error_received_ms = None


def finalize_boundary(
    state: CollectorState,
    pending: list[QueuedEvent],
    boundary_ms: int,
    sample_jitter_ms: int = 0,
) -> dict[str, Any]:
    """Apply causal events for ``[boundary-1000, boundary)`` and snapshot.

    Events exactly on or after the boundary remain in ``pending`` for the next
    row.  Receive time is the causal clock; provider timestamps are diagnostics.
    """

    if not isinstance(state, CollectorState):
        raise TypeError("state must be CollectorState")
    boundary = _required_ms(boundary_ms, "boundary_ms", positive=True)
    if boundary % 1_000:
        raise ValueError("boundary_ms must be aligned to a UTC second")
    lower_bound = boundary - 1_000
    heapq.heapify(pending)
    dropped_stale = False
    while pending and pending[0].received_ms < lower_bound:
        stale_event = heapq.heappop(pending)
        # Connection state is persistent rather than an interval aggregate.
        # Preserve its final ordering so a delayed startup `opened` marker does
        # not leave an otherwise live socket unhealthy until its 24-hour
        # reconnect. All financial observations remain strictly discarded.
        if stale_event.kind in ("connection_opened", "connection_closed"):
            state.apply_event(stale_event)
        dropped_stale = True
    if dropped_stale:
        # Mark the row being finalized, not the stale event's old receive time,
        # so an expired health window cannot hide this causal data loss.
        state.record_gap(boundary)
    while pending and pending[0].received_ms < boundary:
        state.apply_event(heapq.heappop(pending))
    return state.snapshot(
        boundary - 1_000,
        now_ms=boundary,
        sample_jitter_ms=sample_jitter_ms,
    )


__all__ = [
    "AggTradeObservation",
    "BookState",
    "CollectorState",
    "LiquidationAccumulator",
    "LiquidationFill",
    "MAX_SAMPLE_JITTER_MS",
    "MICROSTRUCTURE_COLUMNS",
    "MICROSTRUCTURE_VALUE_COLUMNS",
    "MicrostructureEventSink",
    "MicrostructureParseError",
    "QueuedEvent",
    "REQUIRED_CONNECTION_SOURCES",
    "SCHEMA_VERSION",
    "Top10BookSnapshot",
    "TradeAccumulator",
    "finalize_boundary",
    "parse_futures_depth_payload",
    "parse_futures_force_order",
    "parse_futures_top10_snapshot",
    "parse_liquidation_payload",
    "parse_spot_depth_payload",
    "parse_spot_top10_snapshot",
]
