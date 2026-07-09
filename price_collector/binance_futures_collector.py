import asyncio
import json
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

import httpx

from price_collector.binance_futures_streams import (
    AsyncBookTickerAggregator,
    AsyncFlowAggregator,
    futures_agg_trade_reader_loop,
    futures_book_flush_loop,
    futures_book_ticker_reader_loop,
    futures_flow_flush_loop,
)
from price_collector.collector import (
    current_utc_epoch_ms,
    require_collector_database_url,
    seconds_until_next_utc_second,
    setup_logging,
)
from price_collector.config import Settings
from price_collector.db import (
    create_pool,
    upsert_binance_futures_oi_5m_summary,
    upsert_binance_futures_snapshot,
)
from price_collector.live_cache import (
    FUTURES_LIVE_KEY,
    LIVE_CACHE_WRITE_ERRORS,
    create_live_cache,
)
from price_collector.market import MARKET_MS, MarketWindow, market_for_sample_second


LOGGER = logging.getLogger("price_collector.binance_futures_collector")


class FuturesParseError(ValueError):
    pass


@dataclass(frozen=True)
class BinanceFuturesSnapshot:
    symbol: str
    window: MarketWindow
    sample_second_ms: int
    futures_last_price: Optional[Decimal]
    futures_last_price_time_ms: Optional[int]
    mark_price: Optional[Decimal]
    index_price: Optional[Decimal]
    last_funding_rate: Optional[Decimal]
    next_funding_time_ms: Optional[int]
    premium_index_time_ms: Optional[int]
    open_interest: Optional[Decimal]
    open_interest_time_ms: Optional[int]
    oi_notional_usdt: Optional[Decimal]
    premium_bps: Optional[Decimal]
    received_ms: int
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class BinanceFuturesOi5mSummary:
    symbol: str
    source_window_start_ms: int
    source_window_end_ms: int
    effective_window: MarketWindow
    binance_timestamp_ms: int
    sum_open_interest: Optional[Decimal]
    sum_open_interest_value: Optional[Decimal]
    received_ms: int
    raw: Mapping[str, Any]


def decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise FuturesParseError("numeric value must not be boolean")

    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise FuturesParseError(f"invalid decimal value: {value!r}") from exc

    if not parsed.is_finite():
        return None

    return parsed


def int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        return int(value)

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _require_mapping(value: Any, endpoint_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FuturesParseError(f"{endpoint_name} response must be an object")
    return value


def _validate_symbol(payload: Mapping[str, Any], *, expected_symbol: str, endpoint_name: str) -> None:
    symbol = payload.get("symbol")
    if symbol is not None and symbol != expected_symbol:
        raise FuturesParseError(
            f"unexpected {endpoint_name} symbol: expected {expected_symbol!r}, got {symbol!r}"
        )


def _sample_second_ms_for_source_time(source_time_ms: Optional[int], received_ms: int) -> int:
    timestamp_ms = source_time_ms if source_time_ms is not None and source_time_ms > 0 else received_ms
    return (timestamp_ms // 1000) * 1000


def build_binance_futures_snapshot(
    *,
    symbol: str,
    open_interest_payload: Mapping[str, Any],
    premium_index_payload: Mapping[str, Any],
    ticker_payload: Mapping[str, Any],
    received_ms: int,
) -> BinanceFuturesSnapshot:
    _validate_symbol(
        open_interest_payload,
        expected_symbol=symbol,
        endpoint_name="open interest",
    )
    _validate_symbol(
        premium_index_payload,
        expected_symbol=symbol,
        endpoint_name="premium index",
    )
    _validate_symbol(
        ticker_payload,
        expected_symbol=symbol,
        endpoint_name="ticker",
    )

    open_interest = decimal_or_none(open_interest_payload.get("openInterest"))
    if open_interest is not None and open_interest < 0:
        raise FuturesParseError("openInterest must be non-negative")

    open_interest_time_ms = int_or_none(open_interest_payload.get("time"))
    mark_price = decimal_or_none(premium_index_payload.get("markPrice"))
    index_price = decimal_or_none(premium_index_payload.get("indexPrice"))
    last_funding_rate = decimal_or_none(premium_index_payload.get("lastFundingRate"))
    next_funding_time_ms = int_or_none(premium_index_payload.get("nextFundingTime"))
    premium_index_time_ms = int_or_none(premium_index_payload.get("time"))
    futures_last_price = decimal_or_none(ticker_payload.get("price"))
    futures_last_price_time_ms = int_or_none(ticker_payload.get("time"))

    futures_row_time_ms = (
        futures_last_price_time_ms
        or premium_index_time_ms
        or received_ms
    )
    sample_second_ms = _sample_second_ms_for_source_time(
        futures_row_time_ms,
        received_ms,
    )
    window = market_for_sample_second(sample_second_ms)

    oi_notional_usdt = None
    if open_interest is not None and mark_price is not None:
        oi_notional_usdt = open_interest * mark_price

    premium_bps = None
    if mark_price is not None and index_price is not None and index_price > 0:
        premium_bps = (mark_price / index_price - Decimal("1")) * Decimal("10000")

    return BinanceFuturesSnapshot(
        symbol=symbol,
        window=window,
        sample_second_ms=sample_second_ms,
        futures_last_price=futures_last_price,
        futures_last_price_time_ms=futures_last_price_time_ms,
        mark_price=mark_price,
        index_price=index_price,
        last_funding_rate=last_funding_rate,
        next_funding_time_ms=next_funding_time_ms,
        premium_index_time_ms=premium_index_time_ms,
        open_interest=open_interest,
        open_interest_time_ms=open_interest_time_ms,
        oi_notional_usdt=oi_notional_usdt,
        premium_bps=premium_bps,
        received_ms=received_ms,
        raw={
            "openInterest": dict(open_interest_payload),
            "premiumIndex": dict(premium_index_payload),
            "ticker": dict(ticker_payload),
        },
    )


def source_window_from_hist_oi_timestamp(timestamp_ms: int) -> tuple[int, int, str]:
    if timestamp_ms <= 0:
        raise FuturesParseError("historical OI timestamp must be positive")

    if timestamp_ms % MARKET_MS == 0:
        return (
            timestamp_ms - MARKET_MS,
            timestamp_ms,
            "aligned_timestamp_treated_as_source_window_end_ms",
        )

    source_window_start_ms = (timestamp_ms // MARKET_MS) * MARKET_MS
    return (
        source_window_start_ms,
        source_window_start_ms + MARKET_MS,
        "non_aligned_timestamp_treated_as_inside_source_window",
    )


def build_binance_futures_oi_5m_summary(
    *,
    symbol: str,
    payload: Mapping[str, Any],
    received_ms: int,
    now_ms: int,
) -> Optional[BinanceFuturesOi5mSummary]:
    _validate_symbol(payload, expected_symbol=symbol, endpoint_name="historical OI")

    binance_timestamp_ms = int_or_none(payload.get("timestamp"))
    if binance_timestamp_ms is None:
        raise FuturesParseError("historical OI payload missing timestamp")

    (
        source_window_start_ms,
        source_window_end_ms,
        timestamp_interpretation,
    ) = source_window_from_hist_oi_timestamp(binance_timestamp_ms)

    latest_completed_end_ms = (now_ms // MARKET_MS) * MARKET_MS
    if source_window_end_ms > latest_completed_end_ms:
        return None

    effective_window = market_for_sample_second(source_window_end_ms)
    raw = dict(payload)
    raw["timestamp_interpretation"] = timestamp_interpretation

    return BinanceFuturesOi5mSummary(
        symbol=symbol,
        source_window_start_ms=source_window_start_ms,
        source_window_end_ms=source_window_end_ms,
        effective_window=effective_window,
        binance_timestamp_ms=binance_timestamp_ms,
        sum_open_interest=decimal_or_none(payload.get("sumOpenInterest")),
        sum_open_interest_value=decimal_or_none(payload.get("sumOpenInterestValue")),
        received_ms=received_ms,
        raw=raw,
    )


async def get_json(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    params: Mapping[str, Any],
) -> Any:
    response = await client.get(f"{base_url.rstrip('/')}{path}", params=params)
    response.raise_for_status()
    return json.loads(response.text, parse_float=Decimal)


async def update_futures_live_cache(
    live_cache: Any,
    snapshot: BinanceFuturesSnapshot,
) -> bool:
    if live_cache is None or snapshot.futures_last_price is None:
        return False

    try:
        await live_cache.set_price(
            FUTURES_LIVE_KEY,
            value=snapshot.futures_last_price,
            source_timestamp_ms=snapshot.futures_last_price_time_ms,
            received_ms=snapshot.received_ms,
        )
    except LIVE_CACHE_WRITE_ERRORS as exc:
        LOGGER.warning(
            "live_cache_write_failed",
            extra={
                "event": "live_cache_write_failed",
                "source": "futures",
                "key": FUTURES_LIVE_KEY,
                "error": repr(exc),
            },
        )
        return False

    return True


async def collect_once(
    *,
    pool: Any,
    client: httpx.AsyncClient,
    settings: Settings,
    live_cache: Any = None,
) -> BinanceFuturesSnapshot:
    received_ms = current_utc_epoch_ms()
    symbol = settings.BINANCE_FUTURES_SYMBOL
    base_url = settings.BINANCE_FUTURES_BASE_URL

    open_interest_data, premium_index_data, ticker_data = await asyncio.gather(
        get_json(
            client,
            base_url,
            "/fapi/v1/openInterest",
            {"symbol": symbol},
        ),
        get_json(
            client,
            base_url,
            "/fapi/v1/premiumIndex",
            {"symbol": symbol},
        ),
        get_json(
            client,
            base_url,
            "/fapi/v2/ticker/price",
            {"symbol": symbol},
        ),
    )

    snapshot = build_binance_futures_snapshot(
        symbol=symbol,
        open_interest_payload=_require_mapping(open_interest_data, "open interest"),
        premium_index_payload=_require_mapping(premium_index_data, "premium index"),
        ticker_payload=_require_mapping(ticker_data, "ticker"),
        received_ms=received_ms,
    )

    await update_futures_live_cache(live_cache, snapshot)

    await upsert_binance_futures_snapshot(
        pool,
        symbol=snapshot.symbol,
        window=snapshot.window,
        sample_second_ms=snapshot.sample_second_ms,
        futures_last_price=snapshot.futures_last_price,
        futures_last_price_time_ms=snapshot.futures_last_price_time_ms,
        mark_price=snapshot.mark_price,
        index_price=snapshot.index_price,
        last_funding_rate=snapshot.last_funding_rate,
        next_funding_time_ms=snapshot.next_funding_time_ms,
        premium_index_time_ms=snapshot.premium_index_time_ms,
        open_interest=snapshot.open_interest,
        open_interest_time_ms=snapshot.open_interest_time_ms,
        oi_notional_usdt=snapshot.oi_notional_usdt,
        premium_bps=snapshot.premium_bps,
        received_ms=snapshot.received_ms,
        raw=(
            snapshot.raw
            if getattr(settings, "BINANCE_FUTURES_STORE_RAW_JSON", False)
            else None
        ),
    )

    LOGGER.info(
        "binance_futures_snapshot_written",
        extra={
            "event": "binance_futures_snapshot_written",
            "symbol": snapshot.symbol,
            "sample_second_ms": snapshot.sample_second_ms,
            "market_id": snapshot.window.market_id,
            "open_interest_time_ms": snapshot.open_interest_time_ms,
            "received_ms": snapshot.received_ms,
        },
    )
    return snapshot


async def collect_historical_oi_once(
    *,
    pool: Any,
    client: httpx.AsyncClient,
    settings: Settings,
) -> int:
    received_ms = current_utc_epoch_ms()
    symbol = settings.BINANCE_FUTURES_SYMBOL
    data = await get_json(
        client,
        settings.BINANCE_FUTURES_BASE_URL,
        "/futures/data/openInterestHist",
        {"symbol": symbol, "period": "5m", "limit": 2},
    )

    if not isinstance(data, list):
        raise FuturesParseError("historical OI response must be an array")

    stored = 0
    for item in data:
        if not isinstance(item, Mapping):
            LOGGER.warning(
                "binance_futures_historical_oi_item_skipped",
                extra={
                    "event": "binance_futures_historical_oi_item_skipped",
                    "error": "item is not an object",
                },
            )
            continue

        try:
            summary = build_binance_futures_oi_5m_summary(
                symbol=symbol,
                payload=item,
                received_ms=received_ms,
                now_ms=received_ms,
            )
        except FuturesParseError as exc:
            LOGGER.warning(
                "binance_futures_historical_oi_item_skipped",
                extra={
                    "event": "binance_futures_historical_oi_item_skipped",
                    "error": str(exc),
                },
            )
            continue

        if summary is None:
            continue

        await upsert_binance_futures_oi_5m_summary(
            pool,
            symbol=summary.symbol,
            source_window_start_ms=summary.source_window_start_ms,
            source_window_end_ms=summary.source_window_end_ms,
            effective_window=summary.effective_window,
            binance_timestamp_ms=summary.binance_timestamp_ms,
            sum_open_interest=summary.sum_open_interest,
            sum_open_interest_value=summary.sum_open_interest_value,
            received_ms=summary.received_ms,
            raw=(
                summary.raw
                if getattr(settings, "BINANCE_FUTURES_STORE_RAW_JSON", False)
                else None
            ),
        )
        stored += 1

    if stored:
        LOGGER.info(
            "binance_futures_historical_oi_summaries_written",
            extra={
                "event": "binance_futures_historical_oi_summaries_written",
                "symbol": symbol,
                "count": stored,
                "received_ms": received_ms,
            },
        )

    return stored


async def snapshot_loop(
    *,
    pool: Any,
    client: httpx.AsyncClient,
    settings: Settings,
    live_cache: Any = None,
) -> None:
    while True:
        if settings.BINANCE_FUTURES_POLL_SECONDS <= 1:
            await asyncio.sleep(seconds_until_next_utc_second())
        else:
            await asyncio.sleep(settings.BINANCE_FUTURES_POLL_SECONDS)

        try:
            await collect_once(
                pool=pool,
                client=client,
                settings=settings,
                live_cache=live_cache,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "binance_futures_snapshot_failed",
                extra={"event": "binance_futures_snapshot_failed"},
            )


async def historical_oi_loop(
    *,
    pool: Any,
    client: httpx.AsyncClient,
    settings: Settings,
) -> None:
    while True:
        try:
            await collect_historical_oi_once(pool=pool, client=client, settings=settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "binance_futures_historical_oi_failed",
                extra={"event": "binance_futures_historical_oi_failed"},
            )

        await asyncio.sleep(settings.BINANCE_FUTURES_HIST_OI_POLL_SECONDS)


async def run_collector(settings: Settings) -> None:
    setup_logging(settings.LOG_LEVEL)
    LOGGER.info(
        "binance_futures_collector_starting",
        extra={
            "event": "binance_futures_collector_starting",
            "app_env": settings.APP_ENV,
            "base_url": settings.BINANCE_FUTURES_BASE_URL,
            "symbol": settings.BINANCE_FUTURES_SYMBOL,
            "historical_oi_enabled": settings.BINANCE_FUTURES_HIST_OI_ENABLED,
            "streams_enabled": settings.BINANCE_FUTURES_STREAMS_ENABLED,
            "store_raw_json": settings.BINANCE_FUTURES_STORE_RAW_JSON,
        },
    )

    pool = await create_pool(require_collector_database_url(settings))
    live_cache = create_live_cache(settings)
    try:
        timeout = httpx.Timeout(settings.BINANCE_FUTURES_REST_TIMEOUT_SECONDS)
        async with httpx.AsyncClient(timeout=timeout) as client:
            tasks = [
                asyncio.create_task(
                    snapshot_loop(
                        pool=pool,
                        client=client,
                        settings=settings,
                        live_cache=live_cache,
                    )
                )
            ]
            if settings.BINANCE_FUTURES_HIST_OI_ENABLED:
                tasks.append(
                    asyncio.create_task(
                        historical_oi_loop(pool=pool, client=client, settings=settings)
                    )
                )

            if settings.BINANCE_FUTURES_STREAMS_ENABLED:
                flow_store = AsyncFlowAggregator(
                    venue=settings.BINANCE_FUTURES_PROVIDER_CODE,
                    symbol=settings.BINANCE_FUTURES_SYMBOL,
                )
                book_store = AsyncBookTickerAggregator(
                    venue=settings.BINANCE_FUTURES_PROVIDER_CODE,
                    symbol=settings.BINANCE_FUTURES_SYMBOL,
                )
                tasks.extend(
                    [
                        asyncio.create_task(
                            futures_agg_trade_reader_loop(settings, flow_store)
                        ),
                        asyncio.create_task(
                            futures_flow_flush_loop(
                                pool=pool,
                                settings=settings,
                                flow_store=flow_store,
                            )
                        ),
                        asyncio.create_task(
                            futures_book_ticker_reader_loop(settings, book_store)
                        ),
                        asyncio.create_task(
                            futures_book_flush_loop(
                                pool=pool,
                                settings=settings,
                                book_store=book_store,
                            )
                        ),
                    ]
                )

            await asyncio.gather(*tasks)
    finally:
        await live_cache.close()
        await pool.close()


def main() -> None:
    settings = Settings()
    asyncio.run(run_collector(settings))


if __name__ == "__main__":
    main()
