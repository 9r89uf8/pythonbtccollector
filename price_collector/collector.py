import asyncio
import json
import logging
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

import websockets

from price_collector.config import Settings
from price_collector.db import create_pool, get_instrument_id, upsert_price_sample
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    LIVE_CACHE_WRITE_ERRORS,
    create_live_cache,
)
from price_collector.market import MarketWindow, market_for_sample_second


BINANCE_RECONNECT_SECONDS = 23 * 60 * 60 + 50 * 60
MAX_RECONNECT_BACKOFF_SECONDS = 60.0
LOGGER = logging.getLogger("price_collector.collector")


@dataclass(frozen=True)
class BinanceTicker:
    symbol: str
    price: Decimal
    provider_event_ms: int


@dataclass(frozen=True)
class LatestPrice:
    symbol: str
    price: Decimal
    provider_event_ms: int
    received_ms: int


@dataclass(frozen=True)
class PendingSample:
    symbol: str
    price: Decimal
    provider_event_ms: int
    received_ms: int
    sample_second_ms: int
    window: MarketWindow


class TickerParseError(ValueError):
    pass


class JsonLogFormatter(logging.Formatter):
    _standard_attrs = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)))

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in vars(record).items():
            if key not in self._standard_attrs and key not in payload:
                payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, sort_keys=True, default=str)


class LatestPriceStore:
    def __init__(self) -> None:
        self._latest: Optional[LatestPrice] = None
        self._lock = asyncio.Lock()

    async def update(self, latest: LatestPrice) -> None:
        async with self._lock:
            self._latest = latest

    async def get(self) -> Optional[LatestPrice]:
        async with self._lock:
            return self._latest


def setup_logging(log_level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level.upper())


def current_utc_epoch_ms() -> int:
    return time.time_ns() // 1_000_000


def sample_second_ms_for_now(now_ms: int) -> int:
    return (now_ms // 1000) * 1000


def seconds_until_next_utc_second(now_ms: Optional[int] = None) -> float:
    current_ms = current_utc_epoch_ms() if now_ms is None else now_ms
    next_second_ms = ((current_ms // 1000) + 1) * 1000
    return max((next_second_ms - current_ms) / 1000, 0.0)


def is_latest_price_stale(
    latest: LatestPrice,
    *,
    now_ms: int,
    stale_price_ms: int,
) -> bool:
    return now_ms - latest.received_ms > stale_price_ms


def build_pending_sample(
    *,
    latest: Optional[LatestPrice],
    now_ms: int,
    stale_price_ms: int,
) -> Optional[PendingSample]:
    if latest is None:
        return None

    if is_latest_price_stale(latest, now_ms=now_ms, stale_price_ms=stale_price_ms):
        return None

    sample_second_ms = sample_second_ms_for_now(now_ms)
    window = market_for_sample_second(sample_second_ms)

    return PendingSample(
        symbol=latest.symbol,
        price=latest.price,
        provider_event_ms=latest.provider_event_ms,
        received_ms=latest.received_ms,
        sample_second_ms=sample_second_ms,
        window=window,
    )


def parse_binance_ticker_payload(
    payload: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> BinanceTicker:
    symbol = payload.get("s")
    if symbol != expected_symbol:
        raise TickerParseError(
            f"unexpected Binance ticker symbol: expected {expected_symbol!r}, got {symbol!r}"
        )

    raw_price = payload.get("c")
    if raw_price is None:
        raise TickerParseError("Binance ticker payload is missing last price field 'c'")

    if not isinstance(raw_price, str):
        raise TickerParseError("Binance ticker last price field 'c' must be a string")

    try:
        price = Decimal(raw_price)
    except (InvalidOperation, ValueError) as exc:
        raise TickerParseError("Binance ticker payload has invalid last price field 'c'") from exc

    if not price.is_finite() or price <= 0:
        raise TickerParseError(
            "Binance ticker last price field 'c' must be a finite positive number"
        )

    provider_event_ms = payload.get("E")
    if provider_event_ms is None:
        raise TickerParseError("Binance ticker payload is missing event time field 'E'")

    try:
        provider_event_ms = int(provider_event_ms)
    except (TypeError, ValueError) as exc:
        raise TickerParseError("Binance ticker payload has invalid event time field 'E'") from exc

    if provider_event_ms <= 0:
        raise TickerParseError("Binance ticker event time field 'E' must be positive")

    return BinanceTicker(
        symbol=symbol,
        price=price,
        provider_event_ms=provider_event_ms,
    )


def reconnect_delay_seconds(attempt: int) -> float:
    capped = min(MAX_RECONNECT_BACKOFF_SECONDS, 2 ** max(attempt - 1, 0))
    return random.uniform(0.0, capped)


def require_collector_database_url(settings: Settings) -> str:
    if not settings.DATABASE_URL:
        raise RuntimeError("DATABASE_URL must be set for the collector")
    return settings.DATABASE_URL


async def update_binance_live_cache(live_cache: Any, latest: LatestPrice) -> bool:
    if live_cache is None:
        return False

    try:
        await live_cache.set_price(
            BINANCE_SPOT_LIVE_KEY,
            value=latest.price,
            source_timestamp_ms=latest.provider_event_ms,
            received_ms=latest.received_ms,
        )
    except LIVE_CACHE_WRITE_ERRORS as exc:
        LOGGER.warning(
            "live_cache_write_failed",
            extra={
                "event": "live_cache_write_failed",
                "source": "binance_spot",
                "key": BINANCE_SPOT_LIVE_KEY,
                "error": repr(exc),
            },
        )
        return False

    return True


async def websocket_reader_loop(
    settings: Settings,
    latest_store: LatestPriceStore,
    live_cache: Any = None,
) -> None:
    attempt = 0

    while True:
        try:
            LOGGER.info(
                "websocket_connecting",
                extra={"event": "websocket_connecting", "url": settings.BINANCE_WS_URL},
            )
            async with websockets.connect(
                settings.BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as websocket:
                attempt = 0
                connected_at = time.monotonic()
                LOGGER.info(
                    "websocket_connected",
                    extra={
                        "event": "websocket_connected",
                        "url": settings.BINANCE_WS_URL,
                        "symbol": settings.SYMBOL,
                    },
                )

                while True:
                    connected_seconds = time.monotonic() - connected_at
                    remaining_seconds = BINANCE_RECONNECT_SECONDS - connected_seconds
                    if remaining_seconds <= 0:
                        LOGGER.info(
                            "websocket_proactive_reconnect",
                            extra={
                                "event": "websocket_proactive_reconnect",
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
                        ticker = parse_binance_ticker_payload(
                            payload,
                            expected_symbol=settings.SYMBOL,
                        )
                    except (json.JSONDecodeError, TickerParseError) as exc:
                        LOGGER.warning(
                            "ticker_message_skipped",
                            extra={
                                "event": "ticker_message_skipped",
                                "error": str(exc),
                            },
                        )
                        continue

                    received_ms = current_utc_epoch_ms()
                    latest = LatestPrice(
                        symbol=ticker.symbol,
                        price=ticker.price,
                        provider_event_ms=ticker.provider_event_ms,
                        received_ms=received_ms,
                    )
                    await update_binance_live_cache(live_cache, latest)
                    await latest_store.update(latest)

                    LOGGER.debug(
                        "ticker_received",
                        extra={
                            "event": "ticker_received",
                            "symbol": ticker.symbol,
                            "provider_event_ms": ticker.provider_event_ms,
                            "received_ms": received_ms,
                        },
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "websocket_reconnect_scheduled",
                extra={
                    "event": "websocket_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay)


async def sample_once(
    *,
    pool: Any,
    latest_store: LatestPriceStore,
    instrument_id: int,
    stale_price_ms: int,
    now_ms: Optional[int] = None,
) -> bool:
    current_ms = current_utc_epoch_ms() if now_ms is None else now_ms
    latest = await latest_store.get()
    sample = build_pending_sample(
        latest=latest,
        now_ms=current_ms,
        stale_price_ms=stale_price_ms,
    )

    if sample is None:
        if latest is None:
            LOGGER.debug(
                "sample_skipped_no_price",
                extra={"event": "sample_skipped_no_price", "now_ms": current_ms},
            )
        else:
            LOGGER.debug(
                "sample_skipped_stale_price",
                extra={
                    "event": "sample_skipped_stale_price",
                    "now_ms": current_ms,
                    "received_ms": latest.received_ms,
                    "age_ms": current_ms - latest.received_ms,
                },
            )
        return False

    await upsert_price_sample(
        pool,
        instrument_id=instrument_id,
        sample_second_ms=sample.sample_second_ms,
        window=sample.window,
        price=sample.price,
        provider_event_ms=sample.provider_event_ms,
        received_ms=sample.received_ms,
    )

    LOGGER.info(
        "sample_written",
        extra={
            "event": "sample_written",
            "symbol": sample.symbol,
            "sample_second_ms": sample.sample_second_ms,
            "market_id": sample.window.market_id,
            "provider_event_ms": sample.provider_event_ms,
            "received_ms": sample.received_ms,
        },
    )
    return True


async def sampler_loop(
    *,
    pool: Any,
    latest_store: LatestPriceStore,
    instrument_id: int,
    stale_price_ms: int,
) -> None:
    last_sample_second_ms: Optional[int] = None

    while True:
        await asyncio.sleep(seconds_until_next_utc_second())
        now_ms = current_utc_epoch_ms()
        sample_second_ms = sample_second_ms_for_now(now_ms)
        if sample_second_ms == last_sample_second_ms:
            continue

        last_sample_second_ms = sample_second_ms
        await sample_once(
            pool=pool,
            latest_store=latest_store,
            instrument_id=instrument_id,
            stale_price_ms=stale_price_ms,
            now_ms=now_ms,
        )


async def run_collector(settings: Settings) -> None:
    setup_logging(settings.LOG_LEVEL)
    LOGGER.info(
        "collector_starting",
        extra={
            "event": "collector_starting",
            "app_env": settings.APP_ENV,
            "provider_code": settings.PROVIDER_CODE,
            "symbol": settings.SYMBOL,
            "stale_price_ms": settings.STALE_PRICE_MS,
        },
    )

    pool = await create_pool(require_collector_database_url(settings))
    live_cache = create_live_cache(settings)
    try:
        instrument_id = await get_instrument_id(
            pool,
            provider_code=settings.PROVIDER_CODE,
            symbol=settings.SYMBOL,
        )
        latest_store = LatestPriceStore()
        await asyncio.gather(
            websocket_reader_loop(settings, latest_store, live_cache=live_cache),
            sampler_loop(
                pool=pool,
                latest_store=latest_store,
                instrument_id=instrument_id,
                stale_price_ms=settings.STALE_PRICE_MS,
            ),
        )
    finally:
        await live_cache.close()
        await pool.close()


def main() -> None:
    settings = Settings()
    asyncio.run(run_collector(settings))


if __name__ == "__main__":
    main()
