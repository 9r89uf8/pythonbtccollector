import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

import websockets

from price_collector.collector import (
    current_utc_epoch_ms,
    reconnect_delay_seconds,
    require_collector_database_url,
    setup_logging,
)
from price_collector.config import Settings
from price_collector.db import create_pool, get_instrument_id, upsert_price_sample
from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    LIVE_CACHE_WRITE_ERRORS,
    create_live_cache,
)
from price_collector.market import MarketWindow, market_for_sample_second


LOGGER = logging.getLogger("price_collector.polymarket_chainlink_collector")
RTDS_PING_SECONDS = 5.0


class RtdsParseError(ValueError):
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

    return value


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

    provider_event_ms = _parse_positive_int_ms(
        payload.get("timestamp"),
        "RTDS payload.timestamp",
    )

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
) -> bool:
    if live_cache is None:
        return False

    try:
        await live_cache.set_price(
            CHAINLINK_LIVE_KEY,
            value=sample.price,
            source_timestamp_ms=sample.provider_event_ms,
            received_ms=sample.received_ms,
        )
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


async def rtds_ping_loop(websocket: Any) -> None:
    while True:
        await asyncio.sleep(RTDS_PING_SECONDS)
        await websocket.send("PING")


async def polymarket_chainlink_reader_loop(
    settings: Settings,
    pool: Any,
    instrument_id: int,
    live_cache: Any = None,
) -> None:
    attempt = 0

    while True:
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
                attempt = 0
                subscription = build_polymarket_chainlink_subscription(settings)
                await websocket.send(json.dumps(subscription))
                LOGGER.info(
                    "polymarket_rtds_subscribed",
                    extra={
                        "event": "polymarket_rtds_subscribed",
                        "topic": settings.POLYMARKET_CHAINLINK_TOPIC,
                        "rtd_symbol": settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
                    },
                )

                ping_task = asyncio.create_task(rtds_ping_loop(websocket))
                try:
                    async for raw_message in websocket:
                        if raw_message in ("PONG", "PING", b"PONG", b"PING"):
                            continue

                        try:
                            message = json.loads(raw_message, parse_float=Decimal)
                            tick = parse_polymarket_chainlink_message(
                                message,
                                expected_symbol=settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
                                db_symbol=settings.POLYMARKET_CHAINLINK_SYMBOL,
                                expected_topic=settings.POLYMARKET_CHAINLINK_TOPIC,
                            )
                        except (json.JSONDecodeError, RtdsParseError, TypeError) as exc:
                            LOGGER.warning(
                                "polymarket_rtds_message_skipped",
                                extra={
                                    "event": "polymarket_rtds_message_skipped",
                                    "error": str(exc),
                                },
                            )
                            continue

                        await handle_tick(
                            pool,
                            instrument_id,
                            tick,
                            source_topic=settings.POLYMARKET_CHAINLINK_TOPIC,
                            live_cache=live_cache,
                        )
                finally:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping_task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "polymarket_rtds_reconnect_scheduled",
                extra={
                    "event": "polymarket_rtds_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay)


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
        },
    )

    pool = await create_pool(require_collector_database_url(settings))
    live_cache = create_live_cache(settings)
    try:
        instrument_id = await get_instrument_id(
            pool,
            provider_code=settings.POLYMARKET_CHAINLINK_PROVIDER_CODE,
            symbol=settings.POLYMARKET_CHAINLINK_SYMBOL,
        )
        await polymarket_chainlink_reader_loop(
            settings,
            pool,
            instrument_id,
            live_cache=live_cache,
        )
    finally:
        await live_cache.close()
        await pool.close()


def main() -> None:
    settings = Settings()
    asyncio.run(run_collector(settings))


if __name__ == "__main__":
    main()
