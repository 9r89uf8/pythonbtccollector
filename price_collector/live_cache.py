import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping, Optional

import redis.asyncio as redis
from redis.exceptions import RedisError

from price_collector.market import MarketWindow


BINANCE_SPOT_LIVE_KEY = "btc:live:binance_spot"
CHAINLINK_LIVE_KEY = "btc:live:chainlink"
FUTURES_LIVE_KEY = "btc:live:futures"

LIVE_CACHE_WRITE_ERRORS = (RedisError, OSError, TimeoutError)
LIVE_CACHE_READ_ERRORS = (RedisError, OSError, TimeoutError)


@dataclass(frozen=True)
class LivePrice:
    value: str
    source_timestamp_ms: Optional[int]
    received_ms: int


class LiveCachePayloadError(ValueError):
    pass


def _optional_int(value: Any, field_name: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise LiveCachePayloadError(f"{field_name} must be an integer timestamp")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise LiveCachePayloadError(f"{field_name} must be an integer timestamp") from exc


def _required_int(value: Any, field_name: str) -> int:
    parsed = _optional_int(value, field_name)
    if parsed is None:
        raise LiveCachePayloadError(f"{field_name} is required")
    return parsed


def encode_live_price(
    *,
    value: Decimal,
    source_timestamp_ms: Optional[int],
    received_ms: int,
) -> str:
    return json.dumps(
        {
            "value": str(value),
            "source_timestamp_ms": source_timestamp_ms,
            "received_ms": received_ms,
        },
        separators=(",", ":"),
    )


def decode_live_price(raw: Optional[str]) -> Optional[LivePrice]:
    if raw is None:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LiveCachePayloadError("live cache payload is not valid JSON") from exc

    if not isinstance(payload, Mapping):
        raise LiveCachePayloadError("live cache payload must be a JSON object")

    value = payload.get("value")
    if not isinstance(value, str):
        raise LiveCachePayloadError("live cache value must be a string")

    return LivePrice(
        value=value,
        source_timestamp_ms=_optional_int(
            payload.get("source_timestamp_ms"),
            "source_timestamp_ms",
        ),
        received_ms=_required_int(payload.get("received_ms"), "received_ms"),
    )


def age_ms(server_time_ms: int, timestamp_ms: Optional[int]) -> Optional[int]:
    if timestamp_ms is None:
        return None
    return max(0, server_time_ms - int(timestamp_ms))


def serialize_live_price(
    price: Optional[LivePrice],
    *,
    server_time_ms: int,
    legacy_source_field: Optional[str] = None,
) -> dict[str, Any]:
    if price is None:
        payload = {
            "value": None,
            "source_timestamp_ms": None,
            "received_ms": None,
            "source_age_ms": None,
            "received_age_ms": None,
        }
    else:
        payload = {
            "value": price.value,
            "source_timestamp_ms": price.source_timestamp_ms,
            "received_ms": price.received_ms,
            "source_age_ms": age_ms(server_time_ms, price.source_timestamp_ms),
            "received_age_ms": age_ms(server_time_ms, price.received_ms),
        }

    if legacy_source_field is not None:
        payload[legacy_source_field] = payload["source_timestamp_ms"]

    return payload


class LiveCache:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        socket_timeout: float = 0.25,
        redis_client: Optional[Any] = None,
    ) -> None:
        self.redis = redis_client or redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_connect_timeout=socket_timeout,
            socket_timeout=socket_timeout,
        )

    async def set_price(
        self,
        key: str,
        *,
        value: Decimal,
        source_timestamp_ms: Optional[int],
        received_ms: int,
    ) -> None:
        await self.redis.set(
            key,
            encode_live_price(
                value=value,
                source_timestamp_ms=source_timestamp_ms,
                received_ms=received_ms,
            ),
        )

    async def get_price(self, key: str) -> Optional[LivePrice]:
        return decode_live_price(await self.redis.get(key))

    async def get_prices(self, keys: Iterable[str]) -> dict[str, Optional[LivePrice]]:
        key_list = list(keys)
        raw_values = await self.redis.mget(key_list)
        return {
            key: decode_live_price(raw_value)
            for key, raw_value in zip(key_list, raw_values)
        }

    async def close(self) -> None:
        close = getattr(self.redis, "aclose", None)
        if close is not None:
            await close()
            return

        legacy_close = getattr(self.redis, "close", None)
        if legacy_close is not None:
            result = legacy_close()
            if asyncio.iscoroutine(result):
                await result


def create_live_cache(settings: Any) -> LiveCache:
    return LiveCache(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        socket_timeout=settings.REDIS_SOCKET_TIMEOUT_SECONDS,
    )


async def build_current_live_payload(
    live_cache: LiveCache,
    *,
    window: MarketWindow,
    server_time_ms: int,
) -> dict[str, Any]:
    cached = await live_cache.get_prices(
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            FUTURES_LIVE_KEY,
        ]
    )

    return {
        "server_time_ms": server_time_ms,
        "market_id": window.market_id,
        "market_start_ms": window.market_start_ms,
        "market_end_ms": window.market_end_ms,
        "prices": {
            "binance_spot": serialize_live_price(
                cached.get(BINANCE_SPOT_LIVE_KEY),
                server_time_ms=server_time_ms,
                legacy_source_field="provider_event_ms",
            ),
            "chainlink": serialize_live_price(
                cached.get(CHAINLINK_LIVE_KEY),
                server_time_ms=server_time_ms,
                legacy_source_field="provider_event_ms",
            ),
        },
        "futures": {
            "last": serialize_live_price(
                cached.get(FUTURES_LIVE_KEY),
                server_time_ms=server_time_ms,
                legacy_source_field="time_ms",
            ),
        },
    }
