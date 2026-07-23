import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional

import redis.asyncio as redis
from redis.exceptions import RedisError

from price_collector.binance_microstructure import (
    MICROSTRUCTURE_COLUMNS,
    SCHEMA_VERSION as MICROSTRUCTURE_SCHEMA_VERSION,
)
from price_collector.market import MarketWindow


BINANCE_SPOT_LIVE_KEY = "btc:live:binance_spot"
CHAINLINK_LIVE_KEY = "btc:live:chainlink"
FUTURES_LIVE_KEY = "btc:live:futures"
MICROSTRUCTURE_LIVE_KEY = "btc:live:microstructure"

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
        raise LiveCachePayloadError(
            f"{field_name} must be an integer timestamp"
        ) from exc


def _required_int(value: Any, field_name: str) -> int:
    parsed = _optional_int(value, field_name)
    if parsed is None:
        raise LiveCachePayloadError(f"{field_name} is required")
    return parsed


def _redis_payload_text(raw: Any, field_name: str) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise LiveCachePayloadError(
                f"{field_name} is not valid UTF-8"
            ) from exc
    raise LiveCachePayloadError(f"{field_name} must be a string")


def encode_live_price(
    *,
    value: Decimal,
    source_timestamp_ms: Optional[int],
    received_ms: int,
) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise LiveCachePayloadError(
            "live cache value must be a finite Decimal"
        )
    payload = {
        "value": str(value),
        "source_timestamp_ms": source_timestamp_ms,
        "received_ms": received_ms,
    }
    return json.dumps(payload, separators=(",", ":"))


def decode_live_price(raw: Any) -> Optional[LivePrice]:
    text = _redis_payload_text(raw, "live cache payload")
    if text is None:
        return None

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LiveCachePayloadError("live cache payload is not valid JSON") from exc

    if not isinstance(payload, Mapping):
        raise LiveCachePayloadError("live cache payload must be a JSON object")

    value = payload.get("value")
    if not isinstance(value, str):
        raise LiveCachePayloadError("live cache value must be a string")
    if not value or value.strip() != value:
        raise LiveCachePayloadError(
            "live cache value must be a finite decimal string"
        )
    try:
        parsed_value = Decimal(value)
    except InvalidOperation as exc:
        raise LiveCachePayloadError(
            "live cache value must be a finite decimal string"
        ) from exc
    if not parsed_value.is_finite():
        raise LiveCachePayloadError(
            "live cache value must be a finite decimal string"
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise LiveCachePayloadError(
            "live cache value must contain valid Unicode"
        ) from exc

    return LivePrice(
        value=value,
        source_timestamp_ms=_optional_int(
            payload.get("source_timestamp_ms"),
            "source_timestamp_ms",
        ),
        received_ms=_required_int(payload.get("received_ms"), "received_ms"),
    )


_MICROSTRUCTURE_REQUIRED_INTEGER_FIELDS = frozenset(
    {
        "sample_second_ms",
        "schema_version",
        "sample_jitter_ms",
        "spot_book_snapshot_count",
        "spot_trade_id_span",
        "spot_aggtrade_count",
        "fut_book_snapshot_count",
        "fut_trade_id_span",
        "fut_aggtrade_count",
        "liq_snapshot_count",
        "connection_errors",
        "received_ms",
    }
)
_MICROSTRUCTURE_NULLABLE_INTEGER_FIELDS = frozenset(
    {
        "sample_span_ms",
        "spot_book_age_ms",
        "spot_book_lag_ms",
        "spot_trade_age_ms",
        "spot_trade_lag_max_ms",
        "fut_book_age_ms",
        "fut_book_lag_ms",
        "fut_trade_age_ms",
        "fut_trade_lag_max_ms",
        "spot_fut_book_skew_ms",
        "seconds_to_funding",
        "mark_age_ms",
        "mark_lag_ms",
        "oi_age_ms",
        "oi_exchange_age_ms",
        "oi_http_lag_ms",
    }
)
_MICROSTRUCTURE_BOOLEAN_FIELDS = frozenset({"collector_healthy"})
_MICROSTRUCTURE_REQUIRED_FINANCIAL_FIELDS = frozenset(
    {
        "spot_buy_usdt",
        "spot_sell_usdt",
        "fut_buy_usdt",
        "fut_sell_usdt",
        "long_liq_usdt",
        "short_liq_usdt",
    }
)
_MICROSTRUCTURE_ROW_FIELDS = frozenset(MICROSTRUCTURE_COLUMNS)
_MICROSTRUCTURE_PAYLOAD_FIELDS = frozenset(
    (*MICROSTRUCTURE_COLUMNS, "received_ms")
)
_MICROSTRUCTURE_FINANCIAL_FIELDS = (
    _MICROSTRUCTURE_PAYLOAD_FIELDS
    - _MICROSTRUCTURE_REQUIRED_INTEGER_FIELDS
    - _MICROSTRUCTURE_NULLABLE_INTEGER_FIELDS
    - _MICROSTRUCTURE_BOOLEAN_FIELDS
)
_MICROSTRUCTURE_NULLABLE_FINANCIAL_FIELDS = (
    _MICROSTRUCTURE_FINANCIAL_FIELDS
    - _MICROSTRUCTURE_REQUIRED_FINANCIAL_FIELDS
)

if (
    _MICROSTRUCTURE_REQUIRED_INTEGER_FIELDS
    | _MICROSTRUCTURE_NULLABLE_INTEGER_FIELDS
    | _MICROSTRUCTURE_BOOLEAN_FIELDS
    | _MICROSTRUCTURE_REQUIRED_FINANCIAL_FIELDS
    | _MICROSTRUCTURE_NULLABLE_FINANCIAL_FIELDS
) != _MICROSTRUCTURE_PAYLOAD_FIELDS:
    raise RuntimeError("microstructure live-cache field types are incomplete")


def _strict_json_error(value: str) -> None:
    del value
    raise LiveCachePayloadError(
        "microstructure live cache numeric values must be integers or strings"
    )


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field_name, value in pairs:
        if field_name in result:
            raise LiveCachePayloadError(
                f"microstructure field {field_name!r} is duplicated"
            )
        result[field_name] = value
    return result


def _validate_microstructure_field_names(
    payload: Mapping[str, Any],
    *,
    expected_fields: frozenset[str],
) -> None:
    for field_name in payload:
        if not isinstance(field_name, str) or not field_name:
            raise LiveCachePayloadError(
                "microstructure field names must be non-empty strings"
            )

    actual_fields = frozenset(payload)
    missing = sorted(expected_fields - actual_fields)
    unknown = sorted(actual_fields - expected_fields)
    if missing or unknown:
        raise LiveCachePayloadError(
            "microstructure fields do not match the finalized-row schema; "
            f"missing={missing}, unknown={unknown}"
        )


def _validate_microstructure_integer(
    value: Any,
    field_name: str,
    *,
    nullable: bool,
) -> Optional[int]:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        suffix = " or null" if nullable else ""
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} must be an integer{suffix}"
        )
    return value


def _encode_microstructure_financial(
    value: Any,
    field_name: str,
    *,
    nullable: bool,
) -> Optional[str]:
    if value is None and nullable:
        return None
    if not isinstance(value, Decimal):
        suffix = " or null" if nullable else ""
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} must be a Decimal{suffix}"
        )
    if not value.is_finite():
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} must be finite"
        )
    return str(value)


def _decode_microstructure_financial(
    value: Any,
    field_name: str,
    *,
    nullable: bool,
) -> Optional[str]:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        suffix = " or null" if nullable else ""
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} "
            f"must be a decimal string{suffix}"
        )
    if not value or value.strip() != value:
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} "
            "must contain a decimal string"
        )
    try:
        decimal_value = Decimal(value)
    except InvalidOperation as exc:
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} "
            "must contain a decimal string"
        ) from exc
    if not decimal_value.is_finite():
        raise LiveCachePayloadError(
            f"microstructure field {field_name!r} must be finite"
        )
    return value


def _validate_microstructure_identity(payload: Mapping[str, Any]) -> None:
    if payload["schema_version"] != MICROSTRUCTURE_SCHEMA_VERSION:
        raise LiveCachePayloadError(
            "microstructure schema_version is unsupported"
        )
    if payload["sample_second_ms"] % 1_000:
        raise LiveCachePayloadError(
            "microstructure sample_second_ms must be UTC-second aligned"
        )


def _encode_microstructure_value(value: Any, field_name: str) -> Any:
    if field_name in _MICROSTRUCTURE_REQUIRED_INTEGER_FIELDS:
        return _validate_microstructure_integer(
            value,
            field_name,
            nullable=False,
        )
    if field_name in _MICROSTRUCTURE_NULLABLE_INTEGER_FIELDS:
        return _validate_microstructure_integer(
            value,
            field_name,
            nullable=True,
        )
    if field_name in _MICROSTRUCTURE_BOOLEAN_FIELDS:
        if not isinstance(value, bool):
            raise LiveCachePayloadError(
                f"microstructure field {field_name!r} must be a boolean"
            )
        return value
    if field_name in _MICROSTRUCTURE_REQUIRED_FINANCIAL_FIELDS:
        return _encode_microstructure_financial(
            value,
            field_name,
            nullable=False,
        )
    if field_name in _MICROSTRUCTURE_NULLABLE_FINANCIAL_FIELDS:
        return _encode_microstructure_financial(
            value,
            field_name,
            nullable=True,
        )
    raise LiveCachePayloadError(
        f"unknown microstructure field {field_name!r}"
    )


def _decode_microstructure_value(value: Any, field_name: str) -> Any:
    if field_name in _MICROSTRUCTURE_REQUIRED_INTEGER_FIELDS:
        return _validate_microstructure_integer(
            value,
            field_name,
            nullable=False,
        )
    if field_name in _MICROSTRUCTURE_NULLABLE_INTEGER_FIELDS:
        return _validate_microstructure_integer(
            value,
            field_name,
            nullable=True,
        )
    if field_name in _MICROSTRUCTURE_BOOLEAN_FIELDS:
        if not isinstance(value, bool):
            raise LiveCachePayloadError(
                f"microstructure field {field_name!r} must be a boolean"
            )
        return value
    if field_name in _MICROSTRUCTURE_REQUIRED_FINANCIAL_FIELDS:
        return _decode_microstructure_financial(
            value,
            field_name,
            nullable=False,
        )
    if field_name in _MICROSTRUCTURE_NULLABLE_FINANCIAL_FIELDS:
        return _decode_microstructure_financial(
            value,
            field_name,
            nullable=True,
        )
    raise LiveCachePayloadError(
        f"unknown microstructure field {field_name!r}"
    )


def encode_microstructure_snapshot(
    *,
    row: Mapping[str, Any],
    received_ms: int,
) -> str:
    """Encode one finalized flat row without converting financials to floats."""

    if not isinstance(row, Mapping):
        raise LiveCachePayloadError("microstructure row must be a mapping")
    _validate_microstructure_field_names(
        row,
        expected_fields=_MICROSTRUCTURE_ROW_FIELDS,
    )

    payload = {
        field_name: _encode_microstructure_value(row[field_name], field_name)
        for field_name in MICROSTRUCTURE_COLUMNS
    }
    payload["received_ms"] = _encode_microstructure_value(
        received_ms,
        "received_ms",
    )
    _validate_microstructure_identity(payload)
    return json.dumps(payload, separators=(",", ":"))


def decode_microstructure_snapshot(
    raw: Any,
) -> Optional[dict[str, Any]]:
    """Decode a flat row while rejecting JSON floats and malformed decimals."""

    text = _redis_payload_text(raw, "microstructure live cache payload")
    if text is None:
        return None

    try:
        payload = json.loads(
            text,
            parse_float=_strict_json_error,
            parse_constant=_strict_json_error,
            object_pairs_hook=_strict_json_object,
        )
    except json.JSONDecodeError as exc:
        raise LiveCachePayloadError(
            "microstructure live cache payload is not valid JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise LiveCachePayloadError(
            "microstructure live cache payload must be a JSON object"
        )

    _validate_microstructure_field_names(
        payload,
        expected_fields=_MICROSTRUCTURE_PAYLOAD_FIELDS,
    )
    decoded = {
        field_name: _decode_microstructure_value(
            payload[field_name],
            field_name,
        )
        for field_name in (*MICROSTRUCTURE_COLUMNS, "received_ms")
    }
    _validate_microstructure_identity(decoded)
    return decoded


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
            decode_responses=False,
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

    async def set_microstructure_snapshot(
        self,
        key: str,
        *,
        row: Mapping[str, Any],
        received_ms: int,
    ) -> None:
        await self.redis.set(
            key,
            encode_microstructure_snapshot(
                row=row,
                received_ms=received_ms,
            ),
        )

    async def get_price(self, key: str) -> Optional[LivePrice]:
        return decode_live_price(await self.redis.get(key))

    async def get_microstructure_snapshot(
        self,
        key: str = MICROSTRUCTURE_LIVE_KEY,
    ) -> Optional[dict[str, Any]]:
        return decode_microstructure_snapshot(await self.redis.get(key))

    async def get_prices(
        self,
        keys: Iterable[str],
    ) -> dict[str, Optional[LivePrice]]:
        key_list = list(keys)
        raw_values = await self.redis.mget(key_list)
        if len(raw_values) != len(key_list):
            raise LiveCachePayloadError(
                "live cache MGET returned an unexpected value count"
            )
        return {
            key: decode_live_price(raw_value)
            for key, raw_value in zip(key_list, raw_values)
        }

    async def get_prices_with_microstructure(
        self,
        price_keys: Iterable[str],
        *,
        microstructure_key: str = MICROSTRUCTURE_LIVE_KEY,
    ) -> tuple[dict[str, Optional[LivePrice]], Optional[dict[str, Any]]]:
        """Read source prices and the finalized microstructure row in one MGET."""

        price_key_list = list(price_keys)
        all_keys = [*price_key_list, microstructure_key]
        raw_values = await self.redis.mget(all_keys)
        if len(raw_values) != len(all_keys):
            raise LiveCachePayloadError(
                "live cache MGET returned an unexpected value count"
            )
        prices = {
            key: decode_live_price(raw_value)
            for key, raw_value in zip(price_key_list, raw_values[:-1])
        }
        return prices, decode_microstructure_snapshot(raw_values[-1])

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
