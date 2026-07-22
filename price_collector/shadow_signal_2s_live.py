"""Strict Redis wire contract for the prospective two-second challenger.

This module deliberately has no dependency on the production shadow worker.  It
owns a separate Redis key and freezes every model identity/configuration field
so an experimental payload cannot be mistaken for the selected production
signal.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, fields
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Mapping, Optional

import redis.asyncio as redis
from redis.exceptions import RedisError


LOGGER = logging.getLogger("price_collector.shadow_signal_2s_live")


SHADOW_SIGNAL_2S_LIVE_KEY = "btc:live:chainlink_shadow_2s"
SHADOW_SIGNAL_2S_SCHEMA_VERSION = 1
SHADOW_SIGNAL_2S_MODE = "shadow_candidate"
SHADOW_SIGNAL_2S_PUBLICATION_ROLE = "challenger"
SHADOW_SIGNAL_2S_EXPERIMENT_VERSION = "prospective_catchup_2s_basis_v2"
SHADOW_SIGNAL_2S_MODEL_VERSION = (
    "catchup_v2_l2000_h2000_b100_basis5m"
)
SHADOW_SIGNAL_2S_LEGACY_MODEL_VERSION = "catchup_v1_l2000_h2000_b100"
SHADOW_SIGNAL_2S_BETA = Decimal("1")
SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS = 2_000
SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS = 2_000

SHADOW_SIGNAL_2S_PAYLOAD_ERROR_LOG_EVERY = 100
SHADOW_SIGNAL_2S_MAX_PAYLOAD_CHARS = 65_536
SHADOW_SIGNAL_2S_MAX_STRING_CHARS = 256
SHADOW_SIGNAL_2S_MAX_DECIMAL_CHARS = 128
SHADOW_SIGNAL_2S_MAX_INVALID_REASONS = 32
SHADOW_SIGNAL_2S_MAX_WIRE_INT = 9_223_372_036_854_775_807

SHADOW_SIGNAL_2S_TRANSPORT_ERRORS = (RedisError, OSError, TimeoutError)

_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_DECIMAL_FIELDS = frozenset(
    {
        "beta",
        "current_chainlink",
        "projected_chainlink",
        "pending_move",
        "pending_move_bps",
        "futures_now",
        "futures_reference",
    }
)


class PayloadError(ValueError):
    """Raised when a two-second challenger payload violates its wire schema."""


@dataclass(frozen=True)
class LiveShadowSignal2s:
    schema_version: int
    mode: str
    publication_role: str
    experiment_version: str
    model_version: str
    beta: Decimal
    futures_lookback_ms: int
    forecast_horizon_ms: int
    generated_ms: int
    target_ms: int
    valid: bool
    status: str
    invalid_reasons: tuple[str, ...]
    state: str

    current_chainlink: Optional[Decimal]
    projected_chainlink: Optional[Decimal]
    pending_move: Optional[Decimal]
    pending_move_bps: Optional[Decimal]
    direction: Optional[str]
    futures_now: Optional[Decimal]
    futures_reference: Optional[Decimal]

    chainlink_now_source_timestamp_ms: Optional[int]
    chainlink_now_received_ms: Optional[int]
    anchor_chainlink_source_timestamp_ms: Optional[int]
    anchor_chainlink_received_ms: Optional[int]
    futures_now_source_timestamp_ms: Optional[int]
    futures_now_received_ms: Optional[int]
    futures_reference_source_timestamp_ms: Optional[int]
    futures_reference_received_ms: Optional[int]
    futures_reference_target_ms: Optional[int]
    futures_reference_gap_ms: Optional[int]

    futures_received_age_ms: Optional[int]
    chainlink_received_age_ms: Optional[int]

    market_id: int
    market_start_ms: int
    market_end_ms: int
    ms_to_market_end: int
    full_horizon_before_market_end: bool

    def __post_init__(self) -> None:
        _validate_signal(self)


_SIGNAL_FIELDS = frozenset(field.name for field in fields(LiveShadowSignal2s))


def _strict_int(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PayloadError(f"{field_name} must be an integer")
    if value < minimum:
        raise PayloadError(f"{field_name} must be at least {minimum}")
    if value > SHADOW_SIGNAL_2S_MAX_WIRE_INT:
        raise PayloadError(f"{field_name} exceeds signed BIGINT")
    return value


def _optional_strict_int(value: Any, field_name: str) -> Optional[int]:
    if value is None:
        return None
    return _strict_int(value, field_name)


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PayloadError(f"{field_name} must be a non-empty string")
    if len(value) > SHADOW_SIGNAL_2S_MAX_STRING_CHARS:
        raise PayloadError(f"{field_name} is too long")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PayloadError(f"{field_name} must contain valid Unicode") from exc
    return value


def _required_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PayloadError(f"{field_name} must be a boolean")
    return value


def _require_decimal(
    value: Any,
    field_name: str,
    *,
    optional: bool = False,
    positive: bool = False,
) -> Optional[Decimal]:
    if value is None and optional:
        return None
    if not isinstance(value, Decimal):
        raise PayloadError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise PayloadError(f"{field_name} must be finite")
    if positive and value <= 0:
        raise PayloadError(f"{field_name} must be positive")
    return value


def _decimal_from_json(
    value: Any,
    field_name: str,
    *,
    optional: bool,
) -> Optional[Decimal]:
    if value is None and optional:
        return None
    if (
        not isinstance(value, str)
        or len(value) > SHADOW_SIGNAL_2S_MAX_DECIMAL_CHARS
        or _DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise PayloadError(f"{field_name} must be a fixed-point decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise PayloadError(f"{field_name} is not a decimal") from exc
    if not parsed.is_finite():
        raise PayloadError(f"{field_name} must be finite")
    return parsed


def _decimal_to_json(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else format(value, "f")


def _validate_current_input(
    *,
    value: Optional[Decimal],
    source_timestamp_ms: Optional[int],
    received_ms: Optional[int],
    age_ms_value: Optional[int],
    generated_ms: int,
    field_name: str,
) -> None:
    if value is None:
        if any(
            item is not None
            for item in (source_timestamp_ms, received_ms, age_ms_value)
        ):
            raise PayloadError(f"{field_name} timestamps and age require a value")
        return

    _require_decimal(value, field_name, positive=True)
    _optional_strict_int(source_timestamp_ms, f"{field_name}_source_timestamp_ms")
    if received_ms is None or age_ms_value is None:
        raise PayloadError(
            f"{field_name} received timestamp and age are required"
        )
    received = _strict_int(received_ms, f"{field_name}_received_ms")
    received_age = _strict_int(age_ms_value, f"{field_name}_received_age_ms")
    if received_age != max(0, generated_ms - received):
        raise PayloadError(f"{field_name} received age is inconsistent")


def project_chainlink_2s(
    *,
    current_chainlink: Decimal,
    futures_now: Decimal,
    futures_reference: Decimal,
) -> Decimal:
    """Apply the unadjusted beta-one two-second catch-up formula."""

    current = _require_decimal(
        current_chainlink,
        "current_chainlink",
        positive=True,
    )
    now = _require_decimal(futures_now, "futures_now", positive=True)
    reference = _require_decimal(
        futures_reference,
        "futures_reference",
        positive=True,
    )
    assert current is not None and now is not None and reference is not None
    with localcontext() as context:
        context.prec = 28
        projected = current * (
            Decimal("1")
            + SHADOW_SIGNAL_2S_BETA * (now / reference - Decimal("1"))
        )
    if not projected.is_finite() or projected <= 0:
        raise PayloadError("projected_chainlink must be finite and positive")
    return projected


def _validate_signal(signal: LiveShadowSignal2s) -> None:
    fixed_values = (
        (
            signal.schema_version,
            SHADOW_SIGNAL_2S_SCHEMA_VERSION,
            "schema_version",
        ),
        (signal.mode, SHADOW_SIGNAL_2S_MODE, "mode"),
        (
            signal.publication_role,
            SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
            "publication_role",
        ),
        (
            signal.experiment_version,
            SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
            "experiment_version",
        ),
        (
            signal.model_version,
            SHADOW_SIGNAL_2S_MODEL_VERSION,
            "model_version",
        ),
        (signal.beta, SHADOW_SIGNAL_2S_BETA, "beta"),
        (
            signal.futures_lookback_ms,
            SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
            "futures_lookback_ms",
        ),
        (
            signal.forecast_horizon_ms,
            SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
            "forecast_horizon_ms",
        ),
    )
    for value, expected, field_name in fixed_values:
        if value != expected or type(value) is not type(expected):
            raise PayloadError(f"{field_name} does not match the frozen experiment")

    _require_decimal(signal.beta, "beta")
    generated_ms = _strict_int(signal.generated_ms, "generated_ms")
    target_ms = _strict_int(signal.target_ms, "target_ms")
    if generated_ms > (
        SHADOW_SIGNAL_2S_MAX_WIRE_INT - SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS
    ):
        raise PayloadError("generated_ms cannot accommodate the forecast horizon")
    if target_ms != generated_ms + SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS:
        raise PayloadError("target_ms is inconsistent")

    valid = _required_bool(signal.valid, "valid")
    status = _required_string(signal.status, "status")
    _required_string(signal.state, "state")
    if not isinstance(signal.invalid_reasons, tuple) or len(
        signal.invalid_reasons
    ) > SHADOW_SIGNAL_2S_MAX_INVALID_REASONS:
        raise PayloadError("invalid_reasons must be a tuple of non-empty strings")
    for reason in signal.invalid_reasons:
        _required_string(reason, "invalid_reasons item")
    if len(set(signal.invalid_reasons)) != len(signal.invalid_reasons):
        raise PayloadError("invalid_reasons must not contain duplicates")

    _validate_current_input(
        value=signal.current_chainlink,
        source_timestamp_ms=signal.chainlink_now_source_timestamp_ms,
        received_ms=signal.chainlink_now_received_ms,
        age_ms_value=signal.chainlink_received_age_ms,
        generated_ms=generated_ms,
        field_name="chainlink_now",
    )
    _validate_current_input(
        value=signal.futures_now,
        source_timestamp_ms=signal.futures_now_source_timestamp_ms,
        received_ms=signal.futures_now_received_ms,
        age_ms_value=signal.futures_received_age_ms,
        generated_ms=generated_ms,
        field_name="futures_now",
    )

    projection_fields = (
        signal.projected_chainlink,
        signal.pending_move,
        signal.pending_move_bps,
        signal.direction,
    )
    required_anchor_fields = (
        signal.futures_reference,
        signal.anchor_chainlink_received_ms,
        signal.futures_reference_received_ms,
        signal.futures_reference_target_ms,
        signal.futures_reference_gap_ms,
    )
    all_anchor_fields = required_anchor_fields + (
        signal.anchor_chainlink_source_timestamp_ms,
        signal.futures_reference_source_timestamp_ms,
    )

    if valid:
        if status != "valid" or signal.invalid_reasons:
            raise PayloadError(
                "valid signal must have valid status and no invalid reasons"
            )
        if any(
            value is None for value in projection_fields + required_anchor_fields
        ):
            raise PayloadError("valid signal requires projection and anchor fields")

        current = _require_decimal(
            signal.current_chainlink,
            "current_chainlink",
            positive=True,
        )
        projected = _require_decimal(
            signal.projected_chainlink,
            "projected_chainlink",
            positive=True,
        )
        pending = _require_decimal(signal.pending_move, "pending_move")
        pending_bps = _require_decimal(
            signal.pending_move_bps,
            "pending_move_bps",
        )
        futures_now = _require_decimal(
            signal.futures_now,
            "futures_now",
            positive=True,
        )
        futures_reference = _require_decimal(
            signal.futures_reference,
            "futures_reference",
            positive=True,
        )
        assert (
            current is not None
            and projected is not None
            and pending is not None
            and pending_bps is not None
            and futures_now is not None
            and futures_reference is not None
        )

        if signal.direction not in {"up", "down", "flat"}:
            raise PayloadError("direction is invalid")
        with localcontext() as context:
            context.prec = 28
            expected_pending = projected - current
            expected_bps = expected_pending / current * Decimal("10000")
        if pending != expected_pending:
            raise PayloadError("pending_move is inconsistent")
        if pending_bps != expected_bps:
            raise PayloadError("pending_move_bps is inconsistent")
        expected_direction = "up" if pending > 0 else "down" if pending < 0 else "flat"
        if signal.direction != expected_direction:
            raise PayloadError("direction is inconsistent")

        anchor_received = _strict_int(
            signal.anchor_chainlink_received_ms,
            "anchor_chainlink_received_ms",
        )
        reference_received = _strict_int(
            signal.futures_reference_received_ms,
            "futures_reference_received_ms",
        )
        reference_target = _strict_int(
            signal.futures_reference_target_ms,
            "futures_reference_target_ms",
        )
        reference_gap = _strict_int(
            signal.futures_reference_gap_ms,
            "futures_reference_gap_ms",
        )
        _optional_strict_int(
            signal.anchor_chainlink_source_timestamp_ms,
            "anchor_chainlink_source_timestamp_ms",
        )
        _optional_strict_int(
            signal.futures_reference_source_timestamp_ms,
            "futures_reference_source_timestamp_ms",
        )
        if signal.chainlink_now_received_ms != anchor_received or (
            signal.chainlink_now_source_timestamp_ms
            != signal.anchor_chainlink_source_timestamp_ms
        ):
            raise PayloadError("current Chainlink input must match the active anchor")
        if (
            reference_target
            != anchor_received - SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS
        ):
            raise PayloadError("futures reference target is inconsistent")
        if reference_received > reference_target or (
            reference_gap != reference_target - reference_received
        ):
            raise PayloadError("futures reference gap is inconsistent")
    else:
        if status == "valid" or not signal.invalid_reasons:
            raise PayloadError(
                "invalid signal requires a non-valid status and invalid reasons"
            )
        if any(
            value is not None for value in projection_fields + all_anchor_fields
        ):
            raise PayloadError(
                "invalid signal projection and anchor fields must be null"
            )

    market_id = _strict_int(signal.market_id, "market_id")
    market_start_ms = _strict_int(signal.market_start_ms, "market_start_ms")
    market_end_ms = _strict_int(signal.market_end_ms, "market_end_ms")
    ms_to_market_end = _strict_int(signal.ms_to_market_end, "ms_to_market_end")
    full_horizon = _required_bool(
        signal.full_horizon_before_market_end,
        "full_horizon_before_market_end",
    )
    if market_end_ms - market_start_ms != 300_000:
        raise PayloadError("market window must be five minutes")
    if market_start_ms % 300_000 != 0:
        raise PayloadError("market_start_ms must align to five minutes")
    if market_id != market_start_ms // 300_000:
        raise PayloadError("market_id is inconsistent")
    if not market_start_ms <= generated_ms < market_end_ms:
        raise PayloadError("generated_ms is outside the market window")
    if ms_to_market_end != market_end_ms - generated_ms:
        raise PayloadError("ms_to_market_end is inconsistent")
    if full_horizon != (target_ms <= market_end_ms):
        raise PayloadError("full_horizon_before_market_end is inconsistent")


def _signal_payload(signal: LiveShadowSignal2s) -> dict[str, Any]:
    if not isinstance(signal, LiveShadowSignal2s):
        raise TypeError("signal must be LiveShadowSignal2s")
    _validate_signal(signal)
    payload = {field.name: getattr(signal, field.name) for field in fields(signal)}
    payload["invalid_reasons"] = list(signal.invalid_reasons)
    for field_name in _DECIMAL_FIELDS:
        payload[field_name] = _decimal_to_json(getattr(signal, field_name))
    return payload


def encode_shadow_signal_2s(signal: LiveShadowSignal2s) -> str:
    return json.dumps(_signal_payload(signal), separators=(",", ":"))


def serialize_shadow_signal_2s(
    signal: Optional[LiveShadowSignal2s],
    *,
    server_time_ms: int,
) -> Optional[dict[str, Any]]:
    if signal is None:
        return None
    now_ms = _strict_int(server_time_ms, "server_time_ms")
    payload = _signal_payload(signal)
    payload["signal_age_ms"] = max(0, now_ms - signal.generated_ms)
    return payload


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise PayloadError("two-second signal contains a duplicate field")
        payload[key] = value
    return payload


def _reject_json_float(raw_value: str) -> None:
    raise PayloadError(f"JSON floating-point value is forbidden: {raw_value}")


def _reject_json_constant(raw_value: str) -> None:
    raise PayloadError(f"non-finite JSON value is forbidden: {raw_value}")


def _bounded_json_int(raw_value: str) -> int:
    digits = raw_value[1:] if raw_value.startswith("-") else raw_value
    if len(digits) > 19:
        raise PayloadError("JSON integer exceeds signed BIGINT")
    parsed = int(raw_value)
    if abs(parsed) > SHADOW_SIGNAL_2S_MAX_WIRE_INT:
        raise PayloadError("JSON integer exceeds signed BIGINT")
    return parsed


def _redis_payload_text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PayloadError("two-second signal is not valid UTF-8") from exc
    raise PayloadError("two-second signal must be a string")


def decode_shadow_signal_2s(raw: Any) -> Optional[LiveShadowSignal2s]:
    text = _redis_payload_text(raw)
    if text is None:
        return None
    if len(text) > SHADOW_SIGNAL_2S_MAX_PAYLOAD_CHARS:
        raise PayloadError("two-second signal payload is too large")
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_int=_bounded_json_int,
            parse_float=_reject_json_float,
            parse_constant=_reject_json_constant,
        )
    except PayloadError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise PayloadError("two-second signal payload is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise PayloadError("two-second signal payload must be a JSON object")
    if set(payload) != _SIGNAL_FIELDS:
        raise PayloadError("two-second signal fields differ from its schema")

    invalid_reasons = payload["invalid_reasons"]
    if not isinstance(invalid_reasons, list):
        raise PayloadError("invalid_reasons must be an array")
    direction = payload["direction"]
    if direction is not None and not isinstance(direction, str):
        raise PayloadError("direction must be a string or null")

    decoded = dict(payload)
    decoded["invalid_reasons"] = tuple(invalid_reasons)
    for field_name in _DECIMAL_FIELDS:
        decoded[field_name] = _decimal_from_json(
            payload[field_name],
            field_name,
            optional=field_name != "beta",
        )
    return LiveShadowSignal2s(**decoded)


class ShadowSignal2sStore:
    """Isolated Redis access for the experimental challenger key."""

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
        self._payload_errors_total = 0

    async def set_signal(
        self,
        signal: LiveShadowSignal2s,
        *,
        ttl_ms: int,
    ) -> None:
        if isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int):
            raise TypeError("ttl_ms must be an integer")
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")
        await self.redis.set(
            SHADOW_SIGNAL_2S_LIVE_KEY,
            encode_shadow_signal_2s(signal),
            px=ttl_ms,
        )

    async def get_signal(self) -> Optional[LiveShadowSignal2s]:
        raw = await self.redis.get(SHADOW_SIGNAL_2S_LIVE_KEY)
        try:
            return decode_shadow_signal_2s(raw)
        except PayloadError:
            self._payload_errors_total += 1
            occurrence = self._payload_errors_total
            if occurrence == 1 or (
                occurrence % SHADOW_SIGNAL_2S_PAYLOAD_ERROR_LOG_EVERY == 0
            ):
                LOGGER.warning(
                    "shadow_signal_2s_live_cache_payload_invalid",
                    extra={
                        "event": "shadow_signal_2s_live_cache_payload_invalid",
                        "redis_key": SHADOW_SIGNAL_2S_LIVE_KEY,
                        "error_category": "decode_or_validation_failed",
                        "occurrence": occurrence,
                    },
                )
            return None

    async def delete_signal(self) -> None:
        await self.redis.delete(SHADOW_SIGNAL_2S_LIVE_KEY)

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


def create_shadow_signal_2s_store(settings: Any) -> ShadowSignal2sStore:
    return ShadowSignal2sStore(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        socket_timeout=settings.REDIS_SOCKET_TIMEOUT_SECONDS,
    )


# Small compatibility surface for callers that prefer concise generic names.
LiveSignal = LiveShadowSignal2s
Store = ShadowSignal2sStore
create_store = create_shadow_signal_2s_store
encode = encode_shadow_signal_2s
decode = decode_shadow_signal_2s
serialize = serialize_shadow_signal_2s
TRANSPORT_ERRORS = SHADOW_SIGNAL_2S_TRANSPORT_ERRORS
