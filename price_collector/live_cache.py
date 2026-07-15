import asyncio
import json
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Iterable, Mapping, Optional
from uuid import UUID

import redis.asyncio as redis
from redis.exceptions import RedisError

from price_collector.market import MarketWindow


LOGGER = logging.getLogger("price_collector.live_cache")


BINANCE_SPOT_LIVE_KEY = "btc:live:binance_spot"
CHAINLINK_LIVE_KEY = "btc:live:chainlink"
FUTURES_LIVE_KEY = "btc:live:futures"
CHAINLINK_SHADOW_LIVE_KEY = "btc:live:chainlink_shadow"

SHADOW_SIGNAL_SCHEMA_VERSION = 1
SHADOW_SIGNAL_MODE = "shadow"
SHADOW_PAYLOAD_ERROR_LOG_EVERY = 100
SHADOW_SIGNAL_MAX_PAYLOAD_CHARS = 65_536
SHADOW_SIGNAL_MAX_STRING_CHARS = 256
SHADOW_SIGNAL_MAX_DECIMAL_CHARS = 128
SHADOW_SIGNAL_MAX_INVALID_REASONS = 32
SHADOW_SIGNAL_MAX_WIRE_INT = 9_223_372_036_854_775_807

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")

LIVE_CACHE_WRITE_ERRORS = (RedisError, OSError, TimeoutError)
LIVE_CACHE_READ_ERRORS = (RedisError, OSError, TimeoutError)


@dataclass(frozen=True)
class LivePrice:
    value: str
    source_timestamp_ms: Optional[int]
    received_ms: int
    publisher_epoch: Optional[str] = None
    accepted_event_sequence: Optional[int] = None


@dataclass(frozen=True)
class LiveShadowSignal:
    schema_version: int
    mode: str

    selection_schema_version: int
    selection_policy_version: str
    selection_fingerprint_sha256: str
    selection_artifact_sha256: str
    selection_evidence_end_ms: int

    model_version: str
    beta: Decimal
    generated_ms: int
    valid: bool
    status: str
    invalid_reasons: tuple[str, ...]
    state: str
    horizon_ms: int
    estimated_lag_ms: int

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
        _validate_shadow_signal(self)


class LiveCachePayloadError(ValueError):
    pass


_SHADOW_SIGNAL_FIELDS = frozenset(LiveShadowSignal.__dataclass_fields__)


def _strict_int(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiveCachePayloadError(f"{field_name} must be an integer")
    if value < minimum:
        raise LiveCachePayloadError(f"{field_name} must be at least {minimum}")
    if value > SHADOW_SIGNAL_MAX_WIRE_INT:
        raise LiveCachePayloadError(f"{field_name} exceeds signed BIGINT")
    return value


def _optional_strict_int(value: Any, field_name: str) -> Optional[int]:
    if value is None:
        return None
    return _strict_int(value, field_name)


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LiveCachePayloadError(f"{field_name} must be a non-empty string")
    if len(value) > SHADOW_SIGNAL_MAX_STRING_CHARS:
        raise LiveCachePayloadError(f"{field_name} is too long")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise LiveCachePayloadError(
            f"{field_name} must contain valid Unicode"
        ) from exc
    return value


def _required_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise LiveCachePayloadError(f"{field_name} must be a boolean")
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
        or len(value) > SHADOW_SIGNAL_MAX_DECIMAL_CHARS
        or _DECIMAL_PATTERN.fullmatch(value) is None
    ):
        raise LiveCachePayloadError(
            f"{field_name} must be a fixed-point decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise LiveCachePayloadError(f"{field_name} is not a decimal") from exc
    if not parsed.is_finite():
        raise LiveCachePayloadError(f"{field_name} must be finite")
    return parsed


def _decimal_to_json(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else format(value, "f")


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
        raise LiveCachePayloadError(f"{field_name} must be Decimal")
    if not value.is_finite():
        raise LiveCachePayloadError(f"{field_name} must be finite")
    if positive and value <= 0:
        raise LiveCachePayloadError(f"{field_name} must be positive")
    return value


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
            raise LiveCachePayloadError(
                f"{field_name} timestamps and age require a value"
            )
        return
    _require_decimal(value, field_name, positive=True)
    _optional_strict_int(source_timestamp_ms, f"{field_name}_source_timestamp_ms")
    if received_ms is None or age_ms_value is None:
        raise LiveCachePayloadError(
            f"{field_name} received timestamp and age are required"
        )
    received = _strict_int(received_ms, f"{field_name}_received_ms")
    age = _strict_int(age_ms_value, f"{field_name}_received_age_ms")
    if age != max(0, generated_ms - received):
        raise LiveCachePayloadError(f"{field_name} received age is inconsistent")


def _validate_shadow_signal(signal: LiveShadowSignal) -> None:
    if signal.schema_version != SHADOW_SIGNAL_SCHEMA_VERSION:
        raise LiveCachePayloadError("unsupported shadow signal schema_version")
    if signal.mode != SHADOW_SIGNAL_MODE:
        raise LiveCachePayloadError("shadow signal mode must be shadow")

    _strict_int(
        signal.selection_schema_version,
        "selection_schema_version",
        minimum=1,
    )
    _required_string(signal.selection_policy_version, "selection_policy_version")
    for field_name, digest in (
        ("selection_fingerprint_sha256", signal.selection_fingerprint_sha256),
        ("selection_artifact_sha256", signal.selection_artifact_sha256),
    ):
        if not isinstance(digest, str) or _SHA256_PATTERN.fullmatch(digest) is None:
            raise LiveCachePayloadError(f"{field_name} must be lowercase SHA-256")

    evidence_end_ms = _strict_int(
        signal.selection_evidence_end_ms,
        "selection_evidence_end_ms",
    )
    generated_ms = _strict_int(signal.generated_ms, "generated_ms")
    if evidence_end_ms > generated_ms:
        raise LiveCachePayloadError("selection evidence cannot end in the future")

    _required_string(signal.model_version, "model_version")
    beta = _require_decimal(signal.beta, "beta")
    if beta is not None and beta < 0:
        raise LiveCachePayloadError("beta must be non-negative")
    valid = _required_bool(signal.valid, "valid")
    status = _required_string(signal.status, "status")
    _required_string(signal.state, "state")
    if not isinstance(signal.invalid_reasons, tuple) or (
        len(signal.invalid_reasons) > SHADOW_SIGNAL_MAX_INVALID_REASONS
    ):
        raise LiveCachePayloadError(
            "invalid_reasons must be a tuple of non-empty strings"
        )
    for reason in signal.invalid_reasons:
        _required_string(reason, "invalid_reasons item")
    if len(set(signal.invalid_reasons)) != len(signal.invalid_reasons):
        raise LiveCachePayloadError("invalid_reasons must not contain duplicates")

    horizon_ms = _strict_int(signal.horizon_ms, "horizon_ms", minimum=1)
    estimated_lag_ms = _strict_int(
        signal.estimated_lag_ms,
        "estimated_lag_ms",
        minimum=1,
    )
    if estimated_lag_ms != horizon_ms:
        raise LiveCachePayloadError("estimated_lag_ms must equal horizon_ms")

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
            raise LiveCachePayloadError(
                "valid shadow signal must have valid status and no reasons"
            )
        if any(
            value is None
            for value in projection_fields + required_anchor_fields
        ):
            raise LiveCachePayloadError(
                "valid shadow signal requires projection and anchor fields"
            )
        current_chainlink = _require_decimal(
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
        _require_decimal(signal.futures_now, "futures_now", positive=True)
        _require_decimal(
            signal.futures_reference,
            "futures_reference",
            positive=True,
        )
        if signal.direction not in {"up", "down", "flat"}:
            raise LiveCachePayloadError("direction is invalid")
        if projected - current_chainlink != pending:
            raise LiveCachePayloadError("pending_move is inconsistent")
        with localcontext() as context:
            context.prec = 28
            expected_bps = pending / current_chainlink * Decimal("10000")
        if pending_bps != expected_bps:
            raise LiveCachePayloadError("pending_move_bps is inconsistent")
        expected_direction = (
            "up" if pending > 0 else "down" if pending < 0 else "flat"
        )
        if signal.direction != expected_direction:
            raise LiveCachePayloadError("direction is inconsistent")

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
            raise LiveCachePayloadError(
                "current Chainlink input must match the active anchor"
            )
        if reference_target != anchor_received - estimated_lag_ms:
            raise LiveCachePayloadError("futures reference target is inconsistent")
        if reference_received > reference_target or (
            reference_gap != reference_target - reference_received
        ):
            raise LiveCachePayloadError("futures reference gap is inconsistent")
    else:
        if status == "valid" or not signal.invalid_reasons:
            raise LiveCachePayloadError(
                "invalid shadow signal requires a non-valid status and reasons"
            )
        if any(
            value is not None
            for value in projection_fields + all_anchor_fields
        ):
            raise LiveCachePayloadError(
                "invalid shadow signal projection and anchor fields must be null"
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
        raise LiveCachePayloadError("market window must be five minutes")
    if market_start_ms % 300_000 != 0:
        raise LiveCachePayloadError("market_start_ms must align to five minutes")
    if market_id != market_start_ms // 300_000:
        raise LiveCachePayloadError("market_id is inconsistent")
    if not market_start_ms <= generated_ms < market_end_ms:
        raise LiveCachePayloadError("generated_ms is outside the market window")
    if ms_to_market_end != market_end_ms - generated_ms:
        raise LiveCachePayloadError("ms_to_market_end is inconsistent")
    if full_horizon != (generated_ms + horizon_ms <= market_end_ms):
        raise LiveCachePayloadError(
            "full_horizon_before_market_end is inconsistent"
        )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise LiveCachePayloadError("shadow signal contains a duplicate field")
        payload[key] = value
    return payload


def _reject_json_float(raw_value: str) -> None:
    raise LiveCachePayloadError(
        f"JSON floating-point value is forbidden: {raw_value}"
    )


def _reject_json_constant(raw_value: str) -> None:
    raise LiveCachePayloadError(f"non-finite JSON value is forbidden: {raw_value}")


def _bounded_json_int(raw_value: str) -> int:
    digits = raw_value[1:] if raw_value.startswith("-") else raw_value
    if len(digits) > 19:
        raise LiveCachePayloadError("JSON integer exceeds signed BIGINT")
    parsed = int(raw_value)
    if abs(parsed) > SHADOW_SIGNAL_MAX_WIRE_INT:
        raise LiveCachePayloadError("JSON integer exceeds signed BIGINT")
    return parsed


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


def _price_delivery_metadata(
    publisher_epoch: Any,
    accepted_event_sequence: Any,
    *,
    payload_error: bool,
) -> tuple[Optional[str], Optional[int]]:
    error_type = LiveCachePayloadError if payload_error else ValueError
    present = (
        publisher_epoch is not None,
        accepted_event_sequence is not None,
    )
    if present[0] != present[1]:
        raise error_type(
            "publisher_epoch and accepted_event_sequence must be provided together"
        )
    if publisher_epoch is None:
        return None, None
    if not isinstance(publisher_epoch, str):
        if payload_error:
            raise LiveCachePayloadError("publisher_epoch must be a string")
        raise TypeError("publisher_epoch must be a string")
    try:
        parsed_epoch = UUID(publisher_epoch)
    except (ValueError, AttributeError) as exc:
        raise error_type("publisher_epoch must be a canonical UUID") from exc
    if str(parsed_epoch) != publisher_epoch:
        raise error_type("publisher_epoch must be a canonical UUID")
    if (
        isinstance(accepted_event_sequence, bool)
        or not isinstance(accepted_event_sequence, int)
    ):
        if payload_error:
            raise LiveCachePayloadError(
                "accepted_event_sequence must be an integer"
            )
        raise TypeError("accepted_event_sequence must be an integer")
    if accepted_event_sequence <= 0:
        raise error_type("accepted_event_sequence must be positive")
    if accepted_event_sequence > SHADOW_SIGNAL_MAX_WIRE_INT:
        raise error_type("accepted_event_sequence exceeds signed BIGINT")
    return publisher_epoch, accepted_event_sequence


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
    publisher_epoch: Optional[str] = None,
    accepted_event_sequence: Optional[int] = None,
) -> str:
    publisher_epoch, accepted_event_sequence = _price_delivery_metadata(
        publisher_epoch,
        accepted_event_sequence,
        payload_error=False,
    )
    payload = {
        "value": str(value),
        "source_timestamp_ms": source_timestamp_ms,
        "received_ms": received_ms,
    }
    if publisher_epoch is not None:
        payload["publisher_epoch"] = publisher_epoch
        payload["accepted_event_sequence"] = accepted_event_sequence
    return json.dumps(
        payload,
        separators=(",", ":"),
    )


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
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise LiveCachePayloadError(
            "live cache value must contain valid Unicode"
        ) from exc

    publisher_epoch, accepted_event_sequence = _price_delivery_metadata(
        payload.get("publisher_epoch"),
        payload.get("accepted_event_sequence"),
        payload_error=True,
    )

    return LivePrice(
        value=value,
        source_timestamp_ms=_optional_int(
            payload.get("source_timestamp_ms"),
            "source_timestamp_ms",
        ),
        received_ms=_required_int(payload.get("received_ms"), "received_ms"),
        publisher_epoch=publisher_epoch,
        accepted_event_sequence=accepted_event_sequence,
    )


def _shadow_signal_payload(signal: LiveShadowSignal) -> dict[str, Any]:
    if not isinstance(signal, LiveShadowSignal):
        raise TypeError("signal must be LiveShadowSignal")
    _validate_shadow_signal(signal)
    return {
        "schema_version": signal.schema_version,
        "mode": signal.mode,
        "selection_schema_version": signal.selection_schema_version,
        "selection_policy_version": signal.selection_policy_version,
        "selection_fingerprint_sha256": (
            signal.selection_fingerprint_sha256
        ),
        "selection_artifact_sha256": signal.selection_artifact_sha256,
        "selection_evidence_end_ms": signal.selection_evidence_end_ms,
        "model_version": signal.model_version,
        "beta": _decimal_to_json(signal.beta),
        "generated_ms": signal.generated_ms,
        "valid": signal.valid,
        "status": signal.status,
        "invalid_reasons": list(signal.invalid_reasons),
        "state": signal.state,
        "horizon_ms": signal.horizon_ms,
        "estimated_lag_ms": signal.estimated_lag_ms,
        "current_chainlink": _decimal_to_json(signal.current_chainlink),
        "projected_chainlink": _decimal_to_json(
            signal.projected_chainlink
        ),
        "pending_move": _decimal_to_json(signal.pending_move),
        "pending_move_bps": _decimal_to_json(signal.pending_move_bps),
        "direction": signal.direction,
        "futures_now": _decimal_to_json(signal.futures_now),
        "futures_reference": _decimal_to_json(signal.futures_reference),
        "chainlink_now_source_timestamp_ms": (
            signal.chainlink_now_source_timestamp_ms
        ),
        "chainlink_now_received_ms": signal.chainlink_now_received_ms,
        "anchor_chainlink_source_timestamp_ms": (
            signal.anchor_chainlink_source_timestamp_ms
        ),
        "anchor_chainlink_received_ms": (
            signal.anchor_chainlink_received_ms
        ),
        "futures_now_source_timestamp_ms": (
            signal.futures_now_source_timestamp_ms
        ),
        "futures_now_received_ms": signal.futures_now_received_ms,
        "futures_reference_source_timestamp_ms": (
            signal.futures_reference_source_timestamp_ms
        ),
        "futures_reference_received_ms": (
            signal.futures_reference_received_ms
        ),
        "futures_reference_target_ms": (
            signal.futures_reference_target_ms
        ),
        "futures_reference_gap_ms": signal.futures_reference_gap_ms,
        "futures_received_age_ms": signal.futures_received_age_ms,
        "chainlink_received_age_ms": signal.chainlink_received_age_ms,
        "market_id": signal.market_id,
        "market_start_ms": signal.market_start_ms,
        "market_end_ms": signal.market_end_ms,
        "ms_to_market_end": signal.ms_to_market_end,
        "full_horizon_before_market_end": (
            signal.full_horizon_before_market_end
        ),
    }


def encode_shadow_signal(signal: LiveShadowSignal) -> str:
    return json.dumps(
        _shadow_signal_payload(signal),
        separators=(",", ":"),
    )


def serialize_shadow_signal(
    signal: Optional[LiveShadowSignal],
    *,
    server_time_ms: int,
) -> Optional[dict[str, Any]]:
    if signal is None:
        return None
    payload = _shadow_signal_payload(signal)
    payload["signal_age_ms"] = age_ms(server_time_ms, signal.generated_ms)
    return payload


def decode_shadow_signal(raw: Any) -> Optional[LiveShadowSignal]:
    text = _redis_payload_text(raw, "shadow signal payload")
    if text is None:
        return None
    if len(text) > SHADOW_SIGNAL_MAX_PAYLOAD_CHARS:
        raise LiveCachePayloadError("shadow signal payload is too large")
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_int=_bounded_json_int,
            parse_float=_reject_json_float,
            parse_constant=_reject_json_constant,
        )
    except LiveCachePayloadError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise LiveCachePayloadError(
            "shadow signal payload is not valid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise LiveCachePayloadError("shadow signal payload must be a JSON object")

    fields = set(payload)
    if fields != _SHADOW_SIGNAL_FIELDS:
        raise LiveCachePayloadError("shadow signal fields differ from its schema")

    invalid_reasons = payload["invalid_reasons"]
    if not isinstance(invalid_reasons, list):
        raise LiveCachePayloadError("invalid_reasons must be an array")
    direction = payload["direction"]
    if direction is not None and not isinstance(direction, str):
        raise LiveCachePayloadError("direction must be a string or null")

    beta = _decimal_from_json(payload["beta"], "beta", optional=False)
    if beta is None:
        raise LiveCachePayloadError("beta is required")

    return LiveShadowSignal(
        schema_version=_strict_int(payload["schema_version"], "schema_version"),
        mode=_required_string(payload["mode"], "mode"),
        selection_schema_version=_strict_int(
            payload["selection_schema_version"],
            "selection_schema_version",
            minimum=1,
        ),
        selection_policy_version=_required_string(
            payload["selection_policy_version"],
            "selection_policy_version",
        ),
        selection_fingerprint_sha256=_required_string(
            payload["selection_fingerprint_sha256"],
            "selection_fingerprint_sha256",
        ),
        selection_artifact_sha256=_required_string(
            payload["selection_artifact_sha256"],
            "selection_artifact_sha256",
        ),
        selection_evidence_end_ms=_strict_int(
            payload["selection_evidence_end_ms"],
            "selection_evidence_end_ms",
        ),
        model_version=_required_string(payload["model_version"], "model_version"),
        beta=beta,
        generated_ms=_strict_int(payload["generated_ms"], "generated_ms"),
        valid=_required_bool(payload["valid"], "valid"),
        status=_required_string(payload["status"], "status"),
        invalid_reasons=tuple(invalid_reasons),
        state=_required_string(payload["state"], "state"),
        horizon_ms=_strict_int(payload["horizon_ms"], "horizon_ms", minimum=1),
        estimated_lag_ms=_strict_int(
            payload["estimated_lag_ms"],
            "estimated_lag_ms",
            minimum=1,
        ),
        current_chainlink=_decimal_from_json(
            payload["current_chainlink"],
            "current_chainlink",
            optional=True,
        ),
        projected_chainlink=_decimal_from_json(
            payload["projected_chainlink"],
            "projected_chainlink",
            optional=True,
        ),
        pending_move=_decimal_from_json(
            payload["pending_move"],
            "pending_move",
            optional=True,
        ),
        pending_move_bps=_decimal_from_json(
            payload["pending_move_bps"],
            "pending_move_bps",
            optional=True,
        ),
        direction=direction,
        futures_now=_decimal_from_json(
            payload["futures_now"],
            "futures_now",
            optional=True,
        ),
        futures_reference=_decimal_from_json(
            payload["futures_reference"],
            "futures_reference",
            optional=True,
        ),
        chainlink_now_source_timestamp_ms=_optional_strict_int(
            payload["chainlink_now_source_timestamp_ms"],
            "chainlink_now_source_timestamp_ms",
        ),
        chainlink_now_received_ms=_optional_strict_int(
            payload["chainlink_now_received_ms"],
            "chainlink_now_received_ms",
        ),
        anchor_chainlink_source_timestamp_ms=_optional_strict_int(
            payload["anchor_chainlink_source_timestamp_ms"],
            "anchor_chainlink_source_timestamp_ms",
        ),
        anchor_chainlink_received_ms=_optional_strict_int(
            payload["anchor_chainlink_received_ms"],
            "anchor_chainlink_received_ms",
        ),
        futures_now_source_timestamp_ms=_optional_strict_int(
            payload["futures_now_source_timestamp_ms"],
            "futures_now_source_timestamp_ms",
        ),
        futures_now_received_ms=_optional_strict_int(
            payload["futures_now_received_ms"],
            "futures_now_received_ms",
        ),
        futures_reference_source_timestamp_ms=_optional_strict_int(
            payload["futures_reference_source_timestamp_ms"],
            "futures_reference_source_timestamp_ms",
        ),
        futures_reference_received_ms=_optional_strict_int(
            payload["futures_reference_received_ms"],
            "futures_reference_received_ms",
        ),
        futures_reference_target_ms=_optional_strict_int(
            payload["futures_reference_target_ms"],
            "futures_reference_target_ms",
        ),
        futures_reference_gap_ms=_optional_strict_int(
            payload["futures_reference_gap_ms"],
            "futures_reference_gap_ms",
        ),
        futures_received_age_ms=_optional_strict_int(
            payload["futures_received_age_ms"],
            "futures_received_age_ms",
        ),
        chainlink_received_age_ms=_optional_strict_int(
            payload["chainlink_received_age_ms"],
            "chainlink_received_age_ms",
        ),
        market_id=_strict_int(payload["market_id"], "market_id"),
        market_start_ms=_strict_int(
            payload["market_start_ms"],
            "market_start_ms",
        ),
        market_end_ms=_strict_int(payload["market_end_ms"], "market_end_ms"),
        ms_to_market_end=_strict_int(
            payload["ms_to_market_end"],
            "ms_to_market_end",
        ),
        full_horizon_before_market_end=_required_bool(
            payload["full_horizon_before_market_end"],
            "full_horizon_before_market_end",
        ),
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
            decode_responses=False,
            socket_connect_timeout=socket_timeout,
            socket_timeout=socket_timeout,
        )
        self._shadow_payload_errors_total = 0

    async def set_price(
        self,
        key: str,
        *,
        value: Decimal,
        source_timestamp_ms: Optional[int],
        received_ms: int,
        publisher_epoch: Optional[str] = None,
        accepted_event_sequence: Optional[int] = None,
    ) -> None:
        await self.redis.set(
            key,
            encode_live_price(
                value=value,
                source_timestamp_ms=source_timestamp_ms,
                received_ms=received_ms,
                publisher_epoch=publisher_epoch,
                accepted_event_sequence=accepted_event_sequence,
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

    async def get_prices_independent(
        self,
        keys: Iterable[str],
    ) -> tuple[
        dict[str, Optional[LivePrice]],
        dict[str, LiveCachePayloadError],
    ]:
        key_list = list(keys)
        raw_values = await self.redis.mget(key_list)
        prices: dict[str, Optional[LivePrice]] = {}
        errors: dict[str, LiveCachePayloadError] = {}
        for key, raw_value in zip(key_list, raw_values):
            try:
                prices[key] = decode_live_price(raw_value)
            except LiveCachePayloadError as exc:
                prices[key] = None
                errors[key] = exc
        return prices, errors

    async def get_prices_and_shadow_signal(
        self,
        keys: Iterable[str],
    ) -> tuple[
        dict[str, Optional[LivePrice]],
        Optional[LiveShadowSignal],
    ]:
        key_list = list(keys)
        if CHAINLINK_SHADOW_LIVE_KEY in key_list:
            raise ValueError("shadow signal key must not be a price key")
        redis_keys = [*key_list, CHAINLINK_SHADOW_LIVE_KEY]
        raw_values = await self.redis.mget(redis_keys)
        if len(raw_values) != len(redis_keys):
            raise LiveCachePayloadError(
                "live cache MGET returned an unexpected value count"
            )
        prices = {
            key: decode_live_price(raw_value)
            for key, raw_value in zip(key_list, raw_values[:-1])
        }
        try:
            shadow_signal = decode_shadow_signal(raw_values[-1])
        except LiveCachePayloadError:
            self._shadow_payload_errors_total += 1
            occurrence = self._shadow_payload_errors_total
            if (
                occurrence == 1
                or occurrence % SHADOW_PAYLOAD_ERROR_LOG_EVERY == 0
            ):
                LOGGER.warning(
                    "shadow_signal_live_cache_payload_invalid",
                    extra={
                        "event": "shadow_signal_live_cache_payload_invalid",
                        "redis_key": CHAINLINK_SHADOW_LIVE_KEY,
                        "error_category": "decode_or_validation_failed",
                        "occurrence": occurrence,
                    },
                )
            shadow_signal = None
        return prices, shadow_signal

    async def set_shadow_signal(
        self,
        signal: LiveShadowSignal,
        ttl_ms: int,
    ) -> None:
        if isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int):
            raise TypeError("ttl_ms must be an integer")
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")
        await self.redis.set(
            CHAINLINK_SHADOW_LIVE_KEY,
            encode_shadow_signal(signal),
            px=ttl_ms,
        )

    async def get_shadow_signal(self) -> Optional[LiveShadowSignal]:
        return decode_shadow_signal(
            await self.redis.get(CHAINLINK_SHADOW_LIVE_KEY)
        )

    async def delete_shadow_signal(self) -> None:
        await self.redis.delete(CHAINLINK_SHADOW_LIVE_KEY)

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
    cached, shadow_signal = await live_cache.get_prices_and_shadow_signal(
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
        "signals": {
            "chainlink_catchup": serialize_shadow_signal(
                shadow_signal,
                server_time_ms=server_time_ms,
            ),
        },
    }
