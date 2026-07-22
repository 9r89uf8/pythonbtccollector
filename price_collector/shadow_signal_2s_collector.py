"""Standalone Redis-only publisher for the frozen two-second challenger."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from typing import Any, Awaitable, Callable, Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LIVE_CACHE_READ_ERRORS,
    LiveCache,
    LiveCachePayloadError,
    LivePrice,
    create_live_cache,
)
from price_collector.shadow_signal import (
    CatchupModel,
    EngineObservation,
    ObservedPrice,
    ShadowSignalEngine,
)
from price_collector.shadow_signal_2s_live import (
    SHADOW_SIGNAL_2S_BETA,
    SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
    SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
    SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
    SHADOW_SIGNAL_2S_MODE,
    SHADOW_SIGNAL_2S_MODEL_VERSION,
    SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
    SHADOW_SIGNAL_2S_SCHEMA_VERSION,
    SHADOW_SIGNAL_2S_TRANSPORT_ERRORS,
    LiveShadowSignal2s,
    ShadowSignal2sStore,
    create_shadow_signal_2s_store,
    project_chainlink_2s,
)


LOGGER = logging.getLogger("price_collector.shadow_signal_2s_collector")

SHADOW_SIGNAL_2S_POLL_MS = 100
SHADOW_SIGNAL_2S_TTL_MS = 2_000
SHADOW_SIGNAL_2S_FUTURES_STALE_MS = 3_000
SHADOW_SIGNAL_2S_CHAINLINK_STALE_MS = 2_500
SHADOW_SIGNAL_2S_REFERENCE_MAX_GAP_MS = 3_000
SHADOW_SIGNAL_2S_HISTORY_RETENTION_MS = 10_000
SHADOW_SIGNAL_2S_MAX_FUTURE_SKEW_MS = 250

SHADOW_SIGNAL_2S_MODEL = CatchupModel(
    version=SHADOW_SIGNAL_2S_MODEL_VERSION,
    lag_ms=SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
    beta=SHADOW_SIGNAL_2S_BETA,
)

NowMs = Callable[[], int]
Sleep = Callable[[float], Awaitable[None]]
LiveCacheFactory = Callable[[Any], LiveCache]
SignalStoreFactory = Callable[[Any], ShadowSignal2sStore]


class ShadowSignal2sSettings(BaseSettings):
    """Environment surface for the isolated Redis-only challenger."""

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=True)

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_SOCKET_TIMEOUT_SECONDS: float = 0.25

    SHADOW_SIGNAL_2S_ENABLED: bool = False
    SHADOW_SIGNAL_2S_POLL_MS: int = Field(
        default=SHADOW_SIGNAL_2S_POLL_MS,
        ge=SHADOW_SIGNAL_2S_POLL_MS,
        le=SHADOW_SIGNAL_2S_POLL_MS,
    )
    SHADOW_SIGNAL_2S_TTL_MS: int = Field(
        default=SHADOW_SIGNAL_2S_TTL_MS,
        ge=SHADOW_SIGNAL_2S_TTL_MS,
        le=SHADOW_SIGNAL_2S_TTL_MS,
    )

    @model_validator(mode="after")
    def validate_ttl_exceeds_poll(self) -> "ShadowSignal2sSettings":
        if self.SHADOW_SIGNAL_2S_TTL_MS <= self.SHADOW_SIGNAL_2S_POLL_MS:
            raise ValueError(
                "SHADOW_SIGNAL_2S_TTL_MS must exceed "
                "SHADOW_SIGNAL_2S_POLL_MS"
            )
        return self


class _JsonLogFormatter(logging.Formatter):
    _standard_attrs = set(
        vars(logging.LogRecord("", 0, "", 0, "", (), None))
    )

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "at": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
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


def setup_logging(log_level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonLogFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level.upper())


def current_utc_epoch_ms() -> int:
    return time.time_ns() // 1_000_000


def milliseconds_until_next_poll_boundary(now_ms: int, poll_ms: int) -> int:
    if isinstance(now_ms, bool) or not isinstance(now_ms, int):
        raise TypeError("now_ms must be an integer")
    if now_ms < 0:
        raise ValueError("now_ms must be non-negative")
    if isinstance(poll_ms, bool) or not isinstance(poll_ms, int):
        raise TypeError("poll_ms must be an integer")
    if poll_ms <= 0:
        raise ValueError("poll_ms must be positive")
    remainder = now_ms % poll_ms
    return poll_ms if remainder == 0 else poll_ms - remainder


def _observed_price(
    price: Optional[LivePrice],
    *,
    key: str,
) -> Optional[ObservedPrice]:
    if price is None:
        return None
    try:
        return ObservedPrice(
            value=Decimal(price.value),
            source_timestamp_ms=price.source_timestamp_ms,
            received_ms=price.received_ms,
            publisher_epoch=price.publisher_epoch,
            accepted_event_sequence=price.accepted_event_sequence,
        )
    except (DecimalException, TypeError, ValueError) as exc:
        raise LiveCachePayloadError(
            f"{key} contains an invalid price value"
        ) from exc


def build_live_shadow_signal_2s(
    observation: EngineObservation,
) -> LiveShadowSignal2s:
    signal = observation.signal_for(SHADOW_SIGNAL_2S_MODEL.version)
    anchor = signal.anchor if signal.valid else None
    futures_now = signal.futures_now
    chainlink_now = signal.chainlink_now
    reference = anchor.futures_reference if anchor is not None else None
    projected_chainlink: Optional[Decimal] = None
    pending_move: Optional[Decimal] = None
    pending_move_bps: Optional[Decimal] = None
    direction: Optional[str] = None
    if signal.valid:
        if chainlink_now is None or futures_now is None or reference is None:
            raise RuntimeError("valid challenger signal is missing its inputs")
        projected_chainlink = project_chainlink_2s(
            current_chainlink=chainlink_now.value,
            futures_now=futures_now.value,
            futures_reference=reference.value,
        )
        with localcontext() as context:
            context.prec = 28
            pending_move = projected_chainlink - chainlink_now.value
            pending_move_bps = (
                pending_move / chainlink_now.value * Decimal("10000")
            )
        direction = (
            "up"
            if pending_move > 0
            else "down"
            if pending_move < 0
            else "flat"
        )

    return LiveShadowSignal2s(
        schema_version=SHADOW_SIGNAL_2S_SCHEMA_VERSION,
        mode=SHADOW_SIGNAL_2S_MODE,
        publication_role=SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
        experiment_version=SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
        model_version=SHADOW_SIGNAL_2S_MODEL.version,
        beta=SHADOW_SIGNAL_2S_MODEL.beta,
        futures_lookback_ms=SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
        forecast_horizon_ms=SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
        generated_ms=observation.generated_ms,
        target_ms=(
            observation.generated_ms
            + SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS
        ),
        valid=signal.valid,
        status=signal.status,
        invalid_reasons=signal.invalid_reasons,
        state=signal.state,
        current_chainlink=(
            chainlink_now.value if chainlink_now is not None else None
        ),
        projected_chainlink=projected_chainlink,
        pending_move=pending_move,
        pending_move_bps=pending_move_bps,
        direction=direction,
        futures_now=(
            futures_now.value if futures_now is not None else None
        ),
        futures_reference=(
            reference.value if reference is not None else None
        ),
        chainlink_now_source_timestamp_ms=(
            chainlink_now.source_timestamp_ms
            if chainlink_now is not None
            else None
        ),
        chainlink_now_received_ms=(
            chainlink_now.received_ms
            if chainlink_now is not None
            else None
        ),
        anchor_chainlink_source_timestamp_ms=(
            anchor.chainlink.source_timestamp_ms
            if anchor is not None
            else None
        ),
        anchor_chainlink_received_ms=(
            anchor.chainlink.received_ms if anchor is not None else None
        ),
        futures_now_source_timestamp_ms=(
            futures_now.source_timestamp_ms
            if futures_now is not None
            else None
        ),
        futures_now_received_ms=(
            futures_now.received_ms if futures_now is not None else None
        ),
        futures_reference_source_timestamp_ms=(
            reference.source_timestamp_ms if reference is not None else None
        ),
        futures_reference_received_ms=(
            reference.received_ms if reference is not None else None
        ),
        futures_reference_target_ms=(
            signal.futures_reference_target_ms if signal.valid else None
        ),
        futures_reference_gap_ms=(
            signal.futures_reference_gap_ms if signal.valid else None
        ),
        futures_received_age_ms=signal.futures_received_age_ms,
        chainlink_received_age_ms=signal.chainlink_received_age_ms,
        market_id=observation.market.market_id,
        market_start_ms=observation.market.market_start_ms,
        market_end_ms=observation.market.market_end_ms,
        ms_to_market_end=observation.ms_to_market_end,
        full_horizon_before_market_end=(
            signal.full_horizon_before_market_end
        ),
    )


class ShadowSignal2sWorker:
    def __init__(
        self,
        *,
        live_cache: LiveCache,
        signal_store: ShadowSignal2sStore,
        poll_ms: int = SHADOW_SIGNAL_2S_POLL_MS,
        ttl_ms: int = SHADOW_SIGNAL_2S_TTL_MS,
        now_ms: NowMs = current_utc_epoch_ms,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if poll_ms != SHADOW_SIGNAL_2S_POLL_MS:
            raise ValueError("the two-second challenger poll interval is frozen")
        if ttl_ms != SHADOW_SIGNAL_2S_TTL_MS:
            raise ValueError("the two-second challenger TTL is frozen")
        self.live_cache = live_cache
        self.signal_store = signal_store
        self.poll_ms = poll_ms
        self.ttl_ms = ttl_ms
        self.now_ms = now_ms
        self.sleep = sleep
        self.engine = ShadowSignalEngine(
            models=(SHADOW_SIGNAL_2S_MODEL,),
            futures_stale_ms=SHADOW_SIGNAL_2S_FUTURES_STALE_MS,
            chainlink_stale_ms=SHADOW_SIGNAL_2S_CHAINLINK_STALE_MS,
            reference_max_gap_ms=(
                SHADOW_SIGNAL_2S_REFERENCE_MAX_GAP_MS
            ),
            history_retention_ms=SHADOW_SIGNAL_2S_HISTORY_RETENTION_MS,
            max_future_skew_ms=SHADOW_SIGNAL_2S_MAX_FUTURE_SKEW_MS,
        )
        self._last_logged_state: Optional[tuple[bool, str, str]] = None

    async def run_once(
        self,
        *,
        now_ms: Optional[int] = None,
    ) -> Optional[LiveShadowSignal2s]:
        try:
            prices, payload_errors = (
                await self.live_cache.get_prices_independent(
                    [FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]
                )
            )
        except LIVE_CACHE_READ_ERRORS:
            LOGGER.exception(
                "shadow_signal_2s_cache_read_failed",
                extra={"event": "shadow_signal_2s_cache_read_failed"},
            )
            return None

        # Stamp after MGET: this is when both causal inputs became available.
        generated_ms = self.now_ms() if now_ms is None else now_ms

        for key, error in payload_errors.items():
            LOGGER.warning(
                "shadow_signal_2s_input_payload_invalid",
                extra={
                    "event": "shadow_signal_2s_input_payload_invalid",
                    "redis_key": key,
                    "error": str(error),
                },
            )

        observed: dict[str, Optional[ObservedPrice]] = {}
        for key in (FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY):
            try:
                observed[key] = _observed_price(prices.get(key), key=key)
            except LiveCachePayloadError as error:
                observed[key] = None
                LOGGER.warning(
                    "shadow_signal_2s_input_price_invalid",
                    extra={
                        "event": "shadow_signal_2s_input_price_invalid",
                        "redis_key": key,
                        "error": str(error),
                    },
                )

        observation = self.engine.observe(
            futures=observed[FUTURES_LIVE_KEY],
            chainlink=observed[CHAINLINK_LIVE_KEY],
            now_ms=generated_ms,
        )
        payload = build_live_shadow_signal_2s(observation)
        self._log_state_transition(payload)

        # Invalid observations are deliberately written too, replacing any
        # prior valid challenger result instead of carrying it forward.
        try:
            await self.signal_store.set_signal(payload, ttl_ms=self.ttl_ms)
        except SHADOW_SIGNAL_2S_TRANSPORT_ERRORS:
            LOGGER.exception(
                "shadow_signal_2s_cache_write_failed",
                extra={"event": "shadow_signal_2s_cache_write_failed"},
            )
        return payload

    def _log_state_transition(self, payload: LiveShadowSignal2s) -> None:
        state = (payload.valid, payload.status, payload.state)
        if state == self._last_logged_state:
            return
        self._last_logged_state = state
        LOGGER.info(
            "shadow_signal_2s_state_changed",
            extra={
                "event": "shadow_signal_2s_state_changed",
                "model_version": payload.model_version,
                "valid": payload.valid,
                "status": payload.status,
                "state": payload.state,
            },
        )

    async def run(self, *, max_iterations: Optional[int] = None) -> None:
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            delay_ms = milliseconds_until_next_poll_boundary(
                self.now_ms(),
                self.poll_ms,
            )
            await self.sleep(delay_ms / 1_000)
            await self.run_once()
            iterations += 1


async def run_collector(
    settings: ShadowSignal2sSettings,
    *,
    live_cache_factory: LiveCacheFactory = create_live_cache,
    signal_store_factory: SignalStoreFactory = (
        create_shadow_signal_2s_store
    ),
    max_iterations: Optional[int] = None,
) -> None:
    setup_logging(settings.LOG_LEVEL)
    if not settings.SHADOW_SIGNAL_2S_ENABLED:
        LOGGER.info(
            "shadow_signal_2s_disabled",
            extra={"event": "shadow_signal_2s_disabled"},
        )
        return

    LOGGER.info(
        "shadow_signal_2s_starting",
        extra={
            "event": "shadow_signal_2s_starting",
            "app_env": settings.APP_ENV,
            "model_version": SHADOW_SIGNAL_2S_MODEL.version,
            "futures_lookback_ms": SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
            "forecast_horizon_ms": SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
            "beta": str(SHADOW_SIGNAL_2S_MODEL.beta),
            "poll_ms": settings.SHADOW_SIGNAL_2S_POLL_MS,
            "ttl_ms": settings.SHADOW_SIGNAL_2S_TTL_MS,
            "futures_stale_ms": SHADOW_SIGNAL_2S_FUTURES_STALE_MS,
            "chainlink_stale_ms": SHADOW_SIGNAL_2S_CHAINLINK_STALE_MS,
            "reference_max_gap_ms": (
                SHADOW_SIGNAL_2S_REFERENCE_MAX_GAP_MS
            ),
            "history_retention_ms": (
                SHADOW_SIGNAL_2S_HISTORY_RETENTION_MS
            ),
        },
    )

    live_cache: Optional[LiveCache] = None
    signal_store: Optional[ShadowSignal2sStore] = None
    try:
        live_cache = live_cache_factory(settings)
        signal_store = signal_store_factory(settings)
        worker = ShadowSignal2sWorker(
            live_cache=live_cache,
            signal_store=signal_store,
            poll_ms=settings.SHADOW_SIGNAL_2S_POLL_MS,
            ttl_ms=settings.SHADOW_SIGNAL_2S_TTL_MS,
        )
        await worker.run(max_iterations=max_iterations)
    finally:
        try:
            if signal_store is not None:
                await signal_store.close()
        finally:
            if live_cache is not None:
                await live_cache.close()


def main() -> None:
    settings = ShadowSignal2sSettings()
    try:
        asyncio.run(run_collector(settings))
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
