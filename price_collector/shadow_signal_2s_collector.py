"""Standalone live publisher for the frozen two-second challenger."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, localcontext
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from price_collector.db import create_shadow_evaluation_backend
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
from price_collector.shadow_signal_evaluation import (
    ShadowEvaluationProvenance,
    ShadowEvaluationScheduler,
    ShadowEvaluationWriterRuntime,
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
EvaluationBackendFactory = Callable[[], Any]
EvaluationSchedulerFactory = Callable[..., ShadowEvaluationScheduler]
EvaluationWriterFactory = Callable[..., ShadowEvaluationWriterRuntime]


SHADOW_SIGNAL_2S_REGISTRATION_PATH = Path(__file__).with_name(
    "shadow_signal_2s_registration.json"
)
SHADOW_SIGNAL_2S_REGISTRATION = {
    "basis_feature_included": False,
    "evaluation": {
        "cadence_ms": 500,
        "causal_policy_version": "sequenced_cache_causal_v3",
        "retention_hours": 168,
    },
    "evidence": {
        "first_market_id": 5_948_856,
        "last_market_id": 5_948_955,
        "market_count": 100,
        "session_end_ms": 1_784_686_800_000,
        "session_start_ms": 1_784_656_800_000,
        "source": (
            "received_time_lead_lag_and_fixed_lookback_forecast_proxy"
        ),
        "source_artifact_filename": (
            "chainlink_futures_lead_lag_bundle_20260722-094106044.zip"
        ),
        "source_artifact_sha256": (
            "c763c6ac2811488f2e3ec5ae9992c9a174d40e2915efb51648c3a02e9d3de524"
        ),
    },
    "evidence_end_ms": 1_784_686_800_000,
    "experiment_version": SHADOW_SIGNAL_2S_EXPERIMENT_VERSION,
    "model": {
        "beta": str(SHADOW_SIGNAL_2S_BETA),
        "forecast_horizon_ms": SHADOW_SIGNAL_2S_FORECAST_HORIZON_MS,
        "futures_lookback_ms": SHADOW_SIGNAL_2S_FUTURES_LOOKBACK_MS,
        "model_version": SHADOW_SIGNAL_2S_MODEL_VERSION,
    },
    "policy_version": "prospective_fixed_challenger_v1",
    "publication_role": SHADOW_SIGNAL_2S_PUBLICATION_ROLE,
    "runtime": {
        "chainlink_stale_ms": SHADOW_SIGNAL_2S_CHAINLINK_STALE_MS,
        "futures_stale_ms": SHADOW_SIGNAL_2S_FUTURES_STALE_MS,
        "history_retention_ms": SHADOW_SIGNAL_2S_HISTORY_RETENTION_MS,
        "max_future_skew_ms": SHADOW_SIGNAL_2S_MAX_FUTURE_SKEW_MS,
        "poll_ms": SHADOW_SIGNAL_2S_POLL_MS,
        "reference_max_gap_ms": SHADOW_SIGNAL_2S_REFERENCE_MAX_GAP_MS,
        "ttl_ms": SHADOW_SIGNAL_2S_TTL_MS,
    },
    "schema_version": 4,
    "selected": False,
}


@dataclass(frozen=True)
class ShadowSignal2sRegistration:
    path: Path
    artifact_sha256: str
    fingerprint_sha256: str
    provenance: ShadowEvaluationProvenance


def _registration_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate registration key: {key}")
        result[key] = value
    return result


def _canonical_registration_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _registration_fingerprint_bytes(payload: dict[str, Any]) -> bytes:
    frozen_configuration = {
        key: payload[key]
        for key in (
            "basis_feature_included",
            "evaluation",
            "experiment_version",
            "model",
            "policy_version",
            "publication_role",
            "runtime",
            "selected",
        )
    }
    return _canonical_registration_bytes(frozen_configuration)


def load_shadow_signal_2s_registration(
    path: Path = SHADOW_SIGNAL_2S_REGISTRATION_PATH,
) -> ShadowSignal2sRegistration:
    path = Path(path)
    try:
        artifact_bytes = path.read_bytes()
        payload = json.loads(
            artifact_bytes.decode("utf-8"),
            object_pairs_hook=_registration_object,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(
            f"invalid two-second challenger registration: {path}"
        ) from exc
    if not isinstance(payload, dict) or (
        _canonical_registration_bytes(payload)
        != _canonical_registration_bytes(SHADOW_SIGNAL_2S_REGISTRATION)
    ):
        raise RuntimeError(
            "two-second challenger registration does not match the frozen "
            "prospective configuration"
        )
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    fingerprint_sha256 = hashlib.sha256(
        _registration_fingerprint_bytes(payload)
    ).hexdigest()
    return ShadowSignal2sRegistration(
        path=path,
        artifact_sha256=artifact_sha256,
        fingerprint_sha256=fingerprint_sha256,
        provenance=ShadowEvaluationProvenance(
            selection_schema_version=payload["schema_version"],
            policy_version=payload["policy_version"],
            selection_fingerprint_sha256=fingerprint_sha256,
            selection_artifact_sha256=artifact_sha256,
            evidence_end_ms=payload["evidence_end_ms"],
        ),
    )


class ShadowSignal2sSettings(BaseSettings):
    """Environment surface for the isolated live and evaluation challenger."""

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
    SHADOW_SIGNAL_2S_EVALUATION_ENABLED: bool = False
    SHADOW_SIGNAL_2S_EVALUATION_INTERVAL_MS: int = Field(
        default=500,
        ge=500,
        le=500,
    )
    SHADOW_SIGNAL_2S_EVALUATION_QUEUE_MAX: int = Field(
        default=5_000,
        gt=0,
        le=100_000,
    )
    SHADOW_SIGNAL_2S_EVALUATION_BATCH_MAX_ROWS: int = Field(
        default=500,
        gt=0,
        le=10_000,
    )
    SHADOW_SIGNAL_2S_EVALUATION_FLUSH_MS: int = Field(
        default=1_000,
        ge=100,
        le=60_000,
    )
    SHADOW_SIGNAL_2S_EVALUATION_RETRY_MS: int = Field(
        default=5_000,
        ge=100,
        le=300_000,
    )
    SHADOW_SIGNAL_2S_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS: float = Field(
        default=10.0,
        ge=1.0,
        le=60.0,
    )
    SHADOW_SIGNAL_2S_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS: float = Field(
        default=5.0,
        gt=0.0,
        le=60.0,
    )
    SHADOW_SIGNAL_2S_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS: float = Field(
        default=5.0,
        gt=0.0,
        le=60.0,
    )
    SHADOW_SIGNAL_2S_EVALUATION_RETENTION_HOURS: int = Field(
        default=168,
        ge=168,
        le=168,
    )
    SHADOW_SIGNAL_2S_EVALUATION_RETENTION_CHECK_SECONDS: int = Field(
        default=300,
        ge=60,
        le=86_400,
    )
    SHADOW_SIGNAL_2S_EVALUATION_RETENTION_BATCH_ROWS: int = Field(
        default=5_000,
        gt=0,
        le=100_000,
    )

    DATABASE_URL: Optional[str] = None
    READ_DATABASE_URL: Optional[str] = None

    @model_validator(mode="after")
    def validate_ttl_exceeds_poll(self) -> "ShadowSignal2sSettings":
        if self.SHADOW_SIGNAL_2S_TTL_MS <= self.SHADOW_SIGNAL_2S_POLL_MS:
            raise ValueError(
                "SHADOW_SIGNAL_2S_TTL_MS must exceed "
                "SHADOW_SIGNAL_2S_POLL_MS"
            )
        return self

    @model_validator(mode="after")
    def validate_evaluation_settings(self) -> "ShadowSignal2sSettings":
        if (
            self.SHADOW_SIGNAL_2S_EVALUATION_BATCH_MAX_ROWS
            > self.SHADOW_SIGNAL_2S_EVALUATION_QUEUE_MAX
        ):
            raise ValueError(
                "SHADOW_SIGNAL_2S_EVALUATION_BATCH_MAX_ROWS must be less "
                "than or equal to SHADOW_SIGNAL_2S_EVALUATION_QUEUE_MAX"
            )
        buckets_per_retention_check = (
            self.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_CHECK_SECONDS * 1_000
            + self.SHADOW_SIGNAL_2S_EVALUATION_INTERVAL_MS
            - 1
        ) // self.SHADOW_SIGNAL_2S_EVALUATION_INTERVAL_MS
        minimum_retention_batch_rows = buckets_per_retention_check * 5
        if (
            self.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_BATCH_ROWS
            < minimum_retention_batch_rows
        ):
            raise ValueError(
                "SHADOW_SIGNAL_2S_EVALUATION_RETENTION_BATCH_ROWS must "
                "cover five candidates in the shared table at the "
                "configured evaluation and retention-check cadences"
            )
        if self.READ_DATABASE_URL is not None:
            raise ValueError(
                "two-second challenger must not receive READ_DATABASE_URL"
            )
        if self.SHADOW_SIGNAL_2S_EVALUATION_ENABLED:
            if not self.SHADOW_SIGNAL_2S_ENABLED:
                raise ValueError(
                    "SHADOW_SIGNAL_2S_EVALUATION_ENABLED requires "
                    "SHADOW_SIGNAL_2S_ENABLED=true"
                )
            if self.DATABASE_URL is None or not self.DATABASE_URL.strip():
                raise ValueError(
                    "SHADOW_SIGNAL_2S_EVALUATION_ENABLED requires "
                    "DATABASE_URL"
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
        evaluation_scheduler: Optional[ShadowEvaluationScheduler] = None,
        evaluation_writer: Optional[ShadowEvaluationWriterRuntime] = None,
        now_ms: NowMs = current_utc_epoch_ms,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if poll_ms != SHADOW_SIGNAL_2S_POLL_MS:
            raise ValueError("the two-second challenger poll interval is frozen")
        if ttl_ms != SHADOW_SIGNAL_2S_TTL_MS:
            raise ValueError("the two-second challenger TTL is frozen")
        if (evaluation_scheduler is None) != (evaluation_writer is None):
            raise ValueError(
                "evaluation_scheduler and evaluation_writer must be "
                "provided together"
            )
        self.live_cache = live_cache
        self.signal_store = signal_store
        self.poll_ms = poll_ms
        self.ttl_ms = ttl_ms
        self.evaluation_scheduler = evaluation_scheduler
        self.evaluation_writer = evaluation_writer
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
        self._evaluate_noncritical(
            observation=observation,
            chainlink=observed[CHAINLINK_LIVE_KEY],
        )
        return payload

    def _evaluate_noncritical(
        self,
        *,
        observation: EngineObservation,
        chainlink: Optional[ObservedPrice],
    ) -> None:
        scheduler = self.evaluation_scheduler
        writer = self.evaluation_writer
        if scheduler is None or writer is None:
            return
        try:
            prior_gap_count = scheduler.observation_gap_count
            prior_discontinuity_counts = {
                "sequence_gap": scheduler.chainlink_sequence_gap_count,
                "sequence_regression": (
                    scheduler.chainlink_sequence_regression_count
                ),
                "sequence_identity_mismatch": (
                    scheduler.chainlink_sequence_identity_mismatch_count
                ),
                "publisher_epoch_change": (
                    scheduler.chainlink_publisher_epoch_change_count
                ),
                "sequence_metadata_loss": (
                    scheduler.chainlink_sequence_metadata_loss_count
                ),
                "sequence_confirmation_timeout": (
                    scheduler.chainlink_sequence_confirmation_timeout_count
                ),
            }
            matured = scheduler.observe(observation, chainlink=chainlink)
            if scheduler.observation_gap_count != prior_gap_count:
                LOGGER.warning(
                    "shadow_signal_2s_evaluation_observation_gap",
                    extra={
                        "event": "shadow_signal_2s_evaluation_observation_gap",
                        "generated_ms": observation.generated_ms,
                        "observation_gap_count": (
                            scheduler.observation_gap_count
                        ),
                    },
                )
            discontinuities = (
                (
                    "sequence_gap",
                    "shadow_signal_2s_evaluation_chainlink_sequence_gap",
                    scheduler.chainlink_sequence_gap_count,
                ),
                (
                    "sequence_regression",
                    (
                        "shadow_signal_2s_evaluation_chainlink_"
                        "sequence_regression"
                    ),
                    scheduler.chainlink_sequence_regression_count,
                ),
                (
                    "sequence_identity_mismatch",
                    (
                        "shadow_signal_2s_evaluation_chainlink_"
                        "sequence_identity_mismatch"
                    ),
                    scheduler.chainlink_sequence_identity_mismatch_count,
                ),
                (
                    "publisher_epoch_change",
                    (
                        "shadow_signal_2s_evaluation_chainlink_"
                        "publisher_epoch_change"
                    ),
                    scheduler.chainlink_publisher_epoch_change_count,
                ),
                (
                    "sequence_metadata_loss",
                    (
                        "shadow_signal_2s_evaluation_chainlink_"
                        "sequence_metadata_loss"
                    ),
                    scheduler.chainlink_sequence_metadata_loss_count,
                ),
                (
                    "sequence_confirmation_timeout",
                    (
                        "shadow_signal_2s_evaluation_chainlink_"
                        "sequence_confirmation_timeout"
                    ),
                    scheduler.chainlink_sequence_confirmation_timeout_count,
                ),
            )
            for reason, event, count in discontinuities:
                if count == prior_discontinuity_counts[reason]:
                    continue
                LOGGER.warning(
                    event,
                    extra={
                        "event": event,
                        "reason": reason,
                        "generated_ms": observation.generated_ms,
                        "occurrence": count,
                    },
                )
            for record in matured:
                writer.offer_cohort_nowait((record,))
        except Exception:
            # Persistence is subordinate to the expiring Redis publication.
            # Scheduler and queue faults must never kill the live path.
            LOGGER.exception(
                "shadow_signal_2s_evaluation_tick_failed",
                extra={"event": "shadow_signal_2s_evaluation_tick_failed"},
            )

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
    evaluation_backend_factory: Optional[EvaluationBackendFactory] = None,
    evaluation_scheduler_factory: Optional[
        EvaluationSchedulerFactory
    ] = None,
    evaluation_writer_factory: Optional[EvaluationWriterFactory] = None,
    max_iterations: Optional[int] = None,
) -> None:
    setup_logging(settings.LOG_LEVEL)
    if not settings.SHADOW_SIGNAL_2S_ENABLED:
        LOGGER.info(
            "shadow_signal_2s_disabled",
            extra={"event": "shadow_signal_2s_disabled"},
        )
        return

    # Validate the Git-tracked prospective registration before touching Redis
    # or constructing the lazy PostgreSQL backend.
    registration = load_shadow_signal_2s_registration()
    evaluation_enabled = settings.SHADOW_SIGNAL_2S_EVALUATION_ENABLED
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
            "evaluation_enabled": evaluation_enabled,
            "registration_artifact_sha256": (
                registration.artifact_sha256
            ),
            "registration_fingerprint_sha256": (
                registration.fingerprint_sha256
            ),
            "registration_policy_version": (
                registration.provenance.policy_version
            ),
            "registration_evidence_end_ms": (
                registration.provenance.evidence_end_ms
            ),
        },
    )

    live_cache: Optional[LiveCache] = None
    signal_store: Optional[ShadowSignal2sStore] = None
    evaluation_writer: Optional[ShadowEvaluationWriterRuntime] = None
    try:
        live_cache = live_cache_factory(settings)
        signal_store = signal_store_factory(settings)
        evaluation_scheduler: Optional[ShadowEvaluationScheduler] = None
        if evaluation_enabled:
            scheduler_factory = (
                evaluation_scheduler_factory or ShadowEvaluationScheduler
            )
            writer_factory = (
                evaluation_writer_factory or ShadowEvaluationWriterRuntime
            )
            candidate_model_versions = (
                SHADOW_SIGNAL_2S_MODEL.version,
            )
            if evaluation_backend_factory is None:
                database_url = settings.DATABASE_URL
                connect_timeout_seconds = (
                    settings.SHADOW_SIGNAL_2S_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS
                )
                command_timeout_seconds = (
                    settings.SHADOW_SIGNAL_2S_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS
                )

                async def evaluation_backend_factory() -> Any:
                    return await create_shadow_evaluation_backend(
                        database_url,
                        model_versions=candidate_model_versions,
                        connect_timeout_seconds=connect_timeout_seconds,
                        command_timeout_seconds=command_timeout_seconds,
                    )

            evaluation_scheduler = scheduler_factory(
                models=(SHADOW_SIGNAL_2S_MODEL,),
                provenance=registration.provenance,
                cadence_ms=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_INTERVAL_MS
                ),
                max_observation_gap_ms=(
                    settings.SHADOW_SIGNAL_2S_POLL_MS * 2
                ),
            )
            evaluation_writer = writer_factory(
                backend_factory=evaluation_backend_factory,
                candidate_model_versions=candidate_model_versions,
                queue_max_records=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_QUEUE_MAX
                ),
                batch_max_rows=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_BATCH_MAX_ROWS
                ),
                flush_ms=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_FLUSH_MS
                ),
                retry_ms=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_RETRY_MS
                ),
                shutdown_timeout_ms=int(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS
                    * 1_000
                ),
                retention_ms=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_HOURS
                    * 60
                    * 60
                    * 1_000
                ),
                cleanup_interval_ms=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_CHECK_SECONDS
                    * 1_000
                ),
                cleanup_batch_rows=(
                    settings.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_BATCH_ROWS
                ),
            )
            evaluation_writer.start()
            LOGGER.info(
                "shadow_signal_2s_evaluation_started",
                extra={
                    "event": "shadow_signal_2s_evaluation_started",
                    "cadence_ms": (
                        settings.SHADOW_SIGNAL_2S_EVALUATION_INTERVAL_MS
                    ),
                    "queue_max_records": (
                        settings.SHADOW_SIGNAL_2S_EVALUATION_QUEUE_MAX
                    ),
                    "batch_max_rows": (
                        settings.SHADOW_SIGNAL_2S_EVALUATION_BATCH_MAX_ROWS
                    ),
                    "retention_hours": (
                        settings.SHADOW_SIGNAL_2S_EVALUATION_RETENTION_HOURS
                    ),
                },
            )
        worker = ShadowSignal2sWorker(
            live_cache=live_cache,
            signal_store=signal_store,
            poll_ms=settings.SHADOW_SIGNAL_2S_POLL_MS,
            ttl_ms=settings.SHADOW_SIGNAL_2S_TTL_MS,
            evaluation_scheduler=evaluation_scheduler,
            evaluation_writer=evaluation_writer,
        )
        await worker.run(max_iterations=max_iterations)
    finally:
        try:
            if evaluation_writer is not None:
                try:
                    await evaluation_writer.close()
                except Exception:
                    LOGGER.exception(
                        "shadow_signal_2s_evaluation_shutdown_failed",
                        extra={
                            "event": (
                                "shadow_signal_2s_evaluation_shutdown_failed"
                            )
                        },
                    )
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
