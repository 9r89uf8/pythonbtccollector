from __future__ import annotations

"""Standalone live publisher and noncritical shadow-evaluation worker."""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, DecimalException
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from price_collector.config import Settings
from price_collector.db import create_shadow_evaluation_backend
from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LIVE_CACHE_READ_ERRORS,
    LIVE_CACHE_WRITE_ERRORS,
    LiveCache,
    LiveCachePayloadError,
    LivePrice,
    LiveShadowSignal,
    create_live_cache,
)
from price_collector.shadow_signal import (
    EngineObservation,
    ObservedPrice,
    ShadowSignalEngine,
)
from price_collector.shadow_signal_artifact import (
    ActivatedShadowSelection,
    load_activated_selection,
)
from price_collector.shadow_signal_evaluation import (
    ShadowEvaluationProvenance,
    ShadowEvaluationScheduler,
    ShadowEvaluationWriterRuntime,
)


LOGGER = logging.getLogger("price_collector.shadow_signal_collector")

NowMs = Callable[[], int]
Sleep = Callable[[float], Awaitable[None]]
EvaluationBackendFactory = Callable[[], Any]
EvaluationSchedulerFactory = Callable[..., ShadowEvaluationScheduler]
EvaluationWriterFactory = Callable[..., ShadowEvaluationWriterRuntime]


class _JsonLogFormatter(logging.Formatter):
    """Emit standalone shadow-worker logs as structured JSON."""

    _standard_attrs = set(
        vars(logging.LogRecord("", 0, "", 0, "", (), None))
    )

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
        value = Decimal(price.value)
        return ObservedPrice(
            value=value,
            source_timestamp_ms=price.source_timestamp_ms,
            received_ms=price.received_ms,
            publisher_epoch=price.publisher_epoch,
            accepted_event_sequence=price.accepted_event_sequence,
        )
    except (DecimalException, TypeError, ValueError) as exc:
        raise LiveCachePayloadError(
            f"{key} contains an invalid price value"
        ) from exc


def build_live_shadow_signal(
    *,
    activated: ActivatedShadowSelection,
    observation: EngineObservation,
) -> LiveShadowSignal:
    model = activated.primary_model
    signal = observation.signal_for(model.version)
    projection = signal.projection if signal.valid else None
    anchor = signal.anchor if signal.valid else None
    futures_now = signal.futures_now
    chainlink_now = signal.chainlink_now
    reference = anchor.futures_reference if anchor is not None else None

    return LiveShadowSignal(
        schema_version=1,
        mode="shadow",
        selection_schema_version=activated.selection_schema_version,
        selection_policy_version=activated.policy_version,
        selection_fingerprint_sha256=(
            activated.selection_fingerprint_sha256
        ),
        selection_artifact_sha256=activated.selection_artifact_sha256,
        selection_evidence_end_ms=activated.evidence_end_ms,
        model_version=model.version,
        beta=model.beta,
        generated_ms=observation.generated_ms,
        valid=signal.valid,
        status=signal.status,
        invalid_reasons=signal.invalid_reasons,
        state=signal.state,
        horizon_ms=model.lag_ms,
        estimated_lag_ms=model.lag_ms,
        current_chainlink=(
            chainlink_now.value if chainlink_now is not None else None
        ),
        projected_chainlink=(
            projection.projected_chainlink
            if projection is not None
            else None
        ),
        pending_move=(
            projection.pending_move if projection is not None else None
        ),
        pending_move_bps=(
            projection.pending_move_bps if projection is not None else None
        ),
        direction=projection.direction if projection is not None else None,
        futures_now=futures_now.value if futures_now is not None else None,
        futures_reference=reference.value if reference is not None else None,
        chainlink_now_source_timestamp_ms=(
            chainlink_now.source_timestamp_ms
            if chainlink_now is not None
            else None
        ),
        chainlink_now_received_ms=(
            chainlink_now.received_ms if chainlink_now is not None else None
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


class ShadowSignalWorker:
    def __init__(
        self,
        *,
        live_cache: LiveCache,
        activated: ActivatedShadowSelection,
        ttl_ms: int,
        evaluation_scheduler: Optional[ShadowEvaluationScheduler] = None,
        evaluation_writer: Optional[ShadowEvaluationWriterRuntime] = None,
        now_ms: NowMs = current_utc_epoch_ms,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int):
            raise TypeError("ttl_ms must be an integer")
        if ttl_ms <= activated.poll_ms:
            raise ValueError("ttl_ms must exceed the poll interval")
        if (evaluation_scheduler is None) != (evaluation_writer is None):
            raise ValueError(
                "evaluation_scheduler and evaluation_writer must be provided "
                "together"
            )
        self.live_cache = live_cache
        self.activated = activated
        self.ttl_ms = ttl_ms
        self.evaluation_scheduler = evaluation_scheduler
        self.evaluation_writer = evaluation_writer
        self.now_ms = now_ms
        self.sleep = sleep
        self.engine = ShadowSignalEngine(
            models=activated.models,
            futures_stale_ms=activated.futures_stale_ms,
            chainlink_stale_ms=activated.chainlink_stale_ms,
            reference_max_gap_ms=activated.reference_max_gap_ms,
            history_retention_ms=activated.history_retention_ms,
            max_future_skew_ms=activated.max_future_skew_ms,
        )
        self._last_logged_state: Optional[tuple[bool, str, str]] = None

    async def run_once(self, *, now_ms: Optional[int] = None) -> Optional[LiveShadowSignal]:
        try:
            prices, payload_errors = await self.live_cache.get_prices_independent(
                [FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]
            )
        except LIVE_CACHE_READ_ERRORS:
            LOGGER.exception(
                "shadow_signal_cache_read_failed",
                extra={"event": "shadow_signal_cache_read_failed"},
            )
            return None

        # A forecast cannot exist before its Redis inputs are available. Stamp
        # immediately after MGET so target_ms is based on causal availability,
        # not on the time just before an awaited network operation.
        generated_ms = self.now_ms() if now_ms is None else now_ms

        for key, error in payload_errors.items():
            LOGGER.warning(
                "shadow_signal_input_payload_invalid",
                extra={
                    "event": "shadow_signal_input_payload_invalid",
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
                    "shadow_signal_input_price_invalid",
                    extra={
                        "event": "shadow_signal_input_price_invalid",
                        "redis_key": key,
                        "error": str(error),
                    },
                )

        observation = self.engine.observe(
            futures=observed[FUTURES_LIVE_KEY],
            chainlink=observed[CHAINLINK_LIVE_KEY],
            now_ms=generated_ms,
        )
        payload = build_live_shadow_signal(
            activated=self.activated,
            observation=observation,
        )
        self._log_state_transition(payload)
        try:
            await self.live_cache.set_shadow_signal(
                payload,
                ttl_ms=self.ttl_ms,
            )
        except LIVE_CACHE_WRITE_ERRORS:
            LOGGER.exception(
                "shadow_signal_cache_write_failed",
                extra={"event": "shadow_signal_cache_write_failed"},
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
            matured = scheduler.observe(
                observation,
                chainlink=chainlink,
            )
            if scheduler.observation_gap_count != prior_gap_count:
                LOGGER.warning(
                    "shadow_signal_evaluation_observation_gap",
                    extra={
                        "event": "shadow_signal_evaluation_observation_gap",
                        "generated_ms": observation.generated_ms,
                        "observation_gap_count": (
                            scheduler.observation_gap_count
                        ),
                    },
                )
            discontinuities = (
                (
                    "sequence_gap",
                    "shadow_signal_evaluation_chainlink_sequence_gap",
                    scheduler.chainlink_sequence_gap_count,
                ),
                (
                    "sequence_regression",
                    "shadow_signal_evaluation_chainlink_sequence_regression",
                    scheduler.chainlink_sequence_regression_count,
                ),
                (
                    "publisher_epoch_change",
                    "shadow_signal_evaluation_chainlink_publisher_epoch_change",
                    scheduler.chainlink_publisher_epoch_change_count,
                ),
                (
                    "sequence_metadata_loss",
                    "shadow_signal_evaluation_chainlink_sequence_metadata_loss",
                    scheduler.chainlink_sequence_metadata_loss_count,
                ),
                (
                    "sequence_confirmation_timeout",
                    (
                        "shadow_signal_evaluation_chainlink_"
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
            candidate_count = len(self.activated.models)
            if len(matured) % candidate_count:
                raise RuntimeError(
                    "evaluation scheduler emitted an incomplete candidate cohort"
                )
            for offset in range(0, len(matured), candidate_count):
                writer.offer_cohort_nowait(
                    matured[offset : offset + candidate_count]
                )
        except Exception:
            # Evaluation is deliberately subordinate to the expiring Redis
            # publication. A scheduler or queue defect must not kill that path.
            LOGGER.exception(
                "shadow_signal_evaluation_tick_failed",
                extra={"event": "shadow_signal_evaluation_tick_failed"},
            )

    def _log_state_transition(self, payload: LiveShadowSignal) -> None:
        state = (payload.valid, payload.status, payload.state)
        if state == self._last_logged_state:
            return
        self._last_logged_state = state
        LOGGER.info(
            "shadow_signal_state_changed",
            extra={
                "event": "shadow_signal_state_changed",
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
                self.activated.poll_ms,
            )
            await self.sleep(delay_ms / 1_000)
            await self.run_once()
            iterations += 1


async def run_collector(
    settings: Settings,
    *,
    live_cache_factory: Callable[[Any], LiveCache] = create_live_cache,
    evaluation_backend_factory: Optional[EvaluationBackendFactory] = None,
    evaluation_scheduler_factory: Optional[
        EvaluationSchedulerFactory
    ] = None,
    evaluation_writer_factory: Optional[EvaluationWriterFactory] = None,
    max_iterations: Optional[int] = None,
) -> None:
    setup_logging(settings.LOG_LEVEL)
    if not settings.SHADOW_SIGNAL_ENABLED:
        LOGGER.info(
            "shadow_signal_disabled",
            extra={"event": "shadow_signal_disabled"},
        )
        return

    activated = load_activated_selection(
        Path(settings.SHADOW_SIGNAL_SELECTION_PATH),
        expected_selection_sha256=settings.SHADOW_SIGNAL_SELECTION_SHA256,
        replay_configuration_report_path=Path(
            settings.SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH
        ),
        trusted_directory=Path(settings.SHADOW_SIGNAL_TRUSTED_DECISION_DIR),
    )
    if activated.poll_ms != settings.SHADOW_SIGNAL_POLL_MS:
        raise RuntimeError(
            "configured shadow poll interval does not match replay evidence"
        )
    evaluation_enabled = bool(
        getattr(settings, "SHADOW_SIGNAL_EVALUATION_ENABLED", False)
    )

    LOGGER.info(
        "shadow_signal_starting",
        extra={
            "event": "shadow_signal_starting",
            "app_env": settings.APP_ENV,
            "model_version": activated.primary_model.version,
            "horizon_ms": activated.primary_model.lag_ms,
            "beta": str(activated.primary_model.beta),
            "poll_ms": activated.poll_ms,
            "ttl_ms": settings.SHADOW_SIGNAL_TTL_MS,
            "futures_stale_ms": activated.futures_stale_ms,
            "chainlink_stale_ms": activated.chainlink_stale_ms,
            "reference_max_gap_ms": activated.reference_max_gap_ms,
            "history_retention_ms": activated.history_retention_ms,
            "selection_artifact_sha256": (
                activated.selection_artifact_sha256
            ),
            "selection_fingerprint_sha256": (
                activated.selection_fingerprint_sha256
            ),
            "selection_evidence_end_ms": activated.evidence_end_ms,
            "evaluation_enabled": evaluation_enabled,
        },
    )

    live_cache = live_cache_factory(settings)
    evaluation_writer: Optional[ShadowEvaluationWriterRuntime] = None
    try:
        evaluation_scheduler: Optional[ShadowEvaluationScheduler] = None
        if evaluation_enabled:
            scheduler_factory = (
                evaluation_scheduler_factory or ShadowEvaluationScheduler
            )
            writer_factory = (
                evaluation_writer_factory or ShadowEvaluationWriterRuntime
            )
            if evaluation_backend_factory is None:
                database_url = settings.DATABASE_URL

                async def evaluation_backend_factory() -> Any:
                    return await create_shadow_evaluation_backend(
                        database_url,
                        connect_timeout_seconds=(
                            settings.SHADOW_SIGNAL_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS
                        ),
                        command_timeout_seconds=(
                            settings.SHADOW_SIGNAL_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS
                        ),
                    )

            evaluation_scheduler = scheduler_factory(
                models=activated.models,
                provenance=ShadowEvaluationProvenance(
                    selection_schema_version=(
                        activated.selection_schema_version
                    ),
                    policy_version=activated.policy_version,
                    selection_fingerprint_sha256=(
                        activated.selection_fingerprint_sha256
                    ),
                    selection_artifact_sha256=(
                        activated.selection_artifact_sha256
                    ),
                    evidence_end_ms=activated.evidence_end_ms,
                ),
                cadence_ms=(
                    settings.SHADOW_SIGNAL_EVALUATION_INTERVAL_MS
                ),
                max_observation_gap_ms=activated.poll_ms * 2,
            )
            evaluation_writer = writer_factory(
                backend_factory=evaluation_backend_factory,
                candidate_model_versions=tuple(
                    model.version for model in activated.models
                ),
                queue_max_records=(
                    settings.SHADOW_SIGNAL_EVALUATION_QUEUE_MAX
                ),
                batch_max_rows=(
                    settings.SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS
                ),
                flush_ms=settings.SHADOW_SIGNAL_EVALUATION_FLUSH_MS,
                retry_ms=settings.SHADOW_SIGNAL_EVALUATION_RETRY_MS,
                shutdown_timeout_ms=int(
                    settings.SHADOW_SIGNAL_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS
                    * 1_000
                ),
                retention_ms=(
                    settings.SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS
                    * 60
                    * 60
                    * 1_000
                ),
                cleanup_interval_ms=(
                    settings.SHADOW_SIGNAL_EVALUATION_RETENTION_CHECK_SECONDS
                    * 1_000
                ),
                cleanup_batch_rows=(
                    settings.SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS
                ),
            )
            evaluation_writer.start()
            LOGGER.info(
                "shadow_signal_evaluation_started",
                extra={
                    "event": "shadow_signal_evaluation_started",
                    "cadence_ms": (
                        settings.SHADOW_SIGNAL_EVALUATION_INTERVAL_MS
                    ),
                    "queue_max_records": (
                        settings.SHADOW_SIGNAL_EVALUATION_QUEUE_MAX
                    ),
                    "batch_max_rows": (
                        settings.SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS
                    ),
                    "retention_hours": (
                        settings.SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS
                    ),
                },
            )
        worker = ShadowSignalWorker(
            live_cache=live_cache,
            activated=activated,
            ttl_ms=settings.SHADOW_SIGNAL_TTL_MS,
            evaluation_scheduler=evaluation_scheduler,
            evaluation_writer=evaluation_writer,
        )
        await worker.run(max_iterations=max_iterations)
    finally:
        if evaluation_writer is not None:
            try:
                await evaluation_writer.close()
            except Exception:
                LOGGER.exception(
                    "shadow_signal_evaluation_shutdown_failed",
                    extra={
                        "event": "shadow_signal_evaluation_shutdown_failed"
                    },
                )
        await live_cache.close()


def main() -> None:
    settings = Settings()
    try:
        asyncio.run(run_collector(settings))
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
