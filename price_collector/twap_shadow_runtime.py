"""Noncritical live runtime for the experimental Chainlink TWAP shadow."""

import asyncio
import heapq
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from price_collector.collector import current_utc_epoch_ms, reconnect_delay_seconds
from price_collector.db import (
    delete_expired_twap_shadow_predictions,
    fetch_twap_shadow_preload,
    persist_twap_shadow_prediction_batch,
)
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LIVE_CACHE_READ_ERRORS,
    LIVE_CACHE_WRITE_ERRORS,
    TWAP_LIVE_KEY,
    TWAP_SHADOW_LIVE_KEY,
    LiveCachePayloadError,
    LivePrice,
)
from price_collector.twap_shadow import (
    BASIS_WINDOW_MS,
    DEFAULT_STALE_AFTER_MS,
    PRELOAD_SAFETY_MS,
    SOURCE_BINANCE_SPOT,
    SOURCE_CHAINLINK_SPOT,
    SOURCE_FUTURES,
    ActualTwapObservation,
    ForecastBatch,
    SourceObservation,
    TwapShadowModel,
)


LOGGER = logging.getLogger("price_collector.twap_shadow_runtime")

# Retain the full modeled window plus margin for source endpoint delays before
# the earliest replayed actual. This moves with the versioned TWAP window.
SHADOW_PRELOAD_SAFETY_MS = PRELOAD_SAFETY_MS
SHADOW_PRELOAD_LOOKBACK_MS = BASIS_WINDOW_MS + SHADOW_PRELOAD_SAFETY_MS
SHADOW_MIN_BASIS_SAMPLES = 60
SHADOW_RETENTION_CHECK_SECONDS = 60.0
SHADOW_RETENTION_DELETE_BATCH_SIZE = 5_000
SHADOW_ACTUAL_QUEUE_MAX_EVENTS = 10_000
SHADOW_WARNING_INTERVAL_NS = 60_000_000_000
SHADOW_PRELOAD_YIELD_EVERY_EVENTS = 16
SHADOW_STARTUP_CATCHUP_MAX_PASSES = 3
SHADOW_STARTUP_CATCHUP_TARGET_MS = 250
SHADOW_STARTUP_CATCHUP_MAX_LAG_MS = 1_000

SOURCE_LIVE_KEYS = {
    SOURCE_FUTURES: FUTURES_LIVE_KEY,
    SOURCE_CHAINLINK_SPOT: CHAINLINK_LIVE_KEY,
    SOURCE_BINANCE_SPOT: BINANCE_SPOT_LIVE_KEY,
}
INPUT_LIVE_KEYS = (
    FUTURES_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    BINANCE_SPOT_LIVE_KEY,
    TWAP_LIVE_KEY,
)
SHADOW_LIVE_READ_ERRORS = LIVE_CACHE_READ_ERRORS + (LiveCachePayloadError,)


@dataclass(frozen=True)
class ShadowPersistenceForecast:
    """Only the horizon fields required by the durable writer."""

    horizon_seconds: int
    target_second_ms: int
    value: Decimal
    known_fraction: Decimal
    source_count: int
    estimated_error_bps: Optional[Decimal]


@dataclass(frozen=True)
class ShadowPersistenceBatch:
    """Compact queue item that avoids retaining the full model graph."""

    model_version: int
    origin_second_ms: int
    generated_ms: int
    forecasts: tuple[ShadowPersistenceForecast, ...]

    @classmethod
    def from_forecast_batch(cls, batch: ForecastBatch) -> "ShadowPersistenceBatch":
        forecasts = tuple(
            ShadowPersistenceForecast(
                horizon_seconds=forecast.horizon_seconds,
                target_second_ms=forecast.target_second_ms,
                value=forecast.value,
                known_fraction=forecast.known_fraction,
                source_count=forecast.source_count,
                estimated_error_bps=forecast.estimated_error_bps,
            )
            for forecast in batch.forecasts
            if forecast.value is not None and forecast.known_fraction is not None
        )
        return cls(
            model_version=batch.model_version,
            origin_second_ms=batch.origin_second_ms,
            generated_ms=batch.generated_ms,
            forecasts=forecasts,
        )


class TwapShadowActualSink:
    """Bounded, synchronous handoff from the authoritative TWAP reader."""

    def __init__(self, *, max_events: int = SHADOW_ACTUAL_QUEUE_MAX_EVENTS) -> None:
        if isinstance(max_events, bool) or not isinstance(max_events, int):
            raise TypeError("max_events must be an integer")
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        self.queue: "asyncio.Queue[ActualTwapObservation]" = asyncio.Queue(
            maxsize=max_events
        )
        self.dropped_total = 0
        self._last_drop_warning_ns: Optional[int] = None

    def offer_event(self, event: Any) -> None:
        """Offer after the exact TWAP Redis/durable-queue handoff completes."""

        observation = ActualTwapObservation(
            price=event.price,
            provider_event_ms=int(event.sample_second_ms),
            received_ms=int(event.received_wall_ns) // 1_000_000,
            sequence=int(event.receive_sequence),
        )
        if self.queue.full():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.dropped_total += 1
            now_ns = time.monotonic_ns()
            if (
                self._last_drop_warning_ns is None
                or now_ns - self._last_drop_warning_ns
                >= SHADOW_WARNING_INTERVAL_NS
            ):
                self._last_drop_warning_ns = now_ns
                LOGGER.warning(
                    "twap_shadow_actual_queue_dropped_oldest",
                    extra={
                        "event": "twap_shadow_actual_queue_dropped_oldest",
                        "dropped_total": self.dropped_total,
                        "queue_max_events": self.queue.maxsize,
                    },
                )
        self.queue.put_nowait(observation)


class _ShadowModelRuntimeState:
    """Deduplicate latest-wins cache polls before they enter the model."""

    def __init__(self, model: TwapShadowModel) -> None:
        self.model = model
        self.source_sequence = 0
        self.actual_sequence = 0
        self.last_source_identity: dict[str, tuple[Any, ...]] = {}
        self.actual_identities: "OrderedDict[int, tuple[Any, ...]]" = OrderedDict()

    def observe_source(
        self,
        *,
        source: str,
        price: Decimal,
        source_ms: int,
        received_ms: int,
        sequence: Optional[int] = None,
    ) -> bool:
        identity = (price, source_ms, received_ms)
        if self.last_source_identity.get(source) == identity:
            return False
        if sequence is None:
            self.source_sequence += 1
            sequence = self.source_sequence
        else:
            self.source_sequence = max(self.source_sequence, sequence)
        self.model.observe_source(
            SourceObservation(
                source=source,
                price=price,
                source_ms=source_ms,
                received_ms=received_ms,
                sequence=sequence,
            )
        )
        self.last_source_identity[source] = identity
        return True

    def observe_actual(
        self,
        observation: ActualTwapObservation,
        *,
        record_nowcast_error: bool = True,
    ) -> bool:
        identity = (
            observation.price,
            observation.received_ms,
        )
        provider_ms = observation.provider_event_ms
        if self.actual_identities.get(provider_ms) == identity:
            return False
        if record_nowcast_error:
            self.model.observe_actual(observation)
        else:
            self.model.observe_actual(
                observation,
                record_nowcast_error=False,
            )
        self.actual_identities[provider_ms] = identity
        self.actual_identities.move_to_end(provider_ms)
        while len(self.actual_identities) > 4_000:
            self.actual_identities.popitem(last=False)
        self.actual_sequence = max(self.actual_sequence, observation.sequence)
        return True


def create_twap_shadow_actual_sink(settings: Any) -> TwapShadowActualSink:
    _ = settings
    return TwapShadowActualSink(max_events=SHADOW_ACTUAL_QUEUE_MAX_EVENTS)


def _decimal_price(value: Any, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        price = value
    elif isinstance(value, str):
        try:
            price = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} is not a decimal price") from exc
    else:
        raise TypeError(f"{field_name} must be Decimal or a decimal string")
    if not price.is_finite() or price <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return price


def _actual_from_live_price(
    price: LivePrice,
    *,
    sequence: int,
) -> Optional[ActualTwapObservation]:
    if price.source_timestamp_ms is None:
        return None
    try:
        return ActualTwapObservation(
            price=_decimal_price(price.value, "TWAP live price"),
            provider_event_ms=(int(price.source_timestamp_ms) // 1_000) * 1_000,
            received_ms=int(price.received_ms),
            sequence=sequence,
        )
    except (TypeError, ValueError):
        LOGGER.warning(
            "twap_shadow_live_actual_skipped",
            extra={
                "event": "twap_shadow_live_actual_skipped",
                "provider_event_ms": price.source_timestamp_ms,
                "received_ms": price.received_ms,
            },
        )
        return None


async def _replay_twap_shadow_preload(
    state: _ShadowModelRuntimeState,
    rows: Mapping[str, Any],
) -> Mapping[str, int]:
    """Replay calibration history in cancellable event-loop-sized chunks."""

    source_rows = sorted(
        rows["sources"],
        key=lambda row: (
            int(row["received_ms"]),
            str(row["source"]),
            int(row["source_ms"]),
        ),
    )
    actual_rows = sorted(
        rows["actuals"],
        key=lambda row: (
            int(row["received_ms"]),
            int(row["provider_event_ms"]),
            int(row.get("receive_sequence", 0)),
        ),
    )
    source_timeline = (
        (int(row["received_ms"]), 0, sequence, row)
        for sequence, row in enumerate(source_rows, start=1)
    )
    actual_timeline = (
        (int(row["received_ms"]), 1, sequence, row)
        for sequence, row in enumerate(actual_rows, start=1)
    )

    source_count = 0
    actual_count = 0
    timeline = heapq.merge(source_timeline, actual_timeline)
    for processed, (_received_ms, kind, sequence, row) in enumerate(
        timeline,
        start=1,
    ):
        if kind == 0:
            try:
                if state.observe_source(
                    source=str(row["source"]),
                    price=_decimal_price(
                        row["price"],
                        "preload source price",
                    ),
                    source_ms=int(row["source_ms"]),
                    received_ms=int(row["received_ms"]),
                    sequence=sequence,
                ):
                    source_count += 1
            except (TypeError, ValueError) as exc:
                LOGGER.warning(
                    "twap_shadow_preload_source_skipped",
                    extra={
                        "event": "twap_shadow_preload_source_skipped",
                        "source": row.get("source"),
                        "error": repr(exc),
                    },
                )
        else:
            try:
                observation = ActualTwapObservation(
                    price=_decimal_price(
                        row["price"],
                        "preload actual price",
                    ),
                    provider_event_ms=int(row["provider_event_ms"]),
                    received_ms=int(row["received_ms"]),
                    sequence=sequence,
                )
                # Historical nowcast-error reconstruction repeatedly sorts
                # the growing basis and is diagnostic only. Rebuild the
                # prediction-critical basis now; live actuals repopulate the
                # recent error metric causally after restart.
                if state.observe_actual(
                    observation,
                    record_nowcast_error=False,
                ):
                    actual_count += 1
            except (TypeError, ValueError) as exc:
                LOGGER.warning(
                    "twap_shadow_preload_actual_skipped",
                    extra={
                        "event": "twap_shadow_preload_actual_skipped",
                        "provider_event_ms": row.get("provider_event_ms"),
                        "error": repr(exc),
                    },
                )

        if processed % SHADOW_PRELOAD_YIELD_EVERY_EVENTS == 0:
            await asyncio.sleep(0)

    return {"sources": source_count, "actuals": actual_count}


async def preload_twap_shadow_model(
    *,
    pool: Any,
    state: _ShadowModelRuntimeState,
    cutoff_received_ms: int,
) -> Mapping[str, int]:
    """Causally replay the retained 30-minute calibration window."""

    rows = await fetch_twap_shadow_preload(
        pool,
        cutoff_received_ms=cutoff_received_ms,
        lookback_ms=SHADOW_PRELOAD_LOOKBACK_MS,
    )
    return await _replay_twap_shadow_preload(state, rows)


async def catch_up_twap_shadow_model(
    *,
    pool: Any,
    state: _ShadowModelRuntimeState,
    starting_cutoff_received_ms: int,
) -> Mapping[str, int]:
    """Close the source-history interval that elapsed during initial replay."""

    cutoff_ms = starting_cutoff_received_ms
    total_sources = 0
    total_actuals = 0
    passes = 0
    remaining_lag_ms = max(0, current_utc_epoch_ms() - cutoff_ms)
    for _attempt in range(SHADOW_STARTUP_CATCHUP_MAX_PASSES):
        target_cutoff_ms = current_utc_epoch_ms()
        if target_cutoff_ms <= cutoff_ms:
            remaining_lag_ms = 0
            break
        elapsed_ms = target_cutoff_ms - cutoff_ms
        rows = await fetch_twap_shadow_preload(
            pool,
            cutoff_received_ms=target_cutoff_ms,
            lookback_ms=(
                SHADOW_PRELOAD_SAFETY_MS
                + DEFAULT_STALE_AFTER_MS
                + elapsed_ms
            ),
        )
        counts = await _replay_twap_shadow_preload(state, rows)
        total_sources += counts["sources"]
        total_actuals += counts["actuals"]
        passes += 1
        cutoff_ms = target_cutoff_ms
        remaining_lag_ms = max(0, current_utc_epoch_ms() - cutoff_ms)
        if remaining_lag_ms <= SHADOW_STARTUP_CATCHUP_TARGET_MS:
            break

    if remaining_lag_ms > SHADOW_STARTUP_CATCHUP_MAX_LAG_MS:
        raise RuntimeError(
            "TWAP shadow startup source catch-up did not converge"
        )
    return {
        "sources": total_sources,
        "actuals": total_actuals,
        "passes": passes,
        "cutoff_received_ms": cutoff_ms,
        "remaining_lag_ms": remaining_lag_ms,
    }


def _observe_cached_sources(
    state: _ShadowModelRuntimeState,
    cached: Mapping[str, Optional[LivePrice]],
) -> None:
    for source, key in SOURCE_LIVE_KEYS.items():
        price = cached.get(key)
        if price is None or price.source_timestamp_ms is None:
            continue
        try:
            state.observe_source(
                source=source,
                price=_decimal_price(price.value, f"{source} live price"),
                source_ms=int(price.source_timestamp_ms),
                received_ms=int(price.received_ms),
            )
        except (TypeError, ValueError) as exc:
            LOGGER.warning(
                "twap_shadow_live_source_skipped",
                extra={
                    "event": "twap_shadow_live_source_skipped",
                    "source": source,
                    "error": repr(exc),
                },
            )


def _drain_actual_sink(
    state: _ShadowModelRuntimeState,
    actual_sink: TwapShadowActualSink,
) -> int:
    accepted = 0
    while True:
        try:
            observation = actual_sink.queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            if state.observe_actual(observation):
                accepted += 1
        except ValueError as exc:
            LOGGER.warning(
                "twap_shadow_actual_event_skipped",
                extra={
                    "event": "twap_shadow_actual_event_skipped",
                    "provider_event_ms": observation.provider_event_ms,
                    "received_ms": observation.received_ms,
                    "error": repr(exc),
                },
            )
    return accepted


async def twap_shadow_persistence_worker(
    *,
    pool: Any,
    batches: "asyncio.Queue[ShadowPersistenceBatch]",
) -> None:
    while True:
        batch = await batches.get()
        attempt = 0
        persisted = False
        try:
            while True:
                try:
                    await persist_twap_shadow_prediction_batch(pool, batch)
                    persisted = True
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    attempt += 1
                    delay = reconnect_delay_seconds(attempt)
                    LOGGER.exception(
                        "twap_shadow_persistence_retry_scheduled",
                        extra={
                            "event": "twap_shadow_persistence_retry_scheduled",
                            "origin_second_ms": batch.origin_second_ms,
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                    )
                    await asyncio.sleep(delay)
        finally:
            if persisted:
                batches.task_done()


async def twap_shadow_retention_worker(
    *,
    pool: Any,
    retention_days: int,
) -> None:
    while True:
        try:
            cutoff_ms = current_utc_epoch_ms() - retention_days * 86_400_000
            deleted = await delete_expired_twap_shadow_predictions(
                pool,
                cutoff_target_ms=cutoff_ms,
                batch_size=SHADOW_RETENTION_DELETE_BATCH_SIZE,
            )
            if deleted:
                LOGGER.info(
                    "twap_shadow_retention_deleted",
                    extra={
                        "event": "twap_shadow_retention_deleted",
                        "deleted_rows": deleted,
                        "cutoff_target_ms": cutoff_ms,
                    },
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception(
                "twap_shadow_retention_failed",
                extra={
                    "event": "twap_shadow_retention_failed",
                    "error": repr(exc),
                },
            )
        await asyncio.sleep(SHADOW_RETENTION_CHECK_SECONDS)


async def _shadow_prediction_loop(
    *,
    settings: Any,
    live_cache: Any,
    state: _ShadowModelRuntimeState,
    actual_sink: TwapShadowActualSink,
    batches: "asyncio.Queue[ShadowPersistenceBatch]",
) -> None:
    last_published_origin_ms: Optional[int] = None
    last_input_warning_ns: Optional[int] = None
    last_queue_warning_ns: Optional[int] = None
    persistence_dropped_total = 0
    poll_seconds = int(settings.TWAP_SHADOW_POLL_MS) / 1_000

    while True:
        try:
            cached = await live_cache.get_prices(INPUT_LIVE_KEYS)
        except SHADOW_LIVE_READ_ERRORS as exc:
            now_ns = time.monotonic_ns()
            if (
                last_input_warning_ns is None
                or now_ns - last_input_warning_ns >= SHADOW_WARNING_INTERVAL_NS
            ):
                last_input_warning_ns = now_ns
                LOGGER.warning(
                    "twap_shadow_live_inputs_unavailable",
                    extra={
                        "event": "twap_shadow_live_inputs_unavailable",
                        "error": repr(exc),
                    },
                )
            await asyncio.sleep(poll_seconds)
            continue
        last_input_warning_ns = None

        _observe_cached_sources(state, cached)
        # Source observations are inserted before exact actuals, but every
        # integration still enforces each actual's receive cutoff. This closes
        # the poll interval without admitting a later source observation.
        _drain_actual_sink(state, actual_sink)
        cached_actual = cached.get(TWAP_LIVE_KEY)
        if cached_actual is not None:
            state.actual_sequence += 1
            observation = _actual_from_live_price(
                cached_actual,
                sequence=state.actual_sequence,
            )
            if observation is not None:
                try:
                    state.observe_actual(observation)
                except ValueError:
                    pass

        generated_ms = current_utc_epoch_ms()
        origin_ms = (generated_ms // 1_000) * 1_000
        if (
            last_published_origin_ms is not None
            and origin_ms < last_published_origin_ms
        ):
            LOGGER.warning(
                "twap_shadow_clock_regression",
                extra={
                    "event": "twap_shadow_clock_regression",
                    "origin_second_ms": origin_ms,
                    "last_origin_second_ms": last_published_origin_ms,
                },
            )
        elif origin_ms != last_published_origin_ms:
            batch = state.model.forecast(
                origin_ms=origin_ms,
                issued_ms=generated_ms,
            )
            persistence_batch = ShadowPersistenceBatch.from_forecast_batch(
                batch
            )
            try:
                await live_cache.set_twap_shadow_snapshot(
                    TWAP_SHADOW_LIVE_KEY,
                    snapshot=batch.to_live_payload(),
                )
            except LIVE_CACHE_WRITE_ERRORS as exc:
                LOGGER.warning(
                    "twap_shadow_live_cache_write_failed",
                    extra={
                        "event": "twap_shadow_live_cache_write_failed",
                        "origin_second_ms": origin_ms,
                        "error": repr(exc),
                    },
                )

            if persistence_batch.forecasts:
                try:
                    batches.put_nowait(persistence_batch)
                    last_queue_warning_ns = None
                except asyncio.QueueFull:
                    # Live delivery remains available during a prolonged
                    # PostgreSQL outage. The bounded loss is explicit in logs
                    # and the historical series will show the missing target.
                    persistence_dropped_total += 1
                    now_ns = time.monotonic_ns()
                    if (
                        last_queue_warning_ns is None
                        or now_ns - last_queue_warning_ns
                        >= SHADOW_WARNING_INTERVAL_NS
                    ):
                        last_queue_warning_ns = now_ns
                        LOGGER.error(
                            "twap_shadow_persistence_queue_saturated",
                            extra={
                                "event": (
                                    "twap_shadow_persistence_queue_saturated"
                                ),
                                "origin_second_ms": origin_ms,
                                "queued_batches": batches.qsize(),
                                "queue_max_batches": batches.maxsize,
                                "dropped_total": persistence_dropped_total,
                            },
                        )
            last_published_origin_ms = origin_ms

        next_poll_ms = (
            (current_utc_epoch_ms() // int(settings.TWAP_SHADOW_POLL_MS)) + 1
        ) * int(settings.TWAP_SHADOW_POLL_MS)
        delay_ms = max(1, next_poll_ms - current_utc_epoch_ms())
        await asyncio.sleep(delay_ms / 1_000)


async def run_twap_shadow_runtime(
    settings: Any,
    pool: Any,
    *,
    live_cache: Any,
    actual_sink: TwapShadowActualSink,
) -> None:
    model = TwapShadowModel(
        stale_after_ms=DEFAULT_STALE_AFTER_MS,
        min_basis_samples=SHADOW_MIN_BASIS_SAMPLES,
    )
    state = _ShadowModelRuntimeState(model)
    cutoff_ms = current_utc_epoch_ms()
    preload_counts = await preload_twap_shadow_model(
        pool=pool,
        state=state,
        cutoff_received_ms=cutoff_ms,
    )
    catchup_counts = await catch_up_twap_shadow_model(
        pool=pool,
        state=state,
        starting_cutoff_received_ms=cutoff_ms,
    )
    LOGGER.info(
        "twap_shadow_preload_completed",
        extra={
            "event": "twap_shadow_preload_completed",
            "cutoff_received_ms": cutoff_ms,
            "source_observations": preload_counts["sources"],
            "actual_observations": preload_counts["actuals"],
            "catchup_source_observations": catchup_counts["sources"],
            "catchup_actual_observations": catchup_counts["actuals"],
            "catchup_passes": catchup_counts["passes"],
            "catchup_cutoff_received_ms": catchup_counts[
                "cutoff_received_ms"
            ],
            "catchup_remaining_lag_ms": catchup_counts[
                "remaining_lag_ms"
            ],
            "preload_quality": "coarse_1hz_futures_snapshots",
        },
    )

    batches: "asyncio.Queue[ShadowPersistenceBatch]" = asyncio.Queue(
        maxsize=int(settings.TWAP_SHADOW_PERSIST_QUEUE_MAX_BATCHES)
    )
    writer = asyncio.create_task(
        twap_shadow_persistence_worker(pool=pool, batches=batches)
    )
    retention = asyncio.create_task(
        twap_shadow_retention_worker(
            pool=pool,
            retention_days=int(settings.TWAP_SHADOW_RETENTION_DAYS),
        )
    )
    producer = asyncio.create_task(
        _shadow_prediction_loop(
            settings=settings,
            live_cache=live_cache,
            state=state,
            actual_sink=actual_sink,
            batches=batches,
        )
    )
    try:
        done, _pending = await asyncio.wait(
            {producer, writer},
            return_when=asyncio.FIRST_COMPLETED,
        )
        completed = next(iter(done))
        if completed.cancelled():
            raise asyncio.CancelledError
        exception = completed.exception()
        if exception is not None:
            raise exception
        raise RuntimeError("a critical TWAP shadow task stopped")
    finally:
        if not producer.done():
            producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
        try:
            await asyncio.wait_for(
                batches.join(),
                timeout=float(
                    settings.TWAP_SHADOW_PERSIST_SHUTDOWN_TIMEOUT_SECONDS
                ),
            )
        except asyncio.TimeoutError:
            LOGGER.error(
                "twap_shadow_persistence_shutdown_incomplete",
                extra={
                    "event": "twap_shadow_persistence_shutdown_incomplete",
                    "queued_batches": batches.qsize(),
                },
            )
        for task in (writer, retention):
            if not task.done():
                task.cancel()
        await asyncio.gather(writer, retention, return_exceptions=True)


async def run_twap_shadow_noncritical(
    settings: Any,
    pool: Any,
    *,
    live_cache: Any,
    actual_sink: TwapShadowActualSink,
) -> None:
    """Restart the experimental model without stopping exact feed collection."""

    attempt = 0
    while True:
        try:
            await run_twap_shadow_runtime(
                settings,
                pool,
                live_cache=live_cache,
                actual_sink=actual_sink,
            )
            raise RuntimeError("TWAP shadow runtime stopped unexpectedly")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.exception(
                "twap_shadow_runtime_restarting",
                extra={
                    "event": "twap_shadow_runtime_restarting",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay)


__all__ = [
    "TwapShadowActualSink",
    "create_twap_shadow_actual_sink",
    "preload_twap_shadow_model",
    "run_twap_shadow_noncritical",
    "run_twap_shadow_runtime",
    "twap_shadow_persistence_worker",
    "twap_shadow_retention_worker",
]
