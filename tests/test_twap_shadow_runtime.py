import asyncio
from contextlib import suppress
from decimal import Decimal
from types import SimpleNamespace

import pytest

import price_collector.twap_shadow_runtime as shadow_runtime
from price_collector.live_cache import (
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LivePrice,
    TWAP_LIVE_KEY,
)
from price_collector.twap_shadow import (
    BASIS_WINDOW_MS,
    DEFAULT_STALE_AFTER_MS,
    SOURCE_BINANCE_SPOT,
    SOURCE_CHAINLINK_SPOT,
    SOURCE_FUTURES,
    SOURCE_NAMES,
    SourceObservation,
    TwapShadowModel,
)


D = Decimal


def test_runtime_model_v2_definition_is_pinned() -> None:
    assert DEFAULT_STALE_AFTER_MS == 10_000
    assert shadow_runtime.SHADOW_MIN_BASIS_SAMPLES == 60
    assert shadow_runtime.SHADOW_PRELOAD_YIELD_EVERY_EVENTS == 16
    assert shadow_runtime.SHADOW_STARTUP_CATCHUP_TARGET_MS == 250
    assert shadow_runtime.SHADOW_STARTUP_CATCHUP_MAX_LAG_MS == 1_000
    assert shadow_runtime.SHADOW_PRELOAD_SAFETY_MS == 75_000
    assert _loop_settings().TWAP_SHADOW_POLL_MS == 250


class StopLoop(BaseException):
    pass


class StubBatch:
    def __init__(self, origin_second_ms, value=D("64000")):
        self.origin_second_ms = origin_second_ms
        self.generated_ms = origin_second_ms + 100
        self.model_version = 2
        self.forecasts = (
            SimpleNamespace(
                horizon_seconds=1,
                target_second_ms=origin_second_ms + 1_000,
                value=value,
                known_fraction=D("0.75000000") if value is not None else None,
                source_count=3 if value is not None else 0,
                estimated_error_bps=(
                    D("0.42000000") if value is not None else None
                ),
                # The live model includes a much richer per-source graph. It
                # must not be retained by the bounded persistence queue.
                sources=(object(), object(), object()),
                quality_flags=("live_only_detail",),
            ),
        )

    def to_live_payload(self):
        return {
            "origin_second_ms": self.origin_second_ms,
            "predictions": {"h1": {"value": str(self.forecasts[0].value)}},
        }


class RecordingModel:
    def __init__(self, *, forecast_value=D("64000")):
        self.forecast_value = forecast_value
        self.source_observations = []
        self.actual_observations = []
        self.actual_record_error_modes = []
        self.forecast_origins = []

    def observe_source(self, observation):
        self.source_observations.append(observation)

    def observe_actual(self, observation, *, record_nowcast_error=True):
        self.actual_observations.append(observation)
        self.actual_record_error_modes.append(record_nowcast_error)

    def forecast(self, *, origin_ms, issued_ms):
        del issued_ms
        self.forecast_origins.append(origin_ms)
        return StubBatch(origin_ms, self.forecast_value)


class LoopClock:
    def __init__(self):
        self.now_ms = 0

    def current_ms(self):
        return self.now_ms


class SequencedLiveCache:
    def __init__(
        self,
        *,
        clock,
        poll_times,
        cached=None,
        events=None,
        write_error=None,
    ):
        self.clock = clock
        self.poll_times = list(poll_times)
        self.cached = {} if cached is None else cached
        self.events = [] if events is None else events
        self.write_error = write_error
        self.read_count = 0
        self.snapshots = []

    async def get_prices(self, keys):
        assert tuple(keys) == shadow_runtime.INPUT_LIVE_KEYS
        if self.read_count >= len(self.poll_times):
            raise StopLoop
        self.clock.now_ms = self.poll_times[self.read_count]
        self.read_count += 1
        return self.cached

    async def set_twap_shadow_snapshot(self, key, *, snapshot):
        self.events.append(("redis", snapshot["origin_second_ms"]))
        if self.write_error is not None:
            raise self.write_error
        self.snapshots.append((key, snapshot))


class RecordingQueue(asyncio.Queue):
    def __init__(self, *, events=None, maxsize=0):
        super().__init__(maxsize=maxsize)
        self.events = [] if events is None else events
        self.task_done_calls = 0

    def put_nowait(self, item):
        self.events.append(("queue", item.origin_second_ms))
        return super().put_nowait(item)

    def task_done(self):
        self.task_done_calls += 1
        return super().task_done()


def _loop_settings():
    return SimpleNamespace(TWAP_SHADOW_POLL_MS=250)


async def _run_until_cache_exhausted(
    *, settings, live_cache, state, actual_sink, batches
):
    try:
        await shadow_runtime._shadow_prediction_loop(
            settings=settings,
            live_cache=live_cache,
            state=state,
            actual_sink=actual_sink,
            batches=batches,
        )
    except StopLoop:
        return
    raise AssertionError("shadow loop did not stop when test cache was exhausted")


def test_exact_twap_sink_converts_provider_timestamp_to_sample_second_and_is_bounded():
    async def scenario():
        sink = shadow_runtime.TwapShadowActualSink(max_events=2)
        exact_price = D("64255.113422936400000000")

        for sequence in (1, 2, 3):
            provider_event_ms = 1_786_060_800_120 + sequence
            sink.offer_event(
                SimpleNamespace(
                    price=exact_price
                    + D(sequence) / D("1000000000000000000"),
                    provider_event_ms=provider_event_ms,
                    sample_second_ms=1_786_060_800_000,
                    received_wall_ns=1_786_060_801_500_123_000 + sequence,
                    receive_sequence=sequence,
                )
            )

        assert sink.queue.qsize() == 2
        assert sink.dropped_total == 1
        second = sink.queue.get_nowait()
        third = sink.queue.get_nowait()
        assert second.provider_event_ms == 1_786_060_800_000
        assert second.received_ms == 1_786_060_801_500
        assert second.sequence == 2
        assert second.price == D("64255.113422936400000002")
        assert third.sequence == 3
        assert third.price == D("64255.113422936400000003")

    asyncio.run(scenario())


def test_preload_replay_respects_each_actual_receive_cutoff(monkeypatch):
    provider_ms = 2_000_000
    actual_received_ms = provider_ms + 500
    startup_cutoff_ms = provider_ms + 100_000
    source_rows = []
    for source in SOURCE_NAMES:
        source_rows.extend(
            (
                {
                    "source": source,
                    "price": D("100"),
                    "source_ms": (
                        provider_ms - shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
                    ),
                    "received_ms": (
                        provider_ms
                        - shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
                        + 100
                    ),
                },
                {
                    "source": source,
                    "price": D("100"),
                    "source_ms": provider_ms - 1_000,
                    "received_ms": provider_ms - 900,
                },
                # This revision exists at startup but arrived after the actual.
                # Loading it before actual replay must not leak it through the
                # model's causal received_ms cutoff.
                {
                    "source": source,
                    "price": D("1000"),
                    "source_ms": provider_ms - 10_000,
                    "received_ms": actual_received_ms + 1,
                },
            )
        )
    source_rows.sort(key=lambda row: (row["received_ms"], row["source"]))
    calls = []

    class BasisOnlyRecordingModel(TwapShadowModel):
        def __init__(self):
            super().__init__(min_basis_samples=1)
            self.record_error_modes = []

        def observe_actual(self, observation, *, record_nowcast_error=True):
            self.record_error_modes.append(record_nowcast_error)
            return super().observe_actual(
                observation,
                record_nowcast_error=record_nowcast_error,
            )

    async def fake_fetch(pool, *, cutoff_received_ms, lookback_ms):
        calls.append((pool, cutoff_received_ms, lookback_ms))
        return {
            "sources": source_rows,
            "actuals": [
                {
                    "price": D("101"),
                    "provider_event_ms": provider_ms,
                    "received_ms": actual_received_ms,
                    "receive_sequence": 17,
                }
            ],
        }

    monkeypatch.setattr(shadow_runtime, "fetch_twap_shadow_preload", fake_fetch)
    model = BasisOnlyRecordingModel()
    state = shadow_runtime._ShadowModelRuntimeState(model)
    pool = object()

    counts = asyncio.run(
        shadow_runtime.preload_twap_shadow_model(
            pool=pool,
            state=state,
            cutoff_received_ms=startup_cutoff_ms,
        )
    )

    assert calls == [
        (
            pool,
            startup_cutoff_ms,
            BASIS_WINDOW_MS + shadow_runtime.SHADOW_PRELOAD_SAFETY_MS,
        )
    ]
    assert counts == {"sources": 9, "actuals": 1}
    assert model.record_error_modes == [False]
    batch = model.forecast(
        origin_ms=provider_ms + 1_000,
        issued_ms=provider_ms + 1_100,
    )
    assert batch.basis_sample_counts == {
        SOURCE_FUTURES: 1,
        SOURCE_CHAINLINK_SPOT: 1,
        SOURCE_BINANCE_SPOT: 1,
    }
    assert batch.source_bias_bps == {
        SOURCE_FUTURES: D("100.00000000"),
        SOURCE_CHAINLINK_SPOT: D("100.00000000"),
        SOURCE_BINANCE_SPOT: D("100.00000000"),
    }
    assert batch.recent_error.sample_count == 0


def test_long_preload_replay_can_be_cancelled_before_all_rows_are_applied():
    total_rows = 1_000
    rows = {
        "sources": [
            {
                "source": SOURCE_FUTURES,
                "price": D("64000") + D(index) / D("1000"),
                "source_ms": 1_000_000 + index,
                "received_ms": 1_000_100 + index,
            }
            for index in range(total_rows)
        ],
        "actuals": [],
    }
    model = RecordingModel()
    state = shadow_runtime._ShadowModelRuntimeState(model)

    async def scenario():
        replay = asyncio.create_task(
            shadow_runtime._replay_twap_shadow_preload(state, rows)
        )
        while not model.source_observations:
            await asyncio.sleep(0)
        replay.cancel()
        with pytest.raises(asyncio.CancelledError):
            await replay

    asyncio.run(scenario())

    processed = len(model.source_observations)
    assert processed >= shadow_runtime.SHADOW_PRELOAD_YIELD_EVERY_EVENTS
    assert processed % shadow_runtime.SHADOW_PRELOAD_YIELD_EVERY_EVENTS == 0
    assert processed < total_rows


def test_startup_catch_up_covers_elapsed_time_plus_late_actual_overlap(
    monkeypatch,
):
    starting_cutoff_ms = 1_000_000
    clock_values = iter(
        (
            1_001_000,  # Initial observed lag.
            1_001_100,  # First pass target.
            1_001_500,  # Still 400 ms behind after first replay.
            1_001_600,  # Second pass target.
            1_001_800,  # Closed to 200 ms, below the 250 ms target.
        )
    )
    fetches = []

    async def fake_fetch(pool, *, cutoff_received_ms, lookback_ms):
        fetches.append((pool, cutoff_received_ms, lookback_ms))
        return {"sources": [], "actuals": []}

    monkeypatch.setattr(
        shadow_runtime,
        "current_utc_epoch_ms",
        lambda: next(clock_values),
    )
    monkeypatch.setattr(shadow_runtime, "fetch_twap_shadow_preload", fake_fetch)

    result = asyncio.run(
        shadow_runtime.catch_up_twap_shadow_model(
            pool="pool",
            state=shadow_runtime._ShadowModelRuntimeState(RecordingModel()),
            starting_cutoff_received_ms=starting_cutoff_ms,
        )
    )

    assert len(fetches) == 2
    first_elapsed_ms = 1_001_100 - starting_cutoff_ms
    second_elapsed_ms = 1_001_600 - 1_001_100
    assert fetches == [
        (
            "pool",
            1_001_100,
            shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
            + DEFAULT_STALE_AFTER_MS
            + first_elapsed_ms,
        ),
        (
            "pool",
            1_001_600,
            shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
            + DEFAULT_STALE_AFTER_MS
            + second_elapsed_ms,
        ),
    ]
    # The DB query removes the versioned preload safety margin before replaying
    # actuals. These checks pin the additional stale-source overlap on each
    # elapsed interval.
    assert (
        fetches[0][1]
        - fetches[0][2]
        + shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
        == starting_cutoff_ms - DEFAULT_STALE_AFTER_MS
    )
    assert (
        fetches[1][1]
        - fetches[1][2]
        + shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
        == fetches[0][1] - DEFAULT_STALE_AFTER_MS
    )
    assert result == {
        "sources": 0,
        "actuals": 0,
        "passes": 2,
        "cutoff_received_ms": 1_001_600,
        "remaining_lag_ms": 200,
    }
    assert result["remaining_lag_ms"] <= (
        shadow_runtime.SHADOW_STARTUP_CATCHUP_TARGET_MS
    )


def test_startup_catch_up_fails_closed_when_source_gap_does_not_converge(
    monkeypatch,
):
    clock_values = iter(
        (
            1_002_000,
            1_002_100,
            1_004_100,
            1_004_200,
            1_006_200,
            1_006_300,
            1_008_300,
        )
    )

    async def fake_fetch(_pool, *, cutoff_received_ms, lookback_ms):
        del cutoff_received_ms, lookback_ms
        return {"sources": [], "actuals": []}

    monkeypatch.setattr(
        shadow_runtime,
        "current_utc_epoch_ms",
        lambda: next(clock_values),
    )
    monkeypatch.setattr(shadow_runtime, "fetch_twap_shadow_preload", fake_fetch)

    with pytest.raises(RuntimeError, match="catch-up did not converge"):
        asyncio.run(
            shadow_runtime.catch_up_twap_shadow_model(
                pool="pool",
                state=shadow_runtime._ShadowModelRuntimeState(RecordingModel()),
                starting_cutoff_received_ms=1_000_000,
            )
        )


def test_live_loop_emits_once_per_utc_second_redis_first_and_deduplicates_cached_actual(
    monkeypatch,
):
    clock = LoopClock()
    events = []
    cached_actual = LivePrice(
        value="64255.113422936400000000",
        source_timestamp_ms=199_123,
        received_ms=200_050,
    )
    live_cache = SequencedLiveCache(
        clock=clock,
        poll_times=(200_100, 200_500, 200_999, 201_001),
        cached={TWAP_LIVE_KEY: cached_actual},
        events=events,
    )
    model = RecordingModel()
    state = shadow_runtime._ShadowModelRuntimeState(model)

    async def no_sleep(_delay):
        return None

    async def scenario():
        batches = RecordingQueue(events=events)
        await _run_until_cache_exhausted(
            settings=_loop_settings(),
            live_cache=live_cache,
            state=state,
            actual_sink=shadow_runtime.TwapShadowActualSink(max_events=10),
            batches=batches,
        )
        return batches

    monkeypatch.setattr(shadow_runtime, "current_utc_epoch_ms", clock.current_ms)
    monkeypatch.setattr(shadow_runtime.asyncio, "sleep", no_sleep)
    batches = asyncio.run(scenario())

    assert model.forecast_origins == [200_000, 201_000]
    assert events == [
        ("redis", 200_000),
        ("queue", 200_000),
        ("redis", 201_000),
        ("queue", 201_000),
    ]
    first_persistence_batch = batches.get_nowait()
    assert isinstance(
        first_persistence_batch,
        shadow_runtime.ShadowPersistenceBatch,
    )
    assert first_persistence_batch == shadow_runtime.ShadowPersistenceBatch(
        model_version=2,
        origin_second_ms=200_000,
        generated_ms=200_100,
        forecasts=(
            shadow_runtime.ShadowPersistenceForecast(
                horizon_seconds=1,
                target_second_ms=201_000,
                value=D("64000"),
                known_fraction=D("0.75000000"),
                source_count=3,
                estimated_error_bps=D("0.42000000"),
            ),
        ),
    )
    assert not hasattr(first_persistence_batch.forecasts[0], "sources")
    assert len(model.actual_observations) == 1
    assert model.actual_observations[0].provider_event_ms == 199_000
    assert model.actual_observations[0].price == D("64255.113422936400000000")


def test_live_loop_observes_cached_sources_before_accumulated_actual_without_leakage(
    monkeypatch,
):
    provider_ms = 10_000_000
    actual_received_ms = provider_ms + 500
    clock = LoopClock()

    class CausalRecordingModel(TwapShadowModel):
        def __init__(self):
            super().__init__(min_basis_samples=1)
            self.record_runtime_order = False
            self.runtime_order = []
            self.actual_updates = []

        def observe_source(self, observation):
            if self.record_runtime_order:
                self.runtime_order.append(("source", observation.source))
            return super().observe_source(observation)

        def observe_actual(self, observation, *, record_nowcast_error=True):
            if self.record_runtime_order:
                self.runtime_order.append(("actual", observation.provider_event_ms))
            update = super().observe_actual(
                observation,
                record_nowcast_error=record_nowcast_error,
            )
            self.actual_updates.append(update)
            return update

    model = CausalRecordingModel()
    for source in (SOURCE_FUTURES, SOURCE_CHAINLINK_SPOT):
        model.observe_source(
            SourceObservation(
                source=source,
                price=D("100"),
                source_ms=(
                    provider_ms - shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
                ),
                received_ms=(
                    provider_ms
                    - shadow_runtime.SHADOW_PRELOAD_SAFETY_MS
                    + 100
                ),
            )
        )
    model.record_runtime_order = True
    state = shadow_runtime._ShadowModelRuntimeState(model)
    live_cache = SequencedLiveCache(
        clock=clock,
        poll_times=(provider_ms + 1_100,),
        cached={
            # This fresh observation was known when the accumulated actual
            # arrived, so observing cached inputs first lets it calibrate.
            FUTURES_LIVE_KEY: LivePrice(
                value="100",
                source_timestamp_ms=provider_ms - 1_000,
                received_ms=actual_received_ms - 100,
            ),
            # This one was received after the actual. It enters model history
            # first too, but the actual's received cutoff must exclude it.
            CHAINLINK_LIVE_KEY: LivePrice(
                value="1000",
                source_timestamp_ms=provider_ms - 1_000,
                received_ms=actual_received_ms + 100,
            ),
        },
    )

    async def no_sleep(_delay):
        return None

    async def scenario():
        actual_sink = shadow_runtime.TwapShadowActualSink(max_events=10)
        actual_sink.offer_event(
            SimpleNamespace(
                price=D("101"),
                provider_event_ms=provider_ms + 123,
                sample_second_ms=provider_ms,
                received_wall_ns=actual_received_ms * 1_000_000,
                receive_sequence=1,
            )
        )
        await _run_until_cache_exhausted(
            settings=_loop_settings(),
            live_cache=live_cache,
            state=state,
            actual_sink=actual_sink,
            batches=asyncio.Queue(),
        )

    monkeypatch.setattr(shadow_runtime, "current_utc_epoch_ms", clock.current_ms)
    monkeypatch.setattr(shadow_runtime.asyncio, "sleep", no_sleep)
    asyncio.run(scenario())

    assert model.runtime_order[:3] == [
        ("source", SOURCE_FUTURES),
        ("source", SOURCE_CHAINLINK_SPOT),
        ("actual", provider_ms),
    ]
    assert len(model.actual_updates) == 1
    residuals = {
        update.source: update
        for update in model.actual_updates[0].source_residuals
    }
    assert residuals[SOURCE_FUTURES].stored is True
    assert residuals[SOURCE_FUTURES].residual_bps == D("100.00000000")
    assert residuals[SOURCE_CHAINLINK_SPOT].stored is False
    assert "source_stale" in residuals[SOURCE_CHAINLINK_SPOT].quality_flags


def test_full_history_queue_drops_batch_but_still_publishes_live_prediction(
    monkeypatch,
):
    clock = LoopClock()
    live_cache = SequencedLiveCache(clock=clock, poll_times=(300_100,))
    model = RecordingModel()
    sentinel = object()

    async def no_sleep(_delay):
        return None

    async def scenario():
        batches = asyncio.Queue(maxsize=1)
        batches.put_nowait(sentinel)
        await _run_until_cache_exhausted(
            settings=_loop_settings(),
            live_cache=live_cache,
            state=shadow_runtime._ShadowModelRuntimeState(model),
            actual_sink=shadow_runtime.TwapShadowActualSink(max_events=10),
            batches=batches,
        )
        return batches

    monkeypatch.setattr(shadow_runtime, "current_utc_epoch_ms", clock.current_ms)
    monkeypatch.setattr(shadow_runtime.asyncio, "sleep", no_sleep)
    batches = asyncio.run(scenario())

    assert model.forecast_origins == [300_000]
    assert live_cache.snapshots == [
        (
            shadow_runtime.TWAP_SHADOW_LIVE_KEY,
            {
                "origin_second_ms": 300_000,
                "predictions": {"h1": {"value": "64000"}},
            },
        )
    ]
    assert batches.qsize() == 1
    assert batches.get_nowait() is sentinel


def test_redis_failure_still_enqueues_prediction_history(monkeypatch):
    clock = LoopClock()
    events = []
    live_cache = SequencedLiveCache(
        clock=clock,
        poll_times=(400_100,),
        events=events,
        write_error=OSError("redis unavailable"),
    )

    async def no_sleep(_delay):
        return None

    async def scenario():
        batches = RecordingQueue(events=events)
        await _run_until_cache_exhausted(
            settings=_loop_settings(),
            live_cache=live_cache,
            state=shadow_runtime._ShadowModelRuntimeState(RecordingModel()),
            actual_sink=shadow_runtime.TwapShadowActualSink(max_events=10),
            batches=batches,
        )
        return batches

    monkeypatch.setattr(shadow_runtime, "current_utc_epoch_ms", clock.current_ms)
    monkeypatch.setattr(shadow_runtime.asyncio, "sleep", no_sleep)
    batches = asyncio.run(scenario())

    assert events == [("redis", 400_000), ("queue", 400_000)]
    assert batches.qsize() == 1
    assert batches.get_nowait().origin_second_ms == 400_000


def test_persistence_retries_and_acknowledges_queue_only_after_success(monkeypatch):
    calls = []
    first_failed = None
    second_started = None
    allow_success = None

    async def scenario():
        nonlocal first_failed, second_started, allow_success
        first_failed = asyncio.Event()
        second_started = asyncio.Event()
        allow_success = asyncio.Event()
        batches = RecordingQueue()
        batch = shadow_runtime.ShadowPersistenceBatch.from_forecast_batch(
            StubBatch(500_000)
        )
        assert isinstance(batch, shadow_runtime.ShadowPersistenceBatch)
        assert not hasattr(batch.forecasts[0], "sources")
        batches.put_nowait(batch)

        async def fake_persist(pool, candidate):
            assert pool == "pool"
            assert candidate is batch
            calls.append(candidate.origin_second_ms)
            if len(calls) == 1:
                first_failed.set()
                raise RuntimeError("temporary database failure")
            second_started.set()
            await allow_success.wait()

        monkeypatch.setattr(
            shadow_runtime, "persist_twap_shadow_prediction_batch", fake_persist
        )
        monkeypatch.setattr(shadow_runtime, "reconnect_delay_seconds", lambda _n: 0)
        worker = asyncio.create_task(
            shadow_runtime.twap_shadow_persistence_worker(
                pool="pool", batches=batches
            )
        )
        await first_failed.wait()
        await second_started.wait()
        assert batches.task_done_calls == 0
        assert not batches.empty() or batches._unfinished_tasks == 1

        allow_success.set()
        await asyncio.wait_for(batches.join(), timeout=1)
        assert batches.task_done_calls == 1
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker

    asyncio.run(scenario())
    assert calls == [500_000, 500_000]


def test_retention_uses_exact_day_cutoff_and_bounded_delete(monkeypatch):
    now_ms = 1_900_000_000_000
    calls = []

    class StopRetention(BaseException):
        pass

    async def fake_delete(pool, *, cutoff_target_ms, batch_size):
        calls.append((pool, cutoff_target_ms, batch_size))
        raise StopRetention

    monkeypatch.setattr(shadow_runtime, "current_utc_epoch_ms", lambda: now_ms)
    monkeypatch.setattr(
        shadow_runtime, "delete_expired_twap_shadow_predictions", fake_delete
    )

    with pytest.raises(StopRetention):
        asyncio.run(
            shadow_runtime.twap_shadow_retention_worker(
                pool="pool", retention_days=30
            )
        )

    assert calls == [
        (
            "pool",
            now_ms - 30 * 86_400_000,
            shadow_runtime.SHADOW_RETENTION_DELETE_BATCH_SIZE,
        )
    ]
