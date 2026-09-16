"""Optional event admission must never delay or replace source collection."""
import asyncio
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest

import price_collector.polymarket_chainlink_collector as collector
import price_collector.polymarket_twap as twap
from test_polymarket_chainlink_collector import (
    ScriptedRtdsWebSocket, WebSocketContext, chainlink_run_settings,
    reader_settings, valid_message,
)
from test_polymarket_twap import AcknowledgingQueue, current_twap_message, twap_settings


class RecordingGhost:
    def __init__(self, stages=None, *, fail_price=False, fail_gap=False):
        self.stages = [] if stages is None else stages
        self.fail_price = fail_price
        self.fail_gap = fail_gap
        self.prices = []
        self.gaps = []
        self.stop_reasons = []
        self.closed = 0

    def offer_price(self, **event):
        self.stages.append("ghost_offer")
        self.prices.append(event)
        if self.fail_price:
            raise RuntimeError("optional offer failed")

    def offer_gap(self, feed, reason):
        self.stages.append("ghost_gap")
        self.gaps.append((feed, reason))
        if self.fail_gap:
            raise RuntimeError("optional gap failed")

    async def close(self):
        self.closed += 1

    def stop(self, reason):
        self.stop_reasons.append(reason)


async def stop(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def until(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0)
    await asyncio.wait_for(poll(), timeout=1)


@pytest.mark.parametrize("fail_price", [False, True])
def test_spot_offers_original_clocks_before_yield_and_preserves_delivery(monkeypatch, fail_price):
    async def scenario():
        stages = []
        source = valid_message()
        invalid = valid_message(topic="crypto_prices_twap_sixty")
        websocket = ScriptedRtdsWebSocket(["PONG", "bad-json", json.dumps(invalid, default=str),
                                          json.dumps(source, default=str)])
        ghost = RecordingGhost(stages, fail_price=fail_price)
        state = collector.ChainlinkDeliveryState()
        wall = 1_783_459_200_900_123_456
        mono = 8_000_000_000
        original_parse = collector.parse_polymarket_chainlink_message

        def parse(*args, **kwargs):
            tick = original_parse(*args, **kwargs)
            stages.append("parse")
            asyncio.get_running_loop().call_soon(stages.append, "competing_callback")
            return tick

        monkeypatch.setattr(collector.websockets, "connect", lambda *a, **kw: WebSocketContext(websocket))
        def stamp_wall():
            stages.append("wall")
            return wall
        def stamp_mono():
            stages.append("monotonic")
            return mono
        monkeypatch.setattr(collector.time, "time_ns", stamp_wall)
        monkeypatch.setattr(collector.time, "monotonic_ns", stamp_mono)
        monkeypatch.setattr(collector, "parse_polymarket_chainlink_message", parse)
        task = asyncio.create_task(collector.polymarket_chainlink_reader_loop(
            reader_settings(), state, ghost_runtime=ghost))
        try:
            await until(lambda: bool(ghost.prices) and "competing_callback" in stages)
            assert stages.index("parse") < stages.index("ghost_offer") < stages.index("competing_callback")
            parse_index = stages.index("parse")
            assert stages[parse_index - 2:parse_index] == ["wall", "monotonic"]
            assert len(ghost.prices) == 1
            event = ghost.prices[0]
            assert event == dict(feed="spot", value=Decimal("123456.780000000000000000"),
                                 source_ms=1_783_459_200_123, received_wall_ns=wall,
                                 received_mono_ns=mono, event_id="chainlink:1")
            assert state.telemetry_fields(now_ms=1)["delivery_sequence"] == 1
            assert state.history_pending_count == 1
            if fail_price:
                assert ("spot", "collector_offer_failed") in ghost.gaps
        finally:
            await stop(task)
        assert ("spot", "connection_end") in ghost.gaps
    asyncio.run(scenario())


@pytest.mark.parametrize("fail_price", [False, True])
def test_twap_offer_precedes_redis_and_preserves_durable_event(monkeypatch, fail_price):
    async def scenario():
        stages = []
        message = current_twap_message()
        wrong = current_twap_message(window_s=30)
        websocket = ScriptedRtdsWebSocket(["PING", json.dumps(wrong, default=str),
                                          json.dumps(message, default=str)])
        records = AcknowledgingQueue(maxsize=103)
        ghost = RecordingGhost(stages, fail_price=fail_price)
        original_parse = twap.parse_polymarket_twap_message

        def parse(*args, **kwargs):
            tick = original_parse(*args, **kwargs)
            stages.append("parse")
            asyncio.get_running_loop().call_soon(stages.append, "competing_callback")
            return tick

        class Cache:
            async def set_price(self, key, **values):
                stages.append("source_redis")

        monkeypatch.setattr(twap.websockets, "connect", lambda *a, **kw: WebSocketContext(websocket))
        monkeypatch.setattr(twap, "parse_polymarket_twap_message", parse)
        task = asyncio.create_task(twap.polymarket_twap_reader_loop(
            twap_settings(), instrument_id=77, records=records, live_cache=Cache(), ghost_runtime=ghost))
        try:
            await until(lambda: any(isinstance(row, twap.PolymarketTwapEvent) for row in records._queue))
            event = next(row for row in records._queue if isinstance(row, twap.PolymarketTwapEvent))
            assert len(ghost.prices) == 1
            assert ghost.prices[0] == dict(
                feed="twap", value=event.price, source_ms=event.provider_event_ms,
                received_wall_ns=event.received_wall_ns, received_mono_ns=event.received_monotonic_ns,
                event_id=f"{event.connection_id}:{event.receive_sequence}", window_s=60)
            assert event.price == Decimal("64255.113422936400000000")
            assert stages.index("parse") < stages.index("ghost_offer") < stages.index("competing_callback")
            assert stages.index("ghost_offer") < stages.index("source_redis")
            if fail_price:
                assert ("twap", "collector_offer_failed") in ghost.gaps
        finally:
            await stop(task)
        assert ("twap", "connection_end") in ghost.gaps
    asyncio.run(scenario())


def test_gap_offer_precedes_teardown_and_cannot_mask_cancellation(monkeypatch):
    async def scenario():
        ghost = RecordingGhost(fail_gap=True)
        websocket = ScriptedRtdsWebSocket([json.dumps(valid_message(), default=str)])

        class Context(WebSocketContext):
            async def __aexit__(self, *args):
                assert ("spot", "connection_end") in ghost.gaps
                await asyncio.sleep(0)
                return False

        monkeypatch.setattr(collector.websockets, "connect", lambda *a, **kw: Context(websocket))
        state = collector.ChainlinkDeliveryState()
        task = asyncio.create_task(collector.polymarket_chainlink_reader_loop(
            reader_settings(), state, ghost_runtime=ghost))
        await until(lambda: bool(ghost.prices))
        await stop(task)
    asyncio.run(scenario())


def test_default_off_does_not_create_optional_task_or_call_factory(monkeypatch):
    async def scenario():
        monkeypatch.setattr(collector, "GhostSettings", lambda: SimpleNamespace(enabled=False))
        async def forbidden(_settings):
            pytest.fail("disabled optional runtime must not start")
        monkeypatch.setattr(collector, "start_ghost_runtime", forbidden)
        before = asyncio.all_tasks()
        assert collector._create_optional_ghost_sink(SimpleNamespace()) is None
        assert asyncio.all_tasks() == before
    asyncio.run(scenario())


def test_background_start_shared_admission_order_and_owned_cleanup(monkeypatch):
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        runtime = RecordingGhost()
        async def start(_settings):
            entered.set()
            await release.wait()
            return runtime
        monkeypatch.setattr(collector, "start_ghost_runtime", start)
        sink = collector._OptionalGhostSink(SimpleNamespace())
        await entered.wait()
        sink.offer_price(feed="spot", value=Decimal(1))
        assert runtime.prices == []  # Initialization is a warm-up boundary, not replay.
        release.set()
        await sink.start_task
        sink.offer_price(feed="spot", value=Decimal(2))
        sink.offer_price(feed="twap", value=Decimal(3))
        assert [event["feed"] for event in runtime.prices] == ["spot", "twap"]
        await sink.close()
        sink.offer_price(feed="spot", value=Decimal(4))
        assert len(runtime.prices) == 2
        assert runtime.closed == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("stop_fails", [False, True])
def test_optional_offer_failure_marks_both_feeds_lost_and_logs_once(monkeypatch, caplog, stop_fails):
    async def scenario():
        runtime = RecordingGhost(fail_price=True)
        if stop_fails:
            def broken_stop(reason):
                runtime.stop_reasons.append(reason)
                raise RuntimeError("optional stop failed")
            runtime.stop = broken_stop
        async def start(_settings):
            return runtime
        monkeypatch.setattr(collector, "start_ghost_runtime", start)
        sink = collector._OptionalGhostSink(SimpleNamespace())
        await sink.start_task
        sink.offer_price(feed="spot", value=Decimal(1))
        sink.offer_price(feed="spot", value=Decimal(2))
        assert len(runtime.prices) == 1
        assert runtime.stop_reasons == ["collector_offer_failed"]
        assert runtime.gaps == [("spot", "collector_offer_failed"), ("twap", "collector_offer_failed")]
        assert sum(record.message == "ghost_optional_offer_failed" for record in caplog.records) == 1
        await sink.close()
    asyncio.run(scenario())


def test_collector_core_starts_while_optional_factory_is_blocked(monkeypatch):
    async def scenario():
        core_started, factory_started, factory_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        closed = []
        async def start(_settings):
            factory_started.set()
            try:
                await asyncio.Future()
            finally:
                factory_cancelled.set()
        async def reader(settings, state, *, raw_capture=None, ghost_runtime=None):
            assert isinstance(ghost_runtime, collector._OptionalGhostSink)
            core_started.set()
            await asyncio.Future()
        class Pool:
            async def close(self):
                closed.append("pool")
        class Cache:
            async def close(self):
                closed.append("cache")
        async def create_pool(_url):
            return Pool()
        async def instrument(*args, **kwargs):
            return 2
        monkeypatch.setattr(collector, "GhostSettings", lambda: SimpleNamespace(enabled=True))
        monkeypatch.setattr(collector, "start_ghost_runtime", start)
        monkeypatch.setattr(collector, "create_pool", create_pool)
        monkeypatch.setattr(collector, "get_instrument_id", instrument)
        monkeypatch.setattr(collector, "create_live_cache", lambda _settings: Cache())
        monkeypatch.setattr(collector, "polymarket_chainlink_reader_loop", reader)
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)
        task = asyncio.create_task(collector.run_collector(chainlink_run_settings(raw_enabled=False)))
        await asyncio.wait_for(core_started.wait(), timeout=1)
        await asyncio.wait_for(factory_started.wait(), timeout=1)
        assert not task.done()
        await stop(task)
        assert factory_cancelled.is_set()
        assert closed == ["cache", "pool"]
    asyncio.run(scenario())


def test_ghost_factory_uses_existing_redis_fields_and_closes_only_owned_clients(monkeypatch, tmp_path):
    import asyncpg
    import redis.asyncio as redis_async
    import price_collector.ghost_twap_runtime as runtime_module
    from price_collector.config import Settings

    async def scenario():
        calls = []
        settings = Settings(DATABASE_URL="postgresql://writer@localhost/price_collector")
        assert not hasattr(settings, "REDIS_URL")
        class Pool:
            async def close(self):
                calls.append("pool_close")
        class Redis:
            def __init__(self, **kwargs):
                calls.append(("redis", kwargs))
            async def aclose(self):
                calls.append("redis_close")
        class Runtime:
            def __init__(self, *args):
                pass
            async def start(self):
                calls.append("runtime_start")
            async def close(self):
                calls.append("runtime_close")
        async def create_pool(**kwargs):
            calls.append(("pool", kwargs))
            return Pool()
        monkeypatch.setattr(runtime_module, "GhostSettings", lambda: SimpleNamespace(
            enabled=True, continuous=False, state_directory=tmp_path, audit_max_records=2, record_max_bytes=1024))
        monkeypatch.setattr(runtime_module, "GhostSpool", lambda *args: object())
        monkeypatch.setattr(runtime_module, "GhostRuntime", Runtime)
        monkeypatch.setattr(asyncpg, "create_pool", create_pool)
        monkeypatch.setattr(redis_async, "Redis", Redis)
        runtime = await runtime_module.start_ghost_runtime(settings)
        redis_options = next(value for name, value in calls[:2] if name == "redis")
        assert redis_options["host"] == settings.REDIS_HOST
        assert redis_options["port"] == settings.REDIS_PORT
        assert redis_options["db"] == settings.REDIS_DB
        assert calls[0][1]["dsn"] == settings.DATABASE_URL
        assert calls[0][1]["max_size"] == 2
        await runtime.close()
        assert calls[-3:] == ["runtime_close", "redis_close", "pool_close"]
    asyncio.run(scenario())


@pytest.mark.parametrize("field,value", [
    ("POLYMARKET_TWAP_ENABLED", False),
    ("POLYMARKET_TWAP_WINDOW_SECONDS", 30),
    ("POLYMARKET_TWAP_TOPIC", "crypto_prices_twap_thirty"),
    ("POLYMARKET_TWAP_RTD_SYMBOL", "eth/usd"),
    ("POLYMARKET_CHAINLINK_TOPIC", "unexpected"),
    ("POLYMARKET_CHAINLINK_SYMBOL", "ETHUSD"),
])
def test_ghost_factory_rejects_unsupported_feed_before_allocating_resources(monkeypatch, field, value):
    import asyncpg
    import price_collector.ghost_twap_runtime as runtime_module
    from price_collector.config import Settings

    async def scenario():
        settings = SimpleNamespace(**Settings().model_dump())
        setattr(settings, field, value)
        async def forbidden(**kwargs):
            pytest.fail("invalid feed identity must fail before opening a pool")
        monkeypatch.setattr(runtime_module, "GhostSettings", lambda: SimpleNamespace(enabled=True))
        monkeypatch.setattr(asyncpg, "create_pool", forbidden)
        with pytest.raises(ValueError):
            await runtime_module.start_ghost_runtime(settings)
    asyncio.run(scenario())
