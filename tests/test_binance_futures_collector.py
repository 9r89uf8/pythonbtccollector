import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

import price_collector.binance_futures_collector as collector
from price_collector.live_cache import FUTURES_LIVE_KEY


def open_interest_payload(**overrides):
    payload = {
        "symbol": "BTCUSDT",
        "openInterest": "74321.123",
        "time": 1_783_459_500_456,
    }
    payload.update(overrides)
    return payload


def premium_index_payload(**overrides):
    payload = {
        "symbol": "BTCUSDT",
        "markPrice": "62074.88",
        "indexPrice": "62070.19",
        "lastFundingRate": "0.00010000",
        "nextFundingTime": 1_783_468_800_000,
        "time": 1_783_459_500_500,
    }
    payload.update(overrides)
    return payload


def ticker_payload(**overrides):
    payload = {
        "symbol": "BTCUSDT",
        "price": "62075.12",
        "time": 1_783_459_500_510,
    }
    payload.update(overrides)
    return payload


def futures_settings():
    return SimpleNamespace(
        BINANCE_FUTURES_BASE_URL="https://fapi.binance.com",
        BINANCE_FUTURES_SYMBOL="BTCUSDT",
        BINANCE_FUTURES_STORE_RAW_JSON=False,
    )


def test_build_snapshot_uses_futures_price_time_for_sample_market_and_decimal_math():
    snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=1_783_459_499_456),
        premium_index_payload=premium_index_payload(),
        ticker_payload=ticker_payload(),
        received_ms=1_783_459_500_700,
    )

    assert snapshot.sample_second_ms == 1_783_459_500_000
    assert snapshot.window.market_start_ms == 1_783_459_500_000
    assert snapshot.window.market_end_ms == 1_783_459_800_000
    assert snapshot.futures_last_price == Decimal("62075.12")
    assert snapshot.mark_price == Decimal("62074.88")
    assert snapshot.index_price == Decimal("62070.19")
    assert snapshot.open_interest == Decimal("74321.123")
    assert snapshot.open_interest_time_ms == 1_783_459_499_456
    assert snapshot.oi_notional_usdt == Decimal("74321.123") * Decimal("62074.88")
    assert snapshot.premium_bps == (
        Decimal("62074.88") / Decimal("62070.19") - Decimal("1")
    ) * Decimal("10000")


def test_build_snapshot_uses_futures_price_time_when_oi_time_is_missing():
    snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=None),
        premium_index_payload=premium_index_payload(),
        ticker_payload=ticker_payload(),
        received_ms=1_783_459_499_999,
    )

    assert snapshot.open_interest_time_ms is None
    assert snapshot.sample_second_ms == 1_783_459_500_000
    assert snapshot.window.market_start_ms == 1_783_459_500_000


def test_build_snapshot_falls_back_to_premium_index_then_received_ms_for_row_time():
    premium_snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=1_783_459_499_456),
        premium_index_payload=premium_index_payload(time=1_783_459_500_500),
        ticker_payload=ticker_payload(time=None),
        received_ms=1_783_459_501_700,
    )

    received_snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=1_783_459_499_456),
        premium_index_payload=premium_index_payload(time=None),
        ticker_payload=ticker_payload(time=None),
        received_ms=1_783_459_501_700,
    )

    assert premium_snapshot.sample_second_ms == 1_783_459_500_000
    assert received_snapshot.sample_second_ms == 1_783_459_501_000


def test_build_snapshot_rejects_unexpected_symbol():
    with pytest.raises(collector.FuturesParseError, match="unexpected open interest symbol"):
        collector.build_binance_futures_snapshot(
            symbol="BTCUSDT",
            open_interest_payload=open_interest_payload(symbol="ETHUSDT"),
            premium_index_payload=premium_index_payload(),
            ticker_payload=ticker_payload(),
            received_ms=1_783_459_500_700,
        )


def test_historical_oi_summary_assigns_completed_bucket_to_next_market():
    summary = collector.build_binance_futures_oi_5m_summary(
        symbol="BTCUSDT",
        payload={
            "symbol": "BTCUSDT",
            "sumOpenInterest": "74321.123",
            "sumOpenInterestValue": "4616789012.34",
            "timestamp": 1_783_459_500_000,
        },
        received_ms=1_783_459_501_000,
        now_ms=1_783_459_501_000,
    )

    assert summary is not None
    assert summary.source_window_start_ms == 1_783_459_200_000
    assert summary.source_window_end_ms == 1_783_459_500_000
    assert summary.effective_window.market_start_ms == 1_783_459_500_000
    assert summary.sum_open_interest == Decimal("74321.123")
    assert summary.sum_open_interest_value == Decimal("4616789012.34")
    assert (
        summary.raw["timestamp_interpretation"]
        == "aligned_timestamp_treated_as_source_window_end_ms"
    )


def test_historical_oi_summary_skips_uncompleted_bucket():
    summary = collector.build_binance_futures_oi_5m_summary(
        symbol="BTCUSDT",
        payload={
            "symbol": "BTCUSDT",
            "sumOpenInterest": "74321.123",
            "sumOpenInterestValue": "4616789012.34",
            "timestamp": 1_783_459_800_000,
        },
        received_ms=1_783_459_501_000,
        now_ms=1_783_459_501_000,
    )

    assert summary is None


def test_get_json_parses_json_numbers_as_decimal():
    class FakeResponse:
        text = '{"price": 62075.12, "time": 1783459500510}'

        def raise_for_status(self):
            return None

    class FakeClient:
        async def get(self, url, params):
            assert url == "https://fapi.binance.com/fapi/v2/ticker/price"
            assert params == {"symbol": "BTCUSDT"}
            return FakeResponse()

    data = asyncio.run(
        collector.get_json(
            FakeClient(),
            "https://fapi.binance.com/",
            "/fapi/v2/ticker/price",
            {"symbol": "BTCUSDT"},
        )
    )

    assert data["price"] == Decimal("62075.12")
    assert not isinstance(data["price"], float)


def test_collect_once_fetches_three_endpoints_and_upserts_snapshot(monkeypatch):
    requests = []
    upserts = []

    async def fake_get_json(client, base_url, path, params):
        requests.append((base_url, path, params))
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
            "/fapi/v2/ticker/price": ticker_payload(),
        }[path]

    async def fake_upsert_binance_futures_snapshot(pool, **kwargs):
        upserts.append(kwargs)

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(
        collector,
        "upsert_binance_futures_snapshot",
        fake_upsert_binance_futures_snapshot,
    )
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    snapshot = asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
        )
    )

    assert {path for _base_url, path, _params in requests} == {
        "/fapi/v1/openInterest",
        "/fapi/v1/premiumIndex",
        "/fapi/v2/ticker/price",
    }
    assert all(params == {"symbol": "BTCUSDT"} for _base_url, _path, params in requests)
    assert snapshot.sample_second_ms == 1_783_459_500_000
    assert upserts[0]["symbol"] == "BTCUSDT"
    assert upserts[0]["sample_second_ms"] == 1_783_459_500_000
    assert upserts[0]["open_interest"] == Decimal("74321.123")
    assert upserts[0]["raw"] is None


def test_collect_once_can_store_raw_json_when_enabled(monkeypatch):
    upserts = []

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
            "/fapi/v2/ticker/price": ticker_payload(),
        }[path]

    async def fake_upsert_binance_futures_snapshot(pool, **kwargs):
        upserts.append(kwargs)

    settings = futures_settings()
    settings.BINANCE_FUTURES_STORE_RAW_JSON = True

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(
        collector,
        "upsert_binance_futures_snapshot",
        fake_upsert_binance_futures_snapshot,
    )
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=settings,
        )
    )

    assert upserts[0]["raw"]["openInterest"]["openInterest"] == "74321.123"


def test_collect_once_writes_futures_live_cache_before_postgres(monkeypatch):
    events = []

    class FakeLiveCache:
        async def set_price(
            self,
            key,
            *,
            value,
            source_timestamp_ms,
            received_ms,
        ):
            events.append(
                (
                    "redis",
                    {
                        "key": key,
                        "value": value,
                        "source_timestamp_ms": source_timestamp_ms,
                        "received_ms": received_ms,
                    },
                )
            )

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
            "/fapi/v2/ticker/price": ticker_payload(),
        }[path]

    async def fake_upsert_binance_futures_snapshot(pool, **kwargs):
        events.append(("postgres", kwargs))

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(
        collector,
        "upsert_binance_futures_snapshot",
        fake_upsert_binance_futures_snapshot,
    )
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
            live_cache=FakeLiveCache(),
        )
    )

    assert [event_name for event_name, _payload in events] == ["redis", "postgres"]
    assert events[0][1] == {
        "key": FUTURES_LIVE_KEY,
        "value": Decimal("62075.12"),
        "source_timestamp_ms": 1_783_459_500_510,
        "received_ms": 1_783_459_500_700,
    }
    assert events[1][1]["sample_second_ms"] == 1_783_459_500_000


def test_collect_once_observes_rest_shadow_without_changing_public_source(monkeypatch):
    events = []

    class FakeShadowMonitor:
        def observe_rest(self, **kwargs):
            events.append(("shadow", kwargs))

    class FakeLiveCache:
        async def set_price(
            self,
            key,
            *,
            value,
            source_timestamp_ms,
            received_ms,
        ):
            events.append(
                (
                    "redis",
                    {
                        "key": key,
                        "value": value,
                        "source_timestamp_ms": source_timestamp_ms,
                        "received_ms": received_ms,
                    },
                )
            )

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
            "/fapi/v2/ticker/price": ticker_payload(),
        }[path]

    async def fake_upsert_binance_futures_snapshot(pool, **kwargs):
        events.append(("postgres", kwargs))

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(
        collector,
        "upsert_binance_futures_snapshot",
        fake_upsert_binance_futures_snapshot,
    )
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    snapshot = asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
            live_cache=FakeLiveCache(),
            shadow_monitor=FakeShadowMonitor(),
        )
    )

    assert [event_name for event_name, _payload in events] == [
        "shadow",
        "redis",
        "postgres",
    ]
    expected_rest_value = Decimal("62075.12")
    assert events[0][1] == {
        "price": expected_rest_value,
        "source_timestamp_ms": 1_783_459_500_510,
        "received_ms": 1_783_459_500_700,
    }
    assert events[1][1]["value"] == expected_rest_value
    assert events[2][1]["futures_last_price"] == expected_rest_value
    assert snapshot.futures_last_price == expected_rest_value


def test_collect_historical_oi_once_stores_only_completed_summaries(monkeypatch):
    upserts = []

    async def fake_get_json(client, base_url, path, params):
        assert path == "/futures/data/openInterestHist"
        assert params == {"symbol": "BTCUSDT", "period": "5m", "limit": 2}
        return [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "74321.123",
                "sumOpenInterestValue": "4616789012.34",
                "timestamp": 1_783_459_500_000,
            },
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "74400.000",
                "sumOpenInterestValue": "4620000000.00",
                "timestamp": 1_783_459_800_000,
            },
        ]

    async def fake_upsert_binance_futures_oi_5m_summary(pool, **kwargs):
        upserts.append(kwargs)

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(
        collector,
        "upsert_binance_futures_oi_5m_summary",
        fake_upsert_binance_futures_oi_5m_summary,
    )
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_501_000)

    stored = asyncio.run(
        collector.collect_historical_oi_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
        )
    )

    assert stored == 1
    assert upserts[0]["source_window_start_ms"] == 1_783_459_200_000
    assert upserts[0]["source_window_end_ms"] == 1_783_459_500_000
    assert upserts[0]["effective_window"].market_start_ms == 1_783_459_500_000
    assert upserts[0]["raw"] is None


def run_settings(*, raw_enabled, streams_enabled=True):
    return SimpleNamespace(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql://writer@localhost/price_collector",
        BINANCE_FUTURES_BASE_URL="https://fapi.binance.com",
        BINANCE_FUTURES_SYMBOL="BTCUSDT",
        BINANCE_FUTURES_PROVIDER_CODE="binance_usdm_perp",
        BINANCE_FUTURES_REST_TIMEOUT_SECONDS=5,
        BINANCE_FUTURES_HIST_OI_ENABLED=False,
        BINANCE_FUTURES_STREAMS_ENABLED=streams_enabled,
        BINANCE_FUTURES_STORE_RAW_JSON=False,
        RAW_FUTURES_TRACE_ENABLED=raw_enabled,
        RAW_CAPTURE_RETENTION_HOURS=72,
        RAW_CAPTURE_MAX_RELATION_MB=2048,
        RAW_CAPTURE_QUEUE_MAX_EVENTS=5000,
        RAW_CAPTURE_BATCH_MAX_ROWS=500,
        RAW_CAPTURE_FLUSH_MS=1000,
        RAW_CAPTURE_RETENTION_CHECK_SECONDS=60,
        RAW_FUTURES_BUCKET_MS=100,
    )


class FakeAsyncClient:
    def __init__(self, *, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def test_run_collector_rejects_raw_capture_without_futures_streams(monkeypatch):
    monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
    monkeypatch.setattr(
        collector,
        "require_collector_database_url",
        lambda _settings: pytest.fail("database must not be opened"),
    )

    with pytest.raises(RuntimeError, match="requires BINANCE_FUTURES_STREAMS_ENABLED"):
        asyncio.run(
            collector.run_collector(
                run_settings(raw_enabled=True, streams_enabled=False)
            )
        )


def test_sigterm_handler_cancels_current_task_and_can_be_removed(monkeypatch):
    calls = []

    class FakeTask:
        def cancel(self):
            calls.append("cancel")

    class FakeLoop:
        def add_signal_handler(self, sig, callback):
            calls.append(("add", sig))
            self.callback = callback

        def remove_signal_handler(self, sig):
            calls.append(("remove", sig))
            return True

    loop = FakeLoop()
    monkeypatch.setattr(collector.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(collector.asyncio, "current_task", lambda: FakeTask())

    remove_handler = collector._install_sigterm_cancellation()
    assert remove_handler is not None
    loop.callback()
    remove_handler()

    assert calls == [
        ("add", collector.signal.SIGTERM),
        "cancel",
        ("remove", collector.signal.SIGTERM),
    ]


def test_run_collector_disabled_path_constructs_no_raw_resources(monkeypatch):
    async def scenario():
        started = asyncio.Event()
        events = []

        class FakePool:
            async def close(self):
                events.append("pool_close")

        class FakeLiveCache:
            async def close(self):
                events.append("live_close")

        async def fake_create_pool(database_url):
            assert database_url == "postgresql://writer@localhost/price_collector"
            return FakePool()

        async def fake_snapshot_loop(**kwargs):
            assert kwargs["shadow_monitor"] is None
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("snapshot_stopped")

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "create_live_cache", lambda _settings: FakeLiveCache())
        monkeypatch.setattr(collector, "snapshot_loop", fake_snapshot_loop)
        monkeypatch.setattr(collector.httpx, "AsyncClient", FakeAsyncClient)
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)
        monkeypatch.setattr(
            collector,
            "create_raw_capture_runtime",
            lambda **_kwargs: pytest.fail("disabled capture constructed a runtime"),
        )
        monkeypatch.setattr(
            collector,
            "FuturesShadowMonitor",
            lambda: pytest.fail("disabled capture constructed a monitor"),
        )

        task = asyncio.create_task(
            collector.run_collector(
                run_settings(raw_enabled=False, streams_enabled=False)
            )
        )
        await asyncio.wait_for(started.wait(), timeout=0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events == ["snapshot_stopped", "live_close", "pool_close"]

    asyncio.run(scenario())


def test_run_collector_wires_lazy_raw_runtime_and_closes_after_tasks(monkeypatch):
    async def scenario():
        events = []
        agg_started = asyncio.Event()
        telemetry_started = asyncio.Event()
        runtime_kwargs = {}
        reader_kwargs = {}
        telemetry_kwargs = {}
        backend_calls = []

        class FakePool:
            async def close(self):
                events.append("pool_close")

        class FakeLiveCache:
            async def close(self):
                events.append("live_close")

        class FakeRawRuntime:
            def start(self):
                events.append("raw_start")

            async def close(self):
                events.append("raw_close")

        class FakeMonitor:
            pass

        raw_runtime = FakeRawRuntime()
        monitor = FakeMonitor()

        async def fake_create_pool(database_url):
            assert database_url == "postgresql://writer@localhost/price_collector"
            return FakePool()

        async def fake_create_raw_capture_backend(database_url, **kwargs):
            backend_calls.append((database_url, kwargs))
            return "backend"

        def fake_create_raw_capture_runtime(**kwargs):
            runtime_kwargs.update(kwargs)
            return raw_runtime

        async def blocking_task(name):
            try:
                await asyncio.Event().wait()
            finally:
                events.append(f"{name}_stopped")

        async def fake_snapshot_loop(**kwargs):
            assert kwargs["shadow_monitor"] is monitor
            await blocking_task("snapshot")

        async def fake_agg_reader(settings, flow_store, **kwargs):
            reader_kwargs.update(kwargs)
            agg_started.set()
            await blocking_task("agg")

        async def fake_telemetry_loop(**kwargs):
            telemetry_kwargs.update(kwargs)
            telemetry_started.set()
            await blocking_task("telemetry")

        async def fake_flow_flush_loop(**kwargs):
            await blocking_task("flow_flush")

        async def fake_book_reader(settings, book_store):
            await blocking_task("book_reader")

        async def fake_book_flush_loop(**kwargs):
            await blocking_task("book_flush")

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "create_live_cache", lambda _settings: FakeLiveCache())
        monkeypatch.setattr(collector, "create_raw_capture_backend", fake_create_raw_capture_backend)
        monkeypatch.setattr(collector, "create_raw_capture_runtime", fake_create_raw_capture_runtime)
        monkeypatch.setattr(collector, "FuturesShadowMonitor", lambda: monitor)
        monkeypatch.setattr(collector, "AsyncFlowAggregator", lambda **_kwargs: "flow")
        monkeypatch.setattr(collector, "AsyncBookTickerAggregator", lambda **_kwargs: "book")
        monkeypatch.setattr(collector, "snapshot_loop", fake_snapshot_loop)
        monkeypatch.setattr(collector, "futures_agg_trade_reader_loop", fake_agg_reader)
        monkeypatch.setattr(collector, "futures_raw_capture_telemetry_loop", fake_telemetry_loop)
        monkeypatch.setattr(collector, "futures_flow_flush_loop", fake_flow_flush_loop)
        monkeypatch.setattr(collector, "futures_book_ticker_reader_loop", fake_book_reader)
        monkeypatch.setattr(collector, "futures_book_flush_loop", fake_book_flush_loop)
        monkeypatch.setattr(collector.httpx, "AsyncClient", FakeAsyncClient)
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)

        task = asyncio.create_task(
            collector.run_collector(run_settings(raw_enabled=True))
        )
        await asyncio.wait_for(agg_started.wait(), timeout=0.5)
        await asyncio.wait_for(telemetry_started.wait(), timeout=0.5)

        assert backend_calls == []
        assert await runtime_kwargs["backend_factory"]() == "backend"
        assert backend_calls == [
            (
                "postgresql://writer@localhost/price_collector",
                {"retention_hours": 72, "max_relation_mb": 2048},
            )
        ]
        assert runtime_kwargs["futures_enabled"] is True
        assert runtime_kwargs["chainlink_enabled"] is False
        assert runtime_kwargs["queue_max_events"] == 5000
        assert runtime_kwargs["batch_max_rows"] == 500
        assert runtime_kwargs["flush_ms"] == 1000
        assert runtime_kwargs["maintenance_interval_seconds"] == 60
        assert runtime_kwargs["bucket_ms"] == 100
        assert reader_kwargs == {
            "raw_capture": raw_runtime,
            "shadow_monitor": monitor,
        }
        assert telemetry_kwargs == {
            "raw_capture": raw_runtime,
            "shadow_monitor": monitor,
            "interval_seconds": 60.0,
        }

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        raw_close_index = events.index("raw_close")
        assert all(
            events.index(f"{name}_stopped") < raw_close_index
            for name in (
                "snapshot",
                "agg",
                "telemetry",
                "flow_flush",
                "book_reader",
                "book_flush",
            )
        )
        assert raw_close_index < events.index("live_close") < events.index("pool_close")
        assert events.count("raw_start") == 1

    asyncio.run(scenario())
