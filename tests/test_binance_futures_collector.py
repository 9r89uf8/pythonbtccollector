import asyncio
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

import price_collector.binance_futures_collector as collector
import price_collector.binance_futures_streams as streams
from price_collector.live_cache import FUTURES_LIVE_KEY
from price_collector.raw_capture import ReceiveStamp


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


def sequenced_futures_trade(
    *,
    sequence=1,
    symbol="BTCUSDT",
    price="62075.12",
    trade_time_ms=1_783_459_499_510,
    event_time_ms=1_783_459_499_525,
    received_ms=1_783_459_499_540,
    received_monotonic_ns=1_000_000_000,
    agg_trade_id=10,
    connection_id=UUID("11111111-1111-1111-1111-111111111111"),
):
    trade = streams.BinanceAggTrade(
        symbol=symbol,
        agg_trade_id=agg_trade_id,
        price=Decimal(price),
        quantity=Decimal("0.25"),
        first_trade_id=100,
        last_trade_id=102,
        trade_time_ms=trade_time_ms,
        event_time_ms=event_time_ms,
        buyer_is_maker=False,
    )
    return streams.SequencedFuturesTrade(
        sequence=sequence,
        trade=trade,
        stamp=ReceiveStamp(
            connection_id=connection_id,
            receive_sequence=sequence,
            received_wall_ns=received_ms * 1_000_000,
            received_monotonic_ns=received_monotonic_ns,
        ),
    )


def futures_settings():
    return SimpleNamespace(
        BINANCE_FUTURES_BASE_URL="https://fapi.binance.com",
        BINANCE_FUTURES_SYMBOL="BTCUSDT",
        BINANCE_FUTURES_STORE_RAW_JSON=False,
        STALE_PRICE_MS=10_000,
    )


def test_build_snapshot_uses_premium_time_for_row_and_agg_trade_for_last_price():
    futures_trade = sequenced_futures_trade(
        trade_time_ms=1_783_459_499_510,
    )
    snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=1_783_459_499_456),
        premium_index_payload=premium_index_payload(),
        futures_trade=futures_trade,
        received_ms=1_783_459_500_700,
    )

    assert snapshot.sample_second_ms == 1_783_459_500_000
    assert snapshot.window.market_start_ms == 1_783_459_500_000
    assert snapshot.window.market_end_ms == 1_783_459_800_000
    assert snapshot.futures_last_price == Decimal("62075.12")
    assert snapshot.futures_last_price_time_ms == 1_783_459_499_510
    assert snapshot.mark_price == Decimal("62074.88")
    assert snapshot.index_price == Decimal("62070.19")
    assert snapshot.open_interest == Decimal("74321.123")
    assert snapshot.open_interest_time_ms == 1_783_459_499_456
    assert snapshot.oi_notional_usdt == Decimal("74321.123") * Decimal("62074.88")
    assert snapshot.premium_bps == (
        Decimal("62074.88") / Decimal("62070.19") - Decimal("1")
    ) * Decimal("10000")
    assert snapshot.raw["aggTrade"] == {
        "source": "binance_futures_agg_trade",
        "symbol": "BTCUSDT",
        "agg_trade_id": 10,
        "price": "62075.12",
        "trade_time_ms": 1_783_459_499_510,
        "event_time_ms": 1_783_459_499_525,
        "connection_id": "11111111-1111-1111-1111-111111111111",
        "receive_sequence": 1,
        "received_ms": 1_783_459_499_540,
    }


def test_build_snapshot_without_current_trade_keeps_rest_snapshot_fields():
    snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=None),
        premium_index_payload=premium_index_payload(),
        futures_trade=None,
        received_ms=1_783_459_499_999,
    )

    assert snapshot.open_interest_time_ms is None
    assert snapshot.sample_second_ms == 1_783_459_500_000
    assert snapshot.window.market_start_ms == 1_783_459_500_000
    assert snapshot.futures_last_price is None
    assert snapshot.futures_last_price_time_ms is None
    assert snapshot.mark_price == Decimal("62074.88")
    assert snapshot.open_interest == Decimal("74321.123")
    assert snapshot.raw["aggTrade"] is None


def test_build_snapshot_falls_back_to_premium_index_then_received_ms_for_row_time():
    premium_snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=1_783_459_499_456),
        premium_index_payload=premium_index_payload(time=1_783_459_500_500),
        futures_trade=sequenced_futures_trade(),
        received_ms=1_783_459_501_700,
    )

    received_snapshot = collector.build_binance_futures_snapshot(
        symbol="BTCUSDT",
        open_interest_payload=open_interest_payload(time=1_783_459_499_456),
        premium_index_payload=premium_index_payload(time=None),
        futures_trade=sequenced_futures_trade(),
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
            futures_trade=sequenced_futures_trade(),
            received_ms=1_783_459_500_700,
        )

    with pytest.raises(collector.FuturesParseError, match="unexpected aggTrade symbol"):
        collector.build_binance_futures_snapshot(
            symbol="BTCUSDT",
            open_interest_payload=open_interest_payload(),
            premium_index_payload=premium_index_payload(),
            futures_trade=sequenced_futures_trade(symbol="ETHUSDT"),
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
        text = '{"markPrice": 62075.12, "time": 1783459500510}'

        def raise_for_status(self):
            return None

    class FakeClient:
        async def get(self, url, params):
            assert url == "https://fapi.binance.com/fapi/v1/premiumIndex"
            assert params == {"symbol": "BTCUSDT"}
            return FakeResponse()

    data = asyncio.run(
        collector.get_json(
            FakeClient(),
            "https://fapi.binance.com/",
            "/fapi/v1/premiumIndex",
            {"symbol": "BTCUSDT"},
        )
    )

    assert data["markPrice"] == Decimal("62075.12")
    assert not isinstance(data["markPrice"], float)


def test_collect_once_fetches_only_two_rest_endpoints_and_waits_before_upsert(
    monkeypatch,
):
    requests = []
    events = []
    futures_trade = sequenced_futures_trade()

    class FakeTradeState:
        def fresh_current(self, *, now_monotonic_ns, stale_after_ms):
            assert now_monotonic_ns == 1_000_000_100
            assert stale_after_ms == 10_000
            events.append(("select", futures_trade.sequence))
            return futures_trade

        async def wait_until_live_attempted(self, sequence):
            events.append(("redis_attempted", sequence))
            return futures_trade

        def is_fresh_current_item(self, item, **_kwargs):
            assert item == futures_trade
            return True

    class RecordingMicrostructureSink:
        def offer_context(self, snapshot):
            events.append(("context", snapshot))

    async def fake_get_json(client, base_url, path, params):
        requests.append((base_url, path, params))
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
        }[path]

    async def fake_upsert_binance_futures_snapshot(pool, **kwargs):
        events.append(("postgres", kwargs))

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(
        collector,
        "upsert_binance_futures_snapshot",
        fake_upsert_binance_futures_snapshot,
    )
    wall_times = iter([1_783_459_500_600, 1_783_459_500_700])
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: next(wall_times))
    monkeypatch.setattr(collector.time, "monotonic_ns", lambda: 1_000_000_100)

    snapshot = asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
            trade_state=FakeTradeState(),
            microstructure_sink=RecordingMicrostructureSink(),
        )
    )

    assert {path for _base_url, path, _params in requests} == {
        "/fapi/v1/openInterest",
        "/fapi/v1/premiumIndex",
    }
    assert len(requests) == 2
    assert all(params == {"symbol": "BTCUSDT"} for _base_url, _path, params in requests)
    assert snapshot.sample_second_ms == 1_783_459_500_000
    assert snapshot.futures_last_price == Decimal("62075.12")
    assert snapshot.futures_last_price_time_ms == 1_783_459_499_510
    assert snapshot.request_started_ms == 1_783_459_500_600
    assert [event_name for event_name, _payload in events] == [
        "select",
        "redis_attempted",
        "context",
        "postgres",
    ]
    upsert = events[-1][1]
    assert upsert["symbol"] == "BTCUSDT"
    assert upsert["sample_second_ms"] == 1_783_459_500_000
    assert upsert["futures_last_price"] == Decimal("62075.12")
    assert upsert["futures_last_price_time_ms"] == 1_783_459_499_510
    assert upsert["open_interest"] == Decimal("74321.123")
    assert upsert["raw"] is None


def test_collect_once_context_sink_failure_does_not_block_postgres(monkeypatch):
    events = []

    class NoCurrentTradeState:
        def fresh_current(self, **_kwargs):
            return None

    class FailingMicrostructureSink:
        def offer_context(self, snapshot):
            events.append(("context", snapshot))
            raise RuntimeError("context sink failed")

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
        }[path]

    async def fake_upsert(pool, **kwargs):
        events.append(("postgres", kwargs))

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(collector, "upsert_binance_futures_snapshot", fake_upsert)
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    snapshot = asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
            trade_state=NoCurrentTradeState(),
            microstructure_sink=FailingMicrostructureSink(),
        )
    )

    assert [name for name, _payload in events] == ["context", "postgres"]
    assert events[0][1] == snapshot
    assert events[1][1]["sample_second_ms"] == snapshot.sample_second_ms


def test_snapshot_loop_passes_microstructure_sink_to_collect_once(monkeypatch):
    sink = object()
    settings = futures_settings()
    settings.BINANCE_FUTURES_POLL_SECONDS = 2

    async def fake_sleep(seconds):
        assert seconds == 2

    async def fake_collect_once(**kwargs):
        assert kwargs["microstructure_sink"] is sink
        raise asyncio.CancelledError

    monkeypatch.setattr(collector.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(collector, "collect_once", fake_collect_once)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            collector.snapshot_loop(
                pool="pool",
                client="client",
                settings=settings,
                trade_state="trade-state",
                microstructure_sink=sink,
            )
        )


def test_collect_once_can_store_raw_json_when_enabled(monkeypatch):
    upserts = []
    futures_trade = sequenced_futures_trade()

    class FakeTradeState:
        def fresh_current(self, **_kwargs):
            return futures_trade

        async def wait_until_live_attempted(self, sequence):
            assert sequence == futures_trade.sequence
            return futures_trade

        def is_fresh_current_item(self, item, **_kwargs):
            return item == futures_trade

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
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
            trade_state=FakeTradeState(),
        )
    )

    assert upserts[0]["raw"]["openInterest"]["openInterest"] == "74321.123"
    assert upserts[0]["raw"]["aggTrade"]["source"] == "binance_futures_agg_trade"
    assert upserts[0]["raw"]["aggTrade"]["price"] == "62075.12"
    assert "ticker" not in upserts[0]["raw"]


def test_collect_once_uses_newer_trade_that_satisfied_live_attempt_barrier(
    monkeypatch,
):
    selected = sequenced_futures_trade(
        sequence=1,
        agg_trade_id=10,
        price="62075.12",
    )
    attempted = sequenced_futures_trade(
        sequence=2,
        agg_trade_id=11,
        price="62075.13",
        trade_time_ms=1_783_459_499_610,
        received_ms=1_783_459_499_640,
        received_monotonic_ns=1_100_000_000,
    )
    upserts = []

    class CoalescingTradeState:
        def fresh_current(self, **_kwargs):
            return selected

        async def wait_until_live_attempted(self, sequence):
            assert sequence == selected.sequence
            return attempted

        def is_fresh_current_item(self, item, **_kwargs):
            return item == attempted

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
        }[path]

    async def fake_upsert(pool, **kwargs):
        upserts.append(kwargs)

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(collector, "upsert_binance_futures_snapshot", fake_upsert)
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    snapshot = asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
            trade_state=CoalescingTradeState(),
        )
    )

    assert snapshot.futures_last_price == Decimal("62075.13")
    assert snapshot.futures_last_price_time_ms == 1_783_459_499_610
    assert upserts[0]["futures_last_price"] == Decimal("62075.13")


def test_collect_once_without_fresh_trade_persists_rest_fields_without_barrier(
    monkeypatch,
):
    upserts = []

    class NoFreshTradeState:
        def fresh_current(self, **_kwargs):
            return None

        async def wait_until_live_attempted(self, _sequence):
            pytest.fail("no-trade snapshot must not wait on the live barrier")

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
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
            trade_state=NoFreshTradeState(),
        )
    )

    assert snapshot.futures_last_price is None
    assert snapshot.futures_last_price_time_ms is None
    assert upserts[0]["futures_last_price"] is None
    assert upserts[0]["mark_price"] == Decimal("62074.88")
    assert upserts[0]["open_interest"] == Decimal("74321.123")


def test_collect_once_drops_trade_that_became_invalid_during_live_attempt(
    monkeypatch,
):
    futures_trade = sequenced_futures_trade()
    upserts = []

    class ReconnectedTradeState:
        def fresh_current(self, **_kwargs):
            return futures_trade

        async def wait_until_live_attempted(self, sequence):
            assert sequence == futures_trade.sequence
            return futures_trade

        def is_fresh_current_item(self, item, **_kwargs):
            assert item == futures_trade
            return False

    async def fake_get_json(client, base_url, path, params):
        return {
            "/fapi/v1/openInterest": open_interest_payload(),
            "/fapi/v1/premiumIndex": premium_index_payload(),
        }[path]

    async def fake_upsert(pool, **kwargs):
        upserts.append(kwargs)

    monkeypatch.setattr(collector, "get_json", fake_get_json)
    monkeypatch.setattr(collector, "upsert_binance_futures_snapshot", fake_upsert)
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: 1_783_459_500_700)

    snapshot = asyncio.run(
        collector.collect_once(
            pool="pool",
            client="client",
            settings=futures_settings(),
            trade_state=ReconnectedTradeState(),
        )
    )

    assert snapshot.futures_last_price is None
    assert snapshot.futures_last_price_time_ms is None
    assert upserts[0]["futures_last_price"] is None


def test_futures_live_worker_coalesces_to_latest_and_writes_agg_trade_times(
    monkeypatch,
):
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("22222222-2222-2222-2222-222222222222")
        state.connection_opened(connection_id)
        first = sequenced_futures_trade(
            sequence=1,
            agg_trade_id=10,
            price="62075.11",
            trade_time_ms=1_783_459_500_100,
            received_ms=1_783_459_500_120,
            received_monotonic_ns=1_000_000_000,
            connection_id=connection_id,
        )
        latest = sequenced_futures_trade(
            sequence=2,
            agg_trade_id=11,
            price="62075.12",
            trade_time_ms=1_783_459_500_200,
            received_ms=1_783_459_500_220,
            received_monotonic_ns=1_100_000_000,
            connection_id=connection_id,
        )
        assert state.update_ws(first.trade, first.stamp) is not None
        latest = state.update_ws(latest.trade, latest.stamp)
        assert latest is not None
        writes = []

        class FakeLiveCache:
            async def set_price(self, key, **kwargs):
                writes.append((key, kwargs))

        monkeypatch.setattr(collector.time, "monotonic_ns", lambda: 1_100_000_001)
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 1_783_459_500_300,
        )
        worker = asyncio.create_task(
            collector.futures_live_worker(
                trade_state=state,
                live_cache=FakeLiveCache(),
            )
        )
        await asyncio.wait_for(
            state.wait_until_live_attempted(latest.sequence),
            timeout=0.5,
        )
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

        assert writes == [
            (
                FUTURES_LIVE_KEY,
                {
                    "value": Decimal("62075.12"),
                    "source_timestamp_ms": 1_783_459_500_200,
                    "received_ms": 1_783_459_500_220,
                },
            )
        ]
        fields = state.telemetry_fields(now_ms=1_783_459_500_400)
        assert fields["futures_live_attempted_sequence"] == 2
        assert fields["futures_live_successes_total"] == 1
        assert fields["futures_live_failures_total"] == 0

    asyncio.run(scenario())


def test_futures_live_worker_keeps_newest_trade_arriving_during_redis_write():
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("23232323-2323-2323-2323-232323232323")
        state.connection_opened(connection_id)
        first_write_started = asyncio.Event()
        release_first_write = asyncio.Event()
        writes = []

        class BlockingLiveCache:
            async def set_price(self, key, **kwargs):
                writes.append((key, kwargs))
                if len(writes) == 1:
                    first_write_started.set()
                    await release_first_write.wait()

        worker = asyncio.create_task(
            collector.futures_live_worker(
                trade_state=state,
                live_cache=BlockingLiveCache(),
            )
        )
        first = sequenced_futures_trade(
            agg_trade_id=10,
            price="62075.10",
            connection_id=connection_id,
        )
        first_item = state.update_ws(first.trade, first.stamp)
        assert first_item is not None
        await asyncio.wait_for(first_write_started.wait(), timeout=0.5)

        second = sequenced_futures_trade(
            agg_trade_id=11,
            price="62075.11",
            trade_time_ms=1_783_459_499_610,
            received_ms=1_783_459_499_640,
            received_monotonic_ns=1_100_000_000,
            connection_id=connection_id,
        )
        third = sequenced_futures_trade(
            agg_trade_id=12,
            price="62075.12",
            trade_time_ms=1_783_459_499_710,
            received_ms=1_783_459_499_740,
            received_monotonic_ns=1_200_000_000,
            connection_id=connection_id,
        )
        assert state.update_ws(second.trade, second.stamp) is not None
        third_item = state.update_ws(third.trade, third.stamp)
        assert third_item is not None
        release_first_write.set()
        await asyncio.wait_for(
            state.wait_until_live_attempted(third_item.sequence),
            timeout=0.5,
        )
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

        assert [write[1]["value"] for write in writes] == [
            Decimal("62075.10"),
            Decimal("62075.12"),
        ]

    asyncio.run(scenario())


def test_futures_live_worker_failure_unblocks_barrier_and_processes_next_trade(
    monkeypatch,
):
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("33333333-3333-3333-3333-333333333333")
        state.connection_opened(connection_id)
        writes = []

        class FlakyLiveCache:
            async def set_price(self, key, **kwargs):
                writes.append((key, kwargs))
                if len(writes) == 1:
                    raise OSError("redis unavailable")

        now_ns = [1_000_000_001]
        monkeypatch.setattr(collector.time, "monotonic_ns", lambda: now_ns[0])
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 1_783_459_500_300 + len(writes),
        )
        worker = asyncio.create_task(
            collector.futures_live_worker(
                trade_state=state,
                live_cache=FlakyLiveCache(),
            )
        )

        first = sequenced_futures_trade(
            agg_trade_id=10,
            received_monotonic_ns=1_000_000_000,
            connection_id=connection_id,
        )
        first_item = state.update_ws(first.trade, first.stamp)
        assert first_item is not None
        await asyncio.wait_for(
            state.wait_until_live_attempted(first_item.sequence),
            timeout=0.5,
        )

        now_ns[0] = 1_100_000_001
        second = sequenced_futures_trade(
            agg_trade_id=11,
            price="62075.13",
            trade_time_ms=1_783_459_499_610,
            received_ms=1_783_459_499_640,
            received_monotonic_ns=1_100_000_000,
            connection_id=connection_id,
        )
        second_item = state.update_ws(second.trade, second.stamp)
        assert second_item is not None
        await asyncio.wait_for(
            state.wait_until_live_attempted(second_item.sequence),
            timeout=0.5,
        )
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

        assert len(writes) == 2
        assert writes[-1][1]["value"] == Decimal("62075.13")
        fields = state.telemetry_fields(now_ms=1_783_459_500_400)
        assert fields["futures_live_attempts_total"] == 2
        assert fields["futures_live_successes_total"] == 1
        assert fields["futures_live_failures_total"] == 1

    asyncio.run(scenario())


def test_rest_snapshot_failure_does_not_block_independent_ws_live_delivery(
    monkeypatch,
):
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("44444444-4444-4444-4444-444444444444")
        state.connection_opened(connection_id)
        rest_started = asyncio.Event()
        release_rest = asyncio.Event()
        writes = []

        class FakeLiveCache:
            async def set_price(self, key, **kwargs):
                writes.append((key, kwargs))

        async def failing_get_json(client, base_url, path, params):
            rest_started.set()
            await release_rest.wait()
            raise RuntimeError(f"REST unavailable: {path}")

        monkeypatch.setattr(collector, "get_json", failing_get_json)
        monkeypatch.setattr(collector.time, "monotonic_ns", lambda: 1_000_000_001)
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 1_783_459_500_300,
        )

        worker = asyncio.create_task(
            collector.futures_live_worker(
                trade_state=state,
                live_cache=FakeLiveCache(),
            )
        )
        snapshot = asyncio.create_task(
            collector.collect_once(
                pool="pool",
                client="client",
                settings=futures_settings(),
                trade_state=state,
            )
        )
        await asyncio.wait_for(rest_started.wait(), timeout=0.5)

        trade = sequenced_futures_trade(connection_id=connection_id)
        item = state.update_ws(trade.trade, trade.stamp)
        assert item is not None
        await asyncio.wait_for(
            state.wait_until_live_attempted(item.sequence),
            timeout=0.5,
        )
        assert writes[0][0] == FUTURES_LIVE_KEY
        assert writes[0][1]["value"] == Decimal("62075.12")

        release_rest.set()
        with pytest.raises(RuntimeError, match="REST unavailable"):
            await snapshot
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

    asyncio.run(scenario())


@pytest.mark.parametrize("redis_fails", [False, True])
def test_collect_once_real_state_waits_for_live_attempt_before_postgres(
    monkeypatch,
    redis_fails,
):
    async def scenario():
        state = streams.FuturesTradeState()
        connection_id = UUID("45454545-4545-4545-4545-454545454545")
        state.connection_opened(connection_id)
        trade = sequenced_futures_trade(connection_id=connection_id)
        item = state.update_ws(trade.trade, trade.stamp)
        assert item is not None
        redis_started = asyncio.Event()
        release_redis = asyncio.Event()
        postgres_written = asyncio.Event()

        class ControlledLiveCache:
            async def set_price(self, key, **kwargs):
                redis_started.set()
                await release_redis.wait()
                if redis_fails:
                    raise OSError("redis unavailable")

        async def fake_get_json(client, base_url, path, params):
            return {
                "/fapi/v1/openInterest": open_interest_payload(),
                "/fapi/v1/premiumIndex": premium_index_payload(),
            }[path]

        async def fake_upsert(pool, **kwargs):
            postgres_written.set()

        monkeypatch.setattr(collector, "get_json", fake_get_json)
        monkeypatch.setattr(
            collector,
            "upsert_binance_futures_snapshot",
            fake_upsert,
        )
        monkeypatch.setattr(collector.time, "monotonic_ns", lambda: 1_000_000_001)
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 1_783_459_500_700,
        )

        worker = asyncio.create_task(
            collector.futures_live_worker(
                trade_state=state,
                live_cache=ControlledLiveCache(),
            )
        )
        await asyncio.wait_for(redis_started.wait(), timeout=0.5)
        snapshot_task = asyncio.create_task(
            collector.collect_once(
                pool="pool",
                client="client",
                settings=futures_settings(),
                trade_state=state,
            )
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(postgres_written.wait(), timeout=0.05)

        release_redis.set()
        snapshot = await asyncio.wait_for(snapshot_task, timeout=0.5)
        assert postgres_written.is_set()
        assert snapshot.futures_last_price == Decimal("62075.12")
        fields = state.telemetry_fields(now_ms=1_783_459_500_800)
        assert fields["futures_live_attempted_sequence"] == item.sequence
        assert fields["futures_live_failures_total"] == int(redis_fails)

        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

    asyncio.run(scenario())


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
        STALE_PRICE_MS=10_000,
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


def test_run_collector_rejects_disabled_streams_even_without_raw_capture(monkeypatch):
    monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
    monkeypatch.setattr(
        collector,
        "require_collector_database_url",
        lambda _settings: pytest.fail("database must not be opened"),
    )

    with pytest.raises(RuntimeError, match="requires BINANCE_FUTURES_STREAMS_ENABLED=true"):
        asyncio.run(
            collector.run_collector(
                run_settings(raw_enabled=False, streams_enabled=False)
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


def test_main_treats_collector_cancellation_as_clean_shutdown(monkeypatch):
    settings = object()

    def cancelled_run(coroutine):
        coroutine.close()
        raise asyncio.CancelledError

    monkeypatch.setattr(collector, "Settings", lambda: settings)
    monkeypatch.setattr(collector.asyncio, "run", cancelled_run)

    collector.main()


def test_run_collector_raw_disabled_still_wires_trade_state_and_live_worker(
    monkeypatch,
):
    async def scenario():
        live_started = asyncio.Event()
        events = []
        state = object()
        observed = {}

        class FakePool:
            async def close(self):
                events.append("pool_close")

        class FakeLiveCache:
            async def close(self):
                events.append("live_close")

        live_cache = FakeLiveCache()

        async def fake_create_pool(database_url):
            assert database_url == "postgresql://writer@localhost/price_collector"
            return FakePool()

        async def blocking_task(name):
            try:
                await asyncio.Event().wait()
            finally:
                events.append(f"{name}_stopped")

        async def fake_live_worker(**kwargs):
            observed["live"] = kwargs
            live_started.set()
            await blocking_task("live_worker")

        async def fake_agg_reader(settings, flow_store, **kwargs):
            observed["reader"] = kwargs
            await blocking_task("agg")

        async def fake_snapshot_loop(**kwargs):
            observed["snapshot"] = kwargs
            await blocking_task("snapshot")

        async def fake_flow_flush_loop(**kwargs):
            await blocking_task("flow_flush")

        async def fake_book_reader(settings, book_store):
            await blocking_task("book_reader")

        async def fake_book_flush_loop(**kwargs):
            await blocking_task("book_flush")

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "create_live_cache", lambda _settings: live_cache)
        monkeypatch.setattr(collector, "FuturesTradeState", lambda: state)
        monkeypatch.setattr(collector, "AsyncFlowAggregator", lambda **_kwargs: "flow")
        monkeypatch.setattr(collector, "AsyncBookTickerAggregator", lambda **_kwargs: "book")
        monkeypatch.setattr(collector, "futures_live_worker", fake_live_worker)
        monkeypatch.setattr(collector, "futures_agg_trade_reader_loop", fake_agg_reader)
        monkeypatch.setattr(collector, "snapshot_loop", fake_snapshot_loop)
        monkeypatch.setattr(collector, "futures_flow_flush_loop", fake_flow_flush_loop)
        monkeypatch.setattr(collector, "futures_book_ticker_reader_loop", fake_book_reader)
        monkeypatch.setattr(collector, "futures_book_flush_loop", fake_book_flush_loop)
        monkeypatch.setattr(collector.httpx, "AsyncClient", FakeAsyncClient)
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)
        monkeypatch.setattr(
            collector,
            "create_raw_capture_runtime",
            lambda **_kwargs: pytest.fail("disabled capture constructed a runtime"),
        )

        task = asyncio.create_task(
            collector.run_collector(run_settings(raw_enabled=False))
        )
        await asyncio.wait_for(live_started.wait(), timeout=0.5)
        assert observed["live"] == {
            "trade_state": state,
            "live_cache": live_cache,
        }
        assert observed["reader"] == {
            "trade_state": state,
            "raw_capture": None,
            "microstructure_sink": None,
        }
        assert observed["snapshot"]["trade_state"] is state
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events[-2:] == ["live_close", "pool_close"]
        assert {
            "live_worker_stopped",
            "agg_stopped",
            "snapshot_stopped",
            "flow_flush_stopped",
            "book_reader_stopped",
            "book_flush_stopped",
        }.issubset(events)

    asyncio.run(scenario())


def test_run_collector_microstructure_enabled_wires_and_runs_runtime(monkeypatch):
    async def scenario():
        events = []
        agg_started = asyncio.Event()
        snapshot_started = asyncio.Event()
        runtime_started = asyncio.Event()
        observed = {}
        sink = object()

        class FakePool:
            async def close(self):
                events.append("pool_close")

        class FakeLiveCache:
            async def close(self):
                events.append("live_close")

        class FakeMicrostructureRuntime:
            def __init__(self):
                self.sink = sink

            async def run(self):
                runtime_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    events.append("microstructure_stopped")

        settings = run_settings(raw_enabled=False)
        settings.BINANCE_MICROSTRUCTURE_ENABLED = True
        pool = FakePool()
        runtime = FakeMicrostructureRuntime()

        async def fake_create_pool(database_url):
            assert database_url == "postgresql://writer@localhost/price_collector"
            return pool

        def fake_create_microstructure_runtime(
            received_settings,
            received_pool,
            *,
            live_cache,
        ):
            observed["runtime_factory"] = (
                received_settings,
                received_pool,
                live_cache,
            )
            return runtime

        async def blocking_task(name):
            try:
                await asyncio.Event().wait()
            finally:
                events.append(f"{name}_stopped")

        async def fake_agg_reader(received_settings, flow_store, **kwargs):
            observed["reader"] = (received_settings, flow_store, kwargs)
            agg_started.set()
            await blocking_task("agg")

        async def fake_snapshot_loop(**kwargs):
            observed["snapshot"] = kwargs
            snapshot_started.set()
            await blocking_task("snapshot")

        async def fake_live_worker(**kwargs):
            await blocking_task("live_worker")

        async def fake_flow_flush_loop(**kwargs):
            await blocking_task("flow_flush")

        async def fake_book_reader(received_settings, book_store):
            await blocking_task("book_reader")

        async def fake_book_flush_loop(**kwargs):
            await blocking_task("book_flush")

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(
            collector, "create_live_cache", lambda _settings: FakeLiveCache()
        )
        monkeypatch.setattr(
            collector,
            "create_microstructure_runtime",
            fake_create_microstructure_runtime,
        )
        monkeypatch.setattr(collector, "FuturesTradeState", object)
        monkeypatch.setattr(collector, "AsyncFlowAggregator", lambda **_kwargs: "flow")
        monkeypatch.setattr(
            collector, "AsyncBookTickerAggregator", lambda **_kwargs: "book"
        )
        monkeypatch.setattr(collector, "futures_live_worker", fake_live_worker)
        monkeypatch.setattr(collector, "futures_agg_trade_reader_loop", fake_agg_reader)
        monkeypatch.setattr(collector, "snapshot_loop", fake_snapshot_loop)
        monkeypatch.setattr(collector, "futures_flow_flush_loop", fake_flow_flush_loop)
        monkeypatch.setattr(
            collector, "futures_book_ticker_reader_loop", fake_book_reader
        )
        monkeypatch.setattr(collector, "futures_book_flush_loop", fake_book_flush_loop)
        monkeypatch.setattr(collector.httpx, "AsyncClient", FakeAsyncClient)
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)
        monkeypatch.setattr(
            collector,
            "create_raw_capture_runtime",
            lambda **_kwargs: pytest.fail("disabled capture constructed a runtime"),
        )

        task = asyncio.create_task(collector.run_collector(settings))
        await asyncio.wait_for(agg_started.wait(), timeout=0.5)
        await asyncio.wait_for(snapshot_started.wait(), timeout=0.5)
        await asyncio.wait_for(runtime_started.wait(), timeout=0.5)

        assert observed["runtime_factory"][:2] == (settings, pool)
        assert isinstance(observed["runtime_factory"][2], FakeLiveCache)
        assert observed["reader"][0] is settings
        assert observed["reader"][1] == "flow"
        assert observed["reader"][2]["microstructure_sink"] is sink
        assert observed["snapshot"]["microstructure_sink"] is sink

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert "microstructure_stopped" in events
        assert events.index("microstructure_stopped") < events.index("live_close")
        assert events[-2:] == ["live_close", "pool_close"]

    asyncio.run(scenario())


def test_run_collector_wires_lazy_raw_runtime_and_closes_after_tasks(monkeypatch):
    async def scenario():
        events = []
        agg_started = asyncio.Event()
        live_started = asyncio.Event()
        telemetry_started = asyncio.Event()
        runtime_kwargs = {}
        reader_kwargs = {}
        live_kwargs = {}
        snapshot_kwargs = {}
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

        class FakeState:
            pass

        raw_runtime = FakeRawRuntime()
        state = FakeState()

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
            snapshot_kwargs.update(kwargs)
            await blocking_task("snapshot")

        async def fake_live_worker(**kwargs):
            live_kwargs.update(kwargs)
            live_started.set()
            await blocking_task("live_worker")

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
        monkeypatch.setattr(collector, "FuturesTradeState", lambda: state)
        monkeypatch.setattr(collector, "AsyncFlowAggregator", lambda **_kwargs: "flow")
        monkeypatch.setattr(collector, "AsyncBookTickerAggregator", lambda **_kwargs: "book")
        monkeypatch.setattr(collector, "futures_live_worker", fake_live_worker)
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
        await asyncio.wait_for(live_started.wait(), timeout=0.5)
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
            "trade_state": state,
            "raw_capture": raw_runtime,
            "microstructure_sink": None,
        }
        assert live_kwargs["trade_state"] is state
        assert snapshot_kwargs["trade_state"] is state
        assert telemetry_kwargs == {
            "raw_capture": raw_runtime,
            "trade_state": state,
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
                "live_worker",
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
