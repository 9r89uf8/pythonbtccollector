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
