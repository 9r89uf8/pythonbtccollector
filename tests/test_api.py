from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import price_collector.api as api
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    CHAINLINK_SHADOW_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LiveCachePayloadError,
    LivePrice,
    LiveShadowSignal,
)
from price_collector.market import MARKET_MS
from price_collector.shadow_signal_2s_live import LiveShadowSignal2s
from price_collector.shadow_signal_reporting import ShadowEvaluationFetchResult


class FakePool:
    def __init__(self) -> None:
        self.closed = False
        self.acquire_calls = 0

    async def close(self) -> None:
        self.closed = True

    def acquire(self):
        self.acquire_calls += 1
        raise AssertionError("test endpoint should not acquire PostgreSQL")


class FakeLiveCache:
    def __init__(self) -> None:
        self.closed = False
        self.prices = {}
        self.shadow_signal = None
        self.read_error = None
        self.requested_keys = []

    async def get_prices_and_shadow_signal(self, keys):
        if self.read_error is not None:
            raise self.read_error
        key_list = list(keys)
        self.requested_keys.append([*key_list, CHAINLINK_SHADOW_LIVE_KEY])
        return (
            {key: self.prices.get(key) for key in key_list},
            self.shadow_signal,
        )

    async def close(self) -> None:
        self.closed = True


class FakeShadowSignal2sStore:
    def __init__(self) -> None:
        self.closed = False
        self.signal = None
        self.read_error = None
        self.get_calls = 0

    async def get_signal(self):
        self.get_calls += 1
        if self.read_error is not None:
            raise self.read_error
        return self.signal

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def client(monkeypatch):
    fake_pool = FakePool()
    fake_live_cache = FakeLiveCache()
    fake_shadow_signal_2s_store = FakeShadowSignal2sStore()

    async def fake_create_read_pool(settings):
        return fake_pool

    async def fake_health_check(pool):
        return None

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://price_reader:secret@127.0.0.1:5432/price_collector",
    )
    monkeypatch.setattr(api, "create_read_pool", fake_create_read_pool)
    monkeypatch.setattr(api, "create_live_cache", lambda settings: fake_live_cache)
    monkeypatch.setattr(
        api,
        "create_shadow_signal_2s_store",
        lambda settings: fake_shadow_signal_2s_store,
    )
    monkeypatch.setattr(api, "health_check", fake_health_check)

    with TestClient(api.app) as test_client:
        test_client.fake_pool = fake_pool
        test_client.fake_live_cache = fake_live_cache
        test_client.fake_shadow_signal_2s_store = fake_shadow_signal_2s_store
        yield test_client

    assert fake_shadow_signal_2s_store.closed is True


def utc_dt(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def live_shadow_signal(**overrides):
    values = {
        "schema_version": 1,
        "mode": "shadow",
        "selection_schema_version": 2,
        "selection_policy_version": "chronological_holdout_v2",
        "selection_fingerprint_sha256": "a" * 64,
        "selection_artifact_sha256": "b" * 64,
        "selection_evidence_end_ms": 1_783_400_000_000,
        "model_version": "catchup_ratio_l3000_b100",
        "beta": Decimal("1"),
        "generated_ms": 1_783_459_250_100,
        "valid": True,
        "status": "valid",
        "invalid_reasons": (),
        "state": "anchored",
        "horizon_ms": 3_000,
        "estimated_lag_ms": 3_000,
        "current_chainlink": Decimal("62000"),
        "projected_chainlink": Decimal("62001"),
        "pending_move": Decimal("1"),
        "pending_move_bps": Decimal("0.1612903225806451612903225806"),
        "direction": "up",
        "futures_now": Decimal("62101"),
        "futures_reference": Decimal("62100"),
        "chainlink_now_source_timestamp_ms": 1_783_459_250_000,
        "chainlink_now_received_ms": 1_783_459_250_070,
        "anchor_chainlink_source_timestamp_ms": 1_783_459_250_000,
        "anchor_chainlink_received_ms": 1_783_459_250_070,
        "futures_now_source_timestamp_ms": 1_783_459_250_050,
        "futures_now_received_ms": 1_783_459_250_090,
        "futures_reference_source_timestamp_ms": 1_783_459_247_000,
        "futures_reference_received_ms": 1_783_459_247_050,
        "futures_reference_target_ms": 1_783_459_247_070,
        "futures_reference_gap_ms": 20,
        "futures_received_age_ms": 10,
        "chainlink_received_age_ms": 30,
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "ms_to_market_end": 249_900,
        "full_horizon_before_market_end": True,
    }
    values.update(overrides)
    return LiveShadowSignal(**values)


def live_shadow_signal_2s(**overrides):
    values = {
        "schema_version": 1,
        "mode": "shadow_candidate",
        "publication_role": "challenger",
        "experiment_version": "prospective_catchup_2s_v1",
        "model_version": "catchup_v1_l2000_h2000_b100",
        "beta": Decimal("1"),
        "futures_lookback_ms": 2_000,
        "forecast_horizon_ms": 2_000,
        "generated_ms": 1_783_459_250_100,
        "target_ms": 1_783_459_252_100,
        "valid": True,
        "status": "valid",
        "invalid_reasons": (),
        "state": "anchored",
        "current_chainlink": Decimal("62000"),
        "projected_chainlink": Decimal("62000.99838969404186795491142"),
        "pending_move": Decimal("0.99838969404186795491142"),
        "pending_move_bps": Decimal("0.1610305958132045088566806452"),
        "direction": "up",
        "futures_now": Decimal("62101"),
        "futures_reference": Decimal("62100"),
        "chainlink_now_source_timestamp_ms": 1_783_459_250_000,
        "chainlink_now_received_ms": 1_783_459_250_070,
        "anchor_chainlink_source_timestamp_ms": 1_783_459_250_000,
        "anchor_chainlink_received_ms": 1_783_459_250_070,
        "futures_now_source_timestamp_ms": 1_783_459_250_050,
        "futures_now_received_ms": 1_783_459_250_090,
        "futures_reference_source_timestamp_ms": 1_783_459_248_000,
        "futures_reference_received_ms": 1_783_459_248_050,
        "futures_reference_target_ms": 1_783_459_248_070,
        "futures_reference_gap_ms": 20,
        "futures_received_age_ms": 10,
        "chainlink_received_age_ms": 30,
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "ms_to_market_end": 249_900,
        "full_horizon_before_market_end": True,
    }
    values.update(overrides)
    return LiveShadowSignal2s(**values)


def test_healthz_success(client):
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "database": "ok",
        "service": "price-api",
    }


def test_healthz_returns_503_when_database_check_fails(client, monkeypatch):
    async def fake_health_check(pool):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(api, "health_check", fake_health_check)

    response = client.get("/healthz")

    assert response.status_code == 503
    assert response.json() == {
        "ok": False,
        "database": "error",
        "service": "price-api",
        "error": "database unavailable",
    }


def test_prices_latest_returns_decimal_price_as_string_and_z_datetime(client, monkeypatch):
    async def fake_fetch_latest_price(pool, provider_code, symbol):
        assert provider_code == "binance_spot"
        assert symbol == "BTCUSDT"
        return {
            "provider": "binance_spot",
            "symbol": "BTCUSDT",
            "price": Decimal("123456.780000000000000000"),
            "sample_second_ms": 1_783_459_200_000,
            "sample_second_at": utc_dt(2026, 7, 7, 21, 0, 0),
            "provider_event_ms": 1_783_459_199_876,
            "received_ms": 1_783_459_199_900,
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
        }

    monkeypatch.setattr(api, "fetch_latest_price", fake_fetch_latest_price)

    response = client.get("/prices/latest")

    assert response.status_code == 200
    body = response.json()
    assert body["price"] == "123456.780000000000000000"
    assert isinstance(body["price"], str)
    assert body["sample_second_at"] == "2026-07-07T21:00:00Z"


def test_prices_latest_can_query_polymarket_chainlink_btcusd(client, monkeypatch):
    async def fake_fetch_latest_price(pool, provider_code, symbol):
        assert provider_code == "polymarket_chainlink_rtds"
        assert symbol == "BTCUSD"
        return {
            "provider": "polymarket_chainlink_rtds",
            "symbol": "BTCUSD",
            "price": Decimal("123455.900000000000000000"),
            "sample_second_ms": 1_783_459_200_000,
            "sample_second_at": utc_dt(2026, 7, 7, 21, 0, 0),
            "provider_event_ms": 1_783_459_200_123,
            "received_ms": 1_783_459_200_250,
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
        }

    monkeypatch.setattr(api, "fetch_latest_price", fake_fetch_latest_price)

    response = client.get(
        "/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["provider"] == "polymarket_chainlink_rtds"
    assert body["symbol"] == "BTCUSD"
    assert body["price"] == "123455.900000000000000000"
    assert body["provider_event_ms"] == 1_783_459_200_123


def test_prices_latest_returns_404_when_no_sample_exists(client, monkeypatch):
    async def fake_fetch_latest_price(pool, provider_code, symbol):
        return None

    monkeypatch.setattr(api, "fetch_latest_price", fake_fetch_latest_price)

    response = client.get("/prices/latest")

    assert response.status_code == 404


def test_markets_latest_returns_ohlc_sample_count_and_samples(client, monkeypatch):
    async def fake_fetch_latest_market_id(pool, provider_code, symbol):
        assert provider_code == "binance_spot"
        assert symbol == "BTCUSDT"
        return 5_944_864

    async def fake_fetch_market_summary(pool, provider_code, symbol, market_id):
        assert market_id == 5_944_864
        return {
            "provider": "binance_spot",
            "symbol": "BTCUSDT",
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": utc_dt(2026, 7, 7, 21, 0, 0),
            "market_end_at": utc_dt(2026, 7, 7, 21, 5, 0),
            "is_complete": False,
            "sample_count": 3,
            "open": Decimal("123000.000000000000000000"),
            "high": Decimal("123500.000000000000000000"),
            "low": Decimal("122900.000000000000000000"),
            "close": Decimal("123456.780000000000000000"),
            "samples": [
                {
                    "sample_second_ms": 1_783_459_200_000,
                    "sample_second_at": utc_dt(2026, 7, 7, 21, 0, 0),
                    "price": Decimal("123000.000000000000000000"),
                },
                {
                    "sample_second_ms": 1_783_459_201_000,
                    "sample_second_at": utc_dt(2026, 7, 7, 21, 0, 1),
                    "price": Decimal("123500.000000000000000000"),
                },
                {
                    "sample_second_ms": 1_783_459_202_000,
                    "sample_second_at": utc_dt(2026, 7, 7, 21, 0, 2),
                    "price": Decimal("123456.780000000000000000"),
                },
            ],
        }

    monkeypatch.setattr(api, "fetch_latest_market_id", fake_fetch_latest_market_id)
    monkeypatch.setattr(api, "fetch_market_summary", fake_fetch_market_summary)
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_300_000)

    response = client.get("/markets/latest")

    assert response.status_code == 200
    body = response.json()
    assert body["market_id"] == 5_944_864
    assert body["market_start_at"] == "2026-07-07T21:00:00Z"
    assert body["market_end_at"] == "2026-07-07T21:05:00Z"
    assert body["is_complete"] is False
    assert body["sample_count"] == 3
    assert body["open"] == "123000.000000000000000000"
    assert body["high"] == "123500.000000000000000000"
    assert body["low"] == "122900.000000000000000000"
    assert body["close"] == "123456.780000000000000000"
    assert [sample["sample_second_ms"] for sample in body["samples"]] == [
        1_783_459_200_000,
        1_783_459_201_000,
        1_783_459_202_000,
    ]
    assert body["samples"][0]["price"] == "123000.000000000000000000"
    assert body["samples"][0]["sample_second_at"] == "2026-07-07T21:00:00Z"


def test_markets_index_defaults_to_three_completed_markets(client, monkeypatch):
    async def fake_fetch_recent_market_windows(
        pool,
        *,
        server_time_ms,
        include_current,
        before_market_id,
        limit,
    ):
        assert pool is client.fake_pool
        assert server_time_ms == 1_783_460_100_000
        assert include_current is False
        assert before_market_id is None
        assert limit == 4
        return [
            {
                "market_id": 5_944_866,
                "market_start_ms": 1_783_459_800_000,
                "market_end_ms": 1_783_460_100_000,
                "market_start_at": utc_dt(2026, 7, 7, 21, 10, 0),
                "market_end_at": utc_dt(2026, 7, 7, 21, 15, 0),
                "binance_sample_count": 300,
                "chainlink_sample_count": 298,
                "futures_sample_count": 60,
                "open_interest_sample_count": 60,
                "flow_sample_count": 300,
                "book_sample_count": 299,
                "probability_sample_count": 297,
            },
            {
                "market_id": 5_944_865,
                "market_start_ms": 1_783_459_500_000,
                "market_end_ms": 1_783_459_800_000,
                "market_start_at": utc_dt(2026, 7, 7, 21, 5, 0),
                "market_end_at": utc_dt(2026, 7, 7, 21, 10, 0),
                "binance_sample_count": 300,
                "chainlink_sample_count": 299,
                "futures_sample_count": 60,
                "open_interest_sample_count": 59,
                "flow_sample_count": 300,
                "book_sample_count": 300,
                "probability_sample_count": 296,
            },
            {
                "market_id": 5_944_864,
                "market_start_ms": 1_783_459_200_000,
                "market_end_ms": 1_783_459_500_000,
                "market_start_at": utc_dt(2026, 7, 7, 21, 0, 0),
                "market_end_at": utc_dt(2026, 7, 7, 21, 5, 0),
                "binance_sample_count": 299,
                "chainlink_sample_count": 297,
                "futures_sample_count": 60,
                "open_interest_sample_count": 60,
                "flow_sample_count": 298,
                "book_sample_count": 300,
                "probability_sample_count": 295,
            },
            {
                "market_id": 5_944_863,
                "market_start_ms": 1_783_458_900_000,
                "market_end_ms": 1_783_459_200_000,
                "market_start_at": utc_dt(2026, 7, 7, 20, 55, 0),
                "market_end_at": utc_dt(2026, 7, 7, 21, 0, 0),
            },
        ]

    monkeypatch.setattr(
        api,
        "fetch_recent_market_windows",
        fake_fetch_recent_market_windows,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_460_100_000)

    response = client.get("/markets")

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 1
    assert body["server_time_ms"] == 1_783_460_100_000
    assert [market["market_id"] for market in body["markets"]] == [
        5_944_866,
        5_944_865,
        5_944_864,
    ]
    assert body["markets"][0] == {
        "market_id": 5_944_866,
        "market_start_ms": 1_783_459_800_000,
        "market_end_ms": 1_783_460_100_000,
        "market_start_at": "2026-07-07T21:10:00Z",
        "market_end_at": "2026-07-07T21:15:00Z",
        "is_complete": True,
        "availability": {
            "binance": 300,
            "chainlink": 298,
            "futures": 60,
            "open_interest": 60,
            "flow": 300,
            "book": 299,
            "probabilities": 297,
        },
    }
    assert body["next_before_market_id"] == 5_944_864


def test_markets_index_passes_include_current_and_exclusive_cursor(
    client,
    monkeypatch,
):
    async def fake_fetch_recent_market_windows(
        pool,
        *,
        server_time_ms,
        include_current,
        before_market_id,
        limit,
    ):
        assert pool is client.fake_pool
        assert server_time_ms == 1_783_459_920_123
        assert include_current is True
        assert before_market_id == 5_944_867
        assert limit == 3
        return [
            {
                "market_id": 5_944_866,
                "market_start_ms": 1_783_459_800_000,
                "market_end_ms": 1_783_460_100_000,
                "market_start_at": utc_dt(2026, 7, 7, 21, 10, 0),
                "market_end_at": utc_dt(2026, 7, 7, 21, 15, 0),
                "binance_sample_count": 120,
            }
        ]

    monkeypatch.setattr(
        api,
        "fetch_recent_market_windows",
        fake_fetch_recent_market_windows,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_920_123)

    response = client.get(
        "/markets?limit=2&include_current=true&before_market_id=5944867"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["markets"][0]["market_id"] == 5_944_866
    assert body["markets"][0]["is_complete"] is False
    assert body["markets"][0]["availability"] == {
        "binance": 120,
        "chainlink": 0,
        "futures": 0,
        "open_interest": 0,
        "flow": 0,
        "book": 0,
        "probabilities": 0,
    }
    assert body["next_before_market_id"] is None


def test_markets_index_returns_empty_list_with_200(client, monkeypatch):
    async def fake_fetch_recent_market_windows(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs["limit"] == 4
        return []

    monkeypatch.setattr(
        api,
        "fetch_recent_market_windows",
        fake_fetch_recent_market_windows,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_920_123)

    response = client.get("/markets")

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": 1,
        "server_time_ms": 1_783_459_920_123,
        "markets": [],
        "next_before_market_id": None,
    }


@pytest.mark.parametrize("limit", [0, 51])
def test_markets_index_rejects_out_of_range_limits(client, monkeypatch, limit):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid limit must be rejected before database access")

    monkeypatch.setattr(api, "fetch_recent_market_windows", unexpected_fetch)

    response = client.get(f"/markets?limit={limit}")

    assert response.status_code == 422


def test_markets_by_id_returns_404_when_no_samples_exist(client, monkeypatch):
    async def fake_fetch_market_summary(pool, provider_code, symbol, market_id):
        assert market_id == 5_944_864
        return None

    monkeypatch.setattr(api, "fetch_market_summary", fake_fetch_market_summary)

    response = client.get("/markets/5944864")

    assert response.status_code == 404


def test_markets_sources_by_id_returns_both_sources(client, monkeypatch):
    async def fake_fetch_market_summaries_for_btc_sources(pool, market_id):
        assert market_id == 5_944_864
        return {
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": utc_dt(2026, 7, 7, 21, 0, 0),
            "market_end_at": utc_dt(2026, 7, 7, 21, 5, 0),
            "sources": [
                {
                    "provider": "binance_spot",
                    "symbol": "BTCUSDT",
                    "quote_asset": "USDT",
                    "sample_count": 300,
                    "open": Decimal("123000.000000000000000000"),
                    "high": Decimal("123500.000000000000000000"),
                    "low": Decimal("122900.000000000000000000"),
                    "close": Decimal("123456.780000000000000000"),
                    "latest_sample_second_ms": 1_783_459_499_000,
                    "latest_provider_event_ms": 1_783_459_498_950,
                    "latest_received_ms": 1_783_459_499_010,
                },
                {
                    "provider": "polymarket_chainlink_rtds",
                    "symbol": "BTCUSD",
                    "quote_asset": "USD",
                    "sample_count": 298,
                    "open": Decimal("122998.120000000000000000"),
                    "high": Decimal("123501.990000000000000000"),
                    "low": Decimal("122901.030000000000000000"),
                    "close": Decimal("123455.900000000000000000"),
                    "latest_sample_second_ms": 1_783_459_499_000,
                    "latest_provider_event_ms": 1_783_459_499_123,
                    "latest_received_ms": 1_783_459_499_320,
                },
            ],
        }

    monkeypatch.setattr(
        api,
        "fetch_market_summaries_for_btc_sources",
        fake_fetch_market_summaries_for_btc_sources,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_300_000)

    response = client.get("/markets/5944864/sources")

    assert response.status_code == 200
    body = response.json()
    assert body["market_id"] == 5_944_864
    assert body["is_complete"] is False
    assert [source["provider"] for source in body["sources"]] == [
        "binance_spot",
        "polymarket_chainlink_rtds",
    ]
    assert body["sources"][0]["symbol"] == "BTCUSDT"
    assert body["sources"][0]["quote_asset"] == "USDT"
    assert body["sources"][0]["sample_count"] == 300
    assert body["sources"][1]["symbol"] == "BTCUSD"
    assert body["sources"][1]["quote_asset"] == "USD"
    assert body["sources"][1]["sample_count"] == 298
    assert body["sources"][1]["close"] == "123455.900000000000000000"


def test_markets_current_sources_uses_current_five_minute_market(client, monkeypatch):
    async def fake_fetch_market_summaries_for_btc_sources(pool, market_id):
        assert market_id == 5_944_864
        return {
            "market_id": market_id,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": utc_dt(2026, 7, 7, 21, 0, 0),
            "market_end_at": utc_dt(2026, 7, 7, 21, 5, 0),
            "sources": [
                {
                    "provider": "binance_spot",
                    "symbol": "BTCUSDT",
                    "quote_asset": "USDT",
                    "sample_count": 1,
                    "open": Decimal("123000.00"),
                    "high": Decimal("123000.00"),
                    "low": Decimal("123000.00"),
                    "close": Decimal("123000.00"),
                    "latest_sample_second_ms": 1_783_459_200_000,
                    "latest_provider_event_ms": 1_783_459_199_950,
                    "latest_received_ms": 1_783_459_200_010,
                }
            ],
        }

    monkeypatch.setattr(
        api,
        "fetch_market_summaries_for_btc_sources",
        fake_fetch_market_summaries_for_btc_sources,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get("/markets/current/sources")

    assert response.status_code == 200
    assert response.json()["market_id"] == 5_944_864


def market_data_payload(
    include_probabilities=False,
    include_futures=False,
    include_oi=False,
    include_flow=False,
    include_book=False,
):
    row = {
        "t": 0,
        "timestamp_ms": 1_783_459_200_000,
        "timestamp_at": "2026-07-07T21:00:00Z",
        "prices": {
            "binance": "123000.00",
            "chainlink": "122998.12",
        },
        "freshness": {
            "binance": {
                "source_age_ms": 250,
                "received_age_ms": 200,
            },
            "chainlink": {
                "source_age_ms": 300,
                "received_age_ms": 225,
            },
        },
    }
    if include_probabilities:
        row["probabilities"] = {
            "up": {
                "bid": "0.47",
                "ask": "0.49",
                "mid": "0.48",
                "normalized": "0.48241206",
            },
            "down": {
                "bid": "0.50",
                "ask": "0.53",
                "mid": "0.515",
                "normalized": "0.51758794",
            },
        }

    if include_futures:
        row["futures"] = {
            "last": "62075.12",
            "mark": "62074.88",
            "index": "62070.19",
            "premium_bps": "0.76",
        }

    if include_flow:
        row["flow"] = {
            "buy_quote": "1000.00",
            "sell_quote": "250.00",
            "delta_quote": "750.00",
            "total_quote": "1250.00",
            "taker_imbalance": "0.60000000",
            "cvd_10s": "900.123456000000000000",
            "cvd_30s": "1200.129000000000000000",
            "imbalance_10s": "0.12345678",
            "imbalance_30s": "-0.23456789",
            "agg_trade_count": 4,
            "max_trade_quote": "777.770000000000000000",
        }

    if include_book:
        row["book"] = {
            "bid": "62074.10",
            "ask": "62074.20",
            "spread_bps": "0.01610935",
            "book_imbalance": "0.25000000",
            "microprice": "62074.166789000000000000",
            "update_id": 123456,
        }

    payload = {
        "schema_version": 2,
        "market": {
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": "2026-07-07T21:00:00Z",
            "market_end_at": "2026-07-07T21:05:00Z",
            "seconds_expected": 300,
            "chainlink_resolution": {
                "open": None,
                "close": None,
                "status": "pending",
                "source": None,
            },
            "resolution": {
                "status": "pending",
                "resolution_type": None,
                "winner": None,
                "winning_token_id": None,
                "resolved_at_ms": None,
                "official_payouts": {"up": None, "down": None},
                "source": None,
            },
        },
        "series": [row],
    }

    if include_oi:
        row["open_interest"] = {
            "contracts": "74321.123",
            "notional_usdt": "4616789012.34",
            "delta_30s": None,
            "delta_60s": None,
            "delta_300s": None,
        }
        payload["previous_5m_oi_summary"] = {
            "source_window_start_ms": 1_783_458_900_000,
            "source_window_end_ms": 1_783_459_200_000,
            "effective_market_id": 5_944_864,
            "sum_open_interest": "74000.123",
            "sum_open_interest_value": "4590000000.13",
        }

    return payload


def test_markets_current_data_uses_current_five_minute_market(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert server_time_ms == 1_783_459_250_123
        assert include_probabilities is False
        assert include_futures is False
        assert include_oi is False
        assert include_flow is False
        assert include_book is False
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        payload = market_data_payload()
        payload["market"]["chainlink_resolution"] = {
            "open": "63337.115841440165",
            "close": "63336.71900847139",
            "status": "official",
            "source": "polymarket_gamma_event_metadata",
        }
        return payload

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get("/markets/current/data")

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 2
    assert body["market"]["market_id"] == 5_944_864
    assert body["market"]["market_start_ms"] == 1_783_459_200_000
    assert body["market"]["market_end_ms"] == 1_783_459_500_000
    assert body["market"]["chainlink_resolution"]["open"] == (
        "63337.115841440165"
    )
    assert body["series"][0]["timestamp_ms"] == 1_783_459_200_000
    assert "probabilities" not in body["series"][0]
    assert "futures" not in body["series"][0]
    assert "open_interest" not in body["series"][0]


def test_markets_data_by_id_can_include_probabilities(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert isinstance(server_time_ms, int)
        assert include_probabilities is True
        assert include_futures is False
        assert include_oi is False
        assert include_flow is False
        assert include_book is False
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        return market_data_payload(include_probabilities=True)

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get("/markets/5944864/data?include_probabilities=true")

    assert response.status_code == 200
    body = response.json()
    assert body["series"][0]["probabilities"]["up"]["mid"] == "0.48"


def test_markets_data_by_id_can_include_futures_and_oi(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert isinstance(server_time_ms, int)
        assert include_probabilities is False
        assert include_futures is True
        assert include_oi is True
        assert include_flow is False
        assert include_book is False
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        return market_data_payload(include_futures=True, include_oi=True)

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get("/markets/5944864/data?include_futures=true&include_oi=true")

    assert response.status_code == 200
    body = response.json()
    assert body["series"][0]["futures"]["mark"] == "62074.88"
    assert body["series"][0]["open_interest"]["contracts"] == "74321.123"
    assert body["previous_5m_oi_summary"]["sum_open_interest"] == "74000.123"


def test_markets_data_by_id_can_include_futures_flow_and_book(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert isinstance(server_time_ms, int)
        assert include_probabilities is False
        assert include_futures is False
        assert include_oi is False
        assert include_flow is True
        assert include_book is True
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        return market_data_payload(include_flow=True, include_book=True)

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get("/markets/5944864/data?include_flow=true&include_book=true")

    assert response.status_code == 200
    body = response.json()
    assert body["series"][0]["flow"]["delta_quote"] == "750.00"
    assert body["series"][0]["flow"]["taker_imbalance"] == "0.60000000"
    assert body["series"][0]["book"]["spread_bps"] == "0.01610935"
    assert body["series"][0]["book"]["book_imbalance"] == "0.25000000"


def test_openapi_lists_futures_flow_and_book_include_flags(client):
    response = client.get("/openapi.json")

    assert response.status_code == 200
    schema = response.json()
    current_data_params = {
        param["name"]
        for param in schema["paths"]["/markets/current/data"]["get"]["parameters"]
    }
    market_data_params = {
        param["name"]
        for param in schema["paths"]["/markets/{market_id}/data"]["get"]["parameters"]
    }

    assert "include_flow" in current_data_params
    assert "include_book" in current_data_params
    assert "include_flow" in market_data_params
    assert "include_book" in market_data_params


def test_markets_current_data_passes_display_fill_options(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert server_time_ms == 1_783_459_250_123
        assert include_probabilities is False
        assert include_futures is False
        assert include_oi is False
        assert include_flow is False
        assert include_book is False
        assert fill_display is True
        assert max_carry_forward_ms == 5_000
        return market_data_payload()

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get("/markets/current/data?fill_display=true&max_carry_forward_ms=5000")

    assert response.status_code == 200


def test_markets_current_live_reads_redis_without_postgres_queries(client, monkeypatch):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)
    client.fake_live_cache.prices = {
        BINANCE_SPOT_LIVE_KEY: LivePrice(
            value="62067.89",
            source_timestamp_ms=1_783_459_249_900,
            received_ms=1_783_459_249_950,
        ),
        CHAINLINK_LIVE_KEY: LivePrice(
            value="62037.05",
            source_timestamp_ms=1_783_459_247_000,
            received_ms=1_783_459_247_100,
        ),
        FUTURES_LIVE_KEY: LivePrice(
            value="62099.10",
            source_timestamp_ms=1_783_459_250_000,
            received_ms=1_783_459_250_050,
        ),
    }

    response = client.get("/markets/current/live?max_chainlink_carry_forward_ms=7000")

    assert response.status_code == 200
    body = response.json()
    assert body["server_time_ms"] == 1_783_459_250_123
    assert body["market_id"] == 5_944_864
    assert body["prices"]["binance_spot"]["value"] == "62067.89"
    assert body["prices"]["binance_spot"]["source_timestamp_ms"] == 1_783_459_249_900
    assert body["prices"]["binance_spot"]["provider_event_ms"] == 1_783_459_249_900
    assert body["prices"]["binance_spot"]["source_age_ms"] == 223
    assert body["prices"]["binance_spot"]["received_age_ms"] == 173
    assert body["prices"]["chainlink"]["source_age_ms"] == 3_123
    assert body["futures"]["last"]["source_age_ms"] == 123
    assert body["futures"]["last"]["received_age_ms"] == 73
    assert body["futures"]["last"]["time_ms"] == 1_783_459_250_000
    assert body["signals"] == {"chainlink_catchup": None}
    assert client.fake_live_cache.requested_keys == [
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            FUTURES_LIVE_KEY,
            CHAINLINK_SHADOW_LIVE_KEY,
        ]
    ]
    assert client.fake_shadow_signal_2s_store.get_calls == 0
    assert client.fake_pool.acquire_calls == 0


def test_markets_current_live_returns_full_shadow_signal(client, monkeypatch):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)
    client.fake_live_cache.shadow_signal = live_shadow_signal()

    response = client.get("/markets/current/live")

    assert response.status_code == 200
    signal = response.json()["signals"]["chainlink_catchup"]
    assert signal["schema_version"] == 1
    assert signal["mode"] == "shadow"
    assert signal["model_version"] == "catchup_ratio_l3000_b100"
    assert signal["signal_age_ms"] == 23
    assert signal["valid"] is True
    assert signal["invalid_reasons"] == []
    assert signal["pending_move"] == "1"
    assert signal["pending_move_bps"] == (
        "0.1612903225806451612903225806"
    )
    assert client.fake_pool.acquire_calls == 0


def test_markets_current_live_returns_well_formed_invalid_signal(
    client,
    monkeypatch,
):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)
    client.fake_live_cache.shadow_signal = replace(
        live_shadow_signal(),
        valid=False,
        status="futures_stale",
        invalid_reasons=("futures_stale",),
        projected_chainlink=None,
        pending_move=None,
        pending_move_bps=None,
        direction=None,
        futures_reference=None,
        anchor_chainlink_source_timestamp_ms=None,
        anchor_chainlink_received_ms=None,
        futures_reference_source_timestamp_ms=None,
        futures_reference_received_ms=None,
        futures_reference_target_ms=None,
        futures_reference_gap_ms=None,
    )

    response = client.get("/markets/current/live")

    assert response.status_code == 200
    signal = response.json()["signals"]["chainlink_catchup"]
    assert signal["valid"] is False
    assert signal["status"] == "futures_stale"
    assert signal["invalid_reasons"] == ["futures_stale"]
    assert signal["projected_chainlink"] is None


@pytest.mark.parametrize(
    ("error", "detail"),
    (
        (OSError("redis unavailable"), "live cache unavailable"),
        (LiveCachePayloadError("bad price"), "live cache payload invalid"),
    ),
)
def test_markets_current_live_preserves_cache_error_status(
    client,
    error,
    detail,
):
    client.fake_live_cache.read_error = error

    response = client.get("/markets/current/live")

    assert response.status_code == 503
    assert response.json() == {"detail": detail}
    assert client.fake_pool.acquire_calls == 0


def test_chainlink_catchup_2s_challenger_is_redis_only(client, monkeypatch):
    client.fake_shadow_signal_2s_store.signal = live_shadow_signal_2s()
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get(
        "/markets/current/live/challengers/chainlink-catchup-2s"
    )

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": 1,
        "server_time_ms": 1_783_459_250_123,
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "publication_role": "challenger",
        "prediction": {
            "schema_version": 1,
            "mode": "shadow_candidate",
            "publication_role": "challenger",
            "experiment_version": "prospective_catchup_2s_v1",
            "model_version": "catchup_v1_l2000_h2000_b100",
            "beta": "1",
            "futures_lookback_ms": 2_000,
            "generated_ms": 1_783_459_250_100,
            "target_ms": 1_783_459_252_100,
            "forecast_horizon_ms": 2_000,
            "valid": True,
            "status": "valid",
            "invalid_reasons": [],
            "state": "anchored",
            "current_chainlink": "62000",
            "projected_chainlink": "62000.99838969404186795491142",
            "pending_move": "0.99838969404186795491142",
            "pending_move_bps": "0.1610305958132045088566806452",
            "direction": "up",
            "futures_now": "62101",
            "futures_reference": "62100",
            "chainlink_now_source_timestamp_ms": 1_783_459_250_000,
            "chainlink_now_received_ms": 1_783_459_250_070,
            "anchor_chainlink_source_timestamp_ms": 1_783_459_250_000,
            "anchor_chainlink_received_ms": 1_783_459_250_070,
            "futures_now_source_timestamp_ms": 1_783_459_250_050,
            "futures_now_received_ms": 1_783_459_250_090,
            "futures_reference_source_timestamp_ms": 1_783_459_248_000,
            "futures_reference_received_ms": 1_783_459_248_050,
            "futures_reference_target_ms": 1_783_459_248_070,
            "futures_reference_gap_ms": 20,
            "futures_received_age_ms": 10,
            "chainlink_received_age_ms": 30,
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "ms_to_market_end": 249_900,
            "full_horizon_before_market_end": True,
            "signal_age_ms": 23,
        },
    }
    assert client.fake_shadow_signal_2s_store.get_calls == 1
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


def test_chainlink_catchup_2s_challenger_returns_null_when_unavailable_or_rejected(
    client,
    monkeypatch,
):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get(
        "/markets/current/live/challengers/chainlink-catchup-2s"
    )

    assert response.status_code == 200
    assert response.json()["prediction"] is None
    assert client.fake_shadow_signal_2s_store.get_calls == 1
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


def test_chainlink_catchup_2s_challenger_keeps_well_formed_invalid_signal(
    client,
    monkeypatch,
):
    client.fake_shadow_signal_2s_store.signal = replace(
        live_shadow_signal_2s(),
        valid=False,
        status="futures_stale",
        invalid_reasons=("futures_stale",),
        projected_chainlink=None,
        pending_move=None,
        pending_move_bps=None,
        direction=None,
        futures_reference=None,
        anchor_chainlink_source_timestamp_ms=None,
        anchor_chainlink_received_ms=None,
        futures_reference_source_timestamp_ms=None,
        futures_reference_received_ms=None,
        futures_reference_target_ms=None,
        futures_reference_gap_ms=None,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get(
        "/markets/current/live/challengers/chainlink-catchup-2s"
    )

    assert response.status_code == 200
    prediction = response.json()["prediction"]
    assert prediction["valid"] is False
    assert prediction["status"] == "futures_stale"
    assert prediction["invalid_reasons"] == ["futures_stale"]
    assert prediction["projected_chainlink"] is None
    assert prediction["signal_age_ms"] == 23


def test_chainlink_catchup_2s_challenger_returns_503_on_redis_failure(client):
    client.fake_shadow_signal_2s_store.read_error = OSError("redis unavailable")

    response = client.get(
        "/markets/current/live/challengers/chainlink-catchup-2s"
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "live cache unavailable"}
    assert client.fake_shadow_signal_2s_store.get_calls == 1
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


def test_chainlink_catchup_2s_challenger_has_no_query_parameters(client):
    operation = client.get("/openapi.json").json()["paths"][
        "/markets/current/live/challengers/chainlink-catchup-2s"
    ]["get"]

    assert operation.get("parameters", []) == []


def test_markets_download_returns_attachment_filename(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert isinstance(server_time_ms, int)
        assert include_probabilities is True
        assert include_futures is False
        assert include_oi is False
        assert include_flow is False
        assert include_book is False
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        return market_data_payload(include_probabilities=True)

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get("/markets/5944864/download?include_probabilities=true")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert (
        response.headers["content-disposition"]
        == 'attachment; filename="btc_5m_market_5944864_probabilities.json"'
    )
    body = response.json()
    assert body["series"][0]["probabilities"]["down"]["normalized"] == "0.51758794"
    assert "market_start_ms" not in body["market"]
    assert "market_end_ms" not in body["market"]
    assert body["market"]["market_start_at"] == "2026-07-07T21:00:00Z"
    assert body["market"]["market_end_at"] == "2026-07-07T21:05:00Z"
    assert "timestamp_ms" not in body["series"][0]
    assert body["series"][0]["timestamp_at"] == "2026-07-07T21:00:00Z"
    assert "freshness" not in body["series"][0]


def test_markets_current_download_uses_same_compact_shape(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        payload = market_data_payload()
        payload["market"]["chainlink_resolution"] = {
            "open": "64159.4",
            "close": None,
            "status": "pending",
            "source": "polymarket_gamma_event_metadata",
        }
        return payload

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get("/markets/current/download")

    assert response.status_code == 200
    body = response.json()
    assert "market_start_ms" not in body["market"]
    assert "market_end_ms" not in body["market"]
    assert body["market"]["chainlink_resolution"]["open"] == "64159.40"
    assert body["market"]["chainlink_resolution"]["close"] is None
    assert "timestamp_ms" not in body["series"][0]
    assert body["series"][0]["timestamp_at"] == "2026-07-07T21:00:00Z"


def test_serialize_download_payload_is_compact_without_mutating_source():
    payload = market_data_payload()
    payload["market"]["chainlink_resolution"] = {
        "open": "64159.345",
        "close": "64171",
        "status": "official",
        "source": "polymarket_gamma_event_metadata",
    }
    payload["series"].append(
        {
            **payload["series"][0],
            "t": 1,
            "timestamp_ms": 1_783_459_201_000,
            "timestamp_at": "2026-07-07T21:00:01Z",
        }
    )

    exported = api.serialize_download_payload(payload)

    assert exported["schema_version"] == 2
    assert "market_start_ms" not in exported["market"]
    assert "market_end_ms" not in exported["market"]
    assert exported["market"]["market_start_at"] == "2026-07-07T21:00:00Z"
    assert exported["market"]["market_end_at"] == "2026-07-07T21:05:00Z"
    assert exported["market"]["chainlink_resolution"]["open"] == "64159.35"
    assert exported["market"]["chainlink_resolution"]["close"] == "64171.00"
    assert all("timestamp_ms" not in item for item in exported["series"])
    assert [item["timestamp_at"] for item in exported["series"]] == [
        "2026-07-07T21:00:00Z",
        "2026-07-07T21:00:01Z",
    ]
    assert [item["t"] for item in exported["series"]] == [0, 1]

    assert payload["market"]["market_start_ms"] == 1_783_459_200_000
    assert payload["market"]["market_end_ms"] == 1_783_459_500_000
    assert payload["market"]["chainlink_resolution"]["open"] == "64159.345"
    assert payload["market"]["chainlink_resolution"]["close"] == "64171"
    assert payload["series"][0]["timestamp_ms"] == 1_783_459_200_000


def test_markets_download_preserves_official_resolution_metadata(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        payload = market_data_payload()
        payload["market"]["chainlink_resolution"] = {
            "open": "63337.115841440165",
            "close": "63336.71900847139",
            "status": "official",
            "source": "polymarket_gamma_event_metadata",
        }
        payload["market"]["resolution"] = {
            "status": "resolved",
            "resolution_type": "winner",
            "winner": "Down",
            "winning_token_id": "down-token",
            "resolved_at_ms": 1_783_459_517_000,
            "official_payouts": {"up": "0", "down": "1"},
            "source": "polymarket_clob_rest",
        }
        return payload

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get("/markets/5944864/download")

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 2
    assert body["market"]["chainlink_resolution"]["open"] == "63337.12"
    assert body["market"]["chainlink_resolution"]["close"] == "63336.72"
    assert "market_start_ms" not in body["market"]
    assert "market_end_ms" not in body["market"]
    assert "timestamp_ms" not in body["series"][0]
    assert body["series"][0]["timestamp_at"] == "2026-07-07T21:00:00Z"
    assert body["market"]["resolution"]["winner"] == "Down"
    assert body["market"]["resolution"]["resolved_at_ms"] == 1_783_459_517_000
    assert body["market"]["resolution"]["official_payouts"] == {
        "up": "0",
        "down": "1",
    }


def test_markets_download_filename_includes_requested_optional_layers(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert isinstance(server_time_ms, int)
        assert include_probabilities is True
        assert include_futures is True
        assert include_oi is True
        assert include_flow is False
        assert include_book is False
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        return market_data_payload(
            include_probabilities=True,
            include_futures=True,
            include_oi=True,
        )

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get(
        "/markets/5944864/download?"
        "include_probabilities=true&include_futures=true&include_oi=true"
    )

    assert response.status_code == 200
    assert (
        response.headers["content-disposition"]
        == (
            'attachment; filename="'
            'btc_5m_market_5944864_futures_oi_probabilities.json"'
        )
    )
    body = response.json()
    assert body["series"][0]["prices"] == {
        "binance": "123000.00",
        "chainlink": "122998.12",
        "futures": "62075.12",
    }
    assert "futures" not in body["series"][0]
    assert "freshness" not in body["series"][0]


def test_markets_download_trims_flow_and_book_to_export_fields(client, monkeypatch):
    async def fake_fetch_market_download_payload(
        pool,
        market_id,
        server_time_ms,
        include_probabilities,
        include_futures,
        include_oi,
        include_flow,
        include_book,
        fill_display,
        max_carry_forward_ms,
    ):
        assert market_id == 5_944_864
        assert isinstance(server_time_ms, int)
        assert include_probabilities is False
        assert include_futures is False
        assert include_oi is False
        assert include_flow is True
        assert include_book is True
        assert fill_display is False
        assert max_carry_forward_ms == 10_000
        return market_data_payload(include_flow=True, include_book=True)

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )

    response = client.get("/markets/5944864/download?include_flow=true&include_book=true")

    assert response.status_code == 200
    assert (
        response.headers["content-disposition"]
        == 'attachment; filename="btc_5m_market_5944864_flow_book.json"'
    )
    body = response.json()
    assert body["series"][0]["flow"] == {
        "taker_imbalance": "0.6000",
        "cvd_10s": "900.12",
        "cvd_30s": "1200.13",
        "imbalance_10s": "0.1235",
        "imbalance_30s": "-0.2346",
    }
    assert body["series"][0]["book"] == {
        "book_imbalance": "0.2500",
        "microprice": "62074.17",
    }
    assert "buy_quote" not in body["series"][0]["flow"]
    assert "bid" not in body["series"][0]["book"]
    assert "freshness" not in body["series"][0]


SHADOW_REPORT_NOW_MS = 1_783_459_250_123
SHADOW_REPORT_MARKET_ID = 5_944_864
SHADOW_REPORT_MARKET_START_MS = 1_783_459_200_000
SHADOW_REPORT_MARKET_END_MS = 1_783_459_500_000
SHADOW_REPORT_MODEL = "catchup_ratio_l3000_b100"
SHADOW_REPORT_2S_MODEL = "catchup_v1_l2000_h2000_b100"


def shadow_evaluation_chart_row(**overrides):
    generated_ms = 1_783_459_247_100
    target_ms = generated_ms + 3_000
    values = {
        "selection_schema_version": 2,
        "selection_policy_version": "chronological_holdout_v2",
        "selection_evidence_end_ms": SHADOW_REPORT_MARKET_START_MS - MARKET_MS,
        "selection_fingerprint_sha256": "a" * 64,
        "selection_artifact_sha256": "b" * 64,
        "model_version": SHADOW_REPORT_MODEL,
        "beta": Decimal("1"),
        "generated_ms": generated_ms,
        "target_ms": target_ms,
        "matured_ms": target_ms + 10,
        "horizon_ms": 3_000,
        "valid": True,
        "status": "valid",
        "invalid_reasons": (),
        "state": "anchored",
        "outcome_status": "available",
        "outcome_invalid_reasons": (),
        "forecast_market_id": SHADOW_REPORT_MARKET_ID,
        "full_horizon_before_forecast_market_end": True,
        "chainlink_at_forecast": Decimal("62000"),
        "chainlink_at_forecast_source_timestamp_ms": generated_ms - 1_000,
        "chainlink_at_forecast_received_ms": generated_ms - 100,
        "futures_at_forecast": Decimal("62101"),
        "futures_at_forecast_source_timestamp_ms": generated_ms - 50,
        "futures_at_forecast_received_ms": generated_ms - 10,
        "projected_chainlink": Decimal("62001"),
        "actual_chainlink": Decimal("62000.5"),
        "actual_chainlink_source_timestamp_ms": target_ms - 1_100,
        "actual_chainlink_received_ms": target_ms - 100,
        "actual_chainlink_age_at_target_ms": 100,
        "pending_move": Decimal("1"),
        "pending_move_bps": Decimal(
            "0.1612903225806451612903225806"
        ),
        "direction": "up",
        "forecast_error": Decimal("0.5"),
        "baseline_error": Decimal("-0.5"),
    }
    values.update(overrides)
    return values


def shadow_evaluation_2s_chart_row(**overrides):
    values = shadow_evaluation_chart_row()
    generated_ms = values["generated_ms"]
    target_ms = generated_ms + 2_000
    values.update(
        {
            "model_version": SHADOW_REPORT_2S_MODEL,
            "target_ms": target_ms,
            "matured_ms": target_ms + 10,
            "horizon_ms": 2_000,
            "actual_chainlink_source_timestamp_ms": target_ms - 1_100,
            "actual_chainlink_received_ms": target_ms - 100,
            "actual_chainlink_age_at_target_ms": 100,
        }
    )
    values.update(overrides)
    return values


def test_current_shadow_evaluations_returns_exact_typed_point_without_redis(
    client,
    monkeypatch,
):
    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == SHADOW_REPORT_MARKET_ID
        assert window.market_start_ms == SHADOW_REPORT_MARKET_START_MS
        assert window.market_end_ms == SHADOW_REPORT_MARKET_END_MS
        assert model_version == SHADOW_REPORT_MODEL
        return ShadowEvaluationFetchResult(
            market_exists=True,
            rows=(shadow_evaluation_chart_row(),),
        )

    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: SHADOW_REPORT_NOW_MS)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(
        "/markets/current/shadow-evaluations"
        f"?model_version={SHADOW_REPORT_MODEL}"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 2
    assert body["server_time_ms"] == SHADOW_REPORT_NOW_MS
    assert body["market"] == {
        "market_id": SHADOW_REPORT_MARKET_ID,
        "market_start_ms": SHADOW_REPORT_MARKET_START_MS,
        "market_end_ms": SHADOW_REPORT_MARKET_END_MS,
        "boundary": "[start_ms,end_ms)",
    }
    assert body["evaluation_semantics"] == {
        "scored_input_max_future_skew_ms": 0,
    }
    assert body["model"] == {
        "model_version": SHADOW_REPORT_MODEL,
        "horizon_ms": 3_000,
        "beta": "1",
        "evaluation_cadence_ms": 500,
        "selection_identities": [
            {
                "schema_version": 2,
                "policy_version": "chronological_holdout_v2",
                "evidence_end_ms": (
                    SHADOW_REPORT_MARKET_START_MS - MARKET_MS
                ),
                "fingerprint_sha256": "a" * 64,
                "artifact_sha256": "b" * 64,
            }
        ],
    }
    assert body["coverage"] == {
        "window_buckets": 600,
        "market_window_elapsed": False,
        "observed_buckets": 1,
        "unobserved_buckets_as_of_response": None,
        "attempts": 1,
        "valid_forecasts": 1,
        "scored": 1,
        "invalid": 0,
        "valid_without_actual": 0,
    }
    assert body["performance"] == {
        "cohorts": [
            {
                "selection_identity": {
                    "schema_version": 2,
                    "policy_version": "chronological_holdout_v2",
                    "evidence_end_ms": (
                        SHADOW_REPORT_MARKET_START_MS - MARKET_MS
                    ),
                    "fingerprint_sha256": "a" * 64,
                    "artifact_sha256": "b" * 64,
                },
                "scored_points": 1,
                "forecast": {
                    "mean_absolute_error_usd": "0.5",
                    "median_absolute_error_usd": "0.5",
                    "p95_absolute_error_usd": "0.5",
                    "maximum_absolute_error_usd": "0.5",
                    "root_mean_squared_error_usd": "0.5",
                    "mean_signed_error_usd": "0.5",
                },
                "no_change_baseline": {
                    "mean_absolute_error_usd": "0.5",
                    "root_mean_squared_error_usd": "0.5",
                },
                "mean_absolute_advantage_usd": "0.0",
                "mae_skill_vs_no_change": "0",
                "rmse_skill_vs_no_change": "0",
                "paired_comparison": {
                    "wins": 0,
                    "ties": 1,
                    "losses": 0,
                    "win_rate": "0",
                    "tie_rate": "1",
                    "loss_rate": "0",
                },
            }
        ]
    }
    assert body["points"] == [
        {
            "selection_schema_version": 2,
            "selection_policy_version": "chronological_holdout_v2",
            "selection_evidence_end_ms": (
                SHADOW_REPORT_MARKET_START_MS - MARKET_MS
            ),
            "selection_fingerprint_sha256": "a" * 64,
            "selection_artifact_sha256": "b" * 64,
            "model_version": SHADOW_REPORT_MODEL,
            "beta": "1",
            "generated_ms": 1_783_459_247_100,
            "target_ms": 1_783_459_250_100,
            "matured_ms": 1_783_459_250_110,
            "horizon_ms": 3_000,
            "valid": True,
            "status": "valid",
            "invalid_reasons": [],
            "state": "anchored",
            "outcome_status": "available",
            "outcome_invalid_reasons": [],
            "forecast_market_id": SHADOW_REPORT_MARKET_ID,
            "full_horizon_before_forecast_market_end": True,
            "chainlink_at_forecast": "62000",
            "chainlink_at_forecast_source_timestamp_ms": 1_783_459_246_100,
            "chainlink_at_forecast_received_ms": 1_783_459_247_000,
            "futures_at_forecast": "62101",
            "futures_at_forecast_source_timestamp_ms": 1_783_459_247_050,
            "futures_at_forecast_received_ms": 1_783_459_247_090,
            "projected_chainlink": "62001",
            "actual_chainlink": "62000.5",
            "actual_chainlink_source_timestamp_ms": 1_783_459_249_000,
            "actual_chainlink_received_ms": 1_783_459_250_000,
            "actual_chainlink_age_at_target_ms": 100,
            "pending_move": "1",
            "pending_move_bps": "0.1612903225806451612903225806",
            "direction": "up",
            "forecast_error": "0.5",
            "baseline_error": "-0.5",
        }
    ]
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


def test_shadow_evaluations_by_id_returns_completed_market_report(
    client,
    monkeypatch,
):
    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == SHADOW_REPORT_MARKET_ID
        assert model_version == SHADOW_REPORT_MODEL
        return ShadowEvaluationFetchResult(
            market_exists=True,
            rows=(shadow_evaluation_chart_row(),),
        )

    monkeypatch.setattr(
        api,
        "current_utc_epoch_ms",
        lambda: SHADOW_REPORT_MARKET_END_MS + 1_000,
    )
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(
        f"/markets/{SHADOW_REPORT_MARKET_ID}/shadow-evaluations"
        f"?model_version={SHADOW_REPORT_MODEL}"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["market"]["market_id"] == SHADOW_REPORT_MARKET_ID
    assert body["coverage"]["market_window_elapsed"] is True
    assert body["coverage"]["observed_buckets"] == 1
    assert body["coverage"]["unobserved_buckets_as_of_response"] == 599
    assert body["performance"]["cohorts"][0]["scored_points"] == 1
    assert body["schema_version"] == 2
    assert body["points"][0]["chainlink_at_forecast"] == "62000"
    assert body["points"][0]["futures_at_forecast"] == "62101"
    assert body["points"][0]["projected_chainlink"] == "62001"
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize(
    "path",
    (
        "/markets/current/shadow-evaluations",
        "/markets/current/shadow-evaluations/download",
        f"/markets/{SHADOW_REPORT_MARKET_ID}/shadow-evaluations",
        f"/markets/{SHADOW_REPORT_MARKET_ID}/shadow-evaluations/download",
    ),
)
def test_shadow_evaluation_routes_accept_two_second_challenger_with_same_shape(
    client,
    monkeypatch,
    path,
):
    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == SHADOW_REPORT_MARKET_ID
        assert model_version == SHADOW_REPORT_2S_MODEL
        return ShadowEvaluationFetchResult(
            market_exists=True,
            rows=(shadow_evaluation_2s_chart_row(),),
        )

    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: SHADOW_REPORT_NOW_MS)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(f"{path}?model_version={SHADOW_REPORT_2S_MODEL}")

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 2
    assert body["model"]["model_version"] == SHADOW_REPORT_2S_MODEL
    assert body["model"]["horizon_ms"] == 2_000
    assert body["model"]["evaluation_cadence_ms"] == 500
    assert body["points"][0]["model_version"] == SHADOW_REPORT_2S_MODEL
    assert body["points"][0]["horizon_ms"] == 2_000
    assert body["points"][0]["target_ms"] == (
        body["points"][0]["generated_ms"] + 2_000
    )
    assert body["coverage"]["attempts"] == 1
    assert body["performance"]["cohorts"][0]["scored_points"] == 1
    if path.endswith("/download"):
        assert body["export"]["variant"] == "rounded_download"
        assert response.headers["content-disposition"] == (
            'attachment; filename="'
            f"btc_5m_market_{SHADOW_REPORT_MARKET_ID}_shadow_evaluations_"
            f'{SHADOW_REPORT_2S_MODEL}_rounded.json"'
        )
    else:
        assert "export" not in body
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


def test_serialize_shadow_evaluation_download_payload_rounds_known_fields_only():
    payload = {
        "schema_version": 2,
        "server_time_ms": 123,
        "model": {
            "model_version": SHADOW_REPORT_MODEL,
            "beta": "1.000000000000000000",
        },
        "performance": {
            "cohorts": [
                {
                    "forecast": {
                        "mean_absolute_error_usd": (
                            "2.8978376726887863308547794117647058823529"
                        ),
                        "median_absolute_error_usd": "2.0565",
                        "p95_absolute_error_usd": "8.825",
                        "maximum_absolute_error_usd": "18.562",
                        "root_mean_squared_error_usd": "4.1905",
                        "mean_signed_error_usd": "-0.295",
                    },
                    "no_change_baseline": {
                        "mean_absolute_error_usd": "2.688",
                        "root_mean_squared_error_usd": "4.225",
                    },
                    "mean_absolute_advantage_usd": "-0.205",
                    "mae_skill_vs_no_change": "-0.077896039442535667",
                    "rmse_skill_vs_no_change": "0.007376144460456536",
                    "paired_comparison": {
                        "wins": 202,
                        "ties": 114,
                        "losses": 228,
                        "win_rate": "0.37135",
                        "tie_rate": "0.20955",
                        "loss_rate": "0.41915",
                    },
                }
            ]
        },
        "points": [
            {
                "beta": "1.000000000000000000",
                "chainlink_at_forecast": "64381.765",
                "futures_at_forecast": "64399.900000000000000000",
                "projected_chainlink": "64381.427940530536696941",
                "actual_chainlink": None,
                "pending_move": "-0.005",
                "pending_move_bps": "-0.015527950310559006",
                "forecast_error": "-0.004",
                "baseline_error": "1.235",
                "direction": "down",
                "unmapped_decimal_string": "0.12345678901234567890",
            }
        ],
    }
    original = deepcopy(payload)

    exported = api.serialize_shadow_evaluation_download_payload(payload)

    assert payload == original
    assert exported is not payload
    assert exported["model"] is not payload["model"]
    assert exported["points"] is not payload["points"]
    assert exported["model"]["beta"] == "1.0000"
    assert exported["performance"]["cohorts"][0] == {
        "forecast": {
            "mean_absolute_error_usd": "2.90",
            "median_absolute_error_usd": "2.06",
            "p95_absolute_error_usd": "8.83",
            "maximum_absolute_error_usd": "18.56",
            "root_mean_squared_error_usd": "4.19",
            "mean_signed_error_usd": "-0.30",
        },
        "no_change_baseline": {
            "mean_absolute_error_usd": "2.69",
            "root_mean_squared_error_usd": "4.23",
        },
        "mean_absolute_advantage_usd": "-0.21",
        "mae_skill_vs_no_change": "-0.0779",
        "rmse_skill_vs_no_change": "0.0074",
        "paired_comparison": {
            "wins": 202,
            "ties": 114,
            "losses": 228,
            "win_rate": "0.3714",
            "tie_rate": "0.2096",
            "loss_rate": "0.4192",
        },
    }
    assert exported["points"][0] == {
        "beta": "1.0000",
        "chainlink_at_forecast": "64381.77",
        "futures_at_forecast": "64399.90",
        "projected_chainlink": "64381.43",
        "actual_chainlink": None,
        "pending_move": "-0.01",
        "pending_move_bps": "-0.0155",
        "forecast_error": "0.00",
        "baseline_error": "1.24",
        "direction": "down",
        "unmapped_decimal_string": "0.12345678901234567890",
    }
    assert exported["export"] == {
        "schema_version": 1,
        "variant": "rounded_download",
        "source_report_schema_version": 2,
        "decimal_encoding": "fixed_point_string",
        "rounding_mode": "ROUND_HALF_UP",
        "precision_policy": "shadow_evaluation_download_v1",
        "decimal_places": {
            "usd_price_move_error": 2,
            "basis_points": 4,
            "unitless_beta_rate_skill": 4,
        },
        "derived_metrics_computed_before_rounding": True,
        "classifications_computed_before_rounding": True,
    }


@pytest.mark.parametrize("value", (Decimal("1"), "NaN", "Infinity"))
def test_shadow_download_rejects_non_string_or_non_finite_decimal(value):
    payload = {
        "schema_version": 2,
        "model": {"beta": value},
        "performance": {"cohorts": []},
        "points": [],
    }

    with pytest.raises(ValueError):
        api.serialize_shadow_evaluation_download_payload(payload)


def test_shadow_download_rejects_unknown_source_report_schema():
    with pytest.raises(ValueError, match="schema_version"):
        api.serialize_shadow_evaluation_download_payload({"schema_version": 3})


@pytest.mark.parametrize(
    "path",
    (
        "/markets/current/shadow-evaluations/download",
        f"/markets/{SHADOW_REPORT_MARKET_ID}/shadow-evaluations/download",
    ),
)
def test_shadow_evaluation_download_returns_compact_json_attachment(
    client,
    monkeypatch,
    path,
):
    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == SHADOW_REPORT_MARKET_ID
        assert model_version == SHADOW_REPORT_MODEL
        return ShadowEvaluationFetchResult(
            market_exists=True,
            rows=(shadow_evaluation_chart_row(),),
        )

    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: SHADOW_REPORT_NOW_MS)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(f"{path}?model_version={SHADOW_REPORT_MODEL}")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.headers["content-disposition"] == (
        'attachment; filename="'
        f"btc_5m_market_{SHADOW_REPORT_MARKET_ID}_shadow_evaluations_"
        f'{SHADOW_REPORT_MODEL}_rounded.json"'
    )
    assert b"\n" not in response.content
    body = response.json()
    assert body["schema_version"] == 2
    assert body["server_time_ms"] == SHADOW_REPORT_NOW_MS
    assert body["market"]["market_id"] == SHADOW_REPORT_MARKET_ID
    assert body["model"]["model_version"] == SHADOW_REPORT_MODEL
    assert body["model"]["beta"] == "1.0000"
    assert body["points"][0]["beta"] == "1.0000"
    assert body["points"][0]["futures_at_forecast"] == "62101.00"
    assert body["points"][0]["projected_chainlink"] == "62001.00"
    assert body["points"][0]["actual_chainlink"] == "62000.50"
    assert body["points"][0]["pending_move_bps"] == "0.1613"
    assert body["performance"]["cohorts"][0]["forecast"][
        "mean_absolute_error_usd"
    ] == "0.50"
    assert body["performance"]["cohorts"][0]["paired_comparison"] == {
        "wins": 0,
        "ties": 1,
        "losses": 0,
        "win_rate": "0.0000",
        "tie_rate": "1.0000",
        "loss_rate": "0.0000",
    }
    assert body["export"]["precision_policy"] == (
        "shadow_evaluation_download_v1"
    )

    report_response = client.get(
        f"{path.removesuffix('/download')}?model_version={SHADOW_REPORT_MODEL}"
    )
    assert report_response.status_code == 200
    report_body = report_response.json()
    assert "export" not in report_body
    assert report_body["model"]["beta"] == "1"
    assert report_body["points"][0]["futures_at_forecast"] == "62101"
    assert report_body["points"][0]["projected_chainlink"] == "62001"
    assert report_body["points"][0]["actual_chainlink"] == "62000.5"
    assert report_body["points"][0]["pending_move_bps"] == (
        "0.1612903225806451612903225806"
    )
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize(
    "path",
    (
        "/markets/current/shadow-evaluations",
        "/markets/5944865/shadow-evaluations",
    ),
)
def test_current_shadow_evaluation_window_is_known_before_market_row_exists(
    client,
    monkeypatch,
    path,
):
    server_time_ms = SHADOW_REPORT_MARKET_END_MS + 10
    current_market_id = SHADOW_REPORT_MARKET_ID + 1

    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == current_market_id
        assert model_version == SHADOW_REPORT_MODEL
        return ShadowEvaluationFetchResult(market_exists=False, rows=())

    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: server_time_ms)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(f"{path}?model_version={SHADOW_REPORT_MODEL}")

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": 2,
        "server_time_ms": server_time_ms,
        "market": {
            "market_id": current_market_id,
            "market_start_ms": SHADOW_REPORT_MARKET_END_MS,
            "market_end_ms": SHADOW_REPORT_MARKET_END_MS + 300_000,
            "boundary": "[start_ms,end_ms)",
        },
        "evaluation_semantics": {
            "scored_input_max_future_skew_ms": 0,
        },
        "model": {
            "model_version": SHADOW_REPORT_MODEL,
            "horizon_ms": 3_000,
            "beta": "1",
            "evaluation_cadence_ms": 500,
            "selection_identities": [],
        },
        "coverage": {
            "window_buckets": 600,
            "market_window_elapsed": False,
            "observed_buckets": 0,
            "unobserved_buckets_as_of_response": None,
            "attempts": 0,
            "valid_forecasts": 0,
            "scored": 0,
            "invalid": 0,
            "valid_without_actual": 0,
        },
        "performance": {"cohorts": []},
        "points": [],
    }
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize(
    "path_suffix",
    (
        "shadow-evaluations",
        "shadow-evaluations/download",
    ),
)
def test_shadow_evaluations_unknown_historical_market_returns_404(
    client,
    monkeypatch,
    path_suffix,
):
    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == SHADOW_REPORT_MARKET_ID - 1
        assert model_version == SHADOW_REPORT_MODEL
        return ShadowEvaluationFetchResult(market_exists=False, rows=())

    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: SHADOW_REPORT_NOW_MS)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(
        f"/markets/{SHADOW_REPORT_MARKET_ID - 1}/{path_suffix}"
        f"?model_version={SHADOW_REPORT_MODEL}"
    )

    assert response.status_code == 404
    assert response.json() == {
        "detail": (
            "no market found for "
            f"market_id={SHADOW_REPORT_MARKET_ID - 1}"
        )
    }
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize(
    "path_suffix",
    ("shadow-evaluations", "shadow-evaluations/download"),
)
def test_shadow_evaluations_known_historical_market_can_have_no_retained_rows(
    client,
    monkeypatch,
    path_suffix,
):
    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        assert window.market_id == SHADOW_REPORT_MARKET_ID
        assert model_version == SHADOW_REPORT_MODEL
        return ShadowEvaluationFetchResult(market_exists=True, rows=())

    server_time_ms = SHADOW_REPORT_MARKET_END_MS + 1_000
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: server_time_ms)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(
        f"/markets/{SHADOW_REPORT_MARKET_ID}/{path_suffix}"
        f"?model_version={SHADOW_REPORT_MODEL}"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["server_time_ms"] == server_time_ms
    assert body["market"]["market_id"] == SHADOW_REPORT_MARKET_ID
    assert body["model"]["horizon_ms"] == 3_000
    assert body["model"]["selection_identities"] == []
    assert body["coverage"] == {
        "window_buckets": 600,
        "market_window_elapsed": True,
        "observed_buckets": 0,
        "unobserved_buckets_as_of_response": 600,
        "attempts": 0,
        "valid_forecasts": 0,
        "scored": 0,
        "invalid": 0,
        "valid_without_actual": 0,
    }
    assert body["performance"] == {"cohorts": []}
    assert body["points"] == []
    if path_suffix.endswith("/download"):
        assert response.headers["content-disposition"].startswith("attachment;")
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize(
    "path",
    (
        "/markets/current/shadow-evaluations",
        "/markets/current/shadow-evaluations/download",
        f"/markets/{SHADOW_REPORT_MARKET_ID}/shadow-evaluations",
        f"/markets/{SHADOW_REPORT_MARKET_ID}/shadow-evaluations/download",
    ),
)
@pytest.mark.parametrize("query", ("", "?model_version=unsupported_model"))
def test_shadow_evaluation_routes_require_a_supported_model(
    client,
    monkeypatch,
    path,
    query,
):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid request must not query PostgreSQL")

    monkeypatch.setattr(
        api,
        "fetch_shadow_evaluation_chart_points",
        unexpected_fetch,
    )

    response = client.get(f"{path}{query}")

    assert response.status_code == 422
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize("failure", ("integrity", "row_limit"))
@pytest.mark.parametrize(
    "path",
    (
        "/markets/current/shadow-evaluations",
        "/markets/current/shadow-evaluations/download",
    ),
)
def test_shadow_evaluation_reporting_failures_return_generic_500(
    client,
    monkeypatch,
    failure,
    path,
):
    if failure == "integrity":
        rows = (shadow_evaluation_chart_row(horizon_ms=3_500),)
    else:
        rows = (shadow_evaluation_chart_row(),) * 1_001

    async def fake_fetch(pool, *, window, model_version):
        assert pool is client.fake_pool
        return ShadowEvaluationFetchResult(market_exists=True, rows=rows)

    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: SHADOW_REPORT_NOW_MS)
    monkeypatch.setattr(api, "fetch_shadow_evaluation_chart_points", fake_fetch)

    response = client.get(f"{path}?model_version={SHADOW_REPORT_MODEL}")

    assert response.status_code == 500
    assert response.json() == {
        "detail": "shadow evaluation data inconsistent"
    }
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0


def test_openapi_lists_shadow_evaluation_routes_and_required_model(client):
    response = client.get("/openapi.json")

    assert response.status_code == 200
    openapi = response.json()
    paths = openapi["paths"]
    model_schema = openapi["components"]["schemas"][
        "ShadowEvaluationModelVersion"
    ]
    assert "catchup_ratio_l3000_b100" in model_schema["enum"]
    assert "catchup_v1_l2000_h2000_b100" in model_schema["enum"]
    current_paths = (
        "/markets/current/shadow-evaluations",
        "/markets/current/shadow-evaluations/download",
    )
    by_id_paths = (
        "/markets/{market_id}/shadow-evaluations",
        "/markets/{market_id}/shadow-evaluations/download",
    )

    for path in (*current_paths, *by_id_paths):
        operation = paths[path]["get"]
        model_parameter = next(
            parameter
            for parameter in operation["parameters"]
            if parameter["name"] == "model_version"
        )
        assert model_parameter["in"] == "query"
        assert model_parameter["required"] is True
        assert "500" in operation["responses"]

    for path in by_id_paths:
        operation = paths[path]["get"]
        market_id = next(
            parameter
            for parameter in operation["parameters"]
            if parameter["name"] == "market_id"
        )
        assert market_id["in"] == "path"
        assert market_id["required"] is True
        assert "404" in operation["responses"]


@pytest.mark.parametrize("market_id", ("-1", "not-an-integer", str(api.MAX_MARKET_ID + 1)))
@pytest.mark.parametrize(
    "path_suffix",
    ("shadow-evaluations", "shadow-evaluations/download"),
)
def test_shadow_evaluation_route_rejects_invalid_market_id_before_database(
    client,
    monkeypatch,
    market_id,
    path_suffix,
):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid market_id must not query PostgreSQL")

    monkeypatch.setattr(
        api,
        "fetch_shadow_evaluation_chart_points",
        unexpected_fetch,
    )

    response = client.get(
        f"/markets/{market_id}/{path_suffix}"
        f"?model_version={SHADOW_REPORT_MODEL}"
    )

    assert response.status_code == 422
    assert client.fake_live_cache.requested_keys == []
    assert client.fake_pool.acquire_calls == 0
