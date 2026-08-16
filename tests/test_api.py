
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import price_collector.api as api
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    MICROSTRUCTURE_LIVE_KEY,
    TWAP_LIVE_KEY,
    TWAP_SHADOW_LIVE_KEY,
    LiveCachePayloadError,
    LivePrice,
)


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
        self.microstructure_snapshot = None
        self.twap_shadow_snapshot = None
        self.read_error = None
        self.requested_keys = []
        self.requested_combined_keys = []

    async def get_prices(self, keys):
        if self.read_error is not None:
            raise self.read_error
        key_list = list(keys)
        self.requested_keys.append(key_list)
        return {key: self.prices.get(key) for key in key_list}

    async def get_prices_with_microstructure(
        self,
        price_keys,
        *,
        microstructure_key,
    ):
        if self.read_error is not None:
            raise self.read_error
        price_key_list = list(price_keys)
        self.requested_combined_keys.append(
            [*price_key_list, microstructure_key]
        )
        return (
            {key: self.prices.get(key) for key in price_key_list},
            self.microstructure_snapshot,
        )

    async def get_prices_with_twap_shadow(
        self,
        price_keys,
        *,
        shadow_key,
    ):
        if self.read_error is not None:
            raise self.read_error
        price_key_list = list(price_keys)
        self.requested_combined_keys.append([*price_key_list, shadow_key])
        return (
            {key: self.prices.get(key) for key in price_key_list},
            self.twap_shadow_snapshot,
        )

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def client(monkeypatch):
    fake_pool = FakePool()
    fake_live_cache = FakeLiveCache()

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
    monkeypatch.setattr(api, "health_check", fake_health_check)

    with TestClient(api.app) as test_client:
        test_client.fake_pool = fake_pool
        test_client.fake_live_cache = fake_live_cache
        yield test_client

    assert fake_live_cache.closed is True
    assert fake_pool.closed is True


def utc_dt(year, month, day, hour, minute, second):
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


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


def test_prices_latest_can_query_polymarket_chainlink_twap(client, monkeypatch):
    async def fake_fetch_latest_price(pool, provider_code, symbol):
        assert provider_code == "polymarket_chainlink_twap_rtds"
        assert symbol == "BTCUSD_TWAP_60S"
        return {
            "provider": provider_code,
            "symbol": symbol,
            "price": Decimal("123455.987654321098765432"),
            "sample_second_ms": 1_786_665_600_000,
            "sample_second_at": utc_dt(2026, 8, 14, 0, 0, 0),
            "provider_event_ms": 1_786_665_600_123,
            "received_ms": 1_786_665_600_250,
            "market_id": 5_955_552,
            "market_start_ms": 1_786_665_600_000,
            "market_end_ms": 1_786_665_900_000,
        }

    monkeypatch.setattr(api, "fetch_latest_price", fake_fetch_latest_price)

    response = client.get(
        "/prices/latest?provider=polymarket_chainlink_twap_rtds"
        "&symbol=BTCUSD_TWAP_60S"
    )

    assert response.status_code == 200
    assert response.json()["price"] == "123455.987654321098765432"


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
                "twap_sample_count": 10,
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
                "twap_sample_count": 9,
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
                "twap_sample_count": 10,
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
    assert body["schema_version"] == 2
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
            "twap": 10,
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
        "twap": 0,
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
        "schema_version": 2,
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


def test_markets_sources_by_id_returns_all_market_price_sources(client, monkeypatch):
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
                {
                    "provider": "polymarket_chainlink_twap_rtds",
                    "symbol": "BTCUSD_TWAP_30S",
                    "quote_asset": "USD",
                    "sample_count": 297,
                    "open": Decimal("122997.987654321098765432"),
                    "high": Decimal("123500.987654321098765432"),
                    "low": Decimal("122900.987654321098765432"),
                    "close": Decimal("123454.987654321098765432"),
                    "latest_sample_second_ms": 1_783_459_499_000,
                    "latest_provider_event_ms": 1_783_459_499_111,
                    "latest_received_ms": 1_783_459_501_320,
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
        "polymarket_chainlink_twap_rtds",
    ]
    assert body["sources"][0]["symbol"] == "BTCUSDT"
    assert body["sources"][0]["quote_asset"] == "USDT"
    assert body["sources"][0]["sample_count"] == 300
    assert body["sources"][1]["symbol"] == "BTCUSD"
    assert body["sources"][1]["quote_asset"] == "USD"
    assert body["sources"][1]["sample_count"] == 298
    assert body["sources"][1]["close"] == "123455.900000000000000000"
    assert body["sources"][2]["symbol"] == "BTCUSD_TWAP_30S"
    assert body["sources"][2]["sample_count"] == 297


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
            "twap": "122997.987654321098765432",
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
            "twap": {
                "source_age_ms": 290,
                "received_age_ms": 215,
                "is_carried_forward": False,
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
        "schema_version": 4,
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
            "settlement": {
                "reference": "chainlink_twap",
                "window_s": 30,
                "source_url": (
                    "https://data.chain.link/streams/"
                    "btc-usd-twap-30s-streams"
                ),
                "rule_version": "btc-5m-twap-30",
                "price_to_beat": None,
                "official_final_price": None,
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


def microstructure_row(**updates):
    row = {
        "sample_second_ms": 1_783_459_200_000,
        "schema_version": 1,
        "sample_span_ms": 1_000,
        "sample_jitter_ms": 250,
        "collector_healthy": True,
        "spot_bid": Decimal("65757.900000000000000000"),
        "spot_ask": Decimal("65758.100000000000000000"),
        "spot_mid": Decimal("65758.000000000000000000"),
        "spot_spread_bps": Decimal("0.03040000"),
        "spot_imbalance_1": Decimal("0.15000000"),
        "spot_imbalance_5": Decimal("0.09000000"),
        "spot_imbalance_10": Decimal("0.04000000"),
        "spot_bid_depth_usdt_10": Decimal("1254300.100000000000000000"),
        "spot_ask_depth_usdt_10": Decimal("1189200.500000000000000000"),
        "spot_weighted_mid_offset_bps": Decimal("0.00600000"),
        "spot_snapshot_bbo_ofi_usdt": Decimal("15200.400000000000000000"),
        "spot_book_snapshot_count": 4,
        "fut_bid": Decimal("65723.600000000000000000"),
        "fut_ask": Decimal("65723.700000000000000000"),
        "fut_mid": Decimal("65723.650000000000000000"),
        "fut_spread_bps": Decimal("0.01520000"),
        "fut_imbalance_1": Decimal("0.17000000"),
        "fut_imbalance_5": Decimal("0.11000000"),
        "fut_imbalance_10": Decimal("0.08000000"),
        "fut_bid_depth_usdt_10": Decimal("1432000.200000000000000000"),
        "fut_ask_depth_usdt_10": Decimal("1328000.900000000000000000"),
        "fut_weighted_mid_offset_bps": Decimal("0.00400000"),
        "fut_snapshot_bbo_ofi_usdt": Decimal("18450.250000000000000000"),
        "fut_book_snapshot_count": 5,
        "spot_buy_usdt": Decimal("90450.100000000000000000"),
        "spot_sell_usdt": Decimal("81200.250000000000000000"),
        "fut_buy_usdt": Decimal("120400.150000000000000000"),
        "fut_sell_usdt": Decimal("110200.800000000000000000"),
        "fut_rpi_buy_usdt": Decimal("4200.100000000000000000"),
        "fut_rpi_sell_usdt": None,
        "perp_spot_basis_bps": Decimal("-5.22000000"),
        "spot_fut_book_skew_ms": 41,
        "mark_price": Decimal("65723.610000000000000000"),
        "index_price": Decimal("65721.230000000000000000"),
        "mark_index_basis_bps": Decimal("0.36210000"),
        "funding_rate": Decimal("0.000100000000000000"),
        "seconds_to_funding": 2_400,
        "open_interest_btc": Decimal("80000.100000000000000000"),
        "open_interest_usdt": Decimal("5257890000.100000000000000000"),
        "long_liq_usdt": Decimal("0E-18"),
        "short_liq_usdt": Decimal("12500.200000000000000000"),
        "liq_snapshot_count": 1,
        "spot_book_age_ms": 911,
        "fut_book_age_ms": 363,
        "spot_trade_age_ms": 147,
        "fut_trade_age_ms": 346,
        "connection_errors": 0,
        "received_ms": 1_783_459_201_250,
    }
    row.update(updates)
    return row


def twap_shadow_snapshot():
    origin_second_ms = 1_786_665_650_000
    return {
        "schema_version": 1,
        "model_version": 2,
        "origin_second_ms": origin_second_ms,
        "generated_ms": origin_second_ms + 58,
        "basis_window_seconds": 1_800,
        "status": "ready",
        "quality_flags": ["futures_proxy_polled_latest_wins"],
        "source_bias_bps": {
            "futures": "3.81000000",
            "chainlink_spot": "0.12000000",
            "binance_spot": "3.55000000",
        },
        "basis_sample_counts": {
            "futures": 1_700,
            "chainlink_spot": 1_701,
            "binance_spot": 1_699,
        },
        "recent_p90_abs_error_bps": "0.84000000",
        "predictions": {
            f"h{horizon}": {
                "horizon_seconds": horizon,
                "target_second_ms": origin_second_ms + horizon * 1_000,
                "target_market_id": (
                    origin_second_ms + horizon * 1_000
                )
                // 300_000,
                "expected_actual_received_ms": (
                    origin_second_ms + horizon * 1_000 + 1_800
                ),
                "value": "62066.123456789012345678",
                "known_fraction": "0.90000000",
                "source_count": 3,
                "source_spread_bps": "0.25000000",
                "estimated_error_bps": "0.75000000",
            }
            for horizon in (1, 3, 5, 10)
        },
    }


def flip_market_row(**updates):
    row = {
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "evaluation_status": "confirmed_flip",
        "price_to_beat": Decimal("63337.115841440165000000"),
        "official_close_price": Decimal("63336.719008471390000000"),
        "settlement_reference": "chainlink_twap",
        "settlement_window_s": 30,
        "settlement_source_url": (
            "https://data.chain.link/streams/btc-usd-twap-30s-streams"
        ),
        "settlement_rule_version": "btc-5m-twap-30",
        "official_winner": "Down",
        "matching_crossing_count": 2,
        "total_crossing_count_last_20s": 3,
        "first_crossing_ms_before_end": 4_800,
        "last_crossing_ms_before_end": 1_700,
        "decisive_flip_direction": "up_to_down",
        "decisive_flip_ms_before_end": 1_700,
        "archive_status": "complete",
        "source_microstructure_row_count": 299,
        "archived_microstructure_row_count": 299,
        "retention_safe": True,
        "archived_ms": 1_783_459_560_000,
    }
    row.update(updates)
    return row


def flip_evidence_event(
    *,
    event_sequence=1,
    sample_second_ms=1_783_459_498_000,
    is_decisive=True,
):
    return {
        "event_sequence": event_sequence,
        "direction": "up_to_down",
        "previous_side": "Up",
        "new_side": "Down",
        "previous_price": Decimal("63337.250000000000000001"),
        "new_price": Decimal("63336.990000000000000009"),
        "previous_sample_second_ms": sample_second_ms - 1_000,
        "sample_second_ms": sample_second_ms,
        "previous_provider_event_ms": sample_second_ms - 889,
        "provider_event_ms": sample_second_ms + 222,
        "previous_received_ms": sample_second_ms - 667,
        "received_ms": sample_second_ms + 444,
        "observation_gap_ms": 1_111,
        "observed_ms_before_end": 1_778,
        "is_decisive": is_decisive,
        "observation_precision": "exact_twap_event",
    }


def flip_evidence_cutoff(
    *,
    sample_second_ms=1_783_459_490_000,
):
    return {
        "seconds_before_end": 10,
        "cutoff_ms": 1_783_459_490_000,
        "chainlink_price": Decimal("63337.180000000000000007"),
        "chainlink_sample_second_ms": sample_second_ms,
        "chainlink_provider_event_ms": sample_second_ms + 111,
        "chainlink_received_ms": sample_second_ms + 222,
        "chainlink_source_age_ms": 889,
        "chainlink_received_age_ms": 778,
        "chainlink_fresh": True,
        "price_distance": Decimal("0.064158559835000007"),
        "absolute_price_distance": Decimal("0.064158559835000007"),
        "apparent_side": "Up",
        "up_bid": Decimal("0.81000000"),
        "up_ask": Decimal("0.82000000"),
        "down_bid": Decimal("0.17000000"),
        "down_ask": Decimal("0.18000000"),
        "probability_fresh": True,
        "official_winner": "Down",
        "flipped_after_cutoff": True,
        "microstructure_sample_second_ms": sample_second_ms,
        "microstructure_available": True,
        "microstructure": microstructure_row(
            sample_second_ms=sample_second_ms,
            collector_healthy=False,
        ),
        "quality_flags": [],
    }


def flip_evidence_analysis(*, events=None, cutoffs=None):
    if events is None:
        events = [flip_evidence_event()]
    if cutoffs is None:
        cutoffs = [flip_evidence_cutoff()]
    return {
        "evaluation": {
            **flip_market_row(),
            "definition_version": 2,
            "crossing_count": len(events),
            "touch_count": 0,
            "observation_precision": "exact_twap_event",
            "analysis_start_ms": 1_783_459_480_000,
            "analysis_end_ms": 1_783_459_500_000,
            "chainlink_observation_count": 20,
            "chainlink_strict_observation_count": 20,
            "chainlink_cutoff_count": 20,
            "fresh_chainlink_cutoff_count": 20,
            "probability_cutoff_count": 20,
            "fresh_probability_cutoff_count": 20,
            "microstructure_cutoff_count": len(cutoffs),
            "quality_flags": [],
            "evaluated_ms": 1_783_459_540_000,
        },
        "events": events,
        "cutoffs": cutoffs,
    }


def flip_evidence_market_payload():
    payload = market_data_payload(
        include_probabilities=True,
        include_futures=True,
        include_oi=True,
        include_flow=True,
        include_book=True,
    )
    template = payload["series"][0]
    series = []
    for offset in range(300):
        timestamp_ms = 1_783_459_200_000 + offset * 1_000
        item = deepcopy(template)
        item.update(
            {
                "t": offset,
                "timestamp_ms": timestamp_ms,
                "timestamp_at": datetime.fromtimestamp(
                    timestamp_ms / 1_000,
                    tz=timezone.utc,
                )
                .isoformat()
                .replace("+00:00", "Z"),
            }
        )
        series.append(item)
    payload["series"] = series
    return payload


def install_flip_evidence_sources(
    client,
    monkeypatch,
    *,
    analysis=None,
    microstructure_rows=(),
):
    if analysis is None:
        analysis = flip_evidence_analysis()
    calls = {"market": [], "microstructure": []}

    async def fake_fetch_market_flip_analysis(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs == {
            "market_id": 5_944_864,
            "definition_version": 2,
        }
        return deepcopy(analysis)

    async def fake_fetch_market_download_payload(pool, **kwargs):
        assert pool is client.fake_pool
        calls["market"].append(kwargs)
        return flip_evidence_market_payload()

    async def fake_fetch_market_microstructure_rows(pool, *, market_id):
        assert pool is client.fake_pool
        calls["microstructure"].append(market_id)
        return deepcopy(list(microstructure_rows))

    monkeypatch.setattr(
        api,
        "fetch_market_flip_analysis",
        fake_fetch_market_flip_analysis,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        fake_fetch_market_microstructure_rows,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_460_100_123)
    return calls


def test_markets_flips_serializes_decimals_filters_and_exclusive_cursor(
    client,
    monkeypatch,
):
    async def fake_fetch_flip_markets(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs == {
            "definition_version": 2,
            "within_seconds": 5,
            "kind": "any_crossing",
            "direction": "up_to_down",
            "winner": "Down",
            "start_ms": 1_783_000_000_000,
            "end_ms": 1_784_000_000_000,
            "before_market_id": 5_944_900,
            "limit": 3,
        }
        return [
            flip_market_row(),
            flip_market_row(
                market_id=5_944_863,
                market_start_ms=1_783_458_900_000,
                market_end_ms=1_783_459_200_000,
                matching_crossing_count=1,
            ),
            flip_market_row(
                market_id=5_944_862,
                market_start_ms=1_783_458_600_000,
                market_end_ms=1_783_458_900_000,
            ),
        ]

    monkeypatch.setattr(api, "fetch_flip_markets", fake_fetch_flip_markets)
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_460_100_123)

    response = client.get(
        "/markets/flips"
        "?definition_version=2"
        "&within_seconds=5"
        "&kind=any_crossing"
        "&direction=up_to_down"
        "&winner=Down"
        "&limit=2"
        "&before_market_id=5944900"
        "&start_ms=1783000000000"
        "&end_ms=1784000000000"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 2
    assert body["definition_version"] == 2
    assert body["server_time_ms"] == 1_783_460_100_123
    assert body["filters"] == {
        "within_seconds": 5,
        "kind": "any_crossing",
        "direction": "up_to_down",
        "winner": "Down",
        "start_ms": 1_783_000_000_000,
        "end_ms": 1_784_000_000_000,
    }
    assert len(body["markets"]) == 2
    assert body["next_before_market_id"] == 5_944_863
    market = body["markets"][0]
    assert market["price_to_beat"] == "63337.115841440165000000"
    assert market["official_close"] == "63336.719008471390000000"
    assert market["settlement"] == {
        "reference": "chainlink_twap",
        "window_s": 30,
        "source_url": (
            "https://data.chain.link/streams/btc-usd-twap-30s-streams"
        ),
        "rule_version": "btc-5m-twap-30",
        "price_to_beat": "63337.115841440165000000",
        "official_final_price": "63336.719008471390000000",
    }
    assert market["matching_crossing_count"] == 2
    assert market["decisive_flip"] == {
        "direction": "up_to_down",
        "observed_ms_before_end": 1_700,
    }
    assert market["archive"] == {
        "status": "complete",
        "source_microstructure_rows": 299,
        "archived_microstructure_rows": 299,
        "retention_safe": True,
        "archived_at_ms": 1_783_459_560_000,
    }
    assert market["flip_detail_url"] == (
        "/markets/5944864/flips?definition_version=2"
    )
    assert market["data_url"] == "/markets/5944864/data"
    assert market["evidence_url"] == (
        "/markets/5944864/flips/data?definition_version=2"
    )
    assert (
        market["evidence_download_url"]
        == "/markets/5944864/flips/download?definition_version=2"
    )


def test_markets_flips_returns_empty_list_with_200(client, monkeypatch):
    async def fake_fetch_flip_markets(pool, **kwargs):
        assert kwargs["limit"] == 21
        assert kwargs["definition_version"] == api.FLIP_DEFINITION_VERSION == 3
        return []

    monkeypatch.setattr(api, "fetch_flip_markets", fake_fetch_flip_markets)
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_786_665_900_123)

    response = client.get("/markets/flips")

    assert response.status_code == 200
    assert response.json() == {
        "schema_version": 2,
        "definition_version": api.FLIP_DEFINITION_VERSION,
        "server_time_ms": 1_786_665_900_123,
        "filters": {
            "within_seconds": 20,
            "kind": "any_crossing",
        },
        "markets": [],
        "next_before_market_id": None,
    }


def test_markets_flips_v2_reads_legacy_market_and_versions_all_links(
    client,
    monkeypatch,
):
    async def fake_fetch_flip_markets(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs["definition_version"] == 2
        return [
            flip_market_row(
                settlement_window_s=30,
                settlement_source_url=(
                    "https://data.chain.link/streams/"
                    "btc-usd-twap-30s-streams"
                ),
                settlement_rule_version="btc-5m-twap-30",
            )
        ]

    monkeypatch.setattr(api, "fetch_flip_markets", fake_fetch_flip_markets)
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_460_100_123)

    response = client.get("/markets/flips?definition_version=2")

    assert response.status_code == 200
    body = response.json()
    assert body["definition_version"] == 2
    market = body["markets"][0]
    assert market["settlement"]["window_s"] == 30
    assert market["settlement"]["rule_version"] == "btc-5m-twap-30"
    assert market["flip_detail_url"] == (
        "/markets/5944864/flips?definition_version=2"
    )
    assert market["evidence_url"] == (
        "/markets/5944864/flips/data?definition_version=2"
    )
    assert market["evidence_download_url"] == (
        "/markets/5944864/flips/download?definition_version=2"
    )


@pytest.mark.parametrize(
    "query",
    [
        "definition_version=0",
        "definition_version=4",
        "within_seconds=0",
        "within_seconds=21",
        "limit=0",
        "limit=51",
        "kind=unknown",
        "direction=sideways",
        "winner=up",
        "before_market_id=-1",
        "start_ms=-1",
        "end_ms=-1",
    ],
)
def test_markets_flips_rejects_invalid_query_before_database_access(
    client,
    monkeypatch,
    query,
):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid flip query must not access PostgreSQL")

    monkeypatch.setattr(api, "fetch_flip_markets", unexpected_fetch)

    response = client.get(f"/markets/flips?{query}")

    assert response.status_code == 422


def test_markets_flips_rejects_reversed_date_range_before_database_access(
    client,
    monkeypatch,
):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid flip date range must not access PostgreSQL")

    monkeypatch.setattr(api, "fetch_flip_markets", unexpected_fetch)

    response = client.get("/markets/flips?start_ms=2000&end_ms=2000")

    assert response.status_code == 422
    assert response.json() == {"detail": "start_ms must be less than end_ms"}


def test_markets_flips_distribution_serializes_rates_and_keeps_zero_buckets(
    client,
    monkeypatch,
):
    async def fake_fetch_flip_distribution(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs == {
            "definition_version": 2,
            "max_seconds": 5,
            "direction": "up_to_down",
            "start_ms": 1_783_000_000_000,
            "end_ms": 1_784_000_000_000,
        }
        return {
            "population": {
                "resolved_markets": 2_050,
                "eligible_markets": 2_012,
                "ambiguous_markets": 38,
                "markets_with_any_crossing": 214,
            },
            "crossings_by_time": [
                {
                    "from_ms_before_end": 5_000,
                    "to_ms_before_end": 4_000,
                    "crossing_event_count": 0,
                    "unique_market_count": 0,
                    "decisive_flip_market_count": 0,
                    "to_up_count": 0,
                    "to_down_count": 0,
                    "cumulative_unique_markets_within_window": 33,
                    "cumulative_market_rate": Decimal("0.01640159"),
                }
            ],
            "cutoff_reversals": [
                {
                    "seconds_before_end": 5,
                    "eligible_markets": 2_008,
                    "markets_reversed_by_close": 151,
                    "reversal_rate": Decimal("0.07519920"),
                }
            ],
        }

    monkeypatch.setattr(
        api,
        "fetch_flip_distribution",
        fake_fetch_flip_distribution,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_460_100_123)

    response = client.get(
        "/markets/flips/distribution"
        "?definition_version=2"
        "&max_seconds=5"
        "&direction=up_to_down"
        "&start_ms=1783000000000"
        "&end_ms=1784000000000"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["definition_version"] == 2
    assert body["max_seconds"] == 5
    assert body["population"]["eligible_markets"] == 2_012
    assert body["crossings_by_time"][0]["crossing_event_count"] == 0
    assert body["crossings_by_time"][0]["cumulative_market_rate"] == "0.01640159"
    assert body["cutoff_reversals"][0]["reversal_rate"] == "0.07519920"


def test_markets_flip_detail_serializes_events_cutoffs_and_microstructure(
    client,
    monkeypatch,
):
    async def fake_fetch_market_flip_analysis(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs == {
            "market_id": 5_944_864,
            "definition_version": 2,
        }
        return {
            "evaluation": {
                **flip_market_row(),
                "definition_version": 2,
                "crossing_count": 3,
                "touch_count": 1,
                "observation_precision": "one_second_summary",
                "analysis_start_ms": 1_783_459_480_000,
                "analysis_end_ms": 1_783_459_500_000,
                "chainlink_observation_count": 19,
                "chainlink_strict_observation_count": 18,
                "chainlink_first_provider_event_ms": 1_783_459_480_100,
                "chainlink_last_provider_event_ms": 1_783_459_499_100,
                "chainlink_max_gap_ms": 2_000,
                "chainlink_cutoff_count": 20,
                "fresh_chainlink_cutoff_count": 19,
                "probability_cutoff_count": 20,
                "fresh_probability_cutoff_count": 18,
                "microstructure_cutoff_count": 19,
                "quality_flags": ["missing_microstructure_cutoff"],
                "evaluated_ms": 1_783_459_540_000,
            },
            "events": [
                {
                    "event_sequence": 1,
                    "direction": "up_to_down",
                    "previous_side": "Up",
                    "new_side": "Down",
                    "previous_price": Decimal("63337.20"),
                    "new_price": Decimal("63336.90"),
                    "previous_sample_second_ms": 1_783_459_497_000,
                    "sample_second_ms": 1_783_459_498_000,
                    "previous_provider_event_ms": 1_783_459_497_100,
                    "provider_event_ms": 1_783_459_498_100,
                    "previous_received_ms": 1_783_459_497_150,
                    "received_ms": 1_783_459_498_150,
                    "observation_gap_ms": 1_000,
                    "observed_ms_before_end": 1_900,
                    "is_decisive": True,
                    "observation_precision": "one_second_summary",
                }
            ],
            "cutoffs": [
                {
                    "seconds_before_end": 5,
                    "cutoff_ms": 1_783_459_495_000,
                    "chainlink_price": Decimal("63337.18"),
                    "chainlink_sample_second_ms": 1_783_459_494_000,
                    "chainlink_provider_event_ms": 1_783_459_494_100,
                    "chainlink_received_ms": 1_783_459_494_150,
                    "chainlink_source_age_ms": 900,
                    "chainlink_received_age_ms": 850,
                    "chainlink_fresh": True,
                    "price_distance": Decimal("0.064158559835"),
                    "absolute_price_distance": Decimal("0.064158559835"),
                    "apparent_side": "Up",
                    "up_bid": Decimal("0.81"),
                    "up_ask": Decimal("0.82"),
                    "up_mid": Decimal("0.815"),
                    "up_prob_norm": Decimal("0.82"),
                    "down_bid": Decimal("0.17"),
                    "down_ask": Decimal("0.18"),
                    "down_mid": Decimal("0.175"),
                    "down_prob_norm": Decimal("0.18"),
                    "probability_sample_second_ms": 1_783_459_494_000,
                    "probability_provider_event_ms": 1_783_459_494_100,
                    "probability_received_ms": 1_783_459_494_175,
                    "probability_source_age_ms": 950,
                    "probability_received_age_ms": 850,
                    "up_probability_provider_event_ms": 1_783_459_494_100,
                    "up_probability_received_ms": 1_783_459_494_175,
                    "up_probability_source_age_ms": 900,
                    "up_probability_received_age_ms": 825,
                    "down_probability_provider_event_ms": 1_783_459_494_050,
                    "down_probability_received_ms": 1_783_459_494_150,
                    "down_probability_source_age_ms": 950,
                    "down_probability_received_age_ms": 850,
                    "probability_fresh": True,
                    "official_winner": "Down",
                    "flipped_after_cutoff": True,
                    "microstructure_sample_second_ms": 1_783_459_494_000,
                    "microstructure_available": True,
                    "microstructure": microstructure_row(),
                    "quality_flags": [],
                }
            ],
        }

    monkeypatch.setattr(
        api,
        "fetch_market_flip_analysis",
        fake_fetch_market_flip_analysis,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_460_100_123)

    response = client.get("/markets/5944864/flips?definition_version=2")

    assert response.status_code == 200
    body = response.json()
    assert body["definition_version"] == 2
    assert body["market"]["price_to_beat"] == "63337.115841440165000000"
    assert body["evaluation"]["status"] == "confirmed_flip"
    assert body["evaluation"]["crossing_count"] == 3
    assert body["evaluation"]["twap"]["observation_count"] == 19
    assert body["evaluation"]["cutoff_coverage"] == {
        "twap_count": 20,
        "fresh_twap_count": 19,
        "probability_count": 20,
        "fresh_probability_count": 18,
        "microstructure_count": 19,
    }
    assert body["events"][0]["previous_twap_price"] == "63337.20"
    assert body["events"][0]["new_twap_price"] == "63336.90"
    assert body["cutoffs"][0]["twap"]["price"] == "63337.18"
    cutoff = body["cutoffs"][0]
    assert cutoff["signed_distance"] == "0.064158559835"
    assert cutoff["probabilities"]["up"]["ask"] == "0.82"
    assert cutoff["probabilities"]["up"]["source_age_ms"] == 900
    assert cutoff["probabilities"]["down"]["received_age_ms"] == 850
    assert cutoff["probabilities"]["age_ms"] == 950
    assert cutoff["probabilities"]["received_age_ms"] == 850
    assert cutoff["flipped_after_cutoff"] is True
    assert cutoff["microstructure"]["books"]["spot"]["bid"] == (
        "65757.900000000000000000"
    )
    assert body["archive"]["status"] == "complete"
    assert body["data_url"] == "/markets/5944864/data"
    assert body["evidence_url"] == (
        "/markets/5944864/flips/data?definition_version=2"
    )
    assert (
        body["evidence_download_url"]
        == "/markets/5944864/flips/download?definition_version=2"
    )


def test_markets_flip_detail_returns_404_when_not_evaluated(client, monkeypatch):
    async def fake_fetch_market_flip_analysis(pool, **kwargs):
        return None

    monkeypatch.setattr(
        api,
        "fetch_market_flip_analysis",
        fake_fetch_market_flip_analysis,
    )

    response = client.get("/markets/5944864/flips")

    assert response.status_code == 404
    assert response.json() == {
        "detail": "no flip analysis found for market_id=5944864"
    }


def test_markets_flip_data_defaults_to_decisive_window_and_all_evidence(
    client,
    monkeypatch,
):
    microstructure_rows = [
        microstructure_row(
            sample_second_ms=1_783_459_300_000,
            collector_healthy=True,
        ),
        microstructure_row(
            sample_second_ms=1_783_459_468_000,
            collector_healthy=True,
        ),
        microstructure_row(
            sample_second_ms=1_783_459_490_000,
            collector_healthy=False,
        ),
    ]
    calls = install_flip_evidence_sources(
        client,
        monkeypatch,
        microstructure_rows=microstructure_rows,
    )

    response = client.get(
        "/markets/5944864/flips/data?definition_version=2"
    )

    assert response.status_code == 200
    assert calls["market"] == [
        {
            "market_id": 5_944_864,
            "server_time_ms": 1_783_460_100_123,
            "include_probabilities": True,
            "include_futures": True,
            "include_oi": True,
            "include_flow": True,
            "include_book": True,
            "fill_display": False,
            "max_carry_forward_ms": 10_000,
        }
    ]
    assert calls["microstructure"] == [5_944_864]

    body = response.json()
    assert body["schema_version"] == 2
    assert body["definition_version"] == 2
    assert body["market_data_schema_version"] == 4
    assert body["data_scope"] == "curated_public_api"
    assert body["server_time_ms"] == 1_783_460_100_123
    assert body["market"]["price_to_beat"] == (
        "63337.115841440165000000"
    )
    assert body["market"]["official_close"] == "63336.719008471390000000"

    anchor = body["selection"]["anchor"]
    assert anchor["selection_reason"] == "decisive_event"
    assert anchor["sample_second_ms"] == 1_783_459_498_000
    assert anchor["event"]["event_sequence"] == 1
    assert anchor["event"]["previous_twap_price"] == (
        "63337.250000000000000001"
    )
    assert anchor["event"]["new_twap_price"] == (
        "63336.990000000000000009"
    )
    assert (
        anchor["event"]["previous_provider_event_ms"]
        == 1_783_459_497_111
    )
    assert anchor["event"]["new_provider_event_ms"] == 1_783_459_498_222
    assert anchor["event"]["new_sample_second_ms"] == 1_783_459_498_000

    window = body["selection"]["window"]
    assert window == {
        "before_seconds": 30,
        "start_ms": 1_783_459_468_000,
        "end_ms_exclusive": 1_783_459_500_000,
        "row_count": 32,
        "rows_before_anchor": 30,
        "rows_at_or_after_anchor_second": 2,
        "clipped_at_market_start": False,
    }
    assert body["series"][0]["timestamp_ms"] == 1_783_459_468_000
    assert body["series"][0]["timestamp_at"] == "2026-07-07T21:24:28Z"
    assert body["series"][-1]["timestamp_ms"] == 1_783_459_499_000
    assert body["series"][-1]["timestamp_at"] == "2026-07-07T21:24:59Z"

    assert body["availability"]["selected_window"] == {
        "series_rows": 32,
        "binance_price_rows": 32,
        "chainlink_price_rows": 32,
        "twap_price_rows": 32,
        "probability_rows": 32,
        "futures_rows": 32,
        "open_interest_rows": 32,
        "flow_rows": 32,
        "book_rows": 32,
        "microstructure_rows": 2,
        "microstructure_healthy_rows": 1,
    }
    assert body["availability"]["full_market"] == {
        "series_rows": 300,
        "binance_price_rows": 300,
        "chainlink_price_rows": 300,
        "twap_price_rows": 300,
        "probability_rows": 300,
        "futures_rows": 300,
        "open_interest_rows": 300,
        "flow_rows": 300,
        "book_rows": 300,
        "microstructure_rows": 3,
        "microstructure_healthy_rows": 2,
    }

    row_by_second = {
        row["timestamp_ms"]: row
        for row in body["series"]
    }
    archived_row = row_by_second[1_783_459_490_000]["microstructure"]
    assert archived_row["collector_healthy"] is False
    assert archived_row["books"]["spot"]["bid"] == (
        "65757.900000000000000000"
    )
    assert archived_row["cross_market"]["funding_rate"] == (
        "0.000100000000000000"
    )
    assert archived_row["quality"]["received_ms"] == 1_783_459_201_250

    assert body["flip"]["cutoff_microstructure_rows_reused_from_series"] == 1
    cutoff = body["flip"]["cutoffs"][0]
    assert cutoff["twap"]["price"] == "63337.180000000000000007"
    assert cutoff["twap"]["provider_event_ms"] == 1_783_459_490_111
    assert cutoff["microstructure_reused_from_series"] is True
    assert "microstructure" not in cutoff
    assert body["previous_5m_oi_summary"]["sum_open_interest"] == "74000.123"
    assert body["navigation"] == {
        "older_page_cursor": 5_944_864,
        "list_parameter": "before_market_id",
        "preserve_list_filters": True,
    }


def test_flip_cutoff_microstructure_stays_inline_when_series_slot_is_null(
    client,
    monkeypatch,
):
    install_flip_evidence_sources(client, monkeypatch)

    response = client.get(
        "/markets/5944864/flips/data?definition_version=2"
    )

    assert response.status_code == 200
    body = response.json()
    row_by_second = {
        row["timestamp_ms"]: row
        for row in body["series"]
    }
    assert row_by_second[1_783_459_490_000]["microstructure"] is None
    assert body["flip"]["cutoff_microstructure_rows_reused_from_series"] == 0
    cutoff = body["flip"]["cutoffs"][0]
    assert cutoff["microstructure_reused_from_series"] is False
    assert cutoff["microstructure"]["collector_healthy"] is False
    assert cutoff["microstructure"]["books"]["spot"]["bid"] == (
        "65757.900000000000000000"
    )


def test_flip_microstructure_groups_filter_series_cutoff_and_links(
    client,
    monkeypatch,
):
    install_flip_evidence_sources(
        client,
        monkeypatch,
        microstructure_rows=[
            microstructure_row(sample_second_ms=1_783_459_498_000)
        ],
    )

    response = client.get(
        "/markets/5944864/flips/data"
        "?definition_version=2"
        "&view=event_window"
        "&before_seconds=1"
        "&event_sequence=1"
        "&microstructure_groups=books,quality"
    )

    assert response.status_code == 200
    body = response.json()
    row_by_second = {
        row["timestamp_ms"]: row
        for row in body["series"]
    }
    series_microstructure = row_by_second[
        1_783_459_498_000
    ]["microstructure"]
    assert set(series_microstructure) == {
        "collector_healthy",
        "books",
        "quality",
    }

    cutoff = body["flip"]["cutoffs"][0]
    assert cutoff["microstructure_reused_from_series"] is False
    assert set(cutoff["microstructure"]) == {
        "collector_healthy",
        "books",
        "quality",
    }
    expected_query = (
        "?definition_version=2"
        "&view=event_window"
        "&before_seconds=1"
        "&event_sequence=1"
        "&microstructure_groups=books,quality"
    )
    assert body["links"]["evidence"] == (
        f"/markets/5944864/flips/data{expected_query}"
    )
    assert body["links"]["evidence_download"] == (
        f"/markets/5944864/flips/download{expected_query}"
    )


def test_markets_flip_data_event_sequence_overrides_decisive_anchor(
    client,
    monkeypatch,
):
    requested_event = flip_evidence_event(
        event_sequence=1,
        sample_second_ms=1_783_459_485_000,
        is_decisive=False,
    )
    decisive_event = flip_evidence_event(
        event_sequence=2,
        sample_second_ms=1_783_459_498_000,
        is_decisive=True,
    )
    analysis = flip_evidence_analysis(
        events=[requested_event, decisive_event],
        cutoffs=[],
    )
    install_flip_evidence_sources(
        client,
        monkeypatch,
        analysis=analysis,
    )

    response = client.get(
        "/markets/5944864/flips/data"
        "?definition_version=2&event_sequence=1&before_seconds=3"
    )

    assert response.status_code == 200
    body = response.json()
    anchor = body["selection"]["anchor"]
    assert anchor["selection_reason"] == "requested_event_sequence"
    assert anchor["sample_second_ms"] == 1_783_459_485_000
    assert anchor["event"]["event_sequence"] == 1
    assert anchor["event"]["is_decisive"] is False
    assert anchor["event"]["new_sample_second_ms"] == 1_783_459_485_000
    assert body["selection"]["window"] == {
        "before_seconds": 3,
        "start_ms": 1_783_459_482_000,
        "end_ms_exclusive": 1_783_459_500_000,
        "row_count": 18,
        "rows_before_anchor": 3,
        "rows_at_or_after_anchor_second": 15,
        "clipped_at_market_start": False,
    }


def test_select_flip_anchor_event_falls_back_to_latest_event():
    selected, reason = api.select_flip_anchor_event(
        [
            {"event_sequence": 2, "is_decisive": False},
            {"event_sequence": 9, "is_decisive": False},
            {"event_sequence": 4, "is_decisive": False},
        ],
        event_sequence=None,
    )

    assert selected["event_sequence"] == 9
    assert reason == "latest_event_fallback"


def test_markets_flip_data_full_view_returns_all_300_rows(
    client,
    monkeypatch,
):
    install_flip_evidence_sources(client, monkeypatch)

    response = client.get(
        "/markets/5944864/flips/data"
        "?definition_version=2&view=full&before_seconds=0"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["selection"]["view"] == "full"
    assert body["selection"]["window"] == {
        "before_seconds": None,
        "start_ms": 1_783_459_200_000,
        "end_ms_exclusive": 1_783_459_500_000,
        "row_count": 300,
        "rows_before_anchor": 298,
        "rows_at_or_after_anchor_second": 2,
        "clipped_at_market_start": False,
    }
    assert len(body["series"]) == 300
    assert body["series"][0]["t"] == 0
    assert body["series"][-1]["t"] == 299
    assert body["availability"]["selected_window"] == (
        body["availability"]["full_market"]
    )


@pytest.mark.parametrize(
    "query",
    [
        "view=compact",
        "before_seconds=-1",
        "before_seconds=121",
        "microstructure_groups=books,unknown",
        "microstructure_groups=",
    ],
)
def test_markets_flip_data_rejects_invalid_query_before_database_access(
    client,
    monkeypatch,
    query,
):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid evidence query must not access PostgreSQL")

    monkeypatch.setattr(api, "fetch_market_flip_analysis", unexpected_fetch)
    monkeypatch.setattr(api, "fetch_market_download_payload", unexpected_fetch)
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        unexpected_fetch,
    )

    response = client.get(f"/markets/5944864/flips/data?{query}")

    assert response.status_code == 422


def test_markets_flip_data_without_events_uses_market_end_fallback(
    client,
    monkeypatch,
):
    calls = install_flip_evidence_sources(
        client,
        monkeypatch,
        analysis=flip_evidence_analysis(events=[], cutoffs=[]),
    )

    response = client.get(
        "/markets/5944864/flips/data?definition_version=2"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["selection"]["anchor"] == {
        "selection_reason": "market_end_fallback",
        "sample_second_ms": 1_783_459_500_000,
        "event": None,
    }
    assert body["selection"]["window"] == {
        "before_seconds": 30,
        "start_ms": 1_783_459_470_000,
        "end_ms_exclusive": 1_783_459_500_000,
        "row_count": 30,
        "rows_before_anchor": 30,
        "rows_at_or_after_anchor_second": 0,
        "clipped_at_market_start": False,
    }
    assert body["series"][0]["timestamp_ms"] == 1_783_459_470_000
    assert body["series"][-1]["timestamp_ms"] == 1_783_459_499_000
    assert len(calls["market"]) == 1
    assert calls["microstructure"] == [5_944_864]


@pytest.mark.parametrize(
    ("case", "query", "detail"),
    [
        (
            "missing_analysis",
            "",
            "no flip analysis found for market_id=5944864",
        ),
        (
            "missing_event_sequence",
            "?event_sequence=99",
            "no flip event_sequence=99 found for market_id=5944864",
        ),
    ],
)
def test_markets_flip_data_returns_404_before_market_data_query(
    client,
    monkeypatch,
    case,
    query,
    detail,
):
    if case == "missing_analysis":
        analysis = None
    else:
        analysis = flip_evidence_analysis()

    async def fake_fetch_market_flip_analysis(pool, **kwargs):
        assert pool is client.fake_pool
        return deepcopy(analysis)

    async def unexpected_market_fetch(*args, **kwargs):
        raise AssertionError("404 evidence lookup must not query market data")

    monkeypatch.setattr(
        api,
        "fetch_market_flip_analysis",
        fake_fetch_market_flip_analysis,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        unexpected_market_fetch,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        unexpected_market_fetch,
    )

    response = client.get(f"/markets/5944864/flips/data{query}")

    assert response.status_code == 404
    assert response.json() == {"detail": detail}


def test_markets_flip_download_matches_json_endpoint_and_sets_filename(
    client,
    monkeypatch,
):
    calls = install_flip_evidence_sources(
        client,
        monkeypatch,
        microstructure_rows=[
            microstructure_row(sample_second_ms=1_783_459_498_000)
        ],
    )
    query = (
        "?definition_version=2"
        "&event_sequence=1"
        "&before_seconds=5"
        "&microstructure_groups=books,quality"
    )

    data_response = client.get(f"/markets/5944864/flips/data{query}")
    download_response = client.get(
        f"/markets/5944864/flips/download{query}"
    )

    assert data_response.status_code == 200
    assert download_response.status_code == 200
    assert download_response.headers["content-type"] == "application/json"
    assert download_response.headers["content-disposition"] == (
        'attachment; filename="btc_5m_flip_5944864_event_window.json"'
    )
    assert download_response.json() == data_response.json()
    assert len(calls["market"]) == 2
    assert calls["microstructure"] == [5_944_864, 5_944_864]


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
    assert body["schema_version"] == 4
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


def test_markets_data_by_id_can_include_selected_microstructure_groups(
    client,
    monkeypatch,
):
    async def fake_fetch_market_download_payload(pool, **kwargs):
        assert pool is client.fake_pool
        assert kwargs["market_id"] == 5_944_864
        return market_data_payload()

    async def fake_fetch_market_microstructure_rows(pool, *, market_id):
        assert pool is client.fake_pool
        assert market_id == 5_944_864
        return [microstructure_row()]

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        fake_fetch_market_microstructure_rows,
    )

    response = client.get(
        "/markets/5944864/data"
        "?include_microstructure=true"
        "&microstructure_groups=books,flow,liquidations,quality"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 4
    assert body["availability"] == {
        "microstructure_rows": 1,
        "microstructure_healthy_rows": 1,
        "microstructure_missing_seconds": 299,
    }
    microstructure = body["series"][0]["microstructure"]
    assert set(microstructure) == {
        "collector_healthy",
        "books",
        "flow",
        "liquidations",
        "quality",
    }
    assert microstructure["collector_healthy"] is True
    assert microstructure["books"]["spot"]["bid"] == (
        "65757.900000000000000000"
    )
    assert microstructure["books"]["spot"]["snapshot_count"] == 4
    assert microstructure["flow"]["spot_buy_usdt"] == (
        "90450.100000000000000000"
    )
    assert microstructure["flow"]["futures_rpi_sell_usdt"] is None
    assert microstructure["liquidations"]["observed_long_usdt"] == (
        "0.000000000000000000"
    )
    assert microstructure["quality"]["spot_book_age_ms"] == 911
    assert microstructure["quality"]["received_ms"] == 1_783_459_201_250


def test_markets_current_data_includes_all_microstructure_groups_by_default(
    client,
    monkeypatch,
):
    async def fake_fetch_market_download_payload(pool, **kwargs):
        return market_data_payload()

    async def fake_fetch_market_microstructure_rows(pool, *, market_id):
        return [microstructure_row(collector_healthy=False)]

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        fake_fetch_market_microstructure_rows,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get("/markets/current/data?include_microstructure=true")

    assert response.status_code == 200
    body = response.json()
    assert body["market"]["market_id"] == 5_944_864
    assert body["availability"]["microstructure_healthy_rows"] == 0
    microstructure = body["series"][0]["microstructure"]
    assert microstructure["cross_market"]["perp_spot_basis_bps"] == "-5.22000000"
    assert microstructure["cross_market"]["seconds_to_funding"] == 2_400
    assert microstructure["collector_healthy"] is False


def test_markets_data_by_id_keeps_old_markets_without_microstructure(
    client,
    monkeypatch,
):
    async def fake_fetch_market_download_payload(pool, **kwargs):
        return market_data_payload()

    async def fake_fetch_market_microstructure_rows(pool, *, market_id):
        return []

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        fake_fetch_market_microstructure_rows,
    )

    response = client.get(
        "/markets/5944864/data?include_microstructure=true"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["series"][0]["microstructure"] is None
    assert body["availability"] == {
        "microstructure_rows": 0,
        "microstructure_healthy_rows": 0,
        "microstructure_missing_seconds": 300,
    }


def test_microstructure_market_history_supports_gzip(client, monkeypatch):
    async def fake_fetch_market_download_payload(pool, **kwargs):
        payload = market_data_payload()
        first = payload["series"][0]
        payload["series"] = [
            {
                **deepcopy(first),
                "t": offset,
                "timestamp_ms": 1_783_459_200_000 + offset * 1_000,
            }
            for offset in range(300)
        ]
        return payload

    async def fake_fetch_market_microstructure_rows(pool, *, market_id):
        return []

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(
        api,
        "fetch_market_microstructure_rows",
        fake_fetch_market_microstructure_rows,
    )

    response = client.get(
        "/markets/5944864/data?include_microstructure=true",
        headers={"Accept-Encoding": "gzip"},
    )

    assert response.status_code == 200
    assert response.headers["content-encoding"] == "gzip"
    assert len(response.json()["series"]) == 300


@pytest.mark.parametrize(
    "query",
    [
        "include_microstructure=true&microstructure_groups=books,unknown",
        "microstructure_groups=books",
        "include_microstructure=true&microstructure_groups=",
        "include_microstructure=true&microstructure_groups=books,,flow",
    ],
)
def test_market_data_rejects_invalid_microstructure_group_selection(
    client,
    monkeypatch,
    query,
):
    async def unexpected_fetch(*args, **kwargs):
        raise AssertionError("invalid groups must be rejected before database access")

    monkeypatch.setattr(api, "fetch_market_download_payload", unexpected_fetch)
    monkeypatch.setattr(api, "fetch_market_microstructure_rows", unexpected_fetch)

    response = client.get(f"/markets/5944864/data?{query}")

    assert response.status_code == 422


def test_openapi_lists_data_flags_and_flip_research_routes(client):
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
    flip_list_params = {
        param["name"]
        for param in schema["paths"]["/markets/flips"]["get"]["parameters"]
    }
    flip_distribution_params = {
        param["name"]
        for param in schema["paths"]["/markets/flips/distribution"]["get"][
            "parameters"
        ]
    }
    flip_detail_params = {
        param["name"]
        for param in schema["paths"]["/markets/{market_id}/flips"]["get"][
            "parameters"
        ]
    }
    evidence_data_params = {
        param["name"]: param
        for param in schema["paths"]["/markets/{market_id}/flips/data"]["get"][
            "parameters"
        ]
    }
    evidence_download_params = {
        param["name"]: param
        for param in schema["paths"][
            "/markets/{market_id}/flips/download"
        ]["get"]["parameters"]
    }

    assert "include_flow" in current_data_params
    assert "include_book" in current_data_params
    assert "include_microstructure" in current_data_params
    assert "microstructure_groups" in current_data_params
    assert "include_flow" in market_data_params
    assert "include_book" in market_data_params
    assert "include_microstructure" in market_data_params
    assert "microstructure_groups" in market_data_params
    assert flip_list_params == {
        "definition_version",
        "within_seconds",
        "kind",
        "direction",
        "winner",
        "limit",
        "before_market_id",
        "start_ms",
        "end_ms",
    }
    assert flip_distribution_params == {
        "definition_version",
        "max_seconds",
        "direction",
        "start_ms",
        "end_ms",
    }
    assert flip_detail_params == {"market_id", "definition_version"}
    expected_evidence_params = {
        "market_id",
        "definition_version",
        "view",
        "before_seconds",
        "event_sequence",
        "microstructure_groups",
    }
    assert set(evidence_data_params) == expected_evidence_params
    assert set(evidence_download_params) == expected_evidence_params
    assert evidence_data_params["view"]["schema"]["default"] == "event_window"
    assert evidence_data_params["view"]["schema"]["enum"] == [
        "event_window",
        "full",
    ]
    assert evidence_data_params["before_seconds"]["schema"]["default"] == 30
    assert evidence_data_params["before_seconds"]["schema"]["minimum"] == 0
    assert evidence_data_params["before_seconds"]["schema"]["maximum"] == 120
    assert evidence_data_params["event_sequence"]["schema"]["anyOf"] == [
        {"type": "integer", "minimum": 1},
        {"type": "null"},
    ]


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
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_786_665_650_123)
    client.fake_live_cache.prices = {
        BINANCE_SPOT_LIVE_KEY: LivePrice(
            value="62067.89",
            source_timestamp_ms=1_786_665_649_900,
            received_ms=1_786_665_649_950,
        ),
        CHAINLINK_LIVE_KEY: LivePrice(
            value="62037.05",
            source_timestamp_ms=1_786_665_647_000,
            received_ms=1_786_665_647_100,
        ),
        TWAP_LIVE_KEY: LivePrice(
            value="62036.987654321098765432",
            source_timestamp_ms=1_786_665_647_500,
            received_ms=1_786_665_647_600,
        ),
        FUTURES_LIVE_KEY: LivePrice(
            value="62099.10",
            source_timestamp_ms=1_786_665_650_000,
            received_ms=1_786_665_650_050,
        ),
    }
    client.fake_live_cache.twap_shadow_snapshot = twap_shadow_snapshot()

    response = client.get("/markets/current/live?max_chainlink_carry_forward_ms=7000")

    assert response.status_code == 200
    body = response.json()
    assert body["server_time_ms"] == 1_786_665_650_123
    assert body["market_id"] == 5_955_552
    assert body["prices"]["binance_spot"]["value"] == "62067.89"
    assert body["prices"]["binance_spot"]["source_timestamp_ms"] == 1_786_665_649_900
    assert body["prices"]["binance_spot"]["provider_event_ms"] == 1_786_665_649_900
    assert body["prices"]["binance_spot"]["source_age_ms"] == 223
    assert body["prices"]["binance_spot"]["received_age_ms"] == 173
    assert body["prices"]["chainlink"]["source_age_ms"] == 3_123
    assert body["prices"]["twap"]["value"] == "62036.987654321098765432"
    assert body["prices"]["twap"]["source_age_ms"] == 2_623
    assert body["futures"]["last"]["source_age_ms"] == 123
    assert body["futures"]["last"]["received_age_ms"] == 73
    assert body["futures"]["last"]["time_ms"] == 1_786_665_650_000
    assert set(body) == {
        "server_time_ms",
        "market_id",
        "market_start_ms",
        "market_end_ms",
        "prices",
        "futures",
        "twap_shadow",
    }
    assert body["twap_shadow"]["model_version"] == 2
    assert body["twap_shadow"]["generated_age_ms"] == 65
    assert body["twap_shadow"]["predictions"]["h1"]["value"] == (
        "62066.123456789012345678"
    )
    assert client.fake_live_cache.requested_combined_keys == [
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            TWAP_LIVE_KEY,
            FUTURES_LIVE_KEY,
            TWAP_SHADOW_LIVE_KEY,
        ]
    ]
    assert client.fake_pool.acquire_calls == 0


def test_markets_current_live_hides_legacy_twap_shadow_model(
    client,
    monkeypatch,
):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_786_665_650_123)
    client.fake_live_cache.twap_shadow_snapshot = {
        **twap_shadow_snapshot(),
        "model_version": 1,
    }

    response = client.get("/markets/current/live")

    assert response.status_code == 200
    assert response.json()["twap_shadow"] is None
    assert client.fake_pool.acquire_calls == 0


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


def test_twap_shadow_history_returns_predictions_actuals_and_realized_errors(
    client,
    monkeypatch,
):
    async def fake_fetch(pool, *, market_id, model_version):
        assert pool is client.fake_pool
        assert market_id == 5_944_864
        assert model_version == 1
        return {
            "market_id": market_id,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "model_version": model_version,
            "rows": [
                {
                    "target_second_ms": 1_783_459_250_000,
                    "actual_price": Decimal("100"),
                    "actual_provider_event_ms": 1_783_459_250_000,
                    "actual_received_ms": 1_783_459_251_800,
                    "h1_price": Decimal("100.01"),
                    "h1_generated_ms": 1_783_459_249_050,
                    "h1_known_fraction": Decimal("1"),
                    "h1_source_count": 3,
                    "h1_estimated_error_bps": Decimal("0.75"),
                    "h3_price": None,
                    "h3_generated_ms": None,
                    "h3_known_fraction": None,
                    "h3_source_count": None,
                    "h3_estimated_error_bps": None,
                    "h5_price": None,
                    "h5_generated_ms": None,
                    "h5_known_fraction": None,
                    "h5_source_count": None,
                    "h5_estimated_error_bps": None,
                    "h10_price": None,
                    "h10_generated_ms": None,
                    "h10_known_fraction": None,
                    "h10_source_count": None,
                    "h10_estimated_error_bps": None,
                }
            ],
        }

    monkeypatch.setattr(api, "fetch_twap_shadow_market_history", fake_fetch)

    response = client.get("/markets/5944864/twap-shadow?model_version=1")

    assert response.status_code == 200
    body = response.json()
    assert body["error_definition"] == (
        "10000 * (prediction - actual) / actual"
    )
    assert body["samples"][0]["actual"]["value"] == "100"
    assert body["samples"][0]["predictions"]["h1"] == {
        "horizon_seconds": 1,
        "value": "100.01",
        "generated_ms": 1_783_459_249_050,
        "known_fraction": "1",
        "source_count": 3,
        "estimated_error_bps": "0.75",
        "realized_error_bps": "1.00000000",
    }
    assert body["summary"]["h1"] == {
        "horizon_seconds": 1,
        "paired_count": 1,
        "mean_error_bps": "1.00000000",
        "mae_bps": "1.00000000",
    }
    assert body["summary"]["h3"]["paired_count"] == 0


def test_twap_shadow_history_returns_404_for_unknown_market(client, monkeypatch):
    async def fake_fetch(pool, *, market_id, model_version):
        return None

    monkeypatch.setattr(api, "fetch_twap_shadow_market_history", fake_fetch)

    response = client.get("/markets/1/twap-shadow")

    assert response.status_code == 404
    assert response.json() == {"detail": "market not found"}


def test_markets_current_microstructure_live_uses_one_cache_read_and_snapshot_market(
    client,
    monkeypatch,
):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_786_665_900_123)
    client.fake_live_cache.prices = {
        BINANCE_SPOT_LIVE_KEY: LivePrice(
            value="65758.01",
            source_timestamp_ms=1_786_665_899_900,
            received_ms=1_786_665_899_950,
        ),
        CHAINLINK_LIVE_KEY: LivePrice(
            value="65721.23639093849",
            source_timestamp_ms=1_786_665_899_000,
            received_ms=1_786_665_899_100,
        ),
        TWAP_LIVE_KEY: LivePrice(
            value="65720.918273645546372819",
            source_timestamp_ms=1_786_665_899_010,
            received_ms=1_786_665_899_110,
        ),
        FUTURES_LIVE_KEY: LivePrice(
            value="65723.70",
            source_timestamp_ms=1_786_665_899_800,
            received_ms=1_786_665_899_850,
        ),
    }
    client.fake_live_cache.microstructure_snapshot = microstructure_row(
        sample_second_ms=1_786_665_899_000,
        received_ms=1_786_665_899_250,
    )

    response = client.get(
        "/markets/current/microstructure/live",
        headers={"Accept-Encoding": "gzip"},
    )

    assert response.status_code == 200
    assert response.headers["content-encoding"] == "gzip"
    body = response.json()
    assert body["schema_version"] == 2
    assert body["server_time_ms"] == 1_786_665_900_123
    assert body["market_id"] == 5_955_552
    assert body["sample_second_ms"] == 1_786_665_899_000
    assert body["served_from"] == "redis"
    assert body["prices"] == {
        "binance_spot": "65758.01",
        "chainlink": "65721.23639093849",
        "twap": "65720.918273645546372819",
        "futures": "65723.70",
    }
    assert body["microstructure"]["collector_healthy"] is True
    assert body["microstructure"]["books"]["futures"]["ask"] == (
        "65723.700000000000000000"
    )
    assert client.fake_live_cache.requested_combined_keys == [
        [
            BINANCE_SPOT_LIVE_KEY,
            CHAINLINK_LIVE_KEY,
            TWAP_LIVE_KEY,
            FUTURES_LIVE_KEY,
            MICROSTRUCTURE_LIVE_KEY,
        ]
    ]
    assert client.fake_pool.acquire_calls == 0


def test_markets_current_microstructure_live_uses_current_market_when_snapshot_missing(
    client,
    monkeypatch,
):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_500_123)

    response = client.get("/markets/current/microstructure/live")

    assert response.status_code == 200
    body = response.json()
    assert body["market_id"] == 5_944_865
    assert body["sample_second_ms"] is None
    assert body["prices"] == {
        "binance_spot": None,
        "chainlink": None,
        "twap": None,
        "futures": None,
    }
    assert body["microstructure"] is None
    assert client.fake_pool.acquire_calls == 0


def test_markets_current_microstructure_live_preserves_stale_snapshot_identity(
    client,
    monkeypatch,
):
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_546_000_123)
    client.fake_live_cache.microstructure_snapshot = microstructure_row(
        sample_second_ms=1_783_459_499_000,
        collector_healthy=True,
        received_ms=1_783_459_500_250,
    )

    response = client.get("/markets/current/microstructure/live")

    assert response.status_code == 200
    body = response.json()
    assert body["server_time_ms"] == 1_783_546_000_123
    assert body["market_id"] == 5_944_864
    assert body["sample_second_ms"] == 1_783_459_499_000
    # Health describes the finalized interval, not current key freshness.
    assert body["microstructure"]["collector_healthy"] is True
    assert body["microstructure"]["quality"]["received_ms"] == 1_783_459_500_250
    assert client.fake_pool.acquire_calls == 0


@pytest.mark.parametrize(
    ("error", "detail"),
    (
        (OSError("redis unavailable"), "live cache unavailable"),
        (LiveCachePayloadError("bad snapshot"), "live cache payload invalid"),
    ),
)
def test_markets_current_microstructure_live_maps_cache_errors(
    client,
    error,
    detail,
):
    client.fake_live_cache.read_error = error

    response = client.get("/markets/current/microstructure/live")

    assert response.status_code == 503
    assert response.json() == {"detail": detail}
    assert client.fake_pool.acquire_calls == 0


def test_markets_current_microstructure_live_rejects_invalid_nested_types(
    client,
):
    client.fake_live_cache.microstructure_snapshot = microstructure_row(
        spot_bid=True,
    )

    response = client.get("/markets/current/microstructure/live")

    assert response.status_code == 503
    assert response.json() == {"detail": "live cache payload invalid"}


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

    assert exported["schema_version"] == 4
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
    assert body["schema_version"] == 4
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
        "twap": "122997.987654321098765432",
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
