from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import price_collector.api as api
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
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
        self.requested_keys = []

    async def get_prices(self, keys):
        key_list = list(keys)
        self.requested_keys.append(key_list)
        return {key: self.prices.get(key) for key in key_list}

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
        "schema_version": 1,
        "market": {
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": "2026-07-07T21:00:00Z",
            "market_end_at": "2026-07-07T21:05:00Z",
            "seconds_expected": 300,
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
        return market_data_payload()

    monkeypatch.setattr(
        api,
        "fetch_market_download_payload",
        fake_fetch_market_download_payload,
    )
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: 1_783_459_250_123)

    response = client.get("/markets/current/data")

    assert response.status_code == 200
    body = response.json()
    assert body["market"]["market_id"] == 5_944_864
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
    assert client.fake_live_cache.requested_keys == [
        [BINANCE_SPOT_LIVE_KEY, CHAINLINK_LIVE_KEY, FUTURES_LIVE_KEY]
    ]
    assert client.fake_pool.acquire_calls == 0


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
    assert "freshness" not in body["series"][0]


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
        "taker_imbalance": "0.60000000",
        "cvd_10s": "900.12",
        "cvd_30s": "1200.13",
        "imbalance_10s": "0.12345678",
        "imbalance_30s": "-0.23456789",
    }
    assert body["series"][0]["book"] == {
        "book_imbalance": "0.25000000",
        "microprice": "62074.17",
    }
    assert "buy_quote" not in body["series"][0]["flow"]
    assert "bid" not in body["series"][0]["book"]
    assert "freshness" not in body["series"][0]
