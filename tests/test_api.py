from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import price_collector.api as api


class FakePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def client(monkeypatch):
    fake_pool = FakePool()

    async def fake_create_read_pool(settings):
        return fake_pool

    async def fake_health_check(pool):
        return None

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://price_reader:secret@127.0.0.1:5432/price_collector",
    )
    monkeypatch.setattr(api, "create_read_pool", fake_create_read_pool)
    monkeypatch.setattr(api, "health_check", fake_health_check)

    with TestClient(api.app) as test_client:
        test_client.fake_pool = fake_pool
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
