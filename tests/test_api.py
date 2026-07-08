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


def market_data_payload(include_probabilities=False):
    row = {
        "t": 0,
        "timestamp_ms": 1_783_459_200_000,
        "timestamp_at": "2026-07-07T21:00:00Z",
        "prices": {
            "binance": "123000.00",
            "chainlink": "122998.12",
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

    return {
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


def test_markets_current_data_uses_current_five_minute_market(client, monkeypatch):
    async def fake_fetch_market_download_payload(pool, market_id, include_probabilities):
        assert market_id == 5_944_864
        assert include_probabilities is False
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


def test_markets_data_by_id_can_include_probabilities(client, monkeypatch):
    async def fake_fetch_market_download_payload(pool, market_id, include_probabilities):
        assert market_id == 5_944_864
        assert include_probabilities is True
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


def test_markets_download_returns_attachment_filename(client, monkeypatch):
    async def fake_fetch_market_download_payload(pool, market_id, include_probabilities):
        assert market_id == 5_944_864
        assert include_probabilities is True
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
        == 'attachment; filename="btc_5m_market_5944864_with_probabilities.json"'
    )
    assert response.json()["series"][0]["probabilities"]["down"]["normalized"] == "0.51758794"
