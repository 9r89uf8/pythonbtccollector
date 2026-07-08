import asyncio
import inspect
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import price_collector.db as db


ROOT = Path(__file__).resolve().parents[1]


def test_create_read_pool_prefers_read_database_url(monkeypatch):
    calls = []

    async def fake_create_pool(database_url):
        calls.append(database_url)
        return "pool"

    settings = SimpleNamespace(
        DATABASE_URL="postgresql://writer@127.0.0.1:5432/price_collector",
        READ_DATABASE_URL="postgresql://reader@127.0.0.1:5432/price_collector",
    )
    monkeypatch.setattr(db, "create_pool", fake_create_pool)

    result = asyncio.run(db.create_read_pool(settings))

    assert result == "pool"
    assert calls == ["postgresql://reader@127.0.0.1:5432/price_collector"]


def test_create_read_pool_falls_back_to_database_url(monkeypatch):
    calls = []

    async def fake_create_pool(database_url):
        calls.append(database_url)
        return "pool"

    settings = SimpleNamespace(
        DATABASE_URL="postgresql://writer@127.0.0.1:5432/price_collector",
        READ_DATABASE_URL=None,
    )
    monkeypatch.setattr(db, "create_pool", fake_create_pool)

    result = asyncio.run(db.create_read_pool(settings))

    assert result == "pool"
    assert calls == ["postgresql://writer@127.0.0.1:5432/price_collector"]


def test_create_read_pool_requires_at_least_one_database_url(monkeypatch):
    async def fake_create_pool(database_url):
        raise AssertionError("create_pool should not be called without a database URL")

    settings = SimpleNamespace(DATABASE_URL=None, READ_DATABASE_URL=None)
    monkeypatch.setattr(db, "create_pool", fake_create_pool)

    with pytest.raises(
        RuntimeError,
        match="READ_DATABASE_URL or DATABASE_URL must be set for the API",
    ):
        asyncio.run(db.create_read_pool(settings))


def test_upsert_price_sample_updates_duplicate_instrument_second_rows():
    source = inspect.getsource(db.upsert_price_sample)

    assert "ON CONFLICT (instrument_id, sample_second_ms)" in source
    assert "DO UPDATE SET" in source


def test_schema_seeds_polymarket_chainlink_provider_and_instrument():
    schema = (ROOT / "schema.sql").read_text()

    assert "polymarket_chainlink_rtds" in schema
    assert "Polymarket RTDS Chainlink BTC/USD" in schema
    assert "'BTCUSD'" in schema
    assert "'crypto_prices_chainlink:btc/usd'" in schema


def test_build_market_sources_summary_returns_both_btc_sources():
    market_start_at = datetime(2026, 7, 7, 21, 0, 0, tzinfo=timezone.utc)
    market_end_at = datetime(2026, 7, 7, 21, 5, 0, tzinfo=timezone.utc)
    rows = [
        {
            "provider": "binance_spot",
            "symbol": "BTCUSDT",
            "quote_asset": "USDT",
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": market_start_at,
            "market_end_at": market_end_at,
            "sample_second_ms": 1_783_459_200_000,
            "price": Decimal("123000.00"),
            "provider_event_ms": 1_783_459_199_950,
            "received_ms": 1_783_459_200_010,
        },
        {
            "provider": "binance_spot",
            "symbol": "BTCUSDT",
            "quote_asset": "USDT",
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": market_start_at,
            "market_end_at": market_end_at,
            "sample_second_ms": 1_783_459_201_000,
            "price": Decimal("123500.00"),
            "provider_event_ms": 1_783_459_200_990,
            "received_ms": 1_783_459_201_020,
        },
        {
            "provider": "polymarket_chainlink_rtds",
            "symbol": "BTCUSD",
            "quote_asset": "USD",
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": market_start_at,
            "market_end_at": market_end_at,
            "sample_second_ms": 1_783_459_200_000,
            "price": Decimal("122998.12"),
            "provider_event_ms": 1_783_459_200_123,
            "received_ms": 1_783_459_200_250,
        },
        {
            "provider": "polymarket_chainlink_rtds",
            "symbol": "BTCUSD",
            "quote_asset": "USD",
            "market_id": 5_944_864,
            "market_start_ms": 1_783_459_200_000,
            "market_end_ms": 1_783_459_500_000,
            "market_start_at": market_start_at,
            "market_end_at": market_end_at,
            "sample_second_ms": 1_783_459_202_000,
            "price": Decimal("123455.90"),
            "provider_event_ms": 1_783_459_202_999,
            "received_ms": 1_783_459_203_020,
        },
    ]

    summary = db.build_market_sources_summary(rows)

    assert summary is not None
    assert summary["market_id"] == 5_944_864
    assert [source["provider"] for source in summary["sources"]] == [
        "binance_spot",
        "polymarket_chainlink_rtds",
    ]
    assert summary["sources"][0]["symbol"] == "BTCUSDT"
    assert summary["sources"][0]["sample_count"] == 2
    assert summary["sources"][0]["open"] == Decimal("123000.00")
    assert summary["sources"][0]["close"] == Decimal("123500.00")
    assert summary["sources"][1]["symbol"] == "BTCUSD"
    assert summary["sources"][1]["sample_count"] == 2
    assert summary["sources"][1]["latest_sample_second_ms"] == 1_783_459_202_000
    assert summary["sources"][1]["latest_provider_event_ms"] == 1_783_459_202_999
