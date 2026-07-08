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


def test_upsert_polymarket_probability_sample_updates_duplicate_source_second_rows():
    source = inspect.getsource(db.upsert_polymarket_probability_sample)

    assert "ON CONFLICT (market_id, source, sample_second_ms)" in source
    assert "DO UPDATE SET" in source


def test_upsert_binance_futures_snapshot_updates_duplicate_symbol_second_rows():
    source = inspect.getsource(db.upsert_binance_futures_snapshot)

    assert "await _ensure_market_window(connection, window)" in source
    assert "ON CONFLICT (symbol, sample_second_ms)" in source
    assert "DO UPDATE SET" in source


def test_upsert_binance_futures_oi_5m_summary_updates_duplicate_source_window_rows():
    source = inspect.getsource(db.upsert_binance_futures_oi_5m_summary)

    assert "await _ensure_market_window(connection, effective_window)" in source
    assert "ON CONFLICT (symbol, source_window_start_ms, source_window_end_ms)" in source
    assert "DO UPDATE SET" in source


def test_upsert_polymarket_market_ensures_market_window_before_metadata_insert():
    source = inspect.getsource(db.upsert_polymarket_btc_5m_market)

    assert "await _ensure_market_window(connection, window)" in source
    assert "ON CONFLICT (market_id)" in source


def test_decimal_2dp_or_none():
    assert db.decimal_2dp_or_none(Decimal("62012.870302750816000000")) == "62012.87"
    assert db.decimal_2dp_or_none(Decimal("62067.890000000000000000")) == "62067.89"
    assert db.decimal_2dp_or_none(Decimal("0.55500000")) == "0.56"
    assert db.decimal_2dp_or_none(Decimal("0.55400000")) == "0.55"
    assert db.decimal_2dp_or_none(None) is None


def test_oi_3dp_rounds_contract_values():
    assert db.oi_3dp(Decimal("74321.1234")) == "74321.123"
    assert db.oi_3dp(Decimal("74321.1235")) == "74321.124"
    assert db.oi_3dp(None) is None


def test_schema_seeds_polymarket_chainlink_provider_and_instrument():
    schema = (ROOT / "schema.sql").read_text()

    assert "polymarket_chainlink_rtds" in schema
    assert "Polymarket RTDS Chainlink BTC/USD" in schema
    assert "'BTCUSD'" in schema
    assert "'crypto_prices_chainlink:btc/usd'" in schema


def test_schema_includes_polymarket_probability_tables():
    schema = (ROOT / "schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS polymarket_btc_5m_markets" in schema
    assert "CREATE TABLE IF NOT EXISTS polymarket_probability_samples" in schema
    assert "PRIMARY KEY (market_id, source, sample_second_ms)" in schema
    assert "up_bid NUMERIC(18, 8)" in schema
    assert "down_prob_norm NUMERIC(18, 8)" in schema
    assert "CHECK (sample_second_ms < (market_id + 1) * 300000)" in schema


def test_schema_includes_binance_futures_tables_and_seed():
    schema = (ROOT / "schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS binance_futures_snapshots" in schema
    assert "CREATE TABLE IF NOT EXISTS binance_futures_oi_5m_summaries" in schema
    assert "PRIMARY KEY (symbol, sample_second_ms)" in schema
    assert "PRIMARY KEY (symbol, source_window_start_ms, source_window_end_ms)" in schema
    assert "open_interest NUMERIC(38, 18)" in schema
    assert "premium_bps NUMERIC(20, 8)" in schema
    assert "sum_open_interest_value NUMERIC(38, 18)" in schema
    assert "'binance_usdm_perp'" in schema
    assert "'binance_usdm_perp:BTCUSDT'" in schema


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


def market_download_rows():
    market_start_ms = 1_783_459_200_000
    market_end_ms = 1_783_459_500_000
    market_start_at = datetime(2026, 7, 7, 21, 0, 0, tzinfo=timezone.utc)
    market_end_at = datetime(2026, 7, 7, 21, 5, 0, tzinfo=timezone.utc)
    rows = []

    for t in range(300):
        rows.append(
            {
                "market_id": 5_944_864,
                "market_start_ms": market_start_ms,
                "market_end_ms": market_end_ms,
                "market_start_at": market_start_at,
                "market_end_at": market_end_at,
                "sample_second_ms": market_start_ms + (t * 1000),
                "binance_price": Decimal("123000.004") if t == 0 else None,
                "chainlink_price": Decimal("122998.125") if t == 0 else None,
                "up_bid": Decimal("0.47") if t == 1 else None,
                "up_ask": Decimal("0.485") if t == 1 else None,
                "up_mid": Decimal("0.48") if t == 1 else None,
                "down_bid": Decimal("0.50") if t == 1 else None,
                "down_ask": Decimal("0.534") if t == 1 else None,
                "down_mid": Decimal("0.515") if t == 1 else None,
                "up_prob_norm": Decimal("0.48241206") if t == 1 else None,
                "down_prob_norm": Decimal("0.51758794") if t == 1 else None,
            }
        )

    return rows


def test_build_market_download_payload_returns_300_price_rows_without_probabilities():
    payload = db.build_market_download_payload(
        market_download_rows(),
        include_probabilities=False,
        include_futures=False,
        include_oi=False,
    )

    assert payload is not None
    assert payload["schema_version"] == 1
    assert payload["market"] == {
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "market_start_at": "2026-07-07T21:00:00Z",
        "market_end_at": "2026-07-07T21:05:00Z",
        "seconds_expected": 300,
    }
    assert len(payload["series"]) == 300
    assert [row["t"] for row in payload["series"]] == list(range(300))
    assert payload["series"][0]["prices"] == {
        "binance": "123000.00",
        "chainlink": "122998.13",
    }
    assert payload["series"][1]["prices"] == {"binance": None, "chainlink": None}
    assert "probabilities" not in payload["series"][1]
    assert "futures" not in payload["series"][1]
    assert "open_interest" not in payload["series"][1]
    assert "previous_5m_oi_summary" not in payload


def test_build_market_download_payload_adds_probabilities_only_when_requested():
    payload = db.build_market_download_payload(
        market_download_rows(),
        include_probabilities=True,
        include_futures=False,
        include_oi=False,
    )

    assert payload is not None
    probability_row = payload["series"][1]
    assert probability_row["probabilities"] == {
        "up": {
            "ask": "0.49",
        },
        "down": {
            "ask": "0.53",
        },
    }
    assert payload["series"][2]["probabilities"]["up"]["ask"] is None


def test_download_payload_probability_shape_ask_only():
    payload = db.build_market_download_payload(
        market_download_rows(),
        include_probabilities=True,
        include_futures=False,
        include_oi=False,
    )

    assert payload is not None
    first = payload["series"][0]

    assert set(first["prices"].keys()) == {"binance", "chainlink"}
    assert set(first["probabilities"].keys()) == {"up", "down"}
    assert set(first["probabilities"]["up"].keys()) == {"ask"}
    assert set(first["probabilities"]["down"].keys()) == {"ask"}


def test_build_market_download_payload_adds_optional_futures_and_oi():
    rows = market_download_rows()
    row = rows[12]
    row.update(
        {
            "futures_last_price": Decimal("62075.125"),
            "mark_price": Decimal("62074.884"),
            "index_price": Decimal("62070.185"),
            "last_funding_rate": Decimal("0.00010000"),
            "next_funding_time_ms": 1_783_468_800_000,
            "open_interest": Decimal("74321.1234"),
            "oi_notional_usdt": Decimal("4616789012.345"),
            "premium_bps": Decimal("0.755"),
            "oi_delta_30s": Decimal("12.1234"),
            "oi_delta_60s": Decimal("-3.9876"),
            "oi_delta_300s": None,
            "prev_oi_source_window_start_ms": 1_783_458_900_000,
            "prev_oi_source_window_end_ms": 1_783_459_200_000,
            "prev_oi_sum_open_interest": Decimal("74000.1234"),
            "prev_oi_sum_open_interest_value": Decimal("4590000000.125"),
        }
    )
    for other_row in rows:
        other_row.setdefault("futures_last_price", None)
        other_row.setdefault("mark_price", None)
        other_row.setdefault("index_price", None)
        other_row.setdefault("last_funding_rate", None)
        other_row.setdefault("next_funding_time_ms", None)
        other_row.setdefault("open_interest", None)
        other_row.setdefault("oi_notional_usdt", None)
        other_row.setdefault("premium_bps", None)
        other_row.setdefault("oi_delta_30s", None)
        other_row.setdefault("oi_delta_60s", None)
        other_row.setdefault("oi_delta_300s", None)
        other_row.setdefault("prev_oi_source_window_start_ms", 1_783_458_900_000)
        other_row.setdefault("prev_oi_source_window_end_ms", 1_783_459_200_000)
        other_row.setdefault("prev_oi_sum_open_interest", Decimal("74000.1234"))
        other_row.setdefault(
            "prev_oi_sum_open_interest_value",
            Decimal("4590000000.125"),
        )

    payload = db.build_market_download_payload(
        rows,
        include_probabilities=False,
        include_futures=True,
        include_oi=True,
    )

    assert payload is not None
    item = payload["series"][12]
    assert item["futures"] == {
        "last": "62075.13",
        "mark": "62074.88",
        "index": "62070.19",
        "premium_bps": "0.76",
    }
    assert item["open_interest"] == {
        "contracts": "74321.123",
        "notional_usdt": "4616789012.35",
        "delta_30s": "12.123",
        "delta_60s": "-3.988",
        "delta_300s": None,
    }
    assert payload["previous_5m_oi_summary"] == {
        "source_window_start_ms": 1_783_458_900_000,
        "source_window_end_ms": 1_783_459_200_000,
        "effective_market_id": 5_944_864,
        "sum_open_interest": "74000.123",
        "sum_open_interest_value": "4590000000.13",
    }


def test_fetch_market_download_payload_query_includes_futures_and_oi_joins():
    source = inspect.getsource(db.fetch_market_download_payload)

    assert "include_futures: bool" in source
    assert "include_oi: bool" in source
    assert "FROM binance_futures_snapshots" in source
    assert "FROM binance_futures_oi_5m_summaries" in source
    assert "f.sample_second_ms - 30000" in source
    assert "f.sample_second_ms - 60000" in source
    assert "f.sample_second_ms - 300000" in source
