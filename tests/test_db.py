import asyncio
import inspect
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import price_collector.db as db
from price_collector.market import MarketWindow


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


def test_upsert_binance_flow_1s_updates_duplicate_venue_symbol_second_rows():
    source = inspect.getsource(db.upsert_binance_flow_1s)

    assert "await _ensure_market_window(connection, window)" in source
    assert "ON CONFLICT (venue, symbol, sample_second_ms)" in source
    assert "DO UPDATE SET" in source
    assert "cvd_10s = EXCLUDED.cvd_10s" in source
    assert "imbalance_30s = EXCLUDED.imbalance_30s" in source


def test_upsert_binance_book_1s_updates_duplicate_venue_symbol_second_rows():
    source = inspect.getsource(db.upsert_binance_book_1s)

    assert "await _ensure_market_window(connection, window)" in source
    assert "ON CONFLICT (venue, symbol, sample_second_ms)" in source
    assert "DO UPDATE SET" in source
    assert "microprice = EXCLUDED.microprice" in source


def test_binance_futures_raw_json_upserts_accept_nullable_raw_payloads():
    snapshot_source = inspect.getsource(db.upsert_binance_futures_snapshot)
    oi_source = inspect.getsource(db.upsert_binance_futures_oi_5m_summary)

    assert "raw: Optional[Mapping[str, Any]]" in snapshot_source
    assert "raw: Optional[Mapping[str, Any]]" in oi_source
    assert "if raw is not None else None" in snapshot_source
    assert "if raw is not None else None" in oi_source


def test_upsert_polymarket_market_ensures_market_window_before_metadata_insert():
    source = inspect.getsource(db.upsert_polymarket_btc_5m_market)

    assert "await _ensure_market_window(connection, window)" in source
    assert "ON CONFLICT (market_id)" in source


def test_fetch_recent_market_windows_uses_time_cursor_and_real_observations():
    row = {
        "market_id": 5_944_864,
        "market_start_ms": 1_783_459_200_000,
        "market_end_ms": 1_783_459_500_000,
        "market_start_at": datetime(2026, 7, 7, 21, 0, tzinfo=timezone.utc),
        "market_end_at": datetime(2026, 7, 7, 21, 5, tzinfo=timezone.utc),
        "is_complete": True,
        "binance_sample_count": 300,
        "chainlink_sample_count": 298,
        "futures_sample_count": 60,
        "open_interest_sample_count": 60,
        "flow_sample_count": 300,
        "book_sample_count": 299,
        "probability_sample_count": 297,
    }

    class FakeAcquire:
        def __init__(self, connection):
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeConnection:
        def __init__(self):
            self.calls = []

        async def fetch(self, query, *args):
            self.calls.append((query, args))
            return [row]

    class FakePool:
        def __init__(self):
            self.connection = FakeConnection()

        def acquire(self):
            return FakeAcquire(self.connection)

    pool = FakePool()
    result = asyncio.run(
        db.fetch_recent_market_windows(
            pool,
            server_time_ms=1_783_459_800_123,
            include_current=False,
            before_market_id=5_944_865,
            limit=3,
        )
    )

    assert result == [row]
    query, args = pool.connection.calls[0]
    assert args == (1_783_459_800_123, False, 5_944_865, 3)

    normalized_query = " ".join(query.split())
    assert "mw.market_start_ms <= $1::BIGINT" in normalized_query
    assert "($2::BOOLEAN OR mw.market_end_ms <= $1::BIGINT)" in normalized_query
    assert "($3::BIGINT IS NULL OR mw.market_id < $3::BIGINT)" in normalized_query
    assert "ORDER BY mw.market_id DESC LIMIT $4::INTEGER" in normalized_query
    assert normalized_query.count("OR EXISTS") == 4
    assert "FROM price_samples" in normalized_query
    assert "FROM binance_futures_snapshots" in normalized_query
    assert "FROM binance_flow_1s" in normalized_query
    assert "FROM binance_book_1s" in normalized_query
    assert "FROM polymarket_probability_samples" in normalized_query
    assert "f.futures_last_price IS NOT NULL" in normalized_query
    assert "f.open_interest IS NOT NULL" in normalized_query
    assert "probabilities.up_ask IS NOT NULL" in normalized_query
    assert "probabilities.down_ask IS NOT NULL" in normalized_query
    assert "ORDER BY candidates.market_id DESC" in normalized_query


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


def test_freshness_meta_uses_source_received_and_transport_timestamps():
    assert db.age_ms(10_000, 9_250) == 750
    assert db.age_ms(10_000, 10_250) == 0
    assert db.age_ms(10_000, None) is None
    assert db.freshness_meta(
        server_time_ms=10_000,
        source_time_ms=9_250,
        received_ms=9_500,
    ) == {
        "source_age_ms": 750,
        "received_age_ms": 500,
        "transport_lag_ms": 250,
    }


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
    assert "CREATE TABLE IF NOT EXISTS binance_flow_1s" in schema
    assert "CREATE TABLE IF NOT EXISTS binance_book_1s" in schema
    assert "PRIMARY KEY (symbol, sample_second_ms)" in schema
    assert "PRIMARY KEY (symbol, source_window_start_ms, source_window_end_ms)" in schema
    assert "PRIMARY KEY (venue, symbol, sample_second_ms)" in schema
    assert "open_interest NUMERIC(38, 18)" in schema
    assert "premium_bps NUMERIC(20, 8)" in schema
    assert "sum_open_interest_value NUMERIC(38, 18)" in schema
    assert "buy_quote NUMERIC(38, 18)" in schema
    assert "cvd_30s NUMERIC(38, 18)" in schema
    assert "bid NUMERIC(38, 18)" in schema
    assert "microprice NUMERIC(38, 18)" in schema
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
                "binance_provider_event_ms": (
                    market_start_ms - 100 if t == 0 else None
                ),
                "binance_received_ms": (
                    market_start_ms - 50 if t == 0 else None
                ),
                "binance_price": Decimal("123000.004") if t == 0 else None,
                "chainlink_sample_second_ms": (
                    market_start_ms if t == 0 else None
                ),
                "chainlink_provider_event_ms": (
                    market_start_ms - 1_000 if t == 0 else None
                ),
                "chainlink_provider_message_ms": (
                    market_start_ms - 900 if t == 0 else None
                ),
                "chainlink_received_ms": (
                    market_start_ms - 800 if t == 0 else None
                ),
                "chainlink_price": Decimal("122998.125") if t == 0 else None,
                "up_bid": Decimal("0.47") if t == 1 else None,
                "up_ask": Decimal("0.485") if t == 1 else None,
                "up_mid": Decimal("0.48") if t == 1 else None,
                "down_bid": Decimal("0.50") if t == 1 else None,
                "down_ask": Decimal("0.534") if t == 1 else None,
                "down_mid": Decimal("0.515") if t == 1 else None,
                "up_prob_norm": Decimal("0.48241206") if t == 1 else None,
                "down_prob_norm": Decimal("0.51758794") if t == 1 else None,
                "futures_last_price_time_ms": None,
                "premium_index_time_ms": None,
                "open_interest_time_ms": None,
                "futures_received_ms": None,
                "prev_oi_source_window_start_ms": None,
            }
        )

    return rows


def test_build_market_download_payload_returns_300_price_rows_without_probabilities():
    payload = db.build_market_download_payload(
        market_download_rows(),
        server_time_ms=1_783_459_200_250,
        include_probabilities=False,
        include_futures=False,
        include_oi=False,
    )

    assert payload is not None
    assert payload["schema_version"] == 1
    assert payload["server_time_ms"] == 1_783_459_200_250
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
    assert payload["series"][0]["freshness"]["binance"] == {
        "source_ms": 1_783_459_199_900,
        "received_ms": 1_783_459_199_950,
        "source_age_ms": 350,
        "received_age_ms": 300,
        "transport_lag_ms": 50,
    }
    assert payload["series"][0]["freshness"]["chainlink"] == {
        "source_ms": 1_783_459_199_000,
        "message_ms": 1_783_459_199_100,
        "received_ms": 1_783_459_199_200,
        "is_carried_forward": False,
        "source_age_ms": 1_250,
        "received_age_ms": 1_050,
        "transport_lag_ms": 200,
    }
    assert payload["series"][1]["prices"] == {"binance": None, "chainlink": None}
    assert "probabilities" not in payload["series"][1]
    assert "futures" not in payload["series"][1]
    assert "open_interest" not in payload["series"][1]
    assert "previous_5m_oi_summary" not in payload


def test_build_market_download_payload_adds_probabilities_only_when_requested():
    payload = db.build_market_download_payload(
        market_download_rows(),
        server_time_ms=1_783_459_200_250,
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
        server_time_ms=1_783_459_200_250,
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
            "futures_last_price_time_ms": 1_783_459_212_050,
            "premium_index_time_ms": 1_783_459_212_000,
            "open_interest": Decimal("74321.1234"),
            "open_interest_time_ms": 1_783_459_210_000,
            "oi_notional_usdt": Decimal("4616789012.345"),
            "premium_bps": Decimal("0.755"),
            "futures_received_ms": 1_783_459_212_100,
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
        other_row["prev_oi_source_window_start_ms"] = 1_783_458_900_000
        other_row["prev_oi_source_window_end_ms"] = 1_783_459_200_000
        other_row["prev_oi_sum_open_interest"] = Decimal("74000.1234")
        other_row["prev_oi_sum_open_interest_value"] = Decimal("4590000000.125")

    payload = db.build_market_download_payload(
        rows,
        server_time_ms=1_783_459_212_250,
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
    assert item["freshness"]["futures_last"] == {
        "source_ms": 1_783_459_212_050,
        "received_ms": 1_783_459_212_100,
        "source_age_ms": 200,
        "received_age_ms": 150,
    }
    assert item["freshness"]["open_interest"] == {
        "source_ms": 1_783_459_210_000,
        "received_ms": 1_783_459_212_100,
        "source_age_ms": 2_250,
        "received_age_ms": 150,
    }
    assert payload["previous_5m_oi_summary"] == {
        "source_window_start_ms": 1_783_458_900_000,
        "source_window_end_ms": 1_783_459_200_000,
        "effective_market_id": 5_944_864,
        "sum_open_interest": "74000.123",
        "sum_open_interest_value": "4590000000.13",
    }


def test_build_market_download_payload_adds_optional_futures_flow_and_book():
    rows = market_download_rows()
    row = rows[12]
    row.update(
        {
            "flow_buy_base": Decimal("0.10000000"),
            "flow_sell_base": Decimal("0.02500000"),
            "flow_buy_quote": Decimal("6207.500000000000000000"),
            "flow_sell_quote": Decimal("1551.875000000000000000"),
            "flow_delta_quote": Decimal("4655.625000000000000000"),
            "flow_total_quote": Decimal("7759.375000000000000000"),
            "flow_taker_imbalance": Decimal("0.60000000"),
            "flow_cvd_quote": Decimal("98765.432100000000000000"),
            "flow_cvd_10s": Decimal("12345.670000000000000000"),
            "flow_cvd_30s": Decimal("23456.780000000000000000"),
            "flow_imbalance_10s": Decimal("0.12345678"),
            "flow_imbalance_30s": Decimal("-0.23456789"),
            "flow_agg_trade_count": 7,
            "flow_trade_count": 10,
            "flow_max_trade_quote": Decimal("2500.125000000000000000"),
            "flow_first_agg_trade_id": 100,
            "flow_last_agg_trade_id": 106,
            "flow_last_trade_time_ms": 1_783_459_212_075,
            "flow_last_event_time_ms": 1_783_459_212_090,
            "flow_received_ms": 1_783_459_212_120,
            "book_bid": Decimal("62074.100000000000000000"),
            "book_ask": Decimal("62074.200000000000000000"),
            "book_bid_qty": Decimal("1.500000000000000000"),
            "book_ask_qty": Decimal("0.900000000000000000"),
            "book_mid": Decimal("62074.150000000000000000"),
            "book_spread": Decimal("0.100000000000000000"),
            "book_spread_bps": Decimal("0.01610935"),
            "book_book_imbalance": Decimal("0.25000000"),
            "book_microprice": Decimal("62074.162500000000000000"),
            "book_update_id": 123456,
            "book_event_time_ms": 1_783_459_212_080,
            "book_transaction_time_ms": 1_783_459_212_070,
            "book_received_ms": 1_783_459_212_115,
        }
    )

    payload = db.build_market_download_payload(
        rows,
        server_time_ms=1_783_459_212_250,
        include_probabilities=False,
        include_futures=False,
        include_oi=False,
        include_flow=True,
        include_book=True,
    )

    assert payload is not None
    item = payload["series"][12]
    assert item["flow"] == {
        "buy_base": "0.10000000",
        "sell_base": "0.02500000",
        "buy_quote": "6207.500000000000000000",
        "sell_quote": "1551.875000000000000000",
        "delta_quote": "4655.625000000000000000",
        "total_quote": "7759.375000000000000000",
        "taker_imbalance": "0.60000000",
        "cvd_quote": "98765.432100000000000000",
        "cvd_10s": "12345.670000000000000000",
        "cvd_30s": "23456.780000000000000000",
        "imbalance_10s": "0.12345678",
        "imbalance_30s": "-0.23456789",
        "agg_trade_count": 7,
        "trade_count": 10,
        "max_trade_quote": "2500.125000000000000000",
        "first_agg_trade_id": 100,
        "last_agg_trade_id": 106,
    }
    assert item["book"] == {
        "bid": "62074.100000000000000000",
        "ask": "62074.200000000000000000",
        "bid_qty": "1.500000000000000000",
        "ask_qty": "0.900000000000000000",
        "mid": "62074.150000000000000000",
        "spread": "0.100000000000000000",
        "spread_bps": "0.01610935",
        "book_imbalance": "0.25000000",
        "microprice": "62074.162500000000000000",
        "update_id": 123456,
    }
    assert item["freshness"]["futures_flow"] == {
        "source_ms": 1_783_459_212_075,
        "event_ms": 1_783_459_212_090,
        "received_ms": 1_783_459_212_120,
        "source_age_ms": 175,
        "received_age_ms": 130,
        "transport_lag_ms": 45,
    }
    assert item["freshness"]["futures_book"] == {
        "source_ms": 1_783_459_212_080,
        "event_ms": 1_783_459_212_080,
        "transaction_ms": 1_783_459_212_070,
        "received_ms": 1_783_459_212_115,
        "source_age_ms": 170,
        "received_age_ms": 135,
        "transport_lag_ms": 35,
    }


def test_build_market_download_payload_marks_chainlink_display_carry_forward():
    rows = market_download_rows()
    rows[1]["chainlink_sample_second_ms"] = rows[0]["sample_second_ms"]
    rows[1]["chainlink_price"] = rows[0]["chainlink_price"]
    rows[1]["chainlink_provider_event_ms"] = rows[0]["chainlink_provider_event_ms"]
    rows[1]["chainlink_provider_message_ms"] = rows[0]["chainlink_provider_message_ms"]
    rows[1]["chainlink_received_ms"] = rows[0]["chainlink_received_ms"]

    payload = db.build_market_download_payload(
        rows,
        server_time_ms=1_783_459_201_250,
        include_probabilities=False,
        include_futures=False,
        include_oi=False,
    )

    assert payload is not None
    assert payload["series"][1]["prices"]["chainlink"] == "122998.13"
    assert payload["series"][1]["freshness"]["chainlink"]["is_carried_forward"] is True
    assert payload["series"][1]["freshness"]["chainlink"]["source_age_ms"] == 2_250


def test_fetch_current_live_payload_returns_latest_values_with_freshness():
    class FakeAcquire:
        def __init__(self, connection):
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeConnection:
        def __init__(self):
            self.calls = []

        async def fetchrow(self, query, *args):
            self.calls.append((query, args))
            return {
                "binance_sample_second_ms": 1_783_459_250_000,
                "binance_price": Decimal("62095.491"),
                "binance_provider_event_ms": 1_783_459_249_900,
                "binance_received_ms": 1_783_459_249_950,
                "chainlink_sample_second_ms": 1_783_459_247_000,
                "chainlink_price": Decimal("62037.054"),
                "chainlink_provider_event_ms": 1_783_459_247_000,
                "chainlink_provider_message_ms": 1_783_459_247_050,
                "chainlink_received_ms": 1_783_459_247_100,
                "futures_last_price": Decimal("62099.105"),
                "futures_last_price_time_ms": 1_783_459_250_000,
                "mark_price": Decimal("62098.804"),
                "index_price": Decimal("62098.195"),
                "premium_index_time_ms": 1_783_459_249_000,
                "futures_received_ms": 1_783_459_250_050,
                "open_interest": Decimal("74321.1234"),
                "open_interest_time_ms": 1_783_459_240_000,
                "oi_received_ms": 1_783_459_250_060,
            }

    class FakePool:
        def __init__(self):
            self.connection = FakeConnection()

        def acquire(self):
            return FakeAcquire(self.connection)

    pool = FakePool()
    window = MarketWindow(
        market_id=5_944_864,
        market_start_ms=1_783_459_200_000,
        market_end_ms=1_783_459_500_000,
    )

    payload = asyncio.run(
        db.fetch_current_live_payload(
            pool,
            window=window,
            current_sample_second_ms=1_783_459_250_000,
            server_time_ms=1_783_459_250_123,
            max_chainlink_carry_forward_ms=10_000,
        )
    )

    assert pool.connection.calls[0][1] == (
        5_944_864,
        1_783_459_250_000,
        10_000,
    )
    assert payload["server_time_ms"] == 1_783_459_250_123
    assert payload["prices"]["binance_spot"]["value"] == "62095.49"
    assert payload["prices"]["binance_spot"]["source_age_ms"] == 223
    assert payload["prices"]["binance_spot"]["transport_lag_ms"] == 50
    assert payload["prices"]["chainlink"]["value"] == "62037.05"
    assert payload["prices"]["chainlink"]["provider_message_ms"] == 1_783_459_247_050
    assert payload["prices"]["chainlink"]["is_carried_forward_for_display"] is True
    assert payload["prices"]["chainlink"]["source_age_ms"] == 3_123
    assert payload["futures"]["last"]["value"] == "62099.11"
    assert payload["futures"]["last"]["source_age_ms"] == 123
    assert payload["futures"]["mark"]["time_ms"] == 1_783_459_249_000
    assert payload["open_interest"]["contracts"] == "74321.123"
    assert payload["open_interest"]["source_age_ms"] == 10_123


def test_fetch_market_download_payload_query_includes_optional_futures_joins():
    source = inspect.getsource(db.fetch_market_download_payload)

    assert "server_time_ms: int" in source
    assert "fill_display: bool = False" in source
    assert "provider_event_ms AS binance_provider_event_ms" in source
    assert "provider_message_ms AS chainlink_provider_message_ms" in source
    assert "futures_last_price_time_ms" in source
    assert "open_interest_time_ms" in source
    assert "LEFT JOIN LATERAL" in source
    assert "include_futures: bool" in source
    assert "include_oi: bool" in source
    assert "include_flow: bool" in source
    assert "include_book: bool" in source
    assert "FROM binance_futures_snapshots" in source
    assert "FROM binance_futures_oi_5m_summaries" in source
    assert "FROM binance_flow_1s" in source
    assert "FROM binance_book_1s" in source
    assert "flow.delta_quote AS flow_delta_quote" in source
    assert "book.spread_bps AS book_spread_bps" in source
    assert "LEFT JOIN flow ON flow.sample_second_ms = s.sample_second_ms" in source
    assert "LEFT JOIN book ON book.sample_second_ms = s.sample_second_ms" in source
    assert "f.sample_second_ms - 30000" in source
    assert "f.sample_second_ms - 60000" in source
    assert "f.sample_second_ms - 300000" in source
