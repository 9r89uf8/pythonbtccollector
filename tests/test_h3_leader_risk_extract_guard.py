"""Exercise the extraction's actual guard SELECTs without a database service.

The guard SELECTs use integer arithmetic and ordinary joins shared by SQLite and
PostgreSQL. These tests cover their logic; real PostgreSQL runs verify psql/DO
execution and the repeatable-read transaction separately.
"""

from pathlib import Path
import re
import sqlite3

import pytest

from price_collector.market import market_for_sample_second


SQL = (
    Path(__file__).resolve().parents[1] / "research/leader_risk/extract.sql"
).read_text(encoding="utf-8")
GUARD = SQL.split("DO $h3_source_market_guard$", 1)[1].split(
    "$h3_source_market_guard$;", 1
)[0]
QUERIES = dict(zip(
    ("twap", "spot"),
    re.findall(r"IF EXISTS \(\s*(SELECT 1\b.*?)\s*\) THEN", GUARD, re.DOTALL),
))
MARKET_START = 1_789_007_400_000


@pytest.fixture
def db():
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE polymarket_twap_events (
            symbol TEXT, window_s INTEGER, topic TEXT,
            received_wall_ns INTEGER, sample_second_ms INTEGER, market_id INTEGER
        );
        CREATE TABLE price_samples (
            instrument_id INTEGER, received_ms INTEGER,
            sample_second_ms INTEGER, market_id INTEGER
        );
        CREATE TABLE instruments (
            instrument_id INTEGER, provider_id INTEGER, symbol TEXT, stream_name TEXT
        );
        CREATE TABLE providers (provider_id INTEGER, provider_code TEXT);
        INSERT INTO providers VALUES (1, 'polymarket_chainlink_rtds');
        INSERT INTO instruments VALUES (
            2, 1, 'BTCUSD', 'crypto_prices_chainlink:btc/usd'
        );
    """)
    yield connection
    connection.close()


def add_observation(db, source, sample_ms, received_ms, *, extra_ns=0):
    market_id = market_for_sample_second(sample_ms).market_id
    if source == "twap":
        db.execute(
            "INSERT INTO polymarket_twap_events VALUES (?, ?, ?, ?, ?, ?)",
            ("btc/usd", 60, "crypto_prices_twap_sixty",
             received_ms * 1_000_000 + extra_ns, sample_ms, market_id),
        )
    else:
        db.execute(
            "INSERT INTO price_samples VALUES (?, ?, ?, ?)",
            (2, received_ms, sample_ms, market_id),
        )


@pytest.mark.parametrize("source", ("twap", "spot"))
@pytest.mark.parametrize("lag_ms,rejected", (
    (-3000, True), (-2999, False), (177000, False), (177001, True),
))
def test_guard_inclusive_lag_edges(db, source, lag_ms, rejected):
    add_observation(db, source, MARKET_START, MARKET_START + lag_ms)
    assert (db.execute(QUERIES[source]).fetchone() is not None) is rejected


@pytest.mark.parametrize("source", ("twap", "spot"))
def test_guard_rejects_newer_cross_market_receipt_that_would_hide_staleness(db, source):
    window = market_for_sample_second(MARKET_START)
    cut = window.market_end_ms - 120_000
    older_fresh_source = cut - 2000
    newer_stale_source = MARKET_START - 1000
    add_observation(db, source, older_fresh_source, cut - 1000)
    add_observation(db, source, newer_stale_source, cut - 500)
    table, receipt = (
        ("polymarket_twap_events", "received_wall_ns / 1000000")
        if source == "twap" else ("price_samples", "received_ms")
    )
    selection = (
        f"SELECT sample_second_ms FROM {table} "
        f"WHERE {receipt} BETWEEN ? AND ? {{predicate}} "
        f"ORDER BY {receipt} DESC LIMIT 1"
    )
    original = db.execute(selection.format(predicate=""), (cut - 3000, cut)).fetchone()[0]
    unsafe = db.execute(
        selection.format(predicate="AND market_id = ?"),
        (cut - 3000, cut, window.market_id),
    ).fetchone()[0]
    assert original == newer_stale_source and cut - original > 3000
    assert unsafe == older_fresh_source and 0 <= cut - unsafe <= 3000
    assert db.execute(QUERIES[source]).fetchone() is not None


def test_twap_guard_floors_nanosecond_receipts_as_integer_milliseconds(db):
    add_observation(db, "twap", MARKET_START, MARKET_START + 177000, extra_ns=999999)
    assert db.execute(QUERIES["twap"]).fetchone() is None


@pytest.mark.parametrize("source", ("twap", "spot"))
@pytest.mark.parametrize("t_sec", (120, 90, 60, 30, 15, 10, 5, 3))
def test_guarded_market_join_preserves_receipt_window_and_latest_selection(db, source, t_sec):
    window = market_for_sample_second(MARKET_START)
    cut = window.market_end_ms - t_sec * 1000
    # An older fresh observation and a newer stale-source observation are both
    # legitimate guard-admitted candidates. Freshness must not change selection.
    add_observation(db, source, cut - 2000, cut - 1000)
    add_observation(db, source, cut - 6000, cut - 500)
    # Source-market equality alone would admit these out-of-window receipts.
    add_observation(db, source, cut - 10000, cut - 3001)
    add_observation(db, source, cut, cut + 500)
    assert db.execute(QUERIES[source]).fetchone() is None
    table, receipt, receipt_bucket = (
        ("polymarket_twap_events", "received_wall_ns / 1000000",
         "received_wall_ns / 300000000000")
        if source == "twap" else ("price_samples", "received_ms", "received_ms / 300000")
    )
    selection = (
        f"SELECT sample_second_ms FROM {table} "
        f"WHERE {receipt} BETWEEN ? AND ? AND {{market_key}} = ? "
        f"ORDER BY {receipt} DESC LIMIT 1"
    )
    arguments = (cut - 3000, cut, window.market_id)
    original = db.execute(
        selection.format(market_key=receipt_bucket), arguments
    ).fetchone()[0]
    optimized = db.execute(
        selection.format(market_key="market_id"), arguments
    ).fetchone()[0]
    assert original == optimized == cut - 6000


def test_guard_bounds_keep_every_candidate_in_its_market_at_all_checkpoints():
    schedule = re.search(r"CROSS JOIN \(VALUES (.*?)\) t\(t_sec\)", SQL).group(1)
    checkpoints = [int(value) for value in re.findall(r"\((\d+)\)", schedule)]
    assert checkpoints
    window = market_for_sample_second(MARKET_START)
    for query in QUERIES.values():
        lower, upper = map(int, re.search(r"NOT BETWEEN (-?\d+) AND (-?\d+)", query).groups())
        for t_sec in checkpoints:
            cut = window.market_end_ms - t_sec * 1000
            for receipt in range(cut - 3000, cut + 1):
                assert receipt - upper >= window.market_start_ms
                assert receipt - lower < window.market_end_ms


def test_guard_is_fatal_before_export_in_the_same_read_only_snapshot():
    begin = SQL.index("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
    guard = SQL.index("DO $h3_source_market_guard$")
    export = SQL.index("COPY (")
    assert SQL.index("\\set ON_ERROR_STOP on") < begin < guard < export
    assert "COMMIT;" not in SQL[begin:export]
    assert GUARD.count("RAISE EXCEPTION") == 2
    assert SQL.index("e.market_id = c.market_id") > export
    assert SQL.index("p.market_id = c.market_id") > export
