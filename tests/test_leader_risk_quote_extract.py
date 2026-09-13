"""Check the actual quote-selection SQL on synthetic sampled observations.

The LATERAL body's ordinary integer predicates run unchanged in SQLite after
binding its two outer values. PostgreSQL/psql execution is checked separately.
Prices stay text in these fixtures; the tests exercise clocks and identity.
"""

import json
from pathlib import Path
import re
import sqlite3

import pytest

from price_collector.market import market_for_sample_second


SQL = (
    Path(__file__).resolve().parents[1] / "research/leader_risk/extract_quotes.sql"
).read_text(encoding="utf-8")
SELECTION = re.search(
    r"LEFT JOIN LATERAL \((.*?)\n\) p ON true", SQL, re.DOTALL
).group(1).replace("c.market_id", ":market_id").replace("c.cut_ms", ":cut_ms")
TOKEN_MATCH = re.search(r"(p\.up_token_id = .*?) AS tokens_match", SQL).group(1)
TOKEN_MATCH = TOKEN_MATCH.replace("c.up_token_id", ":up_token_id").replace(
    "c.down_token_id", ":down_token_id"
)
START_MS = 1_789_007_400_000
WINDOW = market_for_sample_second(START_MS)
CUT_MS = WINDOW.market_end_ms - 120_000


@pytest.fixture
def db():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("""
        CREATE TABLE polymarket_probability_samples (
            market_id INTEGER, source TEXT, sample_second_ms INTEGER,
            received_ms INTEGER, up_token_id TEXT, down_token_id TEXT,
            up_ask TEXT, down_ask TEXT, raw TEXT,
            PRIMARY KEY (market_id, source, sample_second_ms)
        )
    """)
    yield connection
    connection.close()


def add_sample(db, sample_offset_ms=0, receipt_offset_ms=0, **overrides):
    fields = {
        "market_id": WINDOW.market_id,
        "source": "polymarket_clob",
        "sample_second_ms": CUT_MS + sample_offset_ms,
        "received_ms": CUT_MS + receipt_offset_ms,
        "up_token_id": "up-token",
        "down_token_id": "down-token",
        "up_ask": "0.98",
        "down_ask": "0.03",
        "raw": json.dumps({
            "up_ask_provider_event_ms": CUT_MS - 1000,
            "up_ask_received_ms": CUT_MS - 1000,
            "down_ask_provider_event_ms": CUT_MS - 1000,
            "down_ask_received_ms": CUT_MS - 1000,
        }),
    }
    fields.update(overrides)
    db.execute(
        "INSERT INTO polymarket_probability_samples VALUES ("
        + ",".join("?" for _ in fields) + ")", tuple(fields.values()),
    )


def select_quote(db):
    return db.execute(SELECTION, {"market_id": WINDOW.market_id, "cut_ms": CUT_MS}).fetchone()


def test_sample_and_receipt_exactly_at_cutoff_are_included(db):
    add_sample(db)
    row = select_quote(db)
    assert row["sample_second_ms"] == row["received_ms"] == CUT_MS


def test_same_second_row_with_later_receipt_is_not_available(db):
    add_sample(db, -1000, -1000)
    add_sample(db, 0, 1)
    assert select_quote(db)["sample_second_ms"] == CUT_MS - 1000


def test_later_sample_is_not_used_even_when_its_last_event_precedes_cutoff(db):
    add_sample(db, -1000, -1000)
    add_sample(db, 1000, -1)
    assert select_quote(db)["sample_second_ms"] == CUT_MS - 1000


@pytest.mark.parametrize("sample_offset,available", ((-10000, True), (-11000, False)))
def test_ten_second_sample_search_boundary(db, sample_offset, available):
    add_sample(db, sample_offset, sample_offset)
    assert (select_quote(db) is not None) is available


@pytest.mark.parametrize("outcome", ("up", "down"))
def test_future_provider_clock_is_retained_for_downstream_rejection(db, outcome):
    add_sample(db, -1000, -1000)
    add_sample(db, raw=json.dumps({f"{outcome}_ask_provider_event_ms": CUT_MS + 1000}))
    row = select_quote(db)
    assert row["sample_second_ms"] == CUT_MS
    assert json.loads(row["raw"])[f"{outcome}_ask_provider_event_ms"] > CUT_MS


@pytest.mark.parametrize("outcome", ("up", "down"))
def test_latest_stale_ask_does_not_fall_back_to_older_fresh_ask(db, outcome):
    add_sample(db, -1000, -1000)
    # Another component can refresh the row receipt while this ask remains old.
    add_sample(db, 0, -100, raw=json.dumps({
        f"{outcome}_ask_provider_event_ms": CUT_MS - 6000,
        f"{outcome}_ask_received_ms": CUT_MS - 6000,
    }))
    row = select_quote(db)
    assert row["sample_second_ms"] == CUT_MS
    assert CUT_MS - json.loads(row["raw"])[f"{outcome}_ask_received_ms"] == 6000


def test_latest_token_mismatch_is_flagged_after_selection_without_fallback(db):
    add_sample(db, -1000, -1000)
    add_sample(db, up_token_id="unexpected-token")
    row = select_quote(db)
    assert row["sample_second_ms"] == CUT_MS
    matches = db.execute(
        f"SELECT {TOKEN_MATCH} FROM polymarket_probability_samples p "
        "WHERE p.market_id = :market_id AND p.sample_second_ms = :sample_ms",
        {"up_token_id": "up-token", "down_token_id": "down-token",
         "market_id": WINDOW.market_id, "sample_ms": row["sample_second_ms"]},
    ).fetchone()[0]
    assert matches == 0


def test_other_markets_and_sources_cannot_supply_the_quote(db):
    add_sample(db, -1000, -1000)
    add_sample(db, market_id=WINDOW.market_id + 1)
    add_sample(db, source="another-source")
    assert select_quote(db)["sample_second_ms"] == CUT_MS - 1000


def test_missing_latest_ask_is_preserved_without_fallback(db):
    add_sample(db, -1000, -1000)
    add_sample(db, up_ask=None)
    row = select_quote(db)
    assert row["sample_second_ms"] == CUT_MS and row["up_ask"] is None


def test_no_retained_quote_leaves_the_outer_join_missing(db):
    assert select_quote(db) is None
    assert "LEFT JOIN LATERAL" in SQL


def test_export_preserves_quote_columns_and_counts_markets_before_checkpoints():
    projection = "c.market_id" + SQL.split("SELECT c.market_id", 1)[1].split("\nFROM cuts c", 1)[0]
    columns = [field.strip().split(" AS ")[-1].rsplit(".", 1)[-1] for field in projection.split(",")]
    assert columns == [
        "market_id", "t_sec", "cut_ms", "quote_extraction_ms", "quote_sample_ms",
        "quote_received_ms", "tokens_match", "up_bid", "up_ask", "down_bid", "down_ask",
        "up_provider_event_ms", "up_received_ms", "down_provider_event_ms", "down_received_ms",
        "event_type", "up_bid_source_ms", "up_bid_received_ms", "up_ask_source_ms",
        "up_ask_received_ms", "down_bid_source_ms", "down_bid_received_ms", "down_ask_source_ms",
        "down_ask_received_ms", "requested_cohort_start_ms", "cohort_start_ms", "cohort_end_ms",
        "cohort_market_count",
    ]
    assert SQL.index("count(*) OVER () AS cohort_market_count") < SQL.index("), cuts AS (")
    assert "GREATEST" not in SQL.upper()
    assert SQL.index("\\set ON_ERROR_STOP on") < SQL.index("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
    assert "SET LOCAL statement_timeout = :'statement_timeout';" in SQL
    assert "\\set statement_timeout '60s'" in SQL
