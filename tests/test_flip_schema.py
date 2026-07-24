import re
from pathlib import Path

from price_collector.binance_microstructure import MICROSTRUCTURE_VALUE_COLUMNS


ROOT = Path(__file__).resolve().parents[1]


def _table_statement(schema: str, table_name: str) -> str:
    marker = f"CREATE TABLE IF NOT EXISTS {table_name}"
    start = schema.index(marker)
    return schema[start : schema.index(";", start) + 1]


def _column_types(table: str) -> dict[str, str]:
    result = {}
    for line in table.splitlines():
        match = re.match(
            r"\s*([a-z][a-z0-9_]*)\s+"
            r"(SMALLINT|INTEGER|BIGINT|BOOLEAN|TEXT(?:\[\])?|"
            r"TIMESTAMPTZ|NUMERIC\(\d+,\s*\d+\))",
            line,
        )
        if match:
            result[match.group(1)] = re.sub(r"\s+", "", match.group(2))
    return result


def test_flip_evaluation_schema_is_versioned_permanent_and_retention_gated():
    schema = (ROOT / "schema.sql").read_text()
    table = _table_statement(schema, "polymarket_btc_5m_flip_evaluations")
    normalized = " ".join(table.split())

    assert "PRIMARY KEY (market_id, definition_version)" in table
    assert "price_to_beat NUMERIC(38, 18) NOT NULL" in table
    assert "official_close_price NUMERIC(38, 18) NOT NULL" in table
    assert "official_winner TEXT," in table
    assert "official_winner IS NULL OR official_winner IN ('Up', 'Down')" in table
    assert "('confirmed_flip', 'non_flip', 'ambiguous')" in table
    assert "decisive_flip_ms_before_end BIGINT" in table
    assert "chainlink_observation_count INTEGER NOT NULL" in table
    assert "chainlink_cutoff_count INTEGER NOT NULL DEFAULT 0" in table
    assert "fresh_chainlink_cutoff_count INTEGER NOT NULL DEFAULT 0" in table
    assert "fresh_probability_cutoff_count INTEGER NOT NULL DEFAULT 0" in table
    assert "archive_status TEXT NOT NULL" in table
    assert "('not_required', 'pending', 'complete', 'failed')" in table
    assert "source_microstructure_row_count INTEGER NOT NULL" in table
    assert "archived_microstructure_row_count INTEGER NOT NULL" in table
    assert "retention_safe BOOLEAN NOT NULL DEFAULT FALSE" in table
    assert "evaluation_attempts INTEGER NOT NULL DEFAULT 1" in table
    assert "next_retry_ms BIGINT" in table
    assert "last_error TEXT" in table
    assert (
        "NOT retention_safe OR archive_status IN ('not_required', 'complete')"
        in normalized
    )
    assert (
        "archive_status NOT IN ('pending', 'failed') "
        "OR retention_safe = FALSE"
        in normalized
    )
    assert (
        "archive_status <> 'complete' OR source_microstructure_row_count "
        "= archived_microstructure_row_count"
        in normalized
    )
    assert (
        "archive_status <> 'not_required' OR "
        "( evaluation_status = 'non_flip' "
        "AND archived_microstructure_row_count = 0 )"
        in normalized
    )
    assert (
        "evaluation_status NOT IN ('confirmed_flip', 'ambiguous') "
        "OR archive_status <> 'not_required'"
        in normalized
    )
    assert "polymarket_btc_5m_flip_evaluations_retry_idx" in schema


def test_flip_events_preserve_decimal_crossings_and_source_timing():
    schema = (ROOT / "schema.sql").read_text()
    table = _table_statement(schema, "polymarket_btc_5m_flip_events")

    assert (
        "PRIMARY KEY (market_id, definition_version, event_sequence)" in table
    )
    assert "previous_price NUMERIC(38, 18) NOT NULL" in table
    assert "new_price NUMERIC(38, 18) NOT NULL" in table
    assert "previous_provider_event_ms BIGINT NOT NULL" in table
    assert "provider_event_ms BIGINT NOT NULL" in table
    assert "previous_received_ms BIGINT NOT NULL" in table
    assert "received_ms BIGINT NOT NULL" in table
    assert "observation_gap_ms BIGINT NOT NULL" in table
    assert "observed_ms_before_end BIGINT NOT NULL" in table
    assert "is_decisive BOOLEAN NOT NULL DEFAULT FALSE" in table
    assert "observation_precision TEXT NOT NULL" in table
    assert "polymarket_btc_5m_flip_events_decisive_time_idx" in schema


def test_flip_cutoffs_have_twenty_causal_rows_and_typed_microstructure_snapshot():
    schema = (ROOT / "schema.sql").read_text()
    table = _table_statement(schema, "polymarket_btc_5m_flip_cutoffs")
    live = _table_statement(schema, "binance_microstructure_1s")
    cutoff_types = _column_types(table)
    live_types = _column_types(live)

    assert (
        "PRIMARY KEY (market_id, definition_version, seconds_before_end)"
        in table
    )
    assert "seconds_before_end BETWEEN 1 AND 20" in table
    assert "chainlink_price NUMERIC(38, 18)" in table
    assert "chainlink_received_age_ms BIGINT" in table
    assert "chainlink_fresh BOOLEAN NOT NULL DEFAULT FALSE" in table
    assert "price_distance NUMERIC(38, 18)" in table
    assert "absolute_price_distance NUMERIC(38, 18)" in table
    assert "up_prob_norm NUMERIC(18, 8)" in table
    assert "down_prob_norm NUMERIC(18, 8)" in table
    assert "probability_received_age_ms BIGINT" in table
    assert "up_probability_provider_event_ms BIGINT" in table
    assert "up_probability_received_ms BIGINT" in table
    assert "down_probability_provider_event_ms BIGINT" in table
    assert "down_probability_received_ms BIGINT" in table
    assert "probability_fresh BOOLEAN NOT NULL DEFAULT FALSE" in table
    assert "flipped_after_cutoff BOOLEAN" in table
    assert "official_winner TEXT," in table
    assert "official_winner IS NULL OR official_winner IN ('Up', 'Down')" in table
    assert "microstructure_available BOOLEAN NOT NULL DEFAULT FALSE" in table

    assert set(MICROSTRUCTURE_VALUE_COLUMNS) <= cutoff_types.keys()
    for column in MICROSTRUCTURE_VALUE_COLUMNS:
        assert cutoff_types[column] == live_types[column]

    assert " DOUBLE" not in table
    assert " REAL" not in table


def test_probability_source_preserves_per_outcome_freshness_timestamps():
    schema = (ROOT / "schema.sql").read_text()
    table = _table_statement(schema, "polymarket_probability_samples")

    assert "up_provider_event_ms BIGINT" in table
    assert "up_received_ms BIGINT" in table
    assert "down_provider_event_ms BIGINT" in table
    assert "down_received_ms BIGINT" in table
    assert (
        "ADD COLUMN IF NOT EXISTS up_provider_event_ms BIGINT" in schema
    )


def test_flip_archive_is_an_exact_typed_no_ttl_clone_with_role_separation():
    schema = (ROOT / "schema.sql").read_text()
    archive = _table_statement(
        schema,
        "binance_microstructure_1s_flip_archive",
    )

    assert "LIKE binance_microstructure_1s INCLUDING ALL" in archive
    assert "EXCLUDING INDEXES" in archive
    assert "PRIMARY KEY (symbol, sample_second_ms)" in archive
    assert "binance_microstructure_1s_flip_archive_market_idx" in schema
    assert "DELETE FROM binance_microstructure_1s_flip_archive" not in schema

    tables = (
        "polymarket_btc_5m_flip_evaluations",
        "polymarket_btc_5m_flip_events",
        "polymarket_btc_5m_flip_cutoffs",
        "binance_microstructure_1s_flip_archive",
    )
    for table_name in tables:
        assert f"REVOKE ALL ON {table_name} FROM PUBLIC" in schema
        assert f"GRANT SELECT ON {table_name} TO price_reader" in schema

    assert (
        "GRANT SELECT, INSERT ON polymarket_btc_5m_flip_events TO price_writer"
        in schema
    )
    assert (
        "REVOKE UPDATE, DELETE ON polymarket_btc_5m_flip_events "
        "FROM price_writer"
        in schema
    )
    for mutable_table in (
        "polymarket_btc_5m_flip_evaluations",
        "polymarket_btc_5m_flip_cutoffs",
        "binance_microstructure_1s_flip_archive",
    ):
        assert (
            "GRANT SELECT, INSERT, UPDATE, DELETE\n"
            f"    ON {mutable_table} TO price_writer"
            in schema
        )
