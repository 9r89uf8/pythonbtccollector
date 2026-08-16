import asyncio
import inspect
from decimal import Decimal

import pytest

from price_collector import flip_research


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _RecordingConnection:
    def __init__(self):
        self.calls = []

    async def fetch(self, query, *args):
        self.calls.append((" ".join(query.split()), args))
        return []


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _AsyncContext(self.connection)


def _twap_market(*, market_start_ms, window_s, source_url, rule_version):
    return {
        "market_start_ms": market_start_ms,
        "settlement_reference": "chainlink_twap",
        "settlement_window_s": window_s,
        "settlement_source_url": source_url,
        "settlement_rule_version": rule_version,
    }


def test_flip_definition_versions_pin_legacy_and_current_twap_identities():
    legacy = _twap_market(
        market_start_ms=flip_research.TWAP_60S_CUTOVER_MS - 300_000,
        window_s=30,
        source_url=flip_research.TWAP_30S_SOURCE_URL,
        rule_version=flip_research.TWAP_30S_RULE_VERSION,
    )
    current = _twap_market(
        market_start_ms=flip_research.TWAP_60S_CUTOVER_MS,
        window_s=60,
        source_url=flip_research.TWAP_60S_SOURCE_URL,
        rule_version=flip_research.TWAP_60S_RULE_VERSION,
    )

    assert flip_research.TWAP_30S_FLIP_DEFINITION_VERSION == 2
    assert flip_research.TWAP_60S_FLIP_DEFINITION_VERSION == 3
    assert flip_research.TWAP_FLIP_DEFINITION_VERSION == 3
    assert flip_research.FLIP_DEFINITION_VERSION == 3
    assert flip_research.market_rule_supports_flip_definition(legacy, 2)
    assert not flip_research.market_rule_supports_flip_definition(legacy, 3)
    assert flip_research.market_rule_supports_flip_definition(current, 3)
    assert not flip_research.market_rule_supports_flip_definition(current, 2)
    assert not flip_research.market_rule_supports_flip_definition(
        {
            **legacy,
            "market_start_ms": flip_research.TWAP_60S_CUTOVER_MS,
        },
        2,
    )
    assert not flip_research.market_rule_supports_flip_definition(
        {
            **current,
            "market_start_ms": flip_research.TWAP_60S_CUTOVER_MS - 300_000,
        },
        3,
    )


def test_flip_read_queries_require_definition_specific_market_identity():
    predicate = " ".join(
        flip_research._market_rule_sql(
            "$1::SMALLINT",
            market_alias="pm",
            window_alias="mw",
        ).split()
    )

    assert (
        f"$1::SMALLINT = {flip_research.TWAP_30S_FLIP_DEFINITION_VERSION}"
        in predicate
    )
    assert (
        f"mw.market_start_ms < {flip_research.TWAP_60S_CUTOVER_MS}"
        in predicate
    )
    assert (
        f"pm.settlement_window_s = {flip_research.TWAP_30S_WINDOW_SECONDS}"
        in predicate
    )
    assert flip_research.TWAP_30S_SOURCE_URL in predicate
    assert flip_research.TWAP_30S_RULE_VERSION in predicate
    assert (
        f"$1::SMALLINT = {flip_research.TWAP_60S_FLIP_DEFINITION_VERSION}"
        in predicate
    )
    assert (
        f"mw.market_start_ms >= {flip_research.TWAP_60S_CUTOVER_MS}"
        in predicate
    )
    assert (
        f"pm.settlement_window_s = {flip_research.TWAP_60S_WINDOW_SECONDS}"
        in predicate
    )
    assert flip_research.TWAP_60S_SOURCE_URL in predicate
    assert flip_research.TWAP_60S_RULE_VERSION in predicate

    assert inspect.getsource(flip_research.fetch_flip_markets).count(
        "_market_rule_sql("
    ) == 1
    assert inspect.getsource(flip_research.fetch_market_flip_analysis).count(
        "_market_rule_sql("
    ) == 1
    assert inspect.getsource(flip_research.fetch_flip_distribution).count(
        "_market_rule_sql("
    ) == 3


@pytest.mark.parametrize(
    ("definition_version", "topic", "window_s", "source_url", "rule_version"),
    (
        (
            flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
            flip_research.TWAP_30S_TOPIC,
            flip_research.TWAP_30S_WINDOW_SECONDS,
            flip_research.TWAP_30S_SOURCE_URL,
            flip_research.TWAP_30S_RULE_VERSION,
        ),
        (
            flip_research.TWAP_60S_FLIP_DEFINITION_VERSION,
            flip_research.TWAP_60S_TOPIC,
            flip_research.TWAP_60S_WINDOW_SECONDS,
            flip_research.TWAP_60S_SOURCE_URL,
            flip_research.TWAP_60S_RULE_VERSION,
        ),
    ),
)
def test_due_scan_binds_definition_specific_twap_identity(
    definition_version,
    topic,
    window_s,
    source_url,
    rule_version,
):
    connection = _RecordingConnection()

    rows = asyncio.run(
        flip_research.fetch_due_flip_markets(
            _Pool(connection),
            now_ms=1_000_000,
            definition_version=definition_version,
            finalization_grace_ms=30_000,
            limit=7,
        )
    )

    assert rows == []
    query, args = connection.calls[0]
    assert "event.topic = $5::TEXT" in query
    assert "event.window_s = $6::SMALLINT" in query
    assert "pm.settlement_window_s = $6::SMALLINT" in query
    assert (
        "r.reconciled_settlement_rule_version = "
        "pm.settlement_rule_version"
    ) in query
    assert args == (
        1_000_000,
        definition_version,
        30_000,
        7,
        topic,
        window_s,
        source_url,
        rule_version,
    )


def test_due_scan_allows_only_terminal_legacy_market_without_post_end_watermark():
    connection = _RecordingConnection()

    asyncio.run(
        flip_research.fetch_due_flip_markets(
            _Pool(connection),
            now_ms=flip_research.TWAP_60S_CUTOVER_MS + 30_000,
            definition_version=flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
            finalization_grace_ms=30_000,
        )
    )

    query, _args = connection.calls[0]
    assert (
        f"mw.market_end_ms = {flip_research.TWAP_60S_CUTOVER_MS}"
        in query
    )
    assert (
        f"$1::BIGINT >= {flip_research.TWAP_60S_CUTOVER_MS}"
        in query
    )
    assert "twap_watermark.provider_event_ms > mw.market_end_ms OR" in query
    assert (
        f"mw.market_end_ms <= {flip_research.TWAP_60S_CUTOVER_MS}"
        not in query
    )


@pytest.mark.parametrize(
    ("definition_version", "topic", "window_s"),
    (
        (
            flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
            flip_research.TWAP_30S_TOPIC,
            flip_research.TWAP_30S_WINDOW_SECONDS,
        ),
        (
            flip_research.TWAP_60S_FLIP_DEFINITION_VERSION,
            flip_research.TWAP_60S_TOPIC,
            flip_research.TWAP_60S_WINDOW_SECONDS,
        ),
    ),
)
def test_flip_input_event_session_and_gap_queries_use_matching_identity(
    definition_version,
    topic,
    window_s,
):
    connection = _RecordingConnection()

    result = asyncio.run(
        flip_research._load_market_inputs(
            connection,
            market_id=42,
            definition_version=definition_version,
        )
    )

    assert result == ([], [], [], [])
    event_query, event_args = connection.calls[0]
    gap_query, gap_args = connection.calls[1]
    assert "event.topic = $2::TEXT" in event_query
    assert "event.window_s = $3::SMALLINT" in event_query
    assert "session.topic = $2::TEXT" in gap_query
    assert "session.window_s = $3::SMALLINT" in gap_query
    assert event_args == (42, topic, window_s)
    assert gap_args == (42, topic, window_s)


def test_terminal_legacy_market_without_replay_gets_conservative_cutover_gap():
    terminal_market_id = flip_research.TWAP_60S_CUTOVER_MS // 300_000 - 1
    connection = _RecordingConnection()

    _, _, _, source_gaps = asyncio.run(
        flip_research._load_market_inputs(
            connection,
            market_id=terminal_market_id,
            definition_version=flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
        )
    )

    assert source_gaps == [
        {
            "gap_start_ms": flip_research.TWAP_60S_CUTOVER_MS,
            "gap_end_ms": None,
            "time_basis": "settlement_rule_cutover",
        }
    ]


def test_earlier_legacy_market_without_watermark_gets_no_synthetic_cutover_gap():
    earlier_market_id = flip_research.TWAP_60S_CUTOVER_MS // 300_000 - 2
    connection = _RecordingConnection()

    _, _, _, source_gaps = asyncio.run(
        flip_research._load_market_inputs(
            connection,
            market_id=earlier_market_id,
            definition_version=flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
        )
    )

    assert source_gaps == []


@pytest.mark.parametrize(
    ("definition_version", "market_start_ms", "source_reference"),
    (
        (
            flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
            flip_research.TWAP_60S_CUTOVER_MS - 300_000,
            "chainlink_twap_30s",
        ),
        (
            flip_research.TWAP_60S_FLIP_DEFINITION_VERSION,
            flip_research.TWAP_60S_CUTOVER_MS,
            "chainlink_twap_60s",
        ),
    ),
)
def test_flip_analysis_records_definition_specific_twap_source_reference(
    definition_version,
    market_start_ms,
    source_reference,
):
    analysis = flip_research.analyze_market(
        market_id=market_start_ms // 300_000,
        market_start_ms=market_start_ms,
        market_end_ms=market_start_ms + 300_000,
        resolution_type="winner",
        threshold=Decimal("100"),
        official_close=Decimal("99"),
        winner="Down",
        chainlink_rows=(),
        probability_rows=(),
        microstructure_rows=(),
        definition_version=definition_version,
    )

    assert analysis.data_quality["source_reference"] == source_reference


def test_due_evaluator_runs_current_v3_before_unfinished_legacy_v2(monkeypatch):
    calls = []

    async def fake_evaluate(settings, pool, *, definition_version, now_ms):
        calls.append((settings, pool, definition_version, now_ms))
        return 1

    monkeypatch.setattr(
        flip_research,
        "_evaluate_due_flip_definition_once",
        fake_evaluate,
    )

    completed = asyncio.run(
        flip_research.evaluate_due_flip_markets_once(
            "settings",
            "pool",
            now_ms=flip_research.TWAP_60S_CUTOVER_MS + 30_000,
        )
    )

    assert completed == 2
    assert [call[2] for call in calls] == [
        flip_research.TWAP_60S_FLIP_DEFINITION_VERSION,
        flip_research.TWAP_30S_FLIP_DEFINITION_VERSION,
    ]
    assert {call[3] for call in calls} == {
        flip_research.TWAP_60S_CUTOVER_MS + 30_000
    }


def test_flip_due_scan_rejects_unversioned_twap_definition():
    with pytest.raises(ValueError, match="unsupported flip definition version"):
        asyncio.run(
            flip_research.fetch_due_flip_markets(
                object(),
                now_ms=1_000_000,
                definition_version=4,
            )
        )
