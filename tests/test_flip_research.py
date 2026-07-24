from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Optional

import pytest

from price_collector import flip_research


MARKET_START_MS = 0
MARKET_END_MS = 300_000


def chainlink(
    event_ms: int,
    price: str,
    *,
    received_ms: Optional[int] = None,
) -> dict:
    return {
        "sample_second_ms": (event_ms // 1000) * 1000,
        "price": Decimal(price),
        "provider_event_ms": event_ms,
        "provider_message_ms": event_ms,
        "received_ms": event_ms if received_ms is None else received_ms,
    }


def probability(
    sample_second_ms: int,
    *,
    received_ms: int,
    up: str = "0.40",
    down: str = "0.60",
) -> dict:
    return {
        "sample_second_ms": sample_second_ms,
        "provider_event_ms": received_ms,
        "received_ms": received_ms,
        "up_bid": Decimal(up),
        "up_ask": Decimal(up),
        "up_mid": Decimal(up),
        "down_bid": Decimal(down),
        "down_ask": Decimal(down),
        "down_mid": Decimal(down),
        "up_prob_norm": Decimal(up),
        "down_prob_norm": Decimal(down),
        "up_provider_event_ms": received_ms,
        "up_received_ms": received_ms,
        "down_provider_event_ms": received_ms,
        "down_received_ms": received_ms,
    }


def resolved_analysis(
    chainlink_rows: list[dict],
    *,
    winner: str | None = "Down",
    resolution_type: str | None = "winner",
    threshold: Decimal | None = Decimal("100"),
    official_close: Decimal | None = Decimal("99"),
    probability_rows: list[dict] | None = None,
    microstructure_rows: list[dict] | None = None,
) -> flip_research.FlipAnalysis:
    return flip_research.analyze_market(
        market_id=0,
        market_start_ms=MARKET_START_MS,
        market_end_ms=MARKET_END_MS,
        resolution_type=resolution_type,
        threshold=threshold,
        official_close=official_close,
        winner=winner,
        chainlink_rows=chainlink_rows,
        probability_rows=probability_rows or [],
        microstructure_rows=microstructure_rows or [],
    )


def every_cutoff_below_threshold() -> list[dict]:
    return [
        chainlink(MARKET_END_MS - seconds * 1000, "99")
        for seconds in range(20, 0, -1)
    ]


def test_side_for_price_preserves_exact_equality_as_tie():
    assert (
        flip_research.side_for_price(Decimal("100.01"), Decimal("100"))
        == "Up"
    )
    assert (
        flip_research.side_for_price(Decimal("99.99"), Decimal("100"))
        == "Down"
    )
    assert (
        flip_research.side_for_price(Decimal("100"), Decimal("100"))
        == "tie"
    )


def test_analysis_retains_all_strict_crossings_across_touches_and_marks_decisive():
    analysis = resolved_analysis(
        [
            chainlink(279_000, "99"),
            chainlink(280_000, "100"),
            chainlink(281_000, "101"),
            chainlink(285_000, "100"),
            chainlink(290_000, "99"),
            chainlink(295_000, "101"),
        ]
    )

    assert analysis.evaluation_status == "confirmed_flip"
    assert [event.direction for event in analysis.events] == [
        "down_to_up",
        "up_to_down",
        "down_to_up",
    ]
    assert [event.milliseconds_before_expiry for event in analysis.events] == [
        19_000,
        10_000,
        5_000,
    ]
    assert [event.decisive for event in analysis.events] == [False, True, False]
    assert analysis.decisive_event_sequence == 2
    assert analysis.first_crossing_ms_before_end == 19_000
    assert analysis.last_crossing_ms_before_end == 5_000


def test_crossing_at_first_window_observation_uses_pre_window_strict_predecessor():
    analysis = resolved_analysis(
        [
            chainlink(279_500, "99"),
            chainlink(280_000, "101"),
        ],
        winner="Up",
        official_close=Decimal("101"),
    )

    assert len(analysis.events) == 1
    assert analysis.events[0].milliseconds_before_expiry == 20_000
    assert analysis.events[0].previous_provider_event_ms == 279_500


def test_crossings_require_source_event_and_receive_before_market_end():
    analysis = resolved_analysis(
        [
            chainlink(298_000, "99"),
            chainlink(299_000, "101", received_ms=MARKET_END_MS),
            chainlink(MARKET_END_MS, "101", received_ms=MARKET_END_MS - 1),
        ]
    )

    assert analysis.events == ()
    assert analysis.chainlink_observation_count == 1


def test_non_flip_requires_all_twenty_causal_chainlink_cutoffs_fresh():
    analysis = resolved_analysis(every_cutoff_below_threshold())

    assert analysis.evaluation_status == "non_flip"
    assert analysis.crossing_count == 0
    assert analysis.fresh_cutoff_count == 20
    assert analysis.needs_archive is False

    stale = resolved_analysis(
        [chainlink(280_000, "99"), chainlink(299_000, "99")]
    )
    assert stale.evaluation_status == "ambiguous"
    assert stale.fresh_cutoff_count < 20
    assert stale.needs_archive is True


def test_crossing_across_stale_observation_gap_is_ambiguous():
    analysis = resolved_analysis(
        [
            chainlink(270_000, "99"),
            chainlink(295_000, "101"),
        ],
        winner="Up",
        official_close=Decimal("101"),
    )

    assert analysis.crossing_count == 1
    assert analysis.events[0].observation_gap_ms == 25_000
    assert analysis.evaluation_status == "ambiguous"
    assert analysis.data_quality["stale_crossing_count"] == 1
    assert "stale_crossing_gap" in flip_research._quality_flags(analysis)


def test_complete_split_is_ambiguous_even_with_fresh_cutoffs():
    analysis = resolved_analysis(
        [
            *every_cutoff_below_threshold(),
            chainlink(290_500, "101"),
        ],
        winner=None,
        resolution_type="split",
        official_close=Decimal("100"),
    )

    assert analysis.evaluation_status == "ambiguous"
    assert analysis.crossing_count > 0
    assert analysis.data_quality["official_complete"] is False


def test_cutoffs_use_only_causal_rows_and_exact_prior_microstructure_interval():
    chainlink_rows = [
        row
        for row in every_cutoff_below_threshold()
        if row["provider_event_ms"] not in {294_000, 295_000}
    ]
    chainlink_rows.extend(
        [
            chainlink(294_500, "101", received_ms=295_001),
            chainlink(293_500, "98", received_ms=294_000),
        ]
    )
    probability_rows = [
        probability(295_000, received_ms=294_900),
        probability(296_000, received_ms=294_950, up="0.90", down="0.10"),
    ]
    microstructure_rows = [
        {
            "sample_second_ms": 294_000,
            "collector_healthy": True,
            "spot_mid": Decimal("100.5"),
        }
    ]

    analysis = resolved_analysis(
        chainlink_rows,
        probability_rows=probability_rows,
        microstructure_rows=microstructure_rows,
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 5
    )

    assert cutoff.cutoff_ms == 295_000
    assert cutoff.chainlink is not None
    assert cutoff.chainlink.provider_event_ms == 293_500
    assert cutoff.apparent_side == "Down"
    assert cutoff.probability is not None
    assert cutoff.probability.sample_second_ms == 295_000
    assert cutoff.probability.up_mid == Decimal("0.40")
    assert cutoff.microstructure is not None
    assert cutoff.microstructure["sample_second_ms"] == 294_000


def test_stale_probability_is_preserved_but_marked_not_fresh():
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        probability_rows=[
            probability(270_000, received_ms=270_000),
        ],
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 1
    )

    assert cutoff.probability is not None
    assert cutoff.probability_fresh is False
    assert cutoff.probability_receive_age_ms == 29_000


def test_delayed_probability_source_is_not_fresh_despite_recent_receive():
    delayed = probability(295_000, received_ms=294_900)
    delayed["provider_event_ms"] = 270_000
    delayed["up_provider_event_ms"] = 270_000
    delayed["down_provider_event_ms"] = 270_000
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        probability_rows=[delayed],
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 5
    )

    assert cutoff.probability is not None
    assert cutoff.probability_source_age_ms == 25_000
    assert cutoff.probability_receive_age_ms == 100
    assert cutoff.probability_fresh is False


def test_probability_freshness_uses_oldest_up_and_down_components():
    mixed = probability(295_000, received_ms=294_900)
    mixed["down_provider_event_ms"] = 270_000
    mixed["down_received_ms"] = 270_000
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        probability_rows=[mixed],
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 5
    )

    assert cutoff.probability is not None
    assert cutoff.up_probability_receive_age_ms == 100
    assert cutoff.down_probability_receive_age_ms == 25_000
    assert cutoff.probability_receive_age_ms == 25_000
    assert cutoff.probability_fresh is False


def test_historical_probability_without_component_timestamps_is_not_fresh():
    historical = probability(295_000, received_ms=294_900)
    for field in (
        "up_provider_event_ms",
        "up_received_ms",
        "down_provider_event_ms",
        "down_received_ms",
    ):
        historical[field] = None
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        probability_rows=[historical],
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 5
    )

    assert cutoff.probability is not None
    assert cutoff.probability_fresh is False
    assert "unknown_probability_component_freshness" in (
        flip_research._cutoff_quality_flags(cutoff)
    )


def test_probability_row_received_after_cutoff_is_not_causal():
    late_row = probability(295_000, received_ms=295_001)
    late_row["provider_event_ms"] = 294_900
    late_row["up_provider_event_ms"] = 294_900
    late_row["down_provider_event_ms"] = 294_900
    late_row["up_received_ms"] = 294_900
    late_row["down_received_ms"] = 294_900
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        probability_rows=[late_row],
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 5
    )

    assert cutoff.probability is None


def test_probability_with_future_provider_timestamp_is_not_causal():
    future_source = probability(295_000, received_ms=294_900)
    future_source["provider_event_ms"] = 295_001
    future_source["up_provider_event_ms"] = 295_001
    future_source["down_provider_event_ms"] = 295_001
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        probability_rows=[future_source],
    )
    cutoff = next(
        item for item in analysis.cutoffs if item.seconds_before_end == 5
    )

    assert cutoff.probability is None
    assert cutoff.probability_fresh is False


def test_analysis_rejects_float_financial_inputs():
    with pytest.raises(TypeError, match="threshold must be Decimal"):
        resolved_analysis(  # type: ignore[arg-type]
            every_cutoff_below_threshold(),
            threshold=100.0,
        )


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _AsyncContext(self.connection)


class _FakeConnection:
    def transaction(self):
        return _AsyncContext(self)


def test_due_scan_waits_for_complete_official_prices_and_orders_oldest_first():
    captured = {}

    class Connection(_FakeConnection):
        async def fetch(self, query, *args):
            captured["query"] = query
            captured["args"] = args
            return []

    rows = asyncio.run(
        flip_research.fetch_due_flip_markets(
            _FakePool(Connection()),
            now_ms=1_000_000,
            finalization_grace_ms=30_000,
            limit=7,
        )
    )

    assert rows == []
    query = captured["query"]
    assert "r.resolution_status = 'resolved'" in query
    assert "r.resolution_type IS NOT NULL" in query
    assert "r.chainlink_open_price IS NOT NULL" in query
    assert "r.chainlink_close_price IS NOT NULL" in query
    assert "ORDER BY mw.market_end_ms ASC" in query
    assert "evaluation.archive_status = 'complete'" in query
    assert "archived_row.received_ms <" in query
    assert "live_row.received_ms" in query
    assert captured["args"] == (1_000_000, 1, 30_000, 7)


def test_chainlink_input_query_excludes_nullable_provider_timestamps():
    queries = []

    class Connection(_FakeConnection):
        async def fetch(self, query, *args):
            queries.append(query)
            return []

    result = asyncio.run(
        flip_research._load_market_inputs(Connection(), market_id=4)
    )

    assert result == ([], [], [])
    assert "ps.provider_event_ms IS NOT NULL" in queries[0]


def test_persistence_arguments_match_final_schema_and_keep_rows_immutable():
    microstructure_rows = [
        {
            "symbol": "BTCUSDT",
            "sample_second_ms": MARKET_END_MS - 21_000,
            "received_ms": MARKET_END_MS - 20_001,
            **{column: None for column in flip_research.MICROSTRUCTURE_VALUE_COLUMNS},
        }
    ]
    analysis = resolved_analysis(
        every_cutoff_below_threshold(),
        microstructure_rows=microstructure_rows,
    )
    values = flip_research._evaluation_arguments(
        analysis,
        evaluated_ms=400_000,
        evaluation_attempts=1,
    )
    persisted = dict(zip(flip_research._EVALUATION_COLUMNS, values))

    assert len(values) == len(flip_research._EVALUATION_COLUMNS)
    assert persisted["chainlink_cutoff_count"] == 20
    assert persisted["fresh_chainlink_cutoff_count"] == 20
    assert persisted["fresh_probability_cutoff_count"] == 0
    assert persisted["source_microstructure_row_count"] == 1
    assert persisted["archive_status"] == "not_required"
    assert persisted["retention_safe"] is True
    assert "DO UPDATE SET" in flip_research._INSERT_EVALUATION_SQL
    assert "observation_precision = 'evaluation_failed'" in (
        flip_research._INSERT_EVALUATION_SQL
    )
    assert "DO NOTHING" in flip_research._INSERT_EVENT_SQL
    assert "DO UPDATE" not in flip_research._INSERT_EVENT_SQL
    assert all(
        len(flip_research._cutoff_arguments(analysis, cutoff))
        == len(flip_research._CUTOFF_COLUMNS)
        for cutoff in analysis.cutoffs
    )


def test_required_archive_failure_is_durably_marked_after_evaluation_commit(
    monkeypatch,
):
    calls = []

    async def fake_load(connection, *, market_id):
        return every_cutoff_below_threshold(), [], []

    async def fake_persist(
        connection,
        analysis,
        *,
        evaluated_ms,
        evaluation_attempts,
    ):
        calls.append(("persist", analysis.evaluation_status))

    async def fake_archive(pool, **kwargs):
        calls.append(("archive", kwargs["market_id"]))
        raise ConnectionError("archive unavailable")

    async def fake_mark_failed(pool, **kwargs):
        calls.append(("failed", kwargs["market_id"], type(kwargs["error"])))

    monkeypatch.setattr(flip_research, "_load_market_inputs", fake_load)
    monkeypatch.setattr(flip_research, "_persist_new_analysis", fake_persist)
    monkeypatch.setattr(
        flip_research,
        "archive_flip_microstructure",
        fake_archive,
    )
    monkeypatch.setattr(
        flip_research,
        "_mark_archive_failed",
        fake_mark_failed,
    )

    succeeded = asyncio.run(
        flip_research.evaluate_flip_market(
            _FakePool(_FakeConnection()),
            {
                "market_id": 1,
                "market_start_ms": 300_000,
                "market_end_ms": 600_000,
                "resolution_type": "split",
                "chainlink_open_price": Decimal("100"),
                "chainlink_close_price": Decimal("100"),
                "winner": None,
                "evaluation_attempts": 0,
            },
            now_ms=700_000,
        )
    )

    assert succeeded is False
    assert calls == [
        ("persist", "ambiguous"),
        ("archive", 1),
        ("failed", 1, ConnectionError),
    ]


def test_archive_upsert_refreshes_late_rows_before_marking_retention_safe():
    executed = []
    values = iter([300, 300, 0])

    class Connection(_FakeConnection):
        async def fetchval(self, query, *args):
            return next(values)

        async def execute(self, query, *args):
            executed.append((query, args))
            if f"UPDATE {flip_research.EVALUATION_TABLE}" in query:
                return "UPDATE 1"
            return "INSERT 0 1"

    result = asyncio.run(
        flip_research.archive_flip_microstructure(
            _FakePool(Connection()),
            market_id=7,
            archived_ms=900_000,
        )
    )

    assert result == (300, 300)
    archive_sql = next(
        query
        for query, _args in executed
        if f"INSERT INTO {flip_research.MICROSTRUCTURE_ARCHIVE_TABLE}" in query
    )
    assert "ON CONFLICT (symbol, sample_second_ms)" in archive_sql
    assert "DO UPDATE SET" in archive_sql
    assert "spot_mid = EXCLUDED.spot_mid" in archive_sql
    assert "EXCLUDED.received_ms >=" in archive_sql
    update_sql, update_args = next(
        (query, args)
        for query, args in executed
        if f"UPDATE {flip_research.EVALUATION_TABLE}" in query
    )
    assert update_args[2:4] == (300, 300)
    assert "source_microstructure_row_count = $3" in update_sql


def test_archive_reconciliation_accepts_live_subset_after_retention():
    values = iter([300, 300, 0])

    class Connection(_FakeConnection):
        async def fetchval(self, query, *args):
            return next(values)

        async def execute(self, query, *args):
            if f"UPDATE {flip_research.EVALUATION_TABLE}" in query:
                return "UPDATE 1"
            return "INSERT 0 1"

    assert asyncio.run(
        flip_research.archive_flip_microstructure(
            _FakePool(Connection()),
            market_id=7,
            archived_ms=900_000,
        )
    ) == (300, 300)


def test_archive_reconciliation_fails_when_archive_lost_recorded_rows():
    values = iter([300, 299, 0])

    class Connection(_FakeConnection):
        async def fetchval(self, query, *args):
            return next(values)

        async def execute(self, query, *args):
            return "INSERT 0 1"

    with pytest.raises(
        flip_research.FlipArchiveVerificationError,
        match="recorded_source=300, archive=299",
    ):
        asyncio.run(
            flip_research.archive_flip_microstructure(
                _FakePool(Connection()),
                market_id=7,
                archived_ms=900_000,
            )
        )


def test_complete_archive_due_row_resumes_archive_without_reanalysis(monkeypatch):
    calls = []

    async def fake_archive(pool, **kwargs):
        calls.append(kwargs)
        return 300, 300

    async def unexpected_load(*args, **kwargs):
        raise AssertionError("complete archive must not be re-analyzed")

    monkeypatch.setattr(
        flip_research,
        "archive_flip_microstructure",
        fake_archive,
    )
    monkeypatch.setattr(flip_research, "_load_market_inputs", unexpected_load)

    succeeded = asyncio.run(
        flip_research.evaluate_flip_market(
            object(),
            {
                "market_id": 11,
                "existing_archive_status": "complete",
            },
            now_ms=950_000,
        )
    )

    assert succeeded is True
    assert calls[0]["market_id"] == 11


def test_failed_evaluation_sentinel_is_reanalyzed_instead_of_archived(monkeypatch):
    calls = []

    async def fake_load(connection, *, market_id):
        calls.append(("load", market_id))
        return every_cutoff_below_threshold(), [], []

    async def fake_persist(connection, analysis, **kwargs):
        calls.append(("persist", analysis.evaluation_status))

    async def unexpected_archive(*args, **kwargs):
        raise AssertionError("evaluation sentinel must be re-analyzed")

    monkeypatch.setattr(flip_research, "_load_market_inputs", fake_load)
    monkeypatch.setattr(flip_research, "_persist_new_analysis", fake_persist)
    monkeypatch.setattr(
        flip_research,
        "archive_flip_microstructure",
        unexpected_archive,
    )

    succeeded = asyncio.run(
        flip_research.evaluate_flip_market(
            _FakePool(_FakeConnection()),
            {
                "market_id": 1,
                "market_start_ms": 0,
                "market_end_ms": MARKET_END_MS,
                "resolution_type": "winner",
                "chainlink_open_price": Decimal("100"),
                "chainlink_close_price": Decimal("99"),
                "winner": "Down",
                "existing_archive_status": "failed",
                "existing_observation_precision": "evaluation_failed",
                "evaluation_attempts": 2,
            },
            now_ms=400_000,
        )
    )

    assert succeeded is True
    assert calls == [("load", 1), ("persist", "non_flip")]


def test_pre_persistence_failure_is_durably_scheduled_without_stopping_batch(
    monkeypatch,
):
    markets = [
        {"market_id": 1},
        {"market_id": 2},
    ]
    calls = []

    async def fake_due(*args, **kwargs):
        return markets

    async def fake_evaluate(pool, market, **kwargs):
        if market["market_id"] == 1:
            raise ValueError("poison market")
        return True

    async def fake_mark(pool, market, **kwargs):
        calls.append(
            (
                market["market_id"],
                kwargs["failed_ms"],
                type(kwargs["error"]),
            )
        )

    monkeypatch.setattr(flip_research, "fetch_due_flip_markets", fake_due)
    monkeypatch.setattr(flip_research, "evaluate_flip_market", fake_evaluate)
    monkeypatch.setattr(flip_research, "_mark_evaluation_failed", fake_mark)

    completed = asyncio.run(
        flip_research.evaluate_due_flip_markets_once(
            SimpleNamespace(
                POLYMARKET_RESOLUTION_BATCH_SIZE=20,
                POLYMARKET_RESOLUTION_POLL_SECONDS=5,
                POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300,
            ),
            object(),
            now_ms=800_000,
        )
    )

    assert completed == 1
    assert calls == [(1, 800_000, ValueError)]


def test_retry_loop_uses_resolution_settings_without_coupling_to_live_collection(
    monkeypatch,
):
    calls = []

    async def fake_due(*args, **kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(flip_research, "fetch_due_flip_markets", fake_due)
    completed = asyncio.run(
        flip_research.evaluate_due_flip_markets_once(
            SimpleNamespace(
                POLYMARKET_RESOLUTION_BATCH_SIZE=9,
                POLYMARKET_RESOLUTION_POLL_SECONDS=5,
                POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300,
            ),
            object(),
            now_ms=800_000,
        )
    )

    assert completed == 0
    assert calls[0]["limit"] == 9
    assert calls[0]["finalization_grace_ms"] == 30_000


def test_cutoff_reversal_list_requires_a_fresh_chainlink_cutoff():
    captured = {}

    class Connection(_FakeConnection):
        async def fetch(self, query, *args):
            captured["query"] = query
            return []

    rows = asyncio.run(
        flip_research.fetch_flip_markets(
            _FakePool(Connection()),
            definition_version=1,
            within_seconds=5,
            kind="cutoff_reversal",
            direction=None,
            winner=None,
            start_ms=None,
            end_ms=None,
            before_market_id=None,
            limit=21,
        )
    )

    assert rows == []
    assert "cutoff.chainlink_fresh = TRUE" in captured["query"]


def test_distribution_bins_survive_an_empty_eligible_population():
    queries = []

    class Connection(_FakeConnection):
        async def fetchrow(self, query, *args):
            queries.append(query)
            return {
                "resolved_markets": 0,
                "eligible_markets": 0,
                "ambiguous_markets": 0,
                "markets_with_any_crossing": 0,
            }

        async def fetch(self, query, *args):
            queries.append(query)
            return []

    result = asyncio.run(
        flip_research.fetch_flip_distribution(
            _FakePool(Connection()),
            definition_version=1,
            max_seconds=20,
            direction=None,
            start_ms=None,
            end_ms=None,
        )
    )

    assert result["population"]["eligible_markets"] == 0
    crossing_query = next(query for query in queries if "WITH bins AS" in query)
    assert "LEFT JOIN eligible ON TRUE" in crossing_query
    assert "CROSS JOIN eligible" not in crossing_query
    assert "evaluation.evaluation_status <> 'ambiguous'" in crossing_query
    assert "ROUND(" in crossing_query
    cutoff_query = next(
        query for query in queries if "WITH seconds AS" in query
    )
    assert "ROUND(" in cutoff_query
