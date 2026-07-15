import asyncio
from decimal import Decimal

import pytest

from price_collector.market import MARKET_MS, MarketWindow
from price_collector.shadow_signal_reporting import (
    MAX_MARKET_ID,
    SHADOW_EVALUATION_MARKET_EXISTS_SQL,
    SHADOW_EVALUATION_MAX_POINTS,
    SHADOW_EVALUATION_POINTS_SQL,
    SHADOW_EVALUATION_QUERY_LIMIT,
    ShadowEvaluationReportingError,
    build_shadow_evaluation_report,
    fetch_shadow_evaluation_chart_points,
    shadow_evaluation_market_window,
    shadow_evaluation_model_spec,
)


MODEL_VERSION = "catchup_ratio_l3000_b100"
HORIZON_MS = 3_000
WINDOW = MarketWindow(
    market_id=10,
    market_start_ms=10 * MARKET_MS,
    market_end_ms=11 * MARKET_MS,
)


def evaluation_row(
    *,
    generated_ms=None,
    valid=True,
    actual=True,
    fingerprint="a" * 64,
    artifact="b" * 64,
    **overrides,
):
    if generated_ms is None:
        generated_ms = WINDOW.market_start_ms + 1_000
    target_ms = generated_ms + HORIZON_MS
    forecast_market_id = generated_ms // MARKET_MS

    chainlink_at_forecast = Decimal("100.000000000000000000")
    projected_chainlink = (
        Decimal("101.250000000000000000") if valid else None
    )
    pending_move = Decimal("1.250000000000000000") if valid else None
    pending_move_bps = Decimal("125.000000000000000000") if valid else None
    actual_chainlink = (
        Decimal("100.750000000000000000") if actual else None
    )

    row = {
        "selection_fingerprint_sha256": fingerprint,
        "selection_artifact_sha256": artifact,
        "model_version": MODEL_VERSION,
        "beta": Decimal("1.000000000000000000"),
        "generated_ms": generated_ms,
        "target_ms": target_ms,
        "matured_ms": target_ms + 7,
        "horizon_ms": HORIZON_MS,
        "valid": valid,
        "status": "valid" if valid else "chainlink_stale",
        "invalid_reasons": () if valid else ("chainlink_stale",),
        "state": "anchored",
        "forecast_market_id": forecast_market_id,
        "full_horizon_before_forecast_market_end": (
            target_ms <= (forecast_market_id + 1) * MARKET_MS
        ),
        "chainlink_at_forecast": chainlink_at_forecast,
        "projected_chainlink": projected_chainlink,
        "actual_chainlink": actual_chainlink,
        "actual_chainlink_source_timestamp_ms": (
            target_ms - 200 if actual else None
        ),
        "actual_chainlink_received_ms": target_ms - 100 if actual else None,
        "actual_chainlink_age_at_target_ms": 100 if actual else None,
        "pending_move": pending_move,
        "pending_move_bps": pending_move_bps,
        "direction": "up" if valid else None,
        "forecast_error": (
            projected_chainlink - actual_chainlink
            if projected_chainlink is not None and actual_chainlink is not None
            else None
        ),
        "baseline_error": (
            chainlink_at_forecast - actual_chainlink
            if actual_chainlink is not None
            else None
        ),
    }
    row.update(overrides)
    return row


class FakeConnection:
    def __init__(self, *, market_exists=True, rows=()):
        self.market_exists = market_exists
        self.rows = list(rows)
        self.fetchval_calls = []
        self.fetch_calls = []

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        return self.market_exists

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return self.rows


class AcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakePool:
    def __init__(self, connection):
        self.connection = connection
        self.acquire_calls = 0

    def acquire(self):
        self.acquire_calls += 1
        return AcquireContext(self.connection)


def build_report(rows, *, server_time_ms=None, model_version=MODEL_VERSION):
    if server_time_ms is None:
        server_time_ms = WINDOW.market_end_ms
    return build_shadow_evaluation_report(
        window=WINDOW,
        server_time_ms=server_time_ms,
        model_version=model_version,
        rows=rows,
    )


def test_fetch_uses_restricted_view_target_bounds_predecessor_and_hard_limit():
    source_row = evaluation_row()
    connection = FakeConnection(rows=[source_row])
    pool = FakePool(connection)

    result = asyncio.run(
        fetch_shadow_evaluation_chart_points(
            pool,
            window=WINDOW,
            model_version=MODEL_VERSION,
        )
    )

    assert result.market_exists is True
    assert result.rows == (source_row,)
    assert result.rows[0] is not source_row
    assert pool.acquire_calls == 1
    assert connection.fetchval_calls == [
        (SHADOW_EVALUATION_MARKET_EXISTS_SQL, (WINDOW.market_id,))
    ]
    assert connection.fetch_calls == [
        (
            SHADOW_EVALUATION_POINTS_SQL,
            (
                WINDOW.market_id,
                MODEL_VERSION,
                WINDOW.market_start_ms,
                WINDOW.market_end_ms,
                SHADOW_EVALUATION_QUERY_LIMIT,
            ),
        )
    ]

    normalized_sql = " ".join(SHADOW_EVALUATION_POINTS_SQL.split())
    assert "FROM public.shadow_signal_evaluation_chart_points" in normalized_sql
    assert "FROM public.shadow_signal_evaluations " not in normalized_sql
    assert (
        "forecast_market_id IN ($1::BIGINT, $1::BIGINT - 1)"
        in normalized_sql
    )
    assert "target_ms >= $3::BIGINT" in normalized_sql
    assert "target_ms < $4::BIGINT" in normalized_sql
    assert (
        "ORDER BY target_ms ASC, generated_ms ASC, horizon_ms ASC"
        in normalized_sql
    )
    assert "LIMIT $5::INTEGER" in normalized_sql
    assert SHADOW_EVALUATION_QUERY_LIMIT == SHADOW_EVALUATION_MAX_POINTS + 1


def test_report_serializes_exact_decimal_strings_without_rounding():
    report = build_report([evaluation_row()])
    point = report["points"][0]

    assert report["schema_version"] == 1
    assert report["market"] == {
        "market_id": WINDOW.market_id,
        "market_start_ms": WINDOW.market_start_ms,
        "market_end_ms": WINDOW.market_end_ms,
        "boundary": "[start_ms,end_ms)",
    }
    assert report["model"]["model_version"] == MODEL_VERSION
    assert report["model"]["horizon_ms"] == HORIZON_MS
    assert report["model"]["beta"] == "1"
    assert point["beta"] == "1.000000000000000000"
    assert point["chainlink_at_forecast"] == "100.000000000000000000"
    assert point["projected_chainlink"] == "101.250000000000000000"
    assert point["actual_chainlink"] == "100.750000000000000000"
    assert point["pending_move"] == "1.250000000000000000"
    assert point["pending_move_bps"] == "125.000000000000000000"
    assert point["forecast_error"] == "0.500000000000000000"
    assert point["baseline_error"] == "-0.750000000000000000"


def test_invalid_and_valid_unscored_attempts_preserve_honest_null_contract():
    invalid = evaluation_row(
        generated_ms=WINDOW.market_start_ms + 1_000,
        valid=False,
        actual=False,
    )
    unscored = evaluation_row(
        generated_ms=WINDOW.market_start_ms + 1_500,
        actual=False,
    )

    report = build_report([invalid, unscored])
    invalid_point, unscored_point = report["points"]

    assert invalid_point["valid"] is False
    assert invalid_point["invalid_reasons"] == ["chainlink_stale"]
    assert invalid_point["projected_chainlink"] is None
    assert invalid_point["pending_move"] is None
    assert invalid_point["pending_move_bps"] is None
    assert invalid_point["direction"] is None

    assert unscored_point["valid"] is True
    assert unscored_point["projected_chainlink"] == "101.250000000000000000"
    assert unscored_point["actual_chainlink"] is None
    assert unscored_point["forecast_error"] is None
    assert unscored_point["baseline_error"] is None
    assert report["coverage"] == {
        "window_buckets": 600,
        "market_window_elapsed": True,
        "observed_buckets": 2,
        "unobserved_buckets_as_of_response": 598,
        "attempts": 2,
        "valid_forecasts": 1,
        "scored": 0,
        "invalid": 1,
        "valid_without_actual": 1,
    }


def test_current_market_does_not_label_future_buckets_unobserved():
    report = build_report(
        [evaluation_row()],
        server_time_ms=WINDOW.market_start_ms + 60_000,
    )

    assert report["coverage"]["market_window_elapsed"] is False
    assert report["coverage"]["observed_buckets"] == 1
    assert report["coverage"]["unobserved_buckets_as_of_response"] is None


def test_completed_market_counts_duplicate_restart_attempts_in_one_bucket_once():
    first = evaluation_row(generated_ms=WINDOW.market_start_ms + 1_001)
    duplicate_bucket = evaluation_row(
        generated_ms=WINDOW.market_start_ms + 1_099,
    )

    report = build_report([duplicate_bucket, first])

    assert [point["generated_ms"] for point in report["points"]] == [
        first["generated_ms"],
        duplicate_bucket["generated_ms"],
    ]
    assert report["coverage"]["attempts"] == 2
    assert report["coverage"]["observed_buckets"] == 1
    assert report["coverage"]["unobserved_buckets_as_of_response"] == 599
    assert report["coverage"]["valid_forecasts"] == 2
    assert report["coverage"]["scored"] == 2


def test_selection_identity_uses_fingerprint_and_artifact_as_a_pair():
    rows = [
        evaluation_row(
            generated_ms=WINDOW.market_start_ms + 2_000,
            fingerprint="b" * 64,
            artifact="d" * 64,
        ),
        evaluation_row(
            generated_ms=WINDOW.market_start_ms + 1_000,
            fingerprint="a" * 64,
            artifact="c" * 64,
        ),
        evaluation_row(
            generated_ms=WINDOW.market_start_ms + 1_500,
            fingerprint="a" * 64,
            artifact="c" * 64,
        ),
    ]

    report = build_report(rows)

    assert report["model"]["selection_identities"] == [
        {
            "fingerprint_sha256": "a" * 64,
            "artifact_sha256": "c" * 64,
        },
        {
            "fingerprint_sha256": "b" * 64,
            "artifact_sha256": "d" * 64,
        },
    ]


def test_preceding_market_forecast_is_selected_by_target_time():
    row = evaluation_row(
        generated_ms=WINDOW.market_start_ms - 2_500,
    )

    report = build_report([row])
    point = report["points"][0]

    assert point["forecast_market_id"] == WINDOW.market_id - 1
    assert point["target_ms"] == WINDOW.market_start_ms + 500
    assert point["full_horizon_before_forecast_market_end"] is False


def test_target_exactly_at_market_start_is_included():
    row = evaluation_row(
        generated_ms=WINDOW.market_start_ms - HORIZON_MS,
    )

    report = build_report([row])
    point = report["points"][0]

    assert point["forecast_market_id"] == WINDOW.market_id - 1
    assert point["target_ms"] == WINDOW.market_start_ms
    assert point["full_horizon_before_forecast_market_end"] is True


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"model_version": "catchup_ratio_l3500_b100"}, "model_version"),
        ({"horizon_ms": 3_500}, "horizon_ms"),
        ({"beta": Decimal("0.5")}, "beta"),
        ({"target_ms": WINDOW.market_end_ms}, "target_ms"),
        ({"forecast_error": Decimal("99")}, "forecast_error"),
        ({"actual_chainlink_age_at_target_ms": 101}, "actual age"),
    ],
)
def test_report_rejects_rows_that_violate_public_integrity(overrides, message):
    row = evaluation_row(**overrides)

    with pytest.raises(ShadowEvaluationReportingError, match=message):
        build_report([row])


def test_report_rejects_invalid_row_with_projection_output():
    row = evaluation_row(valid=False, actual=False)
    row["projected_chainlink"] = Decimal("101")

    with pytest.raises(
        ShadowEvaluationReportingError,
        match="invalid row contains projection output",
    ):
        build_report([row])


def test_report_rejects_more_than_the_hard_row_limit_before_serializing():
    row = evaluation_row()

    with pytest.raises(
        ShadowEvaluationReportingError,
        match="exceeds 1000 rows",
    ):
        build_report([row] * (SHADOW_EVALUATION_MAX_POINTS + 1))


def test_unsupported_model_is_rejected_before_database_acquisition():
    pool = FakePool(FakeConnection())

    with pytest.raises(ValueError, match="unsupported shadow evaluation"):
        asyncio.run(
            fetch_shadow_evaluation_chart_points(
                pool,
                window=WINDOW,
                model_version="unknown_model",
            )
        )

    assert pool.acquire_calls == 0
    with pytest.raises(ValueError, match="unsupported shadow evaluation"):
        shadow_evaluation_model_spec("unknown_model")


@pytest.mark.parametrize(
    ("model_version", "horizon_ms"),
    (
        ("catchup_ratio_l3000_b100", 3_000),
        ("catchup_ratio_l3500_b100", 3_500),
        ("catchup_ratio_l4000_b100", 4_000),
    ),
)
def test_supported_model_registry_has_fixed_horizon_and_beta(
    model_version,
    horizon_ms,
):
    spec = shadow_evaluation_model_spec(model_version)

    assert spec.model_version == model_version
    assert spec.horizon_ms == horizon_ms
    assert spec.beta == Decimal("1")


def test_reporting_market_window_uses_shared_market_boundaries_and_bigint_limit():
    window = shadow_evaluation_market_window(WINDOW.market_id)

    assert window == WINDOW
    assert shadow_evaluation_market_window(MAX_MARKET_ID).market_end_ms <= (
        (1 << 63) - 1
    )

    with pytest.raises(TypeError, match="integer"):
        shadow_evaluation_market_window(True)
    with pytest.raises(ValueError, match="BIGINT"):
        shadow_evaluation_market_window(-1)
    with pytest.raises(ValueError, match="BIGINT"):
        shadow_evaluation_market_window(MAX_MARKET_ID + 1)
