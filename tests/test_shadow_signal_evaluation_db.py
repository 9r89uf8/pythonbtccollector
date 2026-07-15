import asyncio
from dataclasses import fields
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import price_collector.db as db
from price_collector.shadow_signal_evaluation import ShadowEvaluationRecord


ROOT = Path(__file__).resolve().parents[1]


def evaluation_record(**overrides):
    values = {
        "selection_schema_version": 2,
        "selection_policy_version": "chronological_holdout_v2",
        "selection_fingerprint_sha256": "a" * 64,
        "selection_artifact_sha256": "b" * 64,
        "selection_evidence_end_ms": 5_000,
        "model_version": "catchup_ratio_l3000_b100",
        "beta": Decimal("1"),
        "generated_ms": 10_000,
        "target_ms": 13_000,
        "matured_ms": 13_100,
        "horizon_ms": 3_000,
        "valid": True,
        "status": "valid",
        "invalid_reasons": (),
        "state": "anchored",
        "market_id": 0,
        "market_start_ms": 0,
        "market_end_ms": 300_000,
        "ms_to_market_end": 290_000,
        "full_horizon_before_market_end": True,
        "chainlink_at_forecast": Decimal("62000.0"),
        "chainlink_at_forecast_source_timestamp_ms": 9_000,
        "chainlink_at_forecast_received_ms": 9_950,
        "projected_chainlink": Decimal("62001.0"),
        "pending_move": Decimal("1.0"),
        "pending_move_bps": Decimal("0.161290322580645161"),
        "direction": "up",
        "futures_now": Decimal("62101.0"),
        "futures_now_source_timestamp_ms": 9_900,
        "futures_now_received_ms": 9_980,
        "futures_reference": Decimal("62100.0"),
        "futures_reference_source_timestamp_ms": 6_900,
        "futures_reference_received_ms": 6_980,
        "futures_reference_target_ms": 6_950,
        "futures_reference_gap_ms": 30,
        "futures_received_age_ms": 20,
        "chainlink_received_age_ms": 50,
        "actual_chainlink": Decimal("62000.5"),
        "actual_chainlink_source_timestamp_ms": 12_000,
        "actual_chainlink_received_ms": 12_950,
        "actual_chainlink_age_at_target_ms": 50,
        "forecast_error": Decimal("0.5"),
        "baseline_error": Decimal("-0.5"),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakePool:
    def __init__(self, connection):
        self.connection = connection
        self.acquire_calls = []
        self.closed = False

    def acquire(self, **kwargs):
        self.acquire_calls.append(kwargs)
        return FakeAcquire(self.connection)

    async def close(self):
        self.closed = True


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeTransactionalConnection:
    def transaction(self):
        return FakeTransaction()


def test_shadow_evaluation_pool_is_lazy_single_connection_and_tunable(
    monkeypatch,
):
    calls = []

    async def fake_create_pool(database_url, **kwargs):
        calls.append((database_url, kwargs))
        return "shadow-pool"

    monkeypatch.setattr(db.asyncpg, "create_pool", fake_create_pool)

    result = asyncio.run(
        db.create_shadow_evaluation_pool(
            "postgresql://writer@127.0.0.1:5432/price_collector",
            connect_timeout_seconds=2,
            command_timeout_seconds=3,
        )
    )

    assert result == "shadow-pool"
    assert calls == [
        (
            "postgresql://writer@127.0.0.1:5432/price_collector",
            {
                "min_size": 0,
                "max_size": 1,
                "timeout": 2,
                "command_timeout": 3,
                "server_settings": {
                    "application_name": "price_collector_shadow_evaluation"
                },
            },
        )
    ]


@pytest.mark.parametrize(
    ("field_name", "value", "error", "message"),
    [
        ("connect_timeout_seconds", True, TypeError, "must be a number"),
        ("connect_timeout_seconds", 0, ValueError, "must be positive"),
        ("command_timeout_seconds", "5", TypeError, "must be a number"),
        ("command_timeout_seconds", -1, ValueError, "must be positive"),
    ],
)
def test_shadow_evaluation_pool_rejects_invalid_timeouts(
    monkeypatch,
    field_name,
    value,
    error,
    message,
):
    async def fake_create_pool(*args, **kwargs):
        raise AssertionError("invalid timeouts must fail before pool creation")

    monkeypatch.setattr(db.asyncpg, "create_pool", fake_create_pool)
    kwargs = {
        "connect_timeout_seconds": 5,
        "command_timeout_seconds": 5,
        field_name: value,
    }

    with pytest.raises(error, match=message):
        asyncio.run(
            db.create_shadow_evaluation_pool(
                "postgresql://writer/price_collector",
                **kwargs,
            )
        )


def test_shadow_evaluation_row_is_fixed_and_preserves_decimal_values():
    record = evaluation_record()

    row = db.shadow_signal_evaluation_row(record)
    mapped = dict(zip(db.SHADOW_SIGNAL_EVALUATION_COLUMNS, row))

    assert len(row) == len(db.SHADOW_SIGNAL_EVALUATION_COLUMNS) == 43
    assert mapped["model_version"] == record.model_version
    assert mapped["invalid_reasons"] == ()
    assert mapped["projected_chainlink"] is record.projected_chainlink
    assert mapped["forecast_error"] is record.forecast_error
    assert isinstance(mapped["baseline_error"], Decimal)


def test_shadow_evaluation_row_preserves_known_reference_target_without_gap():
    record = evaluation_record(
        valid=False,
        status="anchor_history_missing",
        invalid_reasons=("anchor_history_missing",),
        projected_chainlink=None,
        pending_move=None,
        pending_move_bps=None,
        direction=None,
        futures_reference=None,
        futures_reference_source_timestamp_ms=None,
        futures_reference_received_ms=None,
        futures_reference_target_ms=6_950,
        futures_reference_gap_ms=None,
        forecast_error=None,
    )

    mapped = dict(
        zip(
            db.SHADOW_SIGNAL_EVALUATION_COLUMNS,
            db.shadow_signal_evaluation_row(record),
        )
    )

    assert mapped["futures_reference_target_ms"] == 6_950
    assert mapped["futures_reference_gap_ms"] is None


def test_database_column_contract_matches_typed_evaluation_record():
    assert db.SHADOW_SIGNAL_EVALUATION_COLUMNS == tuple(
        field.name for field in fields(ShadowEvaluationRecord)
    )


def test_shadow_evaluation_row_rejects_float_financial_values():
    with pytest.raises(TypeError, match="pending_move_bps must be Decimal"):
        db.shadow_signal_evaluation_row(
            evaluation_record(pending_move_bps=0.1)
        )


def test_shadow_evaluation_backend_batches_idempotent_inserts_once():
    class FakeConnection(FakeTransactionalConnection):
        def __init__(self):
            self.calls = []

        async def executemany(self, query, rows):
            self.calls.append((" ".join(query.split()), rows))

    connection = FakeConnection()
    pool = FakePool(connection)
    backend = db.AsyncpgShadowEvaluationBackend(
        pool,
        connect_timeout_seconds=2,
    )
    record = evaluation_record()

    result = asyncio.run(backend.write_evaluations([record]))

    assert pool.acquire_calls == [{"timeout": 2}]
    assert len(connection.calls) == 1
    query, rows = connection.calls[0]
    assert "INSERT INTO shadow_signal_evaluations" in query
    assert (
        "ON CONFLICT (model_version, generated_ms, horizon_ms) DO NOTHING"
        in query
    )
    assert rows == [db.shadow_signal_evaluation_row(record)]
    assert result.persisted_count == 1
    assert result.rejected_count == 0
    assert result.deferred_records == ()


def test_shadow_evaluation_backend_skips_empty_batches_without_db_access():
    pool = FakePool(None)
    backend = db.AsyncpgShadowEvaluationBackend(pool)

    result = asyncio.run(backend.write_evaluations([]))

    assert pool.acquire_calls == []
    assert result.persisted_count == 0
    assert result.rejected_count == 0
    assert result.deferred_records == ()


@pytest.mark.parametrize(
    "error_class_name",
    ("IntegrityConstraintViolationError", "DataError"),
)
def test_shadow_evaluation_backend_isolates_permanently_rejected_row(
    monkeypatch,
    caplog,
    error_class_name,
):
    generated_index = db.SHADOW_SIGNAL_EVALUATION_COLUMNS.index(
        "generated_ms"
    )
    poison_generated_ms = 20_000

    class PermanentCheckViolation(Exception):
        sqlstate = "23514"
        constraint_name = (
            "shadow_signal_evaluations_projection_consistency_check"
        )

    class FakeConnection(FakeTransactionalConnection):
        def __init__(self):
            self.calls = []
            self.persisted_generated_ms = []

        async def executemany(self, query, rows):
            generated_values = [row[generated_index] for row in rows]
            self.calls.append(generated_values)
            if poison_generated_ms in generated_values:
                raise PermanentCheckViolation("projection check failed")
            self.persisted_generated_ms.extend(generated_values)

    monkeypatch.setattr(
        db.asyncpg,
        error_class_name,
        PermanentCheckViolation,
    )
    connection = FakeConnection()
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(connection))
    records = [
        evaluation_record(generated_ms=10_000),
        evaluation_record(generated_ms=20_000),
        evaluation_record(generated_ms=30_000),
    ]

    result = asyncio.run(backend.write_evaluations(records))

    assert result.persisted_count == 2
    assert result.rejected_count == 1
    assert result.deferred_records == ()
    assert connection.persisted_generated_ms == [10_000, 30_000]
    assert connection.calls == [
        [10_000, 20_000, 30_000],
        [10_000],
        [20_000, 30_000],
        [20_000],
        [30_000],
    ]
    assert "shadow_evaluation_record_rejected" in caplog.text


def test_shadow_evaluation_backend_bounds_all_poison_isolation(
    monkeypatch,
    caplog,
):
    class PermanentCheckViolation(Exception):
        sqlstate = "23514"
        constraint_name = (
            "shadow_signal_evaluations_projection_consistency_check"
        )

    class FakeConnection(FakeTransactionalConnection):
        def __init__(self):
            self.calls = 0

        async def executemany(self, query, rows):
            self.calls += 1
            raise PermanentCheckViolation("projection check failed")

    monkeypatch.setattr(
        db.asyncpg,
        "IntegrityConstraintViolationError",
        PermanentCheckViolation,
    )
    connection = FakeConnection()
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(connection))
    records = [
        evaluation_record(generated_ms=index)
        for index in range(500)
    ]

    result = asyncio.run(backend.write_evaluations(records))

    events = [getattr(record, "event", None) for record in caplog.records]
    assert result.persisted_count == 0
    assert result.rejected_count == 8
    assert len(result.deferred_records) == 492
    assert connection.calls < 40
    assert events.count("shadow_evaluation_record_rejected") == 1
    assert events.count(
        "shadow_evaluation_rejection_isolation_limit_reached"
    ) == 1
    assert backend._permanent_rejections_total == 8


def test_shadow_evaluation_backend_defers_unprobed_good_rows(
    monkeypatch,
):
    generated_index = db.SHADOW_SIGNAL_EVALUATION_COLUMNS.index(
        "generated_ms"
    )
    poison_values = set(range(8))

    class PermanentCheckViolation(Exception):
        pass

    class FakeConnection(FakeTransactionalConnection):
        def __init__(self):
            self.persisted_generated_ms = []

        async def executemany(self, query, rows):
            generated_values = [row[generated_index] for row in rows]
            if poison_values.intersection(generated_values):
                raise PermanentCheckViolation("poison row")
            self.persisted_generated_ms.extend(generated_values)

    monkeypatch.setattr(
        db.asyncpg,
        "IntegrityConstraintViolationError",
        PermanentCheckViolation,
    )
    connection = FakeConnection()
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(connection))
    records = [evaluation_record(generated_ms=index) for index in range(16)]

    first = asyncio.run(backend.write_evaluations(records))
    second = asyncio.run(
        backend.write_evaluations(first.deferred_records)
    )

    assert first.persisted_count == 0
    assert first.rejected_count == 8
    assert [
        record.generated_ms for record in first.deferred_records
    ] == list(range(8, 16))
    assert second.persisted_count == 8
    assert second.rejected_count == 0
    assert second.deferred_records == ()
    assert connection.persisted_generated_ms == list(range(8, 16))


@pytest.mark.parametrize(
    ("terminal_error", "message"),
    (
        (RuntimeError("connection lost"), "connection lost"),
        (asyncio.CancelledError(), None),
    ),
)
def test_shadow_evaluation_isolation_rolls_back_before_terminal_error(
    monkeypatch,
    terminal_error,
    message,
):
    generated_index = db.SHADOW_SIGNAL_EVALUATION_COLUMNS.index(
        "generated_ms"
    )

    class PermanentCheckViolation(Exception):
        pass

    class Transaction:
        def __init__(self, connection):
            self.connection = connection

        async def __aenter__(self):
            self.connection.frames.append([])
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            staged = self.connection.frames.pop()
            if exc_type is None:
                if self.connection.frames:
                    self.connection.frames[-1].extend(staged)
                else:
                    self.connection.committed.extend(staged)
            return False

    class FakeConnection:
        def __init__(self):
            self.frames = []
            self.committed = []

        def transaction(self):
            return Transaction(self)

        async def executemany(self, query, rows):
            generated_values = [row[generated_index] for row in rows]
            if 20_000 in generated_values:
                raise PermanentCheckViolation("poison row")
            if 30_000 in generated_values:
                raise terminal_error
            self.frames[-1].extend(generated_values)

    monkeypatch.setattr(
        db.asyncpg,
        "IntegrityConstraintViolationError",
        PermanentCheckViolation,
    )
    connection = FakeConnection()
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(connection))
    records = [
        evaluation_record(generated_ms=10_000),
        evaluation_record(generated_ms=20_000),
        evaluation_record(generated_ms=30_000),
    ]

    with pytest.raises(type(terminal_error), match=message):
        asyncio.run(backend.write_evaluations(records))

    assert connection.frames == []
    assert connection.committed == []
    assert backend._permanent_rejections_total == 0


def test_shadow_evaluation_backend_does_not_retry_ambiguous_insert_failure():
    class FakeConnection(FakeTransactionalConnection):
        def __init__(self):
            self.calls = 0

        async def executemany(self, query, rows):
            self.calls += 1
            raise RuntimeError("ambiguous insert failure")

    connection = FakeConnection()
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(connection))

    with pytest.raises(RuntimeError, match="ambiguous insert failure"):
        asyncio.run(backend.write_evaluations([evaluation_record()]))

    assert connection.calls == 1


def test_shadow_evaluation_backend_does_not_bisect_transient_batch_failure():
    class FakeConnection(FakeTransactionalConnection):
        def __init__(self):
            self.calls = 0

        async def executemany(self, query, rows):
            self.calls += 1
            raise RuntimeError("database unavailable")

    connection = FakeConnection()
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(connection))

    with pytest.raises(RuntimeError, match="database unavailable"):
        asyncio.run(
            backend.write_evaluations(
                [evaluation_record(), evaluation_record()]
            )
        )

    assert connection.calls == 1


def test_shadow_evaluation_backend_deletes_expired_rows_in_bounded_order():
    class FakeConnection:
        def __init__(self):
            self.calls = []

        async def execute(self, query, *args):
            self.calls.append((" ".join(query.split()), args))
            return "DELETE 7"

    connection = FakeConnection()
    pool = FakePool(connection)
    backend = db.AsyncpgShadowEvaluationBackend(
        pool,
        connect_timeout_seconds=4,
    )

    deleted = asyncio.run(
        backend.delete_expired(
            cutoff_generated_ms=1_000_000,
            limit=7,
        )
    )

    assert deleted == 7
    assert pool.acquire_calls == [{"timeout": 4}]
    query, args = connection.calls[0]
    assert "WITH expired AS" in query
    assert "WHERE generated_ms < $1" in query
    assert "ORDER BY generated_ms LIMIT $2" in query
    assert "USING expired" in query
    assert args == (1_000_000, 7)


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        (
            {"cutoff_generated_ms": True, "limit": 1},
            TypeError,
            "cutoff_generated_ms must be an integer",
        ),
        (
            {"cutoff_generated_ms": -1, "limit": 1},
            ValueError,
            "cutoff_generated_ms must be non-negative",
        ),
        (
            {"cutoff_generated_ms": 1, "limit": 0},
            ValueError,
            "limit must be positive",
        ),
    ],
)
def test_shadow_evaluation_delete_validates_bounds(kwargs, error, message):
    backend = db.AsyncpgShadowEvaluationBackend(FakePool(None))

    with pytest.raises(error, match=message):
        asyncio.run(backend.delete_expired(**kwargs))


def test_shadow_evaluation_backend_close_delegates_to_pool():
    pool = FakePool(None)
    backend = db.AsyncpgShadowEvaluationBackend(pool)

    asyncio.run(backend.close())

    assert pool.closed is True


def test_schema_has_causal_internal_shadow_evaluation_table():
    schema = (ROOT / "schema.sql").read_text()
    start = schema.index("CREATE TABLE IF NOT EXISTS shadow_signal_evaluations")
    table = schema[start : schema.index(";", start) + 1]

    assert "PRIMARY KEY (model_version, generated_ms, horizon_ms)" in table
    for column in (
        "chainlink_at_forecast NUMERIC(38, 18)",
        "projected_chainlink NUMERIC(38, 18)",
        "futures_now NUMERIC(38, 18)",
        "futures_reference NUMERIC(38, 18)",
        "actual_chainlink NUMERIC(38, 18)",
        "forecast_error NUMERIC(38, 18)",
        "baseline_error NUMERIC(38, 18)",
    ):
        assert column in table

    assert "CHECK (generated_ms >= 0)" in table
    assert "generated_ms % 500" not in table
    assert "target_ms = generated_ms + horizon_ms" in table
    assert "matured_ms >= target_ms" in table
    assert "actual_chainlink_received_ms <= target_ms" in table
    assert "chainlink_at_forecast_received_ms <= generated_ms" in table
    assert "futures_now_received_ms <= generated_ms" in table
    assert "target_ms - actual_chainlink_received_ms" in table
    assert "forecast_error - (projected_chainlink - actual_chainlink)" in table
    assert "baseline_error - (chainlink_at_forecast - actual_chainlink)" in table
    assert "valid = (status = 'valid')" in table
    assert "full_horizon_before_market_end = (target_ms <= market_end_ms)" in table
    assert "selection_fingerprint_sha256 ~ '^[0-9a-f]{64}$'" in table
    assert "selection_artifact_sha256 ~ '^[0-9a-f]{64}$'" in table
    assert (
        "(futures_reference_target_ms IS NULL AND "
        "futures_reference_gap_ms IS NULL)"
        not in table
    )
    assert (
        "futures_reference_target_ms IS NULL\n"
        "        OR futures_reference_target_ms >= 0"
        in table
    )
    assert (
        "futures_reference_gap_ms IS NULL\n"
        "        OR (\n"
        "            futures_reference_target_ms IS NOT NULL"
        in table
    )

    assert "shadow_signal_evaluations_generated_idx" in schema
    assert "shadow_signal_evaluations_market_model_idx" in schema
    assert "REVOKE ALL ON TABLE shadow_signal_evaluations" in schema
    assert "FROM PUBLIC, price_reader, price_writer" in schema
    assert (
        "GRANT SELECT, INSERT, DELETE ON TABLE shadow_signal_evaluations "
        "TO price_writer"
        in schema
    )


def test_schema_exposes_only_the_restricted_shadow_evaluation_chart_view():
    schema = (ROOT / "schema.sql").read_text()
    start = schema.index(
        "CREATE OR REPLACE VIEW public.shadow_signal_evaluation_chart_points"
    )
    end_marker = "FROM public.shadow_signal_evaluations;"
    view = schema[start : schema.index(end_marker, start) + len(end_marker)]

    assert "WITH (security_barrier = true)" in view
    assert "security_invoker" not in view
    for column in (
        "selection_fingerprint_sha256",
        "selection_artifact_sha256",
        "model_version",
        "beta",
        "generated_ms",
        "target_ms",
        "matured_ms",
        "horizon_ms",
        "valid",
        "status",
        "invalid_reasons",
        "state",
        "market_id AS forecast_market_id",
        "AS full_horizon_before_forecast_market_end",
        "chainlink_at_forecast",
        "projected_chainlink",
        "actual_chainlink",
        "actual_chainlink_source_timestamp_ms",
        "actual_chainlink_received_ms",
        "actual_chainlink_age_at_target_ms",
        "pending_move",
        "pending_move_bps",
        "direction",
        "forecast_error",
        "baseline_error",
    ):
        assert column in view

    for forbidden in (
        "selection_policy_version",
        "futures_now",
        "futures_reference",
        "created_at",
    ):
        assert forbidden not in view

    assert (
        "REVOKE ALL ON TABLE public.shadow_signal_evaluation_chart_points\n"
        "    FROM PUBLIC, price_reader, price_writer;"
        in schema
    )
    assert (
        "GRANT SELECT ON TABLE public.shadow_signal_evaluation_chart_points\n"
        "    TO price_reader;"
        in schema
    )
