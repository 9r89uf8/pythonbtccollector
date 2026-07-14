from decimal import Decimal, localcontext
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = (ROOT / "schema.sql").read_text()

PROJECTION_CONSTRAINT = (
    "shadow_signal_evaluations_projection_consistency_check"
)
FORECAST_ERROR_CONSTRAINT = (
    "shadow_signal_evaluations_forecast_error_consistency_check"
)
BASELINE_ERROR_CONSTRAINT = (
    "shadow_signal_evaluations_baseline_error_consistency_check"
)
DEPLOYED_AUTO_CONSTRAINT = "shadow_signal_evaluations_check17"


def _shadow_evaluation_table() -> str:
    start = SCHEMA.index(
        "CREATE TABLE IF NOT EXISTS shadow_signal_evaluations"
    )
    return SCHEMA[start : SCHEMA.index(";", start) + 1]


def _projection_constraint_migration() -> str:
    start = SCHEMA.index(
        "-- Phase 5 initially shipped the projection consistency check"
    )
    end = SCHEMA.index(
        "CREATE INDEX IF NOT EXISTS shadow_signal_evaluations_generated_idx",
        start,
    )
    return SCHEMA[start:end]


def test_fresh_shadow_table_names_dependent_numeric_constraints():
    table = _shadow_evaluation_table()

    assert f"CONSTRAINT {PROJECTION_CONSTRAINT} CHECK" in table
    assert f"CONSTRAINT {FORECAST_ERROR_CONSTRAINT} CHECK" in table
    assert f"CONSTRAINT {BASELINE_ERROR_CONSTRAINT} CHECK" in table
    assert DEPLOYED_AUTO_CONSTRAINT not in table


def test_projection_bps_check_multiplies_before_numeric_division():
    table = _shadow_evaluation_table()
    migration = _projection_constraint_migration()

    assert "pending_move * 10000 / chainlink_at_forecast" in table
    assert "pending_move / chainlink_at_forecast * 10000" not in table
    assert "pending_move * 10000" in migration
    assert "/ chainlink_at_forecast" in migration
    assert "pending_move / chainlink_at_forecast * 10000" not in migration


def test_multiply_first_avoids_postgres_numeric_intermediate_rounding():
    chainlink = Decimal("77379.730579270000000000")
    pending_move = Decimal("95.619238501526500000")
    stored_bps = Decimal("12.357142857142857143")
    tolerance = Decimal("0.000000000000000010")

    with localcontext() as context:
        context.prec = 80
        # PostgreSQL selects scale 20 for the divide-first intermediate in
        # this case, but scale 18 when the larger numerator is multiplied
        # first. These are the values seen by the two CHECK expressions.
        divide_first = (
            (pending_move / chainlink).quantize(Decimal("1e-20"))
            * Decimal("10000")
        )
        multiply_first = (
            pending_move * Decimal("10000") / chainlink
        ).quantize(Decimal("1e-18"))

    assert abs(stored_bps - divide_first) > tolerance
    assert abs(stored_bps - multiply_first) <= tolerance


def test_projection_constraint_migration_is_guarded_and_definition_specific():
    migration = _projection_constraint_migration()

    semantic_guard = migration.index(f"'{PROJECTION_CONSTRAINT}'")
    old_name_guard = migration.index(f"'{DEPLOYED_AUTO_CONSTRAINT}'")
    definition_guard = migration.index(
        "pg_get_constraintdef(oid) LIKE '%pending_move_bps%'"
    )
    drop_old = migration.index(
        f"DROP CONSTRAINT {DEPLOYED_AUTO_CONSTRAINT}"
    )
    add_semantic = migration.index(f"ADD CONSTRAINT\n                {PROJECTION_CONSTRAINT}")

    assert semantic_guard < old_name_guard < definition_guard < drop_old
    assert drop_old < add_semantic
    assert migration.count(f"DROP CONSTRAINT {DEPLOYED_AUTO_CONSTRAINT}") == 1
    assert migration.count(f"ADD CONSTRAINT\n                {PROJECTION_CONSTRAINT}") == 1
