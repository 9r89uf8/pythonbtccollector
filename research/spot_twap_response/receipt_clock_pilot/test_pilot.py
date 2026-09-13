"""Synthetic checks for receipt causality and the declared projection arithmetic."""

from decimal import Decimal, localcontext

import pytest

from research.spot_twap_response.receipt_clock_pilot.pilot import (
    assess_row,
    opening_reference,
    project_slots,
    select_current,
)


E = 1_200_000
D = E - 30_000
FIRST_SLOT = E - 62_000


def spot(source_ms, received_ms, price="100"):
    return {"source_ms": source_ms, "received_ms": received_ms, "price": price}


def twap(source_ms, received_ns, price="100"):
    return {
        "source_ms": source_ms,
        "received_wall_ns": received_ns,
        "price": price,
    }


def current(price="100"):
    return select_current([spot(D, D, price)], D, kind="spot")


def assert_slot_accounting(result):
    assert result["historical_requested_slots"] + result["future_requested_slots"] == 60
    assert (
        result["exact_observed_slots"]
        + result["carried_slots"]
        + result["historical_missing_slots"]
        == result["historical_requested_slots"]
    )
    assert (
        result["future_extrapolated_slots"] + result["future_missing_slots"]
        == result["future_requested_slots"]
    )


def test_current_uses_receipt_order_not_source_order():
    result = select_current(
        [spot(D - 1_000, D - 100, "101"), spot(D - 2_000, D - 1, "99")],
        D,
        kind="spot",
    )
    assert result["source_ms"] == D - 2_000
    assert result["price"] == Decimal("99")
    assert result["available"]


@pytest.mark.parametrize("selected_source", [D + 1_000, D - 600_000, D - 600_001])
def test_current_does_not_reveal_older_fresh_record(selected_source):
    result = select_current(
        [spot(D - 1_000, D - 100, "100"), spot(selected_source, D - 1, "200")],
        D,
        kind="spot",
    )
    assert result["source_ms"] == selected_source
    assert not result["available"]
    assert not result["fresh_3s"]


def test_current_twap_receipt_cutoff_preserves_nanoseconds():
    result = select_current(
        [twap(D, D * 1_000_000, "100"), twap(D, D * 1_000_000 + 1, "200")],
        D,
        kind="twap",
    )
    assert result["price"] == Decimal("100")
    assert result["received_ns"] == D * 1_000_000


@pytest.mark.parametrize("kind", ["spot", "twap"])
def test_current_ten_minute_receipt_lower_bound_is_strict(kind):
    source = D - 599_999
    if kind == "spot":
        event = spot(source, D - 600_000)
    else:
        event = twap(source, (D - 600_000) * 1_000_000)
    result = select_current([event], D, kind=kind)
    assert not result["available"]
    assert result["variants"] == 0


def test_current_receipt_one_nanosecond_inside_primary_bound_is_available():
    result = select_current(
        [twap(D - 599_999, (D - 600_000) * 1_000_000 + 1)],
        D,
        kind="twap",
    )
    assert result["available"]
    assert not result["fresh_3s"]


@pytest.mark.parametrize(
    "source_age, receipt_age_ns, fresh",
    [(3_000, 3_000_000_000, True), (3_001, 0, False), (3_000, 3_000_000_001, False)],
)
def test_freshness_boundaries_are_inclusive_and_independent(source_age, receipt_age_ns, fresh):
    result = select_current(
        [twap(D - source_age, D * 1_000_000 - receipt_age_ns)],
        D,
        kind="twap",
    )
    assert result["available"]
    assert result["fresh_3s"] is fresh


def test_numeric_duplicate_receipts_do_not_create_a_conflict():
    result = select_current(
        [spot(D - 1_000, D, "100.0"), spot(D - 1_000, D, "100.00")],
        D,
        kind="spot",
    )
    assert result["tied_rows"] == 2
    assert result["variants"] == 1
    assert result["available"]


@pytest.mark.parametrize(
    "other",
    [spot(D - 1_000, D, "101"), spot(D - 2_000, D, "100")],
)
def test_equal_receipt_conflicting_state_is_unavailable(other):
    result = select_current([spot(D - 1_000, D), other], D, kind="spot")
    assert result["variants"] == 2
    assert not result["available"]


def test_opening_requires_exact_source_boundary():
    start = E - 300_000
    result = opening_reference(
        [twap(start + 1, (D - 1) * 1_000_000, "200")], start, D
    )
    assert not result["available"]
    assert result["variants"] == 0


def test_opening_numeric_duplicates_keep_earliest_available_receipt():
    start = E - 300_000
    events = [
        twap(start, (D - 5) * 1_000_000, "100.0"),
        twap(start, (D - 1) * 1_000_000, "100.00"),
        twap(start + 1, D * 1_000_000, "999"),
    ]
    result = opening_reference(events, start, D)
    assert result["available"]
    assert result["price"] == Decimal("100")
    assert result["variants"] == 1
    assert result["event_count"] == 2
    assert result["first_received_ns"] == (D - 5) * 1_000_000


def test_later_opening_conflict_cannot_change_an_earlier_cutoff():
    start = E - 300_000
    events = [
        twap(start, D * 1_000_000, "100"),
        twap(start, D * 1_000_000 + 1, "101"),
    ]
    earlier = opening_reference(events, start, D)
    later = opening_reference(events, start, D + 1)
    assert earlier["available"]
    assert earlier["price"] == Decimal("100")
    assert later["variants"] == 2
    assert not later["available"]


@pytest.mark.parametrize(
    "seconds_left, observed, carried, future",
    [(60, 1, 2, 57), (30, 31, 2, 27), (15, 46, 2, 12), (10, 51, 2, 7), (5, 56, 2, 2), (3, 58, 2, 0)],
)
def test_regular_two_second_delivery_lag_changes_slot_classes(
    seconds_left, observed, carried, future
):
    decision = E - seconds_left * 1_000
    events = [spot(u, u + 2_000) for u in range(FIRST_SLOT, E - 2_000, 1_000)]
    state = select_current(events, decision, kind="spot")
    result = project_slots(events, E, decision, state)
    assert result["exact_observed_slots"] == observed
    assert result["carried_slots"] == carried
    assert result["future_extrapolated_slots"] == future
    assert result["historical_missing_slots"] == 0
    assert result["future_missing_slots"] == 0
    assert result["projected_price"] == Decimal("100")
    assert result["projection_available"]
    assert_slot_accounting(result)


def test_historical_path_uses_source_order_after_receipt_eligibility():
    events = [
        spot(FIRST_SLOT, D - 100, "101"),
        spot(FIRST_SLOT - 1_000, D - 1, "100"),
    ]
    state = select_current(events, D, kind="spot")
    assert state["price"] == Decimal("100")
    result = project_slots(events, E, D, state)
    assert result["history_sum"] == Decimal("3333")
    assert result["projected_price"] == Decimal("100.55")
    assert result["exact_observed_slots"] == 1
    assert result["carried_slots"] == 32
    assert result["max_carry_age_ms"] == 32_000
    assert_slot_accounting(result)


def test_historical_arrival_after_its_slot_but_before_decision_is_usable():
    events = [spot(FIRST_SLOT, D, "101")]
    result = project_slots(events, E, D, select_current(events, D, kind="spot"))
    assert result["exact_observed_slots"] == 1
    assert result["history_sum"] == Decimal("3333")
    assert result["projected_price"] == Decimal("101")


def test_post_decision_revision_cannot_fill_historical_slot():
    events = [spot(FIRST_SLOT - 1_000, D - 1), spot(FIRST_SLOT, D + 1, "999")]
    result = project_slots(events, E, D, select_current(events, D, kind="spot"))
    assert result["exact_observed_slots"] == 0
    assert result["carried_slots"] == 33
    assert result["projected_price"] == Decimal("100")


@pytest.mark.parametrize("seed_age, carried, missing", [(599_000, 1, 31), (600_000, 0, 32)])
def test_seed_bound_is_per_slot_and_strict(seed_age, carried, missing):
    seed = spot(FIRST_SLOT - seed_age, FIRST_SLOT - seed_age + 1_000)
    assert seed["source_ms"] < D - 600_000
    events = [seed, spot(D, D)]
    result = project_slots(events, E, D, select_current(events, D, kind="spot"))
    assert result["exact_observed_slots"] == 1
    assert result["carried_slots"] == carried
    assert result["historical_missing_slots"] == missing
    assert not result["projection_available"]
    assert result["projected_price"] is None
    assert_slot_accounting(result)


def test_current_spot_is_not_a_substitute_for_missing_past_seed():
    events = [spot(D, D, "123")]
    result = project_slots(events, E, D, select_current(events, D, kind="spot"))
    assert result["historical_missing_slots"] == 32
    assert result["exact_observed_slots"] == 1
    assert result["future_extrapolated_slots"] == 27
    assert not result["projection_available"]
    assert_slot_accounting(result)


def test_missing_current_spot_leaves_future_slots_unavailable():
    events = [spot(u, u) for u in range(FIRST_SLOT, D + 1_000, 1_000)]
    events.append(spot(D + 1_000, D, "999"))
    state = select_current(events, D, kind="spot")
    assert not state["available"]
    result = project_slots(events, E, D, state)
    assert result["exact_observed_slots"] == 33
    assert result["future_missing_slots"] == 27
    assert not result["projection_available"]
    assert_slot_accounting(result)


def test_no_current_spot_is_needed_when_all_sixty_historical_slots_are_observed():
    decision = E - 3_000
    events = [spot(u, u) for u in range(FIRST_SLOT, E - 2_000, 1_000)]
    events.append(spot(decision + 1_000, decision, "999"))
    state = select_current(events, decision, kind="spot")
    assert not state["available"]
    result = project_slots(events, E, decision, state)
    assert result["future_requested_slots"] == 0
    assert result["projection_available"]
    assert result["projected_price"] == Decimal("100")


def test_projection_preserves_full_numeric_price_precision():
    price = "12345678901234567890.123456789012345678"
    events = [spot(u, u, price) for u in range(FIRST_SLOT, D + 1_000, 1_000)]
    with localcontext() as context:
        context.prec = 60
        expected_history_sum = Decimal(price) * 33
    result = project_slots(events, E, D, current(price))
    assert isinstance(result["history_sum"], Decimal)
    assert isinstance(result["projected_price"], Decimal)
    assert result["history_sum"] == expected_history_sum
    assert result["projected_price"] == Decimal(price)


@pytest.mark.parametrize("price", ["0", "-1", "NaN", "Infinity"])
def test_invalid_price_in_latest_current_record_is_not_silently_skipped(price):
    with pytest.raises(ValueError):
        select_current([spot(D - 1_000, D - 100), spot(D, D, price)], D, kind="spot")


def aggregate_row(**changes):
    row = {
        "market_id": "3",
        "start_ms": str(E - 300_000),
        "end_ms": str(E),
        "t_sec": "30",
        "cut_ms": str(D),
        "snapshot_ms": str(E + 300_000),
        "rule_valid": "t",
        "k_variants": "1",
        "k_event_count": "1",
        "k_first_received_ns": str((E - 298_000) * 1_000_000),
        "k_value": "100",
        "s_variants": "1",
        "s_tied_rows": "1",
        "s_source_ms": str(D - 2_000),
        "s_received_ms": str(D),
        "s_source_key_errors": "0",
        "s_value": "101",
        "w_variants": "1",
        "w_tied_rows": "1",
        "w_source_ms": str(D - 2_000),
        "w_received_ns": str(D * 1_000_000),
        "w_value": "99",
        "history_requested_slots": "33",
        "history_exact_slots": "31",
        "history_carried_slots": "2",
        "history_missing_slots": "0",
        "history_source_key_errors": "0",
        "history_max_carry_age_ms": "2000",
        "history_max_receive_age_ms": "32000",
        "history_min_source_ms": str(FIRST_SLOT),
        "history_max_source_ms": str(D - 2_000),
        "history_latest_received_ms": str(D),
        "history_sum": "3300",
        "official_k_audit_only": "105",
        "official_final_audit_only": "106",
        "official_winner": "Up",
    }
    row.update(changes)
    return row


def test_grading_uses_causal_opening_price_even_when_official_open_differs():
    result = assess_row(aggregate_row())
    assert result["primary_paired_available"]
    assert result["official_k_minus_stream_k"] == Decimal("5")
    assert result["projected_price"] == Decimal("100.45")
    assert result["projected_prediction"] == "Up"
    assert result["projected_correct"] is True
    assert result["twap_correct"] is False


def test_official_winner_takes_precedence_over_price_derived_outcome_for_grading():
    result = assess_row(aggregate_row(official_winner="Down"))
    assert result["official_price_rule_winner_mismatch"]
    assert result["projected_correct"] is False
    assert result["twap_correct"] is True


def test_unknown_outcome_does_not_erase_available_predictions_or_become_a_win():
    result = assess_row(aggregate_row(official_winner=""))
    assert result["primary_paired_available"]
    assert result["projected_prediction"] == "Up"
    assert result["projected_correct"] is None
    assert result["spot_correct"] is None
    assert result["twap_correct"] is None


def test_winner_only_grade_survives_missing_official_final_price():
    result = assess_row(aggregate_row(official_final_audit_only=""))
    assert result["projected_correct"] is True
    assert result["projected_absolute_final_error_bps_audit"] is None


def test_conflicting_opening_reference_cannot_fall_back_to_reconciled_open():
    result = assess_row(aggregate_row(k_variants="2", k_event_count="2"))
    assert not result["k_available"]
    assert not result["primary_paired_available"]
    assert result["projected_prediction"] == ""
    assert result["projected_correct"] is None


def test_equal_predictions_are_counted_as_ties_and_use_the_declared_up_rule():
    result = assess_row(aggregate_row(s_value="100", w_value="100"))
    for prefix in ("projected", "spot", "twap"):
        assert result[f"{prefix}_tie"]
        assert result[f"{prefix}_prediction"] == "Up"
        assert result[f"{prefix}_correct"] is True


def test_three_second_panel_is_separate_from_primary_availability():
    result = assess_row(aggregate_row(s_source_ms=str(D - 3_001)))
    assert result["primary_paired_available"]
    assert not result["fresh_3s_paired_available"]


def test_selected_future_source_in_aggregate_cannot_supply_projection_future():
    result = assess_row(aggregate_row(s_source_ms=str(D + 1)))
    assert result["s_future_source"]
    assert not result["s_available"]
    assert result["future_missing_slots"] == 27
    assert not result["projection_available"]
    assert not result["primary_paired_available"]


def test_inconsistent_slot_accounting_is_rejected():
    with pytest.raises(ValueError, match="Slot accounting"):
        assess_row(aggregate_row(history_carried_slots="1"))


def test_aggregate_opening_receipt_one_nanosecond_after_cutoff_is_rejected():
    with pytest.raises(ValueError, match="Opening reference provenance"):
        assess_row(aggregate_row(k_first_received_ns=str(D * 1_000_000 + 1)))


def test_aggregate_opening_receipt_exactly_at_cutoff_is_allowed():
    result = assess_row(aggregate_row(k_first_received_ns=str(D * 1_000_000)))
    assert result["k_available"]


@pytest.mark.parametrize(
    "changes",
    [
        {"history_latest_received_ms": str(D + 1)},
        {"history_max_source_ms": str(D + 1)},
        {"history_min_source_ms": str(FIRST_SLOT - 600_000)},
        {"history_max_carry_age_ms": "600000"},
        {"history_max_carry_age_ms": "-1"},
        {"history_max_receive_age_ms": "-1"},
    ],
)
def test_aggregate_historical_clock_eligibility_is_checked(changes):
    with pytest.raises(ValueError, match="Historical source/receipt eligibility"):
        assess_row(aggregate_row(**changes))


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "0"])
def test_present_historical_prices_require_a_positive_finite_sum(value):
    with pytest.raises(ValueError, match="Historical value/clock provenance"):
        assess_row(aggregate_row(history_sum=value))


def test_present_historical_prices_require_receipt_provenance():
    with pytest.raises(ValueError, match="Historical value/clock provenance"):
        assess_row(aggregate_row(history_latest_received_ms=""))


def empty_history_row(**changes):
    empty = {
        "history_exact_slots": "0",
        "history_carried_slots": "0",
        "history_missing_slots": "33",
        "history_sum": "0",
        "history_min_source_ms": "",
        "history_max_source_ms": "",
        "history_latest_received_ms": "",
        "history_max_carry_age_ms": "",
        "history_max_receive_age_ms": "",
    }
    empty.update(changes)
    return aggregate_row(**empty)


def test_empty_historical_path_remains_a_missing_forecast_not_a_zero_price():
    result = assess_row(empty_history_row())
    assert result["historical_missing_slots"] == 33
    assert result["projected_price"] is None
    assert not result["projection_available"]
    assert result["twap_prediction"] == "Down"


@pytest.mark.parametrize(
    "changes", [{"history_sum": "1"}, {"history_latest_received_ms": str(D)}]
)
def test_empty_historical_path_cannot_claim_a_sum_or_input_receipt(changes):
    with pytest.raises(ValueError, match="Historical value/clock provenance"):
        assess_row(empty_history_row(**changes))
