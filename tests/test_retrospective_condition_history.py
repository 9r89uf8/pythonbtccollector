from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal
import hashlib

import pytest

from price_collector.ghost_twap import GhostPolicy, _json_bytes
from price_collector.settlement_history import (
    COMBINED_CELL_ENCODING, COMBINED_SELECTION_VERSION, DAY_MS, LEGACY_CONDITION_MODEL,
    MARKET_CONDITION_TIME_BUCKETS, RETROSPECTIVE_COHORT_PREFIX, cohort_key,
    combined_condition_summary, daily_summary, freeze_daily_unknowns, history_summary,
    market_condition_summary, observe, retrospective_cohort, retrospective_condition_daily_summary,
)
from test_market_condition_history import START, condition, market

POLICY = asdict(GhostPolicy(enabled=True))


def legacy(identifier="old", *, window=30, start=START, twap="100.01", spot="99.98",
           remaining_ms=29_000, winner="up", eligible=True):
    projection = condition(identifier, start=start, twap=twap, spot=spot, remaining_ms=remaining_ms)
    projection.update(schema_version=2, kind="settlement", model_version=LEGACY_CONDITION_MODEL,
                      rule_version="historical-settlement-v1", policy=deepcopy(POLICY))
    projection.pop("observation_window_s")
    projection.pop("sampling_interval_ms")
    if window == 60:
        projection.update(schema_version=3, rule_version="historical-settlement-v2",
                          observation_window_s=60, sampling_interval_ms=2000)
    projection["signals"]["ghost"] = {"price": "100.50"}
    state = {"publication": {"ack_wall_ns": projection["decision_wall_ns"]}}
    body = observe({}, projection, state, eligible=eligible)
    if winner is not None:
        body["official_outcome"] = dict(condition_id="c", up_token_id="u", down_token_id="d", winner=winner)
    return body


def source_day(markets, *, final=True):
    return daily_summary(markets, day_ms=START, now_ms=START + 2 * DAY_MS, final=final)


def retrospective(markets, *, source=None, final=True):
    return retrospective_condition_daily_summary(markets, source_daily=source or source_day(markets, final=final),
                                               day_ms=START, now_ms=START + 3 * DAY_MS)


def combined(days, *, policy=POLICY, current=None, now_ms=START + 3 * DAY_MS):
    return combined_condition_summary(days, now_ms, complete=True, cohort_id=current, policy=policy)


def test_derived_identity_matches_sql_prefix_and_original_metadata_is_preserved():
    body = legacy()
    original = deepcopy(body)
    source = source_day([body])
    before = deepcopy(source)
    result = retrospective([body], source=source)
    expected = hashlib.sha256((RETROSPECTIVE_COHORT_PREFIX + body["cohort"]).encode()).hexdigest()
    assert result["cohort"] == expected == retrospective_cohort(body["cohort"])
    assert result["cohort"] != body["cohort"] and body == original and source == before
    description = result["description"]
    assert description["schema_version"] == 1 and description["kind"] == "retrospective_market_conditions"
    assert description["source_cohort"] == body["cohort"]
    assert description["source_description"] == body["description"]
    assert description["model_version"] == LEGACY_CONDITION_MODEL
    assert description["selection_version"] == "first-ack-5s-v1"
    assert description["observation_window_s"] == 30 and "sampling_interval_ms" not in description
    assert result["outcome_freeze_ms"] == source["outcome_freeze_ms"] != result["generated_at_ms"]
    assert result["source_final"] and result["source_outcome_freeze_ms"] == source["outcome_freeze_ms"]
    assert result["source_generated_at_ms"] == source["generated_at_ms"]
    assert result["reconstruction_complete"] and result["retained_markets"] == 1
    assert len(result["coverage"]) == 6


def test_original_ack_clock_upper_edges_and_windows_are_not_expanded():
    old = legacy(window=30, remaining_ms=30_000)
    minute = legacy(window=60, remaining_ms=60_000)
    assert set(old["buckets"]) == {"25-30"} and set(minute["buckets"]) == {"55-60"}
    first, second = retrospective([old]), retrospective([minute])
    assert first["cells"][0][0] == 5 and second["cells"][0][0] == 11
    assert len(first["coverage"]) == 6 and len(second["coverage"]) == 12
    assert first["cohort"] != second["cohort"]
    assert second["description"]["selection_version"] == "first-ack-5s-v2"
    assert second["description"]["sampling_interval_ms"] == 2000
    bad = deepcopy(old)
    bad["buckets"]["30-35"] = bad["buckets"]["25-30"]
    with pytest.raises(ValueError, match="outside historical cohort window"):
        retrospective([bad])


def test_joint_pairs_use_saved_twaps_spot_alignment_and_original_official_outcomes():
    bodies = [legacy("up", twap="100.01", spot="100.02", winner="up"),
              legacy("down", start=START + 300_000, twap="99.99", spot="99.98", winner="down"),
              legacy("opposes", start=START + 600_000, twap="100.01", spot="99.98", winner="down"),
              legacy("spot-tie", start=START + 900_000, twap="100.01", spot="100", winner="up"),
              legacy("twap-tie", start=START + 1_200_000, twap="100", spot="100.02", winner="up")]
    result = combined([retrospective(bodies)])
    cells = {row[4]: row for row in result["cells"]}
    assert len(cells) == 3
    assert cells[0][5:7] == [2, 0]  # Both up and down leaders won with spot agreement.
    assert cells[1][5:7] == [0, 1]  # The original TWAP leader lost despite the ghost price.
    assert cells[2][3] == 0 and cells[2][5:7] == [1, 0]
    coverage = next(item for item in result["coverage"] if item["time_bucket"] == "25-30")
    assert coverage["selected_markets"] == 5 and coverage["classified_markets"] == 4
    assert coverage["ties"] == {"twap": 1, "spot": 1}


def test_missing_outcomes_and_identity_mismatch_stay_unknown_at_original_cutoff():
    known = legacy()
    unknown = legacy("pending", start=START + 300_000, winner=None)
    mismatch = legacy("wrong", start=START + 600_000)
    mismatch["official_outcome"]["condition_id"] = "different-market"
    cells = combined([retrospective([known, unknown, mismatch])])["cells"]
    assert len(cells) == 1 and cells[0][5:10] == [1, 0, 2, 0, 2]
    mutable = retrospective([unknown], final=False)
    assert not mutable["final"] and mutable["outcome_freeze_ms"] is None
    assert combined([mutable])["cells"][0][7:10] == [1, 1, 0]
    frozen = freeze_daily_unknowns(mutable)
    assert combined([frozen])["cells"][0][7:10] == [1, 0, 1]


def test_partially_expired_source_day_preserves_coverage_denominator_and_missingness():
    bodies = [legacy(str(i), start=START + i * 300_000) for i in range(3)]
    source = source_day(bodies)
    result = retrospective(bodies[1:], source=source)
    assert result["covered_start_ms"] == START and result["covered_end_ms"] == START + 900_000
    assert result["observed_markets"] == 3 and result["retained_markets"] == 2
    assert result["unavailable_retained_markets"] == 1
    assert result["reconstruction_complete"] is False and result["persistence_complete"] is False
    table = combined([result])
    cohort, = table["cohorts"]
    assert cohort["unavailable_retained_markets"] == 1 and cohort["incomplete_frozen_days"] == 1
    assert cohort["incomplete_reconstruction_days"] == 1
    coverage = next(item for item in table["coverage"] if item["time_bucket"] == "25-30")
    assert coverage["observed_markets"] == 3 and coverage["selected_markets"] == 2
    assert coverage["no_observation_markets"] == 0 and coverage["no_eligible_observation_markets"] == 0
    assert coverage["unavailable_retained_markets"] == 1


def test_legacy_marginal_counts_are_never_used_to_invent_joint_cells():
    body = legacy()
    source = source_day([body])
    assert combined([source])["cells"] == []
    with pytest.raises(ValueError, match="retained population"):
        retrospective([], source=source)
    reconstructed = retrospective([body], source=source)
    assert history_summary([source, reconstructed], START + 3 * DAY_MS, complete=True) == history_summary(
        [source], START + 3 * DAY_MS, complete=True)
    assert market_condition_summary([reconstructed], START + 3 * DAY_MS, complete=True)["cells"] == []


def test_methods_remain_distinct_and_percentages_still_need_thirty_resolved_markets():
    first = [legacy(str(i), start=START + i * 300_000, winner="up" if i < 24 else "down") for i in range(30)]
    second = [legacy(str(i), window=60, start=START + i * 300_000, winner="up") for i in range(29)]
    current_projection = condition(remaining_ms=29_000, spot="99.98")
    current_projection["policy"] = deepcopy(POLICY)
    current_day = source_day([market(current_projection, winner="down")])
    table = combined([retrospective(first), retrospective(second), current_day], current=current_day["cohort"])
    assert table["schema_version"] == 3 and table["selection_version"] == COMBINED_SELECTION_VERSION
    assert table["cell_encoding"] == COMBINED_CELL_ENCODING and len(table["cohorts"]) == 3
    assert len(table["cells"]) == 3 and all(len(cell) == 12 for cell in table["cells"])
    by_window = {table["cohorts"][row[0]]["observation_window_s"]: row for row in table["cells"]}
    assert by_window[30][5:7] == [24, 6] and by_window[30][10] == "80.00"
    assert by_window[60][5:7] == [29, 0] and by_window[60][10:] == [None, None]
    assert by_window[300][5:7] == [0, 1] and by_window[300][10:] == [None, None]


@pytest.mark.parametrize("fault", ["source_hash", "source_description", "duplicate_market", "outside_day", "changed_model"])
def test_reconstruction_rejects_mixed_or_unverifiable_source_identity(fault):
    body = legacy()
    source = source_day([body])
    bodies = [body]
    if fault == "source_hash": source["cohort"] = "a" * 64
    elif fault == "source_description": body["description"]["policy"]["source_max_age_ms"] = 4000
    elif fault == "duplicate_market": bodies.append(deepcopy(body)); source["observed_markets"] = 2; source["covered_end_ms"] += 300_000
    elif fault == "outside_day": body["market_start_ms"] -= DAY_MS
    else: source["description"]["model_version"] = "unknown-model"
    with pytest.raises(ValueError):
        retrospective(bodies, source=source)


def test_different_policy_cohort_is_not_borrowed_and_expired_days_are_omitted():
    day = retrospective([legacy()])
    assert combined([day], policy=dict(POLICY, source_max_age_ms=4000))["cells"] == []
    assert combined([day], now_ms=START + 90 * DAY_MS)["cells"] == []


def test_all_three_complete_cohorts_fit_existing_daily_and_cache_byte_caps():
    examples = (Decimal("0.5"), Decimal("1.5"), Decimal("3"), Decimal("6"), Decimal("10"))
    cases = [(twap, spot * direction) for twap in examples for spot in examples for direction in (1, -1)]
    cases += [(twap, Decimal(0)) for twap in examples]
    days = []
    current = None
    for window in (30, 60, 300):
        bodies = []
        buckets = MARKET_CONDITION_TIME_BUCKETS if window == 300 else MARKET_CONDITION_TIME_BUCKETS[:window // 5]
        for index, (twap, spot) in enumerate(cases * 5):
            kwargs = dict(start=START + index * 300_000, twap=str(100 + twap / 100), spot=str(100 + spot / 100))
            winner = "up" if index // 55 < 3 else "down"
            if window == 300:
                projection = condition(str(index), **kwargs)
                projection["policy"] = deepcopy(POLICY)
                body = market(projection, winner=winner)
            else:
                body = legacy(str(index), window=window, winner=winner, **kwargs)
            observation, = body["buckets"].values()
            body["buckets"] = {bucket: deepcopy(observation) for bucket in buckets}
            bodies.append(body)
        day = source_day(bodies) if window == 300 else retrospective(bodies)
        assert len(_json_bytes(day)) < 65536
        if window == 300:
            current = day["cohort"]
        days += [dict(day, day_ms=START + i * DAY_MS, covered_start_ms=START + i * DAY_MS,
                      covered_end_ms=START + i * DAY_MS + 275 * 300_000) for i in range(90)]
    result = combined(days, current=current, now_ms=START + 90 * DAY_MS - 1)
    assert len(result["cells"]) == 1540 + 330 + 660 and len(result["cohorts"]) == 3
    assert len(result["coverage"]) == 28 + 6 + 12
    assert sum(row[5] + row[6] for row in result["cells"]) == (28 + 6 + 12) * 275 * 90
    assert len(_json_bytes(result)) + 16_384 < 512 * 1024
