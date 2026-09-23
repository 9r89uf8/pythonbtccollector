from copy import deepcopy
from decimal import Decimal

import pytest

from price_collector.settlement_history import (
    DAY_MS, MARKET_CONDITION_CELL_ENCODING, MARKET_CONDITION_TIME_BUCKETS, cohort_key, daily_summary,
    freeze_daily_unknowns,
    history_summary, market_condition_summary, observation_window_s, observe, time_bucket,
)

START = 20_000 * DAY_MS


def condition(identifier="d1", *, start=START, remaining_ms=290_000, twap="100.01", spot="100.02"):
    end = start + 300_000
    return dict(schema_version=4, kind="market_conditions", rule_version="historical-market-conditions-v1",
                model_version="market-conditions-v1", observation_window_s=300, sampling_interval_ms=5000,
                policy={"source_max_age_ms": 5000}, run_id="run", decision_id=identifier,
                market_id=start // 300_000, market_start_ms=start, market_end_ms=end,
                decision_wall_ns=str((end - remaining_ms) * 1_000_000),
                reference=dict(price_to_beat="100", condition_id="c", up_token_id="u", down_token_id="d"),
                signals={"twap": {"price": twap}, "spot": {"price": spot}})


def market(projection=None, *, eligible=True, winner=None):
    body = observe({}, projection or condition(), {}, eligible=eligible)
    if winner is not None:
        body["official_outcome"] = dict(condition_id="c", up_token_id="u", down_token_id="d", winner=winner)
    return body


def summary(markets, *, final=False, now_ms=START + DAY_MS):
    day = daily_summary(markets, day_ms=START, now_ms=now_ms, final=final)
    return market_condition_summary([day], now_ms, complete=True)


def test_full_market_time_buckets_keep_exact_edges_and_close_excluded():
    assert len(MARKET_CONDITION_TIME_BUCKETS) == 28
    boundaries = {
        0: None, 1: "0-5", 4_999_999_999: "0-5", 5_000_000_000: "5-10",
        59_999_999_999: "55-60", 60_000_000_000: "60-75", 74_999_999_999: "60-75",
        75_000_000_000: "75-90", 299_999_999_999: "285-300", 300_000_000_000: "285-300",
        300_000_000_001: None,
    }
    assert {stamp: time_bucket(stamp, window_s=300) for stamp in boundaries} == boundaries
    assert time_bucket(60_000_000_000, window_s=60) == "55-60"
    assert time_bucket(60_000_000_001, window_s=60) is None


@pytest.mark.parametrize("changes", [dict(schema_version=3), dict(kind="settlement"),
    dict(rule_version="historical-settlement-v2"), dict(model_version="other-model"),
    dict(observation_window_s=60), dict(observation_window_s=True), dict(sampling_interval_ms=2000)])
def test_full_market_contract_is_explicit(changes):
    with pytest.raises(ValueError, match="observation window"):
        observation_window_s(dict(condition(), **changes))


def test_missing_full_market_window_does_not_fall_back_to_legacy():
    projection = condition()
    del projection["observation_window_s"]
    with pytest.raises(ValueError, match="observation window"):
        observation_window_s(projection)


def test_conditions_use_first_decision_clock_without_publication_or_price_selection():
    later = condition("later", remaining_ms=285_100, twap="101", spot="99")
    earlier = condition("earlier", remaining_ms=290_000, twap="100", spot="100.02")
    body = market(later)
    body = observe(body, earlier, {}, eligible=True)
    assert body["buckets"]["285-300"]["order"][2] == "earlier"
    assert body["buckets"]["285-300"]["signals"]["twap"]["side"] == "tie"
    assert observe(body, later, {}, eligible=True) == body
    assert observe(body, earlier, {}, eligible=True) == body
    # A much later ACK cannot move this observation into a later time bucket.
    publication = {"publication": {"ack_wall_ns": str((START + 280_000) * 1_000_000)}}
    assert observe({}, earlier, publication, eligible=True) == body
    tied = summary([body])
    assert tied["cells"] == []
    coverage = next(row for row in tied["coverage"] if row["time_bucket"] == "285-300")
    assert coverage["selected_markets"] == 1 and coverage["classified_markets"] == 0
    assert coverage["ties"] == {"twap": 1, "spot": 0}


def test_combined_cells_pool_leading_sides_and_keep_spot_alignment_distinct():
    inputs = [
        ("100.01", "100.02", "up"), ("99.99", "99.98", "down"),
        ("100.01", "99.98", "down"), ("99.99", "100.02", "up"),
        ("100.01", "100", "up"),
    ]
    bodies = [market(condition(str(index), start=START + index * 300_000, twap=twap, spot=spot), winner=winner)
              for index, (twap, spot, winner) in enumerate(inputs)]
    result = summary(bodies)
    assert len(result["cells"]) == 3
    cells = {cell["spot_alignment"]: cell for cell in result["cells"]}
    assert cells["agrees"]["wins"] == 2 and cells["agrees"]["losses"] == 0
    assert cells["opposes"]["losses"] == 2 and cells["opposes"]["wins"] == 0
    assert cells["tie"]["wins"] == 1 and cells["tie"]["spot_margin_bucket"] == "0-1"
    assert all(cell["signal"] == "combined" and cell["twap_margin_bucket"] == "1-2" for cell in cells.values())
    assert cells["agrees"]["spot_margin_bucket"] == "2-4"
    assert all(cell["win_rate_pct"] is None for cell in cells.values())


def test_unknowns_identity_mismatches_missing_observations_and_ties_are_separate():
    known = market(winner="up")
    unknown = market(condition("unknown", start=START + 300_000))
    mismatch = market(condition("mismatch", start=START + 600_000), winner="up")
    mismatch["official_outcome"]["condition_id"] = "other-market"
    missing = market(condition("missing", start=START + 900_000), eligible=False)
    tied = market(condition("tied", start=START + 1_500_000, twap="100", spot="100"), winner="up")
    result = summary([known, unknown, mismatch, missing, tied])
    cell, = result["cells"]
    assert (cell["wins"], cell["losses"], cell["unknown"], cell["pending"], cell["frozen_unknown"]) == (1, 0, 2, 2, 0)
    coverage = next(row for row in result["coverage"] if row["time_bucket"] == "285-300")
    assert coverage["no_eligible_observation_markets"] == 1
    assert coverage["no_observation_markets"] == 1
    assert coverage["ties"] == {"twap": 1, "spot": 1}
    final = summary([known, unknown, mismatch, missing, tied], final=True)
    assert final["cells"][0]["frozen_unknown"] == 2 and final["cells"][0]["pending"] == 0


def test_percentages_use_resolved_markets_and_only_after_thirty_pairs():
    bodies = [market(condition(str(i), start=START + i * 300_000), winner="down" if i == 0 else "up")
              for i in range(30)]
    assert summary(bodies[:29])["cells"][0]["win_rate_pct"] is None
    bodies.append(market(condition("unknown", start=START + 30 * 300_000)))
    cell, = summary(bodies)["cells"]
    assert cell["resolved"] == 30 and cell["unknown"] == 1 and cell["win_rate_pct"] == "96.67"
    assert Decimal(cell["interval95_pct"][0]) < Decimal("96.67") < Decimal(cell["interval95_pct"][1])


def test_new_summary_ignores_legacy_cohorts_and_preserves_retention():
    projection = condition()
    legacy = dict(projection, schema_version=3, kind="settlement", rule_version="historical-settlement-v2",
                  model_version="legacy-model", observation_window_s=60, sampling_interval_ms=2000)
    legacy["decision_wall_ns"] = str((START + 275_000) * 1_000_000)
    legacy["signals"] = dict(legacy["signals"], ghost={"price": "100.04"})
    old = observe({}, legacy, {"publication": {"ack_wall_ns": legacy["decision_wall_ns"]}}, eligible=True)
    old_day = daily_summary([old], day_ms=START, now_ms=START + DAY_MS, final=True)
    new_day = daily_summary([market(winner="up")], day_ms=START, now_ms=START + DAY_MS, final=True)
    combined = market_condition_summary([old_day, new_day], START + 8 * DAY_MS, complete=True)
    assert combined["schema_version"] == 2 and len(combined["cohorts"]) == 1 and len(combined["cells"]) == 1
    assert combined["cohorts"][0]["id"] != cohort_key(legacy)
    legacy_summary = history_summary([old_day], START + 8 * DAY_MS, complete=True)
    assert legacy_summary["schema_version"] == 1
    assert history_summary([old_day, new_day], START + 8 * DAY_MS, complete=True) == legacy_summary
    assert not market_condition_summary([new_day], START + 90 * DAY_MS, complete=True)["cells"]
    assert market_condition_summary([new_day], START + DAY_MS, complete=False)["status"] == "warming_up"
    with pytest.raises(ValueError, match="duplicate"):
        market_condition_summary([new_day, new_day], START + DAY_MS, complete=True)


def test_policy_cohorts_and_market_identities_cannot_be_combined():
    original = condition()
    changed = dict(original, policy={"source_max_age_ms": 3000})
    assert cohort_key(original) != cohort_key(changed)
    with pytest.raises(ValueError, match="cohort"):
        observe(market(original), changed, {}, eligible=True)
    with pytest.raises(ValueError, match="market"):
        observe(market(original), condition(start=START + 300_000), {}, eligible=True)
    body = market(original)
    with pytest.raises(ValueError, match="duplicate"):
        daily_summary([body, deepcopy(body)], day_ms=START, now_ms=START + DAY_MS, final=False)


def test_open_markets_do_not_enter_historical_counts():
    result = summary([market(winner="up")], now_ms=START + 10_000)
    assert result["cells"] == [] and result["cohorts"] == [] and result["status"] == "warming_up"


def test_daily_unknown_freeze_supports_both_formats_without_mutating_input():
    day = daily_summary([market()], day_ms=START, now_ms=START + DAY_MS, final=False)
    before = deepcopy(day)
    assert day["cell_encoding"] == MARKET_CONDITION_CELL_ENCODING
    frozen = freeze_daily_unknowns(day)
    assert day == before
    cell, = market_condition_summary([frozen], START + DAY_MS, complete=True)["cells"]
    assert (cell["unknown"], cell["pending"], cell["frozen_unknown"]) == (1, 0, 1)
    assert freeze_daily_unknowns(frozen) == frozen
    legacy = {"cells": [{"wins": 8, "losses": 1, "unknown": 3, "pending": 2, "frozen_unknown": 1}]}
    changed = freeze_daily_unknowns(legacy)
    assert changed["cells"][0] == dict(wins=8, losses=1, unknown=3, pending=0, frozen_unknown=3)
    assert legacy["cells"][0]["pending"] == 2


@pytest.mark.parametrize("fault", ["unknown_encoding", "negative_count", "boolean_count", "wrong_count_type",
    "invalid_time", "invalid_twap_margin", "invalid_spot_margin", "invalid_alignment", "impossible_spot_tie",
    "inconsistent_unknowns", "excess_daily_count", "short_row", "duplicate_cell"])
def test_daily_cell_decoder_rejects_corrupt_counts_or_axes(fault):
    day = daily_summary([market()], day_ms=START, now_ms=START + DAY_MS, final=False)
    row = day["cells"][0]
    if fault == "unknown_encoding": day["cell_encoding"] = "other"
    elif fault == "negative_count": row[4] = -1
    elif fault == "boolean_count": row[4] = True
    elif fault == "wrong_count_type": row[4] = "1"
    elif fault == "invalid_time": row[0] = 28
    elif fault == "invalid_twap_margin": row[1] = 5
    elif fault == "invalid_spot_margin": row[2] = 5
    elif fault == "invalid_alignment": row[3] = 3
    elif fault == "impossible_spot_tie": row[2], row[3] = 1, 2
    elif fault == "inconsistent_unknowns": row[6] = 2
    elif fault == "excess_daily_count": row[4] = 288
    elif fault == "short_row": row.pop()
    else: day["cells"].append(list(row))
    with pytest.raises(ValueError, match="market condition"):
        market_condition_summary([day], START + DAY_MS, complete=True)


def test_all_possible_joint_cells_fit_existing_daily_and_live_cache_budgets():
    from dataclasses import asdict
    from price_collector.ghost_twap import GhostPolicy, _json_bytes

    examples = (Decimal("0.5"), Decimal("1.5"), Decimal("3"), Decimal("6"), Decimal("10"))
    combinations = [(twap, spot * direction) for twap in examples for spot in examples for direction in (1, -1)]
    combinations += [(twap, Decimal(0)) for twap in examples]
    assert len(combinations) == 55
    markets = []
    for index, (twap, spot) in enumerate(combinations * 5):
        projection = condition(str(index), start=START + index * 300_000,
                               twap=str(100 + twap / 100), spot=str(100 + spot / 100))
        projection["policy"] = asdict(GhostPolicy(enabled=True))
        body = market(projection, winner="up" if index // 55 < 3 else "down")
        observation, = body["buckets"].values()
        body["buckets"] = {bucket: deepcopy(observation) for bucket in MARKET_CONDITION_TIME_BUCKETS}
        markets.append(body)
    day = daily_summary(markets, day_ms=START, now_ms=START + DAY_MS, final=True)
    assert day["observed_markets"] == 275 and len(day["cells"]) == 1540
    assert len(_json_bytes(day)) < 65536
    days = [dict(day, day_ms=START + i * DAY_MS, covered_start_ms=START + i * DAY_MS,
                 covered_end_ms=START + i * DAY_MS + 275 * 300_000) for i in range(90)]
    active_cohort = day["cohort"]
    other_policy = deepcopy(day)
    other_policy["description"]["policy"]["source_max_age_ms"] = 4000
    other_policy["cohort"] = cohort_key(other_policy["description"])
    days.append(other_policy)
    now = START + 90 * DAY_MS - 1
    history = market_condition_summary(days, now, complete=True, cohort_id=active_cohort)
    assert len(history["cells"]) == 1540 and len(history["cohorts"]) == 1
    assert history["cohorts"][0]["id"] == active_cohort
    assert all(cell["cohort"] == 0 for cell in history["cells"] + history["coverage"])
    assert sum(cell["resolved"] for cell in history["cells"]) == 28 * 275 * 90
    envelope = dict(schema_version=1, status="available", generated_at_ms=now, cache_publish_attempt_ms=now,
                    valid_until_ms=now + 180_000, history=history,
                    runtime=dict(counters={"observations": 1_555_200}, fault=None,
                                 pending_records=84, persistence_pending=0))
    assert len(_json_bytes(envelope)) + 16_384 < 512 * 1024
