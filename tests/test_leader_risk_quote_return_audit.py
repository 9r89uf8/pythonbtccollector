"""Exact quoted-return accounting and exclusion audit on synthetic panels."""

import csv
import json
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from research.leader_risk import market_prices, quote_return_audit, tabulate
from test_leader_risk_market_prices import make_quotes, no_float_values
from test_leader_risk_tabulate import make_export


def make_inputs(*pairs):
    pairs = pairs or (({}, {}),)
    observations = make_export(*(pair[0] for pair in pairs))
    quotes = make_quotes(observations, *(pair[1] for pair in pairs))
    return observations, quotes


def audit(*pairs):
    return quote_return_audit.build_audit(*make_inputs(*pairs))


def select(rows, **labels):
    matches = [row for row in rows if all(row[key] == value for key, value in labels.items())]
    assert len(matches) == 1
    return matches[0]


def band(result, t=120, ask_band="(0,0.90)"):
    return select(result["checkpoint_bands"], t_sec=t, ask_band=ask_band)


def stale_clock(*names, age=3001):
    def edit(row):
        row.update({name: str(int(row["cut_ms"]) - age) for name in names})
    return edit


def test_resolved_only_gross_mean_uses_same_paired_quote_and_outcome_subset():
    result = audit(({}, {"up_bid": "0.19", "up_ask": "0.20"}),
                   ({"official_winner": "Down"}, {"up_bid": "0.59", "up_ask": "0.60"}),
                   ({"official_winner": ""}, {"up_bid": "0.88", "up_ask": "0.89"}),
                   ({"official_winner": "Down"}, {"up_bid": "0.09", "up_ask": "0.10",
                                                   "edit": stale_clock("up_bid_source_ms")}))
    for t in tabulate.CHECKPOINTS:
        row = band(result, t)
        assert (row["N"], row["U"], row["L"], row["n"]) == (3, 1, 1, 2)
        assert row["loss_rate"] == Decimal("0.5")
        assert row["resolved_mean_ask"] == Decimal("0.40")
        assert row["gross_payout_sum"] == Decimal(1)
        assert row["gross_ask_sum"] == Decimal("0.80")
        assert row["gross_pnl_sum"] == Decimal("0.20")
        assert row["gross_pnl_mean"] == Decimal("0.10")
        totals = select(result["checkpoint_totals"], t_sec=t)
        assert totals["N"] == 3 and totals["n"] == 2
        assert totals["gross_pnl_sum"] == Decimal("0.20")
        coverage = select(result["coverage"], t_sec=t)
        assert (coverage["full_N"], coverage["matched_N"], coverage["excluded_N"]) == (4, 3, 1)
        assert (coverage["full_L"], coverage["matched_L"], coverage["excluded_L"]) == (2, 1, 1)
    assert no_float_values(result)


@pytest.mark.parametrize("winner,ask,bid,expected", [
    ("Up", "0.20", "0.19", "0.80"),
    ("Down", "0.60", "0.59", "-0.60"),
    ("Up", "0", "0", "1"),
    ("Down", "0", "0", "0"),
    ("Up", "1", "0.99", "0"),
    ("Down", "1", "0.99", "-1"),
])
def test_outcome_minus_ask_hand_cases_include_negative_and_boundary_values(winner, ask, bid, expected):
    result = audit(({"official_winner": winner}, {"up_bid": bid, "up_ask": ask}))
    row = band(result, ask_band=market_prices.price_band(Decimal(ask)))
    assert row["gross_pnl_mean"] == row["gross_pnl_sum"] == Decimal(expected)
    assert row["gross_ask_sum"] == Decimal(ask)
    assert row["gross_payout_sum"] == (Decimal(1) if winner == "Up" else Decimal(0))
    assert isinstance(row["gross_pnl_mean"], Decimal)


def test_down_leader_gross_return_uses_down_token_ask_and_payout():
    row = band(audit(({"w": "9998", "s": "9997", "official_winner": "Down"},
                      {"up_ask": "NaN", "down_bid": "0.20", "down_ask": "0.25"})))
    assert row["L"] == 0 and row["n"] == 1
    assert row["gross_ask_sum"] == Decimal("0.25")
    assert row["gross_pnl_mean"] == Decimal("0.75")


def test_gross_arithmetic_keeps_eighteen_decimal_places_under_low_ambient_precision():
    with localcontext() as context:
        context.prec = 6
        result = audit(({}, {"up_bid": "0.89", "up_ask": "0.900000000000000001"}))
    row = band(result, ask_band="[0.90,0.95)")
    assert row["gross_ask_sum"] == Decimal("0.900000000000000001")
    assert row["gross_pnl_mean"] == Decimal("0.099999999999999999")


def test_unknown_outcomes_preserve_n_count_but_contribute_no_price_or_return():
    row = band(audit(({"official_winner": ""}, {"up_bid": "0.79", "up_ask": "0.80"})))
    assert (row["N"], row["U"], row["L"], row["n"]) == (1, 1, 0, 0)
    assert row["gross_payout_sum"] == row["gross_ask_sum"] == row["gross_pnl_sum"] == Decimal(0)
    assert row["resolved_mean_ask"] is row["gross_pnl_mean"] is row["loss_rate"] is None


def test_gross_mean_matches_one_minus_resolved_mean_ask_minus_loss_rate():
    result = audit(({}, {"up_bid": "0.09", "up_ask": "0.10"}),
                   ({}, {"up_bid": "0.19", "up_ask": "0.20"}),
                   ({"official_winner": "Down"}, {"up_bid": "0.29", "up_ask": "0.30"}),
                   ({"official_winner": ""}, {"up_bid": "0.88", "up_ask": "0.89"}))
    for row in [*result["checkpoint_totals"], *result["checkpoint_bands"], *result["cell_bands"]]:
        if row["n"]:
            with localcontext() as context:
                context.prec = 60
                expected = 1 - row["resolved_mean_ask"] - Decimal(row["L"]) / row["n"]
                assert abs(row["gross_pnl_mean"] - expected) <= Decimal("1e-55")
                assert abs(row["gross_pnl_mean"] * row["n"] - row["gross_pnl_sum"]) <= Decimal("1e-55")
                screen = 1 - row["resolved_mean_ask"] - row["wilson_95_high"]
                assert abs(row["fixed_mean_ask_wilson_screen_lower"] - screen) <= Decimal("1e-55")


@pytest.mark.parametrize("ask_old,bid_old,expected_status", [(False, False, "matched"),
                                                           (False, True, "ask_only"),
                                                           (True, False, "bid_only"),
                                                           (True, True, "neither_available")])
def test_bid_only_failure_is_distinct_from_both_sides_having_stale_sources(ask_old, bid_old, expected_status):
    names = [f"up_{side}_source_ms" for side, old in (("ask", ask_old), ("bid", bid_old)) if old]
    result = audit(({}, {"edit": stale_clock(*names)}))
    coverage = select(result["coverage"], t_sec=120)
    assert coverage["only_bid_unavailable_with_fresh_ask_N"] == (bid_old and not ask_old)
    assert coverage["only_bid_source_old_with_fresh_ask_N"] == (bid_old and not ask_old)
    assert coverage["both_sources_old_common_fresh_N"] == (ask_old and bid_old)
    assert coverage["matched_N"] == (not ask_old and not bid_old)
    for status in ("matched", "ask_only", "bid_only", "crossed_quote", "neither_available"):
        row = select(result["statuses"], t_sec=120, status=status)
        assert row["N"] == (status == expected_status)


def test_bid_receipt_stale_is_not_misreported_as_bid_source_stale():
    result = audit(({}, {"edit": stale_clock("up_bid_received_ms")}))
    coverage = select(result["coverage"], t_sec=120)
    assert coverage["only_bid_unavailable_with_fresh_ask_N"] == 1
    assert coverage["only_bid_received_stale_with_fresh_ask_N"] == 1
    assert coverage["only_bid_source_old_with_fresh_ask_N"] == 0
    assert coverage["both_sources_old_common_fresh_N"] == 0


@pytest.mark.parametrize("age,matched,old,future", [(3000, 1, 0, 0), (3001, 0, 1, 0), (-1, 0, 0, 1)])
def test_source_diagnostic_separates_inclusive_fresh_boundary_old_and_future(age, matched, old, future):
    result = audit(({}, {"edit": stale_clock("up_bid_source_ms", age=age)}))
    coverage = select(result["coverage"], t_sec=120)
    assert coverage["matched_N"] == matched
    assert coverage["only_bid_source_old_with_fresh_ask_N"] == old
    assert select(result["reasons"], t_sec=120, reason="bid_source_old")["N"] == old
    assert select(result["reasons"], t_sec=120, reason="bid_source_future")["N"] == future


def test_both_sources_old_is_not_attributed_to_a_row_that_also_fails_common_freshness():
    result = audit(({}, {"edit": stale_clock("up_bid_source_ms", "up_ask_source_ms", "quote_sample_ms")}))
    coverage = select(result["coverage"], t_sec=120)
    assert coverage["excluded_N"] == 1
    assert coverage["both_sources_old_common_fresh_N"] == 0
    assert select(result["reasons"], t_sec=120, reason="quote_row_stale")["N"] == 1


def test_crossed_quote_has_its_own_disjoint_exclusion_status_and_no_gross_return():
    result = audit(({"official_winner": "Down"}, {"up_bid": "0.99", "up_ask": "0.98"}))
    assert select(result["statuses"], t_sec=120, status="crossed_quote")["N"] == 1
    assert select(result["coverage"], t_sec=120)["excluded_L"] == 1
    assert select(result["checkpoint_totals"], t_sec=120)["gross_pnl_mean"] is None


def test_overlapping_exclusion_reasons_do_not_create_extra_excluded_observations():
    result = audit(({}, {"edit": stale_clock("up_bid_source_ms", "up_bid_received_ms")}))
    coverage = select(result["coverage"], t_sec=120)
    assert (coverage["full_N"], coverage["matched_N"], coverage["excluded_N"]) == (1, 0, 1)
    assert select(result["reasons"], t_sec=120, reason="bid_source_age")["N"] == 1
    assert select(result["reasons"], t_sec=120, reason="bid_received_age")["N"] == 1
    assert coverage["only_bid_unavailable_with_fresh_ask_N"] == 1
    assert coverage["only_bid_source_old_with_fresh_ask_N"] == 0
    assert sum(row["N"] for row in result["statuses"] if row["t_sec"] == 120) == 1


def test_absent_retained_quote_is_excluded_and_preserves_full_outcome_counts():
    observations, quotes = make_inputs(({"official_winner": "Down"}, {}))
    keep = {"market_id", "t_sec", "cut_ms", "quote_extraction_ms", "requested_cohort_start_ms",
            "cohort_start_ms", "cohort_end_ms", "cohort_market_count"}
    for row in quotes:
        row.update({key: "" for key in row if key not in keep})
    result = quote_return_audit.build_audit(observations, quotes)
    for t in tabulate.CHECKPOINTS:
        coverage = select(result["coverage"], t_sec=t)
        assert (coverage["full_N"], coverage["matched_N"], coverage["excluded_N"], coverage["excluded_L"]) == (1, 0, 1, 1)
        assert select(result["reasons"], t_sec=t, reason="no_quote_row")["N"] == 1


def test_empty_domains_include_all_48_checkpoint_bands_and_960_cell_bands():
    result = audit(({"w": "10000"}, {}))
    assert len(result["checkpoint_bands"]) == 48
    assert len(result["cell_bands"]) == 960
    assert len(result["checkpoint_totals"]) == 8
    assert len(result["statuses"]) == 40
    assert len({(row["t_sec"], row["ask_band"]) for row in result["checkpoint_bands"]}) == 48
    assert len({(row["t_sec"], row["x_bin"], row["y_bin"], row["ask_band"])
                for row in result["cell_bands"]}) == 960
    assert all(row["N"] == row["n"] == 0 and row["gross_pnl_mean"] is None
               for row in [*result["checkpoint_bands"], *result["cell_bands"]])
    assert all(row["N"] == 0 for row in result["statuses"])
    assert sorted(row["groups_considered"] for row in result["screens"]) == [48, 48, 960, 960]
    assert all(row["groups_meeting_minimum"] == 0 for row in result["screens"])


@pytest.mark.parametrize("resolved,unknown,meets_n", [(99, 1, False), (100, 0, True)])
def test_minimum_screen_distinguishes_resolved_n_from_including_unknown_N(resolved, unknown, meets_n):
    specs = [({}, {"up_bid": "0.09", "up_ask": "0.10"}) for _ in range(resolved)]
    specs += [({"official_winner": ""}, {"up_bid": "0.09", "up_ask": "0.10"}) for _ in range(unknown)]
    result = audit(*specs)
    assert len(result["screens"]) == 4
    for screen in result["screens"]:
        assert screen["minimum_count"] == 100
        expected = 8 if screen["minimum_basis"] == "N" or meets_n else 0
        assert screen["groups_meeting_minimum"] == expected
        assert screen["positive_gross_mean_groups"] == expected
        assert screen["positive_fixed_mean_ask_wilson_screen_groups"] == expected


def test_zero_gross_return_is_never_counted_as_a_positive_screen():
    result = audit(*[({}, {"up_bid": "0.99", "up_ask": "1"}) for _ in range(100)])
    for screen in result["screens"]:
        assert screen["groups_meeting_minimum"] == 8
        assert screen["positive_gross_mean_groups"] == 0
        assert screen["positive_fixed_mean_ask_wilson_screen_groups"] == 0


def prepare_previous_pricing_run(tmp_path):
    observations, quotes = make_inputs(({}, {"up_bid": "0.19", "up_ask": "0.20"}),
                                      ({"official_winner": "Down"}, {"up_bid": "0.59", "up_ask": "0.60"}))
    paths = {}
    for name, rows in (("observations", observations), ("quotes", quotes)):
        path = tmp_path / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        paths[name] = path
    sql = tmp_path / "extract_quotes.sql"
    sql.write_bytes(Path(market_prices.__file__).with_name("extract_quotes.sql").read_bytes())
    extraction = {"start_utc": tabulate.utc_text(int(observations[0]["cohort_start_ms"])),
                  "end_utc_exclusive": tabulate.utc_text(int(observations[0]["cohort_end_ms"])),
                  "statement_timeout": "60s", "sql_sha256": tabulate.sha256(sql),
                  "read_only": True, "isolation": "repeatable read", "psql_exit_code": 0}
    extraction_path = tmp_path / "extraction.json"
    extraction_path.write_text(json.dumps(extraction), encoding="utf-8")
    price_output = tmp_path / "prices"
    market_prices.write_results(market_prices.analyze(observations, quotes), price_output,
                                paths["observations"], paths["quotes"], extraction_path, sql)
    paths["price_manifest"] = price_output / "manifest.json"
    paths["output"] = tmp_path / "audit"
    return paths


def test_successful_run_records_input_and_artifact_hashes_and_cannot_overwrite(tmp_path):
    paths = prepare_previous_pricing_run(tmp_path)
    result = quote_return_audit.run_audit(**paths)
    assert len(result["checkpoint_bands"]) == 48 and len(result["cell_bands"]) == 960
    manifest_path = paths["output"] / "manifest.json"
    manifest_before = manifest_path.read_bytes()
    manifest = json.loads(manifest_before)
    assert manifest["observations_sha256"] == tabulate.sha256(paths["observations"])
    assert manifest["quotes_sha256"] == tabulate.sha256(paths["quotes"])
    assert manifest["price_manifest_sha256"] == tabulate.sha256(paths["price_manifest"])
    assert manifest["quote_return_audit_py_sha256"] == tabulate.sha256(Path(quote_return_audit.__file__))
    assert manifest["output_row_counts"]["checkpoint_bands"] == 48
    assert manifest["output_row_counts"]["cell_bands"] == 960
    for name, expected in manifest["artifacts_sha256"].items():
        assert tabulate.sha256(paths["output"] / name) == expected
    persisted = json.loads((paths["output"] / "audit.json").read_text(encoding="utf-8"))
    assert no_float_values(persisted)
    assert all(Decimal(row["gross_pnl_mean"]) == Decimal("0.10") for row in persisted["checkpoint_totals"])
    with pytest.raises(ValueError):
        quote_return_audit.run_audit(**paths)
    assert manifest_path.read_bytes() == manifest_before


@pytest.mark.parametrize("field,value", [("observations_sha256", "0" * 64), ("quotes_sha256", "0" * 64),
                                        ("quote_max_age_ms", 10000), ("outcome_cutoff_ms", 0),
                                        ("quote_extraction_ms", 0), ("cohort_market_count", 99),
                                        ("market_prices_py_sha256", "0" * 64), ("tabulate_helper_sha256", "0" * 64)])
def test_run_rejects_mismatched_prior_pricing_provenance_before_output(tmp_path, field, value):
    paths = prepare_previous_pricing_run(tmp_path)
    manifest = json.loads(paths["price_manifest"].read_text(encoding="utf-8"))
    manifest[field] = value
    paths["price_manifest"].write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        quote_return_audit.run_audit(**paths)
    assert not paths["output"].exists() or not list(paths["output"].iterdir())


@pytest.mark.parametrize("field,value", [("psql_exit_code", 3), ("psql_exit_code", False),
                                        ("read_only", False), ("isolation", "read committed"),
                                        ("sql_sha256", "0" * 64)])
def test_run_rejects_invalid_extraction_process_inside_prior_manifest(tmp_path, field, value):
    paths = prepare_previous_pricing_run(tmp_path)
    manifest = json.loads(paths["price_manifest"].read_text(encoding="utf-8"))
    manifest["quote_extraction_process"][field] = value
    paths["price_manifest"].write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        quote_return_audit.run_audit(**paths)
    assert not paths["output"].exists()
