"""Independent synthetic checks of matched prices and settlement denominators."""

import csv
import json
import sys
from decimal import Decimal, localcontext

import pytest

from research.leader_risk import market_prices, tabulate
from test_leader_risk_tabulate import START_MS, make_export


def make_quotes(observations, *market_specs):
    market_ids = sorted({row["market_id"] for row in observations})
    specs = {market_id: market_specs[index] if index < len(market_specs) else {}
             for index, market_id in enumerate(market_ids)}
    rows = []
    for observation in observations:
        cut = observation["cut_ms"]
        row = {name: observation[name] for name in ("market_id", "t_sec", "cut_ms",
                                                    "requested_cohort_start_ms", "cohort_start_ms",
                                                    "cohort_end_ms", "cohort_market_count")}
        row.update(quote_extraction_ms=str(int(observation["outcome_cutoff_ms"]) + 120_000), quote_sample_ms=cut,
                   quote_received_ms=cut, tokens_match="t", up_bid="0.97", up_ask="0.98",
                   down_bid="0.01", down_ask="0.02", up_provider_event_ms=cut, up_received_ms=cut,
                   down_provider_event_ms=cut, down_received_ms=cut, event_type="book")
        for outcome in ("up", "down"):
            for side in ("bid", "ask"):
                row[f"{outcome}_{side}_source_ms"] = cut
                row[f"{outcome}_{side}_received_ms"] = cut
        spec = specs[row["market_id"]]
        row.update({key: value for key, value in spec.items() if key != "edit"})
        if "edit" in spec:
            spec["edit"](row)
        rows.append(row)
    return rows


def analyzed(*spec_pairs):
    observations = make_export(*(pair[0] for pair in spec_pairs))
    quotes = make_quotes(observations, *(pair[1] for pair in spec_pairs))
    return market_prices.analyze(observations, quotes)


def cell(result, t=120, x="[2,4)", y="[0,2)"):
    matches = [row for row in result["cells"]
               if row["t_sec"] == t and row["x_bin"] == x and row["y_bin"] == y]
    assert len(matches) == 1
    return matches[0]


def no_float_values(value):
    if isinstance(value, dict):
        return all(no_float_values(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return all(no_float_values(item) for item in value)
    return not isinstance(value, float)


@pytest.mark.parametrize("values,expected", [
    ([], None),
    (["0.99"], "0.99"),
    (["0.9", "0.2", "0.5"], "0.5"),
    (["0.900000000000000001", "0.900000000000000003"], "0.900000000000000002"),
    (["1", "0.1", "0.7", "0.5"], "0.6"),
])
def test_median_preserves_exact_decimal_prices_even_with_low_ambient_precision(values, expected):
    prices = [Decimal(value) for value in values]
    before = prices.copy()
    with localcontext() as context:
        context.prec = 6
        median = market_prices.median_decimal(prices)
    assert median == (Decimal(expected) if expected is not None else None)
    assert median is None or isinstance(median, Decimal)
    assert prices == before


@pytest.mark.parametrize("bad_value", [0.5, Decimal("NaN"), Decimal("Infinity")])
def test_median_rejects_binary_float_and_nonfinite_input(bad_value):
    with pytest.raises(ValueError):
        market_prices.median_decimal([bad_value])


@pytest.mark.parametrize("price,display", [("0.999999", "$0.999999"),
                                          ("0.999999999999999999", "$0.999999999999999999"),
                                          ("1", "$1.00"), ("0.98", "$0.98")])
def test_price_display_preserves_exact_decimal_digits_without_rounding_near_one(price, display):
    assert market_prices._money(Decimal(price)) == display


@pytest.mark.parametrize("ask,band", [
    ("0", "ask=0"), ("0.000000000000000001", "(0,0.90)"),
    ("0.899999999999999999", "(0,0.90)"), ("0.90", "[0.90,0.95)"),
    ("0.949999999999999999", "[0.90,0.95)"), ("0.95", "[0.95,0.98)"),
    ("0.979999999999999999", "[0.95,0.98)"), ("0.98", "[0.98,1)"),
    ("0.999999999999999999", "[0.98,1)"), ("1", "ask=1"),
])
def test_price_band_edges_keep_zero_and_one_separate(ask, band):
    assert market_prices.price_band(Decimal(ask)) == band


def test_matched_prices_and_risk_use_identical_subset_including_unknowns():
    result = analyzed(
        ({}, {"up_bid": "0.90", "up_ask": "0.91"}),
        ({"official_winner": "Down"}, {"up_bid": "0.94", "up_ask": "0.95"}),
        ({"official_winner": ""}, {"up_bid": "0.98", "up_ask": "0.99"}),
        ({"official_winner": "Down"}, {"up_bid": "", "up_ask": "0.20"}),
        ({"official_winner": "Down"}, {"tokens_match": "f"}),
    )
    for t in tabulate.CHECKPOINTS:
        row = cell(result, t)
        assert (row["full_N"], row["full_U"], row["full_L"], row["full_resolved"]) == (5, 1, 3, 4)
        assert row["full_loss_rate"] == Decimal("0.75")
        assert (row["matched_N"], row["matched_U"], row["matched_L"], row["matched_resolved"]) == (3, 1, 1, 2)
        assert row["matched_loss_rate"] == Decimal("0.5")
        assert (row["median_bid"], row["median_ask"]) == (Decimal("0.94"), Decimal("0.95"))
        assert row["ask_only_N"] == 1
        assert row["neither_available_N"] == 1
    assert no_float_values(result)


def test_all_unknown_matched_cell_retains_prices_and_unavailable_loss_rate():
    row = cell(analyzed(({"official_winner": ""}, {})))
    assert (row["matched_N"], row["matched_U"], row["matched_L"], row["matched_resolved"]) == (1, 1, 0, 0)
    assert row["matched_loss_rate"] is row["matched_wilson_95_low"] is row["matched_wilson_95_high"] is None
    assert row["median_bid"] == Decimal("0.97") and row["median_ask"] == Decimal("0.98")


def test_cheap_ask_subset_has_its_own_risk_not_full_cell_risk():
    result = analyzed(({}, {"up_bid": "0.98", "up_ask": "0.99"}),
                      ({}, {"up_bid": "0.98", "up_ask": "0.99"}),
                      ({}, {"up_bid": "0.98", "up_ask": "0.99"}),
                      ({"official_winner": "Down"}, {"up_bid": "0.80", "up_ask": "0.81"}),
                      ({"official_winner": ""}, {"up_bid": "0.82", "up_ask": "0.83"}))
    row = cell(result)
    assert (row["matched_N"], row["matched_U"], row["matched_L"], row["matched_resolved"]) == (5, 1, 1, 4)
    assert row["matched_loss_rate"] == Decimal("0.25")
    cheap = [row for row in result["bands"] if row["t_sec"] == 120
             and row["x_bin"] == "[2,4)" and row["y_bin"] == "[0,2)"
             and row["ask_band"] == "(0,0.90)"]
    assert len(cheap) == 1
    assert (cheap[0]["N"], cheap[0]["U"], cheap[0]["L"], cheap[0]["resolved"]) == (2, 1, 1, 1)
    assert cheap[0]["loss_rate"] == Decimal(1)
    assert cheap[0]["median_ask"] == Decimal("0.82")


def test_down_leader_uses_down_quotes_and_opposite_outcome_does_not_refresh_or_invalidate_them():
    observations = make_export({"w": "9998", "s": "9997", "official_winner": "Down"})
    quotes = make_quotes(observations, {"up_bid": "NaN", "up_ask": "", "up_bid_source_ms": "bad",
                                       "up_ask_received_ms": "", "down_bid": "0.79", "down_ask": "0.81"})
    row = cell(market_prices.analyze(observations, quotes))
    assert row["matched_N"] == 1
    assert row["matched_L"] == 0
    assert (row["median_bid"], row["median_ask"]) == (Decimal("0.79"), Decimal("0.81"))
    quote = quotes[0]
    quote["down_ask_source_ms"] = str(int(quote["cut_ms"]) - 3001)
    assessed = market_prices.assess_quote(quote, "Down", int(quote["cut_ms"]))
    assert assessed.bid_available and not assessed.ask_available and not assessed.matched


@pytest.mark.parametrize("clock", ["quote_sample_ms", "quote_received_ms", "up_bid_source_ms",
                                   "up_bid_received_ms", "up_ask_source_ms", "up_ask_received_ms"])
@pytest.mark.parametrize("age", [0, 3000])
def test_each_selected_quote_clock_has_inclusive_zero_to_3000_ms_age(clock, age):
    row = make_quotes(make_export())[0]
    cut = int(row["cut_ms"])
    row[clock] = str(cut - age)
    assessment = market_prices.assess_quote(row, "Up", cut)
    assert assessment.bid_available and assessment.ask_available and assessment.matched


@pytest.mark.parametrize("component", ["bid", "ask"])
@pytest.mark.parametrize("clock_suffix", ["source_ms", "received_ms"])
@pytest.mark.parametrize("age", [-1, 3001])
def test_component_freshness_is_independent_and_primary_match_needs_both(component, clock_suffix, age):
    row = make_quotes(make_export())[0]
    cut = int(row["cut_ms"])
    row[f"up_{component}_{clock_suffix}"] = str(cut - age)
    assessment = market_prices.assess_quote(row, "Up", cut)
    assert not getattr(assessment, f"{component}_available")
    assert getattr(assessment, "ask_available" if component == "bid" else "bid_available")
    assert not assessment.matched
    assert assessment.reasons


@pytest.mark.parametrize("clock", ["quote_sample_ms", "quote_received_ms"])
def test_future_retained_sample_or_row_receipt_is_a_contract_error(clock):
    observations = make_export()
    quotes = make_quotes(observations)
    quotes[0][clock] = str(int(quotes[0]["cut_ms"]) + 1)
    with pytest.raises(ValueError):
        market_prices.analyze(observations, quotes)


@pytest.mark.parametrize("component", ["bid", "ask"])
@pytest.mark.parametrize("bad_price", ["", "NaN", "Infinity", "-Infinity", "not-a-price", "-0.01", "1.01"])
def test_missing_invalid_and_nonfinite_component_prices_are_unavailable(component, bad_price):
    row = make_quotes(make_export())[0]
    row[f"up_{component}"] = bad_price
    assessment = market_prices.assess_quote(row, "Up", int(row["cut_ms"]))
    assert not getattr(assessment, f"{component}_available")
    assert not assessment.matched
    assert assessment.reasons


@pytest.mark.parametrize("clock", ["up_bid_source_ms", "up_bid_received_ms", "up_ask_source_ms", "up_ask_received_ms"])
@pytest.mark.parametrize("bad_clock", ["", "bad"])
def test_missing_component_clocks_never_fall_back_to_fresh_outcome_clocks(clock, bad_clock):
    row = make_quotes(make_export())[0]
    row[clock] = bad_clock
    assessment = market_prices.assess_quote(row, "Up", int(row["cut_ms"]))
    assert not assessment.matched
    assert not getattr(assessment, "bid_available" if "_bid_" in clock else "ask_available")
    assert assessment.reasons


@pytest.mark.parametrize("tokens", ["f", ""])
def test_mismatched_or_unavailable_token_identity_cannot_match_quotes(tokens):
    row = make_quotes(make_export())[0]
    row["tokens_match"] = tokens
    assessment = market_prices.assess_quote(row, "Up", int(row["cut_ms"]))
    assert not assessment.bid_available and not assessment.ask_available and not assessment.matched
    assert assessment.reasons


def test_crossed_pair_remains_counted_but_is_not_a_primary_price_match():
    result = analyzed(({}, {"up_bid": "0.99", "up_ask": "0.98"}))
    row = cell(result)
    assert row["paired_quote_N"] == 1
    assert row["crossed_quote_N"] == 1
    assert row["matched_N"] == 0
    assert row["median_bid"] is None and row["median_ask"] is None


def test_zero_and_one_asks_are_retained_separately_and_never_relabelled_as_fills():
    result = analyzed(({}, {"up_bid": "0", "up_ask": "0"}),
                      ({}, {"up_bid": "0.99", "up_ask": "1"}))
    row = cell(result)
    assert row["matched_N"] == 2
    assert row["matched_ask_eq_0_N"] == row["matched_ask_eq_1_N"] == 1
    bands = {band["ask_band"]: band for band in result["bands"]
             if band["t_sec"] == 120 and band["x_bin"] == "[2,4)" and band["y_bin"] == "[0,2)"}
    assert bands["ask=0"]["N"] == bands["ask=1"]["N"] == 1
    assert sum(band["N"] for band in bands.values()) == 2
    assert all("fill" not in key.lower() for band in bands.values() for key in band)


def test_all_cells_and_ask_bands_are_emitted_and_counts_conserve_at_each_checkpoint():
    result = analyzed(({}, {}), ({"official_winner": "Down"}, {"up_ask": "1"}),
                      ({"official_winner": ""}, {"up_bid": "0.1", "up_ask": "0.2"}),
                      ({}, {"tokens_match": "f"}))
    assert len(result["cells"]) == 160
    assert len(result["bands"]) == 960
    assert len({(row["t_sec"], row["x_bin"], row["y_bin"]) for row in result["cells"]}) == 160
    assert len({(row["t_sec"], row["x_bin"], row["y_bin"], row["ask_band"]) for row in result["bands"]}) == 960
    for t in tabulate.CHECKPOINTS:
        cells = [row for row in result["cells"] if row["t_sec"] == t]
        bands = [row for row in result["bands"] if row["t_sec"] == t]
        assert len(cells) == 20 and len(bands) == 120
        for name, expected in (("N", 3), ("U", 1), ("L", 1)):
            assert sum(row[f"matched_{name}"] for row in cells) == expected
            assert sum(row[name] for row in bands) == expected
        assert sum(row["full_N"] for row in cells) == 4
        assert all(row["matched_loss_rate"] is None and row["median_ask"] is None
                   for row in cells if row["matched_N"] == 0)
        assert all(row["loss_rate"] is None and row["median_bid"] is None
                   for row in bands if row["N"] == 0)


def test_no_eligible_study_inputs_still_emit_all_empty_cells_and_bands():
    result = analyzed(({"k": "", "boundary_variants": "0", "k_received_ns": ""}, {}),
                      ({"w": "10000"}, {}))
    assert len(result["cells"]) == 160 and len(result["bands"]) == 960
    assert all(row["full_N"] == row["matched_N"] == 0 and row["median_ask"] is None for row in result["cells"])
    assert all(row["N"] == 0 and row["loss_rate"] is None for row in result["bands"])
    assert all(row["rows"] == 2 and row["study_input_unavailable_N"] == row["study_tie_N"] == 1
               for row in result["coverage"])


def test_missing_retained_quote_row_preserves_full_study_counts_and_explicit_missingness():
    observations = make_export()
    quotes = make_quotes(observations)
    keep = {"market_id", "t_sec", "cut_ms", "quote_extraction_ms", "requested_cohort_start_ms",
            "cohort_start_ms", "cohort_end_ms", "cohort_market_count"}
    for row in quotes:
        row.update({key: "" for key in row if key not in keep})
    result = market_prices.analyze(observations, quotes)
    for t in tabulate.CHECKPOINTS:
        row = cell(result, t)
        assert row["full_N"] == 1 and row["matched_N"] == 0
        assert row["excluded_no_quote_row_N"] == row["neither_available_N"] == 1


@pytest.mark.parametrize("failure", ["duplicate", "missing_checkpoint", "truncated_market", "extra_key"])
def test_quote_export_must_match_every_base_market_checkpoint(failure):
    observations = make_export({}, {})
    quotes = make_quotes(observations)
    if failure == "duplicate":
        quotes.append(quotes[0].copy())
    elif failure == "missing_checkpoint":
        quotes.pop()
    elif failure == "truncated_market":
        quotes = quotes[:8]
    else:
        quotes[0]["market_id"] = str(int(quotes[0]["market_id"]) + 1000)
    with pytest.raises(ValueError):
        market_prices.analyze(observations, quotes)


@pytest.mark.parametrize("field,value", [
    ("requested_cohort_start_ms", str(START_MS - 300_000)),
    ("cohort_start_ms", str(START_MS + 300_000)),
    ("cohort_end_ms", str(START_MS + 600_000)),
    ("cohort_market_count", "2"),
])
def test_quote_snapshot_bounds_and_market_count_must_match_frozen_base_panel(field, value):
    observations = make_export()
    quotes = make_quotes(observations)
    for row in quotes:
        row[field] = value
    with pytest.raises(ValueError):
        market_prices.analyze(observations, quotes)


def test_later_quote_snapshot_does_not_change_frozen_official_outcome_snapshot():
    observations = make_export()
    quotes = make_quotes(observations)
    result = market_prices.analyze(observations, quotes)
    assert result["metadata"]["outcome_cutoff_ms"] == int(observations[0]["outcome_cutoff_ms"])
    assert result["metadata"]["quote_extraction_ms"] == int(quotes[0]["quote_extraction_ms"])
    assert result["metadata"]["quote_extraction_ms"] > result["metadata"]["outcome_cutoff_ms"]


def prepare_cli_run(tmp_path, monkeypatch, extraction_overrides=None, quote_specs=()):
    observations = make_export({}, {"official_winner": "Down"})
    quotes = make_quotes(observations, *quote_specs)
    paths = {}
    for name, rows in (("observations", observations), ("quotes", quotes)):
        path = tmp_path / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        paths[name] = path
    sql = tmp_path / "extract_quotes.sql"
    sql.write_text("-- frozen synthetic quote extraction\n", encoding="utf-8")
    extraction = {"start_utc": tabulate.utc_text(START_MS),
                  "end_utc_exclusive": tabulate.utc_text(START_MS + 600_000),
                  "statement_timeout": "60s", "sql_sha256": tabulate.sha256(sql),
                  "read_only": True, "isolation": "repeatable read", "psql_exit_code": 0}
    extraction.update(extraction_overrides or {})
    extraction_path = tmp_path / "extraction.json"
    extraction_path.write_text(json.dumps(extraction), encoding="utf-8")
    output = tmp_path / "result"
    monkeypatch.setattr(sys, "argv", ["market_prices.py", "--observations", str(paths["observations"]),
                                     "--quotes", str(paths["quotes"]), "--quote-extraction", str(extraction_path),
                                     "--output", str(output), "--sql", str(sql)])
    return output, extraction, sql


def test_cli_writes_full_outputs_only_from_successful_matching_quote_extraction(tmp_path, monkeypatch):
    output, extraction, sql = prepare_cli_run(tmp_path, monkeypatch)
    market_prices.main()
    for filename, expected in (("market_price_cells.csv", 160), ("price_bands.csv", 960), ("coverage.csv", 8)):
        with (output / filename).open(newline="", encoding="utf-8") as handle:
            assert len(list(csv.DictReader(handle))) == expected
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["primary_cell_count"] == 160 and manifest["price_band_count"] == 960
    assert manifest["quote_extraction_process"] == extraction
    assert manifest["extract_quotes_sql_sha256"] == tabulate.sha256(sql)
    assert manifest["observations_sha256"] == tabulate.sha256(tmp_path / "observations.csv")
    assert manifest["quotes_sha256"] == tabulate.sha256(tmp_path / "quotes.csv")
    assert manifest["quote_extraction_record_sha256"] == tabulate.sha256(tmp_path / "extraction.json")
    for filename, expected_hash in manifest["artifacts_sha256"].items():
        assert tabulate.sha256(output / filename) == expected_hash
    explorer = json.loads((output / "explorer_data.json").read_text(encoding="utf-8"))
    assert len(explorer["cells"]) == 160 and len(explorer["bands"]) == 960
    populated = [row for row in explorer["cells"] if row["matched_N"]]
    assert all(row["median_bid"] == "0.97" and row["median_ask"] == "0.98" for row in populated)
    assert no_float_values(explorer)
    manifest_before = (output / "manifest.json").read_bytes()
    with pytest.raises(ValueError, match="Output artifacts already exist"):
        market_prices.main()
    assert (output / "manifest.json").read_bytes() == manifest_before


@pytest.mark.parametrize("override", [{"psql_exit_code": 3}, {"psql_exit_code": False}, {"read_only": False},
                                       {"isolation": "read committed"}, {"sql_sha256": "0" * 64},
                                       {"start_utc": "2026-08-31T23:55:00Z"},
                                       {"end_utc_exclusive": "2026-09-01T00:15:00Z"}])
def test_cli_rejects_failed_or_mismatched_extraction_without_writing_artifacts(tmp_path, monkeypatch, override):
    output, _, _ = prepare_cli_run(tmp_path, monkeypatch, override)
    with pytest.raises(ValueError):
        market_prices.main()
    assert not output.exists() or not list(output.iterdir())


def test_cli_rejects_sql_changed_since_quote_extraction(tmp_path, monkeypatch):
    output, _, sql = prepare_cli_run(tmp_path, monkeypatch)
    sql.write_text("-- changed after recorded extraction\n", encoding="utf-8")
    with pytest.raises(ValueError):
        market_prices.main()
    assert not output.exists() or not list(output.iterdir())


def test_explorer_and_report_do_not_display_a_subunit_ask_as_one_dollar(tmp_path, monkeypatch):
    output, _, _ = prepare_cli_run(tmp_path, monkeypatch,
                                   quote_specs=({"up_ask": "0.999999"}, {"up_ask": "0.999999"}))
    market_prices.main()
    explorer = json.loads((output / "explorer_data.json").read_text(encoding="utf-8"))
    populated = [row for row in explorer["cells"] if row["matched_N"]]
    assert all(row["median_ask"] == "0.999999" and row["median_ask_display"] == "$0.999999"
               for row in populated)
    report = (output / "report.md").read_text(encoding="utf-8")
    assert "ask $0.999999" in report
