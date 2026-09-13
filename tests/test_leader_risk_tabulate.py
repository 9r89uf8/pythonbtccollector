"""Synthetic exported panels exercise the independent descriptive tabulator."""

import csv
import json
import sys
from decimal import Decimal, localcontext
from itertools import product

import pytest

from research.leader_risk import tabulate


START_MS = tabulate.utc_ms("2026-09-01T00:00:00Z")


def make_export(*market_specs):
    """Build the CSV representation of complete eight-checkpoint markets.

    Each spec can override prices/outcomes or apply an observation edit at each
    cut. SQL's derived flags are supplied too, so malformed-export tests change
    their target field only after construction.
    """
    market_specs = market_specs or ({},)
    offsets = [spec.get("offset", index) for index, spec in enumerate(market_specs)]
    end = START_MS + (max(offsets) + 1) * 300_000
    rows = []
    for index, spec in enumerate(market_specs):
        start = START_MS + offsets[index] * 300_000
        for t in tabulate.CHECKPOINTS:
            cut = start + 300_000 - t * 1000
            row = {
                "requested_cohort_start_ms": str(START_MS),
                "cohort_start_ms": str(START_MS),
                "cohort_end_ms": str(end),
                "outcome_cutoff_ms": str(end + 60_000),
                "cohort_market_count": str(len(market_specs)),
                "market_id": str(start // 300_000),
                "start_ms": str(start),
                "end_ms": str(start + 300_000),
                "t_sec": str(t),
                "cut_ms": str(cut),
                "rule_valid": "t",
                "resolution_status": "resolved",
                "resolution_type": "winner",
                "reconciled_settlement_rule_version": "btc-5m-twap-60",
                "official_winner": "Up",
                "official_k_audit_only": "10000",
                "boundary_variants": "1",
                "k_received_ns": str(start * 1_000_000),
                "k": "10000",
                "w": "10002",
                "w_source_ms": str(cut),
                "w_received_ns": str(cut * 1_000_000),
                "s": "10003",
                "s_source_ms": str(cut),
                "s_received_ms": str(cut),
            }
            row.update({key: value for key, value in spec.items() if key not in {"offset", "edit"}})
            if "edit" in spec:
                spec["edit"](row)
            with localcontext() as context:
                context.prec = 60
                row["w_e18"] = str(Decimal(row["w"]) * Decimal(10) ** 18) if row["w"] else ""
            prices_present = all(row[name] and Decimal(row[name]) > 0 for name in ("k", "w", "s"))
            w_source_fresh = row["w_source_ms"] != "" and cut - 3000 <= int(row["w_source_ms"]) <= cut
            s_source_fresh = row["s_source_ms"] != "" and cut - 3000 <= int(row["s_source_ms"]) <= cut
            flags = {
                "inputs_available": row["rule_valid"] == "t" and prices_present and w_source_fresh and s_source_fresh,
                "current_tie": bool(row["k"] and row["w"]) and Decimal(row["k"]) == Decimal(row["w"]),
                "invalid_market_rule": row["rule_valid"] == "f",
                "boundary_missing_or_late": row["boundary_variants"] == "0",
                "boundary_conflict": int(row["boundary_variants"]) > 1,
                "twap_no_recent_receipt": row["w_received_ns"] == "",
                "spot_no_recent_receipt": row["s_received_ms"] == "",
                "twap_bad_source_age": bool(row["w_received_ns"]) and not w_source_fresh,
                "spot_bad_source_age": bool(row["s_received_ms"]) and not s_source_fresh,
            }
            row.update({name: "t" if value else "f" for name, value in flags.items()})
            rows.append(row)
    return rows


def analyze(rows):
    return tabulate.analyze(rows, START_MS + 300_000)


def test_wilson_empty_zero_loss_and_all_loss_hand_cases():
    assert tabulate.wilson(0, 0) == (None, None, None)
    with localcontext() as context:
        context.prec = 60
        z_squared = tabulate.Z**2
        expected_zero_upper = z_squared / (10 + z_squared)
        expected_all_lower = Decimal(10) / (10 + z_squared)
    zero_rate, zero_lower, zero_upper = tabulate.wilson(0, 10)
    all_rate, all_lower, all_upper = tabulate.wilson(10, 10)
    assert (zero_rate, zero_lower) == (Decimal(0), Decimal(0))
    assert abs(zero_upper - expected_zero_upper) < Decimal("1e-55")
    assert (all_rate, all_upper) == (Decimal(1), Decimal(1))
    assert abs(all_lower - expected_all_lower) < Decimal("1e-55")
    rate, low, high = tabulate.wilson(5, 10)
    assert rate == Decimal("0.5")
    assert abs(low - Decimal("0.236593090512564")) < Decimal("1e-14")
    assert abs(high - Decimal("0.763406909487436")) < Decimal("1e-14")
    assert all(isinstance(value, Decimal) for value in (rate, low, high))


@pytest.mark.parametrize("losses,resolved", [(-1, 10), (11, 10), (0, -1)])
def test_wilson_rejects_impossible_counts(losses, resolved):
    with pytest.raises(ValueError, match="0 <= losses <= resolved"):
        tabulate.wilson(losses, resolved)


@pytest.mark.parametrize("x,xi", [("0.000000000000000001", 0), ("0.999999999999999999", 0),
                                   ("1", 1), ("1.999999999999999999", 1), ("2", 2),
                                   ("3.999999999999999999", 2), ("4", 3),
                                   ("7.999999999999999999", 3), ("8", 4), ("100", 4)])
@pytest.mark.parametrize("y,yi", [("-2.000000000000000001", 0), ("-2", 1),
                                   ("-0.000000000000000001", 1), ("0", 2),
                                   ("1.999999999999999999", 2), ("2", 3)])
def test_decimal_bin_edges_and_down_symmetry(x, xi, y, yi):
    x, y, k = Decimal(x), Decimal(y), Decimal(10000)
    with localcontext() as context:
        context.prec = 60
        up = tabulate.bin_coordinates(k, k + x, k + x + y)
        down = tabulate.bin_coordinates(k, k - x, k - x - y)
    assert up == ("Up", x, y, xi, yi)
    assert down == ("Down", x, y, xi, yi)
    assert all(isinstance(value, Decimal) for value in (*up[1:3], *down[1:3]))


@pytest.mark.parametrize("prices", [("100", "100", "101"), ("0", "1", "1"),
                                    ("100", "-1", "100"), ("100", "101", "0")])
def test_coordinates_reject_ties_and_nonpositive_prices(prices):
    with pytest.raises(ValueError, match="positive prices and a non-tied leader"):
        tabulate.bin_coordinates(*(Decimal(value) for value in prices))


@pytest.mark.parametrize("ages", list(product((0, 3000), repeat=4)))
def test_all_source_and_receipt_age_endpoints_are_inclusive(ages):
    w_source_age, s_source_age, w_receipt_age, s_receipt_age = ages

    def clocks(row):
        cut = int(row["cut_ms"])
        row.update(w_source_ms=str(cut - w_source_age), s_source_ms=str(cut - s_source_age),
                   w_received_ns=str((cut - w_receipt_age) * 1_000_000),
                   s_received_ms=str(cut - s_receipt_age), k_received_ns=str(cut * 1_000_000))

    result = analyze(make_export({"edit": clocks}))
    for t in tabulate.CHECKPOINTS:
        assert result["totals"][t].record()["N"] == 1
        assert result["coverage"][t]["eligible_twap_source_age_3000"] == (w_source_age == 3000)
        assert result["coverage"][t]["eligible_spot_source_age_3000"] == (s_source_age == 3000)


@pytest.mark.parametrize("source,reason", [("w_source_ms", "twap_bad_source_age"),
                                          ("s_source_ms", "spot_bad_source_age")])
@pytest.mark.parametrize("age", [-1, 3001])
def test_latest_received_future_or_stale_source_is_excluded(source, reason, age):
    rows = make_export({"edit": lambda row: row.update({source: str(int(row["cut_ms"]) - age)})})
    result = analyze(rows)
    for t in tabulate.CHECKPOINTS:
        assert result["coverage"][t]["input_unavailable"] == 1
        assert result["reasons"][t][reason] == 1
        assert result["totals"][t].N == 0


@pytest.mark.parametrize("clock,scale,message", [("w_received_ns", 1_000_000, "TWAP receipt"),
                                                ("s_received_ms", 1, "Spot receipt")])
@pytest.mark.parametrize("outside", ["future", "stale"])
def test_export_cannot_smuggle_selected_receipts_outside_asof_interval(clock, scale, message, outside):
    rows = make_export()
    cut = int(rows[0]["cut_ms"])
    rows[0][clock] = str(cut * scale + 1 if outside == "future" else (cut - 3000) * scale - 1)
    with pytest.raises(ValueError, match=message):
        analyze(rows)


@pytest.mark.parametrize("spec,reason", [
    ({"k": "", "boundary_variants": "0", "k_received_ns": ""}, "boundary_missing_or_late"),
    ({"k": "", "boundary_variants": "2"}, "boundary_conflict"),
    ({"w": "", "w_received_ns": "", "w_source_ms": ""}, "twap_no_recent_receipt"),
    ({"s": "", "s_received_ms": "", "s_source_ms": ""}, "spot_no_recent_receipt"),
    ({"rule_valid": "f"}, "invalid_market_rule"),
])
def test_missing_conflicting_or_invalid_inputs_stay_excluded(spec, reason):
    result = analyze(make_export(spec))
    for t in tabulate.CHECKPOINTS:
        assert result["coverage"][t]["input_unavailable"] == 1
        assert result["reasons"][t][reason] == 1
        assert result["totals"][t].N == 0


def test_later_official_open_cannot_repair_missing_decision_time_boundary():
    rows = make_export({"k": "", "boundary_variants": "0", "k_received_ns": "",
                        "official_k_audit_only": "10000"})
    assert all(analyze(rows)["totals"][t].N == 0 for t in tabulate.CHECKPOINTS)
    rows = make_export()
    rows[0]["k_received_ns"] = str(int(rows[0]["cut_ms"]) * 1_000_000 + 1)
    with pytest.raises(ValueError, match="K was not received by the checkpoint"):
        analyze(rows)


def test_available_current_ties_are_separate_from_missing_inputs_and_eligible_rows():
    result = analyze(make_export({"w": "10000"}))
    for t in tabulate.CHECKPOINTS:
        assert result["coverage"][t]["available_tie"] == 1
        assert result["coverage"][t]["input_unavailable"] == 0
        assert result["totals"][t].N == 0


def test_unknown_outcome_is_in_n_but_not_loss_rate_denominator():
    result = analyze(make_export({}, {"official_winner": "Down"},
                                 {"official_winner": "", "resolution_status": "pending", "resolution_type": ""}))
    for t in tabulate.CHECKPOINTS:
        totals = result["totals"][t].record()
        assert (totals["N"], totals["U"], totals["L"], totals["resolved"]) == (3, 1, 1, 2)
        assert totals["loss_rate"] == Decimal("0.5")
        assert result["coverage"][t]["eligible_unknown_outcome"] == 1
    unknown_only = analyze(make_export({"official_winner": ""}))["totals"][120].record()
    assert unknown_only == {"N": 1, "U": 1, "L": 0, "resolved": 0,
                            "loss_rate": None, "wilson_95_low": None, "wilson_95_high": None}


def test_counts_conserve_each_checkpoint_without_pooling_repeated_markets():
    rows = make_export({}, {"official_winner": "Down"}, {"official_winner": ""},
                       {"w": "9998", "s": "9997", "official_winner": "Down"},
                       {"w": "10000"}, {"k": "", "boundary_variants": "0", "k_received_ns": ""})
    result = analyze(rows)
    for t in tabulate.CHECKPOINTS:
        coverage = result["coverage"][t]
        assert (coverage["total"], coverage["eligible"], coverage["available_tie"],
                coverage["input_unavailable"]) == (6, 4, 1, 1)
        for name, expected in (("N", 4), ("U", 1), ("L", 1)):
            assert getattr(result["totals"][t], name) == expected
            for dimension in ("cells", "direction_cells", "day_cells", "period_cells"):
                assert sum(getattr(counts, name) for key, counts in result[dimension].items()
                           if key[0] == t) == expected
        assert sum(count for key, count in result["outcomes"].items() if key[0] == t) == 6
    assert result["metadata"]["markets"] == 6
    assert result["metadata"]["rows"] == 48


@pytest.mark.parametrize("failure,match", [("duplicate", "Duplicate market/checkpoint"),
                                           ("missing_checkpoint", "exactly eight checkpoints"),
                                           ("truncated_whole_market", "snapshot count"),
                                           ("empty", "Empty export")])
def test_incomplete_or_duplicate_csv_is_rejected(failure, match):
    rows = make_export({}, {})
    if failure == "duplicate":
        rows.append(rows[0].copy())
    elif failure == "missing_checkpoint":
        rows.pop()
    elif failure == "truncated_whole_market":
        rows = rows[:8]
    else:
        rows = []
    with pytest.raises(ValueError, match=match):
        analyze(rows)


def test_missing_metadata_calendar_slot_is_reported_without_fabricating_a_market():
    result = analyze(make_export({"offset": 0}, {"offset": 2}))
    metadata = result["metadata"]
    assert metadata["markets"] == metadata["cohort_market_count"] == 2
    assert metadata["calendar_market_slots"] == 3
    assert metadata["missing_metadata_market_ids"] == [START_MS // 300_000 + 1]


@pytest.mark.parametrize("field,value", [
    ("requested_cohort_start_ms", START_MS - 300_000),
    ("cohort_start_ms", START_MS + 1),
    ("cohort_end_ms", START_MS + 300_001),
    ("cohort_end_ms", START_MS),
    ("outcome_cutoff_ms", START_MS + 299_999),
])
def test_invalid_incomplete_or_silently_clamped_bounds_are_rejected(field, value):
    rows = make_export()
    for row in rows:
        row[field] = str(value)
    with pytest.raises(ValueError, match="cohort bounds"):
        analyze(rows)


@pytest.mark.parametrize("field,value,match", [
    ("cohort_market_count", "2", "Inconsistent extraction metadata"),
    ("official_winner", "Down", "identity/outcome changes"),
    ("cut_ms", "0", "market/checkpoint boundary"),
    ("w_e18", "10002000000000000000001", "exact E18"),
    ("inputs_available", "f", "eligibility disagrees"),
    ("current_tie", "t", "tie flag disagrees"),
    ("twap_bad_source_age", "t", "exclusion flags disagree"),
    ("boundary_variants", "0", "variant count disagrees"),
])
def test_contradictory_export_fields_are_rejected(field, value, match):
    rows = make_export()
    rows[1][field] = value
    with pytest.raises(ValueError, match=match):
        analyze(rows)


@pytest.mark.parametrize("spec", [{"resolution_status": "pending"}, {"resolution_type": "void"},
                                  {"reconciled_settlement_rule_version": "btc-5m-twap-30"},
                                  {"official_winner": "Tie"}])
def test_unverified_winner_cannot_enter_losses_or_resolved_denominator(spec):
    with pytest.raises(ValueError, match="Unverified official winner"):
        analyze(make_export(spec))


def write_artifacts(tmp_path, rows):
    observations = tmp_path / "observations.csv"
    with observations.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    sql = tmp_path / "extract.sql"
    sql.write_text("-- synthetic fixture extraction\n", encoding="utf-8")
    output = tmp_path / "result"
    tabulate.write_results(analyze(rows), output, observations, sql)
    return output


def test_artifacts_include_all_160_cells_and_unavailable_empty_cell_rates(tmp_path):
    output = write_artifacts(tmp_path, make_export({}, {"official_winner": "Down"},
                                                   {"official_winner": ""}))
    with (output / "grids.csv").open(newline="", encoding="utf-8") as handle:
        grids = list(csv.DictReader(handle))
    assert len(grids) == 160
    assert len({(row["t_sec"], row["x_bin"], row["y_bin"]) for row in grids}) == 160
    for t in tabulate.CHECKPOINTS:
        cells = [row for row in grids if int(row["t_sec"]) == t]
        assert len(cells) == 20
        assert sum(int(row["N"]) for row in cells) == 3
        populated = [row for row in cells if row["N"] != "0"]
        assert len(populated) == 1
        assert (populated[0]["N"], populated[0]["U"], populated[0]["L"],
                populated[0]["resolved"], populated[0]["loss_rate"]) == ("3", "1", "1", "2", "0.5")
        assert all(row["loss_rate"] == row["wilson_95_low"] == row["wilson_95_high"] == ""
                   for row in cells if row["N"] == "0")
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["verification"]["primary_cells_including_empty"] == 160
    assert manifest["cohort_market_count"] == 3
    assert manifest["artifacts_sha256"]["grids.csv"] == tabulate.sha256(output / "grids.csv")
    assert "0/0 = unavailable" in (output / "report.md").read_text(encoding="utf-8")


@pytest.mark.parametrize("spec", [{"k": "", "boundary_variants": "0", "k_received_ns": ""},
                                  {"w": "10000"}])
def test_zero_eligible_cohort_still_writes_complete_empty_grids_and_totals(tmp_path, spec):
    output = write_artifacts(tmp_path, make_export(spec))
    with (output / "grids.csv").open(newline="", encoding="utf-8") as handle:
        grids = list(csv.DictReader(handle))
    assert len(grids) == 160
    assert all(row["N"] == "0" and row["loss_rate"] == "" for row in grids)
    for filename, expected_rows in (("daily_totals.csv", 8), ("direction_totals.csv", 16),
                                    ("comparison_totals.csv", 16)):
        with (output / filename).open(newline="", encoding="utf-8") as handle:
            totals = list(csv.DictReader(handle))
        assert len(totals) == expected_rows
        assert all(row["N"] == "0" and row["loss_rate"] == "" for row in totals)
    assert (output / "report.md").is_file()


def test_observed_day_with_no_eligible_inputs_remains_in_daily_artifacts(tmp_path):
    output = write_artifacts(tmp_path, make_export({}, {"offset": 288, "k": "", "boundary_variants": "0",
                                                       "k_received_ns": ""}))
    with (output / "daily_totals.csv").open(newline="", encoding="utf-8") as handle:
        totals = list(csv.DictReader(handle))
    assert len(totals) == 16
    next_day = [row for row in totals if row["utc_market_start_date"] == "2026-09-02"]
    assert len(next_day) == 8
    assert all(row["N"] == "0" and row["loss_rate"] == "" for row in next_day)
    with (output / "daily_cells.csv").open(newline="", encoding="utf-8") as handle:
        cells = list(csv.DictReader(handle))
    assert len(cells) == 320
    assert sum(row["utc_market_start_date"] == "2026-09-02" for row in cells) == 160


def test_output_artifacts_cannot_be_overwritten(tmp_path):
    rows = make_export()
    output = write_artifacts(tmp_path, rows)
    report = (output / "report.md").read_bytes()
    with pytest.raises(ValueError, match="Output artifacts already exist"):
        tabulate.write_results(analyze(rows), output, tmp_path / "observations.csv", tmp_path / "extract.sql")
    assert (output / "report.md").read_bytes() == report


def prepare_cli_run(tmp_path, monkeypatch, extraction_overrides=None):
    rows = make_export({}, {"official_winner": "Down"})
    observations = tmp_path / "observations.csv"
    with observations.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    sql = tmp_path / "extract.sql"
    sql.write_text("-- frozen synthetic extraction\n", encoding="utf-8")
    output = tmp_path / "run"
    output.mkdir()
    start, end = tabulate.utc_text(START_MS), tabulate.utc_text(START_MS + 600_000)
    extraction = {"psql_exit_code": 0, "read_only": True, "isolation": "repeatable read",
                  "sql_sha256": tabulate.sha256(sql), "start_utc": start, "end_utc_exclusive": end}
    extraction.update(extraction_overrides or {})
    (output / "extraction.json").write_text(json.dumps(extraction), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["tabulate.py", str(observations), "--output", str(output),
                                     "--sql", str(sql), "--expected-start", start, "--expected-end", end,
                                     "--expected-sql-sha256", tabulate.sha256(sql),
                                     "--comparison-marker", tabulate.utc_text(START_MS + 300_000)])
    return output, extraction, sql


def test_main_accepts_successful_matching_extraction_and_labels_custom_marker_neutrally(tmp_path, monkeypatch, capsys):
    output, extraction, _ = prepare_cli_run(tmp_path, monkeypatch)
    tabulate.main()
    assert "Wrote 8 grids / 160 cells for 2 markets" in capsys.readouterr().out
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["extraction_process"] == extraction
    assert manifest["source_market_guard"]["result"] == (
        "passed before COPY in the extraction's repeatable-read read-only transaction"
    )
    marker = "2026-09-01T00:05:00.000Z"
    assert manifest["comparison_marker_utc"] == marker
    report = (output / "report.md").read_text(encoding="utf-8")
    assert f"The marker is **{marker}**, the requested descriptive split." in report
    assert f"The marker is **{marker}**, the first full market after deployment" not in report
    with (output / "comparison_totals.csv").open(newline="", encoding="utf-8") as handle:
        totals = list(csv.DictReader(handle))
    assert len(totals) == 16
    assert all(row["N"] == "1" for row in totals)
    assert all(row["L"] == ("0" if row["period"] == "pre" else "1") for row in totals)


@pytest.mark.parametrize("override", [
    {"psql_exit_code": 3},
    {"read_only": False},
    {"isolation": "read committed"},
    {"sql_sha256": "0" * 64},
    {"start_utc": "2026-08-31T23:55:00Z"},
    {"end_utc_exclusive": "2026-09-01T00:15:00Z"},
])
def test_main_rejects_failed_or_mismatched_extraction_before_writing_artifacts(tmp_path, monkeypatch, override):
    output, _, _ = prepare_cli_run(tmp_path, monkeypatch, override)
    with pytest.raises(ValueError, match="Extraction process record is unsuccessful or does not match this run"):
        tabulate.main()
    assert {path.name for path in output.iterdir()} == {"extraction.json"}


def test_main_rejects_sql_changed_after_the_recorded_extraction(tmp_path, monkeypatch):
    output, _, sql = prepare_cli_run(tmp_path, monkeypatch)
    sql.write_text("-- changed after extraction\n", encoding="utf-8")
    with pytest.raises(ValueError, match="SQL changed after extraction"):
        tabulate.main()
    assert {path.name for path in output.iterdir()} == {"extraction.json"}
