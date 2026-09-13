from __future__ import annotations

from decimal import Decimal, localcontext
import csv
import json
from pathlib import Path
import tempfile
import unittest

from research.spot_twap_response import leader_risk_companion as companion


def market_rows(market: int = 1, **changes) -> list[dict[str, str]]:
    rows = []
    for t in companion.CHECKPOINTS:
        row = {"market_id": str(market), "t_sec": str(t), "cut_ms": str((market+1)*300000-t*1000),
               "projected_price": "10002", "k": "10000", "s": "10003", "w": "10001",
               "official_winner": "Up", "projection_available": "True", "k_available": "True",
               "s_available": "True", "w_available": "True", "primary_paired_available": "True",
               "fresh_3s_paired_available": "True", "original_extra": "preserve me"}
        row.update(changes)
        rows.append(row)
    return rows


def coverage(audit: dict, basis: str, panel: str = "primary", t: int = 30) -> dict:
    return next(row for row in audit["coverage"] if (row["basis"], row["panel"], row["t_sec"]) == (basis, panel, t))


class CoordinatesTests(unittest.TestCase):
    def test_exact_decimal_precision_under_low_ambient_context(self):
        with localcontext() as context:
            context.prec = 6
            point = companion.coordinates(Decimal("10000"), Decimal("10000.000000000000000001"), Decimal("10000.000000000000000002"))
        self.assertEqual(point["signed_lead_bps"], Decimal("0.000000000000000001"))
        self.assertEqual(point["x"], Decimal("0.000000000000000001"))
        self.assertEqual(point["y"], Decimal("0.000000000000000001"))

    def test_bin_boundaries_for_both_directions(self):
        for direction in (Decimal(1), Decimal(-1)):
            for x, xi in (("0.999999", 0), ("1", 1), ("2", 2), ("4", 3), ("8", 4)):
                for y, yi in (("-2.000001", 0), ("-2", 1), ("0", 2), ("2", 3)):
                    with self.subTest(direction=direction, x=x, y=y):
                        reference = Decimal(10000)+direction*Decimal(x)
                        spot = reference+direction*Decimal(y)
                        point = companion.coordinates(Decimal(10000), reference, spot)
                        self.assertEqual((point["xi"], point["yi"]), (xi, yi))
                        self.assertEqual(point["x"], Decimal(x))
                        self.assertEqual(point["y"], Decimal(y))

    def test_tie_has_no_direction_or_y_bin(self):
        point = companion.coordinates(Decimal(10000), Decimal(10000), Decimal(10050))
        self.assertEqual(point["leader"], "Tie")
        self.assertEqual(point["signed_lead_bps"], 0)
        self.assertEqual(point["x"], 0)
        self.assertIsNone(point["y"])
        self.assertIsNone(point["xi"])
        self.assertIsNone(point["yi"])

    def test_invalid_prices_rejected(self):
        for bad in (Decimal(0), Decimal(-1), Decimal("NaN"), Decimal("Infinity")):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                companion.coordinates(Decimal(10000), bad, Decimal(10001))


class GridTests(unittest.TestCase):
    def test_opposite_leaders_have_explicit_feature_bases(self):
        rows = market_rows(w="10002", projected_price="9997", s="9996")
        audit = companion.build_companion(rows)
        feature = audit["features"][0]
        self.assertEqual(feature["raw_twap_leader"], "Up")
        self.assertEqual(feature["projected_leader"], "Down")
        self.assertEqual(Decimal(feature["Xraw_bps"]), Decimal(2))
        self.assertEqual(Decimal(feature["Yraw_bps"]), Decimal(-6))
        self.assertEqual(Decimal(feature["projected_lead_bps"]), Decimal(-3))
        self.assertEqual(Decimal(feature["Xproj_bps"]), Decimal(3))
        self.assertEqual(Decimal(feature["Yproj_bps"]), Decimal(1))
        self.assertEqual(coverage(audit, "raw_twap")["grid_L"], 0)
        self.assertEqual(coverage(audit, "projected_close")["grid_L"], 1)
        self.assertEqual(feature["w"], "10002")
        self.assertEqual(feature["projected_price"], "9997")
        for original, attached in zip(rows, audit["features"]):
            self.assertEqual({key: attached[key] for key in original}, original)

    def test_basis_ties_excluded_separately_and_unknown_ties_preserved(self):
        rows = (market_rows(1, w="10000")
                + market_rows(2, projected_price="10000", w="9999", official_winner="Down")
                + market_rows(3, projected_price="10000", w="10000", official_winner=""))
        audit = companion.build_companion(rows)
        for basis in companion.BASES:
            result = coverage(audit, basis)
            self.assertEqual(result["initial_paired_N"], 3)
            self.assertEqual(result["initial_paired_U"], 1)
            self.assertEqual(result["ties_N"], 2)
            self.assertEqual(result["ties_U"], 1)
            self.assertEqual(result["grid_N"], 1)
            self.assertEqual(result["grid_U"], 0)
            self.assertEqual(result["grid_L"], 0)

    def test_unknown_outcomes_are_not_counted_as_resolved_losses(self):
        rows = market_rows(1, official_winner="")+market_rows(2, official_winner="Up")+market_rows(3, official_winner="Down")
        audit = companion.build_companion(rows)
        for basis in companion.BASES:
            result = coverage(audit, basis)
            self.assertEqual((result["grid_N"], result["grid_U"], result["grid_L"], result["grid_n"]), (3, 1, 1, 2))
            self.assertEqual(result["leader_loss_rate"], Decimal("0.5"))

    def test_each_panel_has_its_own_shared_initial_cohort(self):
        rows = (market_rows(1)+market_rows(2, fresh_3s_paired_available="False")
                + market_rows(3, primary_paired_available="False", fresh_3s_paired_available="False"))
        audit = companion.build_companion(rows)
        for basis in companion.BASES:
            self.assertEqual(coverage(audit, basis, "primary")["initial_paired_N"], 2)
            self.assertEqual(coverage(audit, basis, "fresh_3s")["initial_paired_N"], 1)

    def test_full_domains_empty_cells_and_count_conservation(self):
        audit = companion.build_companion(market_rows(1)+market_rows(2, w="10000", official_winner=""))
        self.assertEqual(len(audit["grids"]), 2*6*2*5*4)
        self.assertEqual(len(audit["coverage"]), 2*6*2)
        for cov in audit["coverage"]:
            cells = [row for row in audit["grids"] if all(row[key] == cov[key] for key in ("panel", "t_sec", "basis"))]
            for field in ("N", "U", "L", "n"):
                self.assertEqual(sum(row[field] for row in cells), cov[f"grid_{field}"])
            self.assertEqual(cov["grid_N"]+cov["ties_N"], cov["initial_paired_N"])
            self.assertEqual(cov["grid_U"]+cov["ties_U"], cov["initial_paired_U"])
        self.assertTrue(any(row["N"] == 0 and row["leader_loss_rate"] is None for row in audit["grids"]))

    def test_no_resolved_outcomes_has_blank_rate(self):
        audit = companion.build_companion(market_rows(official_winner=""))
        for row in audit["grids"]:
            self.assertEqual(row["n"], 0)
            self.assertIsNone(row["leader_loss_rate"])


class ValidationTests(unittest.TestCase):
    def test_missing_or_partial_export_rejected(self):
        for rows in ([], market_rows()[:-1]):
            with self.subTest(rows=len(rows)), self.assertRaises(ValueError):
                companion.build_companion(rows)

    def test_duplicate_key_rejected(self):
        rows = market_rows()
        with self.assertRaises(ValueError):
            companion.build_companion(rows+[rows[0]])

    def test_cutoff_must_belong_to_market(self):
        rows = market_rows()
        rows[0]["cut_ms"] = str(int(rows[0]["cut_ms"])+1)
        with self.assertRaises(ValueError):
            companion.build_companion(rows)

    def test_missing_prices_or_inconsistent_pair_flags_rejected(self):
        for changes in ({"s": ""}, {"projection_available": "False"},
                        {"primary_paired_available": "False", "fresh_3s_paired_available": "True"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                companion.build_companion(market_rows(**changes))

    def test_explicit_unavailable_rows_stay_in_feature_export(self):
        rows = market_rows(s="", s_available="False", primary_paired_available="False", fresh_3s_paired_available="False")
        audit = companion.build_companion(rows)
        self.assertEqual(len(audit["features"]), 6)
        self.assertTrue(all(row["initial_paired_N"] == 0 for row in audit["coverage"]))
        self.assertTrue(all(Decimal(row["projected_lead_bps"]) == Decimal(2) for row in audit["features"]))
        self.assertTrue(all(row["Yproj_bps"] == "" for row in audit["features"]))


class ArtifactTests(unittest.TestCase):
    def make_export(self, root: Path) -> tuple[Path, Path]:
        directory = root/"receipt_clock_pilot"/"results"
        directory.mkdir(parents=True)
        source = directory/"rows.csv"
        rows = market_rows()
        with source.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        metadata = {"status": "accepted", "rows": 6, "markets": 1,
                    "checkpoint_seconds": list(companion.CHECKPOINTS),
                    "cohort_start_ms": 300000, "cohort_end_ms_exclusive": 600000,
                    "shift_seconds_a": -3, "snapshot_ms": 900000,
                    "artifacts_sha256": {"rows.csv": companion.sha256(source)}}
        manifest = directory/"manifest.json"
        manifest.write_text(json.dumps(metadata), encoding="utf-8")
        return source, manifest

    def test_artifacts_preserve_inputs_hashes_and_refuse_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            source, manifest = self.make_export(Path(temp))
            source_hash, manifest_hash = companion.sha256(source), companion.sha256(manifest)
            result = companion.run_companion(source, manifest)
            output = source.parent.parent/"leader_risk_companion"
            self.assertTrue((output/"features.csv").exists())
            self.assertFalse((source.parent/"leader_risk_companion").exists())
            self.assertEqual(result["input_sha256"], source_hash)
            self.assertEqual(result["source_manifest_sha256"], manifest_hash)
            self.assertEqual(companion.sha256(source), source_hash)
            self.assertEqual(companion.sha256(manifest), manifest_hash)
            for name, expected_hash in result["artifacts_sha256"].items():
                self.assertEqual(companion.sha256(output/name), expected_hash)
            with self.assertRaises(ValueError):
                companion.run_companion(source, manifest)

    def test_source_hash_and_manifest_constraints_fail_before_output_creation(self):
        cases = (
            {"status": "failed"}, {"rows": 12}, {"markets": 2},
            {"checkpoint_seconds": [30]}, {"cohort_start_ms": 600000},
            {"shift_seconds_a": -2}, {"snapshot_ms": None},
            {"artifacts_sha256": {"rows.csv": "wrong"}},
        )
        for changes in cases:
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as temp:
                source, manifest = self.make_export(Path(temp))
                metadata = json.loads(manifest.read_text(encoding="utf-8"))
                metadata.update(changes)
                manifest.write_text(json.dumps(metadata), encoding="utf-8")
                with self.assertRaises(ValueError):
                    companion.run_companion(source, manifest)
                self.assertFalse((source.parent.parent/"leader_risk_companion").exists())

    def test_missing_data_is_not_executed(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)/"receipt_clock_pilot"/"results"
            directory.mkdir(parents=True)
            with self.assertRaises(FileNotFoundError):
                companion.run_companion(directory/"rows.csv", directory/"manifest.json")
            self.assertFalse((directory.parent/"leader_risk_companion").exists())

    def test_unrelated_paths_cannot_receive_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            with self.assertRaises(ValueError):
                companion.run_companion(directory/"observations.csv", directory/"manifest.json")


if __name__ == "__main__":
    unittest.main()
