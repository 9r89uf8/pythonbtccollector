"""Separate Decimal descriptive grids for raw TWAP and projected close.

No database access or production imports. The input is a completed receipt-clock
pilot export. Run after the completed receipt replay and its 30-second comparison
have been reviewed.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

CHECKPOINTS = (60, 30, 15, 10, 5, 3)
PRECISION = 80
X_EDGES = tuple(Decimal(value) for value in (1, 2, 4, 8))
Y_EDGES = tuple(Decimal(value) for value in (-2, 0, 2))
X_LABELS = ("[0,1)", "[1,2)", "[2,4)", "[4,8)", "[8,infinity)")
Y_LABELS = ("<-2", "[-2,0)", "[0,2)", ">=2")
PANELS = {"primary": "primary_paired_available", "fresh_3s": "fresh_3s_paired_available"}
BASES = ("raw_twap", "projected_close")
REQUIRED = ("market_id", "t_sec", "cut_ms", "projected_price", "k", "s", "w",
            "official_winner", "projection_available", "k_available", "s_available",
            "w_available", "primary_paired_available", "fresh_3s_paired_available")
FEATURE_COLUMNS = (
    "projected_lead_bps", "projected_leader", "Xproj_bps", "Yproj_bps",
    "projected_x_bin", "projected_y_bin", "raw_twap_lead_bps", "raw_twap_leader",
    "Xraw_bps", "Yraw_bps", "raw_twap_x_bin", "raw_twap_y_bin",
)


def boolean(value: str | bool) -> bool:
    if value is True or value in ("t", "true", "True"):
        return True
    if value is False or value in ("f", "false", "False"):
        return False
    raise ValueError(f"Expected an explicit boolean, got {value!r}")


def price(value: str) -> Decimal | None:
    if value == "":
        return None
    number = Decimal(value)
    if not number.is_finite() or number <= 0:
        raise ValueError("Prices must be positive finite Decimal values")
    return number


def coordinates(k: Decimal, reference: Decimal, spot: Decimal | None) -> dict:
    if any(not value.is_finite() or value <= 0 for value in (k, reference, spot) if value is not None):
        raise ValueError("Coordinates require positive finite prices")
    with localcontext() as context:
        context.prec = PRECISION
        signed = Decimal(10000)*(reference-k)/k
        if reference == k:
            return {"signed_lead_bps": Decimal(0), "leader": "Tie", "x": Decimal(0),
                    "y": None, "xi": None, "yi": None}
        direction = Decimal(1) if reference > k else Decimal(-1)
        x = abs(signed)
        y = direction*Decimal(10000)*(spot-reference)/k if spot is not None else None
        return {"signed_lead_bps": signed, "leader": "Up" if direction == 1 else "Down",
                "x": x, "y": y, "xi": sum(x >= edge for edge in X_EDGES),
                "yi": sum(y >= edge for edge in Y_EDGES) if y is not None else None}


@dataclass
class Counts:
    N: int = 0
    U: int = 0
    L: int = 0

    def add(self, winner: str, leader: str | None = None) -> None:
        self.N += 1
        if not winner:
            self.U += 1
        elif leader is not None and winner != leader:
            self.L += 1

    def record(self) -> dict:
        n = self.N-self.U
        with localcontext() as context:
            context.prec = PRECISION
            return {"N": self.N, "U": self.U, "L": self.L, "n": n,
                    "leader_loss_rate": Decimal(self.L)/Decimal(n) if n else None}


def validate_rows(rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError("Cannot analyze a missing or empty receipt export")
    seen, market_cuts = set(), defaultdict(set)
    for row in rows:
        if any(key not in row for key in REQUIRED):
            raise ValueError("Missing required receipt export columns")
        if any(key in row for key in FEATURE_COLUMNS):
            raise ValueError("Input already contains companion feature columns")
        market, t, cut = int(row["market_id"]), int(row["t_sec"]), int(row["cut_ms"])
        if t not in CHECKPOINTS or cut != (market+1)*300000-t*1000:
            raise ValueError("Invalid market/checkpoint cutoff")
        if (market, t) in seen:
            raise ValueError("Duplicate market/checkpoint key")
        seen.add((market, t))
        market_cuts[market].add(t)
        if row["official_winner"] not in ("", "Up", "Down"):
            raise ValueError("Official winner must be Up, Down, or blank for unknown")
        parsed = {name: price(row[name]) for name in ("projected_price", "k", "s", "w")}
        flags = {name: boolean(row[name]) for name in REQUIRED if name.endswith("_available")}
        for flag, value in (("projection_available", "projected_price"), ("k_available", "k"),
                            ("s_available", "s"), ("w_available", "w")):
            if flags[flag] and parsed[value] is None:
                raise ValueError(f"{flag} has no corresponding price")
        if flags["primary_paired_available"] and not all(flags[name] for name in (
                "projection_available", "k_available", "s_available", "w_available")):
            raise ValueError("Primary paired row has an unavailable input")
        if flags["fresh_3s_paired_available"] and not flags["primary_paired_available"]:
            raise ValueError("Fresh paired cohort must be a subset of primary paired cohort")
    if any(cuts != set(CHECKPOINTS) for cuts in market_cuts.values()):
        raise ValueError("Every exported market must contain all six checkpoints")


def build_companion(rows: list[dict[str, str]]) -> dict:
    validate_rows(rows)
    cells = defaultdict(Counts)
    initial, ties, included = defaultdict(Counts), defaultdict(Counts), defaultdict(Counts)
    features = []
    for row in rows:
        winner, t = row["official_winner"], int(row["t_sec"])
        k, s, w, projection = (price(row[name]) for name in ("k", "s", "w", "projected_price"))
        coordinate = {}
        feature = dict(row)
        feature.update({key: "" for key in FEATURE_COLUMNS})
        for basis, reference, available, names in (
            ("raw_twap", w, "w_available", ("raw_twap_lead_bps", "raw_twap_leader", "Xraw_bps", "Yraw_bps", "raw_twap_x_bin", "raw_twap_y_bin")),
            ("projected_close", projection, "projection_available", ("projected_lead_bps", "projected_leader", "Xproj_bps", "Yproj_bps", "projected_x_bin", "projected_y_bin")),
        ):
            if all(boolean(row[name]) for name in (available, "k_available")):
                point = coordinates(k, reference, s if boolean(row["s_available"]) else None)
                coordinate[basis] = point
                values = (point["signed_lead_bps"], point["leader"], point["x"], point["y"],
                          X_LABELS[point["xi"]] if point["xi"] is not None else None,
                          Y_LABELS[point["yi"]] if point["yi"] is not None else None)
                feature.update({name: "" if value is None else str(value) for name, value in zip(names, values)})
        features.append(feature)
        for panel, flag in PANELS.items():
            if not boolean(row[flag]):
                continue
            initial[panel, t].add(winner)
            for basis in BASES:
                point = coordinate[basis]
                if point["leader"] == "Tie":
                    ties[panel, t, basis].add(winner)
                    continue
                included[panel, t, basis].add(winner, point["leader"])
                cells[panel, t, basis, point["xi"], point["yi"]].add(winner, point["leader"])
    grids, coverage = [], []
    for panel in PANELS:
        for t in CHECKPOINTS:
            for basis in BASES:
                start, tied, kept = initial[panel, t], ties[panel, t, basis], included[panel, t, basis]
                if kept.N+tied.N != start.N or kept.U+tied.U != start.U:
                    raise ValueError("Tie exclusions do not conserve the paired cohort")
                coverage.append({"panel": panel, "t_sec": t, "basis": basis,
                                 "initial_paired_N": start.N, "initial_paired_U": start.U,
                                 "ties_N": tied.N, "ties_U": tied.U, "ties_resolved_n": tied.N-tied.U,
                                 "grid_N": kept.N, "grid_U": kept.U, "grid_L": kept.L,
                                 "grid_n": kept.N-kept.U, "leader_loss_rate": kept.record()["leader_loss_rate"]})
                for xi, x_label in enumerate(X_LABELS):
                    for yi, y_label in enumerate(Y_LABELS):
                        grids.append({"panel": panel, "t_sec": t, "basis": basis,
                                      "x_bin": x_label, "y_bin": y_label,
                                      **cells[panel, t, basis, xi, yi].record()})
    return {"features": features, "grids": grids, "coverage": coverage}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def validate_source_manifest(manifest: dict, input_path: Path, rows: list[dict[str, str]]) -> None:
    if manifest.get("status") != "accepted":
        raise ValueError("Receipt pilot manifest must be accepted")
    if manifest.get("artifacts_sha256", {}).get("rows.csv") != sha256(input_path):
        raise ValueError("Receipt row export does not match its manifest hash")
    if manifest.get("rows") != len(rows) or manifest.get("markets") != len({row["market_id"] for row in rows}):
        raise ValueError("Receipt row/market counts do not match its manifest")
    if tuple(manifest.get("checkpoint_seconds", ())) != CHECKPOINTS:
        raise ValueError("Receipt manifest has different checkpoints")
    start, end = manifest.get("cohort_start_ms"), manifest.get("cohort_end_ms_exclusive")
    if (type(start) is not int or type(end) is not int or start % 300000
            or end % 300000 or end <= start):
        raise ValueError("Invalid receipt manifest cohort boundaries")
    if any(not start <= int(row["market_id"])*300000 < end for row in rows):
        raise ValueError("Receipt market lies outside its declared cohort")
    if manifest.get("shift_seconds_a") != -3:
        raise ValueError("This companion expects the frozen a=-3 receipt pilot")
    snapshot = manifest.get("snapshot_ms")
    if snapshot is None or int(snapshot) < end:
        raise ValueError("Missing or invalid receipt snapshot clock")


def run_companion(input_path: Path, source_manifest_path: Path) -> dict:
    """Output is confined to receipt_clock_pilot/leader_risk_companion/."""
    input_path, source_manifest_path = input_path.resolve(), source_manifest_path.resolve()
    if (input_path.name != "rows.csv" or input_path.parent.name != "results"
            or input_path.parent.parent.name != "receipt_clock_pilot"
            or source_manifest_path != input_path.parent/"manifest.json"):
        raise ValueError("Use receipt_clock_pilot/results/rows.csv and its manifest.json")
    output = input_path.parent.parent/"leader_risk_companion"
    if output.exists():
        raise ValueError("Refusing to overwrite an existing companion directory")
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    with input_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        original_fields = reader.fieldnames
        rows = list(reader)
    audit = build_companion(rows)
    validate_source_manifest(source_manifest, input_path, rows)
    output.mkdir()
    write_csv(output/"features.csv", audit["features"], original_fields+list(FEATURE_COLUMNS))
    write_csv(output/"grids.csv", audit["grids"])
    write_csv(output/"coverage.csv", audit["coverage"])
    description = """# Receipt-clock leader-risk companion

These descriptive grids use one initial paired cohort per panel and checkpoint.
The raw_twap basis uses the current received TWAP. The projected_close basis
uses the continuation forecast of the closing price. Each basis defines its own
leader, X and Y, so a row may change direction or cell. Projected close is never
relabeled as the current TWAP. Original leader-risk artifacts remain unchanged.

X is the absolute reference-price lead over the decision-time strike in basis
points. Y is leader-signed spot minus the selected reference, divided by that
strike and expressed in basis points. Exact reference=strike ties have no leader;
they are excluded separately for each basis and counted in coverage.csv.

N includes unknown official outcomes U; resolved n=N-U; L is the number of
resolved outcomes in which that basis's leader loses. Loss rate is L/n, blank
when n=0. Empty grid cells remain explicit. There are no confidence intervals,
profitability tests, execution assumptions, or mispricing claims here.

Panel membership is inherited from the validated receipt pilot's primary and
fresh_3s paired availability flags. Coverage before each basis's tie exclusions
is identical. This companion does not independently reconstruct source arrivals.
"""
    (output/"README.md").write_text(description, encoding="utf-8", newline="\n")
    manifest = {
        "status": "accepted", "created_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path), "input_sha256": sha256(input_path),
        "source_manifest_path": str(source_manifest_path), "source_manifest_sha256": sha256(source_manifest_path),
        "source_manifest": source_manifest, "code_sha256": sha256(Path(__file__)),
        "input_rows": len(rows), "markets": len({row["market_id"] for row in rows}),
        "checkpoints": CHECKPOINTS, "panels": PANELS, "bases": BASES,
        "decimal_precision": PRECISION, "x_edges_bps": [str(edge) for edge in X_EDGES],
        "y_edges_bps": [str(edge) for edge in Y_EDGES],
        "feature_formulas": {"projected_lead_bps": "10000*(projected_price-k)/k",
                             "Xproj_bps": "abs(projected_lead_bps)",
                             "Yproj_bps": "sign(projected_price-k)*10000*(s-projected_price)/k",
                             "Xraw_bps": "abs(10000*(w-k)/k)",
                             "Yraw_bps": "sign(w-k)*10000*(s-w)/k"},
        "statistic": "Descriptive leader loss rate L/(N-U); no confidence intervals",
        "artifacts_sha256": {name: sha256(output/name) for name in ("features.csv", "grids.csv", "coverage.csv", "README.md")},
    }
    (output/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8", newline="\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--source-manifest", type=Path, required=True)
    args = parser.parse_args()
    manifest = run_companion(args.input, args.source_manifest)
    print(json.dumps({"status": manifest["status"], "input_rows": manifest["input_rows"], "markets": manifest["markets"]}))


if __name__ == "__main__":
    main()
