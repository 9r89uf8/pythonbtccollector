"""Split audited endpoint side changes by market boundary; no database access."""

from pathlib import Path
from decimal import Decimal
import csv
import hashlib
import json


def main():
    folder = Path(__file__).resolve().parent
    source = folder / "anchor_targets.csv"
    audit = json.loads((folder / "audit_manifest.json").read_text())
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    assert audit["status"] == "accepted"
    assert digest == audit["files_sha256"][source.name]
    counts = {}
    with source.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if not row["w"] or row["target_events"] == "0" or row["resolution_status"] != "resolved":
                continue
            assert row["target_k"]
            assert row["target_min_price"] == row["target_max_price"]
            key = (int(row["h"]), row["baseline_target_cross_market"] == "t")
            group = counts.setdefault(key, {"n": 0, "side_changes": 0})
            group["n"] += 1
            strike = Decimal(row["target_k"])
            group["side_changes"] += (
                (Decimal(row["twap_now"]) >= strike)
                != (Decimal(row["target_min_price"]) >= strike)
            )
    with (folder / "summary.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            horizon = int(row["h_s"])
            assert sum(counts[(horizon, b)]["n"] for b in (False, True)) == int(row["n"])
            assert sum(counts[(horizon, b)]["side_changes"] for b in (False, True)) == int(row["side_changes"])
    result = {
        "status": "accepted",
        "source_sha256": digest,
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "Endpoint side changes against the target market's reconciled strike; no ghost recalculation or causal-strike validation",
        "rows": [
            {"h_s": h, "baseline_target_cross_market": b, **values}
            for (h, b), values in sorted(counts.items())
        ],
    }
    (folder / "boundary_side_summary.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result["rows"], indent=2))


if __name__ == "__main__":
    main()
