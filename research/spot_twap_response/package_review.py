"""Package small, public-market research evidence with hashes; omit raw exports."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import zipfile


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "results/spot_twap_response/2026-09-13-independent-review"
ALLOWED = {".py", ".sql", ".csv", ".json", ".jsonl", ".md", ".txt", ".gz"}
MAX_MEMBER_BYTES = 2_000_000


def main() -> None:
    candidates = [ROOT / "SPOT_TWAP_RESPONSE_STUDY.md",
                  ROOT / "GHOST_TWAP_LIVE_PLAN.md",
                  ROOT / "GHOST_TWAP_CHECKPOINT_A.md",
                  ROOT / "price_collector/ghost_twap.py",
                  ROOT / "price_collector/market.py",
                  ROOT / "price_collector/__init__.py",
                  ROOT / "H3_TWAP_LEADER_RISK_FINAL_REPORT.md",
                  ROOT / "RESEARCH_QUESTIONS.md"]
    candidates.extend((ROOT / "tests").glob("test_ghost_twap*.py"))
    # Explicit release scope also includes the small linked H3 findings. Avoid
    # recursively packaging the output ZIP or its own generated manifests.
    scope_path = ROOT / "research/spot_twap_response/release_scope.json"
    if scope_path.exists():
        scope = json.loads(scope_path.read_text(encoding="utf-8"))
        for item in scope["new_files_exact"]:
            path = (ROOT / item["path"]).resolve()
            if not path.is_relative_to(ROOT) or path.is_relative_to(OUTPUT):
                continue
            candidates.append(path)
    for folder in (ROOT / "research/spot_twap_response",
                   ROOT / "results/spot_twap_response/2026-09-13-pilot",
                   ROOT / "tests/fixtures/ghost_twap"):
        candidates.extend(folder.rglob("*"))
    included = []
    omitted = []
    for path in sorted(set(candidates)):
        if not path.is_file() or path.suffix not in ALLOWED or "__pycache__" in path.parts:
            continue
        relative = path.relative_to(ROOT).as_posix()
        size = path.stat().st_size
        if path.name in {"sources.csv", "observations.csv"} or size > MAX_MEMBER_BYTES:
            omitted.append({"path": relative, "bytes": size,
                            "reason": "raw export or large artifact; retain locally"})
            continue
        data = path.read_bytes()
        included.append((relative, data, hashlib.sha256(data).hexdigest()))

    OUTPUT.mkdir(parents=True, exist_ok=True)
    bundle = OUTPUT / "verification_artifacts.zip"
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative, data, _ in included:
            archive.writestr(relative, data)
    with zipfile.ZipFile(bundle) as archive:
        assert archive.testzip() is None
        for relative, _, digest in included:
            assert hashlib.sha256(archive.read(relative)).hexdigest() == digest
    manifest = {
        "bundle": bundle.name,
        "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
        "archive_paths": "repository-relative; extract together to preserve report links",
        "included": [{"path": name, "bytes": len(data), "sha256": digest}
                     for name, data, digest in included],
        "omitted": omitted,
        "verification": "All archive members re-read and SHA-256 matched; ZIP CRC passed.",
        "scope": "Evidence package; not a new statistical analysis or deployment bundle.",
    }
    (OUTPUT / "bundle_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"bundle": str(bundle), "included_files": len(included),
                      "omitted_files": len(omitted), "bytes": bundle.stat().st_size}))


if __name__ == "__main__":
    main()
