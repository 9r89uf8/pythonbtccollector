# Shadow-Signal Policy v1 to v2 Migration Commands

Run these commands as `root` on the droplet. Keep the same SSH shell open so
the variables remain available between steps.

## 1. Deploy policy v2

Run this only after the policy-v2 change has been pushed to GitHub.

```bash
cd /opt/price-collector && \
sudo -u pricecollector git pull --ff-only && \
sudo -u pricecollector .venv/bin/pip install -r requirements.txt && \
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal_replay.py \
  tests/test_shadow_signal_selection.py
```

No service restart is required.
Do not continue unless `git pull`, dependency installation, and both focused
test files complete successfully.

## 2. List the existing replay and selection reports

```bash
sudo find /var/lib/price-collector -maxdepth 1 -type f \
  \( -name 'shadow-replay-*.json' -o -name 'shadow-primary-selection-*.json' \) \
  -printf '%TY-%Tm-%Td %TH:%TM  %s bytes  %p\n' | sort

sudo -u pricecollector /opt/price-collector/.venv/bin/python - <<'PY'
import hashlib
import json
from pathlib import Path

root = Path("/var/lib/price-collector")
for path in sorted(root.glob("shadow-*.json")):
    raw = path.read_bytes()
    try:
        report = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print({"path": str(path), "error": str(error)})
        continue
    mode = report.get("mode")
    if mode == "shadow_raw_replay":
        print({
            "path": str(path),
            "type": "replay",
            "sha256": hashlib.sha256(raw).hexdigest(),
            "status": report.get("status"),
            "range": report.get("range"),
        })
    elif mode == "shadow_primary_selection":
        print({
            "path": str(path),
            "type": "selection",
            "policy": report.get("policy", {}).get("version"),
            "status": report.get("status"),
            "evidence": report.get("provenance", {}).get("reports"),
        })
PY
```

Match the v1 selection artifact's calibration and holdout SHA-256 values to
the replay files printed above. This identifies the exact two replay paths.

## 3. Assign the two reports that were already inspected under v1

The first path below is the tuned calibration report from the previous run.
Replace only the second path with the holdout report used for the v1 decision.
Both paths must point to replay reports, not a selection artifact.

```bash
V1_CALIBRATION_REPORT="/var/lib/price-collector/shadow-replay-calibration-tuned-1783712058928-1783798458928.json"
V1_INSPECTED_HOLDOUT_REPORT="/var/lib/price-collector/REPLACE-WITH-INSPECTED-V1-HOLDOUT.json"

V1_PATHS_READY=0
if sudo test -f "$V1_CALIBRATION_REPORT" && \
   sudo test -f "$V1_INSPECTED_HOLDOUT_REPORT"; then
  V1_PATHS_READY=1
  printf 'v1 calibration: %s\nv1 inspected holdout: %s\n' \
    "$V1_CALIBRATION_REPORT" \
    "$V1_INSPECTED_HOLDOUT_REPORT"
else
  echo 'STOP: replace the report paths, then repeat step 3.'
fi
```

Do not continue until both `test` commands return successfully.

## 4. Validate the old reports and calculate the end of inspected evidence

```bash
V1_REPORTS_READY=0
if [ "${V1_PATHS_READY:-0}" -ne 1 ]; then
  echo 'STOP: step 3 did not validate both report paths.'
elif V1_EVIDENCE_END_MS="$(
    sudo -u pricecollector /opt/price-collector/.venv/bin/python - \
      "$V1_CALIBRATION_REPORT" \
      "$V1_INSPECTED_HOLDOUT_REPORT" <<'PY'
import json
import sys
from decimal import Decimal

frozen_settings = {
    "futures_stale_ms": 3000,
    "chainlink_stale_ms": 2500,
    "reference_max_gap_ms": 3000,
    "history_retention_ms": 10000,
}
reports = []
for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as stream:
        report = json.load(stream)
    if report.get("schema_version") != 2:
        raise SystemExit(f"STOP: {path} is not replay schema version 2")
    if report.get("mode") != "shadow_raw_replay":
        raise SystemExit(f"STOP: {path} is not a replay report")
    if report.get("status") != "ok":
        raise SystemExit(f"STOP: {path} status is {report.get('status')!r}")
    for name, expected in frozen_settings.items():
        observed = report["configuration"].get(name)
        if observed != expected:
            raise SystemExit(
                f"STOP: {path} has {name}={observed!r}; expected {expected}"
            )
    for candidate in report["candidates"]:
        common = candidate["common_cohort"]
        target_eligible = Decimal(common["target_eligible"])
        valid_generated = Decimal(common["valid_generated"])
        scored = Decimal(common["scored"])
        valid_coverage = valid_generated / target_eligible
        maturation_coverage = scored / valid_generated
        if (
            scored < 10_000
            or valid_coverage < Decimal("0.50")
            or maturation_coverage < Decimal("0.99")
        ):
            raise SystemExit(
                f"STOP: {path} fails policy-v2 evidence gates for "
                f"{candidate['model_version']}"
            )
    reports.append(report)

if reports[0]["configuration"] != reports[1]["configuration"]:
    raise SystemExit("STOP: the two old reports use different configurations")

reports.sort(key=lambda item: item["range"]["start_ms"])
for previous, current in zip(reports, reports[1:]):
    if previous["range"]["end_ms"] > current["range"]["start_ms"]:
        raise SystemExit("STOP: the two old reports overlap")

print(max(report["range"]["end_ms"] for report in reports))
PY
)" && [ -n "$V1_EVIDENCE_END_MS" ]; then
  V1_REPORTS_READY=1
  printf 'last inspected evidence ends at: %s\n' "$V1_EVIDENCE_END_MS"
else
  unset V1_EVIDENCE_END_MS
  echo 'STOP: old-report validation failed. Fix it before step 5.'
fi
```

## 5. Recalculate the latest completed common endpoint

```bash
V2_ENDPOINT_READY=0
if [ "${V1_REPORTS_READY:-0}" -ne 1 ]; then
  echo 'STOP: step 4 did not validate the old evidence.'
else
  LATEST_COMPLETED_MS="$(sudo -u postgres psql -At -d price_collector -c "
SELECT LEAST(
    max(disconnected_wall_ns) FILTER (
        WHERE source = 'binance_futures_agg_trade'
    ),
    max(disconnected_wall_ns) FILTER (
        WHERE source = 'polymarket_chainlink_rtds'
    )
) / 1000000
FROM raw_capture.feed_sessions
WHERE disconnected_wall_ns IS NOT NULL;
")"
  if [ -z "$LATEST_COMPLETED_MS" ]; then
    echo 'STOP: no completed common endpoint is available.'
  else
    AVAILABLE_AFTER_V1_MS="$((LATEST_COMPLETED_MS - V1_EVIDENCE_END_MS))"
    python3 - "$AVAILABLE_AFTER_V1_MS" <<'PY'
import sys

available_ms = int(sys.argv[1])
print({
    "strictly_later_completed_hours": round(available_ms / 3_600_000, 2),
    "enough_for_24_hour_holdout": available_ms >= 86_400_000,
})
PY
    if [ "$AVAILABLE_AFTER_V1_MS" -ge 86400000 ]; then
      V2_ENDPOINT_READY=1
    else
      echo 'STOP: wait for more completed data, then repeat step 5.'
    fi
  fi
fi
```

If `enough_for_24_hour_holdout` is `False`, stop here. Let both collectors
continue running and repeat step 5 after later sessions have completed.

## 6. Define the new untouched 24-hour holdout

Run this only when step 5 prints `enough_for_24_hour_holdout: True`.

```bash
V2_RANGE_READY=0
if [ "${V2_ENDPOINT_READY:-0}" -ne 1 ]; then
  echo 'STOP: step 5 did not find 24 strictly-later completed hours.'
else
  V2_HOLDOUT_END_MS="$LATEST_COMPLETED_MS"
  V2_HOLDOUT_START_MS="$((V2_HOLDOUT_END_MS - 86400000))"
  if [ "$V2_HOLDOUT_START_MS" -lt "$V1_EVIDENCE_END_MS" ]; then
    echo 'STOP: the calculated holdout overlaps inspected v1 evidence.'
  else
    V2_RANGE_READY=1
    V2_NEW_HOLDOUT_REPORT="/var/lib/price-collector/shadow-replay-holdout-v2-${V2_HOLDOUT_START_MS}-${V2_HOLDOUT_END_MS}.json"
    V2_SELECTION_REPORT="/var/lib/price-collector/shadow-primary-selection-chronological-holdout-v2-${V2_HOLDOUT_END_MS}.json"
    printf 'new holdout start: %s\nnew holdout end: %s\nreplay report: %s\nselection report: %s\n' \
      "$V2_HOLDOUT_START_MS" \
      "$V2_HOLDOUT_END_MS" \
      "$V2_NEW_HOLDOUT_REPORT" \
      "$V2_SELECTION_REPORT"
  fi
fi
```

## 7. Generate the new untouched holdout with the frozen replay settings

The replay normally prints nothing while it runs.

```bash
V2_HOLDOUT_FILE_READY=0
if [ "${V2_RANGE_READY:-0}" -ne 1 ]; then
  echo 'STOP: step 6 did not define a safe holdout range.'
elif sudo test -e "$V2_NEW_HOLDOUT_REPORT"; then
  V2_HOLDOUT_FILE_READY=1
  echo 'Replay file already exists; it will not be overwritten.'
  echo 'Continue to step 8 to validate that existing file.'
else
  REPLAY_EXIT=0
  sudo -u pricecollector \
    env START_MS="$V2_HOLDOUT_START_MS" \
        END_MS="$V2_HOLDOUT_END_MS" \
        REPORT="$V2_NEW_HOLDOUT_REPORT" \
    bash -c '
      set -a
      . /etc/price-collector/collector.env
      set +a
      cd /opt/price-collector
      exec .venv/bin/python -m price_collector.shadow_signal_replay \
        --start-ms "$START_MS" \
        --end-ms "$END_MS" \
        --futures-stale-ms 3000 \
        --chainlink-stale-ms 2500 \
        --reference-max-gap-ms 3000 \
        --history-retention-ms 10000 \
        --output "$REPORT"
    ' || REPLAY_EXIT=$?

  printf 'replay_exit=%s\n' "$REPLAY_EXIT"
  if [ "$REPLAY_EXIT" -eq 0 ] && \
     sudo test -f "$V2_NEW_HOLDOUT_REPORT"; then
    V2_HOLDOUT_FILE_READY=1
    sudo ls -lh "$V2_NEW_HOLDOUT_REPORT"
  else
    echo 'STOP: replay generation failed. Do not continue to selection.'
  fi
fi
```

Do not continue unless `V2_HOLDOUT_FILE_READY=1`. A newly generated replay
must also show `replay_exit=0`.

## 8. Check the new holdout report

```bash
V2_HOLDOUT_VALIDATED=0
if [ "${V2_HOLDOUT_FILE_READY:-0}" -ne 1 ]; then
  echo 'STOP: step 7 did not produce or find a holdout file.'
elif sudo -u pricecollector /opt/price-collector/.venv/bin/python - \
  "$V2_NEW_HOLDOUT_REPORT" \
  "$V1_CALIBRATION_REPORT" \
  "$V1_INSPECTED_HOLDOUT_REPORT" \
  "$V2_HOLDOUT_START_MS" \
  "$V2_HOLDOUT_END_MS" <<'PY'
import json
import sys
from decimal import Decimal

new_path, first_old_path, second_old_path = sys.argv[1:4]
expected_start_ms, expected_end_ms = map(int, sys.argv[4:6])

with open(new_path, encoding="utf-8") as stream:
    report = json.load(stream)
with open(first_old_path, encoding="utf-8") as stream:
    first_old = json.load(stream)
with open(second_old_path, encoding="utf-8") as stream:
    second_old = json.load(stream)

if report.get("schema_version") != 2:
    raise SystemExit("STOP: new holdout is not replay schema version 2")
if report.get("mode") != "shadow_raw_replay":
    raise SystemExit("STOP: new holdout path is not a replay report")
if report.get("status") != "ok":
    raise SystemExit(f"STOP: new holdout status is {report.get('status')!r}")
if report.get("range") != {
    "start_ms": expected_start_ms,
    "end_ms": expected_end_ms,
    "boundary": "[start_ms,end_ms)",
}:
    raise SystemExit("STOP: new holdout range differs from step 6")
if not (
    report["configuration"]
    == first_old["configuration"]
    == second_old["configuration"]
):
    raise SystemExit("STOP: replay configurations do not match")

candidate_summaries = []
for candidate in report["candidates"]:
    common = candidate["common_cohort"]
    target_eligible = Decimal(common["target_eligible"])
    valid_generated = Decimal(common["valid_generated"])
    scored = Decimal(common["scored"])
    if target_eligible <= 0 or valid_generated <= 0:
        raise SystemExit("STOP: new holdout has no eligible common cohort")
    valid_coverage = valid_generated / target_eligible
    maturation_coverage = scored / valid_generated
    candidate_summaries.append({
        "model": candidate["model_version"],
        "common_scored": int(scored),
        "common_valid_coverage": str(valid_coverage),
        "common_maturation_coverage": str(maturation_coverage),
    })

ready = all(
    item["common_scored"] >= 10_000
    and Decimal(item["common_valid_coverage"]) >= Decimal("0.50")
    and Decimal(str(item["common_maturation_coverage"])) >= Decimal("0.99")
    for item in candidate_summaries
)
summary = {
    "schema_version": report["schema_version"],
    "status": report["status"],
    "range": report["range"],
    "policy_v2_data_ready": ready,
    "candidates": candidate_summaries,
}
print(summary)

if not ready:
    raise SystemExit("STOP: new holdout is not ready for selection")
PY
then
  V2_HOLDOUT_VALIDATED=1
else
  echo 'STOP: new-holdout validation failed. Do not run policy v2.'
fi
```

## 9. Run policy v2

Both old v1 reports are calibration inputs. Only the new report is the v2
holdout input.

```bash
V2_SELECTION_READY=0
if [ "${V2_HOLDOUT_VALIDATED:-0}" -ne 1 ]; then
  echo 'STOP: step 8 did not validate the new holdout.'
else
  if sudo test -e "$V2_SELECTION_REPORT"; then
    echo 'Selection file exists; the selector will accept it only if identical.'
  fi

  V2_SELECTION_EXIT=0
  (
    cd /opt/price-collector &&
    sudo -u pricecollector .venv/bin/python \
      -m price_collector.shadow_signal_selection \
      --calibration-report "$V1_CALIBRATION_REPORT" \
      --calibration-report "$V1_INSPECTED_HOLDOUT_REPORT" \
      --holdout-report "$V2_NEW_HOLDOUT_REPORT" \
      --output "$V2_SELECTION_REPORT"
  ) || V2_SELECTION_EXIT=$?

  printf 'v2_selection_exit=%s\n' "$V2_SELECTION_EXIT"
  if { [ "$V2_SELECTION_EXIT" -eq 0 ] || \
       [ "$V2_SELECTION_EXIT" -eq 2 ]; } && \
     sudo test -f "$V2_SELECTION_REPORT"; then
    V2_SELECTION_READY=1
    sudo chmod 640 "$V2_SELECTION_REPORT"
  else
    echo 'STOP: policy-v2 selection failed with an input or command error.'
  fi
fi
```

Exit `0` means a provisional primary was selected. Exit `2` means the valid
v2 decision abstained. Exit `1` means an input or command error.

## 10. Print the final decision

```bash
if [ "${V2_SELECTION_READY:-0}" -ne 1 ]; then
  echo 'STOP: step 9 did not create or validate a v2 selection artifact.'
else
  sudo -u pricecollector /opt/price-collector/.venv/bin/python - \
    "$V2_SELECTION_REPORT" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    selection = json.load(stream)

print("schema_version:", selection["schema_version"])
print("policy:", selection["policy"]["version"])
print("status:", selection["status"])
print("decision:", selection["decision"])
for candidate in selection["candidates"]:
    print({
        "rank": candidate["calibration_rank"],
        "model": candidate["model_version"],
        "calibration_mae_skill": candidate["calibration"]["metrics"]["mae_skill_vs_no_change"],
        "holdout_mae_skill": candidate["holdout"]["metrics"]["mae_skill_vs_no_change"],
        "calibration_gates": candidate["calibration"]["gates"],
        "holdout_gates": candidate["holdout"]["gates"],
        "calibration_paired_frequency": candidate["calibration"]["paired_frequency_diagnostic"],
        "holdout_paired_frequency": candidate["holdout"]["paired_frequency_diagnostic"],
    })
PY
fi
```

Keep the two v1 replay reports, the new v2 holdout report, the old v1
selection artifact, and the new v2 selection artifact. Do not overwrite or
delete any of them.
