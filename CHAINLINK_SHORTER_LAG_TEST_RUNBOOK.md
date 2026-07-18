# Chainlink shorter-lag test: commands to run

This is the complete command sequence for the descriptive shorter-lag test.
It does not change the active model or restart any service.

## 1. Commit and push the test now

Run these commands on the Windows development machine before
`2026-07-19 00:00:00 UTC`:

```powershell
cd C:\Users\alexa\PycharmProjects\polycollector

git status --short
git diff --check
.\.venv\Scripts\python.exe -m pytest -q

git add CHAINLINK_ACTUAL_VS_PROJECTED.md CHAINLINK_V4_PRETEST_FIX_PLAN.md CHAINLINK_SHORTER_LAG_TEST_RUNBOOK.md price_collector/shadow_signal_experiment.py price_collector/shadow_signal_replay.py price_collector/shadow_signal_lag_test.py tests/test_shadow_signal_experiment.py tests/test_shadow_signal_replay.py tests/test_shadow_signal_lag_test.py
git commit -m "Simplify Chainlink shorter-lag test"
git push
```

Expected local test result:

```text
751 passed, 11 skipped
```

## 2. Pull the pushed code onto the droplet

Run this only after the commit above has been pushed to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m price_collector.shadow_signal_lag_test --help
```

Do not restart any service. This test is offline and read-only.

## 3. Wait for the holdout to finish

The frozen windows are:

```text
Calibration: 2026-07-18 00:00:00 UTC through 2026-07-19 00:00:00 UTC
             [1784332800000, 1784419200000)

Holdout:     2026-07-19 00:00:00 UTC through 2026-07-20 00:00:00 UTC
             [1784419200000, 1784505600000)
```

Do not run the test before `2026-07-20 00:00:00 UTC`. Check the droplet clock:

```bash
date -u
```

Run promptly after the holdout ends so the raw data remains inside the
configured retention period.

## 4. Run the test once

On the droplet, copy and paste this entire block:

```bash
cd /opt/price-collector

CALIBRATION_START_MS=1784332800000
CALIBRATION_END_MS=1784419200000
HOLDOUT_START_MS=1784419200000
HOLDOUT_END_MS=1784505600000
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260718-20260720.json

if sudo test -e "$OUTPUT"; then
  echo "STOP: $OUTPUT already exists; do not overwrite it."
else
  sudo -u pricecollector \
    env CALIBRATION_START_MS="$CALIBRATION_START_MS" \
        CALIBRATION_END_MS="$CALIBRATION_END_MS" \
        HOLDOUT_START_MS="$HOLDOUT_START_MS" \
        HOLDOUT_END_MS="$HOLDOUT_END_MS" \
        OUTPUT="$OUTPUT" \
    bash -c '
      set -a
      . /etc/price-collector/collector.env
      set +a
      umask 027
      cd /opt/price-collector
      exec .venv/bin/python -m price_collector.shadow_signal_lag_test \
        --calibration-start-ms "$CALIBRATION_START_MS" \
        --calibration-end-ms "$CALIBRATION_END_MS" \
        --holdout-start-ms "$HOLDOUT_START_MS" \
        --holdout-end-ms "$HOLDOUT_END_MS" \
        --output "$OUTPUT"
    '
fi
```

Confirm that the command succeeded and protect the result:

```bash
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260718-20260720.json
sudo test -s "$OUTPUT"
sudo chmod 640 "$OUTPUT"
sudo ls -l "$OUTPUT"
```

If `test -s` fails, stop and read the error printed by the test command. Do not
change the windows or model settings after seeing a result.

## 5. Read the result

Print the short decision summary:

```bash
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260718-20260720.json
sudo -u pricecollector /opt/price-collector/.venv/bin/python -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    result = json.load(source)

print("status:", result["status"])
print("selected shorter lag:", result["calibration"]["winner_lag_ms"])
print("holdout:", json.dumps(result["holdout"], indent=2))
' "$OUTPUT"
```

To inspect the complete JSON:

```bash
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260718-20260720.json
sudo -u pricecollector /opt/price-collector/.venv/bin/python -m json.tool "$OUTPUT" | less
```

Interpret `status` as follows:

- `observed_shorter_better`: the selected shorter lag performed better than
  the same-settings 3000 ms reference on this holdout. This is descriptive;
  do not deploy it from this result.
- `retain_3000_reference`: the shorter lag did not clear the fixed comparison.
- `insufficient_evidence`: the replay or evidence gates were inadequate. Do
  not tune the windows and rerun after inspecting the result.

## 6. Verify that production was untouched

No restart is expected. Confirm the existing services are still running:

```bash
sudo systemctl is-active price-collector
sudo systemctl is-active price-collector-polymarket-chainlink
sudo systemctl is-active price-collector-binance-futures
sudo systemctl is-active price-collector-shadow-signal
```

All four commands should print `active` if those services were already enabled
and running before the test.

## If the deadline is missed

Do not reuse these dates after their raw data has aged out. Choose and record a
new pair of consecutive 24-hour UTC windows before inspecting their results,
then update all four epoch-millisecond values and the output filename together.
