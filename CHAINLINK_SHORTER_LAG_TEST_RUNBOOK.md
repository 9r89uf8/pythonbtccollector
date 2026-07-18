# Chainlink shorter-lag test: commands to run

This is the complete command sequence for the descriptive shorter-lag test.
The test does not change the active model. Its raw inputs were disabled when
first checked, so preparation must enable raw capture and restart the two source
collectors before either evidence window begins.

## 1. Commit and push the test now

Run these commands on the Windows development machine before
`2026-07-19 00:00:00 UTC`:

```powershell
cd C:\Users\alexa\PycharmProjects\polycollector

git status --short
git diff --check
.\.venv\Scripts\python.exe -m pytest -q

git add CHAINLINK_ACTUAL_VS_PROJECTED.md CHAINLINK_V4_PRETEST_FIX_PLAN.md CHAINLINK_SHORTER_LAG_TEST_RUNBOOK.md price_collector/shadow_signal_experiment.py price_collector/shadow_signal_replay.py price_collector/shadow_signal_lag_test.py price_collector/polymarket_chainlink_collector.py tests/test_shadow_signal_experiment.py tests/test_shadow_signal_replay.py tests/test_shadow_signal_lag_test.py tests/test_polymarket_chainlink_collector.py
git commit -m "Simplify Chainlink shorter-lag test"
git push
```

Expected local test result:

```text
752 passed, 11 skipped
```

## 2. Pull the pushed code onto the droplet

Run this only after the commit above has been pushed to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m price_collector.shadow_signal_lag_test --help
```

Do not restart a service yet. Step 3 restarts only the two collectors that must
load the raw-capture flags.

## 3. Enable the required raw capture

The original July 18 calibration window is invalid because both raw-capture
flags were `false`. Raw capture was subsequently enabled and verified. The
replacement calibration window starts at `2026-07-19 00:00:00 UTC`.

First confirm that the three raw-capture tables exist and check available disk
space:

```bash
cd /opt/price-collector

sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -c "
SELECT
    to_regclass('raw_capture.binance_futures_price_trace_100ms') IS NOT NULL
        AS futures_table_exists,
    to_regclass('raw_capture.chainlink_price_events') IS NOT NULL
        AS chainlink_table_exists,
    to_regclass('raw_capture.feed_sessions') IS NOT NULL
        AS sessions_table_exists;
"
df -h /var/lib/postgresql
```

All three database values must be `t`. If any value is `f`, stop and do not
enable capture. High-resolution retention and sustained storage-budget behavior
remain unproven in production, so this is a bounded capture; check disk space
before enabling and restore the original `false` flags in Step 7.

Edit the existing production environment manually:

```bash
sudoedit /etc/price-collector/collector.env
```

Change only these two values from `false` to `true`:

```text
RAW_FUTURES_TRACE_ENABLED=true
RAW_CHAINLINK_EVENTS_ENABLED=true
```

Also confirm that `BINANCE_FUTURES_STREAMS_ENABLED=true` remains unchanged.

Do not replace the environment file or alter its credentials. Confirm the
non-secret capture settings:

```bash
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED|RAW_CAPTURE_RETENTION_HOURS|RAW_CAPTURE_MAX_RELATION_MB)=' /etc/price-collector/collector.env
```

The two flags must be `true`. If `RAW_CAPTURE_RETENTION_HOURS` is explicitly
present, it must be at least `72`; when absent, the code default is `72`.
Restart only the collectors that read those flags, then inspect their startup
logs:

```bash
CAPTURE_ENABLE_UTC="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"

sudo systemctl restart price-collector-binance-futures
sudo systemctl is-active price-collector-binance-futures
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl is-active price-collector-polymarket-chainlink
sudo systemctl status \
  price-collector-binance-futures \
  price-collector-polymarket-chainlink \
  --no-pager --full
sudo journalctl -u price-collector-binance-futures \
  --since "$CAPTURE_ENABLE_UTC" -n 100 -o short-precise --no-pager
sudo journalctl -u price-collector-polymarket-chainlink \
  --since "$CAPTURE_ENABLE_UTC" -n 100 -o short-precise --no-pager
```

Both services must print `active`, and the logs must not show raw-writer,
partition, permission, or database errors. Because the journal filter starts at
whole-second precision, it may include a few lines from the old process in that
same second.

After about one minute, verify that both products are writing current raw data:

```bash
VERIFY_SINCE_MS="$(( $(date -u +%s%3N) - 120000 ))"
VERIFY_SINCE_NS="$(( VERIFY_SINCE_MS * 1000000 ))"

sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -c "
SELECT
    (SELECT count(*)
       FROM raw_capture.binance_futures_price_trace_100ms
      WHERE last_received_wall_ns >= ${VERIFY_SINCE_NS})
        AS futures_rows_last_two_minutes,
    (SELECT count(*)
       FROM raw_capture.chainlink_price_events
      WHERE received_wall_ns >= ${VERIFY_SINCE_NS})
        AS chainlink_rows_last_two_minutes;

SELECT
    source,
    count(*) FILTER (
        WHERE disconnected_wall_ns IS NULL
    ) AS open_sessions
FROM raw_capture.feed_sessions
WHERE source IN (
    'binance_futures_agg_trade',
    'polymarket_chainlink_rtds'
)
GROUP BY source
ORDER BY source;
"
```

Both two-minute row counts must be positive, and both sources must report an
open session. Rerun this block after several seconds if the immediate counts
are zero. If they remain zero, stop; waiting will not create a usable test.

## 4. Wait for the holdout to finish

After Step 3 succeeds, leave the collectors running normally. They collect both
windows automatically; no test command or open shell is needed while waiting.

The frozen windows are:

```text
Calibration: 2026-07-19 00:00:00 UTC through 2026-07-20 00:00:00 UTC
             [1784419200000, 1784505600000)

Holdout:     2026-07-20 00:00:00 UTC through 2026-07-21 00:00:00 UTC
             [1784505600000, 1784592000000)
```

Do not run the test before `2026-07-21 00:00:00 UTC`. Run this readiness check
on the droplet:

```bash
NOW_SECONDS="$(date -u +%s)"
HOLDOUT_END_SECONDS=1784592000

date -u
if [ "$NOW_SECONDS" -lt "$HOLDOUT_END_SECONDS" ]; then
  echo "WAIT: the holdout is still being collected. Do not run Step 5 yet."
else
  echo "READY: the holdout has ended. Continue to Step 5."
fi
```

If it prints `WAIT`, close the shell and run the same readiness check later.
Nothing needs to remain open or running manually. If it prints `READY`, proceed
to Step 5 promptly so the raw data remains inside the configured retention
period.

## 5. Run the test once

On the droplet, copy and paste this entire block:

```bash
cd /opt/price-collector

CALIBRATION_START_MS=1784419200000
CALIBRATION_END_MS=1784505600000
HOLDOUT_START_MS=1784505600000
HOLDOUT_END_MS=1784592000000
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260719-20260721.json

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
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260719-20260721.json
sudo test -s "$OUTPUT"
sudo chmod 640 "$OUTPUT"
sudo ls -l "$OUTPUT"
```

If `test -s` fails, stop and read the error printed by the test command. Do not
change the windows or model settings after seeing a result.

## 6. Read the result

Print the short decision summary:

```bash
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260719-20260721.json
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
OUTPUT=/var/lib/price-collector/shadow-lag-test-20260719-20260721.json
sudo -u pricecollector /opt/price-collector/.venv/bin/python -m json.tool "$OUTPUT" | less
```

Interpret `status` as follows:

- `observed_shorter_better`: the selected shorter lag performed better than
  the same-settings 3000 ms reference on this holdout. This is descriptive;
  do not deploy it from this result.
- `retain_3000_reference`: the shorter lag did not clear the fixed comparison.
- `insufficient_evidence`: the replay or evidence gates were inadequate. Do
  not tune the windows and rerun after inspecting the result.

## 7. Restore the prior capture state

After the JSON has been written and inspected, restore the two flags to their
known pre-test values so raw storage does not keep growing:

```bash
sudoedit /etc/price-collector/collector.env
```

Set:

```text
RAW_FUTURES_TRACE_ENABLED=false
RAW_CHAINLINK_EVENTS_ENABLED=false
```

Then restart only the two affected collectors and verify their health:

```bash
sudo grep -E '^(RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env

sudo systemctl restart price-collector-binance-futures
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl is-active price-collector-binance-futures
sudo systemctl is-active price-collector-polymarket-chainlink
sudo systemctl status \
  price-collector-binance-futures \
  price-collector-polymarket-chainlink \
  --no-pager --full
sudo journalctl -u price-collector-binance-futures \
  -n 50 -o short-precise --no-pager
sudo journalctl -u price-collector-polymarket-chainlink \
  -n 50 -o short-precise --no-pager
```

This stops new raw capture. It does not delete the result JSON or existing raw
rows.

## If the deadline is missed

Do not reuse these dates after their raw data has aged out. Choose and record a
new pair of consecutive 24-hour UTC windows before inspecting their results,
then update all four epoch-millisecond values and the output filename together.
