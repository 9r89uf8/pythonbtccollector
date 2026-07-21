# Chainlink shorter-lag test: commands to run

This is the complete command sequence for the descriptive shorter-lag test.
The test does not change the active model. Its raw inputs were disabled when
first checked, so preparation must enable raw capture and restart the two source
collectors before either evidence window begins.

## Recorded outcome and recovery boundary

The original run completed with `insufficient_evidence` because every
calibration Chainlink session was excluded by the strict parse-error gate. The
collector had classified two normal RTDS startup frames as parse errors: an
empty text frame and the initial `crypto_prices` historical subscription dump.

The original artifact is permanently a failed result. Do not rerun, overwrite,
rename, delete, or reinterpret it:

```text
Path:   /var/lib/price-collector/shadow-lag-test-20260719-20260721.json
SHA-256: 2e715151b011dc051f0064490ad1c5a29c319f6aa054bc71edbee7cdf4251f5a
Status: insufficient_evidence
Reason: calibration_replay_no_eligible_segments
Operator-recorded source commit: ab30ab67fd66b96199b1526c29e897dad7a4ea0e
```

The source commit was printed separately by the operator; it is not embedded
cryptographically in the original JSON. Steps 8 through 12 create a separately
named, post-hoc descriptive recovery artifact. That recovery cannot promote a
production model. If Step 7 is already complete, resume at Step 8; do not rerun
Steps 1 through 7.

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

The command must complete with no failed or errored tests. The exact pass count
can increase as focused recovery coverage is added.

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
    ) AS open_sessions,
    count(*) FILTER (
        WHERE disconnected_wall_ns IS NULL
          AND ready_wall_ns IS NOT NULL
    ) AS ready_open_sessions,
    COALESCE(sum(parse_errors_total) FILTER (
        WHERE disconnected_wall_ns IS NULL
    ), 0) AS open_parse_errors,
    COALESCE(sum(records_dropped_total) FILTER (
        WHERE disconnected_wall_ns IS NULL
    ), 0) AS open_dropped_records
FROM raw_capture.feed_sessions
WHERE source IN (
    'binance_futures_agg_trade',
    'polymarket_chainlink_rtds'
)
GROUP BY source
ORDER BY source;
"
```

Both two-minute row counts must be positive. Each source must report exactly one
open, ready session with zero parse errors and zero dropped records. Rerun this
block several minutes later. Any nonzero startup parse-error or dropped-record
counter is a stop condition; investigate immediately instead of waiting through
the calibration and holdout windows. Expected startup frames increase only the
received-message counter, not the accepted, parse-error, or accepted-event idle
deadline state.

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

## 8. Lock and verify the failed artifact

Step 7 is complete, so this is the next action. Protect the original before
pulling or running any recovery code:

```bash
ORIGINAL_RESULT=/var/lib/price-collector/shadow-lag-test-20260719-20260721.json
EXPECTED_ORIGINAL_SHA256=2e715151b011dc051f0064490ad1c5a29c319f6aa054bc71edbee7cdf4251f5a

sudo test -s "$ORIGINAL_RESULT"
printf '%s  %s\n' \
  "$EXPECTED_ORIGINAL_SHA256" "$ORIGINAL_RESULT" |
  sudo sha256sum --check -

sudo chown root:pricecollector "$ORIGINAL_RESULT"
sudo chmod 0440 "$ORIGINAL_RESULT"
sudo chattr +i "$ORIGINAL_RESULT"

printf '%s  %s\n' \
  "$EXPECTED_ORIGINAL_SHA256" "$ORIGINAL_RESULT" |
  sudo sha256sum --check -
sudo stat -c '%U:%G %a %n' "$ORIGINAL_RESULT"
sudo lsattr "$ORIGINAL_RESULT"
```

The ownership command must show `root:pricecollector 440`, and `lsattr` must
show the immutable `i` flag. If `chattr` or either verification fails, stop.
Mode `0440` alone is not enough because the service user owns the parent state
directory; the immutable flag prevents that user from unlinking or replacing
the file. The recovery process can still read it through its group.

## 9. Pull the recovery checkpoint

First commit only the recovery checkpoint on the Windows development machine;
do not stage the downloaded market JSON files or other unrelated work:

```powershell
cd C:\Users\alexa\PycharmProjects\polycollector

git status --short
git diff --check
.\.venv\Scripts\python.exe -m pytest -q

$RecoveryPaths = @(
  'CHAINLINK_SHORTER_LAG_TEST_RUNBOOK.md'
  'OPERATIONS.md'
  'README.md'
  'price_collector/polymarket_chainlink_collector.py'
  'price_collector/shadow_signal_lag_recovery.py'
  'price_collector/shadow_signal_lag_test.py'
  'price_collector/shadow_signal_replay.py'
  'tests/test_polymarket_chainlink_collector.py'
  'tests/test_shadow_signal_lag_recovery.py'
  'tests/test_shadow_signal_lag_test.py'
  'tests/test_shadow_signal_replay.py'
)

git add -- $RecoveryPaths
git diff --cached --name-status -- $RecoveryPaths
git commit --only -m "Recover July Chainlink lag evidence" -- $RecoveryPaths
git push
```

`git commit --only` keeps any already staged downloaded-market deletion or other
unrelated change out of this recovery commit. The recovery CLI later verifies
that this is one direct child commit with exactly the file list above.

Then run this on the droplet only after that commit has been pushed to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python \
  -m price_collector.shadow_signal_lag_recovery --help

sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status \
  price-collector-polymarket-chainlink --no-pager --full
sudo journalctl -u price-collector-polymarket-chainlink \
  -n 100 -o short-precise --no-pager
curl -fsS http://127.0.0.1:9000/healthz
```

Keep both raw-capture flags `false`. The collector restart loads the corrected
startup-frame classification for future captures. Recovery reads the retained
data and does not require a futures-collector restart.

## 10. Verify the retained recovery inputs

Run this counter-level preflight promptly. It verifies that both raw products
and at least one counter-eligible common session intersection remain for each
window:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 \
  -d price_collector -c "
WITH windows(label, start_ms, end_ms) AS (
    VALUES
        ('calibration', 1784419200000::bigint, 1784505600000::bigint),
        ('holdout',     1784505600000::bigint, 1784592000000::bigint)
), scoped AS (
    SELECT
        windows.label,
        windows.start_ms,
        windows.end_ms,
        sessions.*
    FROM windows
    JOIN raw_capture.feed_sessions sessions
      ON sessions.connected_wall_ns < windows.end_ms * 1000000
     AND COALESCE(
            sessions.disconnected_wall_ns,
            windows.end_ms * 1000000
         ) > windows.start_ms * 1000000
    WHERE sessions.source IN (
        'binance_futures_agg_trade',
        'polymarket_chainlink_rtds'
    )
), counter_eligible AS (
    SELECT *
    FROM scoped
    WHERE ready_wall_ns IS NOT NULL
      AND disconnected_wall_ns IS NOT NULL
      AND records_dropped_total = 0
      AND (
          (
              source = 'binance_futures_agg_trade'
              AND parse_errors_total = 0
          )
          OR (
              source = 'polymarket_chainlink_rtds'
              AND parse_errors_total IN (0, 2)
          )
      )
), common_segments AS (
    SELECT futures.label, count(*) AS segment_count
    FROM counter_eligible futures
    JOIN counter_eligible chainlink
      ON chainlink.label = futures.label
     AND futures.source = 'binance_futures_agg_trade'
     AND chainlink.source = 'polymarket_chainlink_rtds'
     AND GREATEST(
            futures.ready_wall_ns,
            chainlink.ready_wall_ns,
            futures.start_ms * 1000000
         ) < LEAST(
            futures.disconnected_wall_ns,
            chainlink.disconnected_wall_ns,
            futures.end_ms * 1000000
         )
    GROUP BY futures.label
)
SELECT
    windows.label,
    (
        SELECT count(*)
        FROM raw_capture.binance_futures_price_trace_100ms rows
        WHERE rows.bucket_start_ms >= windows.start_ms
          AND rows.bucket_start_ms < windows.end_ms
    ) AS futures_raw_rows,
    (
        SELECT count(*)
        FROM raw_capture.chainlink_price_events rows
        WHERE rows.received_wall_ns >= windows.start_ms * 1000000
          AND rows.received_wall_ns < windows.end_ms * 1000000
    ) AS chainlink_raw_rows,
    (
        SELECT count(*)
        FROM counter_eligible sessions
        WHERE sessions.label = windows.label
          AND sessions.source = 'binance_futures_agg_trade'
    ) AS counter_eligible_futures_sessions,
    (
        SELECT count(*)
        FROM counter_eligible sessions
        WHERE sessions.label = windows.label
          AND sessions.source = 'polymarket_chainlink_rtds'
    ) AS counter_eligible_chainlink_sessions,
    COALESCE(common_segments.segment_count, 0)
        AS counter_eligible_common_segments
FROM windows
LEFT JOIN common_segments USING (label)
ORDER BY windows.start_ms;
"
```

Every numeric result in both rows must be positive. This is only a retention
and counter check; the Python recovery repeats the complete count, duplicate,
clock-regression, session-boundary, and raw-row integrity reconciliation.

Also confirm the exact counter distribution over the two windows:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 \
  -d price_collector -c "
SELECT source, parse_errors_total, count(*) AS sessions
FROM raw_capture.feed_sessions
WHERE connected_wall_ns < 1784592000000000000
  AND COALESCE(disconnected_wall_ns, 1784592000000000000)
      > 1784419200000000000
  AND source IN (
      'binance_futures_agg_trade',
      'polymarket_chainlink_rtds'
  )
GROUP BY source, parse_errors_total
ORDER BY source, parse_errors_total;
"
```

Futures must have only total `0`. Chainlink must reproduce the recorded exact
distribution of one session at `0` and 29 sessions at `2`. Stop on any other
total or count, or if Step 10 shows no common intersection. Do not edit session
counters or run a generic replay with parse checking disabled.

## 11. Run the dedicated recovery once

The command exposes no range, lag, model-timing, or parse-policy option. Those
values are pinned in the incident-specific module; `--chunk-ms` affects only
bounded database reads:

```bash
ORIGINAL_RESULT=/var/lib/price-collector/shadow-lag-test-20260719-20260721.json
RECOVERY_OUTPUT=/var/lib/price-collector/shadow-lag-test-20260719-20260721-posthoc-descriptive-recovery.json

if sudo test -e "$RECOVERY_OUTPUT"; then
  echo "STOP: $RECOVERY_OUTPUT already exists; do not overwrite it."
else
  sudo -u pricecollector \
    env ORIGINAL_RESULT="$ORIGINAL_RESULT" \
        RECOVERY_OUTPUT="$RECOVERY_OUTPUT" \
    bash -c '
      set -a
      . /etc/price-collector/collector.env
      set +a
      umask 027
      cd /opt/price-collector
      exec .venv/bin/python \
        -m price_collector.shadow_signal_lag_recovery \
        --original-result "$ORIGINAL_RESULT" \
        --output "$RECOVERY_OUTPUT"
    '
fi
```

The command first verifies the original basename, SHA-256, failed decision,
ranges, settings, and diagnostics. It also requires a clean recovery worktree,
the recovery commit to be the single direct child of the recorded original
commit, and exactly the audited changed-file set. An independent full-48-hour
database census then requires all Futures sessions to have parse total `0` and
the exact recorded Chainlink distribution `{0: 1, 2: 29}`, even if calibration
later stops before holdout replay. Every other replay integrity gate remains
active. Any mismatch aborts without creating the recovery output.

## 12. Inspect and lock the recovery result

```bash
RECOVERY_OUTPUT=/var/lib/price-collector/shadow-lag-test-20260719-20260721-posthoc-descriptive-recovery.json

sudo test -s "$RECOVERY_OUTPUT"
sudo -u pricecollector /opt/price-collector/.venv/bin/python -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    result = json.load(source)

print("mode:", result["mode"])
print("status:", result["status"])
print("evidence class:", result["evidence_class"])
print("eligible for production:", result["eligible_for_production_promotion"])
print("original preserved:", result["original_result_preserved"])
print("original SHA-256:", result["provenance"]["original_artifact"]["sha256"])
print("recovery commit:", result["provenance"]["recovery_implementation"]["git_commit"])
print("conclusion:", json.dumps(result["conclusion"], indent=2))
' "$RECOVERY_OUTPUT"

sudo chown root:pricecollector "$RECOVERY_OUTPUT"
sudo chmod 0440 "$RECOVERY_OUTPUT"
sudo chattr +i "$RECOVERY_OUTPUT"
sudo sha256sum "$RECOVERY_OUTPUT"
sudo stat -c '%U:%G %a %n' "$RECOVERY_OUTPUT"
sudo lsattr "$RECOVERY_OUTPUT"
```

Expected invariant fields are `mode=posthoc_shadow_lag_recovery`,
`evidence_class=descriptive_only`, and
`eligible_for_production_promotion=false`. The artifact describes what the
retained data shows under the corrected startup-frame classification, but the
journals do not prove the two rejected startup frames for every historical
session. A formal model decision still requires future clean calibration data
and a newly untouched holdout. The final `lsattr` must show the immutable `i`
flag; if it does not, the result is not locked against replacement by the state
directory owner.

## If the deadline is missed

Do not reuse these dates after their raw data has aged out. Choose and record a
new pair of consecutive 24-hour UTC windows before inspecting their results,
then update all four epoch-millisecond values and the output filename together.
