# Simple Chainlink shorter-lag test

## Scope

This is one offline, read-only comparison. It does not change or restart a
collector, Redis, FastAPI, PostgreSQL, the shadow worker, systemd, or the active
selection files. Raw replay is the historical authority; the live evaluator is
only a later production sanity check.

The result is descriptive. `observed_shorter_better` does not deploy or promote
a model and is not a claim of statistical significance.

## Fixed inputs

Choose both half-open UTC ranges before running the command:

- calibration: `[calibration_start_ms, calibration_end_ms)`
- holdout: `[holdout_start_ms, holdout_end_ms)`

Record both ranges before inspecting either result. The helper does not create
or verify preregistration evidence, so its conclusion remains descriptive. The
calibration range must end before or exactly when the later holdout starts, and
each range must be exactly 24 hours.

The wrapper fixes the test to:

- calibration lags: `1500, 2000, 2500, 3000, 3500` ms
- shorter candidates: `1500, 2000, 2500` ms
- fixed-settings reference: `3000` ms
- `beta=1`
- 100 ms poll and epoch-aligned 500 ms evaluation cadence
- 100 ms assumed Futures and Chainlink availability delays
- 250 ms maximum reference gap and zero future skew
- strict exclusion of sessions containing parse errors
- at least 10,000 common scored forecasts, 50% common valid coverage, and 99%
  common maturation coverage in each candidate report

The existing replay configuration validates staleness, history retention, raw
session integrity, common cohorts, causal actuals, and Decimal calculations.
The 3000 ms member uses the same fixed settings as the shorter candidates. It
is a horizon reference, not a reconstruction of the deployed incumbent.

## Decision rule

Calibration ranks only the three shorter candidates by common-cohort MAE skill
versus each horizon's matched no-change baseline. Higher is better; an exact tie
selects the smaller lag. If no shorter candidate has positive skill, retain the
3000 ms reference and do not read the holdout.

The selected shorter lag is frozen before the holdout query. Holdout replays
only that challenger and the 3000 ms reference. It returns
`observed_shorter_better` only when the challenger has positive common-cohort
MAE skill and strictly exceeds the reference's skill. Otherwise it returns
`retain_3000_reference`.

A non-`ok` replay or missing MAE skill returns `insufficient_evidence`. There is
no reranking, fallback candidate, automatic retry, bootstrap, artifact ledger,
lineage state machine, lock protocol, or deployment action.

## Run once

For the exact frozen dates and copy/paste sequence, use
[`CHAINLINK_SHORTER_LAG_TEST_RUNBOOK.md`](CHAINLINK_SHORTER_LAG_TEST_RUNBOOK.md).

Run after the chosen holdout has ended and both source sessions have finalized.
Replace the four epoch-millisecond values before executing:

```bash
cd /opt/price-collector
CALIBRATION_START_MS=1700000000000
CALIBRATION_END_MS=1700086400000
HOLDOUT_START_MS=1700172800000
HOLDOUT_END_MS=1700259200000
OUTPUT=/var/lib/price-collector/shadow-lag-test.json

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
    cd /opt/price-collector
    exec .venv/bin/python -m price_collector.shadow_signal_lag_test \
      --calibration-start-ms "$CALIBRATION_START_MS" \
      --calibration-end-ms "$CALIBRATION_END_MS" \
      --holdout-start-ms "$HOLDOUT_START_MS" \
      --holdout-end-ms "$HOLDOUT_END_MS" \
      --output "$OUTPUT"
  '

sudo chmod 640 "$OUTPUT"
sudo -u pricecollector /opt/price-collector/.venv/bin/python -m json.tool \
  "$OUTPUT" | head -n 80
```

The single JSON document contains the fixed settings, calibration ranking,
frozen winner, holdout comparison, and both underlying replay reports. All
Decimal values are serialized as strings.
