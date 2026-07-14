# Shadow-Signal Phase 4 Migration

This guide contains only the ordered commands needed to deploy the standalone
Phase 4 shadow-signal worker. Run every step on the Ubuntu droplet.

Phase 4:

- reads `btc:live:futures` and `btc:live:chainlink` every 100 ms;
- publishes `btc:live:chainlink_shadow` with a 2-second TTL;
- loads the accepted Phase 3 primary from immutable evidence;
- does not add PostgreSQL writes, API fields, or dashboard code; and
- does not commit replay reports or selection artifacts to Git.

The paths below match the accepted Phase 3 result:

```text
/var/lib/price-collector/shadow-primary-selection-chronological-holdout-v2-1783983205028.json
/var/lib/price-collector/shadow-replay-holdout-v2-1783896805028-1783983205028.json
```

If either actual filename differs, change the corresponding variable before
continuing. Do not substitute an abstained selection or a replay report that
was not used by that selection.

## Step 1 — Pull the code and run all tests

Run this after the Phase 4 changes and the Linux test correction have been
pushed to GitHub:

```bash
(
  set -euo pipefail
  cd /opt/price-collector
  sudo -u pricecollector git pull --ff-only
  sudo -u pricecollector .venv/bin/pip install -r requirements.txt
  sudo -u pricecollector .venv/bin/python -m pytest -q
)
```

On Ubuntu, all tests must pass. A FastAPI/Starlette deprecation warning does not
fail the suite. Stop here if `pytest` exits nonzero.

There is no schema migration and no Redis restart in this phase.

## Step 2 — Validate the source evidence

```bash
(
  set -euo pipefail
  SOURCE_SELECTION="/var/lib/price-collector/shadow-primary-selection-chronological-holdout-v2-1783983205028.json"
  SOURCE_REPLAY_CONFIG="/var/lib/price-collector/shadow-replay-holdout-v2-1783896805028-1783983205028.json"

  sudo test -f "$SOURCE_SELECTION"
  sudo test -f "$SOURCE_REPLAY_CONFIG"

  sudo -u pricecollector /opt/price-collector/.venv/bin/python - \
    "$SOURCE_SELECTION" "$SOURCE_REPLAY_CONFIG" <<'PY'
import hashlib
import json
import sys

selection_path, replay_path = sys.argv[1:]
with open(selection_path, "rb") as stream:
    selection_raw = stream.read()
with open(replay_path, "rb") as stream:
    replay_raw = stream.read()

selection = json.loads(selection_raw)
replay = json.loads(replay_raw)
primary = selection["decision"]["provisional_primary_model"]
replay_sha = hashlib.sha256(replay_raw).hexdigest()
provenance_shas = {
    report["sha256"] for report in selection["provenance"]["reports"]
}

assert selection["schema_version"] == 2
assert selection["mode"] == "shadow_primary_selection"
assert selection["policy"]["version"] == "chronological_holdout_v2"
assert selection["status"] == "selected"
assert selection["selection_performed"] is True
assert selection["dynamic_switching"] is False
assert primary is not None
assert primary["model_version"] == "catchup_ratio_l3000_b100"
assert primary["horizon_ms"] == 3000
assert primary["beta"] == "1"
assert replay["schema_version"] == 2
assert replay["mode"] == "shadow_raw_replay"
assert replay["status"] == "ok"
assert replay_sha in provenance_shas

print({
    "selection_sha256": hashlib.sha256(selection_raw).hexdigest(),
    "replay_sha256": replay_sha,
    "primary": primary,
})
PY
)
```

Stop if an assertion fails.

## Step 3 — Promote immutable runtime evidence

This creates two runtime copies. The original evidence remains untouched in
`/var/lib/price-collector` and stays outside the Git repository.

```bash
(
  set -euo pipefail
  SOURCE_SELECTION="/var/lib/price-collector/shadow-primary-selection-chronological-holdout-v2-1783983205028.json"
  SOURCE_REPLAY_CONFIG="/var/lib/price-collector/shadow-replay-holdout-v2-1783896805028-1783983205028.json"
  DECISION_DIR="/var/lib/price-collector/shadow-decisions"

  SELECTION_SHA="$(sudo sha256sum "$SOURCE_SELECTION" | awk '{print $1}')"
  REPLAY_SHA="$(sudo sha256sum "$SOURCE_REPLAY_CONFIG" | awk '{print $1}')"
  EVIDENCE_END_MS="$(sudo python3 -c '
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    selection = json.load(stream)
print(selection["decision"]["provisional_primary_model"]["evidence_end_ms"])
' "$SOURCE_SELECTION")"

  ACTIVE_SELECTION="$DECISION_DIR/selection-${EVIDENCE_END_MS}-${SELECTION_SHA}.json"
  ACTIVE_REPLAY_CONFIG="$DECISION_DIR/replay-config-${EVIDENCE_END_MS}-${REPLAY_SHA}.json"

  sudo install -d -o root -g pricecollector -m 0750 "$DECISION_DIR"

  if sudo test -e "$ACTIVE_SELECTION"; then
    sudo cmp -s "$SOURCE_SELECTION" "$ACTIVE_SELECTION"
  else
    sudo install -o root -g pricecollector -m 0440 \
      "$SOURCE_SELECTION" "$ACTIVE_SELECTION"
  fi

  if sudo test -e "$ACTIVE_REPLAY_CONFIG"; then
    sudo cmp -s "$SOURCE_REPLAY_CONFIG" "$ACTIVE_REPLAY_CONFIG"
  else
    sudo install -o root -g pricecollector -m 0440 \
      "$SOURCE_REPLAY_CONFIG" "$ACTIVE_REPLAY_CONFIG"
  fi

  sudo chown root:pricecollector "$ACTIVE_SELECTION" "$ACTIVE_REPLAY_CONFIG"
  sudo chmod 0440 "$ACTIVE_SELECTION" "$ACTIVE_REPLAY_CONFIG"

  ENTRY_COUNT="$(sudo find "$DECISION_DIR" \
    -mindepth 1 -maxdepth 1 -printf x | wc -c)"
  test "$ENTRY_COUNT" -eq 2

  sudo ls -ld "$DECISION_DIR"
  sudo ls -l "$ACTIVE_SELECTION" "$ACTIVE_REPLAY_CONFIG"
)
```

The directory must contain exactly those two files. If an existing destination
does not compare byte-for-byte equal, stop. Never overwrite immutable evidence.

To inspect unexpected entries without deleting anything:

```bash
sudo find /var/lib/price-collector/shadow-decisions \
  -mindepth 1 -maxdepth 1 -printf '%M %u:%g %p\n'
```

## Step 4 — Configure the dedicated environment

This step updates only `shadow-signal.env`. It does not replace
`collector.env` or `api.env`.

```bash
(
  set -euo pipefail
  cd /opt/price-collector

  SOURCE_SELECTION="/var/lib/price-collector/shadow-primary-selection-chronological-holdout-v2-1783983205028.json"
  SOURCE_REPLAY_CONFIG="/var/lib/price-collector/shadow-replay-holdout-v2-1783896805028-1783983205028.json"
  DECISION_DIR="/var/lib/price-collector/shadow-decisions"
  SELECTION_SHA="$(sudo sha256sum "$SOURCE_SELECTION" | awk '{print $1}')"
  REPLAY_SHA="$(sudo sha256sum "$SOURCE_REPLAY_CONFIG" | awk '{print $1}')"
  EVIDENCE_END_MS="$(sudo python3 -c '
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    selection = json.load(stream)
print(selection["decision"]["provisional_primary_model"]["evidence_end_ms"])
' "$SOURCE_SELECTION")"
  ACTIVE_SELECTION="$DECISION_DIR/selection-${EVIDENCE_END_MS}-${SELECTION_SHA}.json"
  ACTIVE_REPLAY_CONFIG="$DECISION_DIR/replay-config-${EVIDENCE_END_MS}-${REPLAY_SHA}.json"

  sudo test -f "$ACTIVE_SELECTION"
  sudo test -f "$ACTIVE_REPLAY_CONFIG"

  sudo install -d -o root -g pricecollector -m 0750 /etc/price-collector
  if ! sudo test -e /etc/price-collector/shadow-signal.env; then
    sudo install -o root -g pricecollector -m 0640 \
      deployment/shadow-signal.env.example \
      /etc/price-collector/shadow-signal.env
  fi

  sudo sed -i \
    -e 's|^SHADOW_SIGNAL_ENABLED=.*|SHADOW_SIGNAL_ENABLED=true|' \
    -e "s|^SHADOW_SIGNAL_SELECTION_PATH=.*|SHADOW_SIGNAL_SELECTION_PATH=$ACTIVE_SELECTION|" \
    -e "s|^SHADOW_SIGNAL_SELECTION_SHA256=.*|SHADOW_SIGNAL_SELECTION_SHA256=$SELECTION_SHA|" \
    -e "s|^SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH=.*|SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH=$ACTIVE_REPLAY_CONFIG|" \
    -e 's|^SHADOW_SIGNAL_POLL_MS=.*|SHADOW_SIGNAL_POLL_MS=100|' \
    -e 's|^SHADOW_SIGNAL_TTL_MS=.*|SHADOW_SIGNAL_TTL_MS=2000|' \
    /etc/price-collector/shadow-signal.env

  sudo chown root:pricecollector /etc/price-collector/shadow-signal.env
  sudo chmod 0640 /etc/price-collector/shadow-signal.env

  for REQUIRED_KEY in \
    SHADOW_SIGNAL_ENABLED \
    SHADOW_SIGNAL_TRUSTED_DECISION_DIR \
    SHADOW_SIGNAL_SELECTION_PATH \
    SHADOW_SIGNAL_SELECTION_SHA256 \
    SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH \
    SHADOW_SIGNAL_POLL_MS \
    SHADOW_SIGNAL_TTL_MS
  do
    KEY_COUNT="$(sudo awk -F= -v key="$REQUIRED_KEY" \
      '$1 == key { count += 1 } END { print count + 0 }' \
      /etc/price-collector/shadow-signal.env)"
    test "$KEY_COUNT" -eq 1
  done

  if sudo grep -Eq '^(DATABASE_URL|READ_DATABASE_URL)=' \
    /etc/price-collector/shadow-signal.env; then
    echo 'STOP: database credentials do not belong in shadow-signal.env.'
    exit 1
  fi

  sudo grep -E \
    '^(APP_ENV|LOG_LEVEL|REDIS_|SHADOW_SIGNAL_)' \
    /etc/price-collector/shadow-signal.env
)
```

Expected fixed values include:

```text
SHADOW_SIGNAL_ENABLED=true
SHADOW_SIGNAL_POLL_MS=100
SHADOW_SIGNAL_TTL_MS=2000
```

The primary model and replay-derived freshness/reference settings do not belong
in the environment file.

## Step 5 — Run the fail-closed artifact preflight

Do not install or start the systemd service unless this succeeds:

```bash
sudo -u pricecollector bash <<'BASH'
set -euo pipefail
set -a
. /etc/price-collector/shadow-signal.env
set +a
cd /opt/price-collector
.venv/bin/python - <<'PY'
from pathlib import Path

from price_collector.config import Settings
from price_collector.shadow_signal_artifact import load_activated_selection

settings = Settings()
assert settings.SHADOW_SIGNAL_ENABLED is True
activated = load_activated_selection(
    Path(settings.SHADOW_SIGNAL_SELECTION_PATH),
    settings.SHADOW_SIGNAL_SELECTION_SHA256,
    Path(settings.SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH),
    trusted_directory=Path(settings.SHADOW_SIGNAL_TRUSTED_DECISION_DIR),
)

assert activated.policy_version == "chronological_holdout_v2"
assert activated.primary_model.version == "catchup_ratio_l3000_b100"
assert activated.primary_model.lag_ms == 3000
assert str(activated.primary_model.beta) == "1"
assert activated.poll_ms == 100
assert activated.futures_stale_ms == 3000
assert activated.chainlink_stale_ms == 2500
assert activated.reference_max_gap_ms == 3000
assert activated.history_retention_ms == 10000
assert activated.max_future_skew_ms == 250
assert [model.version for model in activated.models] == [
    "catchup_ratio_l3000_b100",
    "catchup_ratio_l3500_b100",
    "catchup_ratio_l4000_b100",
]

print({
    "policy": activated.policy_version,
    "primary": activated.primary_model.version,
    "candidates": [model.version for model in activated.models],
    "selection_sha256": activated.selection_artifact_sha256,
    "fingerprint": activated.selection_fingerprint_sha256,
    "evidence_end_ms": activated.evidence_end_ms,
    "poll_ms": activated.poll_ms,
    "futures_stale_ms": activated.futures_stale_ms,
    "chainlink_stale_ms": activated.chainlink_stale_ms,
    "reference_max_gap_ms": activated.reference_max_gap_ms,
    "history_retention_ms": activated.history_retention_ms,
})
PY
BASH
```

## Step 6 — Install the unit and start Phase 4

The earlier `Unit ... could not be found` message means this installation step
had not run yet.

```bash
(
  set -euo pipefail
  cd /opt/price-collector

  sudo test -f deployment/price-collector-shadow-signal.service
  sudo test -f /etc/price-collector/shadow-signal.env

  sudo cp \
    deployment/price-collector-shadow-signal.service \
    /etc/systemd/system/price-collector-shadow-signal.service
  sudo systemctl daemon-reload
  sudo systemctl cat price-collector-shadow-signal --no-pager

  sudo systemctl restart \
    price-collector \
    price-collector-polymarket-chainlink \
    price-collector-binance-futures \
    price-collector-polymarket-probabilities \
    price-api

  sudo systemctl enable price-collector-shadow-signal
  sudo systemctl restart price-collector-shadow-signal

  for UNIT in \
    price-collector \
    price-collector-polymarket-chainlink \
    price-collector-binance-futures \
    price-collector-polymarket-probabilities \
    price-api \
    price-collector-shadow-signal
  do
    sudo systemctl is-active --quiet "$UNIT"
  done
)
```

Redis is not restarted. The worker initially passes through warm-up and waits
for a new Chainlink anchor before becoming valid.

## Step 7 — Verify the live shadow signal

Wait up to 30 seconds for a valid projection:

```bash
(
  set -euo pipefail
  SHADOW_KEY="btc:live:chainlink_shadow"
  VALID=0

  for ATTEMPT in $(seq 1 60); do
    VALID="$(redis-cli --raw GET "$SHADOW_KEY" | python3 -c '
import json
import sys
raw = sys.stdin.read().strip()
print(int(bool(raw) and json.loads(raw).get("valid") is True))
')"
    if [ "$VALID" -eq 1 ]; then
      break
    fi
    sleep 0.5
  done

  test "$VALID" -eq 1

  redis-cli --raw GET "$SHADOW_KEY" | python3 -m json.tool

  PTTL="$(redis-cli --raw PTTL "$SHADOW_KEY")"
  printf 'shadow_pttl_ms=%s\n' "$PTTL"
  test "$PTTL" -gt 0
  test "$PTTL" -le 2000

  test "$(redis-cli --raw EXISTS \
    btc:live:binance_spot \
    btc:live:chainlink \
    btc:live:futures)" -eq 3

  curl -fsS http://127.0.0.1:9000/healthz
)
```

Confirm the published model matches the immutable selection:

```bash
ACTIVE_SELECTION="$(sudo sed -n \
  's/^SHADOW_SIGNAL_SELECTION_PATH=//p' \
  /etc/price-collector/shadow-signal.env)"
SELECTION_SHA="$(sudo sed -n \
  's/^SHADOW_SIGNAL_SELECTION_SHA256=//p' \
  /etc/price-collector/shadow-signal.env)"

redis-cli --raw GET btc:live:chainlink_shadow | sudo python3 -c '
import hashlib
import json
import sys

selection_path, expected_sha = sys.argv[1:]
signal = json.load(sys.stdin)
with open(selection_path, "rb") as stream:
    selection_raw = stream.read()
selection = json.loads(selection_raw)
primary = selection["decision"]["provisional_primary_model"]

assert hashlib.sha256(selection_raw).hexdigest() == expected_sha
assert signal["selection_artifact_sha256"] == expected_sha
assert signal["model_version"] == primary["model_version"]
assert signal["horizon_ms"] == primary["horizon_ms"]
assert signal["beta"] == primary["beta"]
assert signal["selection_evidence_end_ms"] == primary["evidence_end_ms"]
print({
    "valid": signal["valid"],
    "status": signal["status"],
    "state": signal["state"],
    "model": signal["model_version"],
    "projected_chainlink": signal["projected_chainlink"],
    "pending_move_bps": signal["pending_move_bps"],
    "full_horizon_before_market_end": signal["full_horizon_before_market_end"],
})
' "$ACTIVE_SELECTION" "$SELECTION_SHA"
```

Inspect bounded service output:

```bash
sudo systemctl status price-collector-shadow-signal --no-pager
sudo journalctl -u price-collector-shadow-signal -n 100 --no-pager
sudo journalctl -u price-collector-binance-futures -n 50 --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 50 --no-pager
```

A journal-rotation notice only means older log entries are unavailable. It is
not a worker failure.

## Step 8 — Prove worker-death expiry

This test stops only the experimental worker. A trap restarts it if any check
fails:

```bash
(
  set -euo pipefail
  SHADOW_KEY="btc:live:chainlink_shadow"

  test "$(redis-cli --raw EXISTS \
    btc:live:binance_spot \
    btc:live:chainlink \
    btc:live:futures)" -eq 3

  sudo systemctl stop price-collector-shadow-signal
  trap 'sudo systemctl start price-collector-shadow-signal' EXIT

  sleep 3
  test "$(redis-cli --raw EXISTS "$SHADOW_KEY")" -eq 0
  test "$(redis-cli --raw EXISTS \
    btc:live:binance_spot \
    btc:live:chainlink \
    btc:live:futures)" -eq 3

  sudo systemctl start price-collector-shadow-signal
  trap - EXIT
  sudo systemctl is-active --quiet price-collector-shadow-signal
)
```

## Step 9 — Roll back Phase 4 if needed

Rollback stops only the experimental worker. It does not touch Redis,
PostgreSQL, either producer, or the evidence files:

```bash
(
  set -euo pipefail
  sudo systemctl disable --now price-collector-shadow-signal
  sleep 3
  test "$(redis-cli --raw EXISTS btc:live:chainlink_shadow)" -eq 0
  test "$(redis-cli --raw EXISTS \
    btc:live:binance_spot \
    btc:live:chainlink \
    btc:live:futures)" -eq 3
)
```

## Phase 4 is complete when

- all tests pass;
- the artifact preflight succeeds;
- all six services are active;
- `btc:live:chainlink_shadow` becomes valid;
- its model is `catchup_ratio_l3000_b100` from the selection artifact;
- its Redis PTTL stays between 1 and 2,000 ms;
- stopping the worker removes only the shadow key after 3 seconds; and
- the existing API health check remains successful.

The next build-order checkpoint is Phase 5: matured forecast evaluations. Do
not expose the signal through the API or dashboard as part of this migration.
