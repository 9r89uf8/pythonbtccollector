# Shadow-Signal Phase 6 Migration

This procedure exposes the already-deployed shadow signal through the existing
Redis-only `GET /markets/current/live` endpoint. It is step 6 of the
shadow-signal build order in `engine.md`. It does not add another route, a
PostgreSQL query, schema change, environment setting, systemd dependency, or
dashboard code. Dashboard implementation remains Phase 7 in a different
repository.

The response adds this stable envelope:

```json
{
  "signals": {
    "chainlink_catchup": null
  }
}
```

When `btc:live:chainlink_shadow` contains a well-formed value,
`chainlink_catchup` is the complete typed shadow payload plus API-computed
`signal_age_ms`. Both valid and invalid model observations are returned as
objects. An absent, expired, or malformed experimental shadow value is isolated
as `null`; it does not turn otherwise healthy source prices into HTTP 503. A
Redis read failure or malformed source-price payload retains the existing HTTP
503 behavior.

The API obtains all three source-price values and the shadow value with one
Redis `MGET`. It performs no PostgreSQL query on this request path. Stop on any
failed assertion below.

## 1. Confirm the Phase 4 worker and source services are healthy

Phase 5 evaluation persistence may be enabled or disabled independently. Phase
6 requires only the Phase 4 shadow Redis publication.

```bash
set -euo pipefail

for UNIT in \
  price-collector \
  price-collector-polymarket-chainlink \
  price-collector-binance-futures \
  price-collector-shadow-signal \
  price-api
do
  sudo systemctl is-active --quiet "$UNIT"
done

SOURCE_KEYS="$(redis-cli -h 127.0.0.1 -p 6379 --raw EXISTS \
  btc:live:binance_spot \
  btc:live:chainlink \
  btc:live:futures)"
printf 'source_keys_present=%s\n' "$SOURCE_KEYS"
test "$SOURCE_KEYS" -eq 3

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
  PTTL btc:live:chainlink_shadow)"
printf 'shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000

redis-cli -h 127.0.0.1 -p 6379 PING | grep -Fx PONG
curl --fail --silent --show-error http://127.0.0.1:9000/healthz
```

Do not continue if the shadow key is already absent or not expiring. Repair the
Phase 4 worker before changing the API contract.

## 2. Pull the code, install dependencies, and run focused tests

Run this only after the Phase 6 change has been pushed to GitHub:

```bash
set -euo pipefail
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_live_cache.py \
  tests/test_api.py \
  tests/test_deployment.py \
  -q

sudo grep -q '^REDIS_HOST=127.0.0.1$' \
  /etc/price-collector/api.env
if sudo grep -Eq '^(DATABASE_URL|SHADOW_SIGNAL_[A-Z0-9_]*)=' \
  /etc/price-collector/api.env; then
  echo 'STOP: api.env contains a writer or shadow-worker setting'
  exit 1
fi
```

There is no schema migration. Do not run `schema.sql`, replace any environment
file, copy a systemd unit, run `systemctl daemon-reload`, or restart Redis or
PostgreSQL for Phase 6. In particular, do not place shadow artifact paths,
`DATABASE_URL`, or a new feature flag in `/etc/price-collector/api.env`.

## 3. Restart the three source-price services that import `live_cache.py`

The shared module changed, so load the same release into every source-price
process before restarting the shadow worker:

```bash
set -euo pipefail

sudo systemctl restart price-collector
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl restart price-collector-binance-futures

for UNIT in \
  price-collector \
  price-collector-polymarket-chainlink \
  price-collector-binance-futures
do
  sudo systemctl is-active --quiet "$UNIT"
done

for ATTEMPT in $(seq 1 60); do
  SOURCE_KEYS="$(redis-cli -h 127.0.0.1 -p 6379 --raw EXISTS \
    btc:live:binance_spot \
    btc:live:chainlink \
    btc:live:futures)"
  if [ "$SOURCE_KEYS" -eq 3 ]; then
    break
  fi
  sleep 0.5
done
printf 'source_keys_present=%s\n' "${SOURCE_KEYS:-0}"
test "${SOURCE_KEYS:-0}" -eq 3
```

`price-collector-polymarket-probabilities` does not import `live_cache.py` and
does not need a Phase 6 restart.

## 4. Restart the shadow worker and wait for a new typed payload

The timestamp check rejects a value left by the pre-restart worker. A valid or
invalid newly generated payload is acceptable.

```bash
set -euo pipefail

sudo systemctl restart price-collector-shadow-signal
sudo systemctl is-active --quiet price-collector-shadow-signal
SHADOW_RESTART_AFTER_MS="$(date +%s%3N)"

SHADOW_JSON=''
SHADOW_READY=0
for ATTEMPT in $(seq 1 60); do
  SHADOW_JSON="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
    GET btc:live:chainlink_shadow)"
  if [ -n "$SHADOW_JSON" ] && \
     python3 - "$SHADOW_JSON" "$SHADOW_RESTART_AFTER_MS" <<'PY' >/dev/null
import json
import sys

payload = json.loads(sys.argv[1])
assert payload['schema_version'] == 1
assert payload['mode'] == 'shadow'
assert payload['generated_ms'] >= int(sys.argv[2])
PY
  then
    SHADOW_READY=1
    break
  fi
  sleep 0.5
done
test "$SHADOW_READY" -eq 1

python3 - "$SHADOW_JSON" <<'PY'
import json
import sys

signal = json.loads(sys.argv[1])
assert isinstance(signal['valid'], bool)
assert signal['estimated_lag_ms'] == signal['horizon_ms']

decimal_fields = (
    'beta',
    'current_chainlink',
    'projected_chainlink',
    'pending_move',
    'pending_move_bps',
    'futures_now',
    'futures_reference',
)
for field in decimal_fields:
    assert signal[field] is None or isinstance(signal[field], str)

projection_fields = (
    'projected_chainlink',
    'pending_move',
    'pending_move_bps',
    'direction',
)
if signal['valid']:
    assert signal['status'] == 'valid'
    assert signal['invalid_reasons'] == []
    assert all(signal[field] is not None for field in projection_fields)
else:
    assert signal['status'] != 'valid'
    assert signal['invalid_reasons']
    assert all(signal[field] is None for field in projection_fields)

print({
    'generated_ms': signal['generated_ms'],
    'model_version': signal['model_version'],
    'valid': signal['valid'],
    'status': signal['status'],
    'invalid_reasons': signal['invalid_reasons'],
})
PY

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
  PTTL btc:live:chainlink_shadow)"
printf 'shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000
```

Startup can briefly publish `warming_up_futures_history` or
`waiting_for_new_chainlink_anchor`. Those are legitimate invalid observations,
not stale valid forecasts.

## 5. Restart the API last

The API remains independent of the shadow service. Its unit must not gain
`After`, `Wants`, or `Requires` entries for
`price-collector-shadow-signal.service`.

```bash
set -euo pipefail

if sudo systemctl cat price-api | \
   grep -F 'price-collector-shadow-signal.service'; then
  echo 'STOP: price-api must not depend on the optional shadow worker'
  exit 1
fi

sudo systemctl restart price-api
sudo systemctl is-active --quiet price-api

API_INVOCATION_ID="$(sudo systemctl show price-api \
  --property=InvocationID \
  --value)"
test -n "$API_INVOCATION_ID"

API_READY=0
for ATTEMPT in $(seq 1 60); do
  if curl --fail --silent --show-error \
    http://127.0.0.1:9000/healthz >/dev/null; then
    API_READY=1
    break
  fi
  sleep 0.5
done
test "$API_READY" -eq 1
printf 'price_api_invocation_id=%s\n' "$API_INVOCATION_ID"
```

## 6. Verify the object contract for valid and invalid signals

This check accepts either model-validity state, but it requires a well-formed
object and the API-only age field. It does not require or query PostgreSQL.

```bash
set -euo pipefail

LIVE_JSON="$(curl --fail --silent --show-error \
  http://127.0.0.1:9000/markets/current/live)"
python3 - "$LIVE_JSON" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
assert isinstance(payload['server_time_ms'], int)
assert set(payload['signals']) == {'chainlink_catchup'}
signal = payload['signals']['chainlink_catchup']
assert isinstance(signal, dict)

assert signal['schema_version'] == 1
assert signal['mode'] == 'shadow'
assert isinstance(signal['signal_age_ms'], int)
assert signal['signal_age_ms'] >= 0
assert signal['signal_age_ms'] == max(
    0,
    payload['server_time_ms'] - signal['generated_ms'],
)
assert isinstance(signal['valid'], bool)
assert signal['estimated_lag_ms'] == signal['horizon_ms']

decimal_fields = (
    'beta',
    'current_chainlink',
    'projected_chainlink',
    'pending_move',
    'pending_move_bps',
    'futures_now',
    'futures_reference',
)
for field in decimal_fields:
    assert signal[field] is None or isinstance(signal[field], str)

projection_fields = (
    'projected_chainlink',
    'pending_move',
    'pending_move_bps',
    'direction',
)
if signal['valid']:
    assert signal['status'] == 'valid'
    assert signal['invalid_reasons'] == []
    assert all(signal[field] is not None for field in projection_fields)
else:
    assert signal['status'] != 'valid'
    assert signal['invalid_reasons']
    assert all(signal[field] is None for field in projection_fields)

for path in (
    ('prices', 'binance_spot'),
    ('prices', 'chainlink'),
    ('futures', 'last'),
):
    value = payload
    for component in path:
        value = value[component]
    assert isinstance(value, dict)

print({
    'server_time_ms': payload['server_time_ms'],
    'model_version': signal['model_version'],
    'signal_age_ms': signal['signal_age_ms'],
    'valid': signal['valid'],
    'status': signal['status'],
    'projected_chainlink': signal['projected_chainlink'],
    'pending_move_bps': signal['pending_move_bps'],
    'full_horizon_before_market_end': (
        signal['full_horizon_before_market_end']
    ),
})
PY
```

The shadow payload's ages and market fields describe its own generation time.
The top-level source prices are read at API request time, so their values may be
newer. Near a five-minute boundary, the nested signal market can also differ
briefly from the API's top-level current market.

## 7. Prove worker-stop isolation, null output, and recovery

This bounded test stops only the experimental worker. The trap restarts it if
an assertion or command fails. The API and all three source-price services must
remain active, the source keys must survive, and the endpoint must return HTTP
200 with a null signal after the shadow TTL expires.

```bash
set -euo pipefail

restore_shadow_worker() {
  sudo systemctl start price-collector-shadow-signal >/dev/null 2>&1 || true
}
trap restore_shadow_worker EXIT

sudo systemctl stop price-collector-shadow-signal
sleep 3

test "$(redis-cli -h 127.0.0.1 -p 6379 --raw \
  EXISTS btc:live:chainlink_shadow)" -eq 0
test "$(redis-cli -h 127.0.0.1 -p 6379 --raw EXISTS \
  btc:live:binance_spot \
  btc:live:chainlink \
  btc:live:futures)" -eq 3

for UNIT in \
  price-collector \
  price-collector-polymarket-chainlink \
  price-collector-binance-futures \
  price-api
do
  sudo systemctl is-active --quiet "$UNIT"
done

NULL_LIVE_JSON="$(curl --fail --silent --show-error \
  http://127.0.0.1:9000/markets/current/live)"
python3 - "$NULL_LIVE_JSON" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
assert payload['signals'] == {'chainlink_catchup': None}
assert payload['prices']['binance_spot']['value'] is not None
assert payload['prices']['chainlink']['value'] is not None
assert payload['futures']['last']['value'] is not None
print({
    'http_contract': 'source prices available, shadow signal null',
    'server_time_ms': payload['server_time_ms'],
})
PY

SHADOW_RECOVERY_AFTER_MS="$(date +%s%3N)"
sudo systemctl start price-collector-shadow-signal
sudo systemctl is-active --quiet price-collector-shadow-signal
trap - EXIT

SHADOW_RECOVERED=0
for ATTEMPT in $(seq 1 60); do
  RECOVERED_LIVE_JSON="$(curl --fail --silent --show-error \
    http://127.0.0.1:9000/markets/current/live)"
  if python3 - "$RECOVERED_LIVE_JSON" "$SHADOW_RECOVERY_AFTER_MS" \
    <<'PY' >/dev/null
import json
import sys

payload = json.loads(sys.argv[1])
signal = payload['signals']['chainlink_catchup']
assert isinstance(signal, dict)
assert signal['generated_ms'] >= int(sys.argv[2])
assert signal['signal_age_ms'] >= 0
PY
  then
    SHADOW_RECOVERED=1
    break
  fi
  sleep 0.5
done
test "$SHADOW_RECOVERED" -eq 1

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
  PTTL btc:live:chainlink_shadow)"
printf 'recovered_shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000
```

A malformed shadow payload has the same API availability behavior as the
expired key: `chainlink_catchup` becomes `null` and a rate-limited
`shadow_signal_live_cache_payload_invalid` warning is logged on the first and
every hundredth occurrence. Do not overwrite the production key to test that
path; the focused tests in step 2 cover it deterministically. Malformed source
prices remain fail-closed with HTTP 503.

## 8. Run bounded status, health, and log checks

Use each service's current invocation so the intentional restart and worker-stop
test do not mix old shutdown logs into acceptance.

```bash
set -euo pipefail

for UNIT in \
  price-collector \
  price-collector-polymarket-chainlink \
  price-collector-binance-futures \
  price-collector-shadow-signal \
  price-api
do
  sudo systemctl is-active --quiet "$UNIT"
  sudo systemctl status "$UNIT" --no-pager
done

redis-cli -h 127.0.0.1 -p 6379 PING | grep -Fx PONG
curl --fail --silent --show-error http://127.0.0.1:9000/healthz
curl --fail --silent --show-error \
  http://127.0.0.1:9000/markets/current/live >/dev/null

API_INVOCATION_ID="$(sudo systemctl show price-api \
  --property=InvocationID \
  --value)"
SHADOW_INVOCATION_ID="$(sudo systemctl show price-collector-shadow-signal \
  --property=InvocationID \
  --value)"
test -n "$API_INVOCATION_ID"
test -n "$SHADOW_INVOCATION_ID"

API_FAILURES="$(
  sudo journalctl \
    -u price-api \
    _SYSTEMD_INVOCATION_ID="$API_INVOCATION_ID" \
    -n 300 \
    --no-pager |
  awk '
    /shadow_signal_live_cache_payload_invalid/ ||
    /Traceback/ || /ERROR/ || /CRITICAL/ { failures += 1 }
    END { print failures + 0 }
  '
)"
printf 'phase6_api_failures=%s\n' "$API_FAILURES"
test "$API_FAILURES" -eq 0

SHADOW_FAILURES="$(
  sudo journalctl \
    -u price-collector-shadow-signal \
    _SYSTEMD_INVOCATION_ID="$SHADOW_INVOCATION_ID" \
    -n 300 \
    --no-pager |
  awk '
    /Traceback/ || /"level": "ERROR"/ || /"level": "CRITICAL"/ ||
    /shadow_evaluation_batch_failed/ ||
    /shadow_signal_evaluation_queue_drop/ { failures += 1 }
    END { print failures + 0 }
  '
)"
printf 'phase6_shadow_failures=%s\n' "$SHADOW_FAILURES"
test "$SHADOW_FAILURES" -eq 0

sudo journalctl \
  -u price-api \
  _SYSTEMD_INVOCATION_ID="$API_INVOCATION_ID" \
  -n 150 \
  --no-pager
sudo journalctl \
  -u price-collector-shadow-signal \
  _SYSTEMD_INVOCATION_ID="$SHADOW_INVOCATION_ID" \
  -n 150 \
  --no-pager

ss -ltnp | grep -E '127\.0\.0\.1:9000\b'
if ss -ltnp | grep -E '(^|[[:space:]])0\.0\.0\.0:9000\b'; then
  echo 'STOP: price-api is publicly bound'
  exit 1
fi
```

Do not treat a well-formed `valid=false` signal as an API failure. Its status
and reasons are part of the model contract. A malformed shadow warning, API or
shadow-worker failure, lost Redis TTL, non-loopback API listener, or inactive
service blocks acceptance.

## 9. Roll back Phase 6 without changing Phase 4 or Phase 5 state

There is no environment or schema toggle for the additive API contract. To roll
it back, first create and push a Git revert of the Phase 6 implementation. Then
run the following on the droplet. Do not use `git reset --hard`, drop the
evaluation table, disable the shadow worker, or alter its trusted artifacts.

```bash
set -euo pipefail
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_live_cache.py \
  tests/test_api.py \
  -q

sudo systemctl restart price-collector
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl restart price-collector-binance-futures

for UNIT in \
  price-collector \
  price-collector-polymarket-chainlink \
  price-collector-binance-futures
do
  sudo systemctl is-active --quiet "$UNIT"
done

sudo systemctl restart price-collector-shadow-signal
sudo systemctl is-active --quiet price-collector-shadow-signal
sudo systemctl restart price-api
sudo systemctl is-active --quiet price-api

API_READY=0
for ATTEMPT in $(seq 1 60); do
  if curl --fail --silent --show-error \
    http://127.0.0.1:9000/healthz >/dev/null; then
    API_READY=1
    break
  fi
  sleep 0.5
done
test "$API_READY" -eq 1

ROLLED_BACK_LIVE="$(curl --fail --silent --show-error \
  http://127.0.0.1:9000/markets/current/live)"
python3 - "$ROLLED_BACK_LIVE" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
assert 'signals' not in payload
assert payload['prices']['binance_spot']['value'] is not None
assert payload['prices']['chainlink']['value'] is not None
assert payload['futures']['last']['value'] is not None
print('Phase 6 API field rolled back; source-price response remains healthy')
PY

SHADOW_EXISTS=0
for ATTEMPT in $(seq 1 60); do
  SHADOW_EXISTS="$(redis-cli -h 127.0.0.1 -p 6379 --raw EXISTS \
    btc:live:chainlink_shadow)"
  if [ "$SHADOW_EXISTS" -eq 1 ]; then
    break
  fi
  sleep 0.5
done
test "$SHADOW_EXISTS" -eq 1
```

The last assertion confirms that rolling back API exposure does not roll back
the Phase 4 worker. If Phase 5 evaluation persistence was enabled before the API
rollback, leave it enabled; it is independent of Phase 6.

## 10. Acceptance checklist

Phase 6 is accepted only when all of the following are true:

- The focused live-cache, API, and deployment tests pass.
- No schema, production environment file, systemd unit, Redis configuration, or
  PostgreSQL configuration was changed.
- The three source-price services, shadow worker, and API are active on the new
  release; the probability collector was not unnecessarily restarted.
- `btc:live:chainlink_shadow` is expiring with a positive TTL no greater than
  2,000 ms.
- A present, well-formed signal returns as an object with an exact
  `signal_age_ms`; valid and invalid observations obey their respective null and
  status invariants.
- Stopping the worker removes only the expiring shadow key. The endpoint stays
  HTTP 200, returns `chainlink_catchup: null`, and keeps all three source prices.
- Restarting the worker restores a newly generated signal object without
  restarting the API.
- Current-invocation logs contain no malformed-shadow warning, traceback,
  evaluation batch failure, or queue drop, and the API remains bound only to
  `127.0.0.1:9000`.
- No dashboard or frontend asset was added to this repository.

Phase 6 exposes an experimental signal; it does not promote the signal to a
settlement, probability, execution, or market-close forecast. Phase 7 may
consume this backend contract from the separate dashboard repository.
