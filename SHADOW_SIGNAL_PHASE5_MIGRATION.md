# Shadow-Signal Phase 5 Migration

This procedure enables matured forecast evaluations for the already accepted
and deployed shadow signal. Run it on the Ubuntu droplet only after the Phase 5
change has been pushed to GitHub and the Phase 4 Redis worker is healthy.

This is step 5 of the shadow-signal build order in `engine.md`. It is unrelated
to the Binance futures-collector Phase 5 source cutover and does not complete
the deferred high-resolution raw-capture Phase 4 partition/retention canary.
It adds no API field and no dashboard code.

The live evaluator is causal over Chainlink cache states returned by successful
100 ms worker polls. It stamps generation after `MGET`, rejects inputs received
after that stamp, and fails outstanding outcomes closed across a gap longer
than two poll intervals. Redis is latest-value-only, so a state created and
overwritten entirely between polls is not reconstructable; the raw replay
remains the event-complete authority for model selection and sub-poll timing.

The commands preserve the existing selection path, replay-configuration path,
selection hash, and database password. Stop on any failed assertion.

## 1. Pull the code, install dependencies, and run focused tests

```bash
set -euo pipefail
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal_evaluation.py \
  tests/test_shadow_signal_evaluation_db.py \
  tests/test_shadow_signal_collector.py \
  tests/test_db.py \
  tests/test_config.py \
  tests/test_deployment.py \
  -q
```

Do not restart the worker yet. The schema must exist before evaluations are
enabled.

## 2. Apply the schema before restarting the service

```bash
set -euo pipefail
cd /opt/price-collector
sudo -u postgres psql \
  -v ON_ERROR_STOP=1 \
  -d price_collector \
  -f /opt/price-collector/schema.sql
```

The migration is idempotent. It creates `public.shadow_signal_evaluations`, its
indexes and constraints, and the exact writer/reader grants.

## 3. Verify the table, primary key, and least-privilege grants

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
SELECT to_regclass('public.shadow_signal_evaluations') AS evaluation_table;

SELECT pg_get_constraintdef(oid) AS primary_key
FROM pg_constraint
WHERE conrelid = 'public.shadow_signal_evaluations'::regclass
  AND contype = 'p';

SELECT
    has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'SELECT'
    ) AS writer_select,
    has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'INSERT'
    ) AS writer_insert,
    has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'DELETE'
    ) AS writer_delete,
    has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'UPDATE'
    ) AS writer_update,
    has_table_privilege(
        'price_reader',
        'public.shadow_signal_evaluations',
        'SELECT'
    ) AS reader_select;
SQL
```

Expected results:

- `evaluation_table` is `shadow_signal_evaluations`.
- The primary key is
  `(model_version, generated_ms, horizon_ms)`.
- Writer `SELECT`, `INSERT`, and `DELETE` are `t`.
- Writer `UPDATE` and reader `SELECT` are `f`.

## 4. Enable the bounded evaluation writer without replacing existing settings

The following command copies the existing writer URL from `collector.env` and
updates only the Phase 5 keys in `shadow-signal.env`. It does not print the URL
and does not replace any trusted decision path or hash.

```bash
sudo python3 - <<'PY'
from pathlib import Path
import grp
import os
import tempfile

shadow_path = Path('/etc/price-collector/shadow-signal.env')
collector_path = Path('/etc/price-collector/collector.env')

if not shadow_path.is_file():
    raise SystemExit('STOP: shadow-signal.env does not exist')
if not collector_path.is_file():
    raise SystemExit('STOP: collector.env does not exist')

writer_lines = [
    line
    for line in collector_path.read_text().splitlines()
    if line.startswith('DATABASE_URL=')
]
if len(writer_lines) != 1:
    raise SystemExit('STOP: collector.env must contain exactly one DATABASE_URL')
database_url = writer_lines[0].partition('=')[2]
if not database_url or 'REPLACE_ME' in database_url:
    raise SystemExit('STOP: collector.env DATABASE_URL is not production-ready')

updates = {
    'DATABASE_URL': database_url,
    'SHADOW_SIGNAL_EVALUATION_ENABLED': 'true',
    'SHADOW_SIGNAL_EVALUATION_INTERVAL_MS': '500',
    'SHADOW_SIGNAL_EVALUATION_QUEUE_MAX': '5000',
    'SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS': '500',
    'SHADOW_SIGNAL_EVALUATION_FLUSH_MS': '1000',
    'SHADOW_SIGNAL_EVALUATION_RETRY_MS': '5000',
    'SHADOW_SIGNAL_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS': '10',
    'SHADOW_SIGNAL_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS': '5',
    'SHADOW_SIGNAL_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS': '5',
    'SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS': '168',
    'SHADOW_SIGNAL_EVALUATION_RETENTION_CHECK_SECONDS': '300',
    'SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS': '5000',
}

output = []
written = set()
for line in shadow_path.read_text().splitlines():
    key = line.partition('=')[0]
    if key == 'READ_DATABASE_URL':
        continue
    if key in updates:
        if key not in written:
            output.append(f'{key}={updates[key]}')
            written.add(key)
        continue
    output.append(line)

for key, value in updates.items():
    if key not in written:
        output.append(f'{key}={value}')

fd, temporary_name = tempfile.mkstemp(
    prefix='.shadow-signal.env.',
    dir=shadow_path.parent,
    text=True,
)
try:
    with os.fdopen(fd, 'w') as temporary:
        temporary.write('\n'.join(output) + '\n')
        temporary.flush()
        os.fsync(temporary.fileno())
    os.chown(temporary_name, 0, grp.getgrnam('pricecollector').gr_gid)
    os.chmod(temporary_name, 0o640)
    os.replace(temporary_name, shadow_path)
finally:
    if os.path.exists(temporary_name):
        os.unlink(temporary_name)

print('updated Phase 5 keys; trusted decision settings were preserved')
PY

sudo test "$(stat -c '%U:%G %a' /etc/price-collector/shadow-signal.env)" \
  = 'root:pricecollector 640'
sudo grep -q '^DATABASE_URL=postgresql://price_writer:' \
  /etc/price-collector/shadow-signal.env
if sudo grep -q '^READ_DATABASE_URL=' \
  /etc/price-collector/shadow-signal.env; then
  echo 'STOP: READ_DATABASE_URL must not be in shadow-signal.env'
  exit 1
fi
sudo grep -E '^SHADOW_SIGNAL_EVALUATION_' \
  /etc/price-collector/shadow-signal.env
```

Do not print or paste the complete `DATABASE_URL` into a terminal transcript.

## 5. Validate configuration as the service user

```bash
sudo -u pricecollector bash -c '
  set -euo pipefail
  set -a
  . /etc/price-collector/shadow-signal.env
  set +a
  cd /opt/price-collector
  .venv/bin/python - <<"PY"
from price_collector.config import Settings

settings = Settings()
assert settings.SHADOW_SIGNAL_ENABLED is True
assert settings.SHADOW_SIGNAL_EVALUATION_ENABLED is True
assert settings.SHADOW_SIGNAL_EVALUATION_INTERVAL_MS == 500
assert settings.SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS <= (
    settings.SHADOW_SIGNAL_EVALUATION_QUEUE_MAX
)
assert settings.SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS == 168
assert settings.SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS == 5000
assert settings.DATABASE_URL
assert settings.READ_DATABASE_URL is None
print({
    "shadow_enabled": settings.SHADOW_SIGNAL_ENABLED,
    "evaluation_enabled": settings.SHADOW_SIGNAL_EVALUATION_ENABLED,
    "interval_ms": settings.SHADOW_SIGNAL_EVALUATION_INTERVAL_MS,
    "queue_max": settings.SHADOW_SIGNAL_EVALUATION_QUEUE_MAX,
    "batch_max_rows": settings.SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS,
    "retention_hours": settings.SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS,
    "retention_batch_rows": (
        settings.SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS
    ),
})
PY
'
```

## 6. Install the updated unit and restart only the shadow worker

```bash
set -euo pipefail
cd /opt/price-collector
sudo cp \
  deployment/price-collector-shadow-signal.service \
  /etc/systemd/system/price-collector-shadow-signal.service
sudo systemctl daemon-reload
sudo systemctl enable price-collector-shadow-signal
sudo systemctl restart price-collector-shadow-signal
sudo systemctl is-active price-collector-shadow-signal
sudo systemctl status price-collector-shadow-signal --no-pager
sudo journalctl \
  -u price-collector-shadow-signal \
  --since '5 minutes ago' \
  -n 150 \
  --no-pager
sudo journalctl \
  -u price-collector-shadow-signal \
  --since '5 minutes ago' \
  -n 150 \
  --no-pager | grep -F 'shadow_signal_evaluation_started' >/dev/null
```

The unit is ordered after local PostgreSQL, Redis, and both source producers.
The writer remains lazy and noncritical after startup.

## 7. Verify that Phase 4 Redis publication still refreshes

```bash
set -euo pipefail
REDIS_VERIFY_AFTER_MS="$(date +%s%3N)"
# Any value written by the pre-restart process has a TTL of at most two seconds.
sleep 3
SHADOW_JSON=''
for ATTEMPT in $(seq 1 60); do
  SHADOW_JSON="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
    GET btc:live:chainlink_shadow)"
  if [ -n "$SHADOW_JSON" ]; then
    break
  fi
  sleep 0.5
done
test -n "$SHADOW_JSON"
python3 - "$SHADOW_JSON" "$REDIS_VERIFY_AFTER_MS" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
verify_after_ms = int(sys.argv[2])
assert payload['schema_version'] == 1
assert payload['mode'] == 'shadow'
assert payload['generated_ms'] >= verify_after_ms
assert payload['model_version'] in {
    'catchup_ratio_l3000_b100',
    'catchup_ratio_l3500_b100',
    'catchup_ratio_l4000_b100',
}
print({
    'model_version': payload['model_version'],
    'generated_ms': payload['generated_ms'],
    'valid': payload['valid'],
    'status': payload['status'],
})
PY

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 \
  PTTL btc:live:chainlink_shadow)"
printf 'shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000
```

Both valid and invalid Redis payloads are legitimate. The important checks are
that the payload is fresh, typed, and expiring.

## 8. Wait for matured rows, then verify all candidates and coverage

```bash
set -euo pipefail
EVALUATION_VERIFY_AFTER_MS="$(date +%s%3N)"
for ATTEMPT in $(seq 1 60); do
  MODEL_COUNT="$(sudo -u postgres psql -At -d price_collector -c "
    SELECT count(DISTINCT model_version)
    FROM shadow_signal_evaluations
    WHERE generated_ms >= $EVALUATION_VERIFY_AFTER_MS;
  ")"
  if [ "$MODEL_COUNT" -eq 3 ]; then
    break
  fi
  sleep 0.5
done
test "${MODEL_COUNT:-0}" -eq 3

MODELS="$(sudo -u postgres psql -At -d price_collector -c "
  SELECT string_agg(DISTINCT model_version, ',' ORDER BY model_version)
  FROM shadow_signal_evaluations
  WHERE generated_ms >= $EVALUATION_VERIFY_AFTER_MS;
")"
test "$MODELS" = \
  'catchup_ratio_l3000_b100,catchup_ratio_l3500_b100,catchup_ratio_l4000_b100'

sudo -u postgres psql \
  -v ON_ERROR_STOP=1 \
  -v verify_after_ms="$EVALUATION_VERIFY_AFTER_MS" \
  -d price_collector <<'SQL'
SELECT
    model_version,
    count(*) AS attempts,
    count(*) FILTER (WHERE valid) AS valid_attempts,
    count(*) FILTER (WHERE NOT valid) AS invalid_attempts,
    count(*) FILTER (WHERE actual_chainlink IS NOT NULL) AS causal_actuals,
    min(generated_ms) AS first_generated_ms,
    max(generated_ms) AS last_generated_ms
FROM shadow_signal_evaluations
WHERE generated_ms >= :'verify_after_ms'::bigint
GROUP BY model_version
ORDER BY model_version;
SQL
```

Three model rows must print. `invalid_attempts` may be zero during a completely
healthy minute; invalid attempts are nevertheless persisted whenever they
occur and remain part of the coverage denominator.

## 9. Verify causal maturation, signed errors, and bucket integrity

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
WITH recent AS (
    SELECT *
    FROM shadow_signal_evaluations
    WHERE generated_ms >= (
        extract(epoch FROM clock_timestamp()) * 1000
    )::bigint - 600000
), duplicate_model_buckets AS (
    SELECT model_version, generated_ms / 500 AS bucket
    FROM recent
    GROUP BY model_version, generated_ms / 500
    HAVING count(*) > 1
)
SELECT
    (SELECT count(*) FROM duplicate_model_buckets)
        AS duplicate_model_bucket_rows,
    count(*) FILTER (
        WHERE target_ms <> generated_ms + horizon_ms
    ) AS wrong_targets,
    count(*) FILTER (
        WHERE matured_ms < target_ms
    ) AS prematurely_matured,
    count(*) FILTER (
        WHERE actual_chainlink_received_ms > target_ms
    ) AS post_target_actuals,
    count(*) FILTER (
        WHERE valid
          AND (
            chainlink_at_forecast_received_ms > generated_ms
            OR futures_now_received_ms > generated_ms
          )
    ) AS post_generated_valid_inputs,
    count(*) FILTER (
        WHERE forecast_error IS NOT NULL
          AND abs(
              forecast_error - (projected_chainlink - actual_chainlink)
          ) > 0.000000000000000002
    ) AS wrong_forecast_errors,
    count(*) FILTER (
        WHERE baseline_error IS NOT NULL
          AND abs(
              baseline_error - (chainlink_at_forecast - actual_chainlink)
          ) > 0.000000000000000002
    ) AS wrong_baseline_errors
FROM recent;

WITH complete_buckets AS (
    SELECT generated_ms, count(DISTINCT model_version) AS candidates
    FROM shadow_signal_evaluations
    WHERE generated_ms < (
        extract(epoch FROM clock_timestamp()) * 1000
    )::bigint - 10000
      AND generated_ms >= (
        extract(epoch FROM clock_timestamp()) * 1000
    )::bigint - 600000
    GROUP BY generated_ms
)
SELECT count(*) AS buckets_missing_a_candidate
FROM complete_buckets
WHERE candidates <> 3;
SQL
```

Every printed diagnostic must be `0`. A nonzero
`buckets_missing_a_candidate` may indicate queue drops or a restart inside the
ten-minute inspection range; inspect the bounded journal before accepting it.

## 10. Verify idempotency and the seven-day retention configuration

The transaction below attempts to insert an existing row and then rolls back.
`source_rows` should be `1` and `rows_inserted` must be `0`.

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
BEGIN;
WITH source_row AS (
    SELECT *
    FROM shadow_signal_evaluations
    ORDER BY generated_ms DESC, model_version
    LIMIT 1
), duplicate_attempt AS (
    INSERT INTO shadow_signal_evaluations
    SELECT * FROM source_row
    ON CONFLICT (model_version, generated_ms, horizon_ms) DO NOTHING
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM source_row) AS source_rows,
    (SELECT count(*) FROM duplicate_attempt) AS rows_inserted;
ROLLBACK;

SELECT
    count(*) FILTER (
        WHERE generated_ms < (
            extract(epoch FROM clock_timestamp()) * 1000
        )::bigint - 168 * 3600000
    ) AS rows_older_than_retention,
    min(generated_ms) AS oldest_generated_ms,
    max(generated_ms) AS newest_generated_ms,
    pg_size_pretty(pg_total_relation_size(
        'public.shadow_signal_evaluations'
    )) AS total_relation_size
FROM shadow_signal_evaluations;
SQL

sudo grep -E \
  '^SHADOW_SIGNAL_EVALUATION_(RETENTION_HOURS|RETENTION_CHECK_SECONDS|RETENTION_BATCH_ROWS)=' \
  /etc/price-collector/shadow-signal.env
```

The configured values must be `168` hours, `300` seconds, and `5000` rows. The
batch can out-delete a conservative five-candidate capacity envelope at the
500 ms cadence. Retention removes old rows in bounded transactions, so a pre-existing
backlog can still decline over multiple checks rather than in one large lock.

## 11. Final bounded health check

```bash
sudo systemctl is-active \
  price-collector-binance-futures \
  price-collector-polymarket-chainlink \
  price-collector-shadow-signal
redis-cli -h 127.0.0.1 -p 6379 PING
curl --fail --silent --show-error http://127.0.0.1:9000/healthz
sudo journalctl \
  -u price-collector-shadow-signal \
  --since '15 minutes ago' \
  -n 250 \
  --no-pager
```

`shadow_signal_evaluation_queue_drop` is rate-limited to the first and every
hundredth queue overflow. `shadow_evaluation_batch_failed`,
`shadow_evaluation_cleanup_failed`, and
`shadow_evaluation_backend_close_failed` concern evidence coverage and require
investigation; failed batches are requeued and retried until bounded capacity
requires shedding old evidence. `shadow_signal_evaluation_observation_gap`
means outstanding targets across a polling gap were deliberately left without
an actual. `shadow_signal_evaluation_writer_closed` prints the final persisted,
dropped, failure, cleanup, and active-batch counters. None of these conditions
may stop `btc:live:chainlink_shadow` from refreshing.
Do not stop PostgreSQL deliberately to test this isolation because the other
collectors also use it.

## 12. Roll back Phase 5 while keeping Phase 4 publication

This rollback disables matured evaluations and removes the unused writer URL
from the shadow service environment. It preserves the trusted selection,
replay configuration, Redis shadow publication, harmless bounded-writer
settings, and stored rows.

```bash
set -euo pipefail
sudo python3 - <<'PY'
from pathlib import Path
import grp
import os
import tempfile

path = Path('/etc/price-collector/shadow-signal.env')
if not path.is_file():
    raise SystemExit('STOP: shadow-signal.env does not exist')

output = []
enabled_written = False
for line in path.read_text().splitlines():
    key = line.partition('=')[0]
    if key == 'DATABASE_URL':
        continue
    if key == 'SHADOW_SIGNAL_EVALUATION_ENABLED':
        if not enabled_written:
            output.append('SHADOW_SIGNAL_EVALUATION_ENABLED=false')
            enabled_written = True
        continue
    output.append(line)
if not enabled_written:
    output.append('SHADOW_SIGNAL_EVALUATION_ENABLED=false')

fd, temporary_name = tempfile.mkstemp(
    prefix='.shadow-signal.env.',
    dir=path.parent,
    text=True,
)
try:
    with os.fdopen(fd, 'w') as temporary:
        temporary.write('\n'.join(output) + '\n')
        temporary.flush()
        os.fsync(temporary.fileno())
    os.chown(temporary_name, 0, grp.getgrnam('pricecollector').gr_gid)
    os.chmod(temporary_name, 0o640)
    os.replace(temporary_name, path)
finally:
    if os.path.exists(temporary_name):
        os.unlink(temporary_name)
PY
sudo test "$(stat -c '%U:%G %a' /etc/price-collector/shadow-signal.env)" \
  = 'root:pricecollector 640'
sudo grep -q '^SHADOW_SIGNAL_EVALUATION_ENABLED=false$' \
  /etc/price-collector/shadow-signal.env
if sudo grep -q '^DATABASE_URL=' \
  /etc/price-collector/shadow-signal.env; then
  echo 'STOP: rollback left DATABASE_URL in shadow-signal.env'
  exit 1
fi
sudo systemctl restart price-collector-shadow-signal
sudo systemctl is-active price-collector-shadow-signal

BEFORE="$(sudo -u postgres psql -At -d price_collector -c \
  'SELECT coalesce(max(generated_ms), 0) FROM shadow_signal_evaluations;')"
sleep 10
AFTER="$(sudo -u postgres psql -At -d price_collector -c \
  'SELECT coalesce(max(generated_ms), 0) FROM shadow_signal_evaluations;')"
printf 'evaluation_max_before=%s\nevaluation_max_after=%s\n' "$BEFORE" "$AFTER"
test "$BEFORE" = "$AFTER"

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 \
  PTTL btc:live:chainlink_shadow)"
printf 'shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000

sudo journalctl \
  -u price-collector-shadow-signal \
  --since '5 minutes ago' \
  -n 100 \
  --no-pager
```

Leave the table in place. Dropping it is unnecessary and would destroy the
evidence already collected. Re-enable Phase 5 by repeating steps 4 through 11
after the cause has been corrected; step 4 restores the writer URL without
changing the trusted decision settings.
