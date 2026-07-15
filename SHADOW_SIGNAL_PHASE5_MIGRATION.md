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

If the journal already reports a violation of
`shadow_signal_evaluations_check17`, stop the normal rollout and use
**Recover from the Phase 5 `check17` writer failure** at the end of this guide.
Do not leave the failing writer enabled while its retry queue grows.

## 1. Pull the code, install dependencies, and run focused tests

```bash
set -euo pipefail
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal_evaluation.py \
  tests/test_shadow_signal_evaluation_db.py \
  tests/test_shadow_signal_schema_hotfix.py \
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
    SELECT
        selection_schema_version,
        selection_policy_version,
        selection_fingerprint_sha256,
        selection_artifact_sha256,
        selection_evidence_end_ms,
        generated_ms,
        count(DISTINCT model_version) AS candidates
    FROM shadow_signal_evaluations
    WHERE generated_ms < (
        extract(epoch FROM clock_timestamp()) * 1000
    )::bigint - 10000
      AND generated_ms >= (
        extract(epoch FROM clock_timestamp()) * 1000
    )::bigint - 600000
    GROUP BY
        selection_schema_version,
        selection_policy_version,
        selection_fingerprint_sha256,
        selection_artifact_sha256,
        selection_evidence_end_ms,
        generated_ms
)
SELECT count(*) AS buckets_missing_a_candidate
FROM complete_buckets
WHERE candidates <> 3;
SQL
```

Every printed diagnostic must be `0`. The full selection provenance plus
`generated_ms` is the cohort identity; grouping only by time could incorrectly
combine two artifacts. A nonzero `buckets_missing_a_candidate` may be a legacy
pre-rollout partial cohort, a queue drop, or a restart inside the ten-minute
inspection range. Restrict acceptance evidence to the cohort-atomic rollout
and inspect the bounded journal before accepting it.

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
    )) AS total_relation_size,
    to_regclass(
        'public.shadow_signal_evaluations_retention_cohort_idx'
    ) AS retention_cohort_index
FROM shadow_signal_evaluations;
SQL

sudo grep -E \
  '^SHADOW_SIGNAL_EVALUATION_(RETENTION_HOURS|RETENTION_CHECK_SECONDS|RETENTION_BATCH_ROWS)=' \
  /etc/price-collector/shadow-signal.env
```

The configured values must be `168` hours, `300` seconds, and `5000` rows. The
batch can out-delete a conservative five-candidate capacity envelope at the
500 ms cadence. The retention row budget must fit at least one complete
candidate cohort. Cleanup examines a bounded ordered set and deletes only whole
cohorts, so a pre-existing backlog can still decline over multiple checks
rather than in one large lock.

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
hundredth dropped cohort. `shadow_evaluation_batch_failed`,
`shadow_evaluation_cleanup_failed`, and
`shadow_evaluation_backend_close_failed` concern evidence coverage and require
investigation; transient failed batches are requeued and retried as whole
cohorts until bounded capacity requires shedding old evidence. A deterministic
PostgreSQL integrity or data violation is bisected only at cohort boundaries
inside one transaction instead: the worker logs
`shadow_evaluation_record_rejected` and
`shadow_evaluation_batch_records_rejected`, drops the entire rejected cohort,
and continues. Isolation is capped at eight rejected cohorts per batch so a
schema-wide fault cannot produce unbounded database calls; if the cap is hit,
the remaining unprobed cohorts are deferred back to the bounded queue and
`shadow_evaluation_rejection_isolation_limit_reached` is logged. Any rejection
event requires investigation.
`shadow_signal_evaluation_observation_gap`
means outstanding targets across a polling gap were deliberately left without
an actual. Sequence-gap, regression, publisher-epoch-change, and
sequence-metadata-loss warnings likewise identify deliberately invalidated
outcome history. `shadow_signal_evaluation_writer_closed` prints row and cohort
offered, enqueued, persisted, rejected, deferred, dropped, queue-high-water,
failure, cleanup, and active-batch counters. None of these conditions may stop
`btc:live:chainlink_shadow` from refreshing.
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

## Recover from the Phase 5 `check17` writer failure

Use this recovery only when an already-enabled Phase 5 worker repeatedly logs
`shadow_evaluation_batch_failed` with
`shadow_signal_evaluations_check17`. The original projection constraint can
reject a correctly rounded `pending_move_bps` value because PostgreSQL applies
numeric division before multiplication. The corrected constraint computes
`pending_move * 10000 / chainlink_at_forecast`. It is mathematically
equivalent to the model calculation but avoids PostgreSQL rounding the
divide-first intermediate at a shorter scale.

Do not weaken the constraint, drop the table, delete existing evidence, or
stop PostgreSQL. The recovery keeps the Phase 4 Redis projection running while
Phase 5 persistence is disabled and repaired.

### Recovery 1. Disable evaluations and remove the unused writer credential

This atomic edit preserves every Phase 4 setting, trusted artifact path, and
artifact hash. It does not print the database URL.

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
  echo 'STOP: DATABASE_URL remains in shadow-signal.env'
  exit 1
fi

sudo systemctl restart price-collector-shadow-signal
sudo systemctl is-active price-collector-shadow-signal
```

Confirm that evaluation rows stop advancing while the Phase 4 Redis key stays
fresh and expiring:

```bash
set -euo pipefail
BEFORE="$(sudo -u postgres psql -At -d price_collector -c \
  'SELECT coalesce(max(generated_ms), 0) FROM shadow_signal_evaluations;')"
REDIS_VERIFY_AFTER_MS="$(date +%s%3N)"
sleep 10
AFTER="$(sudo -u postgres psql -At -d price_collector -c \
  'SELECT coalesce(max(generated_ms), 0) FROM shadow_signal_evaluations;')"
printf 'evaluation_max_before=%s\nevaluation_max_after=%s\n' \
  "$BEFORE" "$AFTER"
test "$BEFORE" = "$AFTER"

SHADOW_JSON="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
  GET btc:live:chainlink_shadow)"
test -n "$SHADOW_JSON"
python3 - "$SHADOW_JSON" "$REDIS_VERIFY_AFTER_MS" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
assert payload['schema_version'] == 1
assert payload['mode'] == 'shadow'
assert payload['generated_ms'] >= int(sys.argv[2])
print({
    'generated_ms': payload['generated_ms'],
    'model_version': payload['model_version'],
    'valid': payload['valid'],
})
PY

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 \
  PTTL btc:live:chainlink_shadow)"
printf 'shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000
```

### Recovery 2. Pull and test the corrected release while disabled

Run this only after the correction has been pushed to GitHub:

```bash
set -euo pipefail
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal_evaluation.py \
  tests/test_shadow_signal_evaluation_db.py \
  tests/test_shadow_signal_schema_hotfix.py \
  tests/test_shadow_signal_collector.py \
  tests/test_db.py \
  tests/test_config.py \
  tests/test_deployment.py \
  -q
sudo grep -q \
  'shadow_signal_evaluations_projection_consistency_check' \
  /opt/price-collector/schema.sql
```

Do not re-enable evaluations yet.

### Recovery 3. Apply the corrected schema while evaluations are disabled

```bash
set -euo pipefail
cd /opt/price-collector
sudo grep -q '^SHADOW_SIGNAL_EVALUATION_ENABLED=false$' \
  /etc/price-collector/shadow-signal.env
sudo -u postgres psql \
  -v ON_ERROR_STOP=1 \
  -d price_collector \
  -f /opt/price-collector/schema.sql
```

The schema migration conditionally replaces the original autogenerated
projection check. It is safe to repeat and does not rewrite evaluation rows.

### Recovery 4. Verify the deployed projection constraint semantically

The old autogenerated name must be absent. The replacement must exist, be
validated, multiply before dividing, and retain the complete projection
validity rules.

```bash
sudo -u postgres psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -P pager=off \
  -d price_collector <<'SQL'
DO $$
DECLARE
    constraint_validated boolean;
    constraint_definition text;
    normalized_definition text;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.shadow_signal_evaluations'::regclass
          AND conname = 'shadow_signal_evaluations_check17'
    ) THEN
        RAISE EXCEPTION 'old check17 constraint is still installed';
    END IF;

    SELECT convalidated, pg_get_constraintdef(oid, true)
    INTO constraint_validated, constraint_definition
    FROM pg_constraint
    WHERE conrelid = 'public.shadow_signal_evaluations'::regclass
      AND conname =
        'shadow_signal_evaluations_projection_consistency_check';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'projection consistency constraint is missing';
    END IF;
    IF NOT constraint_validated THEN
        RAISE EXCEPTION 'projection consistency constraint is not validated';
    END IF;

    normalized_definition := regexp_replace(
        constraint_definition,
        '[[:space:]()]',
        '',
        'g'
    );
    IF normalized_definition NOT LIKE
       '%pending_move*10000%/chainlink_at_forecast%'
       OR normalized_definition LIKE
       '%pending_move/chainlink_at_forecast%*10000%' THEN
        RAISE EXCEPTION
            'projection constraint does not multiply before dividing';
    END IF;
    IF constraint_definition NOT LIKE '%pending_move_bps%'
       OR constraint_definition NOT LIKE '%direction%'
       OR constraint_definition NOT LIKE '%projected_chainlink%' THEN
        RAISE EXCEPTION
            'projection constraint is missing required validity rules';
    END IF;
END
$$;

SELECT
    conname,
    convalidated,
    pg_get_constraintdef(oid, true) AS definition
FROM pg_constraint
WHERE conrelid = 'public.shadow_signal_evaluations'::regclass
  AND conname =
    'shadow_signal_evaluations_projection_consistency_check';
SQL
```

Stop if the block raises any exception.

### Recovery 5. Re-enable with the existing guarded configuration steps

Repeat **step 4** exactly to restore the writer URL without printing it and to
set `SHADOW_SIGNAL_EVALUATION_ENABLED=true`. Then run **step 5**. Do not copy an
example environment file over the production file.

### Recovery 6. Restart and verify Redis, table flow, and the bounded queue

```bash
set -euo pipefail
cd /opt/price-collector
sudo cp \
  deployment/price-collector-shadow-signal.service \
  /etc/systemd/system/price-collector-shadow-signal.service
sudo systemctl daemon-reload

sudo systemctl restart price-collector-shadow-signal
sudo systemctl is-active price-collector-shadow-signal
RECOVERY_INVOCATION_ID="$(sudo systemctl show \
  price-collector-shadow-signal \
  --property=InvocationID \
  --value)"
test -n "$RECOVERY_INVOCATION_ID"
RECOVERY_VERIFY_AFTER_MS="$(date +%s%3N)"

MODEL_COUNT=0
for ATTEMPT in $(seq 1 120); do
  MODEL_COUNT="$(sudo -u postgres psql -At -d price_collector -c "
    SELECT count(DISTINCT model_version)
    FROM shadow_signal_evaluations
    WHERE generated_ms >= $RECOVERY_VERIFY_AFTER_MS;
  ")"
  if [ "$MODEL_COUNT" -eq 3 ]; then
    break
  fi
  sleep 0.5
done
test "$MODEL_COUNT" -eq 3

sudo -u postgres psql \
  -v ON_ERROR_STOP=1 \
  -v verify_after_ms="$RECOVERY_VERIFY_AFTER_MS" \
  -d price_collector <<'SQL'
SELECT
    model_version,
    count(*) AS rows,
    min(generated_ms) AS first_generated_ms,
    max(generated_ms) AS last_generated_ms
FROM shadow_signal_evaluations
WHERE generated_ms >= :'verify_after_ms'::bigint
GROUP BY model_version
ORDER BY model_version;
SQL

EVALUATION_START_LOG="$(
  sudo journalctl \
    -u price-collector-shadow-signal \
    _SYSTEMD_INVOCATION_ID="$RECOVERY_INVOCATION_ID" \
    -o cat \
    --no-pager |
  grep -F 'shadow_signal_evaluation_started' |
  tail -n 1
)"
python3 - "$EVALUATION_START_LOG" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
assert payload['event'] == 'shadow_signal_evaluation_started'
assert payload['cadence_ms'] == 500
assert payload['queue_max_records'] == 5000
assert payload['batch_max_rows'] == 500
print({
    'cadence_ms': payload['cadence_ms'],
    'queue_max_records': payload['queue_max_records'],
    'batch_max_rows': payload['batch_max_rows'],
})
PY

sudo grep -q '^SHADOW_SIGNAL_EVALUATION_QUEUE_MAX=5000$' \
  /etc/price-collector/shadow-signal.env
RECOVERY_ERRORS="$(
  sudo journalctl \
    -u price-collector-shadow-signal \
    _SYSTEMD_INVOCATION_ID="$RECOVERY_INVOCATION_ID" \
    -n 500 \
    --no-pager |
  awk '
    /shadow_evaluation_batch_failed/ ||
    /shadow_evaluation_record_rejected/ ||
    /shadow_evaluation_batch_records_rejected/ ||
    /shadow_evaluation_rejection_isolation_limit_reached/ ||
    /shadow_signal_evaluation_queue_drop/ { failures += 1 }
    END { print failures + 0 }
  '
)"
printf 'post_recovery_writer_errors=%s\n' "$RECOVERY_ERRORS"
test "$RECOVERY_ERRORS" -eq 0

SHADOW_JSON="$(redis-cli -h 127.0.0.1 -p 6379 --raw \
  GET btc:live:chainlink_shadow)"
test -n "$SHADOW_JSON"
python3 - "$SHADOW_JSON" "$RECOVERY_VERIFY_AFTER_MS" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
assert payload['schema_version'] == 1
assert payload['mode'] == 'shadow'
assert payload['generated_ms'] >= int(sys.argv[2])
print({
    'generated_ms': payload['generated_ms'],
    'model_version': payload['model_version'],
    'valid': payload['valid'],
})
PY

SHADOW_PTTL="$(redis-cli -h 127.0.0.1 -p 6379 \
  PTTL btc:live:chainlink_shadow)"
printf 'shadow_pttl_ms=%s\n' "$SHADOW_PTTL"
test "$SHADOW_PTTL" -gt 0
test "$SHADOW_PTTL" -le 2000

sudo systemctl status price-collector-shadow-signal --no-pager
sudo journalctl \
  -u price-collector-shadow-signal \
  _SYSTEMD_INVOCATION_ID="$RECOVERY_INVOCATION_ID" \
  -n 250 \
  --no-pager
```

Three model rows, the logged 5000-row queue and 500-row batch bounds,
`post_recovery_writer_errors=0`, a positive Redis TTL no greater than 2000 ms,
and a fresh Redis `generated_ms` are required. After this recovery passes,
continue with steps 9 through 11 for the full Phase 5 acceptance checks.
