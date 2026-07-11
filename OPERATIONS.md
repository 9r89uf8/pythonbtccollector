# Operations

Use this after the droplet is already deployed and working.

## Check Services

On the droplet:

```bash
systemctl status price-collector --no-pager
systemctl status price-collector-polymarket-chainlink --no-pager
systemctl status price-collector-binance-futures --no-pager
systemctl status price-collector-polymarket-probabilities --no-pager
systemctl status redis-server --no-pager
systemctl status price-api --no-pager
```

Follow logs:

```bash
journalctl -u price-collector -f
journalctl -u price-collector-polymarket-chainlink -f
journalctl -u price-collector-binance-futures -f
journalctl -u price-collector-polymarket-probabilities -f
journalctl -u price-api -f
```

Check the local API from inside the droplet:

```bash
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
curl http://127.0.0.1:9000/markets/latest
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```

Inspect the official resolution objects returned by both historical response
paths. `GET /markets` defaults to completed markets, so its first ID is suitable
for this check:

```bash
MARKET_ID="$(curl -fsS 'http://127.0.0.1:9000/markets?limit=1' | python3 -c 'import json, sys; print(json.load(sys.stdin)["markets"][0]["market_id"])')"
curl -fsS "http://127.0.0.1:9000/markets/${MARKET_ID}/data" | python3 -c 'import json, sys; payload = json.load(sys.stdin); print(json.dumps({"schema_version": payload["schema_version"], "market": payload["market"]}, indent=2))'
curl -fsS "http://127.0.0.1:9000/markets/${MARKET_ID}/download" | python3 -c 'import json, sys; payload = json.load(sys.stdin); print(json.dumps({"schema_version": payload["schema_version"], "market": payload["market"]}, indent=2))'
```

Both responses should use schema version `2` and include
`market.chainlink_resolution` and `market.resolution`. A recently completed
market can legitimately remain `pending` until Polymarket publishes official
resolution data. The data response keeps `market_start_ms`, `market_end_ms`,
and `series[].timestamp_ms`. The download omits those three fields, retains the
corresponding UTC `*_at` strings, and formats official Chainlink open/close
values to two decimal places.

## Connect From Your Local Machine

The API is intentionally bound only to `127.0.0.1` on the droplet. Use an SSH tunnel from your local machine:

Copy the example env file and put your droplet IP in the local ignored file:

```bash
cp droplet.env.example droplet.env
nano droplet.env
```

Load it:

```bash
set -a
. ./droplet.env
set +a
```

Open the tunnel:

```bash
ssh -N -L "${LOCAL_API_PORT}:127.0.0.1:9000" "${DROPLET_USER}@${DROPLET_IP}"
```

Then, from your local machine:

```bash
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/latest"
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/current/sources"
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/current/live"
```

Keep the SSH tunnel terminal open while using the API locally.

## Deploy Code Updates

If this is the first deploy after adding the live Redis cache, install and bind Redis locally before restarting the app services:

```bash
sudo apt update
sudo apt install -y redis-server
sudo sed -i 's/^bind .*/bind 127.0.0.1/' /etc/redis/redis.conf
sudo sed -i 's/^protected-mode .*/protected-mode yes/' /etc/redis/redis.conf
sudo systemctl enable --now redis-server
sudo systemctl restart redis-server
```

After pushing code-only changes to GitHub, update the droplet:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
```

If the update adds database columns, seed rows, indexes, or new systemd unit files, use this fuller sequence instead. Apply the schema before restarting services so new code does not start before PostgreSQL has the expected tables, columns, and seed data:

```bash
cd /opt/price-collector

sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt

sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql

sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo cp /opt/price-collector/deployment/price-collector-binance-futures.service /etc/systemd/system/price-collector-binance-futures.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-probabilities.service /etc/systemd/system/price-collector-polymarket-probabilities.service
sudo cp /opt/price-collector/deployment/price-api.service /etc/systemd/system/price-api.service
sudo systemctl daemon-reload
sudo systemctl enable redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api

sudo systemctl restart price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
```

When deploying the Polymarket resolution reconciler for the first time, review
`/etc/price-collector/collector.env` manually and add these settings if they are
not already present. Do not replace the production file with the example:

```text
POLYMARKET_RESOLUTION_POLL_SECONDS=5
POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300
POLYMARKET_RESOLUTION_BATCH_SIZE=20
POLYMARKET_RESOLUTION_WS_GRACE_SECONDS=30
```

Apply `schema.sql`, confirm the new table grants for `price_writer` and
`price_reader`, and review the environment file before restarting the four
collector services and `price-api`.

Verify after restart:

```bash
systemctl status price-collector --no-pager
systemctl status price-collector-polymarket-chainlink --no-pager
systemctl status price-collector-binance-futures --no-pager
systemctl status price-collector-polymarket-probabilities --no-pager
systemctl status redis-server --no-pager
systemctl status price-api --no-pager
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/markets/latest
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```

For an update that changes the market data/download contract, repeat the
completed-market API check from **Check Services** above.

## Phase 1 High-Resolution Capture Foundation

Phase 1 installs only the private schema, configuration, and inactive capture
primitives. It does not connect either collector to high-resolution capture and
does not change the live API. Apply `schema.sql` before restarting services, as
shown above.

Review `/etc/price-collector/collector.env` manually and add these keys if they
are absent. Do not replace the production file with the repository example,
and keep both capture flags `false` throughout Phase 1:

```text
RAW_FUTURES_TRACE_ENABLED=false
RAW_CHAINLINK_EVENTS_ENABLED=false
RAW_FUTURES_BUCKET_MS=100
RAW_CAPTURE_QUEUE_MAX_EVENTS=5000
RAW_CAPTURE_BATCH_MAX_ROWS=500
RAW_CAPTURE_FLUSH_MS=1000
RAW_CAPTURE_RETENTION_HOURS=72
RAW_CAPTURE_MAX_RELATION_MB=2048
RAW_CAPTURE_RETENTION_CHECK_SECONDS=60
```

The bucket setting is fixed at `100` for the `_100ms` table schema. Review the
2 GB relation budget against the droplet's real capacity before a later capture
phase enables either source.

Confirm the deployed flags without printing the database URL or other secrets:

```bash
sudo grep -E '^RAW_(FUTURES_TRACE_ENABLED|CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env
```

Both values must be `false`.

Inspect the isolated schema, ownership, and privileges:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    n.nspname AS schema_name,
    pg_get_userbyid(n.nspowner) AS schema_owner,
    has_schema_privilege('price_writer', n.oid, 'USAGE') AS writer_usage,
    has_schema_privilege('price_writer', n.oid, 'CREATE') AS writer_create,
    has_schema_privilege('price_reader', n.oid, 'USAGE') AS reader_usage
FROM pg_namespace n
WHERE n.nspname = 'raw_capture';
"

sudo -u postgres psql -d price_collector -c "
SELECT
    c.relname,
    c.relkind,
    pg_get_userbyid(c.relowner) AS owner
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'raw_capture'
  AND c.relkind IN ('p', 'r')
ORDER BY c.relname;
"
```

Expected security state: `postgres` owns the schema, `price_writer` has schema
`USAGE` and `CREATE`, and `price_reader` has no schema `USAGE`. The two event
parents and `feed_sessions` are owned by `price_writer`; pre-created empty
partitions are expected.

With both flags false, all three raw parents must remain empty:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    (SELECT count(*) FROM raw_capture.binance_futures_price_trace_100ms) AS futures_rows,
    (SELECT count(*) FROM raw_capture.chainlink_price_events) AS chainlink_rows,
    (SELECT count(*) FROM raw_capture.feed_sessions) AS session_rows;
"
```

All counts should be zero. A nonzero count means Phase 1 is not inactive as
intended and should be investigated before proceeding.

The configured relation budget is not a hard disk quota. It excludes WAL,
temporary files, and non-capture relations. Check both the capture relations
and the actual filesystem:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0)) AS raw_capture_relation_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'raw_capture'
  AND c.relkind = 'r';
"
sudo -u postgres psql -d price_collector -c "SELECT pg_size_pretty(pg_database_size('price_collector'));"
df -h /var/lib/postgresql
```

### Phase 1 rollback

Keep both feature flags `false`. Prefer reverting the Phase 1 application
commit in GitHub and deploying that revert with the normal fast-forward update
workflow. The unused `raw_capture` schema is inert and may safely remain for a
subsequent corrected deployment.

If Phase 1 must be removed completely, first confirm all three counts above are
zero and that no later capture phase has ever been enabled. Then the PostgreSQL
owner can remove only the isolated schema:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -c "DROP SCHEMA raw_capture CASCADE;"
```

Do not use this schema-removal rollback after capture has been enabled; it
permanently deletes all retained high-resolution evidence.

## Phase 2 Futures-Only Accelerated Capture Canary

Phase 2 wires the Binance futures `aggTrade` reader to the bounded raw-capture
runtime. The repository and environment-example default remains disabled, so
deploying the code does not start capture by itself. The code checkpoint is
ready when its tests pass and it is safely deployed with the flag still
`false`; Phase 2 is operationally complete only after the explicitly
risk-accepted three-hour accelerated production canary has run continuously
and every acceptance check below passes. Three hours is enough for functional,
queue, batch, session, and short-term resource validation, but it provides less
confidence than the original 24-hour window. Continue background monitoring
toward at least 24 uninterrupted hours after advancing to the next phase.
A three-hour window may not cross a six-hour raw partition boundary; Phase 4's
deliberate partition and retention checks therefore remain mandatory.

This is shadow evidence capture only. Keep Binance REST
`/fapi/v2/ticker/price` as the Redis/API `futures.last` source, keep the existing
one-second flow and book paths active, and do not enable Chainlink capture.

### Deploy the code checkpoint disabled

Run this after the Phase 2 change has been pushed to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt

sudoedit /etc/price-collector/collector.env
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env

sudo systemctl restart price-collector-binance-futures
sudo systemctl status price-collector-binance-futures --no-pager
sudo journalctl -u price-collector-binance-futures -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
```

At this checkpoint the expected values are:

```text
BINANCE_FUTURES_STREAMS_ENABLED=true
RAW_FUTURES_TRACE_ENABLED=false
RAW_CHAINLINK_EVENTS_ENABLED=false
```

Verify that the Phase 1 row counts remain zero and that there is no dedicated
raw-capture connection before enabling the canary:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    (SELECT count(*) FROM raw_capture.binance_futures_price_trace_100ms) AS futures_rows,
    (SELECT count(*) FROM raw_capture.chainlink_price_events) AS chainlink_rows,
    (SELECT count(*) FROM raw_capture.feed_sessions) AS session_rows;
"

sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS raw_capture_connections
FROM pg_stat_activity
WHERE application_name = 'price_collector_raw_capture';
"
```

The connection count must be zero while both capture flags are false.

### Record the pre-canary baseline

Check relation size, total database size, and actual filesystem space. The
configured relation budget does not include PostgreSQL WAL, temporary files,
or other relations:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0)) AS raw_capture_relation_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'raw_capture'
  AND c.relkind = 'r';
"
sudo -u postgres psql -d price_collector -c "SELECT pg_size_pretty(pg_database_size('price_collector'));"
df -h /var/lib/postgresql
```

Record the preceding three-hour one-second flow and book coverage before the
restart. These figures are the like-for-like comparison baseline for the final
accelerated-canary query. Also retain the surrounding 24-hour operational
context when investigating unusual gaps:

```bash
sudo -u postgres psql -d price_collector -c "
WITH datasets(dataset) AS (
    VALUES ('flow'), ('book')
), observations AS (
    SELECT 'flow' AS dataset, sample_second_ms
    FROM binance_flow_1s
    WHERE venue = 'binance_usdm_perp'
      AND symbol = 'BTCUSDT'
      AND sample_second_ms >= (extract(epoch FROM now() - interval '3 hours') * 1000)::bigint
    UNION ALL
    SELECT 'book', sample_second_ms
    FROM binance_book_1s
    WHERE venue = 'binance_usdm_perp'
      AND symbol = 'BTCUSDT'
      AND sample_second_ms >= (extract(epoch FROM now() - interval '3 hours') * 1000)::bigint
), ordered AS (
    SELECT
        dataset,
        sample_second_ms,
        lag(sample_second_ms) OVER (
            PARTITION BY dataset ORDER BY sample_second_ms
        ) AS previous_ms
    FROM observations
)
SELECT
    datasets.dataset,
    count(ordered.sample_second_ms) AS rows,
    round(
        count(ordered.sample_second_ms)::numeric / 10800 * 100,
        3
    ) AS second_coverage_percent,
    COALESCE(max((sample_second_ms - previous_ms) / 1000), 0) AS maximum_gap_seconds,
    count(ordered.sample_second_ms) FILTER (
        WHERE previous_ms IS NOT NULL
          AND sample_second_ms - previous_ms > 1000
    ) AS gap_count
FROM datasets
LEFT JOIN ordered ON ordered.dataset = datasets.dataset
GROUP BY datasets.dataset
ORDER BY datasets.dataset;
"
```

### Enable the futures-only canary

Edit the existing production file manually. Do not copy the repository example
over it and do not change any database credentials:

```bash
sudoedit /etc/price-collector/collector.env
```

Set exactly this feature state:

```text
BINANCE_FUTURES_STREAMS_ENABLED=true
RAW_FUTURES_TRACE_ENABLED=true
RAW_CHAINLINK_EVENTS_ENABLED=false
```

Confirm it without printing secrets, then restart only the affected collector.
Record a journal lower bound immediately before the restart and persist the
post-restart half-open three-hour evidence window in the service state
directory:

```bash
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env

PHASE2_CANARY_FILE=/var/lib/price-collector/phase2-canary-window.env
phase2_epoch_ms_to_utc() {
  local epoch_ms="$1"
  local milliseconds
  printf -v milliseconds '%03d' "$((epoch_ms % 1000))"
  date -u --date="@$((epoch_ms / 1000)).${milliseconds}" '+%Y-%m-%dT%H:%M:%S.%3NZ'
}

PHASE2_JOURNAL_START_MS="$(date -u +%s%3N)"
PHASE2_JOURNAL_START_NS="$((PHASE2_JOURNAL_START_MS * 1000000))"
PHASE2_JOURNAL_START_UTC="$(phase2_epoch_ms_to_utc "$PHASE2_JOURNAL_START_MS")"

sudo systemctl restart price-collector-binance-futures
sudo systemctl is-active --quiet price-collector-binance-futures

PHASE2_OBSERVED_START_MS="$(date -u +%s%3N)"
PHASE2_START_MS="$((((PHASE2_OBSERVED_START_MS + 999) / 1000) * 1000))"
PHASE2_END_MS="$((PHASE2_START_MS + 10800000))"
PHASE2_TELEMETRY_END_MS="$((PHASE2_END_MS + 120000))"
PHASE2_START_NS="$((PHASE2_START_MS * 1000000))"
PHASE2_END_NS="$((PHASE2_END_MS * 1000000))"
PHASE2_START_UTC="$(phase2_epoch_ms_to_utc "$PHASE2_START_MS")"
PHASE2_END_UTC="$(phase2_epoch_ms_to_utc "$PHASE2_END_MS")"
PHASE2_TELEMETRY_END_UTC="$(phase2_epoch_ms_to_utc "$PHASE2_TELEMETRY_END_MS")"

{
  printf 'PHASE2_JOURNAL_START_MS=%s\n' "$PHASE2_JOURNAL_START_MS"
  printf 'PHASE2_JOURNAL_START_NS=%s\n' "$PHASE2_JOURNAL_START_NS"
  printf 'PHASE2_JOURNAL_START_UTC=%s\n' "$PHASE2_JOURNAL_START_UTC"
  printf 'PHASE2_START_MS=%s\n' "$PHASE2_START_MS"
  printf 'PHASE2_END_MS=%s\n' "$PHASE2_END_MS"
  printf 'PHASE2_TELEMETRY_END_MS=%s\n' "$PHASE2_TELEMETRY_END_MS"
  printf 'PHASE2_START_NS=%s\n' "$PHASE2_START_NS"
  printf 'PHASE2_END_NS=%s\n' "$PHASE2_END_NS"
  printf 'PHASE2_START_UTC=%s\n' "$PHASE2_START_UTC"
  printf 'PHASE2_END_UTC=%s\n' "$PHASE2_END_UTC"
  printf 'PHASE2_TELEMETRY_END_UTC=%s\n' "$PHASE2_TELEMETRY_END_UTC"
} | sudo -u pricecollector tee "$PHASE2_CANARY_FILE" >/dev/null
sudo -u pricecollector chmod 600 "$PHASE2_CANARY_FILE"
sudo -u pricecollector cat "$PHASE2_CANARY_FILE"

sudo systemctl status price-collector-binance-futures --no-pager
sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE2_JOURNAL_START_UTC" -n 100 --no-pager
```

`PHASE2_START_MS` is the authoritative evidence-window start. The service
activation timestamp is the process-identity baseline and should be no later
than that start. Record the `systemctl show` output and retain
`phase2-canary-window.env`. An unexpected process restart resets the observation
window and the cumulative in-process counters; a normal Binance WebSocket
reconnect does not. Do not overwrite the window file during a valid run.

If the futures-only canary was already running when the duration policy changed
to three hours, do not restart it. Reconstruct the fixed window from the current
process activation timestamp after confirming that this timestamp and
`NRestarts` still match the recorded baseline:

```bash
sudo systemctl is-active --quiet price-collector-binance-futures
sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID

PHASE2_CANARY_FILE=/var/lib/price-collector/phase2-canary-window.env
PHASE2_ACTIVE_UTC="$(systemctl show price-collector-binance-futures -p ActiveEnterTimestamp --value)"
PHASE2_ACTIVE_MS="$(date -u --date="$PHASE2_ACTIVE_UTC" +%s%3N)"
PHASE2_START_MS="$((((PHASE2_ACTIVE_MS + 999) / 1000) * 1000))"
PHASE2_END_MS="$((PHASE2_START_MS + 10800000))"
PHASE2_TELEMETRY_END_MS="$((PHASE2_END_MS + 120000))"
PHASE2_JOURNAL_START_MS="$((PHASE2_START_MS - 60000))"
PHASE2_START_NS="$((PHASE2_START_MS * 1000000))"
PHASE2_END_NS="$((PHASE2_END_MS * 1000000))"
PHASE2_JOURNAL_START_NS="$((PHASE2_JOURNAL_START_MS * 1000000))"
PHASE2_START_UTC="$(date -u --date="@$((PHASE2_START_MS / 1000))" '+%Y-%m-%dT%H:%M:%SZ')"
PHASE2_END_UTC="$(date -u --date="@$((PHASE2_END_MS / 1000))" '+%Y-%m-%dT%H:%M:%SZ')"
PHASE2_TELEMETRY_END_UTC="$(date -u --date="@$((PHASE2_TELEMETRY_END_MS / 1000))" '+%Y-%m-%dT%H:%M:%SZ')"
PHASE2_JOURNAL_START_UTC="$(date -u --date="@$((PHASE2_JOURNAL_START_MS / 1000))" '+%Y-%m-%dT%H:%M:%SZ')"

{
  printf 'PHASE2_JOURNAL_START_MS=%s\n' "$PHASE2_JOURNAL_START_MS"
  printf 'PHASE2_JOURNAL_START_NS=%s\n' "$PHASE2_JOURNAL_START_NS"
  printf 'PHASE2_JOURNAL_START_UTC=%s\n' "$PHASE2_JOURNAL_START_UTC"
  printf 'PHASE2_START_MS=%s\n' "$PHASE2_START_MS"
  printf 'PHASE2_END_MS=%s\n' "$PHASE2_END_MS"
  printf 'PHASE2_TELEMETRY_END_MS=%s\n' "$PHASE2_TELEMETRY_END_MS"
  printf 'PHASE2_START_NS=%s\n' "$PHASE2_START_NS"
  printf 'PHASE2_END_NS=%s\n' "$PHASE2_END_NS"
  printf 'PHASE2_START_UTC=%s\n' "$PHASE2_START_UTC"
  printf 'PHASE2_END_UTC=%s\n' "$PHASE2_END_UTC"
  printf 'PHASE2_TELEMETRY_END_UTC=%s\n' "$PHASE2_TELEMETRY_END_UTC"
} | sudo -u pricecollector tee "$PHASE2_CANARY_FILE" >/dev/null
sudo -u pricecollector chmod 600 "$PHASE2_CANARY_FILE"
sudo -u pricecollector cat "$PHASE2_CANARY_FILE"
```

Verify that the existing public live path is still healthy and REST-derived:

```bash
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live | python3 -c '
import json, sys
payload = json.load(sys.stdin)
print(json.dumps(payload["futures"]["last"], indent=2))
'
redis-cli -h 127.0.0.1 GET btc:live:futures
```

### Monitor the three-hour accelerated run

The futures collector emits one structured `raw_capture_summary` every 60
seconds. Do not log individual raw events. The recorded journal bound extends
two minutes beyond the exact three-hour SQL
evidence window so final acceptance includes at least the first cumulative
summary emitted after the endpoint. This grace is conservative; SQL remains
strictly bounded to `[PHASE2_START_MS, PHASE2_END_MS)`.

Inspect the most recent summaries:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE2_JOURNAL_START_UTC" --until "$PHASE2_TELEMETRY_END_UTC" -o cat \
  | grep '"event": "raw_capture_summary"' \
  | tail -n 20
```

Summarize the observed writer latency, queue pressure, and final counters using
only the structured summary records:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE2_JOURNAL_START_UTC" --until "$PHASE2_TELEMETRY_END_UTC" -o cat \
  | grep '"event": "raw_capture_summary"' \
  | python3 -c '
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
durations = [float(row["last_batch_duration_ms"]) for row in rows]
def maximum(field):
    return max(
        (float(row[field]) for row in rows if row.get(field) is not None),
        default=None,
    )
final = rows[-1] if rows else {}
print({
    "summaries": len(rows),
    "sampled_last_batch_duration_ms_average": (
        round(sum(durations) / len(durations), 3) if durations else None
    ),
    "sampled_last_batch_duration_ms_maximum": (
        max(durations) if durations else None
    ),
    "maximum_reported_queue_depth": max(
        (int(row["queue_depth"]) for row in rows), default=None
    ),
    "queue_high_water": max(
        (int(row["queue_high_water"]) for row in rows), default=None
    ),
    "final_records_dropped_total": (
        int(rows[-1]["records_dropped_total"]) if rows else None
    ),
    "final_batches_failed_total": (
        int(rows[-1]["batches_failed_total"]) if rows else None
    ),
    "final_maintenance_runs_total": (
        int(rows[-1]["maintenance_runs_total"]) if rows else None
    ),
    "final_maintenance_failures_total": (
        int(rows[-1]["maintenance_failures_total"]) if rows else None
    ),
    "final_capture_suspended": (
        bool(rows[-1]["capture_suspended"]) if rows else None
    ),
    "final_messages_received_total": (
        int(rows[-1]["messages_received_total"]) if rows else None
    ),
    "final_messages_accepted_total": (
        int(rows[-1]["messages_accepted_total"]) if rows else None
    ),
    "final_parse_errors_total": (
        int(rows[-1]["parse_errors_total"]) if rows else None
    ),
    "maximum_ws_source_age_ms": maximum("shadow_ws_source_age_ms"),
    "maximum_ws_received_age_ms": maximum("shadow_ws_received_age_ms"),
    "maximum_rest_source_age_ms": maximum("shadow_rest_source_age_ms"),
    "maximum_abs_ws_minus_rest_bps": maximum(
        "shadow_max_abs_ws_minus_rest_bps"
    ),
    "final_ws_gap_events_total": final.get("shadow_ws_id_gap_events_total"),
    "final_ws_missing_agg_trades_total": final.get(
        "shadow_ws_missing_agg_trades_total"
    ),
    "final_ws_duplicate_ids_total": final.get("shadow_ws_duplicate_ids_total"),
    "final_ws_regressions_total": final.get("shadow_ws_regressions_total"),
    "final_ws_reconnects_total": final.get("shadow_reconnects_total"),
    "maximum_ws_interarrival_ms": (
        round(maximum("shadow_ws_max_interarrival_ns") / 1000000, 3)
        if maximum("shadow_ws_max_interarrival_ns") is not None else None
    ),
})
'
```

Look for capture, live-path, or telemetry failures, suspension, queue loss, or
out-of-sequence drops. No output is expected from this command; exact message
matching avoids falsely matching zero-valued fields inside healthy summaries:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE2_JOURNAL_START_UTC" --until "$PHASE2_TELEMETRY_END_UTC" -o cat \
  | grep -E '"level": "(ERROR|CRITICAL)"|"message": "(live_cache_write_failed|raw_capture_queue_oldest_dropped|raw_capture_suspended_by_storage_budget|raw_capture_shutdown_task_still_running)"'
```

At the start, during the run, and after three hours, inspect service resource use.
The final `NRestarts` and `ActiveEnterTimestamp` must both match the values
recorded immediately after canary enablement:

```bash
sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
FUTURES_PID="$(systemctl show price-collector-binance-futures -p MainPID --value)"
ps -p "${FUTURES_PID}" -o pid,etime,%cpu,%mem,rss,vsz,cmd
```

The reported batch-duration average and maximum sample the latest batch only
once per 60-second summary; they are not the true average or maximum across
every batch. Treat them as diagnostics rather than stand-alone pass criteria.
Fail the canary if sampled latency is accompanied by a sustained queue, any
dropped record, or any failed batch. The normal state is a queue that repeatedly
returns near zero, `queue_high_water` below
`RAW_CAPTURE_QUEUE_MAX_EVENTS`, and a dedicated writer that does not degrade
the existing collector. Review REST-versus-WebSocket basis points, source and
receive ages, maximum interarrival time, reconnects, aggregate-trade ID gaps,
duplicates, and regressions as shadow diagnostics. Any nonzero gap or reconnect
count needs correlation with the session and reconnect logs; it does not
authorize switching the public live source to the WebSocket.

### Validate captured futures evidence

Chainlink must remain completely inactive, while the dedicated pool must never
use more than one database connection:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    (SELECT count(*) FROM raw_capture.chainlink_price_events) AS chainlink_rows,
    (SELECT count(*) FROM raw_capture.feed_sessions
     WHERE source = 'polymarket_chainlink_rtds') AS chainlink_sessions;
"

sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS raw_capture_connections
FROM pg_stat_activity
WHERE application_name = 'price_collector_raw_capture';
"
```

Expected: both Chainlink counts are zero and the raw connection count is zero
or one, never more than one.

Confirm that no connection has more than one trace row for the same active 100
ms bucket, and that no connection has more than ten buckets in one second:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS duplicate_bucket_groups
FROM (
    SELECT connection_id, bucket_start_ms
    FROM raw_capture.binance_futures_price_trace_100ms
    GROUP BY connection_id, bucket_start_ms
    HAVING count(*) > 1
) duplicates;

WITH per_second AS (
    SELECT
        connection_id,
        (bucket_start_ms / 1000) * 1000 AS second_ms,
        count(*) AS bucket_rows
    FROM raw_capture.binance_futures_price_trace_100ms
    WHERE bucket_start_ms >= ${PHASE2_START_MS}
      AND bucket_start_ms < ${PHASE2_END_MS}
    GROUP BY connection_id, (bucket_start_ms / 1000) * 1000
)
SELECT
    COALESCE(max(bucket_rows), 0) AS maximum_buckets_per_connection_second,
    count(*) FILTER (WHERE bucket_rows > 10) AS seconds_above_cap
FROM per_second;
"
```

Expected: `duplicate_bucket_groups=0`, the maximum is at most `10`, and
`seconds_above_cap=0`. Different connection IDs may legitimately produce more
than ten global rows during a reconnect second.

Audit the futures session lifecycle and ensure every recent trace connection
has session metadata:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH recent_sessions AS (
    SELECT *
    FROM raw_capture.feed_sessions
    WHERE source = 'binance_futures_agg_trade'
      AND connected_wall_ns >= ${PHASE2_JOURNAL_START_NS}
      AND connected_wall_ns < ${PHASE2_END_NS}
)
SELECT
    count(*) AS session_count,
    count(*) FILTER (
        WHERE ready_wall_ns IS NULL OR ready_monotonic_ns IS NULL
    ) AS sessions_never_ready,
    count(*) FILTER (WHERE disconnected_wall_ns IS NULL) AS sessions_still_open,
    count(*) FILTER (
        WHERE disconnected_wall_ns IS NOT NULL AND close_reason IS NULL
    ) AS closed_without_reason,
    COALESCE(sum(messages_received_total), 0) AS recorded_messages_received,
    COALESCE(sum(messages_accepted_total), 0) AS recorded_messages_accepted,
    COALESCE(sum(parse_errors_total), 0) AS recorded_parse_errors,
    COALESCE(sum(records_dropped_total), 0) AS session_attributed_drops
FROM recent_sessions;

WITH recent_trace_connections AS (
    SELECT DISTINCT connection_id
    FROM raw_capture.binance_futures_price_trace_100ms
    WHERE bucket_start_ms >= ${PHASE2_START_MS}
      AND bucket_start_ms < ${PHASE2_END_MS}
)
SELECT count(*) FILTER (WHERE sessions.connection_id IS NULL)
       AS trace_connections_without_session
FROM recent_trace_connections traces
LEFT JOIN raw_capture.feed_sessions sessions
  ON sessions.connection_id = traces.connection_id
 AND sessions.source = 'binance_futures_agg_trade';
"
```

Expected: at least one session, zero never-ready sessions, zero closed sessions
without a reason, zero trace connections without a session, and zero
session-attributed drops. At most one session may still be open because the
collector remains running during this audit. Session counters for that current
connection remain at their opening values until its final upsert, so the
runtime-wide `raw_capture_summary` counters are authoritative for canary loss.

Measure rows per connection and UTC hour. A complete hour for one connection
cannot exceed 36,000 rows; inactive 100 ms buckets are intentionally absent:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo -u postgres psql -d price_collector -c "
SELECT
    connection_id,
    to_char(
        to_timestamp(((bucket_start_ms / 3600000) * 3600000) / 1000.0)
            AT TIME ZONE 'UTC',
        'YYYY-MM-DD HH24:00:00'
    ) AS utc_hour,
    count(*) AS trace_rows,
    sum(event_count) AS agg_trade_events,
    round(count(*)::numeric / 3600, 3) AS rows_per_second
FROM raw_capture.binance_futures_price_trace_100ms
WHERE bucket_start_ms >= ${PHASE2_START_MS}
  AND bucket_start_ms < ${PHASE2_END_MS}
GROUP BY connection_id, (bucket_start_ms / 3600000) * 3600000
ORDER BY utc_hour, connection_id;
"
```

Measure leaf-partition bytes and the approximate observed bytes per hour. The
rate includes the small fixed cost of pre-created empty partitions:

```bash
sudo -u postgres psql -d price_collector -c "
WITH leaf_sizes AS (
    SELECT
        child.oid,
        child.relname,
        pg_total_relation_size(child.oid) AS bytes
    FROM pg_inherits inheritance
    JOIN pg_class parent ON parent.oid = inheritance.inhparent
    JOIN pg_class child ON child.oid = inheritance.inhrelid
    WHERE parent.oid =
          'raw_capture.binance_futures_price_trace_100ms'::regclass
)
SELECT relname, bytes, pg_size_pretty(bytes) AS size
FROM leaf_sizes
ORDER BY relname;

WITH capture AS (
    SELECT
        count(*) AS rows,
        min(bucket_start_ms) AS first_ms,
        max(bucket_start_ms) AS last_ms
    FROM raw_capture.binance_futures_price_trace_100ms
), size AS (
    SELECT COALESCE(sum(pg_total_relation_size(inhrelid)), 0) AS bytes
    FROM pg_inherits
    WHERE inhparent =
          'raw_capture.binance_futures_price_trace_100ms'::regclass
)
SELECT
    capture.rows,
    round((capture.last_ms - capture.first_ms + 100)::numeric / 3600000, 3)
        AS observed_hours,
    size.bytes,
    pg_size_pretty(size.bytes) AS total_size,
    CASE
        WHEN capture.rows = 0 THEN NULL
        ELSE round(
            size.bytes::numeric /
            GREATEST(
                (capture.last_ms - capture.first_ms + 100)::numeric / 3600000,
                0.001
            )
        )
    END AS approximate_bytes_per_hour
FROM capture CROSS JOIN size;
"
```

Repeat the total database and filesystem checks; relation retention is not a
filesystem quota:

```bash
sudo -u postgres psql -d price_collector -c "SELECT pg_size_pretty(pg_database_size('price_collector'));"
df -h /var/lib/postgresql
```

### Compare one-second flow and book completeness

After the full three hours, compare the immediately preceding three-hour
baseline with the accelerated-canary window:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
NOW_MS="$(date -u +%s%3N)"
if [ "$NOW_MS" -lt "$PHASE2_TELEMETRY_END_MS" ]; then
  printf 'Phase 2 final telemetry is not complete: now=%s required_end=%s\n' \
    "$NOW_MS" "$PHASE2_TELEMETRY_END_MS" >&2
  exit 1
fi
printf 'fixed three-hour window complete: [%s, %s)\n' \
  "$PHASE2_START_UTC" "$PHASE2_END_UTC"
```

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase2-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH periods(period, start_ms, end_ms) AS (
    VALUES
        ('baseline', (${PHASE2_START_MS} - 10800000)::bigint,
         ${PHASE2_START_MS}::bigint),
        ('canary', ${PHASE2_START_MS}::bigint, ${PHASE2_END_MS}::bigint)
), datasets(dataset) AS (
    VALUES ('flow'), ('book')
), observations AS (
    SELECT 'flow' AS dataset, sample_second_ms
    FROM binance_flow_1s
    WHERE venue = 'binance_usdm_perp' AND symbol = 'BTCUSDT'
    UNION ALL
    SELECT 'book', sample_second_ms
    FROM binance_book_1s
    WHERE venue = 'binance_usdm_perp' AND symbol = 'BTCUSDT'
), tagged AS (
    SELECT
        periods.period,
        observations.dataset,
        observations.sample_second_ms,
        lag(observations.sample_second_ms) OVER (
            PARTITION BY periods.period, observations.dataset
            ORDER BY observations.sample_second_ms
        ) AS previous_ms
    FROM periods
    JOIN observations
      ON observations.sample_second_ms >= periods.start_ms
     AND observations.sample_second_ms < periods.end_ms
)
SELECT
    periods.period,
    datasets.dataset,
    count(tagged.sample_second_ms) AS rows,
    round(
        count(tagged.sample_second_ms)::numeric / 10800 * 100,
        3
    ) AS second_coverage_percent,
    COALESCE(max((tagged.sample_second_ms - tagged.previous_ms) / 1000), 0)
        AS maximum_gap_seconds,
    count(tagged.sample_second_ms) FILTER (
        WHERE tagged.previous_ms IS NOT NULL
          AND tagged.sample_second_ms - tagged.previous_ms > 1000
    ) AS gap_count
FROM periods
CROSS JOIN datasets
LEFT JOIN tagged
  ON tagged.period = periods.period
 AND tagged.dataset = datasets.dataset
GROUP BY periods.period, datasets.dataset
ORDER BY datasets.dataset, periods.period;
"
```

Investigate any material coverage reduction, new long gap, flow/book flush
failure, or loss of the REST live futures value. Capture must not be accepted
if it degrades the existing one-second paths.

### Phase 2 acceptance

Phase 2 is operationally complete under the accelerated rollout only when all
of these are true after a continuous three-hour run:

- The futures collector process did not unexpectedly restart; its final
  `ActiveEnterTimestamp` and `NRestarts` match the recorded starting values.
- `records_persisted_total` increased and futures trace rows span the canary.
- `records_dropped_total=0`, `batches_failed_total=0`, and
  `capture_suspended=false` in the final summary.
- Raw maintenance ran at least once and `maintenance_failures_total=0`.
- `parse_errors_total=0`, or every parse error was investigated and the affected
  interval was rejected for training; received and accepted totals were
  reviewed for unexplained loss.
- The queue repeatedly returned near zero and its high-water mark stayed below
  its fixed capacity.
- Duplicate bucket groups and per-connection seconds above ten rows are zero.
- Futures sessions passed the lifecycle audit, no trace connection lacked a
  session row, and no more than the current connection remained open.
- Chainlink raw rows and Chainlink sessions remain zero.
- The dedicated raw pool used no more than one connection.
- Measured CPU, memory, batch latency, rows/hour, bytes/hour, database size, and
  filesystem free space are acceptable for the droplet.
- WebSocket source/receive ages, REST-versus-WebSocket differences,
  interarrival time, reconnects, and aggregate-trade ID continuity were
  reviewed; there are no unexplained gaps, duplicates, or regressions.
- Existing flow and book completeness show no unexplained regression.
- `btc:live:futures` and `/markets/current/live` remain healthy and REST-based.

Passing these checks authorizes the accelerated move to Phase 3; it does not
make three hours equivalent to a full-day soak. Keep the futures collector
running and continue reviewing the same counters, resource use, database size,
and filesystem space until it has accumulated at least 24 uninterrupted hours.
Any later unexplained regression is still a Phase 2 failure and requires the
same rollback and investigation. Once Chainlink capture starts, this extended
observation is no longer futures-only, so the Chainlink-zero isolation check no
longer applies.

### Phase 2 rollback

Rollback disables only best-effort futures evidence capture. Do not set
`BINANCE_FUTURES_STREAMS_ENABLED=false`, because that would also stop the
existing flow and book collectors. Edit the production environment to restore:

```text
BINANCE_FUTURES_STREAMS_ENABLED=true
RAW_FUTURES_TRACE_ENABLED=false
RAW_CHAINLINK_EVENTS_ENABLED=false
```

Then restart and verify the futures collector:

```bash
sudoedit /etc/price-collector/collector.env
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env
sudo systemctl restart price-collector-binance-futures
sudo systemctl status price-collector-binance-futures --no-pager
sudo journalctl -u price-collector-binance-futures -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live

sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS raw_capture_connections
FROM pg_stat_activity
WHERE application_name = 'price_collector_raw_capture';
"
sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS retained_futures_rows
FROM raw_capture.binance_futures_price_trace_100ms;
"
```

The raw connection count must return to zero. Run the retained-row query again
later and confirm it no longer increases. Keep the populated `raw_capture`
schema in place; do not use the Phase 1 schema-removal rollback after capture
has run. The retained evidence is bounded and may be reviewed or removed later
with an explicit data-disposal decision.

## Phase 3 Chainlink Accelerated Capture Canary

Phase 3 removes Redis and PostgreSQL waits from the Polymarket RTDS receive
loop, adds independent latest-wins live and provider-second historical workers,
and wires every valid Chainlink tick to the private best-effort capture runtime.
The repository and environment-example flag remains `false`; deploying the code
does not enable Chainlink raw capture by itself.

This rollout uses an explicitly risk-accepted three-hour accelerated canary.
It validates short-term correctness, isolation, ordering, queueing, batching,
and resource behavior, but has less confidence than a 24-hour soak for slow
leaks, traffic variation, rare reconnects, and storage trends. Passing it
authorizes Phase 4 while both collectors continue background monitoring toward
at least 24 uninterrupted hours.
A three-hour window may not cross a six-hour raw partition boundary; Phase 4's
deliberate partition and retention checks are the required compensation.

The Phase 3 code checkpoint may be deployed while the futures-only Phase 2
canary is still running. Do **not** enable Chainlink raw capture until Phase 2
has completed its uninterrupted three-hour accelerated window and passed every
acceptance check above. Enabling it earlier ends the futures-only isolation
period.

### Deploy the code checkpoint disabled

Run this after the Phase 3 change has been pushed to GitHub. This phase changes
no schema, dependency declaration, or systemd unit. Restart only the Chainlink
collector; restarting the futures collector would reset its process-level
canary counters and uptime.

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt

sudoedit /etc/price-collector/collector.env
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env

sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
redis-cli -h 127.0.0.1 GET btc:live:chainlink
```

The disabled code-checkpoint state is:

```text
BINANCE_FUTURES_STREAMS_ENABLED=true
RAW_FUTURES_TRACE_ENABLED=true
RAW_CHAINLINK_EVENTS_ENABLED=false
```

If Phase 2 has not yet been enabled, `RAW_FUTURES_TRACE_ENABLED` can still be
`false` at this code checkpoint. It must be `true` and its three-hour
accelerated acceptance must be complete before the Phase 3 production canary
begins.

Even with Chainlink raw capture disabled, the new delivery workers are active.
Confirm that RTDS values continue advancing in Redis and normal PostgreSQL
history while no new Chainlink raw session or event is created:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    count(*) AS retained_chainlink_raw_rows,
    max(received_wall_ns) AS latest_chainlink_raw_received_wall_ns
FROM raw_capture.chainlink_price_events;

SELECT
    count(*) AS retained_chainlink_sessions,
    max(connected_wall_ns) AS latest_chainlink_session_connected_wall_ns
FROM raw_capture.feed_sessions
WHERE source = 'polymarket_chainlink_rtds';

SELECT
    ps.sample_second_at,
    ps.price,
    ps.provider_event_ms,
    ps.received_ms
FROM price_samples ps
JOIN instruments i ON i.instrument_id = ps.instrument_id
JOIN providers p ON p.provider_id = i.provider_id
WHERE p.provider_code = 'polymarket_chainlink_rtds'
  AND i.symbol = 'BTCUSD'
ORDER BY ps.sample_second_ms DESC
LIMIT 5;
"

sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS raw_capture_connections
FROM pg_stat_activity
WHERE application_name = 'price_collector_raw_capture';
"
```

Run the raw row/session query again after several minutes. Its two maxima must
not advance while the flag is `false`, while normal `price_samples` rows and the
Redis value should advance. If futures capture is active, zero or one raw
database connection is expected; Chainlink disabled must not add another one. No
Chainlink `raw_capture_summary` is expected while its raw runtime is disabled.

Reject the code checkpoint if the refactor introduces unexplained delivery
overflow, Redis or historical-worker failures, RTDS receive stalls, or normal
history gaps. Raw enablement cannot repair a delivery-worker defect.

### Record the Phase 3 baseline

After Phase 2 is accepted and immediately before enabling Chainlink capture,
record both service lifecycles. The futures values must remain unchanged through
the Phase 3 enablement because that service is not restarted:

```bash
sudo systemctl show price-collector-polymarket-chainlink \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
```

Record the preceding three-hour normal Chainlink-history coverage. RTDS need
not publish in every UTC second, so compare the canary to this like-for-like
baseline rather than requiring 100 percent coverage. Retain the surrounding
24-hour context when investigating unusual gaps:

```bash
sudo -u postgres psql -d price_collector -c "
WITH observations AS (
    SELECT
        ps.sample_second_ms,
        lag(ps.sample_second_ms) OVER (ORDER BY ps.sample_second_ms) AS previous_ms
    FROM price_samples ps
    JOIN instruments i ON i.instrument_id = ps.instrument_id
    JOIN providers p ON p.provider_id = i.provider_id
    WHERE p.provider_code = 'polymarket_chainlink_rtds'
      AND i.symbol = 'BTCUSD'
      AND ps.sample_second_ms >=
          (extract(epoch FROM now() - interval '3 hours') * 1000)::bigint
)
SELECT
    count(*) AS rows,
    round(count(*)::numeric / 10800 * 100, 3) AS second_coverage_percent,
    COALESCE(max((sample_second_ms - previous_ms) / 1000), 0)
        AS maximum_gap_seconds,
    count(*) FILTER (
        WHERE previous_ms IS NOT NULL
          AND sample_second_ms - previous_ms > 1000
    ) AS gap_count
FROM observations;
"
```

Record capture relation size, total database size, actual filesystem space, and
host clock synchronization. The relation limit excludes WAL, temporary files,
and other relations:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0))
       AS raw_capture_relation_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'raw_capture'
  AND c.relkind = 'r';
"
sudo -u postgres psql -d price_collector -c "SELECT pg_size_pretty(pg_database_size('price_collector'));"
df -h /var/lib/postgresql
timedatectl show -p NTPSynchronized --value
```

The final command should report `yes`. Provider-to-receive differences use the
host wall clock and can be negative when clocks disagree; nanosecond storage
does not imply nanosecond clock accuracy.

### Enable the Chainlink canary

Edit the existing production file manually. Do not copy the repository example
over it and do not change credentials:

```bash
sudoedit /etc/price-collector/collector.env
```

Set exactly this feature state:

```text
BINANCE_FUTURES_STREAMS_ENABLED=true
RAW_FUTURES_TRACE_ENABLED=true
RAW_CHAINLINK_EVENTS_ENABLED=true
```

Confirm only those non-secret values and restart only Chainlink. Capture a
journal lower bound immediately before the restart, then define the canary's
post-restart half-open evidence window as
`[PHASE3_START_MS, PHASE3_END_MS)`. The following writes only non-secret time
bounds to the service-user state directory:

```bash
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env

PHASE3_CANARY_FILE=/var/lib/price-collector/phase3-canary-window.env
phase3_epoch_ms_to_utc() {
  local epoch_ms="$1"
  local milliseconds
  printf -v milliseconds '%03d' "$((epoch_ms % 1000))"
  date -u --date="@$((epoch_ms / 1000)).${milliseconds}" '+%Y-%m-%dT%H:%M:%S.%3NZ'
}

PHASE3_JOURNAL_START_MS="$(date -u +%s%3N)"
PHASE3_JOURNAL_START_NS="$((PHASE3_JOURNAL_START_MS * 1000000))"
PHASE3_JOURNAL_START_UTC="$(phase3_epoch_ms_to_utc "$PHASE3_JOURNAL_START_MS")"

sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl is-active --quiet price-collector-polymarket-chainlink

PHASE3_OBSERVED_START_MS="$(date -u +%s%3N)"
PHASE3_START_MS="$((((PHASE3_OBSERVED_START_MS + 999) / 1000) * 1000))"
PHASE3_END_MS="$((PHASE3_START_MS + 10800000))"
PHASE3_TELEMETRY_END_MS="$((PHASE3_END_MS + 120000))"
PHASE3_START_NS="$((PHASE3_START_MS * 1000000))"
PHASE3_END_NS="$((PHASE3_END_MS * 1000000))"
PHASE3_START_UTC="$(phase3_epoch_ms_to_utc "$PHASE3_START_MS")"
PHASE3_END_UTC="$(phase3_epoch_ms_to_utc "$PHASE3_END_MS")"
PHASE3_TELEMETRY_END_UTC="$(phase3_epoch_ms_to_utc "$PHASE3_TELEMETRY_END_MS")"

{
  printf 'PHASE3_JOURNAL_START_MS=%s\n' "$PHASE3_JOURNAL_START_MS"
  printf 'PHASE3_JOURNAL_START_NS=%s\n' "$PHASE3_JOURNAL_START_NS"
  printf 'PHASE3_JOURNAL_START_UTC=%s\n' "$PHASE3_JOURNAL_START_UTC"
  printf 'PHASE3_START_MS=%s\n' "$PHASE3_START_MS"
  printf 'PHASE3_END_MS=%s\n' "$PHASE3_END_MS"
  printf 'PHASE3_TELEMETRY_END_MS=%s\n' "$PHASE3_TELEMETRY_END_MS"
  printf 'PHASE3_START_NS=%s\n' "$PHASE3_START_NS"
  printf 'PHASE3_END_NS=%s\n' "$PHASE3_END_NS"
  printf 'PHASE3_START_UTC=%s\n' "$PHASE3_START_UTC"
  printf 'PHASE3_END_UTC=%s\n' "$PHASE3_END_UTC"
  printf 'PHASE3_TELEMETRY_END_UTC=%s\n' "$PHASE3_TELEMETRY_END_UTC"
} | sudo -u pricecollector tee "$PHASE3_CANARY_FILE" >/dev/null
sudo -u pricecollector chmod 600 "$PHASE3_CANARY_FILE"
sudo -u pricecollector cat "$PHASE3_CANARY_FILE"

sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo systemctl show price-collector-polymarket-chainlink \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
sudo journalctl -u price-collector-polymarket-chainlink \
  --since "$PHASE3_JOURNAL_START_UTC" -n 100 --no-pager
```

The futures activation timestamp and restart count must match the pre-enable
baseline. An unexpected Chainlink process restart resets its observation window
and cumulative in-process counters; a normal RTDS WebSocket reconnect does not.
Do not overwrite `phase3-canary-window.env` during this run. If the Chainlink
process unexpectedly restarts, reject this window and repeat the activation
sequence to create a fresh one. Final acceptance starts only after the current
UTC epoch is at least `PHASE3_TELEMETRY_END_MS`; retain the file with the canary
evidence.

Verify the unchanged public path:

```bash
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live | python3 -c '
import json, sys
payload = json.load(sys.stdin)
print(json.dumps(payload["prices"]["chainlink"], indent=2))
'
redis-cli -h 127.0.0.1 GET btc:live:chainlink
```

### Monitor Chainlink capture and delivery

The Chainlink collector emits one structured `raw_capture_summary` every 60
seconds while capture is enabled. It includes standard raw writer counters,
signed provider/local timing, connection health, and independent live and
historical delivery state. Do not log individual raw ticks. The journal bound
extends two minutes beyond the exact three-hour SQL evidence window so final
acceptance includes at least the first cumulative summary after the endpoint;
SQL remains strictly bounded to `[PHASE3_START_MS, PHASE3_END_MS)`.

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo journalctl -u price-collector-polymarket-chainlink \
  --since "$PHASE3_JOURNAL_START_UTC" --until "$PHASE3_TELEMETRY_END_UTC" -o cat \
  | grep '"event": "raw_capture_summary"' \
  | grep '"source": "polymarket_chainlink_rtds"' \
  | tail -n 20
```

Summarize the cumulative final state and sampled maxima:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
printf 'fixed canary window: [%s, %s)\n' "$PHASE3_START_UTC" "$PHASE3_END_UTC"
sudo journalctl -u price-collector-polymarket-chainlink \
  --since "$PHASE3_JOURNAL_START_UTC" --until "$PHASE3_TELEMETRY_END_UTC" -o cat \
  | grep '"event": "raw_capture_summary"' \
  | grep '"source": "polymarket_chainlink_rtds"' \
  | python3 -c '
import json, sys
rows = [json.loads(line) for line in sys.stdin if line.strip()]
def maximum(field):
    return max((row[field] for row in rows if row.get(field) is not None), default=None)
def minimum(field):
    return min((row[field] for row in rows if row.get(field) is not None), default=None)
final = rows[-1] if rows else {}
print({
    "summaries": len(rows),
    "final_messages_received_total": final.get("messages_received_total"),
    "final_messages_accepted_total": final.get("messages_accepted_total"),
    "final_parse_errors_total": final.get("parse_errors_total"),
    "final_records_coalesced_total": final.get("records_coalesced_total"),
    "final_records_enqueued_total": final.get("records_enqueued_total"),
    "final_records_persisted_total": final.get("records_persisted_total"),
    "final_records_dropped_total": final.get("records_dropped_total"),
    "final_batches_failed_total": final.get("batches_failed_total"),
    "final_maintenance_runs_total": final.get("maintenance_runs_total"),
    "final_maintenance_failures_total": final.get("maintenance_failures_total"),
    "final_capture_suspended": final.get("capture_suspended"),
    "maximum_queue_depth": maximum("queue_depth"),
    "queue_high_water": maximum("queue_high_water"),
    "maximum_last_batch_duration_ms": maximum("last_batch_duration_ms"),
    "delivery_sequence": final.get("delivery_sequence"),
    "delivery_live_attempted_sequence": final.get("delivery_live_attempted_sequence"),
    "delivery_live_attempts_total": final.get("delivery_live_attempts_total"),
    "delivery_live_successes_total": final.get("delivery_live_successes_total"),
    "delivery_live_failures_total": final.get("delivery_live_failures_total"),
    "delivery_history_collapsed_total": final.get("delivery_history_collapsed_total"),
    "delivery_history_persisted_total": final.get("delivery_history_persisted_total"),
    "delivery_history_failures_total": final.get("delivery_history_failures_total"),
    "delivery_history_pending_dropped_total": final.get("delivery_history_pending_dropped_total"),
    "maximum_delivery_pending_seconds": maximum("delivery_history_pending_seconds"),
    "delivery_pending_high_water": maximum("delivery_history_pending_high_water"),
    "last_live_attempt_ms": final.get("delivery_last_live_attempt_ms"),
    "last_history_write_ms": final.get("delivery_last_history_write_ms"),
    "connections_opened_total": final.get("chainlink_connections_opened_total"),
    "reconnects_total": final.get("chainlink_reconnects_total"),
    "latest_price": final.get("chainlink_latest_price"),
    "latest_provider_event_ms": final.get("chainlink_provider_event_ms"),
    "latest_provider_message_ms": final.get("chainlink_provider_message_ms"),
    "latest_received_ms": final.get("chainlink_received_ms"),
    "latest_connection_id": final.get("chainlink_latest_connection_id"),
    "latest_receive_sequence": final.get("chainlink_latest_receive_sequence"),
    "minimum_provider_event_to_receive_ms": minimum("chainlink_provider_event_to_receive_ms"),
    "maximum_provider_event_to_receive_ms": maximum("chainlink_provider_event_to_receive_ms"),
    "minimum_provider_message_to_receive_ms": minimum("chainlink_provider_message_to_receive_ms"),
    "maximum_provider_message_to_receive_ms": maximum("chainlink_provider_message_to_receive_ms"),
    "minimum_provider_message_minus_event_ms": minimum("chainlink_provider_message_minus_event_ms"),
    "maximum_provider_message_minus_event_ms": maximum("chainlink_provider_message_minus_event_ms"),
    "maximum_provider_event_age_ms": maximum("chainlink_provider_event_age_ms"),
    "maximum_received_age_ms": maximum("chainlink_received_age_ms"),
    "latest_raw_interarrival_ms": (
        final.get("chainlink_raw_interarrival_ns") / 1000000
        if final.get("chainlink_raw_interarrival_ns") is not None else None
    ),
    "maximum_raw_interarrival_ms": (
        maximum("chainlink_raw_max_interarrival_ns") / 1000000
        if maximum("chainlink_raw_max_interarrival_ns") is not None else None
    ),
})
'
```

The expected clean state is:

- `records_coalesced_total=0`, because raw Chainlink ticks are never coalesced.
- `records_dropped_total=0`, `batches_failed_total=0`, and
  `capture_suspended=false`.
- `maintenance_runs_total>0` and `maintenance_failures_total=0`.
- Queue depth repeatedly near zero and high-water below
  `RAW_CAPTURE_QUEUE_MAX_EVENTS`.
- `delivery_live_successes_total` and
  `delivery_history_persisted_total` increasing.
- `delivery_live_failures_total=0`,
  `delivery_history_failures_total=0`, and
  `delivery_history_pending_dropped_total=0`.
- Delivery pending state repeatedly settling near zero and its high-water below
  the fixed 5,000-provider-second bound.
- No unexplained parse error, receive stall, connection churn, or stale live
  attempt/history-write timestamp.

`delivery_history_collapsed_total` may be nonzero: it counts normal replacement
of multiple accepted ticks in the same provider second and is not raw loss.
Signed timing fields are deliberately not clamped. Correlate unusual values with
host clock synchronization, provider cadence, reconnects, and local monotonic
interarrival time before interpreting them as network delay.

No output is expected from this bounded failure scan. It reports every
`ERROR`/`CRITICAL` record plus warnings that directly mean live-cache failure,
historical retry, queue loss, storage-budget suspension, or incomplete raw
shutdown. Normal RTDS reconnect warnings and investigated parse rejections are
reviewed through their dedicated counters instead of being automatic failures:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo journalctl -u price-collector-polymarket-chainlink \
  --since "$PHASE3_JOURNAL_START_UTC" --until "$PHASE3_TELEMETRY_END_UTC" -o cat \
  | grep -E '"level": "(ERROR|CRITICAL)"|"message": "(live_cache_write_failed|polymarket_chainlink_history_write_retry_scheduled|raw_capture_queue_oldest_dropped|raw_capture_suspended_by_storage_budget|raw_capture_shutdown_task_still_running)"'
```

Check resource use at the start, during the run, and after three hours:

```bash
sudo systemctl show price-collector-polymarket-chainlink \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
CHAINLINK_PID="$(systemctl show price-collector-polymarket-chainlink -p MainPID --value)"
ps -p "${CHAINLINK_PID}" -o pid,etime,%cpu,%mem,rss,vsz,cmd
```

Before running the final SQL evidence checks below, source the recorded window
and confirm both the full three hours and the two-minute telemetry grace have
elapsed. Do not accept the canary when this prints `window still running`:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
PHASE3_NOW_MS="$(date -u +%s%3N)"
if [ "$PHASE3_NOW_MS" -lt "$PHASE3_TELEMETRY_END_MS" ]; then
  printf 'window still running; wait until %s\n' \
    "$PHASE3_TELEMETRY_END_UTC" >&2
  exit 1
fi
printf 'fixed three-hour window complete: [%s, %s)\n' \
  "$PHASE3_START_UTC" "$PHASE3_END_UTC"
```

### Validate ordering and same-millisecond events

There is intentionally no heavy uniqueness index on the append-only event
table, so audit connection/sequence uniqueness directly:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS duplicate_connection_sequence_groups
FROM (
    SELECT connection_id, receive_sequence
    FROM raw_capture.chainlink_price_events
    WHERE received_wall_ns >= ${PHASE3_START_NS}
      AND received_wall_ns < ${PHASE3_END_NS}
    GROUP BY connection_id, receive_sequence
    HAVING count(*) > 1
) duplicates;

WITH ordered AS (
    SELECT
        connection_id,
        receive_sequence,
        received_wall_ns,
        received_monotonic_ns,
        lag(receive_sequence) OVER (
            PARTITION BY connection_id ORDER BY receive_sequence
        ) AS previous_sequence,
        lag(received_wall_ns) OVER (
            PARTITION BY connection_id ORDER BY receive_sequence
        ) AS previous_wall_ns,
        lag(received_monotonic_ns) OVER (
            PARTITION BY connection_id ORDER BY receive_sequence
        ) AS previous_monotonic_ns
    FROM raw_capture.chainlink_price_events
    WHERE received_wall_ns >= ${PHASE3_START_NS}
      AND received_wall_ns < ${PHASE3_END_NS}
)
SELECT
    count(*) FILTER (
        WHERE previous_sequence IS NOT NULL
          AND receive_sequence <> previous_sequence + 1
    ) AS sequence_gap_edges,
    COALESCE(sum(receive_sequence - previous_sequence - 1) FILTER (
        WHERE previous_sequence IS NOT NULL
          AND receive_sequence > previous_sequence + 1
    ), 0) AS missing_sequence_values,
    count(*) FILTER (
        WHERE previous_monotonic_ns IS NOT NULL
          AND received_monotonic_ns < previous_monotonic_ns
    ) AS monotonic_regressions,
    count(*) FILTER (
        WHERE previous_wall_ns IS NOT NULL
          AND received_wall_ns < previous_wall_ns
    ) AS wall_clock_regressions
FROM ordered;
"
```

Expected: zero duplicate groups and zero monotonic regressions. A sequence gap
is not automatically loss: RTDS `PING`/`PONG`, malformed, and nonmatching frames
are sequenced for session accounting but intentionally have no event row.
Reconcile gaps against session received/accepted/parse counters and logs. A wall
clock regression is a host-clock diagnostic; monotonic time and sequence remain
the receive-order authority.

Confirm that ticks sharing a local wall-clock millisecond remain separate:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH same_ms_groups AS (
    SELECT
        connection_id,
        received_wall_ns / 1000000 AS received_ms,
        count(*) AS event_rows,
        count(DISTINCT receive_sequence) AS distinct_sequences
    FROM raw_capture.chainlink_price_events
    WHERE received_wall_ns >= ${PHASE3_START_NS}
      AND received_wall_ns < ${PHASE3_END_NS}
    GROUP BY connection_id, received_wall_ns / 1000000
    HAVING count(*) > 1
)
SELECT
    count(*) AS same_ms_groups,
    COALESCE(sum(event_rows), 0) AS events_in_same_ms_groups,
    COALESCE(max(event_rows), 0) AS maximum_events_in_one_ms,
    COALESCE(sum(event_rows - distinct_sequences), 0)
        AS duplicate_sequence_rows
FROM same_ms_groups;
"
```

`duplicate_sequence_rows` must be zero. It is valid for `same_ms_groups` to be
zero if the provider did not produce that condition during the canary; focused
tests are the deterministic proof that equal-millisecond events are retained.

### Compare provider and local timing

Measure provider-event cadence, local monotonic arrival cadence, and signed wall
delivery differences independently:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH ordered AS (
    SELECT
        provider_event_ms,
        provider_message_ms,
        received_wall_ns,
        received_monotonic_ns,
        lag(provider_event_ms) OVER (
            PARTITION BY connection_id ORDER BY receive_sequence
        ) AS previous_provider_event_ms,
        lag(received_monotonic_ns) OVER (
            PARTITION BY connection_id ORDER BY receive_sequence
        ) AS previous_monotonic_ns
    FROM raw_capture.chainlink_price_events
    WHERE received_wall_ns >= ${PHASE3_START_NS}
      AND received_wall_ns < ${PHASE3_END_NS}
), timing AS (
    SELECT
        received_wall_ns / 1000000 - provider_event_ms
            AS provider_event_to_receive_ms,
        CASE WHEN provider_message_ms IS NULL THEN NULL
             ELSE received_wall_ns / 1000000 - provider_message_ms
        END AS provider_message_to_receive_ms,
        CASE WHEN provider_message_ms IS NULL THEN NULL
             ELSE provider_message_ms - provider_event_ms
        END AS provider_message_minus_event_ms,
        provider_event_ms - previous_provider_event_ms
            AS provider_interarrival_ms,
        (received_monotonic_ns - previous_monotonic_ns)::numeric / 1000000
            AS local_interarrival_ms,
        provider_event_ms < previous_provider_event_ms
            AS provider_time_regression
    FROM ordered
)
SELECT
    count(*) AS event_rows,
    min(provider_event_to_receive_ms) AS delivery_ms_minimum,
    round((percentile_cont(0.50) WITHIN GROUP (
        ORDER BY provider_event_to_receive_ms
    ))::numeric, 3) AS delivery_ms_p50,
    round((percentile_cont(0.95) WITHIN GROUP (
        ORDER BY provider_event_to_receive_ms
    ))::numeric, 3) AS delivery_ms_p95,
    round((percentile_cont(0.99) WITHIN GROUP (
        ORDER BY provider_event_to_receive_ms
    ))::numeric, 3) AS delivery_ms_p99,
    max(provider_event_to_receive_ms) AS delivery_ms_maximum,
    round((percentile_cont(0.95) WITHIN GROUP (
        ORDER BY provider_interarrival_ms
    ))::numeric, 3) AS provider_interarrival_ms_p95,
    round((percentile_cont(0.95) WITHIN GROUP (
        ORDER BY local_interarrival_ms
    ))::numeric, 3) AS local_interarrival_ms_p95,
    count(*) FILTER (WHERE provider_time_regression)
        AS provider_time_regressions,
    count(provider_message_to_receive_ms) AS rows_with_message_time,
    min(provider_message_to_receive_ms) AS message_delivery_ms_minimum,
    max(provider_message_to_receive_ms) AS message_delivery_ms_maximum,
    min(provider_message_minus_event_ms) AS message_minus_event_ms_minimum,
    max(provider_message_minus_event_ms) AS message_minus_event_ms_maximum
FROM timing;
"

timedatectl show -p NTPSynchronized --value
```

Do not combine provider cadence, local arrival cadence, and wall-clock delivery
into one latency number. Investigate provider-time regressions, sustained local
arrival stalls, or unexplained distribution changes. Negative signed values are
retained deliberately and usually require a clock or timestamp-semantics review.

### Audit Chainlink sessions

Ensure every recent event connection has session metadata and every completed
session has final counters:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH recent_sessions AS (
    SELECT *
    FROM raw_capture.feed_sessions
    WHERE source = 'polymarket_chainlink_rtds'
      AND connected_wall_ns >= ${PHASE3_JOURNAL_START_NS}
      AND connected_wall_ns < ${PHASE3_END_NS}
)
SELECT
    count(*) AS session_count,
    count(*) FILTER (
        WHERE ready_wall_ns IS NULL OR ready_monotonic_ns IS NULL
    ) AS sessions_never_ready,
    count(*) FILTER (WHERE disconnected_wall_ns IS NULL)
        AS sessions_still_open,
    count(*) FILTER (
        WHERE disconnected_wall_ns IS NOT NULL AND close_reason IS NULL
    ) AS closed_without_reason,
    COALESCE(sum(messages_received_total), 0) AS recorded_messages_received,
    COALESCE(sum(messages_accepted_total), 0) AS recorded_messages_accepted,
    COALESCE(sum(parse_errors_total), 0) AS recorded_parse_errors,
    COALESCE(sum(records_dropped_total), 0) AS session_attributed_drops
FROM recent_sessions;

WITH recent_event_connections AS (
    SELECT DISTINCT connection_id
    FROM raw_capture.chainlink_price_events
    WHERE received_wall_ns >= ${PHASE3_START_NS}
      AND received_wall_ns < ${PHASE3_END_NS}
)
SELECT count(*) FILTER (WHERE sessions.connection_id IS NULL)
       AS event_connections_without_session
FROM recent_event_connections events
LEFT JOIN raw_capture.feed_sessions sessions
  ON sessions.connection_id = events.connection_id
 AND sessions.source = 'polymarket_chainlink_rtds';

WITH event_counts AS (
    SELECT connection_id, count(*) AS raw_rows
    FROM raw_capture.chainlink_price_events
    WHERE received_wall_ns >= ${PHASE3_JOURNAL_START_NS}
      AND received_wall_ns < ${PHASE3_END_NS}
    GROUP BY connection_id
), completed_sessions AS (
    SELECT connection_id, messages_accepted_total
    FROM raw_capture.feed_sessions
    WHERE source = 'polymarket_chainlink_rtds'
      AND disconnected_wall_ns IS NOT NULL
      AND connected_wall_ns >= ${PHASE3_JOURNAL_START_NS}
      AND disconnected_wall_ns < ${PHASE3_END_NS}
)
SELECT count(*) FILTER (
    WHERE completed.messages_accepted_total <> COALESCE(events.raw_rows, 0)
) AS completed_sessions_with_event_count_mismatch
FROM completed_sessions completed
LEFT JOIN event_counts events USING (connection_id);
"
```

Expected: at least one ready session, at most one open session, no closed session
without a reason, no event connection without a session, no session-attributed
drop, and no completed-session event-count mismatch when runtime drops and batch
failures are zero. The current open session's persisted counters remain at their
opening values until its final upsert, so `raw_capture_summary` is authoritative
for live canary loss.

### Cross-check the normal historical path

For settled provider seconds, the final latest-received raw event must match the
normal one-second `price_samples` row. The ten-second margin excludes work that
may still be settling or flushing:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH chainlink_instrument AS (
    SELECT i.instrument_id
    FROM instruments i
    JOIN providers p ON p.provider_id = i.provider_id
    WHERE p.provider_code = 'polymarket_chainlink_rtds'
      AND i.symbol = 'BTCUSD'
), recent_raw AS (
    SELECT
        events.*,
        (events.provider_event_ms / 1000) * 1000 AS sample_second_ms
    FROM raw_capture.chainlink_price_events events
    WHERE events.received_wall_ns >= ${PHASE3_START_NS}
      AND events.received_wall_ns < ${PHASE3_END_NS}
), settled_seconds AS (
    SELECT sample_second_ms
    FROM recent_raw
    GROUP BY sample_second_ms
    HAVING max(received_wall_ns) < ${PHASE3_END_NS} - 10000000000
), ranked AS (
    SELECT
        raw.*,
        row_number() OVER (
            PARTITION BY raw.sample_second_ms
            ORDER BY raw.received_monotonic_ns DESC, raw.receive_sequence DESC
        ) AS latest_rank
    FROM recent_raw raw
    JOIN settled_seconds settled USING (sample_second_ms)
), expected AS (
    SELECT * FROM ranked WHERE latest_rank = 1
)
SELECT
    count(*) AS settled_captured_seconds,
    count(*) FILTER (WHERE samples.instrument_id IS NULL)
        AS missing_price_sample_seconds,
    count(*) FILTER (
        WHERE samples.instrument_id IS NOT NULL
          AND (
              samples.price IS DISTINCT FROM expected.price
              OR samples.provider_event_ms IS DISTINCT FROM expected.provider_event_ms
              OR samples.provider_message_ms IS DISTINCT FROM expected.provider_message_ms
              OR samples.received_ms IS DISTINCT FROM expected.received_wall_ns / 1000000
          )
    ) AS latest_value_mismatches
FROM expected
CROSS JOIN chainlink_instrument instrument
LEFT JOIN price_samples samples
  ON samples.instrument_id = instrument.instrument_id
 AND samples.sample_second_ms = expected.sample_second_ms;
"
```

Both missing and mismatch counts must be zero. A mismatch means the refactored
critical path did not preserve the documented latest-received provider-second
semantics even if raw capture itself is healthy.

After the full three hours, compare normal Chainlink history in the immediately
preceding three-hour baseline and accelerated-canary window:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
WITH periods(period, start_ms, end_ms) AS (
    VALUES
        ('baseline', (${PHASE3_START_MS} - 10800000)::bigint,
         ${PHASE3_START_MS}::bigint),
        ('canary', ${PHASE3_START_MS}::bigint, ${PHASE3_END_MS}::bigint)
), observations AS (
    SELECT ps.sample_second_ms
    FROM price_samples ps
    JOIN instruments i ON i.instrument_id = ps.instrument_id
    JOIN providers p ON p.provider_id = i.provider_id
    WHERE p.provider_code = 'polymarket_chainlink_rtds'
      AND i.symbol = 'BTCUSD'
), tagged AS (
    SELECT
        periods.period,
        observations.sample_second_ms,
        lag(observations.sample_second_ms) OVER (
            PARTITION BY periods.period ORDER BY observations.sample_second_ms
        ) AS previous_ms
    FROM periods
    JOIN observations
      ON observations.sample_second_ms >= periods.start_ms
     AND observations.sample_second_ms < periods.end_ms
)
SELECT
    periods.period,
    count(tagged.sample_second_ms) AS rows,
    round(count(tagged.sample_second_ms)::numeric / 10800 * 100, 3)
        AS second_coverage_percent,
    COALESCE(max((tagged.sample_second_ms - tagged.previous_ms) / 1000), 0)
        AS maximum_gap_seconds,
    count(tagged.sample_second_ms) FILTER (
        WHERE tagged.previous_ms IS NOT NULL
          AND tagged.sample_second_ms - tagged.previous_ms > 1000
    ) AS gap_count
FROM periods
LEFT JOIN tagged ON tagged.period = periods.period
GROUP BY periods.period
ORDER BY periods.period;
"
```

Investigate any material coverage reduction, new long gap, pending-state drop,
or mismatch with the raw evidence.

### Validate futures isolation and storage

Both evidence sources should now advance, while the unchanged futures service
continues its accepted Phase 2 state:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
SELECT
    (SELECT count(*)
     FROM raw_capture.binance_futures_price_trace_100ms
     WHERE bucket_start_ms >= ${PHASE3_END_MS} - 3600000
       AND bucket_start_ms < ${PHASE3_END_MS})
        AS futures_rows_last_hour,
    (SELECT count(*)
     FROM raw_capture.chainlink_price_events
     WHERE received_wall_ns >= ${PHASE3_END_NS} - 3600000000000
       AND received_wall_ns < ${PHASE3_END_NS})
        AS chainlink_rows_last_hour;

SELECT count(*) AS raw_capture_connections
FROM pg_stat_activity
WHERE application_name = 'price_collector_raw_capture';
"

sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE3_JOURNAL_START_UTC" --until "$PHASE3_TELEMETRY_END_UTC" -o cat \
  | grep '"event": "raw_capture_summary"' \
  | tail -n 5
curl -fsS http://127.0.0.1:9000/markets/current/live
```

Both row counts should be positive. Across the two independent collector
processes, the dedicated raw pools may use zero, one, or two connections as work
and reconnects occur, but never more than two. The futures service activation
timestamp and restart count must still match the pre-Phase 3 values, and its raw
loss/failure state must remain clean. The public futures value remains REST
based.

Measure Chainlink rows per UTC hour and final storage use:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
sudo -u postgres psql -d price_collector -c "
SELECT
    date_trunc(
        'hour', to_timestamp(received_wall_ns / 1000000000.0)
    ) AT TIME ZONE 'UTC' AS utc_hour,
    count(*) AS event_rows,
    count(DISTINCT connection_id) AS connections
FROM raw_capture.chainlink_price_events
WHERE received_wall_ns >= ${PHASE3_START_NS}
  AND received_wall_ns < ${PHASE3_END_NS}
GROUP BY 1
ORDER BY utc_hour;

SELECT pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0))
       AS raw_capture_relation_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'raw_capture'
  AND c.relkind = 'r';
"
sudo -u postgres psql -d price_collector -c "SELECT pg_size_pretty(pg_database_size('price_collector'));"
df -h /var/lib/postgresql
```

Compare relation, database, and filesystem growth with the recorded baseline.
The configured relation budget is not a filesystem quota.

### Phase 3 acceptance

Phase 3 is operationally complete under the accelerated rollout only after an
uninterrupted three-hour Chainlink run in which all of these are true:

- Phase 2 had already passed its separate futures-only three-hour accelerated
  acceptance.
- The Chainlink process did not unexpectedly restart; final
  `ActiveEnterTimestamp` and `NRestarts` match its enablement baseline.
- Raw Chainlink events and delivery successes increased across the canary.
- Raw drops, failed batches, suspension, and raw coalescing are zero; parse
  errors and sequence gaps are either zero or fully reconciled with control and
  rejected RTDS frames.
- Raw maintenance ran at least once and its cumulative failure count is zero.
- The raw queue repeatedly returned near zero and never reached capacity.
- Live/history worker failures and pending-history drops are zero; pending state
  repeatedly settled near zero and never reached its 5,000-second bound.
- Connection/sequence duplicates and monotonic regressions are zero. Any
  observed same-millisecond group contains a distinct sequence for every row.
- Every event connection has a ready session, completed sessions have close
  reasons and matching accepted/event counts, and at most one session is open.
- Provider cadence, local arrival cadence, signed delivery timing, message-time
  timing, reconnects, and NTP state were reviewed independently with no
  unexplained anomaly.
- Settled raw provider seconds match normal `price_samples`; Redis/API Chainlink
  remains healthy and normal historical coverage has no unexplained regression.
- Futures capture, flow/book behavior, REST live price, service uptime, and raw
  counters remain healthy and unchanged by the Chainlink enablement.
- The two raw pools use no more than two database connections, and CPU, memory,
  rows/hour, batch latency, relation growth, total database size, and filesystem
  free space remain acceptable.

Passing these checks authorizes the accelerated move to Phase 4; it does not
make three hours equivalent to a full-day soak. Keep both capture flags enabled
and continue ordinary counter, restart, resource, database-size, and filesystem
monitoring until the collectors have accumulated at least 24 uninterrupted
hours. Any later drop, failed batch, suspension, delivery failure, restart, or
capacity pressure remains a canary failure and triggers the rollback below.

### Phase 3 rollback

For a raw-capture-only failure, keep the accepted futures capture active and
restore exactly:

```text
BINANCE_FUTURES_STREAMS_ENABLED=true
RAW_FUTURES_TRACE_ENABLED=true
RAW_CHAINLINK_EVENTS_ENABLED=false
```

Then restart only Chainlink and verify its public and historical path:

```bash
sudoedit /etc/price-collector/collector.env
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
redis-cli -h 127.0.0.1 GET btc:live:chainlink

sudo -u postgres psql -d price_collector -c "
SELECT count(*) AS retained_chainlink_rows,
       max(received_wall_ns) AS latest_received_wall_ns
FROM raw_capture.chainlink_price_events;

SELECT count(*) AS raw_capture_connections
FROM pg_stat_activity
WHERE application_name = 'price_collector_raw_capture';
"
```

Run the retained-row query again later and confirm its maximum no longer
advances. The raw connection count should fall to at most one because futures
capture remains enabled. Retain the populated schema and captured evidence.

The raw flag does not disable the Phase 3 live and historical delivery workers.
If that refactor causes Redis, normal history, memory, ordering, or shutdown
failures, first set Chainlink raw capture to `false`, then create and push a
revert of the Phase 3 application commit in GitHub. After that revert is pushed:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
```

Do not drop `raw_capture`; the futures process still uses it and retained
Chainlink evidence may be needed to diagnose the failure.

## Redis Spot Checks

Redis is the live-card cache only. PostgreSQL remains the historical source.

```bash
redis-cli -h 127.0.0.1 MGET btc:live:binance_spot btc:live:chainlink btc:live:futures
```

Each populated key should look like:

```json
{"value":"62067.89","source_timestamp_ms":123,"received_ms":456}
```

## Database Spot Checks

On the droplet:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    count(*) AS rows,
    min(sample_second_at) AS first_sample,
    max(sample_second_at) AS latest_sample
FROM price_samples;
"
```

Latest market counts:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    mw.market_id,
    mw.market_start_at,
    mw.market_end_at,
    count(ps.*) AS sample_count
FROM market_windows mw
JOIN price_samples ps ON ps.market_id = mw.market_id
GROUP BY mw.market_id, mw.market_start_at, mw.market_end_at
ORDER BY mw.market_id DESC
LIMIT 10;
"
```

Futures flow and top-of-book row counts:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT 'binance_flow_1s' AS table_name, count(*) AS rows, max(sample_second_at) AS latest_sample
FROM binance_flow_1s
UNION ALL
SELECT 'binance_book_1s' AS table_name, count(*) AS rows, max(sample_second_at) AS latest_sample
FROM binance_book_1s;
"
```

Latest Polymarket resolution reconciliation state:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    market_id,
    resolution_status,
    resolution_type,
    chainlink_open_price,
    chainlink_close_price,
    winner,
    winning_token_id,
    up_payout,
    down_payout,
    resolved_at_ms,
    resolution_source,
    last_checked_ms,
    next_check_ms,
    resolution_attempts
FROM polymarket_btc_5m_resolutions
ORDER BY market_id DESC
LIMIT 10;
"
```

`pending` rows with a future `next_check_ms` are scheduled for another attempt.
Terminal `resolved` rows use `resolution_type` to distinguish a normal
`winner` from a `split`. Their `next_check_ms` becomes `NULL` after official
Chainlink open/close metadata is also stored.

## Confirm Local-Only Binding

On the droplet:

```bash
ss -ltnp | grep ':9000'
ss -ltnp | grep ':5432'
ss -ltnp | grep ':6379'
```

Acceptable:

```text
127.0.0.1:9000
127.0.0.1:5432
127.0.0.1:6379
```

Not acceptable:

```text
0.0.0.0:9000
0.0.0.0:5432
0.0.0.0:6379
```
