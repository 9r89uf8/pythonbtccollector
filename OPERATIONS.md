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

## Phase 2 Futures-Only Capture Canary

Phase 2 wires the Binance futures `aggTrade` reader to the bounded raw-capture
runtime. The repository and environment-example default remains disabled, so
deploying the code does not start capture by itself. The code checkpoint is
ready when its tests pass and it is safely deployed with the flag still
`false`; Phase 2 is operationally complete only after the production canary has
run continuously for 24 hours and every acceptance check below passes.

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

Record the preceding 24-hour one-second flow and book coverage before the
restart. These figures are the comparison baseline for the final canary query:

```bash
sudo -u postgres psql -d price_collector -c "
WITH datasets(dataset) AS (
    VALUES ('flow'), ('book')
), observations AS (
    SELECT 'flow' AS dataset, sample_second_ms
    FROM binance_flow_1s
    WHERE venue = 'binance_usdm_perp'
      AND symbol = 'BTCUSDT'
      AND sample_second_ms >= (extract(epoch FROM now() - interval '24 hours') * 1000)::bigint
    UNION ALL
    SELECT 'book', sample_second_ms
    FROM binance_book_1s
    WHERE venue = 'binance_usdm_perp'
      AND symbol = 'BTCUSDT'
      AND sample_second_ms >= (extract(epoch FROM now() - interval '24 hours') * 1000)::bigint
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
        count(ordered.sample_second_ms)::numeric / 86400 * 100,
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

Confirm it without printing secrets, then restart only the affected collector:

```bash
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED)=' /etc/price-collector/collector.env
sudo systemctl restart price-collector-binance-futures
sudo systemctl status price-collector-binance-futures --no-pager
sudo systemctl show price-collector-binance-futures \
  -p ActiveEnterTimestamp -p NRestarts -p MainPID -p CPUUsageNSec -p MemoryCurrent
sudo journalctl -u price-collector-binance-futures -n 100 --no-pager
```

The service activation timestamp is the start of the 24-hour canary. Record the
`systemctl show` output. An unexpected process restart resets the observation
window and the cumulative in-process counters; a normal Binance WebSocket
reconnect does not.

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

### Monitor the 24-hour run

The futures collector emits one structured `raw_capture_summary` every 60
seconds. Do not log individual raw events. Inspect the most recent summaries:

```bash
sudo journalctl -u price-collector-binance-futures --since "24 hours ago" -o cat \
  | grep '"event": "raw_capture_summary"' \
  | tail -n 20
```

Summarize the observed writer latency, queue pressure, and final counters using
only the structured summary records:

```bash
sudo journalctl -u price-collector-binance-futures --since "24 hours ago" -o cat \
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
out-of-sequence drops. No output is expected from this command:

```bash
sudo journalctl -u price-collector-binance-futures --since "24 hours ago" -o cat \
  | grep -E 'raw_capture_.*(failed|dropped|suspended)|binance_futures_raw_capture_.*failed|binance_futures_(snapshot|flow_flush|book_flush)_failed'
```

At the start, during the run, and after 24 hours, inspect service resource use.
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
    WHERE bucket_start_ms >=
          (extract(epoch FROM now() - interval '24 hours') * 1000)::bigint
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
sudo -u postgres psql -d price_collector -c "
WITH recent_sessions AS (
    SELECT *
    FROM raw_capture.feed_sessions
    WHERE source = 'binance_futures_agg_trade'
      AND connected_wall_ns >= (
          extract(epoch FROM now() - interval '24 hours') * 1000000000
      )::bigint
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
    WHERE bucket_start_ms >=
          (extract(epoch FROM now() - interval '24 hours') * 1000)::bigint
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
WHERE bucket_start_ms >=
      (extract(epoch FROM now() - interval '24 hours') * 1000)::bigint
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

After the full 24 hours, compare the immediately preceding baseline day with
the canary day:

```bash
sudo -u postgres psql -d price_collector -c "
WITH anchor AS (
    SELECT (extract(epoch FROM date_trunc('second', now())) * 1000)::bigint AS end_ms
), periods AS (
    SELECT
        'baseline' AS period,
        end_ms - 172800000 AS start_ms,
        end_ms - 86400000 AS end_ms
    FROM anchor
    UNION ALL
    SELECT 'canary', end_ms - 86400000, end_ms
    FROM anchor
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
        count(tagged.sample_second_ms)::numeric / 86400 * 100,
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

Phase 2 is operationally complete only when all of these are true after a
continuous 24-hour run:

- The futures collector process did not unexpectedly restart; its final
  `ActiveEnterTimestamp` and `NRestarts` match the recorded starting values.
- `records_persisted_total` increased and futures trace rows span the canary.
- `records_dropped_total=0`, `batches_failed_total=0`, and
  `capture_suspended=false` in the final summary.
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
