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

## Chainlink Accepted-Event Idle Watchdog

The Chainlink RTDS connection can remain open while accepted BTC/USD price
events stop. The collector now starts a monotonic deadline after subscription
and resets it only when a valid expected-topic, expected-symbol Chainlink tick
is accepted. Before that first accepted tick, an empty RTDS bootstrap frame and
a strictly shaped `crypto_prices` subscription-history dump for `btc/usd` are
counted as received-only startup frames instead of parse errors. They, PING/PONG,
malformed JSON, and unrelated frames do not reset the deadline. After a live
tick has been accepted, the same empty or startup-shaped frame is a parse error.
At the default 10,000 ms threshold, the reader emits
`polymarket_rtds_idle_reconnect_triggered`, closes that WebSocket as an existing
`proactive_reconnect`, applies jittered backoff, and resubscribes inside the
same process. Redis, PostgreSQL, the API, and the shadow worker are not
restarted or reset.

This is a code-only rollout. It changes no schema, dependency declaration,
systemd unit, or API shape. After the change is pushed to GitHub, deploy it and
restart only the Chainlink collector:

```bash
set -euo pipefail
cd /opt/price-collector

sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_polymarket_chainlink_collector.py \
  tests/test_config.py \
  tests/test_deployment.py \
  -q

WATCHDOG_SETTING='POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000'
if sudo grep -q \
  '^POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=' \
  /etc/price-collector/collector.env
then
  sudo sed -i \
    's/^POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=.*/POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000/' \
    /etc/price-collector/collector.env
else
  printf '%s\n' "$WATCHDOG_SETTING" | \
    sudo tee -a /etc/price-collector/collector.env >/dev/null
fi
sudo grep -Fqx "$WATCHDOG_SETTING" \
  /etc/price-collector/collector.env

WATCHDOG_DEPLOY_UTC="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"

sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status \
  price-collector-polymarket-chainlink \
  --no-pager
```

The supported timeout range is 5,000 through 60,000 ms. Do not replace the
production environment file with the example. The explicit production value
above matches the default and ensures the active policy is visible to
operators.

Verify startup configuration and advancing Chainlink receipt time:

```bash
sudo journalctl \
  -u price-collector-polymarket-chainlink \
  --since "$WATCHDOG_DEPLOY_UTC" \
  -n 200 \
  --no-pager

curl -fsS http://127.0.0.1:9000/healthz
redis-cli -h 127.0.0.1 -p 6379 PING

for attempt in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:9000/markets/current/live |
    python3 -c '
import json
import sys

payload = json.load(sys.stdin)
chainlink = (payload.get("prices") or {}).get("chainlink") or {}
shadow = (payload.get("signals") or {}).get("chainlink_catchup") or {}
age = chainlink.get("received_age_ms")
print({
    "chainlink_received_ms": chainlink.get("received_ms"),
    "chainlink_received_age_ms": age,
    "shadow_valid": shadow.get("valid"),
    "shadow_status": shadow.get("status"),
})
raise SystemExit(0 if age is not None and age <= 2500 else 1)
'
  then
    CHAINLINK_FRESH=1
    break
  fi
  sleep 1
done
test "${CHAINLINK_FRESH:-0}" -eq 1

redis-cli --raw PTTL btc:live:chainlink_shadow
```

A natural watchdog recovery later appears in the journal in this order:

```text
polymarket_rtds_idle_reconnect_triggered
polymarket_rtds_reconnect_scheduled
polymarket_rtds_connecting
polymarket_rtds_subscribed
```

The idle event contains the accepted-tick and frame idle ages, connection
message counters, total idle reconnect count, and consecutive idle reconnect
count. Repeated connections that accept no tick use increasing jittered
backoff. The next accepted tick resets that consecutive sequence. The raw
session close reason remains the schema-compatible `proactive_reconnect`.

Do not force an upstream outage to test this on production. When a natural
event occurs, compare `MainPID`, `NRestarts`, and `ExecMainStartTimestamp`
before and after the event to confirm that systemd did not restart the process.
Then confirm that the public Chainlink value recovered:

```bash
systemctl show price-collector-polymarket-chainlink \
  -p MainPID \
  -p NRestarts \
  -p ExecMainStartTimestamp

CHAINLINK_START_UTC="$(systemctl show \
  price-collector-polymarket-chainlink \
  -p ExecMainStartTimestamp \
  --value)"
sudo journalctl \
  -u price-collector-polymarket-chainlink \
  --since "$CHAINLINK_START_UTC" \
  -o cat \
  --no-pager | \
grep -E 'polymarket_rtds_(idle_reconnect_triggered|reconnect_scheduled|connecting|subscribed)' || true

curl -fsS http://127.0.0.1:9000/markets/current/live
```

Do not delete Redis keys, evaluation rows, or immutable shadow decision files.
The cached Chainlink value intentionally remains present and ages during the
bounded reconnect interval.

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
A three-hour window may not cross a six-hour raw partition boundary. The
deliberate Phase 4 checks that would close that evidence gap are now explicitly
deferred and remain unproven technical debt.

This was the pre-cutover shadow-evidence checkpoint. It kept Binance REST
`/fapi/v2/ticker/price` as the Redis/API `futures.last` source, kept the existing
one-second flow and book paths active, and did not enable Chainlink capture.
The REST assertions and measurements in this Phase 2 section remain historical
evidence; they are not post-Phase 5 operating instructions.

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
- For this pre-cutover Phase 2 gate, `btc:live:futures` and
  `/markets/current/live` remain healthy and REST-based.

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
authorizes the separately gated Phase 5 source cutover while both collectors
continue background monitoring toward at least 24 uninterrupted hours. The
user explicitly deferred Phase 4 retention validation; automatic maintenance
remains active, but its deliberate expired-partition and relation-pressure
checks remain unproven technical debt.

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
loss/failure state must remain clean. During this pre-cutover Phase 3 evidence
window, the public futures value was REST-based.

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
- Futures capture, flow/book behavior, the then-REST-backed live price, service
  uptime, and raw counters remain healthy and unchanged by the Chainlink
  enablement. This is pre-cutover Phase 3 evidence.
- The two raw pools use no more than two database connections, and CPU, memory,
  rows/hour, batch latency, relation growth, total database size, and filesystem
  free space remain acceptable.

Passing these checks after the fixed telemetry endpoint authorizes the
separately supervised Phase 5 source cutover; it does not make three hours
equivalent to a full-day soak or prove Phase 4 retention behavior. Keep both
capture flags enabled and continue ordinary counter, restart, resource,
database-size, and filesystem monitoring until the collectors have accumulated
at least 24 uninterrupted hours. Any later drop, failed batch, suspension,
delivery failure, restart, or capacity pressure remains a canary failure and
triggers the rollback below.

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

## Phase 4 Retention Validation - Deferred and Unproven

Phase 4 was explicitly deferred on 2026-07-11. The 60-second automatic
partition-maintenance worker remains active, and its counters, relation size,
database size, and filesystem capacity must continue to be monitored. Healthy
automatic runs do **not** prove the two destructive-policy branches that Phase 4
was intended to test.

Keep this open checklist as technical debt:

1. Create deliberately expired test partitions.
2. Run maintenance and prove that it drops whole partitions rather than rows.
3. Prove that relation-size pressure drops the oldest completed futures and
   Chainlink partition pair without touching the current interval.
4. Prove that normal collection continues if retention maintenance fails.
5. Recheck `df -h`, total database size, and raw relation size because the
   relation budget does not bound WAL or total filesystem use.

Until those checks are deliberately executed, describe retention as
**automatic but unvalidated**, not complete. This deferral does not disable
maintenance and does not authorize dropping the `raw_capture` schema.

## Phase 5 Futures WebSocket Last-Price Cutover

Phase 5 is a direct source cutover for the futures service only. The Phase 2 and
Phase 3 statements that `/fapi/v2/ticker/price` supplied `futures.last` document
the accepted pre-cutover state and remain useful baseline evidence.

### Phase 5 behavior contract

- `aggTrade.p` is the sole value for Redis/API `futures.last` and the snapshot
  `futures_last_price`; `aggTrade.T` is the source timestamp.
- Redis `received_ms` is the exact local wall-clock receive stamp captured
  immediately after the WebSocket `recv()`, not a REST-poll or worker time.
- The snapshot loop uses only a validated trade from the current WebSocket
  connection whose local monotonic receive age is at most `STALE_PRICE_MS`.
- If there is no accepted trade for the current connection, or the latest trade
  is stale, the collector does not delete or overwrite `btc:live:futures`; its
  API `source_age_ms` and `received_age_ms` increase naturally. The snapshot
  still stores premium/index and open-interest data, with
  `futures_last_price` and `futures_last_price_time_ms` set to `NULL`.
- Futures REST remains limited to `/fapi/v1/premiumIndex`,
  `/fapi/v1/openInterest`, and the existing
  `/futures/data/openInterestHist` request. There is no ticker fallback and no
  book-midpoint or microprice substitute for "last."
- `BINANCE_FUTURES_STREAMS_ENABLED=true` is mandatory after this cutover. Keep
  `RAW_FUTURES_TRACE_ENABLED=true` and `RAW_CHAINLINK_EVENTS_ENABLED=true`
  unless a separate accepted raw-capture rollback requires otherwise.

### Do not deploy before Phase 3 acceptance

The fixed Phase 3 telemetry endpoint is `2026-07-11T20:29:53Z`. Do not pull or
restart the futures service before that instant, and do not treat reaching the
clock time alone as acceptance. Run all Phase 3 acceptance checks above first.
The Chainlink process must still have the exact activation identity recorded at
the start of this canary:

```text
MainPID=77219
NRestarts=0
ActiveEnterTimestamp=Sat 2026-07-11 17:27:53 UTC
```

Use this hard gate after the Phase 3 evidence queries have passed:

```bash
(
set -e
source <(sudo -u pricecollector cat /var/lib/price-collector/phase3-canary-window.env)
PHASE3_NOW_MS="$(date -u +%s%3N)"
CHAINLINK_PID="$(systemctl show price-collector-polymarket-chainlink -p MainPID --value)"
CHAINLINK_RESTARTS="$(systemctl show price-collector-polymarket-chainlink -p NRestarts --value)"
CHAINLINK_ACTIVE="$(systemctl show price-collector-polymarket-chainlink -p ActiveEnterTimestamp --value)"
FUTURES_PID="$(systemctl show price-collector-binance-futures -p MainPID --value)"
FUTURES_RESTARTS="$(systemctl show price-collector-binance-futures -p NRestarts --value)"
FUTURES_ACTIVE="$(systemctl show price-collector-binance-futures -p ActiveEnterTimestamp --value)"

test "$PHASE3_NOW_MS" -ge "$PHASE3_TELEMETRY_END_MS"
test "$PHASE3_START_MS" = "1783790873000"
test "$PHASE3_END_MS" = "1783801673000"
test "$PHASE3_TELEMETRY_END_MS" = "1783801793000"
test "$CHAINLINK_PID" = "77219"
test "$CHAINLINK_RESTARTS" = "0"
test "$CHAINLINK_ACTIVE" = "Sat 2026-07-11 17:27:53 UTC"
test "$FUTURES_PID" = "74081"
test "$FUTURES_RESTARTS" = "0"
test "$FUTURES_ACTIVE" = "Sat 2026-07-11 13:44:42 UTC"
sudo systemctl is-active --quiet price-collector-polymarket-chainlink
sudo systemctl is-active --quiet price-collector-binance-futures
sudo systemctl show price-collector-binance-futures \
  -p MainPID -p NRestarts -p ActiveEnterTimestamp
printf 'Phase 3 identity/time gate passed; complete the acceptance checklist before Phase 5.\n'
)
```

Any failed `test` stops this command block with a nonzero status. A changed
Chainlink PID, restart count, or activation timestamp invalidates this fixed
window; repeat Phase 3 rather than deploying Phase 5. The unchanged futures
PID/restart evidence must also satisfy the Phase 3 checklist.

### Deploy and record the cutover

Run this only after the Phase 5 change is pushed to GitHub and the preceding
gate and full Phase 3 acceptance both pass. This checkpoint changes futures
runtime code only: it has no schema or systemd-unit step. It installs the locked
requirements as usual, records the exact before/after Git commits and UTC
cutover instant, and restarts **only** `price-collector-binance-futures`.

```bash
(
set -e
cd /opt/price-collector
PHASE5_PREVIOUS_COMMIT="$(sudo -u pricecollector git rev-parse HEAD)"
sudo -u pricecollector git pull --ff-only
PHASE5_COMMIT="$(sudo -u pricecollector git rev-parse HEAD)"
sudo -u pricecollector .venv/bin/pip install -r requirements.txt

if grep -R -n --include='*.py' --fixed-strings '/fapi/v2/ticker/price' price_collector; then
  printf 'Refusing Phase 5: the removed REST ticker endpoint is still present.\n' >&2
  exit 1
fi

for expected in \
  'BINANCE_FUTURES_STREAMS_ENABLED=true' \
  'RAW_FUTURES_TRACE_ENABLED=true' \
  'RAW_CHAINLINK_EVENTS_ENABLED=true'; do
  sudo grep -Fqx "$expected" /etc/price-collector/collector.env
done
sudo grep -E '^(BINANCE_FUTURES_STREAMS_ENABLED|RAW_FUTURES_TRACE_ENABLED|RAW_CHAINLINK_EVENTS_ENABLED|STALE_PRICE_MS)=' /etc/price-collector/collector.env
redis-cli -h 127.0.0.1 GET btc:live:futures

PHASE5_CUTOVER_MS="$(date -u +%s%3N)"
PHASE5_CUTOVER_UTC="$(date -u '+%Y-%m-%dT%H:%M:%S.%3NZ')"
PHASE5_FILE=/var/lib/price-collector/phase5-cutover.env
{
  printf 'PHASE5_PREVIOUS_COMMIT=%s\n' "$PHASE5_PREVIOUS_COMMIT"
  printf 'PHASE5_COMMIT=%s\n' "$PHASE5_COMMIT"
  printf 'PHASE5_CUTOVER_MS=%s\n' "$PHASE5_CUTOVER_MS"
  printf 'PHASE5_CUTOVER_UTC=%s\n' "$PHASE5_CUTOVER_UTC"
} | sudo -u pricecollector tee "$PHASE5_FILE" >/dev/null
sudo -u pricecollector chmod 600 "$PHASE5_FILE"

sudo systemctl restart price-collector-binance-futures
PHASE5_MAIN_PID="$(systemctl show price-collector-binance-futures -p MainPID --value)"
PHASE5_ACTIVE="$(systemctl show price-collector-binance-futures -p ActiveEnterTimestamp --value)"
{
  printf 'PHASE5_MAIN_PID=%s\n' "$PHASE5_MAIN_PID"
  printf 'PHASE5_ACTIVE=%q\n' "$PHASE5_ACTIVE"
} | sudo -u pricecollector tee -a "$PHASE5_FILE" >/dev/null

sudo -u pricecollector cat "$PHASE5_FILE"
sudo systemctl status price-collector-binance-futures --no-pager
sudo systemctl show price-collector-binance-futures \
  -p MainPID -p Result -p NRestarts -p ActiveEnterTimestamp
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE5_CUTOVER_UTC" -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
)
```

`STALE_PRICE_MS` may be absent from the environment file; in that case its code
default is `10000`. The other three displayed flags must all be `true`. Do not
restart Chainlink, the API, Redis, or any other collector for this checkpoint.

The terminated pre-Phase 5 futures process can emit the already-known
`asyncio.CancelledError` and exit-status-1 shutdown record during this one
restart because it is still running the old code. Judge the cutover by the new
PID and service state. Phase 5 handles cancellation as a clean exit, so that
legacy shutdown record must not recur on later restarts of the new build.

### Supervise the first 15 minutes

Keep one terminal on the following bounded loop. It confirms that the original
Phase 5 process remains active and prints the public futures value and both age
fields once per minute. The values and `aggTrade.T` source timestamp should
continue advancing, and the receive age should normally remain small during an
active BTC market:

```bash
(
set -e
source <(sudo -u pricecollector cat /var/lib/price-collector/phase5-cutover.env)
for minute in $(seq 1 16); do
  printf '\nminute=%s at=%s\n' "$minute" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  test "$(systemctl show price-collector-binance-futures -p MainPID --value)" = "$PHASE5_MAIN_PID"
  test "$(systemctl show price-collector-binance-futures -p NRestarts --value)" = "0"
  sudo systemctl is-active --quiet price-collector-binance-futures
  curl -fsS http://127.0.0.1:9000/markets/current/live | python3 -c '
import json, sys
payload = json.load(sys.stdin)
print(json.dumps(payload["futures"]["last"], indent=2))
'
  if [ "$minute" -lt 16 ]; then sleep 60; fi
done
)
```

After at least one 60-second telemetry interval, inspect only the Phase 5
window. The final summary should show an advancing live delivery/attempted
sequence, increasing successes, zero live failures, a current WS trade, and no
new unexplained ID gap, duplicate, regression, raw drop, failed batch, or
storage-suspension counters:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase5-cutover.env)
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE5_CUTOVER_UTC" -o cat --no-pager \
  | grep '"event": "raw_capture_summary"' \
  | tail -n 3

sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE5_CUTOVER_UTC" -o cat --no-pager \
  | grep '"event": "raw_capture_summary"' \
  | tail -n 1 \
  | python3 -c '
import json, sys
row = json.load(sys.stdin)
keys = (
    "futures_live_delivery_sequence",
    "futures_live_attempted_sequence",
    "futures_live_successes_total",
    "futures_live_failures_total",
    "shadow_ws_current_for_connection",
    "shadow_ws_id_gap_events_total",
    "shadow_ws_duplicate_ids_total",
    "shadow_ws_regressions_total",
    "records_dropped_total",
    "batches_failed_total",
    "capture_suspended",
    "queue_depth",
)
print(json.dumps({key: row.get(key) for key in keys}, indent=2))
'
```

Check snapshot, flow, book, and raw-trace progress since the recorded cutover.
An isolated `NULL` futures last during startup, reconnect, or a proven stale
period is the intended safe behavior; investigate repeated `NULL` values during
an otherwise active current WebSocket connection. The latest normal snapshot
should have non-null last-price/time, premium/index, and open-interest fields:

```bash
source <(sudo -u pricecollector cat /var/lib/price-collector/phase5-cutover.env)
sudo -u postgres psql -d price_collector -v cutover_ms="$PHASE5_CUTOVER_MS" -c "
SELECT
    count(*) AS snapshots,
    count(*) FILTER (WHERE futures_last_price IS NULL) AS null_last_snapshots,
    max(sample_second_at) AS latest_snapshot
FROM binance_futures_snapshots
WHERE received_ms >= :'cutover_ms'::bigint;

SELECT
    sample_second_at,
    futures_last_price,
    futures_last_price_time_ms,
    mark_price,
    index_price,
    open_interest,
    received_ms
FROM binance_futures_snapshots
ORDER BY sample_second_ms DESC
LIMIT 5;

SELECT 'flow' AS dataset, count(*) AS rows, max(sample_second_at) AS latest_sample
FROM binance_flow_1s
WHERE received_ms >= :'cutover_ms'::bigint
UNION ALL
SELECT 'book', count(*), max(sample_second_at)
FROM binance_book_1s
WHERE received_ms >= :'cutover_ms'::bigint;

SELECT
    count(*) AS historical_oi_rows,
    max(binance_timestamp_ms) AS latest_binance_oi_timestamp_ms,
    max(received_ms) AS latest_historical_oi_received_ms
FROM binance_futures_oi_5m_summaries
WHERE received_ms >= :'cutover_ms'::bigint;

SELECT
    count(*) AS trace_rows,
    max(last_received_wall_ns) AS latest_trace_received_wall_ns
FROM raw_capture.binance_futures_price_trace_100ms
WHERE last_received_wall_ns >= :'cutover_ms'::bigint * 1000000;
"
```

Finally, scan a bounded journal and confirm the Chainlink canary service was not
restarted by this rollout:

```bash
(
set -e
source <(sudo -u pricecollector cat /var/lib/price-collector/phase5-cutover.env)
sudo journalctl -u price-collector-binance-futures \
  --since "$PHASE5_CUTOVER_UTC" --no-pager \
  | grep -E 'live_cache_write_failed|binance_futures_(live_worker_failed|snapshot_failed|flow_flush_failed|book_flush_failed|historical_oi_failed|raw_capture_.*failed)|raw_capture_(summary_failed|queue_oldest_dropped|batch_group_failed|background_task_failed|maintenance_failed|suspended_by_storage_budget)' || true
sudo systemctl show price-collector-binance-futures \
  -p MainPID -p Result -p NRestarts -p ActiveEnterTimestamp -p MemoryCurrent -p CPUUsageNSec
test "$(systemctl show price-collector-polymarket-chainlink -p MainPID --value)" = "77219"
test "$(systemctl show price-collector-polymarket-chainlink -p NRestarts --value)" = "0"
test "$(systemctl show price-collector-polymarket-chainlink -p ActiveEnterTimestamp --value)" = "Sat 2026-07-11 17:27:53 UTC"
sudo systemctl show price-collector-polymarket-chainlink \
  -p MainPID -p Result -p NRestarts -p ActiveEnterTimestamp
curl -fsS http://127.0.0.1:9000/healthz
redis-cli -h 127.0.0.1 GET btc:live:futures
redis-cli -h 127.0.0.1 GET btc:live:chainlink
sudo -u postgres psql -d price_collector -v cutover_ms="$PHASE5_CUTOVER_MS" -c "
SELECT
    count(*) AS chainlink_raw_rows_since_cutover,
    max(received_wall_ns) AS latest_chainlink_raw_received_wall_ns
FROM raw_capture.chainlink_price_events
WHERE received_wall_ns >= :'cutover_ms'::bigint * 1000000;

SELECT
    count(*) AS chainlink_samples_since_cutover,
    max(ps.sample_second_at) AS latest_chainlink_sample
FROM price_samples ps
JOIN instruments i ON i.instrument_id = ps.instrument_id
JOIN providers p ON p.provider_id = i.provider_id
WHERE p.provider_code = 'polymarket_chainlink_rtds'
  AND i.symbol = 'BTCUSD'
  AND ps.received_ms >= :'cutover_ms'::bigint;
"
)
```

Investigate every matching failure line. A transient REST timeout does not
change the source back to REST, but repeated premium/open-interest failures or
a stalled snapshot path fail the supervised check.

### Phase 5 acceptance

Accept the source cutover only when all of these are true for at least 15
continuous minutes:

- The futures PID remains `PHASE5_MAIN_PID`, `NRestarts=0`, and the service and
  API remain healthy.
- Redis/API futures value, `aggTrade.T` source timestamp, and exact local
  receive timestamp advance; both age fields normally remain fresh.
- Live-delivery successes advance with zero live-delivery failures, and the
  current connection has a fresh accepted trade.
- The deployed Python source contains no `/fapi/v2/ticker/price` call and there
  is no REST or book-derived fallback.
- Snapshots, flow, book, historical five-minute open interest, and raw futures
  trace continue advancing. Any null-last snapshot is explained by startup,
  reconnect, or `STALE_PRICE_MS` behavior.
- Raw drops, failed batches, capture suspension, unexplained stream integrity
  counters, and repeated REST/snapshot failures remain zero.
- The Chainlink PID, restart count, live/history output, and raw capture remain
  unchanged by the futures-only restart.

Record the retained `/var/lib/price-collector/phase5-cutover.env` file with the
acceptance evidence. Phase 4 remains deferred and unproven.

### Phase 5 Git-revert rollback

If futures live data stalls, receipt/source times are wrong, snapshot/flow/book
collection regresses, or any sole-source invariant fails, do not reintroduce a
runtime REST fallback by editing production. Create a Git revert of the exact
Phase 5 commit (or commit range) and push it. On the development checkout,
first compare the intended commit with `PHASE5_COMMIT` recorded on the droplet.
If Phase 5 was the single latest commit:

```bash
git pull --ff-only
git log -1 --oneline
git revert --no-edit HEAD
git push
```

If the recorded deployment contained multiple Phase 5 commits, revert the
complete `PHASE5_PREVIOUS_COMMIT..PHASE5_COMMIT` range instead and push the
resulting revert commit(s). After the revert is on GitHub, run this on the
droplet; restart only futures:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-binance-futures
sudo systemctl status price-collector-binance-futures --no-pager
sudo systemctl show price-collector-binance-futures \
  -p MainPID -p Result -p NRestarts -p ActiveEnterTimestamp
sudo journalctl -u price-collector-binance-futures -n 100 --no-pager
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
redis-cli -h 127.0.0.1 GET btc:live:futures
```

The revert restores the accepted pre-cutover REST behavior. It does not require
a schema change and must not restart Chainlink or disable either raw-capture
flag unless a separate raw-capture failure justifies that action.

## Retired shadow-model experiment tooling

The offline raw-replay, provisional-selection, shorter-lag, and post-hoc
recovery command modules were retired after the accepted model evidence was
consolidated. They are not production dependencies and are no longer
available as commands in this checkout.

The current decision, accepted metrics, failed shorter-lag experiment, and
integrity hashes are preserved in
[`CHAINLINK_SHADOW_MODEL_HISTORY.md`](CHAINLINK_SHADOW_MODEL_HISTORY.md).
Do not recreate or rerun an old decision under the same artifact name. A
future replacement model requires a newly versioned implementation, fresh
calibration, and a later untouched holdout.

## Shadow-signal standalone worker

This section operates the current standalone shadow worker. It is unrelated to
the still-deferred raw-capture partition and retention validation also named
Phase 4 elsewhere in this guide.

The standalone `price-collector-shadow-signal` service reads
`btc:live:futures` and `btc:live:chainlink` together every 100 ms, runs every
candidate from the accepted Phase 3 decision, and publishes only its frozen
primary to `btc:live:chainlink_shadow`. The output key has a 2-second TTL. This
phase adds no PostgreSQL writes and does not expose the signal through the API.

The SHA-named selection and replay-configuration files in the repository root
are immutable restore and provenance copies of the accepted decision. The
activation steps below use byte-identical, root-owned runtime copies under
`/var/lib/price-collector/shadow-decisions`.

### 1. Deploy the Phase 4 code

Run this only after the Phase 4 change has been pushed to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal.py \
  tests/test_shadow_signal_artifact.py \
  tests/test_shadow_signal_collector.py \
  tests/test_shadow_signal_evaluation.py \
  tests/test_shadow_signal_reporting.py \
  tests/test_live_cache.py \
  tests/test_config.py \
  tests/test_deployment.py
```

There is no schema migration in this phase.

### 2. Identify the accepted decision evidence

Use the exact SHA-named restore copies shipped in the repository:

```bash
SOURCE_SELECTION="/opt/price-collector/selection-1783983205028-890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699.json"
SOURCE_REPLAY_CONFIG="/opt/price-collector/replay-config-1783983205028-e11377f4f4cb0a6bfc91a682347c77d67ed1d81a83d03b798ff1d963fed6b5e9.json"

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
replay_sha = hashlib.sha256(replay_raw).hexdigest()
provenance_shas = {
    report["sha256"] for report in selection["provenance"]["reports"]
}

assert selection["schema_version"] == 2
assert selection["policy"]["version"] == "chronological_holdout_v2"
assert selection["status"] == "selected"
assert selection["selection_performed"] is True
assert selection["decision"]["provisional_primary_model"] is not None
assert replay["schema_version"] == 2 and replay["status"] == "ok"
assert replay_sha in provenance_shas
print({
    "selection_sha256": hashlib.sha256(selection_raw).hexdigest(),
    "replay_sha256": replay_sha,
    "primary": selection["decision"]["provisional_primary_model"],
})
PY
```

Stop if any assertion fails. Do not substitute an old selection artifact, an
abstained decision, or a replay report absent from the selection provenance.

### 3. Promote immutable runtime copies

The service validates both files once at startup. It requires direct,
root-owned, non-writable files under the trusted directory:

```bash
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

( set -e
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

An existing destination must compare byte-for-byte equal. If `cmp` returns
nonzero, or if the trusted directory contains anything except this configured
pair, stop. Use a new evidence-specific filename for changed content; never
overwrite an immutable decision file.

### 4. Configure the dedicated Redis-only environment

Create the dedicated file only if it does not already exist, then update only
the shadow keys. Do not replace `collector.env` or `api.env`:

```bash
sudo install -d -o root -g pricecollector -m 0750 /etc/price-collector
if ! sudo test -e /etc/price-collector/shadow-signal.env; then
  sudo install -o root -g pricecollector -m 0640 \
    /opt/price-collector/deployment/shadow-signal.env.example \
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

sudo grep -E \
  '^(SHADOW_SIGNAL_|REDIS_|APP_ENV|LOG_LEVEL)' \
  /etc/price-collector/shadow-signal.env
if sudo grep -Eq '^(DATABASE_URL|READ_DATABASE_URL)=' \
  /etc/price-collector/shadow-signal.env; then
  echo 'STOP: database credentials do not belong in shadow-signal.env.'
  false
fi
```

The primary model and replay-derived staleness/reference settings are not
environment overrides. The worker obtains them from the accepted artifacts so
they cannot drift from Phase 3.

### 5. Run the fail-closed activation preflight

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
assert activated.poll_ms == settings.SHADOW_SIGNAL_POLL_MS
print({
    "policy": activated.policy_version,
    "selection_sha256": activated.selection_artifact_sha256,
    "fingerprint": activated.selection_fingerprint_sha256,
    "evidence_end_ms": activated.evidence_end_ms,
    "primary": activated.primary_model.version,
    "candidate_models": [model.version for model in activated.models],
    "poll_ms": activated.poll_ms,
    "futures_stale_ms": activated.futures_stale_ms,
    "chainlink_stale_ms": activated.chainlink_stale_ms,
    "reference_max_gap_ms": activated.reference_max_gap_ms,
    "history_retention_ms": activated.history_retention_ms,
})
PY
BASH
```

Do not install or start the service if this command fails.

### 6. Install and start the isolated service

```bash
sudo cp \
  /opt/price-collector/deployment/price-collector-shadow-signal.service \
  /etc/systemd/system/price-collector-shadow-signal.service
sudo systemctl daemon-reload
sudo systemctl enable price-collector-shadow-signal
sudo systemctl restart price-collector-shadow-signal
sudo systemctl status price-collector-shadow-signal --no-pager
sudo journalctl -u price-collector-shadow-signal -n 100 --no-pager
```

No Redis restart is required. Startup is expected to pass through
`warming_up_futures_history` and `waiting_for_new_chainlink_anchor` before the
next fresh Chainlink event can produce a valid projection.

### 7. Verify publication and the frozen primary

Wait up to 30 seconds for a valid signal:

```bash
ACTIVE_SELECTION="$(sudo sed -n \
  's/^SHADOW_SIGNAL_SELECTION_PATH=//p' \
  /etc/price-collector/shadow-signal.env)"
SELECTION_SHA="$(sudo sed -n \
  's/^SHADOW_SIGNAL_SELECTION_SHA256=//p' \
  /etc/price-collector/shadow-signal.env)"
SHADOW_KEY="btc:live:chainlink_shadow"
for attempt in $(seq 1 60); do
  VALID="$(redis-cli --raw GET "$SHADOW_KEY" | python3 -c '
import json
import sys
raw = sys.stdin.read().strip()
print(int(bool(raw) and json.loads(raw)["valid"]))
')"
  if [ "$VALID" -eq 1 ]; then
    break
  fi
  sleep 0.5
done
test "${VALID:-0}" -eq 1

redis-cli --raw GET "$SHADOW_KEY" | python3 -c '
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

PTTL="$(redis-cli --raw PTTL "$SHADOW_KEY")"
printf 'shadow_pttl_ms=%s\n' "$PTTL"
test "$PTTL" -gt 0
test "$PTTL" -le 2000
```

At the historical Phase 4 checkpoint, the public API still had no shadow
field. A deployment that has since completed shadow-signal Phase 6 will instead
return the optional signal at `signals.chainlink_catchup`; that later API field
does not change this Phase 4 Redis-publication check:

```bash
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/markets/current/live
```

### 8. Prove worker-death expiry

This bounded test stops only the experimental worker. The three source-price
keys must remain present while the shadow key expires:

```bash
sudo systemctl stop price-collector-shadow-signal
sleep 3
test "$(redis-cli --raw EXISTS btc:live:chainlink_shadow)" -eq 0
test "$(redis-cli --raw EXISTS \
  btc:live:binance_spot \
  btc:live:chainlink \
  btc:live:futures)" -eq 3
redis-cli --raw MGET \
  btc:live:binance_spot \
  btc:live:chainlink \
  btc:live:futures
sudo systemctl start price-collector-shadow-signal
sudo systemctl status price-collector-shadow-signal --no-pager
```

### Roll back Phase 4 publication

Rollback does not touch either producer, Redis, PostgreSQL, or the evidence
files:

```bash
sudo systemctl disable --now price-collector-shadow-signal
sleep 3
redis-cli --raw EXISTS btc:live:chainlink_shadow
```

The final command must print `0`. Leave the immutable evidence and dedicated
environment in place for diagnosis or a later restart.

## Shadow-signal matured evaluations

This current subsystem adds the internal
`shadow_signal_evaluations` table and a bounded, nonblocking writer to the
existing standalone shadow worker. It does not add an API field or dashboard
code.

Keep `SHADOW_SIGNAL_EVALUATION_ENABLED=false` until `schema.sql` has been
applied, the writer URL has been installed without replacing trusted artifact
settings, and the exact table grants have been verified.

When upgrading an existing evaluation deployment to the cohort-wide outcome
contract, stop `price-collector-shadow-signal` before applying `schema.sql` and
restart it only after the schema succeeds. The migration adds non-null
`outcome_status` and `outcome_invalid_reasons` columns. It conservatively
quarantines pre-fix outcomes by labeling the preserved base rows
`legacy_unverified` with `pre_cohort_integrity_fix_unverified` and excluding
them from the reader view; this prevents an already-staged short horizon from
remaining scoreable after an unobserved later reset without a multi-day table
rewrite. The legacy labeling and filtered reader-view replacement commit in one
database transaction, so the API cannot observe the half-applied contract.

For otherwise outcome-eligible schema-v3 cohorts, finalization also requires a
successful sequenced Chainlink cache observation at or after the maximum
target. Missing or malformed Chainlink reads defer the cohort for at most two
poll intervals;
expiration of that bound emits the whole cohort as `integrity_invalid` with
reason `chainlink_sequence_confirmation_timeout`. The worker logs
`shadow_signal_evaluation_chainlink_sequence_confirmation_timeout` without raw
cache contents. This is a code-only behavior change: it requires no schema or
environment-file update, and schema-v2 maturation is unchanged.

Before schema-v3 sequence metadata is first established, the evaluator retains
no legacy Chainlink target history. It continues scheduling complete cadence
cohorts for coverage, but stores them as `integrity_invalid` with reason
`chainlink_sequence_not_established` and null actual/error fields. A later
sequenced observation cannot validate an earlier cohort, and consumed cadence
buckets are not backfilled. Schema v2 continues to allow legacy startup. This
startup quarantine uses the existing outcome columns and requires no database
migration.

Within a publisher epoch, an accepted Chainlink sequence is immutable across
its source timestamp, receive timestamp, and price. If the evaluator sees a
different identity for the same sequence, it clears outcome history, records
`chainlink_sequence_identity_mismatch`, and quarantines both disputed values.
Attempts continue on cadence but cannot score until a newer sequence or
publisher epoch establishes a clean baseline. The evaluator retains the last
sequence binding through a metadata-less read so recovery cannot redefine that
sequence. The worker logs
`shadow_signal_evaluation_chainlink_sequence_identity_mismatch` without either
price or raw cache contents. This uses the existing outcome columns and needs no
schema, environment, publisher, or API change.

If an already-enabled worker reports
`shadow_signal_evaluations_check17`, disable evaluations immediately, reapply
the current `schema.sql`, and verify the named projection constraints before
the writer is re-enabled. This preserves the Redis signal and existing
evaluation rows while replacing the divide-first projection constraint.

## Shadow-signal live API exposure

The current API extends the Redis-only `GET /markets/current/live` response with
`signals.chainlink_catchup`; it does not add a route, PostgreSQL query, API
writer credential, dashboard code, schema migration, environment setting, or
systemd dependency on the optional shadow worker.

Because `price_collector/live_cache.py` is shared, changes to it require an
ordered restart of the three source-price services, then
`price-collector-shadow-signal`, and finally `price-api`. They do not require a
restart of the Polymarket probability collector, Redis, or PostgreSQL unless a
separate change affects those components.

A well-formed signal is returned as an object whether `valid` is true or false.
An expired, absent, or malformed experimental signal is isolated as
`"chainlink_catchup": null` while the three source prices still return with
HTTP 200. A Redis read failure or malformed source-price payload retains the
existing HTTP 503 behavior. Phase 7 dashboard work remains outside this
repository and is not part of this rollout.

## Prospective two-second Chainlink challenger

The challenger remains a separate shadow worker with a dedicated read-only
dashboard endpoint. Its active V2 basis-band change does not modify the
accepted model, promoted evidence, or `btc:live:chainlink_shadow`. Each worker's
retention cleanup is scoped to its frozen model set, so the challenger cannot
delete accepted-model evidence.

The active `prospective_catchup_2s_basis_v2` experiment is fixed as model
`catchup_v2_l2000_h2000_b100_basis5m`: 2,000 ms lookback, 2,000 ms horizon,
and beta `1`. It writes only
`btc:live:chainlink_shadow_2s`, which expires after 2,000 ms, and is exposed at:

```text
GET /markets/current/live/challengers/chainlink-catchup-2s
```

This version defines basis as `futures - Chainlink`. It calculates the normal
basis from strictly prior 500 ms samples over five minutes and requires 600
samples. The soft-band half-width is
`max($1, 0.75 * population standard deviation)`. It leaves the raw two-second
projection unchanged inside the band and, outside it, moves the final projection
50% toward the nearest edge. Warmup or basis-processing failure falls back to
the raw lag projection. This remains an unselected prospective model; live
publication and matured evaluation are separately disabled by default.

When evaluation is enabled, one attempt is scheduled per entered 500 ms bucket
and causally matured at its 2,000 ms target. The worker records both the active
V2 output and a silent raw V1 comparator from the same forecast state and target;
only V2 is published to Redis. The existing bounded nonblocking writer stores
both model series in `shadow_signal_evaluations` for exactly 168 hours. Database
or cleanup failures cannot block the 100 ms Redis path. The existing reporting
and download routes accept `catchup_v2_l2000_h2000_b100_basis5m` with the same
response shape as `catchup_ratio_l3000_b100`, so the dashboard changes only
`model_version` for retained charts and metrics. Historical
`catchup_v1_l2000_h2000_b100` evidence remains separately reportable; do not
merge V1 and V2 rows.

The worker validates both the historical
`price_collector/shadow_signal_2s_registration.json` and the active
`price_collector/shadow_signal_2s_basis_registration.json` before opening Redis
or PostgreSQL. The V2 registration explicitly says the model is an unselected
prospective challenger and binds the frozen formula to
`price_collector/shadow_signal_2s_basis_calibration.json`, with cutoff
`evidence_end_ms=1784757680001` and reproducible artifact and configuration
hashes. The live wire shape remains schema version `1`; `projected_chainlink`
is the final value after any soft correction.

After the change is pushed to GitHub, deploy it from the droplet with:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal_2s_basis.py \
  tests/test_shadow_signal_2s_live.py \
  tests/test_shadow_signal_2s_collector.py \
  tests/test_shadow_signal_evaluation.py \
  tests/test_shadow_signal_evaluation_db.py \
  tests/test_shadow_signal_reporting.py \
  tests/test_api.py \
  tests/test_deployment.py \
  -q

sudo systemctl restart price-collector-shadow-signal-2s price-api
```

The order matters only in that code and dependencies must be installed before
the two affected services restart. This checkpoint changes no schema, systemd
unit, environment key, PostgreSQL/Redis service, producer, or accepted
three-second worker. The accepted model and selection files remain unchanged.

Verify the service, its short-lived key, both API contracts, and bounded logs:

```bash
sudo systemctl status \
  price-collector-shadow-signal-2s price-api \
  --no-pager
redis-cli -h 127.0.0.1 --raw PTTL btc:live:chainlink_shadow_2s
curl -fsS \
  http://127.0.0.1:9000/markets/current/live/challengers/chainlink-catchup-2s
curl -fsS \
  'http://127.0.0.1:9000/markets/current/shadow-evaluations?model_version=catchup_v2_l2000_h2000_b100_basis5m'
sudo -u postgres psql -X -d price_collector -c \
  "SELECT model_version, count(*) AS retained_rows,
          min(to_timestamp(generated_ms / 1000.0)) AS oldest,
          max(to_timestamp(generated_ms / 1000.0)) AS newest
   FROM shadow_signal_evaluations
   WHERE model_version IN (
     'catchup_v1_l2000_h2000_b100',
     'catchup_v2_l2000_h2000_b100_basis5m'
   )
   GROUP BY model_version;"
sudo journalctl -u price-collector-shadow-signal-2s -n 100 --no-pager
sudo journalctl -u price-api -n 50 --no-pager
```

A running, healthy worker should keep the `PTTL` between `1` and `2000` ms.
When inputs are temporarily unusable, the endpoint remains HTTP 200 with a
well-formed invalid prediction; if the worker is stopped and the key expires,
the endpoint remains HTTP 200 with `"prediction": null`. Evaluation rows are
written at 500 ms cadence after their two-second targets mature; allow several
seconds after restart before treating an empty database check as suspicious.

To stop only the prospective experiment without affecting either producer or
the accepted shadow signal:

```bash
sudo systemctl disable --now price-collector-shadow-signal-2s
sleep 3
test "$(redis-cli -h 127.0.0.1 --raw EXISTS btc:live:chainlink_shadow_2s)" -eq 0
curl -fsS \
  http://127.0.0.1:9000/markets/current/live/challengers/chainlink-catchup-2s
```

## Shadow-Signal Evaluation Reporting API

This is the backend prerequisite for the separate Phase 7 dashboard. It adds
two PostgreSQL-backed, read-only reporting routes and two rounded JSON
attachment routes while leaving `GET /markets/current/live` Redis-only:

```text
GET /markets/current/shadow-evaluations?model_version=catchup_ratio_l3000_b100
GET /markets/{market_id}/shadow-evaluations?model_version=catchup_ratio_l3000_b100
GET /markets/current/shadow-evaluations/download?model_version=catchup_ratio_l3000_b100
GET /markets/{market_id}/shadow-evaluations/download?model_version=catchup_ratio_l3000_b100
```

Each chart point includes the persisted Chainlink and futures snapshots from
`generated_ms`, with their provider and local-receive timestamps, alongside
the projected and causal actual Chainlink values for `target_ms`. The
restricted view does not expose `futures_reference` or other internal/writer
metadata.

The attachment routes preserve the schema-v2 report structure, round its
Decimal strings only after exact validation and metric calculation, add a
versioned `export` object, and use deterministic `_rounded.json`
`Content-Disposition` filenames. USD values use two decimal places; basis
points, beta, skills, and rates use four. Evaluation retention defaults to
seven days. Downloads cannot include the dashboard's browser-buffered target-time
`actual_futures`; only the persisted `futures_at_forecast` snapshot at
`generated_ms` is available through this reader path.

For an initial reporting deployment, apply `schema.sql` before restarting
`price-api`; the API queries the restricted view and will fail until that view
exists. Adding only the attachment routes to an installation where the
reporting endpoints already work requires no schema application. No environment
or systemd change is required.

After the change is pushed to GitHub, deploy it with:

Run these commands one at a time in the existing SSH session. Do not enable
shell `errexit` for this interactive update; if one command fails, inspect it
without terminating the session, then continue only after it succeeds.

```bash
cd /opt/price-collector

sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest \
  tests/test_shadow_signal_reporting.py \
  tests/test_shadow_signal_evaluation_db.py \
  tests/test_api.py \
  -q

sudo -u postgres psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -d price_collector \
  -f /opt/price-collector/schema.sql

sudo systemctl restart price-api
sudo systemctl status price-api --no-pager
```

Verify that the reader can select only the chart view:

```bash
sudo -u postgres psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -P pager=off \
  -d price_collector <<'SQL'
SELECT
    to_regclass(
        'public.shadow_signal_evaluation_chart_points'
    ) AS reporting_view,
    has_table_privilege(
        'price_reader',
        'public.shadow_signal_evaluation_chart_points',
        'SELECT'
    ) AS reader_can_select_view,
    has_table_privilege(
        'price_reader',
        'public.shadow_signal_evaluations',
        'SELECT'
    ) AS reader_can_select_base,
    has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluation_chart_points',
        'SELECT'
    ) AS writer_can_select_view;

SELECT
    grantee,
    table_name,
    privilege_type
FROM information_schema.table_privileges
WHERE table_schema = 'public'
  AND table_name IN (
      'shadow_signal_evaluations',
      'shadow_signal_evaluation_chart_points'
  )
ORDER BY table_name, grantee, privilege_type;

SELECT
    ordinal_position,
    column_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'shadow_signal_evaluation_chart_points'
  AND column_name IN (
      'selection_schema_version',
      'selection_policy_version',
      'selection_evidence_end_ms',
      'chainlink_at_forecast',
      'chainlink_at_forecast_source_timestamp_ms',
      'chainlink_at_forecast_received_ms',
      'futures_at_forecast',
      'futures_at_forecast_source_timestamp_ms',
      'futures_at_forecast_received_ms',
      'outcome_status',
      'outcome_invalid_reasons'
  )
ORDER BY ordinal_position;

DO $$
BEGIN
    IF to_regclass(
        'public.shadow_signal_evaluation_chart_points'
    ) IS NULL THEN
        RAISE EXCEPTION 'reporting view is missing';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM (
            VALUES
                ('selection_schema_version'),
                ('selection_policy_version'),
                ('selection_evidence_end_ms'),
                ('chainlink_at_forecast'),
                ('chainlink_at_forecast_source_timestamp_ms'),
                ('chainlink_at_forecast_received_ms'),
                ('futures_at_forecast'),
                ('futures_at_forecast_source_timestamp_ms'),
                ('futures_at_forecast_received_ms'),
                ('outcome_status'),
                ('outcome_invalid_reasons')
        ) AS required(column_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns AS present
            WHERE present.table_schema = 'public'
              AND present.table_name =
                    'shadow_signal_evaluation_chart_points'
              AND present.column_name = required.column_name
        )
    ) THEN
        RAISE EXCEPTION 'required reporting view columns are missing';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'shadow_signal_evaluation_chart_points'
          AND (
              column_name LIKE 'futures_reference%'
              OR column_name IN (
                  'futures_now',
                  'futures_now_source_timestamp_ms',
                  'futures_now_received_ms',
                  'futures_received_age_ms',
                  'chainlink_received_age_ms',
                  'created_at'
              )
          )
    ) THEN
        RAISE EXCEPTION 'reporting view exposes forbidden internal columns';
    END IF;
    IF NOT has_table_privilege(
        'price_reader',
        'public.shadow_signal_evaluation_chart_points',
        'SELECT'
    ) THEN
        RAISE EXCEPTION 'price_reader cannot select reporting view';
    END IF;
    IF has_table_privilege(
        'price_reader',
        'public.shadow_signal_evaluations',
        'SELECT'
    ) THEN
        RAISE EXCEPTION 'price_reader can select internal evaluation table';
    END IF;
    IF has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluation_chart_points',
        'SELECT'
    ) THEN
        RAISE EXCEPTION 'price_writer can select reporting view';
    END IF;
    IF NOT has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'SELECT'
    ) OR NOT has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'INSERT'
    ) OR NOT has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'DELETE'
    ) OR has_table_privilege(
        'price_writer',
        'public.shadow_signal_evaluations',
        'UPDATE'
    ) THEN
        RAISE EXCEPTION 'price_writer base-table privileges are incorrect';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_privileges
        WHERE table_schema = 'public'
          AND table_name IN (
              'shadow_signal_evaluations',
              'shadow_signal_evaluation_chart_points'
          )
          AND grantee = 'PUBLIC'
    ) THEN
        RAISE EXCEPTION 'PUBLIC has a shadow-evaluation relation privilege';
    END IF;
END
$$;
SQL
```

Expected values are `reporting_view` non-null,
`reader_can_select_view = true`, `reader_can_select_base = false`, and
`writer_can_select_view = false`. `price_writer` retains only `SELECT`,
`INSERT`, and `DELETE` on the base table; `PUBLIC` has neither relation. The
five selection-provenance/outcome-integrity columns and all six approved
forecast-time snapshot columns must be listed for the view. The internal
`futures_now` names, all `futures_reference*` fields, worker-age fields, and
`created_at` must remain absent.

Verify both endpoints and the unchanged live route:

```bash
API_BASE_URL="http://127.0.0.1:9000"
MODEL_VERSION="catchup_ratio_l3000_b100"

summarize_shadow_report() {
  REPORT_MODEL_VERSION="${MODEL_VERSION}" python3 -c '
import json
import os
import sys

report = json.load(sys.stdin)
points = report["points"]
market = report["market"]
model = report["model"]
coverage = report["coverage"]
performance_cohorts = report["performance"]["cohorts"]
expected_model = os.environ["REPORT_MODEL_VERSION"]
financial_fields = (
    "beta",
    "chainlink_at_forecast",
    "futures_at_forecast",
    "projected_chainlink",
    "actual_chainlink",
    "pending_move",
    "pending_move_bps",
    "forecast_error",
    "baseline_error",
)
identity_fields = {
    "schema_version",
    "policy_version",
    "evidence_end_ms",
    "fingerprint_sha256",
    "artifact_sha256",
}
assert report["schema_version"] == 2
assert report["evaluation_semantics"] == {
    "scored_input_max_future_skew_ms": 0,
}
assert len(points) <= 1000
assert model["model_version"] == expected_model
assert all(point["model_version"] == model["model_version"] for point in points)
assert all(
    market["market_start_ms"] <= point["target_ms"] < market["market_end_ms"]
    for point in points
)
assert [
    (point["target_ms"], point["generated_ms"], point["horizon_ms"])
    for point in points
] == sorted(
    (point["target_ms"], point["generated_ms"], point["horizon_ms"])
    for point in points
)
assert isinstance(model["beta"], str)
assert all(
    point[field] is None or isinstance(point[field], str)
    for point in points
    for field in financial_fields
)
forecast_snapshots = (
    "chainlink_at_forecast",
    "futures_at_forecast",
)
for point in points:
    for value_field in forecast_snapshots:
        source_field = f"{value_field}_source_timestamp_ms"
        received_field = f"{value_field}_received_ms"
        value = point[value_field]
        source_ms = point[source_field]
        received_ms = point[received_field]
        if value is None:
            assert source_ms is None
            assert received_ms is None
        else:
            assert source_ms is None or (
                type(source_ms) is int and source_ms >= 0
            )
            assert type(received_ms) is int
            assert received_ms >= 0
            if point["valid"]:
                assert received_ms <= point["generated_ms"]
forbidden_point_fields = {
    "futures_now",
    "futures_now_source_timestamp_ms",
    "futures_now_received_ms",
    "futures_reference",
    "futures_reference_source_timestamp_ms",
    "futures_reference_received_ms",
    "futures_reference_target_ms",
    "futures_reference_gap_ms",
    "futures_received_age_ms",
    "chainlink_received_age_ms",
    "created_at",
}
assert all(forbidden_point_fields.isdisjoint(point) for point in points)
assert all(
    set(identity) == identity_fields
    for identity in model["selection_identities"]
)
for point in points:
    assert isinstance(point["selection_schema_version"], int)
    assert point["selection_schema_version"] > 0
    assert isinstance(point["selection_policy_version"], str)
    assert point["selection_policy_version"]
    assert 0 <= point["selection_evidence_end_ms"] <= point["generated_ms"]
    outcome_status = point["outcome_status"]
    outcome_reasons = point["outcome_invalid_reasons"]
    assert outcome_status in {"available", "unavailable", "integrity_invalid"}
    if outcome_status == "available":
        assert point["actual_chainlink"] is not None
        assert outcome_reasons == []
    else:
        assert point["actual_chainlink"] is None
        assert point["actual_chainlink_source_timestamp_ms"] is None
        assert point["actual_chainlink_received_ms"] is None
        assert point["actual_chainlink_age_at_target_ms"] is None
        assert point["forecast_error"] is None
        assert point["baseline_error"] is None
        assert bool(outcome_reasons) == (outcome_status == "integrity_invalid")
assert coverage["scored"] == sum(
    point["valid"] and point["outcome_status"] == "available"
    for point in points
)
assert [
    cohort["selection_identity"] for cohort in performance_cohorts
] == model["selection_identities"]
assert sum(
    cohort["scored_points"] for cohort in performance_cohorts
) == coverage["scored"]
for cohort in performance_cohorts:
    comparison = cohort["paired_comparison"]
    assert (
        comparison["wins"] + comparison["ties"] + comparison["losses"]
        == cohort["scored_points"]
    )
    derived_values = (
        *cohort["forecast"].values(),
        *cohort["no_change_baseline"].values(),
        cohort["mean_absolute_advantage_usd"],
        cohort["mae_skill_vs_no_change"],
        cohort["rmse_skill_vs_no_change"],
        comparison["win_rate"],
        comparison["tie_rate"],
        comparison["loss_rate"],
    )
    assert all(value is None or isinstance(value, str) for value in derived_values)
print({
    "market_id": market["market_id"],
    "model_version": model["model_version"],
    "points": len(points),
    "coverage": coverage,
    "performance": performance_cohorts,
})
'
}

MARKET_ID="$(
  curl -fsS \
    "${API_BASE_URL}/markets?limit=1&include_current=false" |
  python3 -c '
import json
import sys
print(json.load(sys.stdin)["markets"][0]["market_id"])
'
)"

curl -fsS "${API_BASE_URL}/healthz"
curl -fsS \
  "${API_BASE_URL}/markets/current/shadow-evaluations?model_version=${MODEL_VERSION}" |
  summarize_shadow_report
curl -fsS \
  "${API_BASE_URL}/markets/${MARKET_ID}/shadow-evaluations?model_version=${MODEL_VERSION}" |
  summarize_shadow_report

DOWNLOAD_HEADERS="$(mktemp)"
DOWNLOAD_BODY="$(mktemp)"
curl -fsS \
  -D "${DOWNLOAD_HEADERS}" \
  -o "${DOWNLOAD_BODY}" \
  "${API_BASE_URL}/markets/${MARKET_ID}/shadow-evaluations/download?model_version=${MODEL_VERSION}"
grep -i '^content-disposition: attachment;' "${DOWNLOAD_HEADERS}"
grep -i '_rounded\.json"' "${DOWNLOAD_HEADERS}"
summarize_shadow_report < "${DOWNLOAD_BODY}"
python3 - "${DOWNLOAD_BODY}" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    payload = json.load(stream)

export = payload["export"]
assert export == {
    "schema_version": 1,
    "variant": "rounded_download",
    "source_report_schema_version": 2,
    "decimal_encoding": "fixed_point_string",
    "rounding_mode": "ROUND_HALF_UP",
    "precision_policy": "shadow_evaluation_download_v1",
    "decimal_places": {
        "usd_price_move_error": 2,
        "basis_points": 4,
        "unitless_beta_rate_skill": 4,
    },
    "derived_metrics_computed_before_rounding": True,
    "classifications_computed_before_rounding": True,
}
two_places = re.compile(r"^-?\d+\.\d{2}$")
four_places = re.compile(r"^-?\d+\.\d{4}$")

def fixed(value, pattern):
    assert value is None or pattern.fullmatch(value)
    assert value not in {"-0.00", "-0.0000"}

point_usd_fields = (
    "chainlink_at_forecast",
    "futures_at_forecast",
    "projected_chainlink",
    "actual_chainlink",
    "pending_move",
    "forecast_error",
    "baseline_error",
)
for point in payload["points"]:
    fixed(point["beta"], four_places)
    fixed(point["pending_move_bps"], four_places)
    for field in point_usd_fields:
        fixed(point[field], two_places)

fixed(payload["model"]["beta"], four_places)
for cohort in payload["performance"]["cohorts"]:
    for value in cohort["forecast"].values():
        fixed(value, two_places)
    for value in cohort["no_change_baseline"].values():
        fixed(value, two_places)
    fixed(cohort["mean_absolute_advantage_usd"], two_places)
    fixed(cohort["mae_skill_vs_no_change"], four_places)
    fixed(cohort["rmse_skill_vs_no_change"], four_places)
    comparison = cohort["paired_comparison"]
    fixed(comparison["win_rate"], four_places)
    fixed(comparison["tie_rate"], four_places)
    fixed(comparison["loss_rate"], four_places)

print({"rounded_download": "ok", "points": len(payload["points"])})
PY
rm -f -- "${DOWNLOAD_HEADERS}" "${DOWNLOAD_BODY}"

curl -fsS "${API_BASE_URL}/markets/current/live" | python3 -m json.tool

MISSING_MODEL_STATUS="$(
  curl -sS -o /dev/null -w '%{http_code}' \
    "${API_BASE_URL}/markets/${MARKET_ID}/shadow-evaluations"
)"
test "${MISSING_MODEL_STATUS}" = "422"
printf 'missing_model_status=%s\n' "${MISSING_MODEL_STATUS}"

sudo journalctl \
  -u price-api \
  --since '10 minutes ago' \
  -n 200 \
  --no-pager
```

All reporting and attachment responses must contain at most 1,000 ordered
points for only the requested model. Every `target_ms` must satisfy the returned
half-open market boundary, report schema v2 must declare zero-skew scored-input
semantics, and every point or derived financial value must be a JSON string or
`null`. Canonical reporting responses retain full precision. Attachments must
declare rounded export policy v1 and use the fixed two-/four-place scales above;
do not recompute exact identities from rounded values. Each
present forecast-time snapshot must have local receive metadata; for valid
forecasts it must be no later than `generated_ms`, while invalid rows preserve
future-skew evidence. The narrow public point must not contain
anchor/reference or worker/writer metadata. Available, unavailable, and
integrity-invalid outcomes must satisfy their distinct value/reason contracts.
Performance cohorts must match the full response selection identities, their
scored counts must sum to `coverage.scored`, and each paired comparison must
account for every scored point. The missing-model request must return HTTP
`422`. A known market with expired or no evaluation evidence returns HTTP
`200` with `points: []`; an unknown non-current market returns HTTP `404`.

To roll back, first push and deploy a revert that removes the API routes, then
restart `price-api`. Only after the old API is active, remove the no-longer-used
view:

```bash
sudo -u postgres psql \
  -X \
  -v ON_ERROR_STOP=1 \
  -d price_collector <<'SQL'
REVOKE ALL ON TABLE public.shadow_signal_evaluation_chart_points
FROM PUBLIC, price_reader, price_writer;
DROP VIEW public.shadow_signal_evaluation_chart_points;
SQL
```

Do not drop or alter `shadow_signal_evaluations`; rollback must preserve the
writer and its retained evidence.

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
