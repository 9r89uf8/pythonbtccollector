# Production operations

This runbook covers the current Python collectors on the single-user Ubuntu
24.04 droplet. Code and its virtual environment live at `/opt/price-collector`,
root-owned environment files at `/etc/price-collector`, and writable service
state at `/var/lib/price-collector`. Services run as
`pricecollector:pricecollector`. PostgreSQL database `price_collector` is the
historical record; Redis is a live cache only.

Keep PostgreSQL, Redis (`127.0.0.1:6379`, protected mode) and the read-only API
(`127.0.0.1:9000`) local. The API environment has only reader credentials;
collector writer credentials belong in `collector.env`. Access the API from
another machine through an SSH tunnel. Do not install research dependencies
in the production virtual environment or execute research code in services.

## Routine checks

```bash
sudo systemctl status price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api redis-server --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli -h 127.0.0.1 -p 6379 PING
df -h / /var/lib/postgresql
sudo ss -ltnp
sudo journalctl -u price-collector-polymarket-probabilities -n 100 --no-pager
```

Review receive ages as well as service status. An active socket or process
does not establish fresh accepted events. Ports 9000, 5432 and 6379 must not
listen on public interfaces. Current/live API checks must remain read-only.

## Deploy compact Polymarket evidence

Run these commands **after the change is pushed to GitHub**. This checkpoint
changes the probability service and adds schema objects. Pull and install
dependencies, then apply the schema **before** restarting that service:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-probabilities
sudo systemctl status price-collector-polymarket-probabilities --no-pager
sudo journalctl -u price-collector-polymarket-probabilities -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
```

During `sudoedit`, manually add/review the following keys and set
`POLYMARKET_EVIDENCE_ENABLED=true` to start capture. The code and example default
is `false`. Preserve existing credentials and other settings; never replace
the production file with an example. This update needs no unit copy,
`daemon-reload`, Redis restart or API environment change.

| Collector environment key | Example/default |
| --- | ---: |
| `POLYMARKET_EVIDENCE_ENABLED` | `false` |
| `POLYMARKET_EVIDENCE_POLL_SECONDS` | `5` |
| `POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS` | `100` |
| `POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS` | `120` |
| `POLYMARKET_EVIDENCE_QUEUE_MAX_RECORDS` | `5000` |
| `POLYMARKET_EVIDENCE_QUOTE_QUEUE_MAX_RECORDS` | `2000` |
| `POLYMARKET_EVIDENCE_BATCH_MAX_ROWS` | `250` |
| `POLYMARKET_EVIDENCE_FLUSH_MS` | `500` |
| `POLYMARKET_EVIDENCE_WARN_RELATION_MB` | `4096` |
| `POLYMARKET_EVIDENCE_MAX_RELATION_MB` | `6144` |

Do not enable `RAW_FUTURES_TRACE_ENABLED` or `RAW_CHAINLINK_EVENTS_ENABLED` for
this feature. It reuses the probability CLOB state without collecting depth or
quantities. HTTP metadata polling and bounded database writers run separately
from that stream. The normal probability history and official reconciliation
continue alongside the optional evidence path.

The four HTTP observation kinds are `price_to_beat`, `gamma_market`,
`clob_market` and `clob_order_rules`. The last reads identity-validated CLOB
`/markets/{condition_id}` rules, including `seconds_delay`; compact
`/clob-markets/{condition_id}` supplies the separate `itode` flag and fee curve.

## Verify prospective evidence

Run after capture has covered the final 120 seconds of an active market.
Missing/non-OK observations and session/gap rows are evidence, not fabricated
successful samples. This query shows whether Price to Beat was actually
received before close and joins the saved payload by its hash:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
SET statement_timeout = '15s';
SET TIME ZONE 'UTC';
SELECT o.market_id, o.status,
       p.payload->>'price_to_beat' AS price_to_beat,
       p.payload->>'api_timestamp_ms' AS api_cache_timestamp_ms,
       o.response_date, o.response_age_seconds,
       to_timestamp(o.received_wall_ns / 1000000000) AS received_at,
       w.market_end_ms - o.received_wall_ns / 1000000 AS ms_before_close
FROM polymarket_market_observations o
JOIN market_windows w USING (market_id)
LEFT JOIN polymarket_evidence_payloads p USING (payload_hash)
WHERE o.kind = 'price_to_beat'
  AND o.market_id >= floor(extract(epoch FROM now() - interval '1 day') / 300)::bigint
ORDER BY o.market_id DESC, o.received_wall_ns DESC
LIMIT 12;

SELECT kind, status, count(*) AS rows,
       max(received_wall_ns) AS latest_received_wall_ns
FROM polymarket_market_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '1 day') / 300)::bigint
GROUP BY kind, status ORDER BY kind, status;

SELECT market_id, count(*) AS quote_rows,
       min(observed_wall_ns) AS first_observed_wall_ns,
       max(observed_wall_ns) AS last_observed_wall_ns
FROM polymarket_quote_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '1 hour') / 300)::bigint
GROUP BY market_id ORDER BY market_id DESC LIMIT 12;
SQL
```

An `ok` opening reference needs a non-null price and positive
`ms_before_close` to demonstrate pre-close availability. Use its actual receipt
time for later decisions. The official website endpoint can report
`incomplete=true` while providing a valid opening price; Gamma can omit that
price until resolution. Response digest, Date and Age are stored in the
observation columns `response_sha256`, `response_date` and
`response_age_seconds`; the linked payload holds normalized source data.
API/cache timestamps, HTTP round-trip time and
provider timestamps are not measured order-to-fill latency. Keep CLOB `itode`
separate from `seconds_delay`; zero seconds alone does not negate an enabled
taker-delay flag. A 100 ms quote sample cannot reconstruct intermediate
events, demonstrate depth, or prove a chosen-size fill.

## Measure storage and losses

The evidence budget covers these three relations, including indexes and TOAST.
It does not include PostgreSQL WAL, other tables, backups or all disk use.
Measure actual sizes and UTC-day row counts; do not extrapolate a promised
retention period from a short WebSocket probe:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
SET statement_timeout = '15s';
SET TIME ZONE 'UTC';
WITH relations AS (
  SELECT unnest(ARRAY[
    'public.polymarket_evidence_payloads'::regclass,
    'public.polymarket_market_observations'::regclass,
    'public.polymarket_quote_observations'::regclass
  ]) AS rel
)
SELECT rel::text AS relation,
       pg_table_size(rel) AS table_and_toast_bytes,
       pg_indexes_size(rel) AS index_bytes,
       pg_total_relation_size(rel) AS total_bytes,
       sum(pg_total_relation_size(rel)) OVER () AS all_evidence_bytes
FROM relations ORDER BY relation;

SELECT (to_timestamp(observed_wall_ns / 1000000000) AT TIME ZONE 'UTC')::date AS utc_day,
       count(*) AS quote_rows, count(DISTINCT market_id) AS observed_markets
FROM polymarket_quote_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '3 days') / 300)::bigint
GROUP BY utc_day ORDER BY utc_day;

SELECT (to_timestamp(received_wall_ns / 1000000000) AT TIME ZONE 'UTC')::date AS utc_day,
       kind, count(*) AS observation_rows
FROM polymarket_market_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '3 days') / 300)::bigint
GROUP BY utc_day, kind ORDER BY utc_day, kind;
SQL
sudo journalctl -u price-collector-polymarket-probabilities --since '1 hour ago' -n 200 --no-pager
```

Repeat measurements over actual collection days and compare against available
disk space. Queue overflow or write failure must surface as gaps/logs. The
pending queues are in memory, so unclean session recovery marks possible loss
rather than reconstructing unwritten observations. The
default guard warns at 4096 MiB and pauses new quote capture at 6144 MiB;
metadata/control observations continue and accepted writes may drain, so this
is not a strict maximum size. There is no automatic evidence deletion. Choose
an explicit export/retention procedure before the budget is exhausted.

To disable optional capture, manually set `POLYMARKET_EVIDENCE_ENABLED=false`
in `/etc/price-collector/collector.env`, restart only
`price-collector-polymarket-probabilities`, and repeat its status, bounded log
and local health checks above. Preserve the collected tables for review.

## Other runtime changes

For other checkpoints, use the same Git fast-forward, dependency install and
schema-before-restart ordering, then restart every actually affected unit from
the service map in `AGENTS.md`. Copy a changed systemd unit to
`/etc/systemd/system` and run `sudo systemctl daemon-reload` before restarting
it. Never overwrite production environment files with examples.

Historical raw-capture Phase 4 partition/retention validation remains deferred:
future partition creation, expiry, 72-hour retention and sustained raw-table
budget enforcement are not proven by short canaries. This runbook does not
restore retired research procedures or declare that validation complete.
