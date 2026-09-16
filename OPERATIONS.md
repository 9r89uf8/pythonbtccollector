# Production operations

This runbook covers the current Python collectors on the single-user Ubuntu
24.04 droplet. Code and its virtual environment live at `/opt/price-collector`,
root-owned environment files at `/etc/price-collector`, and writable service
state at `/var/lib/price-collector`. Services run as
`pricecollector:pricecollector`, except the bounded retention oneshot, which uses
local `postgres:postgres` peer authentication. PostgreSQL database `price_collector` is the
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

## Ten-day collector history retention

`price-collector-retention.timer` schedules the independent
`price-collector-retention.service` oneshot one minute after boot and one minute
after its previous run finishes. Each run has a 45-second work budget and a
55-second systemd deadline, short database transactions and bounded deletion
batches. The timer does not overlap itself. It runs as `postgres:postgres`,
connects to database `price_collector` through `/var/run/postgresql` using local
peer authentication, and receives no environment file or password.

The policy covers non-ghost collector history: prices, probability samples,
futures/flow/book/open-interest history, microstructure, durable TWAP evidence,
and compact Polymarket observations/payloads. Cleanup uses a fixed allowlist;
supported retired database tables are handled only if present. It does not
restore any retired pipeline. Whole market groups expire at the ten-day cutoff
rounded **up** to a five-minute start boundary, so some rows may expire up to
five minutes early. Children are removed before parents; metadata, sessions and
shared payloads remain while retained/current rows need them. Backfill and due
reconciliation use the same floor to avoid recreating expired markets.
Expired historical 30-second TWAP events may be deleted under this policy.
Every surviving row keeps its original 30- or 60-second topic, instrument,
window and settlement-rule identity; age cleanup never relabels old evidence.

All ghost tables are excluded. Seven-day continuous ghost records, ninety-day
accuracy summaries and legacy ghost export safeguards are unchanged. Existing
raw capture retains its stricter 72-hour policy while enabled. The timer also
enforces the ten-day ceiling on old raw history when capture is disabled, without
creating partitions or enabling a feed. Research/results files outside the
collector database are untouched. The API remains read-only and live Redis
values are not deleted or rewritten.

Run the following **after the change is pushed to GitHub**. Apply the schema and
retention indexes before enabling the timer. Shared `config.py`/`db.py` changes
require reloading all four collectors and the API; the probability backfill and
microstructure retention behavior change in this checkpoint:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/deployment/collector-retention-indexes.sql
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo cp /opt/price-collector/deployment/price-collector-retention.service /etc/systemd/system/price-collector-retention.service
sudo cp /opt/price-collector/deployment/price-collector-retention.timer /etc/systemd/system/price-collector-retention.timer
sudo systemctl daemon-reload
sudo systemctl restart price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
sudo systemctl enable --now price-collector-retention.timer
sudo systemctl start price-collector-retention.service
sudo systemctl status price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api price-collector-retention.timer --no-pager
sudo systemctl list-timers price-collector-retention.timer --all --no-pager
sudo journalctl -u price-collector-retention.service -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
```

The index prebuild is for an existing installation and runs outside a transaction;
it uses concurrent builds for the populated evidence tables. Do not add
`--single-transaction` to that command. If a build fails, stop the rollout and
inspect/repair its index validity before retrying or enabling the timer;
`IF NOT EXISTS` alone does not repair an invalid concurrent-build artifact.

In `collector.env`, manually review `BINANCE_MICROSTRUCTURE_RETENTION_DAYS=10`;
the runtime also caps older values such as 30 at ten days. Collector-local daily
cleanup runs only for a setting shorter than ten days; ten days and older longer
settings delegate to the independent bounded timer. Preserve shorter
settings, existing raw-retention settings and every credential. Never replace
the environment with its example, enable optional captures, or change ghost
flags as part of this installation. No new environment keys are required by
the maintenance service, and Redis does not need a restart.

Initial catch-up can take several passes because each pass is bounded. Read the
retention journal for deleted counts, remaining eligible work and timeouts rather
than treating timer activation as proof that all old data has gone. Diagnose
repeated errors before changing batch limits. Check filesystem and relation
sizes separately: ordinary autovacuum/VACUUM makes deleted space reusable, but
does not promise immediate filesystem shrinkage. Routine cleanup does not run
`VACUUM FULL`, rebuild tables or increase any storage budget.

To inspect eligibility without deleting rows:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.retention
```

To stop cleanup temporarily without stopping collection:

```bash
sudo systemctl stop price-collector-retention.timer
sudo systemctl stop price-collector-retention.service
```

Restarting/enabling the timer resumes the same policy; it does not restore
deleted history. Ongoing cleanup requires the timer to remain enabled.

## Continuous ghost retention and accuracy

This opt-in mode is implemented in the existing Chainlink service. It is disabled
by default and does not change or reset the historical one-hour canaries below.
Both `GHOST_TWAP_ENABLED=true` and `GHOST_TWAP_CONTINUOUS=true` are needed for
continuous production. `GHOST_TWAP_CANARY_START_MS=0` remains valid and is ignored
in this mode. Use `/var/lib/price-collector/ghost-continuous`, separate from every
old canary directory. Its first-start clock is recorded automatically; subsequent
restarts reuse that state and reconcile its outbox before new admission.

**Current deployment:** continuous forecasting started on September 16, 2026 at
15:59:27 UTC on commit `6c6f5bf`, run
`4f55736e031f4184be501c538f7a61d2`. Initial checks found `capacity_ok=true`, no
stop or suspensions, successful compaction and a working accuracy cache. The
[canonical reference](GHOST_TWAP_REFERENCE.md#storage-safeguards-and-maintenance) includes the
shared-capacity calculation and its limits. Seven-day operation and baseline
formation are still ongoing.

The current collector environment overrides are:

| Key | Deployed value |
| --- | --- |
| `GHOST_TWAP_ENABLED` | `true` |
| `GHOST_TWAP_CONTINUOUS` | `true` |
| `GHOST_TWAP_CANARY_START_MS` | `0` |
| `GHOST_TWAP_STATE_DIRECTORY` | `/var/lib/price-collector/ghost-continuous` |
| `GHOST_TWAP_DATABASE_FILESYSTEM_PATH` | `/var/lib/postgresql` |
| `GHOST_TWAP_SOURCE_MAX_AGE_MS` | `5000` |
| `GHOST_TWAP_RECEIPT_MAX_AGE_MS` | `3000` |
| `POLYMARKET_EVIDENCE_WARN_RELATION_MB` | `3072` |
| `POLYMARKET_EVIDENCE_MAX_RELATION_MB` | `4096` |
| `BINANCE_MICROSTRUCTURE_WARN_RELATION_MB` | `3072` |
| `BINANCE_MICROSTRUCTURE_MAX_RELATION_MB` | `4096` |

This activation changed environment settings only. It restarted
`price-collector-binance-futures`, `price-collector-polymarket-probabilities`
and `price-collector-polymarket-chainlink` to load their respective changes.
It did not install new code or apply a schema migration. Keep these current
overrides when reviewing future upgrades; the example defaults are not the
deployed configuration.

For an initial continuous-mode code/schema installation, after the change is
pushed to GitHub, install it with the producer disabled. Later code-only chart
updates preserve the running mode and existing state; use the tailored
[code-only update commands](#ghost-api-and-code-only-updates).
Apply the schema transaction before restarting either affected service:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-chainlink price-api
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -u price-api -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/accuracy
```

During installation keep `GHOST_TWAP_ENABLED=false` and
`GHOST_TWAP_CONTINUOUS=false`. Add only the missing key to the existing collector
environment; never replace the environment file with the example. Keep writer
credentials out of `api.env`. The existing `GHOST_TWAP_API_ENABLED` flag governs
the read-only accuracy route, and does not enable production. An absent or stale
accuracy cache while continuous monitoring is off is an unavailable response,
not a requirement to start forecasts.

Before explicitly enabling continuous mode, review current database-filesystem
free space and the other collectors' remaining relation budgets. Create the
new state directory once with owner `pricecollector:pricecollector` and mode
0700; refuse an unexpected existing directory instead of overwriting it. Then
manually set both enable flags true and the continuous state path, preserving
the 5,000 ms source-age and 3,000 ms receipt-age bounds. Restart only
`price-collector-polymarket-chainlink` for that collector-only environment change.
Never change paths or remove a stop marker to bypass a capacity or integrity
guard. A restart reuses the original continuous state directory.
The existing activated directory must not be recreated or replaced.

The continuous limits are 6 GiB total allocated ghost relation budget, warning
at 5 GiB, admission pause at 5.5 GiB, 1,500,000 retained decisions, and at least
10 GiB free on the actual PostgreSQL filesystem. Table, index and TOAST allocation
all count. This is shared-disk protection, not a guarantee that seven days always
fit. Normal expiry/vacuum may leave allocated space reusable without shrinking
the relation; do not infer free capacity from deleted row counts alone.
At activation, the two optional evidence/microstructure caps were each reduced
to 4 GiB, with 3 GiB warnings. After their remaining growth allowances, the full
6 GiB total ghost allowance and the 10 GiB filesystem reserve, the recorded
conditional balance was 894,377,984 bytes. The measured-rate seven-day new-compact
projection instead left 2,312,806,400 bytes. Neither balance reserves future core
history, WAL, logs, outbox or maintenance space; use fresh measurements before
changing any allowance.

The independent monitor compacts at most 100 eligible continuous decisions per
maintenance cycle, about every five seconds. Terminal status and the 120-second
matching window must both be complete. An atomic compact/hourly-summary commit
precedes removal of verbose inputs; failed work is retried. Compact individual
results expire after seven days, and hourly accuracy/feed-health summaries after
90 days. These operations do not relax the legacy canary's verified-export rule.
Do not expect full slot replay from compact hashes after verbose evidence expires.
The monitor keeps running while new forecasts are paused for capacity. Disabling
the entire worker stops its maintenance too; retained data remains bounded by
admission guards, and maintenance resumes only when that worker is started again.

Accuracy JSON refreshes once per minute with a 180-second Redis TTL. The API
does one cached GET, with no historical query or aggregation. Inspect its
`worker_health`, `runtime_health`, per-group `monitor` states and explicit panel
denominators rather than treating a cached 200 response as fresh feed evidence:

```bash
redis-cli -h 127.0.0.1 -p 6379 TTL btc:live:ghost_chainlink_twap_60s:accuracy
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/accuracy
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
df -h /var/lib/postgresql
```

Completed panels use 1-hour, 24-hour and 7-day windows, with persistence/matching
watermarks. Frozen per-policy baselines use the first available qualifying three
full UTC days in the bounded snapshot, with
at least 3,000 scored pairs and adequate publication/scoring coverage. Current
comparisons require 24 qualifying hours and at least 1,000 pairs after the
baseline. Deterioration requires MAE above 125% of baseline and paired excess
above baseline plus 0.01 bp for three consecutive hourly checks; recovery requires
MAE at most 110% or excess at most baseline plus 0.005 bp for three checks.
Conflicts and inadequate coverage block accuracy classification; missing targets
remain explicit counts rather than fabricated pairs.
These are explicit engineering thresholds. Baselines and warning counters survive
restarts. Receipt-gap durations belong to the hour containing their ending
receipt; incomplete coverage and dropped health hours remain visible. Feed-health
summaries describe accepted observations, not all upstream messages or browser
delivery. No monitor failure may stop the official Chainlink feeds.

The chart dashboard runs in the separate local `ghost-frontend` project and
reaches the API through an SSH tunnel. It is outside the backend checkout and
is not installed on the droplet. A browser or tunnel disconnect does not stop
the server-side continuous worker; cached HTTP/SSE delivery remains subject to
the freshness and expiry contract.

## Ghost API and code-only updates

Current behavior, research provenance and completed canary results are in the
[Ghost TWAP reference](GHOST_TWAP_REFERENCE.md). Continuous production and the
read-only API are enabled on the deployed droplet; do not follow superseded
canary installation steps that disable/reset the current run.

For a reviewed code-only change affecting the Chainlink ghost worker and API,
run after the change is pushed to GitHub. Preserve production environment files,
the continuous state directory and all retention settings:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink price-api
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/accuracy
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/comparison
sudo journalctl -u price-collector-polymarket-chainlink -u price-api -n 100 --no-pager
```

Restart only `price-api` for an API-only change, or only the Chainlink collector
for a collector-only change. If schema changes, apply the schema transaction
before either affected restart, using the installation sequence above. If a
systemd unit changes, install that exact unit and run `daemon-reload` first.
Documentation/local frontend changes alone do not require service restarts.
Forecast warm-up and the monitor's first refresh may temporarily return typed
503 responses; absence does not authorize resetting state or extending expiry.

The API flag `GHOST_TWAP_API_ENABLED=true` belongs in `/etc/price-collector/api.env`
with reader credentials only. It does not enable the producer. Default bounds
are 16 SSE clients, 250 ms Redis snapshot reads and 2,000 ms ASGI send waits.
Optional `GHOST_TWAP_API_MAX_CLIENTS`, `GHOST_TWAP_API_READ_TIMEOUT_MS` and
`GHOST_TWAP_API_SEND_TIMEOUT_MS` can adjust within the existing validated limits.
Malformed optional settings disable ghost routes with `invalid_settings`, not
ordinary source routes. Never expose port 9000 publicly.

On the owner's computer, the existing launcher manages the local proxy and
loopback tunnel. A manual equivalent for the tunnel is:

```powershell
ssh -N -L 127.0.0.1:19000:127.0.0.1:9000 root@152.42.247.86
```

Subscribe to `event: ghost`; producer state is in the nested `ghost` object and
delivery state in `api`. A null ghost is unavailable. Resync, generation changes
and skipped updates replace local current state; event IDs provide no historical
replay. Expire browser values locally even when the tunnel stops delivering.
Never restart a full `remaining_ns` lifetime upon browser receipt. The saved
comparison route uses finalized compact records and can recover its chart after
sleep; its cohort and lag differ from the live current-state stream.

A bounded stream check is:

```bash
curl -N --max-time 12 http://127.0.0.1:9000/forecasts/chainlink-twap/stream
```

The timeout intentionally ends this diagnostic. API disablement is independent:
set only its flag false and restart `price-api`. To stop production, set the
collector's `GHOST_TWAP_ENABLED=false` and restart the Chainlink collector;
editing the file alone does not stop an already running worker. Review its
shutdown completion and retained outbox rather than assuming a restart drained
all audit evidence. Do not re-enable or reset state merely to resolve a tail.

For the dashboard-history/Price-to-Beat reader addition, deployment is API-only:
no schema, environment, collector restart or forecast restart is required. After
pushing to GitHub, run:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-api
sudo systemctl status price-api --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/dashboard
sudo journalctl -u price-api -n 60 --no-pager
```

The local frontend proxy must also be restarted to load the new allowlisted
route. Missing official opening evidence remains unavailable; never fill it
with a sampled spot/TWAP approximation. That route's bounded historical reads
are independent of live ghost publication.

## Legacy ghost canary evidence

The completed one-hour campaigns use their original terminal/export/age rules;
continuous seven-day compaction does not silently apply to those records.
Past activation sequences are historical evidence, not a current operating
procedure. Do not create a new campaign or change state/start to bypass a guard.
The canonical reference records the Git revision containing the exact original
protocols, result manifests and clock/cohort definitions.

Read current audit state without changing it:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin status
sudo journalctl -u price-collector-polymarket-chainlink --since '10 minutes ago' -n 100 --no-pager
df -h /var/lib/postgresql /var/lib/price-collector
```

For an explicitly requested external export, run on the owner's computer with
a fresh destination filename:

```powershell
python -m price_collector.ghost_twap_admin download --ssh root@152.42.247.86 --output ghost-canary.jsonl
```

The command refuses overwrite, verifies every row and whole-file hash, then
acknowledges only unchanged current versions. Interrupted downloads remain
`.part` files. A same-droplet copy is not external verification. Preserve the
adjacent manifest and original outbox until recovery/export is confirmed.
Only terminal rows at least 96 hours old with matching verified export hashes
are eligible for the explicit bounded legacy expiry command:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin expire --limit 100
```

A later result change invalidates export eligibility. The archive adapter has
no configured always-on external transport; it is not a prerequisite for the
owner's seven-day continuous retention design. Production runtime/admin/observer
modules and their tests remain supported even though dated reports are removed.
Ordinary deletion/vacuum can leave allocated bytes reusable instead of shrinking
files. Do not add `VACUUM FULL`, rewrite relations or raise caps as routine cleanup.

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

These are example/code defaults. The deployed September 16 overrides are
`POLYMARKET_EVIDENCE_WARN_RELATION_MB=3072` and
`POLYMARKET_EVIDENCE_MAX_RELATION_MB=4096`; preserve them unless the shared
capacity allocation is deliberately revised. Microstructure uses the same
3072/4096 MiB warning/cap pair. See the
[current activation record](GHOST_TWAP_REFERENCE.md#storage-safeguards-and-maintenance).

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
default guard warns at 4096 MiB and pauses new quote capture at 6144 MiB; the
current production overrides warn at 3072 MiB and pause at 4096 MiB.
Metadata/control observations continue and accepted writes may drain, so this
is not a strict maximum size. The separate ten-day history-retention timer
removes expired evidence; the size guard itself does not delete it. Export any
evidence needed beyond that retention window before it expires.

To disable optional capture, manually set `POLYMARKET_EVIDENCE_ENABLED=false`
in `/etc/price-collector/collector.env`, restart only
`price-collector-polymarket-probabilities`, and repeat its status, bounded log
and local health checks above. Existing data remains subject to ten-day history
retention even while optional collection is off.

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
