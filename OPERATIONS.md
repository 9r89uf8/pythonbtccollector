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

## Ghost TWAP checkpoint B

The optional worker stays inside `price-collector-polymarket-chainlink`. B adds
the worker; optional HTTP/SSE routes are covered separately in Checkpoint C below.
Its Redis key/channel are separate from official
prices. Read the [checkpoint report](GHOST_TWAP_CHECKPOINT_B.md) before a canary;
the first capacity canary is limited to one hour, with earlier guard stops.
Longer validation needs a new storage review; a 72-hour run is not planned with
the present full-detail row layout.

After the reviewed B change is pushed to GitHub, install the schema **before**
restarting the Chainlink service. Keep ghost disabled during this installation:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql --single-transaction -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
```

Review these keys manually in `collector.env`; preserve every other setting and
credential. Do not put them in the API environment or the local tunnel file.

| Key | Installation value |
|---|---|
| `GHOST_TWAP_ENABLED` | `false` |
| `GHOST_TWAP_CANARY_START_MS` | `0` |
| `GHOST_TWAP_STATE_DIRECTORY` | `/var/lib/price-collector/ghost-twap` |
| `GHOST_TWAP_DATABASE_FILESYSTEM_PATH` | `/var/lib/postgresql` |
| `GHOST_TWAP_SOURCE_MAX_AGE_MS` | `5000` |
| `GHOST_TWAP_RECEIPT_MAX_AGE_MS` | `3000` |

Runtime `ghost-canary-v4` / calculation contract 3 separates these freshness
limits. Source age may be at most 5,000 ms; both wall and monotonic receipt ages
may be at most 3,000 ms. Configuration may tighten either bound, not exceed it.
The published validity deadline and Redis TTL honor the earliest of all six
deadlines (three clocks for each feed). The historical ten-second carry limit
and future-clock checks are unchanged. See the [freshness checkpoint](GHOST_TWAP_FRESHNESS_CHECKPOINT.md).

When upgrading an existing B installation for freshness/expiry, no schema or
dependency change is required. After this change is pushed to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudoedit /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli EXISTS btc:live:ghost_chainlink_twap_60s
```

Review or add only `GHOST_TWAP_SOURCE_MAX_AGE_MS=5000` and
`GHOST_TWAP_RECEIPT_MAX_AGE_MS=3000` in the existing collector environment. Keep
`GHOST_TWAP_ENABLED=false` during this upgrade. Preserve the completed campaign's
start, state directory and stop latch. Installing this revision does not start
another run or permit resetting the old one; a later canary needs a separately
reviewed fresh campaign. Old frozen audit JSON, policy and its hash remain
unchanged; mutable reconciliation state can still be versioned.

The subsequent [batch-eligibility checkpoint](GHOST_TWAP_BATCH_ELIGIBILITY_CHECKPOINT.md)
uses runtime `ghost-canary-v5`, the same calculation contract 3 and the same two
freshness settings. It needs the same code-only upgrade sequence above: no
schema migration, new environment keys or other service restarts. Keep ghost
disabled and preserve the stopped campaign when installing it.

V5's attempted payload includes `publication_eligibility` with selection clocks,
eligible horizon IDs and excluded horizon reasons. Arrived forecasts remain in
the six-entry live array with a null price and unavailable quality. Frozen
calculation prices and target observation statuses remain unchanged. If all
originally available forecasts have arrived, publication is withheld with
`no_eligible_horizons`; the previous cache value expires normally. Existing
all-unavailable warmup/health messages retain their earlier behavior.

The full calculation evidence and intent are fsynced before Redis. Final
selection and actual attempt/acknowledgement metadata are reconciled afterward.
A crash before that state persists can leave the exact transmitted subset
unknown; restart marks the outcome unconfirmed and never republishes it. For a
new canary, score acknowledged forecasts only when the exact horizon, target and
price appear as eligible in the attempted payload. The original September 14
canary scripts assume complete batches and must not be reused unchanged for v5.

Before an enabled run, verify default tablespace placement and that the configured
filesystem path shares the database's device. Confirm the writer can inspect
that filesystem and write its state directory. Review the measured storage
probe and remaining disk; preserve the 1.5 GiB stop, 2 GiB budget and 10 GiB
reserve. Exact initial row count plus subsequent admissions bounds 600,000 rows.
The 512-record outbox and 128 KiB record limit reserve bounded growth; they do not
establish sustained production CPU, latency or storage behavior.

For an accepted canary, set the enabled flag and an explicit current UTC epoch
millisecond start once. Keep that start through every restart. The worker stops
new decisions at start plus one hour or an earlier hard guard. Its `campaign.json`
persists stop state; do not delete it or advance the start to bypass a stop.
Its advisory filesystem lock prevents a second worker sharing that outbox.
Changing the outbox directory requires a separately reviewed new run, not a
way around existing limits. Redis/API/PostgreSQL bindings remain loopback only.

Temporary guard/query, outbox or persistence failures suspend new forecasts and
let the cache expire. Recovery requires a fresh successful guard and audit
catch-up, then a new decision. Actual clock-order faults, evidence conflicts,
capacity and deadline stops remain latched for review. Suspension/resumption
logs and audit counters distinguish these cases; do not restart just to clear a
temporary pause. Campaign progress is checkpointed every 30 seconds and at
stop/shutdown, with persisted decisions also constraining restart clock checks.

Apply the schema in one transaction as shown so trigger replacement never leaves
an unguarded committed interval. Collector credentials can insert evidence and
update result state; only the operator's PostgreSQL role acknowledges exports
or expires eligible rows. Do not grant those maintenance privileges to the writer.

Check the isolated audit state and bounded logs with:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin status
redis-cli -h 127.0.0.1 -p 6379 GET btc:live:ghost_chainlink_twap_60s
sudo journalctl -u price-collector-polymarket-chainlink --since '10 minutes ago' -n 100 --no-pager
df -h /var/lib/postgresql /var/lib/price-collector
```

A Redis value expiring is expected when inputs stop, guards stop admissions or
publication fails. An acknowledged write earns confirmed lead only if its
monotonic acknowledgement precedes the target's first receipt. Conflicted or
causally invalid targets are excluded. An attempt without acknowledged ordering
is uncertain, not a successful early forecast. A target at or after the 120-second
matching deadline is late. Frozen forecasts, first matches and terminal missing
statuses are never replaced by a more favorable later result.

At the end, disable the ghost flag and restart only the Chainlink service to
stop production and request its bounded shutdown drain. A disabled startup does
**not** create the ghost worker or reconcile retained audit rows. Review
shutdown/outbox errors and require both an empty outbox and `incomplete_count=0`
before final export; do not infer completion from a successful service restart.

The completed C observation exposed nested five-second shutdown budgets: the
optional sink's outer deadline includes worker cancellation plus the runtime's
separate five-second drain. It expired with 64 nonterminal audit rows and 65
retained outbox files. A reviewed recovery-only pass resolved that tail with the
producer disabled, preserving frozen inputs and observed target/publication
evidence. If this recurs, preserve the exact campaign and outbox, acquire its
exclusive lock, durably archive it, and use a reviewed campaign-specific recovery
procedure. Never re-enable forecasts just to clean up audit state, fetch later
prices to replace missing targets, or reset the campaign. See the
[C findings and recovery evidence](results/spot_twap_response/2026-09-15-checkpoint-c/FINDINGS.md).
The shutdown-budget fix remains outstanding. Stop admissions before a final export.
Run this on the **owner's computer** from its B checkout and development Python:

```powershell
python -m price_collector.ghost_twap_admin download --ssh root@152.42.247.86 --output ghost-canary.jsonl
```

The SSH export streams only audit records. The local command verifies every row
and the whole file before acknowledging unchanged hashes/versions to PostgreSQL.
It will not overwrite a file, and interrupted downloads remain `.part` files.
The adjacent manifest records exact bytes/row count. Retain both outside the
droplet. A changed result makes its acknowledgement ineligible; re-export those
current results before expiry. A local droplet copy alone is not external proof.

After verified export and at least 96 hours of row age, explicit maintenance can
delete at most 100 eligible rows per command:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin expire --limit 100
```

Each deletion rechecks terminal state, age and current export hashes under row
locks. No daemon automatically deletes audit evidence, and no compact summary
tier accumulates indefinitely. Ordinary deletion may leave allocated space;
measure relation/filesystem usage and arrange bounded vacuum maintenance rather
than assume the bytes returned to the filesystem. Keep the full export and
bounded findings after removing eligible database rows.

## Ghost reconnect recovery upgrade

The [reconnect checkpoint](GHOST_TWAP_RECONNECT_CHECKPOINT.md) is runtime
`ghost-canary-v6` / contract 4. Only a qualified short spot connection end may
preserve its bounded observed history. Current input remains unavailable until
the first new spot passes the advancing-source, freshness and source/wall/mono
gap bounds. Bridged seconds remain carried/degraded; hard loss still resets.
Gap admission fences older pending publications, including those awaiting fsync.
Redis attempts already begun retain their actual recorded outcome.

This is a code-only Chainlink collector update. After the reviewed change is
pushed to GitHub, keep `GHOST_TWAP_ENABLED=false` and run:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli EXISTS btc:live:ghost_chainlink_twap_60s
```

No schema, dependencies, systemd units or environment keys change. Do not alter
either completed campaign's start, outbox or stop latch. The pure policy's
`spot_reconnect_max_gap_ms` is frozen at 10,000 ms in production; there is no
environment override. The effective bound also cannot exceed historical carry.
The cache key should be absent while ghost is disabled. Installation does not
enable a new canary. A later run must use reviewed fresh campaign state and the
contract-4-aware observer; the archived combined-canary scorer remains v5/3-only.

## Combined ghost canary observer

The owner authorized the new one-hour run in
[GHOST_TWAP_COMBINED_CANARY.md](GHOST_TWAP_COMBINED_CANARY.md). Install the
observer after pushing the reviewed change to GitHub:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m price_collector.ghost_twap_observer --help
sudo systemctl status price-collector-polymarket-chainlink --no-pager
curl --fail http://127.0.0.1:9000/healthz
```

This adds a standalone operational module, with no schema or dependency change.
Installation alone needs no restart. The accepted activation procedure below
restarts only `price-collector-polymarket-chainlink` after changing its three
campaign keys. The observer is a bounded transient systemd job, not a permanent
service. It receives no collector/API environment file or database credentials.

First run a 15-second observer dry run while ghost stays disabled. Stop it with
SIGTERM, verify the resulting incomplete manifest and inspect read latency,
timeouts, missed bins, CPU and output size. Absence in this test is expected;
estimate live payload storage separately against the fixed 128 MiB output cap.
An interrupted dry run is never a completed coverage measurement.

Before the live run, complete the old-campaign checks in the
[handoff](results/spot_twap_response/2026-09-14-batch-eligibility/CANARY_HANDOFF.md).
Keep its directory and externally verified export untouched. Create a unique
empty campaign directory and a separate observer output directory beneath
`/var/lib/price-collector`, owned by `pricecollector` with mode 0700. The observer
creates its own output directory exclusively; do not pre-create that directory.

Choose one fixed start and end (start plus 3,600,000 ms). Launch the observer a
few seconds before that start with the production interpreter:

```bash
sudo -u pricecollector /opt/price-collector/.venv/bin/python -m price_collector.ghost_twap_observer observe --start-ms "$ghost_start_ms" --output-directory "$ghost_observer_dir"
```

For an unattended hour, run that command in a transient systemd unit with
`User=pricecollector`, working directory `/opt/price-collector`, and a bounded
runtime. Require `ready.json` from an actual successful Redis probe before
activation. Wait until the configured start is current, then change only
`GHOST_TWAP_STATE_DIRECTORY`, `GHOST_TWAP_CANARY_START_MS` and
`GHOST_TWAP_ENABLED=true` in the existing env file. Preserve all other keys,
including source 5,000 ms and receipt 3,000 ms. Restart only the Chainlink
collector and record its runtime ID, PID, commit, fixed interval and paths.
The hour includes startup/warmup; do not extend it after delays or a guard stop.

After the deadline, allow at least 120 seconds for final target matching and
drain the audit. Disable ghost, restart only the Chainlink collector, verify
terminal state, empty outbox, absent key and healthy official feeds. Download a
new complete audit export to the owner's computer with the existing verified
download command; keep both exports. Copy the observer files and verify their
manifest before analyzing. Run offline from the repository root:

```bash
python -m price_collector.ghost_twap_observer analyze --directory "$ghost_observer_copy"
python research/spot_twap_response/combined_canary/analyze.py --input "$ghost_audit_copy" --campaign-start-ms "$ghost_start_ms" --expected-sha256 "$ghost_export_sha256" --expected-rows "$ghost_export_rows" --output "$ghost_analysis_dir"
python -m research.spot_twap_response.combined_canary.join_observer --observer-directory "$ghost_observer_copy" --analysis-directory "$ghost_analysis_dir" --output "$ghost_join_file"
```

Supply hash and row count from the verified download manifest. The scorer checks
the entire export, filters the exact campaign and produces `payload_index.json`
for exact observed-byte joining. It distinguishes calculated, acknowledged
eligible and confirmed-early cohorts. Coverage counts all 36,000 planned bins,
including missing/error bins, and reports full-hour, post-65-second and minute
results. It describes freshness at read time, not continued target non-arrival
or browser delivery. A present but unacknowledged write keeps that audit status.

## Ghost TWAP Checkpoint C

The API was deployed at `5bc676c` and the bounded browser observation has
completed. The producer is disabled; the API remains enabled and reports
unavailable while no current publication exists. The
[findings](results/spot_twap_response/2026-09-15-checkpoint-c/FINDINGS.md) record
the measurements, recovered audit tail and external-export status. The commands
below remain an installation procedure, not authorization for another campaign.

After the change is pushed to GitHub, install the read-only API with the producer
disabled. No schema, collector code, service unit or dependency change is needed.
Keep the API's reader credentials and existing environment entries; add only
`GHOST_TWAP_API_ENABLED=true` to `/etc/price-collector/api.env` when activating
these routes. The example defaults to false. This flag does not start forecasts.

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudoedit /etc/price-collector/api.env
sudo systemctl restart price-api
sudo systemctl status price-api --no-pager
sudo journalctl -u price-api -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/live
curl -N --max-time 12 http://127.0.0.1:9000/forecasts/chainlink-twap/stream
```

With the producer off, GET correctly returns 503 `no_current_publication`; SSE
stays connected and reports unavailable state. The bounded curl stream command
ends with a timeout by design. With the API flag off, both routes return 503
`disabled`. Roll back delivery independently by setting that API flag false and
restarting only `price-api`. Do not reset or remove a completed collector state.

Default limits are 16 SSE clients, 250 ms Redis snapshot reads and 2,000 ms ASGI
send waits. Optional `GHOST_TWAP_API_MAX_CLIENTS`,
`GHOST_TWAP_API_READ_TIMEOUT_MS` and `GHOST_TWAP_API_SEND_TIMEOUT_MS` may tighten
capacity/send bounds; read timeout is bounded between 10 and 1,000 ms. SSE idle
health uses a separate connection and never inherits the snapshot read timeout.

On the owner's computer, forward the API without opening public ports:

```powershell
ssh -N -L 127.0.0.1:19000:127.0.0.1:9000 root@152.42.247.86
```

The local frontend should use that same origin, or its own local same-origin
proxy. No broad CORS permission is installed. Subscribe to `event: ghost` and
read the nested `ghost` object; treat `api.resync`, instance/generation changes
and skipped-update counts as explicit current-state replacement. Event IDs do
not provide replay. Expire displayed values locally even if no more bytes
arrive. Do not restart a full `remaining_ns` lifetime at browser receipt.

The completed C observation followed [the C report](GHOST_TWAP_CHECKPOINT_C.md):
15 minutes of browser forecast admissions plus 120 seconds of anchor collection,
with a fresh directory and unchanged one-hour producer cap. Follow the existing old-campaign export,
disk, permissions and fixed-start checks. Set only the new state directory,
start and enabled flag; restart only the Chainlink service. After 15 minutes the
browser stops admitting forecasts to its measured cohort but continues reading
official anchors for 120 seconds. Then disable the producer flag and restart
the Chainlink service. Editing the environment alone does not stop the running
worker. The final two minutes of extra audit decisions are outside the browser
cohort. Shutdown attempts to mark the unmatched tail, but its nested deadline can
leave retained rows; use the recovery checks above rather than assuming a
disabled restart reconciles them. Verify the stopped worker, absent key, empty
outbox, terminal audit and externally verified export. Never repoint an active
process at another campaign to bypass a guard. Another run needs separate
authorization; ongoing forecast production also requires the shutdown-budget fix and a
reviewed capacity/retention policy.

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
