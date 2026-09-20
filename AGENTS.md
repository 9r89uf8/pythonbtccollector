# AGENTS.md

Guidance for future work in this repository.

## Project Goal

Build and maintain a production-ready Python market-data collector for a
single-user Ubuntu 24.04 DigitalOcean droplet.

The deployed system is:

- A local PostgreSQL database named `price_collector`, used for historical data
- A local Redis instance, used only for current live values
- A Binance Spot collector managed by systemd
- A Polymarket Chainlink spot and 60-second TWAP RTDS collector managed by systemd
- A Binance USD-M futures, flow, and book collector managed by systemd
- A Polymarket BTC five-minute probability collector managed by systemd
- A small read-only FastAPI API managed by systemd
- API binding fixed to `127.0.0.1:9000`
- Redis binding fixed to `127.0.0.1:6379`
- No public PostgreSQL, Redis, or API exposure
- No Docker, Compose, TypeScript, frontend, or dashboard on the droplet

Production paths and identities:

- Repository: `/opt/price-collector`
- Virtual environment: `/opt/price-collector/.venv`
- Environment files: `/etc/price-collector`
- State directory: `/var/lib/price-collector`
- Service user and group: `pricecollector:pricecollector`
- Retention maintenance user and group: `postgres:postgres`, using only local
  Unix-socket peer authentication; no collector/API environment file

## Implementation Rules

- Work in reviewable checkpoints. Do not implement the entire system in one
  session unless the user explicitly asks.
- Keep the project Python-only. Keep production/runtime code under
  `price_collector/`; research-only Python may live under `research/`. A
  production Git clone may contain those files, but production services must
  never import or execute them, research
  dependencies must never be installed into the production virtual
  environment, and research directories must not be added to a service
  `PYTHONPATH`.
- Use `Decimal` for prices and financial calculations. Never convert raw
  prices, dollar values, or intermediate financial arithmetic to `float`.
  Research-only model fitting may convert finalized dimensionless model
  features to an explicitly named floating type at a tested, documented
  boundary. Figure rendering may likewise use isolated floating display copies
  of finalized values; those copies must never feed calculations, models, or
  persisted truth. Persisted and tabulated financial values remain `Decimal`.
- Use UTC epoch milliseconds for sampling, source timestamps, and market
  windows.
- Keep Binance stream symbols lowercase inside stream names, such as
  `btcusdt@ticker`, `btcusdt@aggTrade`, and `btcusdt@bookTicker`.
- Treat the API as read-only. The API environment must contain reader
  credentials only.
- Keep the Uvicorn host fixed to `127.0.0.1` in systemd.
- Keep Redis and PostgreSQL local to the droplet.
- Do not add Docker, Compose, frontend code, or dashboard assets.
- Preserve unrelated user changes in a dirty worktree.

## Research Reset and Historical Results

- The previous staged research pipeline was retired at the owner's request.
  Begin new research from `RESEARCH_QUESTIONS.md`, with a newly designed study.
- `results/lockflip-2026/` is a read-only historical results archive for learning
  what was observed and what remains unresolved. Its old stage names, model
  choices, thresholds, splits, gates, and procedures are historical descriptions,
  not instructions or defaults for new work.
- Do not restore or reuse the retired pipeline, model bundles, labels, or
  execution state from Git history, backups, or temporary directories unless
  the owner explicitly requests that restoration or reuse.
- Keep the collector and API independent of any new research environment.

## Service Map

Use these exact service names in deployment and handoff commands:

- Binance Spot: `price-collector`
- Polymarket Chainlink: `price-collector-polymarket-chainlink`
- Binance futures, flow, and book: `price-collector-binance-futures`
- Polymarket probabilities: `price-collector-polymarket-probabilities`
- Read-only API: `price-api`
- Live cache: `redis-server`
- Historical cleanup: `price-collector-retention.service`, scheduled by
  `price-collector-retention.timer`

The corresponding Python entry points are:

- `python -m price_collector.collector`
- `python -m price_collector.polymarket_chainlink_collector`
- `python -m price_collector.binance_futures_collector`
- `python -m price_collector.polymarket_probability_collector`
- `uvicorn price_collector.api:app --host 127.0.0.1 --port 9000 --workers 1`
- `python -m price_collector.retention --apply --max-seconds 45`

## Collector History Retention

- Keep non-ghost collector history for at most ten days. Expire whole market
  groups using the ten-day cutoff rounded up to the next five-minute boundary;
  this may delete up to five minutes early. Initial backlog and interrupted jobs
  require multiple bounded passes, so monitor progress rather than assuming a
  timer activation instantly enforces the limit.
- Run cleanup in the separate `price-collector-retention` oneshot/timer, never
  on feed or API request paths. Use bounded transactions, statement/lock limits,
  and a fixed allowlist of current and explicitly supported retired tables.
  Do not restore retired pipelines or execute research code to prune data.
- Keep all ghost tables outside this cleanup. Continuous ghost individual and
  accuracy retention remain seven and ninety days respectively; legacy ghost
  export safeguards remain unchanged. Repository research/results archives are
  outside collector-database retention and must not be removed by it.
- Preserve stricter existing raw-capture retention, normally 72 hours. The
  independent cleanup also enforces the ten-day ceiling when raw capture is
  disabled; it must not create partitions or enable raw collection. Microstructure
  retention is capped at ten days even if an older environment requests thirty.
- Delete children before parents. Keep required market metadata, sessions and
  shared evidence payloads while retained/current rows reference them. Prevent
  metadata backfill and reconciliation from recreating expired market history.
  Instrument/provider identities are configuration, not expiring observations.
- The maintenance service runs as local `postgres` with peer authentication to
  `/var/run/postgresql`, database `price_collector`. It receives no environment
  credentials and must not touch Redis or change live values.
- Use ordinary autovacuum/VACUUM for reusable space. Do not promise that row
  deletion shrinks allocated relations or filesystem use, and do not add
  `VACUUM FULL`, table rewrites or budget increases as part of routine retention.

## Collector Rules

### Binance Spot

- Connect to `wss://stream.binance.com:9443/ws/btcusdt@ticker`.
- Parse ticker payload field `c` as the last price.
- Parse payload field `E` as provider event time in milliseconds.
- Write each newly received latest value to Redis immediately, then update the
  in-memory latest-price store.
- Keep the latest received price in memory.
- Write at most one PostgreSQL sample per UTC second.
- Skip writes when the latest price is older than `STALE_PRICE_MS`, default
  `10000`, based on local received time.
- Reconnect automatically on WebSocket errors.
- Use exponential backoff with full jitter, capped at 60 seconds.
- Proactively reconnect before 24 hours, at about 23 hours 50 minutes.

### Polymarket Chainlink Spot and TWAP

- Connect only through Polymarket RTDS at
  `wss://ws-live-data.polymarket.com`; do not add a direct Chainlink WebSocket.
- Subscribe to topic `crypto_prices_chainlink` with filter
  `{"symbol":"btc/usd"}`.
- Parse `payload.value` as a `Decimal` price.
- Use `payload.timestamp` as provider event time and floor it to its UTC second
  for the sample key and market window.
- Write Redis key `btc:live:chainlink` before PostgreSQL storage.
- Duplicate source events in the same second must update the same row.
- Start an accepted-event idle deadline after each RTDS subscription and reset
  it only after a valid expected-topic, expected-symbol Chainlink tick is
  accepted. PING/PONG, malformed, and unrelated frames must not reset it.
- When the accepted-event deadline expires, close and reconnect only the RTDS
  WebSocket using the existing jittered reconnect path. Preserve the cached
  value so its receive age exposes the gap; do not fabricate a fallback.
- Keep the standard `crypto_prices_chainlink` feed and Redis key
  `btc:live:chainlink` as context; it is not the five-minute settlement feed.
- Independently subscribe to `crypto_prices_twap_sixty` with the same
  `{"symbol":"btc/usd"}` filter and require `payload.window_s = 60`.
- Parse `payload.full_accuracy_value` as an exact E18 integer/`Decimal`. Never
  use `payload.value`, binary floating point, standard Chainlink spot, Binance,
  or a locally computed average as the settlement TWAP.
- Publish each accepted TWAP tick to Redis key
  `btc:live:chainlink_twap_60s` before PostgreSQL persistence.
- Persist every accepted TWAP event, connection session, and explicit no-replay
  gap durably. The optional `raw_capture` feature is not the TWAP source of
  record and may not gate this path.
- Preserve the historical 30-second instrument, events, sessions, and gaps.
  Markets before `2026-08-14T00:00:00Z` retain the
  `crypto_prices_twap_thirty` / `payload.window_s = 30` identity; the market
  starting exactly at that UTC boundary and later markets use the 60-second
  identity. Never relabel historical 30-second evidence as 60-second evidence.
- Apply an accepted-event idle deadline independently to the TWAP socket.
  PING/PONG, malformed, wrong-topic, wrong-symbol, and wrong-window frames must
  not reset it. Preserve the last cached value so gaps remain visible by age.

### Optional Ghost TWAP

- Keep the ghost worker inside `price-collector-polymarket-chainlink`, default
  off through dedicated `GhostSettings`. Require canonical spot and 60-second
  TWAP identities. Offer accepted events synchronously before yielding after
  their original receipt clocks; never coalesce constituent events silently.
- Keep the official TWAP and source Redis keys untouched. The ghost uses its
  own bounded workers, Redis key/channel and database pool. No research imports,
  extra feed connection, live database input polling, or API calculation.
- Persist complete frozen evidence in the bounded fsynced outbox before Redis
  publication. Keep first target matches immutable and distinguish successful
  acknowledgement, uncertain publication, late results and conflicting reports.
- Preserve the fixed one-hour initial canary deadline and persisted hard-stop latch across
  restarts. Keep 1.5 GiB relation stop/2 GiB budget, 600,000 rows, 10 GiB free-space
  reserve, bounded records and fresh guard readings. A cap stop is incomplete
  validation. Never reset the canary clock or raise limits automatically.
- Suspend admission/publication during transient guard or audit failures; resume
  only after fresh guards and successful audit catch-up. Keep actual causality,
  integrity, capacity and deadline failures latched. Only the operator role may
  acknowledge exports or delete eligible ghost rows.
- Expire only terminal rows aged at least 96 hours whose current version and
  hashes match an externally verified export. Later updates invalidate export
  eligibility. No automatic unverified deletion or indefinite summary tier.
- Keep A's any-interior-carry quality rule in this B version; pending and future
  slots remain assumptions. B exposes no ghost API routes; those belong to C
  after prospective review. See `GHOST_TWAP_REFERENCE.md` and the production
  operations section of `README.md`.
- The bounded `ghost_twap_observer` operational CLI may read the local ghost
  Redis key for an explicitly authorized canary. Run it separately from the
  collector, without database credentials or feed connections. Preserve its
  fixed grid, read timeout, byte cap and exact payload ledger. Never interpret
  read errors as absence or local cache observations as browser delivery.

### Optional Historical Settlement Win Rates

The optional historical estimator is documented in `GHOST_TWAP_REFERENCE.md`.
Keep `SETTLEMENT_ENABLED`/`SETTLEMENT_API_ENABLED` default off and independent of
the unchanged six-horizon ghost contract. Reuse the exact-close projection only
in the final 60 seconds, with the decision's ending market and causally observed
website opening reference. Do not restore the retired two-day first-2-bp study,
evaluation start setting, candidate label or automatic study finalization.
Preserve its saved research and results. The exact-close projection uses schema
3, rule `historical-settlement-v2`, `observation_window_s=60` and
`sampling_interval_ms=2000`; keep its new history cohort separate from the
original thirty-second cohort.

Admit at most the first successfully admitted settlement decision in each fixed
two-second UTC wall-clock interval, counting further offers in that interval as
`sampling_skipped`. Do not reopen an admitted interval after a clock regression.
Apply this limit before publication and audit creation, never by dropping already published
evidence. Keep the existing row/byte caps; the rolling ghost's event cadence and
six forecasts are unchanged. Do not extend freshness or TTL to bridge a missed
update; wait for the next valid publication. Migrate the settlement audit's
schedule check to accept the versioned sixty-second window before restarting its writer.

Select the first eligible acknowledged publication in each market's fixed
five-second remaining-time bucket across the final minute before classifying
its absolute margin into [0,1), [1,2), [2,4), [4,8) or at least 8 bp. Use actual
acknowledgement time and identity-validated official outcomes. Keep ghost, TWAP and spot counts distinct;
pool Up/Down explicitly, preserve ties/unknowns/missing observations and separate
incompatible calculation/settlement/policy versions. A cell counts markets, not
ticks. Counts describe historical outcomes, never a locked winner or certified
live probability. Any Wilson range must state its comparable-independent-market
assumption; below 30 resolved observations display counts without a percentage.

Keep per-market evidence seven days and daily count summaries ninety days.
Replace daily totals idempotently, freeze outcomes before individual expiry,
and keep unresolved outcomes explicit. The background worker builds the bounded
Redis history cache; API requests read Redis only. A stale cache is unavailable.
The operator retention timer enforces finite expiry even with the producer off.

### Binance Futures, Flow, and Book

- Require `BINANCE_FUTURES_STREAMS_ENABLED=true`; the collector cannot provide
  its Phase 5 last-price path when streams are disabled.
- Use only `btcusdt@aggTrade.p` as the futures last price and `aggTrade.T` as
  its source timestamp. Do not restore `/fapi/v2/ticker/price`, add a REST
  fallback, or label book midpoint/microprice as "last."
- Record the futures WebSocket wall receive time immediately after `recv()` and
  before parsing. Publish accepted current-connection trades to latest-wins
  state and deliver Redis key `btc:live:futures` from a worker independent of
  REST and optional raw capture.
- Reuse `STALE_PRICE_MS`, default `10000`, to accept a current-connection trade
  for snapshots. Startup, disconnect, or staleness leaves the Redis value aging
  and stores `null` snapshot last-price fields instead of using a fallback.
- Poll Binance USD-M futures REST only for premium/index, funding, and open
  interest, plus the separate historical open-interest series.
- Key snapshot seconds/windows from the premium-index timestamp or snapshot
  observation fallback, not from the aggTrade timestamp. Keep historical
  `received_ms` as the snapshot observation time while the last-price source
  timestamp remains `aggTrade.T`.
- Ensure a selected trade's Redis attempt completes before its historical
  snapshot write; a logged Redis failure must not discard the snapshot.
- Keep financial values as `Decimal` throughout parsing and derived math.
- Aggregate `btcusdt@aggTrade` into one-second `binance_flow_1s` rows.
- Aggregate `btcusdt@bookTicker` into one-second `binance_book_1s` rows.
- Keep the historical five-minute open-interest summary aligned with its
  effective market window.
- Keep `RAW_FUTURES_TRACE_ENABLED` independent of the public live and snapshot
  source. Disabling raw capture must not disable connection identity,
  pre-parse receive timing, Redis delivery, or normal flow aggregation.
- Respect the stream flush-delay and raw-JSON settings in `Settings`.
- Keep the optional microstructure path inside
  `price-collector-binance-futures`. Reuse the accepted futures `aggTrade`
  observation and the existing REST premium/open-interest snapshot; do not add
  a duplicate futures trade connection or REST poller.
- When `BINANCE_MICROSTRUCTURE_ENABLED=true`, consume spot aggregate trades and
  spot top-10 depth together, futures top-10 depth through `/public`, and
  observed forced orders through `/market`. Record wall receive time before
  parsing and retain only one causal PostgreSQL summary row per second.
- Treat `binance_microstructure_1s.sample_second_ms` as the start of the local
  receipt interval `[sample_second_ms, sample_second_ms + 1000)`. Events
  received exactly at the ending boundary belong to the next row. Preserve
  source ages, lags, quote skew, connection gaps, and unhealthy rows rather
  than fabricating fresh values.
- Publish each finalized row to Redis key `btc:live:microstructure` before
  retention checks and PostgreSQL storage. Redis holds only the latest
  finalized second; a Redis failure must not discard its PostgreSQL row, and a
  PostgreSQL size pause or write failure must not suppress the Redis attempt.
- Drain finalized PostgreSQL rows through the independent bounded persistence
  queue. PostgreSQL latency must not delay later causal finalization or Redis
  publication. Queue overflow drops and logs the oldest unwritten row so the
  newest rows and all critical futures paths continue.
- Keep every microstructure financial value as `Decimal` and PostgreSQL
  `NUMERIC`. The forced-order feed is censored observed stress; never label its
  notional as total liquidations or infer future liquidation levels.
- Microstructure retention and relation-size guards apply only to
  `binance_microstructure_1s`. Reaching its cap may pause optional summary
  writes but must not stop the critical futures Redis, snapshot, flow, or book
  paths. Start with the measured PostgreSQL canary retention, not the starter's
  DuckDB size estimate.

### High-Resolution Rollout Status

- Phase 4 partition-boundary and retention validation was explicitly deferred
  while Phase 5 proceeded. Do not claim it is complete or infer it from the
  accelerated three-hour Phase 2/3 canaries.
- Future-partition creation, expired-partition removal, configured 72-hour
  retention, and sustained raw-relation budget enforcement remain unproven
  production risks until Phase 4 is deliberately run and accepted.
- Phase 5 changes the public futures last-price source only; it does not close
  any of those raw-capture retention risks.

### Polymarket Probabilities

- Discover BTC five-minute Up/Down markets through Polymarket Gamma and select
  the required settlement identity from the market start. Markets before
  `2026-08-14T00:00:00Z` must exactly match
  `chainlink_twap`/30-second/`btc-5m-twap-30` and
  `https://data.chain.link/streams/btc-usd-twap-30s-streams`; the market
  starting exactly at that boundary and all later markets must exactly match
  `chainlink_twap`/60-second/`btc-5m-twap-60` and
  `https://data.chain.link/streams/btc-usd-twap-60s-streams`. Unknown or
  contradictory settlement rules fail closed.
- Preserve both identities. Reconciliation and historical API reads must remain
  boundary- and rule-aware rather than rewriting 30-second markets as
  60-second settlements.
- Use `POLYMARKET_MARKET_BACKFILL_START_MS` only as the inclusive lower bound
  for metadata-only completed-market backfill. It defaults to the immutable
  `2026-08-14T00:00:00Z` rule cutover and must be a UTC five-minute-aligned epoch
  millisecond value at or after that cutover. An intentional clean reset may
  move this floor forward to its reset/current market boundary; it must never
  change which rule applies on either side of the cutover.
- Subscribe only to the discovered Up and Down token IDs through the CLOB
  WebSocket.
- Store at most one probability snapshot per UTC second in the active market.
- Skip stale, resolved, incomplete, or out-of-window snapshots instead of
  backfilling fabricated values.
- Track the oldest bid/ask component timestamp for Up and Down independently;
  freshness of one outcome must never refresh the opposite outcome.
- Preload the next market before the current market boundary.
- Reconcile ended markets against official Polymarket Gamma/CLOB resolution
  data and persist the exact Price to Beat, official final price, settlement
  rule identity, and outcome.
- Never infer an official winner from the final Up/Down probability quote.

## Live Cache Rules

- PostgreSQL remains the historical source of record; Redis is only a live
  cache.
- Use the source-price keys exactly:
  - `btc:live:binance_spot`
  - `btc:live:chainlink`
  - `btc:live:chainlink_twap_60s`
  - `btc:live:futures`
- Store each live price as JSON with only `value`, `source_timestamp_ms`, and
  `received_ms`; price values remain decimal strings.
- `/markets/current/live` must read all four source-price keys with one Redis
  `MGET` and must not query PostgreSQL or run derived models.
- `/markets/current/microstructure/live` must read the four source-price keys
  and `btc:live:microstructure` with one Redis `MGET`; it must not query
  PostgreSQL. Derive its market ID from the cached finalized sample when one
  exists so a boundary or stale snapshot is never assigned to a later market.
- Historical and current-market microstructure series remain PostgreSQL-backed.
  Add them to the data routes only when `include_microstructure=true`; missing
  historical rows remain `null` and do not make an otherwise valid market a
  `404`.
- A Redis write failure may be logged without corrupting the historical sample
  or changing numeric types.

## Database Rules

- Store spot/oracle prices as PostgreSQL `NUMERIC(38,18)`.
- The `price_samples` primary key must remain
  `(instrument_id, sample_second_ms)`.
- Duplicate inserts for the same source key and second must update the existing
  row or otherwise avoid creating a duplicate.
- Keep schema changes in `schema.sql` idempotent where practical and add tests
  for keys, constraints, indexes, and seeds.
- Seed and preserve:
  - `binance_spot` / `BTCUSDT` / `BTC` / `USDT` / `btcusdt@ticker`
  - `polymarket_chainlink_rtds` / `BTCUSD` / `BTC` / `USD` /
    `crypto_prices_chainlink:btc/usd`
  - `polymarket_chainlink_twap_rtds` / `BTCUSD_TWAP_30S` / `BTC` / `USD` /
    `crypto_prices_twap_thirty:btc/usd`
  - `polymarket_chainlink_twap_rtds` / `BTCUSD_TWAP_60S` / `BTC` / `USD` /
    `crypto_prices_twap_sixty:btc/usd`
  - `binance_usdm_perp` / `BTCUSDT` / `BTC` / `USDT`
- Collectors use `DATABASE_URL` with the writer role.
- The API uses `READ_DATABASE_URL` with the reader role and must not receive the
  writer password.

## Market Window Rule

For every saved one-second sample:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

Boundary behavior is half-open:

- `[04:05:00.000, 04:10:00.000)`
- `[04:10:00.000, 04:15:00.000)`
- Exactly `04:10:00.000` belongs to the new market.

Use the shared helper in `price_collector.market`; do not duplicate the formula
in collector-specific code.

## Security and Deployment Rules

- Do not expose ports `9000`, `5432`, or `6379` publicly.
- Do not change Uvicorn to `--host 0.0.0.0`.
- Redis must use `bind 127.0.0.1` and protected mode.
- Access the API from another machine only through an SSH tunnel.
- Install production code by cloning the GitHub repository; update it with a
  fast-forward-only Git pull.
- Keep real secrets out of Git. Never overwrite an existing production env file
  with an example file during an update.

## Droplet Update Handoff — Required

Whenever an agent changes collector runtime code, a shared runtime module,
`requirements.txt`, `schema.sql`, a production environment example, or a
collector systemd unit, the final response must contain a **Droplet update**
section with copy/paste-ready commands for the Ubuntu droplet.

The handoff must:

1. Say that the commands are run after the change is pushed to GitHub.
2. Start in `/opt/price-collector` and use
   `sudo -u pricecollector git pull --ff-only`.
3. Install `requirements.txt` into `/opt/price-collector/.venv`.
4. Restart every service affected by the change, using the exact service names
   in this file. Do not leave placeholders in the final commands.
5. Include `systemctl status`, a relevant local API or datastore check, and a
   bounded `journalctl` command when logs help verification.
6. Base the sequence on the production operations section of `README.md`,
   adjusting it to the actual files and services changed.
7. State any required ordering, especially schema before service restart.

For a normal code-only collector update, tailor this template to the affected
service or services:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector
sudo systemctl status price-collector --no-pager
sudo journalctl -u price-collector -n 100 --no-pager
curl http://127.0.0.1:9000/healthz
```

Do not blindly restart only `price-collector`: replace it with every affected
unit. In particular, changes to shared modules such as `config.py`, `db.py`,
`market.py`, `live_cache.py`, or helpers imported from `collector.py` can affect
multiple collectors and the API.

If `schema.sql` changed, pull and install dependencies, apply the schema, and
only then restart affected services:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudo systemctl restart AFFECTED_SERVICE_NAMES
```

The final handoff must replace `AFFECTED_SERVICE_NAMES` with exact units and
must include verification commands.

If a systemd unit changed, copy that exact unit before restarting it:

```bash
sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo systemctl daemon-reload
sudo systemctl enable price-collector
sudo systemctl restart price-collector
```

Again, tailor the filename and service name to the actual unit changed. If an
environment example changed, explain the exact keys that must be reviewed or
added manually in `/etc/price-collector/collector.env` or
`/etc/price-collector/api.env`; never tell the user to replace a production env
file wholesale. Include Redis setup or a Redis restart only when
the change actually requires it.

Documentation-only and test-only changes do not require droplet commands unless
they change the documented production procedure. Collector changes never omit
the droplet commands merely because local implementation and tests are done.

## Testing Expectations

Add or update focused tests for each checkpoint. Relevant coverage includes:

- Five-minute market boundary behavior
- Binance ticker and futures stream parsing
- Polymarket spot and TWAP RTDS subscription, E18 precision, window validation,
  source timestamps, durable sessions/events/gaps, and independent idle deadlines
- Polymarket probability discovery, state, staleness, and rollover behavior
- Decimal-only financial calculations
- Redis-before-PostgreSQL live writes
- Duplicate same-second upserts
- API latest, current, data, download, and live responses
- API and Redis loopback-only deployment configuration
- Reader/writer credential separation
Run the relevant tests before handoff. Run the full suite when practical:

```bash
python -m pytest
```

## Documentation Expectations

- Keep `README.md` aligned with the current architecture, settings, service
  names, and API routes.
- When adding a collector or service, update the README (including production
  operations), environment examples, service map in this file, and deployment
  tests in the same checkpoint.

## Compact Polymarket Evidence Rules

- Keep optional H3 evidence inside `price-collector-polymarket-probabilities`
  with dedicated `EvidenceSettings`; `POLYMARKET_EVIDENCE_ENABLED` defaults to
  `false`. Use the existing Up/Down CLOB connection and a separate bounded HTTP
  metadata worker. No new service, authenticated order path or research model.
- Preserve the owner's exclusion of Polymarket depth and quantities. Record
  paired quotes at the declared 100 ms interval in the final 120 seconds,
  using actual observation wall/monotonic clocks and all four independent
  bid/ask provider and local receipt clocks. Do not backdate a delayed sample,
  reconstruct intermediate events, or infer arbitrary-size fills.
- Validate discovered Gamma identity and settlement rules before requesting
  the website `/api/crypto/crypto-price` reference with explicit BTC,
  five-minute start/end and applicable TWAP parameters. Persist missingness;
  never substitute spot, a local TWAP, or a later reconciled strike for an
  unavailable decision-time Price to Beat. A valid pre-close opening reference
  may coexist with `incomplete=true` and a missing close price.
- Append request/response clocks, status and compact provenance in
  `polymarket_market_observations`; deduplicate normalized payloads by hash in
  `polymarket_evidence_payloads`. Keep response SHA-256, Date and Age in typed
  observation columns so changing headers do not defeat payload deduplication.
  Treat HTTP/API/cache clocks as distinct from
  source observation and fill clocks. Keep fee curves, increments, minimum
  order size, acceptance state and delay flags as observed, including `fd`
  and `itode`; `seconds_delay=0` alone does not prove no taker delay.
- Capture identity-validated CLOB `/markets/{condition_id}` metadata as
  `clob_order_rules` observations, including `seconds_delay`, separately from
  `/clob-markets/{condition_id}` fee curves and the `itode` delay flag. Missing
  required fee parameters remain missing and malformed types remain invalid.
- Store quote rows in `polymarket_quote_observations` and connection
  `session_start`/`session_end` plus explicit loss `gap` records in market
  observations. Use bounded independent persistence queues and retain Decimal
  financial values with `NUMERIC(38,18)` quote columns. Preserve original
  metadata/rule identity and official
  outcome/payout evidence.
- Measure evidence tables, indexes and TOAST. Warn at the configured relation
  budget and pause only new high-rate quotes at the cap. Keep metadata and core
  probability collection active, expose losses as gaps, and drain already
  accepted writes. The separate authorized history-retention timer expires old
  evidence; the collector's size guard itself must not delete data or enable
  unrelated raw futures/Chainlink capture. A quote cap is not a total-disk cap.
- Collection does not establish H3 profitability or execution readiness. Any
  later study must declare sampled-data, missing-depth and execution-delay
  limits and choose fresh evaluation settings without inheriting retired work.
