# AGENTS.md

Guidance for future work in this repository.

## Project Goal

Build and maintain a production-ready Python market-data collector for a
single-user Ubuntu 24.04 DigitalOcean droplet.

The deployed system is:

- A local PostgreSQL database named `price_collector`, used for historical data
- A local Redis instance, used only for current live values
- A Binance Spot collector managed by systemd
- A Polymarket Chainlink RTDS collector managed by systemd
- A Binance USD-M futures, flow, and book collector managed by systemd
- A Polymarket BTC five-minute probability collector managed by systemd
- An opt-in standalone Chainlink catch-up shadow worker managed by systemd
- An opt-in standalone Chainlink two-second challenger managed by systemd
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

## Implementation Rules

- Work in reviewable checkpoints. Do not implement the entire system in one
  session unless the user explicitly asks.
- Keep the project Python-only and use the package layout under
  `price_collector/`.
- Use `Decimal` for prices and financial calculations. Never convert prices to
  `float`.
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

## Service Map

Use these exact service names in deployment and handoff commands:

- Binance Spot: `price-collector`
- Polymarket Chainlink: `price-collector-polymarket-chainlink`
- Binance futures, flow, and book: `price-collector-binance-futures`
- Polymarket probabilities: `price-collector-polymarket-probabilities`
- Chainlink shadow signal: `price-collector-shadow-signal`
- Chainlink two-second challenger: `price-collector-shadow-signal-2s`
- Read-only API: `price-api`
- Live cache: `redis-server`

The corresponding Python entry points are:

- `python -m price_collector.collector`
- `python -m price_collector.polymarket_chainlink_collector`
- `python -m price_collector.binance_futures_collector`
- `python -m price_collector.polymarket_probability_collector`
- `python -m price_collector.shadow_signal_collector`
- `python -m price_collector.shadow_signal_2s_collector`
- `uvicorn price_collector.api:app --host 127.0.0.1 --port 9000 --workers 1`

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

### Polymarket Chainlink

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

### High-Resolution Rollout Status

- Phase 4 partition-boundary and retention validation was explicitly deferred
  while Phase 5 proceeded. Do not claim it is complete or infer it from the
  accelerated three-hour Phase 2/3 canaries.
- Future-partition creation, expired-partition removal, configured 72-hour
  retention, and sustained raw-relation budget enforcement remain unproven
  production risks until Phase 4 is deliberately run and accepted.
- Phase 5 changes the public futures last-price source only; it does not close
  any of those raw-capture retention risks.

### Chainlink Shadow Signal

- Keep the experimental worker in the standalone
  `price-collector-shadow-signal` service. It must never run inside FastAPI or
  either producer collector.
- Keep `SHADOW_SIGNAL_ENABLED=false` unless the accepted selection artifact and
  its exact replay-configuration report have both been promoted into
  `/var/lib/price-collector/shadow-decisions`.
- Install both decision files as `root:pricecollector` mode `0440`. Configure
  their absolute paths and the full selection-file SHA-256 in the dedicated
  root-owned `/etc/price-collector/shadow-signal.env` file.
- Keep matured evaluation persistence independently opt-in with
  `SHADOW_SIGNAL_EVALUATION_ENABLED=false` by default. When it is enabled, the
  dedicated shadow environment contains the writer `DATABASE_URL`; it must
  never contain `READ_DATABASE_URL`.
- Poll `btc:live:futures` and `btc:live:chainlink` together with one Redis
  `MGET` on an epoch-aligned 100 ms cadence.
- Treat the accepted selection artifact as authoritative. Load and validate it
  once at startup, verify it against the replay configuration, publish only
  its frozen provisional primary, and never switch models dynamically.
- Keep all three provisional V0 candidates instantiated for silent evaluation,
  while exposing only the accepted primary in the Redis payload.
- Schedule every candidate attempt, including invalid attempts, once when the
  worker enters an epoch-aligned 500 ms bucket. Do not backfill skipped buckets.
  Mature each attempt at its own horizon against the newest observed Chainlink
  value whose `received_ms` is less than or equal to the target; an observation
  received after the target must never leak into the outcome.
- Stamp `generated_ms` immediately after the Redis `MGET`, when both inputs are
  locally available. Fail an evaluation closed if an input is newer than that
  stamp. If successful observations are separated by more than two configured
  poll intervals, invalidate outstanding actuals across that gap rather than
  pretending the latest-value cache preserves overwritten states.
- Treat one Chainlink publisher epoch and accepted-event sequence as one
  immutable `(source_timestamp_ms, received_ms, value)` identity. A different
  identity under the same sequence invalidates outstanding cohorts and
  quarantines both disputed values. Continue scheduling attempts as
  integrity-invalid during quarantine; recover only on a newer sequence or
  publisher epoch. Preserve the last immutable sequence binding across a
  metadata-less read so recovery cannot redefine the same sequence.
- The live evaluator is exact over cache states returned to its successful
  100 ms observations. Redis is latest-value-only, so an intermediate state
  overwritten entirely between polls is not reconstructable; keep raw replay
  as the event-complete authority for model selection and fine-grained timing.
- Send matured rows through a separate bounded, nonblocking batch writer. A
  database outage, retry, full queue, dropped evaluation row, retention pass,
  or shutdown timeout must not delay or terminate the 100 ms Redis publication
  path. Queue overflow drops the oldest queued persistence item rather than
  blocking the live loop, with a rate-limited warning on the first and every
  hundredth drop.
- Requeue idempotent failed batches ahead of newer rows and retry them after the
  configured delay. Discard records only when bounded capacity is exceeded or
  bounded shutdown cannot drain them; log loss counters at shutdown.
- Store signed `forecast_error` as `projected_chainlink - actual_chainlink` and
  signed `baseline_error` as `chainlink_at_forecast - actual_chainlink`. Keep
  the evaluation table idempotent on
  `(model_version, generated_ms, horizon_ms)` and retain seven days by default.
  Size each bounded retention deletion to outpace five candidates at the
  configured evaluation and cleanup cadences.
- Write `btc:live:chainlink_shadow` with one atomic Redis `SET` carrying a
  1.5-to-2.0-second TTL. Every invalid observation must replace any prior valid
  projection with null projection fields; never carry a valid forecast forward.
- Keep every financial value as `Decimal` and every decimal JSON field as a
  string. The payload must distinguish catch-up validity from
  `full_horizon_before_market_end`; it is not a settlement, probability,
  execution, or market-close forecast.
- A Redis or model failure in the shadow service must not interrupt either
  producer. Phase 5 added internal PostgreSQL evaluation rows only and
  deliberately exposed neither the shadow key nor evaluations through the API.
- Phase 6 exposes only the accepted primary's cached payload through the
  existing `/markets/current/live` response at
  `signals.chainlink_catchup`. FastAPI must remain a serializer: it must not
  import or run the shadow engine, hold model state, query PostgreSQL on this
  route, or expose persisted evaluation rows through the live response.
- Phase 7 reporting exposes persisted evidence only through the JSON routes
  `/markets/current/shadow-evaluations` and
  `/markets/{market_id}/shadow-evaluations`, plus their rounded attachment
  variants ending in `/download`. Keep the reporting routes at full Decimal
  precision. Format only the completed download payload using fixed
  `ROUND_HALF_UP` strings: two decimal places for USD prices, moves, errors,
  and USD performance metrics; four for basis points, beta, skills, and rates.
  Preserve nulls and canonicalize rounded zero as unsigned. Include the
  versioned `export` metadata and `_rounded.json` filename so the lossy artifact
  cannot be mistaken for canonical evidence. Require one supported V0 `model_version`,
  select the requested half-open window by `target_ms`, inspect only its
  generation market and predecessor, and reject more than 1,000 rows.
  Keep this PostgreSQL read path separate from `/markets/current/live`.
  Expose the persisted forecast-time cache snapshots only as
  `chainlink_at_forecast`, `chainlink_at_forecast_source_timestamp_ms`,
  `chainlink_at_forecast_received_ms`, `futures_at_forecast`,
  `futures_at_forecast_source_timestamp_ms`, and
  `futures_at_forecast_received_ms`. These fields belong to `generated_ms`;
  `projected_chainlink` and `actual_chainlink` belong to `target_ms`.
- Read the three source-price keys and `btc:live:chainlink_shadow` with one
  four-key Redis `MGET`; do not issue a second `GET` for the shadow value.
  Decode the shadow value independently with its dedicated strict decoder,
  never as a `LivePrice`.
- Return a well-formed shadow payload even when `valid=false`, and add
  `signal_age_ms = max(0, server_time_ms - generated_ms)`. A missing, expired,
  or malformed shadow value sets `signals.chainlink_catchup` to `null`. Log a
  malformed shadow payload without including its raw contents and without
  failing the three actual price fields. Existing Redis transport and malformed
  source-price failure behavior remains unchanged.
- Phase 6 adds backend exposure only. Do not add dashboard code or assets here;
  dashboard integration belongs to Phase 7 in its separate repository.

### Chainlink Two-Second Challenger

- Keep the prospective two-second experiment in its own
  `price-collector-shadow-signal-2s` service. Do not add it to the accepted
  worker's candidate set or alter the accepted selection/replay artifacts,
  `btc:live:chainlink_shadow`, or `signals.chainlink_catchup`.
- Keep `SHADOW_SIGNAL_2S_ENABLED=false` by default in the dedicated root-owned
  `/etc/price-collector/shadow-signal-2s.env`. This worker is Redis-only and
  must not receive either database URL.
- Freeze the first challenger as `catchup_v1_l2000_h2000_b100`: 2,000 ms
  production-style lookback, 2,000 ms forecast horizon, and Decimal beta `1`.
  Do not describe it as selected or production-proven.
- Poll `btc:live:futures` and `btc:live:chainlink` together on the same
  epoch-aligned 100 ms cadence used by the accepted shadow runtime, while
  keeping independent model state and failure handling.
- Publish only to `btc:live:chainlink_shadow_2s` with one atomic Redis `SET` and
  a 2,000 ms TTL. Every invalid observation must overwrite any prior valid
  prediction with null projection fields. Decimal payload fields remain JSON
  strings.
- Expose the challenger only through
  `/markets/current/live/challengers/chainlink-catchup-2s`. FastAPI remains a
  Redis serializer: it must not execute the engine or query PostgreSQL on this
  route. A missing, expired, or malformed challenger value returns
  `prediction: null`; a Redis transport failure returns HTTP 503.
- The challenger is a lag-only experiment. Do not add a futures–Chainlink basis
  feature until its formula, calibration evidence, and versioned payload have
  been reviewed separately.

### Polymarket Probabilities

- Discover BTC five-minute Up/Down markets through Polymarket Gamma.
- Subscribe only to the discovered Up and Down token IDs through the CLOB
  WebSocket.
- Store at most one probability snapshot per UTC second in the active market.
- Skip stale, resolved, incomplete, or out-of-window snapshots instead of
  backfilling fabricated values.
- Preload the next market before the current market boundary.
- Reconcile ended markets against official Polymarket Gamma/CLOB resolution
  data and persist the official Chainlink open/final prices and outcome.
- Never infer an official winner from the final Up/Down probability quote.

## Live Cache Rules

- PostgreSQL remains the historical source of record; Redis is only a live
  cache.
- Use the source-price keys exactly:
  - `btc:live:binance_spot`
  - `btc:live:chainlink`
  - `btc:live:futures`
- Use `btc:live:chainlink_shadow` only for the standalone experimental shadow
  payload, with its required short TTL. It is not a `LivePrice` value.
- Use `btc:live:chainlink_shadow_2s` only for the independent two-second
  challenger payload. It must not replace or alias the accepted shadow key.
- Store live prices as strings in the existing JSON shape with `value`,
  `source_timestamp_ms`, and `received_ms`.
- `/markets/current/live` must read the three source-price keys and the shadow
  key together in one Redis `MGET`. Its `signals.chainlink_catchup` field is the
  decoded shadow object plus `signal_age_ms`, or `null` when the key is absent,
  expired, or malformed.
- Keep the shadow decoder and failure boundary separate from source-price
  decoding. Shadow decimal fields remain JSON strings. A malformed actual
  source-price payload retains the existing endpoint error behavior, while a
  malformed shadow payload cannot suppress readable actual prices.
- `/markets/current/live` must not add PostgreSQL queries, evaluation-table
  reads, or model execution to its request path.
- `/markets/current/live/challengers/chainlink-catchup-2s` must read only the
  two-second challenger key and must keep its decoder and failure boundary
  separate from both source prices and the accepted shadow payload.
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
  - `binance_usdm_perp` / `BTCUSDT` / `BTC` / `USDT`
- Collectors use `DATABASE_URL` with the writer role.
- The API uses `READ_DATABASE_URL` with the reader role and must not receive the
  writer password.
- `shadow_signal_evaluations` is internal evidence: grant the writer only
  `SELECT`, `INSERT`, and bounded-retention `DELETE`; explicitly revoke the API
  reader and `PUBLIC`. Preserve its primary key on
  `(model_version, generated_ms, horizon_ms)`.
- Grant `price_reader` `SELECT` only on the owner-rights
  `shadow_signal_evaluation_chart_points` view. Revoke that view from `PUBLIC`
  and `price_writer`. Its only forecast-input surface is the six narrowly
  approved Chainlink/futures-at-forecast value and timestamp fields above. Do
  not expose `futures_reference` or any of its metadata, worker age/internal
  fields, writer metadata, or `created_at` through it.

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
- Keep the two-second challenger environment separate at
  `/etc/price-collector/shadow-signal-2s.env` and free of database credentials.
- Keep the shadow decision directory limited to the two promoted, root-owned
  evidence files required by the configured worker. Do not let the service user
  write its own selection decision.

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
6. Base the sequence on `OPERATIONS.md`, adjusting it to the actual files and
   services changed.
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
added manually in `/etc/price-collector/collector.env`, `api.env`, or
`shadow-signal.env`, or `shadow-signal-2s.env`; never tell the user to replace a
production env file wholesale. Include Redis setup or a Redis restart only when
the change actually requires it.

Documentation-only and test-only changes do not require droplet commands unless
they change the documented production procedure. Collector changes never omit
the droplet commands merely because local implementation and tests are done.

## Testing Expectations

Add or update focused tests for each checkpoint. Relevant coverage includes:

- Five-minute market boundary behavior
- Binance ticker and futures stream parsing
- Polymarket RTDS subscription and source-timestamp behavior
- Polymarket probability discovery, state, staleness, and rollover behavior
- Decimal-only financial calculations
- Redis-before-PostgreSQL live writes
- Duplicate same-second upserts
- API latest, current, data, download, and live responses
- API and Redis loopback-only deployment configuration
- Reader/writer credential separation
- Shadow decision trust, Redis TTL, disabled-by-default deployment, and
  producer isolation
- Shadow 500 ms scheduling, horizon-specific causal maturation, invalid-attempt
  coverage, idempotent evaluation persistence, and database-outage isolation
- Shadow live-API four-key `MGET`, backward-compatible source-price fields,
  strict decoder isolation, missing/malformed shadow behavior, and clamped
  `signal_age_ms`
- Two-second challenger fixed configuration, disabled-by-default deployment,
  independent Redis key and worker failure domain, invalid overwrite/TTL, and
  endpoint null-versus-503 behavior

Run the relevant tests before handoff. Run the full suite when practical:

```bash
python -m pytest
```

## Documentation Expectations

- Keep `README.md` aligned with the current architecture, settings, service
  names, and API routes.
- Keep `FRONTEND_API.md` synchronized with `price_collector/api.py`. In the same
  checkpoint as any FastAPI endpoint addition, change, rename, or removal,
  update its frontend-facing method and path, request parameters, example call,
  response fields and shape, and relevant error responses.
- Keep `OPERATIONS.md` aligned with deploy, restart, verification, and recovery
  commands.
- When adding a collector or service, update the README, operations guide,
  environment examples, service map in this file, and deployment tests in the
  same checkpoint.
