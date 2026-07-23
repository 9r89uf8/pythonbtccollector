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
- Read-only API: `price-api`
- Live cache: `redis-server`

The corresponding Python entry points are:

- `python -m price_collector.collector`
- `python -m price_collector.polymarket_chainlink_collector`
- `python -m price_collector.binance_futures_collector`
- `python -m price_collector.polymarket_probability_collector`
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
- Store each live price as JSON with only `value`, `source_timestamp_ms`, and
  `received_ms`; price values remain decimal strings.
- `/markets/current/live` must read all three source-price keys with one Redis
  `MGET` and must not query PostgreSQL or run derived models.
- `/markets/current/microstructure/live` must read the three source-price keys
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
- Polymarket RTDS subscription and source-timestamp behavior
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
- Keep `FRONTEND_API.md` synchronized with `price_collector/api.py`. In the same
  checkpoint as any FastAPI endpoint addition, change, rename, or removal,
  update its frontend-facing method and path, request parameters, example call,
  response fields and shape, and relevant error responses.
- Keep `OPERATIONS.md` aligned with deploy, restart, verification, and recovery
  commands.
- When adding a collector or service, update the README, operations guide,
  environment examples, service map in this file, and deployment tests in the
  same checkpoint.
