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

### Binance Futures, Flow, and Book

- Poll Binance USD-M futures REST for futures price, premium/index data, and
  open interest.
- Write Redis key `btc:live:futures` before historical snapshot storage.
- Keep financial values as `Decimal` throughout parsing and derived math.
- Aggregate `btcusdt@aggTrade` into one-second `binance_flow_1s` rows.
- Aggregate `btcusdt@bookTicker` into one-second `binance_book_1s` rows.
- Keep the historical five-minute open-interest summary aligned with its
  effective market window.
- Respect the stream flush-delay and raw-JSON settings in `Settings`.

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
- Use the existing keys exactly:
  - `btc:live:binance_spot`
  - `btc:live:chainlink`
  - `btc:live:futures`
- Store live prices as strings in the existing JSON shape with `value`,
  `source_timestamp_ms`, and `received_ms`.
- `/markets/current/live` must read Redis and must not add PostgreSQL queries to
  its request path.
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
added manually in `/etc/price-collector/collector.env` or `api.env`; never tell
the user to replace a production env file wholesale. Include Redis setup or a
Redis restart only when the change actually requires it.

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
