# Price Collector

Production-oriented BTC market-data collection for a single-user Ubuntu 24.04
DigitalOcean droplet. The application collects spot, oracle, futures, order-flow,
top-of-book, and Polymarket probability data into local PostgreSQL, while Redis
holds only the latest values needed by the live API response.

The deployment is deliberately private:

- PostgreSQL listens only on the droplet loopback interface.
- Redis listens only on `127.0.0.1:6379` with protected mode enabled.
- FastAPI listens only on `127.0.0.1:9000`.
- Remote API access goes through an SSH tunnel.
- No Docker, public dashboard, public database port, or public API port is used.

## Architecture

```text
DigitalOcean droplet
├── systemd
│   ├── price-collector.service
│   ├── price-collector-polymarket-chainlink.service
│   ├── price-collector-binance-futures.service
│   ├── price-collector-polymarket-probabilities.service
│   └── price-api.service
├── /opt/price-collector              Git checkout and Python virtualenv
├── /etc/price-collector              Root-owned environment files
├── /var/lib/price-collector          Service-user state directory
├── Redis on 127.0.0.1:6379           Live-value cache only
└── PostgreSQL database price_collector
                                      Historical source of record
```

The API uses a read-only PostgreSQL role. Collectors use a separate writer role.
All prices remain `Decimal` values in Python, are stored in PostgreSQL numeric
columns, and are serialized as strings by the API.

## Collectors

### Binance Spot

`python -m price_collector.collector`

- Connects to `wss://stream.binance.com:9443/ws/btcusdt@ticker`.
- Parses ticker field `c` as the last price and `E` as provider event time.
- Writes each newly received latest value to Redis key
  `btc:live:binance_spot` immediately.
- Keeps the latest price in memory and writes at most one PostgreSQL sample per
  UTC second.
- Skips a sample when the latest received price is older than
  `STALE_PRICE_MS`, which defaults to `10000`.
- Reconnects with exponential full-jitter backoff capped at 60 seconds and
  proactively reconnects after about 23 hours 50 minutes.

### Polymarket Chainlink BTC/USD

`python -m price_collector.polymarket_chainlink_collector`

- Connects to Polymarket RTDS at `wss://ws-live-data.polymarket.com`.
- Subscribes to topic `crypto_prices_chainlink` with filter
  `{"symbol":"btc/usd"}`.
- Parses `payload.value` as the price and `payload.timestamp` as source time.
- Writes Redis key `btc:live:chainlink` before historical storage.
- Floors the source payload timestamp to its UTC second and upserts that second
  into PostgreSQL.

### Binance USD-M Futures

`python -m price_collector.binance_futures_collector`

- Polls Binance futures REST data for the latest price, mark/index price,
  funding data, and open interest.
- Writes the latest futures price to Redis key `btc:live:futures` before its
  PostgreSQL snapshot.
- Stores futures snapshots and completed five-minute historical open-interest
  summaries.
- Aggregates `btcusdt@aggTrade` into one-second `binance_flow_1s` rows.
- Aggregates `btcusdt@bookTicker` into one-second `binance_book_1s` rows.
- Can optionally coalesce the same `aggTrade` feed into private 100 ms OHLC
  evidence rows when `RAW_FUTURES_TRACE_ENABLED=true`. This shadow capture
  does not replace the REST price written to `btc:live:futures`.

### Polymarket BTC 5-Minute Probabilities

`python -m price_collector.polymarket_probability_collector`

- Discovers the current BTC Up/Down five-minute market through Polymarket
  Gamma.
- Subscribes to the market's Up and Down tokens through the Polymarket CLOB
  WebSocket.
- Stores one-second bid, ask, midpoint, and normalized probability snapshots
  when the source data is complete and fresh.
- Preloads the next market before the current five-minute boundary.
- Reconciles ended markets against Polymarket Gamma and CLOB REST data, with
  durable retries for resolutions that are not official yet.
- Stores Polymarket's official Chainlink open/close prices, winner or split
  result, official payouts, winning token ID, and resolution timestamp when
  available. Probability quotes are never used to infer the winner.

## Five-Minute Market Windows

Every persisted one-second sample is assigned to a half-open UTC market window:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

For example, `[04:05:00.000, 04:10:00.000)` is one market. A sample at exactly
`04:10:00.000` belongs to the new `[04:10:00.000, 04:15:00.000)` market.

## Data Storage

PostgreSQL is the historical source of record. The main tables are:

- `providers`, `instruments`, and `market_windows`
- `price_samples` for Binance Spot and Polymarket Chainlink prices
- `polymarket_btc_5m_markets`, `polymarket_probability_samples`, and
  `polymarket_btc_5m_resolutions` for discovered markets, probability history,
  and official Polymarket resolution metadata
- `binance_futures_snapshots`
- `binance_futures_oi_5m_summaries`
- `binance_flow_1s`
- `binance_book_1s`

Prices in `price_samples` use `NUMERIC(38,18)`. Its primary key is
`(instrument_id, sample_second_ms)`, and a duplicate sample for the same
instrument and second updates the existing row instead of creating a duplicate.
The other one-second tables follow the same upsert pattern with source-specific
keys.

### High-resolution evidence capture

The high-resolution-capture foundation adds an isolated PostgreSQL schema and
tested bounded-write infrastructure:

- `raw_capture.binance_futures_price_trace_100ms` is the partitioned
  destination for one compact OHLC row per futures WebSocket connection and
  active 100 ms receive bucket.
- `raw_capture.chainlink_price_events` is the planned partitioned destination
  for individual valid RTDS ticks that are successfully captured.
- `raw_capture.feed_sessions` is the planned connection/session metadata table.

The `raw_capture` schema is owned separately from the normal historical tables,
is unavailable to `PUBLIC` and the API's `price_reader` role, and grants the
collector writer DDL capability only inside that schema. Six-hour current and
next partitions are managed independently from the public historical tables.

Phase 2 wires the Binance futures `aggTrade` reader to this infrastructure but
keeps it opt-in. Both feature flags still default to `false`. When
`RAW_FUTURES_TRACE_ENABLED=true`, receive clocks are recorded immediately after
`recv()` and before JSON parsing, valid trades continue through the existing
one-second flow path, and compact 100 ms buckets are offered without waiting
for the dedicated raw database writer. Connection/session records, bounded
queueing, batched `COPY`, and partition maintenance run only while capture is
enabled.

Chainlink capture remains unintegrated in Phase 2 and
`RAW_CHAINLINK_EVENTS_ENABLED` must stay `false`. The public futures live value
also remains the Binance REST `/fapi/v2/ticker/price` result: the futures
WebSocket evidence does not update Redis or change the API response. With both
flags `false`, no capture queue, task, or extra raw database connection is
created. Deploying the Phase 2 code is only the code checkpoint; Phase 2 is not
operationally complete until the futures-only canary in `OPERATIONS.md` has run
continuously for 24 hours and passed its checks.

Redis is not a historical store. It contains only these live keys:

- `btc:live:binance_spot`
- `btc:live:chainlink`
- `btc:live:futures`

Each value has this shape:

```json
{"value":"62067.89","source_timestamp_ms":123,"received_ms":456}
```

## API

The FastAPI application is read-only and is started by systemd with:

```text
uvicorn price_collector.api:app --host 127.0.0.1 --port 9000 --workers 1
```

Current routes:

- `GET /healthz`
- `GET /prices/latest?provider=...&symbol=...`
- `GET /markets?limit=3&include_current=false&before_market_id=...`
- `GET /markets/latest?provider=...&symbol=...`
- `GET /markets/{market_id}?provider=...&symbol=...`
- `GET /markets/current/sources`
- `GET /markets/{market_id}/sources`
- `GET /markets/current/data`
- `GET /markets/{market_id}/data`
- `GET /markets/current/download`
- `GET /markets/{market_id}/download`
- `GET /markets/current/live`

The data and download routes accept optional `include_probabilities`,
`include_futures`, `include_oi`, `include_flow`, `include_book`, `fill_display`,
and `max_carry_forward_ms` query parameters. `/markets/current/live` reads Redis;
the historical and aggregate routes read PostgreSQL.

The data and download responses use schema version `2` and always include
`market.chainlink_resolution` and `market.resolution`, independently of the
optional series flags. These objects contain only official Polymarket
Gamma/CLOB data. Ended markets can remain `pending` briefly while the collector
waits for official resolution; the last Up/Down probability is never treated as
the winner. The active CLOB connection stays open for a short grace period after
the market boundary to capture an official resolution event, while durable REST
reconciliation handles delayed results and fills the official Chainlink values.
Downloads keep schema version `2` as a compact projection: they omit the market
start/end millisecond fields and per-row `timestamp_ms`, retain the equivalent
UTC `*_at` strings, and format official Chainlink open/close values to two
decimal places. The data routes retain their full timing and precision fields.

`GET /markets` is the frontend discovery route. It returns the newest three
completed markets by default, newest first, with market timestamps and
per-source availability counts. Use `include_current=true` to include an
observed active market, and pass the returned `next_before_market_id` as the
exclusive `before_market_id` cursor for older pages. Future and observation-empty
windows are not returned. The frontend should select a returned `market_id` and
then request `/markets/{market_id}/data` with the desired optional datasets.

See [`LIVE_DATA.md`](LIVE_DATA.md) for the end-to-end live price pipeline,
Redis handoff, freshness semantics, and dashboard consumption guidance. See
[`FRONTEND_API.md`](FRONTEND_API.md) for frontend call examples, query
parameters, complete response shapes, optional fields, and error responses.

## Repository Layout

```text
price_collector/       Collectors, database access, live cache, market logic, API
deployment/            systemd units and environment-file examples
tests/                 Unit and deployment-safety tests
schema.sql             PostgreSQL tables, indexes, constraints, and seed rows
OPERATIONS.md          Update, verification, logs, tunnel, and spot-check commands
FRONTEND_API.md        Frontend-facing FastAPI endpoint and response reference
LIVE_DATA.md           Live BTC extraction, cache, and dashboard consumption guide
requirements.txt       Python runtime and test dependencies
```

## Local Development

Python 3.12 is the deployment baseline because it is the default Python version
on Ubuntu 24.04.

```bash
python -m venv .venv
```

Activate the environment on Linux or macOS:

```bash
source .venv/bin/activate
```

Or in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies and run the test suite:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest
```

The test suite uses mocks and fakes; it does not require live exchange feeds or
a live PostgreSQL instance.

## Configuration

Pydantic settings read case-sensitive environment variables without a prefix.
The collector services and API intentionally use separate environment files:

- `/etc/price-collector/collector.env`, created from
  `deployment/collector.env.example`, contains the writer database URL and
  collector settings.
- `/etc/price-collector/api.env`, created from
  `deployment/api.env.example`, contains only the reader database URL and Redis
  settings.

At minimum, replace the database passwords in:

```text
DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector
READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector
```

The probability collector's resolution reconciler uses these settings from
`collector.env`:

- `POLYMARKET_RESOLUTION_POLL_SECONDS=5` sets the scan interval and initial
  retry delay.
- `POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300` caps exponential retry
  backoff while official data is pending or temporarily unavailable.
- `POLYMARKET_RESOLUTION_BATCH_SIZE=20` limits the number of due markets checked
  in one scan.
- `POLYMARKET_RESOLUTION_WS_GRACE_SECONDS=30` keeps the ending market's CLOB
  subscription open briefly for an official winner event without delaying
  collection of the preloaded next market.

High-resolution evidence capture uses these settings. Repository and production
example defaults remain disabled:

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

The Phase 1 schema accepts only a 100 ms futures bucket. The relation budget
applies only to raw-capture PostgreSQL relations; it is not a filesystem quota
and does not include WAL, temporary files, or the rest of the database.

For the Phase 2 production canary, manually set only
`RAW_FUTURES_TRACE_ENABLED=true`, keep
`RAW_CHAINLINK_EVENTS_ENABLED=false`, and keep
`BINANCE_FUTURES_STREAMS_ENABLED=true`. Do not commit that production override
to the example file. Follow the full 24-hour acceptance and rollback procedure
in [`OPERATIONS.md`](OPERATIONS.md).

Do not commit `collector.env`, `api.env`, `droplet.env`, `.env`, or real
credentials. These files are ignored by Git; only their examples belong in the
repository.

## Initial Droplet Deployment

### Assumptions

- Ubuntu 24.04 LTS
- DigitalOcean Singapore region
- 1 vCPU and 2 GB RAM for the initial single-user deployment
- SSH key access
- The GitHub repository is cloned into `/opt/price-collector`
- Only SSH is exposed publicly

### Install Base Packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip postgresql postgresql-contrib redis-server git openssh-client ufw
```

Allow SSH before enabling the firewall:

```bash
sudo ufw allow OpenSSH
sudo ufw enable
sudo ufw status
```

Do not open application or datastore ports:

```text
Do not run: sudo ufw allow 9000
Do not run: sudo ufw allow 5432
Do not run: sudo ufw allow 6379
```

### Create the Service User

```bash
sudo useradd --system --user-group --home /var/lib/price-collector --shell /usr/sbin/nologin pricecollector
sudo install -d -o pricecollector -g pricecollector -m 750 /var/lib/price-collector
```

If the user already exists, only ensure the state directory has the correct
ownership:

```bash
sudo chown -R pricecollector:pricecollector /var/lib/price-collector
```

### Clone From GitHub

The droplet installs and updates the application from GitHub. For this
repository:

```bash
export PRICE_COLLECTOR_REPO="https://github.com/9r89uf8/pythonbtccollector.git"
export PRICE_COLLECTOR_BRANCH="main"

sudo install -d -o pricecollector -g pricecollector -m 750 /opt/price-collector
sudo -u pricecollector git clone --branch "$PRICE_COLLECTOR_BRANCH" "$PRICE_COLLECTOR_REPO" /opt/price-collector
```

For a private repository, configure a read-only GitHub deploy key for the
`pricecollector` user and use the repository's SSH URL instead.

Create the virtual environment and install dependencies:

```bash
cd /opt/price-collector
sudo -u pricecollector python3 -m venv .venv
sudo -u pricecollector .venv/bin/pip install --upgrade pip
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest
```

### Configure PostgreSQL

Keep PostgreSQL on loopback and restart it after changing the setting:

```bash
sudo -u postgres psql -c "ALTER SYSTEM SET listen_addresses = '127.0.0.1';"
sudo systemctl restart postgresql
```

Open PostgreSQL:

```bash
sudo -u postgres psql
```

Create the database and separate writer/reader roles, replacing both passwords:

```sql
CREATE DATABASE price_collector;

CREATE USER price_writer WITH PASSWORD 'REPLACE_WITH_STRONG_WRITER_PASSWORD';
CREATE USER price_reader WITH PASSWORD 'REPLACE_WITH_STRONG_READER_PASSWORD';

REVOKE ALL ON DATABASE price_collector FROM PUBLIC;
GRANT CONNECT ON DATABASE price_collector TO price_writer;
GRANT CONNECT ON DATABASE price_collector TO price_reader;
\q
```

Load the schema as PostgreSQL's owner role:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudo -u postgres psql -d price_collector
```

Grant write access to collectors and read-only access to the API:

```sql
GRANT USAGE ON SCHEMA public TO price_writer;
GRANT USAGE ON SCHEMA public TO price_reader;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO price_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO price_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO price_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE ON TABLES TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO price_reader;
\q
```

`schema.sql` separately creates and secures the `raw_capture` schema. Do not
grant `price_reader` or `PUBLIC` access to it. Its narrowly scoped
`price_writer` ownership and `CREATE` permission are for raw partition
maintenance only and do not grant DDL access in `public`.

### Configure Redis

Redis is a loopback-only live cache:

```bash
sudo sed -i 's/^bind .*/bind 127.0.0.1/' /etc/redis/redis.conf
sudo sed -i 's/^protected-mode .*/protected-mode yes/' /etc/redis/redis.conf
sudo systemctl enable --now redis-server
sudo systemctl restart redis-server
redis-cli -h 127.0.0.1 PING
```

The expected reply is `PONG`.

### Install Environment Files

```bash
sudo install -d -o root -g pricecollector -m 750 /etc/price-collector
sudo test -e /etc/price-collector/collector.env || sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/collector.env.example /etc/price-collector/collector.env
sudo test -e /etc/price-collector/api.env || sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/api.env.example /etc/price-collector/api.env

sudo nano /etc/price-collector/collector.env
sudo nano /etc/price-collector/api.env
```

The guarded install commands create each file only when it does not already
exist, so rerunning them cannot replace production secrets with example values.
Replace `REPLACE_ME` on the first deployment. Keep writer credentials out of
`api.env`.

### Install systemd Units

```bash
sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo cp /opt/price-collector/deployment/price-collector-binance-futures.service /etc/systemd/system/price-collector-binance-futures.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-probabilities.service /etc/systemd/system/price-collector-polymarket-probabilities.service
sudo cp /opt/price-collector/deployment/price-api.service /etc/systemd/system/price-api.service

sudo systemctl daemon-reload
sudo systemctl enable --now price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
```

### Verify the Deployment

```bash
systemctl status redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api --no-pager
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```

Confirm every network service is private:

```bash
ss -ltnp | grep ':9000'
ss -ltnp | grep ':5432'
ss -ltnp | grep ':6379'
```

Acceptable listeners include:

```text
127.0.0.1:9000
127.0.0.1:5432
127.0.0.1:6379
```

These public listeners are not acceptable:

```text
0.0.0.0:9000
0.0.0.0:5432
0.0.0.0:6379
```

## Connect Through an SSH Tunnel

On your local machine, copy `droplet.env.example` to the ignored
`droplet.env`, replace its values, and load it:

```bash
cp droplet.env.example droplet.env
set -a
. ./droplet.env
set +a
```

Open the tunnel and keep the terminal running:

```bash
ssh -N -L "${LOCAL_API_PORT}:127.0.0.1:9000" "${DROPLET_USER}@${DROPLET_IP}"
```

Then query the API locally:

```bash
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/current/live"
```

## Deploy Updates From GitHub

After changes have been pushed to GitHub, a normal code update is:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
```

If `schema.sql` changed, apply it **before** restarting affected services:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
```

If a systemd unit changed, copy the changed unit and reload systemd before the
restart:

```bash
sudo cp /opt/price-collector/deployment/CHANGED.service /etc/systemd/system/CHANGED.service
sudo systemctl daemon-reload
```

Always verify the affected services and local API afterward. See
[`OPERATIONS.md`](OPERATIONS.md) for copy/paste update sequences, logs, database
checks, Redis checks, and service verification.

## Maintenance

- Follow service logs with `journalctl -u SERVICE_NAME -f`.
- Monitor disk space with `df -h`.
- Check PostgreSQL size with
  `SELECT pg_size_pretty(pg_database_size('price_collector'));`.
- `RAW_CAPTURE_MAX_RELATION_MB` does not replace either check; PostgreSQL WAL
  and non-capture relations remain outside that budget.
- No automatic pruning is included for the long-term historical tables. The
  raw-capture partition-maintenance task runs only in a collector whose capture
  flag is enabled.
