
# Price Collector

Production-oriented BTC market-data collection for a single-user Ubuntu 24.04
DigitalOcean droplet. The application collects spot, oracle, futures, order-flow,
top-of-book, and Polymarket probability data into local PostgreSQL. Redis holds
the latest source prices and latest finalized microstructure second needed by
the live API responses.

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
├── /var/lib/price-collector          Collector state
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
- Proactively reconnects an active-but-unproductive RTDS socket when no valid
  BTC/USD Chainlink event is accepted for 10 seconds by default. Before the
  first accepted tick, the empty RTDS bootstrap frame and the narrowly validated
  `crypto_prices` subscription-history dump are received-only startup frames,
  not parse errors. They, control frames, and malformed frames do not reset that
  monotonic deadline.

### Binance USD-M Futures

`python -m price_collector.binance_futures_collector`

- Uses the required `btcusdt@aggTrade` WebSocket as the futures last-price
  source: `p` is the price and `T` is its source timestamp.
- Records local wall time immediately after `recv()` and before parsing, then a
  latest-wins worker writes accepted trades to Redis key `btc:live:futures`
  independently of REST polling and optional raw capture.
- Polls futures REST only for premium/index, funding, open interest, and the
  separate historical open-interest series. It does not call the REST ticker
  endpoint and does not use book midpoint or microprice as "last."
- Stores futures snapshots and completed five-minute historical open-interest
  summaries. A snapshot uses only a fresh trade from the current WebSocket
  connection; otherwise its last-price fields are `null` while the REST fields
  can still be stored. `STALE_PRICE_MS` controls that freshness gate.
- Aggregates `btcusdt@aggTrade` into one-second `binance_flow_1s` rows.
- Aggregates `btcusdt@bookTicker` into one-second `binance_book_1s` rows.
- Can additionally produce one causal `binance_microstructure_1s` research row
  per second when `BINANCE_MICROSTRUCTURE_ENABLED=true`. That path reuses the
  accepted futures `aggTrade` feed and REST premium/OI snapshot, and adds a
  combined spot aggregate-trade/top-10 stream, futures top-10 depth, and the
  censored futures forced-order stream. It retains Decimal summaries, not raw
  messages.
- Publishes that same latest finalized one-second row to Redis key
  `btc:live:microstructure`. Redis holds only the newest finalized row; the
  PostgreSQL table remains the source of record for current and completed
  five-minute history.
- Finalized PostgreSQL rows drain through an independent bounded queue, so a
  slow or locked historical write cannot delay later Redis seconds. At queue
  capacity the oldest unwritten row is dropped and logged, retaining the
  newest history without blocking the live path.
- The microstructure row includes spot/perpetual aggressive flow, top-1/5/10
  depth imbalance, spread, weighted midpoint, sampled BBO OFI, spot/perpetual
  basis, RPI-involved perpetual flow, mark/index/funding/OI context, observed
  long/short forced fills, and source age/lag/skew/gap health fields. A Binance
  forced-order snapshot is observed liquidation stress, not total liquidated
  volume or a future liquidation level.
- Can optionally coalesce the same `aggTrade` feed into private 100 ms OHLC
  evidence rows when `RAW_FUTURES_TRACE_ENABLED=true`. The raw flag does not
  select or disable the public price source; `BINANCE_FUTURES_STREAMS_ENABLED`
  must be `true` for the collector to run.

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
- `binance_microstructure_1s` for the optional receipt-time-aligned research
  summary

Prices in `price_samples` use `NUMERIC(38,18)`. Its primary key is
`(instrument_id, sample_second_ms)`, and a duplicate sample for the same
instrument and second updates the existing row instead of creating a duplicate.
The other one-second tables follow the same upsert pattern with source-specific
keys.

`binance_microstructure_1s.sample_second_ms` labels the start of the local
receipt interval `[sample_second_ms, sample_second_ms + 1000)`. The coordinator
waits for the configured short flush delay, heap-orders events by their
pre-parse wall-receive timestamp, and never moves an event received on or after
the interval boundary backward into the previous row. This also keeps exact
five-minute boundaries in the correct half-open market window.

The compatibility columns `spot_trade_id_span` and `fut_trade_id_span` count
the underlying trades represented by the accepted aggregate-trade messages in
that receipt interval. They sum each message's first-to-last trade-ID span;
they are not gaps between aggregate-trade IDs. The legacy `*_trade_id_span`
names are retained so starter-derived research features keep the same contract.

### High-resolution evidence capture

The high-resolution-capture foundation adds an isolated PostgreSQL schema and
tested bounded-write infrastructure:

- `raw_capture.binance_futures_price_trace_100ms` is the partitioned
  destination for one compact OHLC row per futures WebSocket connection and
  active 100 ms receive bucket.
- `raw_capture.chainlink_price_events` is the partitioned destination for every
  individual valid RTDS tick successfully accepted by the best-effort capture
  queue; same-millisecond and unchanged-price ticks are not coalesced.
- `raw_capture.feed_sessions` stores connection/session metadata for each
  enabled futures or Chainlink capture source.

The `raw_capture` schema is owned separately from the normal historical tables,
is unavailable to `PUBLIC` and the API's `price_reader` role, and grants the
collector writer DDL capability only inside that schema. Six-hour current and
next partitions are managed independently from the public historical tables.

Phase 2 wires the Binance futures `aggTrade` reader to this infrastructure but
keeps raw capture opt-in. Both raw feature flags still default to `false`. When
`RAW_FUTURES_TRACE_ENABLED=true`, receive clocks are recorded immediately after
`recv()` and before JSON parsing, valid trades continue through the existing
one-second flow path, and compact 100 ms buckets are offered without waiting
for the dedicated raw database writer. Connection/session records, bounded
queueing, batched `COPY`, and partition maintenance run only while capture is
enabled.

Phase 3 integrates the Chainlink RTDS reader while keeping
`RAW_CHAINLINK_EVENTS_ENABLED` opt-in. The reader records wall receive time
immediately after `recv()` and before parsing, synchronously updates versioned
latest-wins live state, publishes the critical provider-second historical
version, offers each valid raw event without waiting when enabled, and returns
to RTDS receive work. A separate worker attempts Redis for the newest live
version, which remains pending until that attempt completes. The historical
worker waits for the relevant live-cache attempt and holds the newest provider
second for 1,000 ms after its latest receipt unless a newer second makes it
ready sooner. Pending versions for one provider second are coalesced, reducing
repeated same-second upserts, but a later corrective tick can upsert that second
again even after an earlier write. An update received during an in-flight write
also remains pending for a follow-up upsert. The historical store is bounded at
5,000 pending provider seconds with explicit overflow and failure counters.
Shutdown uses a bounded final drain. The raw queue is independent of both normal
delivery states.

Phase 5 cuts the public futures last price over to the required Binance
`btcusdt@aggTrade` WebSocket. The Redis value is `p`, its
`source_timestamp_ms` is `T`, and `received_ms` is the local pre-parse receive
wall time. The latest-wins Redis worker is independent of the one-second REST
snapshot and raw-capture paths. REST remains responsible only for premium/index,
funding, open interest, and historical open-interest data; there is no REST
ticker fallback, and book-derived values are not labeled as last price. The API
shape is unchanged.

The public Chainlink value remains RTDS `payload.value` delivered through
`btc:live:chainlink`. With both raw flags `false`, neither collector creates a
raw queue, raw writer/maintenance task, raw feed-session record, or dedicated
raw database connection. The futures reader still records its connection and
pre-parse receive stamp because those are now part of the public last-price
path. A phase's deployed code is only its code checkpoint. The corresponding
explicitly accelerated three-hour production canary and all acceptance checks
in `OPERATIONS.md` must pass before that phase is operationally complete. This
short gate provides less confidence about slow leaks, reconnects, daily traffic
variation, and sustained storage growth than a 24-hour canary. After advancing,
leave capture enabled and continue background observation toward 24
uninterrupted hours; a later failure still requires the documented rollback.

Phase 4's deliberate partition-boundary and retention validation has been
explicitly deferred while work proceeds to Phase 5. It is not proven by either
three-hour canary: a short window may not cross a six-hour partition boundary.
Automatic future-partition creation, expired-partition removal, the configured
72-hour retention behavior, and sustained relation-budget enforcement therefore
remain known production risks and must not be described as validated.

Redis is not a historical store. The three source-price keys are:

- `btc:live:binance_spot`
- `btc:live:chainlink`
- `btc:live:futures`

Each value has this shape:

```json
{"value":"62067.89","source_timestamp_ms":123,"received_ms":456}
```

The optional microstructure collector also writes
`btc:live:microstructure`. That key contains the latest finalized flat
one-second microstructure row as compact JSON. Decimal values are JSON strings;
integers and booleans retain their JSON types, and unknown values are `null`.
It never contains the current or a completed five-minute history.

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
- `GET /markets/current/microstructure/live`

The data and download responses use schema version `2` by default and always
include `market.chainlink_resolution` and `market.resolution`, independently of
the optional series flags. These objects contain only official Polymarket
Gamma/CLOB data. Ended markets can remain `pending` briefly while the collector
waits for official resolution; the last Up/Down probability is never treated as
the winner. The active CLOB connection stays open for a short grace period after
the market boundary to capture an official resolution event, while durable REST
reconciliation handles delayed results and fills the official Chainlink values.

The two data routes accept `include_microstructure=true`. That opt-in reads at
most 300 indexed PostgreSQL rows, adds `series[].microstructure` and
microstructure availability counts, and raises the response schema version to
`3`. `microstructure_groups` can select any comma-separated subset of
`books,flow,cross_market,liquidations,quality`; all five are returned by default.
Missing seconds remain `null`, and older markets without microstructure still
return normally. These larger JSON responses support gzip compression.

Downloads remain schema version `2` and do not include microstructure. They omit
the market start/end millisecond fields and per-row `timestamp_ms`, retain the
equivalent UTC `*_at` strings, and format official Chainlink open/close values
to two decimal places. The data routes retain their full timing and precision
fields.

`GET /markets/current/microstructure/live` reads the three source-price keys and
the latest finalized microstructure key with one Redis `MGET`. It returns simple
string-or-`null` prices and the nested microstructure groups without querying
PostgreSQL. `GET /markets/current/live` is unchanged and remains the isolated,
small three-price response.

`GET /markets` is the frontend discovery route. It returns the newest three
completed markets by default, newest first, with market timestamps and
per-source availability counts. Use `include_current=true` to include an
observed active market, and pass the returned `next_before_market_id` as the
exclusive `before_market_id` cursor for older pages. Future and observation-empty
windows are not returned. The frontend should select a returned `market_id` and
then request `/markets/{market_id}/data` with the desired optional datasets.

See [`MICROSTRUCTURE_API.md`](MICROSTRUCTURE_API.md) for a focused guide to the
live and historical microstructure additions, including copy/paste calls,
response examples, field semantics, and the recommended dashboard update flow.
See [`FRONTEND_API.md`](FRONTEND_API.md) for the complete frontend API
reference.

## Repository Layout

```text
price_collector/       Source collectors, shared storage helpers, and API
deployment/            systemd units and environment-file examples
tests/                 Unit and deployment-safety tests
schema.sql             PostgreSQL tables, indexes, constraints, and seed rows
OPERATIONS.md          Update, verification, logs, tunnel, and spot-check commands
MICROSTRUCTURE_API.md  Focused live/history microstructure API usage guide
FRONTEND_API.md        Frontend-facing FastAPI endpoint and response reference
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
Collectors and the API intentionally use separate environment files:

- `/etc/price-collector/collector.env`, created from
  `deployment/collector.env.example`, contains the writer database URL and
  collector settings.
- `/etc/price-collector/api.env`, created from
  `deployment/api.env.example`, contains only the reader database URL and Redis
  settings.

The Chainlink collector's accepted-event watchdog is configured in
`collector.env`:

```text
POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000
```

The value must be between 5,000 and 60,000 ms and is independent of
`STALE_PRICE_MS`.
Only a successfully parsed and accepted `crypto_prices_chainlink` `btc/usd`
event resets the monotonic timer. When it expires, the collector classifies the
connection close as `proactive_reconnect`, applies the existing jittered
backoff, and resubscribes without restarting the process. The last Redis value
is left in place and continues aging until a fresh event arrives.

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

The optional microstructure summary has a separate bounded policy:

```text
BINANCE_MICROSTRUCTURE_ENABLED=false
BINANCE_MICROSTRUCTURE_SPOT_WS_URL=wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth10
BINANCE_MICROSTRUCTURE_FUTURES_DEPTH_WS_URL=wss://fstream.binance.com/public/ws/btcusdt@depth10@500ms
BINANCE_MICROSTRUCTURE_FUTURES_LIQUIDATION_WS_URL=wss://fstream.binance.com/market/ws/btcusdt@forceOrder
BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=100000
BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=600
BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS=250
BINANCE_MICROSTRUCTURE_RETENTION_DAYS=30
BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=4096
BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=6144
```

It defaults off so applying a schema/code update does not silently begin a new
high-rate dataset. Enable it only after applying `schema.sql` and adding the
single production override manually. The collector deletes rows older than the
configured retention once per UTC day. It checks the table plus indexes once
per minute, warns at the lower relation threshold, and pauses only new
microstructure writes at the upper threshold; the critical futures live,
snapshot, flow, and book paths continue. The size gate is hysteretic: after it
pauses, writes resume only when a later `pg_total_relation_size` measurement is
strictly below the warning threshold. Retention `DELETE` removes logical rows
but normally does not shrink the physical PostgreSQL relation, so it must not
be expected to resume a size-paused writer by itself. Resumption requires an
operator-controlled compaction/rebuild or another real physical shrink, plus a
confirmed measurement below the warning threshold. The 30-day starting
retention is a PostgreSQL canary policy, not the DuckDB starter's 400-day
estimate. Measure real PostgreSQL growth before raising it.

For the Phase 2 accelerated three-hour production canary, manually set only
`RAW_FUTURES_TRACE_ENABLED=true`, keep
`RAW_CHAINLINK_EVENTS_ENABLED=false`, and keep
`BINANCE_FUTURES_STREAMS_ENABLED=true`. Do not commit that production override
to the example file. Follow the full accelerated acceptance and rollback
procedure in [`OPERATIONS.md`](OPERATIONS.md). Passing the three-hour gate
permits Phase 3, but leave futures capture running and continue observing the
same health indicators until it reaches at least 24 uninterrupted hours.

Prepare and deploy the Phase 3 code with Chainlink capture still `false`. Do not
enable it until the futures-only Phase 2 canary has completed its uninterrupted
three-hour accelerated acceptance window. The Phase 3 accelerated three-hour
production canary keeps
`BINANCE_FUTURES_STREAMS_ENABLED=true` and
`RAW_FUTURES_TRACE_ENABLED=true`, manually sets
`RAW_CHAINLINK_EVENTS_ENABLED=true`, and restarts only
`price-collector-polymarket-chainlink`. The two enabled collectors can then use
at most two dedicated raw-capture database connections in total. The repository
example remains disabled. Keep both captures enabled and continue background
observation of each one toward 24 uninterrupted hours from its activation time.
Phase 4 retention validation was explicitly deferred so Phase 5 source-cutover
work could proceed; Phase 5 does not validate raw partition rollover, expiry,
or storage-budget enforcement, and that residual risk remains open.

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
Replace `REPLACE_ME` on the first deployment and keep writer credentials out of
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
