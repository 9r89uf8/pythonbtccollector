# Blueprint: DigitalOcean BTCUSDT price collector


## 1. Target architecture

```text
DigitalOcean Droplet, Singapore
Ubuntu 24.04 LTS assumed

systemd
├── postgresql.service
├── price-collector.service
│   └── Python asyncio collector
│       └── Binance Spot WS: wss://stream.binance.com:9443/ws/btcusdt@ticker
└── price-api.service
    └── tiny read-only FastAPI API
        └── binds only to 127.0.0.1:9000

Local PostgreSQL
└── database: price_collector
    ├── providers
    ├── instruments
    ├── market_windows
    └── price_samples
```

The dashboard does **not** run on the droplet. You tunnel the local-only API:

```bash
ssh -N -L 9000:127.0.0.1:9000 root@YOUR_DROPLET_IP
```

Then the dashboard queries:

```text
http://127.0.0.1:9000/markets/latest
```

The API must bind to `127.0.0.1`, not `0.0.0.0`.

---

## 2. Binance assumptions the agent must implement

Use the Binance Spot raw stream:

```text
wss://stream.binance.com:9443/ws/btcusdt@ticker
```

Binance documents the Spot WebSocket base endpoint, raw stream format `/ws/<streamName>`, lowercase stream symbols, 24-hour connection validity, ping/pong expectations, and millisecond timestamp fields. ([Binance Developer Center][2])

For this stream:

```text
stream name: btcusdt@ticker
update speed: 1000ms
last price field: c
event time field: E
```

The `@ticker` payload includes `c` as last price and updates every 1000ms. ([Binance Developer Center][2])

Important correction: the stream is not necessarily sending “many prices per millisecond”; Binance timestamps are in milliseconds, but this specific ticker stream is documented as 1000ms. Still, the collector must enforce **one database write per second max** so the design stays safe when more providers are added.

---

## 3. Market-window rule

Use exactly this rule for every saved sample:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

Use **UTC epoch milliseconds**. Do not use local timezone math.

The collector should define:

```python
sample_second_ms = (now_ms // 1000) * 1000
```

where `now_ms` is current UTC epoch time in milliseconds when the sampler fires.

Store Binance’s event time separately as `provider_event_ms = payload["E"]`.

This means:

```text
[4:05:00.000, 4:10:00.000)
[4:10:00.000, 4:15:00.000)
[4:15:00.000, 4:20:00.000)
```

And exactly:

```text
4:09:59.000 → previous market
4:10:00.000 → new market
```

Do **not** create one physical database table per 5-minute market. Use one `price_samples` table with `market_id`, plus a `market_windows` table.

---

## 4. Manual steps you do outside the agent

Create the DigitalOcean droplet manually:

```text
Provider: DigitalOcean
Region: Singapore
CPU: Premium AMD
vCPU: 1
RAM: 2 GB
Disk: 50 GB
Bandwidth: 2 TB
OS: Ubuntu 24.04 LTS preferred
```

Security choices:

```text
Use SSH key authentication.
Do not expose port 9000 publicly.
Do not expose port 5432 publicly.
Only allow SSH inbound.
Prefer restricting SSH to your own IP in the DigitalOcean firewall.
```

After the agent deploys the app, you connect your dashboard machine using:

```bash
ssh -N -L 9000:127.0.0.1:9000 root@YOUR_DROPLET_IP
```

---

## 5. Agent implementation scope

Give the agent this goal:

> Build a Python systemd-managed price collector on an Ubuntu DigitalOcean droplet. It connects to Binance Spot WebSocket `btcusdt@ticker`, keeps the latest BTCUSDT last price from field `c`, writes at most one row per UTC second into local PostgreSQL, assigns each row to a deterministic 5-minute market window, and exposes a read-only local API on `127.0.0.1:9000`.

The agent should implement:

```text
/opt/price-collector
├── requirements.txt
├── schema.sql
├── README.md
└── price_collector
    ├── __init__.py
    ├── config.py
    ├── db.py
    ├── market.py
    ├── collector.py
    └── api.py
```

Use these Python packages:

```text
websockets
asyncpg
fastapi
uvicorn[standard]
pydantic-settings
```

Do not use Docker unless you explicitly decide to later. For this droplet, systemd + venv is simpler.

---

## 6. Database schema

Create `schema.sql` like this.

```sql
CREATE TABLE IF NOT EXISTS providers (
    provider_id SMALLSERIAL PRIMARY KEY,
    provider_code TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id BIGSERIAL PRIMARY KEY,
    provider_id SMALLINT NOT NULL REFERENCES providers(provider_id),
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider_id, symbol)
);

CREATE TABLE IF NOT EXISTS market_windows (
    market_id BIGINT PRIMARY KEY,
    market_start_ms BIGINT NOT NULL UNIQUE,
    market_end_ms BIGINT NOT NULL,
    market_start_at TIMESTAMPTZ NOT NULL,
    market_end_at TIMESTAMPTZ NOT NULL,

    CHECK (market_start_ms % 300000 = 0),
    CHECK (market_end_ms = market_start_ms + 300000),
    CHECK (market_id = market_start_ms / 300000)
);

CREATE TABLE IF NOT EXISTS price_samples (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    price NUMERIC(38, 18) NOT NULL,
    provider_event_ms BIGINT,
    received_ms BIGINT NOT NULL,

    source_price_field TEXT NOT NULL DEFAULT 'c',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (instrument_id, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (price > 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000)
);

CREATE INDEX IF NOT EXISTS price_samples_market_idx
    ON price_samples (market_id, instrument_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS price_samples_instrument_latest_idx
    ON price_samples (instrument_id, sample_second_ms DESC);

INSERT INTO providers (provider_code, display_name)
VALUES ('binance_spot', 'Binance Spot')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSDT',
    'BTC',
    'USDT',
    'btcusdt@ticker'
FROM providers
WHERE provider_code = 'binance_spot'
ON CONFLICT (provider_id, symbol) DO NOTHING;
```

The agent should create two DB roles:

```text
price_writer: collector uses this, can INSERT/SELECT.
price_reader: API uses this, SELECT only.
```

---

## 7. Collector behavior

The collector must have two loops:

```text
Loop A: WebSocket reader
Loop B: once-per-second sampler/writer
```

### WebSocket reader

Connect to:

```text
wss://stream.binance.com:9443/ws/btcusdt@ticker
```

For each message:

```python
payload = json.loads(message)

symbol = payload["s"]          # should be BTCUSDT
price = Decimal(payload["c"])  # last price
provider_event_ms = payload["E"]
received_ms = current_utc_epoch_ms()
```

Store this in memory as the latest known price:

```python
latest_price = {
    "price": Decimal(...),
    "provider_event_ms": ...,
    "received_ms": ...
}
```

Do not write every WebSocket message directly to the database.

### Reconnect behavior

The collector must reconnect automatically:

```text
Reconnect on any WebSocket close/error.
Use exponential backoff with jitter.
Maximum backoff: 60 seconds.
Reset backoff after successful connection.
Proactively reconnect before 24 hours, for example after 23h 50m.
```

Binance documents that a single connection is only valid for 24 hours, so do not depend on the connection living forever. ([Binance Developer Center][2])

### Sampler/writer loop

Every second, aligned to the next UTC second:

```python
now_ms = current_utc_epoch_ms()
sample_second_ms = (now_ms // 1000) * 1000
```

If no price has been received yet, skip.

If the latest received price is stale, skip. Suggested default:

```text
stale threshold: 10 seconds
```

That means do not keep writing old BTC prices during a connection outage.

Then calculate:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

Insert into `market_windows` first:

```sql
INSERT INTO market_windows (
    market_id,
    market_start_ms,
    market_end_ms,
    market_start_at,
    market_end_at
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (market_id) DO NOTHING;
```

Then insert into `price_samples`:

```sql
INSERT INTO price_samples (
    instrument_id,
    sample_second_ms,
    sample_second_at,
    market_id,
    price,
    provider_event_ms,
    received_ms,
    source_price_field
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'c')
ON CONFLICT (instrument_id, sample_second_ms)
DO UPDATE SET
    price = EXCLUDED.price,
    provider_event_ms = EXCLUDED.provider_event_ms,
    received_ms = EXCLUDED.received_ms;
```

The primary key guarantees no more than one row per instrument per second.

---

## 8. Market helper function

The agent should implement and test this in `market.py`:

```python
from dataclasses import dataclass


MARKET_MS = 300_000


@dataclass(frozen=True)
class MarketWindow:
    market_id: int
    market_start_ms: int
    market_end_ms: int


def market_for_sample_second(sample_second_ms: int) -> MarketWindow:
    if sample_second_ms % 1000 != 0:
        raise ValueError("sample_second_ms must be floored to a whole second")

    market_start_ms = (sample_second_ms // MARKET_MS) * MARKET_MS
    market_end_ms = market_start_ms + MARKET_MS
    market_id = market_start_ms // MARKET_MS

    return MarketWindow(
        market_id=market_id,
        market_start_ms=market_start_ms,
        market_end_ms=market_end_ms,
    )
```

Required tests:

```python
def test_market_boundary():
    # Use explicit epoch millisecond values in real tests.
    # sample at exact boundary belongs to new market.
    assert market_for_sample_second(300_000).market_start_ms == 300_000

    # one second before boundary belongs to previous market.
    assert market_for_sample_second(299_000).market_start_ms == 0
```

---

## 9. Read-only local API

Use FastAPI.

Bind only to:

```text
127.0.0.1:9000
```

Endpoints:

```text
GET /healthz
GET /markets/latest
GET /markets/{market_id}
GET /prices/latest
```

### `GET /healthz`

Return:

```json
{
  "ok": true,
  "database": "ok",
  "service": "price-api"
}
```

### `GET /prices/latest`

Default query:

```text
/provider=binance_spot
/symbol=BTCUSDT
```

Response shape:

```json
{
  "provider": "binance_spot",
  "symbol": "BTCUSDT",
  "price": "123456.78000000",
  "sample_second_ms": 1783459200000,
  "sample_second_at": "2026-07-07T21:00:00Z",
  "provider_event_ms": 1783459199876,
  "received_ms": 1783459199900,
  "market_id": 5944864,
  "market_start_ms": 1783459200000,
  "market_end_ms": 1783459500000
}
```

### `GET /markets/latest`

This should return the latest market that has at least one sample.

Response shape:

```json
{
  "provider": "binance_spot",
  "symbol": "BTCUSDT",
  "market_id": 5944864,
  "market_start_ms": 1783459200000,
  "market_end_ms": 1783459500000,
  "market_start_at": "2026-07-07T21:00:00Z",
  "market_end_at": "2026-07-07T21:05:00Z",
  "is_complete": false,
  "sample_count": 123,
  "open": "123000.00000000",
  "high": "123500.00000000",
  "low": "122900.00000000",
  "close": "123456.78000000",
  "samples": [
    {
      "sample_second_ms": 1783459200000,
      "sample_second_at": "2026-07-07T21:00:00Z",
      "price": "123000.00000000"
    }
  ]
}
```

For a 5-minute market, `samples` should be at most about 300 rows per instrument.

### `GET /markets/{market_id}`

Same shape as `/markets/latest`, but for a requested market ID.

---

## 10. systemd services

Create application user:

```bash
sudo useradd --system --home /var/lib/price-collector --shell /usr/sbin/nologin pricecollector
sudo mkdir -p /var/lib/price-collector
sudo chown -R pricecollector:pricecollector /var/lib/price-collector
```

Application path:

```bash
sudo mkdir -p /opt/price-collector
sudo chown -R pricecollector:pricecollector /opt/price-collector
```

Environment file:

```bash
sudo mkdir -p /etc/price-collector
sudo nano /etc/price-collector/price-collector.env
sudo chmod 640 /etc/price-collector/price-collector.env
sudo chown root:pricecollector /etc/price-collector/price-collector.env
```

Example env file:

```bash
APP_ENV=production
LOG_LEVEL=INFO

BINANCE_WS_URL=wss://stream.binance.com:9443/ws/btcusdt@ticker
PROVIDER_CODE=binance_spot
SYMBOL=BTCUSDT
STALE_PRICE_MS=10000

DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector
READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector

API_HOST=127.0.0.1
API_PORT=9000
```

### Collector service

Create:

```bash
sudo nano /etc/systemd/system/price-collector.service
```

Service:

```ini
[Unit]
Description=BTCUSDT price collector
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pricecollector
Group=pricecollector
WorkingDirectory=/opt/price-collector
EnvironmentFile=/etc/price-collector/price-collector.env
ExecStart=/opt/price-collector/.venv/bin/python -m price_collector.collector

Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/price-collector

[Install]
WantedBy=multi-user.target
```

### API service

Create:

```bash
sudo nano /etc/systemd/system/price-api.service
```

Service:

```ini
[Unit]
Description=Local read-only price API
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pricecollector
Group=pricecollector
WorkingDirectory=/opt/price-collector
EnvironmentFile=/etc/price-collector/price-collector.env
ExecStart=/opt/price-collector/.venv/bin/uvicorn price_collector.api:app --host 127.0.0.1 --port 9000 --workers 1

Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/price-collector

[Install]
WantedBy=multi-user.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable postgresql
sudo systemctl enable price-collector
sudo systemctl enable price-api
sudo systemctl start price-collector
sudo systemctl start price-api
```

---

## 11. Droplet install commands

Assuming Ubuntu:

```bash
sudo apt update
sudo apt upgrade -y

sudo apt install -y \
  python3 \
  python3-venv \
  python3-pip \
  postgresql \
  postgresql-contrib \
  git \
  ufw
```

Firewall:

```bash
sudo ufw allow OpenSSH
sudo ufw enable
sudo ufw status
```

Do **not** run:

```bash
sudo ufw allow 9000
sudo ufw allow 5432
```

---

## 12. PostgreSQL setup

Create DB and roles:

```bash
sudo -u postgres psql
```

Inside `psql`:

```sql
CREATE DATABASE price_collector;

CREATE USER price_writer WITH PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';
CREATE USER price_reader WITH PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';

GRANT CONNECT ON DATABASE price_collector TO price_writer;
GRANT CONNECT ON DATABASE price_collector TO price_reader;
```

Then:

```bash
sudo -u postgres psql -d price_collector -f /opt/price-collector/schema.sql
```

Grant permissions:

```bash
sudo -u postgres psql -d price_collector
```

Inside `psql`:

```sql
GRANT USAGE ON SCHEMA public TO price_writer;
GRANT USAGE ON SCHEMA public TO price_reader;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO price_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO price_writer;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO price_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE ON TABLES TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO price_reader;
```

Make sure PostgreSQL is local only. Check:

```bash
sudo ss -ltnp | grep 5432
```

Acceptable:

```text
127.0.0.1:5432
```

Not acceptable:

```text
0.0.0.0:5432
```

---

## 13. Verification commands

Check collector logs:

```bash
sudo journalctl -u price-collector -f
```

Check API logs:

```bash
sudo journalctl -u price-api -f
```

Check services:

```bash
sudo systemctl status price-collector
sudo systemctl status price-api
```

Check latest DB samples:

```bash
sudo -u postgres psql -d price_collector
```

Then:

```sql
SELECT
    count(*) AS rows,
    min(sample_second_at) AS first_sample,
    max(sample_second_at) AS latest_sample
FROM price_samples;
```

Check latest markets:

```sql
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
```

Check API from inside droplet:

```bash
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl http://127.0.0.1:9000/markets/latest
```

Check from your laptop after SSH tunnel:

```bash
curl http://127.0.0.1:9000/markets/latest
```

---

## 14. Acceptance criteria for the LLM agent

The implementation is acceptable only if all of these pass:

```text
1. Collector connects to wss://stream.binance.com:9443/ws/btcusdt@ticker.
2. Collector extracts BTCUSDT last price from field c.
3. Collector stores no more than one row per second per instrument.
4. sample_second_ms is floored to whole UTC seconds.
5. market_id, market_start_ms, and market_end_ms exactly follow the provided formula.
6. A sample at exact 5-minute boundary belongs to the new market.
7. Collector automatically reconnects after disconnects.
8. Collector proactively reconnects before Binance’s 24-hour connection limit.
9. Collector does not keep writing stale prices during outage.
10. API is read-only.
11. API binds to 127.0.0.1:9000 only.
12. PostgreSQL is not exposed publicly.
13. Services restart after reboot.
14. systemd logs are useful enough to debug connection, insert, and reconnect events.
```

---

## 15. Things the agent must not do

```text
Do not expose FastAPI on 0.0.0.0.
Do not expose PostgreSQL publicly.
Do not put the dashboard on the droplet.
Do not write every WebSocket message to the database.
Do not use float for prices.
Do not calculate markets using local timezone.
Do not create one database table per 5-minute market.
Do not store a fake PostgreSQL file at /var/lib/price-collector/markets.postsgress.
Do not require Binance API keys; this is public market data.
```

---





[1]: https://www.postgresql.org/docs/current/storage-file-layout.html "PostgreSQL: Documentation: 18: 66.1. Database File Layout"
[2]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams "WebSocket Streams | Binance Open Platform"
