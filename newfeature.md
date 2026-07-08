# Blueprint: add downloadable 5-minute market JSON + Polymarket Up/Down probabilities

You now want each 5-minute market to contain:

```text
1. BTC price history from Binance Spot BTCUSDT
2. BTC price history from Polymarket Chainlink RTDS BTC/USD
3. Optional Polymarket Up/Down probability history
4. Downloadable JSON per 5-minute market
```

Your current code already has the right base shape: `price_samples` is keyed by `instrument_id + sample_second_ms`, the Polymarket Chainlink price collector uses `crypto_prices_chainlink`, and the API already has combined BTC source endpoints like `/markets/current/sources` and `/markets/{market_id}/sources`.  

The missing pieces are:

```text
1. A table for Polymarket BTC 5m market metadata.
2. A table for one-second Up/Down probability snapshots.
3. A new probability collector service.
4. A JSON builder that aligns Binance, Chainlink, and optional probabilities by second.
5. Download API endpoints.
```

---

## 1. Main design decision

Do **not** store probability data inside `price_samples`.

Keep price data and probability data separate:

```text
price_samples
├── Binance BTCUSDT prices
└── Polymarket Chainlink RTDS BTCUSD prices

polymarket_probability_samples
└── Polymarket Up/Down probability snapshots
```

Then the download API joins them by:

```text
market_id + sample_second_ms
```

This keeps the system clean because BTC price and Polymarket market odds are different kinds of data.

---

## 2. Polymarket probabilities source

Use **Gamma** only to discover the current BTC 5-minute market and extract the CLOB token IDs. Polymarket’s docs say markets can be fetched by slug, and that markets map to a pair of CLOB token IDs. ([Polymarket Documentation][1]) ([Polymarket Documentation][2])

Use **CLOB WebSocket** to collect live Up/Down market prices. Polymarket’s WSS Market Channel is public, subscribes by asset/token IDs, and provides orderbook, price-change, and best-bid/ask updates. ([GitHub][3])

For the BTC 5-minute market, derive the current slug from your existing market window:

```python
slug = f"btc-updown-5m-{market_start_ms // 1000}"
```

Example:

```text
market_start_ms = 1783459200000
slug = btc-updown-5m-1783459200
```

Treat the slug pattern as a shortcut, but treat Gamma as the source of truth for token IDs, outcomes, condition ID, start time, and end time. Your uploaded helper already follows this idea: discover with Gamma, parse `clobTokenIds`, map outcomes to Up/Down, and collect one row per second.  

---

## 3. Download JSON format

Default download should include **only BTC prices**.

Endpoint:

```text
GET /markets/{market_id}/download
```

Response:

```json
{
  "schema_version": 1,
  "market": {
    "market_id": 5944864,
    "market_start_ms": 1783459200000,
    "market_end_ms": 1783459500000,
    "market_start_at": "2026-07-07T21:00:00Z",
    "market_end_at": "2026-07-07T21:05:00Z",
    "seconds_expected": 300
  },
  "series": [
    {
      "t": 0,
      "timestamp_ms": 1783459200000,
      "timestamp_at": "2026-07-07T21:00:00Z",
      "prices": {
        "binance": "65832.98",
        "chainlink": "65810.12"
      }
    },
    {
      "t": 1,
      "timestamp_ms": 1783459201000,
      "timestamp_at": "2026-07-07T21:00:01Z",
      "prices": {
        "binance": "65822.08",
        "chainlink": "65789.12"
      }
    }
  ]
}
```

Use decimal strings in JSON, not floats. The dashboard can convert them to numbers for charts. This avoids precision surprises.

Optional probabilities are included only when requested:

```text
GET /markets/{market_id}/download?include_probabilities=true
```

Response rows then become:

```json
{
  "t": 1,
  "timestamp_ms": 1783459201000,
  "timestamp_at": "2026-07-07T21:00:01Z",
  "prices": {
    "binance": "65822.08",
    "chainlink": "65789.12"
  },
  "probabilities": {
    "up": {
      "bid": "0.47",
      "ask": "0.49",
      "mid": "0.48",
      "normalized": "0.4824"
    },
    "down": {
      "bid": "0.50",
      "ask": "0.53",
      "mid": "0.515",
      "normalized": "0.5176"
    }
  }
}
```

Always return 300 rows for a full 5-minute market:

```text
t = 0 through 299
```

Missing data should be `null`, not backfilled:

```json
{
  "t": 27,
  "timestamp_ms": 1783459227000,
  "prices": {
    "binance": "65840.12",
    "chainlink": null
  }
}
```

Do **not** invent missing Chainlink prices. Do **not** invent missing Polymarket probabilities.

---

## 4. Clickable dashboard option

The dashboard should have two download links or one checkbox.

Default:

```html
<a href="http://127.0.0.1:9000/markets/5944864/download">
  Download BTC prices JSON
</a>
```

With probability data:

```html
<a href="http://127.0.0.1:9000/markets/5944864/download?include_probabilities=true">
  Download BTC prices + Up/Down probabilities JSON
</a>
```

Or with a checkbox:

```text
[ ] Include Polymarket Up/Down probabilities
```

When checked, append:

```text
?include_probabilities=true
```

This keeps BTC price download as the default and makes probabilities a clickable option.

---

# Codex implementation instructions

Paste the following into the Codex agent.

---

## Task

Modify the existing `price_collector` project.

Current working components:

```text
1. Binance BTCUSDT collector.
2. Polymarket Chainlink RTDS BTCUSD collector.
3. Local PostgreSQL.
4. FastAPI API bound to 127.0.0.1:9000.
5. Existing 5-minute market windows using:
   market_start_ms = (sample_second_ms // 300_000) * 300_000
   market_end_ms = market_start_ms + 300_000
   market_id = market_start_ms // 300_000
```

The project already has combined BTC source API helpers and a `fetch_market_summaries_for_btc_sources()` function that groups Binance and Polymarket Chainlink price samples by `market_id`. 

Add:

```text
1. Polymarket BTC 5-minute market metadata storage.
2. Polymarket Up/Down probability collector.
3. One-second probability snapshots.
4. JSON market data endpoint.
5. JSON download endpoint.
```

---

## Add dependencies

Update `requirements.txt`:

```text
httpx
```

Keep existing:

```text
websockets
asyncpg
fastapi
uvicorn[standard]
pydantic-settings
```

---

## Add config settings

Update `price_collector/config.py`:

```python
POLYMARKET_GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_BTC_5M_SLUG_PREFIX: str = "btc-updown-5m"
POLYMARKET_PROBABILITY_SOURCE: str = "polymarket_clob"
POLYMARKET_PROBABILITY_STALE_MS: int = 15_000
POLYMARKET_CLOB_PING_SECONDS: int = 10
```

Keep existing Chainlink RTDS settings untouched:

```python
POLYMARKET_RTDS_WS_URL: str = "wss://ws-live-data.polymarket.com"
POLYMARKET_CHAINLINK_PROVIDER_CODE: str = "polymarket_chainlink_rtds"
POLYMARKET_CHAINLINK_SYMBOL: str = "BTCUSD"
POLYMARKET_CHAINLINK_RTD_SYMBOL: str = "btc/usd"
POLYMARKET_CHAINLINK_TOPIC: str = "crypto_prices_chainlink"
```

---

## Add database migration

Add this to `schema.sql` or a new migration file.

```sql
CREATE TABLE IF NOT EXISTS polymarket_btc_5m_markets (
    market_id BIGINT PRIMARY KEY REFERENCES market_windows(market_id),

    slug TEXT NOT NULL UNIQUE,
    gamma_event_id TEXT,
    gamma_market_id TEXT,
    condition_id TEXT,

    question TEXT,
    start_ms BIGINT,
    end_ms BIGINT,
    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,

    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,
    up_outcome TEXT NOT NULL DEFAULT 'Up',
    down_outcome TEXT NOT NULL DEFAULT 'Down',

    active BOOLEAN,
    closed BOOLEAN,
    archived BOOLEAN,

    raw_gamma JSONB,
    first_seen_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (market_id >= 0)
);

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_markets_slug_idx
    ON polymarket_btc_5m_markets (slug);

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_markets_tokens_idx
    ON polymarket_btc_5m_markets (up_token_id, down_token_id);


CREATE TABLE IF NOT EXISTS polymarket_probability_samples (
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    source TEXT NOT NULL DEFAULT 'polymarket_clob',

    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,

    up_bid NUMERIC(18, 8),
    up_ask NUMERIC(18, 8),
    up_mid NUMERIC(18, 8),

    down_bid NUMERIC(18, 8),
    down_ask NUMERIC(18, 8),
    down_mid NUMERIC(18, 8),

    up_prob_norm NUMERIC(18, 8),
    down_prob_norm NUMERIC(18, 8),

    provider_event_ms BIGINT,
    received_ms BIGINT NOT NULL,

    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (market_id, source, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),

    CHECK (up_bid IS NULL OR (up_bid >= 0 AND up_bid <= 1)),
    CHECK (up_ask IS NULL OR (up_ask >= 0 AND up_ask <= 1)),
    CHECK (up_mid IS NULL OR (up_mid >= 0 AND up_mid <= 1)),

    CHECK (down_bid IS NULL OR (down_bid >= 0 AND down_bid <= 1)),
    CHECK (down_ask IS NULL OR (down_ask >= 0 AND down_ask <= 1)),
    CHECK (down_mid IS NULL OR (down_mid >= 0 AND down_mid <= 1)),

    CHECK (up_prob_norm IS NULL OR (up_prob_norm >= 0 AND up_prob_norm <= 1)),
    CHECK (down_prob_norm IS NULL OR (down_prob_norm >= 0 AND down_prob_norm <= 1))
);

CREATE INDEX IF NOT EXISTS polymarket_probability_samples_market_idx
    ON polymarket_probability_samples (market_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS polymarket_probability_samples_latest_idx
    ON polymarket_probability_samples (sample_second_ms DESC);
```

---

## Add DB helper functions

Add these to `db.py`.

### `upsert_polymarket_btc_5m_market`

```python
async def upsert_polymarket_btc_5m_market(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    slug: str,
    gamma_event_id: str | None,
    gamma_market_id: str | None,
    condition_id: str | None,
    question: str | None,
    start_ms: int | None,
    end_ms: int | None,
    up_token_id: str,
    down_token_id: str,
    up_outcome: str,
    down_outcome: str,
    active: bool | None,
    closed: bool | None,
    archived: bool | None,
    raw_gamma: dict[str, Any],
    seen_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO polymarket_btc_5m_markets (
                    market_id,
                    slug,
                    gamma_event_id,
                    gamma_market_id,
                    condition_id,
                    question,
                    start_ms,
                    end_ms,
                    start_at,
                    end_at,
                    up_token_id,
                    down_token_id,
                    up_outcome,
                    down_outcome,
                    active,
                    closed,
                    archived,
                    raw_gamma,
                    first_seen_ms,
                    last_seen_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, $16, $17,
                    $18::jsonb, $19, $20
                )
                ON CONFLICT (market_id)
                DO UPDATE SET
                    slug = EXCLUDED.slug,
                    gamma_event_id = EXCLUDED.gamma_event_id,
                    gamma_market_id = EXCLUDED.gamma_market_id,
                    condition_id = EXCLUDED.condition_id,
                    question = EXCLUDED.question,
                    start_ms = EXCLUDED.start_ms,
                    end_ms = EXCLUDED.end_ms,
                    start_at = EXCLUDED.start_at,
                    end_at = EXCLUDED.end_at,
                    up_token_id = EXCLUDED.up_token_id,
                    down_token_id = EXCLUDED.down_token_id,
                    up_outcome = EXCLUDED.up_outcome,
                    down_outcome = EXCLUDED.down_outcome,
                    active = EXCLUDED.active,
                    closed = EXCLUDED.closed,
                    archived = EXCLUDED.archived,
                    raw_gamma = EXCLUDED.raw_gamma,
                    last_seen_ms = EXCLUDED.last_seen_ms,
                    updated_at = now()
                """,
                window.market_id,
                slug,
                gamma_event_id,
                gamma_market_id,
                condition_id,
                question,
                start_ms,
                end_ms,
                epoch_ms_to_utc_datetime(start_ms) if start_ms is not None else None,
                epoch_ms_to_utc_datetime(end_ms) if end_ms is not None else None,
                up_token_id,
                down_token_id,
                up_outcome,
                down_outcome,
                active,
                closed,
                archived,
                json.dumps(raw_gamma),
                seen_ms,
                seen_ms,
            )
```

### `upsert_polymarket_probability_sample`

```python
async def upsert_polymarket_probability_sample(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    source: str,
    sample_second_ms: int,
    up_token_id: str,
    down_token_id: str,
    up_bid: Decimal | None,
    up_ask: Decimal | None,
    up_mid: Decimal | None,
    down_bid: Decimal | None,
    down_ask: Decimal | None,
    down_mid: Decimal | None,
    up_prob_norm: Decimal | None,
    down_prob_norm: Decimal | None,
    provider_event_ms: int | None,
    received_ms: int,
    raw: dict[str, Any] | None,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO polymarket_probability_samples (
                    market_id,
                    source,
                    sample_second_ms,
                    sample_second_at,
                    up_token_id,
                    down_token_id,
                    up_bid,
                    up_ask,
                    up_mid,
                    down_bid,
                    down_ask,
                    down_mid,
                    up_prob_norm,
                    down_prob_norm,
                    provider_event_ms,
                    received_ms,
                    raw
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14,
                    $15, $16,
                    $17::jsonb
                )
                ON CONFLICT (market_id, source, sample_second_ms)
                DO UPDATE SET
                    up_token_id = EXCLUDED.up_token_id,
                    down_token_id = EXCLUDED.down_token_id,
                    up_bid = EXCLUDED.up_bid,
                    up_ask = EXCLUDED.up_ask,
                    up_mid = EXCLUDED.up_mid,
                    down_bid = EXCLUDED.down_bid,
                    down_ask = EXCLUDED.down_ask,
                    down_mid = EXCLUDED.down_mid,
                    up_prob_norm = EXCLUDED.up_prob_norm,
                    down_prob_norm = EXCLUDED.down_prob_norm,
                    provider_event_ms = EXCLUDED.provider_event_ms,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                window.market_id,
                source,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                up_token_id,
                down_token_id,
                up_bid,
                up_ask,
                up_mid,
                down_bid,
                down_ask,
                down_mid,
                up_prob_norm,
                down_prob_norm,
                provider_event_ms,
                received_ms,
                json.dumps(raw) if raw is not None else None,
            )
```

---

## Add probability collector file

Create:

```text
price_collector/polymarket_probability_collector.py
```

This service should:

```text
1. Compute the current 5-minute window using market_for_sample_second().
2. Build the current Polymarket BTC slug:
   btc-updown-5m-<market_start_unix_seconds>
3. Fetch Gamma event/market data for that slug.
4. Extract Up and Down CLOB token IDs.
5. Store metadata in polymarket_btc_5m_markets.
6. Connect to CLOB WSS Market Channel.
7. Subscribe only to the Up and Down token IDs.
8. Maintain latest best bid/ask for Up and Down.
9. Once per UTC second, write one probability snapshot.
10. At market_end_ms, roll to the next 5-minute market.
```

Polymarket’s WSS Market Channel uses asset/token IDs in the subscription request, and `best_bid_ask` requires `custom_feature_enabled: true`. ([GitHub][3]) ([Polymarket Documentation][4])

Subscription message:

```python
{
    "type": "market",
    "assets_ids": [up_token_id, down_token_id],
    "custom_feature_enabled": True,
}
```

Heartbeat:

```python
await websocket.send("PING")
```

Send every 10 seconds. Polymarket’s WSS docs describe the client heartbeat as a string ping every 10 seconds and the server response as a pong. ([Polymarket Documentation][4])

---

## Probability collector parsing rules

Handle these event types:

```text
book
price_change
best_bid_ask
market_resolved
```

Ignore:

```text
last_trade_price
tick_size_change
unknown events
```

For `book`:

```python
best_bid = max(Decimal(level["price"]) for level in bids)
best_ask = min(Decimal(level["price"]) for level in asks)
```

For `price_change`:

```python
for change in message["price_changes"]:
    asset_id = change["asset_id"]
    best_bid = Decimal(change["best_bid"]) if present
    best_ask = Decimal(change["best_ask"]) if present
```

For `best_bid_ask`:

```python
asset_id = message["asset_id"]
best_bid = Decimal(message["best_bid"])
best_ask = Decimal(message["best_ask"])
```

Official examples show `book`, `price_change`, and `best_bid_ask` carrying timestamped orderbook/top-of-book fields. ([Polymarket Documentation][4]) ([Polymarket Documentation][4])

---

## Probability calculation

Use this helper:

```python
def midpoint(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is not None and ask is not None:
        return (bid + ask) / Decimal("2")
    if bid is not None:
        return bid
    if ask is not None:
        return ask
    return None


def normalized_probs(
    up_mid: Decimal | None,
    down_mid: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    if up_mid is None or down_mid is None:
        return None, None

    total = up_mid + down_mid
    if total <= 0:
        return None, None

    return up_mid / total, down_mid / total
```

Store both:

```text
up_mid / down_mid
up_prob_norm / down_prob_norm
```

Reason:

```text
up_mid and down_mid = actual market prices.
up_prob_norm and down_prob_norm = model-friendly values that sum to 1.
```

---

## Probability sampling rule

Probability collector samples once per **local UTC second**:

```python
now_ms = current_utc_epoch_ms()
sample_second_ms = (now_ms // 1000) * 1000
window = market_for_sample_second(sample_second_ms)
```

Only write if:

```text
sample_second_ms >= current_market.market_start_ms
sample_second_ms < current_market.market_end_ms
```

Skip writing if:

```text
1. Current Polymarket market has not been discovered.
2. Up token or Down token is missing.
3. Up and Down midpoints cannot be calculated.
4. Latest CLOB update is older than POLYMARKET_PROBABILITY_STALE_MS.
```

Do not write probability samples after the market ends.

At exactly the 5-minute boundary:

```text
4:14:59.000 belongs to old market.
4:15:00.000 belongs to new market.
```

Use the same `market_for_sample_second()` logic already in `market.py`. Your existing `market.py` correctly floors to 5-minute windows and makes exact boundaries belong to the new market. 

---

## Gamma discovery logic

Add these helpers:

```python
def slug_for_window(window: MarketWindow, prefix: str) -> str:
    return f"{prefix}-{window.market_start_ms // 1000}"
```

Discovery flow:

```text
1. Compute current window.
2. Build exact slug.
3. GET /events/slug/{slug}
4. If event has markets, use the first BTC Up/Down market.
5. Fallback to GET /markets?slug={slug}&active=true&closed=false.
6. Parse outcomes and clobTokenIds.
7. Identify Up token and Down token by outcome labels.
8. Store metadata.
9. Return a CurrentPolymarketMarket object.
```

Polymarket docs show event lookup by slug with `/events/slug/{slug}`, and the market-data docs describe fetching by slug as a main strategy. ([Polymarket Documentation][5]) ([Polymarket Documentation][1])

Use robust parsing because `outcomes` and `clobTokenIds` can appear as JSON strings or arrays:

```python
def parse_jsonish(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return [
                x.strip().strip('"')
                for x in value.strip("[]").split(",")
                if x.strip()
            ]
    return value
```

Your uploaded draft already has this parsing approach and maps `Up`/`Down` outcomes to token IDs. 

---

## Add systemd service

Create:

```text
/etc/systemd/system/price-collector-polymarket-probabilities.service
```

Service:

```ini
[Unit]
Description=Polymarket BTC 5m Up/Down probability collector
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pricecollector
Group=pricecollector
WorkingDirectory=/opt/price-collector
EnvironmentFile=/etc/price-collector/price-collector.env
ExecStart=/opt/price-collector/.venv/bin/python -m price_collector.polymarket_probability_collector

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
sudo systemctl enable price-collector-polymarket-probabilities
sudo systemctl start price-collector-polymarket-probabilities
```

Logs:

```bash
sudo journalctl -u price-collector-polymarket-probabilities -f
```

---

# Add market JSON API

## Add DB function: `fetch_market_download_payload`

Add a function that returns one row for every second in the market:

```text
t = 0 through 299
```

Pseudo-SQL:

```sql
WITH mw AS (
    SELECT *
    FROM market_windows
    WHERE market_id = $1
),
seconds AS (
    SELECT
        generate_series(
            (SELECT market_start_ms FROM mw),
            (SELECT market_end_ms FROM mw) - 1000,
            1000
        )::BIGINT AS sample_second_ms
),
binance AS (
    SELECT ps.sample_second_ms, ps.price
    FROM price_samples ps
    JOIN instruments i ON i.instrument_id = ps.instrument_id
    JOIN providers p ON p.provider_id = i.provider_id
    WHERE ps.market_id = $1
      AND p.provider_code = 'binance_spot'
      AND i.symbol = 'BTCUSDT'
),
chainlink AS (
    SELECT ps.sample_second_ms, ps.price
    FROM price_samples ps
    JOIN instruments i ON i.instrument_id = ps.instrument_id
    JOIN providers p ON p.provider_id = i.provider_id
    WHERE ps.market_id = $1
      AND p.provider_code = 'polymarket_chainlink_rtds'
      AND i.symbol = 'BTCUSD'
),
probs AS (
    SELECT *
    FROM polymarket_probability_samples
    WHERE market_id = $1
      AND source = 'polymarket_clob'
),
pm AS (
    SELECT *
    FROM polymarket_btc_5m_markets
    WHERE market_id = $1
)
SELECT
    mw.market_id,
    mw.market_start_ms,
    mw.market_end_ms,
    mw.market_start_at,
    mw.market_end_at,

    pm.slug,
    pm.question,
    pm.condition_id,
    pm.up_token_id,
    pm.down_token_id,

    s.sample_second_ms,

    b.price AS binance_price,
    c.price AS chainlink_price,

    probs.up_bid,
    probs.up_ask,
    probs.up_mid,
    probs.down_bid,
    probs.down_ask,
    probs.down_mid,
    probs.up_prob_norm,
    probs.down_prob_norm
FROM seconds s
CROSS JOIN mw
LEFT JOIN pm ON pm.market_id = mw.market_id
LEFT JOIN binance b ON b.sample_second_ms = s.sample_second_ms
LEFT JOIN chainlink c ON c.sample_second_ms = s.sample_second_ms
LEFT JOIN probs ON probs.sample_second_ms = s.sample_second_ms
ORDER BY s.sample_second_ms ASC;
```

The Python function should build:

```python
{
    "schema_version": 1,
    "market": {...},
    "series": [...]
}
```

When `include_probabilities=False`, omit the `"probabilities"` key entirely.

When `include_probabilities=True`, include it per row.

---

## Add FastAPI endpoints

Add to `api.py`:

```python
from fastapi.responses import Response
import json
```

Endpoints:

```python
@app.get("/markets/current/data")
async def markets_current_data(
    request: Request,
    include_probabilities: bool = Query(False),
) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    sample_second_ms = (now_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=window.market_id,
        include_probabilities=include_probabilities,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="no current market data found")
    return payload


@app.get("/markets/{market_id}/data")
async def markets_data_by_id(
    request: Request,
    market_id: int,
    include_probabilities: bool = Query(False),
) -> dict[str, Any]:
    payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=market_id,
        include_probabilities=include_probabilities,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail=f"no market data found for market_id={market_id}")
    return payload
```

Download endpoints:

```python
@app.get("/markets/current/download")
async def markets_current_download(
    request: Request,
    include_probabilities: bool = Query(False),
) -> Response:
    now_ms = current_utc_epoch_ms()
    sample_second_ms = (now_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    return await market_download_response(
        request,
        market_id=window.market_id,
        include_probabilities=include_probabilities,
    )


@app.get("/markets/{market_id}/download")
async def markets_download_by_id(
    request: Request,
    market_id: int,
    include_probabilities: bool = Query(False),
) -> Response:
    return await market_download_response(
        request,
        market_id=market_id,
        include_probabilities=include_probabilities,
    )


async def market_download_response(
    request: Request,
    *,
    market_id: int,
    include_probabilities: bool,
) -> Response:
    payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=market_id,
        include_probabilities=include_probabilities,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail=f"no market data found for market_id={market_id}")

    suffix = "with_probabilities" if include_probabilities else "prices"
    filename = f"btc_5m_market_{market_id}_{suffix}.json"

    return Response(
        content=json.dumps(payload, default=str, separators=(",", ":")),
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )
```

---

# Output rules

For JSON:

```text
Use strings for Decimal values.
Use null for missing data.
Use UTC timestamps.
Do not use local timezone.
Do not forward-fill missing values.
Do not use floats from the database.
```

Example decimal serializer:

```python
def decimal_or_none(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")
```

---

# Verification SQL

After the probability collector has run for a few minutes:

```sql
SELECT
    market_id,
    slug,
    question,
    up_token_id,
    down_token_id,
    start_at,
    end_at,
    last_seen_ms
FROM polymarket_btc_5m_markets
ORDER BY market_id DESC
LIMIT 5;
```

Probability samples:

```sql
SELECT
    market_id,
    count(*) AS rows,
    min(sample_second_at) AS first_sample,
    max(sample_second_at) AS latest_sample,
    min(up_mid) AS min_up_mid,
    max(up_mid) AS max_up_mid,
    min(down_mid) AS min_down_mid,
    max(down_mid) AS max_down_mid
FROM polymarket_probability_samples
GROUP BY market_id
ORDER BY market_id DESC
LIMIT 5;
```

Combined price/probability check:

```sql
SELECT
    mw.market_id,
    count(DISTINCT ps.sample_second_ms) FILTER (
        WHERE p.provider_code = 'binance_spot'
    ) AS binance_seconds,
    count(DISTINCT ps.sample_second_ms) FILTER (
        WHERE p.provider_code = 'polymarket_chainlink_rtds'
    ) AS chainlink_seconds,
    count(DISTINCT prob.sample_second_ms) AS probability_seconds
FROM market_windows mw
LEFT JOIN price_samples ps ON ps.market_id = mw.market_id
LEFT JOIN instruments i ON i.instrument_id = ps.instrument_id
LEFT JOIN providers p ON p.provider_id = i.provider_id
LEFT JOIN polymarket_probability_samples prob ON prob.market_id = mw.market_id
GROUP BY mw.market_id
ORDER BY mw.market_id DESC
LIMIT 10;
```

API checks:

```bash
curl "http://127.0.0.1:9000/markets/current/data"

curl "http://127.0.0.1:9000/markets/current/data?include_probabilities=true"

curl -OJ "http://127.0.0.1:9000/markets/current/download"

curl -OJ "http://127.0.0.1:9000/markets/current/download?include_probabilities=true"
```

---

# Acceptance criteria

The Codex agent is done only when all of these pass:

```text
1. Existing Binance collector still works.
2. Existing Polymarket Chainlink RTDS collector still works.
3. New probability collector discovers the current BTC 5m Polymarket market using Gamma.
4. New probability collector extracts Up and Down CLOB token IDs.
5. New probability collector subscribes only to those two token IDs.
6. New probability collector stores at most one probability row per second.
7. Probability rows are stored only inside the correct 5-minute market window.
8. At exact 5-minute boundary, collector rolls to the new market.
9. Probability collector does not write after market_end_ms.
10. Probability collector stores bid, ask, mid, and normalized probabilities.
11. Download endpoint returns 300 second rows for a full market.
12. Default download includes Binance + Chainlink prices only.
13. `include_probabilities=true` adds Up/Down probability data.
14. Missing data is null, not forward-filled.
15. Download endpoint returns `Content-Disposition: attachment`.
16. API remains bound only to 127.0.0.1.
17. PostgreSQL remains private.
```



This gives you one clean JSON file per 5-minute market, with BTC prices by default and Polymarket Up/Down probabilities available as an optional download.

[1]: https://docs.polymarket.com/market-data/fetching-markets "Fetching Markets - Polymarket Documentation"
[2]: https://docs.polymarket.com/market-data/overview "Overview - Polymarket Documentation"
[3]: https://github.com/Polymarket/agent-skills/blob/main/websocket.md "agent-skills/websocket.md at main · Polymarket/agent-skills · GitHub"
[4]: https://docs.polymarket.com/api-reference/wss/market "Market Channel - Polymarket Documentation"
[5]: https://docs.polymarket.com/api-reference/events/get-event-by-slug "Get event by slug - Polymarket Documentation"
