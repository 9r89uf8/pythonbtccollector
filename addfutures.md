## Recommendation

Yes, I would collect **Binance USDⓈ-M BTCUSDT perpetual OI**, and I would also add **Binance futures BTC price context**. But I would keep them as **optional analysis layers**, not part of the default BTC price download.

For your purpose — understanding BTC 5-minute movement, not live trading — the useful stack becomes:

```text
Default:
- Binance spot BTCUSDT price
- Polymarket Chainlink BTC/USD RTDS price

Optional:
- Polymarket Up/Down ask probabilities
- Binance futures mark/index/last price
- Binance futures open interest
```

Your current database already has a clean pattern: prices are stored by `instrument_id + sample_second_ms`, and probabilities are stored separately by `market_id + source + sample_second_ms`. That is the right idea. OI should also be separate, not forced into `price_samples`. 

---

# How to think about OI market assignment

There are two different Binance OI concepts:

```text
1. Current OI snapshot:
   GET /fapi/v1/openInterest

2. Historical 5-minute OI bucket:
   GET /futures/data/openInterestHist?period=5m
```

Binance’s current OI endpoint returns present OI for a symbol and includes `openInterest`, `symbol`, and `time`. Binance’s historical OI endpoint is `/futures/data/openInterestHist`, and the smallest documented period is `5m`. ([Binance Developers][1])

## My recommendation

Use **current OI polling every 1 second** as your primary OI source.

That gives you:

```text
t=0    OI snapshot
t=1    OI snapshot
t=2    OI snapshot
...
t=299  OI snapshot
```

Then every snapshot belongs to the 5-minute market that contains its timestamp:

```python
sample_second_ms = (binance_oi_time_ms // 1000) * 1000
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

That is the cleanest, least confusing method.

Then calculate:

```text
oi_delta_30s
oi_delta_60s
oi_delta_300s
oi_notional_usdt
premium_bps
```

## What about the Binance 5-minute historical OI bucket?

Your instinct is good: if Binance gives a 5-minute OI summary for `[4:05, 4:10)`, then it is more useful as **context at the start of the next market**, `[4:10, 4:15)`, because it tells you what just happened before this market began.

So I would expose it like this:

```json
"previous_5m_oi_summary": {
  "source_window_start_ms": 1783514700000,
  "source_window_end_ms": 1783515000000,
  "effective_market_id": 5945050,
  "sum_open_interest": "74321.123",
  "sum_open_interest_value": "4616789012.34"
}
```

Do **not** silently insert the previous 5-minute OI summary into `t=0` as if it happened at that second. That would mix “previous-window context” with “current-window measurement.”

So the rule should be:

```text
Current OI snapshot:
- assign to the market containing the OI timestamp.

Historical 5m OI bucket:
- store the original source window.
- expose it as previous_5m_oi_summary on the next market.
```

---

# Should you add Binance futures BTC price?

Yes. I would add it, but keep it optional in the JSON.

For understanding BTC movement, futures context is useful because Polymarket odds may react more to leveraged futures flow than to spot alone. I would start with:

```text
futures_last_price
mark_price
index_price
premium_bps
open_interest
oi_notional_usdt
```

Binance documents the USDⓈ-M futures latest price endpoint as `/fapi/v2/ticker/price`, and the mark/index/funding endpoint as `/fapi/v1/premiumIndex`, which returns fields like `markPrice`, `indexPrice`, `lastFundingRate`, `nextFundingTime`, and `time`. ([Binance Developers][2])

For your use case, I would **not** start with high-frequency depth/book/liquidations yet. That adds noise and data volume. Add futures mark/index/OI first. Later, if you want, add liquidations and book pressure.

---

# Data model recommendation

Do **not** put OI inside `price_samples`.

Add a new table:

```text
binance_futures_snapshots
```

This table stores one row per second with:

```text
market_id
sample_second_ms
futures_last_price
mark_price
index_price
open_interest
oi_notional_usdt
premium_bps
funding fields
```

Then optionally add another table for Binance’s historical 5-minute OI buckets:

```text
binance_futures_oi_5m_summaries
```

That table stores:

```text
source_window_start_ms
source_window_end_ms
effective_market_id
sum_open_interest
sum_open_interest_value
```

The current download query only joins spot Binance price, Polymarket Chainlink price, and Polymarket probabilities, so Codex needs to extend that query with futures/OI joins. 

---

# Download JSON design

Keep default download clean:

```text
GET /markets/{market_id}/download
```

Default should include only:

```json
"prices": {
  "binance": "62067.89",
  "chainlink": "62012.87"
}
```

Then add options:

```text
GET /markets/{market_id}/download?include_probabilities=true
GET /markets/{market_id}/download?include_futures=true
GET /markets/{market_id}/download?include_oi=true
GET /markets/{market_id}/download?include_probabilities=true&include_futures=true&include_oi=true
```

Example row with all options:

```json
{
  "t": 12,
  "timestamp_ms": 1783515012000,
  "timestamp_at": "2026-07-08T12:50:12Z",
  "prices": {
    "binance": "62067.90",
    "chainlink": "62013.53"
  },
  "futures": {
    "last": "62075.12",
    "mark": "62074.88",
    "index": "62070.19",
    "premium_bps": "0.76"
  },
  "open_interest": {
    "contracts": "74321.123",
    "notional_usdt": "4616789012.34",
    "delta_30s": null,
    "delta_60s": null,
    "delta_300s": null
  },
  "probabilities": {
    "up": {
      "ask": "0.56"
    },
    "down": {
      "ask": "0.45"
    }
  }
}
```

Market-level previous OI summary:

```json
"previous_5m_oi_summary": {
  "source_window_start_ms": 1783514700000,
  "source_window_end_ms": 1783515000000,
  "effective_market_id": 5945050,
  "sum_open_interest": "74321.123",
  "sum_open_interest_value": "4616789012.34"
}
```

---

# Codex instructions

Paste this into Codex.

```text
Modify the existing price_collector project.

Goal:
Add Binance USDⓈ-M BTCUSDT perpetual futures context and open interest collection.

This is for research/analysis, not live trading. Keep the system simple and robust.

Current system:
- Binance Spot BTCUSDT collector exists.
- Polymarket Chainlink RTDS BTCUSD collector exists.
- Polymarket BTC 5m Up/Down probability collector exists.
- market_windows table uses deterministic 5-minute windows.
- price_samples stores spot/Chainlink prices.
- polymarket_probability_samples stores Up/Down probability samples.
- Download endpoint currently returns Binance spot + Chainlink prices and optionally probabilities.

Keep default download unchanged:
- Default JSON includes only Binance spot and Polymarket Chainlink prices.

Add optional query params:
- include_probabilities: bool = false
- include_futures: bool = false
- include_oi: bool = false

Endpoints to update:
- GET /markets/current/data
- GET /markets/{market_id}/data
- GET /markets/current/download
- GET /markets/{market_id}/download

New data sources:
Use Binance USDⓈ-M Futures REST API.

Base:
https://fapi.binance.com

Endpoints:
1. Current open interest:
   GET /fapi/v1/openInterest?symbol=BTCUSDT

2. Futures latest price:
   GET /fapi/v2/ticker/price?symbol=BTCUSDT

3. Mark/index/funding:
   GET /fapi/v1/premiumIndex?symbol=BTCUSDT

4. Optional historical 5-minute OI:
   GET /futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=2

Use Decimal. Do not use float.

Primary OI rule:
- Poll current OI once per second.
- Use Binance OI response field `time` as the source timestamp.
- If `time` is missing or invalid, use local received_ms.
- sample_second_ms = (source_time_ms // 1000) * 1000
- window = market_for_sample_second(sample_second_ms)
- Store one row max per symbol per second.

Historical 5m OI rule:
- Treat historical OI as a completed previous-window summary.
- Do not insert it as if it happened at t=0.
- Store source_window_start_ms, source_window_end_ms, and effective_market_id.
- For a completed bucket [4:05, 4:10), effective_market_id should be the 4:10-4:15 market.
- Expose it in JSON as previous_5m_oi_summary.
```

---

## Add config

In `config.py` add:

```python
BINANCE_FUTURES_BASE_URL: str = "https://fapi.binance.com"
BINANCE_FUTURES_SYMBOL: str = "BTCUSDT"
BINANCE_FUTURES_PROVIDER_CODE: str = "binance_usdm_perp"

BINANCE_FUTURES_POLL_SECONDS: int = 1
BINANCE_FUTURES_REST_TIMEOUT_SECONDS: int = 5

BINANCE_FUTURES_HIST_OI_ENABLED: bool = True
BINANCE_FUTURES_HIST_OI_POLL_SECONDS: int = 30
```

Do not remove existing Binance Spot or Polymarket settings.

---

## Add schema

Add this migration to `schema.sql`.

```sql
CREATE TABLE IF NOT EXISTS binance_futures_snapshots (
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    futures_last_price NUMERIC(38, 18),
    futures_last_price_time_ms BIGINT,

    mark_price NUMERIC(38, 18),
    index_price NUMERIC(38, 18),
    last_funding_rate NUMERIC(38, 18),
    next_funding_time_ms BIGINT,
    premium_index_time_ms BIGINT,

    open_interest NUMERIC(38, 18),
    open_interest_time_ms BIGINT,

    oi_notional_usdt NUMERIC(38, 18),
    premium_bps NUMERIC(20, 8),

    received_ms BIGINT NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (open_interest IS NULL OR open_interest >= 0)
);

CREATE INDEX IF NOT EXISTS binance_futures_snapshots_market_idx
    ON binance_futures_snapshots (market_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_futures_snapshots_latest_idx
    ON binance_futures_snapshots (symbol, sample_second_ms DESC);


CREATE TABLE IF NOT EXISTS binance_futures_oi_5m_summaries (
    symbol TEXT NOT NULL,

    source_window_start_ms BIGINT NOT NULL,
    source_window_end_ms BIGINT NOT NULL,

    effective_market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    binance_timestamp_ms BIGINT NOT NULL,

    sum_open_interest NUMERIC(38, 18),
    sum_open_interest_value NUMERIC(38, 18),

    received_ms BIGINT NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, source_window_start_ms, source_window_end_ms),

    CHECK (source_window_end_ms = source_window_start_ms + 300000),
    CHECK (source_window_start_ms % 300000 = 0)
);

CREATE INDEX IF NOT EXISTS binance_futures_oi_5m_effective_market_idx
    ON binance_futures_oi_5m_summaries (effective_market_id);
```

Optional provider seed:

```sql
INSERT INTO providers (provider_code, display_name)
VALUES ('binance_usdm_perp', 'Binance USD-M Perpetual Futures')
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
    'binance_usdm_perp:BTCUSDT'
FROM providers
WHERE provider_code = 'binance_usdm_perp'
ON CONFLICT (provider_id, symbol) DO NOTHING;
```

---

## Add DB helpers

Add to `db.py`:

```python
async def upsert_binance_futures_snapshot(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    futures_last_price: Decimal | None,
    futures_last_price_time_ms: int | None,
    mark_price: Decimal | None,
    index_price: Decimal | None,
    last_funding_rate: Decimal | None,
    next_funding_time_ms: int | None,
    premium_index_time_ms: int | None,
    open_interest: Decimal | None,
    open_interest_time_ms: int | None,
    oi_notional_usdt: Decimal | None,
    premium_bps: Decimal | None,
    received_ms: int,
    raw: Mapping[str, Any],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_futures_snapshots (
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    futures_last_price,
                    futures_last_price_time_ms,
                    mark_price,
                    index_price,
                    last_funding_rate,
                    next_funding_time_ms,
                    premium_index_time_ms,
                    open_interest,
                    open_interest_time_ms,
                    oi_notional_usdt,
                    premium_bps,
                    received_ms,
                    raw
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9, $10, $11,
                    $12, $13,
                    $14, $15,
                    $16,
                    $17::jsonb
                )
                ON CONFLICT (symbol, sample_second_ms)
                DO UPDATE SET
                    futures_last_price = EXCLUDED.futures_last_price,
                    futures_last_price_time_ms = EXCLUDED.futures_last_price_time_ms,
                    mark_price = EXCLUDED.mark_price,
                    index_price = EXCLUDED.index_price,
                    last_funding_rate = EXCLUDED.last_funding_rate,
                    next_funding_time_ms = EXCLUDED.next_funding_time_ms,
                    premium_index_time_ms = EXCLUDED.premium_index_time_ms,
                    open_interest = EXCLUDED.open_interest,
                    open_interest_time_ms = EXCLUDED.open_interest_time_ms,
                    oi_notional_usdt = EXCLUDED.oi_notional_usdt,
                    premium_bps = EXCLUDED.premium_bps,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                futures_last_price,
                futures_last_price_time_ms,
                mark_price,
                index_price,
                last_funding_rate,
                next_funding_time_ms,
                premium_index_time_ms,
                open_interest,
                open_interest_time_ms,
                oi_notional_usdt,
                premium_bps,
                received_ms,
                json.dumps(raw, default=str),
            )
```

Add:

```python
async def upsert_binance_futures_oi_5m_summary(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    source_window_start_ms: int,
    source_window_end_ms: int,
    effective_window: MarketWindow,
    binance_timestamp_ms: int,
    sum_open_interest: Decimal | None,
    sum_open_interest_value: Decimal | None,
    received_ms: int,
    raw: Mapping[str, Any],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, effective_window)
            await connection.execute(
                """
                INSERT INTO binance_futures_oi_5m_summaries (
                    symbol,
                    source_window_start_ms,
                    source_window_end_ms,
                    effective_market_id,
                    binance_timestamp_ms,
                    sum_open_interest,
                    sum_open_interest_value,
                    received_ms,
                    raw
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                ON CONFLICT (symbol, source_window_start_ms, source_window_end_ms)
                DO UPDATE SET
                    effective_market_id = EXCLUDED.effective_market_id,
                    binance_timestamp_ms = EXCLUDED.binance_timestamp_ms,
                    sum_open_interest = EXCLUDED.sum_open_interest,
                    sum_open_interest_value = EXCLUDED.sum_open_interest_value,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                symbol,
                source_window_start_ms,
                source_window_end_ms,
                effective_window.market_id,
                binance_timestamp_ms,
                sum_open_interest,
                sum_open_interest_value,
                received_ms,
                json.dumps(raw, default=str),
            )
```

---

## Add collector

Create:

```text
price_collector/binance_futures_collector.py
```

Implementation behavior:

```text
1. Poll once per second.
2. Fetch:
   - /fapi/v1/openInterest
   - /fapi/v1/premiumIndex
   - /fapi/v2/ticker/price
3. Parse all numeric fields as Decimal.
4. Use openInterest.time as the primary source timestamp.
5. sample_second_ms = floor(openInterest.time to whole second.
6. market_id comes from market_for_sample_second(sample_second_ms).
7. Upsert one row per second into binance_futures_snapshots.
8. Do not use float.
9. Reuse current_utc_epoch_ms, sample_second_ms_for_now, seconds_until_next_utc_second, reconnect_delay_seconds, setup_logging.
```

Pseudo-code:

```python
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

import asyncio
import httpx

from price_collector.collector import (
    current_utc_epoch_ms,
    require_collector_database_url,
    sample_second_ms_for_now,
    seconds_until_next_utc_second,
    setup_logging,
)
from price_collector.config import Settings
from price_collector.db import (
    create_pool,
    upsert_binance_futures_snapshot,
)
from price_collector.market import market_for_sample_second


def dec_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    if not parsed.is_finite():
        return None
    return parsed


def int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


async def get_json(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    params: dict[str, Any],
) -> Mapping[str, Any]:
    response = await client.get(f"{base_url.rstrip('/')}{path}", params=params)
    response.raise_for_status()
    return response.json()


async def collect_once(
    *,
    pool,
    client: httpx.AsyncClient,
    settings: Settings,
) -> None:
    received_ms = current_utc_epoch_ms()
    symbol = settings.BINANCE_FUTURES_SYMBOL
    base_url = settings.BINANCE_FUTURES_BASE_URL

    oi = await get_json(
        client,
        base_url,
        "/fapi/v1/openInterest",
        {"symbol": symbol},
    )

    premium = await get_json(
        client,
        base_url,
        "/fapi/v1/premiumIndex",
        {"symbol": symbol},
    )

    ticker = await get_json(
        client,
        base_url,
        "/fapi/v2/ticker/price",
        {"symbol": symbol},
    )

    open_interest = dec_or_none(oi.get("openInterest"))
    open_interest_time_ms = int_or_none(oi.get("time"))

    mark_price = dec_or_none(premium.get("markPrice"))
    index_price = dec_or_none(premium.get("indexPrice"))
    last_funding_rate = dec_or_none(premium.get("lastFundingRate"))
    next_funding_time_ms = int_or_none(premium.get("nextFundingTime"))
    premium_index_time_ms = int_or_none(premium.get("time"))

    futures_last_price = dec_or_none(ticker.get("price"))
    futures_last_price_time_ms = int_or_none(ticker.get("time"))

    source_time_ms = open_interest_time_ms or received_ms
    sample_second_ms = (source_time_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    oi_notional_usdt = None
    if open_interest is not None and mark_price is not None:
        oi_notional_usdt = open_interest * mark_price

    premium_bps = None
    if mark_price is not None and index_price is not None and index_price > 0:
        premium_bps = (mark_price / index_price - Decimal("1")) * Decimal("10000")

    await upsert_binance_futures_snapshot(
        pool,
        symbol=symbol,
        window=window,
        sample_second_ms=sample_second_ms,
        futures_last_price=futures_last_price,
        futures_last_price_time_ms=futures_last_price_time_ms,
        mark_price=mark_price,
        index_price=index_price,
        last_funding_rate=last_funding_rate,
        next_funding_time_ms=next_funding_time_ms,
        premium_index_time_ms=premium_index_time_ms,
        open_interest=open_interest,
        open_interest_time_ms=open_interest_time_ms,
        oi_notional_usdt=oi_notional_usdt,
        premium_bps=premium_bps,
        received_ms=received_ms,
        raw={
            "openInterest": oi,
            "premiumIndex": premium,
            "ticker": ticker,
        },
    )


async def run_collector(settings: Settings) -> None:
    setup_logging(settings.LOG_LEVEL)

    pool = await create_pool(require_collector_database_url(settings))
    try:
        timeout = httpx.Timeout(settings.BINANCE_FUTURES_REST_TIMEOUT_SECONDS)
        async with httpx.AsyncClient(timeout=timeout) as client:
            while True:
                await asyncio.sleep(seconds_until_next_utc_second())
                try:
                    await collect_once(
                        pool=pool,
                        client=client,
                        settings=settings,
                    )
                except Exception:
                    # log exception and continue; do not kill the service for one bad REST call
                    LOGGER.exception("binance_futures_snapshot_failed")
    finally:
        await pool.close()
```

---

## Optional historical 5-minute OI collector loop

Inside the same collector, add a second loop:

```text
Every 30 seconds:
- GET /futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=2
- Take completed buckets only.
- Store them into binance_futures_oi_5m_summaries.
```

Important timestamp rule:

```text
The summary must retain its original Binance timestamp.
The summary must also store source_window_start_ms and source_window_end_ms.
Expose the summary on the next market as previous_5m_oi_summary.
```

If Codex is unsure whether Binance’s `timestamp` represents bucket start or bucket end, do this conservative rule:

```text
1. Store raw timestamp exactly.
2. Store timestamp_interpretation in raw JSON.
3. For download, label it previous_5m_oi_summary.
4. Do not use it as a per-second current OI sample.
```

---

## Extend download query

Change `fetch_market_download_payload` signature:

```python
async def fetch_market_download_payload(
    pool: asyncpg.Pool,
    *,
    market_id: int,
    include_probabilities: bool,
    include_futures: bool,
    include_oi: bool,
) -> Optional[dict[str, Any]]:
```

Extend the SQL with:

```sql
futures AS (
    SELECT *
    FROM binance_futures_snapshots
    WHERE market_id = $1
      AND symbol = 'BTCUSDT'
),
oi_prev AS (
    SELECT *
    FROM binance_futures_oi_5m_summaries
    WHERE effective_market_id = $1
      AND symbol = 'BTCUSDT'
    ORDER BY source_window_end_ms DESC
    LIMIT 1
),
oi_30 AS (
    SELECT
        f.sample_second_ms,
        prev.open_interest AS open_interest_30s_ago
    FROM futures f
    LEFT JOIN binance_futures_snapshots prev
      ON prev.symbol = f.symbol
     AND prev.sample_second_ms = f.sample_second_ms - 30000
),
oi_60 AS (
    SELECT
        f.sample_second_ms,
        prev.open_interest AS open_interest_60s_ago
    FROM futures f
    LEFT JOIN binance_futures_snapshots prev
      ON prev.symbol = f.symbol
     AND prev.sample_second_ms = f.sample_second_ms - 60000
),
oi_300 AS (
    SELECT
        f.sample_second_ms,
        prev.open_interest AS open_interest_300s_ago
    FROM futures f
    LEFT JOIN binance_futures_snapshots prev
      ON prev.symbol = f.symbol
     AND prev.sample_second_ms = f.sample_second_ms - 300000
)
```

Then select:

```sql
f.futures_last_price,
f.mark_price,
f.index_price,
f.last_funding_rate,
f.next_funding_time_ms,
f.open_interest,
f.oi_notional_usdt,
f.premium_bps,

(f.open_interest - oi_30.open_interest_30s_ago) AS oi_delta_30s,
(f.open_interest - oi_60.open_interest_60s_ago) AS oi_delta_60s,
(f.open_interest - oi_300.open_interest_300s_ago) AS oi_delta_300s,

oi_prev.source_window_start_ms AS prev_oi_source_window_start_ms,
oi_prev.source_window_end_ms AS prev_oi_source_window_end_ms,
oi_prev.sum_open_interest AS prev_oi_sum_open_interest,
oi_prev.sum_open_interest_value AS prev_oi_sum_open_interest_value
```

Join:

```sql
LEFT JOIN futures f ON f.sample_second_ms = s.sample_second_ms
LEFT JOIN oi_30 ON oi_30.sample_second_ms = s.sample_second_ms
LEFT JOIN oi_60 ON oi_60.sample_second_ms = s.sample_second_ms
LEFT JOIN oi_300 ON oi_300.sample_second_ms = s.sample_second_ms
LEFT JOIN oi_prev ON TRUE
```

---

## JSON builder rules

Keep database full precision.

For JSON:

```text
BTC prices: 2 decimals
futures prices: 2 decimals
premium_bps: 2 decimals
open_interest contracts: 3 decimals
open_interest notional USDT: 2 decimals
probability asks: 2 decimals
missing data: null
```

Add helpers:

```python
from decimal import Decimal, ROUND_HALF_UP


def decimal_fixed_or_none(
    value: Decimal | None,
    places: str,
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    quantum = Decimal(places)
    return format(value.quantize(quantum, rounding=ROUND_HALF_UP), "f")


def money_2dp(value: Decimal | None) -> str | None:
    return decimal_fixed_or_none(value, "0.01")


def oi_3dp(value: Decimal | None) -> str | None:
    return decimal_fixed_or_none(value, "0.001")
```

In each row:

```python
if include_futures:
    item["futures"] = {
        "last": money_2dp(row["futures_last_price"]),
        "mark": money_2dp(row["mark_price"]),
        "index": money_2dp(row["index_price"]),
        "premium_bps": money_2dp(row["premium_bps"]),
    }

if include_oi:
    item["open_interest"] = {
        "contracts": oi_3dp(row["open_interest"]),
        "notional_usdt": money_2dp(row["oi_notional_usdt"]),
        "delta_30s": oi_3dp(row["oi_delta_30s"]),
        "delta_60s": oi_3dp(row["oi_delta_60s"]),
        "delta_300s": oi_3dp(row["oi_delta_300s"]),
    }
```

At the market level:

```python
if include_oi and first["prev_oi_source_window_start_ms"] is not None:
    payload["previous_5m_oi_summary"] = {
        "source_window_start_ms": first["prev_oi_source_window_start_ms"],
        "source_window_end_ms": first["prev_oi_source_window_end_ms"],
        "effective_market_id": int(first["market_id"]),
        "sum_open_interest": oi_3dp(first["prev_oi_sum_open_interest"]),
        "sum_open_interest_value": money_2dp(first["prev_oi_sum_open_interest_value"]),
    }
```

---

## Update API parameters

Update routes:

```python
@app.get("/markets/current/data")
async def markets_current_data(
    request: Request,
    include_probabilities: bool = Query(False),
    include_futures: bool = Query(False),
    include_oi: bool = Query(False),
) -> dict[str, Any]:
```

Same for:

```text
/markets/{market_id}/data
/markets/current/download
/markets/{market_id}/download
```

Pass all flags into:

```python
fetch_market_download_payload(
    pool,
    market_id=market_id,
    include_probabilities=include_probabilities,
    include_futures=include_futures,
    include_oi=include_oi,
)
```

Download filename:

```python
parts = ["btc_5m_market", str(market_id)]

if include_futures:
    parts.append("futures")

if include_oi:
    parts.append("oi")

if include_probabilities:
    parts.append("probabilities")

filename = "_".join(parts) + ".json"
```

---

## Add systemd service

Create:

```text
/etc/systemd/system/price-collector-binance-futures.service
```

```ini
[Unit]
Description=Binance USD-M BTCUSDT futures and open interest collector
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pricecollector
Group=pricecollector
WorkingDirectory=/opt/price-collector
EnvironmentFile=/etc/price-collector/price-collector.env
ExecStart=/opt/price-collector/.venv/bin/python -m price_collector.binance_futures_collector

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
sudo systemctl enable price-collector-binance-futures
sudo systemctl start price-collector-binance-futures
```

---


My main recommendation: **collect current OI every second and put each snapshot into the market matching its own timestamp. Also expose Binance’s 5-minute historical OI as “previous 5m OI summary” on the next market. Add futures mark/index/last price as optional context, but do not make it part of the default Polymarket-resolution price feed.**

[1]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
[2]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Symbol-Price-Ticker-v2 "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
