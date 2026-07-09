For 10–30 second BTC moves, I would **not** collect “everything.” I’d collect a **small per-second feature set** from Binance trades + top-of-book + your existing futures/OI data.

The core idea: **read high-frequency Binance streams, aggregate in memory, store only 1 row per second**. Keep raw messages only temporarily for debugging.

## My recommendation

### 1. Collect CVD/taker imbalance from `btcusdt@aggTrade`, not Binance’s REST taker ratio

Use **Binance USD-M futures `btcusdt@aggTrade`** as the primary flow source. Binance says futures aggregate trade streams push market-trade information grouped by same price and taking side every 100ms, with `p` price, `q` quantity, `T` trade time, and `m` = “is the buyer the market maker?” ([Binance Developers][1])

Interpretation:

```text
m = false  => buyer was taker  => aggressive buy
m = true   => seller was taker => aggressive sell
```

So per message:

```python
quote_notional = price * quantity

if m is False:
    buy_quote += quote_notional
else:
    sell_quote += quote_notional
```

Then per second:

```text
delta_quote      = buy_quote - sell_quote
total_quote      = buy_quote + sell_quote
taker_imbalance  = delta_quote / total_quote
cvd_quote        = cumulative sum(delta_quote)
cvd_10s          = rolling 10s sum(delta_quote)
cvd_30s          = rolling 30s sum(delta_quote)
```

Do **not** rely on Binance’s REST taker buy/sell volume endpoint for 10–30s work: the documented periods start at `5m`, not seconds, and it returns `buyVol`, `sellVol`, and `buySellRatio` at those coarser intervals. ([Binance Developers][2])

### 2. Store per-second flow, not raw trades

Create one compact table like this:

```sql
CREATE TABLE IF NOT EXISTS binance_flow_1s (
    venue TEXT NOT NULL,                 -- binance_usdm_perp or binance_spot
    symbol TEXT NOT NULL,                -- BTCUSDT
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    buy_base DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_base DOUBLE PRECISION NOT NULL DEFAULT 0,
    buy_quote DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_quote DOUBLE PRECISION NOT NULL DEFAULT 0,

    delta_quote DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_quote DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_imbalance DOUBLE PRECISION,

    agg_trade_count INTEGER NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    max_trade_quote DOUBLE PRECISION,

    first_agg_trade_id BIGINT,
    last_agg_trade_id BIGINT,
    last_trade_time_ms BIGINT,
    last_event_time_ms BIGINT,
    received_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (venue, symbol, sample_second_ms),
    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000)
);

CREATE INDEX IF NOT EXISTS binance_flow_1s_market_idx
    ON binance_flow_1s (market_id, venue, symbol, sample_second_ms);
```

This fits your existing style: your current schema already keys price samples by `sample_second_ms`, validates 1-second alignment, and maps each row into a 5-minute `market_id`.  Your existing futures table also uses `(symbol, sample_second_ms)` and stores futures price, mark, index, OI, premium, and raw JSON per second. 

I would use `DOUBLE PRECISION` for flow features. These are analytics features, not accounting records. Using `NUMERIC(38,18)` everywhere will cost more disk and CPU.

### 3. Start with futures flow first; add spot flow second

For short BTC moves, I would start with:

```text
binance_usdm_perp BTCUSDT aggTrade
```

Then add:

```text
binance_spot BTCUSDT aggTrade
```

Why this order: the BTC perpetual market often leads short-term moves because of leverage, liquidations, and derivatives positioning. Spot flow is still useful, but I would not collect 10 venues or 20 symbols yet.

Your current spot collector only stores one price sample per second from `btcusdt@ticker`, and your config currently points at `wss://stream.binance.com:9443/ws/btcusdt@ticker`.  For CVD, replace or supplement that with `btcusdt@aggTrade`; ticker alone cannot tell you buy/sell pressure.

### 4. Add top-of-book once per second

CVD tells you **aggressive flow**. It does not tell you whether the book is thin, skewed, or easy to push. Add one per-second top-of-book snapshot from Binance futures `btcusdt@bookTicker`.

Binance’s futures book ticker stream pushes best bid/ask price and quantity for a symbol in real time, with fields for best bid price/qty and best ask price/qty. ([Binance Developers][3])

Store only the latest top-of-book per second:

```sql
CREATE TABLE IF NOT EXISTS binance_book_1s (
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    bid DOUBLE PRECISION NOT NULL,
    ask DOUBLE PRECISION NOT NULL,
    bid_qty DOUBLE PRECISION NOT NULL,
    ask_qty DOUBLE PRECISION NOT NULL,

    mid DOUBLE PRECISION NOT NULL,
    spread DOUBLE PRECISION NOT NULL,
    spread_bps DOUBLE PRECISION NOT NULL,
    book_imbalance DOUBLE PRECISION,
    microprice DOUBLE PRECISION,

    update_id BIGINT,
    event_time_ms BIGINT,
    transaction_time_ms BIGINT,
    received_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (venue, symbol, sample_second_ms)
);
```

Derived values:

```text
mid             = (bid + ask) / 2
spread          = ask - bid
spread_bps      = spread / mid * 10000
book_imbalance  = (bid_qty - ask_qty) / (bid_qty + ask_qty)
microprice      = (ask * bid_qty + bid * ask_qty) / (bid_qty + ask_qty)
```

This is much cheaper than storing depth updates. I would **not** collect full depth diffs yet. Binance supports futures depth streams at 100ms/250ms/500ms and partial depth with 5/10/20 levels, but that creates much more data and more complicated reconstruction logic. ([Binance Developers][3])

### 5. Keep your OI/futures snapshot, but trim raw JSON

Your current futures collector polls `/fapi/v1/openInterest`, `/fapi/v1/premiumIndex`, and `/fapi/v2/ticker/price` and writes the result as a futures snapshot.  It also derives OI notional and premium bps from open interest, mark price, and index price. 

That is already useful. For 10–30s moves, I would keep:

```text
futures_last_price
mark_price
index_price
premium_bps
open_interest
oi_notional_usdt
oi_delta_30s
oi_delta_60s
oi_delta_300s
```

Your API already exports futures fields and OI deltas at 30s, 60s, and 300s when `include_futures` / `include_oi` are enabled.  The query already computes those OI deltas by joining against snapshots 30s, 60s, and 300s back. 

But I would **stop storing `raw JSONB` forever** in the futures snapshot table. Your current table has `raw JSONB` on per-second futures rows.  That is fine for debugging, but it is a bad long-term use of a 50GB database. Keep raw for 24–72 hours, or make it nullable and only populate it on parse errors / debug mode.

### 6. Optional: collect liquidations, but only aggregated per second

Liquidations are useful around sudden 10–30s moves, but they are sparse. Binance’s current USD-M futures websocket migration page lists `<symbol>@forceOrder` as the liquidation stream. ([Binance Developers][4])

Store per second:

```text
long_liq_quote
short_liq_quote
liq_count
max_liq_quote
```

Do not store every liquidation message forever unless you later prove it helps.

## Minimal feature set I would actually store

For each second:

| Group        | Store                                                            | Why                                               |
| ------------ | ---------------------------------------------------------------- | ------------------------------------------------- |
| Price        | spot price, futures price, mark, index, premium_bps              | Basic movement + futures premium                  |
| Flow         | buy_quote, sell_quote, delta_quote, total_quote, taker_imbalance | Core CVD/taker imbalance                          |
| Rolling flow | cvd_10s, cvd_30s, imbalance_10s, imbalance_30s                   | Directly matches your 10–30s horizon              |
| Book         | bid, ask, spread_bps, book_imbalance, microprice                 | Tells whether flow can move price                 |
| OI           | open_interest, oi_notional, oi_delta_30s/60s/300s                | Helps distinguish new position opening vs closing |
| Quality      | agg_count, first/last agg id, source time, received time         | Detect gaps/stale data                            |

That is enough. I would not add funding, long/short ratios, all-depth, all symbols, all liquidation streams, or full raw trades yet.

## Disk-budget estimate

At 1 row/second:

```text
86,400 rows/day/table
```

A compact per-second flow table plus indexes might be roughly **20–60 MB/day**, depending on PostgreSQL overhead, indexes, and numeric types. A few per-second tables for flow + book + futures/OI may land around **100–250 MB/day**. With 50GB, that gives roughly **200–500 days** if you avoid raw JSON and avoid raw trades.

Raw trades are the danger. Even `aggTrade` raw messages for BTCUSDT can become large quickly, and depth streams are worse. The right pattern is:

```text
websocket messages -> in-memory 1s aggregator -> compact DB row
```

Not:

```text
websocket messages -> raw DB table forever
```

## Implementation pattern

Use two in-memory buckets keyed by `sample_second_ms`:

```python
second_ms = (trade_time_ms // 1000) * 1000
bucket = buckets[second_ms]

price = Decimal(payload["p"])
qty = Decimal(payload["q"])
notional = price * qty

buyer_is_maker = payload["m"]
seller_is_taker = buyer_is_maker

if seller_is_taker:
    bucket.sell_base += qty
    bucket.sell_quote += notional
else:
    bucket.buy_base += qty
    bucket.buy_quote += notional

bucket.agg_trade_count += 1
bucket.trade_count += int(payload["l"]) - int(payload["f"]) + 1
bucket.first_agg_trade_id = min(...)
bucket.last_agg_trade_id = max(...)
bucket.last_trade_time_ms = max(bucket.last_trade_time_ms, payload["T"])
bucket.last_event_time_ms = max(bucket.last_event_time_ms, payload["E"])
```

Flush seconds only after a short delay:

```text
flush any bucket where sample_second_ms <= now_ms - 1500
```

That gives late websocket messages a chance to arrive.

## Current Binance URL note

For USD-M futures, Binance’s newer docs show split websocket routes. Aggregate trades are under the market route, for example:

```text
wss://fstream.binance.com/market/ws/btcusdt@aggTrade
```

Book ticker is under the public route:

```text
wss://fstream.binance.com/public/ws/btcusdt@bookTicker
```

Binance says the legacy futures websocket URLs are no longer available after April 23, 2026 and that integrations should use `/public`, `/market`, and `/private` routes. ([Binance Developers][1])

## Priority order

1. **Futures `aggTrade` → `binance_flow_1s`**
2. **Futures `bookTicker` → `binance_book_1s`**
3. **Keep your existing futures/OI snapshots, but remove long-term raw JSON**

do not add number 4 or 5
4. **Add spot `aggTrade` only after futures flow is stable**(dont do this)
5. **Add liquidation-per-second only if you see it helps around sharp moves**(dont do this)

This should give you a clean, interpretable dataset for 10–30 second BTC movement without filling the database with noise.

[1]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Contract-Info-Stream "Market - Futures (USDⓈ-M) WebSocket Market Streams | Binance Developer Docs"
[2]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
[3]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams "Public - Futures (USDⓈ-M) WebSocket Market Streams | Binance Developer Docs"
[4]: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice "Important WebSocket Change Notice — Base URL Split & Migration | Binance Developer Docs"
