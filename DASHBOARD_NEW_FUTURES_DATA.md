# Dashboard New Futures Data

Use this as a supplemental note for the separate dashboard repository. Leave the existing dashboard behavior unchanged unless these opt-in API flags are requested.

## New Data Available

The backend now stores compact per-second Binance USD-M futures microstructure data:

```text
binance_flow_1s = futures aggTrade flow aggregated to one row per second
binance_book_1s = futures bookTicker top-of-book snapshot, one row per second
```

Existing futures/OI snapshot data is still available:

```text
futures last price
mark price
index price
premium_bps
open_interest
oi_notional_usdt
oi_delta_30s / oi_delta_60s / oi_delta_300s
```

Raw futures JSON is not a dashboard feature. It is default-off for storage and should not be displayed.

## API Flags

Request the new data through the same market data endpoints:

```text
GET /price-api/markets/current/data?include_flow=true&include_book=true
GET /price-api/markets/{market_id}/data?include_flow=true&include_book=true
```

These flags can be combined with the existing flags:

```text
include_futures=true
include_oi=true
include_probabilities=true
fill_display=true
max_carry_forward_ms=10000
```

Example full current-market request:

```text
GET /price-api/markets/current/data?include_futures=true&include_oi=true&include_flow=true&include_book=true&fill_display=true&max_carry_forward_ms=10000
```

The dashboard must still use the FastAPI API through the local Vite proxy:

```text
/price-api/* -> http://127.0.0.1:9000/*
```

Do not connect the dashboard directly to PostgreSQL, Redis, Binance, or the droplet IP.

## Flow Fields

`binance_flow_1s` represents aggressive futures trade flow from `btcusdt@aggTrade`.

When `include_flow=true`, each row may contain:

```js
row.flow
row.freshness.futures_flow
```

Suggested dashboard fields:

| UI label | Backend field | Meaning |
| --- | --- | --- |
| Buy flow | `buy_quote` | Aggressive buyer notional in USDT for the second |
| Sell flow | `sell_quote` | Aggressive seller notional in USDT for the second |
| Net flow | `delta_quote` | `buy_quote - sell_quote` |
| Total flow | `total_quote` | `buy_quote + sell_quote` |
| Taker imbalance | `taker_imbalance` | `delta_quote / total_quote`, range `-1` to `1` |
| CVD | `cvd_quote` | Cumulative sum of `delta_quote` since collector start |
| CVD 10s | `cvd_10s` | Rolling 10-second sum of `delta_quote` |
| CVD 30s | `cvd_30s` | Rolling 30-second sum of `delta_quote` |
| Imbalance 10s | `imbalance_10s` | Rolling 10-second flow imbalance |
| Imbalance 30s | `imbalance_30s` | Rolling 30-second flow imbalance |
| Agg trades | `agg_trade_count` | Number of aggregate trade messages in the second |
| Trades | `trade_count` | Underlying trade count from aggTrade first/last IDs |
| Largest trade | `max_trade_quote` | Largest aggregate trade notional in the second |

Useful visualizations:

- A red/green histogram for `delta_quote`.
- A line chart for `cvd_10s` and `cvd_30s`.
- A compact gauge or signed value for `taker_imbalance`.
- A volume bar for `total_quote`.
- Small quality badges for `agg_trade_count`, `trade_count`, and stale source time.

## Book Fields

`binance_book_1s` represents the latest futures top-of-book snapshot for each second from `btcusdt@bookTicker`.

When `include_book=true`, each row may contain:

```js
row.book
row.freshness.futures_book
```

Suggested dashboard fields:

| UI label | Backend field | Meaning |
| --- | --- | --- |
| Bid | `bid` | Best bid price |
| Ask | `ask` | Best ask price |
| Bid size | `bid_qty` | Quantity available at best bid |
| Ask size | `ask_qty` | Quantity available at best ask |
| Mid | `mid` | `(bid + ask) / 2` |
| Spread | `spread` | `ask - bid` |
| Spread bps | `spread_bps` | Spread normalized by mid price |
| Book imbalance | `book_imbalance` | `(bid_qty - ask_qty) / (bid_qty + ask_qty)` |
| Microprice | `microprice` | Top-of-book pressure-weighted price |

Useful visualizations:

- A spread chart using `spread_bps`.
- A signed bar or gauge for `book_imbalance`.
- A small bid/ask ladder row showing `bid`, `ask`, `bid_qty`, and `ask_qty`.
- A microprice-vs-mid line or tiny delta display.

## Best Combined Views

The most useful new dashboard section is a per-second futures pressure panel for the current 5-minute market:

| Panel | Fields |
| --- | --- |
| Aggressive flow | `delta_quote`, `total_quote`, `taker_imbalance` |
| Rolling pressure | `cvd_10s`, `cvd_30s`, `imbalance_10s`, `imbalance_30s` |
| Top of book | `spread_bps`, `book_imbalance`, `microprice`, `mid` |
| Position context | existing `open_interest`, `oi_delta_30s`, `oi_delta_60s`, `oi_delta_300s` |
| Price context | existing spot price, futures last, mark, index, `premium_bps` |

Use these comparisons:

- Strong positive `delta_quote` plus positive `book_imbalance`: aggressive buyers and bid support.
- Strong positive `delta_quote` plus negative `book_imbalance`: buyers hitting a thin or ask-heavy book.
- Rising `cvd_30s` plus rising open interest: likely new long positioning.
- Rising price plus falling open interest: possible short closing rather than new longs.
- High `spread_bps`: lower confidence in small price moves.

## Display Rules

- Keep decimal strings as strings at the API boundary.
- Convert values to `Number` only for chart rendering and UI math.
- Treat `null` imbalance fields as no-data, usually because the denominator was zero.
- Leave chart gaps where values are missing.
- Use `sample_second_ms` for chart x-axis alignment.
- Use `market_id` to align these rows with the existing 5-minute market view.
- Prefer signed colors for flow and imbalance: positive green, negative red, neutral gray.

## What Not To Add

- Do not add spot aggTrade UI yet.
- Do not add liquidation UI yet.
- Do not display raw JSON.
- Do not build direct Binance websocket subscriptions in the dashboard.
- Do not add a separate database connection from the dashboard.
