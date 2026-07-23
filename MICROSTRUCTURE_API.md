# Microstructure API Guide

This guide explains the microstructure API additions, how to call them, what
each response means, and how a dashboard should combine the live and historical
paths.

## What Was Added

There are three new ways to request microstructure data:

| Purpose | Request | Storage used |
| --- | --- | --- |
| Latest finalized second | `GET /markets/current/microstructure/live` | Redis only |
| Current five-minute market | `GET /markets/current/data?include_microstructure=true` | PostgreSQL |
| A specific five-minute market | `GET /markets/{market_id}/data?include_microstructure=true` | PostgreSQL |

The architecture intentionally separates the fast live value from history:

```text
Binance streams
      |
      v
one-second causal aggregator
      |-- Redis: only the latest finalized row
      `-- PostgreSQL: durable historical source of record

Dashboard live poll ----> Redis endpoint
Dashboard market load --> PostgreSQL data endpoint
```

The existing `GET /markets/current/live` endpoint is unchanged. It remains the
small, isolated live-price response.

## Connecting to the Private API

The API listens only on `127.0.0.1:9000` on the droplet. Do not expose port
`9000` publicly.

When running a command on the droplet:

```bash
export API_BASE_URL=http://127.0.0.1:9000
```

When calling it from another computer, first open an SSH tunnel and leave that
terminal running:

```bash
ssh -N -L 9000:127.0.0.1:9000 root@152.42.247.86
```

Then, in a second local terminal:

```bash
export API_BASE_URL=http://127.0.0.1:9000
```

PowerShell equivalent:

```powershell
$env:API_BASE_URL = "http://127.0.0.1:9000"
```

## 1. Latest Finalized Microstructure Second

### Request

```http
GET /markets/current/microstructure/live
```

There are no query parameters.

```bash
curl --compressed \
  "${API_BASE_URL}/markets/current/microstructure/live"
```

Browser example:

```javascript
const response = await fetch(
  `${API_BASE_URL}/markets/current/microstructure/live`
);

if (!response.ok) {
  throw new Error(`Microstructure request failed: ${response.status}`);
}

const live = await response.json();
```

### What It Does

The endpoint performs one Redis `MGET` for:

- Binance Spot price
- Chainlink price
- Binance futures last price
- Latest finalized microstructure row

It does not query PostgreSQL and does not return five minutes of history.
Redis contains only the newest finalized microstructure second.

### Response

This example shows the full response shape. Values are illustrative.

```json
{
  "schema_version": 1,
  "server_time_ms": 1784774594250,
  "market_id": 5949248,
  "sample_second_ms": 1784774593000,
  "served_from": "redis",
  "prices": {
    "binance_spot": "65758.01",
    "chainlink": "65721.23639093849",
    "futures": "65723.70"
  },
  "microstructure": {
    "collector_healthy": true,
    "books": {
      "spot": {
        "bid": "65757.90",
        "ask": "65758.10",
        "mid": "65758.00",
        "spread_bps": "0.0304",
        "imbalance_1": "0.15",
        "imbalance_5": "0.09",
        "imbalance_10": "0.04",
        "bid_depth_usdt_10": "1254300.10",
        "ask_depth_usdt_10": "1189200.50",
        "weighted_mid_offset_bps": "0.006",
        "bbo_ofi_usdt": "15200.40",
        "snapshot_count": 2
      },
      "futures": {
        "bid": "65723.60",
        "ask": "65723.70",
        "mid": "65723.65",
        "spread_bps": "0.0152",
        "imbalance_1": "0.17",
        "imbalance_5": "0.11",
        "imbalance_10": "0.08",
        "bid_depth_usdt_10": "1432000.20",
        "ask_depth_usdt_10": "1328000.90",
        "weighted_mid_offset_bps": "0.004",
        "bbo_ofi_usdt": "18450.25",
        "snapshot_count": 2
      }
    },
    "flow": {
      "spot_buy_usdt": "90450.10",
      "spot_sell_usdt": "81200.25",
      "futures_buy_usdt": "120400.15",
      "futures_sell_usdt": "110200.80",
      "futures_rpi_buy_usdt": "4200.10",
      "futures_rpi_sell_usdt": "3100.20",
      "spot_trade_id_span": 42,
      "spot_aggtrade_count": 18,
      "spot_max_aggtrade_usdt": "11000.50",
      "spot_vwap": "65757.98",
      "spot_trade_high": "65758.20",
      "spot_trade_low": "65757.60",
      "spot_last_trade": "65758.01",
      "futures_trade_id_span": 51,
      "futures_aggtrade_count": 23,
      "futures_max_aggtrade_usdt": "14300.25",
      "futures_vwap": "65723.66",
      "futures_trade_high": "65723.90",
      "futures_trade_low": "65723.30",
      "futures_last_trade": "65723.70"
    },
    "cross_market": {
      "perp_spot_basis_bps": "-5.22",
      "spot_futures_book_skew_ms": 41,
      "mark_price": "65723.62",
      "index_price": "65721.24",
      "mark_index_basis_bps": "0.3621",
      "funding_rate": "0.00010000",
      "seconds_to_funding": 7123,
      "open_interest_btc": "74321.123",
      "open_interest_usdt": "4885093456.12"
    },
    "liquidations": {
      "observed_long_usdt": "0",
      "observed_short_usdt": "12500.20",
      "snapshot_count": 1
    },
    "quality": {
      "schema_version": 1,
      "sample_span_ms": 1000,
      "sample_jitter_ms": 251,
      "spot_book_age_ms": 911,
      "spot_book_lag_ms": 33,
      "futures_book_age_ms": 363,
      "futures_book_lag_ms": 27,
      "spot_trade_age_ms": 147,
      "spot_trade_lag_mean_ms": "42.25",
      "spot_trade_lag_max_ms": 81,
      "futures_trade_age_ms": 346,
      "futures_trade_lag_mean_ms": "38.50",
      "futures_trade_lag_max_ms": 75,
      "mark_age_ms": 621,
      "mark_lag_ms": 91,
      "open_interest_age_ms": 621,
      "open_interest_exchange_age_ms": 702,
      "open_interest_http_lag_ms": 87,
      "liquidation_lag_mean_ms": "54.00",
      "connection_errors": 0,
      "received_ms": 1784774593251
    }
  }
}
```

### Live Response Meaning

| Field | Meaning |
| --- | --- |
| `schema_version` | Version of this live response contract |
| `server_time_ms` | API server time when the response was created |
| `market_id` | Five-minute market containing the finalized snapshot |
| `sample_second_ms` | Start of the finalized one-second receipt interval |
| `served_from` | Always `"redis"` for this endpoint |
| `prices` | Latest independently cached source prices |
| `microstructure` | The latest finalized row, or `null` if one is unavailable |

The interval represented by a row is:

```text
[sample_second_ms, sample_second_ms + 1000)
```

The collector normally publishes the row shortly after that interval closes.
This is a low-latency path to finalized one-second data, not a subsecond feed.

The Redis key can remain present if the collector stops producing rows.
`collector_healthy` describes the finalized interval and does not change while
an old cached row remains. Calculate current live staleness separately:

```javascript
const snapshotAgeMs =
  live.sample_second_ms === null
    ? null
    : Math.max(
        0,
        live.server_time_ms - (live.sample_second_ms + 1000)
      );
```

Use `snapshotAgeMs`, `collector_healthy`, and the `quality` fields together
when deciding whether to display, dim, or exclude a live point.

If no microstructure snapshot exists, the endpoint still returns HTTP `200`:

```json
{
  "schema_version": 1,
  "server_time_ms": 1784774594250,
  "market_id": 5949248,
  "sample_second_ms": null,
  "served_from": "redis",
  "prices": {
    "binance_spot": "65758.01",
    "chainlink": "65721.23639093849",
    "futures": "65723.70"
  },
  "microstructure": null
}
```

A missing source-price key makes only that price `null`.

## 2. Current Market History

### Request

```http
GET /markets/current/data?include_microstructure=true
```

```bash
curl --compressed \
  "${API_BASE_URL}/markets/current/data?include_microstructure=true"
```

This reads the current five-minute market from PostgreSQL and adds
microstructure to the existing 300-second grid. The response schema becomes
version `3`.

You can combine this flag with the existing data-route flags:

```bash
curl --compressed \
  "${API_BASE_URL}/markets/current/data?include_microstructure=true&include_futures=true&include_probabilities=true"
```

## 3. A Specific Market's History

### Request

```http
GET /markets/{market_id}/data?include_microstructure=true
```

Example:

```bash
curl --compressed \
  "${API_BASE_URL}/markets/5949248/data?include_microstructure=true"
```

This has the same shape as the current-market response but reads the requested
five-minute window.

## Selecting Smaller Response Groups

By default, historical requests include all five groups:

```text
books,flow,cross_market,liquidations,quality
```

Use `microstructure_groups` to request a subset:

```bash
curl --compressed \
  "${API_BASE_URL}/markets/current/data?include_microstructure=true&microstructure_groups=books,flow,liquidations,quality"
```

Or for a past market:

```bash
curl --compressed \
  "${API_BASE_URL}/markets/5949248/data?include_microstructure=true&microstructure_groups=books,quality"
```

Rules:

- Group names are case-sensitive.
- Separate names with commas.
- `collector_healthy` is always included on matched rows.
- Group filtering does not change the availability counts.
- Empty or unknown group names return HTTP `422`.
- Sending `microstructure_groups` without `include_microstructure=true`
  returns HTTP `422`.
- The Redis live endpoint always returns all groups and does not accept this
  parameter.

## Historical Response

The following is an abbreviated response to a request using
`microstructure_groups=books,quality`. The real `series` array contains 300
one-second rows.

```json
{
  "schema_version": 3,
  "server_time_ms": 1784774594250,
  "market": {
    "market_id": 5949248,
    "market_start_ms": 1784774400000,
    "market_end_ms": 1784774700000,
    "seconds_expected": 300
  },
  "availability": {
    "microstructure_rows": 298,
    "microstructure_healthy_rows": 295,
    "microstructure_missing_seconds": 2
  },
  "series": [
    {
      "t": 0,
      "timestamp_ms": 1784774400000,
      "prices": {
        "binance": "65740.10",
        "chainlink": "65738.82"
      },
      "microstructure": {
        "collector_healthy": true,
        "books": {
          "spot": {
            "bid": "65739.90",
            "ask": "65740.10",
            "mid": "65740.00",
            "spread_bps": "0.0304",
            "imbalance_1": "0.15",
            "imbalance_5": "0.09",
            "imbalance_10": "0.04",
            "bid_depth_usdt_10": "1254300.10",
            "ask_depth_usdt_10": "1189200.50",
            "weighted_mid_offset_bps": "0.006",
            "bbo_ofi_usdt": "15200.40",
            "snapshot_count": 2
          },
          "futures": {
            "bid": "65739.70",
            "ask": "65739.90",
            "mid": "65739.80",
            "spread_bps": "0.0304",
            "imbalance_1": "0.17",
            "imbalance_5": "0.11",
            "imbalance_10": "0.08",
            "bid_depth_usdt_10": "1432000.20",
            "ask_depth_usdt_10": "1328000.90",
            "weighted_mid_offset_bps": "0.004",
            "bbo_ofi_usdt": "18450.25",
            "snapshot_count": 2
          }
        },
        "quality": {
          "schema_version": 1,
          "sample_span_ms": 1000,
          "sample_jitter_ms": 251,
          "spot_book_age_ms": 911,
          "spot_book_lag_ms": 33,
          "futures_book_age_ms": 363,
          "futures_book_lag_ms": 27,
          "spot_trade_age_ms": 147,
          "spot_trade_lag_mean_ms": "42.25",
          "spot_trade_lag_max_ms": 81,
          "futures_trade_age_ms": 346,
          "futures_trade_lag_mean_ms": "38.50",
          "futures_trade_lag_max_ms": 75,
          "mark_age_ms": 621,
          "mark_lag_ms": 91,
          "open_interest_age_ms": 621,
          "open_interest_exchange_age_ms": 702,
          "open_interest_http_lag_ms": 87,
          "liquidation_lag_mean_ms": "54.00",
          "connection_errors": 0,
          "received_ms": 1784774401251
        }
      }
    },
    {
      "t": 1,
      "timestamp_ms": 1784774401000,
      "prices": {
        "binance": "65740.20",
        "chainlink": "65738.82"
      },
      "microstructure": null
    }
  ]
}
```

The existing data response contains additional market, availability, price,
freshness, futures, probability, flow, and book fields depending on the other
query flags. Microstructure is added without removing those fields.

### Availability

| Field | Meaning |
| --- | --- |
| `microstructure_rows` | Stored rows matched to the selected 300-second grid |
| `microstructure_healthy_rows` | Matched rows with `collector_healthy=true` |
| `microstructure_missing_seconds` | `seconds_expected - microstructure_rows` |

An active market is incomplete by definition, so missing seconds are normal
until the market ends.

An existing older market that predates microstructure collection, or whose
microstructure rows are outside the configured retention window, still returns
HTTP `200`. Its availability is:

```json
{
  "microstructure_rows": 0,
  "microstructure_healthy_rows": 0,
  "microstructure_missing_seconds": 300
}
```

Each of its `series` rows has:

```json
{
  "microstructure": null
}
```

The API returns `404` only if the requested market itself does not exist, not
because its microstructure data is missing.

## Microstructure Groups

### `books`

Contains independent Spot and futures top-10 book summaries:

- `bid`, `ask`, and `mid`
- `spread_bps`
- `imbalance_1`, `imbalance_5`, and `imbalance_10`
- `bid_depth_usdt_10` and `ask_depth_usdt_10`
- `weighted_mid_offset_bps`
- `bbo_ofi_usdt`
- `snapshot_count`

### `flow`

Contains one-second observed aggregate-trade flow:

- Spot and futures buy/sell USDT notionals
- Futures RPI buy/sell USDT notionals
- Trade-ID spans and aggregate-trade counts
- Largest aggregate-trade notionals
- VWAP, high, low, and last trade for Spot and futures

### `cross_market`

Contains relationships and slower futures state:

- Perpetual-versus-Spot basis
- Spot/futures book observation skew
- Mark and index prices
- Mark/index basis
- Funding rate and seconds until funding
- Open interest in BTC and USDT

### `liquidations`

Contains forced-order events observed during the interval:

- `observed_long_usdt`
- `observed_short_usdt`
- `snapshot_count`

These are censored observations from Binance's forced-order feed. They are not
total market liquidations and must not be used as predicted future liquidation
levels.

### `quality`

Contains the evidence needed to decide whether a row is trustworthy:

- Schema, interval span, and flush jitter
- Book/trade ages and source lags
- Mark and open-interest ages/lags
- Liquidation lag
- Connection-error count
- Row finalization receive time

Rows are never hidden merely because they are unhealthy.

## Value and Null Semantics

- Financial values are JSON strings so decimal precision is preserved.
- Do not convert financial strings to binary floating point when exact
  calculations matter. Use a decimal library.
- Counts, timestamps, ages, and whole-millisecond lags are JSON integers.
- Missing or unknown values are `null`; the API does not invent zeroes.
- A numeric zero can be legitimate when no event was observed in an interval.
- When `collector_healthy=false`, zero flow or liquidation values are not proof
  that market activity was zero. Treat the interval as untrusted.

Example:

```javascript
import Decimal from "decimal.js";

const spotBuy = new Decimal(row.microstructure.flow.spot_buy_usdt);
const spotSell = new Decimal(row.microstructure.flow.spot_sell_usdt);
const netSpotFlow = spotBuy.minus(spotSell);
```

## Errors

| Status | Meaning |
| --- | --- |
| `200` | Request succeeded, including when microstructure is absent |
| `404` | Requested market window does not exist |
| `422` | Invalid query parameter or microstructure group selection |
| `503` | Redis is unavailable or contains an invalid live payload |

Redis connection/read failure:

```json
{
  "detail": "live cache unavailable"
}
```

Invalid cached payload:

```json
{
  "detail": "live cache payload invalid"
}
```

## Recommended Dashboard Flow

For an active five-minute chart:

1. Load the current PostgreSQL grid once:

   ```http
   GET /markets/current/data?include_microstructure=true
   ```

2. Index the returned rows by `timestamp_ms`.
3. Poll the Redis endpoint about once per second:

   ```http
   GET /markets/current/microstructure/live
   ```

4. Ignore a live response whose `sample_second_ms` is `null`.
5. Append a new row or replace the existing row with the same
   `sample_second_ms`.
6. Check current staleness, `collector_healthy`, and quality fields before
   styling or using the row in calculations.
7. When `market_id` changes, fetch the completed market once from PostgreSQL:

   ```http
   GET /markets/{completed_market_id}/data?include_microstructure=true
   ```

8. Load the new active market and continue polling.

Do not ask Redis for five-minute history and do not cache past markets there.
PostgreSQL remains the source of record.

## Important Limitations

- Historical microstructure responses are limited to the 300 seconds in one
  five-minute market; there is no pagination.
- Microstructure history is durable PostgreSQL data, but it is still subject to
  `BINANCE_MICROSTRUCTURE_RETENTION_DAYS`, which defaults to 30 days.
- The `/download` routes do not include microstructure and do not accept
  `include_microstructure` or `microstructure_groups`.
- The live endpoint does not support group filtering.
- Live microstructure is finalized once per second; Redis reduces API latency
  but does not make the underlying observations subsecond.
- Browser `fetch` accepts HTTP compression automatically. Use
  `curl --compressed` for command-line requests.
