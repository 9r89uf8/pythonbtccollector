# Frontend FastAPI Reference

This is the frontend-facing contract for the read-only FastAPI application in
`price_collector/api.py`. It covers every application data endpoint, how to
call it, and the fields returned to a dashboard.

## Access and Base URL

The production API listens only on the droplet at `127.0.0.1:9000`. It is not
public and has no application-level authentication. Network access is the
security boundary.

From the droplet, the base URL is:

```text
http://127.0.0.1:9000
```

From another machine, first open the SSH tunnel documented in `OPERATIONS.md`:

```bash
ssh -N -L "${LOCAL_API_PORT}:127.0.0.1:9000" "${DROPLET_USER}@${DROPLET_IP}"
```

Then use this base URL while that tunnel remains open:

```text
http://127.0.0.1:${LOCAL_API_PORT}
```

For example, set it once for the `curl` calls in this document:

```bash
API_BASE_URL="http://127.0.0.1:${LOCAL_API_PORT}"
curl "${API_BASE_URL}/markets/current/live"
```

The API does not currently install CORS middleware. A browser dashboard should
call it through a same-origin backend or development proxy. Point that proxy at
the tunneled base URL; do not expose port `9000` publicly just to avoid CORS.

A reusable browser call can look like this:

```javascript
const API_BASE_URL = "/api"; // Same-origin proxy to 127.0.0.1:${LOCAL_API_PORT}

async function apiGet(path, query = {}) {
  const url = new URL(`${API_BASE_URL}${path}`, window.location.origin);
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url, { headers: { Accept: "application/json" } });
  const body = await response.json();
  if (!response.ok) {
    throw new Error(body.detail ?? body.error ?? `HTTP ${response.status}`);
  }
  return body;
}

const live = await apiGet("/markets/current/live");
```

## Data Conventions

- All application routes use `GET`; there is no mutation endpoint.
- Prices and other financial decimals are JSON strings, or `null` when data is
  unavailable. Keep them as strings or parse them with a decimal library; do
  not use binary floating-point for financial calculations.
- Fields ending in `_ms` are UTC epoch milliseconds.
- Fields ending in `_at` are UTC ISO 8601 strings ending in `Z`.
- A market is a half-open five-minute UTC window. Its identifier is
  `market_start_ms / 300000`.
- A `current` route uses the API server's current five-minute window. A route
  with `{market_id}` addresses a specific historical or current window.
- A successful JSON response uses HTTP `200` unless stated otherwise.
- Missing observations inside a market series are represented by `null`; they
  are not fabricated.

## Endpoint Summary

| Method | Path | Dashboard use | Backing store |
| --- | --- | --- | --- |
| `GET` | `/healthz` | API/database health | PostgreSQL |
| `GET` | `/prices/latest` | Latest stored price for one source | PostgreSQL |
| `GET` | `/markets` | Discover recent market IDs and data availability | PostgreSQL |
| `GET` | `/markets/latest` | Latest stored five-minute market for one source | PostgreSQL |
| `GET` | `/markets/{market_id}` | One source's five-minute OHLC and samples | PostgreSQL |
| `GET` | `/markets/current/sources` | Current-window source comparison | PostgreSQL |
| `GET` | `/markets/{market_id}/sources` | Source comparison for one market | PostgreSQL |
| `GET` | `/markets/current/data` | Full current 300-second dashboard series | PostgreSQL |
| `GET` | `/markets/{market_id}/data` | Full series for one market | PostgreSQL |
| `GET` | `/markets/current/download` | Download current market JSON | PostgreSQL |
| `GET` | `/markets/{market_id}/download` | Download one market as JSON | PostgreSQL |
| `GET` | `/markets/current/live` | Lowest-latency current prices and experimental Chainlink catch-up signal | Redis only |

## Health

### `GET /healthz`

Checks whether the API can query PostgreSQL.

```bash
curl "${API_BASE_URL}/healthz"
```

Healthy response, HTTP `200`:

```json
{
  "ok": true,
  "database": "ok",
  "service": "price-api"
}
```

Database failure response, HTTP `503`:

```json
{
  "ok": false,
  "database": "error",
  "service": "price-api",
  "error": "database unavailable"
}
```

The `error` value is the database exception text and is not a stable value for
frontend branching. Branch on the HTTP status and `ok` instead.

## Latest Stored Price

### `GET /prices/latest`

Returns the newest PostgreSQL price sample for one provider and symbol.

Query parameters:

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `provider` | string | `binance_spot` | Provider code |
| `symbol` | string | `BTCUSDT` | Instrument symbol |

Supported stored spot/oracle combinations are:

| Provider | Symbol | Source |
| --- | --- | --- |
| `binance_spot` | `BTCUSDT` | Binance Spot ticker |
| `polymarket_chainlink_rtds` | `BTCUSD` | Polymarket Chainlink RTDS |

Calls:

```bash
curl "${API_BASE_URL}/prices/latest"
curl "${API_BASE_URL}/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
```

Response:

```json
{
  "provider": "binance_spot",
  "symbol": "BTCUSDT",
  "price": "123456.780000000000000000",
  "sample_second_ms": 1783459200000,
  "sample_second_at": "2026-07-07T21:00:00Z",
  "provider_event_ms": 1783459199876,
  "received_ms": 1783459199900,
  "market_id": 5944864,
  "market_start_ms": 1783459200000,
  "market_end_ms": 1783459500000
}
```

| Field | Meaning |
| --- | --- |
| `provider`, `symbol` | Source identity selected by the query |
| `price` | Exact stored decimal price as a string |
| `sample_second_ms`, `sample_second_at` | UTC second used for the historical sample |
| `provider_event_ms` | Timestamp supplied by the upstream provider, when available |
| `received_ms` | Time the collector received the upstream event |
| `market_id`, `market_start_ms`, `market_end_ms` | Five-minute window containing the sample |

Returns HTTP `404` with `{"detail":"no latest price found ..."}` when the
provider/symbol has no sample.

## Market Discovery

### `GET /markets`

Returns a lightweight, newest-first list of market windows for navigation. This
is the authoritative way for a frontend to discover market IDs; do not derive
IDs from the browser clock or assume that subtracting one produces a stored
market.

Query parameters:

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `limit` | integer | `3` | Number of markets to return, from `1` through `50` |
| `include_current` | boolean | `false` | Include the active market when it has at least one observation |
| `before_market_id` | integer | omitted | Return only markets with IDs strictly less than this cursor |

The default request returns the three newest completed markets that contain
real observations. Results always exclude future windows, including a market
that a collector has preloaded, and exclude windows whose availability counts
are all zero. `include_current=true` adds the observed active market when one
exists; it never includes a future or empty window.

Calls:

```bash
curl "${API_BASE_URL}/markets"
curl "${API_BASE_URL}/markets?limit=3&include_current=true"
curl "${API_BASE_URL}/markets?limit=3&before_market_id=5944861"
```

Response:

```json
{
  "schema_version": 1,
  "server_time_ms": 1783459250123,
  "markets": [
    {
      "market_id": 5944863,
      "market_start_ms": 1783458900000,
      "market_end_ms": 1783459200000,
      "market_start_at": "2026-07-07T20:55:00Z",
      "market_end_at": "2026-07-07T21:00:00Z",
      "is_complete": true,
      "availability": {
        "binance": 300,
        "chainlink": 298,
        "futures": 300,
        "open_interest": 60,
        "flow": 300,
        "book": 299,
        "probabilities": 297
      }
    },
    {
      "market_id": 5944862,
      "market_start_ms": 1783458600000,
      "market_end_ms": 1783458900000,
      "market_start_at": "2026-07-07T20:50:00Z",
      "market_end_at": "2026-07-07T20:55:00Z",
      "is_complete": true,
      "availability": {
        "binance": 300,
        "chainlink": 300,
        "futures": 300,
        "open_interest": 60,
        "flow": 299,
        "book": 300,
        "probabilities": 300
      }
    },
    {
      "market_id": 5944861,
      "market_start_ms": 1783458300000,
      "market_end_ms": 1783458600000,
      "market_start_at": "2026-07-07T20:45:00Z",
      "market_end_at": "2026-07-07T20:50:00Z",
      "is_complete": true,
      "availability": {
        "binance": 299,
        "chainlink": 298,
        "futures": 300,
        "open_interest": 60,
        "flow": 300,
        "book": 300,
        "probabilities": 296
      }
    }
  ],
  "next_before_market_id": 5944861
}
```

Each `availability` value is the number of stored one-second observations for
that dataset in the market:

| Field | Dataset |
| --- | --- |
| `binance` | Binance Spot price samples |
| `chainlink` | Polymarket Chainlink RTDS price samples |
| `futures` | Binance USD-M futures snapshots |
| `open_interest` | Open-interest snapshots |
| `flow` | Aggregated trade-flow seconds |
| `book` | Aggregated book-ticker seconds |
| `probabilities` | Polymarket Up/Down probability snapshots |

`futures` counts rows with a last price, `open_interest` counts rows with a
contracts value, and `probabilities` counts rows where both Up and Down asks
are available. A `flow` count can include an intentional zero-trade second.

`next_before_market_id` is non-null only when an older page exists. Pass it
unchanged as `before_market_id`; it is exclusive, so the last market from the
current page is not repeated. An empty result is HTTP `200` with `markets: []`
and `next_before_market_id: null`.

### Frontend selection flow

Load the discovery list, keep the selected ID in component state or the page
URL, and use that ID with the full data endpoint:

```javascript
const discovery = await apiGet("/markets", {
  limit: 3,
  include_current: false,
});

const selectedMarketId = discovery.markets[0]?.market_id;
const selectedMarket = selectedMarketId === undefined
  ? null
  : await apiGet(`/markets/${selectedMarketId}/data`, {
      include_probabilities: true,
      include_futures: true,
      include_oi: true,
      include_flow: true,
      include_book: true,
    });
```

This keeps discovery responses small while the detail route supplies the
three prices, flow, book, open-interest, and Polymarket probability series for
the selected market. For an active dashboard, `/markets/current/data` remains
available and its response also contains `market.market_id`.

## Single-Source Market Summary

These two routes return the same response shape:

- `GET /markets/latest` finds the greatest stored market ID for the requested
  source.
- `GET /markets/{market_id}` returns the requested market ID.

Both accept these query parameters:

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `provider` | string | `binance_spot` | Provider code |
| `symbol` | string | `BTCUSDT` | Instrument symbol |

Calls:

```bash
curl "${API_BASE_URL}/markets/latest"
curl "${API_BASE_URL}/markets/5944864?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
```

Response:

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
  "sample_count": 2,
  "open": "123000.000000000000000000",
  "high": "123500.000000000000000000",
  "low": "123000.000000000000000000",
  "close": "123500.000000000000000000",
  "samples": [
    {
      "sample_second_ms": 1783459200000,
      "sample_second_at": "2026-07-07T21:00:00Z",
      "price": "123000.000000000000000000"
    },
    {
      "sample_second_ms": 1783459201000,
      "sample_second_at": "2026-07-07T21:00:01Z",
      "price": "123500.000000000000000000"
    }
  ]
}
```

`is_complete` is computed at request time by comparing the server clock with
`market_end_ms`. `samples` is ordered by `sample_second_ms`; `open`, `high`,
`low`, and `close` are computed from those stored samples. `sample_count` can be
less than `300` when seconds are missing or the market is still active.

`/markets/latest` returns HTTP `404` if no market exists for the source.
`/markets/{market_id}` returns HTTP `404` if that source has no samples in the
requested market.

## Multi-Source Market Summary

These routes compare the stored Binance Spot and Chainlink RTDS samples:

- `GET /markets/current/sources`
- `GET /markets/{market_id}/sources`

They do not take query parameters.

```bash
curl "${API_BASE_URL}/markets/current/sources"
curl "${API_BASE_URL}/markets/5944864/sources"
```

Response:

```json
{
  "market_id": 5944864,
  "market_start_ms": 1783459200000,
  "market_end_ms": 1783459500000,
  "market_start_at": "2026-07-07T21:00:00Z",
  "market_end_at": "2026-07-07T21:05:00Z",
  "is_complete": false,
  "sources": [
    {
      "provider": "binance_spot",
      "symbol": "BTCUSDT",
      "quote_asset": "USDT",
      "sample_count": 300,
      "open": "123000.000000000000000000",
      "high": "123500.000000000000000000",
      "low": "122900.000000000000000000",
      "close": "123456.780000000000000000",
      "latest_sample_second_ms": 1783459499000,
      "latest_provider_event_ms": 1783459498950,
      "latest_received_ms": 1783459499010
    },
    {
      "provider": "polymarket_chainlink_rtds",
      "symbol": "BTCUSD",
      "quote_asset": "USD",
      "sample_count": 298,
      "open": "122998.120000000000000000",
      "high": "123501.990000000000000000",
      "low": "122901.030000000000000000",
      "close": "123455.900000000000000000",
      "latest_sample_second_ms": 1783459499000,
      "latest_provider_event_ms": 1783459499123,
      "latest_received_ms": 1783459499320
    }
  ]
}
```

Only sources that have samples are present in `sources`. The list is ordered
with Binance Spot before Chainlink. Returns HTTP `404` when neither source has
samples for the selected market.

## Full Market Data Series

The dashboard series routes are:

- `GET /markets/current/data`
- `GET /markets/{market_id}/data`

They return a 300-row, one-row-per-second grid for an existing five-minute
market window. Future or missing observations remain `null`.

### Query parameters

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `include_probabilities` | boolean | `false` | Adds `series[].probabilities` |
| `include_futures` | boolean | `false` | Adds `series[].futures` |
| `include_oi` | boolean | `false` | Adds `series[].open_interest` and, when available, top-level `previous_5m_oi_summary` |
| `include_flow` | boolean | `false` | Adds `series[].flow` and `series[].freshness.futures_flow` |
| `include_book` | boolean | `false` | Adds `series[].book` and `series[].freshness.futures_book` |
| `fill_display` | boolean | `false` | Carries the latest prior Chainlink value into a missing second for display only |
| `max_carry_forward_ms` | integer | `10000` | Maximum Chainlink display carry-forward age; negative values act as `0` |

Boolean query values should be sent as `true` or `false`.

Calls:

```bash
curl "${API_BASE_URL}/markets/current/data"
curl "${API_BASE_URL}/markets/5944864/data?include_probabilities=true&include_futures=true&include_oi=true&include_flow=true&include_book=true"
curl "${API_BASE_URL}/markets/current/data?fill_display=true&max_carry_forward_ms=5000"
```

Browser example:

```javascript
const market = await apiGet("/markets/current/data", {
  include_probabilities: true,
  include_futures: true,
  include_oi: true,
  include_flow: true,
  include_book: true,
  fill_display: true,
  max_carry_forward_ms: 5000,
});
```

### Base response

These fields are always present on a successful response:

```json
{
  "schema_version": 2,
  "server_time_ms": 1783459250123,
  "market": {
    "market_id": 5944864,
    "market_start_ms": 1783459200000,
    "market_end_ms": 1783459500000,
    "market_start_at": "2026-07-07T21:00:00Z",
    "market_end_at": "2026-07-07T21:05:00Z",
    "seconds_expected": 300,
    "chainlink_resolution": {
      "open": null,
      "close": null,
      "status": "pending",
      "source": null
    },
    "resolution": {
      "status": "pending",
      "resolution_type": null,
      "winner": null,
      "winning_token_id": null,
      "resolved_at_ms": null,
      "official_payouts": {
        "up": null,
        "down": null
      },
      "source": null
    }
  },
  "series": [
    {
      "t": 0,
      "timestamp_ms": 1783459200000,
      "timestamp_at": "2026-07-07T21:00:00Z",
      "prices": {
        "binance": "123000.00",
        "chainlink": "122998.12"
      },
      "freshness": {
        "binance": {
          "source_ms": 1783459199750,
          "received_ms": 1783459199800,
          "source_age_ms": 50373,
          "received_age_ms": 50323,
          "transport_lag_ms": 50
        },
        "chainlink": {
          "source_ms": 1783459200000,
          "message_ms": 1783459200020,
          "received_ms": 1783459200040,
          "is_carried_forward": false,
          "source_age_ms": 50123,
          "received_age_ms": 50083,
          "transport_lag_ms": 40
        },
        "futures_last": {
          "source_ms": 1783459200000,
          "received_ms": 1783459200050,
          "source_age_ms": 50123,
          "received_age_ms": 50073
        },
        "open_interest": {
          "source_ms": 1783459200000,
          "received_ms": 1783459200050,
          "source_age_ms": 50123,
          "received_age_ms": 50073
        }
      }
    }
  ]
}
```

`t` is the zero-based second offset from `market_start_ms`. Price strings in
this response are rounded to two decimal places. Any value or freshness
timestamp/age can be `null` when its source observation is absent.

The ages are measured against `server_time_ms`, not against the series row's
timestamp. `transport_lag_ms` is `received_ms - source_ms`, clamped to zero.
For `freshness.futures_last`, `source_ms` is Binance `aggTrade.T`, while
`received_ms` is the local observation time of the combined historical
snapshot, not the WebSocket's pre-parse receive time. The snapshot row itself is
keyed by the premium-index timestamp (or its local observation fallback), not by
the trade timestamp.

When `fill_display=true`, only Chainlink is carried forward, and
`freshness.chainlink.is_carried_forward` reports whether the row uses an older
sample. Stored data is not changed.

### Official market resolution

`market.chainlink_resolution` and `market.resolution` are always present on the
data and download routes. They do not require `include_probabilities=true`.
For a completed market they can look like:

```json
{
  "market": {
    "chainlink_resolution": {
      "open": "63337.115841440165",
      "close": "63336.71900847139",
      "status": "official",
      "source": "polymarket_gamma_event_metadata"
    },
    "resolution": {
      "status": "resolved",
      "resolution_type": "winner",
      "winner": "Down",
      "winning_token_id": "22257037717815677829542896526504988088700721885271716267073503286407544507251",
      "resolved_at_ms": 1783647917000,
      "official_payouts": {
        "up": "0",
        "down": "1"
      },
      "source": "polymarket_clob_rest"
    }
  }
}
```

On the data routes, Chainlink `open` and `close` are exact decimal strings from
Polymarket's official Gamma event metadata. The download routes format those
two values to fixed two-decimal strings. `chainlink_resolution.status` is:

- `pending` while either official price is not yet available; or
- `official` when both official prices are available.

`resolution.status` is `pending` or `resolved`. While pending,
`resolution_type` is `null`. A normal resolved binary market has
`resolution_type: "winner"`, `winner` set to `Up` or `Down`, its winning token
ID, and official `1`/`0` payouts. A split resolution has
`resolution_type: "split"`, `winner` and `winning_token_id` set to `null`, and
official payouts of `0.5`/`0.5`. `resolved_at_ms` can be `null` when Polymarket
does not supply a resolution timestamp. Source fields are also `null` until
their corresponding official data is available.
`resolution.source` identifies whether the official result came from the CLOB
WebSocket (`polymarket_clob_ws`), CLOB REST market
(`polymarket_clob_rest`), or Gamma (`polymarket_gamma`).

Resolution is reconciled after the market ends. The API can therefore return
`pending` briefly for a completed window. Only official Gamma/CLOB resolution
data can set `winner`; probability quotes are never treated as a result.

### Optional `probabilities`

Added to each series row by `include_probabilities=true`:

```json
{
  "probabilities": {
    "up": { "ask": "0.48" },
    "down": { "ask": "0.52" }
  }
}
```

The ask values are two-decimal strings or `null`. They are market quotes, not
settlement results. In particular, the last Up/Down probability snapshot never
determines `market.resolution.winner`.

### Optional `futures`

Added to each series row by `include_futures=true`:

```json
{
  "futures": {
    "last": "62075.12",
    "mark": "62074.88",
    "index": "62070.19",
    "premium_bps": "0.76"
  }
}
```

All four values are two-decimal strings or `null`. `last` comes from
`btcusdt@aggTrade.p`; `mark`, `index`, and `premium_bps` remain REST-derived.
The collector uses `last` only when the newest accepted trade is fresh and from
the current WebSocket connection. During startup, disconnect, or a stale-trade
interval, `last` and its source timestamp can be `null` while the REST-derived
fields and snapshot row remain present. There is no REST-ticker or book-derived
fallback for `last`.

### Optional `open_interest`

Added to each series row by `include_oi=true`:

```json
{
  "open_interest": {
    "contracts": "74321.123",
    "notional_usdt": "4616789012.34",
    "delta_30s": null,
    "delta_60s": null,
    "delta_300s": null
  }
}
```

`contracts` and delta values are three-decimal strings; `notional_usdt` is a
two-decimal string. Values can be `null` when the required snapshot is absent.

When the corresponding prior summary exists, `include_oi=true` also adds:

```json
{
  "previous_5m_oi_summary": {
    "source_window_start_ms": 1783458900000,
    "source_window_end_ms": 1783459200000,
    "effective_market_id": 5944864,
    "sum_open_interest": "74000.123",
    "sum_open_interest_value": "4590000000.13"
  }
}
```

### Optional `flow`

Added to each series row by `include_flow=true`:

```json
{
  "flow": {
    "buy_base": "0.016",
    "sell_base": "0.004",
    "buy_quote": "1000.00",
    "sell_quote": "250.00",
    "delta_quote": "750.00",
    "total_quote": "1250.00",
    "taker_imbalance": "0.60000000",
    "cvd_quote": "5000.00",
    "cvd_10s": "900.123456000000000000",
    "cvd_30s": "1200.129000000000000000",
    "imbalance_10s": "0.12345678",
    "imbalance_30s": "-0.23456789",
    "agg_trade_count": 4,
    "trade_count": 4,
    "max_trade_quote": "777.770000000000000000",
    "first_agg_trade_id": 123450,
    "last_agg_trade_id": 123453
  }
}
```

Financial values are exact decimal strings or `null`; counts and IDs are
integers or `null`. The same flag adds this object under `freshness`:

```json
{
  "futures_flow": {
    "source_ms": 1783459200900,
    "event_ms": 1783459200920,
    "received_ms": 1783459200950,
    "source_age_ms": 49223,
    "received_age_ms": 49173,
    "transport_lag_ms": 50
  }
}
```

### Optional `book`

Added to each series row by `include_book=true`:

```json
{
  "book": {
    "bid": "62074.10",
    "ask": "62074.20",
    "bid_qty": "1.25",
    "ask_qty": "0.75",
    "mid": "62074.15",
    "spread": "0.10",
    "spread_bps": "0.01610935",
    "book_imbalance": "0.25000000",
    "microprice": "62074.166789000000000000",
    "update_id": 123456
  }
}
```

Financial values are exact decimal strings or `null`; `update_id` is an
integer or `null`. The same flag adds this object under `freshness`:

```json
{
  "futures_book": {
    "source_ms": 1783459200900,
    "event_ms": 1783459200900,
    "transaction_ms": 1783459200890,
    "received_ms": 1783459200950,
    "source_age_ms": 49223,
    "received_age_ms": 49173,
    "transport_lag_ms": 50
  }
}
```

The data routes return HTTP `404` when the selected market window does not
exist. The current route uses `{"detail":"no current market data found"}`;
the ID route includes the requested market ID in `detail`.

## Market JSON Downloads

The download routes accept the same seven query parameters as the data routes:

- `GET /markets/current/download`
- `GET /markets/{market_id}/download`

```bash
curl -OJ "${API_BASE_URL}/markets/5944864/download?include_futures=true&include_oi=true&include_flow=true&include_book=true&include_probabilities=true"
```

The response has `Content-Type: application/json` and a
`Content-Disposition: attachment` filename assembled in this order:

```text
btc_5m_market_{market_id}[_futures][_oi][_flow][_book][_probabilities].json
```

The downloaded JSON starts from the data-route response but intentionally uses
a smaller export shape:

- `market.market_start_ms` and `market.market_end_ms` are removed;
  `market_start_at` and `market_end_at` are retained.
- Every `series[].timestamp_ms` is removed; `series[].timestamp_at` and `t` are
  retained.
- Every `series[].freshness` object is removed.
- `market.chainlink_resolution.open` and `.close` are formatted as fixed
  two-decimal strings when present. `null` values remain `null`.
- With `include_futures=true`, only the futures `last` value is retained. It is
  moved to `series[].prices.futures`; `mark`, `index`, and `premium_bps` are not
  exported.
- With `include_flow=true`, `series[].flow` contains only `taker_imbalance`,
  `cvd_10s`, `cvd_30s`, `imbalance_10s`, and `imbalance_30s`.
- With `include_book=true`, `series[].book` contains only `book_imbalance` and
  `microprice`.
- Probability, open-interest, resolution, and server-time fields otherwise keep
  their data-route shapes. In particular,
  `market.chainlink_resolution` and `market.resolution` are always retained in
  the download, even when `include_probabilities=false`.

Exported flow/book formatting is fixed:

```json
{
  "prices": {
    "binance": "123000.00",
    "chainlink": "122998.12",
    "futures": "62075.12"
  },
  "flow": {
    "taker_imbalance": "0.6000",
    "cvd_10s": "900.12",
    "cvd_30s": "1200.13",
    "imbalance_10s": "0.1235",
    "imbalance_30s": "-0.2346"
  },
  "book": {
    "book_imbalance": "0.2500",
    "microprice": "62074.17"
  }
}
```

Browser download example:

```javascript
const url = new URL(`${API_BASE_URL}/markets/5944864/download`, window.location.origin);
url.searchParams.set("include_futures", "true");
url.searchParams.set("include_oi", "true");
window.location.assign(url);
```

The download routes return HTTP `404` with the requested market ID in `detail`
when the market window does not exist.

## Current Live Prices and Shadow Signal

### `GET /markets/current/live`

Reads `btc:live:binance_spot`, `btc:live:chainlink`, `btc:live:futures`, and
`btc:live:chainlink_shadow` with one Redis `MGET`. It does not query PostgreSQL
and does not return historical samples, probabilities, mark/index prices, open
interest, flow, book data, or persisted shadow evaluations.

See [`LIVE_DATA.md`](LIVE_DATA.md) for the upstream extraction paths, Redis
handoff, freshness semantics, and recommended dashboard polling behavior.

Query parameters:

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `max_chainlink_carry_forward_ms` | integer | `10000` | Accepted for compatibility; the current Redis implementation does not filter or carry values based on this parameter |

```bash
curl "${API_BASE_URL}/markets/current/live"
```

Response:

```json
{
  "server_time_ms": 1783988794075,
  "market_id": 5946629,
  "market_start_ms": 1783988700000,
  "market_end_ms": 1783989000000,
  "prices": {
    "binance_spot": {
      "value": "62310.12",
      "source_timestamp_ms": 1783988793900,
      "received_ms": 1783988793950,
      "source_age_ms": 175,
      "received_age_ms": 125,
      "provider_event_ms": 1783988793900
    },
    "chainlink": {
      "value": "62290.21096323273",
      "source_timestamp_ms": 1783988792000,
      "received_ms": 1783988793346,
      "source_age_ms": 2075,
      "received_age_ms": 729,
      "provider_event_ms": 1783988792000
    }
  },
  "futures": {
    "last": {
      "value": "62331.80",
      "source_timestamp_ms": 1783988793451,
      "received_ms": 1783988793638,
      "source_age_ms": 624,
      "received_age_ms": 437,
      "time_ms": 1783988793451
    }
  },
  "signals": {
    "chainlink_catchup": {
      "schema_version": 1,
      "mode": "shadow",
      "selection_schema_version": 2,
      "selection_policy_version": "chronological_holdout_v2",
      "selection_fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
      "selection_artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699",
      "selection_evidence_end_ms": 1783983205028,
      "model_version": "catchup_ratio_l3000_b100",
      "beta": "1",
      "generated_ms": 1783988794005,
      "valid": true,
      "status": "valid",
      "invalid_reasons": [],
      "state": "anchored",
      "horizon_ms": 3000,
      "estimated_lag_ms": 3000,
      "current_chainlink": "62290.21096323273",
      "projected_chainlink": "62292.00981418305598931493660",
      "pending_move": "1.79885095032598931493660",
      "pending_move_bps": "0.2887854965506176800898399415",
      "direction": "up",
      "futures_now": "62331.80",
      "futures_reference": "62330.00",
      "chainlink_now_source_timestamp_ms": 1783988792000,
      "chainlink_now_received_ms": 1783988793346,
      "anchor_chainlink_source_timestamp_ms": 1783988792000,
      "anchor_chainlink_received_ms": 1783988793346,
      "futures_now_source_timestamp_ms": 1783988793451,
      "futures_now_received_ms": 1783988793638,
      "futures_reference_source_timestamp_ms": 1783988789826,
      "futures_reference_received_ms": 1783988790015,
      "futures_reference_target_ms": 1783988790346,
      "futures_reference_gap_ms": 331,
      "futures_received_age_ms": 367,
      "chainlink_received_age_ms": 659,
      "market_id": 5946629,
      "market_start_ms": 1783988700000,
      "market_end_ms": 1783989000000,
      "ms_to_market_end": 205995,
      "full_horizon_before_market_end": true,
      "signal_age_ms": 70
    }
  }
}
```

`provider_event_ms` and `time_ms` are compatibility aliases for the same value
as `source_timestamp_ms`. The futures `last` value comes from Binance
`btcusdt@aggTrade.p`; its `source_timestamp_ms`/`time_ms` is `aggTrade.T`, and
its `received_ms` is local wall time recorded immediately after WebSocket
`recv()` and before parsing. This source change does not alter the response
shape. The source-price objects and compatibility aliases are unchanged by the
additional `signals` object.

`signals.chainlink_catchup` is either the complete typed shadow payload shown
above or `null`. The API calculates its only API-specific field as:

```text
signal_age_ms = max(0, server_time_ms - generated_ms)
```

This object is an experimental projected Chainlink catch-up, not a probability,
settlement, execution, or market-close forecast.

`beta`, `current_chainlink`, `projected_chainlink`, `pending_move`,
`pending_move_bps`, `futures_now`, and `futures_reference` are fixed-point JSON
strings, or `null` where the typed shadow contract permits it. Do not parse
them with binary floating-point. Timestamps, ages, horizons, and market IDs are
JSON integers. `invalid_reasons` is an array of strings.

The top-level market window is calculated by the API for `server_time_ms`. The
nested `market_id`, `market_start_ms`, `market_end_ms`, `ms_to_market_end`, and
`full_horizon_before_market_end` were fixed by the shadow worker at
`generated_ms`. Around a five-minute boundary, a still-live signal can
therefore contain the preceding window briefly; consumers must not overwrite
the nested generation-time context with the top-level current window.

A well-formed signal with `valid: false` is still returned as an object. Its
non-`valid` `status` and non-empty `invalid_reasons` explain why it cannot be
used, while projection and anchor fields are `null`; current input fields can
still be present for diagnostics. Consumers should display its status rather
than reuse a previous valid projection. If the shadow worker is disabled or
stops, the short Redis TTL expires and `signals.chainlink_catchup` becomes
`null`. A missing shadow key is normal and the endpoint still returns HTTP
`200`.

If a Redis key is absent, its value, timestamps, ages, and alias are all `null`;
the endpoint still returns HTTP `200`. The endpoint does not reject stale
values. If the futures WebSocket disconnects or its current trade becomes
stale, the collector leaves the last cached value in place and its ages keep
growing. The dashboard should use both `source_age_ms` and `received_age_ms` to
decide whether to display a stale indicator.

Redis connection/read failures return HTTP `503` with:

```json
{ "detail": "live cache unavailable" }
```

Malformed JSON in any of the three source-price keys still returns HTTP `503`
with:

```json
{ "detail": "live cache payload invalid" }
```

A malformed `btc:live:chainlink_shadow` payload is isolated from the three
actual prices. The API logs the shadow decode error, returns HTTP `200`, and
sets `signals.chainlink_catchup` to `null`. It does not expose the malformed
raw value or turn an experimental model-payload problem into an outage of the
actual live-price response. A Redis connection or `MGET` failure still returns
the unchanged `503 live cache unavailable` response.

## Common Errors

Except for `/healthz`, FastAPI errors use this JSON shape:

```json
{ "detail": "human-readable message" }
```

Expected statuses are:

| Status | Meaning |
| --- | --- |
| `404` | The requested source or market has no stored data |
| `405` | The route was called with a method other than `GET` |
| `422` | A typed path/query value is invalid, such as a non-integer market ID or a discovery `limit` outside `1..50` |
| `503` | `/markets/current/live` cannot read Redis or one of its three actual source-price payloads is malformed, or `/healthz` cannot query PostgreSQL |

The exact `detail` text is useful for logs but should not be treated as a stable
frontend enum. Use the HTTP status and the endpoint context.

## Framework-Generated Developer Endpoints

FastAPI also exposes these developer/discovery routes. They are not dashboard
data sources:

| Path | Purpose |
| --- | --- |
| `/openapi.json` | Machine-readable OpenAPI document |
| `/docs` | Swagger UI |
| `/redoc` | ReDoc UI |
| `/docs/oauth2-redirect` | Swagger UI OAuth redirect helper; the application itself does not configure authentication |

## Maintenance Requirement

Update this file in the same change whenever an endpoint in
`price_collector/api.py` is added, changed, renamed, or removed. Keep the
method/path inventory, parameters, call examples, response shapes, optional
fields, and error responses synchronized with the implementation.
