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
  const contentType = response.headers.get("content-type") ?? "";
  const body = contentType.includes("application/json")
    ? await response.json()
    : await response.text();
  if (!response.ok) {
    const detail = typeof body === "object" && body !== null
      ? body.detail ?? body.error
      : body;
    throw new Error(detail || `HTTP ${response.status}`);
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
| `GET` | `/markets/current/shadow-evaluations` | Persisted shadow forecasts and causal outcomes for the current target window | PostgreSQL |
| `GET` | `/markets/{market_id}/shadow-evaluations` | Persisted shadow forecasts and causal outcomes for one target window | PostgreSQL |
| `GET` | `/markets/current/shadow-evaluations/download` | Download rounded current-window shadow evaluations as JSON | PostgreSQL |
| `GET` | `/markets/{market_id}/shadow-evaluations/download` | Download one target window's rounded shadow evaluations as JSON | PostgreSQL |
| `GET` | `/markets/current/live` | Lowest-latency current prices and experimental Chainlink catch-up signal | Redis only |
| `GET` | `/markets/current/live/challengers/chainlink-catchup-2s` | Separate unselected two-second Chainlink catch-up challenger | Redis only |

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

## Persisted Shadow-Evaluation Chart Data

The shadow-evaluation JSON reporting routes are:

- `GET /markets/current/shadow-evaluations`
- `GET /markets/{market_id}/shadow-evaluations`

The rounded attachment routes are:

- `GET /markets/current/shadow-evaluations/download`
- `GET /markets/{market_id}/shadow-evaluations/download`

All four return the persisted 500 ms forecast attempts, the Chainlink and
futures cache snapshots used at forecast time, and causal Chainlink outcomes
for exactly one five-minute **target-time** window and one explicitly requested
model. They read PostgreSQL through the API's reader connection and the
restricted `shadow_signal_evaluation_chart_points` view. They do not read
Redis, run a model, alter evaluation rows, or expose an arbitrary time-range
query. The attachment variants preserve the report structure but round its
Decimal strings for readability after all validation, classifications, and
performance calculations are complete. They also add versioned `export`
metadata. The reporting routes remain the canonical full-precision source.

These reporting routes are independent from `GET /markets/current/live`. The
live route remains one Redis `MGET`, performs no PostgreSQL query, and continues
to return only the newest short-lived primary projection.

The reporting point exposes only the current Chainlink/futures snapshots for
the forecast attempt. It does not expose the model's `futures_reference`, its
anchor metadata, worker-age diagnostics, or database-writer metadata.

### Request

All four routes require the same query parameter:

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `model_version` | enum string | none; required | Return exactly one supported persisted candidate |

Supported values and their fixed report metadata are:

| `model_version` | `horizon_ms` | `beta` |
| --- | ---: | ---: |
| `catchup_ratio_l3000_b100` | `3000` | `"1"` |
| `catchup_ratio_l3500_b100` | `3500` | `"1"` |
| `catchup_ratio_l4000_b100` | `4000` | `"1"` |

There is no implicit default and the API does not choose or rerank a model for
the caller. A dashboard should request its explicitly configured frozen
primary and verify the returned selection identity rather than selecting the
candidate with the best recent result.

Calls:

```bash
curl "${API_BASE_URL}/markets/current/shadow-evaluations?model_version=catchup_ratio_l3000_b100"
curl "${API_BASE_URL}/markets/5946630/shadow-evaluations?model_version=catchup_ratio_l3000_b100"
curl -OJ "${API_BASE_URL}/markets/current/shadow-evaluations/download?model_version=catchup_ratio_l3000_b100"
curl -OJ "${API_BASE_URL}/markets/5946630/shadow-evaluations/download?model_version=catchup_ratio_l3000_b100"
```

Browser example using the `apiGet` helper from the access section:

```javascript
const modelVersion = "catchup_ratio_l3000_b100";

const currentEvaluations = await apiGet(
  "/markets/current/shadow-evaluations",
  { model_version: modelVersion },
);

const selectedEvaluations = await apiGet(
  "/markets/5946630/shadow-evaluations",
  { model_version: modelVersion },
);
```

Browser download example:

```javascript
const marketId = 5946630;
const modelVersion = "catchup_ratio_l3000_b100";
const url = new URL(
  `${API_BASE_URL}/markets/${marketId}/shadow-evaluations/download`,
  window.location.origin,
);
url.searchParams.set("model_version", modelVersion);

const anchor = document.createElement("a");
anchor.href = url.toString();
anchor.click();
```

The server filename is
`btc_5m_market_{market_id}_shadow_evaluations_{model_version}_rounded.json`.
Prefer the ID-addressed route after a market ends; a current-window download is
a partial snapshot and can resolve to an adjacent market if requested across a
five-minute boundary.

`market_id` must be a non-negative integer that can be represented safely by
the PostgreSQL market-window calculation. FastAPI returns HTTP `422` before
querying PostgreSQL when the ID, required model parameter, or supported-model
enum is invalid.

### Response

The reporting routes use this canonical full-precision response shape. The
attachment routes retain these fields and add the rounded-export metadata
described below.

```json
{
  "schema_version": 2,
  "server_time_ms": 1783989010123,
  "market": {
    "market_id": 5946630,
    "market_start_ms": 1783989000000,
    "market_end_ms": 1783989300000,
    "boundary": "[start_ms,end_ms)"
  },
  "evaluation_semantics": {
    "scored_input_max_future_skew_ms": 0
  },
  "model": {
    "model_version": "catchup_ratio_l3000_b100",
    "horizon_ms": 3000,
    "beta": "1",
    "evaluation_cadence_ms": 500,
    "selection_identities": [
      {
        "schema_version": 2,
        "policy_version": "chronological_holdout_v2",
        "evidence_end_ms": 1783983205028,
        "fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
        "artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699"
      }
    ]
  },
  "coverage": {
    "window_buckets": 600,
    "market_window_elapsed": false,
    "observed_buckets": 1,
    "unobserved_buckets_as_of_response": null,
    "attempts": 1,
    "valid_forecasts": 1,
    "scored": 1,
    "invalid": 0,
    "valid_without_actual": 0
  },
  "performance": {
    "cohorts": [
      {
        "selection_identity": {
          "schema_version": 2,
          "policy_version": "chronological_holdout_v2",
          "evidence_end_ms": 1783983205028,
          "fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
          "artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699"
        },
        "scored_points": 1,
        "forecast": {
          "mean_absolute_error_usd": "3.250000000000000000",
          "median_absolute_error_usd": "3.250000000000000000",
          "p95_absolute_error_usd": "3.250000000000000000",
          "maximum_absolute_error_usd": "3.250000000000000000",
          "root_mean_squared_error_usd": "3.250000000000000000",
          "mean_signed_error_usd": "3.250000000000000000"
        },
        "no_change_baseline": {
          "mean_absolute_error_usd": "19.350000000000000000",
          "root_mean_squared_error_usd": "19.350000000000000000"
        },
        "mean_absolute_advantage_usd": "16.100000000000000000",
        "mae_skill_vs_no_change": "0.83204134366925064599483204134366925064599483204134366925064599483204134366925065",
        "rmse_skill_vs_no_change": "0.83204134366925064599483204134366925064599483204134366925064599483204134366925065",
        "paired_comparison": {
          "wins": 1,
          "ties": 0,
          "losses": 0,
          "win_rate": "1",
          "tie_rate": "0",
          "loss_rate": "0"
        }
      }
    ]
  },
  "points": [
    {
      "selection_schema_version": 2,
      "selection_policy_version": "chronological_holdout_v2",
      "selection_evidence_end_ms": 1783983205028,
      "selection_fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
      "selection_artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699",
      "model_version": "catchup_ratio_l3000_b100",
      "beta": "1.000000000000000000",
      "generated_ms": 1783989000507,
      "target_ms": 1783989003507,
      "matured_ms": 1783989003512,
      "horizon_ms": 3000,
      "valid": true,
      "status": "valid",
      "invalid_reasons": [],
      "state": "anchored",
      "outcome_status": "available",
      "outcome_invalid_reasons": [],
      "forecast_market_id": 5946630,
      "full_horizon_before_forecast_market_end": true,
      "chainlink_at_forecast": "64080.470000000000000000",
      "chainlink_at_forecast_source_timestamp_ms": 1783989000000,
      "chainlink_at_forecast_received_ms": 1783989000330,
      "futures_at_forecast": "64121.300000000000000000",
      "futures_at_forecast_source_timestamp_ms": 1783989000420,
      "futures_at_forecast_received_ms": 1783989000475,
      "projected_chainlink": "64103.070000000000000000",
      "actual_chainlink": "64099.820000000000000000",
      "actual_chainlink_source_timestamp_ms": 1783989003000,
      "actual_chainlink_received_ms": 1783989003340,
      "actual_chainlink_age_at_target_ms": 167,
      "pending_move": "22.600000000000000000",
      "pending_move_bps": "3.526815580472490292",
      "direction": "up",
      "forecast_error": "3.250000000000000000",
      "baseline_error": "-19.350000000000000000"
    }
  ]
}
```

On the reporting routes, decimal strings can contain different amounts of
trailing precision; their lexical scale is not a display-format guarantee.
`model.beta`, point `beta`, `chainlink_at_forecast`, `futures_at_forecast`,
projected/actual prices, moves, basis points, and error values are fixed-point
JSON strings or `null`. Keep those original strings and use a decimal library
for exact calculations. Do not calculate chart errors with binary
floating-point.

#### Rounded attachment format

The `/download` routes apply this fixed-scale policy with `ROUND_HALF_UP`:

| Decimal places | Download fields |
| ---: | --- |
| 2 | Point `chainlink_at_forecast`, `futures_at_forecast`, `projected_chainlink`, `actual_chainlink`, `pending_move`, `forecast_error`, and `baseline_error`; all forecast/no-change USD performance metrics; `mean_absolute_advantage_usd` |
| 4 | Model and point `beta`; point `pending_move_bps`; `mae_skill_vs_no_change`, `rmse_skill_vs_no_change`, and paired win/tie/loss rates |

Fixed trailing zeros are intentional, so a download uses strings such as
`"64399.90"`, `"0.0155"`, and `"1.0000"`. JSON `null` remains `null`, and a
value that rounds to zero is emitted without a negative sign. All timestamps,
counts, booleans, statuses, reasons, hashes, direction values, and market/model
identifiers remain unchanged.

Each attachment adds:

```json
{
  "export": {
    "schema_version": 1,
    "variant": "rounded_download",
    "source_report_schema_version": 2,
    "decimal_encoding": "fixed_point_string",
    "rounding_mode": "ROUND_HALF_UP",
    "precision_policy": "shadow_evaluation_download_v1",
    "decimal_places": {
      "usd_price_move_error": 2,
      "basis_points": 4,
      "unitless_beta_rate_skill": 4
    },
    "derived_metrics_computed_before_rounding": true,
    "classifications_computed_before_rounding": true
  }
}
```

The top-level `schema_version: 2` still identifies the source report contract;
`export.schema_version: 1` versions the rounded representation. Rounding is
lossy and happens independently for each final field. Consequently, rounded
errors need not exactly equal subtraction of rounded prices, a non-flat
direction can accompany `pending_move: "0.00"`, and rounded rates need not sum
lexically to `1.0000`. Exact classifications and counts remain authoritative.
Use the non-download reporting route for recomputation, audit evidence, or any
calculation requiring full precision.

`points` are sorted by `target_ms`, then `generated_ms`, then `horizon_ms`.
For every point:

```text
target_ms = generated_ms + horizon_ms
market_start_ms <= target_ms < market_end_ms
```

The point's price fields have two distinct chart times:

| Fields | Chart time | Meaning |
| --- | ---: | --- |
| `chainlink_at_forecast`, `futures_at_forecast` | `generated_ms` | Persisted cache snapshots read for the forecast attempt |
| `projected_chainlink`, `actual_chainlink` | `target_ms` | Predicted and causal observed Chainlink values for the forecast target |

For a projection/futures/Chainlink chart, plot `futures_at_forecast` at
`generated_ms`, and plot `projected_chainlink` and `actual_chainlink` at
`target_ms`. If the no-change baseline is also shown, plot
`chainlink_at_forecast` at `generated_ms`. Do not move a forecast-time snapshot
to `target_ms`: the response does not contain a target-time futures outcome.
The dashboard's browser-buffered `actual_futures` and `futures_received_ms` are
therefore not present in a later backend download and cannot be reconstructed
from latest-only Redis. `matured_ms` is the persistence time, not a chart time.

The forecast-time `*_source_timestamp_ms` fields identify the provider events;
they can be `null` when that metadata was unavailable. The corresponding
`*_received_ms` fields identify the local cache observations. For a valid
forecast they never exceed `generated_ms`. An invalid attempt deliberately can
retain a snapshot received after `generated_ms`; that timing exposes the
zero-skew violation that invalidated the attempt. A null forecast-time price
has null source and receive timing metadata. A valid forecast has both
forecast-time prices; an invalid attempt can retain either snapshot when that
individual input was available.

The paired actual is the newest Chainlink cache observation the live evaluator
had successfully observed with `actual_chainlink_received_ms <= target_ms`; it
is the causal value used by the stored error calculation. Raw replay remains
the event-complete authority for events overwritten between live evaluator
polls.

`evaluation_semantics.scored_input_max_future_skew_ms` is the receive-time
tolerance used when deciding whether a persisted live forecast can be scored.
It is always `0`: forecast-time Chainlink and futures inputs received after
`generated_ms` make that evaluation invalid. This field is independent of the
selection artifact fields. In particular, a point with selection schema `2`
and policy `chronological_holdout_v2` still uses this stricter zero-skew live
evaluation rule; it is not directly comparable to historical v2 replay
evidence that allowed nonzero future skew. The short-lived
`/markets/current/live` projection continues to follow its activated artifact
configuration and is not described by this reporting field.

The requested market is selected by `target_ms`, not by
`forecast_market_id`. `forecast_market_id` identifies the five-minute window
containing `generated_ms`. A point generated in the preceding market's last
four seconds can therefore appear at the beginning of the requested target
window. `full_horizon_before_forecast_market_end` also refers to that
generation-time market, not to the requested target window.

Error signs are:

```text
forecast_error = projected_chainlink - actual_chainlink
baseline_error = chainlink_at_forecast - actual_chainlink
```

Forecast validity and outcome integrity are independent axes. `valid`,
`status`, and `invalid_reasons` describe the generation-time forecast. An
invalid attempt is still returned. It has `valid: false`, a non-`valid`
`status`, at least one `invalid_reasons` entry, and null
`projected_chainlink`, `pending_move`, `pending_move_bps`, `direction`, and
`forecast_error`. Its forecast-time snapshots, causal actual, and
`baseline_error` fields can still be present when those individual values were
available. Do not carry a preceding valid projection across such a point.

`outcome_status` and `outcome_invalid_reasons` describe target-time evidence:

- `available` has an actual value and no outcome-invalid reasons.
- `unavailable` has no actual value and no outcome-invalid reasons; no causal
  target observation was available.
- `integrity_invalid` has no actual value and at least one explicit reason,
  such as a sequence gap or metadata-integrity failure.

For both null-actual statuses, the actual timing and error fields are null.
Never infer outcome integrity from `actual_chainlink: null`; inspect
`outcome_status`. A valid attempt always has a projection. It is counted as
`scored` only when `outcome_status` is `available`; otherwise it contributes to
`valid_without_actual`.

### Per-market performance

`performance` is calculated on demand from the same bounded rows returned in
`points`; it does not add a query or persist a second accuracy record. Each
cohort is scoped to the requested target-time market, requested model, and one
exact selection identity: schema version, policy version, evidence end,
selection fingerprint, and selection artifact hash. Never combine cohorts
when `model.selection_identities` contains more than one identity.

Only valid points whose `outcome_status` is `available` contribute. For every
such point, the persisted signed errors are used directly:

```text
forecast absolute error = abs(forecast_error)
baseline absolute error = abs(baseline_error)
```

`forecast` reports mean, median, nearest-rank p95, and maximum absolute error;
root mean squared error; and mean signed error. Median averages the two middle
values for an even cohort. The p95 uses every scored point and selects the
1-based value at `ceil(0.95 * scored_points)` after sorting absolute errors.
No sampling or binary floating-point calculation is used.

`no_change_baseline` measures the same scored points using the Chainlink value
at forecast time as the prediction. The comparison fields are:

```text
mean_absolute_advantage_usd = baseline MAE - forecast MAE
mae_skill_vs_no_change = 1 - (forecast MAE / baseline MAE)
rmse_skill_vs_no_change = 1 - (forecast RMSE / baseline RMSE)
```

A positive advantage or skill means the forecast was closer than no change;
zero means equal aggregate error; a negative value means worse. Skill is not
clamped. It is `null` when its baseline denominator is zero.

`paired_comparison` compares each forecast and baseline absolute error from the
same point. A smaller forecast error is a win, an exact equality is a tie, and
a larger forecast error is a loss. Counts always sum to `scored_points`; rates
are counts divided by that value. Comparisons use exact stored decimals with no
tie tolerance.

All derived financial metrics and rates are fixed-point JSON strings or
`null`, never JSON numbers. The cohort `scored_points` values sum to
`coverage.scored`. A represented selection identity with no scored points has
zero counts and `null` metrics and rates. When there are no points at all,
`performance.cohorts` is empty.

Treat these as descriptive forecast-error measurements for one five-minute
market, not settlement accuracy, probability accuracy, or evidence of
statistical significance. The overlapping 500 ms forecasts are strongly
autocorrelated. For an active market, label the metrics "so far" and show
`coverage.scored` beside them.

### Coverage and bounds

`window_buckets` is always `600` for the 300-second market and 500 ms
evaluation cadence. `observed_buckets` counts distinct generation buckets
represented by returned attempts. `attempts` is the number of returned rows;
`valid_forecasts + invalid` equals `attempts`, and
`scored + valid_without_actual` equals `valid_forecasts`.

`market_window_elapsed` is calculated against `server_time_ms`. While the
window is active, `unobserved_buckets_as_of_response` is `null` because future
buckets have not occurred. Once the market has elapsed, it is
`window_buckets - observed_buckets`. Missing buckets are not fabricated or
backfilled.

The database query is bounded to 1,001 rows so the API can reject rather than
truncate an anomalous result. A successful response contains at most 1,000
points and at most 600 distinct generation buckets. Exceeding either public
invariant returns HTTP `500`; the response never silently drops excess rows.

`model.selection_identities` is the sorted set of unique full selection
identities represented by the points. Each contains `schema_version`,
`policy_version`, `evidence_end_ms`, `fingerprint_sha256`, and
`artifact_sha256`. It is empty when `points` is empty and can contain more than
one entry if persisted rows span a selection change. Consumers must not
silently join different selection identities into one claimed-primary series.

### Empty markets, current rollover, and errors

A known market with no retained rows for the requested model returns HTTP
`200` with the normal metadata, zero coverage counts, an empty
`selection_identities` array, `performance: {"cohorts": []}`, and `points: []`.
This is normal before the first current-window evaluation is persisted and
after derived-evidence retention has removed older rows.
`evaluation_semantics.scored_input_max_future_skew_ms` remains present and zero
even when there are no points.

Derived evaluation retention defaults to `168` hours (seven days), not seven
minutes, although the production environment can configure a different value
of at least 24 hours. Cleanup is bounded and asynchronous, and queue loss or a
disabled evaluator can create gaps, so a download is a snapshot of retained
evidence rather than a completeness guarantee or permanent archive.

Each current route snapshots `server_time_ms` and its corresponding market once
before querying PostgreSQL. Its returned `market` object and attachment
filename are authoritative for that response. Calls made on opposite sides of
a five-minute boundary can therefore return adjacent market IDs. A dashboard
that needs a stable window should read the returned ID and continue with an
ID-addressed route.

The current target window returns HTTP `200` even if it has not yet appeared in
`market_windows`. An ID-addressed route uses the same behavior when the
requested ID is the API server's current market. A non-current ID absent from
`market_windows` returns HTTP `404`:

```json
{ "detail": "no market found for market_id=5946630" }
```

A missing or unsupported `model_version`, a negative or too-large market ID,
or a non-integer market ID returns FastAPI's HTTP `422` validation response.

If persisted rows violate the typed reporting invariants, including the
1,000-point cap, the API logs the inconsistency and returns HTTP `500`:

```json
{ "detail": "shadow evaluation data inconsistent" }
```

A PostgreSQL read failure also returns HTTP `500` and can use the server's
generic plain-text error body rather than the JSON `detail` shape. Treat either
response as a reporting-path failure; it does not imply that the Redis-only
live route or the standalone shadow worker has failed.

## Current Live Prices and Shadow Signal

### `GET /markets/current/live`

Reads `btc:live:binance_spot`, `btc:live:chainlink`, `btc:live:futures`, and
`btc:live:chainlink_shadow` with one Redis `MGET`. It does not query PostgreSQL
and does not return historical samples, probabilities, mark/index prices, open
interest, flow, book data, or persisted shadow evaluations.

Upstream extraction and Redis handoff are summarized in
[`README.md`](README.md); deployment and freshness checks are in
[`OPERATIONS.md`](OPERATIONS.md).

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

The Chainlink producer also leaves its last cached value in place during a
feed gap. If no valid expected BTC/USD Chainlink event is accepted for 10
seconds by default, its monotonic accepted-event watchdog reconnects only the
RTDS WebSocket. PING/PONG and malformed or unrelated frames do not reset that
deadline. The response shape does not change during recovery: the Chainlink
ages continue growing until a fresh accepted event overwrites the Redis value,
after which the shadow worker re-anchors automatically.

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

## Two-Second Chainlink Catch-Up Challenger

### `GET /markets/current/live/challengers/chainlink-catchup-2s`

Reads the separate short-lived `btc:live:chainlink_shadow_2s` Redis value. It
does not query PostgreSQL and does not alter the four-key `MGET`, response
shape, or accepted `signals.chainlink_catchup` value returned by
`GET /markets/current/live`. There are no query parameters.

```bash
curl "${API_BASE_URL}/markets/current/live/challengers/chainlink-catchup-2s"
```

The response wrapper is always schema version `1`. A missing or expired value,
or a value rejected by the challenger's strict decoder, returns HTTP `200`
with a null prediction:

```json
{
  "schema_version": 1,
  "server_time_ms": 1783988794075,
  "market_id": 5946629,
  "market_start_ms": 1783988700000,
  "market_end_ms": 1783989000000,
  "publication_role": "challenger",
  "prediction": null
}
```

When present, `prediction` is the complete strict two-second signal payload:

```json
{
  "schema_version": 1,
  "server_time_ms": 1783988794075,
  "market_id": 5946629,
  "market_start_ms": 1783988700000,
  "market_end_ms": 1783989000000,
  "publication_role": "challenger",
  "prediction": {
    "schema_version": 1,
    "mode": "shadow_candidate",
    "publication_role": "challenger",
    "experiment_version": "prospective_catchup_2s_v1",
    "model_version": "catchup_v1_l2000_h2000_b100",
    "beta": "1",
    "futures_lookback_ms": 2000,
    "forecast_horizon_ms": 2000,
    "generated_ms": 1783988794005,
    "target_ms": 1783988796005,
    "valid": true,
    "status": "valid",
    "invalid_reasons": [],
    "state": "anchored",
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
    "futures_reference_source_timestamp_ms": 1783988790826,
    "futures_reference_received_ms": 1783988791015,
    "futures_reference_target_ms": 1783988791346,
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
```

The API adds `signal_age_ms = max(0, server_time_ms - generated_ms)` without
changing its Decimal strings. `target_ms` is exactly two seconds after
`generated_ms`; the independent `futures_lookback_ms` is also two seconds for
this experiment. A well-formed payload with `valid: false` remains an object;
consumers must clear any earlier projection and display its status instead.

This endpoint is an unselected, lag-only challenger. It must not replace or be
presented as the accepted `signals.chainlink_catchup` model. It also does not
include the normal-basis or basis-implied component discussed in later hybrid
research; that work still requires broader chronological evaluation.

A malformed challenger is logged without its raw contents and produces the
same HTTP `200` null response as an absent key. A Redis connection/read failure
returns HTTP `503` with:

```json
{ "detail": "live cache unavailable" }
```

## Common Errors

Application-raised FastAPI errors normally use this JSON shape:

```json
{ "detail": "human-readable message" }
```

An unhandled internal HTTP `500`, including a PostgreSQL read failure on a
shadow-evaluation route, can instead use a generic plain-text body. Clients
should branch on HTTP status and tolerate either content type; the `apiGet`
helper above does so.

Expected statuses are:

| Status | Meaning |
| --- | --- |
| `404` | The requested source or historical market has no stored data; a current shadow-evaluation window and a known market with no retained evaluation points instead return HTTP `200` with `points: []` |
| `405` | The route was called with a method other than `GET` |
| `422` | A typed path/query value is invalid, such as a non-integer market ID, a discovery `limit` outside `1..50`, or a missing/unsupported shadow-evaluation `model_version` |
| `500` | A PostgreSQL-backed shadow-evaluation request failed or its persisted result violated the public reporting invariants |
| `503` | A live route cannot read Redis, one of `/markets/current/live`'s three actual source-price payloads is malformed, or `/healthz` cannot query PostgreSQL |

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
