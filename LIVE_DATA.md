# Live BTC Data Pipeline

This document explains how the application obtains the three latest BTC prices,
places them in the live cache, and exposes them to a dashboard through
`GET /markets/current/live`.

The endpoint is deliberately a latest-value endpoint. PostgreSQL remains the
historical source of record, while Redis holds only the most recently received
value from each live price source.

## End-to-End Flow

```text
Binance Spot WebSocket ───────────────┐
                                      │
Polymarket RTDS (Chainlink BTC/USD) ──┼─> Redis ─> FastAPI live endpoint ─> Dashboard
                                      │      │
Binance USD-M Futures REST ───────────┘      └─ latest values only

Each collector ─────────────────────────> PostgreSQL historical tables
```

For its live values, the dashboard does not connect to Binance, Polymarket,
PostgreSQL, or Redis directly. It reads one read-only HTTP endpoint. The API
retrieves all three cached values with one Redis `MGET` and performs no
PostgreSQL query on this request path.

There are three live price sources, even though two of them are operated by
Binance:

| Live price source | Upstream transport | Price field | Source-time field | Redis key | Response path |
| --- | --- | --- | --- | --- | --- |
| Binance Spot | WebSocket `btcusdt@ticker` | `c` | `E` | `btc:live:binance_spot` | `prices.binance_spot` |
| Chainlink BTC/USD through Polymarket | Polymarket RTDS WebSocket | `payload.value` | `payload.timestamp` | `btc:live:chainlink` | `prices.chainlink` |
| Binance USD-M Futures | REST `/fapi/v2/ticker/price` | `price` | `time` | `btc:live:futures` | `futures.last` |

All prices are parsed and calculated as Python `Decimal` values. They are
serialized as JSON strings so a binary floating-point conversion does not occur
between collection and the dashboard.

## 1. Binance Spot

The Binance Spot collector connects to:

```text
wss://stream.binance.com:9443/ws/btcusdt@ticker
```

For every valid ticker update, the collector:

1. Confirms that `s` is `BTCUSDT`.
2. Parses the last-price field `c` as a finite, positive `Decimal`.
3. Parses `E` as the Binance provider event time in UTC epoch milliseconds.
4. Records `received_ms` from the collector's local UTC clock.
5. Writes the value immediately to `btc:live:binance_spot`.
6. Updates the in-memory latest-price store used by the PostgreSQL sampler.

The Redis update happens for every accepted WebSocket ticker. Historical
storage is separate: the in-memory value is sampled at most once per UTC second
and is not written to PostgreSQL when its local receive age exceeds
`STALE_PRICE_MS` (10 seconds by default).

That PostgreSQL staleness rule does not delete the Redis value. If the feed
stops, the last cached value remains available and its age continues to grow.
The collector reconnects automatically with full-jitter exponential backoff and
also reconnects proactively after about 23 hours and 50 minutes.

## 2. Chainlink BTC/USD Through Polymarket RTDS

The Chainlink price is obtained only through Polymarket's Real-Time Data Socket
(RTDS):

```text
wss://ws-live-data.polymarket.com
```

The application does not open a direct Chainlink WebSocket. It sends this RTDS
subscription; `filters` is a JSON-encoded string in the actual message:

```json
{
  "action": "subscribe",
  "subscriptions": [
    {
      "topic": "crypto_prices_chainlink",
      "type": "*",
      "filters": "{\"symbol\":\"btc/usd\"}"
    }
  ]
}
```

For every valid `update` event, the collector:

1. Confirms the topic is `crypto_prices_chainlink` and the payload symbol is
   `btc/usd`.
2. Parses `payload.value` as a finite, positive `Decimal`.
3. Uses `payload.timestamp` as the authoritative source timestamp.
4. Records the collector's local `received_ms`.
5. Writes the value to `btc:live:chainlink` before historical storage.

For PostgreSQL, `payload.timestamp` is floored to its UTC second and used to
choose the five-minute market window. Multiple source events in the same second
update the same historical row. This deduplication does not reduce live-cache
updates: each accepted event can replace the Redis value.

The optional top-level RTDS message timestamp is stored only as historical
metadata. It is not the live price's `source_timestamp_ms`.

## 3. Binance USD-M Futures

The futures collector polls `https://fapi.binance.com` on a one-second cadence
by default. Each polling cycle requests these endpoints concurrently:

- `/fapi/v1/openInterest`
- `/fapi/v1/premiumIndex`
- `/fapi/v2/ticker/price`

Only `/fapi/v2/ticker/price` supplies the value shown at `futures.last` by the
live endpoint. Its `price` field becomes the live value and its `time` field
becomes `source_timestamp_ms`.

The same polling cycle also collects mark price, index price, funding data, and
open interest for PostgreSQL. Those fields are not written to the live Redis
key and are not returned by `/markets/current/live`.

After all three REST responses have been parsed, the collector:

1. Builds a Decimal-only futures snapshot.
2. Writes the ticker price to `btc:live:futures` when a price is present.
3. Writes the complete historical snapshot to PostgreSQL.

For this collector, `received_ms` is the local time captured at the start of the
polling cycle. If any request in the combined cycle fails, that cycle does not
replace the cached value; the prior value remains and becomes visibly older.
The futures `aggTrade` WebSocket continues to feed the historical one-second
flow table. During the opt-in Phase 2 canary, it also feeds private 100 ms OHLC
evidence capture when `RAW_FUTURES_TRACE_ENABLED=true`. The `bookTicker`
WebSocket continues to feed the historical one-second book table. Neither
WebSocket updates `btc:live:futures`; the public live futures value remains the
REST ticker price.

The private evidence path is intentionally absent from the live-flow diagram
above because Redis and the API do not read it. Its bounded PostgreSQL writer
is best-effort and is never awaited by the WebSocket reader; the live and
one-second paths do not share its queue. Chainlink high-resolution capture
remains disabled and unintegrated during Phase 2.

## Redis Live Cache

Redis is bound to `127.0.0.1:6379` and uses these exact keys:

```text
btc:live:binance_spot
btc:live:chainlink
btc:live:futures
```

Each key contains a compact JSON object:

```json
{
  "value": "62067.89000000",
  "source_timestamp_ms": 1783459249900,
  "received_ms": 1783459249950
}
```

| Redis field | Meaning |
| --- | --- |
| `value` | Price encoded as a JSON string to preserve decimal precision |
| `source_timestamp_ms` | Timestamp supplied by the upstream source; it can be `null` if a futures ticker omits `time` |
| `received_ms` | Collector-local UTC epoch time associated with receiving or starting collection of the value |

The keys are written with plain Redis `SET` operations and have no application
TTL. A Redis write failure is logged, but it does not change the price's numeric
type or prevent the collector from continuing its historical PostgreSQL write.

## Dashboard Live Endpoint

### Request

```http
GET /markets/current/live
```

Example:

```bash
curl http://127.0.0.1:9000/markets/current/live
```

The route also accepts `max_chainlink_carry_forward_ms`, defaulting to `10000`,
for compatibility. The current Redis implementation ignores this parameter: it
performs no carry-forward, filtering, or staleness rejection.

### Response

```json
{
  "server_time_ms": 1783459250123,
  "market_id": 5944864,
  "market_start_ms": 1783459200000,
  "market_end_ms": 1783459500000,
  "prices": {
    "binance_spot": {
      "value": "62067.89",
      "source_timestamp_ms": 1783459249900,
      "received_ms": 1783459249950,
      "source_age_ms": 223,
      "received_age_ms": 173,
      "provider_event_ms": 1783459249900
    },
    "chainlink": {
      "value": "62037.05",
      "source_timestamp_ms": 1783459247000,
      "received_ms": 1783459247100,
      "source_age_ms": 3123,
      "received_age_ms": 3023,
      "provider_event_ms": 1783459247000
    }
  },
  "futures": {
    "last": {
      "value": "62099.10",
      "source_timestamp_ms": 1783459250000,
      "received_ms": 1783459250050,
      "source_age_ms": 123,
      "received_age_ms": 73,
      "time_ms": 1783459250000
    }
  }
}
```

### Other Live Data Returned

In addition to the three price strings, the response supplies the timing and
market context a dashboard needs:

| Response field | Meaning |
| --- | --- |
| `server_time_ms` | API server's current UTC epoch time, used as the age reference |
| `market_id` | Identifier of the API server's current five-minute UTC market window |
| `market_start_ms` | Inclusive start of that current market window |
| `market_end_ms` | Exclusive end of that current market window |
| `source_timestamp_ms` | Timestamp attached by the upstream source |
| `received_ms` | Timestamp recorded locally by the collector |
| `source_age_ms` | `server_time_ms - source_timestamp_ms`, clamped to zero |
| `received_age_ms` | `server_time_ms - received_ms`, clamped to zero |
| `provider_event_ms` | Compatibility alias for Spot and Chainlink `source_timestamp_ms` |
| `time_ms` | Compatibility alias for Futures `source_timestamp_ms` |

The market fields are calculated from API server time when the request arrives;
they are not stored in the Redis values. Windows are half-open five-minute UTC
intervals, so a request at exactly a boundary belongs to the new window.

The age fields have different diagnostic value:

- `source_age_ms` shows how old the upstream provider says the value is.
- `received_age_ms` shows how long it has been since the collector handled the
  value.

The endpoint does not emit an `is_stale` flag. The dashboard must choose
source-appropriate freshness thresholds and derive its own fresh, stale, and
unavailable states.

### Missing, Stale, and Invalid Data

- A missing Redis key is not an endpoint failure. The route returns HTTP `200`,
  with that source's value, timestamps, ages, and compatibility alias set to
  `null`.
- A cached value is not removed or rejected merely because it is old. HTTP
  `200` therefore means the cache read succeeded, not that every value is fresh.
- A futures value may have a `null` source timestamp and `source_age_ms` if the
  upstream response omitted `time`; `received_ms` is still required.
- A Redis connection, read, or timeout failure returns HTTP `503` with
  `{"detail":"live cache unavailable"}`.
- An invalid cached payload returns HTTP `503` with
  `{"detail":"live cache payload invalid"}`.

## Recommended Dashboard Consumption

The live API is an HTTP polling endpoint, not a WebSocket or Server-Sent Events
feed. A dashboard can poll it about once per second, matching the default
futures collection cadence, while preventing overlapping requests and stopping
the poll when the component is no longer active.

```javascript
export async function fetchLiveBtc(signal) {
  const response = await fetch("/markets/current/live", {
    method: "GET",
    headers: { Accept: "application/json" },
    cache: "no-store",
    signal,
  });

  if (!response.ok) {
    throw new Error(`Live BTC request failed with HTTP ${response.status}`);
  }

  return response.json();
}

export function liveSourceState(
  source,
  {
    receivedStaleAfterMs,
    sourceStaleAfterMs = receivedStaleAfterMs,
  },
) {
  if (source.value === null || source.received_age_ms === null) {
    return "unavailable";
  }

  const receiptIsStale = source.received_age_ms > receivedStaleAfterMs;
  const providerIsStale =
    source.source_age_ms !== null &&
    source.source_age_ms > sourceStaleAfterMs;

  return receiptIsStale || providerIsStale ? "stale" : "fresh";
}
```

Dashboard handling guidelines:

- Keep `value` as a string or pass it to a decimal library. Do not assume a
  JavaScript `Number` preserves the stored precision.
- Treat `null` as unavailable and old age fields as stale, even when the HTTP
  response is `200`.
- Choose freshness thresholds per source; do not use
  `max_chainlink_carry_forward_ms` as a server-side freshness guarantee.
- Evaluate both source and receive ages when the source timestamp is present.
  A futures response without a source timestamp can still be evaluated by its
  required receive age.
- Use `market_id`, `market_start_ms`, and `market_end_ms` from each response to
  roll the dashboard into the current five-minute window.
- On HTTP `503`, show an unavailable or last-known/stale state and retry with a
  bounded client backoff.
- Read the API through the dashboard's same-origin backend or development
  proxy because the API does not install CORS middleware. Do not connect
  browser code directly to Redis.

In production, FastAPI is bound only to `127.0.0.1:9000`. Keep that port private
and use the SSH-tunnel workflow in [`OPERATIONS.md`](OPERATIONS.md) for access
from another machine.

## Data Not Returned by the Live Endpoint

`GET /markets/current/live` does **not** return:

- Polymarket Up/Down probabilities or resolution data
- Futures mark price, index price, premium, or funding data
- Open interest or open-interest notional
- Aggregated trade flow
- Top-of-book data
- Historical one-second series

Most of these datasets are stored in PostgreSQL and are available through the
historical/current-window API, primarily `GET /markets/current/data` with its
`include_probabilities`, `include_futures`, `include_oi`, `include_flow`, and
`include_book` query flags. Funding fields are stored in PostgreSQL but are not
currently serialized by the dashboard API. These datasets are separate from
the low-latency, Redis-only live response. The Polymarket probability collector
is therefore not a fourth live price source or Redis key.

For the complete frontend API contract and all historical response shapes, see
[`FRONTEND_API.md`](FRONTEND_API.md).

## Implementation Reference

- [`price_collector/collector.py`](price_collector/collector.py) — Binance Spot
  parsing, caching, sampling, and reconnect behavior
- [`price_collector/polymarket_chainlink_collector.py`](price_collector/polymarket_chainlink_collector.py)
  — Polymarket RTDS subscription and Chainlink event handling
- [`price_collector/binance_futures_collector.py`](price_collector/binance_futures_collector.py)
  — Binance futures REST polling and snapshot creation
- [`price_collector/live_cache.py`](price_collector/live_cache.py) — Redis keys,
  serialization, freshness ages, and live response construction
- [`price_collector/api.py`](price_collector/api.py) — FastAPI route
