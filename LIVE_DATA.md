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
Binance USD-M Futures aggTrade WS ────┘      └─ latest values only

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
| Binance USD-M Futures | WebSocket `btcusdt@aggTrade` | `p` | `T` | `btc:live:futures` | `futures.last` |

All prices are parsed and calculated as Python `Decimal` values. They are
serialized as JSON strings so a binary floating-point conversion does not occur
between collection and the dashboard. Phase 5 changes the futures upstream
source and delivery path without changing the live API response shape.

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

Immediately after each RTDS receive returns, the collector records local wall
time before JSON parsing or validation. That wall timestamp, floored from
nanoseconds to milliseconds, is the accepted tick's `received_ms`. When private
Chainlink evidence capture is enabled, the collector also records monotonic time
and assigns a receive sequence within the current WebSocket connection at this
same pre-parse boundary.

For every valid `update` event, the reader then:

1. Confirms the topic is `crypto_prices_chainlink` and the payload symbol is
   `btc/usd`.
2. Parses `payload.value` as a finite, positive `Decimal` and uses
   `payload.timestamp` as the authoritative source timestamp.
3. Synchronously publishes a new version to a process-local latest-wins live
   state.
4. Offers the versioned sample to the critical provider-second historical
   state.
5. When `RAW_CHAINLINK_EVENTS_ENABLED=true`, offers the individual event to the
   bounded private raw-capture queue without awaiting it, then returns to
   receiving RTDS messages without awaiting Redis or PostgreSQL.

A separate live worker attempts to write the newest published version to
`btc:live:chainlink`. If several ticks arrive while an earlier Redis call is in
flight, obsolete intermediate versions can be coalesced, but the newest version
remains pending until its live-cache attempt completes. A separate historical
worker waits until the corresponding live-cache attempt has completed before
writing PostgreSQL, preserving the Redis-before-historical-storage rule without
delaying the RTDS reader.

For PostgreSQL, `payload.timestamp` is floored to its UTC second and used to
choose the five-minute market window. The historical state is versioned by that
provider second. It coalesces replacements while a second is pending, and holds
the newest provider second for 1,000 ms after its latest local receipt unless a
newer provider second makes it ready sooner. This reduces repeated same-second
upserts. A later corrective tick can still update the same `price_samples` row,
including after an earlier version was written; a newer tick that arrives while
a write is in flight remains pending for a follow-up upsert. The pending
historical state is capped at 5,000 provider seconds; overflow and worker
failures are explicitly counted and logged instead of being silent. Shutdown
performs a bounded final drain.

The optional top-level RTDS message timestamp is stored as historical and raw
evidence metadata. It is not the live price's `source_timestamp_ms`.

Private Chainlink evidence is deliberately independent of both delivery
workers. Every valid tick, including an unchanged price or another tick received
in the same local millisecond, is offered without intentional sampling to
`raw_capture.chainlink_price_events`. The connection UUID and receive sequence
preserve order. This bounded raw queue is best-effort: any overload or database
loss is counted and makes the affected evidence interval unsuitable for
analysis, but it cannot block or discard the normal Redis and one-second paths.
With the flag `false`, the Chainlink process creates no raw queue, writer task,
session UUID, or dedicated raw database connection.

## 3. Binance USD-M Futures

The futures collector requires the Binance USD-M `btcusdt@aggTrade` WebSocket.
For every accepted trade, it uses `p` as the exact Decimal last price and `T` as
the source timestamp. Local wall time is recorded immediately after `recv()`
returns and before JSON parsing; that pre-parse time, floored to milliseconds,
becomes the live value's `received_ms`.

Accepted, non-duplicate, non-regressing trades publish to process-local
latest-wins state. A dedicated worker writes the newest pending version to
`btc:live:futures`. It does not wait for the one-second REST poll, PostgreSQL,
or the optional raw writer. When updates arrive while Redis is in flight,
obsolete intermediate versions may be coalesced, but the newest one remains
eligible for an attempt.

The collector also polls `https://fapi.binance.com` on a one-second cadence by
default. Each snapshot cycle requests only these endpoints concurrently:

- `/fapi/v1/openInterest`
- `/fapi/v1/premiumIndex`

REST still supplies mark price, index price, funding data, and open interest for
PostgreSQL. Historical open-interest polling also remains separate. There is no
call or fallback to `/fapi/v2/ticker/price`, and neither book midpoint nor
microprice is treated as last price. Those REST and book fields are not written
to the live Redis key and are not returned by `/markets/current/live`.

After both REST responses have been parsed, the collector:

1. Builds a Decimal-only futures snapshot.
2. Adds `aggTrade.p` only when the latest accepted trade is fresh under
   `STALE_PRICE_MS` and belongs to the currently open WebSocket connection.
3. Waits until that trade's Redis attempt has completed, then writes the
   historical snapshot to PostgreSQL. A Redis failure is logged but does not
   discard an otherwise valid snapshot.

The historical snapshot's second and market window are based on the premium
index timestamp, falling back to the snapshot observation time. They are not
keyed by the trade timestamp. `futures_last_price_time_ms` still records
`aggTrade.T`, while the row's historical `received_ms` remains the local
snapshot observation time. This distinction keeps the REST observation aligned
without changing the meaning of the WebSocket source timestamp.

Until the first valid trade on a connection, while disconnected, or after the
latest trade exceeds `STALE_PRICE_MS`, the Redis key is not deleted or replaced:
its old value simply ages. REST snapshot cycles can continue, but the affected
rows store `null` last-price and last-price-source-time fields. There is no
ticker fallback. `BINANCE_FUTURES_STREAMS_ENABLED` must therefore be `true`;
the collector refuses to start when it is `false`.

The same `aggTrade` reader continues to feed the historical one-second flow
table and can independently feed private 100 ms OHLC evidence capture when
`RAW_FUTURES_TRACE_ENABLED=true`. The `bookTicker` WebSocket continues to feed
the historical one-second book table. Turning raw capture off does not disable
the live price, flow aggregation, connection identity, or pre-parse receive
stamp, and turning it on does not change the public price selection.

The private futures and Chainlink evidence paths are intentionally absent from
the live-flow diagram above because Redis and the API do not read them. Their
bounded PostgreSQL writers are best-effort and are never awaited by either
WebSocket reader; the live and one-second paths do not share their queues. Both
capture flags remain opt-in and default to `false` even after their collector
integrations are deployed.

The rollout uses explicitly accelerated three-hour validation gates for the
futures-only Phase 2 canary and the subsequent Chainlink Phase 3 canary. These
short gates provide less confidence than 24 hours about slow leaks, reconnect
behavior, daily traffic variation, and sustained storage growth. Passing a gate
allows the next phase to start; it does not stop observation. Leave each
successful capture enabled and continue monitoring its normal and raw paths
toward at least 24 uninterrupted hours from that collector's activation time.
Any later regression still invokes the phase's documented rollback.

Phase 4's deliberate partition-boundary and retention validation has been
explicitly deferred while Phase 5 proceeds. It remains unproven in production:
a three-hour window may not cross a six-hour raw partition boundary. Future
partition creation, expired-partition removal, configured 72-hour retention,
and sustained relation-budget enforcement remain known residual risks; the
Phase 5 source cutover does not validate them.

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
| `source_timestamp_ms` | Timestamp supplied by the upstream source; for futures this is required `aggTrade.T` |
| `received_ms` | Collector-local UTC epoch time recorded for the value; for futures this is the pre-parse WebSocket receive time |

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
- A valid newly written futures value has both `aggTrade.T` and its pre-parse
  receive time. During a disconnect or stale interval the old Redis value is
  retained and its ages grow; it is not replaced with a `null` snapshot value.
- A Redis connection, read, or timeout failure returns HTTP `503` with
  `{"detail":"live cache unavailable"}`.
- An invalid cached payload returns HTTP `503` with
  `{"detail":"live cache payload invalid"}`.

## Recommended Dashboard Consumption

The live API is an HTTP polling endpoint, not a WebSocket or Server-Sent Events
feed. Although the futures Redis value is updated from the WebSocket rather
than the one-second REST cycle, a dashboard can still poll about once per
second while preventing overlapping requests and stopping the poll when the
component is no longer active.

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
- Evaluate both source and receive ages. Newly written futures values always
  carry the required trade timestamp and local receive timestamp.
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
  — Binance futures live delivery, REST polling, and snapshot creation
- [`price_collector/binance_futures_streams.py`](price_collector/binance_futures_streams.py)
  — required futures WebSocket parsing, validated latest-trade state, flow, and
  book aggregation
- [`price_collector/live_cache.py`](price_collector/live_cache.py) — Redis keys,
  serialization, freshness ages, and live response construction
- [`price_collector/api.py`](price_collector/api.py) — FastAPI route
