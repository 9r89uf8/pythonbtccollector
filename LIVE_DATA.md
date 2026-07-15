# Live BTC Data Pipeline

This document explains how the application obtains the three latest BTC prices,
places them in the live cache, and exposes them through
`GET /markets/current/live`. The same response can include one optional,
short-lived Chainlink catch-up signal; that signal is not a fourth price source.

The endpoint is deliberately a latest-value endpoint. PostgreSQL remains the
historical source of record, while Redis holds only the most recently received
value from each live price source.

## End-to-End Flow

```text
Binance Spot WebSocket ───────────────┐
                                      │
Polymarket RTDS (Chainlink BTC/USD) ──┼─> Redis ─> one four-key MGET ─> FastAPI
                                      │      │
Binance USD-M Futures aggTrade WS ────┘      └─ latest values only

Each collector ─────────────────────────> PostgreSQL historical tables
```

Consumers do not connect to Binance, Polymarket, PostgreSQL, or Redis directly.
They read one read-only HTTP endpoint. The API retrieves the three cached source
prices and the optional shadow value with one Redis `MGET` and performs no
PostgreSQL query on this request path.

Shadow-signal Phases 4 and 5 added a separate, opt-in branch. The standalone
`price-collector-shadow-signal` worker reads only the Futures and Chainlink
source keys together every 100 ms and writes a short-lived experimental result.
When Phase 5 evaluation is separately enabled, those observations also feed a
noncritical outcome path:

```text
[btc:live:futures] ----\
                        +-- MGET every 100 ms --> ShadowSignalEngine
[btc:live:chainlink] --/                              |
                                      +----------------+----------------+
                                      |                                 |
                                      v                                 v
                          atomic SET with TTL               every entered 500 ms bucket
                                      |                     schedule all candidates
                                      v                                 |
                    [btc:live:chainlink_shadow]                         v
                         1.5–2.0 seconds                    horizon-specific pending heap
                                                                        |
                                                                        v
                                                           causal Chainlink outcome
                                                                        |
                                                                        v
                                                          bounded PostgreSQL writer
```

Phases 4 and 5 deliberately kept both outputs out of FastAPI. Phase 6 now
serializes only the short-lived Redis signal as the optional nested
`signals.chainlink_catchup` field. The later Phase 7 backend prerequisite
exposes a restricted projection of persisted evaluations through separate,
bounded PostgreSQL reporting routes; the base table remains internal.
PostgreSQL evaluation writes run behind a bounded nonblocking queue, so a
database outage, retry, or dropped evaluation cannot interrupt the Redis
signal, either producer, or the live endpoint. This repository contains no
dashboard implementation; that remains in a different repository.

There are three live price sources, even though two of them are operated by
Binance:

| Live price source | Upstream transport | Price field | Source-time field | Redis key | Response path |
| --- | --- | --- | --- | --- | --- |
| Binance Spot | WebSocket `btcusdt@ticker` | `c` | `E` | `btc:live:binance_spot` | `prices.binance_spot` |
| Chainlink BTC/USD through Polymarket | Polymarket RTDS WebSocket | `payload.value` | `payload.timestamp` | `btc:live:chainlink` | `prices.chainlink` |
| Binance USD-M Futures | WebSocket `btcusdt@aggTrade` | `p` | `T` | `btc:live:futures` | `futures.last` |

All prices are parsed and calculated as Python `Decimal` values. They are
serialized as JSON strings so a binary floating-point conversion does not occur
between collection and an API consumer. The Phase 6 `signals` addition leaves
the existing `prices` and `futures` shapes unchanged.

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

Redis is bound to `127.0.0.1:6379`. The three source-price keys are:

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

### Experimental Shadow-Signal Phases 4–6

The opt-in worker writes one additional key:

```text
btc:live:chainlink_shadow
```

This is not a fourth source price and does not use the `LivePrice` shape. It is
a typed `LiveShadowSignal` payload whose decimal values remain JSON strings.
The payload identifies the frozen selection and model, input and anchor
timestamps, source ages, market window, catch-up horizon, and whether the full
horizon fits before the market ends. It describes only an experimental
Chainlink catch-up projection; it is not a settlement, probability, execution,
or market-close forecast.

The worker runs on epoch-aligned 100 ms boundaries and obtains Futures and
Chainlink with one `MGET`. It keeps the three provisional candidates
(`catchup_ratio_l3000_b100`, `catchup_ratio_l3500_b100`, and
`catchup_ratio_l4000_b100`) instantiated, but publishes only the provisional
primary frozen by the accepted Phase 3 selection artifact. The currently
accepted production artifact selects `catchup_ratio_l3000_b100`; the worker
discovers that value from the artifact and never dynamically reranks or falls
back to another candidate.

Before opening Redis, the service validates two immutable files inside
`/var/lib/price-collector/shadow-decisions`: the accepted selection and the
exact replay report containing its runtime configuration. Both must be
`root:pricecollector` mode `0440`. The dedicated root-owned
`/etc/price-collector/shadow-signal.env` supplies their absolute paths and the
complete selection-file SHA-256. Startup also verifies the replay report hash
against selection provenance and verifies its configuration digest, policy,
candidate set, evidence, and primary. The shadow environment contains the
writer `DATABASE_URL` only when matured evaluation is enabled. It never
contains the API's `READ_DATABASE_URL`.

Each observation is written atomically with Redis `SET` and a 1.5-to-2.0-second
TTL; the configured default is 2,000 ms. Invalid observations still overwrite
the key, setting `valid=false` and every projection and anchor-dependent field
to `null`. Missing, malformed, stale, regressing, or insufficient-history input
therefore cannot leave a previous valid forecast visible. If the process stops,
the TTL removes the key without deleting or changing any source-price key.

Phase 5 schedules all three candidates once when the worker enters an
epoch-aligned 500 ms bucket. It does not synthesize forecasts for buckets missed
during a pause. Valid and invalid attempts are both retained so coverage cannot
be inflated by discarding failures. Each candidate matures independently at
`target_ms = generated_ms + horizon_ms`. The causal outcome is the newest
Chainlink observation actually known by that target, which requires
`actual_chainlink_received_ms <= target_ms`; a later observation visible at the
maturation tick is excluded.

The forecast clock is stamped after the input `MGET`. Any input received after
that stamp makes the evaluation invalid, and an observation gap longer than two
poll intervals invalidates outstanding outcomes across the gap. Causality is
therefore exact for cache states returned by successful worker polls. Redis is
still a latest-value cache: a Chainlink state created and overwritten entirely
between two polls cannot be reconstructed here, so raw replay remains the
event-complete authority for model selection and sub-poll timing analysis.

Matured attempts are enqueued without awaiting PostgreSQL. The bounded writer
batches idempotent inserts into `shadow_signal_evaluations`; its unique key is
`(model_version, generated_ms, horizon_ms)`. When its queue is full, it drops
the oldest queued row and emits a rate-limited warning on the first and every
hundredth drop. Failed idempotent batches are requeued ahead of newer rows and
retried on that background path; the queue sheds its oldest evidence only after
bounded capacity is exhausted.
The default retention is seven days; every 300 seconds the writer may
delete up to 5,000 expired rows, enough for a conservative five-candidate
capacity envelope at a 500 ms cadence. For valid rows with a causal actual,
`forecast_error` is the signed
`projected_chainlink - actual_chainlink`, while `baseline_error` is the signed
no-change error `chainlink_at_forecast - actual_chainlink`.

Move-size, direction, expiry, and sampled-volatility slices are derivable from
the evaluation rows. A reconnect slice must join by receive time to
`raw_capture.feed_sessions`; materialize it before the separate 72-hour raw
retention removes the required session evidence.

Phase 5 deliberately added no API response field or dashboard integration. A
later read-only reporting layer exposes only selected chart columns through
`shadow_signal_evaluation_chart_points`; the base table and writer remain
internal. This is also unrelated to, and does not validate, the separately
deferred high-resolution raw-capture Phase 4 partition and 72-hour-retention
rollout.

Phase 6 adds the shadow key to the live endpoint's existing Redis read. The API
requests `btc:live:binance_spot`, `btc:live:chainlink`, `btc:live:futures`, and
`btc:live:chainlink_shadow` together in one four-key `MGET`; it does not perform
a separate shadow `GET`. The three source-price slots retain their existing
`LivePrice` decoder and response shape. The shadow slot uses its own strict
typed decoder and is returned at `signals.chainlink_catchup`, never forced into
a price field.

A missing or expired shadow key is normal and serializes as
`signals.chainlink_catchup: null`. A malformed shadow payload is logged without
its raw contents and is isolated to that same `null` slot; it does not suppress
otherwise readable actual prices. A well-formed `valid=false` signal remains a
useful typed diagnostic and is returned as an object. The API adds only
`signal_age_ms = max(0, server_time_ms - generated_ms)` to the cached payload.

`GET /markets/current/live` does not query PostgreSQL, read evaluation rows,
import or run the model, or hold model state. Phase 6 remains a Redis-only live
exposure. The separate `/markets/.../shadow-evaluations` reporting routes use
the API's PostgreSQL reader and restricted view without changing this live
request path. Dashboard implementation remains in a different repository.

## Redis-Only Live Endpoint

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
| `signals.chainlink_catchup` | Complete typed shadow payload, or `null` when unavailable or malformed |
| `signal_age_ms` | `server_time_ms - generated_ms`, clamped to zero |

The market fields are calculated from API server time when the request arrives;
they are not stored in the Redis values. Windows are half-open five-minute UTC
intervals, so a request at exactly a boundary belongs to the new window.

The age fields have different diagnostic value:

- `source_age_ms` shows how old the upstream provider says the value is.
- `received_age_ms` shows how long it has been since the collector handled the
  value.
- `signal_age_ms` shows how long it has been since the worker generated the
  nested catch-up signal.

The source-price objects and their compatibility aliases are unchanged by the
additional `signals` object. Shadow decimal fields such as `beta`,
`current_chainlink`, `projected_chainlink`, `pending_move`,
`pending_move_bps`, `futures_now`, and `futures_reference` are strings or
`null`; do not parse them with binary floating-point. A signal's nested market
window is generation-time context and can briefly differ from the top-level
request-time market window around a five-minute boundary.

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
- A missing or expired shadow key returns
  `signals.chainlink_catchup: null`. A well-formed `valid=false` shadow signal
  is returned as an object so its `status` and `invalid_reasons` remain visible;
  consumers must not reuse a preceding valid projection.
- A malformed shadow payload is logged without its raw contents and returns
  `signals.chainlink_catchup: null`. The endpoint remains HTTP `200` when the
  three actual cached payloads are readable.
- A Redis connection, read, or timeout failure returns HTTP `503` with
  `{"detail":"live cache unavailable"}`.
- An invalid payload in one of the three actual source-price keys returns HTTP
  `503` with
  `{"detail":"live cache payload invalid"}`.

## Recommended Future Dashboard Consumption

Phase 6 implements only the backend response contract. No dashboard code or
assets are included in this repository; a separate dashboard repository can
consume this endpoint during Phase 7.

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

- Persisted evaluation history, candidate rankings, or replay reports
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

Persisted shadow evaluations are intentionally not folded into `/data`.
Request them through either dedicated, one-model, one-window reporting path:

```text
GET /markets/current/shadow-evaluations?model_version=catchup_ratio_l3000_b100
GET /markets/{market_id}/shadow-evaluations?model_version=catchup_ratio_l3000_b100
```

Those routes use PostgreSQL and remain separate from `/live`.

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
  four-key live reads, independent source-price and shadow decoding, atomic
  shadow TTL writes, freshness ages, and live response construction
- [`price_collector/shadow_signal.py`](price_collector/shadow_signal.py) — pure
  catch-up models, history, anchors, and validity state
- [`price_collector/shadow_signal_artifact.py`](price_collector/shadow_signal_artifact.py)
  — trusted Phase 3 selection and replay-configuration validation
- [`price_collector/shadow_signal_collector.py`](price_collector/shadow_signal_collector.py)
  — standalone epoch-aligned 100 ms Redis worker and Phase 5 integration
- [`price_collector/shadow_signal_evaluation.py`](price_collector/shadow_signal_evaluation.py)
  — 500 ms scheduler, causal horizon maturation, and bounded async writer
- [`price_collector/shadow_signal_reporting.py`](price_collector/shadow_signal_reporting.py)
  — bounded read-only evaluation query and response validation
- [`price_collector/db.py`](price_collector/db.py) — idempotent matured-evaluation
  batches and bounded retention deletion
- [`price_collector/api.py`](price_collector/api.py) — FastAPI route
