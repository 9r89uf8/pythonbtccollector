# Price Collector

Production-oriented BTC market-data collection for a single-user Ubuntu 24.04
DigitalOcean droplet. The application collects spot, oracle, futures, order-flow,
top-of-book, and Polymarket probability data into local PostgreSQL. Redis holds
the latest source prices and the latest finalized microstructure second needed
by the live API responses.

The deployment is deliberately private:

- PostgreSQL listens only on the droplet loopback interface.
- Redis listens only on `127.0.0.1:6379` with protected mode enabled.
- FastAPI listens only on `127.0.0.1:9000`.
- Remote API access goes through an SSH tunnel.
- No Docker, public dashboard, public database port, or public API port is used.

## Navigation

- [Architecture and collectors](#architecture), [data storage](#data-storage)
- [API routes](#api), [microstructure API](#microstructure-api), [ghost forecasts](#ghost-twap--optional-worker)
- [Local development](#local-development), [configuration](#configuration), [initial droplet setup](#initial-droplet-deployment)
- [Production operations](#production-operations): [routine checks](#routine-checks), [updates](#deploy-updates-from-github), [history retention](#ten-day-collector-history-retention), [ghost retention](#continuous-ghost-retention-and-accuracy)
- [Research findings and forecast interpretation](GHOST_TWAP_REFERENCE.md)

## Architecture

```text
DigitalOcean droplet
├── systemd
│   ├── price-collector.service
│   ├── price-collector-polymarket-chainlink.service
│   ├── price-collector-binance-futures.service
│   ├── price-collector-polymarket-probabilities.service
│   ├── price-api.service
│   └── price-collector-retention.timer → price-collector-retention.service
├── /opt/price-collector              Git checkout and Python virtualenv
├── /etc/price-collector              Root-owned environment files
├── /var/lib/price-collector          Collector state
├── Redis on 127.0.0.1:6379           Live-value cache only
└── PostgreSQL database price_collector
                                      Historical source of record
```

The API uses a read-only PostgreSQL role. Collectors use a separate writer role.
All prices remain `Decimal` values in Python, are stored in PostgreSQL numeric
columns, and are serialized as strings by the API.

Non-ghost history has a ten-day maximum, with stricter raw capture normally at
72 hours. Continuous ghost forecasts/results retain seven days, and hourly
accuracy/feed-health summaries retain 90 days. Separate bounded workers enforce
these scopes; legacy canary export rules remain distinct. See
[history retention](#ten-day-collector-history-retention) and
[ghost retention](#continuous-ghost-retention-and-accuracy) for cutoff, backlog,
privilege and storage details.

## Collectors

### Binance Spot

`python -m price_collector.collector`

- Connects to `wss://stream.binance.com:9443/ws/btcusdt@ticker`.
- Parses ticker field `c` as the last price and `E` as provider event time.
- Writes each newly received latest value to Redis key
  `btc:live:binance_spot` immediately.
- Keeps the latest price in memory and writes at most one PostgreSQL sample per
  UTC second.
- Skips a sample when the latest received price is older than
  `STALE_PRICE_MS`, which defaults to `10000`.
- Reconnects with exponential full-jitter backoff capped at 60 seconds and
  proactively reconnects after about 23 hours 50 minutes.

### Polymarket Chainlink BTC/USD and 60-second TWAP

`python -m price_collector.polymarket_chainlink_collector`

- Connects to Polymarket RTDS at `wss://ws-live-data.polymarket.com`.
- Keeps the standard `crypto_prices_chainlink` / `payload.value` feed as spot
  context and writes it to `btc:live:chainlink` before historical storage.
- In parallel, subscribes to `crypto_prices_twap_sixty`, requires
  `payload.window_s = 60`, parses exact `payload.full_accuracy_value` with
  `Decimal`, and writes `btc:live:chainlink_twap_60s` before PostgreSQL.
- Persists every accepted TWAP event plus connection sessions and explicit
  no-replay gaps. The one-second `price_samples` TWAP series is a materialized
  display layer; settlement research reads the exact event history.
- Floors the source payload timestamp to its UTC second and upserts that second
  into PostgreSQL.
- Proactively reconnects either active-but-unproductive RTDS socket when no
  valid expected-topic BTC/USD event is accepted for 10 seconds by default. Before the
  first accepted tick, the empty RTDS bootstrap frame and the narrowly validated
  `crypto_prices` subscription-history dump are received-only startup frames,
  not parse errors. They, control frames, and malformed frames do not reset that
  monotonic deadline.

Polymarket changed BTC five-minute settlement at the UTC market boundary
`2026-08-14T00:00:00Z`. Earlier markets and their stored evidence retain the
historical `BTCUSD_TWAP_30S` / `crypto_prices_twap_thirty` / 30-second identity;
the market starting exactly at the boundary and all later markets use the
`BTCUSD_TWAP_60S` / `crypto_prices_twap_sixty` / 60-second identity. Gamma rule
versions and source URLs likewise remain `btc-5m-twap-30` /
`btc-usd-twap-30s-streams` before the cutover and `btc-5m-twap-60` /
`btc-usd-twap-60s-streams` from the cutover onward. Retained historical rows are
not renamed or recomputed. The ten-day history policy may delete expired
30-second events and their unneeded parents; preserving settlement identity
does not exempt those records from age retention.

### Binance USD-M Futures

`python -m price_collector.binance_futures_collector`

- Uses the required `btcusdt@aggTrade` WebSocket as the futures last-price
  source: `p` is the price and `T` is its source timestamp.
- Records local wall time immediately after `recv()` and before parsing, then a
  latest-wins worker writes accepted trades to Redis key `btc:live:futures`
  independently of REST polling and optional raw capture.
- Polls futures REST only for premium/index, funding, open interest, and the
  separate historical open-interest series. It does not call the REST ticker
  endpoint and does not use book midpoint or microprice as "last."
- Stores futures snapshots and completed five-minute historical open-interest
  summaries. A snapshot uses only a fresh trade from the current WebSocket
  connection; otherwise its last-price fields are `null` while the REST fields
  can still be stored. `STALE_PRICE_MS` controls that freshness gate.
- Aggregates `btcusdt@aggTrade` into one-second `binance_flow_1s` rows.
- Aggregates `btcusdt@bookTicker` into one-second `binance_book_1s` rows.
- Can additionally produce one causal `binance_microstructure_1s` research row
  per second when `BINANCE_MICROSTRUCTURE_ENABLED=true`. That path reuses the
  accepted futures `aggTrade` feed and REST premium/OI snapshot, and adds a
  combined spot aggregate-trade/top-10 stream, futures top-10 depth, and the
  censored futures forced-order stream. It retains Decimal summaries, not raw
  messages.
- Publishes that same latest finalized one-second row to Redis key
  `btc:live:microstructure`. Redis holds only the newest finalized row; the
  PostgreSQL table remains the source of record for current and completed
  five-minute history.
- Finalized PostgreSQL rows drain through an independent bounded queue, so a
  slow or locked historical write cannot delay later Redis seconds. At queue
  capacity the oldest unwritten row is dropped and logged, retaining the
  newest history without blocking the live path.
- The microstructure row includes spot/perpetual aggressive flow, top-1/5/10
  depth imbalance, spread, weighted midpoint, sampled BBO OFI, spot/perpetual
  basis, RPI-involved perpetual flow, mark/index/funding/OI context, observed
  long/short forced fills, and source age/lag/skew/gap health fields. A Binance
  forced-order snapshot is observed liquidation stress, not total liquidated
  volume or a future liquidation level.
- Can optionally coalesce the same `aggTrade` feed into private 100 ms OHLC
  evidence rows when `RAW_FUTURES_TRACE_ENABLED=true`. The raw flag does not
  select or disable the public price source; `BINANCE_FUTURES_STREAMS_ENABLED`
  must be `true` for the collector to run.

### Polymarket BTC 5-Minute Probabilities

`python -m price_collector.polymarket_probability_collector`

- Discovers the current BTC Up/Down five-minute market through Polymarket
  Gamma.
- Subscribes to the market's Up and Down tokens through the Polymarket CLOB
  WebSocket.
- Stores one-second bid, ask, midpoint, and normalized probability snapshots
  when the source data is complete and fresh.
- Preloads the next market before the current five-minute boundary.
- Reconciles ended markets against Polymarket Gamma and CLOB REST data, with
  durable retries for resolutions that are not official yet.
- Stores `reconciled_settlement_rule_version` with each complete, rule-validated
  resolution. A missing or stale marker keeps that market in reconciliation
  until the official result has been revalidated against the market's exact
  settlement identity.
- In bounded pages, scans completed windows from
  `POLYMARKET_MARKET_BACKFILL_START_MS` through the last completed market,
  advancing oldest to newest. The floor defaults to the 60-second cutover. It
  cannot precede that cutover and repairs missing or noncanonical Gamma
  metadata to the exact 60-second settlement identity, then wraps to retry
  failures so one bad window cannot starve later windows.
  Ordinary resolution reconciliation then processes those recovered markets.
- The deployment-gap backfill is metadata-only. It never fabricates historical
  probability snapshots, TWAP events, or TWAP samples; missing observations
  remain explicitly missing.
- Accepts current markets only when their own Gamma rule metadata identifies
  the supported BTC five-minute 60-second TWAP settlement source. Unknown or
  contradictory rules fail closed and are not collected. Historical markets
  before `2026-08-14T00:00:00Z` retain their 30-second settlement identity.
- Stores Polymarket's exact published Price to Beat and official final price,
  rule identity, winner or split result, official payouts, winning token ID,
  and resolution timestamp. Probability quotes never infer the winner.
- Tracks Up and Down probability component timestamps independently so a fresh
  update on one token cannot make a stale opposite-side quote appear fresh.

## Five-Minute Market Windows

Every persisted one-second sample is assigned to a half-open UTC market window:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

For example, `[04:05:00.000, 04:10:00.000)` is one market. A sample at exactly
`04:10:00.000` belongs to the new `[04:10:00.000, 04:15:00.000)` market.

## Data Storage

PostgreSQL is the historical source of record. The main tables are:

- `providers`, `instruments`, and `market_windows`
- `price_samples` for Binance Spot, standard Chainlink context, and the
  one-second Chainlink TWAP materialization
- `polymarket_twap_sessions`, `polymarket_twap_events`, and
  `polymarket_twap_gaps` for durable exact TWAP evidence and coverage gaps
- `polymarket_btc_5m_markets`, `polymarket_probability_samples`, and
  `polymarket_btc_5m_resolutions` for discovered markets, probability history,
  and official Polymarket resolution metadata
- `binance_futures_snapshots`
- `binance_futures_oi_5m_summaries`
- `binance_flow_1s`
- `binance_book_1s`
- `binance_microstructure_1s` for the optional receipt-time-aligned research
  summary

Prices in `price_samples` use `NUMERIC(38,18)`. Its primary key is
`(instrument_id, sample_second_ms)`, and a duplicate sample for the same
instrument and second updates the existing row instead of creating a duplicate.
The other one-second tables follow the same upsert pattern with source-specific
keys.

`binance_microstructure_1s.sample_second_ms` labels the start of the local
receipt interval `[sample_second_ms, sample_second_ms + 1000)`. The coordinator
waits for the configured short flush delay, heap-orders events by their
pre-parse wall-receive timestamp, and never moves an event received on or after
the interval boundary backward into the previous row. This also keeps exact
five-minute boundaries in the correct half-open market window.

The compatibility columns `spot_trade_id_span` and `fut_trade_id_span` count
the underlying trades represented by the accepted aggregate-trade messages in
that receipt interval. They sum each message's first-to-last trade-ID span;
they are not gaps between aggregate-trade IDs. The legacy `*_trade_id_span`
names are retained so starter-derived research features keep the same contract.

### High-resolution evidence capture

The high-resolution-capture foundation adds an isolated PostgreSQL schema and
tested bounded-write infrastructure:

- `raw_capture.binance_futures_price_trace_100ms` is the partitioned
  destination for one compact OHLC row per futures WebSocket connection and
  active 100 ms receive bucket.
- `raw_capture.chainlink_price_events` is the partitioned destination for every
  individual valid RTDS tick successfully accepted by the best-effort capture
  queue; same-millisecond and unchanged-price ticks are not coalesced.
- `raw_capture.feed_sessions` stores connection/session metadata for each
  enabled futures or Chainlink capture source.

The `raw_capture` schema is owned separately from the normal historical tables,
is unavailable to `PUBLIC` and the API's `price_reader` role, and grants the
collector writer DDL capability only inside that schema. Six-hour current and
next partitions are managed independently from the public historical tables.

Phase 2 wires the Binance futures `aggTrade` reader to this infrastructure but
keeps raw capture opt-in. Both raw feature flags still default to `false`. When
`RAW_FUTURES_TRACE_ENABLED=true`, receive clocks are recorded immediately after
`recv()` and before JSON parsing, valid trades continue through the existing
one-second flow path, and compact 100 ms buckets are offered without waiting
for the dedicated raw database writer. Connection/session records, bounded
queueing, batched `COPY`, and partition maintenance run only while capture is
enabled.

Phase 3 integrates the Chainlink RTDS reader while keeping
`RAW_CHAINLINK_EVENTS_ENABLED` opt-in. The reader records wall receive time
immediately after `recv()` and before parsing, synchronously updates versioned
latest-wins live state, publishes the critical provider-second historical
version, offers each valid raw event without waiting when enabled, and returns
to RTDS receive work. A separate worker attempts Redis for the newest live
version, which remains pending until that attempt completes. The historical
worker waits for the relevant live-cache attempt and holds the newest provider
second for 1,000 ms after its latest receipt unless a newer second makes it
ready sooner. Pending versions for one provider second are coalesced, reducing
repeated same-second upserts, but a later corrective tick can upsert that second
again even after an earlier write. An update received during an in-flight write
also remains pending for a follow-up upsert. The historical store is bounded at
5,000 pending provider seconds with explicit overflow and failure counters.
Shutdown uses a bounded final drain. The raw queue is independent of both normal
delivery states.

Phase 5 cuts the public futures last price over to the required Binance
`btcusdt@aggTrade` WebSocket. The Redis value is `p`, its
`source_timestamp_ms` is `T`, and `received_ms` is the local pre-parse receive
wall time. The latest-wins Redis worker is independent of the one-second REST
snapshot and raw-capture paths. REST remains responsible only for premium/index,
funding, open interest, and historical open-interest data; there is no REST
ticker fallback, and book-derived values are not labeled as last price. The API
shape is unchanged.

The public standard Chainlink context value remains RTDS `payload.value`
delivered through `btc:live:chainlink`. The current settlement-reference TWAP
uses exact `payload.full_accuracy_value` and `btc:live:chainlink_twap_60s`; unlike
optional raw capture, its event/session/gap persistence is always durable while
TWAP is enabled. With both raw flags `false`, neither legacy raw-capture path creates a
raw queue, raw writer/maintenance task, raw feed-session record, or dedicated
raw database connection. The futures reader still records its connection and
pre-parse receive stamp because those are now part of the public last-price
path. A phase's deployed code is only its code checkpoint. The historical
accelerated three-hour canaries provide limited evidence about slow leaks,
reconnects, daily traffic variation, and sustained storage growth. Current
deployment and storage checks are in [production operations](#production-operations); they do
not reconstruct those older canary procedures or establish their completion.

Phase 4's deliberate partition-boundary and retention validation has been
explicitly deferred while work proceeds to Phase 5. It is not proven by either
three-hour canary: a short window may not cross a six-hour partition boundary.
Automatic future-partition creation, expired-partition removal, the configured
72-hour retention behavior, and sustained relation-budget enforcement therefore
remain known production risks and must not be described as validated.

Redis is not a historical store. The four authoritative source-price keys are:

- `btc:live:binance_spot`
- `btc:live:chainlink`
- `btc:live:chainlink_twap_60s`
- `btc:live:futures`

Each value has this shape:

```json
{"value":"62067.89","source_timestamp_ms":123,"received_ms":456}
```

The optional microstructure collector also writes
`btc:live:microstructure`. That key contains the latest finalized flat
one-second microstructure row as compact JSON. Decimal values are JSON strings;
integers and booleans retain their JSON types, and unknown values are `null`.
It never contains the current or a completed five-minute history.

## API

The FastAPI application is read-only and is started by systemd with:

```text
uvicorn price_collector.api:app --host 127.0.0.1 --port 9000 --workers 1
```

Current routes:

- `GET /healthz`
- `GET /prices/latest?provider=...&symbol=...`
- `GET /markets?limit=3&include_current=false&before_market_id=...`
- `GET /markets/latest?provider=...&symbol=...`
- `GET /markets/{market_id}?provider=...&symbol=...`
- `GET /markets/current/sources`
- `GET /markets/{market_id}/sources`
- `GET /markets/current/data`
- `GET /markets/{market_id}/data`
- `GET /markets/current/download`
- `GET /markets/{market_id}/download`
- `GET /markets/current/live`
- `GET /markets/current/microstructure/live`
- `GET /markets/current/dashboard`
- `GET /forecasts/chainlink-twap/live`
- `GET /forecasts/chainlink-twap/stream`
- `GET /forecasts/chainlink-twap/accuracy`
- `GET /forecasts/chainlink-twap/comparison`

The data and download responses use schema version `4` and always include
`market.settlement` and `market.resolution`, independently of optional series
flags. `settlement` names the market-specific TWAP reference and window, rule
version, source URL, exact Price to Beat, official final price, and official-price
status/source. `series[].prices.chainlink` remains standard Chainlink spot
context; `series[].prices.twap` is the settlement-reference feed. Ended markets
can remain `pending` while official Gamma/CLOB data is incomplete, and the last
Up/Down probability is never treated as the winner.

The two data routes accept `include_microstructure=true`. That opt-in reads at
most 300 indexed PostgreSQL rows, adds `series[].microstructure` and
microstructure availability counts without changing schema version `4`.
`microstructure_groups` can select any comma-separated subset of
`books,flow,cross_market,liquidations,quality`; all five are returned by default.
Missing seconds remain `null`, and older markets without microstructure still
return normally. These larger JSON responses support gzip compression.
The ordinary current/by-ID downloads remain schema version `4` and do not
include microstructure. They omit the market start/end millisecond fields and
per-row `timestamp_ms`, retain the equivalent UTC `*_at` strings, and format
official benchmark/final values to two decimal places. The data routes
retain their full timing and precision fields.

`GET /markets/current/microstructure/live` reads the four source-price keys and
the latest finalized microstructure key with one Redis `MGET`. It returns simple
string-or-`null` prices and the nested microstructure groups without querying
PostgreSQL. `GET /markets/current/live` reads the same four prices with one
Redis `MGET` and returns standard Chainlink context separately from the
authoritative `twap`.

`GET /markets` is the frontend discovery route. It returns the newest three
completed markets by default, newest first, with market timestamps and
per-source availability counts. Use `include_current=true` to include an
observed active market, and pass the returned `next_before_market_id` as the
exclusive `before_market_id` cursor for older pages. Future and observation-empty
windows are not returned. The frontend should select a returned `market_id` and
then request `/markets/{market_id}/data` with the desired optional datasets.


## Microstructure API

These routes expose the optional Binance receipt-time microstructure summaries.
The implementation is [microstructure_api.py](price_collector/microstructure_api.py);
the route wiring is in [api.py](price_collector/api.py).

| Purpose | Request | Storage |
| --- | --- | --- |
| Latest finalized second | `GET /markets/current/microstructure/live` | Redis |
| Current market's grid | `GET /markets/current/data?include_microstructure=true` | PostgreSQL |
| Selected market's grid | `GET /markets/{market_id}/data?include_microstructure=true` | PostgreSQL |

Use the [private SSH connection](#connect-through-an-ssh-tunnel). This example runs on
the droplet itself; remote clients use their local SSH-tunnel port:

```bash
curl --compressed 'http://127.0.0.1:9000/markets/current/data?include_microstructure=true&microstructure_groups=books,quality'
```

### Live response and interval identity

The live route performs one `MGET` for the four source-price keys and
`btc:live:microstructure`. It has no PostgreSQL read and returns only the newest
finalized microstructure second. There are no supported query parameters or
group filters on this route.

| Live field | Meaning |
| --- | --- |
| `schema_version` | Live response contract, currently `2` |
| `server_time_ms` | UTC epoch milliseconds sampled by the API |
| `market_id` | Market containing the cached snapshot, even when that snapshot is old |
| `sample_second_ms` | Start of the finalized local receipt interval, or `null` |
| `served_from` | `"redis"` |
| `prices` | Independent latest cached prices; each value is a decimal string or `null` |
| `microstructure` | Nested row with `collector_healthy` and all five groups, or `null` |

`prices` contains `binance_spot`, `chainlink`, `twap`, and `futures`.
`chainlink` is standard Chainlink spot context; the current `twap` key is the
independent official **60-second** TWAP. Neither is a fallback for the other.
The four cached prices are not a synchronized snapshot of the microstructure
interval, and this route's simple price fields do not include source ages.
Use `/markets/current/live` when the per-source clocks are needed.

A row covers `[sample_second_ms, sample_second_ms + 1000)` on the collector's
local receipt clock. An event received exactly at the end belongs to the next
row. Publication happens after the interval closes; this is finalized
one-second data, not a subsecond feed. Its five-minute market is derived from
the row's interval start, rather than the API request time.

If no microstructure snapshot exists, the route still returns `200` with
`sample_second_ms: null` and `microstructure: null`; only then does `market_id`
refer to the API's current market. A missing source-price key makes only that
price `null`.

An old Redis row may remain after collection stops. `collector_healthy`
describes its historical interval, not its freshness now. Compute age since
the interval closed and advance that age as the browser waits:

```javascript
const snapshotAgeAtResponseMs = live.sample_second_ms === null
  ? null
  : Math.max(0, live.server_time_ms - (live.sample_second_ms + 1000));
```

This example uses only timestamps as JavaScript numbers. Use snapshot age,
`collector_healthy`, and the quality fields together before displaying a row
as current or using it in calculations.

### Historical grid and group selection

Historical requests add `series[].microstructure` and availability counts to
the existing **schema version `4`** response without removing other fields.
Each request covers one five-minute window: 300 one-second grid positions,
with at most 300 matched stored rows and no microstructure pagination.
The current-market endpoint selects its window using the API server clock;
the by-ID endpoint reads the requested window.

`include_microstructure=true` combines with the other supported data flags,
such as `include_futures=true` and `include_probabilities=true`.
By default all groups are included:
`books,flow,cross_market,liquidations,quality`.
Use `microstructure_groups=books,quality` for a smaller historical payload.

- Names are case-sensitive and comma-separated; surrounding whitespace is
  trimmed, duplicate names are deduplicated, and output uses canonical order.
- Empty entries or unknown names return `422`.
- Supplying `microstructure_groups` without `include_microstructure=true`
  returns `422`.
- `collector_healthy` remains present on every matched row, regardless of
  selection. Group selection does not change availability counts.
- `fill_display` does not fill missing microstructure rows. They remain `null`.

| `availability` field | Meaning |
| --- | --- |
| `microstructure_rows` | Stored rows matched to the selected grid |
| `microstructure_healthy_rows` | Matched rows whose `collector_healthy` is `true` |
| `microstructure_missing_seconds` | Expected grid positions minus matched rows |

Future seconds within the current market count as missing until collected.
Missing historical rows may also reflect a disabled collector, retention,
connection gaps, or dropped persistence work; they are not fabricated as zero.
An existing market without microstructure still returns `200`, with zero
matched rows and `microstructure: null` at every grid position. A `404` means
the requested base market data is absent, not merely its microstructure.

### Field groups

`books` has independent `spot` and `futures` objects with these same fields:

| Fields | Meaning |
| --- | --- |
| `bid`, `ask`, `mid` | Best prices and their midpoint |
| `spread_bps` | Bid/ask spread divided by midpoint, in basis points |
| `imbalance_1`, `imbalance_5`, `imbalance_10` | `(bid quantity - ask quantity) / total quantity` across the stated number of levels |
| `bid_depth_usdt_10`, `ask_depth_usdt_10` | Price-times-quantity totals across ten levels; `null` without enough levels |
| `weighted_mid_offset_bps` | Quantity-weighted best-price midpoint offset from midpoint, in basis points |
| `bbo_ofi_usdt` | Observed best-bid/offer snapshot order-flow imbalance, valued at midpoint |
| `snapshot_count` | Number of book snapshots observed in the interval |

Book fields describe the retained book state; the count and quality clocks
show whether new observations arrived during the interval. Snapshot OFI is
not a complete exchange order-by-order event ledger.

`flow` contains observed aggregate-trade flow in the receipt interval:

| Fields | Meaning |
| --- | --- |
| `spot_buy_usdt`, `spot_sell_usdt` | Spot aggressive buy/sell notionals |
| `futures_buy_usdt`, `futures_sell_usdt` | Futures aggressive buy/sell notionals |
| `futures_rpi_buy_usdt`, `futures_rpi_sell_usdt` | Separately reported futures RPI notionals |
| `spot_trade_id_span`, `futures_trade_id_span` | Observed underlying trade-ID spans |
| `spot_aggtrade_count`, `futures_aggtrade_count` | Aggregate-trade message counts |
| `spot_max_aggtrade_usdt`, `futures_max_aggtrade_usdt` | Largest observed aggregate-trade notional |
| `spot_vwap`, `futures_vwap` | Observed volume-weighted average trade prices |
| `spot_trade_high`, `spot_trade_low`, `spot_last_trade` | Spot interval trade extrema and last price |
| `futures_trade_high`, `futures_trade_low`, `futures_last_trade` | Futures interval trade extrema and last price |

`cross_market` contains book relationships and slower futures state:

| Fields | Meaning |
| --- | --- |
| `perp_spot_basis_bps` | `(futures midpoint / spot midpoint - 1) × 10,000` |
| `spot_futures_book_skew_ms` | Absolute difference between the two books' latest local receipt times |
| `mark_price`, `index_price`, `mark_index_basis_bps` | Futures reference prices and `(mark / index - 1) × 10,000` |
| `funding_rate`, `seconds_to_funding` | Latest funding rate and nonnegative whole seconds until funding |
| `open_interest_btc`, `open_interest_usdt` | Open interest, with USDT value computed using the futures book midpoint |

These values have independent update times. Read the age, lag, and skew fields
before interpreting the displayed relationships as contemporaneous.

`liquidations` exposes `observed_long_usdt`, `observed_short_usdt`, and
`snapshot_count`. These are **censored observed forced orders** from Binance;
they are neither total market liquidations nor predicted liquidation levels.

`quality` preserves diagnostics even when a row is unhealthy:

| Fields | Meaning |
| --- | --- |
| `schema_version` | Stored row schema, separate from the outer API schema |
| `sample_span_ms` | Distance from the preceding sampled second; may be `null` for the first row |
| `sample_jitter_ms` | Finalization delay after the receipt interval's ending boundary |
| `spot_book_age_ms`, `futures_book_age_ms` | Age of the retained book receipt at interval evaluation |
| `spot_book_lag_ms`, `futures_book_lag_ms` | Latest observed book transport lag |
| `spot_trade_age_ms`, `futures_trade_age_ms` | Age of the latest received trade |
| `spot_trade_lag_mean_ms`, `spot_trade_lag_max_ms` | Mean and maximum observed spot trade transport lag |
| `futures_trade_lag_mean_ms`, `futures_trade_lag_max_ms` | Mean and maximum observed futures trade transport lag |
| `mark_age_ms`, `mark_lag_ms` | Mark-context age and lag |
| `open_interest_age_ms`, `open_interest_exchange_age_ms`, `open_interest_http_lag_ms` | Local receipt age, exchange timestamp age, and HTTP lag for open interest |
| `liquidation_lag_mean_ms` | Mean transport lag of observed forced orders |
| `connection_errors` | Recorded connection/error diagnostic count |
| `received_ms` | Local row-finalization time |

### Values, errors, and client usage

Financial quantities are decimal strings, never binary floats. Counts,
timestamps, whole-millisecond ages/lags, and durations are integers; mean lag
fields are decimal strings. Missing values are `null`. Zero is a legitimate
observation, but zero flow in an unhealthy interval does not establish that
market activity was zero. Preserve unhealthy rows and visibly distinguish them.
Use Decimal or scaled-integer arithmetic for prices and financial calculations;
round only for display, with any floating chart coordinates kept separate.

| Status | Meaning |
| --- | --- |
| `200` | Success, including an absent microstructure snapshot or missing historical rows |
| `404` | Requested base market data is absent |
| `422` | Invalid typed query value or historical group selection |
| `503` | Live Redis read failed or its payload was invalid |

The live `503` body's `detail` is `"live cache unavailable"` for a read failure
or `"live cache payload invalid"` for invalid cached data.

For a chart, load the market's historical grid, index by `timestamp_ms`, then
poll the live route about once per second. Ignore null snapshot timestamps;
append or replace by `sample_second_ms`, respecting the returned `market_id`.
On rollover, backfill the completed market and load the new market. Restore
history from PostgreSQL after a browser interruption; Redis cannot backfill it.

Microstructure history follows the configured retention up to the current
**ten-day maximum**, not the superseded thirty-day canary policy. Ordinary
`/markets/current/download` and `/markets/{market_id}/download` exports do not
include microstructure; its query flags apply only to the two data routes.
The live route always returns all groups. HTTP compression reduces transfer
size, but does not change the one-second sampling or make these data subsecond.

## Repository Layout

```text
price_collector/       Source collectors, shared storage helpers, and API
deployment/            systemd units and environment-file examples
tests/                 Unit and deployment-safety tests
schema.sql             PostgreSQL tables, indexes, constraints, and seed rows
README.md              Setup, API usage, production operations and maintenance
GHOST_TWAP_REFERENCE.md Research findings and the ghost forecast/accuracy contract
FRONTEND_API.md        Frontend-facing FastAPI endpoint and response reference
requirements.txt       Python runtime and test dependencies
```

The [production operations](#production-operations) chapter covers updates,
evidence verification, storage measurement and disabling optional capture.
The [microstructure API](#microstructure-api) section covers its live/history views.

## Local Development

Python 3.12 is the deployment baseline because it is the default Python version
on Ubuntu 24.04.

```bash
python -m venv .venv
```

Activate the environment on Linux or macOS:

```bash
source .venv/bin/activate
```

Or in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies and run the test suite:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest
```

The test suite uses mocks and fakes; it does not require live exchange feeds or
a live PostgreSQL instance.

## Configuration

Pydantic settings read case-sensitive environment variables without a prefix.
Collectors and the API intentionally use separate environment files:

- `/etc/price-collector/collector.env`, created from
  `deployment/collector.env.example`, contains the writer database URL and
  collector settings.
- `/etc/price-collector/api.env`, created from
  `deployment/api.env.example`, contains only the reader database URL and Redis
  settings.

The Chainlink collector's accepted-event watchdog is configured in
`collector.env`:

```text
POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000
```

The value must be between 5,000 and 60,000 ms and is independent of
`STALE_PRICE_MS`.
Only a successfully parsed and accepted `crypto_prices_chainlink` `btc/usd`
event resets the monotonic timer. When it expires, the collector classifies the
connection close as `proactive_reconnect`, applies the existing jittered
backoff, and resubscribes without restarting the process. The last Redis value
is left in place and continues aging until a fresh event arrives.

The same service owns an independent durable TWAP runtime. Its production
defaults are:

```text
POLYMARKET_TWAP_ENABLED=true
POLYMARKET_TWAP_PROVIDER_CODE=polymarket_chainlink_twap_rtds
POLYMARKET_TWAP_SYMBOL=BTCUSD_TWAP_60S
POLYMARKET_TWAP_RTD_SYMBOL=btc/usd
POLYMARKET_TWAP_TOPIC=crypto_prices_twap_sixty
POLYMARKET_TWAP_WINDOW_SECONDS=60
POLYMARKET_TWAP_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000
POLYMARKET_TWAP_PERSIST_QUEUE_MAX_EVENTS=10000
POLYMARKET_TWAP_PERSIST_SHUTDOWN_TIMEOUT_SECONDS=5
```

At minimum, replace the database passwords in:

```text
DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector
READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector
```

The probability collector's resolution reconciler uses these settings from
`collector.env`:

- `POLYMARKET_MARKET_BACKFILL_START_MS=1786665600000` is the inclusive lower
  bound for metadata-only completed-market backfill. It must be a UTC
  five-minute boundary (`value % 300000 == 0`) at or after the
  `2026-08-14T00:00:00Z` cutover. Moving this operational floor forward after
  an intentional clean reset does not move or redefine that immutable rule
  cutover.
- `POLYMARKET_RESOLUTION_POLL_SECONDS=5` sets the scan interval and initial
  retry delay.
- `POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300` caps exponential retry
  backoff while official data is pending or temporarily unavailable.
- `POLYMARKET_RESOLUTION_BATCH_SIZE=20` limits the number of due markets checked
  in one scan.
- `POLYMARKET_RESOLUTION_WS_GRACE_SECONDS=30` keeps the ending market's CLOB
  subscription open briefly for an official winner event without delaying
  collection of the preloaded next market.

High-resolution evidence capture uses these settings. Repository and production
example defaults remain disabled:

```text
RAW_FUTURES_TRACE_ENABLED=false
RAW_CHAINLINK_EVENTS_ENABLED=false
RAW_FUTURES_BUCKET_MS=100
RAW_CAPTURE_QUEUE_MAX_EVENTS=5000
RAW_CAPTURE_BATCH_MAX_ROWS=500
RAW_CAPTURE_FLUSH_MS=1000
RAW_CAPTURE_RETENTION_HOURS=72
RAW_CAPTURE_MAX_RELATION_MB=2048
RAW_CAPTURE_RETENTION_CHECK_SECONDS=60
```

The Phase 1 schema accepts only a 100 ms futures bucket. The relation budget
applies only to raw-capture PostgreSQL relations; it is not a filesystem quota
and does not include WAL, temporary files, or the rest of the database.

The optional microstructure summary has a separate bounded policy:

```text
BINANCE_MICROSTRUCTURE_ENABLED=false
BINANCE_MICROSTRUCTURE_SPOT_WS_URL=wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth10
BINANCE_MICROSTRUCTURE_FUTURES_DEPTH_WS_URL=wss://fstream.binance.com/public/ws/btcusdt@depth10@500ms
BINANCE_MICROSTRUCTURE_FUTURES_LIQUIDATION_WS_URL=wss://fstream.binance.com/market/ws/btcusdt@forceOrder
BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS=100000
BINANCE_MICROSTRUCTURE_PERSIST_QUEUE_MAX_ROWS=600
BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS=250
BINANCE_MICROSTRUCTURE_RETENTION_DAYS=10
BINANCE_MICROSTRUCTURE_WARN_RELATION_MB=4096
BINANCE_MICROSTRUCTURE_MAX_RELATION_MB=6144
```

It defaults off so applying a schema/code update does not silently begin a new
high-rate dataset. Enable it only after applying `schema.sql` and adding the
production overrides manually. The collector performs its own daily cleanup
only for a configured retention shorter than ten days. At ten days, or with an
older longer setting such as thirty days, the independent bounded ten-day timer
owns cleanup; it also runs when optional capture is off. The current deployed
microstructure warning/cap overrides are 3072/4096 MiB, as recorded in the
[September 16 shared-capacity review and activation](GHOST_TWAP_REFERENCE.md#storage-safeguards-and-maintenance).
The collector checks the
table plus indexes once per minute, warns at the lower relation threshold, and
pauses only new
microstructure writes at the upper threshold; the critical futures live,
snapshot, flow, and book paths continue. The size gate is hysteretic: after it
pauses, writes resume only when a later `pg_total_relation_size` measurement is
strictly below the warning threshold. Retention `DELETE` removes logical rows
but normally does not shrink the physical PostgreSQL relation, so it must not
be expected to resume a size-paused writer by itself. Resumption requires an
operator-controlled compaction/rebuild or another real physical shrink, plus a
confirmed measurement below the warning threshold. The ten-day maximum
supersedes the initial PostgreSQL canary's thirty-day policy and the DuckDB
starter's 400-day estimate. Measure real PostgreSQL growth before changing
capacity settings.

For the Phase 2 accelerated three-hour production canary, manually set only
`RAW_FUTURES_TRACE_ENABLED=true`, keep
`RAW_CHAINLINK_EVENTS_ENABLED=false`, and keep
`BINANCE_FUTURES_STREAMS_ENABLED=true`. Passing the three-hour gate
permits Phase 3, but leave futures capture running and continue observing the
same health indicators until it reaches at least 24 uninterrupted hours.

Prepare and deploy the Phase 3 code with Chainlink capture still `false`. Do not
enable it until the futures-only Phase 2 canary has completed its uninterrupted
three-hour accelerated acceptance window. The Phase 3 accelerated three-hour
production canary keeps
`BINANCE_FUTURES_STREAMS_ENABLED=true` and
`RAW_FUTURES_TRACE_ENABLED=true`, manually sets
`RAW_CHAINLINK_EVENTS_ENABLED=true`, and restarts only
`price-collector-polymarket-chainlink`. The two enabled collectors can then use
at most two dedicated raw-capture database connections in total. The repository
example remains disabled. Keep both captures enabled and continue background
observation of each one toward 24 uninterrupted hours from its activation time.
Phase 4 retention validation was explicitly deferred so Phase 5 source-cutover
work could proceed; Phase 5 does not validate raw partition rollover, expiry,
or storage-budget enforcement, and that residual risk remains open.

Do not commit `collector.env`, `api.env`, `droplet.env`, `.env`, or real
credentials. These files are ignored by Git; only their examples belong in the
repository.

## Initial Droplet Deployment

### Assumptions

- Ubuntu 24.04 LTS
- DigitalOcean Singapore region
- 1 vCPU and 2 GB RAM for the initial single-user deployment
- SSH key access
- The GitHub repository is cloned into `/opt/price-collector`
- Only SSH is exposed publicly

### Install Base Packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip postgresql postgresql-contrib redis-server git openssh-client ufw
```

Allow SSH before enabling the firewall:

```bash
sudo ufw allow OpenSSH
sudo ufw enable
sudo ufw status
```

Do not open application or datastore ports:

```text
Do not run: sudo ufw allow 9000
Do not run: sudo ufw allow 5432
Do not run: sudo ufw allow 6379
```

### Create the Service User

```bash
sudo useradd --system --user-group --home /var/lib/price-collector --shell /usr/sbin/nologin pricecollector
sudo install -d -o pricecollector -g pricecollector -m 750 /var/lib/price-collector
```

If the user already exists, only ensure the state directory has the correct
ownership:

```bash
sudo chown -R pricecollector:pricecollector /var/lib/price-collector
```

### Clone From GitHub

The droplet installs and updates the application from GitHub. For this
repository:

```bash
export PRICE_COLLECTOR_REPO="https://github.com/9r89uf8/pythonbtccollector.git"
export PRICE_COLLECTOR_BRANCH="main"

sudo install -d -o pricecollector -g pricecollector -m 750 /opt/price-collector
sudo -u pricecollector git clone --branch "$PRICE_COLLECTOR_BRANCH" "$PRICE_COLLECTOR_REPO" /opt/price-collector
```

For a private repository, configure a read-only GitHub deploy key for the
`pricecollector` user and use the repository's SSH URL instead.

Create the virtual environment and install dependencies:

```bash
cd /opt/price-collector
sudo -u pricecollector python3 -m venv .venv
sudo -u pricecollector .venv/bin/pip install --upgrade pip
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest
```

### Configure PostgreSQL

Keep PostgreSQL on loopback and restart it after changing the setting:

```bash
sudo -u postgres psql -c "ALTER SYSTEM SET listen_addresses = '127.0.0.1';"
sudo systemctl restart postgresql
```

Open PostgreSQL:

```bash
sudo -u postgres psql
```

Create the database and separate writer/reader roles, replacing both passwords:

```sql
CREATE DATABASE price_collector;

CREATE USER price_writer WITH PASSWORD 'REPLACE_WITH_STRONG_WRITER_PASSWORD';
CREATE USER price_reader WITH PASSWORD 'REPLACE_WITH_STRONG_READER_PASSWORD';

REVOKE ALL ON DATABASE price_collector FROM PUBLIC;
GRANT CONNECT ON DATABASE price_collector TO price_writer;
GRANT CONNECT ON DATABASE price_collector TO price_reader;
\q
```

Load the schema as PostgreSQL's owner role:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudo -u postgres psql -d price_collector
```

Grant write access to collectors and read-only access to the API:

```sql
GRANT USAGE ON SCHEMA public TO price_writer;
GRANT USAGE ON SCHEMA public TO price_reader;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO price_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO price_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO price_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE ON TABLES TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO price_reader;

\q
```

### Configure Redis

Redis is a loopback-only live cache:

```bash
sudo sed -i 's/^bind .*/bind 127.0.0.1/' /etc/redis/redis.conf
sudo sed -i 's/^protected-mode .*/protected-mode yes/' /etc/redis/redis.conf
sudo systemctl enable --now redis-server
sudo systemctl restart redis-server
redis-cli -h 127.0.0.1 PING
```

The expected reply is `PONG`.

### Install Environment Files

```bash
sudo install -d -o root -g pricecollector -m 750 /etc/price-collector
sudo test -e /etc/price-collector/collector.env || sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/collector.env.example /etc/price-collector/collector.env
sudo test -e /etc/price-collector/api.env || sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/api.env.example /etc/price-collector/api.env

sudo nano /etc/price-collector/collector.env
sudo nano /etc/price-collector/api.env
```

The guarded install commands create each file only when it does not already
exist, so rerunning them cannot replace production secrets with example values.
Replace `REPLACE_ME` on the first deployment and keep writer credentials out of
`api.env`.

### Install systemd Units

```bash
sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo cp /opt/price-collector/deployment/price-collector-binance-futures.service /etc/systemd/system/price-collector-binance-futures.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-probabilities.service /etc/systemd/system/price-collector-polymarket-probabilities.service
sudo cp /opt/price-collector/deployment/price-api.service /etc/systemd/system/price-api.service
sudo cp /opt/price-collector/deployment/price-collector-retention.service /etc/systemd/system/price-collector-retention.service
sudo cp /opt/price-collector/deployment/price-collector-retention.timer /etc/systemd/system/price-collector-retention.timer

sudo systemctl daemon-reload
sudo systemctl enable --now price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
sudo systemctl enable --now price-collector-retention.timer
```

Apply the schema before enabling retention. Existing installations should use
the [bounded cleanup rollout](#ten-day-collector-history-retention),
including its backlog and datastore checks.

### Verify the Deployment

```bash
systemctl status redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api price-collector-retention.timer --no-pager
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_twap_rtds&symbol=BTCUSD_TWAP_60S"
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```

Confirm every network service is private:

```bash
ss -ltnp | grep ':9000'
ss -ltnp | grep ':5432'
ss -ltnp | grep ':6379'
```

Acceptable listeners include:

```text
127.0.0.1:9000
127.0.0.1:5432
127.0.0.1:6379
```

These public listeners are not acceptable:

```text
0.0.0.0:9000
0.0.0.0:5432
0.0.0.0:6379
```

## Connect Through an SSH Tunnel

On your local machine, copy `droplet.env.example` to the ignored
`droplet.env`, replace its values, and load it:

```bash
cp droplet.env.example droplet.env
set -a
. ./droplet.env
set +a
```

Open the tunnel and keep the terminal running:

```bash
ssh -N -L "${LOCAL_API_PORT}:127.0.0.1:9000" "${DROPLET_USER}@${DROPLET_IP}"
```

Then query the API locally:

```bash
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/current/live"
```

## Compact Polymarket evidence for H3

The existing `price-collector-polymarket-probabilities` service can collect
prospective decision-time evidence with `POLYMARKET_EVIDENCE_ENABLED=true`.
It defaults to `false`; review the settings in
`deployment/collector.env.example` and apply the schema before enabling it as
described in [production operations](#production-operations). `EvidenceSettings` belongs to this
optional path rather than the shared collector configuration. It adds no
service, research model, order submission, authenticated trading connection,
or public API endpoint.

A separate HTTP worker observes four kinds of metadata: the official website's
`/api/crypto/crypto-price` opening reference, Gamma market/rule metadata,
CLOB `/clob-markets/{condition_id}` fee curves and taker-delay flags, and
CLOB `/markets/{condition_id}` order rules including the separate
`seconds_delay` setting. These are recorded as `price_to_beat`, `gamma_market`,
`clob_market` and `clob_order_rules` observations respectively.
Requests use the discovered market identity; BTC five-minute TWAP requests
explicitly select `twapEnabled=true` and the applicable TWAP window. Live Gamma
metadata can omit Price to Beat, so a later reconciled strike is not treated as
something known before an earlier decision. HTTP request and response clocks,
status, provenance and missing values are preserved. Response SHA-256, HTTP
Date and Age are observation columns rather than duplicated payload fields.
API/cache timestamps do
not establish the Chainlink observation time or order-to-fill latency.

The existing CLOB state also produces one paired Up/Down quote observation
every 100 ms during the final 120 seconds. Each row records its actual wall and
monotonic observation clocks and separate provider/receipt clocks for all four
bid/ask components. These are sampled observations, not reconstructed ticks:
intermediate price changes and quote lifetimes cannot be recovered. Neither
Polymarket depth nor quantities are stored. Consequently, these observations
cannot establish fills for a chosen order size or complete H3's execution test.

`polymarket_market_observations` appends metadata observations and
`session_start`, `session_end` and `gap` records. It links normalized payloads
stored once by hash in `polymarket_evidence_payloads`.
`polymarket_quote_observations` holds the compact typed quote rows. Quote prices
remain exact `Decimal`/PostgreSQL `NUMERIC(38,18)`; saved JSON prices are
decimal strings. Bounded queues and batched database writes keep this path
independent of the core probability stream. Pending queues live in memory;
an interrupted session exposes possible data loss and cannot replay unwritten
observations.

The size guard measures these relations including indexes and TOAST. Its
default warning/cap settings are 4096/6144 MiB; the current deployed evidence
overrides are 3072/4096 MiB following the
[September 16 shared-capacity review](GHOST_TWAP_REFERENCE.md#storage-safeguards-and-maintenance).
Reaching the cap pauses new
quote capture, records gaps, and leaves metadata and core collection running;
already accepted writes can still drain. It is not a hard disk limit and does
not itself delete evidence. The separate ten-day history timer removes expired
evidence and unreferenced shared payloads. Measure actual growth with the
operations queries; age retention does not replace the relation-size guard.
Do not enable unrelated
`RAW_FUTURES_TRACE_ENABLED` or `RAW_CHAINLINK_EVENTS_ENABLED` flags for this path.

Store CLOB `fd` fee-curve parameters and `itode` independently of legacy base
fees and `seconds_delay`. The [CLOB market-info documentation](https://docs.polymarket.com/api-reference/markets/get-clob-market-info)
describes a 250 ms taker delay when `itode=true`; an observed
`seconds_delay=0` does not negate that flag. The [market-details documentation](https://docs.polymarket.com/market-data/market-details)
defines current fee schedules and minimum size/tick constraints. Their
timestamped observations describe the available configuration, not realized
fills or a frozen fee schedule for future markets.

## Ghost TWAP — optional worker

The optional historical settlement estimator replaces the retired two-day
candidate study. In the final 60 seconds it shows how often the leading side
won in past markets with a similar projected margin and remaining time, with
counts, unknowns and the history dates. These are descriptive frequencies,
not a guarantee that the live winner is locked. The rolling 1-, 2-, 3-, 5-,
10- and 30-second ghost prices and their accuracy monitor are unchanged.
See [historical settlement win rates](GHOST_TWAP_REFERENCE.md#historical-settlement-win-rates)
for selection rules, API routes, retention and the schema-first update.
`SETTLEMENT_ENABLED` controls the bounded context handoff and producer inside
the existing probabilities/Chainlink services; `SETTLEMENT_API_ENABLED` controls
Redis-only delivery. Both remain off by default. No new service or public
listener is introduced. Remove the obsolete `SETTLEMENT_EVALUATION_START_MS`
setting when updating; saved study results remain preserved. History records
with an incomplete freeze remain visibly partial; the panel reports these
days rather than presenting them as complete evidence.

The exact-close estimator admits at most one decision per fixed two-second UTC
interval and uses twelve five-second time buckets. Its schema-3 sixty-second observation window
has a new history cohort; retained thirty-second observations keep their
original identity and are not relabelled or pooled into it. The existing row
and storage caps remain fixed. Expired projections wait for a fresh update;
their TTL is not extended to bridge the interval. Apply the schedule-check
migration in `schema.sql` before restarting the Chainlink writer; the referenced update sequence does this.

The [Ghost TWAP reference](GHOST_TWAP_REFERENCE.md) is the single guide to the
verified research, measured 3.1-second inclusion delay, live-canary findings,
forecast formula, API behavior, chart metrics, retention and limitations. Dated
plans and generated reports have been consolidated there; historical artifacts
remain available at the Git revision recorded in that guide.

The optional worker inside `price-collector-polymarket-chainlink` forecasts
1-, 2-, 3-, 5-, 10- and 30-second future TWAP source stamps using causal Chainlink
spot history and flat continuation of unseen inputs. It keeps official TWAP
and source Redis values untouched. Prices use Decimal and full frozen evidence
is durable before publication. Source age is bounded at 5,000 ms and receipt
age at 3,000 ms; unavailable/stale forecasts are never relabelled fresh.

Continuous forecasting was activated September 16, 2026 at 15:59:27 UTC.
`GHOST_TWAP_ENABLED=true` and `GHOST_TWAP_CONTINUOUS=true` select this deployed
mode; both remain off by default in code. State persists in
`/var/lib/price-collector/ghost-continuous` and must be reused across restarts.
Individual continuous forecasts/results expire after seven days; hourly
accuracy/feed-health summaries expire after 90 days. These are retention rules,
not a guarantee that a shared-disk guard can never pause new forecasts.

Four read-only routes under `/forecasts/chainlink-twap` use Redis only:

- `/live`: latest serialized prediction snapshot, with expiry validation.
- `/stream`: immediate updates over SSE, with resync and client expiry.
- `/accuracy`: minute-refreshed 1-hour, 24-hour and 7-day accuracy panels.
- `/comparison`: finalized 15-minute frozen prediction/actual pairs for the
  3/5/10/30-second charts, refreshed every minute and at least 125 seconds behind
  live time. It freezes the first eligible acknowledged publication per target
  before outcome checks, and compares ghost error with actual TWAP movement on
  identical pairs.

The local dashboard is a separate project in `dist/ghost-frontend`, outside this
backend checkout. It reaches the loopback API only through the SSH tunnel; no
frontend assets or dashboard service are installed on the droplet. Browser or
laptop availability does not determine whether the producer runs. Ghost prices
are forecasts, not official settlement values or validated trading signals.
See [continuous operations](#continuous-ghost-retention-and-accuracy)
for settings, guards, installation and verification commands.

`GET /markets/current/dashboard` is a separate bounded PostgreSQL-backed view
for restoring the local Recent movement chart and current five-minute market
context after opening or resuming the page. It returns saved spot/TWAP history,
server time, boundaries and an observed official Price to Beat when available.
This does not add database queries to the live Redis/SSE request path.

## Production operations

Use this chapter for an existing installation; use
[Initial Droplet Deployment](#initial-droplet-deployment) for first setup.
Commands run on the Ubuntu droplet unless labelled as local. Keep the private
bindings and reader/writer credential separation described above. Production
services must not execute research code or install research dependencies.

### Routine checks

```bash
sudo systemctl status price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api redis-server --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli -h 127.0.0.1 -p 6379 PING
df -h / /var/lib/postgresql
sudo ss -ltnp
sudo journalctl -u price-collector-polymarket-probabilities -n 100 --no-pager
```

Review receive ages as well as service status. An active socket or process
does not establish fresh accepted events. Ports 9000, 5432 and 6379 must not
listen on public interfaces. Current/live API checks must remain read-only.

### Deploy Updates From GitHub

After changes have been pushed to GitHub, pull fast-forward only and install
dependencies. The sequence below restarts all collectors and the API, appropriate
for shared runtime changes; use only actually affected units for a narrower change.
The feature-specific procedures below give the exact narrower service lists:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
```

If `schema.sql` changed, apply it **before** restarting affected services:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
```

If a systemd unit changed, copy that exact unit using its matching command in
[Install systemd Units](#install-systemd-units), then run
`sudo systemctl daemon-reload` before restarting it. Never overwrite an existing
production environment file with an example. Finish each update with the relevant
[routine checks](#routine-checks) and bounded service logs.

### Maintenance

Check database allocation as well as filesystem free space; raw/evidence caps
exclude WAL, unrelated tables, logs and some other disk use:

```bash
sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d price_collector -c "SELECT pg_size_pretty(pg_database_size('price_collector'));"
```

The [ten-day cleanup procedure](#ten-day-collector-history-retention) and
[ghost retention procedure](#continuous-ghost-retention-and-accuracy) below have
separate scopes. Review their journals/backlog; a successful pass is not proof
that all old rows expired. Ordinary vacuum permits reuse and does not promise
smaller filesystem allocation. Follow a relevant service's bounded logs from
[Routine checks](#routine-checks); do not expand a cap to conceal stalled cleanup.

### Ten-day collector history retention

`price-collector-retention.timer` schedules the independent
`price-collector-retention.service` oneshot one minute after boot and one minute
after its previous run finishes. Each run has a 45-second work budget and a
55-second systemd deadline, short database transactions and bounded deletion
batches. The timer does not overlap itself. It runs as `postgres:postgres`,
connects to database `price_collector` through `/var/run/postgresql` using local
peer authentication, and receives no environment file or password.

The policy covers non-ghost collector history: prices, probability samples,
futures/flow/book/open-interest history, microstructure, durable TWAP evidence,
and compact Polymarket observations/payloads. Cleanup uses a fixed allowlist;
supported retired database tables are handled only if present. It does not
restore any retired pipeline. Whole market groups expire at the ten-day cutoff
rounded **up** to a five-minute start boundary, so some rows may expire up to
five minutes early. Children are removed before parents; metadata, sessions and
shared payloads remain while retained/current rows need them. Backfill and due
reconciliation use the same floor to avoid recreating expired markets.
Expired historical 30-second TWAP events may be deleted under this policy.
Every surviving row keeps its original 30- or 60-second topic, instrument,
window and settlement-rule identity; age cleanup never relabels old evidence.

All ghost tables are excluded. Seven-day continuous ghost records, ninety-day
accuracy summaries and legacy ghost export safeguards are unchanged. Existing
raw capture retains its stricter 72-hour policy while enabled. The timer also
enforces the ten-day ceiling on old raw history when capture is disabled, without
creating partitions or enabling a feed. Research/results files outside the
collector database are untouched. The API remains read-only and live Redis
values are not deleted or rewritten.

Run the following **after the change is pushed to GitHub**. Apply the schema and
retention indexes before enabling the timer. Shared `config.py`/`db.py` changes
require reloading all four collectors and the API; the probability backfill and
microstructure retention behavior change in this checkpoint:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -X -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/deployment/collector-retention-indexes.sql
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo cp /opt/price-collector/deployment/price-collector-retention.service /etc/systemd/system/price-collector-retention.service
sudo cp /opt/price-collector/deployment/price-collector-retention.timer /etc/systemd/system/price-collector-retention.timer
sudo systemctl daemon-reload
sudo systemctl restart price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
sudo systemctl enable --now price-collector-retention.timer
sudo systemctl start price-collector-retention.service
sudo systemctl status price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api price-collector-retention.timer --no-pager
sudo systemctl list-timers price-collector-retention.timer --all --no-pager
sudo journalctl -u price-collector-retention.service -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
```

The index prebuild is for an existing installation and runs outside a transaction;
it uses concurrent builds for the populated evidence tables. Do not add
`--single-transaction` to that command. If a build fails, stop the rollout and
inspect/repair its index validity before retrying or enabling the timer;
`IF NOT EXISTS` alone does not repair an invalid concurrent-build artifact.

In `collector.env`, manually review `BINANCE_MICROSTRUCTURE_RETENTION_DAYS=10`;
the runtime also caps older values such as 30 at ten days. Collector-local daily
cleanup runs only for a setting shorter than ten days; ten days and older longer
settings delegate to the independent bounded timer. Preserve shorter
settings, existing raw-retention settings and every credential. Never replace
the environment with its example, enable optional captures, or change ghost
flags as part of this installation. No new environment keys are required by
the maintenance service, and Redis does not need a restart.

Initial catch-up can take several passes because each pass is bounded. Read the
retention journal for deleted counts, remaining eligible work and timeouts rather
than treating timer activation as proof that all old data has gone. Diagnose
repeated errors before changing batch limits. Check filesystem and relation
sizes separately: ordinary autovacuum/VACUUM makes deleted space reusable, but
does not promise immediate filesystem shrinkage. Routine cleanup does not run
`VACUUM FULL`, rebuild tables or increase any storage budget.

To inspect eligibility without deleting rows:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.retention
```

To stop cleanup temporarily without stopping collection:

```bash
sudo systemctl stop price-collector-retention.timer
sudo systemctl stop price-collector-retention.service
```

Restarting/enabling the timer resumes the same policy; it does not restore
deleted history. Ongoing cleanup requires the timer to remain enabled.

### Continuous ghost retention and accuracy

This opt-in mode is implemented in the existing Chainlink service. It is disabled
by default and does not change or reset the historical one-hour canaries below.
Both `GHOST_TWAP_ENABLED=true` and `GHOST_TWAP_CONTINUOUS=true` are needed for
continuous production. `GHOST_TWAP_CANARY_START_MS=0` remains valid and is ignored
in this mode. Use `/var/lib/price-collector/ghost-continuous`, separate from every
old canary directory. Its first-start clock is recorded automatically; subsequent
restarts reuse that state and reconcile its outbox before new admission.

**Current deployment:** continuous forecasting started on September 16, 2026 at
15:59:27 UTC on commit `6c6f5bf`, run
`4f55736e031f4184be501c538f7a61d2`. Initial checks found `capacity_ok=true`, no
stop or suspensions, successful compaction and a working accuracy cache. The
[canonical reference](GHOST_TWAP_REFERENCE.md#storage-safeguards-and-maintenance) includes the
shared-capacity calculation and its limits. Seven-day operation and baseline
formation are still ongoing.

The current collector environment overrides are:

| Key | Deployed value |
| --- | --- |
| `GHOST_TWAP_ENABLED` | `true` |
| `GHOST_TWAP_CONTINUOUS` | `true` |
| `GHOST_TWAP_CANARY_START_MS` | `0` |
| `GHOST_TWAP_STATE_DIRECTORY` | `/var/lib/price-collector/ghost-continuous` |
| `GHOST_TWAP_DATABASE_FILESYSTEM_PATH` | `/var/lib/postgresql` |
| `GHOST_TWAP_SOURCE_MAX_AGE_MS` | `5000` |
| `GHOST_TWAP_RECEIPT_MAX_AGE_MS` | `3000` |
| `POLYMARKET_EVIDENCE_WARN_RELATION_MB` | `3072` |
| `POLYMARKET_EVIDENCE_MAX_RELATION_MB` | `4096` |
| `BINANCE_MICROSTRUCTURE_WARN_RELATION_MB` | `3072` |
| `BINANCE_MICROSTRUCTURE_MAX_RELATION_MB` | `4096` |

Preserve these production overrides during upgrades; the environment examples
are defaults, not the deployed configuration. The historical activation record
is in the [ghost reference](GHOST_TWAP_REFERENCE.md).

For an initial continuous-mode code/schema installation, after the change is
pushed to GitHub, install it with the producer disabled. Later code-only chart
updates preserve the running mode and existing state; use the tailored
[code-only update commands](#ghost-api-and-code-only-updates).
Apply the schema transaction before restarting either affected service:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-chainlink price-api
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -u price-api -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/accuracy
```

During installation keep `GHOST_TWAP_ENABLED=false` and
`GHOST_TWAP_CONTINUOUS=false`. Add only the missing key to the existing collector
environment; never replace the environment file with the example. Keep writer
credentials out of `api.env`. The existing `GHOST_TWAP_API_ENABLED` flag governs
the read-only accuracy route, and does not enable production. An absent or stale
accuracy cache while continuous monitoring is off is an unavailable response,
not a requirement to start forecasts.

Before explicitly enabling continuous mode, review current database-filesystem
free space and the other collectors' remaining relation budgets. Create the
new state directory once with owner `pricecollector:pricecollector` and mode
0700; refuse an unexpected existing directory instead of overwriting it. Then
manually set both enable flags true and the continuous state path, preserving
the 5,000 ms source-age and 3,000 ms receipt-age bounds. Restart only
`price-collector-polymarket-chainlink` for that collector-only environment change.
Never change paths or remove a stop marker to bypass a capacity or integrity
guard. A restart reuses the original continuous state directory.
The existing activated directory must not be recreated or replaced.

The continuous limits are 6 GiB total allocated ghost relation budget, warning
at 5 GiB, admission pause at 5.5 GiB, 1,500,000 retained decisions, and at least
10 GiB free on the actual PostgreSQL filesystem. Table, index and TOAST allocation
all count. This is shared-disk protection, not a guarantee that seven days always
fit. Normal expiry/vacuum may leave allocated space reusable without shrinking
the relation; do not infer free capacity from deleted row counts alone.
At activation, the two optional evidence/microstructure caps were each reduced
to 4 GiB, with 3 GiB warnings. After their remaining growth allowances, the full
6 GiB total ghost allowance and the 10 GiB filesystem reserve, the recorded
conditional balance was 894,377,984 bytes. The measured-rate seven-day new-compact
projection instead left 2,312,806,400 bytes. Neither balance reserves future core
history, WAL, logs, outbox or maintenance space; use fresh measurements before
changing any allowance.

The independent monitor compacts at most 100 eligible continuous decisions per
maintenance cycle, about every five seconds. Terminal status and the 120-second
matching window must both be complete. An atomic compact/hourly-summary commit
precedes removal of verbose inputs; failed work is retried. Compact individual
results expire after seven days, and hourly accuracy/feed-health summaries after
90 days. These operations do not relax the legacy canary's verified-export rule.
Do not expect full slot replay from compact hashes after verbose evidence expires.
The monitor keeps running while new forecasts are paused for capacity. Disabling
the entire worker stops its maintenance too; retained data remains bounded by
admission guards, and maintenance resumes only when that worker is started again.

Accuracy JSON refreshes once per minute with a 180-second Redis TTL. The API
does one cached GET, with no historical query or aggregation. Inspect its
`worker_health`, `runtime_health`, per-group `monitor` states and explicit panel
denominators rather than treating a cached 200 response as fresh feed evidence:

```bash
redis-cli -h 127.0.0.1 -p 6379 TTL btc:live:ghost_chainlink_twap_60s:accuracy
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/accuracy
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
df -h /var/lib/postgresql
```

Completed panels use 1-hour, 24-hour and 7-day windows, with persistence/matching
watermarks. Frozen per-policy baselines use the first available qualifying three
full UTC days in the bounded snapshot, with
at least 3,000 scored pairs and adequate publication/scoring coverage. Current
comparisons require 24 qualifying hours and at least 1,000 pairs after the
baseline. Deterioration requires MAE above 125% of baseline and paired excess
above baseline plus 0.01 bp for three consecutive hourly checks; recovery requires
MAE at most 110% or excess at most baseline plus 0.005 bp for three checks.
Conflicts and inadequate coverage block accuracy classification; missing targets
remain explicit counts rather than fabricated pairs.
These are explicit engineering thresholds. Baselines and warning counters survive
restarts. Receipt-gap durations belong to the hour containing their ending
receipt; incomplete coverage and dropped health hours remain visible. Feed-health
summaries describe accepted observations, not all upstream messages or browser
delivery. No monitor failure may stop the official Chainlink feeds.

### Ghost API and code-only updates

Current behavior, research provenance and completed canary results are in the
[Ghost TWAP reference](GHOST_TWAP_REFERENCE.md). Continuous production and the
read-only API are enabled on the deployed droplet; do not follow superseded
canary installation steps that disable/reset the current run.

For a reviewed code-only change affecting the Chainlink ghost worker and API,
run after the change is pushed to GitHub. Preserve production environment files,
the continuous state directory and all retention settings:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink price-api
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/accuracy
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/comparison
sudo journalctl -u price-collector-polymarket-chainlink -u price-api -n 100 --no-pager
```

Restart only `price-api` for an API-only change, or only the Chainlink collector
for a collector-only change. If schema changes, apply the schema transaction
before either affected restart, using the installation sequence above. If a
systemd unit changes, install that exact unit and run `daemon-reload` first.
Documentation/local frontend changes alone do not require service restarts.
Forecast warm-up and the monitor's first refresh may temporarily return typed
503 responses; absence does not authorize resetting state or extending expiry.

The API flag `GHOST_TWAP_API_ENABLED=true` belongs in `/etc/price-collector/api.env`
with reader credentials only. It does not enable the producer. Default bounds
are 16 SSE clients, 250 ms Redis snapshot reads and 2,000 ms ASGI send waits.
Optional `GHOST_TWAP_API_MAX_CLIENTS`, `GHOST_TWAP_API_READ_TIMEOUT_MS` and
`GHOST_TWAP_API_SEND_TIMEOUT_MS` can adjust within the existing validated limits.
Malformed optional settings disable ghost routes with `invalid_settings`, not
ordinary source routes. Never expose port 9000 publicly.

On the owner's computer, the existing launcher manages the local proxy and
loopback tunnel. A manual equivalent for the tunnel is:

```powershell
ssh -N -L 127.0.0.1:19000:127.0.0.1:9000 root@152.42.247.86
```

Subscribe to `event: ghost`; producer state is in the nested `ghost` object and
delivery state in `api`. A null ghost is unavailable. Resync, generation changes
and skipped updates replace local current state; event IDs provide no historical
replay. Expire browser values locally even when the tunnel stops delivering.
Never restart a full `remaining_ns` lifetime upon browser receipt. The saved
comparison route uses finalized compact records and can recover its chart after
sleep; its cohort and lag differ from the live current-state stream.

A bounded stream check is:

```bash
curl -N --max-time 12 http://127.0.0.1:9000/forecasts/chainlink-twap/stream
```

The timeout intentionally ends this diagnostic. API disablement is independent:
set only its flag false and restart `price-api`. To stop production, set the
collector's `GHOST_TWAP_ENABLED=false` and restart the Chainlink collector;
editing the file alone does not stop an already running worker. Review its
shutdown completion and retained outbox rather than assuming a restart drained
all audit evidence. Do not re-enable or reset state merely to resolve a tail.

For the dashboard-history/Price-to-Beat reader addition, deployment is API-only:
no schema, environment, collector restart or forecast restart is required. After
pushing to GitHub, run:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-api
sudo systemctl status price-api --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/dashboard
sudo journalctl -u price-api -n 60 --no-pager
```

The local frontend proxy must also be restarted to load the new allowlisted
route. Missing official opening evidence remains unavailable; never fill it
with a sampled spot/TWAP approximation. That route's bounded historical reads
are independent of live ghost publication.

### Legacy ghost canary evidence

The completed one-hour campaigns use their original terminal/export/age rules;
continuous seven-day compaction does not silently apply to those records.
Past activation sequences are historical evidence, not a current operating
procedure. Do not create a new campaign or change state/start to bypass a guard.
The canonical reference records the Git revision containing the exact original
protocols, result manifests and clock/cohort definitions.

Read current audit state without changing it:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin status
sudo journalctl -u price-collector-polymarket-chainlink --since '10 minutes ago' -n 100 --no-pager
df -h /var/lib/postgresql /var/lib/price-collector
```

For an explicitly requested external export, run on the owner's computer with
a fresh destination filename:

```powershell
python -m price_collector.ghost_twap_admin download --ssh root@152.42.247.86 --output ghost-canary.jsonl
```

The command refuses overwrite, verifies every row and whole-file hash, then
acknowledges only unchanged current versions. Interrupted downloads remain
`.part` files. A same-droplet copy is not external verification. Preserve the
adjacent manifest and original outbox until recovery/export is confirmed.
Only terminal rows at least 96 hours old with matching verified export hashes
are eligible for the explicit bounded legacy expiry command:

```bash
cd /opt/price-collector
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin expire --limit 100
```

A later result change invalidates export eligibility. The archive adapter has
no configured always-on external transport; it is not a prerequisite for the
owner's seven-day continuous retention design. Production runtime/admin/observer
modules and their tests remain supported even though dated reports are removed.
Ordinary deletion/vacuum can leave allocated bytes reusable instead of shrinking
files. Do not add `VACUUM FULL`, rewrite relations or raise caps as routine cleanup.

### Deploy compact Polymarket evidence

Run these commands **after the change is pushed to GitHub**. This checkpoint
changes the probability service and adds schema objects. Pull and install
dependencies, then apply the schema **before** restarting that service:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudoedit /etc/price-collector/collector.env
sudo systemctl restart price-collector-polymarket-probabilities
sudo systemctl status price-collector-polymarket-probabilities --no-pager
sudo journalctl -u price-collector-polymarket-probabilities -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
```

During `sudoedit`, manually add/review the following keys and set
`POLYMARKET_EVIDENCE_ENABLED=true` to start capture. The code and example default
is `false`. Preserve existing credentials and other settings; never replace
the production file with an example. This update needs no unit copy,
`daemon-reload`, Redis restart or API environment change.

| Collector environment key | Example/default |
| --- | ---: |
| `POLYMARKET_EVIDENCE_ENABLED` | `false` |
| `POLYMARKET_EVIDENCE_POLL_SECONDS` | `5` |
| `POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS` | `100` |
| `POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS` | `120` |
| `POLYMARKET_EVIDENCE_QUEUE_MAX_RECORDS` | `5000` |
| `POLYMARKET_EVIDENCE_QUOTE_QUEUE_MAX_RECORDS` | `2000` |
| `POLYMARKET_EVIDENCE_BATCH_MAX_ROWS` | `250` |
| `POLYMARKET_EVIDENCE_FLUSH_MS` | `500` |
| `POLYMARKET_EVIDENCE_WARN_RELATION_MB` | `4096` |
| `POLYMARKET_EVIDENCE_MAX_RELATION_MB` | `6144` |

These are example/code defaults. The deployed September 16 overrides are
`POLYMARKET_EVIDENCE_WARN_RELATION_MB=3072` and
`POLYMARKET_EVIDENCE_MAX_RELATION_MB=4096`; preserve them unless the shared
capacity allocation is deliberately revised. Microstructure uses the same
3072/4096 MiB warning/cap pair. See the
[current activation record](GHOST_TWAP_REFERENCE.md#storage-safeguards-and-maintenance).

Keep unrelated raw-capture flags off. The
[evidence architecture](#compact-polymarket-evidence-for-h3) explains its four
metadata kinds, quote timing and independence from core probability collection.

### Verify prospective evidence

Run after capture has covered the final 120 seconds of an active market.
Missing/non-OK observations and session/gap rows are evidence, not fabricated
successful samples. This query shows whether Price to Beat was actually
received before close and joins the saved payload by its hash:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
SET statement_timeout = '15s';
SET TIME ZONE 'UTC';
SELECT o.market_id, o.status,
       p.payload->>'price_to_beat' AS price_to_beat,
       p.payload->>'api_timestamp_ms' AS api_cache_timestamp_ms,
       o.response_date, o.response_age_seconds,
       to_timestamp(o.received_wall_ns / 1000000000) AS received_at,
       w.market_end_ms - o.received_wall_ns / 1000000 AS ms_before_close
FROM polymarket_market_observations o
JOIN market_windows w USING (market_id)
LEFT JOIN polymarket_evidence_payloads p USING (payload_hash)
WHERE o.kind = 'price_to_beat'
  AND o.market_id >= floor(extract(epoch FROM now() - interval '1 day') / 300)::bigint
ORDER BY o.market_id DESC, o.received_wall_ns DESC
LIMIT 12;

SELECT kind, status, count(*) AS rows,
       max(received_wall_ns) AS latest_received_wall_ns
FROM polymarket_market_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '1 day') / 300)::bigint
GROUP BY kind, status ORDER BY kind, status;

SELECT market_id, count(*) AS quote_rows,
       min(observed_wall_ns) AS first_observed_wall_ns,
       max(observed_wall_ns) AS last_observed_wall_ns
FROM polymarket_quote_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '1 hour') / 300)::bigint
GROUP BY market_id ORDER BY market_id DESC LIMIT 12;
SQL
```

An `ok` opening reference needs a non-null price and positive
`ms_before_close` to demonstrate pre-close availability. Use its actual receipt
time for later decisions. The official website endpoint can report
`incomplete=true` while providing a valid opening price; Gamma can omit that
price until resolution. Response digest, Date and Age are stored in the
observation columns `response_sha256`, `response_date` and
`response_age_seconds`; the linked payload holds normalized source data.
API/cache timestamps, HTTP round-trip time and
provider timestamps are not measured order-to-fill latency. Keep CLOB `itode`
separate from `seconds_delay`; zero seconds alone does not negate an enabled
taker-delay flag. A 100 ms quote sample cannot reconstruct intermediate
events, demonstrate depth, or prove a chosen-size fill.

### Measure storage and losses

The evidence budget covers these three relations, including indexes and TOAST.
It does not include PostgreSQL WAL, other tables, backups or all disk use.
Measure actual sizes and UTC-day row counts; do not extrapolate a promised
retention period from a short WebSocket probe:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector <<'SQL'
SET statement_timeout = '15s';
SET TIME ZONE 'UTC';
WITH relations AS (
  SELECT unnest(ARRAY[
    'public.polymarket_evidence_payloads'::regclass,
    'public.polymarket_market_observations'::regclass,
    'public.polymarket_quote_observations'::regclass
  ]) AS rel
)
SELECT rel::text AS relation,
       pg_table_size(rel) AS table_and_toast_bytes,
       pg_indexes_size(rel) AS index_bytes,
       pg_total_relation_size(rel) AS total_bytes,
       sum(pg_total_relation_size(rel)) OVER () AS all_evidence_bytes
FROM relations ORDER BY relation;

SELECT (to_timestamp(observed_wall_ns / 1000000000) AT TIME ZONE 'UTC')::date AS utc_day,
       count(*) AS quote_rows, count(DISTINCT market_id) AS observed_markets
FROM polymarket_quote_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '3 days') / 300)::bigint
GROUP BY utc_day ORDER BY utc_day;

SELECT (to_timestamp(received_wall_ns / 1000000000) AT TIME ZONE 'UTC')::date AS utc_day,
       kind, count(*) AS observation_rows
FROM polymarket_market_observations
WHERE market_id >= floor(extract(epoch FROM now() - interval '3 days') / 300)::bigint
GROUP BY utc_day, kind ORDER BY utc_day, kind;
SQL
sudo journalctl -u price-collector-polymarket-probabilities --since '1 hour ago' -n 200 --no-pager
```

Repeat measurements over actual collection days and compare against available
disk space. Queue overflow or write failure must surface as gaps/logs. The
pending queues are in memory, so unclean session recovery marks possible loss
rather than reconstructing unwritten observations. The
default guard warns at 4096 MiB and pauses new quote capture at 6144 MiB; the
current production overrides warn at 3072 MiB and pause at 4096 MiB.
Metadata/control observations continue and accepted writes may drain, so this
is not a strict maximum size. The separate ten-day history-retention timer
removes expired evidence; the size guard itself does not delete it. Export any
evidence needed beyond that retention window before it expires.

To disable optional capture, manually set `POLYMARKET_EVIDENCE_ENABLED=false`
in `/etc/price-collector/collector.env`, restart only
`price-collector-polymarket-probabilities`, and repeat its status, bounded log
and local health checks above. Existing data remains subject to ten-day history
retention even while optional collection is off.

### Other runtime changes

Use [Deploy Updates From GitHub](#deploy-updates-from-github) and the exact
service map in `AGENTS.md` for changes outside the feature procedures above.

Historical raw-capture Phase 4 partition/retention validation remains deferred:
future partition creation, expiry, 72-hour retention and sustained raw-table
budget enforcement are not proven by short canaries. This runbook does not
restore retired research procedures or declare that validation complete.
