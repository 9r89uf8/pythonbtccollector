# Price Collector

Production-oriented BTC market-data collection for a single-user Ubuntu 24.04
DigitalOcean droplet. The application collects spot, oracle, futures, order-flow,
top-of-book, and Polymarket probability data into local PostgreSQL. Redis holds
the latest values needed by the live API response and, when explicitly enabled,
a short-lived experimental Chainlink catch-up projection.

The deployment is deliberately private:

- PostgreSQL listens only on the droplet loopback interface.
- Redis listens only on `127.0.0.1:6379` with protected mode enabled.
- FastAPI listens only on `127.0.0.1:9000`.
- Remote API access goes through an SSH tunnel.
- No Docker, public dashboard, public database port, or public API port is used.

## Architecture

```text
DigitalOcean droplet
├── systemd
│   ├── price-collector.service
│   ├── price-collector-polymarket-chainlink.service
│   ├── price-collector-binance-futures.service
│   ├── price-collector-polymarket-probabilities.service
│   ├── price-collector-shadow-signal.service  Opt-in experimental worker
│   └── price-api.service
├── /opt/price-collector              Git checkout and Python virtualenv
├── /etc/price-collector              Root-owned environment files
├── /var/lib/price-collector          State and trusted decision evidence
├── Redis on 127.0.0.1:6379           Live-value cache only
└── PostgreSQL database price_collector
                                      Historical source of record
```

The API uses a read-only PostgreSQL role. Collectors use a separate writer role.
All prices remain `Decimal` values in Python, are stored in PostgreSQL numeric
columns, and are serialized as strings by the API.

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

### Polymarket Chainlink BTC/USD

`python -m price_collector.polymarket_chainlink_collector`

- Connects to Polymarket RTDS at `wss://ws-live-data.polymarket.com`.
- Subscribes to topic `crypto_prices_chainlink` with filter
  `{"symbol":"btc/usd"}`.
- Parses `payload.value` as the price and `payload.timestamp` as source time.
- Writes Redis key `btc:live:chainlink` before historical storage.
- Floors the source payload timestamp to its UTC second and upserts that second
  into PostgreSQL.
- Proactively reconnects an active-but-unproductive RTDS socket when no valid
  BTC/USD Chainlink event is accepted for 10 seconds by default. Control and
  malformed frames do not reset that monotonic deadline.

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
- Stores Polymarket's official Chainlink open/close prices, winner or split
  result, official payouts, winning token ID, and resolution timestamp when
  available. Probability quotes are never used to infer the winner.

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
- `price_samples` for Binance Spot and Polymarket Chainlink prices
- `polymarket_btc_5m_markets`, `polymarket_probability_samples`, and
  `polymarket_btc_5m_resolutions` for discovered markets, probability history,
  and official Polymarket resolution metadata
- `binance_futures_snapshots`
- `binance_futures_oi_5m_summaries`
- `binance_flow_1s`
- `binance_book_1s`

Prices in `price_samples` use `NUMERIC(38,18)`. Its primary key is
`(instrument_id, sample_second_ms)`, and a duplicate sample for the same
instrument and second updates the existing row instead of creating a duplicate.
The other one-second tables follow the same upsert pattern with source-specific
keys.

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

The public Chainlink value remains RTDS `payload.value` delivered through
`btc:live:chainlink`. With both raw flags `false`, neither collector creates a
raw queue, raw writer/maintenance task, raw feed-session record, or dedicated
raw database connection. The futures reader still records its connection and
pre-parse receive stamp because those are now part of the public last-price
path. A phase's deployed code is only its code checkpoint. The corresponding
explicitly accelerated three-hour production canary and all acceptance checks
in `OPERATIONS.md` must pass before that phase is operationally complete. This
short gate provides less confidence about slow leaks, reconnects, daily traffic
variation, and sustained storage growth than a 24-hour canary. After advancing,
leave capture enabled and continue background observation toward 24
uninterrupted hours; a later failure still requires the documented rollback.

Phase 4's deliberate partition-boundary and retention validation has been
explicitly deferred while work proceeds to Phase 5. It is not proven by either
three-hour canary: a short window may not cross a six-hour partition boundary.
Automatic future-partition creation, expired-partition removal, the configured
72-hour retention behavior, and sustained relation-budget enforcement therefore
remain known production risks and must not be described as validated.

Redis is not a historical store. The three source-price keys are:

- `btc:live:binance_spot`
- `btc:live:chainlink`
- `btc:live:futures`

Each value has this shape:

```json
{"value":"62067.89","source_timestamp_ms":123,"received_ms":456}
```

The Chainlink producer additionally writes internal
`publisher_epoch` and `accepted_event_sequence` fields in the same atomic
value. The three fields above remain unchanged, other source keys remain
compatible, and API serialization never exposes the continuity metadata.

Shadow-signal Phases 4 and 5 can also create `btc:live:chainlink_shadow`. That key is a
typed shadow projection rather than a source price, and it expires 1.5 to 2.0
seconds after each write. Those phases deliberately kept it out of the API.
Phase 6 reads it alongside the three source-price keys and exposes it only as
the optional nested `signals.chainlink_catchup` object.

### Shadow-signal design engine

Shadow-signal development is separate from every live collector and from
FastAPI. Phase 1 adds the dependency-free anchor/ratio state machine in
`price_collector.shadow_signal`. Phase 2 adds the offline, read-only raw replay
in `price_collector.shadow_signal_replay`. Phase 3 adds the offline,
deterministic provisional selector in
`price_collector.shadow_signal_selection`.

The replay:

- maps each futures 100 ms row to its `close_price`,
  `last_received_wall_ns`, and `last_trade_time_ms`;
- preserves individual Chainlink events on their exact receive-wall timeline,
  while separately delaying source visibility by configured replay sensitivity
  assumptions;
- emulates an epoch-aligned Redis `MGET` every 100 ms and attempts forecasts
  every 500 ms at a configured phase offset;
- evaluates 3.0, 3.5, and 4.0-second ratio candidates against a separately
  paired no-change baseline;
- uses only completed, drop-free, count-reconciled futures/Chainlink session
  intersections and resets state at every common session boundary;
- reports coverage, censoring, median/mean error, RMSE, baseline skill, a full
  `up`/`neutral`/`down` confusion matrix and derived directional rates,
  move-size, raw-bucket-return RMS, market-expiry, and session-edge slices; and
- keeps prices and calculations as `Decimal`, serialized as JSON strings.

Each candidate's top-level metrics include all of its horizon-specific scored
rows. Cross-model comparisons must use `candidates[].common_cohort.metrics`,
which restricts every candidate to the same generation times where the maximum
horizon fits and all configured models are valid. Replay schema version 3 also
provides exact sufficient statistics and common-cohort slices so reports can be
pooled without averaging RMSE, skill rates, directional rates, or sampled
medians. Directional action precision includes every predicted action,
including false actions when the actual outcome is neutral.

Schema v3 freezes `max_future_skew_ms=0` and records independent fixed
futures/Chainlink availability delays plus one of the five 100 ms evaluation
phase offsets. These controls are sensitivity assumptions, not measurements of
Redis publication completion. A timing study must run a preregistered delay and
phase grid and compare the complete grid; it must not select whichever timing
scenario makes a candidate look best.

The volatility-regime slices use the same causal visibility boundary. A
futures-bucket return enters the volatility series only on the worker poll that
makes the delayed futures event visible, and the rolling lookback uses that
worker-poll visibility time. New reports record this contract as
`configuration.volatility_time_basis="worker_poll_visibility_ms"`. The v3
selector rejects reports without that marker. Pre-fix timing-sensitive v3
reports must be regenerated from retained raw events before they are selected
or compared with post-fix reports. Existing immutable v2/v3 decision pairs
remain loadable because this diagnostic does not affect projection or selection
ranking; they are not valid inputs to a new post-fix selection.

The command requires an explicit inclusive/exclusive UTC epoch-ms range and
uses the writer `DATABASE_URL`, because the API reader cannot access
`raw_capture`. A single report is limited to 24 hours and uses bounded
reservoirs for quantiles; means, RMSE, counts, and coverage remain exact. Raw
reads are split into short time chunks so the offline job does not hold a
transaction or raw-partition locks for the duration of the replay. Operational
bounds retain the 100 ms raw grid, require at least a 500 ms evaluation cadence,
cap lags at 10 seconds and retained history at 30 seconds, cap each
deterministic quantile reservoir at 50,000 values with a 10,000-value default,
and allow at most five lag candidates under a shared candidate/sample budget.

Phase 2 does not choose or configure a primary model, write PostgreSQL or
Redis, add an API response, or run as a service. The report explicitly sets
`selection_performed=false`. Run it while the required evidence remains inside
raw retention, and preserve its JSON output beyond that retention window. See
the shadow-signal replay section in `OPERATIONS.md` for the copy/paste command.

Phase 3 accepts explicitly assigned, non-overlapping older calibration reports
and one later holdout report. Policy `chronological_holdout_v3` ranks the three
fixed V0 candidates only on calibration common-cohort MAE and RMSE skill,
freezes that winner, and either accepts it on holdout or abstains. It never
reranks on the holdout or falls back to another model. Each report must contain
at least 10,000 common scored forecasts, at least 50% common valid coverage,
and at least 99% maturation coverage. The winner must have positive MAE and
RMSE skill against its paired no-change baseline in both calibration and
holdout. Paired win/loss frequency remains a visible diagnostic but does not
affect eligibility or ranking because overlapping 500 ms rows are
autocorrelated. These are explicit project policy thresholds, not
statistical-significance claims or values supplied by `engine.md`.

Policy v3 supersedes v2. Every holdout inspected while developing v3, including
the accepted v2 holdout, must be reclassified as calibration evidence. V3
requires exactly one new, strictly later untouched holdout. Existing v1 and v2
selection artifacts remain historical and must not be overwritten.

The selector writes a deterministic schema-version-3 artifact with report
hashes, evidence ranges, pooled metrics, common-cohort slices, gates, warnings,
and either one provisional primary or `null`. Artifact creation is atomic and
create-once; an identical rerun is idempotent, while different content cannot
replace an existing decision. No production replay reports are committed to
this repository, so the code does not guess or hard-code a winner. The accepted
artifact is an input to the later live-worker phase; Phase 3 itself does not
change configuration, Redis, PostgreSQL, the API, or systemd. See the Phase 3
selection section in `OPERATIONS.md`.

This v3 checkpoint deliberately retains the 3.0/3.5/4.0-second V0 candidate
set so the measurement corrections can be verified independently. It does not
test 2.0 or 2.5 seconds. Adding shorter candidates requires a newly versioned,
preregistered candidate policy, all previously inspected evidence as
calibration, and a genuinely later untouched holdout.

Shadow-signal Phase 4 adds the opt-in, standalone
`price-collector-shadow-signal` service. Its epoch-aligned 100 ms loop reads
`btc:live:futures` and `btc:live:chainlink` together with one Redis `MGET`,
feeds the pure engine, and atomically refreshes
`btc:live:chainlink_shadow` with a default 2,000 ms TTL. It instantiates all
three fixed V0 candidates, but serializes only the provisional primary frozen
by the accepted Phase 3 artifact. The accepted production decision currently
selects `catchup_ratio_l3000_b100`; that model is discovered from the artifact,
not hard-coded in the worker or its environment.

The worker starts only when an accepted selection artifact and the exact replay
report that defines its configuration have been promoted to
`/var/lib/price-collector/shadow-decisions`. Both files must be
`root:pricecollector` mode `0440`. The dedicated root-owned
`/etc/price-collector/shadow-signal.env` pins both absolute paths and the full
selection-file SHA-256. At startup the worker verifies the selection hash, the
report hash recorded in selection provenance, the replay configuration digest,
policy, evidence, candidate set, and frozen primary before it opens Redis. It
does not choose a model dynamically or fall back to another candidate. The
loader keeps the currently promoted v2 selection/replay pair valid under its
original semantics and separately accepts only matching v3 pairs with the
exact v3 policy and zero future skew; schema versions cannot be mixed.

Every tick replaces the cached payload. When either source is missing, stale,
malformed, regresses, lacks anchor history, or otherwise fails validation, the
new payload is invalid and all projection fields are `null`; a previous valid
forecast is never carried forward. If the worker dies, the short TTL removes
the key while the three source-price keys and producer collectors continue
normally.

Shadow-signal Phase 5 retains that live contract and adds independently opt-in
matured evaluations. Once per entered epoch-aligned 500 ms bucket, the worker
schedules every configured candidate attempt, including invalid attempts. It
does not backfill buckets missed while paused. The complete generated-time
cohort remains pending until its maximum
`target_ms = generated_ms + horizon_ms`. At that finalization tick, each
candidate is resolved separately against the newest retained Chainlink
observation whose `received_ms` is no later than that candidate's target. A
Chainlink update received after a target is excluded even though the complete
cohort is finalized later.

`generated_ms` is captured immediately after the Redis `MGET`, so it represents
when the forecast inputs are locally available. A future-dated input is stored
as an invalid evaluation rather than scored. The Chainlink producer includes a
process epoch and monotonic accepted-event sequence in the same Redis value.
The evaluator invalidates outcome history on a sequence jump or regression, a
producer-epoch change, or metadata loss. Within one publisher epoch, each
accepted sequence is also bound to one immutable identity consisting of source
timestamp, receive timestamp, and price. An identical repeat is normal; a
different identity under the same sequence resets outstanding history and
enters `chainlink_sequence_identity_mismatch` quarantine. Neither disputed
identity is admitted to the new history epoch or allowed to confirm maturation.
The last sequence binding remains known across a metadata-less read, preventing
metadata recovery from silently redefining that sequence.
Attempts generated while quarantined remain scheduled for coverage but mature
`integrity_invalid`; only a newer sequence or publisher epoch establishes a
clean baseline. This
integrity rule applies whenever sequence metadata is present, including v2.

Schema v2 retains legacy-only startup and its conservative gap check. Schema v3
does not admit any Chainlink value into outcome history until the first atomic
producer-epoch/accepted-sequence pair is observed. It still schedules every
candidate attempt for coverage, but cohorts generated
before establishment are permanently unscoreable: they mature with null
actual/error fields, `outcome_status=integrity_invalid`, and
`chainlink_sequence_not_established`. Later sequence establishment cannot
retroactively validate them, and consumed cadence buckets are not backfilled.
The first newly entered bucket after establishment can score normally. Once
sequencing has been established, metadata loss suppresses actual-outcome
ingestion until a sequenced value recovers continuity. Redis is still
latest-value-only, so missing events are not reconstructed. Sequence
discontinuities that become visible invalidate the affected live evidence,
while the raw replay remains
the event-complete authority for selection.

For an otherwise outcome-eligible schema-v3 cohort, reaching the maximum target
is necessary but not enough to finalize it. The evaluator also requires a
successful Chainlink cache observation with sequence metadata at or after that
target. A missing or malformed Chainlink value defers maturation for at most
two configured poll intervals. Confirmation at or before that deadline permits
normal causal target
resolution; otherwise, the whole cohort is emitted with null actual/error
fields, `outcome_status=integrity_invalid`, and
`chainlink_sequence_confirmation_timeout`. Schema v2 retains its prior
maturation behavior.

No final evaluation row is constructed at a shorter target. If any outcome
history reset occurs between generation and the maximum target, every row in
that cohort has null actual/error fields, `outcome_status=integrity_invalid`,
and the same explicit reset reasons. With intact continuity, each row is
`available` when it has a causal actual or `unavailable` when it does not.
Every row receives the common `matured_ms` at which the complete cohort became
eligible for persistence, including any bounded v3 confirmation wait. The
cohort then enters a bounded nonblocking queue
and is batch-inserted into `shadow_signal_evaluations`. Database connection,
retry, retention, and
shutdown work never runs on the 100 ms publication path. If the queue fills
during an outage, whole oldest cohorts are dropped rather than individual
candidate rows;
a rate-limited warning is emitted on the first and every hundredth dropped
cohort, and Redis signal generation continues. Batching, retry, permanent-error
isolation, deferral, and retention also preserve whole cohorts. The database
backend receives typed cohorts and returns exact, disjoint persisted, rejected,
and deferred cohort identities; the writer fails and retries the complete batch
if any identity is missing, unknown, or classified more than once. Transiently
failed batches are requeued ahead of newer cohorts and retried safely because
inserts are idempotent on
`(model_version, generated_ms, horizon_ms)`, and the default derived-evidence
retention is seven days. A deterministic PostgreSQL integrity or data error is
isolated at cohort boundaries inside one transaction and the entire affected
cohort is rejected instead of poisoning the retry queue. Isolation is capped;
rejection and cap events are rate-limited, and unprobed whole cohorts return to
the bounded queue instead of being mislabeled as rejected. These events remain
evidence-coverage failures that require investigation. Shutdown telemetry keeps
both row and cohort totals for offered, enqueued, persisted, rejected, deferred,
and dropped work, plus row/cohort queue high-water marks. Invalid model attempts
that satisfy the storage contract are
stored to preserve an honest coverage denominator. For valid attempts with an
actual, signed `forecast_error` is
`projected_chainlink - actual_chainlink` and signed
`baseline_error` is the no-change result
`chainlink_at_forecast - actual_chainlink`.

The schema migration that introduced cohort-wide finalization quarantines
pre-fix rows because their shorter-horizon actuals cannot be proven clean after
the fact. It preserves the base rows, labels them `legacy_unverified` with
`pre_cohort_integrity_fix_unverified` as the reason, and excludes them from the
reader view so they cannot contribute to API reporting.

Move-size, direction, expiry, and sampled-volatility slices can be derived from
these rows. Reconnect slices require a receive-time join to
`raw_capture.feed_sessions`; materialize any longer-lived reconnect report
before the separate 72-hour raw-capture retention removes that join evidence.

The evaluation table is internal: `price_writer` receives only `SELECT`,
`INSERT`, and retention `DELETE`, while `price_reader` and `PUBLIC` are
explicitly revoked. Phase 5 deliberately added no API response or dashboard
code. It is distinct from the Binance futures-collector Phase 5 source cutover
and from the still-deferred high-resolution raw-capture Phase 4 partition and
72-hour-retention validation; it does not validate either rollout.

Shadow-signal Phase 6 adds a Redis-only serialization path to the existing
`GET /markets/current/live` endpoint. The route obtains the three source-price
keys and `btc:live:chainlink_shadow` with one four-key `MGET`. It decodes the
shadow value independently with its dedicated typed decoder, never as a
`LivePrice`, and returns it under `signals.chainlink_catchup`. A present,
well-formed signal is returned as an object, including a request-time
`signal_age_ms = max(0, server_time_ms - generated_ms)`; a missing, expired, or
malformed shadow value produces `null` in that slot. A malformed shadow value is
logged but cannot turn otherwise readable actual prices into an HTTP `503`.

FastAPI remains a read-only serializer in Phase 6. The live route performs no
PostgreSQL query and neither runs nor imports model execution or model state.
The shadow worker remains isolated in its standalone service. Phase 6 is
backend-only.

The Phase 7 backend prerequisite adds PostgreSQL reporting routes at
`GET /markets/current/shadow-evaluations` and
`GET /markets/{market_id}/shadow-evaluations`, plus rounded JSON attachment
variants ending in `/download`. All four require one explicitly
supported V0 `model_version`, select one five-minute window by forecast
`target_ms`, include boundary-crossing forecasts generated in the predecessor
market, and reject an anomalous result above 1,000 rows. Financial values remain
Decimal-derived JSON strings or `null`. The reporting routes retain full
precision. Downloads round only after validation and performance calculation:
USD prices, moves, errors, and USD metrics use two decimal places; basis points,
beta, skills, and rates use four. Their `export` object records this lossy
policy, and attachment filenames end in `_rounded.json`. Evaluation persistence
defaults to seven days; an export contains only rows still retained when it is
requested. Every point carries its persisted forecast-time
Chainlink and futures cache snapshots: `chainlink_at_forecast` and
`futures_at_forecast`, each with source and local-receive timestamps. Those
snapshots correspond to `generated_ms`; `projected_chainlink` and the causal
`actual_chainlink` correspond to `target_ms`. This makes chart rows
self-contained without reconstructing forecast-time futures history from the
latest-only live cache. The backend does not persist the dashboard's
target-aligned `actual_futures`, so that browser-computed field is not present
in either reporting or download responses.

Report schema v2 exposes the complete selection identity (schema, policy,
evidence end, fingerprint, and artifact hash) and keeps generation-time
forecast validity separate from target-time `outcome_status`. Consumers can
therefore distinguish an ordinary `unavailable` target from
`integrity_invalid` evidence and its explicit reasons. Each response also
derives per-market performance cohorts from valid points with an `available`
outcome, separated by the full selection identity: forecast MAE,
median/p95/maximum absolute error, RMSE, signed bias, no-change baseline
comparisons, skill, and paired wins/ties/losses. This calculation uses the
already fetched rows and does not add storage or another query. The API reader
still has no privilege on the base `shadow_signal_evaluations` table; it can
select only the deliberately narrow
`shadow_signal_evaluation_chart_points` view. The view exposes only the six
approved forecast-time snapshot fields and excludes `futures_reference` and
its metadata, worker age/internal fields, and writer metadata such as
`created_at`. `/markets/current/live` remains Redis-only and unchanged.
Reporting also declares
`evaluation_semantics.scored_input_max_future_skew_ms=0`: persisted live
evaluations use zero-skew forecast inputs even when their selection provenance
is schema v2, so those rows are not directly comparable to v2 replay evidence
that allowed nonzero skew. This narrow causality rule does not relabel the
selection or change the live Redis projection's activated configuration. No
dashboard code is included in this repository.
The Phase 7 product, chart semantics, laptop networking, and Vite build plan
are specified in
[`SHADOW_SIGNAL_DASHBOARD_DESIGN.md`](SHADOW_SIGNAL_DASHBOARD_DESIGN.md).
Deploy the reporting prerequisite with the schema-first procedure in
[`OPERATIONS.md`](OPERATIONS.md#shadow-signal-evaluation-reporting-api).
Deploy and verify the Phase 6 API addition with
[`SHADOW_SIGNAL_PHASE6_MIGRATION.md`](SHADOW_SIGNAL_PHASE6_MIGRATION.md).

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
- `GET /markets/current/shadow-evaluations?model_version=...`
- `GET /markets/{market_id}/shadow-evaluations?model_version=...`
- `GET /markets/current/shadow-evaluations/download?model_version=...`
- `GET /markets/{market_id}/shadow-evaluations/download?model_version=...`

The data and download routes accept optional `include_probabilities`,
`include_futures`, `include_oi`, `include_flow`, `include_book`, `fill_display`,
and `max_carry_forward_ms` query parameters. `/markets/current/live` reads the
three source prices plus the optional shadow signal from Redis with one four-key
`MGET`; the historical and aggregate routes read PostgreSQL.
The shadow-evaluation reporting and attachment routes also read PostgreSQL, but
only through their restricted view and only for one explicit model and
five-minute target window. They do not alter the Redis-only live request path.

The data and download responses use schema version `2` and always include
`market.chainlink_resolution` and `market.resolution`, independently of the
optional series flags. These objects contain only official Polymarket
Gamma/CLOB data. Ended markets can remain `pending` briefly while the collector
waits for official resolution; the last Up/Down probability is never treated as
the winner. The active CLOB connection stays open for a short grace period after
the market boundary to capture an official resolution event, while durable REST
reconciliation handles delayed results and fills the official Chainlink values.
Downloads keep schema version `2` as a compact projection: they omit the market
start/end millisecond fields and per-row `timestamp_ms`, retain the equivalent
UTC `*_at` strings, and format official Chainlink open/close values to two
decimal places. The data routes retain their full timing and precision fields.

`GET /markets` is the frontend discovery route. It returns the newest three
completed markets by default, newest first, with market timestamps and
per-source availability counts. Use `include_current=true` to include an
observed active market, and pass the returned `next_before_market_id` as the
exclusive `before_market_id` cursor for older pages. Future and observation-empty
windows are not returned. The frontend should select a returned `market_id` and
then request `/markets/{market_id}/data` with the desired optional datasets.

See [`LIVE_DATA.md`](LIVE_DATA.md) for the end-to-end live price pipeline,
Redis handoff, freshness semantics, and dashboard consumption guidance. See
[`FRONTEND_API.md`](FRONTEND_API.md) for frontend call examples, query
parameters, complete response shapes, optional fields, and error responses.

## Repository Layout

```text
price_collector/       Collectors, API, shadow engine, replay, and selection
price_collector/shadow_signal_reporting.py  Bounded read-only evaluation reporting
deployment/            systemd units and environment-file examples
tests/                 Unit and deployment-safety tests
schema.sql             PostgreSQL tables, indexes, constraints, and seed rows
OPERATIONS.md          Update, verification, logs, tunnel, and spot-check commands
SHADOW_SIGNAL_PHASE5_MIGRATION.md  Matured-evaluation rollout and rollback
SHADOW_SIGNAL_PHASE6_MIGRATION.md  Redis-only live API rollout and rollback
SHADOW_SIGNAL_DASHBOARD_DESIGN.md  Phase 7 Vite dashboard and chart specification
FRONTEND_API.md        Frontend-facing FastAPI endpoint and response reference
LIVE_DATA.md           Live BTC extraction, cache, and dashboard consumption guide
requirements.txt       Python runtime and test dependencies
```

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
The collectors, API, and experimental shadow worker intentionally use separate
environment files:

- `/etc/price-collector/collector.env`, created from
  `deployment/collector.env.example`, contains the writer database URL and
  collector settings.
- `/etc/price-collector/api.env`, created from
  `deployment/api.env.example`, contains only the reader database URL and Redis
  settings.
- `/etc/price-collector/shadow-signal.env`, created from
  `deployment/shadow-signal.env.example`, contains Redis, pinned shadow
  decision settings, and disabled evaluation controls. The default example has
  no database credential. The Phase 5 migration adds `DATABASE_URL` only while
  matured evaluation is enabled; this file must never contain
  `READ_DATABASE_URL`.

The Chainlink collector's accepted-event watchdog is configured in
`collector.env`:

```text
POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000
```

The value must be between 5,000 and 60,000 ms. It is independent of
`STALE_PRICE_MS` and of the frozen shadow model's Chainlink freshness gate.
Only a successfully parsed and accepted `crypto_prices_chainlink` `btc/usd`
event resets the monotonic timer. When it expires, the collector classifies the
connection close as `proactive_reconnect`, applies the existing jittered
backoff, and resubscribes without restarting the process. The last Redis value
is left in place and continues aging until a fresh event arrives.

The shadow example remains disabled. Enabling it requires exact, distinct
selection and replay-report paths inside the trusted decision directory plus
the lowercase 64-character SHA-256 of the complete selection file:

```text
SHADOW_SIGNAL_ENABLED=false
SHADOW_SIGNAL_TRUSTED_DECISION_DIR=/var/lib/price-collector/shadow-decisions
SHADOW_SIGNAL_SELECTION_PATH=/var/lib/price-collector/shadow-decisions/primary-selection.json
SHADOW_SIGNAL_SELECTION_SHA256=0000000000000000000000000000000000000000000000000000000000000000
SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH=/var/lib/price-collector/shadow-decisions/replay-configuration.json
SHADOW_SIGNAL_POLL_MS=100
SHADOW_SIGNAL_TTL_MS=2000
SHADOW_SIGNAL_EVALUATION_ENABLED=false
SHADOW_SIGNAL_EVALUATION_INTERVAL_MS=500
SHADOW_SIGNAL_EVALUATION_QUEUE_MAX=5000
SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS=500
SHADOW_SIGNAL_EVALUATION_FLUSH_MS=1000
SHADOW_SIGNAL_EVALUATION_RETRY_MS=5000
SHADOW_SIGNAL_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS=10
SHADOW_SIGNAL_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS=5
SHADOW_SIGNAL_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS=5
SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS=168
SHADOW_SIGNAL_EVALUATION_RETENTION_CHECK_SECONDS=300
SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS=5000
```

The poll cadence is fixed at 100 ms. The TTL is constrained to 1,500 through
2,000 ms and defaults to 2,000 ms. Freshness, history, reference-gap, future
skew, candidate, and beta settings come from the verified replay configuration;
they are not independent live overrides. Follow the Phase 4 procedure in
[`OPERATIONS.md`](OPERATIONS.md) to promote immutable evidence, replace the
placeholder paths and hash, and only then set `SHADOW_SIGNAL_ENABLED=true`.
Keep `SHADOW_SIGNAL_EVALUATION_ENABLED=false` until the Phase 5 schema and
writer URL have been installed using
[`SHADOW_SIGNAL_PHASE5_MIGRATION.md`](SHADOW_SIGNAL_PHASE5_MIGRATION.md).

At minimum, replace the database passwords in:

```text
DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector
READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector
```

The probability collector's resolution reconciler uses these settings from
`collector.env`:

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

For the Phase 2 accelerated three-hour production canary, manually set only
`RAW_FUTURES_TRACE_ENABLED=true`, keep
`RAW_CHAINLINK_EVENTS_ENABLED=false`, and keep
`BINANCE_FUTURES_STREAMS_ENABLED=true`. Do not commit that production override
to the example file. Follow the full accelerated acceptance and rollback
procedure in [`OPERATIONS.md`](OPERATIONS.md). Passing the three-hour gate
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

REVOKE ALL ON TABLE shadow_signal_evaluations
FROM PUBLIC, price_reader, price_writer;
GRANT SELECT, INSERT, DELETE ON TABLE shadow_signal_evaluations
TO price_writer;

REVOKE ALL ON TABLE shadow_signal_evaluation_chart_points
FROM PUBLIC, price_reader, price_writer;
GRANT SELECT ON TABLE shadow_signal_evaluation_chart_points
TO price_reader;
\q
```

`schema.sql` separately creates and secures the `raw_capture` schema. Do not
grant `price_reader` or `PUBLIC` access to it. Its narrowly scoped
`price_writer` ownership and `CREATE` permission are for raw partition
maintenance only and do not grant DDL access in `public`.
The explicit base-table and reporting-view privilege corrections must remain
after the broad first-install grants. The API reader can select the restricted
view, but cannot access the internal evaluation table; the writer does not gain
view access.

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
sudo test -e /etc/price-collector/shadow-signal.env || sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/shadow-signal.env.example /etc/price-collector/shadow-signal.env

sudo nano /etc/price-collector/collector.env
sudo nano /etc/price-collector/api.env
```

The guarded install commands create each file only when it does not already
exist, so rerunning them cannot replace production secrets with example values.
Replace `REPLACE_ME` on the first deployment. Keep writer credentials out of
`api.env`, and keep all database credentials out of `shadow-signal.env` until
the Phase 5 migration copies in the existing writer `DATABASE_URL`. Never put
`READ_DATABASE_URL` in the shadow file. Leave the shadow worker disabled until
the Phase 4 evidence-promotion procedure has replaced its placeholder decision
settings, and leave evaluations disabled until the Phase 5 schema has been
applied.

### Install systemd Units

```bash
sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo cp /opt/price-collector/deployment/price-collector-binance-futures.service /etc/systemd/system/price-collector-binance-futures.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-probabilities.service /etc/systemd/system/price-collector-polymarket-probabilities.service
sudo cp /opt/price-collector/deployment/price-collector-shadow-signal.service /etc/systemd/system/price-collector-shadow-signal.service
sudo cp /opt/price-collector/deployment/price-api.service /etc/systemd/system/price-api.service

sudo systemctl daemon-reload
sudo systemctl enable --now price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api
```

The command installs the shadow unit but deliberately does not enable or start
it. Activate that unit only after its trusted evidence and dedicated environment
have passed the Phase 4 checks in [`OPERATIONS.md`](OPERATIONS.md). Enable its
PostgreSQL evaluations separately with
[`SHADOW_SIGNAL_PHASE5_MIGRATION.md`](SHADOW_SIGNAL_PHASE5_MIGRATION.md).

### Verify the Deployment

```bash
systemctl status redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-collector-polymarket-probabilities price-api --no-pager
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
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

## Deploy Updates From GitHub

After changes have been pushed to GitHub, a normal code update is:

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

If a systemd unit changed, copy the changed unit and reload systemd before the
restart:

```bash
sudo cp /opt/price-collector/deployment/CHANGED.service /etc/systemd/system/CHANGED.service
sudo systemctl daemon-reload
```

Always verify the affected services and local API afterward. See
[`OPERATIONS.md`](OPERATIONS.md) for copy/paste update sequences, logs, database
checks, Redis checks, and service verification.

## Maintenance

- Follow service logs with `journalctl -u SERVICE_NAME -f`.
- Monitor disk space with `df -h`.
- Check PostgreSQL size with
  `SELECT pg_size_pretty(pg_database_size('price_collector'));`.
- `RAW_CAPTURE_MAX_RELATION_MB` does not replace either check; PostgreSQL WAL
  and non-capture relations remain outside that budget.
- No automatic pruning is included for the long-term historical tables. The
  raw-capture partition-maintenance task runs only in a collector whose capture
  flag is enabled.
