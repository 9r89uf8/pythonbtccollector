# Sequence 1 plan: minimal high-resolution capture

I would narrow the name from **raw event capture** to **high-resolution evidence capture**.

The purpose is not to reproduce every upstream message. It is to answer four specific questions:

1. What did Binance futures price do?
2. When did Binance say it happened?
3. When did our collector receive it?
4. When did the corresponding Chainlink movement occur?

For that purpose, the completed Sequence 1 runtime scope needs:

* A capped **100 ms Binance futures price trace** derived from `aggTrade`
* Every valid **Chainlink RTDS price tick** offered to bounded capture without
  intentional sampling
* Tiny connection/session records so outages and reconnects are distinguishable from genuine lag

It does **not** need raw `bookTicker`, full trade payloads, Polymarket order-book depth, raw JSON, or permanent storage.

The first implementation checkpoint is deliberately smaller than that runtime
scope. Phase 1 adds only schema, validated configuration, and inactive capture
primitives. Both feature flags default to `False`, and no collector imports,
constructs, or invokes the new capture path in that phase. Live values,
one-second history, WebSocket timing, and API behavior therefore remain
unchanged until later phases.

---

## 1. Exact capture scope

### Sequence 1 runtime capture scope

| Dataset                | Source             |                         Stored resolution | Why it is required                                  |
| ---------------------- | ------------------ | ----------------------------------------: | --------------------------------------------------- |
| Futures price trace    | Binance `aggTrade` | One row per connection and active 100 ms bucket | Leading price signal                         |
| Chainlink price events | Polymarket RTDS    | Every valid update offered; overload loss counted | Prediction target                         |
| Feed sessions          | Both WebSockets    |                    One row per connection | Detect reconnects, outages, gaps and clock changes  |
| Existing aggregates    | Current tables     |                  Existing one-second rows | Flow, book and probability context                  |

Phase 1 creates the storage and inactive primitives for this scope but captures
none of these new rows.

### Do not capture now

| Excluded data                                          | Reason                                                                   |
| ------------------------------------------------------ | ------------------------------------------------------------------------ |
| Every `bookTicker` event                               | Potentially high volume; not needed to prove the futures-price lag       |
| Full `aggTrade` payloads                               | Quantity and maker direction already feed the one-second flow aggregates |
| Raw JSON or exact wire messages                        | Large, duplicated and not used by the proposed model                     |
| Binance spot raw events                                | The current hypothesis is specifically futures → Chainlink               |
| Full Polymarket CLOB books                             | Needed for executable-edge analysis, not Chainlink prediction            |
| Every CLOB `price_change` event                        | Same reason; defer until the predictor itself is validated               |
| Ping, pong and malformed payload bodies                | Keep counters only                                                       |
| Mark, index and open interest at millisecond frequency | Existing polling and historical tables are sufficient                    |
| Redis Streams or other persistent memory queues        | Raw history belongs on bounded disk storage, not in Redis memory         |

The current CLOB code already reduces `book`, `price_change`, and `best_bid_ask` messages into best-bid/best-ask state and writes a one-second probability snapshot. That is enough for the current stage.  

---

# 2. Futures capture: 100 ms price trace

Do **not** insert every `aggTrade` into PostgreSQL.

Instead, process every event in memory and produce one compact OHLC row for
each active 100 ms receive-time bucket and WebSocket connection. Keeping the
connection boundary prevents a reconnect from being hidden by merging two
sessions into one row.

### Proposed fields

`raw_capture.binance_futures_price_trace_100ms`

| Column                        | Purpose                                          |
| ----------------------------- | ------------------------------------------------ |
| `bucket_start_ms`             | Collector receive-time bucket, aligned to 100 ms |
| `connection_id`               | Changes on every WebSocket reconnect             |
| `first_received_wall_ns`      | Absolute time of first event in the bucket       |
| `last_received_wall_ns`       | Absolute time of last event                      |
| `first_received_monotonic_ns` | Clock-jump-resistant local ordering              |
| `last_received_monotonic_ns`  | Same, for the final event                        |
| `first_trade_time_ms`         | First Binance `T` in the bucket                  |
| `last_trade_time_ms`          | Last Binance `T`                                 |
| `first_event_time_ms`         | First Binance `E`                                |
| `last_event_time_ms`          | Last Binance `E`                                 |
| `open_price`                  | First trade price                                |
| `high_price`                  | Highest trade price                              |
| `low_price`                   | Lowest trade price                               |
| `close_price`                 | Final trade price                                |
| `event_count`                 | Number of `aggTrade` messages represented        |
| `first_agg_trade_id`          | Gap detection                                    |
| `last_agg_trade_id`           | Gap detection                                    |

Do not store:

* Quantity
* Buyer-maker direction
* First and last underlying trade IDs
* Quote notional
* Symbol
* Venue
* Raw JSON

Those values either already contribute to `binance_flow_1s` or are not required for the Chainlink price forecast.

### Why 100 ms is a good initial resolution

A 3–4 second response window contains 30–40 observations at 100 ms resolution.
In steady state, a single connection is capped at:

[
10\ rows/second \times 86{,}400 = 864{,}000\ rows/day
]

For 72 hours, that steady-state rate is approximately **2.59 million futures
rows**. Quiet periods produce fewer rows because empty buckets are not
inserted. A reconnect within a 100 ms bucket can legitimately create one row
for each connection, so 10 rows per second is not a strict global bound.

OHLC preserves a jump and retracement that both occur inside one bucket. A simple last-price-only sample would lose that information.

Phase 1 fixes and validates the bucket width at 100 ms. If later testing
demonstrates material forecast error at that resolution, a 50 ms experiment
must use an explicit schema/version change rather than silently writing a new
resolution into a table named `_100ms`. Full event persistence should not
become the default without evidence that it improves out-of-sample results.

---

# 3. Chainlink capture: offer every valid tick

Chainlink is the target, so its individual events should not be aggregated before short-term storage.

### Proposed fields

`raw_capture.chainlink_price_events`

| Column                  | Purpose                                           |
| ----------------------- | ------------------------------------------------- |
| `received_wall_ns`      | When the collector finished receiving the message |
| `received_monotonic_ns` | Stable local ordering                             |
| `connection_id`         | Identifies the RTDS WebSocket session             |
| `receive_sequence`      | Monotonic counter within the connection           |
| `provider_event_ms`     | RTDS `payload.timestamp`                          |
| `provider_message_ms`   | Optional top-level RTDS message timestamp         |
| `price`                 | RTDS `payload.value`                              |

Do not store topic, symbol or event type because every row in this source-specific table has the same values.

Two successfully captured Chainlink messages received in the same millisecond
remain two rows. `connection_id + receive_sequence` preserves their order
without requiring a heavy unique index.

The reader does not intentionally sample or coalesce valid Chainlink ticks, but
the bounded best-effort queue cannot promise lossless storage during database
failure or sustained overload. Every drop must be counted and must mark that
interval as unsuitable for model training.

Unchanged-price Chainlink ticks should still be retained. They help determine:

* Normal publication cadence
* Whether a lack of movement was a real unchanged update
* Whether the RTDS feed paused
* Provider-time versus delivery-time lag

---

# 4. Capture receive time before parsing

This is a required correction.

The futures loops currently:

1. Await `websocket.recv()`
2. Parse JSON
3. Validate the event
4. Only then record `received_ms`

That means JSON parsing and validation time are included in the apparent
delivery latency.

The Chainlink reader similarly parses the RTDS message and only assigns receive time later inside `handle_tick`. It also awaits Redis and PostgreSQL work before returning to receive the next message.  

The new order should be:

```python
raw_message = await websocket.recv()

received_wall_ns = time.time_ns()
received_monotonic_ns = time.monotonic_ns()
receive_sequence += 1

# Parsing happens after the clocks are captured.
event = parse_message(raw_message)

latest_state.update(event, received_wall_ns)
capture.offer_nowait(event)
```

This timestamp is the collector application's observation immediately after
`recv()` returns. It excludes the application's JSON parsing time, but it is
not a kernel or wire-arrival timestamp, and nanosecond representation does not
imply nanosecond clock accuracy. `time.monotonic_ns()` can expose local wall
clock jumps within a process; it cannot prove that the host's absolute clock
offset is correct.

In the later integration phases, valid events are offered to capture. Invalid
messages increment counters but their raw body is discarded. Phase 1 does not
change either reader loop.

## Phase 3 Chainlink refactor

Phase 3 makes the Chainlink receive loop stop awaiting both Redis and PostgreSQL
for every tick. Otherwise, a temporary downstream delay can delay the next
`recv()` and falsely look like RTDS latency.

Use this flow:

```text
RTDS recv
   │
   ├─ capture receive clocks immediately
   ├─ parse and validate
   ├─ synchronously publish versioned latest-live state
   ├─ offer versioned provider-second history
   ├─ offer event to bounded raw-capture queue
   └─ return immediately to RTDS receive
```

A separate latest-wins live worker attempts Redis for the newest published
version. A separate historical worker waits for the corresponding live attempt,
then writes the latest-received pending sample for each provider second. The
newest provider second settles 1,000 ms after its latest local receipt unless a
newer provider second makes it ready sooner. Versions prevent an update arriving
during an in-flight operation from being mistakenly acknowledged as persisted.
Intermediate live versions may be coalesced, but the newest remains pending
until its live attempt completes. The historical pending store is bounded at
5,000 provider seconds, reports overflow and write failures explicitly, and
performs a bounded shutdown drain.

Pending same-second versions are coalesced, which reduces repeated normal
updates to the same `price_samples` row while preserving the latest-received
value. It does not eliminate every repeated upsert: a later corrective tick can
update an already-written provider second, and a version arriving during an
in-flight write remains pending for a follow-up upsert.

The critical Redis and one-second historical path must not share the
best-effort raw-capture buffer. A raw-capture drop is allowed; silently losing
the newest live version or a historical second because that raw buffer filled
is not.

---

# 5. The raw writer must be incapable of blocking collection

Add a shared module:

```text
price_collector/raw_capture.py
```

It should contain:

* Compact `@dataclass(slots=True)` record types
* The futures 100 ms coalescer
* A bounded asynchronous buffer
* A batch PostgreSQL writer
* Capture counters
* Partition-retention logic

## Queue policy

Recommended starting values:

```text
RAW_CAPTURE_QUEUE_MAX_EVENTS=5000
RAW_CAPTURE_BATCH_MAX_ROWS=500
RAW_CAPTURE_FLUSH_MS=1000
```

The reader calls `offer_nowait()`. It must never execute:

```python
await raw_queue.put(...)
```

When the buffer is full:

1. Drop the oldest pending capture record.
2. Preserve the newest data.
3. Increment `records_dropped_total`.
4. Emit a rate-limited warning.

The live price state, existing flow calculation and WebSocket receive loop continue normally.

This makes memory usage deterministic. The queue cannot grow indefinitely, even if PostgreSQL becomes unavailable.

## Database failure policy

Raw capture is useful but non-critical. Therefore:

* Do not pause the WebSocket reader.
* Do not retry the same batch forever.
* Do not create an unbounded local disk spool.
* Do not retry the same `COPY` batch. A connection failure after PostgreSQL
  commits but before the acknowledgement is ambiguous, and retrying an
  append-only batch without a unique index can create duplicates.
* Discard the reported failed batch, increment a failure counter, and reacquire
  a connection for later batches.
* Keep the existing live and one-second collectors operating.

---

# 6. Batch with `COPY`, not per-row inserts

The current database functions generally acquire a connection, open a transaction and execute one insert/upsert at a time. The general pool currently allows up to five connections.  

The high-resolution writer should not reuse that pattern.

Use:

```python
connection.copy_records_to_table(...)
```

or equivalent PostgreSQL binary `COPY`, with:

* One dedicated raw-capture connection
* At most one flush per second under ordinary volume
* One implicit atomic operation per `COPY` batch, with no transaction per row
* No `ON CONFLICT`
* No per-row market-window lookup
* No `_ensure_market_window` call
* No per-event log message

A separate one-connection pool prevents the capture writer from taking all connections used by the existing historical pipeline. It does not eliminate all database contention, so batch size and storage volume still need monitoring.

---

# 7. Use narrow source-specific tables

I would not use the previously suggested generic `raw_market_events` table.

A generic table would require:

* A source column on every row
* An event-type column on every row
* Numerous nullable price/book/trade fields
* Repeated symbol and token identifiers
* More complicated indexes
* Potential JSON storage

Two narrow event tables are smaller, easier to query and harder to misuse.
They and the small session table live in a dedicated `raw_capture` PostgreSQL
schema. The API reader role receives no access to that schema.

## Table design rules

Both raw tables should be:

* Append-only
* Partitioned by local receive time
* Free of JSONB
* Free of foreign keys
* Free of a serial `raw_event_id`
* Free of `created_at`, since receive time already exists
* Free of large B-tree indexes
* Queried only by internal analysis jobs

Do not add a raw-event index in Phase 1. Six-hour partition pruning may already
be sufficient for the short retention window. Add a small BRIN index on receive
time only if canary query measurements show that it is beneficial.

`raw_capture.feed_sessions` is required, not optional. It stores one small row
per futures or Chainlink WebSocket connection, using an application-generated
UUID also present on captured event rows. It records connection/ready and close
timing plus final counters. The event tables deliberately have no foreign key
to it, so a failed session-metadata write cannot reject an evidence batch.

Do not associate every row with `market_windows` during insertion. A market ID can be derived later:

[
market_id=\left\lfloor\frac{provider_event_ms}{300{,}000}\right\rfloor
]

Avoiding a foreign key and market-window write keeps the capture path independent from the main schema.

---

# 8. Retention and relation-size budget

Use native PostgreSQL range partitions covering **six hours** each.

Six-hour partitions give:

* Four partitions per day
* Fast whole-partition deletion
* Reasonably precise retention
* No large row-by-row `DELETE`
* No prolonged vacuum caused by retention cleanup

## Initial policy

```text
RAW_CAPTURE_RETENTION_HOURS=72
RAW_CAPTURE_MAX_RELATION_MB=2048
RAW_CAPTURE_RETENTION_CHECK_SECONDS=60
```

Delete the oldest complete partition whenever either condition is true:

1. It is older than 72 hours.
2. The combined raw-capture leaf relations exceed the configured relation
   budget.

The age and size limits are both required. Age protects against permanent
accumulation; the relation budget bounds the intended capture relations during
an unexpected increase in event volume.

This is not a hard filesystem limit. PostgreSQL WAL, temporary files, other
tables, and filesystem overhead are outside the relation-size calculation.
The suggested 2 GB budget must be reviewed against the droplet's real free
space, and operations must continue monitoring `df -h` and total PostgreSQL
size.

### Partition management

* Pre-create the current and next partition interval for both event tables.
* Check retention and relation size every 60 seconds.
* Drop matching six-hour futures and Chainlink partitions as a pair so the
  retained sources keep the same time coverage.
* Under relation pressure, keep dropping the oldest completed pair until the
  budget is met. Never drop the current interval; suspend raw capture and count
  drops if no completed interval remains.
* Sum leaf-relation sizes, rather than measuring only the partitioned parents.
* Use a PostgreSQL advisory lock so only one collector performs maintenance.
* If a partition is missing, raw capture may drop a batch and alert; it must not block the reader while creating schema.
* Do not include these tables in long-term backups or exports unless specifically needed.

Partition DDL is isolated from the main `public` schema. The `raw_capture`
schema remains owned by `postgres`, access is revoked from `PUBLIC`, and the
API reader receives no schema access. `price_writer` receives `USAGE` and
`CREATE` only within `raw_capture` and owns only the two raw event parents and
the session table. This allows its maintenance worker to create and drop their
partitions without granting DDL rights in `public`. All runtime SQL uses fixed,
fully qualified raw object names and the advisory lock.

Once Sequence 2 produces compact model-evaluation rows, reduce raw retention from 72 to **48 hours**. Keep the compact lag/prediction observations for a longer period, such as 90 days.

---

# 9. Keep the live predictor out of PostgreSQL

The eventual dashboard predictor must not query the raw tables.

The live calculation should use a fixed-size in-memory structure, for example:

```text
Futures trace: most recent 10 seconds, max 100–200 entries
Chainlink ticks: most recent 10 seconds, bounded deque
```

PostgreSQL raw capture is only for:

* Offline lag measurement
* Model calibration
* Forecast-error analysis
* Debugging feed timing
* Detecting data-quality problems

This prevents analytical storage from becoming part of the latency-sensitive dashboard path.

---

# 10. Polymarket CLOB capture is a later checkpoint

Do not add CLOB event persistence in this implementation.

When the goal changes from:

> “Can futures predict Chainlink?”

to:

> “Was there an executable Polymarket price after the futures signal?”

add a separate, minimal table:

```text
polymarket_bbo_events
```

That later table should store only:

* Market ID
* Up/Down side or compact token identifier
* Best bid
* Best ask
* Receive timestamp
* Provider timestamp
* Connection ID

Insert only when the best bid or best ask changes. Do not persist full book depth or raw `book` messages.

The existing CLOB reader already captures local receive time immediately after `recv()` and before JSON parsing, which is the correct placement for that later work. 

---

# 11. Configuration

Add only these settings initially:

```python
RAW_FUTURES_TRACE_ENABLED: bool = False
RAW_CHAINLINK_EVENTS_ENABLED: bool = False

RAW_FUTURES_BUCKET_MS: int = 100

RAW_CAPTURE_QUEUE_MAX_EVENTS: int = 5_000
RAW_CAPTURE_BATCH_MAX_ROWS: int = 500
RAW_CAPTURE_FLUSH_MS: int = 1_000

RAW_CAPTURE_RETENTION_HOURS: int = 72
RAW_CAPTURE_MAX_RELATION_MB: int = 2_048
RAW_CAPTURE_RETENTION_CHECK_SECONDS: int = 60
```

Validate every numeric setting as positive, require
`RAW_CAPTURE_BATCH_MAX_ROWS <= RAW_CAPTURE_QUEUE_MAX_EVENTS`, and reject any
`RAW_FUTURES_BUCKET_MS` value other than `100` in this schema version. The two
feature flags remain `False` in code and in the production environment example.

Do not overload `BINANCE_FUTURES_STORE_RAW_JSON`. That existing setting concerns JSON attached to existing futures snapshots, not the new high-resolution price trace. The current configuration contains no bounded raw-capture or retention settings. 

There should be no CLOB raw-capture setting in this checkpoint because the feature is intentionally excluded.

---

# 12. File-change plan

## Phase 1 required

| File                               | Change                                                                                              |
| ---------------------------------- | --------------------------------------------------------------------------------------------------- |
| `price_collector/raw_capture.py`   | Inactive record types, bounded buffer, futures coalescer, counters, batch writer, and maintenance   |
| `db.py`                            | Dedicated one-connection raw pool plus batch `COPY` and restricted maintenance calls               |
| `schema.sql`                       | Isolated `raw_capture` schema, two narrow partitioned event tables, required session table, partitions, and restricted ownership/grants |
| `config.py`                        | Disabled and validated settings listed above                                                       |
| `deployment/collector.env.example` | Document all settings with both flags `false`                                                       |
| `README.md`                        | Document the inactive foundation without claiming capture is active                                |
| `OPERATIONS.md`                    | Manual environment review, Phase 1 verification, relation/disk monitoring, and rollback            |

## Tests

* New `tests/test_raw_capture.py`
* `tests/test_db.py`
* `tests/test_config.py`
* `tests/test_deployment.py`

## Intentionally unchanged

* `polymarket_probability_collector.py`
* `binance_futures_streams.py`
* `binance_futures_collector.py`
* `polymarket_chainlink_collector.py`
* API response schema
* Frontend
* `live_cache.py`
* `LIVE_DATA.md`, because Phase 1 changes no live source or timing behavior
* Full `bookTicker` persistence
* CLOB raw persistence
* Existing one-second flow and book tables

Phase 2 integrates the futures reader and adds its focused stream/collector
tests. The repository and environment-example flag remains `False`; integration
being merged and deployed safely is the Phase 2 code checkpoint, not completion
of its required 24-hour production canary. Phase 3 integrates and refactors the
Chainlink reader and adds its focused tests. Deferring those files is what makes
Phase 1 observably inactive.

---

# 13. Separate capture deployment from futures-price cutover

The live futures value currently comes from `/fapi/v2/ticker/price`; the existing `aggTrade` and `bookTicker` WebSockets do not update the futures Redis value. 

Do not change the live source in the same rollout that first enables raw capture.

Use two checkpoints:

### Checkpoint A: shadow validation

* Keep REST as the public `futures.last`.
* Update an in-memory latest WS trade state.
* Record REST-versus-WS differences, source age and gaps.
* Run for at least a full day.
* Confirm reconnect and staleness behavior.

### Checkpoint B: WS cutover

* Use `aggTrade.p` as `futures.last`.
* Existing one-second snapshots read the latest validated WS trade.
* Remove `/fapi/v2/ticker/price`.
* Keep REST premium/index price and open-interest calls.
* Do not call book midpoint or microprice “last.”

This keeps any capture bug separate from a user-visible price-source change.

---

# 14. Rollout sequence

## Phase 1 — Schema and inactive code

1. Add the private `raw_capture` schema, two event parents, required session
   table, six-hour partitions, and narrowly scoped `price_writer` ownership and
   grants.
2. Add and validate configuration with both capture flags `False` and the
   futures bucket fixed at 100 ms.
3. Add record types, coalescer, bounded buffer, counters, dedicated writer,
   batch `COPY`, and partition-maintenance primitives without importing them
   from a collector.
4. Verify that applying the schema succeeds idempotently and that `price_reader`
   cannot access `raw_capture`.
5. Verify that disabled capture creates no queue, extra database pool/task, or
   raw rows. Empty pre-created partitions are expected and are not capture
   activity.

## Phase 2 — Futures-only canary

### Phase 2 code checkpoint

1. Integrate the futures `aggTrade` reader with the session tracker, 100 ms
   coalescer, non-blocking capture sink, and 60-second structured telemetry.
2. Capture wall and monotonic receive clocks immediately after `recv()` and
   before JSON parsing while preserving the existing one-second flow path.
3. Keep Binance REST as the public `futures.last`; do not update the Redis live
   value from the WebSocket in this checkpoint.
4. Keep `RAW_FUTURES_TRACE_ENABLED=False` in code and in
   `deployment/collector.env.example`. Keep Chainlink capture unintegrated and
   `RAW_CHAINLINK_EVENTS_ENABLED=False`.
5. Add focused reader, connection lifecycle, disabled-path, failure-isolation,
   telemetry, and shutdown tests. Deploy this checkpoint with both flags still
   false before starting the production canary.

### Phase 2 operational completion

1. Confirm `BINANCE_FUTURES_STREAMS_ENABLED=true`, then manually enable only
   `RAW_FUTURES_TRACE_ENABLED` in the existing production environment file.
   Keep `RAW_CHAINLINK_EVENTS_ENABLED=false`.
2. Restart only `price-collector-binance-futures` and run continuously for 24
   hours. An unexpected collector-process restart resets the canary window; a
   normal Binance WebSocket reconnect does not.
3. Measure actual rows/hour, bytes/hour, queue high-water, batch/database
   latency, collector CPU and memory, total database size, and filesystem free
   space.
4. Require zero capture drops, zero failed batches, no storage suspension, no
   unexplained parse errors, and a queue that repeatedly returns near zero
   without reaching capacity.
5. Confirm at most one row exists per connection and active 100 ms bucket, and
   no connection produces more than ten buckets per second. Reconnect
   boundaries may make the global rate briefly exceed ten rows per second.
6. Confirm every trace connection has a ready futures session row, completed
   connections have a close reason, and no more than the current connection
   remains open. Treat runtime-wide drops as authoritative if a final session
   update itself was lost.
7. Confirm Chainlink raw rows and sessions remain zero, the dedicated raw pool
   uses no more than one connection, and the REST-backed Redis/API futures live
   value remains healthy.
8. Compare the canary's one-second flow and book coverage and gaps with the
   immediately preceding 24-hour baseline; investigate any degradation.
9. Declare Phase 2 complete only after all checks pass. On failure, set
   `RAW_FUTURES_TRACE_ENABLED=false`, restart
   `price-collector-binance-futures`, retain the populated raw schema, and
   confirm capture rows stop increasing.

## Phase 3 — Chainlink capture

### Phase 3 code checkpoint

1. Refactor the RTDS reader to stamp receive time before parsing, synchronously
   update versioned delivery state, offer critical provider-second history
   before best-effort raw evidence, and await neither Redis nor PostgreSQL.
2. Add independent live and historical workers. The live worker is latest-wins;
   its newest version remains pending until an attempt completes. The historical
   worker waits for that live attempt, holds the newest provider second for
   1,000 ms unless a newer second makes it ready, coalesces pending versions,
   retains later corrective and concurrent updates, caps pending history at
   5,000 provider seconds, exposes overflow/failure counters, and performs a
   bounded shutdown drain.
3. Integrate `FeedSession`, individual `ChainlinkPriceEvent` records, the
   non-blocking capture sink, and a 60-second `raw_capture_summary` containing
   both raw and delivery health plus signed provider/local timing fields.
4. Keep `RAW_CHAINLINK_EVENTS_ENABLED=False` in code and
   `deployment/collector.env.example`. Deploy and verify the refactored normal
   Redis and one-second paths with Chainlink raw capture still disabled.
5. Add focused timing, same-millisecond ordering, reconnect/session,
   latest-wins, same-provider-second, in-flight update, bounded-overflow,
   failure-isolation, disabled-path, telemetry, and shutdown tests.

### Phase 3 operational completion

1. Do not begin until the uninterrupted futures-only Phase 2 canary has passed
   all 24-hour acceptance checks. Enabling Chainlink sooner invalidates that
   isolation window.
2. Keep `BINANCE_FUTURES_STREAMS_ENABLED=true` and
   `RAW_FUTURES_TRACE_ENABLED=true`, manually set only
   `RAW_CHAINLINK_EVENTS_ENABLED=true`, and restart only
   `price-collector-polymarket-chainlink` so the futures service is not reset.
3. Run Chainlink capture continuously for 24 hours. Require zero raw drops,
   failed batches, storage suspension, and unexplained parse errors; require a
   queue that repeatedly returns near zero and never reaches capacity.
4. Confirm successfully persisted ticks have unique connection/sequence pairs,
   monotonic receive order, and distinct rows when two ticks share a local
   millisecond. Sequence gaps caused by counted RTDS control or malformed frames
   are legitimate and must be reconciled rather than treated as raw loss.
5. Audit session readiness and closure, compare provider timestamps, local wall
   delivery differences, monotonic arrival cadence, and optional message time
   independently, and verify host clock synchronization before interpreting
   signed wall-clock lag.
6. Confirm settled captured provider seconds reach normal `price_samples`, the
   latest Redis/API Chainlink value stays healthy, delivery overflow and worker
   failure counters remain zero, and historical coverage does not regress.
7. Confirm futures evidence and service uptime remain unaffected, relation and
   filesystem growth are acceptable, and the two enabled collectors together
   use no more than two dedicated raw database connections.
8. On a raw-only failure, disable only Chainlink raw capture and restart only
   its service. On a delivery-refactor failure, deploy a code revert as well;
   the raw feature flag does not disable the new live and historical workers.

## Phase 4 — Validate retention

1. Create deliberately expired test partitions.
2. Run maintenance.
3. Confirm partitions are dropped, not row-deleted.
4. Confirm the relation-size budget drops the oldest completed futures and
   Chainlink partition pair.
5. Confirm normal collection continues if retention fails.
6. Confirm `df -h` and total database-size monitoring remain in place because
   the relation budget does not bound PostgreSQL WAL or filesystem usage.

## Phase 5 — Futures WS source cutover

Only after the capture data shows that WS state is stable and complete, replace the REST ticker price with `aggTrade.p`.

## Phase 6 — Produce the compact modeling dataset

Before raw rows expire, Sequence 2 should create compact lag observations and forecast outcomes. At that point, raw retention can be reduced to 48 hours.

---

# 15. Required operational counters

Log one structured summary per source every 60 seconds:

```text
messages_received_total
messages_accepted_total
parse_errors_total
records_coalesced_total
records_enqueued_total
records_persisted_total
records_dropped_total
batches_failed_total
queue_depth
queue_high_water
last_batch_rows
last_batch_duration_ms
current_partition
raw_table_bytes
connection_id
```

The Phase 3 Chainlink summary also reports its versioned delivery state and
signed timing diagnostics:

```text
delivery_sequence
delivery_live_attempted_sequence
delivery_live_attempts_total
delivery_live_successes_total
delivery_live_failures_total
delivery_history_collapsed_total
delivery_history_persisted_total
delivery_history_failures_total
delivery_history_pending_dropped_total
delivery_history_pending_seconds
delivery_history_pending_high_water
delivery_last_live_attempt_ms
delivery_last_history_write_ms
chainlink_connections_opened_total
chainlink_reconnects_total
chainlink_latest_price
chainlink_provider_event_ms
chainlink_provider_message_ms
chainlink_received_ms
chainlink_latest_receive_sequence
chainlink_latest_connection_id
chainlink_provider_event_to_receive_ms
chainlink_provider_message_to_receive_ms
chainlink_provider_message_minus_event_ms
chainlink_provider_event_age_ms
chainlink_received_age_ms
chainlink_raw_interarrival_ns
chainlink_raw_max_interarrival_ns
```

Do not log every raw event.

A dropped-event count of zero is expected under normal conditions. A nonzero count should be visible and should mark that interval as unsuitable for model training, but it should never stop the live collector.

---

# Definition of done

Phase 1 is complete when all of these are true:

* The private `raw_capture` schema contains the two narrow partitioned event
  parents, the required session table, and current/next six-hour partitions.
* `PUBLIC` and `price_reader` cannot access `raw_capture`; `price_writer` has
  only the ownership and schema-local DDL permissions required for raw
  partitions.
* Both capture flags default to `False`, the bucket setting accepts only 100,
  and the remaining numeric settings are validated.
* Record, coalescer, bounded-buffer, batch-writer, counters, and maintenance
  primitives have focused tests, including full-buffer and failed-`COPY`
  behavior.
* A failed `COPY` batch is counted and discarded without an ambiguous
  same-batch retry.
* No collector imports or constructs the capture primitives.
* Disabled capture creates no queue, extra pool/task, or raw rows. Live Redis,
  existing one-second PostgreSQL data, and API behavior are unchanged.
* Operations documentation explains manual environment review, relation and
  filesystem monitoring, verification, and rollback.

Sequence 1 is complete when all of these are true:

* Receive clocks are captured immediately after `recv()` and before JSON parsing.
* The futures trace is capped at one row per connection and active 100 ms bucket.
* Every valid Chainlink tick is offered without intentional sampling;
  successfully captured ticks remain individually ordered and drops are
  counted.
* No raw JSON or full book depth is persisted.
* The receive loops never await the raw database writer.
* The Chainlink receive loop awaits neither Redis nor PostgreSQL; its latest-live
  and provider-second states are versioned, bounded, and independently worked.
* Delivery overflow and worker failures are explicit counters rather than silent
  loss, and the final pending state receives a bounded shutdown drain.
* Queue memory has a fixed upper bound.
* PostgreSQL uses batched append-only `COPY`.
* Raw tables have an age limit and a relation-size budget, while filesystem
  monitoring remains separate.
* Expired data is removed by dropping partitions.
* Raw-capture failure cannot interrupt live values or existing one-second data.
* The dashboard and predictor do not query raw tables.
* CLOB BBO capture remains deferred until an executable-edge requirement exists.
* REST-to-WebSocket futures cutover is deployed separately from capture.

The first implementation checkpoint therefore contains only the inactive
schema, configuration, and tested primitives. Futures integration, Chainlink
integration/refactoring, live-source cutover, and model production remain
separate later phases.
