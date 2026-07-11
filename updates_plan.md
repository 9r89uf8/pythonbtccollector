# Sequence 1 plan: minimal high-resolution capture

I would narrow the name from **raw event capture** to **high-resolution evidence capture**.

The purpose is not to reproduce every upstream message. It is to answer four specific questions:

1. What did Binance futures price do?
2. When did Binance say it happened?
3. When did our collector receive it?
4. When did the corresponding Chainlink movement occur?

For that purpose, the first checkpoint only needs:

* A capped **100 ms Binance futures price trace** derived from `aggTrade`
* Every accepted **Chainlink RTDS price tick**
* Tiny connection/session records so outages and reconnects are distinguishable from genuine lag

It does **not** need raw `bookTicker`, full trade payloads, Polymarket order-book depth, raw JSON, or permanent storage.

---

## 1. Exact capture scope

### Capture now

| Dataset                | Source             |          Stored resolution | Why it is required                                  |
| ---------------------- | ------------------ | -------------------------: | --------------------------------------------------- |
| Futures price trace    | Binance `aggTrade` | At most one row per 100 ms | Leading price signal                                |
| Chainlink price events | Polymarket RTDS    |      Every accepted update | Prediction target                                   |
| Feed sessions          | Both WebSockets    |     One row per connection | Detect reconnects, outages, gaps and clock problems |
| Existing aggregates    | Current tables     |   Existing one-second rows | Flow, book and probability context                  |

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

Instead, process every event in memory and produce one compact OHLC row for each active 100 ms receive-time bucket.

### Proposed fields

`binance_futures_price_trace_100ms`

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

A 3–4 second response window contains 30–40 observations at 100 ms resolution. That is enough to estimate the response shape while placing a hard upper bound on storage:

[
10\ rows/second \times 86{,}400 = 864{,}000\ rows/day
]

For 72 hours, the absolute maximum is approximately **2.59 million futures rows**. Quiet periods will produce fewer rows because empty buckets should not be inserted.

OHLC preserves a jump and retracement that both occur inside one bucket. A simple last-price-only sample would lose that information.

If later testing demonstrates that 100 ms causes material forecast error, the bucket can temporarily be changed to 50 ms. Full event persistence should not become the default without evidence that it improves out-of-sample results.

---

# 3. Chainlink capture: preserve every accepted tick

Chainlink is the target, so its individual events should not be aggregated before short-term storage.

### Proposed fields

`chainlink_price_events`

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

Two Chainlink messages received in the same millisecond must remain two rows. `connection_id + receive_sequence` preserves their order without requiring a heavy unique index.

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

That means JSON parsing and validation time are included in the apparent network latency. 

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

Only accepted events are stored. Invalid messages increment counters but their raw body is discarded.

## Important Chainlink refactor

The Chainlink receive loop should no longer await a PostgreSQL upsert for every tick. Otherwise, a temporary database delay can delay the next `recv()` and falsely look like RTDS latency.

Use this flow:

```text
RTDS recv
   │
   ├─ capture receive clocks immediately
   ├─ parse and validate
   ├─ update in-memory latest Chainlink state
   ├─ offer event to bounded raw-capture queue
   └─ notify latest-value/historical workers
```

A separate worker should:

* Publish the latest Chainlink value to Redis
* Collapse events to the existing one-second historical representation
* Write the one-second row outside the receive loop

This also eliminates repeated updates to the same `price_samples` row when several Chainlink messages share one source second. The current historical function performs a transaction and an upsert for each call. 

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
3. Increment `capture_dropped_total`.
4. Emit a rate-limited warning.

The live price state, existing flow calculation and WebSocket receive loop continue normally.

This makes memory usage deterministic. The queue cannot grow indefinitely, even if PostgreSQL becomes unavailable.

## Database failure policy

Raw capture is useful but non-critical. Therefore:

* Do not pause the WebSocket reader.
* Do not retry the same batch forever.
* Do not create an unbounded local disk spool.
* Attempt one short retry.
* If it still fails, discard that raw batch and increment a failure counter.
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
* No transaction per row
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

Two narrow tables are smaller, easier to query and harder to misuse.

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

Use one small BRIN index on receive time if testing shows it is beneficial. Partition pruning may already be sufficient for short retention windows.

Do not associate every row with `market_windows` during insertion. A market ID can be derived later:

[
market_id=\left\lfloor\frac{provider_event_ms}{300{,}000}\right\rfloor
]

Avoiding a foreign key and market-window write keeps the capture path independent from the main schema.

---

# 8. Retention and hard storage limits

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
RAW_CAPTURE_MAX_DISK_MB=2048
RAW_CAPTURE_RETENTION_CHECK_SECONDS=3600
```

Delete the oldest complete partition whenever either condition is true:

1. It is older than 72 hours.
2. The combined raw-capture tables exceed the configured disk budget.

The age and size limits are both required. Age protects against permanent accumulation; the disk limit protects against an unexpected increase in event volume.

The suggested 2 GB limit should be adjusted to the droplet’s actual free-disk budget, but it should be explicit rather than inferred.

### Partition management

* Pre-create the current and next partition.
* Run retention once per hour.
* Use a PostgreSQL advisory lock so only one collector performs maintenance.
* If a partition is missing, raw capture may drop a batch and alert; it must not block the reader while creating schema.
* Do not include these tables in long-term backups or exports unless specifically needed.

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
RAW_CAPTURE_MAX_DISK_MB: int = 2_048
RAW_CAPTURE_RETENTION_CHECK_SECONDS: int = 3_600
```

Do not overload `BINANCE_FUTURES_STORE_RAW_JSON`. That existing setting concerns JSON attached to existing futures snapshots, not the new high-resolution price trace. The current configuration contains no bounded raw-capture or retention settings. 

There should be no CLOB raw-capture setting in this checkpoint because the feature is intentionally excluded.

---

# 12. File-change plan

## Required

| File                                | Change                                                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `price_collector/raw_capture.py`    | New bounded writer, futures coalescer, records, counters and retention                                        |
| `binance_futures_streams.py`        | Capture clocks before parsing; connection IDs; latest WS trade state; send compact events to 100 ms coalescer |
| `binance_futures_collector.py`      | Construct capture components and later consume the latest validated WS trade                                  |
| `polymarket_chainlink_collector.py` | Capture clocks before parsing; fast latest state; raw offer; move Redis/Postgres work out of reader           |
| `db.py`                             | Dedicated raw pool, batch `COPY`, partition creation and retention functions                                  |
| `schema.sql`                        | Two narrow partitioned tables and optional tiny session table                                                 |
| `config.py`                         | Settings listed above                                                                                         |
| `deployment/collector.env.example`  | Document settings                                                                                             |
| `LIVE_DATA.md`                      | Explain timing and WS futures source rollout                                                                  |
| `OPERATIONS.md`                     | Disk growth, drop counters, partition retention and rollback                                                  |

## Tests

* New `tests/test_raw_capture.py`
* `tests/test_binance_futures_streams.py`
* `tests/test_binance_futures_collector.py`
* `tests/test_polymarket_chainlink_collector.py`
* `tests/test_db.py`
* `tests/test_config.py`
* `tests/test_deployment.py`

## Intentionally unchanged

* `polymarket_probability_collector.py`
* API response schema
* Frontend
* `live_cache.py`, until the separate futures source cutover
* Full `bookTicker` persistence
* CLOB raw persistence
* Existing one-second flow and book tables

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

1. Add schema, partitions and config with both capture flags `False`.
2. Add the writer and coalescer.
3. Verify that disabled capture produces no queue, connection or storage overhead.

## Phase 2 — Futures-only canary

1. Enable `RAW_FUTURES_TRACE_ENABLED`.
2. Run for 24 hours.
3. Measure actual rows/hour, bytes/hour, queue high-water, database latency and collector CPU.
4. Confirm the row rate never exceeds 10 per second with a 100 ms bucket.
5. Confirm existing one-second flow and book completeness is unchanged.

## Phase 3 — Chainlink capture

1. Refactor the RTDS reader so it does not await PostgreSQL.
2. Enable `RAW_CHAINLINK_EVENTS_ENABLED`.
3. Confirm all accepted ticks retain receive order, including two events sharing a millisecond.
4. Compare provider-time and local-receive-time lag independently.

## Phase 4 — Validate retention

1. Create deliberately expired test partitions.
2. Run maintenance.
3. Confirm partitions are dropped, not row-deleted.
4. Confirm the hard disk cap also drops the oldest complete partition.
5. Confirm normal collection continues if retention fails.

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

Do not log every raw event.

A dropped-event count of zero is expected under normal conditions. A nonzero count should be visible and should mark that interval as unsuitable for model training, but it should never stop the live collector.

---

# Definition of done

Sequence 1 is complete when all of these are true:

* Receive clocks are captured immediately after `recv()` and before JSON parsing.
* The futures trace is capped at one row per 100 ms.
* Every accepted Chainlink tick remains individually ordered.
* No raw JSON or full book depth is persisted.
* The receive loops never await the raw database writer.
* The Chainlink receive loop no longer awaits a per-event PostgreSQL upsert.
* Queue memory has a fixed upper bound.
* PostgreSQL uses batched append-only `COPY`.
* Raw tables have an age limit and a hard disk limit.
* Expired data is removed by dropping partitions.
* Raw-capture failure cannot interrupt live values or existing one-second data.
* The dashboard and predictor do not query raw tables.
* CLOB BBO capture remains deferred until an executable-edge requirement exists.
* REST-to-WebSocket futures cutover is deployed separately from capture.

The first implementation checkpoint should therefore contain only **Binance futures 100 ms price trace + individual Chainlink ticks + bounded writer + retention**. Everything else should remain outside the scope until data demonstrates that it adds predictive value.
