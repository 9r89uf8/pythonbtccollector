# Recommended shadow-signal design

Build the first version as a **separate `chainlink_shadow_signal.py` service**.

It should not run inside FastAPI, the futures collector, or the Chainlink collector. Those processes should remain responsible only for collecting and publishing prices. A broken experimental model must never interrupt either feed.

Your current architecture already gives the shadow worker everything it needs:

* Every validated Binance futures `aggTrade` updates `FUTURES_LIVE_KEY`.
* Every accepted Chainlink RTDS update writes to `CHAINLINK_LIVE_KEY`.
* Both cache entries include the producer’s source timestamp and local receive timestamp.
* Raw capture preserves futures in 100 ms receive-time buckets and Chainlink as individual received events.   

So the first shadow version does **not require collector changes**.

```text
btc:live:futures ────┐
                     ├── MGET every 100 ms
btc:live:chainlink ──┘
                              │
                              ▼
                   ShadowSignalEngine
                   - futures ring buffer
                   - Chainlink anchor
                   - model candidates
                   - validity checks
                              │
                  ┌───────────┴────────────┐
                  ▼                        ▼
btc:live:chainlink_shadow       evaluation queue
        Redis + TTL                     │
                  │                     ▼
                  ▼               PostgreSQL results
GET /markets/current/live
                  │
                  ▼
              Dashboard
```

A local Redis `MGET` every 100 ms is the right first implementation. It is simple, matches your 100 ms raw futures resolution, and adds only ten reads per second. Redis Pub/Sub or Streams can replace polling later, but they are not necessary to validate a three-to-four-second lead.

---

# 1. Define exactly what the signal predicts

The first signal should predict:

> **The Chainlink RTDS value after it catches up with the futures move currently visible.**

It should not initially claim to predict:

* The official settlement close
* Polymarket’s Up probability
* Whether an order is executable
* The cause of the futures movement

Those come later.

## Basis-neutral anchored calculation

Let:

* (C_j) be the latest Chainlink price.
* (r_j) be the local time at which that Chainlink update was received.
* (L) be the estimated futures-to-Chainlink receive-time lag.
* (F_{r_j-L}) be the futures price approximately (L) milliseconds before the Chainlink event arrived.
* (F_t) be the current futures price.
* (\beta) be the estimated pass-through coefficient.

Use:

[
\widehat C_{t+L}
================

C_j
\left[
1+\beta\left(\frac{F_t}{F_{r_j-L}}-1\right)
\right]
]

For the first version, use (\beta=1):

[
\widehat C_{t+L}
================

C_j\frac{F_t}{F_{r_j-L}}
]

Then:

[
\text{pending move}
===================

\widehat C_{t+L}-C_j
]

This avoids the permanent Binance-futures/Chainlink basis. It only transfers the **futures percentage move that has occurred since the futures observation associated with the latest Chainlink update**.

## Why the anchor matters

Do not simply calculate:

```text
current_chainlink + futures_now - futures_3_seconds_ago
```

against the wall clock indefinitely.

Instead, whenever a new Chainlink event arrives:

1. Take its `received_ms`.
2. Look back by the model’s lag.
3. Find the futures price at or immediately before that target.
4. Save that pair as a new anchor.
5. Build subsequent forecasts from the new anchor.

Crucially, **re-anchor even when the Chainlink price did not change**. An unchanged-price event still tells you that Chainlink refreshed and considered newer source information. Ignoring it can make the model count an old futures move twice.

Example test:

```text
Futures rises +$10
Signal predicts Chainlink +$10
Chainlink later updates +$10
Futures stays unchanged
New pending signal should be approximately $0
```

---

# 2. Do not hard-code a single lag yet

Your observation suggests approximately three to four seconds, but the shadow phase should test that rather than assume it.

Run these candidates in parallel:

```text
catchup_ratio_l3000_b100
catchup_ratio_l3500_b100
catchup_ratio_l4000_b100
baseline_no_change
```

Where:

* `l3000` means a 3,000 ms lag.
* `b100` means (\beta=1.00).
* `baseline_no_change` predicts that Chainlink will remain at its current value.

The dashboard should show only one configured primary model, chosen using the initial raw-data replay. All candidates should continue to be evaluated silently.

Do not dynamically switch to whichever candidate recently performed best. That would make the historical evaluation difficult to interpret and could introduce selection bias.

Once enough data exists, estimate (L) and (\beta) properly instead of retaining these provisional values.

---

# 3. Pure signal engine

Keep the mathematical state machine separate from Redis, PostgreSQL, FastAPI, and asyncio. That lets the exact same engine run in:

* The live shadow worker
* Unit tests
* Historical raw-data replay
* Future simulation

A simplified core structure:

```python
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Deque, Optional


ONE = Decimal("1")


@dataclass(frozen=True)
class ObservedPrice:
    value: Decimal
    source_timestamp_ms: Optional[int]
    received_ms: int


@dataclass(frozen=True)
class CatchupModel:
    version: str
    lag_ms: int
    beta: Decimal


@dataclass(frozen=True)
class ModelAnchor:
    chainlink: ObservedPrice
    futures_reference: ObservedPrice


@dataclass(frozen=True)
class Projection:
    model_version: str
    horizon_ms: int
    projected_chainlink: Decimal
    pending_move: Decimal
    pending_move_bps: Decimal


def project_from_anchor(
    *,
    model: CatchupModel,
    anchor: ModelAnchor,
    futures_now: ObservedPrice,
) -> Projection:
    futures_return = (
        futures_now.value / anchor.futures_reference.value
    ) - ONE

    projected = anchor.chainlink.value * (
        ONE + model.beta * futures_return
    )
    pending = projected - anchor.chainlink.value
    pending_bps = pending / anchor.chainlink.value * Decimal("10000")

    return Projection(
        model_version=model.version,
        horizon_ms=model.lag_ms,
        projected_chainlink=projected,
        pending_move=pending,
        pending_move_bps=pending_bps,
    )
```

The surrounding `ShadowSignalEngine` would maintain:

```python
self._futures_history: Deque[ObservedPrice]
self._last_futures_identity
self._last_chainlink_identity
self._anchors: dict[str, ModelAnchor]
```

Use this identity for detecting a new event:

```python
(
    price.source_timestamp_ms,
    price.received_ms,
    price.value,
)
```

Do not detect updates using price alone.

---

# 4. Shadow worker loop

The worker should run on a 100 ms cadence:

```python
while True:
    now_ms = current_utc_epoch_ms()

    futures, chainlink = await live_cache.get_prices(
        [FUTURES_LIVE_KEY, CHAINLINK_LIVE_KEY]
    )

    signal = engine.observe(
        futures=futures,
        chainlink=chainlink,
        now_ms=now_ms,
    )

    await live_cache.set_shadow_signal(
        signal,
        ttl_ms=settings.SHADOW_SIGNAL_TTL_MS,
    )

    evaluator.observe(signal, chainlink=chainlink, now_ms=now_ms)

    await sleep_until_next_100ms_boundary()
```

## Futures history

Retain roughly:

```text
maximum candidate lag
+ maximum Chainlink freshness allowance
+ safety margin
```

Ten seconds is ample for the initial three-to-four-second models.

For a reference lookup at `target_ms`:

1. Choose the newest futures observation with `received_ms <= target_ms`.
2. Reject it if it is too far behind the target.
3. Never use a future observation from after the target.

With a 100 ms loop, an initial maximum reference gap of approximately 200–250 ms is reasonable for shadow mode, but the final threshold should come from the observed data.

## Startup behavior

The service cannot immediately forecast because Redis only contains the latest value, not the previous four seconds.

Expected startup states:

```text
warming_up_futures_history
waiting_for_new_chainlink_anchor
valid
```

After accumulating enough futures history, wait for the next Chainlink event and create the first valid anchor. Do not backfill the live worker from PostgreSQL; keep the latency-sensitive path independent of the database.

---

# 5. Validity rules

A numerical forecast should be emitted only when all necessary conditions hold.

Suggested statuses:

```text
valid
warming_up
chainlink_unavailable
futures_unavailable
chainlink_stale
futures_stale
anchor_history_missing
anchor_reference_gap
timestamp_regression
model_error
```

The payload may always be written, but projection fields must be `null` when invalid.

Important rules:

* A missing value is invalid.
* A stale value is invalid even though Redis still contains it.
* A new Chainlink event without enough preceding futures history is invalid.
* A regressing timestamp should reset that source’s model state.
* Never carry forward the previous valid forecast after the current inputs become invalid.
* The Redis signal key must expire automatically if the worker stops.

The existing live endpoint deliberately returns old Redis values and does not perform server-side staleness rejection, so the signal worker must implement these rules itself.  

Derive the actual freshness thresholds from raw capture:

```text
futures stale threshold   = healthy-session p99 interarrival + margin
Chainlink stale threshold = healthy-session p99 interarrival + margin
```

Avoid using one common threshold for both feeds.

---

# 6. Redis signal contract

Use a stable key:

```python
CHAINLINK_SHADOW_LIVE_KEY = "btc:live:chainlink_shadow"
```

Put the model version inside the payload rather than in the Redis key.

Example:

```json
{
  "schema_version": 1,
  "mode": "shadow",
  "model_version": "catchup_ratio_l3500_b100",
  "generated_ms": 1783727695125,
  "valid": true,
  "status": "valid",
  "invalid_reasons": [],
  "horizon_ms": 3500,
  "estimated_lag_ms": 3500,

  "current_chainlink": "64080.47",
  "projected_chainlink": "64103.08",
  "pending_move": "22.61",
  "pending_move_bps": "3.528",
  "direction": "up",

  "futures_now": "64109.10",
  "futures_reference": "64086.50",

  "anchor_chainlink_source_timestamp_ms": 1783727695000,
  "anchor_chainlink_received_ms": 1783727695070,
  "futures_now_received_ms": 1783727695102,
  "futures_reference_received_ms": 1783727691574,

  "futures_received_age_ms": 23,
  "chainlink_received_age_ms": 55,

  "market_id": 5945758,
  "market_end_ms": 1783727700000,
  "ms_to_market_end": 4875,
  "full_horizon_before_market_end": true
}
```

All decimal values should remain JSON strings, consistent with the rest of your live-cache contract.

Use a short TTL such as:

```text
1.5–2.0 seconds
```

because the worker should refresh the key every 100 ms. If the worker dies, the dashboard sees the signal disappear rather than displaying a frozen “valid” forecast.

## Keep it separate from `LivePrice`

Do not force this structure into the existing `LivePrice` dataclass. Add something like:

```python
@dataclass(frozen=True)
class LiveShadowSignal:
    ...
```

with dedicated encoder and decoder functions.

A malformed experimental shadow payload should also **not cause the entire live-price endpoint to return HTTP 503**. The three actual prices are more critical. Decode the signal separately and return it as unavailable while logging the model-payload error.

---

# 7. API response

Extend the existing endpoint without adding a PostgreSQL query:

```json
{
  "server_time_ms": 1783727695200,
  "market_id": 5945758,
  "market_start_ms": 1783727400000,
  "market_end_ms": 1783727700000,

  "prices": {
    "...": "existing fields"
  },

  "futures": {
    "...": "existing fields"
  },

  "signals": {
    "chainlink_catchup": {
      "...": "shadow payload",
      "signal_age_ms": 75
    }
  }
}
```

Add the signal key to the same Redis `MGET`. The API should calculate:

```python
signal_age_ms = max(0, server_time_ms - generated_ms)
```

FastAPI remains a serializer, not a model host.

---

# 8. Market-expiry handling

For the fixed-lag V0 model, include:

```text
full_horizon_before_market_end =
    generated_ms + horizon_ms <= market_end_ms
```

When this is false, the signal may still predict the next Chainlink move, but it cannot claim that the full move will arrive before that five-minute market closes.

Do **not** linearly scale the pending move based on time remaining. A fixed three-second lag does not imply that one-third of the move arrives in one second.

For V0:

```text
Projected Chainlink catch-up: available
Projected Chainlink at market close: null when horizon crosses expiry
```

A later distributed-lag model can calculate how much of the response is expected to arrive before expiry.

Also keep the market-window logic independent from model state. The price relationship does not reset every five minutes; only the displayed `market_id`, `market_end_ms`, and expiry relevance change.

---

# 9. Shadow evaluation is part of the feature

A shadow signal without stored outcomes will produce an attractive dashboard but no reliable evidence.

## Exact evaluation target

For a forecast generated at (t) with horizon (L), define the target as:

> The latest Chainlink RTDS value actually known at local time (t+L).

This is a precise, causal receive-time target. It measures what the dashboard would have displayed at the model horizon.

Store:

* Chainlink at forecast time
* Projected Chainlink
* Actual Chainlink as of target time
* Actual Chainlink event age at target
* No-change baseline
* Futures current/reference inputs
* Model version
* Freshness
* Market context

## Evaluation cadence

The live signal can update every 100 ms, but persist an evaluation forecast every 500 ms.

The worker keeps pending forecasts in a small time-ordered heap:

```text
forecast generated
        │
        ├── target = generated + horizon
        │
        ▼
wait until target matures
        │
        ▼
attach latest Chainlink value
        │
        ▼
enqueue noncritical DB write
```

Database writes must be done by a separate bounded queue and batch writer. A slow database must never block live signal generation.

A compact table could contain:

```text
model_version
generated_ms
target_ms
horizon_ms
market_id

chainlink_at_forecast
projected_chainlink
futures_now
futures_reference

actual_chainlink
actual_chainlink_received_ms
actual_chainlink_age_at_target_ms

forecast_error
baseline_error
pending_move_bps

futures_age_ms
chainlink_age_ms
created_at
```

Use a unique key on:

```text
(model_version, generated_ms, horizon_ms)
```

## Required comparisons

For every model candidate, measure:

* Median absolute error in dollars and basis points
* RMSE
* Error versus no-change baseline
* Directional accuracy outside a neutral/noise band
* Valid-signal coverage
* Performance by move size
* Performance by upward versus downward movement
* Performance by volatility regime
* Performance near market expiry
* Performance around collector reconnects

The most important number is not raw directional accuracy. It is:

[
|\text{forecast error}|
\quad\text{versus}\quad
|\text{no-change error}|
]

A model that predicts tiny movements in the correct direction but increases price error is not useful.

---

# 10. Raw-data replay

Before selecting the primary model, replay the raw capture chronologically.

Use:

* Futures `close_price` from each 100 ms receive-time bucket
* Chainlink events ordered by `received_wall_ns`
* Feed-session records to exclude reconnect gaps and sessions with dropped records

The raw schema stores futures 100 ms OHLC traces with source and local receive timing, while Chainlink events retain their individual receive, provider-event, and provider-message timestamps.  

For cross-feed alignment:

* Use `received_wall_ns` as the primary actionable timeline.
* Use source timestamps for diagnostics and a separate source-lag analysis.
* Use monotonic timestamps mainly to validate ordering and timing consistency within each collector session.

Measure two different concepts:

```text
source-time lag:
    Does the Chainlink source price trail futures source events?

receive-time lead:
    How much earlier does your application receive the useful futures move?
```

The second is the lead the dashboard can actually exploit.

Your configured raw retention is currently 72 hours. Run the replay/evaluation job at least daily, or preserve a derived evaluation dataset beyond raw retention. Otherwise useful calibration evidence will be automatically removed. 

---

# 11. Do not add flow and book inputs to V0

Keep the first signal entirely price-based.

Your current flow and book products are one-second aggregates and have configured 1.5-second flush delays. That makes the persisted versions unsuitable as low-latency inputs for a three-to-four-second lead. 

They can be added later only after the price-only signal is proven. At that stage, consume direct live WebSocket state rather than querying the delayed PostgreSQL aggregates.

Possible later confidence features include:

```text
aggressive trade direction
100 ms quote notional
book mid or microprice
futures/spot confirmation
very short-term reversal
```

They should alter confidence or the pass-through estimate only if they improve chronological out-of-sample results.

---

# 12. File-by-file implementation plan

| File                         | Change                                                              |
| ---------------------------- | ------------------------------------------------------------------- |
| `shadow_signal.py`           | Pure models, ring buffer, anchor logic, projections, validity state |
| `shadow_signal_collector.py` | 100 ms Redis polling loop, cache writing, evaluation scheduler      |
| `live_cache.py`              | Shadow key, typed encoder/decoder, TTL write, combined raw `MGET`   |
| `config.py`                  | Poll cadence, candidate models, freshness, TTL, evaluation cadence  |
| `api.py`                     | Add `signals.chainlink_catchup`; isolate signal decode failures     |
| `schema.sql`                 | Add shadow evaluation table and indexes                             |
| `db.py`                      | Batched insert function for matured evaluations                     |
| tests                        | Unit, integration, and raw-replay parity tests                      |

Suggested configuration:

```python
SHADOW_SIGNAL_ENABLED: bool = False
SHADOW_SIGNAL_POLL_MS: int = 100
SHADOW_SIGNAL_TTL_MS: int = 2_000

SHADOW_SIGNAL_PRIMARY_MODEL: str = "catchup_ratio_l3500_b100"
SHADOW_SIGNAL_LAG_CANDIDATES_MS: str = "3000,3500,4000"
SHADOW_SIGNAL_BETA: Decimal = Decimal("1.0")

SHADOW_SIGNAL_FUTURES_STALE_MS: int = 1_000
SHADOW_SIGNAL_CHAINLINK_STALE_MS: int = 5_000
SHADOW_SIGNAL_REFERENCE_MAX_GAP_MS: int = 250

SHADOW_SIGNAL_EVALUATION_INTERVAL_MS: int = 500
SHADOW_SIGNAL_EVALUATION_QUEUE_MAX: int = 5_000
```

The freshness numbers above are provisional configuration values, not calibrated recommendations.

---

# 13. Tests that should block deployment

The highest-value deterministic tests are:

1. **Basis removal:** A stable $10 futures/Chainlink basis produces zero pending move.
2. **Upward projection:** A 10 bp futures move produces approximately a 10 bp projected Chainlink move with (\beta=1).
3. **Downward projection:** Same for negative movement.
4. **Same-price Chainlink refresh:** A newer timestamp with identical price creates a new anchor.
5. **No double count:** After Chainlink catches up, unchanged futures produces near-zero pending movement.
6. **Warm startup:** No valid signal until sufficient futures history and a new Chainlink anchor exist.
7. **Stale futures:** Projection becomes null immediately.
8. **Stale Chainlink:** Projection becomes null immediately.
9. **Timestamp regression:** State resets rather than using out-of-order data.
10. **Worker death:** Redis signal expires while the three actual live-price values remain available.
11. **Market boundary:** Model state continues, but `market_id` and expiry relevance roll correctly.
12. **Replay parity:** Feeding recorded events into the pure engine reproduces the same projections as the live-worker algorithm.

---

# Build order

1. Implement and test the pure anchor-based engine.
2. Build raw-capture replay and compare 3.0, 3.5, and 4.0-second models with no-change.
3. Select a provisional primary model.
4. Add the standalone 100 ms shadow worker and expiring Redis key.
5. Add matured forecast evaluations.
6. Expose the signal through the existing live endpoint.
7. Display it on the dashboard as **Shadow / Experimental**.
8. After enough chronological evidence, fit a distributed response curve and add an expiry-aware close forecast.

The first dashboard card should contain only:

```text
Current Chainlink
Projected Chainlink
Pending move: $ and bps
Catch-up horizon
Freshness/status
Full horizon before market close: yes/no
```

That produces a clean, falsifiable shadow signal without yet turning a single observed lag pattern into an overconfident market-probability model.
