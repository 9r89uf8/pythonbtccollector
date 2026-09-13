# Recorded-hour ghost TWAP fixture

This fixture freezes the first UTC-aligned hour in the existing September 11
export with at least 120 seconds of both-feed export coverage on either side:
**2026-09-11 01:00:00–02:00:00 UTC**, with receipt context
**00:58:00–02:02:00 UTC**. The selection was chronological, before evaluating
forecast quality. Midnight lacked the required TWAP warm-up coverage.

The three CSV files use deterministic gzip encoding (`mtime=0`, no embedded
filename). All prices remain decimal strings. See `manifest.json` for source,
generator, and artifact SHA-256 hashes.

| File | Rows | Purpose |
|---|---:|---|
| `recorded_hour_events.csv.gz` | 7,505 | 3,751 retained Chainlink spot samples and 3,754 durable 60-second TWAP events |
| `recorded_hour_decisions.csv.gz` | 3,600 | One decision at every UTC second in the selected hour |
| `recorded_hour_expected.csv.gz` | 21,600 | Independent expected calculation at horizons 1, 2, 3, 5, 10, and 30 seconds |

## Events and clocks

Every original source field is retained as text without price or clock rounding:
`kind`, `source_ms`, `sample_second_ms`, `price`, `price_e18`, `received_ms`, and
`received_wall_ns`. The original export was not sorted. `source_export_row` is
its one-based **data-row ordinal**, excluding the header; it is not the original
acceptance sequence.

For deterministic replay only:

- `replay_received_wall_ns` uses the original TWAP nanoseconds or spot
  `received_ms × 1,000,000`. The spot values retain millisecond measurement
  resolution; conversion does not supply submillisecond precision.
- `replay_order` sorts by normalized receipt, then original data-row ordinal.
  There are no receipt ties in this fixture.
- `replay_monotonic_ns` is normalized receipt minus the first fixture receipt.
  It is **synthetic**, not a recorded monotonic clock.
- `event_id` is a derived fixture identifier. `window_s` is `60` for TWAP and
  blank for spot.

The engine adapter maps `kind` to feed, `Decimal(price)` to value, `source_ms`
to source timestamp, the two replay clock fields to receipt clocks,
`replay_order` to sequence, and blank `window_s` to `None`. It accepts only
events whose normalized receipt is at or before the next decision cutoff.
Events after a decision remain unavailable to that decision. Follow-through
events support later checking; they never enter earlier expected calculations.

## Independent expected calculation

The builder under `research/spot_twap_response/checkpoint_a_replay/` has no
production imports. It calculates each 60-slot window directly from the causal
prefix, using a Decimal context of 80 digits and final 18-place half-even
rounding. It does not compare forecasts to subsequently realized TWAP values.

At decision `D`, the latest received spot and TWAP rows are selected before
freshness is tested. Current source and receipt ages must be between zero and
3,000 ms inclusive; a source timestamp later than its original receipt is always
invalid. A stale latest value blocks availability rather than selecting an
older fresh value. The 120-second retained source history includes its latest
preceding spot seed within the 10-second carry allowance.

For TWAP anchor source stamp `w` and horizon `h`, target `U = w + h × 1,000`.
The window contains the 60 second slots `U − 62,000` through `U − 3,000`,
inclusive. A historical slot at or before `D` selects the greatest observed spot
source stamp at or before that slot, using the latest received revision and at
most 10,000 ms of carry. A future slot after `D` uses the valid current spot.
Every row partitions 60 slots into `exact`, `carried`, `future`, and `missing`;
`exact` maps to the engine's `observed` count. Invalid current inputs, missing
slots, or an already-received exact target make the forecast unavailable.

`price` is blank when unavailable. `quality` is `unavailable`, otherwise
`degraded` if any slot was carried, otherwise `healthy`. `reasons` is a
pipe-separated sequence of explicit reason codes. Current event identifiers and
`included_sequence` permit checking which receipt prefix was used.

`estimated_arrival_wall_ns` is the baseline TWAP receipt plus the horizon.
`estimated_remaining_ns` subtracts the decision cutoff and remains **signed and
unclipped**. These are model estimates, not measured future arrivals. A target
source stamp at or before `D` does not by itself invalidate a nowcast.

At every horizon, 3,585 forecasts are available and 15 are unavailable. The 15
unavailable cutoffs all have stale current TWAP; 13 also have stale current
spot. The maximum retained history at a decision is 240 events, below the 1,024
event limit. No source regressions, future-at-receipt timestamps, duplicate
source stamps, conflicting source prices, or normalized receipt ties occur in
this hour. Adversarial tests are still required for those separate cases.

## Limits of this evidence

Spot is a same-second upserted table: overwritten earlier arrivals are absent.
TWAP comes from durable events, but the export omitted original event IDs,
connection sessions, accepted sequence, monotonic clocks, and explicit gap
records. Missing source seconds remain observable as absent rows, but they do
not establish a connection failure or complete first-arrival history. The
synthetic ordering cannot test real clock corrections or original cross-feed
acceptance ordering.

This fixture tests deterministic, causal calculation against retained data. It
does not establish complete live replay, predictive accuracy, trading fills,
frontend delivery, or production latency. No production connection or query was
used to prepare it.
