# Pending-category fixture, contract 2

This is a versioned relabeling of the original recorded-hour fixture in the
parent directory. The original fixture and its generator remain unchanged.
The new independent generator is
`research/spot_twap_response/checkpoint_a_replay/build_fixture_pending.py`.
It imports no production engine and uses no network or database connection.

The event and decision gzip files are byte-identical to the original files:
7,505 retained events, 3,600 one-second decisions, and six horizons give 21,600
expected forecast rows. The five expected count columns are `exact`, `carried`,
`pending`, `future`, and `missing`; every row sums to 60. `exact` corresponds to
the engine's `observed` count. All other expected CSV field names are unchanged.

For a historical slot at or before the decision, the original price selection
and 10-second carry limit are applied first. An exact source stamp remains
`exact`. A usable nonexact slot later than the maximum admissible received spot
source stamp is `pending`. A usable nonexact slot below that maximum is
`carried`: it is an interior absent stamp in the retained causal prefix.
Admissibility excludes source timestamps later than their original receipt.
The maximum source stamp is independent of which event arrived most recently.

Missing and future-slot rules are unchanged. Availability reasons still take
precedence over quality. An available forecast is `degraded` only if it has an
interior `carried` slot; otherwise it is `healthy`. `pending` and `future` counts
remain explicit assumptions. Healthy does not mean assumption-free or correct.

| Horizon | Original healthy | Original degraded | New healthy | New degraded | Unavailable, both |
|---|---:|---:|---:|---:|---:|
| 1s | 1,322 | 2,263 | 1,322 | 2,263 | 15 |
| 2s | 1,320 | 2,265 | 1,320 | 2,265 | 15 |
| 3s | 1,335 | 2,250 | 1,340 | 2,245 | 15 |
| 5s | 0 | 3,585 | 1,383 | 2,202 | 15 |
| 10s | 0 | 3,585 | 1,496 | 2,089 | 15 |
| 30s | 0 | 3,585 | 2,066 | 1,519 | 15 |

The generator checked all 21,600 forecasts against the immutable original CSV.
Every price, current source event identifier, included sequence, target stamp,
availability reason, exact/future/missing count, and signed arrival estimate is
unchanged. For every row, original `carried` equals new `carried + pending`.
The original slot lookup and chosen financial inputs are unchanged; only the
nonexact historical-slot label is split after selection. The manifest and
`research/spot_twap_response/checkpoint_a_replay/pending_review.json` record the
comparison and all relevant hashes.

The [original fixture limits](../README.md) still apply. Spot rows are retained
same-second upserts, and replay acceptance ordering and monotonic clocks are
synthetic. A pending label does not promise an eventual exact report. An
interior absent stamp does not prove silence or connection loss. This relabeling
does not measure prediction accuracy, live arrival completeness, frontend
delivery, or production performance.
