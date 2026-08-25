# Lock, Late-Flip, and Futures-to-TWAP Research Plan

Created from a read-only review of the current repository and the live
production database on 2026-08-25 UTC. Revised after validation to incorporate
the Q1-Q5 presentation, `$10` distinction, compact-model idea, latency
decomposition, extraction workflow, and fixed-terminal-window arithmetic.
Standard Chainlink spot is close on median in exploratory terminal diagnostics
but has material tails and is treated only as a proxy for the unobserved
settlement input path; it is not settlement truth and does not restore any
certainty claim. This plan does not use Git history,
deleted files, backups, precomputed predictions, or prior derived flip labels.

## Objective

Answer five questions with definitions that can be reproduced and used live:

1. When can the current Up or Down side be called "locked," meaning its chance
   of losing is below a stated risk threshold?
2. In which part of the final 20 seconds do flips occur most often, and how
   does that risk change with exact-TWAP and spot distance, including `$10`?
3. Which observable conditions increase the probability of a late flip?
4. How long after a futures move does the exact 60-second TWAP visibly respond?
5. Can a brief futures move materially move TWAP, and how do amplitude,
   duration, and spot confirmation change the observed response?

The work should produce calibrated risk ranges and uncertainty, not a single
unqualified rule such as "three bps means locked."

This research build ends with reproducible artifacts and, if the prospective
gate passes, a rule eligible for a separate productionization decision. It does
not itself add live scoring to a collector or API. Any later runtime integration
is a new owner-authorized production checkpoint with inference-parity tests and
the normal schema, service, security, and droplet-update requirements.

## Executive recommendation

The main descriptive and exploratory calibrated lock/late-flip study is ready
to run with the current data. There are approximately 2,549 fully resolved
post-cutover markets with strong final-20-second coverage and a balanced
Up/Down outcome split. Nine days of data may still be insufficient to certify
a production 1% lock frontier in sparse time/distance cells; those cells must
return `insufficient evidence` rather than a threshold.

Run the study in two resolutions and two evidence layers:

- **Primary now:** one-second, causally aligned analysis using exact TWAP
  events, one-second probability state, futures, flow, book, and
  microstructure. Official values and exact E18 TWAP events remain the
  settlement core.
- **Secondary now:** an error-aware, standard-Chainlink-spot reconstruction of
  the elapsed part of the fixed terminal 60-second window. It is a Context
  feature and sensitivity analysis, never a replacement label or exact TWAP.
- **Parallel operational track:** seek separate authorization for Phase 4
  high-resolution validation while the one-second study starts. The current
  100 ms futures table is empty, so current data cannot distinguish a 100 ms
  flash from a 900 ms flash. Capture remains gated by explicit production
  checks and authorization.

The core lock model must not depend on short-retention optional data. Add
microstructure as an incremental model so the lock rule remains usable and can
continue learning from longer-lived core history.

Run three releases rather than waiting for every modeling layer:

- **Release 1A — descriptive truth plus exploratory proxy:** immediately after
  Checkpoint 2,
  publish the evidence audit, boundary alignment, late-change timing, and
  exact-TWAP time-by-bps surface with `$10` overlays. Publish the paired
  spot-proxy surface only if its validation gate proceeds; otherwise publish
  the proxy abstention verdict and evidence. Later
  modeling, Q4/Q5, and certification cannot block it. Set a UTC deadline before
  extraction; the default is 120 hours after the frozen extraction cutoff,
  whether the evidence gate passes or fails. If the gate or Checkpoint 2 is not
  complete, publish the gate failure, exclusions, completed descriptive
  results, and explicit remaining blockers by that deadline instead of
  silently waiting. Label every spot-proxy panel exploratory.
- **Release 1B — descriptive models:** publish CLOB calibration,
  condition/ablation results, and seconds-scale lag/flash bounds after
  Checkpoint 5. These are useful even if rare-event support is insufficient for
  a 1% lock declaration.
- **Release 2 — prospective certification:** freeze the lock rule, then test it
  once on a genuinely post-freeze block. Publish a production threshold only
  when its aggregate upper error bound and coverage pass; otherwise retain the
  descriptive surface and return `insufficient evidence` for certification.

## Q1-Q5 at a glance

| Question | Primary measurement | Main output |
| --- | --- | --- |
| Q1: when is Up/Down locked? | Official-loss risk of the current exact-TWAP leader and, separately, the fresh CLOB favorite | 10%/5%/1% lock regions with upper error bounds, coverage, revocations, and abstention |
| Q2: where do final-20-second flips happen? | Recurrent observed side changes plus leader/final-outcome disagreement at `T-20` through `T-1` | One-second transition/last-change distributions and time-by-bps risk surface |
| Q3: what raises flip risk? | Held-out conditional risk differences after controlling for time and exact-TWAP margin | Ranked condition table plus one deliberately small calibrated model |
| Q4: when is futures movement visible in TWAP? | Interval-censored futures shock -> spot response -> TWAP response -> relay delivery | Per-leg and end-to-end p10/p50/p90 latency ranges by shock size |
| Q5: can a flash materially move TWAP? | Empirical amplitude x duration x spot-confirmation response | Supported dose-response matrix, empirical response-ratio ranges, and strike-crossing contours |

The compact table is the execution map. The later sections are the safeguards
needed to make each row defensible.

## What is collected now

### Settlement identity and truth labels

- Five-minute half-open market windows and IDs.
- Polymarket market identity, Up/Down token IDs, and exact settlement rule
  identity.
- Official Price to Beat, official final price, official winner or split,
  payouts, winning token, Gamma `closedTime`-derived resolution time, raw
  resolution evidence, and reconciled settlement-rule marker.
- Current 60-second markets are identified by `chainlink_twap`, window `60`,
  rule `btc-5m-twap-60`, with the immutable cutover at
  `2026-08-14T00:00:00Z`.

### Polymarket probability state

At most one fresh two-outcome state snapshot per market second. Both outcome
asks must be present, but either bid may be missing; in that case the stored
midpoint falls back to the available ask:

- Up and Down bid, ask, midpoint, and normalized midpoint probability.
- Independent Up and Down provider-event and local-receive freshness
  timestamps. Each is the **oldest** bid/ask component used for that outcome,
  not the time at which the complete state became available.
- Raw state with separate bid/ask component timestamps. Causal reconstruction
  must verify all used components and take their latest receipt as the state's
  availability time.

This is a one-second state series, not every CLOB quote event. It can measure
one-second and persistent favorite flips, but not subsecond quote flicker.

### Standard Chainlink spot and exact settlement TWAP

- Standard Chainlink BTC/USD spot context in `price_samples`, keyed by provider
  second.
- Exact 60-second settlement TWAP events in `polymarket_twap_events`, including
  E18 value, Decimal price, provider time, local wall and monotonic receive
  times, connection ID, and sequence.
- Durable TWAP sessions and explicit no-replay gap records.
- A one-second TWAP materialization in `price_samples` for convenient joins;
  exact event history remains the research source of truth.

### Binance prices, futures state, flow, and book

- Binance spot last price sampled on each local UTC second when the latest
  ticker is fresh. Provider event and actual receive time are retained, and
  consecutive sampler rows can repeat the same ticker observation.
- Futures `aggTrade` last price and trade timestamp in the one-second REST
  snapshot; that row's `received_ms` is the snapshot observation time, not the
  selected trade's receive time. When snapshot raw JSON persistence is enabled,
  `raw.aggTrade.received_ms` retains the selected trade's exact pre-parse
  receive millisecond; audit its live coverage before use because the setting
  defaults off. Even when present this is only one selected trade—not complete
  per-trade history or the timing/order of the one-second high and low.
  Receipt-aligned path summaries come from microstructure; a continuous
  high-resolution path requires the disabled 100 ms trace.
- Mark, index, premium, funding, open interest, OI notional, and five-minute OI
  summaries.
- One-second taker buy/sell volume and notional, delta, imbalance, CVD, counts,
  and maximum trade notional.
- One-second top-of-book bid/ask, quantities, spread, imbalance, and
  microprice.

The standard flow table deliberately emits zero-flow rows for missing source
seconds, so its row coverage alone is not proof of collector health.

### One-second receipt-aligned microstructure

The live deployment has microstructure enabled. Each causal local-receipt
second can contain:

- Spot and futures top-1/5/10 imbalance, top-10 bid/ask notional depth,
  weighted midpoint, spread, BBO OFI, and book ages/lags.
- Spot and futures buy/sell notional, aggregate-trade counts, largest trade,
  VWAP, high, low, last, and trade ages/lags.
- Futures RPI-involved flow, perp/spot basis, mark/index state, funding, and OI.
- Observed forced-order long/short notional and collection health fields.

It stores summaries, not raw spot trades, depth frames, or forced-order frames.

### High-resolution data not currently available

- `raw_capture.binance_futures_price_trace_100ms`: zero rows; disabled.
- `raw_capture.chainlink_price_events`: zero rows; disabled.

The exact TWAP event table is populated independently of these optional raw
tables. It provides precise TWAP receipt timestamps, but there is no matching
subsecond futures path today.

## Live readiness snapshot

Read-only audit at approximately `2026-08-25 00:34-00:38 UTC`:

- All collectors, the API, and Redis were active.
- Data spans approximately `2026-08-16 01:27 UTC` through
  `2026-08-25 00:38 UTC`, about nine days and 2,583 market windows.
- 2,549 markets were fully resolved with official open, close, winner, and
  reconciled settlement identity: 1,294 Up and 1,255 Down. Another 33 were
  pending.

Approximate live row counts, which continued moving during the audit:

| Series | Rows | Span-second coverage |
| --- | ---: | ---: |
| Binance spot | 774,420 | 99.995% |
| Chainlink spot | 729,717 | 94.223% |
| Chainlink 60-second TWAP materialization | 729,819 | 94.236% |
| Polymarket probability | 761,203 | 98.262% |
| Futures snapshots | 770,348 | 99.443% |
| Futures flow | 774,646 | 99.998% |
| Futures book | 774,464 | 99.974% |
| Microstructure | 774,649 | 99.998% |

The resolution count advanced while collectors remained live. The earlier
resolution query found 2,549 markets; the later final-20 coverage query found
2,550, producing 51,000 checkpoint cells:

| Series | Available cells / 51,000 | Coverage |
| --- | ---: | ---: |
| Polymarket probability | 50,415 | 98.85% |
| TWAP event-second | 48,133 | 94.38% |
| Futures snapshot | 50,946 | 99.89% |
| Futures flow | 50,995 | 99.99% |
| Futures book | 50,993 | 99.99% |
| Microstructure | 50,996 | 99.99% |

In the earlier 2,549-market cohort, at `T-1`, 2,537 markets had a probability
snapshot and 2,410 had a TWAP event in that exact provider second. The analysis
must use the latest causally available valid TWAP event rather than require an
event in every exact second.

TWAP timing is material:

- Approximately 729,943 exact TWAP events. The frozen extract must recompute
  distinct provider seconds and duplicates; the durable event key permits
  multiple accepted events in one provider second, while `price_samples`
  retains only the latest materialization for that second.
- Provider-event spacing: p50 1 second, p95 2 seconds, p99 2 seconds, maximum
  44 seconds.
- Local receipt minus provider event: p50 1.822 seconds, p95 2.478 seconds,
  p99 2.730 seconds.
- 137 explicit gaps: 89 remote closes, 33 idle timeouts, 14 connection errors,
  and one planned restart.

Consequently, "the oracle had changed" and "the change was visible to this
system" can differ by roughly two seconds and must be reported separately.

### Follow-up feedback audit

A second read-only audit reproduced the quoted comparison on the first 2,564
eligible resolved markets ordered by resolution update, through approximately
`2026-08-25 01:50:25 UTC`. These are exploratory diagnostics; Checkpoint 1 must
still freeze and hash the extract and SQL.

| Candidate versus official close | Eligible n | Median absolute error | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Last exact TWAP event before `E` | 2,564 | 0.02994 bps | 0.47424 bps | 2.77700 bps |
| Exact TWAP event at `E` | 2,410 | <0.0000000005 bps | <0.0000000005 bps | <0.0000000005 bps |
| First exact TWAP event at/after `E`, unrestricted | 2,564 | <0.0000000005 bps | <0.0000000005 bps | 1.61136 bps |
| First event only when no later than `E+3s` | 2,559 | <0.0000000005 bps | <0.0000000005 bps | 0.12214 bps |
| Available-second standard-spot mean over `[E-60s,E)`, at least 50 observations | 2,519 | 0.05795 bps | 0.64657 bps | 4.49901 bps |

The feedback's quoted `n=2,559` and `max=0.12 bps` first-after result is
reproduced only when eligibility is capped at `E+3s`; the unrestricted result
has the larger maximum shown above. Exact-boundary events match within the
explicit table tolerance; a delayed first-after value may have moved. For the
standard-spot mean, only 226 markets had all 60 provider seconds and even that
subset reached about `1.32 bps` maximum error, so missing seconds do not explain
the entire tail. The available-second mean picked the wrong official side in
8 of 2,519 eligible markets, all near the strike; a last-observation-carried
integration convention changed that count. This supports a useful proxy and a
convention-sensitivity study, not settlement identity.

The opening boundary showed the same pattern: 2,409 exact-start events matched
at rounding scale, while an unrestricted delayed first-after candidate reached
about `2.04 bps` error. At exact intermediate checkpoint seconds, local rolling
means had roughly `0.058-0.066 bps` median error and `0.70-0.89 bps` p99 error
against the streamed TWAP, with a 50/60-observation maximum above `12 bps`.
Daily
terminal-error medians also varied materially. Terminal agreement alone
therefore cannot certify partial-window identity or a stable physical bound.

## Settlement-evidence validation gate

Complete this before calculating a flip, lock, or required-shock feature.

1. For every training market, compare the official Price to Beat and official
   final price with the exact E18 TWAP events nearest the market start and end
   under each plausible boundary alignment: last provider event before the
   boundary and first provider event at/after it.
2. Treat an event exactly on the boundary separately from a positive-offset
   first-after event. A boundary event is stored in the next half-open market;
   optimized SQL must still join it back to the old market's end for label
   validation. Report exact-match frequency, signed and absolute error, and
   error conditional on provider offset. A delayed first-after event is a later
   rolling window, not automatically the missing boundary value.
3. Freeze the source-time convention now as the exact provider-time boundary
   event when present. The full-span feedback audit has already selected this
   candidate, so historical splits can reproduce or reject it but cannot serve
   as untouched selection evidence. Bounded positive-offset events remain
   sensitivity evidence only; the default for a missing boundary event is
   `boundary evidence absent`, not silent substitution; the independently
   reconciled official close remains the label. Apply the rule without retuning
   across sessions, days, calibration/internal-audit markets, offset strata,
   gaps, both directions, and the later prospective block.
4. Confirm separately that the Price to Beat itself was available in evidence
   received before each live checkpoint. Equality with a later reconciled value
   is necessary but does not prove historical availability.
5. Quantify boundary-event absence, event age, multiple-event seconds,
   provider/receipt offsets, and explicit gaps. Never manufacture a boundary
   value by filling across a gap.
6. Freeze the duplicate-event policy on training data. The observable path may
   order same-provider-time events by their persisted acceptance/receive order.
   The provider-time path treats them as a tied set and collapses to one value
   per provider timestamp using a deterministic last-accepted rule, with
   first/last/range sensitivity reported. Do not count oscillations inside one
   tied provider timestamp as separate provider-path one-second flips.
7. Apply the frozen alignment and duplicate policy unchanged to the historical
   internal-audit and later prospective blocks. A failure is a failed evidence
   gate, not permission to tune another convention on evaluation labels.

An exact-boundary event may validate the old market's official-close label
under the frozen alignment. A positive-offset first-after event is post-close
sensitivity evidence only: it never substitutes for a missing boundary event,
labels the old close, or becomes a feature available inside that old market.

### Standard-spot proxy validation

The standard `crypto_prices_chainlink` spot series may also be integrated as a
contextual approximation to the settlement input path. Follow-up exploratory
queries show that this is worth testing, but do not establish exact identity:
the terminal local mean has roughly `0.058 bps` median absolute error against
the official close, while its approximately `0.65 bps` p99 and `4.5 bps`
maximum are material around a `$10` case. In the audited cohort, `$10` was
about `1.25-1.60 bps` (median approximately `1.39 bps`); always use the
per-market conversion rather than a fixed approximation. The
collector does not observe Chainlink's internal TWAP constituents or weights,
and the one-second spot materialization collapses multiple events in a provider
second.

Validate this proxy separately, using training data only to freeze:

1. Inclusion endpoints, second weighting, available-row versus carry/hold
   policy, minimum observed cadence, maximum known gap, and maximum age.
   Same-provider-second duplicate status and overwritten versions are unknown
   in current spot storage, so do not invent a duplicate policy or flag.
2. Terminal local 60-second means against official closes.
3. Rolling local 60-second estimates against contemporaneous exact E18 TWAP
   events across the full training span, not only market boundaries.
4. Signed and absolute residuals by horizon, day, session, provider offset,
   cadence, known gap state, volatility, and direction.
5. A conservative observable-time approximation using only final-materialized
   spot rows whose stored `received_ms` is before the checkpoint, with
   provider-time results reported separately. It cannot reconstruct an earlier
   same-second value overwritten after that checkpoint.

Aggregate terminal error can hide cancellation and does not validate every
partial-window contribution. The default uncertainty target is therefore a
horizon/quality-specific predictive interval for terminal margin or official
loss calibrated directly on training or out-of-fold markets—not a claimed
measurement interval for the unobserved partial input. Never use that market's
realized final reconstruction error as a feature, and never retroactively
exclude a checkpoint because a spot gap happened later. The proxy may inform
Context features and sensitivity results; it must not certify or replace the
settlement TWAP, official close, or exact-TWAP leader.

## Definitions fixed before analysis

### Time bins

The closing boundary belongs to the next market. The final observable bin for
an old market is:

```text
[market_end_ms - 1000, market_end_ms)
```

Call that bin `0-1 seconds remaining`. Build checkpoints at `T-20` through
`T-1`; never join a row at exactly `market_end_ms` back to the old market.

For a risk checkpoint with `h` seconds remaining, define the decision instant
as `market_end_ms - h * 1000` and construct state strictly as of the instant
immediately before it. A database row keyed to that same second may summarize
events after the decision instant and is not automatically eligible.

### Price distances

For a TWAP value `W` and official Price to Beat `K`:

```text
signed_distance_bps = 10,000 * (W - K) / K
absolute_distance_bps = abs(signed_distance_bps)
dollar_distance = W - K
```

Up leads when `W > K`; Down leads when `W < K`. Exact equality is a split/tie
candidate and is excluded from the binary leader state unless the official
resolution establishes how that market was treated. Official split resolutions
remain a separate cohort.

A fixed `$10` distance is converted for every market:

```text
dollar_10_in_bps = 10,000 * 10 / K
```

The main analysis uses bps because `$10` changes meaning as BTC's price changes.
Every bps result should also show its contemporaneous dollar equivalent.

Keep two `$10` questions separate:

- **Exact TWAP margin:** the latest causally available settlement TWAP is only
  `$10` from the Price to Beat. This is directly part of the lock state.
- **Spot/futures margin:** standard Chainlink spot, Binance spot, or futures is
  `$10` across while the exact TWAP remains elsewhere. This is pressure/context
  that may predict a later change; it is not a banked settlement lead.

For `T-20`, `T-10`, `T-5`, `T-3`, and `T-1`, report official-loss risk for
both situations, their overlap, and cases where spot/futures and TWAP imply
opposite sides.

### Fixed-terminal-window spot proxy

The exact TWAP observed at a checkpoint is a rolling 60-second value. It does
not isolate the part of the terminal settlement window that will still be
present at close. Add a separate, explicitly approximate decomposition from
causally available standard Chainlink spot.

Let `E` be market end, `h` the seconds remaining, `K` the Price to Beat, and
`L = 60 - h` the elapsed length of the terminal window. Under the frozen local
integration rule, define:

```text
D_hat_done(h) = mean spot deviation from K over [E-60, E-h), in bps
B_hat(h) = (L / 60) * D_hat_done(h)
F_hat_required(h) = -(60 / h) * B_hat(h)
                  = -(L / h) * D_hat_done(h), for L > 0
R_hat(h) = abs(F_hat_required(h))
flat_future_margin_hat(h)
    = B_hat(h) + (h / 60) * current_spot_distance_from_K
```

`B_hat` is the estimated contribution already present in the fixed terminal
window. `F_hat_required` is the mean future spot deviation from `K` needed to
offset it if the local path were the production input and equally time
weighted. When an exceedance curve is expressed relative to current spot,
translate this strike-relative requirement rather than comparing the curve
directly with `R_hat`. If:

```text
S_K = 10,000 * (P_t - K) / K
G = 10,000 * (mean_future - P_t) / P_t
```

then the required spot-relative move is:

```text
G_required = (K / P_t) * (F_hat_required - S_K)
```

Use the lower tail when the current exact-TWAP leader is Up and the upper tail
when it is Down. Orient the target by that primary leader, not automatically by
the sign of `B_hat` when the exact and proxy sides disagree.

Call these `spot_reconstructed_*` or `*_hat` features, never an exact banked
lead. The true identity is mechanical only for the true settlement input and
weighting. Here, known cadence gaps, unobserved same-second spot events, source
differences, and estimation residuals make it a proxy. Do not infer
partial-window error by mechanically rescaling a terminal or rolling 60-second
residual; cancellation can hide partial error. Freeze horizon/quality-specific
uncertainty from training or out-of-fold evidence and state its units. A
directly justified `D_hat_done` residual scales by `L / h` when mapped to
`F_hat_required`; a `B_hat` or final-margin residual scales by `60 / h`.
Current storage has no observed true `D_done` target, so the default is to
calibrate the resulting terminal-margin/official-loss prediction directly and
not call either scaling factor measurement uncertainty. The factors are used
only for a sensitivity interval whose target and units justify them.
Required-future-mean uncertainty can grow sharply near close. If the resulting
predictive interval straddles a declaration boundary, the proxy layer abstains
while Settlement Core may continue.

This arithmetic does not create a generic hard lock. Future BTC price has no
finite upper bound, the lower bound creates directional asymmetry, and the
production input path is unobserved. All production declarations remain
probabilistic and subject to held-out error control.

For the market favorite, reproduce the collector's normalized midpoint:

```text
up_norm = up_mid / (up_mid + down_mid)
down_norm = down_mid / (up_mid + down_mid)
```

The strict CLOB-lock cohort additionally requires both bids and asks so an
executable uncertainty interval can be evaluated; ask-only midpoint fallbacks
remain visible in coverage reporting but do not establish a lock.

### Four different flips

Do not merge these concepts:

1. **Current-leader/final-outcome disagreement:** at checkpoint `h`, the
   current valid TWAP leader differs from the official final winner. This does
   not assert that a crossing was observed later.
2. **Observed oracle side change:** consecutive valid TWAP observations are on
   opposite sides of the Price to Beat. The continuous crossing time is
   interval-censored between those observations.
3. **Market-favorite flip:** normalized CLOB probability crosses 50%.
4. **Persistent market-favorite flip:** the favorite changes and remains on
   the new side for at least two consecutive valid snapshots.

The primary settlement lock target is current-leader/final-outcome
disagreement. A parallel CLOB lock target asks whether the current fresh market
favorite loses officially and whether it crosses 50% again. CLOB flips measure
belief and market mechanics; they are not settlement truth.

Before looking at outcome labels, freeze the normalized-probability formula,
crossed-market handling, maximum component age, maximum cross-outcome skew, and
executable bid/ask uncertainty rule. Use unlabeled cadence diagnostics to set
freshness cutoffs, with the collector's 15-second gate only as an outer cap.
If quotes are incomplete, stale, crossed, or their executable uncertainty does
not establish a side, label the quote state `indeterminate`. A persistent flip
requires adjacent one-second snapshots with no missing interval, not merely
the next two available rows.

### Ex-post and prospective lock

Two lock concepts are useful but must be named separately:

- **Ex-post point of no return:** earliest checkpoint after which the observed
  valid settlement leader never differs from the official winner again. It is
  descriptive and cannot be known live. It is left-censored as "at least 20
  seconds" when already final-side at `T-20`, and unknown when fresh, healthy
  coverage does not continue through close.
- **Prospective alpha-lock:** a fresh state inside a monotone declaration region
  frozen using training/calibration data, where the rule's aggregate error on
  prospective evaluation markets excluded from rule selection has a one-sided
  95% upper bound no greater than alpha. Calibration alone does not create a
  valid individual confidence bound for each prediction.

Publish at least 10%, 5%, and 1% lock frontiers. Add 0.5% or 0.1% only when the
sample supports them; otherwise report `insufficient evidence`.

## Cohort construction

The primary cohort requires all of the following:

1. Market starts at or after `2026-08-14T00:00:00Z`.
2. Exact 60-second settlement identity, source URL, and rule version agree.
3. Resolution is complete and its reconciled rule version matches the market.
4. Official Price to Beat, official final price, and official result exist.
5. The same Price to Beat is auditable in market evidence received before the
   checkpoint and equals the later reconciled value. A strike that is visible
   only in post-resolution/backfilled evidence may be used for retrospective
   description but not as a live-model feature.
6. Winner/final-price sign is internally consistent; contradictions are
   quarantined, never silently repaired.
7. No split/tie is forced into the Up/Down model.
8. Required source evidence is causally available at the checkpoint and passes
   its freshness rules.

Historical 30-second markets may later form a separate comparison study. Never
pool them with the 60-second cohort or relabel their evidence.

If the current schema cannot prove pre-checkpoint strike availability, complete
the retrospective study but label any prospective lock rule provisional. Add
an immutable discovery-time Price-to-Beat record in a separate future collector
checkpoint before claiming live validation; do not infer its historical
availability from a later raw Gamma snapshot. That checkpoint emits a separate,
hashed, owner-approved prospective readiness record; it never rewrites the
frozen historical availability verdict.

Build six linked data-quality cohorts, with Context, Probability, and the spot
proxy branching independently from Settlement Core:

- **Settlement Core:** official result + causally valid exact TWAP.
- **Context:** Settlement Core + standard Chainlink/Binance spot and futures
  state.
- **Spot-reconstruction proxy:** Settlement Core + enough causally received
  standard Chainlink spot evidence for the elapsed terminal interval under the
  frozen cadence/gap rule. Eligibility is determined independently at every
  checkpoint. A gap observed after that checkpoint cannot retroactively remove
  it, and ineligible proxy rows remain in the operational denominator as proxy
  abstentions.
- **Probability:** Settlement Core + strict fresh Up/Down state satisfying the
  frozen two-sided quote and component-availability rules.
- **Full:** the intersection of Context and Probability for combined models.
- **Microstructure:** Full + healthy receipt-aligned microstructure.

This preserves standalone settlement, context, proxy, and CLOB calibration
samples, exposes feed-specific selection effects, and tests extra predictors
without reducing the main study to the shortest-retained series. Never require
future final-window completeness to keep an earlier live checkpoint. `Full`
does not require proxy eligibility: combined models must represent proxy
abstention explicitly and report the eligible `Full + proxy` subset rather than
silently complete-case filtering.

## Causal time alignment

Create two parallel timelines.

### Observable timeline: primary for lock and trading questions

At a decision checkpoint, include only an event or state with local receive
time strictly before that checkpoint. A provider timestamp before the checkpoint
does not make a late-received message knowable earlier.

- Use TWAP `received_wall_ns` for exact availability.
- For the standard-spot reconstruction proxy, require `received_ms` strictly
  before the checkpoint. Because `price_samples` retains only the latest
  accepted value for a provider second, it cannot reproduce an overwritten
  same-second observation that was visible earlier. If the final stored version
  was received after the checkpoint, exclude that second; the historical
  observable path is a conservative subset of the final materialization, not
  an exact reconstruction. Record this structural limitation separately from
  measured cadence.
- For probability, use the maximum raw bid/ask component receive time among
  every value used in the state. The persisted per-outcome timestamp is an
  oldest-component freshness bound, not full-state availability.
- A microstructure interval `[s, s+1000)` becomes usable only after it is
  finalized, using its finalization `received_ms`; do not use its contents at
  time `s`.
- Flow is keyed by trade time, book by event/transaction/fallback receive time,
  probability and Binance spot by local sampler second, Chainlink/TWAP by
  provider second, and futures snapshots by premium/observation second. Encode
  these semantics in a source dictionary rather than equating row key with
  availability.
- Flow/book event `received_ms` is not bucket publication time. Use a recorded
  publication/finalization time where available; otherwise gate with
  `created_at` as a conservative availability proxy or impose the configured
  source-specific flush delay. `created_at` is never an economic predictor.
- Connection/gap/health features may include only status detected by the
  checkpoint. A later session close reason or later gap record must not be
  projected backward.

### Provider timeline: secondary for oracle mechanics

Use source timestamps to reconstruct the provider-reported path and estimate
source lag. Keep it separate from the observable timeline. Report both when
answering "how soon is a futures move visible in TWAP?"

Only sources that persist both wall and monotonic clocks, currently exact TWAP
events and any enabled high-resolution raw sessions, can use monotonic ordering
to detect wall-clock steps. Cluster their wall-minus-monotonic offset to
identify reboots or clock discontinuities. Current one-second futures and
microstructure alignment relies on wall time plus interval bounds.

## Canonical last-20-second panel

Create exactly one row per resolved market per checkpoint `T-20` through `T-1`.
The market, not its 20 rows, remains the independent statistical unit.

### Core features

- Seconds remaining.
- TWAP leader, signed/absolute bps margin, and dollar margin.
- TWAP event source age, receive age, session, and nearby explicit gap status.
- Backward-looking TWAP return, velocity, and acceleration over 1/2/5/10/20
  seconds from actual irregular event times. A carried-forward second retains
  its age and never becomes a fabricated zero return.
- Chainlink spot, Binance spot, and futures distances from the Price to Beat.
- Futures-to-TWAP, spot-to-TWAP, and futures-to-spot divergences.
- Backward-looking returns, realized range/volatility, and direction toward or
  away from the strike.
- Mark/index premium, funding state, open interest, and recent OI change.

### Spot-reconstruction proxy features

- `B_hat`, signed `F_hat_required`, `R_hat`, and the flat-future terminal
  projection defined above.
- Proxy integration coverage, observed/expected seconds, maximum known gap,
  last observation age, materialization limitation, and conditional predictive
  band. Duplicate-second status is unavailable and is not a feature.
- Agreement among the exact rolling-TWAP leader, proxy banked-contribution
  side, flat-future projected side, current Chainlink spot, and official label
  only when evaluating outcomes.
- Required future mean translated both relative to the Price to Beat and to the
  current causally available spot value.

### Probability features

- Up/Down midpoint and normalized probability.
- Favorite, distance from 50%, probability momentum, and favorite persistence.
- Up and Down spreads.
- Independent outcome ages and bid/ask component asynchrony.
- Whether the CLOB favorite agrees with the current TWAP leader.

### Flow, book, and microstructure features

- Directional taker flow, imbalance, CVD, counts, and largest trade.
- Top-book and top-1/5/10 imbalance, spread, weighted-mid offset, top-10
  bid/ask notional depth, and OFI.
- Futures and spot one-second high/low/VWAP/last and within-second range.
- Perp/spot basis and spot/futures observation skew.
- Observed forced-order stress, separated by long and short side.
- Collector health, ages, lags, jitter, and connection errors.

### New derived features to test

1. **Leader-challenge pressure:** orient every momentum, flow, and book feature
   so positive means pressure against the current TWAP leader. This makes Up
   and Down effects comparable without assuming symmetry.
2. **TWAP replacement-pressure proxy:** compare current spot/TWAP context with
   observed context approximately 60 seconds earlier. Keep this rolling-window
   signal distinct from `B_hat`, which integrates only the elapsed part of the
   fixed terminal window. Neither observes Chainlink's internal constituents or
   weights.
3. **Spot-confirmation breadth:** number and strength of Binance spot,
   Chainlink spot, and spot microstructure signals that confirm a futures move.
4. **Quote-oracle disagreement:** calibrated probability confidence minus the
   empirical current-leader/final-outcome disagreement risk implied by distance
   and time.
5. **Remaining influence budget:** compare the exact current TWAP margin with
   both `R_hat` plus its target-specific predictive band and the movement
   supported by the training-only empirical response kernel. Test whether this
   improves held-out risk over exact-TWAP distance alone; do not assume it does.
6. **Required shock-budget contour:** the empirically supported
   amplitude-duration region associated with crossing the Price to Beat before
   close, without extrapolating beyond observed support or calling it a
   physical minimum.

Any feature using an estimated risk surface, response kernel, proxy residual,
or forward-excursion curve—including quote-oracle disagreement, remaining
influence, and shock budget—is generated training-only or out-of-fold. It is
never computed once from the full dataset. All price values and financial
feature calculations remain Decimal. No forward feature may use information
received after its checkpoint.

## Q1 / Workstream A: when is Up or Down locked?

At each checkpoint `h`, estimate:

```text
risk(h, x) = P(official winner differs from current TWAP leader |
               information observable by h)
```

In parallel estimate the same official-loss risk for the current fresh CLOB
favorite, plus the chance that the favorite crosses 50% again before close.
Never substitute CLOB confidence for settlement state.

### Analysis

1. Produce raw current-leader/final-outcome disagreement counts by second
   remaining and adaptive bps-distance bands, always with market counts and
   confidence intervals.
2. Fit a transparent nonlinear baseline using time remaining, signed distance,
   TWAP momentum, and volatility.
3. If the spot-proxy validation gate proceeds, fit a paired Context model and
   descriptive surface using `B_hat`,
   `F_hat_required`, `R_hat`, the flat-future projection, and proxy quality/error
   fields. Measure their held-out incremental value over exact-TWAP distance;
   keep the exact-TWAP baseline as the primary rule if they do not improve it.
4. Estimate one-sided, market-free forward-average spot excursion curves for
   each horizon and direction using training-only or out-of-fold origins. Do
   not let a response window cross a split boundary or extraction cutoff. Use
   the same receive-time, age, and carry/hold rule as a live checkpoint for the
   primary origin state; compute the forward response only after its window has
   closed. Keep provider-time origins as a secondary mechanics sensitivity.
   Use the frozen cadence/gap convention, day/regime-blocked uncertainty,
   effective support, and a nonoverlapping-origin sensitivity because adjacent
   one-second windows are not independent. Translate spot-relative moves to
   the strike using current spot-to-`K` distance and the proxy predictive band.
5. Calibrate the probabilities on a later chronological block.
6. Create time-by-distance contours for 10%, 5%, and 1% disagreement risk,
   paired with `R_hat`/banked-contribution surfaces where supported.
7. Overlay the per-market bps equivalent of `$10` on those contours.
8. Report Up-leading and Down-leading results separately, then test whether
   pooling is defensible.
9. Freeze monotone declaration regions on training/calibration data, then build
   risk-controlled selective settlement and CLOB classifiers: return Up locked,
   Down locked, or abstain. Report aggregate held-out error and coverage
   together.
10. Account for 20 horizons, two directions, and multiple risk thresholds with
   simultaneous risk-control bounds, or label results explicitly as pointwise.
   An isolated low-error bin cannot define a frontier.
11. Measure revocations out-of-sample from each market's first declaration:
   later abstention, later opposite declaration, and eventual official error.
12. Compare strict fresh CLOB favorite confidence with observed official win
    rates and the settlement-lock model in predeclared confidence bands around
    0.90/0.95/0.97/0.99. Highlight states where CLOB prints 0.97/0.99 but the
    held-out official-loss estimate is above 3%/1%, and the reverse. Report
    spread, quote age, sample count, and uncertainty so midpoint disagreement is
    not mistaken for executable profit.

The market-free curves are unconditional contextual evidence or a frozen
auxiliary predictor. They do not impute a sparse market-conditional cell,
create independent labeled flips, override `insufficient evidence`, or certify
a production lock frontier. Count observed Chainlink rows and eligible windows,
not the roughly 775,000 elapsed grid seconds as independent observations.

### Required result

A table for each final-second checkpoint giving:

- Conservative margin boundary supported for each risk threshold under the
  stated feature/quality conditions.
- Point estimate and one-sided 95% upper aggregate error bound for the frozen
  declaration region.
- Number of unique markets, UTC-day blocks, and conservative effective support
  behind the estimate.
- Up/Down symmetry result.
- Coverage/abstention rate and data-quality exclusions.
- Dollar equivalent, including a direct `$10` row.
- Exact-TWAP-only versus spot-proxy incremental performance, proxy eligibility,
  and target-specific predictive-band abstention.
- The corresponding unconditional forward-excursion estimate, clearly labeled
  contextual and never substituted for the held-out official-loss estimate.
- CLOB reliability and CLOB-versus-settlement-risk disagreement at the same
  checkpoint and data-quality state.

No cell or declaration region with inadequate selection-independent
prospective evidence may be described as production-certified locked.

## Q2 / Workstream B: where do late flips happen?

Produce three complementary timing views for intervals `19-20s` through
`0-1s`:

1. One-second recurrent transition probabilities for every observed oracle and
   CLOB side change.
2. Descriptive distributions of the **last observed oracle side change** and
   **last persistent CLOB-favorite side change**.
3. At every checkpoint, probability that the current valid leader eventually
   loses.

An ordinary first-event survival hazard is inappropriate because a market can
cross repeatedly. Report recurrent transition probabilities and last-change
distributions instead, including:

- Observed lower-bound fraction of all markets flipping in each interval, plus
  an upper/sensitivity bound that treats every unusable gap-censored path as a
  possible transition; unknown paths are never counted as nonflips.
- Conditional distribution among markets with at least one late flip.
- Up-to-Down and Down-to-Up separately.
- Single-crossing and multiple-crossing markets separately.
- Provider-path and observable receipt-path changes separately.
- Persistent and nonpersistent CLOB changes separately.

Every continuous crossing time is interval-censored between the last valid
old-side observation and first valid new-side observation, even when no
explicit gap exists. A gap widens the interval; if its bounds are unusable,
mark the change unknown. Gapped/stale states remain in the operational
population denominator as abstentions, with their frequency and official loss
rate reported separately.

The headline Q2 figure is the final-20-second time-by-exact-TWAP-margin heatmap,
paired with the two `$10` panels defined above. If the spot-proxy gate proceeds,
add a secondary time-by-`R_hat`/banked-contribution surface with proxy
eligibility, predictive bands, and disagreement against the exact rolling-TWAP
leader. If the gate abstains, publish that verdict and its evidence instead. If
supported, also show the empirical 50%-risk contour. A largest observed region
with zero reversals is descriptive only and must carry its one-sided upper
bound; never rename it an arithmetic or certain lock.

## Q3 / Workstream C: what conditions increase end-flip probability?

Make `T-20` the predeclared primary checkpoint and `T-10` the secondary
checkpoint. Each market contributes once at a checkpoint. First publish
univariate held-out risk differences/ratios within exact-TWAP-margin strata;
this prevents every condition from merely rediscovering that small margins
flip more often.

The headline multivariable model is deliberately small: a penalized logistic
model with no more than eight effective degrees of freedom, including exact
TWAP margin and the predeclared core state. Feature choice and regularization
are frozen using training/calibration data only. A boosted-tree model may probe
interactions, but it is never the headline risk rule.

Fit nested, auditable secondary models so the incremental contribution of each
data family is clear:

1. Exact-TWAP distance, with time remaining added only in pooled-horizon
   sensitivity models.
2. Add TWAP/spot/futures momentum and realized volatility.
3. Add Chainlink-spot/TWAP and futures/TWAP divergence.
4. Add probability confidence, spread, disagreement, and quote freshness.
5. Add flow and top-of-book state.
6. Add healthy microstructure, depth, OFI, basis, and observed stress.
7. Add slower mark/index/funding/OI context only where it is truly available
   before the checkpoint.

Hypotheses to test rather than assume:

- Small current TWAP margin.
- Momentum or acceleration toward the Price to Beat.
- High recent range or volatility.
- Futures and spot already on the opposite side while TWAP is still behind.
- Broad spot confirmation of a futures move.
- Directional taker flow, OFI, and shallow opposing depth against the leader.
- A contemporaneous spot/TWAP divergence associated with later exact-TWAP
  movement.
- The observed replacement-pressure proxy points against the leader; internal
  Chainlink window constituents are not observed.
- Large or small spot-reconstructed `R_hat`, a flat-future projected side that
  disagrees with the exact rolling-TWAP leader, and proxy residual uncertainty.
  These are held-out hypotheses, not assumed to be the dominant variables.
- High probability confidence that agrees or disagrees with oracle state.
- Wide, asynchronous, or stale CLOB quotes.
- Feed gaps or stale state; these may raise uncertainty and trigger abstention
  rather than predict a genuine economic flip.

Compare layers using held-out Brier score, log loss, calibration error, and
lock coverage. The published rule remains interpretable and calibrated.
Feature effects are predictive associations, not proof of causation. For every
claimed condition that increases risk, report a held-out conditional risk
difference or ratio with uncertainty and stability across temporal folds;
ablation or feature importance alone is insufficient.

## Q4 / Workstream D: futures-to-TWAP visibility lag

### Study possible with current data

Use receipt-aligned one-second spot/futures high, low, VWAP, and last prices to
identify non-overlapping seconds-scale shocks. Use the exact TWAP event receive
time for the response.

This shock path depends on the optional microstructure table and its configured
30-day retention. Record that setting and the actual surviving interval in the
extract manifest; do not assume this cohort grows indefinitely.

Row presence is not enough for this cohort. Require `collector_healthy=true`
and non-null trade-range fields. In the live audit, about 1.87% of
microstructure rows were unhealthy, about 13.47% lacked spot high/low, and
about 4.80% lacked futures high/low; shock eligibility must report these
exclusions.

Define the headline onset cohort causally. The detector fires at the first
predeclared amplitude/volatility crossing using only the quiet pre-period and
the information available through that receipt bucket. Record direction and an
interval-censored onset; one-second high/low does not reveal within-bucket order
or exact peak time. Later shocks censor the response rather than retroactively
changing entry into the cohort.

Describe, but do not select the onset cohort by, ex-post morphology:

- Peak excursion in bps over the following 1/2/5 seconds.
- Interval-censored peak bucket.
- Bounds on duration above a fraction of peak at one-second resolution.
- Reversal fraction with interval-censored reversal time.
- Integrated displacement in **bp-seconds**.

Future peak, duration, and reversal may stratify descriptive Q5 dose-response
results. They cannot define entry into the headline Q4 latency cohort or any
live predictor.

Separate:

- Futures-only dislocations.
- Moves confirmed by Binance spot.
- Moves confirmed by both Binance and Chainlink spot.
- Sustained moves matched on amplitude.
- Quiet matched control windows.

Freeze the causal onset definition, quiet-period rule, ex-post morphology,
response threshold, amplitude bands, matching/censoring rules, and model family
using training/calibration data only. Estimate the headline Q4 and Q5 response
distributions on the purged historical internal-audit block. Once those audit
results are inspected, do not revise a definition or contour and reuse that
block.

For every shock, follow TWAP for 0-90 seconds and estimate:

- First same-direction TWAP change above normal update jitter.
- Time to 10%, 50%, and 90% of the training-defined event-study impulse
  response, not fractions of each event's future-selected maximum.
- TWAP displacement at each horizon.
- Peak displacement and response retained at market close.
- Probability of crossing the Price to Beat.

TWAP updates arrive at irregular one-to-two-second cadence. Treat first-response
latency as interval-censored between the last no-response update and first
response update. The shock start is also interval-censored by its one-second
bucket. Separately report:

- **System-visible lag:** bounded local futures shock interval to local TWAP
  receipt.
- **Provider-time lag:** futures trade time to TWAP provider-event time.
- **TWAP delivery lag:** TWAP provider-event time to local receipt.

Provider-time shock onset is available only when the shock can be anchored to
a persisted trade timestamp. A within-second high/low has no stored peak
timestamp, so its provider-time onset remains an interval or is unavailable.

The observed median TWAP delivery lag of about 1.8 seconds is not the economic
response time and must not be counted as such.

Use event studies/local projections and a regularized distributed-lag model on
price changes, not price levels. Require no pre-shock TWAP response, add random
quiet-window placebos, prevent overlapping shocks from being counted multiple
times, censor the response at the next intervening shock, and use day-blocked
uncertainty. Define normal TWAP jitter and response thresholds from training or
quiet-control data only.

Keep two cohorts separate:

- A general transmission cohort may follow response through 90 seconds, even
  across a five-minute boundary.
- An end-of-market cohort censors at close. A response after close cannot be
  credited with crossing the old market's strike or changing its outcome.

Decompose the observed path:

```text
futures shock-onset interval
    -> Binance/Chainlink spot-response interval
    -> exact TWAP provider-response interval
    -> exact TWAP local receipt
```

The headline figure is a latency waterfall showing each leg and the composite
on aligned provider-time and local-receive axes. For every leg report
p10/p50/p90, event count, interval-censoring share, and panels for futures-only,
Binance-spot-confirmed, and Chainlink-confirmed shocks across predeclared
amplitude bands.

Binance futures is not an input to Chainlink. Call the result predictive lead
or observed transmission through a common BTC move, not direct futures
causation. Provider-time lag is only a sensitivity analysis because Binance
and Chainlink timestamps have different source semantics.

## Q5 / Workstream E: can a flash move TWAP materially?

Amplitude alone is insufficient. Estimate a dose-response surface over:

- Futures peak amplitude in bps.
- Duration or integrated bp-seconds.
- Spot-confirmation breadth.
- Baseline volatility.
- TWAP direction and distance from the Price to Beat.
- Seconds remaining at shock onset.

Primary output:

| Shock input | TWAP response to report |
| --- | --- |
| Peak amplitude | Median, p90, and maximum supported displacement |
| Integrated bp-seconds | Response per 100 bp-seconds |
| Duration | Peak-response and retained-response curves |
| Spot confirmation | Futures-only versus broad-market transmission |
| Time remaining | Probability of crossing the Price to Beat |

Report the empirical response ratio and predictive association for observed
futures-to-Binance/Chainlink-spot movement and for spot-context-to-exact-TWAP
movement separately before their composite. Neither is mechanical
pass-through: Binance futures is not the TWAP input, and standard Chainlink
spot is not proven to be a production TWAP constituent. Include the
largest **supported observed** TWAP displacement with its market regime,
duration bounds, confirmation state, and uncertainty; it is not a worst-case
bound. Stratify observed forced-order stress as a censored context signal,
never as total liquidation volume or proof of causation.

Keep two arithmetic cases separate:

- **General rolling-TWAP response:** an ideal equal-time-weighted 60-second
  rolling mean receives an isolated counterfactual contribution at response
  time `t` of
  `A * overlap([shock_start, shock_end), [t-60, t)) / 60` bps when its **actual
  averaging input**—not Binance futures—is additively perturbed by `A`
  fixed-denominator bps against an otherwise identical path. It reaches
  `A * d / 60` only while the rolling window contains the entire rectangular
  `d`-second perturbation. That counterfactual contribution is exact under the
  idealization, but it is not a bound on total observed `W(t) - W(t0)`: the
  common baseline rolling mean also changes as observations enter and leave,
  and the actual path may contain other movement.
- **Fixed terminal close:** under that same equal-weight and identical-path
  counterfactual, a perturbation to the actual input inside `[E-60, E)` changes
  the terminal close by its integrated displacement divided by 60, exactly
  `A * d / 60` for a rectangular perturbation wholly inside that interval, or
  the corresponding overlap expression when only part lies inside. This is the
  fixed-window identity behind `B_hat`; it is not the total change from an
  earlier rolling TWAP, not a bound when the rest of the path moves, and not
  mechanically applicable to Binance futures or the standard Chainlink spot
  proxy.

Do not assume the unobserved production inputs and weights follow this
idealization. Test the spot proxy empirically and carry its target-specific
predictive band.

The final deliverable is an amplitude-by-duration-by-confirmation matrix with a
predeclared minimum event count and uncertainty in every cell, plus empirical
predictive shock contours for moving TWAP by 0.5, 1, 2, and 5 bps and for
crossing the current Price to Beat. Do not call a contour a physical minimum,
choose any monotonicity constraint using domain assumptions and
training/calibration data only, and never extrapolate beyond observed
amplitude-duration support. If the purged historical internal-audit block
rejects that constraint, report the internal failure/inconclusive result and
wait for a new future evaluation block before refitting.

With current one-second summaries, call very short events "within-second
excursions." Their exact duration and ordering are unknown. Do not label them
100 ms flashes.

## Optional high-resolution follow-up

Precise subsecond flash duration and futures-to-TWAP onset require a populated
100 ms futures trace. The code path exists but is disabled, and the current raw
table is empty.

Begin the operational decision track in parallel with research Checkpoints 1-2
so avoidable delay does not discard future subsecond history. This calendar
change does not authorize capture and does not weaken the Phase 4 gate.
Partition-boundary creation, 72-hour expiration, and sustained raw-relation
budget enforcement remain unproven production risks until the explicitly
deferred Phase 4 validation is deliberately run and accepted.

Parallel operational sequence:

1. Obtain separate authorization for a futures-only Phase 4 production canary;
   do not enable raw capture merely because the analysis has started.
2. Before activation, freeze the acceptance plan and execute every safely
   testable preflight: capacity, critical-path health, rollback readiness,
   next-partition availability, and monitoring/alert visibility. All must pass
   before capture is enabled; record the result.
3. Run the bounded futures-only 100 ms Phase 4 canary for long enough to cross a
   future partition boundary and at least one configured 72-hour
   retention-expiry cycle. During it, demonstrate expired-partition removal and
   sustained budget enforcement, monitor drops/sessions/storage, and
   continuously verify that critical futures Redis, snapshot, flow, and book
   paths remain healthy.
4. Accept Phase 4 or roll back. Before acceptance, canary rows are engineering
   QA only, and a short healthy interval cannot be used to claim retention or
   partition validation.
5. Preserve the exact durable TWAP event stream as the response path and analyze
   only the intersection of healthy futures and TWAP sessions.
6. Only after Phase 4 acceptance may canary rows become exploratory/training
   evidence. Freeze subsecond shock/response definitions afterward and reserve
   a later bounded capture window for validation.
7. State the limited retention regime and rerun across multiple bounded windows
   before generalizing.

## Statistical validation

The present sample is useful but late flips are likely rare. Twenty checkpoints
per market do not create 20 independent observations.

Required safeguards:

- Freeze the historical internal-audit split as: training from collection
  start through `2026-08-21T23:59:59.999Z`, calibration on `2026-08-22`, and
  a historical internal-audit block on complete markets from `2026-08-23`
  through `2026-08-24`. Exclude the partial `2026-08-25` day. This establishes
  feasibility and finds defects; because those labels already existed when the
  plan was written, it cannot certify a production 1% rule. Never random-split
  rows.
- The follow-up feedback audit inspected boundary alignment and local-spot
  reconstruction behavior across the historical span. Therefore first-after
  alignment and `spot_reconstructed_*` feature choices are already exploratory
  hypotheses, even if their code is later fit on the nominal training block.
  The nominal historical internal-audit block is not selection-independent
  confirmation of those choices; require the later prospective block for a
  production claim.
- Keep all rows from a market and preferably a UTC day in one split.
- For Q4/Q5, purge 150 seconds on each side of every train/calibration/test
  boundary, covering the maximum 60-second lookback plus 90-second response
  horizon. Recompute this purge if either horizon grows.
- Rolling-origin checks across days and volatility regimes.
- Day-blocked bootstrap or market-clustered uncertainty.
- Out-of-sample calibration curves, Brier score, log loss, and precision-recall
  metrics; accuracy alone is not useful for a rare flip target.
- Separate Up/Down results before testing a pooled model.
- Sensitivity to observable-time versus provider-time alignment.
- Sensitivity to exact versus persistent quote flips.
- Sensitivity to local-spot integration/hold conventions, elapsed-window
  coverage, empirical full-window residuals, and target-specific predictive
  bands. Never use an ex-post market-specific residual or a future gap to
  decide checkpoint eligibility.
- Fresh complete cases estimate economic relationships, but gapped/stale states
  stay in the operational denominator as abstentions. Report their frequency
  and official loss rate so missing volatile periods cannot make risk look
  safer.
- Suppress or merge sparse descriptive cells.
- Label a condition estimate with fewer than 10 outcome events in either
  comparison group as descriptive/anecdotal and exclude it from headline
  ranking. Lock declarations remain subject to the stricter aggregate upper
  bound regardless of raw event count.
- Keep official final price and winner strictly as labels, never features.
- Do not use future reversal information to define a live flash predictor.

Before prospective certification, freeze and hash the extract SQL, as-of rules,
features, model/hyperparameters, calibration, declaration regions, exclusions,
and acceptance criteria. Before inspecting any feature, response, or label from
the prospective cohort, set and record the prospective start as the next UTC
midnight after that freeze and the exclusive end exactly 14 complete UTC days
later, including the market-ID range and artifact hashes. Permit no study-data
inspection, early stopping, or refitting during the block. Use it once. If it
has too few events, first declarations, or independent day blocks, the result
is `interim/inconclusive`; do not weaken the rule. Any refit requires a newly
declared future cohort.

For a frozen lock rule with zero observed errors, the approximate 95% upper
error bound is `3/n`. Apply this only to at most one predefined or first lock
declaration per market. About 300 such declarations with zero errors are needed
even for a nominal 1% bound, and adjacent markets remain temporally dependent;
report day-block sensitivity or a conservative effective sample. A point
estimate of zero is not evidence of zero risk.

Keep collecting core data continuously and schedule descriptive reruns at
predeclared calendar intervals. More markets improve rare-event support, but
repeated peeking does not turn the same historical audit block into new
evidence. Each model refit or threshold change requires a new future validation
block.

## Execution checkpoints

Recommended analysis order:

```text
freeze extract -> settlement-evidence gate -> Q2 descriptive truth set
    -> Release 1A
    -> train/calibrate core + proxy Q1 surfaces and Q4/Q5 event-study design
    -> freeze shock, response, Q1, and Q3 definitions
    -> one purged historical Q1/Q3/Q4/Q5 internal audit
    -> Release 1B
    -> genuinely prospective lock certification

parallel, separately authorized:
Phase 4 preflight -> futures-only operational canary -> accept or roll back
    -> freeze subsecond design -> later bounded validation capture
```

This produces useful descriptive answers early without letting full-data
latency/flash discoveries leak into later predictors.

The ordered flow above controls execution. The numbered checkpoints below
describe workstream responsibilities; they do not authorize opening the
historical internal-audit block out of sequence.

### Checkpoint 1: reproducible read-only extract and QA

- Freeze the source cutoff, historical splits, SQL text, and query versions.
- Use read-only PostgreSQL queries with bounded statement timeouts and a fixed
  cutoff. Perform no analysis writes, temporary production tables, or schema
  changes.
- Stream normalized, day-chunked compressed CSV extracts over SSH for local
  analysis rather than building one opaque wide frame on the droplet. Preserve
  Decimal values as text.
- Export in source groups: (1) markets/resolutions/rule evidence; (2) exact
  TWAP events/sessions/gaps and probability raw states; (3) core spot,
  futures, flow, and book; and (4) the surviving microstructure interval.
- Preserve normalized extracts separately from all derived panels.
- Record extraction UTC time, cutoff, min/max source times, SQL/file hashes,
  row counts, cohort counts, and missingness for every chunk.
- Build a source-specific timestamp dictionary.
- Verify boundary behavior, rule identity, outcome consistency, Decimal
  precision, TWAP event cadence, and gap/session handling.
- Audit duplicate-provider-time TWAP events and freeze/test the one-value
  collapse and tied-event sensitivity rules.
- Run the settlement-evidence validation gate locally before label generation.
- Run the standard-spot proxy validation, including terminal and all-event
  rolling comparisons, and freeze only training-supported integration, cadence,
  and residual rules. Preserve every tested convention in the manifest.
- Verify the Price to Beat was available pre-checkpoint and equals the later
  reconciled value before allowing it into a live feature set.
- Test every as-of join against source-specific publication availability.
- Assert that deleting or changing every post-checkpoint row cannot change any
  checkpoint feature.
- Do not use existing derived research tables as truth or predictors.

Deliverable: data manifest, exclusion report, and canonical data dictionary.

### Parallel operational checkpoint P: Phase 4 decision and canary

- During Checkpoints 1-2, seek separate authorization and freeze the Phase 4
  preflight, acceptance, monitoring, and rollback plan.
- If authorized, run the futures-only operational sequence in the optional
  high-resolution section without weakening any retention or critical-path
  gate.
- Keep canary data out of the historical one-second test. Treat it as
  engineering QA/exploratory data and reserve a later bounded window for
  subsecond validation.

Deliverable: explicit `not authorized`, `rolled back`, or Phase 4 acceptance
record. Research proceeds at one-second resolution regardless.

### Checkpoint 2: descriptive truth set

- Build the T-20 through T-1 panel from core events.
- Calculate settlement and CLOB flip definitions independently.
- Produce recurrent transition, last-observed-change, distance, and `$10`
  tables with interval censoring and uncertainty.
- If the proxy gate proceeds, produce the secondary `B_hat`/`R_hat` surface,
  proxy coverage/residual audit, and disagreement with the exact rolling-TWAP
  path; otherwise publish its abstention verdict and evidence.
- Publish Release 1A by its predeclared deadline; later work cannot hold it.

Deliverable: Release 1A last-flip timing report, exact time-by-bps heatmap, and
either the paired spot-proxy surface or its abstention verdict/evidence.

### Checkpoint 3: calibrated lock frontier

- Train the transparent exact-TWAP baseline and the paired proxy ablation.
- Build training-only/out-of-fold market-free forward-average excursion curves
  with blocked uncertainty and keep them supplemental to labeled risk.
- Calibrate on the historical calibration day and freeze the Q1 rule without
  opening the historical internal-audit block.
- After the Q3 and Q4/Q5 definitions in Checkpoints 4 and 5 are also frozen,
  run Q1/Q3/Q4/Q5 together in the one purged historical internal-audit job
  required by the ordered flow. Treat it only as an interim feasibility check.
- After that joint audit, publish supported historical 10%/5%/1% frontiers,
  confidence bounds, error, coverage, revocations, and abstentions as interim
  results; unsupported levels say `insufficient evidence`.
- After that joint audit, and only if the complete rule remains unchanged, run
  the separate prospective block before using the word `certified`.

Deliverable: decision table answering when Up/Down is defensibly locked.

### Checkpoint 4: end-flip conditions

- Add probability, flow/book, and healthy microstructure layers in order.
- Run ablations, direction/regime splits, and challenger model.

Deliverable: ranked condition groups with calibrated effect plots and an
explicit statement that they are predictive, not causal.

### Checkpoint 5: seconds-scale futures/TWAP study

- Detect non-overlapping seconds-scale shocks.
- Estimate receipt-time and provider-time response separately.
- Produce spot-confirmation decomposition, placebos, and dose-response matrix.

Deliverable: p10/p50/p90 visibility-lag ranges and TWAP movement per amplitude,
duration, and bp-seconds—not one unsupported average.

### Checkpoint 6: optional subsecond capture decision

- Decide whether one-second bounds already answer the practical question.
- Review the separately authorized Phase 4 record. If Phase 4 passed and
  subsecond precision remains valuable, freeze the subsecond research design
  and schedule a later bounded validation capture. If it did not pass or was
  not authorized, report the one-second limit and stop.

## Acceptance criteria

The study is complete only when:

1. Every flip and lock definition has a causal timestamp rule and test.
2. Changing or removing all post-checkpoint rows leaves every checkpoint
   feature unchanged, and every as-of join respects recorded or conservative
   publication availability.
3. Every reported risk/response cell has market/event count and uncertainty.
4. The lock frontier is evaluated on chronologically held-out markets and
   reports both error and coverage; a production-certified frontier also
   passes the separately frozen prospective block.
5. Insufficient held-out support automatically produces `insufficient
   evidence`; it never relaxes the error bound or minimum cell count.
6. `$10` is evaluated as a per-market bps overlay, not a fixed universal rule,
   and exact-TWAP `$10` margin is reported separately from spot/futures `$10`
   pressure.
7. The late-flip answer separates settlement side changes from CLOB favorite
   crossings.
8. The lag answer separates economic response, publication cadence, and local
   delivery delay.
9. The flash answer separates futures-only excursions from spot-confirmed
   moves and uses amplitude plus duration.
10. No subsecond conclusion is drawn from one-second data.
11. No historical 30-second settlement evidence is pooled with 60-second data.
12. Every claimed risk-increasing condition has a held-out risk difference or
    ratio, uncertainty, and temporal-fold stability—not only feature
    importance.
13. Results can be regenerated from the extraction manifest without using
    prior derived labels or predictions.
14. The settlement-evidence gate uses exact E18 TWAP events and official
    values. Standard Chainlink spot/local averages never replace settlement
    evidence.
15. CLOB calibration comparisons require strict fresh two-sided quotes and
    report spread/age; they are not presented as executable edge by default.
16. Duplicate-provider-time TWAP events use the frozen deterministic collapse;
    tied-event sensitivity is reported, and tied oscillations are not counted
    as separate provider-path one-second flips.
17. Boundary validation separates exact-boundary events from delayed
    first-after events and never exposes a post-close event as a pre-close
    feature.
18. Every `spot_reconstructed_*` result reports its frozen integration rule,
    causal eligibility, target-specific predictive band, and held-out
    incremental value. It never uses that market's realized final residual or
    a future gap, or labels full-window residuals as partial-input measurement
    error.
19. Market-free excursion curves are split-safe, gap-aware, blocked for
    dependence, and labeled unconditional. They never fill a sparse labeled
    cell or certify a lock.
20. Rolling-TWAP response arithmetic and fixed-terminal counterfactual
    arithmetic are reported separately; neither treats a futures move as the
    mechanical TWAP input.
21. Release 1A publishes within the predeclared 120-hour default timebox. If
    the gate or Checkpoint 2 is incomplete, it publishes the failure/exclusions,
    completed descriptive results, and explicit blockers instead.
    Certification work cannot delay the descriptive release.

## Expected final outputs

- Data coverage, timestamp, and exclusion audit.
- Exact-TWAP boundary/official-value validation report and Price-to-Beat
  availability audit.
- Standard-spot proxy validation across terminal and rolling windows, including
  convention sensitivity, residual bands, coverage, and sign disagreement.
- Canonical market/checkpoint data dictionary.
- Oracle and CLOB recurrent-transition and last-observed-change plots.
- Time-by-bps reversal-risk heatmap with separate exact-TWAP `$10` and
  spot/futures `$10` panels.
- Paired `B_hat`/`R_hat`/flat-future surfaces with proxy abstention and exact
  rolling-TWAP disagreement, plus supplemental market-free forward-excursion
  curves.
- Up/Down 10%/5%/1% lock frontiers with error and coverage.
- Strict CLOB calibration and CLOB-versus-settlement-risk disagreement table.
- Conditions/ablation report with out-of-sample calibration.
- Futures-to-spot-to-TWAP-to-delivery latency waterfall and response
  distributions.
- Flash amplitude/duration/confirmation dose-response matrix.
- Futures-to-spot and spot-context-to-TWAP empirical response-ratio ranges,
  plus the largest supported observed TWAP displacement with context.
- Required shock-budget contours by seconds remaining.
- Clear list of conclusions supported now versus questions requiring new
  high-resolution collection.
