## Bottom line

**Yes, 2.0 s or 2.5 s could outperform 3.0 s.** The evidence points in that direction because 3.0 s is both the shortest candidate tested and the best candidate on calibration and holdout. That is a classic boundary result: the optimum may lie below the tested range. It is not proof, however; shortening the horizon may eventually reduce performance because fewer Chainlink updates arrive before the target.

The core projection arithmetic and historical causal pairing are mostly sound. I did not find an obvious look-ahead error in the raw replay or an arithmetic error in the catch-up formula. The chronological selection and immutable artifact design are also unusually careful.

I did find:

1. One **confirmed directional-metric bug** that makes the approximately 99% “accuracy when action” figure misleading.
2. A **replay-versus-live timing mismatch** capable of making historical results more optimistic than live results.
3. A **live outcome-pairing limitation** that can select the wrong target value when a latest-value cache overwrites an intermediate Chainlink update.
4. Several measurement and statistical risks that do not necessarily bias the point estimate, but substantially overstate how much independent evidence you have.

I would keep 3.0 s as the provisional shadow model for now, but treat the directional results as invalid until corrected and run a new lag-selection policy that includes shorter candidates.

## What the current evidence says

These are the common-cohort results used by selection:

|   Horizon | Calibration MAE skill | Calibration RMSE skill | Holdout MAE skill | Holdout RMSE skill | Holdout model MAE vs baseline |
| --------: | --------------------: | ---------------------: | ----------------: | -----------------: | ----------------------------: |
| **3.0 s** |            **16.74%** |             **30.73%** |        **28.21%** |         **34.86%** |          **$2.265 vs $3.155** |
|     3.5 s |                11.75% |                 22.53% |            21.83% |             27.00% |              $2.787 vs $3.566 |
|     4.0 s |                 4.32% |                 13.18% |            13.28% |             17.79% |              $3.433 vs $3.959 |

The ordering is monotone in both datasets: **3.0 > 3.5 > 4.0** on both MAE and RMSE skill. That is meaningful evidence for testing lower horizons, although it cannot tell us whether the optimum is 2.5 s, 2.0 s, or somewhere else. Also remember that each horizon predicts a different target time; comparing raw MAE alone would favor shorter forecasts simply because they predict less far ahead. Your use of skill against a horizon-matched no-change baseline is the better comparison. 

I independently recomputed the integrity chain:

* Both supplied JSON files’ SHA-256 values match their filename suffixes.
* The configuration digest recomputes exactly.
* The selection fingerprint recomputes exactly.
* The supplied replay report’s hash is exactly the holdout hash recorded in the selection artifact.

So the files are internally consistent and do not appear to have been accidentally substituted or modified.  

## What is being done correctly

### The projection formula is coherent

The model anchors a Chainlink price to the latest causal futures observation at or before `chainlink_received_ms - lag`, calculates the subsequent futures return, and applies that return to the Chainlink anchor:

[
\widehat C_{t+L}
================

C_{\text{anchor}}
\left(
1+\beta\left(\frac{F_t}{F_{\text{reference}}}-1\right)
\right)
]

For a multiplicative lagged-price relationship, this is mathematically reasonable. The use of a futures reference at or before the target anchor time is conservative and avoids selecting a later futures value. 

### Raw replay outcome pairing is causal

The historical replay scores against the last Chainlink event whose receive time is at or before the target. It does not select the first event after the target or use a future value. The no-change baseline is paired at the same horizon, and cross-model ranking uses the same generated-time common cohort.  

### The selection procedure avoids obvious holdout abuse

Selection ranks candidates only on calibration, freezes the calibration winner, and uses the later holdout only as a pass/fail confirmation. It does not rerank using the holdout and does not fall back to another model if the frozen winner fails. That is the right general structure. 

### Artifact activation is appropriately fail-closed

The artifact code checks exact schemas, candidate specifications, policy content, report provenance, configuration hashes, the selection fingerprint, immutable-file properties, and the selected model/configuration relationship. It also rejects unsupported candidate sets instead of silently accepting them. 

## Confirmed bug: the directional metrics exclude false actions on neutral outcomes

The replay code increments all three directional counters only inside:

```python
if actual_direction != "neutral":
```

That means:

* `directional_eligible` counts only rows where the **actual** move was non-neutral.
* `directional_action` counts model actions only inside those already-moving rows.
* A predicted up/down action when the actual outcome was neutral is completely omitted.
* `directional_accuracy_when_action` therefore does **not** divide by all model actions.

The reported metric is effectively:

> Directional correctness among rows where the actual outcome moved and the model also emitted a direction.

It is **not**:

> The percentage of all model actions that were correct.

That distinction is large. The reported 3.0 s holdout value of about 99.46% cannot be interpreted as action precision. `directional_accuracy` is closer to directional recall, while `directional_action_coverage` is action coverage conditional on an actual move—not overall action coverage. 

This is especially important because the 3.0 s holdout had:

* 144,959 actual-neutral rows out of 171,669, or **84.44%**.
* Model MAE of approximately **$1.831** in that slice versus baseline MAE of **$1.444**.
* MAE skill of **−26.82%** in the neutral/small slice.

Calibration was even quieter: about 95.15% of rows were neutral, with approximately −19.64% neutral-slice MAE skill. The overall improvement is generated by large gains on moving outcomes, while the model is worse during the much more numerous quiet outcomes. 

This does not invalidate the MAE/RMSE selection—the directional fields do not affect ranking—but it invalidates the directional headline.

The replacement should be a full three-by-three confusion matrix over `up`, `neutral`, and `down`, followed by at least:

* Overall three-class accuracy.
* Action precision: correct up/down predictions divided by **all** predicted up/down actions.
* Move recall: correctly predicted up/down outcomes divided by all actual up/down outcomes.
* False-action rate on neutral outcomes.
* Wrong-direction rate.
* Overall predicted-action frequency.

The existing aggregate report cannot reconstruct these metrics. The per-forecast rows or raw events must be rescored.

## Replay results can be more optimistic than the live worker

The replay exposes each raw event at its raw receive timestamp. Its own report explicitly notes that raw receive time occurs before parsing and Redis publication latency. The live worker, by contrast, reads Redis and stamps `generated_ms` only after the `MGET` returns.  

Therefore, the replay assumes a futures or Chainlink value is available earlier than the live worker can actually consume it. This is not future-data leakage in the conventional sense, but it is an **availability-time mismatch** and can inflate historical performance relative to production.

There is also a phase difference:

* Replay polls at ideal exact 100 ms boundaries.
* Replay generates evaluations at exact 500 ms epoch-aligned boundaries.
* Live execution sleeps to a boundary, then incurs scheduling, Redis, parsing, and Python runtime latency.
* A slow live iteration may skip one or more ideal polls.

For a 3–4 second model, tens or hundreds of milliseconds may already matter. For a 2.0 s model, the same fixed latency is a larger fraction of the horizon.

The robust solution is to replay an explicit `available_to_shadow_worker_ms`, ideally measured at Redis publication completion, rather than raw socket receive time. At minimum, run latency sensitivity scenarios such as raw receive plus 25, 50, 100, 200, and 300 ms, and run all evaluation phase offsets rather than only the epoch-aligned phase.

## The live evaluator can pair a forecast with the wrong target value

The live evaluator does not receive every Chainlink event. It periodically samples a latest-value cache and stores only the identities it happened to observe. At maturation, it searches that sampled history for the latest observed value received at or before the target.  

Consider this sequence:

* Last observed Chainlink value: received at 900 ms.
* Forecast target: 1,000 ms.
* Chainlink updates at 950 ms.
* Chainlink updates again at 1,050 ms, overwriting the cache.
* Worker next reads the cache at 1,100 ms.

The evaluator sees the 1,050 ms value, rejects it as being after the target, and falls back to the 900 ms value. The correct “latest known at target” value was the missing 950 ms update.

The code recognizes that a latest-value cache cannot reconstruct overwritten values, but it resets only when the observation-time gap exceeds a configured threshold. An intermediate update can be missed even during normal 100 ms operation, and exactly 200 ms does not trigger the current `> max_observation_gap_ms` condition. 

This can make live errors either better or worse; its bias direction is not fixed. It does mean live evaluation cannot be treated as authoritative without one of these changes:

* Consume an append-only Chainlink stream.
* Include a monotonic event sequence in the cache and invalidate targets whenever a sequence jump proves updates were missed.
* Query an event store for `MAX(received_ms) <= target_ms`.
* Persist every Chainlink update directly into the evaluator’s history.

## Published signals and evaluated signals use different future-timestamp rules

The engine permits an input timestamp to be as much as `max_future_skew_ms`—250 ms in the artifact—ahead of `generated_ms`. That is presumably intended to tolerate clock differences between hosts. The live evaluation scheduler, however, invalidates any forecast input whose `received_ms` is even 1 ms after `generated_ms`.  

Consequently, a signal can be:

* Published as valid by the live engine.
* Simultaneously marked invalid by live evaluation.

That creates a population mismatch: the performance monitor is not necessarily evaluating the same signals that consumers received.

The preferred fix is to base causality on timestamps in one clock domain—for example, the worker’s cache-observation or cache-publication time. Otherwise, apply the same documented skew rule in both the engine and evaluator and record the measured host clock offset.

## Queue overflow can break cross-model cohorts

The scheduler produces records for every candidate, but the collector offers them to the writer individually. When the bounded queue fills, the writer drops the oldest individual record. Backend rejections and deferred-record requeues can also result in record-level loss.  

If any dropping occurs, the persisted database may contain one model from a generated-time cohort but not the others. Cross-model live comparisons will then be biased unless they explicitly require a complete cohort.

The safer design is to queue an entire generated-time candidate cohort atomically. At minimum, every row should carry a cohort identifier, and analysis should include only cohort IDs containing every expected model. The writer’s offered, persisted, rejected, deferred, dropped, and queue-high-water counters need to be part of every live evaluation report.

## Statistical and model-design risks

### The raw row count greatly overstates independent evidence

Forecasts are created every 500 ms while horizons are 3–4 seconds, so adjacent predictions overlap heavily. Chainlink values are also reused across multiple targets. The selection policy correctly acknowledges that the rows are autocorrelated, but it only changes how win/loss frequency is interpreted. The MAE and RMSE gates still require merely that estimated skill be greater than zero; there is no block confidence interval or minimum meaningful effect. 

The approximately 171,669 holdout rows are therefore not 171,669 independent tests. The current point estimates are substantial, so this is not evidence that the effect is imaginary. It does mean conventional row-level standard errors would be severely overconfident.

Use paired moving-block bootstrap intervals, daily/session-level skill, and a non-overlapping-origin sensitivity analysis. The bootstrap block must be longer than both the forecast horizon and the update-dependence period.

### The evidence covers only a narrow contiguous period

The artifact contains two adjacent calibration windows and one later 24-hour holdout, covering roughly three consecutive days. A single day can be a volatility or update-cadence regime rather than a representative holdout. 

The difference between calibration and holdout illustrates this: the holdout had many more moving outcomes and consequently much higher overall skill, while both periods showed negative quiet-outcome skill.

### The 3,000 ms allowable reference gap is very broad

The replay configuration permits the futures anchor reference to be as much as 3,000 ms older than the desired reference time. For the selected 3.0 s model:

* Reference-gap p50: 130 ms.
* Reference-gap p99: 1,419 ms.
* Maximum: 2,966 ms.

The target Chainlink value itself had:

* Age at target p50: 515 ms.
* Age at target p99: 1,605 ms.
* Maximum: 5,498 ms.

Thus the system predicts the latest known cache value, which is frequently hundreds of milliseconds old, rather than a contemporaneous underlying price. 

This matters especially for a 2.0 s candidate: an allowed 3.0 s reference gap would exceed the candidate lag. A nominal 2.0 s model could use a futures reference nearly 5.0 seconds before the Chainlink anchor. I would run reference-gap sensitivity at approximately 250, 500, 1,000, and 3,000 ms and require the chosen gap to be comfortably smaller than the chosen lag.

### Beta 1 appears too aggressive in quiet periods

The full futures return is currently passed through with `beta=1`. Given the persistent negative skill in actual-neutral/small outcomes, a shrinkage beta below 1, or a no-change/deadband rule for small predicted moves, may yield more improvement than changing the lag alone.

Any gate must use information known at forecast time—predicted move size, futures volatility, reference gap, input age, or similar. It cannot use the existing `actual_direction` or `actual_move_size` slices, because those are outcome-conditioned diagnostics.

## How to test 2.0 s and 2.5 s correctly

### 1. Treat the existing holdout as calibration

Now that 2.0 s and 2.5 s have been proposed after inspecting the existing holdout, that holdout is no longer untouched for the expanded model family. Your own policy already states that previously inspected holdouts must become calibration and that a new later holdout is required after a policy revision. 

### 2. Fix the measurement defects first

Before generating a new selection artifact:

* Replace the directional metrics with a full confusion matrix.
* Make historical availability reflect live availability.
* Repair or replace the live latest-cache target reconstruction.
* Harmonize future-skew handling.
* Make persisted cohorts atomic or enforce complete-cohort analysis.

Otherwise, the shorter-horizon experiment will measure a mixture of model quality and timing artifacts.

### 3. Pre-register a candidate grid that is not bounded at 3.0 s

The replay supports up to five lag candidates, so an exploratory grid could be:

```text
1500, 2000, 2500, 3000, 3500 ms
```

Including 1.5 s prevents another immediate lower-bound winner. Alternatively, use 2.0 s as the explicit lower bound only when there is a documented product reason that forecasts shorter than 2.0 s are not useful.

The replay code can accept this grid, but both selection and artifact activation hardcode the current 3.0/3.5/4.0 s policy. Supporting shorter candidates requires a new selection schema or policy version and corresponding artifact validator changes.   

### 4. Use current data only for model development

On the existing raw data, explore:

* Lag.
* Beta/shrinkage.
* Predicted-move deadband.
* Reference-gap limit.
* Live-availability delay assumptions.
* Evaluation phase offsets.

Then freeze the complete formula and thresholds before the new holdout starts. Do not select lag on one holdout and beta on a second holdout; all tuning belongs in calibration.

### 5. Evaluate the frozen model on new independent windows

The new holdout should report:

* MAE and RMSE skill.
* Paired block-bootstrap confidence intervals for the error differences.
* Per-day and per-session skill.
* Quiet, moving, volatility, reference-gap, input-age, target-age, reconnect, and expiry slices.
* Full-time operational performance, including invalid periods and the production fallback behavior.
* Complete directional confusion metrics.
* Results with non-overlapping forecast origins.

Several weeks spanning weekdays, weekends, quiet periods, volatile periods, and reconnects would be much more informative than another adjacent 24-hour window. The stopping criterion should be stability of block-level estimates, not merely reaching a large number of overlapping 500 ms rows.

## Additional data needed

The current aggregate artifacts are enough to review the selected 3.0 s result, but they are not enough to calculate 2.0 s or 2.5 s performance or repair the directional metrics.

The highest-priority data is:

1. **Raw events for all three evidence windows.** For futures: connection ID, bucket key, close, last raw receive wall and monotonic timestamps, source/trade timestamp, sequence or trade ID, and event count. For Chainlink: price, receive wall and monotonic timestamps, provider event timestamp, receive sequence, and connection ID. Include the full feed-session counters and boundaries.

2. **The two calibration replay reports referenced by the selection artifact.** Only the later holdout replay report was supplied, so I could verify its hash and metrics but could not independently verify or reparse the two calibration reports.

3. **End-to-end live timing telemetry.** Record raw socket receive, parse completion, aggregation completion, Redis `SET` start/end, worker `MGET` start/end, `generated_ms`, signal publication completion, process/host identifiers, and clock-offset/NTP measurements.

4. **Live evaluation rows and health counters.** Include offered, persisted, dropped, rejected, deferred, queue-high-water, observation-gap, regression, and skipped-sequence counts, plus a generated-time cohort ID.

5. **Surrounding source and tests.** A complete audit also needs `market.py`, `live_cache.py`, the raw-capture writers and schemas, the evaluation database backend, migrations/constraints, and the relevant unit/integration tests. Those modules determine whether the timestamps and atomicity assumptions made by these six files actually hold.

6. **Execution data, if this will drive trading.** Bid/ask, fees, slippage, order latency, available size, and fill outcomes are required. The selection artifact correctly states that the current result is a price forecast, not an execution, settlement, probability, or market-close forecast. 

## Recommended decision

The defensible conclusion today is:

* **3.0 s is the best tested model under the current raw-replay definition.**
* **2.0 s and 2.5 s are strong candidates for the next experiment, but are not yet supported by results.**
* **The approximately 99% directional figure should not be used in any decision or presentation.**
* **The model should remain provisional and shadow-only while the timing and live-evaluation defects are corrected.**
* **A new policy version should freeze a shorter-horizon candidate grid on all existing data, followed by a genuinely new holdout with block-level uncertainty and live-faithful timing.**
