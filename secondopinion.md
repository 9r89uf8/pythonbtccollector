## Bottom line

Proceed with the **1.5, 2.0, 2.5, 3.0, 3.5-second** family. It is a sensible, compact grid for a lag-only experiment, provided that:

1. A complete five-candidate calibration is run first under the new frozen configuration.
2. Only the resulting frozen calibration winner is tested on the later 24-hour holdout.
3. The incumbent 3.0-second shadow primary remains active unless that single challenger passes every holdout gate.
4. A holdout failure never causes selection of the runner-up.

I would **not** describe the current source as fully fixed yet. Most of the previously identified defects are substantially addressed, but there are still two important integrity gaps and one experiment-support gap:

* Replay timing remains assumption-based rather than measured.
* Database idempotency does not verify that conflicting existing rows are identical.
* The selector cannot presently combine the required delay/phase robustness grid, and the selector, validator, and public reporting still hardcode the old family.

Also, because you have now specified a **24-hour test**, remove all “multi-week holdout” language from the new policy. One 24-hour window can support a shadow-primary decision, but it should not be described as evidence of multi-week regime stability.

---

## 1. Grid and causal scoring

The grid is appropriate.

The historical evidence is strongly monotonic: the old calibration MAE skill fell from about **16.7% at 3.0 seconds** to **11.7% at 3.5** and **4.3% at 4.0**; the inspected old holdout showed the same ordering at approximately **28.2%, 21.8%, and 13.3%**. RMSE skill was also monotonic. 

The old replay diagnostics reported Chainlink receive-minus-source latency around **1,438 ms at p50** and **2,200 ms at p99**, with median Chainlink event spacing around 1.02 seconds. That makes 1.5 seconds a defensible lower endpoint for this first expansion. It does not prove that 1.5 is optimal, but it gives the lower boundary an operational interpretation. 

The scoring design should be:

* Candidate lags and horizons are exactly `1500, 2000, 2500, 3000, 3500` ms.
* `beta=1` for every candidate.
* Use the same small reference-gap limit for all candidates; **250 ms** is the natural choice because it is already the current replay default.
* Candidate comparison uses the same generated times where the 3.5-second target fits and every candidate is valid.
* Each candidate is scored against its own no-change forecast at its own target time.
* Rank on **MAE skill versus matched no-change**. Use RMSE skill as an eligibility and confirmation metric, not as a way to rescue a practically tied MAE result.
* Do not rank candidates using raw MAE across horizons: the targets have different intrinsic difficulty.
* Do not use per-row win rate or directional accuracy for selection.

A boundary rule is essential: **if 1.5 seconds wins calibration or holdout, record it as a boundary winner. Do not add 1.0 seconds or rerun the same holdout under a wider family.**

One important procedural limitation is that the supplied artifacts cannot select among the expanded family. They contain only 3.0/3.5/4.0-second candidates and use the old v2 timing configuration, including `max_future_skew_ms=250` and `reference_max_gap_ms=3000`. They are legitimate design/calibration evidence, but they cannot be pooled directly into the new five-candidate ranking.  

You therefore need at least one **pre-holdout v4 replay containing all five candidates**. If suitable retained raw data exists, use it. Otherwise collect an earlier calibration window, inspect it, freeze the winner, and only then begin the untouched 24-hour holdout.

---

## 2. Current-source defect audit

Line references below refer to the [audited source archive](sandbox:/mnt/data/chainlink-second-opinion-20260716-190547.zip).

| Previously reported defect           | Current finding                                                                                                                                                                                                                                                                                                                                                                    | Assessment                                                                                                                                                                                                               |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Incorrect directional metrics        | `shadow_signal_replay.py:126–205` constructs the complete 3×3 up/neutral/down matrix, including false actions on neutral outcomes and opposite-direction actions. `shadow_signal_selection.py:704–730` validates and pools it.                                                                                                                                                     | **Fixed in the v3 replay/selection path.** The supplied v2 artifacts predate this fix. Public reporting still does not expose the new matrix.                                                                            |
| Replay/live timing mismatch          | Replay now has independent source delays and five valid 100 ms phases (`shadow_signal_replay.py:252–352`, `1525–1545`, `1593–1601`). The report itself explicitly says these are assumptions, not measured publication completion (`1404–1413`).                                                                                                                                   | **Partially fixed.** The simulator is much better, but it remains sensitivity analysis rather than measured live equivalence.                                                                                            |
| Missed latest-cache Chainlink events | The publisher assigns an epoch and monotonically increasing accepted-event sequence (`polymarket_chainlink_collector.py:93–112`, `161–169`, `715–745`). The scheduler invalidates history on gaps, regressions, identity conflicts, epoch changes, and metadata loss (`shadow_signal_evaluation.py:791–923`) and requires bounded target-time sequence confirmation (`1032–1075`). | **Strongly fixed by design.** This is the most convincing of the repairs.                                                                                                                                                |
| Future-skew inconsistency            | New replay defaults to zero (`shadow_signal_replay.py:264`), and the v3 selector rejects anything else (`shadow_signal_selection.py:934–936`). Activation passes the validated artifact value into the engine.                                                                                                                                                                     | **Fixed for the v3/v4 supported path.** The engine constructor still defaults to 250 ms and historical v2 artifacts remain at 250 ms. Make the constructor argument required or default it to zero for future hardening. |
| Non-atomic candidate persistence     | Scheduling, maturation, queueing, retry, permanent-error isolation, and retention operate on full cohorts. Transactions cover a complete insert attempt.                                                                                                                                                                                                                           | **Mostly fixed, not completely.** See below.                                                                                                                                                                             |
| Candidate-family hardcoding          | The replay permits five arbitrary lags, but `shadow_signal_selection.py:15–27`, `shadow_signal_artifact.py:18–36`, and `shadow_signal_reporting.py:23–47` still define only 3.0/3.5/4.0.                                                                                                                                                                                           | **Not fixed. This is a blocker for the expanded policy.**                                                                                                                                                                |

### Residual persistence problem

`db.py:111–117` performs:

```sql
ON CONFLICT (model_version, generated_ms, horizon_ms) DO NOTHING
```

The backend then labels the whole attempted cohort as persisted whenever no exception is raised (`db.py:918–957`). That is safe for ordinary retries when the existing rows are identical. It is not fail-closed if a pre-existing conflicting row has different provenance or contents: the conflict is silently ignored, and the cohort can still be reported as persisted.

The primary key also omits selection provenance (`schema.sql:423–483`), so two policy versions cannot coexist at the same model/version/generated-time/horizon key.

The simplest correction is:

1. Keep the single transaction.
2. Insert the whole cohort.
3. Read back all rows for that cohort identity.
4. Verify the exact candidate set, provenance, and immutable values against the attempted rows.
5. Commit only if all rows are present and identical; otherwise raise and quarantine the cohort.

Adding `selection_artifact_sha256` to the unique key would also prevent cross-policy collisions. This issue is not a blocker if the 24-hour decision uses only immutable raw replay evidence, but it should be fixed before live PostgreSQL evaluation rows are treated as authoritative selection evidence.

### Test status

After installing the declared requirements under Python 3.13.5:

```text
749 passed, 1 failed
```

The failure is `tests/test_deployment.py::test_redis_server_is_documented_as_loopback_only`, because the archive does not contain the referenced `OPERATIONS.md`. I did not run a live Redis/PostgreSQL integration test.

The requirements are unpinned, and the archive has no Git metadata. The audited archive SHA-256 is:

```text
21fabc91b0db38e6b7d306a16a73644b20dc69953c6a0a737f0a6607632dd300
```

That is adequate for identifying this review copy, but the eventual holdout freeze needs a new source hash plus exact dependency hashes.

---

## 3. Minimal new versioned policy

Use a new immutable policy rather than modifying v2 or v3 in place. A suitable name is:

```text
chronological_holdout_v4_lag_grid_24h
```

Its core should be frozen as follows:

```yaml
schema_version: 4
policy_version: chronological_holdout_v4_lag_grid_24h

candidate_set:
  - catchup_ratio_l1500_b100
  - catchup_ratio_l2000_b100
  - catchup_ratio_l2500_b100
  - catchup_ratio_l3000_b100
  - catchup_ratio_l3500_b100

incumbent: catchup_ratio_l3000_b100
beta: 1
lag_is_horizon: true

poll_ms: 100
evaluation_interval_ms: 500
evaluation_phase_offsets_ms: [0, 100, 200, 300, 400]
reference_max_gap_ms: 250
max_future_skew_ms: 0

common_cohort:
  same_generated_ms: true
  maximum_horizon_target_eligible: true
  all_candidates_valid: true

ranking:
  source: canonical_timing_common_cohort
  primary: higher_mae_skill_vs_matched_no_change
  rmse_is_required_gate: true
  minimum_calibration_mae_lead: 0.01

holdout:
  duration_ms: 86400000
  exact_single_window: true
  rerank: false
  fallback: false
  dynamic_switching: false
```

All other settings—staleness limits, history retention, neutral band, volatility slicing, expiry slicing, and session rules—must be copied unchanged from one designated v3 calibration configuration. Since this is a lag-only experiment, none of them may vary by candidate or be changed after calibration results are inspected.

The artifact must additionally contain:

* The exact holdout `[start_ms,end_ms)` fixed before collection.
* Every calibration input hash and range.
* A declaration that every previously inspected result is calibration-only.
* The frozen calibration winner.
* The incumbent 3-second model.
* The complete timing-scenario manifest.
* The common-cohort and target definitions.
* The non-overlapping-origin rule.
* Bootstrap block length, replicate count, confidence level, PRNG algorithm, and seed.
* Every numerical gate below.
* The source archive or Git tree hash.
* Python version and an exact dependency lock hash.
* `schema.sql` hash.
* Raw-input manifest hash.
* Per-report and ordered-loss-ledger hashes.
* Atomic create-once/no-overwrite semantics.
* Explicit `holdout_reranking=false` and `fallback_after_holdout_failure=false`.

Keep the existing v2/v3 validator profiles for the currently active 3-second artifact. Add v4 as a separate profile; do not mutate old historical expectations.

A small central module such as `shadow_signal_policy.py` should contain the versioned candidate specifications. The selector, artifact validator, and public reporting should all import that registry. This is simpler and safer than maintaining three independent hardcoded lists.

---

## 4. Timing-delay and phase robustness

The current selector cannot implement the documented complete timing grid: it hashes the entire replay configuration and then requires every supplied calibration and holdout report to have one identical configuration digest (`shadow_signal_selection.py:768–769`, `1138–1142`). Different phase offsets or availability delays necessarily produce different digests.

For a simple but meaningful v4 robustness design, use three fixed delay scenarios:

| Scenario                | Futures delay | Chainlink delay | Role                          |
| ----------------------- | ------------: | --------------: | ----------------------------- |
| Canonical               |        100 ms |          100 ms | Ranking and primary inference |
| Futures-slower stress   |        200 ms |          100 ms | Robustness gate only          |
| Chainlink-slower stress |        100 ms |          200 ms | Robustness gate only          |

Run all five phase offsets for each scenario: **15 replay configurations from the same raw 24-hour input**. This adds no extra data collection and can be fully automated.

The rules should be:

* Pool the five canonical phases because they represent disjoint generated times on the 100 ms grid.
* Do not pool canonical and stress scenarios; they reuse the same origins.
* Never choose the delay scenario or phase that looks best.
* Rank only on the canonical pooled result.
* Require positive performance in every canonical phase and in both pooled stress scenarios.
* Record a single `raw_input_manifest_sha256` in every replay report so the selector can prove that all 15 configurations used identical raw evidence.

The 100/200 ms values are deliberately simple one-poll/two-poll sensitivity assumptions. They are not estimates of true publication latency. Add actual collector-receive, Redis-write-completion, and first-worker-observation timestamps for a later policy revision, but do not use measurements from this holdout to change v4.

---

## 5. Time-ordered evidence and statistical method

The current replay JSON has exact aggregate sufficient statistics but no ordered loss series. Aggregate JSON alone cannot support a moving-block bootstrap or deterministic non-overlapping-origin analysis.

Have replay optionally write a compressed ordered ledger with one row per candidate and generated time:

```text
raw_manifest_sha256
scenario_id
phase_offset_ms
generated_ms
target_ms
common_cohort_member
model_version
horizon_ms
valid
outcome_status
model_absolute_loss
baseline_absolute_loss
model_squared_loss
baseline_squared_loss
market_id
common_feed_segment_id
```

Sort and hash it by:

```text
scenario_id, generated_ms, horizon_ms
```

### Non-overlapping inference origins

Use all common rows for the descriptive MAE/RMSE point estimates, but use a deterministic subset for inference:

* Fix one origin every **4,000 ms**.
* Origin times are determined from the holdout start before any data is seen.
* Do not shift an origin to the next valid time; an invalid scheduled origin remains missing.
* The maximum candidate horizon is 3.5 seconds, so 4-second spacing prevents forecast-interval overlap.
* A complete 24-hour window contains at most **21,600** such origins.

Residual serial dependence remains even after removing horizon overlap, so use a block method rather than treating origins as independent. The forecast-comparison literature frames the relevant object as a time-ordered loss differential, and block bootstrap methods were developed specifically to retain local dependence in stationary sequences. ([University of Wisconsin User Portal][1])

Freeze:

```text
bootstrap: circular moving-block bootstrap
scheduled-origin spacing: 4,000 ms
block length: 15 minutes
replicates: 10,000
confidence bound: one-sided 95%
reported lower bound: 5th bootstrap percentile
seed: derived deterministically from the policy artifact hash
```

For each bootstrap replicate, resample the model and baseline losses together and recompute the skill ratio. For comparison with the incumbent, synchronously resample the challenger and 3-second block vectors and recompute the difference in their matched-baseline skills.

### Daily and session reporting

For the 24-hour holdout, produce:

* One exact UTC-day aggregate.
* Twenty-four UTC-hour aggregates.
* One aggregate per 5-minute Polymarket market/session.
* Results by timing scenario and phase.
* Candidate-specific and common-cohort coverage.

Store counts and sums of absolute and squared losses. Recompute daily/hourly/session MAE, RMSE, and skill from those sums. Do **not** average session RMSE values, session skills, or reservoir medians.

Hourly and 5-minute results should be diagnostics, not independent promotion gates. Requiring every hour or every session to be positive would create a noisy multiple-testing system and is unnecessary for this simple lag experiment.

---

## 6. Exact 24-hour no-peeking workflow

1. **Generate compatible calibration evidence.** Run the full five-candidate family under the frozen v4 configuration using only pre-holdout data.

2. **Freeze one challenger.** Apply the calibration rules below. Record the winner and all hashes in an immutable pre-holdout artifact. If there is no clear winner, retain 3.0 seconds and do not manufacture a challenger.

3. **Declare the exact window.** Use one full UTC day:

   ```text
   [00:00:00 UTC, next 00:00:00 UTC)
   ```

   The start and end timestamps must be written into the artifact before the start.

4. **Do not display efficacy metrics during collection.** Operational monitoring may show feed status, session state, queue depth, disk usage, row counts, sequence-gap counts, and writer health. It must not show candidate errors, rankings, or skill.

5. **Do not extend the window.** A reconnect, outage, or low-coverage period remains part of the 24-hour evidence. Coverage gates determine whether the window is usable.

6. **Preserve raw evidence immediately.** Archive the raw input from at least:

   ```text
   holdout_start - history_retention
   through
   holdout_end + max_horizon + sequence-confirmation allowance
   ```

   Store hourly compressed raw shards and a manifest with row counts, session identifiers, boundaries, and SHA-256 hashes. These are raw-data exports, not interim performance reports, so automatic creation does not constitute peeking.

7. **After the window closes**, wait for maximum-horizon maturation and bounded sequence confirmation, then run all 15 replay configurations and write all reports and ledgers atomically.

8. **Apply data-quality gates before exposing efficacy.** If quality fails, label the window `insufficient_evidence` and do not reveal or use its candidate ranking. A new future 24-hour window may be preregistered under the unchanged policy.

9. **Evaluate only the frozen winner.** The selector may also calculate the concurrent 3-second incumbent comparison required for replacement, but it must not rank the other three candidates.

10. **Make exactly one decision.** Pass creates a new activation artifact. Failure leaves the existing 3-second artifact untouched. There is no fallback.

Because raw shards are preserved outside the 72-hour PostgreSQL retention boundary, reports can be regenerated later without turning retention into an emergency.

---

## 7. Recommended numerical gates

These are operational preregistration thresholds, not universal statistical constants.

### Calibration freeze gates

A non-incumbent challenger is frozen only when:

| Gate                                          |            Threshold |
| --------------------------------------------- | -------------------: |
| Canonical pooled common valid coverage        |                ≥ 95% |
| Common valid coverage in each canonical phase |                ≥ 90% |
| Canonical MAE skill versus matched no-change  |                 ≥ 5% |
| Canonical RMSE skill versus matched no-change |                 ≥ 5% |
| MAE and RMSE skill in each canonical phase    |                  > 0 |
| Pooled skill in each asymmetric delay stress  | MAE > 0 and RMSE > 0 |
| MAE-skill lead over calibration runner-up     | ≥ 1 percentage point |

If the leading MAE-skill difference is below one percentage point, declare **no clear challenger**. Do not use a tiny RMSE difference to break that practical tie.

### Holdout data-quality gates

| Gate                                                      |                             Threshold |
| --------------------------------------------------------- | ------------------------------------: |
| Holdout duration                                          |                 Exactly 86,400,000 ms |
| Timing reports present                                    |            All 3 scenarios × 5 phases |
| Common maturation coverage                                |                               ≥ 99.9% |
| Canonical pooled common valid coverage                    |                                 ≥ 95% |
| Common valid coverage in every canonical phase            |                                 ≥ 90% |
| Valid fixed 4-second origins, canonical                   |                    ≥ 20,000 of 21,600 |
| Valid fixed origins in each stress scenario               |                              ≥ 18,000 |
| Challenger candidate-specific valid coverage              |                                 ≥ 95% |
| Challenger coverage relative to concurrent 3-second model | No more than 1 percentage point lower |
| Candidate cohort completeness                             | No incomplete or unclassified cohorts |

### Holdout efficacy and replacement gates

The frozen challenger must pass all of these on the canonical timing scenario:

| Gate                                                             |                                              Threshold |
| ---------------------------------------------------------------- | -----------------------------------------------------: |
| MAE skill versus its no-change baseline                          |                                                   ≥ 5% |
| RMSE skill versus its no-change baseline                         |                                                   ≥ 5% |
| One-sided 95% block-bootstrap lower bound, MAE skill             |                                                    > 0 |
| One-sided 95% block-bootstrap lower bound, RMSE skill            |                                                    > 0 |
| MAE-skill improvement over concurrent 3-second incumbent         |                                  ≥ 2 percentage points |
| RMSE-skill improvement over concurrent 3-second incumbent        |                                  ≥ 2 percentage points |
| Bootstrap lower bound for MAE-skill difference versus 3 seconds  |                                                    > 0 |
| Bootstrap lower bound for RMSE-skill difference versus 3 seconds |                                                    > 0 |
| Each canonical phase                                             |                       MAE skill > 0 and RMSE skill > 0 |
| Each pooled asymmetric delay stress                              |                       MAE skill > 0 and RMSE skill > 0 |
| Stress comparison with 3-second incumbent                        | Neither MAE- nor RMSE-skill difference may be negative |

Directional confusion-matrix rates, win/loss frequency, volatility slices, market-expiry slices, hourly results, and individual 5-minute sessions remain descriptive diagnostics. They cannot rescue or overturn the aggregate decision.

If the frozen winner is 3.0 seconds, the operational result is simply **retain the incumbent**. If a shorter winner fails any holdout gate, retain 3.0 seconds; do not inspect the runner-up as a substitute.

---

## 8. Minimum code changes before freezing v4

The smallest effective change set is:

1. Add one central immutable versioned candidate-policy registry.
2. Add the v4 five-candidate profile to the selector and artifact validator while retaining v2/v3 compatibility.
3. Make public reporting read the candidate set from that registry rather than its old enum.
4. Add a timing-grid manifest that validates the complete three-scenario/five-phase matrix and identical raw-input hash.
5. Add ordered loss-ledger output and the fixed block-bootstrap implementation.
6. Verify persisted cohorts after `ON CONFLICT`, or amend the key to include selection provenance.
7. Include the missing `OPERATIONS.md` and create an exact dependency lock before hashing the final code.

Measured Redis publication-completion timestamps are valuable, but they need not delay this 24-hour shadow experiment as long as the artifact accurately calls the timing grid a fixed sensitivity analysis rather than measured live timing.

The supplied historical files remain useful as immutable calibration provenance:

* [Historical selection artifact](sandbox:/mnt/data/selection-1783983205028-890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699%281%29.json)
* [Historical replay artifact](sandbox:/mnt/data/replay-config-1783983205028-e11377f4f4cb0a6bfc91a682347c77d67ed1d81a83d03b798ff1d963fed6b5e9%281%29.json)

The practical decision is therefore: **freeze a v4 full-family calibration winner, run one exact untouched 24-hour UTC holdout, and require both statistically positive matched-baseline skill and a meaningful improvement over the concurrent 3-second incumbent. Anything else is abstention, with the existing 3-second primary unchanged.**

[1]: https://users.ssc.wisc.edu/~behansen/718/DieboldMariano1995.pdf "https://users.ssc.wisc.edu/~behansen/718/DieboldMariano1995.pdf"
