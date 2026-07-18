## Overall verdict

This plan is **substantially better and is now focused on the right problems**. The seven-cell design, canonical-phase-only decision rule, quality-before-efficacy separation, fixed challenger, matched baselines, no reranking, no fallback, immutable raw evidence, and offline-only execution are all appropriate. 

I would not start calibration yet, though. I see **five material issues and one missing calibration requirement**. These are not cosmetic.

## 1. Calibration and holdout currently evaluate different samples

Calibration ranks candidates using **all 500 ms `canonical_p0` rows**, but the holdout decision uses only:

```text
holdout_start_ms + n * 4000
```

That changes the estimand between selection and confirmation. More importantly, the 4-second sequence always selects only one of the two 500 ms generation positions:

```text
...00.000, ...04.000, ...08.000
```

It never samples:

```text
...00.500, ...04.500, ...08.500
```

The same aliasing occurs in the other phase cells: `p100` samples `.100` but never `.600`, for example. Thus the primary holdout result represents one systematic eighth of the operational 500 ms predictions, not the complete operational stream that selected the challenger. 

There are two defensible fixes:

**Preferred and simpler:** use all 500 ms `canonical_p0` decision-eligible rows for calibration ranking, holdout point estimates, and the 15-minute moving-block bootstrap. A moving-block bootstrap is already intended to preserve serial dependence, including dependence caused by overlapping horizons. Keep the 4-second non-overlapping analysis as a diagnostic or confirmation.

**Alternative:** retain fixed non-overlapping origins, but use exactly the same fixed-origin rule for both calibration ranking and holdout decision. Also add a second preregistered stream offset by 500 ms as a rejection-only robustness check, so the result is not tied entirely to one subphase.

Do not select on 500 ms rows and then promote on only the `n*4000` rows.

### Related missing quality gate

If the fixed-origin approach remains, add a minimum valid fixed-origin count for **every robustness cell**. The current plan requires 90% robustness coverage over all 500 ms rows, but then calculates robustness efficacy on the 4-second subset. Those are not equivalent.

A cell could have 90% overall coverage but only about 20% of its fixed origins present. Add:

```text
valid robustness fixed origins >= 18,000 of 21,600
```

for each of the six rejection cells. The existing 20,000 requirement for canonical fixed origins is appropriate. 

## 2. The “exact operational control” is not fully reconstructable from this raw data

The plan correctly recognizes that raw capture lacks Chainlink publisher epoch and accepted-event sequence. But it still describes the replayed control as the exact active implementation and says the configuration digest includes cohort behavior while explicitly excluding candidate-family membership. 

There are two problems.

First, a cohort definition such as “all configured candidates valid” changes when the family changes from three candidates to five. A digest cannot include the effective cohort contract while excluding the candidate family that defines that contract.

Second, the active live evaluator uses publisher epochs and accepted-event sequences to invalidate history on missed cache events, metadata loss, sequence gaps, epoch changes, and failed post-target confirmation. Against the uploaded source, this occurs in:

```text
price_collector/shadow_signal_evaluation.py:699–752
price_collector/shadow_signal_evaluation.py:791–923
price_collector/shadow_signal_evaluation.py:1052–1074
```

Raw replay can provide an event-complete offline stream, but it cannot reproduce the exact latest-cache delivery and sequence-confirmation behavior when those metadata were not captured.

The simple correction is to split the identity:

```text
forecast_config_digest
    lag, horizon, beta, staleness, gap/skew,
    anchor/reference selection, projection and forecast validity

evaluation_policy_digest
    complete candidate family, common-cohort rule,
    target resolution, finalization, continuity rules,
    delay/phase cell and delivery metadata semantics
```

Then define the comparator as:

```text
offline_replay_replacement_control
```

It is the active 3-second **forecast rule and settings**, evaluated under the same causal raw-replay contract as the challenger.

That is enough for this experiment because the plan already says it does not establish live-production safety. Do not claim that it reproduces the complete active live evaluator. If complete live-evaluator equivalence is required for `promotion_eligible`, the missing delivery metadata would have to be captured prospectively, and this test could not proceed under the current offline-only scope.

## 3. The raw archive can still be sealed with incomplete data

The archive design is directionally correct, but two completeness conditions need to be explicit. 

### Late database writes

The raw writer is asynchronous and batches for up to the configured flush interval. In the supplied source, that interval is converted at `raw_capture.py:1272`, and batches are collected and written at `raw_capture.py:1416–1491`.

Therefore, an hourly shard exported immediately after an hour ends can miss records whose receive time belongs to that hour but whose database write completes shortly afterward.

Use this rule:

```text
Hourly shards are provisional until final sealing.
At sealing, regenerate them or verify them from a stable database snapshot
after all overlapping sessions have finalized and reconciled.
```

The final manifest—not the presence of an hourly file—marks the bytes as authoritative.

### Expired session prefixes

The plan proposes prefix/range/suffix sufficient statistics for sessions extending outside the requested range. That works only when the prefix still exists when the summary is calculated.

Add a hard preflight:

```text
For every potentially overlapping session:
    session.ready_wall_ns must be at or after the actual oldest retained
    raw boundary,
    OR a previously sealed trustworthy prefix summary must already exist.
Otherwise the session cannot be integrity-reconciled and the range is unusable.
```

Use the **actual oldest retained partition and relation-budget status**, not merely the configured 72-hour retention value. The storage budget can cause earlier partitions to disappear before the nominal retention limit.

Also verify before the holdout that raw-capture partition maintenance is healthy and that the current/next partitions exist. This is not the deferred full retention rollout; it is a minimum precondition for collecting the evidence at all.

## 4. The holdout loss products are internally contradictory

Phase 2 requires a day/hour/session/confusion report based on all 500 ms `canonical_descriptive_rows`. But Phase 5 says canonical losses are calculated **only** on `canonical_decision_rows`, the 4-second subset.  

You cannot produce the proposed 500 ms descriptive report without the 500 ms losses.

The correct contract is:

```text
After quality passes:

canonical_p0:
    generate challenger/control losses for all 500 ms decision-eligible rows
    mark each row:
        descriptive_eligible = true
        fixed_origin_eligible = true/false

promotion and bootstrap:
    use only the frozen gate-eligible subset

day/hour/session/confusion report:
    use all canonical descriptive rows

robustness cells:
    generate only the challenger/control losses required by their frozen
    robustness sample
```

This retains the protection that holdout losses exist only for challenger and control while making the descriptive report possible.

## 5. Preregistration cannot contain the observed fixed-origin eligibility mask

The preregistration section says it binds the fixed-origin “rule/mask/count.” The scheduled origin vector and its expected count are known before the holdout. The **eligibility mask and eligible count are future data** and cannot be known before collection. 

Freeze these before the holdout:

```text
origin-generation formula
scheduled origin timestamps
expected scheduled count
missing-origin treatment
coverage threshold
```

Bind these only after the holdout:

```text
observed eligibility mask
observed eligible count
reasons for each missing origin
hash of the observed mask
```

Otherwise the artifact schema either asks for impossible future information or risks confusing the scheduled mask with the observed-validity mask.

## 6. Correct the `source_timestamp_ms` contract

The plan says `source_timestamp_ms` drives none of the model decisions and is retained for identity/watermarks. 

In the supplied source, it does affect behavior: `ShadowSignalEngine._timestamps_regress()` treats a decreasing source timestamp as a timestamp regression and resets or invalidates state:

```text
price_collector/shadow_signal.py:373–410
```

The accurate frozen statement should be:

```text
source_timestamp_ms does not drive:
    staleness age,
    reference target,
    reference gap,
    target eligibility,
    actual pre-target eligibility.

source_timestamp_ms does drive:
    the existing source-timestamp regression watermark and associated
    validity/reset behavior.
```

Add a direct regression test where receipt times increase but source timestamps decrease. V3 and matching v4 must produce the same reset and invalid cohort behavior.

## One important missing calibration rule

The plan predeclares a contiguous calibration range but sets no minimum duration or minimum independent-origin count. 

That permits a technically valid but very short calibration range to freeze the challenger. The untouched holdout still protects against automatic false promotion, but challenger selection could be highly unstable and unnecessarily consume the only efficacy-bearing day.

A simple rule is:

```text
calibration duration >= 24 hours
```

or, if fixed origins remain the calibration basis:

```text
valid canonical calibration origins >= 20,000
```

Freeze that before the first calibration quality audit. Do not choose the calibration duration after seeing efficacy.

## Approval assessment

After those changes, I would consider the plan ready for implementation and rehearsal. I would keep the following parts exactly as they are:

* Seven timing cells with no pooling.
* Canonical phase 0 as the only selection source.
* Quality commands unable to produce losses.
* MAE as primary and RMSE as confirmation only.
* One frozen challenger.
* No holdout reranking, fallback, or dynamic switching.
* Existing 3-second primary unchanged.
* Raw replay as the efficacy authority.
* `promotion_eligible` separated from actual deployment.
* Retry only after genuinely loss-free insufficient evidence.

The most important correction is the first one: **calibration selection and holdout promotion must measure the same predefined stream.** The archive completeness and exact-control scope are the other two items that could otherwise make a formally clean result misleading.
