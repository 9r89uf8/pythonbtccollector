## Verdict

**Not fully.** The replay-side fixes are largely correct, and the in-memory cohort handling is much better. I would **not approve v3 evidence for model selection yet**, because the live outcome-integrity and database-atomicity guarantees still have several blocking gaps.

| Claimed fix                                        | Assessment                               |
| -------------------------------------------------- | ---------------------------------------- |
| Full 3×3 directional matrix                        | **Pass**                                 |
| Source-visibility delays and phase offsets         | **Pass, with modeling limitations**      |
| Zero future skew for v3                            | **Not verified at the selection layer**  |
| Sequence metadata invalidates overwritten outcomes | **Incomplete; blocking bugs remain**     |
| Atomic cohorts through maturation and queueing     | **Mostly pass in memory**                |
| Atomic cohorts through database and retention      | **Not established**                      |
| v2/v3 evidence separation                          | **v2 side passes; v3 side not supplied** |
| Inspected evidence becomes calibration-only        | **Not verifiable from supplied files**   |

Both Python bodies pass static compilation. I also ran targeted state-machine tests against the new evaluator.

## What is implemented correctly

### 1. The directional metric bug is fixed

The v3 replay now puts every scored forecast into one of all nine actual/predicted cells and verifies that the matrix count equals the metric count. Its definitions are correct:

* Action precision divides correct up/down actions by **all** predicted up/down actions.
* Move recall divides correct directional actions by all actual up/down outcomes.
* False-action rate includes up/down predictions on actual-neutral outcomes.
* Three-class accuracy includes neutral-to-neutral predictions.
* Predicted-action frequency uses the complete scored population.

That removes the prior approximately 99% denominator inflation. 

One remaining dependency is that the v3 selection parser must independently validate all nine cells, their total, and every reported derived rate rather than trusting the replay JSON. That parser was not included.

### 2. Replay visibility timing is mechanically sound

The replay now queues events at:

```text
raw_received_time + configured_visibility_delay
```

and exposes them only when a replay poll reaches that availability time. Evaluation generation is shifted by the configured phase offset, and the report records the two delays and phase as part of its configuration. 

It also correctly describes these as fixed sensitivity assumptions rather than measured Redis publication-completion times. That disclosure is important. The model still does not reproduce variable MGET latency, process scheduling jitter, or missed live polls, but the implementation is suitable for pre-registered sensitivity scenarios.

### 3. In-memory queue handling is substantially improved

The writer buffer now contains `ShadowEvaluationCohort` objects rather than individual rows. Capacity checks ensure one cohort fits, batching does not split cohorts, overflow drops whole cohorts, and failed/deferred batches are requeued in whole-cohort units.  

The scheduler also waits until every configured model has matured before releasing the records for a generated timestamp. 

Those are good structural changes. The remaining maturation problem concerns the **contents** of a cohort after a sequence discontinuity, not whether all model rows are present.

---

# Blocking findings

## 1. A sequence gap does not invalidate a shorter-horizon result that has already matured

The scheduler creates and stores each model’s completed record as soon as that model reaches its target. Shorter-horizon records sit in `_matured_by_generated` until the longest model matures.

However, `_reset_outcome_history()` only advances the history epoch and clears Chainlink history. It does not clear or invalidate records already staged in `_matured_by_generated`.  

I reproduced this with a two-model harness:

```text
short target: 100 ms
long target:  200 ms

sequence 1 observed at 0 ms
short model matures at 100 ms
sequence jumps from 1 to 3 at 150 ms
long model matures at 200 ms
```

The emitted cohort was:

```text
short: actual=100, actual_received_ms=0, forecast_error=0
long:  actual=None, forecast_error=None
```

The missing sequence-2 event could have arrived before the 100 ms short target. Therefore the short result is no longer trustworthy either.

This issue applies to every history reset, including:

* Sequence gaps.
* Publisher-epoch changes.
* Sequence regressions.
* Metadata loss.
* Observation/clock resets.

### Recommended fix

Do not construct final records one model at a time. Retain the cohort’s forecasts until the maximum target has been reached and outcome continuity has been proved. Then resolve all targets together.

A robust sequence is:

1. Store the entire generated-time forecast cohort with its starting history epoch.
2. Wait until the maximum horizon has matured.
3. Require a successful sequenced Chainlink observation at or after that maximum target.
4. Require that the cohort’s history epoch has not changed.
5. Resolve every model target from the retained history.
6. Emit one `ShadowEvaluationCohort`.

If continuity fails, emit the whole cohort with all affected actuals/errors null and an explicit outcome-integrity reason. Do not preserve earlier model outcomes merely because they matured before the discontinuity was discovered.

## 2. Missing Chainlink cache observations can still produce scored outcomes

A `chainlink=None` observation does not itself invalidate history or defer maturation. The scheduler calls sequence handling, performs no Chainlink ingestion, and then continues into `_mature()`. The configured observation-gap test measures gaps between worker ticks—not gaps between successful Chainlink cache reads—so a worker that continues ticking every 100 ms with Chainlink absent never triggers that gap condition. 

I tested this sequence:

```text
t=0:   sequence 1 is observed
t=100: Chainlink is None
t=200: Chainlink is None; both targets have matured
```

Both model rows were emitted with the old `actual=100`.

That is not causally proven. During the missing-cache interval, an update could have occurred and been overwritten. A later sequence gap may reveal the problem, but the cohort might already have been persisted.

### Recommended fix

For v3, outcome finalization should require a successful sequenced observation made at or after the target. Track something such as:

```text
last_sequence_continuity_observed_ms
```

A target must not be finalized until that watermark is at least the target time. When Chainlink is missing, defer maturation. If continuity cannot be re-established within a bounded period, invalidate the outcome with a reason rather than scoring stale history.

A simpler but more conservative alternative is to advance the outcome epoch immediately whenever a v3 Chainlink read is absent or malformed.

## 3. Legacy startup values are scored before sequence continuity is established

When an initial Chainlink value has no `publisher_epoch`, the code sets `_chainlink_startup_legacy_observed=True` but returns without marking sequence metadata as lost. The value is then ingested and can be used for outcomes. Only when the first sequenced value later appears does the scheduler reset the history. 

I confirmed that a complete cohort can be emitted with scored actuals while all observed Chainlink values remain legacy/unsequenced.

That later reset cannot retract a cohort already persisted.

### Recommended fix

Make this behavior explicitly version-dependent:

* **v2 evaluation:** legacy operation may be allowed to preserve old semantics.
* **v3 evaluation:** do not ingest target history, schedule scoreable outcomes, or emit scored cohorts until atomic sequence metadata has been established.

The scheduler already receives `selection_schema_version`, so it has enough provenance to enforce this distinction.

## 4. A repeated sequence with changed event data is accepted

Within one publisher epoch, the sequence checker handles:

```text
sequence < previous
sequence > previous + 1
```

It does not handle:

```text
sequence == previous
but the price/timestamps/identity changed
```

That represents a corrupt or torn cache state: one accepted-event sequence must identify one immutable event. The current code accepts that changed event into outcome history without recording a sequence discontinuity. 

Track the identity associated with the last sequence and require:

```text
same sequence ⇒ identical event identity
```

A changed identity with the same sequence should reset continuity and increment a dedicated integrity counter.

---

# End-to-end integration is currently broken against the previously supplied files

The new evaluator accesses:

```python
chainlink.publisher_epoch
chainlink.accepted_event_sequence
```

But the previously supplied `ObservedPrice` contains only `value`, `source_timestamp_ms`, and `received_ms`. 

Likewise, the supplied collector still constructs only those three fields. 

The new writer also requires:

```python
candidate_model_versions=...
```

but the supplied collector does not pass that constructor argument.  

Finally, the collector still offers matured rows one at a time:

```python
for record in matured:
    writer.offer_nowait(record)
```

while the new `offer_nowait()` is explicitly only a compatibility entry point for a one-model cohort. A three-model writer will reject the first single-row offer. 

Therefore, with the files previously supplied:

* The default writer construction fails because of the missing required argument.
* Sequence access fails when a non-null old-style `ObservedPrice` reaches the scheduler.
* Per-row offering fails the expected model-set validation.

### Required integration changes

The sequence metadata must pass through the complete path:

```text
Chainlink publisher
→ atomic Redis payload
→ LivePrice parser
→ collector conversion
→ ObservedPrice
→ evaluation scheduler
```

`publisher_epoch` and `accepted_event_sequence` must be both present or both absent, and they must be written atomically with the price and timestamps.

The writer should be initialized with:

```python
candidate_model_versions=tuple(
    model.version for model in activated.models
)
```

The cleaner API is for the scheduler itself to return:

```python
tuple[ShadowEvaluationCohort, ...]
```

Then the collector can call `offer_cohort_nowait()` once per cohort. Returning a flattened record tuple creates an implicit grouping contract and fails when multiple generated-time cohorts complete during one tick.

If those integration files have been updated elsewhere, they need to be included before this portion can be approved.

---

# Database and retention atomicity are not yet guaranteed

## The backend result contract can conceal split cohorts

The writer flattens cohorts into records before calling:

```python
backend.write_evaluations(records)
```

The backend reports only:

```text
persisted_count
rejected_count
deferred_records
```

The writer verifies that persisted and rejected counts are multiples of the candidate count. That does **not** prove that each cohort was handled atomically. 

For example, with two two-row cohorts, a backend could:

* Persist one row from cohort A and one row from cohort B.
* Reject the other row from each cohort.

The result is:

```text
persisted_count = 2
rejected_count  = 2
```

Both counts are divisible by two, so the writer accepts the result.

I reproduced this against the writer: it reported one persisted cohort and one rejected cohort with zero batch failures, despite the simulated backend having split both cohorts.

### Required fix

Make the backend contract cohort-based:

```python
write_evaluation_cohorts(
    cohorts: Sequence[ShadowEvaluationCohort]
) -> CohortWriteResult
```

The result should contain exact, disjoint sets of:

```text
persisted_cohort_ids
rejected_cohort_ids
deferred_cohort_ids
```

Their union must equal the input cohort identities.

## The schema does not enforce complete cohorts

The table still has a row-level primary key:

```sql
PRIMARY KEY (model_version, generated_ms, horizon_ms)
```

and the only cohort-specific schema addition is an index. An index improves cleanup lookup performance but does not require all expected models to exist, prevent partial transactions, or make deletion atomic. The writer role still has direct row-level `INSERT` and `DELETE` privileges.  

It also means provenance is not part of the primary key. Running v2 and v3 concurrently with an overlapping model version and generation time can cause collisions instead of cleanly isolated evidence.

A stronger schema would use:

```text
shadow_signal_evaluation_cohorts
    cohort_id
    full provenance
    generated_ms
    expected model-set hash/count
    committed flag

shadow_signal_evaluations
    cohort_id FK ... ON DELETE CASCADE
    model_version
    ...
    PRIMARY KEY (cohort_id, model_version)
```

Insertion should create the parent and every child row in one transaction, then mark the parent committed. Readers should see only committed cohorts. Retention should select parent cohort IDs and delete parents, allowing cascade deletion of all member rows.

## Retention remains unverified

The backend protocol exposes:

```python
delete_expired(cutoff_generated_ms, limit) -> int
```

That is a row-count interface. The supplied schema index does not demonstrate that the implementation limits by complete cohort identities. This is particularly important if v2 and v3 candidate counts differ.

The database backend and its deletion SQL were not included, so the retention claim cannot currently be confirmed.

---

# Version separation is only partly demonstrated

The supplied v2 selection and artifact loaders support schema version 2 only. They therefore reject a schema-3 replay rather than accidentally mixing it, which is correct for preserving existing v2 artifacts.  

However, no v3 `shadow_signal_selection.py` or v3 artifact/activation path was supplied. Therefore I could not verify that v3:

* Accepts only replay schema 3.
* Recomputes all confusion-matrix counts and rates.
* Requires `max_future_skew_ms == 0`.
* Binds visibility delays and phase offsets into the configuration hash.
* Rejects mixed timing assumptions across calibration and holdout reports.
* Rejects every previously inspected report hash when presented as holdout evidence.
* Requires the new holdout to begin after all inspected evidence.
* Coexists with the existing v2 activation path without fallback or version ambiguity.

The replay’s default future skew is zero, but the command line and `ReplayConfig` still permit a nonzero value. That is reasonable for sensitivity testing; it means the zero requirement must be enforced by the missing v3 selection policy, not inferred from the default. 

There is also a v2 compatibility issue: the new evaluator always invalidates an input received after `generated_ms`, while the existing v2 evidence allowed 250 ms of future skew. Thus v2 live evaluations can describe a different population from v2 replay/published signals. The database constraints likewise enforce zero skew for every schema version. Either pass the artifact’s allowed skew into evaluation and persist it, or explicitly label all new live evaluation rows as v3 causality semantics rather than treating them as comparable v2 evidence.

---

# Secondary correction

The volatility-regime diagnostic is still computed when the raw futures event is consumed, before that event passes through the configured visibility delay. The model inputs obey the delay, but a forecast’s volatility slice can include a futures event that was not yet visible to the simulated worker.

Move `_observe_volatility_event(event)` from raw `consume()` into `_apply_visible_events()` when a futures event actually becomes visible. Use the visibility timestamp for the lookback if the intended slice is “volatility known to the worker.” This does not alter the model projection itself, but it makes the timing-sensitive diagnostic slices causal. 

The reporting view should also expose at least:

```text
selection_schema_version
selection_policy_version
outcome_status
outcome_invalid_reasons
```

At present, a null actual caused by a sequence gap is indistinguishable from a normal unavailable target, and the view omits the explicit version fields even though the base table stores them. 

## Required acceptance tests

Before v3 reports become selectable, I would require these tests to pass:

1. A sequence gap after each shorter horizon but before the maximum horizon invalidates the entire affected cohort.
2. Chainlink missing at a target does not score stale history.
3. V3 startup with legacy metadata produces no scored outcomes.
4. Same epoch and same sequence with changed event identity is rejected.
5. Metadata loss, epoch change, sequence regression, clock regression, and observation gaps produce explicit outcome reasons.
6. A backend that splits rows across cohorts is rejected by exact cohort identity.
7. A database failure after inserting only part of a cohort leaves no reader-visible cohort.
8. Retention limits and cutoffs can never delete a subset of a cohort.
9. Multiple cohorts maturing during one scheduler tick are offered separately and atomically.
10. V2 rejects v3, v3 rejects v2, mixed report sets are rejected, and inspected v2 hashes cannot be assigned a holdout role.
11. V3 selection rejects nonzero future skew.
12. Replay volatility slices change only when delayed futures events become visible.

## Files needed to close the remaining review

The unverified or broken boundaries are outside the three newly supplied files. A complete approval requires the current versions of:

* `shadow_signal_selection.py` v3.
* `shadow_signal_artifact.py` or the dual-version activation/router.
* `shadow_signal_collector.py`.
* `shadow_signal.py`.
* `live_cache.py` and the Chainlink Redis publisher.
* The PostgreSQL evaluation backend, including insert and retention SQL.
* Unit/integration tests and the deployed migration path.

**Current approval status:** replay metric correction and timing sensitivity implementation are acceptable; live sequence finalization, runtime integration, database isolation, retention, and v3 policy enforcement are not yet sufficient for trusted shorter-horizon evidence.
