# Actual vs. Projected Chainlink

## Purpose

The Chainlink catch-up signal is an experiment that asks one narrow question:

> Given the move already visible in Binance BTC futures, what Chainlink BTC/USD
> value should we expect to have received about three seconds later?

The project compares that projection with the latest Chainlink value available
no later than the forecast target time. This gives us a causal, repeatable way
to measure whether the futures lead contains useful information about
Chainlink's next few seconds.

It is important to keep the claim narrow. The signal is **not** predicting:

- the official five-minute market settlement price;
- whether BTC will finish Up or Down;
- Polymarket probability or order execution;
- a new price move that has not happened yet; or
- why either market moved.

It is estimating a short-lived Chainlink catch-up value from a futures move
that is already observable.

## What “actual” and “projected” mean

A projection generated at time `t` by the selected model has a target at
`t + 3,000 ms`.

- **Projected Chainlink** is the model's estimate, made only from information
  locally available at generation time.
- **Actual Chainlink** is the newest Chainlink observation the evaluator had
  observed whose producer receive time is no later than the target time.
- **No-change baseline** is the Chainlink value at generation time carried
  forward unchanged to the same target.

The actual is selected by local `received_ms`, not by looking ahead for the
next convenient update. A Chainlink observation received after the target is
excluded, even if it is available when the evaluation row is eventually
written. This prevents future information from leaking into the result.

The Chainlink cache value also carries an internal producer epoch and monotonic
accepted-event sequence. If the evaluator observes a sequence jump,
regression, producer restart, or loss of sequence metadata, it discards outcome
history that crosses that discontinuity. It cannot reconstruct an overwritten
latest-cache value, so affected evidence is invalidated rather than paired with
an older, apparently eligible actual.

One accepted sequence within a publisher epoch must also identify exactly one
source timestamp, receive timestamp, and price. Re-reading that identical event
is valid continuity evidence. If the same sequence carries a different
identity, the evaluator resets outstanding history and enters
`chainlink_sequence_identity_mismatch` quarantine. It admits neither disputed
identity to the new history epoch, does not let either confirm a target, and
permanently invalidates cohorts generated during quarantine. Only a newer
sequence or publisher epoch establishes a clean baseline. This invariant
applies to every sequenced evaluation, including v2. The last sequence binding
survives a metadata-less read, so metadata recovery cannot silently redefine
that sequence.

Schema v2 continues to support legacy-only startup. Schema v3 retains no target
history before the first complete producer-epoch/accepted-sequence pair.
Candidate attempts are still scheduled, but every pre-establishment cohort is
permanently marked
`chainlink_sequence_not_established` and matures `integrity_invalid` with null
actual/error fields. A later first sequence cannot retroactively validate those
cohorts, and already entered cadence buckets are not backfilled. Once sequence
metadata has been established, losing it suppresses actual-outcome ingestion
until a new sequenced value re-establishes continuity.

All candidates generated together remain one pending cohort until the longest
candidate target has been reached. Only then does the evaluator resolve every
target from retained history and construct the final rows. A history reset at
any point before that common finalization invalidates the outcome for every row
in the cohort, including a shorter target that passed before the reset. Those
rows keep their forecast fields but store null actual/error fields with
`outcome_status=integrity_invalid` and explicit reset reasons. With continuous
history, `outcome_status` is `available` when a causal actual exists and
`unavailable` otherwise. The common `matured_ms` is the time the full cohort
became persistence-eligible, not the individual target time.

For an otherwise outcome-eligible schema-v3 cohort, the maximum target alone
does not make it eligible. A successful Chainlink cache observation carrying
sequence metadata must occur at or after that target. Missing or malformed
Chainlink reads defer the cohort for up to two poll intervals. If continuity is
confirmed by the deadline, every target is resolved normally from retained
history. If it is not, every row is
stored with null actual/error fields, `outcome_status=integrity_invalid`, and
`chainlink_sequence_confirmation_timeout`. This extra confirmation gate does
not change schema-v2 maturation.

Each row retains the five-minute market in which it was generated. Reporting
windows are selected by `target_ms`, so a forecast generated immediately before
a boundary can be scored in the following target window. The reporting query
therefore inspects only that target window's generation market and its
predecessor.

## How the model works

The model is a deterministic, basis-neutral ratio calculation. It is not a
machine-learning model with continuously fitted weights.

When a new Chainlink event arrives, the engine creates an anchor:

1. Record the new Chainlink value, `C_anchor`, and its local receive time.
2. Look back by the model lag and select the most recent eligible futures price
   at or before that point. This becomes `F_reference`.
3. At forecast generation, compare current futures, `F_now`, with that
   reference.
4. Transfer that percentage move to the Chainlink anchor.

The calculation is:

```text
futures_return      = (F_now / F_reference) - 1
projected_chainlink = C_anchor * (1 + beta * futures_return)
```

For the selected model, `beta = 1`, so it simplifies to:

```text
projected_chainlink = C_anchor * F_now / F_reference
```

The current model is named `catchup_ratio_l3000_b100`:

- `catchup_ratio` describes the anchored futures-ratio method;
- `l3000` means a 3,000 ms lag and forecast horizon; and
- `b100` means `beta = 1.00`.

The anchor is what makes the calculation useful. Binance futures and Chainlink
can have a persistent price difference, so directly adding a futures dollar
move to Chainlink would mix that basis into the forecast. The ratio model
instead transfers only the futures percentage move since the reference
associated with the latest Chainlink update.

The engine re-anchors on every accepted Chainlink event, even if the reported
price is unchanged. An unchanged refresh is still new timing information.
Without re-anchoring, the same earlier futures move could be counted twice.

The complete sequence is:

```text
Chainlink event arrives
        |
        +--> locate futures reference 3 seconds earlier
        |
        +--> compare current futures with that reference
        |
        +--> generate a Chainlink estimate for 3 seconds ahead
        |
        +--> retain every candidate until the longest target
        |
        +--> resolve all targets from one unchanged outcome-history epoch
```

## How we arrived at the selected model

The starting observation was that Binance futures often appeared to move
roughly three to four seconds before the corresponding Chainlink update. That
was treated as a hypothesis, not as a fixed conclusion.

The first version deliberately kept the candidate set small and interpretable:

- `catchup_ratio_l3000_b100` — 3.0 seconds, beta 1.00;
- `catchup_ratio_l3500_b100` — 3.5 seconds, beta 1.00;
- `catchup_ratio_l4000_b100` — 4.0 seconds, beta 1.00; and
- a separately paired no-change baseline.

Those were the only catch-up horizons in the selection experiment. We did
**not** test 2.0-second or 2.5-second candidates. Therefore, the accepted
decision shows that 3.0 seconds was the best eligible model among 3.0, 3.5,
and 4.0 seconds and that it passed the no-change baseline gates. It does not
show that 3.0 seconds is better than 2.0 or 2.5 seconds; their performance is
currently unknown.

Testing 2.0 and 2.5 seconds later would mean expanding the predefined
candidate set and repeating replay, calibration, and untouched holdout
selection. It should not be done by changing the frozen live primary in place.

Raw data made it possible to test the hypothesis on the receive-time timeline:

- futures prices were reconstructed on the 100 ms grid;
- individual Chainlink events retained their exact local receive ordering;
- replay emulated the live 100 ms reads and 500 ms forecast cadence; and
- only complete, clean, count-reconciled overlaps between both feeds were
  eligible.

Replay reports are limited to at most 24 hours each. The accepted selection was
not based on picking whichever candidate looked best in the final market. The
selection policy used explicitly assigned older calibration evidence and a
strictly later, untouched holdout:

1. Compare all three candidates on the same common cohort of forecast times.
2. Require positive calibration MAE and RMSE skill against each candidate's own
   paired no-change baseline.
3. Rank eligible candidates lexicographically by calibration MAE skill first
   and RMSE skill second, with deterministic tie-breakers.
4. Freeze the calibration winner.
5. Test only that frozen winner on the later holdout.
6. Accept it only if it retains positive MAE and RMSE skill and passes the
   coverage gates; otherwise, select no model.

Each replay report also had to contain at least 10,000 common scored forecasts,
at least 50% valid common coverage, and at least 99% maturation coverage. The
common cohort matters because it prevents one horizon from being compared on
easier or different moments than another.

That process selected `catchup_ratio_l3000_b100` as the provisional primary.
The decision and its evidence hashes were frozen in a selection artifact. The
live worker reads and verifies that artifact; it does not contain a hard-coded
winner, rerank recent results, or silently fall back to another candidate.

This is best described as **model selection**, not model training. The current
version selected a lag from three fixed candidates while keeping beta fixed at
1. A future version could test additional lags or estimate beta, but that would
require a new replay, new calibration evidence, and a new untouched holdout.

## Measurement-correction checkpoint

The accepted production decision is immutable schema/policy v2 evidence and
continues to select the provisional 3.0-second model. A later code review found
measurement defects that do not change that frozen file but must be corrected
before a shorter-horizon experiment:

- Replay/selection v3 replaces the old directional counters with a complete
  `up`/`neutral`/`down` confusion matrix. It reports three-class accuracy,
  action precision over every predicted action, move recall, neutral
  false-action rate, opposite-direction rate, and action frequency. The old
  approximately 99% figure excluded false actions on neutral outcomes and must
  not be described as action precision.
- Replay v3 separates raw receive time from configured source-visibility time,
  records fixed futures and Chainlink delay assumptions, and supports all five
  100 ms generation-phase offsets. These are sensitivity inputs, not measured
  Redis publication-completion timestamps.
- New v3 evidence requires zero future skew so a signal accepted for
  publication is evaluated under the same strict local-time causality rule.
- The live Chainlink sequence metadata described above makes overwritten-cache
  gaps detectable and fail-closed.
- Live evaluations are persisted as complete generated-time candidate cohorts.
  Maturation retains each horizon's causal target, while queue overflow,
  batching, retry, permanent-error isolation, deferral, and retention operate
  on whole cohorts.

Rows written before the cohort-atomic rollout can still be partial. Any future
live comparison must start at the rollout/new-artifact boundary and verify the
exact artifact-defined candidate set at each common `generated_ms`; old rows
cannot be made complete retroactively.

This checkpoint still contains only the 3.0/3.5/4.0-second candidate family;
it does not run or imply a 2.0/2.5-second result. Because the shorter candidates
were proposed after inspecting the accepted v2 holdout, all existing evidence
is now development/calibration data for that future expanded family. Its policy
must be preregistered and frozen before collecting a genuinely later untouched
holdout.

## V4 pretest contract checkpoint

The first v4 implementation checkpoint now defines the offline experiment
contract, but it does not run calibration or inspect a holdout. The contract
fixes the ordered `1500`, `2000`, `2500`, `3000`, and `3500` ms comparison
family, permits only the three shorter members to challenge, keeps 3000 ms as
the incumbent-aligned comparator, and treats 3500 ms as a non-promotable
guardrail. It also fixes the canonical timing cell plus the six rejection-only
delay/phase cells. Only the canonical cell may rank or support promotion.

Forecast configuration, forecast implementation, and offline evaluation policy
have separate canonical digests. Within the full/non-lag forecast-configuration
pair, only the full digest retains lag and horizon; changing a shared rule also
changes the non-lag digest. The evaluation-policy digest separately binds the
ordered family and its forecast-config identities, matched baselines, cohort
rules, timing lattice, tie order, target resolution, continuity rules, and seven
cells. Model identity includes role, version, forecast-configuration digest, and
evaluation-policy digest, so a v4 3000 ms candidate cannot be confused with a
distinct operational control merely because their version strings match.

The contract requires the active-primary freeze to bind a 3000 ms, beta-1 rule,
its loaded and installed runtime identities, and the supporting selection,
configuration, code-manifest, and reconstruction artifacts. The later
provenance checkpoint must obtain those bindings from the deployed system. The
incumbent aliases the v4 3000 ms replay control only when both its complete
forecast configuration and forecast-code digest match. Otherwise, the test must
replay it as a separate non-selectable control. In that case the eventual
question is whether to replace the complete current forecast configuration with
the complete v4 challenger configuration, not which lag alone is better.

The plan fixes the v4 reference gap at 250 ms and future skew at zero, but it
does not state the Futures-staleness, Chainlink-staleness, or history-retention
values. The contract therefore requires those values explicitly when a lineage
is created and binds them cryptographically; it does not inherit a legacy or
deployed default silently.

Strict schemas now fail closed on altered policy/configuration fields,
non-canonical Decimal values, role-only control identities, unknown artifact
fields, invalid stage/marker combinations, changes relative to a supplied frozen
preregistration or bound anchor artifact, or terminal results that expose
efficacy values under `insufficient_evidence`. Forecast-code manifests are
self-describing schema-v1 artifacts: they record four component hashes and
recompute the derived forecast-code digest from those identities; construction
from implementation bytes is available to the later provenance loader. A
holdout selection anchor is not accepted from a timestamp claim alone:
preregistration validation also requires the canonical raw completion-marker or
retry-eligibility artifact and its authorization, verifies both bound hashes,
and reads the anchor timestamp from that hashed source.

Terminal result validation is decision-derived rather than label-derived. It
checks the stage-specific marker matrix, closed failure-reason vocabulary,
bounded retry state, parent ancestry, candidate-ledger binding, exact seven-cell
quality counts/masks/report bindings, calibration ranking summaries, and every
conjunctive holdout promotion gate. `promotion_eligible` is valid only when all
point, bootstrap, RMSE, and six-cell robustness gates recompute to true; a 3000
or 3500 ms calibration winner remains `retain_incumbent` without a shorter
runner-up substitution.

Previously inspected evidence now uses an authoritative scoped inventory rather
than artifact-name heuristics. Each entry binds the artifact identity, source
lineage and experiment identifiers, half-open evidence window, inspection role,
and evidence scope. This permits an old holdout artifact to be declared honestly
as historical calibration input while rejecting an attempt to relabel current
holdout-quality evidence as calibration-only. Calibration and holdout successor
authorizations bind the selected-window/freeze identity where applicable,
candidate-day ledger, provenance root, and exhausted post-allocation retry
budget in addition to the parent and retry-eligibility hashes.

The pushed-preregistration path is also transitive. The preregistration freezes
the authoritative remote ref and remote-URL digest. A canonical pushed receipt
then binds the preregistration and sidecar hashes, pushed commit, observed ref,
and verification time; its canonical deadline check binds the expected and
observed ref/commit, presence and timeliness booleans, and check time. Terminal
validation requires the raw receipt/check bytes and recomputes those bindings.
The efficacy-completion marker inventories the preregistration, receipt,
deadline check, raw manifest, and pre-efficacy provenance gate alongside its
start marker and immutable efficacy artifacts, so changing receipt evidence
also changes the completion identity. Completed holdout efficacy cannot predate
the frozen archive input tail.

The fixed contract is structurally feasible at construction time. This
checkpoint therefore rejects any `structural_gate_infeasibility` claim unless a
later work item supplies an independently derived feasibility proof; it does not
let a failed day masquerade as proof that every permitted successor is
impossible. The durable proof publisher and lineage-closing reducer remain part
of the later state-machine work.

This checkpoint does not yet add the v4 causal replay mode, create-once file
publisher, lineage/day chooser, retry reducer, or attempt-owner lock. In
particular, the in-memory duplicate-result check is validation defense only;
durable exactly-once publication belongs to the later state-machine and locking
work items. Production artifacts, services, data, and the selected live primary
remain unchanged.

## V4 causal replay checkpoint

The second v4 implementation checkpoint adds an explicit offline causal-replay
path without changing the schema-v3 replay defaults, report shape, database
reader, or CLI. A `V4CausalReplayConfig` accepts only a validated
`V4ExperimentContract`, one exact frozen timing cell, and a half-open scoring
window. Candidate settings, identities, delays, phase, control resolution, and
policy digests cannot be supplied again as replay overrides. The existing
`ReplayConfig`, `ReplayReport`, and `replay_shadow_signals` path remains the
legacy schema-v3 implementation.

The v4 runner uses the global epoch-aligned 100 ms poll and 500 ms generation
lattices even during a feed gap or an interval without a common clean session.
It therefore retains every scheduled origin instead of compacting the evidence
to healthy session intersections. The strict maximum-horizon tail rule is
determined only from the scoring window. All-five generation eligibility is
fixed at generation without consulting a later actual, session end, control
value, or reset.

Raw events use the frozen receive-time, source-kind, monotonic-time,
source-sequence, and connection ordering. Each source's fixed sensitivity delay
is added in nanoseconds and visibility is ceiling-aligned to the next poll.
Events visible exactly at a poll are applied before observation; a value one
nanosecond late waits for the next poll. Forecast state processes every newly
visible raw event in deterministic order: intermediate Futures values remain
available to causal reference selection, intermediate regressions are not
hidden by a later cache winner, and only the final per-source event becomes the
current cache-shaped observation. The established prior-Futures-history guard
still applies to a same-poll Futures reference and Chainlink anchor. Chainlink
actual history likewise retains the complete visible raw order needed for
horizon-specific target resolution.

Every selected anchor, current Futures value, Futures reference, and Chainlink
actual preserves its Decimal value, exact wall and monotonic receipt times,
floored receipt millisecond, assumed available time, visible poll, source
timestamp, connection ID, and generic raw source sequence. Publisher epoch and
accepted-event sequence are explicitly null and marked `not_captured`; the raw
sequence is not relabeled as live Redis publisher metadata.

Each origin is held through the 3,500 ms maximum target plus the frozen 200 ms
continuity/finalization allowance. Every candidate resolves its own actual as
the newest Chainlink observation whose visible poll and full nanosecond receipt
are both at or before that candidate's target. An observation first visible
after a shorter target cannot leak into that row merely because it exists by
common-cohort finalization. A clean-session/connection reset through
finalization invalidates all five candidates together without retroactively
changing generation eligibility. A source-timestamp regression still resets
forecast state and invalidates that poll's generation, but does not invalidate
an already-pending cohort by itself.

Any overlapping session that fails the strict raw/session-integrity checks
aborts this loss-free replay path. Its final counters are never used to alter an
earlier generation mask or to turn a corrupt interval into ordinary missing
data.

The replacement control is always stored under its full, separate
`ModelIdentity`, including when it aliases the numerically identical v4 3,000 ms
calculation. A distinct incumbent configuration with the same supported
forecast implementation runs in an independent one-model state machine. If its
forecast-code digest or declared forecast rules differ, replay fails closed
until a manifest-verified reconstruction is available; it never substitutes the
v4 implementation and calls that the operational control.

At this checkpoint, the validated frozen experiment contract is the trust
boundary for those forecast-code manifests. The replay runner does not claim to
derive a manifest from its own local functions. The later experiment-tree and
provenance work must verify the supplied component bytes and bind this replay
implementation separately before any output can become efficacy or promotion
evidence.

The causal output contains forecast validity, causal inputs and actuals, the
scheduled and target vectors, generation/common/decision masks, integrity
epochs, and deterministic missing reasons. It does not calculate an error,
loss, skill, ranking, bootstrap statistic, calibration winner, or holdout
decision. `iter_v4_causal_origins` streams finalized origins with bounded replay
state for a full-day evidence writer; `replay_v4_causal_signals` is the
collection convenience used by bounded tests.

This checkpoint remains offline and has no production importer. It does not
publish canonical create-once artifacts, choose a calibration or holdout day,
run seven-cell inference, inspect efficacy data, acquire an attempt-owner lock,
or alter a production decision, service, database, Redis key, API, dependency,
or deployment file. Those later work items remain untouched.

## How the model is used now

The catch-up engine runs in the standalone
`price-collector-shadow-signal` service. It remains isolated from both source
collectors, so an experimental-model failure cannot stop futures or Chainlink
collection.

Its live workflow is:

1. Read the latest futures and Chainlink cache values together every 100 ms.
2. Feed those observations into all three fixed candidate engines.
3. Publish only the accepted 3.0-second primary projection.
4. Replace an invalid projection with null fields instead of carrying an old
   valid forecast forward.
5. Give the live projection a short TTL so it disappears if the worker stops.

For evaluation, every entered epoch-aligned 500 ms bucket creates one attempt
per candidate, including invalid attempts. Each valid attempt matures at its
own horizon and is paired with the causal actual described above. Missed
buckets are not backfilled.

For a scored attempt, the stored errors are:

```text
forecast_error = projected_chainlink - actual_chainlink
baseline_error = chainlink_at_forecast - actual_chainlink
```

Positive forecast error means the projection was high; negative means it was
low. Absolute error measures distance without direction. The no-change
baseline answers the practical question: was using the futures move better
than simply assuming Chainlink would stay where it was?

Evaluation persistence uses a separate bounded queue and writer. Candidates
from one generated time are held until the maximum configured horizon matures,
then queued, retried, rejected, and retained only as a complete cohort.
Database slowness or failure is not allowed to delay the 100 ms signal loop.
Stored evaluation evidence is retained for seven days by default.

## Results from three recent five-minute markets

Each market contained 600 scored attempts at the selected 3.0-second horizon,
with no invalid attempts and no missing actuals.

| Market | Forecast MAE | Baseline MAE | MAE reduction | Forecast RMSE | Baseline RMSE | P95 absolute error | Maximum error | Average bias |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | $1.37 | $1.79 | 23.4% | $2.12 | $3.24 | $4.36 | $9.36 | $0.02 high |
| 2 | $1.34 | $2.10 | 36.3% | $2.24 | $3.57 | $4.62 | $12.84 | $0.03 low |
| 3 | $2.23 | $3.29 | 32.3% | $4.50 | $6.47 | $7.25 | $39.65 | $0.21 high |

The paired outcomes were:

| Market | Projection closer | Equal | Projection worse | Closer among non-ties |
| --- | ---: | ---: | ---: | ---: |
| 1 | 209 | 186 | 205 | 50.5% |
| 2 | 248 | 191 | 161 | 60.6% |
| 3 | 260 | 157 | 183 | 58.7% |
| **Total** | **717** | **534** | **549** | **56.6%** |

Because every market has the same 600 scored rows, the displayed summaries
give these approximate combined results:

- forecast MAE: **$1.65**, versus **$2.39** for no change;
- MAE advantage: approximately **$0.75**, or **31.2% lower**;
- pooled forecast RMSE: approximately **$3.15**, versus **$4.66**;
- RMSE reduction: approximately **32.4%**;
- paired outcomes: **39.8% closer, 29.7% equal, and 30.5% worse**; and
- average signed bias: approximately **$0.07 high**.

These combined dollar figures are approximate because they were calculated
from the rounded market summaries. Median and P95 errors cannot be pooled
correctly from summary values alone, so no combined median or P95 is claimed.

## What the results say

### The signal is adding useful short-horizon information

The projection beat no change on both MAE and RMSE in all three markets. The
improvement was not confined to one quiet interval, and the pooled reduction
was about one third. That is the right comparison for this experiment: not
whether the error is always zero, but whether transferring the already visible
futures move gives a closer three-second Chainlink estimate than doing nothing.

### The advantage also appears in paired counts and RMSE

The projection was closer more often than it was worse in every market. After
removing exact ties, it won 56.6% of the comparisons. RMSE, which penalizes
large errors more heavily than MAE, also improved by about one third. This
means the observed advantage survives an outlier-sensitive metric.

### Directional bias is currently small

The first two windows were nearly unbiased, and the approximate combined bias
was only $0.07 high compared with an approximate $1.65 MAE. There is no strong
evidence in these windows of a persistent tendency to project too high or too
low.

### Sudden moves remain the main visible weakness

The third market had a $39.65 maximum error and materially higher P95, MAE, and
RMSE. That is consistent with the behavior already observed around sharp
drops or increases, although the summaries alone cannot prove the cause of
that individual error.

The model can translate only the futures move visible when the forecast is
generated. It cannot anticipate a new shock, continuation, or reversal that
occurs during the following three seconds. A fixed three-second lag and beta of
1 can also be imperfect when Chainlink's catch-up speed or pass-through changes
with market conditions. Those are expected limits of this V0 model, not data
that should be hidden from its evaluation.

### Evaluation coverage was operationally clean in these windows

All 1,800 attempts were scored, with zero invalid attempts and zero valid
forecasts missing an actual. That confirms complete evaluation coverage for
these three windows. It does not prove that every future window will have the
same coverage, especially through feed gaps, reconnects, or service outages.

## What the results do not prove

These three markets are encouraging evidence, but they contain only 15 minutes
of observations in total. They also contain overlapping observations: a
forecast is attempted every 500 ms for a 3-second horizon, so roughly six
forecast horizons are active at once. The 1,800 rows are therefore highly
autocorrelated and should not be described as 1,800 independent trials or as a
statistical-significance result.

The results do not yet establish:

- performance across all volatility and liquidity regimes;
- profitability after latency, spread, slippage, and execution constraints;
- accuracy at the five-minute settlement boundary;
- that 3.0 seconds will always remain the best lag; or
- that the largest errors are acceptable for a trading decision.

They do support a more limited and defensible conclusion:

> In these three recent markets, the frozen 3-second catch-up model produced a
> materially closer estimate of the causally observed Chainlink value than
> carrying the current Chainlink value forward unchanged.

## When to reconsider or retune the model

The model should not be changed because of one dramatic miss or because a few
recent markets favor another setting. Retuning becomes justified after enough
new evidence covers quiet periods, trends, reversals, sudden shocks, feed
reconnects, and different times of day.

Any future tuning should repeat the same discipline:

1. Preregister a candidate grid that extends below 2.0 seconds so another
   lower-bound winner does not immediately trigger a second search.
2. Treat every already inspected window as calibration and freeze lag, beta or
   deadband, reference-gap limits, timing-delay scenarios, phase offsets, and
   the robustness rule before the new holdout.
3. Replay every candidate on identical common cohorts and keep no change as the
   horizon-matched paired baseline.
4. Report full confusion metrics, daily/session skill, non-overlapping-origin
   sensitivity, and paired moving-block-bootstrap intervals. The block length
   must exceed both forecast overlap and the update-dependence period.
5. Freeze the calibration winner before evaluating one strictly later,
   untouched multi-window holdout; do not rerank or fall back on holdout.
6. Promote a new immutable decision artifact only if all preregistered error,
   uncertainty, timing-robustness, and operational-coverage gates pass.

Until then, the current production behavior should remain stable: publish only
`catchup_ratio_l3000_b100`, continue evaluating the fixed candidates silently,
and treat the signal as shadow/experimental evidence rather than an
authoritative future price.

## Which files are responsible

`engine.md` is the original design document. It explains the hypothesis and
recommended architecture, but it is not executable and does not run in
production.

The executable Actual vs. Projected Chainlink path is divided by
responsibility:

| Responsibility | File | What it does |
| --- | --- | --- |
| Core mathematical engine | [`price_collector/shadow_signal.py`](price_collector/shadow_signal.py) | Defines the candidate models, Chainlink anchors, futures-reference lookup, validity checks, and anchored-ratio projection. This is the main “engine.” |
| Live worker | [`price_collector/shadow_signal_collector.py`](price_collector/shadow_signal_collector.py) | Runs the 100 ms Redis loop, feeds observations into the engine, publishes the frozen primary, and passes attempts to evaluation. |
| Causal actual and error calculation | [`price_collector/shadow_signal_evaluation.py`](price_collector/shadow_signal_evaluation.py) | Schedules 500 ms attempts, matures each horizon, chooses the latest eligible actual, calculates forecast and baseline errors, and manages the nonblocking writer queue. |
| Evaluation database writes | [`price_collector/db.py`](price_collector/db.py) | Inserts evaluation records and performs bounded retention cleanup. |
| Evaluation database schema | [`schema.sql`](schema.sql) | Defines `shadow_signal_evaluations`, its constraints and privileges, and the narrow reporting view. |
| Redis signal encoding | [`price_collector/live_cache.py`](price_collector/live_cache.py) | Strictly encodes, writes, reads, and validates the short-lived shadow payload. |
| Historical replay | [`price_collector/shadow_signal_replay.py`](price_collector/shadow_signal_replay.py) | Replays raw futures and Chainlink evidence through the same core engine and produces candidate metrics. |
| Model selection | [`price_collector/shadow_signal_selection.py`](price_collector/shadow_signal_selection.py) | Applies the chronological calibration/holdout policy and creates the frozen selection decision. |
| Runtime decision validation | [`price_collector/shadow_signal_artifact.py`](price_collector/shadow_signal_artifact.py) | Verifies the selection artifact, replay configuration, hashes, candidate set, and primary before activation. |
| Per-market performance calculation | [`price_collector/shadow_signal_reporting.py`](price_collector/shadow_signal_reporting.py) | Reads validated evaluation points and derives MAE, RMSE, bias, baseline skill, and paired outcomes. It reports on the engine; it does not execute the model. |
| Read-only route wiring | [`price_collector/api.py`](price_collector/api.py) | Exposes live and persisted evaluation responses. It does not import or run the mathematical engine. |
| Settings | [`price_collector/config.py`](price_collector/config.py) | Defines and validates worker, artifact, cadence, evaluation, queue, and retention settings. |
| Production process | [`deployment/price-collector-shadow-signal.service`](deployment/price-collector-shadow-signal.service) and [`deployment/shadow-signal.env.example`](deployment/shadow-signal.env.example) | Define the isolated systemd service and its environment contract. |

The central live path is:

```text
futures + Chainlink Redis values
        -> shadow_signal_collector.py
        -> shadow_signal.py
        -> shadow_signal_evaluation.py
        -> db.py / shadow_signal_evaluations
```

The model-selection path is separate:

```text
raw capture
        -> shadow_signal_replay.py
        -> shadow_signal_selection.py
        -> frozen selection artifact
        -> shadow_signal_artifact.py
        -> live worker activation
```

If the question is specifically “where would we change the formula?”, the
answer is `price_collector/shadow_signal.py`. If the question is “where is
projected paired with actual?”, the answer is
`price_collector/shadow_signal_evaluation.py`.

## Related implementation references

- [`engine.md`](engine.md) — original signal definition and model rationale
- [`README.md`](README.md) — implemented replay, selection, worker, and
  evaluation behavior
- [`OPERATIONS.md`](OPERATIONS.md#shadow-signal-phase-2-raw-replay) — replay
  and selection procedures
- [`price_collector/shadow_signal.py`](price_collector/shadow_signal.py) — pure
  anchored-ratio engine
- [`price_collector/shadow_signal_evaluation.py`](price_collector/shadow_signal_evaluation.py)
  — causal maturation and evaluation records
