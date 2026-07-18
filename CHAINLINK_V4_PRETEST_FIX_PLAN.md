# Chainlink v4 compact 24-hour test plan

## Scope

This replaces the earlier eight-phase plan. It keeps the controls needed to
answer the lag question and moves unrelated production work off the critical
path.

The experiment is offline. It must not change or restart the Binance or
Chainlink producers, Redis, FastAPI, PostgreSQL, the active shadow worker, the
production virtual environment, systemd, or the installed decision files. Raw
replay is the selection authority.

The supplied schema-v2 files use `reference_max_gap_ms=3000` and
`max_future_skew_ms=250`; v4 uses `250` and `0`. Those files are historical
evidence, not proof of what is installed. The exact deployed selection and
replay configuration loaded by the active invocation, plus its matching
installed state, must be proven from the droplet before calibration.

Repository baseline: `739 passed, 11 skipped`.

## Decision the test produces

Calibration first answers whether one of `1500`, `2000`, or `2500` ms is an
eligible, robust shorter-lag winner. If `3000` or `3500` ms wins, or no shorter
candidate clears every calibration gate, the process emits a calibration-stage
`retain_incumbent` result and stops without spending the future-day holdout. If
calibration quality, archive, causality, or provenance cannot support a
conclusion, it emits calibration-stage `insufficient_evidence` instead.

If a challenger is frozen, the single efficacy-bearing holdout answers whether
that exact lag beats both its own horizon-matched no-change baseline and the
active 3000 ms forecast rule under the shared offline replay contract on the
complete 500 ms decision stream, without becoming timing-fragile in the six
rejection cells. Each unique `experiment_id` emits exactly one create-once
terminal result. A bounded quality-only retry uses a new `experiment_id` in the
same `calibration_lineage_id`, so a lineage retains every prior terminal attempt
artifact. Its operative conclusion is the latest completed authorized attempt's
result; while an authorized successor is outstanding, lineage status is
`pending` and no operative conclusion is asserted. Every result is exactly one
of:

- `insufficient_evidence`: data quality, coverage, archive, causality, or
  provenance was inadequate, so the test makes no performance claim;
- `retain_incumbent`: usable calibration did not justify an eligible shorter
  challenger, or the frozen challenger missed at least one holdout efficacy or
  robustness gate; this is terminal for that calibration/challenger;
- `promotion_eligible`: every frozen gate passed, so the challenger may advance
  to a separate production-promotion review; nothing is activated by this test.

It does not answer profitability, execution quality, settlement accuracy,
measured Redis latency, or live-production safety.

`efficacy_attempt_consumed` always refers to the single future holdout efficacy
attempt, not calibration loss generation. It is `false` for a terminal
calibration-stage result.

## Frozen design

### Family and timing cells

| Item | Frozen value |
| --- | --- |
| Policy | `chronological_holdout_v4_shorter_challenger_24h` |
| Comparison family | `1500, 2000, 2500, 3000, 3500` ms |
| Promotion-eligible set | `1500, 2000, 2500` ms only |
| Incumbent comparison member | `3000` ms |
| Non-promotable guardrail | `3500` ms |
| Model rule | `beta=1`; lag equals horizon |
| Reference gap / future skew | `250` ms / `0` ms |
| Poll / generation cadence | Epoch-aligned `100` ms / `500` ms |
| Ranking metric | MAE skill versus each horizon's matched no-change baseline |
| RMSE | Confirmation only; never ranks or breaks an MAE tie |
| Holdout | At most one efficacy-bearing exact future UTC day, `[start_ms,end_ms)`; one bounded quality-only successor |
| Rerank / fallback / dynamic switching | Prohibited |

This is a shorter-only promotion policy, not a full-grid primary-selection
policy. All five models remain in the ordered `comparison_family` and its common
cohort. The `3500` ms model is a non-promotable longer-lag guardrail/comparator:
it can defeat or reject a shorter candidate during calibration, but it can never
be frozen, promoted, or replaced by a shorter runner-up. The `3000` ms model is
the incumbent-aligned v4 comparator and is likewise non-promotable by this test.

Use exactly seven cells over the same raw input and scoring window:

| Cell | Futures delay | Chainlink delay | Phase | Role |
| --- | ---: | ---: | ---: | --- |
| `canonical_p0` | 100 ms | 100 ms | 0 ms | Rank, estimate, bootstrap, decide |
| `canonical_p100` | 100 ms | 100 ms | 100 ms | Robustness only |
| `canonical_p200` | 100 ms | 100 ms | 200 ms | Robustness only |
| `canonical_p300` | 100 ms | 100 ms | 300 ms | Robustness only |
| `canonical_p400` | 100 ms | 100 ms | 400 ms | Robustness only |
| `futures_slower_p0` | 200 ms | 100 ms | 0 ms | Robustness only |
| `chainlink_slower_p0` | 100 ms | 200 ms | 0 ms | Robustness only |

Never pool cells. Only `canonical_p0` can select or promote. The other six may
only reject a timing-sensitive winner. Delays are sensitivity assumptions, not
measured Redis/worker latency.

### Offline replacement control

Use these terms:

- `v4_3000_candidate`: the 3-second model under the complete v4 configuration.
- `operational_incumbent`: the exact primary artifact/configuration loaded by
  the active runtime invocation at the pre-calibration provenance/control
  freeze, which must match the relevant installed state at that freeze.
- `offline_replay_replacement_control`: the operational incumbent's exact
  3-second forecast rule and settings, evaluated under the same event-complete
  causal raw-replay contract as the v4 family.
- `frozen_challenger`: the eligible `1500`, `2000`, or `2500` ms v4 calibration
  winner.

Record `active_incumbent_selection_sha256`,
`active_incumbent_replay_config_sha256`,
`active_incumbent_primary_model_version`,
`active_incumbent_forecast_config_digest`,
`active_incumbent_non_lag_forecast_config_digest`,
`v4_3000_forecast_config_digest`, `v4_non_lag_forecast_config_digest`,
`active_incumbent_forecast_code_digest`, `v4_forecast_code_digest`, and one
`offline_evaluation_policy_digest`.

Define the digests over canonical JSON and keep their responsibilities
separate:

- `forecast_config_digest` covers lag, horizon, beta, staleness, gap/skew,
  history retention, anchor and Futures-reference selection, projection, and
  forecast validity. It excludes role/version labels and evidence/report
  metadata. Derive each
  `*_non_lag_forecast_config_digest` from the same inputs with only lag and
  horizon omitted.
- `offline_evaluation_policy_digest` covers the ordered five-member comparison
  family, the shorter-only promotion-eligible set, the incumbent and guardrail
  roles, each ordered forecast-config identity, the separate offline-control
  role, horizon-matched baseline construction/pairing, common-cohort and
  decision-cohort rules, target resolution, poll and generation cadences/origin
  scheduling, poll/tie order, missing-origin treatment, finalization and
  continuity rules, the exact seven delay/phase cells, and the raw-replay
  delivery-metadata semantics.
- Each forecast-code digest covers only anchor formation, Futures-reference
  selection, projection, and forecast validity. Bind the replay/evaluation code
  separately through the experiment tree and `offline_evaluation_policy_digest`.

Comparison-family membership cannot be omitted from a digest that claims to
cover an all-model common cohort.

- Require the active primary to remain exactly `lag_ms=3000`,
  `horizon_ms=3000`, and `beta=1`. Otherwise stop and redesign this policy.
- Use `v4_3000_candidate` as the offline replacement control only when its
  forecast configuration and forecast-code digests match the incumbent's.
- Otherwise replay the exact active 3-second forecast implementation/settings
  as a non-selectable forecast control over the same raw data, offline
  evaluation policy, and seven timing cells.
- A mismatched result is a v4-family lag selection plus an operational
  replacement test; it is not a purely lag-only production change.
- When the non-lag forecast digests differ, state the decision question
  explicitly as replacing the incumbent forecast configuration with the
  complete v4 challenger configuration under the offline replay contract.
- If the active forecast rule/settings cannot be reconstructed, treat it as a
  provenance failure and emit `insufficient_evidence` before efficacy whenever
  possible; do not continue to a performance conclusion.

Raw capture does not contain the publisher epochs, accepted-event sequences,
cache overwrites, or failed read history needed to reproduce the complete live
evaluator. Therefore the offline replacement control is not called the exact
live operational control, and `promotion_eligible` does not establish live-
evaluator or deployment equivalence. Requiring that stronger claim would need
prospective delivery-metadata capture and is outside this offline-only test.

Capture and bind the proven loaded active selection/configuration, matching
installed state, invocation-start record, and relevant model/producer hashes at
the pre-calibration provenance/control freeze. If a
challenger advances, recheck them immediately before preregistration, at holdout
start, holdout end, and final analysis, and use the interval-wide ledger below to
exclude an intervening deploy/revert between checkpoints. Any unexpected change
invalidates the compatible calibration and produces `insufficient_evidence`.

Serialize identity as `(model_role, model_version, forecast_config_digest,
offline_evaluation_policy_digest)`, not model name alone, so an active control
and v4 candidate with the same version string remain distinct.

### Attempt identity, deterministic windows, and bounded retries

Create one stable, random `calibration_lineage_id` before any evidence window is
chosen. Give the initial attempt and every permitted successor a distinct random
`experiment_id`; neither identifier may encode market conditions or be reused.
Use separate zero-based `calibration_attempt_index` and
`holdout_attempt_index`; the latter is canonical JSON `null` until the initial
holdout-selection authorization is published, then `0`, and becomes `1` only
for its permitted successor.
A successful calibration keeps its `experiment_id` through the initial holdout
and receives that ID's sole terminal result there; a terminal calibration result
uses it earlier with the holdout index still `null`. Only a quality-only
successor gets a new ID. A holdout successor inherits the exact completed
calibration freeze and advances only `holdout_attempt_index`.

Every terminal result binds both identifiers, both stage-local indexes, its
terminal stage, parent result/hash when present, and the retry eligibility/
budget as of publication. A successor authorization later binds that immutable
parent. The latest result binds the complete prior-result/authorization ancestry
available at its publication; a create-once parent is never expected to predict
a future successor.

Choose windows mechanically, without price, volatility, news, calendar-event,
or efficacy inputs. Freeze `calibration_attempt_freeze_lead_ms=3_600_000`,
`minimum_preregistration_lead_ms=86_400_000`, and
`preregistration_publication_allowance_ms=3_600_000`, and
`max_candidate_days_per_selection=7`:

- Calibration is one exact midnight-aligned UTC day. The initial window is the
  earliest such day whose full archive input interval, including warmup, starts
  at least the calibration-freeze lead after the create-once pre-calibration
  provenance freeze, whose attempt-freeze publication deadline has not passed,
  and whose frozen pre-window readiness checks pass. Publish
  `calibration_attempt_freeze.json` no later than
  `archive_input_start_ms - calibration_attempt_freeze_lead_ms`; otherwise the
  attempt ends loss-free as `insufficient_evidence`.
- A calibration quality-only successor is the earliest such day after a
  create-once retry-eligibility record proves the failed prerequisite is
  objectively restored and whose full archive input starts at least the attempt-
  freeze lead after `retry_eligibility.created_at`; the identical attempt-freeze
  deadline/lead applies.
- When calibration completes with an eligible shorter winner, publish
  `holdout_selection_authorization.json` as the next decision artifact after the
  mandatory `final_analysis_checkpoint`, under the same owner lock. It binds the
  calibration completion marker/winner and uses that marker's `completed_at` as
  immutable `selection_anchor_ms`; delaying artifact publication cannot move the
  anchor.
- The initial holdout is the earliest midnight-aligned future UTC day whose full
  archive input interval starts at least the minimum lead plus publication
  allowance after `selection_anchor_ms` and whose frozen pre-window readiness
  checks pass. A replacement uses `retry_eligibility.created_at` as its anchor;
  only after the first passing entry does its successor authorization bind the
  exact selected window. Its pushed-commit receipt must be published by
  `archive_input_start_ms - minimum_preregistration_lead_ms`.
  Missing that deadline emits attempt-terminal `insufficient_evidence` with both
  holdout efficacy markers false and `efficacy_attempt_consumed=false`; it counts
  against, but may use, the one holdout quality-only successor. It never silently
  moves the day.

The pre-window readiness schema contains exactly the Phase 3 archive-boundary,
partition-pair, capture/maintenance, headroom, seal-feasibility, provenance, and
publication-path-readiness checks, with every input, threshold, and evaluation
deadline frozen in the policy. Day selection does not require the not-yet-created
preregistration or receipt; the actual pushed receipt is the independent post-
selection deadline gate above. Readiness contains no price, volatility, news,
calendar-event, loss, or efficacy input and permits no discretionary pass.

For each stage, publish candidate-day entries as a hash-chained sequence of
create-once files beginning at the authorization-bound earliest day. Publish
each calibration entry by `archive_input_start_ms -
calibration_attempt_freeze_lead_ms` and each holdout entry by
`archive_input_start_ms - minimum_preregistration_lead_ms -
preregistration_publication_allowance_ms`. The canonical entry body binds the
candidate window, deadline, `evaluated_at`, exact readiness values, source-
artifact hashes, pass/fail reasons, `previous_entry_hash`, and the current
`provenance_continuity_root`; the entry's own SHA-256 becomes the new candidate-
ledger root and is not embedded in its hashed body. Never rewrite or backdate an
entry or resample readiness opportunistically after its deadline.

The watchdog normally publishes by the deadline. If it does not, the first
recovery after the deadline publishes a create-once late `deadline_missed`
failure entry with its actual publication time; that entry can never pass and
counts toward the seven-day cap. The first timely passing day is mandatory. A
failed day or missed deadline never grants discretion to skip ahead.
After seven consecutive candidate entries without a passing day, the allocated
experiment emits one loss-free `insufficient_evidence` result with
`candidate_days_exhausted=true`, absent efficacy markers, and the applicable
stage's single-successor budget; scanning never continues indefinitely.

Before each calibration archive input interval, publish a create-once
`calibration_attempt_freeze.json` binding the lineage/experiment IDs, both stage-
local indexes, exact chosen window and archive bounds, candidate-day ledger root,
complete policy/control/config/code/environment identities, provenance-freeze hash,
readiness artifacts, quality gates, and retry state. This is the calibration
counterpart of the later holdout preregistration; quality and efficacy commands
must accept no unbound range or setting.

Allow at most one quality-only replacement calibration range and at most one
quality-only replacement holdout per lineage: initial attempt plus one successor
at each stage. First publish `retry_eligibility.json` binding the parent result/
hash and failure kind plus objective evidence that the failed prerequisite or
publication path is ready, and allocating the successor's new `experiment_id`
and stage-local indexes before its candidate scan. For a missed lead it also
binds the failed deadline, actual receipt state/timestamps, and unchanged no-
efficacy markers. After the first passing candidate entry, publish one create-
once successor authorization binding that eligibility record, allocated ID,
exact window, unchanged policy/calibration/control/provenance identities, and
remaining retry count. One parent may allocate/authorize only one successor. The
last permitted attempt's single
`insufficient_evidence` result records `retries_exhausted=true` and closes the
lineage; exhaustion never creates a second result for the same ID.

No successor is allowed after the applicable efficacy-start marker exists,
after `retain_incumbent` or `promotion_eligible`, after a relevant policy/code/
configuration/control/provenance change, or when a frozen gate is structurally
infeasible. Those cases close the lineage or require a new policy and a wholly
new calibration lineage; they cannot be called quality-only retries.

### Crash-safe efficacy state

After all loss-free quality gates pass, but before entering any code path that
reads, derives, computes, returns, logs, or persists a prediction error, loss,
skill, ranking, or bootstrap value, durably publish the applicable create-once
marker: `calibration_efficacy_started.json` or
`holdout_efficacy_started.json`. Quality code may read raw observations only to
form causal/validity/coverage masks; it must not materialize loss values.

Publish each marker atomically and without replacement: write and `fsync` a
same-directory temporary file, exclusively publish it with `linkat`/equivalent
no-replace semantics, `fsync` the directory, then clean up the temporary name.
Never use `os.replace`. A malformed or otherwise ambiguous marker fails closed
as already started/consumed. Each start marker binds the lineage and experiment
IDs, both stage-local indexes, stage/type, exact window, raw-manifest hash, all
seven completed quality-artifact hashes, policy/control/config/code/environment/
provenance hashes, owner identity, and `started_at`. The calibration marker binds
the `calibration_attempt_freeze.json` hash. The holdout marker binds both the
preregistration hash and pushed-receipt hash, receipt/deadline-check hash,
observed remote ref, and receipt verification time.

Before publishing a start marker, acquire and hold an attempt-specific exclusive
OS lock through the completion marker and next terminal or holdout-selection-
authorization artifact.
Bind host boot ID, PID, and process-start identity as the marker owner. A process
that loses the lock exits without running efficacy and without publishing a
failure while the owner is alive. An existing valid start marker never licenses
a second efficacy execution. Recovery may act only after acquiring the lock and
proving there is no live lock owner; for a valid marker, also verify that its
recorded owner is dead. Without a completion marker recovery emits the consumed
`insufficient_evidence` result without recomputation; with a valid completion
marker it performs only deterministic finalization from bound artifacts. For a
conflicting/malformed marker, bind its raw-file hash and parse/conflict reason in
the terminal result. Never let a racing process overwrite or execute efficacy.

Use separate create-once `calibration_efficacy_completed.json` and
`holdout_efficacy_completed.json` markers. The required order is:

```text
loss-free quality pass
-> pre-efficacy provenance gate
-> efficacy-start marker
-> immutable efficacy ledger/report/bootstrap artifacts
-> efficacy-completed marker binding their hashes
-> final-analysis checkpoint
-> terminal result, or holdout-selection authorization
```

Each completion marker binds the start-marker SHA-256, lineage/experiment IDs,
both stage-local indexes, stage/type/window, the identical policy/input/control/
code/environment/provenance identities, a complete ordered inventory and hashes
of every immutable efficacy artifact, and `completed_at`. Verify the complete
inventory and its hashes before publishing the marker; a completion marker may
not add, omit, or substitute an attempt or artifact later.

The machine-readable state is authoritative:

| State | Started | Completed | Consequence |
| --- | --- | --- | --- |
| Quality failure | false | false | No efficacy; bounded quality-only successor may be authorized |
| Calibration efficacy completed | true | true | Calibration range is immutable; freeze or terminal calibration decision |
| Crash/failure after start but before completion | true | false | Non-retryable `insufficient_evidence`; expose no efficacy values |
| Holdout efficacy completed | true | true | Holdout consumed; emit `retain_incumbent` or `promotion_eligible` |
| Completed-marker artifact integrity failure | true | true | Non-retryable `insufficient_evidence`; expose no efficacy values |

Any holdout start marker sets `efficacy_attempt_consumed=true`; no marker keeps
it false. Calibration records the independent
`calibration_efficacy_started/completed` booleans even though the holdout-only
flag remains false. After a completed marker but before the next artifact, the
process may deterministically finalize only from the immutable hashes already
bound by that completion marker. Before completion it must never resume or
recompute efficacy. Every reference below to an artifact that "later" fails
integrity means failure discovered during this pre-result verification. Once a
create-once terminal result exists, never emit a second result for that ID; any
post-result evidence revocation would require a separately specified artifact
and is outside this plan.

### Prospective producer and deployment provenance

This policy permits prospective calibration only. Publish a create-once
`precalibration_provenance_freeze.json` before the selected calibration
`archive_input_start_ms`; every warmup, scoring, finalization-tail, and sealing
input must therefore be produced after the freeze. Raw rows or session records
from before that freeze are not made compatible by a later endpoint snapshot.
A future policy may admit retrospective evidence only with a separately trusted
per-interval provenance source that reconstructs every relevant transition; this
policy does not.

For `price-collector-binance-futures`,
`price-collector-polymarket-chainlink`, and
`price-collector-shadow-signal`, bind both the installed state and the state
actually loaded by each active invocation: repository commit/tree and relevant
source hashes, production Python and installed distributions, systemd unit/drop-
ins, digest of all relevant nonsecret effective settings, and active selection/
replay artifact identities. Also bind the experiment environment/code
separately. Do not copy secret values into an artifact.

For every invocation that predates the provenance freeze, require a trustworthy
startup/deployment record that binds its exact systemd `InvocationID`, host boot
ID, `MainPID`, and `ExecMainStartTimestampMonotonic` to those loaded identities,
with uninterrupted lifecycle evidence from invocation start through the freeze.
At the freeze, the relevant installed identities must match the proven loaded
identities. A startup journal message or the identity fields from a strictly
decoded current shadow payload may be hashed as corroboration, but neither
substitutes for that binding. If the record
is unavailable or loaded/installed state differs, emit `insufficient_evidence`;
the controlled restart that could establish a fresh identity is outside this
offline no-restart plan.

Start one provenance-continuity ledger as a hash-chained sequence of create-once
entries before the pre-calibration freeze and keep it gap-free through every
calibration/holdout attempt, retry interval, pushed receipt, seal, and final
analysis. Its monitor binds journal cursor ranges and lifecycle events plus file/
config transition observations and before/after hashes for the repository,
production environment, unit/drop-ins, nonsecret settings, and decision
artifacts. Captured systemd-
journal slices are lifecycle inputs to this ledger; journal evidence alone is
not sufficient to detect checkout, environment, unit, or decision-file edit-and-
revert cycles. Any cursor, monitor, or hash-chain gap is
`insufficient_evidence`.

Define the rolling root without self-reference. Each canonical entry stores its
segment/index and `previous_entry_hash` (`null` only for the first genesis); its
SHA-256 over those canonical bytes becomes both `entry_hash` and the new
`provenance_continuity_root`. Do not include a derived current-root display field
in the hashed body. At the provenance freeze, each archive start, each six-hour
rollover, scoring end, seal, preregistration/pushed receipt, successor
authorization, and other frozen stage boundary, publish a create-once checkpoint
entry with the current service/process/loaded/installed identities and prior
root.

If operational segmentation is unavoidable, the new segment's genesis binds the
prior segment's final root and the shared overlap-checkpoint hash; linkage is one-
way and the prior segment never predicts a future root. Otherwise the chain has a
fatal gap. A later return to an old identity therefore cannot hide an intervening
deploy/revert, including between attempts or preregistration and push.

Every recorded unit/process transition must have before/after checkpoints and
must preserve the relevant code/config/artifact identities. A recorded restart
with identical identities may remain compatible when the raw session-integrity
evidence covers its feed gap; an unrecorded transition or a relevant identity
change makes the lineage `insufficient_evidence`. WebSocket reconnects are feed-
session events, not deployments, and remain governed by the raw archive/session
integrity and coverage rules. Revalidate the frozen calibration/control
identities at every required checkpoint and continue the same chain through the
terminal path.

After loss-free quality and sealing pass, but before the efficacy-start marker,
hold the attempt-owner/provenance coordination lock, drain the monitor through a
frozen watermark, publish `pre_efficacy_provenance_gate`, verify its root remains
current, and immediately publish the start marker; a mismatch caught before the
marker is loss-free. An intervening transition discovered after the marker is
consumed even if its source time precedes marker publication. Keep the monitor
running throughout efficacy. After efficacy artifacts/completion, drain all
intervening events and publish `final_analysis_checkpoint` immediately before
the terminal result or holdout-selection authorization. Any relevant transition
after the start marker produces consumed/post-efficacy `insufficient_evidence`
with no metrics. A path that terminates before that stage instead publishes a
stage-appropriate terminal checkpoint immediately before its result.

Each terminal result binds the latest stage-available immediately preceding
checkpoint root; an attempt that reaches final analysis must bind
`final_analysis_checkpoint`. A calibration holdout-selection authorization binds
that same final checkpoint and continues the open chain through holdout. Only
after the lineage's terminal result append a closure entry binding that result
hash; the closure's derived root is the final ledger root and is recorded in a
create-once sidecar, avoiding result/root self-reference.

### Causality and cohorts

The four model observations are the Chainlink anchor, current Futures value,
lagged Futures reference, and future Chainlink actual. For each, preserve
receipt time, simulated visible time, source timestamp, connection ID, and a
generic `source_sequence`. Futures uses `last_agg_trade_id`; Chainlink uses
`receive_sequence`. Raw capture does not contain Redis publisher epoch or
accepted-event sequence; serialize those as explicit `null`/`not_captured`
rather than fabricating them.

```text
available_wall_ns = received_wall_ns + source_delay_ms * 1_000_000
poll_ns = poll_ms * 1_000_000
visible_tick_index = (available_wall_ns + poll_ns - 1) // poll_ns
visible_ms = visible_tick_index * poll_ms
received_ms = received_wall_ns // 1_000_000

every input:             input_visible_ms <= generated_ms
every v4-family actual:  actual_visible_ms <= target_ms
```

At each poll, apply available events before observation/maturation/generation.
Define raw order as `(received_wall_ns, source_kind_order,
received_monotonic_ns, source_sequence, connection_id)`, with Futures kind `0`
and Chainlink kind `1`; define visibility-queue order as
`(available_wall_ns, raw_order)`. Exact-generation and exact-target events are
eligible; anything later is not.

Freeze the current anchor/reference model exactly:

```text
when a new Chainlink event becomes visible:
    chainlink_anchor = that event
    reference_target_ms = chainlink_anchor.received_ms - lag_ms
    futures_reference = newest already-visible Futures observation whose
                        original received_ms <= reference_target_ms
    reference_gap_ms = reference_target_ms - futures_reference.received_ms
    require reference_gap_ms <= 250

at generated_ms:
    futures_now = newest Futures observation visible by generated_ms
    projected_chainlink =
        chainlink_anchor.price * futures_now.price / futures_reference.price
    matched_no_change_prediction = chainlink_anchor.price
    target_ms = generated_ms + lag_ms

at target_ms:
    actual = newest Chainlink event visible by target_ms whose original
             received_wall_ns <= target_ms * 1_000_000
```

Visibility controls when an event may be used; original `received_ms` controls
staleness and the reference target/gap. Staleness threshold equality is fresh;
one millisecond over is stale. A stale new Chainlink event clears the old anchor
without creating a new one, and a stale Futures event is not added to reference
history. The selected reference remains attached to its anchor and is not
recomputed at each generation. `source_timestamp_ms` does not drive staleness
age, the reference target or gap, target eligibility, or actual pre-target
eligibility. It is part of each observation's identity and drives the existing
per-source timestamp-regression watermark and its associated reset/invalidation
behavior; retain it for those behaviors and for provenance.

All frozen generation phases and horizons are multiples of the 100 ms poll, so
every v4 target is poll-aligned. Preserve the generic replay rule as well:
mature at the first poll at or after a target, after applying newly visible
events, then require the actual's full original `received_wall_ns` to be at or
before the target. Assert v4 target alignment rather than silently changing
generic non-aligned replay behavior.

Preserve current same-poll behavior: snapshot whether Futures history existed
before the poll, ingest newly visible Futures before the new Chainlink anchor,
and allow that same-poll Futures event into reference lookup only when prior
history existed and its original receipt time meets the target. Add a direct
v3/v4 3-second regression fixture for this rule.

```text
target_eligible:
    scheduled generated_ms is inside the declared scoring window
    AND generated_ms + 3500 < scoring_window_end_ms

generation_eligible:
    target_eligible AND all five forecasts are valid at generated_ms

common_scored:
    generation_eligible
    AND every horizon has its own causal actual
    AND no integrity reset occurs through maximum-target finalization

decision_eligible:
    common_scored
    AND offline_replay_replacement_control forecast is valid
    AND that control has its own causal actual
```

The frozen challenger is always one of the five models already required by
`common_scored`, so this control-paired definition is usable before selection.
Use it unchanged for calibration ranking and holdout promotion; it prevents a
different incumbent configuration from removing a nonrandom set of origins only
at holdout time.

The existing `200` ms two-poll allowance is continuity/finalization time only;
it never extends a horizon or admits a post-target actual. Coverage denominators
use `target_eligible`, never a future outcome.

```text
common_scored_coverage = common_scored / target_eligible
decision_eligible_coverage = decision_eligible / target_eligible
```

Report `generation_eligible / target_eligible` separately as a diagnostic.

## Phase 1 — Isolated v4 replay core

### Work

1. Add one offline v4 experiment module containing the immutable v4 policy,
   offline replacement-control logic, and strict preregistration/result
   validation.
2. Add an explicit v4 mode to replay without changing schema-v3 defaults or
   refactoring v2/v3 selection/artifact/runtime paths.
3. Implement the visibility, tie-order, horizon-specific actual, cohort, and
   full-cohort invalidation rules above.
4. Emit canonical create-once reports and ordered JSONL ledgers, with Decimal
   strings, sorted by `(cell_id, generated_ms, model_role, model_version,
   forecast_config_digest, offline_evaluation_policy_digest)` and bound to the
   SHA-256 of their uncompressed bytes.
   Every raw manifest, quality report, efficacy ledger, lineage/day/provenance
   entry, freeze/authorization, start/completion marker, receipt/check,
   preregistration, and final result has explicit `artifact_type` and
   `schema_version` fields.
5. Keep evidence role-specific:
   - loss-free calibration quality/validity for all five plus the offline
     replacement control, then all-five calibration losses only on quality pass;
   - holdout quality/validity for all five plus the offline replacement control,
     but no losses;
   - after quality passes, holdout losses only for challenger and control.
6. Implement the stable-lineage/unique-attempt state machine, deterministic UTC-
   day chooser and append-only candidate-day ledger, stage-local indexes,
   bounded successor authorization, and exactly-one-terminal-result validation.
7. Implement one shared durable create-once artifact helper plus strict schemas
   for both efficacy-start and completion markers, with attempt-owner locking and
   dead-owner recovery. The loss-bearing entry points must require a valid start
   marker and the result/preregistration paths must derive their state only from
   the persisted markers and immutable artifacts.

### Affected files

- `price_collector/shadow_signal_replay.py`
- New: `price_collector/shadow_signal_experiment.py`
- `tests/test_shadow_signal_replay.py`
- New: `tests/test_shadow_signal_experiment.py`
- `CHAINLINK_ACTUAL_VS_PROJECTED.md`

### Tests

- Mutate every v4 candidate/config field and fail closed; v2/v3 fixtures and
  defaults remain unchanged.
- Cover matching/mismatching incumbent forecast digests and prohibit a control
  identified only by model name.
- Golden-test canonical full/non-lag forecast digests: changing lag alters only
  the full forecast digest, while changing beta, history retention, or another
  shared forecast rule alters both. Changing family/cohort, matched-baseline
  construction/pairing, cadence/origin scheduling, poll/tie order, missing
  handling, actual resolution, continuity, or a delay/phase cell must alter the
  evaluation-policy digest without falsely altering the forecast digest.
- Cover receipt/visibility disagreement, exact ties, one-nanosecond-late events,
  same-poll anchor/reference behavior, and later-confirmation leakage. The v4
  3-second model must match a v3 fixture when all effective settings match.
- Accept references exactly at the target and 250 ms gap, reject 251 ms, and
  cover millisecond-floor staleness versus the full-nanosecond actual cutoff.
- With receipt times increasing but source timestamps decreasing, prove v3/v4
  both emit `timestamp_regression`, reset the same forecast state, and make
  every model at that generation invalid, so that generation is not
  `generation_eligible`/`common_scored`. A source-timestamp regression alone
  must not be reinterpreted as invalidating already-pending cohorts.
- Prove generation eligibility has no future facts and all five candidates are
  invalidated together on a pre-finalization integrity reset.
- Prove calibration/quality/holdout ledger schemas cannot expose disallowed
  losses; reject tamper and overwrite.
- Golden-test deterministic initial/replacement window selection, every skipped-
  day reason, fixed evaluation/publication deadlines, append-only entry hashes,
  seven-day scan exhaustion, the completion-time selection anchor and required
  artifact order, the one-successor-per-parent rule, retry exhaustion, stable
  lineage plus unique experiment IDs/stage-local null/value transitions,
  successful-calibration ID continuation, and exactly one terminal result per
  experiment ID. Reject late/backdated/rewritten readiness entries and an
  unbound or late calibration-attempt freeze; prove a late `deadline_missed`
  entry can never pass, advances the external candidate-ledger root without
  self-reference, and counts toward exhaustion.
- Race concurrent owner/marker acquisition and prove only the lock winner runs;
  every loser exits without efficacy or a failure result while the owner lives.
  Reject overwrite, partial/malformed/conflicting markers, mismatched start/
  completion identities or artifact inventory, or entry to efficacy without the
  marker. Prove dead-owner recovery never re-enters efficacy.
- Inject crashes before start, after start, during artifact generation, after
  completion, and before result/preregistration. Prove only the pre-start case is
  quality-only retryable, an incomplete start is fail-closed/non-retryable, and
  a completed attempt can be finalized only from its already-bound immutable
  artifacts without recomputing losses.

### Exit gate

V4 has one causal offline contract and an honestly scoped replacement
comparator for the incumbent forecast rule, with no production behavior change.

## Phase 2 — Seven-cell selection and paired inference

### Work

1. Validate the exact seven-cell set; no subsets, extras, renamed cells, or
   favorable-cell selection.
2. Freeze one global epoch-aligned 500 ms lattice per cell:
   `generated_ms = cell.phase_offset_ms + 500*k` for integer `k`, restricted to
   `[scoring_start_ms, scoring_end_ms)`. Never rebase the lattice on a possibly
   nonaligned scoring start. The delay-only cells have phase offset zero. An
   exact 24-hour UTC window has 172,800 scheduled positions per cell and 172,793
   `target_eligible` positions under the strict maximum-horizon tail rule.
   Serialize the domains unambiguously:
   - `scheduled_origin_vector[cell_id]` contains the ordered lattice positions
     in the scoring window: length 172,800 for the holdout.
   - `target_eligible_mask[cell_id]` is a deterministic preregistered Boolean
     mask over that scheduled vector, and `target_eligible_origin_vector[cell_id]`
     is its ordered true subset: length 172,793 for the holdout. The seven
     structural tail exclusions are not missing observations.
   - Observed `generation_eligible_mask`, `common_scored_mask`, and
     `decision_eligible_mask` are each indexed over the 172,793-position target-
     eligible vector. Per-origin missing reasons apply only to false observed
     eligibility at one of those target-eligible positions.
   Never shift or compact an observed-ineligible position.
   - `canonical_efficacy_rows`: every `canonical_p0` 500 ms
     `decision_eligible` row after holdout quality passes. Mark every row
     `gate_eligible=true` and `descriptive_eligible=true`.
   - `robustness_efficacy_rows[cell_id]`: every 500 ms `decision_eligible` row
     for that rejection-only cell.
   - Mark the canonical rows at `holdout_start_ms + n*4000` with
     `nonoverlap_diagnostic_eligible=true`. This one-eighth subphase is a
     diagnostic only; it cannot enter a gate, bootstrap, or model choice.
   Bind each scheduled vector and deterministic target mask/vector before
   collection, and each observed eligibility mask/count only in the later
   loss-free quality evidence.
3. Use the complete canonical-p0 500 ms stream for both stages: all
   control-paired `decision_eligible` rows for calibration ranking and all
   `canonical_efficacy_rows` for every holdout promotion point estimate and
   bootstrap. Use each robustness cell's complete 500 ms control-paired
   `decision_eligible` stream for calibration robustness and its complete
   `robustness_efficacy_rows[cell_id]` for holdout rejection gates. Never select
   on the 500 ms stream and promote on a systematic subphase.
4. Add relative calibration robustness. Within each robustness cell, calculate
   all five candidates on that cell's own all-five-plus-control
   `decision_eligible` 500 ms cohort. The canonical winner may be no more than
   one percentage point below that cell's best candidate; do not use a cross-
   cell intersection.
5. Use a standard circular moving-block bootstrap:
   - `seed_bytes = hashlib.sha256(preregistration_bytes).digest()`;
   - `seed_int = int.from_bytes(seed_bytes, byteorder="big", signed=False)`;
   - `rng = random.Random(seed_int)`;
   - 1,800-origin (15-minute) blocks on the 172,793-position canonical
     `target_eligible` grid;
   - 96 calls to `rng.randrange(172_793)` per replicate, concatenate the
     circular blocks, then truncate the seven excess sampled positions back to
     172,793;
   - 10,000 paired replicates;
   - one-sided 95% lower bound = 500th one-indexed sorted statistic.
6. Bootstrap indexes the ordered 172,793-position canonical target-eligible
   vector and joins its observed decision mask. Truncate the concatenated block
   draw to 172,793 first, then skip only paired observed-missing rows inside the
   sampled positions. Never treat the seven structural tail exclusions as
   missing, or compact, shift, impute, or replace observed missing values with
   zero. Undefined replicates/zero baselines fail inference.
7. Resample challenger, its baseline, control, and its baseline synchronously;
   recompute both MAE skills and retain the challenger/control MAE-skill
   difference statistic for each replicate. The bootstrap lower bound applies
   only to that replacement improvement; it is not an additional absolute-skill
   promotion gate. Randomness selects indices only. Compute each per-row loss
   and every final MAE skill/difference or RMSE in an explicit local Decimal
   context with precision `50` and `ROUND_HALF_EVEN`, using one frozen operation
   order; do not algebraically cancel the common count.
8. Implement the bootstrap with exact circular-block sufficient statistics, not
   1.73 billion sampled-row visits.
   - Freeze the four finite, nonnegative per-row absolute-loss Decimals after
     their precision-50 calculation. Apply one paired decision mask to all four
     series; an excluded position contributes exact zero and count zero.
   - Let `e` be the minimum exponent across all nonzero frozen losses, or `0` if
     all are zero. After exact rescaling to `e`, let `Dmax` be the largest
     coefficient digit width. For `M=172793`, use accumulator precision
     `P=max(50, Dmax + len(str(2*M)))`, retain `ROUND_HALF_EVEN`, and trap
     `Inexact` and `Rounded`. Validate exponent bounds. This context must make
     every prefix, block, and replicate sum exact.
   - Precompute circular prefix/block queries for paired valid count and the four
     exact Decimal sums for lengths `1800` and `1793`. Each replicate adds the
     first 95 complete block summaries plus the 1,793-position prefix of the
     96th draw, preserving the exact RNG calls, draw order, wraparound, and final
     truncation above.
   - Convert the exact aggregate sums to MAEs, then compute challenger skill,
     control skill, and their difference in the frozen precision-50 operation
     order. The canonical naive reference uses the same exact no-rounding
     accumulator before those divisions, so optimized and row-by-row results
     must match exactly.
9. After a quality pass, derive one offline report exclusively from the same
   all-500-ms `canonical_efficacy_rows`: one exact UTC-day aggregate, exactly 24
   UTC-hour aggregates, one aggregate per five-minute market/session, and the
   full corrected 3x3 up/neutral/down confusion matrix. Include only challenger/
   control efficacy. Retain the forecast, matched baseline, causal actual, and
   frozen direction categories needed to reconstruct the matrix; derive metrics
   from counts and absolute/squared-loss sums, never average subgroup RMSE/
   skill, and never use the four-second or subgroup diagnostics for selection.

### Affected files

- `price_collector/shadow_signal_replay.py`
- `price_collector/shadow_signal_experiment.py`
- `tests/test_shadow_signal_replay.py`
- `tests/test_shadow_signal_experiment.py`
- `CHAINLINK_ACTUAL_VS_PROJECTED.md`

No separate policy, ledger, statistics, or shared registry module is needed.

### Tests

- Validate exactly seven cells and prove only `canonical_p0` can rank/infer.
- Prove calibration ranking and every holdout point estimate/bootstrap use the
  same complete 500 ms canonical lattice and control-paired eligibility rule;
  the four-second diagnostic subset cannot enter a gate.
- Prove every holdout robustness gate uses that cell's complete 500 ms
  phase-relative grid and cell-specific observed mask, and cannot select a
  model.
- Reject a winner that violates relative robustness in any cell; prove each
  calibration comparison uses that cell's own all-five-plus-control decision
  cohort.
- Prove all-five common membership and identical challenger/control pairing;
  sparse control pairing must fail even when v4 common coverage passes.
- Golden-test the 172,800-position scheduled grid, 172,793-position
  target-eligible grid, 1,800-position blocks, 96 draws and final truncation,
  missing handling, circular draws, paired four-series resampling, MAE-skill-
  difference recomputation, exact percentile/seed conversion, Decimal context
  isolation, and deterministic result under the frozen Python version.
- Prove the optimized block-summary bootstrap is exactly equal to the canonical
  naive row-by-row reference on a small adversarial fixture covering varied
  Decimal exponents, wraparound, paired missingness, repeated block starts, and
  the truncated 96th block. Trap any rounded/inexact sufficient-stat sum.
- With a non-500-ms-aligned calibration boundary, prove origins remain on the
  global epoch lattice. Golden-test the scheduled-vector, deterministic target-
  mask/vector, and target-indexed observed-mask lengths/hashes; structural tail
  positions must never receive missing-observation reasons.
- Fail on undefined metrics, wrong replicate count, tampered hashes, or reused
  output paths.
- Recompute daily/hourly/session diagnostics from counts/sums, retain the full
  confusion matrix exclusively from `canonical_efficacy_rows`, and prove no
  diagnostic can change selection.

### Exit gate

One 500 ms canonical replay cell answers the question; six cells test robustness;
standard reproducible inference replaces custom PRNG machinery.

## Phase 3 — Minimal raw evidence preservation

### Work

1. Before building the full archive flow, inspect operational metadata only:
   Binance/Chainlink session-duration distributions, current open ages,
   proactive-reconnect frequency, delay from disconnect to finalized counters,
   each source's oldest contiguous retained boundary and any internal partition
   gaps, maintenance/capture-suspension state, relation-budget status/headroom,
   and the existence of both current and next six-hour partition pairs. Compare
   the observed boundary and projected headroom through sealing—not only the
   configured 72-hour expiry—with the proposed archive and seal deadlines. If a
   retention-safe seal timeout is routinely impossible or partition maintenance
   is unhealthy, resolve that before calibration efficacy; this preflight
   exposes no model performance. Recheck the exact paired partitions,
   contiguous boundary, maintenance/capture state, and headroom at calibration/
   holdout start, every six-hour rollover, scoring end, and sealing. Any raw
   loss or disappearance of a required partition makes the range unusable.
   These are experiment evidence-readiness checks; they do not constitute or
   claim completion of the deferred high-resolution retention Phase 4.
   Define the oldest contiguous retained boundary from exact bounds of the
   actually attached, gap-free partition chain through the current interval;
   never infer it from a configured age or partition name alone. Normalize it
   to `oldest_contiguous_retained_wall_ns` before comparing it with session
   wall-clock fields.
2. Add an offline exporter for the existing futures trace, Chainlink event, and
   feed-session tables; do not change producers or `schema.sql`.
3. Add a read-only provenance recorder to the offline archive tooling. Before
   freezing calibration, verify each current invocation's startup binding and
   loaded/installed match; then continuously consume gap-checked journal cursors
   and relevant repository/environment/unit/decision-file transitions into the
   create-once hash chain. It must never restart, signal, or modify a producer.
   Recorder downtime or an unbound pre-freeze invocation fails closed.
4. Export provisional hourly UTF-8 JSONL with LF endings and Decimal strings for
   progress only. Because raw writes are asynchronously batched, an hour-ending
   export is never authoritative. At sealing, after every session overlapping
   the full archive input interval from warmup through finalization tail is
   finalized and its final row is visible, regenerate every in-range shard and
   every still-retained slice summary in one PostgreSQL `REPEATABLE READ, READ
   ONLY` transaction. Verify/hash any allowed presealed prefix artifact, then
   derive and reconcile each lifetime summary from the stable-snapshot slices
   plus that prefix. Hash only final canonical uncompressed content; the final
   manifest, not an hourly file's existence, marks the bytes authoritative.
5. Write one create-once final manifest with range/source boundaries, counts,
   first/last keys, shard hashes, complete finalized session rows, per-session
   integrity summaries, every partition/maintenance/capture/headroom checkpoint
   hash, the applicable provenance-freeze/checkpoint hashes and
   `provenance_continuity_root`, code/tree, and schema hash.
6. For each overlapping session, store prefix/range/suffix sufficient counts
   over its lifetime (raw/accepted counts, duplicates, regressions,
   out-of-session rows, and ready/disconnect bounds), plus each slice's first/
   last logical key, `received_wall_ns`, and `received_monotonic_ns`. Require the
   declared half-open prefix/range/suffix intervals to tile the session lifetime
   without an interval gap or overlap. Use boundary tuples only to detect
   duplicates, regressions, and ordering violations across slices; raw logical
   keys need not be consecutive. Reconcile the slice sums with final session
   counters without archiving prefix/suffix event rows. Before accepting a
   range, require for every potentially overlapping session that
   `session.ready_wall_ns` is at or after that source's actual
   `oldest_contiguous_retained_wall_ns`, or that a previously sealed trustworthy
   prefix summary already covers the missing lifetime prefix. Such a summary
   must bind source, `connection_id`, exact half-open interval, schema/code
   identity, and canonical content hash. Otherwise the session cannot be
   integrity-reconciled and the range is unusable.
7. Add archive input to the existing replay engine and prove database/archive
   equivalence.
8. Start the archive early enough for the maximum history/warmup required by
   both v4 and the offline replay replacement control, plus one poll. End after
   the maximum v4/control horizon plus the `200` ms finalization allowance.
   Warmup/finalization rows never become scored origins.
9. Because final session counters exist only after disconnect and raw rows are
   asynchronously flushed, choose and rehearse a finite `seal_timeout_ms`
   before calibration efficacy. Size it using the 3,700 ms horizon/finalization
   tail, configured flush interval, observed queue/backlog, and database-write/
   session-finalization delays, but never treat elapsed flush time as proof of
   completeness. Authority requires visible final session rows, exact
   reconciliation, and final shard regeneration from the stable snapshot. For
   each window derive
   `seal_deadline_ms = scoring_end_ms + seal_timeout_ms`; the deadline must
   remain retention-safe under the frozen projected budget/headroom through seal
   and the source-specific contiguous retained boundaries, with a documented
   safety margin and repeated actual-state checks. An open/unreconciled session,
   an actual boundary/gap/budget breach, or a missing final shard regeneration
   at the deadline yields `insufficient_evidence`.
10. If that deadline is operationally unusable, stop and request a separate
   choice between a controlled post-window reconnect and producer checkpoints;
   neither is authorized here.

### Affected files

- New: `price_collector/shadow_signal_raw_archive.py`
- `price_collector/shadow_signal_replay.py`
- `price_collector/shadow_signal_experiment.py`
- New: `tests/test_shadow_signal_raw_archive.py`
- `tests/test_shadow_signal_replay.py`
- `tests/test_shadow_signal_experiment.py`
- `README.md`
- `OPERATIONS.md`

### Tests

- Fixture-test session-duration/open-age/finalization summaries and the
  retention-safe timeout decision without reading efficacy data.
- Round-trip exact order, identities, timestamps, and Decimal strings.
- Prove an hour-ending export remains provisional when a delayed batch later
  adds an in-hour row, and prove final sealing regenerates all shards from one
  stable snapshot before hashing them.
- Reject missing/duplicate/reordered/truncated/tampered shards, bad boundaries,
  missing sufficient summaries, and manifest overwrite.
- Produce identical events, cohorts, ledgers, and metrics from database/archive
  inputs on one fixture.
- Cover pre-range/post-range/reconnected/open sessions; reconcile lifetime
  summary counts plus cross-slice first/last key and receipt-time boundaries.
  Fail on interval-tiling gaps/overlaps, duplicate/regressing/out-of-order
  boundary tuples, drops, integrity mismatches, deadline expiry, or a retention-
  unsafe deadline, but accept legitimate nonconsecutive raw logical keys.
- Reject a session whose ready time predates the actual retained boundary
  without a trusted sealed prefix summary. Reject unhealthy maintenance,
  capture suspension/relation-budget exhaustion, or missing current/next raw
  partition pairs before evidence collection; also detect any such failure at a
  six-hour rollover or sealing checkpoint.
- Prove the archive range covers the larger of v4/control warmup and horizon
  requirements.
- Interrupted export must not publish a complete manifest.
- Reject a manifest whose archive input predates its provenance freeze or whose
  `provenance_continuity_root` does not cover the complete input-through-sealing
  interval.
- Reject a pre-freeze invocation without a trustworthy loaded-state startup
  binding, any loaded/installed mismatch, journal-only provenance, cursor or
  monitor gaps, broken segment-root linkage, and edit/revert of any watched
  relevant identity. Prove the chain remains gap-free across quality-only
  attempts and binds every checkpoint root.
- Golden-test one-way entry/root derivation, non-circular segment genesis, the
  final-result preceding root, and the result-hash closure entry/sidecar; reject
  any self-referential or mutually linked root encoding.

### Exit gate

The test evidence survives the observed retention/budget state and replays
identically without a general archive platform or indefinite wait.

## Phase 4 — Calibrate, freeze, and rehearse offline

### Work

1. Create a separate pinned experiment environment from new
   `requirements-shadow-v4.txt`. Do not alter production `requirements.txt` or
   `/opt/price-collector/.venv`, and do not restart services.
2. Publish the pre-calibration provenance freeze and record the separate
   experiment and interval-wide producer/deployment provenance specified above.
   Continue the immutable provenance ledger established before the freeze across
   the calibration archive input interval and through sealing. Do not use
   retrospective raw rows or infer their production identity from the later
   freeze. A relevant producer,
   control, experiment-environment, or unrecorded deployment change invalidates
   compatible calibration.
3. Run the frozen deterministic candidate-day chooser. If a day passes,
   predeclare and prospectively collect that exact `86,400,000` ms midnight-
   aligned UTC calibration day, publishing its
   `calibration_attempt_freeze.json` by the frozen lead deadline. Run seven loss-
   free quality cells first. A quality failure emits a terminal attempt with both
   calibration efficacy markers false; at most one successor may be authorized
   under the frozen lineage/window rule. If no candidate day passes within the
   frozen seven-entry scan, instead emit
   `insufficient_evidence` at `failure_stage=calibration_window_selection`, with
   no range/freeze/manifest/quality or efficacy artifacts and the candidate-
   ledger/provenance roots bound.
   - On quality pass, publish `pre_efficacy_provenance_gate`, then durably publish
     `calibration_efficacy_started.json` before entering the all-five loss/
     ranking path, create immutable efficacy artifacts, and publish
     `calibration_efficacy_completed.json`. Drain the continuing monitor and
     publish `final_analysis_checkpoint` immediately before the terminal result
     or holdout-selection authorization. Never choose or extend a range after the
     start marker exists.
   - A start marker without a completion marker makes the attempt non-retryable
     and produces `insufficient_evidence` without efficacy fields. A completed
     marker makes the range/output immutable; for an eligible shorter winner,
     publish the mandatory final-analysis checkpoint and then the holdout-
     selection authorization as the next decision artifact under the same lock;
     then run its deterministic candidate-day/preregistration path.
   - If a completed marker's bound immutable artifacts later fail integrity,
     emit terminal calibration-stage `insufficient_evidence` with both
     calibration marker booleans true, their hashes, no efficacy fields, and the
     exact integrity reason; never recompute the calibration.
   - Any relevant provenance transition after the calibration start marker is
     likewise consumed calibration-stage `insufficient_evidence` with no
     efficacy fields, regardless of completion-marker state.
   - If the `250` ms reference gap makes the frozen coverage gates infeasible,
     do not expose efficacy or loosen it. Stop and create a new policy version.
4. Rank all five candidates only on canonical phase 0's control-paired
   `decision_eligible` cohort and apply:

   | Calibration gate | Threshold |
   | --- | ---: |
   | Frozen calibration duration | exactly 24 hours |
   | Scheduled / target-eligible origins per cell | exactly 172,800 / 172,793 |
   | Canonical common-scored coverage / count | at least 95% / 164,154 |
   | Each robustness-cell common-scored coverage / count | at least 90% / 155,514 |
   | Canonical decision-eligible coverage / count | at least 95% / 164,154 |
   | Each robustness-cell decision-eligible coverage / count | at least 90% / 155,514 |
   | Winner canonical MAE skill | at least 5% |
   | Winner canonical RMSE skill | greater than 0 |
   | MAE lead over canonical runner-up | at least 1 percentage point |
   | Relative robustness | winner no more than 1 point below each cell's best candidate |

   RMSE cannot rank or rescue an MAE tie. Always record the full-family
   `calibration_winner`, its ordered runner-up/lead evidence, and
   `winner_promotion_eligible`. Only `1500`, `2000`, or `2500` ms can become the
   frozen shorter challenger. If 1.5 seconds wins, record
   `boundary_winner=true`; do not widen the grid. A `3000` or `3500` ms winner,
   or failure of every shorter candidate to clear all gates, emits terminal
   `retain_incumbent` at `decision_stage=calibration` without selecting a
   shorter runner-up or spending the holdout. Calibration that terminates
   without usable quality/causal/archive/provenance evidence emits terminal
   `insufficient_evidence` instead.
5. A canonical create-once terminal calibration result sets
   `holdout_attempted=false` and
   `efficacy_attempt_consumed=false` and does not require a nonexistent holdout
   preregistration hash. It always binds the control/digests, attempt/window-
   selection and provenance evidence, exact
   terminal stage, decision, and the independent
   `calibration_efficacy_started/completed` state plus marker hashes when
   present; it binds a calibration range/freeze, archive, and quality artifacts
   only when they were actually created. It binds the latest stage-terminal or
   final-analysis provenance checkpoint immediately preceding publication. A
   calibration-stage
   `insufficient_evidence` body contains no losses, rankings, skills, or other
   efficacy values.
6. For a clear eligible shorter winner and its immediate selection
   authorization, run the frozen holdout candidate-day scan. Seven failures emit
   attempt-terminal `insufficient_evidence` at
   `failure_stage=holdout_window_selection`, with no preregistration/receipt or
   holdout efficacy markers; the one holdout successor may be allocated if
   unused. On the first passing day, create one canonical, non-overwritable
   preregistration binding: policy/settings; offline replacement control and
   the split forecast-configuration, forecast-code, and offline-evaluation-
   policy digests;
   calibration range/hashes; frozen challenger; exact future UTC day; seven
   cells; causal/cohort/baseline rules; bootstrap and all gates; archive range,
   seal timeout/deadline; the archive-health contract and checkpoint schedule;
   source-specific contiguous-boundary/partition requirements; the frozen
   headroom projection method, inputs, minimum threshold, and failure rules;
   experiment/interval-wide producer provenance; the deterministic window-
   selection rule and candidate-day ledger root; `minimum_preregistration_lead_ms`;
   `preregistration_publication_allowance_ms` and exact publication deadline;
   both calibration efficacy-marker hashes; and explicit no-rerank/no-fallback/
   one-efficacy-bearing-attempt flags.
   Also bind the stable lineage, continuing unique experiment ID, both stage-
   local attempt indexes, retry/successor state as of preregistration,
   continuous-checkpoint/provenance-ledger rule and current root, model-role
   identities, and Decimal context. A first-attempt preregistration must declare
   `all_previously_inspected_evidence_is_calibration_only=true`, with hashes or
   identifiers for every previously inspected selection/replay/old-holdout
   artifact. A permitted retry must instead bind the prior attempt and declare
   `prior_attempt_was_loss_free_quality_only=true` and
   `no_holdout_efficacy_generated_or_exposed=true`; it must not relabel prior
   holdout quality evidence as calibration-only.
   For each future holdout cell, preregister only the origin-generation formula,
   complete scheduled timestamp vector, deterministic target-eligible mask/
   vector, expected scheduled/target-eligible counts, target-indexed observed-
   mask schemas, missing-origin treatment, and coverage thresholds. The
   observed generation/common/decision eligibility masks/counts and per-origin
   missing reasons are future evidence; they must not appear in preregistration
   and are bound only by the loss-free holdout quality artifacts and final
   result.
7. Compute its SHA-256, commit the preregistration plus sidecar, and push to the
   authoritative remote. The JSON binds the experiment source tree; a create-
   once receipt binds the lineage/experiment IDs, both stage-local indexes,
   preregistration and sidecar hashes, pushed commit ID, observed remote ref, and
   local verification time, avoiding self-reference. Require that receipt before
   the holdout's full archive input interval by the frozen 24-hour lead. A missed
   lead is a loss-free failed attempt, not permission to move the window silently.
   In both success and failure, publish the canonical receipt/deadline check used
   by the result schema. No signed-tag/timestamp workflow is required.
8. Rehearse once on calibration-only data with `promotable=false` and document
   exact commands. The runbook must state that the critical path performs no
   production install or restart.

### Affected files

- `price_collector/shadow_signal_experiment.py`
- New: `requirements-shadow-v4.txt`
- `tests/test_shadow_signal_experiment.py`
- `README.md`
- `OPERATIONS.md`
- `CHAINLINK_ACTUAL_VS_PROJECTED.md`
- Generated: provenance freeze/checkpoints and continuity ledger, candidate-day
  ledger, calibration-attempt freeze, optional retry-eligibility/successor
  authorization,
  calibration efficacy start/completion markers, and one terminal result for
  every attempt that stops in calibration. An eligible shorter challenger's
  experiment ID instead continues into holdout and produces a holdout-selection
  authorization plus
  `experiments/chainlink-v4/<experiment-id>/preregistration.json`, its SHA-256
  sidecar, pushed-commit receipt when present, and receipt/deadline check.

### Tests

- Fail every calibration quality/MAE/RMSE-confirmation/lead/robustness gate;
  prove RMSE never ranks.
- Fail calibration decision coverage when the offline control is sparse even if
  all-five common coverage passes; every candidate must be ranked on the exact
  same control-paired rows.
- Reject calibration ranges with any duration other than exactly 24 hours,
  non-midnight UTC alignment, pre-freeze archive input, a non-earliest passing
  candidate day, a missing/mutated/late calibration-attempt freeze or under-one-
  hour attempt-freeze lead, or any extension/change after the efficacy-start
  marker.
- Reject changed control, digest, range, hash, cell, rule, gate, window,
  provenance, Python, or lock.
- Reject an omitted/false first-attempt or retry-specific prior-evidence
  declaration or a missing inspected artifact identity, including the old
  holdout and any prior loss-free holdout-quality attempt.
- Reject a missing/delayed holdout-selection authorization, an anchor other than
  calibration `completed_at`, or candidate/preregistration artifacts published
  out of the frozen initial/retry order.
- Reject late/overlapping calibration, non-UTC/wrong-duration holdout, changed
  publication allowance/deadline, late or transitively mismatched pushed
  receipt, reused output, or unpushed preregistration. Verify the missed-lead
  terminal state and its single-successor budget.
- Reject preregistration that claims any observed future eligibility mask/count;
  reject a changed global lattice, scheduled vector, deterministic target mask/
  vector, expected count, observed-mask index schema, missing treatment, or
  coverage threshold after preregistration.
- Reject an omitted or changed archive-health checkpoint schedule, contiguous-
  boundary/partition rule, headroom projection inputs/method/minimum, or failure
  rule after preregistration.
- Allow only the single deterministic calibration successor after quality-only
  failure. Reject a missing/tampered retry-eligibility record or candidate-day
  ledger, a second successor, a forked parent authorization, or any alternate
  range after the efficacy-start marker exists.
- Prove a `3000` or `3500` ms winner and every no-clear-shorter case emits
  calibration-stage `retain_incumbent`, never freezes a longer model, and never
  substitutes a runner-up. Map exhausted calibration quality/provenance to
  calibration-stage `insufficient_evidence`.
- Validate that both terminal calibration results set the holdout flags false,
  omit holdout-preregistration fields, and bind only stage-available artifacts;
  calibration insufficiency must expose no efficacy values. Validate every
  calibration start/completion-marker combination and its retry consequence,
  including a relevant transition between pre-efficacy and final-analysis
  checkpoints.
- Reject a 250 ms gap-policy change after any quality result and require a new
  policy version/replay when the frozen threshold is infeasible.
- Reject retrospective calibration, missing invocation-start provenance,
  loaded/installed mismatch, a continuity cursor/monitor/hash-chain gap, an
  unrecorded deploy/revert, or a changed producer/control/environment identity.
  Accept a recorded process restart only when
  code/config/artifact identities are unchanged and feed-session evidence
  accounts for it. A rehearsal artifact cannot be relabeled as promotable.
- Build a clean environment from `requirements-shadow-v4.txt` and run focused
  tests.

### Exit gate

The calibration stage ends with one operative state: a lineage-closing
calibration-stage `retain_incumbent`, a lineage-closing calibration-stage
`insufficient_evidence`, or one eligible shorter challenger whose continuing
experiment ID has an honestly scoped offline replacement control, finite archive
rule, and future day pushed before collection. Production remains unchanged.

## Phase 5 — One quality-first 24-hour holdout

### Work

1. Before the full holdout archive input interval begins, verify its deterministic
   candidate-day ledger, successor state when applicable, pushed-commit receipt,
   24-hour lead, readiness gates, and the gap-free provenance chain from the pre-
   calibration freeze through the archive-start checkpoint.
   Collect exactly the preregistered UTC day without extension. Show operational
   health/archive/provenance progress only—never forecast errors, losses,
   rankings, or skill.
2. Seal/verify the archive by the frozen deadline and complete the holdout-end,
   sealing active-runtime/provenance checkpoints. Reconcile the complete
   provenance-continuity ledger through the seal checkpoint; any missing
   interval, unrecorded transition, or relevant mismatch is a loss-free quality
   failure.
3. Run seven loss-free quality cells and require:

   | Quality gate | Threshold |
   | --- | ---: |
   | Duration / cells | exactly `86,400,000` ms / all seven |
   | Scheduled / target-eligible origins per cell | exactly 172,800 / 172,793 |
   | Canonical common-scored coverage / count | at least 95% / 164,154 |
   | Each robustness-cell common-scored coverage / count | at least 90% / 155,514 |
   | Canonical decision-eligible coverage / count | at least 95% / 164,154 |
   | Each robustness-cell decision-eligible coverage / count | at least 90% / 155,514 |
   | Cohort classification / causal violations | 100% / 0 |
   | Archive sealing, health-checkpoint artifacts, and exact provenance | pass |

   The integer minima are the authoritative exact-ratio checks; displayed
   percentages are not rounded substitutes.

   Quality failure emits only `insufficient_evidence` with no holdout efficacy-
   start/completion marker and `efficacy_attempt_consumed=false`; efficacy is
   unreachable. A replacement requires the bounded deterministic successor
   path, not an operator-selected day.
4. On quality pass, acquire the attempt-owner/provenance coordination lock, drain
   the monitor, publish `pre_efficacy_provenance_gate`, verify its root remains
   current, and durably publish `holdout_efficacy_started.json` immediately
   before entering any loss-bearing path. An intervening transition detected
   before the marker is loss-free; once the marker exists, the attempt is
   consumed even if that transition is only drained later. Then calculate
   canonical challenger/control losses for every all-500-ms
   `canonical_efficacy_rows` row, with the descriptive/gate/nonoverlap-diagnostic
   flags frozen above. Calculate
   robustness losses for every row in each cell's complete 500 ms
   `robustness_efficacy_rows[cell_id]`, compute the frozen bootstrap/gates, write
   all immutable efficacy artifacts, and finally publish
   `holdout_efficacy_completed.json` binding their hashes. Drain the continuously
   running provenance monitor and publish `final_analysis_checkpoint` immediately
   before the result; a relevant post-start transition changes the result to
   consumed `insufficient_evidence` with no metrics. Require:

   | Promotion gate | Threshold |
   | --- | ---: |
   | Challenger canonical 500 ms MAE skill | at least 5% |
   | Challenger MAE skill minus control MAE skill | at least 2 percentage points |
   | 95% bootstrap lower bound for MAE-skill difference versus control | greater than 0 |
   | Challenger canonical RMSE skill | greater than 0 |
   | RMSE-skill difference versus control | at least 0 |
   | Challenger MAE skill versus its baseline in every robustness cell | greater than 0 |
   | Challenger MAE skill minus control MAE skill in every robustness cell | at least -1 percentage point |
   | Rerank / runner-up fallback | prohibited |

   RMSE is confirmation only: no RMSE bootstrap or two-point RMSE requirement.
   The point `>=5%` challenger skill gate is the frozen absolute-baseline
   requirement. Do not add a second absolute-skill bootstrap gate; the sole
   inferential lower-bound gate is the preregistered paired improvement versus
   the replacement control.
5. Emit exactly one offline result:
   - seven-day holdout window-selection exhaustion before preregistration:
     `insufficient_evidence` at `failure_stage=holdout_window_selection`, with
     the selection/retry anchor and candidate-ledger/provenance roots bound, no
     preregistration/receipt or efficacy markers, and
     `efficacy_attempt_consumed=false`;
   - missed pushed-receipt/publication deadline before collection:
     `insufficient_evidence` at `failure_stage=preregistration_lead`, both holdout
     efficacy markers false, `efficacy_attempt_consumed=false`, and the one
     quality-only holdout successor still available if unused;
   - quality failure before the start marker: `insufficient_evidence` with
     `holdout_efficacy_started=false`, `holdout_efficacy_completed=false`, and
     `efficacy_attempt_consumed=false`;
   - a start marker without a completion marker, including any crash/failure
     during efficacy generation: `insufficient_evidence` with
     `holdout_efficacy_started=true`, `holdout_efficacy_completed=false`,
     `efficacy_attempt_consumed=true`, no efficacy values, and a terminal failure
     reason;
   - a completion marker whose bound immutable artifacts later fail integrity:
     `insufficient_evidence` with both markers true,
     `efficacy_attempt_consumed=true`, no efficacy values, and the exact terminal
     integrity reason;
   - any relevant provenance transition after the start marker:
     `insufficient_evidence` with `efficacy_attempt_consumed=true`, no efficacy
     values, the final-checkpoint reason, and the completion boolean matching
     whether its marker was already published;
   - completed efficacy plus any failed gate: `retain_incumbent` with both
     markers true and `efficacy_attempt_consumed=true`;
   - completed efficacy plus every passed gate: `promotion_eligible` with both
     markers true and `efficacy_attempt_consumed=true`.
6. Make result bindings stage-conditional. A pre-preregistration holdout-window-
   selection result binds the lineage/experiment IDs, both stage-local indexes,
   selection or retry anchor, candidate-day/provenance roots, retry state, and
   explicit absence of preregistration/receipt/efficacy artifacts. Every
   preregistered holdout-attempt result always binds the lineage/experiment IDs,
   both stage-local indexes and parent/successor authorization, deterministic
   candidate-day ledger root,
   preregistration hash, publication deadline, and a canonical create-once
   `receipt_deadline_check.json`; that check binds the expected/observed remote
   ref and commit, `checked_at`, and receipt-present/timely booleans. Bind the
   pushed-receipt hash/ref/time when it exists; for a missing receipt bind the
   explicit absence observation instead. Also bind the decision, both holdout
   efficacy booleans and marker hashes when present,
   `efficacy_attempt_consumed`, exact failure/completion stage, and the latest
   stage-available preceding `provenance_continuity_root`. An attempt reaching
   final analysis must bind `final_analysis_checkpoint`; an earlier result binds
   its stage-terminal checkpoint. It also binds the incumbent/challenger
   forecast-config and forecast-code digests, the offline-evaluation-policy
   digest, every
   completed active-runtime/archive-health checkpoint, and the canonical
   hash, `artifact_type`, and `schema_version` of every artifact actually
   created.
   - A successfully sealed archive additionally binds its final raw manifest.
   - A completed seven-cell loss-free quality stage additionally binds all seven
     quality hashes, each preregistered scheduled vector and deterministic
     target mask/vector, each target-indexed observed generation/common/decision
     mask/count and per-origin missing reason, and the quality-gate inputs/
     results.
   - A completed `retain_incumbent` or `promotion_eligible` result additionally
     binds the challenger/control efficacy ledger, offline day/hour/session/
     confusion report, the improvement bootstrap lower bound, bootstrap
     contract, derived seed, and all efficacy-gate inputs/results.
   - A terminal post-efficacy failure binds hashes of successfully created
     artifacts and the exact failure stage, but exposes no metrics.
   An `insufficient_evidence` result binds only the quality/integrity evidence
   available at its failure stage and contains no losses, skills, bootstrap
   values, or other efficacy fields. Its `efficacy_attempt_consumed` flag
   distinguishes retryable quality-only insufficiency from terminal post-
   efficacy failure.
7. Do not install outputs or alter production. Freeze this retry state machine:
   - `insufficient_evidence`: the one permitted replacement future day may be
     authorized only when both holdout marker booleans are false,
     `efficacy_attempt_consumed=false`, no holdout loss or efficacy value was
     generated or exposed, the holdout retry count is unused, and every frozen
     calibration/control/provenance prerequisite remains valid. After a create-
     once retry-eligibility record, select the earliest passing day with the
     frozen chooser and issue a single successor authorization binding the
     already allocated new experiment ID, then its preregistration, pushed
     receipt, publication allowance, and 24-hour lead. Bind the prior result;
     never fork it or choose a later market day. A
     changed primary, model/
     producer implementation, or invalidated calibration cannot use this path.
   - `retain_incumbent`: terminal for this calibration/challenger; no second
     holdout may reuse it.
   - `promotion_eligible`: terminal for this calibration/challenger.
   A start marker consumes the efficacy-bearing holdout even if the process
   crashes before publishing a ledger or result. Never recompute that attempt,
   never authorize more than the one quality-only successor, and never
   recalibrate after any holdout data is seen.

### Affected files

No new tracked implementation files. Generated evidence outside the production
decision directory: candidate-day/provenance-continuity ledgers, retry-
eligibility/successor authorization and a new preregistration/pushed receipt when
applicable, its receipt/deadline check, archive-health checkpoint records, raw
archive/manifest, seven quality reports, optional holdout efficacy start/
completion markers, optional
challenger/control efficacy ledger, optional offline efficacy report with the
day/hour/session/confusion diagnostics, and final result. Each has its own
`artifact_type` and `schema_version`.

### Tests and verification

- Every quality failure keeps both holdout efficacy markers absent, efficacy
  unreachable, `efficacy_attempt_consumed=false`, and loss output absent.
- Holdout efficacy can contain only challenger/control rows; the bootstrap
  resamples challenger, challenger baseline, control, and control baseline
  synchronously. Its single improvement lower bound plus all point/robustness
  gates are conjunctive and use the full frozen 500 ms samples.
- Allow exactly one deterministic replacement day only after a no-marker/no-
  efficacy `insufficient_evidence`; reject a non-earliest passing day, missing
  candidate ledger/retry-eligibility record, forked authorization, late pushed
  receipt, second replacement, retry after either efficacy marker,
  `retain_incumbent`, or `promotion_eligible`, and every rerank/fallback.
- Cap each initial/successor candidate scan at seven entries and validate the
  pre-preregistration exhaustion result, artifact absences, retry budget, and
  prohibition on an eighth day.
- Inject crashes before start, after start, during efficacy writing, after
  completion, and before the final result. Prove the persisted state/consumption
  mapping, no recomputation after start, and deterministic finalization from a
  valid completion marker. A post-start integrity failure emits a result with no
  efficacy values and `efficacy_attempt_consumed=true`.
- Validate exact bindings for insufficient, retain, and promotion results;
  reject changed window/control/code or config digest/checkpoint/provenance/
  archive/report/scheduled or target vector/deterministic or observed mask/
  missing reasons/seed/gate.
- Prove an early unsealable-archive result need not bind a nonexistent final
  manifest or seven quality reports, and a partial quality failure binds only
  the checkpoint/artifact hashes actually created without efficacy fields.
- Prove a missing-receipt lead failure binds the preregistration, deadline, and
  canonical remote-ref absence check without requiring a nonexistent receipt;
  success and late-receipt paths bind their actual receipt/check consistently.
- Reject a gap in the single provenance chain across preregistration, push,
  attempts, or retries; missing loaded-state/startup binding; loaded/installed
  mismatch; or any unrecorded deploy/revert. Accept only recorded identity-
  preserving restarts whose raw session gap is reconciled.
- Inject a relevant transition after the pre-efficacy gate but before the final-
  analysis checkpoint and require consumed `insufficient_evidence` with no
  metrics and the actual completion-marker state; endpoint equality cannot hide
  it.
- Prove the `>=5%` point skill and paired improvement point/lower-bound gates are
  exactly the frozen contract; no additional absolute-skill lower-bound gate may
  alter the decision.
- Reproduce the result twice from the same inputs with identical canonical
  hashes/statistics/decision.
- Rerun replay/experiment/archive and unchanged v2/v3 regressions, then the full
  suite.

### Exit gate

Each allocated holdout `experiment_id`, including one that stops before
preregistration, produces one create-once insufficient/retain/promotion result;
the lineage permits no more than its single deterministic holdout quality-only
successor (in addition to the separately bounded calibration successor).
Production is unchanged and no alternative holdout candidate is inspected or
chosen using market/efficacy information.

## Deferred from this test

- PostgreSQL conflict readback.
- FastAPI/reporting/frontend and public confusion-matrix expansion.
- Production dependency locking, shared-venv rollout, and service restarts.
- Refactoring v2/v3 into a common registry.
- Signed tags/timestamp workflows and custom PRNG/hash protocols.
- Full 15-cell cross-product.
- Deterministic compressed bytes, lifetime event archives, permanent archive
  services, and generalized recovery infrastructure.
- Five-candidate live evaluation, runtime v4 activation, actual promotion, and
  its later `AGENTS.md`/operations/deployment handoff.
- Measured Redis-write/worker-read timing.
- Full high-resolution Phase 4 acceptance remains deferred: future-partition
  creation, expired-partition removal, configured 72-hour retention, and
  sustained raw-relation budget remain unproven beyond the evidence-specific
  readiness/recheck gates in this plan.

Do not modify for this test: `price_collector/api.py`,
`shadow_signal_reporting.py`, `shadow_signal_collector.py`,
`shadow_signal_evaluation.py`, `db.py`, `schema.sql`, production
`requirements.txt`, deployment units/environment examples, `FRONTEND_API.md`,
or `AGENTS.md`.

## Ready to calibrate

Begin real calibration only after Phases 1–3, the isolated environment, full
regressions, and the non-promotable rehearsal pass. Permit replacement of a
quality-only calibration range or holdout day only through the one-successor
deterministic lineage rule and only while the applicable efficacy-start marker
is absent. If calibration produces one eligible shorter challenger, run the
bounded deterministic holdout-window selection; on its first passing day, push
the preregistration with the frozen lead and allow at most one efficacy-bearing
untouched future UTC-day attempt. Any exhausted/failed stage emits its applicable
terminal result and leaves the operational incumbent unchanged.
