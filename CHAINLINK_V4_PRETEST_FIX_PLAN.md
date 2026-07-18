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
replay configuration must be read from the droplet before calibration.

Repository baseline: `739 passed, 11 skipped`.

## Decision the test produces

Calibration first answers whether one non-3000 v4 lag is an eligible, robust
winner. If 3000 ms wins or no non-3000 candidate clears every calibration gate,
the process stops without spending the future-day holdout.

If a challenger is frozen, the single efficacy-bearing holdout answers whether
that exact lag beats both its own horizon-matched no-change baseline and the
exact active 3000 ms replacement control on the fixed decision origins, without
becoming timing-fragile in the six rejection cells. It emits one of:

- `insufficient_evidence`: data quality, coverage, archive, causality, or
  provenance was inadequate, so the test makes no performance claim;
- `retain_incumbent`: evidence was usable, but the challenger missed at least
  one frozen efficacy or robustness gate; this is terminal for that challenger;
- `promotion_eligible`: every frozen gate passed, so the challenger may advance
  to a separate production-promotion review; nothing is activated by this test.

It does not answer profitability, execution quality, settlement accuracy,
measured Redis latency, or live-production safety.

## Frozen design

### Family and timing cells

| Item | Frozen value |
| --- | --- |
| Policy | `chronological_holdout_v4_lag_grid_24h` |
| Candidates | `1500, 2000, 2500, 3000, 3500` ms |
| Model rule | `beta=1`; lag equals horizon |
| Reference gap / future skew | `250` ms / `0` ms |
| Poll / generation cadence | Epoch-aligned `100` ms / `500` ms |
| Ranking metric | MAE skill versus each horizon's matched no-change baseline |
| RMSE | Confirmation only; never ranks or breaks an MAE tie |
| Holdout | One exact future UTC day, `[start_ms,end_ms)` |
| Rerank / fallback / dynamic switching | Prohibited |

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

### Exact operational control

Use these terms:

- `v4_3000_candidate`: the 3-second model under the complete v4 configuration.
- `operational_incumbent`: the exact primary artifact/configuration installed
  immediately before preregistration.
- `replacement_control`: the comparator for a production replacement.
- `frozen_challenger`: the non-3000 v4 calibration winner.

Record `active_incumbent_selection_sha256`,
`active_incumbent_replay_config_sha256`,
`active_incumbent_primary_model_version`,
`active_incumbent_non_lag_config_digest`, `v4_non_lag_config_digest`,
`active_incumbent_model_code_digest`, and `v4_model_code_digest`. The code
digest covers anchor formation, Futures-reference selection, projection,
validity, and target resolution. Define `model_config_digest` as SHA-256 over
canonical JSON for the complete effective role-independent model configuration,
including lag, horizon, beta, gap/skew, staleness, cadence, validity, cohort, and
actual-resolution settings. Exclude role/version labels, candidate-family
membership, evidence/report metadata, and timing-cell delay/phase overrides.
Derive each `*_non_lag_config_digest` from the same canonical inputs with only
lag and horizon additionally omitted.

- Require the active primary to remain exactly `lag_ms=3000`,
  `horizon_ms=3000`, and `beta=1`. Otherwise stop and redesign this policy.
- Use `v4_3000_candidate` as the control only when both configuration and code
  digests match.
- Otherwise replay the exact active 3-second implementation/settings as a
  non-selectable control over the same raw data and seven timing cells.
- A mismatched result is a v4-family lag selection plus an operational
  replacement test; it is not a purely lag-only production change.
- If the active control cannot be reconstructed, the run cannot produce a
  promotion-eligible result.

Bind and recheck the installed selection/configuration plus relevant model and
producer hashes at preregistration, holdout start, holdout end, and final
analysis. Any unexpected change produces `insufficient_evidence`.

Serialize identity as `(model_role, model_version, model_config_digest)`, not
model name alone, so an active control and v4 candidate with the same version
string remain distinct.

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
recomputed at each generation. `source_timestamp_ms` drives none of these
decisions; retain it only for provenance and existing engine identity/
watermarks.

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
    AND challenger/control forecasts are valid
    AND both have causal actuals at their own targets
```

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
   exact-control logic, and strict preregistration/result validation.
2. Add an explicit v4 mode to replay without changing schema-v3 defaults or
   refactoring v2/v3 selection/artifact/runtime paths.
3. Implement the visibility, tie-order, horizon-specific actual, cohort, and
   full-cohort invalidation rules above.
4. Emit canonical create-once reports and ordered JSONL ledgers, with Decimal
   strings, sorted by `(cell_id, generated_ms, model_role, model_version,
   model_config_digest)` and bound to the SHA-256 of their uncompressed bytes.
   Every raw manifest, quality report, efficacy ledger, preregistration, and
   final result has explicit `artifact_type` and `schema_version` fields.
5. Keep evidence role-specific:
   - loss-free calibration quality, then all-five calibration losses only on
     quality pass;
   - holdout quality/validity for all five plus the exact control, but no losses;
   - after quality passes, holdout losses only for challenger and control.

### Affected files

- `price_collector/shadow_signal_replay.py`
- New: `price_collector/shadow_signal_experiment.py`
- `tests/test_shadow_signal_replay.py`
- New: `tests/test_shadow_signal_experiment.py`
- `CHAINLINK_ACTUAL_VS_PROJECTED.md`

### Tests

- Mutate every v4 candidate/config field and fail closed; v2/v3 fixtures and
  defaults remain unchanged.
- Cover matching/mismatching active-control digests and prohibit a control
  identified only by model name.
- Golden-test canonical full/non-lag configuration digests: changing lag alters
  only the full digest, while changing beta or another shared rule alters both.
- Cover receipt/visibility disagreement, exact ties, one-nanosecond-late events,
  same-poll anchor/reference behavior, and later-confirmation leakage. The v4
  3-second model must match a v3 fixture when all effective settings match.
- Accept references exactly at the target and 250 ms gap, reject 251 ms, and
  cover millisecond-floor staleness versus the full-nanosecond actual cutoff.
- Prove generation eligibility has no future facts and all five candidates are
  invalidated together on a pre-finalization integrity reset.
- Prove calibration/quality/holdout ledger schemas cannot expose disallowed
  losses; reject tamper and overwrite.

### Exit gate

V4 has one causal offline contract and the true operational comparator, with no
production behavior change.

## Phase 2 — Seven-cell selection and paired inference

### Work

1. Validate the exact seven-cell set; no subsets, extras, renamed cells, or
   favorable-cell selection.
2. Freeze the holdout sample types:
   - `canonical_descriptive_rows`: every `canonical_p0` 500 ms
     `decision_eligible` row;
   - `canonical_decision_rows`: only those rows at
      `holdout_start_ms + n*4000`, `n=0..21599`.
   - `robustness_decision_rows[cell_id]`: each of the other six cells'
     `decision_eligible` rows at
     `holdout_start_ms + cell.phase_offset_ms + n*4000`, `n=0..21599`.
   The delay-only cells have phase offset zero. Never shift a missing fixed
   origin; bind the scheduled/eligible masks and counts separately for every
   cell.
3. Use all canonical-p0 `common_scored` 500 ms rows for calibration ranking.
   Use `canonical_decision_rows` for every holdout promotion point estimate and
   bootstrap. All-row 500 ms, hourly, and session estimates are descriptive
   only. Use each cell's `robustness_decision_rows` for holdout robustness
   gates; robustness cells remain rejection-only.
4. Add relative calibration robustness. Within each robustness cell, calculate
   all five candidates on that cell's own all-five `common_scored` 500 ms
   cohort. The canonical winner may be no more than one percentage point below
   that cell's best candidate; do not use a cross-cell intersection.
5. Use a standard circular moving-block bootstrap:
   - `seed_bytes = hashlib.sha256(preregistration_bytes).digest()`;
   - `seed_int = int.from_bytes(seed_bytes, byteorder="big", signed=False)`;
   - `rng = random.Random(seed_int)`;
   - 225-origin (15-minute) blocks;
   - 96 calls to `rng.randrange(21_600)` per replicate;
   - 10,000 paired replicates;
   - one-sided 95% lower bound = 500th one-indexed sorted statistic.
6. Preserve missing positions in the full grid; skip only paired-missing rows
   inside sampled blocks. Never compact, shift, impute, or replace with zero.
   Undefined replicates/zero baselines fail inference.
7. Resample challenger, its baseline, control, and its baseline synchronously;
   recompute the challenger's MAE skill and challenger/control MAE-skill
   difference each replicate. Randomness selects indices only. Use explicit
   local Decimal contexts with precision `50` and `ROUND_HALF_EVEN` for every
   loss, skill, RMSE, and bootstrap calculation.
8. After a quality pass, derive one offline report exclusively from
   `canonical_descriptive_rows`: one exact UTC-day aggregate, exactly 24
   UTC-hour aggregates, one aggregate per five-minute market/session, and the
   full corrected 3x3 up/neutral/down confusion matrix. Include only challenger/
   control efficacy, derive metrics from counts and absolute/squared-loss sums,
   never average subgroup RMSE/skill, and never use diagnostics for selection.

### Affected files

- `price_collector/shadow_signal_replay.py`
- `price_collector/shadow_signal_experiment.py`
- `tests/test_shadow_signal_replay.py`
- `tests/test_shadow_signal_experiment.py`
- `CHAINLINK_ACTUAL_VS_PROJECTED.md`

No separate policy, ledger, statistics, or shared registry module is needed.

### Tests

- Validate exactly seven cells and prove only `canonical_p0` can rank/infer.
- Prove every canonical promotion point estimate and both bootstrap bounds use
  exactly the fixed `canonical_decision_rows`; 500 ms descriptive rows cannot
  enter a gate.
- Prove every holdout robustness gate uses that cell's fixed four-second
  phase-relative schedule and cell-specific mask, and cannot select a model.
- Reject a winner that violates relative robustness in any cell; prove each
  calibration comparison uses that cell's own all-five common cohort.
- Prove all-five common membership and identical challenger/control pairing;
  sparse control pairing must fail even when v4 common coverage passes.
- Golden-test the fixed grid, missing handling, circular draws, paired
  absolute/difference recomputation, exact percentile/seed conversion, Decimal
  context isolation, and deterministic result under the frozen Python version.
- Fail on undefined metrics, wrong replicate count, tampered hashes, or reused
  output paths.
- Recompute daily/hourly/session diagnostics from counts/sums, retain the full
  confusion matrix exclusively from `canonical_descriptive_rows`, and prove no
  diagnostic can change selection.

### Exit gate

One 500 ms operational cell answers the question; six cells test robustness;
standard reproducible inference replaces custom PRNG machinery.

## Phase 3 — Minimal raw evidence preservation

### Work

1. Before building the full archive flow, inspect operational metadata only:
   Binance/Chainlink session-duration distributions, current open ages,
   proactive-reconnect frequency, and delay from disconnect to finalized
   counters. Compare them with the configured 72-hour raw expiry. If a
   retention-safe seal timeout is routinely impossible, resolve that before
   calibration efficacy; this preflight exposes no model performance.
2. Add an offline exporter for the existing futures trace, Chainlink event, and
   feed-session tables; do not change producers or `schema.sql`.
3. Export hourly UTF-8 JSONL with LF endings and Decimal strings. Hash canonical
   uncompressed content only.
4. Write one create-once manifest with range/source boundaries, counts,
   first/last keys, shard hashes, complete finalized session rows, per-session
   integrity summaries, code/tree, and schema hash.
5. For each overlapping session, store prefix/range/suffix sufficient counts
   over its lifetime (raw/accepted counts, duplicates, regressions,
   out-of-session rows, and ready/disconnect bounds). Reconcile their sums with
   final session counters without archiving prefix/suffix event rows.
6. Add archive input to the existing replay engine and prove database/archive
   equivalence.
7. Start the archive early enough for the maximum history/warmup required by
   both v4 and the exact active control, plus one poll. End after the maximum
   v4/control horizon plus the `200` ms finalization allowance.
   Warmup/finalization rows never become scored origins.
8. Because final session counters exist only after disconnect, choose and
   rehearse a finite `seal_timeout_ms` before calibration efficacy. It must
   exceed the 3,700 ms horizon/finalization tail. For each window derive
   `seal_deadline_ms = scoring_end_ms + seal_timeout_ms`; the deadline must
   precede expiry of required raw rows by a documented safety margin. An
   open/unreconciled session at the deadline yields `insufficient_evidence`.
9. If that deadline is operationally unusable, stop and request a separate
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
- Reject missing/duplicate/reordered/truncated/tampered shards, bad boundaries,
  missing sufficient summaries, and manifest overwrite.
- Produce identical events, cohorts, ledgers, and metrics from database/archive
  inputs on one fixture.
- Cover pre-range/post-range/reconnected/open sessions; reconcile lifetime
  summaries and fail on drops, integrity mismatches, deadline expiry, or a
  retention-unsafe deadline.
- Prove the archive range covers the larger of v4/control warmup and horizon
  requirements.
- Interrupted export must not publish a complete manifest.

### Exit gate

The test evidence survives retention and replays identically without a general
archive platform or indefinite wait.

## Phase 4 — Calibrate, freeze, and rehearse offline

### Work

1. Create a separate pinned experiment environment from new
   `requirements-shadow-v4.txt`. Do not alter production `requirements.txt` or
   `/opt/price-collector/.venv`, and do not restart services.
2. Record separate provenance for:
   - experiment code/tree, Python, installed distributions, and lock hash;
   - producer code/tree, Python, installed distributions, relevant unit hashes,
     nonsecret settings, and active decision-file hashes.
   Any producer/environment change invalidates compatible calibration.
3. Predeclare one contiguous calibration range. Run seven quality cells first;
   only on pass calculate all-five calibration efficacy.
   - Before any calibration efficacy exists, a quality-failing range may be
     replaced by another range declared before its own quality audit begins.
   - Once any calibration efficacy is generated, that range and result are
     terminal for the experiment; no alternative calibration range may be
     tried.
   - If the `250` ms reference gap makes the frozen coverage gates infeasible,
     do not expose efficacy or loosen it. Stop and create a new policy version.
4. Rank only canonical phase 0 and apply:

   | Calibration gate | Threshold |
   | --- | ---: |
   | Canonical common-scored coverage | at least 95% |
   | Each robustness-cell common-scored coverage | at least 90% |
   | Winner canonical MAE skill | at least 5% |
   | Winner canonical RMSE skill | greater than 0 |
   | MAE lead over canonical runner-up | at least 1 percentage point |
   | Relative robustness | winner no more than 1 point below each cell's best candidate |

   RMSE cannot rank or rescue an MAE tie. If 1.5 seconds wins, record
   `boundary_winner=true`; do not widen the grid. If 3 seconds wins or no clear
   challenger exists, stop without a holdout.
5. For a clear non-3000 winner, create one canonical, non-overwritable
   preregistration binding: policy/settings; exact control and configuration/
   model-code digests;
   calibration range/hashes; frozen challenger; exact future UTC day; seven
   cells; causal/cohort/baseline rules; bootstrap and all gates; archive range,
   seal timeout/deadline; experiment/producer provenance; and explicit
   no-rerank/no-fallback/one-efficacy-bearing-attempt flags.
   Also bind the stable calibration identity/retry state, required
   four-checkpoint verification rule, fixed-origin rule/mask/count, model-role
   identities, Decimal context, and
   `all_previously_inspected_evidence_is_calibration_only=true`, with hashes or
   identifiers for every previously inspected selection/replay/old-holdout
   artifact.
6. Compute its SHA-256, commit the preregistration plus sidecar, and push to the
   authoritative remote before the holdout. The JSON binds the experiment
   source tree; a small receipt/later result records its pushed commit ID,
   avoiding self-reference. No signed-tag/timestamp workflow is required.
7. Rehearse once on calibration-only data with `promotable=false` and document
   exact commands. The runbook must state that the critical path performs no
   production install or restart.

### Affected files

- `price_collector/shadow_signal_experiment.py`
- New: `requirements-shadow-v4.txt`
- `tests/test_shadow_signal_experiment.py`
- `README.md`
- `OPERATIONS.md`
- `CHAINLINK_ACTUAL_VS_PROJECTED.md`
- Generated: `experiments/chainlink-v4/<experiment-id>/preregistration.json`,
  SHA-256 sidecar, and pushed-commit receipt

### Tests

- Fail every calibration quality/MAE/RMSE-confirmation/lead/robustness gate;
  prove RMSE never ranks.
- Reject changed control, digest, range, hash, cell, rule, gate, window,
  provenance, Python, or lock.
- Reject an omitted/false prior-evidence declaration or a missing inspected
  artifact identity, including the old holdout.
- Reject late/overlapping calibration, non-UTC/wrong-duration holdout, reused
  output, or unpushed preregistration.
- Allow calibration-range replacement only after quality-only failure; reject
  any alternate range after calibration efficacy exists.
- Freeze a 3,500 ms winner like every other non-3000 winner; never substitute a
  shorter candidate.
- Reject a 250 ms gap-policy change after any quality result and require a new
  policy version/replay when the frozen threshold is infeasible.
- A producer/environment change invalidates calibration; a rehearsal artifact
  cannot be relabeled as promotable.
- Build a clean environment from `requirements-shadow-v4.txt` and run focused
  tests.

### Exit gate

One challenger, exact control, finite archive rule, and future day are pushed
before collection while production remains unchanged.

## Phase 5 — One quality-first 24-hour holdout

### Work

1. Collect exactly the preregistered UTC day without extension. Show operational
   health/archive progress only—never forecast errors, losses, rankings, or
   skill.
2. Seal/verify the archive by the frozen deadline and confirm provenance.
   Complete the holdout-end and final-analysis operational-control checkpoints
   before any loss-bearing command; any mismatch is a loss-free quality
   failure.
3. Run seven loss-free quality cells and require:

   | Quality gate | Threshold |
   | --- | ---: |
   | Duration / cells | exactly `86,400,000` ms / all seven |
   | Canonical common-scored coverage | at least 95% |
   | Each robustness-cell common-scored coverage | at least 90% |
   | Canonical decision-eligible coverage | at least 95% |
   | Each robustness-cell decision-eligible coverage | at least 90% |
   | Fixed canonical decision origins | at least 20,000 of 21,600 |
   | Cohort classification / causal violations | 100% / 0 |
   | Archive sealing and exact provenance | pass |

   Quality failure emits only `insufficient_evidence`; efficacy is unreachable.
4. On quality pass, calculate canonical challenger/control losses only on the
   identical `canonical_decision_rows`, and robustness losses only on each
   cell's `robustness_decision_rows`. Require:

   | Promotion gate | Threshold |
   | --- | ---: |
   | Challenger fixed-origin canonical MAE skill | at least 5% |
   | 95% bootstrap lower bound for challenger MAE skill | greater than 0 |
   | Challenger MAE skill minus control MAE skill | at least 2 percentage points |
   | 95% bootstrap lower bound for MAE-skill difference versus control | greater than 0 |
   | Challenger canonical RMSE skill | greater than 0 |
   | RMSE-skill difference versus control | at least 0 |
   | Challenger MAE skill versus its baseline in every robustness cell | greater than 0 |
   | Challenger MAE skill minus control MAE skill in every robustness cell | at least -1 percentage point |
   | Rerank / runner-up fallback | prohibited |

   RMSE is confirmation only: no RMSE bootstrap or two-point RMSE requirement.
5. Emit exactly one offline result:
   - quality failure: `insufficient_evidence` with
     `efficacy_attempt_consumed=false`;
   - integrity/reporting failure after any loss or efficacy generation:
     `insufficient_evidence` with `efficacy_attempt_consumed=true`, no efficacy
     values, and a terminal failure reason;
   - quality pass plus efficacy failure: `retain_incumbent` with
     `efficacy_attempt_consumed=true`;
   - full pass: `promotion_eligible` with
     `efficacy_attempt_consumed=true`.
6. Every result binds preregistration hash/pushed commit, raw manifest, seven
   quality hashes, all four operational-control checkpoint records, exact
   provenance, fixed-origin masks/counts, gate inputs/results, and decision. A
   completed `retain_incumbent` or `promotion_eligible` result additionally
   binds the challenger/control efficacy ledger, offline day/hour/session/
   confusion report, both bootstrap lower bounds, bootstrap contract, and
   derived seed. Bind every evidence artifact by canonical hash,
   `artifact_type`, and `schema_version`. A terminal post-efficacy failure binds
   the successfully created artifact hashes and exact failure stage, but no
   metrics. Any insufficient result contains no losses, skills, bootstrap
   values, or other efficacy fields. Its `efficacy_attempt_consumed` flag
   distinguishes retryable quality-only insufficiency from terminal
   post-efficacy failure.
7. Do not install outputs or alter production. Freeze this retry state machine:
   - `insufficient_evidence`: a replacement future day is allowed only when
     `efficacy_attempt_consumed=false`, no holdout loss or efficacy value was
     generated or exposed, and every frozen calibration, control, and
     provenance prerequisite remains valid. Reuse the exact frozen calibration/
     challenger, but issue a new experiment ID, preregistration, and future UTC
     window that bind the prior attempt. A changed primary, model/producer
     implementation, or invalidated calibration cannot use this retry path.
   - `retain_incumbent`: terminal for this calibration/challenger; no second
     holdout may reuse it.
   - `promotion_eligible`: terminal for this calibration/challenger.
   Any efficacy-bearing attempt consumes the single holdout attempt even if a
   later reporting step fails. Never recalibrate after holdout data is seen.

### Affected files

No new tracked implementation files. Generated evidence outside the production
decision directory: raw archive/manifest, seven quality reports, optional
challenger/control efficacy ledger, optional offline efficacy report with the
day/hour/session/confusion diagnostics, and final result. Each has its own
`artifact_type` and `schema_version`.

### Tests and verification

- Every quality failure keeps efficacy unreachable and loss output absent.
- Holdout efficacy can contain only challenger/control rows; both bootstrap
  lower bounds and all point/robustness gates are conjunctive.
- Allow a replacement day only after a no-efficacy `insufficient_evidence`;
  reject retries after any efficacy generation, `retain_incumbent`, or
  `promotion_eligible` and reject rerank/fallback.
- Force a simulated post-efficacy reporting/integrity failure to emit a
  loss-free terminal result with `efficacy_attempt_consumed=true`.
- Validate exact bindings for insufficient, retain, and promotion results;
  reject changed window/control/code or config digest/checkpoint/provenance/
  archive/report/fixed-origin mask/seed/gate.
- Reproduce the result twice from the same inputs with identical canonical
  hashes/statistics/decision.
- Rerun replay/experiment/archive and unchanged v2/v3 regressions, then the full
  suite.

### Exit gate

One preregistered insufficient/retain/promotion result is produced without
changing production or inspecting alternative holdout candidates.

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
- High-resolution Phase 4 risks: future partitions, expired partition removal,
  72-hour retention, and sustained raw-relation budget.

Do not modify for this test: `price_collector/api.py`,
`shadow_signal_reporting.py`, `shadow_signal_collector.py`,
`shadow_signal_evaluation.py`, `db.py`, `schema.sql`, production
`requirements.txt`, deployment units/environment examples, `FRONTEND_API.md`,
or `AGENTS.md`.

## Ready to calibrate

Begin real calibration only after Phases 1–3, the isolated environment, full
regressions, and the non-promotable rehearsal pass. Permit replacement of a
quality-only calibration range or holdout day only before its efficacy exists.
Then freeze one eligible challenger, push one preregistration, and allow at
most one efficacy-bearing untouched future UTC-day attempt. Otherwise abstain
and leave the operational incumbent unchanged.
