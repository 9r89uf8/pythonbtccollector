# Stage 3 — Settlement-evidence gate + standard-spot proxy validation

**Depends on:** Stage 2.
**Plan sections:** "Settlement-evidence validation gate" (all seven steps),
"Standard-spot proxy validation", "Follow-up feedback audit" (context — those
diagnostics were exploratory; this stage does the frozen version), "Cohort
construction" items 4-5 (Price-to-Beat availability).
**Acceptance criteria touched:** #14, #16 (freeze half), #17, #18
(validation half).

## Goal

Run the plan's evidence gate on the frozen extract and freeze every
settlement-evidence and spot-proxy convention. The plan is explicit: no
flip, lock, or required-shock feature may be computed before this gate
completes. Stage 4 is blocked until this stage records that Settlement Core
may proceed; merely recording a verdict is not sufficient.

## Tasks

1. Before reading outcome evidence, freeze the component tests, tolerances,
   hard-block conditions, and verdict aggregation in a machine-readable gate
   configuration. Then implement gate steps 1-7 as code producing a single
   report with an explicit verdict per component. Honor the details the plan
   fixes:
   boundary events stored in the next half-open market joined back for
   validation; exact-boundary vs positive-offset first-after kept separate;
   `boundary evidence absent` as the default for a missing boundary event.
2. Record the frozen source-time convention (the plan already selected the
   exact provider-time boundary event; this stage verifies and records — it
   does not retune) in `frozen/DECISIONS.md`.
3. Freeze the duplicate-event collapse (gate step 6) on **training data
   only**, using Stage 2's audit and sensitivity harness; record the
   deterministic last-accepted rule and the reported first/last/range
   sensitivity.
4. Price-to-Beat availability audit (gate step 4 + cohort items 4-5):
   confirm pre-checkpoint availability where the schema can prove it. If it
   cannot, do exactly what the plan says — complete the retrospective study
   but mark any prospective lock rule **provisional**, and record that
   status where Stage 6 and Stage 10 will see it. This historical verdict is
   immutable. A later, separately authorized production checkpoint may create
   a hashed, owner-approved
   `frozen/readiness/price-to-beat-<checkpoint-id>.json` from newly collected
   immutable evidence; that prospective readiness record never rewrites the
   Stage 3 result.
5. Standard-spot proxy validation items 1-5: freeze integration endpoints,
   weighting, carry/hold policy, cadence and gap limits, and maximum age on
   training data only; run terminal and rolling comparisons; report
   residuals by the plan's strata; implement the conservative
   observable-time approximation separately from provider-time results.
6. Preserve every tested convention in a hashed Stage 3 validation-manifest
   supplement that references both the immutable Stage 1 extraction-manifest
   hash and Stage 2 availability-bundle hash; never edit either predecessor.
   Append all freezes to `DECISIONS.md`.
7. Emit three explicit machine-readable statuses for later stages:
   `settlement_core = proceed|block`, `spot_proxy = proceed|abstain`, and
   `price_to_beat = proven|provisional`. A proxy failure does not block exact
   Settlement Core. A settlement-identity contradiction does.

## Hard gates

- No flip/lock/required-shock features anywhere in the repo until the gate
  verdict is recorded, and no Settlement Core feature construction proceeds
  unless that status is `proceed`.
- Conventions freeze on training evidence only. The already-frozen evidence
  verifier may then run unchanged across calibration and historical-audit
  periods as the plan requires, but it may not aggregate official-loss rates
  or tune a convention there. This evidence access does not make Stage 9 a
  prospective holdout.
- Failure outside training is not permission to tune another convention (gate
  step 7). A Settlement Core block stops Stage 4 and routes Stage 5 to its
  deadline failure report. Proxy `abstain` allows core work to continue without
  proxy results. A provisional Price-to-Beat status allows retrospective work
  but cannot support Stage 10 certification by itself. Only the named,
  pre-window prospective readiness record from newly collected immutable
  evidence can remove that operational blocker without changing this verdict.
- No duplicate policy invented for spot storage (proxy item 1 forbids it).

## Needs the owner

- Approval of the frozen gate criteria before evidence is read.
- Any Settlement Core block or provisional Price-to-Beat verdict — both change
  what later stages may claim, so surface them immediately.

## Done when

- [ ] Gate report in `artifacts/stage-3/` with per-component verdicts and the
      three downstream statuses recorded before Stage 4 starts.
- [ ] Proxy validation report with frozen conventions and residual bands.
- [ ] `DECISIONS.md` entries: boundary alignment, duplicate collapse,
      proxy integration rules, Price-to-Beat availability status.
- [ ] Machine-readable gate configuration and validation-manifest supplement
      hashed and linked to the immutable extraction manifest and Stage 2
      availability bundle.
