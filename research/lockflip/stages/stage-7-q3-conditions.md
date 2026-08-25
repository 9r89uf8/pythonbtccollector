# Stage 7 — Q3 condition models (train/calibration only)

**Depends on:** Stage 4 complete and Stage 5 Release 1A published. Development
may then run in parallel with Stages 6 and 8.
Derived feature #4 needs Stage 6's fold-fitted risk surface; features #5-#6 may
need Stage 6 risk outputs and Stage 8's training-only response kernel or shock
contours. Before the Stage 7 freeze, either consume the required frozen
out-of-fold artifacts or preregister those optional features as omitted. Never
freeze first and add them later.
**Plan sections:** "Q3 / Workstream C: what conditions increase end-flip
probability?" (in full), "Canonical last-20-second panel" → "New derived
features to test", "Statistical validation".
**Acceptance criteria touched:** #12 (construction half), #3.

## Goal

Build and freeze the condition-analysis machinery on train/cal data:
univariate held-out risk differences within margin strata, the deliberately
small headline model, and the nested data-family ablation ladder. Audit
evaluation is Stage 9.

## Tasks

1. Predeclare `T-20` primary, `T-10` secondary; one contribution per market
   per checkpoint.
2. Univariate held-out (train/cal folds) risk differences and ratios within
   exact-TWAP-margin strata, for every hypothesis in the plan's list —
   implemented as a table-driven harness so each hypothesis is tested, not
   assumed.
3. Headline model: penalized logistic, at most eight effective degrees of
   freedom, exact TWAP margin plus the predeclared core state; feature
   choice and regularization frozen on train/cal only.
4. Nested secondary models in the plan's exact seven-layer order; compare by
   held-out Brier, log loss, calibration error, and lock coverage.
5. Boosted-tree challenger for interactions — never the headline rule.
6. Temporal-fold stability machinery for every claimed condition (#12);
   conditions with fewer than 10 outcome events in either group are marked
   descriptive/anecdotal and excluded from headline ranking (plan,
   "Statistical validation").
7. Resolve the Stage 6/8 derived-feature barrier: verify the hashes of every
   consumed out-of-fold artifact, or record the optional features omitted.
8. Freeze the headline model, layer definitions, hypothesis list, and resolved
   derived-feature inventory as a hashed bundle under `frozen/q3/`; append to
   `DECISIONS.md`.

## Hard gates

- Split-specific loaders may read train/calibration-period rows only across the
  checkpoint panel, labels, event-derived inputs, and response targets.
  Audit/tail rows of every kind are barred and test-enforced.
- Any estimated-surface feature enters only through the Stage 4 fold-aware
  interface — never fit on the full dataset.
- Feed gaps/staleness may predict abstention-worthy uncertainty; report
  them separately rather than blending them into economic flip risk (plan's
  hypothesis list, last item).
- Effects language stays predictive, never causal.

## Needs the owner

- Nothing, in the normal path.

## Done when

- [ ] Frozen hashed bundle in `frozen/q3/` + `DECISIONS.md` entry.
- [ ] Train/cal report in `artifacts/stage-7/`: univariate table, layer
      comparison, challenger notes.
- [ ] Stage 6/8 derived-feature inputs resolved by verified hash or explicit
      preregistered omission before freeze.
- [ ] Fold-honesty test green (no audit/tail-period data access).
