# Stage 4 — Cohorts, analysis datasets, period-partitioned labels

**Depends on:** Stage 3 with `settlement_core = proceed`.
**Plan sections:** "Definitions fixed before analysis" (all subsections),
"Cohort construction", "Causal time alignment", "Canonical last-20-second
panel" (all feature groups), "Statistical validation" (unit-of-analysis and
label rules).
**Acceptance criteria touched:** #1, #2, #6 (feature half), #7
(definitions), #11, #15 (cohort half), #16, #17, #18 (feature half).

## Goal

Build the study's two central analysis datasets: (1) the six data-quality
cohorts and one-row-per-market-per-checkpoint `T-20`..`T-1` panel for Q1-Q3,
and (2) a causally aligned longitudinal event tape for Q4/Q5. Also build the
four flip definitions and period-partitioned labels. Downstream code reads
these artifacts through split-specific loaders, not directly from extracts.

## Tasks

1. Before any outcome label is inspected, verify the Stage 2 frozen
   CLOB-convention hash and load it unchanged. Label construction cannot begin
   until this verification passes.
2. Cohort builders for Settlement Core, Context, Spot-reconstruction proxy,
   Probability, Full, and Microstructure, honoring the plan's branching
   (proxy eligibility per checkpoint; ineligible rows stay in the
   operational denominator as abstentions; `Full` does not require proxy
   eligibility). Emit a cohort count report reconciled to the Stage 1
   manifest. If Stage 3 set `spot_proxy = abstain`, retain the operational
   denominator and omit proxy analysis rather than blocking Settlement Core.
3. Panel builder: decision instants per "Time bins" (state strictly before
   the instant, via Stage 2's `asof.py` only), one row per resolved market
   per checkpoint. Feature modules, one per plan group: core; proxy
   (`B_hat`, `F_hat_required`, `R_hat`, flat-future projection, quality
   fields — exactly the "Fixed-terminal-window spot proxy" arithmetic under
   Stage 3's frozen conventions); probability (normalized midpoint formula,
   strict two-sided CLOB rules, `indeterminate` states); flow/book/
   microstructure.
4. Longitudinal event-tape builder for Q4/Q5: causally aligned one-second
   spot/futures/microstructure paths plus exact TWAP events, session/gap state,
   source availability, quiet pre-period support, and enough follow-up for the
   plan's 0-90-second response window even when it crosses a market boundary.
   Partition market-linked rows by the frozen market period and raw
   free-standing rows by causal timestamp, with fixed boundary buffers tagged
   as context-only. Do not detect or assign shock origins here; Stage 8 owns
   causal onset, origin split assignment, and the 150-second purge. Do not
   compute future-selected responses here.
5. Derived features #1-#6 ("New derived features to test"): build as
   **fold-aware plumbing only** — an interface that takes a fold context so
   estimated surfaces/kernels are fit training-only or out-of-fold later
   (Stages 6-8). No full-dataset fitting here.
6. Flip labelers, implemented independently and never merged: (1)
   current-leader/final-outcome disagreement; (2) observed oracle side
   change with interval censoring; (3) CLOB favorite flip; (4) persistent
   CLOB flip (adjacent seconds, no missing interval). Plus the ex-post
   point-of-no-return descriptor with its left-censoring rules.
7. Write labels and both analysis artifacts for the assigned
   train/calibration/audit periods. Feature files contain no label-derived
   columns; anything from official final price or winner lives only under
   `labels/`. Reserved-tail raw rows remain mechanically partitioned in the
   extract, but this stage builds no tail labels, panel rows, event origins, or
   response targets. All later access goes through period-enforcing loaders.
8. Run every feature module through the Stage 2 mutation harness; add unit
   tests for tie/split exclusion, boundary events never joining back as
   features (#17), and Decimal survival end to end.
9. Complete canonical data dictionaries and hashed build-manifest supplements
   for both the checkpoint panel and longitudinal event tape. Each references
   the Stage 1 extraction-manifest hash, Stage 2 availability-bundle hash,
   Stage 3 validation-manifest hash, and frozen CLOB-convention hash.

## Hard gates

- Labels only under `labels/`; the panel itself is label-free.
- No outcome-label read occurs until the CLOB convention is frozen and hashed.
- Fixed Stage 4 builders may transform assigned historical periods, but this
  stage produces no outcome-rate or response analysis and inspects no aggregate
  audit result. Reserved-tail rows stay unavailable to its analysis builders
  and every generated period artifact remains behind tested loaders.
- Tied same-provider-timestamp oscillations are not separate provider-path
  flips (#16); positive-offset first-after events never become pre-close
  features (#17).
- No estimated-surface feature is computed from the full dataset (plan,
  "New derived features to test" closing paragraph).

## Needs the owner

- Nothing, in the normal path.

## Done when

- [ ] Checkpoint-panel and longitudinal-event-tape files generated, hashed,
      and partitioned for the assigned historical periods; data files remain
      gitignored while manifests and data dictionaries are committed.
- [ ] Pre-label Stage 2 CLOB-convention hash verified before label construction.
- [ ] Cohort report reconciles with the Stage 1 manifest.
- [ ] Four flip labelers + ex-post descriptor implemented independently,
      tested, and period-partitioned.
- [ ] Mutation harness green over every feature module.
- [ ] `python -m pytest research/lockflip/tests` passes.
