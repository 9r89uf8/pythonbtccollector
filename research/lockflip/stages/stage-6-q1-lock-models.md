# Stage 6 — Q1 lock models (train/calibration only)

**Depends on:** Stage 4 complete and Stage 5 Release 1A published. May then run
in parallel with Stages 7 and 8.
**Plan sections:** "Q1 / Workstream A: when is Up or Down locked?" (Analysis
and Required result, in full), "Definitions fixed before analysis" →
"Ex-post and prospective lock" and "Fixed-terminal-window spot proxy",
"Statistical validation".
**Acceptance criteria touched:** #3, #4 (construction half), #5, #15
(comparison half), #19.

## Goal

Fit, calibrate, and **freeze** the Q1 lock machinery using training and
calibration data only. Stage 9 evaluates it in the single joint preregistered
historical backtest, and the word "certified" belongs to Stage 10 alone.

## Tasks

1. Raw disagreement counts by second remaining and adaptive bps bands
   (training data), always with market counts and intervals.
2. Transparent nonlinear baseline (time remaining, signed distance, TWAP
   momentum, volatility) — the primary rule unless the proxy demonstrably
   improves held-out performance (plan, Analysis step 3).
3. Paired Context/proxy model with `B_hat`/`F_hat_required`/`R_hat`/
   flat-future features and quality fields; measure incremental value via
   train/cal folds only. If Stage 3 set `spot_proxy = abstain`, record this
   branch as unavailable and keep the exact-TWAP baseline; do not synthesize
   proxy inputs.
4. Market-free forward-average excursion curves: training-only or
   out-of-fold origins, frozen cadence/gap convention, day/regime-blocked
   uncertainty, nonoverlapping-origin sensitivity, no window crossing a
   split boundary or the cutoff. Apply only the split-safe lookback/forward
   exclusion required by the curve's own frozen horizon; the plan's fixed
   150-second purge belongs to Q4/Q5, not Q1. These curves remain supplemental
   context — they never fill sparse labeled cells (#19).
5. Calibrate on the `2026-08-22` calibration day; freeze monotone
   declaration regions and the abstention rule; build the selective
   settlement and CLOB classifiers (Up locked / Down locked / abstain).
6. Multiplicity: simultaneous risk-control bounds across horizons,
   directions, and thresholds, or explicit pointwise labeling (Analysis
   step 10).
7. Revocation measurement machinery (first declaration per market; later
   abstention/opposite/official error) — to be executed on audit data in
   Stage 9.
8. CLOB reliability comparison machinery at the predeclared confidence bands
   (0.90/0.95/0.97/0.99) with spread/age/sample reporting (#15).
9. Freeze everything — features, hyperparameters, calibration, regions,
   abstention, exclusions — as a hashed bundle under `frozen/q1/`; append
   the freeze to `DECISIONS.md`.

## Hard gates

- Split-specific loaders may read train/calibration-period rows only across the
  checkpoint panel, labels, event-derived inputs, and any response targets.
  Audit/tail feature rows and labels, not merely their label directories, are
  barred.
- If the Stage 3 Price-to-Beat verdict was provisional, every artifact here
  carries the `provisional` marker (plan, "Cohort construction").
- Sparse cells return `insufficient evidence`; no bound relaxation (#5).
- Excursion curves are contextual; they never impute, override abstention,
  or certify (plan, Q1 closing paragraph).
- After the Stage 9 backtest result is exposed, nothing in `frozen/q1/` may be
  retuned against historical data.

## Needs the owner

- Nothing, in the normal path.

## Done when

- [ ] Frozen hashed bundle in `frozen/q1/` + `DECISIONS.md` entry.
- [ ] Train/cal report in `artifacts/stage-6/` with the "Required result"
      table structure ready to be filled by the Stage 9 audit.
- [ ] Fold-honesty test: no code path reads any audit/tail-period data
      (enforced by loader tests, not convention).
