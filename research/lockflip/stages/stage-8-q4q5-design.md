# Stage 8 — Q4/Q5 event-study design (train/calibration only)

**Depends on:** Stage 4's longitudinal event tape and period loaders, and Stage
5 Release 1A published. May then run in parallel with Stages 6 and 7.
**Plan sections:** "Q4 / Workstream D: futures-to-TWAP visibility lag" (in
full), "Q5 / Workstream E: can a flash move TWAP materially?" (in full),
"Statistical validation" (the 150-second purge rule).
**Acceptance criteria touched:** #8, #9, #10, #20 (construction half).

## Goal

Design and freeze the shock-detection and response-estimation machinery on
train/cal event-tape periods only. The plan is explicit that the **headline
Q4/Q5 response distributions are estimated on the purged historical
internal-audit block** — that estimation is Stage 9's single run, not this
stage.

## Tasks

1. Causal shock detector: fires at the first predeclared amplitude/
   volatility crossing using only the quiet pre-period and information
   through that receipt bucket on the longitudinal event tape; direction +
   interval-censored onset. Assign each detected origin to its split by that
   causal onset, then expose only train/cal origins during this stage. Frozen
   on train/cal.
2. Eligibility accounting: require `collector_healthy = true` and non-null
   trade-range fields; report exclusion rates against the plan's audited
   figures (~1.87% unhealthy, ~13.47% missing spot high/low, ~4.80% missing
   futures high/low). This cohort is bounded by the microstructure surviving
   interval recorded in the Stage 1 manifest — restate that bound in every
   artifact.
3. Ex-post morphology descriptors (peak, censored peak bucket, duration
   bounds, reversal, integrated bp-seconds) — descriptive stratifiers only,
   never cohort-entry criteria or live predictors.
4. Shock classes and controls: futures-only, Binance-confirmed,
   Binance+Chainlink-confirmed, sustained amplitude-matched, quiet matched
   controls; plus random quiet-window placebos.
5. Response design: event studies / local projections and the regularized
   distributed-lag model on price changes; TWAP jitter and response
   thresholds from training/quiet controls only; interval-censored
   first-response handling; overlapping-shock exclusion and censoring at the
   next shock; day-blocked uncertainty; 150-second purge at every split
   boundary (recompute if any horizon grows).
6. The three lag decompositions (system-visible, provider-time, TWAP
   delivery) kept separate; the ~1.8 s delivery lag is never presented as
   economic response time.
7. Two cohorts wired separately: general transmission (through 90 s, may
   cross the five-minute boundary) vs end-of-market (censored at close).
8. Q5 surface definitions: dose-response cells (amplitude × duration/
   bp-seconds × confirmation × volatility × distance × seconds remaining)
   with predeclared minimum event counts; the two arithmetic cases (rolling
   response vs fixed-terminal counterfactual) implemented as clearly
   separated reference calculations (#20); any monotonicity constraint
   chosen now, on train/cal only.
9. Freeze detector, thresholds, cohorts, purge, placebos, cell definitions,
   and model families as a hashed bundle under `frozen/q4q5/`; append to
   `DECISIONS.md`. Produce train-side diagnostics only (detector counts,
   placebo behavior, censoring shares).

## Hard gates

- Split-specific loaders may read train/calibration event-tape periods only.
  Audit/tail prices, paths, future responses, response tensors, and outcome
  labels are all barred; Q4/Q5 leakage control is not limited to label files.
- One-second data yields interval statements only: "within-second
  excursions", never "100 ms flashes" (#10).
- No cohort entry or live predictor uses future reversal/morphology
  information (plan, "Statistical validation" last bullet).
- After any Stage 9 backtest result is exposed, no definition or contour here
  may be revised against that period (plan, Q4 freeze paragraph and Q5
  closing).

## Needs the owner

- Nothing, in the normal path.

## Done when

- [ ] Frozen hashed bundle in `frozen/q4q5/` + `DECISIONS.md` entry.
- [ ] Train-side diagnostics in `artifacts/stage-8/` (detector counts,
      exclusions vs expected rates, placebo sanity).
- [ ] Purge logic tested; fold-honesty test proves no audit/tail event path or
      future response is readable.
