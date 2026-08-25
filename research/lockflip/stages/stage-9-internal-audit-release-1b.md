# Stage 9 — Joint preregistered historical backtest + Release 1B

**Depends on:** Stage 5 published and Stages 6, 7, and 8 all frozen. Release 1B
builds on 1A's descriptive base.
**Plan sections:** "Execution checkpoints" (the ordered flow block — one
purged historical Q1/Q3/Q4/Q5 internal audit), "Checkpoint 3: calibrated
lock frontier" (audit bullets), "Checkpoint 4: end-flip conditions",
"Checkpoint 5: seconds-scale futures/TWAP study", "Q1 / Workstream A" →
"Required result", "Statistical validation", "Executive recommendation"
(Release 1B bullet).
**Acceptance criteria touched:** #3, #4 (evaluation half), #5, #8, #9, #12
(evaluation half), #15, #19, #20.

## Goal

Run every frozen Q1/Q3/Q4/Q5 rule jointly against the
`2026-08-23`..`2026-08-24` period and publish Release 1B. Stage 3 evidence
validation and Stage 5 Q2 description have already accessed limited results
from this period, so it is not an untouched holdout. This stage is one
preregistered historical backtest and **interim feasibility check**; only the
later prospective block can certify a production rule.

## Tasks

1. Pre-flight: register one joint evaluation run ID and verify the hashes of
   `frozen/q1/`, `frozen/q3/`, and
   `frozen/q4q5/` match their `DECISIONS.md` entries **before** any audit
   target or response path is loaded; record the verification. The evaluator
   writes only atomic, run-ID-scoped outputs.
2. Run the single joint historical backtest with each workstream's frozen
   split-safe exclusions: Q1/Q3 use their own required horizons, while Q4/Q5
   use the plan's 150-second boundary purge. Use the checkpoint panel for Q1/Q3
   and longitudinal event tape for Q4/Q5:
   - Q1: frontiers at 10%/5%/1% with one-sided 95% upper aggregate error
     bounds, coverage, abstention, revocations, Up/Down symmetry, `$10`
     rows, exact-vs-proxy incremental result, CLOB reliability and
     CLOB-vs-settlement disagreement — the full "Required result" table.
     Unsupported cells say `insufficient evidence`.
   - Q3: held-out condition risk differences/ratios with uncertainty and
     temporal-fold stability; ranked condition table; sub-10-event
     conditions labeled descriptive/anecdotal.
   - Q4: latency waterfall (per-leg and composite p10/p50/p90, event
     counts, censoring shares) across shock classes and amplitude bands.
   - Q5: dose-response matrix with per-cell counts and uncertainty;
     response-ratio ranges; largest supported observed displacement with
     context; strike-crossing contours within observed support.
3. Assemble `releases/release-1b/REPORT.md` + figures: CLOB calibration,
   condition/ablation results, seconds-scale lag/flash bounds (plan's 1B
   scope), each section stating "interim feasibility — not certification".
4. Record per-rule verdicts (feasible / inconclusive / failed) in the run
   artifact and `DECISIONS.md`. A failed monotonicity or definition does not
   get refit here — it is reported, and any refit waits for a new future
   block (plan, Q5 closing and "Statistical validation").
5. Publish: commit, tag `release-1b`, notify the owner.

## Hard gates

- Audit-period Q1/Q3 targets and Q4/Q5 paths/responses are available only to
  this stage's registered joint evaluator. No exploratory re-slicing follows;
  follow-up questions go to the prospective block or a newly declared future
  window (repeated peeking does not create new evidence).
- A hash-identical restart is allowed only when a technical failure occurred
  before any result became inspectable; record the failed run ID and reason.
  Once any result is exposed, the historical evaluation is spent.
- No model, threshold, contour, or convention changes in this stage — run,
  report, publish.
- Release 1B makes no certification or production-readiness claim; those terms
  may appear only in an explicit disclaimer.
- Reserved-tail rows and anything post-cutoff stay unavailable to this
  evaluator.

## Needs the owner

- Publish sign-off, and a decision point: proceed to Stage 10 certification
  as-is, or revise rules (which restarts the freeze and requires a new
  future evaluation block — say this cost out loud).

## Done when

- [ ] Run ID and hash pre-flight recorded before any audit target/response read.
- [ ] Joint historical backtest executed once; per-rule verdicts recorded.
- [ ] `releases/release-1b/` complete, committed, tagged.
- [ ] Owner briefed on verdicts and the Stage 10 go/no-go decision.
