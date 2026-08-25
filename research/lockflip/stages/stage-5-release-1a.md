# Stage 5 — Q2 descriptive truth set + Release 1A

**Starts with:** Stage 1's recorded Release 1A deadline. The complete
descriptive success path depends on Stage 4; the deadline failure/blocker path
has no dependency beyond Stage 1 and must remain runnable if Stages 3-4 fail or
remain incomplete.
**Plan sections:** "Q2 / Workstream B: where do late flips happen?",
"Checkpoint 2: descriptive truth set", "Executive recommendation" (Release
1A bullet, in full — including the failure-variant obligation), "Q1-Q5 at a
glance" (Q2 row), "Definitions fixed before analysis" → "Price distances"
(the two `$10` questions).
**Acceptance criteria touched:** #3, #6, #7, #20 (reporting half), #21.

## Goal

Publish Release 1A: the descriptive truth about late flips, on time,
whatever state the rest of the study is in. This is the only stage with a
hard calendar deadline.

## Tasks

1. Use Stage 1's recorded deadline and prebuilt failure-report
   template/checklist. Keep that fallback completable without importing
   unfinished Stage 4 code or bypassing a failed evidence gate.
2. On the success path, produce recurrent one-second transition probabilities
   for oracle and CLOB side changes, with every split the plan lists
   (direction, single/multiple crossing, provider vs observable path,
   persistent vs not).
3. Last-observed-change distributions with interval censoring: lower-bound
   and gap-sensitive upper-bound flip fractions; unknown paths never counted
   as nonflips; gapped/stale states reported as abstentions with their
   official loss rate.
4. Leader/final-outcome disagreement rates by checkpoint (`T-20`..`T-1`).
5. Headline heatmap: final-20-second time-by-exact-TWAP-margin risk surface,
   plus the two separate `$10` panels (exact-TWAP margin vs spot/futures
   pressure), each converted per market.
6. Secondary exploratory surface: time-by-`R_hat`/banked-contribution with
   proxy eligibility, predictive bands, and disagreement vs the exact
   rolling-TWAP leader. Label every spot-proxy panel exploratory (plan). If
   Stage 3 set `spot_proxy = abstain`, publish that verdict and its evidence
   instead of manufacturing the surface.
7. Evidence-audit summary: Stage 1 manifest counts, Stage 2 QA verdicts,
   Stage 3 gate verdict and boundary-alignment results.
8. Assemble `releases/release-1a/REPORT.md` + figures. Every cell carries
   market count and uncertainty; sparse cells suppressed or merged.
9. Publish: commit, tag `release-1a`, notify the owner. External
   distribution is the owner's call.
10. If the gate failed or Checkpoint-2 work is incomplete at the deadline:
   publish the plan's failure variant instead — gate failure, exclusions,
   completed descriptive results, and explicit remaining blockers. Do not
   silently wait (plan, Release 1A bullet).

## Hard gates

- Descriptive only: no fitted risk models, unsupported lock declarations, or
  certification claims. References to "locked" may explain that Release 1A
  does not establish such a rule; they may not present one.
- Labels for all historical splits (train/cal/audit) may be aggregated here
  — that is what a descriptive truth set is — but reserved-tail rows remain
  unavailable to its loaders, and nothing here tunes a model convention.
  Because this stage uses audit-period Q2 outcomes, Stage 9 is described as a
  preregistered historical backtest rather than an untouched holdout.
- A largest observed zero-reversal region is descriptive and carries its
  one-sided upper bound; never call it an arithmetic or certain lock (plan,
  Q2 closing paragraph).
- The deadline is immovable. Scope shrinks; the date does not.

## Needs the owner

- Sign-off on the publish action (commit + tag is the default "publish";
  say so explicitly when notifying).

## Done when

- [ ] `releases/release-1a/` complete with report, figures, and the evidence
      audit — or the failure variant.
- [ ] Published (committed + tagged) at or before the recorded deadline.
- [ ] Every reported cell has n + uncertainty; spot-proxy panels labeled
      exploratory.
- [ ] Owner notified with a one-paragraph summary of what shipped.
