# Stage 10 — Prospective certification (Release 2)

**Depends on:** Stage 9 (verdicts inspected, owner chose to certify as-is) and
either Stage 3 `price_to_beat = proven` or a hashed, owner-approved
`frozen/readiness/price-to-beat-<checkpoint-id>.json` produced by a separately
authorized production checkpoint from newly collected immutable evidence. A
provisional historical verdict is never rewritten. If new collector/storage
work is needed, the missing `OPERATIONS.md` must first be restored and reviewed
in its separate repository checkpoint.
**Plan sections:** "Statistical validation" (freeze/hash paragraph, the
prospective-block paragraph, and the `3/n` zero-error paragraph — in full),
"Checkpoint 3: calibrated lock frontier" (final bullets), "Definitions fixed
before analysis" → "Ex-post and prospective lock", "Acceptance criteria"
#4-#5, "Executive recommendation" (Release 2 bullet).

## Goal

Certify (or decline to certify) the frozen lock rule on a genuinely
post-freeze block, used exactly once. This stage spans real calendar time:
declaration, a 14-day untouched window while collectors keep running, then a
single evaluation.

## Tasks

1. Verify the entry gate. Accept either the frozen Stage 3 `proven` status or a
   matching hashed, owner-approved Price-to-Beat readiness record that covers
   the future live-use path. If neither exists, report `not eligible for
   certification` and stop before declaring a window. Do not rewrite Stage 3
   or turn a provisional result into a qualified `certified` result.
2. Build the exact unattended evaluator before the freeze. It performs a fresh
   bounded read-only extract of only the eventual declared window under Stage
   1 conventions, rebuilds the analysis artifacts with frozen code, and emits
   one atomic, run-ID-scoped report. Synthetic-test the complete evaluator —
   never on prospective data — including its date lock, resolution-maturity
   watermark, missing/unresolved-market behavior, and technical-failure path.
3. Freeze-and-hash the complete bundle: evaluator source and environment,
   extract SQL, as-of rules, features, model and hyperparameters, calibration,
   declaration regions, exclusions, acceptance criteria, maturity watermark,
   and crash/restart rule. Record the bundle hash in `DECISIONS.md`.
4. **Before inspecting any prospective feature, response, or label**, declare
   and record: prospective start = next UTC midnight after the freeze;
   exclusive end = exactly 14 complete UTC days later; the market-ID range;
   the artifact hashes.
5. During the window: no inspection of study rows or aggregates, early
   stopping, or refitting. Existing routine service-health monitoring and
   incident response may continue without exposing study features, responses,
   or outcomes; record any material collection change and apply the frozen
   invalidation rule. Collectors otherwise keep running unchanged.
6. After the window closes, wait for the frozen resolution-maturity watermark
   before spending the evaluation. The watermark uses only completion status
   and counts, never outcome values, and predefines how markets still unresolved
   at its deadline are handled.
7. Run the frozen evaluator once. Apply the plan's standards — aggregate
   one-sided 95% upper error bound vs alpha, coverage,
   first-declaration-per-market accounting, `3/n` logic for zero-error cells
   (~300 declarations needed for a nominal 1% bound), day-block sensitivity or
   conservative effective sample.
8. Report exactly one of: `certified` (bounds and coverage pass),
   `insufficient evidence`, or `interim/inconclusive` (too few events, first
   declarations, or independent day blocks — the rule is not weakened).
9. Assemble `releases/release-2/`, commit, tag `release-2`, notify owner.

## Hard gates

- The prospective block is used once. Any refit or threshold change
  afterward requires a newly declared future block — no exceptions.
- No agent, script, or "sanity check" touches window features, responses, or
  labels before the declared end and frozen maturity watermark. Build the
  evaluator so its real-data extraction cannot run early.
- A hash-identical restart is permitted only if a technical failure occurs
  before any result becomes inspectable; record the failed run ID and reason.
  Once any result is exposed, the prospective block is spent.
- `certified` appears only if the plan's bounds and coverage pass, on the
  exact frozen rule, and Price-to-Beat readiness was proven or separately
  recorded before the window. There is no provisional-certification path.
- Certification produces a research release, not a runtime deployment. Any
  live scoring integration is a separate owner-authorized production
  checkpoint with inference-parity and normal deployment verification.

## Needs the owner

- Resolution of any provisional Price-to-Beat blocker through a separately
  reviewed collector/storage checkpoint and named append-only readiness record
  before this stage starts. Restore/review `OPERATIONS.md` first if production
  changes are required.
- Sign-off on the freeze moment after the evaluator passes synthetic tests (it
  starts the calendar).
- Authorization for the single post-window evaluation run.
- Distribution decisions for Release 2.

## Done when

Exactly one terminal branch applies:

**Ineligible branch**

- [ ] Neither permitted Price-to-Beat proof exists; `not eligible for
      certification` is recorded, no window is declared or spent, and the
      owner is briefed.

**Eligible prospective-evaluation branch**

- [ ] Price-to-Beat entry gate satisfied by the frozen Stage 3 verdict or the
      named prospective readiness record.
- [ ] Exact evaluator, maturity watermark, and crash policy synthetic-tested.
- [ ] Complete bundle frozen, hashed, recorded before the declaration.
- [ ] Window declared before any prospective feature, response, or label
      inspection, with ID range + hashes.
- [ ] Frozen maturity watermark reached after the window, with unresolved
      handling recorded before any outcome inspection.
- [ ] Single evaluation run executed; report states `certified` /
      `insufficient evidence` / `interim/inconclusive`.
- [ ] `releases/release-2/` committed and tagged; owner briefed.
