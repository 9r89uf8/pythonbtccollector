# Stage P — Parallel subsecond operational track (Phase 4 canary + decision)

**Runs in parallel** with Stages 1-2 onward (plan: start this decision track
early so avoidable delay does not discard future subsecond history). Its
closing memo lands after Stage 9.
**Plan sections:** "Optional high-resolution follow-up" (in full),
"Parallel operational checkpoint P: Phase 4 decision and canary",
"Checkpoint 6: optional subsecond capture decision", plus `AGENTS.md` →
"High-Resolution Rollout Status" (the production truth about what Phase 4
has and has not validated).
**Acceptance criteria touched:** #10.

**Operational prerequisite:** the repository's referenced `OPERATIONS.md` is
currently absent. That does not prevent the owner from declining authorization
and completing the `not authorized` branch. If authorization is granted,
restoring and reviewing the runbook is a separate repository prerequisite
before any Phase 4 preflight or canary action.

Here — and only here — "Phase 4" carries its production meaning: the
deferred raw-capture partition-boundary and retention validation. This stage
is mostly owner-executed; the agent prepares, documents, and records.

## Goal

Get an explicit, recorded outcome for the subsecond question: authorized and
accepted, rolled back, or not authorized — and, after Stage 9, a decision
memo on whether subsecond capture is still worth pursuing. Research proceeds
at one-second resolution regardless of this track.

## Tasks

1. Draft the authorization request for the owner: what a futures-only 100 ms
   canary would validate, the unproven risks named in `AGENTS.md`
   (future-partition creation, expired-partition removal, 72-hour
   retention, sustained raw-relation budget), and the plan's acceptance
   bar. The owner grants or declines; record either outcome.
2. If authorization is declined, record `not authorized`, skip the operational
   tasks, and continue only to the post-Stage-9 decision memo.
3. If authorized, verify that `OPERATIONS.md` exists and contains the applicable
   bounded Phase 4 deployment, monitoring, acceptance, and rollback commands.
   If it is absent or incomplete, record the nonterminal status
   `authorized — operationally blocked` and stop before operational work.
4. On the authorized branch, freeze the preflight, acceptance, monitoring, and
   rollback plan before activation, then support the plan's seven-step
   operational sequence — preflight checks, a canary long enough to cross a
   partition boundary and at least one 72-hour retention-expiry cycle,
   continuous critical-path health verification, then accept or roll back.
5. Once the branch reaches a final decision, record the outcome in
   `frozen/DECISIONS.md` and the status table as exactly one of:
   `not authorized`, `rolled back`, or `Phase 4 accepted`. The authorized-but-
   blocked status is nonterminal and must not be relabeled as a final outcome.
6. After Stage 9: write the Checkpoint-6 decision memo in
   `artifacts/stage-p/` — do one-second bounds already answer the practical
   question? If Phase 4 passed and subsecond precision remains valuable,
   propose the frozen subsecond research design and a later bounded
   validation capture; otherwise state the one-second limit and stop.

## Hard gates

- The agent never enables capture. `RAW_FUTURES_TRACE_ENABLED`, related
  settings, schema, and services are changed only by the owner on the
  droplet; any droplet change follows the `AGENTS.md` droplet-update
  handoff, prepared as copy/paste commands for the owner to run.
- Analysis having started is not authorization (plan). No gate is weakened
  because the calendar moved.
- Missing or unreviewed `OPERATIONS.md` blocks an authorized canary, not the
  owner's `not authorized` decision. Do not improvise production commands from
  the research plan or README fragments. Record this as
  `authorized — operationally blocked` until the prerequisite is resolved or
  the owner later declines authorization.
- Canary rows are engineering QA until Phase 4 acceptance, and even then
  they are exploratory/training evidence only — never part of the
  historical one-second audit or the prospective block.
- A short healthy interval cannot claim retention or partition validation
  (plan and `AGENTS.md` agree on this).

## Needs the owner

- The authorization decision itself; every droplet action; the acceptance
  or rollback call.
- If authorization is granted, restoration/review of `OPERATIONS.md` as a
  separate repo checkpoint before operational execution.

## Done when

- [ ] Authorization request delivered and a final outcome recorded
      (`not authorized` / `rolled back` / `Phase 4 accepted`). Leave this
      unchecked while status is `authorized — operationally blocked`.
- [ ] If authorized: `OPERATIONS.md` prerequisite verified before preflight or
      canary work.
- [ ] If run: preflight and acceptance evidence archived in
      `artifacts/stage-p/`.
- [ ] Checkpoint-6 decision memo written after Stage 9.
