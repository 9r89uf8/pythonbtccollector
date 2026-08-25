# Lock/Flip/TWAP Study — Execution Stages

This directory is the build layer for
[`LOCK_FLIP_TWAP_RESEARCH_PLAN.md`](../../../LOCK_FLIP_TWAP_RESEARCH_PLAN.md)
(the "plan"). The plan is the single source of truth for methodology. Stage
files add only what the plan does not decide: engineering choices, task
breakdown, artifact locations, and per-stage guardrails. They reference the
plan by section heading and deliberately do not restate its content.

**Conflict rule:** if a stage file and the plan disagree, the plan wins. Do
not silently follow either version; report the discrepancy to the owner and
record the resolution in `../frozen/DECISIONS.md`.

**Naming warning:** the plan and [`AGENTS.md`](../../../AGENTS.md) already use
"Phase 4"/"Phase 5" for the raw-capture rollout on the droplet. Execution
units in this directory are therefore called **stages**. Never write "Phase N"
when you mean a stage; "Phase 4" always means the deferred raw-capture
partition/retention validation.

## How to work a stage

1. Read this README, your stage file, and every plan section listed under its
   "Plan sections" heading — in full. The stage file is not a substitute.
2. Confirm each dependency stage shows `done` in the status table and its
   "Done when" items actually hold.
3. Repo-wide rules in `AGENTS.md` apply (Decimal-only prices, UTC epoch
   milliseconds, shared market-window helper, testing expectations). The
   research-only package and model-boundary numeric exception are the explicit
   carveouts recorded there; Stage 0 implements them but does not broaden them.
4. Stop and ask at every "Needs the owner" item. Never guess credentials,
   cutoff instants, publication decisions, or authorizations.
5. On completion: tick the stage's "Done when" boxes, update the status table
   below, and append any newly frozen convention to `../frozen/DECISIONS.md`
   (append-only: UTC date, decision, evidence pointer; never edit or delete
   past entries).

## Global rules (every stage)

- Production PostgreSQL is strictly read-only for this study: reader role,
  session `default_transaction_read_only = on`, bounded `statement_timeout`,
  no temporary tables, no schema changes, no writes of any kind.
- Per the plan's preamble: no Git history, deleted files, backups,
  precomputed predictions, or prior derived flip labels as inputs. The
  hash-verified off-repo mirror created by Stage 1 is only disaster recovery
  for that same canonical extract; it is not an alternate evidence source.
- Raw prices, dollar values, margins, and financial arithmetic stay `Decimal`.
  Only finalized dimensionless model features may cross to an explicitly
  named floating type for research-only model fitting, at a documented and
  tested boundary. Figure rendering may use isolated floating display copies
  of finalized values, but those copies never feed calculations, models, or
  persisted truth. Persisted and tabulated financial values remain `Decimal`.
- Secrets never enter the repo, manifests, artifacts, or reports.
- Period access control applies to every series, not only outcome-label files.
  Split-specific loaders enforce the market/time periods frozen in Stage 1:
  - Stages 1-4 may transform the assigned train/calibration/audit periods with
    fixed, non-modeling code. Stage 1 may mechanically extract and partition
    reserved-tail rows, but no analysis loader may expose them and no tail
    labels, panel rows, event origins, or response targets are built. Historical
    settlement-evidence access is limited to extraction, row-level QA, the
    frozen evidence gate, and split materialization; these stages do not
    publish or tune on outcome rates.
  - Stage 5 may read the historical periods for its predeclared Q2 descriptive
    aggregates. This means the historical audit period is not an untouched
    holdout.
  - Stages 6-8 may load train/calibration-period rows only, across features,
    outcomes, event paths, and forward-response targets. Audit and tail periods
    are barred by tested loaders, not merely by directory convention.
  - Stage 9 alone runs the one frozen joint Q1/Q3/Q4/Q5 evaluator against the
    historical audit period. It is a preregistered historical backtest and
    interim feasibility check, not prospective certification.
  - The post-`2026-08-24` tail remains unavailable to analysis/evaluation
    loaders unless the owner explicitly reassigns it before a new freeze.
    Mechanical extraction alone does not spend it. Prospective-block data is
    read only by Stage 10's single frozen evaluation job after its declared
    window matures.
- Frozen means frozen. Once a convention, model, or declaration region is
  recorded under `../frozen/`, later stages verify it; they do not retune it.
  Every freeze has a machine-readable configuration and hash referenced by
  `../frozen/DECISIONS.md`; prose is not the executable source of truth. Any
  refit requires a newly declared future evaluation block (plan, "Statistical
  validation").

## The Release 1A clock

Stage 1 begins with a clock-free preflight that writes, tests, commits, and
hashes the extraction runner and SQL. The plan's default 120-hour Release 1A
deadline starts only when the owner approves those hashes and the cutoff is
recorded immediately before extraction — pass or fail, something publishes.
At that instant, create the recorded deadline plus a prebuilt failure-report
template/checklist that Stage 5 can use even if a later gate fails. Missing the
window does not extend the deadline; it changes what Stage 5 publishes (the
failure/blockers variant the plan specifies).

## Stage map

| Stage | File | Plan checkpoint | Depends on |
| --- | --- | --- | --- |
| 0 Scaffolding | `stage-0-scaffolding.md` | none (new) | — |
| 1 Frozen extract | `stage-1-frozen-extract.md` | Checkpoint 1 (part) | 0 |
| 2 QA + causality harness | `stage-2-qa-and-causality.md` | Checkpoint 1 (part) | 1 |
| 3 Settlement gate + proxy validation | `stage-3-settlement-gate.md` | Checkpoint 1 (part) | 2 |
| 4 Cohorts, panel, labels | `stage-4-panel.md` | Checkpoint 2 (part) | 3 (`settlement_core = proceed`) |
| 5 Q2 descriptive + Release 1A | `stage-5-release-1a.md` | Checkpoint 2 | 1; success path also 4 |
| 6 Q1 lock models (train/cal) | `stage-6-q1-lock-models.md` | Checkpoint 3 (part) | 4, 5 |
| 7 Q3 conditions (train/cal) | `stage-7-q3-conditions.md` | Checkpoint 4 (part) | 4, 5; freeze barrier on required 6/8 artifacts or omission |
| 8 Q4/Q5 event-study design (train/cal) | `stage-8-q4q5-design.md` | Checkpoint 5 (part) | 4, 5 |
| 9 Joint historical backtest + Release 1B | `stage-9-internal-audit-release-1b.md` | Checkpoints 3-5 (audit) | 5, 6, 7, 8 |
| 10 Prospective certification | `stage-10-certification.md` | Checkpoint 3 (end) | 9 + Stage 3 `proven` status or a pre-window owner-approved Price-to-Beat readiness record |
| P Subsecond operational track | `stage-p-subsecond-track.md` | Checkpoint P + 6 | parallel |

Sequencing notes:

- Stages 6, 7, and 8 may develop in parallel after Stage 4 is complete and
  Stage 5 has published Release 1A, as required by the plan's ordered flow.
  Their final freezes must honor cross-stage derived-feature dependencies or
  explicitly omit those optional features before freezing. The plan requires
  **one** joint purged Q1/Q3/Q4/Q5 historical evaluation after all definitions
  freeze — that job is Stage 9. Do not run a per-stage audit inside 6, 7, or 8.
- Stage 1 records the Release 1A deadline and creates its failure-report
  template/checklist when extraction starts. Stage 5's complete descriptive
  success path begins after Stage 4; Release 1A publishes before Stages 6-8
  start.
- Stage P starts in parallel with Stages 1-2 (plan: the operational decision
  track must not wait), and its final decision memo lands after Stage 9.

## Status

| Stage | Status | Completed (UTC) | Notes |
| --- | --- | --- | --- |
| 0 | not started | | |
| 1 | not started | | clock-free SQL preflight, then owner cutoff starts 120h clock |
| 2 | not started | | |
| 3 | not started | | |
| 4 | not started | | |
| 5 | not started | | deadline set in Stage 1 |
| 6 | not started | | |
| 7 | not started | | |
| 8 | not started | | |
| 9 | not started | | single frozen historical backtest job |
| 10 | not started | | blocked on Price-to-Beat proof/readiness record + owner freeze sign-off |
| P | not started | | awaiting owner authorization; `OPERATIONS.md` required only to run canary |
