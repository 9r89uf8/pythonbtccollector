# Stage 1 — Frozen read-only extract

**Depends on:** Stage 0.
**Plan sections:** "Checkpoint 1: reproducible read-only extract and QA",
"What is collected now" (all subsections), "Live readiness snapshot",
"Statistical validation" (the frozen-split paragraph), "Executive
recommendation" (Release 1A deadline sentence).
**Acceptance criteria touched:** #11, #13.

## Goal

Produce the frozen, hashed, day-chunked extract that every later stage treats
as the only data source. After this stage, no analysis touches the live
database until Stage 10's prospective evaluation. Separately authorized
production checkpoints for Price-to-Beat readiness and Stage P operational
checks are outside this analysis-data rule; their evidence may not leak into
the frozen historical analysis.

## Clock-free preflight

1. Write one `.sql` file per normalized relation or series under
   `sql/extract/`; the top-level manifest groups them into the plan's four
   source groups. Each query declares its source-specific cutoff field,
   half-open UTC chunk key, stable total `ORDER BY`, and source identity.
   Filter nothing else; cohort filtering is Stage 4's job. Preserve Decimal as
   canonical text and use fixed COPY options, UTC session settings, NULL
   encoding, and line endings.
2. Run static/offline runner and query tests, commit the runner and SQL, and
   calculate their hashes. The owner reviews and approves those exact hashes
   before extraction. This preflight does not set a cutoff or start the
   Release 1A clock.

## Start extraction (owner + agent together)

1. Owner sets the extraction cutoff (a UTC instant), names an off-repo backup
   destination, and confirms the team is ready to work Stages 1-5 inside the
   Release 1A window using the approved runner and SQL hashes.
2. Record in `frozen/DECISIONS.md`, immediately before the first extract query:
   the cutoff, and the Release 1A deadline (default: cutoff + 120 hours —
   plan, "Executive recommendation"). The deadline is set before extraction,
   not after. At the same instant, create a prebuilt Release 1A
   failure-report template/checklist that Stage 5 can complete without
   importing unfinished Stage 4 code or bypassing a failed evidence gate.
3. Record the frozen historical splits exactly as the plan fixes them
   ("Statistical validation"): training = collection start through
   `2026-08-21T23:59:59.999Z`; calibration = `2026-08-22`; historical
   internal audit = `2026-08-23` through `2026-08-24`; the partial
   `2026-08-25` day excluded. Market-linked rows are assigned by
   `market_start_ms` to half-open UTC ranges; resolution time never moves a
   market between splits. Stage 4 partitions free-standing raw tape rows only
   by their causal timestamps with fixed boundary buffers; Stage 8 later
   detects event origins, assigns them by causal shock-onset time, and applies
   the frozen purge.
4. Markets after `2026-08-24` up to the cutoff are **unassigned by the plan**.
   Default: reserve them. Extraction may mechanically serialize/partition these
   raw rows, but no analysis loader exposes them and no tail labels, panel rows,
   event origins, or response targets are built. Owner confirms or reassigns;
   record the choice.

## Tasks

1. Run each approved query through the Stage 0 runner: day-chunked `csv.gz` into
   `extracts/<cutoff-tag>/`, one manifest per chunk plus an immutable top-level
   extraction manifest. Hash the canonical uncompressed CSV byte stream rather
   than version-dependent gzip bytes. Record extraction time, cutoff, SQL and
   canonical-content hashes, raw extraction row counts, min/max times, and
   missingness. Stage 3 validation and Stage 4 cohort/eligibility results become
   separate manifest supplements that reference this extraction-manifest hash.
2. Record the microstructure retention setting and the actual surviving
   interval in the manifest (plan, Q4: "Record that setting and the actual
   surviving interval in the extract manifest").
3. Sanity-check counts against "Live readiness snapshot" expectations
   (~2,549+ resolved markets, series coverage percentages). Escalate
   discrepancies beyond growth-since-audit; do not "fix" them silently.
4. Build the source-group data dictionary skeleton (column name, type, unit,
   source-key semantics) for Stage 2 to complete.
5. At the end, rerun a cheap deterministic stability query over the extracted
   markets/resolutions slice. If its row count or digest changed during the
   run, redo only the affected not-yet-frozen chunks under the same cutoff and
   record the event. Do not hold an hours-long cross-chunk PostgreSQL snapshot;
   the fixed predicates, frozen files, and stability check define canonicality.
6. After local hash verification, copy the complete extract and manifests to
   the owner-named off-repo backup location and verify the canonical hashes
   there. Record only the non-secret destination description and verification
   result in a committed backup receipt that references, but never mutates, the
   frozen extraction-manifest hash. This ordinary backup is mandatory because
   retained source data, especially microstructure, may later be deleted.

## Hard gates

- Read-only, bounded-timeout queries only; no temp tables, no writes.
- No label aggregation, no outcome analysis — manifest counts only.
- A chunk may be atomically redone before the top-level manifest freezes; the
  failed/partial file is never treated as canonical. After freeze, no
  re-extraction occurs without owner sign-off, and a sanctioned re-extraction
  gets a new `<cutoff-tag>` and manifest version rather than overwriting.
- Extracts and their off-repo backup stay outside Git; manifests and SQL are
  committed.

## Needs the owner

- The cutoff instant and explicit go-ahead to start the 120-hour clock.
- Confirmation of the post-`2026-08-24` tail handling (default: reserve).
- Approval of the exact runner/SQL hashes and the off-repo backup destination.

## Done when

- [ ] All four source groups extracted, chunked, hashed, manifested.
- [ ] Clock-free SQL/runner preflight committed, hash-reviewed, and approved
      before the cutoff was recorded.
- [ ] Cutoff, deadline, and splits recorded in `DECISIONS.md` before the
      extract ran.
- [ ] Microstructure retention + surviving interval recorded.
- [ ] Count sanity report in `artifacts/stage-1/` (pass, or escalated).
- [ ] End-of-run markets/resolutions stability check passed or affected chunks
      were redone and recorded before freeze.
- [ ] Off-repo backup copied and independently hash-verified.
- [ ] Stage 5 deadline record and failure-report template/checklist created.
- [ ] SQL files committed and hash-matched to the manifest.
