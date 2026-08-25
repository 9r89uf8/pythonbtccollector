# Stage 0 — Engineering scaffolding

**Depends on:** nothing.
**Plan sections:** "Checkpoint 1: reproducible read-only extract and QA"
(read-only constraints only), "What is collected now" (orientation),
"Causal time alignment" (orientation only — implemented in Stage 2).
**Acceptance criteria touched:** groundwork for #2 and #13.

## Goal

Create the engineering substrate every later stage assumes. This stage
produces zero research results and inspects zero outcome labels.

## Layout to create

```text
research/lockflip/
  stages/          # these documents
  lockflip/        # Python package: config, dbio, manifest, decimal_io, asof
  sql/             # every extraction/QA query as a .sql file (hashed later)
  extracts/        # day-chunked csv.gz extract output   (gitignored)
  manifests/       # extraction manifests, JSON          (committed)
  frozen/          # DECISIONS.md + hashed frozen bundles (committed)
  labels/          # period-partitioned label files       (gitignored)
  panel/           # built checkpoint panels              (gitignored)
  event_tape/      # longitudinal Q4/Q5 analysis data     (gitignored)
  artifacts/       # per-stage reports and figures       (reports committed)
  releases/        # release-1a/, release-1b/, release-2/ (committed)
  tests/
  requirements.txt # research-only dependencies
  pytest.ini
```

## Decisions this stage records (defaults; owner may override)

- Research uses the repository's Python 3.12 baseline. The fully resolved
  environment, including numpy, scipy, statsmodels, scikit-learn, matplotlib,
  pytest, the selected dataframe library, and their transitive dependencies,
  is exactly pinned in `research/lockflip/requirements.txt` for a separate
  venv. It is never added to the root `requirements.txt` or installed on the
  droplet; the selected library and environment fingerprint are recorded in
  the Stage 0 freeze artifact.
- Research code may import `price_collector.market` (window math must not be
  reimplemented — AGENTS.md) but must never import collector write paths, and
  no collector code may import `research/`.
- Database access: run `psql` on the droplet over SSH using the reader role,
  with `SET default_transaction_read_only = on;` and a recorded
  `statement_timeout`, streaming `\copy (SELECT ...) TO STDOUT (FORMAT csv,
  HEADER)` through gzip to the local `extracts/` directory. Credentials stay
  on the droplet (sourced from `/etc/price-collector/`); they are never
  copied locally, into the repo, or into any manifest. The runner is the only
  automated database path, accepts committed SQL files only, and refuses
  anything other than a single read-only `SELECT`. Before Stage 1 extraction
  and before its cutoff/clock is recorded, the owner reviews and approves the
  exact runner and SQL hashes. A restricted SSH account or forced command is an
  optional owner-selected hardening step, not a prerequisite for this study.
- Research tests run with an explicit `python -m pytest research/lockflip/tests`
  (root `pytest.ini` pins `testpaths = tests` and stays untouched).
- All timestamps UTC epoch milliseconds; CSV prices are text, parsed to
  `Decimal` via `lockflip.decimal_io` only.

## Tasks

1. Create the layout, venv, `requirements.txt`, `pytest.ini`, and gitignore
   entries for `extracts/`, `labels/`, `panel/`, `event_tape/`, and large
   artifacts.
2. `lockflip/config.py`: loads droplet host/user (reuse the
   `droplet.env.example` convention) and research settings from env; no
   secrets in code or defaults.
3. `lockflip/dbio.py`: the SSH + read-only psql streaming runner described
   above, taking a committed `.sql` file and emitting day-chunked `csv.gz`
   plus a manifest entry. Refuse to run unapproved SQL hashes or any statement
   that is not a single read-only `SELECT`.
4. `lockflip/manifest.py`: manifest writer/verifier — extraction UTC time,
   cutoff, SQL sha256, canonical uncompressed-content sha256, row counts,
   min/max source timestamps, and missingness fields (plan, Checkpoint 1).
5. `lockflip/decimal_io.py`: Decimal-preserving CSV read/write helpers. Tests
   assert that raw financial values and calculations never pass through
   `float`, any dimensionless model-matrix conversion occurs only at its named
   boundary, and isolated rendering copies cannot feed calculations or
   persisted results.
6. Create `frozen/DECISIONS.md` with its append-only header and a first entry
   recording the defaults above. Define the common freeze contract: every
   later freeze is a machine-readable configuration plus hash referenced from
   `DECISIONS.md`; prose is never the executable source of truth.
7. Schema snapshot: use the reader role for every reader-visible table the
   plan names, diff the result against `schema.sql`, commit the snapshot, and
   note drift in `artifacts/stage-0/`. Because `price_reader` intentionally has
   no `raw_capture` access, prepare a bounded count query for both raw tables
   for the owner to run with an administrative role. Record the owner-returned
   output and query hash if supplied; otherwise record the check as deferred to
   Stage P. This optional raw-capture check does not block the one-second study,
   and the API reader's grants are never broadened.
8. Integration smoke test: with owner approval, extract one trivial bounded
   query end to end (chunked csv.gz + manifest + hash verification). Keep this
   live SSH check explicitly separate from the ordinary offline pytest suite.

## Hard gates

- No production writes of any kind; no temp tables (plan, Checkpoint 1).
- Do not read derived research/prediction tables.
- The Stage 0 query validator is defense in depth, not permission for an agent
  to run unreviewed SQL or arbitrary SSH commands. Live execution requires the
  owner-approved code and SQL hashes.
- No droplet package installs, service changes, or env edits — this stage is
  analysis-side only, so the AGENTS.md droplet-update handoff is not
  triggered.

## Needs the owner

- SSH access to the droplet and confirmation the reader role may be used
  this way.
- Approval of the layout and the separate-research-venv decision (or an
  override, recorded in `DECISIONS.md`).
- Optional execution of the bounded administrative raw-table count query, or
  delivery of its recorded output. The automated reader runner cannot perform
  it, and deferring it does not block Stage 1.
- Optional decision on stricter SSH hardening; declining it does not block the
  owner-reviewed reader-only workflow.

## Done when

- [ ] Smoke extraction produces chunked `csv.gz` + verified manifest.
- [ ] `python -m pytest research/lockflip/tests` passes.
- [ ] Schema snapshot committed; drift vs `schema.sql` reported or none.
- [ ] Raw-capture count result recorded if supplied; otherwise its explicit,
      non-blocking deferral to Stage P recorded without changing reader grants.
- [ ] Pinned research environment and selected dataframe library recorded.
- [ ] `frozen/DECISIONS.md` exists with the Stage 0 defaults and freeze-contract
      entry.
