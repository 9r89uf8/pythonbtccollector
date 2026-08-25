# Stage 2 — Data QA, timestamp dictionary, causality harness

**Depends on:** Stage 1.
**Plan sections:** "Checkpoint 1: reproducible read-only extract and QA" (QA
bullets), "Causal time alignment" (both timelines, in full), "Definitions
fixed before analysis" → "Time bins" and the pre-label CLOB-convention
paragraph under "Four different flips", "Live readiness snapshot" (TWAP timing
material).
**Acceptance criteria touched:** #2, #13, #16 (audit half).

## Goal

Freeze the pre-label CLOB convention, verify the frozen extract, and build the
two pieces of infrastructure that every downstream feature depends on: the
source timestamp/availability dictionary and the as-of join library that
enforces the observable timeline.
All work here runs against the assigned train/calibration/historical-audit
partitions of `extracts/` only — never the live database. Reserved-tail files
receive byte/hash integrity verification only; their rows are not decoded.

## Tasks

1. Before reading any outcome evidence, use training-period unlabeled cadence
   diagnostics to freeze the normalized-probability formula, crossed-market
   handling, maximum component age, maximum cross-outcome skew, executable
   bid/ask uncertainty, and `indeterminate` rule required by the plan. Store
   the machine-readable CLOB convention and hash under `frozen/` and reference
   it from `DECISIONS.md`.
2. QA checks over the extract (each emits a pass/fail artifact):
   half-open market-boundary behavior (reuse `price_collector.market`),
   settlement-rule identity vs the `2026-08-14T00:00:00Z` cutover, outcome
   internal consistency, Decimal precision survival, TWAP event cadence and
   receipt-lag distributions vs "Live readiness snapshot" expectations, and
   session/gap record handling.
3. Source timestamp dictionary: one entry per series — row-key semantics,
   provider-time field, receive-time field, and the frozen **availability
   rule** for the observable timeline, transcribed from "Causal time
   alignment" (probability = max raw component receive time; microstructure
   = finalization time; flow/book = publication/`created_at`/flush-delay
   gating; etc.). Ship it as both a committed doc and a code table the join
   library reads.
4. `lockflip/asof.py`: as-of join library. Given a decision instant, return
   the latest causally available observation per source under the
   dictionary's rules; expose the provider timeline as an explicitly
   separate mode. No caller constructs availability logic by hand. Freeze the
   dictionary, code table, and as-of implementation as one machine-readable
   hashed bundle and reference its hash from `frozen/DECISIONS.md`.
5. Causality mutation harness (acceptance #2): a reusable pytest fixture
   that, for sampled markets and checkpoints, deletes or perturbs every
   post-checkpoint row in a copy of the extract and asserts computed
   features are byte-identical. Later stages must run their features
   through it.
6. Duplicate provider-time TWAP audit: recompute distinct provider seconds
   and duplicates from the durable event table (plan: the materialization
   hides them), and build the first/last/range collapse **sensitivity
   harness**. The deterministic collapse rule itself is frozen in Stage 3
   (gate step 6) using this audit as evidence.
7. Wall-vs-monotonic offset clustering for TWAP sessions (plan, provider
   timeline) to flag clock steps; record findings.

## Hard gates

- Extract-only; no live-database access.
- No outcome-evidence read occurs until the CLOB convention is frozen and
  hashed.
- No outcome-label aggregation. Fixed QA code may read settlement evidence
  across the assigned historical periods only for row-level
  internal-consistency checks
  (winner/final-price sign), never for a rate, risk, feature choice, or model.
- Reserved-tail rows are unavailable even to QA and duplicate-event loaders;
  only their manifest bytes, counts, and hashes may be integrity-checked
  without decoding row contents.
- Do not freeze the duplicate policy here — that belongs to the Stage 3
  gate, on training data.

## Needs the owner

- Nothing, unless QA fails in a way that suggests a collector defect (then
  stop and report before anyone patches production).

## Done when

- [ ] QA artifact set in `artifacts/stage-2/` — all pass or escalated.
- [ ] Pre-label CLOB convention frozen, hashed, and referenced from
      `DECISIONS.md` before outcome-evidence QA began.
- [ ] Timestamp dictionary and `asof.py` committed, hashed as one frozen
      availability bundle, and referenced from `DECISIONS.md`.
- [ ] `asof.py` implemented; observable vs provider modes tested.
- [ ] Mutation harness merged and green on sample features.
- [ ] Duplicate-event audit artifact + sensitivity harness ready for
      Stage 3.
