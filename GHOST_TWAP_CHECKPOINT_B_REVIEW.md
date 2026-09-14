# Checkpoint B peer-review corrections

Reviewed commit: `485550e9b26b21b84d6ce5f84e9883f63e47b96b`.
The corrections were accepted as release `3e1ef50`, pushed to GitHub and installed
disabled on the droplet, verified September 14, 2026 UTC. No prospective canary
has started. [Deployment receipt](results/spot_twap_response/2026-09-14-deployment/checkpoint_b.json).
The validation manifest describes the reviewed files at `3e1ef50`, before these
deployment-status edits; its hashes can be checked against that Git revision.

## Verdict on the supplied review

The terminal-row version race, permanent handling of transient I/O failures,
campaign-file write churn, overly broad writer privileges and missing sink stop
reason were real issues. The review was also right to reject a presumed 72-hour
storage budget and identify missing integration tests.

Two predictions in the review exceed the evidence. A seven-hour storage stop is
a conditional extrapolation, not a measured limit. Likewise, no test establishes
that an I/O stop would inevitably occur within a day. These weaknesses warrant
correction without presenting those predictions as observed results. Actual
receipt/wall-clock causality regressions intentionally remain hard stops; safe
automatic recovery across a new clock epoch is outside this checkpoint.

The original local result was 1,084 passed with five opt-in PostgreSQL tests
skipped. The archived validation confirms those five ran among 208 passing ghost
tests on Python 3.12 in a disposable database, separately from production. The
review correctly distinguishes those tests from a live canary.

Current policy update: the owner subsequently shortened the first canary to
**one hour**, versioned as `ghost-canary-v3`. The four-hour decision and v2 test
evidence below describe the original peer review; all other guards, retention
and forecast calculations are unchanged.

## Storage decision

The original reviewed commit's complete-runtime probe had 64 decisions and used 1,974,272 relation bytes
after terminal updates, including indexes and TOAST. Subtracting its 49,152-byte
empty allocation gives 30,080 bytes per decision in this bounded sample.

| Conditional extrapolation at two decisions per second | Estimate |
|---|---:|
| Four hours | 826 MiB |
| Reach the 1.5 GiB stop | 7.44 hours |
| 72 hours | 14.52 GiB |

This is allocation arithmetic, including update/maintenance effects, not the
physical size of one live tuple or a proven steady storage slope. The smaller
engine-only probe had a different slope; actual event rate, compression,
autovacuum and update shape can change the result.

At this review, the first canary was shortened to **four hours for capacity measurement**
(subsequently reduced to one hour at the owner's request).
Keep all decision evidence, the 1.5 GiB stop, 2 GiB budget, bounded reservations
and 96-hour externally verified retention. It can stop earlier. Completion of
this first run would not establish multi-day or continuous operation; those need
new storage evidence and review. Nothing automatically extends the deadline,
raises the cap or deletes unexported rows.

## Corrections and validation

The reviewed runtime policy was `ghost-canary-v2`:

- **One version owner per row.** Database late-target updates exclude every
  decision still owned in memory through its terminal commit and outbox removal.
  In-flight publication also prevents eviction. Conflicting evidence still fails
  closed; the fix prevents competing version allocation rather than choosing one
  divergent result during recovery.
- **Recoverable I/O pauses.** Temporary guard, outbox and database faults suspend
  admission/publication. A fresh guard and audit catch-up are required to resume.
  Publication epochs prevent an old queued decision from being retried after
  recovery. Hard capacity/deadline/causality/integrity stops remain persisted.
- **Bounded persistence work.** Failed rows rotate fairly while unrelated rows
  continue draining. Late-target pagination retains its cursor across batch
  yields. Campaign progress is checkpointed every 30 seconds and at stop/shutdown;
  stopped loops do not keep rewriting it. Identical outbox writes are no-ops.
  Database placement metadata is cached after initialization instead of queried
  on every guard sample.
- **Operator-only maintenance.** The writer can insert evidence and update
  result state, but cannot acknowledge exports, delete/truncate rows or change
  the frozen input columns. The trigger also protects first publication clocks,
  attempted payload, terminal outcomes and first target-result evidence, while
  allowing later adverse flags and versioned suspension metadata.
- **Collector and export handling.** An optional offer failure records its stop
  reason without escaping into the feed reader. Export detects a nonprogressing
  or growing stream before it can run indefinitely. The installation command
  applies schema and trigger replacement in one transaction.

Validation of the corrected revision:

- **1,130 tests passed** locally on Python 3.9, with ten environment opt-in tests
  skipped (eight PostgreSQL and two private Redis).
- **259 ghost tests passed** on Python 3.12, including all ten opt-in cases.
  PostgreSQL used a newly created disposable database, removed afterward.
  Each Redis integration test launched its own temporary Unix-socket server,
  with TCP/persistence disabled, and terminated that child afterward. Production
  data, Redis keys, settings and services were not changed.
- Regressions cover the former competing version allocations, actual restricted
  writer operations and denials, direct-SQL evidence protection, started worker
  loops, real Redis byte equality/expiry, pause/resumption, deadline boundaries,
  in-flight crash/acknowledgement handling, fair retries, pagination progress and
  server export failures.
- The repeated 64-decision PostgreSQL probe allocated 1,933,312 terminal relation
  bytes from a 49,152-byte baseline, versus the original 1,974,272 bytes. This is
  another bounded allocation observation, not a revised promise of sustained
  capacity. The largest serialized outbox record was 44,992 bytes. All 64 full
  runtime rows and all 128 engine-shaped rows were exported, verified and expired.
  Live production publication latency and multi-day capacity remain unmeasured.

The [validation manifest](results/spot_twap_response/2026-09-13-checkpoint-b-review/validation.json)
contains exact source/artifact hashes and links the captured test receipts.
The accepted pure A engine, forecast arithmetic, slot categories, quality policy
and recorded replay fixture remain unchanged. Original evidence stays under
`results/spot_twap_response/2026-09-13-checkpoint-b/`; new validation is recorded
separately so old hashes never appear to validate changed code.

See [the B report](GHOST_TWAP_CHECKPOINT_B.md),
[live plan](GHOST_TWAP_LIVE_PLAN.md) and
[schema-before-restart handoff](OPERATIONS.md#ghost-twap-checkpoint-b).
