# Bounded compact PostgreSQL storage experiment

This is an isolated storage experiment, authorized separately from production
enablement. It compares two representations of finalized ghost decisions and
measures allocation, update churn and space reuse. It does not change the live
audit schema, retention policy, forecast calculation, services or producer flag.
External archival remains optional for the owner's seven-day policy. Production
compaction and expiry are separate from this measurement checkpoint.

## Frozen input and meaning of the compact record

Use the verified reliability-canary export with SHA-256
`f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954`
(23,411 full-export rows, 1,078,545,445 bytes), selecting only run
`fe2dd06da5754c50b696cb9419ca7251`: **7,082 decisions**, including its seven
unpublished decisions. Validate the complete export's checksum, count and row
hashes before yielding selected records. Keep the original export unchanged.

The encoder requires terminal evidence and an explicit finalized-as-of cutoff
at least 120 seconds after each decision. Preserve early-shutdown
`restart_unmatched`, other missingness, conflicts and uncertain publication
states; terminal is not synonymous with a matched forecast. Prices are exact
canonical E18 decimal strings; typed storage uses `NUMERIC(38,18)`. Clocks stay
integer nanoseconds/milliseconds as declared. No financial float conversion.

Retain immutable source frozen/state hashes, state version, original attempted
payload byte hash, identity, clocks, all six horizon summaries and a separate
compact-record hash. The compact record omits the 60-slot inputs and original
attempted payload bytes. Consequently it cannot independently reproduce the
forecast arithmetic, prove complete feed-event history or recover every
interarrival interval after the original evidence is discarded. This experiment
does not discard that original evidence or call those weaker records equivalent
to the current full audit.

## Isolation and hard bounds

- Root owns one explicitly named disposable database with prefix
  `ghost_compact_storage_validation_`. Verify its exact name before creation,
  use or cleanup. Never apply experiment DDL/DML to `price_collector`.
- Use ordinary **LOGGED** relations and the actual proposed keys/indexes in
  both layouts. Do not claim savings from unlogged tables or missing indexes.
- Run layouts and mutation phases sequentially. The aggregate allocation of
  **every experimental relation across both layouts**, including heap, TOAST,
  indexes and auxiliary relations, must remain below **2 GiB**. The runner uses
  the stricter whole-disposable-database size and reserves **64 MiB** below
  that ceiling before admitting more work; neither layout has a separate cap.
- Use bounded batches of at most **500 decisions**; any associated six-horizon
  child batch is separately bounded to at most 3,000 rows. Record physical row
  counts and the exact transaction/statement batch sizes used by the runner.
- Set statement timeout to **20 seconds**, lock timeout to **1 second** and a
  **20-minute total experiment deadline**. PostgreSQL statements/maintenance
  also need bounded client deadlines; a cancelled operation is not a completed
  measurement. Do not automatically extend the limits or retry a large phase.
- Require at least **11 GiB available** on the actual PostgreSQL filesystem:
  the existing 10 GiB floor plus a 1 GiB experiment reserve. Check before each
  batch, update phase, turnover and vacuum, and stop if either size/free-space
  guard fails. Relation caps do not bound WAL or concurrent core growth, so the
  filesystem guard is separate. A point check alone cannot guarantee no
  overshoot; the fixed small batches bound new work between checks.
- No collector/API restart, new feed, producer enablement, production deletion,
  schema modification, `VACUUM FULL` or index-rebuild shortcut. Cleanup removes
  only the exact disposable database created by this experiment, after saving
  results and verifying that identity; failed cleanup is reported explicitly.

## Population and sequence

1. Decode/validate the immutable input and produce one compact record for each
   selected decision. Independently test E18 precision, clock types, nulls,
   missing/conflicting targets, unpublished records and exact compact-hash
   round trips for both layouts.
2. Insert seven replicas, with snapshots after **one, three and seven copies**.
   Per layout the counts are respectively 7,082 / 21,246 / **49,574 decisions**
   and 42,492 / 127,476 / **297,444 horizon summaries**. JSON retains horizons
   inside its decision record; typed storage has six child rows per decision.
   Count the physical relations of both layouts toward the shared cap.
3. Keep `copy_id`, synthetic `retention_created_ms`, `probe_revision`,
   `summarized_revision` and lab terminal flags separate from the unchanged
   compact identity/hash and original timestamps. Day-spaced retention labels
   are artificial lab dates; do not shift original receipts or target clocks.
4. Apply **six revision-update rounds to one copy only**, measuring before and
   after. Preserve all decoded financial/clock evidence and compact hashes.
   The JSON stress rewrites full record TEXT (a trailing-whitespace marker may
   force a physical TOAST rewrite without changing decoded JSON); typed parent
   and horizon children receive only declared lab revision changes. This is
   an explicit storage/MVCC stress, not a demonstrated production write pattern.
5. Perform **three turnover cycles**: remove only the oldest synthetic block
   eligible under the lab's seven-day cutoff, run ordinary `VACUUM`/`ANALYZE`,
   insert one new replica and vacuum again. Verify exactly one block removed/added, unchanged
   retained compact hashes and six children per typed decision. Retained
   population stays seven copies. Record pre-delete, post-delete, post-insert
   and post-vacuum allocation; deletion and ordinary vacuum need not return
   allocated files to the operating system.
6. Save schema/encoder/runner versions, input/output hashes, timings, row counts,
   relation-size breakdowns, measured filesystem headroom and any early stop.
   Recheck production services, disabled producer and disposable cleanup.

## Measurement and interpretation

Report `pg_total_relation_size`, table/TOAST and index allocation separately,
plus whole disposable-database size, available filesystem bytes and elapsed
batch/phase time. Any live/dead tuple estimates are labeled estimates; verify
logical experiment counts explicitly. Do not sum a parent table's inclusive
TOAST allocation and that same TOAST relation again. Ordinary-vacuum reuse is
different from filesystem reclamation. Record failures and guard-triggered
partial results instead of extrapolating them as successful completion.

Seven copies of one observed hour are **seven replicated samples**, not seven
days of live operation or independent price paths. At this sample's rate,
seven full days would be 168 observed hours, not seven; projections must state
their rate and divide total allocation by the actual logical population.
Report insert-only, revision-stressed and turnover allocations separately.
Repeated payloads, TOAST compression, block rounding, index growth and concurrent
core collection limit extrapolation. These measurements cannot approve an
unbounded runtime, complete retention/export policy or reduced evidence tier.

## Read-only capacity preflight

[preflight.json](../../../results/spot_twap_response/2026-09-16-compact-storage/preflight.json)
records the exact metadata-only SQL/probe and source hash. It used a read-only
transaction, five-second statement timeout and one-second lock timeout; no
production data rows were queried or mutated. Production HEAD was
`5d6083c5f8eab4fdf6603f157b2dce20af18f3ea`, all six services were active,
the producer was disabled in the file and running process, and its Redis key
was absent. PostgreSQL's filesystem is `/var/lib/postgresql/16/main` with
**24,251,863,040 available bytes** at the point check.

| Existing allocation | Bytes |
| --- | ---: |
| Whole `price_collector` database | 13,544,225,815 |
| Core public relations excluding the named optional/ghost tables | 10,250,928,128 |
| Ghost audit, heap/TOAST/indexes | 317,726,720 |
| Polymarket evidence payloads | 48,644,096 |
| Polymarket market observations | 179,748,864 |
| Polymarket quote observations | 635,125,760 |
| Binance microstructure | 2,101,157,888 |
| Entire `raw_capture` schema | 16,384 |

Configured evidence and microstructure guards each warn at 4,096 MiB and cap
at 6,144 MiB; microstructure retention is 30 days. Raw capture is disabled, with
2,048 MiB/72-hour settings. Ghost fixed code guards warn at 1 GiB, stop at
1.5 GiB and budget 2 GiB, with a 600,000-row ceiling and a 10 GiB disk floor.
These are separate configured limits, not reserved filesystem capacity or proof
of sustained retention enforcement. Live core collectors continue during the
experiment, so the preflight does not substitute for repeated guards.
