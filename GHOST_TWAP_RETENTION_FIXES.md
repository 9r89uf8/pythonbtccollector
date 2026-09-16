# Ghost retention maintenance fixes

This follow-up prevents a failing compact-record conversion from repeatedly
blocking later maintenance, and indexes the existing completeness watermark.
It preserves seven-day individual retention, 90-day aggregate retention, the
6 GiB budget, 5.5 GiB admission pause, 5 GiB warning, 1,500,000-decision cap and
10 GiB database-filesystem floor. It does not activate forecasts or reduce
another collector's allocation.

## Maintenance behavior

- Expire already eligible records first, then select at most 100 compaction
  candidates. Compaction has a cooperative three-second cycle budget; existing
  statement/lock limits and cancellation still apply.
- Each candidate has its own transaction. Python value/type/key/arithmetic
  failures and PostgreSQL validation/data/constraint failures (`P0001`, `22*`,
  `23*`) roll back that row and retain its verbose evidence. Connection, schema
  and time-budget errors propagate as global maintenance failures.
- A cursor ordered by creation time, run and decision advances across cycles
  and wraps at the end. This permits later rows to progress even when the
  bounded failure map is full. Failed identities wait 60 monotonic seconds before
  retry; at most 128 are tracked. Tracking evictions are reported, and only the
  first ten identity/error-class samples are exposed. Exception messages are
  excluded. Retry scheduling is in memory; restart does not delete or repair
  failed database records.
- Runtime-owned rows remain excluded. Compact evidence and hourly contributions
  commit atomically before verbose deletion; a failed row is never treated as
  summarized. Monitor health remains unavailable while failures are tracked.
- The oldest active continuous row still bounds accuracy completeness, including
  failed terminal rows and pending rows. The new partial index includes both;
  the watermark query and matching-window rule are unchanged. Later successful
  compaction is not permission to declare the failed row's hour complete.

These changes improve maintenance progress. They do not automatically correct
malformed evidence; an unresolved row can continue to block newer complete
accuracy windows while other eligible retention work proceeds.

## Current shared-disk budget

Read-only snapshot: **2026-09-16 14:35:07 UTC**. Existing allocations are already
included in filesystem usage; subtract only each other collector's remaining
allowance when estimating future growth.

| Quantity | Bytes |
| --- | ---: |
| Actual database-filesystem free space | 23,556,210,688 |
| Required free-space floor | 10,737,418,240 |
| Existing ghost allocation | 318,398,464 |
| Existing compact Polymarket evidence allocation | 935,739,392 |
| Evidence configured cap | 6,442,450,944 |
| Evidence remaining allowance | 5,506,711,552 |
| Existing microstructure allocation | 2,135,588,864 |
| Microstructure configured cap | 6,442,450,944 |
| Microstructure remaining allowance | 4,306,862,080 |
| Residual additional growth before margin/core growth | **3,005,218,816** |

The residual is **2.798828125 GiB**. It is conditional shared headroom, not a
reservation for ghost. Existing legacy ghost evidence remains preserved; this
fix does not authorize reclaiming its 318,398,464 bytes. The measured JSON layout
projects to **4,705,615,872 bytes (4.382446289 GiB) for a new week** at the one-hour
sample's rate. That exceeds available additional growth by **1,700,397,056 bytes
(1.583618164 GiB)** before safety margins and other core growth.
Consequently this snapshot does not support a continuous seven-day
capacity claim. Seven days remains the retention rule; admission may pause first.
The logical 6 GiB budget does not create filesystem space. No cap increase,
collector reallocation or automatic deletion of legacy evidence is authorized
by this maintenance fix.

## Verification and release status

The focused maintenance/index fixtures passed: **4 tests in 0.81 seconds**.
The separately authorized isolated PostgreSQL case passed in **1.86 seconds**
against source `9f03f40`. It applied the actual schema and exercised writer-role
compaction, late annotation, expiry, progress past a deliberately failing row,
and index eligibility. The disposable database measured **9,903,127 bytes**.
Final counters were one retained failing active row, zero incomplete rows, one
healthy compact row, two hourly rows and one feed-health row. The single expired
recovery count was expected from the deliberate expired-outbox rejection check.
These retained records are validation fixtures, not production forecast data.

[validation.json](results/spot_twap_response/2026-09-16-retention-fixes/validation.json)
records the exact run and cleanup evidence. This was not a live canary, sustained
capacity run, seven-day retention observation or browser test.

The producer-disabled corrective deployment is authorized and pending final
deployment evidence; no activation is claimed. After the reviewed
release is pushed, apply `schema.sql` with its own transaction wrapper before
restarting affected services; preserve disabled producer flags and existing
state. Use the [operating procedure](OPERATIONS.md#continuous-ghost-retention-and-accuracy).
The [retention contract](GHOST_TWAP_RETENTION_MONITORING_PLAN.md) and historical
[compact-storage findings](results/spot_twap_response/2026-09-16-compact-storage/FINDINGS.md)
retain their distinct scopes.
