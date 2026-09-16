# Compact ghost audit storage: measured results and decision

Completed September 16, 2026 UTC. **The bounded measurement passed; continuous
seven-day operation is not approved by this measurement.** Compact records
preserve the tested accuracy evidence, but neither measured layout fits the
existing ghost budget or the conditional shared-disk remainder with a useful
margin. No live forecast calculation, production schema, service or setting was
changed. The producer remains disabled.

## What was measured

The frozen reliability-canary export contains 23,411 audit rows. Its selected
one-hour run contains **7,082 decisions**, including 7,075 acknowledged and seven
unpublished decisions. The encoder retained all of them and all six horizons.
It preserved exact E18 prices, target observations, eligibility, clocks, quality,
missing/conflict states and lineage hashes. It omitted the full slot arrays and
original payload bytes. All 36 accuracy cohorts and 144 quarter-hour groups
reproduce exactly; this does not preserve full slot-arithmetic replay or complete
feed-gap history after verbose evidence is removed.

Seven replicas give **49,574 decisions and 297,444 horizon summaries** per
layout. These are seven copies of one observed hour, not seven days of live
traffic. At the observed rate, seven days means **1,189,776 decisions and
7,138,656 horizons**, or 24 times the lab population. Financial calculations and
projections use Decimal; prices never pass through binary floating point.

Two LOGGED layouts retained the same compact information:

- Canonical JSON TEXT with primary-key, retention and target-lookup indexes.
- Typed shared-decision and six-horizon tables, with NUMERIC(38,18), binary
  hashes, equivalent lookup capabilities and cascading child deletion.

The final probe ran repository commit `e8d0dd0` in a detached disposable checkout
and database. It completed in **874.085653942 seconds (14m 34s)** within the
20-minute deadline. Maximum observed whole-database allocation was **531,250,199
bytes (506.64 MiB)**, including both layouts, against a 2 GiB limit with 64 MiB
write reservation. Minimum observed available disk space was **22,710,419,456
bytes (21.15 GiB)**, above the 11 GiB experiment floor.

## Allocated storage, including indexes and TOAST

Totals include TOAST and indexes once. The week columns are proportional
estimates at 7,082 decisions/hour, not measured week-long storage or hard limits.

| Phase, 49,574 decisions | JSON allocated MiB | Typed allocated MiB | JSON projected week GiB | Typed projected week GiB |
| --- | ---: | ---: | ---: | ---: |
| Seven copies loaded | 164.30 | 192.38 | 3.85 | 4.51 |
| Summary markers committed, then vacuumed | 177.51 | 227.61 | 4.16 | 5.33 |
| Six revisions on one copy, then vacuumed | 298.47 | 311.88 | 7.00 | 7.31 |
| Third expiry/refill cycle | 186.98 | 235.59 | **4.38** | **5.52** |

The final refill totals are **196,067,328 bytes for JSON** and **247,037,952
bytes for typed tables**. This typed design is approximately 26% larger at that
point; the proposed assumption that typed storage would be three times smaller
is not supported. This result compares these two designs, not every possible
normalized schema or serialization.

The logical JSONL file is 53,216,127 bytes for the original hour. That size alone
does not predict PostgreSQL allocation. Likewise, the earlier verbose audit's
14.19 GiB/week illustration does not make a 4.38 GiB compact estimate safe under
the current budgets.

## Expiry, integrity and reuse

All **22 complete readbacks** passed: 155,804 compact record comparisons against
the verified input, including post-update and newly refilled copies. Typed rows
always had exactly six children per decision. The laboratory expiry rule:

- Retained a record one millisecond short of seven days.
- Deleted eligible records at exactly seven days and older.
- Protected nonterminal, unsummarized and stale-summary-version records.
- Protected records after a rolled-back summary transaction.
- Allowed deletion after committed summarization and tolerated repeated calls.

These checks validate the lab query and markers, not an installed production
retention trigger or production aggregation transaction.

Deletion alone released no allocated bytes in the three cycles. After the first
deletion, ordinary vacuum reclaimed 124,084,224 bytes for JSON and 85,131,264
bytes for typed tables. The full-population refill sequences were:

| Refill | JSON bytes | Typed bytes |
| --- | ---: | ---: |
| 1 | 191,700,992 | 246,947,840 |
| 2 | 194,166,784 | 246,988,800 |
| 3 | 196,067,328 | 247,037,952 |

Both reused substantial space. JSON still grew by approximately 2.35 MiB and
1.81 MiB on the final two refills; typed grew by 40 KiB and 48 KiB. Three cycles
do not establish an allocation plateau or unattended vacuum equilibrium.

## Capacity decision

The current **1.5 GiB admission stop, 2 GiB budget and 600,000-decision ceiling**
cannot accommodate these seven-day projections. Existing legacy ghost data
remains allocated; this experiment claims no reclaimable credit for it.

The preflight had 22.586 GiB available. After preserving the existing 10 GiB
filesystem floor and allowing only the *remaining* growth to evidence and
microstructure thresholds, the conditional remainder was 3.347 GiB. After the
experiment and verified cleanup, the corresponding current calculation is:

| Postflight capacity item | Bytes |
| --- | ---: |
| Available filesystem space | 23,234,297,856 |
| Existing filesystem floor | 10,737,418,240 |
| Evidence remaining growth to its threshold | 5,572,362,240 |
| Microstructure remaining growth to its threshold | 4,338,212,864 |
| Conditional remainder | **2,586,304,512 (2.409 GiB)** |

Already allocated data is not subtracted twice. This remainder still omits
future core history, WAL, logs, outbox and maintenance growth. Evidence's
threshold pauses high-rate quotes, not all metadata growth, so these allowances
are scenarios rather than guaranteed disk reservations. Free space after cleanup
need not equal the earlier reading while the cluster and collectors keep writing;
the capacity decision uses the current observation rather than assuming it did.

Both final-refill projections exceed even this conditional remainder, before
adding operating margin. Higher publication rates also require more space; the
observed cadence is not a maximum-rate guarantee.

**Next checkpoint:** test a leaner representation that avoids repeating shared
policy/metadata and identical target observations, while preserving every
published decision and its accuracy/lead evidence. Its savings remain unmeasured.
Alternatively, explicitly review additional capacity and revised shared budgets.
Then implement and validate seven-day expiry, idempotent accuracy aggregates and
monitoring in a separate checkpoint. External permanent archival remains optional;
the owner's seven-day target has not been shortened. Do not simply raise the caps
or enable continuous production on the strength of this experiment.

## Limitations and operational record

- Copies retain original prices, IDs and target stamps. In particular, repeated
  target keys affect GIN/B-tree cardinality and compression differently from
  seven days of advancing stamps and independent prices.
- Population snapshots include lab summary-marker writes. The field named
  `insert_only_vacuumed` includes these marker updates; it is not a pristine
  append-only measurement. Reported elapsed times are cumulative experiment time.
- Revision stress changes only lab markers: JSON rewrites a whole TEXT value;
  typed stress updates parent and all horizon markers. It is not equivalent to
  the actual frequency or shape of future financial result updates.
- Autovacuum was disabled only for the three disposable tables and their TOAST;
  prescribed ordinary vacuum was explicit. No `VACUUM FULL`, rebuild or production
  maintenance-setting change was used.
- Run `0c4f9f2` stopped on the one-second lock timeout; the subsequent diagnostic
  observed an autovacuum ANALYZE holding the incompatible table lock. Run
  `c86bd00` was stopped cleanly to fix marker queries that omitted the run-ID
  component of the existing key. Both reports and diagnoses are preserved. No
  cap was increased, index added or failed run presented as a pass.
- Final focused tests: **50 passed, one opt-in skip** locally and on production
  Python 3.12. The unchanged encoder's separate full-canary run passed all
  **14 tests**, including the skipped case (13 overlap): **51 distinct tests**.
- All three disposable databases, checkouts, uploaded seed files and temporary
  remote results were removed after copying/verifying the reports. Postflight
  confirmed no remaining lab paths/databases, clean production checkout at
  `5d6083c5f8eab4fdf6603f157b2dce20af18f3ea`, all six service PIDs unchanged and
  active, producer disabled in configuration and process, and ghost Redis key
  absent. The original local canary export remains unchanged.

## Reproducible evidence

[Protocol](../../../research/spot_twap_response/compact_storage/PROTOCOL.md),
[successful probe](probe_results.json), [allocation analysis](analysis.json),
[postflight capacity](capacity_postflight.json), [physical schema](physical_schema.json),
[input manifest](compact_input_manifest.json), [postflight](postflight.json),
[validation record](VALIDATION.md) and [artifact manifest](MANIFEST.json).

The successful raw report SHA-256 is
`9880234883b57675d2108ac0d29e5cc2bc0c98c7bcb69e716ffdf4185b874306`,
verified against the droplet copy before cleanup. The analysis script rechecks
all phase populations, 22 parity checks and retention cases before projecting.
