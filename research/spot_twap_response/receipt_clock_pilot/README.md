# Fixed-week receipt-clock projection pilot

This pilot retains all **2,016 markets from September 1–8, 2026 UTC** and all
six requested checkpoints: **12,096 market/checkpoint rows**. At 30 seconds,
the projection makes **36 errors among 1,925 paired markets**, compared with
**125 for current TWAP** and **59 for current spot**. The prespecified
three-second freshness subset retains the improvement. These are exploratory
results on a previously examined week, not a fresh validation sample.

## Causal inputs and projection

For market end E and decision cutoff D = E − T seconds, the projected final
window has the 60 source slots **E−62 seconds through E−3 seconds inclusive**
(signed shift a = −3). Each historical slot u ≤ D selects the greatest retained
spot source timestamp ≤ u whose row was received by D and whose source is
strictly greater than u−600 seconds. A prior source fills a silent slot only
as a carry-forward estimate. Slots later than D use the selected current spot
price as a flat future estimate.

| T | Historical slots requested | Future slots requested |
|---:|---:|---:|
| 60 | 3 | 57 |
| 30 | 33 | 27 |
| 15 | 48 | 12 |
| 10 | 53 | 7 |
| 5 | 58 | 2 |
| 3 | 60 | 0 |

Current spot and TWAP are selected by **latest receipt first**, within the
strict receipt lookback `(D−600 seconds, D]`. TWAP receipt eligibility uses
the exact nanosecond cutoff. The selected value is then rejected if its source
timestamp is future-dated, its source age is at least 600 seconds, or the latest
receipt timestamp contains different source/value pairs. An older fresh value
cannot replace a newer invalid candidate. The current spot baseline is the
same value used for future extrapolation.

The opening reference K must have exactly one distinct E18 value among durable
TWAP events at the **exact market-start source timestamp** received by D.
Missing or conflicting opening values are unavailable. A later reconciled
official opening is audit-only. Official verified winners supply grading;
ties are retained separately and the applicable Up-on-equality rule is used
for the declared prediction. Final official prices are audit targets only.

The primary comparison requires available projection, causal K, current spot
and current TWAP on the **same market/checkpoint rows**. The separate
`fresh_3s` diagnostic additionally requires both current feeds' source and
receipt ages to be at most 3,000 ms, inclusive. This diagnostic did not select
the projection formula.

## Paired error counts

Primary paired n is **1,925 at every checkpoint**, with zero unknown winners.
The excluded 91 markets lack the exact stream opening reference. All 2,016
markets still remain in the row-level export at every checkpoint.

| T | Projection errors | Current TWAP errors | Current spot errors | Projection improves / worsens TWAP |
|---:|---:|---:|---:|---:|
| 60 | 156 | 240 | 156 | 131 / 47 |
| 30 | 36 | 125 | 59 | 102 / 13 |
| 15 | 12 | 62 | 77 | 57 / 7 |
| 10 | 5 | 43 | 96 | 42 / 4 |
| 5 | 2 | 23 | 112 | 22 / 1 |
| 3 | 1 | 18 | 129 | 17 / 0 |

| T | Freshness-diagnostic n | Projection errors | Current TWAP errors | Current spot errors |
|---:|---:|---:|---:|---:|
| 60 | 1,894 | 152 | 233 | 152 |
| 30 | 1,887 | 36 | 123 | 58 |
| 15 | 1,896 | 11 | 61 | 76 |
| 10 | 1,902 | 5 | 43 | 95 |
| 5 | 1,907 | 2 | 22 | 112 |
| 3 | 1,910 | 1 | 17 | 125 |

[summary.csv](results/summary.csv) contains exact Decimal accuracies and paired
agreement/disagreement counts. [daily_summary.csv](results/daily_summary.csv)
keeps the seven UTC days separate. [rows.csv — retained locally; provenance](results/manifest.json) retains all
inputs, projections, predictions, ages, eligibility flags and missingness.

All market rules were valid. No selected current source was future-dated or
receipt-ambiguous, no opening conflict occurred, and no historical slot was
unavailable under the specified carry rule. The maximum historical carry age
across checkpoints was 18 seconds. At T=30 every paired market uses at least
one carried historical slot: 56,588 historical slots are exact observations
and 6,937 are carried estimates. These counts must not be described as a
complete observed 60-second path. See [coverage_quality.json](results/coverage_quality.json).

## Execution and verification

[run_export.py](run_export.py) uses one SSH connection and one PostgreSQL
read-only repeatable-read transaction. [guard.sql](guard.sql) runs before the
42 sequential day/checkpoint statements in [extract.sql](extract.sql), followed
by COMMIT. Each statement has a **20-second timeout**, and each extraction
statement has only 288 markets with indexed per-market lookups. No timeout was
increased. The transaction completed successfully in about three minutes;
stderr was empty and the snapshot timestamp was **2026-09-13 15:06:39.309 UTC**.

The guards prove a receipt/source lag within ±600 seconds over the fixed
current-receipt interval, making the indexed current source-candidate bounds
complete. They also validate historical source keys and instrument identity.
Both bounded feed ranges contained **zero off-second source timestamps**.
They run in the same snapshot as every exported row.

[pilot.py](pilot.py) validates aggregate opening/history provenance, complete
cohort and checkpoint keys, snapshot consistency, slot accounting and the
successful extraction record before publishing accepted results. Financial
arithmetic uses 80-digit Decimal precision. The **61 focused tests** in
[test_pilot.py](test_pilot.py) cover receipt cutoffs, late arrivals, ordering,
ambiguous receipts, opening conflicts, strict per-slot lookbacks, missingness,
Decimal precision and aggregate leakage checks.

The source CSV remains local and ignored by Git. [extraction.json](extraction.json)
records its hash, SQL/runner hashes and successful process exit.
[results/manifest.json](results/manifest.json) records accepted output hashes,
the code and source hashes, and the complete recipe. The row export SHA-256 is:

```text
dd1a85fdfb171e3121de913f374582eb1cef16f1583822092b1cb8ca2433f998
```

Reproduce local tabulation into a new directory from the repository root:

```bash
python research/spot_twap_response/receipt_clock_pilot/pilot.py research/spot_twap_response/receipt_clock_pilot/observations.csv --output research/spot_twap_response/receipt_clock_pilot/reproduced_results
python -m pytest research/spot_twap_response/receipt_clock_pilot/test_pilot.py -q
```

Retained spot upserts can erase an earlier value and its first arrival. Filtering
the surviving row by receipt time cannot reconstruct that lost state. Historical
carry and future-flat prices remain estimates. The selected offset and week
have already been examined; the pilot does not establish out-of-sample
performance, executable fills, profitability, or independence of repeated
checkpoints within one market. Original leader-risk results remain separate.
