# Receipt-clock ghost TWAP: section 19 review

The supplied section 19 **reproduces exactly**, including every cohort count, rounded error statistic, assumed-slot statistic, target-arrival quantile and side count. The unchanged aggregate succeeded on its first attempt with a **20-second statement timeout**, taking **25.016 seconds including SSH/setup**. A separate baseline/target audit took **11.531 seconds including SSH** under the same per-statement limit. Both transactions were read-only and repeatable-read; their distinct snapshots are recorded in their manifests.

## Reproduced results

The horizon `h` advances the selected TWAP's **source stamp** from `w` to `w+h`. It is not a fixed wall-clock arrival horizon from the decision instant `tau`.

| h | N | Ghost median error, bp | Ghost p90 | Ghost p99 | Persistence median | Persistence p90 | Target median arrival after tau |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5s | 9,503 | 0.005 | 0.060 | 0.209 | 0.157 | 0.572 | 4.614s |
| 10s | 9,609 | 0.027 | 0.154 | 0.410 | 0.310 | 1.110 | 9.571s |
| 30s | 9,548 | 0.292 | 1.243 | 2.783 | 0.879 | 3.110 | 29.611s |

| h | Side changes | Ghost identifies new side | Ghost false alarms | Ghost wrong-side total | Reported tail slots, mean / max |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5s | 1,931 | 1,893 | 41 | 79 | 1.94 / 9 |
| 10s | 1,976 | 1,933 | 111 | 154 | 6.94 / 14 |
| 30s | 2,218 | 1,954 | 334 | 598 | 26.94 / 34 |

## Availability and target audit

Each horizon starts from **10,081 inclusive minute anchors**, September 1 00:00 UTC through September 8 00:00 UTC. Every anchor has a baseline under the original selection rule. Missing exact targets account for **578 / 472 / 533** exclusions at horizons 5 / 10 / 30 seconds. Target-market resolution records, resolved status and opening prices are present for all remaining cases. The audit's baseline/target cohort counts match the original full-window cohorts exactly; there are no further slot-completeness exclusions reflected in those counts.

Every graded target arrived **strictly after tau at nanosecond precision**. There are no negative arrival offsets, exact-zero offsets, or offsets that truncate to zero milliseconds. Minimum actual arrival delays are:

- h=5: **1.735933030 seconds**.
- h=10: **1.732463609 seconds**.
- h=30: **20.493746930 seconds**.

The audit found **zero duplicate target stamps, conflicting target prices, differences between minimum target price and the price attached to earliest receipt, unexpected target identities, or ties for the selected baseline receipt**. Thus independently taking `min(price)` and `min(receipt)` does not mix different target records in this pilot. That aggregation is still not a generally safe conflict-resolution rule.

The baseline is the latest receipt **within the restricted provider-stamp range `[tau−70s,tau]`**. Calling it simply the globally latest received TWAP omits this restriction. The baseline lookup excludes future-stamped and older source events before selecting the latest receipt. This audit did not search outside that source range or redefine the baseline.

The latest eligible retained spot source used by the combined slot grid was received after the selected baseline TWAP in **2,988 / 3,055 / 3,030** eligible cases. None of these selected spot source timestamps is later than tau. The forecast can therefore use information arriving between the baseline TWAP and the decision, while remaining subject to retained spot history's upsert limitation.

## Market-boundary effect in side counts

The target market differs from the market containing the baseline stamp in **1,912 / 1,930 / 1,915** eligible cases. Every target belongs to the market containing **tau**; the difference is relative to the older baseline stamp. Exact target stamps on a market boundary occur **0 / 1 / 0** times.

| h | Side changes across baseline/target market boundary | Side changes within one market | Total changes |
| ---: | ---: | ---: | ---: |
| 5s | 1,800 | 131 | 1,931 |
| 10s | 1,731 | 245 | 1,976 |
| 30s | 1,526 | 692 | 2,218 |

These counts were calculated locally from the exported baseline/target prices and strike. Most short-horizon side changes compare a pre-opening baseline price with a **new market's strike**. They should not be described as reversals of the same market's leader. The opening price is taken from later reconciliation; its decision-time availability is not established by this query. Consequently, price forecasting and a directly usable side signal are separate claims.

## Assumptions and limits

`assumed_slots = max(0, target_last_source_second − s_last)` counts the tail beyond the greatest retained source stamp in the combined slot grid. It does **not** count internal missing source seconds filled by carry-forward. A missing internal slot can use an older value while contributing zero to this reported tail count. Nor does a missing retained row establish that no report ever arrived. A full actual-carried-slot recount was deliberately not run in this bounded audit.

The original slot lookup uses the greatest retained source second at or before each slot, subject to receipt by tau and a strict source lookback below 600 seconds. This is a causal filter on retained records, but same-second spot upserts can erase earlier values that were available at tau. Carry-forward through absent source seconds and continuation beyond the latest source are estimates, not observed unchanged prices.

The side target remains the **future TWAP's side of the target strike**, not the eventual official market winner. These are exploratory results on the previously examined fixed week, not held-out performance or evidence of profitability.

## Artifacts

- [section19_original.sql](section19_original.sql), [attempt1_query.sql](attempt1_query.sql), [summary.csv](summary.csv), and [attempt1_manifest.json](attempt1_manifest.json) preserve the exact reproduction and hashes.
- [audit.sql](audit.sql) performs only baseline/target and selected-source lookups; it does not recompute forecast windows.
- [anchor_targets.csv — retained locally; provenance](audit_manifest.json) preserves all **30,243 anchor/horizon rows**, including unavailable targets, source/receipt clocks, target price/receipt variants and boundary flags.
- [audit_manifest.json](audit_manifest.json) records the audit counts, snapshot and hashes. [run_original.py](run_original.py) and [run_audit.py](run_audit.py) refuse to overwrite existing artifacts.

No supplied pilot, main protocol, runtime, schema, service or production setting was changed. No further database work is required for this review.
