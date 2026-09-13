# Rolling ghost TWAP: section 18 reproduction

The supplied section 18 **reproduces exactly**, including all three cohort sizes, three-decimal error quantiles and side counts. The original aggregate ran unchanged, with a **20-second statement timeout**, in a read-only repeatable-read transaction. Total elapsed time was **26.125 seconds including SSH, setup and the separate point-lookup coverage query**. The first attempt succeeded; no split or longer-timeout retry was needed.

## Exact results

Errors are absolute basis points relative to the actual TWAP stamped `w+h`. Persistence predicts that this future TWAP equals the current TWAP stamped `w`.

| Horizon | Selected anchors | Ghost median | Ghost p90 | Ghost p99 | Persistence median | Persistence p90 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 seconds | 9,199 | 0.006 | 0.058 | 0.194 | 0.155 | 0.561 |
| 10 seconds | 9,172 | 0.031 | 0.180 | 0.448 | 0.307 | 1.093 |
| 30 seconds | 9,135 | 0.311 | 1.274 | 3.095 | 0.868 | 3.083 |

Here a side change means current and future TWAP are on different sides of the target market's reconciled Price to Beat. Equality is assigned to the `>=` side, as in the original SQL.

| Horizon | Actual side changes | Ghost identifies new side | Ghost false alarms | Ghost wrong-side total |
| ---: | ---: | ---: | ---: | ---: |
| 5 seconds | 1,052 | 1,021 | 24 | 55 |
| 10 seconds | 1,158 | 1,071 | 74 | 161 |
| 30 seconds | 1,564 | 1,239 | 225 | 550 |

False alarms occur where current and future TWAP remain on the same side but the ghost predicts the other side. Missed changes plus false alarms reconcile to the total wrong-side count: `31+24=55`, `87+74=161`, and `325+225=550`.

## Coverage

The original generates **10,081 candidate minute anchors per horizon**, from September 1 00:00 UTC through September 8 00:00 UTC **inclusive**. This includes one ending boundary anchor whose forecasts extend beyond the nominal pilot week.

| Horizon | Missing exact current TWAP | Missing exact future TWAP | Union excluded | Selected |
| ---: | ---: | ---: | ---: | ---: |
| 5 seconds | 461 | 462 | 882 | 9,199 |
| 10 seconds | 461 | 468 | 909 | 9,172 |
| 30 seconds | 461 | 507 | 946 | 9,135 |

Current/future missing counts overlap. No candidate lacks a target resolution record, resolved status or opening price. The joint current/future-target availability counts exactly equal the final cohorts, so the spot-completeness filter causes no additional exclusions among those jointly available cases. Cohorts differ across horizons.

## What this establishes

The discrete `a=-3` projection algebra is consistent: the target slots are `[w+h−62, w+h−3]`; source stamps through `w` occupy `63−h` slots, leaving `h−3` slots for the flat continuation assumption. For horizons 5, 10 and 30 seconds, these splits are respectively **58+2**, **53+7**, and **33+27**.

Each historical slot uses the latest retained spot source second at or before that slot, with strict source-age lookback below 600 seconds. Missing prices remain null, and the complete-window/current-spot checks exclude them. This is a carry-forward reconstruction, not evidence that an unobserved underlying price was unchanged.

This remains a **retrospective source-clock benchmark**. The anchor `w` is a fixed minute-grid source stamp with an exact retained current TWAP row, not a reconstructed local decision time at which `w` was the latest received TWAP. There are no receipt cutoffs, and spot same-second upserts can erase earlier states. Reconciled opening prices are used without establishing their decision-time availability.

The side target is the **future TWAP's side of the strike**, not the eventual official market winner. These results do not establish a live forecast's performance or a tradable opportunity. A receipt-clock version would require the separately declared causal input and opening-reference conventions.

## Artifacts

- [section18_original.sql](section18_original.sql) preserves the supplied section.
- [attempt1_query.sql](attempt1_query.sql) contains that unchanged aggregate plus the bounded read-only preamble and separate coverage query.
- [summary.csv](summary.csv) contains all three reproduced rows.
- [attempt1_stdout.txt](attempt1_stdout.txt), [attempt1_stderr.txt](attempt1_stderr.txt), and [attempt1_manifest.json](attempt1_manifest.json) preserve the execution, coverage, snapshot and hashes.
- [run_review.py](run_review.py) reproduces this audit and refuses to overwrite existing artifacts.

The original PostgreSQL NUMERIC financial calculations and `percentile_cont` approximation on finalized dimensionless basis-point errors were preserved to reproduce the original three-decimal output. No other pilot sections or whole-history calculations were run, and no production settings, services, schema, or supplied files were changed.
