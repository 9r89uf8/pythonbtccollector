# Retention-plan peer review: verified findings and corrections

The peer's accuracy table and quarter-hour ratios reproduce from the immutable
canary export. Most monitoring recommendations are useful. The retention period
remains the owner's **seven-day target**, pending measurement of the actual
compact PostgreSQL schema. This review does not reduce it to four days, enable
monitoring, change a production procedure or deploy code.

## Accuracy verification

The independent [checker](../../../research/spot_twap_response/retention_plan_review/metrics_check.py)
uses standard-library Decimal arithmetic, imports neither the engine nor the
historical scorer, and verifies the entire 23,411-row export's SHA-256 and every
row's frozen/state hashes. All 36 reference cohort/count/better/equal/worse
comparisons pass. It rechecks exact target identity, original attempted-payload
membership, clock order, matching window and confirmed publication lead. It does
not redo the sixty-slot arithmetic already validated in the historical study.

The peer table uses decisions with a complete 120-second observation window and
strictly early acknowledged publication:

| Horizon | Pairs | Signed mean USD | Ghost MAE bps | Held-TWAP MAE bps | Ratio | Ghost strictly better |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 s | 6,326 | -0.014089 | 0.012208 | 0.055707 | 0.219153 | 81.5207% |
| 5 s | 6,650 | -0.011202 | 0.017141 | 0.274576 | 0.062427 | 97.6241% |
| 10 s | 6,658 | -0.008232 | 0.062845 | 0.543597 | 0.115610 | 93.8871% |
| 30 s | 6,695 | +0.042103 | 0.525146 | 1.543888 | 0.340145 | 81.9268% |

Overprediction shares also reproduce at the quoted rounding. The all-valid-ACK
one-second cohort has 6,345 pairs, including 19 additional late acknowledgements;
it must remain separately named. Five, ten and thirty seconds have the same
counts for those two cohorts in this hour.

Campaign-relative quarter-hour ratios reproduce: 0.055957–0.078616 at five seconds
and 0.304830–0.428466 at thirty seconds. The final quarter is shortened by the
complete-120-second cutoff. These four correlated blocks support caution, but
cannot establish that hourly alarms are generally too noisy, that 24 hours is
sufficient, or that there is no persistent bias over time. Use multiple days to
calibrate a slow deterioration flag. Exact metrics, additional cohorts and all
six horizons are in [metrics.json](metrics.json).

## Storage verification

At the measured 7,082 decisions/hour and 90,710,016 allocated bytes/hour:

- Four days projects to 679,872 decisions and 8.11 GiB in the current format.
- Seven days projects to 1,189,776 decisions and 14.19 GiB.
- The 600,000-row cap corresponds to 3.53 days before existing rows/reservations.
- The peer's assumed 2,865 logical bytes/decision gives 1.81/3.17 GiB for four/seven
  days; multiplying by two gives 3.63/6.35 GiB. The arithmetic is correct. No encoder
  was supplied with this review, so its field completeness and measured logical
  size cannot be reproduced here. The 2x allowance and claimed 3x relational
  saving are not PostgreSQL measurements.
- The four-day logical 1.81 GiB exceeds the 1.5 GiB stop, but not the nominal 2 GiB
  budget by itself. The present 256 MiB in-flight reservation leaves a 1.75 GiB
  relation allowance under that budget. Both candidate periods need measured
  compaction and revised guards; neither is established as comfortable.
- The preflight's 12.84 GiB above the 10 GiB reserve is correct and shared with
  other collection. Existing evidence allocations are already deducted from free
  disk; account for their remaining growth, not an additional whole budget that
  double-counts allocated bytes.
- Keep physical allocated-byte/free-disk guards. Deletion/vacuum may allow reuse
  without shrinking the relation; estimating only live bytes can hide real disk
  pressure. Set limits from measured steady-state allocation with safety margin.
- Four days already equals the legacy Python/SQL 96-hour threshold and therefore
  does not inherently require changing that age constant. It still requires a
  different export/deletion policy and other guard changes. Seven days requires
  explicit seven-day enforcement.

The original plan already required compact records and warned that changing the
expiry constant was insufficient. The revised plan makes the guard and shared
disk consequences more explicit. The user's seven-day target is unchanged.

## Monitoring changes accepted, with qualifications

The [updated plan](../../../GHOST_TWAP_RETENTION_MONITORING_PLAN.md) now explicitly
requires payload/frozen/state hashes, per-feed receipt-gap metrics before
compaction, read-side short-horizon usefulness, and the loss of full slot replay
when verbose inputs are removed. Hashes retain lineage and let retained browser
bytes be compared with the original payload digest; hashes do not reconstruct
deleted payloads or slot inputs.

Two factual corrections are material:

1. **Matching ends 120 seconds after decision, not 120 seconds after target.**
   `observe_target()` scores valid arriving reports within that half-open window;
   `finalize_due()` terminalizes at the decision deadline. Initial hourly summaries
   need the last decision's +120-second deadline and a caught-up persistence
   watermark. +150 seconds could be an extra operational buffer, but it is not
   implied by the thirty-second forecast horizon. Later annotations need versioned
   corrections even after initial terminalization.
2. **Local holes are not proof of never-published reports.** The saved union has
   56 unobserved stamps across 38 bracketed gap intervals. Of those, 46 stamps in
   34 intervals belong to fully-aged missing targets; the other ten belong to the
   earlier-hole shutdown cohort. The peer's “38 holes, 46 skipped stamps” mixes
   those sets. The three major pauses have source-bracket spans of seven/eight
   seconds; receipt gaps are a separate clock measurement. Use explicit cohort
   and clock labels, and describe missing targets as unobserved locally.

The 1,427 one-second cache bins with an already-received target reproduce from
the existing observer join. This qualifies short-horizon read-time usefulness;
it does not invalidate forecast accuracy or established publication lead.

## Checks and current deployment

- Archive subset rerun: **41 passed, 3 PostgreSQL cases skipped** in the local
  repository environment. All five source hashes in the archive validation record
  match current files. The previously recorded full suite and isolated 145-test
  run were not repeated for this documentation/research review.
- Matching-boundary regressions: **4 passed**, including receipt exactly at the
  120-second deadline and callback-order independence.
- Read-only droplet check: checkout remains
  `5d6083c5f8eab4fdf6603f157b2dce20af18f3ea`; Chainlink/API services are active,
  `GHOST_TWAP_ENABLED=false` is configured, and the ghost Redis key is absent.
- At review start, local `codex/ghost-audit-storage` was three commits ahead of
  `origin/main` (`fa23166`, `3a366fe`, `74b0d9a`). No production code, schema,
  environment, data or services were changed during this review.

To reproduce the independent metric check, choose a new output filename:

```powershell
python research/spot_twap_response/retention_plan_review/metrics_check.py --input dist/ghost-reliability-canary/audit.jsonl --reference results/spot_twap_response/2026-09-15-reliability-canary/audit_analysis/summary.json --output dist/retention-plan-review-metrics.json
```
