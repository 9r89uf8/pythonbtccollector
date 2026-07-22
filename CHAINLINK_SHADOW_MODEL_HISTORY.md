# Chainlink shadow model decision history

## Current decision

The active shadow primary is:

```text
model_version: catchup_ratio_l3000_b100
lag_ms:        3000
horizon_ms:    3000
beta:          1
```

It remains the best-supported production choice. This is a narrower statement
than saying that 3,000 ms is universally optimal: 3,000 ms beat 3,500 ms in the
accepted experiment, while the later attempt to compare 2,500 ms did not
produce production-eligible evidence.

The worker discovers this primary from the accepted decision artifact. It does
not dynamically choose a model from recent results.

## Model calculation

The model transfers a causally observed futures return to the latest Chainlink
anchor:

```text
futures_return = futures_now / futures_reference - 1
projection     = chainlink_anchor * (1 + beta * futures_return)
```

For the current model, `beta=1`. The futures reference is the newest eligible
observation at or before the Chainlink anchor's receive time minus 3,000 ms,
subject to the frozen artifact's freshness, reference-gap, session, and causal
integrity rules. The prediction target is the latest Chainlink value causally
known at the 3,000 ms target.

## Accepted 3,000-versus-3,500-versus-4,000 selection

The accepted `chronological_holdout_v2` procedure used two older reports as
calibration and one strictly later 24-hour report as an untouched holdout:

| Role | Half-open UTC window |
|---|---|
| Calibration 1 | `2026-07-10 19:34:18.928` to `2026-07-11 19:34:18.928` |
| Calibration 2 | `2026-07-11 21:58:15.522` to `2026-07-12 21:58:15.522` |
| Holdout | `2026-07-12 22:53:25.028` to `2026-07-13 22:53:25.028` |

Candidates were compared on common causal rows. Each report had to have at
least 10,000 common scored forecasts, 50% valid coverage, 99% maturation, and
positive MAE and RMSE skill against its horizon-matched no-change baseline.
Calibration ranked candidates by MAE skill and then RMSE skill. The calibration
winner was frozen before the holdout; the holdout could pass or reject it but
could not rerank the candidates or select a runner-up.

The pooled calibration contained 184,127 common scored forecasts:

| Candidate | Rank | Model MAE | Baseline MAE | MAE skill | Model RMSE | Baseline RMSE | RMSE skill |
|---|---:|---:|---:|---:|---:|---:|---:|
| 3,000 ms | 1 | `$1.0802` | `$1.2974` | `16.74%` | `$2.1887` | `$3.1594` | `30.73%` |
| 3,500 ms | 2 | `$1.3048` | `$1.4784` | `11.75%` | `$2.6911` | `$3.4737` | `22.53%` |
| 4,000 ms | 3 | `$1.5827` | `$1.6541` | `4.32%` | `$3.2716` | `$3.7681` | `13.18%` |

Raw errors across different horizons have different target difficulty, so the
selection authority was skill against each candidate's matched no-change
baseline. On that metric, 3,000 ms ranked above 3,500 ms and 4,000 ms.

The frozen 3,000 ms winner then passed the later holdout:

| Holdout measure | Result |
|---|---:|
| Common scored forecasts | `171,669` |
| Valid coverage | `99.4232%` |
| Maturation coverage | `100%` |
| Model MAE / no-change MAE | `$2.2647` / `$3.1546` |
| MAE skill | `28.21%` |
| Model RMSE / no-change RMSE | `$4.0080` / `$6.1525` |
| RMSE skill | `34.86%` |

That calibration win followed by a successful untouched holdout is the valid
authority for the current 3,000 ms primary. The accepted test is also why
3,500 ms was not selected.

## Why 2,500 ms was not adopted

The accepted selection above did not include 2,500 ms. A later July experiment
attempted a five-candidate comparison at 1,500, 2,000, 2,500, 3,000, and
3,500 ms, but it did not yield a valid production decision.

The original immutable result was:

```text
path:   /var/lib/price-collector/shadow-lag-test-20260719-20260721.json
sha256: 2e715151b011dc051f0064490ad1c5a29c319f6aa054bc71edbee7cdf4251f5a
status: insufficient_evidence
reason: calibration_replay_no_eligible_segments
```

The strict integrity policy excluded every Chainlink calibration session
because normal RTDS startup frames had been counted as parse errors. That
artifact is a failed result and must not be overwritten or reinterpreted.

A separately named post-hoc recovery admitted the known startup-frame counts
and excluded one different count-three session. It successfully replayed the
retained rows, but it was deliberately marked `descriptive_only`,
`eligible_for_production_promotion=false`, and `insufficient_evidence`. Its
all-five-candidate common cohort had only 7.54565% valid coverage against the
50% gate. The holdout decision therefore remained `null`.

The recovered calibration diagnostics were:

| Lag | Individual MAE skill | Common-cohort MAE skill |
|---|---:|---:|
| 1,500 ms | `-45.57%` | `-40.45%` |
| 2,000 ms | `-6.52%` | `1.46%` |
| 2,500 ms | `11.26%` | `20.19%` |
| 3,000 ms | `15.95%` | `19.57%` |
| 3,500 ms | `10.88%` | `7.20%` |

All common figures used only 13,029 scored forecasts from the low-coverage
all-five intersection. Within that intersection, 2,500 ms led 3,000 ms by only
about 0.63 percentage points of MAE skill. On the larger individual cohorts,
the ordering reversed and 3,000 ms led by about 4.69 points. Because coverage
failed, the evidence was post-hoc, and the rankings disagreed, this run could
not authorize a change to 2,500 ms.

The accurate conclusion is therefore:

- 3,000 ms won the only accepted calibration and passed its untouched holdout.
- 3,500 ms lost to 3,000 ms in that accepted calibration.
- 2,500 ms has not passed a valid accepted head-to-head test; its inconclusive
  July result did not displace the frozen 3,000 ms incumbent.

## Preserved runtime evidence

These two repository files are the immutable decision pair used to restore or
verify the live worker, not disposable market-test downloads:

| File | SHA-256 |
|---|---|
| `selection-1783983205028-890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699.json` | `890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699` |
| `replay-config-1783983205028-e11377f4f4cb0a6bfc91a682347c77d67ed1d81a83d03b798ff1d963fed6b5e9.json` | `e11377f4f4cb0a6bfc91a682347c77d67ed1d81a83d03b798ff1d963fed6b5e9` |

Production uses root-owned, read-only copies under
`/var/lib/price-collector/shadow-decisions`. Both are required: the worker
validates the selection hash, replay-report provenance, configuration, model
family, and frozen primary before publishing.

The accepted v2 evidence preserves its original timing semantics. Any future
replacement model requires a newly versioned causal policy, fresh calibration,
and a genuinely later untouched holdout; it must not reuse the failed July
artifact as production evidence.
