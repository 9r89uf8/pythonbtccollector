# Ghost TWAP: official one-hour canary results

**Follow-up:** the [peer-review addendum](GHOST_TWAP_CANARY_REVIEW.md) verifies the
freshness-related coverage concern and supersedes the initial work order:
review freshness/expiry and whole-batch rejection before publication profiling.
The numerical canary results below are unchanged. The original artifact hashes
refer to report revision `5f8307b`; the addendum has separate evidence hashes.

The live ghost worked as expected in this hour: it substantially improved on
holding the last official TWAP unchanged, and useful lead survived publication.
The under-10-ms receipt-to-Redis objective was **not met**. This completes the
bounded operational canary; it does not establish browser delivery performance,
long-running capacity, or a trading edge.

The authoritative numerical record is [summary.json](results/spot_twap_response/2026-09-14-live-canary/summary.json).
The independent arithmetic and integrity check [passed with zero mismatches](results/spot_twap_response/2026-09-14-live-canary/independent_check.json).

## Run and shutdown

- UTC window: **2026-09-14 00:45:20.328–01:45:20.328**.
  Chicago: **September 13, 7:45:20.328–8:45:20.328 PM CDT**.
- Production commit: `aa78346b2c8c2898a0a522798be3105552aaec7d`;
  runtime policy `ghost-canary-v3`, calculation contract 2.
- Run ID: `06f1134f06c14a94b5cab90a43b2eee6`.
- The worker latched `canary_deadline` at 01:45:20.329212 UTC, about 1.2 ms
  after the configured deadline. Its ERROR-level stop log represents the planned
  stop, not a worker failure. Redis expired normally.
- After the 120-second matching allowance, ghost was disabled and only the
  Chainlink service restarted. All **7,292 rows are terminal**, the outbox is
  drained, the ghost key is absent, the six services are active, and the local
  API/database health check passes. The fixed start and stopped campaign remain.

[Preflight](results/spot_twap_response/2026-09-14-live-canary/preflight.json),
[activation](results/spot_twap_response/2026-09-14-live-canary/activation.json),
and [shutdown evidence](results/spot_twap_response/2026-09-14-live-canary/finish.json)
preserve these checks.

## Accuracy

Each row below uses acknowledged publications with an available forecast and
a clean first report at the exact target source timestamp. Ghost and persistence
are graded against the **same target**. Persistence means keeping the anchor
official TWAP unchanged. Error is absolute price error divided by actual target
price, multiplied by 10,000. Quantiles use Decimal linear interpolation.

| Source horizon | Paired forecasts | Ghost error median / p90 / p99 (bp) | Persistence error median / p90 (bp) | Ghost median / p90 error in actual dollars |
|---|---:|---:|---:|---:|
| 1 s | 5,862 | 0.0020 / 0.0632 / 0.1836 | 0.0422 / 0.1414 | $0.016 / $0.486 |
| 2 s | 5,846 | 0.0020 / 0.0639 / 0.1844 | 0.0897 / 0.2599 | $0.016 / $0.491 |
| 3 s | 5,825 | 0.0022 / 0.0648 / 0.1804 | 0.1386 / 0.3852 | $0.017 / $0.498 |
| 5 s | 5,806 | 0.0074 / 0.0729 / 0.1876 | 0.2312 / 0.6285 | $0.057 / $0.559 |
| 10 s | 5,812 | 0.0459 / 0.1749 / 0.3734 | 0.4667 / 1.2461 | $0.352 / $1.341 |
| 30 s | 5,828 | 0.4223 / 1.3167 / 2.3533 | 1.3506 / 3.7414 | $3.239 / $10.102 |

At 5/10/30 seconds the ghost had a smaller absolute error than persistence in
**97.6% / 93.4% / 80.5%** of these paired observations. These are price-forecast
comparisons, not settlement-side accuracy or profitability.

The [development-week receipt replay](SPOT_TWAP_RESPONSE_STUDY.md) reported
5/10/30-second median errors of **0.005 / 0.027 / 0.292 bp**, with p90
**0.060 / 0.154 / 1.243 bp**. This live hour's medians were worse, while the
error scale and improvement over persistence were consistent with the earlier
result. Different dates, market paths, decision frequency, complete live arrivals,
freshness/carry policies and publication selection prevent an exact replication
claim. No universal tolerance was established by the replay.

Short-horizon medians are small, but their tails are material. A short horizon
does not guarantee exact prediction: missing historical inputs and the residual
between reconstructed and official TWAP remain relevant. The stored quality and
slot counts must accompany the prices.

## Lead actually retained after publication

Lead below is target receipt minus successful Redis acknowledgement on the same
monotonic clock. Only strict positive lead earns credit. It is not measured at a
browser and is not obtained by adding or subtracting component medians.

| Source horizon | Confirmed-early forecasts | Lead p10 / median / p90 (seconds) |
|---|---:|---:|
| 1 s | 5,849 | 0.060 / 0.688 / 1.358 |
| 2 s | 5,846 | 0.855 / 1.839 / 2.274 |
| 3 s | 5,825 | 1.999 / 2.777 / 3.343 |
| 5 s | 5,806 | 3.942 / 4.785 / 5.359 |
| 10 s | 5,812 | 8.962 / 9.771 / 10.375 |
| 30 s | 5,828 | 28.949 / 29.772 / 30.445 |

Thirteen clean matched one-second forecasts reached their targets before Redis
acknowledgement and receive **no confirmed lead**. They remain in the published
accuracy/coverage panel; the summary also contains a separate confirmed-early
accuracy panel. There were no conflicting target prices or invalid causal clocks.

The one-second horizon is particularly sensitive to frontend delivery: its
10th-percentile server lead is only 60 ms. Checkpoint C must measure delivery of
both the ghost and corresponding official report on the browser's own clock.
The h-second label remains a source-stamp offset, not an exact wall-clock lead.

The anchor-receipt-plus-h ETA remained a useful estimate: signed median ETA errors
at 5/10/30 seconds were +16 / +6 / +24 ms. Absolute p90 ETA errors were
0.612 / 0.639 / 0.736 seconds. It must remain labeled an estimate.

## Publication speed: the objective missed

Timing covers all 6,082 acknowledged decisions. The receipt endpoint is the latest
included accepted event, identified by sequence; it is not every incoming event.
All component differences use the same monotonic clock. No invalid/missing timing
case occurred in this panel.

| Stage | Median / p90 / p99 (ms) |
|---|---:|
| Latest included event receipt to decision | 2.99 / 6.19 / 12.59 |
| Calculation | 1.05 / 2.41 / 5.59 |
| Calculation complete to Redis attempt | 32.88 / 52.37 / 97.04 |
| Redis attempt to acknowledgement | 1.76 / 3.82 / 7.95 |
| Full receipt to Redis acknowledgement | **39.19 / 62.13 / 113.28** |

**Zero of 6,082 publications met 10 ms**; the minimum was 14.31 ms and maximum
477.19 ms. The largest measured stage is before the Redis call. That stage
includes serialization, durable outbox I/O and waiting; these clocks do not
isolate fsync as its sole cause. Component quantiles do not add to total quantiles.

The next speed investigation should instrument that stage and reduce its cost
while preserving reproducibility before publication. Increasing API polling
frequency would not remove this server-side delay. SSE/browser timing remains
unmeasured because B has no ghost HTTP/SSE routes.

## Coverage and operational behavior

All 7,292 issued decisions remain in the evidence, including warm-up and rejected
publication attempts. **6,082 (83.4%)** were acknowledged; 6,021 of those contained
at least one available horizon. The others were 791 `expired_or_target_received`,
418 `preempted_after_spool`, and one `expired_before_attempt`. Withheld publications
are not credited as delivered. These figures describe issued decisions, not
wall-clock availability or percentage of all feed events.

| Horizon | Available / all issued | Available / acknowledged | Clean matched / available acknowledged | Missing target among acknowledged |
|---|---:|---:|---:|---:|
| 1 s | 6,403 / 7,292 | 5,969 / 6,082 | 5,862 / 5,969 | 107 |
| 2 s | 6,404 / 7,292 | 5,970 / 6,082 | 5,846 / 5,970 | 124 |
| 3 s | 6,407 / 7,292 | 5,973 / 6,082 | 5,825 / 5,973 | 148 |
| 5 s | 6,410 / 7,292 | 5,976 / 6,082 | 5,806 / 5,976 | 170 |
| 10 s | 6,420 / 7,292 | 5,986 / 6,082 | 5,812 / 5,986 | 174 |
| 30 s | 6,455 / 7,292 | 6,021 / 6,082 | 5,828 / 6,021 | 193 |

First available publications appeared after approximately 65/63/62/60/55/35
seconds for horizons 1/2/3/5/10/30, as expected from live-only window warm-up.
Across the issued decisions, reasons included stale spot (476), stale TWAP (594),
and regressing TWAP anchors (5). Reasons overlap and must not be summed as distinct
outages. Five one-second decisions also rejected already-received targets.

The existing quality rule remained unchanged. At five seconds 4,262 available
publications were degraded by interior carry and 1,714 were healthy. Healthy can
still include pending/future assumptions. A degraded label is not by itself a
measured forecast failure.

The frozen counter high-water marks were two queued inputs and 261 reserved audit
rows, below their 2,048/512 bounds. No input drops, operational gaps, resets or
suspensions were recorded. Frozen counters can lag the final decision; the exact
decision/publication totals above come from the full export. Full-run logs show
no collector reconnect or worker failure; the startup subscription acknowledgement
was skipped as a non-price message. The official feeds were fresh at the checks.
These checks do not quantify incremental core-feed latency against a controlled
ghost-off baseline.

Audit storage was **87 MiB** after target finalization and about **91 MiB** after
external-export attestations, comfortably below the 1.5 GiB stop. No data was
expired or deleted. The Chainlink service peaked at **83.34 MiB** of cgroup memory.
It used 536.96 CPU seconds over about 63.21 minutes through the pre-shutdown
measurement, averaging **14.16% of one CPU**. This is the whole collector process,
including post-deadline drainage, not isolated ghost CPU or a peak-load measurement.
One hour does not establish a sustainable storage slope or 96-hour expiry behavior.

## Reproducibility and interpretation

The external export is **326,727,434 bytes**, SHA-256
`d7536bcafcf62170abbf9329f6b720d7df35fb710b9f79f64ec96ca2bda05b80`.
All 7,292 current row versions were acknowledged after local verification, with
zero stale/ineligible proofs. Its [manifest](results/spot_twap_response/2026-09-14-live-canary/export_manifest.json)
records the local path; the full 312 MiB export is retained outside Git.

The independent standard-library checker verified 648,899 slots, all **38,499
available 60-slot Decimal means**, 37,548 matched error pairs, 6,501 frozen/payload
comparisons and 34,966 confirmed-lead cases, with zero mismatches. This establishes
reproducibility from recorded selected inputs; it does not reconstruct rejected
events or prove the upstream feed had no unobserved messages.

Run from this checkout with the development Python:

```powershell
python research/spot_twap_response/live_canary/analyze.py C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-canary-2026-09-14/audit.jsonl results/spot_twap_response/2026-09-14-live-canary/summary.json
```

The [independent checker](research/spot_twap_response/live_canary/independent_check.py)
and [its result](results/spot_twap_response/2026-09-14-live-canary/independent_check.json)
record its own invocation and scope. Neither analysis runs on the droplet.

The 5/10/30-second published panels contain only 3,323/3,329/3,346 distinct target
stamps; multiple overlapping forecasts share outcomes. Boundary-crossing targets
remain included. This is one hour, not thousands of independent experiments, and
no confidence interval or market-wide profitability claim is inferred.

**Decision:** the calculation and bounded live operation are supported by this
canary. The follow-up review prioritizes freshness/expiry and whole-batch
rejection, followed by publication profiling without weakening durability.
Checkpoint C still needs Redis-only API/SSE implementation and measured browser
lead before frontend readiness can be accepted.
