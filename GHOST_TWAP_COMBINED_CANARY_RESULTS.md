# Ghost TWAP: combined one-hour canary results

**The combined changes passed this bounded canary.** Frozen forecast arithmetic,
attempted publication membership and lead credit passed the full audit. A separate
implementation reproduced the main price/lead metrics exactly. Every observed
qualified Redis payload matched the verified campaign audit. Ghost is disabled
again; this report is the official record of the combined run.

The combined freshness and per-horizon publication policies ran for the fixed
hour. After the declared 65-second warm-up, the Redis key was present in
**99.632%** of samples; usable 5/10/30-second forecasts were present in
**98.003% / 98.156% / 98.727%**. Key presence alone would overstate forecast
availability. A spot-feed disconnect triggered history rebuilding and the longest
five-second unavailability sequence spanned approximately 59 seconds of probes.
Price forecasts remained useful, with median 5/10/30-second errors of about
**$0.026 / $0.173 / $2.04** and confirmed Redis lead of
**4.662 / 9.658 / 29.657 seconds**. Publication latency was **38.22 ms median**:
the under-10-ms objective remains unmet.

## Run and completed shutdown

- UTC interval: **2026-09-14 22:34:04.002–23:34:04.002**; Chicago **5:34–6:34 PM CDT**.
- Deployed code: `770df37cc0f0dbdea1138141ee67746356e9563a`; runtime
  `ghost-canary-v5`, calculation contract 3. Source freshness remained 5,000 ms,
  wall/monotonic receipt freshness 3,000 ms, and historical carry limit 10 seconds.
- Run: `9b3cf7d21f9640de989062ddd4001d5b`. The observer was ready before activation.
  The ghost runtime started at 22:34:06.752413 UTC, within the fixed interval.
- `canary_deadline` stopped decisions at **23:34:04.033874 UTC**, about 31.9 ms
  after the deadline. This intentional ERROR-level stop is not a worker failure.
- After the full matching window and audit drainage, ghost was disabled and only
  the Chainlink collector restarted. Shutdown verification completed at
  **23:37:17.402 UTC**. Both outboxes were empty, the ghost key absent, all six
  services active, and local API/database health passed. Both campaign directories
  remain; the first campaign's state hash is unchanged. No evidence was deleted.

The new campaign contains **7,054 terminal decisions**. The combined export
contains **14,346**, including the previous 7,292. All current rows received
external verification and an **export acknowledgement**, with **zero
stale/ineligible export acknowledgements**.
[Launch](results/spot_twap_response/2026-09-14-combined-canary/LAUNCH.json),
[shutdown](results/spot_twap_response/2026-09-14-combined-canary/FINISH.json),
and [export acknowledgement](results/spot_twap_response/2026-09-14-combined-canary/EXPORT_ACKNOWLEDGEMENT.json)
record these checks.

## Actual sampled cache coverage

The observer used a persistent loopback Redis connection and an atomic GET/PTTL
transaction at each fixed 100 ms bin. **All 36,000 bins contained a recorded read**:
there were no missed/error/unrecorded bins, invalid payloads, clock anomalies,
expired-at-read classifications or ambiguous expiry boundaries. All reads stayed
inside their assigned bin. The post-warm-up denominator is the predeclared
**35,350 bins**, beginning at 22:35:09.002 UTC; it includes absences and health
payloads with unavailable forecasts.

| Measure | Full hour | After 65 seconds |
|---|---:|---:|
| Key present | 35,830 / 36,000 (99.528%) | 35,220 / 35,350 (99.632%) |
| +1 s usable | 34,527 (95.908%) | 34,524 (97.663%) |
| +2 s usable | 34,630 (96.194%) | 34,614 (97.918%) |
| +3 s usable | 34,645 (96.236%) | 34,623 (97.943%) |
| +5 s usable | 34,686 (96.350%) | 34,644 (98.003%) |
| +10 s usable | 34,794 (96.650%) | 34,698 (98.156%) |
| +30 s usable | 35,191 (97.753%) | 34,900 (98.727%) |

Here, usable means an eligible forecast with valid payload/input freshness at
the end of the read. It does not prove that its official target was still
unreceived then. These fractions describe sampled local cache states, not exact
continuous uptime, browser delivery or trade execution. The source-horizon label
is not a guaranteed wall-clock lead.

The first canary had no equivalent direct cache observer. Its decision-level
unavailability and inferred expiry gaps cannot serve as a like-for-like measured
coverage baseline. This run establishes the combined policy's measured coverage;
it does not separately estimate either change's causal contribution.

[Observer review](results/spot_twap_response/2026-09-14-combined-canary/OBSERVER_REVIEW.md)
and [minute-level analysis](results/spot_twap_response/2026-09-14-combined-canary/observer_analysis.json)
give all counts and denominators.

## Why the remaining gap matters for the frontend

After warm-up, the five-second forecast was unavailable in **706 probes**:
130 had no key, while 576 had a valid key containing `missing_slots` for that
horizon. The ten- and thirty-second equivalents were 522 and 320 present-but-
unavailable probes, in addition to the same 130 absent-key probes.

These `missing_slots` cases followed the recorded spot connection loss at
**22:47:37.919940 UTC**. The reconnect was scheduled after 0.854 seconds. Its
explicit `connection_end` marker cleared spot history. Fresh current values
returned before enough historical slots existed for each forecast. This was
history rebuilding after a gap, not evidence that ten-second carry was exceeded.

| Horizon | Longest consecutive unusable probes after warm-up | First–last scheduled probe UTC |
|---|---:|---|
| +5 s | 588 | 22:47:39.402–22:48:38.102 |
| +10 s | 534 | 22:47:39.402–22:48:32.702 |
| +30 s | 332 | 22:47:39.402–22:48:12.502 |

Those sequences began with 12 absent-key probes, followed by a present health
payload with the horizon unavailable. The elapsed probe spans are descriptive;
they are not exact continuous outage durations. Longer horizons needed fewer
old slots and recovered sooner.

There were six sampled absence episodes including startup. The longest key-only
absence sequence was 56 probes from 23:12:08.002 through 23:12:13.502 UTC. A
separate TWAP connection loss was recorded at 23:03:19.970949 UTC, followed by
automatic reconnection. ConnectionClosedError identifies a connection loss; these
records do not establish its network cause.

A frontend must honor each horizon's null price, quality and reasons even when
the key exists. It should show an unavailable/rebuilding state during recovery.
Any change to history preservation after disconnect requires a separate reviewed
policy; this canary did not change that behavior.
[Exact exclusion and streak evidence](results/spot_twap_response/2026-09-14-combined-canary/observer_gap_breakdown.json)
records the affected payloads and slot counts.

## Price accuracy and retained lead

The accuracy cohort requires an acknowledged publication, the exact horizon,
target stamp and original price in its actual eligible payload, and a clean first
official target report. Ghost and persistence use that same target. Persistence
means keeping the anchor official TWAP unchanged. Basis-point error divides
absolute error by the target price and multiplies by 10,000. These financial
quantiles use Decimal precision 80 and linear interpolation.

| Source horizon | Valid published pairs | Ghost median / p90 error (bp) | Persistence median / p90 error (bp) | Ghost median / p90 error ($) |
|---|---:|---:|---:|---:|
| +1 s | 6,385 | 0.000392 / 0.033191 | 0.023521 / 0.107404 | 0.0031 / 0.2602 |
| +2 s | 6,697 | 0.000392 / 0.033190 | 0.051512 / 0.204117 | 0.0031 / 0.2602 |
| +3 s | 6,703 | 0.000491 / 0.033191 | 0.078581 / 0.297656 | 0.0039 / 0.2602 |
| +5 s | 6,692 | 0.003372 / 0.042568 | 0.133163 / 0.497851 | 0.0265 / 0.3336 |
| +10 s | 6,700 | 0.022017 / 0.123667 | 0.261611 / 1.004151 | 0.1727 / 0.9705 |
| +30 s | 6,775 | 0.260228 / 1.056628 | 0.688524 / 2.788679 | 2.0423 / 8.2839 |

At 5/10/30 seconds, ghost beat persistence in **95.74% / 90.66% / 74.41%** of
these pairs. Its absolute errors were lower than in the first canary, but the
persistence baseline's errors were also lower. Different hours, paths and
publication selection prevent attributing that accuracy change to either policy
fix. This is not a controlled before/after experiment or a trading return.

Confirmed lead requires Redis acknowledgement strictly before the official
target's first receipt on the same monotonic clock. No component medians are
added or subtracted to produce it.

| Source horizon | Confirmed-early forecasts | Median / p90 lead (seconds) |
|---|---:|---:|
| +1 s | 6,375 | 0.637 / 1.294 |
| +2 s | 6,697 | 1.744 / 2.246 |
| +3 s | 6,703 | 2.675 / 3.287 |
| +5 s | 6,692 | 4.662 / 5.321 |
| +10 s | 6,700 | 9.658 / 10.318 |
| +30 s | 6,775 | 29.657 / 30.358 |

Ten valid matched one-second forecasts did not precede target receipt at ACK and
earn no confirmed lead. They remain in the published accuracy cohort. The
published and confirmed-early accuracy cohorts coincide at every longer horizon.
Missing target reports remain excluded: there were 101/101/102/121/134/136 among
eligible acknowledged horizons 1/2/3/5/10/30. No conflicted, future-clock or
causality-invalid targets were silently included, and no uncertain publication
received credit. Redis lead is not measured browser lead.

The [full audit summary](results/spot_twap_response/2026-09-14-combined-canary/audit_analysis/summary.json)
contains p99/extreme errors, all calculation/publication/missingness denominators,
and the separate confirmed-early panel. The
[independent check](results/spot_twap_response/2026-09-14-combined-canary/independent_check.json)
reproduced **33 main comparisons with zero differences**, including exact Decimal
5/10/30 error quantiles and median lead.

## What the batch fix recovered

**7,040 of 7,054 decisions were acknowledged**; 14 were withheld as
`expired_or_target_received`. All **6,911 decisions with at least one calculated
forecast** retained at least one acknowledged eligible horizon. The other 129
acknowledged rows were health payloads without a calculated forecast; their
presence is why ACK/key counts alone are insufficient.

There were **314 partial acknowledged batches**, withholding 311 one-second
horizons and three two-second horizons because their target had already arrived.
The remaining eligible horizons were published. This directly demonstrates the
intended per-horizon behavior: an arrived short target no longer discards the
whole batch. These are observed recoveries under the new rule, not a causal
estimate of extra wall-clock coverage against an unobserved old-policy run.

| Horizon | Calculated | Eligible in ACK payload | Valid matched | Missing target |
|---|---:|---:|---:|---:|
| +1 s | 6,797 | 6,486 | 6,385 | 101 |
| +2 s | 6,801 | 6,798 | 6,697 | 101 |
| +3 s | 6,805 | 6,805 | 6,703 | 102 |
| +5 s | 6,813 | 6,813 | 6,692 | 121 |
| +10 s | 6,834 | 6,834 | 6,700 | 134 |
| +30 s | 6,911 | 6,911 | 6,775 | 136 |

The frozen audit recorded two explicit feed gaps/resets, matching the spot and
TWAP reconnects. Input queue high-water was two; audit reservation high-water
was 244, below the 2,048/512 bounds. No input drops, worker failures, runtime
suspensions, restart recovery or uncertain publications were recorded. Frozen
running counters can lag the last row, so final totals above come from the
complete export rather than the last counter snapshot.

## Publication speed remains the next engineering issue

| Stage | Observations | Median / p90 / p99 (ms) |
|---|---:|---:|
| Latest included receipt to decision | 7,054 | 3.443 / 7.026 / 14.593 |
| Calculation | 7,054 | 0.992 / 2.238 / 5.146 |
| Calculation complete to intent recorded | 7,040 | 6.745 / 15.666 / 39.512 |
| Intent to Redis attempt | 7,040 | 23.413 / 36.904 / 76.500 |
| Redis attempt to ACK | 7,040 | 1.726 / 3.605 / 7.471 |
| Latest included receipt to Redis ACK | 7,040 | **38.221 / 59.875 / 110.250** |

The receipt endpoint is the latest included event, not every received callback.
Stage sample sizes differ, and component quantiles do not add to total quantiles.
Intent-to-attempt includes scheduling, durable outbox work and the final
eligibility check/serialization; it does not isolate fsync alone.

**Zero of 7,040 acknowledged publications met 10 ms**; the minimum was 12.503 ms
and the maximum 331.443 ms. The median is similar to the first run's 39.19 ms.
The two correctness/coverage fixes did not attempt to change the durable-before-
publication design. Profile the stage costs before choosing an optimization;
the current evidence does not justify removing durability or claiming an API
speed improvement. SSE/browser delivery still needs its own measurement.

## Observer cost and storage

The observer's local Redis read duration was **1.008 ms median, 3.110 ms p90,
5.619 ms p99**, and 24.900 ms maximum. These include client scheduling and the
local transaction; they are not API, SSE, network-to-browser or forecast
publication latency. Its quantiles use the observer's fixed lower-index rule.

Raw observer files totaled **71,779,068 bytes (68.45 MiB)**, below the fixed
128 MiB cap. They contain 6,231 distinct observed payloads. The observer used
about 1.2–1.3% CPU in the periodic process samples. This is descriptive overhead,
not a controlled effect on core-feed latency.

The audit relation, including the first campaign, was **184,844,288 bytes**
before the new export acknowledgement, up **89,440,256 bytes (85.30 MiB)** from
preflight. It remained well below the 1.5 GiB admission stop. The Chainlink
collector peaked at **97,083,392 bytes (92.59 MiB)** of cgroup memory and used
532.934 CPU seconds through the pre-disable reading. That is the whole collector,
including target drainage, not isolated ghost CPU. The longest periodic sample
showed 14.5% CPU; it is not a peak-load or long-run-capacity guarantee.

After external-export attestations, total audit allocation was **191,275,008
bytes**, with all 14,346 current rows still terminal and externally verified.
Ghost remained disabled and the local API was healthy at that follow-up check.

## Reproducibility and limits

The external audit is **656,591,966 bytes**, SHA-256
`2353132cbc689a02d6eaf3009d93c6550f9693205ee6978c70c77a3faf51fdc3`.
Raw audit and observer data remain outside Git on the owner's computer, with
the original export retained. The [observer manifest](results/spot_twap_response/2026-09-14-combined-canary/OBSERVER_MANIFEST.json)
preserves the two file hashes. The tooling passed **1,299 tests** (10 opt-in
datastore skips), plus **86 measurement-tool tests under Python 3.12**, before
launch; a separate reviewer checked the scorer, observer and join.

The [independent price checker](research/spot_twap_response/combined_canary/independent_results_check.py)
and [observer recount/gap checker](research/spot_twap_response/combined_canary/observer_results_check.py)
are inspectable and reproduce their saved outputs. The
[final manifest](results/spot_twap_response/2026-09-14-combined-canary/FINAL_MANIFEST.json)
ties the report, summaries, code and raw evidence to their hashes.

The [exact payload join](results/spot_twap_response/2026-09-14-combined-canary/coverage_audit_join.json)
matched all **6,231 observed payload identities** and **35,830 present-key reads**
to acknowledged attempts in this campaign. The audit's own statuses were
preserved; no observation was promoted from uncertainty into acknowledged lead.

One hour of overlapping forecasts is descriptive evidence, not independent
trials, long-run retention validation, a guarantee for another market regime,
settlement-side accuracy or trading edge. API/SSE and paired browser arrival
measurements remain Checkpoint C.
