# Combined canary: independent observer review

The local observer completed the full interval **2026-09-14 22:34:04.002–23:34:04.002 UTC**, with one recorded read in each of the **36,000 planned 100 ms bins**. The key was present in **35,830 reads (99.528%)**. Presence did not always mean an available forecast: 639 present-key reads had no usable horizon.

This is sampled local-cache coverage. “Usable” means the horizon was included in the attempted publication, its payload and current inputs remained fresh at reply end, and the read stayed inside its planned bin. It does **not** establish that the official target was still unreceived at the read, continuous uptime between reads, forecast accuracy, or browser delivery.

## Evidence verification

Both copied files matched the operator-provided hashes and manifest sizes before analysis:

| File | Bytes | SHA-256 |
|---|---:|---|
| `payloads.jsonl` | 39,374,777 | `746dd635af3e6d13f4ab08604096327ed52b7abec771524917b3352544a4ccb3` |
| `samples.jsonl` | 32,404,291 | `45bd7dde945a412d411d84966b24e8f421ea7f68a6b41dce76afca7438534fb1` |

Source directory on the owner's computer: `dist/ghost-combined-canary-2026-09-14/observer`. The manifest reports `complete`, 36,000 sample records, zero unrecorded bins and 6,231 distinct raw payloads. The two data files total 71,779,068 bytes, below the fixed 134,217,728-byte cap.

The production observer analyzer verified file and payload hashes, reconstructed classifications from exact raw payloads plus recorded TTL/read clocks, and checked the planned grid, record counts and saved classifications. A separate direct recount of all sample records independently matched the full-hour and post-65-second status, eligibility, usability and unknown-bin totals. No source code or classifier rules changed during this review.

[Verified analyzer output](observer_analysis.json) includes the minute panels and all 6,231 observed qualified payload hashes. One provisional run ID was seen: `9b3cf7d21f9640de989062ddd4001d5b`. Qualification here means the v5 payload and decision time match the campaign window. Exact attempted-payload membership in the frozen campaign audit, acknowledgement outcome and target receipt ordering require the separate audit join; this observer-only review does not substitute for it.

## Coverage and denominators

The full-hour denominator is 36,000 planned bins. The declared post-warmup interval starts at **22:35:09.002 UTC**, exactly 65 seconds after the campaign start, and contains **35,350 bins**. Neither denominator discards absent-key reads or health payloads.

| Measure | Full hour | After 65 seconds |
|---|---:|---:|
| Key present | 35,830 / 36,000 (99.528%) | 35,220 / 35,350 (99.632%) |
| Key absent | 170 | 130 |
| At least one usable horizon | 35,191 | 34,900 |
| All six horizons usable | 34,527 | 34,524 |
| Key present but no usable horizon | 639 | 320 |
| Read errors / missed / unrecorded bins | 0 / 0 / 0 | 0 / 0 / 0 |
| Unknown bins / invalid payloads | 0 / 0 | 0 / 0 |

Eligibility and conservative read-end usability counts were equal for every horizon in this run. No read crossed its bin or campaign end, and no clock anomaly, stale-input classification, missing expiry or expiry-boundary ambiguity was recorded.

| Source horizon | Full-hour eligible = usable | Full-hour fraction | Post-65s eligible = usable | Post-65s fraction |
|---|---:|---:|---:|---:|
| +1 second | 34,527 | 95.908% | 34,524 | 97.663% |
| +2 seconds | 34,630 | 96.194% | 34,614 | 97.918% |
| +3 seconds | 34,645 | 96.236% | 34,623 | 97.943% |
| +5 seconds | 34,686 | 96.350% | 34,644 | 98.003% |
| +10 seconds | 34,794 | 96.650% | 34,698 | 98.156% |
| +30 seconds | 35,191 | 97.753% | 34,900 | 98.727% |

The horizons refer to target source timestamps relative to the current TWAP anchor. They are not guaranteed wall-clock lead times.

## Sampled absence episodes and minute panels

There were six runs of consecutive absent-key probes. The table gives the first and last **planned probe timestamps**, not exact outage start/end times. The key could change between probes; multiplying the count by 100 ms does not establish continuous downtime.

| First absent probe UTC | Last absent probe UTC | Consecutive absent probes |
|---|---|---:|
| 22:34:04.002 | 22:34:07.902 | 40 |
| 22:47:39.402 | 22:47:40.502 | 12 |
| 22:49:50.102 | 22:49:54.702 | 47 |
| 23:03:21.202 | 23:03:21.702 | 6 |
| 23:12:08.002 | 23:12:13.502 | 56 |
| 23:28:24.402 | 23:28:25.202 | 9 |

Minute panels are anchored to the campaign start, so each begins at `:04.002`, with 600 planned probes. Every minute had zero unknown bins. The panel starting 23:12:04.002 had the most key absences: 56/600. The panel starting 22:49:04.002 had 47/600.

Key absence was not the only coverage limitation. In the panel starting **22:48:04.002**, all 600 probes found a key, while +1 second was usable in only 217/600 and +30 seconds in 514/600. In the preceding panel starting 22:47:04.002, the key was present in 588/600 probes but every horizon was usable in only 354/600. These are observations of the served payloads; the observer alone does not establish the underlying feed or worker cause.

## Measured read duration

Durations use local monotonic clocks from immediately before the Redis transaction through reply completion, including client scheduling and local Redis round-trip work. They are not one-way latency, source-feed delay, forecast computation time or browser delivery time.

| Quantile | Milliseconds |
|---|---:|
| Median | 1.007645 |
| p90 | 3.110377 |
| p99 | 5.618696 |
| Maximum | 24.900199 |

The analyzer uses its fixed lower-index order-statistic convention for quantiles. No read timed out, and every recorded read completed inside its assigned 100 ms bin. The absence of sampling failures in this one-hour run does not establish continuous availability or sustained production performance.

## Why a present key could still lack a forecast

A second pass joined each post-65-second ineligible sample to its hash-verified raw payload and inspected the specific horizon's exclusion reasons. For +5/+10/+30 seconds, **every present-but-ineligible sample had `missing_slots` as its only forecast reason**. These cases were separate from the 130 absent-key samples in the same interval.

| Horizon | Present, eligible and usable | Present but ineligible (`missing_slots`) | Absent key | Total planned bins |
|---|---:|---:|---:|---:|
| +5 seconds | 34,644 | 576 | 130 | 35,350 |
| +10 seconds | 34,698 | 522 | 130 | 35,350 |
| +30 seconds | 34,900 | 320 | 130 | 35,350 |

All of these present-but-ineligible payloads retained the explicit **spot `connection_end` gap marker, after accepted sequence 1588**. The first such observed payload was decision `1592`, SHA-256 `cda5be81488dc35d6f2f2d14f248ee6b96af34ef2a7d08cfb8b9eb61643bd4c1`. Its +5-second forecast had 58 missing slots, one observed slot and one pending slot; +10 seconds had 53 missing slots, and +30 seconds had 33. The recorded maximum interior carry was zero at that point.

This evidence supports **history rebuilding after an explicitly recorded connection gap**, rather than the proposed explanation of a carry exceeding ten seconds. The deployed engine's `record_gap` clears that feed's retained history and current state. Fresh spot/TWAP values can therefore restore a valid Redis health payload while historical slots required for some horizons remain unavailable. Longer source horizons require fewer old slots and recovered sooner in this observed episode. This does not establish the network reason for the connection ending.

| Horizon | Longest consecutive unusable sequence, including absence | First–last planned probe UTC | Present-but-ineligible portion |
|---|---:|---|---:|
| +5 seconds | 588 probes | 22:47:39.402–22:48:38.102 | 576 probes, beginning 22:47:40.602 |
| +10 seconds | 534 probes | 22:47:39.402–22:48:32.702 | 522 probes, beginning 22:47:40.602 |
| +30 seconds | 332 probes | 22:47:39.402–22:48:12.502 | 320 probes, beginning 22:47:40.602 |

Each sequence began with the same 12 absent-key probes, followed by a present key with the horizon unavailable. These are consecutive sampled outcomes, not measured continuous outage durations. A frontend must honor each horizon's null price, quality and reasons even when the key exists and current inputs are fresh; key presence alone would conceal this recovery period.

[Independent exclusion and streak breakdown](observer_gap_breakdown.json) retains the reason counts, missing-slot distributions, exact example payload identities and sampled sequence endpoints.
