# Official results: one-hour ghost TWAP reliability canary

Attribution corrections recorded September 16 UTC are documented in
[the review addendum](peer_review_corrections/CORRECTIONS.md). The owner confirmed
closing the laptop, and a corrected Windows query identifies the matching sleep
interval. Scored prices, lead, coverage and raw evidence are unchanged. The
original `FINAL_MANIFEST.json` describes the report at commit `7c31c2a`; the
addendum's manifest records this revised interpretation separately.

The bounded producer and shutdown checks passed. The local Redis observer
covered the full hour, and the audit was exported and verified. The browser
capture missed about 49 minutes while the laptop slept, so this run does
**not** establish full-hour frontend reliability. Missing browser observations
remain unknown; server observations do not replace them.

The producer is disabled again. The read-only snapshot and SSE routes remain
enabled behind the existing SSH-only, loopback API. No continuous campaign,
storage-policy change or replacement canary was started.

## Run and evidence

- Fixed interval: **2026-09-15 22:56:38–23:56:38 UTC** (one hour).
- Run: `fe2dd06da5754c50b696cb9419ca7251`; runtime v6 / calculation contract 4.
- Horizons: 1, 2, 3, 5, 10 and 30 source-stamp seconds. These are future TWAP
  reports, not settlement predictions except when a target coincides with close.
- Policies unchanged: source age at most 5,000 ms; receipt age at most 3,000 ms;
  Decimal arithmetic, exact attempted payloads and durable-before-publish ordering.
- **7,082 new decisions**, all terminal; **7,075 acknowledged publications** and
  seven unpublished decisions with no calculable forecast: one missing spot and
  six stale TWAP inputs (one of those six also had stale spot). The saved generic
  publication status is `expired_or_target_received`, but these seven did not
  lose a previously calculable target.
- Full audit export: **23,411 rows**, **1,078,545,445 bytes**; SHA-256
  `f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954`.
  All 23,411 export acknowledgements succeeded, with zero stale/ineligible rows.
- All **16,329 prior canonical audit rows are byte-for-byte unchanged**; exactly
  7,082 rows were added. The three earlier campaign-file hashes also match.

The frozen [protocol](../../../GHOST_TWAP_RELIABILITY_CANARY.md),
[launch](LAUNCH.json), [export acknowledgement](EXPORT_ACKNOWLEDGEMENT.json),
[prior-evidence comparison](PRIOR_EVIDENCE_CHECK.json) and
[audit/browser join](AUDIT_BROWSER_JOIN.json) record the identities and checks.
Large raw audit, browser and observer exports stay outside Git on the owner's
computer. The final manifest records their paths and hashes.

## Forecast accuracy and confirmed publication lead

The primary comparison uses decisions at or before **23:54:38.428617010 UTC**,
at least 120 seconds before the actual stop request. That leaves 6,859 decisions
with a full matching window. The table includes only acknowledged, eligible
payload members with a valid exact official target. Baseline errors use the
same target pairs while holding the decision's official TWAP unchanged.

| Horizon | Valid published pairs | Ghost median error | Ghost p90 error | Unchanged TWAP median error | Median confirmed lead |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 seconds | 6,650 | $0.0300 | $0.3850 | $1.6570 | 4.650 s |
| 10 seconds | 6,658 | $0.2684 | $1.2207 | $3.2609 | 9.668 s |
| 30 seconds | 6,695 | $2.9955 | $8.9480 | $9.1264 | 29.654 s |

Median ghost errors are **0.003960 / 0.035399 / 0.395528 basis points** for
5 / 10 / 30 seconds. The short forecast remains much closer to the future
official TWAP than simply retaining the last TWAP; uncertainty grows with the
number of future spot slots assumed. These are measurements for this hour,
not an invariant error tolerance.

Confirmed lead starts at Redis acknowledgement and ends at the recorded first
official target receipt. It requires exact target stamp, price and eligible
membership in the attempted payload, with acknowledgement strictly before
target receipt. It is not browser lead. Full-hour, post-65-second and censored
tail panels, all six horizons and baseline comparisons are in
[audit_analysis/tables.md](audit_analysis/tables.md) and
[summary.json](audit_analysis/summary.json). The independent verifier checks
saved 60-slot arithmetic, receipt causality, both expiry clocks and exact wire
membership; no missing target is filled from a neighboring stamp.

The per-horizon publication rule preserved **324 partial batches**: 315 masked
one-second targets and nine masked two-second targets. Longer eligible
forecasts remained publishable. Frozen counter snapshots can lag one decision;
the counts here also use actual saved payload membership.

## Publication speed

Across 7,075 acknowledged publications, included-event receipt to Redis
acknowledgement was **39.212 ms median, 60.682 ms p90 and 106.527 ms p99**.
The under-10-ms end-to-end objective is still missed. Decision to acknowledgement
alone was 35.494 ms median; confusing these two clocks would understate latency.

Median stages were 3.517 ms from included receipt to decision, 0.998 ms for
calculation, 6.934 ms from calculation completion to intent, 23.136 ms from
intent to Redis attempt, and 2.775 ms from attempt to acknowledgement. These
separate medians must not be added to reconstruct a median total. The durable
write still precedes publication; this run does not isolate serialization,
thread scheduling and fsync costs within that stage.

## Local cache observation

The independent droplet observer completed all **36,000 planned 100 ms bins**.
The full hour includes startup: the key was present in 35,803 bins (99.453%).
One scheduling miss and one read crossing a bin boundary are retained as unknown
for strict freshness classification; there were no invalid payloads.

After the fixed first 65 seconds, there are 35,350 planned bins:

| Observation | Bins / 35,350 | Fraction |
| --- | ---: | ---: |
| Key present | 35,198 | 99.570% |
| Fresh, eligible 5-second forecast | 35,197 | 99.567% |
| Fresh, eligible 10-second forecast | 35,197 | 99.567% |
| Fresh, eligible 30-second forecast | 35,197 | 99.567% |

These are sampled cache observations at actual read time, not continuous uptime
or browser delivery. The observer's basic freshness result alone does not know
whether a target arrived after publication; the separate exact audit join checks
that distinction. Full counts and read latency are in
[OBSERVER_ANALYSIS.json](OBSERVER_ANALYSIS.json).

All 6,246 distinct observed wire payloads and all 35,803 present probes joined
to exact acknowledged attempts. The stricter full-hour join gives:

| Horizon | Fresh eligible bins | Exact target still later at read end | Target receipt unknown | Already received |
| --- | ---: | ---: | ---: | ---: |
| 5 seconds | 35,233 | 34,650 | 583 | 0 |
| 10 seconds | 35,277 | 34,621 | 656 | 0 |
| 30 seconds | 35,484 | 34,662 | 822 | 0 |

These counts use the **full 36,000-bin hour**, not the post-65-second denominator.
Unknown exact target receipts are not credited as proven remaining lead. The
one-second horizon had 1,427 fresh bins whose target had already arrived, and
the two-second horizon had 14; fresh cache contents alone therefore do not
guarantee a forecast is still ahead of its print. See
[observer_join.json](observer_join.json) for both-clock comparisons, same-host
provenance and all six horizons.

After 65 seconds there were three series of absent-key observations, covering
56, 51 and 44 consecutive 100 ms bins: 5.6, 5.1 and 4.4 seconds of planned-bin
coverage, respectively. The longest first-to-last probe span was 5.5 seconds.
The same gaps account for known unusability at 5, 10 and 30 seconds; the two
unknown bins remain separate. These sampled spans do not prove uninterrupted
absence between probes. [OBSERVER_GAPS.json](OBSERVER_GAPS.json) preserves the
exact intervals and startup warm-up separately.

The [feed-gap review](peer_review_corrections/feed_gap_review.json) links all
three cache absences to locally received input pauses and exact payload expiry.
TWAP source stamps jump from 22:57:56 to 22:58:04, 23:12:01 to 23:12:08, and
23:55:27 to 23:55:35 UTC. Spot also pauses. The limiting deadlines were,
respectively, TWAP receipt age, spot receipt age and TWAP source age. The input
sequence is contiguous across each boundary, but this does not identify whether
the cause was upstream, network delivery or collector handling. Three episodes
describe this hour; they do not establish a recurring three-per-hour rate.

## Browser result: incomplete hour

The primary browser hour has **6,578 recorded probes out of 36,000**. It missed
29,422 bins, and another 713 recorded bins were uncalibrated: **30,135 bins remain
unknown**. The longest missing interval was 49 minutes 1.9 seconds. Reaching the
end marker and exporting consistent bytes does not make that a complete delivery
observation.

All **1,184 distinct observed producer bodies** matched their exact audited
attempted payloads. The browser saw 593 distinct official anchor stamps without
conflicting prices. Conditional price/lead pairs describe only the observed
early segment; targets absent from this browser stay censored.

| Horizon | Matched browser pairs | Median absolute error | p90 error | Median browser lead |
| --- | ---: | ---: | ---: | ---: |
| 5 seconds | 1,039 | $0.0659 | $0.4943 | 4.704 s |
| 10 seconds | 1,038 | $0.3434 | $1.3509 | 9.734 s |
| 30 seconds | 1,035 | $4.0812 | $9.6888 | 29.704 s |

These are differences between two clocks in the same browser: forecast receipt
and receipt of its exact later official target anchor. They were independently
recomputed from raw JSON with Decimal arithmetic, without substituting audit
receipt times. The explicit pre-stop comparison panel gives the same pairs
because all observed forecasts arrived early; it cannot recover later coverage.

The owner confirmed closing the laptop. Windows records sleep at
**23:06:46.252 UTC** and wake at **23:55:48.708 UTC**, matching the browser pause.
The original Windows event query supplied UTC-kind bounds in a form that yielded
a false negative on this machine. Corrected local-time bounds and a separate
explicit-UTC XPath query return the same four records. The original empty check
is preserved but superseded by
[WINDOWS_SLEEP_CORRECTION.json](peer_review_corrections/WINDOWS_SLEEP_CORRECTION.json).
The tunnel logged an untimestamped connection reset and was found exited after wake;
the exact SSH failure instant is not present in its stderr.
All 28 failed GETs occurred before response headers, and none recorded its
three-second abort timer firing. The 28 EventSource errors and missing samples
remain in the evidence. No reload or replacement capture was used.

[BROWSER_REVIEW.md](BROWSER_REVIEW.md) provides all six horizons, explicit
denominators, failed-request timings and server/browser clock brackets. The
brackets have no successful calibration after the long gap, so retrospective
campaign-overlap panels are conditional on the stated clock assumption.

## Shutdown and operational result

The timer requested the stop **428.617 ms after the fixed deadline**. The matching
producer flag was disabled, and only the Chainlink service was restarted.
Stop request to completed restart took **6.829260077 seconds**.

The shutdown report recorded **223 initial tail records, 223 drained, zero
retained**, zero pending dirty writes, zero pending late events and a completed
final outbox pass. The outbox is empty and every audit row is terminal. There
was no recovery-script intervention. This directly closes the previously
observed five-second drain failure for this run.

Terminal does not mean every target was observed. The 169 targets labelled
`restart_unmatched` comprise two different groups:

| Horizon | Earlier absent exact stamps | Beyond the last observed TWAP stamp | Total |
| --- | ---: | ---: | ---: |
| 1 second | 8 | 0 | 8 |
| 2 seconds | 10 | 2 | 12 |
| 3 seconds | 12 | 3 | 15 |
| 5 seconds | 15 | 7 | 22 |
| 10 seconds | 20 | 16 | 36 |
| 30 seconds | 21 | 55 | 76 |

The first group has 86 forecast-target pairs across ten absent earlier stamps;
the second has 83 pairs beyond the last observed source stamp, 23:56:37 UTC.
Thus the shutdown status alone does not establish that stopping caused all 169
missing targets. Separately, 498 fully-aged `missing` pairs cover 46 earlier
absent stamps. All are absent from the retained received-event union; this is
not proof of provider nonpublication or what a longer observation would have
received. These are missingness categories, not forecast losses. The detailed
[target attribution check](peer_review_corrections/audit_target_corrections.json)
preserves the exact stamps and brackets without changing the audit statuses.

All six core services were active afterward, the four source prices were fresh,
the ghost Redis key was absent and listeners remained local. The disabled
producer's snapshot route returned the expected typed 503 in 2.66 ms at that
point check. The raw verifier's sole failed check required exact equality to
launch HEAD: the observed newer `d5a2e65` commit changed only documentation and
launch evidence. The separate provenance addendum confirms identical committed
runtime, schema, requirements and deployment objects; the original verification
file is preserved unchanged.

See [stop verification](STOP_VERIFICATION.json), [shutdown journal](STOP_JOURNAL.jsonl)
and [runtime provenance addendum](STOP_RUNTIME_PROVENANCE_ADDENDUM.json).

## What this closes and what remains

The new drain budget worked on a real pending tail, and bounded production
continued while the laptop slept. Three received-input silences and cache
expiries are documented above. No explicit transport-gap or reconnect-recovery
record appears around those silences; absence of that metadata is not continuous
feed availability. This run does not
demonstrate a live Redis resubscription; the deliberate-drop proof remains the
previous real-client TCP regression tests. The [bounded API journal check](API_RECONNECT_OBSERVATION.json)
found no matching entries, but the hub has no dedicated reconnect journal
logger, so that absence cannot exclude an unlogged Redis interruption.

Full-hour frontend delivery still needs a successful observation with the laptop
awake and the local capture and SSH tunnel kept running. A browser running on
the droplet would not measure the owner's laptop-to-droplet path. Before
continuous production, the full-evidence storage/export/expiry policy still
needs implementation and validation for that operating mode. The audit relation
grew from 218,710,016 to 309,420,032 bytes between preflight and the post-stop
measurement (90,710,016 bytes); one-hour guards do not establish sustainable
continuous operation. No evidence was expired or compacted in this run.

At that single hour's net growth, the recorded 309,420,032-byte relation would
have about **14.34 additional hours** before the 1.5 GiB admission threshold.
This is a linear capacity illustration, not a prediction or authorization:
the independent campaign deadline still stops the current implementation at
one hour. Storage/export/expiry for continuous operation remains the priority
before enabling that mode.

The results measure forecast reconstruction and delivery. They do not establish
profitability, fills, a trading advantage or a universal error bound.

## Verification and reproduction

All **17 focused analysis tests passed**. The full audit, independent browser
recount, exact payload joins and prior-evidence comparison also passed their
data checks. This completion changes research code and reports only; it changes
no collector, API, schema, dependency or production setting.

The [analysis README](../../../research/spot_twap_response/reliability_canary/README.md),
[browser review](BROWSER_REVIEW.md), [validation record](ANALYSIS_VALIDATION.json)
and [original measurement manifest](FINAL_MANIFEST.json) provide commands,
definitions, file hashes and raw evidence locations. That original manifest's
document hashes apply to commit `7c31c2a`; the
[correction manifest](peer_review_corrections/CORRECTION_MANIFEST.json) records
revised documents without rewriting the original evidence. The dedicated browser was closed after
export; its original tunnel had already exited. Other sessions were preserved.
