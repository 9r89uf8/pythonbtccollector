# Browser evidence: reliability canary

The export completed, but the browser observation did **not** provide full-hour
delivery coverage. It contains a 49-minute gap while the laptop slept, followed by
failed fetches and EventSource errors. The server-side observer, runtime audit
and shutdown checks are separate evidence and cannot fill this browser gap.

The September 16 attribution correction below changes no measured browser count,
price or lead. [The addendum](peer_review_corrections/CORRECTIONS.md) preserves
the original report at commit `7c31c2a` and records the new evidence separately.

The raw JSONL has 9,214 records and 11,686,040 bytes; SHA-256 is
`cf7c58705a29b30d9148ebeb376839d51423ec4fef583cb8f22d7684aa893be8`.
Its final marker, sidecar totals, unique planned indices and elapsed duration
were checked. “Complete capture” means the planned end was reached and export
was consistent; it does not mean every planned bin was observed.
The [audit join](AUDIT_BROWSER_JOIN.json) verifies all 1,184 distinct observed
producer bodies against exact attempted bytes for run
`fe2dd06da5754c50b696cb9419ca7251`. This establishes membership, not browser
delivery of unobserved decisions or targets.

## Fixed-grid coverage

The primary denominator is 36,000 planned 100 ms bins. There are 6,578 recorded
primary probes, 29,422 missing bins, and 713 recorded but uncalibrated bins:
30,135 bins remain unknown. No delayed observation is backfilled. The longest
missing run is bins 6,399–35,817, covering capture-relative
`[639,900, 3,581,800) ms`: 29,419 bins, or 49 minutes 1.9 seconds. The actual
interval between the bordering probe handlers is 2,942.0646 seconds.

| Source horizon | Full-hour usable / 36,000 | After first object +65s usable / 34,994 |
| --- | ---: | ---: |
| 1s | 5,224 (14.5111%) | 5,183 (14.8111%) |
| 2s | 5,376 (14.9333%) | 5,326 (15.2198%) |
| 3s | 5,390 (14.9722%) | 5,334 (15.2426%) |
| 5s | 5,416 (15.0444%) | 5,334 (15.2426%) |
| 10s | 5,462 (15.1722%) | 5,334 (15.2426%) |
| 30s | 5,625 (15.6250%) | 5,334 (15.2426%) |

The first same-run object arrived 35,518.8000000119 ms after capture start.
Adding 65 seconds and rounding forward for the integer analyzer argument gives
`--warmup-ms 100519`; the first included grid index is 1,006. The resulting
34,994-bin panel has 29,420 missing/unknown bins and no recorded uncalibrated
bins. This exclusion measures time since first receipt, not producer health.
The separate 1,200 follow-through bins contain 1,199 recorded probes, one
missing bin and zero usable forecasts for every horizon.

“Usable” is the frozen browser classifier's recorded freshness/membership mask
at the actual probe handler, not proof of continuous availability, rendering,
or that an official target was still unreceived. The cohort helper validates
the grid and mask shape but does not independently reconstruct that classifier.

## Conditional browser price pairs

The browser received 1,250 SSE envelopes: 1,244 non-null and six unavailable.
There were 60 repeated producer bodies, leaving 1,184 unique decisions. All
arrived between capture-relative 35.519s and 639.256s. Exactly 593 distinct
official anchor stamps were observed; none had conflicting prices.

| Horizon | Admitted | Matched | Missing exact target | Median browser lead | Median absolute error | p90 absolute error |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1s | 1,003 | 988 | 15 | 0.664s | $0.0073 | $0.4548 |
| 2s | 1,061 | 1,044 | 17 | 1.730s | $0.0073 | $0.4548 |
| 3s | 1,066 | 1,045 | 21 | 2.715s | $0.0073 | $0.4597 |
| 5s | 1,070 | 1,039 | 31 | 4.704s | $0.0659 | $0.4943 |
| 10s | 1,081 | 1,038 | 43 | 9.734s | $0.3434 | $1.3509 |
| 30s | 1,120 | 1,035 | 85 | 29.704s | $4.0812 | $9.6888 |

Each horizon has 1,184 candidate decisions before eligibility exclusions.
Lead is the difference between two browser handler clocks: first receipt of
the forecast and first receipt of its exact later source-stamp anchor. Missing
anchors remain censored. Positive matched lead is conditional on excluding
targets already observed at forecast receipt; there were zero such cases here.
These errors describe the observed early segment, not the hour's missing
forecasts, settlement outcomes or economic performance. The
[independent raw recount](BROWSER_INDEPENDENT_CHECK.json) reproduced every
horizon's counts and error/lead quantiles with Decimal precision 80, without
importing the primary analyzer or using audit receipt clocks.

## Clock panels and transport limits

The 113 successful GET clock brackets have a nonempty constant-offset
intersection, 265.332 ms wide. They span capture-relative 71.401–635.302s;
there is no successful calibration after the long missing interval. Under the
explicit assumption that server wall time minus browser performance time stayed
constant, 35,692 primary grid points are inside the campaign, 306 outside and
two boundary-uncertain. Of the inside points, 29,827 are missing or uncalibrated.
This retrospective model cannot certify clock behavior during the gap. The
local wall-minus-performance change between capture endpoints is -5.700 ms;
that is not proof of continuous execution or connectivity.

The same model places the conservative accuracy slice at elapsed
`[30,785.9110490001, 3,510,949.1962499881) ms`, ending at least 120 seconds before
the actual stop request. Its observed decisions and pairs equal the full-hour
admission panel because all delivered forecasts arrived early. No late-hour
accuracy is recovered by this selection.

There were 128 completed GET responses (113 HTTP 200, 15 HTTP 503), with median
full-response duration 446.300 ms, p90 1,389.040 ms and maximum 2,586.400 ms.
All 28 failed GETs occurred after the gap, before response headers, taking
578.0–1,765.7 ms; none recorded its three-second abort timer firing. There were
28 EventSource errors, one successful connection-open event and no reported
client skip increments in received envelopes. Zero reported skips cannot
describe the unobserved period.

[Tunnel stderr](TUNNEL_STDERR.txt) records an untimestamped connection reset.
The owner subsequently confirmed closing the laptop. Windows sleep/wake records
give **23:06:46.252–23:55:48.708 UTC**, matching the missing browser interval.
The [original event check](WINDOWS_SYSTEM_EVENT_CHECK.json) was a false negative:
its UTC-kind hashtable bounds returned no records on this machine. Corrected
local-time bounds and an independent explicit-UTC XPath query agree on four
records, including the sleep and wake times. See
[the preserved correction evidence](peer_review_corrections/WINDOWS_SLEEP_CORRECTION.json).
Laptop sleep explains the missing capture; it does not supply the absent browser
receipts or prove the exact instant/mechanism of the SSH reset. The droplet's
independent producer and observer continued during the sleep interval.

## Reproduction and provenance

Authoritative outputs are [BROWSER_COHORTS.json](BROWSER_COHORTS.json),
[BROWSER_INDEPENDENT_CHECK.json](BROWSER_INDEPENDENT_CHECK.json) and the frozen
[baseline summary](browser_analysis/summary.json). The baseline's generic
`snapshots.error_timing` text predates this richer instrument: failed requests
here do have start/end clocks. Use the enriched failure panel above and in
`BROWSER_COHORTS.json` for that measurement.

The [cohort helper](../../../research/spot_twap_response/reliability_canary/browser_cohorts.py)
checks the exact export, operator metadata and audit join; the
[independent check](../../../research/spot_twap_response/reliability_canary/independent_browser_check.py)
recounts raw browser pairs. Their four focused tests passed. Input/code
hashes are retained in the JSON artifacts. The earlier
provisional cohort result is superseded and kept outside this results directory.
