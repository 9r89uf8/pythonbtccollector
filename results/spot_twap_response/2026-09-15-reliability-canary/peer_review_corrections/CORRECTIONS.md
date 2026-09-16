# Review corrections: laptop sleep and missing-target attribution

The peer review confirms the headline accuracy, lead, cache coverage and clean
shutdown results. The owner's report that they closed the laptop is consistent
with independently rechecked Windows sleep/wake events. This addendum corrects
attribution and wording; it changes no raw audit, forecast, observed target,
price-error statistic, coverage count or original measurement output.

## Browser gap: explained by laptop sleep

The local Power-Troubleshooter event records sleep at
**2026-09-15 23:06:46.2521057 UTC** and wake at
**23:55:48.7079296 UTC** (6:06:46–6:55:48 PM CDT). This matches the approximately
49-minute browser pause. Kernel-Power and Kernel-General events corroborate the
sleep/resume and clock update. The owner independently confirmed closing the lid.

The original event query returned a false negative. On this machine, the
hashtable query with UTC-kind DateTime bounds returned zero events. Passing
converted local bounds returned four. A separate XPath query filtering the
event XML's explicit UTC `SystemTime` returned the identical four record IDs.
The correction is independently repeatable with
[check_windows_sleep.ps1](../../../../research/spot_twap_response/reliability_review_corrections/check_windows_sleep.ps1),
and the sanitized results are in [WINDOWS_SLEEP_CORRECTION.json](WINDOWS_SLEEP_CORRECTION.json).
The original empty result is retained unchanged for auditability.

The laptop's sleep interrupted browser observation; it did not stop the remote
producer or observer. The SSH stderr contains an untimestamped connection reset.
We do not infer its exact failure instant from the missing browser records.
The full hour of browser receipts still does not exist, even though the reason
for the missing capture is now established. This is not evidence of a 49-minute
ghost API outage.

## Short cache absences: local feed silence and expiry

All three post-warm-up cache absences align with pauses in offered spot and
TWAP inputs and the exact expiry of the preceding payload:

| Prior/next received TWAP source stamp (UTC) | Limiting expiry (UTC) | Limiting input clock | Absent sampled-bin footprint |
| --- | --- | --- | ---: |
| 22:57:56 → 22:58:04 | 22:58:00.195843694 | TWAP receipt +3 seconds | 5.6 seconds |
| 23:12:01 → 23:12:08 | 23:12:05.385963725 | Spot receipt +3 seconds | 4.4 seconds |
| 23:55:27 → 23:55:35 | 23:55:32.000000000 | TWAP source +5 seconds | 5.1 seconds |

There are contiguous offered-event sequence bridges around each boundary.
Freshness expiry and restoration after fresh inputs explain the local cache
behavior. There are no explicit transport-gap/reconnect records around those
pauses; that is different from uninterrupted feed availability.

The data do not locate the origin of the silence upstream, in the network or
in collector handling. Nor do they prove the provider never published a missing
stamp. Three episodes were observed in this hour; a general three-per-hour rate
has not been established. See [feed_gap_review.json](feed_gap_review.json).

## Unpublished and unmatched decisions

All seven unpublished decisions had no calculable forecast: one missing spot
and six stale TWAP (one of the latter also had stale spot). The generic stored
status `expired_or_target_received` did not mean those seven had lost an already
calculated target. The report's original causal wording was too broad.

The 169 `restart_unmatched` forecast-target pairs split **86 / 83** between absent
earlier stamps and stamps beyond the last observed TWAP source stamp. The
review's per-horizon split reproduces exactly:

| Horizon | Absent earlier stamp | Unobserved continuation |
| --- | ---: | ---: |
| 1 | 8 | 0 |
| 2 | 10 | 2 |
| 3 | 12 | 3 |
| 5 | 15 | 7 |
| 10 | 20 | 16 |
| 30 | 21 | 55 |

One sentence in the peer review needs a narrower scope: those 86 earlier misses
are **not all** at 23:55:28–23:55:34. Fifty are in that seven-stamp hole; the
remaining 36 are at 23:55:47, 23:56:10 and 23:56:28. That makes ten distinct
earlier absent stamps. Separately, 498 fully-aged `missing` pairs cover 46 other
stamps. The two stamp sets are disjoint.

The label `restart_unmatched` describes how the pending state was closed. It
does not prove that stopping caused every missing target. The 83 continuation
pairs lie beyond the last retained source stamp, not necessarily in the future
relative to the wall clock. None of these observations establishes what a
longer or different feed capture would have received. See
[audit_target_corrections.json](audit_target_corrections.json).

## Continuous operation and the next browser check

Storage is still the main operational constraint. The observed 90,710,016-byte
hourly relation increase gives a simple **14.34 additional hours** of headroom
from the recorded post-stop size to 1.5 GiB. That extrapolation assumes unchanged
net growth and is not a storage-capacity guarantee. The current campaign also
has an independent one-hour deadline, so it would stop first; no continuous run
is enabled or authorized by this review.

The next implementation checkpoint should establish sustainable full-evidence
storage/export/expiry for continuous operation. A subsequent browser observation
should use the owner's laptop kept awake, with its SSH tunnel maintained. A
browser on the droplet would bypass the chosen frontend/network path and would
not validate its delivery latency. A separate always-on test client can provide
different supporting evidence, labelled as such. No browser or frontend is to
be installed on this Python-only production droplet.

The approximately 39 ms publication path remains above its 10 ms objective, but
does not invalidate the measured multi-second lead at the longer horizons.
Redis resubscription remains supported by the existing local TCP regressions,
not a demonstrated live disconnect in this run. Adding bounded resync logging
is a reasonable future observability change; none is implemented here.

## Evidence lineage and verification

Original results commit: `7c31c2a1c9e1db3224af2fe5a1ddbde8642c6e4d`.
The original [FINAL_MANIFEST.json](../FINAL_MANIFEST.json), raw exports and scored
JSON artifacts remain unchanged. Its document hashes identify the earlier
report version in that commit. [CORRECTION_MANIFEST.json](CORRECTION_MANIFEST.json)
records original and revised document hashes, new correction evidence and
reproducers. Old statements are not silently substituted into frozen evidence.

The new audit check streamed and hash-verified the existing 23,411-row export;
the feed check verified its saved event index and observer hashes. Windows
verification used two agreeing query methods. This work changes research and
documentation only; it does not query or modify production services, settings,
the database or the paused automation.
