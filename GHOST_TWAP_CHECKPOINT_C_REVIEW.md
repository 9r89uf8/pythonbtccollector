# Checkpoint C peer-review verification

Reviewed commit: `6c6115d6047c546cde13e412e83fb4d010d12501`.
This is a verification addendum. Production code, settings and services were not
changed, and no producer campaign was enabled. Earlier canary artifacts remain
unchanged.

The review identifies a real Redis resynchronization defect and a real optional
settings isolation defect. The deployed API and recorded price results remain
valid within their original cohorts. Several explanations in the review need
narrower claims, especially browser usability, HTTP aborts and storage policy.

## Deployment and tests

The fresh read-only check found the droplet at the stated commit, a clean
checkout, all six relevant services active, producer disabled, API enabled,
empty ghost key and empty outbox. PostgreSQL, Redis and the API listen on
loopback; SSH remains the expected public listener. All 16,329 audit rows are
terminal. The previously completed external export and acknowledgement cover
every retained row, including the 1,983 decisions from C.

The full development-environment suite independently reproduces **1,523 passed, 10 skipped**, with two
third-party deprecation warnings, in 47.75 seconds. A fresh ten-request loopback
probe returned typed `503/no_current_publication` throughout; its median was
2.580 ms. A separate SSE request received its first resync/unavailable envelope
in 4.785 ms. The peer's 3.4 ms observation is consistent with a separate small
timing sample, not a fixed latency promise.

Evidence: [production state](results/spot_twap_response/2026-09-15-checkpoint-c-review/production_state.json),
[test output](results/spot_twap_response/2026-09-15-checkpoint-c-review/full_suite.txt),
[fresh API probe](results/spot_twap_response/2026-09-15-checkpoint-c-review/api_probe.json).

## Coverage, price cohorts and recovery counts

The **99.6% audit coverage and 3.7-second gap reproduce**: the stricter reconstruction
gives 99.6168% and 3.6593 seconds between configured producer start +65 seconds and
its scheduled stop. It uses acknowledged publication membership, actual TTL
arithmetic, replacement by later payloads and known target receipts. It remains
an audit reconstruction rather than continuous Redis observation.

The claim that roughly a minute of warm-up explains the browser's 94–96% is
incorrect. That browser metric uses only capture +65 s through +900 s. The
initial pre-eligible period still inside that window is 16.44 s at the five-second
horizon, 11.23 s at ten seconds, and zero at thirty seconds.

| Horizon | Original browser probes | Browser probes after producer start +65 s |
|---:|---:|---:|
| 5 s | 93.62% | approximately 95.48% |
| 10 s | 94.24% | approximately 95.48% |
| 30 s | 95.59% | approximately 95.48% |

The alternate cutoff is mapped using both endpoints of the measured host/browser
clock bracket; it is a sensitivity check, not a replacement for the original
cohort. All three alternate panels still contain 365 scheduled point probes
classified `expired_at_probe`, plus three missing probes. Over the aligned
post-warm-up browser interval the audit reconstruction reports 100% coverage.
Thus startup does not account for the remaining difference. Conservative expiry,
delivery timing and observation methods must stay separate; this comparison
does not prove packet loss or identify the entire cause of the expiry-classified
browser gaps.

For the **same delivered decision cohort and exact matched targets**, audit and
browser price errors agree exactly: medians $0.02716476, $0.20230247 and $2.30141120
at 5, 10 and 30 seconds. Median audit-confirmed lead is 4.7141, 9.6986 and
29.7020 seconds; median browser lead is 4.7170, 9.7256 and 29.7318 seconds. Similar
medians and exact payload bytes support correct delivery of the observed objects.
They do not establish that nothing was lost: the original missing-target
denominators and conservative browser usability still apply. Comparing the whole
17-minute audit with a 15-minute browser admission cohort can change price medians.

The peer's specific audit figures also reproduce when its cohort starts at
configured producer start +65 seconds:

| Horizon | Matched | Median absolute error | Median confirmed audit lead |
|---:|---:|---:|---:|
| 5 s | 1,816 | $0.02880473 | 4.7226 s |
| 10 s | 1,807 | $0.21213393 | 9.7049 s |
| 30 s | 1,766 | $2.17107237 | 29.7194 s |

Those are valid rounded post-warm-up audit results. They should not replace the
distinct browser cohort or serve as a measurement of zero delivery loss.

The **56 newly unmatched targets** are correct. The 65 archived pre-recovery
records match the highest prestate versions/hashes in the recovery plan. Exactly
56 pending targets became `restart_unmatched`; every previously nonpending target
and acknowledged publication was preserved. Another 62 targets already had that
status, giving 118 across the final run. These are missing observations, not
forecast errors. This distinction is consistent with the 64 nonterminal rows
reconciled and 65 outbox records processed.

Evidence and reproducible cohort definitions:
[audit/browser review](results/spot_twap_response/2026-09-15-checkpoint-c-review/audit_review.json),
[review code](research/spot_twap_response/checkpoint_c_review/audit_review.py).

## Confirmed Redis resync defect

Production uses redis-py **8.0.1**, rather than the development environment's
7.0.1. The exact production-version reproduction uses a real redis-py client and
a synthetic local TCP/RESP server, with the deployed hub unchanged. After the
library resubscribes, the test observes two TCP connections and two consumed
subscription acknowledgements, but only one authoritative cache read and no
hub generation change. The hub still holds decision 1 while Redis contains
decision 2; the next publication advances it directly to decision 3. This
reproduces the missed-update/resync problem without disconnecting production.

The cause is `_pump` ignoring subscription acknowledgements after the initial
handshake. The existing expiry still bounds the cached object's lifetime; the
defect does not grant unlimited lifetime. It can retain superseded information
until the next accepted update or expiry, or leave an expired/unavailable state
while a valid newer cache entry exists.

**Disabling retries alone is incomplete.** `PubSub.parse_response` also reconnects
directly if its connection is already disconnected, outside the retry wrapper.
A correct change must treat a later subscription acknowledgement/reconnection
as a resync, invalidate old delivery state and run the acknowledged-subscription
plus authoritative GET/PTTL bootstrap again. Tests should cover both direct
reconnection and exception/retry paths. Retry policy can then be chosen to keep
the supervisor in control of retry timing.

Evidence: [production source](results/spot_twap_response/2026-09-15-checkpoint-c-review/production_redis_source.json),
[exact-version reproduction](results/spot_twap_response/2026-09-15-checkpoint-c-review/redis8_delivery_proof.json),
[reproduction code](research/spot_twap_response/checkpoint_c_review/reproduce_delivery_review.py).

## Optional settings and health payloads

The optional-settings problem is also confirmed. With `GHOST_TWAP_API_ENABLED=false`
and `GHOST_TWAP_API_MAX_CLIENTS=0`, API lifespan raises a validation error before
starting its core database pool. A malformed optional setting can therefore take
down the ordinary API at startup. This should be isolated to the optional feature,
with an explicit diagnostic; silently accepting malformed values is not the fix.

The GET/SSE difference is real and follows the declared contract. Snapshot GET
requires an eligible forecast and returns `503/no_eligible_forecasts` for an
all-unavailable payload. SSE preserves the producer's health information and
official anchor, using reason `no_eligible_horizons` with all forecast prices
null. A consumer must use horizon eligibility, not infer a usable price merely
from `state=snapshot`. In the recorded capture, **six** of the twelve startup
503s were `no_eligible_forecasts` and six were `no_current_publication`, rather
than eleven being caused by this distinction. There was one additional 503
during follow-through.

Evidence: [local behavior proof](results/spot_twap_response/2026-09-15-checkpoint-c-review/local_delivery_proof_final.json)
and [original transport recount](results/spot_twap_response/2026-09-15-checkpoint-c/transport_check.json).

## HTTP aborts and slow readers

The keep-alive hypothesis is plausible, but it has not been established as the
cause of the eleven aborts. The deployed Uvicorn 0.50.2 has no keep-alive override
and defaults to five seconds, matching the probe's nominal request interval.
The server starts its idle timer after response completion and cancels it on
new request data. Nominal request spacing alone is not an exact measure of that
idle interval. [Uvicorn's timeout documentation](https://www.uvicorn.org/settings/#timeouts)
describes the default.

The captured API-time header is sampled **after** Redis retrieval, payload parsing
and eligibility checks; it is not a handler-entry timestamp. The evidence cannot
place all excess time before handler entry. A selected eight-request network
sample alternates four new local connections/slow requests and four reused/fast
requests, which supports further investigation. It does not show FIN/RST events,
automatic retries, the cause of every abort, or that half of all polls used closed
connections. Raising keep-alive above the poll interval is a reasonable controlled
test, not a proven cure. No production timeout was changed during this review.

The claimed 64 KiB slow-reader threshold also needs qualification. Uvicorn's
`HIGH_WATER_LIMIT=65536` in the inspected source controls incoming request-body
buffering. SSE write flow control uses the transport's pause/resume-writing and
drain behavior. Transport, kernel, SSH and browser buffers can absorb data before
an ASGI send blocks. The two-second send deadline starts to matter at a blocked
send; it is not a two-second browser-consumption guarantee or a universal 64 KiB
end-to-end backlog cap. Browser expiry remains necessary.

Evidence: [installed Uvicorn source](results/spot_twap_response/2026-09-15-checkpoint-c-review/uvicorn_source_probe.json)
and [transport claim check](results/spot_twap_response/2026-09-15-checkpoint-c-review/transport_claim_review.json).

## Storage and operator hygiene

The capacity concern is valid, but there were **two one-hour runs and one
17-minute producer observation**, not three one-hour runs. The audit relation
grew from 191,275,008 bytes before C to 218,710,016 bytes after its export
acknowledgements: 27,435,008 bytes. A simple linear extrapolation is about
96.8 MB/hour, leaving roughly 14.4 more hours to the 1.5 GiB admission stop from
the measured current allocation. This is descriptive allocation arithmetic,
including updates and export attestations, not a sustained storage benchmark.
The existing one-hour campaign cap remains enabled independently.

The present 96-hour expiry policy cannot sustain that extrapolated rate under
the current budget. Slot stripping after target matching is **one proposed policy**,
not a required remedy. Earlier verified external export plus bounded whole-row
expiry, reduced retained detail, or a different reviewed capacity budget are
other possibilities. Removing constituent detail before preserving a verified
external record would weaken the promised reproducibility. No retention policy
or evidence was changed here.

Exactly five root-owned mode-0644 operator scripts remain directly under
`/var/lib/price-collector`; none has a running process or matching loaded
transient unit. Their hashes match the operator sources already archived in the
repository's canary evidence. The recovery helper refers to `DATABASE_URL` in its
environment; it does **not** embed the writer password. It received writer access
only when the one-off recovery job loaded the collector environment. Keeping
temporary runnable copies indefinitely is unnecessary; reusable operator actions
should be reviewed into the repo and the one-off copies cleaned up while retaining
the evidence archive. This is a hygiene recommendation, not a credential leak.

Evidence: [storage arithmetic](results/spot_twap_response/2026-09-15-checkpoint-c-review/storage_review.json),
[operator inventory](results/spot_twap_response/2026-09-15-checkpoint-c-review/operator_inventory.json).

## What should change next

1. Correct Redis reconnection/resubscription handling and isolate invalid optional
   API settings, with regression tests for both failures.
2. Fix the known nested shutdown budgets and prove complete bounded tail drainage
   while preserving recovery evidence when persistence cannot complete.
3. Review an ongoing storage/export/expiry policy before changing the one-hour
   campaign cap. Preserve reproducibility before discarding constituent detail.
4. Investigate browser expiry gaps and test the keep-alive hypothesis with
   connection and timing evidence. Preserve SSE as the primary delivery path.
5. Clean up the five inactive one-off scripts after retaining their exact archived
   evidence; promote any reusable operation to a reviewed repository command.

This review does not implement or deploy those changes. It records the confirmed
defects and the limits of the proposed explanations so the next checkpoint can
address them explicitly.
