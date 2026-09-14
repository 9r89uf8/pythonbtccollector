# Combined freshness and batch-eligibility canary

The owner authorized building the scorer and local Redis observer, testing them,
and running a new one-hour canary. This protocol is frozen before activation.
The previous campaign and its results remain separate evidence.

## Question and fixed scope

Measure how often the combined source/receipt freshness split and per-horizon
publication rule produce a present, fresh, usable ghost value during the hour.
Also measure price error versus unchanged official TWAP, confirmed Redis lead,
publication latency, skipped horizons, losses and resource use.

The ghost runtime remains `ghost-canary-v5`, calculation contract 3, with source
age at most 5,000 ms, wall/monotonic receipt age at most 3,000 ms, ten-second
historical carry and six source-stamp horizons: 1, 2, 3, 5, 10 and 30 seconds.
The existing one-hour deadline, persistent stop latch, relation/row/free-space
caps, input/audit queues and durable-before-publication ordering remain intact.
There is no trading, ghost HTTP/SSE route or frontend in this test.

## Scoring contract

The new offline scorer verifies the full export's hash, row count and individual
frozen/state hashes. It selects exactly the configured campaign start from
frozen runtime policy, preserving multiple runtime IDs if a restart occurred.
Old rows are verified but excluded from this campaign's metrics. Prices, errors,
means and quantiles use Decimal precision 80 with E18 half-even rounding.

Report calculated forecasts separately from acknowledged publications. An
acknowledged forecast enters the published accuracy cohort only when its exact
horizon, target stamp and original price are eligible in the actual attempted
payload. Confirmed lead additionally requires acknowledgement strictly before
the target's first receipt, with no conflict, clock anomaly or causality fault.
Missing targets, unavailable forecasts, uncertain publication and incomplete
evidence remain explicit; they are never filled with a successful observation.
Original candidate prices and means are independently checked from frozen slots.

The scorer writes an attempted-payload hash index. Join each observer payload to
this verified campaign index before calling its membership confirmed. An
observed but unacknowledged payload does not silently become an acknowledged
forecast in the accuracy report. Preserve the original complete-batch analysis
scripts and outputs unchanged.

## Coverage observation

A bounded operational CLI runs locally on the droplet, using a persistent
loopback Redis connection and atomic transactional GET/PTTL reads. It uses no
database credentials or feed connections, and makes no Redis writes. It is a
temporary observer, separate from the collector's critical paths.

The grid is fixed at **100 ms** over `[start, start + 3,600,000 ms)`, giving
**36,000 planned sample bins**. Start the observer and verify readiness before
enabling the new campaign. Record scheduled indices, actual request/reply wall
and monotonic clocks, missing samples, read errors, key absence, expiry and
payload validity. Do not fabricate catch-up observations after a slow read.
Deduplicate exact payload bytes by SHA-256 for later audit joining. Bound each
read and total output size; interrupted/capped observation is incomplete evidence.

Report full-hour and predeclared post-65-second results, plus one-minute bins.
Separate key presence from usable per-horizon prices: an all-unavailable payload
can still be present. Read failures and missed bins are unknown observations,
not absent keys. State denominators and conservative coverage bounds when
observations are unknown. Expiry/freshness checks describe the recorded read
clocks; eligibility does not prove the target remains unseen at the browser.

These measurements are sampled local cache coverage, not exact continuous
uptime, deletion time or frontend delivery. The first campaign has no equivalent
direct observer, so its earlier decision counts and inferred TTL gaps are not a
like-for-like measured coverage baseline.

## Launch and completion

Follow the reviewed [campaign handoff](results/spot_twap_response/2026-09-14-batch-eligibility/CANARY_HANDOFF.md).
Before activation, require the old spool to be empty, all old audit rows terminal
and their current versions externally verified, the ghost key absent, and adequate
total database/filesystem capacity. Preserve the first directory and stop latch.
Use a new unique directory; do not copy or reset the old campaign.

Record the code and observer hashes, new directory, fixed UTC start/end,
observer identity, runtime IDs and original-state hash in a launch manifest.
Start the collector only once the configured start is current/past. Keep that
same start and directory across any restart. An early guard stop ends admission;
do not raise a cap or extend the campaign to fill the hour.

At the fixed deadline, stop new decisions through the existing runtime guard.
Allow the full 120-second matching window after the final decision and drain
audit writes. Then disable ghost and restart only the Chainlink collector.
Verify terminal audit state, an empty new outbox, absent ghost key, healthy
official feeds and unchanged old campaign files. Export audit/observer evidence
to the owner's computer, verify hashes, analyze locally and record results.
Retain both campaigns and exports; no data expiry is part of this task.

## Interpretation

Correctness requires reproducible candidate prices, internally consistent
attempted membership, no false confirmed lead, and explicit accounting for
missing/uncertain evidence. A storage/queue/observer stop or integrity mismatch
must be reported as a limitation or incomplete validation, not hidden.

No coverage or profitability threshold is selected from the observed results.
Report median/tail errors and latency, denominators, distinct targets and losses.
Overlapping forecasts reuse outcomes, and one hour is not a set of independent
trials or evidence of multi-day retention, frontend readiness or trading edge.

## Active run

The combined canary started at **2026-09-14 22:34:04.002 UTC**, with its fixed
end at **23:34:04.002 UTC**. The observer was ready before activation. Runtime
startup confirmed the correct deadline and no stop reason. This is a running
test; coverage and accuracy findings are not available yet.

See the [launch record](results/spot_twap_response/2026-09-14-combined-canary/LAUNCH.json),
[prelaunch validation](results/spot_twap_response/2026-09-14-combined-canary/PRELAUNCH_VALIDATION.json),
[observer dry run](results/spot_twap_response/2026-09-14-combined-canary/OBSERVER_DRY_RUN.json)
and [completion handoff](results/spot_twap_response/2026-09-14-combined-canary/COMPLETION_HANDOFF.md).
