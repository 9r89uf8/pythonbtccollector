# Live ghost TWAP — implementation plan

**Status: the one-hour B canary is complete and ghost is disabled again.**
The [official results](GHOST_TWAP_CANARY_RESULTS.md) support the calculation and
bounded live operation. Median receipt-to-Redis publication was 39.19 ms,
missing the under-10-ms optimization objective. The 7,292 terminal audit rows
are externally verified; C's API/SSE and browser delivery checks remain planned.
Longer operation still requires a storage review. [A contract](GHOST_TWAP_CHECKPOINT_A.md),
[B implementation](GHOST_TWAP_CHECKPOINT_B.md), [review corrections](GHOST_TWAP_CHECKPOINT_B_REVIEW.md).

The [canary peer-review addendum](GHOST_TWAP_CANARY_REVIEW.md) prioritized freshness,
then whole-batch rejection when a short target arrives, then publication profiling.
The [freshness/expiry checkpoint](GHOST_TWAP_FRESHNESS_CHECKPOINT.md) now implements
separate five-second source and three-second wall/monotonic receipt limits with
consistent expiry. The original three-second canary remains separate evidence;
new-policy live publication and browser coverage still require validation.
Batch eligibility and publication durability are unchanged in this checkpoint.


Build an optional ghost-price worker in the existing Chainlink collector, using its accepted spot and TWAP events. Publish forecasts separately from official TWAP, then expose them through a Redis-only read API after a prospective shadow run.

## V1 output

- Source-stamp horizons **1, 2, 3, 5, 10 and 30 seconds**. Short horizons also serve as reconstruction diagnostics; no fixed error tolerance is promised.
- Each horizon carries its target source timestamp, estimated local arrival time, predicted Decimal price, quality state and observed/carried/pending/future/missing slot counts (payload contract 3).
- No strike, side call or settlement winner in v1. Each target keeps its calendar market window, using the existing [market helper](price_collector/market.py).
- Proposed Redis key: `btc:live:ghost_chainlink_twap_60s`. Snapshot: `GET /forecasts/chainlink-twap/live`; primary frontend delivery: SSE at `GET /forecasts/chainlink-twap/stream`.
- The owner confirmed a frontend on their own computer, accessing the loopback API through a persistent SSH tunnel. Frontend code stays in a separate project; no public API binding or frontend on the droplet.

## Calculation and input rules

At actual decision time D, freeze the worker's received-event snapshot. Let w be the latest received official TWAP source stamp. For each horizon h, target U = w + h seconds:

```text
ghost_D(U) = mean of the 60 reconstructed spot slots U−62 through U−3
```

Use UTC epoch milliseconds in code; retain wall/monotonic receipt clocks and event order. All prices, sums and errors remain Decimal. Keep official E18 values unchanged and define sufficient Decimal precision and serialization rounding explicitly.

For each historical slot u <= D, choose the greatest source stamp <= u in the frozen snapshot, using the latest accepted revision at that source stamp. An exact source match is observed. For an older permissible value, label the slot pending if u is beyond the greatest admissible received spot source stamp; otherwise label the interior absent slot carried. This maximum comes from the received history, not necessarily the latest-receipt current input. For u > D, use the selected current spot as future flat continuation. Input selection, prices and age limits are unchanged by the pending label. Missing or invalid inputs make that horizon unavailable; never average fewer than 60 slots. The five category counts always sum to 60.

Choose current spot/TWAP by latest receipt/acceptance order before applying quality checks. Do not fall back to an older favorable state after rejecting a newer stale or invalid one. Preserve revision identities for the values actually selected. Reject ambiguous ordering, conflicting anchor TWAP values, future-dated inputs and unsupported non-second-aligned source stamps. A regressing latest TWAP anchor suspends new forecasts until a valid advancing anchor is available.

**A target with U <= D may still be unreceived and useful.** Remove the earlier blanket source-time rejection. Reject an exact target already received; separately record targets that arrive during calculation or before publication. Forecast usefulness is determined by receipt/publication order, not whether its source timestamp is in the past.

The 1–3-second windows often need no tail continuation, but missing historical seconds, late inputs and feed stalls can still require estimates. Enumerate the actual slots; do not use negative `h−3` counts or label every short forecast fully known. The [offline short-horizon check](research/spot_twap_response/short_horizon_review/summary.json) demonstrates these exceptions; it does not measure 1–3-second forecast errors.

## Expected arrival time

Include an explicitly labeled baseline estimate:

```text
estimated_arrival = anchor_TWAP_receipt + h seconds
estimated_remaining_at_D = h seconds − (D − anchor_TWAP_receipt)
```

This assumes the target has the same source-to-receipt delay as its anchor. It is an estimate, not a scheduled publication time. The observed median h−0.4 seconds came from the minute-grid replay's anchor age; it is not a universal correction.

Retain the anchor receipt, estimate method/version and signed remaining time. If the estimate has passed but the target remains unreceived, mark the estimate overdue rather than pushing it into the future. Publish an uncertainty interval only after calibrating target-minus-estimated arrival errors on the prospective run, including gaps; do not hard-code “half a second of jitter.” API client receipt time remains unmeasured.

## Worker and quality policy

Use small nonblocking event offers from the existing [Chainlink collector](price_collector/polymarket_chainlink_collector.py). An optional supervised worker owns its bounded history, forecasting and pending-target state; separate bounded workers handle ghost Redis writes and audit persistence. Preserve critical source delivery, TWAP durability, idle deadlines and official source keys. No extra feed connection, service, database input polling or request-time calculation is needed.

Trigger refreshes on accepted spot/TWAP events and **publish immediately when idle**. Coalesce only bursts or updates arriving during in-flight work, with a bounded latest-state queue; remove the proposed mandatory 100 ms interval. Keep a separate health/expiry timer. Measure receipt-to-publication latency and skipped updates; freeze any rate cap from those measurements. Never backdate a delayed calculation. Receipt-to-Redis acknowledgement below 10 ms is an initial optimization objective under normal load, not a measured guarantee or a bound during failures.

The current implementation limits below use `ghost-canary-v4` / contract 3.
The completed first canary used v3 / contract 2 with three seconds for all age
checks; it is not a prospective validation of the new source-age policy:

| Policy | Initial choice |
|---|---|
| Enablement | Dedicated `GHOST_TWAP_ENABLED=false`; canonical 60-second feed required |
| Warm-up | Live-only input history; no PostgreSQL bootstrap or predictions for downtime |
| Current input freshness | Selected spot/TWAP source age <= 5,000 ms; wall and monotonic receipt ages <= 3,000 ms. Expiry is the earliest source/wall-receipt/monotonic-receipt deadline across both feeds |
| Historical carry | <= 10,000 ms per slot; expose count and maximum age |
| Memory | 120 seconds of context plus a necessary bounded seed, with hard event-count/queue caps |
| Gaps and loss | Invalidate affected windows; record drops/backlog and rebuild coverage before recovery |
| First canary | At most one hour for initial capacity measurement, then automatic stop; earlier stop on hard capacity/deadline/integrity guards |
| Audit retention | Whole decision rows for 96 hours, eligible for expiry only after terminal matching and a verified external export; no indefinite compact-row history |
| Audit guards | Warn at 1 GiB; stop new ghost decisions at 1.5 GiB, with a 2 GiB total-relation budget; maximum 600,000 decision rows |
| Filesystem reserve | Stop below 10 GiB available on the database filesystem; suspend admission/publication when no fresh guard reading is available |
| Target matching | Terminal missing status 120 seconds after decision time; later reports are separately flagged and never replace frozen scoring |

The carry and freshness limits are stricter than the broad replay. Report the resulting availability loss, including during stalls, rather than imply its historical sample counts will repeat.

Use **healthy / degraded / unavailable**, never an uncalibrated confidence percentage. Healthy passes all checks without interior carried slots; allowed interior carry or incomplete audit persistence is degraded. Pending and future slots remain explicit assumptions even when healthy. Pending does not promise eventual delivery, and an interior absent stamp does not prove producer silence. Missing slots, stale/invalid inputs, relevant gaps, unresolved conflicts or an already-seen target make the affected horizon unavailable. The three-second receipt guards and five-second source guards still bound stalled or delayed feeds.

In B, measure constituent-queue high-water marks, input drops, reset causes and unavailable duration/recovery for each horizon. Size and fault-test the bounded queue against observed bursts; never assume overflow is impossible or coalesce away constituent events. After loss, availability recovers as each horizon's required window is covered, not after a universal 62-second timer. Preserve core feed priority and the explicit missingness policy.

A remains accepted with any interior carry labeled degraded. B may explicitly version a more tolerant quality threshold, such as interior carry at most three seconds, while preserving counts, prices and current freshness guards. The recorded h=5 carry distribution is 1,383/2,031/57/114 forecasts at maximum interior ages 0/1/2/6 seconds. It describes this hour's overlapping windows; the aggregate identity fit does not prove a conditional accuracy bound for short carries. Treat any threshold change as a tested policy decision, not an arithmetic fix or a prerequisite to deploying A.

## One audit table

**Use one new audit table, with one decision row containing the frozen snapshot and all horizon results.** It must reconstruct each horizon's 60 slots. The six target windows share a union of **89 slot timestamps**, w−61 through w+27 inclusive; store overlapping slots/inputs once within the row. A second full input ledger is not required for v1, and existing TWAP records can be referenced.

The frozen decision data contains:

- Actual decision wall/monotonic time, run/decision IDs, model/configuration version, included event sequence, backlog/drop/gap state and publication intent.
- Current spot and official TWAP anchor values and identities, including source and receipt clocks.
- For every selected slot: exact Decimal value or missing marker, slot timestamp, original input source/receipt clocks and sequence identity, category and carry age. Current-spot continuation points to its actual input record.
- Forecast values, quality/reasons and ETA assumptions. An unavailable/status-only row records why no usable forecast was produced.

Separate result/status fields hold computation completion, Redis attempt/acknowledgement or failure, and each target's actual price, event identity, receipt time and matching status. Fill these idempotently as events occur; never overwrite the frozen inputs, forecast, cutoff or first target match. Later conflicting target reports are flags, not replacements chosen to improve error.

Sixty prices alone reproduce an average but cannot establish receipt-time eligibility. The richer slot records preserve what this worker actually used despite later `price_samples` upserts. They still do not independently reconstruct every unused input or prove that ingestion/selection was complete; sequence/loss watermarks and engine tests address that operationally. A full event ledger remains an optional future forensic capability, not a prerequisite based on an unmeasured claim that revisions are rare.

Audit writes are asynchronous and bounded. Reserve capacity for a complete audit row for **every published decision**, including pending result updates. Do not sample detail at one row/second while publishing more often. Record missing decisions/status intervals explicitly; on audit pressure, suspend new publishable decisions and let cached forecasts expire while core feeds continue.

B implements that reservation with at most 512 active decisions and a 128 KiB
serialized-record bound, reserving 64 KiB within each admitted row for later
publication/results metadata. A fsynced atomic disk outbox precedes Redis;
PostgreSQL does not gate publication. The outbox is at most 64 MiB plus one
atomic-write temporary record and campaign metadata. Recovery selects the newest
consistent disk/database version and never republishes old forecasts. Publication
attempt or acknowledgement lost in a crash remains uncertain even though its
prices and inputs are reproducible. Pending PostgreSQL persistence is an explicit
audit state; a durable complete outbox is not missing audit evidence.

The initial B ceiling is 10 decision admissions per second during bursts, with
immediate admission when idle. Constituent offers remain separate and bounded at
2,048 entries; only decision/publication refreshes coalesce. These are conservative
canary limits, not measured throughput or latency guarantees. Exact initial row
count plus all subsequent decision admissions bounds rows without scanning a
growing audit table every second. Relation size, filesystem reserve and reading
freshness are checked independently.

The limits above define the first canary, not an approved continuous-production retention policy. Measure representative inserts, all six result updates, indexes, TOAST, dead tuples and maintenance overhead before enabling it. The 1.5 GiB stop threshold leaves 0.5 GiB for bounded in-flight writes/updates within the 2 GiB budget; verify that margin and freeze queue/batch/record limits first. A sampled size check alone cannot guarantee zero overshoot. An early cap stop is an incomplete canary; do not silently raise caps or reduce evidence.

Export the entire canary's frozen inputs/results and hashes to the owner's computer, verify the export, then expire eligible whole rows in bounded maintenance batches. Expiry must atomically match the currently stored result/status version to its verified export; any later status or conflict update invalidates export eligibility until re-exported. The 96-hour age is unchanged after shortening the first run to one hour; it does not relieve storage pressure during that run. Longer validation needs a new capacity review. If export fails, retain the bounded evidence and keep ghost production paused. Keep only bounded reports/aggregates outside the production database afterward. No in-place slot stripping or indefinite per-decision summary tier in v1. Expiry, restart, disk-pressure and space-reuse behavior must be tested before enablement; deleting rows does not itself return their allocated storage to the filesystem. [Capacity evidence and retention rationale](research/spot_twap_response/storage_review/README.md).

## Publication and frontend delivery

Serialize one compact snapshot with all horizons, prices as decimal strings, producer run ID and increasing publication sequence. Include input/decision time, computation time, publication-attempt time, ETA, quality and validity deadline; leave the detailed slot audit out of the live payload. One writer atomically executes `SET` with expiry and `PUBLISH` of identical bytes to a dedicated ghost channel. Use a small writer-only Redis script without price arithmetic. Atomic execution prevents interleaving; it does not promise rollback, durable delivery or exactly-once publication. Record acknowledgements, uncertain outcomes and retries in the asynchronous audit.

**Use SSE push for the local frontend, with snapshot GET as fallback.** After connection, send each new state without a polling wait or a new request per update. Network transport, buffering and browser scheduling still cost time; push latency must be measured. Keep the SSH tunnel and stream open. Prefer a same-origin local frontend proxy to the forwarded port; direct cross-origin browser access would require narrowly scoped local-origin CORS in its own implementation checkpoint.

The [API](price_collector/api.py) uses one long-lived Redis subscriber per process, separate from ordinary request timeouts, and fans out a prebuilt SSE frame. GET reads one key, checks structure/quality/expiry and returns the original JSON bytes; put request-specific age/time in headers rather than re-encoding the body. Neither route calculates forecasts or queries PostgreSQL. Use `Cache-Control: no-store, no-transform`, explicit SSE content type and no stream compression or proxy buffering. Existing source payloads, official TWAP, four-key MGET, reader-only credentials and loopback binding stay intact.

Subscribe and acknowledge before fetching the bootstrap snapshot; buffer concurrent publications and deduplicate by run/sequence. New producer runs require authoritative snapshot resync, not UUID sorting. Register and seed client queues without an update race. Redis Pub/Sub can lose messages on disconnect, so reconnect with a current snapshot and explicit resync state rather than promise historical replay. Each slow client gets a one-item latest-state queue, skipped-update count and send timeout. Neither Redis consumption nor other clients wait for that client.

A health timer must emit stale/unavailable state even when no publications arrive; heartbeats never extend a forecast's validity. The frontend independently expires its display if the stream or tunnel stalls. Permission, timeout, reconnect and buffering details are recorded in the [delivery review](research/spot_twap_response/api_latency_review/README.md).

**Measured baseline, not a ghost benchmark:** the existing endpoint took 2.48 ms median / 5.75 ms p99 on droplet loopback (50 requests), and 578 ms median / 595 ms p90 from this computer through SSH (10 requests). Both reused an HTTP connection and measured complete responses. They do not measure one-way delivery, payload freshness, browser rendering or the future SSE route. The suggested 2–3 ms ghost response, 3% CPU at 10 requests/second and 150–400 ms update delivery remain unverified. [Methods and hashed evidence](research/spot_twap_response/api_latency_review/README.md).

A second instrumented tunnel run measured 597 ms median with exactly one connection call and the same socket/port for all eleven requests including warmup. Reconnection does not explain the earlier 578 ms. Five direct TCP-connect probes had a 362 ms median, but do not establish a 300 ms API response or 150 ms one-way SSE delay. Most HTTP time preceded headers; the transport cause remains unlocalized.

## Prospective scoring and latency acceptance

Match each unseen target U to its first subsequently received canonical TWAP event. Score a **confirmed Redis-visible lead only if successful Redis acknowledgement precedes that target receipt** on the local monotonic clock. A target between attempt and acknowledgement has uncertain publication order. Preserve late, missing, conflicted and unauditable targets.

Compare price errors with persistence on the same published decisions/targets. Report per-horizon availability, observed/carried/pending/future/missing counts, median/tail errors, ETA errors, confirmed publication lead, losses and CPU/memory/queue/storage/core-feed latency. Include every issued decision and boundary-crossing target, without treating overlapping forecasts as independent trials. Short-horizon residuals are diagnostic measurements; 0.003 bp is not a pre-established failure threshold.

Measure event receipt → computation → Redis acknowledgement → API fan-out separately. Report median/p90/p99 under normal and burst load. API send completion is not browser receipt. For frontend usefulness, timestamp ghost and corresponding official-target delivery on the same browser monotonic clock, matching exact target stamps/identities from later anchor updates; skipped/unobserved targets remain censored. Do not subtract unsynchronized browser/server clocks or divide HTTP round-trip time by two to claim measured one-way latency. Checkpoint C must establish the actual useful lead for each horizon through this computer's tunnel.

For a matched pair, browser-visible lead equals collector-visible lead plus the official target's delivery delay minus the ghost's delivery delay. Equal delivery delays cancel, but this does not turn a source-stamp horizon h into exactly h seconds of visible lead. Queueing, polling, connection differences and skipped target updates must remain visible. Moving the droplet nearer the user could alter upstream feed/execution latency as well as display latency; relocation is not a conclusion of this study.

## Recorded replay acceptance for A

Add a frozen, hashed hour of recorded spot/TWAP evidence with sufficient warm-up and target follow-through, selected before examining its errors. State whether it contains complete arrival versions or only retained/upserted samples. Freeze receipt cutoffs/order, anchor and spot selection, carry/freshness/gap rules, target stamps and Decimal rounding. Require the engine to match an independent direct slot sum and all category/quality decisions **exactly at the declared precision** on identical snapshots. The empirical residual against official TWAP is a separate model-error check, not an allowance for implementation discrepancies.

Compare with the research replay only where inputs and policies coincide; explicitly tabulate availability/selection changes under the live policy's stricter limits. Retained historical rows cannot prove first-arrival completeness. Keep synthetic step, reversal, missing-input and ordering tests as well. The frozen September 11 hour and independent calculation are now implemented: all 21,600 horizon comparisons pass, including unavailable results. Contract 2's pending-label revision preserves all original prices, availability, input identities and ETAs. Its replay order and monotonic clock are explicitly synthetic. [Current fixture and provenance](tests/fixtures/ghost_twap/pending_v2/README.md).

## Three checkpoints

| Checkpoint | Work | Required evidence |
|---|---|---|
| A. Pure engine and contract | Implement local Decimal state/calculation, all six horizons, slot accounting, ETA and frozen snapshot format; finalize limits | Exact recorded-fixture parity plus steps, reversals, outgoing prices, gaps, late/repeated inputs, source-time-past/unreceived targets, seen targets, clock errors and market boundaries |
| B. Optional worker, audit and shadow run | Add default-off integration and the single audit table; prove guards/expiry/export, then run the bounded canary with ghost API routes disabled | Every published decision reproducible; failures preserve core collection; counts reconcile; publication precedes credited targets; measured storage, retention and latency fit the frozen budget |
| C. Read API and frontend delivery | Review the canary; expose snapshot GET and SSE through the existing SSH-only access path | Decimal/expiry checks; reconnect and producer restart resync; slow-client isolation; no buffering or stale display; browser delivery/lead and bounded load measurements; no model/DB execution; existing endpoints and official values pass regressions |

[Verified study results](SPOT_TWAP_RESPONSE_STUDY.md) justify this canary, not a prospective accuracy guarantee. The [causal-opening settlement replay](research/spot_twap_response/receipt_clock_pilot/README.md) is already complete: T−30 had 36 projection errors versus 125 TWAP errors among 1,925 markets. It is not an outstanding prerequisite.

A is accepted and B's original one-hour prospective canary is complete. Its
results support bounded operation and forecast utility while identifying freshness
losses and a publication-speed miss. The freshness revision has separate offline
validation; a new live run is still needed. Enabling SSE remains C. Longer capacity, retention maintenance and browser
delivery are not established by this run.

Keep production code under `price_collector/`, with no research imports or retired-pipeline reuse. Each implemented checkpoint gets focused tests and relevant documentation; run the full suite when practical. B/C runtime/schema/API changes require the repository's normal droplet handoff and schema-before-restart ordering. A's standalone module is not imported by running services and needs no service restart.
