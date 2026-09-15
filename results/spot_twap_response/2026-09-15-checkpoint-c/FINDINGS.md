# Checkpoint C: live API and browser delivery findings

The Redis-only snapshot API and SSE stream were deployed at commit
`5bc676cda3a0817bfac6b9ce288b3e9f694cc435` and exercised through the owner's
SSH tunnel. The browser received useful forecasts ahead of their matching
official TWAP anchors. This completes the bounded delivery experiment, not a
rollout of an indefinitely running forecast producer.

The producer is disabled after the observation. The API remains enabled on
`127.0.0.1:9000`; `/forecasts/chainlink-twap/live` returns a truthful HTTP 503
while no current forecast exists, and `/forecasts/chainlink-twap/stream` reports
unavailable state. Existing official source feeds remain healthy.

## Measurement and results

The predeclared cohort admits forecasts during the first 900 seconds of a
1,020-second browser capture, followed by 120 seconds collecting later anchors.
The producer started at `2026-09-15T02:15:52.981Z`, about 22 seconds after browser
capture began. It was stopped by a separately scheduled operator action after
17 minutes. Its original one-hour hard cap and storage limits were unchanged.
The raw capture has 11,242 records and an actual duration of 1,020,118.5 ms.

Each forecast is paired with the exact target stamp and value in a later
official TWAP anchor delivered through this same SSE stream. Lead is the
difference between the two browser `performance.now()` handler-entry times.
It measures delivery to browser code, not paint time, an exchange advantage,
or absolute one-way network delay. Missing exact anchors are censored.

| Horizon | Admitted / matched / censored | Median browser lead | Median absolute error | p90 absolute error |
|---:|---:|---:|---:|---:|
| 1 s | 1,481 / 1,453 / 28 | 0.696 s | $0.00486 | $0.263 |
| 2 s | 1,601 / 1,575 / 26 | 1.756 s | $0.00486 | $0.251 |
| 3 s | 1,605 / 1,574 / 31 | 2.720 s | $0.00486 | $0.253 |
| 5 s | 1,609 / 1,574 / 35 | 4.717 s | $0.02716 | $0.312 |
| 10 s | 1,619 / 1,581 / 38 | 9.726 s | $0.20230 | $0.957 |
| 30 s | 1,658 / 1,617 / 41 | 29.732 s | $2.30141 | $8.037 |

Financial calculations use Decimal with precision 80; tabulated values above
are rounded for readability. Full quantiles, maximum errors, exclusions and
cohorts are in [the analysis](browser_analysis/summary.json). The independent
implementation reproduced all 30 checked comparisons from the compressed raw
capture without importing the scorer or engine: [independent check](independent_check.json).

Long lead tails are not extra predictive power: delayed delivery of an official
anchor can enlarge measured lead. For example, the five-second horizon has
10.98 s p99 lead and 16.08 s maximum lead. The matched cohort excludes targets
already observed at forecast receipt, so positive matched lead is conditional
on that selection and is not a separate success rate. These are rolling TWAP
price forecasts, not settlement-side accuracy or evidence of a trading edge.

## Availability and transport

There was one EventSource open, zero observed SSE error events, no API generation
change, and zero reported client skips. This does not prove that every producer
decision or every official target reached the browser. Missing targets above
remain visible in the denominators.

Of 204 timed snapshot attempts, 180 returned HTTP 200, 13 returned HTTP 503 and
11 recorded abort errors on requests configured with a three-second timer.
Twelve 503s occurred during startup;
the remaining one was in the follow-through period. The cause of the 11 aborts
is unresolved. Sampled server logs show no API exception, but that does not
prove a network cause or eliminate an application scheduling cause.

A reused-connection loopback benchmark of 30 snapshot requests returned 200
throughout: median 3.343 ms, nearest-rank p90 7.056 ms, maximum 17.797 ms. The
serialized producer bytes were returned without gzip. The browser capture's
2,031 unique API envelopes had median API read-to-fanout 0.760 ms and median
fanout-to-send 0.060 ms. This envelope cohort includes heartbeats and repeated
producer payloads; it is not a measurement of new-publication latency alone.
Producer receipt-to-Redis latency was not optimized in this checkpoint.

The predeclared 100 ms browser point probes from capture +65 s up to +900 s
classified 93.62%, 94.24% and 95.59% of 8,350 planned observations as
usable at 5, 10 and 30 seconds. Each horizon had three unknown observations,
included in the denominator. These are conservative point classifications using
a server-clock bracket from GET round trips, not continuous Redis-key coverage.
The cutoff is relative to browser start, not confirmed producer warm-up: first
eligible forecasts appeared at capture +81.44 s, +76.23 s and +57.29 s for those
three horizons. The scorer preserves this original cutoff and does not independently
reconstruct the probe's clock bracket or selected-payload eligibility. A frontend must expire values
locally when the tunnel stalls; server `remaining_ns` is not a new lifetime
starting when the browser receives it.

During the declared 30-second concurrent GET/second-client exercise, the measured
SSE stream delivered 68 envelopes containing 65 unique producer decisions without
an error event. Six recorded timed GETs started in that interval and all returned
200. Those diagnostics do not record every background load request, so they do
not certify the achieved concurrency or a load-dependent speed improvement.
An unread browser stream also does not prove server-side send backpressure.
[Transport details](transport_check.json) preserve the interval definitions.
Isolated tests, rather than this short live exercise,
cover blocked sends, subscriber reconnects, conflicting/replaced producer runs
and queue overflow. This run does not establish indefinite load capacity or
validate a reconnect-retention event unless one actually occurs.

## Stop, audit recovery and current state

The new campaign produced 1,983 decisions, bringing the retained audit table
from 14,346 to 16,329 rows. Of those decisions, 1,974 have acknowledged
publications; the other nine were withheld or coalesced. All 1,933 unique producer
objects delivered during the complete browser capture match the audit's exact
attempted bytes, including all 1,708 objects from the admission interval. The
[streaming audit check](audit_check.json) verifies this without importing runtime
code. Byte equality and recorded acknowledgement status do not establish the
time of browser receipt; that comes from the separate capture above.

The only explicit connection-end gaps in the frozen audit occur at operator
shutdown. The reconnect-retention success path was not exercised by this run.
Shutdown began at 02:32:53.291 UTC. The optional
collector wrapper's five-second close budget interrupted the runtime's own
audit drain; `ghost_optional_shutdown_incomplete` appeared at 02:32:58.531.
There were 64 nonterminal tail rows and 65 retained outbox records after stop.
This was an operational failure of the shutdown drain, not a clean automatic
completion, and should be fixed before continuous operation.

With the producer disabled, a reviewed recovery-only helper acquired the
existing spool lock, checked the exact campaign/run, and archived/fsynced the
original outbox before invoking the existing tested recovery routine. It never
started workers, read feeds or accessed Redis. Recovery preserved frozen inputs,
acknowledged publication evidence and already observed targets; unobserved tail
targets stayed explicitly unmatched. The campaign file and previous campaigns
were preserved. All rows are now terminal and the outbox is empty.

Recovery evidence: [plan](recovery_plan.json), [completion](recovery_complete.json),
[original outbox archive](outbox_before_recovery.tar.gz), and
[post-stop checks](production_verification.json). The complete 747,548,263-byte
external audit export was verified locally and all 16,329 unchanged row proofs
were acknowledged, with zero stale or ineligible rows. Its SHA-256 is
`8cf583804c51855d5b443e359ca1fd3f8f751066ff07924498428e15a188a9be`.
[Export verification](export_verified.json) records the command, location and
counts. No audit rows were expired or deleted. The first post-stop check's Git
read failed under root due to repository ownership; the linked subsequent check
uses the service user and verifies the clean deployed checkout.

## Validation and next work

Before deployment the full suite passed 1,517 tests with 10 opt-in datastore
tests skipped. The deployed Python 3.12 environment passed 97 focused payload,
hub and API tests. The final browser scorer has 46 passing tests, plus the
independent raw-capture recomputation. Source hashes and original test evidence
are retained with these results. No schema migration or new dependency was
needed; the API still uses reader credentials and loopback access only.

The next production checkpoint should fix the nested shutdown deadline and
prove complete tail drainage, then define and review the retention/capacity
policy for an ongoing worker. The previous 38 ms publication path remains a
separate measured optimization task. The API delivery path is installed and
tested; it is not necessary to build another forecast engine or endpoint before
those operational issues are addressed.

## Reproduction and artifacts

`browser.jsonl.gz` is the complete lossless capture. Its SHA-256 is
`1f15469f371fb345407384701c29f0860a4f3302a8a5d855c9f271ba3a46729b`;
the decompressed bytes hash to
`986fd7d42cc8084ef5c35fc8913ab4c8a07a0a3cf2835c1056e20e41082fa9b3`.
From the repository root, select a new output directory:

```bash
python research/spot_twap_response/checkpoint_c/analyze_browser.py --input results/spot_twap_response/2026-09-15-checkpoint-c/browser.jsonl.gz --output dist/checkpoint-c-reproduction
python -m pytest tests/test_ghost_browser_analysis.py
```

The original analysis, earlier test manifests and capture are frozen. The raw
full-table audit is retained on the owner's computer outside Git; its manifest
records the verified file hash, row count and location. The main result is the
browser delivery evidence above, with audit acknowledgement and collector-side
timing treated as distinct measurements.
