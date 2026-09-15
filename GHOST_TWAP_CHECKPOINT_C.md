# Checkpoint C: Redis-only ghost delivery

Status: deployed at `5bc676c` and the bounded browser observation is complete.
The producer is disabled again; the read-only API remains enabled. The full local
suite passed 1,517 tests with 10 opt-in datastore skips. The collector remains the
only forecast producer. [Completed observations, limitations and export status](results/spot_twap_response/2026-09-15-checkpoint-c/FINDINGS.md)
are recorded separately; this short run does not approve ongoing forecast production.
The subsequent [independent review](GHOST_TWAP_CHECKPOINT_C_REVIEW.md) records
corrections to interpretation, and the [reliability checkpoint](GHOST_TWAP_RELIABILITY_CHECKPOINT.md)
addresses Redis resubscription, optional-setting isolation and shutdown budgets.

## Delivery contract

The optional API is disabled unless `GHOST_TWAP_API_ENABLED=true` is set in the
existing **reader-only** API environment. This flag does not enable the collector
or alter its one-hour campaign cap, storage guards, or official source keys.

`GET /forecasts/chainlink-twap/live` performs one Redis GET and returns the exact
published JSON bytes. A bounded independent parser validates contract 4 / runtime
v6, financial strings, clocks, membership and expiry. No database query or
forecast calculation runs on this route. Missing, expired, malformed or wholly
unavailable publications return HTTP 503 with a short reason. Request-time age,
remaining lifetime and API wall time are response headers, not mutations of the
producer record. The six horizons are 1, 2, 3, 5, 10 and 30 provider seconds
after the anchor; they are not guaranteed wall-clock arrival times.

`GET /forecasts/chainlink-twap/stream` uses one shared Redis subscriber per API
process. Every `event: ghost` contains an `api` delivery envelope and the original
producer object under `ghost`, or `ghost: null` when delivery is unavailable.
Prices remain decimal strings and nanosecond clocks remain strings. The envelope
records API instance, generation, sequence, state, reason, resync, cumulative
client skips, and read/fanout/send clocks. SSE IDs order this API's delivery only;
producer run IDs are opaque identities, never sortable clocks.

Subscribe acknowledgement precedes bootstrap and reconnect snapshot reads.
Concurrent publications are buffered with a fixed bound. A changed producer run
requires a new authoritative cache read. Same-run decision sequences reject old
or duplicate messages. A reconnect starts a new generation and resyncs current
state; `Last-Event-ID` never requests historical replay. Each client's initial
envelope declares resync, and generation/skipped metadata accompanies subsequent
surviving envelopes even when intermediate updates were coalesced.

Each client has a one-item latest-wins queue. The default cap is 16 clients, and
an actual ASGI header/body send has a two-second deadline. Slow clients cannot
block fanout or other clients. A separate expiry timer invalidates old values
even if the producer goes quiet. Heartbeats do not extend price lifetime. SSE
bypasses compression explicitly and all responses use `no-store, no-transform`.
The existing source API retains its four-key MGET and loopback binding.

The independent parser uses producer monotonic differences only within the
producer's clock domain. API reads anchor a separate monotonic expiry deadline;
re-reading the same publication cannot extend its wall deadline. A browser must
also expire a displayed price if its tunnel stalls. `remaining_ns` was measured
at the server and is **not** a new lifetime starting at browser receipt.

For conservative browser expiry, bracket server wall time using a timed GET and
the browser's monotonic request start/end, then map the absolute producer expiry
to the earliest possible browser deadline. Invalidate that calibration on wall
clock discontinuity or an incompatible later bracket. Browser delivery lead is
measured independently with `performance.now()` and needs no wall-clock offset.

## Completed bounded verification

After deterministic tests and deployment with the producer disabled, the run
used a **15-minute** browser forecast cohort with a fresh state directory,
followed by 120 seconds of continued anchor collection. Operator shutdown was
scheduled after about 17 minutes; the runtime's one-hour hard cap was unchanged.
Actual observation and stop clocks are retained in the findings. Forecasts
admitted during follow-through remain audited outside the browser cohort. No old
campaign directory, stop latch or audit record was reset or removed.

The early stop exposed a shutdown-budget limitation. `_OptionalGhostSink.close`
allows five seconds for `GhostRuntime.close`, which first joins its workers and
then allows up to another five seconds for its own audit drain. The outer budget
can expire before that work finishes. The journal recorded
`ghost_optional_shutdown_incomplete`; 64 audit rows remained nonterminal and 65
outbox files remained, including one already-terminal record. SIGTERM did invoke
cleanup, but it did not complete the drain.

A bounded, publication-free recovery pass has now reconciled that tail using the
existing recovery path, after locking and archiving the exact campaign outbox.
It preserved frozen inputs, existing target matches and acknowledged publication
evidence; unresolved targets were marked `restart_unmatched`, not filled from
later data. The producer stayed disabled and no Redis publication or new forecast
was issued. Disabled collector startup does not run this recovery automatically.
External export status remains recorded with the findings. The subsequent
reliability checkpoint fixes the shutdown budgets; continuous forecast production
still requires a reviewed capacity/retention policy.

Use the owner's browser through the SSH tunnel. Instrument EventSource on the
forwarded API origin, without adding frontend assets or broad CORS on the
droplet. Record handler-entry browser monotonic times and raw SSE data; rendering
is not claimed unless separately measured. Pair each published horizon with the
exact target stamp in later official TWAP anchors delivered to that same
browser. Unobserved, already-received or conflicted anchors are censored, not
substituted by a later target. Report matched/censored denominators and
median/p90/p99 browser lead by horizon, separately from collector/Redis lead.

The capture records API reconnects, skipped updates, expiry/availability,
snapshot timing and payload hashes. It included a bounded concurrent GET load
and a second unread stream, without fault-injecting production Redis or the
collector. Isolated tests cover reconnect/run replacement/send stalls; an unread
browser stream alone does not prove server send backpressure occurred. Stop
admissions and reconcile any retained tail before final export, then verify the
external export, empty outbox and fresh official source feeds. This short
observation does not establish permanent operation or prove reconnect retention
unless a qualifying disconnect actually occurs.

Publication profiling remains a separate checkpoint. This delivery change does
not relax durable-before-publication ordering or claim the 10 ms producer target
has been met. Rolling TWAP forecasts are not settlement-side or trading-edge
validation.
