# API delivery review — 2026-09-13

The owner confirmed that the frontend will run on their computer through an SSH tunnel. The [live plan](../../../GHOST_TWAP_LIVE_PLAN.md) therefore selects SSE push with a GET snapshot fallback. This is a design review and two read-only benchmarks of the existing endpoint; no ghost runtime or stream endpoint has been implemented.

## Measured baseline

Both runs used `/markets/current/live`, one warmup and a persistent HTTP/1.1 connection. Durations start before sending the request and end after reading the entire body, using the client's monotonic clock. No price bodies were retained. Percentiles use linear interpolation over integer-nanosecond measurements, converted to Decimal milliseconds.

| Path | Measured requests | Schedule | Median | p90 | p99 | Range |
|---|---:|---|---:|---:|---:|---:|
| Droplet loopback | 50 | 10/second | 2.481 ms | 4.455 ms | 5.748 ms | 1.903–6.104 ms |
| This Windows computer → temporary SSH tunnel → droplet loopback | 10 | Sequential | 578.060 ms | 594.858 ms | Not reported | 550.805–609.379 ms |

The loopback run started at 19:22:55 UTC and completed in 5.017 seconds. The tunnel helper started at 19:25:53 UTC and completed in 11.147 seconds including tunnel startup, warmup and cleanup. Every measured request returned HTTP 200; payloads were 840–847 and 843–847 bytes respectively. The temporary tunnel process exited and its local listener was verified closed. No production files, configuration, services or database rows changed.

[Loopback results](result.json) and [code/output hashes](manifest.json); [tunnel results](tunnel_result.json) and [code/output hashes](tunnel_manifest.json). These small samples describe those runs only. They do not establish the future ghost route's latency, CPU utilization, capacity, browser rendering, one-way network time or price freshness. Do not halve the tunnel result and call it measured push latency.

The peer's 3.1 ms loopback observation is consistent with a server path taking a few milliseconds. Their 2–3 ms ghost estimate and 3% CPU claim remain unmeasured. Their 150–400 ms estimate concerns update delivery, while this tunnel benchmark measures a whole request/response: these are different quantities. Neither a browser delivery bound nor an SSE lead guarantee follows from this benchmark.

## Connection-reuse recheck

The peer suggested that the 578 ms response included a fresh connection per request because a direct TCP probe took about 300 ms. A second diagnostic at 19:45:18 UTC instrumented actual `HTTPConnection.connect()` calls, socket identity, local port, response framing and stage timings:

| Measurement | Result |
|---|---:|
| Direct TCP connection time to SSH port 22, five probes | Median 362.234 ms; range 277.671–382.250 ms |
| HTTP connect calls for warmup plus ten measured requests | **1** |
| Socket objects / local ports throughout the HTTP run | **1 / 1** |
| Full HTTP response, ten measured requests | Median 597.433 ms; p90 613.248 ms |
| Request start through headers | Median 597.127 ms |
| Headers through complete body | Median 0.047 ms |

All responses were HTTP 200 with explicit Content-Length, no Transfer-Encoding and `will_close=false`; the client socket had TCP_NODELAY enabled. The same socket remained open throughout. [Detailed traces](reuse_review_result.json), [code/output hashes](reuse_review_manifest.json). The original benchmark artifacts remain unchanged and the temporary tunnel was cleaned up.

**Repeated HTTP connections do not explain the observation.** Almost all the measured time preceded response headers. This diagnostic does not localize that time among SSH/network scheduling, queueing or other transport behavior, and it does not prove the cause of the extra time relative to the separate TCP-connect probes. TCP-connect duration is a connection-establishment measurement, not a matched measurement of one-way application delivery. Round-trip symmetry cannot simply be assumed. [One-way-delay measurement considerations](https://www.rfc-editor.org/rfc/rfc7679.html)

For a correctly matched forecast and official report:

```text
browser lead = collector receipt lead + official delivery delay − ghost delivery delay
```

If both delivery delays are equal, they cancel in relative on-screen lead. This is a conditional identity, not proof of equal delays on different events or paths. Source horizon h is also distinct from remaining receipt lead at the decision. Thus a three-second source horizon does not guarantee exactly three seconds on screen. Polling the official feed while pushing the ghost could artificially increase apparent lead; use comparable delivery and preserve skipped-target missingness.

The suggestion that moving the droplet closer to the user is the only remedy is too strong. Transport/client overhead remains unlocalized, and moving the collector can change its path to upstream feeds and any execution venue. This feature has not measured actionable lead over other participants. No relocation or execution architecture is part of this plan.

## Delivery choices and implementation checks

- **Publish immediately when idle.** Coalesce only bursts/in-flight work; a mandatory 100 ms interval can delay an update and is incompatible with a universal sub-10 ms publication target. Keep PostgreSQL audit work off the publication path. Measure the actual accepted-event rate rather than assume exactly two changes per second.
- **One serialized payload, one publisher.** The dedicated worker sends identical bytes to the snapshot key and proposed channel `btc:live:ghost_chainlink_twap_60s:updates` through a small `SET`/`PUBLISH` script. Validate arguments before mutation; no price arithmetic or JSON parsing in Lua. Script isolation is not rollback, and lost acknowledgements can cause duplicate retries. [Redis scripting](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- **One subscriber per API process.** Use an independent long-lived subscription connection with explicit reconnect/liveness policy; the existing ordinary Redis client's 250 ms socket timeout must not become the stream's idle lifetime. Wait for subscription acknowledgement, then GET/bootstrap and reconcile buffered publications. Producer run IDs plus sequence numbers identify states; unfamiliar runs trigger authoritative resync. API access is GET/subscription-only for the exact key/channel. Existing configuration has no Redis credential/ACL settings: any role enforcement requires explicit configuration work, not an assumption that an ACL already exists. [Redis ACLs](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/)
- **Bounded fan-out and honest recovery.** Register/seed clients without a race; retain only the newest pending state for a slow client and expose skipped counts. Disconnect clients exceeding the send deadline. Pub/Sub has at-most-once delivery, so resync after reconnect and expose gaps; `Last-Event-ID` is an ordering/deduplication hint, not a durable replay promise. Timers emit unavailable states after expiry even without new events. [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/), [SSE specification](https://html.spec.whatwg.org/multipage/server-sent-events.html)
- **No serialization or buffering delay added per client.** GET returns checked original JSON bytes; subscriber validation/framing happens once per publication. Use no-store/no-transform headers. Verify the installed Starlette version excludes SSE from the current global gzip middleware, and test stream flushing through the local frontend proxy. Dependencies are unpinned, so current documentation alone is not proof of installed behavior. [FastAPI response handling](https://fastapi.tiangolo.com/advanced/custom-response/), [Starlette middleware](https://starlette.dev/middleware/)
- **Local browser access.** Keep a persistent loopback-bound SSH tunnel. A separate local frontend can proxy the forwarded API on its own origin. Direct access from another localhost port needs a narrowly scoped CORS decision; do not enable public exposure or wildcard permissions. SSE sends an initial snapshot and subsequent updates; GET remains useful for diagnostics and fallback. Fallback polling should avoid overlapping requests and reject older generations.

Push removes the wait for another poll and the per-update request leg after the stream connects. It still incurs downstream transport and processing. “Half the poll interval” assumes polling phase is roughly uniform; it is not the full browser freshness delay. The future canary must measure receipt-to-Redis, API fan-out and actual browser receipt separately, including stalls, burst load and p90/p99 tails. Browser-visible lead is measured by matching ghost and official target arrivals on that same browser clock; missing target deliveries are censored.
