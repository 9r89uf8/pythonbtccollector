# Independent delivery review

Reviewed checkout `6c6115d6047c546cde13e412e83fb4d010d12501`. No production
Redis fault, service change, runtime edit, or database query was performed.

## Transparent reconnect: confirmed defect

The hub consumes the initial subscription acknowledgement, starts its reader,
and performs its authoritative GET/PTTL. Later acknowledgements are discarded
by [`GhostStreamHub._pump`](../../../price_collector/ghost_twap_stream.py#L272):
any message refreshes the connection health clock, but only `type=message`
enters the processing queue. A later `subscribe` acknowledgement therefore does
not create a new generation or force a current-cache read.

Production has redis-py **8.0.1**. Its `PubSub.parse_response` reconnects directly
when the connection is already disconnected; `_execute` can also retry a failed
read internally; `on_connect` resubscribes to the saved channels. These paths can
return a subscription acknowledgement without surfacing an exception to the hub.
The deployed default retry has ten retries with exponential jitter. The hub's
one-second outer deadline can expose slower failures, but cannot detect every
successful internal reconnect. Exact installed method source, module hashes and
retry settings are saved in [production_redis_source.json](production_redis_source.json).

The [standalone proof](../../../research/spot_twap_response/checkpoint_c_review/reproduce_delivery_review.py)
uses the installed real redis-py connection, parser and PubSub class against a
small synthetic TCP peer on an ephemeral **local loopback** port. It checks two
paths: an already-disconnected connection with the default retry configuration,
and a server-closed socket with one zero-backoff retry to eliminate timing
randomness. The latter changes retry timing only. Source clocks are fixed valid
fixture clocks; the authoritative cache is an in-memory test double.

Both paths produced the following result with Python 3.12 / redis-py 8.0.1 /
default RESP3, and also with the workspace's Python 3.9 / redis-py 7.0.1:

| Check | Before drop | After internal reconnect | After next publication |
|---|---:|---:|---:|
| Hub generation | 1 | 1 | 1 |
| Authoritative cache reads | 1 | 1 | 1 |
| Hub decision | 1 | 1 | 3 |
| Cache decision | 1 | 2 | 3 |

The real client consumed two subscribe acknowledgements over two TCP connections
while retaining one PubSub object. The missed decision 2 was never fetched. This
proves a recovery/current-state defect, not indefinite presentation of an expired
price: existing expiry still removes decision 1 at its deadline. A subsequent
publication can recover freshness without revealing the missed boundary. If no
publication follows, the hub can remain unavailable despite a valid newer cache
value until that value itself expires. The short C canary did not exercise this
failure, so its observed delivery measurements are not invalidated by the proof.

Minimal correction: any further expected-channel subscription acknowledgement
after the initial handshake must trigger the existing bounded resync path, clear
uncertain buffered messages, and require another acknowledged subscription plus
authoritative GET/PTTL before accepting publications. Unexpected subscription
loss should also fail closed. Disabling retries alone is insufficient because
`parse_response` has the separate disconnected-connection branch. Preserve the
existing deadline pinning for identical payloads and run-identity checks.

Regression tests should cover both real-client paths, a cache update lost during
the disconnect with **no later publication**, a reconnect while bootstrap GET is
blocked, and a reconnect when the cache has expired or changed producer run.
Require an observable resync/generation boundary and authoritative cache read;
do not merely assert that a later publication eventually arrives.

## Optional settings: verified startup failure

[`api.lifespan`](../../../price_collector/api.py#L324) constructs
`GhostApiSettings()` unconditionally, before initializing the ordinary API pool.
The local proof sets `GHOST_TWAP_API_ENABLED=false` together with
`GHOST_TWAP_API_MAX_CLIENTS=0`: startup raises `ValidationError` before the core
pool is reached. Thus malformed optional settings can prevent the entire API
from starting even when ghost delivery is disabled. Current lifecycle tests
check cleanup on these failures; they do not require continued core service.

This is a real failure mode, rather than evidence of a current production outage.
If optional ghost failure isolation is the contract, validate/catch ghost-only
configuration errors separately, log a sanitized reason, and keep ghost endpoints
disabled while the existing API starts. Test malformed enabled and disabled
configurations and ordinary endpoint availability. Do not swallow core settings
or database startup errors under that policy.

## Wholly unavailable GET versus SSE: intentional distinction

With a valid fresh payload, current official anchor, and all six forecast prices
null, GET reports `no_eligible_forecasts` (HTTP 503). SSE carries
`state=snapshot`, `reason=no_eligible_horizons`, and the original diagnostic
payload including that anchor. The proof reproduces this exact behavior.
It preserves stream health/anchor context and is not a wrong-price bug.
Document that `snapshot` describes a fresh producer object: clients must inspect
per-horizon eligibility and price, not use the state label as proof of an
available prediction. Unifying the routes by dropping this SSE payload would also
drop useful official-anchor observations.

## Evidence and reproduction

- [redis8_delivery_proof.json](redis8_delivery_proof.json): exact production-version
  local reconnect proof. Its two redis source file hashes match the corresponding
  installed production files.
- [local_delivery_proof_final.json](local_delivery_proof_final.json): workspace
  version reconnect, malformed-settings and GET/SSE checks.
- [production_redis_source.json](production_redis_source.json): read-only installed
  source inspection; client construction does not connect to Redis.
- `local_delivery_proof.json` was a preliminary successful run before adding the
  exact-version harness. It is superseded by the final proof files above.

Run the script with `--output` pointing to an unused JSON path. `--reconnect-only`
avoids API-specific dependencies. To reproduce the exact-version run use Python
3.12 and redis-py 8.0.1; the review installed neither into the production nor the
workspace virtual environment. The isolated wheel SHA-256 was
`47daa35a058c23468d6437f17a8c76882cb316b838ef763036af99b96cedd743`.

Primary references: [redis-py 8.0.1 PubSub source](https://github.com/redis/redis-py/blob/v8.0.1/redis/asyncio/client.py)
and [Redis Pub/Sub delivery semantics](https://redis.io/docs/latest/develop/pubsub/).
Pub/Sub does not replay messages lost while a subscriber is disconnected.
