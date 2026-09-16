# Ghost API and shutdown reliability checkpoint

This checkpoint addresses the three confirmed defects from the
[C peer review](GHOST_TWAP_CHECKPOINT_C_REVIEW.md): hidden Redis resubscription,
invalid optional settings preventing API startup, and the nested shutdown drain
timeout. It preserves calculation contract 4, the six forecast horizons, exact
producer bytes, input freshness policies and durable-before-publication ordering.
The producer remains bounded and default-off.

Status: pushed to GitHub `main` and deployed as runtime commit `d404312` on
September 15 at 22:26 UTC. The Chainlink collector and API were restarted; the
installed Chainlink stop timeout is 120 seconds. The API is enabled, and the
ghost producer was disabled at that deployment.

The subsequent [one-hour reliability canary](GHOST_TWAP_RELIABILITY_CANARY.md)
completed on September 15 at 23:56:38 UTC. Its shutdown drained all 223 tail
records in 6.829 seconds with no retained outbox rows or recovery helper; all
7,082 new audit rows are terminal and externally verified. Production cache
observation covered the full hour, but the browser capture missed about 49
minutes during confirmed laptop sleep and does not establish full-hour frontend delivery. The producer is
disabled again. No Redis or source reconnect was observed in this campaign;
the deliberate Redis-drop regression evidence remains the local TCP tests.

Validation: the full development suite passes **1,554 tests**, with 10 opt-in
datastore tests skipped and two existing dependency warnings. The real-client
Redis stream regressions also pass all **32 tests** on Python 3.12 with redis-py
8.0.1. The full run includes 12 new shutdown cases covering delayed and failed
storage, a 243-row tail, in-flight acknowledgements, caller cancellation and
pending late-event persistence. [Validation and preflight evidence](results/spot_twap_response/2026-09-15-ghost-reliability/)
is retained separately. An additional **210 tests passed** in the droplet's
Python 3.12.3 / redis-py 8.0.1 environment before service restart. The
[post-deployment check](results/spot_twap_response/2026-09-15-ghost-reliability/post_deploy.json)
passed: six active services, fresh source feeds, loopback-only listeners, typed
ghost GET/SSE unavailable responses, empty outbox and all 16,329 audit rows still
terminal and verified against the saved export. All three campaign hashes and
the deployed source hashes match their expected values.

## Redis resubscription

A subscription acknowledgement after the initial handshake is now treated as
lost continuity. The hub immediately emits unavailable/resync state, discards
uncertain buffered messages and rebuilds through its existing supervisor. The
new connection must acknowledge its subscription and fetch an authoritative
GET/PTTL snapshot before delivering forecasts again. No subsequent publication
is needed to recover the current cached value.

This handles both redis-py's direct reconnect path and its internal retry path.
Retry settings alone cannot cover both. New-run replacement, an absent cache,
resubscription while bootstrap is in flight and an already expired identical
payload retain their existing safety rules. Re-reading identical bytes never
renews their lifetime.

Regression tests include a synthetic local TCP/RESP peer with the real installed
redis-py client, and were exercised with the production version 8.0.1 as well as
the development version 7.0.1. No production Redis connection was interrupted.
The earlier review reproducer intentionally asserts the defect at `6c6115d` and
is retained as historical evidence. The expected-pass regression for the fixed
hub is `tests/test_ghost_twap_stream_tcp.py`.

## Invalid optional API settings

Only validation errors from `GhostApiSettings` are isolated. Invalid values,
including an invalid enabled flag, construct no ghost clients. Both ghost routes
return HTTP 503 with reason `invalid_settings`; a fixed diagnostic is logged
without rejected values or a validation traceback. Ordinary API health and
source-price routes remain available. Invalid core settings and unexpected
programming/resource errors continue to fail visibly.

Valid disabled settings retain reason `disabled`, and valid enabled settings are
used exactly as supplied. This introduces no new environment key and does not
loosen configuration bounds.

## Shutdown and operational limits

Shutdown stops admissions and new publications, then gives worker quiescence
5 seconds, final outbox preservation 15 seconds, PostgreSQL/late-event drainage
30 seconds and each resource close 5 seconds. Already accepted inputs are drained
before pending targets are terminalized; an in-flight Redis call retains its
actual acknowledgement or an explicitly uncertain cancellation outcome. A final
outbox pass attempts to save every retained decision before database drainage,
with an explicit incomplete result if that pass cannot finish. The database loop
also checks its own monotonic deadline; completion depends on empty pending
queues, rather than on cancellation alone.

The collector waits up to 70 seconds for optional cleanup. If a filesystem call
cannot finish within its stage budget, ownership is retained until it actually
settles: the spool lock and clients are never closed underneath a running write.
The outer timeout logs incomplete cleanup and stops waiting, rather than claiming
success. Stage budgets therefore bound the request to stop, not completion of
unkillable operating-system I/O. The Chainlink unit's `TimeoutStopSec` increases
from 30 to 120 seconds to cover core teardown plus normal optional cleanup and
provide the final process-stop bound. These changes affect stopping, not the feed
processing or forecast publication path.

The completion report must distinguish a drained audit from retained evidence
after a persistence failure. Existing observed targets and acknowledged
publication bytes remain intact; unobserved tail targets are explicitly unmatched.
Shutdown does not fill missing targets with later prices or restart admissions.

Continuous-production storage policy remains a separate decision. The existing
one-hour campaign limit, 1.5 GiB admission stop, verified-export requirement and
96-hour eligibility for whole-row expiry remain unchanged. This patch does not
strip constituent evidence, enable a new canary or claim that browser expiry
gaps or the keep-alive hypothesis have been resolved.

Five inactive one-off C operator copies were verified against the existing
repository evidence archive, then moved into a root-only evidence directory with
non-executable `.txt` names. No helper was executed. The campaign state, audit and
outbox were preserved; [the cleanup record](results/spot_twap_response/2026-09-15-ghost-reliability/operator_cleanup.json)
contains the exact paths and hashes.

## Deployment

After pushing the tested change to GitHub, keep the producer disabled and retain
the existing API enable flag and credentials. No schema or requirements change
is required. Copy the changed Chainlink unit before restarting either affected
service:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo cp deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo systemctl daemon-reload
sudo systemctl enable price-collector-polymarket-chainlink
sudo systemctl restart price-collector-polymarket-chainlink price-api
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -u price-api -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli EXISTS btc:live:ghost_chainlink_twap_60s
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/live
```

With the producer disabled, an absent ghost key and typed snapshot 503 are
expected. Verify the audit remains terminal and the completed campaign's outbox
is empty; a disabled startup does not reconcile old audit rows.
