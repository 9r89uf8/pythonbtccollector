# Batch-eligibility deployment

The owner's reviewed release request was completed on September 14, 2026 UTC.
GitHub `main` was fast-forwarded from `25236ab` to **`4585349`**, then the droplet
installed that exact runtime commit using `sudo -u pricecollector git pull --ff-only`.
All requirements were already satisfied. No schema migration, unit update or
environment edit was needed.

The Chainlink collector restarted at **21:53:26 UTC**, PID `1371627`, with
`ghost-canary-v5` installed. Post-restart checks at **21:55:09 UTC** confirmed:

- Fresh official Chainlink spot and TWAP arrivals after restart.
- HTTP 200 from both `/healthz` and `/markets/current/live`, with database healthy.
- Ghost disabled, source age 5,000 ms and receipt age 3,000 ms in the running process.
- The collector environment file is byte-identical to its pre-update state.
- The old campaign file and deadline stop latch are byte-identical; old outbox is empty.
- All 7,292 previous audit rows remain, with zero incomplete rows.
- All six services are active; only the Chainlink collector PID changed.
- Ghost's Redis key is absent; API, Redis and PostgreSQL remain loopback-only.

Ten reviewed release hashes matched the installed Git objects, and all six
included sparse-checkout artifacts matched their on-disk bytes. No research
checkout or production test workload was added. The previous 1,213/10 and 219
test results remain the validation for this unchanged runtime commit. The peer's
15 test functions correspond to 34 parameterized cases.

The bounded journal sample contained one warning for skipping a subscription
acknowledgement and no errors or critical entries. Canonical feeds were active
afterward. Exact observations are in [deployment.json](deployment.json).

The [combined-canary handoff](CANARY_HANDOFF.md) now documents old-spool
reconciliation, external evidence verification, a unique new state directory,
fixed start/deadline and end-of-run drain. It preserves the old directory rather
than moving or deleting it. A scorer that checks attempted membership and a
timed local Redis coverage observer still need implementation and review before
that later run is enabled. No new campaign was started.

This follow-up documentation commit changes no runtime code and requires no
additional service restart when pulled.
