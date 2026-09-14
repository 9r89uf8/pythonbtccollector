# Freshness/expiry deployment

The owner approved release after the independent review. GitHub `main` was
fast-forwarded from `2cc0436` to **`a7b0cbf`**, then the droplet pulled that exact
commit with `sudo -u pricecollector git pull --ff-only`. Requirements were already
satisfied; no schema migration or systemd-unit update was required.

Only `GHOST_TWAP_SOURCE_MAX_AGE_MS=5000` and
`GHOST_TWAP_RECEIPT_MAX_AGE_MS=3000` were added to the existing collector env file.
The update preserved unrelated settings and owner/mode, with a private backup.
`GHOST_TWAP_ENABLED=false` and the previous canary's start, state and deadline stop
latch remain unchanged. The production sparse checkout was preserved.

`price-collector-polymarket-chainlink` restarted at **2026-09-14 03:21:54 UTC**.
Post-restart checks at 03:23 UTC confirmed:

- The running process has the two new limits and ghost disabled.
- Chainlink spot and official TWAP are fresh, received after the restart.
- Both `/healthz` and `/markets/current/live` return HTTP 200; the database is healthy.
- Ghost's Redis key is absent. The stopped campaign file's SHA-256 is unchanged.
- All 7,292 previous audit rows remain, with zero incomplete rows and no new campaign.
- All six services are active. Only the Chainlink collector PID changed.
- API, Redis and PostgreSQL still listen only on loopback.

The bounded journal sample contained one warning for skipping the subscription
acknowledgement, followed by normal official-feed activity; it contained no error
or critical entries. The [deployment evidence](deployment.json) records the
checks, exact source hashes verification, selected non-secret process settings
and campaign preservation. This documentation commit changes no runtime code
and needs no additional service restart when pulled.

This installation does not measure live ghost coverage. The next implementation
checkpoint remains per-horizon batch eligibility, followed by a separately
authorized short canary to measure both changes together.
