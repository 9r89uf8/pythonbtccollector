# Continuous retention release deployment

Deployment completed September 16, 2026 UTC. Runtime release
`4529b5a162ad7a83609baaf7e0c359ab3df2c1e5` was fast-forwarded to GitHub `main`
and pulled into `/opt/price-collector` from the previous `5d6083c` release.

The production requirements install succeeded. A schema-only backup and bounded
migration logs are retained in the root-only directory
`/var/lib/price-collector/deployments/ghost-retention-20260916T063224Z`.
The complete schema transaction committed before either affected service
restarted, with a five-second lock timeout and sixty-second statement timeout.

`price-api` restarted at 06:32:58 UTC and
`price-collector-polymarket-chainlink` at 06:32:59 UTC. Requirements and systemd
units were unchanged; no other services were restarted.

## Observed installation checks

- All six production services were active after deployment.
- `/healthz` returned HTTP 200 with database health `ok`.
- `/markets/current/live` returned HTTP 200 with current Binance spot,
  Chainlink spot, official TWAP and futures observations.
- Both ghost producer flags were `false` in the running Chainlink process;
  `GHOST_TWAP_CONTINUOUS=false` was the sole new environment key. The existing
  canary start and state directory were preserved.
- The running API kept `GHOST_TWAP_API_ENABLED=true` and had no `DATABASE_URL`
  writer variable. API, Redis and PostgreSQL listeners remained loopback-only.
- `/forecasts/chainlink-twap/accuracy` returned the expected HTTP 503
  `no_accuracy_summary`; `/live` returned `no_current_publication`. Both had
  `Cache-Control: no-store, no-transform`. Neither ghost Redis key existed.
- The old audit held **23,411 rows and zero incomplete rows**, unchanged before
  and after migration. The transactional retention counter agreed. The new
  compact, hourly and feed-health tables were empty; expired-recovery count was
  zero.
- The writer could read the new tables but could not directly update or delete
  them. It could execute the bounded expiry helper; the reader could not.
- The deployment command's final log-tail display initially received a stray
  Windows carriage return in its filename. This happened after successful
  migration, restart and audit-count comparison. The subsequent read-only check
  read the correct log and confirmed `COMMIT`; no mutation was repeated.

This establishes installation with the producer disabled. It does not claim
live compaction, seven-day expiry, accuracy baseline formation or continuous
forecasting has run. No additional test suite or canary was launched.
