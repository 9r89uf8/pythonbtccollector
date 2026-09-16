# Continuous Ghost TWAP activation — September 16, 2026

Continuous forecasting started at **2026-09-16 15:59:27 UTC**, on deployed
commit `6c6f5bf`, with run ID `4f55736e031f4184be501c538f7a61d2`.
Initial runtime checks reported `capacity_ok=true`, no stop or suspensions,
successful compaction and a working accuracy cache. This records activation
and initial health; it does not establish seven days of operation, a stable
storage plateau or a completed accuracy baseline.

The activation used environment changes only. It restarted
`price-collector-binance-futures`, `price-collector-polymarket-probabilities`
and `price-collector-polymarket-chainlink` to load their respective settings.
No code was installed and no schema migration was applied in this activation.
The existing canary evidence and state directories were preserved.

## Initial verification

[postflight.json](postflight.json), checked at
`2026-09-16T16:15:17.184070+00:00`, records 1,766 issued decisions and 1,763
Redis acknowledgements in the cached runtime health, including 122 partial
publication batches. Capacity remained available, with no stop, suspensions
or monitor worker failures. The direct PostgreSQL check found 1,608 compact
rows and 233 incomplete rows, consistent with the active target-matching tail.
The accuracy cache was available; its persisted baseline was still unset.

The live publication, minute-cached accuracy/runtime health and PostgreSQL
retention state have different observation clocks. These counts are not one
atomic snapshot and must not be subtracted to infer missing decisions or
publication failures. The JSON preserves the cache generation and maintenance
clocks alongside the outer check time.

Operator checks found all services active and no entries in the bounded
warning-level journal query beginning at 16:00 UTC. The separate local browser
displayed valid 5-, 10- and 30-second forecasts; its offline/reconnect check
cleared unavailable values and restored live display after reconnection.
These are initial functional checks, not a sustained availability measurement.

## Deployed settings

| Setting | Value |
| --- | --- |
| `GHOST_TWAP_ENABLED` | `true` |
| `GHOST_TWAP_CONTINUOUS` | `true` |
| `GHOST_TWAP_CANARY_START_MS` | `0` (ignored in continuous mode) |
| `GHOST_TWAP_STATE_DIRECTORY` | `/var/lib/price-collector/ghost-continuous` |
| `GHOST_TWAP_DATABASE_FILESYSTEM_PATH` | `/var/lib/postgresql` |
| `GHOST_TWAP_SOURCE_MAX_AGE_MS` | `5000` |
| `GHOST_TWAP_RECEIPT_MAX_AGE_MS` | `3000` |
| `POLYMARKET_EVIDENCE_WARN_RELATION_MB` | `3072` |
| `POLYMARKET_EVIDENCE_MAX_RELATION_MB` | `4096` |
| `BINANCE_MICROSTRUCTURE_WARN_RELATION_MB` | `3072` |
| `BINANCE_MICROSTRUCTURE_MAX_RELATION_MB` | `4096` |

The continuous state directory was clean at activation and is separate from
all prior canaries. Its persisted start, stop state and outbox must be reused
on subsequent restarts. The optional capture warning/cap values above are
production overrides; their code/example defaults remain different. The
`_MB` relation settings use MiB (1,048,576 bytes).

## Initial shared-disk allocation

[capacity-before.json](capacity-before.json) records the bounded pre-activation
snapshot at `2026-09-16T15:59:27.715165+00:00`. It found no incomplete audit rows
and no continuous rows yet. PostgreSQL's data directory was
`/var/lib/postgresql/16/main`, on the filesystem checked through the configured
`/var/lib/postgresql` path.

| Quantity | Bytes |
| --- | ---: |
| Filesystem free | 23,250,493,440 |
| Required filesystem reserve (10 GiB) | 10,737,418,240 |
| Existing allocated ghost relations | 318,406,656 |
| Existing allocated evidence relations | 957,276,160 |
| Existing allocated microstructure relation | 2,138,005,504 |
| Evidence cap (4 GiB) | 4,294,967,296 |
| Remaining evidence allowance | 3,337,691,136 |
| Microstructure cap (4 GiB) | 4,294,967,296 |
| Remaining microstructure allowance | 2,156,961,792 |
| Additional ghost headroom after those allowances and reserve | 7,018,422,272 |

Two conditional calculations describe different assumptions:

| Scenario | Additional ghost allocation deducted | Balance after reserve and optional allowances |
| --- | ---: | ---: |
| Fill the 6 GiB **total** ghost relation budget | 6,124,044,288 | 894,377,984 |
| Add the measured-rate seven-day **new compact** projection | 4,705,615,872 | 2,312,806,400 |

The first scenario subtracts `6,442,450,944 − 318,406,656`: existing ghost
allocation already consumes part of the total cap. The second subtracts the
entire projected new compact allocation; it does not deduct the existing
allocation again or assume that legacy evidence will be deleted. The new-data
projection comes from the
[disposable compact-storage experiment](../../spot_twap_response/2026-09-16-compact-storage/FINDINGS.md),
not seven measured live days.

These balances do not reserve additional growth in core collector history,
WAL, logs, durable outboxes or maintenance working space. The new-compact
projection also excludes additional summary/feed-health allocation; the total
ghost budget covers all measured ghost relations. Evidence metadata/control
writes can continue after high-rate quotes
pause. The relation caps are guards, not exact total-disk ceilings. Normal
deletion and vacuum can make relation space reusable without returning it to
the filesystem. Neither calculation guarantees seven days will fit; ongoing
capacity checks and the shared filesystem reserve remain necessary.

## Retention and local display

Continuous compact forecasts retain seven days and their accuracy summaries
retain ninety days, under the
[continuous operating procedure](../../../OPERATIONS.md#continuous-ghost-retention-and-accuracy).
The independent ten-day collector history timer excludes ghost tables. It may
delete expired historical 30-second TWAP events and unused parents; every
surviving event keeps its original settlement identity. Microstructure's own
daily deletion applies only to configured retention shorter than ten days.
At ten days, or with an older longer setting such as thirty days, the bounded
independent timer owns cleanup, including while optional capture is disabled.

The dashboard is a separate local project at the owner's workspace
`dist/ghost-frontend`, outside this backend checkout. It reaches the loopback
API through an SSH tunnel. No frontend assets or dashboard service were
deployed to the droplet. Browser or tunnel availability is separate from
continuous server operation, and the browser must enforce cached-price expiry.

The [earlier retention deployment](../../spot_twap_response/2026-09-16-continuous-deployment/README.md)
and dated canary findings remain unchanged historical records. See the
[main README](../../../README.md#ghost-twap--optional-worker) for current
architecture and links to those studies.
