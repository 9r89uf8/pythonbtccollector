# Ghost TWAP per-horizon publication eligibility

This checkpoint fixes the whole-batch loss identified in the
[one-hour canary review](GHOST_TWAP_CANARY_REVIEW.md). Previously, a one-second
target arriving before publication could discard valid 5-, 10- and 30-second
forecasts in the same decision. Runtime **`ghost-canary-v5`** checks each horizon
separately, including a fresh check after the durable audit write.

Implementation is complete locally; the feature remains default-off. This
checkpoint does not enable a new canary or change the droplet. The deployed
freshness revision is separately recorded in the
[deployment record](results/spot_twap_response/2026-09-14-freshness-expiry/DEPLOYMENT.md).

## Behavior

The six horizons remain in the live array in their original order. A calculated
forecast whose target has already been received has `price=null`,
`quality="unavailable"` and reason `target_received_before_publication` in that
live copy. A terminal/ineligible target without a first event uses
`target_not_pending_before_publication`. Other eligible forecasts retain their
exact original Decimal prices and metadata. Originally unavailable forecasts
retain all original fields and reasons.

For example, if the one-second target arrives while the disk write is in
progress, the final attempted array contains:

| Horizon | Attempted live price |
|---|---|
| 1 s | Null, unavailable: target already received |
| 2 / 3 / 5 / 10 / 30 s | Original calculated price, if otherwise eligible |

If every originally available horizon has become ineligible, the runtime records
`no_eligible_horizons` and makes no Redis call. The previous cache entry expires
at its existing deadline. Existing all-unavailable warmup/health messages keep
their earlier behavior and may still publish when the shared checks pass.

Shared expiry, both receipt clocks, the source-age cap, guard freshness,
suspensions/publication epochs, capacity stops and the fixed canary deadline
retain their existing checks. A target's source stamp being in the past is not
grounds for exclusion: an unseen report can still arrive later. No horizon is
extended to a newer target, recalculated with later spot, or granted a renewed
TTL.

## Contract, evidence and scoring

The pure engine and calculation **contract 3** are unchanged. Each new frozen
audit records `publication_eligibility_policy="per-horizon-unreceived-v1"`.
Runtime intent and attempted payloads add a `publication_eligibility` object:

- `version=1`;
- `checked_wall_ns` and `checked_monotonic_ns`, serialized as strings;
- `eligible_horizons`, an ordered list of integer horizon IDs;
- `excluded_horizons`, mapping horizon IDs to reason lists.

Intent selection is checked before the disk write; attempted selection is
checked afterward. `publication.payload_json` holds the actual attempted bytes
once an attempt exists. It is the single source for attempted membership; there
is no separately mutable membership list. Existing Python store transitions and
the PostgreSQL trigger already protect that payload from later changes.

Original frozen forecast prices and target observations remain intact, including
the calculated error for a forecast later withheld from publication. A matched
target is still `matched`; filtering does not relabel it as an absent forecast.
This preserves the distinction between calculated and delivered results.

Confirmed Redis lead additionally requires the exact horizon, target stamp,
price and eligible quality in the attempted payload, along with a successful
acknowledgement strictly before target receipt. Existing conflict, clock-anomaly
and causality checks still apply. An acknowledged batch alone cannot grant lead
to a withheld horizon. Targets arriving while Redis is in flight retain the
immutable attempted selection, but receive no confirmed lead if receipt is at
or before acknowledgement. Eligibility is measured at the selection instant;
it does not promise the target remains unseen until delivery.

The next canary must similarly filter its acknowledged accuracy cohort by actual
attempted membership, retaining all calculated forecasts as a separate cohort.
The historical canary scripts assume complete batches and remain unchanged;
they must not be reused unchanged for v5.

## Durability and operational scope

The original calculation and all constituent evidence are still fsynced before
Redis publication. Final selection, actual attempt clocks and acknowledgement
are recorded in subsequent audit state, following the existing ordering for
publication metadata. There is no extra fsync loop or Postgres write on the
publication path.

A crash after that first durable write but before the attempted state persists
can leave the exact transmitted subset unknown. The complete candidate prices
remain reproducible. Recovery retains the saved evidence, marks an unfinished
intent/attempt unconfirmed, never infers final membership from intent, and never
republishes it. Exact wire membership across that crash interval would require
an additional durability mechanism; this checkpoint does not claim it.

New counters distinguish attempts that withhold calculated horizons
(`publication_partial_batches`, `publication_horizons_withheld`) from batches
with no eligible calculated horizon (`publication_no_eligible_horizons`). They
are attempt/selection counters, not successful delivery measurements. The final
audit payload and outcome determine per-horizon delivery accounting.

Only `price_collector/ghost_twap_runtime.py` changes in production code. No
schema, store, Redis script, collector feed connection, environment setting or
API route changes. Source freshness remains 5,000 ms; wall and monotonic receipt
freshness remain 3,000 ms. The ghost flag and original stopped campaign stay
unchanged.

## Validation and next step

Tests cover arrival before/during the durable write, cumulative exclusions,
arrival during the Redis await, receipt/ack ties, attempted-membership checks,
original unavailability, empty eligible sets, shared guards, uncertain outcomes,
restart and frozen evidence. Exact test commands, counts and hashes are recorded
in the [validation record](results/spot_twap_response/2026-09-14-batch-eligibility/validation.json).

- Full development suite, Python 3.9.5: **1,213 passed, 10 skipped**.
- Focused ghost suite, Python 3.12.0: **219 passed**.
- New batch-eligibility file: **34 tests**, including five cases requiring
  selection metadata for current, unknown or missing runtime versions while
  preserving the known v3/v4 complete-batch interpretation.

The ten skipped tests require opt-in datastore instances; this checkpoint ran no
production test workload. The pure engine and its original 21,600-row fixture
remain unchanged. Independent review found no implementation or scoring blocker.

The original canary's 375 bounded target-only cases support the mechanism, not
a measured number of rescued publications under v5. No new coverage, accuracy,
latency or browser-lead figure is claimed by these tests. The next short canary
needs a reviewed fresh campaign, a scorer aware of attempted membership, and
wall-clock observations of cache presence to measure freshness and batch
eligibility together. Publication profiling remains separate work.

## Droplet update

After this reviewed change is pushed to GitHub and reaches the droplet's tracked
branch, run:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli EXISTS btc:live:ghost_chainlink_twap_60s
```

No schema migration or environment edit is required for the existing freshness
installation. Keep `GHOST_TWAP_ENABLED=false`; the last check should return zero.
Preserve the previous campaign start, state directory and hard-stop latch.
Installing this checkpoint does not start another canary.
