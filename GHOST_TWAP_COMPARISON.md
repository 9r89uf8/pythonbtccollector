# Frozen ghost predictions against actual TWAP

The local dashboard shows 3-, 5-, 10- and 30-second comparisons aligned to the
target TWAP source timestamp. A horizon advances the official anchor's source
stamp; it is not a promise of exactly that much lead at the browser.

For each target second and horizon, choose the first acknowledged eligible
publication (ACK wall time, then run/decision identity for ties). Freeze that
choice before outcome checks. Later revisions never replace a missing,
conflicted or inaccurate first prediction. A scored pair must have an official
first match, valid clocks and causality, no conflict, and an acknowledgement
strictly before target receipt on that decision's monotonic clock. A tie is not
early. Monotonic clocks are never compared between runs.

Ghost MAE and held-TWAP MAE use precisely the same plotted pairs. The held
baseline is the official TWAP known at the chosen decision. Dollar arithmetic
stays Decimal, serialized as decimal strings. Missing matches, restart censoring,
conflicts, clock errors and late acknowledgements remain counted; reason counts
may overlap. "No matched print" means no first match in the original recorded
matching window, not proof a later official report never arrived.

This cohort differs from the accuracy monitor, which scores every eligible early
publication including refinements. Their MAEs need not agree. Available 3-second
windows have no future-stamped slots, but may contain pending or carried inputs;
that panel is an alignment check, not guaranteed exact reconstruction.

## Bounded data path

The continuous monitor reads `ghost_twap_compact` once per minute and publishes
`btc:live:ghost_chainlink_twap_60s:comparison` with a 180-second TTL and 4 MiB
payload ceiling. `/forecasts/chainlink-twap/comparison` performs one Redis GET,
checks metadata/expiry and returns the serialized bytes. It shares the existing
`GHOST_TWAP_API_ENABLED` flag. No new credentials, settings, service or schema.

The window contains the latest finalized 15 minutes of target source stamps.
Its end is the minimum of now minus the 120-second matching age, the database's
earliest un-compacted continuous decision, and the runtime's pending persistence
watermark, minus a further five-second source-age allowance and floored to a
second. This prevents a still-pending earlier publication from later changing
which prediction is first. The end is at least 125 seconds behind now and can
lag farther during recovery; the page displays its exact UTC range.

A single MVCC statement reads the database watermark and candidates via the
existing creation-time indexes. Candidates include 30 seconds before the target
window and five seconds after it. The worker refuses more than 10,000 records
or 32 MiB of source bodies instead of silently exporting an incomplete cohort.
It verifies compact hashes and computes/serializes in the existing CPU worker.
Chart failures are isolated from live forecasts and accuracy-monitor status.

The local frontend shows shared price/time scales and a shared symmetric dollar
error scale. It retains stale historical pairs with an explicit stale label
during connection loss. Data comes from saved records even after laptop sleep.
The historical response can use HTTP gzip; the live SSE path remains unchanged.

## Verification

The focused chart/cohort, cache/delivery and existing API checks pass: 60 tests.
The read-only production `EXPLAIN (ANALYZE, BUFFERS)` on September 16 selected
1,822 candidate records in 15.812 ms using both the compact age index and the
continuous watermark index. This query-plan measurement excludes transferring,
decoding and composing the bodies. A separate 1,800-row sample averaged 7,580
bytes per compact body (maximum 7,659), comfortably within the read budget.
No full suite or new forecast canary was required for this read-only view.

## Droplet update

Run after this change is pushed to GitHub. This is code-only: preserve the
enabled continuous flags, state directory, all production credentials and
retention settings. No schema application or environment edit is needed.

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink price-api
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/comparison
sudo journalctl -u price-collector-polymarket-chainlink -u price-api -n 60 --no-pager
```

The comparison route can return 503 before the monitor's first refresh. An
unavailable or expired cache does not authorize resetting state or extending a
forecast lifetime. Collector restart may briefly rewarm the live forecasts;
saved finalized comparisons remain independent of that warm-up.
