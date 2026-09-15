# One-hour ghost reliability canary

The owner authorized this run after deployment of the Redis resubscription,
optional API settings and shutdown fixes. This protocol is frozen before
activation. `results/spot_twap_response/2026-09-15-reliability-canary/LAUNCH.json`
records the exact campaign start, deadline, directories, commit and transient
units once started. Previous campaigns, state files and audit evidence remain
untouched.

## Fixed scope and timing

Use calculation contract 4 / runtime v6, all six horizons and the deployed
source-age 5,000 ms / receipt-age 3,000 ms policies. Do not change calculations,
carry limits, capacity guards, storage policy, API settings or public bindings.
The producer runs over `[S, S + 3,600,000 ms)`, including startup and warm-up.
Do not extend the hour after a delay, guard stop or observation failure.

Arm a droplet systemd timer before activation. At the fixed end it executes the
versioned `price_collector.ghost_twap_canary_stop` module as root, with the exact
new state directory and a separate control-directory output. The helper verifies
the campaign, atomically changes only the enabled flag to false, and restarts
only `price-collector-polymarket-chainlink`. It records clocks before and after
the restart and preserves existing stop records. The module never enables,
resets, exports or expires a campaign. Do not run an archived one-off helper.

Stop at the deadline without the old extra 120-second producer waiting period.
Recent targets should still be pending so that the new shutdown path is tested.
Record actual stop-request and drain-completion clocks; systemd scheduling and
core feed teardown are not instantaneous. The existing runtime's one-hour
admission cap remains an independent backstop. A successful service restart
alone does not prove an empty outbox or terminal audit.

## Readiness and observation

Before enabling, verify the deployed sources, all six services, loopback-only
ports, unchanged previous campaign hashes, terminal/export-verified old audit,
empty old outbox, absent ghost key, relation budget and database disk reserve.
Use new unique state and control directories; never reset an earlier stop latch.
The state directory is owned by `pricecollector` with mode 0700. The bounded
observer creates its own separate output directory.

Start the current repo's `ghost_twap_observer` as a transient job before S and
require its actual Redis readiness file. It records the same fixed 36,000-bin
hour at 100 ms intervals, with its existing 128 MiB cap and read timeout. It has
no database credentials. Its interrupted 15-second disabled-producer dry run is
retained separately and is not a coverage result.

Use a dedicated Chrome session on this computer through loopback SSH forwarding.
The local browser probe captures one hour plus two minutes of observation, with
100 ms bins, a single SSE consumer and at most one snapshot GET every five
seconds. There is no extra load episode. The producer never runs longer to fill
the browser interval. Capture-start and campaign-start clocks remain distinct;
attach the exact S/run identity without rewriting previously recorded samples.
The extra browser observation permits seeing expiry after shutdown.

Report capture-relative browser coverage, independently derived campaign overlap
and coverage after first verified same-campaign object receipt plus 65 seconds
as separate panels. Include missing bins and clock-calibration uncertainty.
Use the recorded server/browser clock brackets; do not subtract unsynchronized
clocks or relabel audit-derived intervals as browser observations. Report Redis
coverage independently. Preserve exact producer bytes and use Decimal offline.

For accuracy, report all matched/censored admissions and a comparable slice
ending at least 120 seconds before actual operator stop. Tail targets unobserved
at shutdown stay unmatched. Browser lead requires an exact later target anchor
seen by that same browser; an audit match is not a browser receipt. A Redis or
source reconnect is only claimed exercised live if an actual event is recorded;
do not interrupt shared production Redis to manufacture one.

## Completion and evidence

The droplet timer ends production independently of this computer. A temporary
thread heartbeat checks meaningful failures and performs completion work after
the deadline, staying quiet on unchanged normal state. It must not reset the
campaign or restart it to fill missing observations. Export the finalized local
browser capture without overwriting files and preserve any incomplete capture.

After stop, require the disabled flag, healthy official feeds, empty current
outbox, terminal audit, absent/expired key and unchanged old campaign hashes.
Review explicit shutdown counters for drained/retained rows and late events.
If drainage is incomplete, preserve the evidence and report the failure; do not
guess targets or invoke an unreviewed recovery path. Export and verify the audit
with the existing admin download command and copy/verify observer evidence.
Run contract-4-aware scoring and exact attempted-payload membership joins; the
archived combined-canary scorer is v5/3-only and must not run unchanged.

Write the official results and limitations into this run's findings file,
including browser and Redis coverage, price errors, measured lead, publication
latency, reconnect observations, shutdown duration and retained/unmatched counts.
Keep raw large exports outside Git; retain compact results, manifests and
instrumentation provenance. Pause the temporary heartbeat after completion.

## Operator-helper installation

After pushing the tested stop helper to GitHub, run on the droplet with the
producer still disabled:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u pricecollector .venv/bin/python -m pytest -q tests/test_ghost_twap_canary_stop.py
sudo systemctl status price-collector-polymarket-chainlink price-api --no-pager
curl --fail http://127.0.0.1:9000/healthz
```

The helper is an operator CLI, not imported by running collectors. No schema,
service unit or dependency changes are needed. Activation changes only the
three existing campaign keys and then restarts Chainlink; the exact invocation
and automatic-stop command are frozen in the launch record.
