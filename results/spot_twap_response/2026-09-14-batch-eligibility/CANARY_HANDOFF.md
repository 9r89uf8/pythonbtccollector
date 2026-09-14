# Combined freshness and batch-eligibility canary handoff

This supplements [OPERATIONS.md](../../../OPERATIONS.md). It documents a later
authorized one-hour run; none of these preparation or enablement commands were
executed during the disabled batch-eligibility deployment.

## Prerequisites before enabling

The v5 scorer and wall-clock cache observer must first be implemented, tested and
reviewed. The scorer must verify exact horizon, target and Decimal price
membership in the attempted payload before including an acknowledged forecast
in the published accuracy cohort. Keep calculated-only forecasts, uncertain
publications, missing targets and conflicts separate. Filter the new campaign
explicitly; the database will still contain the first campaign's rows. Include
all runtime run IDs if the new campaign restarts.

The observer must record actual timed reads of the local ghost Redis key over
the entire hour, including warmup and stalls. Freeze its sampling interval and
report read start/end clocks, payload validity, per-horizon eligibility, absence,
read failures and missed sampling intervals. Key presence alone is not a usable
forecast: an all-unavailable health payload can exist. Report both key presence
and usable per-horizon coverage, for the full hour and a declared post-warmup
interval. Sampling estimates coverage; it does not prove exact continuous
downtime or classify timeouts as missing keys. An SSH polling loop from the
owner's computer measures tunnel availability too and is insufficient for local
Redis coverage. Start the reviewed observer before admitting new forecasts.

## Preserve and reconcile the first campaign

Keep `/var/lib/price-collector/ghost-twap` in place with its original
`campaign.json` and stop latch. Use a new unique directory for the next campaign.
Moving the old directory is unnecessary and can obscure unreconciled state.

While ghost is disabled, run on the droplet:

```bash
cd /opt/price-collector
sudo grep '^GHOST_TWAP_ENABLED=' /etc/price-collector/collector.env
sudo systemctl show price-collector-polymarket-chainlink --property=MainPID,ActiveState
pgrep -af 'price_collector.polymarket_chainlink_collector'
sudo -u pricecollector .venv/bin/python -c 'from pathlib import Path; p=Path("/var/lib/price-collector/ghost-twap"); rows=list(p.glob("*.row")); print("old_outbox_rows", len(rows)); assert not rows, "reconcile retained outbox evidence first"'
sudo -u postgres .venv/bin/python -m price_collector.ghost_twap_admin status
sudo sha256sum /var/lib/price-collector/ghost-twap/campaign.json
redis-cli EXISTS btc:live:ghost_chainlink_twap_60s
df -h /var/lib/postgresql /var/lib/price-collector
```

Require `enabled=false`, one collector process matching systemd, zero retained
old `.row` files, zero incomplete database rows and no ghost Redis key. A new
directory's lock does not exclude a worker using a different directory. If old
outbox files remain, reconcile their exact frozen content, versions and state
with PostgreSQL first; do not delete/move them to bypass recovery. Startup reads
only its configured spool, but globally reconciles incomplete database rows.

Keep the original external export and manifest. Before the new run, verify the
current database versions against externally verified evidence. The existing
download command can create and verify a fresh full export when necessary;
from the owner's development checkout on Windows:

```powershell
$ghostExport = Join-Path (Get-Location) ("ghost-before-combined-" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ") + ".jsonl")
python -m price_collector.ghost_twap_admin download --ssh root@152.42.247.86 --output $ghostExport
```

Retain existing database rows. Review total relation/index/TOAST size, row count,
tablespace and free space against the existing 1.5 GiB stop, 2 GiB budget,
600,000-row cap and 10 GiB reserve. The limits include old campaigns; a different
state directory resets none of those database guards.

## Configure the separately authorized new run

Only after the prerequisites above and the run's scope are accepted, use a new
directory beneath the service's permitted state root. This creates an empty
directory without changing or copying the old campaign:

```bash
set -euo pipefail
cd /opt/price-collector
ghost_run_dir="/var/lib/price-collector/ghost-twap-combined-$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$ghost_run_dir"
sudo install -d -m 0700 -o pricecollector -g pricecollector "$ghost_run_dir"
ghost_start_ms="$(date -u +%s%3N)"
printf 'GHOST_TWAP_STATE_DIRECTORY=%s\nGHOST_TWAP_CANARY_START_MS=%s\nGHOST_TWAP_ENABLED=true\n' "$ghost_run_dir" "$ghost_start_ms"
printf 'Fixed end UTC epoch ms: %s\n' "$((ghost_start_ms + 3600000))"
sudoedit /etc/price-collector/collector.env
```

In the existing environment file, change only those three keys to the printed
values. Preserve credentials and every other setting, including source 5,000 ms
and receipt 3,000 ms. Capture the start close to actual startup; it must not be a
future timestamp. The deadline is start plus 3,600,000 ms and includes startup
and warmup time. Record the directory, start, fixed end and deployed commit in
the run manifest. Then restart only the Chainlink service:

```bash
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink --since '2 minutes ago' -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
```

Verify the runtime's new run ID, campaign deadline, guards, observer operation
and initial warmup. Preserve that same directory and start on every restart;
never advance the clock or remove a latch to extend the run. An early cap stop
is an incomplete run, not permission to raise the cap.

## Stop, reconcile and evaluate

Let the fixed deadline stop new decisions. Leave the collector running for at
least the full 120-second matching window after the last decision, then allow
pending audit writes to drain. The measured canary coverage interval still ends
at the fixed one-hour deadline. Then set `GHOST_TWAP_ENABLED=false` and restart
only the Chainlink service, following the
existing operations procedure. Confirm the new outbox has no retained rows,
database incomplete count is zero, the ghost key is absent, official feeds are
healthy and the first campaign's files are unchanged. Export and verify current
audit evidence externally; retain both campaign directories and exports.

Use the reviewed v5 scorer and observer records together. Report actual sampled
key/usable-horizon coverage, partial batches, no-eligible batches, missingness,
matched errors, and strictly confirmed lead. Do not equate replay eligibility
gains with measured live coverage or claim browser delivery from Redis reads.
