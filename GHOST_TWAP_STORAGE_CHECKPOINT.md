# Ghost audit storage — archive foundation

This checkpoint implements the destination-independent archive transaction.
It does **not** enable continuous production, shorten retention, delete rows,
start an archive service, or complete the interrupted browser canary. The
producer remains disabled. An always-on external destination has not yet been
selected; a local fake backend is test evidence only.

## Implemented contract

`GhostAuditStore.archive_candidates()` takes one short database snapshot of at
most 100 oldest terminal rows without current export verification. Pending rows
can coexist with the batch. There is no permanent pagination cursor: a formerly
pending or subsequently changed older row must be revisited. A partial index
supports this selection. No database connection or row lock spans network I/O.

`price_collector.ghost_twap_archive.archive_once()` then:

1. Caps canonical JSONL at 8 MiB and sorts selected records by identity for the
   existing full-row verifier. Prices and financial values stay exact strings.
2. Compresses with an empty gzip filename and zero modification time. The object
   key includes the raw JSONL SHA-256, row count and compressed SHA-256.
3. Requests create-only external storage. A pre-existing object is never
   overwritten and is never assumed correct merely because its key exists.
4. Reads the entire remote object back. Both byte sizes, both SHA-256 hashes,
   every canonical row and its frozen/result hashes must verify before any
   database acknowledgement.
5. Uses the existing exact-version/hash compare-and-set acknowledgement. Changed
   rows are reported as stale and selected again later; unchanged rows may be
   acknowledged. The archive remains valid evidence of the versions it contains.

The operation has a cooperative 60-second async timeout, a 128 MiB staging free-space reserve,
an 8 MiB raw limit, an 8 MiB + 64 KiB compressed limit, and 64 KiB download chunks.
Normal failures clean up only the operation's own temporary directory. A process
kill may leave staging debris; a later call refuses to allocate another batch
until the operator reconciles that directory. The eventual service must serialize
calls and enforce its backend's network deadlines and cancellation cleanup.
Local compression, decompression and verification are synchronous and bounded
by bytes. Blocking filesystem operations or cancellation cleanup can exceed the
async timeout; it is not a hard wall-clock process-kill deadline.

The archive adapter is a trust boundary: it must use a genuinely external
durable destination, enforce create-only writes and TLS/SSH identity checks, and
read back actual remote bytes rather than its upload buffer or an ETag. Its
credentials belong to a separate operator job. The collector and API must not
receive them. No production adapter or scheduled worker is installed here.

Before acknowledgement, retry state remains in PostgreSQL. After an uncertain
upload, a retry may encounter an existing object and must verify it again. After
an uncertain acknowledgement, it selects whatever is still unverified. An
unreferenced remote object is possible; deleting such objects needs a separate
reviewed manifest/reference policy. This checkpoint never deletes remote data.

## Why this does not yet authorize continuous operation

The latest canary added 90,710,016 allocated PostgreSQL bytes in one hour, with
7,082 decisions. A linear 96-hour extrapolation is about 8.11 GiB and 679,872
rows, exceeding both the 1.5 GiB admission stop and 600,000-row cap. These are
capacity illustrations from one hour, not a sustained-growth forecast. The
current 96-hour age rule remains enforced in both Python and the SQL trigger.

Before enabling ongoing production, the remaining implementation must provide:

- A configured always-on archive adapter and a real upload/readback failure test.
- An explicit shorter local retention policy for new continuous runs, with full
  external evidence retained and exact-version verification still required.
- Protection against recreating an expired identity from an old outbox retry.
- Row accounting that releases expired capacity while retaining reservations for
  uncommitted rows and admissions during measurements.
- Bounded expiry, restart tests and a sustained insert/update/expire/vacuum test.
  Ordinary deletion/vacuum can allow internal reuse without reducing the allocated
  relation size. The byte cap and free-space guards must remain conservative.
- Checks for the actual database, outbox and archive-staging filesystems, followed
  by a reviewed continuous-mode configuration. The persisted one-hour canary stop
  remains unchanged in this foundation.

After those checks, repeat the one-hour browser measurement on the owner's
computer with sleep disabled for that run and a maintained SSH tunnel. A browser
on the droplet would bypass the delivery path being measured.

## Deployment ordering

This foundation is not an archive-service deployment. After the reviewed change
is pushed to GitHub, its additive index must be applied before any future archive
worker is launched. Keep `GHOST_TWAP_ENABLED=false` and preserve existing env files.
The store is imported by the Chainlink collector; the other collectors and API
do not need a restart for this change.

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo -u postgres psql --single-transaction -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
sudo -u postgres psql -X -d price_collector -c "SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='ghost_twap_audit_archive_idx';"
curl --fail http://127.0.0.1:9000/healthz
```
