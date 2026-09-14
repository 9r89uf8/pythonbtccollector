# Ghost TWAP — Checkpoint B

Status: B release `3e1ef50` is installed on the droplet with
`GHOST_TWAP_ENABLED=false`, verified on September 14, 2026 UTC. The schema was
applied before restarting only the Chainlink collector. The API and source feeds
are healthy; the ghost audit is empty and the ghost Redis key is absent.
No live canary has started; C's API/SSE work is pending.
[Deployment receipt](results/spot_twap_response/2026-09-14-deployment/checkpoint_b.json).

The implementation was based on A release `b62285a`. Its review manifests capture
the files at `3e1ef50`, before these deployment-status edits; compare those hashes
with that Git revision. Installation does not establish prospective validation.

The peer review of `485550e` found defects in terminal-row version ownership,
transient failure handling and campaign checkpoint frequency. The correction
record is [Checkpoint B review](GHOST_TWAP_CHECKPOINT_B_REVIEW.md). Its validation
supersedes the original implementation test counts below; the original hashed
evidence remains preserved.

## Implemented behavior

- The existing canonical spot/TWAP readers offer original receipt clocks and
  accepted values synchronously before yielding. One worker drains their bounded
  queue and freezes its decision cutoff without yielding. Queue loss invalidates
  coverage, while target receipt matching happens before possible queue loss.
- The pure A calculation, six horizons, Decimal precision, slot selection and
  quality labels are unchanged. A carried interior slot still degrades quality;
  pending/future slots remain assumptions. Maximum interior carry is exposed
  alongside all five counts. No strike, side or winner is produced.
- A bounded fsynced outbox stores complete frozen inputs before Redis can receive
  a forecast. Separate workers handle publication, PostgreSQL audit and guards.
  PostgreSQL latency cannot delay the critical feed readers or gate publication.
  Redis receives identical pre-serialized bytes in its dedicated key and channel.
- One `ghost_twap_audit` row contains the frozen decision, 89 shared slots,
  deduplicated input versions, publication state and all six target results.
  The database protects immutable inputs and first matches. Mutable state has
  increasing versions and exact byte hashes; stale retries cannot replace it.
- The first accepted target is preserved. Confirmed lead requires successful
  Redis acknowledgement strictly before its receipt on the same monotonic clock.
  Ties, uncertain publication, conflicting reports and broken causality receive
  no confirmed lead. Targets received at or after decision plus 120 seconds are
  late. Shutdown/restart censors unmatched targets and never issues retrospective
  forecasts; already-established evidence is preserved.

## Bounds and restart behavior

The first capacity-canary start is explicit and fixed, with an absolute four-hour deadline and a
monotonic remaining-time bound. Persisted stop state and clock high-water marks
survive restart. Missing/invalid campaign state fails closed. An exclusive outbox
lock prevents concurrent ownership of its files. Recovery reconciles the newer
consistent disk/database version before discarding any outbox record.

| Resource | Bound or stop rule |
|---|---|
| Constituent offers | 2,048 entries; loss recorded and coverage reset |
| Active decision/result reservations | 512 complete rows |
| Serialized outbox record | 128 KiB, including a 64 KiB reservation for future result/publication metadata |
| Outbox files | 64 MiB plus one atomic-write temporary record and campaign metadata |
| Burst decision admission | At most 10 per second; idle publication has no mandatory interval |
| Audit relation size | Warn 1 GiB; stop 1.5 GiB; budget 2 GiB |
| Audit row population | Initial exact count plus all admissions capped at 600,000 |
| Database filesystem | At least 10 GiB free; missing/stale readings suspend new decisions |
| Matching | Half-open receipt interval ending at decision plus 120 seconds |
| Retention | Terminal rows aged 96 hours, with current externally verified hashes/version |

The reservation also limits result-update memory. Existing data can drain after
a stop. Events and gaps remain separate from coalesced calculations/publications;
warm-up/recovery is per horizon. Invalid event IDs and integer overflow invalidate
coverage; record growth beyond its bound or conflicting evidence stops the
optional worker. Transient I/O failures suspend admission/publication until a
fresh guard and successful audit catch-up. Recovery publishes only a newly frozen
decision. Core source collection remains independently supervised.

The outbox makes prices and their selected inputs reproducible after a crash.
It does not invent a lost Redis acknowledgement or missing target receipt. A
publication in progress at a crash remains uncertain. No exactly-once delivery
claim is made. Filesystem operations run outside the feed loop; a cancelled task
retains file ownership until its outstanding filesystem operation finishes.

## Export and expiry

The admin command streams an entire stopped/reconciled canary to the owner's
computer over SSH. It checks exact per-row financial strings/hashes, unique
ordered identities, row count and full-file hash before acknowledging any row.
Proofs come from the same verified read. Failed transfers remain `.part` files;
existing files are never overwritten. PostgreSQL acknowledges only unchanged
terminal versions. A subsequent late/conflicting result invalidates eligibility.

Expiry is explicit, limited to 100 rows per call, and atomically rechecks current
proofs and 96-hour age. A database trigger also rejects unverified/young row
deletion. There is no automatic deletion or indefinite compact history tier.
Ordinary row deletion need not immediately release allocated filesystem space.

## Verification and limits

The original B commit's full local suite passed **1,084 tests**, with five PostgreSQL tests
skipped only because they require explicit disposable-database opt-in. All
**208 ghost tests**, including those five, passed on Python 3.12 in the isolated
droplet validation environment. The production database and services were not
updated or enabled by these checks. After peer-review corrections, the full
local suite passes **1,130 tests** with ten opt-in tests skipped; all **259 ghost
tests**, including the eight PostgreSQL and two private Redis checks, pass on
Python 3.12. See the separate [review validation record](results/spot_twap_response/2026-09-13-checkpoint-b-review/validation.json).

The final package normalizes edited text to LF. A
[source comparison](results/spot_twap_response/2026-09-13-checkpoint-b/source_line_endings.json)
verifies it otherwise matches the tested code; fixture bytes remain unchanged.

The [validation record](results/spot_twap_response/2026-09-13-checkpoint-b/validation.json) lists the final test commands,
counts, source hashes and PostgreSQL probe artifacts. The accepted A engine and
fixtures remain unchanged. A's historical validation hashes refer to its release
and archived review bundle; mutable B documentation is not a new A experiment.

The actual PostgreSQL checks used a disposable database on the droplet, separate
from `price_collector`, and removed it afterward. All 208 ghost tests passed on
Python 3.12, including the five PostgreSQL cases: concurrent retries, immutable
first matches, recovery versions, export-version invalidation, locked-row expiry
and age guards. The 128 engine-shaped rows used 89 slots each and six committed
result revisions. In the final run, relation size rose from 49,152 bytes empty
to 1,433,600 bytes after updates. All 128 exported/verified rows expired; ordinary
deletion left 1,458,176 bytes allocated, while vacuum reduced the empty relation
to 188,416 bytes.

That bounded sample is not a 600,000-row or 72-hour measurement, and its initial
form omitted some runtime publication metadata. A separate complete-runtime
probe tested 64 complete decisions, all 384 target matches, a deliberately
uncertain Redis acknowledgement and a later conflict. The largest serialized
outbox row was 44,899 bytes and live payload 4,270 bytes. All 64 rows were
exported, verified and expired. The relation reached 1,974,272 bytes before
expiry/vacuum; the Redis and receipt clocks were controlled test doubles. These
figures measure this bounded test shape and recovery. Neither probe establishes a sustained
storage slope. At the observed order of magnitude and roughly two decisions per
second, linear extrapolation of this sample's net allocation reaches the
1.5 GiB guard in about 7.44 hours (about 826 MiB after four hours). The estimate
assumes a constant row/update shape and rate; it is not a measured storage slope.
A 72-hour run is not credible under that extrapolation. The revised first run
is capped at four hours to measure capacity while preserving every published
decision's complete evidence. Earlier cap stops remain possible and are incomplete
validation; the cap will not be increased automatically. Longer validation
requires a new storage review. The unchanged 96-hour minimum retention provides
no relief during this first run.

Prospective price errors, published coverage, confirmed lead, CPU/memory,
core-feed effects and event-to-Redis latency still require the bounded live
canary. The under-10-ms receipt-to-publication objective remains unverified.
Browser delivery latency belongs to C. No ghost API routes are present in B.

## Deployment boundary

Review this implementation and its measured storage limits before enabling a
canary. The default-off installation requires the new schema before restarting
only `price-collector-polymarket-chainlink`; other services and source keys are
unchanged. Manually review the four `GHOST_TWAP_*` keys in the collector environment
example. Preserve production credentials and existing settings.

See [copy/paste deployment, canary and export commands](OPERATIONS.md#ghost-twap-checkpoint-b)
and the [remaining live plan](GHOST_TWAP_LIVE_PLAN.md).
