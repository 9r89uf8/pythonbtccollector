# Ten-day collector history retention

Owner request: expire non-ghost collector history after a maximum of ten days.

Implementation: `d3a5877`; final deployed runtime: `789792b`.
The recurring `price-collector-retention.timer` invokes a separate PostgreSQL
peer-authenticated maintenance worker, with 2,000-row transactions and a
45-second work budget. No collector/API permission was broadened. Expiry is
limited to explicitly named history tables; catalogs and all ghost tables are
excluded. Retained observations keep their necessary session/payload references.

Market-based history expires in whole five-minute groups, rounding the ten-day
cutoff up, so a row can expire up to five minutes early. Raw receive-time rows
use the exact cutoff; the existing shorter raw-capture policy remains in effect
when enabled. Backfill, resolution selection and evidence recovery honor the
same rolling market floor. The normal microstructure writer delegates ten-day
cleanup to the timer to avoid an unbounded startup deletion.

Validation:

- 163 targeted local tests passed; the opt-in PostgreSQL case was skipped there.
- All three retention tests passed in 1.08 seconds against a newly provisioned,
  schema-complete disposable PostgreSQL database on the droplet. The lifecycle
  case checked child/parent deletion, optional retired tables, raw partitions
  sharing ctids, exact/rounded cutoffs, OI source-window age, shared references,
  fresh orphan payloads, unchanged ghost bytes and idempotence.
- The two small CLI/policy checks passed after adding the catch-up batch option.
- After optimizing the catch-up schedule in `895c897`, the same three retention
  tests passed again in 3.30 seconds in a fresh disposable database. The final
  read-only minimum/index check in `789792b` was verified against production.
- The disposable database and worktree were removed after the check.

Deployment began at 2026-09-16 15:19 UTC. Populated evidence and retired-shadow
indexes were prebuilt concurrently and verified valid, then the schema was
applied. The microstructure environment was reduced from 30 to 10 days. All four
collectors and the API were restarted; the retention timer was enabled. Ghost
production remained disabled, and the four live prices remained fresh.

The first 45-second pass completed without errors. A separate one-time catch-up
used the same repository CLI, a 900-second limit and at most 10,000 rows per
transaction. It was deliberately restarted when `895c897` stopped rescanning
drained leaf tables and deferred parent checks until their children had drained.
Each previously committed deletion batch remained durable.

Initial post-deletion scans and a parent cleanup exceeded their short timeouts
while PostgreSQL vacuumed and refreshed statistics. No cutoff or transaction
limit was relaxed. `789792b` makes simple backlog checks read the indexed minimum
instead of allowing stale selectivity estimates to choose a large sequential
scan. The normal timer then completed remaining work successfully.

At **2026-09-16 15:35:55 UTC**, the bounded worker finished with **no errors and
no eligible expired rows in any of the 24 checked tables**. An independent CLI
inspection agreed. The oldest remaining market starts at **September 6,
15:40 UTC**, within the documented rounded ten-day window. The regular timer
remains enabled; its advisory lock prevents overlapping cleanup.

The [postflight record](postflight.json), captured at 15:37 UTC, confirms all
four collectors and the API active, API/database healthy, and all four live
receive ages below one second. The ghost audit still has **23,411 rows**, version
sum **245,639**, and exactly its original latest-update timestamp; compact/hourly
counts remain zero. Ghost production remains disabled. The failed historical
catch-up-unit state was cleared after the final successful normal pass.

Deletion makes storage reusable through normal vacuuming; this rollout does not
run `VACUUM FULL`, increase quotas, or promise an immediate filesystem shrink.
Root-only deployment logs and the preserved environment backup are under
`/var/lib/price-collector/deployments/collector-ten-day-retention-d3a5877`.
