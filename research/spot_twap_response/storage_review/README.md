# Ghost audit capacity and retention review — 2026-09-13

A bounded read-only metadata check at 19:43:17 UTC found:

| Quantity | Bytes | GiB, rounded |
|---|---:|---:|
| PostgreSQL database | 12,080,061,463 | 11.25 |
| Database filesystem available to ordinary users | 26,551,746,560 | 24.73 |
| Database filesystem total | 50,884,108,288 | 47.39 |

[Raw capacity](capacity.json), [script and output hashes](manifest.json). The check used `pg_database_size` and `statvfs` on the actual database directory, with a read-only transaction and five-second statement timeout. It scanned no application rows and changed no production data/configuration. Capacity is a point-in-time observation; existing collectors continue consuming space.

## Assessment of the proposed estimate

At an **assumed**, not measured, 3,000 bytes per decision and two decisions/second, storage is 518,400,000 bytes/day and 15,552,000,000 bytes/30 days before additional costs. The peer's rough half-GB/day estimate is arithmetically sound. Another table's bytes/row does not establish the new audit's size. Six horizons require 89 distinct slot timestamps, although each horizon averages 60; exact representation, shared input references, TOAST compression, indexes, target updates and dead tuples change the physical cost.

Keeping detail for 48 hours can be a valid later policy, but one detailed row/second omits evidence if more forecasts are published. Full reproducibility would then apply only to the sampled subset. Compact per-decision history also grows without bound unless it has a finite cap or expiry.

## Selected first-canary policy

The [plan](../../../GHOST_TWAP_LIVE_PLAN.md) now bounds the first run to at most 72 hours and 600,000 complete decision rows, with every publication represented. It warns at 1 GiB of total relation size and stops new ghost decisions at 1.5 GiB, reserving margin within a 2 GiB budget. A separate 10 GiB filesystem-availability floor protects headroom against growth elsewhere. In-flight writes, result updates, measurement cadence and maintenance costs must fit the reserved margin in a preflight test. These are chosen safety budgets, not a measurement that 72 hours of the proposed schema will fit.

Match targets for up to 120 seconds after decision. Retain whole rows for 96 hours, and expire only terminal, verified-exported evidence. Expiry must atomically compare the current row/result version with the verified export; a subsequent status or conflict update invalidates eligibility and requires re-export. Export to the owner's computer before expiry; keep forecasts paused if export/capacity checks fail. A cap-triggered early stop is recorded as incomplete. No automatic expansion of the budget, sampling of published decisions, permanent compact tier or reuse of retired tables is included.

The bounded canary avoids adding detail compaction and another history tier before row size is known. It preserves frozen decision/input fields until whole-row expiry; result/status fields remain versioned updates. Counts, publication identities and hashes accompany the external export; later summaries remain explicitly separate from full evidence.

`UPDATE`/`DELETE` leaves old row versions until vacuum can reclaim them for reuse; ordinary vacuum generally does not return allocated files to the operating system. Thus dropping slot JSON after 48 hours is not an immediate disk-space release, and neither byte guards nor retention are complete until maintenance and sustained space reuse are tested. Do not run blocking table rewrites as an improvised response to pressure. [PostgreSQL vacuum documentation](https://www.postgresql.org/docs/current/routine-vacuuming.html)

Only new optional ghost decisions pause on these guards. Existing collectors, official TWAP durability and the API's original source keys continue. Stopped ghost values expire; capacity checks must not block source-feed processing. This is a plan, not an enabled retention worker or a deployment change.
