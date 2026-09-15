# Reconnect recovery: review and completed deployment

**The reviewed reconnect implementation was pushed to GitHub and installed on
the droplet. Ghost remains disabled.** Runtime code commit:
`f575d1170810b4285e6e6a6ce537624ce3b84254`, contract 4 / `ghost-canary-v6`.
The Chainlink service restart began at **2026-09-15 01:18:58.716 UTC**;
initial verification completed at **01:19:04.602 UTC**.

The patch was verified against all 24 committed checkpoint artifact hashes
before publication. GitHub `main` advanced from `2256320` to `f575d11`, and the
review branch was published. The clean production checkout on `main` advanced
from `770df37` with `sudo -u pricecollector git pull --ff-only`. The unchanged
requirements were installed using the production virtual environment. Only
`price-collector-polymarket-chainlink` was restarted.

## Verification

- The deployed engine, runtime and observer file hashes match the reviewed
  implementation. An import check reports contract 4, `ghost-canary-v6` and a
  10,000 ms reconnect bound.
- All six services are active. The other five service PIDs are unchanged.
- Both Chainlink spot and official sixty-second TWAP received fresh values
  after the restart. All four source cache keys were fresh at verification.
- `/healthz` reports API/database health, and `/markets/current/live` responds.
- `GHOST_TWAP_ENABLED=false`, the ghost key is absent, and the new service's
  startup journal contains no `ghost_runtime_started` event.
- The environment and sparse-checkout configuration are unchanged. Both
  completed campaign state files retain their pre-deployment hashes and have
  empty outboxes. Their fixed start times and deadline stops remain intact.
- The audit still contains 14,346 rows with zero incomplete records. No schema,
  systemd unit, environment setting or campaign was changed.

[Exact deployment evidence](DEPLOYMENT.json), copied from the operation record
under `/var/lib/price-collector/ghost-reconnect-deploy-20260915T011853Z`, records
the clocks, hashes, PIDs, cache ages, health and audit counts. The source JSON's
SHA-256 is `50b00a9a29e670ac168db33dad8c6e74f8bce987c47c62623b0018e6b5f8ff6c`.
The original [checkpoint](../../../GHOST_TWAP_RECONNECT_CHECKPOINT.md) remains
the implementation/test record at `f575d11`; this file records its later deployment.

## Corrections to the external review

The patch is correct and the review raises no deployment blocker, but these
distinctions matter:

1. The publication fence blocks older **pending attempts**, including candidates
   awaiting fsync. It cannot cancel an application Redis attempt already begun.
   Actual attempt, acknowledgement and target ordering remain recorded.
2. Old five-second forecast availability returned at **22:48:38.153780110 UTC**,
   not 22:48:36. The new replay cutoff is **22:47:40.569607072**, a gain of
   **57.584173038 seconds**. These are calculation cutoffs, not live delivery.
3. `connection_end` includes abnormal disconnects. The recorded incident was
   `ConnectionClosedError(None, None, None)`; qualification depends on freshness,
   source advancement and bounded clocks rather than a clean WebSocket close.
4. The accepted-event idle deadline is ten seconds. Such a long silence normally
   fails the three-second pre-gap receipt-freshness gate and still clears history.
   Receipt silence and source-slot gaps are distinct measurements.
5. A periodic two-hour relay closure is not established by this incident. Its
   spot subscription lasted about 13 minutes 31 seconds before the connection
   error. The saved one-hour evidence cannot establish a universal schedule or
   predict the next close. A next canary should record whether recovery is
   actually exercised rather than assume its timing.

The supplied Python 3.12 count describes a different subset. Our recorded checks
remain 1,380 full-suite passes (10 opt-in skips), and 433 standalone Python 3.12
passes (two opt-in skips), with exact commands and exclusions in the
[validation record](../2026-09-15-reconnect-recovery/VALIDATION.json).

No new canary was started. This deployment does not establish live coverage for
the reconnect policy. API/SSE implementation and browser delivery validation
remain Checkpoint C.
