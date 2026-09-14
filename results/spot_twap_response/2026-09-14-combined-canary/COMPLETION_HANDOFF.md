# Active combined canary: completion handoff

The owner authorized implementation, deployment, this one-hour run, monitoring,
and the final export/analysis. No further authorization is needed for the steps
below. Do not extend the run, raise limits, start C, or expire either campaign.

## Frozen run

- Code: `770df37cc0f0dbdea1138141ee67746356e9563a` on GitHub and droplet.
- Start: `1789425244002` = 2026-09-14 22:34:04.002 UTC.
- End: `1789428844002` = 2026-09-14 23:34:04.002 UTC.
- Drain no earlier than `1789428969002` = 23:36:09.002 UTC (125 seconds after end).
- Runtime ID: `9b3cf7d21f9640de989062ddd4001d5b`; initial collector PID 1372692.
- State: `/var/lib/price-collector/ghost-twap-combined-20260914T223348Z`.
- Observer: `/var/lib/price-collector/ghost-observer-combined-20260914T223348Z`.
- Transient unit: `ghost-twap-coverage-20260914t223348z`; initial PID 1372683.
- Operator records: `/var/lib/price-collector/ghost-operation-combined-20260914T223348Z`.
- First campaign remains `/var/lib/price-collector/ghost-twap`; its campaign file
  SHA-256 is `f570421579fd0156096ea522e5afd7817953e19593820fb3962ea0eed61efa86`.

The observer was ready before start. Runtime startup confirmed the exact deadline
and no stop reason. A two-minute check found all six forecasts available, no
ghost warnings/errors, healthy API, unchanged old campaign and both processes
active. CPU then was 1.3% observer / 9.0% collector; these are early samples.

## Checkout and evidence

Use only the isolated release checkout:
`C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-checkpoint-a-release`.
The main workspace has unrelated dirty work; do not stage/reset it. Development
Python is `C:/Users/alexa/PycharmProjects/polycollector/.venv/Scripts/python.exe`.
Production is the sparse checkout `/opt/price-collector`; preserve sparse rules.
SSH is `root@152.42.247.86` with BatchMode and ConnectTimeout=10.

Store new raw evidence outside Git under:
`C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-combined-canary-2026-09-14`.
Keep the existing first export under `dist/ghost-canary-2026-09-14` untouched.
The complete new export includes both campaigns; filter the exact new start.

## During the hour

Use short read-only checks of service/observer state, logs, file growth and
resources. Preserve the full fixed hour even if an early guard stops admission;
report that stop, never clear it. Avoid repeated large database scans. Do not
infer coverage or accuracy from occasional snapshots. The observer's 36,000
planned bins and verified export provide those measurements after completion.

## Completion

1. At/after the frozen end, confirm the runtime deadline stop and observer
   manifest. Wait until at least 23:36:09.002 UTC and allow the audit to drain.
   Read `ghost_twap_admin status` as postgres and require incomplete_count=0;
   require the new outbox has no `.row` files. If not drained, investigate and
   allow matching/persistence to finish; do not discard records or reset state.
2. Re-read the three ghost campaign keys from the existing env file. Require
   they still identify this exact start and state directory. Atomically change
   only `GHOST_TWAP_ENABLED` to false, preserving file owner/mode, credentials,
   every other setting and the old stop latch. Restart only
   `price-collector-polymarket-chainlink`. Confirm ghost key absent, both
   campaigns retained, old hash unchanged, audit terminal/outbox empty and
   official feeds/API healthy. Never print the full environment file.
3. From the owner's development checkout run the existing external export:

   ```powershell
   & C:/Users/alexa/PycharmProjects/polycollector/.venv/Scripts/python.exe -m price_collector.ghost_twap_admin download --ssh root@152.42.247.86 --output C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-combined-canary-2026-09-14/audit.jsonl
   ```

   This verifies every row/full hash then acknowledges current versions. Do not
   overwrite output; an interrupted `.part` needs inspection before retry.
4. Copy the completed observer directory to the raw evidence directory as
   `observer`, along with operator records. Verify remote/local hashes and run
   `python -m price_collector.ghost_twap_observer analyze --directory PATH`.
   Incomplete observation must remain explicit, never relabeled complete.
5. Read `audit.jsonl.manifest.json` (`sha256`, `row_count`). Run
   `research/spot_twap_response/combined_canary/analyze.py` with `--input`,
   `--campaign-start-ms 1789425244002`, `--expected-sha256`, `--expected-rows`
   and an unused `--output` directory under this results folder. Then run
   `python -m research.spot_twap_response.combined_canary.join_observer` with
   `--observer-directory`, `--analysis-directory`, and unused `--output`.
   This revalidates observed bytes against the campaign's attempted payloads.
   Any integrity failure needs investigation; never weaken the scorer just to
   obtain a clean result. Preserve rejected output/error provenance if found.
6. Write the official combined findings: full-hour/post-65-second coverage with
   denominators and unknown bins, per-horizon eligible/early accuracy, confirmed
   lead, publication stages, masked horizons, misses/conflicts, resource growth
   and stop reason. State that local cache observations are not browser lead or
   trading edge, and first-canary inferred gaps are not directly comparable.
   Keep raw audit/observer payloads outside Git. Hash portable summaries and
   update `.gitattributes` for any new nested evidence before committing.
7. Commit/push the bounded findings and update the official canary-results index.
   Do not restart services just for documentation. Report the result to the
   user and pause the temporary combined-canary heartbeat once complete.

There are no new API routes in this run. C and any later latency optimization
remain later checkpoints after the combined results review.
