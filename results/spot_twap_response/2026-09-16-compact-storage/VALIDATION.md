# Compact storage checkpoint validation

The experiment is complete. See [FINDINGS.md](FINDINGS.md) for the measured
results, capacity decision and limitations. Continuous production was not enabled.

- Final research code: `e8d0dd0` on `codex/ghost-audit-storage`.
- Local focused suite: **50 passed, one opt-in skip**, in 0.56 seconds.
- Same final suite on Python 3.12.3 in the disposable checkout: **50 passed,
  one opt-in skip**, in 1.08 seconds.
- Unchanged frozen encoder's separate full-canary suite: **14 passed** in
  187.30 seconds. Thirteen overlap the focused suite: **51 distinct tests**.
- Complete source export: 23,411 rows; checksum and row hashes verified before
  compact output. Selected: 7,082 decisions, 42,492 horizons, including seven
  unpublished decisions. Original export unchanged.
- Exact agreement for all 36 accuracy cohorts and 144 quarter-hour groups.
- Successful PostgreSQL run: 32 allocation checkpoints, 22 exact complete
  readbacks, both layouts' seven-day boundary/summary rollback/retry checks.
- Exact successful-report SHA-256 agreement between local and remote copies:
  `9880234883b57675d2108ac0d29e5cc2bc0c98c7bcb69e716ffdf4185b874306`.
- Both layouts' actual logged persistence, indexes and table/TOAST maintenance
  options captured in `physical_schema.json`.
- Independent agents recomputed the allocation totals, Decimal projections,
  counts, reuse interpretation and postflight capacity conclusion.
- Postflight verified all three disposable databases and their temporary paths
  removed; production commit, clean worktree and all service PIDs unchanged;
  services active, producer disabled and ghost key absent.

The first failed run and second operator-interrupted run are retained with their
stdout and diagnoses. They are not counted as completed experiments. The final
run retained the same time, size and batch bounds; no new index was introduced.

The initial GitHub metadata upload was blocked by automatic approval review.
The owner explicitly approved that disclosed upload before it occurred. Pushed
artifacts contain nonsecret capacity metadata and research evidence, not the raw
canary export or credentials.
