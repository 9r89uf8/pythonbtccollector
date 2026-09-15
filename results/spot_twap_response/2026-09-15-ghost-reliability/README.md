# Ghost delivery reliability release

Runtime release `d404312dadd1748f38c91cf4911c74b5c47206ea` was pushed to GitHub
main and deployed on September 15, 2026 at 22:26 UTC. The ghost producer stayed
disabled and the read-only ghost API stayed enabled. No canary, schema migration,
retention deletion, Redis restart or change to forecast arithmetic occurred.

The release fixes internal Redis resubscription without waiting for a new
publication, isolates malformed optional ghost settings, and gives shutdown
separate preservation/drain budgets with truthful incomplete status. Five
verified inactive operator copies were archived without executing them.

## Validation and deployment

- `full_suite.txt`: 1,554 passed, 10 opt-in datastore skips, two existing warnings.
- `redis8_suite.txt`: 32 stream/TCP tests passed locally under Python 3.12 and
  the production redis-py 8.0.1 version.
- `tests.txt`: 210 API, stream, shutdown and collector/runtime tests passed in
  the droplet venv, Python 3.12.3 and redis-py 8.0.1, before restart. These tests
  use isolated peers and fake stores; production Redis was not interrupted.
- `install.*` and `restart.*`: fast-forward install, existing requirements,
  exact Chainlink unit copy, daemon reload and restart of Chainlink plus API.
  `TimeoutStopUSec=2min` verifies the installed stop budget.
- `pre_deploy.json` and `post_deploy.json`: six active services, loopback
  listeners, unchanged whitelisted settings, fresh source values, absent ghost
  key, empty outbox, unchanged three campaign hashes and 16,329 terminal audit
  rows still verified for the saved external export.
- `log_review.json`: bounded startup journal inspection, no error records;
  the one warning is the existing non-update TWAP subscription acknowledgement.
  All six deployed production/unit source hashes match the tested Git bytes.
- `operator_cleanup.json`: exact original/archive paths and unchanged hashes.

The remote restart exited successfully and its raw output and exit status were
saved. Printing its status marker then hit a local Windows console-encoding
error; no restart was repeated, and the independent post-check passed.

`MANIFEST.json` freezes pre-deployment validation and source evidence.
`DEPLOYMENT_MANIFEST.json` freezes the subsequent installation and verification
artifacts. The previous C review remains historical evidence, including its
distinction between browser expiry coverage and audit-modelled coverage.

These are bounded deployment checks, not a new live forecast accuracy or
continuous-operation test. An active-producer shutdown still needs a future
bounded run to establish its production drain time. Continuous operation remains
subject to a reviewed storage policy; the one-hour campaign cap and 1.5 GiB
admission stop remain in force. Publication latency, browser expiry gaps and the
keep-alive hypothesis are not claimed fixed here.
