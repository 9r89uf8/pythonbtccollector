# Ghost TWAP freshness and expiry checkpoint

Implemented and validated offline on September 14, 2026 UTC. The ghost remains
default-off. This checkpoint does not start another canary or establish live
coverage under the new policy. It follows the
[completed canary review](GHOST_TWAP_CANARY_REVIEW.md).

## Change and contract

The selected Chainlink spot and official TWAP now have separate age limits:

| Setting | Default and runtime maximum | Applied to |
|---|---:|---|
| `GHOST_TWAP_SOURCE_MAX_AGE_MS` | 5,000 ms | Decision wall time minus source timestamp |
| `GHOST_TWAP_RECEIPT_MAX_AGE_MS` | 3,000 ms | Both wall and monotonic time since receipt |

Either setting can be tightened. Each feed must pass all three checks, with
inclusive age limits. A source timestamp that was in the future at its original
receipt remains invalid even after waiting. Source disorder, conflicts, gaps,
already-received targets and missing-history checks retain their existing rules.

The engine policy replaces `current_max_age_ms` with `source_max_age_ms` and
`receipt_max_age_ms`. New live/audit payloads use **contract 3**, and the runtime
uses **`ghost-canary-v4`**. Both limits are frozen in each decision's policy.
The model identity, Decimal precision/rounding, sixty-slot calculation, six
horizons, pending/carried categories, quality mapping and ten-second historical
carry limit are unchanged. Existing frozen audit JSON and its hash remain
unchanged; recovery does not reinterpret them using the new policy. Mutable
reconciliation state can still receive a new version.

For each of the two current inputs, let S be its source time, Rw/Rm its receipt
wall/monotonic times, and Dw/Dm the decision wall/monotonic times. In a common
nanosecond unit, the three deadlines are:

```text
S  + source_limit
Rw + receipt_limit
Dw + receipt_limit - (Dm - Rm)
```

`valid_until_wall_ns` is the earliest of all six deadlines. A current-input or
decision-level rejection cannot acquire a future deadline; per-horizon
unavailability can coexist with a future cache deadline. Runtime expiry and Redis TTL consume
that deadline. Equality passes the engine's age comparison but leaves no positive
publication lifetime: publication at or after expiry is refused. Repeated
snapshots of the same inputs cannot renew their lifetime. The separate
monotonic elapsed-time check remains active during publication.

## Replay findings

The input is the original verified 326,727,434-byte canary export, SHA-256
`d7536bcafcf62170abbf9329f6b720d7df35fb710b9f79f64ec96ca2bda05b80`.
The reconstruction contains every accepted sequence through every saved decision
cutoff, including actual wall and monotonic receipt clocks. No later-received
input enters a calculation. The old 3s/3s policy reproduces all **7,292 decisions,
43,752 horizons and 7,292 expiry deadlines**, with zero mismatches. An independent
slot calculation also matches all 43,035 available candidate forecasts.

On the same saved decision times:

| Warm-up excluded | Decisions | Any horizon unavailable, original | New policy | Recovered decisions |
|---|---:|---:|---:|---:|
| First 60 seconds | 7,174 | 771 (10.75%) | 18 (0.25%) | 753 |
| First 65 seconds | 7,163 | 761 (10.62%) | 8 (0.11%) | 753 |

These are offline eligibility counts. They do not measure new timer scheduling,
Redis-key coverage, batch rejection or browser availability. Existing eligible
forecasts keep exactly the same prices. All 753 newly eligible decisions were
withheld under the original policy.

For each horizon, 35 of the 753 new forecasts lack a completely observed
120-second grading window and are excluded from accuracy scoring. Among the
remaining 718, missing exact targets are reported rather than filled. The table
uses the first exact subsequent official target and compares the forecast with
keeping the current official TWAP unchanged, on the same matched pairs.

| Source horizon | Matched / missing / incomplete | Median / p90 absolute error (bp) | Beats unchanged TWAP |
|---|---:|---:|---:|
| 1 s | 568 / 150 / 35 | 0.046005 / 0.143263 | 51.4% |
| 2 s | 648 / 70 / 35 | 0.027695 / 0.136252 | 94.4% |
| 3 s | 642 / 76 / 35 | 0.016188 / 0.131749 | 97.0% |
| 5 s | 661 / 57 / 35 | 0.016875 / 0.122560 | 97.4% |
| 10 s | 678 / 40 / 35 | 0.044421 / 0.175352 | 95.3% |
| 30 s | 705 / 13 / 35 | 0.422379 / 0.990955 | 77.7% |

The newly eligible 5/10/30-second median dollar errors are **$0.130 / $0.341 /
$3.244**, with p90 errors **$0.941 / $1.344 / $7.603**. This supports further live
evaluation of the relaxed source limit at the user's main horizons. It does not
establish equal accuracy under all freshness conditions: for example, the new
five-second group has higher errors than the previously eligible group.

The new one-second group is considerably weaker. Its matched target arrives a
median **183 ms** after the saved decision, with only **31 ms** at p10, before
any hypothetical publication work. All those matched targets already have source
timestamps in the past, although they have not arrived yet. No Redis or browser
lead is credited to these unpublished forecasts. The broader source allowance
does not make every horizon equally useful.

The accepted-event sequence is incomplete only in the post-deadline drain. The
grading rule therefore excludes a decision unless its entire 120-second target
observation window lies within the complete sequence prefix. Forecasts overlap
and reuse targets; this hour is not a set of independent trials or a profitability
test. Full cohorts, matching rules, exact values and reproduction instructions
are in the [replay report](research/spot_twap_response/freshness_expiry/README.md),
[summary](research/spot_twap_response/freshness_expiry/results/summary.json) and
[provenance manifest](research/spot_twap_response/freshness_expiry/results/manifest.json).

## Verification and remaining work

- Full development suite on Python 3.9.5: **1,179 passed, 10 skipped**. The skips
  are opt-in datastore integration checks; schema, store and Redis script are
  unchanged in this checkpoint.
- Ghost calculation/runtime/spool/deployment subset on Python 3.12.0:
  **185 passed**. This is the production Python version, not a full API suite.
- Research replay tests: **12 passed**; original canary replay and independent
  candidate calculation: **zero mismatches**.
- The original frozen hour still matches all **21,600** expected forecasts under
  explicit 3s/3s settings; no historical fixture was regenerated.
- New tests exercise each feed and clock at the boundary, competing deadlines,
  unchanged-input expiry, future-clock rejection, serialized settings and Redis
  TTL/post-spool expiry. The [validation record](results/spot_twap_response/2026-09-14-freshness-expiry/validation.json)
  binds implementation, tests, documentation and results to exact file hashes.

Batch eligibility and durable-before-publication ordering remain unchanged.
The next implementation checkpoint is per-horizon publication eligibility so an
arrived short target does not discard still-valid longer forecasts. Publication
profiling follows. A subsequent bounded live run must measure the combined
policy's coverage and accuracy; SSE and browser delivery remain Checkpoint C.

## Droplet update

No droplet change was made for this checkpoint. After the reviewed change is
pushed to GitHub and reaches the droplet's tracked branch, use the
[freshness upgrade procedure](OPERATIONS.md). No schema migration is needed.
Review/add the two settings above in the existing collector environment and keep
`GHOST_TWAP_ENABLED=false`. Preserve the completed campaign's start, state and
stop latch. Restart only `price-collector-polymarket-chainlink` after installation;
this upgrade does not authorize or start another campaign.
