# Ghost TWAP: bounded spot reconnect recovery

**Scope:** implement and test step 1, reconnect recovery. Runtime
`ghost-canary-v6` uses calculation/payload contract 4. The implementation is local;
ghost remains disabled on the droplet. API/SSE work and publication profiling
remain separate. No new live campaign is enabled by this change.

## Problem and resulting behavior

The [combined one-hour canary](GHOST_TWAP_COMBINED_CANARY_RESULTS.md) lost about
59/53/33 seconds of sampled 5/10/30-second availability after a brief spot
connection loss. The engine had deliberately cleared every retained spot value.
The source stamps surrounding that disconnect were four seconds apart, leaving
three unobserved source seconds. The records do not prove what prices occurred
during those seconds.

A qualified short spot `connection_end` now keeps already observed history and
clears current spot availability. Once a fresh advancing spot arrives within the
bounded recovery allowance, the engine can use those earlier observations again.
Unobserved seconds remain ordinary carried estimates, with exact counts and ages.
A three-second hole creates three carried slots wherever the target window
includes them; it never creates three supposedly observed samples.

This changes eligibility after a transport gap. The 60-slot calculation,
three-second source alignment, Decimal precision, input freshness, target
selection, future/pending semantics and publication durability are unchanged.

## Contract

The policy freezes `spot_reconnect_max_gap_ms=10000` in every decision. It is a
pure-engine policy field, with no new environment setting. Zero selects the old
clear-on-disconnect behavior for comparison. Its effective allowance is the
smaller of this value and `max_carry_ms`; neither production limit exceeds ten
seconds.

Retaining history requires all of the following at the gap:

- The feed is spot and the exact reason is `connection_end`.
- The runtime supplies actual gap observation wall and monotonic clocks. They
  cannot precede accepted data or an issued decision.
- Current spot passes the existing 5,000 ms source and 3,000 ms receipt limits
  at that boundary, including future-clock checks.
- Its source timestamp is the greatest admissible retained spot timestamp.
  A regressing latest-receipt spot cannot seed recovery.
- The engine is enabled, recovery is enabled and no causal fault is latched.

The engine then removes current spot, records `waiting`, and suppresses all
forecast prices until the first subsequent accepted spot is evaluated. That
event must strictly advance the source timestamp, be fresh and source-valid at
its own receipt, and occur after the recorded gap on both receipt clocks. Three
separate differences from the pre-gap spot must each be within the effective
allowance, inclusively: source timestamp, wall receipt and monotonic receipt.
Passing records `retained`; every requested slot still undergoes its own carry,
freshness and availability checks.

The **first** post-gap spot is decisive. A stale, future-stamped, duplicate,
regressing or out-of-bound event clears the retained history; a later favorable
event cannot revive it. A waiting snapshot also clears history once either
receipt clock exceeds the allowance from the previous spot receipt. Repeated
connection ends cannot renew the window: current spot is already absent, so the
next gap clears history. Missing gap clocks use the old clear behavior.

Sequence loss, queue overflow, capacity loss, invalid input and every other spot
gap reason still clear spot history. TWAP gap/regression/conflict handling keeps
its existing semantics. A stale TWAP blocks prices even after spot recovers.
The engine never bootstraps from PostgreSQL or reconstructs prices for decisions
already issued.

## Publication ordering and immutable evidence

`offer_gap` now immediately advances the publication epoch for either feed.
Direct input-queue overflow does the same. Older candidates cannot begin a Redis
attempt after that fence, including candidates awaiting their durable outbox
write. A later successful reconnect does not restore their eligibility.

A Redis attempt already begun cannot be revoked by a subsequent gap. Its
actual attempted bytes, acknowledgement and target-receipt order remain recorded;
the implementation does not claim cancellation or invent lead. This is the
application's attempt boundary; it does not prove that socket bytes had already
left the machine. Previously
published values retain their existing TTL until a later health/fresh payload
replaces them or expiry removes them.

Each new live/audit snapshot contains `spot_reconnect`, either null or one bounded
immutable record with the gap ordinal, actual gap clocks, pre-gap spot,
first post-gap spot when present, status and reason. Prices remain E18 strings
and nanosecond clocks remain strings in JSON. The original gap markers, operational
gap clocks, slot inputs, selected prices and prior frozen decisions are preserved.
Cleared metadata may describe an invalid first event as evidence; it is not a
fallback input. Recovery counters distinguish waiting, retained and cleared
transitions; the frozen per-decision state remains the detailed source of truth.

No schema, audit-store, spool, dependency or systemd change is needed. Real
spool/reopen/restart/export tests check that the frozen metadata and publication
bytes remain unchanged through those paths.

## Compatibility and verification

The original recorded-hour fixture, generator and manifest are unchanged. Its
21,600 forecasts retain the explicit historical 3s/3s freshness policy and
reconnect disabled. It checks ordinary calculation parity; it contains no
connection gaps and is not the reconnect test.

The archived combined-canary analyzer remains v5/contract-3-specific. Its tests
now construct explicitly legacy, gap-free fixtures instead of inheriting the
current runtime contract. They refuse to disguise reconnect evidence as old
data. A future live canary needs a scorer for the new contract; old results
must not be relabeled.

The bounded cache observer accepts only the explicit v5/3 and v6/4 pairs. It
validates the new bounded recovery metadata and availability state without
claiming to reconstruct history from a live payload. Reanalysis of old observer
manifests retains its original classifications and labels. The observer remains
a standalone bounded read-only job, not a collector import or API endpoint.

Focused recovery tests cover the actual three-slot shape, exact Decimal prices,
inclusive source/wall/monotonic bounds, policy/carry limits, first-arrival failure,
timeouts, source disorder, future history, repeated/hard gaps, clock faults,
unchanged frozen snapshots and stale TWAP. Runtime tests cover queued clocks,
before/during-fsync fences, overflow, already-in-flight ordering, and durable
evidence recovery. The full repository suite passes **1,380 tests**, with 10
opt-in datastore tests skipped, on the repository's Python 3.9.5 environment.
The standalone ghost subset also passes **433 tests** under Python 3.12, with
two opt-in Redis tests skipped. That separate local interpreter lacks `asyncpg`;
the admin, collector-integration and two PostgreSQL modules are covered by the
full repository environment rather than this standalone subset. No production
environment was changed for testing.
One pre-existing timeout assertion now allows one nanosecond of floating-clock
rounding; no financial arithmetic or administrative runtime code changed.

An independent review found a missing counter for a waiting recovery cancelled
by another gap. That counter is fixed, with repeated/hard-gap regressions. The
final review found no remaining actionable engine or publication issue.

### Recorded disconnect regression

The [recorded incident check](research/spot_twap_response/reconnect_recovery/README.md)
verified the original full-export hash once and extracted a 210-second local
fixture. It contains **413 consecutive accepted event sequences**, 414 original
decision cutoffs and the exact operational gap clocks. There are no missing
sequences. After 65 seconds of warm-up, replay with recovery disabled exactly
matches all **1,722** original price/count/quality/target comparisons across 287
decisions. The original global sequences and decision clocks are retained.

At every horizon, the new policy first becomes available at decision 1592,
**22:47:40.569607072 UTC**, after the first qualifying received spot. The old
five-second forecast first recovered at **22:48:38.153780110**. This is a
**57.584-second earlier calculation cutoff**, not measured Redis or browser
delivery. No previously available forecast is lost and no price changes where
both policies were available.

| Horizon | Newly available decision forecasts | Graded / missing target | Median / p90 absolute error ($), newly available only | Earlier calculation availability |
|---|---:|---:|---:|---:|
| 5 s | 113 | 112 / 1 | 0.0640 / 0.2804 | 57.584 s |
| 10 s | 102 | 100 / 2 | 0.2487 / 0.5328 | 52.131 s |
| 30 s | 65 | 62 / 3 | 1.4333 / 3.7537 | 31.962 s |

These error figures use Decimal precision 80, an ordinary median and a
nearest-rank p90. They are conditional on a recorded clean earliest target inside
the bounded fixture. Missing targets stay ungraded; later conflicts outside the
fixture are outside this check. Decisions sharing a target are correlated.
The replay does not simulate new publication scheduling, acknowledgements, cache
coverage or lead, and this one incident is not a general recovery guarantee.
[Final results](research/spot_twap_response/reconnect_recovery/result_final.json)
and [manifest](research/spot_twap_response/reconnect_recovery/manifest_final.json)
retain all horizons, exact values, limits and hashes. The raw fixture stays
outside Git.

### Historical observer reproducibility

Actual reanalysis of all **36,000** old cache probes and **6,231** qualified
payloads produced **zero JSON differences**, including types, ordering and
legacy labels. Both historical fixture sets and their generators, and the
archived combined analyzer, also match their original hashes.
[Compatibility proof and reproduction command](research/spot_twap_response/reconnect_recovery/observer_compatibility.json).

The completed canaries remain evidence for their deployed versions. A fresh
bounded run is still required to measure this revision's real cache recovery and
publication behavior. None was started here.

## Droplet update

Run only after this reviewed change is pushed to GitHub. Keep
`GHOST_TWAP_ENABLED=false` and preserve both completed campaigns. No schema or
environment changes are required. Only the Chainlink collector is affected.

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl status price-collector-polymarket-chainlink --no-pager
sudo journalctl -u price-collector-polymarket-chainlink -n 100 --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl --fail http://127.0.0.1:9000/markets/current/live
redis-cli EXISTS btc:live:ghost_chainlink_twap_60s
```

The ghost key should be absent while disabled. This upgrade does not authorize
resetting a stopped campaign or starting another canary. The required operational
sequence is also recorded in [OPERATIONS.md](OPERATIONS.md#ghost-reconnect-recovery-upgrade).
