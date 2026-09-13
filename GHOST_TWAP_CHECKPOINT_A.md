# Ghost TWAP — Checkpoint A

Implemented locally on 2026-09-13, now at payload contract 2 with a separate pending-input category. This checkpoint adds the pure calculation module and tests. It adds no collector integration, environment settings, database schema, Redis writes, API route or deployment change. `GhostPolicy.enabled` defaults to `False`; existing services do not import the module. The collector-level `GHOST_TWAP_ENABLED` setting remains work for B.

## Calculation contract

[`price_collector/ghost_twap.py`](price_collector/ghost_twap.py) exposes immutable `PriceEvent`, `GhostPolicy`, slot/count/forecast/decision records, and a single-owner `GhostTwapEngine`. Callers inject every clock and event; the engine performs no network, filesystem, database or system-clock operations.

For the latest received official TWAP source stamp w, forecasts target w+h for h=1,2,3,5,10,30 seconds. Each averages slots U−62 through U−3. All six windows share 89 slot timestamps. Historical slots use the latest accepted revision of the greatest permissible source stamp; future slots hold the latest received current spot. Each horizon counts observed, carried, pending, future and missing slots, totaling 60. Missing/invalid inputs produce an unavailable result, never a shorter average.

After historical input selection, an exact stamp is observed. A usable nonexact slot beyond the greatest admissible received source stamp is pending; an interior absent stamp is carried. The maximum source stamp can differ from the latest-receipt current input during source disorder. Pending retains the original historical selection, rather than substituting the current spot. Available forecasts with interior carry are degraded; those without it are healthy. Pending/future inputs remain assumptions, so healthy does not mean fully observed or predictively certain. Missing/stale-input guards take precedence.

Prices are positive finite Decimal values exactly representable at E18 and within `NUMERIC(38,18)`. Input values and official TWAP are preserved. Calculation uses an isolated 80-digit context and rounds the final mean to 18 decimals with `ROUND_HALF_EVEN`; ambient Decimal precision cannot change it. Source and market timestamps remain integer UTC milliseconds.

Freshness includes source age, wall receipt age and monotonic receipt age, with an inclusive 3,000 ms limit. Historical carry is at most 10,000 ms. A future-dated latest input fails quality instead of revealing an older current value. Source-past targets remain eligible when unreceived; already received targets are unavailable. TWAP regressions need a strict new valid high-water stamp, and a conflicting anchor remains unusable until clean advancement.

ETA is anchor receipt plus h, with a signed remaining interval and overdue flag. It is an estimate. Validity ends at the earliest source, wall-receipt or monotonic-receipt freshness limit; the latter is mapped to decision wall time using its remaining allowance.

## Ordering, bounds and frozen evidence

Acceptance sequences cover only the two canonical ghost input feeds. The first sequence may start anywhere, then increments by one; a jump invalidates both histories and starts recovery. Receipt clocks may tie if sequence orders the events. Regressing receipt clocks, repeated/regressing sequences or an event backlogged behind an issued decision latch an unavailable fault and require a fresh engine run. A snapshot cannot be backdated behind accepted events or a prior decision.

**B must establish a receipt-time admission barrier before each snapshot.** It cannot issue a decision and later ingest a queued event received before that cutoff. Publication coalescing may skip calculations, but it may not silently skip constituent events. This strict rule is intentional in A; any later processed-prefix design requires an explicit contract change and new tests.

State retains 120 seconds of source history plus at most one necessary spot predecessor, with a hard 1,024-record bound. Explicit feed gaps and capacity loss clear affected coverage; new valid inputs can rewarm it. Frozen decisions retain a cumulative gap count and the last immutable gap marker per feed (reason, ordinal and logical after-sequence boundary). These markers do not fabricate gap timestamps. B still owns its complete operational gap/audit records and queues.

Prior snapshots own immutable records, so later revisions cannot change their values or serialized audit bytes. The audit JSON contains all 89 slots and deduplicated selected-input records; forecast offsets identify each group of 60. Compact JSON omits slot detail. Prices are decimal strings, nanosecond fields are strings to preserve exactness in JavaScript, and millisecond fields/counts are integers. Outputs explicitly say `publication_state=not_published`: B must supply actual publication evidence, audit admission, result matching and storage controls.

## Recorded replay and tests

The [current frozen fixture](tests/fixtures/ghost_twap/pending_v2/README.md) uses the first UTC-aligned hour in the existing local September 11 export with at least 120 seconds of context on both sides: 01:00–02:00 UTC. It contains 7,505 retained events, 3,600 one-second decisions and 21,600 independently calculated horizon rows. No new droplet query was required. The original four-category fixture and generator remain unchanged in the parent directory.

The [replay test](tests/test_ghost_twap_replay.py) requires exact agreement for every price, current-input identity, included sequence, target, slot count, quality/reason and ETA. All 21,600 comparisons pass, including 15 unavailable decisions at each horizon. These are implementation/availability results, not a new forecast-accuracy estimate against realized TWAP.

Spot rows are retained upserts. The export lacks original cross-feed ordering, monotonic clocks and gap/session records. The deterministic replay order and monotonic clock are explicitly synthetic. They permit an exact arithmetic check on retained receipt prefixes, not a claim about complete first arrivals or measured production latency. The [independent builder](research/spot_twap_response/checkpoint_a_replay/README.md) imports no engine code and preserves source/query/artifact hashes.

[Core tests](tests/test_ghost_twap.py) and [adversarial tests](tests/test_ghost_twap_edges.py) also cover steps, reversals, outgoing prices, E18 tie rounding, invalid numeric types, market boundaries, inclusive limits, ordering faults, missing slots, conflicting/regressing reports, gaps, capacity loss, exact JSON and immutable prior snapshots. [Validation record](research/spot_twap_response/checkpoint_a_validation.json).

The [pending-category tests](tests/test_ghost_twap_pending.py) add source-disorder, interior-hole, delayed-arrival, stall, immutable relabeling and partial-recovery cases. The full suite passes 934 tests in the repository's Python 3.9.5 environment; all 53 ghost tests also pass under local Python 3.12.0. The peer's [third implementation](research/spot_twap_response/checkpoint_a_replay/third_implementation_check.py) is now available and was independently run: all 3,090 sampled forecasts match the expected table and engine, and the 21,600-row v1/v2 baseline comparison has zero differences. A wrapper checks the printed mismatch counts because the supplied script does not fail its exit status on a mismatch. [Run evidence and limitations](research/spot_twap_response/checkpoint_a_pending_review/third_review_README.md).

Contract 2 changes quality labels only: every original price, availability decision, current input identity, target and ETA still matches across all 21,600 rows. Previously all available 5/10/30-second results were degraded; they now include 1,383/1,496/2,066 healthy results, respectively. Each horizon still has 15 unavailable results. [Complete comparison](research/spot_twap_response/checkpoint_a_replay/pending_review.json).

Run from the repository root with the project's development environment:

```powershell
.venv/Scripts/python.exe -m pytest -q tests/test_ghost_twap.py tests/test_ghost_twap_edges.py tests/test_ghost_twap_pending.py tests/test_ghost_twap_replay.py
.venv/Scripts/python.exe -m pytest -q
```

## Remaining checkpoints

B implements the default-off collector worker, receipt barrier, audit admission, target matching, storage/retention/export guards and prospective canary. C exposes and measures snapshot/SSE delivery through SSH after canary review. They remain unimplemented. Their budgets and acceptance criteria stay in the [live plan](GHOST_TWAP_LIVE_PLAN.md); A's exact replay does not establish their runtime performance.

A sequence gap clears both input histories conservatively. Recovery is per horizon: with a dense new spot history starting at source s0, the source coverage condition is w+h−62 >= s0. Required anchor advances are 61/60/59/57/52/32 seconds for h=1/2/3/5/10/30; actual wall recovery also depends on receipts and all freshness checks. A TWAP-only gap can recover sooner because spot history remains. B must record queue pressure, input loss, reset counts and recovery duration for each horizon; a fixed 62-second outage is not the implemented rule.

The peer's suggested three-second interior-carry threshold is reserved for B as an optional versioned quality policy. At h=5, maximum carry ages of 0/1/2/6 seconds occur in 1,383/2,031/57/114 available forecasts. These are overlapping windows, not 114 separate stalls. The aggregate identity residual does not establish zero conditional error for one-second interior absences. Accepted A keeps its existing quality rule; this recommendation does not block installing it.
