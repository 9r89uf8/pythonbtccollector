# Freshness policy: fixed-decision causal replay

This offline replay checks source-age allowance **5 seconds** with both local
receipt ages still limited to **3 seconds**, using the one-hour canary's saved
decision times. It validates the calculation and measures newly eligible
forecasts. It does not simulate the candidate's decision scheduling, expiry,
Redis publications, browser delivery or live availability.

The primary post-warm-up cohort begins 65 seconds after the campaign start.
The source-only gate change makes all six horizons available in 753 additional
saved decisions: any-unavailable falls from **761/7,163 to 8/7,163**. After
60 seconds the corresponding counts are **771/7,174 to 18/7,174**. These are
fixed-decision eligibility counts, not wall-clock coverage measurements.

## Evidence and reconstruction

Input is the complete verified JSONL export of the September 14, 2026 UTC canary,
00:45:20.328–01:45:20.328, run `06f1134f06c14a94b5cab90a43b2eee6`:

- Path: `C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-canary-2026-09-14/audit.jsonl`.
- Size: 326,727,434 bytes.
- SHA-256: `d7536bcafcf62170abbf9329f6b720d7df35fb710b9f79f64ec96ca2bda05b80`.

All exported current inputs, slot inputs and first/late/conflicting target
records are combined by accepted event sequence. Their 7,008 unique events have
no contradictory sequence versions. **Every sequence 1–6,982 is present**;
the final saved decision includes sequence 6,981. Therefore all decision inputs
can be replayed from complete accepted prefixes, without a saved-slot override.
The export is ordered by text identity, so the reproducer seeks rows in numeric
decision order before replaying. It admits only events included in that row's
prefix and checks both receipt clocks against the original decision clocks.

There are 29 missing sequences after 6,982, through exported sequence 7,037.
The complete prefix ends at receipt wall time `1789350321678270534` ns and
monotonic time `5885351447638187` ns. The primary grading rule requires the entire
120-second observation window to end no later than that complete prefix. This
conservatively excludes 35 newly eligible decisions per horizon near the end.
It avoids assuming that partial target-drain evidence proves an unseen first
report or absence of conflicting revisions. Completeness here concerns the
collector's accepted sequence, not upstream messages or independent clock truth.

## Calculation checks

Contract 3 is instantiated twice: source/receipt limits of 3,000/3,000 ms for
the old policy and 5,000/3,000 ms for the candidate. Other policy settings remain
10,000 ms historical carry, 120,000 ms retained history and 1,024 event capacity.

The full replay passed with **zero mismatches**:

- 7,292 decisions reproduce the original current inputs and `valid_until_wall_ns`.
- All 648,899 original slot selections, values, categories and carry ages match.
- All 43,752 original horizon prices, availability, counts, qualities and reasons match.
- An independent source-key reference matches all 648,899 candidate slots and
  all 43,035 available candidate 60-slot means. Original available prices remain equal.
- All 36,323 original clean target/error pairs inside the complete observation
  windows match the persisted first target and both persisted errors.

The independent reference uses the last admitted value for each source second,
selects historical sources at or before the slot, preserves the 10-second carry
limit, and uses the current eligible spot only for future slots. It sums 60
Decimal values at precision 80 and rounds the mean to E18 with half-even rounding.
No future received event is admitted to a decision.

## Primary newly eligible cohort, after 65 seconds

Each horizon has 753 newly eligible forecasts, all withheld as
`expired_or_target_received` under the old policy. Targets are the first exact
TWAP source report with receipt in `[decision, decision + 120 seconds)`, with no
already-received target, future clock or conflicting value within that window.
Thirty-five incomplete-window cases per horizon are excluded before grading.
Missing targets remain in the availability denominator and are not scored.

Errors compare ghost and unchanged current official TWAP against the same target.
Basis points divide absolute error by that actual target price and multiply by
10,000. Quantiles use Decimal linear interpolation at position `(n − 1) × p`.

| Source horizon | Scored / missing / incomplete | Ghost median / p90 (bp) | Persistence median / p90 (bp) | Ghost strictly better |
|---|---:|---:|---:|---:|
| 1 s | 568 / 150 / 35 | 0.046005 / 0.143263 | 0.064873 / 0.182454 | 51.4% |
| 2 s | 648 / 70 / 35 | 0.027695 / 0.136252 | 0.113473 / 0.308467 | 94.4% |
| 3 s | 642 / 76 / 35 | 0.016188 / 0.131749 | 0.164749 / 0.380727 | 97.0% |
| 5 s | 661 / 57 / 35 | 0.016875 / 0.122560 | 0.276685 / 0.646500 | 97.4% |
| 10 s | 678 / 40 / 35 | 0.044421 / 0.175352 | 0.565847 / 1.237708 | 95.3% |
| 30 s | 705 / 13 / 35 | 0.422379 / 0.990955 | 1.482624 / 3.559916 | 77.7% |

The 5/10/30-second median dollar errors are $0.130 / $0.341 / $3.244;
p90 errors are $0.941 / $1.344 / $7.603. These strata have only 426/441/455
distinct scored target stamps; overlapping forecasts reuse outcomes.

For comparison, the existing-eligibility intersection under the same post-65s
and complete-120s-window rules has 6,023/6,016/5,998 scored observations at
5/10/30 seconds. Its median errors are 0.006849/0.044064/0.411164 bp and p90
errors are 0.068386/0.168458/1.275959 bp. Candidate and original prices are
identical on that intersection. These descriptive strata occur under different
conditions; their difference is not a randomized estimate of policy harm.
They also differ from the original report's acknowledged-publication cohort.

## Target arrival headroom is not delivered lead

| Source horizon | Target receipt minus decision, p10 / median (seconds) | Scored targets with source stamp at or before decision |
|---|---:|---:|
| 1 s | 0.030927 / 0.182559 | 568 / 568 |
| 2 s | 0.249986 / 1.077553 | 610 / 648 |
| 3 s | 1.298806 / 2.038329 | 485 / 642 |
| 5 s | 3.239897 / 3.897036 | 0 / 661 |
| 10 s | 8.220237 / 8.777188 | 0 / 678 |
| 30 s | 28.087250 / 28.582628 | 0 / 705 |

All scored first receipts are strictly after decision time. A past source stamp
can still identify an unreceived official report. However, the newly eligible
one-second group has weak improvement over persistence, 150 missing targets and
very short remaining headroom. Its median source stamp is already 2.074 seconds
in the past. Broadening the age gate therefore does not establish uniformly
useful short-horizon predictions. These forecasts were never published; no
Redis, browser or confirmed-lead credit is assigned and no old latency median is
subtracted to manufacture such a measurement.

## Reproduce and inspect

Run from this checkout with the development Python. The output directory must
not already exist. The reproducer verifies the full export hash, each row's
frozen/state hashes, accepted prefix completeness and a stable engine hash.

```powershell
C:/Users/alexa/PycharmProjects/polycollector/.venv/Scripts/python.exe research/spot_twap_response/freshness_expiry/review.py C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-canary-2026-09-14/audit.jsonl research/spot_twap_response/freshness_expiry/reproduced
C:/Users/alexa/PycharmProjects/polycollector/.venv/Scripts/python.exe -m pytest research/spot_twap_response/freshness_expiry/test_review.py -q
```

The focused suite has **12 passing tests** for independent clock gates,
source/future/carry rules, E18 arithmetic, first-target boundaries, conflicting
revisions, incomplete observation windows, hash/version rejection and numeric
replay ordering. `results/summary.json` contains exact values, both all-decision
and post-65-second cohorts, policy settings, sequence gaps and code hashes.
`results/manifest.json` records the reviewed artifact hashes and invocation.

No production, service, database or remote state was touched. This is an offline
validation of saved decisions, not acceptance of a new live policy, profitability
or browser readiness.
