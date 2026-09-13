# Supplied third implementation: local verification

The unchanged supplied `checkpoint_a_replay/third_implementation_check.py`
completed locally in 13.82 seconds with exit code zero. Its body reads fixture
files and compares calculations; it makes no network or database calls.
`third_review_run.py` captures its output and explicitly asserts the results,
because the supplied script only prints mismatches and does not fail on them.

The accepted run reported:

- 3,090 sampled forecasts: zero mismatches against the independent expected
  fixture and zero against the engine. Sampling takes every seventh decision,
  including the first: 515 decisions across six horizons.
- All 21,600 v1/v2 rows: zero changes in price, availability, or the estimated
  arrival timestamp; zero mismatches in `old carried = carried + pending`.
- All six carry-distribution totals reconcile to 3,585 available forecasts,
  and their zero-carry counts match the independently frozen v2 healthy counts.
- Every hashed input remained unchanged during the run; stderr was empty.

The supplied script calculates the carry table over all 3,600 decisions, though
its comparisons against the engine and expected table use the smaller sample.

| Horizon | No interior carry | Maximum 1s | Maximum 2s | Maximum 6s | Maximum at most 3s |
|---|---:|---:|---:|---:|---:|
| 1s | 1,322 | 2,081 | 60 | 122 | 3,463 |
| 2s | 1,320 | 2,085 | 60 | 120 | 3,465 |
| 3s | 1,340 | 2,069 | 59 | 117 | 3,468 |
| 5s | 1,383 | 2,031 | 57 | 114 | 3,471 |
| 10s | 1,496 | 1,935 | 52 | 102 | 3,483 |
| 30s | 2,066 | 1,425 | 32 | 62 | 3,523 |

Each row totals 3,585 available forecasts. These are maximum interior carry ages
per forecast window, not numbers of missing source seconds or independent gap
incidents. Many overlapping forecasts can contain the same gap. The 15
unavailable decisions at each horizon are excluded from this table.

## What a three-second quality threshold would mean

Treating interior carry up to three seconds as healthy would relabel many
available forecasts in this hour. It would not establish that the missing
source values were unchanged or that the resulting forecast error was small.
The three-second current-input freshness rule measures a different property
from interior carry age and does not justify using the same cutoff for both.

No policy was changed. The accepted rules retain the ten-second maximum carry
limit and mark any interior carry as degraded while exposing pending and future
assumptions separately. A later tolerance label could be considered explicitly,
but this one-hour fixture does not establish its suitability or a price-error
bound.

## Scope and limitations

The third calculation independently rebuilds slot values from the retained
receipt prefix. It imports the production engine only for comparison. Its
claimed authorship process cannot be verified from execution; the useful
evidence is the distinct calculation and observed agreement.

It is a fixture check, not a general implementation of every engine rule. It
does not model the engine's bounded-history/capacity state, explicit gaps,
TWAP regression/conflict recovery, acceptance-order failures, or disagreement
between real monotonic and wall clocks. The fixture lacks those adversarial
conditions and uses declared synthetic monotonic timing. The no-TWAP branch
also skips detailed count parity. The sampled comparison checks price, counts,
and quality; it does not independently compare reason-code identity or ETA
arithmetic against the engine. Those remain covered by the separate full
fixture replay and focused engine tests, not by this supplied check alone.

All original retained/upserted-data limits remain: this is not complete
first-arrival history, measured frontend delay, or a prediction-accuracy study.
The source, engine, fixture, runner, stdout, and stderr hashes are recorded in
`third_review_manifest.json`. Existing artifacts are preserved; the runner
refuses to overwrite them.
