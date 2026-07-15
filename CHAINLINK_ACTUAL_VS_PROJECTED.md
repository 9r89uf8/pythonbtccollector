# Actual vs. Projected Chainlink

## Purpose

The Chainlink catch-up signal is an experiment that asks one narrow question:

> Given the move already visible in Binance BTC futures, what Chainlink BTC/USD
> value should we expect to have received about three seconds later?

The project compares that projection with the latest Chainlink value available
no later than the forecast target time. This gives us a causal, repeatable way
to measure whether the futures lead contains useful information about
Chainlink's next few seconds.

It is important to keep the claim narrow. The signal is **not** predicting:

- the official five-minute market settlement price;
- whether BTC will finish Up or Down;
- Polymarket probability or order execution;
- a new price move that has not happened yet; or
- why either market moved.

It is estimating a short-lived Chainlink catch-up value from a futures move
that is already observable.

## What “actual” and “projected” mean

A projection generated at time `t` by the selected model has a target at
`t + 3,000 ms`.

- **Projected Chainlink** is the model's estimate, made only from information
  locally available at generation time.
- **Actual Chainlink** is the newest Chainlink observation the evaluator had
  observed whose producer receive time is no later than the target time.
- **No-change baseline** is the Chainlink value at generation time carried
  forward unchanged to the same target.

The actual is selected by local `received_ms`, not by looking ahead for the
next convenient update. A Chainlink observation received after the target is
excluded, even if it is available when the evaluation row is eventually
written. This prevents future information from leaking into the result.

The Chainlink cache value also carries an internal producer epoch and monotonic
accepted-event sequence. If the evaluator observes a sequence jump,
regression, producer restart, or loss of sequence metadata, it discards outcome
history that crosses that discontinuity. It cannot reconstruct an overwritten
latest-cache value, so affected evidence is invalidated rather than paired with
an older, apparently eligible actual. Legacy-only startup remains supported,
but the first sequenced value resets that history. Once sequence metadata has
been established, losing it suppresses actual-outcome ingestion until a new
sequenced value re-establishes continuity.

Each row retains the five-minute market in which it was generated. Reporting
windows are selected by `target_ms`, so a forecast generated immediately before
a boundary can be scored in the following target window. The reporting query
therefore inspects only that target window's generation market and its
predecessor.

## How the model works

The model is a deterministic, basis-neutral ratio calculation. It is not a
machine-learning model with continuously fitted weights.

When a new Chainlink event arrives, the engine creates an anchor:

1. Record the new Chainlink value, `C_anchor`, and its local receive time.
2. Look back by the model lag and select the most recent eligible futures price
   at or before that point. This becomes `F_reference`.
3. At forecast generation, compare current futures, `F_now`, with that
   reference.
4. Transfer that percentage move to the Chainlink anchor.

The calculation is:

```text
futures_return      = (F_now / F_reference) - 1
projected_chainlink = C_anchor * (1 + beta * futures_return)
```

For the selected model, `beta = 1`, so it simplifies to:

```text
projected_chainlink = C_anchor * F_now / F_reference
```

The current model is named `catchup_ratio_l3000_b100`:

- `catchup_ratio` describes the anchored futures-ratio method;
- `l3000` means a 3,000 ms lag and forecast horizon; and
- `b100` means `beta = 1.00`.

The anchor is what makes the calculation useful. Binance futures and Chainlink
can have a persistent price difference, so directly adding a futures dollar
move to Chainlink would mix that basis into the forecast. The ratio model
instead transfers only the futures percentage move since the reference
associated with the latest Chainlink update.

The engine re-anchors on every accepted Chainlink event, even if the reported
price is unchanged. An unchanged refresh is still new timing information.
Without re-anchoring, the same earlier futures move could be counted twice.

The complete sequence is:

```text
Chainlink event arrives
        |
        +--> locate futures reference 3 seconds earlier
        |
        +--> compare current futures with that reference
        |
        +--> generate a Chainlink estimate for 3 seconds ahead
        |
        +--> at the target, pair it with the causally available actual
```

## How we arrived at the selected model

The starting observation was that Binance futures often appeared to move
roughly three to four seconds before the corresponding Chainlink update. That
was treated as a hypothesis, not as a fixed conclusion.

The first version deliberately kept the candidate set small and interpretable:

- `catchup_ratio_l3000_b100` — 3.0 seconds, beta 1.00;
- `catchup_ratio_l3500_b100` — 3.5 seconds, beta 1.00;
- `catchup_ratio_l4000_b100` — 4.0 seconds, beta 1.00; and
- a separately paired no-change baseline.

Those were the only catch-up horizons in the selection experiment. We did
**not** test 2.0-second or 2.5-second candidates. Therefore, the accepted
decision shows that 3.0 seconds was the best eligible model among 3.0, 3.5,
and 4.0 seconds and that it passed the no-change baseline gates. It does not
show that 3.0 seconds is better than 2.0 or 2.5 seconds; their performance is
currently unknown.

Testing 2.0 and 2.5 seconds later would mean expanding the predefined
candidate set and repeating replay, calibration, and untouched holdout
selection. It should not be done by changing the frozen live primary in place.

Raw data made it possible to test the hypothesis on the receive-time timeline:

- futures prices were reconstructed on the 100 ms grid;
- individual Chainlink events retained their exact local receive ordering;
- replay emulated the live 100 ms reads and 500 ms forecast cadence; and
- only complete, clean, count-reconciled overlaps between both feeds were
  eligible.

Replay reports are limited to at most 24 hours each. The accepted selection was
not based on picking whichever candidate looked best in the final market. The
selection policy used explicitly assigned older calibration evidence and a
strictly later, untouched holdout:

1. Compare all three candidates on the same common cohort of forecast times.
2. Require positive calibration MAE and RMSE skill against each candidate's own
   paired no-change baseline.
3. Rank eligible candidates lexicographically by calibration MAE skill first
   and RMSE skill second, with deterministic tie-breakers.
4. Freeze the calibration winner.
5. Test only that frozen winner on the later holdout.
6. Accept it only if it retains positive MAE and RMSE skill and passes the
   coverage gates; otherwise, select no model.

Each replay report also had to contain at least 10,000 common scored forecasts,
at least 50% valid common coverage, and at least 99% maturation coverage. The
common cohort matters because it prevents one horizon from being compared on
easier or different moments than another.

That process selected `catchup_ratio_l3000_b100` as the provisional primary.
The decision and its evidence hashes were frozen in a selection artifact. The
live worker reads and verifies that artifact; it does not contain a hard-coded
winner, rerank recent results, or silently fall back to another candidate.

This is best described as **model selection**, not model training. The current
version selected a lag from three fixed candidates while keeping beta fixed at
1. A future version could test additional lags or estimate beta, but that would
require a new replay, new calibration evidence, and a new untouched holdout.

## Measurement-correction checkpoint

The accepted production decision is immutable schema/policy v2 evidence and
continues to select the provisional 3.0-second model. A later code review found
measurement defects that do not change that frozen file but must be corrected
before a shorter-horizon experiment:

- Replay/selection v3 replaces the old directional counters with a complete
  `up`/`neutral`/`down` confusion matrix. It reports three-class accuracy,
  action precision over every predicted action, move recall, neutral
  false-action rate, opposite-direction rate, and action frequency. The old
  approximately 99% figure excluded false actions on neutral outcomes and must
  not be described as action precision.
- Replay v3 separates raw receive time from configured source-visibility time,
  records fixed futures and Chainlink delay assumptions, and supports all five
  100 ms generation-phase offsets. These are sensitivity inputs, not measured
  Redis publication-completion timestamps.
- New v3 evidence requires zero future skew so a signal accepted for
  publication is evaluated under the same strict local-time causality rule.
- The live Chainlink sequence metadata described above makes overwritten-cache
  gaps detectable and fail-closed.
- Live evaluations are persisted as complete generated-time candidate cohorts.
  Maturation retains each horizon's causal target, while queue overflow,
  batching, retry, permanent-error isolation, deferral, and retention operate
  on whole cohorts.

Rows written before the cohort-atomic rollout can still be partial. Any future
live comparison must start at the rollout/new-artifact boundary and verify the
exact artifact-defined candidate set at each common `generated_ms`; old rows
cannot be made complete retroactively.

This checkpoint still contains only the 3.0/3.5/4.0-second candidate family;
it does not run or imply a 2.0/2.5-second result. Because the shorter candidates
were proposed after inspecting the accepted v2 holdout, all existing evidence
is now development/calibration data for that future expanded family. Its policy
must be preregistered and frozen before collecting a genuinely later untouched
holdout.

## How the model is used now

The catch-up engine runs in the standalone
`price-collector-shadow-signal` service. It remains isolated from both source
collectors, so an experimental-model failure cannot stop futures or Chainlink
collection.

Its live workflow is:

1. Read the latest futures and Chainlink cache values together every 100 ms.
2. Feed those observations into all three fixed candidate engines.
3. Publish only the accepted 3.0-second primary projection.
4. Replace an invalid projection with null fields instead of carrying an old
   valid forecast forward.
5. Give the live projection a short TTL so it disappears if the worker stops.

For evaluation, every entered epoch-aligned 500 ms bucket creates one attempt
per candidate, including invalid attempts. Each valid attempt matures at its
own horizon and is paired with the causal actual described above. Missed
buckets are not backfilled.

For a scored attempt, the stored errors are:

```text
forecast_error = projected_chainlink - actual_chainlink
baseline_error = chainlink_at_forecast - actual_chainlink
```

Positive forecast error means the projection was high; negative means it was
low. Absolute error measures distance without direction. The no-change
baseline answers the practical question: was using the futures move better
than simply assuming Chainlink would stay where it was?

Evaluation persistence uses a separate bounded queue and writer. Candidates
from one generated time are held until the maximum configured horizon matures,
then queued, retried, rejected, and retained only as a complete cohort.
Database slowness or failure is not allowed to delay the 100 ms signal loop.
Stored evaluation evidence is retained for seven days by default.

## Results from three recent five-minute markets

Each market contained 600 scored attempts at the selected 3.0-second horizon,
with no invalid attempts and no missing actuals.

| Market | Forecast MAE | Baseline MAE | MAE reduction | Forecast RMSE | Baseline RMSE | P95 absolute error | Maximum error | Average bias |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | $1.37 | $1.79 | 23.4% | $2.12 | $3.24 | $4.36 | $9.36 | $0.02 high |
| 2 | $1.34 | $2.10 | 36.3% | $2.24 | $3.57 | $4.62 | $12.84 | $0.03 low |
| 3 | $2.23 | $3.29 | 32.3% | $4.50 | $6.47 | $7.25 | $39.65 | $0.21 high |

The paired outcomes were:

| Market | Projection closer | Equal | Projection worse | Closer among non-ties |
| --- | ---: | ---: | ---: | ---: |
| 1 | 209 | 186 | 205 | 50.5% |
| 2 | 248 | 191 | 161 | 60.6% |
| 3 | 260 | 157 | 183 | 58.7% |
| **Total** | **717** | **534** | **549** | **56.6%** |

Because every market has the same 600 scored rows, the displayed summaries
give these approximate combined results:

- forecast MAE: **$1.65**, versus **$2.39** for no change;
- MAE advantage: approximately **$0.75**, or **31.2% lower**;
- pooled forecast RMSE: approximately **$3.15**, versus **$4.66**;
- RMSE reduction: approximately **32.4%**;
- paired outcomes: **39.8% closer, 29.7% equal, and 30.5% worse**; and
- average signed bias: approximately **$0.07 high**.

These combined dollar figures are approximate because they were calculated
from the rounded market summaries. Median and P95 errors cannot be pooled
correctly from summary values alone, so no combined median or P95 is claimed.

## What the results say

### The signal is adding useful short-horizon information

The projection beat no change on both MAE and RMSE in all three markets. The
improvement was not confined to one quiet interval, and the pooled reduction
was about one third. That is the right comparison for this experiment: not
whether the error is always zero, but whether transferring the already visible
futures move gives a closer three-second Chainlink estimate than doing nothing.

### The advantage also appears in paired counts and RMSE

The projection was closer more often than it was worse in every market. After
removing exact ties, it won 56.6% of the comparisons. RMSE, which penalizes
large errors more heavily than MAE, also improved by about one third. This
means the observed advantage survives an outlier-sensitive metric.

### Directional bias is currently small

The first two windows were nearly unbiased, and the approximate combined bias
was only $0.07 high compared with an approximate $1.65 MAE. There is no strong
evidence in these windows of a persistent tendency to project too high or too
low.

### Sudden moves remain the main visible weakness

The third market had a $39.65 maximum error and materially higher P95, MAE, and
RMSE. That is consistent with the behavior already observed around sharp
drops or increases, although the summaries alone cannot prove the cause of
that individual error.

The model can translate only the futures move visible when the forecast is
generated. It cannot anticipate a new shock, continuation, or reversal that
occurs during the following three seconds. A fixed three-second lag and beta of
1 can also be imperfect when Chainlink's catch-up speed or pass-through changes
with market conditions. Those are expected limits of this V0 model, not data
that should be hidden from its evaluation.

### Evaluation coverage was operationally clean in these windows

All 1,800 attempts were scored, with zero invalid attempts and zero valid
forecasts missing an actual. That confirms complete evaluation coverage for
these three windows. It does not prove that every future window will have the
same coverage, especially through feed gaps, reconnects, or service outages.

## What the results do not prove

These three markets are encouraging evidence, but they contain only 15 minutes
of observations in total. They also contain overlapping observations: a
forecast is attempted every 500 ms for a 3-second horizon, so roughly six
forecast horizons are active at once. The 1,800 rows are therefore highly
autocorrelated and should not be described as 1,800 independent trials or as a
statistical-significance result.

The results do not yet establish:

- performance across all volatility and liquidity regimes;
- profitability after latency, spread, slippage, and execution constraints;
- accuracy at the five-minute settlement boundary;
- that 3.0 seconds will always remain the best lag; or
- that the largest errors are acceptable for a trading decision.

They do support a more limited and defensible conclusion:

> In these three recent markets, the frozen 3-second catch-up model produced a
> materially closer estimate of the causally observed Chainlink value than
> carrying the current Chainlink value forward unchanged.

## When to reconsider or retune the model

The model should not be changed because of one dramatic miss or because a few
recent markets favor another setting. Retuning becomes justified after enough
new evidence covers quiet periods, trends, reversals, sudden shocks, feed
reconnects, and different times of day.

Any future tuning should repeat the same discipline:

1. Preregister a candidate grid that extends below 2.0 seconds so another
   lower-bound winner does not immediately trigger a second search.
2. Treat every already inspected window as calibration and freeze lag, beta or
   deadband, reference-gap limits, timing-delay scenarios, phase offsets, and
   the robustness rule before the new holdout.
3. Replay every candidate on identical common cohorts and keep no change as the
   horizon-matched paired baseline.
4. Report full confusion metrics, daily/session skill, non-overlapping-origin
   sensitivity, and paired moving-block-bootstrap intervals. The block length
   must exceed both forecast overlap and the update-dependence period.
5. Freeze the calibration winner before evaluating one strictly later,
   untouched multi-window holdout; do not rerank or fall back on holdout.
6. Promote a new immutable decision artifact only if all preregistered error,
   uncertainty, timing-robustness, and operational-coverage gates pass.

Until then, the current production behavior should remain stable: publish only
`catchup_ratio_l3000_b100`, continue evaluating the fixed candidates silently,
and treat the signal as shadow/experimental evidence rather than an
authoritative future price.

## Which files are responsible

`engine.md` is the original design document. It explains the hypothesis and
recommended architecture, but it is not executable and does not run in
production.

The executable Actual vs. Projected Chainlink path is divided by
responsibility:

| Responsibility | File | What it does |
| --- | --- | --- |
| Core mathematical engine | [`price_collector/shadow_signal.py`](price_collector/shadow_signal.py) | Defines the candidate models, Chainlink anchors, futures-reference lookup, validity checks, and anchored-ratio projection. This is the main “engine.” |
| Live worker | [`price_collector/shadow_signal_collector.py`](price_collector/shadow_signal_collector.py) | Runs the 100 ms Redis loop, feeds observations into the engine, publishes the frozen primary, and passes attempts to evaluation. |
| Causal actual and error calculation | [`price_collector/shadow_signal_evaluation.py`](price_collector/shadow_signal_evaluation.py) | Schedules 500 ms attempts, matures each horizon, chooses the latest eligible actual, calculates forecast and baseline errors, and manages the nonblocking writer queue. |
| Evaluation database writes | [`price_collector/db.py`](price_collector/db.py) | Inserts evaluation records and performs bounded retention cleanup. |
| Evaluation database schema | [`schema.sql`](schema.sql) | Defines `shadow_signal_evaluations`, its constraints and privileges, and the narrow reporting view. |
| Redis signal encoding | [`price_collector/live_cache.py`](price_collector/live_cache.py) | Strictly encodes, writes, reads, and validates the short-lived shadow payload. |
| Historical replay | [`price_collector/shadow_signal_replay.py`](price_collector/shadow_signal_replay.py) | Replays raw futures and Chainlink evidence through the same core engine and produces candidate metrics. |
| Model selection | [`price_collector/shadow_signal_selection.py`](price_collector/shadow_signal_selection.py) | Applies the chronological calibration/holdout policy and creates the frozen selection decision. |
| Runtime decision validation | [`price_collector/shadow_signal_artifact.py`](price_collector/shadow_signal_artifact.py) | Verifies the selection artifact, replay configuration, hashes, candidate set, and primary before activation. |
| Per-market performance calculation | [`price_collector/shadow_signal_reporting.py`](price_collector/shadow_signal_reporting.py) | Reads validated evaluation points and derives MAE, RMSE, bias, baseline skill, and paired outcomes. It reports on the engine; it does not execute the model. |
| Read-only route wiring | [`price_collector/api.py`](price_collector/api.py) | Exposes live and persisted evaluation responses. It does not import or run the mathematical engine. |
| Settings | [`price_collector/config.py`](price_collector/config.py) | Defines and validates worker, artifact, cadence, evaluation, queue, and retention settings. |
| Production process | [`deployment/price-collector-shadow-signal.service`](deployment/price-collector-shadow-signal.service) and [`deployment/shadow-signal.env.example`](deployment/shadow-signal.env.example) | Define the isolated systemd service and its environment contract. |

The central live path is:

```text
futures + Chainlink Redis values
        -> shadow_signal_collector.py
        -> shadow_signal.py
        -> shadow_signal_evaluation.py
        -> db.py / shadow_signal_evaluations
```

The model-selection path is separate:

```text
raw capture
        -> shadow_signal_replay.py
        -> shadow_signal_selection.py
        -> frozen selection artifact
        -> shadow_signal_artifact.py
        -> live worker activation
```

If the question is specifically “where would we change the formula?”, the
answer is `price_collector/shadow_signal.py`. If the question is “where is
projected paired with actual?”, the answer is
`price_collector/shadow_signal_evaluation.py`.

## Related implementation references

- [`engine.md`](engine.md) — original signal definition and model rationale
- [`README.md`](README.md) — implemented replay, selection, worker, and
  evaluation behavior
- [`OPERATIONS.md`](OPERATIONS.md#shadow-signal-phase-2-raw-replay) — replay
  and selection procedures
- [`price_collector/shadow_signal.py`](price_collector/shadow_signal.py) — pure
  anchored-ratio engine
- [`price_collector/shadow_signal_evaluation.py`](price_collector/shadow_signal_evaluation.py)
  — causal maturation and evaluation records
