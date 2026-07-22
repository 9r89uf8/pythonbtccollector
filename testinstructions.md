You do **not** need to upload the 200 markets here. Run the complete chronological test locally and only share the small result files afterward.

## Download the backtest

[Download the complete backtest bundle](sandbox:/mnt/data/chainlink_hybrid_backtest_bundle.zip)

Individual files:

* [Python backtest script](sandbox:/mnt/data/chainlink_hybrid_backtest.py)
* [Setup and interpretation guide](sandbox:/mnt/data/CHAINLINK_HYBRID_BACKTEST_README.md)
* [Python requirements](sandbox:/mnt/data/chainlink_hybrid_requirements.txt)

I tested the script end-to-end against the 20 uploaded markets. It successfully parsed all 20 files, processed 11,879 scored predictions, performed chronological model selection, and generated the intended reports. The script is tailored to your JSON structure, including the 3,000 ms forecast horizon, 500 ms evaluation cadence, source and receipt timestamps, current Chainlink price, futures price, projected Chainlink price, and actual Chainlink outcome.  

## Run it on the 200 markets

Install the two dependencies:

```powershell
python -m pip install -r chainlink_hybrid_requirements.txt
```

Then run:

```powershell
python chainlink_hybrid_backtest.py --input-dir "C:\Users\alexa\PycharmProjects\polycollector\YOUR_MARKET_DIRECTORY" --pattern "btc_5m_market_*_shadow_evaluations_catchup_ratio_l3000_b100.json" --output-dir "C:\Users\alexa\PycharmProjects\polycollector\chainlink_hybrid_results" --test-markets 40 --min-train-markets 40 --validation-block-markets 10 --validation-step-markets 10
```

Replace `YOUR_MARKET_DIRECTORY` with the directory containing the 200 JSON files.

The file pattern is deliberately narrow. It avoids accidentally mixing rounded exports, duplicate market files, other model versions, or other horizon configurations.

## Test structure

For 200 chronologically ordered markets, the default recommended setup is:

```text
First 160 markets: model development
Last 40 markets:   untouched final test
```

Inside the first 160 markets, it uses expanding-window validation:

```text
Train 1–40   → validate 41–50
Train 1–50   → validate 51–60
Train 1–60   → validate 61–70
...
Train 1–150  → validate 151–160
```

Only after choosing the basis window, estimator, threshold, model family, and coefficients does it evaluate the final 40 markets.

This avoids selecting parameters using the same markets on which performance is reported.

## Models it compares

The script compares six approaches.

### 1. No-change baseline

```text
prediction = current_chainlink
```

### 2. Existing lag projection

```text
prediction = existing_projected_chainlink
```

### 3. Calibrated lag projection

```text
lag_move = existing_projection − current_chainlink

prediction =
    current_chainlink
    + lag_scale × lag_move
```

This is an essential benchmark. The 20-market analysis suggested that much of the improvement comes from shrinking an overly aggressive lag prediction, so a hybrid should be judged against **calibrated lag**, not only against the original projection.

### 4. Scaled basis-only prediction

```text
prediction =
    current_chainlink
    + basis_scale × basis_move
```

This determines whether the basis has independent predictive value.

### 5. Direct convex combination

```text
prediction =
    current_chainlink
    + lag_weight × lag_move
    + basis_weight × basis_move
```

Subject to:

```text
lag_weight >= 0
basis_weight >= 0
lag_weight + basis_weight <= 1
```

The unused weight is shrinkage toward the current Chainlink price.

### 6. Basis-confirmed gated lag

This does not add the two signals together. Instead, it asks whether the basis confirms the lag projection.

```text
agreement:
    both signals are sufficiently large
    and both have the same direction

disagreement:
    both signals are sufficiently large
    but have opposite directions

neutral:
    one or both signals are small
```

Then:

```text
prediction =
    current_chainlink
    + group_specific_scale × lag_move
```

The model learns separate scales for agreement, disagreement, and neutral situations.

This directly tests the pattern seen in the 20 markets: the basis may be more useful as a **confirmation or confidence feature** than as a second full-strength price prediction.

## Basis calculations tested

The script tests two causal methods.

### Prior rolling row basis

```text
current_basis_bps =
    10,000 × (futures_now / chainlink_now − 1)

normal_basis_bps =
    mean or median of previous basis observations only

basis_implied_chainlink =
    futures_now / (1 + normal_basis_bps / 10,000)
```

It searches these rolling windows:

```text
30 seconds
60 seconds
120 seconds
300 seconds
600 seconds
```

### Synchronized basis

For each distinct Chainlink source observation, the script finds the latest sampled futures quote whose source timestamp is no later than the Chainlink source timestamp.

It then calculates:

```text
synchronized_basis_bps =
    10,000 × (
        futures_at_chainlink_source_time
        / chainlink_update_price
        − 1
    )
```

The observation is not considered available until both quotes have actually been received.

This is more appropriate for estimating the structural futures basis. The script approximates the futures tape from the observations stored in the JSON files. Since your backend likely has the complete futures tick stream, the most accurate production version would use that raw stream inside `build_synchronized_basis_events()`.

## Handling overlapping predictions

Your forecasts occur every 500 milliseconds but predict three seconds ahead. Therefore, neighboring outcomes overlap heavily.

The script handles this in two ways:

```text
Fitting and parameter selection:
    one forecast at least every three seconds

Final reporting:
    all valid 500 ms predictions
    plus a separate non-overlapping three-second evaluation
```

It also bootstraps **blocks of markets**, rather than pretending that thousands of 500 ms predictions are independent observations.

## Output files

The output directory will contain:

```text
report.md
selected_models.json
cv_candidate_summary.csv
cv_market_scores.csv
cv_fitted_parameters.csv
holdout_metrics_all_rows.csv
holdout_metrics_nonoverlap.csv
holdout_per_market.csv
holdout_predictions.csv
markets_loaded.csv
```

The most important are:

### `report.md`

Readable final summary of validation selection, holdout performance, fitted coefficients, and confidence intervals.

### `selected_models.json`

Contains the exact selected basis method, rolling window, gate threshold, and coefficients required for implementation.

### `holdout_metrics_nonoverlap.csv`

The cleaner statistical comparison because observations are spaced by the three-second forecast horizon.

### `holdout_per_market.csv`

Shows whether improvement was consistent or caused by only a few volatile markets.

### `holdout_predictions.csv`

Contains every final holdout prediction and error. Use this to inspect the largest wins and failures.

## How to judge the result

The main comparison is:

```text
selected hybrid or gate
versus
scaled_lag
```

Not merely:

```text
selected model
versus
raw_lag
```

In the report, the market-level comparison is defined as:

```text
delta =
    challenger market MAE
    − calibrated-lag market MAE
```

Therefore:

```text
negative delta = challenger is better
positive delta = calibrated lag is better
```

I would consider the basis contribution convincingly supported when:

1. Its non-overlapping MAE is below calibrated lag.
2. Its RMSE and p95 error are not worse.
3. Its market-block bootstrap has `ci_high < 0`.
4. It wins in substantially more than half of the final 40 markets.
5. Its selected parameters are reasonably stable across validation folds.
6. Similar results appear across different sessions or days.

For example:

```text
mean_delta = -$0.04
95% CI = [-$0.07, -$0.01]
market win rate = 67%
```

That would be credible evidence that the hybrid adds value.

This result would not be sufficient:

```text
mean_delta = -$0.01
95% CI = [-$0.05, +$0.03]
market win rate = 52%
```

That would mean the average result is slightly favorable, but the evidence does not separate it from normal market-to-market variation.

## Conservative versus research selection

The default is:

```text
--selection-rule one_se
```

This uses the one-standard-error rule. It chooses the simplest model statistically close to the validation winner.

That means:

* If a complicated hybrid is clearly better, it can be selected.
* If the hybrid is only marginally better, the script will retain calibrated lag.
* The best-performing hybrid is still evaluated as a challenger.

A second research run can use:

```powershell
python chainlink_hybrid_backtest.py ... --selection-rule best
```

Do not repeatedly modify the model after examining the final 40 markets. That would convert the final holdout into another validation set.

After running it, upload only [`report.md` and `selected_models.json`]; those two small files are enough to interpret the full 200-market result and translate the selected model into backend prediction code.
