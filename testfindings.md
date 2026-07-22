# Yes—more markets are needed, but these 20 already reveal the main improvement

I analyzed markets **5948878–5948897** as one continuous 100-minute sequence. They contain **11,879 scored forecasts** and 121 invalid rows. All use the same three-second horizon, 500-millisecond cadence, and model version.  

I constructed a causal basis signal by:

1. Matching each Chainlink observation to futures at the Chainlink source timestamp.
2. Estimating the normal basis using only previously received observations.
3. Training the weights and basis window on the **first five markets**.
4. Freezing the model and evaluating it on the **remaining 15 markets**.

## Holdout results

The basis window selected using only the first five markets was a **10-minute rolling mean** of the synchronized basis.

| Model                     |        MAE |       RMSE | p95 absolute error |
| ------------------------- | ---------: | ---------: | -----------------: |
| No-change prediction      |     $1.303 |     $2.827 |             $6.084 |
| Existing lag projection   |     $1.121 |     $2.159 |             $4.672 |
| Calibrated lag projection | **$0.933** |     $1.683 |             $3.552 |
| Lag + basis hybrid        | **$0.930** | **$1.620** |         **$3.352** |

The fitted hybrid was:

```text
hybrid_price =
    0.333 × current_chainlink
    + 0.585 × lag_projected_chainlink
    + 0.083 × basis_implied_chainlink
```

Equivalently:

```text
hybrid_move =
    0.585 × lag_move
    + 0.083 × basis_move
```

The remaining 33.3% is shrinkage toward no change.

## What this means

The existing lag projection is **too aggressive in this batch**. The first and most important improvement is:

```text
calibrated_lag_move ≈ 0.67 × original_lag_move
```

That alone reduced holdout MAE from `$1.121` to `$0.933`, approximately a **16.8% improvement**.

Adding the basis signal reduced MAE only slightly further:

```text
$0.933 → $0.930
```

That is an incremental improvement of only about **$0.0026 per prediction**, or **0.28%**. The hybrid beat calibrated lag in **9 of the 15 holdout markets**, but the market-level confidence interval includes no improvement. In other words, the current evidence does **not yet establish that the basis component improves average MAE**.

The basis signal is more encouraging for larger errors:

```text
p95 error: $3.552 → $3.352
RMSE:      $1.683 → $1.620
```

That suggests it may be useful as a **tail-risk correction or confidence signal**, even though its average-MAE benefit is currently very small.

A non-overlapping evaluation—one forecast at least every three seconds—produced essentially the same result:

```text
raw lag MAE:        $1.168
calibrated lag MAE: $0.964
hybrid MAE:         $0.963
```

So the conclusion is not an artifact of counting overlapping 500-millisecond forecasts multiple times.

## How many more markets?

For the next useful checkpoint, upload **at least 80 additional consecutive markets**, bringing the total to approximately **100 markets**.

For a production decision about the basis component, **200–300 markets** would be substantially better. A rough paired-market power calculation suggests approximately 250 independent holdout markets would be required to distinguish an incremental MAE effect as small as the one currently observed.

The new markets should ideally:

* Be consecutive and unfiltered.
* Cover several different hours or days rather than one continuous session.
* Include both quiet and volatile periods.
* Retain the same fields, model version, horizon, and cadence.
* Include at least **10 minutes of warm-up data** before each separate block, because the current best basis estimate uses a 10-minute history.

A practical collection design would be **five blocks of 20 markets on different sessions or days**, plus two warm-up markets before each block. The immediate model change supported by the current data is to **shrink the lag move by roughly one-third**; the basis blend should remain a challenger until the larger multi-regime sample confirms its incremental value.
