# TWAP leader-loss risk: full available history

Requested cohort: **2026-08-16T00:00:00.000Z** through **2026-09-12T21:15:00.000Z** (exclusive). Official outcomes read at **2026-09-12T22:17:21.776Z**.

The export contains **8,014 markets / 64,112 checkpoints**. Of 8,031 calendar slots, 17 lack stored market metadata; they are outside the observed sample. The first stored market starts 2026-08-16T01:25:00.000Z.

Each cell shows **L/resolved = loss rate [pointwise 95% Wilson interval]; U=unknown outcomes**. N = resolved + U. A loss means the checkpoint's TWAP leader differs from the verified official winner. Unknown outcomes remain counted; rates use resolved observations only. Empty or all-unknown cells have unavailable rates.

X is TWAP distance from the stream-observed Price to Beat in the leader's direction, in basis points. Y is Chainlink spot minus TWAP in that direction, using the same Price to Beat denominator. Negative Y means spot trails TWAP; it does not alone mean spot crossed the Price to Beat.

## Coverage and overall loss rates

| Seconds remaining | Rows | Inputs unavailable | Available ties | Eligible N | Losses / resolved; 95% interval; U |
|---:|---:|---:|---:|---:|:---|
| 120 | 8014 | 516 | 0 | 7498 | 1568/7471 = 20.99% [20.08%, 21.93%]; U=27 |
| 90 | 8014 | 541 | 0 | 7473 | 1266/7447 = 17.00% [16.16%, 17.87%]; U=26 |
| 60 | 8014 | 527 | 0 | 7487 | 904/7460 = 12.12% [11.40%, 12.88%]; U=27 |
| 30 | 8014 | 543 | 0 | 7471 | 478/7444 = 6.42% [5.89%, 7.00%]; U=27 |
| 15 | 8014 | 525 | 0 | 7489 | 282/7462 = 3.78% [3.37%, 4.24%]; U=27 |
| 10 | 8014 | 537 | 0 | 7477 | 194/7451 = 2.60% [2.27%, 2.99%]; U=26 |
| 5 | 8014 | 521 | 0 | 7493 | 110/7466 = 1.47% [1.22%, 1.77%]; U=27 |
| 3 | 8014 | 517 | 0 | 7497 | 84/7471 = 1.12% [0.91%, 1.39%]; U=26 |

Unavailable-reason counts can overlap; see [coverage.csv](coverage.csv) and [exclusions.csv](exclusions.csv). [Outcome accounting](outcome_accounting.csv) separates eligible unknowns from unavailable inputs and ties. Missing calendar IDs and source-market guard evidence are in [manifest.json](manifest.json).

## 120 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 103/135 = 76.30% [68.46%, 82.68%]; U=0 | 248/493 = 50.30% [45.91%, 54.70%]; U=1 | 162/557 = 29.08% [25.47%, 32.99%]; U=1 | 18/116 = 15.52% [10.05%, 23.20%]; U=0 |
| [1,2) | 77/118 = 65.25% [56.30%, 73.24%]; U=0 | 137/373 = 36.73% [32.00%, 41.73%]; U=0 | 95/462 = 20.56% [17.13%, 24.48%]; U=2 | 15/124 = 12.10% [7.47%, 19.00%]; U=1 |
| [2,4) | 105/180 = 58.33% [51.03%, 65.29%]; U=2 | 157/524 = 29.96% [26.20%, 34.02%]; U=0 | 93/663 = 14.03% [11.59%, 16.88%]; U=0 | 23/234 = 9.83% [6.64%, 14.32%]; U=0 |
| [4,8) | 104/270 = 38.52% [32.91%, 44.45%]; U=2 | 73/498 = 14.66% [11.82%, 18.04%]; U=0 | 47/672 = 6.99% [5.30%, 9.18%]; U=1 | 21/342 = 6.14% [4.05%, 9.20%]; U=3 |
| [8,infinity) | 51/341 = 14.96% [11.56%, 19.13%]; U=4 | 14/319 = 4.39% [2.63%, 7.23%]; U=3 | 11/464 = 2.37% [1.33%, 4.19%]; U=3 | 14/586 = 2.39% [1.43%, 3.97%]; U=4 |

## 90 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 98/115 = 85.22% [77.60%, 90.56%]; U=0 | 250/480 = 52.08% [47.62%, 56.52%]; U=1 | 124/491 = 25.25% [21.61%, 29.28%]; U=1 | 15/126 = 11.90% [7.35%, 18.72%]; U=2 |
| [1,2) | 90/118 = 76.27% [67.84%, 83.04%]; U=1 | 116/339 = 34.22% [29.37%, 39.42%]; U=1 | 60/447 = 13.42% [10.57%, 16.90%]; U=1 | 6/112 = 5.36% [2.48%, 11.20%]; U=0 |
| [2,4) | 110/191 = 57.59% [50.50%, 64.38%]; U=1 | 96/493 = 19.47% [16.22%, 23.20%]; U=0 | 57/644 = 8.85% [6.89%, 11.30%]; U=0 | 13/226 = 5.75% [3.39%, 9.59%]; U=0 |
| [4,8) | 86/245 = 35.10% [29.40%, 41.27%]; U=1 | 55/513 = 10.72% [8.33%, 13.70%]; U=2 | 20/675 = 2.96% [1.93%, 4.53%]; U=0 | 7/362 = 1.93% [0.94%, 3.94%]; U=1 |
| [8,infinity) | 47/364 = 12.91% [9.85%, 16.75%]; U=4 | 3/371 = 0.81% [0.28%, 2.35%]; U=3 | 9/502 = 1.79% [0.95%, 3.37%]; U=4 | 4/633 = 0.63% [0.25%, 1.61%]; U=3 |

## 60 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 106/119 = 89.08% [82.20%, 93.50%]; U=0 | 219/451 = 48.56% [43.98%, 53.16%]; U=0 | 57/457 = 12.47% [9.75%, 15.82%]; U=2 | 6/91 = 6.59% [3.06%, 13.65%]; U=0 |
| [1,2) | 80/99 = 80.81% [71.96%, 87.35%]; U=1 | 97/346 = 28.03% [23.56%, 32.99%]; U=1 | 23/397 = 5.79% [3.89%, 8.54%]; U=0 | 5/91 = 5.49% [2.37%, 12.22%]; U=0 |
| [2,4) | 108/170 = 63.53% [56.07%, 70.39%]; U=1 | 50/466 = 10.73% [8.23%, 13.87%]; U=1 | 29/677 = 4.28% [3.00%, 6.08%]; U=0 | 2/214 = 0.93% [0.26%, 3.34%]; U=3 |
| [4,8) | 80/247 = 32.39% [26.86%, 38.46%]; U=0 | 10/509 = 1.96% [1.07%, 3.58%]; U=2 | 7/714 = 0.98% [0.48%, 2.01%]; U=1 | 2/353 = 0.57% [0.16%, 2.04%]; U=2 |
| [8,infinity) | 21/437 = 4.81% [3.16%, 7.23%]; U=1 | 2/438 = 0.46% [0.13%, 1.65%]; U=7 | 0/579 = 0.00% [0.00%, 0.66%]; U=2 | 0/605 = 0.00% [0.00%, 0.63%]; U=3 |

## 30 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 78/82 = 95.12% [88.12%, 98.09%]; U=0 | 170/417 = 40.77% [36.16%, 45.55%]; U=0 | 19/456 = 4.17% [2.68%, 6.42%]; U=2 | 0/90 = 0.00% [0.00%, 4.09%]; U=1 |
| [1,2) | 77/95 = 81.05% [72.03%, 87.67%]; U=0 | 36/284 = 12.68% [9.30%, 17.05%]; U=1 | 5/378 = 1.32% [0.57%, 3.06%]; U=1 | 1/116 = 0.86% [0.15%, 4.72%]; U=0 |
| [2,4) | 65/141 = 46.10% [38.08%, 54.32%]; U=0 | 8/530 = 1.51% [0.77%, 2.95%]; U=0 | 0/658 = 0.00% [0.00%, 0.58%]; U=0 | 0/178 = 0.00% [0.00%, 2.11%]; U=2 |
| [4,8) | 17/247 = 6.88% [4.34%, 10.74%]; U=3 | 1/563 = 0.18% [0.03%, 1.00%]; U=2 | 0/694 = 0.00% [0.00%, 0.55%]; U=2 | 0/301 = 0.00% [0.00%, 1.26%]; U=0 |
| [8,infinity) | 1/464 = 0.22% [0.04%, 1.21%]; U=1 | 0/462 = 0.00% [0.00%, 0.82%]; U=3 | 0/658 = 0.00% [0.00%, 0.58%]; U=1 | 0/630 = 0.00% [0.00%, 0.61%]; U=8 |

## 15 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 83/91 = 91.21% [83.60%, 95.48%]; U=0 | 132/420 = 31.43% [27.17%, 36.02%]; U=0 | 9/449 = 2.00% [1.06%, 3.77%]; U=0 | 0/75 = 0.00% [0.00%, 4.87%]; U=1 |
| [1,2) | 33/85 = 38.82% [29.16%, 49.45%]; U=0 | 4/303 = 1.32% [0.51%, 3.34%]; U=0 | 1/375 = 0.27% [0.05%, 1.49%]; U=1 | 0/95 = 0.00% [0.00%, 3.89%]; U=1 |
| [2,4) | 17/150 = 11.33% [7.20%, 17.40%]; U=1 | 2/513 = 0.39% [0.11%, 1.41%]; U=0 | 0/608 = 0.00% [0.00%, 0.63%]; U=0 | 0/177 = 0.00% [0.00%, 2.12%]; U=1 |
| [4,8) | 1/212 = 0.47% [0.08%, 2.62%]; U=2 | 0/612 = 0.00% [0.00%, 0.62%]; U=2 | 0/719 = 0.00% [0.00%, 0.53%]; U=1 | 0/300 = 0.00% [0.00%, 1.26%]; U=4 |
| [8,infinity) | 0/440 = 0.00% [0.00%, 0.87%]; U=0 | 0/512 = 0.00% [0.00%, 0.74%]; U=4 | 0/684 = 0.00% [0.00%, 0.56%]; U=5 | 0/642 = 0.00% [0.00%, 0.59%]; U=4 |

## 10 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 79/93 = 84.95% [76.30%, 90.82%]; U=0 | 88/390 = 22.56% [18.69%, 26.97%]; U=0 | 4/446 = 0.90% [0.35%, 2.28%]; U=1 | 1/75 = 1.33% [0.24%, 7.17%]; U=0 |
| [1,2) | 17/76 = 22.37% [14.46%, 32.93%]; U=0 | 2/315 = 0.63% [0.17%, 2.29%]; U=0 | 1/378 = 0.26% [0.05%, 1.48%]; U=0 | 0/83 = 0.00% [0.00%, 4.42%]; U=1 |
| [2,4) | 2/147 = 1.36% [0.37%, 4.82%]; U=2 | 0/509 = 0.00% [0.00%, 0.75%]; U=0 | 0/615 = 0.00% [0.00%, 0.62%]; U=0 | 0/178 = 0.00% [0.00%, 2.11%]; U=0 |
| [4,8) | 0/212 = 0.00% [0.00%, 1.78%]; U=1 | 0/620 = 0.00% [0.00%, 0.62%]; U=2 | 0/728 = 0.00% [0.00%, 0.52%]; U=1 | 0/280 = 0.00% [0.00%, 1.35%]; U=4 |
| [8,infinity) | 0/432 = 0.00% [0.00%, 0.88%]; U=1 | 0/540 = 0.00% [0.00%, 0.71%]; U=3 | 0/685 = 0.00% [0.00%, 0.56%]; U=5 | 0/649 = 0.00% [0.00%, 0.59%]; U=5 |

## 5 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 48/82 = 58.54% [47.73%, 68.58%]; U=0 | 53/412 = 12.86% [9.97%, 16.44%]; U=0 | 6/435 = 1.38% [0.63%, 2.98%]; U=1 | 0/83 = 0.00% [0.00%, 4.42%]; U=0 |
| [1,2) | 2/80 = 2.50% [0.69%, 8.66%]; U=1 | 0/336 = 0.00% [0.00%, 1.13%]; U=1 | 0/358 = 0.00% [0.00%, 1.06%]; U=0 | 0/90 = 0.00% [0.00%, 4.09%]; U=0 |
| [2,4) | 1/142 = 0.70% [0.12%, 3.88%]; U=2 | 0/535 = 0.00% [0.00%, 0.71%]; U=0 | 0/612 = 0.00% [0.00%, 0.62%]; U=0 | 0/154 = 0.00% [0.00%, 2.43%]; U=1 |
| [4,8) | 0/215 = 0.00% [0.00%, 1.76%]; U=0 | 0/610 = 0.00% [0.00%, 0.63%]; U=0 | 0/719 = 0.00% [0.00%, 0.53%]; U=4 | 0/283 = 0.00% [0.00%, 1.34%]; U=3 |
| [8,infinity) | 0/413 = 0.00% [0.00%, 0.92%]; U=2 | 0/565 = 0.00% [0.00%, 0.68%]; U=2 | 0/685 = 0.00% [0.00%, 0.56%]; U=5 | 0/657 = 0.00% [0.00%, 0.58%]; U=5 |

## 3 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 32/74 = 43.24% [32.57%, 54.59%]; U=0 | 44/405 = 10.86% [8.19%, 14.27%]; U=0 | 5/444 = 1.13% [0.48%, 2.61%]; U=0 | 0/83 = 0.00% [0.00%, 4.42%]; U=0 |
| [1,2) | 3/92 = 3.26% [1.12%, 9.15%]; U=2 | 0/321 = 0.00% [0.00%, 1.18%]; U=1 | 0/361 = 0.00% [0.00%, 1.05%]; U=0 | 0/83 = 0.00% [0.00%, 4.42%]; U=1 |
| [2,4) | 0/129 = 0.00% [0.00%, 2.89%]; U=0 | 0/530 = 0.00% [0.00%, 0.72%]; U=1 | 0/620 = 0.00% [0.00%, 0.62%]; U=0 | 0/154 = 0.00% [0.00%, 2.43%]; U=1 |
| [4,8) | 0/214 = 0.00% [0.00%, 1.76%]; U=0 | 0/602 = 0.00% [0.00%, 0.63%]; U=1 | 0/745 = 0.00% [0.00%, 0.51%]; U=2 | 0/277 = 0.00% [0.00%, 1.37%]; U=4 |
| [8,infinity) | 0/433 = 0.00% [0.00%, 0.88%]; U=2 | 0/541 = 0.00% [0.00%, 0.71%]; U=3 | 0/728 = 0.00% [0.00%, 0.52%]; U=4 | 0/635 = 0.00% [0.00%, 0.60%]; U=4 |

## One date comparison

The marker is **2026-09-10T02:30:00.000Z**, the first full market after deployment of `ace8d19`. The September 10 deployment `ace8d19` did not modify these spot/TWAP writers or official-resolution parsing, and the TWAP connection spanned deployment. Earlier valid history remains in every primary grid. This split is descriptive: overlapping intervals do not establish equal risk, and different X/Y composition can change overall rates.

| Seconds remaining | Before marker: losses / resolved; 95% interval; U | At/after marker: losses / resolved; 95% interval; U |
|---:|:---|:---|
| 120 | 1401/6706 = 20.89% [19.94%, 21.88%]; U=27 | 167/765 = 21.83% [19.05%, 24.89%]; U=0 |
| 90 | 1125/6682 = 16.84% [15.96%, 17.75%]; U=26 | 141/765 = 18.43% [15.84%, 21.33%]; U=0 |
| 60 | 815/6693 = 12.18% [11.42%, 12.98%]; U=27 | 89/767 = 11.60% [9.53%, 14.06%]; U=0 |
| 30 | 433/6677 = 6.48% [5.92%, 7.10%]; U=27 | 45/767 = 5.87% [4.41%, 7.76%]; U=0 |
| 15 | 256/6694 = 3.82% [3.39%, 4.31%]; U=27 | 26/768 = 3.39% [2.32%, 4.91%]; U=0 |
| 10 | 176/6680 = 2.63% [2.28%, 3.05%]; U=26 | 18/771 = 2.33% [1.48%, 3.66%]; U=0 |
| 5 | 99/6700 = 1.48% [1.22%, 1.80%]; U=27 | 11/766 = 1.44% [0.80%, 2.55%]; U=0 |
| 3 | 76/6705 = 1.13% [0.91%, 1.42%]; U=26 | 8/766 = 1.04% [0.53%, 2.05%]; U=0 |

The same split by X/Y cell appears once in [comparison_cells.csv](comparison_cells.csv). [Direction totals](direction_totals.csv), [direction cells](direction_cells.csv), [daily totals](daily_totals.csv), and [daily cells](daily_cells.csv) are descriptive breakdowns; days use UTC market-start dates. The complete primary table is [grids.csv](grids.csv).

## Interpretation limits

These are sampled-history conditional loss rates, not fill probabilities, profitability, or evidence that a cell will stay below 1%. The opening reference must have an exact boundary TWAP event received by the checkpoint; missing or conflicting references exclude the checkpoint. Spot is retained one-second history with same-second upserts, not a complete replay. Selected prices must pass both source and receipt age, inclusively 0–3,000 ms; the number using exactly 3,000 ms source age is recorded in coverage.csv. A later fresh tick cannot repair an earlier missing decision.

Wilson intervals are pointwise descriptions under independent comparable-market assumptions. They do not adjust for temporal dependence, searching 160 cells, or future regime changes. Repeated checkpoints in one market are never pooled into a larger independent sample. Low observed losses and zero-loss cells can still have wide intervals. Any optional future validation requires its own frozen condition and fresh observations.
