# September 11 carry-forward reconstruction check

This is an offline check on **2026-09-11 00:00:00 UTC through September 12 00:00:00 UTC**, using the existing hashed [source export — retained locally; provenance](../pilot_review/extraction.json). No additional database queries ran. The day is separate from the September 1–8 pilot week, but it was already examined in the timestamp-alignment audit; it is not held-out validation.

For each observed TWAP source second `u`, both methods use the same candidate constituent slots `[u−62 seconds, u−3 seconds]` (`a=−3`). Carry-forward assigns each of the 60 slots its latest retained Chainlink spot at or before that slot, with a strict source-second age below 600 seconds. The comparison baseline averages only the actual retained observations inside those same slots. Errors are `10000 × abs(reconstruction − TWAP) / TWAP` in basis points.

Prices, sums, means, residuals and quantiles use Decimal arithmetic with 80-digit working precision. Quantiles interpolate at `(n−1) × p`. The CSV contains the full-precision values; the following table rounds only their display.

| Cohort and method | Windows | Median absolute error (bp) | 99th percentile (bp) | Maximum (bp) |
| --- | ---: | ---: | ---: | ---: |
| Complete; both methods identical | 27,434 | 0.00000000000317 | 0.205766 | 1.395073 |
| Incomplete; carry-forward | 56,807 | 0.002584 | 0.266187 | 1.073072 |
| Incomplete; retained-only mean | 56,807 | 0.020777 | 0.437664 | 3.554974 |
| All paired; carry-forward | 84,241 | 0.001109 | 0.250297 | 1.395073 |
| All paired; retained-only mean | 84,241 | 0.011634 | 0.374557 | 3.554974 |

Carry-forward has lower absolute error in **46,867** incomplete windows and higher error in **9,940**. It improves the aggregate fit on this day without dominating every window. Incomplete windows contain 47–59 retained observations; complete-window reconstructions coincide exactly.

## Coverage and seeds

- All **84,241** exported TWAP events have unique, nonconflicting exact source seconds. There are **2,159** calendar seconds without an exported TWAP event; no target TWAP values are invented for them.
- The input contains 84,383 spot rows including margins. Its first retained spot is at `day start − 63 seconds`, before the earliest required slot at `day start − 62 seconds`.
- The required constituent grid spans 86,459 seconds, including pre-day context: **84,375** slots have exact retained spot rows and **2,084** use carry-forward.
- There are **1,752** runs of absent retained source seconds, including **1,669** single-second runs. The longest run and maximum carried source-second age are both **8 seconds**. Neither end of the required grid falls inside a missing run. These are source-grid missingness statistics, not measured connection outages.
- **Zero** candidate windows require an unavailable pre-export seed, an expired seed, or a retained-only mean with zero observations. These conditions are nevertheless checked and excluded explicitly. No price before the export is assumed. A seed exactly 600 seconds old fails the strict lookback condition.

This compares retrospective source-time reconstructions. It does not test receipt-time availability, frontend delay, settlement-side forecasts, or profitability. Retained spot rows still have the same-second-upsert limitation.

## Reproducibility

[local_day_check.py](local_day_check.py) validates the original export and extraction SQL hashes, performs a rolling source-second calculation, and refuses to overwrite existing outputs. [local_day_summary.csv](local_day_summary.csv) contains all summaries and maximum-error timestamps. [local_day_manifest.json](local_day_manifest.json) records coverage, exclusions, conventions and input/code/output hashes. The offline calculation completed in **5.860 seconds**.

Focused checks cover an exact persistent step, a missing-stamp run where carry-forward and retained-only means differ, absent initial seeds, the strict 600-second boundary, both ends of the inclusive 60-slot window, linear median/99th-percentile calculation, and 18-decimal-place sums under a deliberately low ambient Decimal precision. Run only those checks without changing artifacts:

```powershell
python research/spot_twap_response/carry_forward_review/local_day_check.py --self-check
```

The earlier week-long projection reproduction and all external pilot files remain unchanged.
