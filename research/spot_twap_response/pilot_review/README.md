# Independent one-day spot/TWAP timing diagnostic

For **2026-09-11 UTC**, shift **a = −3 seconds** gives the smallest median and
p99 absolute residual among the nine tested shifts. An independent PostgreSQL
calculation using the external pilot's exact target table confirms the result.
This checks one day of source-time alignment; it does not reproduce the
external week's results, its longer cohort, or its settlement projections.

## Inputs and signed shift

- TWAP: 84,241 durable `polymarket_twap_events` records with topic
  `crypto_prices_twap_sixty`, symbol `btc/usd`, window 60, and provider timestamps
  in `[2026-09-11T00:00:00Z, 2026-09-12T00:00:00Z)`.
- Spot: `price_samples` instrument 2, verified as Chainlink RTDS BTC/USD.
  There are 84,316 retained seconds inside the day and 84,383 including the
  required 63-second leading and 4-second trailing margins.
- All exported provider timestamps equal their whole-second sample keys.
  TWAP has no duplicate or conflicting exact provider timestamps on this day.
- `raw_capture.chainlink_price_events` has **zero rows received on this day**.
  All six attached raw partitions report zero storage bytes. No whole-history
  source-data scan was run.

For a TWAP value **W(u)** at source timestamp u, define:

```text
mean_a(u) = [S(u + a seconds) + ... + S(u + a seconds − 59 seconds)] / 60
absolute residual (bp) = 10000 × |mean_a(u) − W(u)| / W(u)
```

The **newest** spot source timestamp is **u + a seconds**. Thus a = −3 uses
`[u−62 seconds, u−3 seconds]`; a = −2 uses `[u−61 seconds, u−2 seconds]`.
Positive shifts use spot timestamps later than u. Every accepted window has
all **60 unique consecutive retained seconds**. Missing seconds are never
filled or reweighted.

## Results

Values below are basis points, rounded for display. The tiny −3 median is
shown explicitly rather than reported as zero. [shift_summary.csv](shift_summary.csv)
retains the full Decimal results.

| a (seconds) | Accepted n | Median absolute bp | p99 absolute bp | Maximum absolute bp |
|---:|---:|---:|---:|---:|
| −4 | 27,415 | 0.042596 | 0.522772 | 2.066466 |
| −3 | 27,434 | 3.1744 × 10⁻¹² | 0.205766 | 1.395073 |
| −2 | 27,469 | 0.025002 | 0.371231 | 1.534423 |
| −1 | 27,478 | 0.065052 | 0.740491 | 2.068146 |
| 0 | 27,933 | 0.104070 | 1.124851 | 3.097058 |
| 1 | 27,935 | 0.142659 | 1.512216 | 4.109089 |
| 2 | 27,938 | 0.180293 | 1.901785 | 5.075313 |
| 3 | 27,938 | 0.218146 | 2.282309 | 6.027288 |
| 4 | 27,938 | 0.255920 | 2.648088 | 6.965290 |

The **common cohort of 24,285 TWAP timestamps** has complete windows at all
nine shifts. It also favors −3: median **3.1139 × 10⁻¹² bp**, p99
**0.200274 bp**, maximum **1.395073 bp**. At −2 its median is **0.025469 bp**
and p99 **0.369106 bp**. All common-cohort values are in the CSV and manifest.

Strict 60-second completeness admits only about one third of the day's
84,241 TWAP timestamps. The full day is not represented by those accepted
windows; missingness and retained same-second spot upserts remain material
limits. Upserts erase earlier values and potential within-second conflicts,
which the absent raw capture cannot audit.

## Independent check and numerical method

[crosscheck.sql](crosscheck.sql) uses the external pilot's materialized TWAP
target, `price_samples` instrument 4, on the same bounded day. It finds
**84,241 matching timestamps, no missing rows in either direction, no price
differences, and no source/sample timestamp mismatches** against durable TWAP.
It independently computes shifts −4, −3, −2 and −1 with PostgreSQL `NUMERIC`,
60-row range-window counts, and ordered numeric quantiles. Counts match the
local calculation; the largest numerical summary difference is
**8.4590026526 × 10⁻⁷¹ bp**. The target-table choice therefore does not explain
the −3 result on this day. See [crosscheck.stdout.jsonl](crosscheck.stdout.jsonl).

[calculate.py](calculate.py) uses 80-digit Decimal arithmetic. Median and p99
sort absolute residuals and interpolate linearly at `(n−1) × p`, for p = 0.5
and p = 0.99. Prices and residuals never pass through binary floating point.
Its self-checks verify exact 60-second sums, rejection of a missing second,
median/p99 interpolation, and 18-place sums under low ambient precision.
It also verifies input hashes and the independent SQL summaries.

All database calls used read-only repeatable-read transactions, a 20-second
statement timeout, a 2-second lock timeout, and bounded source ranges.
The raw file remains local and is ignored by Git:

```text
sources.csv SHA-256:
729c46d042d8eb9178cb901c67b60f14abceb053187e6c57ee5542a40df9b98f
```

[manifest.json](manifest.json) records all input, SQL, code and summary hashes;
[extraction.json](extraction.json) and [crosscheck_execution.json](crosscheck_execution.json)
record successful process exits and execution times. With the preserved local
export, reproduce the summaries from the repository root with:

```bash
python research/spot_twap_response/pilot_review/calculate.py
```

No receipt-time decision replay, future projection, official settlement-rule
reconstruction, or universal offset has been established. The nine-shift
comparison is descriptive and does not validate the selected shift on new data.
