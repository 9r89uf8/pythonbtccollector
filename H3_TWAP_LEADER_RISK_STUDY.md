# H3 — TWAP leader risk study

> With T seconds remaining, when the current TWAP leader leads the Price to Beat by X basis points and fresh spot is Y basis points ahead of or behind that TWAP, how often does the leader lose officially?

Produce **eight frequency grids** from all available qualifying history below. This is descriptive research: no fitted model, threshold search or train/test split. The optional [1% validation plan](H3_TWAP_LEADER_RISK_VALIDATION.md) is separate.

## Cohort and definitions

Use available BTC five-minute markets starting at or after **2026-08-16 00:00:00 UTC**. Require the validated 60-second settlement identity and retain pending outcomes in the accounting. Report missing startup/calendar slots and unavailable observations; do not fabricate them.

Record an exclusive **cohort end**, in UTC: include market starts before it and market closes at or before it. Both requested boundaries must align to five minutes. The extraction uses the requested range without a September 10 clamp.

The September 10 update `ace8d19` added evidence for trading studies, including API PTB observations, fees and detailed CLOB quotes. It did not change this study's spot/TWAP writers or official-resolution parser; the TWAP connection continued through the probability-service restart. The [recovered readiness report](H3_DATA_READINESS_2026-09-10.md) records that deployment and its broader historical scope. **September 10, 02:30 UTC is a comparison marker, not an eligibility boundary.**

Checkpoints: **T = 120, 90, 60, 30, 15, 10, 5 and 3 seconds**. Each market contributes once per checkpoint, at `cut = market_end_ms - T * 1000`.

| Input | Definition |
| --- | --- |
| K | Stream-derived Price to Beat (PTB): exact 60-second TWAP event stamped at market start and received by the checkpoint |
| W | Latest received 60-second settlement-reference TWAP by the checkpoint |
| S | Latest retained Chainlink BTC/USD spot observation received by the checkpoint |
| Leader | Up if W > K; Down if W < K; skip and count W = K ties |

Keep stream-derived K so the reference is recoverable from this collector's pre-checkpoint records. Count missing, late or conflicting opening events as unavailable; there is no fallback to a later reconciled price. Report that coverage cost. The fixed opening reference has no freshness limit.

Chainlink BTC/USD keeps spot and TWAP aligned by provider and USD denomination, avoiding an added Binance BTC/USDT basis. Public spot updates are not a specification of the TWAP's internal sampling. [Source documentation](https://docs.polymarket.com/market-data/chainlink-twap).

For W and S, require **both receipt age and provider-source age in [0, 3,000] ms**. **Exactly 3,000 ms passes.** Select by receipt time before testing source freshness. Compare nanosecond receipts against `cut * 1,000,000`. Spot history retains one-second samples with same-second upserts, so it cannot reproduce every live update.

Let d = +1 for Up and -1 for Down:

- **X = 10,000 × d × (W - K) / K**: TWAP lead in bps.
- **Y = 10,000 × d × (S - W) / K**: spot gap in bps.

Negative Y means spot trails TWAP in the leader's direction; it need not have crossed K. **X + Y is spot's leader-relative distance from K.** Keep prices, E18 conversion, arithmetic and bin comparisons in Decimal.

## Grids and scoring

| Axis | Fixed bins, in bps |
| --- | --- |
| X | [0, 1), [1, 2), [2, 4), [4, 8), [8, infinity) |
| Y | Y < -2; -2 <= Y < 0; 0 <= Y < 2; Y >= 2 |

Label Y as spot **trailing** or **level with/ahead of TWAP**, in the leader's direction. Keep bins fixed.

Score the recorded leader against the **official resolved Up/Down winner**. Each cell reports N eligible non-tied markets, L known official losses, U outcomes without a verified Up/Down winner, **L / (N - U)**, and a pointwise **95% Wilson interval**. Show the resolved denominator. Split/unverified outcomes belong in U with reason counts. Empty resolved samples have unavailable rates and intervals.

Report unique unavailable totals separately from overlapping reason counts. Include UTC market-start-day and Up/Down breakdowns. Keep checkpoints separate. Intervals assume independent, comparable markets and do not simultaneously certify all cells; zero observed losses does not establish zero risk. [Interval method](https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm).

Show **one supporting pre/post comparison** of the same cells, split by market start at **2026-09-10 02:30 UTC**. The eight primary grids continue to use the full cohort. Overlapping intervals do not establish equivalence, and this descriptive comparison does not establish a deployment effect.

Decreasing loss with time, X or spot confirmation is a pattern to examine, not a required result. A late observed loss rate is not a universal noise floor.

## Report and provenance

Use the [official consolidated results and findings](H3_TWAP_LEADER_RISK_FINAL_REPORT.md) for the completed study and subsequent quote-return/exclusion audit. It preserves this design's outcome snapshot and distinguishes descriptive results from unrun execution and prospective tests.

The full-history run uses **2026-08-16 00:00 UTC** through the exclusive end **2026-09-12 21:15 UTC**: **8,014 markets / 64,112 checkpoint rows**, including **59,885 eligible checkpoint observations**. Official outcomes were read at **2026-09-12 22:17:21.776 UTC**. Seventeen startup calendar slots lack market metadata. The [report](results/leader_risk/2026-09-12-full-history/report.md) and accompanying manifest preserve the grids, coverage, comparison and hashes.

The [earlier post-update verification](results/leader_risk/2026-09-12-post-update-check/verification.json) and its [manifest](results/leader_risk/2026-09-12-post-update-check/manifest.json) remain dated checks of a smaller cohort. They are not the main study answer. Use the full-history report for rates and cell counts; preserve small samples and their uncertainty.

## Implementation

Use [extract.sql](research/leader_risk/extract.sql), then [tabulate.py](research/leader_risk/tabulate.py). Follow the [run instructions and tabulation contract](research/leader_risk/README.md): successful extraction exit, checked row counts, adjustable timeout, Decimal Wilson calculation and reconciled breakdowns. Raw extracts are ignored by Git; summaries and manifests are retained.

Keep research independent of production and retired work under [AGENTS.md](AGENTS.md) and [RESEARCH_QUESTIONS.md](RESEARCH_QUESTIONS.md). Trading prices, fees, execution, depth, futures and profitability are outside this settlement-risk study.

The [market-price companion](results/leader_risk/2026-09-13-market-prices/report.md) now adds retained leader-token bid/ask prices beside these results. It reports risk again on the quote-matched observations and within ask-price bands, with quote coverage and missingness. The original settlement findings and outcome snapshot remain the reference. This descriptive addition does not model fees or executions.
