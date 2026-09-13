# H3 TWAP leader-risk study — official results and findings

**Status:** completed descriptive historical study, with a completed market-price comparison and a gross-return/exclusion audit. **No prospective risk validation, execution test, or net-profitability test has been completed.**

**Report date:** 2026-09-13 UTC. This is the canonical consolidated report for the work described below. It incorporates the original settlement results, the price addition, and the independently checked review. Earlier dated reports remain preserved as provenance. “Official” here identifies the project's consolidated record; only the recorded market resolutions are official Polymarket outcomes.

## Findings that the completed work supports

1. **Settlement loss rates vary substantially with time remaining, TWAP lead size, and spot alignment.** Large leads with confirming spot rarely lost late in this historical sample. Small leads with opposing spot remained vulnerable, including in the final seconds.
2. **A low settlement-loss rate does not establish an attractive buying price.** In the 30-second region with a lead of at least 2 bp and confirming spot, 2,946 of 2,961 qualifying asks were exactly $1. Only three were below $0.98. The cheaper subset must be assessed on its own observations.
3. **The gross payout-minus-ask results contain both positive and negative historical differences.** Some broad price bands are close to zero, but the claim that every band is within approximately one cent of zero is false. Neither a selected positive cell nor a count of mostly nonpositive cells establishes future profitability or universal market efficiency.
4. **Quote eligibility selects a different risk population.** Among settlement-input-eligible checkpoints, those excluded by the quote rule lost more often overall than those retained. Most of the cited stale-bid cases also had a stale ask. This is an association with quote availability, not a demonstrated isolated bid-staleness effect.
5. **A profitable leader-buying strategy has not been established.** The work does not prove that all mispricing is absent, that X/Y/T contain no information beyond prices, or that speed is the only possible source of an advantage. Those conclusions require tests that were not performed.

## Study scope, data and fixed definitions

| Item | Definition or frozen value |
|---|---|
| Market | Polymarket BTC five-minute Up/Down, validated 60-second Chainlink TWAP settlement identity |
| Requested market-start range | 2026-08-16 00:00:00 UTC through 2026-09-12 21:15:00 UTC, end exclusive |
| Stored markets / checkpoint rows | 8,014 / 64,112 |
| Eligible checkpoint observations | 59,885; another 4,227 lack required settlement inputs; zero available-input ties |
| Missing calendar slots | 17 startup slots before the first stored market at 2026-08-16 01:25 UTC |
| Official-outcome snapshot | 2026-09-12 22:17:21.776 UTC; no later outcomes were substituted for this audit |
| Quote-history snapshot | 2026-09-13 00:40:38.993 UTC; separate from the outcome snapshot |
| Checkpoints T | 120, 90, 60, 30, 15, 10, 5 and 3 seconds remaining |
| Checkpoint cut | Market end minus T seconds; one observation per market at each checkpoint |
| K | Exact 60-second TWAP event stamped at market start and received by the checkpoint; stream-derived Price to Beat |
| W | Latest accepted settlement-reference TWAP received by the checkpoint |
| S | Latest retained Chainlink BTC/USD spot observation available by the checkpoint |
| Leader | Up when W > K; Down when W < K; W = K excluded and counted |
| Outcome | Verified official Up/Down winner with the required settlement-rule identity; missing/unverified outcomes remain unknown |
| W/S freshness | Provider-source age and local receipt age each in [0, 3,000] ms; the boundary is inclusive |
| Arithmetic | Decimal, precision 60; no financial values or intermediate financial arithmetic converted to float |

With d = +1 for Up and -1 for Down:

- X = 10,000 × d × (W − K) / K: the TWAP leader's distance from the Price to Beat, in basis points.
- Y = 10,000 × d × (S − W) / K: spot's gap from TWAP in the leader's direction, in basis points.
- X + Y is spot's leader-relative distance from the Price to Beat. Negative Y means spot trails TWAP; it does not necessarily mean spot has crossed the Price to Beat.

The fixed X bins are [0,1), [1,2), [2,4), [4,8), [8,infinity). The fixed Y bins are below -2, [-2,0), [0,2), and at least 2 bp. Exact TWAP ties are excluded from the first X bin. “Confirming spot” in the region summaries means Y >= 0. Results pool Up and Down unless stated otherwise.

N counts eligible observations, U counts observations without a verified outcome, L counts known leader losses, and n = N − U. Reported loss rates use L/n, with unavailable rates when n = 0. U is never treated as a win. The same market can appear at eight checkpoints; checkpoint results are not pooled into independent trades or markets.

K is an observed stream-derived reference, not a backfilled decision-time website quote. A missing, late, or conflicting opening event excludes the checkpoint. W and S are selected causally before their age checks. These definitions are preserved from the [study design](H3_TWAP_LEADER_RISK_STUDY.md). The [original full-history manifest](results/leader_risk/2026-09-12-full-history/manifest.json) records the inputs and exact hashes.

## Settlement-risk results

| T (seconds) | Unavailable inputs | N eligible | Losses / resolved | Loss rate | U |
|---|---|---|---|---|---|
| 120 | 516 | 7498 | 1568 / 7471 | 20.99% | 27 |
| 90 | 541 | 7473 | 1266 / 7447 | 17.00% | 26 |
| 60 | 527 | 7487 | 904 / 7460 | 12.12% | 27 |
| 30 | 543 | 7471 | 478 / 7444 | 6.42% | 27 |
| 15 | 525 | 7489 | 282 / 7462 | 3.78% | 27 |
| 10 | 537 | 7477 | 194 / 7451 | 2.60% | 26 |
| 5 | 521 | 7493 | 110 / 7466 | 1.47% | 27 |
| 3 | 517 | 7497 | 84 / 7471 | 1.12% | 26 |

The overall loss rate decreased from 20.99% at 120 seconds to 1.12% at three seconds. These averages combine very different X/Y conditions and can change composition with checkpoint eligibility.

At 60 seconds with a TWAP lead of 2 to less than 4 bp, spot position separated substantially different risks:

| Spot gap in leader's direction | Losses / resolved | Loss rate | U |
|---|---:|---:|---:|
| Below -2 bp | 108 / 170 | 63.53% | 1 |
| -2 to less than 0 bp | 50 / 466 | 10.73% | 1 |
| 0 to less than 2 bp | 29 / 677 | 4.28% | 0 |
| At least 2 bp | 2 / 214 | 0.93% | 3 |

With confirming spot, the broad region summaries are:

| T (seconds) | X >= 2 bp | X >= 4 bp | X >= 8 bp |
|---|---|---|---|
| 120 | 209/2961 = 7.06%; U=11 | 93/2064 = 4.51%; U=11 | 25/1050 = 2.38%; U=7 |
| 90 | 110/3042 = 3.62%; U=8 | 40/2172 = 1.84%; U=8 | 13/1135 = 1.15%; U=7 |
| 60 | 40/3142 = 1.27%; U=11 | 9/2251 = 0.40%; U=8 | 0/1184 = 0.00%; U=5 |
| 30 | 0/3119 = 0.00%; U=13 | 0/2283 = 0.00%; U=11 | 0/1288 = 0.00%; U=9 |

These nested thresholds are descriptive sums of the fixed cells, examined after the results were available. They overlap, were not selected through a prospective protocol, and are not separate independent samples. No future loss guarantee follows from them.

At three seconds, 81 of 84 known losses had X < 1 bp, all 84 had X < 2 bp, and spot trailed TWAP in 79 of those 84 losses. The X < 1 and Y < -2 cell lost 32/74 times (43.24%, U=0). A short remaining time alone does not make a tiny lead reliable.

The example 30-second cell X=[2,4), Y=[0,2) had 0/658 losses, U=0. Its pointwise 95% Wilson interval is approximately 0%–0.58%; this is not zero future risk or a simultaneous guarantee over all 160 cells. All eight original grids are included in Appendix A.

## Direction and date comparison

| T (seconds) | Up leader | Down leader | Before date marker | At/after date marker |
|---|---|---|---|---|
| 120 | 779/3711 = 20.99%; U=10 | 789/3760 = 20.98%; U=17 | 1401/6706 = 20.89%; U=27 | 167/765 = 21.83%; U=0 |
| 90 | 618/3674 = 16.82%; U=10 | 648/3773 = 17.17%; U=16 | 1125/6682 = 16.84%; U=26 | 141/765 = 18.43%; U=0 |
| 60 | 446/3706 = 12.03%; U=11 | 458/3754 = 12.20%; U=16 | 815/6693 = 12.18%; U=27 | 89/767 = 11.60%; U=0 |
| 30 | 238/3715 = 6.41%; U=11 | 240/3729 = 6.44%; U=16 | 433/6677 = 6.48%; U=27 | 45/767 = 5.87%; U=0 |
| 15 | 146/3724 = 3.92%; U=11 | 136/3738 = 3.64%; U=16 | 256/6694 = 3.82%; U=27 | 26/768 = 3.39%; U=0 |
| 10 | 95/3711 = 2.56%; U=10 | 99/3740 = 2.65%; U=16 | 176/6680 = 2.63%; U=26 | 18/771 = 2.33%; U=0 |
| 5 | 54/3718 = 1.45%; U=11 | 56/3748 = 1.49%; U=16 | 99/6700 = 1.48%; U=27 | 11/766 = 1.44%; U=0 |
| 3 | 41/3717 = 1.10%; U=11 | 43/3754 = 1.15%; U=15 | 76/6705 = 1.13%; U=26 | 8/766 = 1.04%; U=0 |

The date marker is 2026-09-10 02:30 UTC, the first full market after the compact-evidence deployment. It is a descriptive comparison marker, not an eligibility boundary or a held-out evaluation split. Earlier qualifying history remains included. Similar aggregate rates do not establish equivalence across directions, dates, or individual cells, and the comparison does not estimate a deployment effect. Full daily, direction, and date-by-cell tables remain linked in the evidence inventory.

## Market-price results and quote availability

For each frozen market/checkpoint, the quote extraction selects the latest retained one-second CLOB sample in the preceding ten seconds whose sample timestamp and row receipt are no later than the checkpoint. Freshness and token checks follow selection; an unusable newer row cannot be replaced with an older fresh row.

A primary matched quote requires correct Up/Down token identity, valid leader bid and ask in [0,1], bid <= ask, and ages in [0,3,000] ms for the selected sample, its row receipt, and each leader component's source and local receipt clock. A recent opposite-outcome update does not refresh the leader's components. The three-second cap is an observation rule, not an assumed fixed two-to-three-second execution delay.

There are **53,361 matched checkpoint observations out of 59,885 settlement-eligible observations (89.11%)**. Independent fresh-ask availability is 53,362 and fresh-bid availability is 53,361. The original full-population settlement grids are unchanged.

| T (seconds) | Full N | Matched N | Matched losses / resolved | Matched loss rate | U | Median bid | Median ask | Exact-$1 asks |
|---|---|---|---|---|---|---|---|---|
| 120 | 7498 | 6068 | 1142 / 6043 | 18.90% | 25 | $0.88 | $0.89 | 516 |
| 90 | 7473 | 6098 | 921 / 6073 | 15.17% | 25 | $0.94 | $0.95 | 1345 |
| 60 | 7487 | 6275 | 633 / 6250 | 10.13% | 25 | $0.98 | $0.99 | 2884 |
| 30 | 7471 | 6785 | 359 / 6758 | 5.31% | 27 | $0.99 | $1.00 | 5553 |
| 15 | 7489 | 6960 | 214 / 6933 | 3.09% | 27 | $0.99 | $1.00 | 6418 |
| 10 | 7477 | 7005 | 142 / 6980 | 2.03% | 25 | $0.99 | $1.00 | 6707 |
| 5 | 7493 | 7071 | 81 / 7045 | 1.15% | 26 | $0.99 | $1.00 | 6940 |
| 3 | 7497 | 7099 | 61 / 7074 | 0.86% | 25 | $0.99 | $1.00 | 7001 |

Price medians in this table use all matched observations, including those with unknown outcomes; loss rates use resolved matched observations. In the gross-return audit below, price means instead use the exact resolved sample paired with the payouts. The distinction is intentional.

The economically relevant price-selection comparison is particularly clear in the region X >= 2 bp, Y >= 0:

| T | Quote subset | N | Losses / resolved | Loss rate | U |
|---:|---|---:|---:|---:|---:|
| 60 s | All matched | 2,827 | 34 / 2,816 | 1.21% | 11 |
| 60 s | Ask below $0.98 | 266 | 24 / 265 | 9.06% | 1 |
| 30 s | All matched | 2,961 | 0 / 2,948 | 0.00% | 13 |
| 30 s | Ask below $0.98 | 3 | 0 / 3 | 0.00% | 0 |

At 30 seconds, 2,946/2,961 asks in this region are exactly $1 (99.49%); only 15 are below $1. The three asks below $0.98 provide too little evidence for a low-risk cheap-entry rule. Applying the full region's loss rate to a cheaper subset is misleading.

Exact $0 and $1 asks are retained boundary observations, not demonstrated orderable prices or fills. The one-second probability writer can skip missing-ask states, so a subsequent withdrawal can be absent while an older retained quote still passes the age limit. No order size, quantity, delay, or fill has been measured by these tables. Full cell prices and matched risks appear in [market_price_cells.csv](results/leader_risk/2026-09-13-market-prices/market_price_cells.csv); six disjoint ask bands for every cell appear in [price_bands.csv](results/leader_risk/2026-09-13-market-prices/price_bands.csv).

## Gross payout-minus-ask audit

For each resolved matched observation i, define a hypothetical one-share benchmark:

g_i = 1{the recorded leader won officially} − recorded leader ask_i.

Its sample mean is exactly 1 − resolved-sample mean ask − L/n, subject only to Decimal division rounding. This is **historical gross payout minus the recorded ask per hypothetical one-share purchase**, in dollars per share. It is not a future expected value, an observed trading profit, a return on one dollar invested, or a fee-adjusted result. Unknown outcomes are excluded from both payout and price means and retained as U. Boundary asks remain separate. No trade is claimed to have executed.

Pooling all X/Y cells and ask prices within each checkpoint, gross averages are close to zero, from -0.5977 to +0.0277 cents per hypothetical share. This includes boundary asks and is not an executable all-market strategy. It supports a narrow observation about these pooled historical means; it does not establish that every band or condition is correctly priced.

| T (seconds) | n resolved | U | Resolved mean ask | Mean gross cents/share |
|---:|---:|---:|---:|---:|
| 120 | 6043 | 25 | $0.813961 | -0.2940 |
| 90 | 6073 | 25 | $0.854322 | -0.5977 |
| 60 | 6250 | 25 | $0.899101 | -0.0381 |
| 30 | 6758 | 27 | $0.948875 | -0.1997 |
| 15 | 6933 | 27 | $0.969673 | -0.0539 |
| 10 | 6980 | 25 | $0.980159 | -0.0503 |
| 5 | 7045 | 26 | $0.988343 | +0.0160 |
| 3 | 7074 | 25 | $0.991100 | +0.0277 |

The five examples in the external review reproduce as follows. These bands pool all eligible X/Y cells at each T; they are broader populations than the confirming-spot region in the previous table.

| T (seconds) | Ask band | n resolved | U | Resolved mean ask | Losses / resolved | Loss rate | Gross cents/share |
|---|---|---|---|---|---|---|---|
| 60 | (0,0.90) | 1345 | 5 | $0.580135 | 558 / 1345 | 41.49% | +0.4995 |
| 60 | [0.95,0.98) | 516 | 1 | $0.962721 | 19 / 516 | 3.68% | +0.0457 |
| 60 | [0.98,1) | 1101 | 8 | $0.987483 | 21 / 1101 | 1.91% | -0.6557 |
| 30 | [0.95,0.98) | 153 | 2 | $0.963791 | 6 / 153 | 3.92% | -0.3007 |
| 30 | ask=1 | 5531 | 22 | $1.000000 | 3 / 5531 | 0.05% | -0.0542 |

The review's universal “within approximately one cent of zero” claim is false. Counterexamples with more than 100 resolved observations include:

| T | Ask band | n | Mean gross dollars/share | Mean gross cents/share |
|---:|---|---:|---:|---:|
| 90 s | [0.90,0.95) | 705 | -0.015901 | -1.5901 |
| 30 s | (0,0.90) | 532 | -0.023664 | -2.3664 |
| 10 s | (0,0.90) | 166 | -0.021241 | -2.1241 |

There are larger positive and negative differences in smaller samples, including +9 cents with n=1. Small samples are not proof of a repeatable opportunity. Appendix B includes **every checkpoint/ask band**, so the interpretation does not depend on selecting near-zero examples.

The screen over cell-by-band groups reproduces **133 groups with n >= 100 resolved observations, 16 positive gross means, and one positive fixed-mean-ask Wilson diagnostic**. Using N >= 100 instead gives 134 groups and 17 positive means; the number with a positive diagnostic remains one. The “100 observations” threshold must therefore specify whether unknown outcomes are included.

The lone positive diagnostic occurs at T=60 s, X=[0,1) bp, Y=[0,2) bp, ask in (0,0.90): N=197, n=196, U=1, L=38; resolved mean ask $0.728316, gross mean **+$0.077806/share (+7.7806 cents)**. The transformed diagnostic is +$0.016795/share. This is a selected historical cell, not a confirmed trade or recommended entry rule.

The diagnostic is 1 − sample mean ask − upper pointwise Wilson loss bound. Wilson estimates a binomial proportion; subtracting its bound from a sampled price mean does not by itself validate a conservative confidence bound for future paired returns. A return analysis must account for the sampling of prices and outcomes, their relationship, temporal dependence, and selection. Inspecting many cells also changes the interpretation of isolated positive bounds. Merely observing one positive screen neither proves an advantage nor proves that the result is “what chance produces.” [NIST's Wilson interval guidance](https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm) and [multiple-comparison guidance](https://www.itl.nist.gov/div898/handbook/prc/section4/prc47.htm) describe the relevant limits of pointwise inference; the application to these paired returns is the audit's methodological assessment.

Fees and execution costs would reduce otherwise identical gross taker results, but their effect has not been calculated here. Applicable historical fee parameters and trading rules must be used in any later net-return test; today's schedule cannot be assigned to every historical observation. Polymarket documents fees as market-dependent at matching. [Official fee documentation](https://docs.polymarket.com/trading/fees). This audit adds no fee model, maker strategy, or simulated fills.

## Excluded-checkpoint results

The following comparison is within the **59,885 settlement-input-eligible checkpoint observations**. “Excluded” means the complement of the quote-matched subset; it does not include checkpoints that already failed settlement-input eligibility.

| T (seconds) | Full eligible losses / resolved; rate; U | Quote-matched losses / resolved; rate; U | Quote-excluded losses / resolved; rate; U |
|---|---|---|---|
| 120 | 1568/7471 = 20.99%; U=27 | 1142/6043 = 18.90%; U=25 | 426/1428 = 29.83%; U=2 |
| 90 | 1266/7447 = 17.00%; U=26 | 921/6073 = 15.17%; U=25 | 345/1374 = 25.11%; U=1 |
| 60 | 904/7460 = 12.12%; U=27 | 633/6250 = 10.13%; U=25 | 271/1210 = 22.40%; U=2 |
| 30 | 478/7444 = 6.42%; U=27 | 359/6758 = 5.31%; U=27 | 119/686 = 17.35%; U=0 |
| 15 | 282/7462 = 3.78%; U=27 | 214/6933 = 3.09%; U=27 | 68/529 = 12.85%; U=0 |
| 10 | 194/7451 = 2.60%; U=26 | 142/6980 = 2.03%; U=25 | 52/471 = 11.04%; U=1 |
| 5 | 110/7466 = 1.47%; U=27 | 81/7045 = 1.15%; U=26 | 29/421 = 6.89%; U=1 |
| 3 | 84/7471 = 1.12%; U=26 | 61/7074 = 0.86%; U=25 | 23/397 = 5.79%; U=1 |

At 60 seconds, the excluded rate is 271/1,210 = 22.3967%; at 30 seconds it is 119/686 = 17.3469% (17.35% to two decimals, or 17.3% to one decimal). The direction of the review's exclusion finding is correct. Matched rates should not be used as the full population's rates. The earlier companion already kept full_* and matched_* results separate, but this audit makes the risk difference explicit.

The suggested isolated stale-bid explanation needs correction:

| T | Fresh common row, both leader source clocks over 3 seconds old: N | Losses / resolved | U | Loss rate |
|---:|---:|---:|---:|---:|
| 60 s | 885 | 226 / 883 | 2 | 25.59% |
| 30 s | 341 | 88 / 341 | 0 | 25.81% |

At both checkpoints these observations also have a stale leader ask. There are **zero fresh-ask/unusable-bid observations at 60 and 30 seconds**, and only **one in the entire eligible dataset**, at 10 seconds; it lost. The marginal source-age exclusion buckets overlap (at 60 seconds there are 1,111 ask-age failures and 1,111 bid-age failures; at 30 seconds, 423 of each). They must not be added or interpreted as independent causal effects. The reproducible audit records both overlapping reasons and disjoint quote statuses.

Quote staleness and loss are associated in this sample. This does not establish that bid staleness independently predicts loss after time, lead, spot, and price are controlled. Nor does failure of this data-quality rule prove that no executable offer existed: it means no qualifying retained quote is available under this protocol.

## Resolution of the external review's claims

| Claim | Audit result |
|---|---|
| Original 30-second/60-second matched-region counts | Verified exactly. |
| Five displayed gross-return examples | Verified after labeling them resolved-sample hypothetical gross payout-minus-ask means, not future EV. |
| Every checkpoint/ask band is within about one cent of zero | False; counterexamples and all bands are reported above and in Appendix B. |
| 133 groups, 16 positive means, one positive Wilson screen | Reproduces for n >= 100; it is a descriptive screen, not an adjusted test of market efficiency. |
| One positive screen is necessarily chance | Not established; neither a calibrated null comparison nor an adequate multiple-comparison/dependence procedure was run. |
| Excluded checkpoints have higher loss rates | Verified overall at the stated checkpoints; the 30-second rate is 17.3469%. The two populations were already separately labeled. |
| The dominant effect is an old bid while the rest is fresh | Misleading as a bid-only claim: the leader ask is also stale in the cited large buckets. |
| X/Y/T are correctly priced and buying the leader has no edge | Not established. The audit has not tested incremental information beyond prices, net executable returns, or performance on new markets. |
| Inputs are uniformly two to three seconds old | Not the protocol: source and receipt ages are allowed from zero through 3,000 ms. |
| Binance arrives in 35 ms versus 1,700 ms for Chainlink | Not reproduced by the frozen study artifacts or readiness report. Binance is not an input to this study's S. A separately measured source-to-receipt lag would still not establish same-move information arrival or trading advantage. |
| Only speed can provide an advantage | Not established by any completed test. |
| The existing validation plan has nothing left to validate | Incorrect as a consequence of these results. A future risk-validation plan still addresses an unresolved risk question; it does not independently establish profitability. |

The [research question Q4](RESEARCH_QUESTIONS.md) concerns propagation of futures moves to spot/TWAP and observed updates, separating price response, oracle timing, and delivery. Testing Polymarket quote response and executable profitability would add scope. The compact 100 ms quote evidence was deployed on September 10 and supports finer sampled comparisons going forward; it does not reconstruct earlier quote withdrawals, measure fills, or by itself establish an information lead. See the preserved [data-readiness report](H3_DATA_READINESS_2026-09-10.md).

## What is complete and what remains untested

| Work | Status |
|---|---|
| Frozen historical extraction, causal input checks, outcome accounting | Complete for the declared cohort and snapshots |
| Eight fixed settlement grids, direction/day/date summaries | Complete; descriptive |
| Matched leader bid/ask prices, six ask bands, coverage accounting | Complete; sampled quote evidence |
| Resolved-only gross payout-minus-ask and excluded-population audit | Complete; hypothetical one-share benchmark |
| Predictive performance on genuinely new markets | Not tested |
| A maximum future losing-risk guarantee | Not established |
| Net profitability after applicable fees and execution | Not tested |
| Actual fills, capacity, slippage, and order-to-fill timing | Not measured; depth/quantities remain outside the owner's collection scope |
| A first-qualification trading policy within a time window | Not tested; fixed-checkpoint results cannot be substituted for it |
| No incremental value of X/Y/T beyond market quotes | Not tested |
| Universal absence of mispricing or proof that only speed matters | Not established |
| Q2 flip paths, broader Q3 factors, Q4 response timing, Q5 transient-impact questions | Not answered by this study |

The [optional forward risk-validation plan](H3_TWAP_LEADER_RISK_VALIDATION.md) remains inactive and unrun. It has not been deleted, silently executed, or converted into a profitability claim. The completed descriptive checkpoint can be closed with this report; further research requires a separately declared question and protocol. No trading or production changes were made for this audit.

## Reproduction and verification

- Full repository test suite: **881 passed**, using `.venv/Scripts/python.exe -m pytest -q`.
- New focused quote-return audit suite: **41 passed**; includes resolved-only price/payout matching, Decimal precision, unknown outcomes, empty/boundary groups, independent clocks, exclusion overlaps, provenance and overwrite protection.
- An independent calculation that did not call `assess_quote` or `price_band` reproduced every gross sum/mean and count in all 8 checkpoint totals, 48 checkpoint bands, and 960 cell bands. It also reproduced full/matched/excluded counts, all disjoint status counts, and the key two-side-stale versus bid-only diagnostics.
- The audit's 133/16/1 and 134/17/1 screen counts were separately reproduced. Original and price-companion artifact hashes remain unchanged, and the audit inputs/helpers/outputs match its manifest.
- These are software and accounting checks. They do not constitute financial, execution, or prospective validation.

Reproduce the audit from the repository root, using a new output directory:

```powershell
python -m research.leader_risk.quote_return_audit `
    --observations results/leader_risk/2026-09-12-full-history/observations.csv `
    --quotes results/leader_risk/2026-09-13-market-prices/observations.csv `
    --price-manifest results/leader_risk/2026-09-13-market-prices/manifest.json `
    --output results/leader_risk/example-return-audit
```

The raw CSVs are local frozen inputs and are ignored by Git. Preserve them with their manifests when reproducing this exact outcome snapshot; a new database query cannot recreate a prior outcome state merely by using the same market dates.

The audit reuses the frozen observations and quoted history; it does not query newer official outcomes or silently overwrite previous artifacts. Decimal sums and means are retained at calculation precision in CSV. The rounded report tables are presentation only. The original full-history grids and all manifest-listed original artifacts were checked for unchanged hashes.

Evidence inventory:

- [Original study design](H3_TWAP_LEADER_RISK_STUDY.md), [original report](results/leader_risk/2026-09-12-full-history/report.md), [original manifest](results/leader_risk/2026-09-12-full-history/manifest.json), and [verification](results/leader_risk/2026-09-12-full-history/verification.json).
- [Original grids](results/leader_risk/2026-09-12-full-history/grids.csv), [coverage](results/leader_risk/2026-09-12-full-history/coverage.csv), [outcome accounting](results/leader_risk/2026-09-12-full-history/outcome_accounting.csv), [daily totals](results/leader_risk/2026-09-12-full-history/daily_totals.csv), [daily cells](results/leader_risk/2026-09-12-full-history/daily_cells.csv), [direction totals](results/leader_risk/2026-09-12-full-history/direction_totals.csv), [direction cells](results/leader_risk/2026-09-12-full-history/direction_cells.csv), [date totals](results/leader_risk/2026-09-12-full-history/comparison_totals.csv), and [date cells](results/leader_risk/2026-09-12-full-history/comparison_cells.csv).
- [Market-price report](results/leader_risk/2026-09-13-market-prices/report.md), [market-price manifest](results/leader_risk/2026-09-13-market-prices/manifest.json), [price cells](results/leader_risk/2026-09-13-market-prices/market_price_cells.csv), and [price bands](results/leader_risk/2026-09-13-market-prices/price_bands.csv).
- [Audit implementation](research/leader_risk/quote_return_audit.py), [audit manifest](results/leader_risk/2026-09-13-return-audit/manifest.json), [independent verification](results/leader_risk/2026-09-13-return-audit/verification.json), [checkpoint totals](results/leader_risk/2026-09-13-return-audit/checkpoint_totals.csv), [checkpoint bands](results/leader_risk/2026-09-13-return-audit/checkpoint_bands.csv), [cell bands](results/leader_risk/2026-09-13-return-audit/cell_bands.csv), [full/matched/excluded coverage](results/leader_risk/2026-09-13-return-audit/coverage.csv), [disjoint quote statuses](results/leader_risk/2026-09-13-return-audit/statuses.csv), [overlapping reasons](results/leader_risk/2026-09-13-return-audit/reasons.csv), [screen counts](results/leader_risk/2026-09-13-return-audit/screens.csv), [report provenance](results/leader_risk/2026-09-13-return-audit/final_report_manifest.json).

## Appendix A — all 160 original settlement cells

Each entry is L/n, loss rate, pointwise 95% Wilson interval, and U. N = n + U. These grids preserve the full settlement-input-eligible population. They are not replaced by quote-matched results.

### 120 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 103/135 = 76.30% [68.46%, 82.68%]; U=0 | 248/493 = 50.30% [45.91%, 54.70%]; U=1 | 162/557 = 29.08% [25.47%, 32.99%]; U=1 | 18/116 = 15.52% [10.05%, 23.20%]; U=0 |
| [1,2) | 77/118 = 65.25% [56.30%, 73.24%]; U=0 | 137/373 = 36.73% [32.00%, 41.73%]; U=0 | 95/462 = 20.56% [17.13%, 24.48%]; U=2 | 15/124 = 12.10% [7.47%, 19.00%]; U=1 |
| [2,4) | 105/180 = 58.33% [51.03%, 65.29%]; U=2 | 157/524 = 29.96% [26.20%, 34.02%]; U=0 | 93/663 = 14.03% [11.59%, 16.88%]; U=0 | 23/234 = 9.83% [6.64%, 14.32%]; U=0 |
| [4,8) | 104/270 = 38.52% [32.91%, 44.45%]; U=2 | 73/498 = 14.66% [11.82%, 18.04%]; U=0 | 47/672 = 6.99% [5.30%, 9.18%]; U=1 | 21/342 = 6.14% [4.05%, 9.20%]; U=3 |
| [8,infinity) | 51/341 = 14.96% [11.56%, 19.13%]; U=4 | 14/319 = 4.39% [2.63%, 7.23%]; U=3 | 11/464 = 2.37% [1.33%, 4.19%]; U=3 | 14/586 = 2.39% [1.43%, 3.97%]; U=4 |

### 90 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 98/115 = 85.22% [77.60%, 90.56%]; U=0 | 250/480 = 52.08% [47.62%, 56.52%]; U=1 | 124/491 = 25.25% [21.61%, 29.28%]; U=1 | 15/126 = 11.90% [7.35%, 18.72%]; U=2 |
| [1,2) | 90/118 = 76.27% [67.84%, 83.04%]; U=1 | 116/339 = 34.22% [29.37%, 39.42%]; U=1 | 60/447 = 13.42% [10.57%, 16.90%]; U=1 | 6/112 = 5.36% [2.48%, 11.20%]; U=0 |
| [2,4) | 110/191 = 57.59% [50.50%, 64.38%]; U=1 | 96/493 = 19.47% [16.22%, 23.20%]; U=0 | 57/644 = 8.85% [6.89%, 11.30%]; U=0 | 13/226 = 5.75% [3.39%, 9.59%]; U=0 |
| [4,8) | 86/245 = 35.10% [29.40%, 41.27%]; U=1 | 55/513 = 10.72% [8.33%, 13.70%]; U=2 | 20/675 = 2.96% [1.93%, 4.53%]; U=0 | 7/362 = 1.93% [0.94%, 3.94%]; U=1 |
| [8,infinity) | 47/364 = 12.91% [9.85%, 16.75%]; U=4 | 3/371 = 0.81% [0.28%, 2.35%]; U=3 | 9/502 = 1.79% [0.95%, 3.37%]; U=4 | 4/633 = 0.63% [0.25%, 1.61%]; U=3 |

### 60 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 106/119 = 89.08% [82.20%, 93.50%]; U=0 | 219/451 = 48.56% [43.98%, 53.16%]; U=0 | 57/457 = 12.47% [9.75%, 15.82%]; U=2 | 6/91 = 6.59% [3.06%, 13.65%]; U=0 |
| [1,2) | 80/99 = 80.81% [71.96%, 87.35%]; U=1 | 97/346 = 28.03% [23.56%, 32.99%]; U=1 | 23/397 = 5.79% [3.89%, 8.54%]; U=0 | 5/91 = 5.49% [2.37%, 12.22%]; U=0 |
| [2,4) | 108/170 = 63.53% [56.07%, 70.39%]; U=1 | 50/466 = 10.73% [8.23%, 13.87%]; U=1 | 29/677 = 4.28% [3.00%, 6.08%]; U=0 | 2/214 = 0.93% [0.26%, 3.34%]; U=3 |
| [4,8) | 80/247 = 32.39% [26.86%, 38.46%]; U=0 | 10/509 = 1.96% [1.07%, 3.58%]; U=2 | 7/714 = 0.98% [0.48%, 2.01%]; U=1 | 2/353 = 0.57% [0.16%, 2.04%]; U=2 |
| [8,infinity) | 21/437 = 4.81% [3.16%, 7.23%]; U=1 | 2/438 = 0.46% [0.13%, 1.65%]; U=7 | 0/579 = 0.00% [0.00%, 0.66%]; U=2 | 0/605 = 0.00% [0.00%, 0.63%]; U=3 |

### 30 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 78/82 = 95.12% [88.12%, 98.09%]; U=0 | 170/417 = 40.77% [36.16%, 45.55%]; U=0 | 19/456 = 4.17% [2.68%, 6.42%]; U=2 | 0/90 = 0.00% [0.00%, 4.09%]; U=1 |
| [1,2) | 77/95 = 81.05% [72.03%, 87.67%]; U=0 | 36/284 = 12.68% [9.30%, 17.05%]; U=1 | 5/378 = 1.32% [0.57%, 3.06%]; U=1 | 1/116 = 0.86% [0.15%, 4.72%]; U=0 |
| [2,4) | 65/141 = 46.10% [38.08%, 54.32%]; U=0 | 8/530 = 1.51% [0.77%, 2.95%]; U=0 | 0/658 = 0.00% [0.00%, 0.58%]; U=0 | 0/178 = 0.00% [0.00%, 2.11%]; U=2 |
| [4,8) | 17/247 = 6.88% [4.34%, 10.74%]; U=3 | 1/563 = 0.18% [0.03%, 1.00%]; U=2 | 0/694 = 0.00% [0.00%, 0.55%]; U=2 | 0/301 = 0.00% [0.00%, 1.26%]; U=0 |
| [8,infinity) | 1/464 = 0.22% [0.04%, 1.21%]; U=1 | 0/462 = 0.00% [0.00%, 0.82%]; U=3 | 0/658 = 0.00% [0.00%, 0.58%]; U=1 | 0/630 = 0.00% [0.00%, 0.61%]; U=8 |

### 15 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 83/91 = 91.21% [83.60%, 95.48%]; U=0 | 132/420 = 31.43% [27.17%, 36.02%]; U=0 | 9/449 = 2.00% [1.06%, 3.77%]; U=0 | 0/75 = 0.00% [0.00%, 4.87%]; U=1 |
| [1,2) | 33/85 = 38.82% [29.16%, 49.45%]; U=0 | 4/303 = 1.32% [0.51%, 3.34%]; U=0 | 1/375 = 0.27% [0.05%, 1.49%]; U=1 | 0/95 = 0.00% [0.00%, 3.89%]; U=1 |
| [2,4) | 17/150 = 11.33% [7.20%, 17.40%]; U=1 | 2/513 = 0.39% [0.11%, 1.41%]; U=0 | 0/608 = 0.00% [0.00%, 0.63%]; U=0 | 0/177 = 0.00% [0.00%, 2.12%]; U=1 |
| [4,8) | 1/212 = 0.47% [0.08%, 2.62%]; U=2 | 0/612 = 0.00% [0.00%, 0.62%]; U=2 | 0/719 = 0.00% [0.00%, 0.53%]; U=1 | 0/300 = 0.00% [0.00%, 1.26%]; U=4 |
| [8,infinity) | 0/440 = 0.00% [0.00%, 0.87%]; U=0 | 0/512 = 0.00% [0.00%, 0.74%]; U=4 | 0/684 = 0.00% [0.00%, 0.56%]; U=5 | 0/642 = 0.00% [0.00%, 0.59%]; U=4 |

### 10 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 79/93 = 84.95% [76.30%, 90.82%]; U=0 | 88/390 = 22.56% [18.69%, 26.97%]; U=0 | 4/446 = 0.90% [0.35%, 2.28%]; U=1 | 1/75 = 1.33% [0.24%, 7.17%]; U=0 |
| [1,2) | 17/76 = 22.37% [14.46%, 32.93%]; U=0 | 2/315 = 0.63% [0.17%, 2.29%]; U=0 | 1/378 = 0.26% [0.05%, 1.48%]; U=0 | 0/83 = 0.00% [0.00%, 4.42%]; U=1 |
| [2,4) | 2/147 = 1.36% [0.37%, 4.82%]; U=2 | 0/509 = 0.00% [0.00%, 0.75%]; U=0 | 0/615 = 0.00% [0.00%, 0.62%]; U=0 | 0/178 = 0.00% [0.00%, 2.11%]; U=0 |
| [4,8) | 0/212 = 0.00% [0.00%, 1.78%]; U=1 | 0/620 = 0.00% [0.00%, 0.62%]; U=2 | 0/728 = 0.00% [0.00%, 0.52%]; U=1 | 0/280 = 0.00% [0.00%, 1.35%]; U=4 |
| [8,infinity) | 0/432 = 0.00% [0.00%, 0.88%]; U=1 | 0/540 = 0.00% [0.00%, 0.71%]; U=3 | 0/685 = 0.00% [0.00%, 0.56%]; U=5 | 0/649 = 0.00% [0.00%, 0.59%]; U=5 |

### 5 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 48/82 = 58.54% [47.73%, 68.58%]; U=0 | 53/412 = 12.86% [9.97%, 16.44%]; U=0 | 6/435 = 1.38% [0.63%, 2.98%]; U=1 | 0/83 = 0.00% [0.00%, 4.42%]; U=0 |
| [1,2) | 2/80 = 2.50% [0.69%, 8.66%]; U=1 | 0/336 = 0.00% [0.00%, 1.13%]; U=1 | 0/358 = 0.00% [0.00%, 1.06%]; U=0 | 0/90 = 0.00% [0.00%, 4.09%]; U=0 |
| [2,4) | 1/142 = 0.70% [0.12%, 3.88%]; U=2 | 0/535 = 0.00% [0.00%, 0.71%]; U=0 | 0/612 = 0.00% [0.00%, 0.62%]; U=0 | 0/154 = 0.00% [0.00%, 2.43%]; U=1 |
| [4,8) | 0/215 = 0.00% [0.00%, 1.76%]; U=0 | 0/610 = 0.00% [0.00%, 0.63%]; U=0 | 0/719 = 0.00% [0.00%, 0.53%]; U=4 | 0/283 = 0.00% [0.00%, 1.34%]; U=3 |
| [8,infinity) | 0/413 = 0.00% [0.00%, 0.92%]; U=2 | 0/565 = 0.00% [0.00%, 0.68%]; U=2 | 0/685 = 0.00% [0.00%, 0.56%]; U=5 | 0/657 = 0.00% [0.00%, 0.58%]; U=5 |

### 3 seconds remaining

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | 32/74 = 43.24% [32.57%, 54.59%]; U=0 | 44/405 = 10.86% [8.19%, 14.27%]; U=0 | 5/444 = 1.13% [0.48%, 2.61%]; U=0 | 0/83 = 0.00% [0.00%, 4.42%]; U=0 |
| [1,2) | 3/92 = 3.26% [1.12%, 9.15%]; U=2 | 0/321 = 0.00% [0.00%, 1.18%]; U=1 | 0/361 = 0.00% [0.00%, 1.05%]; U=0 | 0/83 = 0.00% [0.00%, 4.42%]; U=1 |
| [2,4) | 0/129 = 0.00% [0.00%, 2.89%]; U=0 | 0/530 = 0.00% [0.00%, 0.72%]; U=1 | 0/620 = 0.00% [0.00%, 0.62%]; U=0 | 0/154 = 0.00% [0.00%, 2.43%]; U=1 |
| [4,8) | 0/214 = 0.00% [0.00%, 1.76%]; U=0 | 0/602 = 0.00% [0.00%, 0.63%]; U=1 | 0/745 = 0.00% [0.00%, 0.51%]; U=2 | 0/277 = 0.00% [0.00%, 1.37%]; U=4 |
| [8,infinity) | 0/433 = 0.00% [0.00%, 0.88%]; U=2 | 0/541 = 0.00% [0.00%, 0.71%]; U=3 | 0/728 = 0.00% [0.00%, 0.52%]; U=4 | 0/635 = 0.00% [0.00%, 0.60%]; U=4 |

## Appendix B — all 48 checkpoint/ask-band gross benchmarks

These bands pool X/Y within each checkpoint, retain empty bands, and keep boundary asks separate. Means use resolved observations only; U remains explicit. Dollar prices and cent results are rounded for display. A gross mean is a hypothetical historical payout-minus-ask benchmark, not a predicted return or a fill.

| T (seconds) | Ask band | N | Losses / resolved | U | Resolved mean ask | Loss rate | Gross cents/share |
|---|---|---|---|---|---|---|---|
| 120 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 120 | (0,0.90) | 3114 | 1030 / 3103 | 11 | $0.673845 | 33.19% | -0.5782 |
| 120 | [0.90,0.95) | 931 | 71 / 925 | 6 | $0.922054 | 7.68% | +0.1189 |
| 120 | [0.95,0.98) | 776 | 26 / 773 | 3 | $0.961199 | 3.36% | +0.5166 |
| 120 | [0.98,1) | 731 | 14 / 728 | 3 | $0.986150 | 1.92% | -0.5380 |
| 120 | ask=1 | 516 | 1 / 514 | 2 | $1.000000 | 0.19% | -0.1946 |
| 90 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 90 | (0,0.90) | 2234 | 803 / 2225 | 9 | $0.645622 | 36.09% | -0.6521 |
| 90 | [0.90,0.95) | 711 | 65 / 705 | 6 | $0.923702 | 9.22% | -1.5901 |
| 90 | [0.95,0.98) | 737 | 30 / 734 | 3 | $0.961846 | 4.09% | -0.2718 |
| 90 | [0.98,1) | 1071 | 17 / 1068 | 3 | $0.986501 | 1.59% | -0.2419 |
| 90 | ask=1 | 1345 | 6 / 1341 | 4 | $1.000000 | 0.45% | -0.4474 |
| 60 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 60 | (0,0.90) | 1350 | 558 / 1345 | 5 | $0.580135 | 41.49% | +0.4995 |
| 60 | [0.90,0.95) | 415 | 33 / 415 | 0 | $0.920759 | 7.95% | -0.0277 |
| 60 | [0.95,0.98) | 517 | 19 / 516 | 1 | $0.962721 | 3.68% | +0.0457 |
| 60 | [0.98,1) | 1109 | 21 / 1101 | 8 | $0.987483 | 1.91% | -0.6557 |
| 60 | ask=1 | 2884 | 2 / 2873 | 11 | $1.000000 | 0.07% | -0.0696 |
| 30 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 30 | (0,0.90) | 533 | 340 / 532 | 1 | $0.384566 | 63.91% | -2.3664 |
| 30 | [0.90,0.95) | 88 | 6 / 87 | 1 | $0.922310 | 6.90% | +0.8724 |
| 30 | [0.95,0.98) | 155 | 6 / 153 | 2 | $0.963791 | 3.92% | -0.3007 |
| 30 | [0.98,1) | 456 | 4 / 455 | 1 | $0.987264 | 0.88% | +0.3945 |
| 30 | ask=1 | 5553 | 3 / 5531 | 22 | $1.000000 | 0.05% | -0.0542 |
| 15 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 15 | (0,0.90) | 263 | 205 / 262 | 1 | $0.222794 | 78.24% | -0.5237 |
| 15 | [0.90,0.95) | 36 | 4 / 36 | 0 | $0.925556 | 11.11% | -3.6667 |
| 15 | [0.95,0.98) | 44 | 3 / 44 | 0 | $0.963182 | 6.82% | -3.1364 |
| 15 | [0.98,1) | 199 | 1 / 199 | 0 | $0.988281 | 0.50% | +0.6693 |
| 15 | ask=1 | 6418 | 1 / 6392 | 26 | $1.000000 | 0.02% | -0.0156 |
| 10 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 10 | (0,0.90) | 166 | 139 / 166 | 0 | $0.183892 | 83.73% | -2.1241 |
| 10 | [0.90,0.95) | 13 | 1 / 13 | 0 | $0.923077 | 7.69% | +0.0000 |
| 10 | [0.95,0.98) | 25 | 1 / 25 | 0 | $0.962800 | 4.00% | -0.2800 |
| 10 | [0.98,1) | 94 | 0 / 94 | 0 | $0.988447 | 0.00% | +1.1553 |
| 10 | ask=1 | 6707 | 1 / 6682 | 25 | $1.000000 | 0.01% | -0.0150 |
| 5 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 5 | (0,0.90) | 97 | 81 / 97 | 0 | $0.160309 | 83.51% | +0.4639 |
| 5 | [0.90,0.95) | 2 | 0 / 2 | 0 | $0.925000 | 0.00% | +7.5000 |
| 5 | [0.95,0.98) | 5 | 0 / 5 | 0 | $0.958000 | 0.00% | +4.2000 |
| 5 | [0.98,1) | 27 | 0 / 27 | 0 | $0.988259 | 0.00% | +1.1741 |
| 5 | ask=1 | 6940 | 0 / 6914 | 26 | $1.000000 | 0.00% | +0.0000 |
| 3 | ask=0 | 0 | 0 / 0 | 0 | unavailable | unavailable | unavailable |
| 3 | (0,0.90) | 74 | 60 / 74 | 0 | $0.155000 | 81.08% | +3.4189 |
| 3 | [0.90,0.95) | 1 | 0 / 1 | 0 | $0.910000 | 0.00% | +9.0000 |
| 3 | [0.95,0.98) | 5 | 0 / 5 | 0 | $0.962000 | 0.00% | +3.8000 |
| 3 | [0.98,1) | 18 | 1 / 18 | 0 | $0.991889 | 5.56% | -4.7444 |
| 3 | ask=1 | 7001 | 0 / 6976 | 25 | $1.000000 | 0.00% | +0.0000 |

## Appendix C — all 160 matched cell price/risk summaries

Each cell reports median ask/bid, matched losses/resolved and pointwise loss interval, unknown outcomes, matched/full N, and exact-one ask count. Medians use matched N; loss rates use resolved n. The 960 cell-by-band counts and gross benchmarks remain available as machine-readable evidence linked above.

### 120 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.23; bid $0.22<br>66/89 (74.16%; 95% 64.20%–82.12%); U=0<br>N=89/135; ask=1: 0 | ask $0.51; bid $0.50<br>207/418 (49.52%; 95% 44.75%–54.30%); U=1<br>N=419/494; ask=1: 0 | ask $0.70; bid $0.69<br>135/479 (28.18%; 95% 24.34%–32.37%); U=1<br>N=480/558; ask=1: 0 | ask $0.81; bid $0.79<br>9/69 (13.04%; 95% 7.02%–22.97%); U=0<br>N=69/116; ask=1: 1 |
| [1,2) | ask $0.35; bid $0.34<br>43/73 (58.90%; 95% 47.45%–69.47%); U=0<br>N=73/118; ask=1: 0 | ask $0.65; bid $0.64<br>103/309 (33.33%; 95% 28.31%–38.77%); U=0<br>N=309/373; ask=1: 0 | ask $0.83; bid $0.82<br>71/382 (18.59%; 95% 15.01%–22.79%); U=2<br>N=384/464; ask=1: 0 | ask $0.88; bid $0.87<br>11/90 (12.22%; 95% 6.96%–20.57%); U=1<br>N=91/125; ask=1: 0 |
| [2,4) | ask $0.46; bid $0.45<br>57/95 (60.00%; 95% 49.95%–69.28%); U=1<br>N=96/182; ask=1: 0 | ask $0.78; bid $0.77<br>119/410 (29.02%; 95% 24.84%–33.60%); U=0<br>N=410/524; ask=1: 0 | ask $0.90; bid $0.89<br>77/564 (13.65%; 95% 11.06%–16.73%); U=0<br>N=564/663; ask=1: 2 | ask $0.92; bid $0.91<br>16/165 (9.70%; 95% 6.06%–15.17%); U=0<br>N=165/234; ask=1: 1 |
| [4,8) | ask $0.67; bid $0.66<br>58/157 (36.94%; 95% 29.79%–44.72%); U=2<br>N=159/272; ask=1: 0 | ask $0.89; bid $0.88<br>59/404 (14.60%; 95% 11.49%–18.38%); U=0<br>N=404/498; ask=1: 2 | ask $0.95; bid $0.94<br>39/600 (6.50%; 95% 4.79%–8.76%); U=1<br>N=601/673; ask=1: 21 | ask $0.96; bid $0.95<br>14/263 (5.32%; 95% 3.20%–8.74%); U=3<br>N=266/345; ask=1: 22 |
| [8,infinity) | ask $0.92; bid $0.91<br>29/257 (11.28%; 95% 7.97%–15.74%); U=3<br>N=260/345; ask=1: 16 | ask $0.97; bid $0.96<br>8/272 (2.94%; 95% 1.50%–5.70%); U=3<br>N=275/322; ask=1: 44 | ask $0.99; bid $0.98<br>9/426 (2.11%; 95% 1.12%–3.97%); U=3<br>N=429/467; ask=1: 147 | ask $0.999; bid $0.99<br>12/521 (2.30%; 95% 1.32%–3.98%); U=4<br>N=525/590; ask=1: 260 |

### 90 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.18; bid $0.17<br>61/71 (85.92%; 95% 75.98%–92.17%); U=0<br>N=71/115; ask=1: 0 | ask $0.51; bid $0.50<br>209/409 (51.10%; 95% 46.27%–55.91%); U=1<br>N=410/481; ask=1: 0 | ask $0.76; bid $0.75<br>97/406 (23.89%; 95% 20.00%–28.27%); U=1<br>N=407/492; ask=1: 0 | ask $0.90; bid $0.89<br>8/78 (10.26%; 95% 5.29%–18.95%); U=2<br>N=80/128; ask=1: 2 |
| [1,2) | ask $0.29; bid $0.28<br>54/70 (77.14%; 95% 66.05%–85.41%); U=1<br>N=71/119; ask=1: 0 | ask $0.68; bid $0.67<br>85/261 (32.57%; 95% 27.17%–38.47%); U=1<br>N=262/340; ask=1: 0 | ask $0.90; bid $0.89<br>44/371 (11.86%; 95% 8.95%–15.55%); U=1<br>N=372/448; ask=1: 1 | ask $0.94; bid $0.93<br>3/75 (4.00%; 95% 1.37%–11.11%); U=0<br>N=75/112; ask=1: 7 |
| [2,4) | ask $0.455; bid $0.445<br>74/127 (58.27%; 95% 49.57%–66.48%); U=1<br>N=128/192; ask=1: 0 | ask $0.84; bid $0.83<br>79/385 (20.52%; 95% 16.79%–24.84%); U=0<br>N=385/493; ask=1: 1 | ask $0.95; bid $0.94<br>47/557 (8.44%; 95% 6.40%–11.04%); U=0<br>N=557/644; ask=1: 11 | ask $0.97; bid $0.96<br>11/153 (7.19%; 95% 4.06%–12.41%); U=0<br>N=153/226; ask=1: 15 |
| [4,8) | ask $0.70; bid $0.69<br>47/150 (31.33%; 95% 24.45%–39.14%); U=0<br>N=150/246; ask=1: 0 | ask $0.95; bid $0.94<br>36/396 (9.09%; 95% 6.64%–12.33%); U=2<br>N=398/515; ask=1: 30 | ask $0.98; bid $0.97<br>17/599 (2.84%; 95% 1.78%–4.50%); U=0<br>N=599/675; ask=1: 139 | ask $0.99; bid $0.98<br>4/298 (1.34%; 95% 0.52%–3.40%); U=1<br>N=299/363; ask=1: 116 |
| [8,infinity) | ask $0.96; bid $0.9545<br>32/278 (11.51%; 95% 8.27%–15.80%); U=4<br>N=282/368; ask=1: 76 | ask $0.999; bid $0.99<br>2/339 (0.59%; 95% 0.16%–2.13%); U=3<br>N=342/374; ask=1: 168 | ask $1.00; bid $0.99<br>7/458 (1.53%; 95% 0.74%–3.12%); U=4<br>N=462/506; ask=1: 310 | ask $1.00; bid $0.99<br>4/592 (0.68%; 95% 0.26%–1.72%); U=3<br>N=595/636; ask=1: 469 |

### 60 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.095; bid $0.08<br>69/76 (90.79%; 95% 82.19%–95.47%); U=0<br>N=76/119; ask=1: 0 | ask $0.565; bid $0.55<br>175/380 (46.05%; 95% 41.11%–51.08%); U=0<br>N=380/451; ask=1: 0 | ask $0.89; bid $0.87<br>46/379 (12.14%; 95% 9.22%–15.81%); U=1<br>N=380/459; ask=1: 1 | ask $0.955; bid $0.945<br>4/60 (6.67%; 95% 2.62%–15.93%); U=0<br>N=60/91; ask=1: 6 |
| [1,2) | ask $0.17; bid $0.15<br>52/63 (82.54%; 95% 71.38%–89.96%); U=1<br>N=64/100; ask=1: 0 | ask $0.815; bid $0.80<br>64/249 (25.70%; 95% 20.67%–31.47%); U=1<br>N=250/347; ask=1: 0 | ask $0.97; bid $0.95<br>16/325 (4.92%; 95% 3.05%–7.85%); U=0<br>N=325/397; ask=1: 22 | ask $0.98; bid $0.97<br>4/68 (5.88%; 95% 2.31%–14.17%); U=0<br>N=68/91; ask=1: 10 |
| [2,4) | ask $0.37; bid $0.36<br>65/100 (65.00%; 95% 55.25%–73.64%); U=0<br>N=100/171; ask=1: 0 | ask $0.95; bid $0.94<br>40/380 (10.53%; 95% 7.83%–14.02%); U=1<br>N=381/467; ask=1: 26 | ask $0.99; bid $0.98<br>24/580 (4.14%; 95% 2.80%–6.08%); U=0<br>N=580/677; ask=1: 164 | ask $1.00; bid $0.99<br>2/170 (1.18%; 95% 0.32%–4.19%); U=3<br>N=173/217; ask=1: 91 |
| [4,8) | ask $0.82; bid $0.805<br>43/150 (28.67%; 95% 22.03%–36.36%); U=0<br>N=150/247; ask=1: 2 | ask $0.99; bid $0.98<br>8/426 (1.88%; 95% 0.95%–3.66%); U=2<br>N=428/511; ask=1: 181 | ask $1.00; bid $0.99<br>6/647 (0.93%; 95% 0.43%–2.01%); U=1<br>N=648/715; ask=1: 493 | ask $1.00; bid $0.99<br>2/307 (0.65%; 95% 0.18%–2.34%); U=2<br>N=309/355; ask=1: 255 |
| [8,infinity) | ask $1.00; bid $0.99<br>11/366 (3.01%; 95% 1.69%–5.30%); U=1<br>N=367/438; ask=1: 227 | ask $1.00; bid $0.99<br>2/412 (0.49%; 95% 0.13%–1.75%); U=7<br>N=419/445; ask=1: 340 | ask $1.00; bid $0.99<br>0/535 (0.00%; 95% 0.00%–0.71%); U=2<br>N=537/581; ask=1: 511 | ask $1.00; bid $0.99<br>0/577 (0.00%; 95% 0.00%–0.66%); U=3<br>N=580/608; ask=1: 555 |

### 30 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.02; bid $0.01<br>61/65 (93.85%; 95% 85.22%–97.58%); U=0<br>N=65/82; ask=1: 0 | ask $0.75; bid $0.73<br>140/337 (41.54%; 95% 36.41%–46.87%); U=0<br>N=337/417; ask=1: 18 | ask $0.99; bid $0.98<br>14/397 (3.53%; 95% 2.11%–5.83%); U=2<br>N=399/458; ask=1: 111 | ask $1.00; bid $0.99<br>0/69 (0.00%; 95% 0.00%–5.27%); U=1<br>N=70/91; ask=1: 60 |
| [1,2) | ask $0.105; bid $0.095<br>51/66 (77.27%; 95% 65.83%–85.71%); U=0<br>N=66/95; ask=1: 0 | ask $0.99; bid $0.98<br>24/239 (10.04%; 95% 6.84%–14.51%); U=1<br>N=240/285; ask=1: 99 | ask $1.00; bid $0.99<br>4/343 (1.17%; 95% 0.45%–2.96%); U=1<br>N=344/379; ask=1: 282 | ask $1.00; bid $0.99<br>1/101 (0.99%; 95% 0.17%–5.40%); U=0<br>N=101/116; ask=1: 95 |
| [2,4) | ask $0.76; bid $0.67<br>43/91 (47.25%; 95% 37.32%–57.41%); U=0<br>N=91/141; ask=1: 10 | ask $1.00; bid $0.99<br>6/480 (1.25%; 95% 0.57%–2.70%); U=0<br>N=480/530; ask=1: 380 | ask $1.00; bid $0.99<br>0/615 (0.00%; 95% 0.00%–0.62%); U=0<br>N=615/658; ask=1: 608 | ask $1.00; bid $0.99<br>0/158 (0.00%; 95% 0.00%–2.37%); U=2<br>N=160/180; ask=1: 158 |
| [4,8) | ask $1.00; bid $0.99<br>13/207 (6.28%; 95% 3.71%–10.45%); U=3<br>N=210/250; ask=1: 154 | ask $1.00; bid $0.99<br>1/539 (0.19%; 95% 0.03%–1.04%); U=2<br>N=541/565; ask=1: 529 | ask $1.00; bid $0.99<br>0/664 (0.00%; 95% 0.00%–0.58%); U=2<br>N=666/696; ask=1: 666 | ask $1.00; bid $0.99<br>0/284 (0.00%; 95% 0.00%–1.33%); U=0<br>N=284/301; ask=1: 283 |
| [8,infinity) | ask $1.00; bid $0.99<br>1/436 (0.23%; 95% 0.04%–1.29%); U=1<br>N=437/465; ask=1: 429 | ask $1.00; bid $0.99<br>0/440 (0.00%; 95% 0.00%–0.87%); U=3<br>N=443/465; ask=1: 440 | ask $1.00; bid $0.99<br>0/618 (0.00%; 95% 0.00%–0.62%); U=1<br>N=619/659; ask=1: 615 | ask $1.00; bid $0.99<br>0/609 (0.00%; 95% 0.00%–0.63%); U=8<br>N=617/638; ask=1: 616 |

### 15 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.01; bid $0.00<br>58/62 (93.55%; 95% 84.55%–97.46%); U=0<br>N=62/91; ask=1: 0 | ask $0.988; bid $0.97<br>105/352 (29.83%; 95% 25.29%–34.81%); U=0<br>N=352/420; ask=1: 124 | ask $1.00; bid $0.99<br>8/396 (2.02%; 95% 1.03%–3.94%); U=0<br>N=396/449; ask=1: 280 | ask $1.00; bid $0.99<br>0/65 (0.00%; 95% 0.00%–5.58%); U=1<br>N=66/76; ask=1: 64 |
| [1,2) | ask $0.795; bid $0.775<br>27/66 (40.91%; 95% 29.87%–52.95%); U=0<br>N=66/85; ask=1: 12 | ask $1.00; bid $0.99<br>3/273 (1.10%; 95% 0.37%–3.18%); U=0<br>N=273/303; ask=1: 251 | ask $1.00; bid $0.99<br>1/353 (0.28%; 95% 0.05%–1.59%); U=1<br>N=354/376; ask=1: 352 | ask $1.00; bid $0.99<br>0/84 (0.00%; 95% 0.00%–4.37%); U=1<br>N=85/96; ask=1: 84 |
| [2,4) | ask $1.00; bid $0.99<br>10/127 (7.87%; 95% 4.33%–13.89%); U=1<br>N=128/151; ask=1: 99 | ask $1.00; bid $0.99<br>1/480 (0.21%; 95% 0.04%–1.17%); U=0<br>N=480/513; ask=1: 476 | ask $1.00; bid $0.99<br>0/578 (0.00%; 95% 0.00%–0.66%); U=0<br>N=578/608; ask=1: 576 | ask $1.00; bid $0.99<br>0/161 (0.00%; 95% 0.00%–2.33%); U=1<br>N=162/178; ask=1: 162 |
| [4,8) | ask $1.00; bid $0.99<br>1/198 (0.51%; 95% 0.09%–2.80%); U=2<br>N=200/214; ask=1: 189 | ask $1.00; bid $0.99<br>0/589 (0.00%; 95% 0.00%–0.65%); U=2<br>N=591/614; ask=1: 589 | ask $1.00; bid $0.99<br>0/689 (0.00%; 95% 0.00%–0.55%); U=1<br>N=690/720; ask=1: 688 | ask $1.00; bid $0.99<br>0/277 (0.00%; 95% 0.00%–1.37%); U=4<br>N=281/304; ask=1: 281 |
| [8,infinity) | ask $1.00; bid $0.99<br>0/417 (0.00%; 95% 0.00%–0.91%); U=0<br>N=417/440; ask=1: 415 | ask $1.00; bid $0.99<br>0/493 (0.00%; 95% 0.00%–0.77%); U=4<br>N=497/516; ask=1: 495 | ask $1.00; bid $0.99<br>0/651 (0.00%; 95% 0.00%–0.59%); U=5<br>N=656/689; ask=1: 656 | ask $1.00; bid $0.99<br>0/622 (0.00%; 95% 0.00%–0.61%); U=4<br>N=626/646; ask=1: 625 |

### 10 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.01; bid $0.00<br>54/61 (88.52%; 95% 78.16%–94.33%); U=0<br>N=61/93; ask=1: 2 | ask $1.00; bid $0.99<br>73/345 (21.16%; 95% 17.18%–25.77%); U=0<br>N=345/390; ask=1: 205 | ask $1.00; bid $0.99<br>4/405 (0.99%; 95% 0.38%–2.51%); U=1<br>N=406/447; ask=1: 349 | ask $1.00; bid $0.99<br>0/62 (0.00%; 95% 0.00%–5.83%); U=0<br>N=62/75; ask=1: 62 |
| [1,2) | ask $1.00; bid $0.99<br>9/52 (17.31%; 95% 9.38%–29.73%); U=0<br>N=52/76; ask=1: 29 | ask $1.00; bid $0.99<br>1/294 (0.34%; 95% 0.06%–1.90%); U=0<br>N=294/315; ask=1: 288 | ask $1.00; bid $0.99<br>1/362 (0.28%; 95% 0.05%–1.55%); U=0<br>N=362/378; ask=1: 362 | ask $1.00; bid $0.99<br>0/74 (0.00%; 95% 0.00%–4.93%); U=1<br>N=75/84; ask=1: 75 |
| [2,4) | ask $1.00; bid $0.99<br>0/133 (0.00%; 95% 0.00%–2.81%); U=1<br>N=134/149; ask=1: 128 | ask $1.00; bid $0.99<br>0/490 (0.00%; 95% 0.00%–0.78%); U=0<br>N=490/509; ask=1: 488 | ask $1.00; bid $0.99<br>0/580 (0.00%; 95% 0.00%–0.66%); U=0<br>N=580/615; ask=1: 580 | ask $1.00; bid $0.99<br>0/160 (0.00%; 95% 0.00%–2.34%); U=0<br>N=160/178; ask=1: 160 |
| [4,8) | ask $1.00; bid $0.99<br>0/200 (0.00%; 95% 0.00%–1.88%); U=1<br>N=201/213; ask=1: 200 | ask $1.00; bid $0.99<br>0/594 (0.00%; 95% 0.00%–0.64%); U=2<br>N=596/622; ask=1: 596 | ask $1.00; bid $0.99<br>0/697 (0.00%; 95% 0.00%–0.55%); U=1<br>N=698/729; ask=1: 697 | ask $1.00; bid $0.99<br>0/257 (0.00%; 95% 0.00%–1.47%); U=4<br>N=261/284; ask=1: 261 |
| [8,infinity) | ask $1.00; bid $0.99<br>0/409 (0.00%; 95% 0.00%–0.93%); U=1<br>N=410/433; ask=1: 410 | ask $1.00; bid $0.99<br>0/518 (0.00%; 95% 0.00%–0.74%); U=3<br>N=521/543; ask=1: 519 | ask $1.00; bid $0.99<br>0/657 (0.00%; 95% 0.00%–0.58%); U=5<br>N=662/690; ask=1: 661 | ask $1.00; bid $0.99<br>0/630 (0.00%; 95% 0.00%–0.61%); U=5<br>N=635/654; ask=1: 635 |

### 5 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.02; bid $0.01<br>36/57 (63.16%; 95% 50.18%–74.48%); U=0<br>N=57/82; ask=1: 16 | ask $1.00; bid $0.99<br>39/368 (10.60%; 95% 7.85%–14.16%); U=0<br>N=368/412; ask=1: 308 | ask $1.00; bid $0.99<br>5/403 (1.24%; 95% 0.53%–2.87%); U=1<br>N=404/436; ask=1: 384 | ask $1.00; bid $0.99<br>0/68 (0.00%; 95% 0.00%–5.35%); U=0<br>N=68/83; ask=1: 67 |
| [1,2) | ask $1.00; bid $0.99<br>1/73 (1.37%; 95% 0.24%–7.36%); U=1<br>N=74/81; ask=1: 71 | ask $1.00; bid $0.99<br>0/327 (0.00%; 95% 0.00%–1.16%); U=1<br>N=328/337; ask=1: 327 | ask $1.00; bid $0.99<br>0/342 (0.00%; 95% 0.00%–1.11%); U=0<br>N=342/358; ask=1: 342 | ask $1.00; bid $0.99<br>0/82 (0.00%; 95% 0.00%–4.48%); U=0<br>N=82/90; ask=1: 82 |
| [2,4) | ask $1.00; bid $0.99<br>0/131 (0.00%; 95% 0.00%–2.85%); U=1<br>N=132/144; ask=1: 131 | ask $1.00; bid $0.99<br>0/514 (0.00%; 95% 0.00%–0.74%); U=0<br>N=514/535; ask=1: 513 | ask $1.00; bid $0.99<br>0/580 (0.00%; 95% 0.00%–0.66%); U=0<br>N=580/612; ask=1: 579 | ask $1.00; bid $0.99<br>0/133 (0.00%; 95% 0.00%–2.81%); U=1<br>N=134/155; ask=1: 134 |
| [4,8) | ask $1.00; bid $0.99<br>0/210 (0.00%; 95% 0.00%–1.80%); U=0<br>N=210/215; ask=1: 210 | ask $1.00; bid $0.99<br>0/582 (0.00%; 95% 0.00%–0.66%); U=0<br>N=582/610; ask=1: 582 | ask $1.00; bid $0.99<br>0/693 (0.00%; 95% 0.00%–0.55%); U=4<br>N=697/723; ask=1: 697 | ask $1.00; bid $0.99<br>0/264 (0.00%; 95% 0.00%–1.43%); U=3<br>N=267/286; ask=1: 266 |
| [8,infinity) | ask $1.00; bid $0.99<br>0/391 (0.00%; 95% 0.00%–0.97%); U=2<br>N=393/415; ask=1: 393 | ask $1.00; bid $0.999<br>0/542 (0.00%; 95% 0.00%–0.70%); U=2<br>N=544/567; ask=1: 544 | ask $1.00; bid $0.99<br>0/661 (0.00%; 95% 0.00%–0.58%); U=5<br>N=666/690; ask=1: 665 | ask $1.00; bid $0.99<br>0/624 (0.00%; 95% 0.00%–0.61%); U=5<br>N=629/662; ask=1: 629 |

### 3 seconds remaining

Each cell: median ask/bid; matched loss rate and interval; U; matched/full N; ask=1 count.

| X (bp) \ Y (bp) | <-2 | [-2,0) | [0,2) | >=2 |
|:---|:---|:---|:---|:---|
| [0,1) | ask $0.875; bid $0.67<br>24/50 (48.00%; 95% 34.80%–61.49%); U=0<br>N=50/74; ask=1: 20 | ask $1.00; bid $0.99<br>33/369 (8.94%; 95% 6.44%–12.29%); U=0<br>N=369/405; ask=1: 322 | ask $1.00; bid $0.99<br>3/414 (0.72%; 95% 0.25%–2.11%); U=0<br>N=414/444; ask=1: 404 | ask $1.00; bid $0.99<br>0/67 (0.00%; 95% 0.00%–5.42%); U=0<br>N=67/83; ask=1: 67 |
| [1,2) | ask $1.00; bid $0.99<br>1/86 (1.16%; 95% 0.21%–6.30%); U=1<br>N=87/94; ask=1: 85 | ask $1.00; bid $0.99<br>0/311 (0.00%; 95% 0.00%–1.22%); U=1<br>N=312/322; ask=1: 311 | ask $1.00; bid $0.99<br>0/346 (0.00%; 95% 0.00%–1.10%); U=0<br>N=346/361; ask=1: 344 | ask $1.00; bid $0.99<br>0/76 (0.00%; 95% 0.00%–4.81%); U=1<br>N=77/84; ask=1: 77 |
| [2,4) | ask $1.00; bid $0.99<br>0/122 (0.00%; 95% 0.00%–3.05%); U=0<br>N=122/129; ask=1: 121 | ask $1.00; bid $0.99<br>0/511 (0.00%; 95% 0.00%–0.75%); U=1<br>N=512/531; ask=1: 511 | ask $1.00; bid $0.99<br>0/592 (0.00%; 95% 0.00%–0.64%); U=0<br>N=592/620; ask=1: 591 | ask $1.00; bid $0.99<br>0/135 (0.00%; 95% 0.00%–2.77%); U=1<br>N=136/155; ask=1: 136 |
| [4,8) | ask $1.00; bid $0.99<br>0/208 (0.00%; 95% 0.00%–1.81%); U=0<br>N=208/214; ask=1: 208 | ask $1.00; bid $0.99<br>0/578 (0.00%; 95% 0.00%–0.66%); U=1<br>N=579/603; ask=1: 579 | ask $1.00; bid $0.99<br>0/716 (0.00%; 95% 0.00%–0.53%); U=2<br>N=718/747; ask=1: 717 | ask $1.00; bid $0.99<br>0/259 (0.00%; 95% 0.00%–1.46%); U=4<br>N=263/281; ask=1: 262 |
| [8,infinity) | ask $1.00; bid $0.999<br>0/412 (0.00%; 95% 0.00%–0.92%); U=2<br>N=414/435; ask=1: 414 | ask $1.00; bid $0.999<br>0/518 (0.00%; 95% 0.00%–0.74%); U=3<br>N=521/544; ask=1: 521 | ask $1.00; bid $0.999<br>0/700 (0.00%; 95% 0.00%–0.55%); U=4<br>N=704/732; ask=1: 703 | ask $1.00; bid $0.99<br>0/604 (0.00%; 95% 0.00%–0.63%); U=4<br>N=608/639; ask=1: 608 |
