# Spot-to-TWAP response study — results and methods

**Status: the historical ghost/delay/settlement pilots are verified, and two one-hour live ghost canaries completed on September 14, 2026 UTC.** The fixed September 1–8 replays and projected-lead grid companion are complete. The revised event-response study and precise first-arrival attribution remain outstanding. This file consolidates the study; the [original leader-risk findings](H3_TWAP_LEADER_RISK_FINAL_REPORT.md) retain separate provenance. [Checkpoint A](GHOST_TWAP_CHECKPOINT_A.md) and B's bounded live runs are complete. API/SSE delivery remains C in the [live plan](GHOST_TWAP_LIVE_PLAN.md).

**Latest live result:** the [combined freshness and batch-eligibility canary](GHOST_TWAP_COMBINED_CANARY_RESULTS.md) verified 7,054 new decisions and all 36,000 scheduled cache observations. After 65 seconds, key presence was **99.632%** and usable 5/10/30-second forecasts **98.003% / 98.156% / 98.727%**. Median forecast errors were **0.003372 / 0.022017 / 0.260228 bp**, with confirmed Redis lead **4.662 / 9.658 / 29.657 seconds**. The new rule retained 314 partial batches. A spot disconnect caused history rebuilding, and publication remained **38.22 ms median**, above the 10 ms objective. These sampled local-cache and price findings do not measure browser delivery or trading edge; the first run below remains separate evidence.

**Prospective ghost finding:** the [official one-hour canary report](GHOST_TWAP_CANARY_RESULTS.md) records 5/10/30-second median errors of **0.0074 / 0.0459 / 0.4223 bp**, versus **0.2312 / 0.4667 / 1.3506 bp** for persistence on identical published target pairs. Median confirmed lead after Redis acknowledgement was **4.785 / 9.771 / 29.772 seconds**. All 7,292 audit rows were finalized and externally verified. Publication took **39.19 ms median**, missing the under-10-ms objective. These are one-hour live measurements with different sampling/policies from the development week; browser delivery, long-running capacity and trading returns remain untested.


**Answer on leading delay:** the first TWAP stamp predicted to include a Chainlink spot slot arrives a median **3.088 seconds (about 3.1 seconds) after the retained spot receipt**. The 10th–90th percentiles are 2.552–3.611 seconds, and the 99th percentile is 4.042 seconds, across 550,393 matched pairs. This is a useful measured receipt gap under the verified sampled alignment. It is conditional on retained matching reports and does not establish exact input-version causality or the delay from a Binance price move. The part-4 verification below records coverage and exceptional arrivals.

**What the 3.1-second delay means:** it marks the typical arrival of the first report predicted to include the changed spot slot. Under the ideal equal-weight 60-slot reconstruction, one changed one-second slot contributes **1/60 of the sustained move, approximately 1.67%**. Three changed slots contribute 5%. The contribution builds as new slots replace old ones.

| Illustrative time after receiving the first changed Chainlink spot slot | Changed slots in the 60-slot window | Ideal share of the move incorporated |
|---|---:|---:|
| About 3.1 seconds | 1 | 1.67% |
| About 4.1 seconds | 2 | 3.33% |
| About 5.1 seconds | 3 | 5.00% |

**This timeline is a model illustration, not a measured local-receipt response curve.** It assumes flat prices before an instantaneous permanent step, the new price remaining constant, one update per second, complete windows and a constant 3.1-second receipt gap. Actual gaps vary. The paired receipt-delay distribution was independently reproduced; these later receipt-time percentages were not measured. The illustration neither shifts nor replaces the provider-time pilot table below.

**Answer on the response curve:** the saved section-3 pilot reports the following mean normalized TWAP changes for **190 selected sustained Chainlink moves**. Each move is at least 2 bp over two seconds and is selected using its subsequent persistence. These are reported pilot results whose SQL was inspected; this event query has not been independently rerun under the revised protocol.

| Seconds after the detected move, on provider timestamps | Mean TWAP change as a percentage of the spot move |
|---:|---:|
| 5 | 5.0% |
| 10 | 13.1% |
| 30 | 49.5% |
| 60 | 103.6% |

The practical pattern is little response in the first 5–10 seconds, roughly half by 30 seconds, and roughly the full move by one minute. The pilot measures TWAP change from W(t−1), divided by S(t)−S(t−2); its time zero is the end of the detected two-second move. It does not isolate the new move's causal contribution from later spot changes or prices leaving the window. Those effects and non-flat prehistory can produce values above 100%. For a clean permanent step with flat prehistory, the ideal 60-second average incorporates about 1/60 of the step per second during its ramp. **The table uses provider time; the separate 3.088-second result uses local receipts. Do not add that delay to these table horizons or relabel the table as a measured local-receipt curve.** [Saved response output](results/spot_twap_response/2026-09-13-pilot/pilot_output.txt), [pilot query](research/spot_twap_response/pilot.sql).

**Separate forecasting finding:** at 30 seconds before close, the receipt-clock projection is correct in 1,889 of 1,925 eligible markets (98.13%), versus 1,800 (93.51%) for current TWAP and 1,866 (96.94%) for current spot. The gain persists in the prespecified three-second freshness subset. This is an exploratory retained-history replay of an already studied week; prospective accuracy and trading returns remain untested. Full counts, missingness and verification appear below.

**Rolling ghost-price finding:** the part-5 provider-clock pilot is independently reproduced. At source-stamp horizons of 5, 10 and 30 seconds, the median absolute price errors are 0.006, 0.031 and 0.311 bp, compared with 0.155, 0.307 and 0.868 bp for keeping TWAP unchanged. This uses the same reconstructed-window/current-spot continuation idea, with continuous-price grading. It has no receipt-time input restrictions, so these are historical source-clock results; this provider-clock pilot did not measure live accuracy; the subsequent canary is recorded above. The full part-5 review below records counts, tails and scope.

**Subsequent receipt-clock ghost finding:** part 6 adds receipt-time input cutoffs and records actual target arrivals. It is independently reproduced: at source horizons 5/10/30 seconds, median absolute errors are **0.005/0.027/0.292 bp**, and median arrival leads from the decision instant are **4.614/9.571/29.611 seconds**. These are encouraging retained-history results on the same development week. They do not establish a uniform error tolerance or prospective publication performance. Part 6 below records the error tails, carry-count limitation and market-boundary effects in the side tables.

This is fresh research connected to [Q4 and Q5](RESEARCH_QUESTIONS.md). No retired model, label, threshold, or execution state is reused. Work stays under a new research-only directory and starts with read-only historical analysis.

## Agreed scope and interpretation

The useful structure is **identity/alignment → step response → timing → settlement projection**. If the simple average reproduces the observed TWAP closely, a general distributed-lag model and a separate threshold-selection pilot are unnecessary. Descriptive identity and event tables need neither a train/test split nor bootstrap inference simply to report what happened.

Three distinctions remain essential:

1. The best-fitting three-second timestamp alignment is an empirical relationship between these retained streams. It does not establish three seconds of physical dead time or the publisher's exact internal inputs.
2. Under that alignment, 63 − T counts candidate past final-window slots with complete, instantaneous delivery, clamped to [0,60]. A receipt-clock projection must distinguish actually received observations from carried estimates and future extrapolation.
3. Days examined to select the alignment are development evidence, not held-out days. The projection adds an assumption about unobserved prices and needs a frozen rule plus later untouched markets before its accuracy can be described as out of sample.

The other agent supplied [pilot SQL](research/spot_twap_response/pilot.sql), [follow-up SQL](research/spot_twap_response/pilot_part2.sql), and [outputs/notes](results/spot_twap_response/2026-09-13-pilot/NOTES.md). The follow-up corrects the original two-second alignment to a newest constituent stamp three seconds earlier and explicitly labels the pilot provider-clock only. Independent one-day reconstruction and fixed-week projection checks are recorded below. The full-history event and delivery tables have been inspected but not independently rerun here.

The saved sustained-event output contains **190 events**, rather than the earlier message's 183. Its seven quoted response fractions correspond to 5/10/20/30/45/60/62 seconds. Counts from a growing full-history query must be tied to their saved run. The notes correctly acknowledge that this pilot has no explicit cooldown or flat-prehistory filter.

The saved Binance-transfer output agrees with the quoted Chainlink fractions after rounding: 80.8%, 91.8%, 95.8%, 100.6%, and 102.9% at 0/1/2/5/10 seconds for 1,178 events. Its Binance fractions at those horizons are 100%, 106.3%, 109.5%, 114.5%, and 116.3%; use these saved values rather than the earlier message's 100/104/106/110/115%. They are selected-cohort average paths, not a claim that every move persists or a direct physical-latency estimate.

## Data and clocks

Primary input is retained Chainlink BTC/USD spot; output is the actual BTC/USD crypto_prices_twap_sixty feed with window_s=60 and exact E18 price. Binance BTC/USDT is a separate upstream comparison with its own price basis and timestamp semantics. Earlier 30-second settlements retain their historical identity and are excluded.

- price_samples retains one selected spot state per instrument/second, with provider and local receipt timestamps. Chainlink rows use the floored provider second; Binance rows use the local sampler's second and preserve the selected ticker's earlier provider/receipt clocks. These keys are not interchangeable. Same-second upserts prevent reconstruction of every revision and first arrival.
- polymarket_twap_events retains accepted TWAP events, including observation, publisher and wall/monotonic receipt clocks, connection identity, and sequence.
- TWAP sessions/gaps and spot coverage identify missing evidence. A missing spot row is not automatically a measured connection failure. The accepted-event ledger omits rejected upstream messages.
- Check the optional raw Chainlink event table once for the stated date range. If empty, record that and use retained spot history with its limitations. Do not enable collection or assume hidden raw observations.
- Keep continuous history across five-minute market boundaries, with at least 120 seconds of context on either side of the analysis interval. Incomplete follow-ups remain missing.

Keep **source-time** and **local-visibility** views separate. Source time describes retrospectively reported dynamics. Visibility uses only values received by the cutoff; with the current spot table it is a retained-history approximation. Do not insert a later retained revision into an earlier cutoff, backdate arrivals, or interpolate future observations into an as-of value. Ambiguous same-time revisions and unresolved ordering remain flagged.

Use UTC epoch milliseconds for source/decision keys and preserve nanosecond receipt evidence. Prices and financial arithmetic remain Decimal/NUMERIC. Floating-point display copies must not feed financial calculations or persisted truth. Freeze input/output hashes, extraction time, cohort boundaries, code version and conventions in a manifest.

## 1. Identity and timestamp alignment

Test an explicitly signed candidate law. Timestamps in the following formulas are seconds for readability:

    M_a(u) = [S(u+a−59) + ... + S(u+a)] / 60
    a ∈ {−4, −3, ..., +4}

Here u is the TWAP observation timestamp and a is the offset of the **newest** constituent spot stamp. The corrected a=−3 means the 60 spot stamps u−62, ..., u−3. Spell out these endpoints in the query and report; an SQL shift's sign alone is insufficient.

Require 60 distinct consecutive constituent seconds for the primary sample-mean check. Do not average 57 rows and call it a complete 60-second window. Compare shifts on a common complete cohort and report each shift's own available count. State how non-integer timestamps, spot revisions, identical duplicate TWAP reports and conflicting prices are handled. Report conflicts and exclusions.

For each UTC day and overall, report:

| Output | Definition |
|---|---|
| Coverage | Candidate observations, complete windows, missing/conflicting windows and exclusions |
| Signed residual | 10,000 × (W(u) − M_a(u)) / W(u), in basis points |
| Absolute residual | Median, 99th percentile, maximum, timestamp of maximum |
| Alignment profile | Those errors for all nine shifts, on the common cohort |
| Stability | Best shift and residual distribution per day, with counts |

The earlier roughly 0.02 bp median and 1.6 bp maximum described the superseded a=−2 reconstruction. Preserve its denominator and cohort in historical comparisons. A sharp minimum supports the preferred sampled-data alignment; precision is bounded by the lag grid, timestamp granularity and input proxy. Inspect the largest residuals and missing windows before declaring the relationship adequately explained.

Chainlink describes a duration-weighted average over underlying report windows, which can widen. Public RTDS spot samples are not proven to contain those exact reports. Carrying each retained observation forward until the next source observation creates a piecewise-constant reconstruction; averaging it on the declared one-second grid gives elapsed-time weighting under that convention. The new part-3 pilot supports this reconstruction over averaging only retained rows. It does not prove that every missing collector row represents an unchanged underlying price. Preserve carry ages and explicit gaps. Do not conflate a discrete 60-stamp sum with a continuous interval having different endpoints. [Chainlink timestamp and TWAP documentation](https://docs.chain.link/data-streams/how-report-timestamps-work), [Polymarket TWAP documentation](https://docs.polymarket.com/market-data/chainlink-twap).

Use all available history for this descriptive audit. Do not call the daily breakdown held out if it participated in selecting the shift. If the candidate leaves systematic unexplained errors, report them and revisit the reconstruction; no automatic flexible-model expansion is required.

## 2. Step response

Use the proposed detector: **an absolute Chainlink spot move of at least 2 bp over two seconds, followed by a 65-second cooldown from the last accepted trigger**. The supplied pilot SQL does not implement this cooldown for Chainlink events; its numerical count belongs to a different event definition. Freeze remaining sampling/gap and persistence conventions before comparing response curves; do not tune them to obtain a preferred ramp. Cooldown reduces duplicate triggers but does not make all observations independent.

For the source view, record the actual observations defining the move, their elapsed time, and the onset bracket (t_pre, t0]. For the receipt view, reconstruct qualification from observations available then and preserve both constituent receipt clocks. Show common-event comparisons and counts of events that cannot be reconstructed locally. The two views need not select identical events.

Let A = S(t0) − S(t_pre). Use this **pre-move spot baseline** to normalize the later spot path. For TWAP, W_base is the latest valid observation strictly before t_pre, with its age recorded:

    R(h) = [W(t0+h) − W_base] / A

Each horizon uses only the latest valid observation at or before that horizon on the declared clock. Include h=0: response during the detection interval must not be reset to zero. Keep negative responses, reversals and values above 100%. Report dollars and normalized fractions. Declare baseline-age and horizon-gap limits; missing observations are not zero.

Show the full curve from 0 through 62 seconds and tables at **0, 5, 10, 15, 30, 45, 60 and 62 seconds**, with pre-event context and follow-up to 120 seconds where available. Include total and per-horizon counts, a common complete cohort, up/down splits and daily summaries.

All detected moves are the primary population. Report hindsight-defined sustained steps and transient/reversed moves separately. The supplied pilot requires future spot to remain within 1.5 bp of its detected level for seconds 1 through 62, with a complete 65-second follow-up, but has no flat-prehistory condition. Reproduce that definition under its own label first. An ideal-step subgroup additionally needs a declared sufficiently flat preceding-minute condition. Its count will differ; the proposed cooldown and any revised baseline also require fresh output. A future-persistence label is not available at onset.

Compare two references:

1. **Ideal persistent step.** With flat prehistory and an instantaneous sustained move, a duration-weighted 60-second average absorbs min(h/60,1) without extra delay. With delay delta, the reference is clip((h−delta)/60,0,1). Clipping applies only to the ideal reference. Show the alignment range implied by the actual move's onset bracket.
2. **Observed-path reconstruction.** Apply the frozen identity candidate to the complete spot path, including prehistory, prices leaving the window, additional moves and reversals. Use corresponding pre-move baselines.

| Time after an ideal step | Fraction absorbed without extra delay | TWAP change for a sustained +$60 step |
|---:|---:|---:|
| 5 seconds | 8.33% | $5 |
| 10 seconds | 16.67% | $10 |
| 30 seconds | 50.00% | $30 |
| 60 seconds | 100.00% | $60 |

The ideal half-response time is 30 seconds plus delay; averaging time is not dead time. A brief spike need not reach 100% absorption. Prices leaving the window can move TWAP before or against the new move.

For descriptive curves, report event dispersion and daily summaries. Confidence intervals are optional; if presented, account for overlapping windows and day/event dependence rather than resampling one-second rows independently. Milestone times are optional; if reported, bracket them by observed reports and count non-crossings.

## 3. Timing chain and local visibility

Use **direct per-second receipt pairing as the primary Chainlink timing measurement**. Under the empirically supported a=−3 reconstruction, a spot slot stamped s first belongs to the TWAP stamped s+3:

    L(s) = earliest local receipt of TWAP stamped s+3
           − retained local receipt of Chainlink spot stamped s

Jump detection and curve fitting are unnecessary for this statistic. This adopts the simpler part-4 method and supersedes the earlier requirement to estimate delay from isolated jumps. Name the result **receipt lag to the first TWAP stamp predicted to include the spot slot**. It estimates visible incorporation under the sampled reconstruction; it does not prove that the retained spot revision was the publisher's exact input or identify internal computation time.

Preserve earliest TWAP receipt at nanosecond precision, spot's millisecond precision, valid topic/symbol/window, duplicate/conflicting report counts, total candidate spot slots and missing exact s+3 reports. Reproduce the supplied integer-millisecond calculation separately. Keep negative differences and inspect their receipt/source sequence; a negative difference alone does not prove a clock error. Do not replace one with a later positive pair. Missing s+3 reports leave this statistic unavailable rather than silently substituting a later report. A separate earliest-received containing-report analysis would require its own coverage and ordering rules.

Keep three supporting analyses separate:

- **Binance-to-Chainlink response.** Use actual Binance provider/receipt clocks and observed price paths to assess corresponding moves. Pairing Binance's local sampling bucket with a TWAP source stamp does not identify a move's transfer time. The proposed s+3 and s+4 pairings are schedule diagnostics, not measured same/next-second pickup. The earlier 3 bp/two-second event detector, pre-move normalization and cross-correlation remain descriptive evidence with their own cohorts and timestamp limitations; a correlation peak is not a physical latency measurement.
- **Spot/TWAP timestamp alignment.** Carry forward the full step-1 profile. It supplies the candidate first-containing stamp for the pairing, without proving a fixed physical dead time. Use jump response and first-difference residuals as illustrations and model checks; their noise does not invalidate direct clock pairing.
- **Recorded delivery offsets.** Measure RTDS envelope timestamp minus payload observation timestamp, local receipt minus envelope timestamp, and local receipt minus observation timestamp. Report the clock-field semantics, distributions, missing clocks and exceptional sequences. The envelope timestamp is not a measured timestamp of Chainlink's internal calculation. For an additive decomposition, calculate the terms for each matched pair first; separate marginal medians do not add.

Receipt-based step curves still use observations actually available at each horizon. Retrospective source curves and timing-pair histograms are different summaries; preserve incomplete coverage and the spot-upsert limitation in each. Tight retained receipt-offset distributions do not establish how often earlier versions were overwritten.

Do **not** prescribe that everything shifts right by 1.8 seconds. A local response anchored to local spot receipt involves both spot and TWAP arrival times. Measure their joint receipt difference directly instead of adding the TWAP's marginal delivery median to source alignment or subtracting it into an information advantage.

## 4. Receipt-clock settlement projection

This forecasts the final value under a declared continuation assumption, then tests whether side calls improve over current TWAP or spot.

First establish which source timestamp the official closing TWAP represents. Let market close be E, decision time D=E−T, and closing TWAP source stamp C. Use C=E only after verifying that relationship for the settlement identity. Later official outcomes are grading evidence, not input features. Audit the opening Price to Beat and contract equality rule.

Determine C from the settlement rule or a schedule fixed before D. Do not select the forecast window using a closing timestamp learned only during later reconciliation. If C cannot be determined at D, report that case separately as a retrospective diagnostic conditional on the closing timestamp.

Under the discrete candidate from step 1:

    I_C = {C+a−59, ..., C+a}
    K(D) = members of I_C whose required spot values were received by D

Require valid values and unambiguous retained availability. Count actual members, not inferred completeness from the newest timestamp.

If C=E, all seconds are present, and delivery has a fixed integer lag ell, the count under the discrete candidate is clamp(60−T−a−ell,0,60). Under the corrected a=−3 alignment, instantaneous delivery gives clamp(63−T,0,60). At 30 seconds out that is **33** with instantaneous delivery and **31** with a two-second lag. The original a=−2 convention would give 32 and 30 respectively. Fractional lags require actual timestamp comparisons. These shortcuts explain the algebra; implementation uses actual receipt eligibility.

The agreed carry-forward continuation projection replaces the earlier rule that filled every unobserved slot with current spot:

    projected_close(D) = sum over the 60 target slots of S_hat_D(slot) / 60

For a target slot at or before D, S_hat_D is the latest preceding source observation available by D, carrying it forward only from its own source timestamp. For a genuinely future target slot, it is the declared current spot available at D, held flat. No observation received after D may enter either part. In particular, a late-arriving update cannot be backdated into an earlier decision. A received update may reconstruct an earlier source slot at the current decision if both its source timestamp is at/before that slot and its receipt is at/before the current decision.

Keep separate counts for **exact retained observations, carried historical estimates, future extrapolations, and unavailable slots**. Carrying a value does not turn a silent second into an observed unchanged price. Missing current seeds, prolonged gaps and ambiguous revisions remain explicit; do not force all markets to be eligible.

For the next bounded pilot, preserve the provider pilot's strict 600-second predecessor lookback as the broad comparison rule. Report the ages rather than calling those values fresh. Also show a prespecified comparison requiring current spot and raw TWAP source/receipt ages of at most three seconds, to connect with the earlier leader-risk study. This secondary threshold is not chosen after inspecting accuracy. Select the current received state before evaluating freshness and source-clock anomalies; do not reveal an older favorable value by filtering first.

If the reconstruction is expressed as continuous duration weighting, retain the same source interval endpoints and availability rules. A close historical fit does not eliminate reconstruction error, particularly near the strike.

At **60, 30, 15, 10, 5 and 3 seconds before close**, persist:

- Decision time, identities, input timestamps/ages, known coverage and missingness.
- Projected close and projected lead over the decision-time Price to Beat.
- Raw TWAP and raw spot forecasts from the same cutoff.
- Official final value/outcome used for grading, signed/absolute price error, predicted side and correctness.

Compare all three on the **same eligible markets**, alongside full-cohort availability and exclusion counts. Report price-error distributions, exact correct/incorrect/tied/unresolved counts, daily summaries and side accuracy. For projection versus raw TWAP, include the paired table: both correct, projection only correct, raw TWAP only correct, both wrong. Print numerators and denominators, including the error count behind 99.9%.

Use the actual TWAP stream event stamped exactly at market opening and received by the decision cutoff, following the earlier study's boundary-event rule. Multiple different pre-cutoff E18 boundary values make the opening reference unavailable; no received boundary event means unavailable. Keep its receipt/source provenance and compare it with the official opening value only in the audit. A later reconciled opening price cannot become a purported decision-time input.

The quoted pilot week is development evidence unless the complete projection rule was frozen before examining it. Reproduce it retrospectively, then freeze alignment, projection, price/gap eligibility, tie rules and evaluation dates for later untouched complete markets. Historical feature context across the boundary is legitimate; omit event follow-ups or training outcomes crossing into evaluation. A blanket embargo is unnecessary absent such overlap. If all history was inspected, label historical replay accordingly and reserve a later period for prospective validation.

Add projected lead in a **separate versioned companion extraction**, preserving the original leader-risk inputs and results. Re-cut descriptive grids after auditing the forecast. Do not select favorable cells and validate them on the same data. Better side accuracy does not establish better returns at available asks; a pricing test needs causal quote matching, fees and execution limits.

## Deliverables and agreement

Keep reproducible queries/code and a manifest with:

1. Daily identity residuals, the complete shift profile and coverage.
2. Event definitions/catalogue, normalized spot/TWAP paths, reference curves, sustained/transient summaries and counts.
3. Timing distributions and separate source/receipt results.
4. Projection rows, paired baseline comparisons, price errors, coverage, and separated retrospective/prospective results.
5. One final response/projection report linking the artifacts and stating what was established, rejected or unresolved.

The completed leader-risk report remains the official record of that study. This file is the consolidated record of the response study's completed checks and remaining work. Results explicitly marked independently reproduced are verified findings of the stated historical cohort; supplied-only outputs retain that label. The outstanding revised event study and prospective validation must not be described as completed.

The supplied follow-up and independent check agree on the corrected discrete alignment. Proceed with the four-step structure, actual receipt-based constituent counts, paired causal comparisons, and honest development/held-out labels. The broader unknown-system machinery can be dropped while the simple reconstruction explains the response adequately; availability and forecasting validation still need to be measured.

The previously questioned results.csv, section10_results.csv and section10_manifest.json are present in this workspace; the two CSV hashes match their manifests. A [portable verification bundle](results/spot_twap_response/2026-09-13-independent-review/verification_artifacts.zip) keeps the small queries, summaries, manifests and documents together using repository-relative paths. [Bundle inventory and hashes](results/spot_twap_response/2026-09-13-independent-review/bundle_manifest.json) record included and omitted files; large raw extracts remain local. The bundle does not establish that another application's workspace has received those files.

## Review of the supplied pilot SQL

Both SQL files explicitly label projection as a **provider-clock pilot**. That is a valid exploratory question, but its figures cannot be relabeled receipt-clock performance. The notes supersede original sections 2 and 7 with sections 9 and 10; the table below preserves the audit of the original code and identifies what remains relevant.

| Location in pilot.sql | What the code does | Implication for the merged test |
|---|---|---|
| Original sections 1–2 | Scans shifts, then assumes a=−2 for the daily table | Follow-up sections 8–9 correct this to a=−3, with constituent stamps t−62 through t−3. Add common-cohort coverage/tail reporting. |
| Section 3 | Uses a 2 bp/two-second move, future ±1.5 bp persistence, no Chainlink cooldown, and TWAP baseline at t−1 | These are the pilot's definitions, not the proposed cooldown and pre-move TWAP baseline. Recompute under the frozen merged definitions and report both clearly. |
| Section 5 | Keeps Binance candidates more than 62 seconds after the immediately previous qualifying candidate | This is a quiet-period filter, not cooldown measured from the last accepted event. It can exclude additional otherwise-separated events. Preserve this definition for reproduction. |
| Section 7 input selection | Selects prices by exact source second, without any received-by-decision condition | It uses information that may not yet have arrived locally. Rerun with actual receipt cutoffs for a live-information interpretation. |
| Section 7 opening reference | Reads chainlink_open_price from the later resolutions table | It supplies the official strike for retrospective grading; it does not establish that the strike was known at the checkpoint. Use the causal opening reference for the visibility test. |
| Section 7 missing constituents | Uses known_sum / known_n × (62−T), allowing up to three absent seconds | It fills missing past slots with the mean of retained past slots. This differs from both a fully observed known sum and the proposed current-spot fill for unobserved inputs. Report that imputation explicitly and compare like-for-like. |
| Section 7 scoring | Compares predicted side with close ≥ open; null price comparisons fall into the incorrect branch | The independent week check found no missing opening/closing prices or winners, and no disagreements with official winners on selected rows. Preserve those checks in the new test. |

The supplied script is not modified by this review. The reviewed SHA-256 is 378dc0cbddcbcb62a4199b3de2e9e1b6a1cd3b17d5b9f2715818b321980d5328. Preserve that exact version when reproducing its exploratory outputs. Do not run its full-history 1,500-second-timeout batch as an unbounded production diagnostic; use bounded read-only extracts and local research analysis.

## Reproduced checks and follow-up findings

### Alignment: independent one-day check

For 2026-09-11 UTC, the durable TWAP ledger and the exact materialized TWAP table used by the other agent contain the same **84,241 timestamps and prices**, with no timestamp conflicts or source/key mismatches. The optional raw Chainlink table has zero received-day rows. Of the TWAP timestamps, **24,285** have complete 60-spot-second windows at every shift from −4 through +4. On that same common cohort:

| Newest constituent offset a | Median absolute error (bp) | 99th percentile (bp) | Maximum (bp) |
|---:|---:|---:|---:|
| −4 seconds | 0.042397 | 0.508409 | 2.066466 |
| **−3 seconds** | **less than 0.000001** | **0.200274** | **1.395073** |
| −2 seconds | 0.025469 | 0.369106 | 1.534423 |
| −1 second | 0.065472 | 0.737288 | 2.068146 |
| 0 seconds | 0.104340 | 1.114349 | 3.097058 |

All nine shifts are retained in the [summary](research/spot_twap_response/pilot_review/shift_summary.csv) and [manifest](research/spot_twap_response/pilot_review/manifest.json). Decimal local calculations were cross-checked with an independent NUMERIC SQL calculation on the original materialized table; 500 direct 60-constituent sums also exactly matched the rolling sums. Complete-window coverage is selective, and errors have nonzero tails. This supports the a=−3 sampled alignment, not an exact identity at every timestamp or a measurement of physical dead time.

The supplied full-history follow-up independently reports the same preferred alignment: n=301,334 complete windows, median rounded to 0.0000 bp, p99=0.1686 bp and maximum=1.427 bp. Its daily a=−3 versus a=−2 comparison uses paired complete windows. Those all-history outputs were inspected, not rerun here. The daily tables are descriptive; they were not held out from selecting the offset.

### Projection: independent reproduction of the original fixed-week pilot

The original a=−2 section-7 results reproduce for the 2,016 resolved markets starting in [2026-09-01, 2026-09-08) UTC:

| Seconds left | Selected markets | Projection correct / errors | Projection accuracy | Raw TWAP accuracy | Excluded markets |
|---:|---:|---:|---:|---:|---:|
| 60 | 1,918 | 1,776 / 142 | 92.60% | 87.75% | 98 |
| 30 | 1,752 | 1,718 / 34 | 98.06% | 93.55% | 264 |
| 15 | 1,574 | 1,565 / 9 | 99.43% | 97.27% | 442 |
| 10 | 1,481 | 1,480 / 1 | 99.93% | 97.91% | 535 |
| 5 | 1,405 | 1,405 / 0 | 100.00% | 99.22% | 611 |
| 3 | 1,374 | 1,374 / 0 | 100.00% | 99.49% | 642 |

The matched rows compare projection, raw TWAP and raw spot on the same cohort. At 30 seconds, 91 decisions improve over raw TWAP and 12 worsen; 22 are wrong under both. At 10 seconds, 30 improve and none worsen; one is wrong under both. Missing-constituent imputation is used in 1,117 of 1,752 selected rows at 30 seconds and 1,157 of 1,481 at 10 seconds. These are source-clock results with a reconciled strike and the original past-mean imputation, not live forecasts. [Results](research/spot_twap_response/projection_pilot_review/results.csv), [query and execution manifest](research/spot_twap_response/projection_pilot_review/manifest.json).

### Independently reproduced follow-up: corrected alignment and paired sample

Section 10 compares both alignments on the intersection of their eligible markets. An independent bounded, read-only repeatable-read execution reproduced **all six supplied output rows exactly**:

| Seconds left | Paired markets | a=−3 projection errors | a=−2 projection errors | Raw TWAP errors | a=−3 improves / worsens versus TWAP |
|---:|---:|---:|---:|---:|---:|
| 60 | 1,918 | 142 | 142 | 235 | 131 / 38 |
| 30 | 1,739 | 35 | 34 | 112 | 88 / 11 |
| 15 | 1,553 | 7 | 9 | 43 | 42 / 6 |
| 10 | 1,460 | 1 | 1 | 31 | 30 / 0 |
| 5 | 1,384 | 0 | 0 | 11 | 11 / 0 |
| 3 | 1,353 | 0 | 0 | 7 | 7 / 0 |

The corrected alignment preserves the exploratory forecasting improvement. Its changed denominator must not be compared directly with the original section-7 percentages as if membership were identical. The updated supplied notes now correctly state that errors differ by **at most two markets** between alignments on these paired rows; the 15-second row is 7 versus 9.

The independent query completed in 17.391 seconds with a 30-second statement limit, exit 0. [Reproduced section-10 results](research/spot_twap_response/projection_pilot_review/section10_results.csv), [execution and comparison manifest](research/spot_twap_response/projection_pilot_review/section10_manifest.json). Original section-7 artifacts were preserved; no all-history section was rerun for this check.

### Carry-forward follow-up: all-market provider-clock comparison

The supplied [part-3 query](research/spot_twap_response/pilot_part3.sql) and [output](results/spot_twap_response/2026-09-13-pilot/pilot_part3_output.txt) compare two reconstructions on the pilot week. For 505,308 observed TWAP timestamps whose underlying retained spot window is incomplete, carry-forward has median absolute residual 0.0032 bp, p99 0.2025 bp and maximum 1.775 bp. Averaging only retained spot rows gives 0.0202, 0.3339 and 2.759 bp respectively. Both methods agree on the 70,944 complete windows. These are observed TWAP timestamps; the query does not establish reconstruction error at every scheduled second with no TWAP report.

This supports adopting carry-forward as the research reconstruction rule. It does not establish why an observation is absent or prove the publisher's exact missing-input policy. The saved run-length table contains 23,971 one-second missing runs and 25 runs of at least ten seconds. Keep long gaps and observation ages visible. The heavy whole-week per-second query was inspected, not rerun on the production droplet.

An independent local check used the already-exported September 11 history, without another database query. On 56,807 incomplete paired windows, carry-forward reduced median absolute error from 0.020777 to 0.002584 bp and p99 from 0.437664 to 0.266187 bp; maxima were 3.554974 and 1.073072 bp respectively. The 27,434 complete windows agreed exactly between methods. No window lacked a usable seed; the longest carry was eight seconds. Carry-forward improved 46,867 incomplete windows and worsened 9,940, so this is an aggregate improvement, not pointwise dominance. The day had already been inspected for alignment and is not an untouched test set. [Local-day summary](research/spot_twap_response/carry_forward_review/local_day_summary.csv), [source, code and output manifest](research/spot_twap_response/carry_forward_review/local_day_manifest.json).

The [per-market part-3b query](research/spot_twap_response/pilot_part3b.sql) uses the corrected alignment, historical carry-forward, current-spot continuation for future slots, and a carried raw-TWAP baseline. An independent execution reproduced all six supplied rows exactly:

| Seconds left | Markets | Projection errors | Raw TWAP errors | Raw spot errors | Projection improves / worsens versus TWAP |
|---:|---:|---:|---:|---:|---:|
| 60 | 2,016 | 149 | 245 | 149 | 135 / 39 |
| 30 | 2,016 | 40 | 124 | 61 | 97 / 13 |
| 15 | 2,016 | 10 | 57 | 85 | 53 / 6 |
| 10 | 2,016 | 1 | 41 | 113 | 40 / 0 |
| 5 | 2,016 | 1 | 20 | 137 | 19 / 0 |
| 3 | 2,016 | 1 | 15 | 139 | 14 / 0 |

At 30 seconds, 1,879 are correct under both methods and 27 wrong under both. The new all-market result replaces the earlier selective-cohort tables as the provider-clock comparison; preserve the earlier outputs as versioned pilot history. Both cohort and missing-input policy changed, so differences between versions cannot be attributed solely to a better forecasting formula.

The query permits source values strictly less than ten minutes old, and excludes rows with no suitable predecessor, no baseline TWAP, or missing historical seeds. **Zero exclusions is the observed result for this week, not a guarantee of the method.** Its rows_using_carried_value field counts only a carried decision-time spot; it does not count every carried constituent or carried TWAP. The independent same-snapshot check found 2,016 resolved markets and no missing opening prices, closing prices or winners. [Independent results](research/spot_twap_response/carry_forward_review/results.csv), [query and execution manifest](research/spot_twap_response/carry_forward_review/manifest.json).

The bounded reproduction used a read-only repeatable-read transaction with a 20-second statement timeout and completed successfully in 10.234 seconds including SSH. The earlier heavy per-second query was not executed. Future reads use this per-market pattern or small bounded extracts; timeout failures must not trigger automatic escalation to a long-running query.

Provider-clock results are a retrospective reference, **not a mathematical ceiling**: additional information does not guarantee that this particular fixed forecast makes fewer errors on every sample. The completed receipt-clock test below uses a different information cutoff and causal opening reference, and reports its own eligible cohort.

## Completed receipt-clock projection pilot

The replay covers all 2,016 scheduled five-minute markets starting in [2026-09-01, 2026-09-08) UTC at six checkpoints, preserving **12,096 rows**. One read-only repeatable-read transaction supplied all rows at snapshot 1789311999309. Three guards and a clock profile preceded 42 sequential per-day/checkpoint extracts, each with a 20-second statement timeout. The export ran from 15:06:32 to 15:09:35 UTC, exited zero, and produced no stderr. No full-week per-second series was constructed.

The input rule was fixed before this run: final source slots E−62 through E−3, historical predecessor received by D, current spot for future slots, and the exact opening stream event received by D. Current spot/TWAP selection uses latest receipt before source-age eligibility; ambiguous simultaneous states remain unavailable. The guards verified that the indexed source ranges include the relevant receipt candidates. Both feeds had zero off-second source stamps in the checked frames. [Extraction code](research/spot_twap_response/receipt_clock_pilot/extract.sql), [guard and execution record](research/spot_twap_response/receipt_clock_pilot/extraction.json), [accepted result manifest](research/spot_twap_response/receipt_clock_pilot/results/manifest.json).

### Forecasting results on identical eligible markets

Every checkpoint has 1,925 primary paired markets, all with verified official outcomes. Exactly **91 markets lack a usable opening stream event by the cutoff**; there are no opening conflicts. Current spot, raw TWAP, projection and market-rule identities are available for all 2,016 markets. The missing opening reference is not replaced with the reconciled strike. There are no prediction ties in the paired panels.

| Seconds left | Paired markets | Projection errors | Raw TWAP errors | Raw spot errors | Projection improves / worsens versus TWAP |
|---:|---:|---:|---:|---:|---:|
| 60 | 1,925 | 156 | 240 | 156 | 131 / 47 |
| 30 | 1,925 | 36 | 125 | 59 | 102 / 13 |
| 15 | 1,925 | 12 | 62 | 77 | 57 / 7 |
| 10 | 1,925 | 5 | 43 | 96 | 42 / 4 |
| 5 | 1,925 | 2 | 23 | 112 | 22 / 1 |
| 3 | 1,925 | 1 | 18 | 129 | 17 / 0 |

At 30 seconds, 1,787 are correct under both projection and TWAP, 102 improve with projection, 13 worsen, and 23 are wrong under both. The net gain is 89 correct calls, or 4.62 percentage points. Projection also makes 23 fewer errors than current spot. At 60 seconds its error count matches current spot, so the improvement over raw TWAP at that horizon does not establish an incremental benefit over the spot baseline.

The prespecified panel requiring current spot and TWAP source/receipt ages within three seconds also preserves the forecasting improvement:

| Seconds left | Fresh paired markets | Projection errors | Raw TWAP errors | Raw spot errors |
|---:|---:|---:|---:|---:|
| 60 | 1,894 | 152 | 233 | 152 |
| 30 | 1,887 | 36 | 123 | 58 |
| 15 | 1,896 | 11 | 61 | 76 |
| 10 | 1,902 | 5 | 43 | 95 |
| 5 | 1,907 | 2 | 22 | 112 |
| 3 | 1,910 | 1 | 17 | 125 |

At 30 seconds this is 98.09% projection accuracy versus 93.48% TWAP accuracy. The primary comparison improves on each of the seven UTC days, but those days already informed development. [Checkpoint summary](research/spot_twap_response/receipt_clock_pilot/results/summary.csv), [daily summaries](research/spot_twap_response/receipt_clock_pilot/results/daily_summary.csv), [all retained decision rows — retained locally; provenance](research/spot_twap_response/receipt_clock_pilot/results/manifest.json).

### What was observed, carried and unavailable

At T30, the 1,925 paired decisions contain 63,525 past target slots: 56,588 exact retained observations and 6,937 historical carried estimates. All paired decisions use some historical carry, including source seconds whose updates had not arrived by D. The remaining 27 slots per decision are explicit current-spot extrapolations. The maximum historical carry age at T30 is 11 seconds; current spot and TWAP source ages reach 10 seconds in the broad panel. The three-second current-value subset does not imply that every historical carried slot is equally fresh.

Historical spot upserts still prevent recovery of every earlier within-second version. The replay therefore measures what can be reconstructed causally from retained evidence, not every original live arrival. None of the 91 unavailable-opening markets is included in the accuracy denominator. Their later official opening price cannot repair decision-time availability.

### Closing boundary and numerical verification

A separate bounded audit checked the scheduled closing stamp E against official final prices. All 2,016 market and resolution identities were valid. A unique exact-E TWAP event was retained for 1,925 markets; 91 lacked it and none had conflicting values. Where comparable, the largest difference from the reconciled final was $0.000000000014051072 (about 1.78 × 10^-12 bp). The agreement strongly supports the fixed scheduled target for this cohort, but it is not exact Decimal/E18 equality. Missing closing events remain unverified by that check. [Closing-boundary audit](research/spot_twap_response/receipt_clock_pilot/closing_boundary_audit_manifest.json).

All 61 focused receipt/precision tests passed. An independent direct CSV/Decimal recalculation, without importing the pilot's assessment or summarization functions, matched every projected price, primary/fresh availability flag, and paired error count across all 12,096 rows and 12 panel/checkpoint groups. It also checked historical receipt cutoffs and slot accounting. [Independent validation](research/spot_twap_response/receipt_clock_pilot/results/independent_validation.json).

## Projected-lead companion grids

Because the 30-second improvement survives the receipt-clock test, the requested companion has been generated. It preserves all 12,096 receipt rows and appends signed projected lead, projected leader, and explicitly labeled projected/raw grid coordinates. It contains 480 grid rows and 24 coverage groups; hashes and N/U/L/n/tie conservation checks pass. N includes unknown outcomes U, resolved n=N−U, and L counts resolved leader losses. No outcomes are unknown in this pilot.

The companion's 19 focused tests pass. A final combined run of the receipt and companion suites passed **80 tests and 57 subtests**. No production/runtime module was changed.

For each reference R (current TWAP or projected close), X = |R−K| / K in basis points and Y = sign(R−K) × (S−R) / K in basis points. X edges remain 1/2/4/8 bp and Y edges −2/0/2 bp. Changing R can change the leader and cell membership; these are separately labeled exploratory grids, not a fixed-cell treatment comparison. Exact ties are counted separately and excluded from each basis's leader grid.

Across all Y bands, the predefined T30 primary X bands contain:

| Lead magnitude X | Raw TWAP leader losses / n | Projected leader losses / n |
|---|---:|---:|
| [0,1) bp | 70 / 285 | 31 / 247 |
| [1,2) bp | 32 / 235 | 4 / 261 |
| [2,4) bp | 18 / 421 | 0 / 393 |
| [4,8) bp | 5 / 482 | 1 / 494 |
| At least 8 bp | 0 / 502 | 0 / 530 |

Thus 31 of 36 projected-leader losses occur below one basis point of projected lead. Zero observed losses in a band do not establish zero future risk. The complete two-dimensional grids, empty cells and fresh-panel comparisons are preserved; no favorable cells were selected as validated trading rules.

[Augmented features — retained locally; provenance](research/spot_twap_response/receipt_clock_pilot/leader_risk_companion/manifest.json), [complete grids](research/spot_twap_response/receipt_clock_pilot/leader_risk_companion/grids.csv), [coverage](research/spot_twap_response/receipt_clock_pilot/leader_risk_companion/coverage.csv), [companion manifest](research/spot_twap_response/receipt_clock_pilot/leader_risk_companion/manifest.json).

The completed checkpoint establishes historical forecasting improvement over raw TWAP, with incremental improvement over current spot from 30 seconds onward in this week. The revised event-response experiment, later untouched evaluation, and quote/fee/execution comparison remain separate unfinished work. No profitable trading strategy is established by these prediction and descriptive risk tables.

## Verified local-visibility delay and part-4 review

The supplied [part-4 SQL](research/spot_twap_response/pilot_part4.sql) and [saved output](results/spot_twap_response/2026-09-13-pilot/pilot_part4_output.txt) provide the simpler measurement adopted in step 3. An independent bounded read-only execution reproduced **all eight section-14 statistics exactly**. It retained the original inclusive September 1 00:00 through September 8 00:00 UTC source bounds, exact s+3 pairing, and integer-millisecond floor of the earliest TWAP receipt. The report should therefore say inclusive bounds; this calculation includes the September 8 boundary spot row.

| Chainlink retained spot receipt → model-aligned TWAP receipt | Result |
|---|---:|
| Matched pairs | 550,393 |
| Median | 3.088 s |
| 10th percentile | 2.552 s |
| 90th percentile | 3.611 s |
| 99th percentile | 4.042 s |
| Minimum / maximum | −32.829 / 10.241 s |
| Negative pairs, retained in the table | 9 |

There are 576,404 retained spot seconds in the inclusive range; **26,011 lack the exact s+3 TWAP report**. Thus the result covers 550,393 retained pairs, not all 604,801 calendar positions. The TWAP query range contains 576,262 events/stamps, with no unexpected instrument/topic/symbol, no conflicting prices and no duplicate report stamps. Spot provider stamps equal their sample-second keys throughout this range. The exact replication used a 20-second statement limit and completed successfully in 25.984 seconds including SSH/session setup. [Query, outputs and verification manifest](research/spot_twap_response/visibility_delay_review/manifest.json).

**The nine negative pairs do not establish clock failures.** Their retained spot receipts occur 4.928–37.258 seconds after their own source stamps; neither feed has a negative receipt-minus-source offset in those nine pairs. In the most negative case, the spot's RTDS envelope follows its source stamp by 1.263 seconds, but its retained receipt is another 35.995 seconds later. The paired TWAP arrives only 1.429 seconds after its own source stamp. This explains the recorded −32.829-second ordering without requiring a clock-error explanation. It does not identify the cause of that delay or reconstruct any overwritten earlier spot version. [All nine clock records](research/spot_twap_response/visibility_delay_review/negative_pairs.csv).

**Method agreement:** use direct pairing for this primary Chainlink timing statistic; jump fitting is unnecessary. Keep the empirical identity and missingness caveats. The selected 771-jump calculation in section 17 has rounded median normalized residuals of zero, with roughly −1.2 to +1.3 jump-unit tails. It is consistent with the sampled alignment but does not demonstrate absence of subsecond splitting. Tight receipt-offset distributions do not establish that spot overwrites are rare. Section 16 measures RTDS envelope/payload/receipt clock differences, without identifying Chainlink's internal calculation or publication completion. Its supplied whole-history distributions were inspected, not independently rerun in this review. [Static code and method audit](research/spot_twap_response/part4_method_review.md).

### Binance pairing reproduces, but does not measure move propagation

An independent bounded read-only execution also reproduced both section-15 rows exactly:

| TWAP provider stamp minus Binance sampling key | Pairs | Median receipt difference | 10th–90th percentiles | 99th percentile |
|---|---:|---:|---:|---:|
| 3 s | 576,222 | 5.707 s | 5.328–6.189 s | 6.673 s |
| 4 s | 576,219 | 6.707 s | 6.328–7.190 s | 7.671 s |

These are valid **sampling-key pairings**, but the labels “same-stamp pickup” and “next-stamp pickup” overstate what the query establishes. The Binance collector sets sample_second_ms from the local sampler clock and copies the previously received ticker's provider event time and receipt time. Chainlink/TWAP rows instead use provider source seconds. Section 15 does not compare a Binance price move with a corresponding Chainlink move. [Binance sampling implementation](price_collector/collector.py).

In the same inclusive week, **603,313 of 604,756 Binance sampling keys differ from the ticker's floored provider-event second**. The retained ticker receipt precedes its row's sampling key by a median **948 ms** (p10/p90 944/955 ms; maximum 9,956 ms). The key follows the provider event by a median 984 ms, whereas ticker receipt follows the provider event by a median 36 ms. There are 3,551 consecutive-second rows reusing an identical cached ticker event, receipt and price. Thus the section-15 construction includes roughly one second of sampler/cache age in the typical row; it cannot identify the actual transfer of a particular Binance move into the TWAP. Do not subtract these separate medians and label the result a corrected propagation delay.

The verification adds the expected topic/symbol restrictions to the TWAP query; the section-14 audit found no unexpected identities in the same fixed source range, and the original section-15 counts and percentiles match exactly. The two bounded statements ran sequentially in one read-only repeatable-read snapshot, each with a 20-second limit; the session completed in 30.641 seconds including SSH/setup. [Binance clock profile](research/spot_twap_response/binance_delay_review/clock_profile.csv), [pairing results](research/spot_twap_response/binance_delay_review/pairing.csv), [query and verification manifest](research/spot_twap_response/binance_delay_review/manifest.json).

**What this closes:** the historical matched Chainlink receipt gap is now measured and independently verified, with a practical median of about 3.1 seconds under the sampled alignment. Exact first-version incorporation and Binance-move-to-TWAP delay remain unresolved. The original normalized step-response question still has its labeled pilot evidence; the revised event definition has not been run. These timing findings do not turn the separate forecast replay into a prospective test or an execution result.

## Verified rolling ghost-price pilot: part 5

The supplied [part-5 query](research/spot_twap_response/pilot_part5.sql) implements the direct-window version of the rolling ghost forecast: reconstruct the target TWAP window from retained Chainlink history and hold current spot constant for future source slots. This is the same forecasting idea already described for rolling targets and used for the settlement projection. Its contribution is an empirical rolling-price comparison with a TWAP-persistence baseline. It does not establish that a different forecasting method is better.

For source anchor w and h in {5,10,30}, target U=w+h has constituent stamps [U−62,U−3]. Under the pilot's assumption that source slots through w are available, there are 63−h reconstructed historical slots and h−3 flat extrapolated slots. This gives 58+2, 53+7 and 33+27 respectively. Historical slots use the latest retained predecessor strictly less than 600 seconds old; a carried slot is an estimate, not an observed unchanged second. The query averages those constituents directly. It does not anchor the forecast on the current official TWAP: W(w) is used only for the persistence and side-change comparisons. An observed-TWAP anchor would preserve the current reconstruction residual and would be a separate variant to test.

An independent execution reproduced every field in the three supplied result rows exactly:

| Source-stamp horizon | Eligible instants | Ghost absolute error, median / p90 / p99 (bp) | Persistence absolute error, median / p90 (bp) |
|---|---:|---:|---:|
| 5 s | 9,199 | 0.006 / 0.058 / 0.194 | 0.155 / 0.561 |
| 10 s | 9,172 | 0.031 / 0.180 / 0.448 | 0.307 / 1.093 |
| 30 s | 9,135 | 0.311 / 1.274 / 3.095 | 0.868 / 3.083 |

| Source-stamp horizon | Endpoint side changes | Changes correctly indicated by ghost | False change indications | Total wrong ghost sides |
|---|---:|---:|---:|---:|
| 5 s | 1,052 | 1,021 | 24 | 55 |
| 10 s | 1,158 | 1,071 | 74 | 161 |
| 30 s | 1,564 | 1,239 | 225 | 550 |

At a hypothetical $80,000 price, 0.006 bp equals $0.048 and 0.194 bp equals $1.552. Thus the five-second median error is very small, but the tail is materially larger. The result is encouraging for this retrospective source-clock task; “the TWAP printed five seconds early” would imply receipt-time validation that has not been performed.

**Coverage and execution.** There are 10,081 candidate minute anchors per horizon, from September 1 00:00 through September 8 00:00 UTC inclusive. Current exact-stamp TWAP is missing at 461 anchors; target exact-stamp TWAP is missing at 462/468/507 anchors for h=5/10/30. These exclusions overlap and must not be added. Their joint available counts are exactly 9,199/9,172/9,135. No target market resolution or opening price is missing in this candidate cohort, and historical predecessor completeness adds no further exclusion among those joint-available rows. The final included anchor produces targets just beyond September 8 midnight. With minute-aligned anchors and horizons no greater than 30 seconds, none of these predictions crosses a five-minute market boundary. The original aggregate ran unchanged, alongside a cheap target-coverage check, in one read-only repeatable-read transaction. Each statement had a 20-second timeout; execution completed successfully in 26.125 seconds including SSH/setup. [Reproduced summary](research/spot_twap_response/ghost_pilot_review/summary.csv), [execution, coverage and hashes](research/spot_twap_response/ghost_pilot_review/attempt1_manifest.json).

**Limits of the live interpretation.** The query has no received_ms or received_wall_ns cutoff. Its w is a generated source-time anchor, not a demonstrated wall-clock instant when W(w) is the latest received report and spot through w is available. Similar marginal receipt delays on the two feeds do not establish their joint availability or that W(w+h) arrives exactly h seconds after W(w). The measured 3.088-second paired gap is likewise not a constant arrival schedule. Rolling live performance therefore cannot be inferred by carrying the quoted error numbers over unchanged.

Forecast error also includes the empirical TWAP reconstruction residual, missing-second estimates and retained-version effects. It is not solely the movement of future spot away from the flat continuation. The side table compares current and future TWAP against a later reconciled opening reference. It counts endpoint side differences, not every crossing within the interval, a tradable signal with a reference known at decision time, or the official settlement winner.

**Connections to the earlier study.** The settlement projection is the same family of window forecast, but the source-stamp horizon from a current TWAP stamp w to settlement stamp C is C−w. This equals seconds remaining E−D only when the target and decision/source clocks coincide as assumed; receipt lag cannot be silently discarded. Likewise, an adverse spot-versus-leader spread does not establish that a particular-horizon ghost has already crossed the opening reference. For example, an otherwise flat $100,000 TWAP window, a $99,900 reference and new spot at $99,950 give an adverse spread while a ten-slot flat-continuation forecast remains above the reference. That relationship needs its own explicit comparison.

**Next-test specification and subsequent result.** Keep the direct-window forecast as the baseline. Freeze each actual decision time and use only spot versions already received; grade the report at an explicitly chosen future source stamp and record elapsed receipt time. A forecast of the latest TWAP visible five or ten wall-clock seconds later is a distinct target and must be graded at that receipt cutoff. Part 6 below now supplies a retained-history version on a wall-clock minute grid, with the stated differences in selection and carry accounting. A prospective live test remains outstanding. Retired shadow-prediction tables and code remain unused.

## Verified receipt-clock rolling ghost pilot: part 6

The supplied [part-6 SQL](research/spot_twap_response/pilot_part6.sql) issues forecasts at wall-clock minute instants tau. Within source range [tau−70 seconds,tau], it selects the last received TWAP event, stamped w. For each target U=w+h, every reconstructed slot selects the greatest spot source stamp at or before that slot whose retained row was received by tau. This one rule carries old observations through silent historical seconds and into unobserved future slots. Forecast targets remain **source-stamp offsets from w**, not fixed wall-clock horizons from tau.

The independent run reproduced every supplied field exactly, with a 20-second statement timeout, exit 0 and elapsed session time 25.016 seconds including SSH/setup. [Reproduced results](research/spot_twap_response/receipt_ghost_review/summary.csv), [query, execution and hashes](research/spot_twap_response/receipt_ghost_review/attempt1_manifest.json).

| Source horizon | Paired forecasts | Ghost absolute error: median / p90 / p99 (bp) | Persistence absolute error: median / p90 (bp) |
|---|---:|---:|---:|
| 5 s | 9,503 | 0.005 / 0.060 / 0.209 | 0.157 / 0.572 |
| 10 s | 9,609 | 0.027 / 0.154 / 0.410 | 0.310 / 1.110 |
| 30 s | 9,548 | 0.292 / 1.243 / 2.783 | 0.879 / 3.110 |

| Source horizon | Target arrival after tau: p10 / median / p90 (s) | Tail extension slots: mean / maximum |
|---|---:|---:|
| 5 s | 3.752 / 4.614 / 5.109 | 1.94 / 9 |
| 10 s | 8.705 / 9.571 / 10.117 | 6.94 / 14 |
| 30 s | 28.634 / 29.611 / 30.158 | 26.94 / 34 |

Arrival quantiles above are rounded for display; the original millisecond output is preserved. The supplied assumed_slots field measures only the number of target slots beyond the latest selected source stamp. **It does not count internal historical carries**, and it can exceed 60 in a sufficiently long stall. The quoted maxima therefore do not describe all estimated inputs. The live plan instead counts every slot as exact, carried, future or missing, with a total of 60.

At an illustrative $80,000 price, the five-second median error of 0.005 bp is $0.040; p90 is $0.480 and p99 is $1.672. “Within a hundredth of a basis point” is not a general error bound. Future spot changes, reconstruction error, stale inputs and retained-version effects can all contribute; feed stalls are an important condition to expose, not the only possible failure mode. The replay does not measure how frequently spot versions were overwritten.

**Coverage and target integrity.** A separate bounded read-only audit checked all 30,243 candidate anchor/horizon pairs. Each of the 10,081 anchors has a baseline under the original selection rule. Exact target reports are missing in 578/472/533 cases, leaving exactly the original paired counts 9,503/9,609/9,548; there is no additional full-slot exclusion among those rows. All paired targets arrived strictly after tau at nanosecond precision. The minimum leads were 1.735933030, 1.732463609 and 20.493746930 seconds. No target duplicates, conflicting target prices, minimum-price-versus-earliest-price mismatch, unexpected target identity, baseline receipt tie or missing target strike was found. The latest selected spot source was never future-dated relative to tau in these audited rows. The audit ran in a separate snapshot, completed in 11.531 seconds including SSH/setup, and reproduced the same horizon cohorts. [Audit manifest and full coverage counts](research/spot_twap_response/receipt_ghost_review/audit_manifest.json).

The original query's independent min(price) and min(receipt) target selection therefore did not combine different report versions in this cohort. A live implementation must still associate a price with its actual event and handle conflicts. Likewise, production current-state selection will apply freshness/clock checks after selecting the latest received state; the historical source-range prefilter is not proof that an excluded newer state never existed. Replaying retained spot rows remains an approximation to every originally observed input version.

**Side-table interpretation.** The quoted side counts also reproduce, but their reference is the target market's later reconciled opening price. Baseline W(w) and target W(U) lie in different five-minute markets in 1,912/1,930/1,915 paired cases. The target market is the decision-time current market in all audited cases. In these boundary cases the baseline comes from before the new market opened; it is compared against that new market's reference. This is not necessarily a side change against one unchanged, decision-time-known contract reference.

| Source horizon | All endpoint side changes | Changes with baseline/target in different markets | Changes within the same market |
|---|---:|---:|---:|
| 5 s | 1,931 | 1,800 | 131 |
| 10 s | 1,976 | 1,731 | 245 |
| 30 s | 2,218 | 1,526 | 692 |

The original correctly indicated-change / false-alarm / total-wrong counts are 1,893/41/79, 1,933/111/154 and 1,954/334/598. They are valid descriptions of this grading rule, but must not be presented as causal settlement signals. A local Decimal re-tabulation of the exported audit reproduced all endpoint-change totals and the boundary split without another database query. [Boundary-side summary and source hash](research/spot_twap_response/receipt_ghost_review/boundary_side_summary.json). The first live version will expose price forecasts without strike or side calls.

**Settlement status and remaining work.** The receipt-clock settlement replay with a causal opening stream reference is already complete: at 30 seconds before close, the projection has 36 errors versus 125 for raw TWAP and 59 for spot on 1,925 matched markets. [Completed receipt-clock projection](research/spot_twap_response/receipt_clock_pilot/README.md). A settlement forecast targets the scheduled closing stamp C directly; from baseline source w its horizon is C−w, which need not equal wall-clock seconds remaining E−tau. The target market's strike and the ending contract's strike must also be distinguished at an exact boundary.

The historical ghost results support moving to a planned live shadow implementation. Prospective usefulness, complete revision capture, actual publication-before-target timing, resource limits and recovery remain to be demonstrated there. The [live implementation plan](GHOST_TWAP_LIVE_PLAN.md) defines three reviewable checkpoints. That research review added verification and a plan only; the later Checkpoint A implementation is recorded below.

## Plan refinement: short horizons, audit size and arrival estimates

The revised live plan now includes source horizons **1, 2, 3, 5, 10 and 30 seconds**, one compact decision audit table and an explicit arrival-time estimate. It preserves the same three implementation checkpoints. These are planned changes, not a completed implementation or a new prospective result.

**Short horizons are often based on already received inputs, but are not guaranteed assumption-free.** An offline check reused all 10,081 unique decision anchors from the existing receipt-clock export. For h=1/2/3, the tail beyond the newest selected source observation still requires continuation at 2/5/173 anchors, with maxima of 5/6/7 tail slots. These counts omit internal historical carries and do not check every older short-horizon slot, so zero tail does not establish a fully observed window. The export has actual target prices and receipt clocks only for h=5/10/30; no h=1/2/3 accuracy or arrival distribution was measured by this check. The suggested 0.003 bp short-horizon error is therefore not a verified prediction result or a health threshold. [Offline diagnostic, code and source hashes](research/spot_twap_response/short_horizon_review/summary.json).

The same check found U=w+h <= D at all 10,081 anchors for h=1 and h=2, and at 2,748 anchors for h=3. **The earlier plan's blanket U<=D rejection was incorrect for advance visibility.** A source-stamped report can remain unreceived after its source time has passed. The revised plan rejects targets already received and measures whether the forecast was actually published before the target, rather than equating a past source timestamp with a known target. It also replaces a sole one-second refresh timer with bounded event-triggered updates so the publication cadence does not consume the short lead.

**Arrival estimate.** The proposed baseline is anchor receipt plus h seconds. Remaining time at decision D is h−(D−anchor receipt), with the appropriate units. This assumes equal source-to-receipt delay for anchor and target. The replay's approximate h−0.4-second median reflects its minute-grid anchor ages and is not a universal adjustment. ETA error and publication latency will be measured during the prospective run; an overdue estimate stays labeled overdue and is not moved forward to create artificial lead.

**Audit simplification.** One decision table can reproduce the forecasts if the frozen row includes the selected slot values, source/receipt clocks, input/revision identities, categories, carry ages, snapshot/configuration identity, and publication evidence. The six horizons may share overlapping slot records inside that row. Target/result fields may be filled idempotently later without changing frozen inputs or replacing the first target with a more favorable revision. This avoids a second full input ledger in v1. It records the worker's actual snapshot; it cannot independently reconstruct all unused arrivals or prove complete selection. The existing upserted spot table alone cannot recover a prior selected revision, regardless of an unmeasured assumption that overwrites are rare.

**Peer's within-market correction.** The supplied [part-6b query](research/spot_twap_response/pilot_part6b.sql) and [output](results/spot_twap_response/2026-09-13-pilot/pilot_part6b_output.txt) report the following same-market subset:

| Source horizon | Same-market instants | Endpoint side changes | Changes indicated by ghost | False alarms |
|---|---:|---:|---:|---:|
| 5 s | 7,591 | 131 | 121 | 4 |
| 10 s | 7,679 | 245 | 225 | 12 |
| 30 s | 7,633 | 692 | 497 | 91 |

The same-market and cross-market denominators and actual endpoint-change counts agree exactly with our earlier independent boundary audit. The ghost-specific subset detections, false alarms and errors above are supplied results: their SQL was inspected, but these fields were not independently recalculated in this refinement. They still use a reconciled strike without testing its decision-time availability. The revision therefore does not change the decision to omit side calls from the first live version. No database query or retired-pipeline inspection was performed for this plan refinement.

## API delivery baseline and live-plan revision

The owner confirmed a frontend on their computer through an SSH tunnel. Two bounded read-only measurements of the existing `/markets/current/live` endpoint, each with a warmup and a reused HTTP connection, found:

| Measured path | Requests | Median complete response | Tail |
|---|---:|---:|---:|
| Droplet loopback | 50 at 10/second | 2.481 ms | p99 5.748 ms |
| This computer through a temporary SSH tunnel | 10 sequential | 578.060 ms | p90 594.858 ms |

All measured requests returned HTTP 200. The tunnel was closed and cleanup verified. These are current endpoint request/response durations, not ghost or SSE measurements, one-way transport estimates, browser freshness or CPU benchmarks. The supplied 2–3 ms future endpoint estimate, 3% CPU claim and 150–400 ms update-delivery estimate remain unverified; request round-trip time and update freshness are distinct. [Methods, raw timings and hashed evidence](research/spot_twap_response/api_latency_review/README.md).

The [live plan](GHOST_TWAP_LIVE_PLAN.md) uses SSE push as the primary frontend path, with a one-key snapshot GET fallback. It removes the mandatory 100 ms publication interval in favor of immediate publication when idle and bounded burst coalescing. It adds serialized snapshot/notification delivery, reconnect resync, slow-client isolation, independent expiry and actual browser-lead measurements. The planned API stays read-only, loopback-bound and accessible through SSH. Implementation had not started at that benchmark review; the later Checkpoint A result is recorded below.

## Implementation-readiness and retention review

**Connection reuse was confirmed a second time.** A fresh diagnostic counted exactly one HTTP connection call and one socket/local port for the warmup and all ten measured requests. The median full response was 597.433 ms, with 597.127 ms to headers and 0.047 ms from headers through the body. Five separate direct TCP-connect probes had a median of 362.234 ms. Repeated connection establishment does not explain the earlier 578 ms result; the extra transport time remains unlocalized. Neither 300 ms persistent HTTP nor 150 ms SSE delivery is established. [Recheck traces and explanation](research/spot_twap_response/api_latency_review/README.md).

Equal ghost and official-report delivery delays would cancel in their relative browser-visible lead. Equality has not been measured, and a source-stamp horizon is not exactly the remaining receipt lead. Actual matched browser arrivals remain the acceptance metric. This research does not measure an execution advantage over other participants or justify moving the droplet.

**Audit capacity is a real design constraint.** A read-only check found 12,080,061,463 database bytes and 26,551,746,560 available bytes on its filesystem (11.25 and 24.73 GiB). Assuming 3,000 bytes per decision and two decisions/second gives 0.5184 GB/day before additional costs; the proposed row's physical size has not been measured. The six horizons share 89 distinct slot timestamps. One detailed row per second would omit some more-frequent publications, and unlimited compact history would still grow indefinitely. [Capacity and storage review](research/spot_twap_response/storage_review/README.md).

The first-canary plan now specifies at most 72 hours, 600,000 complete decision rows, a 1 GiB warning, a 1.5 GiB publication-stop threshold within a 2 GiB total-relation budget, and a 10 GiB filesystem reserve. Whole rows become eligible for expiry after 96 hours only when matching is terminal and an external export is verified. Every publication retains full evidence; there is no permanent compact tier. Actual row/update overhead, queued-write margin, export, maintenance and expiry must be tested before enabling the worker. These chosen bounds do not prove that a full 72 hours will fit; an early guard stop is incomplete validation.

**The readiness review specified Checkpoint A for implementation.** Its acceptance includes a hashed recorded-hour fixture with warm-up and follow-through, exact Decimal agreement on identical selected inputs, and explicit coverage differences under the live engine's stricter policies. The official-TWAP identity residual is a separate empirical error measure, not an implementation tolerance. Retained/upserted historical rows cannot establish original first-arrival completeness. Prospective live accuracy, delivery, resource use and recovery are still to be validated.

## Checkpoint A implementation and exact replay

The default-off [pure engine](price_collector/ghost_twap.py) is now implemented locally, with no collector/API import, network access, schema change or deployment. It calculates all six horizons from frozen 89-slot shared snapshots, uses Decimal context precision 80 with final E18 half-even rounding, preserves signed ETA, and validates receipt/source freshness, ordering, gaps, missingness and unseen targets. Nanosecond JSON clocks are strings to avoid JavaScript integer precision loss. The [checkpoint contract](GHOST_TWAP_CHECKPOINT_A.md) documents the strict receipt-time admission barrier required of B.

The deterministic fixture selects September 11 01:00–02:00 UTC before examining quality/errors, with two-minute context on each side, from an existing local export. It contains 7,505 retained events and 3,600 decisions. **All 21,600 forecast rows match the independent reference exactly**, including prices, selected input identities, sequence, target, all four slot counts, quality/reasons and signed ETA. Each horizon has 3,585 available and 15 unavailable decisions; unavailable rows remain in the comparison. [Fixture provenance](tests/fixtures/ghost_twap/README.md).

The fixture's ordering and monotonic clock are explicitly synthetic, and spot inputs are retained upserts. This is an implementation parity test, not proof of original first arrivals or a new prospective forecast-accuracy result. Synthetic/adversarial tests additionally cover behaviors absent from that hour, including clock faults, revisions, source regression/conflicts, sequence loss and bounded-history recovery.

The initial Checkpoint A repository suite passed **923 tests**, including 42 ghost-engine/fixture tests. The later contract-2 label refinement and updated validation are recorded below. Original leader-risk findings remain unchanged. [Current validation record and hashes](research/spot_twap_response/checkpoint_a_validation.json). B and C remain unimplemented; no live ghost is published yet.

## Checkpoint A review: pending inputs and useful quality labels

The peer correctly identified that the original available 5/10/30-second forecasts were all labeled degraded in the recorded hour: 0 healthy, 3,585 degraded and 15 unavailable at each horizon. Recent source slots awaiting delivery were mixed with interior absent slots in one carried count. This obscured whether a forecast had interior missing history; it did not change its arithmetic.

Payload **contract 2** adds `pending`. Historical input selection is unchanged. After selecting a usable value for a slot at or before D, an exact stamp is observed; a nonexact stamp beyond the greatest admissible received spot source is pending; an interior absent stamp is carried. This maximum is not necessarily the latest-receipt input's source during source disorder. Pending keeps the same selected historical price. Future slots, missingness, current freshness checks and all other guards remain unchanged. The five counts sum to 60.

Pending and future slots remain assumptions even when quality is healthy. Pending does not promise a later report, and an interior absent stamp does not prove genuine producer silence. Available forecasts are degraded when they contain interior carried slots; any unavailable reason still takes priority.

| Horizon | Original healthy / degraded | Contract-2 healthy / degraded | Unavailable, both |
|---|---:|---:|---:|
| 1 s | 1,322 / 2,263 | 1,322 / 2,263 | 15 |
| 2 s | 1,320 / 2,265 | 1,320 / 2,265 | 15 |
| 3 s | 1,335 / 2,250 | 1,340 / 2,245 | 15 |
| 5 s | 0 / 3,585 | 1,383 / 2,202 | 15 |
| 10 s | 0 / 3,585 | 1,496 / 2,089 | 15 |
| 30 s | 0 / 3,585 | 2,066 / 1,519 | 15 |

The original generator and fixture remain unchanged. A separately generated [contract-2 fixture](tests/fixtures/ghost_twap/pending_v2/README.md) preserves identical event/decision files and all **21,600 prices, availability decisions, current event identities, sequences, targets and signed ETAs**. In every row, original carried equals new carried plus pending. [Exhaustive comparison and hashes](research/spot_twap_response/checkpoint_a_replay/pending_review.json). The updated engine matches all new expected rows exactly.

The full suite passes **934 tests** in the repository's Python 3.9.5 environment. All **53 ghost tests** also pass under local Python 3.12.0. Added cases cover source disorder, delayed arrivals, unobserved interior seconds, staleness, immutable prior snapshots, missing seeds and horizon-specific recovery. The peer's separate 3,090-case implementation was unavailable at the initial label review; its later supplied version is verified below.

A sequence loss conservatively clears both histories, but recovery is not a fixed 62-second wait. With dense new source coverage beginning at s0, each horizon needs w+h−62 >= s0, plus fresh valid current feeds. This means source-anchor advances of 61/60/59/57/52/32 seconds for h=1/2/3/5/10/30; actual wall duration depends on receipt timing and other guards. TWAP-only gaps preserve spot history. B's plan now requires per-horizon recovery/outage measurements, reset causes, input-drop counts and queue-pressure testing. This update changes labels and validation only; B/C and production deployment remain unstarted.

## Third implementation supplied and verified

The supplied [third implementation](research/spot_twap_response/checkpoint_a_replay/third_implementation_check.py) was inspected and run locally under Python 3.12. It reports **3,090 sampled forecasts with zero price/count/quality mismatches against either expected results or the engine**, plus zero v1/v2 price/availability/arrival-estimate differences and zero carried-split mismatches over all 21,600 rows. Its mismatch counters are explicitly asserted by the wrapper; a zero script exit alone would not establish a pass. [Accepted manifest](research/spot_twap_response/checkpoint_a_pending_review/third_review_manifest.json), [scope and limitations](research/spot_twap_response/checkpoint_a_pending_review/third_review_README.md).

The h=5 maximum interior-carry distribution also reproduces exactly: no carry in 1,383 available forecasts, one second in 2,031, two seconds in 57 and six seconds in 114. Thus 3,471/3,585 would meet a carry-at-most-three-seconds label. Those 114 forecasts are overlapping windows, not a count of distinct incidents; these data alone do not establish the cause of the missing inputs. The aggregate reconstruction residual does not prove that all one-second interior absences have no accuracy cost. A retains its accepted mapping; a more tolerant quality threshold is an optional B policy, with unchanged financial calculations and explicit versioning/tests.
