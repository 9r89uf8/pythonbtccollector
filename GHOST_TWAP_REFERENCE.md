# Ghost TWAP: findings, calculation and operations

This is the canonical reference for the leader-risk and spot-to-TWAP research
and the live ghost endpoint. It replaces dated H3/ghost plans, checkpoint reports
and canary narratives in the working tree. It records evidence through September
16, 2026; historical measurements below are not claims about today's accuracy.
Current installation commands remain in the [production operations guide](README.md#production-operations).

## What was established

The received Chainlink spot history closely reconstructs the official
60-second TWAP with a three-second source-stamp alignment. Using that history
and holding unobserved future spot constant predicts short-horizon TWAP much
better than holding the last TWAP constant in the measured cohorts. Independent
historical replays and subsequent live canaries support that finding.

This is a forecast of a future official TWAP report. It is not itself the
official settlement price, an exact identity at every second, a guaranteed
error bound, or evidence of a profitable trading strategy. Settlement-side
accuracy is a separate experiment. The earlier leader-risk study is consolidated
in [Historical leader-risk findings](#historical-leader-risk-findings) below.

### Reconstruction and response

With timestamps expressed in seconds, the supported reconstruction is:

```text
W_hat(u) = mean(S(u - 62), ..., S(u - 3))       # 60 one-second slots
```

`W(u)` is the official TWAP source stamp. `S` is retained Chainlink spot,
carried forward through a missing source second. This is an empirical
reconstruction of public retained inputs, not proof of the publisher's exact
private constituent reports. A missing collector row does not prove unchanged
upstream price.

The original two-second alignment was corrected to **three seconds**. Shift
profiles, a direct 60-constituent recomputation and daily tables support that
correction. The all-history complete-window follow-up had 301,334 windows,
median absolute residual rounded to 0.0000 bp, p99 0.1686 bp and maximum
1.427 bp. That supplied all-history output was inspected, not independently
rerun here; an independent September 11 extraction reproduced the alignment.
Those days informed development and are not a held-out evaluation set.

For the pilot week's 505,308 observed TWAP stamps with incomplete retained spot
windows, carry-forward gave median/p99 residual **0.0032/0.2025 bp**, versus
**0.0202/0.3339 bp** when averaging only retained rows. An independent local-day
check reproduced the aggregate improvement. Neither method dominates on every
timestamp, and observed-stamp coverage excludes seconds with no TWAP report.

The retained spot receipt to the first TWAP report predicted to contain its
slot was measured directly: pair spot stamp `s` with TWAP stamp `s + 3`, then
subtract their local receipts. Across **550,393 pairs**, median delay was
**3.088 seconds (about 3.1 seconds)**, p10/p90 **2.552/3.611 seconds**, and p99
**4.042 seconds**. Of 576,404 retained spot seconds, 26,011 lacked that exact
target report. Nine negative-delay pairs remain clock anomalies. The statistic
uses retained same-second spot versions, so exact original first-arrival
causality and the upstream calculation/publication split remain unmeasured.

At that first included slot, a clean permanent step contributes **1/60 = 1.67%**
of its size, not 5%. Three changed slots contribute 5%. With flat prehistory,
unchanged new spot and constant delivery, that would be approximately 1.67% at
3.1 seconds, 3.33% at 4.1 seconds and 5% at 5.1 seconds after the spot receipt.
This is an ideal illustration, not a measured receipt-clock response curve.

The saved exploratory event query selected **190** sustained moves of at least
2 bp over two source seconds. It reported these mean normalized TWAP changes:

| Seconds after detected move, provider clock | 5 | 10 | 30 | 60 |
| --- | ---: | ---: | ---: | ---: |
| Share of spot move appearing in TWAP | 5.0% | 13.1% | 49.5% | 103.6% |

The query normalizes from `W(t-1)` by `S(t)-S(t-2)` and hindsight-selects
persistent moves. It has no explicit cooldown or flat-prehistory filter.
Leaving prices, prior trends and later spot movement affect this curve; values
above 100% are possible. Its SQL was inspected, but the revised response
protocol was not executed. Do not add the 3.088-second receipt delay to these
source-clock horizons or call this table a causal isolated step response.

### Historical forecast results

The fixed September 1–8, 2026 development-week rolling ghost pilots sampled
once a minute. Errors are absolute error relative to actual target price in
basis points; **these are medians/p90, not MAE**. Missing exact target stamps
were excluded and counted. The receipt replay used only retained values whose
receipt was at or before the decision; overwritten same-second spot revisions
still limit historical availability reconstruction.

| Clock / horizon | Matched pairs | Ghost median / p90, bp | Held-TWAP median / p90, bp | Median target arrival after decision |
| --- | ---: | ---: | ---: | ---: |
| Source / 5 s | 9,199 | 0.006 / 0.058 | 0.155 / 0.561 | Not measured |
| Source / 10 s | 9,172 | 0.031 / 0.180 | 0.307 / 1.093 | Not measured |
| Source / 30 s | 9,135 | 0.311 / 1.274 | 0.868 / 3.083 | Not measured |
| Receipt / 5 s | 9,503 | 0.005 / 0.060 | 0.157 / 0.572 | 4.614 s |
| Receipt / 10 s | 9,609 | 0.027 / 0.154 | 0.310 / 1.110 | 9.571 s |
| Receipt / 30 s | 9,548 | 0.292 / 1.243 | 0.879 / 3.110 | 29.611 s |

The receipt replay's apparent 98% short-horizon side-change capture was mostly
market-boundary artifacts involving a changed strike. Restricting to genuine
within-market endpoint side changes gave 121/131 at 5 seconds, 225/245 at
10 seconds and 497/692 at 30 seconds, with 4/12/91 false alarms. These are
future-TWAP side comparisons, not necessarily settlement winners or trading
signals. Error includes reconstruction and carry uncertainty as well as future
spot movement; it is not only the latter.

A separate settlement replay used a receipt-time cutoff and an opening stream
reference available then, rather than substituting the reconciled opening price.
Of 2,016 markets, **91 lacked a usable causal opening event**; 1,925 remained at
every checkpoint:

| Seconds before close | Projected settlement wrong | Current TWAP wrong | Current spot wrong |
| ---: | ---: | ---: | ---: |
| 60 | 156 | 240 | 156 |
| 30 | 36 | 125 | 59 |
| 15 | 12 | 62 | 77 |
| 10 | 5 | 43 | 96 |
| 5 | 2 | 23 | 112 |
| 3 | 1 | 18 | 129 |

At 30 seconds that is 98.13% correct versus 93.51% for current TWAP. This was
the already studied week, with retained-input limitations and explicit carry;
it is not untouched forward validation. A settlement target is the scheduled
closing source stamp, whose distance from the current TWAP source stamp need
not equal wall-clock seconds remaining.

### Prospective live measurements

The table keeps distinct runs and measurement clocks separate. Published
accuracy pairs require the exact acknowledged eligible forecast and a clean
first report at its exact target stamp; the unchanged-TWAP baseline uses the
same pairs. Lead is confirmed only when acknowledgement strictly precedes
target receipt. Dollars below are **median absolute error**, not chart MAE.

| Run | Decisions | 5 / 10 / 30 s median dollar error | 5 / 10 / 30 s median lead |
| --- | ---: | --- | --- |
| September 14, 00:45:20–01:45:20 UTC | 7,292 | $0.057 / $0.352 / $3.239 | 4.785 / 9.771 / 29.772 s, Redis |
| September 14, 22:34:04–23:34:04 UTC | 7,054 | about $0.026 / $0.173 / $2.04 | 4.662 / 9.658 / 29.657 s, Redis |
| September 15, bounded browser observation | 1,983 audit decisions | $0.02716 / $0.20230 / $2.30141 | 4.717 / 9.726 / 29.732 s, browser |
| September 15, 22:56:38–23:56:38 UTC | 7,082 | $0.0300 / $0.2684 / $2.9955 | 4.650 / 9.668 / 29.654 s, Redis |

The first hour beat unchanged TWAP in 97.6%/93.4%/80.5% of its published pairs.
Its original 3-second source-age bound excluded many late-feed moments. The
next run used 5-second source age, 3-second receipt age and per-horizon
publication masking. After 65 seconds, sampled key presence was **99.632%**,
and usable 5/10/30-second coverage **98.003%/98.156%/98.727%**, across 35,350
planned 100-ms observation bins. It preserved 314 partial batches. A reconnect
cleared history and caused approximately 59 seconds of five-second forecast
unavailability; bounded reconnect retention was implemented afterwards.

The browser experiment admitted forecasts for 900 seconds and collected targets
for another 120. Browser lead came from both messages' handler-entry
`performance.now()` values in that browser, not audit timestamps or screen paint.
It matched 1,574/1,581/1,617 targets at 5/10/30 seconds. Its 93.62%–95.59%
usable fractions included producer startup because the browser began 22 seconds
earlier. Eleven aborted snapshot requests were observed; a keep-alive timing
explanation was plausible, not established. The stream had no observed error.
Shutdown exposed a nested drain timeout; reviewed recovery preserved frozen
inputs, acknowledged publications and observed targets, leaving unknown tails
unmatched. The shutdown budgets were fixed afterwards.

The final reliability hour used a complete 36,000-bin server observer. After
65 seconds its usable cache coverage was **99.567%** at all three horizons.
The accuracy comparison above excludes decisions in the final 120 seconds to
give them a complete matching window; it has 6,650/6,658/6,695 pairs. The new
shutdown drained **223 tail records in 6.829 seconds** without intervention.
The owner's laptop slept and missed approximately 49 minutes of browser data:
full-hour browser reliability was **not measured**. Short server cache gaps
aligned with received-feed pauses and expiry; missing browser observations
cannot be filled with server observations.

All four campaigns' **23,411** audit records were terminal and externally
verified after the final run. The final export SHA-256 was
`f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954`.
Receipt-to-Redis publication remained approximately **38–39 ms median**, with
about 106–113 ms p99: the under-10-ms objective was not met. Full evidence is
durable before publication; stage medians cannot identify fsync's sole cost.
Accuracy and coverage vary with market conditions and input delivery. These
short runs do not establish uninterrupted long-term availability or trading edge.

## Historical leader-risk findings

The H3 study completed a descriptive settlement-risk analysis, a quote-price
comparison and an independently verified gross payout-minus-ask audit. It did
**not** complete prospective risk validation, an execution simulation, a
net-profitability test or an incremental-information test against market prices.
The optional forward validation plan was never run and is now a historical
design, not an active instruction.

An earlier proposal, previously stored under the misleading root filename
`MICROSTRUCTURE_API.md`, reserved three development days, seven untouched
evaluation days and a fixed 24-hour outcome-reporting cutoff. It proposed
freezing at most three T/X/Y conditions (one per checkpoint), evaluating every
qualifying market separately for each condition, and keeping first qualification
as a separate secondary summary. Descriptive losses were L/(N−U); support for a
1% risk target would use L+U possible losses out of N with one-sided exact
Clopper–Pearson bounds and error allowance 0.05/m for m frozen claims. The
zero-loss benchmarks of 299 observations for one claim or 408 for three relied
on independent, comparable markets. The later archived plan narrowed to one
condition. Neither design was executed; these proposed settings were not the
validation procedure for the retrospective results below.

The requested market-start interval was **August 16, 2026 00:00 UTC through
September 12 21:15 UTC, end exclusive**, for the validated 60-second TWAP
five-minute instrument. There were **8,014 markets / 64,112 checkpoint rows**,
of which **59,885** had eligible settlement inputs and **4,227** did not; no
available-input TWAP ties occurred. Seventeen initial calendar slots lacked
stored market metadata. The outcome snapshot was September 12 at
22:17:21.776 UTC; quote extraction was September 13 at 00:40:38.993 UTC.
Later resolved outcomes were not silently substituted into the frozen cohort.

At market end minus T seconds, K was the exact opening TWAP event received by
then, W the latest received official TWAP, and S retained Chainlink spot.
Source and local receipt ages for W/S were each within **[0,3,000] ms**.
The leader was Up for W>K and Down for W<K. Missing/late/conflicting opening
events were excluded; this stream-derived opening reference was not a backfilled
website quote. For leader direction d=+1/-1:

```text
X = 10,000 * d * (W - K) / K       # TWAP lead, basis points
Y = 10,000 * d * (S - W) / K       # spot confirmation, same denominator
```

Confirming spot means Y≥0. Negative Y does not necessarily put spot across K.
Fixed X bins were [0,1), [1,2), [2,4), [4,8), [8,infinity); Y bins were
below −2, [−2,0), [0,2), at least 2 bp. Unknown official outcomes were counted
separately and never treated as wins. Prices and calculations remained Decimal.

| Seconds remaining | Known losses / resolved eligible | Loss rate | Unknown outcomes |
| ---: | ---: | ---: | ---: |
| 120 | 1,568 / 7,471 | 20.99% | 27 |
| 90 | 1,266 / 7,447 | 17.00% | 26 |
| 60 | 904 / 7,460 | 12.12% | 27 |
| 30 | 478 / 7,444 | 6.42% | 27 |
| 15 | 282 / 7,462 | 3.78% | 27 |
| 10 | 194 / 7,451 | 2.60% | 26 |
| 5 | 110 / 7,466 | 1.47% | 27 |
| 3 | 84 / 7,471 | 1.12% | 26 |

Time, lead and spot alignment separated substantially different historical risks.
At T=60 and X=[2,4), loss rates were **63.53%, 10.73%, 4.28% and 0.93%** across
the four Y bins. At T=30 with X≥2 and Y≥0 there were **0/3,119** known losses
and 13 unknowns; this overlapping, retrospectively summarized region is not a
zero-risk rule. At T=3, **81 of 84** losses had X<1 bp, all 84 had X<2, and
spot trailed TWAP in 79. The X<1,Y<−2 cell lost **32/74 (43.24%)**. The same
markets appear at eight checkpoints, so these are not independent trade samples.
Direction and date comparisons were descriptive, not held-out validation.

The quote companion retained **53,361/59,885 (89.11%)** checkpoint observations.
It selected the latest retained row causally, then required correct tokens,
leader bid/ask in [0,1], no crossing, and ≤3-second sample, row and independent
component ages. An invalid newer row was not replaced by an older fresh row.
Boundary asks of $0/$1 were retained observations, not demonstrated fills.

In the X≥2,Y≥0 region, the T=30 quotes were **$1 for 2,946 of 2,961 observations**;
only **three** asks were below $0.98, all resolved winners. At T=60 the matched
region lost **34/2,816**, but the asks below $0.98 lost **24/265 (9.06%)**.
A cheap subset cannot inherit the full region's lower loss rate.

For resolved matched observations, the audit calculated hypothetical gross
one-share payout minus recorded ask, `1{leader won} - ask`. It is neither future
expected value nor a measured trade return. Pooled checkpoint means ranged from
**−0.5977 to +0.0277 cents/share**, but not every price band was near zero:
T=30 asks in (0,0.90), n=532, averaged **−2.3664 cents/share**; T=90 asks in
[0.90,0.95), n=705, averaged **−1.5901 cents/share**. Across **133** cell/band
groups with ≥100 resolved observations, **16** gross means were positive and
**one** transformed Wilson diagnostic was positive. That selected cell averaged
+7.7806 cents/share at T=60,X=[0,1),Y=[0,2),ask in (0,0.90), n=196. A Wilson
bound for a binomial loss proportion minus a sampled mean ask is not itself a
validated bound for future paired returns; dependence and multiple selection
were not resolved. Neither an edge nor "necessarily chance" was established.

Quote matching also selected a different risk population:

| Seconds remaining | Quote-matched loss rate | Quote-excluded loss rate |
| ---: | ---: | ---: |
| 60 | 633/6,250 = 10.13% | 271/1,210 = 22.40% |
| 30 | 359/6,758 = 5.31% | 119/686 = 17.35% |
| 10 | 142/6,980 = 2.03% | 52/471 = 11.04% |
| 3 | 61/7,074 = 0.86% | 23/397 = 5.79% |

These exclusions are within settlement-input-eligible checkpoints, not the
entire calendar population. The large stale-bid buckets also had stale asks:
there were **zero fresh-ask/unusable-bid cases at T=60 or T=30**, and one in the
whole dataset. This is a quote-availability association, not evidence of an
isolated stale-bid predictor or proof no executable offer existed.

Independent arithmetic reproduced all 8 checkpoint totals, 48 checkpoint bands,
960 cell bands and exclusion/status counts. The historical software checks were
881 full-suite and 41 focused audit passes; those are accounting checks, not
prospective validation. Full grids, exact manifests and the original report are
available in Git at `0a1f998:H3_TWAP_LEADER_RISK_FINAL_REPORT.md` and
`0a1f998:results/leader_risk/`. Frozen input SHA-256 values were
`8f2141b4e09a415e47375782ddefafcfd41b64d8062b96e7e11ebc35eba0fc77`
(settlement observations) and
`ef9999ce3ca2d4f0ec1dd73adc2cae9f6d35c906e83879d541429cd7b6dac964`
(quote observations). Original raw inputs were local, not in Git; after requested
cleanup, a new date-filtered query cannot recreate their old outcome snapshot.
The five source files under `research/leader_risk/` remain because active tests
import/read them; they are not production service imports.

The September 10 readiness work added timestamped pre-close official opening
references, fee/rule snapshots, session gaps and 100-ms paired quotes in the
last 120 seconds. It did not reconstruct older missing evidence or measure order
fills. Depth/quantities were excluded by owner choice. A profitable leader-buying
strategy remains **unestablished**; these findings do not prove universal market
efficiency, no incremental X/Y/T value, or that speed is the only possible edge.
Net returns require applicable historical fees and declared execution assumptions.

## How the current producer works

The pure engine is [ghost_twap.py](price_collector/ghost_twap.py). The optional
worker in the existing Chainlink collector uses the accepted canonical spot and
60-second TWAP feeds; it adds no feed connection and changes no official price.
Continuous production was activated **September 16 at 15:59:27 UTC**, run
`4f55736e031f4184be501c538f7a61d2`, initially at commit `6c6f5bf`. Current code
retains calculation contract 4 and runtime `ghost-continuous-v1`.

At decision D, let the latest received official TWAP have source stamp w. For
each horizon **1, 2, 3, 5, 10 or 30 seconds**, target U is w+h, and the engine
averages the 60 slots U−62 through U−3. Historical slots select the latest
admissible received predecessor at or before the slot; a later source revision
not received by D cannot enter. Future slots use current received Chainlink spot.
The official TWAP is the comparison baseline and target-stamp anchor; the
forecast is the direct slot average, not the official value plus an increment.

With ideal instantaneous observations through w, horizons 5/10/30 have 2/7/27
future assumed slots. Live pending/carry counts can differ. Each snapshot records
60 slots partitioned into **observed, carried, pending, future and missing**:

- Observed: an exact admissible received source stamp.
- Carried: a nonexact interior historical stamp, within the received source range.
- Pending: a nonexact historical stamp beyond the newest admissible received
  source stamp, but no later than D. It retains historical predecessor selection.
- Future: a stamp after D, filled with current spot.
- Missing: no permitted input or carry; the forecast is unavailable.

Historical carry is bounded at 10 seconds. Any interior carry makes an available
forecast `degraded`; pending/future assumptions remain separately visible.
Healthy does not mean certain or assumption-free. Available 3-second forecasts
have no future-stamped slots, but may have pending/carried inputs and residuals;
that chart is an alignment check, not an identical copy of the future TWAP.

Current spot and TWAP must meet inclusive source age ≤5,000 ms and receipt age
≤3,000 ms on both wall and monotonic clocks. Future-stamped/invalid inputs fail
closed. The earliest of all six input deadlines controls expiry. Re-snapshotting
unchanged inputs cannot renew their lifetime. Decimal precision is 80 digits,
with final 18-place half-even rounding and decimal-string serialization.

Every received update can refine a future target. Full frozen inputs and intent
are fsynced to the bounded outbox before Redis publication. Eligibility is checked
again after that write: a horizon whose target has already arrived is masked,
while eligible siblings keep their calculated prices. Audit evidence records the
actual attempted bytes and acknowledgement. Uncertain crash outcomes remain
unconfirmed. No lead credit is assigned to acknowledgement ties or late arrivals.

Estimated arrival is anchor receipt plus h; it is labelled an estimate and can
be overdue. Horizon h advances a **source timestamp**, not a promise of h seconds
remaining at the frontend. Source, publisher, local receipt, Redis acknowledgement
and browser clocks must not be conflated.

A clean spot connection end can preserve history if its last value was fresh
and newest, and the first returning event is fresh, advances the stamp and
bridges at most 10 seconds on source, wall and monotonic clocks. Availability
pauses until that qualification; bridged slots remain carried. Other gaps,
sequence loss, long silence or an invalid first event clear history and rewarm.
Every gap fences pending publications. A silent socket noticed only at the idle
deadline can still require a longer rebuild.

## API and local dashboard

The API binds only to `127.0.0.1:9000`, Redis to `127.0.0.1:6379`. The frontend
is a separate local project, `dist/ghost-frontend`, served at
`http://127.0.0.1:8765`; its proxy reaches the API through the SSH tunnel on
local port 19000. No dashboard assets or browser service run on the droplet.
Browser sleep or tunnel loss does not stop server forecasting.

| Route under `/forecasts/chainlink-twap` | Behavior |
| --- | --- |
| `/live` | One Redis GET, validates expiry, returns original producer JSON bytes with clock headers |
| `/stream` | SSE `event: ghost`; envelope has delivery metadata in `api` and current producer object in `ghost` |
| `/accuracy` | One GET of minute-refreshed monitor JSON; 180-second cache TTL |
| `/comparison` | One GET of finalized frozen chart pairs; minute refresh, 180-second TTL, 4 MiB payload cap |

These requests do not query PostgreSQL or calculate forecasts/accuracy. The
independent monitor performs bounded historical reads. API enablement is
separate from producer enablement. Missing, expired or disabled state returns
typed unavailability rather than inventing a price. Invalid optional ghost API
settings disable the feature, not the ordinary source routes.

SSE is current-state delivery, not an event replay log. One shared subscriber
resyncs from Redis after reconnect, including transparent library resubscription;
clients receive a fresh state even without another publication. Slow clients
have bounded queues/send deadlines. Live snapshot/SSE are uncompressed with
`no-store, no-transform`; historical comparison may be gzip-compressed. Browser
clients expire stale values independently using a conservative clock bracket.
They must not start a new full server `remaining_ns` lifetime upon receipt.

The separate `GET /markets/current/dashboard` route restores **Recent movement**
from a bounded 15-minute history of saved Chainlink spot and official TWAP.
Missing source seconds stay missing; no interpolated samples are invented. It
also returns server time, the current five-minute market boundaries and the
observed official **Price to Beat** from existing validated pre-close Polymarket
observations. A missing/conflicting reference remains null rather than being
replaced with spot, a local average or a later market's value. This historical
route uses two indexed database reads, separate from the Redis/SSE live path,
with three-second SQL and four-second overall bounds. It adds no schema/settings.

The local page fetches this snapshot on opening/resuming and every minute,
retrying after ten seconds while the reference is unavailable. It shows the
Price to Beat, signed distance of current official TWAP from that reference and
time remaining using the server-clock anchor. Its horizontal reference line
uses the same market identity. These are current-market context, not a forecast
of which side will ultimately win.

### September 16 dashboard deployment verification

The owner-approved release `6a4db1f` (dashboard endpoint code `c75ee84`) was
pushed to GitHub and installed by fast-forward pull. Only `price-api` restarted,
at **21:46:24 UTC**; it shut down cleanly and remained bound to loopback. All
collectors stayed active. No schema, environment or collector-state changes
were made. The separate local frontend is at `e8f4643`.

The deployed endpoint returned 886 saved TWAP points and 887 spot points in one
15-minute snapshot, plus an official pre-close opening reference. Browser
navigation away/back restored history immediately; a simulated visibility
pause/resume triggered a fresh historical read. Physical laptop sleep was not
repeated. The current-market panel advanced from the 21:45–21:50 market to
21:50–21:55, with its new reference, signed distance and countdown. Desktop and
390-pixel mobile layouts passed visual checks. The endpoint/API checks passed
56 tests before deployment; no broad new test campaign was run.

The first browser pass saw three transient 503 responses on existing live and
comparison reads, which recovered. The final browser session had no console
errors or warnings; this is a bounded check, not a continuous-uptime claim.
Cleanup removed 408 superseded tracked artifacts (10.4 MB) and four local
scratch-export directories (about 1.06 GB), plus superseded duplicate notes.
Runtime code, active state, tests and the immutable replay fixtures were kept.

### Reading the charts and accuracy

The saved comparison charts align **first acknowledged eligible forecasts** and
actual TWAP at each target source second, separately for 3/5/10/30 seconds.
Selection is frozen before outcome exclusions: a missing or bad first forecast
is not replaced by a later better one. Clean pairs require exact first target
matches, valid causality/clocks, no conflict and strictly early acknowledgement.
The 15-minute window ends at least 125 seconds behind now, potentially farther
during persistence recovery. Saved pairs survive browser sleep.

**Ghost MAE** is mean absolute error in dollars on those plotted pairs.
**Average TWAP move**, formerly **Held-TWAP MAE**, is mean absolute change from
the official TWAP known at each selected decision to the same future target.
It measures the error of assuming no change. For example, if the TWAP moves
$5.37 on average while ghost misses by $1.04, ghost has roughly 81% less error
than assuming no change: `100 × (1 − Ghost MAE / baseline MAE)`. A zero baseline
does not support a percentage improvement calculation. Dollar values are exact
Decimal strings; rendering alone uses normalized display coordinates.

All cards share time/price scales and a symmetric signed-dollar-error scale.
Frozen means no later revision overwrites the chosen historical prediction; the
live cards can continue refining forecasts as new spot arrives. Missing prints,
conflicts and late acknowledgements remain visible counts. "No matched print"
means none was matched in the recorded window, not that no print ever existed.

The accuracy endpoint scores every eligible published decision, including
refinements, so its MAE need not equal the first-publication chart MAE. It has
completed 1-hour, 24-hour and 7-day panels with explicit issued/published/early/
missing/conflicted denominators. Hourly Decimal sums yield means; merged
histograms yield quantile brackets, not averages of hourly medians. Confirmed
Redis lead and issue coverage are not independently measured browser uptime.

A frozen policy/horizon/cohort baseline needs three qualifying full UTC days
and at least 3,000 scored pairs. Current 24-hour evaluation needs at least 1,000
pairs and coverage. Deterioration requires MAE >125% of baseline and paired
excess MAE >baseline+0.01 bp for three hourly evaluations. Recovery needs
MAE ≤110% or excess ≤baseline+0.005 bp for three evaluations. These are
operational heuristics, not statistical significance. Until qualification, the
status remains `collecting_baseline`. Feed health separately records receipt
gaps, source holes, connection ends and worker errors.

## Storage, safeguards and maintenance

Continuous terminal decisions compact only after their 120-second matching
window. Compact evidence and hourly contributions commit atomically before full
slot detail is removed; failed rows retain evidence and block the completeness
watermark rather than pretending their hour is complete. Maintenance is bounded
to at most 100 rows and a cooperative three-second compaction budget per cycle,
with statement/lock limits and retry/cursor handling so one bad row cannot
permanently prevent all later maintenance. Runtime-owned rows are excluded.

| Data | Retention |
| --- | --- |
| Continuous compact forecast/result records | 7 days after issuance |
| Hourly ghost accuracy and accepted-feed health | 90 days |
| Non-ghost collector history | Maximum 10 days, bounded separate timer |
| Existing stricter raw capture policy | Normally 72 hours |
| Legacy canary full audit | Existing terminal + externally verified + ≥96-hour expiry rule |

Compact rows preserve forecast/target prices, clocks, attempted membership and
hashes; they cannot reproduce discarded full slot inputs. Late/conflicting target
annotations adjust compact records and hourly contributions atomically.
Old outboxes cannot resurrect expired records. Repository cleanup is separate
from production database retention and never deletes active state or credentials.

Continuous ghost relations warn at **5 GiB**, pause admissions at **5.5 GiB**,
and have a **6 GiB** total budget, a **1,500,000 retained-decision** cap and a
**10 GiB database-filesystem free-space** reserve. Maintenance continues during a
capacity pause. At activation optional evidence and microstructure caps were each
4 GiB with 3 GiB warnings; these are overrides, not example defaults. A measured
one-hour compact layout extrapolated to 4.38 GiB per new week, not seven observed
days. Shared disk, core data, WAL and logs still consume capacity. Logical budgets
do not create disk space; deleting rows/vacuuming can permit reuse without reducing
allocated relation size. Seven-day retention does not guarantee seven-day uptime.

The current state directory is `/var/lib/price-collector/ghost-continuous`.
Preserve its original start, stop latch and durable outbox across restarts.
Do not reset state or raise caps to bypass a fault. Freshness/cache expiry,
rebuilding, capacity pause and worker failure are different unavailable states.
Intermittent maintenance timeouts were observed before and after the comparison
deployment and recovered; the deployment did not establish zero failures.

## Evidence, reproducibility and cleanup provenance

Historical reports and generated logs are recoverable in Git at the complete
pre-consolidation snapshot **`0a1f9983f8bdcc9c3ec8cb834c30c20d46703380`**. For
example, `git show 0a1f998:SPOT_TWAP_RESPONSE_STUDY.md` retrieves the full study,
and `git show 0a1f998:results/spot_twap_response/2026-09-15-reliability-canary/FINDINGS.md`
retrieves the final canary report. This is access to the ghost study's provenance,
not permission to restore the separately retired research pipeline.

Key release/evidence identities:

| Milestone | Commit / location at that snapshot |
| --- | --- |
| Historical identity, delay and projection | `SPOT_TWAP_RESPONSE_STUDY.md`; `research/spot_twap_response/`; saved pilot outputs under `results/spot_twap_response/2026-09-13-pilot/` |
| First live hour | `aa78346b2c8c2898a0a522798be3105552aaec7d`; September 14 live-canary results |
| Freshness + partial batches | `770df37cc0f0dbdea1138141ee67746356e9563a`; September 14 combined-canary results |
| Initial browser delivery | `5bc676cda3a0817bfac6b9ce288b3e9f694cc435`; September 15 checkpoint-c results |
| Reliability report before attribution addendum | `7c31c2a`; September 15 reliability-canary results and September 16 peer-review corrections |
| Continuous activation | `6c6f5bf`; `results/ghost_continuous/2026-09-16/` |
| Saved comparison endpoint | `512a4f1`; deployment recorded at `0a1f998` |

Large raw exports were outside Git and some inputs expire by design. Source
code/provenance alone cannot regenerate deleted original observations. Do not
claim a newly run study when reading archived outputs. The preserved recorded
hour under [tests/fixtures/ghost_twap](tests/fixtures/ghost_twap/README.md) tests
21,600 exact Decimal forecasts with independent expected values, original
3s/3s policy and synthetic replay ordering; it is not a full original-message
capture. Its generators, manifests and test-imported analysis helpers remain.
Runtime, schema, deployment units, automated tests and replay fixtures are not
obsolete reports and are retained. The unrelated read-only `results/lockflip-2026`
archive remains untouched. At the owner's subsequent cleanup request, the H3
reports and generated leader-risk results were also consolidated above; their
test-imported source files remain, and Git preserves the detailed old reports.

The historical local dashboard checks used real tunnel-delivered responses,
desktop 1440-pixel and mobile 390-pixel views, and bounded browser checks rather
than another producer canary. Startup/reconnect restored SSE, expired values
cleared after tunnel loss, and the page's start button recreated only its managed
local tunnel while preserving the server. Foreign-origin/missing-action-header
start requests were rejected. Frozen comparison values, counts and windows
matched their exact API response. Expand/collapse, 1x/2x/4x zoom, horizontal
scrolling and keyboard/pointer inspection worked without mobile page overflow.
The average-move explanation handles equal errors and a zero baseline. Synthetic
chart fixtures were removed after isolated checks and were not accuracy evidence.
These functional UI checks establish neither indefinite browser reliability nor
a trading advantage; their old screenshots/logs are disposable local artifacts.

At the owner's cleanup request, superseded local canary-export/scratch copies
can be removed after this consolidation; they are not inputs to the preserved
recorded-hour regression fixtures. Their original hashes and report provenance
remain in the historical Git snapshot. Deleting raw observations reduces future
reanalysis possibilities; the consolidated findings are not a replacement raw
message ledger. Cleanup does not remove active state, production audit tables,
credentials, supported operator code or unrelated research.

The September 16 local cleanup also removed the redundant H3 worktree, old
pilot copies, canary exports, deployment scratch scripts, browser captures and
retired research plans: 2,203 files totaling 2,349,687,766 bytes. The workspace's
`dist` folder now contains only the active collector checkout
(`ghost-checkpoint-a-release`) and local dashboard (`ghost-frontend`). Those
directories, the dashboard's live state and interpreter, and the recorded-hour
test fixtures remain required; `dist` is not wholly disposable build output.

No broad new test campaign is needed merely to consolidate documentation.
Implementation changes still receive focused checks appropriate to the changed
behavior. For current operations and exact service commands, use
[README production operations](README.md#continuous-ghost-retention-and-accuracy).

## Proposed settlement-winner monitor

**Status: jointly agreed design; implementation authorized, not activated or validated.**
The independent reviewer accepted this section subject to symmetric baseline
rules, an explicit report deadline and precise eligibility/context definitions.
Those three amendments are incorporated below; this is the agreed build scope.

### Evidence checked during planning

Saved CSVs at `0a1f998` reproduce the receipt-clock pilot's 30-second projected
lead bins: 31/247, 4/261, 0/393, 1/494 and 0/530 losses/markets for [0,1),
[1,2), [2,4), [4,8) and at least 8 bp. The paired comparison is 102 markets
improved over current TWAP, 13 worsened and 23 wrong under both. These are
re-summed historical outputs, not a fresh replay of deleted observations.

Projected lead at least 2 bp had 1/1,417 losses in that development cohort.
Current-TWAP lead at least 2 bp had 23/1,405; at least 4 bp had 5/984. These
selected subsets overlap but are not identical matched pairs. Comparing the
1/1,417 result directly with H3's 19/4,019 mixes cohorts and cannot establish a
coverage gain at a fixed risk. The 1.243-bp p90 belongs to the generic rolling
30-second ghost, not the exact-close replay. At a $110,000 strike, an $11 lead
is exactly 1 bp and belongs to [1,2), not [0,1). Historical requested slots also
included carry; they were not all directly observed.

The archived closing-price comparison found near equality, not exact equality:
the maximum difference was $0.000000000014051072 across 1,925 comparisons.
That observation does not establish an opening-reference rounding tolerance.

### One output and one candidate rule

1. During the final 30 wall-clock seconds, update one forecast for the ending
   market's exact close E using the same 60-slot calculation, E−62 through E−3.
   Reuse the existing freshness, carry and reconnect rules. Source-age limits
   can require a target up to 35 seconds beyond the latest TWAP stamp; do not
   interpolate the six existing fixed horizons. Preserve the ending market
   identity with the shared helper applied to the decision's own UTC second
   while it precedes E, rather than assigning E to the next market. Healthy and
   degraded forecasts qualify under the unchanged ten-second carry cap; retain
   carry counts and ages. Extend the frozen slot view through anchor+32 when
   necessary without changing the existing six-horizon contract.
2. Use the validated website Price to Beat already observed by the existing
   evidence collector, available to the producer before its decision. Missing,
   invalid or conflicting website references mean no candidate call. Do not
   substitute a reconciled later price or silently fall back to the opening
   stream event. Record that event and its difference for diagnostics when
   available; website/stream precision agreement needs an explicit policy
   before it can become a qualification rule.
3. Record ghost, current-TWAP and spot sides and signed dollar/bp leads on every
   published settlement update. Each signal has its own **first
   eligible, acknowledged-before-close update with absolute projected lead
   at least 2 bp**, within the final 30 seconds. The ghost is the primary signal;
   TWAP and spot are symmetric baselines. This cutoff is an explicitly
   selected development hypothesis, not an established low-risk threshold.
   Keep the first call immutable, even if later updates reverse or become
   stale. Later updates remain visible but are not independent first calls.
4. Display projected closing price, projected side, Price to Beat, signed
   distance, time remaining and feed quality. Label the candidate rule
   **unvalidated**. Do not show "locked", a confidence percentage or a claim
   that no further TWAP/quote crossing will occur. At E, expire live eligibility
   and retain the ending call only as historical/awaiting official outcome.

### Small implementation boundary

Keep the current six-horizon contract unchanged. Reuse the existing Chainlink
worker and calculation/publication helpers for a separate versioned settlement
record and Redis output, with thin read-only API/SSE delivery. No new service,
model, feed connection or duplicate HTTP poller. The existing evidence worker
delivers bounded market context from the probabilities collector through a
dedicated Redis key to the Chainlink producer's in-memory state. The writer
applies the same identity, request-parameter, receipt and completion checks as
the dashboard opening-reference path, and rejects conflicting website values.
Do not query PostgreSQL on the feed or forecast request path. Record
both the reference's original observation time and when the producer obtained it.

Reuse frozen-input evidence and durable-before-publication ordering; slot counts
alone cannot reproduce same-second revisions. Preserve the frozen calculation
and reference while acknowledgement, first exact-E target and official outcome
are attached separately. Cap live expiry at E, recheck before publication and
record actual acknowledgement: late/unknown acknowledgement does not qualify
as a pre-close call merely because it preceded a delayed closing print. Outcome
matching must follow the market's official resolution and tie rule.

Individual forecasts/results retain the existing seven-day limit; do not add
an indefinite research exception. Compact after matching using the existing
bounded pattern, and retain declared cohort/day counts under the existing
90-day accuracy-summary policy. Raw observations retain their existing limits.
Automatically materialize the final report within six hours after the fixed
outcome cutoff, before the earliest individual rows expire at seven days.
Retained report aggregates include scheduled-market coverage/reasons; per-signal
calls, resolved losses and unresolved calls; call-time summaries; daily, side
and quality breakdowns; paired wins/losses at the ghost call; and revocations.
Keep these bounded report summaries for ninety days. A missed finalization
deadline is an explicit incomplete report, never a silently reconstructed one.

### One bounded prospective evaluation

Owner amendment, September 17, 2026 UTC: shorten the originally proposed five
days to **two complete UTC days (576 scheduled markets)**. Keep the 2-bp rule,
24-hour outcome cutoff, six-hour finalization deadline and retention unchanged.
The armed window is September 18 00:00 through September 20 00:00 UTC;
deployment and the two unarmed live-close checks are recorded below.

After focused boundary, reference-causality, expiry and outcome-matching checks,
freeze code, the rule and exact dates for two fresh complete UTC days. Fix the
outcome-reporting cutoff at 24 hours after the final market closes. Start this
new evidence period explicitly; the existing rolling-ghost producer did not
already collect the required settlement calls. Do not tune the rule during the
window or stop early when a repeatedly checked statistic looks favorable.

Evaluate each of ghost, TWAP and spot at its own first acknowledged 2-bp
qualification in the window, reporting coverage, losses and call timing.
Additionally compare all three sides at the ghost's first-call instant on the
same markets and report paired improvements/worsenings. Do not mistake that
ghost-selected paired comparison for the symmetric baseline-rule comparison.
Neither establishes an entire coverage-versus-risk frontier. Include daily,
Up/Down and feed-quality breakdowns.

TWAP and spot baseline calls are evaluated only at eligible acknowledged ghost
settlement publications: ghost-specific unavailability also removes baseline
opportunities, so their coverage does not measure independent baseline feeds.

- Coverage denominator: every scheduled market in the declared window, with
  unavailable references, input gaps, below-threshold markets and late/unknown
  publications counted separately.
- Descriptive loss denominator: resolved issued first calls, not abstentions.
  Keep unresolved issued calls explicit, and never remove an earlier issued
  call because it was later revoked, stale or wrong.
- If a binomial upper bound is reported, label its fixed-sample independent,
  comparable-market assumptions. For conservative unresolved-outcome analysis,
  also calculate the bound treating every unresolved issued call as a possible
  loss. One call per market does not prove independence; resampling all-zero-loss
  days does not establish a useful zero-event risk bound.

The first deliverable is a prospective result with coverage and limitations,
not a calibrated 99% probability for an individual market. Any later confidence
label needs a separate acceptance decision supported by fresh evidence. A
20-cell tier system, extra spot feature or machine-learning model is outside v1.

### Settlement implementation and rollout

Deployed and enabled on September 17, 2026 UTC; the two-day evaluation is
**scheduled, not yet prospectively validated**. The existing rolling ghost remains
contract 4. The settlement output has its own schema 1 and
`settlement-first-2bp-v1` rule. This implementation incorporates all three agreed
amendments above. The new dashboard card calls it **Unvalidated** and shows the
exact-close price, website opening reference, side, signed lead, remaining time
and slot quality. At expiry/close it removes the live signal and labels any
retained last observation as historical, not the immutable first call.

The small calculation is in `price_collector/settlement.py`. It reuses the
frozen ghost slots, adds only the necessary future tail through anchor+32, and
uses Decimal throughout. The current TWAP and spot baselines use the identical
reference. Both healthy and degraded projections qualify under the existing
carry limit. The first observed stream boundary event is retained separately
for an opening-reference difference diagnostic; a missing boundary event is
not reconstructed or substituted for the website reference.

`settlement_runtime.py` runs inside the existing Chainlink ghost worker. The
probabilities evidence worker supplies the bounded context cache from its
existing HTTP request; there is no extra feed or metadata poller. The consumer
pins when it first obtained each context. Frozen inputs reach a separate
fsynced outbox before publication. PostgreSQL writes run independently. Each
attempt is rechecked, has an absolute expiry no later than E, and records its
exact transmitted bytes and actual acknowledgement clocks. A gap or reference
change after a valid attempt is recorded separately: it cannot retrospectively
erase a timely acknowledged first call. Late/unknown acknowledgements earn no
first-call credit. Startup reconciles durable records without republishing.
Optional failures cannot stop the existing six-horizon input path.

The new read-only routes are:

| Route | Purpose |
| --- | --- |
| `/forecasts/chainlink-twap/settlement/live` | One Redis GET; original serialized snapshot or typed 503 |
| `/forecasts/chainlink-twap/settlement/stream` | Existing bounded SSE transport; named `settlement` events and reconnect resync |
| `/forecasts/chainlink-twap/settlement/report` | Cached prospective evaluation report; no request-time SQL |

Cache keys are `btc:live:ghost_settlement_context`, `btc:live:ghost_settlement`,
its `:updates` channel, and its `:report` summary. Snapshot/SSE have no-store
headers, independent wire validation and the existing pinned monotonic expiry.
The API still binds only to loopback and the local dashboard uses the SSH
tunnel. The original source caches and rolling ghost endpoints are unchanged.

`settlement_store.py` and the three additive `settlement_*` tables retain
individual frozen projections/target results, per-market first calls, and
aggregate reports. Full slot detail compacts after the 120-second match window;
individual and per-market evidence expires at seven days. Only aggregate
reports retain ninety days. The existing operator retention timer also expires
these tables when the settlement flag is off. If the producer is absent at the
report deadline, that timer preserves a bounded **incomplete** report from
already captured evidence before expiry; it never invents late outcomes.

Reports include each signal's own first-call coverage, resolved losses,
unknown outcomes, acknowledgement timing, day/side/quality breakdowns,
abstention/publication reasons, revocations, and the paired comparison at the
ghost's first call. Official outcome identity and the fixed reporting cutoff
are checked. No individual confidence percentage or independence-based bound
is emitted. A changed configuration cannot reassign recovered decisions to a
different evaluation: the chosen evaluation start is frozen with each record.

Resource bounds: at most 512 in-memory/outbox records, 128 KiB per record,
0.5-second explicit Redis operation deadlines, 256 MiB relation warning and
512 MiB relation cap with reserve, and 200,000 audit rows. The existing ghost
disk/health guards also gate admission. Sustained storage growth has not
yet been established for this output. Optional startup initiates cancellation
after five seconds; owned filesystem cleanup may take longer. On shutdown,
ordinary ghost preservation proceeds independently of settlement cleanup.

Settings to review manually, preserving the existing environment files:

- `/etc/price-collector/collector.env`: `SETTLEMENT_ENABLED=false` and
  `SETTLEMENT_EVALUATION_START_MS=0` are the safe install defaults. Enabling
  requires the existing `GHOST_TWAP_ENABLED=true`, `GHOST_TWAP_CONTINUOUS=true`
  and `POLYMARKET_EVIDENCE_ENABLED=true`. Do not enable unrelated raw capture.
- `/etc/price-collector/api.env`: `SETTLEMENT_API_ENABLED=false` by default.
  The API retains only its reader credentials.
- Zero evaluation start means **unarmed live display/audit**, not a running
  study. After deployment verification, set one reviewed future UTC-midnight
  epoch millisecond value to declare two complete days. Keep that value and
  the frozen rule unchanged through the 24-hour outcome cutoff. Finalization
  runs after the cutoff, with an additional six-hour deadline; late/missing
  completion is explicitly incomplete.

During the two-day evaluation, check the cached `/settlement/report` endpoint
(full route above) at least daily and after any collector restart. Its
`runtime.fault` must be null: a non-null value stops new settlement admission
for that process lifetime, even while fresh reports continue. Inspect the
Chainlink journal, correct the cause and restart that collector only after
preserving its outbox; keep the evaluation dates unchanged and count the gap.
Also verify the report is current (`generated_at_ms`/`valid_until_ms`) and its
decision counters advance during the final 30 seconds of markets. A missing or
stale report is an unknown monitoring state, not evidence of a clear fault.
With a fresh report and null fault, rising
`evaluation.reason_counts.reference_unavailable_or_noncausal` or
`runtime.counters.context_read_errors` instead points to missing/invalid market
context; inspect the probabilities evidence worker and context cache. That
condition withholds calls without latching `runtime.fault`. A missing live
snapshot outside the final 30 seconds is expected, so use the report for this
operating check.

Validation for this build: the full backend suite passed **1,733 tests with
17 optional integration tests skipped**. A final recovery-association change
then passed the affected runtime/store/retention subset, **44 passed, 1 skip**.
Focused frontend consumer and proxy checks passed. A real browser loaded an
isolated fixture through the production settlement consumer, displayed an
eligible 2.5-bp candidate, and cleared the live values at expiry/close while
preserving only the historical note. This is UI verification, not live market
evidence. The PostgreSQL schema check was subsequently completed as recorded
below. These were pre-deployment checks; the subsequent two live-close checks
are recorded below and do not establish two-day performance.

Review follow-up, September 17, 2026 UTC: the full schema was applied to an
isolated `settlement_validation_*` database on the droplet's PostgreSQL 16.15
server. `tests/settlement_postgres_smoke.py` then exercised the real store with
the writer role and checked persistence/idempotency, each signal's first call,
revocation accounting, rejected changes to frozen evidence/ACK clocks/first
calls, the seven-day deletion guard, terminal compaction, the actual
outcome/report queries, immutable final reports, reader permissions and the
capacity query. All passed. Reapplying the entire schema also succeeded and
preserved both audit rows, the market summary and the final report. These are
synthetic database checks, not additional forecasting evidence or a live-rate
storage measurement. The scratch database and all temporary files were removed;
production tables, service code, flags and processes were unchanged. The
affected local report tests also passed: 27 tests.

The schema already begins with `BEGIN;` and ends with `COMMIT;`, with no
intervening top-level commit. Consequently the documented
`psql -v ON_ERROR_STOP=1 -f schema.sql` apply is already transactional; an
additional `--single-transaction` option is unnecessary. The executed schema
file's SHA-256 was
`173390d095c94a7ee755086be0c66d03a8cf047ff6ccaea0baacd45e6b1a0fef`.

For the code/schema installation, run **after pushing this release to GitHub**.
Apply schema before restarting the probabilities, Chainlink and API services.
For an initial installation, keep the settlement flags off and evaluation
unarmed. Pause the two affected writers and the retention job while applying
the full schema: its existing ghost-audit DDL can deadlock with live writes.
Use a subshell cleanup trap so a failed apply restores those services.
No new service/unit, public port, or production frontend is needed.

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
(
  set -eu
  trap 'sudo systemctl start price-collector-polymarket-probabilities price-collector-polymarket-chainlink price-collector-retention.timer' EXIT
  sudo systemctl stop price-collector-retention.timer price-collector-retention.service
  sudo systemctl stop price-collector-polymarket-probabilities price-collector-polymarket-chainlink
  sudo -u postgres env PGOPTIONS='-c lock_timeout=3s -c statement_timeout=30s' psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql
  sudo systemctl restart price-api
)
sudo systemctl status price-collector-polymarket-probabilities price-collector-polymarket-chainlink price-api price-collector-retention.timer --no-pager
curl --fail http://127.0.0.1:9000/healthz
curl -i http://127.0.0.1:9000/forecasts/chainlink-twap/settlement/live
sudo journalctl -u price-collector-polymarket-probabilities -u price-collector-polymarket-chainlink -u price-api -n 80 --no-pager
```

The settlement route should return a typed disabled 503 during initial install.
The existing retention timer loads the updated module on its next invocation;
no unit change/reload or immediate extra deletion run is needed. The local
dashboard proxy must be restarted to load its two new allowlisted routes;
the frontend stays on the user's computer. Live enabling and the dated
two-day evaluation are separate operational steps after review.

### September 17 deployment and two-day evaluation

Backend runtime `a33fccf` was pushed to GitHub and installed by fast-forward-only
pull. The separate local dashboard was fast-forwarded to `a090d02`; its proxy
and SSH tunnel were restarted on the user's computer. Nothing was deployed as
a frontend on the droplet. The two-day build passed **1,734 tests, 17 optional
integration skips**. Its revised schema/store smoke check also passed on the
droplet's PostgreSQL 16.15 in an isolated scratch database, including rejection
of a market at the two-day end and finalization before the three-day cutoff.
The scratch database was removed.

The first production schema attempt deadlocked against an active ghost-audit
writer and rolled back its transaction. Applying it with the affected writers
and retention job paused succeeded. The installation was checked with settlement
disabled before enabling producer/context and API flags. Existing source feeds,
reader credentials, private listeners and retention settings were preserved.

Two unarmed live markets verified the complete publication and matching path:

| Closing UTC stamp | Market ID | Available / acknowledged before close / exact-target matched | First exact closing TWAP |
| --- | --- | --- | --- |
| September 17 02:15 | 5965370 | 60 / 60 / 60 | 76126.528295786627727360 |
| September 17 02:20 | 5965371 | 60 / 60 / 60 | 76093.777889763472179200 |

At 02:21 UTC the first market's 60 rows were terminal and compacted, the report
showed 120 decisions and acknowledgements, no fault and no persistence backlog.
These are operational checks, not a candidate loss-rate estimate. A real browser
observed the second market from its first eligible projection at 02:19:30 until
close: the projected price updated from $76,101.18 to $76,093.78, using the
observed website reference $76,126.53. It cleared the live candidate at the market
boundary and kept only a labelled historical note. This checks live rendering
and expiry; it does not measure browser delivery latency or establish zero gaps.

After these checks, `SETTLEMENT_EVALUATION_START_MS=1789689600000` was installed
in the existing collector environment, preserving all other settings and file
permissions, and only the Chainlink service was restarted. The cached report
confirmed **scheduled**, persistence complete, null runtime fault and:

| Boundary | UTC | Epoch milliseconds |
| --- | --- | --- |
| Start, inclusive | September 18 00:00 | 1789689600000 |
| End, exclusive | September 20 00:00 | 1789862400000 |
| Outcome cutoff | September 21 00:00 | 1789948800000 |
| Final report deadline | September 21 06:00 | 1789970400000 |

The two-day window covers 576 scheduled five-minute markets. In Chicago it runs
from September 17 at 7 p.m. through September 19 at 7 p.m. CDT. Live projections
are already enabled, but earlier unarmed observations do not belong to this
evaluation. Keep the frozen 2-bp rule and dates unchanged. The producer generates
the final aggregate after the outcome cutoff; absence at the final deadline is
reported as incomplete. Individual evidence still expires after seven days and
aggregate reports after ninety days. The dashboard remains **Unvalidated**;
no "locked winner" confidence is established by this deployment.
