# H3 data readiness audit — 2026-09-10 UTC

**Initial audit verdict:** the running collector lacked pre-decision Price to
Beat observations, fee/trading-rule history and subsecond execution-timing
evidence. The prospective collection update below is now deployed; it does
not reconstruct the historical gaps documented in this audit.

**Deployed checkpoint, 2026-09-10:** Following owner approval, commit
`ace8d19bef1c72d0141ebf729c3a37b534c90efb` was published to GitHub `main`,
validated on the droplet, installed by a fast-forward Git pull, and enabled
at **02:26:56 UTC**. All 565 tests passed on production Python 3.12.3; the
schema applied twice in a disposable PostgreSQL database, and real writer-role
integration checks passed for exact Decimal storage, deduplication, restricted
grants, graceful shutdown and unclean-session recovery. The disposable database
and checkout were removed after validation.

The new path retains receipt-timestamped official Price to Beat observations,
fee and order-rule snapshots, tick changes, CLOB sessions/gaps, and paired
Up/Down quotes sampled at actual observation times every 100 ms during the
final 120 seconds. Financial values remain exact. Compact metadata payloads
are deduplicated; full book depth and quantities are excluded. The default
storage guard warns at 4096 MiB and pauses new quote capture at 6144 MiB
across the three evidence relations, including indexes and TOAST. Metadata
continues, and there is no automatic deletion; this is not a hard disk cap.

The first verified market was **5963357 (02:25–02:30 UTC)**. Its opening
Price to Beat `77993.07048958661` was received **181,253 ms before close**.
The final 120 seconds produced **1,200 rows in 1,200 distinct 100 ms bins**,
with no observations outside the market, future component receipts or inverted
monotonic receipt ordering. Four rows faithfully retained missing quote sides
from book updates. Maximum spacing between actual observations was
131,698,166 ns; the first observation was 391,087 ns after T-120. Actual
observation times, rather than nominal bins, determine availability.

Rollover succeeded with a durable session end. The next market's opening
Price to Beat was received **292,278 ms before close**, after ten explicitly
recorded unavailable observations during preload and the initial opening
seconds. At **02:30:50 UTC**, the three new relations together occupied
**663,552 bytes (0.6328125 MiB)** including indexes and TOAST. This short
measurement does not establish sustained daily growth or retention duration.
All six services remained active, the API was healthy, and the probability
service had zero restarts and no warning/error events. Existing environment
values and file permissions were verified unchanged outside the ten new
evidence settings. PostgreSQL, Redis and the API remained loopback-only.

At **02:33:58 UTC**, the second market was collecting quotes normally,
all six services remained healthy, and the evidence relations occupied
933,888 bytes. The first verified market's official resolution was still
pending. Its recorded opening-to-official-settlement equality check therefore
remains pending; the existing ended-market reconciler continues its scheduled
retries. No winner or final settlement price was inferred from quotes.

Sustained storage growth still needs observation over real collection days.
Recorded public-data timing and exchange delay
settings do not measure actual order-to-fill latency. A future H3 evaluation
must declare its quote-sampling, execution-delay, liquidity and slippage
assumptions. These additions do not reconstruct missing historical evidence.
See [OPERATIONS.md](OPERATIONS.md) for the schema-before-restart update and
verification procedure.

Owner scope revision after the initial audit: **Polymarket order-book depth and quantities are excluded from the required collection**, based on the owner's assessment of market volume. The study will use quoted ask prices with an explicitly declared trade-size, liquidity, delay and slippage assumption. Aggregate market volume does not measure available size at a particular ask. Consequently, estimated returns will be conditional on these assumptions; the study will not measure fill capacity or claim observed filled quantities from quote data. The initial audit's depth finding remains a factual limitation, but is no longer a collection blocker for this scope.

This audit checks collection and availability only. It does not test H3, estimate profitability, choose thresholds, or freeze an evaluation protocol.

Audit provenance: read-only SSH inspection of `152.42.247.86`, PostgreSQL database `price_collector`, on 2026-09-10 starting at 01:17 UTC. SQL sessions enforced `default_transaction_read_only=on`, a 20-second statement timeout and a 2-second lock timeout. Historical comparisons below use markets closing no later than **2026-09-10 00:00:00 UTC**. The latest-day diagnostic uses **2026-09-09 UTC**. Queries ran while collection continued; this was not an immutable dataset export.

The deployed checkout was clean at `11df6150be764c878ba008adc4fa83ef3c31aef2`. LF-normalized hashes of local `schema.sql`, `price_collector/db.py` and `price_collector/polymarket_probability_collector.py` matched the deployed files. Existing local changes were preserved. No production files, configuration, services or database contents were changed. Retired research code and derived research tables were not used as study inputs.

## What is collected

| H3 input | Readiness | Evidence and limitation |
| --- | --- | --- |
| Market identity, start/end, Up/Down tokens and settlement rules | Available | Market metadata and UTC windows are stored. All 7,200 markets present at the initial inventory identify `chainlink_twap`, 60 seconds, `btc-5m-twap-60`, and the corresponding Chainlink URL. |
| Official winner, payout, strike and final settlement price | Available for most ended markets | 7,152 of 7,183 markets closing through the cutoff have resolved status and both exact prices. Actual Up/Down payouts, source and reconciled rule version are retained. 31 remain pending. |
| Rolling settlement-reference TWAP | Available, with gaps | The durable `polymarket_twap_events` ledger retains exact E18 and Decimal-compatible NUMERIC values, provider time, wall/monotonic receipt times, connection and sequence. There were 452 sessions and 451 explicit gap records at inspection. |
| Chainlink spot and Binance spot | Available at sampled resolution | `price_samples` retains separate instruments, exact price, source field/topic, provider event and local receipt timestamps. Sampling/upserts do not retain every incoming spot event. |
| Binance futures extension | Available at sampled resolution | `binance_futures_snapshots` retains the last price and its source timestamp, plus mark/index, basis-related fields, funding and open interest. The collector's last-price definition is `aggTrade.p` with source time `aggTrade.T`. The row's `received_ms` is the snapshot observation, not the trade's original receipt timestamp. |
| Contemporaneous market-quote benchmark | Available, with missing/stale cases | One-second Up/Down bid, ask, midpoint and normalized quote probabilities, with per-outcome and individual bid/ask component timestamps. These support quote comparisons after causal availability filtering. |
| Up/Down purchase-price estimate | Quotes available; liquidity assumed | Use the ask observed under the declared delay and slippage convention. Polymarket depth and quantities are excluded by owner decision. Stored `raw` is synthesized quote state; it cannot measure fill capacity. |
| Price to Beat known before each decision | **Missing in the current collection path** | None of the 7,200 saved discovery payloads contained `event.eventMetadata.priceToBeat`. Exact strikes are obtained by the ended-market reconciler. There is no immutable strike-observation ledger. |
| Applicable fees, tick and minimum size | **Partial** | All 7,200 discovery payloads contained `feesEnabled`, `feeSchedule`, `orderMinSize` and `orderPriceMinTickSize`. This is a discovery snapshot, not a history of changes applicable at execution. |
| Execution delay and fill assumptions | **Evidence still needed** | No order submission/acknowledgement/fill measurements or declared execution simulator records are collected. Record timing evidence and quote availability at the modeled execution time. Filled quantity and partial fills must be explicitly modeled assumptions or supported by separately supplied execution records. |
| Missingness and freshness | Partial | Source and receipt clocks support age filters; missing rows expose coverage gaps. TWAP has durable sessions/gaps. The current schema has no equivalent durable Polymarket CLOB session/gap ledger. |

The latest discovery metadata inspected contained `feesEnabled=true`, fee schedule `{rate: 0.07, exponent: 1, takerOnly: true, rebateRate: 0.2}`, `orderMinSize=5`, and `orderPriceMinTickSize=0.01`. These are observed field values, not frozen study assumptions or proof that a particular order could execute. Preserve the endpoint-defined units and exact numeric parsing when building an execution dataset.

All six expected services were active, `/healthz` returned database OK, and the droplet reported NTP synchronization. Both optional raw-capture flags were false; the raw futures trace and raw Chainlink event tables each contained exactly zero rows. The durable TWAP ledger remains active independently. Binance microstructure collection was enabled; it provides Binance context, not Polymarket depth.

## Historical coverage actually measured

The first stored spot/TWAP/futures samples were at **2026-08-16 01:27:34 UTC**; probability samples began one second later. Initial market metadata starts at 01:25 UTC that day. The inspected data continued into September 10.

For all 7,183 markets closing by the fixed cutoff:

- There were **827,393 / 861,960** possible one-second quote rows in the final 120 seconds: **95.99%** row coverage.
- **3,803** markets had all 120 quote rows; **24** had no quote rows in that interval. The remaining markets had partial coverage.
- Quote completeness varied by day. August 31 had 28,896 / 34,560 possible rows (83.61%); September 9 had 34,337 / 34,560 (99.35%).
- **31** historical resolutions remained pending: one on August 17, four on August 20, and 26 on August 21. They must remain unscored or explicitly excluded until official results are available; quotes are not replacement outcomes.

Row density is not execution eligibility. In particular, provider-second TWAP/Chainlink materializations need not have a row in every second, and a recently received prior observation can still be usable under a declared freshness rule.

For a closer availability check, I inspected the proposed eight checkpoints on all **288 markets on September 9**. I selected stored observations received no later than each decision, then required both provider and receipt ages to be within 0–10,000 ms. The 10-second limit is a diagnostic based on the existing default, **not an accepted H3 freshness threshold**. TWAP used the durable event ledger. Quote checks required both bid and ask for each outcome and applied its oldest-component timestamps. Futures receipt age used the persisted snapshot observation time.

| Seconds remaining | Fresh quotes | Fresh TWAP | Fresh Binance spot | Fresh Chainlink spot | Fresh futures snapshot | All five matched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 120 | 276 | 287 | 288 | 288 | 288 | 275 |
| 90 | 284 | 286 | 288 | 288 | 288 | 282 |
| 60 | 285 | 285 | 288 | 288 | 288 | 282 |
| 45 | 286 | 288 | 288 | 288 | 288 | 286 |
| 30 | 285 | 287 | 288 | 287 | 288 | 284 |
| 20 | 287 | 286 | 288 | 287 | 288 | 285 |
| 10 | 286 | 285 | 288 | 287 | 288 | 283 |
| 5 | 286 | 286 | 288 | 288 | 288 | 284 |

These are checkpoint-specific availability counts, not independent trades, whole-market policy coverage, or H3-eligible entries. They omit the missing causal strike and execution requirements. Tighter freshness rules can reduce availability. This one-day diagnostic does not establish quality across every historical day.

## Why the missing pieces matter

**Depth and quantity — excluded from the revised requirements:** `polymarket_probability_collector.py:1728` reduces exchange book arrays to best bid/ask prices. Its `raw_snapshot()` at line 286 preserves prices and clocks but no quantities. The persisted columns are in `schema.sql:482`. This is sufficient to observe quoted asks; translating them into purchases requires the declared liquidity assumption. No depth collector is required for the revised study. Polymarket's market channel exposes quote and tick-change events that can still support timestamped quote/rule tracking. [Official market channel](https://docs.polymarket.com/api-reference/wss/market)

**Causal strike:** `polymarket_probability_collector.py:1010` parses the exact Price to Beat during reconciliation; `db.py:1253` limits that reconciler's selection to ended markets. All initial resolution checks observed were after close. Discovery metadata is overwritten by `db.py:1182`, and its `seen_ms` is taken before the HTTP request (`polymarket_probability_collector.py:752`). It is not an exact response-receipt timestamp. Discovery also uses ordinary JSON numeric parsing; a new exact strike-observation path should follow the Decimal-aware resolution parser. A later official strike can support explicitly retrospective analysis, but does not prove the agent knew it at T-120 or T-20.

**Trading conditions and execution:** Preserve timestamped fee configuration, minimum order size, tick-size changes, accepting-orders status and exchange-delay settings. Official market details and CLOB market info expose these fields, including market-specific delay configuration. Existing discovery JSON is useful evidence but does not establish every later change. Separately obtain evidence for the chosen latency/fill assumptions and define how quote replay treats stale/missing asks, changed prices, partial fills and failures. Public market data alone does not demonstrate actual order-to-fill latency. With depth excluded, size-dependent fills and slippage remain assumptions unless separate execution records support them. [Market details](https://docs.polymarket.com/market-data/market-details), [CLOB market info](https://docs.polymarket.com/api-reference/markets/get-clob-market-info)

The current one-second quote snapshots do not preserve every intervening price change. A subsecond execution-delay test needs quote-event receipt times at corresponding resolution, without requiring depth or quantities. Alternatively, explicitly use a coarser quote observation convention and disclose its timing uncertainty. Keep durable CLOB session/gap records so a missing connection is not mistaken for an unchanged executable ask.

**TWAP interpretation:** The retained feed is a rolling settlement-reference TWAP. It does not independently reveal the constituents already accumulated in the final settlement window. Before T-60, H3's signal is predictive; any later accumulated-window proxy needs a separate documented validation and must preserve uncertainty. No missing component should be fabricated from Binance or standard Chainlink spot.

## Minimum collection checkpoint for the revised H3 test

1. Persist immutable exact Price-to-Beat observations with market/rule identity, response receipt time, source and raw provenance; record unavailable responses and revisions. Confirm that an official source actually exposes it before the decisions being studied.
2. Retain exact, timestamped fee and trading-constraint observations and updates, including relevant exchange-delay settings.
3. Supply execution-assumption evidence and define a delayed-quote simulation that records the decision, delayed ask, assumed executed size, slippage allowance, fees, abstention and failure reasons. Specify partial-fill handling and distinguish modeled quantities from any separately observed fills. No live orders are authorized by this audit or the scope revision.
4. Preserve quote-event timing at the chosen execution resolution and durable CLOB sessions/gaps. Cover T-120 through close and any chosen lookbacks. This requirement concerns quote prices, availability and clocks; it does not require book depth or quantities.

Existing prices, quotes and resolved results can inform exploratory development with the retrospective-strike limitation stated. The revised H3 test needs the additional evidence above and a newly supplied/exported dataset with recorded provenance. Freeze the trade-size and liquidity assumptions, delay/slippage conventions, freshness, price cap below $0.98, loss-risk ceiling, value buffer and later-day evaluation split before evaluating the claim. Report profitability as conditional on the execution assumptions. Do not reuse retired study models, thresholds, splits or derived labels.
