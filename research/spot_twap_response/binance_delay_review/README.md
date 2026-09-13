# Binance receipt-pairing audit

The supplied section-15 numbers reproduce exactly. They are receipt differences between **chosen timestamp pairs**, not identified Binance-to-TWAP economic response times.

## What was checked

Two fixed-week diagnostic statements ran sequentially within one read-only repeatable-read transaction, each limited to 20 seconds. Execution completed successfully in 30.641 seconds including SSH. No production code, configuration, collection or database contents changed. The inclusive sample-second bounds intentionally match the supplied SQL: 2026-09-01 00:00:00 through 2026-09-08 00:00:00 UTC.

| Pairing chosen by section 15 | Pairs | p10 | Median | p90 | p99 |
|---|---:|---:|---:|---:|---:|
| TWAP source stamp = Binance sampler second + 3 s | 576,222 | 5.328 s | 5.707 s | 6.189 s | 6.673 s |
| TWAP source stamp = Binance sampler second + 4 s | 576,219 | 6.328 s | 6.707 s | 7.190 s | 7.671 s |

The extra second in the second row comes from selecting a later TWAP stamp. The query does not identify which Binance move actually reached Chainlink in either purported case.

## The clock mismatch

In `price_collector/collector.py`, `build_pending_sample` keys Binance history from the sampler's local `now_ms` (lines 138–146). The sampled row separately retains the ticker's provider event field E and its original local receipt time. `sampler_loop` writes the cached latest ticker once per local second (lines 403–418). Receipt is recorded after parsing (line 302). Thus Binance `sample_second_ms` is a local sampler bucket; it is not the ticker's provider observation stamp.

Across 604,756 sampled Binance rows:

- **603,313** have a sampler key different from the provider timestamp's floored second.
- Sampler key minus ticker receipt: **948 ms median**, 944/955 ms at p10/p90, 992 ms at p99, maximum 9,956 ms.
- Sampler key minus provider event: **984 ms median**.
- Ticker receipt minus provider event: **36 ms median**, 30/40 ms at p10/p90, 49 ms at p99.
- **3,551 consecutive sample rows reuse the identical cached ticker**, with the same provider event, receipt and price.

For each pair selected by section 15, let b be the Binance sampler key, rB its retained ticker receipt, u the selected TWAP source stamp, and rW that report's first retained receipt. The query sets u=b+k seconds, so the exact accounting identity is:

    rW - rB = k seconds + (rW - u) + (b - rB)

Consequently its measured distribution includes the sampled ticker's cache age. Do not subtract marginal medians to manufacture a corrected propagation estimate; medians do not generally add, and the economic move remains unidentified.

## Interpretation

The empirically supported Chainlink-spot/TWAP source-window mapping gives a defensible **model-implied pairing** between those two retained streams, subject to their coverage, revision and proxy limitations. Binance is not an identified constituent stream in that mapping. Its BTC/USDT price also differs from the BTC/USD Chainlink input.

Section 15 pairs every available Binance sampler row, including unchanged prices and repeated cached events. It neither matches individual Binance and Chainlink moves nor demonstrates that a particular Binance change caused a particular TWAP response. The earlier aggregate move comparison cannot turn these timestamp joins into an event-specific causal relationship. The 5.707/6.707-second figures therefore must not be described as measured first incorporation of Binance prices or as actual economic propagation delay.

Artifacts: [clock profile](clock_profile.csv), [reproduced pairings](pairing.csv), [query](query.sql), [raw output](stdout.txt), [manifest](manifest.json). The manifest preserves supplied-input and collector-code hashes. The supplied pilot files were not edited.
