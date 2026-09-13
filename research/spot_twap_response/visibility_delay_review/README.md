# Section 14 visibility-pair review

The fixed-week section 14 result **reproduces exactly**. This is the recorded receipt offset between a retained Chainlink spot row stamped `s` and the earliest retained TWAP event stamped `s+3 seconds`, conditional on both exact stamps being present. It is not a measurement of frontend display delay, nor proof that this report was the first TWAP to incorporate this particular spot value.

| Matched pairs | 10th percentile | Median | 90th percentile | 99th percentile | Minimum | Maximum | Negative pairs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 550,393 | 2,552 ms | 3,088 ms | 3,611 ms | 4,042 ms | −32,829 ms | 10,241 ms | 9 |

The original inclusive bounds are preserved: spot source seconds run from September 1 00:00 UTC through September 8 00:00 UTC, including the latter boundary second. TWAP extraction extends another ten seconds. There are 604,801 possible spot positions, 576,404 retained spot rows, and **26,011 retained spot rows without an exact `s+3` TWAP report**. Pair coverage is therefore **95.487% of retained spots**, or **91.004% of calendar positions**. A missing exact report is not assigned a later receipt or an invented arrival time.

The original query filters TWAP only by `window_s=60`, not topic/symbol/instrument. The bounded audit found **zero unexpected-identity events**, **zero conflicting TWAP-price stamps**, and **zero paired stamps with multiple events**. It also found no spot provider/sample-second mismatches. Instrument identities matched Chainlink BTC/USD spot and BTC/USD 60-second TWAP. These are measured properties of this snapshot, not general guarantees of the original query.

## Nine negative pairs

All figures below are milliseconds. Source-to-receipt offsets on both feeds remain positive; none of these rows requires a negative own-feed offset to explain the paired result.

| Spot source time, UTC | Paired delay | Spot receipt − source | Spot receipt − publisher | TWAP receipt − its source | TWAP receipt − publisher |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sep 1 04:03:04 | −524 | 8,288 | 1,513 | 4,764 | 493 |
| Sep 2 04:57:30 | −522 | 12,584 | 11,403 | 9,062 | 7,825 |
| Sep 2 04:57:34 | −1,517 | 12,412 | 11,381 | 7,895 | 6,493 |
| Sep 2 04:57:38 | −1,137 | 10,087 | 9,078 | 5,950 | 4,355 |
| Sep 2 04:57:40 | −19 | 7,368 | 6,332 | 4,349 | 3,063 |
| Sep 2 04:57:44 | −76 | 7,873 | 7,049 | 4,797 | 3,521 |
| Sep 3 02:21:06 | −482 | 5,740 | 4,188 | 2,258 | 378 |
| Sep 4 21:10:33 | −32,829 | 37,258 | 35,995 | 1,429 | 250 |
| Sep 6 23:44:22 | −94 | 4,928 | 3,675 | 1,834 | 375 |

In the largest case, the retained spot publisher clock is `s+1.263s`, its receipt is `s+37.258s`, and the paired TWAP receipt is `s+4.429s`. Their difference is exactly **−32.829s**. Five other negatives cluster within fourteen source seconds on September 2, with elevated recorded receipt-minus-publisher offsets on both streams. Differential delivery/reception timing is consistent with these observations. The recorded clocks do not establish whether the delay arose upstream, in transport, or locally, and they do not establish a clock failure.

Spot rows are mutable within a source second. The retained spot receipt need not be that source second's first arrival, and its earlier value/receipt may have been overwritten. The TWAP side uses earliest retained arrival. Consequently, negative pairs do not prove that the collector never saw an earlier spot value. Nor does a close sampled reconstruction establish the publisher's exact internal constituent reports. The pairing is a useful conditional timing statistic under the candidate `a=−3` relationship.

## Artifacts and execution

- [query.sql](query.sql) computes the exact original statistics, coverage and detailed negatives in one fixed-week statement. Additional market-index bounds are redundant under the schema's source-second/market constraints. Original TWAP identity scope and millisecond truncation are preserved.
- [section14_original.sql](section14_original.sql) and [supplied_section14.txt](supplied_section14.txt) preserve the supplied material.
- [negative_pairs.csv](negative_pairs.csv) contains complete source/publisher/receipt clocks, source values and TWAP identity/session details from the audited snapshot. [negative_pairs.sql](negative_pairs.sql) is a saved nine-key inspection query; it was not separately executed.
- [stdout.jsonl](stdout.jsonl), [manifest.json](manifest.json), and [run_review.py](run_review.py) record results, hashes and reproducibility.

Execution exited zero, used one read-only repeatable-read transaction with a **20-second statement timeout**, and took **25.984 seconds including SSH/process overhead**. Sections 15–17 and whole-history calculations were not run. No production settings, schema, services, or source files were changed.
