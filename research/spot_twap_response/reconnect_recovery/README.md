# Recorded spot reconnect regression

The authoritative result is [result_final.json](result_final.json), with input,
result and script hashes in [manifest_final.json](manifest_final.json). The
inspectable implementation is [recorded_reconnect_check.py](recorded_reconnect_check.py).
This is a regression for one recorded connection interruption, not a new canary
or a study-wide coverage estimate.

The original combined canary ran the policy that clears spot history at a gap.
The saved audit reconstructs 413 consecutive accepted input sequences
1353–1765, with no renumbering or conflicting copies, and 414 original decision
cutoffs from 22:45:37.919 through 22:49:07.919 UTC on 2026-09-14. After 65 seconds
of warmup, 287 decisions remain. Policy zero reproduces all 1,722 original
horizon rows exactly for price, counts, quality and target source timestamp.
The comparison changes only `spot_reconnect_max_gap_ms`, from 0 to 10000.

The recorded spot `connection_end` occurred after sequence 1588, at wall time
1789426057915796688 ns and monotonic time 5961087685163650 ns. The last pre-gap
spot source stamp was 22:47:35.000; the first post-gap source stamp was
22:47:39.000. Their local receipt interval was 4,260.133088 ms. The retained
history policy consequently exposes three interior seconds as carried values;
it does not establish that the producer was silent or that prices stayed flat.

## Recovery at the recorded calculation cutoffs

All six horizons first become available under the new policy at decision 1592,
22:47:40.569607072 UTC. These are engine calculation cutoffs, not Redis
publication times or continuously observed cache coverage.

| Horizon | Old first available UTC | Earlier calculation cutoff (ms) | Old / new available | Newly available |
| --- | --- | ---: | ---: | ---: |
| 1 s | 22:48:42.225380599 | 61655.773527 | 162 / 283 | 121 |
| 2 s | 22:48:41.114415884 | 60544.808812 | 164 / 283 | 119 |
| 3 s | 22:48:40.185322874 | 59615.715802 | 166 / 283 | 117 |
| 5 s | 22:48:38.153780110 | 57584.173038 | 170 / 283 | 113 |
| 10 s | 22:48:32.700426672 | 52130.819600 | 181 / 283 | 102 |
| 30 s | 22:48:12.531782887 | 31962.175815 | 218 / 283 | 65 |

Each availability denominator is the same 287 recorded decisions. No formerly
available forecast becomes unavailable. No price changes where both policies
produce a price. The final JSON also preserves the exact monotonic-clock gains.

## Conditional price errors for the newly available rows

| Horizon | Graded / newly available | Median absolute error (USD) | p90 absolute error (USD) |
| --- | ---: | ---: | ---: |
| 5 s | 112 / 113 | 0.063986764757122459 | 0.280401822558352768 |
| 10 s | 100 / 102 | 0.2486580629376692905 | 0.532762112236755509 |
| 30 s | 62 / 65 | 1.4333200881121574455 | 3.753668344737957483 |

Prices and error calculations use `Decimal` with precision 80. The median
averages the two center values when the count is even; p90 uses nearest rank
`ceil(0.9*n)`. Grade targets are the earliest recorded matching TWAP source
events in the bounded receipt interval, with one unique recorded price. The
remaining 1 / 2 / 3 rows have no recorded matching target in that interval and
stay ungraded. There are no conflicting or clock-invalid targets in these
graded panels. Later conflicts outside the fixture remain outside this check.
Repeated source targets across decisions are correlated observations.

Input evidence was recovered from frozen current/slot inputs and recorded
target events, then selected by the original accepted sequence prefix at each
cutoff. Later target evidence is used only for grading. Neither the original
unavailable rows nor this replay establish that these new prices were actually
published, acknowledged, or visible before a target arrived. Runtime queue and
publication behavior is covered separately by the focused runtime tests.

## Reproduction and provenance

The full 14,346-row local audit was streamed once and verified against SHA256
`2353132cbc689a02d6eaf3009d93c6550f9693205ee6978c70c77a3faf51fdc3`.
The resulting 2,215,595-byte raw fixture is retained outside Git at
`dist/ghost-reconnect-recovery-2026-09-14/fixture.json` in the parent workspace,
with SHA256
`06e444dcf63efd66cb17506a918f1326bbf0349c0868209713578221b40c40ca`.
Its absence from this repository checkout is intentional; the manifest records
its original local path. Replaying that fixture does not read the full audit.

From the release checkout, use unused output paths:

```powershell
python research/spot_twap_response/reconnect_recovery/recorded_reconnect_check.py `
  --fixture-input ../ghost-reconnect-recovery-2026-09-14/fixture.json `
  --expected-fixture-sha256 06e444dcf63efd66cb17506a918f1326bbf0349c0868209713578221b40c40ca `
  --result-output ../ghost-reconnect-recovery-2026-09-14/reproduced_result.json `
  --manifest-output ../ghost-reconnect-recovery-2026-09-14/reproduced_manifest.json
```

The output pair must not already exist. Initial `result.json` / `manifest.json`
and intermediate `result_decimal80.json` / `manifest_decimal80.json` are
superseded; they are retained only under the raw fixture's `provisional/`
directory outside Git. Only the final pair above supports this report.
