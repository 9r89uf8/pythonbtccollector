# Browser delivery canary

Complete capture: 11242 records, 1020118.5 ms.

Browser lead is handler receipt of the exact target minus handler receipt of its forecast. It is separate from collector receipt and Redis acknowledgement.

SSE connection errors: 0. Failed GETs: 11 (11 AbortError records).

API timing uses 2031 unique envelopes with valid clocks; 0 have missing or invalid timing metadata. Median read→fanout: 0.7596070 ms; median fanout→send: 0.0603360 ms.

## All admissions

| Horizon | Admitted | Matched | Censored | Median lead ms | Median absolute error USD |
|---:|---:|---:|---:|---:|---:|
| 1 | 1481 | 1453 | 28 | 695.7000000030 | 0.0048609957602005010 |
| 2 | 1601 | 1575 | 26 | 1755.7999999970 | 0.0048609957524966610 |
| 3 | 1605 | 1574 | 31 | 2720.2500000000 | 0.0048609957671411630 |
| 5 | 1609 | 1574 | 35 | 4717.0499999970 | 0.0271647626266730560 |
| 10 | 1619 | 1581 | 38 | 9725.5999999940 | 0.2023024678586905390 |
| 30 | 1658 | 1617 | 41 | 29731.7999999970 | 2.3014112039001542720 |

## After the first 65000 ms of browser capture

| Horizon | Admitted | Matched | Censored | Median lead ms | Median absolute error USD |
|---:|---:|---:|---:|---:|---:|
| 1 | 1481 | 1453 | 28 | 695.7000000030 | 0.0048609957602005010 |
| 2 | 1601 | 1575 | 26 | 1755.7999999970 | 0.0048609957524966610 |
| 3 | 1605 | 1574 | 31 | 2720.2500000000 | 0.0048609957671411630 |
| 5 | 1609 | 1574 | 35 | 4717.0499999970 | 0.0271647626266730560 |
| 10 | 1619 | 1581 | 38 | 9725.5999999940 | 0.2023024678586905390 |
| 30 | 1643 | 1602 | 41 | 29731.3999999985 | 2.3032210607441336800 |

## Browser probe coverage

Usability includes unknown and missing planned slots in its denominator. These are recorded point observations, not continuous-availability measurements.

| Horizon | Usable after the initial browser cutoff | Unknown | Planned | Usable fraction |
|---:|---:|---:|---:|---:|
| 1 | 7502 | 3 | 8350 | 0.89844311377245508982035928143712574850299401197604790419161676646706586826347305 |
| 2 | 7779 | 3 | 8350 | 0.93161676646706586826347305389221556886227544910179640718562874251497005988023952 |
| 3 | 7798 | 3 | 8350 | 0.93389221556886227544910179640718562874251497005988023952095808383233532934131737 |
| 5 | 7817 | 3 | 8350 | 0.93616766467065868263473053892215568862275449101796407185628742514970059880239521 |
| 10 | 7869 | 3 | 8350 | 0.94239520958083832335329341317365269461077844311377245508982035928143712574850299 |
| 30 | 7982 | 3 | 8350 | 0.95592814371257485029940119760479041916167664670658682634730538922155688622754491 |

## Limits

- Lead measures browser handler entry, not rendering or collector/Redis acknowledgement.
- Targets are exact anchors delivered over this SSE stream; skipped or absent targets are censored.
- Admitted/excluded denominators cover unique delivered producer decisions; null envelopes do not reveal omitted forecasts.
- Server remaining lifetime is not a new TTL at browser receipt; this analysis does not certify browser display freshness.
- Positive lead among matched pairs is guaranteed by excluding targets already observed at forecast receipt.
- GET snapshots are timing diagnostics only and never substitute for SSE target anchors.
- The post_warmup panel begins at warmup_ms after browser capture start; it does not certify that producer history has warmed up.
- Accuracy is conditional on observed, nonconflicting, later targets; it is not a trading or settlement result.
- Probe usability is the browser classifier recorded at actual handler times; this analyzer does not independently reconstruct clock-bracket or selected-payload eligibility. Multiple probes in one slot are combined conservatively, and point samples do not prove continuous availability.
