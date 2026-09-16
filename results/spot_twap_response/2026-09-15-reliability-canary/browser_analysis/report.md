# Browser delivery canary

Complete capture: 9214 records, 3720001.6999999881 ms.

Browser lead is handler receipt of the exact target minus handler receipt of its forecast. It is separate from collector receipt and Redis acknowledgement.

SSE connection errors: 28. Failed GETs: 28 (0 AbortError records).

API timing uses 1244 unique envelopes with valid clocks; 0 have missing or invalid timing metadata. Median read→fanout: 0.7167755 ms; median fanout→send: 0.0575170 ms.

## All admissions

| Horizon | Admitted | Matched | Censored | Median lead ms | Median absolute error USD |
|---:|---:|---:|---:|---:|---:|
| 1 | 1003 | 988 | 15 | 663.95000000295 | 0.0072629609948839250 |
| 2 | 1061 | 1044 | 17 | 1729.79999999705 | 0.0072629610027000430 |
| 3 | 1066 | 1045 | 21 | 2714.69999998800 | 0.0072629610147277970 |
| 5 | 1070 | 1039 | 31 | 4703.50000000000 | 0.0658806279189695360 |
| 10 | 1081 | 1038 | 43 | 9733.50000000000 | 0.3434310858701076640 |
| 30 | 1120 | 1035 | 85 | 29704.09999999400 | 4.0812427175332817920 |

## After the first 100519 ms of browser capture

| Horizon | Admitted | Matched | Censored | Median lead ms | Median absolute error USD |
|---:|---:|---:|---:|---:|---:|
| 1 | 995 | 980 | 15 | 663.24999999995 | 0.0072629609968083095 |
| 2 | 1052 | 1035 | 17 | 1728.20000001790 | 0.0072629610027000430 |
| 3 | 1055 | 1034 | 21 | 2712.15000000595 | 0.0072629610151745225 |
| 5 | 1055 | 1024 | 31 | 4696.15000000595 | 0.0654949003489937975 |
| 10 | 1055 | 1012 | 43 | 9726.99999998515 | 0.3266441298849710080 |
| 30 | 1055 | 984 | 71 | 29694.84999999410 | 3.9751477795470311625 |

## Browser probe coverage

Usability includes unknown and missing planned slots in its denominator. These are recorded point observations, not continuous-availability measurements.

| Horizon | Usable after the initial browser cutoff | Unknown | Planned | Usable fraction |
|---:|---:|---:|---:|---:|
| 1 | 5183 | 29420 | 34994 | 0.14811110476081613990969880550951591701434531633994399039835400354346459393038807 |
| 2 | 5326 | 29420 | 34994 | 0.15219751957478424872835343201691718580328056238212264959707378407727038920957878 |
| 3 | 5334 | 29420 | 34994 | 0.15242613019374749957135508944390466937189232439846830885294621935188889523918386 |
| 5 | 5334 | 29420 | 34994 | 0.15242613019374749957135508944390466937189232439846830885294621935188889523918386 |
| 10 | 5334 | 29420 | 34994 | 0.15242613019374749957135508944390466937189232439846830885294621935188889523918386 |
| 30 | 5334 | 29420 | 34994 | 0.15242613019374749957135508944390466937189232439846830885294621935188889523918386 |

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
