# Combined one-hour ghost canary

Campaign: `1789425244002` to `1789428844002` (exclusive).
Verified 14346 exported rows; selected 7054 terminal decisions across 1 run IDs.

| Horizon | Calculated | ACK eligible | Confirmed early | Valid ACK pairs | Ghost median abs bp | Persistence median abs bp |
|---:|---:|---:|---:|---:|---:|---:|
| 1s | 6797 | 6486 | 6375 | 6385 | 0.00039198218251050839786034601521952352194770898387973272301302130484419864519006071 | 0.023520982303190918427477471177972669283891732025684358479473124250181813905206649 |
| 2s | 6801 | 6798 | 6697 | 6697 | 0.00039197351717544518287440770832777405352947660048670941956819450469145005537962031 | 0.051512423832150117075798406308046734261914989777008105280485416874110909038351760 |
| 3s | 6805 | 6805 | 6703 | 6703 | 0.00049054617858503617360365768740109247001354320952410805558730592195796189729662072 | 0.078580823404781282906452016460450804088413800054751892628325783297176400342032918 |
| 5s | 6813 | 6813 | 6692 | 6692 | 0.0033718363904723755962149835679845176541199272254692525460621208077631394040957850 | 0.13316307658355795686332952036552157413257262915588731203143167282947339081618396 |
| 10s | 6834 | 6834 | 6700 | 6700 | 0.022016576212825676360591862162249992170931686666579142158135576035614788084534497 | 0.26161090682358869056344213619904185878203534263783382580070665899168836571018820 |
| 30s | 6911 | 6911 | 6775 | 6775 | 0.26022830273359735341400896948949479590859297724565173281450072179824934522929411 | 0.68852423869035925550160309903287102075739572481901546304458198828146828040078045 |

Publication outcomes: `{"acknowledged": 7040, "expired_or_target_received": 14}`.

Denominators, excluded targets, withheld reasons and stage-specific latency sample sizes are in summary.json.

- Saved selected inputs validate saved arithmetic, not full input/rejected-event history or callback completeness.
- Calculated means are distinct from attempted, acknowledged and confirmed-early forecasts.
- Valid matched error cohorts exclude missing, restart-unmatched, conflicted, future-clock and causality-invalid targets.
- Intent-only recovery cannot establish the exact transmitted subset; uncertainty receives no confirmed credit.
- Recorded attempt payloads include any expired-before-attempt row; their final publication status distinguishes suppression from Redis acknowledgement.
- Latency is local monotonic timing. Included-event receipt is reported only when its exact sequence exists in the saved snapshot.
- Intent-to-attempt includes scheduling and durable-outbox work; it does not isolate fsync.
- Confirmed early means Redis acknowledgement before the recorded first official target receipt, not client/browser delivery.
- No continuous-time cache-coverage percentage is inferred from decision rows; use the separate observer.
- Error basis points divide by the official target value; descriptive quantiles use linear interpolation.
- Campaign row count is observed, not a promised timer rate; full export count is checked against its supplied verified manifest.
