# Follow-up review: freshness, publication losses and speed

This review checks the peer's interpretation against the same verified 7,292-row
export as the [one-hour canary report](GHOST_TWAP_CANARY_RESULTS.md). Its SHA-256
remains `d7536bcafcf62170abbf9329f6b720d7df35fb710b9f79f64ec96ca2bda05b80`.
No runtime policy, production setting or canary evidence was changed.

**Conclusion:** the peer correctly identifies freshness-driven availability as
a frontend priority that deserved more emphasis. Their price calculations use a
different population. Their explanations of the 418 withheld publications and
the supposed need to abandon durability for speed are not established.

## Freshness: verified, and worth addressing first

After excluding the first 60 seconds, **771 of 7,174 decisions (10.747%)** had at
least one unavailable horizon. This reproduces the quoted 10.7%. Full warm-up
actually finished at 64.949 seconds; starting then gives **761 of 7,164 (10.623%)**.
These are proportions of issued decisions, not wall-clock downtime.

Of the 771 decisions, **757 failed a three-second source-age check while both
feeds' receipt-age checks still passed**. Post-minute failure counts by feed
were 470 spot source-only, 581 TWAP source-only, two spot source-and-receipt,
and one TWAP source-and-receipt. These overlap between feeds and must not be
summed as distinct unavailable decisions.

The mechanism is supported. Among unique current inputs in this export, median
delivery lags were 1.562 seconds for spot and 1.623 seconds for TWAP; p90 was
2.033 and 2.098 seconds. At decision time, selected source ages had medians
1.692 and 1.911 seconds and p90 2.678 and 2.882 seconds. A source-age cap of three
seconds leaves little room for the next report to arrive. Receipt age and source
age measure different things.

Using the stored Redis attempt/acknowledgement clocks and exact TTL formula, the
review reconstructs **669–680 TTL-induced logical cache-gap intervals** between writes
after 65 seconds and before the deadline; at least 669 have positive lower bounds.
Their summed absence lies between about **211 and 216 seconds** under stable
host-clock assumptions and a one-millisecond Redis clock-granularity margin.
The median positive lower-bound gap is 230 ms, with a longest bound of 6.74 seconds.
These bounds also assume sequential single-writer publication. They are not
sampled Redis-key observations, actual deletion times, all possible cache outages
or measured browser flicker. A frontend using this key would need to handle these
gaps; there was no frontend running in this canary.

### Proposed split: promising offline, not yet a live acceptance result

A diagnostic on the **same saved decisions**, keeping all other rules fixed,
uses a five-second source-age limit and three-second wall/monotonic receipt-age
limits. It preserves the ten-second historical carry limit and all conflict,
future-clock, missing-input and already-received-target rules.

| Population | Original: any horizon unavailable | Proposed split on the same decisions |
|---|---:|---:|
| After 60 seconds | 771 / 7,174 (10.747%) | 18 / 7,174 (0.251%) |
| After full warm-up | 761 / 7,164 (10.623%) | 8 / 7,164 (0.112%) |

The diagnostic first reproduces all **43,752 original horizon availability
decisions with zero mismatches**. It then removes only the proposed source-age
restriction; future slots withheld solely for current-spot staleness use the
saved current spot when it passes the new rules. Historical slot selection and
carry remain fixed. This recovers all horizons in 753 previously unavailable
decisions in either population.

This does **not** prove that live unavailability would be 0.251%, nor that the
newly admitted forecasts have acceptable error. Changing expiry changes timer
decisions, publication selection and cache coverage. A replay must grade those
additional forecasts and measure coverage before a new prospective test.

It is also not merely one constant in the current code. `current_max_age_ms`
currently controls source age, wall receipt age, monotonic receipt age, and
`valid_until` construction. Raising it to 5,000 alone would relax receipt guards
too. Separate versioned policy fields, consistent expiry calculation and focused
tests are needed. The production policy remains unchanged.

[Reproducible freshness analysis](research/spot_twap_response/canary_peer_review/freshness_review.py)
and [results](results/spot_twap_response/2026-09-14-canary-peer-review/freshness_review.json).

## Price figures: the difference is publication eligibility

The peer's numbers exactly match **all clean matched forecasts**, including ones
that never reached Redis. The original report uses acknowledged publications with
clean target matches. Neither changes the arithmetic or target-matching rules.

| Horizon | All clean matches | Acknowledged clean matches | All: median / p90 error ($) | Acknowledged: median / p90 error ($) |
|---|---:|---:|---:|---:|
| 5 s | 6,234 | 5,806 | 0.0550 / 0.5517 | 0.0569 / 0.5589 |
| 10 s | 6,236 | 5,812 | 0.3495 / 1.3408 | 0.3522 / 1.3413 |
| 30 s | 6,253 | 5,828 | 3.2362 / 10.0545 | 3.2394 / 10.1016 |

Beat-persistence rates are 97.5/93.3/80.5% for the broader group and
97.6/93.4/80.5% for acknowledged publications. The original figures need no
correction: delivered forecasts are the relevant primary frontend population.
Both panels are already preserved in the [numerical summary](results/spot_twap_response/2026-09-14-live-canary/summary.json).

Higher median errors than the development-week replay are not evidence of a code
defect. Different dates, price paths, decision frequency, policies and publication
selection are confounded. The 30-second median is about 45% higher numerically;
the code's frozen-window arithmetic still independently reproduces exactly.

## The 418 withheld batches were not ordinary coalescing

The audit has **418 `preempted_after_spool` records and zero
`coalesced_before_publication` records**. The former means the post-write
eligibility check failed. The latter is the separate status for a pending
publication replaced by a newer decision.

The runtime rejects the **entire batch when any forecast target has arrived**.
Thus a one-second target arriving during the durable-write stage can prevent
otherwise useful five-, ten- and thirty-second forecasts from being delivered.
This is a concrete behavior to evaluate, not evidence of lost constituent input.

The exact post-spool check time is not stored. Using the next publication intent
as an upper bound, 375 records have target receipt as the only recorded candidate
trigger, 15 have validity expiry only, 27 have both, and the final record has no
later-intent bound. These identify candidates within known timing bounds, not
exact per-stage measurements. All 418 batches had an available horizon when
calculated.

In all 375 target-only bounded cases, the **one-second target arrived first**;
the 5/10/30-second forecasts were available and their targets remained unreceived
through the next-intent upper bound. The final record separately records the
canary deadline; it still lacks an exact post-spool completion timestamp.

[Code/timing review](research/spot_twap_response/canary_peer_review/publication_review.py)
and [bounded classification](results/spot_twap_response/2026-09-14-canary-peer-review/publication_review.json).

## Durability and short horizons: claims need qualification

The 32.88-ms median calculation-complete-to-attempt stage includes serialization,
async-lock waiting, executor scheduling, existing-file checks, file writing and
fsync, rename, directory fsync, and the eligibility recheck. There are no separate
timestamps measuring how much each contributes. Fsync is already offloaded to
an executor thread; publication still waits for durability, as designed.

The recorded clocks allow one further split: calculation complete to publication
intent has a median of 7.25 ms, and intent to attempt has a median of 24.28 ms.
The latter includes the durable-write path and waiting, not just fsync. These
component medians do not sum to the stage median.

Publishing before durable completion would weaken reproducibility after a crash.
That tradeoff is real, but this run does not show it is **necessary** to improve
speed or meet 10 ms. Instrumentation should precede that conclusion. Reducing
serialization, contention or durable-write overhead might help while retaining
the ordering guarantee; achieving the objective remains unproven.

The one-second ghost beats persistence in 78.1% of clean published pairs, as the
peer says. That does not establish that usefulness begins at three seconds:
the two-second result wins **97.2%**, with median confirmed server lead **1.839 s**
and median absolute error about **$0.016**. Browser-visible usefulness at any
horizon still needs measurement. No trading conclusion follows from a beat rate.

The roughly 87 MiB end-of-run relation and existing cap are consistent with a
multi-day run ending early under a roughly similar growth rate. One hour is not
a measured multi-day storage trajectory, and no cap increase is proposed.

## Updated work order

1. Review and test separate source/receipt freshness limits and matching expiry;
   grade newly admitted forecasts and measure cache coverage in replay.
2. Review batch publication when a short-horizon target arrives, preserving
   explicit per-horizon availability and audit evidence for any revised behavior.
3. Instrument the durable publication stage and optimize it while preserving the
   current durability guarantee unless the owner explicitly chooses a tradeoff.
4. Run a bounded validation of the revised policy, then measure SSE/browser
   delivery in C. A faster frontend alone will not fix producer-side expiry gaps.

This changes the priority from the initial short summary. It does not authorize
or silently enact a looser production freshness policy.
