# Static review of the part 4 receipt-delay claims

Reviewed the supplied [SQL](pilot_part4.sql), [output](../../results/spot_twap_response/2026-09-13-pilot/pilot_part4_output.txt), and current collector code. No database queries were run for this review.

The direct Chainlink pairing is sufficient for a useful **model-implied receipt lag**: under the sampled identity that spot stamped s first enters TWAP stamped s+3 seconds, section 14 pairs the retained spot row's receipt with the earliest durable TWAP receipt at that target stamp. Its 550,393 pairs have median **3.088 seconds**, p10–p90 **2.552–3.611 seconds**, and p99 **4.042 seconds**. This is a clear practical timing result for the matched retained data. It is not proof that the exact retained spot version was first incorporated in that exact earliest TWAP event. The output also preserves nine negative pairs, with a minimum of −32.829 seconds; their cause is not identified by this aggregate.

## Section 17 does not identify a literal incorporation fraction

The query selects 771 jumps of at least 3 bp with the required adjacent spot and TWAP rows. It computes deviations from the assumed rolling-mean first difference, divided by the jump contribution. These are **signed normalized residuals**, not independently observed fractions of an input entering the calculation.

The reported f3 median is 0.000 and f4 median is 0.000, rounded to three decimal places. This supports a residual centered near zero in the selected sample. It does not establish that every jump enters fully at s+3, or that there is no systematic subsecond split. The p10/p90 values are **−1.199/+1.257 for f3** and **−1.223/+1.332 for f4**. Those broad, non-fraction-like ranges show that a literal share interpretation needs assumptions the test has not established. Retained-price version differences, errors in the assumed incoming or outgoing contribution, and adjacent changes can all enter the residual. Opposing residuals can also leave a zero median.

A defensible description is: “The selected-jump residual has median near zero, consistent with the s+3 sampled alignment, but this test does not resolve subsecond input incorporation.” There is no need to resolve that stronger question to report section 14's conditional paired lag.

## A tight retained-row distribution does not bound overwrites

The current spot collector [coalesces pending history for an existing source second](../../price_collector/polymarket_chainlink_collector.py#L171), replacing the pending sample. The [database upsert](../../price_collector/db.py#L737) also replaces the price, provider event timestamp, receipt timestamp, and message timestamp on the same `(instrument_id, sample_second_ms)` key. Thus the table contains a surviving version, not a preserved first-arrival history or a version count.

Frequent duplicates or revisions arriving close together could still produce a narrow receipt-delay distribution. Its tightness therefore cannot demonstrate that overwrites are rare. Their incidence needs retained event history or independently recorded counters. Section 17 additionally uses materialized TWAP rows, whereas section 14 uses the earliest durable receipt at each TWAP stamp; it does not check that the materialized and earliest-event versions match. These limitations qualify exact input-version causality without invalidating the retained-data pairing.

## The message timestamp is the RTDS envelope timestamp

For both [spot](../../price_collector/polymarket_chainlink_collector.py#L574) and [TWAP](../../price_collector/polymarket_twap.py#L291), the parser sets `provider_message_ms` from the outer **`message.timestamp`**. `provider_event_ms` separately comes from **`payload.timestamp`**. The local receive clock is captured after the WebSocket receive completes, before payload parsing.

The supported label is **RTDS envelope/message timestamp**. The code does not establish that it is Chainlink calculation completion, underlying report creation, or publication completion. Section 16's differences should therefore be named “envelope minus payload timestamp” and “local receipt minus envelope timestamp.” They do not isolate computation time or pure network transit. Section 16 also uses whole-history populations, unlike the fixed-week pairing, and its per-feed marginal quantiles cannot be added or subtracted to obtain paired delay quantiles.

Minor query-scope note: section 14 uses inclusive week endpoints, and section 17's padded joins can admit jump stamps from two seconds before the nominal start through six seconds after the nominal end. This is a boundary-description issue, separate from the main timing interpretation.

**Recommended conclusion:** Retained Chainlink spot and its model-aligned TWAP target arrive about three seconds apart at the collector in this matched sample. Direct pairing establishes that conditional timing distribution; it does not establish exact input-version causality, overwrite frequency, or the publisher's subsecond calculation mechanics.
