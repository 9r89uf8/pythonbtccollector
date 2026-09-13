# Receipt-clock projection: design review

Design review only, 2026-09-13. No database query or empirical run was performed for this review. The supplied part-3/3b SQL, output files, notes, merged protocol and opening-reference extraction were inspected.

## What the new evidence supports

Part 3 reports materially smaller reconstruction errors from carrying retained spot prices across missing source seconds than from averaging only retained rows. This supports that **sampled reconstruction convention**. It does not prove that a missing sample means unchanged underlying spot, or establish the publisher's exact duration-weighted calculation. Chainlink's underlying report windows can widen; their semantics are not established by our retained one-second rows. A last-value hold is duration-weighted only for the explicitly assumed piecewise-constant path and interval convention.

The provider-clock forecast is a **retrospective benchmark**, not a ceiling on receipt-clock accuracy. A fixed estimator with delayed inputs can accidentally improve individual forecasts; changed eligibility and opening references can also change the comparison. More information does not make this particular estimator a proven optimal upper bound.

Part 3b's `rows_using_carried_value` counts only whether the spot value at D was carried. It does not count decisions using a carried value anywhere in the reconstructed final window. Its ten-minute searches also permit old raw-TWAP/current-spot values. Retain those details when reproducing it.

## Frozen recipe for the next bounded pilot

- Development/reproduction cohort: market starts in `[2026-09-01T00:00:00Z, 2026-09-08T00:00:00Z)`, six checkpoints T=60/30/15/10/5/3. Preserve every market/checkpoint row and its availability flags; do not build the cohort by dropping unresolved outcomes or unavailable predictions. Validate the 60-second market/rule identity.
- Use the scheduled closing stamp C=E only under the declared settlement convention; do not choose a closing stamp after inspecting reconciliation. Freeze the empirical discrete alignment a=-3. The 60 candidate source slots are E-62s through E-3s, inclusive. Keep the discrete endpoints explicit rather than silently converting them into a differently indexed continuous interval.
- Input eligibility is receipt-based: spot `received_ms <= D`, durable TWAP `received_wall_ns <= D*1,000,000`. Source timestamps locate observations in the reconstructed source path; they do not establish receipt availability. Preserve source/key mismatches, non-integer source stamps and clock anomalies.
- For **current S and W**, select the latest retained receipt first. Then evaluate source-clock validity, source age and receipt age. A newer-received stale/future-source record must not disappear and reveal an older fresh candidate. Equal-receipt conflicting states require an ambiguity flag; arbitrary connection UUID ordering is not evidence of order.
- The primary panel retains the provider pilot's explicit 600-second bound, with ages and gaps reported. A separate fixed panel requires current S and W source and receipt ages each within `[0,3000]` ms, inclusive. Select once before applying that diagnostic. This second panel does not certify that every historical carried slot is fresh.

For each decision D and historical candidate slot u<=D:

1. Among retained spot rows already received by D, select the greatest source slot at or before u, with the same per-slot strict bound as part 3b: `source_slot > u-600000`.
2. An available exact-slot row is an **observed retained input**. An earlier row is a **historical carried estimate**; record its original source/receipt clocks and carry age `u-source_slot`.
3. No qualifying seed means **missing/expired**, not zero and not a silently manufactured current-price value. A later-arriving revision is unavailable at D even if an overwritten earlier version might once have existed.
4. Rows received after u but by D may inform the historical estimate at D. Requiring receipt by u would answer a different question and discard information actually available at the decision.

For candidate slots u>D, use the selected current S as an explicit **future flat-price extrapolation**, never an observed constituent. The projection is the sum of the 60 observed/carried/forecast slot values divided by 60; incomplete required slots make the prediction unavailable. Record counts that sum to 60: exact observed, historical carried, future extrapolated and unavailable. Also record maximum historical carry age, current S/W ages, any-carried flag, carried-at-D flag, and relevant TWAP gap/session evidence. Carrying a value during a gap never establishes that the source was unchanged.

Extract sufficient seed history for the **earliest candidate slot minus 600 seconds**. A source lower bound of D-600 seconds alone can omit a needed seed for an earlier slot. Future rows in a frozen extract may support later audit labels, but never cutoff-time selection.

## Current-state extraction guard

A source-indexed bounded extract can omit an extremely delayed or future-stamped record that was actually the latest receipt. Before certifying latest-receipt selection, validate coverage for the relevant receipt interval, in the same read-only snapshot. For a global source export, check whether any expected-feed record received between the first D-600s and last D lies outside that export's source bounds. Zero such records establishes this particular coverage condition.

For per-cut indexed source ranges, derive bounds from a checked receive-minus-source range. If receipt is in `(D-600s,D]` and its source lag lies in `[Lmin,Lmax]`, the candidate source lies in `(D-600s-Lmax,D-Lmin]`. Merely restricting source to D plus/minus 600 seconds is insufficient. Use a bounded statement timeout; an incomplete guard leaves an explicitly bounded diagnostic, not a certified full latest-receipt reconstruction. Do not reuse an old snapshot's guard as current evidence.

## Opening reference and grading

Reuse the causal **opening-reference logic**, not the old study's broader checkpoint/source-market assumptions: exact expected 60-second TWAP events with `provider_event_ms = market_start_ms` and receipt by D. Count distinct E18 values received by D. One distinct value supplies K and its earliest receipt; zero is missing; multiple values are a conflict. Later events/conflicts cannot alter earlier-cutoff information. The official reconciled opening value is audit-only; equality with it must not be a feature-eligibility filter.

Grade against the verified official winner with the correct rule identity. Record prediction equality separately and apply Up-on-equality only after verifying that contract rule. Unknown outcomes are ungraded. Compare projection, current TWAP and current spot on identical eligible rows, with full-cohort coverage, exact paired correctness counts and daily summaries. Price-error grading requires an official final price; its absence must not erase an otherwise valid winner-only comparison. Persist differences between causal K and official K for audit.

The week already informed development. This run measures retained-history receipt-clock performance, with upsert limitations; it is neither an untouched evaluation nor a complete reconstruction of all live spot arrivals.
