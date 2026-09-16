# Ghost forecast retention and ongoing accuracy

Owner direction recorded September 16, 2026 UTC: retain individual production
forecasts for approximately seven days, then delete them, and monitor accuracy
over time. External permanent archival is no longer a prerequisite for this
production path. The archive foundation remains optional, tested code.
The [peer-review verification](results/spot_twap_response/2026-09-16-retention-plan-review/REVIEW.md)
records reproduced canary metrics and the changes incorporated below.
The [completed compact-storage experiment](results/spot_twap_response/2026-09-16-compact-storage/FINDINGS.md)
now measures allocation and reuse. The owner subsequently directed implementation
without another study. Production now uses the compact JSON representation, a
separate continuous-mode capacity policy, seven-day expiry and cached monitoring.
The old canary budget cannot support this mode. The new limits are 6 GiB total
allocated ghost storage, warning at 5 GiB, admission pause at 5.5 GiB or 1,500,000
retained decisions, and the existing 10 GiB filesystem reserve. These guards
protect shared disk; they do not guarantee uninterrupted seven-day coverage.

This contract is implemented by the continuous runtime, compact store and accuracy
monitor. Both enable flags default to false. Installation requires schema before
the Chainlink/API restart; activation uses a separate continuous state directory.
The historical canary mode keeps its deadline and export rules. Completed
study/canary reports and exports remain historical evidence. See
[the operating procedure](OPERATIONS.md#continuous-ghost-retention-and-accuracy).

## Retention and capacity

- Retain compact individual forecast/result records for seven days from issuance.
  Include all six horizons, exact Decimal forecast and official target prices,
  anchor price, source target stamp, decision/publication/target receipt clocks,
  exact attempted-payload eligibility, quality, assumed-slot/carry counts,
  calculation/policy version, and missing/conflict status. Retain the original
  attempted-payload SHA-256, frozen/state hashes and record version as lineage,
  plus a hash of the compact record itself. Hash the exact attempted bytes before
  discarding them; a re-serialization is not a substitute. Those fields support
  re-scoring accuracy and confirmed lead throughout the retention window.
  The original hashes allow comparison with independently saved browser/observer
  bytes; they do not reconstruct deleted bytes or verify slot arithmetic alone.
- Preserve small hourly/daily aggregates beyond individual-row expiry so gradual
  deterioration remains visible. Continuous mode retains 90 days of summaries.
- Finalize idempotent summaries before deleting eligible terminal records in
  bounded batches. A summary failure defers deletion; it must not silently lose
  measurements. Record missing targets separately rather than guessing prices.
  An abnormal nonterminal record past the age boundary needs an explicit recovery
  status and must count as a monitoring failure.
- Preserve full durable inputs until the decision/result lifecycle has safely
  completed. The new compact format will not retain a complete 60-slot debug
  snapshot per forecast for a week. Existing immutable audit rows are not stripped
  in place; migration and retirement of legacy rows require their own tested path.
  Full slot-arithmetic replay ends when those verbose inputs are removed. The
  compact rows continue to support accuracy and timing checks, not full replay.
- Measure actual compact-row, index, update and maintenance costs before choosing
  a revised storage budget. The old format grew 90,710,016 allocated bytes in the
  latest canary hour. A simple seven-day extrapolation is 15,239,282,688 bytes
  (about 14.19 GiB), and 7,082 decisions/hour implies 1,189,776 decisions/week.
  These are illustrations from one hour, not a sustained capacity guarantee.
  Both exceed the current 1.5 GiB admission stop and 600,000-row cap. Merely
  changing an expiry constant to seven days cannot make continuous operation fit.
  Four days of the same format would still imply about 8.11 GiB and 679,872 rows.
  Compaction, measured allocation and guard changes are needed for either period;
  seven days remains the owner's requested target. The reproducible measurements
  below supersede earlier compact-JSON size and overhead estimates. They measure
  two explicit formats with indexes and TOAST, not all possible compact designs.
- That measurement is now complete for two explicit layouts. At the final refill,
  compact JSON allocated 196,067,328 bytes for 49,574 decisions; typed tables
  allocated 247,037,952 bytes including all six child horizons. At the observed
  rate those project to 4.38 and 5.52 GiB/week, respectively. These are scaled
  replicas, not observed seven-day capacity. Three turnover cycles demonstrated
  reuse but did not prove an allocation plateau. The post-cleanup conditional
  shared-disk remainder was 2.409 GiB before other future growth. Neither layout
  guarantees capacity under every traffic pattern. The implementation uses the
  measured JSON format with the separate limits above and pauses publication if
  allocated size or actual free disk reaches a guard. Additional normalization
  is not assumed in the capacity arithmetic.
- Capacity is shared with other collectors. The latest experiment's postflight
  had 11.64 GiB above the 10 GiB filesystem reserve before allowing for other
  collectors' remaining growth, not space reserved exclusively for ghost.
  Recheck current free space and other relations' remaining growth allowances;
  their existing allocations are already included in filesystem usage. Budget
  the measured steady-state allocated relation plus in-flight writes and safety
  margin. Keep allocated-byte and actual free-disk guards: estimated live bytes
  cannot replace checks for storage still allocated after deletion/vacuum.
- Replace the external-export-only deletion requirement for the new production
  policy explicitly, with tests. Preserve safeguards against resurrecting expired
  rows from old outboxes and release row capacity without losing reservations for
  in-flight writes. Verify actual filesystem reserves and physical space reuse.
  The legacy 96-hour minimum already equals four days; a four-day policy would
  not require changing that age constant, although the export prerequisite and
  other guards would change. Seven-day expiry requires an explicit seven-day
  eligibility rule in both maintenance and the database protection.

## Continuous scoring

Score each horizon against the exact official TWAP target stamp and first valid
observed target, once it arrives. Use only acknowledged, eligible members of the
actual attempted payload whose publication preceded that target's receipt.
Conflicted, missing and unmatched targets do not become fabricated price pairs.
Count them and unavailable/unpublished forecasts separately, with explicit
denominators, to prevent improved-looking accuracy caused by lost coverage.
Keep a secondary all-valid-published-pairs panel so loss of early publication
cannot silently remove difficult predictions from every accuracy view. Confirmed
Redis lead remains server evidence, not a claim about browser arrival.

The existing runtime matches targets in the half-open monotonic interval
`[decision_time, decision_time + 120 seconds)`, independently of forecast horizon.
Errors are calculated when valid targets arrive; normal terminalization occurs
at decision +120 seconds. Aggregate committed terminal versions, and finalize an
initial hourly summary only after every decision in the hour has crossed that
deadline and the persistence watermark has caught up. This is at least hour-end
+120 seconds, plus scheduling/persistence lag, not a required +150 seconds.
A +150-second schedule could be an explicit extra buffer, not a changed target
matching rule. Shutdown/recovery tails remain separately censored. Later conflict
or late-target annotations can revise terminal rows: apply idempotent corrections
to summaries while source records remain within retention, and label summary
revisions instead of treating initial terminalization as irreversible finality.

Maintain rolling one-hour, 24-hour and seven-day views, separated by horizon and
calculation/policy version. The compact daily/hourly summaries should expose:

| Measurement | Purpose |
| --- | --- |
| Mean, median and p90 absolute error in basis points; dollar error for display | Typical and tail forecast error across price levels |
| Signed mean error | Persistent overprediction or underprediction |
| Paired mean absolute error versus unchanged official TWAP | Value of the ghost relative to the existing live price |
| Confirmed publication lead and late-publication rate | Whether the forecast still precedes the actual report |
| Issued/published/scored counts, target match rate, freshness and unavailable counts | Whether the accuracy cohort is losing difficult observations |
| Source/receipt ages, pending/future slots, interior carry and known-input reconstruction error | Distinguish input stalls, ordinary forecast uncertainty and identity/alignment drift |

Record per-feed accepted-event receipt gaps before compacting inputs: maximum
monotonic inter-arrival gap, counts/durations over three and five seconds, observed
source-stamp holes and late arrivals, reconnect/session boundaries, and unavailable
decisions by reason. A stale-decision count alone cannot measure the duration of
a feed pause. Source-stamp holes mean locally unobserved stamps over a declared
interval; they do not establish that the provider never published those values.

For one- and two-second horizons, separately measure read-time usefulness:
whether an exact target was already observed by the cache/browser read, remained
unreceived, or was unknown. Fresh payload TTL and positive publication lead do
not establish remaining lead at read time. Keep all six accuracy panels, but make
short-horizon usefulness claims only from the appropriate read-side evidence.

The baseline must use the same target pairs as the ghost. If its absolute-error
sum is zero, relative improvement is undefined, not an artificial success/failure.
Also retain paired excess MAE (ghost absolute error minus baseline absolute error),
which remains defined when the baseline error is zero.
Accumulate exact Decimal error sums and counts. Combine sums/counts across time;
do not average hourly medians or percentiles. Longer-range percentile summaries
need an explicitly documented bounded histogram or retained finalized buckets.

Spot volatility naturally raises longer-horizon error. Compare with the paired
baseline and group by a frozen causal volatility/freshness/assumed-slot policy.
Report all-publication metrics alongside a fixed-clock sample to reveal changes
in publication cadence. Overlapping forecasts are correlated; thousands of
forecasts from a short burst do not establish thousands of independent trials.

## Warnings and deployment

Build a baseline over multiple complete days; the previous one-hour canaries
are references, not fixed universal error thresholds. Require sufficient elapsed
coverage and sample counts, and sustained deterioration across complete windows,
before flagging degraded accuracy. Freeze thresholds after baseline review and
use a recovery threshold to avoid repeated alert toggling. Feed stalls, unknown
targets and identity drift have separate status from ordinary forecast error.
Use hourly panels diagnostically and a completed rolling 24-hour view against a
frozen accepted baseline for slow accuracy deterioration, particularly at thirty
seconds. Four quarter-hours from one canary cannot establish a universal hourly
noise threshold or prove that 24 hours is sufficient; calibrate the warning's
persistence, minimum coverage and recovery band over multiple days.

Compute summaries off the feed and API request paths. Publish cached monitor
status for a read-only health endpoint so the frontend does not trigger database
scans or calculations. A sleeping laptop must not interrupt server-side scoring;
it still interrupts browser/tunnel measurement. This plan does not create a
Codex reminder or notification automation, or imply any monitoring is running.

The owner directed implementation without another study or test campaign. The
[implementation checkpoint](GHOST_TWAP_CONTINUOUS_CHECKPOINT.md) records the
shipped code, explicit thresholds, capacity policy and verification limits.
It takes precedence over earlier prospective language in this plan. No new
canary, PostgreSQL integration run or production activation is claimed. Read-time
short-horizon usefulness still requires separate browser evidence; the new
accuracy endpoint reports forecast performance and server-side lead only.
