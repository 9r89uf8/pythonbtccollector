# Ghost forecast retention and ongoing accuracy

Owner direction recorded September 16, 2026 UTC: retain individual production
forecasts for approximately seven days, then delete them, and monitor accuracy
over time. External permanent archival is no longer a prerequisite for this
production path. The archive foundation remains optional, tested code.

This is the next implementation contract to refine and validate. It changes no
running service, SQL deletion guard, one-hour producer deadline or stored data.
The producer remains disabled. Completed study/canary reports and their existing
exports remain historical evidence; this plan concerns ongoing production data.

## Retention and capacity

- Retain compact individual forecast/result records for seven days from issuance.
  Include all six horizons, exact Decimal forecast and official target prices,
  anchor price, source target stamp, decision/publication/target receipt clocks,
  exact attempted-payload eligibility, quality, assumed-slot/carry counts,
  calculation/policy version, and missing/conflict status. Those fields support
  re-scoring accuracy and confirmed lead throughout the retention window.
- Preserve small hourly/daily aggregates beyond individual-row expiry so gradual
  deterioration remains visible. Proposed bounded default: 90 days of summaries.
  This is a design default, not an enabled retention setting.
- Finalize idempotent summaries before deleting eligible terminal records in
  bounded batches. A summary failure defers deletion; it must not silently lose
  measurements. Record missing targets separately rather than guessing prices.
  An abnormal nonterminal record past the age boundary needs an explicit recovery
  status and must count as a monitoring failure.
- Preserve full durable inputs until the decision/result lifecycle has safely
  completed. The new compact format will not retain a complete 60-slot debug
  snapshot per forecast for a week. Existing immutable audit rows are not stripped
  in place; migration and retirement of legacy rows require their own tested path.
- Measure actual compact-row, index, update and maintenance costs before choosing
  a revised storage budget. The old format grew 90,710,016 allocated bytes in the
  latest canary hour. A simple seven-day extrapolation is 15,239,282,688 bytes
  (about 14.19 GiB), and 7,082 decisions/hour implies 1,189,776 decisions/week.
  These are illustrations from one hour, not a sustained capacity guarantee.
  Both exceed the current 1.5 GiB admission stop and 600,000-row cap. Merely
  changing an expiry constant to seven days cannot make continuous operation fit.
- Replace the external-export-only deletion requirement for the new production
  policy explicitly, with tests. Preserve safeguards against resurrecting expired
  rows from old outboxes and release row capacity without losing reservations for
  in-flight writes. Verify actual filesystem reserves and physical space reuse.

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

Compute summaries off the feed and API request paths. Publish cached monitor
status for a read-only health endpoint so the frontend does not trigger database
scans or calculations. A sleeping laptop must not interrupt server-side scoring;
it still interrupts browser/tunnel measurement. This plan does not create a
Codex reminder or notification automation, or imply any monitoring is running.

Next checkpoint: implement compact records and seven-day expiry, deterministic
scoring/summary aggregation and monitoring health with focused tests; prove
restart/late-result handling, expiry, cap behavior and space reuse in a disposable
database. Then perform a reviewed bounded live run and repeat the awake-laptop
browser check before enabling continuous production.
