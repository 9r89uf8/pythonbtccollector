# Continuous ghost retention and accuracy implementation

This checkpoint implements the owner's seven-day forecast retention and ongoing
accuracy request. It follows the measured compact-storage work without another
study or live canary. Production activation is not part of this checkpoint.

## Implemented behavior

- `GHOST_TWAP_CONTINUOUS=true` with `GHOST_TWAP_ENABLED=true` selects runtime
  `ghost-continuous-v1`, retaining engine contract 4 and the existing Decimal
  forecast calculation. A separate state directory records the actual first
  start and preserves recovery across restarts. Both flags default to false.
- Full inputs remain durable before publication. After terminalization and the
  120-second matching window, maintenance commits compact evidence and its
  hourly contribution atomically before deleting the verbose row. Runtime-owned
  rows are excluded until persistence and outbox removal finish.
- Compact forecasts/results expire seven days after issuance; accuracy and
  accepted-feed health summaries remain 90 days. Existing canary rows retain
  their export requirements and are excluded from automatic compaction.
- Compact evidence retains exact Decimal prices, attempted membership, target
  observations, publication clocks and original byte hashes. It supports
  accuracy/lead scoring, but cannot reproduce discarded slot inputs.
- Late/conflicting targets update their compact record and subtract/add the
  hourly contribution in the same transaction. Immutable first observations and
  forecast values remain protected. Expired outboxes cannot resurrect old rows;
  unreconciled expired evidence raises an explicit recovery fault.
- Allocated ghost storage warns at 5 GiB, pauses forecast admission at 5.5 GiB,
  and has a 6 GiB budget with bounded in-flight reservations. The retained-decision
  cap is 1,500,000 and the PostgreSQL filesystem reserve remains 10 GiB.
  Maintenance continues during a capacity pause. Deletion does not imply that
  allocated bytes shrink, and these limits do not guarantee uninterrupted
  seven-day coverage on the shared disk.

## Accuracy API

`GET /forecasts/chainlink-twap/accuracy` reads one Redis key. The independent
monitor refreshes it every minute with a 180-second TTL; requests do not query
PostgreSQL or calculate forecasts or historical metrics. Live/SSE routes accept
the new runtime version and keep their existing freshness rules.

The cached panels cover completed 1-hour, 24-hour and 7-day windows, bounded by
matching and persistence watermarks. Each horizon has confirmed-early and
all-valid-published cohorts, paired against unchanged official TWAP. Missing,
unavailable, unpublished, conflicted and clock-invalid observations remain
explicit counts. Decimal sums produce means; merged histograms produce quantile
brackets rather than averages of hourly medians. Confirmed Redis lead is not a
browser measurement.

A policy/cohort baseline is frozen from the first available qualifying three
full UTC days in the bounded monitoring snapshot, with at least 3,000 scored
pairs and adequate coverage. A monitor returning after a long outage does not
claim that this is the earliest qualifying window in all history. Baselines and
warning counters persist across restarts. After the baseline, current 24-hour
windows need at least 1,000 pairs and adequate coverage. Deterioration requires
MAE above 125% of baseline and paired excess MAE above baseline plus 0.01 bp for
three hourly evaluations; recovery needs MAE at most 110% or excess at most
baseline plus 0.005 bp for three evaluations. These are operational heuristics,
not statistical significance claims. Panel coverage concerns issued forecasts,
not independently measured browser or wall-clock key availability.

Feed health records receipt gaps over three/five seconds, maximum gaps, local
source-stamp holes and connection ends separately. Whole gap intervals belong
to the ending receipt hour; overlapping thresholds are not additive. Bounded
in-memory history eviction is distinguished from skipped health hours.

## Verification and deployment status

Static Python compilation and whitespace checks completed. Existing schema
assertions/disposable-database setup were adjusted for the new tables and atomic
schema transaction. No further full test suite, live canary or database migration
was run for this checkpoint, as requested. New runtime/database integration is
therefore implemented but has not been demonstrated against PostgreSQL here.

At implementation completion the droplet and production flags were unchanged.
The subsequent [deployment record](results/spot_twap_response/2026-09-16-continuous-deployment/README.md)
confirms installation and schema migration with the producer still disabled.
For future installations after release through GitHub,
apply `schema.sql` before restarting `price-collector-polymarket-chainlink` and
`price-api`; install with the producer disabled. See the exact environment keys,
capacity limits and commands in
[OPERATIONS.md](OPERATIONS.md#continuous-ghost-retention-and-accuracy).
