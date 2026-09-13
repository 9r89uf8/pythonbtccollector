\echo === 14. Chainlink spot stamped s -> TWAP event stamped s+3 s: receipt-clock delay in ms, week; spot receipt = price_samples.received_ms, TWAP receipt = earliest received_wall_ns for that stamp ===
\echo n_pairs|p10_ms|p50_ms|p90_ms|p99_ms|min_ms|max_ms|negative_pairs
with s as (
  select sample_second_ms t, received_ms r from price_samples
  where instrument_id=2 and sample_second_ms between 1788220800000 and 1788825600000
), e as (
  select provider_event_ms t, min(received_wall_ns)/1000000 r from polymarket_twap_events
  where window_s=60 and provider_event_ms between 1788220800000 and 1788825600000 + 10000 group by 1
)
select count(*),
  percentile_cont(0.1) within group (order by e.r - s.r), percentile_cont(0.5) within group (order by e.r - s.r),
  percentile_cont(0.9) within group (order by e.r - s.r), percentile_cont(0.99) within group (order by e.r - s.r),
  min(e.r - s.r), max(e.r - s.r), count(*) filter (where e.r - s.r < 0)
from s join e on e.t = s.t + 3000;

