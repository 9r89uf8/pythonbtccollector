-- Spot-to-TWAP response pilot, part 4 (read-only, research only). 2026-09-13.
-- Observer-visible delay between receiving a spot value and receiving the first TWAP value that
-- contains it, on the collector's local clock. Because TWAP(t) = mean of Chainlink spot stamped
-- [t-62 s, t-3 s] (parts 2-3), the spot value stamped s first enters the TWAP stamped s+3 s, so the
-- delay can be measured for EVERY second by pairing receipt clocks, without jump detection or fitting.
-- Section 17 checks the one thing the identity leaves open at sub-second resolution: whether a
-- one-second spot jump enters the TWAP fully at stamp s+3 or partly at s+3 and partly at s+4.
-- Week: 1788220800000 = 2026-09-01T00:00Z .. 1788825600000 = 2026-09-08T00:00Z.
-- Run: sudo -u postgres psql -X -At -v ON_ERROR_STOP=1 -d price_collector -f pilot_part4.sql

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '900s';

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

\echo === 15. Binance spot stamped s -> TWAP event stamped s+3 s and s+4 s: receipt-clock delay in ms, week (Chainlink reflects a Binance move within 0-1 stamp seconds, part 1 section 5) ===
\echo twap_stamp_offset_s|n_pairs|p10_ms|p50_ms|p90_ms|p99_ms
with b as (
  select sample_second_ms t, received_ms r from price_samples
  where instrument_id=1 and sample_second_ms between 1788220800000 and 1788825600000
), e as (
  select provider_event_ms t, min(received_wall_ns)/1000000 r from polymarket_twap_events
  where window_s=60 and provider_event_ms between 1788220800000 and 1788825600000 + 10000 group by 1
)
select k.k, count(*),
  percentile_cont(0.1) within group (order by e.r - b.r), percentile_cont(0.5) within group (order by e.r - b.r),
  percentile_cont(0.9) within group (order by e.r - b.r), percentile_cont(0.99) within group (order by e.r - b.r)
from b cross join (values (3),(4)) k(k) join e on e.t = b.t + k.k*1000 group by 1 order by 1;

\echo === 16. Delivery decomposition per feed, whole history (ms): publisher stamp minus observation stamp, receipt minus publisher stamp, receipt minus observation stamp ===
\echo feed|n|pub_minus_obs_p50|pub_minus_obs_p99|recv_minus_pub_p50|recv_minus_pub_p99|recv_minus_obs_p50|recv_minus_obs_p99
select 'chainlink_spot', count(*),
  percentile_cont(0.5) within group (order by provider_message_ms - provider_event_ms),
  percentile_cont(0.99) within group (order by provider_message_ms - provider_event_ms),
  percentile_cont(0.5) within group (order by received_ms - provider_message_ms),
  percentile_cont(0.99) within group (order by received_ms - provider_message_ms),
  percentile_cont(0.5) within group (order by received_ms - provider_event_ms),
  percentile_cont(0.99) within group (order by received_ms - provider_event_ms)
from price_samples where instrument_id=2 and provider_message_ms is not null
union all
select 'chainlink_twap60', count(*),
  percentile_cont(0.5) within group (order by provider_message_ms - provider_event_ms),
  percentile_cont(0.99) within group (order by provider_message_ms - provider_event_ms),
  percentile_cont(0.5) within group (order by received_wall_ns/1000000 - provider_message_ms),
  percentile_cont(0.99) within group (order by received_wall_ns/1000000 - provider_message_ms),
  percentile_cont(0.5) within group (order by received_wall_ns/1000000 - provider_event_ms),
  percentile_cont(0.99) within group (order by received_wall_ns/1000000 - provider_event_ms)
from polymarket_twap_events where window_s=60 and provider_message_ms is not null
union all
select 'binance_spot', count(*), null, null, null, null,
  percentile_cont(0.5) within group (order by received_ms - provider_event_ms),
  percentile_cont(0.99) within group (order by received_ms - provider_event_ms)
from price_samples where instrument_id=1;

\echo === 17. Sub-second onset check, week: single-second Chainlink jumps >= 3 bps between stamps s-1 and s. f3 = share of the jump missing from the TWAP first difference at s+3 (0 = fully entered at s+3, -1 = not yet entered); f4 = share appearing late at s+4. Both use the identity to remove the price leaving the window. ===
\echo n_jumps|f3_p10|f3_p50|f3_p90|f4_p10|f4_p50|f4_p90
with sp as (
  select sample_second_ms t, price p from price_samples
  where instrument_id=2 and sample_second_ms between 1788220800000 - 70000 and 1788825600000 + 10000
), tw as (
  select sample_second_ms t, price w from price_samples
  where instrument_id=4 and sample_second_ms between 1788220800000 and 1788825600000 + 10000
), j as (
  select s.t, (s.p - s1.p) as jmp,
    ((t3.w - t2.w) - (s.p - s60.p)/60) / ((s.p - s1.p)/60) as f3,
    ((t4.w - t3.w) - (sp1.p - s59.p)/60) / ((s.p - s1.p)/60) as f4
  from sp s
  join sp s1 on s1.t = s.t - 1000
  join sp s60 on s60.t = s.t - 60000
  join sp s59 on s59.t = s.t - 59000
  join sp sp1 on sp1.t = s.t + 1000
  join tw t2 on t2.t = s.t + 2000
  join tw t3 on t3.t = s.t + 3000
  join tw t4 on t4.t = s.t + 4000
  where abs(s.p - s1.p)/s1.p*1e4 >= 3
)
select count(*),
  round(percentile_cont(0.1) within group (order by f3)::numeric,3), round(percentile_cont(0.5) within group (order by f3)::numeric,3), round(percentile_cont(0.9) within group (order by f3)::numeric,3),
  round(percentile_cont(0.1) within group (order by f4)::numeric,3), round(percentile_cont(0.5) within group (order by f4)::numeric,3), round(percentile_cont(0.9) within group (order by f4)::numeric,3)
from j;

COMMIT;
