\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='2s';
SET LOCAL TIME ZONE 'UTC';
SELECT 'H3_AUDIT_META',(extract(epoch FROM transaction_timestamp())*1000)::bigint,
  current_setting('transaction_read_only'),current_setting('transaction_isolation'),current_setting('statement_timeout');

\echo === 19. Receipt-clock ghost TWAP (week, every 60 s of wall time): information received by tau only ===
\echo h_s|n|ghost_err_p50_bps|ghost_err_p90_bps|ghost_err_p99_bps|persistence_err_p50_bps|persistence_err_p90_bps|assumed_slots_avg|assumed_slots_max|target_arrival_p10_ms|target_arrival_p50_ms|target_arrival_p90_ms|side_changes|ghost_shows_new_side|ghost_false_alarms|ghost_side_wrong_total
with inst as (
  select generate_series(1788220800000, 1788825600000, 60000) tau
), tw as (
  select i.tau, e.provider_event_ms as w, e.price as twap_now
  from inst i
  join lateral (
    select provider_event_ms, price from polymarket_twap_events
    where window_s=60 and symbol='btc/usd'
      and provider_event_ms between i.tau - 70000 and i.tau
      and received_wall_ns <= i.tau::bigint * 1000000
    order by received_wall_ns desc limit 1
  ) e on true
), slots as (
  select t.tau, t.w, t.twap_now, u.u, ps.price as p, ps.sample_second_ms as src_t
  from tw t
  cross join lateral generate_series(t.w - 57000, t.w + 27000, 1000) u(u)
  left join lateral (
    select price, sample_second_ms from price_samples
    where instrument_id=2 and sample_second_ms <= u.u and sample_second_ms > u.u - 600000
      and received_ms <= t.tau
    order by sample_second_ms desc limit 1
  ) ps on true
), h as (select unnest(array[5,10,30]) h),
agg as (
  select s.tau, s.w, s.twap_now, h.h,
    sum(s.p) filter (where s.u between s.w + h.h*1000 - 62000 and s.w + h.h*1000 - 3000) as win_sum,
    count(*) filter (where s.u between s.w + h.h*1000 - 62000 and s.w + h.h*1000 - 3000) as n_slots,
    count(s.p) filter (where s.u between s.w + h.h*1000 - 62000 and s.w + h.h*1000 - 3000) as n_filled,
    max(s.src_t) as s_last
  from slots s cross join h
  group by 1, 2, 3, 4
), g as (
  select a.*, a.win_sum / 60 as ghost,
    greatest(0, (a.w + a.h*1000 - 3000 - a.s_last) / 1000) as assumed_slots,
    tf.price as twap_future, tf.recv_ms - a.tau as target_arrival_ms,
    r.chainlink_open_price as k
  from agg a
  join lateral (
    select min(price) as price, min(received_wall_ns)/1000000 as recv_ms from polymarket_twap_events
    where window_s=60 and symbol='btc/usd' and provider_event_ms = a.w + a.h*1000
  ) tf on true
  join polymarket_btc_5m_resolutions r
    on r.market_id = (a.w + a.h*1000) / 300000 and r.resolution_status = 'resolved'
  where a.n_slots = 60 and a.n_filled = 60 and tf.price is not null
)
select h, count(*),
  round(percentile_cont(0.5) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.9) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.99) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(twap_now - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.9) within group (order by abs(twap_now - twap_future)/twap_future*1e4)::numeric,3),
  round(avg(assumed_slots)::numeric,2), max(assumed_slots),
  percentile_cont(0.1) within group (order by target_arrival_ms),
  percentile_cont(0.5) within group (order by target_arrival_ms),
  percentile_cont(0.9) within group (order by target_arrival_ms),
  sum(case when (twap_now >= k) <> (twap_future >= k) then 1 else 0 end),
  sum(case when (twap_now >= k) <> (twap_future >= k) and (ghost >= k) = (twap_future >= k) then 1 else 0 end),
  sum(case when (twap_now >= k) = (twap_future >= k) and (ghost >= k) <> (twap_future >= k) then 1 else 0 end),
  sum(case when (ghost >= k) <> (twap_future >= k) then 1 else 0 end)
from g group by 1 order by 1;

COMMIT;
