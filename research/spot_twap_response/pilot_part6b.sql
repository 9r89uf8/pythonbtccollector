-- Spot-to-TWAP response pilot, part 6b (read-only, research only). 2026-09-13.
-- Same receipt-clock ghost replay as pilot_part6.sql, with the side-call counts split by whether the
-- target stamp w+h lies in a different five-minute market than the anchor stamp w. At a boundary the
-- Price to Beat of the target market is the TWAP stamped at the boundary itself, so the anchor's side
-- against it is close to a coin flip; those cases are not genuine within-market crossings.
-- Run: sudo -u postgres psql -X -At -v ON_ERROR_STOP=1 -d price_collector -f pilot_part6b.sql

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '900s';

\echo === 20. Receipt-clock ghost side calls split by boundary crossing (week, every 60 s of wall time) ===
\echo h_s|target_in_new_market|n|side_changes|ghost_shows_new_side|ghost_false_alarms|ghost_side_wrong_total|ghost_err_p50_bps
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
  select t.tau, t.w, t.twap_now, u.u, ps.price as p
  from tw t
  cross join lateral generate_series(t.w - 57000, t.w + 27000, 1000) u(u)
  left join lateral (
    select price from price_samples
    where instrument_id=2 and sample_second_ms <= u.u and sample_second_ms > u.u - 600000
      and received_ms <= t.tau
    order by sample_second_ms desc limit 1
  ) ps on true
), h as (select unnest(array[5,10,30]) h),
agg as (
  select s.tau, s.w, s.twap_now, h.h,
    sum(s.p) filter (where s.u between s.w + h.h*1000 - 62000 and s.w + h.h*1000 - 3000) as win_sum,
    count(*) filter (where s.u between s.w + h.h*1000 - 62000 and s.w + h.h*1000 - 3000) as n_slots,
    count(s.p) filter (where s.u between s.w + h.h*1000 - 62000 and s.w + h.h*1000 - 3000) as n_filled
  from slots s cross join h
  group by 1, 2, 3, 4
), g as (
  select a.*, a.win_sum / 60 as ghost, tf.price as twap_future, r.chainlink_open_price as k,
    (a.w / 300000) <> ((a.w + a.h*1000) / 300000) as new_market
  from agg a
  join lateral (
    select min(price) as price from polymarket_twap_events
    where window_s=60 and symbol='btc/usd' and provider_event_ms = a.w + a.h*1000
  ) tf on true
  join polymarket_btc_5m_resolutions r
    on r.market_id = (a.w + a.h*1000) / 300000 and r.resolution_status = 'resolved'
  where a.n_slots = 60 and a.n_filled = 60 and tf.price is not null
)
select h, new_market, count(*),
  sum(case when (twap_now >= k) <> (twap_future >= k) then 1 else 0 end),
  sum(case when (twap_now >= k) <> (twap_future >= k) and (ghost >= k) = (twap_future >= k) then 1 else 0 end),
  sum(case when (twap_now >= k) = (twap_future >= k) and (ghost >= k) <> (twap_future >= k) then 1 else 0 end),
  sum(case when (ghost >= k) <> (twap_future >= k) then 1 else 0 end),
  round(percentile_cont(0.5) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3)
from g group by 1, 2 order by 1, 2;

COMMIT;
