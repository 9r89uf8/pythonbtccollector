-- Spot-to-TWAP response pilot, part 5 (read-only, research only). 2026-09-13.
-- Rolling "ghost TWAP": at an instant whose latest TWAP stamp is w, where will the TWAP stamped w+h be
-- if Chainlink spot stays at its current value? Uses the verified identity
--   TWAP(t) = mean of Chainlink spot stamped [t-62 s, t-3 s], silent seconds carried forward.
-- The window for TWAP(w+h) is [w+h-62, w+h-3]. Stamps up to w are already known (63-h of them);
-- the remaining h-3 are assumed equal to the current spot (the carried-forward value at w).
-- Instants: every 60 s of the pilot week; provider clocks; horizons 5, 10 and 30 s.
-- Graded against the TWAP actually stamped w+h, and for side calls against the Price to Beat of the
-- market containing w+h. "Persistence" = assuming the TWAP stays where it is now.
-- Week: 1788220800000 = 2026-09-01T00:00Z .. 1788825600000 = 2026-09-08T00:00Z.
-- Run: sudo -u postgres psql -X -At -v ON_ERROR_STOP=1 -d price_collector -f pilot_part5.sql

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '900s';

\echo === 18. Rolling ghost TWAP at horizon h from the latest TWAP stamp w (week, provider clock, every 60 s) ===
\echo h_s|n_instants|ghost_err_p50_bps|ghost_err_p90_bps|ghost_err_p99_bps|persistence_err_p50_bps|persistence_err_p90_bps|side_changes|ghost_shows_new_side|ghost_false_alarms|ghost_side_wrong_total
with inst as (
  select generate_series(1788220800000, 1788825600000, 60000) w
), spot as (
  select i.w, u.u, ps.price as p
  from inst i
  cross join lateral generate_series(i.w - 59000, i.w, 1000) u(u)
  left join lateral (
    select price from price_samples
    where instrument_id=2 and sample_second_ms <= u.u and sample_second_ms > u.u - 600000
    order by sample_second_ms desc limit 1
  ) ps on true
), h as (select unnest(array[5,10,30]) h),
agg as (
  select s.w, h.h,
    sum(s.p) filter (where s.u >= s.w + h.h*1000 - 62000) as known_sum,
    count(*) filter (where s.u >= s.w + h.h*1000 - 62000) as n_known,
    count(s.p) filter (where s.u >= s.w + h.h*1000 - 62000) as n_present,
    max(s.p) filter (where s.u = s.w) as s_now
  from spot s cross join h
  group by 1, 2
), g as (
  select a.w, a.h, (a.known_sum + a.s_now * (a.h - 3)) / 60 as ghost,
    tn.price as twap_now, tf.price as twap_future, r.chainlink_open_price as k
  from agg a
  join lateral (select price from price_samples where instrument_id=4 and sample_second_ms = a.w) tn on true
  join lateral (select price from price_samples where instrument_id=4 and sample_second_ms = a.w + a.h*1000) tf on true
  join polymarket_btc_5m_resolutions r
    on r.market_id = (a.w + a.h*1000) / 300000 and r.resolution_status = 'resolved'
  where a.n_present = a.n_known and a.s_now is not null
)
select h, count(*),
  round(percentile_cont(0.5) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.9) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.99) within group (order by abs(ghost - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(twap_now - twap_future)/twap_future*1e4)::numeric,3),
  round(percentile_cont(0.9) within group (order by abs(twap_now - twap_future)/twap_future*1e4)::numeric,3),
  sum(case when (twap_now >= k) <> (twap_future >= k) then 1 else 0 end),
  sum(case when (twap_now >= k) <> (twap_future >= k) and (ghost >= k) = (twap_future >= k) then 1 else 0 end),
  sum(case when (twap_now >= k) = (twap_future >= k) and (ghost >= k) <> (twap_future >= k) then 1 else 0 end),
  sum(case when (ghost >= k) <> (twap_future >= k) then 1 else 0 end)
from g group by 1 order by 1;

COMMIT;
