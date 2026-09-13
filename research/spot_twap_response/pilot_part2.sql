-- Spot-to-TWAP response pilot, part 2 (read-only, research only). 2026-09-13.
-- Follow-up to pilot.sql. Its nine-shift profile (section 1) put the minimum residual at
-- shift -3 s, not the -2 s that sections 2 and 7 assumed. This file re-runs those two
-- checks with both alignments side by side and adds the paired side-call comparison.
-- Alignment convention, explicit: shift a means the 60 constituent spot stamps for the
-- TWAP stamped t are [t + a - 59 s, t + a]. So a = -3 s -> [t-62 s, t-3 s]; a = -2 s -> [t-61 s, t-2 s].
-- Run: sudo -u postgres psql -X -At -v ON_ERROR_STOP=1 -d price_collector -f pilot_part2.sql
-- Clocks: provider stamps throughout. Instruments: 2 = Chainlink spot, 4 = Chainlink 60 s TWAP.

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '1500s';

\echo === 8. Alignment profile, whole history: residual of TWAP(t) vs the 60-sample mean of Chainlink spot stamped [t+a-59 s, t+a], complete windows only ===
\echo a_ms|n|p50_abs_bps|p90_abs_bps|p99_abs_bps|max_abs_bps
with c as (
  select sample_second_ms t,
         avg(price) over (order by sample_second_ms range between 59000 preceding and current row) avg60,
         count(*) over (order by sample_second_ms range between 59000 preceding and current row) n60
  from price_samples where instrument_id=2
), w as (select sample_second_ms t, price w from price_samples where instrument_id=4)
select s.sh, count(*),
  round(percentile_cont(0.5) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,4),
  round(percentile_cont(0.9) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,4),
  round(percentile_cont(0.99) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,4),
  round(max(abs(w.w-c.avg60)/w.w*1e4)::numeric,3)
from w join (values (-4000),(-3000),(-2000),(-1000),(0)) s(sh) on true
join c on c.t = w.t + s.sh where c.n60=60 group by s.sh order by s.sh;

\echo === 9. Identity per UTC day, whole history: a = -3 s (constituents [t-62 s, t-3 s]) beside a = -2 s ([t-61 s, t-2 s]) ===
\echo utc_day|n|p50_bps_a3|p99_bps_a3|max_bps_a3|p50_bps_a2|p99_bps_a2|max_bps_a2
with c as (
  select sample_second_ms t,
         avg(price) over (order by sample_second_ms range between 59000 preceding and current row) avg60,
         count(*) over (order by sample_second_ms range between 59000 preceding and current row) n60
  from price_samples where instrument_id=2
), w as (select sample_second_ms t, price w from price_samples where instrument_id=4)
select to_char(to_timestamp(w.t/1000) at time zone 'UTC','MM-DD'), count(*),
  round(percentile_cont(0.5) within group (order by abs(w.w-c3.avg60)/w.w*1e4)::numeric,4),
  round(percentile_cont(0.99) within group (order by abs(w.w-c3.avg60)/w.w*1e4)::numeric,3),
  round(max(abs(w.w-c3.avg60)/w.w*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(w.w-c2.avg60)/w.w*1e4)::numeric,4),
  round(percentile_cont(0.99) within group (order by abs(w.w-c2.avg60)/w.w*1e4)::numeric,3),
  round(max(abs(w.w-c2.avg60)/w.w*1e4)::numeric,3)
from w join c c3 on c3.t = w.t - 3000 and c3.n60=60
       join c c2 on c2.t = w.t - 2000 and c2.n60=60
group by 1 order by 1;

\echo === 10. Projection pilot (week 09-01..09-08, provider clock) under both alignments, with paired side-call comparison against the raw TWAP ===
\echo a = -3 s: known constituents at decision D = E - T are the stamps in [E-62 s, D], count 63-T; unknown T-3 filled with current spot.
\echo a = -2 s: known are the stamps in [E-61 s, D], count 62-T; unknown T-2. Up to 3 missing known stamps allowed; official close = the TWAP event stamped at E.
\echo tsec|n|proj3_err_p50_bps|proj2_err_p50_bps|twap_err_p50_bps|proj3_wrong|proj2_wrong|twap_wrong|spot_wrong|both_ok|proj3_only_ok|twap_only_ok|both_wrong
with m as (
  select r.market_id, mw.market_end_ms e, r.chainlink_open_price k, r.chainlink_close_price cl
  from polymarket_btc_5m_resolutions r join market_windows mw using (market_id)
  where r.resolution_status='resolved' and mw.market_start_ms between 1788220800000 and 1788825600000-300000
), t as (select unnest(array[60,30,15,10,5,3]) tsec),
p as (
  select m.market_id, t.tsec, m.k, m.cl,
    (select price from price_samples where instrument_id=4 and sample_second_ms = m.e - t.tsec*1000) w,
    (select price from price_samples where instrument_id=2 and sample_second_ms = m.e - t.tsec*1000) s,
    (select sum(price) from price_samples where instrument_id=2 and sample_second_ms between m.e-62000 and m.e - t.tsec*1000) sum3,
    (select count(*) from price_samples where instrument_id=2 and sample_second_ms between m.e-62000 and m.e - t.tsec*1000) n3,
    (select sum(price) from price_samples where instrument_id=2 and sample_second_ms between m.e-61000 and m.e - t.tsec*1000) sum2,
    (select count(*) from price_samples where instrument_id=2 and sample_second_ms between m.e-61000 and m.e - t.tsec*1000) n2
  from m cross join t
), q as (
  select *, (sum3/n3 * (63 - tsec) + s * (tsec - 3)) / 60 as proj3,
            (sum2/n2 * (62 - tsec) + s * (tsec - 2)) / 60 as proj2
  from p where w is not null and s is not null and n3 >= (63 - tsec) - 3 and n2 >= (62 - tsec) - 3
)
select tsec, count(*),
  round(percentile_cont(0.5) within group (order by abs(proj3-cl)/cl*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(proj2-cl)/cl*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(w-cl)/cl*1e4)::numeric,3),
  sum(case when (proj3>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (proj2>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (w>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (s>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (proj3>=k) = (cl>=k) and (w>=k) = (cl>=k) then 1 else 0 end),
  sum(case when (proj3>=k) = (cl>=k) and (w>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (proj3>=k) <> (cl>=k) and (w>=k) = (cl>=k) then 1 else 0 end),
  sum(case when (proj3>=k) <> (cl>=k) and (w>=k) <> (cl>=k) then 1 else 0 end)
from q group by 1 order by 1 desc;

COMMIT;
