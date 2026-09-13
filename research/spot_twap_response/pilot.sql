-- Spot-to-TWAP response pilot (read-only, research only). 2026-09-13.
-- Reproduces the ad hoc checks behind the merged protocol discussed on 2026-09-13:
--   1. identity + stamp offset, 2. per-day identity, 3. step response,
--   4. Binance/Chainlink return cross-correlation, 5. Binance-move transfer,
--   6. delivery timing, 7. settlement projection pilot.
-- Run: sudo -u postgres psql -X -At -v ON_ERROR_STOP=1 -d price_collector -f pilot.sql
-- Instruments: 1 = Binance spot (1 s ticker), 2 = Chainlink spot (RTDS), 4 = Chainlink 60 s TWAP (RTDS).
-- Clocks: provider stamps (sample_second_ms / provider_event_ms), not local receipt.
-- Week bounds: 1788220800000 = 2026-09-01T00:00Z, 1788825600000 = 2026-09-08T00:00Z.
-- Prices stay NUMERIC; only dimensionless returns are cast to float8 for corr().

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '1500s';

\echo === 1. Identity: TWAP(t) vs trailing 60-sample mean of Chainlink spot ending at t+shift; residual in bps by shift (week 09-01..09-08) ===
\echo shift_ms|n|p50_abs_bps|p90_abs_bps|p99_abs_bps
with c as (
  select sample_second_ms t,
         avg(price) over (order by sample_second_ms range between 59000 preceding and current row) avg60,
         count(*) over (order by sample_second_ms range between 59000 preceding and current row) n60
  from price_samples where instrument_id=2 and sample_second_ms between 1788220800000-70000 and 1788825600000+10000
), w as (select sample_second_ms t, price w from price_samples where instrument_id=4 and sample_second_ms between 1788220800000 and 1788825600000)
select s.sh, count(*),
  round(percentile_cont(0.5) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,3),
  round(percentile_cont(0.9) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,3),
  round(percentile_cont(0.99) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,3)
from w join (values (-4000),(-3000),(-2000),(-1000),(0),(1000),(2000),(3000),(4000)) s(sh) on true
join c on c.t = w.t + s.sh where c.n60=60 group by s.sh order by s.sh;

\echo === 2. Identity per UTC day, whole history, shift -2 s: TWAP(t) vs mean of Chainlink spot stamped [t-61 s, t-2 s] ===
\echo utc_day|n|p50_bps|p99_bps|max_bps
with c as (
  select sample_second_ms t,
         avg(price) over (order by sample_second_ms range between 59000 preceding and current row) avg60,
         count(*) over (order by sample_second_ms range between 59000 preceding and current row) n60
  from price_samples where instrument_id=2
), w as (select sample_second_ms t, price w from price_samples where instrument_id=4)
select to_char(to_timestamp(w.t/1000) at time zone 'UTC','MM-DD'), count(*),
  round(percentile_cont(0.5) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,3),
  round(percentile_cont(0.99) within group (order by abs(w.w-c.avg60)/w.w*1e4)::numeric,3),
  round(max(abs(w.w-c.avg60)/w.w*1e4)::numeric,2)
from w join c on c.t = w.t - 2000 where c.n60=60 group by 1 order by 1;

\echo === 3. Step response: Chainlink spot moves >= 2 bps over 2 s that hold within 1.5 bps for 62 s; TWAP path normalized by the move (1.0 = full), whole history ===
\echo n_events|at_0s|at_5s|at_10s|at_20s|at_30s|at_45s|at_60s|at_62s|at_65s|median_at_30s|median_at_62s
with x as (
  select c.sample_second_ms t, c.price c, w.price w
  from price_samples c join price_samples w on w.instrument_id=4 and w.sample_second_ms=c.sample_second_ms
  where c.instrument_id=2
), y as (
  select t, c, w, lag(c,2) over (order by t) c_m2, lag(t,2) over (order by t) t_m2, lag(w,1) over (order by t) w_m1,
    max(c) over (order by t range between 1000 following and 62000 following) cmax,
    min(c) over (order by t range between 1000 following and 62000 following) cmin,
    count(*) over (order by t range between 1000 following and 65000 following) nfwd,
    lead(w,5) over (order by t) w5, lead(w,10) over (order by t) w10, lead(w,20) over (order by t) w20, lead(w,30) over (order by t) w30,
    lead(w,45) over (order by t) w45, lead(w,60) over (order by t) w60, lead(w,62) over (order by t) w62, lead(w,65) over (order by t) w65
  from x
), e as (
  select *, (c-c_m2) d from y
  where t_m2 = t-2000 and nfwd = 65 and abs(c-c_m2)/c_m2*1e4 >= 2 and (cmax-c)/c*1e4 <= 1.5 and (c-cmin)/c*1e4 <= 1.5
)
select count(*),
  round(avg((w-w_m1)/d)::numeric,3), round(avg((w5-w_m1)/d)::numeric,3), round(avg((w10-w_m1)/d)::numeric,3),
  round(avg((w20-w_m1)/d)::numeric,3), round(avg((w30-w_m1)/d)::numeric,3), round(avg((w45-w_m1)/d)::numeric,3),
  round(avg((w60-w_m1)/d)::numeric,3), round(avg((w62-w_m1)/d)::numeric,3), round(avg((w65-w_m1)/d)::numeric,3),
  round(percentile_cont(0.5) within group (order by (w30-w_m1)/d)::numeric,3),
  round(percentile_cont(0.5) within group (order by (w62-w_m1)/d)::numeric,3)
from e;

\echo === 4. Cross-correlation of 1 s simple returns: Binance at t vs Chainlink at t+k, whole history ===
\echo k_seconds|corr|n
with x as (
  select b.sample_second_ms t, b.price b, c.price c
  from price_samples b join price_samples c on c.instrument_id=2 and c.sample_second_ms=b.sample_second_ms where b.instrument_id=1
), r as (
  select t, ((b - lag(b) over (order by t))/lag(b) over (order by t))::float8 rb,
            ((c - lag(c) over (order by t))/lag(c) over (order by t))::float8 rc,
            t - lag(t) over (order by t) dt from x
), rr as (select * from r where dt=1000)
select ks.k, round(corr(a.rb, b.rc)::numeric,3), count(*)
from rr a cross join (values (-2),(-1),(0),(1),(2)) ks(k) join rr b on b.t = a.t + ks.k*1000 group by 1 order by 1;

\echo === 5. Binance 2 s move >= thr bps, non-overlapping (>62 s apart): share reflected by Chainlink spot and kept by Binance, baseline = 2 s before the move, whole history ===
\echo thr_bps|n_events|cl_0s|cl_1s|cl_2s|cl_5s|cl_10s|cl_30s|cl_60s|bin_1s|bin_2s|bin_5s|bin_10s|bin_30s|bin_60s
with x as (
  select b.sample_second_ms t, b.price b, c.price c
  from price_samples b join price_samples c on c.instrument_id=2 and c.sample_second_ms=b.sample_second_ms where b.instrument_id=1
), y as (
  select t, b, c, lag(b,2) over (order by t) b0, lag(c,2) over (order by t) c0, lag(t,2) over (order by t) t_m2, lead(t,60) over (order by t) t60,
    lead(c,1) over (order by t) c1, lead(c,2) over (order by t) c2, lead(c,5) over (order by t) c5, lead(c,10) over (order by t) c10, lead(c,30) over (order by t) c30, lead(c,60) over (order by t) c60,
    lead(b,1) over (order by t) b1, lead(b,2) over (order by t) b2, lead(b,5) over (order by t) b5, lead(b,10) over (order by t) b10, lead(b,30) over (order by t) b30, lead(b,60) over (order by t) b60
  from x
), e as (
  select *, (b-b0) d, abs(b-b0)/b0*1e4 d_bps, t - lag(t) over (order by t) gap_prev
  from (select * from y where t_m2 = t-2000 and t60 = t+60000 and abs(b-b0)/b0*1e4 >= 3) z
), ne as (select * from e where gap_prev is null or gap_prev > 62000)
select th.thr, count(*),
  round(avg((c-c0)/d)::numeric,3), round(avg((c1-c0)/d)::numeric,3), round(avg((c2-c0)/d)::numeric,3), round(avg((c5-c0)/d)::numeric,3), round(avg((c10-c0)/d)::numeric,3), round(avg((c30-c0)/d)::numeric,3), round(avg((c60-c0)/d)::numeric,3),
  round(avg((b1-b0)/d)::numeric,3), round(avg((b2-b0)/d)::numeric,3), round(avg((b5-b0)/d)::numeric,3), round(avg((b10-b0)/d)::numeric,3), round(avg((b30-b0)/d)::numeric,3), round(avg((b60-b0)/d)::numeric,3)
from ne join (values (3),(6),(10)) th(thr) on d_bps >= th.thr group by 1 order by 1;

\echo === 6. Delivery timing of TWAP events (ms), whole history: publisher minus observation, receipt minus publisher, receipt minus observation ===
\echo n|pub_minus_obs_p50|pub_minus_obs_p99|recv_minus_pub_p50|recv_minus_pub_p99|recv_minus_obs_p50|recv_minus_obs_p99
select count(*),
  percentile_cont(0.5) within group (order by provider_message_ms - provider_event_ms),
  percentile_cont(0.99) within group (order by provider_message_ms - provider_event_ms),
  percentile_cont(0.5) within group (order by received_wall_ns/1000000 - provider_message_ms),
  percentile_cont(0.99) within group (order by received_wall_ns/1000000 - provider_message_ms),
  percentile_cont(0.5) within group (order by received_wall_ns/1000000 - provider_event_ms),
  percentile_cont(0.99) within group (order by received_wall_ns/1000000 - provider_event_ms)
from polymarket_twap_events where window_s=60 and provider_message_ms is not null;

\echo === 7. Projection pilot (week 09-01..09-08, provider clock): at T s before close, projected settlement = (sum of known final-window Chainlink stamps + current spot x remaining)/60; error vs official close and side accuracy vs Price to Beat, compared with raw TWAP and raw spot ===
\echo tsec|n|proj_err_p50_bps|proj_err_p90_bps|twap_err_p50_bps|spot_err_p50_bps|proj_side_ok_pct|twap_side_ok_pct|spot_side_ok_pct
with m as (
  select r.market_id, mw.market_end_ms e, r.chainlink_open_price k, r.chainlink_close_price cl
  from polymarket_btc_5m_resolutions r join market_windows mw using (market_id)
  where r.resolution_status='resolved' and mw.market_start_ms between 1788220800000 and 1788825600000-300000
), t as (select unnest(array[60,30,15,10,5,3]) tsec),
p as (
  select m.market_id, t.tsec, m.k, m.cl,
    (select price from price_samples where instrument_id=4 and sample_second_ms = m.e - t.tsec*1000) w,
    (select price from price_samples where instrument_id=2 and sample_second_ms = m.e - t.tsec*1000) s,
    (select sum(price) from price_samples where instrument_id=2 and sample_second_ms between m.e-61000 and m.e - t.tsec*1000) known_sum,
    (select count(*) from price_samples where instrument_id=2 and sample_second_ms between m.e-61000 and m.e - t.tsec*1000) known_n
  from m cross join t
), q as (
  select *, (known_sum/known_n * (62 - tsec) + s * (tsec - 2)) / 60 as proj
  from p where w is not null and s is not null and known_n >= (62 - tsec) - 3
)
select tsec, count(*),
  round(percentile_cont(0.5) within group (order by abs(proj-cl)/cl*1e4)::numeric,2),
  round(percentile_cont(0.9) within group (order by abs(proj-cl)/cl*1e4)::numeric,2),
  round(percentile_cont(0.5) within group (order by abs(w-cl)/cl*1e4)::numeric,2),
  round(percentile_cont(0.5) within group (order by abs(s-cl)/cl*1e4)::numeric,2),
  round(100.0*avg(case when (proj>=k) = (cl>=k) then 1 else 0 end),2),
  round(100.0*avg(case when (w>=k) = (cl>=k) then 1 else 0 end),2),
  round(100.0*avg(case when (s>=k) = (cl>=k) then 1 else 0 end),2)
from q group by 1 order by 1 desc;

COMMIT;
