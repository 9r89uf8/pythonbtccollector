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
