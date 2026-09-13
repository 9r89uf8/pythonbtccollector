-- Exact pilot_part2.sql section 10 only; source-clock retrospective reproduction.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '3s';
SET LOCAL TIME ZONE 'UTC';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM instruments i JOIN providers p USING (provider_id)
    WHERE i.instrument_id=2 AND p.provider_code='polymarket_chainlink_rtds'
      AND i.symbol='BTCUSD' AND i.stream_name='crypto_prices_chainlink:btc/usd'
  ) OR NOT EXISTS (
    SELECT 1 FROM instruments i JOIN providers p USING (provider_id)
    WHERE i.instrument_id=4 AND p.provider_code='polymarket_chainlink_twap_rtds'
      AND i.symbol='BTCUSD_TWAP_60S'
      AND i.stream_name='crypto_prices_twap_sixty:btc/usd'
  ) THEN
    RAISE EXCEPTION 'Original pilot instrument IDs do not match expected sources';
  END IF;
END
$$;


SELECT 'H3_AUDIT_META',
  (extract(epoch FROM transaction_timestamp())*1000)::bigint,
  current_setting('transaction_read_only'),
  current_setting('transaction_isolation'),
  current_setting('statement_timeout');

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
