-- Exact pilot_part3b.sql section 13 only; retrospective source-clock reproduction.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '20s';
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
SELECT 'H3_COHORT_META', count(*),
  count(*) FILTER (WHERE r.chainlink_open_price IS NULL),
  count(*) FILTER (WHERE r.chainlink_close_price IS NULL),
  count(*) FILTER (WHERE r.winner IS NULL)
FROM polymarket_btc_5m_resolutions r JOIN market_windows mw USING (market_id)
WHERE r.resolution_status='resolved'
  AND mw.market_start_ms BETWEEN 1788220800000 AND 1788825600000-300000;

\echo === 13. Projection pilot with carry-forward inputs (week, provider clock, a = -3 s): known constituents [E-62 s, D] carried forward, unknown T-3 filled with the last-known spot at D; raw TWAP = last TWAP sample at or before D; no market excluded for missing samples ===
\echo tsec|n|projff_err_p50_bps|projff_err_p90_bps|twap_err_p50_bps|projff_wrong|twap_wrong|spot_wrong|both_ok|projff_only_ok|twap_only_ok|both_wrong|rows_using_carried_value
with m as (
  select r.market_id, mw.market_end_ms e, r.chainlink_open_price k, r.chainlink_close_price cl
  from polymarket_btc_5m_resolutions r join market_windows mw using (market_id)
  where r.resolution_status='resolved' and mw.market_start_ms between 1788220800000 and 1788825600000-300000
), sec as (
  select m.market_id, m.e, m.k, m.cl, u.u
  from m cross join lateral generate_series(m.e - 62000, m.e - 3000, 1000) u(u)
), ffs as (
  select sec.*,
    ps.price as p_ff, ps.sample_second_ms as src_t
  from sec
  left join lateral (
    select price, sample_second_ms from price_samples
    where instrument_id=2 and sample_second_ms <= sec.u and sample_second_ms > sec.u - 600000
    order by sample_second_ms desc limit 1
  ) ps on true
), t as (select unnest(array[60,30,15,10,5,3]) tsec),
agg as (
  select f.market_id, t.tsec, f.e, f.k, f.cl,
    sum(f.p_ff) filter (where f.u <= f.e - t.tsec*1000) as known_sum,
    count(*) filter (where f.u <= f.e - t.tsec*1000 and f.p_ff is null) as missing_known,
    max(f.p_ff) filter (where f.u = f.e - t.tsec*1000) as s,
    bool_or(f.u = f.e - t.tsec*1000 and f.src_t <> f.u) as s_is_carried
  from ffs f cross join t
  group by 1,2,3,4,5
), p as (
  select a.*, w.price as w
  from agg a
  left join lateral (
    select price from price_samples
    where instrument_id=4 and sample_second_ms <= a.e - a.tsec*1000 and sample_second_ms > a.e - a.tsec*1000 - 600000
    order by sample_second_ms desc limit 1
  ) w on true
), q as (
  select *, (known_sum + s * (tsec - 3)) / 60 as projff
  from p where w is not null and s is not null and missing_known = 0
)
select tsec, count(*),
  round(percentile_cont(0.5) within group (order by abs(projff-cl)/cl*1e4)::numeric,3),
  round(percentile_cont(0.9) within group (order by abs(projff-cl)/cl*1e4)::numeric,3),
  round(percentile_cont(0.5) within group (order by abs(w-cl)/cl*1e4)::numeric,3),
  sum(case when (projff>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (w>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (s>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (projff>=k) = (cl>=k) and (w>=k) = (cl>=k) then 1 else 0 end),
  sum(case when (projff>=k) = (cl>=k) and (w>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when (projff>=k) <> (cl>=k) and (w>=k) = (cl>=k) then 1 else 0 end),
  sum(case when (projff>=k) <> (cl>=k) and (w>=k) <> (cl>=k) then 1 else 0 end),
  sum(case when s_is_carried then 1 else 0 end)
from q group by 1 order by 1 desc;

COMMIT;
