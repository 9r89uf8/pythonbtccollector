-- Reproduce ONLY pilot.sql section 7 over its exact fixed September 1--8 week.
-- Preserve the pilot's source-clock selection, reconciled strike, missing-slot
-- mean imputation, and inferred side labels. This is NOT a receipt-causal test.
-- percentile_cont converts finalized dimensionless error-in-bps values to
-- double precision; it is preserved solely to reproduce the original report.
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

COPY (
WITH m AS (
  SELECT r.market_id, mw.market_end_ms e,
    r.chainlink_open_price k, r.chainlink_close_price cl,
    r.winner, r.resolution_type
  FROM polymarket_btc_5m_resolutions r JOIN market_windows mw USING (market_id)
  WHERE r.resolution_status='resolved'
    AND mw.market_start_ms BETWEEN 1788220800000 AND 1788825600000-300000
), t AS (SELECT unnest(ARRAY[60,30,15,10,5,3]) tsec),
p AS MATERIALIZED (
  SELECT m.market_id, t.tsec, m.k, m.cl, m.winner, m.resolution_type,
    (SELECT price FROM price_samples WHERE instrument_id=4 AND sample_second_ms=m.e-t.tsec*1000) w,
    (SELECT price FROM price_samples WHERE instrument_id=2 AND sample_second_ms=m.e-t.tsec*1000) s,
    (SELECT sum(price) FROM price_samples WHERE instrument_id=2 AND sample_second_ms BETWEEN m.e-61000 AND m.e-t.tsec*1000) known_sum,
    (SELECT count(*) FROM price_samples WHERE instrument_id=2 AND sample_second_ms BETWEEN m.e-61000 AND m.e-t.tsec*1000) known_n
  FROM m CROSS JOIN t
), q AS MATERIALIZED (
  SELECT *, (known_sum/known_n*(62-tsec)+s*(tsec-2))/60 AS proj
  FROM p WHERE w IS NOT NULL AND s IS NOT NULL AND known_n>=(62-tsec)-3
), cohort AS (
  SELECT tsec, count(*) resolved_cohort_n,
    count(*) FILTER (WHERE w IS NULL) cohort_missing_twap_n,
    count(*) FILTER (WHERE s IS NULL) cohort_missing_spot_n,
    count(*) FILTER (WHERE known_n<(62-tsec)-3) cohort_insufficient_constituents_n,
    count(*) FILTER (WHERE k IS NULL) cohort_missing_open_n,
    count(*) FILTER (WHERE cl IS NULL) cohort_missing_close_n,
    count(*) FILTER (WHERE winner IS NULL) cohort_missing_winner_n
  FROM p GROUP BY tsec
), stats AS (
  SELECT tsec, count(*) n,
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(proj-cl)/cl*1e4)::numeric,2) proj_err_p50_bps,
    round(percentile_cont(0.9) WITHIN GROUP (ORDER BY abs(proj-cl)/cl*1e4)::numeric,2) proj_err_p90_bps,
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(w-cl)/cl*1e4)::numeric,2) twap_err_p50_bps,
    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(s-cl)/cl*1e4)::numeric,2) spot_err_p50_bps,
    round(100.0*avg(CASE WHEN (proj>=k)=(cl>=k) THEN 1 ELSE 0 END),2) proj_side_ok_pct,
    round(100.0*avg(CASE WHEN (w>=k)=(cl>=k) THEN 1 ELSE 0 END),2) twap_side_ok_pct,
    round(100.0*avg(CASE WHEN (s>=k)=(cl>=k) THEN 1 ELSE 0 END),2) spot_side_ok_pct,
    count(*) FILTER (WHERE (proj>=k)=(cl>=k)) proj_correct_n,
    count(*)-count(*) FILTER (WHERE (proj>=k)=(cl>=k)) proj_pilot_error_n,
    count(*) FILTER (WHERE (w>=k)=(cl>=k)) twap_correct_n,
    count(*)-count(*) FILTER (WHERE (w>=k)=(cl>=k)) twap_pilot_error_n,
    count(*) FILTER (WHERE (s>=k)=(cl>=k)) spot_correct_n,
    count(*)-count(*) FILTER (WHERE (s>=k)=(cl>=k)) spot_pilot_error_n,
    count(*) FILTER (WHERE k IS NULL) selected_missing_open_n,
    count(*) FILTER (WHERE cl IS NULL) selected_missing_close_n,
    count(*) FILTER (WHERE winner IS NULL) selected_missing_winner_n,
    count(*) FILTER (WHERE resolution_type='split') selected_split_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL) inferred_side_eligible_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL AND winner IN ('Up','Down')) winner_comparison_eligible_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL AND winner IN ('Up','Down') AND (cl>=k)<>(winner='Up')) inferred_vs_official_winner_disagreement_n,
    count(*) FILTER (WHERE k IS NOT NULL AND winner IN ('Up','Down') AND (proj>=k)=(winner='Up')) proj_official_correct_n,
    count(*) FILTER (WHERE k IS NOT NULL AND winner IN ('Up','Down') AND (w>=k)=(winner='Up')) twap_official_correct_n,
    count(*) FILTER (WHERE k IS NOT NULL AND winner IN ('Up','Down') AND (s>=k)=(winner='Up')) spot_official_correct_n,
    count(*) FILTER (WHERE known_n<62-tsec) selected_imputed_rows_n,
    sum((62-tsec)-known_n) selected_imputed_slots_n,
    min(known_n) min_known_n, max(known_n) max_known_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL AND (proj>=k)=(cl>=k) AND (w>=k)=(cl>=k)) both_proj_twap_correct_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL AND (proj>=k)=(cl>=k) AND (w>=k)<>(cl>=k)) projection_only_correct_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL AND (proj>=k)<>(cl>=k) AND (w>=k)=(cl>=k)) twap_only_correct_n,
    count(*) FILTER (WHERE k IS NOT NULL AND cl IS NOT NULL AND (proj>=k)<>(cl>=k) AND (w>=k)<>(cl>=k)) both_proj_twap_wrong_n
  FROM q GROUP BY tsec
)
SELECT stats.*, cohort.resolved_cohort_n,
  cohort.resolved_cohort_n-stats.n excluded_from_pilot_n,
  cohort.cohort_missing_twap_n, cohort.cohort_missing_spot_n,
  cohort.cohort_insufficient_constituents_n, cohort.cohort_missing_open_n,
  cohort.cohort_missing_close_n, cohort.cohort_missing_winner_n,
  1788220800000::bigint cohort_start_ms, 1788825600000::bigint cohort_end_ms,
  (extract(epoch FROM transaction_timestamp())*1000)::bigint snapshot_ms,
  current_setting('transaction_read_only') transaction_read_only,
  current_setting('transaction_isolation') transaction_isolation,
  current_setting('statement_timeout') statement_timeout
FROM stats JOIN cohort USING (tsec) ORDER BY tsec DESC
) TO STDOUT WITH (FORMAT csv, HEADER true);
COMMIT;
