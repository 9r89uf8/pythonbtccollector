-- Independent PostgreSQL NUMERIC cross-check of the one-day local calculation.
-- Uses the external pilot's exact materialized TWAP target: instrument_id=4.
-- Both history scans are bounded; four shifts are joined to already bounded
-- one-day inputs. No source price or residual is converted to float.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

WITH bounds AS (
  SELECT 1789084800000::bigint AS start_ms, 1789171200000::bigint AS end_ms
), durable AS (
  SELECT e.provider_event_ms AS stamp, min(e.price) AS price, count(*) AS rows,
         count(DISTINCT e.price_e18) AS variants
  FROM polymarket_twap_events e CROSS JOIN bounds b
  WHERE e.symbol = 'btc/usd' AND e.window_s = 60 AND e.topic = 'crypto_prices_twap_sixty'
    AND e.provider_event_ms >= b.start_ms AND e.provider_event_ms < b.end_ms
  GROUP BY e.provider_event_ms
), materialized AS (
  SELECT p.sample_second_ms AS stamp, p.provider_event_ms, p.price
  FROM price_samples p CROSS JOIN bounds b
  WHERE p.instrument_id = 4 AND p.sample_second_ms >= b.start_ms AND p.sample_second_ms < b.end_ms
)
SELECT json_build_object('check', 'materialized_vs_durable_twap',
  'durable_source_timestamps', count(d.stamp), 'materialized_seconds', count(m.stamp),
  'durable_only_timestamps', count(*) FILTER (WHERE m.stamp IS NULL),
  'materialized_only_seconds', count(*) FILTER (WHERE d.stamp IS NULL),
  'matched_prices_different', count(*) FILTER (WHERE d.stamp IS NOT NULL AND m.stamp IS NOT NULL AND d.price <> m.price),
  'durable_conflict_groups', count(*) FILTER (WHERE d.variants > 1),
  'materialized_source_sample_mismatches', count(*) FILTER (WHERE m.provider_event_ms <> m.stamp),
  'instrument_4_identity', (SELECT json_build_object('provider', pr.provider_code, 'symbol', i.symbol, 'stream', i.stream_name)
                          FROM instruments i JOIN providers pr USING (provider_id) WHERE i.instrument_id = 4))
FROM durable d FULL JOIN materialized m USING (stamp);

WITH spot_windows AS MATERIALIZED (
  SELECT p.sample_second_ms AS newest_ms,
         sum(p.price) OVER (ORDER BY p.sample_second_ms RANGE BETWEEN 59000 PRECEDING AND CURRENT ROW) AS sum60,
         count(*) OVER (ORDER BY p.sample_second_ms RANGE BETWEEN 59000 PRECEDING AND CURRENT ROW) AS n60
  FROM price_samples p
  WHERE p.instrument_id = 2 AND p.sample_second_ms >= 1789084800000 - 63000
    AND p.sample_second_ms < 1789171200000 + 4000
), materialized_twap AS MATERIALIZED (
  SELECT p.sample_second_ms AS stamp, p.price
  FROM price_samples p
  WHERE p.instrument_id = 4 AND p.sample_second_ms >= 1789084800000 AND p.sample_second_ms < 1789171200000
), errors AS (
  SELECT shifts.a,
    (abs(s.sum60 - 60 * w.price)::numeric(100,70) * 10000)
      / (60 * w.price)::numeric(100,70) AS absolute_bps
  FROM materialized_twap w CROSS JOIN (VALUES (-4), (-3), (-2), (-1)) shifts(a)
  JOIN spot_windows s ON s.newest_ms = w.stamp + shifts.a * 1000 AND s.n60 = 60
), ranked AS (
  SELECT a, absolute_bps, row_number() OVER (PARTITION BY a ORDER BY absolute_bps) AS rn,
         count(*) OVER (PARTITION BY a) AS n
  FROM errors
), aggregates AS (
  SELECT a, n,
    avg(absolute_bps) FILTER (WHERE rn IN ((n + 1) / 2, (n + 2) / 2)) AS median_absolute_bps,
    max(absolute_bps) FILTER (WHERE rn = floor((n - 1) * 99::numeric / 100) + 1) AS p99_left,
    max(absolute_bps) FILTER (WHERE rn = ceil((n - 1) * 99::numeric / 100) + 1) AS p99_right,
    max(absolute_bps) AS maximum_absolute_bps
  FROM ranked GROUP BY a, n
)
SELECT json_build_object('check', 'materialized_twap_shift', 'shift_seconds_a', a, 'n', n,
  'median_absolute_bps', median_absolute_bps::text,
  'p99_absolute_bps', (p99_left + (p99_right - p99_left) *
     ((n - 1) * 99::numeric / 100 - floor((n - 1) * 99::numeric / 100)))::text,
  'maximum_absolute_bps', maximum_absolute_bps::text)
FROM aggregates ORDER BY a;

COMMIT;
