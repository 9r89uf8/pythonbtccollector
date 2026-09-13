\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

WITH bounds AS (
  SELECT (extract(epoch FROM timestamptz '2026-09-11 00:00:00+00') * 1000)::bigint AS start_ms,
         (extract(epoch FROM timestamptz '2026-09-12 00:00:00+00') * 1000)::bigint AS end_ms
)
SELECT json_build_object('raw_chainlink_received_day_count', count(*),
                         'day_start_utc', '2026-09-11T00:00:00Z',
                         'day_end_utc_exclusive', '2026-09-12T00:00:00Z')
FROM raw_capture.chainlink_price_events e CROSS JOIN bounds b
WHERE e.received_wall_ns >= b.start_ms * 1000000
  AND e.received_wall_ns < b.end_ms * 1000000;

WITH bounds AS (
  SELECT (extract(epoch FROM timestamptz '2026-09-11 00:00:00+00') * 1000)::bigint AS start_ms,
         (extract(epoch FROM timestamptz '2026-09-12 00:00:00+00') * 1000)::bigint AS end_ms
), grouped AS (
  SELECT e.provider_event_ms, count(*) AS rows, count(DISTINCT e.price_e18) AS variants
  FROM polymarket_twap_events e CROSS JOIN bounds b
  WHERE e.symbol = 'btc/usd' AND e.window_s = 60 AND e.topic = 'crypto_prices_twap_sixty'
    AND e.provider_event_ms >= b.start_ms AND e.provider_event_ms < b.end_ms
  GROUP BY e.provider_event_ms
)
SELECT json_build_object('twap_event_rows', coalesce(sum(rows), 0),
  'twap_unique_source_timestamps', count(*),
  'twap_source_timestamp_duplicate_groups', count(*) FILTER (WHERE rows > 1),
  'twap_source_timestamp_conflict_groups', count(*) FILTER (WHERE variants > 1),
  'twap_nonsecond_source_timestamp_groups', count(*) FILTER (WHERE provider_event_ms % 1000 <> 0),
  'first_twap_source_ms', min(provider_event_ms), 'last_twap_source_ms', max(provider_event_ms))
FROM grouped;

WITH bounds AS (
  SELECT (extract(epoch FROM timestamptz '2026-09-11 00:00:00+00') * 1000)::bigint AS start_ms,
         (extract(epoch FROM timestamptz '2026-09-12 00:00:00+00') * 1000)::bigint AS end_ms
)
SELECT json_build_object('spot_rows_with_required_margins', count(*),
  'spot_day_rows', count(*) FILTER (WHERE p.sample_second_ms >= b.start_ms AND p.sample_second_ms < b.end_ms),
  'spot_nonsecond_source_timestamps', count(*) FILTER (WHERE p.provider_event_ms % 1000 <> 0),
  'spot_null_source_timestamps', count(*) FILTER (WHERE p.provider_event_ms IS NULL),
  'spot_source_floor_key_mismatches', count(*) FILTER (WHERE p.provider_event_ms / 1000 * 1000 <> p.sample_second_ms),
  'first_spot_sample_ms', min(p.sample_second_ms), 'last_spot_sample_ms', max(p.sample_second_ms))
FROM price_samples p CROSS JOIN bounds b
WHERE p.instrument_id = 2
  AND p.sample_second_ms >= b.start_ms - 63000 AND p.sample_second_ms < b.end_ms + 4000;

COMMIT;
