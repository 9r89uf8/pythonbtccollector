-- One UTC day of TWAP source timestamps, plus only the spot-second margins
-- needed for 60-second means at signed shifts -4 through +4 seconds.
-- Inventory verified instrument_id=2 is the expected Chainlink spot identity.
-- TWAP conflicts remain separate rows in the export; the local diagnostic
-- must reject conflicting values at the same exact provider timestamp.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

COPY (
  WITH bounds AS (
    SELECT (extract(epoch FROM timestamptz '2026-09-11 00:00:00+00') * 1000)::bigint AS start_ms,
           (extract(epoch FROM timestamptz '2026-09-12 00:00:00+00') * 1000)::bigint AS end_ms
  )
  SELECT 'spot'::text AS kind, p.provider_event_ms AS source_ms,
         p.sample_second_ms, p.price::text AS price,
         NULL::text AS price_e18, p.received_ms::text AS received_ms,
         NULL::text AS received_wall_ns
  FROM price_samples p CROSS JOIN bounds b
  WHERE p.instrument_id = 2
    AND p.sample_second_ms >= b.start_ms - 63000 AND p.sample_second_ms < b.end_ms + 4000
  UNION ALL
  SELECT 'twap'::text, e.provider_event_ms, e.sample_second_ms,
         e.price::text, e.price_e18::text, NULL::text, e.received_wall_ns::text
  FROM polymarket_twap_events e CROSS JOIN bounds b
  WHERE e.symbol = 'btc/usd' AND e.window_s = 60 AND e.topic = 'crypto_prices_twap_sixty'
    AND e.provider_event_ms >= b.start_ms AND e.provider_event_ms < b.end_ms
) TO STDOUT WITH (FORMAT CSV, HEADER true);

COMMIT;
