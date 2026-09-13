\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

-- Current values use receipt lookback (D-600s,D]. A proved receive/source lag
-- in [-600s,+600s] makes source bounds (D-1200s,D+600s] complete for that
-- candidate set. Selection still precedes source-age eligibility checks.
-- These guards inspect the fixed week's receipt interval, not a generated
-- slot cross join. Any outlier or timeout aborts the complete export.
DO $guard_spot$
BEGIN
  IF EXISTS (
    SELECT 1 FROM price_samples p
    WHERE p.instrument_id = 2
      AND p.received_ms > 1788220440000 AND p.received_ms <= 1788825597000
      AND (p.provider_event_ms IS NULL
           OR p.received_ms - p.provider_event_ms NOT BETWEEN -600000 AND 600000
           OR p.sample_second_ms <> p.provider_event_ms / 1000 * 1000)
  ) THEN RAISE EXCEPTION 'Receipt pilot spot receipt/source range guard failed'; END IF;
  IF NOT EXISTS (
    SELECT 1 FROM instruments i JOIN providers p USING (provider_id)
    WHERE i.instrument_id=2 AND p.provider_code='polymarket_chainlink_rtds'
      AND i.symbol='BTCUSD' AND i.stream_name='crypto_prices_chainlink:btc/usd'
  ) THEN RAISE EXCEPTION 'Receipt pilot spot instrument identity failed'; END IF;
END;
$guard_spot$;

DO $guard_twap$
BEGIN
  IF EXISTS (
    SELECT 1 FROM polymarket_twap_events e
    WHERE e.symbol='btc/usd' AND e.window_s=60 AND e.topic='crypto_prices_twap_sixty'
      AND e.received_wall_ns > 1788220440000000000 AND e.received_wall_ns <= 1788825597000000000
      AND e.received_wall_ns - e.provider_event_ms * 1000000 NOT BETWEEN -600000000000 AND 600000000000
  ) THEN RAISE EXCEPTION 'Receipt pilot TWAP receipt/source range guard failed'; END IF;
END;
$guard_twap$;

DO $guard_historical_keys$
BEGIN
  IF EXISTS (
    SELECT 1 FROM price_samples p
    WHERE p.instrument_id=2 AND p.sample_second_ms >= 1788220438000 AND p.sample_second_ms <= 1788825597000
      AND (p.provider_event_ms IS NULL OR p.sample_second_ms <> p.provider_event_ms / 1000 * 1000
           OR p.received_ms - p.provider_event_ms NOT BETWEEN -600000 AND 600000)
  ) THEN RAISE EXCEPTION 'Receipt pilot historical source-key guard failed'; END IF;
END;
$guard_historical_keys$;
SELECT 'PILOT_CLOCK_PROFILE ' || json_build_object(
  'spot_offsecond_source_rows', (SELECT count(*) FROM price_samples p
    WHERE p.instrument_id=2 AND p.sample_second_ms>=1788219839000 AND p.sample_second_ms<=1788826197000
      AND p.provider_event_ms % 1000 <> 0),
  'twap_offsecond_source_rows', (SELECT count(*) FROM polymarket_twap_events e
    WHERE e.symbol='btc/usd' AND e.window_s=60 AND e.topic='crypto_prices_twap_sixty'
      AND e.provider_event_ms>1788219840000 AND e.provider_event_ms<=1788826197000
      AND e.provider_event_ms % 1000 <> 0)
)::text;
\echo PILOT_GUARDS_PASSED
