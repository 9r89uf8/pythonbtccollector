\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

SELECT json_build_object(
  'inventory_time_utc', transaction_timestamp(),
  'transaction_read_only', current_setting('transaction_read_only'),
  'raw_chainlink_relation', to_regclass('raw_capture.chainlink_price_events'),
  'raw_chainlink_partition_count', (
    SELECT count(*) FROM pg_inherits
    WHERE inhparent = to_regclass('raw_capture.chainlink_price_events')
  ),
  'raw_chainlink_partition_statistics', (
    SELECT coalesce(json_agg(json_build_object(
      'relation', c.oid::regclass::text,
      'estimated_rows', c.reltuples::numeric,
      'total_bytes', pg_total_relation_size(c.oid),
      'bound', pg_get_expr(c.relpartbound, c.oid))), '[]'::json)
    FROM pg_inherits inh JOIN pg_class c ON c.oid = inh.inhrelid
    WHERE inh.inhparent = to_regclass('raw_capture.chainlink_price_events')
  )
);

SELECT json_build_object('spot_instruments', coalesce(json_agg(json_build_object(
  'instrument_id', i.instrument_id, 'provider_code', p.provider_code,
  'symbol', i.symbol, 'stream_name', i.stream_name)), '[]'::json))
FROM instruments i JOIN providers p USING (provider_id)
WHERE p.provider_code = 'polymarket_chainlink_rtds'
  AND i.symbol = 'BTCUSD' AND i.stream_name = 'crypto_prices_chainlink:btc/usd';

SELECT json_build_object('source_indexes', coalesce(json_agg(json_build_object(
  'schema', schemaname, 'table', tablename, 'index', indexname, 'definition', indexdef)), '[]'::json))
FROM pg_indexes
WHERE (schemaname = 'public' AND tablename IN ('price_samples', 'polymarket_twap_events'))
   OR (schemaname = 'raw_capture' AND tablename = 'chainlink_price_events');

COMMIT;
