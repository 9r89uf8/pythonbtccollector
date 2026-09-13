-- Section 14 original pairing/statistics, plus fixed-week coverage and negative pairs.
-- Original inclusive endpoint and window_s-only TWAP selection are preserved.
-- Redundant market bounds follow schema's floor(source second)/market constraints
-- and allow bounded use of the existing market index; no production index changes.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';
SET LOCAL TIME ZONE 'UTC';
WITH s AS MATERIALIZED (
  SELECT sample_second_ms t, provider_event_ms source_ms,
    provider_message_ms publisher_ms, received_ms r, price::text price, source_topic
  FROM price_samples
  WHERE instrument_id=2
    AND sample_second_ms BETWEEN 1788220800000 AND 1788825600000
), e AS MATERIALIZED (
  SELECT provider_event_ms t, min(received_wall_ns)/1000000 r,
    min(received_wall_ns) first_ns, count(*) event_n,
    min(price_e18) min_price_e18, max(price_e18) max_price_e18,
    count(*) FILTER (WHERE instrument_id<>4 OR symbol<>'btc/usd'
      OR topic<>'crypto_prices_twap_sixty') unexpected_identity_n
  FROM polymarket_twap_events
  WHERE window_s=60
    AND provider_event_ms BETWEEN 1788220800000 AND 1788825600000+10000
    AND market_id BETWEEN 1788220800000/300000 AND (1788825600000+10000)/300000
  GROUP BY provider_event_ms
), paired AS MATERIALIZED (
  SELECT s.*, e.t twap_t, e.r twap_r, e.first_ns, e.event_n,
    e.unexpected_identity_n, e.min_price_e18<>e.max_price_e18 conflicting_twap_price,
    e.r-s.r delay_ms
  FROM s JOIN e ON e.t=s.t+3000
), stats AS (
  SELECT count(*) n_pairs,
    percentile_cont(0.1) WITHIN GROUP (ORDER BY delay_ms) p10_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY delay_ms) p50_ms,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY delay_ms) p90_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY delay_ms) p99_ms,
    min(delay_ms) min_ms, max(delay_ms) max_ms,
    count(*) FILTER (WHERE delay_ms<0) negative_pairs
  FROM paired
), negative AS (
  SELECT p.t spot_sample_second_ms, p.source_ms spot_provider_event_ms,
    p.publisher_ms spot_provider_message_ms, p.r spot_received_ms,
    p.price spot_price, p.source_topic spot_topic,
    p.r-p.source_ms spot_receipt_minus_source_ms,
    p.publisher_ms-p.source_ms spot_publisher_minus_source_ms,
    p.r-p.publisher_ms spot_receipt_minus_publisher_ms,
    p.twap_t twap_provider_event_ms, p.twap_r twap_earliest_received_ms,
    p.first_ns twap_earliest_received_wall_ns, p.delay_ms,
    p.event_n twap_events_for_stamp, p.unexpected_identity_n,
    p.conflicting_twap_price,
    earliest.provider_message_ms twap_provider_message_ms,
    p.twap_r-earliest.provider_message_ms twap_receipt_minus_publisher_ms,
    earliest.provider_message_ms-p.twap_t twap_publisher_minus_source_ms,
    p.twap_r-p.twap_t twap_receipt_minus_source_ms,
    earliest.price::text twap_price, earliest.price_e18::text twap_price_e18,
    earliest.connection_id, earliest.receive_sequence,
    earliest.instrument_id, earliest.topic, earliest.symbol,
    earliest.received_monotonic_ns
  FROM paired p
  JOIN LATERAL (
    SELECT * FROM polymarket_twap_events original
    WHERE original.market_id=p.twap_t/300000
      AND original.provider_event_ms=p.twap_t AND original.window_s=60
      AND original.received_wall_ns=p.first_ns
    ORDER BY original.connection_id, original.receive_sequence LIMIT 1
  ) earliest ON true
  WHERE p.delay_ms<0
)
SELECT jsonb_build_object(
  'snapshot_ms',(extract(epoch FROM transaction_timestamp())*1000)::bigint,
  'read_only',current_setting('transaction_read_only'),
  'isolation',current_setting('transaction_isolation'),
  'statement_timeout',current_setting('statement_timeout'),
  'stats',(SELECT to_jsonb(stats) FROM stats),
  'original_result_line',(SELECT concat_ws('|',n_pairs,p10_ms,p50_ms,p90_ms,p99_ms,min_ms,max_ms,negative_pairs) FROM stats),
  'coverage',jsonb_build_object(
    'calendar_spot_seconds_inclusive',604801,
    'retained_spot_seconds',(SELECT count(*) FROM s),
    'retained_spot_at_exclusive_week_end',(SELECT count(*) FROM s WHERE t=1788825600000),
    'spot_without_exact_plus3_twap',(SELECT count(*) FROM s LEFT JOIN e ON e.t=s.t+3000 WHERE e.t IS NULL),
    'spot_provider_sample_mismatches',(SELECT count(*) FROM s WHERE source_ms IS DISTINCT FROM t),
    'twap_source_stamps_in_query_range',(SELECT count(*) FROM e),
    'twap_events_in_query_range',(SELECT sum(event_n) FROM e),
    'twap_unexpected_identity_events',(SELECT sum(unexpected_identity_n) FROM e),
    'twap_conflicting_price_stamps',(SELECT count(*) FROM e WHERE min_price_e18<>max_price_e18),
    'paired_conflicting_price_stamps',(SELECT count(*) FROM paired WHERE conflicting_twap_price),
    'paired_unexpected_identity_stamps',(SELECT count(*) FROM paired WHERE unexpected_identity_n>0),
    'paired_multiple_event_stamps',(SELECT count(*) FROM paired WHERE event_n>1)
  ),
  'source_identities',(SELECT jsonb_agg(jsonb_build_object('instrument_id',i.instrument_id,
    'provider',p.provider_code,'symbol',i.symbol,'stream',i.stream_name) ORDER BY i.instrument_id)
    FROM instruments i JOIN providers p USING(provider_id) WHERE i.instrument_id IN (2,4)),
  'negative_pairs',coalesce((SELECT jsonb_agg(to_jsonb(negative) ORDER BY spot_sample_second_ms) FROM negative),'[]'::jsonb)
);
COMMIT;
