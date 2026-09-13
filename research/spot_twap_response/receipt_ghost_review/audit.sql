-- Baseline/target audit only: no 60-slot forecast recalculation.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='2s';
SET LOCAL TIME ZONE 'UTC';
SELECT 'H3_AUDIT_META',(extract(epoch FROM transaction_timestamp())*1000)::bigint,
  current_setting('transaction_read_only'),current_setting('transaction_isolation'),current_setting('statement_timeout');
COPY (
WITH inst AS (SELECT generate_series(1788220800000,1788825600000,60000) tau),
tw AS MATERIALIZED (
  SELECT i.tau, e.provider_event_ms w, e.price twap_now,
    e.received_wall_ns baseline_received_ns, e.provider_message_ms baseline_publisher_ms,
    e.instrument_id baseline_instrument_id, e.topic baseline_topic
  FROM inst i LEFT JOIN LATERAL (
    SELECT provider_event_ms,price,received_wall_ns,provider_message_ms,instrument_id,topic
    FROM polymarket_twap_events
    WHERE window_s=60 AND symbol='btc/usd'
      AND provider_event_ms BETWEEN i.tau-70000 AND i.tau
      AND received_wall_ns<=i.tau::bigint*1000000
    ORDER BY received_wall_ns DESC LIMIT 1
  ) e ON true
), inputs AS MATERIALIZED (
  SELECT tw.*, sp.sample_second_ms spot_last_source_ms, sp.received_ms spot_last_received_ms,
    sp.provider_message_ms spot_last_publisher_ms, sp.price spot_last_price,
    (SELECT count(*) FROM polymarket_twap_events d
     WHERE d.window_s=60 AND d.symbol='btc/usd'
       AND d.provider_event_ms BETWEEN tw.tau-70000 AND tw.tau
       AND d.received_wall_ns=tw.baseline_received_ns) baseline_receipt_ties
  FROM tw LEFT JOIN LATERAL (
    -- For a fully filled original window, the greatest source in its combined
    -- slot grid is the latest eligible source for its final slot w+27s.
    SELECT sample_second_ms,received_ms,provider_message_ms,price FROM price_samples
    WHERE instrument_id=2 AND sample_second_ms<=tw.w+27000
      AND sample_second_ms>tw.w+27000-600000 AND received_ms<=tw.tau
    ORDER BY sample_second_ms DESC LIMIT 1
  ) sp ON true
), horizons AS (SELECT unnest(ARRAY[5,10,30]) h)
SELECT a.tau,h.h,a.w,a.twap_now,a.baseline_received_ns,a.baseline_publisher_ms,
  a.baseline_instrument_id,a.baseline_topic,a.baseline_receipt_ties,
  a.tau-a.w baseline_source_age_ms,
  a.tau::bigint*1000000-a.baseline_received_ns baseline_receive_age_ns,
  a.spot_last_source_ms,a.spot_last_received_ms,a.spot_last_publisher_ms,a.spot_last_price,
  a.w+h.h*1000 target_source_ms,
  tf.event_n target_events,tf.min_price target_min_price,tf.max_price target_max_price,
  tf.earliest_price target_earliest_price,tf.first_ns target_first_received_ns,
  tf.last_ns target_last_received_ns,tf.unexpected_identity_events,
  tf.first_ns/1000000-a.tau original_target_arrival_ms,
  tf.first_ns-a.tau::bigint*1000000 target_arrival_ns,
  (a.w/300000<>(a.w+h.h*1000)/300000) baseline_target_cross_market,
  ((a.w+h.h*1000)%300000=0) target_exact_market_boundary,
  ((a.w+h.h*1000)/300000<>a.tau/300000) decision_target_different_market,
  greatest(0,(a.w+h.h*1000-3000-a.spot_last_source_ms)/1000) tail_assumed_slots,
  r.market_id target_market_id,r.resolution_status,r.chainlink_open_price target_k
FROM inputs a CROSS JOIN horizons h
LEFT JOIN LATERAL (
  SELECT count(*) event_n,min(price) min_price,max(price) max_price,
    (array_agg(price ORDER BY received_wall_ns,connection_id,receive_sequence))[1] earliest_price,
    min(received_wall_ns) first_ns,max(received_wall_ns) last_ns,
    count(*) FILTER (WHERE instrument_id<>4 OR topic<>'crypto_prices_twap_sixty') unexpected_identity_events
  FROM polymarket_twap_events
  WHERE window_s=60 AND symbol='btc/usd' AND provider_event_ms=a.w+h.h*1000
) tf ON true
LEFT JOIN polymarket_btc_5m_resolutions r ON r.market_id=(a.w+h.h*1000)/300000
ORDER BY a.tau,h.h
) TO STDOUT WITH (FORMAT csv,HEADER true);
COMMIT;
