-- Optional focused inspection of the nine negative source-stamp pairs identified
-- by query.sql. Saved for review; not separately executed during this audit.
-- Original window_s=60 identity scope is preserved; market bounds use the schema
-- source-second/market invariant. The earliest tied event is a representative;
-- query.sql found one event per paired stamp in the audited snapshot.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='20s';
SET LOCAL lock_timeout='2s';
WITH keys(s) AS (VALUES
  (1788235384000::bigint),(1788325050000),(1788325054000),
  (1788325058000),(1788325060000),(1788325064000),
  (1788402066000),(1788556233000),(1788738262000)
)
SELECT k.s spot_sample_second_ms, p.provider_event_ms spot_provider_event_ms,
  p.provider_message_ms spot_provider_message_ms, p.received_ms spot_received_ms,
  p.price spot_price, p.received_ms-p.provider_event_ms spot_receipt_minus_source_ms,
  p.received_ms-p.provider_message_ms spot_receipt_minus_publisher_ms,
  e.provider_event_ms twap_provider_event_ms,
  e.provider_message_ms twap_provider_message_ms,
  e.received_wall_ns twap_earliest_received_wall_ns,
  e.received_wall_ns/1000000-p.received_ms paired_delay_ms,
  e.received_wall_ns/1000000-e.provider_event_ms twap_receipt_minus_source_ms,
  e.received_wall_ns/1000000-e.provider_message_ms twap_receipt_minus_publisher_ms,
  e.price twap_price, e.price_e18, e.instrument_id, e.topic, e.symbol,
  e.connection_id, e.receive_sequence, e.received_monotonic_ns
FROM keys k
JOIN price_samples p ON p.instrument_id=2 AND p.sample_second_ms=k.s
JOIN LATERAL (
  SELECT * FROM polymarket_twap_events
  WHERE window_s=60 AND provider_event_ms=k.s+3000
    AND market_id=(k.s+3000)/300000
  ORDER BY received_wall_ns, connection_id, receive_sequence LIMIT 1
) e ON true
ORDER BY k.s;
COMMIT;
