-- Run inside guard.sql's still-open read-only repeatable-read transaction.
-- Required psql variables: slice_start_ms, slice_end_ms, t_sec.
-- Each statement covers one UTC day (288 markets) and one checkpoint.
COPY (
WITH calendar AS (
  SELECT stamp AS start_ms, stamp + 300000 AS end_ms, stamp / 300000 AS market_id,
         :'t_sec'::integer AS t_sec, stamp + 300000 - :'t_sec'::integer * 1000 AS cut_ms
  FROM generate_series(:'slice_start_ms'::bigint, :'slice_end_ms'::bigint - 300000, 300000) AS g(stamp)
), markets AS (
  SELECT c.*, coalesce(m.start_ms=c.start_ms AND m.end_ms=c.end_ms
    AND m.settlement_reference='chainlink_twap' AND m.settlement_window_s=60
    AND m.settlement_rule_version='btc-5m-twap-60'
    AND m.settlement_source_url='https://data.chain.link/streams/btc-usd-twap-60s-streams',false) AS rule_valid,
    r.resolution_status, r.resolution_type, r.reconciled_settlement_rule_version,
    CASE WHEN r.resolution_status='resolved' AND r.resolution_type='winner'
      AND r.reconciled_settlement_rule_version='btc-5m-twap-60' AND r.winner IN ('Up','Down')
      THEN r.winner END AS official_winner,
    r.chainlink_open_price AS official_k_audit_only, r.chainlink_close_price AS official_final_audit_only
  FROM calendar c LEFT JOIN polymarket_btc_5m_markets m USING (market_id)
  LEFT JOIN polymarket_btc_5m_resolutions r USING (market_id)
)
SELECT m.*,(extract(epoch FROM transaction_timestamp())*1000)::bigint AS snapshot_ms,
  k.k_variants,k.k_event_count,k.k_value,k.k_first_received_ns,
  s.s_variants,s.s_tied_rows,s.s_source_ms,s.s_value,s.s_received_ms,s.s_source_key_errors,
  w.w_variants,w.w_tied_rows,w.w_source_ms,w.w_value,w.w_received_ns,
  h.history_requested_slots,h.history_exact_slots,h.history_carried_slots,h.history_missing_slots,
  h.history_sum,h.history_source_key_errors,h.history_max_carry_age_ms,h.history_max_receive_age_ms,
  h.history_min_source_ms,h.history_max_source_ms,h.history_latest_received_ms
FROM markets m
LEFT JOIN LATERAL (
  SELECT count(DISTINCT e.price_e18) AS k_variants,count(*) AS k_event_count,
    min(e.price) AS k_value,min(e.received_wall_ns) AS k_first_received_ns
  FROM polymarket_twap_events e
  WHERE e.symbol='btc/usd' AND e.window_s=60 AND e.topic='crypto_prices_twap_sixty'
    AND e.provider_event_ms=m.start_ms AND e.received_wall_ns<=m.cut_ms*1000000
) k ON true
LEFT JOIN LATERAL (
  SELECT count(DISTINCT (p.provider_event_ms,p.price)) AS s_variants,count(*) AS s_tied_rows,
    min(p.provider_event_ms) AS s_source_ms,min(p.price) AS s_value,min(p.received_ms) AS s_received_ms,
    count(*) FILTER (WHERE p.provider_event_ms IS NULL OR p.sample_second_ms<>p.provider_event_ms/1000*1000) AS s_source_key_errors
  FROM (
    SELECT p.provider_event_ms,p.sample_second_ms,p.price,p.received_ms
    FROM price_samples p
    WHERE p.instrument_id=2 AND p.sample_second_ms>=m.cut_ms-1201000 AND p.sample_second_ms<=m.cut_ms+600000
      AND p.received_ms>m.cut_ms-600000 AND p.received_ms<=m.cut_ms
    ORDER BY p.received_ms DESC FETCH FIRST 1 ROW WITH TIES
  ) p
) s ON true
LEFT JOIN LATERAL (
  SELECT count(DISTINCT (e.provider_event_ms,e.price_e18)) AS w_variants,count(*) AS w_tied_rows,
    min(e.provider_event_ms) AS w_source_ms,min(e.price) AS w_value,min(e.received_wall_ns) AS w_received_ns
  FROM (
    SELECT e.provider_event_ms,e.price_e18,e.price,e.received_wall_ns
    FROM polymarket_twap_events e
    WHERE e.symbol='btc/usd' AND e.window_s=60 AND e.topic='crypto_prices_twap_sixty'
      AND e.provider_event_ms>m.cut_ms-1200000 AND e.provider_event_ms<=m.cut_ms+600000
      AND e.received_wall_ns>(m.cut_ms-600000)*1000000 AND e.received_wall_ns<=m.cut_ms*1000000
    ORDER BY e.received_wall_ns DESC FETCH FIRST 1 ROW WITH TIES
  ) e
) w ON true
LEFT JOIN LATERAL (
  SELECT count(*) AS history_requested_slots,
    count(*) FILTER (WHERE p.provider_event_ms=u.slot_ms) AS history_exact_slots,
    count(*) FILTER (WHERE p.provider_event_ms<u.slot_ms) AS history_carried_slots,
    count(*) FILTER (WHERE p.price IS NULL) AS history_missing_slots,
    coalesce(sum(p.price),0) AS history_sum,
    count(*) FILTER (WHERE p.price IS NOT NULL AND p.sample_second_ms<>p.provider_event_ms/1000*1000) AS history_source_key_errors,
    max(u.slot_ms-p.provider_event_ms) AS history_max_carry_age_ms,
    max(m.cut_ms-p.received_ms) AS history_max_receive_age_ms,
    min(p.provider_event_ms) AS history_min_source_ms,max(p.provider_event_ms) AS history_max_source_ms,
    max(p.received_ms) AS history_latest_received_ms
  FROM (SELECT m.end_ms-62000+j*1000 AS slot_ms FROM generate_series(0,59) AS slots(j)
        WHERE m.end_ms-62000+j*1000<=m.cut_ms) u
  LEFT JOIN LATERAL (
    SELECT p.provider_event_ms,p.sample_second_ms,p.price,p.received_ms
    FROM price_samples p
    WHERE p.instrument_id=2 AND p.sample_second_ms>=u.slot_ms-600000 AND p.sample_second_ms<=u.slot_ms
      AND p.provider_event_ms>u.slot_ms-600000 AND p.provider_event_ms<=u.slot_ms
      AND p.received_ms<=m.cut_ms
    ORDER BY p.sample_second_ms DESC LIMIT 1
  ) p ON true
) h ON true
ORDER BY m.market_id
) TO STDOUT WITH (FORMAT CSV,HEADER true);
