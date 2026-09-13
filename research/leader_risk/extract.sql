-- Runnable extraction skeleton for H3_TWAP_LEADER_RISK_STUDY.md.
-- sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -d price_collector \
--   -v start_utc=2026-08-16T00:00:00Z -v end_utc=2026-09-12T00:25:00Z \
--   -f extract.sql > observations.csv
-- Run against price_collector; -q keeps transaction messages out of the CSV.
-- Supplied start/end boundaries are honored; there is no hidden cohort floor.
-- Latest deployed code: ace8d19, pulled 2026-09-10T02:26:54Z. The first full
-- post-update market starts 2026-09-10T02:30:00Z, a descriptive comparison boundary.
-- statement_timeout defaults to 60s; pass -v statement_timeout=180s only when
-- an explicitly chosen longer limit is appropriate. No automatic timeout increase.
-- Accept output only after psql exits zero and CSV validation confirms exactly
-- eight distinct checkpoints per included market, with no duplicate market/T pairs.
-- The distinct market count must also equal the exported cohort_market_count.
-- Outcomes are read at this transaction's snapshot; this is not an old-state replay.
-- Tabulation uses inputs_available AND NOT current_tie; never filter on winner presence.

\set ON_ERROR_STOP on
\if :{?statement_timeout}
\else
\set statement_timeout '60s'
\endif

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = :'statement_timeout';
SET LOCAL lock_timeout = '2s';

-- Guard the index-friendly source-market equalities in this same snapshot.
-- At the fixed T=120..3 checkpoints, candidate receipts lie between market start
-- +177000 ms and market end -3000 ms. A received-ms minus sample-second lag in
-- [-2999,177000] therefore keeps every candidate's source second in that market.
-- Check ALL expected-source rows before any freshness filtering. A newer stale
-- or future-source receipt must never be hidden to reveal an older fresh value.
-- These conservative global checks scan source history and may reject an outlier
-- outside the requested cohort; failing aborts rather than exporting altered data.
DO $h3_source_market_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM polymarket_twap_events e
        WHERE e.symbol = 'btc/usd' AND e.window_s = 60
          AND e.topic = 'crypto_prices_twap_sixty'
          AND (e.received_wall_ns / 1000000 - e.sample_second_ms)
              NOT BETWEEN -2999 AND 177000
    ) THEN
        RAISE EXCEPTION 'H3 TWAP source-market guard failed: receipt/sample lag is outside [-2999,177000] ms. No CSV exported; investigate the lag and use receipt-only selection before retrying.';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM price_samples p
        JOIN instruments i USING (instrument_id)
        JOIN providers pr USING (provider_id)
        WHERE pr.provider_code = 'polymarket_chainlink_rtds'
          AND i.symbol = 'BTCUSD' AND i.stream_name = 'crypto_prices_chainlink:btc/usd'
          AND (p.received_ms - p.sample_second_ms)
              NOT BETWEEN -2999 AND 177000
    ) THEN
        RAISE EXCEPTION 'H3 spot source-market guard failed: receipt/sample lag is outside [-2999,177000] ms. No CSV exported; investigate the lag and use receipt-only selection before retrying.';
    END IF;
END;
$h3_source_market_guard$;

COPY (
WITH requested_bounds AS (
    SELECT (extract(epoch FROM :'start_utc'::timestamptz) * 1000)::bigint AS requested_start_ms,
           (extract(epoch FROM :'end_utc'::timestamptz) * 1000)::bigint AS end_ms,
           (extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS outcome_cutoff_ms
), bounds AS (
    SELECT requested_start_ms,
           requested_start_ms AS start_ms,
           end_ms, outcome_cutoff_ms
    FROM requested_bounds
), markets AS (
    SELECT m.market_id, count(*) OVER () AS cohort_market_count,
           mw.market_start_ms AS start_ms, mw.market_end_ms AS end_ms,
           coalesce(m.start_ms = mw.market_start_ms AND m.end_ms = mw.market_end_ms
             AND m.settlement_reference = 'chainlink_twap'
             AND m.settlement_window_s = 60
             AND m.settlement_rule_version = 'btc-5m-twap-60'
             AND m.settlement_source_url =
                 'https://data.chain.link/streams/btc-usd-twap-60s-streams', false) AS rule_valid,
           r.resolution_status, r.resolution_type,
           r.reconciled_settlement_rule_version,
           r.chainlink_open_price AS official_k_audit_only,
           CASE WHEN r.resolution_status = 'resolved'
                     AND r.resolution_type = 'winner'
                     AND r.reconciled_settlement_rule_version = 'btc-5m-twap-60'
                     AND r.winner IN ('Up', 'Down') THEN r.winner END AS official_winner,
           b.requested_start_ms AS requested_cohort_start_ms,
           b.start_ms AS cohort_start_ms, b.end_ms AS cohort_end_ms, b.outcome_cutoff_ms
    FROM polymarket_btc_5m_markets m
    JOIN market_windows mw USING (market_id)
    CROSS JOIN bounds b
    LEFT JOIN polymarket_btc_5m_resolutions r USING (market_id)
    WHERE mw.market_start_ms >= b.start_ms AND mw.market_start_ms < b.end_ms
      AND mw.market_end_ms <= b.end_ms AND mw.market_end_ms <= b.outcome_cutoff_ms
), cuts AS MATERIALIZED (
    SELECT m.*, t.t_sec, m.end_ms - t.t_sec * 1000 AS cut_ms
    FROM markets m
    CROSS JOIN (VALUES (120), (90), (60), (30), (15), (10), (5), (3)) t(t_sec)
), twap_asof AS MATERIALIZED (
    -- The same-snapshot guard above makes source-market grouping safe here.
    -- Filter ONLY receipt freshness here. A newer stale/future-source tick must win
    -- selection and then fail source freshness, rather than reveal an older fresh tick.
    SELECT DISTINCT ON (c.market_id, c.t_sec)
           c.market_id, c.t_sec, e.price AS w, e.price_e18 AS w_e18,
           e.provider_event_ms AS w_source_ms, e.received_wall_ns AS w_received_ns,
           e.connection_id AS w_connection_id, e.receive_sequence AS w_receive_sequence
    FROM polymarket_twap_events e
    JOIN cuts c ON e.market_id = c.market_id
      AND e.received_wall_ns BETWEEN (c.cut_ms - 3000) * 1000000 AND c.cut_ms * 1000000
    WHERE e.symbol = 'btc/usd' AND e.window_s = 60
      AND e.topic = 'crypto_prices_twap_sixty'
    ORDER BY c.market_id, c.t_sec, e.received_wall_ns DESC,
             e.connection_id, e.receive_sequence DESC
), spot_asof AS MATERIALIZED (
    SELECT DISTINCT ON (c.market_id, c.t_sec)
           c.market_id, c.t_sec, p.price AS s,
           p.provider_event_ms AS s_source_ms, p.received_ms AS s_received_ms,
           p.sample_second_ms AS s_sample_second_ms
    FROM price_samples p
    JOIN instruments i USING (instrument_id)
    JOIN providers pr USING (provider_id)
    JOIN cuts c ON p.market_id = c.market_id
      AND p.received_ms BETWEEN c.cut_ms - 3000 AND c.cut_ms
    WHERE pr.provider_code = 'polymarket_chainlink_rtds'
      AND i.symbol = 'BTCUSD' AND i.stream_name = 'crypto_prices_chainlink:btc/usd'
    ORDER BY c.market_id, c.t_sec, p.received_ms DESC,
             p.sample_second_ms DESC, p.instrument_id
), panel AS (
    SELECT c.*, k.boundary_variants, k.k_received_ns,
           CASE WHEN k.boundary_variants = 1 THEN k.k_value END AS k,
           w.w, w.w_e18, w.w_source_ms, w.w_received_ns,
           w.w_connection_id, w.w_receive_sequence,
           s.s, s.s_source_ms, s.s_received_ms, s.s_sample_second_ms
    FROM cuts c
    LEFT JOIN LATERAL (
        -- Only the exact opening boundary and pre-checkpoint receipts establish K.
        -- Multiple different pre-checkpoint boundary values make K unavailable.
        SELECT count(DISTINCT e.price_e18) AS boundary_variants,
               min(e.price) AS k_value, min(e.received_wall_ns) AS k_received_ns
        FROM polymarket_twap_events e
        WHERE e.symbol = 'btc/usd' AND e.window_s = 60
          AND e.topic = 'crypto_prices_twap_sixty'
          AND e.provider_event_ms = c.start_ms
          AND e.received_wall_ns <= c.cut_ms * 1000000
    ) k ON true
    LEFT JOIN twap_asof w USING (market_id, t_sec)
    LEFT JOIN spot_asof s USING (market_id, t_sec)
)
SELECT *,
       coalesce(rule_valid AND k > 0 AND w > 0 AND s > 0
         AND cut_ms - w_source_ms BETWEEN 0 AND 3000
         AND cut_ms - s_source_ms BETWEEN 0 AND 3000, false) AS inputs_available,
       CASE WHEN k = w THEN true ELSE false END AS current_tie,
       NOT rule_valid AS invalid_market_rule,
       boundary_variants = 0 AS boundary_missing_or_late,
       boundary_variants > 1 AS boundary_conflict,
       w_received_ns IS NULL AS twap_no_recent_receipt,
       s_received_ms IS NULL AS spot_no_recent_receipt,
       w_received_ns IS NOT NULL
         AND NOT coalesce(cut_ms - w_source_ms BETWEEN 0 AND 3000, false)
         AS twap_bad_source_age,
       s_received_ms IS NOT NULL
         AND NOT coalesce(cut_ms - s_source_ms BETWEEN 0 AND 3000, false)
         AS spot_bad_source_age
FROM panel
ORDER BY market_id, t_sec DESC
) TO STDOUT WITH (FORMAT CSV, HEADER true);

COMMIT;
