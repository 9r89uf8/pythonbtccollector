-- Grading-only scheduled-close anchor check. Not a forecast input selection.
-- Fixed market-start cohort [2026-09-01,2026-09-08) UTC; 2,016 markets.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

COPY (
WITH calendar AS (
    SELECT start_ms, start_ms + 300000 AS end_ms, start_ms / 300000 AS market_id
    FROM generate_series(1788220800000::bigint, 1788825600000::bigint - 300000, 300000) AS g(start_ms)
), panel AS MATERIALIZED (
    SELECT c.*,
        coalesce(m.start_ms = c.start_ms AND m.end_ms = c.end_ms
            AND m.settlement_reference = 'chainlink_twap'
            AND m.settlement_window_s = 60
            AND m.settlement_rule_version = 'btc-5m-twap-60'
            AND m.settlement_source_url = 'https://data.chain.link/streams/btc-usd-twap-60s-streams', false) AS market_rule_valid,
        coalesce(r.resolution_status = 'resolved' AND r.resolution_type = 'winner'
            AND r.reconciled_settlement_rule_version = 'btc-5m-twap-60'
            AND r.winner IN ('Up', 'Down'), false) AS official_rule_valid,
        r.reconciled_settlement_rule_version AS official_rule_version,
        r.chainlink_close_price AS official_close,
        e.event_count, e.price_variants, e.boundary_price
    FROM calendar c
    LEFT JOIN polymarket_btc_5m_markets m USING (market_id)
    LEFT JOIN polymarket_btc_5m_resolutions r USING (market_id)
    LEFT JOIN LATERAL (
        SELECT count(*) AS event_count, count(DISTINCT t.price_e18) AS price_variants,
            CASE WHEN count(DISTINCT t.price_e18) = 1 THEN min(t.price) END AS boundary_price
        FROM polymarket_twap_events t
        WHERE t.symbol = 'btc/usd' AND t.window_s = 60
            AND t.topic = 'crypto_prices_twap_sixty'
            AND t.provider_event_ms = c.end_ms
    ) e ON true
), measured AS (
    SELECT *, coalesce(market_rule_valid AND official_rule_valid
        AND official_close > 0 AND price_variants = 1, false) AS comparable
    FROM panel
)
SELECT
    (extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS snapshot_ms,
    current_setting('transaction_read_only') AS transaction_read_only,
    current_setting('transaction_isolation') AS transaction_isolation,
    current_setting('statement_timeout') AS statement_timeout,
    count(*) AS calendar_markets,
    count(*) FILTER (WHERE market_rule_valid) AS validated_market_rules,
    count(*) FILTER (WHERE official_rule_valid) AS validated_official_rules,
    string_agg(DISTINCT coalesce(official_rule_version, '<missing>'), ';' ORDER BY coalesce(official_rule_version, '<missing>')) AS official_rule_versions,
    count(*) FILTER (WHERE official_close IS NULL) AS official_close_missing,
    count(*) FILTER (WHERE official_close IS NOT NULL AND official_close <= 0) AS official_close_invalid,
    sum(event_count) AS exact_end_events,
    count(*) FILTER (WHERE price_variants = 0) AS exact_end_missing_markets,
    count(*) FILTER (WHERE price_variants = 1) AS exact_end_unique_markets,
    count(*) FILTER (WHERE price_variants > 1) AS exact_end_conflicting_markets,
    count(*) FILTER (WHERE comparable) AS comparable_markets,
    count(*) FILTER (WHERE comparable AND boundary_price = official_close) AS exact_price_matches,
    count(*) FILTER (WHERE comparable AND boundary_price <> official_close) AS price_mismatches,
    max(CASE WHEN comparable THEN abs(boundary_price - official_close) END) AS maximum_absolute_price_error,
    max(CASE WHEN comparable THEN 10000 * abs(boundary_price - official_close) / official_close END) AS maximum_absolute_error_bps
FROM measured
) TO STDOUT WITH (FORMAT CSV, HEADER true);

COMMIT;
