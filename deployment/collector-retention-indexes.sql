-- Run with psql -v ON_ERROR_STOP=1, outside a transaction, before schema.sql
-- on an existing installation. Prebuild the two nonempty evidence indexes
-- without blocking its collector writes. schema.sql then sees them as present.
CREATE INDEX CONCURRENTLY IF NOT EXISTS polymarket_market_observations_payload_idx
    ON public.polymarket_market_observations (payload_hash);
CREATE INDEX CONCURRENTLY IF NOT EXISTS polymarket_evidence_payloads_retention_idx
    ON public.polymarket_evidence_payloads (created_at);

-- psql executes this fixed statement only if the optional retired table exists.
SELECT 'CREATE INDEX CONCURRENTLY IF NOT EXISTS chainlink_twap_shadow_retention_market_idx ON public.chainlink_twap_shadow_predictions (market_id)'
WHERE to_regclass('public.chainlink_twap_shadow_predictions') IS NOT NULL
\gexec
