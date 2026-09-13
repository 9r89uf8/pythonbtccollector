-- Two bounded fixed-week timing diagnostics. No price-response inference.
-- Match supplied section 15's inclusive sample-second endpoints.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '20s';
SET LOCAL lock_timeout = '2s';

DO $identity$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM instruments i JOIN providers p USING (provider_id)
        WHERE i.instrument_id = 1 AND p.provider_code = 'binance_spot'
            AND i.symbol = 'BTCUSDT' AND i.stream_name = 'btcusdt@ticker'
    ) THEN RAISE EXCEPTION 'Unexpected Binance instrument identity'; END IF;
END;
$identity$;

\echo BINANCE_PROFILE_BEGIN
COPY (
WITH b AS (
    SELECT sample_second_ms, provider_event_ms, received_ms, price,
        lag(sample_second_ms) OVER w AS previous_sample_ms,
        lag(provider_event_ms) OVER w AS previous_source_ms,
        lag(received_ms) OVER w AS previous_received_ms,
        lag(price) OVER w AS previous_price
    FROM price_samples
    WHERE instrument_id = 1
        AND sample_second_ms BETWEEN 1788220800000 AND 1788825600000
    WINDOW w AS (ORDER BY sample_second_ms)
)
SELECT
    (extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS snapshot_ms,
    current_setting('transaction_read_only') AS transaction_read_only,
    current_setting('transaction_isolation') AS transaction_isolation,
    current_setting('statement_timeout') AS statement_timeout,
    count(*) AS sample_rows,
    count(*) FILTER (WHERE provider_event_ms IS NULL) AS missing_source_clock_rows,
    min(sample_second_ms) AS first_sample_ms,
    max(sample_second_ms) AS last_sample_ms,
    min(provider_event_ms) AS first_provider_event_ms,
    max(provider_event_ms) AS last_provider_event_ms,
    count(*) FILTER (WHERE sample_second_ms <> provider_event_ms / 1000 * 1000) AS sampler_key_differs_from_provider_second_rows,
    count(*) FILTER (WHERE provider_event_ms = previous_source_ms
        AND received_ms = previous_received_ms AND price = previous_price) AS repeated_cached_state_adjacent_rows,
    count(*) FILTER (WHERE sample_second_ms - previous_sample_ms = 1000
        AND provider_event_ms = previous_source_ms AND received_ms = previous_received_ms
        AND price = previous_price) AS repeated_cached_state_consecutive_second_rows,
    percentile_cont(ARRAY[0.1,0.5,0.9,0.99]) WITHIN GROUP (ORDER BY sample_second_ms - received_ms) AS sampler_minus_receipt_ms_p10_p50_p90_p99,
    min(sample_second_ms - received_ms) AS sampler_minus_receipt_ms_min,
    max(sample_second_ms - received_ms) AS sampler_minus_receipt_ms_max,
    percentile_cont(ARRAY[0.1,0.5,0.9,0.99]) WITHIN GROUP (ORDER BY sample_second_ms - provider_event_ms) AS sampler_minus_provider_ms_p10_p50_p90_p99,
    percentile_cont(ARRAY[0.1,0.5,0.9,0.99]) WITHIN GROUP (ORDER BY received_ms - provider_event_ms) AS receipt_minus_provider_ms_p10_p50_p90_p99
FROM b
) TO STDOUT WITH (FORMAT CSV, HEADER true);
\echo BINANCE_PROFILE_END

\echo BINANCE_PAIRING_BEGIN
COPY (
WITH b AS (
    SELECT sample_second_ms AS t, received_ms AS r
    FROM price_samples
    WHERE instrument_id = 1
        AND sample_second_ms BETWEEN 1788220800000 AND 1788825600000
), e AS (
    SELECT provider_event_ms AS t, min(received_wall_ns) / 1000000 AS r
    FROM polymarket_twap_events
    WHERE symbol = 'btc/usd' AND window_s = 60 AND topic = 'crypto_prices_twap_sixty'
        AND provider_event_ms BETWEEN 1788220800000 AND 1788825610000
    GROUP BY provider_event_ms
)
SELECT k.k AS twap_stamp_offset_s, count(*) AS n_pairs,
    percentile_cont(0.1) WITHIN GROUP (ORDER BY e.r - b.r) AS p10_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY e.r - b.r) AS p50_ms,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY e.r - b.r) AS p90_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY e.r - b.r) AS p99_ms
FROM b CROSS JOIN (VALUES (3), (4)) k(k)
JOIN e ON e.t = b.t + k.k * 1000
GROUP BY k.k ORDER BY k.k
) TO STDOUT WITH (FORMAT CSV, HEADER true);
\echo BINANCE_PAIRING_END

COMMIT;
\echo BINANCE_REVIEW_DONE
