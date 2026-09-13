-- Causal retained-quote companion to the H3 leader-risk observations.
-- sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -d price_collector \
--   -v start_utc=2026-08-16T00:00:00Z -v end_utc=2026-09-12T21:15:00Z \
--   -f extract_quotes.sql > quotes.csv
-- Use the same explicit cohort boundaries as the base observations. There is no
-- hidden date floor. statement_timeout defaults to 60s; an explicit override such
-- as -v statement_timeout=180s changes this run only, with no automatic increase.
-- Accept output only after psql exits zero and validation finds eight distinct
-- checkpoints per market and a distinct market count equal to cohort_market_count.
-- Quotes are read in this transaction's snapshot, not replayed from an old state.
-- Select a retained sample before checking token identity or component freshness.
-- Missing, stale, future-source and mismatched-token observations remain visible.

\set ON_ERROR_STOP on
\if :{?statement_timeout}
\else
\set statement_timeout '60s'
\endif

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = :'statement_timeout';
SET LOCAL lock_timeout = '2s';

COPY (
WITH bounds AS (
    SELECT (extract(epoch FROM :'start_utc'::timestamptz) * 1000)::bigint AS start_ms,
           (extract(epoch FROM :'end_utc'::timestamptz) * 1000)::bigint AS end_ms,
           (extract(epoch FROM transaction_timestamp()) * 1000)::bigint AS quote_extraction_ms
), markets AS (
    SELECT m.market_id, m.up_token_id, m.down_token_id,
           mw.market_end_ms AS end_ms, count(*) OVER () AS cohort_market_count,
           b.start_ms AS requested_cohort_start_ms, b.start_ms AS cohort_start_ms,
           b.end_ms AS cohort_end_ms, b.quote_extraction_ms
    FROM polymarket_btc_5m_markets m
    JOIN market_windows mw USING (market_id)
    CROSS JOIN bounds b
    WHERE mw.market_start_ms >= b.start_ms AND mw.market_start_ms < b.end_ms
      AND mw.market_end_ms <= b.end_ms AND mw.market_end_ms <= b.quote_extraction_ms
), cuts AS (
    SELECT m.*, t.t_sec, m.end_ms - t.t_sec * 1000 AS cut_ms
    FROM markets m
    CROSS JOIN (VALUES (120), (90), (60), (30), (15), (10), (5), (3)) t(t_sec)
)
SELECT c.market_id, c.t_sec, c.cut_ms, c.quote_extraction_ms,
       p.sample_second_ms AS quote_sample_ms, p.received_ms AS quote_received_ms,
       p.up_token_id = c.up_token_id AND p.down_token_id = c.down_token_id AS tokens_match,
       p.up_bid, p.up_ask, p.down_bid, p.down_ask,
       p.up_provider_event_ms, p.up_received_ms, p.down_provider_event_ms, p.down_received_ms,
       p.raw->>'event_type' AS event_type,
       p.raw->>'up_bid_provider_event_ms' AS up_bid_source_ms,
       p.raw->>'up_bid_received_ms' AS up_bid_received_ms,
       p.raw->>'up_ask_provider_event_ms' AS up_ask_source_ms,
       p.raw->>'up_ask_received_ms' AS up_ask_received_ms,
       p.raw->>'down_bid_provider_event_ms' AS down_bid_source_ms,
       p.raw->>'down_bid_received_ms' AS down_bid_received_ms,
       p.raw->>'down_ask_provider_event_ms' AS down_ask_source_ms,
       p.raw->>'down_ask_received_ms' AS down_ask_received_ms,
       c.requested_cohort_start_ms, c.cohort_start_ms, c.cohort_end_ms,
       c.cohort_market_count
FROM cuts c
LEFT JOIN LATERAL (
    -- Ten seconds bounds the sampled-history search, not quote freshness.
    -- A row at the cutoff may contain later-received components, so both the
    -- sample clock and the row's newest-event receipt must be at/before cutoff.
    -- Never filter ask/source ages, missing sides or token identity here: doing
    -- so could replace a latest unusable observation with an older fresh quote.
    SELECT p.*
    FROM polymarket_probability_samples p
    WHERE p.market_id = c.market_id AND p.source = 'polymarket_clob'
      AND p.sample_second_ms BETWEEN c.cut_ms - 10000 AND c.cut_ms
      AND p.received_ms <= c.cut_ms
    ORDER BY p.sample_second_ms DESC LIMIT 1
) p ON true
ORDER BY c.market_id, c.t_sec DESC
) TO STDOUT WITH (FORMAT CSV, HEADER true);

COMMIT;
