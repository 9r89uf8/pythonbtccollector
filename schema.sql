CREATE TABLE IF NOT EXISTS providers (
    provider_id SMALLSERIAL PRIMARY KEY,
    provider_code TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id BIGSERIAL PRIMARY KEY,
    provider_id SMALLINT NOT NULL REFERENCES providers(provider_id),
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider_id, symbol)
);

CREATE TABLE IF NOT EXISTS market_windows (
    market_id BIGINT PRIMARY KEY,
    market_start_ms BIGINT NOT NULL UNIQUE,
    market_end_ms BIGINT NOT NULL,
    market_start_at TIMESTAMPTZ NOT NULL,
    market_end_at TIMESTAMPTZ NOT NULL,

    CHECK (market_start_ms % 300000 = 0),
    CHECK (market_end_ms = market_start_ms + 300000),
    CHECK (market_id = market_start_ms / 300000)
);

CREATE TABLE IF NOT EXISTS polymarket_btc_5m_markets (
    market_id BIGINT PRIMARY KEY REFERENCES market_windows(market_id),

    slug TEXT NOT NULL UNIQUE,
    gamma_event_id TEXT,
    gamma_market_id TEXT,
    condition_id TEXT,

    question TEXT,
    start_ms BIGINT,
    end_ms BIGINT,
    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,

    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,
    up_outcome TEXT NOT NULL DEFAULT 'Up',
    down_outcome TEXT NOT NULL DEFAULT 'Down',

    active BOOLEAN,
    closed BOOLEAN,
    archived BOOLEAN,

    raw_gamma JSONB,
    first_seen_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (market_id >= 0)
);

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_markets_slug_idx
    ON polymarket_btc_5m_markets (slug);

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_markets_tokens_idx
    ON polymarket_btc_5m_markets (up_token_id, down_token_id);

CREATE TABLE IF NOT EXISTS polymarket_btc_5m_resolutions (
    market_id BIGINT PRIMARY KEY REFERENCES polymarket_btc_5m_markets(market_id),

    resolution_status TEXT NOT NULL DEFAULT 'pending',
    resolution_type TEXT,

    chainlink_open_price NUMERIC(38, 18),
    chainlink_close_price NUMERIC(38, 18),
    chainlink_source TEXT,

    winner TEXT,
    winning_token_id TEXT,
    up_payout NUMERIC(18, 8),
    down_payout NUMERIC(18, 8),

    resolved_at_ms BIGINT,
    resolution_source TEXT,
    raw_resolution JSONB,

    first_checked_ms BIGINT NOT NULL,
    last_checked_ms BIGINT NOT NULL,
    next_check_ms BIGINT,
    resolution_attempts INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (resolution_status IN ('pending', 'resolved')),
    CHECK (resolution_type IS NULL OR resolution_type IN ('winner', 'split')),
    CHECK (chainlink_open_price IS NULL OR chainlink_open_price > 0),
    CHECK (chainlink_close_price IS NULL OR chainlink_close_price > 0),
    CHECK (
        (chainlink_open_price IS NULL AND chainlink_close_price IS NULL
            AND chainlink_source IS NULL)
        OR
        ((chainlink_open_price IS NOT NULL OR chainlink_close_price IS NOT NULL)
            AND chainlink_source IS NOT NULL)
    ),
    CHECK (up_payout IS NULL OR (up_payout >= 0 AND up_payout <= 1)),
    CHECK (down_payout IS NULL OR (down_payout >= 0 AND down_payout <= 1)),
    CHECK (resolved_at_ms IS NULL OR resolved_at_ms >= 0),
    CHECK (last_checked_ms >= first_checked_ms),
    CHECK (next_check_ms IS NULL OR next_check_ms >= last_checked_ms),
    CHECK (resolution_attempts >= 0),
    CHECK (
        (
            resolution_status = 'pending'
            AND resolution_type IS NULL
            AND winner IS NULL
            AND winning_token_id IS NULL
            AND up_payout IS NULL
            AND down_payout IS NULL
            AND resolved_at_ms IS NULL
            AND resolution_source IS NULL
        )
        OR
        (
            resolution_status = 'resolved'
            AND resolution_type IS NOT NULL
            AND resolution_type = 'winner'
            AND resolution_source IS NOT NULL
            AND winner IS NOT NULL
            AND winning_token_id IS NOT NULL
            AND up_payout IS NOT NULL
            AND down_payout IS NOT NULL
            AND (
                (winner = 'Up' AND up_payout = 1 AND down_payout = 0)
                OR
                (winner = 'Down' AND up_payout = 0 AND down_payout = 1)
            )
        )
        OR
        (
            resolution_status = 'resolved'
            AND resolution_type IS NOT NULL
            AND resolution_type = 'split'
            AND winner IS NULL
            AND winning_token_id IS NULL
            AND up_payout IS NOT NULL
            AND down_payout IS NOT NULL
            AND up_payout = 0.5
            AND down_payout = 0.5
            AND resolution_source IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS polymarket_btc_5m_resolutions_due_idx
    ON polymarket_btc_5m_resolutions (next_check_ms, market_id)
    WHERE next_check_ms IS NOT NULL;

CREATE TABLE IF NOT EXISTS price_samples (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    price NUMERIC(38, 18) NOT NULL,
    provider_event_ms BIGINT,
    received_ms BIGINT NOT NULL,

    source_price_field TEXT NOT NULL DEFAULT 'c',
    provider_message_ms BIGINT,
    source_topic TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (instrument_id, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (price > 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000)
);

ALTER TABLE price_samples
ADD COLUMN IF NOT EXISTS provider_message_ms BIGINT;

ALTER TABLE price_samples
ADD COLUMN IF NOT EXISTS source_topic TEXT;

CREATE INDEX IF NOT EXISTS price_samples_market_idx
    ON price_samples (market_id, instrument_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS price_samples_instrument_latest_idx
    ON price_samples (instrument_id, sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS polymarket_probability_samples (
    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    source TEXT NOT NULL DEFAULT 'polymarket_clob',

    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    up_token_id TEXT NOT NULL,
    down_token_id TEXT NOT NULL,

    up_bid NUMERIC(18, 8),
    up_ask NUMERIC(18, 8),
    up_mid NUMERIC(18, 8),

    down_bid NUMERIC(18, 8),
    down_ask NUMERIC(18, 8),
    down_mid NUMERIC(18, 8),

    up_prob_norm NUMERIC(18, 8),
    down_prob_norm NUMERIC(18, 8),

    provider_event_ms BIGINT,
    received_ms BIGINT NOT NULL,

    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (market_id, source, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),

    CHECK (up_bid IS NULL OR (up_bid >= 0 AND up_bid <= 1)),
    CHECK (up_ask IS NULL OR (up_ask >= 0 AND up_ask <= 1)),
    CHECK (up_mid IS NULL OR (up_mid >= 0 AND up_mid <= 1)),

    CHECK (down_bid IS NULL OR (down_bid >= 0 AND down_bid <= 1)),
    CHECK (down_ask IS NULL OR (down_ask >= 0 AND down_ask <= 1)),
    CHECK (down_mid IS NULL OR (down_mid >= 0 AND down_mid <= 1)),

    CHECK (up_prob_norm IS NULL OR (up_prob_norm >= 0 AND up_prob_norm <= 1)),
    CHECK (down_prob_norm IS NULL OR (down_prob_norm >= 0 AND down_prob_norm <= 1))
);

CREATE INDEX IF NOT EXISTS polymarket_probability_samples_market_idx
    ON polymarket_probability_samples (market_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS polymarket_probability_samples_latest_idx
    ON polymarket_probability_samples (sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS binance_futures_snapshots (
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    futures_last_price NUMERIC(38, 18),
    futures_last_price_time_ms BIGINT,

    mark_price NUMERIC(38, 18),
    index_price NUMERIC(38, 18),
    last_funding_rate NUMERIC(38, 18),
    next_funding_time_ms BIGINT,
    premium_index_time_ms BIGINT,

    open_interest NUMERIC(38, 18),
    open_interest_time_ms BIGINT,

    oi_notional_usdt NUMERIC(38, 18),
    premium_bps NUMERIC(20, 8),

    received_ms BIGINT NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (open_interest IS NULL OR open_interest >= 0)
);

CREATE INDEX IF NOT EXISTS binance_futures_snapshots_market_idx
    ON binance_futures_snapshots (market_id, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_futures_snapshots_latest_idx
    ON binance_futures_snapshots (symbol, sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS binance_flow_1s (
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    buy_base NUMERIC(38, 18) NOT NULL DEFAULT 0,
    sell_base NUMERIC(38, 18) NOT NULL DEFAULT 0,
    buy_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    sell_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,

    delta_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    total_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    taker_imbalance NUMERIC(20, 8),

    cvd_quote NUMERIC(38, 18) NOT NULL DEFAULT 0,
    cvd_10s NUMERIC(38, 18) NOT NULL DEFAULT 0,
    cvd_30s NUMERIC(38, 18) NOT NULL DEFAULT 0,
    imbalance_10s NUMERIC(20, 8),
    imbalance_30s NUMERIC(20, 8),

    agg_trade_count INTEGER NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    max_trade_quote NUMERIC(38, 18),

    first_agg_trade_id BIGINT,
    last_agg_trade_id BIGINT,
    last_trade_time_ms BIGINT,
    last_event_time_ms BIGINT,
    received_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (venue, symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (buy_base >= 0),
    CHECK (sell_base >= 0),
    CHECK (buy_quote >= 0),
    CHECK (sell_quote >= 0),
    CHECK (total_quote >= 0),
    CHECK (agg_trade_count >= 0),
    CHECK (trade_count >= 0),
    CHECK (taker_imbalance IS NULL OR (taker_imbalance >= -1 AND taker_imbalance <= 1)),
    CHECK (imbalance_10s IS NULL OR (imbalance_10s >= -1 AND imbalance_10s <= 1)),
    CHECK (imbalance_30s IS NULL OR (imbalance_30s >= -1 AND imbalance_30s <= 1))
);

CREATE INDEX IF NOT EXISTS binance_flow_1s_market_idx
    ON binance_flow_1s (market_id, venue, symbol, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_flow_1s_latest_idx
    ON binance_flow_1s (venue, symbol, sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS binance_book_1s (
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,

    market_id BIGINT NOT NULL REFERENCES market_windows(market_id),
    sample_second_ms BIGINT NOT NULL,
    sample_second_at TIMESTAMPTZ NOT NULL,

    bid NUMERIC(38, 18) NOT NULL,
    ask NUMERIC(38, 18) NOT NULL,
    bid_qty NUMERIC(38, 18) NOT NULL,
    ask_qty NUMERIC(38, 18) NOT NULL,

    mid NUMERIC(38, 18) NOT NULL,
    spread NUMERIC(38, 18) NOT NULL,
    spread_bps NUMERIC(20, 8) NOT NULL,
    book_imbalance NUMERIC(20, 8),
    microprice NUMERIC(38, 18),

    update_id BIGINT,
    event_time_ms BIGINT,
    transaction_time_ms BIGINT,
    received_ms BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (venue, symbol, sample_second_ms),

    CHECK (sample_second_ms % 1000 = 0),
    CHECK (sample_second_ms >= market_id * 300000),
    CHECK (sample_second_ms < (market_id + 1) * 300000),
    CHECK (bid > 0),
    CHECK (ask > 0),
    CHECK (ask >= bid),
    CHECK (bid_qty >= 0),
    CHECK (ask_qty >= 0),
    CHECK (mid > 0),
    CHECK (spread >= 0),
    CHECK (spread_bps >= 0),
    CHECK (book_imbalance IS NULL OR (book_imbalance >= -1 AND book_imbalance <= 1))
);

CREATE INDEX IF NOT EXISTS binance_book_1s_market_idx
    ON binance_book_1s (market_id, venue, symbol, sample_second_ms);

CREATE INDEX IF NOT EXISTS binance_book_1s_latest_idx
    ON binance_book_1s (venue, symbol, sample_second_ms DESC);

CREATE TABLE IF NOT EXISTS binance_futures_oi_5m_summaries (
    symbol TEXT NOT NULL,

    source_window_start_ms BIGINT NOT NULL,
    source_window_end_ms BIGINT NOT NULL,

    effective_market_id BIGINT NOT NULL REFERENCES market_windows(market_id),

    binance_timestamp_ms BIGINT NOT NULL,

    sum_open_interest NUMERIC(38, 18),
    sum_open_interest_value NUMERIC(38, 18),

    received_ms BIGINT NOT NULL,
    raw JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, source_window_start_ms, source_window_end_ms),

    CHECK (source_window_end_ms = source_window_start_ms + 300000),
    CHECK (source_window_start_ms % 300000 = 0)
);

CREATE INDEX IF NOT EXISTS binance_futures_oi_5m_effective_market_idx
    ON binance_futures_oi_5m_summaries (effective_market_id);

-- Matured shadow forecasts are internal model evidence. The standalone shadow
-- worker owns the base-table write path. The API reader receives only the
-- narrow reporting view declared after this table.
CREATE TABLE IF NOT EXISTS shadow_signal_evaluations (
    selection_schema_version INTEGER NOT NULL,
    selection_policy_version TEXT NOT NULL,
    selection_fingerprint_sha256 TEXT NOT NULL,
    selection_artifact_sha256 TEXT NOT NULL,
    selection_evidence_end_ms BIGINT NOT NULL,

    model_version TEXT NOT NULL,
    beta NUMERIC(38, 18) NOT NULL,

    generated_ms BIGINT NOT NULL,
    target_ms BIGINT NOT NULL,
    matured_ms BIGINT NOT NULL,
    horizon_ms BIGINT NOT NULL,

    valid BOOLEAN NOT NULL,
    status TEXT NOT NULL,
    invalid_reasons TEXT[] NOT NULL,
    state TEXT NOT NULL,
    outcome_status TEXT NOT NULL,
    outcome_invalid_reasons TEXT[] NOT NULL,

    market_id BIGINT NOT NULL,
    market_start_ms BIGINT NOT NULL,
    market_end_ms BIGINT NOT NULL,
    ms_to_market_end BIGINT NOT NULL,
    full_horizon_before_market_end BOOLEAN NOT NULL,

    chainlink_at_forecast NUMERIC(38, 18),
    chainlink_at_forecast_source_timestamp_ms BIGINT,
    chainlink_at_forecast_received_ms BIGINT,

    projected_chainlink NUMERIC(38, 18),
    pending_move NUMERIC(38, 18),
    pending_move_bps NUMERIC(38, 18),
    direction TEXT,

    futures_now NUMERIC(38, 18),
    futures_now_source_timestamp_ms BIGINT,
    futures_now_received_ms BIGINT,

    futures_reference NUMERIC(38, 18),
    futures_reference_source_timestamp_ms BIGINT,
    futures_reference_received_ms BIGINT,
    futures_reference_target_ms BIGINT,
    futures_reference_gap_ms BIGINT,

    futures_received_age_ms BIGINT,
    chainlink_received_age_ms BIGINT,

    actual_chainlink NUMERIC(38, 18),
    actual_chainlink_source_timestamp_ms BIGINT,
    actual_chainlink_received_ms BIGINT,
    actual_chainlink_age_at_target_ms BIGINT,

    forecast_error NUMERIC(38, 18),
    baseline_error NUMERIC(38, 18),

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (model_version, generated_ms, horizon_ms),

    CHECK (selection_schema_version > 0),
    CHECK (btrim(selection_policy_version) <> ''),
    CHECK (selection_fingerprint_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (selection_artifact_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        selection_evidence_end_ms >= 0
        AND selection_evidence_end_ms <= generated_ms
    ),
    CHECK (btrim(model_version) <> ''),
    CHECK (beta >= 0 AND beta <> 'NaN'::NUMERIC),
    CHECK (generated_ms >= 0),
    CHECK (horizon_ms > 0),
    CHECK (target_ms = generated_ms + horizon_ms),
    CHECK (matured_ms >= target_ms),
    CHECK (btrim(status) <> ''),
    CHECK (valid = (status = 'valid')),
    CHECK (array_position(invalid_reasons, NULL) IS NULL),
    CHECK (
        (valid AND cardinality(invalid_reasons) = 0)
        OR (NOT valid AND cardinality(invalid_reasons) > 0)
    ),
    CHECK (btrim(state) <> ''),
    CHECK (market_id >= 0),
    CHECK (market_start_ms = market_id * 300000),
    CHECK (market_end_ms = market_start_ms + 300000),
    CHECK (
        generated_ms >= market_start_ms
        AND generated_ms < market_end_ms
    ),
    CHECK (ms_to_market_end = market_end_ms - generated_ms),
    CHECK (full_horizon_before_market_end = (target_ms <= market_end_ms)),
    CHECK (
        (
            chainlink_at_forecast IS NULL
            AND chainlink_at_forecast_source_timestamp_ms IS NULL
            AND chainlink_at_forecast_received_ms IS NULL
        )
        OR (
            chainlink_at_forecast IS NOT NULL
            AND chainlink_at_forecast > 0
            AND chainlink_at_forecast <> 'NaN'::NUMERIC
            AND chainlink_at_forecast_received_ms IS NOT NULL
            AND chainlink_at_forecast_received_ms >= 0
            AND (
                chainlink_at_forecast_source_timestamp_ms IS NULL
                OR chainlink_at_forecast_source_timestamp_ms >= 0
            )
        )
    ),
    CHECK (
        (
            futures_now IS NULL
            AND futures_now_source_timestamp_ms IS NULL
            AND futures_now_received_ms IS NULL
        )
        OR (
            futures_now IS NOT NULL
            AND futures_now > 0
            AND futures_now <> 'NaN'::NUMERIC
            AND futures_now_received_ms IS NOT NULL
            AND futures_now_received_ms >= 0
            AND (
                futures_now_source_timestamp_ms IS NULL
                OR futures_now_source_timestamp_ms >= 0
            )
        )
    ),
    CHECK (
        (
            futures_reference IS NULL
            AND futures_reference_source_timestamp_ms IS NULL
            AND futures_reference_received_ms IS NULL
        )
        OR (
            futures_reference IS NOT NULL
            AND futures_reference > 0
            AND futures_reference <> 'NaN'::NUMERIC
            AND futures_reference_received_ms IS NOT NULL
            AND futures_reference_received_ms >= 0
            AND (
                futures_reference_source_timestamp_ms IS NULL
                OR futures_reference_source_timestamp_ms >= 0
            )
        )
    ),
    CHECK (
        futures_reference_target_ms IS NULL
        OR futures_reference_target_ms >= 0
    ),
    CHECK (
        futures_reference_gap_ms IS NULL
        OR (
            futures_reference_target_ms IS NOT NULL
            AND futures_reference_gap_ms >= 0
        )
    ),
    CHECK (futures_received_age_ms IS NULL OR futures_received_age_ms >= 0),
    CHECK (chainlink_received_age_ms IS NULL OR chainlink_received_age_ms >= 0),
    CHECK (
        NOT valid
        OR chainlink_at_forecast_received_ms <= generated_ms
    ),
    CHECK (
        NOT valid
        OR futures_now_received_ms <= generated_ms
    ),
    CHECK (
        (
            actual_chainlink IS NULL
            AND actual_chainlink_source_timestamp_ms IS NULL
            AND actual_chainlink_received_ms IS NULL
            AND actual_chainlink_age_at_target_ms IS NULL
        )
        OR (
            actual_chainlink IS NOT NULL
            AND actual_chainlink > 0
            AND actual_chainlink <> 'NaN'::NUMERIC
            AND actual_chainlink_received_ms IS NOT NULL
            AND actual_chainlink_received_ms >= 0
            AND actual_chainlink_received_ms <= target_ms
            AND actual_chainlink_age_at_target_ms IS NOT NULL
            AND actual_chainlink_age_at_target_ms
                = target_ms - actual_chainlink_received_ms
            AND (
                actual_chainlink_source_timestamp_ms IS NULL
                OR actual_chainlink_source_timestamp_ms >= 0
            )
        )
    ),
    CONSTRAINT shadow_signal_evaluations_projection_consistency_check CHECK (
        (
            NOT valid
            AND projected_chainlink IS NULL
            AND pending_move IS NULL
            AND pending_move_bps IS NULL
            AND direction IS NULL
        )
        OR (
            valid
            AND chainlink_at_forecast IS NOT NULL
            AND futures_now IS NOT NULL
            AND futures_reference IS NOT NULL
            AND futures_reference_target_ms IS NOT NULL
            AND futures_reference_gap_ms IS NOT NULL
            AND projected_chainlink IS NOT NULL
            AND projected_chainlink > 0
            AND projected_chainlink <> 'NaN'::NUMERIC
            AND pending_move IS NOT NULL
            AND pending_move <> 'NaN'::NUMERIC
            AND pending_move_bps IS NOT NULL
            AND pending_move_bps <> 'NaN'::NUMERIC
            AND direction IS NOT NULL
            AND direction IN ('up', 'down', 'flat')
            AND abs(
                pending_move - (projected_chainlink - chainlink_at_forecast)
            ) <= 0.000000000000000002
            AND abs(
                pending_move_bps
                - (pending_move * 10000 / chainlink_at_forecast)
            ) <= 0.000000000000000010
            AND (
                (pending_move > 0 AND direction = 'up')
                OR (pending_move < 0 AND direction = 'down')
                OR (pending_move = 0 AND direction = 'flat')
            )
        )
    ),
    CONSTRAINT shadow_signal_evaluations_forecast_error_consistency_check CHECK (
        (
            (projected_chainlink IS NULL OR actual_chainlink IS NULL)
            AND forecast_error IS NULL
        )
        OR (
            projected_chainlink IS NOT NULL
            AND actual_chainlink IS NOT NULL
            AND forecast_error IS NOT NULL
            AND forecast_error <> 'NaN'::NUMERIC
            AND abs(
                forecast_error - (projected_chainlink - actual_chainlink)
            ) <= 0.000000000000000002
        )
    ),
    CONSTRAINT shadow_signal_evaluations_baseline_error_consistency_check CHECK (
        (
            (chainlink_at_forecast IS NULL OR actual_chainlink IS NULL)
            AND baseline_error IS NULL
        )
        OR (
            chainlink_at_forecast IS NOT NULL
            AND actual_chainlink IS NOT NULL
            AND baseline_error IS NOT NULL
            AND baseline_error <> 'NaN'::NUMERIC
            AND abs(
                baseline_error - (chainlink_at_forecast - actual_chainlink)
            ) <= 0.000000000000000002
        )
    ),
    CONSTRAINT shadow_signal_evaluations_outcome_consistency_check CHECK (
        btrim(outcome_status) <> ''
        AND array_position(outcome_invalid_reasons, NULL) IS NULL
        AND array_position(outcome_invalid_reasons, '') IS NULL
        AND (
            (
                outcome_status = 'available'
                AND cardinality(outcome_invalid_reasons) = 0
                AND actual_chainlink IS NOT NULL
            )
            OR (
                outcome_status = 'unavailable'
                AND cardinality(outcome_invalid_reasons) = 0
                AND actual_chainlink IS NULL
                AND actual_chainlink_source_timestamp_ms IS NULL
                AND actual_chainlink_received_ms IS NULL
                AND actual_chainlink_age_at_target_ms IS NULL
                AND forecast_error IS NULL
                AND baseline_error IS NULL
            )
            OR (
                outcome_status = 'integrity_invalid'
                AND cardinality(outcome_invalid_reasons) > 0
                AND actual_chainlink IS NULL
                AND actual_chainlink_source_timestamp_ms IS NULL
                AND actual_chainlink_received_ms IS NULL
                AND actual_chainlink_age_at_target_ms IS NULL
                AND forecast_error IS NULL
                AND baseline_error IS NULL
            )
            OR (
                outcome_status = 'legacy_unverified'
                AND outcome_invalid_reasons =
                    ARRAY['pre_cohort_integrity_fix_unverified']::TEXT[]
            )
        )
    )
);

-- Rows written before cohort-wide outcome finalization cannot be proven clean:
-- a short-horizon actual may have been staged before a later continuity reset.
-- Label those rows without rewriting their large numeric payload, then exclude
-- them from the reader view below. PostgreSQL fast-default column addition keeps
-- this bounded on an existing multi-day evaluation table.
-- The worker must be stopped while this migration is applied so the old insert
-- contract cannot create another row with missing outcome metadata mid-migration.
BEGIN;

ALTER TABLE public.shadow_signal_evaluations
    ADD COLUMN IF NOT EXISTS outcome_status TEXT NOT NULL
        DEFAULT 'legacy_unverified';
ALTER TABLE public.shadow_signal_evaluations
    ADD COLUMN IF NOT EXISTS outcome_invalid_reasons TEXT[] NOT NULL
        DEFAULT ARRAY['pre_cohort_integrity_fix_unverified']::TEXT[];

UPDATE public.shadow_signal_evaluations
SET outcome_status = 'legacy_unverified',
    outcome_invalid_reasons =
        ARRAY['pre_cohort_integrity_fix_unverified']::TEXT[]
WHERE outcome_status IS NULL
   OR outcome_invalid_reasons IS NULL;

ALTER TABLE public.shadow_signal_evaluations
    ALTER COLUMN outcome_status SET NOT NULL;
ALTER TABLE public.shadow_signal_evaluations
    ALTER COLUMN outcome_invalid_reasons SET NOT NULL;
ALTER TABLE public.shadow_signal_evaluations
    ALTER COLUMN outcome_status DROP DEFAULT;
ALTER TABLE public.shadow_signal_evaluations
    ALTER COLUMN outcome_invalid_reasons DROP DEFAULT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = to_regclass('public.shadow_signal_evaluations')
          AND conname =
            'shadow_signal_evaluations_outcome_consistency_check'
    ) THEN
        ALTER TABLE public.shadow_signal_evaluations
            ADD CONSTRAINT
                shadow_signal_evaluations_outcome_consistency_check
            CHECK (
                btrim(outcome_status) <> ''
                AND array_position(outcome_invalid_reasons, NULL) IS NULL
                AND array_position(outcome_invalid_reasons, '') IS NULL
                AND (
                    (
                        outcome_status = 'available'
                        AND cardinality(outcome_invalid_reasons) = 0
                        AND actual_chainlink IS NOT NULL
                    )
                    OR (
                        outcome_status = 'unavailable'
                        AND cardinality(outcome_invalid_reasons) = 0
                        AND actual_chainlink IS NULL
                        AND actual_chainlink_source_timestamp_ms IS NULL
                        AND actual_chainlink_received_ms IS NULL
                        AND actual_chainlink_age_at_target_ms IS NULL
                        AND forecast_error IS NULL
                        AND baseline_error IS NULL
                    )
                    OR (
                        outcome_status = 'integrity_invalid'
                        AND cardinality(outcome_invalid_reasons) > 0
                        AND actual_chainlink IS NULL
                        AND actual_chainlink_source_timestamp_ms IS NULL
                        AND actual_chainlink_received_ms IS NULL
                        AND actual_chainlink_age_at_target_ms IS NULL
                        AND forecast_error IS NULL
                        AND baseline_error IS NULL
                    )
                    OR (
                        outcome_status = 'legacy_unverified'
                        AND outcome_invalid_reasons =
                            ARRAY[
                                'pre_cohort_integrity_fix_unverified'
                            ]::TEXT[]
                    )
                )
            );
    END IF;
END
$$;

-- Replace the reader boundary in the same transaction as legacy labeling.
-- This remains an owner-rights view: security_invoker must not be enabled
-- because price_reader has no base-table privilege. Dashboard reporting
-- intentionally omits futures inputs, writer metadata, retention controls,
-- and created_at. Selection-version and outcome-integrity provenance are part
-- of the reader contract so unavailable and integrity-invalid targets remain
-- distinguishable without granting access to the base table.
CREATE OR REPLACE VIEW public.shadow_signal_evaluation_chart_points
WITH (security_barrier = true) AS
SELECT
    selection_fingerprint_sha256,
    selection_artifact_sha256,
    model_version,
    beta,
    generated_ms,
    target_ms,
    matured_ms,
    horizon_ms,
    valid,
    status,
    invalid_reasons,
    state,
    market_id AS forecast_market_id,
    full_horizon_before_market_end
        AS full_horizon_before_forecast_market_end,
    chainlink_at_forecast,
    projected_chainlink,
    actual_chainlink,
    actual_chainlink_source_timestamp_ms,
    actual_chainlink_received_ms,
    actual_chainlink_age_at_target_ms,
    pending_move,
    pending_move_bps,
    direction,
    forecast_error,
    baseline_error,
    -- New reader columns must remain append-only so CREATE OR REPLACE VIEW is
    -- compatible with the already-deployed column order.
    selection_schema_version,
    selection_policy_version,
    selection_evidence_end_ms,
    outcome_status,
    outcome_invalid_reasons
FROM public.shadow_signal_evaluations
WHERE outcome_status IN ('available', 'unavailable', 'integrity_invalid');

COMMIT;

-- Phase 5 initially shipped the projection consistency check without a name.
-- PostgreSQL named that deployed constraint shadow_signal_evaluations_check17.
-- Guard on the semantic replacement name first so a repeated schema run does
-- not drop or recreate the corrected constraint. The definition predicates
-- ensure that only the deployed projection check is removed by its old name.
DO $$
BEGIN
    IF to_regclass('public.shadow_signal_evaluations') IS NOT NULL
       AND NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass(
                'public.shadow_signal_evaluations'
            )
              AND conname =
                'shadow_signal_evaluations_projection_consistency_check'
       ) THEN
        IF EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass(
                'public.shadow_signal_evaluations'
            )
              AND conname = 'shadow_signal_evaluations_check17'
              AND pg_get_constraintdef(oid) LIKE '%pending_move_bps%'
              AND pg_get_constraintdef(oid) LIKE '%direction%'
        ) THEN
            ALTER TABLE public.shadow_signal_evaluations
                DROP CONSTRAINT shadow_signal_evaluations_check17;
        END IF;

        ALTER TABLE public.shadow_signal_evaluations
            ADD CONSTRAINT
                shadow_signal_evaluations_projection_consistency_check
            CHECK (
                (
                    NOT valid
                    AND projected_chainlink IS NULL
                    AND pending_move IS NULL
                    AND pending_move_bps IS NULL
                    AND direction IS NULL
                )
                OR (
                    valid
                    AND chainlink_at_forecast IS NOT NULL
                    AND futures_now IS NOT NULL
                    AND futures_reference IS NOT NULL
                    AND futures_reference_target_ms IS NOT NULL
                    AND futures_reference_gap_ms IS NOT NULL
                    AND projected_chainlink IS NOT NULL
                    AND projected_chainlink > 0
                    AND projected_chainlink <> 'NaN'::NUMERIC
                    AND pending_move IS NOT NULL
                    AND pending_move <> 'NaN'::NUMERIC
                    AND pending_move_bps IS NOT NULL
                    AND pending_move_bps <> 'NaN'::NUMERIC
                    AND direction IS NOT NULL
                    AND direction IN ('up', 'down', 'flat')
                    AND abs(
                        pending_move
                        - (
                            projected_chainlink
                            - chainlink_at_forecast
                        )
                    ) <= 0.000000000000000002
                    AND abs(
                        pending_move_bps
                        - (
                            pending_move * 10000
                            / chainlink_at_forecast
                        )
                    ) <= 0.000000000000000010
                    AND (
                        (pending_move > 0 AND direction = 'up')
                        OR (pending_move < 0 AND direction = 'down')
                        OR (pending_move = 0 AND direction = 'flat')
                    )
                )
            );
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS shadow_signal_evaluations_generated_idx
    ON shadow_signal_evaluations (generated_ms);

CREATE INDEX IF NOT EXISTS shadow_signal_evaluations_retention_cohort_idx
    ON shadow_signal_evaluations (
        generated_ms,
        selection_artifact_sha256,
        selection_fingerprint_sha256,
        selection_schema_version,
        selection_policy_version,
        selection_evidence_end_ms
    );

CREATE INDEX IF NOT EXISTS shadow_signal_evaluations_market_model_idx
    ON shadow_signal_evaluations (market_id, model_version, generated_ms);

REVOKE ALL ON TABLE shadow_signal_evaluations
    FROM PUBLIC, price_reader, price_writer;
GRANT SELECT, INSERT, DELETE ON TABLE shadow_signal_evaluations TO price_writer;

REVOKE ALL ON TABLE public.shadow_signal_evaluation_chart_points
    FROM PUBLIC, price_reader, price_writer;
GRANT SELECT ON TABLE public.shadow_signal_evaluation_chart_points
    TO price_reader;

-- High-resolution evidence is isolated from the public application schema.
-- The writer may manage partitions only inside this schema; the API reader is
-- intentionally not granted access.
CREATE SCHEMA IF NOT EXISTS raw_capture;
ALTER SCHEMA raw_capture OWNER TO CURRENT_USER;
REVOKE ALL ON SCHEMA raw_capture FROM PUBLIC, price_reader;
GRANT USAGE, CREATE ON SCHEMA raw_capture TO price_writer;

CREATE TABLE IF NOT EXISTS raw_capture.binance_futures_price_trace_100ms (
    bucket_start_ms BIGINT NOT NULL,
    connection_id UUID NOT NULL,

    first_received_wall_ns BIGINT NOT NULL,
    last_received_wall_ns BIGINT NOT NULL,
    first_received_monotonic_ns BIGINT NOT NULL,
    last_received_monotonic_ns BIGINT NOT NULL,

    first_trade_time_ms BIGINT NOT NULL,
    last_trade_time_ms BIGINT NOT NULL,
    first_event_time_ms BIGINT NOT NULL,
    last_event_time_ms BIGINT NOT NULL,

    open_price NUMERIC(38, 18) NOT NULL,
    high_price NUMERIC(38, 18) NOT NULL,
    low_price NUMERIC(38, 18) NOT NULL,
    close_price NUMERIC(38, 18) NOT NULL,

    event_count INTEGER NOT NULL,
    first_agg_trade_id BIGINT NOT NULL,
    last_agg_trade_id BIGINT NOT NULL,

    CHECK (bucket_start_ms >= 0 AND bucket_start_ms % 100 = 0),
    CHECK (first_received_wall_ns > 0),
    CHECK (last_received_wall_ns > 0),
    CHECK (
        first_received_wall_ns >= bucket_start_ms * 1000000
        AND first_received_wall_ns < (bucket_start_ms + 100) * 1000000
    ),
    CHECK (
        last_received_wall_ns >= bucket_start_ms * 1000000
        AND last_received_wall_ns < (bucket_start_ms + 100) * 1000000
    ),
    CHECK (first_received_monotonic_ns > 0),
    CHECK (last_received_monotonic_ns >= first_received_monotonic_ns),
    CHECK (first_trade_time_ms > 0),
    CHECK (last_trade_time_ms > 0),
    CHECK (first_event_time_ms > 0),
    CHECK (last_event_time_ms > 0),
    CHECK (low_price > 0),
    CHECK (high_price >= low_price),
    CHECK (open_price >= low_price AND open_price <= high_price),
    CHECK (close_price >= low_price AND close_price <= high_price),
    CHECK (event_count > 0),
    CHECK (first_agg_trade_id >= 0),
    CHECK (last_agg_trade_id >= 0)
) PARTITION BY RANGE (bucket_start_ms);

ALTER TABLE raw_capture.binance_futures_price_trace_100ms
    OWNER TO price_writer;

CREATE TABLE IF NOT EXISTS raw_capture.chainlink_price_events (
    received_wall_ns BIGINT NOT NULL,
    received_monotonic_ns BIGINT NOT NULL,
    connection_id UUID NOT NULL,
    receive_sequence BIGINT NOT NULL,
    provider_event_ms BIGINT NOT NULL,
    provider_message_ms BIGINT,
    price NUMERIC(38, 18) NOT NULL,

    CHECK (received_wall_ns > 0),
    CHECK (received_monotonic_ns > 0),
    CHECK (receive_sequence > 0),
    CHECK (provider_event_ms > 0),
    CHECK (provider_message_ms IS NULL OR provider_message_ms > 0),
    CHECK (price > 0)
) PARTITION BY RANGE (received_wall_ns);

ALTER TABLE raw_capture.chainlink_price_events
    OWNER TO price_writer;

CREATE TABLE IF NOT EXISTS raw_capture.feed_sessions (
    connection_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    connected_wall_ns BIGINT NOT NULL,
    connected_monotonic_ns BIGINT NOT NULL,
    ready_wall_ns BIGINT,
    ready_monotonic_ns BIGINT,
    disconnected_wall_ns BIGINT,
    disconnected_monotonic_ns BIGINT,
    close_reason TEXT,
    messages_received_total BIGINT NOT NULL DEFAULT 0,
    messages_accepted_total BIGINT NOT NULL DEFAULT 0,
    parse_errors_total BIGINT NOT NULL DEFAULT 0,
    records_dropped_total BIGINT NOT NULL DEFAULT 0,
    last_receive_sequence BIGINT NOT NULL DEFAULT 0,

    CHECK (source IN (
        'binance_futures_agg_trade',
        'polymarket_chainlink_rtds'
    )),
    CHECK (connected_wall_ns > 0),
    CHECK (connected_monotonic_ns > 0),
    CHECK ((ready_wall_ns IS NULL) = (ready_monotonic_ns IS NULL)),
    CHECK (ready_wall_ns IS NULL OR ready_wall_ns > 0),
    CHECK (ready_monotonic_ns IS NULL OR ready_monotonic_ns >= connected_monotonic_ns),
    CHECK ((disconnected_wall_ns IS NULL) = (disconnected_monotonic_ns IS NULL)),
    CHECK (disconnected_wall_ns IS NULL OR disconnected_wall_ns > 0),
    CHECK (
        disconnected_monotonic_ns IS NULL
        OR disconnected_monotonic_ns >= connected_monotonic_ns
    ),
    CHECK (
        disconnected_monotonic_ns IS NULL
        OR ready_monotonic_ns IS NULL
        OR disconnected_monotonic_ns >= ready_monotonic_ns
    ),
    CHECK (
        close_reason IS NULL
        OR close_reason IN (
            'remote_close',
            'error',
            'proactive_reconnect',
            'cancelled',
            'shutdown'
        )
    ),
    CHECK ((disconnected_wall_ns IS NULL) = (close_reason IS NULL)),
    CHECK (messages_received_total >= 0),
    CHECK (messages_accepted_total >= 0),
    CHECK (parse_errors_total >= 0),
    CHECK (records_dropped_total >= 0),
    CHECK (messages_accepted_total <= messages_received_total),
    CHECK (parse_errors_total <= messages_received_total),
    CHECK (
        messages_accepted_total + parse_errors_total
        <= messages_received_total
    ),
    CHECK (last_receive_sequence = messages_received_total)
);

ALTER TABLE raw_capture.feed_sessions
    OWNER TO price_writer;

-- Seed the receive-time partition covering deployment and the following one.
-- Runtime maintenance refreshes this pair before later boundaries.
SET ROLE price_writer;

DO $$
DECLARE
    partition_width_ms CONSTANT BIGINT := 21600000;
    current_start_ms BIGINT;
    partition_start_ms BIGINT;
    partition_end_ms BIGINT;
    partition_start_ns BIGINT;
    partition_end_ns BIGINT;
    offset_index INTEGER;
BEGIN
    current_start_ms := (
        (floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT)
        / partition_width_ms
    ) * partition_width_ms;

    FOR offset_index IN 0..1 LOOP
        partition_start_ms := current_start_ms + offset_index * partition_width_ms;
        partition_end_ms := partition_start_ms + partition_width_ms;
        partition_start_ns := partition_start_ms * 1000000;
        partition_end_ns := partition_end_ms * 1000000;

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS raw_capture.%I '
            'PARTITION OF raw_capture.binance_futures_price_trace_100ms '
            'FOR VALUES FROM (%s) TO (%s)',
            'binance_futures_price_trace_100ms_p' || partition_start_ms,
            partition_start_ms,
            partition_end_ms
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS raw_capture.%I '
            'PARTITION OF raw_capture.chainlink_price_events '
            'FOR VALUES FROM (%s) TO (%s)',
            'chainlink_price_events_p' || partition_start_ms,
            partition_start_ns,
            partition_end_ns
        );
    END LOOP;
END
$$;

RESET ROLE;

REVOKE ALL ON ALL TABLES IN SCHEMA raw_capture FROM PUBLIC, price_reader;

INSERT INTO providers (provider_code, display_name)
VALUES ('binance_spot', 'Binance Spot')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSDT',
    'BTC',
    'USDT',
    'btcusdt@ticker'
FROM providers
WHERE provider_code = 'binance_spot'
ON CONFLICT (provider_id, symbol) DO NOTHING;

INSERT INTO providers (provider_code, display_name)
VALUES ('polymarket_chainlink_rtds', 'Polymarket RTDS Chainlink BTC/USD')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSD',
    'BTC',
    'USD',
    'crypto_prices_chainlink:btc/usd'
FROM providers
WHERE provider_code = 'polymarket_chainlink_rtds'
ON CONFLICT (provider_id, symbol) DO NOTHING;

INSERT INTO providers (provider_code, display_name)
VALUES ('binance_usdm_perp', 'Binance USD-M Perpetual Futures')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSDT',
    'BTC',
    'USDT',
    'binance_usdm_perp:BTCUSDT'
FROM providers
WHERE provider_code = 'binance_usdm_perp'
ON CONFLICT (provider_id, symbol) DO NOTHING;
