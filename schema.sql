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
