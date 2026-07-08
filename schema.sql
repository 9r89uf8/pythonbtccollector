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
