from price_collector.config import Settings


def test_settings_allows_api_reader_url_without_writer_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "READ_DATABASE_URL",
        "postgresql://price_reader:secret@127.0.0.1:5432/price_collector",
    )

    settings = Settings()

    assert settings.DATABASE_URL is None
    assert (
        settings.READ_DATABASE_URL
        == "postgresql://price_reader:secret@127.0.0.1:5432/price_collector"
    )


def test_settings_include_polymarket_chainlink_defaults(monkeypatch):
    monkeypatch.delenv("POLYMARKET_RTDS_WS_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_CHAINLINK_PROVIDER_CODE", raising=False)
    monkeypatch.delenv("POLYMARKET_CHAINLINK_SYMBOL", raising=False)
    monkeypatch.delenv("POLYMARKET_CHAINLINK_RTD_SYMBOL", raising=False)
    monkeypatch.delenv("POLYMARKET_CHAINLINK_TOPIC", raising=False)

    settings = Settings()

    assert settings.POLYMARKET_RTDS_WS_URL == "wss://ws-live-data.polymarket.com"
    assert settings.POLYMARKET_CHAINLINK_PROVIDER_CODE == "polymarket_chainlink_rtds"
    assert settings.POLYMARKET_CHAINLINK_SYMBOL == "BTCUSD"
    assert settings.POLYMARKET_CHAINLINK_RTD_SYMBOL == "btc/usd"
    assert settings.POLYMARKET_CHAINLINK_TOPIC == "crypto_prices_chainlink"


def test_settings_include_polymarket_probability_defaults(monkeypatch):
    monkeypatch.delenv("POLYMARKET_GAMMA_BASE_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_CLOB_BASE_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_CLOB_WS_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_BTC_5M_SLUG_PREFIX", raising=False)
    monkeypatch.delenv("POLYMARKET_PROBABILITY_SOURCE", raising=False)
    monkeypatch.delenv("POLYMARKET_PROBABILITY_STALE_MS", raising=False)
    monkeypatch.delenv("POLYMARKET_CLOB_PING_SECONDS", raising=False)
    monkeypatch.delenv("POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS", raising=False)
    monkeypatch.delenv("POLYMARKET_NEXT_MARKET_RETRY_MS", raising=False)
    monkeypatch.delenv("POLYMARKET_REST_PRIME_SECONDS", raising=False)

    settings = Settings()

    assert settings.POLYMARKET_GAMMA_BASE_URL == "https://gamma-api.polymarket.com"
    assert settings.POLYMARKET_CLOB_BASE_URL == "https://clob.polymarket.com"
    assert (
        settings.POLYMARKET_CLOB_WS_URL
        == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    )
    assert settings.POLYMARKET_BTC_5M_SLUG_PREFIX == "btc-updown-5m"
    assert settings.POLYMARKET_PROBABILITY_SOURCE == "polymarket_clob"
    assert settings.POLYMARKET_PROBABILITY_STALE_MS == 15_000
    assert settings.POLYMARKET_CLOB_PING_SECONDS == 10
    assert settings.POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS == 45
    assert settings.POLYMARKET_NEXT_MARKET_RETRY_MS == 500
    assert settings.POLYMARKET_REST_PRIME_SECONDS == 15


def test_settings_include_binance_futures_defaults(monkeypatch):
    monkeypatch.delenv("BINANCE_FUTURES_BASE_URL", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_SYMBOL", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_PROVIDER_CODE", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_POLL_SECONDS", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_REST_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_HIST_OI_ENABLED", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_HIST_OI_POLL_SECONDS", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_STREAMS_ENABLED", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_AGG_TRADE_WS_URL", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_BOOK_TICKER_WS_URL", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_FLOW_FLUSH_DELAY_MS", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_BOOK_FLUSH_DELAY_MS", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_STREAM_FLUSH_SECONDS", raising=False)
    monkeypatch.delenv("BINANCE_FUTURES_STORE_RAW_JSON", raising=False)

    settings = Settings()

    assert settings.BINANCE_FUTURES_BASE_URL == "https://fapi.binance.com"
    assert settings.BINANCE_FUTURES_SYMBOL == "BTCUSDT"
    assert settings.BINANCE_FUTURES_PROVIDER_CODE == "binance_usdm_perp"
    assert settings.BINANCE_FUTURES_POLL_SECONDS == 1
    assert settings.BINANCE_FUTURES_REST_TIMEOUT_SECONDS == 5
    assert settings.BINANCE_FUTURES_HIST_OI_ENABLED is True
    assert settings.BINANCE_FUTURES_HIST_OI_POLL_SECONDS == 30
    assert settings.BINANCE_FUTURES_STREAMS_ENABLED is True
    assert (
        settings.BINANCE_FUTURES_AGG_TRADE_WS_URL
        == "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
    )
    assert (
        settings.BINANCE_FUTURES_BOOK_TICKER_WS_URL
        == "wss://fstream.binance.com/public/ws/btcusdt@bookTicker"
    )
    assert settings.BINANCE_FUTURES_FLOW_FLUSH_DELAY_MS == 1_500
    assert settings.BINANCE_FUTURES_BOOK_FLUSH_DELAY_MS == 1_500
    assert settings.BINANCE_FUTURES_STREAM_FLUSH_SECONDS == 0.25
    assert settings.BINANCE_FUTURES_STORE_RAW_JSON is False


def test_settings_include_local_redis_defaults(monkeypatch):
    monkeypatch.delenv("REDIS_HOST", raising=False)
    monkeypatch.delenv("REDIS_PORT", raising=False)
    monkeypatch.delenv("REDIS_DB", raising=False)
    monkeypatch.delenv("REDIS_SOCKET_TIMEOUT_SECONDS", raising=False)

    settings = Settings()

    assert settings.REDIS_HOST == "127.0.0.1"
    assert settings.REDIS_PORT == 6379
    assert settings.REDIS_DB == 0
    assert settings.REDIS_SOCKET_TIMEOUT_SECONDS == 0.25
