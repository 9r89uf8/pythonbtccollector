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
    monkeypatch.delenv("POLYMARKET_CLOB_WS_URL", raising=False)
    monkeypatch.delenv("POLYMARKET_BTC_5M_SLUG_PREFIX", raising=False)
    monkeypatch.delenv("POLYMARKET_PROBABILITY_SOURCE", raising=False)
    monkeypatch.delenv("POLYMARKET_PROBABILITY_STALE_MS", raising=False)
    monkeypatch.delenv("POLYMARKET_CLOB_PING_SECONDS", raising=False)

    settings = Settings()

    assert settings.POLYMARKET_GAMMA_BASE_URL == "https://gamma-api.polymarket.com"
    assert (
        settings.POLYMARKET_CLOB_WS_URL
        == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    )
    assert settings.POLYMARKET_BTC_5M_SLUG_PREFIX == "btc-updown-5m"
    assert settings.POLYMARKET_PROBABILITY_SOURCE == "polymarket_clob"
    assert settings.POLYMARKET_PROBABILITY_STALE_MS == 15_000
    assert settings.POLYMARKET_CLOB_PING_SECONDS == 10
