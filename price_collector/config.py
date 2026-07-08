from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=True)

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    BINANCE_WS_URL: str = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    PROVIDER_CODE: str = "binance_spot"
    SYMBOL: str = "BTCUSDT"
    STALE_PRICE_MS: int = 10_000

    POLYMARKET_RTDS_WS_URL: str = "wss://ws-live-data.polymarket.com"
    POLYMARKET_CHAINLINK_PROVIDER_CODE: str = "polymarket_chainlink_rtds"
    POLYMARKET_CHAINLINK_SYMBOL: str = "BTCUSD"
    POLYMARKET_CHAINLINK_RTD_SYMBOL: str = "btc/usd"
    POLYMARKET_CHAINLINK_TOPIC: str = "crypto_prices_chainlink"

    POLYMARKET_GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    POLYMARKET_CLOB_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    POLYMARKET_BTC_5M_SLUG_PREFIX: str = "btc-updown-5m"
    POLYMARKET_PROBABILITY_SOURCE: str = "polymarket_clob"
    POLYMARKET_PROBABILITY_STALE_MS: int = 15_000
    POLYMARKET_CLOB_PING_SECONDS: int = 10

    DATABASE_URL: Optional[str] = None
    READ_DATABASE_URL: Optional[str] = None
