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

    DATABASE_URL: Optional[str] = None
    READ_DATABASE_URL: Optional[str] = None
