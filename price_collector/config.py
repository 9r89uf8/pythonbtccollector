import re
from pathlib import PurePosixPath
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _absolute_posix_path(value: str, field_name: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(
            f"{field_name} must be an absolute normalized POSIX path"
        )
    return path


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
    POLYMARKET_CLOB_BASE_URL: str = "https://clob.polymarket.com"
    POLYMARKET_CLOB_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    POLYMARKET_BTC_5M_SLUG_PREFIX: str = "btc-updown-5m"
    POLYMARKET_PROBABILITY_SOURCE: str = "polymarket_clob"
    POLYMARKET_PROBABILITY_STALE_MS: int = 15_000
    POLYMARKET_CLOB_PING_SECONDS: int = 10
    POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS: int = 45
    POLYMARKET_NEXT_MARKET_RETRY_MS: int = 500
    POLYMARKET_REST_PRIME_SECONDS: int = 15
    POLYMARKET_RESOLUTION_POLL_SECONDS: int = 5
    POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS: int = 300
    POLYMARKET_RESOLUTION_BATCH_SIZE: int = 20
    POLYMARKET_RESOLUTION_WS_GRACE_SECONDS: int = 30

    BINANCE_FUTURES_BASE_URL: str = "https://fapi.binance.com"
    BINANCE_FUTURES_SYMBOL: str = "BTCUSDT"
    BINANCE_FUTURES_PROVIDER_CODE: str = "binance_usdm_perp"
    BINANCE_FUTURES_POLL_SECONDS: int = 1
    BINANCE_FUTURES_REST_TIMEOUT_SECONDS: int = 5
    BINANCE_FUTURES_HIST_OI_ENABLED: bool = True
    BINANCE_FUTURES_HIST_OI_POLL_SECONDS: int = 30
    BINANCE_FUTURES_STREAMS_ENABLED: bool = True
    BINANCE_FUTURES_AGG_TRADE_WS_URL: str = (
        "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
    )
    BINANCE_FUTURES_BOOK_TICKER_WS_URL: str = (
        "wss://fstream.binance.com/public/ws/btcusdt@bookTicker"
    )
    BINANCE_FUTURES_FLOW_FLUSH_DELAY_MS: int = 1_500
    BINANCE_FUTURES_BOOK_FLUSH_DELAY_MS: int = 1_500
    BINANCE_FUTURES_STREAM_FLUSH_SECONDS: float = 0.25
    BINANCE_FUTURES_STORE_RAW_JSON: bool = False

    RAW_FUTURES_TRACE_ENABLED: bool = False
    RAW_CHAINLINK_EVENTS_ENABLED: bool = False
    RAW_FUTURES_BUCKET_MS: int = Field(default=100, ge=100, le=100)
    RAW_CAPTURE_QUEUE_MAX_EVENTS: int = Field(default=5_000, gt=0)
    RAW_CAPTURE_BATCH_MAX_ROWS: int = Field(default=500, gt=0)
    RAW_CAPTURE_FLUSH_MS: int = Field(default=1_000, gt=0)
    RAW_CAPTURE_RETENTION_HOURS: int = Field(default=72, ge=6)
    RAW_CAPTURE_MAX_RELATION_MB: int = Field(default=2_048, gt=0)
    RAW_CAPTURE_RETENTION_CHECK_SECONDS: int = Field(default=60, gt=0)

    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_SOCKET_TIMEOUT_SECONDS: float = 0.25

    SHADOW_SIGNAL_ENABLED: bool = False
    SHADOW_SIGNAL_TRUSTED_DECISION_DIR: str = (
        "/var/lib/price-collector/shadow-decisions"
    )
    SHADOW_SIGNAL_SELECTION_PATH: Optional[str] = None
    SHADOW_SIGNAL_SELECTION_SHA256: Optional[str] = None
    SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH: Optional[str] = None
    SHADOW_SIGNAL_POLL_MS: int = Field(default=100, ge=100, le=100)
    SHADOW_SIGNAL_TTL_MS: int = Field(default=2_000, ge=1_500, le=2_000)

    DATABASE_URL: Optional[str] = None
    READ_DATABASE_URL: Optional[str] = None

    @model_validator(mode="after")
    def validate_raw_capture_batch_size(self) -> "Settings":
        if self.RAW_CAPTURE_BATCH_MAX_ROWS > self.RAW_CAPTURE_QUEUE_MAX_EVENTS:
            raise ValueError(
                "RAW_CAPTURE_BATCH_MAX_ROWS must be less than or equal to "
                "RAW_CAPTURE_QUEUE_MAX_EVENTS"
            )
        return self

    @model_validator(mode="after")
    def validate_shadow_signal_settings(self) -> "Settings":
        trusted_dir = _absolute_posix_path(
            self.SHADOW_SIGNAL_TRUSTED_DECISION_DIR,
            "SHADOW_SIGNAL_TRUSTED_DECISION_DIR",
        )
        configured_paths = (
            (
                "SHADOW_SIGNAL_SELECTION_PATH",
                self.SHADOW_SIGNAL_SELECTION_PATH,
            ),
            (
                "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH",
                self.SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH,
            ),
        )
        normalized_paths: dict[str, PurePosixPath] = {}
        for field_name, raw_path in configured_paths:
            if raw_path is None:
                continue
            path = _absolute_posix_path(raw_path, field_name)
            if path.parent != trusted_dir:
                raise ValueError(
                    f"{field_name} must be a direct file inside "
                    "SHADOW_SIGNAL_TRUSTED_DECISION_DIR"
                )
            normalized_paths[field_name] = path

        if (
            self.SHADOW_SIGNAL_SELECTION_SHA256 is not None
            and SHA256_PATTERN.fullmatch(
                self.SHADOW_SIGNAL_SELECTION_SHA256
            )
            is None
        ):
            raise ValueError(
                "SHADOW_SIGNAL_SELECTION_SHA256 must be 64 lowercase "
                "hexadecimal characters"
            )

        if self.SHADOW_SIGNAL_ENABLED:
            required = {
                "SHADOW_SIGNAL_SELECTION_PATH": (
                    self.SHADOW_SIGNAL_SELECTION_PATH
                ),
                "SHADOW_SIGNAL_SELECTION_SHA256": (
                    self.SHADOW_SIGNAL_SELECTION_SHA256
                ),
                "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH": (
                    self.SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH
                ),
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError(
                    "enabled shadow signal requires " + ", ".join(missing)
                )

        selection_path = normalized_paths.get("SHADOW_SIGNAL_SELECTION_PATH")
        replay_path = normalized_paths.get(
            "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH"
        )
        if selection_path is not None and selection_path == replay_path:
            raise ValueError(
                "SHADOW_SIGNAL_SELECTION_PATH and "
                "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH must differ"
            )
        if self.SHADOW_SIGNAL_TTL_MS <= self.SHADOW_SIGNAL_POLL_MS:
            raise ValueError(
                "SHADOW_SIGNAL_TTL_MS must exceed SHADOW_SIGNAL_POLL_MS"
            )
        return self
