
import pytest
from pydantic import ValidationError

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
    monkeypatch.delenv(
        "POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS",
        raising=False,
    )

    settings = Settings()

    assert settings.POLYMARKET_RTDS_WS_URL == "wss://ws-live-data.polymarket.com"
    assert settings.POLYMARKET_CHAINLINK_PROVIDER_CODE == "polymarket_chainlink_rtds"
    assert settings.POLYMARKET_CHAINLINK_SYMBOL == "BTCUSD"
    assert settings.POLYMARKET_CHAINLINK_RTD_SYMBOL == "btc/usd"
    assert settings.POLYMARKET_CHAINLINK_TOPIC == "crypto_prices_chainlink"
    assert (
        settings.POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS
        == 10_000
    )


@pytest.mark.parametrize("idle_timeout_ms", (4_999, 60_001))
def test_settings_reject_invalid_chainlink_accepted_event_idle_timeout(
    monkeypatch,
    idle_timeout_ms,
):
    monkeypatch.setenv(
        "POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS",
        str(idle_timeout_ms),
    )

    with pytest.raises(
        ValidationError,
        match="POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS",
    ):
        Settings()

@pytest.mark.parametrize("idle_timeout_ms", (5_000, 60_000))
def test_settings_accept_chainlink_accepted_event_idle_timeout_bounds(
    monkeypatch,
    idle_timeout_ms,
):
    monkeypatch.setenv(
        "POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS",
        str(idle_timeout_ms),
    )

    settings = Settings()

    assert (
        settings.POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS
        == idle_timeout_ms
    )


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
    monkeypatch.delenv("POLYMARKET_RESOLUTION_POLL_SECONDS", raising=False)
    monkeypatch.delenv("POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS", raising=False)
    monkeypatch.delenv("POLYMARKET_RESOLUTION_BATCH_SIZE", raising=False)
    monkeypatch.delenv("POLYMARKET_RESOLUTION_WS_GRACE_SECONDS", raising=False)

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
    assert settings.POLYMARKET_RESOLUTION_POLL_SECONDS == 5
    assert settings.POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS == 300
    assert settings.POLYMARKET_RESOLUTION_BATCH_SIZE == 20
    assert settings.POLYMARKET_RESOLUTION_WS_GRACE_SECONDS == 30


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
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_ENABLED", raising=False)
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_SPOT_WS_URL", raising=False)
    monkeypatch.delenv(
        "BINANCE_MICROSTRUCTURE_FUTURES_DEPTH_WS_URL", raising=False
    )
    monkeypatch.delenv(
        "BINANCE_MICROSTRUCTURE_FUTURES_LIQUIDATION_WS_URL", raising=False
    )
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS", raising=False)
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS", raising=False)
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_WARN_RELATION_MB", raising=False)
    monkeypatch.delenv("BINANCE_MICROSTRUCTURE_MAX_RELATION_MB", raising=False)

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
    assert settings.BINANCE_MICROSTRUCTURE_ENABLED is False
    assert settings.BINANCE_MICROSTRUCTURE_SPOT_WS_URL == (
        "wss://stream.binance.com:9443/stream?streams="
        "btcusdt@aggTrade/btcusdt@depth10"
    )
    assert settings.BINANCE_MICROSTRUCTURE_FUTURES_DEPTH_WS_URL == (
        "wss://fstream.binance.com/public/ws/btcusdt@depth10@500ms"
    )
    assert settings.BINANCE_MICROSTRUCTURE_FUTURES_LIQUIDATION_WS_URL == (
        "wss://fstream.binance.com/market/ws/btcusdt@forceOrder"
    )
    assert settings.BINANCE_MICROSTRUCTURE_QUEUE_MAX_EVENTS == 100_000
    assert settings.BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS == 250
    assert settings.BINANCE_MICROSTRUCTURE_RETENTION_DAYS == 30
    assert settings.BINANCE_MICROSTRUCTURE_WARN_RELATION_MB == 4_096
    assert settings.BINANCE_MICROSTRUCTURE_MAX_RELATION_MB == 6_144


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


RAW_CAPTURE_ENV_NAMES = (
    "RAW_FUTURES_TRACE_ENABLED",
    "RAW_CHAINLINK_EVENTS_ENABLED",
    "RAW_FUTURES_BUCKET_MS",
    "RAW_CAPTURE_QUEUE_MAX_EVENTS",
    "RAW_CAPTURE_BATCH_MAX_ROWS",
    "RAW_CAPTURE_FLUSH_MS",
    "RAW_CAPTURE_RETENTION_HOURS",
    "RAW_CAPTURE_MAX_RELATION_MB",
    "RAW_CAPTURE_RETENTION_CHECK_SECONDS",
)


def test_settings_include_inactive_raw_capture_defaults(monkeypatch):
    for name in RAW_CAPTURE_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    settings = Settings()

    assert settings.RAW_FUTURES_TRACE_ENABLED is False
    assert settings.RAW_CHAINLINK_EVENTS_ENABLED is False
    assert settings.RAW_FUTURES_BUCKET_MS == 100
    assert settings.RAW_CAPTURE_QUEUE_MAX_EVENTS == 5_000
    assert settings.RAW_CAPTURE_BATCH_MAX_ROWS == 500
    assert settings.RAW_CAPTURE_FLUSH_MS == 1_000
    assert settings.RAW_CAPTURE_RETENTION_HOURS == 72
    assert settings.RAW_CAPTURE_MAX_RELATION_MB == 2_048
    assert settings.RAW_CAPTURE_RETENTION_CHECK_SECONDS == 60


def test_settings_raw_capture_bucket_is_fixed_at_100ms(monkeypatch):
    monkeypatch.setenv("RAW_FUTURES_BUCKET_MS", "50")

    with pytest.raises(ValidationError, match="RAW_FUTURES_BUCKET_MS"):
        Settings()


def test_settings_raw_capture_batch_cannot_exceed_queue(monkeypatch):
    monkeypatch.setenv("RAW_CAPTURE_QUEUE_MAX_EVENTS", "100")
    monkeypatch.setenv("RAW_CAPTURE_BATCH_MAX_ROWS", "101")

    with pytest.raises(
        ValidationError,
        match="RAW_CAPTURE_BATCH_MAX_ROWS must be less than or equal to",
    ):
        Settings()


def test_microstructure_warning_relation_size_must_be_below_cap(monkeypatch):
    monkeypatch.setenv("BINANCE_MICROSTRUCTURE_WARN_RELATION_MB", "100")
    monkeypatch.setenv("BINANCE_MICROSTRUCTURE_MAX_RELATION_MB", "100")

    with pytest.raises(
        ValidationError,
        match="BINANCE_MICROSTRUCTURE_WARN_RELATION_MB must be less than",
    ):
        Settings()


@pytest.mark.parametrize("flush_delay_ms", ["0", "251"])
def test_microstructure_flush_delay_matches_health_jitter_bound(
    monkeypatch,
    flush_delay_ms,
):
    monkeypatch.setenv("BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS", flush_delay_ms)

    with pytest.raises(ValidationError, match="BINANCE_MICROSTRUCTURE_FLUSH_DELAY_MS"):
        Settings()
