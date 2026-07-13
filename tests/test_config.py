import pytest
from pydantic import ValidationError

from price_collector.config import Settings


SHADOW_SIGNAL_ENV_NAMES = (
    "SHADOW_SIGNAL_ENABLED",
    "SHADOW_SIGNAL_TRUSTED_DECISION_DIR",
    "SHADOW_SIGNAL_SELECTION_PATH",
    "SHADOW_SIGNAL_SELECTION_SHA256",
    "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH",
    "SHADOW_SIGNAL_POLL_MS",
    "SHADOW_SIGNAL_TTL_MS",
)


def clear_shadow_signal_environment(monkeypatch):
    for name in SHADOW_SIGNAL_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def set_valid_shadow_signal_environment(monkeypatch):
    values = {
        "SHADOW_SIGNAL_ENABLED": "true",
        "SHADOW_SIGNAL_TRUSTED_DECISION_DIR": (
            "/var/lib/price-collector/shadow-decisions"
        ),
        "SHADOW_SIGNAL_SELECTION_PATH": (
            "/var/lib/price-collector/shadow-decisions/selection.json"
        ),
        "SHADOW_SIGNAL_SELECTION_SHA256": "a" * 64,
        "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH": (
            "/var/lib/price-collector/shadow-decisions/replay.json"
        ),
        "SHADOW_SIGNAL_POLL_MS": "100",
        "SHADOW_SIGNAL_TTL_MS": "2000",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


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


def test_settings_include_disabled_shadow_signal_defaults(monkeypatch):
    clear_shadow_signal_environment(monkeypatch)

    settings = Settings()

    assert settings.SHADOW_SIGNAL_ENABLED is False
    assert settings.SHADOW_SIGNAL_TRUSTED_DECISION_DIR == (
        "/var/lib/price-collector/shadow-decisions"
    )
    assert settings.SHADOW_SIGNAL_SELECTION_PATH is None
    assert settings.SHADOW_SIGNAL_SELECTION_SHA256 is None
    assert settings.SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH is None
    assert settings.SHADOW_SIGNAL_POLL_MS == 100
    assert settings.SHADOW_SIGNAL_TTL_MS == 2_000


def test_settings_accept_enabled_shadow_signal_with_trusted_files(monkeypatch):
    clear_shadow_signal_environment(monkeypatch)
    set_valid_shadow_signal_environment(monkeypatch)

    settings = Settings()

    assert settings.SHADOW_SIGNAL_ENABLED is True
    assert settings.SHADOW_SIGNAL_SELECTION_SHA256 == "a" * 64
    assert settings.SHADOW_SIGNAL_SELECTION_PATH.endswith("selection.json")
    assert settings.SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH.endswith(
        "replay.json"
    )


@pytest.mark.parametrize(
    "missing_name",
    (
        "SHADOW_SIGNAL_SELECTION_PATH",
        "SHADOW_SIGNAL_SELECTION_SHA256",
        "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH",
    ),
)
def test_enabled_shadow_signal_requires_decision_inputs(
    monkeypatch,
    missing_name,
):
    clear_shadow_signal_environment(monkeypatch)
    set_valid_shadow_signal_environment(monkeypatch)
    monkeypatch.delenv(missing_name)

    with pytest.raises(ValidationError, match=missing_name):
        Settings()


@pytest.mark.parametrize("invalid_sha", ("a" * 63, "A" * 64, "g" * 64))
def test_shadow_signal_selection_sha_is_lowercase_sha256(
    monkeypatch,
    invalid_sha,
):
    clear_shadow_signal_environment(monkeypatch)
    set_valid_shadow_signal_environment(monkeypatch)
    monkeypatch.setenv("SHADOW_SIGNAL_SELECTION_SHA256", invalid_sha)

    with pytest.raises(
        ValidationError,
        match="64 lowercase hexadecimal characters",
    ):
        Settings()


@pytest.mark.parametrize(
    ("field_name", "path"),
    (
        ("SHADOW_SIGNAL_SELECTION_PATH", "selection.json"),
        (
            "SHADOW_SIGNAL_SELECTION_PATH",
            "/var/lib/price-collector/outside.json",
        ),
        (
            "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH",
            "/var/lib/price-collector/shadow-decisions/../outside.json",
        ),
        (
            "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH",
            "/var/lib/price-collector/shadow-decisions/nested/replay.json",
        ),
    ),
)
def test_shadow_signal_files_must_be_normalized_and_trusted(
    monkeypatch,
    field_name,
    path,
):
    clear_shadow_signal_environment(monkeypatch)
    set_valid_shadow_signal_environment(monkeypatch)
    monkeypatch.setenv(field_name, path)

    with pytest.raises(ValidationError, match=field_name):
        Settings()


def test_shadow_signal_decision_files_must_differ(monkeypatch):
    clear_shadow_signal_environment(monkeypatch)
    set_valid_shadow_signal_environment(monkeypatch)
    selection_path = (
        "/var/lib/price-collector/shadow-decisions/selection.json"
    )
    monkeypatch.setenv(
        "SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH",
        selection_path,
    )

    with pytest.raises(ValidationError, match="must differ"):
        Settings()


@pytest.mark.parametrize("poll_ms", (99, 101))
def test_shadow_signal_poll_cadence_is_fixed_at_100ms(monkeypatch, poll_ms):
    clear_shadow_signal_environment(monkeypatch)
    monkeypatch.setenv("SHADOW_SIGNAL_POLL_MS", str(poll_ms))

    with pytest.raises(ValidationError, match="SHADOW_SIGNAL_POLL_MS"):
        Settings()


@pytest.mark.parametrize("ttl_ms", (1_499, 2_001))
def test_shadow_signal_ttl_stays_in_short_expiry_range(monkeypatch, ttl_ms):
    clear_shadow_signal_environment(monkeypatch)
    monkeypatch.setenv("SHADOW_SIGNAL_TTL_MS", str(ttl_ms))

    with pytest.raises(ValidationError, match="SHADOW_SIGNAL_TTL_MS"):
        Settings()
