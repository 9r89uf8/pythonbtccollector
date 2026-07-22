from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_price_api_service_binds_to_loopback_only():
    service = (ROOT / "deployment" / "price-api.service").read_text()

    assert "EnvironmentFile=/etc/price-collector/api.env" in service
    assert "--host 127.0.0.1" in service
    assert "--host 0.0.0.0" not in service
    assert "redis-server.service" in service
    assert "price-collector-shadow-signal.service" not in service


def test_api_env_example_contains_reader_credentials_only():
    lines = (ROOT / "deployment" / "api.env.example").read_text().splitlines()

    assert "READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector" in lines
    assert "REDIS_HOST=127.0.0.1" in lines
    assert "REDIS_PORT=6379" in lines
    assert not any(line.startswith("DATABASE_URL=") for line in lines)


def test_collector_env_example_contains_writer_credentials_only():
    lines = (ROOT / "deployment" / "collector.env.example").read_text().splitlines()

    assert "DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector" in lines
    assert "REDIS_HOST=127.0.0.1" in lines
    assert "REDIS_PORT=6379" in lines
    assert "BINANCE_FUTURES_STREAMS_ENABLED=true" in lines
    assert (
        "BINANCE_FUTURES_AGG_TRADE_WS_URL=wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
        in lines
    )
    assert (
        "BINANCE_FUTURES_BOOK_TICKER_WS_URL=wss://fstream.binance.com/public/ws/btcusdt@bookTicker"
        in lines
    )
    assert "BINANCE_FUTURES_STORE_RAW_JSON=false" in lines
    assert "RAW_FUTURES_TRACE_ENABLED=false" in lines
    assert "RAW_CHAINLINK_EVENTS_ENABLED=false" in lines
    assert (
        "POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10000"
        in lines
    )
    assert "RAW_FUTURES_BUCKET_MS=100" in lines
    assert "RAW_CAPTURE_QUEUE_MAX_EVENTS=5000" in lines
    assert "RAW_CAPTURE_BATCH_MAX_ROWS=500" in lines
    assert "RAW_CAPTURE_FLUSH_MS=1000" in lines
    assert "RAW_CAPTURE_RETENTION_HOURS=72" in lines
    assert "RAW_CAPTURE_MAX_RELATION_MB=2048" in lines
    assert "RAW_CAPTURE_RETENTION_CHECK_SECONDS=60" in lines
    assert not any(line.startswith("READ_DATABASE_URL=") for line in lines)
    assert not any(line.startswith("RAW_CAPTURE_MAX_DISK_MB=") for line in lines)


def test_api_env_example_has_no_raw_capture_or_writer_settings():
    lines = (ROOT / "deployment" / "api.env.example").read_text().splitlines()

    assert not any(line.startswith("RAW_") for line in lines)
    assert not any(line.startswith("SHADOW_SIGNAL_") for line in lines)
    assert not any(line.startswith("DATABASE_URL=") for line in lines)


def test_polymarket_chainlink_collector_service_execs_new_module():
    service = (ROOT / "deployment" / "price-collector-polymarket-chainlink.service").read_text()

    assert "EnvironmentFile=/etc/price-collector/collector.env" in service
    assert (
        "ExecStart=/opt/price-collector/.venv/bin/python "
        "-m price_collector.polymarket_chainlink_collector"
    ) in service
    assert "-m price_collector.collector" not in service


def test_polymarket_probability_collector_service_execs_probability_module():
    service = (
        ROOT / "deployment" / "price-collector-polymarket-probabilities.service"
    ).read_text()

    assert "EnvironmentFile=/etc/price-collector/collector.env" in service
    assert (
        "ExecStart=/opt/price-collector/.venv/bin/python "
        "-m price_collector.polymarket_probability_collector"
    ) in service
    assert "--host 0.0.0.0" not in service


def test_binance_futures_collector_service_execs_futures_module():
    service = (ROOT / "deployment" / "price-collector-binance-futures.service").read_text()

    assert "EnvironmentFile=/etc/price-collector/collector.env" in service
    assert "redis-server.service" in service
    assert (
        "ExecStart=/opt/price-collector/.venv/bin/python "
        "-m price_collector.binance_futures_collector"
    ) in service
    assert "--host 0.0.0.0" not in service


def test_shadow_signal_env_is_disabled_and_has_phase5_controls_without_secrets():
    lines = (
        ROOT / "deployment" / "shadow-signal.env.example"
    ).read_text().splitlines()

    assert "SHADOW_SIGNAL_ENABLED=false" in lines
    assert "REDIS_HOST=127.0.0.1" in lines
    assert "REDIS_PORT=6379" in lines
    assert "SHADOW_SIGNAL_POLL_MS=100" in lines
    assert "SHADOW_SIGNAL_TTL_MS=2000" in lines
    assert "SHADOW_SIGNAL_EVALUATION_ENABLED=false" in lines
    assert "SHADOW_SIGNAL_EVALUATION_INTERVAL_MS=500" in lines
    assert "SHADOW_SIGNAL_EVALUATION_QUEUE_MAX=5000" in lines
    assert "SHADOW_SIGNAL_EVALUATION_BATCH_MAX_ROWS=500" in lines
    assert "SHADOW_SIGNAL_EVALUATION_FLUSH_MS=1000" in lines
    assert "SHADOW_SIGNAL_EVALUATION_RETRY_MS=5000" in lines
    assert "SHADOW_SIGNAL_EVALUATION_SHUTDOWN_TIMEOUT_SECONDS=10" in lines
    assert "SHADOW_SIGNAL_EVALUATION_DB_CONNECT_TIMEOUT_SECONDS=5" in lines
    assert "SHADOW_SIGNAL_EVALUATION_DB_COMMAND_TIMEOUT_SECONDS=5" in lines
    assert "SHADOW_SIGNAL_EVALUATION_RETENTION_HOURS=168" in lines
    assert "SHADOW_SIGNAL_EVALUATION_RETENTION_CHECK_SECONDS=300" in lines
    assert "SHADOW_SIGNAL_EVALUATION_RETENTION_BATCH_ROWS=5000" in lines
    assert (
        "SHADOW_SIGNAL_TRUSTED_DECISION_DIR="
        "/var/lib/price-collector/shadow-decisions"
    ) in lines
    assert any(line.startswith("SHADOW_SIGNAL_SELECTION_PATH=") for line in lines)
    assert any(
        line.startswith("SHADOW_SIGNAL_SELECTION_SHA256=") for line in lines
    )
    assert any(
        line.startswith("SHADOW_SIGNAL_REPLAY_CONFIG_REPORT_PATH=")
        for line in lines
    )
    assert not any(line.startswith("DATABASE_URL=") for line in lines)
    assert not any(line.startswith("READ_DATABASE_URL=") for line in lines)


def test_shadow_signal_service_is_isolated_and_ordered_after_producers():
    service = (
        ROOT / "deployment" / "price-collector-shadow-signal.service"
    ).read_text()

    assert "EnvironmentFile=/etc/price-collector/shadow-signal.env" in service
    assert (
        "ExecStart=/opt/price-collector/.venv/bin/python "
        "-m price_collector.shadow_signal_collector"
    ) in service
    assert "redis-server.service" in service
    assert "price-collector-binance-futures.service" in service
    assert "price-collector-polymarket-chainlink.service" in service
    assert "postgresql.service" in service
    assert "Restart=on-failure" in service
    assert "StartLimitIntervalSec=60" in service
    assert "StartLimitBurst=3" in service
    assert "NoNewPrivileges=true" in service
    assert "PrivateDevices=true" in service
    assert "PrivateTmp=true" in service
    assert "ProtectSystem=strict" in service
    assert "ProtectHome=true" in service
    assert "CapabilityBoundingSet=\n" in service
    assert "ReadWritePaths=" not in service


def test_shadow_signal_2s_env_is_disabled_and_redis_only():
    lines = (
        ROOT / "deployment" / "shadow-signal-2s.env.example"
    ).read_text().splitlines()

    assert "SHADOW_SIGNAL_2S_ENABLED=false" in lines
    assert "SHADOW_SIGNAL_2S_POLL_MS=100" in lines
    assert "SHADOW_SIGNAL_2S_TTL_MS=2000" in lines
    assert "REDIS_HOST=127.0.0.1" in lines
    assert "REDIS_PORT=6379" in lines
    assert not any(line.startswith("DATABASE_URL=") for line in lines)
    assert not any(line.startswith("READ_DATABASE_URL=") for line in lines)
    assert not any("SELECTION" in line for line in lines)
    assert not any("DECISION" in line for line in lines)


def test_shadow_signal_2s_service_is_redis_only_and_hardened():
    service = (
        ROOT / "deployment" / "price-collector-shadow-signal-2s.service"
    ).read_text()

    assert (
        "EnvironmentFile=/etc/price-collector/shadow-signal-2s.env"
        in service
    )
    assert (
        "ExecStart=/opt/price-collector/.venv/bin/python "
        "-m price_collector.shadow_signal_2s_collector"
    ) in service
    assert "redis-server.service" in service
    assert "price-collector-binance-futures.service" in service
    assert "price-collector-polymarket-chainlink.service" in service
    assert "postgresql.service" not in service
    assert "Restart=on-failure" in service
    assert "StartLimitIntervalSec=60" in service
    assert "StartLimitBurst=3" in service
    assert "NoNewPrivileges=true" in service
    assert "PrivateDevices=true" in service
    assert "PrivateTmp=true" in service
    assert "ProtectSystem=strict" in service
    assert "ProtectHome=true" in service
    assert "CapabilityBoundingSet=\n" in service
    assert "ReadWritePaths=" not in service


def test_collector_services_depend_on_local_redis_service():
    for filename in (
        "price-collector.service",
        "price-collector-polymarket-chainlink.service",
        "price-collector-binance-futures.service",
    ):
        service = (ROOT / "deployment" / filename).read_text()

        assert "After=network-online.target postgresql.service redis-server.service" in service
        assert "Wants=network-online.target redis-server.service" in service


def test_redis_server_is_documented_as_loopback_only():
    readme = (ROOT / "README.md").read_text()
    operations = (ROOT / "OPERATIONS.md").read_text()

    assert "redis-server" in readme
    assert "bind 127.0.0.1" in readme
    assert "127.0.0.1:6379" in readme
    assert "0.0.0.0:6379" in readme
    assert "btc:live:binance_spot" in operations
    assert "btc:live:chainlink" in operations
    assert "btc:live:futures" in operations


def test_no_runtime_code_uses_direct_chainlink_websocket():
    scanned_files = []
    for folder in ("price_collector", "deployment"):
        for path in (ROOT / folder).rglob("*"):
            if path.suffix not in {".py", ".service"}:
                continue
            scanned_files.append(path)
            text = path.read_text()
            assert "wss://ws.dataengine.chain.link" not in text
            assert "ws.dataengine.chain.link" not in text

    assert scanned_files
