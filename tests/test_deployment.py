
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_price_api_service_binds_to_loopback_only():
    service = (ROOT / "deployment" / "price-api.service").read_text()

    assert "EnvironmentFile=/etc/price-collector/api.env" in service
    assert "--host 127.0.0.1" in service
    assert "--host 0.0.0.0" not in service
    assert "redis-server.service" in service


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


def test_deployment_contains_only_active_runtime_units():
    unit_names = {
        path.name
        for path in (ROOT / "deployment").glob("*.service")
    }

    assert unit_names == {
        "price-api.service",
        "price-collector.service",
        "price-collector-binance-futures.service",
        "price-collector-polymarket-chainlink.service",
        "price-collector-polymarket-probabilities.service",
    }


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
