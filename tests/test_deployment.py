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
    assert not any(line.startswith("READ_DATABASE_URL=") for line in lines)


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
