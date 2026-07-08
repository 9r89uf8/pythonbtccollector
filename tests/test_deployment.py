from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_price_api_service_binds_to_loopback_only():
    service = (ROOT / "deployment" / "price-api.service").read_text()

    assert "EnvironmentFile=/etc/price-collector/api.env" in service
    assert "--host 127.0.0.1" in service
    assert "--host 0.0.0.0" not in service


def test_api_env_example_contains_reader_credentials_only():
    lines = (ROOT / "deployment" / "api.env.example").read_text().splitlines()

    assert "READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector" in lines
    assert not any(line.startswith("DATABASE_URL=") for line in lines)


def test_collector_env_example_contains_writer_credentials_only():
    lines = (ROOT / "deployment" / "collector.env.example").read_text().splitlines()

    assert "DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector" in lines
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
