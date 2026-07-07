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

