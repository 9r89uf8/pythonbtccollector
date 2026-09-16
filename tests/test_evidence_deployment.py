from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def _operations_section(title: str) -> str:
    document = (ROOT / "README.md").read_text(encoding="utf-8")
    operations = re.split(r"(?m)^#{1,6} Production operations[ \t]*$", document, maxsplit=1)[1]
    heading = re.search(r"(?m)^(#{1,6}) " + re.escape(title) + r"[ \t]*$", operations)
    assert heading is not None, f"Missing operations section: {title}"
    rest = operations[heading.end():]
    return re.split(r"(?m)^#{1," + str(len(heading.group(1))) + r"} ", rest, maxsplit=1)[0]


def _environment_values(path: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1)
        for line in path.read_text().splitlines()
        if line and not line.startswith("#") and "=" in line
    )


def test_evidence_example_is_opt_in_and_has_bounded_storage_settings():
    values = _environment_values(ROOT / "deployment" / "collector.env.example")
    expected = {
        "POLYMARKET_EVIDENCE_ENABLED": "false",
        "POLYMARKET_EVIDENCE_POLL_SECONDS": "5",
        "POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS": "100",
        "POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS": "120",
        "POLYMARKET_EVIDENCE_QUEUE_MAX_RECORDS": "5000",
        "POLYMARKET_EVIDENCE_QUOTE_QUEUE_MAX_RECORDS": "2000",
        "POLYMARKET_EVIDENCE_BATCH_MAX_ROWS": "250",
        "POLYMARKET_EVIDENCE_FLUSH_MS": "500",
        "POLYMARKET_EVIDENCE_WARN_RELATION_MB": "4096",
        "POLYMARKET_EVIDENCE_MAX_RELATION_MB": "6144",
    }
    assert {key: values[key] for key in expected} == expected
    assert values["RAW_FUTURES_TRACE_ENABLED"] == "false"
    assert values["RAW_CHAINLINK_EVENTS_ENABLED"] == "false"


def test_evidence_settings_stay_out_of_reader_and_local_tunnel_environments():
    for name in ("deployment/api.env.example", "droplet.env.example"):
        values = _environment_values(ROOT / name)
        assert not any(key.startswith("POLYMARKET_EVIDENCE_") for key in values)
        assert "DATABASE_URL" not in values

    tunnel = (ROOT / "droplet.env.example").read_text()
    assert "/etc/price-collector/collector.env" in tunnel
    assert "POLYMARKET_EVIDENCE_ENABLED=false" in tunnel


def test_evidence_rollout_applies_schema_before_probability_restart():
    operations = _operations_section("Deploy compact Polymarket evidence")
    blocks = re.findall(r"```bash\n(.*?)\n```", operations, flags=re.DOTALL)
    rollout = next(block for block in blocks if "git pull --ff-only" in block
                   and "sudo systemctl restart price-collector-polymarket-probabilities" in block)
    ordered = [
        "cd /opt/price-collector",
        "sudo -u pricecollector git pull --ff-only",
        "sudo -u pricecollector .venv/bin/pip install -r requirements.txt",
        "sudo -u postgres psql -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql",
        "sudoedit /etc/price-collector/collector.env",
        "sudo systemctl restart price-collector-polymarket-probabilities",
        "sudo systemctl status price-collector-polymarket-probabilities --no-pager",
    ]
    positions = [rollout.index(command) for command in ordered]
    assert positions == sorted(positions)
    assert rollout.count("systemctl restart") == 1
    assert "journalctl -u price-collector-polymarket-probabilities -n 100" in rollout
    assert "curl --fail http://127.0.0.1:9000/healthz" in rollout


def test_evidence_runbook_checks_linked_receipt_time_and_total_relation_size():
    operations = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "JOIN polymarket_evidence_payloads p USING (payload_hash)" in operations
    assert "p.payload->>'price_to_beat'" in operations
    assert "o.response_date, o.response_age_seconds" in operations
    assert "`response_sha256`" in operations
    assert "`clob_order_rules`" in operations
    assert "`/markets/{condition_id}`" in operations
    assert "`/clob-markets/{condition_id}`" in operations
    assert "o.received_wall_ns / 1000000 AS ms_before_close" in operations
    assert "to_timestamp(observed_wall_ns / 1000000000)" in operations
    assert "pg_total_relation_size(rel)" in operations
    assert "pg_table_size(rel)" in operations
    assert "pg_indexes_size(rel)" in operations
    assert "SET statement_timeout = '15s'" in operations
    assert "currently missing" not in (ROOT / "README.md").read_text()
