"""Check the operator-facing default-off and schema-before-restart contract."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def test_ghost_install_applies_schema_before_chainlink_restart():
    operations = (ROOT / 'OPERATIONS.md').read_text()
    section = operations.split('## Ghost TWAP checkpoint B', 1)[1].split('\n## ', 1)[0]
    blocks = re.findall(r'```bash\n(.*?)\n```', section, re.DOTALL)
    block = next(value for value in blocks if 'git pull --ff-only' in value)
    required = [
        'cd /opt/price-collector',
        'sudo -u pricecollector git pull --ff-only',
        'sudo -u pricecollector .venv/bin/pip install -r requirements.txt',
        'sudo -u postgres psql --single-transaction -v ON_ERROR_STOP=1 -d price_collector -f /opt/price-collector/schema.sql',
        'sudoedit /etc/price-collector/collector.env',
        'sudo systemctl restart price-collector-polymarket-chainlink',
        'sudo systemctl status price-collector-polymarket-chainlink --no-pager',
    ]
    positions = [block.index(command) for command in required]
    assert positions == sorted(positions)
    assert 'restart price-api' not in block
    assert 'restart redis-server' not in block


def test_ghost_example_is_disabled_and_tunnel_has_no_runtime_credentials():
    example = (ROOT / 'deployment/collector.env.example').read_text()
    settings = dict(line.split('=', 1) for line in example.splitlines()
                    if line and not line.startswith('#') and '=' in line)
    assert settings['GHOST_TWAP_ENABLED'] == 'false'
    assert settings['GHOST_TWAP_CANARY_START_MS'] == '0'
    assert settings['GHOST_TWAP_STATE_DIRECTORY'] == '/var/lib/price-collector/ghost-twap'
    assert settings['GHOST_TWAP_DATABASE_FILESYSTEM_PATH'] == '/var/lib/postgresql'
    assert settings['GHOST_TWAP_SOURCE_MAX_AGE_MS'] == '5000'
    assert settings['GHOST_TWAP_RECEIPT_MAX_AGE_MS'] == '3000'
    for name in ('droplet.env.example', 'deployment/api.env.example'):
        active = '\n'.join(line for line in (ROOT / name).read_text().splitlines()
                           if not line.startswith('#'))
        ghost_settings = dict(line.split('=', 1) for line in active.splitlines()
                              if line.startswith('GHOST_TWAP_') and '=' in line)
        assert ghost_settings == ({'GHOST_TWAP_API_ENABLED': 'false'}
                                  if name == 'deployment/api.env.example' else {})


def test_chainlink_stop_budget_covers_core_and_optional_ghost_cleanup():
    from price_collector import polymarket_chainlink_collector as collector
    from price_collector.config import Settings

    unit = (ROOT / 'deployment/price-collector-polymarket-chainlink.service').read_text()
    stop_seconds = int(re.search(r'^TimeoutStopSec=(\d+)$', unit, re.MULTILINE).group(1))
    settings = Settings()
    readers = max(collector.CHAINLINK_READER_SHUTDOWN_TIMEOUT_SECONDS,
                  settings.POLYMARKET_TWAP_PERSIST_SHUTDOWN_TIMEOUT_SECONDS + 2)
    # Core delivery and ghost shutdown run sequentially, after reader teardown.
    # Leave ten seconds for normal final cache/pool cleanup and scheduler slack.
    assert stop_seconds >= (readers + collector.CHAINLINK_DELIVERY_SHUTDOWN_TIMEOUT_SECONDS
                            + collector.GHOST_SHUTDOWN_TIMEOUT_SECONDS + 10)
