"""Operator stop tests use local temporary files and never invoke systemctl."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

import price_collector.ghost_twap_canary_stop as module


@pytest.fixture
def operation(tmp_path, monkeypatch):
    root = tmp_path / 'state-root'
    root.mkdir()
    state = root / 'campaign-new'
    state.mkdir()
    (state / 'campaign.json').write_text('{"start_ms":123,"stop_reason":null}')
    old = root / 'campaign-old'
    old.mkdir()
    (old / 'campaign.json').write_text('old evidence')
    env = tmp_path / 'collector.env'
    raw = ('# keep comments\r\nDATABASE_URL=secret-do-not-log\r\n'
           f'GHOST_TWAP_STATE_DIRECTORY={state}\r\n'
           'GHOST_TWAP_CANARY_START_MS=123\r\nGHOST_TWAP_ENABLED=true\r\n'
           'GHOST_TWAP_SOURCE_MAX_AGE_MS=5000\r\nLAST=no-newline').encode()
    env.write_bytes(raw)
    os.chmod(env, 0o600)
    monkeypatch.setattr(module, 'STATE_ROOT', root)
    monkeypatch.setattr(module, 'ENV_PATH', env)
    monkeypatch.setattr(module, '_require_root', lambda: None)
    @contextmanager
    def lock(path):
        # fcntl is Linux-only; path checks still run on Windows.
        module._check_path(path, bounded=True, missing_leaf=True)
        yield
    monkeypatch.setattr(module, '_exclusive_lock', lock)
    return state, root / 'stop.json', env, raw, old


def test_only_flag_changes_and_clocks_bracket_single_restart(operation):
    state, output, env, raw, old = operation
    original_mode = stat.S_IMODE(env.stat().st_mode)
    before = {p: p.read_bytes() for p in (state / 'campaign.json', old / 'campaign.json')}
    calls = []
    times = iter([100, 120])
    monos = iter([200, 230])
    def restart():
        calls.append('restart')
        assert env.read_bytes() == raw.replace(b'GHOST_TWAP_ENABLED=true', b'GHOST_TWAP_ENABLED=false')
        return 0
    result = module.stop_canary(state, output, restart=restart,
                                wall_ns=lambda: next(times), monotonic_ns=lambda: next(monos))
    assert result['status'] == 'stopped'
    assert (result['requested_wall_ns'], result['completed_wall_ns']) == ('100', '120')
    assert (result['requested_monotonic_ns'], result['completed_monotonic_ns']) == ('200', '230')
    assert stat.S_IMODE(env.stat().st_mode) == original_mode
    assert {p: p.read_bytes() for p in before} == before
    saved = output.read_bytes()
    assert b'secret-do-not-log' not in saved
    assert json.loads(saved) == result
    assert module.stop_canary(state, output, restart=restart) == result
    assert output.read_bytes() == saved and calls == ['restart']


@pytest.mark.parametrize('failure', ['nonzero', 'timeout', 'exception'])
def test_restart_failure_is_recorded_without_secrets_or_reenable(operation, failure):
    state, output, env, _, _ = operation
    def restart():
        if failure == 'nonzero':
            return 7
        if failure == 'timeout':
            raise subprocess.TimeoutExpired('secret-command', 150, output='secret-response')
        raise OSError('secret-credential')
    result = module.stop_canary(state, output, restart=restart)
    assert result['status'] == 'failed' and result['phase'] == 'restart'
    assert result['restart_timed_out'] == (failure == 'timeout')
    assert b'GHOST_TWAP_ENABLED=false' in env.read_bytes()
    assert b'secret' not in output.read_bytes()
    assert module.stop_canary(state, output, restart=lambda: pytest.fail('must preserve failed attempt')) == result


def test_failed_atomic_update_never_restarts_and_saves_failure(operation, monkeypatch):
    state, output, env, raw, _ = operation
    def fail(*args):
        raise OSError('secret-fsync-path')
    monkeypatch.setattr(module.os, 'replace', fail)
    result = module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert result['status'] == 'failed' and result['phase'] == 'disable'
    assert env.read_bytes() == raw
    assert b'secret' not in output.read_bytes()
    assert not list(env.parent.glob('.ghost-stop-*'))


def test_other_campaign_duplicate_and_missing_keys_fail_before_edit(operation):
    state, output, env, raw, old = operation
    for data, expected in [(raw, old),
                           (raw + b'\nGHOST_TWAP_ENABLED=true\n', state),
                           (raw.replace(b'GHOST_TWAP_ENABLED=true\r\n', b''), state),
                           (raw.replace(b'GHOST_TWAP_ENABLED=', b' GHOST_TWAP_ENABLED='), state)]:
        env.write_bytes(data)
        with pytest.raises(ValueError):
            module.stop_canary(expected, output, restart=lambda: pytest.fail('must not restart'))
        assert env.read_bytes() == data and not output.exists()


def test_existing_evidence_and_reenabled_campaign_are_not_overwritten(operation):
    state, output, env, raw, _ = operation
    output.write_text('{"unrelated":"evidence"}')
    saved = output.read_bytes()
    with pytest.raises(ValueError):
        module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert output.read_bytes() == saved and env.read_bytes() == raw
    output.unlink()
    module.stop_canary(state, output, restart=lambda: 0)
    saved = output.read_bytes()
    env.write_bytes(raw)
    with pytest.raises(ValueError):
        module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert env.read_bytes() == raw and output.read_bytes() == saved


@pytest.mark.parametrize('which', ['outside', 'prefix', 'relative', 'parent', 'root'])
def test_strict_path_boundary_before_mutations(operation, which):
    state, output, env, raw, _ = operation
    invalid = {'outside': env.parent / 'outside.json',
               'prefix': state.parent.with_name(state.parent.name + '-other') / 'stop.json',
               'relative': Path('stop.json'),
               'parent': state / '..' / 'stop.json',
               'root': state.parent}[which]
    with pytest.raises(ValueError):
        module.stop_canary(state, invalid, restart=lambda: pytest.fail('must not restart'))
    assert env.read_bytes() == raw and not output.exists()


def test_stop_evidence_cannot_create_files_inside_campaign_state(operation):
    state, _, env, raw, _ = operation
    before = set(state.iterdir())
    with pytest.raises(ValueError):
        module.stop_canary(state, state / 'unused.json', restart=lambda: pytest.fail('must not restart'))
    assert set(state.iterdir()) == before and env.read_bytes() == raw


def test_hardlinked_output_rejected(operation):
    state, output, env, raw, _ = operation
    target = state.parent / 'evidence.txt'
    target.write_text('original')
    os.link(target, output)
    with pytest.raises(ValueError):
        module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert target.read_text() == 'original' and env.read_bytes() == raw


def test_symlink_parent_rejected(operation):
    state, output, env, raw, _ = operation
    alias = state.parent / 'alias'
    try:
        alias.symlink_to(state, target_is_directory=True)
    except OSError:
        pytest.skip('creating Windows symlinks requires a privilege')
    with pytest.raises(ValueError):
        module.stop_canary(state, alias / 'stop.json', restart=lambda: pytest.fail('must not restart'))
    assert env.read_bytes() == raw and not output.exists()


def test_hardlinked_environment_is_rejected(operation):
    state, output, env, raw, _ = operation
    os.link(env, env.with_suffix('.copy'))
    with pytest.raises(ValueError):
        module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert env.read_bytes() == raw


def test_environment_change_during_preparation_is_not_overwritten(operation, monkeypatch):
    state, output, env, _, _ = operation
    read = module._read_file
    calls = []
    changed = b'OTHER=changed-by-operator\n'
    def racing(path, maximum):
        if path == env:
            calls.append(path)
            if len(calls) == 2:
                env.write_bytes(changed)
        return read(path, maximum)
    monkeypatch.setattr(module, '_read_file', racing)
    result = module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert result['status'] == 'failed' and env.read_bytes() == changed


def test_systemctl_command_is_fixed_bounded_and_does_not_capture_output(monkeypatch):
    seen = []
    def run(args, **kwargs):
        seen.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0)
    monkeypatch.setattr(module.subprocess, 'run', run)
    assert module._restart() == 0
    args, options = seen[0]
    assert args == ['/usr/bin/systemctl', 'restart', 'price-collector-polymarket-chainlink']
    assert options['timeout'] == 150 and options['stdout'] == subprocess.DEVNULL
    assert options['stderr'] == subprocess.DEVNULL and options['stdin'] == subprocess.DEVNULL


def test_cli_has_no_start_operation_and_reports_only_fixed_error_type(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['stop', '--expected-state', '/var/lib/price-collector/test',
                                    '--output', '/var/lib/price-collector/stop.json'])
    def reject(*args):
        raise PermissionError('secret')
    monkeypatch.setattr(module, 'stop_canary', reject)
    assert module.main() == 2
    assert 'secret' not in capsys.readouterr().out
    monkeypatch.setattr(sys, 'argv', ['stop', '--enable'])
    with pytest.raises(SystemExit):
        module.main()


def test_root_required(monkeypatch):
    monkeypatch.setattr(module.os, 'geteuid', lambda: 123, raising=False)
    with pytest.raises(PermissionError):
        module._require_root()


@pytest.mark.skipif(os.name == 'nt', reason='production lock uses POSIX flock')
def test_real_operator_lock_is_exclusive_nonblocking_and_released(tmp_path, monkeypatch):
    monkeypatch.setattr(module, 'STATE_ROOT', tmp_path)
    lock = tmp_path / 'stop.json.lock'
    with module._exclusive_lock(lock):
        with pytest.raises(BlockingIOError):
            with module._exclusive_lock(lock):
                pytest.fail('second operator acquired the active lock')
    with module._exclusive_lock(lock):
        assert stat.S_IMODE(lock.stat().st_mode) == 0o600


@pytest.mark.parametrize('change', [
    lambda value: value.pop('completed_wall_ns'),
    lambda value: value.update(service='other-service'),
    lambda value: value.update(restart_returncode=2),
    lambda value: value.update(requested_monotonic_ns=1.5),
    lambda value: value.update(unrelated='secret'),
])
def test_malformed_existing_success_is_never_treated_as_idempotent(operation, change):
    state, output, _, _, _ = operation
    record = module.stop_canary(state, output, restart=lambda: 0)
    change(record)
    output.write_text(json.dumps(record))
    saved = output.read_bytes()
    with pytest.raises(ValueError):
        module.stop_canary(state, output, restart=lambda: pytest.fail('must not restart'))
    assert output.read_bytes() == saved
