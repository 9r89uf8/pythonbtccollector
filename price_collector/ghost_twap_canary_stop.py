"""Root-only, campaign-matched operator stop; never enables or resets a canary."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time

VERSION = 1
STATE_ROOT = Path('/var/lib/price-collector')
ENV_PATH = Path('/etc/price-collector/collector.env')
SERVICE = 'price-collector-polymarket-chainlink'
RESTART_TIMEOUT_SECONDS = 150
MAX_ENV_BYTES = 262144
MAX_RECORD_BYTES = 8192


def _require_root() -> None:
    if getattr(os, 'geteuid', lambda: -1)() != 0:
        raise PermissionError('canary stop requires root')


def _check_path(path: Path, *, bounded: bool, directory: bool = False,
                missing_leaf: bool = False) -> Path:
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('an absolute path without parent traversal is required')
    if bounded:
        try:
            relative = path.relative_to(STATE_ROOT)
        except ValueError:
            raise ValueError('operator paths must stay beneath the state root') from None
        if not relative.parts:
            raise ValueError('the state root itself is not a campaign path')
    # Check every component, including the root, rather than resolving a link
    # and then accepting its destination. Existing hard-linked files also fail.
    parts = [*reversed(path.parents), path]
    for item in parts:
        try:
            info = item.lstat()
        except FileNotFoundError:
            if item == path and missing_leaf:
                return path
            raise ValueError('operator path parent is missing') from None
        if stat.S_ISLNK(info.st_mode):
            raise ValueError('operator paths must not contain links')
        if item != path or directory:
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError('operator path parent must be a directory')
        elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError('operator file must be a single-link regular file')
    return path


def _read_file(path: Path, maximum: int) -> tuple[bytes, os.stat_result]:
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError('operator file must be a single-link regular file')
        raw = handle.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError('operator file exceeds its size bound')
    return raw, info


def _environment(raw: bytes) -> tuple[list[bytes], dict[str, str]]:
    lines = raw.splitlines(keepends=True)
    values = {}
    wanted = {'GHOST_TWAP_STATE_DIRECTORY', 'GHOST_TWAP_ENABLED'}
    for line in lines:
        text = line.decode('utf-8')
        if not text.strip() or text.lstrip().startswith('#'):
            continue
        key, sep, value = text.partition('=')
        if sep and key.strip() in wanted:
            key = key.strip()
            if key in values or not text.startswith(key + '='):
                raise ValueError('ambiguous campaign environment key')
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key] = value
    if set(values) != wanted:
        raise ValueError('required campaign environment keys are missing')
    return lines, values


def _sync_directory(path: Path) -> None:
    # Production is Linux. Windows tests exercise file fsync and atomic replace.
    if os.name != 'nt':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextmanager
def _exclusive_lock(path: Path):
    import fcntl
    _check_path(path, bounded=True, missing_leaf=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError('unsafe stop lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def _disable(raw: bytes, lines: list[bytes], info: os.stat_result) -> None:
    updated = []
    for line in lines:
        if line.startswith(b'GHOST_TWAP_ENABLED='):
            ending = b'\r\n' if line.endswith(b'\r\n') else b'\n' if line.endswith(b'\n') else b''
            line = b'GHOST_TWAP_ENABLED=false' + ending
        updated.append(line)
    replacement = b''.join(updated)
    if replacement == raw:
        return
    fd, name = tempfile.mkstemp(prefix='.ghost-stop-', dir=ENV_PATH.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'wb') as handle:
            if hasattr(os, 'fchown'):
                os.fchown(handle.fileno(), info.st_uid, info.st_gid)
            if hasattr(os, 'fchmod'):
                os.fchmod(handle.fileno(), stat.S_IMODE(info.st_mode))
            else:
                os.chmod(temporary, stat.S_IMODE(info.st_mode))
            handle.write(replacement)
            handle.flush()
            os.fsync(handle.fileno())
        _check_path(ENV_PATH, bounded=False)
        current, current_info = _read_file(ENV_PATH, MAX_ENV_BYTES)
        identity = lambda item: (item.st_dev, item.st_ino, item.st_mode, item.st_uid, item.st_gid)
        if current != raw or identity(current_info) != identity(info):
            raise ValueError('environment changed during stop preparation')
        os.replace(temporary, ENV_PATH)
        _sync_directory(ENV_PATH.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _restart() -> int:
    # No environment file is sourced or forwarded; no subprocess text can leak
    # secrets into the compact operation record.
    result = subprocess.run(['/usr/bin/systemctl', 'restart', SERVICE],
                            timeout=RESTART_TIMEOUT_SECONDS, check=False,
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    return result.returncode


def _valid_previous(record: object, expected_state: Path) -> bool:
    keys = {'version', 'state_directory', 'service', 'requested_wall_ns',
            'requested_monotonic_ns', 'completed_wall_ns', 'completed_monotonic_ns',
            'status', 'phase', 'restart_returncode', 'restart_timed_out', 'error_type'}
    if (not isinstance(record, dict) or set(record) != keys
            or type(record['version']) is not int or record['version'] != VERSION
            or record['state_directory'] != str(expected_state) or record['service'] != SERVICE
            or type(record['restart_timed_out']) is not bool
            or (record['restart_returncode'] is not None and type(record['restart_returncode']) is not int)
            or (record['error_type'] is not None and (not isinstance(record['error_type'], str)
                or not record['error_type'].isidentifier() or len(record['error_type']) > 64))):
        return False
    if any(not isinstance(record[key], str) or not record[key].isascii()
           or not record[key].isdigit() or len(record[key]) > 20
           for key in ('requested_wall_ns', 'requested_monotonic_ns',
                       'completed_wall_ns', 'completed_monotonic_ns')):
        return False
    if record['status'] == 'stopped':
        return (record['phase'] == 'complete' and record['restart_returncode'] == 0
                and record['error_type'] is None and not record['restart_timed_out'])
    return record['status'] == 'failed' and record['phase'] in ('disable', 'restart')


def stop_canary(expected_state: Path, output: Path, *, restart=None,
                wall_ns=time.time_ns, monotonic_ns=time.monotonic_ns) -> dict:
    _require_root()
    expected_state = _check_path(expected_state, bounded=True, directory=True)
    output = _check_path(output, bounded=True, missing_leaf=True)
    try:
        output.relative_to(expected_state)
    except ValueError:
        pass
    else:
        raise ValueError('stop evidence must be outside the campaign state directory')
    if output.name.endswith('.lock'):
        raise ValueError('output must not be a lock file')
    lock = output.with_name(output.name + '.lock')
    with _exclusive_lock(lock):
        _check_path(expected_state, bounded=True, directory=True)
        _check_path(output, bounded=True, missing_leaf=True)
        _check_path(ENV_PATH, bounded=False)
        raw, info = _read_file(ENV_PATH, MAX_ENV_BYTES)
        lines, values = _environment(raw)
        if values['GHOST_TWAP_STATE_DIRECTORY'] != str(expected_state):
            raise ValueError('refusing to stop a different campaign')
        if output.exists():
            previous = json.loads(_read_file(output, MAX_RECORD_BYTES)[0])
            if (not _valid_previous(previous, expected_state)
                    or values['GHOST_TWAP_ENABLED'] != 'false'):
                raise ValueError('existing stop record does not match stopped campaign')
            return previous
        record = dict(version=VERSION, state_directory=str(expected_state), service=SERVICE,
            requested_wall_ns=str(wall_ns()), requested_monotonic_ns=str(monotonic_ns()),
            status='failed', phase='disable', restart_returncode=None, restart_timed_out=False,
            error_type=None)
        try:
            _disable(raw, lines, info)
            record['phase'] = 'restart'
            code = (restart or _restart)()
            record['restart_returncode'] = code
            if code == 0:
                record.update(status='stopped', phase='complete')
        except subprocess.TimeoutExpired:
            record.update(error_type='TimeoutExpired', restart_timed_out=True)
        except Exception as exc:
            record['error_type'] = type(exc).__name__
        record.update(completed_wall_ns=str(wall_ns()), completed_monotonic_ns=str(monotonic_ns()))
        # Exclusive creation preserves any racing/existing evidence. A killed
        # write remains an explicit truncated record, never an apparent success.
        _check_path(output, bounded=True, missing_leaf=True)
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'wb') as handle:
            handle.write((json.dumps(record, sort_keys=True) + '\n').encode())
            handle.flush()
            os.fsync(handle.fileno())
        _sync_directory(output.parent)
        return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-state', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    try:
        result = stop_canary(args.expected_state, args.output)
    except Exception as exc:
        # Exception messages can contain paths or rejected configuration values.
        print(json.dumps({'version': VERSION, 'status': 'rejected', 'error_type': type(exc).__name__}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result['status'] == 'stopped' else 1


if __name__ == '__main__':
    raise SystemExit(main())
