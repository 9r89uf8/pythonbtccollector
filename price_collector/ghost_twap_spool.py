"""Bounded, fsynced outbox for the optional ghost worker (never source data)."""
from __future__ import annotations

import hashlib
import json
from itertools import islice
import os
from pathlib import Path
import re


class GhostSpool:
    def __init__(self, directory: Path, max_records: int = 512,
                 max_record_bytes: int = 131072) -> None:
        self.directory = Path(directory)
        self.max_records = max_records
        self.max_record_bytes = max_record_bytes
        self._lock = None

    def open(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._lock = (self.directory / 'worker.lock').open('a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                self._lock.seek(0)
                self._lock.write(b'0')
                self._lock.flush()
                self._lock.seek(0)
                msvcrt.locking(self._lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # A .tmp file never authorized publication: replace+directory fsync
            # completes first. Clean only our own interrupted atomic writes.
            for temporary in self.directory.glob('*.tmp'):
                if (temporary.name == 'campaign.tmp' or re.fullmatch(r'[0-9a-f]{64}\.tmp', temporary.name)):
                    if temporary.is_symlink() or temporary.resolve().parent != self.directory.resolve():
                        raise ValueError('unsafe ghost outbox temporary path')
                    temporary.unlink()
            self._sync_directory()
        except BaseException:
            self._lock.close()
            self._lock = None
            raise

    def close(self) -> None:
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def _sync_directory(self) -> None:
        if os.name != 'nt':
            fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def _atomic(self, path: Path, raw: bytes) -> None:
        if len(raw) > self.max_record_bytes:
            raise ValueError('ghost outbox record exceeds bound')
        temporary = path.with_suffix('.tmp')
        with temporary.open('wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        self._sync_directory()

    def _path(self, record: dict) -> Path:
        key = str(record['run_id']) + ':' + str(record['decision_id'])
        return self.directory / (hashlib.sha256(key.encode()).hexdigest() + '.row')

    def write(self, record: dict) -> None:
        path = self._path(record)
        if not path.exists() and len(list(islice(self.directory.glob('*.row'), self.max_records))) >= self.max_records:
            raise ValueError('ghost outbox capacity exhausted')
        raw = json.dumps(record, sort_keys=True, separators=(',', ':')).encode()
        if path.exists():
            existing = json.loads(path.read_bytes())
            if existing['frozen_json'] != record['frozen_json']:
                raise ValueError('frozen outbox data changed')
            if existing['version'] > record['version']:
                return
            if existing['version'] == record['version'] and existing != record:
                raise ValueError('outbox version conflict')
        self._atomic(path, raw)

    def read_all(self) -> list[dict]:
        paths = sorted(islice(self.directory.glob('*.row'), self.max_records + 1))
        if len(paths) > self.max_records:
            raise ValueError('oversized ghost outbox')
        result = []
        for path in paths:
            if path.is_symlink():
                raise ValueError('unsafe ghost outbox record path')
            if path.stat().st_size > self.max_record_bytes:
                raise ValueError('oversized ghost outbox record')
            record = json.loads(path.read_bytes())
            if self._path(record) != path:
                raise ValueError('invalid ghost outbox identity')
            result.append(record)
        return result

    def remove(self, record: dict) -> None:
        path = self._path(record)
        if path.exists():
            current = json.loads(path.read_bytes())
            if current['version'] != record['version']:
                raise ValueError('refusing to remove newer outbox version')
            path.unlink()
            self._sync_directory()

    def campaign(self, start_ms: int) -> dict:
        path = self.directory / 'campaign.json'
        if path.exists():
            state = json.loads(path.read_bytes())
            self._validate_campaign(state)
            if state['start_ms'] != start_ms:
                raise ValueError('existing canary start differs; archive the completed outbox first')
            return state
        state = {'start_ms': start_ms, 'stop_reason': None, 'last_wall_ms': start_ms}
        self.save_campaign(state)
        return state

    def save_campaign(self, state: dict) -> None:
        self._validate_campaign(state)
        self._atomic(self.directory / 'campaign.json',
                     json.dumps(state, sort_keys=True, separators=(',', ':')).encode())

    @staticmethod
    def _validate_campaign(state: dict) -> None:
        if not isinstance(state, dict) or set(state) != {'start_ms', 'stop_reason', 'last_wall_ms'}:
            raise ValueError('invalid canary metadata fields')
        if type(state['start_ms']) is not int or type(state['last_wall_ms']) is not int:
            raise ValueError('invalid canary clock types')
        if state['start_ms'] <= 0 or state['last_wall_ms'] < state['start_ms']:
            raise ValueError('invalid canary clock order')
        reason = state['stop_reason']
        if reason is not None and (not isinstance(reason, str) or not reason or len(reason) > 128):
            raise ValueError('invalid canary stop reason')
