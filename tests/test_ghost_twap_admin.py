"""External verification and explicit acknowledgement must precede expiry."""
import asyncio
from hashlib import sha256
import io
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import price_collector.ghost_twap_admin as admin
from price_collector.ghost_twap_store import encode_export_row, validate_record


def export_bytes(count=1):
    rows = []
    for index in range(count):
        identity = f"d{index:06d}"
        rows.append(validate_record(dict(
            run_id="run", decision_id=identity, decision_wall_ns=1000000000000,
            created_ms=1000000, version=1, terminal=True,
            frozen_json=json.dumps(dict(run_id="run", decision_id=identity,
                                        price="61234.123456789012345678")),
            state_json='{"targets":{},"publication":"failed"}',
        )))
    return b"".join(encode_export_row(row) for row in rows)


def scripted_transfer(monkeypatch, content, *, export_code=0, ack=None, manifest=None):
    calls = []
    manifest = manifest or dict(sha256=sha256(content).hexdigest(), row_count=content.count(b"\n"))
    ack = ack or dict(verified=manifest['row_count'], stale_or_ineligible=0)

    def run(argv, **kwargs):
        command = argv[-1].split()[-1]
        assert kwargs['stdout'] != subprocess.PIPE
        assert kwargs['stderr'] != subprocess.PIPE
        # Subtracting floating monotonic deadlines can round a few picoseconds
        # above the integer limit. Allow 1 ns; this is not a price calculation.
        assert 0 < kwargs['timeout'] <= admin.DEFAULT_TRANSFER_TIMEOUT_SECONDS + 1e-9
        calls.append((command, kwargs))
        if command == 'export':
            assert kwargs['stdin'] == subprocess.DEVNULL
            kwargs['stdout'].write(content)
            kwargs['stderr'].write((json.dumps(manifest) + '\n').encode())
            return SimpleNamespace(returncode=export_code)
        assert command == 'acknowledge'
        assert kwargs['stdin'] != subprocess.PIPE
        payload = kwargs['stdin'].read()
        proofs = [json.loads(line) for line in payload.splitlines()]
        calls[-1] = (command, dict(kwargs, proofs=proofs))
        kwargs['stdout'].write(json.dumps(ack).encode())
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(admin.subprocess, 'run', run)
    return calls


def test_download_verifies_exact_external_bytes_before_acknowledgement(monkeypatch, tmp_path):
    content = export_bytes(2)
    output = tmp_path / 'export.jsonl'
    calls = scripted_transfer(monkeypatch, content)
    original_run = admin.subprocess.run
    def checked_run(argv, **kwargs):
        if argv[-1].endswith(' acknowledge'):
            assert output.read_bytes() == content
            assert output.with_name(output.name + '.manifest.json').exists()
            assert not output.with_name(output.name + '.part').exists()
        return original_run(argv, **kwargs)
    monkeypatch.setattr(admin.subprocess, 'run', checked_run)
    result = admin.download('root@example.test', output)
    assert result['row_count'] == 2
    assert result['sha256'] == sha256(content).hexdigest()
    assert result['external_path'] == str(output.resolve())
    assert result['acknowledgement'] == {'verified': 2, 'stale_or_ineligible': 0}
    assert [command for command, _ in calls] == ['export', 'acknowledge']
    proofs = calls[1][1]['proofs']
    assert proofs[0]['export_sha256'] == result['sha256']
    assert proofs[0]['external_location'].endswith(str(output.resolve()))
    assert len(proofs) == 3
    assert b'61234.123456789012345678' in output.read_bytes()


@pytest.mark.parametrize('failure', ['nonzero', 'truncated', 'wrong_hash', 'wrong_count', 'missing_manifest'])
def test_bad_export_retains_only_partial_and_never_acknowledges(monkeypatch, tmp_path, failure):
    content = export_bytes()
    expected = dict(sha256=sha256(content).hexdigest(), row_count=1)
    if failure == 'truncated':
        content = content[:-1]
    elif failure == 'wrong_hash':
        expected['sha256'] = '0' * 64
    elif failure == 'wrong_count':
        expected['row_count'] = 2
    calls = scripted_transfer(monkeypatch, content, manifest=expected,
                              export_code=7 if failure == 'nonzero' else 0)
    if failure == 'missing_manifest':
        def missing(argv, **kwargs):
            calls.append(('export', kwargs))
            kwargs['stdout'].write(content)
            return SimpleNamespace(returncode=0)
        monkeypatch.setattr(admin.subprocess, 'run', missing)
    output = tmp_path / 'export.jsonl'
    with pytest.raises((ValueError, RuntimeError)):
        admin.download('root@example.test', output)
    assert [command for command, _ in calls] == ['export']
    assert not output.exists()
    assert output.with_name(output.name + '.part').read_bytes() == content
    assert not output.with_name(output.name + '.manifest.json').exists()


@pytest.mark.parametrize('suffix', ['', '.part', '.manifest.json', '.manifest.json.part'])
def test_download_never_overwrites_any_existing_artifact(monkeypatch, tmp_path, suffix):
    output = tmp_path / 'export.jsonl'
    existing = output.with_name(output.name + suffix)
    existing.write_bytes(b'owned existing bytes')
    monkeypatch.setattr(admin.subprocess, 'run', lambda *a, **kw: pytest.fail('must not start SSH'))
    with pytest.raises(ValueError, match='overwrite'):
        admin.download('root@example.test', output)
    assert existing.read_bytes() == b'owned existing bytes'


def test_atomic_publication_does_not_replace_a_racing_destination(monkeypatch, tmp_path):
    content = export_bytes()
    calls = scripted_transfer(monkeypatch, content)
    output = tmp_path / 'export.jsonl'
    publish = admin._publish_without_overwrite
    def race(partial, final):
        final.write_bytes(b'another owners file')
        publish(partial, final)
    monkeypatch.setattr(admin, '_publish_without_overwrite', race)
    with pytest.raises(FileExistsError):
        admin.download('root@example.test', output)
    assert output.read_bytes() == b'another owners file'
    assert output.with_name(output.name + '.part').read_bytes() == content
    assert [command for command, _ in calls] == ['export']


def test_second_verification_failure_starts_no_acknowledgement_process(monkeypatch, tmp_path):
    calls = scripted_transfer(monkeypatch, export_bytes())
    original = admin.iter_export_proofs
    def tamper(path, **kwargs):
        Path(path).write_bytes(b'corrupted after initial verification\n')
        return original(path, **kwargs)
    monkeypatch.setattr(admin, 'iter_export_proofs', tamper)
    with pytest.raises(ValueError):
        admin.download('root@example.test', tmp_path / 'export.jsonl')
    assert [command for command, _ in calls] == ['export']


@pytest.mark.parametrize('ack', [
    {'verified': 0, 'stale_or_ineligible': 1},
    {'verified': 0, 'stale_or_ineligible': 0},
    {'verified': True, 'stale_or_ineligible': 0},
])
def test_stale_or_incomplete_ack_is_failure_and_never_invokes_expiry(monkeypatch, tmp_path, ack):
    calls = scripted_transfer(monkeypatch, export_bytes(), ack=ack)
    output = tmp_path / 'export.jsonl'
    with pytest.raises(ValueError, match='acknowledgement counts'):
        admin.download('root@example.test', output)
    assert output.exists()
    assert [command for command, _ in calls] == ['export', 'acknowledge']


def test_one_deadline_is_shared_across_export_and_acknowledgement(monkeypatch, tmp_path):
    calls = scripted_transfer(monkeypatch, export_bytes())
    times = iter([1000.0, 1001.0, 1009.0])
    monkeypatch.setattr(admin.time, 'monotonic', lambda: next(times))
    admin.download('root@example.test', tmp_path / 'export.jsonl', timeout_seconds=10)
    assert [kwargs['timeout'] for _, kwargs in calls] == [9.0, 1.0]


def test_transfer_timeout_preserves_partial_and_suppresses_ack(monkeypatch, tmp_path):
    calls = []
    def timeout(argv, **kwargs):
        calls.append(argv)
        kwargs['stdout'].write(b'partial')
        raise subprocess.TimeoutExpired(argv, kwargs['timeout'])
    monkeypatch.setattr(admin.subprocess, 'run', timeout)
    output = tmp_path / 'export.jsonl'
    with pytest.raises(TimeoutError, match='partial file retained'):
        admin.download('root@example.test', output, timeout_seconds=1)
    assert len(calls) == 1
    assert not output.exists()
    assert output.with_name(output.name + '.part').read_bytes() == b'partial'


def test_early_ack_failure_cannot_deadlock_against_parent_fed_pipes(monkeypatch, tmp_path):
    # More proof bytes than a normal pipe buffer; child fills stderr without
    # reading stdin. It is a local Python child, never an SSH/network test.
    content = export_bytes(1000)
    real_run = subprocess.run
    child = [sys.executable, '-c', "import sys; sys.stderr.write('x'*300000); sys.exit(7)"]
    monkeypatch.setattr(admin, '_ssh', lambda host, command: ['fake-export'] if command == 'export' else child)
    def run(argv, **kwargs):
        assert kwargs['stdout'] != subprocess.PIPE and kwargs['stderr'] != subprocess.PIPE
        if argv == ['fake-export']:
            kwargs['stdout'].write(content)
            kwargs['stderr'].write(json.dumps({'sha256': sha256(content).hexdigest(), 'row_count': 1000}).encode())
            return SimpleNamespace(returncode=0)
        assert kwargs['stdin'] != subprocess.PIPE
        return real_run(argv, **kwargs)
    monkeypatch.setattr(admin.subprocess, 'run', run)
    output = tmp_path / 'export.jsonl'
    with pytest.raises(ValueError, match='Oversized SSH control output'):
        admin.download('ignored-local-test-host', output, timeout_seconds=5)
    assert output.read_bytes() == content


class CapturedOutput:
    def __init__(self):
        self.buffer = io.BytesIO()
    def write(self, value):
        return self.buffer.write(value.encode())
    def flush(self):
        pass


class BoundedInput(io.BytesIO):
    def __init__(self, value):
        super().__init__(value)
        self.limits = []
    def readline(self, size=-1):
        assert 0 < size <= admin.MAX_HEADER_BYTES + 1
        self.limits.append(size)
        return super().readline(size)
    def __iter__(self):
        raise AssertionError('unbounded line iteration is forbidden')


class FakeStore:
    def __init__(self):
        self.batches = []
        self.expiry_limits = []
        self.stale = False
        self.rows = []
    async def initialize(self):
        return {'row_count': len(self.rows), 'incomplete_count': 0}
    async def measure(self, *, include_count):
        assert include_count is True
        return {'relation_bytes': 8192, 'row_count': len(self.rows)}
    async def mark_verified_export(self, batch, **header):
        self.batches.append((list(batch), header))
        return {'verified': [] if self.stale else list(batch), 'stale_or_ineligible': len(batch) if self.stale else 0}
    async def expire_verified(self, *, limit):
        self.expiry_limits.append(limit)
        return []
    async def export_page(self, *, after):
        return self.rows if after is None else []


def wire_server(monkeypatch, store, content=b''):
    import asyncpg
    closed = []
    output = CapturedOutput()
    incoming = BoundedInput(content)
    class Pool:
        async def close(self):
            closed.append(True)
    async def create_pool(*args, **kwargs):
        assert kwargs['max_size'] == 1
        assert kwargs['command_timeout'] == kwargs['timeout'] == 5
        return Pool()
    monkeypatch.setattr(asyncpg, 'create_pool', create_pool)
    monkeypatch.setattr(admin, 'GhostAuditStore', lambda pool: store)
    monkeypatch.setattr(admin.sys, 'stdout', output)
    monkeypatch.setattr(admin.sys, 'stdin', SimpleNamespace(buffer=incoming))
    return incoming, output, closed


def proof_stream(count=1):
    header = {'export_sha256': 'a' * 64, 'external_location': 'owner-computer:/exports/full.jsonl'}
    proof = {'run_id': 'run', 'decision_id': 'd', 'version': 1,
             'frozen_sha256': 'b' * 64, 'state_sha256': 'c' * 64}
    return (json.dumps(header) + '\n' + ''.join(json.dumps(dict(proof, decision_id=f'd{i}')) + '\n'
                                               for i in range(count))).encode()


def test_server_acknowledgement_reads_bounded_lines_and_batches(monkeypatch):
    store = FakeStore()
    incoming, output, closed = wire_server(monkeypatch, store, proof_stream(205))
    asyncio.run(admin.serve('acknowledge'))
    assert [len(batch) for batch, _ in store.batches] == [100, 100, 5]
    assert incoming.limits[0] == admin.MAX_HEADER_BYTES + 1
    assert set(incoming.limits[1:]) == {admin.MAX_PROOF_BYTES + 1}
    assert json.loads(output.buffer.getvalue()) == {'verified': 205, 'stale_or_ineligible': 0}
    assert closed == [True]
    assert store.expiry_limits == []


@pytest.mark.parametrize('content', [b'x' * 9000, proof_stream(0) + b'x' * 5000,
                                    proof_stream(1)[:-1]])
def test_server_rejects_oversized_or_truncated_input_before_ack(monkeypatch, content):
    store = FakeStore()
    _, _, closed = wire_server(monkeypatch, store, content)
    with pytest.raises(ValueError, match='Truncated or oversized'):
        asyncio.run(admin.serve('acknowledge'))
    assert not store.batches
    assert closed == [True]


def test_server_stale_ack_fails_without_expiring_any_row(monkeypatch):
    store = FakeStore()
    store.stale = True
    _, output, closed = wire_server(monkeypatch, store, proof_stream())
    with pytest.raises(ValueError, match='re-export before expiry'):
        asyncio.run(admin.serve('acknowledge'))
    assert json.loads(output.buffer.getvalue()) == {'verified': 0, 'stale_or_ineligible': 1}
    assert not store.expiry_limits
    assert closed == [True]


def test_status_and_expiry_are_explicit_bounded_operations(monkeypatch):
    store = FakeStore()
    _, output, closed = wire_server(monkeypatch, store)
    asyncio.run(admin.serve('status'))
    assert json.loads(output.buffer.getvalue())['measurement']['relation_bytes'] == 8192
    assert not store.expiry_limits
    output.buffer.seek(0)
    output.buffer.truncate()
    asyncio.run(admin.serve('expire', limit=7))
    assert store.expiry_limits == [7]
    assert json.loads(output.buffer.getvalue()) == {'expired': []}
    assert closed == [True, True]


class PagedExportStore(FakeStore):
    def __init__(self, count):
        super().__init__()
        self.rows = [json.loads(line) for line in export_bytes(count).splitlines()]
        self.cursors = []
        self.initializations = 0
        self.initial_incomplete = 0
        self.final_incomplete = 0
        self.final_count_delta = 0

    async def initialize(self):
        self.initializations += 1
        final = self.initializations > 1
        return {'row_count': len(self.rows) + (self.final_count_delta if final else 0),
                'incomplete_count': self.final_incomplete if final else self.initial_incomplete}

    async def export_page(self, *, after):
        self.cursors.append(after)
        rows = self.rows if after is None else [row for row in self.rows
                                                if (row['run_id'], row['decision_id']) > after]
        return rows[:100]


def wire_export_server(monkeypatch, store):
    incoming, output, closed = wire_server(monkeypatch, store)
    errors = CapturedOutput()
    monkeypatch.setattr(admin.sys, 'stderr', errors)
    return output, errors, closed


def test_server_export_streams_all_pages_and_hashes_the_exact_emitted_bytes(monkeypatch):
    store = PagedExportStore(205)
    output, errors, closed = wire_export_server(monkeypatch, store)
    asyncio.run(admin.serve('export'))
    expected = export_bytes(205)
    assert output.buffer.getvalue() == expected
    assert json.loads(errors.buffer.getvalue()) == {
        'row_count': 205, 'sha256': sha256(expected).hexdigest()}
    assert store.cursors == [None, ('run', 'd000099'), ('run', 'd000199'), ('run', 'd000204')]
    assert store.initializations == 2
    assert not store.batches and not store.expiry_limits
    assert closed == [True]


def test_server_empty_export_has_a_valid_empty_manifest(monkeypatch):
    store = PagedExportStore(0)
    output, errors, closed = wire_export_server(monkeypatch, store)
    asyncio.run(admin.serve('export'))
    assert output.buffer.getvalue() == b''
    assert json.loads(errors.buffer.getvalue()) == {'row_count': 0, 'sha256': sha256(b'').hexdigest()}
    assert closed == [True]


@pytest.mark.parametrize('fault', ['initial_incomplete', 'nonterminal_page',
                                  'final_count', 'final_incomplete', 'bad_row_hash'])
def test_server_failed_export_emits_no_success_manifest_and_closes_pool(monkeypatch, fault):
    store = PagedExportStore(2)
    if fault == 'initial_incomplete':
        store.initial_incomplete = 1
    elif fault == 'nonterminal_page':
        store.rows[1]['terminal'] = False
    elif fault == 'final_count':
        store.final_count_delta = 1
    elif fault == 'final_incomplete':
        store.final_incomplete = 1
    else:
        store.rows[1]['state_sha256'] = '0' * 64
    output, errors, closed = wire_export_server(monkeypatch, store)
    with pytest.raises(ValueError):
        asyncio.run(admin.serve('export'))
    assert errors.buffer.getvalue() == b''
    assert closed == [True]
    assert not store.batches and not store.expiry_limits
    if fault == 'initial_incomplete':
        assert output.buffer.getvalue() == b''
        assert store.cursors == []


@pytest.mark.parametrize('fault', ['duplicate_page', 'reversed_page', 'excess_rows'])
def test_server_export_rejects_nonprogressing_or_growing_streams_early(monkeypatch, fault):
    store = PagedExportStore(2)
    if fault == 'duplicate_page':
        async def repeated(*, after):
            store.cursors.append(after)
            return store.rows[:1]
        store.export_page = repeated
    elif fault == 'reversed_page':
        store.rows.reverse()
    else:
        async def initial_count():
            return {'row_count': 1, 'incomplete_count': 0}
        store.initialize = initial_count
    output, errors, closed = wire_export_server(monkeypatch, store)
    with pytest.raises(ValueError, match='ordered|population changed'):
        asyncio.run(admin.serve('export'))
    assert errors.buffer.getvalue() == b''
    assert output.buffer.getvalue().count(b'\n') == 1
    assert closed == [True]


def test_server_broken_output_pipe_closes_database_without_success_manifest(monkeypatch):
    store = PagedExportStore(1)
    output, errors, closed = wire_export_server(monkeypatch, store)
    class ClosedPipe(io.BytesIO):
        def write(self, value):
            raise BrokenPipeError('external reader disconnected')
    output.buffer = ClosedPipe()
    with pytest.raises(BrokenPipeError):
        asyncio.run(admin.serve('export'))
    assert errors.buffer.getvalue() == b''
    assert closed == [True]


@pytest.mark.parametrize('limit', [0, 101, True, -1])
def test_invalid_direct_expiry_limit_never_opens_database(monkeypatch, limit):
    import asyncpg
    monkeypatch.setattr(asyncpg, 'create_pool', lambda *a, **kw: pytest.fail('must not allocate a pool'))
    with pytest.raises(ValueError, match='Expiry limit'):
        asyncio.run(admin.serve('expire', limit=limit))


def test_cli_passes_explicit_download_deadline(monkeypatch, tmp_path, capsys):
    seen = []
    monkeypatch.setattr(admin.sys, 'argv', ['ghost-admin', 'download', '--ssh', 'root@example.test',
                                           '--output', str(tmp_path / 'file'), '--timeout-seconds', '7200'])
    monkeypatch.setattr(admin, 'download', lambda *args, **kwargs: seen.append((args, kwargs)) or {'row_count': 0})
    admin.main()
    assert seen == [(('root@example.test', tmp_path / 'file'), {'timeout_seconds': 7200})]
    assert json.loads(capsys.readouterr().out) == {'row_count': 0}


@pytest.mark.parametrize('arguments', [
    ['expire', '--limit', '0'], ['expire', '--limit', '101'],
    ['download', '--ssh', 'host', '--output', 'file', '--timeout-seconds', '0'],
    ['download', '--ssh', 'host', '--output', 'file', '--timeout-seconds', '86401'],
    ['download', '--ssh', 'host', '--output', 'file', '--timeout-seconds', 'nan'],
])
def test_invalid_cli_limits_are_rejected_before_any_action(monkeypatch, arguments):
    monkeypatch.setattr(admin.sys, 'argv', ['ghost-admin'] + arguments)
    with pytest.raises(SystemExit) as failure:
        admin.main()
    assert failure.value.code == 2


@pytest.mark.parametrize('host', ['', '-oProxyCommand=bad', 'host with space', 'host\nother'])
def test_bad_ssh_destination_is_rejected_before_writes(tmp_path, host):
    output = tmp_path / 'new-directory' / 'file'
    with pytest.raises(ValueError, match='SSH destination'):
        admin.download(host, output)
    assert not output.parent.exists()
