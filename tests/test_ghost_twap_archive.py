"""Archive transport failures; no network, production credentials or database."""
import asyncio
from copy import deepcopy
from decimal import Decimal
import gzip
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from price_collector import ghost_twap_archive as archive
from price_collector.ghost_twap_store import encode_export_row, validate_record


def record(decision_id='d1', *, created_ms=1000000, terminal=True):
    price = '61234.123456789012345678'
    frozen = dict(run_id='run', decision_id=decision_id, decision_wall_ns='1000000000000',
                  price=price, note='Quotes " and newline\n and Unicode \u03bb',
                  forecasts=[dict(horizon_s=5, target_source_timestamp_ms=1005000)])
    state = dict(publication=dict(status='acknowledged', ack_wall_ns='1000000000100'),
                 targets={'5': dict(target_source_timestamp_ms=1005000, status='matched',
                    first_event=dict(value=price, event_id='official-first'))})
    return validate_record(dict(run_id='run', decision_id=decision_id,
        decision_wall_ns=1000000000000, created_ms=created_ms,
        frozen_json=json.dumps(frozen, ensure_ascii=False, indent=1),
        state_json=json.dumps(state, indent=2), version=0, terminal=terminal))


class Store:
    def __init__(self, rows):
        self.rows = {(r['run_id'], r['decision_id']):deepcopy(r) for r in rows}
        self.verified = {}
        self.selections = 0
        self.ack_calls = []
        self.before_ack = None
        self.fail_after_ack = False
        self.ack_result = None

    async def archive_candidates(self):
        self.selections += 1
        return deepcopy(sorted((r for k,r in self.rows.items()
            if r['terminal'] and k not in self.verified),
            key=lambda r:(r['created_ms'],r['run_id'],r['decision_id']))[:100])

    def advance(self, identity):
        row = deepcopy(self.rows[identity])
        state = json.loads(row['state_json'])
        state['late_evidence'] = 'preserved with a later version'
        row.update(version=row['version']+1, state_json=json.dumps(state))
        self.rows[identity] = validate_record(row)
        self.verified.pop(identity,None)

    async def mark_verified_export(self, proofs, **metadata):
        self.ack_calls.append((deepcopy(proofs),deepcopy(metadata)))
        if self.before_ack is not None:
            callback, self.before_ack = self.before_ack, None
            callback()
        matched = []
        for proof in proofs:
            key = proof['run_id'],proof['decision_id']
            row = self.rows.get(key)
            if row is not None and row['terminal'] and all(row[k] == proof[k]
                    for k in ('version','frozen_sha256','state_sha256')):
                self.verified[key] = deepcopy(metadata)
                matched.append(key)
        if self.fail_after_ack:
            self.fail_after_ack = False
            raise OSError('acknowledgement committed but response lost')
        if self.ack_result is not None:
            return self.ack_result
        return dict(verified=matched,stale_or_ineligible=len(proofs)-len(matched))

    async def expire_verified(self, **_kwargs):
        raise AssertionError('archival must not expire evidence')

    async def persist(self, *_args, **_kwargs):
        raise AssertionError('archival must not alter frozen or target evidence')


class Backend:
    """Filesystem is only a test double, never claimed to be an external backup."""
    def __init__(self, directory):
        directory.mkdir()
        self.directory = directory
        self.puts, self.reads = [], []
        self.fail_after_upload = False
        self.read_transform = None
        self.chunks_override = None
        self.block_read = False
        self.read_entered = None
        self.read_settled = False

    def path(self, key):
        return self.directory/(sha256(key.encode()).hexdigest()+'.object')

    def location(self, key):
        return 'fake-external://independent-test-store/'+key

    async def put_if_absent(self, key, source):
        self.puts.append(key)
        try:
            with self.path(key).open('xb') as target:
                target.write(source.read_bytes())
        except FileExistsError:
            pass
        if self.fail_after_upload:
            self.fail_after_upload = False
            raise OSError('upload committed but response lost')

    async def read(self, key):
        self.reads.append(key)
        if self.read_entered is not None:
            self.read_entered.set()
        try:
            if self.block_read:
                await asyncio.Future()
            content = self.path(key).read_bytes()
            if self.read_transform:
                content = self.read_transform(content)
            if self.chunks_override is not None:
                for chunk in self.chunks_override:
                    yield chunk
            else:
                for position in range(0,len(content),17):
                    yield content[position:position+17]
        finally:
            self.read_settled = True

    async def delete(self, *_args):
        raise AssertionError('archival must not delete remote evidence')


@pytest.fixture
def setup(tmp_path):
    stage = tmp_path/'stage'
    stage.mkdir()
    return stage,Backend(tmp_path/'remote')


def test_roundtrip_preserves_exact_decimal_and_original_text_sorts_and_never_deletes(setup):
    stage, backend = setup
    original = [record('z',created_ms=1),record('a',created_ms=2)]
    store = Store(original)
    result = asyncio.run(archive.archive_once(store,backend,stage))
    assert result['status'] == 'archived'
    assert (result['selected'],result['verified'],result['stale_or_ineligible']) == (2,2,0)
    packed = backend.path(backend.puts[0]).read_bytes()
    content = gzip.decompress(packed)
    expected = b''.join(encode_export_row(r) for r in sorted(original,key=lambda r:r['decision_id']))
    assert content == expected
    assert result['sha256'] == sha256(expected).hexdigest()
    assert result['compressed_sha256'] == sha256(packed).hexdigest()
    assert packed[4:8] == b'\x00\x00\x00\x00'  # No run-time gzip timestamp.
    decoded = [json.loads(line) for line in content.splitlines()]
    assert decoded[0]['frozen_json'] == original[1]['frozen_json']
    assert decoded[0]['state_json'] == original[1]['state_json']
    assert Decimal(json.loads(decoded[0]['frozen_json'])['price']) == Decimal('61234.123456789012345678')
    assert store.rows == {(r['run_id'],r['decision_id']):r for r in original}
    assert len(store.ack_calls) == 1 and len(backend.reads) == 1
    assert list(stage.iterdir()) == []


def test_idle_selection_never_touches_transport_and_nonterminal_is_not_exported(setup):
    stage, backend = setup
    result = asyncio.run(archive.archive_once(Store([record(terminal=False)]),backend,stage))
    assert result == dict(status='idle',selected=0,verified=0,stale_or_ineligible=0)
    assert backend.puts == backend.reads == []


def test_bad_chunk_closes_remote_reader_before_returning_to_long_lived_worker(setup):
    stage, backend = setup
    backend.chunks_override = [b'']
    store = Store([record()])

    async def scenario():
        with pytest.raises(ValueError, match='Invalid archive readback chunk'):
            await archive.archive_once(store, backend, stage)
        # Check before asyncio.run shuts down async generators for us.
        assert backend.read_settled
        assert not store.ack_calls

    asyncio.run(scenario())


@pytest.mark.parametrize('fault',[
    lambda b:b[:-1],lambda b:bytes([b[0]^1])+b[1:],lambda b:b+b'x',lambda b:b'',
])
def test_corrupt_truncated_or_extra_readback_never_acknowledges(setup,fault):
    stage, backend = setup
    store = Store([record()])
    backend.read_transform = fault
    with pytest.raises(ValueError,match='readback'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert not store.ack_calls and not store.verified
    assert len(list(backend.directory.iterdir())) == 1
    assert list(stage.iterdir()) == []


@pytest.mark.parametrize('chunk',[b'',bytearray(b'x'),'text',b'x'*(archive.CHUNK_BYTES+1)],
                         ids=['empty','mutable-bytes','text','oversized'])
def test_invalid_readback_chunks_are_bounded_and_never_acknowledged(setup,chunk):
    stage, backend = setup
    store = Store([record()])
    backend.chunks_override = [chunk]
    with pytest.raises(ValueError,match='chunk'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert not store.ack_calls and list(stage.iterdir()) == []


def test_upload_committed_then_failed_retries_same_key_and_reads_remote_before_ack(setup):
    stage, backend = setup
    store = Store([record()])
    backend.fail_after_upload = True
    with pytest.raises(OSError,match='upload committed'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert not store.ack_calls and not backend.reads
    assert list(stage.iterdir()) == []
    first_bytes = backend.path(backend.puts[0]).read_bytes()
    result = asyncio.run(archive.archive_once(store,backend,stage))
    assert result['verified'] == 1 and backend.puts[0] == backend.puts[1]
    assert backend.path(backend.puts[0]).read_bytes() == first_bytes
    assert backend.reads == [backend.puts[0]]
    assert len(list(backend.directory.iterdir())) == 1


def test_existing_wrong_object_is_not_replaced_or_acknowledged(setup):
    stage, backend = setup
    store = Store([record()])
    backend.fail_after_upload = True
    with pytest.raises(OSError):
        asyncio.run(archive.archive_once(store,backend,stage))
    key = backend.puts[0]
    backend.path(key).write_bytes(b'pre-existing corrupt object')
    with pytest.raises(ValueError,match='readback'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert backend.path(key).read_bytes() == b'pre-existing corrupt object'
    assert not store.ack_calls


def test_partial_cas_race_reports_stale_then_rearchives_only_new_version(setup):
    stage, backend = setup
    store = Store([record('a'),record('b')])
    store.before_ack = lambda:store.advance(('run','a'))
    first = asyncio.run(archive.archive_once(store,backend,stage))
    assert (first['selected'],first['verified'],first['stale_or_ineligible']) == (2,1,1)
    assert ('run','a') not in store.verified
    second = asyncio.run(archive.archive_once(store,backend,stage))
    assert (second['selected'],second['verified'],second['stale_or_ineligible']) == (1,1,0)
    assert first['sha256'] != second['sha256']
    assert len(list(backend.directory.iterdir())) == 2
    assert store.ack_calls[-1][0][0]['version'] == 1


def test_update_after_success_invalidates_and_revisits_old_identity(setup):
    stage, backend = setup
    store = Store([record('a'),record('z')])
    asyncio.run(archive.archive_once(store,backend,stage))
    store.advance(('run','a'))
    result = asyncio.run(archive.archive_once(store,backend,stage))
    assert result['selected'] == result['verified'] == 1
    assert store.ack_calls[-1][0][0]['decision_id'] == 'a'
    assert store.ack_calls[-1][0][0]['version'] == 1


def test_uncertain_ack_commit_retry_is_idle_and_preserves_remote_object(setup):
    stage, backend = setup
    store = Store([record()])
    store.fail_after_ack = True
    with pytest.raises(OSError,match='acknowledgement committed'):
        asyncio.run(archive.archive_once(store,backend,stage))
    result = asyncio.run(archive.archive_once(store,backend,stage))
    assert result['status'] == 'idle' and len(backend.puts) == 1
    assert len(store.verified) == len(list(backend.directory.iterdir())) == 1


def test_failure_after_verified_readback_before_cas_rechecks_remote_on_retry(setup):
    stage, backend = setup
    store = Store([record()])
    def interrupted():
        raise OSError('interrupted before database acknowledgement')
    store.before_ack = interrupted
    with pytest.raises(OSError,match='before database'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert not store.verified and len(backend.reads) == 1
    result = asyncio.run(archive.archive_once(store,backend,stage))
    assert result['verified'] == 1
    assert backend.reads == [backend.puts[0],backend.puts[0]]
    assert len(list(backend.directory.iterdir())) == 1


@pytest.mark.parametrize('result',[
    dict(verified=[('run','d1'),('run','d1')],stale_or_ineligible=0),
    dict(verified=[('other','foreign')],stale_or_ineligible=0),
    dict(verified=[],stale_or_ineligible=True),
    dict(verified=[],stale_or_ineligible=-1),
    dict(verified=[],stale_or_ineligible=0),
])
def test_invalid_cas_summary_cannot_be_reported_as_archive_success(setup,result):
    stage, backend = setup
    store = Store([record()])
    store.ack_result = result
    with pytest.raises(ValueError,match='acknowledgement result'):
        asyncio.run(archive.archive_once(store,backend,stage))


def test_encoded_byte_budget_exports_prefix_and_next_cycle_keeps_remainder(setup,monkeypatch):
    stage, backend = setup
    rows = [record('a'),record('b'),record('c')]
    monkeypatch.setattr(archive,'MAX_RAW_BYTES',len(encode_export_row(rows[0])))
    store = Store(rows)
    results = [asyncio.run(archive.archive_once(store,backend,stage)) for _ in rows]
    assert all(r['selected'] == r['verified'] == 1 for r in results)
    assert len(store.verified) == 3
    assert len(list(backend.directory.iterdir())) == 3


@pytest.mark.parametrize('setting',['MAX_RAW_BYTES','MAX_COMPRESSED_BYTES'])
def test_single_record_or_compressed_budget_failure_does_not_upload(setup,monkeypatch,setting):
    stage, backend = setup
    monkeypatch.setattr(archive,setting,1)
    store = Store([record()])
    with pytest.raises(ValueError,match='byte budget'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert not backend.puts and not store.ack_calls and list(stage.iterdir()) == []


def test_disk_reserve_fails_before_select_or_upload(setup,monkeypatch):
    stage, backend = setup
    monkeypatch.setattr(archive.shutil,'disk_usage',lambda _:SimpleNamespace(free=0))
    store = Store([record()])
    with pytest.raises(ValueError,match='disk reserve'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert store.selections == 0 and not backend.puts


def test_leftover_staging_is_preserved_and_blocks_more_admission(setup):
    stage, backend = setup
    abandoned = stage/'ghost-archive-killed-process'
    abandoned.mkdir()
    marker = abandoned/'upload.jsonl.gz'
    marker.write_bytes(b'preserve interrupted evidence')
    store = Store([record()])
    with pytest.raises(ValueError,match='(?i)(stale|leftover|unfinished|existing|staging)'):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert marker.read_bytes() == b'preserve interrupted evidence'
    assert store.selections == 0 and not backend.puts


def test_entire_operation_timeout_cancels_read_and_cleans_only_own_stage(setup,monkeypatch):
    stage, backend = setup
    monkeypatch.setattr(archive,'ARCHIVE_TIMEOUT_SECONDS',0.01)
    store = Store([record()])
    backend.block_read = True
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(archive.archive_once(store,backend,stage))
    assert backend.read_settled and not store.ack_calls
    assert list(stage.iterdir()) == [] and len(list(backend.directory.iterdir())) == 1


def test_external_cancellation_settles_read_before_staging_cleanup(setup):
    stage, backend = setup
    store = Store([record()])
    backend.block_read = True
    async def scenario():
        backend.read_entered = asyncio.Event()
        task = asyncio.create_task(archive.archive_once(store,backend,stage))
        await asyncio.wait_for(backend.read_entered.wait(),1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(scenario())
    assert backend.read_settled and not store.ack_calls and list(stage.iterdir()) == []


def test_readback_decompression_cap_precedes_proof_emission(tmp_path):
    backend = Backend(tmp_path/'remote')
    packed = gzip.compress(b'x'*1000,mtime=0)
    key = 'test-decompression-bound'
    backend.path(key).write_bytes(packed)
    work = tmp_path/'readback'
    work.mkdir()
    manifest = dict(compressed_bytes=len(packed),compressed_sha256=sha256(packed).hexdigest(),
                    raw_bytes=100,sha256='0'*64,row_count=1)
    with pytest.raises(ValueError,match='decompression exceeds'):
        asyncio.run(archive._readback(backend,key,work,manifest))


@pytest.mark.parametrize('fault',['row-hash','row-count','identity-duplicate'])
def test_valid_compression_and_whole_file_hash_are_not_enough_for_row_proofs(tmp_path,fault):
    backend = Backend(tmp_path/'remote')
    first = encode_export_row(record('a'))
    second = encode_export_row(record('b'))
    rows = 2
    if fault == 'row-hash':
        # Still valid canonical outer JSON, but its exact frozen text no longer
        # agrees with the claimed frozen hash.
        second = second.replace(b'61234.123456789012345678',b'61234.123456789012345679',1)
    elif fault == 'row-count':
        rows = 3
    else:
        second = first
    raw = first+second
    packed = gzip.compress(raw,mtime=0)
    key = 'test-complete-row-verification'
    backend.path(key).write_bytes(packed)
    work = tmp_path/'readback'
    work.mkdir()
    manifest = dict(compressed_bytes=len(packed),compressed_sha256=sha256(packed).hexdigest(),
                    raw_bytes=len(raw),sha256=sha256(raw).hexdigest(),row_count=rows)
    with pytest.raises(ValueError):
        asyncio.run(archive._readback(backend,key,work,manifest))
