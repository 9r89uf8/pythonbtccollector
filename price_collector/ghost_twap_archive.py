"""Bounded archive transaction, separate from collector and API processes.

This module supplies the transport-independent part of an operator worker. It
does not enable a service, select a destination, expire rows or relax retention.
A real backend must provide durable create-only storage on a different host,
verified TLS/host identity, bounded network timeouts and an async chunk reader.
"""
from __future__ import annotations

import asyncio
import gzip
from hashlib import sha256
from pathlib import Path
import shutil
import tempfile
from typing import AsyncGenerator, Protocol

from price_collector.ghost_twap_store import (
    MAX_BATCH, encode_export_row, iter_export_proofs,
)

MAX_RAW_BYTES = 8 * 1024 * 1024
MAX_COMPRESSED_BYTES = MAX_RAW_BYTES + 64 * 1024
CHUNK_BYTES = 64 * 1024
# Three bounded files plus headroom; an archive is never staged on a full disk.
MIN_STAGE_FREE_BYTES = 128 * 1024 * 1024
ARCHIVE_TIMEOUT_SECONDS = 60


class ArchiveBackend(Protocol):
    """Trusted operator adapter; local/collector cache storage is not external.

    put_if_absent must never replace existing bytes. An existing key is success
    only for proceeding to readback (its contents are not trusted until then).
    read must be a fresh remote read, not the upload buffer, an ETag or HEAD.
    Backend cancellation must settle its own I/O before returning. Credentials
    belong only to the operator job, never the collector or API environment.
    """

    async def put_if_absent(self, key: str, source: Path) -> None: ...

    def read(self, key: str) -> AsyncGenerator[bytes, None]: ...

    def location(self, key: str) -> str: ...


def _stage(rows: list, directory: Path) -> dict:
    if not 0 < len(rows) <= MAX_BATCH:
        raise ValueError('Invalid archive candidate count')
    selected, raw_bytes = [], 0
    seen = set()
    for row in rows:
        if row['terminal'] is not True:
            raise ValueError('Archive candidate is not terminal')
        identity = (row['run_id'], row['decision_id'])
        if identity in seen:
            raise ValueError('Duplicate archive candidate')
        seen.add(identity)
        line = encode_export_row(row)
        if raw_bytes + len(line) > MAX_RAW_BYTES:
            break
        selected.append((identity, line))
        raw_bytes += len(line)
    if not selected:
        raise ValueError('One archive record exceeds the byte budget')
    # Candidate priority is oldest creation time; export verification requires
    # strict identity order within each independently verifiable file.
    selected.sort(key=lambda item: item[0])
    digest = sha256()
    packed = directory / 'upload.jsonl.gz'
    with packed.open('xb') as output:
        with gzip.GzipFile(filename='', mode='wb', fileobj=output, mtime=0) as zipped:
            for _, line in selected:
                digest.update(line)
                zipped.write(line)
    compressed_bytes = packed.stat().st_size
    if compressed_bytes > MAX_COMPRESSED_BYTES:
        raise ValueError('Compressed archive exceeds byte budget')
    packed_digest = sha256()
    with packed.open('rb') as source:
        for chunk in iter(lambda: source.read(CHUNK_BYTES), b''):
            packed_digest.update(chunk)
    return dict(sha256=digest.hexdigest(), row_count=len(selected), raw_bytes=raw_bytes,
                compressed_sha256=packed_digest.hexdigest(), compressed_bytes=compressed_bytes)


async def _readback(backend: ArchiveBackend, key: str, directory: Path, manifest: dict) -> list:
    packed, raw = directory / 'readback.jsonl.gz', directory / 'verified.jsonl'
    count, digest = 0, sha256()
    chunks = backend.read(key)
    try:
        with packed.open('xb') as output:
            async for chunk in chunks:
                if not isinstance(chunk, bytes) or not 0 < len(chunk) <= CHUNK_BYTES:
                    raise ValueError('Invalid archive readback chunk')
                count += len(chunk)
                if count > manifest['compressed_bytes']:
                    raise ValueError('Archive readback exceeds expected compressed size')
                output.write(chunk)
                digest.update(chunk)
    finally:
        # A validation error occurs between yields; do not leave a remote body
        # open until async-generator garbage collection or process shutdown.
        await chunks.aclose()
    if count != manifest['compressed_bytes'] or digest.hexdigest() != manifest['compressed_sha256']:
        raise ValueError('Archive compressed readback mismatch')
    count = 0
    with gzip.open(packed, 'rb') as source, raw.open('xb') as output:
        while True:
            chunk = source.read(min(CHUNK_BYTES, manifest['raw_bytes'] - count + 1))
            if not chunk:
                break
            count += len(chunk)
            if count > manifest['raw_bytes']:
                raise ValueError('Archive decompression exceeds expected size')
            output.write(chunk)
    if count != manifest['raw_bytes']:
        raise ValueError('Archive decompressed size mismatch')
    # The verifier checks every canonical record and hash before yielding proof.
    return list(iter_export_proofs(raw, expected_sha256=manifest['sha256'],
                                   expected_rows=manifest['row_count']))


async def _archive_once(store, backend: ArchiveBackend, staging_directory: Path) -> dict:
    """Export one batch, fetch and verify it, then acknowledge unchanged versions.

    No durable local journal is needed: before acknowledgement the DB continues
    to select the row; afterwards its external object is the recovery source.
    A crash after upload may leave an unreferenced object. Retrying never replaces
    it and always reads it back again. Later DB updates invalidate verification
    and become candidates again. Partial CAS success is reported, not concealed.
    """
    directory = Path(staging_directory)
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError('Archive staging directory must be an existing real directory')
    if any(directory.glob('ghost-archive-*')):
        raise ValueError('Archive staging contains an active or interrupted batch; reconcile it first')
    if shutil.disk_usage(directory).free < MIN_STAGE_FREE_BYTES:
        raise ValueError('Insufficient archive staging disk reserve')
    rows = await store.archive_candidates()
    if not rows:
        return dict(status='idle', selected=0, verified=0, stale_or_ineligible=0)
    # Only our temporary directory is removed. Stale directories from SIGKILL
    # are not guessed at/deleted; the disk reserve bounds subsequent admission.
    with tempfile.TemporaryDirectory(prefix='ghost-archive-', dir=directory) as work:
        work = Path(work)
        manifest = _stage(rows, work)
        key = ('ghost-audit-v1/' + manifest['sha256'] + '/'
               + str(manifest['row_count']) + '-' + manifest['compressed_sha256'] + '.jsonl.gz')
        location = backend.location(key)
        if not isinstance(location, str) or not 0 < len(location) <= 2048 or '\x00' in location:
            raise ValueError('Invalid external archive location')
        await backend.put_if_absent(key, work / 'upload.jsonl.gz')
        proofs = await _readback(backend, key, work, manifest)
        result = await store.mark_verified_export(proofs, export_sha256=manifest['sha256'],
                                                  external_location=location)
        # Validate the CAS response without requiring every version to survive
        # upload unchanged. A stale/missing version simply needs another cycle.
        verified = [tuple(identity) for identity in result['verified']]
        expected = {(proof['run_id'], proof['decision_id']) for proof in proofs}
        stale = result['stale_or_ineligible']
        if (len(verified) != len(set(verified)) or not set(verified) <= expected
                or type(stale) is not int or stale < 0
                or len(verified) + stale != len(proofs)):
            raise ValueError('Invalid archive acknowledgement result')
        return dict(status='archived', selected=len(proofs), verified=len(verified),
                    stale_or_ineligible=stale, location=location, **manifest)


async def archive_once(store, backend: ArchiveBackend, staging_directory: Path) -> dict:
    """Run one operator-owned batch with a cooperative 60-second async timeout.

    The future service must serialize calls into its dedicated staging directory.
    Interrupted staging fails closed for operator reconciliation, rather than
    allocating another batch after every restart. Nothing here deletes DB rows.
    Bounded local compression/verification is synchronous; blocking filesystem
    operations and backend cancellation cleanup can exceed the timeout.
    """
    return await asyncio.wait_for(_archive_once(store, backend, staging_directory),
                                  timeout=ARCHIVE_TIMEOUT_SECONDS)
