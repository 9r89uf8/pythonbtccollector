"""Explicit ghost audit status, external export/verification, and bounded expiry.

No collector calls this CLI. ``download`` runs on the owner's computer over SSH;
only after verifying that external file does it acknowledge unchanged row hashes.
The other commands run on the droplet as postgres through its local Unix socket.
"""
from __future__ import annotations

import argparse
import asyncio
from hashlib import sha256
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

from price_collector.ghost_twap_store import (
    GhostAuditStore, encode_export_row, iter_export_proofs, verify_export_file,
)


DEFAULT_TRANSFER_TIMEOUT_SECONDS = 3600
MAX_TRANSFER_TIMEOUT_SECONDS = 86_400
MAX_HEADER_BYTES = 8192
MAX_PROOF_BYTES = 4096
MAX_CONTROL_OUTPUT_BYTES = 65_536


def _bounded_json_line(stream, maximum: int, label: str, *, eof_allowed: bool = False):
    raw = stream.readline(maximum + 1)
    if not raw and eof_allowed:
        return None
    if not raw or len(raw) > maximum or not raw.endswith(b'\n'):
        raise ValueError(f'Truncated or oversized {label}')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f'{label} must be an object')
    return value


async def serve(command: str, *, limit: int = 100) -> None:
    if command not in ('status', 'export', 'acknowledge', 'expire'):
        raise ValueError('Unknown ghost audit command')
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('Expiry limit must be between 1 and 100')
    import asyncpg
    dsn = os.environ.get('GHOST_AUDIT_DSN', 'postgresql:///price_collector?host=/var/run/postgresql')
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1, command_timeout=5, timeout=5)
    store = GhostAuditStore(pool)
    try:
        if command == 'status':
            print(json.dumps(dict(await store.initialize(), measurement=await store.measure(include_count=True))))
        elif command == 'export':
            initial = await store.initialize()
            if initial['incomplete_count']:
                raise ValueError('Stop ghost and reconcile all decisions before final export')
            cursor, previous, count, digest = None, None, 0, sha256()
            while True:
                rows = await store.export_page(after=cursor)
                if not rows:
                    break
                for row in rows:
                    if not row['terminal']:
                        raise ValueError('Ghost admissions resumed during export')
                    identity = (row['run_id'], row['decision_id'])
                    if previous is not None and identity <= previous:
                        raise ValueError('Audit export identities must be unique and strictly ordered')
                    if count >= initial['row_count']:
                        raise ValueError('Audit row population changed during export; repeat it')
                    raw = encode_export_row(row)
                    sys.stdout.buffer.write(raw)
                    digest.update(raw)
                    count += 1
                    previous = identity
                sys.stdout.buffer.flush()
                cursor = (rows[-1]['run_id'], rows[-1]['decision_id'])
            final = await store.initialize()
            if count != initial['row_count'] or count != final['row_count'] or final['incomplete_count']:
                raise ValueError('Audit row population changed during export; repeat it')
            print(json.dumps({'sha256': digest.hexdigest(), 'row_count': count}), file=sys.stderr)
        elif command == 'acknowledge':
            # Each request is bounded; a remote caller must supply already-verified proofs.
            header = _bounded_json_line(sys.stdin.buffer, MAX_HEADER_BYTES, 'export proof header')
            if set(header) != {'export_sha256', 'external_location'}:
                raise ValueError('Unexpected export proof header fields')
            if (not isinstance(header['export_sha256'], str)
                    or len(header['export_sha256']) != 64
                    or any(c not in '0123456789abcdef' for c in header['export_sha256'])
                    or not isinstance(header['external_location'], str)
                    or not 0 < len(header['external_location']) <= 2048):
                raise ValueError('Invalid export proof header')
            batch, verified, stale = [], 0, 0
            while True:
                proof = _bounded_json_line(sys.stdin.buffer, MAX_PROOF_BYTES, 'export proof', eof_allowed=True)
                if proof is None:
                    break
                if set(proof) != {'run_id', 'decision_id', 'version', 'frozen_sha256', 'state_sha256'}:
                    raise ValueError('Unexpected export proof fields')
                batch.append(proof)
                if len(batch) == 100:
                    result = await store.mark_verified_export(batch, **header)
                    verified += len(result['verified'])
                    stale += result['stale_or_ineligible']
                    batch.clear()
            if batch:
                result = await store.mark_verified_export(batch, **header)
                verified += len(result['verified'])
                stale += result['stale_or_ineligible']
            print(json.dumps({'verified': verified, 'stale_or_ineligible': stale}))
            if stale:
                raise ValueError('Some states changed; re-export before expiry')
        elif command == 'expire':
            print(json.dumps({'expired': await store.expire_verified(limit=limit)}))
    finally:
        try:
            await asyncio.wait_for(pool.close(), timeout=5)
        except asyncio.TimeoutError:
            pool.terminate()
            raise


def _ssh(host: str, command: str) -> list[str]:
    if not host or host.startswith('-') or any(c.isspace() for c in host):
        raise ValueError('Invalid SSH destination')
    # The remote shell command is constant; user input is a separate local argv.
    remote = ('sudo -u postgres env PYTHONPATH=/opt/price-collector '
              '/opt/price-collector/.venv/bin/python -m price_collector.ghost_twap_admin ' + command)
    return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', host, remote]


def _remaining_seconds(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('The total download deadline expired')
    return remaining


def _control_output(handle) -> bytes:
    handle.seek(0, os.SEEK_END)
    if handle.tell() > MAX_CONTROL_OUTPUT_BYTES:
        raise ValueError('Oversized SSH control output')
    handle.seek(0)
    return handle.read(MAX_CONTROL_OUTPUT_BYTES + 1)


def _run_ssh(host: str, command: str, *, stdin, stdout, stderr, deadline: float):
    # File descriptors, not parent-fed pipes: remote failure or verbose stderr
    # cannot deadlock proof input against blocked stdout/stderr reads.
    return subprocess.run(
        _ssh(host, command), stdin=stdin, stdout=stdout, stderr=stderr,
        timeout=_remaining_seconds(deadline),
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0) if os.name == 'nt' else 0,
    )


def _publish_without_overwrite(partial: Path, final: Path) -> None:
    # Both names share a directory/filesystem. Hard-link creation atomically
    # publishes verified bytes and fails if any final path already exists.
    # Unlike POSIX rename(), it cannot overwrite a file created during download.
    os.link(partial, final)
    partial.unlink()


def download(host: str, output: Path, *, timeout_seconds: int = DEFAULT_TRANSFER_TIMEOUT_SECONDS) -> dict:
    _ssh(host, 'export')  # Reject invalid destinations before filesystem writes.
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_TRANSFER_TIMEOUT_SECONDS:
        raise ValueError('Transfer timeout must be between 1 and 86400 seconds')
    output = Path(output)
    partial = output.with_name(output.name + '.part')
    manifest_path = output.with_name(output.name + '.manifest.json')
    manifest_partial = manifest_path.with_name(manifest_path.name + '.part')
    if any(os.path.lexists(path) for path in (output, partial, manifest_path, manifest_partial)):
        raise ValueError('Refusing to overwrite an existing export, partial file, or manifest')
    deadline = time.monotonic() + timeout_seconds
    output.parent.mkdir(parents=True, exist_ok=True)
    # An unsuccessful transfer or verification leaves only the explicit .part.
    with partial.open('xb') as handle, tempfile.TemporaryFile(mode='w+b') as errors:
        try:
            result = _run_ssh(host, 'export', stdin=subprocess.DEVNULL, stdout=handle,
                              stderr=errors, deadline=deadline)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f'Export deadline expired; partial file retained at {partial}') from exc
        finally:
            handle.flush()
            os.fsync(handle.fileno())
        stderr = _control_output(errors)
    if result.returncode:
        raise RuntimeError(f'Export failed; partial file retained at {partial}: ' + stderr.decode(errors='replace')[-2000:])
    lines = stderr.decode('utf-8').strip().splitlines()
    if not lines:
        raise ValueError('Export completed without its stream manifest; partial file retained')
    manifest = json.loads(lines[-1])
    if not isinstance(manifest, dict) or set(manifest) != {'sha256', 'row_count'}:
        raise ValueError('Invalid export stream manifest; partial file retained')
    verification = verify_export_file(partial, expected_sha256=manifest['sha256'],
                                      expected_rows=manifest['row_count'])
    verification['external_path'] = str(output.resolve())
    with manifest_partial.open('x', encoding='utf-8') as handle:
        json.dump(verification, handle, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    _publish_without_overwrite(partial, output)
    _publish_without_overwrite(manifest_partial, manifest_path)
    # Verify once more while building proofs into a temporary file. No SSH
    # acknowledgement process starts until the full proof pass succeeds.
    with tempfile.TemporaryFile(mode='w+b') as proof_file, \
            tempfile.TemporaryFile(mode='w+b') as response, \
            tempfile.TemporaryFile(mode='w+b') as errors:
        header = {'export_sha256': manifest['sha256'],
                  'external_location': socket.gethostname() + ':' + str(output.resolve())}
        proof_file.write((json.dumps(header) + '\n').encode())
        for proof in iter_export_proofs(output, expected_sha256=manifest['sha256'],
                                        expected_rows=manifest['row_count']):
            proof_file.write((json.dumps(proof, separators=(',', ':')) + '\n').encode())
        proof_file.seek(0)
        try:
            result = _run_ssh(host, 'acknowledge', stdin=proof_file, stdout=response,
                              stderr=errors, deadline=deadline)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError('Export is verified, but the acknowledgement deadline expired') from exc
        stdout, stderr = _control_output(response), _control_output(errors)
    if result.returncode:
        raise RuntimeError('Export is verified, but acknowledgement failed: ' + stderr.decode()[-2000:])
    acknowledgement = json.loads(stdout)
    if (not isinstance(acknowledgement, dict)
            or set(acknowledgement) != {'verified', 'stale_or_ineligible'}
            or type(acknowledgement['verified']) is not int
            or acknowledgement['verified'] != manifest['row_count']
            or type(acknowledgement['stale_or_ineligible']) is not int
            or acknowledgement['stale_or_ineligible'] != 0):
        raise ValueError('Export is verified, but acknowledgement counts do not match; re-export before expiry')
    return dict(verification, acknowledgement=acknowledgement)


def _timeout_argument(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('timeout must be an integer number of seconds') from exc
    if not 1 <= seconds <= MAX_TRANSFER_TIMEOUT_SECONDS:
        raise argparse.ArgumentTypeError('timeout must be between 1 and 86400 seconds')
    return seconds


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('status', 'export', 'acknowledge'):
        sub.add_parser(name)
    expiry = sub.add_parser('expire')
    expiry.add_argument('--limit', type=int, default=100, choices=range(1, 101))
    fetch = sub.add_parser('download')
    fetch.add_argument('--ssh', required=True)
    fetch.add_argument('--output', required=True, type=Path)
    fetch.add_argument('--timeout-seconds', type=_timeout_argument, default=DEFAULT_TRANSFER_TIMEOUT_SECONDS,
                       help='Total export and acknowledgement deadline (1–86400 seconds; default 3600)')
    args = parser.parse_args()
    if args.command == 'download':
        print(json.dumps(download(args.ssh, args.output, timeout_seconds=args.timeout_seconds)))
    else:
        asyncio.run(serve(args.command, limit=getattr(args, 'limit', 100)))


if __name__ == '__main__':
    main()
