"""Opt-in real Redis protocol/started-worker checks, never an existing endpoint.

GHOST_TEST_REDIS=1 requires a local redis-server executable on POSIX. Each test
launches its own bounded server with TCP and persistence disabled, using only a
private Unix socket. PostgreSQL is an in-memory double here; the existing opt-in
PostgreSQL suite separately covers actual database behavior.
"""
import asyncio
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

import pytest
import redis.asyncio as redis_async

from price_collector.ghost_twap import NS_PER_MS, NS_PER_SECOND
from price_collector.ghost_twap_runtime import (
    GHOST_CHANNEL, GHOST_KEY, PUBLISH_LUA, RESERVE_BYTES, GhostRuntime, GhostSettings,
)
from price_collector.ghost_twap_spool import GhostSpool


pytestmark = pytest.mark.skipif(os.environ.get('GHOST_TEST_REDIS') != '1',
                                reason='requires explicit GHOST_TEST_REDIS=1 private-server opt-in')


@pytest.fixture
def private_redis():
    if os.name != 'posix':
        pytest.skip('private Unix-socket Redis integration requires POSIX')
    executable = shutil.which('redis-server')
    if executable is None:
        pytest.fail('GHOST_TEST_REDIS=1 requires redis-server; no existing server will be used')
    with tempfile.TemporaryDirectory(prefix='ghost-redis-', dir='/tmp') as directory:
        root = Path(directory)
        socket = root / 'redis.sock'
        assert len(os.fsencode(socket)) < 100
        log = root / 'redis.log'
        command = [executable, '--port', '0', '--bind', '127.0.0.1',
                   '--unixsocket', str(socket), '--unixsocketperm', '700',
                   '--protected-mode', 'yes', '--save', '', '--appendonly', 'no',
                   '--daemonize', 'no', '--dir', str(root), '--logfile', str(log),
                   '--maxmemory', '16mb', '--maxmemory-policy', 'noeviction']
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
        try:
            yield socket, process, log, root
        finally:
            # Terminate only the child we started. No Redis shutdown/flush
            # command and no TCP connection can reach a production instance.
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)


async def connect_private(server):
    socket, process, log, _ = server
    client = redis_async.Redis(unix_socket_path=str(socket), decode_responses=False,
                               socket_timeout=1, socket_connect_timeout=1)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            detail = log.read_text(errors='replace')[-2000:] if log.exists() else 'no server log'
            await client.aclose()
            pytest.fail('private Redis startup failed: ' + detail)
        try:
            if await client.ping():
                return client
        except (ConnectionError, OSError, redis_async.ConnectionError):
            pass
        await asyncio.sleep(0.01)
    await client.aclose()
    raise TimeoutError('private Redis did not start within five seconds')


async def subscription(client):
    subscriber = client.pubsub()
    await subscriber.subscribe(GHOST_CHANNEL)
    acknowledgement = await subscriber.get_message(ignore_subscribe_messages=False, timeout=1)
    assert acknowledgement and acknowledgement['type'] == 'subscribe'
    return subscriber


async def eventually(predicate, *, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.01)
    raise TimeoutError('started worker did not reach the expected state')


def test_real_redis_lua_sets_and_publishes_identical_bytes_then_expires(private_redis):
    async def scenario():
        client = await connect_private(private_redis)
        subscriber = await subscription(client)
        try:
            body = b'{"price":"61234.123456789012345678","state":"test"}'
            assert await client.eval(PUBLISH_LUA, 2, GHOST_KEY, GHOST_CHANNEL, body, 120) == 1
            message = await subscriber.get_message(ignore_subscribe_messages=True, timeout=1)
            assert message and message['data'] == body
            assert await client.get(GHOST_KEY) == body
            assert 0 < await client.pttl(GHOST_KEY) <= 120
            deadline = time.monotonic() + 1
            while await client.exists(GHOST_KEY):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.01)
            assert await client.pttl(GHOST_KEY) == -2
        finally:
            await subscriber.aclose()
            await client.aclose()
    asyncio.run(scenario())


class MemoryAudit:
    """No PostgreSQL connection: retain evidence from the four real workers."""
    def __init__(self):
        self.rows = {}
        self.measurements = 0
        self.late_events = []

    async def initialize(self):
        return {'row_count': len(self.rows), 'incomplete_count':
                sum(not row['terminal'] for row in self.rows.values())}

    async def list_incomplete(self, *, after=None, limit=100):
        keys = [key for key in sorted(self.rows) if after is None or key > after]
        return [deepcopy(self.rows[key]) for key in keys if not self.rows[key]['terminal']][:limit]

    async def get_record(self, run_id, decision_id):
        return deepcopy(self.rows.get((run_id, decision_id)))

    async def persist(self, record):
        key = record['run_id'], record['decision_id']
        existing = self.rows.get(key)
        if existing:
            assert existing['frozen_json'] == record['frozen_json']
            if existing['version'] > record['version']:
                return {'version': existing['version'], 'outcome': 'stale'}
            if existing['version'] == record['version']:
                assert existing == record
                return {'version': existing['version'], 'outcome': 'unchanged'}
        self.rows[key] = deepcopy(record)
        return {'version': record['version'], 'outcome': 'updated' if existing else 'inserted'}

    async def measure(self):
        self.measurements += 1
        return {'relation_bytes': 0, 'row_count': len(self.rows), 'tablespaces': ['pg_default'],
                'db_data_directory': None}

    async def note_late_target(self, event, *, after=None, limit=100, exclude=()):
        self.late_events.append(deepcopy(event))
        return {'next_after': None}


def test_started_workers_publish_real_redis_and_persist_matching_evidence(private_redis):
    async def scenario():
        client = await connect_private(private_redis)
        subscriber = await subscription(client)
        directory = private_redis[3] / 'audit'
        now_ms = time.time_ns() // NS_PER_MS
        settings = GhostSettings(enabled=True, canary_start_ms=now_ms - 100000,
                                 state_directory=directory)
        store = MemoryAudit()
        spool = GhostSpool(directory, settings.audit_max_records, settings.record_max_bytes)
        runtime = GhostRuntime(settings, store, client, spool, disk_free=lambda: 2 * RESERVE_BYTES)
        try:
            await runtime.start()
            assert len(runtime._tasks) == 4 and all(not task.done() for task in runtime._tasks)
            wall, mono = time.time_ns(), time.monotonic_ns()
            anchor = (wall // NS_PER_SECOND - 1) * 1000
            # Offer the whole historical prefix synchronously, just as queued
            # accepted events reach the no-await admission barrier.
            for second in range(-62, 1):
                source = anchor + second * 1000
                received = (source + 300) * NS_PER_MS
                runtime.offer_price('spot', Decimal('100.000000000000000001'), source,
                                    received, mono + received - wall, f'spot-{second}')
            received = (anchor + 300) * NS_PER_MS + 1
            runtime.offer_price('twap', Decimal('100'), anchor, received,
                                mono + received - wall, 'anchor', 60)
            message = await subscriber.get_message(ignore_subscribe_messages=True, timeout=2)
            assert message and message['type'] == 'message'
            body = message['data']
            live = json.loads(body)
            assert live['publication_state'] == 'attempted'
            assert all(forecast['price'] is not None for forecast in live['forecasts'])
            assert live['current_spot']['value'] == '100.000000000000000001'
            assert await client.get(GHOST_KEY) == body
            row = runtime.records[live['decision_id']]
            await eventually(lambda: row.state['publication']['status'] == 'acknowledged')
            path = spool._path(row.record())
            assert path.exists()
            assert json.loads(path.read_bytes())['frozen_json'] == row.frozen_json
            assert 0 < await client.pttl(GHOST_KEY) <= 3000
            receipt_wall, receipt_mono = time.time_ns(), time.monotonic_ns()
            runtime.offer_price('twap', Decimal('101'), anchor + 1000,
                                receipt_wall, receipt_mono, 'first-target', 60)
            result = row.state['targets']['1']
            assert result['first_event']['event_id'] == 'first-target'
            assert result['confirmed_redis_lead_ns'] > 0
            identity = row.decision.run_id, row.decision.decision_id
            await eventually(lambda: identity in store.rows and
                             json.loads(store.rows[identity]['state_json'])['targets']['1']['status'] == 'matched')
            assert runtime.stop_reason is None
        finally:
            await asyncio.wait_for(runtime.close(), timeout=8)
            await subscriber.aclose()
            await client.aclose()
        assert all(task.done() for task in runtime._tasks)
        assert spool._lock is None
        assert all(row['terminal'] for row in store.rows.values())
    asyncio.run(scenario())
