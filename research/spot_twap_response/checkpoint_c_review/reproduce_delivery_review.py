"""Local transport/configuration proof; never contacts production or a database.

Uses installed redis-py's real TCP connection, RESP parser and PubSub callbacks
against a tiny loopback RESP peer. Only the authoritative cache is an in-memory
fake. Existing test fixtures provide valid Decimal-string wire payloads. The
server-close case sets zero retry backoff solely to remove timing randomness;
the already-disconnected case retains the client's default retry policy.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]

import redis
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from price_collector.ghost_twap_stream import GhostStreamHub, GHOST_CHANNEL
from price_collector.ghost_twap_payload import bind_read_clock, parse_ghost_payload
from test_ghost_twap_payload import READ_WALL_NS, API_MONO_NS, raw_payload, wire_payload


def body(decision=1):
    value = wire_payload()
    value.update(decision_id=str(decision), publication_sequence=decision)
    return raw_payload(value)


def resp(values):
    parts = [f'*{len(values)}\r\n'.encode()]
    for value in values:
        if type(value) is int:
            parts.append(f':{value}\r\n'.encode())
        else:
            parts.extend([f'${len(value)}\r\n'.encode(), value, b'\r\n'])
    return b''.join(parts)


class Peer:
    def __init__(self):
        self.connections = []
        self.subscriptions = []
        self.commands = []
        self.protocols = {}

    async def handle(self, reader, writer):
        self.connections.append(writer)
        identity = len(self.connections)
        try:
            while True:
                head = await reader.readline()
                if not head:
                    return
                assert head.startswith(b'*'), head
                args = []
                for _ in range(int(head[1:])):
                    length = await reader.readline()
                    assert length.startswith(b'$')
                    args.append(await reader.readexactly(int(length[1:])))
                    assert await reader.readexactly(2) == b'\r\n'
                command = args[0].upper()
                self.commands.append([identity, command.decode()])
                if command == b'HELLO':
                    assert args[1] == b'3'
                    self.protocols[writer] = 3
                    writer.write(b'%1\r\n$5\r\nproto\r\n:3\r\n')
                elif command == b'SUBSCRIBE':
                    self.subscriptions.append(writer)
                    data = resp([b'subscribe', args[1], 1])
                    writer.write(b'>'+data[1:] if self.protocols.get(writer) == 3 else data)
                elif command == b'PING':
                    writer.write(resp([b'pong', b'']))
                elif command == b'CLIENT':
                    writer.write(b'+OK\r\n')
                else:
                    raise AssertionError(args)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()


class Cache:
    def __init__(self, raw):
        self.raw = raw
        self.calls = 0

    def pipeline(self, transaction):
        assert transaction
        cache = self
        class Pipe:
            def get(self, key):
                return self
            def pttl(self, key):
                return self
            async def execute(self):
                cache.calls += 1
                return cache.raw, 1900
        return Pipe()

    async def get(self, key):
        return self.raw


class TrackedRedis(Redis):
    def pubsub(self, *args, **kwargs):
        pubsub = super().pubsub(*args, **kwargs)
        self.pubsubs.append(pubsub)
        original = pubsub.handle_message
        async def tracked(*args, **kwargs):
            result = await original(*args, **kwargs)
            if result is not None and result.get('type') == 'subscribe':
                self.observed_ack_count += 1
            return result
        pubsub.handle_message = tracked
        original_get = pubsub.get_message
        async def checkpointed(*args, **kwargs):
            if self.pause_reads:
                self.read_paused.set()
                await self.resume_read.wait()
            return await original_get(*args, **kwargs)
        pubsub.get_message = checkpointed
        return pubsub


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(.001)
    await asyncio.wait_for(wait(), 3)


async def reconnect_case(drop):
    peer = Peer()
    server = await asyncio.start_server(peer.handle, '127.0.0.1', 0)
    extra = {} if drop == 'client_disconnected' else {'retry': Retry(NoBackoff(), 1)}
    subscriber = TrackedRedis(host='127.0.0.1', port=server.sockets[0].getsockname()[1],
        db=0, decode_responses=False, socket_connect_timeout=1,
        socket_keepalive=True, socket_timeout=None, max_connections=1, **extra)
    subscriber.pubsubs = []
    subscriber.observed_ack_count = 0
    subscriber.pause_reads = False
    subscriber.read_paused = asyncio.Event()
    subscriber.resume_read = asyncio.Event()
    cache = Cache(body(1))
    hub = GhostStreamHub(cache, subscriber, wall_ns=lambda: READ_WALL_NS,
        monotonic_ns=lambda: API_MONO_NS, heartbeat_seconds=60)
    await hub.start()
    try:
        await until(lambda: hub.snapshot.read is not None)
        before = {'generation': hub.generation, 'decision': hub.snapshot.read.payload.decision_id,
                  'authoritative_reads': cache.calls}
        subscriber.pause_reads = True
        await asyncio.wait_for(subscriber.read_paused.wait(), 1)
        cache.raw = body(2)  # A publication lost while the subscription is absent.
        if drop == 'client_disconnected':
            await subscriber.pubsubs[0].connection.disconnect()
        else:
            peer.subscriptions[-1].close()
            await peer.subscriptions[-1].wait_closed()
        subscriber.pause_reads = False
        subscriber.resume_read.set()
        await until(lambda: subscriber.observed_ack_count == 2)
        await asyncio.sleep(.025)  # Let the hub consume the returned subscribe ACK.
        after = {'generation': hub.generation,
                 'decision': hub.snapshot.read.payload.decision_id,
                 'authoritative_reads': cache.calls,
                 'cache_decision': parse_ghost_payload(cache.raw).decision_id,
                 'real_pubsub_objects': len(subscriber.pubsubs),
                 'tcp_connections': len(peer.connections),
                 'subscribe_acks_consumed_by_real_client': subscriber.observed_ack_count}
        assert before == {'generation': 1, 'decision': 1, 'authoritative_reads': 1}
        assert after == {'generation': 1, 'decision': 1, 'authoritative_reads': 1,
                         'cache_decision': 2, 'real_pubsub_objects': 1,
                         'tcp_connections': 2, 'subscribe_acks_consumed_by_real_client': 2}
        cache.raw = body(3)
        writer = peer.subscriptions[-1]
        data = resp([b'message', GHOST_CHANNEL.encode(), cache.raw])
        writer.write(b'>'+data[1:] if peer.protocols.get(writer) == 3 else data)
        await peer.subscriptions[-1].drain()
        await until(lambda: hub.snapshot.read.payload.decision_id == 3)
        resumed = {'decision': hub.snapshot.read.payload.decision_id,
                   'generation': hub.generation, 'authoritative_reads': cache.calls}
        assert resumed == {'decision': 3, 'generation': 1, 'authoritative_reads': 1}
        return {'drop': drop, 'retry_policy': 'default' if not extra else 'one retry, zero backoff',
                'before': before, 'after_hidden_reconnect': after,
                'after_next_publication': resumed, 'peer_commands': peer.commands}
    finally:
        await hub.close()
        await subscriber.aclose()
        for writer in peer.connections:
            writer.close()
        server.close()
        await server.wait_closed()


async def configuration_case():
    from fastapi import FastAPI
    import price_collector.api as api
    reached_core = False
    async def forbidden_pool(settings):
        nonlocal reached_core
        reached_core = True
        raise AssertionError('core initialization unexpectedly reached')
    with patch.object(api, 'Settings', lambda: object()), patch.object(api, 'create_read_pool', forbidden_pool), \
         patch.dict('os.environ', {'GHOST_TWAP_API_ENABLED': 'false', 'GHOST_TWAP_API_MAX_CLIENTS': '0'}):
        try:
            async with api.lifespan(FastAPI()):
                raise AssertionError('malformed optional settings did not fail startup')
        except ValueError as exc:
            assert not reached_core
            return {'ghost_enabled': False, 'malformed_setting': 'GHOST_TWAP_API_MAX_CLIENTS=0',
                    'startup_failed': True, 'core_pool_started': reached_core,
                    'error_type': type(exc).__name__, 'error': str(exc)}


async def no_eligible_case():
    from price_collector.ghost_twap_api import (
        GhostApiService, GhostApiSettings, GhostUnavailable, stream_frame,
    )
    raw = raw_payload(missing_history=True)
    service = GhostApiService(GhostApiSettings(enabled=True), Cache(raw), None,
                             wall_ns=lambda: READ_WALL_NS, monotonic_ns=lambda: API_MONO_NS)
    read = bind_read_clock(parse_ghost_payload(raw), wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS)
    service.hub._accept(read, authoritative=True, resync=True)
    try:
        await service.get_snapshot()
        raise AssertionError('GET unexpectedly accepted no eligible prices')
    except GhostUnavailable as exc:
        reason = exc.reason
    frame = stream_frame(service.hub.snapshot, skipped_updates=0, resync=True,
                         wall_ns=READ_WALL_NS, monotonic_ns=API_MONO_NS)
    envelope = json.loads(next(line[6:] for line in frame.splitlines() if line.startswith(b'data: ')))
    assert reason == 'no_eligible_forecasts'
    assert envelope['api']['state'] == 'snapshot'
    assert envelope['api']['reason'] == 'no_eligible_horizons'
    assert envelope['ghost']['current_twap'] is not None
    assert all(f['price'] is None for f in envelope['ghost']['forecasts'])
    return {'GET_unavailable_reason': reason, 'SSE_state': envelope['api']['state'],
            'SSE_reason': envelope['api']['reason'], 'SSE_preserves_official_anchor': True,
            'SSE_all_forecast_prices_null': True}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--reconnect-only', action='store_true')
    args = parser.parse_args()
    assert not args.output.exists(), 'refuse overwrite'
    result = {'status': 'reproduced', 'scope': 'local synthetic TCP peer, no production connection',
              'python': sys.version, 'redis_py_version': redis.__version__,
              'checkout_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'reconnect': [await reconnect_case('client_disconnected'), await reconnect_case('server_closed')]}
    if not args.reconnect_only:
        result.update(optional_settings=await configuration_case(), wholly_unavailable=await no_eligible_case())
    files = [Path(__file__), ROOT/'price_collector/ghost_twap_stream.py',
             ROOT/'price_collector/ghost_twap_api.py', ROOT/'price_collector/api.py',
             ROOT/'tests/test_ghost_twap_payload.py',
             Path(inspect.getsourcefile(redis.asyncio.client.PubSub)),
             Path(inspect.getsourcefile(redis.asyncio.connection.AbstractConnection))]
    result['source_sha256'] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as handle:
        json.dump(result, handle, indent=2)
        handle.write('\n')
    print(json.dumps({'status': result['status'], 'output': str(args.output)}))


if __name__ == '__main__':
    asyncio.run(main())
