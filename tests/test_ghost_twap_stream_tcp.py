"""Real redis-py PubSub reconnection against a bounded synthetic loopback peer.

No Redis service or production connection is used. Both RESP2 (redis-py 7) and
RESP3 (redis-py 8 default) handshakes are supported. The cache read is a fake;
transport reconnect, callbacks, retry and acknowledgement parsing are real.
"""
import asyncio

import pytest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from price_collector.ghost_twap_stream import GHOST_CHANNEL, GhostStreamHub
from test_ghost_twap_stream import Clock, Reader, body


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
        self.writers = []
        self.subscriptions = []
        self.tasks = set()
        self.errors = []

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.append(writer)
        protocol = 2
        try:
            while True:
                head = await reader.readline()
                if not head:
                    return
                assert head.startswith(b'*')
                args = []
                for _ in range(int(head[1:])):
                    length = await reader.readline()
                    assert length.startswith(b'$')
                    args.append(await reader.readexactly(int(length[1:])))
                    assert await reader.readexactly(2) == b'\r\n'
                command = args[0].upper()
                if command == b'HELLO':
                    assert args[1] == b'3'
                    protocol = 3
                    writer.write(b'%1\r\n$5\r\nproto\r\n:3\r\n')
                elif command == b'SUBSCRIBE':
                    assert args[1] == GHOST_CHANNEL.encode()
                    self.subscriptions.append(writer)
                    data = resp([b'subscribe', args[1], 1])
                    writer.write(b'>'+data[1:] if protocol == 3 else data)
                elif command == b'CLIENT':
                    writer.write(b'+OK\r\n')
                elif command == b'PING':
                    writer.write(resp([b'pong', b'']))
                else:
                    raise AssertionError(args)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as exc:
            self.errors.append(exc)
        finally:
            writer.close()
            self.tasks.discard(task)

    async def close(self):
        for writer in self.writers:
            writer.close()
        if self.tasks:
            await asyncio.wait_for(asyncio.gather(*self.tasks), 1)


class TrackedRedis(Redis):
    """Pause only between real reads to make disconnect placement deterministic."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pubsubs = []
        self.ack_count = 0
        self.pause_reads = False
        self.read_paused = asyncio.Event()
        self.resume_read = asyncio.Event()

    def pubsub(self, *args, **kwargs):
        pubsub = super().pubsub(*args, **kwargs)
        self.pubsubs.append(pubsub)
        original_handle = pubsub.handle_message
        original_get = pubsub.get_message
        async def handle(*args, **kwargs):
            result = await original_handle(*args, **kwargs)
            if result is not None and result.get('type') == 'subscribe':
                self.ack_count += 1
            return result
        async def get(*args, **kwargs):
            if self.pause_reads:
                self.read_paused.set()
                await self.resume_read.wait()
            return await original_get(*args, **kwargs)
        pubsub.handle_message = handle
        pubsub.get_message = get
        return pubsub


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(.001)
    await asyncio.wait_for(wait(), 3)


@pytest.mark.parametrize('drop', ['already_disconnected', 'server_closed'])
def test_real_pubsub_hidden_reconnect_refreshes_cache_without_next_publish(drop):
    async def check():
        peer = Peer()
        server = await asyncio.start_server(peer.handle, '127.0.0.1', 0)
        # Keep the actual default retry for parse_response's disconnected path.
        # The failed-read path uses zero backoff to avoid random retry timing.
        retry = {} if drop == 'already_disconnected' else {'retry': Retry(NoBackoff(), 1)}
        subscriber = TrackedRedis(host='127.0.0.1', port=server.sockets[0].getsockname()[1],
            decode_responses=False, socket_connect_timeout=1, socket_keepalive=True,
            socket_timeout=None, max_connections=1, **retry)
        clock, reader = Clock(), Reader(body())
        hub = GhostStreamHub(reader, subscriber, wall_ns=lambda: clock.wall,
                             monotonic_ns=lambda: clock.mono, reconnect_delay=.01)
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            first = hub.snapshot
            client = hub.register()
            await client.next()
            subscriber.pause_reads = True
            await asyncio.wait_for(subscriber.read_paused.wait(), 1)
            if drop == 'already_disconnected':
                await subscriber.pubsubs[0].connection.disconnect()
            else:
                peer.subscriptions[0].close()
                await peer.subscriptions[0].wait_closed()
            reader.value = body(2)
            reader.gate = asyncio.Event()
            subscriber.pause_reads = False
            subscriber.resume_read.set()
            # There is intentionally no PUBLISH after reconnect. The current
            # cache state must become visible through a new GET/PTTL bootstrap.
            await until(lambda: reader.calls == 2)
            assert subscriber.ack_count == 3
            assert len(subscriber.pubsubs) == 2
            assert len(peer.writers) == 3
            assert hub.snapshot.generation > first.generation
            unavailable = await client.next()
            assert unavailable.update.read is None and unavailable.update.resync
            reader.gate.set()
            await until(lambda: hub.snapshot.read is not None)
            assert hub.snapshot.read.payload.raw == body(2)
            assert hub.snapshot.resync
            assert not peer.errors
        finally:
            await hub.close()
            await subscriber.aclose()
            server.close()
            await server.wait_closed()
            await peer.close()
    asyncio.run(check())
