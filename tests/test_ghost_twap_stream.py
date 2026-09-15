"""Deterministic Redis ordering/fan-out faults; no service or database access."""
import asyncio
from dataclasses import FrozenInstanceError
import json

import pytest

from price_collector.ghost_twap_stream import (
    GHOST_CHANNEL, GHOST_KEY, GhostStreamClosed, GhostStreamHub, TooManyGhostClients,
)
from test_ghost_twap_payload import API_MONO_NS, READ_WALL_NS, raw_payload, wire_payload


class Clock:
    def __init__(self):
        self.wall = READ_WALL_NS
        self.mono = API_MONO_NS

    def advance(self, milliseconds):
        self.wall += milliseconds * 1_000_000
        self.mono += milliseconds * 1_000_000


def body(sequence=1, run='wire-run'):
    value = wire_payload()
    value.update(run_id=run, decision_id=str(sequence), publication_sequence=sequence)
    return raw_payload(value)


class Pipe:
    def __init__(self, reader):
        self.reader = reader
        self.commands = []

    def get(self, key):
        self.commands.append(('get', key))
        return self

    def pttl(self, key):
        self.commands.append(('pttl', key))
        return self

    async def execute(self):
        assert self.commands == [('get', GHOST_KEY), ('pttl', GHOST_KEY)]
        self.reader.calls += 1
        result = self.reader.value, self.reader.ttl
        if self.reader.gate is not None:
            await self.reader.gate.wait()
        if self.reader.error is not None:
            raise self.reader.error
        if self.reader.advance is not None:
            self.reader.advance()
        return result


class Reader:
    def __init__(self, value=None, ttl=1900):
        self.value, self.ttl = value, ttl
        self.calls = 0
        self.gate = None
        self.error = None
        self.advance = None

    def pipeline(self, transaction):
        assert transaction is True
        return Pipe(self)


class PubSub:
    def __init__(self, auto_ack=True):
        self.queue = asyncio.Queue()
        self.auto_ack = auto_ack
        self.subscribed = False
        self.closed = False
        self.read_calls = 0

    def ack(self):
        self.queue.put_nowait(dict(type='subscribe', channel=GHOST_CHANNEL.encode(), data=1))

    async def subscribe(self, channel):
        assert channel == GHOST_CHANNEL
        self.subscribed = True
        if self.auto_ack:
            self.ack()

    def publish(self, value):
        self.queue.put_nowait(dict(type='message', channel=GHOST_CHANNEL.encode(), data=value))

    async def get_message(self, *, ignore_subscribe_messages, timeout):
        assert ignore_subscribe_messages is False
        self.read_calls += 1
        try:
            value = await asyncio.wait_for(self.queue.get(), timeout)
        except asyncio.TimeoutError:
            return None
        if isinstance(value, Exception):
            raise value
        return value

    async def ping(self):
        self.queue.put_nowait(dict(type='pong', data=b''))

    async def aclose(self):
        self.closed = True


class Subscriber:
    def __init__(self, first=None):
        self.first = first
        self.sessions = []

    def pubsub(self):
        session = self.first if not self.sessions and self.first is not None else PubSub()
        self.sessions.append(session)
        return session


def setup(*, raw=None, **options):
    clock = Clock()
    reader = Reader(body() if raw is None else raw)
    subscriber = Subscriber()
    hub = GhostStreamHub(reader, subscriber, wall_ns=lambda: clock.wall,
        monotonic_ns=lambda: clock.mono, reconnect_delay=.01,
        read_timeout_seconds=options.pop('read_timeout_seconds', .2),
        expiry_poll_seconds=.005, **options)
    return hub, clock, reader, subscriber


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(.001)
    await asyncio.wait_for(wait(), timeout=1)


def test_subscription_ack_precedes_read_and_bootstrap_buffers_newer_update():
    async def check():
        hub, _, reader, subscriber = setup()
        subscriber.first = PubSub(auto_ack=False)
        reader.gate = asyncio.Event()
        await hub.start()
        try:
            await until(lambda: subscriber.first.subscribed)
            await asyncio.sleep(.01)
            assert reader.calls == 0
            subscriber.first.ack()
            await until(lambda: reader.calls == 1)
            subscriber.first.publish(body(2))
            await asyncio.sleep(.01)
            assert hub.snapshot.read is None
            reader.gate.set()
            await until(lambda: hub.snapshot.read is not None and hub.snapshot.read.payload.decision_id == 2)
            assert hub.snapshot.read.payload.raw == body(2)
            assert reader.calls == 1
        finally:
            await hub.close()
        assert subscriber.first.closed
    asyncio.run(check())


def test_same_run_duplicates_and_older_messages_do_not_regress_or_rebind():
    async def check():
        hub, clock, _, subscriber = setup(raw=body(3))
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            first = hub.snapshot
            clock.advance(100)
            subscriber.sessions[0].publish(body(3))
            subscriber.sessions[0].publish(body(2))
            await asyncio.sleep(.015)
            assert hub.snapshot is first
            assert hub.snapshot.read.payload.decision_id == 3
            with pytest.raises(FrozenInstanceError):
                first.reason = 'changed'
        finally:
            await hub.close()
    asyncio.run(check())


def test_same_identity_different_bytes_remains_conflicted_until_advancement():
    async def check():
        hub, _, reader, subscriber = setup()
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            subscriber.sessions[0].publish(body()+b' ')
            await until(lambda: hub.snapshot.reason == 'conflicting_payload')
            subscriber.sessions[0].publish(body())
            await asyncio.sleep(.01)
            assert hub.snapshot.read is None
            reader.value = body(2)
            subscriber.sessions[0].publish(reader.value)
            await until(lambda: hub.snapshot.read is not None)
            assert hub.snapshot.read.payload.decision_id == 2
        finally:
            await hub.close()
    asyncio.run(check())


def test_new_run_uses_current_snapshot_and_delayed_old_run_never_restores():
    async def check():
        hub, _, reader, subscriber = setup(raw=body(9, 'run-z'))
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            generation = hub.snapshot.generation
            reader.value = body(1, 'run-a')
            subscriber.sessions[0].publish(reader.value)
            await until(lambda: hub.snapshot.read is not None and hub.snapshot.read.payload.run_id == 'run-a')
            assert hub.snapshot.generation > generation
            subscriber.sessions[0].publish(body(10, 'run-z'))
            await until(lambda: reader.calls >= 3)
            await asyncio.sleep(.01)
            assert hub.snapshot.read.payload.run_id == 'run-a'
            assert hub.snapshot.read.payload.decision_id == 1
        finally:
            await hub.close()
    asyncio.run(check())


def test_absent_bootstrap_cannot_be_revived_by_preexisting_buffered_body():
    async def check():
        hub, _, reader, subscriber = setup()
        reader.value, reader.ttl = None, -2
        reader.gate = asyncio.Event()
        await hub.start()
        try:
            await until(lambda: reader.calls == 1)
            subscriber.sessions[0].publish(body())
            await asyncio.sleep(.01)
            reader.gate.set()
            await until(lambda: reader.calls == 2)
            await asyncio.sleep(.01)
            assert hub.snapshot.reason == 'absent' and hub.snapshot.read is None
        finally:
            await hub.close()
    asyncio.run(check())


@pytest.mark.parametrize('ttl,reason', [(-1, 'invalid_cache_ttl'), (0, 'invalid_cache_ttl'),
                                      (-2, 'invalid_cache_ttl'), (True, 'invalid_cache_ttl')])
def test_present_key_requires_positive_integer_ttl(ttl, reason):
    async def check():
        hub, _, reader, _ = setup()
        reader.ttl = ttl
        await hub.start()
        try:
            await until(lambda: hub.snapshot.reason == reason)
            assert hub.snapshot.read is None
        finally:
            await hub.close()
    asyncio.run(check())


def test_expired_buffered_message_is_not_freshened_at_processing_time():
    async def check():
        hub, clock, reader, subscriber = setup()
        reader.gate = asyncio.Event()
        await hub.start()
        try:
            await until(lambda: reader.calls == 1)
            subscriber.sessions[0].publish(body(2))
            await asyncio.sleep(.01)
            clock.advance(2000)
            reader.gate.set()
            await until(lambda: hub.snapshot.reason == 'expired')
            await asyncio.sleep(.01)
            assert hub.snapshot.read is None
        finally:
            await hub.close()
    asyncio.run(check())


def test_expired_same_identity_stays_expired_after_reconnect_even_if_wall_stalls():
    async def check():
        hub, clock, reader, subscriber = setup()
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            deadline = hub.snapshot.read.api_deadline_monotonic_ns
            clock.mono = deadline + 1
            assert hub.expire()
            subscriber.sessions[0].queue.put_nowait(OSError('connection lost'))
            await until(lambda: reader.calls >= 2)
            await asyncio.sleep(.01)
            assert hub.snapshot.reason == 'expired' and hub.snapshot.read is None
        finally:
            await hub.close()
    asyncio.run(check())


def test_buffer_overflow_during_bootstrap_discards_uncertain_prefix_and_resyncs():
    async def check():
        hub, _, reader, subscriber = setup(buffer_size=1)
        incoming = [body(n) for n in (2, 3, 4)]
        reader.gate = asyncio.Event()
        emitted = []
        original = hub._emit
        def capture(*args, **kwargs):
            original(*args, **kwargs)
            emitted.append(hub.snapshot)
        hub._emit = capture
        await hub.start()
        try:
            await until(lambda: reader.calls == 1)
            for raw in incoming:
                subscriber.sessions[0].publish(raw)
            # Wait for the receiver to consume the second publication while
            # the one-slot buffer's consumer is still held at bootstrap GET.
            await until(lambda: subscriber.sessions[0].queue.qsize() <= 1)
            await asyncio.sleep(0)
            reader.value = incoming[-1]
            reader.gate.set()
            await until(lambda: reader.calls >= 2 and hub.snapshot.read is not None)
            assert any(x.reason == 'subscriber_buffer_overflow' for x in emitted)
            assert all(x.read is None or x.read.payload.decision_id == 4 for x in emitted)
            assert hub.snapshot.read.payload.decision_id == 4
        finally:
            await hub.close()
    asyncio.run(check())


def test_heartbeat_and_expiry_preserve_original_deadline_and_client_latest_wins():
    async def check():
        hub, clock, _, _ = setup(heartbeat_seconds=.015)
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            client = hub.register()
            first = await client.next()
            await until(lambda: hub.snapshot.sequence > first.update.sequence)
            second = await client.next()
            assert second.update.read is first.update.read
            clock.advance(2000)
            final = await client.next()
            assert final.update.reason == 'expired' and final.update.read is None
        finally:
            await hub.close()
    asyncio.run(check())


def test_slow_client_is_bounded_and_fast_client_receives_each_current_update():
    async def check():
        hub, _, _, subscriber = setup(max_clients=2)
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            slow, fast = hub.register(), hub.register()
            await fast.next()
            with pytest.raises(TooManyGhostClients):
                hub.register()
            for n in (2, 3, 4):
                subscriber.sessions[0].publish(body(n))
                update = await asyncio.wait_for(fast.next(), .2)
                assert update.update.read.payload.decision_id == n
                assert update.skipped_updates == 0
            last = await slow.next()
            assert last.update.read.payload.decision_id == 4
            assert last.skipped_updates == 3
            hub.unregister(slow)
            assert hub.client_count == 1
            hub.register()
        finally:
            await hub.close()
        assert hub.client_count == 0
    asyncio.run(check())


def test_close_unblocks_waiting_consumer_and_preserves_caller_redis_ownership():
    async def check():
        hub, _, _, subscriber = setup()
        await hub.start()
        await until(lambda: hub.snapshot.read is not None)
        client = hub.register()
        await client.next()
        waiter = asyncio.create_task(client.next())
        await asyncio.sleep(0)
        await hub.close()
        with pytest.raises(GhostStreamClosed):
            await waiter
        assert all(session.closed for session in subscriber.sessions)
        with pytest.raises(GhostStreamClosed):
            hub.register()
        await hub.close()
    asyncio.run(check())


@pytest.mark.parametrize('raw', [b'{broken', b'x' * 65537, b'{}', b'[]'],
                         ids=['invalid_json', 'oversized', 'missing_fields', 'wrong_shape'])
def test_malformed_or_oversized_pubsub_update_fails_closed(raw):
    async def check():
        hub, _, _, subscriber = setup()
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            subscriber.sessions[0].publish(raw)
            await until(lambda: hub.snapshot.reason == 'invalid_payload')
            assert hub.snapshot.read is None
        finally:
            await hub.close()
    asyncio.run(check())


@pytest.mark.parametrize('duration_ms,ttl', [(0, 1), (3, 4), (3, 3)])
def test_ttl_requires_remaining_budget_after_full_read_and_one_ms_margin(duration_ms, ttl):
    async def check():
        hub, clock, reader, _ = setup()
        reader.ttl = ttl
        reader.advance = lambda: clock.advance(duration_ms)
        await hub.start()
        try:
            await until(lambda: hub.snapshot.reason == 'expired')
            assert hub.snapshot.read is None
        finally:
            await hub.close()
    asyncio.run(check())


def test_failed_read_recovers_without_poisoning_other_routes_or_future_updates():
    async def check():
        hub, _, reader, _ = setup()
        reader.error = OSError('offline')
        await hub.start()
        try:
            await until(lambda: hub.snapshot.reason == 'redis_unavailable')
            reader.error = None
            await until(lambda: hub.snapshot.read is not None)
            assert hub.snapshot.read.payload.raw == body()
        finally:
            await hub.close()
    asyncio.run(check())


def test_ten_ms_get_deadline_does_not_cancel_healthy_hundred_ms_idle_subscription():
    async def check():
        hub, _, reader, subscriber = setup(read_timeout_seconds=.01)
        await hub.start()
        try:
            await until(lambda: hub.snapshot.read is not None)
            first = hub.snapshot
            session = subscriber.sessions[0]
            # Fake PubSub uses the real event-loop wait for each idle poll;
            # no injected fast response hides a timeout shorter than that poll.
            await asyncio.sleep(.25)
            assert len(subscriber.sessions) == 1
            assert reader.calls == 1
            assert hub.snapshot is first
            assert 3 <= session.read_calls <= 5
        finally:
            await hub.close()
    asyncio.run(check())
