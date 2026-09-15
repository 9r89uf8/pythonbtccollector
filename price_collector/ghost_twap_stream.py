"""Bounded read-only Redis fan-out, independent of HTTP and the producer.

Redis Pub/Sub supplies current updates, never a replay log. Every subscription
starts with an acknowledged subscription and an authoritative GET/PTTL. Client
queues retain one latest update; HTTP adapters must also bound their send await.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import time
from uuid import uuid4

from price_collector.ghost_twap_payload import (
    GhostRead, InvalidGhostPayload, MAX_PAYLOAD_BYTES, bind_read_clock,
    parse_ghost_payload,
)

GHOST_KEY = 'btc:live:ghost_chainlink_twap_60s'
GHOST_CHANNEL = 'btc:live:ghost_chainlink_twap_60s:updates'
NS_PER_MS = 1_000_000
# An idle Pub/Sub read intentionally waits 100 ms. It must not inherit the
# independently configurable GET deadline, which may be only 10 ms.
SUBSCRIBER_IO_SECONDS = 1.0
SUBSCRIBER_POLL_SECONDS = 0.1


class TooManyGhostClients(RuntimeError):
    pass


class GhostStreamClosed(RuntimeError):
    pass


class _Resync(RuntimeError):
    pass


@dataclass(frozen=True)
class StreamUpdate:
    read: GhostRead | None
    state: str
    reason: str
    instance_id: str
    generation: int
    sequence: int
    wall_ns: int
    monotonic_ns: int
    resync: bool


@dataclass(frozen=True)
class StreamDelivery:
    update: StreamUpdate
    skipped_updates: int


class StreamClient:
    def __init__(self, hub: GhostStreamHub):
        self._hub = hub
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._closed = False
        self.skipped_updates = 0

    def _offer(self, update: StreamUpdate) -> None:
        if self._closed:
            return
        if self._queue.full():
            self._queue.get_nowait()
            self.skipped_updates += 1
        self._queue.put_nowait(update)

    def _close(self) -> None:
        self._closed = True
        if self._queue.full():
            self._queue.get_nowait()
        self._queue.put_nowait(None)

    async def next(self) -> StreamDelivery:
        if self._closed:
            raise GhostStreamClosed('ghost stream client closed')
        self._hub.expire()
        update = await self._queue.get()
        if update is None:
            raise GhostStreamClosed('ghost stream client closed')
        # Expiry can pass while this consumer is waiting. No await follows the
        # second check: an adapter still checks again immediately before send.
        self._hub.expire()
        if update.read is not None and not update.read.is_fresh(
                wall_ns=self._hub.wall_ns(), monotonic_ns=self._hub.monotonic_ns()):
            update = self._hub.snapshot
            if not self._queue.empty():
                self._queue.get_nowait()
        return StreamDelivery(update, self.skipped_updates)


class GhostStreamHub:
    def __init__(self, request_client, subscriber_client, *, wall_ns=time.time_ns,
                 monotonic_ns=time.monotonic_ns, max_clients=8, buffer_size=128,
                 read_timeout_seconds=0.5, reconnect_delay=0.25, heartbeat_seconds=10.0,
                 expiry_poll_seconds=0.1):
        for name, value, upper in (('max_clients', max_clients, 128),
                                   ('buffer_size', buffer_size, 1024)):
            if type(value) is not int or not 1 <= value <= upper:
                raise ValueError('invalid ' + name)
        for value in (read_timeout_seconds, reconnect_delay, heartbeat_seconds, expiry_poll_seconds):
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 < value <= 60:
                raise ValueError('invalid stream timing bound')
        self.request_client = request_client
        self.subscriber_client = subscriber_client
        self.wall_ns, self.monotonic_ns = wall_ns, monotonic_ns
        self.max_clients, self.buffer_size = max_clients, buffer_size
        self.read_timeout, self.reconnect_delay = read_timeout_seconds, reconnect_delay
        self.heartbeat_seconds, self.expiry_poll_seconds = heartbeat_seconds, expiry_poll_seconds
        self.instance_id = uuid4().hex
        self.generation = 0
        self._sequence = 0
        self._clients: set[StreamClient] = set()
        self._tasks: list[asyncio.Task] = []
        self._closed = False
        self._producer_run = None
        self._producer_sequence = None
        self._producer_raw = None
        self._producer_deadline = None
        self._needs_authoritative = True
        self._conflicted_identity = None
        self._snapshot = StreamUpdate(None, 'unavailable', 'awaiting_data', self.instance_id,
                                      0, 0, wall_ns(), monotonic_ns(), True)

    @property
    def snapshot(self) -> StreamUpdate:
        self.expire()
        return self._snapshot

    @property
    def client_count(self) -> int:
        return len(self._clients)

    def register(self) -> StreamClient:
        if self._closed:
            raise GhostStreamClosed('ghost hub closed')
        if len(self._clients) >= self.max_clients:
            raise TooManyGhostClients('ghost stream client limit reached')
        self.expire()
        client = StreamClient(self)
        self._clients.add(client)
        client._offer(self._snapshot)
        return client

    def unregister(self, client: StreamClient) -> None:
        if client in self._clients:
            self._clients.remove(client)
            client._close()

    def _emit(self, read: GhostRead | None, reason: str, *, resync=False) -> None:
        self._sequence += 1
        self._snapshot = StreamUpdate(read, 'snapshot' if read is not None else 'unavailable',
                                      reason, self.instance_id, self.generation, self._sequence,
                                      self.wall_ns(), self.monotonic_ns(), resync)
        for client in self._clients:
            client._offer(self._snapshot)

    def expire(self) -> bool:
        read = self._snapshot.read
        if read is not None and not read.is_fresh(wall_ns=self.wall_ns(), monotonic_ns=self.monotonic_ns()):
            self._emit(None, 'expired')
            return True
        return False

    async def start(self) -> None:
        if self._closed:
            raise GhostStreamClosed('ghost hub closed')
        if not self._tasks:
            self._tasks = [asyncio.create_task(self._supervise()), asyncio.create_task(self._health())]

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for client in tuple(self._clients):
            self.unregister(client)

    async def _health(self) -> None:
        last = asyncio.get_running_loop().time()
        while not self._closed:
            await asyncio.sleep(self.expiry_poll_seconds)
            expired = self.expire()
            now = asyncio.get_running_loop().time()
            if expired:
                last = now
            elif now-last >= self.heartbeat_seconds:
                # Reuse the original bound: a heartbeat never rebinds a clock.
                self._emit(self._snapshot.read, self._snapshot.reason)
                last = now

    async def _authoritative(self, *, resync: bool, faults=None) -> None:
        start_wall, start_mono = self.wall_ns(), self.monotonic_ns()
        pipe = self.request_client.pipeline(transaction=True)
        pipe.get(GHOST_KEY)
        pipe.pttl(GHOST_KEY)
        raw, ttl = await asyncio.wait_for(pipe.execute(), self.read_timeout)
        if faults:
            raise faults[0]
        end_wall, end_mono = self.wall_ns(), self.monotonic_ns()
        if min(end_wall-start_wall, end_mono-start_mono) < 0:
            raise _Resync('api_clock_regression')
        if raw is None:
            self._needs_authoritative = True
            self._emit(None, 'absent' if ttl == -2 else 'inconsistent_cache', resync=resync)
            return
        if type(ttl) is not int or ttl <= 0:
            self._needs_authoritative = True
            self._emit(None, 'invalid_cache_ttl', resync=resync)
            return
        try:
            payload = parse_ghost_payload(raw)
            read = bind_read_clock(payload, wall_ns=end_wall, monotonic_ns=end_mono)
        except InvalidGhostPayload:
            self._needs_authoritative = True
            self._emit(None, 'invalid_payload', resync=resync)
            return
        # PTTL was sampled somewhere during the request. Subtract the complete
        # request duration and one millisecond of integer TTL granularity.
        remaining = ttl*NS_PER_MS-max(end_wall-start_wall, end_mono-start_mono)-NS_PER_MS
        read = replace(read, api_deadline_monotonic_ns=min(read.api_deadline_monotonic_ns,
                                                         end_mono+max(0, remaining)))
        self._accept(read, authoritative=True, resync=resync)

    def _accept(self, read: GhostRead, *, authoritative=False, resync=False) -> None:
        payload = read.payload
        identity = (payload.run_id, payload.decision_id)
        if self._conflicted_identity == identity:
            self._emit(None, 'conflicting_payload', resync=resync)
            return
        if payload.run_id == self._producer_run:
            if payload.decision_id < self._producer_sequence:
                if authoritative:
                    self._emit(None, 'regressed_snapshot', resync=resync)
                return
            if payload.decision_id == self._producer_sequence:
                if payload.raw != self._producer_raw:
                    self._conflicted_identity = identity
                    self._emit(None, 'conflicting_payload', resync=resync)
                    return
                if not authoritative:
                    return
                # Re-reading the same bytes may shorten their local deadline,
                # but must not move an existing bound later.
                read = replace(read, api_deadline_monotonic_ns=min(
                    read.api_deadline_monotonic_ns, self._producer_deadline))
        else:
            if not authoritative:
                raise _Resync('unconfirmed_producer')
            if self._producer_run is not None:
                self.generation += 1
            self._conflicted_identity = None
        self._producer_run = payload.run_id
        self._producer_sequence = payload.decision_id
        self._producer_raw = payload.raw
        self._producer_deadline = read.api_deadline_monotonic_ns
        self._needs_authoritative = False
        if not read.is_fresh(wall_ns=self.wall_ns(), monotonic_ns=self.monotonic_ns()):
            self._emit(None, 'expired', resync=resync)
        else:
            self._emit(read, 'fresh' if payload.has_eligible_prices else 'no_eligible_horizons', resync=resync)

    async def _pump(self, pubsub, queue, faults) -> None:
        loop = asyncio.get_running_loop()
        last_server, ping_sent = loop.time(), None
        try:
            while not self._closed:
                message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=False,
                                                                    timeout=SUBSCRIBER_POLL_SECONDS),
                                                 SUBSCRIBER_IO_SECONDS)
                now = loop.time()
                if message is not None:
                    if (message.get('type') in ('subscribe', 'unsubscribe')
                            and message.get('channel') in (GHOST_CHANNEL, GHOST_CHANNEL.encode())):
                        # redis-py can reconnect and resubscribe inside a read
                        # without raising to this supervisor. Only _connection
                        # consumes the initial ACK; a later ACK is a new loss
                        # boundary, even if no subsequent publication arrives.
                        # Invalidate now, including while bootstrap GET awaits.
                        # The shared fault check fences that read's result.
                        self._needs_authoritative = True
                        self._emit(None, 'subscriber_reconnected', resync=True)
                        raise _Resync('subscriber_reconnected')
                    last_server, ping_sent = now, None
                    if message.get('type') == 'message' and message.get('channel') in (GHOST_CHANNEL, GHOST_CHANNEL.encode()):
                        raw = message.get('data')
                        if type(raw) is not bytes or len(raw) > MAX_PAYLOAD_BYTES:
                            raw = None
                        item = (raw, self.wall_ns(), self.monotonic_ns())
                        if queue.full():
                            raise _Resync('subscriber_buffer_overflow')
                        queue.put_nowait(item)
                if ping_sent is not None and now-ping_sent >= SUBSCRIBER_IO_SECONDS:
                    raise _Resync('subscriber_health_timeout')
                if ping_sent is None and now-last_server >= 2.0:
                    await asyncio.wait_for(pubsub.ping(), SUBSCRIBER_IO_SECONDS)
                    ping_sent = loop.time()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            faults.append(exc)
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(exc)

    async def _connection(self) -> None:
        pubsub = self.subscriber_client.pubsub()
        pump = None
        try:
            await asyncio.wait_for(pubsub.subscribe(GHOST_CHANNEL), SUBSCRIBER_IO_SECONDS)
            loop = asyncio.get_running_loop()
            deadline = loop.time()+SUBSCRIBER_IO_SECONDS
            while True:
                remaining = deadline-loop.time()
                if remaining <= 0:
                    raise _Resync('subscription_ack_timeout')
                message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=False, timeout=remaining), remaining)
                if (message is not None and message.get('type') == 'subscribe'
                        and message.get('channel') in (GHOST_CHANNEL, GHOST_CHANNEL.encode())):
                    break
            queue = asyncio.Queue(maxsize=self.buffer_size)
            faults = []
            pump = asyncio.create_task(self._pump(pubsub, queue, faults))
            await self._authoritative(resync=True, faults=faults)
            while not self._closed:
                item = await queue.get()
                if isinstance(item, Exception):
                    raise item
                raw, wall, mono = item
                try:
                    payload = parse_ghost_payload(raw)
                    read = bind_read_clock(payload, wall_ns=wall, monotonic_ns=mono)
                except InvalidGhostPayload:
                    self._emit(None, 'invalid_payload')
                    continue
                if self._needs_authoritative or payload.run_id != self._producer_run:
                    self._emit(None, 'resyncing', resync=True)
                    await self._authoritative(resync=True, faults=faults)
                else:
                    self._accept(read)
        finally:
            if pump is not None:
                pump.cancel()
                await asyncio.gather(pump, return_exceptions=True)
            close = getattr(pubsub, 'aclose', None) or getattr(pubsub, 'close')
            try:
                await asyncio.wait_for(close(), SUBSCRIBER_IO_SECONDS)
            except Exception:
                pass

    async def _supervise(self) -> None:
        while not self._closed:
            self.generation += 1
            self._emit(None, 'connecting', resync=True)
            try:
                await self._connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = str(exc) if isinstance(exc, _Resync) else 'redis_unavailable'
                self._emit(None, reason, resync=True)
                await asyncio.sleep(self.reconnect_delay)
