"""Read-only ghost snapshots and bounded SSE delivery; no forecast calculation."""
from __future__ import annotations

import asyncio
from contextlib import suppress
from enum import Enum
import json
import time
from typing import Any

from fastapi import APIRouter, Request
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.responses import JSONResponse, Response

from price_collector.ghost_twap_payload import (
    GhostRead, InvalidGhostPayload, bind_read_clock, parse_ghost_payload,
)
from price_collector.ghost_twap_stream import GhostStreamClosed, GhostStreamHub, TooManyGhostClients

GHOST_KEY = 'btc:live:ghost_chainlink_twap_60s'
STREAM_PATH = '/forecasts/chainlink-twap/stream'
HEADERS = {'Cache-Control': 'no-store, no-transform', 'X-Accel-Buffering': 'no'}
router = APIRouter(prefix='/forecasts/chainlink-twap', tags=['ghost forecasts'])


class GhostApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='GHOST_TWAP_API_', case_sensitive=False)
    enabled: bool = False
    max_clients: int = Field(default=16, ge=1, le=16)
    read_timeout_ms: int = Field(default=250, ge=10, le=1000)
    send_timeout_ms: int = Field(default=2000, ge=100, le=2000)


class GhostApiDisabledReason(str, Enum):
    DISABLED = 'disabled'
    INVALID_SETTINGS = 'invalid_settings'


class GhostUnavailable(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class GhostApiService:
    def __init__(self, settings: GhostApiSettings, reader: Any, subscriber: Any,
                 *, wall_ns=time.time_ns, monotonic_ns=time.monotonic_ns) -> None:
        self.settings = settings
        self.reader = reader
        self.subscriber = subscriber
        self.wall_ns = wall_ns
        self.monotonic_ns = monotonic_ns
        self.hub = GhostStreamHub(reader, subscriber, max_clients=settings.max_clients,
                                  read_timeout_seconds=settings.read_timeout_ms / 1000,
                                  wall_ns=wall_ns, monotonic_ns=monotonic_ns)

    async def start(self) -> None:
        await self.hub.start()

    async def close(self) -> None:
        await self.hub.close()
        for client in (self.subscriber, self.reader):
            with suppress(RedisError, OSError, asyncio.TimeoutError):
                await asyncio.wait_for(client.aclose(), timeout=1)

    async def get_snapshot(self) -> GhostRead:
        # One key, one GET. Timeouts include pool admission and parsing follows
        # receipt; API monotonic clocks never share an assumed producer origin.
        try:
            raw = await asyncio.wait_for(self.reader.get(GHOST_KEY),
                                        timeout=self.settings.read_timeout_ms / 1000)
        except (RedisError, OSError, asyncio.TimeoutError) as exc:
            raise GhostUnavailable('redis_unavailable') from exc
        if raw is None:
            raise GhostUnavailable('no_current_publication')
        try:
            read = bind_read_clock(parse_ghost_payload(raw), wall_ns=self.wall_ns(),
                                   monotonic_ns=self.monotonic_ns())
        except InvalidGhostPayload as exc:
            raise GhostUnavailable('invalid_publication') from exc
        if not read.is_fresh(wall_ns=self.wall_ns(), monotonic_ns=self.monotonic_ns()):
            raise GhostUnavailable('expired')
        if not read.payload.has_eligible_prices:
            raise GhostUnavailable('no_eligible_forecasts')
        return read


def create_ghost_api_service(settings: Any, ghost_settings: GhostApiSettings) -> GhostApiService:
    connection = dict(host=settings.REDIS_HOST, port=settings.REDIS_PORT,
                      db=settings.REDIS_DB, decode_responses=False,
                      socket_connect_timeout=1, socket_keepalive=True)
    reader = Redis(**connection, socket_timeout=ghost_settings.read_timeout_ms / 1000,
                   max_connections=8)
    # An SSE idle interval must not inherit the ordinary 250 ms GET deadline.
    subscriber = Redis(**connection, socket_timeout=None, max_connections=1)
    return GhostApiService(ghost_settings, reader, subscriber)


def unavailable(reason: str) -> JSONResponse:
    return JSONResponse({'state': 'unavailable', 'reason': reason}, status_code=503,
                        headers=HEADERS)


def _service(request: Request) -> GhostApiService | None:
    service = getattr(request.app.state, 'ghost_api', None)
    return service if service is not None and service.settings.enabled else None


def _disabled_response(request: Request) -> JSONResponse:
    reason = getattr(request.app.state, 'ghost_api_disabled_reason', GhostApiDisabledReason.DISABLED)
    return unavailable(reason)


@router.get('/live', response_class=Response)
async def ghost_live(request: Request) -> Response:
    service = _service(request)
    if service is None:
        return _disabled_response(request)
    try:
        read = await service.get_snapshot()
    except GhostUnavailable as exc:
        return unavailable(exc.reason)
    wall, mono = service.wall_ns(), service.monotonic_ns()
    remaining = read.remaining_ns(wall_ns=wall, monotonic_ns=mono)
    if remaining <= 0:
        return unavailable('expired')
    headers = dict(HEADERS)
    headers.update({
        'X-Ghost-API-Time-Ns': str(wall),
        'X-Ghost-Decision-Age-Ns': str(wall - read.payload.decision_wall_ns),
        'X-Ghost-Remaining-Ns': str(remaining),
        'X-Ghost-Run-Id': read.payload.run_id,
        'X-Ghost-Decision-Id': str(read.payload.decision_id),
    })
    return Response(content=read.payload.raw, media_type='application/json', headers=headers)


def stream_frame(update: Any, *, skipped_updates: int, resync: bool,
                 wall_ns: int, monotonic_ns: int) -> bytes:
    read = update.read
    fresh = read is not None and read.is_fresh(wall_ns=wall_ns, monotonic_ns=monotonic_ns)
    state = update.state if fresh or read is None else 'unavailable'
    reason = update.reason if fresh or read is None else 'expired'
    metadata = {
        'version': 1, 'instance_id': update.instance_id, 'generation': update.generation,
        'sequence': update.sequence, 'state': state, 'reason': reason,
        'resync': resync, 'skipped_updates': skipped_updates,
        'fanout_wall_ns': str(update.wall_ns),
        'fanout_monotonic_ns': str(update.monotonic_ns),
        'send_wall_ns': str(wall_ns), 'send_monotonic_ns': str(monotonic_ns),
        'remaining_ns': str(read.remaining_ns(wall_ns=wall_ns, monotonic_ns=monotonic_ns)) if fresh else '0',
        'read_wall_ns': str(read.received_wall_ns) if read else None,
        'read_monotonic_ns': str(read.received_monotonic_ns) if read else None,
    }
    # The nested ghost is the exact producer JSON byte sequence. Only delivery
    # metadata is encoded here; prices and forecast arithmetic are untouched.
    body = b'{"api":' + json.dumps(metadata, separators=(',', ':'), allow_nan=False).encode() + b',"ghost":'
    body += read.payload.raw if fresh else b'null'
    body += b'}'
    event_id = f'{update.instance_id}:{update.generation}:{update.sequence}'
    return (f'id: {event_id}\nevent: ghost\n'.encode()
            + b''.join(b'data: ' + line + b'\n' for line in body.splitlines()) + b'\n')


class GhostStreamResponse(Response):
    """Deadline the actual ASGI send, including headers, and always unregister."""
    media_type = 'text/event-stream'

    def __init__(self, service: GhostApiService, client: Any) -> None:
        super().__init__(content=None, headers=HEADERS, media_type=self.media_type)
        self.raw_headers = [(name, value) for name, value in self.raw_headers
                            if name != b'content-length']
        self.service = service
        self.client = client

    async def __call__(self, scope, receive, send) -> None:
        async def checked_send(message):
            await asyncio.wait_for(send(message), timeout=self.service.settings.send_timeout_ms / 1000)

        async def listen():
            while True:
                if (await receive())['type'] == 'http.disconnect':
                    return

        async def transmit():
            await checked_send({'type': 'http.response.start', 'status': 200,
                                'headers': self.raw_headers})
            previous_generation = None
            while True:
                try:
                    delivery = await self.client.next()
                except GhostStreamClosed:
                    await checked_send({'type': 'http.response.body', 'body': b'', 'more_body': False})
                    return
                update = delivery.update
                frame = stream_frame(
                    update, skipped_updates=delivery.skipped_updates,
                    resync=update.resync or previous_generation != update.generation,
                    wall_ns=self.service.wall_ns(), monotonic_ns=self.service.monotonic_ns())
                await checked_send({'type': 'http.response.body', 'body': frame, 'more_body': True})
                previous_generation = update.generation

        tasks = [asyncio.create_task(transmit()), asyncio.create_task(listen())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                with suppress(asyncio.TimeoutError, OSError, GhostStreamClosed):
                    task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.service.hub.unregister(self.client)


@router.get('/stream', response_class=Response)
async def ghost_stream(request: Request) -> Response:
    service = _service(request)
    if service is None:
        return _disabled_response(request)
    try:
        client = service.hub.register()
    except TooManyGhostClients:
        return unavailable('client_limit')
    except GhostStreamClosed:
        return unavailable('shutting_down')
    # A Last-Event-ID is deliberately not replayed. The initial envelope always
    # declares a resync to current state, including after EventSource reconnect.
    return GhostStreamResponse(service, client)


class GhostCompressionBypass:
    """Preserve ghost bytes and stream timing regardless of Starlette version."""
    def __init__(self, app, **gzip_options) -> None:
        from starlette.middleware.gzip import GZipMiddleware
        self.app = app
        self.compressed = GZipMiddleware(app, **gzip_options)

    async def __call__(self, scope, receive, send) -> None:
        app = self.app if scope['type'] == 'http' and scope.get('path') in (
            STREAM_PATH, '/forecasts/chainlink-twap/live') else self.compressed
        await app(scope, receive, send)
