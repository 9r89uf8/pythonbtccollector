"""Optional Redis-only settlement snapshot, stream and evaluation report."""
from __future__ import annotations

import asyncio
from fastapi import APIRouter, Request
from pydantic_settings import SettingsConfigDict
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.responses import Response

from price_collector.ghost_twap_api import (
    GhostApiSettings, GhostApiService, GhostUnavailable, GhostStreamResponse,
    HEADERS, unavailable,
)
from price_collector.ghost_twap_payload import bind_read_clock, InvalidGhostPayload
from price_collector.ghost_twap_stream import GhostStreamHub, GhostStreamClosed, TooManyGhostClients
from price_collector.settlement_wire import KEY, CHANNEL, REPORT_KEY, parse_settlement_payload

router = APIRouter(prefix='/forecasts/chainlink-twap/settlement', tags=['settlement candidate'])


class SettlementApiSettings(GhostApiSettings):
    model_config = SettingsConfigDict(env_prefix='SETTLEMENT_API_', case_sensitive=False)


class SettlementApiService(GhostApiService):
    def __init__(self, settings, reader, subscriber, **clocks):
        super().__init__(settings, reader, subscriber, **clocks)
        self.hub = GhostStreamHub(reader, subscriber, max_clients=settings.max_clients,
            read_timeout_seconds=settings.read_timeout_ms / 1000,
            wall_ns=self.wall_ns, monotonic_ns=self.monotonic_ns,
            key=KEY, channel=CHANNEL, parser=parse_settlement_payload)

    async def get_snapshot(self):
        try:
            raw = await asyncio.wait_for(self.reader.get(KEY), self.settings.read_timeout_ms / 1000)
        except (RedisError, OSError, asyncio.TimeoutError) as exc:
            raise GhostUnavailable('redis_unavailable') from exc
        if raw is None:
            raise GhostUnavailable('no_current_settlement')
        try:
            read = bind_read_clock(parse_settlement_payload(raw), wall_ns=self.wall_ns(), monotonic_ns=self.monotonic_ns())
        except InvalidGhostPayload as exc:
            raise GhostUnavailable('invalid_publication') from exc
        if not read.is_fresh(wall_ns=self.wall_ns(), monotonic_ns=self.monotonic_ns()):
            raise GhostUnavailable('expired')
        return read


def create_settlement_api_service(settings, config):
    connection = dict(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB,
                      decode_responses=False, socket_connect_timeout=1, socket_keepalive=True)
    return SettlementApiService(config,
        Redis(**connection, socket_timeout=config.read_timeout_ms / 1000, max_connections=4),
        Redis(**connection, socket_timeout=None, max_connections=1))


def _service(request):
    service = getattr(request.app.state, 'settlement_api', None)
    return service if service is not None and service.settings.enabled else None


@router.get('/live', response_class=Response)
async def settlement_live(request: Request):
    service = _service(request)
    if service is None:
        return unavailable(getattr(request.app.state, 'settlement_api_disabled_reason', 'disabled'))
    try:
        read = await service.get_snapshot()
    except GhostUnavailable as exc:
        return unavailable(exc.reason)
    wall, mono = service.wall_ns(), service.monotonic_ns()
    remaining = read.remaining_ns(wall_ns=wall, monotonic_ns=mono)
    if remaining <= 0:
        return unavailable('expired')
    return Response(read.payload.raw, media_type='application/json', headers={**HEADERS,
        'X-Ghost-API-Time-Ns': str(wall), 'X-Ghost-Remaining-Ns': str(remaining),
        'X-Ghost-Run-Id': read.payload.run_id, 'X-Ghost-Decision-Id': str(read.payload.decision_id)})


@router.get('/stream', response_class=Response)
async def settlement_stream(request: Request):
    service = _service(request)
    if service is None:
        return unavailable(getattr(request.app.state, 'settlement_api_disabled_reason', 'disabled'))
    try:
        client = service.hub.register()
    except TooManyGhostClients:
        return unavailable('client_limit')
    except GhostStreamClosed:
        return unavailable('shutting_down')
    return GhostStreamResponse(service, client, event_name='settlement')


@router.get('/report', response_class=Response)
async def settlement_report(request: Request):
    service = _service(request)
    if service is None:
        return unavailable('disabled')
    try:
        raw, status = await service._cached_summary(REPORT_KEY, 512 * 1024, 'settlement_report')
    except GhostUnavailable as exc:
        return unavailable(exc.reason)
    return Response(raw, status_code=status, media_type='application/json', headers=HEADERS)
