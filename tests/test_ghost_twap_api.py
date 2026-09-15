"""Independent snapshot and real-ASGI stream boundary regressions."""
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
import json
import logging
import socket
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

import price_collector.api as api
from price_collector.ghost_twap_api import (
    GHOST_KEY, GhostApiDisabledReason, GhostApiService, GhostApiSettings, GhostCompressionBypass, router,
)
from price_collector.ghost_twap_payload import bind_read_clock, parse_ghost_payload
from test_ghost_twap_payload import API_MONO_NS, READ_WALL_NS, raw_payload, wire_payload


class AsgiRequest:
    """Drive streaming sends directly; buffered HTTP clients conceal stalls."""
    def __init__(self, path, *, headers=(), on_send=None):
        self.path = path
        self.headers = list(headers)
        self.on_send = on_send
        self.sent = []
        self.events = asyncio.Queue()
        self.events.put_nowait({'type': 'http.request', 'body': b'', 'more_body': False})

    def disconnect(self):
        self.events.put_nowait({'type': 'http.disconnect'})

    async def send(self, message):
        self.sent.append(deepcopy(message))
        if self.on_send is not None:
            await self.on_send(self, message)

    async def invoke(self, application):
        scope = {
            'type': 'http', 'asgi': {'version': '3.0', 'spec_version': '2.3'},
            'http_version': '1.1', 'method': 'GET', 'scheme': 'http',
            'path': self.path, 'raw_path': self.path.encode(), 'query_string': b'',
            'root_path': '', 'headers': self.headers,
            'server': ('127.0.0.1', 9000), 'client': ('127.0.0.1', 40000),
        }
        await application(scope, self.events.get, self.send)

    @property
    def start(self):
        return next(m for m in self.sent if m['type'] == 'http.response.start')

    @property
    def body(self):
        return b''.join(m.get('body', b'') for m in self.sent if m['type'] == 'http.response.body')


def no_store(headers):
    headers = dict(headers)
    assert b'no-store' in headers[b'cache-control']
    assert b'no-transform' in headers[b'cache-control']


class PostgreSQLTrap:
    def acquire(self, *args, **kwargs):
        raise AssertionError('ghost delivery must not acquire PostgreSQL')


class Clock:
    def __init__(self):
        self.wall, self.mono = READ_WALL_NS, API_MONO_NS

    def advance(self, ns):
        self.wall += ns
        self.mono += ns


class Reader:
    def __init__(self, raw):
        self.raw = raw
        self.calls = []
        self.error = None
        self.block = False
        self.cancelled = False
        self.closed = False

    async def get(self, key):
        self.calls.append(key)
        if self.error:
            raise self.error
        if self.block:
            try:
                await asyncio.Future()
            finally:
                self.cancelled = True
        return self.raw

    async def aclose(self):
        self.closed = True

    def pubsub(self, *args, **kwargs):
        raise AssertionError('an endpoint must not create a per-request subscriber')


def service_for(raw=None, **settings):
    clock = Clock()
    reader, subscriber = Reader(raw), Reader(None)
    service = GhostApiService(GhostApiSettings(enabled=True, **settings), reader, subscriber,
                              wall_ns=lambda: clock.wall, monotonic_ns=lambda: clock.mono)
    return service, clock, reader


def application(service=None):
    app = FastAPI()
    app.include_router(router)
    app.add_middleware(GhostCompressionBypass, minimum_size=1)
    app.state.ghost_api = service
    app.state.pool = PostgreSQLTrap()
    return app


def seed(service, raw):
    read = bind_read_clock(parse_ghost_payload(raw), wall_ns=service.wall_ns(),
                           monotonic_ns=service.monotonic_ns())
    service.hub._accept(read, authoritative=True, resync=True)
    return read


def frames(request):
    return [b'\n'.join(line[6:] for line in chunk.split(b'\n') if line.startswith(b'data: '))
            for chunk in request.body.split(b'\n\n') if b'event: ghost\n' in chunk]


def test_settings_default_off_and_hard_bounds(monkeypatch):
    monkeypatch.delenv('GHOST_TWAP_API_ENABLED', raising=False)
    assert not GhostApiSettings().enabled
    for values in ({'max_clients': 17}, {'max_clients': 0}, {'read_timeout_ms': 1001},
                   {'read_timeout_ms': 9}, {'send_timeout_ms': 2001}, {'send_timeout_ms': 99}):
        with pytest.raises(ValueError):
            GhostApiSettings(**values)


@pytest.mark.parametrize('path', ['/live', '/stream'])
def test_disabled_endpoints_are_no_store_503_without_services(path):
    async def scenario():
        request = AsgiRequest('/forecasts/chainlink-twap' + path)
        await request.invoke(application())
        assert request.start['status'] == 503
        assert json.loads(request.body) == {'state': 'unavailable', 'reason': 'disabled'}
        no_store(request.start['headers'])
    asyncio.run(scenario())


@pytest.mark.parametrize('path', ['/live', '/stream'])
def test_attached_but_disabled_service_does_not_read_or_register(path):
    async def scenario():
        service, _, reader = service_for(raw_payload())
        service.settings.enabled = False
        request = AsgiRequest('/forecasts/chainlink-twap' + path)
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        assert request.start['status'] == 503
        assert json.loads(request.body)['reason'] == 'disabled'
        assert not reader.calls and service.hub.client_count == 0
        no_store(request.start['headers'])
    asyncio.run(scenario())


def test_actual_api_lifespan_default_off_never_constructs_ghost_clients(monkeypatch):
    class Resource(PostgreSQLTrap):
        closed = False
        async def close(self):
            self.closed = True

    pool, cache = Resource(), Resource()
    async def create_pool(settings):
        return pool
    def forbidden_factory(*args):
        raise AssertionError('default-off API must not construct Redis ghost resources')
    monkeypatch.setenv('DATABASE_URL', 'postgresql://price_reader:unused@127.0.0.1:5432/price_collector')
    monkeypatch.delenv('GHOST_TWAP_API_ENABLED', raising=False)
    monkeypatch.setattr(api, 'create_read_pool', create_pool)
    monkeypatch.setattr(api, 'create_live_cache', lambda settings: cache)
    monkeypatch.setattr(api, 'create_ghost_api_service', forbidden_factory)
    with TestClient(api.app) as client:
        for path in ('/live', '/stream'):
            response = client.get('/forecasts/chainlink-twap' + path)
            assert response.status_code == 503 and response.json()['reason'] == 'disabled'
    assert pool.closed and cache.closed


@pytest.mark.parametrize('fault', ['factory', 'start', 'close'])
def test_optional_ghost_lifecycle_failure_cleans_every_acquired_api_resource(monkeypatch, fault):
    acquired, closed = set(), set()

    class Resource:
        def __init__(self, name):
            self.name = name
            acquired.add(name)

        async def close(self):
            closed.add(self.name)

    class Ghost(Resource):
        async def start(self):
            if fault == 'start':
                raise RuntimeError('ghost start fault')

        async def close(self):
            await super().close()
            if fault == 'close':
                raise RuntimeError('ghost close fault')

    async def create_pool(settings):
        return Resource('pool')

    def create_ghost(*args):
        if fault == 'factory':
            raise RuntimeError('ghost factory fault')
        return Ghost('ghost')

    monkeypatch.setenv('DATABASE_URL', 'postgresql://price_reader:unused@127.0.0.1:5432/price_collector')
    monkeypatch.setenv('GHOST_TWAP_API_ENABLED', 'true')
    monkeypatch.setenv('GHOST_TWAP_API_MAX_CLIENTS', '16')
    monkeypatch.setattr(api, 'create_read_pool', create_pool)
    monkeypatch.setattr(api, 'create_live_cache', lambda settings: Resource('cache'))
    monkeypatch.setattr(api, 'create_ghost_api_service', create_ghost)

    async def scenario():
        with pytest.raises(RuntimeError, match='ghost ' + fault + ' fault'):
            async with api.lifespan(FastAPI()):
                pass
        assert acquired == closed
    asyncio.run(scenario())


@pytest.fixture
def core_api_resources(monkeypatch):
    class Resource(PostgreSQLTrap):
        def __init__(self):
            self.closed = False
            self.requested_keys = []

        async def close(self):
            self.closed = True

        async def get_prices(self, keys):
            self.requested_keys.append(list(keys))
            return {key: None for key in keys}

    pool, cache = Resource(), Resource()

    async def create_pool(settings):
        return pool

    async def health_check(actual_pool):
        assert actual_pool is pool

    monkeypatch.setenv('DATABASE_URL', 'postgresql://price_reader:unused@127.0.0.1:5432/price_collector')
    for field in ('ENABLED', 'MAX_CLIENTS', 'READ_TIMEOUT_MS', 'SEND_TIMEOUT_MS'):
        monkeypatch.delenv('GHOST_TWAP_API_' + field, raising=False)
    monkeypatch.setattr(api, 'create_read_pool', create_pool)
    monkeypatch.setattr(api, 'create_live_cache', lambda settings: cache)
    monkeypatch.setattr(api, 'health_check', health_check)
    return pool, cache


@pytest.mark.parametrize('enabled,field,value', [
    ('false', 'MAX_CLIENTS', '0'),
    ('true', 'MAX_CLIENTS', '17'),
    ('false', 'READ_TIMEOUT_MS', 'private-invalid-timeout'),
    ('true', 'SEND_TIMEOUT_MS', '2001'),
    ('private-invalid-enabled', 'MAX_CLIENTS', '16'),
])
def test_invalid_optional_settings_disable_only_ghost_with_safe_reason(
        monkeypatch, caplog, core_api_resources, enabled, field, value):
    pool, cache = core_api_resources
    monkeypatch.setenv('GHOST_TWAP_API_ENABLED', enabled)
    monkeypatch.setenv('GHOST_TWAP_API_' + field, value)

    def forbidden_factory(*args):
        raise AssertionError('invalid configuration must not fall back to operational defaults')

    monkeypatch.setattr(api, 'create_ghost_api_service', forbidden_factory)
    with caplog.at_level(logging.ERROR, logger=api.__name__):
        with TestClient(api.app) as client:
            assert api.app.state.ghost_api is None
            assert api.app.state.ghost_api_disabled_reason is GhostApiDisabledReason.INVALID_SETTINGS
            health = client.get('/healthz')
            assert health.status_code == 200 and health.json() == {
                'ok': True, 'database': 'ok', 'service': 'price-api',
            }
            live = client.get('/markets/current/live')
            assert live.status_code == 200
            assert set(live.json()['prices']) == {'binance_spot', 'chainlink', 'twap'}
            assert 'last' in live.json()['futures']
            assert cache.requested_keys == [[
                api.BINANCE_SPOT_LIVE_KEY, api.CHAINLINK_LIVE_KEY,
                api.TWAP_LIVE_KEY, api.FUTURES_LIVE_KEY,
            ]]
            for path in ('/live', '/stream'):
                response = client.get('/forecasts/chainlink-twap' + path)
                assert response.status_code == 503
                assert response.json() == {'state': 'unavailable', 'reason': 'invalid_settings'}
                assert 'no-store' in response.headers['cache-control']
    assert pool.closed and cache.closed
    records = [record for record in caplog.records if record.name == api.__name__]
    assert len(records) == 1
    assert records[0].getMessage() == 'Ghost API disabled: reason=invalid_settings error_type=ValidationError'
    assert records[0].exc_info is None and not records[0].args
    assert 'private-invalid' not in caplog.text


@pytest.mark.parametrize('enabled', ['false', 'true'])
def test_valid_settings_survive_prior_invalid_lifespan_without_fallback(
        monkeypatch, core_api_resources, enabled):
    monkeypatch.setenv('GHOST_TWAP_API_MAX_CLIENTS', '0')
    with TestClient(api.app) as client:
        assert client.get('/forecasts/chainlink-twap/live').json()['reason'] == 'invalid_settings'

    monkeypatch.setenv('GHOST_TWAP_API_ENABLED', enabled)
    monkeypatch.setenv('GHOST_TWAP_API_MAX_CLIENTS', '2')
    monkeypatch.setenv('GHOST_TWAP_API_READ_TIMEOUT_MS', '31')
    monkeypatch.setenv('GHOST_TWAP_API_SEND_TIMEOUT_MS', '499')
    constructed, started = [], []
    service, _, reader = service_for(raw_payload())

    async def start():
        started.append(True)
        seed(service, raw_payload())

    def factory(settings, ghost_settings):
        constructed.append(ghost_settings.model_dump())
        service.settings = ghost_settings
        return service

    monkeypatch.setattr(service, 'start', start)
    monkeypatch.setattr(api, 'create_ghost_api_service', factory)
    with TestClient(api.app) as client:
        response = client.get('/forecasts/chainlink-twap/live')
        assert api.app.state.ghost_api_disabled_reason is GhostApiDisabledReason.DISABLED
        if enabled == 'true':
            assert response.status_code == 200 and response.content == raw_payload()
            assert constructed == [{'enabled': True, 'max_clients': 2,
                                    'read_timeout_ms': 31, 'send_timeout_ms': 499}]
            assert started == [True] and reader.calls == [GHOST_KEY]
        else:
            assert response.status_code == 503 and response.json()['reason'] == 'disabled'
            assert not constructed and not started and not reader.calls
    if enabled == 'true':
        assert reader.closed and service.subscriber.closed


@pytest.mark.parametrize('fault', ['core_settings', 'ghost_unexpected', 'pool', 'cache'])
def test_optional_settings_isolation_does_not_swallow_other_startup_errors(
        monkeypatch, core_api_resources, fault):
    pool, cache = core_api_resources
    with pytest.raises(ValidationError) as caught:
        GhostApiSettings(max_clients=0)
    validation_error = caught.value

    def fail():
        if fault == 'core_settings':
            raise validation_error
        raise RuntimeError('unrelated startup fault')

    if fault == 'core_settings':
        monkeypatch.setattr(api, 'Settings', fail)
    elif fault == 'ghost_unexpected':
        monkeypatch.setattr(api, 'GhostApiSettings', fail)
    elif fault == 'pool':
        async def fail_pool(settings):
            fail()
        monkeypatch.setattr(api, 'create_read_pool', fail_pool)
    else:
        monkeypatch.setattr(api, 'create_live_cache', lambda settings: fail())
    with pytest.raises(ValidationError if fault == 'core_settings' else RuntimeError):
        with TestClient(api.app):
            raise AssertionError('startup error was swallowed')
    assert pool.closed is (fault == 'cache')
    assert not cache.closed


def test_get_returns_original_partial_payload_bytes_one_get_and_no_postgres():
    async def scenario():
        body = wire_payload()
        body['forecasts'][0].update(price=None, quality='unavailable',
                                    reasons=['target_received_before_publication'])
        body['publication_eligibility']['eligible_horizons'].remove(1)
        body['publication_eligibility']['excluded_horizons']['1'] = ['target_received_before_publication']
        raw = raw_payload(body)
        service, clock, reader = service_for(raw)
        request = AsgiRequest('/forecasts/chainlink-twap/live', headers=[(b'accept-encoding', b'gzip')])
        await request.invoke(application(service))
        assert request.start['status'] == 200 and request.body == raw
        assert reader.calls == [GHOST_KEY]
        assert service.hub.client_count == 0
        headers = dict(request.start['headers'])
        no_store(request.start['headers'])
        assert headers[b'content-type'] == b'application/json'
        assert b'content-encoding' not in headers
        assert int(headers[b'x-ghost-api-time-ns']) == clock.wall
        assert headers[b'x-ghost-run-id'] == b'wire-run'
        assert headers[b'x-ghost-decision-id'] == b'1'
        assert int(headers[b'x-ghost-remaining-ns']) == 1900 * 1_000_000
        assert json.loads(request.body)['forecasts'][1]['price'] == '100.000000000000000001'
    asyncio.run(scenario())


@pytest.mark.parametrize('kind,reason', [
    ('missing', 'no_current_publication'), ('invalid', 'invalid_publication'),
    ('expired', 'expired'), ('health', 'no_eligible_forecasts'),
])
def test_get_unavailable_reasons_are_503_without_pg_or_fallback(kind, reason):
    async def scenario():
        raw = None if kind == 'missing' else b'{' if kind == 'invalid' else raw_payload(missing_history=kind == 'health')
        service, clock, reader = service_for(raw)
        if kind == 'expired':
            clock.advance(1900 * 1_000_000)
        request = AsgiRequest('/forecasts/chainlink-twap/live')
        await request.invoke(application(service))
        assert request.start['status'] == 503
        assert json.loads(request.body) == {'state': 'unavailable', 'reason': reason}
        assert reader.calls == [GHOST_KEY]
        no_store(request.start['headers'])
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['timeout', 'connection'])
def test_get_redis_error_and_actual_read_deadline_fail_closed(failure):
    async def scenario():
        service, _, reader = service_for(raw_payload(), read_timeout_ms=10)
        if failure == 'timeout':
            reader.block = True
        else:
            reader.error = OSError('private connection detail')
        request = AsgiRequest('/forecasts/chainlink-twap/live')
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        assert request.start['status'] == 503
        assert json.loads(request.body)['reason'] == 'redis_unavailable'
        assert b'private' not in request.body and reader.calls == [GHOST_KEY]
        assert reader.cancelled is (failure == 'timeout')
        no_store(request.start['headers'])
    asyncio.run(scenario())


def test_stream_actual_asgi_preserves_bytes_resync_skips_and_disables_gzip():
    async def scenario():
        raw = raw_payload()
        service, _, reader = service_for(raw)
        read = seed(service, raw)
        count = 0
        async def receive_frames(request, message):
            nonlocal count
            if message['type'] != 'http.response.body':
                return
            count += 1
            if count == 1:
                service.hub._emit(read, 'intermediate')
                service.hub._emit(read, 'fresh')
            elif count == 2:
                service.hub.generation += 1
                service.hub._emit(read, 'resynchronized', resync=True)
            else:
                request.disconnect()
        request = AsgiRequest('/forecasts/chainlink-twap/stream',
            headers=[(b'accept-encoding', b'gzip'), (b'last-event-id', b'old:9:999')],
            on_send=receive_frames)
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        assert request.start['status'] == 200 and count == 3
        headers = dict(request.start['headers'])
        no_store(request.start['headers'])
        assert headers[b'content-type'].startswith(b'text/event-stream')
        assert headers[b'x-accel-buffering'] == b'no'
        assert b'content-encoding' not in headers and b'content-length' not in headers
        data = frames(request)
        assert len(data) == 3 and all(frame.endswith(b',"ghost":' + raw + b'}') for frame in data)
        metadata = [json.loads(frame)['api'] for frame in data]
        assert [m['resync'] for m in metadata] == [True, False, True]
        assert [m['skipped_updates'] for m in metadata] == [0, 1, 1]
        assert metadata[2]['generation'] > metadata[1]['generation']
        assert all(m['remaining_ns'] == '1900000000' for m in metadata)
        assert service.hub.client_count == 0 and reader.calls == []
    asyncio.run(scenario())


def test_stream_rechecks_expiry_before_actual_send_and_does_not_extend_it():
    async def scenario():
        raw = raw_payload()
        service, clock, _ = service_for(raw)
        seed(service, raw)
        async def expire_after_headers(request, message):
            if message['type'] == 'http.response.start':
                clock.advance(1900 * 1_000_000)
            else:
                request.disconnect()
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=expire_after_headers)
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        data, = frames(request)
        envelope = json.loads(data)
        assert envelope['ghost'] is None
        assert envelope['api']['state'] == 'unavailable'
        assert envelope['api']['reason'] == 'expired'
        assert envelope['api']['remaining_ns'] == '0'
        assert raw not in data and service.hub.client_count == 0
    asyncio.run(scenario())


def test_stream_preserves_explicit_resync_without_generation_change():
    async def scenario():
        service, _, _ = service_for(raw_payload())
        seed(service, raw_payload())
        received = 0
        async def resynchronize(request, message):
            nonlocal received
            if message['type'] != 'http.response.body':
                return
            received += 1
            if received == 1:
                service.hub._emit(None, 'resyncing', resync=True)
            else:
                request.disconnect()
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=resynchronize)
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        first, second = map(json.loads, frames(request))
        assert first['api']['generation'] == second['api']['generation']
        assert second['api']['resync'] is True
        assert second['api']['reason'] == 'resyncing' and second['ghost'] is None
        assert service.hub.client_count == 0
    asyncio.run(scenario())


def test_stream_multiline_json_has_valid_sse_data_framing_and_original_nested_bytes():
    async def scenario():
        raw = json.dumps(wire_payload(), indent=2).encode()
        assert b'\n' in raw
        service, _, _ = service_for(raw)
        seed(service, raw)
        async def disconnect(request, message):
            if message['type'] == 'http.response.body':
                request.disconnect()
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=disconnect)
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        data, = frames(request)
        assert data.endswith(b',"ghost":' + raw + b'}')
        assert json.loads(data)['ghost'] == json.loads(raw)
        assert all(line.startswith((b'id: ', b'event: ', b'data: '))
                   for line in request.body.split(b'\n') if line)
    asyncio.run(scenario())


@pytest.mark.parametrize('stall_type', ['http.response.start', 'http.response.body'])
def test_stream_times_out_the_actual_asgi_send_and_always_unregisters(stall_type):
    async def scenario():
        service, _, _ = service_for(raw_payload(), send_timeout_ms=100)
        seed(service, raw_payload())
        cancelled = False
        async def stalled_send(request, message):
            nonlocal cancelled
            if message['type'] == stall_type:
                try:
                    await asyncio.Future()
                finally:
                    cancelled = True
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=stalled_send)
        await asyncio.wait_for(request.invoke(application(service)), timeout=1)
        assert cancelled and service.hub.client_count == 0
    asyncio.run(scenario())


def test_stream_disconnect_while_waiting_for_update_releases_client():
    async def scenario():
        service, _, _ = service_for(raw_payload())
        seed(service, raw_payload())
        first = asyncio.Event()
        async def sent(request, message):
            if message['type'] == 'http.response.body':
                first.set()
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=sent)
        running = asyncio.create_task(request.invoke(application(service)))
        await asyncio.wait_for(first.wait(), timeout=1)
        assert service.hub.client_count == 1
        request.disconnect()
        await asyncio.wait_for(running, timeout=1)
        assert service.hub.client_count == 0
    asyncio.run(scenario())


def test_stream_real_hub_client_limit_is_503_and_disconnect_restores_slot():
    async def scenario():
        service, _, _ = service_for(raw_payload(), max_clients=1)
        seed(service, raw_payload())
        first = asyncio.Event()
        async def sent(request, message):
            if message['type'] == 'http.response.body':
                first.set()
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=sent)
        app = application(service)
        running = asyncio.create_task(request.invoke(app))
        await asyncio.wait_for(first.wait(), timeout=1)
        rejected = AsgiRequest('/forecasts/chainlink-twap/stream')
        await rejected.invoke(app)
        assert rejected.start['status'] == 503
        assert json.loads(rejected.body)['reason'] == 'client_limit'
        no_store(rejected.start['headers'])
        request.disconnect()
        await asyncio.wait_for(running, timeout=1)
        assert service.hub.client_count == 0
        async def stop_after_frame(request, message):
            if message['type'] == 'http.response.body':
                request.disconnect()
        replacement = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=stop_after_frame)
        await asyncio.wait_for(replacement.invoke(app), timeout=1)
        assert replacement.start['status'] == 200 and service.hub.client_count == 0
    asyncio.run(scenario())


def test_service_shutdown_gracefully_releases_existing_stream_and_rejects_new_clients():
    async def scenario():
        service, _, _ = service_for(raw_payload())
        seed(service, raw_payload())
        first = asyncio.Event()
        async def sent(request, message):
            if message['type'] == 'http.response.body':
                first.set()
        request = AsgiRequest('/forecasts/chainlink-twap/stream', on_send=sent)
        app = application(service)
        running = asyncio.create_task(request.invoke(app))
        await asyncio.wait_for(first.wait(), timeout=1)
        await service.close()
        await asyncio.wait_for(running, timeout=1)
        assert request.sent[-1] == {'type': 'http.response.body', 'body': b'', 'more_body': False}
        assert service.hub.client_count == 0
        assert service.reader.closed and service.subscriber.closed
        rejected = AsgiRequest('/forecasts/chainlink-twap/stream')
        await rejected.invoke(app)
        assert rejected.start['status'] == 503
        no_store(rejected.start['headers'])
    asyncio.run(scenario())


def test_real_loopback_uvicorn_delivers_first_uncompressed_sse_before_stream_ends():
    raw = raw_payload()
    ready = threading.Event()
    services, failures = [], []

    @asynccontextmanager
    async def lifespan(app):
        service, _, _ = service_for(raw)
        seed(service, raw)
        app.state.ghost_api = service
        app.state.pool = PostgreSQLTrap()
        services.append(service)
        ready.set()
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    app.add_middleware(GhostCompressionBypass, minimum_size=1)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('127.0.0.1', 0))
    listener.listen(4)
    port = listener.getsockname()[1]
    config = uvicorn.Config(app, host='127.0.0.1', port=port, loop='asyncio',
                            lifespan='on', access_log=False, log_level='critical',
                            timeout_graceful_shutdown=1)
    server = uvicorn.Server(config)

    def serve():
        try:
            server.run(sockets=[listener])
        except BaseException as exc:
            failures.append(exc)
            ready.set()

    worker = threading.Thread(target=serve, name='ghost-api-loopback-test', daemon=True)
    worker.start()
    try:
        assert ready.wait(3), 'isolated Uvicorn startup did not complete'
        assert not failures
        started = time.monotonic()
        with httpx.Client(timeout=httpx.Timeout(3, read=1), trust_env=False) as client:
            with client.stream('GET', f'http://127.0.0.1:{port}/forecasts/chainlink-twap/stream',
                               headers={'Accept-Encoding': 'gzip'}) as response:
                assert response.status_code == 200
                assert response.headers['content-type'].startswith('text/event-stream')
                assert 'content-encoding' not in response.headers
                assert 'content-length' not in response.headers
                assert response.headers['x-accel-buffering'] == 'no'
                assert 'no-store' in response.headers['cache-control']
                received = b''
                # Retain the iterator through the open-stream assertions:
                # generator cleanup can close its underlying HTTP stream.
                chunks = response.iter_raw()
                for chunk in chunks:
                    received += chunk
                    assert len(received) <= 128 * 1024
                    assert time.monotonic() - started < 3
                    if b'\n\n' in received:
                        break
                first = received.split(b'\n\n', 1)[0]
                data = b'\n'.join(line[6:] for line in first.split(b'\n') if line.startswith(b'data: '))
                assert data.endswith(b',"ghost":' + raw + b'}')
                assert json.loads(data)['api']['resync'] is True
                assert not response.is_closed
                assert worker.is_alive() and services[0].hub.client_count == 1
    finally:
        server.should_exit = True
        worker.join(3)
        if worker.is_alive():
            server.force_exit = True
            worker.join(2)
        listener.close()
    assert not worker.is_alive() and not failures
