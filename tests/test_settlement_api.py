import asyncio
from copy import deepcopy
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import price_collector.api as api
from price_collector.ghost_twap_api import GhostCompressionBypass
from price_collector.ghost_twap_payload import InvalidGhostPayload, bind_read_clock
from price_collector.settlement import public_payload
from price_collector.settlement_api import SettlementApiService, SettlementApiSettings, router
from price_collector.settlement_wire import KEY, parse_settlement_payload
from test_ghost_twap_api import AsgiRequest, PostgreSQLTrap, Reader
from test_settlement import END, NS, decision, project


def payload():
    body = public_payload(project())
    body.update(publication_state='attempted',
        publication_attempt_wall_ns=str(int(body['decision_wall_ns']) + 10 * NS),
        publication_attempt_monotonic_ns=str(int(body['decision_monotonic_ns']) + 10 * NS))
    return body


def encoded(body):
    return json.dumps(body, sort_keys=True, separators=(',', ':')).encode()


def service_for(body=None):
    body = payload() if body is None else body
    clock = {'wall': int(body['decision_wall_ns']) + 20 * NS, 'mono': 123456000000}
    reader = Reader(encoded(body))
    service = SettlementApiService(SettlementApiSettings(enabled=True), reader, Reader(None),
        wall_ns=lambda: clock['wall'], monotonic_ns=lambda: clock['mono'])
    app = FastAPI()
    app.include_router(router)
    app.add_middleware(GhostCompressionBypass, minimum_size=1)
    app.state.settlement_api, app.state.pool = service, PostgreSQLTrap()
    return service, app, reader, clock


def test_wire_accepts_real_pure_projection_and_preserves_exact_bytes():
    raw = encoded(payload())
    parsed = parse_settlement_payload(raw)
    assert parsed.raw is raw
    assert parsed.forecasts[0].target_source_timestamp_ms == END
    assert parsed.forecasts[0].horizon_s == 32
    assert parsed.current_twap.value == '100.010000000000000000'
    assert parsed.has_eligible_prices


@pytest.mark.parametrize('fault', ['market_start', 'window', 'source_url', 'params', 'http_status',
    'prestart_reference', 'reference_availability', 'baseline_price', 'side', 'lead', 'qualifies',
    'expiry_after_close', 'attempt_at_close', 'carry_count'])
def test_wire_rejects_identity_causal_and_arithmetic_tampering(fault):
    body = payload()
    reference = body['reference']
    if fault == 'market_start': reference['market_start_ms'] -= 300000
    elif fault == 'window': reference['settlement_window_s'] = 30
    elif fault == 'source_url': reference['source_url'] = 'https://example.test/'
    elif fault == 'params': reference['request_params']['eventStartTime'] = '2026-09-16T21:05:00Z'
    elif fault == 'http_status': reference['http_status'] = 500
    elif fault == 'prestart_reference': reference['requested_wall_ns'] = str((body['market_start_ms'] - 1) * NS)
    elif fault == 'reference_availability': reference['available_wall_ns'] = str(int(body['decision_wall_ns']) + 1)
    elif fault == 'baseline_price': body['signals']['twap']['price'] = '101.000000000000000000'
    elif fault == 'side': body['signals']['ghost']['side'] = 'down'
    elif fault == 'lead': body['signals']['ghost']['signed_lead_usd'] = '1.000000000000000000'
    elif fault == 'qualifies': body['signals']['twap']['qualifies'] = True
    elif fault == 'expiry_after_close':
        body['valid_until_wall_ns'] = str((END + 1) * NS)
        body['valid_until_ms'] = END + 1
    elif fault == 'attempt_at_close': body['publication_attempt_wall_ns'] = str(END * NS)
    else: body['max_interior_carry_ms'] = 1000
    with pytest.raises(InvalidGhostPayload): parse_settlement_payload(encoded(body))


def test_redis_only_snapshot_returns_original_json_and_expires_at_market_end():
    service, app, reader, clock = service_for()
    client = TestClient(app)
    response = client.get('/forecasts/chainlink-twap/settlement/live', headers={'Accept-Encoding': 'gzip'})
    assert response.status_code == 200 and response.content == reader.raw
    assert reader.calls == [KEY] and 'content-encoding' not in response.headers
    assert 'no-store' in response.headers['cache-control']
    assert int(response.headers['x-ghost-remaining-ns']) > 0
    clock['wall'] = END * NS
    clock['mono'] += 30000 * NS
    expired = client.get('/forecasts/chainlink-twap/settlement/live')
    assert expired.status_code == 503 and expired.json()['reason'] == 'expired'


def test_stream_uses_separate_event_and_same_original_projection():
    async def run():
        service, app, reader, clock = service_for()
        read = bind_read_clock(parse_settlement_payload(reader.raw), wall_ns=clock['wall'], monotonic_ns=clock['mono'])
        service.hub._accept(read, authoritative=True, resync=True)
        async def stop_after_frame(request, message):
            if message['type'] == 'http.response.body' and b'event: settlement' in message.get('body', b''):
                request.disconnect()
        request = AsgiRequest('/forecasts/chainlink-twap/settlement/stream', on_send=stop_after_frame)
        await asyncio.wait_for(request.invoke(app), timeout=2)
        assert b'event: settlement\n' in request.body and b'event: ghost\n' not in request.body
        assert b'"kind":"settlement"' in request.body
        assert service.hub.client_count == 0
    asyncio.run(run())


def test_bad_optional_api_settings_disable_only_settlement(monkeypatch):
    class Pool:
        async def close(self): pass
    class Cache:
        async def close(self): pass
    async def create_pool(settings): return Pool()
    async def health(pool): return None
    monkeypatch.setenv('DATABASE_URL', 'postgresql://price_reader:placeholder@127.0.0.1:5432/price_collector')
    monkeypatch.setenv('GHOST_TWAP_API_ENABLED', 'false')
    monkeypatch.setenv('SETTLEMENT_API_ENABLED', 'true')
    monkeypatch.setenv('SETTLEMENT_API_READ_TIMEOUT_MS', 'not-an-integer')
    monkeypatch.setattr(api, 'create_read_pool', create_pool)
    monkeypatch.setattr(api, 'create_live_cache', lambda settings: Cache())
    monkeypatch.setattr(api, 'health_check', health)
    with TestClient(api.app) as client:
        assert client.get('/healthz').status_code == 200
        response = client.get('/forecasts/chainlink-twap/settlement/live')
        assert response.status_code == 503
        assert response.json()['reason'] == 'invalid_settings'
