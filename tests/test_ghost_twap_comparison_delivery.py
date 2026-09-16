"""Bounded chart-cache delivery stays separate from live forecast publication."""
import asyncio
import json

import pytest

import price_collector.ghost_twap_monitor as monitor_module
from price_collector.ghost_twap_api import ACCURACY_KEY, COMPARISON_KEY
from price_collector.ghost_twap_monitor import GhostMonitor
from test_ghost_twap_api import AsgiRequest, application, no_store, service_for


NOW_MS = 1_800_000_000_000


class ComparisonStore:
    def __init__(self):
        self.calls = []
        self.error = None

    async def comparison_snapshot(self, now_ms, *, runtime_watermark_ms):
        self.calls.append((now_ms, runtime_watermark_ms))
        if self.error is not None:
            raise self.error
        end = now_ms - 125_000
        return dict(raw_rows=[], row_count=0, body_bytes=0,
                    window_start_ms=end - 900_000, window_end_ms=end,
                    persistence_watermark_ms=now_ms - 120_000,
                    runtime_watermark_ms=runtime_watermark_ms)


class Cache:
    def __init__(self):
        self.calls = []
        self.error = None
        self.acknowledged = True

    async def set(self, key, raw, *, ex):
        self.calls.append((key, raw, ex))
        if self.error is not None:
            raise self.error
        return self.acknowledged


def make_monitor(*, wall_ns=lambda: NOW_MS * 1_000_000, runtime=None):
    store, cache = ComparisonStore(), Cache()
    monitor = GhostMonitor(store, cache, wall_ns=wall_ns,
        runtime_health=lambda: runtime if runtime is not None else
        {'persistence_watermark_ms': NOW_MS - 120_000})
    return monitor, store, cache


@pytest.mark.parametrize('watermark', [None, NOW_MS - 150_000])
def test_monitor_passes_live_watermark_and_publishes_bounded_typed_snapshot(watermark):
    async def scenario():
        monitor, store, cache = make_monitor(runtime={'persistence_watermark_ms': watermark})
        try:
            await monitor._publish_comparison()
            assert store.calls == [(NOW_MS, watermark)]
            assert len(cache.calls) == 1
            key, raw, ttl = cache.calls[0]
            body = json.loads(raw)
            assert (key, ttl) == (COMPARISON_KEY, 180)
            assert body['status'] == 'unavailable'
            assert body['reason'] == 'no_compacted_predictions'
            assert body['runtime_watermark_ms'] == watermark
            assert body['window_duration_ms'] == 900_000
            assert [item['horizon_s'] for item in body['horizons']] == [3, 5, 10, 30]
            assert body['valid_until_ms'] == NOW_MS + 180_000
            assert monitor.snapshot_health()['comparison'] == {
                'last_cache_ack_ms': NOW_MS, 'error': None}
        finally:
            await monitor.close()
    asyncio.run(scenario())


def test_comparison_failure_is_typed_and_does_not_mute_accuracy():
    async def scenario():
        monitor, store, cache = make_monitor()
        monitor._errors = {'maintenance': 'ExistingFailure'}
        monitor._last_publish_ack_ms = NOW_MS - 1000
        store.error = ValueError('invalid chart snapshot')
        try:
            await monitor._publish_comparison()
            assert monitor._errors == {'maintenance': 'ExistingFailure'}
            assert monitor._last_publish_ack_ms == NOW_MS - 1000
            assert monitor._failures == 0
            body = json.loads(cache.calls[0][1])
            assert body['status'] == 'unavailable'
            assert body['reason'] == 'comparison_snapshot_unavailable'
            assert monitor.snapshot_health()['comparison']['error'] == 'ValueError'
            # A later successful chart read clears only its own error.
            store.error = None
            await monitor._publish_comparison()
            assert monitor.snapshot_health()['comparison']['error'] is None
            assert monitor._errors == {'maintenance': 'ExistingFailure'}
        finally:
            await monitor.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['byte_budget', 'expired', 'redis_error', 'redis_nack'])
def test_comparison_publication_guards_fail_without_changing_accuracy(monkeypatch, failure):
    async def scenario():
        clocks = iter([NOW_MS, NOW_MS, NOW_MS + 180_000])
        monitor, _, cache = make_monitor(wall_ns=(lambda: next(clocks) * 1_000_000)
                                        if failure == 'expired' else lambda: NOW_MS * 1_000_000)
        if failure == 'byte_budget':
            monkeypatch.setattr(monitor_module, 'MAX_COMPARISON_BYTES', 64)
        elif failure == 'redis_error':
            cache.error = OSError('disconnected')
        elif failure == 'redis_nack':
            cache.acknowledged = False
        try:
            await monitor._publish_comparison()
            assert monitor._comparison_ack_ms is None
            assert monitor._comparison_error is not None
            assert monitor._errors == {} and monitor._failures == 0
            assert len(cache.calls) == (1 if failure.startswith('redis_') else 0)
        finally:
            await monitor.close()
    asyncio.run(scenario())


def summary_raw(now_ms, **overrides):
    body = dict(schema_version=1, generated_at_ms=now_ms,
                cache_publish_attempt_ms=now_ms, valid_until_ms=now_ms + 180_000,
                status='available', price='81120.123456789012345678')
    body.update(overrides)
    # Intentional whitespace verifies the API returns original bytes rather
    # than decoding and re-encoding financial strings or the full payload.
    return json.dumps(body, indent=2).encode()


def test_comparison_and_existing_accuracy_each_read_one_key_and_preserve_bytes():
    async def scenario():
        service, clock, reader = service_for()
        reader.raw = summary_raw(clock.wall // 1_000_000)
        for path, key in (('comparison', COMPARISON_KEY), ('accuracy', ACCURACY_KEY)):
            reader.calls.clear()
            request = AsgiRequest('/forecasts/chainlink-twap/' + path)
            await request.invoke(application(service))  # PostgreSQLTrap is installed.
            assert request.start['status'] == 200
            assert request.body == reader.raw
            assert reader.calls == [key]
            no_store(request.start['headers'])
    asyncio.run(scenario())


def test_comparison_unavailable_preserves_payload_and_expiry_is_exclusive():
    async def scenario():
        service, clock, reader = service_for()
        now_ms = clock.wall // 1_000_000
        reader.raw = summary_raw(now_ms, status='unavailable', reason='no_compacted_predictions')
        request = AsgiRequest('/forecasts/chainlink-twap/comparison')
        await request.invoke(application(service))
        assert request.start['status'] == 503 and request.body == reader.raw
        clock.advance(180_000_000_000)
        expired = AsgiRequest('/forecasts/chainlink-twap/comparison')
        await expired.invoke(application(service))
        assert expired.start['status'] == 503
        assert json.loads(expired.body)['reason'] == 'invalid_or_expired_comparison_snapshot'
        no_store(expired.start['headers'])
    asyncio.run(scenario())


def test_comparison_disabled_performs_no_cache_read():
    async def scenario():
        service, _, reader = service_for(b'not json')
        service.settings.enabled = False
        request = AsgiRequest('/forecasts/chainlink-twap/comparison')
        await request.invoke(application(service))
        assert request.start['status'] == 503
        assert json.loads(request.body)['reason'] == 'disabled'
        assert reader.calls == []
        no_store(request.start['headers'])
    asyncio.run(scenario())


def test_comparison_has_separate_byte_budget_without_relaxing_accuracy():
    async def scenario():
        service, clock, reader = service_for()
        reader.raw = summary_raw(clock.wall // 1_000_000, padding='x' * (512 * 1024))
        request = AsgiRequest('/forecasts/chainlink-twap/comparison')
        await request.invoke(application(service))
        assert request.start['status'] == 200 and request.body == reader.raw
        accuracy = AsgiRequest('/forecasts/chainlink-twap/accuracy')
        await accuracy.invoke(application(service))
        assert accuracy.start['status'] == 503
        reader.raw = summary_raw(clock.wall // 1_000_000, padding='x' * (4 * 1024 * 1024))
        oversized = AsgiRequest('/forecasts/chainlink-twap/comparison')
        await oversized.invoke(application(service))
        assert oversized.start['status'] == 503
        assert json.loads(oversized.body)['reason'] == 'invalid_or_expired_comparison_snapshot'
    asyncio.run(scenario())
