"""Independent bounded maintenance and cached accuracy for continuous ghosts.

The collector owns the clients and the runtime-health callback. This worker
never reads a price feed, creates a forecast, or runs on an API request path.
Stopping forecast admission does not stop this worker. A caller must await
wait_closed() before releasing clients if close() reports incomplete cleanup.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
import json
import logging
import time
from typing import Any, Callable

from price_collector.ghost_twap_accuracy import compose_status

LOGGER = logging.getLogger(__name__)
ACCURACY_KEY = 'btc:live:ghost_chainlink_twap_60s:accuracy'
MAINTENANCE_SECONDS = 5
PUBLICATION_SECONDS = 60
CACHE_TTL_SECONDS = 180
STORE_TIMEOUT_SECONDS = 10
REDIS_TIMEOUT_SECONDS = 3
CLOSE_TIMEOUT_SECONDS = 10
MAX_PAYLOAD_BYTES = 512 * 1024
MAX_RUNTIME_HEALTH_BYTES = 256 * 1024


def _json_bytes(value: Any) -> bytes:
    def decimal_text(item):
        if isinstance(item, Decimal) and item.is_finite():
            return format(item, 'f')
        raise TypeError('Unsupported monitoring JSON value')
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      allow_nan=False, default=decimal_text).encode('utf-8')


class GhostMonitor:
    """One sequential task; its state is separate from publication admission."""

    def __init__(self, store: Any, redis: Any, *, runtime_health: Callable[[], dict],
                 active_identities: Callable[[], tuple] = lambda: (),
                 wall_ns=time.time_ns, monotonic_ns=time.monotonic_ns):
        self.store, self.redis = store, redis
        self.runtime_health = runtime_health
        self.active_identities = active_identities
        self.wall_ns, self.monotonic_ns = wall_ns, monotonic_ns
        self._task: asyncio.Task | None = None
        self._closing = False
        self._errors: dict[str, str] = {}
        self._last_maintenance_ms: int | None = None
        self._last_snapshot_ms: int | None = None
        self._last_publish_ack_ms: int | None = None
        self._maintenance_result: dict | None = None
        self._maintenance_runs = 0
        self._publications = 0
        self._failures = 0
        self._close_complete = False
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='ghost-accuracy')
        self._executor_closed = False

    def snapshot_health(self) -> dict:
        return dict(status='unavailable' if self._errors else
                    ('ready' if self._last_publish_ack_ms is not None else 'starting'),
                    running=self._task is not None and not self._task.done(),
                    closing=self._closing, close_complete=self._close_complete,
                    last_maintenance_ms=self._last_maintenance_ms,
                    last_snapshot_ms=self._last_snapshot_ms,
                    last_cache_ack_ms=self._last_publish_ack_ms,
                    maintenance_runs=self._maintenance_runs,
                    publications=self._publications, failures=self._failures,
                    errors=dict(self._errors),
                    maintenance=deepcopy(self._maintenance_result))

    def _failure(self, operation: str, exc: BaseException) -> None:
        kind = type(exc).__name__
        changed = self._errors.get(operation) != kind
        self._errors[operation] = kind
        self._failures += 1
        if changed:
            # Exception messages can contain connection strings. Never log them.
            LOGGER.warning('ghost_accuracy_monitor_unavailable operation=%s error_type=%s',
                           operation, kind)

    def _success(self, operation: str) -> None:
        if self._errors.pop(operation, None) is not None:
            LOGGER.info('ghost_accuracy_monitor_recovered operation=%s', operation)

    def _runtime_snapshot(self) -> dict:
        value = deepcopy(self.runtime_health())
        if not isinstance(value, dict):
            raise ValueError('Runtime monitoring health is not a bounded object')
        return value

    async def _cpu(self, function, *args):
        # Composition is bounded but can scan 90 days of hourly aggregates.
        # Keep it off the collector loop, with one owned job and no backlog.
        future = asyncio.get_running_loop().run_in_executor(self._executor, function, *args)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # Cancelling an asyncio wrapper cannot stop an executing thread.
            # Retain ownership until it actually exits before completing close.
            while not future.done():
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not future.cancelled():
                future.exception()
            raise

    @staticmethod
    def _metadata(value: Any) -> dict:
        if (not isinstance(value, dict) or type(value.get('schema_version')) is not int
                or value['schema_version'] != 1 or not isinstance(value.get('groups'), dict)):
            raise ValueError('Invalid persisted accuracy metadata')
        return value

    def _close_executor(self) -> None:
        if not self._executor_closed:
            self._executor.shutdown(wait=False)
            self._executor_closed = True

    async def start(self) -> None:
        if self._closing:
            raise RuntimeError('Closed monitor cannot be restarted')
        if self._task is not None:
            return
        self._task = asyncio.create_task(self.run(), name='ghost-retention-accuracy-monitor')

    async def _maintenance(self) -> None:
        try:
            # Capture ownership without an await: a terminal row still belongs
            # to the runtime until its latest commit and spool removal finish.
            # Never compact that row while an in-memory update can follow.
            identities = self.active_identities()
            if not isinstance(identities, (tuple, list)) or len(identities) > 512:
                raise ValueError('Invalid active audit identity bounds')
            identities = tuple(identities)
            result = await asyncio.wait_for(self.store.maintenance(
                self.wall_ns() // 1_000_000, limit=100, exclude=identities),
                timeout=STORE_TIMEOUT_SECONDS)
            if not isinstance(result, dict):
                raise ValueError('Invalid maintenance result')
            # Retain only the bounded counters promised by the store contract.
            counts = {name: result[name] for name in ('compacted', 'expired', 'summary_expired')}
            if 'feed_expired' in result:
                counts['feed_expired'] = result['feed_expired']
            if any(type(value) is not int or value < 0 for value in counts.values()):
                raise ValueError('Invalid maintenance counters')
            self._maintenance_result = counts
            self._last_maintenance_ms = self.wall_ns() // 1_000_000
            self._maintenance_runs += 1
            self._success('maintenance')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failure('maintenance', exc)

    async def _compose(self, now_ms: int) -> dict:
        runtime = self._runtime_snapshot()
        if len(await self._cpu(_json_bytes, runtime)) > MAX_RUNTIME_HEALTH_BYTES:
            raise ValueError('Runtime monitoring health is not a bounded object')
        feed_health = runtime.get('feed_health')
        if feed_health is not None:
            if not isinstance(feed_health, dict):
                raise ValueError('Invalid feed-health snapshot')
            await asyncio.wait_for(self.store.record_feed_health(feed_health),
                                   timeout=STORE_TIMEOUT_SECONDS)
        snapshot = await asyncio.wait_for(self.store.monitoring_snapshot(now_ms),
                                          timeout=STORE_TIMEOUT_SECONDS)
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get('hourly'), list):
            raise ValueError('Invalid stored monitoring snapshot')
        watermarks = [value for value in (runtime.get('persistence_watermark_ms'),
                      snapshot.get('persistence_watermark_ms')) if value is not None]
        if any(type(value) is not int or value < 0 for value in watermarks):
            raise ValueError('Invalid accuracy persistence watermark')
        # In-memory reservations can precede insertion; committed active rows
        # can outlive this process. Both prefixes must finish before an hour is
        # complete. A stale earlier watermark only delays classification.
        if watermarks:
            runtime['persistence_watermark_ms'] = min(watermarks)
        else:
            runtime.pop('persistence_watermark_ms', None)
        runtime['store_health'] = snapshot.get('health', {})
        runtime['accuracy_baseline'] = snapshot.get('accuracy_baseline')
        runtime['accuracy_warning_state'] = snapshot.get('accuracy_warning_state')
        status = await self._cpu(compose_status, snapshot['hourly'], now_ms, runtime)
        candidate = status.pop('_baseline_candidate', None)
        if candidate is not None and self._metadata(candidate)['groups']:
            # Store merges only missing group keys; existing accepted baselines
            # survive retries, later time windows and process restarts.
            runtime['accuracy_baseline'] = self._metadata(await asyncio.wait_for(
                self.store.save_baseline(candidate), timeout=STORE_TIMEOUT_SECONDS))
            status = await self._cpu(compose_status, snapshot['hourly'], now_ms, runtime)
            status.pop('_baseline_candidate', None)
        warning_state = status.pop('_warning_state', None)
        if warning_state is not None and warning_state != runtime.get('accuracy_warning_state'):
            persisted = self._metadata(await asyncio.wait_for(
                self.store.save_warning_state(self._metadata(warning_state)),
                timeout=STORE_TIMEOUT_SECONDS))
            if persisted != warning_state:
                runtime['accuracy_warning_state'] = persisted
                status = await self._cpu(compose_status, snapshot['hourly'], now_ms, runtime)
                status.pop('_baseline_candidate', None)
                status.pop('_warning_state', None)
        # Private persistence instructions never enter the live cache.
        if any(key.startswith('_') for key in status):
            raise ValueError('Unexpected private accuracy fields')
        self._last_snapshot_ms = now_ms
        return status

    async def _publish(self) -> None:
        now_ms = self.wall_ns() // 1_000_000
        try:
            status = await self._compose(now_ms)
            if not isinstance(status, dict):
                raise ValueError('Accuracy status must be an object')
            self._success('snapshot')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failure('snapshot', exc)
            status = dict(schema_version=1, generated_at_ms=now_ms,
                          status='unavailable', reason='monitoring_snapshot_unavailable')
        try:
            # This clock is an attempt, not a completed Redis publication. The
            # acknowledgement is retained locally for the next health snapshot.
            attempt_ms = self.wall_ns() // 1_000_000
            status['worker_health'] = self.snapshot_health()
            status.setdefault('status', 'available')
            status['cache_publish_attempt_ms'] = attempt_ms
            status['valid_until_ms'] = attempt_ms + CACHE_TTL_SECONDS * 1000
            if self._errors:
                status['status'] = 'unavailable'
                status['monitoring_unavailable_reasons'] = sorted(self._errors)
            payload = await self._cpu(_json_bytes, status)
            if len(payload) > MAX_PAYLOAD_BYTES:
                raise ValueError('Accuracy status exceeds cache byte budget')
            if not attempt_ms <= self.wall_ns() // 1_000_000 < status['valid_until_ms']:
                raise ValueError('Accuracy cache clock regressed or payload already expired')
            acknowledged = await asyncio.wait_for(self.redis.set(
                ACCURACY_KEY, payload, ex=CACHE_TTL_SECONDS), timeout=REDIS_TIMEOUT_SECONDS)
            if not acknowledged:
                raise RuntimeError('Redis did not acknowledge accuracy cache write')
            self._last_publish_ack_ms = self.wall_ns() // 1_000_000
            self._publications += 1
            self._success('cache')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failure('cache', exc)

    async def run(self) -> None:
        try:
            await self._run()
        finally:
            self._close_executor()

    async def _run(self) -> None:
        next_maintenance = next_publication = self.monotonic_ns()
        while not self._closing:
            try:
                now = self.monotonic_ns()
                if now >= next_maintenance:
                    await self._maintenance()
                    next_maintenance = self.monotonic_ns() + MAINTENANCE_SECONDS * 1_000_000_000
                if self._closing:
                    break
                if self.monotonic_ns() >= next_publication:
                    await self._publish()
                    next_publication = self.monotonic_ns() + PUBLICATION_SECONDS * 1_000_000_000
                # Skip elapsed intervals. Never fabricate catch-up cycles or
                # launch another maintenance call while one is in progress.
                remaining = min(next_maintenance, next_publication) - self.monotonic_ns()
                self._success('worker')
                await asyncio.sleep(max(0.05, remaining / 1_000_000_000))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._failure('worker', exc)
                await asyncio.sleep(MAINTENANCE_SECONDS)

    async def close(self) -> bool:
        self._closing = True
        task = self._task
        if task is None:
            self._close_executor()
            self._close_complete = True
            return True
        if not task.done():
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=CLOSE_TIMEOUT_SECONDS)
            if not done:
                LOGGER.error('ghost_accuracy_monitor_close_incomplete')
                return False
        await self.wait_closed()
        return True

    async def wait_closed(self) -> None:
        task = self._task
        if task is not None:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.done():
                    raise
            except Exception as exc:
                self._failure('worker', exc)
        self._close_complete = task is None or task.done()
