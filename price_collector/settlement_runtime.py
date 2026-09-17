"""Bounded optional settlement work inside the existing Chainlink worker.

The frozen decision reaches a durable outbox before Redis. Database persistence,
outcome matching and reporting run independently of publication and feed receipt.
Nothing here places orders or declares a calibrated winner probability.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict, Counter, deque
from contextlib import suppress
from copy import deepcopy
from decimal import Decimal, localcontext
from functools import partial
import json
import logging

from price_collector.ghost_twap import MATH_CONTEXT, _event_record, _json_bytes
from price_collector.ghost_twap_spool import GhostSpool
from price_collector.settlement import (
    SettlementSettings, build_projection, public_payload, decode_context, CONTEXT_KEY,
    SETTLEMENT_KEY, SETTLEMENT_CHANNEL,
)
from price_collector.settlement_wire import REPORT_KEY, parse_settlement_payload

LOGGER = logging.getLogger(__name__)
NS_MS = 1_000_000
MAX_RECORDS = 512
MAX_BYTES = 131072
MATCH_MS = 120_000
PUBLISH = """
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
if now >= tonumber(ARGV[2]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PXAT', ARGV[2])
redis.call('PUBLISH', KEYS[2], ARGV[1])
return 1
"""


class SettlementRuntime:
    def __init__(self, parent, settings, store, spool):
        self.parent, self.settings, self.store, self.spool = parent, settings, store, spool
        self.redis, self.wall_ns, self.mono_ns = parent.redis, parent.wall_ns, parent.mono_ns
        self.records = OrderedDict()
        self.pending = deque()
        self.dirty = set()
        self.counters = Counter()
        self.context = None
        self.context_raw = None
        self.context_wall = self.context_mono = 0
        self.openings = OrderedDict()
        self.guard = None
        self.guard_mono = 0
        self._wake = asyncio.Event()
        self._spool_lock = asyncio.Lock()
        self._tasks = []
        self._closed = False
        self._closing = None
        self._fault = None
        self._last_maintenance = 0

    async def _owned_thread(self, method, *args):
        task = asyncio.get_running_loop().run_in_executor(None, partial(method, *args))
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _disk(self, method, *args):
        async with self._spool_lock:
            return await self._owned_thread(method, *args)

    @staticmethod
    def record(row):
        p = row['projection']
        return dict(run_id=p['run_id'], decision_id=p['decision_id'], created_ms=p['decision_time_ms'],
                    decision_wall_ns=int(p['decision_wall_ns']), frozen_json=row['frozen_json'],
                    state_json=_json_bytes(row['state']).decode(), version=row['version'], terminal=row['terminal'])

    async def _save(self, row):
        # Freeze the latest version only after acquiring the filesystem owner.
        async with self._spool_lock:
            record = self.record(row)
            await self._owned_thread(self.spool.write, record)
            return record

    def _changed(self, row):
        row['version'] += 1
        self.dirty.add(row['projection']['decision_id'])

    async def start(self):
        await self._disk(self.spool.open)
        for record in await self._disk(self.spool.read_all):
            if record['created_ms'] <= self.wall_ns() // NS_MS - 7 * 86_400_000:
                # The owner's expiry applies even to a long-disabled outbox.
                await self._disk(self.spool.remove, record)
                self.counters['expired_outbox_records'] += 1
                continue
            state = json.loads(record['state_json'])
            publication = state.setdefault('publication', {})
            if publication.get('status') in ('reserved', 'attempted'):
                publication.update(status='unconfirmed', eligible_before_close=False)
            target = state.setdefault('target', {})
            if target.get('first_event') is None:
                target['status'] = 'restart_unmatched'
            state['recovered_without_republication'] = True
            record.update(state_json=_json_bytes(state).decode(), terminal=True, version=record['version'] + 1)
            await self._disk(self.spool.write, record)
            await self.store.persist(record)
            await self._disk(self.spool.remove, record)
        await self.refresh_guard()
        self._tasks = [asyncio.create_task(self._context_loop(), name='settlement-context'),
                       asyncio.create_task(self._publication_loop(), name='settlement-publication'),
                       asyncio.create_task(self._maintenance_loop(), name='settlement-audit')]

    async def refresh_guard(self):
        self.guard = await self.store.guard()
        self.guard_mono = self.mono_ns()

    def _safe(self, epoch=None):
        mono = self.mono_ns()
        parent = self.parent
        return (not self._closed and not self._fault and not parent._closed
                and not parent.stop_reason and not parent.suspensions
                and (epoch is None or epoch == parent._publication_epoch)
                and self.guard is not None and self.guard.get('capacity_ok') is True
                and 0 <= mono - self.guard_mono <= 30_000 * NS_MS
                and parent.guard is not None
                and 0 <= mono - parent.guard['monotonic_ns'] <= 3000 * NS_MS)

    def offer(self, decision, epoch):
        now = decision.decision_wall_ns // NS_MS
        remaining = 300_000 - now % 300_000
        if remaining > 30_000 or self._closed:
            return
        if not self._safe(epoch) or len(self.records) >= MAX_RECORDS:
            self.counters['admission_paused'] += 1
            return
        try:
            p = build_projection(decision, self.context, self.context_wall, self.context_mono)
            p['evaluation_start_ms'] = self.settings.evaluation_start_ms
            opening = self.openings.get(p['market_start_ms'])
            p['opening_stream'] = deepcopy(opening)
            p['opening_stream_difference_usd'] = None
            if opening and p['reference'].get('price_to_beat') is not None:
                with localcontext(MATH_CONTEXT):
                    p['opening_stream_difference_usd'] = format(
                        Decimal(opening['value']) - Decimal(p['reference']['price_to_beat']), '.18f')
            frozen = _json_bytes(p).decode()
            row = dict(projection=p, frozen_json=frozen, version=1, terminal=False, busy=False,
                state=dict(publication_epoch=epoch,
                           publication=dict(status='reserved', eligible_before_close=False),
                           target=dict(status='pending', first_event=None, conflicted=False)))
            if len(_json_bytes(self.record(row))) + 16_384 > MAX_BYTES:
                raise ValueError('settlement record byte budget')
        except Exception:
            self._fault = 'calculation_or_record_failure'
            LOGGER.exception('settlement calculation paused')
            return
        self.records[p['decision_id']] = row
        self.pending.append(p['decision_id'])
        self.dirty.add(p['decision_id'])
        self.counters['decisions'] += 1
        self._wake.set()

    def observe_target(self, event):
        if event.feed != 'twap' or event.source_timestamp_ms * NS_MS > event.received_wall_ns:
            return
        serialized = json.loads(_json_bytes(_event_record(event)))
        if event.source_timestamp_ms % 300_000 == 0:
            self.openings.setdefault(event.source_timestamp_ms, serialized)
            while len(self.openings) > 4:
                self.openings.popitem(last=False)
        for row in self.records.values():
            p, target = row['projection'], row['state']['target']
            if (row['terminal'] or p['target_source_timestamp_ms'] != event.source_timestamp_ms
                    or event.received_wall_ns < int(p['decision_wall_ns'])
                    or event.received_monotonic_ns < int(p['decision_monotonic_ns'])):
                continue
            if target['first_event'] is None:
                target.update(status='matched', first_event=serialized)
                self._changed(row)
            elif Decimal(target['first_event']['value']) != event.value and not target['conflicted']:
                target.update(conflicted=True, first_conflicting_event=serialized)
                self._changed(row)

    async def read_context(self):
        try:
            raw = await asyncio.wait_for(self.redis.get(CONTEXT_KEY), .5)
            if raw is None:
                self.context = self.context_raw = None
                return
            if raw == self.context_raw:
                return
            parsed = decode_context(raw)
            self.context_wall, self.context_mono = self.wall_ns(), self.mono_ns()
            self.context, self.context_raw = parsed, raw
        except Exception:
            self.context = self.context_raw = None
            self.counters['context_read_errors'] += 1

    async def _context_loop(self):
        while not self._closed:
            await self.read_context()
            await asyncio.sleep(.5)

    def _eligible_now(self, row):
        p = row['projection']
        wall, mono = self.wall_ns(), self.mono_ns()
        remaining = int(p['valid_until_wall_ns']) - int(p['decision_wall_ns'])
        # Any newly observed reference conflict also fences an older decision.
        context = self.context
        return (self._safe(row['state']['publication_epoch']) and p['status'] == 'available'
                and int(p['decision_wall_ns']) <= wall < int(p['valid_until_wall_ns'])
                and 0 <= mono - int(p['decision_monotonic_ns']) < remaining
                and context is not None and context.get('status') == 'available'
                and context.get('market_id') == p['market_id'] and context.get('conflicted') is False
                and context.get('price_to_beat') == p['reference'].get('price_to_beat'))

    async def publish_one(self, row):
        row['busy'] = True
        try:
            # Frozen input evidence must survive before any publish can happen.
            await self._save(row)
            publication = row['state']['publication']
            if not self._eligible_now(row):
                publication.update(status='withheld', reason='unavailable_or_expired', eligible_before_close=False)
            else:
                wall, mono = self.wall_ns(), self.mono_ns()
                payload = public_payload(row['projection'])
                payload.update(publication_state='attempted', publication_attempt_wall_ns=str(wall),
                               publication_attempt_monotonic_ns=str(mono))
                raw = _json_bytes(payload)
                parse_settlement_payload(raw)
                publication.update(status='attempted', attempt_wall_ns=wall, attempt_monotonic_ns=mono,
                                   attempted_payload=raw.decode())
                try:
                    published = await asyncio.wait_for(self.redis.eval(PUBLISH, 2,
                        SETTLEMENT_KEY, SETTLEMENT_CHANNEL, raw, row['projection']['valid_until_ms']), .5)
                except Exception:
                    publication.update(status='unconfirmed', eligible_before_close=False)
                    self.counters['publication_unconfirmed'] += 1
                else:
                    ack_wall, ack_mono = self.wall_ns(), self.mono_ns()
                    projection = row['projection']
                    lifetime = int(projection['valid_until_wall_ns']) - int(projection['decision_wall_ns'])
                    publication.update(status='acknowledged' if published == 1 else 'expired_before_set',
                        ack_wall_ns=ack_wall, ack_monotonic_ns=ack_mono,
                        eligible_before_close=published == 1
                            and wall <= ack_wall < int(projection['valid_until_wall_ns'])
                            and mono <= ack_mono < int(projection['decision_monotonic_ns']) + lifetime,
                        changed_during_flight=not self._eligible_now(row))
                    self.counters[publication['status']] += 1
            self._changed(row)
            await self._save(row)
        except Exception:
            # Stop new settlement publication, preserving the outbox as the
            # authority. The ordinary six-horizon producer remains independent.
            self._fault = 'publication_or_outbox_failure'
            LOGGER.exception('settlement publication paused')
        finally:
            row['busy'] = False

    async def _publication_loop(self):
        while not self._closed:
            if not self.pending:
                self._wake.clear()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), .5)
                continue
            key = self.pending.popleft()
            await self.publish_one(self.records[key])

    async def flush(self):
        now = self.wall_ns() // NS_MS
        for key, row in list(self.records.items()):
            if row['busy'] or row['state']['publication']['status'] == 'reserved':
                continue
            if not row['terminal'] and now >= row['projection']['market_end_ms'] + MATCH_MS:
                row['terminal'] = True
                if row['state']['target']['first_event'] is None:
                    row['state']['target']['status'] = 'missing'
                self._changed(row)
            if key not in self.dirty:
                continue
            record = await self._save(row)
            await self.store.persist(record)
            if row['version'] == record['version']:
                self.dirty.discard(key)
                if row['terminal']:
                    await self._disk(self.spool.remove, record)
                    del self.records[key]

    async def maintain(self):
        await self.refresh_guard()
        await self.flush()
        mono, now = self.mono_ns(), self.wall_ns() // NS_MS
        if not self._last_maintenance or mono - self._last_maintenance >= 30_000 * NS_MS:
            report = await self.store.maintain(now,
                persistence_complete=not self.dirty and not self.pending and not self._fault)
            attempted = self.wall_ns() // NS_MS
            envelope = dict(schema_version=1, status='available', generated_at_ms=now,
                cache_publish_attempt_ms=attempted, valid_until_ms=attempted + 180_000,
                evaluation=report, runtime=dict(counters=dict(self.counters), fault=self._fault,
                    pending_records=len(self.records), persistence_pending=len(self.dirty)))
            await asyncio.wait_for(self.redis.set(REPORT_KEY, _json_bytes(envelope), px=180_000), .5)
            self._last_maintenance = mono

    async def _maintenance_loop(self):
        while not self._closed:
            try:
                await self.maintain()
            except Exception:
                self.guard = None
                self.counters['maintenance_errors'] += 1
                if self.counters['maintenance_errors'] % 30 == 1:
                    LOGGER.exception('settlement maintenance unavailable')
            await asyncio.sleep(1)

    async def close(self):
        if self._closing is None:
            self._closing = asyncio.create_task(self._close(), name='settlement-close')
        await asyncio.shield(self._closing)

    async def _close(self):
        self._closed = True
        self._wake.set()
        for task in self._tasks:
            if task.get_name() != 'settlement-publication':
                task.cancel()
        # Publication has a bounded Redis socket timeout; settle ownership before
        # freezing the shutdown records. Do not cancel an fsync thread mid-write.
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for row in self.records.values():
            publication = row['state']['publication']
            if publication['status'] == 'reserved':
                publication.update(status='withheld', reason='shutdown', eligible_before_close=False)
            row['terminal'] = True
            if row['state']['target']['first_event'] is None:
                row['state']['target']['status'] = 'shutdown_unmatched'
            self._changed(row)
            await self._save(row)
        try:
            await asyncio.wait_for(self.flush(), 5)
        except (Exception, asyncio.CancelledError):
            LOGGER.warning('settlement shutdown retains durable outbox')
        await self._disk(self.spool.close)


async def attach_settlement(parent, pool):
    """Failure of this optional feature never disables the existing producer."""
    from price_collector.settlement_store import SettlementStore
    runtime = None
    try:
        config = SettlementSettings()
        if not config.enabled:
            return
        if not parent.settings.continuous:
            raise ValueError('settlement requires the continuous ghost worker')
        runtime = SettlementRuntime(parent, config, SettlementStore(pool, config.evaluation_start_ms),
            GhostSpool(parent.settings.state_directory / 'settlement', MAX_RECORDS, MAX_BYTES))
        await runtime.start()
        parent.settlement = runtime
    except BaseException as exc:
        LOGGER.exception('settlement feature disabled after startup failure')
        if runtime is not None:
            await runtime.close()
        if isinstance(exc, asyncio.CancelledError):
            raise
