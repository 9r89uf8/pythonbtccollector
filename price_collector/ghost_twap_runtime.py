"""Optional receipt-causal ghost worker; all I/O is separate from feed readers."""
from __future__ import annotations

import asyncio
from collections import Counter, OrderedDict, deque
from dataclasses import dataclass
from decimal import Decimal, localcontext
from functools import partial
import json
import logging
from pathlib import Path
import shutil
import time
from typing import Any
from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from price_collector.ghost_twap import (
    Decision, GhostPolicy, GhostTwapEngine, HORIZONS, MATH_CONTEXT,
    NS_PER_MS, NS_PER_SECOND, PRICE_QUANTUM, PriceEvent, _event_record, _json_bytes,
)
from price_collector.ghost_twap_spool import GhostSpool

LOGGER = logging.getLogger(__name__)
GHOST_KEY = 'btc:live:ghost_chainlink_twap_60s'
GHOST_CHANNEL = 'btc:live:ghost_chainlink_twap_60s:updates'
RUNTIME_VERSION = 'ghost-canary-v2'
CANARY_MS = 4 * 60 * 60 * 1000
CAMPAIGN_CHECKPOINT_SECONDS = 30
AUDIT_BATCH_SECONDS = 2.5
MATCH_NS = 120 * NS_PER_SECOND
WARN_BYTES = 1024 ** 3
STOP_BYTES = 1536 * 1024 ** 2
BUDGET_BYTES = 2 * 1024 ** 3
MAX_ROWS = 600000
RESERVE_BYTES = 10 * 1024 ** 3
# Reserve space for publication bytes, six first results and bounded conflict/
# late annotations. Input IDs are bounded ASCII; all clocks fit signed bigint.
RESULT_GROWTH_RESERVE_BYTES = 64 * 1024
# No financial arithmetic; both operations use the same immutable bytes.
PUBLISH_LUA = """
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('PUBLISH', KEYS[2], ARGV[1])
return 1
"""


class GhostSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='GHOST_TWAP_', case_sensitive=False)
    enabled: bool = False
    canary_start_ms: int = Field(default=0, ge=0)
    state_directory: Path = Path('/var/lib/price-collector/ghost-twap')
    database_filesystem_path: Path = Path('/var/lib/postgresql')
    input_queue_max: int = Field(default=2048, ge=16, le=2048)
    audit_max_records: int = Field(default=512, ge=8, le=512)
    record_max_bytes: int = Field(default=131072, ge=32768, le=131072)
    # Explicit admission ceiling for the first canary, not an idle delay.
    decisions_per_second: int = Field(default=10, ge=1, le=10)


@dataclass
class PendingDecision:
    decision: Decision
    frozen_json: str
    state: dict
    version: int = 1
    persisted_version: int = 0
    terminal: bool = False
    def record(self) -> dict:
        return dict(run_id=self.decision.run_id, decision_id=self.decision.decision_id,
                    decision_wall_ns=self.decision.decision_wall_ns,
                    created_ms=self.decision.decision_wall_ns // NS_PER_MS,
                    frozen_json=self.frozen_json,
                    state_json=_json_bytes(self.state).decode(), version=self.version,
                    terminal=self.terminal)


class GhostRuntime:
    """Single-loop admission/engine owner with independent spool, Redis and PG tasks.

    offer_price must be called immediately after synchronous parsing, before any
    yield following the original receipt clocks. The loop drains the entire
    queue and freezes its cutoff without awaiting; coalescing affects decisions
    only. A bounded fsynced outbox precedes every possible Redis publication.
    """
    def __init__(self, settings: GhostSettings, store: Any, redis: Any,
                 spool: GhostSpool, *, wall_ns=time.time_ns,
                 mono_ns=time.monotonic_ns, disk_free=None) -> None:
        self.settings, self.store, self.redis, self.spool = settings, store, redis, spool
        self.wall_ns, self.mono_ns = wall_ns, mono_ns
        self.disk_free = disk_free or (lambda: shutil.disk_usage(settings.database_filesystem_path).free)
        self.run_id = uuid4().hex
        self.engine = GhostTwapEngine(self.run_id, GhostPolicy(enabled=True))
        self.queue: deque = deque()
        self.records: OrderedDict[str, PendingDecision] = OrderedDict()
        self.late_events: deque = deque()
        self._late_cursors: dict = {}
        self.counters: Counter = Counter()
        self.gaps: deque = deque(maxlen=128)
        self.recovery: dict = {}
        self._unavailable_since: dict = {}
        self._dirty: set[str] = set()
        self._wake, self._audit_wake, self._publish_wake = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self._spool_lock = asyncio.Lock()
        self._pending_publication: str | None = None
        self._sequence = self._decisions = 0
        self._admission_times: deque = deque()
        self._last_valid_until = 0
        self._expired_emitted = True
        self._tasks: list = []
        self._closed = False
        self.stop_reason: str | None = None
        self.suspensions: dict = {}
        self.suspension_history: dict = {}
        self._publication_epoch = 0
        self._publishing: str | None = None
        self._audit_cursor: str | None = None
        self.guard: dict | None = None
        self.campaign: dict = {}
        self._campaign_dirty = False
        self._campaign_saved_mono: int | None = None
        self._start_mono = self.mono_ns()
        self._last_check_mono = self._start_mono
        self._end_mono = self._start_mono
        self.initial_rows = 0
        self._last_admitted_receipt: tuple | None = None
        self._last_frozen_receipt: tuple | None = None
        self._last_warning_ns = 0

    async def _spool(self, method, *args):
        async with self._spool_lock:
            task = asyncio.get_running_loop().run_in_executor(None, partial(method, *args))
            cancelled = False
            while True:
                try:
                    result = await asyncio.shield(task)
                    break
                except asyncio.CancelledError:
                    # A filesystem thread cannot be cancelled. Keep ownership
                    # until it ends so another write cannot race its .tmp file.
                    cancelled = True
            if cancelled:
                raise asyncio.CancelledError
            return result

    async def start(self) -> None:
        now_ms = self.wall_ns() // NS_PER_MS
        if not self.settings.enabled or not 0 < self.settings.canary_start_ms <= now_ms:
            raise ValueError('enabled ghost needs an explicit past/current canary start')
        await self._spool(self.spool.open)
        self.campaign = await self._spool(self.spool.campaign, self.settings.canary_start_ms)
        self.stop_reason = self.campaign.get('stop_reason')
        self._end_mono = self.mono_ns() + max(0, self.settings.canary_start_ms + CANARY_MS - now_ms) * NS_PER_MS
        initial = await self.store.initialize()
        saved_records = await self._spool(self.spool.read_all)
        latest_decision_ms = max([initial.get('latest_created_ms') or 0]
                                 + [record['created_ms'] for record in saved_records])
        self.campaign['last_wall_ms'] = max(self.campaign['last_wall_ms'], latest_decision_ms)
        if now_ms < self.campaign['last_wall_ms']:
            self.stop('wall_clock_regression')
        # Reconcile disk intent before any new admission. Never replay prices to Redis.
        for record in saved_records:
            await self._recover(record)
            await self._spool(self.spool.remove, record)
        cursor = None
        while True:
            rows = await self.store.list_incomplete(limit=100, after=cursor)
            if not rows:
                break
            for row in rows:
                await self._recover(dict(row))
            cursor = (rows[-1]['run_id'], rows[-1]['decision_id'])
        self.initial_rows = (await self.store.initialize())['row_count']
        await self.refresh_guard()
        self._tasks = [asyncio.create_task(self._supervise(fn), name=name) for fn, name in (
            (self._calculation_loop, 'ghost-calculation'),
            (self._publication_loop, 'ghost-publication'),
            (self._audit_loop, 'ghost-audit'), (self._guard_loop, 'ghost-guard'))]
        LOGGER.info('ghost_runtime_started', extra={'run_id': self.run_id,
                    'canary_end_ms': self.settings.canary_start_ms + CANARY_MS,
                    'stop_reason': self.stop_reason})

    async def _recover(self, record: dict) -> None:
        stored = await self.store.get_record(record['run_id'], record['decision_id'])
        if stored is not None:
            if stored['frozen_json'] != record['frozen_json']:
                raise ValueError('restart frozen audit conflict')
            if stored['version'] == record['version'] and (stored['state_json'] != record['state_json'] or stored['terminal'] != record['terminal']):
                raise ValueError('restart same-version audit conflict')
            if stored['version'] > record['version']:
                record = dict(stored)
        if record['terminal']:
            if stored is None or stored['version'] < record['version']:
                await self.store.persist(record)
            return
        state = json.loads(record['state_json'])
        state['restart_reconciled'] = True
        if state['publication']['status'] in ('reserved', 'intent', 'attempting'):
            state['publication']['restart_outcome'] = 'unconfirmed_after_restart'
        for target in state['targets'].values():
            if target['status'] == 'pending':
                target['status'] = 'restart_unmatched'
        recovered = dict(record, version=record['version'] + 1, terminal=True,
                         state_json=_json_bytes(state).decode())
        await self.store.persist(recovered)

    async def _supervise(self, fn) -> None:
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception('ghost_optional_worker_failed')
            self.stop('worker_failure')

    def stop(self, reason: str) -> None:
        if self.stop_reason is None:
            self.stop_reason = reason
            self.campaign['stop_reason'] = reason
            self._campaign_dirty = True
            self.counters['stops'] += 1
            LOGGER.error('ghost_decisions_stopped', extra={'reason': reason, 'run_id': self.run_id})
            for row in self.records.values():
                row.state['runtime_stop'] = dict(reason=reason, observed_wall_ns=self.wall_ns(),
                                                 observed_monotonic_ns=self.mono_ns())
                self._changed(row)
        if reason in ('receipt_order_fault', 'publication_clock_regression', 'wall_clock_regression',
                      'monotonic_clock_regression'):
            for row in self.records.values():
                if not row.state.get('causality_invalid'):
                    row.state['causality_invalid'] = True
                    row.state['causality_fault_reason'] = reason
                    for target in row.state['targets'].values():
                        target['confirmed_redis_lead_ns'] = None
                    self._changed(row)
        self._audit_wake.set()

    @staticmethod
    def _integrity_failure(exc: Exception) -> bool:
        if isinstance(exc, (ValueError, TypeError, AssertionError)):
            return True
        code = getattr(exc, 'sqlstate', None)
        # Connection, resource, lock, cancellation and serialization errors may
        # recover. Data/schema/authentication errors require operator repair.
        return bool(code and code[:2] not in ('08', '40', '53', '55', '57'))

    def suspend(self, scope: str, reason: str) -> None:
        if scope not in ('guard', 'audit', 'publication'):
            raise ValueError('invalid ghost suspension scope')
        if self.stop_reason or scope in self.suspensions:
            return
        self._publication_epoch += 1
        status = dict(reason=reason, started_wall_ns=self.wall_ns(),
                      started_monotonic_ns=self.mono_ns(), active=True)
        self.suspensions[scope] = status
        self.suspension_history[scope] = status
        self.counters['suspensions'] += 1
        for row in self.records.values():
            row.state.setdefault('runtime_suspensions', {})[scope] = dict(status)
            self._changed(row)
        LOGGER.warning('ghost_decisions_suspended', extra={'scope': scope, 'reason': reason})
        self._audit_wake.set()

    def _io_failure(self, scope: str, reason: str, exc: Exception) -> None:
        if self._integrity_failure(exc):
            self.stop(scope + '_integrity_failure')
        else:
            self.suspend(scope, reason)

    def _resume_if_caught_up(self) -> None:
        if (not self.suspensions or self.stop_reason or self.guard is None
                or self.mono_ns() - self.guard['monotonic_ns'] > 3 * NS_PER_SECOND
                or self._dirty or self.late_events or self._campaign_dirty
                or self._publishing is not None):
            return
        for scope, status in self.suspensions.items():
            completed = dict(status, active=False, resumed_wall_ns=self.wall_ns(),
                             resumed_monotonic_ns=self.mono_ns())
            self.suspension_history[scope] = completed
            for row in self.records.values():
                row.state.setdefault('runtime_suspensions', {})[scope] = dict(completed)
                self._changed(row)
        self.suspensions.clear()
        self.counters['resumptions'] += 1
        LOGGER.info('ghost_decisions_resumed', extra={'run_id': self.run_id})
        # The next snapshot drains the full current receipt prefix. Never retry
        # a decision from an earlier publication epoch after a suspension.
        self._wake.set()

    def offer_gap(self, feed: str, reason: str) -> None:
        if self._closed:
            return
        if feed not in ('spot', 'twap'):
            self.stop('invalid_gap_feed')
            return
        if len(self.queue) >= self.settings.input_queue_max:
            self.counters['input_drops'] += len(self.queue)
            self.queue.clear()
            self.queue.extend([('gap', 'spot', 'input_overflow'), ('gap', 'twap', 'input_overflow')])
        self.queue.append(('gap', feed, reason[:256], self._sequence,
                           self.wall_ns(), self.mono_ns()))
        self._wake.set()

    def offer_price(self, feed: str, value: Decimal, source_ms: int,
                    received_wall_ns: int, received_mono_ns: int, event_id: str,
                    window_s: int | None = None) -> None:
        if self._closed:
            return
        self._sequence += 1
        try:
            event = PriceEvent(feed, value, source_ms, received_wall_ns, received_mono_ns,
                               self._sequence, event_id, window_s)
            if not event_id.isascii() or max(source_ms, received_wall_ns, received_mono_ns, self._sequence) > 9223372036854775807:
                raise ValueError('ghost event identity/clocks exceed audit bounds')
        except (ValueError, TypeError):
            self.counters['invalid_inputs'] += 1
            self.offer_gap(feed, 'invalid_ghost_input')
            return
        for boundary in (self._last_admitted_receipt, self._last_frozen_receipt):
            if boundary is not None and (received_wall_ns < boundary[0] or received_mono_ns < boundary[1]):
                self.stop('receipt_order_fault')
                self.counters['receipt_order_faults'] += 1
                return
        self._last_admitted_receipt = (received_wall_ns, received_mono_ns)
        # Observe targets at admission, even when the constituent queue overflows.
        if feed == 'twap':
            self.observe_target(event)
            if event.source_timestamp_ms * NS_PER_MS > event.received_wall_ns:
                self.counters['invalid_target_clocks'] += 1
            elif len(self.late_events) >= self.settings.input_queue_max:
                self.counters['late_audit_drops'] += 1
                self.stop('late_target_queue_full')
            else:
                self.late_events.append(json.loads(_json_bytes(_event_record(event))))
                self._audit_wake.set()
        if len(self.queue) >= self.settings.input_queue_max:
            self.counters['input_drops'] += len(self.queue)
            self.queue.clear()
            self.queue.extend([('gap', 'spot', 'input_overflow'), ('gap', 'twap', 'input_overflow')])
        self.queue.append(event)
        self.counters['input_queue_high_water'] = max(self.counters['input_queue_high_water'], len(self.queue))
        self._wake.set()

    def _changed(self, row: PendingDecision) -> None:
        row.version += 1
        self._dirty.add(row.decision.decision_id)
        self._audit_wake.set()

    def observe_target(self, event: PriceEvent) -> None:
        for row in list(self.records.values()):
            for target in row.state['targets'].values():
                if target['target_source_timestamp_ms'] != event.source_timestamp_ms:
                    continue
                if event.received_monotonic_ns < row.decision.decision_monotonic_ns:
                    continue
                first = target.get('first_event')
                if first is not None:
                    if Decimal(first['value']) != event.value and not target.get('conflicted'):
                        target['conflicted'] = True
                        target['first_conflicting_event'] = _event_record(event)
                        target['confirmed_redis_lead_ns'] = None
                        self._changed(row)
                    continue
                if target['status'] == 'pending' and event.received_monotonic_ns >= row.decision.decision_monotonic_ns + MATCH_NS:
                    target['status'] = 'missing'
                if target['status'] != 'pending':
                    if target['status'] == 'missing' and 'first_late_event' not in target:
                        target['first_late_event'] = _event_record(event)
                        self._changed(row)
                    continue
                target['first_event'] = _event_record(event)
                target['status'] = 'matched'
                target['clock_anomaly'] = event.source_timestamp_ms * NS_PER_MS > event.received_wall_ns
                forecast = next(f for f in row.decision.forecasts if str(f.horizon_s) == target['horizon'])
                with localcontext(MATH_CONTEXT):
                    target['error'] = format((forecast.price - event.value).quantize(PRICE_QUANTUM), '.18f')
                    target['persistence_error'] = format((row.decision.current_twap.value - event.value).quantize(PRICE_QUANTUM), '.18f')
                target['eta_error_ns'] = event.received_wall_ns - forecast.estimated_arrival_wall_ns
                self._score_lead(row, target)
                self._changed(row)

    def _score_lead(self, row: PendingDecision, target: dict) -> None:
        pub = row.state['publication']
        first = target.get('first_event')
        ack = pub.get('ack_monotonic_ns')
        target['confirmed_redis_lead_ns'] = None
        if (first is not None and ack is not None and pub['status'] == 'acknowledged'
                and not target.get('conflicted') and not target.get('clock_anomaly')
                and not row.state.get('causality_invalid')):
            received = int(first['received_monotonic_ns'])
            if ack < received:
                target['confirmed_redis_lead_ns'] = received - ack

    def drain_inputs(self) -> None:
        # There is intentionally no await between queue drain and snapshot.
        while self.queue:
            item = self.queue.popleft()
            if isinstance(item, tuple):
                _, feed, reason, *boundary = item
                self.engine.record_gap(feed, reason)
                sequence, wall, mono = boundary or (self._sequence, self.wall_ns(), self.mono_ns())
                self.gaps.append(dict(feed=feed, reason=reason, after_sequence=sequence,
                                      observed_wall_ns=wall, observed_monotonic_ns=mono))
                self.counters['resets'] += 1
            else:
                try:
                    self.engine.accept(item)
                except ValueError:
                    self.stop('receipt_order_fault')

    async def refresh_guard(self) -> None:
        if self.stop_reason:
            return
        try:
            measurement = await asyncio.wait_for(self.store.measure(), timeout=2)
            if measurement.get('tablespaces', ['pg_default']) != ['pg_default']:
                raise ValueError('ghost audit needs verified default tablespace placement')
            measurement['row_count'] = max(measurement.get('row_count') or 0,
                                           self.initial_rows + self._decisions)
            free = await asyncio.to_thread(self.disk_free)
            sample = dict(measurement, free_bytes=free, monotonic_ns=self.mono_ns())
            if measurement['relation_bytes'] >= WARN_BYTES and self.mono_ns() - self._last_warning_ns >= 60 * NS_PER_SECOND:
                LOGGER.warning('ghost_audit_relation_warning', extra={'bytes': measurement['relation_bytes']})
                self._last_warning_ns = self.mono_ns()
            if measurement['relation_bytes'] >= STOP_BYTES:
                self.stop('audit_size_cap')
            if measurement['row_count'] >= MAX_ROWS:
                self.stop('audit_row_cap')
            if free < RESERVE_BYTES:
                self.stop('database_disk_reserve')
            self.guard = sample
            self._resume_if_caught_up()
        except Exception as exc:
            self.guard = None
            self._io_failure('guard', 'guard_unavailable', exc)

    def _can_issue(self, wall: int, mono: int) -> bool:
        if self.stop_reason:
            return False
        now = wall // NS_PER_MS
        if mono < self._last_check_mono:
            self.stop('monotonic_clock_regression')
        self._last_check_mono = max(mono, self._last_check_mono)
        if now < self.campaign.get('last_wall_ms', now):
            self.stop('wall_clock_regression')
        self.campaign['last_wall_ms'] = max(now, self.campaign.get('last_wall_ms', now))
        if (self._campaign_saved_mono is None
                or mono - self._campaign_saved_mono >= CAMPAIGN_CHECKPOINT_SECONDS * NS_PER_SECOND):
            self._campaign_dirty = True
        if now >= self.settings.canary_start_ms + CANARY_MS or mono >= self._end_mono:
            self.stop('canary_deadline')
        if self.stop_reason:
            return False
        if self.guard is None or mono - self.guard['monotonic_ns'] > 3 * NS_PER_SECOND:
            self.suspend('guard', 'stale_guard')
            return False
        if self.suspensions:
            return False
        if max(self.guard['row_count'], self.initial_rows + self._decisions) >= MAX_ROWS:
            self.stop('audit_row_reserve')
            return False
        # Full bounded serialized records plus extra update allowance stay under budget.
        if self.guard['relation_bytes'] + self.settings.audit_max_records * self.settings.record_max_bytes * 4 >= BUDGET_BYTES:
            self.stop('audit_byte_reserve')
            return False
        if len(self.records) >= self.settings.audit_max_records:
            self.counters['audit_admission_pauses'] += 1
            return False
        while self._admission_times and self._admission_times[0] <= mono - NS_PER_SECOND:
            self._admission_times.popleft()
        if len(self._admission_times) >= self.settings.decisions_per_second:
            self.counters['rate_coalesced'] += 1
            return False
        return True

    def issue(self) -> PendingDecision | None:
        self.drain_inputs()
        wall, mono = self.wall_ns(), self.mono_ns()
        if not self._can_issue(wall, mono):
            return None
        self._decisions += 1
        decision = self.engine.snapshot(str(self._decisions), wall, mono)
        self._last_frozen_receipt = (wall, mono)
        complete_wall, complete_mono = self.wall_ns(), self.mono_ns()
        self._admission_times.append(mono)
        frozen = json.loads(decision.to_audit_json())
        frozen['runtime_policy'] = self.settings.model_dump(mode='json')
        frozen['runtime_version'] = RUNTIME_VERSION
        frozen['campaign_checkpoint_seconds'] = CAMPAIGN_CHECKPOINT_SECONDS
        frozen['runtime_suspension_history'] = dict(self.suspension_history)
        frozen['runtime_counters'] = dict(self.counters)
        frozen['operational_gaps'] = list(self.gaps)
        for forecast in frozen['forecasts']:
            start = forecast['slot_start_index']
            selected = [] if start is None else decision.slots[start:start + 60]
            forecast['max_interior_carry_ms'] = max((slot.carry_age_ms for slot in selected
                                                    if slot.category == 'carried'), default=0)
        state = dict(computation_completed_wall_ns=complete_wall,
                     computation_completed_monotonic_ns=complete_mono,
                     publication_epoch=self._publication_epoch,
                     publication={'status': 'reserved'}, targets={})
        for forecast in decision.forecasts:
            h = str(forecast.horizon_s)
            state['targets'][h] = dict(horizon=h, target_source_timestamp_ms=forecast.target_source_timestamp_ms,
                                      status='pending' if forecast.price is not None else 'not_forecast',
                                      first_event=None, conflicted=False, confirmed_redis_lead_ns=None)
            if forecast.price is None:
                self._unavailable_since.setdefault(h, mono)
            elif h in self._unavailable_since:
                self.recovery[h] = dict(last_recovery_ns=mono-self._unavailable_since.pop(h), recovered_at_ns=mono)
        frozen['horizon_recovery'] = dict(self.recovery)
        row = PendingDecision(decision, _json_bytes(frozen).decode(), state)
        if len(_json_bytes(row.record())) + RESULT_GROWTH_RESERVE_BYTES > self.settings.record_max_bytes:
            self.stop('record_size_cap')
            return None
        if self._pending_publication is not None:
            previous = self.records[self._pending_publication]
            previous.state['publication']['status'] = 'coalesced_before_publication'
            self._changed(previous)
            self.counters['publication_coalesced'] += 1
        self.records[decision.decision_id] = row
        self._dirty.add(decision.decision_id)
        self._pending_publication = decision.decision_id
        self.counters['decisions_issued'] += 1
        self.counters['audit_reserve_high_water'] = max(self.counters['audit_reserve_high_water'], len(self.records))
        self._last_valid_until = decision.valid_until_wall_ns
        self._expired_emitted = decision.valid_until_wall_ns <= wall
        self._audit_wake.set()
        self._publish_wake.set()
        return row

    def finalize_due(self) -> None:
        mono = self.mono_ns()
        for row in list(self.records.values()):
            if row.terminal or mono < row.decision.decision_monotonic_ns + MATCH_NS:
                continue
            for target in row.state['targets'].values():
                if target['status'] == 'pending':
                    target['status'] = 'missing'
            row.terminal = True
            self._changed(row)

    async def _calculation_loop(self) -> None:
        while not self._closed:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass
            triggered = self._wake.is_set()
            self._wake.clear()
            self.finalize_due()
            if triggered or (not self._expired_emitted and self.wall_ns() > self._last_valid_until):
                self.issue()

    def _publishable(self, row: PendingDecision) -> bool:
        if (self.stop_reason or row.terminal or self.suspensions
                or row.state.get('publication_epoch', 0) != self._publication_epoch):
            return False
        now, mono = self.wall_ns(), self.mono_ns()
        remaining = row.decision.valid_until_wall_ns - now
        elapsed = mono - row.decision.decision_monotonic_ns
        mono_remaining = row.decision.valid_until_wall_ns - row.decision.decision_wall_ns - elapsed
        if elapsed < 0 or now < row.decision.decision_wall_ns:
            self.stop('publication_clock_regression')
            return False
        if remaining <= 0 or mono_remaining <= 0:
            return False
        if self.guard is None or mono - self.guard['monotonic_ns'] > 3 * NS_PER_SECOND:
            self.suspend('guard', 'stale_guard')
            return False
        if now // NS_PER_MS >= self.settings.canary_start_ms + CANARY_MS or mono >= self._end_mono:
            self.stop('canary_deadline')
            return False
        return not any(t.get('first_event') is not None for t in row.state['targets'].values())

    async def publish(self, row: PendingDecision) -> None:
        self._publishing = row.decision.decision_id
        try:
            await self._publish(row)
        except Exception as exc:
            row.state['publication']['status'] = 'spool_unavailable'
            self._changed(row)
            self._io_failure('publication', 'spool_unavailable', exc)
            raise
        finally:
            self._publishing = None
            self._audit_wake.set()

    async def _publish(self, row: PendingDecision) -> None:
        if not self._publishable(row):
            row.state['publication']['status'] = 'expired_or_target_received'
            self._changed(row)
            return
        # Intent bytes and their full constituent audit survive before Redis is touched.
        wall, mono = self.wall_ns(), self.mono_ns()
        live = json.loads(row.decision.to_live_json())
        for forecast, audit_forecast in zip(live['forecasts'], json.loads(row.frozen_json)['forecasts']):
            forecast['max_interior_carry_ms'] = audit_forecast['max_interior_carry_ms']
        live.update(runtime_version=RUNTIME_VERSION, publication_sequence=int(row.decision.decision_id),
                    computation_completed_wall_ns=row.state['computation_completed_wall_ns'],
                    computation_completed_monotonic_ns=row.state['computation_completed_monotonic_ns'],
                    publication_intent_wall_ns=wall, publication_intent_monotonic_ns=mono,
                    publication_state='intent', audit_state='durable_outbox_postgres_pending')
        # Quality is about arithmetic inputs; persistence is a separate, explicit state.
        body = _json_bytes(live)
        row.state['publication'] = dict(status='intent', intent_wall_ns=wall,
                                       intent_monotonic_ns=mono, payload_json=body.decode())
        self._changed(row)
        await self._spool(self.spool.write, row.record())
        if not self._publishable(row):
            row.state['publication']['status'] = 'preempted_after_spool'
            self._changed(row)
            return
        attempt_wall, attempt_mono = self.wall_ns(), self.mono_ns()
        row.state['publication'].update(status='attempting', attempt_wall_ns=attempt_wall,
                                        attempt_monotonic_ns=attempt_mono)
        live.update(publication_state='attempted', publication_attempt_wall_ns=attempt_wall,
                    publication_attempt_monotonic_ns=attempt_mono)
        body = _json_bytes(live)
        row.state['publication']['payload_json'] = body.decode()
        self._changed(row)
        ttl_ms = min((row.decision.valid_until_wall_ns-attempt_wall)//NS_PER_MS,
                     (row.decision.valid_until_wall_ns-row.decision.decision_wall_ns
                      -(attempt_mono-row.decision.decision_monotonic_ns))//NS_PER_MS)
        if ttl_ms <= 0:
            row.state['publication']['status'] = 'expired_before_attempt'
            self._changed(row)
            return
        try:
            await asyncio.wait_for(self.redis.eval(PUBLISH_LUA, 2, GHOST_KEY, GHOST_CHANNEL,
                                                  body, ttl_ms), timeout=0.5)
        except Exception:
            row.state['publication'].update(status='uncertain', failure_wall_ns=self.wall_ns(),
                                            failure_monotonic_ns=self.mono_ns())
            self.counters['redis_uncertain'] += 1
        else:
            row.state['publication'].update(status='acknowledged', ack_wall_ns=self.wall_ns(),
                                            ack_monotonic_ns=self.mono_ns())
            self.counters['redis_acknowledged'] += 1
        for target in row.state['targets'].values():
            self._score_lead(row, target)
        self._changed(row)

    async def _publication_loop(self) -> None:
        while not self._closed:
            await self._publish_wake.wait()
            self._publish_wake.clear()
            key, self._pending_publication = self._pending_publication, None
            if key is not None:
                try:
                    await self.publish(self.records[key])
                except Exception:
                    # publish() records the fault and preserves its reservation.
                    # Resume only after audit catchup, using a new decision.
                    await asyncio.sleep(0.5)

    async def flush_audit_once(self) -> None:
        errors = []
        started = asyncio.get_running_loop().time()
        if self._campaign_dirty:
            campaign = dict(self.campaign)
            try:
                await self._spool(self.spool.save_campaign, campaign)
                self._campaign_saved_mono = self.mono_ns()
                # Ordinary progress during fsync does not force another write;
                # an intervening stop or full checkpoint interval does.
                self._campaign_dirty = (self.campaign.get('stop_reason') != campaign.get('stop_reason')
                    or self.campaign.get('last_wall_ms', 0) - campaign.get('last_wall_ms', 0)
                    >= CAMPAIGN_CHECKPOINT_SECONDS * 1000)
            except Exception as exc:
                errors.append(exc)
                self._io_failure('audit', 'campaign_persistence_unavailable', exc)
        # Rotate the starting identity after every attempt, including failures.
        # A poisoned row or a full batch of timeouts cannot starve later rows.
        keys = sorted(self._dirty)
        if self._audit_cursor is not None:
            keys = [key for key in keys if key > self._audit_cursor] + [key for key in keys if key <= self._audit_cursor]
        for key in keys[:32]:
            self._audit_cursor = key
            row = self.records.get(key)
            if row is None:
                self._dirty.discard(key)
                continue
            record = row.record()
            try:
                await self._spool(self.spool.write, record)
                await asyncio.wait_for(self.store.persist(record), timeout=2)
                row.persisted_version = record['version']
                if row.version == record['version']:
                    self._dirty.discard(key)
                    if (row.terminal and self._pending_publication != key
                            and self._publishing != key):
                        await self._spool(self.spool.remove, record)
                        if row.version == record['version']:
                            self.records.pop(key)
            except Exception as exc:
                self._dirty.add(key)
                errors.append(exc)
                self.counters['audit_failures'] += 1
                self._io_failure('audit', 'audit_persistence_unavailable', exc)
            if asyncio.get_running_loop().time() - started >= AUDIT_BATCH_SECONDS:
                break
        for _ in range(min(32, len(self.late_events))):
            if asyncio.get_running_loop().time() - started >= AUDIT_BATCH_SECONDS:
                break
            event = self.late_events[0]
            event_key = id(event)
            cursor = self._late_cursors.get(event_key)
            try:
                while True:
                    # The audit task is the sole database writer for these
                    # in-memory identities until commit + outbox removal.
                    owned = [(row.decision.run_id, row.decision.decision_id) for row in self.records.values()]
                    result = await asyncio.wait_for(self.store.note_late_target(
                        event, after=cursor, exclude=owned), timeout=2)
                    cursor = None if result is None else result.get('next_after')
                    if cursor is None:
                        self.late_events.popleft()
                        self._late_cursors.pop(event_key, None)
                        break
                    self._late_cursors[event_key] = cursor
                    if asyncio.get_running_loop().time() - started >= AUDIT_BATCH_SECONDS:
                        break  # Keep event; already committed flags are idempotent.
            except Exception as exc:
                self.late_events.rotate(-1)
                errors.append(exc)
                self.counters['audit_failures'] += 1
                self._io_failure('audit', 'late_target_persistence_unavailable', exc)
        self._resume_if_caught_up()
        if errors:
            raise errors[0]

    async def _audit_loop(self) -> None:
        while not self._closed:
            try:
                await asyncio.wait_for(self._audit_wake.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            self._audit_wake.clear()
            try:
                await self.flush_audit_once()
            except Exception:
                # The batch recorded each failure and continued unrelated rows.
                await asyncio.sleep(0.5)
            if self._dirty or self.late_events:
                self._audit_wake.set()

    async def _guard_loop(self) -> None:
        while not self._closed:
            started = asyncio.get_running_loop().time()
            if not self.stop_reason:
                await self.refresh_guard()
                self._can_issue(self.wall_ns(), self.mono_ns())
            elapsed = asyncio.get_running_loop().time() - started
            await asyncio.sleep(max(0.05, 1 - elapsed))

    async def close(self) -> None:
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if not self.stop_reason:
            self.campaign['last_wall_ms'] = max(self.wall_ns() // NS_PER_MS,
                                               self.campaign.get('last_wall_ms', 0))
        self._campaign_dirty = True
        for row in self.records.values():
            for target in row.state['targets'].values():
                if target['status'] == 'pending':
                    target['status'] = 'restart_unmatched'
            row.terminal = True
            row.state['shutdown'] = 'unobserved_after_shutdown'
            self._changed(row)
        try:
            async def drain():
                while self._dirty or self._campaign_dirty:
                    await self.flush_audit_once()
            await asyncio.wait_for(drain(), timeout=5)
        except Exception:
            LOGGER.exception('ghost_shutdown_audit_incomplete_outbox_retained')
        finally:
            await self._spool(self.spool.close)


async def start_ghost_runtime(settings: Any) -> GhostRuntime | None:
    """Optional factory: owns independent pools and never alters source paths."""
    config = GhostSettings()
    if not config.enabled:
        return None
    import asyncpg
    import redis.asyncio as redis_async
    from price_collector.ghost_twap_store import GhostAuditStore
    from price_collector.polymarket_twap import validate_twap_runtime_identity
    pool = client = runtime = None
    try:
        # Validate the live instrument independently of ordinary RTDS context.
        if getattr(settings, 'POLYMARKET_TWAP_ENABLED', False) is not True:
            raise ValueError('ghost requires the enabled canonical 60-second TWAP feed')
        validate_twap_runtime_identity(settings)
        expected_spot = {
            'POLYMARKET_CHAINLINK_PROVIDER_CODE': 'polymarket_chainlink_rtds',
            'POLYMARKET_CHAINLINK_SYMBOL': 'BTCUSD',
            'POLYMARKET_CHAINLINK_RTD_SYMBOL': 'btc/usd',
            'POLYMARKET_CHAINLINK_TOPIC': 'crypto_prices_chainlink',
        }
        if any(getattr(settings, name, None) != value for name, value in expected_spot.items()):
            raise ValueError('ghost requires canonical Chainlink BTC/USD spot identity')
        pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL, min_size=1, max_size=2,
                                        command_timeout=2, timeout=5)
        client = redis_async.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT,
                                   db=settings.REDIS_DB, decode_responses=False,
                                   socket_connect_timeout=0.5, socket_timeout=0.5)
        spool = GhostSpool(config.state_directory, config.audit_max_records, config.record_max_bytes)
        runtime = GhostRuntime(config, GhostAuditStore(pool), client, spool)
        await runtime.start()
    except BaseException:
        if runtime is not None:
            await runtime.close()
        if client is not None:
            await client.aclose()
        if pool is not None:
            await pool.close()
        raise
    original_close = runtime.close
    async def close_owned():
        try:
            await original_close()
        finally:
            await client.aclose()
            await pool.close()
    runtime.close = close_owned
    return runtime
