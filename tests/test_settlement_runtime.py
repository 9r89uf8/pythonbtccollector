import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest

from price_collector.ghost_twap import PriceEvent
from price_collector.ghost_twap_spool import GhostSpool
from price_collector.settlement import SettlementSettings
from price_collector.settlement_runtime import SettlementRuntime, attach_settlement, PUBLISH
from price_collector.settlement_store import publication_eligible
from price_collector.settlement_wire import parse_settlement_payload
from test_settlement import START, END, NS, context, decision


class Store:
    def __init__(self):
        self.rows = []
        self.failure = False

    async def guard(self):
        return {'capacity_ok': True}

    async def persist(self, row):
        if self.failure:
            raise OSError('database offline')
        self.rows.append(deepcopy(row))

    async def maintain(self, now, **kwargs):
        return {'status': 'unarmed'}


class Redis:
    def __init__(self):
        self.raw = None
        self.publications = []
        self.hook = None
        self.failure = False

    async def get(self, key): return self.raw
    async def set(self, *args, **kwargs): return True

    async def eval(self, *args):
        self.publications.append(args)
        if self.hook: self.hook()
        if self.failure: raise OSError('ack lost')
        return 1


def setup(tmp_path, snapshot=None):
    snapshot = snapshot or decision()
    clock = {'wall': snapshot.decision_wall_ns, 'mono': snapshot.decision_monotonic_ns}
    parent = SimpleNamespace(redis=Redis(), wall_ns=lambda: clock['wall'], mono_ns=lambda: clock['mono'],
        _publication_epoch=0, _closed=False, stop_reason=None, suspensions={},
        guard={'monotonic_ns': clock['mono']},
        settings=SimpleNamespace(continuous=True, state_directory=tmp_path), settlement=None)
    spool = GhostSpool(tmp_path / 'settlement')
    spool.open()
    runtime = SettlementRuntime(parent, SettlementSettings(enabled=True), Store(), spool)
    runtime.context = context()
    runtime.context_wall, runtime.context_mono = (START + 11000) * NS, 11000 * NS
    runtime.guard, runtime.guard_mono = {'capacity_ok': True}, clock['mono']
    return runtime, snapshot, clock


def test_durable_frozen_input_precedes_publish_and_postgres_is_independent(tmp_path):
    async def run():
        runtime, snapshot, _ = setup(tmp_path)
        runtime.offer(snapshot, 0)
        row = runtime.records['1']
        def at_publish():
            saved = runtime.spool.read_all()
            assert len(saved) == 1
            assert saved[0]['frozen_json'] == row['frozen_json']
            assert not runtime.store.rows
        runtime.redis.hook = at_publish
        await runtime.publish_one(row)
        p = row['state']['publication']
        assert p['status'] == 'acknowledged' and p['eligible_before_close']
        assert publication_eligible(row['projection'], row['state'])
        raw = runtime.redis.publications[0][4]
        assert parse_settlement_payload(raw).raw == raw
        assert p['attempted_payload'].encode() == raw
        runtime.store.failure = True
        with pytest.raises(OSError): await runtime.flush()
        assert runtime.spool.read_all()[0]['state_json'] == runtime.record(row)['state_json']
        runtime.store.failure = False
        await runtime.flush()
        assert runtime.store.rows[-1]['frozen_json'] == row['frozen_json']
        await runtime.close()
    asyncio.run(run())


@pytest.mark.parametrize('race', ['gap', 'reference_conflict', 'expiry'])
def test_recheck_after_fsync_withholds_without_any_redis_send(tmp_path, race):
    async def run():
        runtime, snapshot, clock = setup(tmp_path)
        runtime.offer(snapshot, 0)
        original = runtime._save
        async def delayed(row):
            result = await original(row)
            if race == 'gap': runtime.parent._publication_epoch += 1
            elif race == 'reference_conflict': runtime.context.update(status='unavailable', conflicted=True)
            else: clock['wall'] = int(row['projection']['valid_until_wall_ns'])
            return result
        runtime._save = delayed
        await runtime.publish_one(runtime.records['1'])
        assert runtime.records['1']['state']['publication']['status'] == 'withheld'
        assert not runtime.redis.publications
        await runtime.close()
    asyncio.run(run())


@pytest.mark.parametrize('race', ['at_close', 'gap', 'lost_ack', 'monotonic_expiry'])
def test_inflight_changes_never_earn_first_call_credit(tmp_path, race):
    async def run():
        snapshot = decision(horizon=3, remaining=500)
        runtime, snapshot, clock = setup(tmp_path, snapshot)
        runtime.offer(snapshot, 0)
        def during_flight():
            if race == 'at_close':
                clock['wall'], clock['mono'] = END * NS, 300_000 * NS
            elif race == 'gap': runtime.parent._publication_epoch += 1
            elif race == 'lost_ack': runtime.redis.failure = True
            else: clock['mono'] += 500 * NS
        runtime.redis.hook = during_flight
        row = runtime.records['1']
        await runtime.publish_one(row)
        assert runtime.redis.publications
        assert row['state']['publication']['eligible_before_close'] is (race == 'gap')
        assert publication_eligible(row['projection'], row['state']) is (race == 'gap')
        if race == 'gap': assert row['state']['publication']['changed_during_flight']
        assert row['state']['publication']['status'] == ('unconfirmed' if race == 'lost_ack' else 'acknowledged')
        await runtime.close()
    asyncio.run(run())


def test_reference_receipt_is_pinned_and_first_closing_print_is_immutable(tmp_path):
    async def run():
        runtime, snapshot, clock = setup(tmp_path)
        runtime.redis.raw = json.dumps(context()).encode()
        await runtime.read_context()
        available = runtime.context_wall
        clock['wall'] += NS
        clock['mono'] += NS
        await runtime.read_context()
        assert runtime.context_wall == available
        snapshot = replace(snapshot, decision_wall_ns=clock['wall'], decision_monotonic_ns=clock['mono'])
        runtime.offer(snapshot, 0)
        row = runtime.records['1']
        await runtime.publish_one(row)
        first = PriceEvent('twap', Decimal('101'), END, (END + 1800) * NS, 301800 * NS, 100, 'close-1', 60)
        runtime.observe_target(first)
        runtime.observe_target(replace(first, value=Decimal('102'), event_id='close-2', sequence=101))
        assert row['state']['target']['first_event']['value'] == '101.000000000000000000'
        assert row['state']['target']['conflicted']
        await runtime.close()
    asyncio.run(run())


def test_restart_reconciles_durable_evidence_without_republication(tmp_path):
    async def run():
        runtime, snapshot, _ = setup(tmp_path)
        runtime.offer(snapshot, 0)
        await runtime.publish_one(runtime.records['1'])
        before = runtime.spool.read_all()[0]
        runtime.spool.close()
        resumed = SettlementRuntime(runtime.parent, runtime.settings, Store(), GhostSpool(tmp_path / 'settlement'))
        await resumed.start()
        assert len(runtime.redis.publications) == 1
        recovered = resumed.store.rows[0]
        assert recovered['frozen_json'] == before['frozen_json']
        assert json.loads(recovered['state_json'])['publication'] == json.loads(before['state_json'])['publication']
        assert json.loads(recovered['state_json'])['target']['status'] == 'restart_unmatched'
        assert recovered['terminal']
        await resumed.close()
    asyncio.run(run())


def test_optional_default_off_does_not_create_resources(monkeypatch):
    monkeypatch.setenv('SETTLEMENT_ENABLED', 'false')
    parent = SimpleNamespace(settlement=None)
    asyncio.run(attach_settlement(parent, object()))
    assert parent.settlement is None


def test_publication_script_expires_on_absolute_close_without_refresh():
    assert "redis.call('TIME')" in PUBLISH
    assert "'PXAT'" in PUBLISH
    assert "now >= tonumber(ARGV[2])" in PUBLISH


def test_settlement_hook_failure_preserves_ordinary_ghost_admission():
    from test_ghost_twap_runtime_faults import runtime, issue
    async def run():
        parent, _, _, _, _ = runtime()
        def fail(*args): raise ValueError('optional hook fault')
        parent.settlement = SimpleNamespace(observe_target=fail, offer=fail)
        row = issue(parent)
        assert row is not None and len(row.decision.forecasts) == 6
        assert parent.stop_reason is None
        assert parent.settlement._fault == 'input_hook_failure'
    asyncio.run(run())


def test_shutdown_cancels_slow_optional_maintenance_and_preserves_outbox(tmp_path):
    async def run():
        runtime, snapshot, _ = setup(tmp_path)
        runtime.offer(snapshot, 0)
        await runtime.publish_one(runtime.records['1'])
        entered = asyncio.Event()
        async def stuck_maintenance():
            entered.set()
            await asyncio.Event().wait()
        runtime._tasks = [asyncio.create_task(stuck_maintenance(), name='settlement-audit')]
        await entered.wait()
        await asyncio.wait_for(runtime.close(), 2)
        assert runtime.store.rows[-1]['terminal']
    asyncio.run(run())
