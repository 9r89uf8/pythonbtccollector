"""Independent per-horizon selection races; no changes to shared test helpers."""
import asyncio
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json

import pytest

from price_collector.ghost_twap import HORIZONS, NS_PER_MS, NS_PER_SECOND
from price_collector.ghost_twap_runtime import GhostRuntime, RESERVE_BYTES
from price_collector.ghost_twap_store import (
    GhostAuditConflict, _transition, encode_export_row, iter_export_proofs,
    validate_record, verify_export_file,
)
from test_ghost_twap_runtime_faults import (
    BASE, Redis, Store, issue, run, runtime, target,
    same_loop_for_runtime_construction_and_io,
)


def body(row):
    return json.loads(row.state['publication']['payload_json'])


def expect_mask(payload, eligible, excluded):
    mask = payload['publication_eligibility']
    assert mask['version'] == 1
    assert mask['eligible_horizons'] == list(eligible)
    assert mask['excluded_horizons'] == excluded
    assert set(mask['eligible_horizons']).isdisjoint(map(int, mask['excluded_horizons']))
    assert set(mask['eligible_horizons']) | set(map(int, mask['excluded_horizons'])) == set(HORIZONS)
    assert [f['horizon_s'] for f in payload['forecasts']] == list(HORIZONS)
    assert isinstance(mask['checked_wall_ns'], str)
    assert isinstance(mask['checked_monotonic_ns'], str)


def expect_forecast_preservation(row, payload, excluded_received):
    original = json.loads(row.frozen_json)['forecasts']
    for before, after in zip(original, payload['forecasts']):
        expected = deepcopy(before)
        if before['horizon_s'] in excluded_received and before['price'] is not None:
            expected['price'] = None
            expected['quality'] = 'unavailable'
            expected['reasons'].append('target_received_before_publication')
        assert after == expected


def block_spool(value, spool):
    """Pause on the owner loop while its durable-write await is outstanding."""
    entered, release = asyncio.Event(), asyncio.Event()
    original = value._spool

    async def blocked(method, *args):
        if method == spool.write:
            entered.set()
            await release.wait()
        return await original(method, *args)

    value._spool = blocked
    return entered, release


def test_one_received_target_before_publish_preserves_other_five_and_frozen_input():
    value, clock, spool, store, redis = runtime()
    row = issue(value)
    frozen, decision = row.frozen_json, row.decision
    assert json.loads(frozen)['publication_eligibility_policy'] == 'per-horizon-unreceived-v1'
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=1))
    run(value.publish(row))
    payload = body(row)
    expect_mask(payload, (2, 3, 5, 10, 30), {'1': ['target_received_before_publication']})
    expect_forecast_preservation(row, payload, {1})
    assert payload['runtime_version'] == 'ghost-canary-v6'
    assert payload['contract_version'] == 4
    assert int(payload['publication_eligibility']['checked_monotonic_ns']) == clock.mono
    assert row.state['publication']['status'] == 'acknowledged'
    assert value.counters['publication_partial_batches'] == 1
    assert value.counters['publication_horizons_withheld'] == 1
    assert len(redis.calls) == 1
    assert redis.calls[0][-2].decode() == row.state['publication']['payload_json']
    assert redis.calls[0][-1] == 2000
    assert row.frozen_json == frozen and row.decision == decision
    assert row.state['targets']['1']['confirmed_redis_lead_ns'] is None
    run(value.flush_audit_once())
    stored = store.persisted[-1]
    assert stored['frozen_json'] == frozen
    assert json.loads(stored['state_json'])['publication']['payload_json'] == redis.calls[0][-2].decode()


@pytest.mark.parametrize('also_received_before', [False, True])
def test_target_during_spool_changes_final_mask_but_not_durable_intent_or_frozen_input(also_received_before):
    async def scenario():
        value, clock, spool, _, redis = runtime()
        row = issue(value)
        frozen = row.frozen_json
        if also_received_before:
            clock.advance(NS_PER_SECOND)
            value.observe_target(target(value, row, clock, horizon=1))
        entered, release = block_spool(value, spool)
        publication = asyncio.create_task(value.publish(row))
        await entered.wait()
        intent = deepcopy(body(row))
        before_excluded = {'1': ['target_received_before_publication']} if also_received_before else {}
        expect_mask(intent, tuple(h for h in HORIZONS if str(h) not in before_excluded), before_excluded)
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock, horizon=2 if also_received_before else 1))
        release.set()
        await publication
        attempted = body(row)
        removed = {1, 2} if also_received_before else {1}
        expect_mask(attempted, tuple(h for h in HORIZONS if h not in removed),
                    {str(h): ['target_received_before_publication'] for h in sorted(removed)})
        expect_forecast_preservation(row, attempted, removed)
        assert value.counters['publication_partial_batches'] == 1
        assert value.counters['publication_horizons_withheld'] == len(removed)
        durable = spool.rows[(row.decision.run_id, row.decision.decision_id)]
        durable_publication = json.loads(durable['state_json'])['publication']
        assert durable_publication['status'] == 'intent'
        assert json.loads(durable_publication['payload_json']) == intent
        assert durable['frozen_json'] == row.frozen_json == frozen
        assert redis.calls[0][-2].decode() == row.state['publication']['payload_json']
        assert int(attempted['publication_eligibility']['checked_monotonic_ns']) > int(intent['publication_eligibility']['checked_monotonic_ns'])
    run(scenario())


@pytest.mark.parametrize('ack_after_target_ns', [0, 1])
def test_target_during_redis_await_keeps_attempt_membership_but_cannot_earn_lead(ack_after_target_ns):
    async def scenario():
        value, clock, _, _, redis = runtime()
        row = issue(value)
        entered, release = asyncio.Event(), asyncio.Event()

        async def awaiting_ack(*args):
            redis.calls.append(args)
            entered.set()
            await release.wait()
            return 1

        redis.eval = awaiting_ack
        publication = asyncio.create_task(value.publish(row))
        await entered.wait()
        attempted_bytes = redis.calls[0][-2]
        expect_mask(json.loads(attempted_bytes), HORIZONS, {})
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock, horizon=1))
        clock.advance(ack_after_target_ns)
        release.set()
        await publication
        assert row.state['publication']['payload_json'].encode() == attempted_bytes
        assert row.state['targets']['1']['confirmed_redis_lead_ns'] is None
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock, horizon=2))
        assert row.state['targets']['2']['confirmed_redis_lead_ns'] == NS_PER_SECOND
    run(scenario())


def test_originally_unavailable_horizons_keep_original_fields_and_exclusion_reasons():
    value, clock, _, _, redis = runtime()
    # Starting history at43s leaves the1/2/3s windows incomplete while5/10/30
    # have their60 constituents. No mutation of a frozen decision is needed.
    for second in range(43, 101):
        value.offer_price('spot', Decimal('100'), BASE+second*1000,
                          (BASE+second*1000)*NS_PER_MS, second*NS_PER_SECOND,
                          'spot-'+str(second))
        value.drain_inputs()
    value.offer_price('twap', Decimal('100'), BASE+100000, clock.wall, clock.mono, 'anchor', 60)
    row = value.issue()
    assert row is not None
    frozen = json.loads(row.frozen_json)
    assert [f['horizon_s'] for f in frozen['forecasts'] if f['price'] is None] == [1, 2, 3]
    run(value.publish(row))
    expected = {str(f['horizon_s']): f['reasons'] for f in frozen['forecasts'] if f['price'] is None}
    expect_mask(body(row), (5, 10, 30), expected)
    expect_forecast_preservation(row, body(row), set())
    assert len(redis.calls) == 1
    assert value.counters['publication_partial_batches'] == 0
    assert value.counters['publication_horizons_withheld'] == 0


def test_all_originally_unavailable_health_payload_is_still_published():
    value, clock, _, _, redis = runtime()
    value.offer_price('spot', Decimal('100'), BASE+100000, clock.wall, clock.mono, 'spot')
    value.offer_price('twap', Decimal('100'), BASE+100000, clock.wall, clock.mono, 'anchor', 60)
    row = value.issue()
    assert row is not None and all(f.price is None for f in row.decision.forecasts)
    run(value.publish(row))
    original = json.loads(row.frozen_json)['forecasts']
    expect_mask(body(row), (), {str(f['horizon_s']): f['reasons'] for f in original})
    expect_forecast_preservation(row, body(row), set())
    assert row.state['publication']['status'] == 'acknowledged'
    assert len(redis.calls) == 1


@pytest.mark.parametrize('during_spool', [False, True])
def test_all_previously_available_targets_received_means_no_redis_call(during_spool):
    async def scenario():
        value, clock, spool, _, redis = runtime()
        row = issue(value)
        frozen = row.frozen_json
        if during_spool:
            entered, release = block_spool(value, spool)
            publication = asyncio.create_task(value.publish(row))
            await entered.wait()
        clock.advance(NS_PER_SECOND)
        # Longer-horizon reports here are future-stamped anomalies. They have
        # nevertheless been received and must not remain eligible predictions.
        for horizon in HORIZONS:
            value.observe_target(target(value, row, clock, horizon=horizon,
                                        identity='received-'+str(horizon)))
        if during_spool:
            release.set()
            await publication
        else:
            await value.publish(row)
        assert row.state['publication']['status'] == 'no_eligible_horizons'
        assert value.counters['publication_no_eligible_horizons'] == 1
        assert value.counters['publication_partial_batches'] == 0
        assert not redis.calls
        expect_mask(body(row), (), {str(h): ['target_received_before_publication'] for h in HORIZONS})
        expect_forecast_preservation(row, body(row), set(HORIZONS))
        assert row.frozen_json == frozen
        assert all(t['confirmed_redis_lead_ns'] is None for t in row.state['targets'].values())
    run(scenario())


@pytest.mark.parametrize('guard', ['wall_expiry', 'mono_expiry', 'stale_guard', 'suspension', 'stop', 'epoch'])
def test_surviving_subset_does_not_bypass_global_guards_after_spool(guard):
    async def scenario():
        value, clock, spool, _, redis = runtime()
        row = issue(value)
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock))
        entered, release = block_spool(value, spool)
        publication = asyncio.create_task(value.publish(row))
        await entered.wait()
        if guard == 'wall_expiry':
            clock.wall = row.decision.valid_until_wall_ns
        elif guard == 'mono_expiry':
            clock.mono = row.decision.decision_monotonic_ns + row.decision.valid_until_wall_ns-row.decision.decision_wall_ns
        elif guard == 'stale_guard':
            value.guard['monotonic_ns'] = clock.mono-3*NS_PER_SECOND-1
        elif guard == 'suspension':
            value.suspend('audit', 'test_audit_unavailable')
        elif guard == 'stop':
            value.stop('audit_size_cap')
        elif guard == 'epoch':
            value._publication_epoch += 1
        release.set()
        await publication
        assert not redis.calls
        assert row.state['publication']['status'] == 'preempted_after_spool'
    run(scenario())


@pytest.mark.parametrize('mutation', ['mask_excluded', 'wrong_target', 'wrong_price',
                                      'unavailable_quality', 'intent_only', 'no_attempt_clock'])
def test_ack_lead_requires_exact_attempted_payload_membership(mutation):
    value, clock, _, _, _ = runtime()
    row = issue(value)
    run(value.publish(row))
    payload = body(row)
    forecast = next(f for f in payload['forecasts'] if f['horizon_s'] == 1)
    if mutation == 'mask_excluded':
        payload['publication_eligibility']['eligible_horizons'].remove(1)
        payload['publication_eligibility']['excluded_horizons']['1'] = ['target_received_before_publication']
    elif mutation == 'wrong_target':
        forecast['target_source_timestamp_ms'] += 1000
    elif mutation == 'wrong_price':
        forecast['price'] = '999.000000000000000000'
    elif mutation == 'unavailable_quality':
        forecast['quality'] = 'unavailable'
    elif mutation == 'intent_only':
        payload['publication_state'] = 'intent'
    elif mutation == 'no_attempt_clock':
        payload.pop('publication_attempt_monotonic_ns')
        row.state['publication'].pop('attempt_monotonic_ns')
    row.state['publication']['payload_json'] = json.dumps(payload)
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=1))
    assert row.state['targets']['1']['confirmed_redis_lead_ns'] is None


def test_uncertain_redis_outcome_never_credits_surviving_subset():
    value, clock, _, _, redis = runtime()
    row = issue(value)
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=1))
    redis.failure = ConnectionError('reply lost after possible SET')
    run(value.publish(row))
    assert row.state['publication']['status'] == 'uncertain'
    expect_mask(body(row), (2, 3, 5, 10, 30), {'1': ['target_received_before_publication']})
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=2))
    assert row.state['targets']['2']['confirmed_redis_lead_ns'] is None


def test_restart_from_durable_intent_does_not_infer_post_spool_subset_or_ack():
    async def scenario():
        value, clock, spool, _, redis = runtime()
        row = issue(value)
        entered, release = block_spool(value, spool)
        at_redis = asyncio.Event()

        async def lose_process_before_ack(*args):
            redis.calls.append(args)
            at_redis.set()
            await asyncio.Future()

        redis.eval = lose_process_before_ack
        publication = asyncio.create_task(value.publish(row))
        await entered.wait()
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock, horizon=1))
        release.set()
        await at_redis.wait()
        durable = deepcopy(spool.rows[(row.decision.run_id, row.decision.decision_id)])
        expect_mask(json.loads(json.loads(durable['state_json'])['publication']['payload_json']), HORIZONS, {})
        expect_mask(json.loads(redis.calls[0][-2]), (2, 3, 5, 10, 30), {'1': ['target_received_before_publication']})
        publication.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publication
        recovered_store, recovered_redis = Store(), Redis()
        recovered = GhostRuntime(value.settings, recovered_store, recovered_redis, spool,
                                 wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono,
                                 disk_free=lambda: 2*RESERVE_BYTES)
        await recovered._recover(durable)
        result = recovered_store.persisted[-1]
        state = json.loads(result['state_json'])
        assert result['terminal'] and result['frozen_json'] == row.frozen_json
        assert state['publication']['restart_outcome'] == 'unconfirmed_after_restart'
        assert 'ack_monotonic_ns' not in state['publication']
        assert state['publication']['payload_json'] == json.loads(durable['state_json'])['publication']['payload_json']
        assert all(t['confirmed_redis_lead_ns'] is None for t in state['targets'].values())
        assert not recovered_redis.calls
    run(scenario())


def test_spool_failure_prevents_subset_publication_and_preserves_frozen_input():
    value, clock, spool, _, redis = runtime()
    row = issue(value)
    frozen = row.frozen_json
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock))
    spool.failure = OSError('durable intent could not be written')
    with pytest.raises(OSError):
        run(value.publish(row))
    assert not redis.calls
    assert row.frozen_json == frozen
    assert row.state['publication']['status'] == 'spool_unavailable'


def test_filtered_attempted_payload_survives_real_export_and_proof_roundtrip(tmp_path):
    value, clock, _, _, redis = runtime()
    row = issue(value)
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=1))
    run(value.publish(row))
    clock.advance(120*NS_PER_SECOND)
    value.finalize_due()
    validated = validate_record(row.record())
    assert validated['terminal']
    raw = encode_export_row(validated)
    path = tmp_path/'filtered-audit.jsonl'
    path.write_bytes(raw)
    digest = sha256(raw).hexdigest()
    verified = verify_export_file(path, expected_sha256=digest, expected_rows=1)
    assert verified['row_count'] == 1
    exported = json.loads(path.read_text())
    pub = json.loads(exported['state_json'])['publication']
    assert pub['payload_json'].encode() == redis.calls[0][-2]
    expect_mask(json.loads(pub['payload_json']), (2, 3, 5, 10, 30),
                {'1': ['target_received_before_publication']})
    assert exported['frozen_json'] == row.frozen_json
    proofs = list(iter_export_proofs(path, expected_sha256=digest, expected_rows=1))
    assert proofs == [{key: validated[key] for key in
                       ('run_id', 'decision_id', 'version', 'frozen_sha256', 'state_sha256')}]


@pytest.mark.parametrize('changed_field', ['eligible_horizons', 'excluded_horizons', 'filtered_price'])
def test_real_store_transition_rejects_rewriting_attempted_mask_or_price(changed_field):
    value, clock, _, _, _ = runtime()
    row = issue(value)
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=1))
    run(value.publish(row))
    original = validate_record(row.record())
    state = json.loads(original['state_json'])
    payload = json.loads(state['publication']['payload_json'])
    if changed_field == 'eligible_horizons':
        payload['publication_eligibility']['eligible_horizons'].insert(0, 1)
    elif changed_field == 'excluded_horizons':
        payload['publication_eligibility']['excluded_horizons'] = {}
    else:
        payload['forecasts'][0]['price'] = '100.000000000000000000'
    state['publication']['payload_json'] = json.dumps(payload)
    updated = validate_record(dict(original, version=original['version']+1,
                                   state_json=json.dumps(state)))
    with pytest.raises(GhostAuditConflict, match='attempted publication payload is immutable'):
        _transition(original, updated)


def test_recovery_preserves_acknowledged_filtered_bytes_and_only_eligible_lead():
    async def scenario():
        value, clock, _, _, redis = runtime()
        row = issue(value)
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock, horizon=1))
        await value.publish(row)
        clock.advance(NS_PER_SECOND)
        value.observe_target(target(value, row, clock, horizon=2))
        saved = validate_record(row.record())
        recovered, _, _, store, recovered_redis = runtime()
        await recovered._recover(saved)
        record = store.persisted[-1]
        state = json.loads(record['state_json'])
        assert record['terminal'] and record['frozen_json'] == row.frozen_json
        assert state['publication']['payload_json'].encode() == redis.calls[0][-2]
        assert state['targets']['1']['confirmed_redis_lead_ns'] is None
        assert int(state['targets']['2']['confirmed_redis_lead_ns']) == NS_PER_SECOND
        assert state['targets']['30']['status'] == 'restart_unmatched'
        assert state['targets']['30']['confirmed_redis_lead_ns'] is None
        assert not recovered_redis.calls
    run(scenario())


@pytest.mark.parametrize('version,eligible', [
    (None, False), ('ghost-canary-v5', False), ('ghost-canary-v6', False),
    ('ghost-canary-v3', True), ('ghost-canary-v4', True),
])
def test_only_known_complete_batch_versions_allow_missing_selection(version, eligible):
    value, clock, _, _, _ = runtime()
    row = issue(value)
    run(value.publish(row))
    payload = body(row)
    payload.pop('publication_eligibility')
    if version is None:
        payload.pop('runtime_version')
    else:
        payload['runtime_version'] = version
    row.state['publication']['payload_json'] = json.dumps(payload)
    clock.advance(NS_PER_SECOND)
    value.observe_target(target(value, row, clock, horizon=1))
    assert row.state['targets']['1']['confirmed_redis_lead_ns'] == (NS_PER_SECOND if eligible else None)
