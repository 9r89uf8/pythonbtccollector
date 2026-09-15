"""Fixed-grid observations preserve uncertainty, exact payloads and clock bounds."""
import asyncio
import base64
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
import json

import pytest

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent
from price_collector.ghost_twap_observer import (
    CLOCK_STEP_NS, HORIZONS, INTERVAL_MS, MAX_PAYLOAD_BYTES, ObservationFiles,
    analyze, classify_payload, observe, read_snapshot,
)

BASE = 1_800_000_000_000
NS = 1_000_000


def primed_engine(*, reconnect_contract=False):
    policy = {'enabled': True}
    if 'spot_reconnect_max_gap_ms' in GhostPolicy.__dataclass_fields__:
        policy['spot_reconnect_max_gap_ms'] = 10000 if reconnect_contract else 0
    engine = GhostTwapEngine('test-run', GhostPolicy(**policy))
    sequence = 0
    for second in range(-65, 1):
        sequence += 1
        stamp = BASE + second*1000
        engine.accept(PriceEvent('spot', Decimal('100'), stamp, stamp*NS,
                                 (100_000+second*1000)*NS, sequence, str(sequence)))
    sequence += 1
    engine.accept(PriceEvent('twap', Decimal('100'), BASE, BASE*NS, 100_000*NS,
                             sequence, str(sequence), 60))
    return engine


def payload(*, reconnect_contract=False):
    # Existing observer-v1 tests must continue to exercise actual v5 bytes,
    # regardless of the current engine's additive contract fields.
    engine = primed_engine(reconnect_contract=reconnect_contract)
    row = json.loads(engine.snapshot('1', BASE*NS, 100_000*NS).to_live_json())
    if not reconnect_contract:
        assert row.pop('spot_reconnect', None) is None
        assert row['policy'].pop('spot_reconnect_max_gap_ms', 0) == 0
        row['contract_version'] = 3
    else:
        row['contract_version'] = 4
        row['policy'].setdefault('spot_reconnect_max_gap_ms', 10000)
        row.setdefault('spot_reconnect', None)
    row.update(runtime_version='ghost-canary-v6' if reconnect_contract else 'ghost-canary-v5', publication_state='attempted',
               publication_attempt_wall_ns=str(BASE*NS+NS),
               publication_attempt_monotonic_ns=str(100_001*NS),
               publication_eligibility=dict(version=1, checked_wall_ns=str(BASE*NS+NS),
                    checked_monotonic_ns=str(100_001*NS), eligible_horizons=list(HORIZONS), excluded_horizons={}))
    return row


def raw_payload(row=None):
    return json.dumps(payload() if row is None else row).encode()


def classify(raw=None, ttl=1000, **overrides):
    kwargs = dict(start_ms=BASE, end_ms=BASE+3_600_000,
        read_start_wall_ns=(BASE+10)*NS, read_end_wall_ns=(BASE+11)*NS,
        read_start_monotonic_ns=100_010*NS, read_end_monotonic_ns=100_011*NS)
    kwargs.update(overrides)
    return classify_payload(raw_payload() if raw is None else raw, ttl, **kwargs)


def test_healthy_payload_and_exact_decimal_price_are_not_target_arrival_claims():
    result = classify()
    assert result['payload_valid'] and result['campaign_qualified']
    assert result['usable_horizons'] == list(HORIZONS)
    assert not result['reasons'] and not result['freshness_reasons']


def test_legacy_classification_fields_and_v1_reanalysis_label_are_unchanged(tmp_path):
    result = classify()
    assert result == dict(payload_valid=True, campaign_qualified=True,
        eligible_horizons=list(HORIZONS), usable_horizons=list(HORIZONS),
        reasons=[], freshness_reasons=[], run_id='test-run', decision_id='1', decision_time_ms=BASE)
    directory = tmp_path/'legacy'
    run_observer(directory, [(None, -2, 1), (raw_payload(), 1000, 1)])
    path = directory/'manifest.json'
    old_manifest = json.loads(path.read_bytes())
    old_manifest['campaign_membership'] = 'provisional_v5_decision_window_requires_audit_join'
    path.write_text(json.dumps(old_manifest))
    summary = analyze(directory)
    assert summary['version'] == 'ghost-observer-v1'
    assert summary['campaign_membership'] == old_manifest['campaign_membership']
    assert summary['full_hour']['usable'] == {str(h): 1 for h in HORIZONS}


def test_v6_contract4_is_usable_with_existing_observation_schema(tmp_path):
    body = raw_payload(payload(reconnect_contract=True))
    assert classify(body)['usable_horizons'] == list(HORIZONS)
    directory = tmp_path/'new'
    run_observer(directory, [(None, -2, 1), (body, 1000, 1)])
    summary = analyze(directory)
    assert summary['version'] == 'ghost-observer-v1'
    assert summary['campaign_membership'] == 'provisional_decision_window_requires_audit_join'
    assert summary['full_hour']['usable'] == {str(h): 1 for h in HORIZONS}


@pytest.mark.parametrize('runtime,contract', [
    ('ghost-canary-v5', 4), ('ghost-canary-v6', 3), ('ghost-canary-v7', 4),
    ('ghost-canary-v6', True), ('ghost-canary-v6', '4'),
])
def test_only_reviewed_runtime_contract_pairs_are_supported(runtime, contract):
    p = payload(reconnect_contract=True)
    p.update(runtime_version=runtime, contract_version=contract)
    assert not classify(raw_payload(p))['payload_valid']


@pytest.mark.parametrize('limit', [True, -1, 10001, '10000'])
def test_new_contract_reconnect_policy_is_bounded(limit):
    p = payload(reconnect_contract=True)
    p['policy']['spot_reconnect_max_gap_ms'] = limit
    assert not classify(raw_payload(p))['payload_valid']


def test_new_contract_requires_explicit_reconnect_snapshot():
    p = payload(reconnect_contract=True)
    p.pop('spot_reconnect')
    assert not classify(raw_payload(p))['payload_valid']


def recovery_payload(state='retained'):
    engine = primed_engine(reconnect_contract=True)
    sequence = engine.snapshot('before-gap', BASE*NS, 100_000*NS).included_sequence
    if state == 'cleared':
        engine.record_gap('spot', 'connection_end')  # Missing gap clocks are explicit.
    else:
        engine.record_gap('spot', 'connection_end', observed_wall_ns=(BASE+100)*NS,
                          observed_monotonic_ns=100_100*NS)
    when = 200
    if state in ('retained', 'future_cleared'):
        source = BASE+1000 if state == 'retained' else BASE+10000
        engine.accept(PriceEvent('spot', Decimal('101'), source, (BASE+1000)*NS,
                                 101_000*NS, sequence+1, 'resumed-spot'))
        when = 1100
    row = json.loads(engine.snapshot('after-gap', (BASE+when)*NS, (100_000+when)*NS).to_live_json())
    eligible = [f['horizon_s'] for f in row['forecasts'] if f['price'] is not None]
    row.update(runtime_version='ghost-canary-v6', publication_state='attempted',
        publication_attempt_wall_ns=str((BASE+when+1)*NS),
        publication_attempt_monotonic_ns=str((100_000+when+1)*NS),
        publication_eligibility=dict(version=1,
            checked_wall_ns=str((BASE+when+1)*NS), checked_monotonic_ns=str((100_000+when+1)*NS),
            eligible_horizons=eligible,
            excluded_horizons={str(f['horizon_s']): f['reasons'] for f in row['forecasts'] if f['price'] is None}))
    clocks = dict(read_start_wall_ns=(BASE+when+10)*NS, read_end_wall_ns=(BASE+when+11)*NS,
                  read_start_monotonic_ns=(100_000+when+10)*NS, read_end_monotonic_ns=(100_000+when+11)*NS)
    return row, clocks


def test_retained_recovery_payload_uses_normal_freshness_without_continuity_claim():
    p, clocks = recovery_payload()
    assert p['spot_reconnect']['status'] == 'retained'
    result = classify(raw_payload(p), **clocks)
    assert result['payload_valid'] and result['usable_horizons'] == list(HORIZONS)
    assert 'history_complete' not in result and 'recovery_math_verified' not in result


@pytest.mark.parametrize('state', ['waiting', 'cleared', 'future_cleared'])
def test_unavailable_reconnect_states_remain_observable_without_fresh_prices(state):
    p, clocks = recovery_payload(state)
    result = classify(raw_payload(p), **clocks)
    assert result['payload_valid'] and not result['usable_horizons']
    assert not result['eligible_horizons']


@pytest.mark.parametrize('mutation', [
    lambda p: p['spot_reconnect'].update(status='unknown'),
    lambda p: p['spot_reconnect'].update(gap_wall_ns=None),
    lambda p: p['spot_reconnect'].update(previous_spot=None),
    lambda p: p['spot_reconnect']['first_post_gap_spot'].update(source_timestamp_ms=BASE),
    lambda p: p['spot_reconnect']['first_post_gap_spot'].update(sequence=1),
    lambda p: p['spot_reconnect']['previous_spot'].update(value='NaN'),
    lambda p: p['policy'].update(spot_reconnect_max_gap_ms=0),
    lambda p: p.update(last_gaps=[]),
])
def test_retained_metadata_cannot_bypass_resume_requirements(mutation):
    p, clocks = recovery_payload()
    mutation(p)
    assert not classify(raw_payload(p), **clocks)['payload_valid']


def test_waiting_state_cannot_claim_a_current_spot_or_selected_price():
    p, clocks = recovery_payload('waiting')
    p['current_spot'] = p['spot_reconnect']['previous_spot']
    assert not classify(raw_payload(p), **clocks)['payload_valid']


def test_recovery_reason_bound_preserves_maximum_hard_gap_reason():
    p, clocks = recovery_payload('cleared')
    p['spot_reconnect']['reason'] = 'hard_gap:' + 'x'*256
    assert classify(raw_payload(p), **clocks)['payload_valid']
    p['spot_reconnect']['reason'] += 'x'
    assert not classify(raw_payload(p), **clocks)['payload_valid']


@pytest.mark.parametrize('ttl,reason', [(-1, 'no_expiry'), (-2, 'inconsistent_ttl'), (0, 'expiry_boundary_ambiguous'), (1, 'expiry_boundary_ambiguous')])
def test_ttl_is_conservative_at_reply_boundary(ttl, reason):
    result = classify(ttl=ttl)
    assert result['payload_valid']
    assert reason in result['freshness_reasons']
    assert result['eligible_horizons'] and not result['usable_horizons']


def test_expired_payload_and_negative_monotonic_input_age_cannot_be_usable():
    result = classify(read_end_wall_ns=(BASE+3000)*NS, read_end_monotonic_ns=103_000*NS)
    assert 'payload_expired' in result['freshness_reasons']
    assert not result['usable_horizons']
    p = payload()
    p['current_spot']['received_monotonic_ns'] = str(100_012*NS)
    result = classify(raw_payload(p))
    assert not result['payload_valid']
    assert not result['usable_horizons']


def test_clock_jump_unknown_even_when_ttl_is_long():
    result = classify(read_end_wall_ns=(BASE+11)*NS+CLOCK_STEP_NS+1)
    assert 'read_clock_anomaly' in result['freshness_reasons']
    assert not result['usable_horizons']


def test_partial_batch_does_not_revive_masked_price():
    p = payload()
    p['forecasts'][0].update(price=None, quality='unavailable')
    p['publication_eligibility']['eligible_horizons'].remove(1)
    p['publication_eligibility']['excluded_horizons']['1'] = ['target_received_before_publication']
    result = classify(raw_payload(p))
    assert result['payload_valid'] and result['usable_horizons'] == [2, 3, 5, 10, 30]
    p['forecasts'][0]['price'] = '100.000000000000000000'
    assert not classify(raw_payload(p))['payload_valid']


@pytest.mark.parametrize('mutation', [
    lambda p: p.update(runtime_version='ghost-canary-v4'),
    lambda p: p.update(decision_wall_ns=str((BASE-1)*NS)),
    lambda p: p.update(publication_state='intent'),
    lambda p: p['forecasts'][0].update(price=100.0),
    lambda p: p['forecasts'][0].update(price='NaN'),
    lambda p: p['forecasts'][0].update(target_source_timestamp_ms=BASE+2000),
    lambda p: p['publication_eligibility']['eligible_horizons'].append(1),
    lambda p: p['publication_eligibility'].update(excluded_horizons={'1': []}),
])
def test_invalid_or_foreign_payloads_are_not_qualified(mutation):
    p = payload()
    mutation(p)
    result = classify(raw_payload(p))
    assert not result['payload_valid'] and not result['usable_horizons']


def test_duplicate_json_and_oversize_are_rejected():
    assert not classify(b'{"run_id":"x","run_id":"y"}')['payload_valid']
    assert classify(b'x'*(MAX_PAYLOAD_BYTES+1))['reasons'] == ['oversized_payload']
    assert not classify(raw_payload().replace(b'"gap_count": 0', b'"gap_count": NaN'))['payload_valid']
    assert not classify(b'['*1500+b'0'+b']'*1500)['payload_valid']


class Clock:
    def __init__(self):
        self.wall = BASE*NS
        self.mono = 100_000*NS
    def advance(self, ms):
        self.wall += ms*NS
        self.mono += ms*NS
    async def sleep(self, seconds):
        self.advance(round(seconds*1000))
        await asyncio.sleep(0)


class FakeRedis:
    def __init__(self, clock, responses=None):
        self.clock = clock
        self.responses = iter(responses or [])
        self.reads = []
        self.transactions = []
        self.commands = []
        self.closed = 0
    def pipeline(self, transaction):
        self.transactions.append(transaction)
        return self
    async def __aenter__(self):
        self.commands = []
        return self
    async def __aexit__(self, *args):
        self.closed += 1
    def get(self, key):
        self.commands.append(('GET', key))
        return self
    def pttl(self, key):
        self.commands.append(('PTTL', key))
        return self
    async def execute(self):
        self.reads.append(self.clock.wall)
        response = next(self.responses, (None, -2, 1))
        self.clock.advance(response[2])
        if isinstance(response[0], Exception):
            raise response[0]
        return response[:2]


def run_observer(path, responses=None, duration_ms=1000, **kwargs):
    clock = kwargs.pop('clock', Clock())
    client = FakeRedis(clock, responses)
    result = asyncio.run(observe(client, path, BASE, duration_ms=duration_ms,
        wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono, sleep=clock.sleep, **kwargs))
    return result, client, clock


def test_atomic_transaction_contains_only_get_and_pttl():
    client = FakeRedis(Clock())
    assert asyncio.run(read_snapshot(client)) == (None, -2)
    assert client.transactions == [True]
    assert [c[0] for c in client.commands] == ['GET', 'PTTL']
    assert client.closed == 1


def test_exact_grid_and_absence_errors_and_deduplicated_bodies(tmp_path):
    raw = raw_payload()
    responses = [(None, -2, 1), (raw, 1000, 1), (raw, 1000, 1), (OSError('not key absence'), 0, 1)]
    result, client, clock = run_observer(tmp_path/'run', responses)
    assert result['status'] == 'complete' and result['recorded_bins'] == 10
    assert result['unique_payloads'] == 1
    records = [json.loads(line) for line in (tmp_path/'run'/'samples.jsonl').read_bytes().splitlines()]
    assert [r['index'] for r in records] == list(range(10))
    assert records[2]['status'] == 'error'
    assert all(BASE*NS <= stamp < (BASE+1000)*NS for stamp in client.reads[1:])
    assert clock.wall >= (BASE+1000)*NS
    summary = analyze(tmp_path/'run')
    assert summary['full_hour']['statuses'] == {'present': 2, 'error': 1, 'absent': 7}
    assert summary['full_hour']['unknown_bins'] == 1
    assert summary['run_ids'] == ['test-run']
    ledger = json.loads((tmp_path/'run'/'payloads.jsonl').read_bytes())
    assert base64.b64decode(ledger['raw_base64']) == raw
    assert ledger['sha256'] == sha256(raw).hexdigest()


def test_long_read_marks_missed_slots_without_catchup(tmp_path):
    result, client, _ = run_observer(tmp_path/'run', [(None, -2, 1), (None, -2, 350)])
    rows = [json.loads(x) for x in (tmp_path/'run'/'samples.jsonl').read_bytes().splitlines()]
    assert rows[1]['status'] == 'missed' and rows[1]['index'] == 1 and rows[1]['count'] == 2
    assert rows[2]['index'] == 3
    assert len(client.reads) == 9  # readiness + eight actual observations
    assert analyze(tmp_path/'run')['full_hour']['statuses']['missed'] == 2


def test_interruption_finalizes_prefix_without_fabricating_rest(tmp_path):
    async def scenario():
        stop = asyncio.Event()
        clock = Clock()
        async def sleep(seconds):
            await clock.sleep(seconds)
            stop.set()
        return await observe(FakeRedis(clock), tmp_path/'run', BASE, stop=stop,
            duration_ms=1000, wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono, sleep=sleep)
    result = asyncio.run(scenario())
    assert result['status'] == 'incomplete' and result['reason'] == 'interrupted'
    assert result['recorded_bins'] == 1
    assert (tmp_path/'run'/'ready.json').exists()
    summary = analyze(tmp_path/'run')
    assert summary['full_hour']['statuses'] == {'absent': 1, 'unrecorded': 9}


def test_no_overwrite_and_existing_key_blocks_readiness(tmp_path):
    directory = tmp_path/'old'
    directory.mkdir()
    with pytest.raises(FileExistsError):
        run_observer(directory)
    result, _, _ = run_observer(tmp_path/'new', [(raw_payload(), 1000, 1)])
    assert result['status'] == 'incomplete'
    assert result['reason'] == 'ghost_key_must_be_absent_at_readiness'
    assert not (tmp_path/'new'/'ready.json').exists()


def test_size_cap_stops_and_remaining_bins_stay_unknown(tmp_path):
    result, _, _ = run_observer(tmp_path/'run', max_bytes=33_000)
    assert result['reason'] == 'observer_file_size_cap'
    assert result['status'] == 'incomplete'
    assert analyze(tmp_path/'run')['full_hour']['unknown_bins'] == 10


def test_tampered_file_fails_hash_validation(tmp_path):
    run_observer(tmp_path/'run')
    with (tmp_path/'run'/'samples.jsonl').open('ab') as stream:
        stream.write(b'{}\n')
    with pytest.raises(ValueError, match='hash mismatch'):
        analyze(tmp_path/'run')


def test_postwarmup_and_minute_bins_count_fixed_denominator(tmp_path):
    result, _, _ = run_observer(tmp_path/'run', duration_ms=66_000)
    summary = analyze(tmp_path/'run')
    assert summary['full_hour']['planned_bins'] == 660
    assert summary['post_65_seconds']['planned_bins'] == 10
    assert summary['minute_bins'][0]['planned_bins'] == 600
    assert summary['minute_bins'][1]['planned_bins'] == 60
    assert result['status'] == 'complete'


def rewrite_samples_and_hash(directory, mutate):
    path = directory/'samples.jsonl'
    rows = [json.loads(x) for x in path.read_bytes().splitlines()]
    mutate(rows)
    raw = b''.join(json.dumps(x).encode()+b'\n' for x in rows)
    path.write_bytes(raw)
    manifest = json.loads((directory/'manifest.json').read_bytes())
    manifest['files']['samples.jsonl'] = dict(bytes=len(raw), sha256=sha256(raw).hexdigest())
    (directory/'manifest.json').write_text(json.dumps(manifest))


@pytest.mark.parametrize('mutation', [
    lambda rows: rows[0].update(usable_horizons=[30]),
    lambda rows: rows[0].update(payload_valid=False),
    lambda rows: rows[0].update(read_duration_ns=999),
    lambda rows: rows[0].update(clock_anomaly=True),
    lambda rows: rows[0].update(read_crosses_campaign_end=True),
])
def test_analyzer_recomputes_raw_payload_and_clocks_even_with_new_manifest_hash(tmp_path, mutation):
    run_observer(tmp_path/'run', [(None, -2, 1), (raw_payload(), 1000, 1)])
    rewrite_samples_and_hash(tmp_path/'run', mutation)
    with pytest.raises(ValueError, match='classification mismatch'):
        analyze(tmp_path/'run')


def test_absence_cannot_be_marked_usable_even_with_correct_manifest_hash(tmp_path):
    run_observer(tmp_path/'run')
    rewrite_samples_and_hash(tmp_path/'run', lambda rows: rows[0].update(usable_horizons=[1]))
    with pytest.raises(ValueError, match='non-present'):
        analyze(tmp_path/'run')


def test_future_ready_then_explicit_start_has_no_early_samples(tmp_path):
    clock = Clock()
    clock.wall -= 500*NS
    clock.mono -= 500*NS
    result, client, _ = run_observer(tmp_path/'run', clock=clock)
    ready = json.loads((tmp_path/'run'/'ready.json').read_bytes())
    assert int(ready['ready_wall_ns']) < BASE*NS
    assert all(stamp >= BASE*NS for stamp in client.reads[1:])
    assert result['status'] == 'complete'


def test_timed_out_read_is_error_and_pipeline_is_closed(tmp_path):
    class SlowRedis(FakeRedis):
        async def execute(self):
            if self.reads:
                await asyncio.sleep(1)
            return await super().execute()
    async def scenario():
        clock = Clock()
        client = SlowRedis(clock)
        result = await observe(client, tmp_path/'run', BASE, duration_ms=100,
            wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono, sleep=clock.sleep)
        return result, client
    result, client = asyncio.run(scenario())
    row = json.loads((tmp_path/'run'/'samples.jsonl').read_bytes())
    assert row['status'] == 'error' and row['error'] == 'TimeoutError'
    assert client.closed == 2
    assert result['status'] == 'complete'


def test_complete_claim_requires_full_grid_and_qualified_hashes_exposed(tmp_path):
    run_observer(tmp_path/'run', [(None, -2, 1), (raw_payload(), 1000, 1)])
    summary = analyze(tmp_path/'run')
    assert summary['observed_qualified_payload_sha256'] == [sha256(raw_payload()).hexdigest()]
    assert summary['observed_run_ids'] == ['test-run']
    manifest = json.loads((tmp_path/'run'/'manifest.json').read_bytes())
    manifest['recorded_bins'] -= 1
    manifest['unrecorded_bins'] += 1
    (tmp_path/'run'/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='invalid manifest grid'):
        analyze(tmp_path/'run')


def test_ttl_one_ms_is_ambiguous_even_for_submillisecond_read():
    result = classify(ttl=1, read_end_wall_ns=(BASE+10)*NS+100_000,
                      read_end_monotonic_ns=100_010*NS+100_000)
    assert 'expiry_boundary_ambiguous' in result['freshness_reasons']
    assert not result['usable_horizons']


def test_current_source_future_at_original_receipt_is_not_fresh():
    p = payload()
    p['current_spot']['source_timestamp_ms'] = BASE+1
    result = classify(raw_payload(p))
    assert result['payload_valid']
    assert 'spot_source_future_at_receipt' in result['freshness_reasons']
    assert not result['usable_horizons']


def test_clock_step_between_bins_survives_sleep_until_next_record(tmp_path):
    async def scenario():
        clock = Clock()
        slept = False
        async def sleep(seconds):
            nonlocal slept
            if not slept:
                slept = True
                clock.advance(40)
                clock.wall += 10*NS  # forward wall step, still before next bin
            else:
                await clock.sleep(seconds)
        return await observe(FakeRedis(clock), tmp_path/'run', BASE, duration_ms=1000,
            wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono, sleep=sleep)
    result = asyncio.run(scenario())
    rows = [json.loads(line) for line in (tmp_path/'run'/'samples.jsonl').read_bytes().splitlines()]
    assert rows[1]['clock_anomaly'] is True and rows[1]['clock_step'] is not None
    assert analyze(tmp_path/'run')['full_hour']['unknown_bins'] == 1
    assert result['status'] == 'complete'


def test_clock_step_that_skips_bins_keeps_current_sample_anomalous(tmp_path):
    async def scenario():
        clock = Clock()
        jumped = False
        async def sleep(seconds):
            nonlocal jumped
            if not jumped:
                jumped = True
                clock.advance(10)
                clock.wall += 250*NS
            else:
                await clock.sleep(seconds)
        return await observe(FakeRedis(clock), tmp_path/'run', BASE, duration_ms=1000,
            wall_ns=lambda: clock.wall, mono_ns=lambda: clock.mono, sleep=sleep)
    asyncio.run(scenario())
    rows = [json.loads(line) for line in (tmp_path/'run'/'samples.jsonl').read_bytes().splitlines()]
    assert rows[1]['status'] == 'missed'
    assert rows[2]['clock_anomaly'] is True
    assert analyze(tmp_path/'run')['full_hour']['unknown_bins'] == 2
