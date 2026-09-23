"""Focused first-publication cohort checks with real engine compact evidence."""
import asyncio
from copy import deepcopy
from hashlib import sha256

import pytest

from price_collector import ghost_twap_accuracy as accuracy
from price_collector.ghost_twap_chart import compose_comparison, MAX_BODY_BYTES, MAX_ROWS
from price_collector.ghost_twap_retention import GhostRetentionStore
from test_ghost_twap_retention_postgres import terminal_record

BASE = 1_800_000_000_000


@pytest.fixture
def compact():
    record = asyncio.run(terminal_record(BASE + 100_000))
    return accuracy.compact_record(record, finalized_as_of_wall_ns=(BASE + 220_000)*1_000_000)


def row(record):
    record = deepcopy(record)
    record.pop('compact_sha256', None)
    record['compact_sha256'] = sha256(accuracy.canonical_bytes(record)).hexdigest()
    raw = accuracy.canonical_bytes(record).decode()
    d = record['decision']
    return dict(run_id=d['run_id'], decision_id=d['decision_id'], created_ms=d['created_ms'],
                body_json=raw, body_sha256=sha256(raw.encode()).hexdigest())


def snapshot(records):
    rows = [row(r) for r in records]
    return dict(raw_rows=rows, row_count=len(rows), body_bytes=sum(len(r['body_json'].encode()) for r in rows),
                window_start_ms=BASE, window_end_ms=BASE+150_000,
                persistence_watermark_ms=BASE+155_000, runtime_watermark_ms=None)


def build(records):
    return compose_comparison(snapshot(records), BASE+300_000)


def second(record):
    result = deepcopy(record)
    result['decision']['decision_id'] += '-later'
    result['decision']['ack_wall_ns'] += 1
    result['decision']['ack_monotonic_ns'] += 1
    for target in result['horizons']:
        target['forecast_price'] = '101.000000000000000000'
    return result


def group(result, horizon=5):
    return next(g for g in result['horizons'] if g['horizon_s'] == horizon)


def target(record, horizon=5):
    return next(t for t in record['horizons'] if t['horizon_s'] == horizon)


def test_freezes_first_ack_before_later_improvement_and_uses_identical_baseline_pairs(compact):
    result = build([second(compact), compact])
    g = group(result)
    assert g['counts']['selected'] == g['counts']['paired'] == 1
    assert g['ghost_mae_usd'] == g['held_mae_usd'] == '1.000000000000000000'
    assert g['points'][0]['forecast_price'] == '100.000000000000000000'
    assert g['points'][0]['error_usd'] == '-1.000000000000000000'
    assert g['points'][0]['lead_ns'].isdigit()
    assert [g['horizon_s'] for g in result['horizons']] == [3, 5, 10, 30]


@pytest.mark.parametrize('defect', ['conflict', 'causality', 'late_ack', 'restart', 'wall_regression'])
def test_outcome_exclusions_never_replace_the_first_publication(compact, defect):
    later = second(compact)
    first = deepcopy(compact)
    t, d = target(first), first['decision']
    if defect == 'conflict':
        t['conflicted'] = True
    elif defect == 'causality':
        d['causality_invalid'] = True
    elif defect == 'late_ack':
        d['ack_monotonic_ns'] = t['first_event']['received_monotonic_ns']
    elif defect == 'restart':
        t.update(first_event=None, target_status='restart_unmatched')
    else:
        d['attempt_wall_ns'] = d['decision_wall_ns']-1
    g = group(build([first, later]))
    assert g['counts']['selected'] == g['counts']['unpaired'] == 1
    assert g['counts']['paired'] == 0
    assert g['ghost_mae_usd'] is None and g['held_mae_usd'] is None
    assert g['points'][0]['decision_id'] == first['decision']['decision_id']
    assert g['points'][0]['error_usd'] is None
    if defect == 'restart':
        assert g['counts']['missing_official'] == g['counts']['restart_unmatched'] == 1


def test_eligibility_mask_is_applied_before_first_selection(compact):
    later = second(compact)
    target(compact)['attempted_eligible'] = False
    g = group(build([compact, later]))
    assert g['points'][0]['decision_id'] == later['decision']['decision_id']
    assert g['ghost_mae_usd'] == '0.000000000000000000'
    assert g['held_mae_usd'] == '1.000000000000000000'


def test_ack_ties_across_runs_are_deterministic_without_cross_run_monotonic_comparison(compact):
    later = second(compact)
    later['decision'].update(run_id='a', ack_wall_ns=compact['decision']['ack_wall_ns'])
    compact['decision']['run_id'] = 'z'
    expected = group(build([later, compact]))['points'][0]
    assert expected['run_id'] == 'a'
    assert group(build([compact, later]))['points'][0] == expected


def test_corrupt_hash_or_truncated_budget_fails_closed(compact):
    snap = snapshot([compact])
    snap['raw_rows'][0]['body_sha256'] = '0'*64
    with pytest.raises(ValueError, match='body hash'):
        compose_comparison(snap, BASE+300_000)
    snap = snapshot([compact])
    snap['row_count'] = MAX_ROWS+1
    with pytest.raises(ValueError, match='row budget'):
        compose_comparison(snap, BASE+300_000)
    snap = snapshot([compact])
    snap['body_bytes'] = MAX_BODY_BYTES+1
    with pytest.raises(ValueError, match='body budget'):
        compose_comparison(snap, BASE+300_000)


def test_window_is_target_aligned_and_mae_has_no_unplotted_pairs(compact):
    snap = snapshot([compact])
    snap['window_start_ms'] = BASE+105_000
    snap['window_end_ms'] = BASE+110_000
    result = compose_comparison(snap, BASE+300_000)
    assert group(result, 3)['counts']['selected'] == 0
    assert group(result, 5)['counts']['paired'] == 1
    assert group(result, 10)['counts']['selected'] == 0


def test_empty_snapshot_reports_collecting_with_null_mae():
    result = build([])
    assert result['status'] == 'unavailable' and result['reason'] == 'no_compacted_predictions'
    assert all(g['ghost_mae_usd'] is None and not g['points'] for g in result['horizons'])


def test_store_snapshot_uses_one_bounded_statement_and_passes_both_watermarks():
    class Context:
        def __init__(self, value): self.value = value
        async def __aenter__(self): return self.value
        async def __aexit__(self, *args): pass

    class Connection:
        calls = []
        def transaction(self): return Context(self)
        async def execute(self, *args): pass
        async def fetch(self, sql, *args):
            self.calls.append((sql, args))
            return [dict(window_start_ms=0, window_end_ms=100_000, persistence_watermark_ms=105_000,
                         row_count=0, body_bytes=0, run_id=None, decision_id=None, created_ms=None,
                         body_json=None, body_sha256=None)]

    class Pool:
        def __init__(self): self.connection = Connection()
        def acquire(self, **kwargs): return Context(self.connection)

    pool = Pool()
    result = asyncio.run(GhostRetentionStore(pool).comparison_snapshot(300_000, runtime_watermark_ms=110_000))
    assert result['raw_rows'] == [] and result['runtime_watermark_ms'] == 110_000
    assert len(pool.connection.calls) == 1
    sql, args = pool.connection.calls[0]
    assert args == (300_000, 110_000, 120_000, 5_000, 900_000, 10_001, MAX_BODY_BYTES)
    assert 'min(created_ms)' in sql and 'LIMIT $6' in sql and 's.body_bytes<=$7' in sql
    # Scalar bounds become index conditions. A CROSS JOIN can instead scan
    # every older compact row in order, applying these bounds as a join filter.
    candidates = sql.split('candidates AS MATERIALIZED (', 1)[1].split('), sizes AS (', 1)[0]
    assert 'CROSS JOIN' not in candidates
    assert 'c.created_ms>=(SELECT greatest(0,window_start_ms-30000) FROM chart_window)' in candidates
    assert 'c.created_ms<(SELECT window_end_ms+$4::bigint FROM chart_window)' in candidates
    assert 'ORDER BY c.created_ms,c.run_id,c.decision_id LIMIT $6' in candidates
