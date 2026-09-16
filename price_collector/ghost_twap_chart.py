"""Frozen first-publication comparisons from a bounded compact-record snapshot.

This module is called by the maintenance worker, never by the API request path.
Selection precedes outcome exclusions: an inconvenient first publication cannot
be replaced by a later, more accurate revision of the same target and horizon.
"""
from __future__ import annotations

from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from hashlib import sha256

from . import ghost_twap_accuracy as accuracy

HORIZONS = (3, 5, 10, 30)
WINDOW_MS = 900_000
MATCHING_AGE_MS = 120_000
SOURCE_AGE_ALLOWANCE_MS = 5_000
MAX_ROWS = 10_000
MAX_BODY_BYTES = 32 * 1024 * 1024
METHOD = "first_acknowledged_eligible_per_target_and_horizon"
EXCLUSIONS = ('missing_official', 'restart_unmatched', 'conflicted', 'invalid_clock', 'late_ack')


def _ns(value):
    return accuracy.integer(value)


def _price(value):
    return accuracy.money(value, positive=True)


def _text(value):
    return format(value.quantize(accuracy.E18, rounding=ROUND_HALF_EVEN), '.18f')


def _point(decision, target):
    """Describe the already-selected first publication without replacing it."""
    first = target['first_event']
    held = decision['current_twap']
    flags = []
    if first is None:
        flags.append('missing_official')
    if target['target_status'] == 'restart_unmatched':
        flags.append('restart_unmatched')
    if target['conflicted']:
        flags.append('conflicted')
    wall, mono = _ns(decision['decision_wall_ns']), _ns(decision['decision_monotonic_ns'])
    ack_wall, ack_mono = _ns(decision['ack_wall_ns']), _ns(decision['ack_monotonic_ns'])
    invalid = bool(decision['causality_invalid'] or target['clock_anomaly'])
    invalid |= not (wall <= _ns(decision['attempt_wall_ns']) <= ack_wall and
                    mono <= _ns(decision['attempt_monotonic_ns']) <= ack_mono)
    for event in (decision['current_spot'], held):
        invalid |= event is None or not (
            _ns(event['source_timestamp_ms']) * 1_000_000 <= wall and
            _ns(event['received_wall_ns']) <= wall and
            _ns(event['received_monotonic_ns']) <= mono)
    if first is not None:
        invalid |= (target['target_status'] != 'matched' or
                    _ns(first['source_timestamp_ms']) != target['target_source_timestamp_ms'] or
                    _ns(first['source_timestamp_ms']) * 1_000_000 > _ns(first['received_wall_ns']) or
                    _ns(first['received_wall_ns']) < wall or
                    _ns(first['received_monotonic_ns']) < mono)
        if ack_mono >= _ns(first['received_monotonic_ns']):
            flags.append('late_ack')
        elif _ns(first['received_wall_ns']) < ack_wall:
            invalid = True
    if invalid:
        flags.append('invalid_clock')
    # Reasons overlap (a restart-unmatched target also lacks an official print).
    # The primary status is stable; counts.unpaired is the unique denominator.
    primary = next((name for name in ('invalid_clock', 'conflicted', 'restart_unmatched',
                                      'missing_official', 'late_ack') if name in flags), 'paired')
    paired = primary == 'paired'
    actual = None if first is None else _price(first['value'])
    forecast = _price(target['forecast_price'])
    held_price = None if held is None else _price(held['value'])
    error = Decimal(forecast) - Decimal(actual) if paired else None
    held_error = Decimal(held_price) - Decimal(actual) if paired else None
    return dict(target_source_timestamp_ms=target['target_source_timestamp_ms'],
        forecast_price=forecast, actual_price=actual, held_twap_price=held_price,
        error_usd=None if error is None else _text(error),
        held_error_usd=None if held_error is None else _text(held_error),
        status=primary, flags=flags, run_id=decision['run_id'], decision_id=decision['decision_id'],
        ack_wall_ns=str(ack_wall),
        target_received_wall_ns=None if first is None else str(_ns(first['received_wall_ns'])),
        lead_ns=str(_ns(first['received_monotonic_ns']) - ack_mono) if paired else None,
        counts=target['counts'])


def compose_comparison(snapshot, now_ms):
    """Decode, verify, freeze, and score the store's single-snapshot result.

    All financial work uses Decimal. Monotonic clocks are compared only within
    a decision's recorded process, never between runs. ACK wall time orders the
    first publication across runs, with identity tie breakers.
    """
    accuracy.require(type(now_ms) is int and now_ms >= 0, 'Invalid chart generation time')
    rows = snapshot['raw_rows']
    accuracy.require(len(rows) <= MAX_ROWS and snapshot['row_count'] == len(rows),
                     'Comparison snapshot exceeds bounded row budget')
    accuracy.require(snapshot['body_bytes'] <= MAX_BODY_BYTES, 'Comparison snapshot exceeds bounded body budget')
    start, end = snapshot['window_start_ms'], snapshot['window_end_ms']
    accuracy.require(type(start) is int and type(end) is int and 0 <= start <= end <= now_ms
                     and end - start <= WINDOW_MS, 'Invalid comparison window')
    selected = {}
    actual_bytes = 0
    for row in rows:
        raw = row['body_json']
        accuracy.require(isinstance(raw, str), 'Missing bounded comparison body')
        encoded = raw.encode('utf-8')
        actual_bytes += len(encoded)
        accuracy.require(actual_bytes <= MAX_BODY_BYTES, 'Comparison snapshot exceeds bounded body budget')
        accuracy.require(sha256(encoded).hexdigest() == row['body_sha256'], 'Stored compact body hash mismatch')
        record = accuracy.verify_compact(accuracy.decode(raw))
        decision = record['decision']
        accuracy.require((decision['run_id'], decision['decision_id'], decision['created_ms']) ==
                         (row['run_id'], row['decision_id'], row['created_ms']), 'Compact row identity mismatch')
        if decision['publication_status'] != 'acknowledged':
            continue
        order = (_ns(decision['ack_wall_ns']), decision['run_id'], decision['decision_id'])
        for target in record['horizons']:
            h, stamp = target['horizon_s'], target['target_source_timestamp_ms']
            if (h not in HORIZONS or type(stamp) is not int or not start <= stamp < end or
                    target['attempted_eligible'] is not True or target['forecast_price'] is None or
                    target['quality'] not in ('healthy', 'degraded')):
                continue
            key = h, stamp
            if key not in selected or order < selected[key][0]:
                selected[key] = order, decision, target
    accuracy.require(actual_bytes == snapshot['body_bytes'], 'Comparison body size mismatch')
    groups = []
    with localcontext() as context:
        context.prec = 256
        for horizon in HORIZONS:
            points = [_point(decision, target) for (h, _), (_, decision, target) in
                      sorted(selected.items()) if h == horizon]
            counts = dict(selected=len(points), paired=0, unpaired=0, **{name: 0 for name in EXCLUSIONS})
            ghost_sum = held_sum = Decimal(0)
            for point in points:
                for flag in point['flags']:
                    counts[flag] += 1
                if point['status'] == 'paired':
                    counts['paired'] += 1
                    ghost_sum += abs(Decimal(point['error_usd']))
                    held_sum += abs(Decimal(point['held_error_usd']))
                else:
                    counts['unpaired'] += 1
            n = counts['paired']
            groups.append(dict(horizon_s=horizon, counts=counts,
                ghost_mae_usd=_text(ghost_sum/n) if n else None,
                held_mae_usd=_text(held_sum/n) if n else None, points=points))
    return dict(schema_version=1, status='available' if selected else 'unavailable',
        reason=None if selected else 'no_compacted_predictions', method=METHOD,
        generated_at_ms=now_ms, window_start_ms=start, window_end_ms=end,
        window_duration_ms=end-start, window_lag_ms=now_ms-end,
        matching_age_ms=MATCHING_AGE_MS,
        persistence_watermark_ms=snapshot['persistence_watermark_ms'],
        runtime_watermark_ms=snapshot['runtime_watermark_ms'],
        source_records=len(rows), source_body_bytes=actual_bytes, horizons=groups)
