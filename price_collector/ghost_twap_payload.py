"""Independent bounded ghost wire validation for read-only delivery.

No producer, observer, database, or forecast-engine imports. Prices remain
original decimal strings; this module checks structure and clocks, not means.
Producer monotonic values are used only as differences within their own clock
domain. API expiry is separately anchored to the API's local read clocks.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
import re

MAX_PAYLOAD_BYTES = 64 * 1024
MAX_INTEGER = 9_223_372_036_854_775_807
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000
HORIZONS = (1, 2, 3, 5, 10, 30)
COUNT_NAMES = ('observed', 'carried', 'pending', 'future', 'missing')


class InvalidGhostPayload(ValueError):
    """The original bytes cannot safely be delivered as this wire contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InvalidGhostPayload(message)


def _integer(value, name: str, minimum: int = 0, maximum: int = MAX_INTEGER) -> int:
    _require(type(value) is int and minimum <= value <= maximum, 'invalid integer: ' + name)
    return value


def _clock(value, name: str, *, signed: bool = False) -> int:
    pattern = r'-?(?:0|[1-9][0-9]{0,18})' if signed else r'(?:0|[1-9][0-9]{0,18})'
    _require(isinstance(value, str) and re.fullmatch(pattern, value) is not None, 'invalid clock string: ' + name)
    parsed = int(value)
    _require((-MAX_INTEGER if signed else 0) <= parsed <= MAX_INTEGER, 'clock out of bounds: ' + name)
    return parsed


def _text(value, name: str, limit: int = 256) -> str:
    _require(isinstance(value, str) and 0 < len(value) <= limit
             and all(ord(c) >= 32 and ord(c) != 127 for c in value), 'invalid text: ' + name)
    return value


def _price(value) -> str:
    _require(isinstance(value, str) and re.fullmatch(r'(?:0|[1-9][0-9]{0,19})\.[0-9]{18}', value) is not None,
             'price must be an exact E18 decimal string')
    _require(Decimal(value) > 0, 'price must be positive')
    return value


def _reasons(value) -> tuple[str, ...]:
    _require(isinstance(value, list) and len(value) <= 32, 'invalid reasons')
    result = tuple(_text(v, 'reason') for v in value)
    _require(len(set(result)) == len(result), 'duplicate reasons')
    return result


def _no_float(value):
    raise InvalidGhostPayload('JSON floating/nonfinite numbers are forbidden')


def _object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


@dataclass(frozen=True)
class InputPrice:
    feed: str
    value: str
    source_timestamp_ms: int
    received_wall_ns: int
    producer_received_monotonic_ns: int
    sequence: int
    event_id: str
    window_s: int | None


@dataclass(frozen=True)
class SlotCounts:
    observed: int
    carried: int
    pending: int
    future: int
    missing: int


@dataclass(frozen=True)
class GhostForecast:
    horizon_s: int
    target_source_timestamp_ms: int | None
    price: str | None
    quality: str
    reasons: tuple[str, ...]
    counts: SlotCounts
    estimated_arrival_wall_ns: int | None
    estimated_remaining_ns: int | None
    max_interior_carry_ms: int


@dataclass(frozen=True)
class GhostPayload:
    raw: bytes
    run_id: str
    decision_id: int
    decision_wall_ns: int
    producer_decision_monotonic_ns: int
    publication_attempt_wall_ns: int
    producer_publication_attempt_monotonic_ns: int
    valid_until_wall_ns: int
    source_max_age_ms: int
    receipt_max_age_ms: int
    current_spot: InputPrice | None
    current_twap: InputPrice | None
    forecasts: tuple[GhostForecast, ...]
    eligible_horizons: tuple[int, ...]

    @property
    def has_eligible_prices(self) -> bool:
        return bool(self.eligible_horizons)

    @property
    def anchor_source_timestamp_ms(self) -> int | None:
        return None if self.current_twap is None else self.current_twap.source_timestamp_ms

    @property
    def anchor_received_wall_ns(self) -> int | None:
        return None if self.current_twap is None else self.current_twap.received_wall_ns

    @property
    def anchor_value(self) -> str | None:
        return None if self.current_twap is None else self.current_twap.value


@dataclass(frozen=True)
class GhostRead:
    payload: GhostPayload
    received_wall_ns: int
    received_monotonic_ns: int
    api_deadline_monotonic_ns: int

    def remaining_ns(self, *, wall_ns: int, monotonic_ns: int) -> int:
        _integer(wall_ns, 'API wall clock')
        _integer(monotonic_ns, 'API monotonic clock')
        if wall_ns < self.received_wall_ns or monotonic_ns < self.received_monotonic_ns:
            return 0
        return max(0, min(self.payload.valid_until_wall_ns - wall_ns,
                          self.api_deadline_monotonic_ns - monotonic_ns))

    def is_fresh(self, *, wall_ns: int, monotonic_ns: int) -> bool:
        return self.remaining_ns(wall_ns=wall_ns, monotonic_ns=monotonic_ns) > 0


def _input(record, feed: str, wall: int, mono: int, *, allow_source_future: bool = False) -> InputPrice | None:
    if record is None:
        return None
    fields = {'feed', 'value', 'source_timestamp_ms', 'received_wall_ns', 'received_monotonic_ns',
              'sequence', 'event_id', 'window_s', 'received_ms'}
    _require(isinstance(record, dict) and set(record) == fields, 'invalid current-input layout')
    _require(record['feed'] == feed and record['window_s'] == (60 if feed == 'twap' else None), 'wrong source identity')
    if feed == 'twap':
        _require(type(record['window_s']) is int, 'invalid TWAP window type')
    source = _integer(record['source_timestamp_ms'], 'input source')
    _require(source % 1000 == 0, 'input source is not second-aligned')
    receipt_wall = _clock(record['received_wall_ns'], 'input receipt wall')
    receipt_mono = _clock(record['received_monotonic_ns'], 'input receipt monotonic')
    _require(receipt_wall <= wall and receipt_mono <= mono, 'input received after decision')
    _require(_integer(record['received_ms'], 'input received ms') == receipt_wall // NS_PER_MS, 'input receipt clock mismatch')
    _require(allow_source_future or source * NS_PER_MS <= receipt_wall, 'input source is future at receipt')
    return InputPrice(feed, _price(record['value']), source, receipt_wall, receipt_mono,
                      _integer(record['sequence'], 'input sequence', 1), _text(record['event_id'], 'event id'), record['window_s'])


def _fresh_at(event: InputPrice, wall: int, mono: int, source_cap: int, receipt_cap: int) -> bool:
    ages = (wall-event.source_timestamp_ms*NS_PER_MS, wall-event.received_wall_ns,
            mono-event.producer_received_monotonic_ns)
    return min(ages) >= 0 and ages[0] <= source_cap*NS_PER_MS and max(ages[1:]) <= receipt_cap*NS_PER_MS


def _reconnect(p, spot, wall, mono, source_cap, receipt_cap, limit, carry, eligible):
    recovery = p['spot_reconnect']
    if recovery is None:
        return
    fields = {'gap_ordinal', 'gap_wall_ns', 'gap_monotonic_ns', 'previous_spot', 'first_post_gap_spot', 'status', 'reason'}
    _require(isinstance(recovery, dict) and set(recovery) == fields, 'invalid reconnect layout')
    ordinal = _integer(recovery['gap_ordinal'], 'reconnect ordinal', 1, p['gap_count'])
    status = recovery['status']
    _require(status in ('waiting', 'retained', 'cleared'), 'unknown reconnect state')
    _text(recovery['reason'], 'reconnect reason', 265)
    gap_wall = None if recovery['gap_wall_ns'] is None else _clock(recovery['gap_wall_ns'], 'gap wall')
    gap_mono = None if recovery['gap_monotonic_ns'] is None else _clock(recovery['gap_monotonic_ns'], 'gap monotonic')
    _require((gap_wall is None or gap_wall <= wall) and (gap_mono is None or gap_mono <= mono), 'gap after decision')
    previous = _input(recovery['previous_spot'], 'spot', wall, mono, allow_source_future=status == 'cleared')
    first = _input(recovery['first_post_gap_spot'], 'spot', wall, mono, allow_source_future=status == 'cleared')
    if status == 'cleared':
        return
    _require(limit > 0 and previous is not None and gap_wall is not None and gap_mono is not None,
             'retention lacks policy/clocks/input')
    _require(any(g['feed'] == 'spot' and g['reason'] == 'connection_end' and g['ordinal'] == ordinal for g in p['last_gaps']),
             'retention lacks transport gap')
    _require(_fresh_at(previous, gap_wall, gap_mono, source_cap, receipt_cap), 'previous input invalid at gap')
    if status == 'waiting':
        _require(first is None and spot is None and not eligible, 'waiting recovery exposes a price')
        return
    _require(first is not None and spot is not None, 'retained recovery lacks resumed input')
    deltas = ((first.source_timestamp_ms-previous.source_timestamp_ms)*NS_PER_MS,
              first.received_wall_ns-previous.received_wall_ns,
              first.producer_received_monotonic_ns-previous.producer_received_monotonic_ns)
    _require(deltas[0] > 0 and min(deltas) >= 0 and max(deltas) <= min(limit, carry)*NS_PER_MS
             and first.received_wall_ns >= gap_wall and first.producer_received_monotonic_ns >= gap_mono
             and first.sequence > previous.sequence and spot.sequence >= first.sequence
             and _fresh_at(first, first.received_wall_ns, first.producer_received_monotonic_ns, source_cap, receipt_cap),
             'invalid retained recovery bounds')


def _parse(raw: bytes) -> GhostPayload:
    _require(type(raw) is bytes and 0 < len(raw) <= MAX_PAYLOAD_BYTES, 'payload bytes exceed bound or have wrong type')
    p = json.loads(raw.decode('utf-8'), parse_float=_no_float, parse_constant=_no_float, object_pairs_hook=_object)
    _require(isinstance(p, dict), 'payload must be an object')
    _require(p['runtime_version'] == 'ghost-canary-v6' and type(p['contract_version']) is int and p['contract_version'] == 4,
             'unsupported runtime/contract')
    _require(p['model_version'] == 'chainlink-60s-offset3-v1' and p['publication_state'] == 'attempted', 'unsupported model/publication state')
    _require(p['price_precision'] == 18 and type(p['price_precision']) is int and p['context_precision'] == 80
             and type(p['context_precision']) is int and p['rounding'] == 'ROUND_HALF_EVEN', 'unsupported precision contract')
    run_id = _text(p['run_id'], 'run id', 128)
    _require(re.fullmatch(r'[A-Za-z0-9_-]+', run_id) is not None, 'unsafe run identity')
    _require(isinstance(p['decision_id'], str) and re.fullmatch(r'[1-9][0-9]{0,18}', p['decision_id']) is not None,
             'invalid decision identity')
    decision_id = _integer(int(p['decision_id']), 'decision id', 1)
    _require(_integer(p['publication_sequence'], 'publication sequence', 1) == decision_id, 'publication sequence differs from decision')
    wall, mono = _clock(p['decision_wall_ns'], 'decision wall'), _clock(p['decision_monotonic_ns'], 'decision monotonic')
    _require(_integer(p['decision_time_ms'], 'decision ms') == wall // NS_PER_MS, 'decision clocks disagree')
    complete_wall = _clock(p['computation_completed_wall_ns'], 'computation wall')
    complete_mono = _clock(p['computation_completed_monotonic_ns'], 'computation monotonic')
    intent_wall = _clock(p['publication_intent_wall_ns'], 'intent wall')
    intent_mono = _clock(p['publication_intent_monotonic_ns'], 'intent monotonic')
    attempt_wall = _clock(p['publication_attempt_wall_ns'], 'attempt wall')
    attempt_mono = _clock(p['publication_attempt_monotonic_ns'], 'attempt monotonic')
    _require(wall <= complete_wall <= intent_wall <= attempt_wall and mono <= complete_mono <= intent_mono <= attempt_mono,
             'producer publication clocks regress')
    policy = p['policy']
    _require(isinstance(policy, dict) and set(policy) == {'enabled', 'source_max_age_ms', 'receipt_max_age_ms',
             'max_carry_ms', 'history_ms', 'max_events', 'spot_reconnect_max_gap_ms'} and policy['enabled'] is True, 'invalid policy')
    source_cap = _integer(policy['source_max_age_ms'], 'source cap', 1, 5000)
    receipt_cap = _integer(policy['receipt_max_age_ms'], 'receipt cap', 1, 3000)
    carry = _integer(policy['max_carry_ms'], 'carry cap', 1, 10000)
    _integer(policy['history_ms'], 'history budget', 62000+source_cap, 120000)
    _integer(policy['max_events'], 'event budget', 1, 1024)
    reconnect_limit = _integer(policy['spot_reconnect_max_gap_ms'], 'reconnect cap', 0, 10000)
    spot, anchor = _input(p['current_spot'], 'spot', wall, mono), _input(p['current_twap'], 'twap', wall, mono)
    included = None if p['included_sequence'] is None else _integer(p['included_sequence'], 'included sequence', 1)
    inputs = tuple(e for e in (spot, anchor) if e is not None)
    _require(not inputs or included is not None and all(e.sequence <= included for e in inputs), 'current input exceeds included sequence')
    _require(len({e.sequence for e in inputs}) == len(inputs), 'current inputs reuse sequence')
    global_reasons = _reasons(p['reasons'])
    for feed, event in (('spot', spot), ('twap', anchor)):
        _require(event is not None or 'missing_' + feed in global_reasons, 'missing input lacks global reason')
    gap_count = _integer(p['gap_count'], 'gap count')
    _require(isinstance(p['last_gaps'], list) and len(p['last_gaps']) <= 2, 'invalid gap markers')
    seen_feeds = set()
    for gap in p['last_gaps']:
        _require(isinstance(gap, dict) and set(gap) == {'feed', 'reason', 'ordinal', 'after_sequence'}
                 and gap['feed'] in ('spot', 'twap') and gap['feed'] not in seen_feeds, 'invalid gap marker identity')
        seen_feeds.add(gap['feed'])
        _text(gap['reason'], 'gap reason')
        _integer(gap['ordinal'], 'gap ordinal', 1, gap_count)
        if gap['after_sequence'] is not None:
            _integer(gap['after_sequence'], 'gap sequence')
    selection = p['publication_eligibility']
    _require(isinstance(selection, dict) and set(selection) == {'version', 'checked_wall_ns', 'checked_monotonic_ns',
             'eligible_horizons', 'excluded_horizons'} and type(selection['version']) is int and selection['version'] == 1,
             'invalid publication selection')
    _require(_clock(selection['checked_wall_ns'], 'selection wall') == attempt_wall and
             _clock(selection['checked_monotonic_ns'], 'selection monotonic') == attempt_mono, 'selection/attempt mismatch')
    eligible = selection['eligible_horizons']
    _require(isinstance(eligible, list) and all(type(h) is int for h in eligible)
             and eligible == [h for h in HORIZONS if h in eligible], 'invalid/duplicate eligible horizons')
    excluded = selection['excluded_horizons']
    _require(isinstance(excluded, dict) and set(excluded) == {str(h) for h in HORIZONS if h not in eligible}, 'selection does not partition horizons')
    _require(isinstance(p['forecasts'], list) and len(p['forecasts']) == 6, 'six forecasts required')
    forecasts = []
    for h, f in zip(HORIZONS, p['forecasts']):
        _require(isinstance(f, dict) and type(f['horizon_s']) is int and f['horizon_s'] == h, 'forecast horizon identity mismatch')
        counts = f['counts']
        _require(isinstance(counts, dict) and set(counts) == set(COUNT_NAMES), 'invalid count fields')
        numbers = tuple(_integer(counts[name], 'slot count', 0, 60) for name in COUNT_NAMES)
        _require(sum(numbers) == 60, 'slot counts do not sum to 60')
        max_carry = _integer(f['max_interior_carry_ms'], 'interior carry', 0, carry)
        _require((max_carry == 0) == (counts['carried'] == 0), 'interior carry count/maximum mismatch')
        reasons = _reasons(f['reasons'])
        _require(set(global_reasons) <= set(reasons), 'forecast omits global reasons')
        target = None if anchor is None else anchor.source_timestamp_ms+h*1000
        _require(f['target_source_timestamp_ms'] == target and
                 (target is None or type(f['target_source_timestamp_ms']) is int), 'forecast target does not match anchor')
        _require(f['slot_start_index'] is None if anchor is None else
                 type(f['slot_start_index']) is int and f['slot_start_index'] == h-1,
                 'wrong forecast window offset')
        eta = None if anchor is None else anchor.received_wall_ns+h*NS_PER_SECOND
        if anchor is None:
            _require(f['market'] is None and f['estimated_arrival_wall_ns'] is None
                     and f['estimated_remaining_ns'] is None and counts['missing'] == 60, 'anchor-free forecast has target data')
            remaining = None
        else:
            start = target // 300000 * 300000
            _require(isinstance(f['market'], dict) and all(type(v) is int for v in f['market'].values())
                     and f['market'] == {'market_id': start // 300000, 'market_start_ms': start, 'market_end_ms': start+300000},
                     'target market mismatch')
            _require(_clock(f['estimated_arrival_wall_ns'], 'ETA') == eta, 'ETA does not match anchor receipt')
            remaining = _clock(f['estimated_remaining_ns'], 'remaining ETA', signed=True)
            _require(remaining == eta-wall, 'remaining ETA mismatch')
        _require(type(f['estimate_overdue']) is bool and f['estimate_overdue'] == (eta is not None and eta < wall), 'overdue flag mismatch')
        value = f['price']
        if h in eligible:
            value = _price(value)
            _require(not reasons and counts['missing'] == 0 and f['quality'] == ('degraded' if counts['carried'] else 'healthy'),
                     'eligible price has invalid quality/counts/reasons')
        else:
            _require(value is None and f['quality'] == 'unavailable' and reasons
                     and _reasons(excluded[str(h)]) == reasons, 'excluded horizon exposes price or mismatched reasons')
        forecasts.append(GhostForecast(h, target, value, f['quality'], reasons, SlotCounts(*numbers), eta, remaining, max_carry))
    _reconnect(p, spot, wall, mono, source_cap, receipt_cap, reconnect_limit, carry, eligible)
    if eligible:
        _require(spot is not None and anchor is not None and not global_reasons
                 and all(_fresh_at(e, wall, mono, source_cap, receipt_cap) for e in inputs), 'eligible prices lack fresh current inputs')
    deadlines = [wall] if global_reasons else []
    for event in inputs:
        deadlines.extend(((event.source_timestamp_ms+source_cap)*NS_PER_MS,
                          event.received_wall_ns+receipt_cap*NS_PER_MS,
                          wall+receipt_cap*NS_PER_MS-(mono-event.producer_received_monotonic_ns)))
    derived_deadline = min(deadlines, default=wall)
    declared_deadline = _clock(p['valid_until_wall_ns'], 'valid until')
    _require(_integer(p['valid_until_ms'], 'valid until ms') == declared_deadline // NS_PER_MS, 'expiry clocks disagree')
    _require(declared_deadline <= derived_deadline, 'declared expiry exceeds verified input deadline')
    _require(not eligible or (attempt_wall < declared_deadline and
             attempt_mono-mono < declared_deadline-wall), 'eligible attempt was already expired')
    return GhostPayload(raw, run_id, decision_id, wall, mono, attempt_wall, attempt_mono, declared_deadline,
                        source_cap, receipt_cap, spot, anchor, tuple(forecasts), tuple(eligible))


def parse_ghost_payload(raw: bytes) -> GhostPayload:
    """Validate original v6/contract4 bytes without recalculation/re-encoding."""
    try:
        return _parse(raw)
    except InvalidGhostPayload:
        raise
    except (KeyError, TypeError, ValueError, ArithmeticError, UnicodeError, RecursionError, OverflowError) as exc:
        raise InvalidGhostPayload('malformed ghost payload') from exc


def bind_read_clock(payload: GhostPayload, *, wall_ns: int, monotonic_ns: int) -> GhostRead:
    """Anchor the remaining safe wall lifetime to this API process's clock."""
    _require(isinstance(payload, GhostPayload), 'parsed payload required')
    _integer(wall_ns, 'API read wall')
    _integer(monotonic_ns, 'API read monotonic')
    _require(wall_ns >= payload.publication_attempt_wall_ns, 'producer publication is future at API read')
    # The producer's own monotonic elapsed duration may exhaust validity sooner
    # than its wall clock. Translate that remaining duration at publication to
    # a wall deadline before anchoring it to this process's independent clock.
    producer_remaining = (payload.valid_until_wall_ns-payload.decision_wall_ns
                          -(payload.producer_publication_attempt_monotonic_ns
                            -payload.producer_decision_monotonic_ns))
    remaining = max(0, min(payload.valid_until_wall_ns-wall_ns,
                          payload.publication_attempt_wall_ns+producer_remaining-wall_ns,
                          payload.receipt_max_age_ms*NS_PER_MS))
    _require(monotonic_ns+remaining <= MAX_INTEGER, 'API deadline exceeds clock bound')
    return GhostRead(payload, wall_ns, monotonic_ns, monotonic_ns+remaining)
