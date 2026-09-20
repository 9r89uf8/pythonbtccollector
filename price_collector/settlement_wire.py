"""Bounded read-only settlement validation; no engine or database imports."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Context, Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
import json
import re

from price_collector.ghost_twap_payload import (
    GhostPayload, GhostForecast, SlotCounts, InvalidGhostPayload, _require, _integer,
    _clock, _text, _price, _input, _fresh_at, _no_float, _object,
)
from price_collector.market import market_for_sample_second
from price_collector.settlement_history import cohort_key

KEY = 'btc:live:ghost_settlement'
CHANNEL = KEY + ':updates'
HISTORY_KEY = KEY + ':history'
NS_MS = 1_000_000
VALIDATION_CONTEXT = Context(prec=80, rounding=ROUND_HALF_EVEN)
PRICE_ENDPOINT = 'https://polymarket.com/api/crypto/crypto-price'
TWAP_SOURCE = 'https://data.chain.link/streams/btc-usd-twap-60s-streams'


def _iso(value):
    return datetime.fromtimestamp(value // 1000, timezone.utc).isoformat().replace('+00:00', 'Z')


def parse_settlement_payload(raw: bytes) -> GhostPayload:
    """Reuse delivery clocks only; this remains a separate versioned contract."""
    try:
        _require(type(raw) is bytes and 0 < len(raw) <= 65536, 'settlement byte budget')
        p = json.loads(raw, parse_float=_no_float, parse_constant=_no_float, object_pairs_hook=_object)
        legacy = p.get('schema_version') == 2 and p.get('rule_version') == 'historical-settlement-v1'
        current = (p.get('schema_version') == 3 and p.get('rule_version') == 'historical-settlement-v2'
                   and type(p.get('observation_window_s')) is int and p['observation_window_s'] == 60
                   and type(p.get('sampling_interval_ms')) is int and p['sampling_interval_ms'] == 2000)
        window_s = 30 if legacy else 60
        _require(type(p['schema_version']) is int and (legacy or current) and p['kind'] == 'settlement'
                 and p['model_version'] == 'chainlink-60s-offset3-settlement-v1'
                 and (not legacy or 'observation_window_s' not in p)
                 and 'threshold_bps' not in p, 'unsupported settlement contract')
        _require(p['history_cohort'] == cohort_key(p), 'incompatible history cohort')
        run = _text(p['run_id'], 'run id', 128)
        _require(re.fullmatch(r'[A-Za-z0-9_-]+', run) is not None, 'invalid run id')
        _require(isinstance(p['decision_id'], str) and re.fullmatch(r'[1-9][0-9]{0,18}', p['decision_id']) is not None,
                 'invalid decision id')
        identifier = _integer(int(p['decision_id']), 'decision id', 1)
        wall, mono = _clock(p['decision_wall_ns'], 'decision wall'), _clock(p['decision_monotonic_ns'], 'decision mono')
        _require(_integer(p['decision_time_ms'], 'decision ms') == wall // NS_MS, 'decision time mismatch')
        market = market_for_sample_second(wall // 1_000_000_000 * 1000)
        end = market.market_end_ms
        _require(_integer(p['market_id'], 'market id') == market.market_id
                 and _integer(p['market_start_ms'], 'market start') == market.market_start_ms
                 and _integer(p['market_end_ms'], 'market end') == end
                 and _integer(p['target_source_timestamp_ms'], 'target') == end
                 and _integer(p['remaining_ms'], 'remaining ms') == end - wall // NS_MS
                 and 0 < end * NS_MS - wall <= window_s * 1000 * NS_MS, 'wrong ending market')
        attempt = _clock(p['publication_attempt_wall_ns'], 'attempt wall')
        attempt_mono = _clock(p['publication_attempt_monotonic_ns'], 'attempt mono')
        expiry = _clock(p['valid_until_wall_ns'], 'expiry')
        _require(p['publication_state'] == 'attempted' and wall <= attempt < expiry <= end * NS_MS
                 and mono <= attempt_mono and attempt_mono - mono < expiry - wall
                 and p['valid_until_ms'] == expiry // NS_MS, 'publication clocks invalid')
        policy = p['policy']
        _require(policy['enabled'] is True, 'policy disabled')
        source_cap = _integer(policy['source_max_age_ms'], 'source cap', 1, 5000)
        receipt_cap = _integer(policy['receipt_max_age_ms'], 'receipt cap', 1, 3000)
        carry = _integer(policy['max_carry_ms'], 'carry cap', 1, 10000)
        spot, twap = _input(p['current_spot'], 'spot', wall, mono), _input(p['current_twap'], 'twap', wall, mono)
        _require(spot is not None and twap is not None, 'missing current inputs')
        deadlines = [end * NS_MS]
        for event in (spot, twap):
            _require(_fresh_at(event, wall, mono, source_cap, receipt_cap), 'stale decision input')
            deadlines.extend(((event.source_timestamp_ms + source_cap) * NS_MS,
                              event.received_wall_ns + receipt_cap * NS_MS,
                              wall + event.producer_received_monotonic_ns + receipt_cap * NS_MS - mono))
        _require(expiry <= min(deadlines), 'expiry exceeds frozen input lifetime')
        horizon = _integer(p['source_horizon_s'], 'horizon', 1, window_s + 5)
        _require(twap.source_timestamp_ms + horizon * 1000 == end, 'wrong horizon')
        quality = p['quality']
        _require(p['status'] == 'available' and quality in ('healthy', 'degraded') and p['reasons'] == [],
                 'no eligible settlement')
        counts = SlotCounts(**{name: _integer(p['counts'][name], name, 0, 60)
                             for name in ('observed', 'carried', 'pending', 'future', 'missing')})
        _require(sum(vars(counts).values()) == 60 and counts.missing == 0, 'invalid slot counts')
        _require(quality == ('degraded' if counts.carried else 'healthy'), 'quality/count mismatch')
        maximum = _integer(p['max_interior_carry_ms'], 'carry age', 0, carry)
        _require((maximum == 0) == (counts.carried == 0), 'carry age/count mismatch')
        reference = p['reference']
        _require(reference['status'] == 'available' and reference['conflicted'] is False
                 and reference['identity_validated'] is True and reference['completed'] is not True
                 and reference['market_id'] == market.market_id
                 and reference['market_start_ms'] == market.market_start_ms and reference['market_end_ms'] == end
                 and reference['settlement_reference'] == 'chainlink_twap'
                 and type(reference['settlement_window_s']) is int and reference['settlement_window_s'] == 60
                 and reference['settlement_source_url'] == TWAP_SOURCE
                 and reference['settlement_rule_version'] == 'btc-5m-twap-60'
                 and reference['observation_status'] == 'ok' and reference['http_status'] == 200
                 and reference['source_url'] == PRICE_ENDPOINT,
                 'invalid reference identity')
        condition = _text(reference['condition_id'], 'condition id')
        up, down = _text(reference['up_token_id'], 'up token'), _text(reference['down_token_id'], 'down token')
        _require(up != down and condition, 'invalid outcome identity')
        _require(reference['request_params'] == {
            'symbol': 'BTC', 'variant': 'fiveminute', 'twapEnabled': 'true', 'twapLookbackSeconds': '60',
            'eventStartTime': _iso(market.market_start_ms), 'endDate': _iso(end),
        }, 'reference request differs from ending market')
        for suffix, upper in (('wall_ns', wall), ('monotonic_ns', mono)):
            requested = _clock(reference['requested_' + suffix], 'reference request')
            received = _clock(reference['received_' + suffix], 'reference receipt')
            available = _clock(reference['available_' + suffix], 'reference availability')
            _require(requested <= received <= available <= upper, 'noncausal reference')
            if suffix == 'wall_ns':
                _require(requested >= market.market_start_ms * NS_MS, 'reference requested before market start')
        _require(isinstance(reference['price_to_beat'], str)
                 and re.fullmatch(r'[0-9]{1,20}(?:\.[0-9]{1,18})?', reference['price_to_beat']) is not None
                 and 0 < Decimal(reference['price_to_beat']) < Decimal('1e20'), 'invalid opening price')
        price = _price(p['projected_price'])
        with localcontext(VALIDATION_CONTEXT):
            opening = Decimal(reference['price_to_beat'])
            for name, expected_price in (('ghost', price), ('twap', twap.value), ('spot', spot.value)):
                signal = p['signals'][name]
                _require(_price(signal['price']) == expected_price, 'signal price differs from input')
                _require(signal['side'] in ('up', 'down', 'tie') and 'qualifies' not in signal, 'invalid signal')
                for field in ('signed_lead_usd', 'lead_bps'):
                    _require(isinstance(signal[field], str) and re.fullmatch(r'-?[0-9]{1,30}\.[0-9]{18}', signal[field]) is not None,
                             'invalid signal decimal')
                lead = Decimal(expected_price) - opening
                bps = (lead / opening * Decimal(10000)).quantize(Decimal('1e-18'))
                _require(Decimal(signal['signed_lead_usd']) == lead and Decimal(signal['lead_bps']) == bps,
                         'signal lead differs from price and reference')
                side = 'up' if lead > 0 else 'down' if lead < 0 else 'tie'
                _require(signal['side'] == side, 'signal side mismatch')
        forecast = GhostForecast(horizon, end, price, quality, (), counts, None, None, maximum)
        return GhostPayload(raw, run, identifier, wall, mono, attempt, attempt_mono, expiry,
                            source_cap, receipt_cap, spot, twap, (forecast,), (horizon,))
    except (KeyError, TypeError, ValueError, InvalidOperation, UnicodeError) as exc:
        if isinstance(exc, InvalidGhostPayload):
            raise
        raise InvalidGhostPayload('invalid settlement payload') from exc
