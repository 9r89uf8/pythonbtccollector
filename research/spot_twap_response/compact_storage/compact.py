"""Research-only compact audit encoder; no runtime, database or network imports.

Version 1 keeps shared decision metadata and six horizon outcomes. It deliberately
does not retain slot arrays or original payload bytes. Lineage hashes identify
those deleted bytes but cannot reconstruct or re-verify their arithmetic.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path
import tempfile

HORIZONS = (1, 2, 3, 5, 10, 30)
CATEGORIES = ('observed', 'carried', 'pending', 'future', 'missing')
NS = 1000000000
MAX_LINE = 1048576
E18 = Decimal('0.000000000000000001')
EVENT_FIELDS = ('value', 'source_timestamp_ms', 'received_wall_ns',
                'received_monotonic_ns', 'sequence', 'event_id', 'window_s')
STAGES = ('intent', 'attempt', 'ack')


def require(value, message):
    if not value:
        raise ValueError(message)


def reject(value):
    raise ValueError('JSON floating/nonfinite value: ' + value)


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'Duplicate JSON key')
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, parse_float=reject, parse_constant=reject, object_pairs_hook=object_pairs)


def canonical_bytes(value):
    # Round-trip with strict decoding also rejects accidental Python floats.
    raw = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    decode(raw)
    return raw


def integer(value, *, nullable=False):
    if value is None and nullable:
        return None
    require(type(value) is int or isinstance(value, str) and value.lstrip('-').isdigit(), 'Expected integer')
    return int(value)


def money(value, *, positive=False, nullable=False):
    if value is None and nullable:
        return None
    require(isinstance(value, str), 'Financial value must be a string')
    result = Decimal(value)
    with localcontext() as context:
        context.prec = 80
        require(result.is_finite() and abs(result) < Decimal('1e20') and result.quantize(E18) == result,
                'Financial value does not fit NUMERIC(38,18)')
    require(not positive or result > 0, 'Price must be positive')
    # A fixed spelling makes NUMERIC(38,18) round-trips canonical. Original
    # source spelling remains identified by the original evidence hashes.
    return format(abs(result) if result.is_zero() else result, '.18f')


def event_record(event, feed):
    if event is None:
        return None
    require(event['feed'] == feed and event['window_s'] == (60 if feed == 'twap' else None), 'Event identity')
    result = {key:event[key] for key in EVENT_FIELDS}
    result['value'] = money(result['value'], positive=True)
    for name in ('source_timestamp_ms', 'received_wall_ns', 'received_monotonic_ns', 'sequence'):
        result[name] = integer(result[name])
        require(result[name] >= 0, 'Negative event clock/sequence')
    require(result['source_timestamp_ms'] % 1000 == 0 and result['sequence'] > 0, 'Invalid event stamp/sequence')
    require(isinstance(result['event_id'], str) and bool(result['event_id']), 'Missing event ID')
    return result


def compact_record(row, *, finalized_as_of_wall_ns):
    """Compact only an already-terminal recorded version after decision+120s.

    The as-of wall time is an offline age gate, not invented terminal evidence.
    Shutdown/restart-unmatched records remain censored even after this gate.
    """
    with localcontext() as context:
        context.prec = 80
        require(row['terminal'] is True, 'Cannot compact nonterminal record')
        for kind in ('frozen', 'state'):
            require(sha256(row[kind+'_json'].encode()).hexdigest() == row[kind+'_sha256'], 'Original hash mismatch')
        frozen, state = decode(row['frozen_json']), decode(row['state_json'])
        identity = row['run_id'], row['decision_id']
        require((frozen['run_id'], frozen['decision_id']) == identity, 'Frozen identity mismatch')
        require(frozen['runtime_version'] == 'ghost-canary-v6' and frozen['contract_version'] == 4,
                'Unsupported runtime/contract')
        wall, mono = integer(frozen['decision_wall_ns']), integer(frozen['decision_monotonic_ns'])
        require(integer(row['decision_wall_ns']) == wall and row['created_ms'] == wall//1000000,
                'Decision row clock mismatch')
        require(integer(finalized_as_of_wall_ns) >= wall+120*NS, 'Matching window has not aged 120 seconds')
        pub = state['publication']
        attempted = pub.get('attempt_monotonic_ns') is not None
        wire = None if pub.get('payload_json') is None else decode(pub['payload_json'])
        if attempted:
            require(wire is not None and wire['publication_state'] == 'attempted', 'Attempt without exact payload')
        require(pub['status'] not in ('acknowledged', 'uncertain') or attempted, 'Publication without attempt')
        eligible, excluded, checked = set(), {}, {'wall_ns':None, 'monotonic_ns':None}
        if wire is not None:
            require((wire['run_id'], wire['decision_id']) == identity, 'Wire identity mismatch')
            for key in ('decision_wall_ns','decision_monotonic_ns','current_spot','current_twap',
                        'contract_version','policy','valid_until_wall_ns'):
                require(wire[key] == frozen[key], 'Wire/frozen shared metadata mismatch')
            selection = wire['publication_eligibility']
            require(selection['version'] == 1, 'Unsupported eligibility mask')
            ids = selection['eligible_horizons']
            require(all(type(h) is int for h in ids) and ids == [h for h in HORIZONS if h in ids], 'Bad eligibility IDs')
            require(set(selection['excluded_horizons']) == {str(h) for h in HORIZONS if h not in ids}, 'Mask partition')
            checked = {k:integer(selection['checked_'+k]) for k in checked}
            if attempted:
                eligible = set(ids)
                require(all(checked[k] == integer(pub['attempt_'+k]) for k in checked), 'Selection/attempt clocks')
            excluded = selection['excluded_horizons']
            require([f['horizon_s'] for f in wire['forecasts']] == list(HORIZONS), 'Wire horizon domain')
        decision = dict(run_id=row['run_id'], decision_id=row['decision_id'], created_ms=row['created_ms'],
            decision_wall_ns=wall, decision_monotonic_ns=mono, record_version=integer(row['version']),
            frozen_sha256=row['frozen_sha256'], state_sha256=row['state_sha256'],
            attempted_payload_sha256=sha256(pub['payload_json'].encode()).hexdigest() if attempted else None,
            model_version=frozen['model_version'], runtime_version=frozen['runtime_version'],
            contract_version=frozen['contract_version'], policy=frozen['policy'],
            publication_eligibility_policy=frozen['publication_eligibility_policy'],
            campaign_start_ms=frozen['runtime_policy']['canary_start_ms'],
            current_spot=event_record(frozen['current_spot'], 'spot'), current_twap=event_record(frozen['current_twap'], 'twap'),
            included_sequence=integer(frozen['included_sequence']),
            computation_completed_wall_ns=integer(state['computation_completed_wall_ns']),
            computation_completed_monotonic_ns=integer(state['computation_completed_monotonic_ns']),
            valid_until_wall_ns=integer(frozen['valid_until_wall_ns']), publication_status=pub['status'],
            eligibility_checked_wall_ns=checked['wall_ns'], eligibility_checked_monotonic_ns=checked['monotonic_ns'],
            global_reasons=frozen['reasons'], causality_invalid=bool(state.get('causality_invalid')),
            shutdown=state.get('shutdown'), restart_reconciled=bool(state.get('restart_reconciled')),
            publication_restart_outcome=pub.get('restart_outcome'),
            gap_count=frozen.get('gap_count'), spot_reconnect_status=(frozen.get('spot_reconnect') or {}).get('status'),
            spot_reconnect_reason=(frozen.get('spot_reconnect') or {}).get('reason'))
        for stage in STAGES:
            for clock in ('wall_ns', 'monotonic_ns'):
                decision[stage+'_'+clock] = integer(pub.get(stage+'_'+clock), nullable=True)
        require(decision['computation_completed_monotonic_ns'] >= mono, 'Calculation clock regression')
        if attempted:
            require(mono <= decision['computation_completed_monotonic_ns'] <= decision['intent_monotonic_ns'] <=
                    decision['attempt_monotonic_ns'], 'Publication clock regression')
        if pub['status'] in ('acknowledged', 'uncertain'):
            left = min(decision['valid_until_wall_ns']-decision['attempt_wall_ns'],
                       decision['valid_until_wall_ns']-wall-(decision['attempt_monotonic_ns']-mono))
            require(left >= 1000000, 'Actual attempt expired')
        if pub['status'] == 'acknowledged':
            require(decision['ack_monotonic_ns'] >= decision['attempt_monotonic_ns'], 'ACK precedes attempt')
        require([f['horizon_s'] for f in frozen['forecasts']] == list(HORIZONS), 'Frozen horizon domain')
        require(set(state['targets']) == {str(h) for h in HORIZONS}, 'Target horizon domain')
        horizons = []
        for index, forecast in enumerate(frozen['forecasts']):
            h = forecast['horizon_s']; target = state['targets'][str(h)]
            require(type(target['conflicted']) is bool, 'Invalid conflict flag')
            require(target['target_source_timestamp_ms'] == forecast['target_source_timestamp_ms'], 'Target stamp mismatch')
            require(target['status'] in ('matched','missing','not_forecast','restart_unmatched'), 'Nonterminal target')
            counts = forecast['counts']
            require(set(counts) == set(CATEGORIES) and all(type(n) is int and n >= 0 for n in counts.values()) and
                    sum(counts.values()) == 60, 'Invalid slot counts')
            if h in eligible:
                require(wire['forecasts'][index] == forecast and forecast['price'] is not None, 'Attempted forecast differs')
            first = event_record(target.get('first_event'), 'twap')
            late = event_record(target.get('first_late_event'), 'twap')
            conflict = event_record(target.get('first_conflicting_event'), 'twap')
            for event in (first, late, conflict):
                require(event is None or event['source_timestamp_ms'] == target['target_source_timestamp_ms'], 'Event target mismatch')
            entry = dict(horizon_s=h, target_source_timestamp_ms=forecast['target_source_timestamp_ms'],
                forecast_price=money(forecast['price'], positive=True, nullable=True), quality=forecast['quality'],
                counts=counts, max_interior_carry_ms=forecast['max_interior_carry_ms'], reasons=forecast['reasons'],
                attempted_eligible=h in eligible, exclusion_reasons=excluded.get(str(h), []),
                estimated_arrival_wall_ns=integer(forecast['estimated_arrival_wall_ns'], nullable=True),
                estimated_remaining_ns=integer(forecast['estimated_remaining_ns'], nullable=True),
                target_status=target['status'], first_event=first, first_late_event=late, first_conflicting_event=conflict,
                conflicted=target['conflicted'], clock_anomaly=bool(target.get('clock_anomaly')),
                late_missing=bool(target.get('late_missing')), error=money(target.get('error'), nullable=True),
                persistence_error=money(target.get('persistence_error'), nullable=True),
                eta_error_ns=integer(target.get('eta_error_ns'), nullable=True),
                confirmed_redis_lead_ns=integer(target.get('confirmed_redis_lead_ns'), nullable=True))
            if first is not None:
                require(entry['target_status'] == 'matched' and entry['forecast_price'] is not None, 'Invalid matched target')
                require(mono <= first['received_monotonic_ns'] < mono+120*NS and first['received_wall_ns'] >= wall,
                        'Target outside matching window')
                require(entry['clock_anomaly'] == (first['source_timestamp_ms']*1000000 > first['received_wall_ns']), 'Clock anomaly flag')
                actual = Decimal(first['value'])
                require(Decimal(entry['error']) == Decimal(entry['forecast_price'])-actual and
                        Decimal(entry['persistence_error']) == Decimal(decision['current_twap']['value'])-actual, 'Error mismatch')
                if h in eligible:
                    require(first['received_monotonic_ns'] >= decision['attempt_monotonic_ns'], 'Target preceded eligibility')
            else:
                require(entry['target_status'] != 'matched' and entry['error'] is None and entry['persistence_error'] is None,
                        'Error without target')
            if conflict is not None:
                require(first is not None and entry['conflicted'] is True and Decimal(conflict['value']) != Decimal(first['value']),
                        'Unflagged conflict')
            ack_valid = (pub['status'] == 'acknowledged' and h in eligible and first is not None and
                         not (entry['conflicted'] or entry['clock_anomaly'] or decision['causality_invalid']))
            lead = first['received_monotonic_ns']-decision['ack_monotonic_ns'] if ack_valid and decision['ack_monotonic_ns'] < first['received_monotonic_ns'] else None
            require(entry['confirmed_redis_lead_ns'] == lead, 'Confirmed lead mismatch')
            horizons.append(entry)
        result = dict(schema_version=1, decision=decision, horizons=horizons)
        result['compact_sha256'] = sha256(canonical_bytes(result)).hexdigest()
        return result


def verify_compact(record):
    require(set(record) == {'schema_version','decision','horizons','compact_sha256'} and record['schema_version'] == 1,
            'Compact record contract')
    body = {key:value for key,value in record.items() if key != 'compact_sha256'}
    require(sha256(canonical_bytes(body)).hexdigest() == record['compact_sha256'], 'Compact hash mismatch')
    require([h['horizon_s'] for h in record['horizons']] == list(HORIZONS), 'Compact horizon domain')
    return record


def iter_verified_export(source, *, expected_sha256, expected_rows, run_id,
                         finalized_as_of_wall_ns, expected_selected_rows=7082):
    """Verify entire input before yielding from a temporary compact spool."""
    total, selected, size, digest, seen = 0, 0, 0, sha256(), set()
    with tempfile.TemporaryFile() as compact:
        with Path(source).open('rb') as stream:
            while True:
                raw = stream.readline(MAX_LINE+1)
                if not raw:
                    break
                require(len(raw) <= MAX_LINE and raw.endswith(b'\n'), 'Oversized/truncated export')
                digest.update(raw); size += len(raw); total += 1
                require(total <= expected_rows, 'Unexpected extra row')
                row = decode(raw)
                identity = row['run_id'],row['decision_id']
                require(identity not in seen, 'Duplicate export identity')
                seen.add(identity)
                for field in ('frozen','state'):
                    require(sha256(row[field+'_json'].encode()).hexdigest() == row[field+'_sha256'], 'Export row hash mismatch')
                if row['run_id'] == run_id:
                    record = compact_record(row, finalized_as_of_wall_ns=finalized_as_of_wall_ns)
                    compact.write(canonical_bytes(record)+b'\n'); selected += 1
        require(total == expected_rows and digest.hexdigest() == expected_sha256, 'Whole export verification failed')
        require(selected == expected_selected_rows, 'Selected decision count mismatch')
        compact.seek(0)
        for raw in compact:
            yield verify_compact(decode(raw))


def scoring_pair(record, horizon_s, *, require_early=True):
    """Exact compact-only paired errors; missingness stays outside the pairs."""
    verify_compact(record)
    decision = record['decision']; horizon = next(h for h in record['horizons'] if h['horizon_s'] == horizon_s)
    first = horizon['first_event']
    if (decision['publication_status'] != 'acknowledged' or not horizon['attempted_eligible'] or
        first is None or horizon['target_status'] != 'matched' or horizon['conflicted'] or
        horizon['clock_anomaly'] or decision['causality_invalid']):
        return None
    if require_early and not decision['ack_monotonic_ns'] < first['received_monotonic_ns']:
        return None
    with localcontext() as context:
        context.prec = 80
        actual = Decimal(first['value'])
        return (Decimal(horizon['forecast_price'])-actual,
                Decimal(decision['current_twap']['value'])-actual, actual, first['source_timestamp_ms'])


def write_export(source, output, **kwargs):
    output = Path(output); manifest_path = output.with_name(output.name+'.manifest.json')
    require(not output.exists() and not manifest_path.exists(), 'Refusing overwrite')
    code_hash = sha256(Path(__file__).read_bytes()).hexdigest()
    # next() performs full source verification before creating output.
    records = iter_verified_export(source, **kwargs)
    first = next(records)
    output.parent.mkdir(parents=True, exist_ok=True)
    digest, total, size, minimum, maximum = sha256(), 0, 0, None, 0
    statuses = Counter()
    with output.open('xb') as handle:
        def emit(record):
            nonlocal total, size, minimum, maximum
            raw = canonical_bytes(record)+b'\n'
            handle.write(raw); digest.update(raw); total += 1; size += len(raw)
            minimum = len(raw) if minimum is None else min(minimum,len(raw)); maximum = max(maximum,len(raw))
            statuses[record['decision']['publication_status']] += 1
        emit(first)
        for record in records:
            emit(record)
    require(code_hash == sha256(Path(__file__).read_bytes()).hexdigest(), 'Encoder changed during run')
    manifest = dict(schema_version=1, code_sha256=code_hash, input_path=str(Path(source).resolve()),
        input_sha256=kwargs['expected_sha256'], input_rows=kwargs['expected_rows'], run_id=kwargs['run_id'],
        finalized_as_of_wall_ns=kwargs['finalized_as_of_wall_ns'], decisions=total, horizon_records=total*6,
        compact_jsonl_sha256=digest.hexdigest(), logical_bytes=size, minimum_line_bytes=minimum,
        maximum_line_bytes=maximum, publication_statuses=dict(statuses),
        limitations=['No full slot/payload replay after original bytes expire.',
                    'This encoder does not reconstruct complete feed-gap metrics from selected snapshots.',
                    'Logical JSON bytes are not PostgreSQL allocated bytes.'])
    with manifest_path.open('x', encoding='utf-8') as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True); handle.write('\n')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--expected-sha256',required=True)
    parser.add_argument('--expected-rows',type=int,required=True)
    parser.add_argument('--expected-selected-rows',type=int,default=7082)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--finalized-as-of-wall-ns',type=int,required=True)
    args=vars(parser.parse_args()); source=args.pop('input'); output=args.pop('output')
    print(json.dumps(write_export(source,output,**args),sort_keys=True))


if __name__ == '__main__':
    main()
