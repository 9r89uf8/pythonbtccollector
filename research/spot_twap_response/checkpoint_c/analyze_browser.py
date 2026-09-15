"""Offline, browser-clock pairing of bounded Checkpoint C SSE captures.

No collector/runtime imports and no reconstructed forecast arithmetic. Browser
handler times are timing quantities; prices enter only as exact E18 strings.
An observed target is a later delivered exact TWAP anchor, not an inferred
collector receipt or Redis acknowledgement.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, localcontext
import gzip
import hashlib
import json
from pathlib import Path
import re

VERSION = 'ghost-browser-analysis-v1'
HORIZONS = (1, 2, 3, 5, 10, 30)
MAX_RECORD_BYTES = 262_144
MAX_RECORDS = 100_000
LIMITATIONS = [
    'Lead measures browser handler entry, not rendering or collector/Redis acknowledgement.',
    'Targets are exact anchors delivered over this SSE stream; skipped or absent targets are censored.',
    'Admitted/excluded denominators cover unique delivered producer decisions; null envelopes do not reveal omitted forecasts.',
    'Server remaining lifetime is not a new TTL at browser receipt; this analysis does not certify browser display freshness.',
    'Positive lead among matched pairs is guaranteed by excluding targets already observed at forecast receipt.',
    'GET snapshots are timing diagnostics only and never substitute for SSE target anchors.',
    'The post_warmup panel begins at warmup_ms after browser capture start; it does not certify that producer history has warmed up.',
    'Accuracy is conditional on observed, nonconflicting, later targets; it is not a trading or settlement result.',
]


class InvalidCapture(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidCapture(message)


def integer(value, name, minimum=0, maximum=9_223_372_036_854_775_807):
    require(type(value) is int and minimum <= value <= maximum, 'invalid integer: ' + name)
    return value


def timing(value, name):
    require(type(value) in (int, Decimal), 'invalid timing: ' + name)
    result = Decimal(value)
    require(result.is_finite() and 0 <= result <= Decimal('1e15'), 'invalid timing: ' + name)
    return result


def ns(value, name, *, positive=False):
    require(isinstance(value, str) and re.fullmatch(r'(?:0|[1-9][0-9]{0,18})', value) is not None,
            'invalid ns string: ' + name)
    return integer(int(value), name, 1 if positive else 0)


def text(value, name, maximum=256):
    require(isinstance(value, str) and 0 < len(value) <= maximum and
            all(ord(c) >= 32 and ord(c) != 127 for c in value), 'invalid text: ' + name)
    return value


def price(value):
    require(isinstance(value, str) and re.fullmatch(r'(?:0|[1-9][0-9]{0,19})\.[0-9]{18}', value) is not None,
            'price must be an E18 string')
    result = Decimal(value)
    require(result > 0, 'price must be positive')
    return result


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def _int(value):
    require(len(value) <= 20, 'oversized JSON integer')
    return int(value)


def _constant(value):
    raise InvalidCapture('nonfinite JSON value')


def decode(value):
    try:
        return json.loads(value, parse_float=Decimal, parse_int=_int,
                          parse_constant=_constant, object_pairs_hook=_pairs)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise InvalidCapture('malformed JSON: ' + str(exc)) from exc


def no_wire_floats(value):
    if isinstance(value, dict):
        for item in value.values():
            no_wire_floats(item)
    elif isinstance(value, list):
        for item in value:
            no_wire_floats(item)
    else:
        require(not isinstance(value, Decimal), 'producer JSON contains a floating number')


def reasons(value):
    require(isinstance(value, list) and len(value) <= 32, 'invalid forecast reasons')
    result = tuple(text(item, 'reason') for item in value)
    require(len(set(result)) == len(result), 'duplicate reason')
    return result


def wire(data):
    """Validate the identity/membership fields required for independent pairing."""
    require(isinstance(data, str) and len(data.encode('utf-8')) <= 131_072, 'invalid SSE data size')
    envelope = decode(data)
    require(isinstance(envelope, dict) and set(envelope) == {'api', 'ghost'}, 'invalid SSE envelope')
    api = envelope['api']
    require(isinstance(api, dict), 'invalid API envelope')
    require(integer(api['version'], 'API version', 1) == 1, 'unsupported API version')
    text(api['instance_id'], 'API instance', 128)
    integer(api['generation'], 'generation')
    integer(api['sequence'], 'sequence')
    integer(api['skipped_updates'], 'client skips')
    require(type(api['resync']) is bool, 'invalid resync flag')
    require(api['state'] in ('snapshot', 'unavailable'), 'invalid API state')
    text(api['reason'], 'API reason')
    remaining = ns(api['remaining_ns'], 'API remaining')
    body = envelope['ghost']
    if body is None:
        require(api['state'] == 'unavailable' and remaining == 0, 'null ghost has available metadata')
        return api, None, None, ()
    require(api['state'] == 'snapshot' and remaining > 0, 'non-null ghost has unavailable metadata')
    require(isinstance(body, dict), 'invalid producer body')
    no_wire_floats(body)
    require(body['runtime_version'] == 'ghost-canary-v6' and type(body['contract_version']) is int
            and body['contract_version'] == 4, 'unsupported producer contract')
    require(body['model_version'] == 'chainlink-60s-offset3-v1' and body['publication_state'] == 'attempted',
            'unsupported model/publication state')
    run = text(body['run_id'], 'run ID', 128)
    require(isinstance(body['decision_id'], str) and re.fullmatch(r'[1-9][0-9]{0,18}', body['decision_id']) is not None,
            'invalid decision ID')
    decision = integer(int(body['decision_id']), 'decision ID', 1)
    require(integer(body['publication_sequence'], 'publication sequence', 1) == decision, 'decision sequence mismatch')
    global_reasons = reasons(body['reasons'])
    selection = body['publication_eligibility']
    require(isinstance(selection, dict) and type(selection['version']) is int and selection['version'] == 1,
            'unsupported selection version')
    eligible = selection['eligible_horizons']
    require(isinstance(eligible, list) and all(type(h) is int for h in eligible)
            and eligible == [h for h in HORIZONS if h in eligible], 'invalid eligible membership')
    excluded = selection['excluded_horizons']
    require(isinstance(excluded, dict) and set(excluded) == {str(h) for h in HORIZONS if h not in eligible},
            'membership does not partition horizons')
    current = body['current_twap']
    anchor = None
    if current is not None:
        require(isinstance(current, dict) and current['feed'] == 'twap' and type(current['window_s']) is int
                and current['window_s'] == 60, 'wrong official anchor identity')
        source = integer(current['source_timestamp_ms'], 'anchor source')
        require(source % 1000 == 0, 'anchor source is not second-aligned')
        received = ns(current['received_wall_ns'], 'anchor receipt')
        require(source*1_000_000 <= received, 'anchor source is future at collector receipt')
        text(current['event_id'], 'anchor event ID')
        anchor = (source, price(current['value']))
    forecasts = body['forecasts']
    require(isinstance(forecasts, list) and len(forecasts) == 6, 'six forecasts required')
    parsed = []
    for h, forecast in zip(HORIZONS, forecasts):
        require(isinstance(forecast, dict) and type(forecast['horizon_s']) is int and forecast['horizon_s'] == h,
                'forecast horizon mismatch')
        target = None if anchor is None else anchor[0]+h*1000
        require(forecast['target_source_timestamp_ms'] == target and
                (target is None or type(forecast['target_source_timestamp_ms']) is int), 'target does not match anchor')
        counts = forecast['counts']
        require(isinstance(counts, dict) and set(counts) == {'observed', 'carried', 'pending', 'future', 'missing'},
                'invalid slot count fields')
        require(sum(integer(c, 'slot count', 0, 60) for c in counts.values()) == 60, 'slot counts do not sum to 60')
        why = reasons(forecast['reasons'])
        require(set(global_reasons) <= set(why), 'forecast omits global reasons')
        if h in eligible:
            require(anchor is not None and not why and not global_reasons and counts['missing'] == 0 and
                    forecast['quality'] == ('degraded' if counts['carried'] else 'healthy'), 'invalid eligible forecast')
            amount = price(forecast['price'])
        else:
            require(forecast['price'] is None and forecast['quality'] == 'unavailable' and why
                    and reasons(excluded[str(h)]) == why, 'excluded forecast exposes a price or mismatched reasons')
            amount = None
        parsed.append(dict(horizon_s=h, target=target, price=amount, excluded_reasons=why,
                           quality=forecast['quality']))
    # Canonical body hashing is only a same-identity consistency check. It is
    # deliberately not called a hash of the original producer byte sequence.
    signature = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    return api, (run, decision, signature), anchor, tuple(parsed)


def quantile(values, probability):
    if not values:
        return None
    ordered = sorted(values)
    position = Decimal(len(ordered)-1)*probability
    lower = int(position)
    return ordered[lower] + (ordered[min(lower+1, len(ordered)-1)]-ordered[lower])*(position-lower)


def distribution(values):
    return dict(n=len(values), median=quantile(values, Decimal('.5')),
                p90=quantile(values, Decimal('.9')), p99=quantile(values, Decimal('.99')),
                maximum=max(values) if values else None)


def _jsonable(value):
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _panel(rows, anchors, lower, upper):
    result = []
    for h in HORIZONS:
        selected = [r for r in rows if r['horizon_s'] == h and lower <= r['elapsed_ms'] < upper]
        exclusions, censored = Counter(), Counter()
        leads, dollars, bps = [], [], []
        admitted = 0
        resync_admitted = 0
        resync_matched = 0
        qualities = Counter()
        for row in selected:
            if row['price'] is None:
                exclusions.update(row['excluded_reasons'])
                continue
            admitted += 1
            resync_admitted += int(row['resync'])
            qualities[row['quality']] += 1
            actual = anchors.get(row['target'])
            if actual is None:
                censored['missing_exact_target'] += 1
            elif len(actual['values']) != 1:
                censored['conflicting_target_values'] += 1
            elif actual['first_ms'] <= row['elapsed_ms']:
                censored['target_already_observed'] += 1
            else:
                target_price = next(iter(actual['values']))
                error = abs(row['price']-target_price)
                leads.append(actual['first_ms']-row['elapsed_ms'])
                dollars.append(error)
                bps.append(error/target_price*Decimal(10000))
                resync_matched += int(row['resync'])
        matched = len(leads)
        observed = matched+censored['target_already_observed']
        result.append(dict(horizon_s=h, candidate_N=len(selected), admitted=admitted,
            excluded=len(selected)-admitted, matched=matched, censored=admitted-matched,
            censor_reasons={key: censored[key] for key in ('missing_exact_target', 'conflicting_target_values', 'target_already_observed')},
            overlapping_exclusion_reasons=dict(sorted(exclusions.items())), admitted_quality=dict(sorted(qualities.items())),
            resync_admitted=resync_admitted, resync_matched=resync_matched,
            browser_lead_ms=distribution(leads), absolute_error_usd=distribution(dollars), absolute_error_bps=distribution(bps),
            positive_fraction_of_matched=Decimal(sum(value > 0 for value in leads))/matched if matched else None,
            nonconflicting_observed_targets=observed,
            positive_fraction_of_nonconflicting_observed_targets=Decimal(matched)/observed if observed else None))
    return result


def _probe_panel(probes, lower, upper):
    """Bucket actual observations; never backfill a delayed timer callback."""
    first_slot = (lower+99)//100
    final_slot = (upper+99)//100
    planned = final_slot-first_slot
    buckets = {}
    included = []
    for probe in probes:
        slot = int(probe['elapsed_ms']//100)
        if first_slot <= slot < final_slot and probe['elapsed_ms'] < upper:
            buckets.setdefault(slot, []).append(probe)
            included.append(probe)
    observed = len(buckets)
    calibrated = sum(all(item['calibrated'] for item in group) for group in buckets.values())
    missing = planned-observed
    uncalibrated = observed-calibrated
    by_horizon = []
    for h in HORIZONS:
        usable = sum(all(item['calibrated'] and h in item['usable_horizons'] for item in group)
                     for group in buckets.values())
        by_horizon.append(dict(horizon_s=h, planned_slots=planned, usable_slots=usable,
            not_usable_slots=calibrated-usable, unknown_slots=missing+uncalibrated,
            usable_fraction_of_planned=Decimal(usable)/planned if planned else None))
    return dict(planned_slots=planned, observed_slots=observed, missing_slots=missing,
        calibrated_observed_slots=calibrated, uncalibrated_observed_slots=uncalibrated,
        multiple_probe_slots=sum(len(group) > 1 for group in buckets.values()),
        actual_probe_records=len(included),
        observed_fraction_of_planned=Decimal(observed)/planned if planned else None,
        interprobe_ms=distribution([after['elapsed_ms']-before['elapsed_ms']
                                   for before, after in zip(included, included[1:])]),
        by_horizon=by_horizon)


def _delivery_metrics(envelopes, duplicate_count):
    invalid = Counter()
    read_to_fanout, fanout_to_send = [], []
    for api in envelopes.values():
        fields = ('read_monotonic_ns', 'fanout_monotonic_ns', 'send_monotonic_ns')
        if any(api.get(field) is None for field in fields):
            invalid['missing_clock'] += 1
            continue
        try:
            read, fanout, send = (ns(api[field], field) for field in fields)
        except InvalidCapture:
            invalid['invalid_clock'] += 1
            continue
        if not read <= fanout <= send:
            invalid['negative_interval'] += 1
            continue
        read_to_fanout.append(Decimal(fanout-read)/1_000_000)
        fanout_to_send.append(Decimal(send-fanout)/1_000_000)
    return dict(unique_fresh_envelopes=len(envelopes), duplicate_fresh_envelopes=duplicate_count,
        valid_metadata_envelopes=len(read_to_fanout), invalid_metadata_envelopes=sum(invalid.values()),
        invalid_metadata_reasons=dict(sorted(invalid.items())),
        read_to_fanout_ms=distribution(read_to_fanout), fanout_to_send_ms=distribution(fanout_to_send),
        sample_unit='First browser receipt of each non-null envelope per (API instance_id, sequence), across the complete capture.',
        clock_domain='Read, fanout, and send monotonic timestamps from the same API instance only; no producer or browser clock subtraction.')


def analyze_capture(path, *, observation_ms=None, drain_ms=None, warmup_ms=65_000):
    """Read a complete immutable capture and return a JSON-safe summary."""
    with localcontext() as context:
        context.prec = 80
        try:
            return _jsonable(_analyze_capture(Path(path), observation_ms=observation_ms,
                                             drain_ms=drain_ms, warmup_ms=warmup_ms))
        except (OSError, EOFError, UnicodeError) as exc:
            raise InvalidCapture('capture input could not be read completely: '+str(exc)) from exc


def _analyze_capture(path, *, observation_ms, drain_ms, warmup_ms):
    stat_before = path.stat()
    digest = hashlib.sha256()
    # Provenance always hashes the exact supplied file, including gzip headers
    # and footer when compressed. Decompressed record bytes are not substituted.
    with path.open('rb') as supplied:
        for chunk in iter(lambda: supplied.read(1_048_576), b''):
            digest.update(chunk)
    kinds, states, api_reasons = Counter(), Counter(), Counter()
    rows, anchors, decisions, runs, instances = [], {}, {}, set(), set()
    probes, load_events = [], []
    delivery_envelopes, duplicate_delivery = {}, 0
    start = end = previous = None
    connection = 0
    skips = {}
    skipped_total = resyncs = duplicates = outside_anchors = 0
    snapshots, durations = Counter(), []
    error_reasons = Counter()
    snapshot_aborts = snapshot_timeouts = 0
    first_snapshot = last_snapshot = None
    record_count = 0
    opener = gzip.open if path.suffix.lower() == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', newline='') as stream:
        while True:
            raw = stream.readline(MAX_RECORD_BYTES+1)
            if not raw:
                break
            record_count += 1
            require(record_count <= MAX_RECORDS and len(raw.encode('utf-8')) <= MAX_RECORD_BYTES and raw.endswith('\n'),
                    'capture record exceeds bound or has truncated final line')
            record = decode(raw)
            require(isinstance(record, dict), 'capture record is not an object')
            kind = record.get('kind')
            require(kind in ('start', 'ghost', 'open', 'error', 'end', 'snapshot', 'probe', 'load_start', 'load_end'),
                    'unknown record kind')
            require(end is None, 'records follow end marker')
            browser = timing(record['end_ms'] if kind == 'snapshot' else record['browser_ms'], 'browser clock')
            require(previous is None or browser >= previous, 'browser clock regressed')
            previous = browser
            kinds[kind] += 1
            if kind == 'start':
                require(start is None and record_count == 1, 'start must be the first and only start marker')
                start = browser
                declared_observation = integer(record['observation_ms'], 'observation duration', 1, 3_600_000)
                declared_drain = integer(record['drain_ms'], 'drain duration', 120_000, 3_600_000)
                require(observation_ms is None or observation_ms == declared_observation, 'observation duration mismatch')
                require(drain_ms is None or drain_ms == declared_drain, 'drain duration mismatch')
                observation_ms, drain_ms = declared_observation, declared_drain
                integer(warmup_ms, 'warmup', 0, observation_ms-1)
                continue
            require(start is not None, 'missing initial start marker')
            elapsed = browser-start
            if kind == 'end':
                require(elapsed >= observation_ms+drain_ms, 'capture ended before observation and drain completed')
                end = browser
            elif kind == 'open':
                connection += 1
            elif kind == 'error':
                reason = text(record.get('reason', 'eventsource_error'), 'capture error reason')
                error_reasons[reason] += 1
                if reason == 'snapshot_error':
                    message = record.get('message', '')
                    require(isinstance(message, str), 'invalid snapshot error message')
                    snapshot_aborts += int(message.startswith('AbortError:'))
                    snapshot_timeouts += int(message.startswith('TimeoutError:'))
            elif kind == 'probe':
                calibrated = record['calibrated']
                require(type(calibrated) is bool, 'invalid probe calibration flag')
                usable = record['usable_horizons']
                require(isinstance(usable, list) and all(type(h) is int for h in usable)
                        and usable == [h for h in HORIZONS if h in usable], 'invalid probe horizon list')
                require(calibrated or not usable, 'uncalibrated probe claims usable horizons')
                state = text(record['state'], 'probe state')
                probes.append(dict(elapsed_ms=elapsed, calibrated=calibrated,
                                   usable_horizons=tuple(usable), state=state))
            elif kind in ('load_start', 'load_end'):
                load_events.append(dict(kind=kind, elapsed_ms=elapsed))
            elif kind == 'snapshot':
                request_start = timing(record['start_ms'], 'snapshot start')
                require(start <= request_start <= browser, 'snapshot clocks invalid')
                status = integer(record['status'], 'snapshot HTTP status', 0, 599)
                snapshots[str(status)] += 1
                durations.append(browser-request_start)
                if record['server_time_ns'] is not None:
                    ns(record['server_time_ns'], 'snapshot server clock')
                require(record['data'] is None or isinstance(record['data'], str), 'invalid snapshot body')
                if first_snapshot is None:
                    first_snapshot = elapsed
                last_snapshot = elapsed
            elif kind == 'ghost':
                try:
                    api, identity, anchor, forecasts = wire(record['data'])
                except (KeyError, TypeError, ValueError, RecursionError, ArithmeticError) as exc:
                    raise InvalidCapture('invalid ghost record ' + str(record_count) + ': ' + str(exc)) from exc
                states[api['state']] += 1
                api_reasons[api['reason']] += 1
                instances.add(api['instance_id'])
                resyncs += int(api['resync'])
                skip_key = (connection, api['instance_id'])
                old_skip = skips.get(skip_key, 0)
                require(api['skipped_updates'] >= old_skip, 'client cumulative skip counter regressed')
                skipped_total += api['skipped_updates']-old_skip
                skips[skip_key] = api['skipped_updates']
                if identity is None:
                    continue
                delivery_key = (api['instance_id'], api['sequence'])
                if delivery_key in delivery_envelopes:
                    duplicate_delivery += 1
                else:
                    delivery_envelopes[delivery_key] = api
                run, decision, signature = identity
                runs.add(run)
                if anchor is not None:
                    if elapsed <= observation_ms+drain_ms:
                        seen = anchors.setdefault(anchor[0], dict(first_ms=elapsed, values=set()))
                        seen['values'].add(anchor[1])
                    else:
                        outside_anchors += 1
                key = (run, decision)
                if key in decisions:
                    require(decisions[key] == signature, 'conflicting payloads for one producer identity')
                    duplicates += 1
                    continue
                decisions[key] = signature
                if elapsed < observation_ms:
                    for forecast in forecasts:
                        rows.append(dict(forecast, run_id=run, decision_id=decision,
                                         elapsed_ms=elapsed, resync=api['resync']))
    stat_after = path.stat()
    require((stat_before.st_size, stat_before.st_mtime_ns) == (stat_after.st_size, stat_after.st_mtime_ns),
            'capture changed during analysis')
    require(start is not None and end is not None and kinds['end'] == 1, 'capture is incomplete: complete end marker required')
    per_run = {run: dict(unique_delivered_decisions=sum(key[0] == run for key in decisions),
                        admission_decisions=sum(r['run_id'] == run for r in rows)//6) for run in sorted(runs)}
    result = dict(version=VERSION, status='accepted_complete_capture', source_path=str(path.resolve()),
        source_sha256=digest.hexdigest(), source_bytes=stat_after.st_size, record_count=record_count,
        capture_start_browser_ms=start, capture_end_browser_ms=end, capture_elapsed_ms=end-start,
        observation_ms=observation_ms, drain_ms=drain_ms, warmup_ms=warmup_ms,
        admission_interval='[start, start + observation_ms)', anchor_interval='[start, start + observation_ms + drain_ms]',
        record_counts=dict(sorted(kinds.items())), connects=kinds['open'], connection_errors=error_reasons['eventsource_error'],
        error_reasons=dict(sorted(error_reasons.items())),
        other_errors=sum(value for key, value in error_reasons.items() if key not in ('eventsource_error', 'snapshot_error')),
        api_states=dict(sorted(states.items())), api_reasons=dict(sorted(api_reasons.items())),
        api_instances=sorted(instances), resync_envelopes=resyncs, skipped_updates=skipped_total,
        duplicate_producer_envelopes=duplicates, producer_run_count=len(runs), runs=per_run,
        unique_exact_anchor_stamps=len(anchors), conflicting_anchor_stamps=sum(len(v['values']) > 1 for v in anchors.values()),
        anchor_observations_after_drain_ignored=outside_anchors,
        snapshots=dict(status_counts=dict(sorted(snapshots.items())), full_response_ms=distribution(durations),
                       first_elapsed_ms=first_snapshot, last_elapsed_ms=last_snapshot,
                       completed_http_responses=sum(snapshots.values()), error_count=error_reasons['snapshot_error'],
                       aborted_request_count=snapshot_aborts, explicit_timeout_error_count=snapshot_timeouts,
                       error_timing='Failed GET records have no request-start clock; no duration is reconstructed. '
                                    'AbortError identifies abortion; a timeout cause requires the capture instrument evidence.'),
        api_delivery=_delivery_metrics(delivery_envelopes, duplicate_delivery),
        all_admissions=_panel(rows, anchors, Decimal(0), Decimal(observation_ms)),
        post_warmup=_panel(rows, anchors, Decimal(warmup_ms), Decimal(observation_ms)),
        quantiles='Linear interpolation at (n-1)*p, Decimal precision 80; no financial float conversion.',
        error_bps_denominator='Exact observed target TWAP value', limitations=list(LIMITATIONS))
    if probes:
        result['probes'] = dict(grid_ms=100, actual_probe_records=len(probes),
            records_after_admission_ignored=sum(p['elapsed_ms'] >= observation_ms for p in probes),
            state_counts=dict(sorted(Counter(p['state'] for p in probes).items())),
            all_admissions=_probe_panel(probes, 0, observation_ms),
            post_warmup=_probe_panel(probes, warmup_ms, observation_ms),
            method='Actual times map to floor(elapsed_ms/100). No delayed observation fills earlier slots. '
                   'A slot is usable only when every observation in it is calibrated and lists that horizon. '
                   'Absent or uncalibrated slots are unknown; all planned slots remain in the denominator.')
        result['limitations'].append('Probe usability is the browser classifier recorded at actual handler times; '
            'this analyzer does not independently reconstruct clock-bracket or selected-payload eligibility. '
            'Multiple probes in one slot are combined conservatively, and point samples do not prove continuous availability.')
    if load_events:
        result['load_diagnostics'] = load_events
    return result


def _report(summary):
    lines = ['# Browser delivery canary', '',
        'Complete capture: ' + str(summary['record_count']) + ' records, ' + summary['capture_elapsed_ms'] + ' ms.', '',
        'Browser lead is handler receipt of the exact target minus handler receipt of its forecast. '
        'It is separate from collector receipt and Redis acknowledgement.', '',
        'SSE connection errors: ' + str(summary['connection_errors']) + '. Failed GETs: '
        + str(summary['snapshots']['error_count']) + ' (' + str(summary['snapshots']['aborted_request_count']) + ' AbortError records).', '',
        'API timing uses ' + str(summary['api_delivery']['valid_metadata_envelopes']) + ' unique envelopes with valid clocks; '
        + str(summary['api_delivery']['invalid_metadata_envelopes']) + ' have missing or invalid timing metadata. '
        'Median read→fanout: ' + str(summary['api_delivery']['read_to_fanout_ms']['median']) + ' ms; '
        'median fanout→send: ' + str(summary['api_delivery']['fanout_to_send_ms']['median']) + ' ms.', '']
    cutoff_label = 'After the first ' + str(summary['warmup_ms']) + ' ms of browser capture'
    for key, label in (('all_admissions', 'All admissions'), ('post_warmup', cutoff_label)):
        lines += ['## ' + label, '', '| Horizon | Admitted | Matched | Censored | Median lead ms | Median absolute error USD |',
                  '|---:|---:|---:|---:|---:|---:|']
        for row in summary[key]:
            lines.append('| ' + ' | '.join(str(value) if value is not None else '—' for value in (
                row['horizon_s'], row['admitted'], row['matched'], row['censored'],
                row['browser_lead_ms']['median'], row['absolute_error_usd']['median'])) + ' |')
        lines.append('')
    if 'probes' in summary:
        lines += ['## Browser probe coverage', '',
                  'Usability includes unknown and missing planned slots in its denominator. '
                  'These are recorded point observations, not continuous-availability measurements.', '',
                  '| Horizon | Usable after the initial browser cutoff | Unknown | Planned | Usable fraction |',
                  '|---:|---:|---:|---:|---:|']
        for row in summary['probes']['post_warmup']['by_horizon']:
            lines.append('| ' + ' | '.join(str(row[key]) for key in (
                'horizon_s', 'usable_slots', 'unknown_slots', 'planned_slots', 'usable_fraction_of_planned')) + ' |')
        lines.append('')
    lines += ['## Limits', ''] + ['- ' + item for item in summary['limitations']]
    return '\n'.join(lines)+'\n'


def write_results(input_path, output_path, **kwargs):
    output = Path(output_path)
    require(not output.exists(), 'output already exists; refusing overwrite')
    summary = analyze_capture(input_path, **kwargs)
    output.mkdir(parents=True, exist_ok=False)
    summary_path, report_path = output/'summary.json', output/'report.md'
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False)+'\n', encoding='utf-8')
    report_path.write_text(_report(summary), encoding='utf-8')
    manifest = dict(version=VERSION, status='accepted', source_path=summary['source_path'],
        source_sha256=summary['source_sha256'], source_bytes=summary['source_bytes'],
        code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        artifacts_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (summary_path, report_path)})
    (output/'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path, help='Complete UTF-8 JSONL or .jsonl.gz; hashes supplied file bytes')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--observation-ms', type=int)
    parser.add_argument('--drain-ms', type=int)
    parser.add_argument('--warmup-ms', type=int, default=65_000)
    args = parser.parse_args(argv)
    try:
        result = write_results(args.input, args.output, observation_ms=args.observation_ms,
                               drain_ms=args.drain_ms, warmup_ms=args.warmup_ms)
    except (InvalidCapture, OSError, KeyError, TypeError) as exc:
        parser.exit(2, 'Browser capture rejected: '+str(exc)+'\n')
    print(json.dumps(dict(status=result['status'], records=result['record_count'], source_sha256=result['source_sha256'])))


if __name__ == '__main__':
    main()
