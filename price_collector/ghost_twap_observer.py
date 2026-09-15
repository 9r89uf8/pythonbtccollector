"""Bounded local Redis sampling, independent of collectors and PostgreSQL.

The grid counts observations, not continuous uptime or target arrival. Exact raw
payloads are deduplicated; sample buffers are fsynced periodically, not per read.
Redis GET/PTTL execute together in MULTI/EXEC (no writes or scripts).
"""
from __future__ import annotations

import argparse
import asyncio
import base64
from collections import Counter
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import signal
import time

VERSION = 'ghost-observer-v1'
# The observation-file schema is unchanged. Keep historical v1 reanalysis
# stable while accepting only explicitly reviewed payload contracts.
PAYLOAD_CONTRACTS = {('ghost-canary-v5', 3), ('ghost-canary-v6', 4)}
KEY = 'btc:live:ghost_chainlink_twap_60s'
HORIZONS = (1, 2, 3, 5, 10, 30)
CANARY_MS = 3_600_000
INTERVAL_MS = 100
READ_TIMEOUT_SECONDS = 0.1
MAX_PAYLOAD_BYTES = 65_536
MAX_TOTAL_BYTES = 128 * 1024 ** 2
MANIFEST_RESERVE = 32_768
NS_MS = 1_000_000
CLOCK_STEP_NS = 5 * NS_MS


def _json(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'


def _integer(value) -> int:
    if type(value) is int and 0 <= value <= 9_223_372_036_854_775_807:
        return value
    if isinstance(value, str) and re.fullmatch(r'[0-9]{1,19}', value):
        result = int(value)
        if result <= 9_223_372_036_854_775_807:
            return result
    raise ValueError('invalid integer clock')


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError('non-finite JSON number')


def _validate_reconnect(p: dict, eligible: list, decision: int, decision_mono: int) -> None:
    """Check bounded recovery evidence/availability, not retained slot history."""
    policy = p['policy']
    limit = policy.get('spot_reconnect_max_gap_ms')
    carry = policy.get('max_carry_ms')
    if (type(limit) is not int or not 0 <= limit <= 10000
            or type(carry) is not int or carry <= 0 or 'spot_reconnect' not in p):
        raise ValueError('unexpected reconnect policy/snapshot')
    recovery = p['spot_reconnect']
    if recovery is None:
        return
    fields = {'gap_ordinal', 'gap_wall_ns', 'gap_monotonic_ns', 'previous_spot',
              'first_post_gap_spot', 'status', 'reason'}
    if (not isinstance(recovery, dict) or set(recovery) != fields
            or type(recovery['gap_ordinal']) is not int
            or not 0 < recovery['gap_ordinal'] <= _integer(p['gap_count'])
            or recovery['status'] not in ('waiting', 'retained', 'cleared')
            or not isinstance(recovery['reason'], str) or not 0 < len(recovery['reason']) <= 265):
        raise ValueError('invalid reconnect metadata')
    wall, mono = recovery['gap_wall_ns'], recovery['gap_monotonic_ns']
    if wall is not None and _integer(wall) > decision:
        raise ValueError('gap wall clock after decision')
    if mono is not None and _integer(mono) > decision_mono:
        raise ValueError('gap monotonic clock after decision')
    previous, first = recovery['previous_spot'], recovery['first_post_gap_spot']
    for event in (previous, first):
        if event is None:
            continue
        if (not isinstance(event, dict) or event['feed'] != 'spot' or event['window_s'] is not None
                or not isinstance(event['value'], str)
                or re.fullmatch(r'[0-9]{1,20}\.[0-9]{18}', event['value']) is None
                or Decimal(event['value']) <= 0 or type(event['source_timestamp_ms']) is not int
                or _integer(event['source_timestamp_ms']) % 1000
                or type(event['sequence']) is not int or event['sequence'] <= 0
                or not isinstance(event['event_id'], str) or not 0 < len(event['event_id']) <= 256
                or _integer(event['received_wall_ns']) > decision
                or _integer(event['received_monotonic_ns']) > decision_mono
                or event['received_ms'] != _integer(event['received_wall_ns']) // NS_MS):
            raise ValueError('invalid reconnect event')
    if recovery['status'] == 'cleared':
        # A rejected first event may be source-future/stale; it is evidence,
        # never a fallback input. Normal current-input checks still apply.
        return
    if not limit or wall is None or mono is None or previous is None:
        raise ValueError('retention lacks gap clocks/previous event/policy')
    wall, mono = _integer(wall), _integer(mono)
    if not any(g['feed'] == 'spot' and g['reason'] == 'connection_end'
               and g['ordinal'] == recovery['gap_ordinal'] for g in p['last_gaps']):
        raise ValueError('retention lacks transport gap marker')
    source = _integer(previous['source_timestamp_ms']) * NS_MS
    ages = (wall-source, wall-_integer(previous['received_wall_ns']),
            mono-_integer(previous['received_monotonic_ns']))
    if (source > _integer(previous['received_wall_ns']) or min(ages) < 0
            or ages[0] > 5000*NS_MS or max(ages[1:]) > 3000*NS_MS):
        raise ValueError('retention previous event invalid at gap')
    if recovery['status'] == 'waiting':
        if first is not None or p['current_spot'] is not None or eligible:
            raise ValueError('waiting reconnect cannot expose current prices')
        return
    if first is None or p['current_spot'] is None:
        raise ValueError('retained recovery lacks resumed input')
    first_wall, first_mono = _integer(first['received_wall_ns']), _integer(first['received_monotonic_ns'])
    source_delta = (_integer(first['source_timestamp_ms']) - _integer(previous['source_timestamp_ms'])) * NS_MS
    deltas = (source_delta, first_wall-_integer(previous['received_wall_ns']),
              first_mono-_integer(previous['received_monotonic_ns']))
    first_source_age = first_wall - _integer(first['source_timestamp_ms'])*NS_MS
    if (first_wall < wall or first_mono < mono or source_delta <= 0
            or first['sequence'] <= previous['sequence'] or not 0 <= first_source_age <= 5000*NS_MS
            or max(deltas) > min(limit, carry)*NS_MS
            or _integer(p['current_spot']['sequence']) < first['sequence']):
        raise ValueError('retained recovery does not satisfy bounded resume clocks')


def classify_payload(raw: bytes, ttl_ms: int, *, start_ms: int, end_ms: int,
                     read_start_wall_ns: int, read_end_wall_ns: int,
                     read_start_monotonic_ns: int, read_end_monotonic_ns: int) -> dict:
    """Conservative freshness at reply end, not still-unreceived-target evidence."""
    result = dict(payload_valid=False, campaign_qualified=False, eligible_horizons=[],
                  usable_horizons=[], reasons=[], freshness_reasons=[])
    if len(raw) > MAX_PAYLOAD_BYTES:
        result['reasons'] = ['oversized_payload']
        return result
    try:
        p = json.loads(raw, parse_float=Decimal, parse_constant=_reject_constant, object_pairs_hook=_unique_object)
        if not isinstance(p, dict):
            raise ValueError('payload must be object')
        for name in ('run_id', 'decision_id'):
            if not isinstance(p[name], str) or not 0 < len(p[name]) <= 128:
                raise ValueError('invalid identity')
            result[name] = p[name]
        decision = _integer(p['decision_wall_ns'])
        decision_mono = _integer(p['decision_monotonic_ns'])
        deadline = _integer(p['valid_until_wall_ns'])
        result['decision_time_ms'] = decision // NS_MS
        contract = p.get('contract_version')
        runtime_version = p.get('runtime_version')
        if (not isinstance(runtime_version, str) or type(contract) is not int
                or (runtime_version, contract) not in PAYLOAD_CONTRACTS):
            raise ValueError('unexpected runtime/contract')
        if not start_ms * NS_MS <= decision < end_ms * NS_MS:
            raise ValueError('outside campaign decision window')
        if p.get('publication_state') != 'attempted':
            raise ValueError('payload is not attempted')
        selection = p['publication_eligibility']
        if not isinstance(selection, dict) or type(selection.get('version')) is not int or selection.get('version') != 1:
            raise ValueError('invalid selection')
        eligible = selection['eligible_horizons']
        excluded = selection['excluded_horizons']
        if (not isinstance(eligible, list) or any(type(h) is not int or h not in HORIZONS for h in eligible)
                or len(set(eligible)) != len(eligible) or not isinstance(excluded, dict)
                or set(excluded) != {str(h) for h in HORIZONS if h not in eligible}):
            raise ValueError('inconsistent selection partition')
        checked = _integer(selection['checked_wall_ns'])
        checked_mono = _integer(selection['checked_monotonic_ns'])
        attempt = _integer(p['publication_attempt_wall_ns'])
        attempt_mono = _integer(p['publication_attempt_monotonic_ns'])
        if not (decision <= checked == attempt <= read_end_wall_ns
                and decision_mono <= checked_mono == attempt_mono <= read_end_monotonic_ns):
            raise ValueError('invalid publication clock order')
        forecasts = p['forecasts']
        if (not isinstance(forecasts, list) or len(forecasts) != len(HORIZONS)
                or any(type(f['horizon_s']) is not int for f in forecasts)
                or [f['horizon_s'] for f in forecasts] != list(HORIZONS)):
            raise ValueError('invalid horizon layout')
        policy = p['policy']
        if policy['source_max_age_ms'] != 5000 or policy['receipt_max_age_ms'] != 3000:
            raise ValueError('unexpected freshness policy')
        if contract == 4:
            _validate_reconnect(p, eligible, decision, decision_mono)
        fresh = result['freshness_reasons']
        if read_end_wall_ns >= deadline or read_end_monotonic_ns >= decision_mono + deadline - decision:
            fresh.append('payload_expired')
        elapsed = read_end_monotonic_ns - read_start_monotonic_ns
        if (read_end_wall_ns < read_start_wall_ns or elapsed < 0
                or abs((read_end_wall_ns-read_start_wall_ns)-elapsed) > CLOCK_STEP_NS):
            fresh.append('read_clock_anomaly')
        if type(ttl_ms) is not int or ttl_ms < 0:
            fresh.append('no_expiry' if ttl_ms == -1 else 'inconsistent_ttl')
        elif ttl_ms * NS_MS <= elapsed + NS_MS:
            # Server execution lies inside the read interval; this does not
            # assert the key actually expired before the reply was received.
            fresh.append('expiry_boundary_ambiguous')
        for feed in ('spot', 'twap'):
            event = p['current_' + feed]
            if event is None:
                fresh.append(feed + '_missing')
                continue
            if (not isinstance(event['value'], str) or re.fullmatch(r'[0-9]{1,20}\.[0-9]{18}', event['value']) is None
                    or Decimal(event['value']) <= 0):
                raise ValueError('invalid current price')
            if event['feed'] != feed or event.get('window_s') != (60 if feed == 'twap' else None):
                raise ValueError('invalid current feed/window')
            if (_integer(event['received_wall_ns']) > decision
                    or _integer(event['received_monotonic_ns']) > decision_mono):
                raise ValueError('current event received after decision')
            if _integer(event['source_timestamp_ms'])*NS_MS > _integer(event['received_wall_ns']):
                fresh.append(feed + '_source_future_at_receipt')
            ages = (read_end_wall_ns-_integer(event['source_timestamp_ms'])*NS_MS,
                    read_end_wall_ns-_integer(event['received_wall_ns']),
                    read_end_monotonic_ns-_integer(event['received_monotonic_ns']))
            if min(ages) < 0:
                fresh.append(feed + '_future_clock')
            elif ages[0] > 5000*NS_MS or max(ages[1:]) > 3000*NS_MS:
                fresh.append(feed + '_stale')
        for f in forecasts:
            h, price = f['horizon_s'], f['price']
            if h in eligible:
                if (f['quality'] not in ('healthy', 'degraded') or not isinstance(price, str)
                        or re.fullmatch(r'[0-9]{1,20}\.[0-9]{18}', price) is None or Decimal(price) <= 0):
                    raise ValueError('invalid selected forecast')
                anchor = p['current_twap']
                if anchor is None or _integer(f['target_source_timestamp_ms']) != _integer(anchor['source_timestamp_ms']) + h*1000:
                    raise ValueError('target does not match anchor')
            elif price is not None or f['quality'] != 'unavailable':
                raise ValueError('excluded horizon retains price')
        result.update(payload_valid=True, campaign_qualified=True, eligible_horizons=eligible,
                      usable_horizons=eligible if not fresh else [])
    except (ValueError, TypeError, KeyError, ArithmeticError, UnicodeError, RecursionError) as exc:
        result['reasons'] = ['invalid_payload:' + type(exc).__name__]
    return result


async def read_snapshot(client):
    # MULTI/EXEC prevents a publication between GET and PTTL. The client is
    # persistent; execute obtains/releases a pooled connection for each probe.
    async with client.pipeline(transaction=True) as pipe:
        pipe.get(KEY)
        pipe.pttl(KEY)
        raw, ttl = await pipe.execute()
    return raw, ttl


def _hash_file(path: Path) -> dict:
    digest, size = sha256(), 0
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            digest.update(block)
            size += len(block)
    return dict(sha256=digest.hexdigest(), bytes=size)


def _lines(path: Path, maximum: int):
    with path.open('rb') as stream:
        while True:
            line = stream.readline(maximum+1)
            if not line:
                return
            if len(line) > maximum or not line.endswith(b'\n'):
                raise ValueError('truncated or oversized observation record')
            yield json.loads(line)


class ObservationFiles:
    def __init__(self, directory: Path, max_bytes: int = MAX_TOTAL_BYTES):
        if type(max_bytes) is not int or not MANIFEST_RESERVE < max_bytes <= MAX_TOTAL_BYTES:
            raise ValueError('invalid observer byte cap')
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.samples = (self.directory/'samples.jsonl').open('xb')
        self.payloads = (self.directory/'payloads.jsonl').open('xb')
        self.seen = set()
        self.bytes = 0
        self.max_bytes = max_bytes
        self.sample_records = 0

    def append(self, record: dict, raw: bytes | None = None) -> None:
        ledger = b''
        if raw is not None:
            digest = sha256(raw).hexdigest()
            record['payload_sha256'] = digest
            record['payload_bytes'] = len(raw)
            if len(raw) <= MAX_PAYLOAD_BYTES and digest not in self.seen:
                ledger = _json(dict(sha256=digest, bytes=len(raw), raw_base64=base64.b64encode(raw).decode()))
        sample = _json(record)
        if self.bytes + len(ledger) + len(sample) + MANIFEST_RESERVE > self.max_bytes:
            raise ValueError('observer_file_size_cap')
        if ledger:
            self.payloads.write(ledger)
            self.seen.add(record['payload_sha256'])
        self.samples.write(sample)
        self.bytes += len(ledger) + len(sample)
        self.sample_records += 1

    def flush(self) -> None:
        for stream in (self.payloads, self.samples):
            stream.flush()
            os.fsync(stream.fileno())

    def finish(self, manifest: dict) -> dict:
        self.flush()
        self.payloads.close()
        self.samples.close()
        manifest.update(files={name: _hash_file(self.directory/name) for name in ('samples.jsonl', 'payloads.jsonl')},
                        sample_records=self.sample_records, unique_payloads=len(self.seen))
        with (self.directory/'manifest.json').open('xb') as stream:
            stream.write(_json(manifest))
            stream.flush()
            os.fsync(stream.fileno())
        return manifest


async def observe(client, directory: Path, start_ms: int, *, stop=None,
                  wall_ns=time.time_ns, mono_ns=time.monotonic_ns, sleep=asyncio.sleep,
                  duration_ms=CANARY_MS, max_bytes=MAX_TOTAL_BYTES) -> dict:
    """CLI fixes one hour; shorter integer durations are for isolated tests only."""
    if type(start_ms) is not int or start_ms <= 0 or duration_ms % INTERVAL_MS or not 0 < duration_ms <= CANARY_MS:
        raise ValueError('invalid observation interval')
    stop = stop or asyncio.Event()
    files = ObservationFiles(directory, max_bytes)
    end_ms = start_ms + duration_ms
    count = duration_ms // INTERVAL_MS
    manifest = dict(version=VERSION, start_ms=start_ms, end_ms=end_ms, interval_ms=INTERVAL_MS,
                    planned_bins=count, read_timeout_ms=100, max_payload_bytes=MAX_PAYLOAD_BYTES,
                    max_total_bytes=max_bytes, flush_interval_ms=1000, fsync_each_sample=False,
                    campaign_membership='provisional_decision_window_requires_audit_join',
                    target_unreceived_at_read='unobserved', status='incomplete', reason=None,
                    pid=os.getpid(), redis_host='127.0.0.1', redis_key=KEY)
    boot = Path('/proc/sys/kernel/random/boot_id')
    try:
        manifest['boot_id'] = boot.read_text().strip() if boot.exists() else None
    except OSError:
        manifest['boot_id'] = None
    index, last_flush, pending_clock_step = 0, mono_ns(), None
    previous_wall, previous_mono = wall_ns(), mono_ns()
    monotonic_end = previous_mono + max(0, end_ms*NS_MS-previous_wall)
    try:
        if start_ms*NS_MS-wall_ns() > 300_000*NS_MS:
            raise ValueError('readiness_wait_exceeds_five_minutes')
        raw, ttl = await asyncio.wait_for(read_snapshot(client), timeout=1)
        if raw is not None or ttl != -2:
            raise ValueError('ghost_key_must_be_absent_at_readiness')
        if not wall_ns() < end_ms*NS_MS:
            raise ValueError('observation_window_already_ended')
        ready = dict(version=VERSION, start_ms=start_ms, end_ms=end_ms, ready_wall_ns=str(wall_ns()),
                     ready_monotonic_ns=str(mono_ns()), pid=os.getpid(), boot_id=manifest['boot_id'])
        with (Path(directory)/'ready.json').open('xb') as stream:
            stream.write(_json(ready)); stream.flush(); os.fsync(stream.fileno())
        while index < count:
            if stop.is_set():
                manifest['reason'] = 'interrupted'
                break
            now, mono = wall_ns(), mono_ns()
            prior_wall, prior_mono = previous_wall, previous_mono
            clock_step = abs((now-previous_wall)-(mono-previous_mono)) > CLOCK_STEP_NS
            if clock_step:
                pending_clock_step = dict(before_wall_ns=str(prior_wall), before_monotonic_ns=str(prior_mono),
                                          after_wall_ns=str(now), after_monotonic_ns=str(mono))
            if now < previous_wall or mono < previous_mono:
                manifest['reason'] = 'clock_regression'
                break
            previous_wall, previous_mono = now, mono
            if mono >= monotonic_end and now < end_ms*NS_MS:
                manifest['reason'] = 'monotonic_deadline_before_wall_end'
                break
            if now < (start_ms + index*INTERVAL_MS)*NS_MS:
                await sleep(min(0.1, ((start_ms + index*INTERVAL_MS)*NS_MS-now)/1_000_000_000))
                continue
            current = min(count, (now//NS_MS-start_ms)//INTERVAL_MS)
            if current > index:
                files.append(dict(status='missed', index=index, count=current-index,
                                  reason='scheduler_lag', detected_wall_ns=str(now),
                                  detected_monotonic_ns=str(mono), clock_step=pending_clock_step))
                pending_clock_step = None
                index = current
            if index >= count:
                break
            start_wall, start_mono = wall_ns(), mono_ns()
            if start_wall >= (start_ms+(index+1)*INTERVAL_MS)*NS_MS:
                # Scheduling or file work may cross another bin after the first
                # clock read. Reclassify it as missed instead of starting late.
                continue
            record = dict(index=index, count=1, planned_ms=start_ms+index*INTERVAL_MS,
                          read_start_wall_ns=str(start_wall), read_start_monotonic_ns=str(start_mono),
                          scheduling_lag_ns=start_wall-(start_ms+index*INTERVAL_MS)*NS_MS,
                          grid_check_wall_ns=str(now), grid_check_monotonic_ns=str(mono),
                          prior_grid_check_wall_ns=str(prior_wall), prior_grid_check_monotonic_ns=str(prior_mono),
                          clock_step=pending_clock_step)
            raw = None
            try:
                raw, ttl = await asyncio.wait_for(read_snapshot(client), timeout=READ_TIMEOUT_SECONDS)
                end_wall, end_mono = wall_ns(), mono_ns()
                record.update(status='absent' if raw is None and ttl == -2 else 'present', ttl_ms=ttl)
                if raw is None and ttl != -2:
                    record.update(status='error', error='inconsistent_absence_ttl')
                elif raw is not None:
                    if not isinstance(raw, bytes):
                        raise ValueError('Redis response must be bytes')
                    record.update(classify_payload(raw, ttl, start_ms=start_ms, end_ms=end_ms,
                        read_start_wall_ns=start_wall, read_end_wall_ns=end_wall,
                        read_start_monotonic_ns=start_mono, read_end_monotonic_ns=end_mono))
            except Exception as exc:
                end_wall, end_mono = wall_ns(), mono_ns()
                record.update(status='error', error=type(exc).__name__)
            record.update(read_end_wall_ns=str(end_wall), read_end_monotonic_ns=str(end_mono),
                          read_duration_ns=end_mono-start_mono)
            record['read_crosses_bin_end'] = end_wall >= (start_ms+(index+1)*INTERVAL_MS)*NS_MS
            if record['read_crosses_bin_end']:
                record['usable_horizons'] = []
            if (clock_step or pending_clock_step is not None or end_wall < start_wall or end_mono < start_mono
                    or abs((end_wall-start_wall)-(end_mono-start_mono)) > CLOCK_STEP_NS):
                record['clock_anomaly'] = True
                record['usable_horizons'] = []
            if end_wall >= end_ms*NS_MS:
                record['read_crosses_campaign_end'] = True
                record['usable_horizons'] = []
            files.append(record, raw)
            pending_clock_step = None
            index += 1
            if mono_ns()-last_flush >= 1_000_000_000:
                files.flush()
                last_flush = mono_ns()
        if index == count:
            # The final bin begins 100ms before the end. Stay alive through the
            # declared boundary; no additional read outside the half-open hour.
            while wall_ns() < end_ms*NS_MS and mono_ns() < monotonic_end and not stop.is_set():
                await sleep(min(0.1, (end_ms*NS_MS-wall_ns())/1_000_000_000))
            manifest['status'] = 'complete' if not stop.is_set() and wall_ns() >= end_ms*NS_MS else 'incomplete'
            manifest['reason'] = None if manifest['status'] == 'complete' else 'interrupted_or_clock_boundary'
    except asyncio.CancelledError:
        manifest['reason'] = 'cancelled'
    except Exception as exc:
        manifest['reason'] = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
    finally:
        manifest.update(recorded_bins=index, unrecorded_bins=count-index,
                        finished_wall_ns=str(wall_ns()), finished_monotonic_ns=str(mono_ns()))
        files.finish(manifest)
    return manifest


def analyze(directory: Path) -> dict:
    """Verify files and count fixed sampling bins, preserving all unknown bins."""
    directory = Path(directory)
    if (directory/'manifest.json').stat().st_size > MANIFEST_RESERVE:
        raise ValueError('oversized manifest')
    manifest = json.loads((directory/'manifest.json').read_bytes())
    if manifest['version'] != VERSION or manifest['interval_ms'] != INTERVAL_MS:
        raise ValueError('unsupported observer manifest')
    expected_bins = (manifest['end_ms']-manifest['start_ms'])//INTERVAL_MS
    if (not 0 < expected_bins <= CANARY_MS//INTERVAL_MS or manifest['planned_bins'] != expected_bins
            or (manifest['end_ms']-manifest['start_ms']) % INTERVAL_MS
            or type(manifest['recorded_bins']) is not int or not 0 <= manifest['recorded_bins'] <= expected_bins
            or manifest['unrecorded_bins'] != expected_bins-manifest['recorded_bins']
            or manifest['status'] not in ('complete', 'incomplete')
            or (manifest['status'] == 'complete' and manifest['recorded_bins'] != expected_bins)):
        raise ValueError('invalid manifest grid')
    if sum((directory/name).stat().st_size for name in ('samples.jsonl', 'payloads.jsonl')) > MAX_TOTAL_BYTES:
        raise ValueError('oversized observation files')
    for name in ('samples.jsonl', 'payloads.jsonl'):
        if _hash_file(directory/name) != manifest['files'][name]:
            raise ValueError('observer file hash mismatch')
    ledger = {}
    for row in _lines(directory/'payloads.jsonl', 100_000):
        raw = base64.b64decode(row['raw_base64'], validate=True)
        if len(raw) != row['bytes'] or len(raw) > MAX_PAYLOAD_BYTES or sha256(raw).hexdigest() != row['sha256'] or row['sha256'] in ledger:
            raise ValueError('invalid payload ledger')
        ledger[row['sha256']] = raw
    def empty():
        return dict(planned_bins=0, statuses=Counter(), eligible=Counter(), usable=Counter(),
                    invalid_payloads=0, unknown_bins=0, expiry_boundary_ambiguous=0,
                    reasons=Counter(), freshness_reasons=Counter())
    full, warm, minutes = empty(), empty(), [empty() for _ in range(60)]
    cursor, sample_records, durations, runs, qualified_hashes = 0, 0, [], set(), set()
    def add(record, first, n):
        for i in range(first, first+n):
            targets = [full, minutes[(i*INTERVAL_MS)//60_000]]
            if i*INTERVAL_MS >= 65_000:
                targets.append(warm)
            for bucket in targets:
                bucket['planned_bins'] += 1
                bucket['statuses'][record['status']] += 1
                bucket['eligible'].update(map(str, record.get('eligible_horizons', [])))
                bucket['usable'].update(map(str, record.get('usable_horizons', [])))
                bucket['reasons'].update(record.get('reasons', []))
                bucket['freshness_reasons'].update(record.get('freshness_reasons', []))
                bucket['invalid_payloads'] += record['status'] == 'present' and not record.get('payload_valid', False)
                bucket['unknown_bins'] += (record['status'] in ('error', 'missed', 'unrecorded')
                    or bool(record.get('clock_anomaly')) or bool(record.get('read_crosses_campaign_end'))
                    or bool(record.get('read_crosses_bin_end'))
                    or 'expiry_boundary_ambiguous' in record.get('freshness_reasons', []))
                bucket['expiry_boundary_ambiguous'] += 'expiry_boundary_ambiguous' in record.get('freshness_reasons', [])
    for row in _lines(directory/'samples.jsonl', 16_384):
        if row['index'] != cursor or type(row['count']) is not int or row['count'] <= 0 or cursor+row['count'] > manifest['planned_bins']:
            raise ValueError('sample grid overlap/gap')
        if row['status'] not in ('present', 'absent', 'error', 'missed') or (row['status'] != 'missed' and row['count'] != 1):
            raise ValueError('invalid sample status/count')
        if row['status'] != 'present' and any(row.get(key) for key in
                ('payload_valid', 'campaign_qualified', 'eligible_horizons', 'usable_horizons', 'payload_sha256')):
            raise ValueError('non-present sample cannot claim payload eligibility')
        if row['status'] == 'present' and not row.get('payload_sha256'):
            raise ValueError('present sample requires exact payload hash')
        if row.get('payload_sha256') and row['payload_sha256'] not in ledger and row.get('reasons') != ['oversized_payload']:
            raise ValueError('missing payload ledger evidence')
        step = row.get('clock_step')
        if step is not None:
            difference = ((_integer(step['after_wall_ns'])-_integer(step['before_wall_ns']))
                          -(_integer(step['after_monotonic_ns'])-_integer(step['before_monotonic_ns'])))
            if abs(difference) <= CLOCK_STEP_NS:
                raise ValueError('invalid clock step evidence')
        if row['status'] != 'missed':
            ws, we = _integer(row['read_start_wall_ns']), _integer(row['read_end_wall_ns'])
            ms, me = _integer(row['read_start_monotonic_ns']), _integer(row['read_end_monotonic_ns'])
            grid_w, grid_m = _integer(row['grid_check_wall_ns']), _integer(row['grid_check_monotonic_ns'])
            prior_w, prior_m = _integer(row['prior_grid_check_wall_ns']), _integer(row['prior_grid_check_monotonic_ns'])
            clock_anomaly = (step is not None or we < ws or me < ms or abs((we-ws)-(me-ms)) > CLOCK_STEP_NS
                             or abs((grid_w-prior_w)-(grid_m-prior_m)) > CLOCK_STEP_NS)
            crosses_end = we >= manifest['end_ms']*NS_MS
            planned = manifest['start_ms'] + row['index']*INTERVAL_MS
            crosses_bin = we >= (planned+INTERVAL_MS)*NS_MS
            if not planned*NS_MS <= ws < (planned+INTERVAL_MS)*NS_MS:
                raise ValueError('read did not start in its planned bin')
            if (row['planned_ms'] != planned or row['read_duration_ns'] != me-ms
                    or row['scheduling_lag_ns'] != ws-planned*NS_MS
                    or bool(row.get('clock_anomaly')) != clock_anomaly
                    or bool(row.get('read_crosses_bin_end')) != crosses_bin
                    or bool(row.get('read_crosses_campaign_end')) != crosses_end):
                raise ValueError('sample clock classification mismatch')
            if row['status'] == 'present':
                if row['payload_sha256'] in ledger:
                    raw = ledger[row['payload_sha256']]
                    if row['payload_bytes'] != len(raw):
                        raise ValueError('payload size mismatch')
                    expected = classify_payload(raw, row['ttl_ms'], start_ms=manifest['start_ms'],
                        end_ms=manifest['end_ms'], read_start_wall_ns=ws, read_end_wall_ns=we,
                        read_start_monotonic_ns=ms, read_end_monotonic_ns=me)
                    if clock_anomaly or crosses_end or crosses_bin:
                        expected['usable_horizons'] = []
                    if any(row.get(key) != value for key, value in expected.items()):
                        raise ValueError('payload classification mismatch')
                elif (row.get('payload_bytes', 0) <= MAX_PAYLOAD_BYTES or row.get('payload_valid')
                      or row.get('eligible_horizons') or row.get('usable_horizons')):
                    raise ValueError('invalid oversized-payload classification')
            elif row['status'] == 'absent' and (row.get('ttl_ms') != -2 or row.get('payload_sha256')):
                raise ValueError('invalid absence classification')
        add(row, cursor, row['count'])
        cursor += row['count']
        sample_records += 1
        if 'read_duration_ns' in row:
            durations.append(row['read_duration_ns'])
        if row.get('run_id'):
            runs.add(row['run_id'])
        if row.get('campaign_qualified') and row.get('payload_valid'):
            qualified_hashes.add(row['payload_sha256'])
    if (cursor != manifest['recorded_bins'] or sample_records != manifest['sample_records']
            or len(ledger) != manifest['unique_payloads']):
        raise ValueError('manifest sample count mismatch')
    add(dict(status='unrecorded'), cursor, manifest['planned_bins']-cursor)
    durations.sort()
    latency = {name: durations[(len(durations)-1)*percent//100] if durations else None
               for name, percent in (('p50_ns', 50), ('p90_ns', 90), ('p99_ns', 99), ('max_ns', 100))}
    return dict(version=VERSION, status=manifest['status'], reason=manifest['reason'],
                start_ms=manifest['start_ms'], end_ms=manifest['end_ms'], run_ids=sorted(runs),
                observed_run_ids=sorted(runs), observed_qualified_payload_sha256=sorted(qualified_hashes),
                campaign_membership=manifest['campaign_membership'], target_unreceived_at_read='unobserved',
                usability_definition='publication-membership and read-end freshness, with read confined to planned bin; target receipt since publication is unobserved',
                interpretation='sampled local cache observations; not continuous uptime, target arrival or browser delivery',
                full_hour=full, post_65_seconds=warm, minute_bins=minutes, read_latency=latency)


async def _main_observe(args):
    import redis.asyncio as redis_async
    from redis.backoff import NoBackoff
    from redis.asyncio.retry import Retry
    client = redis_async.Redis(host='127.0.0.1', port=args.port, db=args.db, decode_responses=False,
        socket_connect_timeout=0.1, socket_timeout=0.1, retry=Retry(NoBackoff(), 0), retry_on_timeout=False)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        result = await observe(client, Path(args.output_directory), args.start_ms, stop=stop)
    finally:
        await client.aclose()
    print(_json(result).decode(), end='')
    return 0 if result['status'] == 'complete' else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('observe')
    run.add_argument('--start-ms', type=int, required=True)
    run.add_argument('--output-directory', required=True)
    run.add_argument('--port', type=int, default=6379, choices=range(1, 65536), metavar='PORT')
    run.add_argument('--db', type=int, default=0, choices=range(16))
    report = commands.add_parser('analyze')
    report.add_argument('--directory', required=True)
    args = parser.parse_args()
    if args.command == 'observe':
        return asyncio.run(_main_observe(args))
    print(_json(analyze(Path(args.directory))).decode(), end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
