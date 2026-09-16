"""Two research-only LOGGED layouts in a pre-provisioned disposable database.

Original compact records, identities, clocks and lineage hashes are unchanged.
copy_id, retention_created_ms, terminal, probe_revision and summarized_revision
are explicit laboratory columns, excluded from reconstructed compact records.
The encoder compacts terminal rows only, so terminal starts true. Pending-row
retention tests may alter that laboratory flag without changing source evidence.
No production schema, database creation or database deletion is implemented here.
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, localcontext
from itertools import islice
from pathlib import Path
import re

from research.spot_twap_response.compact_storage.compact import (
    CATEGORIES, EVENT_FIELDS, HORIZONS, canonical_bytes, decode, verify_compact,
)

SCHEMA = 'ghost_compact_probe'
SCHEMA_PATH = Path(__file__).with_name('schema.sql')
JSON_TABLE = SCHEMA + '.compact_json'
DECISION_TABLE = SCHEMA + '.decision'
HORIZON_TABLE = SCHEMA + '.horizon'
PARENT_TABLES = {'json': JSON_TABLE, 'typed': DECISION_TABLE}
TABLES = {'json': (JSON_TABLE,), 'typed': (DECISION_TABLE, HORIZON_TABLE)}
ROW_KEYS = ('copy_id', 'run_id', 'decision_id')
row_keys = ROW_KEYS
MAX_BATCH = 1000
MAX_RECORD_BYTES = 128 * 1024

# (original field, storage kind, nullable). Nested dictionaries are fully typed;
# no duplicate financial JSON representation remains in the typed layout.
DECISION_FIELDS = (
    ('run_id', 'text', False), ('decision_id', 'text', False),
    ('created_ms', 'bigint', False), ('decision_wall_ns', 'bigint', False),
    ('decision_monotonic_ns', 'bigint', False), ('record_version', 'bigint', False),
    ('frozen_sha256', 'hash', False), ('state_sha256', 'hash', False),
    ('attempted_payload_sha256', 'hash', True), ('model_version', 'text', False),
    ('runtime_version', 'text', False), ('contract_version', 'integer', False),
    ('publication_eligibility_policy', 'text', False), ('campaign_start_ms', 'bigint', False),
    ('included_sequence', 'bigint', False),
    ('computation_completed_wall_ns', 'bigint', False),
    ('computation_completed_monotonic_ns', 'bigint', False),
    ('valid_until_wall_ns', 'bigint', False), ('publication_status', 'text', False),
    ('eligibility_checked_wall_ns', 'bigint', True),
    ('eligibility_checked_monotonic_ns', 'bigint', True),
    ('global_reasons', 'text_array', False), ('causality_invalid', 'boolean', False),
    ('shutdown', 'text', True), ('restart_reconciled', 'boolean', False),
    ('publication_restart_outcome', 'text', True), ('gap_count', 'bigint', True),
    ('spot_reconnect_status', 'text', True), ('spot_reconnect_reason', 'text', True),
    ('intent_wall_ns', 'bigint', True), ('intent_monotonic_ns', 'bigint', True),
    ('attempt_wall_ns', 'bigint', True), ('attempt_monotonic_ns', 'bigint', True),
    ('ack_wall_ns', 'bigint', True), ('ack_monotonic_ns', 'bigint', True),
)
POLICY_FIELDS = (
    ('enabled', 'boolean', False), ('source_max_age_ms', 'integer', False),
    ('receipt_max_age_ms', 'integer', False), ('max_carry_ms', 'integer', False),
    ('history_ms', 'integer', False), ('max_events', 'integer', False),
    ('spot_reconnect_max_gap_ms', 'integer', False),
)
HORIZON_FIELDS = (
    ('horizon_s', 'smallint', False), ('target_source_timestamp_ms', 'bigint', True),
    ('forecast_price', 'money', True), ('quality', 'text', False),
    ('max_interior_carry_ms', 'integer', False), ('reasons', 'text_array', False),
    ('attempted_eligible', 'boolean', False), ('exclusion_reasons', 'text_array', False),
    ('estimated_arrival_wall_ns', 'bigint', True), ('estimated_remaining_ns', 'bigint', True),
    ('target_status', 'text', False), ('conflicted', 'boolean', False),
    ('clock_anomaly', 'boolean', False), ('late_missing', 'boolean', False),
    ('error', 'money', True), ('persistence_error', 'money', True),
    ('eta_error_ns', 'bigint', True), ('confirmed_redis_lead_ns', 'bigint', True),
)
EVENT_SPECS = (
    ('value', 'money', True), ('source_timestamp_ms', 'bigint', True),
    ('received_wall_ns', 'bigint', True), ('received_monotonic_ns', 'bigint', True),
    ('sequence', 'bigint', True), ('event_id', 'text', True), ('window_s', 'smallint', True),
)
DECISION_EVENTS = ('current_spot', 'current_twap')
HORIZON_EVENTS = ('first_event', 'first_late_event', 'first_conflicting_event')


def require(value, message):
    if not value:
        raise ValueError(message)


def storage_value(value, kind, nullable):
    if value is None:
        require(nullable, 'Unexpected null')
        return None
    if kind in ('smallint', 'integer', 'bigint'):
        bits = {'smallint': 16, 'integer': 32, 'bigint': 64}[kind]
        require(type(value) is int and -(2**(bits-1)) <= value < 2**(bits-1), 'Integer type/range')
    elif kind == 'boolean':
        require(type(value) is bool, 'Boolean type')
    elif kind == 'text':
        require(isinstance(value, str), 'Text type')
    elif kind == 'text_array':
        require(isinstance(value, list) and all(isinstance(v, str) for v in value), 'Text array type')
    elif kind == 'hash':
        require(isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None, 'SHA256 format')
        return bytes.fromhex(value)
    elif kind == 'money':
        require(isinstance(value, str), 'Decimal string required')
        number = Decimal(value)
        with localcontext() as context:
            context.prec = 80
            require(number.is_finite() and abs(number) < Decimal('1e20') and
                    format(number, '.18f') == value and not (number.is_zero() and number.is_signed()),
                    'Canonical NUMERIC(38,18) value required')
        return number
    else:
        raise ValueError('Unknown storage kind')
    return value


def original_value(value, kind):
    if value is None:
        return None
    if kind == 'money':
        require(isinstance(value, Decimal), 'Database price must remain Decimal')
        return format(value, '.18f')
    if kind == 'hash':
        return bytes(value).hex()
    if kind == 'text_array':
        return list(value)
    return value


def _fields(source, specs, prefix=''):
    return {prefix+field: storage_value(source[field], kind, nullable) for field, kind, nullable in specs}


def _event_fields(event, prefix):
    if event is None:
        return {prefix+'_'+field: None for field, _, _ in EVENT_SPECS}
    require(set(event) == set(EVENT_FIELDS), 'Unexpected event fields')
    require(event['value'] is not None, 'Present event needs a price')
    return _fields(event, EVENT_SPECS, prefix+'_')


def flatten(record, copy_id, retention_offset_ms):
    """Pure conversion, also used to test reconstruction without a database."""
    verify_compact(record)
    storage_value(copy_id, 'integer', False)
    storage_value(retention_offset_ms, 'bigint', False)
    require(copy_id >= 0, 'copy_id must be nonnegative')
    require(len(canonical_bytes(record)) <= MAX_RECORD_BYTES, 'Compact record exceeds lab bound')
    decision = record['decision']
    require(set(decision) == {s[0] for s in DECISION_FIELDS} | {'policy'} | set(DECISION_EVENTS),
            'Unexpected decision fields; update both layouts explicitly')
    require(set(decision['policy']) == {s[0] for s in POLICY_FIELDS}, 'Unexpected policy fields')
    retention = decision['created_ms'] + retention_offset_ms
    storage_value(retention, 'bigint', False)
    require(retention >= 0, 'Negative synthetic retention clock')
    parent = dict(copy_id=copy_id, retention_created_ms=retention, terminal=True,
                  schema_version=record['schema_version'],
                  compact_sha256=storage_value(record['compact_sha256'], 'hash', False))
    parent.update(_fields(decision, DECISION_FIELDS))
    parent.update(_fields(decision['policy'], POLICY_FIELDS, 'policy_'))
    for name in DECISION_EVENTS:
        parent.update(_event_fields(decision[name], name))
    children = []
    for horizon in record['horizons']:
        require(set(horizon) == {s[0] for s in HORIZON_FIELDS} | {'counts'} | set(HORIZON_EVENTS),
                'Unexpected horizon fields; update both layouts explicitly')
        require(set(horizon['counts']) == set(CATEGORIES), 'Unexpected count fields')
        child = dict(copy_id=copy_id, run_id=decision['run_id'], decision_id=decision['decision_id'])
        child.update(_fields(horizon, HORIZON_FIELDS))
        child.update({'count_'+key: storage_value(horizon['counts'][key], 'smallint', False) for key in CATEGORIES})
        for name in HORIZON_EVENTS:
            child.update(_event_fields(horizon[name], name))
        children.append(child)
    return parent, children


def _restore_event(row, prefix):
    event = {field: original_value(row[prefix+'_'+field], kind) for field, kind, _ in EVENT_SPECS}
    if event['value'] is None:
        require(all(v is None for v in event.values()), 'Partially absent stored event')
        return None
    return event


def reconstruct(parent, children):
    """Restore exact canonical compact fields; laboratory markers are ignored."""
    decision = {field: original_value(parent[field], kind) for field, kind, _ in DECISION_FIELDS}
    decision['policy'] = {field: original_value(parent['policy_'+field], kind) for field, kind, _ in POLICY_FIELDS}
    for name in DECISION_EVENTS:
        decision[name] = _restore_event(parent, name)
    ordered = sorted(children, key=lambda item: item['horizon_s'])
    require([r['horizon_s'] for r in ordered] == list(HORIZONS), 'Missing/duplicate stored horizons')
    horizons = []
    for row in ordered:
        require(all(row[key] == parent[key] for key in ROW_KEYS), 'Child identity mismatch')
        horizon = {field: original_value(row[field], kind) for field, kind, _ in HORIZON_FIELDS}
        horizon['counts'] = {key: row['count_'+key] for key in CATEGORIES}
        for name in HORIZON_EVENTS:
            horizon[name] = _restore_event(row, name)
        horizons.append(horizon)
    result = dict(schema_version=parent['schema_version'], decision=decision, horizons=horizons,
                  compact_sha256=original_value(parent['compact_sha256'], 'hash'))
    return verify_compact(result)


async def insert_batch(conn, records, copy_id, retention_offset_ms, format_name):
    require(format_name in TABLES, 'Unknown layout format')
    records = list(islice(iter(records), MAX_BATCH+1))
    require(0 < len(records) <= MAX_BATCH, 'Batch must contain1..1000 decisions')
    parents, children, json_rows, seen = [], [], [], set()
    for record in records:
        parent, items = flatten(record, copy_id, retention_offset_ms)
        identity = parent['run_id'], parent['decision_id']
        require(identity not in seen, 'Duplicate decision in batch')
        seen.add(identity)
        parents.append(parent)
        children.extend(items)
        target_stamps = sorted({child['target_source_timestamp_ms'] for child in items
                                if child['target_source_timestamp_ms'] is not None})
        json_rows.append((copy_id, *identity, parent['retention_created_ms'], target_stamps, True,
                          canonical_bytes(record).decode('ascii')))
    async with conn.transaction():
        if format_name == 'json':
            await conn.copy_records_to_table('compact_json', schema_name=SCHEMA,
                columns=('copy_id','run_id','decision_id','retention_created_ms',
                         'target_source_timestamps_ms','terminal','record_json'),
                records=json_rows)
        else:
            for table, rows in (('decision', parents), ('horizon', children)):
                columns = tuple(rows[0])
                require(all(tuple(row) == columns for row in rows), 'Inconsistent typed columns')
                await conn.copy_records_to_table(table, schema_name=SCHEMA, columns=columns,
                    records=[tuple(row[key] for key in columns) for row in rows])
    return {'parent_rows': len(records), 'horizon_rows': len(children) if format_name == 'typed' else 0}


async def read_records(conn, format_name, copy_id, *, after=None, limit=1000):
    require(format_name in TABLES, 'Unknown layout format')
    storage_value(copy_id, 'integer', False)
    require(type(limit) is int and 1 <= limit <= MAX_BATCH, 'Invalid page limit')
    after = ('', '') if after is None else after
    require(isinstance(after, (tuple,list)) and len(after) == 2 and all(isinstance(x,str) for x in after), 'Invalid cursor')
    # The table identifier comes only from the fixed allowlist, never caller SQL.
    parents = await conn.fetch(f'SELECT * FROM {PARENT_TABLES[format_name]} WHERE copy_id=$1 '
        'AND (run_id,decision_id)>($2,$3) ORDER BY run_id,decision_id LIMIT $4', copy_id, *after, limit)
    if format_name == 'json':
        result = []
        for parent in parents:
            record = verify_compact(decode(parent['record_json']))
            require((record['decision']['run_id'], record['decision']['decision_id']) ==
                    (parent['run_id'], parent['decision_id']), 'JSON container identity mismatch')
            result.append(record)
        return result
    if not parents:
        return []
    rows = await conn.fetch(f'SELECT h.* FROM {HORIZON_TABLE} h '
        'JOIN unnest($2::text[],$3::text[]) AS k(run_id,decision_id) '
        'ON h.run_id=k.run_id AND h.decision_id=k.decision_id '
        'WHERE h.copy_id=$1 ORDER BY h.run_id,h.decision_id,h.horizon_s', copy_id,
        [r['run_id'] for r in parents], [r['decision_id'] for r in parents])
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['run_id'],row['decision_id']].append(row)
    return [reconstruct(parent, grouped[parent['run_id'],parent['decision_id']]) for parent in parents]


async def stress_batch(conn, copy_id, decision_ids, revision, format_name, *, run_id):
    """Explicit whole-record versus typed MVCC stress, never financial edits.

    JSON receives only trailing JSON whitespace, forcing a changed TEXT value.
    The compact canonical body/hash remains unchanged after decoding. This
    models write amplification, not actual result-update timing or seven days.
    A prior lab summary marker is invalidated until the runner verifies parity.
    """
    require(format_name in TABLES, 'Unknown layout format')
    require(isinstance(run_id, str) and 0 < len(run_id) <= 128, 'Invalid stress run identity')
    storage_value(copy_id, 'integer', False)
    decision_ids = list(islice(iter(decision_ids), MAX_BATCH+1))
    require(0 < len(decision_ids) <= MAX_BATCH and all(isinstance(x,str) for x in decision_ids)
            and len(set(decision_ids)) == len(decision_ids), 'Invalid stress decision IDs')
    require(type(revision) is int and 1 <= revision <= 64, 'Invalid bounded stress revision')
    counts = {}
    async with conn.transaction():
        parent = PARENT_TABLES[format_name]
        rewrite = ",record_json=rtrim(record_json,chr(10))||repeat(chr(10),$3)" if format_name=='json' else ''
        rows = await conn.fetch(f'UPDATE {parent} SET probe_revision=$3,summarized_revision=NULL{rewrite} '
            'WHERE copy_id=$1 AND run_id=$4 AND decision_id=ANY($2::text[]) RETURNING run_id,decision_id',
            copy_id, decision_ids, revision, run_id)
        counts['parent_rows'] = len(rows)
        counts['horizon_rows'] = 0
        if format_name == 'typed':
            children = await conn.fetch(f'UPDATE {HORIZON_TABLE} SET probe_revision=$3 '
                'WHERE copy_id=$1 AND run_id=$4 AND decision_id=ANY($2::text[]) RETURNING horizon_s',
                copy_id, decision_ids, revision, run_id)
            counts['horizon_rows'] = len(children)
    return counts
