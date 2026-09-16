"""Pure layout checks. Actual PostgreSQL sizing/readback belongs to the probe."""
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal, localcontext
from hashlib import sha256
import re

import pytest

from research.spot_twap_response.compact_storage import compact, layout
from research.spot_twap_response.compact_storage.test_compact import encode, fixture


def rehash(record):
    body = {key: value for key, value in record.items() if key != 'compact_sha256'}
    record['compact_sha256'] = sha256(compact.canonical_bytes(body)).hexdigest()
    return record


def test_typed_roundtrip_preserves_every_field_and_hash_under_low_decimal_precision():
    record = encode(fixture())
    with localcontext() as context:
        context.prec = 6
        parent, children = layout.flatten(record, 7, 1234567)
        assert parent['retention_created_ms'] == record['decision']['created_ms'] + 1234567
        assert parent['decision_wall_ns'] == record['decision']['decision_wall_ns']
        assert isinstance(parent['frozen_sha256'], bytes) and len(parent['frozen_sha256']) == 32
        assert isinstance(children[0]['forecast_price'], Decimal)
        parent.update(probe_revision=6, summarized_revision=6, terminal=False)
        for child in children:
            child['probe_revision'] = 6
        assert layout.reconstruct(parent, children) == record
        assert compact.canonical_bytes(layout.reconstruct(parent, children)) == compact.canonical_bytes(record)


def test_optional_events_and_flags_survive_without_a_financial_json_blob():
    record = encode(fixture())
    record['decision']['current_spot'] = None
    record['decision']['attempted_payload_sha256'] = None
    record['decision']['shutdown'] = 'unobserved_after_shutdown'
    record['decision']['spot_reconnect_status'] = 'cleared'
    record['decision']['spot_reconnect_reason'] = 'test-gap'
    last = record['horizons'][-1]
    last['first_late_event'] = deepcopy(last['first_event'])
    last['first_late_event']['event_id'] = 'late-event'
    last['first_conflicting_event'] = deepcopy(last['first_event'])
    last['first_conflicting_event']['value'] = '101.123456789012345678'
    last['first_conflicting_event']['event_id'] = 'conflict-event'
    last['conflicted'] = True
    last['late_missing'] = True
    rehash(record)
    parent, children = layout.flatten(record, 0, 0)
    assert all(parent['current_spot_'+name] is None for name in compact.EVENT_FIELDS)
    assert layout.reconstruct(parent, children) == record
    assert not any(isinstance(value, dict) for value in parent.values())
    assert not any(isinstance(value, dict) for child in children for value in child.values())


@pytest.mark.parametrize('location', ['decision', 'policy', 'horizon', 'event'])
def test_new_fields_cannot_be_silently_dropped(location):
    record = encode(fixture())
    target = {'decision': record['decision'], 'policy': record['decision']['policy'],
              'horizon': record['horizons'][0], 'event': record['decision']['current_twap']}[location]
    target['new_evidence'] = 1
    rehash(record)
    with pytest.raises(ValueError, match='Unexpected'):
        layout.flatten(record, 0, 0)


@pytest.mark.parametrize('value', ['1.0', '-0.000000000000000000', '1.0000000000000000001',
                                  '100000000000000000000.000000000000000000', 'NaN'])
def test_numeric_storage_cannot_silently_change_canonical_financial_values(value):
    with pytest.raises(ValueError, match='Canonical'):
        layout.storage_value(value, 'money', True)


def test_reconstruction_rejects_missing_or_wrong_identity_horizon():
    parent, children = layout.flatten(encode(fixture()), 1, 0)
    with pytest.raises(ValueError, match='Missing/duplicate'):
        layout.reconstruct(parent, children[:-1])
    children[0]['copy_id'] = 2
    with pytest.raises(ValueError, match='identity'):
        layout.reconstruct(parent, children)


def test_research_ddl_covers_typed_fields_and_both_lookup_workloads():
    parent, children = layout.flatten(encode(fixture()), 1, 0)
    ddl = layout.SCHEMA_PATH.read_text()
    for table, row in [('decision', parent), ('horizon', children[0])]:
        body = ddl.split('CREATE TABLE ghost_compact_probe.'+table+' (', 1)[1].split('\n) WITH ', 1)[0]
        columns = set(re.findall(r'^    ([a-z_0-9]+) (?:INTEGER|SMALLINT|BIGINT|TEXT|BOOLEAN|NUMERIC|BYTEA)', body, re.M))
        assert set(row).issubset(columns)
        assert 'probe_revision' in columns
    assert 'USING GIN (target_source_timestamps_ms)' in ddl
    assert 'CREATE INDEX horizon_target_idx' in ddl
    assert ddl.count('ON DELETE CASCADE') == 1
    assert ddl.count('summarized_revision INTEGER') == 2
    assert 'UNLOGGED' not in ddl and 'DROP ' not in ddl and 'CREATE DATABASE' not in ddl
    assert 'JSONB' not in ddl  # Every current encoder field has a real typed home.


def test_manual_vacuum_isolation_is_limited_to_three_disposable_tables_and_toast():
    ddl = layout.SCHEMA_PATH.read_text()
    options = 'autovacuum_enabled = false, toast.autovacuum_enabled = false'
    names = re.findall(r'CREATE TABLE ghost_compact_probe\.([a-z_]+) \([\s\S]*?\n\) WITH \('
                       + re.escape(options) + r'\);', ddl)
    assert names == ['compact_json', 'decision', 'horizon']
    assert ddl.count(') WITH ('+options+');') == 3
    assert 'ALTER SYSTEM' not in ddl and 'ALTER DATABASE' not in ddl
    assert 'CREATE TABLE public.' not in ddl and 'ALTER TABLE public.' not in ddl


class CaptureConnection:
    def __init__(self):
        self.copies = []
        self.statements = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def copy_records_to_table(self, table, **kwargs):
        self.copies.append((table, kwargs))

    async def fetch(self, sql, *args):
        self.statements.append((sql, args))
        return []


def test_json_target_lookup_keys_and_canonical_body_come_from_same_record():
    record = encode(fixture())
    conn = CaptureConnection()
    asyncio.run(layout.insert_batch(conn, [record], 2, 0, 'json'))
    table, copy = conn.copies[0]
    row = dict(zip(copy['columns'], copy['records'][0]))
    assert table == 'compact_json' and copy['schema_name'] == 'ghost_compact_probe'
    assert row['target_source_timestamps_ms'] == sorted({h['target_source_timestamp_ms'] for h in record['horizons']})
    assert compact.decode(row['record_json']) == record
    assert row['terminal'] is True


def test_stress_forces_json_rewrite_and_invalidates_parent_summary_only_in_fixed_tables():
    conn = CaptureConnection()
    asyncio.run(layout.stress_batch(conn, 3, ['1'], 6, 'json'))
    sql, arguments = conn.statements[0]
    assert 'rtrim(record_json,chr(10))||repeat(chr(10),$3)' in sql
    assert 'summarized_revision=NULL' in sql
    assert arguments == (3, ['1'], 6)
    with pytest.raises(ValueError, match='Unknown layout'):
        asyncio.run(layout.stress_batch(conn, 3, ['1'], 6, 'public.ghost_twap_audit'))
    assert len(conn.statements) == 1
