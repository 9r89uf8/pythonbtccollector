"""Bounded operator experiment; refuses the production database before any DDL.

Run only from a separate checkout against an explicitly provisioned disposable
database. No production services import this research module. No DB is created
or dropped here and no production role or settings are changed.
"""
from __future__ import annotations

import argparse
import asyncio
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import time

from .compact import canonical_bytes, decode, verify_compact
from . import layout

DAY_MS = 86400000
RETENTION_MS = 7 * DAY_MS
BATCH = 500
COPIES = 7
TURNOVERS = 3
MAX_DB_BYTES = 2 * 1024**3
WRITE_RESERVE_BYTES = 64 * 1024**2
MIN_FREE_BYTES = 11 * 1024**3
MAX_SECONDS = 1200
PREFIX = 'ghost_compact_storage_validation_'
SCHEMA = Path(__file__).with_name('schema.sql')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def check_database_name(name):
    require(isinstance(name, str) and re.fullmatch(PREFIX+r'[a-z0-9_]{1,35}', name),
            'Refusing database outside the dedicated disposable namespace')


def batches(source):
    batch = []
    with Path(source).open('rb') as stream:
        for raw in iter(lambda: stream.readline(262145), b''):
            require(len(raw) <= 262144 and raw.endswith(b'\n'), 'Oversized/truncated compact record')
            record = verify_compact(decode(raw))
            batch.append(record)
            if len(batch) == BATCH:
                yield batch
                batch = []
    if batch:
        yield batch


def inspect_input(source, manifest):
    digest, size, count = sha256(), 0, 0
    expected, minimum, maximum = {}, None, 0
    with Path(source).open('rb') as stream:
        while True:
            raw = stream.readline(262145)
            if not raw:
                break
            require(len(raw) <= 262144 and raw.endswith(b'\n'), 'Invalid compact line')
            digest.update(raw); size += len(raw); count += 1
            record = verify_compact(decode(raw)); d = record['decision']
            key = (d['run_id'], d['decision_id'])
            require(key not in expected, 'Duplicate compact identity')
            expected[key] = sha256(canonical_bytes(record)).hexdigest()
            minimum = d['created_ms'] if minimum is None else min(minimum, d['created_ms'])
            maximum = max(maximum, d['created_ms'])
    require((digest.hexdigest(), count, size) == (
        manifest['compact_jsonl_sha256'], manifest['decisions'], manifest['logical_bytes']),
        'Compact input does not match its manifest')
    require(count == 7082 and manifest['publication_statuses'] == {
        'acknowledged': 7075, 'expired_or_target_received': 7}, 'Wrong canary cohort')
    require(manifest['maximum_line_bytes']*BATCH*4 < WRITE_RESERVE_BYTES,
            'Input batch shape exceeds experiment write reservation')
    return expected, minimum, maximum


def progress(phase, **values):
    print(json.dumps(dict(phase=phase, **values), sort_keys=True), flush=True)


class Experiment:
    def __init__(self, connection, source, expected, report):
        self.c = connection
        self.source, self.expected, self.report = source, expected, report
        self.decision_ids = tuple(key[1] for key in expected)
        require(len(set(self.decision_ids)) == len(self.decision_ids), 'Ambiguous canary decision IDs')
        self.started = time.monotonic_ns()
        self.directory = None

    def id_batches(self):
        # IDs come from the already hash-verified input inventory. Marker-only
        # updates need no repeated decoding of financial records.
        for offset in range(0, len(self.decision_ids), BATCH):
            yield self.decision_ids[offset:offset+BATCH]

    async def guard(self):
        require(time.monotonic_ns()-self.started < MAX_SECONDS*1000000000, 'Experiment time cap')
        size = await self.c.fetchval('SELECT pg_database_size(current_database())')
        free = shutil.disk_usage(self.directory).free
        require(size + WRITE_RESERVE_BYTES < MAX_DB_BYTES, 'Disposable database allocation/reservation cap')
        require(free >= MIN_FREE_BYTES, 'Filesystem reserve reached')
        return dict(database_bytes=size, filesystem_free_bytes=free)

    async def measure(self, name, format_name):
        guarded = await self.guard()
        tables = []
        for table in layout.TABLES[format_name]:
            row = dict(await self.c.fetchrow('''
                SELECT $1::text AS name, pg_relation_size(c.oid) AS heap_main_bytes,
                  pg_table_size(c.oid) AS table_including_toast_bytes,
                  pg_indexes_size(c.oid) AS base_indexes_bytes,
                  pg_total_relation_size(c.oid) AS total_bytes,
                  CASE WHEN c.reltoastrelid=0 THEN 0 ELSE pg_total_relation_size(c.reltoastrelid) END AS toast_total_bytes,
                  (SELECT n_live_tup FROM pg_stat_user_tables WHERE relid=c.oid) AS estimated_live_tuples,
                  (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid=c.oid) AS estimated_dead_tuples
                FROM pg_class c WHERE c.oid=$1::regclass
                ''', table))
            row['rows'] = await self.c.fetchval('SELECT count(*) FROM '+table)
            row['mean_tuple_bytes'] = str(await self.c.fetchval('SELECT avg(pg_column_size(t)) FROM '+table+' t'))
            tables.append(row)
        result = dict(stage=name, format=format_name, **guarded, tables=tables,
                      total_relation_bytes=sum(t['total_bytes'] for t in tables),
                      elapsed_ms=(time.monotonic_ns()-self.started)//1000000)
        self.report['measurements'].append(result)
        progress(name, format=format_name, relation_bytes=result['total_relation_bytes'],
                 rows=[t['rows'] for t in tables])
        return result

    async def vacuum(self, format_name):
        for table in layout.TABLES[format_name]:
            await self.guard()
            await self.c.execute('VACUUM (ANALYZE) '+table, timeout=60)

    async def insert_copy(self, copy_id, format_name):
        count = 0
        for batch in batches(self.source):
            await self.guard()
            async with self.c.transaction():
                await layout.insert_batch(self.c, batch, copy_id, copy_id*DAY_MS, format_name)
            count += len(batch)
            await asyncio.sleep(0.02)
        require(count == len(self.expected), 'Incomplete copy insert')

    async def verify_copy(self, copy_id, format_name):
        seen, after = set(), None
        while True:
            await self.guard()
            records = await layout.read_records(self.c, format_name, copy_id, after=after, limit=BATCH)
            if not records:
                break
            for record in records:
                verify_compact(record)
                d = record['decision']; key = d['run_id'], d['decision_id']
                require(key not in seen and key in self.expected, 'Unexpected reconstructed identity')
                require(sha256(canonical_bytes(record)).hexdigest() == self.expected[key],
                        'Reconstructed compact record differs from the original')
                seen.add(key)
            after = records[-1]['decision']['run_id'], records[-1]['decision']['decision_id']
        require(len(seen) == len(self.expected), 'Readback row count mismatch')
        self.report['parity_checks'].append(dict(format=format_name, copy_id=copy_id, rows=len(seen), exact=True))

    async def summarize(self, copy_id, format_name):
        # This is a laboratory summary-commit marker, not an implementation of
        # production rollups. It is set only after full reconstruction parity.
        for ids in self.id_batches():
            await self.guard()
            async with self.c.transaction():
                await self.c.execute('UPDATE '+layout.PARENT_TABLES[format_name]+
                    ' SET summarized_revision=probe_revision WHERE copy_id=$1 AND decision_id=ANY($2::text[])',
                    copy_id, ids)

    async def expire(self, copy_id, as_of_ms, format_name, limit=BATCH):
        require(type(limit) is int and 1 <= limit <= BATCH, 'Invalid expiry batch')
        table = layout.PARENT_TABLES[format_name]
        total = 0
        while True:
            await self.guard()
            async with self.c.transaction():
                rows = await self.c.fetch('''WITH eligible AS (
                    SELECT copy_id,run_id,decision_id FROM '''+table+'''
                    WHERE copy_id=$1 AND terminal AND summarized_revision=probe_revision
                      AND retention_created_ms <= $2
                    ORDER BY retention_created_ms,run_id,decision_id
                    LIMIT $3 FOR UPDATE SKIP LOCKED
                  ) DELETE FROM '''+table+''' p USING eligible e
                    WHERE (p.copy_id,p.run_id,p.decision_id)=(e.copy_id,e.run_id,e.decision_id)
                    RETURNING p.decision_id''', copy_id, as_of_ms-RETENTION_MS, limit)
            total += len(rows)
            if len(rows) < limit:
                return total
            await asyncio.sleep(0.02)

    async def stress(self, copy_id, format_name):
        for revision in range(1, 7):
            for ids in self.id_batches():
                await self.guard()
                async with self.c.transaction():
                    await layout.stress_batch(self.c, copy_id, ids, revision, format_name)
                await asyncio.sleep(0.02)
            progress('stress_revision', format=format_name, copy_id=copy_id, revision=revision)

    async def boundaries(self, format_name):
        first = next(batches(self.source))[0]
        created = first['decision']['created_ms']
        asof = created+RETENTION_MS+1000
        table = layout.PARENT_TABLES[format_name]
        cases = [(9001,-1,True,True,1), (9002,0,True,True,1),
                 (9003,1,True,True,0), (9004,-1,False,True,0), (9005,-1,True,False,0)]
        results = []
        for copy_id, delta, terminal, summarized, expected_count in cases:
            offset = asof-RETENTION_MS+delta-created
            await layout.insert_batch(self.c, [first], copy_id, offset, format_name)
            await self.c.execute('UPDATE '+table+' SET terminal=$2,summarized_revision=$3 WHERE copy_id=$1',
                                copy_id, terminal, 0 if summarized else None)
            removed = await self.expire(copy_id, asof, format_name)
            require(removed == expected_count, 'Seven-day boundary/eligibility failure')
            results.append(dict(copy_id=copy_id, delta_ms=delta, terminal=terminal,
                                summarized=summarized, expired=removed))
        transaction = self.c.transaction()
        await transaction.start()
        await self.summarize(9005, format_name)
        await transaction.rollback()  # Simulates a failed summary commit.
        require(await self.expire(9005, asof, format_name) == 0, 'Failed summary allowed expiry')
        await self.summarize(9005, format_name)
        await self.summarize(9005, format_name)
        require(await self.expire(9005, asof, format_name) == 1, 'Committed summary did not allow expiry')
        require(await self.expire(9005, asof, format_name) == 0, 'Expiry retry is not idempotent')
        for copy_id in (9003,9004):
            await self.c.execute('UPDATE '+table+' SET terminal=true WHERE copy_id=$1', copy_id)
            require(await self.expire(copy_id, asof+1, format_name) == 1, 'Fixture cleanup failed')
        self.report['retention_checks'].append(dict(format=format_name, cases=results,
            failed_summary_prevents_delete=True, repeated_summary_and_expiry_idempotent=True))


async def run(dsn, source, manifest, report):
    import asyncpg
    expected, minimum, maximum = inspect_input(source, manifest)
    connection = await asyncpg.connect(dsn, timeout=10, command_timeout=20)
    try:
        name = await connection.fetchval('SELECT current_database()')
        check_database_name(name)
        report['database'] = name
        # Session-only resource settings; not ALTER SYSTEM or cluster tuning.
        for statement in ("SET statement_timeout='20s'", "SET lock_timeout='1s'",
                          "SET work_mem='4MB'", "SET maintenance_work_mem='64MB'",
                          "SET temp_file_limit='128MB'", "SET max_parallel_workers_per_gather=0"):
            await connection.execute(statement)
        experiment = Experiment(connection, source, expected, report)
        experiment.directory = await connection.fetchval('SHOW data_directory')
        report['server_settings'] = [dict(r) for r in await connection.fetch('''
            SELECT name,setting,unit FROM pg_settings WHERE name = ANY($1::text[]) ORDER BY name
            ''', ['server_version','block_size','default_toast_compression','autovacuum',
                  'full_page_writes','wal_level','work_mem','maintenance_work_mem'])]
        report['initial_guard'] = await experiment.guard()
        require(not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='ghost_compact_probe')"),
                'Refusing to reuse an existing experiment schema')
        async with connection.transaction():
            await connection.execute(SCHEMA.read_text(encoding='utf-8'))
        for format_name in ('json','typed'):
            await experiment.measure('empty', format_name)
            for copy_id in range(COPIES):
                await experiment.insert_copy(copy_id, format_name)
                if copy_id in (0,2,6):
                    await experiment.measure('after_'+str(copy_id+1)+'_copies', format_name)
                if copy_id == 0:
                    await experiment.verify_copy(0, format_name)
                    await experiment.summarize(0, format_name)
            for copy_id in range(COPIES):
                if copy_id == 0:
                    continue
                await experiment.verify_copy(copy_id, format_name)
                await experiment.summarize(copy_id, format_name)
            await experiment.vacuum(format_name)
            await experiment.measure('insert_only_vacuumed', format_name)
            await experiment.stress(0, format_name)
            await experiment.measure('after_six_rewrites_one_copy', format_name)
            require(await experiment.expire(0, maximum+RETENTION_MS, format_name) == 0,
                    'Stale summary revision allowed deletion')
            await experiment.verify_copy(0, format_name)
            await experiment.summarize(0, format_name)
            await experiment.vacuum(format_name)
            await experiment.measure('rewrites_vacuumed', format_name)
            for cycle in range(TURNOVERS):
                removed = await experiment.expire(cycle, maximum+(cycle+7)*DAY_MS, format_name)
                require(removed == len(expected), 'Rolling expiry selected wrong cohort')
                await experiment.measure('turnover_'+str(cycle+1)+'_deleted', format_name)
                await experiment.vacuum(format_name)
                await experiment.measure('turnover_'+str(cycle+1)+'_vacuumed', format_name)
                new_copy = COPIES+cycle
                await experiment.insert_copy(new_copy, format_name)
                await experiment.verify_copy(new_copy, format_name)
                await experiment.summarize(new_copy, format_name)
                await experiment.vacuum(format_name)
                await experiment.measure('turnover_'+str(cycle+1)+'_refilled', format_name)
            await experiment.boundaries(format_name)
        report['status'] = 'passed'
        report['final_guard'] = await experiment.guard()
    finally:
        await connection.close(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Refusing to overwrite experiment report')
    manifest = decode(args.input.with_name(args.input.name+'.manifest.json').read_bytes())
    files = [Path(__file__), SCHEMA, Path(layout.__file__), Path(__file__).with_name('compact.py')]
    report = dict(status='running', started_wall_ns=str(time.time_ns()),
        source_manifest=manifest, code_sha256={p.name:sha256(p.read_bytes()).hexdigest() for p in files},
        limits=dict(database_bytes=MAX_DB_BYTES, write_reserve_bytes=WRITE_RESERVE_BYTES, filesystem_free_bytes=MIN_FREE_BYTES,
                    seconds=MAX_SECONDS, batch_decisions=BATCH, copies=COPIES, turnovers=TURNOVERS),
        measurements=[], parity_checks=[], retention_checks=[],
        limitations=['Seven replicated canary hours are a scaled cohort, not seven days of live traffic.',
            'Ordinary manual vacuum here does not establish unattended autovacuum equilibrium.',
            'Six whole-record revisions on one copy are a stress case; normal compact records are inserted after terminalization.',
            'Retention summary markers and probe_revision are laboratory fields, not production monitoring/expiry implementation.',
            'Allocated table totals include indexes and TOAST once; the TOAST breakdown must not be added again.'])
    try:
        asyncio.run(asyncio.wait_for(run(args.dsn, args.input, manifest, report), timeout=MAX_SECONDS))
        require(report['code_sha256'] == {p.name:sha256(p.read_bytes()).hexdigest() for p in files}, 'Probe source changed')
    except BaseException as exc:
        report.update(status='failed', failure_type=type(exc).__name__, failure=str(exc))
        raise
    finally:
        report['finished_wall_ns'] = str(time.time_ns())
        with args.output.open('x', encoding='utf-8') as stream:
            json.dump(report, stream, indent=2, sort_keys=True)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        progress('finished', status=report['status'], output=str(args.output))


if __name__ == '__main__':
    main()
