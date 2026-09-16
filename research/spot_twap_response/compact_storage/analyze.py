"""Offline Decimal projections of a completed disposable storage experiment.

No database/client dependencies or production commands. Projections are
conditional illustrations from replicated records, never a capacity guarantee.
"""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path
import re

GIB = 1024**3
PER_HOUR = 7082
FULL_POPULATION = PER_HOUR * 7
WEEK_POPULATION = PER_HOUR * 24 * 7
TABLES = {'json': ('ghost_compact_probe.compact_json',),
          'typed': ('ghost_compact_probe.decision', 'ghost_compact_probe.horizon')}
STAGES = ('empty', 'after_1_copies', 'after_3_copies', 'after_7_copies',
          'insert_only_vacuumed', 'after_six_rewrites_one_copy', 'rewrites_vacuumed') + tuple(
              f'turnover_{cycle}_{phase}' for cycle in (1,2,3) for phase in ('deleted','vacuumed','refilled'))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, name):
    require(type(value) is int and value >= 0, 'Invalid nonnegative integer: '+name)
    return value


def strict_json(raw):
    def reject(value):
        raise ValueError('Unexpected floating/nonfinite JSON number: '+value)
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'Duplicate JSON key')
            result[key] = value
        return result
    return json.loads(raw, parse_float=reject, parse_constant=reject, object_pairs_hook=unique)


def expected_decisions(stage):
    if stage == 'empty':
        return 0
    if stage == 'after_1_copies':
        return PER_HOUR
    if stage == 'after_3_copies':
        return PER_HOUR*3
    if stage.startswith('turnover_') and not stage.endswith('_refilled'):
        return PER_HOUR*6
    return FULL_POPULATION


def validate_report(report):
    require(report['status'] == 'passed', 'Only a fully passed probe may be projected')
    require(re.fullmatch(r'ghost_compact_storage_validation_[a-z0-9_]{1,35}', report['database']) is not None,
            'Unexpected experiment database')
    source = report['source_manifest']
    require(source['decisions'] == PER_HOUR and source['horizon_records'] == PER_HOUR*6,
            'Wrong source population')
    require(source['publication_statuses'] == {'acknowledged':7075,'expired_or_target_received':7},
            'Wrong source publication cohort')
    require(source['run_id'] == 'fe2dd06da5754c50b696cb9419ca7251' and source['input_rows'] == 23411,
            'Wrong canary identity')
    require(source['input_sha256'] == 'f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954',
            'Wrong verified source export')
    for name in ('input_sha256','compact_jsonl_sha256','code_sha256'):
        require(re.fullmatch('[0-9a-f]{64}', source[name]) is not None, 'Malformed source hash')
    require(report['limits']['copies'] == 7 and report['limits']['turnovers'] == 3,
            'Unexpected replication/turnover protocol')
    require(len(report['measurements']) == len(STAGES)*2, 'Incomplete measurement domain')
    measurements = {}
    for measurement in report['measurements']:
        fmt, stage = measurement['format'], measurement['stage']
        require(fmt in TABLES and stage in STAGES and (fmt,stage) not in measurements,
                'Duplicate/unexpected stage')
        tables = measurement['tables']
        require(tuple(t['name'] for t in tables) == TABLES[fmt], 'Wrong relation measurement domain')
        count = expected_decisions(stage)
        for index, table in enumerate(tables):
            require(table['rows'] == count*(6 if index else 1), 'Wrong logical population at '+stage)
            for field in ('rows','heap_main_bytes','table_including_toast_bytes','base_indexes_bytes',
                          'total_bytes','toast_total_bytes'):
                integer(table[field], field)
            require(table['total_bytes'] == table['table_including_toast_bytes']+table['base_indexes_bytes'],
                    'Relation total/index accounting mismatch')
            require(table['table_including_toast_bytes'] >= table['heap_main_bytes']+table['toast_total_bytes'],
                    'Invalid TOAST breakdown')
        require(measurement['total_relation_bytes'] == sum(t['total_bytes'] for t in tables),
                'Layout total double-counts or omits allocation')
        require(measurement['database_bytes'] >= measurement['total_relation_bytes'], 'Database smaller than layout')
        require(measurement['database_bytes'] + report['limits']['write_reserve_bytes'] < report['limits']['database_bytes'],
                'Reported database/write reservation exceeds experiment cap')
        require(measurement['filesystem_free_bytes'] >= report['limits']['filesystem_free_bytes'],
                'Reported filesystem crossed experiment reserve')
        measurements[fmt,stage] = measurement
    required_copies = Counter({copy_id:1 for copy_id in range(10)})
    required_copies[0] = 2  # Initial and after six whole-record marker revisions.
    for fmt in TABLES:
        checks = [r for r in report['parity_checks'] if r['format'] == fmt]
        require(Counter(r['copy_id'] for r in checks) == required_copies and
                all(r['rows'] == PER_HOUR and r['exact'] is True for r in checks), 'Incomplete exact parity checks')
        checks = [r for r in report['retention_checks'] if r['format'] == fmt]
        require(len(checks) == 1, 'Missing retention test')
        check = checks[0]
        require(check['failed_summary_prevents_delete'] is True and
                check['repeated_summary_and_expiry_idempotent'] is True, 'Failed retention-marker checks')
        cases = [(r['delta_ms'],r['terminal'],r['summarized'],r['expired']) for r in check['cases']]
        require(cases == [(-1,True,True,1),(0,True,True,1),(1,True,True,0),
                          (-1,False,True,0),(-1,True,False,0)], 'Wrong seven-day boundary cases')
    require(len(report['parity_checks']) == 22 and len(report['retention_checks']) == 2,
            'Extra/missing validation formats')
    return measurements


def capacity(preflight):
    require(preflight['read_only'] is True and preflight['initial_headroom_ok'] is True,
            'Preflight did not pass its initial headroom check')
    directory = preflight['database']['data_directory']
    filesystems = [f for f in preflight['filesystems'] if f['path'] == directory]
    require(len(filesystems) == 1, 'Missing/ambiguous actual database filesystem')
    available = integer(filesystems[0]['available_bytes'], 'available bytes')
    reserve = integer(preflight['ghost_fixed_code_caps']['RESERVE_BYTES'], 'disk reserve')
    relations = {(r['schema_name'],r['relname']):r for r in preflight['database']['relations']}

    def setting(service, key):
        process = preflight['running_process_env_whitelist'][service]
        if key in process:
            value, origin = process[key], 'running_process_override'
        else:
            value, origin = preflight['code_default_whitelist'][key]['value'], 'code_default'
        return value, origin

    groups = []
    for name, service, prefix, names in (
        ('evidence','price-collector-polymarket-probabilities','POLYMARKET_EVIDENCE',
         ('polymarket_evidence_payloads','polymarket_market_observations','polymarket_quote_observations')),
        ('microstructure','price-collector-binance-futures','BINANCE_MICROSTRUCTURE',('binance_microstructure_1s',))):
        cap, origin = setting(service,prefix+'_MAX_RELATION_MB')
        require(type(cap) is int or isinstance(cap,str) and cap.isdigit(), 'Invalid configured threshold')
        cap_bytes = int(cap)*1024**2
        allocated = sum(integer(relations['public',name]['total_bytes'],name) for name in names)
        enabled, _ = setting(service,prefix+'_ENABLED')
        groups.append(dict(name=name, relations=list(names), allocated_bytes=allocated,
            configured_threshold_bytes=cap_bytes, configured_threshold_source=origin,
            enabled_at_preflight=enabled in (True,'true','True','1'),
            remaining_growth_to_threshold_bytes=max(0,cap_bytes-allocated)))
    remaining = sum(g['remaining_growth_to_threshold_bytes'] for g in groups)
    result = dict(preflight_available_filesystem_bytes=available, preserved_disk_reserve_bytes=reserve,
        available_above_reserve_bytes=available-reserve, other_relation_groups=groups,
        reserved_remaining_growth_to_other_thresholds_bytes=remaining,
        conditional_residual_bytes=available-reserve-remaining,
        existing_ghost_allocated_bytes=relations['public','ghost_twap_audit']['total_bytes'],
        no_existing_allocation_subtracted_twice=True, no_legacy_reclamation_credit=True)
    result['conditional_residual_gib'] = str(Decimal(result['conditional_residual_bytes'])/GIB)
    result['limits'] = [
        'Point-in-time available filesystem bytes already subtract all currently allocated data.',
        'Subtract only remaining growth to the two configured relation thresholds, not their full thresholds again.',
        'Evidence threshold pauses high-rate quotes; metadata can continue. These thresholds are not total disk guarantees.',
        'Core histories, WAL, temporary/maintenance files, outbox and other future growth remain unbudgeted.',
        'Existing legacy ghost storage remains allocated; no assumed deletion or filesystem reclamation is credited.',
        'This residual is conditional headroom, not a recommended ghost storage budget.']
    return result


def analyze(report, preflight):
    with localcontext() as context:
        context.prec = 80
        measured = validate_report(report)
        shared = capacity(preflight)
        layouts = {}
        for fmt in TABLES:
            empty = measured[fmt,'empty']['total_relation_bytes']
            stages = []
            for stage in STAGES:
                item = measured[fmt,stage]
                n, allocated = expected_decisions(stage), item['total_relation_bytes']
                row = dict(stage=stage, decisions=n, allocation_bytes=allocated,
                    allocation_gib=str(Decimal(allocated)/GIB), tables=item['tables'],
                    disposable_database_bytes=item['database_bytes'], filesystem_free_bytes=item['filesystem_free_bytes'])
                if n:
                    ratio = Decimal(WEEK_POPULATION)/n
                    proportional = Decimal(allocated)*ratio
                    baseline_adjusted = Decimal(empty)+Decimal(allocated-empty)*ratio
                    row.update(allocated_bytes_per_live_decision=str(Decimal(allocated)/n),
                        week_population_multiplier=str(ratio),
                        proportional_week_allocation_bytes=str(proportional),
                        proportional_week_allocation_gib=str(proportional/GIB),
                        fixed_empty_baseline_week_allocation_bytes=str(baseline_adjusted),
                        fixed_empty_baseline_week_allocation_gib=str(baseline_adjusted/GIB),
                        conditional_headroom_after_proportional_week_bytes=str(Decimal(shared['conditional_residual_bytes'])-proportional))
                stages.append(row)
            full = [r for r in stages if r['decisions'] == FULL_POPULATION]
            maximum = max(full,key=lambda r:r['allocation_bytes'])
            refill = [measured[fmt,f'turnover_{cycle}_refilled']['total_relation_bytes'] for cycle in (1,2,3)]
            layouts[fmt] = dict(measured_empty_allocation_bytes=empty, stages=stages,
                largest_full_population_stage=maximum['stage'],
                largest_full_population_allocation_bytes=maximum['allocation_bytes'],
                largest_full_population_proportional_week_allocation_bytes=maximum['proportional_week_allocation_bytes'],
                largest_full_population_proportional_week_allocation_gib=maximum['proportional_week_allocation_gib'],
                conditional_headroom_after_largest_proportional_week_bytes=maximum['conditional_headroom_after_proportional_week_bytes'],
                turnover_refilled_allocation_bytes=refill,
                turnover_refilled_deltas_bytes=[b-a for a,b in zip(refill,refill[1:])])
        comparison = {}
        for stage in STAGES:
            a, b = measured['json',stage]['total_relation_bytes'], measured['typed',stage]['total_relation_bytes']
            comparison[stage] = dict(json_bytes=a, typed_bytes=b, typed_minus_json_bytes=b-a,
                typed_divided_by_json=str(Decimal(b)/a) if a else None)
        return dict(status='accepted', source_decisions=PER_HOUR, replicated_full_population=FULL_POPULATION,
            projected_week_decisions=WEEK_POPULATION, horizons_per_decision=6,
            rate_assumption='7082 decisions/hour repeated for168hours; descriptive assumption from one observed hour',
            layouts=layouts, same_stage_comparison=comparison, conditional_shared_capacity=shared,
            probe_status=report['status'], probe_code_sha256=report['code_sha256'],
            source_manifest=report['source_manifest'],
            method_limits=[
                'pg_total_relation_size totals include base indexes and TOAST once. Breakdown columns are not added again.',
                'Proportional projections scale the actual allocated total by target/live population; the optional fixed-empty-baseline projection retains initial table overhead once.',
                'Neither projection is a fitted or validated seven-day capacity model; replicated hours are not independent/live seven-day observations.',
                'Deleted stages have fewer live decisions but can retain dead allocation; their per-live-decision projections must not be mistaken for insert-only costs.',
                'Six lab revision changes affect one of seven copies. JSON is physically rewritten with trailing whitespace; typed marker updates are not equivalent real financial-field updates.',
                'Summary markers add measured MVCC overhead; they are not a production rollup implementation.',
                'Ordinary manual vacuum and three turnover cycles do not establish unattended autovacuum equilibrium or a sustained allocation plateau.',
                'Seven-day eligibility tests use simulated retention clocks and a maintenance query, not seven elapsed days or installed production deletion protection.',
                'Hashes preserve lineage and exact compact roundtrip; compact records do not permit deleted slot or exact-payload-byte replay.',
                'Keep allocated-byte and actual filesystem guards. No live enablement, guard increase or guaranteed storage budget follows from these results.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--preflight',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Refusing to overwrite storage analysis')
    raw, preflight = args.report.read_bytes(), args.preflight.read_bytes()
    result = analyze(strict_json(raw),strict_json(preflight))
    result['provenance'] = {str(path):{'sha256':sha256(data).hexdigest(),'bytes':len(data)}
        for path,data in ((args.report,raw),(args.preflight,preflight),(Path(__file__),Path(__file__).read_bytes()))}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as stream:
        json.dump(result,stream,indent=2,sort_keys=True)
        stream.write('\n')
    print(json.dumps({'status':result['status'],'output':str(args.output),
        'conditional_residual_bytes':result['conditional_shared_capacity']['conditional_residual_bytes']}))


if __name__ == '__main__':
    main()
