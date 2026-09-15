"""Exact browser-clock cohorts and Decimal pairing, with no runtime dependency."""
from copy import deepcopy
from decimal import Decimal, localcontext
import hashlib
import gzip
import json
from pathlib import Path

import pytest

from research.spot_twap_response.checkpoint_c.analyze_browser import (
    HORIZONS, InvalidCapture, analyze_capture, main, write_results,
)

BASE = 1_800_000_000_000
E18 = '100.000000000000000000'


def ghost(at, *, source=BASE, run='run-a', decision=1, value=E18, predicted=E18,
          eligible=(5,), resync=False, skips=0, instance='api-a'):
    forecasts = []
    for h in HORIZONS:
        forecasts.append(dict(horizon_s=h, target_source_timestamp_ms=source+h*1000,
            counts=dict(observed=60, carried=0, pending=0, future=0, missing=0),
            reasons=[] if h in eligible else ['unavailable'],
            quality='healthy' if h in eligible else 'unavailable', price=predicted if h in eligible else None))
    body = dict(runtime_version='ghost-canary-v6', contract_version=4,
        model_version='chainlink-60s-offset3-v1', publication_state='attempted',
        run_id=run, decision_id=str(decision), publication_sequence=decision, reasons=[],
        current_twap=dict(feed='twap', window_s=60, source_timestamp_ms=source,
            received_wall_ns=str((source+500)*1_000_000), event_id='twap-'+str(source), value=value),
        publication_eligibility=dict(version=1, eligible_horizons=list(eligible),
            excluded_horizons={str(h): ['unavailable'] for h in HORIZONS if h not in eligible}),
        forecasts=forecasts)
    api = dict(version=1, state='snapshot', reason='fresh', remaining_ns='1000000000',
               instance_id=instance, generation=1, sequence=decision, resync=resync, skipped_updates=skips)
    return dict(kind='ghost', browser_ms=at, data=json.dumps(dict(api=api, ghost=body), separators=(',', ':')))


def mutate(record, function):
    result = deepcopy(record)
    envelope = json.loads(result['data'])
    function(envelope)
    result['data'] = json.dumps(envelope, separators=(',', ':'))
    return result


def capture(tmp_path, records=(), *, end=1_020_000, initial=0, name='capture.jsonl'):
    path = tmp_path/name
    content = [dict(kind='start', browser_ms=initial, observation_ms=900_000, drain_ms=120_000),
               dict(kind='open', browser_ms=initial)]
    content.extend(records)
    if end is not None:
        content.append(dict(kind='end', browser_ms=end))
    path.write_text(''.join(json.dumps(row, separators=(',', ':'))+'\n' for row in content), encoding='utf-8')
    return path


def horizon(summary, h=5, panel='all_admissions'):
    return next(row for row in summary[panel] if row['horizon_s'] == h)


def test_exact_decimal_prices_and_browser_lead_under_low_ambient_precision(tmp_path):
    records = [ghost(100.25, predicted='100.000000000000000001'),
               ghost(200.75, source=BASE+100_000, decision=2, predicted='100.000000000000000003'),
               ghost(1000.5, source=BASE+5000, decision=3, eligible=()),
               ghost(1200.25, source=BASE+105_000, decision=4, eligible=())]
    path = capture(tmp_path, records)
    with localcontext() as context:
        context.prec = 3
        result = analyze_capture(path)
    row = horizon(result)
    assert row['candidate_N'] == 4 and row['admitted'] == row['matched'] == 2 and row['excluded'] == 2
    assert Decimal(row['absolute_error_usd']['median']) == Decimal('0.000000000000000002')
    assert Decimal(row['absolute_error_bps']['median']) == Decimal('0.0000000000000002')
    assert Decimal(row['browser_lead_ms']['median']) == Decimal('949.875')
    assert Decimal(row['browser_lead_ms']['p90']) == Decimal('989.575')
    assert row['positive_fraction_of_matched'] == '1'
    assert row['censored'] == 0
    assert result['source_sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_exact_target_censoring_including_conflict_and_already_observed(tmp_path):
    records = [ghost(0, decision=1),
        ghost(100, source=BASE+105_000, decision=2, eligible=()),
        ghost(200, source=BASE+100_000, decision=3),  # Target already observed.
        ghost(300, source=BASE+200_000, decision=4),  # Conflicting later target.
        ghost(400, source=BASE+300_000, decision=5),  # Only the next stamp arrives.
        ghost(500, source=BASE+5000, decision=6, eligible=()),
        ghost(600, source=BASE+205_000, decision=7, eligible=()),
        ghost(700, source=BASE+205_000, decision=8, eligible=(), value='101.000000000000000000'),
        ghost(800, source=BASE+306_000, decision=9, eligible=())]
    result = analyze_capture(capture(tmp_path, records))
    row = horizon(result)
    assert row['admitted'] == 4 and row['matched'] == 1 and row['censored'] == 3
    assert row['censor_reasons'] == dict(missing_exact_target=1, conflicting_target_values=1, target_already_observed=1)
    assert row['positive_fraction_of_matched'] == '1'
    assert row['positive_fraction_of_nonconflicting_observed_targets'] == '0.5'
    assert result['conflicting_anchor_stamps'] == 1
    for item in result['all_admissions']:
        assert item['candidate_N'] == item['admitted']+item['excluded']
        assert item['admitted'] == item['matched']+item['censored']
        assert item['censored'] == sum(item['censor_reasons'].values())


def test_duplicate_envelopes_do_not_count_held_last_and_restart_identity_is_distinct(tmp_path):
    first = ghost(10, resync=True)
    duplicate = deepcopy(first)
    duplicate['browser_ms'] = 20
    second = ghost(30, run='run-b')
    actual = ghost(500, source=BASE+5000, decision=2, run='run-b', eligible=())
    result = analyze_capture(capture(tmp_path, [first, duplicate, second, actual]))
    assert horizon(result)['admitted'] == horizon(result)['matched'] == 2
    assert horizon(result)['resync_admitted'] == horizon(result)['resync_matched'] == 1
    assert result['duplicate_producer_envelopes'] == 1
    assert result['producer_run_count'] == 2
    assert result['runs']['run-a']['admission_decisions'] == 1
    assert result['runs']['run-b']['admission_decisions'] == 2


def test_conflicting_body_same_run_decision_fails_integrity(tmp_path):
    first = ghost(0)
    different = ghost(1, predicted='101.000000000000000000')
    with pytest.raises(InvalidCapture, match='conflicting payloads'):
        analyze_capture(capture(tmp_path, [first, different]))


def test_admission_warmup_and_drain_boundaries(tmp_path):
    records = [ghost(64_999, decision=1), ghost(65_000, source=BASE+100_000, decision=2),
        ghost(899_999, source=BASE+200_000, decision=3),
        ghost(900_000, source=BASE+300_000, decision=4),
        ghost(1_020_000, source=BASE+105_000, decision=5, eligible=()),
        ghost(1_020_001, source=BASE+205_000, decision=6, eligible=())]
    result = analyze_capture(capture(tmp_path, records, end=1_020_001))
    assert horizon(result)['admitted'] == 3
    assert horizon(result, panel='post_warmup')['admitted'] == 2
    assert horizon(result, panel='post_warmup')['matched'] == 1
    assert result['anchor_observations_after_drain_ignored'] == 1


def test_relative_browser_origin_and_nonconflicting_first_anchor_is_used(tmp_path):
    first = ghost(50_100)
    actual = ghost(50_500, source=BASE+5000, decision=2, eligible=())
    repeated = ghost(51_000, source=BASE+5000, decision=3, eligible=())
    result = analyze_capture(capture(tmp_path, [first, actual, repeated], initial=50_000, end=1_070_000))
    assert horizon(result)['browser_lead_ms']['median'] == '400.0'
    assert result['capture_elapsed_ms'] == '1020000'


def test_equal_handler_time_target_is_already_observed(tmp_path):
    rows = [ghost(100), ghost(100, source=BASE+5000, decision=2, eligible=())]
    result = analyze_capture(capture(tmp_path, rows))
    assert horizon(result)['matched'] == 0
    assert horizon(result)['censor_reasons']['target_already_observed'] == 1


def test_snapshot_diagnostics_do_not_supply_targets(tmp_path):
    snapshot = dict(kind='snapshot', start_ms=100.25, end_ms=120.75, status=200,
                    server_time_ns=str((BASE+5000)*1_000_000), data=ghost(0, source=BASE+5000)['data'])
    result = analyze_capture(capture(tmp_path, [ghost(0), snapshot]))
    assert horizon(result)['censor_reasons']['missing_exact_target'] == 1
    assert result['snapshots']['status_counts'] == {'200': 1}
    assert Decimal(result['snapshots']['full_response_ms']['median']) == Decimal('20.50')


def test_skips_resync_errors_and_connection_counter_reset(tmp_path):
    rows = [ghost(10, skips=2, resync=True), ghost(20, decision=2, skips=5),
        dict(kind='error', browser_ms=30), dict(kind='open', browser_ms=40),
        ghost(50, decision=3, skips=2, resync=True)]
    result = analyze_capture(capture(tmp_path, rows))
    assert result['skipped_updates'] == 7 and result['resync_envelopes'] == 2
    assert result['connects'] == 2 and result['connection_errors'] == 1


@pytest.mark.parametrize('mutation', [
    lambda e: e['ghost'].update(contract_version=3),
    lambda e: e['ghost'].update(runtime_version='ghost-canary-v5'),
    lambda e: e['ghost']['current_twap'].update(window_s=30),
    lambda e: e['ghost']['current_twap'].update(value=100),
    lambda e: e['ghost']['current_twap'].update(value=100.0),
    lambda e: e['ghost']['forecasts'][3].update(price='100.0'),
    lambda e: e['ghost']['forecasts'][3].update(quality='degraded'),
    lambda e: e['ghost']['forecasts'][3].update(target_source_timestamp_ms=BASE+6000),
    lambda e: e['ghost']['forecasts'][0].update(price=E18),
    lambda e: e['ghost']['forecasts'][0]['counts'].update(observed=True),
    lambda e: e['ghost'].update(reasons=['stale_twap']),
    lambda e: e['ghost']['publication_eligibility']['eligible_horizons'].append(5),
    lambda e: e['ghost']['publication_eligibility']['excluded_horizons'].pop('1'),
    lambda e: e['api'].update(remaining_ns='0'),
    lambda e: e['api'].update(skipped_updates=True),
])
def test_invalid_prices_identity_and_membership_fail_closed(tmp_path, mutation):
    with pytest.raises(InvalidCapture):
        analyze_capture(capture(tmp_path, [mutate(ghost(0), mutation)]))


@pytest.mark.parametrize('mode', ['no_end', 'early_end', 'truncated', 'regressed', 'records_after_end', 'duplicate_key'])
def test_incomplete_or_invalid_capture_cannot_produce_results(tmp_path, mode):
    path = capture(tmp_path, [ghost(0)], end=None if mode == 'no_end' else 1_020_000)
    if mode == 'early_end':
        path.write_bytes(path.read_bytes().replace(b'1020000', b'1019999'))
    elif mode == 'truncated':
        path.write_bytes(path.read_bytes()[:-1])
    elif mode == 'regressed':
        path = capture(tmp_path, [ghost(100), ghost(99, decision=2)])
    elif mode == 'records_after_end':
        path.write_bytes(path.read_bytes()+b'{"kind":"open","browser_ms":1020001}\n')
    elif mode == 'duplicate_key':
        path.write_bytes(path.read_bytes().replace(b'"observation_ms":900000', b'"observation_ms":1,"observation_ms":900000'))
    out = tmp_path/'analysis'
    with pytest.raises(InvalidCapture):
        write_results(path, out)
    assert not out.exists()


def test_empty_complete_capture_preserves_six_empty_domains(tmp_path):
    result = analyze_capture(capture(tmp_path))
    assert len(result['all_admissions']) == len(result['post_warmup']) == 6
    assert result['producer_run_count'] == 0
    assert 'probes' not in result and 'load_diagnostics' not in result
    assert all(row['candidate_N'] == 0 and row['absolute_error_usd']['median'] is None for row in result['all_admissions'])


def test_unavailable_envelopes_are_coverage_not_forecast_candidates(tmp_path):
    unavailable = dict(kind='ghost', browser_ms=1, data=json.dumps(dict(
        api=dict(version=1, state='unavailable', reason='expired', remaining_ns='0',
                 instance_id='api-a', generation=1, sequence=1, resync=True, skipped_updates=0), ghost=None)))
    result = analyze_capture(capture(tmp_path, [unavailable]))
    assert result['api_states'] == {'unavailable': 1}
    assert result['api_reasons'] == {'expired': 1}
    assert all(row['candidate_N'] == 0 for row in result['all_admissions'])


def test_real_engine_wire_envelope_is_supported(tmp_path):
    from tests.test_ghost_twap_payload import wire_payload
    record = ghost(1)
    data = json.loads(record['data'])
    data['ghost'] = wire_payload()
    record['data'] = json.dumps(data)
    result = analyze_capture(capture(tmp_path, [record]))
    assert all(row['admitted'] == 1 for row in result['all_admissions'])


def probe(at, usable=(), calibrated=True, **extra):
    return dict(kind='probe', browser_ms=at, calibrated=calibrated,
                usable_horizons=list(usable), state='calibrated' if calibrated else 'uncalibrated', **extra)


def test_probe_actual_grid_does_not_backfill_and_duplicate_bins_are_conservative(tmp_path):
    records = [probe(99.9, (5, 10)), probe(100.1, (5,)), probe(150, (5, 10)),
               probe(350, calibrated=False), probe(65_000, (5,)), probe(65_001, (10,)),
               probe(65_105, (5,)), probe(900_000, (5,))]
    result = analyze_capture(capture(tmp_path, records))['probes']
    full, warm = result['all_admissions'], result['post_warmup']
    assert full['planned_slots'] == 9000 and full['observed_slots'] == 5
    assert full['missing_slots'] == 8995 and full['uncalibrated_observed_slots'] == 1
    assert full['multiple_probe_slots'] == 2
    assert result['records_after_admission_ignored'] == 1
    assert warm['planned_slots'] == 8350 and warm['observed_slots'] == 2
    for panel in (full, warm):
        for row in panel['by_horizon']:
            assert row['usable_slots']+row['not_usable_slots']+row['unknown_slots'] == row['planned_slots']
    full5 = next(row for row in full['by_horizon'] if row['horizon_s'] == 5)
    warm5 = next(row for row in warm['by_horizon'] if row['horizon_s'] == 5)
    assert (full5['usable_slots'], full5['not_usable_slots'], full5['unknown_slots']) == (3, 1, 8996)
    assert (warm5['usable_slots'], warm5['not_usable_slots'], warm5['unknown_slots']) == (1, 1, 8348)
    assert Decimal(full5['usable_fraction_of_planned']) < Decimal('0.001')


@pytest.mark.parametrize('bad_probe', [
    dict(calibrated=1, usable_horizons=[]),
    dict(calibrated=False, usable_horizons=[5]),
    dict(calibrated=True, usable_horizons=[5, 5]),
    dict(calibrated=True, usable_horizons=[True]),
])
def test_probe_false_claims_or_malformed_membership_rejected(tmp_path, bad_probe):
    record = probe(1)
    record.update(bad_probe)
    with pytest.raises(InvalidCapture):
        analyze_capture(capture(tmp_path, [record]))


def test_probe_extra_audit_fields_preserved_in_hashed_source_and_load_kinds_supported(tmp_path):
    record = probe(11, (5,), selected_run_id='run-a', selected_decision_id='1',
        api_instance_id='api-a', api_generation=1, api_sequence=2,
        offset_lower_ns='-123', offset_upper_ns='100', effective_deadline_browser_ns='999999',
        latest_anchor_source_ms=BASE)
    path = capture(tmp_path, [dict(kind='load_start', browser_ms=10), record,
                             dict(kind='load_end', browser_ms=12)])
    result = write_results(path, tmp_path/'analysis')
    assert result['source_sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert result['load_diagnostics'] == [dict(kind='load_start', elapsed_ms='10'), dict(kind='load_end', elapsed_ms='12')]
    assert any('does not independently reconstruct' in line for line in result['limitations'])
    assert 'Browser probe coverage' in (tmp_path/'analysis'/'report.md').read_text(encoding='utf-8')


def test_output_hashes_no_overwrite_and_cli_duration_contract(tmp_path):
    path = capture(tmp_path, [ghost(0)])
    out = tmp_path/'analysis'
    result = write_results(path, out)
    manifest = json.loads((out/'manifest.json').read_text())
    assert manifest['source_sha256'] == result['source_sha256']
    assert manifest['code_sha256'] == hashlib.sha256(Path('research/spot_twap_response/checkpoint_c/analyze_browser.py').read_bytes()).hexdigest()
    for name, expected in manifest['artifacts_sha256'].items():
        assert hashlib.sha256((out/name).read_bytes()).hexdigest() == expected
    before = {file.name: file.read_bytes() for file in out.iterdir()}
    with pytest.raises(InvalidCapture, match='overwrite'):
        write_results(path, out)
    assert before == {file.name: file.read_bytes() for file in out.iterdir()}
    with pytest.raises(SystemExit) as error:
        main(['--input', str(path), '--output', str(tmp_path/'bad'), '--observation-ms', '1'])
    assert error.value.code == 2 and not (tmp_path/'bad').exists()


def test_gzip_capture_has_identical_analysis_and_hashes_exact_compressed_input(tmp_path, capsys):
    plain = capture(tmp_path, [ghost(10), probe(11, (5,)),
                               ghost(1000, source=BASE+5000, decision=2, eligible=())])
    compressed = tmp_path/'capture.jsonl.gz'
    compressed.write_bytes(gzip.compress(plain.read_bytes(), mtime=0))
    original, packed = analyze_capture(plain), analyze_capture(compressed)
    provenance = {'source_path', 'source_sha256', 'source_bytes'}
    assert {k: v for k, v in original.items() if k not in provenance} == {
        k: v for k, v in packed.items() if k not in provenance}
    expected = hashlib.sha256(compressed.read_bytes()).hexdigest()
    assert packed['source_sha256'] == expected != original['source_sha256']
    assert packed['source_bytes'] == compressed.stat().st_size
    output = tmp_path/'compressed-analysis'
    main(['--input', str(compressed), '--output', str(output)])
    assert json.loads(capsys.readouterr().out)['source_sha256'] == expected
    assert json.loads((output/'manifest.json').read_text())['source_sha256'] == expected


@pytest.mark.parametrize('invalid', ['truncated_footer', 'invalid_utf8'])
def test_invalid_gzip_capture_cannot_publish_an_accepted_manifest(tmp_path, invalid):
    plain = capture(tmp_path)
    data = gzip.compress(plain.read_bytes(), mtime=0)
    data = data[:-8] if invalid == 'truncated_footer' else gzip.compress(b'\xff\n', mtime=0)
    path = tmp_path/'invalid.jsonl.gz'
    path.write_bytes(data)
    with pytest.raises(InvalidCapture, match='could not be read completely'):
        write_results(path, tmp_path/'invalid-analysis')
    assert not (tmp_path/'invalid-analysis').exists()
