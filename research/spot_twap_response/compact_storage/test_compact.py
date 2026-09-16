"""Focused compaction, lineage, lifecycle and Decimal-only scoring tests."""
from copy import deepcopy
from decimal import Decimal, localcontext
from hashlib import sha256
import json
import os
from pathlib import Path

import pytest

from research.spot_twap_response.compact_storage import compact

WALL = 1700000000000000000
MONO = 1000000000000


def event(value='100.123456789012345678', *, source_ms=None, wall=WALL-1000000000,
          mono=MONO-1000000000, feed='twap'):
    return dict(feed=feed, value=value, source_timestamp_ms=source_ms or WALL//1000000-2000,
                received_wall_ns=str(wall),received_monotonic_ns=str(mono),
                sequence=1,event_id='event',window_s=60 if feed=='twap' else None)


def fixture():
    forecasts=[]; targets={}
    for h in compact.HORIZONS:
        stamp=WALL//1000000+h*1000
        forecasts.append(dict(horizon_s=h,target_source_timestamp_ms=stamp,
            price='100.123456789012345679',quality='healthy',reasons=[],
            counts=dict(observed=60-h,carried=0,pending=0,future=h,missing=0),
            max_interior_carry_ms=0,estimated_arrival_wall_ns=str(WALL+h*compact.NS),
            estimated_remaining_ns=str(h*compact.NS)))
        targets[str(h)]=dict(horizon=str(h),target_source_timestamp_ms=stamp,status='matched',
            first_event=event(source_ms=stamp,wall=WALL+(h+1)*compact.NS,mono=MONO+(h+1)*compact.NS),
            conflicted=False,clock_anomaly=False,error='0.000000000000000001',
            persistence_error='0.000000000000000000',eta_error_ns=str(compact.NS),
            confirmed_redis_lead_ns=str((h+1)*compact.NS-3000000))
    frozen=dict(run_id='run',decision_id='1',decision_wall_ns=str(WALL),decision_monotonic_ns=str(MONO),
        runtime_version='ghost-canary-v6',contract_version=4,model_version='model',
        policy={'enabled':True,'source_max_age_ms':5000,'receipt_max_age_ms':3000,
                'max_carry_ms':10000,'history_ms':120000,'max_events':1024,'spot_reconnect_max_gap_ms':10000},
        publication_eligibility_policy='per-horizon-unreceived-v1',runtime_policy={'canary_start_ms':WALL//1000000},
        current_spot=event(feed='spot'),current_twap=event(),included_sequence=1,
        valid_until_wall_ns=str(WALL+2000000000),reasons=[],forecasts=forecasts,
        slots=['not retained'],slot_inputs=['not retained'])
    wire=deepcopy(frozen)
    wire.update(publication_state='attempted',publication_eligibility=dict(version=1,
        eligible_horizons=list(compact.HORIZONS),excluded_horizons={},
        checked_wall_ns=str(WALL+2000000),checked_monotonic_ns=str(MONO+2000000)))
    pub=dict(status='acknowledged',payload_json=json.dumps(wire,indent=2),
        intent_wall_ns=str(WALL+1000000),intent_monotonic_ns=str(MONO+1000000),
        attempt_wall_ns=str(WALL+2000000),attempt_monotonic_ns=str(MONO+2000000),
        ack_wall_ns=str(WALL+3000000),ack_monotonic_ns=str(MONO+3000000))
    state=dict(computation_completed_wall_ns=WALL+500000,computation_completed_monotonic_ns=MONO+500000,
               publication=pub,targets=targets)
    return seal(frozen,state)


def seal(frozen,state,terminal=True,version=1):
    f=json.dumps(frozen);s=json.dumps(state)
    return dict(run_id=frozen['run_id'],decision_id=frozen['decision_id'],decision_wall_ns=WALL,
        created_ms=WALL//1000000,frozen_json=f,state_json=s,version=version,terminal=terminal,
        frozen_sha256=sha256(f.encode()).hexdigest(),state_sha256=sha256(s.encode()).hexdigest())


def change(row, callback):
    f,s=json.loads(row['frozen_json']),json.loads(row['state_json'])
    callback(f,s)
    return seal(f,s)


def encode(row):
    return compact.compact_record(row,finalized_as_of_wall_ns=WALL+120*compact.NS)


def test_exact_e18_scoring_hash_lineage_and_no_slot_snapshot_under_low_precision():
    row=fixture()
    with localcontext() as context:
        context.prec=6
        result=encode(row)
        pair=compact.scoring_pair(result,5)
    assert pair[0]==Decimal('1e-18') and pair[1]==0
    assert result['decision']['attempted_payload_sha256']==sha256(json.loads(row['state_json'])['publication']['payload_json'].encode()).hexdigest()
    assert result['decision']['frozen_sha256']==row['frozen_sha256']
    assert result['decision']['state_sha256']==row['state_sha256']
    assert result['decision']['record_version']==1
    assert 'slots' not in result['decision'] and 'slot_inputs' not in result['decision']
    assert b'not retained' not in compact.canonical_bytes(result)
    assert compact.verify_compact(compact.decode(compact.canonical_bytes(result)))==result
    assert encode(row)==result
    tampered=deepcopy(result);tampered['horizons'][0]['forecast_price']='1.000000000000000000'
    with pytest.raises(ValueError,match='hash'):
        compact.verify_compact(tampered)


def test_exact_original_payload_hash_is_not_reserialized_hash():
    row=fixture();p=json.loads(row['state_json'])['publication']['payload_json']
    assert sha256(p.encode()).hexdigest()!=sha256(compact.canonical_bytes(json.loads(p))).hexdigest()
    assert encode(row)['decision']['attempted_payload_sha256']==sha256(p.encode()).hexdigest()


@pytest.mark.parametrize('delta',[1,1000000000])
def test_no_compaction_before_120s(delta):
    with pytest.raises(ValueError,match='120'):
        compact.compact_record(fixture(),finalized_as_of_wall_ns=WALL+120*compact.NS-delta)


def test_nonterminal_fails_even_after120s():
    row=fixture();row['terminal']=False
    with pytest.raises(ValueError,match='nonterminal'):
        encode(row)


def test_unpublished_null_health_and_recovery_tail_remain_records():
    def null(f,s):
        f['reasons']=['missing_spot'];f['current_spot']=None
        s['publication']={'status':'expired_or_target_received'}
        for forecast in f['forecasts']:
            forecast.update(price=None,quality='unavailable',reasons=['missing_spot'])
            target=s['targets'][str(forecast['horizon_s'])]
            target.update(status='not_forecast',first_event=None,error=None,persistence_error=None,
                          confirmed_redis_lead_ns=None)
    result=encode(change(fixture(),null))
    assert result['decision']['publication_status']=='expired_or_target_received'
    assert result['decision']['attempted_payload_sha256'] is None
    assert all(not h['attempted_eligible'] and h['forecast_price'] is None for h in result['horizons'])
    assert compact.scoring_pair(result,5) is None
    def tail(f,s):
        t=s['targets']['30'];t.update(status='restart_unmatched',first_event=None,error=None,persistence_error=None,
                                     confirmed_redis_lead_ns=None)
        s['shutdown']='forced_terminal'
    result=encode(change(fixture(),tail))
    assert result['horizons'][-1]['target_status']=='restart_unmatched'
    assert result['decision']['shutdown']=='forced_terminal'
    assert compact.scoring_pair(result,30) is None


def test_calculated_but_withheld_cannot_receive_published_credit():
    def withheld(f,s):
        p=s['publication'];w=json.loads(p['payload_json'])
        w['publication_eligibility']['eligible_horizons'].remove(1)
        w['publication_eligibility']['excluded_horizons']['1']=['target_received_before_publication']
        w['forecasts'][0].update(price=None,quality='unavailable',reasons=['target_received_before_publication'])
        p['payload_json']=json.dumps(w);s['targets']['1']['confirmed_redis_lead_ns']=None
    result=encode(change(fixture(),withheld))
    assert result['horizons'][0]['forecast_price'] is not None
    assert result['horizons'][0]['attempted_eligible'] is False
    assert compact.scoring_pair(result,1) is None


@pytest.mark.parametrize('case',['late_ack','conflict','causality'])
def test_early_scoring_does_not_invent_credit(case):
    def mutate(f,s):
        if case=='late_ack':
            s['publication']['ack_monotonic_ns']=str(MONO+2*compact.NS)
            s['publication']['ack_wall_ns']=str(WALL+2*compact.NS)
            for t in s['targets'].values():
                difference=int(t['first_event']['received_monotonic_ns'])-(MONO+2*compact.NS)
                t['confirmed_redis_lead_ns']=str(difference) if difference>0 else None
        elif case=='conflict':
            t=s['targets']['1'];t['conflicted']=True;t['confirmed_redis_lead_ns']=None
            t['first_conflicting_event']=deepcopy(t['first_event']);t['first_conflicting_event']['value']='101'
        else:
            s['causality_invalid']=True
            for t in s['targets'].values():t['confirmed_redis_lead_ns']=None
    result=encode(change(fixture(),mutate))
    assert compact.scoring_pair(result,1) is None
    assert (compact.scoring_pair(result,1,require_early=False) is not None)==(case=='late_ack')


def test_financial_and_original_hash_validation():
    with pytest.raises(ValueError):compact.money(0.1)
    with pytest.raises(ValueError):compact.money('1.0000000000000000001')
    assert compact.money('1')=='1.000000000000000000'
    assert compact.money('-0.000000000000000000')=='0.000000000000000000'
    with pytest.raises(ValueError):compact.canonical_bytes({'price':0.1})
    row=fixture();row['state_json']+=' '
    with pytest.raises(ValueError,match='hash'):encode(row)


def test_full_export_verified_before_yield_or_output_and_no_overwrite(tmp_path):
    row=fixture();raw=compact.canonical_bytes(row)+b'\n';source=tmp_path/'audit.jsonl';source.write_bytes(raw)
    args=dict(expected_sha256=sha256(raw).hexdigest(),expected_rows=1,run_id='run',
              finalized_as_of_wall_ns=WALL+120*compact.NS,expected_selected_rows=1)
    assert list(compact.iter_verified_export(source,**args))==[encode(row)]
    output=tmp_path/'compact.jsonl'
    bad=dict(args,expected_sha256='0'*64)
    with pytest.raises(ValueError,match='verification'):
        compact.write_export(source,output,**bad)
    assert not output.exists()
    result=compact.write_export(source,output,**args)
    assert result['decisions']==1 and result['horizon_records']==6
    assert result['logical_bytes']==output.stat().st_size
    with pytest.raises(ValueError,match='overwrite'):compact.write_export(source,output,**args)


def test_truncated_duplicate_and_missing_selected_export_rejected(tmp_path):
    raw=compact.canonical_bytes(fixture())+b'\n';p=tmp_path/'input'
    for content,selected,match in [(raw[:-1],1,'truncated'),(raw+raw,2,'Duplicate'),(raw,2,'Selected')]:
        p.write_bytes(content)
        with pytest.raises(ValueError,match=match):
            list(compact.iter_verified_export(p,expected_sha256=sha256(content).hexdigest(),
                expected_rows=2 if content==raw+raw else 1,expected_selected_rows=selected,run_id='run',
                finalized_as_of_wall_ns=WALL+120*compact.NS))


@pytest.mark.skipif(os.environ.get('GHOST_COMPACT_CANARY_VERIFY') != '1',
                    reason='opt-in immutable 1.08GB local canary verification')
def test_all_7082_compact_records_preserve_every_reference_scoring_cohort():
    from collections import defaultdict
    from research.spot_twap_response.retention_plan_review.metrics_check import summary
    root=Path(__file__).resolve().parents[3]
    reference=json.loads((root/'results/spot_twap_response/2026-09-16-retention-plan-review/metrics.json').read_text())
    source=root/'dist/ghost-reliability-canary/audit.jsonl'
    require_start=1789512998000*1000000
    stop=1789516598428617010
    pairs=defaultdict(list);statuses={};count=0
    records=compact.iter_verified_export(source,expected_sha256=reference['source']['sha256'],
        expected_rows=23411,run_id=reference['run_id'],expected_selected_rows=7082,
        finalized_as_of_wall_ns=stop+120*compact.NS)
    with localcontext() as context:
        context.prec=80
        for record in records:
            count+=1;decision=record['decision'];wall=decision['decision_wall_ns']
            statuses[decision['publication_status']]=statuses.get(decision['publication_status'],0)+1
            panels=['all_decisions']
            if wall+120*compact.NS<=stop:
                panels.append('complete_120s_window')
                if wall>=require_start+65*compact.NS:panels.append('after_65s_complete_120s_window')
            quarter=min(3,(wall-require_start)//(900*compact.NS))
            for h in compact.HORIZONS:
                for early,name in [(False,'acknowledged_matched_valid'),(True,'confirmed_early_matched_valid')]:
                    pair=compact.scoring_pair(record,h,require_early=early)
                    if pair is not None:
                        for panel in panels:
                            pairs[panel,name,h,'all'].append(pair)
                            pairs[panel,name,h,quarter].append(pair)
        assert count==7082 and statuses==reference['publication_statuses']
        for panel,cohorts in reference['panels'].items():
            for name,horizons in cohorts.items():
                for expected in horizons:
                    h=expected['horizon_s']
                    actual=summary(pairs[panel,name,h,'all'])
                    assert actual=={k:v for k,v in expected.items() if k not in ('horizon_s','quarter_hours')}
                    for quarter in expected['quarter_hours']:
                        assert summary(pairs[panel,name,h,quarter['quarter']-1])=={
                            k:v for k,v in quarter.items() if k not in ('quarter','elapsed_start_s','elapsed_end_s')}
