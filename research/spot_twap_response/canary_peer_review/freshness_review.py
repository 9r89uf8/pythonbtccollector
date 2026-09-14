"""Offline review of freshness gates and bounded cache-expiry evidence.

The 5s-source/3s-receipt counterfactual changes eligibility on saved decisions
only. It neither issues a forecast nor claims live coverage/accuracy under a
new policy. No production imports or I/O.
"""
from __future__ import annotations
import argparse
from collections import Counter
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from pathlib import Path

D = Decimal
START_MS = 1789346720328
END_MS = START_MS + 3600000
NS_MS = 1000000

def stats(values):
    if not values: return {'n':0}
    v=sorted(map(D,values))
    def q(p):
        x=(len(v)-1)*D(p);i=int(x)
        return v[i]+(x-i)*(v[min(i+1,len(v)-1)]-v[i])
    return dict(n=len(v),min=v[0],median=q('.5'),p90=q('.9'),p99=q('.99'),max=v[-1],total=sum(v,D(0)))

def assess(f,source_ms,receipt_ms):
    dw,dm=int(f['decision_wall_ns']),int(f['decision_monotonic_ns'])
    gates={}
    for feed in ('spot','twap'):
        e=f['current_'+feed]
        if not e:
            gates[feed]=dict(ok=False,missing=True)
            continue
        ages=[dw-e['source_timestamp_ms']*NS_MS,dw-int(e['received_wall_ns']),dm-int(e['received_monotonic_ns'])]
        invalid=min(ages)<0 or e['source_timestamp_ms']*NS_MS>int(e['received_wall_ns'])
        gates[feed]=dict(ok=not invalid and ages[0]<=source_ms*NS_MS and max(ages[1:])<=receipt_ms*NS_MS,
                        source_age_ns=ages[0],wall_age_ns=ages[1],mono_age_ns=ages[2],invalid=invalid,
                        source_over_3=ages[0]>3000*NS_MS,receipt_over_3=max(ages[1:])>3000*NS_MS)
    availability={}
    for forecast in f['forecasts']:
        issues=set(forecast['reasons'])-{'stale_spot','stale_twap','missing_slots'}
        for feed in ('spot','twap'):
            if not gates[feed]['ok']:issues.add('bad_current_'+feed)
        index=forecast['slot_start_index']
        selected=[] if index is None else f['slots'][index:index+60]
        for slot in selected:
            if slot['value'] is not None:continue
            # Historical selection/carry do not change. A future slot withheld
            # solely for current-spot staleness can use that saved current spot.
            if slot['slot_timestamp_ms']*NS_MS>dw and gates['spot']['ok']:continue
            issues.add('missing_slots')
        if len(selected)!=60:issues.add('missing_slots')
        availability[forecast['horizon_s']]=not issues
    return gates,availability

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('export',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    digest=sha256();decisions=[];pubs=[];gate_counts=Counter();gate_ages={k:[] for k in ('spot','twap')}
    event_delivery={};original_check_mismatches=0
    for raw in args.export.open('rb'):
        digest.update(raw);r=json.loads(raw);f=json.loads(r['frozen_json']);s=json.loads(r['state_json'])
        gates,original=assess(f,3000,3000)
        new_gates,candidate=assess(f,5000,3000)
        original_check_mismatches+=sum(original[x['horizon_s']]!=(x['price'] is not None) for x in f['forecasts'])
        after60=r['created_ms']>=START_MS+60000
        if after60:
            for feed,g in gates.items():
                if not g.get('missing'):
                    gate_ages[feed].append(D(g['source_age_ns'])/NS_MS)
                    if not g['ok']:
                        cause='both' if g['source_over_3'] and g['receipt_over_3'] else 'source_only' if g['source_over_3'] else 'receipt_only' if g['receipt_over_3'] else 'invalid'
                        gate_counts[feed+'_'+cause]+=1
        for e in (f['current_spot'],f['current_twap']):
            if e:event_delivery[e['sequence']]=(e['feed'],D(int(e['received_wall_ns'])-e['source_timestamp_ms']*NS_MS)/NS_MS)
        decisions.append(dict(created_ms=r['created_ms'],any_unavailable=not all(original.values()),
                              any_unavailable_candidate=not all(candidate.values()),
                              all_available=all(original.values()),candidate_all_available=all(candidate.values()),
                              source_only_failure=any(g.get('source_over_3') for g in gates.values()) and not any(g.get('receipt_over_3') or g.get('invalid') or g.get('missing') for g in gates.values()),
                              per_horizon=original,candidate_per_horizon=candidate,
                              first_all_available_ms=r['created_ms'] if all(original.values()) else None))
        pub=s['publication']
        if pub['status']=='acknowledged':
            pm,pw=int(pub['attempt_monotonic_ns']),int(pub['attempt_wall_ns'])
            am,aw=int(pub['ack_monotonic_ns']),int(pub['ack_wall_ns'])
            dm,dw=int(f['decision_monotonic_ns']),int(f['decision_wall_ns'])
            valid=int(f['valid_until_wall_ns'])
            ttl_ms=min((valid-pw)//NS_MS,(valid-dw-(pm-dm))//NS_MS)
            assert ttl_ms>0 and pm<=am
            pubs.append(dict(decision_id=r['decision_id'],attempt=pm,ack=am,attempt_wall=pw,ack_wall=aw,
                             ttl_ns=ttl_ms*NS_MS,all_available=all(original.values()),
                             valid_remaining_ns=valid-aw))
    assert original_check_mismatches==0
    decisions.sort(key=lambda x:x['created_ms']);pubs.sort(key=lambda x:x['attempt'])
    first_all=min(d['created_ms'] for d in decisions if d['all_available'])
    populations={}
    for label,cut in [('after_60_seconds',START_MS+60000),('after_65_seconds',START_MS+65000),('after_first_all_horizons_available',first_all)]:
        pop=[d for d in decisions if d['created_ms']>=cut]
        n=len(pop);bad=sum(d['any_unavailable'] for d in pop);remaining=sum(d['any_unavailable_candidate'] for d in pop)
        populations[label]=dict(cutoff_ms=cut,decisions=n,any_unavailable=bad,percent=D(bad)*100/n,
                                source_only_failure=sum(d['source_only_failure'] for d in pop),
                                candidate_any_unavailable=remaining,candidate_percent=D(remaining)*100/n,
                                candidate_recovers_all_horizons=sum(d['any_unavailable'] and not d['any_unavailable_candidate'] for d in pop),
                                per_horizon={h:dict(original_available=sum(d['per_horizon'][h] for d in pop),candidate_available=sum(d['candidate_per_horizon'][h] for d in pop)) for h in (1,2,3,5,10,30)})
    gaps=[]
    for a,b in zip(pubs,pubs[1:]):
        if a['attempt_wall']<(START_MS+65000)*NS_MS or b['ack_wall']>END_MS*NS_MS:continue
        # SET happens somewhere within each EVAL attempt→ACK interval. Expiry
        # therefore lies between attempt+TTL and ACK+TTL. These are bounds,
        # not measurements of actual Redis execution or browser receipt.
        # Allow one millisecond on either side for Redis millisecond clock
        # granularity. Assumes no separate Redis host-clock discontinuity.
        lower=max(0,b['attempt']-(a['ack']+a['ttl_ns']+NS_MS))
        upper=max(0,b['ack']-(a['attempt']+a['ttl_ns']-NS_MS))
        if upper:
            gaps.append(dict(previous_decision=a['decision_id'],next_decision=b['decision_id'],lower_ms=D(lower)/NS_MS,upper_ms=D(upper)/NS_MS))
    result=dict(export_sha256=digest.hexdigest(),rows=len(decisions),original_policy_availability_mismatches=original_check_mismatches,
                first_all_horizons_available_ms=first_all,populations=populations,
                failure_gate_counts_after60=gate_counts,source_age_at_decision_ms={k:stats(v) for k,v in gate_ages.items()},
                delivery_lag_ms_unique_current_inputs={feed:stats([v for f,v in event_delivery.values() if f==feed]) for feed in ('spot','twap')},
                expiry_gaps_after65=dict(guaranteed_count=sum(g['lower_ms']>0 for g in gaps),possible_count=len(gaps),
                                        guaranteed_gap_ms=stats([g['lower_ms'] for g in gaps if g['lower_ms']>0]),
                                        possible_gap_ms=stats([g['upper_ms'] for g in gaps]),
                                        total_absence_lower_seconds=sum((g['lower_ms'] for g in gaps),D(0))/1000,
                                        total_absence_upper_seconds=sum((g['upper_ms'] for g in gaps),D(0))/1000),
                counterfactual_policy=dict(source_max_age_ms=5000,receipt_wall_and_monotonic_max_age_ms=3000,historical_carry_max_ms=10000),
                notes=['Same-decision eligibility diagnostic only; new policy changes future expiry-triggered decisions and publications.',
                       'Newly eligible forecasts have not been graded here; retained forecast slots and current inputs do not establish complete first-arrival history.',
                       'Original 3s policy is exactly reproduced for all 43,752 horizon decisions before considering the alternative.',
                       'Expiry absence bounds use attempt/ACK ordering and actual TTL formula, with a 1ms margin for Redis clock granularity and assuming no host-clock discontinuity; no claim about actual UI flicker.',
                       'Only gaps between acknowledged writes wholly after 65s and before deadline are included; startup/shutdown tails excluded.'])
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_bytes((json.dumps(result,default=lambda v:format(v,'f'),indent=2)+'\n').encode())
    print(json.dumps(result,default=lambda v:format(v,'f'),indent=2))

if __name__=='__main__':
    with localcontext() as ctx:
        ctx.prec=80
        main()
