"""Recount raw browser pairs without importing the primary analyzer or audit data."""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

HORIZONS = (1, 2, 3, 5, 10, 30)


def distribution(values):
    values = sorted(values)
    def percentile(percent):
        if not values:
            return None
        index, remainder = divmod((len(values)-1)*percent, 100)
        following = values[min(index+1, len(values)-1)]
        return values[index] + (following-values[index])*Decimal(remainder)/100
    return dict(n=len(values), median=percentile(50), p90=percentile(90),
                p99=percentile(99), maximum=max(values) if values else None)


def recount(path, run_id):
    decisions, anchors = {}, {}
    kinds, errors = Counter(), Counter()
    first = last = None
    total = 0
    with path.open('rb') as handle:
        for raw in handle:
            total += len(raw)
            if total > 128*1024**2 or len(raw) > 262144:
                raise ValueError('capture exceeds frozen byte bounds')
            row = json.loads(raw, parse_float=Decimal)
            kinds[row['kind']] += 1
            if row['kind'] == 'start':
                start = Decimal(row['browser_ms'])
                assert row['observation_ms'] == 3600000
            elif row['kind'] == 'error':
                errors[row['reason']] += 1
            elif row['kind'] == 'ghost':
                body = json.loads(row['data'], parse_float=Decimal)['ghost']
                if body is None or body['run_id'] != run_id:
                    continue
                elapsed = Decimal(row['browser_ms'])-start
                first = elapsed if first is None else first
                last = elapsed
                current = body['current_twap']
                if current is not None:
                    source, price = current['source_timestamp_ms'], Decimal(current['value'])
                    assert price.is_finite() and price > 0
                    point = anchors.setdefault(source, [elapsed, set()])
                    point[0] = min(point[0], elapsed)
                    point[1].add(price)
                key = body['decision_id']
                if key in decisions:
                    assert decisions[key][1] == body, 'same identity changed body'
                else:
                    decisions[key] = elapsed, body
    panels = []
    for horizon in HORIZONS:
        lead, usd, bps = [], [], []
        candidate = admitted = 0
        censored = Counter()
        for elapsed, body in decisions.values():
            if not 0 <= elapsed < 3600000:
                continue
            candidate += 1
            forecast = next(f for f in body['forecasts'] if f['horizon_s'] == horizon)
            if horizon not in body['publication_eligibility']['eligible_horizons']:
                assert forecast['price'] is None
                continue
            admitted += 1
            prediction = Decimal(forecast['price'])
            assert prediction.is_finite() and prediction > 0
            actual = anchors.get(forecast['target_source_timestamp_ms'])
            if actual is None:
                censored['missing_exact_target'] += 1
            elif len(actual[1]) != 1:
                censored['conflicting_target_values'] += 1
            elif actual[0] <= elapsed:
                censored['target_already_observed'] += 1
            else:
                price = next(iter(actual[1]))
                error = abs(prediction-price)
                lead.append(actual[0]-elapsed)
                usd.append(error)
                bps.append(error/price*10000)
        panels.append(dict(horizon_s=horizon, candidate_N=candidate, admitted=admitted,
            matched=len(lead), censored=admitted-len(lead),
            censor_reasons={key:censored[key] for key in ('missing_exact_target',
                'conflicting_target_values','target_already_observed')},
            browser_lead_ms=distribution(lead), absolute_error_usd=distribution(usd),
            absolute_error_bps=distribution(bps), sum_absolute_error_usd=sum(usd,Decimal(0)),
            sum_browser_lead_ms=sum(lead,Decimal(0))))
    return dict(record_kinds=dict(kinds), errors=dict(errors), unique_run_decisions=len(decisions),
        first_run_object_elapsed_ms=first,last_run_object_elapsed_ms=last,panels=panels)


def serial(value):
    if isinstance(value, Decimal):
        return format(value,'f')
    if isinstance(value, dict):
        return {k:serial(v) for k,v in value.items()}
    if isinstance(value, list):
        return [serial(v) for v in value]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('browser','baseline','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--run-id',required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('refuse overwrite')
    with localcontext() as context:
        context.prec = 80
        result = recount(args.browser,args.run_id)
        baseline = json.loads(args.baseline.read_text(encoding='utf-8'))
        for actual, expected in zip(result['panels'],baseline['all_admissions']):
            for key in ('horizon_s','candidate_N','admitted','matched','censored','censor_reasons'):
                assert actual[key] == expected[key], key
            for name in ('browser_lead_ms','absolute_error_usd','absolute_error_bps'):
                assert actual[name]['n'] == expected[name]['n'], name
                for stat in ('median','p90','p99','maximum'):
                    assert actual[name][stat] == (None if expected[name][stat] is None
                                                  else Decimal(expected[name][stat])), (name,stat)
        result.update(status='passed',run_id=args.run_id,compared_horizons=6,
            method='Independent raw JSON decoding, identity deduplication, exact SSE anchor pairing and Decimal80 arithmetic; no audit receipts or primary analyzer imports.',
            source_sha256=hashlib.sha256(args.browser.read_bytes()).hexdigest(),
            baseline_sha256=hashlib.sha256(args.baseline.read_bytes()).hexdigest(),
            code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    with args.output.open('x',encoding='utf-8') as handle:
        json.dump(serial(result),handle,indent=2,sort_keys=True,allow_nan=False)
        handle.write('\n')
    print(json.dumps({'status':'passed','compared_horizons':6}))


if __name__ == '__main__':
    main()
