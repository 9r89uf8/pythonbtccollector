"""Read-only loopback GET benchmark; run through SSH stdin, no remote files."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, localcontext
import http.client
import json
import time

REQUESTS = 50
PERIOD_NS = 100_000_000
PATH = '/markets/current/live'


def quantile(values: list[int], p: Decimal) -> str | None:
    if not values:
        return None
    ordered = sorted(values)
    with localcontext() as context:
        context.prec = 40
        position = Decimal(len(ordered)-1)*p
        left = int(position)
        value = Decimal(ordered[left])
        if position != left:
            value += (position-left)*Decimal(ordered[left+1]-ordered[left])
        return str(value/Decimal(1_000_000))


def main() -> None:
    conn = http.client.HTTPConnection('127.0.0.1',9000,timeout=1)
    started_utc = datetime.now(timezone.utc).isoformat()
    benchmark_started = time.perf_counter_ns()
    rows = []
    errors = []
    last_started = None
    try:
        for index in range(-1,REQUESTS):
            if last_started is not None:
                wait = last_started+PERIOD_NS-time.perf_counter_ns()
                if wait>0:
                    time.sleep(wait/1_000_000_000)
            if time.perf_counter_ns()-benchmark_started>12_000_000_000:
                errors.append('Twelve-second benchmark guard reached')
                break
            started = time.perf_counter_ns()
            last_started = started
            try:
                conn.request('GET',PATH,headers={'Connection':'keep-alive'})
                response = conn.getresponse()
                status = response.status
                protocol = response.version
                body_bytes = len(response.read())
                finished = time.perf_counter_ns()
                local_port = conn.sock.getsockname()[1] if conn.sock else None
                rows.append({'index':index,'warmup':index==-1,'status':status,
                             'duration_ns':finished-started,'response_body_bytes':body_bytes,
                             'http_version':protocol,'local_connection_port':local_port,
                             'start_offset_ns':started-benchmark_started})
            except Exception as exc:
                errors.append(type(exc).__name__+': '+str(exc))
                break
    finally:
        conn.close()
    measured = [row for row in rows if not row['warmup']]
    durations = [row['duration_ns'] for row in measured]
    ports = {row['local_connection_port'] for row in rows}
    result = {
        'status':'accepted' if not errors and len(measured)==REQUESTS else 'failed',
        'started_utc':started_utc,'finished_utc':datetime.now(timezone.utc).isoformat(),
        'scope':'Current existing /markets/current/live, droplet loopback only',
        'method':'stdlib persistent HTTPConnection; duration includes request send through complete response-body read',
        'host':'127.0.0.1','port':9000,'path':PATH,'warmup_requests':1,
        'requested_measured_requests':REQUESTS,'completed_measured_requests':len(measured),
        'requested_rate_per_second':10,'minimum_requested_start_spacing_ns':PERIOD_NS,
        'wall_elapsed_ms':str(Decimal(time.perf_counter_ns()-benchmark_started)/Decimal(1_000_000)),
        'connection_reused_including_warmup':len(ports)==1 and None not in ports,
        'unique_connection_ports':sorted(port for port in ports if port is not None),
        'measured_status_counts':{str(status):sum(row['status']==status for row in measured)
                                  for status in sorted({row['status'] for row in measured})},
        'measured_body_bytes_min':min((row['response_body_bytes'] for row in measured),default=None),
        'measured_body_bytes_max':max((row['response_body_bytes'] for row in measured),default=None),
        'measured_body_bytes_total':sum(row['response_body_bytes'] for row in measured),
        'latency_ms':{'p50':quantile(durations,Decimal('.5')),
                      'p90':quantile(durations,Decimal('.9')),
                      'p99':quantile(durations,Decimal('.99')),
                      'minimum':quantile(durations,Decimal(0)),
                      'maximum':quantile(durations,Decimal(1))},
        'quantile_convention':'Linear interpolation at (n-1)*p; nanosecond durations converted to Decimal milliseconds',
        'observed_minimum_start_spacing_ns':min((b['start_offset_ns']-a['start_offset_ns']
                                                for a,b in zip(measured,measured[1:])),default=None),
        'errors':errors,'requests':rows,
        'limitations':['One short local sample; no ghost endpoint or CPU utilization measured',
                       'No external network path, client RTT, browser render time, or host geography inferred'],
    }
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
