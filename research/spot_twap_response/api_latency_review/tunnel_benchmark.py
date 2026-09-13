"""Short local-to-droplet HTTP benchmark over a temporary loopback SSH forward."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import time


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def quantile(values: list[int], p: Decimal) -> str | None:
    if not values:
        return None
    ordered=sorted(values)
    with localcontext() as context:
        context.prec=40
        position=Decimal(len(ordered)-1)*p
        left=int(position)
        value=Decimal(ordered[left])
        if position!=left:
            value+=(position-left)*Decimal(ordered[left+1]-ordered[left])
        return str(value/Decimal(1_000_000))


def main() -> None:
    root=Path(__file__).resolve().parent
    paths=[root/name for name in ('tunnel_result.json','tunnel_stderr.txt','tunnel_manifest.json')]
    if any(path.exists() for path in paths):
        raise SystemExit('Refusing to overwrite tunnel benchmark artifacts')
    started_utc=datetime.now(timezone.utc).isoformat()
    started=time.perf_counter_ns()
    deadline=time.monotonic()+26
    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as reserve:
        reserve.bind(('127.0.0.1',0))
        port=reserve.getsockname()[1]
    args=['ssh','-N','-o','BatchMode=yes','-o','ExitOnForwardFailure=yes',
          '-o','ConnectTimeout=10','-L',f'127.0.0.1:{port}:127.0.0.1:9000',
          'root@152.42.247.86']
    process=None
    conn=None
    rows=[]
    errors=[]
    ready=False
    ready_offset_ms=None
    forced_kill=False
    returncode=None
    stderr=b''
    cleanup_started=None
    cleanup_finished=None
    try:
        flags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0
        process=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                                 stderr=subprocess.PIPE,creationflags=flags)
        readiness_deadline=min(deadline-3,time.monotonic()+12)
        while time.monotonic()<readiness_deadline:
            if process.poll() is not None:
                raise RuntimeError(f'SSH tunnel exited before readiness: {process.returncode}')
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.2):
                    ready=True
                    ready_offset_ms=str(Decimal(time.perf_counter_ns()-started)/Decimal(1_000_000))
                    break
            except OSError:
                time.sleep(.05)
        if not ready:
            raise TimeoutError('Temporary SSH forward did not become ready within12seconds')
        conn=http.client.HTTPConnection('127.0.0.1',port,timeout=3)
        for index in range(-1,10):
            remaining=deadline-time.monotonic()
            if remaining<=1:
                raise TimeoutError('Benchmark deadline reached before all requests')
            conn.timeout=min(3,remaining)
            if conn.sock is not None:
                conn.sock.settimeout(conn.timeout)
            request_started=time.perf_counter_ns()
            conn.request('GET','/markets/current/live',headers={'Connection':'keep-alive'})
            response=conn.getresponse()
            status=response.status
            protocol=response.version
            size=len(response.read())
            finished=time.perf_counter_ns()
            local_port=conn.sock.getsockname()[1] if conn.sock else None
            rows.append({'index':index,'warmup':index==-1,'status':status,
                         'duration_ns':finished-request_started,'response_body_bytes':size,
                         'http_version':protocol,'local_http_connection_port':local_port,
                         'start_offset_ns':request_started-started})
    except Exception as exc:
        errors.append(type(exc).__name__+': '+str(exc))
    finally:
        cleanup_started=datetime.now(timezone.utc).isoformat()
        if conn is not None:
            conn.close()
        if process is not None:
            if process.poll() is None:
                process.terminate()
            try:
                _,stderr=process.communicate(timeout=1.5)
            except subprocess.TimeoutExpired:
                forced_kill=True
                process.kill()
                _,stderr=process.communicate(timeout=1.5)
            returncode=process.returncode
        cleanup_finished=datetime.now(timezone.utc).isoformat()
    process_exited=process is not None and process.poll() is not None
    listener_closed=False
    try:
        with socket.create_connection(('127.0.0.1',port),timeout=.2):
            listener_closed=False
    except OSError:
        listener_closed=True
    if not process_exited or not listener_closed:
        errors.append('Owned tunnel process/listener cleanup verification failed')
    measured=[row for row in rows if not row['warmup']]
    if len(measured)!=10:
        errors.append('Expected exactly10measured requests after one warmup')
    ports={row['local_http_connection_port'] for row in rows}
    durations=[row['duration_ns'] for row in measured]
    result={
        'status':'accepted' if not errors else 'failed','started_utc':started_utc,
        'finished_utc':datetime.now(timezone.utc).isoformat(),
        'scope':'Current endpoint from this Windows computer through its temporary SSH tunnel',
        'endpoint':'/markets/current/live','local_bind_host':'127.0.0.1','local_forward_port':port,
        'remote_forward_host':'127.0.0.1','remote_forward_port':9000,
        'ssh_process_id':process.pid if process else None,'ssh_arguments':args,
        'hidden_process_requested':os.name=='nt','tunnel_ready':ready,'tunnel_ready_offset_ms':ready_offset_ms,
        'warmup_requests':1,'measured_requests':len(measured),'request_schedule':'Sequential, no fixed request-rate target',
        'duration_definition':'Local perf_counter_ns from HTTP request send through complete response-body read',
        'connection_reused_including_warmup':len(ports)==1 and None not in ports,
        'measured_status_counts':{str(status):sum(row['status']==status for row in measured)
                                  for status in sorted({row['status'] for row in measured})},
        'response_body_bytes_min':min((row['response_body_bytes'] for row in measured),default=None),
        'response_body_bytes_max':max((row['response_body_bytes'] for row in measured),default=None),
        'latency_ms':{'p50':quantile(durations,Decimal('.5')),'p90':quantile(durations,Decimal('.9')),
                      'minimum':quantile(durations,Decimal(0)),'maximum':quantile(durations,Decimal(1))},
        'quantile_convention':'Linear interpolation at (n-1)*p, Decimal milliseconds from integer nanoseconds',
        'cleanup_started_utc':cleanup_started,'cleanup_finished_utc':cleanup_finished,
        'owned_ssh_process_exited':process_exited,'owned_ssh_exit_code':returncode,
        'forced_kill_used':forced_kill,'local_listener_closed':listener_closed,
        'total_wall_elapsed_ms':str(Decimal(time.perf_counter_ns()-started)/Decimal(1_000_000)),
        'errors':errors,'requests':rows,
        'limitations':['Observed HTTP response latency on this specific tunnel path, not a direct network RTT or one-way measurement',
                       'No ghost endpoint, CPU usage, browser rendering or geography measured'],
    }
    (root/'tunnel_result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
    (root/'tunnel_stderr.txt').write_bytes(stderr)
    manifest={'status':result['status'],'read_only':True,'production_changes':False,
              'temporary_tunnel_cleaned_up':process_exited and listener_closed,
              'files_sha256':{name:digest(root/name) for name in ('tunnel_benchmark.py','tunnel_result.json','tunnel_stderr.txt')}}
    (root/'tunnel_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({key:result[key] for key in ('status','started_utc','measured_requests',
                      'connection_reused_including_warmup','measured_status_counts','response_body_bytes_min',
                      'response_body_bytes_max','latency_ms','owned_ssh_process_exited',
                      'local_listener_closed','total_wall_elapsed_ms','errors')},indent=2))
    if errors:
        raise SystemExit(1)


if __name__=='__main__':
    main()
