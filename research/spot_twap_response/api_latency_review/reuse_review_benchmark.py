"""Bounded connection-reuse and response-stage diagnostic, without payload logs."""
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
    values=sorted(values)
    with localcontext() as context:
        context.prec=40
        position=Decimal(len(values)-1)*p
        left=int(position)
        result=Decimal(values[left])
        if position!=left:
            result+=(position-left)*Decimal(values[left+1]-values[left])
        return str(result/Decimal(1_000_000))


def distribution(values: list[int]) -> dict:
    return {name:quantile(values,Decimal(p)) for name,p in (
        ('p50','.5'),('p90','.9'),('minimum','0'),('maximum','1'))}


def socket_info(sock) -> dict | None:
    if sock is None:
        return None
    return {'object_id':id(sock),'fileno':sock.fileno(),
            'local_address':list(sock.getsockname()),'peer_address':list(sock.getpeername()),
            'tcp_nodelay':sock.getsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY)}


class CountedConnection(http.client.HTTPConnection):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.connect_events=[]

    def connect(self):
        started=time.perf_counter_ns()
        super().connect()
        self.connect_events.append({'connect_call':len(self.connect_events)+1,
                                    'connect_duration_ns':time.perf_counter_ns()-started,
                                    'socket':socket_info(self.sock)})


def main() -> None:
    root=Path(__file__).resolve().parent
    names=('reuse_review_result.json','reuse_review_stderr.txt','reuse_review_manifest.json')
    if any((root/name).exists() for name in names):
        raise SystemExit('Refusing to overwrite connection-reuse review')
    started_utc=datetime.now(timezone.utc).isoformat()
    started=time.perf_counter_ns(); deadline=time.monotonic()+35
    tcp_rows=[]; http_rows=[]; errors=[]
    conn=None; process=None; stderr=b''; port=None
    readiness_probes=0; ready=False; forced_kill=False
    try:
        for index in range(5):
            with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as probe:
                probe.settimeout(2)
                begin=time.perf_counter_ns()
                try:
                    probe.connect(('152.42.247.86',22))
                    tcp_rows.append({'index':index,'status':'connected',
                                     'connect_duration_ns':time.perf_counter_ns()-begin,
                                     'local_address':list(probe.getsockname())})
                except OSError as exc:
                    tcp_rows.append({'index':index,'status':'failed',
                                     'connect_duration_ns':time.perf_counter_ns()-begin,
                                     'error':type(exc).__name__+': '+str(exc)})
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as reserve:
            reserve.bind(('127.0.0.1',0)); port=reserve.getsockname()[1]
        args=['ssh','-N','-o','BatchMode=yes','-o','ExitOnForwardFailure=yes','-o','ConnectTimeout=10',
              '-L',f'127.0.0.1:{port}:127.0.0.1:9000','root@152.42.247.86']
        process=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                                 stderr=subprocess.PIPE,
                                 creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
        readiness_deadline=min(deadline-3,time.monotonic()+12)
        while time.monotonic()<readiness_deadline:
            if process.poll() is not None:
                raise RuntimeError(f'SSH exited before readiness: {process.returncode}')
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.2):
                    readiness_probes+=1; ready=True; break
            except OSError:
                time.sleep(.05)
        if not ready:
            raise TimeoutError('SSH forward readiness deadline exceeded')
        conn=CountedConnection('127.0.0.1',port,timeout=3)
        for index in range(-1,10):
            remaining=deadline-time.monotonic()
            if remaining<=1:
                raise TimeoutError('Diagnostic deadline reached')
            conn.timeout=min(3,remaining/3)
            if conn.sock is not None:
                conn.sock.settimeout(conn.timeout)
            before=socket_info(conn.sock)
            calls_before=len(conn.connect_events)
            begin=time.perf_counter_ns()
            conn.request('GET','/markets/current/live',headers={'Connection':'keep-alive'})
            sent=time.perf_counter_ns()
            after_send=socket_info(conn.sock)
            response=conn.getresponse()
            headers_received=time.perf_counter_ns()
            response_headers={name:response.getheader(name) for name in ('Connection','Content-Length','Transfer-Encoding')}
            will_close=response.will_close
            status=response.status; version=response.version
            size=len(response.read())
            body_complete=time.perf_counter_ns()
            after_body=socket_info(conn.sock)
            http_rows.append({'index':index,'warmup':index==-1,'status':status,'http_version':version,
                              'will_close':will_close,'response_headers':response_headers,
                              'response_body_bytes':size,'connect_calls_before':calls_before,
                              'connect_calls_after':len(conn.connect_events),
                              'socket_before_request':before,'socket_after_send':after_send,
                              'socket_after_body':after_body,
                              'request_send_ns':sent-begin,'request_to_headers_ns':headers_received-begin,
                              'headers_to_full_body_ns':body_complete-headers_received,
                              'full_response_ns':body_complete-begin})
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
                forced_kill=True; process.kill(); _,stderr=process.communicate(timeout=1.5)
        cleanup_finished=datetime.now(timezone.utc).isoformat()
    exited=process is not None and process.poll() is not None
    listener_closed=False
    if port is not None:
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.2):
                listener_closed=False
        except OSError:
            listener_closed=True
    if not exited or not listener_closed:
        errors.append('Tunnel cleanup was not verified')
    measured=[row for row in http_rows if not row['warmup']]
    if len(measured)!=10:
        errors.append('Did not complete ten measured HTTP requests')
    sockets={row['socket_after_body']['object_id'] for row in http_rows if row['socket_after_body']}
    local_ports={row['socket_after_body']['local_address'][1] for row in http_rows if row['socket_after_body']}
    result={
        'status':'accepted' if not errors else 'failed','started_utc':started_utc,
        'finished_utc':datetime.now(timezone.utc).isoformat(),
        'scope':'Five TCP connection-time probes to droplet SSH port22; one warmup and ten sequential HTTP GETs through one temporary loopback SSH forward',
        'endpoint':'/markets/current/live','tcp_connect_probe_count':len(tcp_rows),
        'tcp_connect_time_ms':distribution([r['connect_duration_ns'] for r in tcp_rows if r['status']=='connected']),
        'tcp_connect_probes':tcp_rows,'http_connect_invocations':len(conn.connect_events) if conn else 0,
        'http_connect_events':conn.connect_events if conn else [],
        'http_socket_object_count':len(sockets),'http_local_port_count':len(local_ports),
        'http_connection_reused':bool(http_rows) and len(conn.connect_events)==1 and len(sockets)==1 and len(local_ports)==1,
        'measured_http_requests':len(measured),'measured_status_counts':{str(s):sum(r['status']==s for r in measured) for s in sorted({r['status'] for r in measured})},
        'response_stage_ms':{name:distribution([r[field] for r in measured]) for name,field in (
            ('request_send','request_send_ns'),('request_to_headers','request_to_headers_ns'),
            ('headers_to_full_body','headers_to_full_body_ns'),('full_response','full_response_ns'))},
        'http_requests':http_rows,'local_forward_port':port,'readiness_tcp_probe_connections':readiness_probes,
        'readiness_note':'The separately closed readiness probe is not an HTTP GET or a measured request connection',
        'ssh_process_id':process.pid if process else None,'owned_ssh_process_exited':exited,
        'owned_ssh_exit_code':process.returncode if process else None,'local_listener_closed':listener_closed,
        'forced_kill_used':forced_kill,'cleanup_started_utc':cleanup_started,'cleanup_finished_utc':cleanup_finished,
        'total_wall_elapsed_ms':str(Decimal(time.perf_counter_ns()-started)/Decimal(1_000_000)),
        'errors':errors,
        'limitations':['TCP connect time is not a one-way-delay estimate and HTTP response time is not a pure RTT measurement',
                       'Response-stage timing locates observed waiting but does not identify its network or implementation cause',
                       'No packet capture, SSH verbose logs, CPU measurement, public API or production changes'],
    }
    (root/'reuse_review_result.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8',newline='\n')
    (root/'reuse_review_stderr.txt').write_bytes(stderr)
    manifest={'status':result['status'],'read_only':True,'original_artifacts_unchanged':True,
              'tunnel_cleanup_verified':exited and listener_closed,
              'files_sha256':{name:digest(root/name) for name in ('reuse_review_benchmark.py','reuse_review_result.json','reuse_review_stderr.txt')}}
    (root/'reuse_review_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({key:result[key] for key in ('status','tcp_connect_time_ms','http_connect_invocations',
                      'http_socket_object_count','http_local_port_count','http_connection_reused',
                      'measured_status_counts','response_stage_ms','owned_ssh_process_exited',
                      'local_listener_closed','total_wall_elapsed_ms','errors')},indent=2))
    if errors:
        raise SystemExit(1)


if __name__=='__main__': main()
