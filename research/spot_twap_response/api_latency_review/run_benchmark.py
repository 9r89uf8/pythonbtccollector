"""Send the read-only benchmark over SSH stdin and preserve its local evidence."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import subprocess


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    root=Path(__file__).resolve().parent
    for name in ('result.json','stderr.txt','manifest.json'):
        if (root/name).exists(): raise SystemExit('Refusing to overwrite benchmark artifacts')
    script=root/'benchmark_remote.py'
    run=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',
                        'root@152.42.247.86','python3 -'],
                       input=script.read_bytes(),capture_output=True,timeout=25)
    (root/'result.json').write_bytes(run.stdout)
    (root/'stderr.txt').write_bytes(run.stderr)
    result=json.loads(run.stdout) if run.returncode==0 else None
    errors=[]
    if run.returncode: errors.append(f'SSH/benchmark exit {run.returncode}')
    if result and (result['status']!='accepted' or result['completed_measured_requests']!=50):
        errors.append('Incomplete benchmark')
    manifest={'status':'accepted' if not errors else 'failed','ssh_exit_code':run.returncode,
              'validation_errors':errors,'remote_target':'root@152.42.247.86',
              'read_only':True,'remote_files_written':False,'database_queries':0,
              'files_sha256':{name:digest(root/name) for name in ('benchmark_remote.py','run_benchmark.py','result.json','stderr.txt')}}
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8',newline='\n')
    print(json.dumps({'status':manifest['status'],'errors':errors,
                      'summary':{key:result[key] for key in ('started_utc','wall_elapsed_ms','completed_measured_requests',
                                'connection_reused_including_warmup','measured_status_counts','measured_body_bytes_min',
                                'measured_body_bytes_max','latency_ms','observed_minimum_start_spacing_ns')} if result else None},indent=2))
    if errors: raise SystemExit(1)


if __name__=='__main__': main()
