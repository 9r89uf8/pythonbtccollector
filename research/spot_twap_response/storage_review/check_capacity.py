"""Read current database/filesystem capacity without scanning application rows."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess

REMOTE = r'''
import json, os, subprocess
from datetime import datetime, timezone
sql = """BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SELECT json_build_object('database_bytes', pg_database_size(current_database()),
                        'data_directory', current_setting('data_directory'));
COMMIT;"""
p = subprocess.run(['sudo','-u','postgres','psql','-X','-q','-A','-t',
                    '-v','ON_ERROR_STOP=1','-d','price_collector'],
                   input=sql,text=True,capture_output=True,timeout=8)
if p.returncode:
    raise RuntimeError(p.stderr)
data = json.loads(p.stdout)
stat = os.statvfs(data['data_directory'])
data.update(utc=datetime.now(timezone.utc).isoformat(),
            filesystem_bytes=stat.f_blocks*stat.f_frsize,
            filesystem_free_bytes=stat.f_bfree*stat.f_frsize,
            filesystem_available_bytes=stat.f_bavail*stat.f_frsize,
            method='pg_database_size and statvfs on database data_directory; no application row scan')
print(json.dumps(data,indent=2))
'''

def main():
    folder = Path(__file__).resolve().parent
    names = ['capacity.json','stderr.txt','manifest.json']
    if any((folder/name).exists() for name in names):
        raise SystemExit('Refusing to overwrite existing capacity evidence')
    run = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',
                          'root@152.42.247.86','python3 -'],
                         input=REMOTE.encode(),capture_output=True,timeout=22)
    (folder/'capacity.json').write_bytes(run.stdout)
    (folder/'stderr.txt').write_bytes(run.stderr)
    assert run.returncode == 0, run.stderr.decode()
    result = json.loads(run.stdout)
    assert all(isinstance(result[key],int) and result[key]>0 for key in
               ['database_bytes','filesystem_bytes','filesystem_available_bytes'])
    manifest = dict(status='accepted',checked_utc=datetime.now(timezone.utc).isoformat(),
                    read_only=True,application_rows_scanned=0,production_changes=False,
                    files_sha256={name:hashlib.sha256((folder/name).read_bytes()).hexdigest()
                                  for name in ['check_capacity.py','capacity.json','stderr.txt']})
    (folder/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))

if __name__ == '__main__':
    main()
