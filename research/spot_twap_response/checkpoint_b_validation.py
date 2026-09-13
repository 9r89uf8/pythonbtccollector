"""Record local B checks and an already completed disposable-PostgreSQL run."""
import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'results/spot_twap_response/2026-09-13-checkpoint-b'


def record_hashes(manifest):
    files = set(subprocess.check_output(['git', 'diff', '--name-only', 'b62285a'],
                                       cwd=ROOT, text=True).splitlines())
    files.update(subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'],
                                        cwd=ROOT, text=True).splitlines())
    files.add('price_collector/ghost_twap.py')
    manifest['source_sha256'] = {name: sha256((ROOT/name).read_bytes()).hexdigest()
                               for name in sorted(files) if (ROOT/name).is_file()
                               and not name.startswith('results/')}
    manifest['artifact_sha256'] = {path.name: sha256(path.read_bytes()).hexdigest()
                                  for path in sorted(OUTPUT.iterdir())
                                  if path.is_file() and path.name != 'validation.json'}
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--python', default=sys.executable)
    parser.add_argument('--postgres-receipt', type=Path)
    parser.add_argument('--refresh-hashes-only', action='store_true')
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    path = OUTPUT/'validation.json'
    if args.refresh_hashes_only:
        manifest = json.loads(path.read_text())
    else:
        if args.postgres_receipt is None:
            parser.error('--postgres-receipt is required for a validation run')
        receipt = json.loads(args.postgres_receipt.read_text())
        assert receipt['ssh_returncode'] == 0 and receipt['result']['tests_returncode'] == 0
        assert receipt['result']['database'].startswith('ghost_checkpoint_b_validation_')
        assert receipt['commands'][-1]['command'][-2:] == ['dropdb', receipt['result']['database']]
        assert receipt['commands'][-1]['returncode'] == 0
        (OUTPUT/'postgresql_validation.json').write_bytes(args.postgres_receipt.read_bytes())
        result = subprocess.run([args.python, '-m', 'pytest', '-q'], cwd=ROOT,
                                capture_output=True, text=True)
        (OUTPUT/'local_tests.txt').write_bytes((result.stdout + result.stderr).encode('utf-8'))
        print(result.stdout[-3000:])
        if result.returncode:
            raise SystemExit(result.returncode)
        match = re.search(r'(\d+) passed(?:, (\d+) skipped)? in ([0-9.]+)s', result.stdout)
        assert match
        pg_tests = next(c for c in receipt['commands'] if 'pytest' in c['command'])
        pg_match = re.search(r'(\d+) passed in ([0-9.]+)s', pg_tests['stdout'])
        assert pg_match
        manifest = dict(status='passed', checkpoint='B implementation; prospective canary not started',
                        validated_utc=datetime.now(timezone.utc).isoformat(),
                        base_commit='b62285a040aff4787414327d42c75cb651058099',
                        local_tests=dict(passed=int(match[1]), skipped=int(match[2] or 0),
                                         elapsed_seconds=match[3], command=[args.python, '-m', 'pytest', '-q'],
                                         python=subprocess.check_output([args.python, '--version'], text=True).strip()),
                        production_python_tests=dict(passed=int(pg_match[1]), elapsed_seconds=pg_match[2],
                                                     environment='Python 3.12, disposable PostgreSQL database, controlled Redis/clocks'),
                        postgres_database_removed=True, production_deployed=False, canary_started=False,
                        limitations=['Bounded synthetic storage probes, not a full-capacity canary.',
                                     'No measured live publication or browser delivery latency.',
                                     'Database filesystem placement and sustained storage margin must be accepted before enablement.'])
    path.write_bytes((json.dumps(record_hashes(manifest), indent=2) + '\n').encode('utf-8'))
    print(path)


if __name__ == '__main__':
    main()
