"""Stream two frozen exports and verify prior publication/target evidence unchanged."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time

OLD_SHA = '8cf583804c51855d5b443e359ca1fd3f8f751066ff07924498428e15a188a9be'
NEW_SHA = 'f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954'
OLD_BYTES, NEW_BYTES = 747548263, 1078545445
OLD_ROWS, NEW_ROWS = 16329, 23411
NEW_RUN = 'fe2dd06da5754c50b696cb9419ca7251'
KEYS = {'created_ms', 'decision_id', 'decision_wall_ns', 'frozen_json', 'frozen_sha256',
        'run_id', 'state_json', 'state_sha256', 'terminal', 'version'}
MAX_LINE_BYTES = 1048576


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def object_hash(value: object) -> str:
    # Financial truth is already decimal text. A numeric Decimal would indicate
    # an unexpected JSON financial number and deliberately fails serialization.
    return digest(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())


def stream(path: Path, expected_sha: str, expected_bytes: int, expected_rows: int):
    total, count, whole = 0, 0, hashlib.sha256()
    previous = None
    with path.open('rb') as handle:
        while True:
            raw = handle.readline(MAX_LINE_BYTES + 1)
            if not raw:
                break
            if len(raw) > MAX_LINE_BYTES or not raw.endswith(b'\n'):
                raise ValueError('oversized or truncated export row')
            total += len(raw); count += 1; whole.update(raw)
            row = json.loads(raw, parse_float=Decimal)
            if set(row) != KEYS:
                raise ValueError('unexpected canonical export columns')
            key = (row['run_id'], row['decision_id'])
            if previous is not None and key <= previous:
                raise ValueError('export identities are duplicated or unordered')
            previous = key
            if digest(row['frozen_json'].encode()) != row['frozen_sha256']:
                raise ValueError('frozen bytes do not match saved hash')
            if digest(row['state_json'].encode()) != row['state_sha256']:
                raise ValueError('state bytes do not match saved hash')
            state = json.loads(row['state_json'], parse_float=Decimal)
            signature = (digest(raw), row['frozen_sha256'], row['state_sha256'],
                         object_hash(state.get('publication')), object_hash(state.get('targets')))
            yield key, signature
    if (total, count, whole.hexdigest()) != (expected_bytes, expected_rows, expected_sha):
        raise ValueError('whole export bytes/count/hash mismatch')


def compare(old: Path, new: Path) -> dict:
    started = time.monotonic_ns()
    index = dict(stream(old, OLD_SHA, OLD_BYTES, OLD_ROWS))
    if len(index) != OLD_ROWS:
        raise ValueError('unexpected old identity count')
    counts = Counter()
    mismatches = []
    labels = ('canonical_row_bytes', 'frozen_bytes', 'state_bytes', 'publication_object', 'target_object')
    additions = Counter()
    for key, current in stream(new, NEW_SHA, NEW_BYTES, NEW_ROWS):
        previous = index.pop(key, None)
        if previous is None:
            additions[key[0]] += 1
            continue
        counts['prior_identities_found'] += 1
        for i, label in enumerate(labels):
            if current[i] == previous[i]:
                counts[label + '_unchanged'] += 1
            else:
                counts[label + '_changed'] += 1
                if len(mismatches) < 20:
                    mismatches.append({'run_id':key[0], 'decision_id':key[1], 'field':label})
    accepted = (not index and not mismatches and counts['prior_identities_found'] == OLD_ROWS
                and additions == {NEW_RUN: NEW_ROWS - OLD_ROWS})
    return {'accepted':accepted, 'read_only':True,
        'completed_utc':datetime.now(timezone.utc).isoformat(),
        'duration_ns':str(time.monotonic_ns()-started),
        'old_export':{'path':str(old), 'bytes':OLD_BYTES, 'rows':OLD_ROWS, 'sha256':OLD_SHA},
        'new_export':{'path':str(new), 'bytes':NEW_BYTES, 'rows':NEW_ROWS, 'sha256':NEW_SHA},
        'whole_file_hashes_and_embedded_hashes_verified':True,
        'comparisons':{label:{'unchanged':counts[label+'_unchanged'],
                              'changed':counts[label+'_changed']} for label in labels},
        'prior_identities_found':counts['prior_identities_found'],
        'prior_identities_missing':len(index), 'added_rows_by_run':dict(additions),
        'mismatch_examples':mismatches,
        'export_columns':sorted(KEYS),
        'attestation_columns_in_export':False,
        'interpretation':'Exact canonical old-row bytes also preserve version, terminal status and decision clocks. Exact state bytes and publication/target object hashes preserve the original acknowledged publication and observed target evidence.',
        'limitations':['Attestation metadata is excluded from canonical exports; this check makes no claim that database verification columns are unchanged.',
                       'This is an immutable export comparison, not a re-creation of omitted source events or an independent forecast accuracy score.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old', type=Path, required=True)
    parser.add_argument('--new', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit('output exists; prior evidence will not be overwritten')
    result = compare(args.old, args.new)
    result['code_sha256'] = digest(Path(__file__).read_bytes())
    with args.output.open('x', encoding='utf-8') as handle:
        json.dump(result, handle, indent=2); handle.write('\n')
    print(json.dumps({'accepted':result['accepted'], 'comparisons':result['comparisons'],
        'prior_identities_missing':result['prior_identities_missing'],
        'added_rows_by_run':result['added_rows_by_run'], 'duration_ns':result['duration_ns']}, indent=2))
    if not result['accepted']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
