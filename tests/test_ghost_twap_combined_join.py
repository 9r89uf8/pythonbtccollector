import importlib.util
import json
from hashlib import sha256
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location('combined_join', Path(__file__).resolve().parents[1] /
    'research/spot_twap_response/combined_canary/join_observer.py')
join = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(join)


def evidence(status='acknowledged'):
    payload = dict(run_id='run', decision_id='decision', publication_attempt_wall_ns='100',
        publication_attempt_monotonic_ns='200', publication_eligibility=dict(eligible_horizons=[5, 10, 30]))
    raw = json.dumps(payload).encode()
    digest = sha256(raw).hexdigest()
    entry = dict(run_id='run', decision_id='decision', status=status, attempt_wall_ns='100',
        attempt_monotonic_ns='200', eligible_horizons=[5, 10, 30])
    return dict(observed_qualified_payload_sha256=[digest]), {digest: entry}, {digest: raw}


def test_join_preserves_uncertain_publication_status():
    result = join.join_records(*evidence('uncertain'))
    assert result['unique_payloads_by_final_audit_status'] == {'uncertain': 1}
    assert result['verified_unique_payloads'] == 1


@pytest.mark.parametrize('field,value', [('run_id', 'other'), ('decision_id', 'other'),
    ('eligible_horizons', [1, 5, 10, 30]), ('attempt_wall_ns', '101'), ('attempt_monotonic_ns', '201')])
def test_join_rejects_wrong_identity_membership_or_clock(field, value):
    coverage, index, payloads = evidence()
    next(iter(index.values()))[field] = value
    with pytest.raises(ValueError, match='mismatch'):
        join.join_records(coverage, index, payloads)


def test_join_rejects_foreign_audit_payload():
    coverage, _, payloads = evidence()
    with pytest.raises(ValueError, match='absent from verified campaign'):
        join.join_records(coverage, {}, payloads)


def test_join_rejects_tampered_bytes():
    coverage, index, payloads = evidence()
    payloads[next(iter(payloads))] += b' '
    with pytest.raises(ValueError, match='hash mismatch'):
        join.join_records(coverage, index, payloads)


def test_join_rejects_duplicate_json_keys():
    with pytest.raises(ValueError, match='Duplicate'):
        join.decode(b'{"run_id":"first","run_id":"second"}')


def test_join_rejects_known_non_write_outcome():
    with pytest.raises(ValueError, match='non-write'):
        join.join_records(*evidence('expired_before_attempt'))


def test_checked_reread_rejects_changed_bytes(tmp_path):
    path = tmp_path / 'samples.jsonl'
    raw = b'{"count":1}\n'
    path.write_bytes(raw)
    expected = dict(bytes=len(raw), sha256=sha256(raw).hexdigest())
    assert list(join.checked_lines(path, 100, expected)) == [{'count': 1}]
    path.write_bytes(b'{"count":2}\n')
    with pytest.raises(ValueError, match='changed'):
        list(join.checked_lines(path, 100, expected))
