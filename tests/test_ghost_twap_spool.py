"""Crash-boundary and capacity checks for the optional ghost outbox."""
from copy import deepcopy
import json
import os

import pytest

from price_collector.ghost_twap_spool import GhostSpool


def record(decision="1", version=1):
    return dict(run_id="run", decision_id=decision, decision_wall_ns=1000000,
                created_ms=1, frozen_json='{"price":"100.000000000000000001"}',
                state_json='{"publication":{"status":"intent"}}',
                version=version, terminal=False)


@pytest.fixture
def spool(tmp_path):
    value = GhostSpool(tmp_path / "outbox", max_records=2, max_record_bytes=2048)
    value.open()
    yield value
    value.close()


def test_single_owner_lock_releases_for_next_process(spool):
    other = GhostSpool(spool.directory)
    try:
        with pytest.raises(OSError):
            other.open()
        spool.close()
        other.open()
    finally:
        other.close()


def test_capacity_allows_updates_but_rejects_another_identity(spool):
    spool.write(record("1"))
    spool.write(record("2"))
    spool.write(record("1", 2))
    with pytest.raises(ValueError, match="capacity"):
        spool.write(record("3"))
    assert {(r["decision_id"], r["version"]) for r in spool.read_all()} == {("1", 2), ("2", 1)}


def test_retries_are_idempotent_and_stale_versions_do_not_replace_newer(spool):
    original = record(version=2)
    spool.write(original)
    spool.write(deepcopy(original))
    spool.write(record(version=1))
    assert spool.read_all() == [original]


def test_identical_record_and_campaign_retries_do_not_repeat_fsync(spool, monkeypatch):
    original = record()
    campaign = spool.campaign(1800000000000)
    spool.write(original)
    def unexpected_sync(*args):
        raise AssertionError("unchanged durable bytes do not need another fsync")
    monkeypatch.setattr(os, "fsync", unexpected_sync)
    spool.write(deepcopy(original))
    spool.save_campaign(deepcopy(campaign))
    assert spool.read_all() == [original]


@pytest.mark.parametrize("field,new_value", [("frozen_json", '{"price":"99"}'),
                                              ("state_json", '{"different":true}')])
def test_same_version_conflicts_never_modify_existing_bytes(spool, field, new_value):
    original = record()
    spool.write(original)
    changed = dict(original, **{field: new_value})
    with pytest.raises(ValueError):
        spool.write(changed)
    assert spool.read_all() == [original]


def test_frozen_snapshot_cannot_change_even_at_a_higher_version(spool):
    original = record()
    spool.write(original)
    with pytest.raises(ValueError, match="frozen"):
        spool.write(dict(original, version=2, frozen_json='{"price":"101"}'))
    assert spool.read_all() == [original]


def test_stale_completion_cannot_remove_newer_outbox_work(spool):
    spool.write(record(version=2))
    with pytest.raises(ValueError, match="newer"):
        spool.remove(record(version=1))
    assert spool.read_all()[0]["version"] == 2
    spool.remove(record(version=2))
    assert spool.read_all() == []


def test_failed_replace_keeps_previous_complete_record(spool, monkeypatch):
    original = record()
    spool.write(original)
    def fail_replace(*args):
        raise OSError("simulated interruption before rename")
    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError):
        spool.write(record(version=2))
    assert spool.read_all() == [original]
    assert list(spool.directory.glob("*.tmp")), "interrupted temporary file remains distinguishable"


def test_fsync_failure_does_not_publish_an_unflushed_record(spool, monkeypatch):
    original = record()
    spool.write(original)
    def fail_sync(*args):
        raise OSError("disk fsync failed")
    monkeypatch.setattr(os, "fsync", fail_sync)
    with pytest.raises(OSError):
        spool.write(record(version=2))
    assert spool.read_all() == [original]


def test_byte_limit_rejects_before_replacing_old_snapshot(spool):
    original = record()
    spool.write(original)
    with pytest.raises(ValueError, match="bound"):
        spool.write(dict(original, version=2, state_json="x" * 2048))
    assert spool.read_all() == [original]


def test_recovery_rejects_misnamed_identity_and_oversized_files(spool):
    spool.write(record())
    path = next(spool.directory.glob("*.row"))
    path.write_text(json.dumps(record("another")))
    with pytest.raises(ValueError, match="identity"):
        spool.read_all()
    path.write_bytes(b"x" * 2049)
    with pytest.raises(ValueError, match="oversized"):
        spool.read_all()


def test_recovery_rejects_truncated_json(spool):
    spool.write(record())
    next(spool.directory.glob("*.row")).write_bytes(b'{"run_id":')
    with pytest.raises(ValueError):
        spool.read_all()


def test_reopen_removes_only_owned_interrupted_temporaries(spool):
    original = record()
    spool.write(original)
    row_path = next(spool.directory.glob("*.row"))
    interrupted = row_path.with_suffix(".tmp")
    interrupted.write_bytes(b"incomplete later state")
    campaign_tmp = spool.directory / "campaign.tmp"
    campaign_tmp.write_bytes(b"incomplete campaign")
    unrelated = spool.directory / "user-note.tmp"
    unrelated.write_bytes(b"keep me")
    spool.close()
    spool.open()
    assert not interrupted.exists()
    assert not campaign_tmp.exists()
    assert unrelated.read_bytes() == b"keep me"
    assert spool.read_all() == [original]


def test_campaign_keeps_deadline_stop_and_last_clock_across_reopen(spool):
    start = 1800000000000
    state = spool.campaign(start)
    state.update(stop_reason="audit_size_cap", last_wall_ms=start + 123456)
    spool.save_campaign(state)
    spool.close()
    spool.open()
    assert spool.campaign(start) == state
    with pytest.raises(ValueError, match="start differs"):
        spool.campaign(start + 1)


@pytest.mark.parametrize("raw", ['{"start_ms":100}',
                                  '{"start_ms":100,"stop_reason":null,"last_wall_ms":"bad"}',
                                  '{"start_ms":100,"stop_reason":null,"last_wall_ms":99}'])
def test_corrupt_campaign_metadata_fails_closed(spool, raw):
    (spool.directory / "campaign.json").write_text(raw)
    with pytest.raises(ValueError):
        spool.campaign(100)
