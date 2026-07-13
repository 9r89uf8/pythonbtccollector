import hashlib
import json
import os
import stat
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

import price_collector.shadow_signal_artifact as artifact_module
from price_collector.shadow_signal_artifact import (
    ShadowSignalArtifactError,
    load_activated_selection,
)


MODEL_SPECS = (
    ("catchup_ratio_l3000_b100", 3_000),
    ("catchup_ratio_l3500_b100", 3_500),
    ("catchup_ratio_l4000_b100", 4_000),
)
CONFIGURATION = {
    "poll_ms": 100,
    "evaluation_interval_ms": 500,
    "lags_ms": [3_000, 3_500, 4_000],
    "beta": "1",
    "futures_stale_ms": 3_000,
    "chainlink_stale_ms": 2_500,
    "reference_max_gap_ms": 3_000,
    "history_retention_ms": 10_000,
    "max_future_skew_ms": 250,
    "neutral_band_bps": "1",
    "move_size_thresholds_bps": ["1", "3"],
    "volatility_thresholds_bps": ["0.5", "1.5"],
    "volatility_lookback_ms": 10_000,
    "volatility_measure": "rms_of_consecutive_raw_bucket_returns",
    "near_expiry_ms": 10_000,
    "near_reconnect_ms": 10_000,
    "session_boundary_measure": (
        "time_since_common_segment_start_and_until_segment_end"
    ),
    "quantile_sample_max": 10_000,
    "exclude_parse_error_sessions": False,
}


def canonical_json_bytes(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def encoded_json(value):
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def coverage_payload():
    return {
        "target_eligible": 10_000,
        "valid_generated": 10_000,
        "scored": 10_000,
        "valid_coverage": "1",
        "maturation_coverage": "1",
        "gates": {
            "minimum_common_scored": {
                "observed": 10_000,
                "threshold_inclusive": 10_000,
                "passed": True,
            },
            "minimum_common_valid_coverage": {
                "observed": "1",
                "threshold_inclusive": "0.50",
                "passed": True,
            },
            "minimum_common_maturation_coverage": {
                "observed": "1",
                "threshold_inclusive": "0.99",
                "passed": True,
            },
        },
        "passed": True,
    }


def evidence_payload():
    return {
        "metrics": {},
        "gates": {
            "mae_skill_positive": {
                "observed": "0.2",
                "threshold_exclusive": "0",
                "passed": True,
            },
            "rmse_skill_positive": {
                "observed": "0.3",
                "threshold_exclusive": "0",
                "passed": True,
            },
        },
        "paired_frequency_diagnostic": {
            "hard_gate": False,
            "affects_eligibility": False,
            "affects_ranking": False,
        },
        "slices": {},
    }


def replay_payload():
    return {
        "schema_version": 2,
        "mode": "shadow_raw_replay",
        "status": "ok",
        "selection_performed": False,
        "range": {
            "start_ms": 1_000_000,
            "end_ms": 2_000_000,
            "boundary": "[start_ms,end_ms)",
        },
        "configuration": deepcopy(CONFIGURATION),
        "candidates": [
            {
                "model_version": version,
                "horizon_ms": horizon_ms,
                "beta": "1",
            }
            for version, horizon_ms in MODEL_SPECS
        ],
    }


def selection_payload(replay_raw):
    replay_digest = sha256(replay_raw)
    configuration_digest = sha256(canonical_json_bytes(CONFIGURATION))
    reports = [
        {
            "role": "calibration",
            "start_ms": 1_000_000,
            "end_ms": 2_000_000,
            "boundary": "[start_ms,end_ms)",
            "gap_from_previous_ms": None,
            "sha256": replay_digest,
            "coverage": coverage_payload(),
        },
        {
            "role": "holdout",
            "start_ms": 2_100_000,
            "end_ms": 3_100_000,
            "boundary": "[start_ms,end_ms)",
            "gap_from_previous_ms": 100_000,
            "sha256": "b" * 64,
            "coverage": coverage_payload(),
        },
    ]
    policy = deepcopy(artifact_module._EXPECTED_POLICY)
    fingerprint_payload = {
        "policy": policy,
        "configuration_sha256": configuration_digest,
        "reports": [
            {
                "role": report["role"],
                "start_ms": report["start_ms"],
                "end_ms": report["end_ms"],
                "sha256": report["sha256"],
            }
            for report in reports
        ],
    }
    return {
        "schema_version": 2,
        "mode": "shadow_primary_selection",
        "status": "selected",
        "selection_performed": True,
        "provisional": True,
        "dynamic_switching": False,
        "prediction_target": (
            "latest_chainlink_value_known_at_generated_ms_plus_horizon_ms"
        ),
        "policy": policy,
        "provenance": {
            "configuration_sha256": configuration_digest,
            "selection_fingerprint_sha256": sha256(
                canonical_json_bytes(fingerprint_payload)
            ),
            "reports": reports,
        },
        "decision": {
            "reason": "the frozen calibration winner passed its holdout gates",
            "frozen_calibration_winner": MODEL_SPECS[0][0],
            "provisional_primary_model": {
                "model_version": MODEL_SPECS[0][0],
                "horizon_ms": MODEL_SPECS[0][1],
                "beta": "1",
                "evidence_end_ms": 3_100_000,
            },
            "holdout_reranking_performed": False,
            "fallback_after_holdout_failure_performed": False,
        },
        "candidates": [
            {
                "calibration_rank": rank,
                "model_version": version,
                "horizon_ms": horizon_ms,
                "beta": "1",
                "calibration_eligible": True,
                "calibration": evidence_payload(),
                "holdout": evidence_payload(),
                "slice_warnings": [],
            }
            for rank, (version, horizon_ms) in enumerate(MODEL_SPECS, start=1)
        ],
        "limitations": ["provisional shadow signal"],
    }


def write_valid_artifacts(tmp_path):
    replay = replay_payload()
    replay_raw = encoded_json(replay)
    replay_path = tmp_path / "replay.json"
    replay_path.write_bytes(replay_raw)
    selection = selection_payload(replay_raw)
    selection_raw = encoded_json(selection)
    selection_path = tmp_path / "selection.json"
    selection_path.write_bytes(selection_raw)
    return selection_path, sha256(selection_raw), replay_path, selection


def rewrite_selection(selection_path, payload):
    raw = encoded_json(payload)
    selection_path.write_bytes(raw)
    return sha256(raw)


def recalculate_fingerprint(selection):
    provenance = selection["provenance"]
    fingerprint_payload = {
        "policy": selection["policy"],
        "configuration_sha256": provenance["configuration_sha256"],
        "reports": [
            {
                "role": report["role"],
                "start_ms": report["start_ms"],
                "end_ms": report["end_ms"],
                "sha256": report["sha256"],
            }
            for report in provenance["reports"]
        ],
    }
    provenance["selection_fingerprint_sha256"] = sha256(
        canonical_json_bytes(fingerprint_payload)
    )


def load(selection_path, digest, replay_path, **overrides):
    return load_activated_selection(
        selection_path,
        digest,
        replay_path,
        trusted_directory=overrides.pop("trusted_directory", selection_path.parent),
        enforce_posix_permissions=overrides.pop(
            "enforce_posix_permissions",
            False,
        ),
        **overrides,
    )


def test_load_activated_selection_binds_primary_models_and_runtime_config(tmp_path):
    selection_path, digest, replay_path, selection = write_valid_artifacts(tmp_path)

    activated = load(selection_path, digest, replay_path)

    assert activated.selection_schema_version == 2
    assert activated.selection_artifact_sha256 == digest
    assert activated.selection_fingerprint_sha256 == selection["provenance"][
        "selection_fingerprint_sha256"
    ]
    assert activated.policy_version == "chronological_holdout_v2"
    assert activated.evidence_end_ms == 3_100_000
    assert activated.primary_model.version == "catchup_ratio_l3000_b100"
    assert activated.primary_model.lag_ms == 3_000
    assert activated.primary_model.beta == Decimal("1")
    assert [model.version for model in activated.models] == [
        version for version, _horizon_ms in MODEL_SPECS
    ]
    assert all(isinstance(model.beta, Decimal) for model in activated.models)
    assert activated.poll_ms == 100
    assert activated.futures_stale_ms == 3_000
    assert activated.chainlink_stale_ms == 2_500
    assert activated.reference_max_gap_ms == 3_000
    assert activated.history_retention_ms == 10_000
    assert activated.max_future_skew_ms == 250


def test_selection_whole_file_sha_is_required_before_activation(tmp_path):
    selection_path, _digest, replay_path, _selection = write_valid_artifacts(tmp_path)

    with pytest.raises(ShadowSignalArtifactError, match="trusted digest"):
        load(selection_path, "0" * 64, replay_path)


@pytest.mark.parametrize("invalid_json", ["duplicate", "float", "nonfinite"])
def test_selection_json_rejects_ambiguous_numeric_and_duplicate_input(
    tmp_path,
    invalid_json,
):
    selection_path, _digest, replay_path, _selection = write_valid_artifacts(tmp_path)
    raw = selection_path.read_bytes()
    if invalid_json == "duplicate":
        raw = raw.replace(
            b'"schema_version": 2,',
            b'"schema_version": 2,\n  "schema_version": 2,',
            1,
        )
        match = "duplicate JSON key"
    elif invalid_json == "float":
        raw = raw.replace(b'"schema_version": 2', b'"schema_version": 2.0', 1)
        match = "floating-point"
    else:
        raw = raw.replace(
            b'"schema_version": 2,',
            b'"schema_version": 2,\n  "unexpected": NaN,',
            1,
        )
        match = "non-finite"
    selection_path.write_bytes(raw)

    with pytest.raises(ShadowSignalArtifactError, match=match):
        load(selection_path, sha256(raw), replay_path)


def test_selection_fingerprint_must_match_policy_configuration_and_report_set(
    tmp_path,
):
    selection_path, _digest, replay_path, selection = write_valid_artifacts(tmp_path)
    selection["provenance"]["selection_fingerprint_sha256"] = "c" * 64
    digest = rewrite_selection(selection_path, selection)

    with pytest.raises(ShadowSignalArtifactError, match="fingerprint"):
        load(selection_path, digest, replay_path)


def test_preserved_replay_raw_sha_must_appear_in_selection_provenance(tmp_path):
    selection_path, digest, replay_path, _selection = write_valid_artifacts(tmp_path)
    replay = json.loads(replay_path.read_text(encoding="utf-8"))
    replay["diagnostic_tamper"] = "changed"
    replay_path.write_bytes(encoded_json(replay))

    with pytest.raises(
        ShadowSignalArtifactError,
        match="absent from selection provenance",
    ):
        load(selection_path, digest, replay_path)


def test_replay_configuration_digest_must_match_selection_provenance(tmp_path):
    selection_path, _digest, replay_path, selection = write_valid_artifacts(tmp_path)
    selection["provenance"]["configuration_sha256"] = "d" * 64
    recalculate_fingerprint(selection)
    digest = rewrite_selection(selection_path, selection)

    with pytest.raises(ShadowSignalArtifactError, match="configuration digest"):
        load(selection_path, digest, replay_path)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda selection: selection.update(status="holdout_failed"),
            "not an accepted primary selection",
        ),
        (
            lambda selection: selection["decision"][
                "provisional_primary_model"
            ].update(horizon_ms=3_500),
            "inconsistent with its candidate",
        ),
        (
            lambda selection: selection["provenance"]["reports"][1].update(
                start_ms=1_900_000,
                gap_from_previous_ms=-100_000,
            ),
            "overlap",
        ),
        (
            lambda selection: selection["provenance"]["reports"][0][
                "coverage"
            ].update(passed=False),
            "passed must be true",
        ),
    ],
)
def test_selection_status_model_chronology_and_coverage_are_fail_closed(
    tmp_path,
    mutation,
    match,
):
    selection_path, _digest, replay_path, selection = write_valid_artifacts(tmp_path)
    mutation(selection)
    recalculate_fingerprint(selection)
    digest = rewrite_selection(selection_path, selection)

    with pytest.raises(ShadowSignalArtifactError, match=match):
        load(selection_path, digest, replay_path)


def test_trusted_directory_requires_both_files_as_direct_regular_children(tmp_path):
    selection_path, digest, replay_path, _selection = write_valid_artifacts(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(ShadowSignalArtifactError, match="direct child"):
        load_activated_selection(
            selection_path,
            digest,
            replay_path,
            trusted_directory=outside,
        )


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits only")
def test_posix_permission_enforcement_is_configurable_for_fixture_files(tmp_path):
    selection_path, digest, replay_path, _selection = write_valid_artifacts(tmp_path)

    activated = load(
        selection_path,
        digest,
        replay_path,
        enforce_posix_permissions=False,
    )
    assert activated.primary_model.version == MODEL_SPECS[0][0]


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership only")
def test_posix_strict_mode_requires_an_explicit_trusted_directory(tmp_path):
    selection_path, digest, replay_path, _selection = write_valid_artifacts(tmp_path)

    with pytest.raises(
        ShadowSignalArtifactError,
        match="trusted_directory is required",
    ):
        load_activated_selection(
            selection_path,
            digest,
            replay_path,
            enforce_posix_permissions=True,
        )


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership only")
@pytest.mark.parametrize(
    ("uid", "extra_mode", "match"),
    [
        (1_000, 0, "root-owned"),
        (0, stat.S_IWGRP, "group- or world-writable"),
        (0, stat.S_IWOTH, "group- or world-writable"),
    ],
)
def test_posix_trusted_directory_must_be_root_owned_and_not_broadly_writable(
    tmp_path,
    monkeypatch,
    uid,
    extra_mode,
    match,
):
    metadata = os.lstat(tmp_path)
    fake_metadata = SimpleNamespace(
        st_uid=uid,
        st_mode=metadata.st_mode | extra_mode,
    )
    monkeypatch.setattr(artifact_module.os, "lstat", lambda _path: fake_metadata)

    with pytest.raises(ShadowSignalArtifactError, match=match):
        artifact_module._validate_trusted_directory(
            tmp_path,
            enforce_posix_permissions=True,
        )


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership only")
@pytest.mark.parametrize(
    ("uid", "mode", "match"),
    [
        (1_000, stat.S_IFREG | 0o440, "root-owned"),
        (0, stat.S_IFREG | 0o640, "no owner, group, or world write bit"),
        (0, stat.S_IFREG | 0o460, "no owner, group, or world write bit"),
        (0, stat.S_IFREG | 0o442, "no owner, group, or world write bit"),
    ],
)
def test_posix_decision_files_must_be_root_owned_and_fully_read_only(
    uid,
    mode,
    match,
):
    metadata = SimpleNamespace(st_uid=uid, st_mode=mode)

    with pytest.raises(ShadowSignalArtifactError, match=match):
        artifact_module._validate_posix_decision_file(metadata, "selection_path")


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership only")
def test_posix_root_owned_0440_decision_file_metadata_is_accepted():
    metadata = SimpleNamespace(st_uid=0, st_mode=stat.S_IFREG | 0o440)

    artifact_module._validate_posix_decision_file(metadata, "selection_path")


@pytest.mark.skipif(os.name != "posix", reason="reliable symlinks required")
def test_artifact_files_must_not_be_symlinks(tmp_path):
    selection_path, digest, replay_path, _selection = write_valid_artifacts(tmp_path)
    symlink_path = tmp_path / "selection-link.json"
    symlink_path.symlink_to(selection_path)

    with pytest.raises(ShadowSignalArtifactError, match="non-symlink"):
        load(symlink_path, digest, replay_path)
