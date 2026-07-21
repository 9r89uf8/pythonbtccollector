import asyncio
import hashlib
import json
from decimal import Decimal

import pytest

import price_collector.shadow_signal_lag_recovery as recovery
from price_collector.shadow_signal_lag_test import CALIBRATION_LAGS_MS
from price_collector.shadow_signal_replay import (
    CHAINLINK_SESSION_SOURCE,
    FUTURES_SESSION_SOURCE,
)


def original_payload():
    return {
        "schema_version": 1,
        "mode": "simple_shadow_lag_test",
        "status": "insufficient_evidence",
        "calibration_range": dict(recovery.EXPECTED_CALIBRATION_RANGE),
        "holdout_range": dict(recovery.EXPECTED_HOLDOUT_RANGE),
        "fixed_replay_settings": dict(
            recovery.EXPECTED_ORIGINAL_FIXED_REPLAY_SETTINGS
        ),
        "selection_rule": dict(recovery.EXPECTED_SELECTION_RULE),
        "calibration": {
            "status": "insufficient_evidence",
            "reason": "calibration_replay_no_eligible_segments",
            "winner_lag_ms": None,
            "ranking": [],
        },
        "holdout": None,
        "calibration_report": {
            "status": "no_eligible_segments",
            "range": dict(recovery.EXPECTED_CALIBRATION_RANGE),
            "data_quality": {
                "sessions_total_by_source": {
                    FUTURES_SESSION_SOURCE: 2,
                    CHAINLINK_SESSION_SOURCE: 15,
                },
                "sessions_eligible_by_source": {FUTURES_SESSION_SOURCE: 2},
                "sessions_excluded_by_reason": {"parse_errors": 15},
                "excluded_integrity_scope_raw_rows": 94_664,
                "common_healthy_segments": 0,
                "input_events": 0,
                "ignored_events": 0,
                "polls_processed": 0,
            },
        },
        "holdout_report": None,
    }


def write_original(tmp_path, monkeypatch, payload=None, *, raw=None):
    path = tmp_path / recovery.ORIGINAL_RESULT_BASENAME
    if raw is None:
        raw = (json.dumps(payload or original_payload()) + "\n").encode()
    path.write_bytes(raw)
    monkeypatch.setattr(
        recovery,
        "ORIGINAL_RESULT_SHA256",
        hashlib.sha256(raw).hexdigest(),
    )
    return path, raw


def candidate(lag_ms, skill):
    return {
        "model_version": f"catchup_ratio_l{lag_ms}_b100",
        "horizon_ms": lag_ms,
        "common_cohort": {
            "target_eligible": 10_000,
            "valid_generated": 10_000,
            "scored": 10_000,
            "scored_coverage": Decimal("1"),
            "maturation_coverage": Decimal("1"),
            "metrics": {
                "model_mean_absolute_error_usd": Decimal("1"),
                "baseline_mean_absolute_error_usd": Decimal("2"),
                "mae_skill_vs_no_change": skill,
                "wins": 100,
                "ties": 0,
                "losses": 0,
            },
        },
    }


class FakeReplayReport:
    def __init__(
        self,
        config,
        *,
        status="no_scored_forecasts",
        candidates=(),
        futures_counts=None,
        chainlink_counts=None,
        excluded=None,
        excluded_ids=None,
        excluded_raw_rows=0,
    ):
        self.config = config
        self.status = status
        self.candidate_summaries = tuple(candidates)
        self.futures_counts = futures_counts or {"0": 2}
        self.chainlink_counts = chainlink_counts or {"2": 15}
        self.excluded = excluded or {}
        self.excluded_ids = excluded_ids or {}
        self.excluded_raw_rows = excluded_raw_rows

    def to_dict(self):
        return {
            "status": self.status,
            "configuration": {
                "exclude_parse_error_sessions": (
                    self.config.exclude_parse_error_sessions
                ),
                "allowed_chainlink_parse_error_totals": list(
                    self.config.allowed_chainlink_parse_error_totals
                ),
            },
            "data_quality": {
                "session_policy": (
                    "completed_integrity_checked_with_exact_chainlink_"
                    "parse_error_allowlist"
                ),
                "sessions_excluded_by_reason": self.excluded,
                "excluded_session_ids_by_reason": self.excluded_ids,
                "excluded_integrity_scope_raw_rows": self.excluded_raw_rows,
                "parse_error_totals_by_source": {
                    FUTURES_SESSION_SOURCE: self.futures_counts,
                    CHAINLINK_SESSION_SOURCE: self.chainlink_counts,
                },
                "parse_error_exception_applied_by_source": {
                    CHAINLINK_SESSION_SOURCE: self.chainlink_counts.get("2", 0)
                },
            },
            "candidates": list(self.candidate_summaries),
        }


def original_artifact(tmp_path):
    return recovery.OriginalRecoveryArtifact(
        path=tmp_path / recovery.ORIGINAL_RESULT_BASENAME,
        sha256=recovery.ORIGINAL_RESULT_SHA256,
        payload=original_payload(),
    )


def valid_parse_error_census():
    return {
        FUTURES_SESSION_SOURCE: {"0": 3},
        CHAINLINK_SESSION_SOURCE: {"2": 29, "3": 1},
    }


def valid_excluded_session_evidence():
    return dict(recovery.EXPECTED_EXCLUDED_SESSION_EVIDENCE)


def recovery_source_provenance():
    return recovery.RecoverySourceProvenance(
        git_commit="b" * 40,
        parent_git_commit=recovery.INITIAL_RECOVERY_GIT_COMMIT,
        original_git_commit=recovery.OPERATOR_RECORDED_SOURCE_GIT_COMMIT,
        changed_paths_from_parent=recovery.EXPECTED_RECOVERY_FIX_CHANGED_PATHS,
        changed_paths_from_original=recovery.EXPECTED_RECOVERY_CHANGED_PATHS,
    )


def test_load_original_requires_frozen_hash_and_incident_fields(
    tmp_path,
    monkeypatch,
):
    path, _raw = write_original(tmp_path, monkeypatch)

    artifact = recovery.load_original_recovery_artifact(path)

    assert artifact.payload["calibration"]["winner_lag_ms"] is None
    assert artifact.sha256 == recovery.ORIGINAL_RESULT_SHA256

    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(recovery.RecoveryValidationError, match="SHA-256"):
        recovery.load_original_recovery_artifact(path)


def test_load_original_rejects_duplicate_json_keys(tmp_path, monkeypatch):
    payload = original_payload()
    encoded = json.dumps(payload)
    raw = encoded.replace(
        '"schema_version": 1,',
        '"schema_version": 1, "schema_version": 1,',
        1,
    ).encode()
    path, _raw = write_original(tmp_path, monkeypatch, raw=raw)

    with pytest.raises(recovery.RecoveryValidationError, match="duplicate JSON key"):
        recovery.load_original_recovery_artifact(path)


def test_output_validation_refuses_wrong_or_existing_name(tmp_path):
    original = tmp_path / recovery.ORIGINAL_RESULT_BASENAME
    original.write_text("original", encoding="utf-8")
    wrong = tmp_path / "recovery.json"
    with pytest.raises(recovery.RecoveryValidationError, match="basename"):
        recovery.validate_recovery_output_path(
            original_path=original,
            output_path=wrong,
        )

    output = tmp_path / recovery.RECOVERY_RESULT_BASENAME
    output.write_text("existing", encoding="utf-8")
    with pytest.raises(recovery.RecoveryValidationError, match="already exists"):
        recovery.validate_recovery_output_path(
            original_path=original,
            output_path=output,
        )


def test_posthoc_recovery_is_descriptive_and_freezes_winner_before_holdout(
    tmp_path,
    monkeypatch,
):
    seen_configs = []
    census_loads = 0
    session_evidence_loads = 0

    async def load_census(_database_url):
        nonlocal census_loads
        census_loads += 1
        return valid_parse_error_census()

    async def load_session_evidence(_database_url):
        nonlocal session_evidence_loads
        session_evidence_loads += 1
        return valid_excluded_session_evidence()

    monkeypatch.setattr(recovery, "load_recovery_session_census", load_census)
    monkeypatch.setattr(
        recovery,
        "load_excluded_session_evidence",
        load_session_evidence,
    )

    async def replay(**kwargs):
        config = kwargs["config"]
        seen_configs.append(config)
        if len(seen_configs) == 1:
            skills = {
                1_500: Decimal("0.1"),
                2_000: Decimal("0.3"),
                2_500: Decimal("0.2"),
                3_000: Decimal("0.8"),
                3_500: Decimal("0.9"),
            }
            return FakeReplayReport(
                config,
                status="ok",
                candidates=[candidate(lag, skills[lag]) for lag in CALIBRATION_LAGS_MS],
            )
        return FakeReplayReport(
            config,
            status="ok",
            candidates=[
                candidate(2_000, Decimal("0.25")),
                candidate(3_000, Decimal("0.2")),
            ],
            chainlink_counts={"2": 14, "3": 1},
            excluded={"parse_errors": 1},
            excluded_ids={
                "parse_errors": [recovery.EXCLUDED_CHAINLINK_CONNECTION_ID]
            },
            excluded_raw_rows=6_023,
        )

    result = asyncio.run(
        recovery.run_posthoc_lag_recovery(
            database_url="postgresql://writer@example/test",
            original=original_artifact(tmp_path),
            recovery_source=recovery_source_provenance(),
            replay=replay,
        )
    )

    assert [config.lags_ms for config in seen_configs] == [
        (1_500, 2_000, 2_500, 3_000, 3_500),
        (2_000, 3_000),
    ]
    assert all(
        config.allowed_chainlink_parse_error_totals == (0, 2)
        for config in seen_configs
    )
    assert result["status"] == "recovery_complete"
    assert result["evidence_class"] == "descriptive_only"
    assert result["eligible_for_production_promotion"] is False
    assert result["original_result_preserved"] is True
    assert result["conclusion"]["recovered_analysis_status"] == (
        "observed_shorter_better"
    )
    assert result["provenance"]["configuration_delta_from_original"] == {
        "only_changed_frozen_replay_setting": (
            "allowed_chainlink_parse_error_totals"
        ),
        "original_effective_value": [0],
        "recovery_value": [0, 2],
        "all_model_timing_evidence_and_decision_settings_unchanged": True,
    }
    implementation = result["provenance"]["recovery_implementation"]
    assert (
        implementation["parent_git_commit"]
        == recovery.INITIAL_RECOVERY_GIT_COMMIT
    )
    assert implementation["original_git_commit"] == (
        recovery.OPERATOR_RECORDED_SOURCE_GIT_COMMIT
    )
    assert (
        implementation["single_direct_child_of_initial_recovery_commit"]
        is True
    )
    assert result["provenance"]["recovery_policy"][
        "excluded_chainlink_session"
    ] == valid_excluded_session_evidence()
    assert census_loads == 2
    assert session_evidence_loads == 2
    assert result["provenance"]["recovery_policy"][
        "database_evidence_reloaded_after_replay"
    ] == {
        "full_range_session_census": True,
        "excluded_chainlink_session": True,
    }


@pytest.mark.parametrize(
    ("futures_counts", "chainlink_counts", "excluded", "match"),
    [
        ({"0": 1, "2": 1}, {"2": 15}, {}, "Futures"),
        ({"0": 2}, {"1": 1, "2": 14}, {}, "unexpected Chainlink"),
        ({"0": 2}, {"2": 14, "3": 1}, {}, "unexpected Chainlink"),
        ({"0": 2}, {"2": 15}, {"parse_errors": 1}, "frozen incident"),
    ],
)
def test_recovery_fails_closed_on_any_unexpected_parse_total(
    tmp_path,
    futures_counts,
    chainlink_counts,
    excluded,
    match,
):
    async def replay(**kwargs):
        return FakeReplayReport(
            kwargs["config"],
            futures_counts=futures_counts,
            chainlink_counts=chainlink_counts,
            excluded=excluded,
        )

    with pytest.raises(recovery.RecoveryValidationError, match=match):
        asyncio.run(
            recovery.run_posthoc_lag_recovery(
                database_url="postgresql://writer@example/test",
                original=original_artifact(tmp_path),
                recovery_source=recovery_source_provenance(),
                parse_error_census=valid_parse_error_census(),
                excluded_session_evidence=valid_excluded_session_evidence(),
                replay=replay,
            )
        )


@pytest.mark.parametrize(
    (
        "chainlink_counts",
        "excluded",
        "excluded_ids",
        "excluded_raw_rows",
        "match",
    ),
    [
        (
            {"2": 15},
            {"parse_errors": 1},
            {"parse_errors": [recovery.EXCLUDED_CHAINLINK_CONNECTION_ID]},
            6_023,
            "count-three",
        ),
        (
            {"2": 13, "3": 2},
            {"parse_errors": 1},
            {"parse_errors": [recovery.EXCLUDED_CHAINLINK_CONNECTION_ID]},
            6_023,
            "count-three",
        ),
        (
            {"2": 14, "3": 1},
            {"parse_errors": 1},
            {
                "parse_errors": [
                    "00000000-0000-0000-0000-000000000000"
                ]
            },
            6_023,
            "frozen incident",
        ),
        (
            {"2": 14, "3": 1},
            {"parse_errors": 1},
            {"parse_errors": [recovery.EXCLUDED_CHAINLINK_CONNECTION_ID]},
            1,
            "frozen incident",
        ),
    ],
)
def test_recovery_requires_exact_count_three_holdout_session_exclusion(
    tmp_path,
    chainlink_counts,
    excluded,
    excluded_ids,
    excluded_raw_rows,
    match,
):
    replay_calls = 0

    async def replay(**kwargs):
        nonlocal replay_calls
        replay_calls += 1
        config = kwargs["config"]
        if replay_calls == 1:
            return FakeReplayReport(
                config,
                status="ok",
                candidates=[
                    candidate(lag, Decimal("0.1"))
                    for lag in CALIBRATION_LAGS_MS
                ],
            )
        return FakeReplayReport(
            config,
            status="ok",
            candidates=[
                candidate(1_500, Decimal("0.1")),
                candidate(3_000, Decimal("0.05")),
            ],
            chainlink_counts=chainlink_counts,
            excluded=excluded,
            excluded_ids=excluded_ids,
            excluded_raw_rows=excluded_raw_rows,
        )

    with pytest.raises(recovery.RecoveryValidationError, match=match):
        asyncio.run(
            recovery.run_posthoc_lag_recovery(
                database_url="postgresql://writer@example/test",
                original=original_artifact(tmp_path),
                recovery_source=recovery_source_provenance(),
                parse_error_census=valid_parse_error_census(),
                excluded_session_evidence=valid_excluded_session_evidence(),
                replay=replay,
            )
        )


@pytest.mark.parametrize(
    "census",
    [
        {
            FUTURES_SESSION_SOURCE: {"0": 2, "2": 1},
            CHAINLINK_SESSION_SOURCE: {"2": 29, "3": 1},
        },
        {
            FUTURES_SESSION_SOURCE: {"0": 2},
            CHAINLINK_SESSION_SOURCE: {"2": 29, "3": 1},
        },
        {
            FUTURES_SESSION_SOURCE: {"0": 3},
            CHAINLINK_SESSION_SOURCE: {"1": 1, "2": 28, "3": 1},
        },
        {
            FUTURES_SESSION_SOURCE: {"0": 3},
            CHAINLINK_SESSION_SOURCE: {"2": 28, "3": 1},
        },
        {
            FUTURES_SESSION_SOURCE: {"0": 3},
            CHAINLINK_SESSION_SOURCE: {"2": 30},
        },
    ],
)
def test_full_range_census_aborts_before_replay(tmp_path, census):
    replay_called = False

    async def replay(**_kwargs):
        nonlocal replay_called
        replay_called = True
        raise AssertionError("invalid census must stop before replay")

    with pytest.raises(recovery.RecoveryValidationError):
        asyncio.run(
            recovery.run_posthoc_lag_recovery(
                database_url="postgresql://writer@example/test",
                original=original_artifact(tmp_path),
                recovery_source=recovery_source_provenance(),
                parse_error_census=census,
                excluded_session_evidence=valid_excluded_session_evidence(),
                replay=replay,
            )
        )

    assert replay_called is False


def test_changed_excluded_session_evidence_aborts_before_replay(tmp_path):
    replay_called = False
    evidence = valid_excluded_session_evidence()
    evidence["raw_rows"] = 6_022

    async def replay(**_kwargs):
        nonlocal replay_called
        replay_called = True
        raise AssertionError("invalid session evidence must stop before replay")

    with pytest.raises(
        recovery.RecoveryValidationError,
        match="excluded Chainlink session evidence",
    ):
        asyncio.run(
            recovery.run_posthoc_lag_recovery(
                database_url="postgresql://writer@example/test",
                original=original_artifact(tmp_path),
                recovery_source=recovery_source_provenance(),
                parse_error_census=valid_parse_error_census(),
                excluded_session_evidence=evidence,
                replay=replay,
            )
        )

    assert replay_called is False


def test_database_census_change_during_replay_aborts(monkeypatch, tmp_path):
    census_loads = 0

    async def load_census(_database_url):
        nonlocal census_loads
        census_loads += 1
        census = valid_parse_error_census()
        if census_loads == 2:
            census[FUTURES_SESSION_SOURCE] = {"0": 2}
        return census

    async def load_session_evidence(_database_url):
        return valid_excluded_session_evidence()

    async def replay(**kwargs):
        return FakeReplayReport(kwargs["config"])

    monkeypatch.setattr(recovery, "load_recovery_session_census", load_census)
    monkeypatch.setattr(
        recovery,
        "load_excluded_session_evidence",
        load_session_evidence,
    )

    with pytest.raises(
        recovery.RecoveryValidationError,
        match="post-replay full-range session census",
    ):
        asyncio.run(
            recovery.run_posthoc_lag_recovery(
                database_url="postgresql://writer@example/test",
                original=original_artifact(tmp_path),
                recovery_source=recovery_source_provenance(),
                replay=replay,
            )
        )

    assert census_loads == 2


def test_database_census_is_read_only_and_covers_full_frozen_range(monkeypatch):
    class FakeConnection:
        def __init__(self):
            self.fetch_call = None
            self.closed = False

        async def fetch(self, query, *arguments):
            self.fetch_call = (query, arguments)
            return [
                {
                    "source": FUTURES_SESSION_SOURCE,
                    "parse_errors_total": 0,
                    "sessions": 3,
                },
                {
                    "source": CHAINLINK_SESSION_SOURCE,
                    "parse_errors_total": 2,
                    "sessions": 29,
                },
                {
                    "source": CHAINLINK_SESSION_SOURCE,
                    "parse_errors_total": 3,
                    "sessions": 1,
                },
            ]

        async def close(self):
            self.closed = True

    connection = FakeConnection()
    connect_call = {}

    async def connect(**kwargs):
        connect_call.update(kwargs)
        return connection

    monkeypatch.setattr(recovery.asyncpg, "connect", connect)

    census = asyncio.run(
        recovery.load_recovery_session_census(
            "postgresql://writer@example/test"
        )
    )

    assert census == valid_parse_error_census()
    assert connect_call["server_settings"]["default_transaction_read_only"] == (
        "on"
    )
    assert connection.fetch_call == (
        recovery.RECOVERY_SESSION_CENSUS_SQL,
        (
            recovery.CALIBRATION_START_MS * 1_000_000,
            recovery.HOLDOUT_END_MS * 1_000_000,
        ),
    )
    assert connection.closed is True


def test_excluded_session_evidence_is_loaded_read_only_and_exact(monkeypatch):
    class FakeConnection:
        def __init__(self):
            self.fetchrow_call = None
            self.closed = False

        async def fetchrow(self, query, *arguments):
            self.fetchrow_call = (query, arguments)
            return valid_excluded_session_evidence()

        async def close(self):
            self.closed = True

    connection = FakeConnection()
    connect_call = {}

    async def connect(**kwargs):
        connect_call.update(kwargs)
        return connection

    monkeypatch.setattr(recovery.asyncpg, "connect", connect)

    evidence = asyncio.run(
        recovery.load_excluded_session_evidence(
            "postgresql://writer@example/test"
        )
    )

    assert evidence == valid_excluded_session_evidence()
    assert (
        connect_call["server_settings"]["default_transaction_read_only"]
        == "on"
    )
    assert connection.fetchrow_call == (
        recovery.RECOVERY_EXCLUDED_SESSION_SQL,
        (recovery.EXCLUDED_CHAINLINK_CONNECTION_ID,),
    )
    assert connection.closed is True


def test_recovery_source_provenance_pins_clean_direct_child(monkeypatch, tmp_path):
    recovery_commit = "b" * 40

    def git_stdout(_repository_path, *arguments):
        if arguments == ("rev-parse", "HEAD"):
            return recovery_commit
        if arguments == ("rev-list", "--parents", "-n", "1", "HEAD"):
            return (
                f"{recovery_commit} "
                f"{recovery.INITIAL_RECOVERY_GIT_COMMIT}"
            )
        if arguments == (
            "rev-list",
            "--parents",
            "-n",
            "1",
            recovery.INITIAL_RECOVERY_GIT_COMMIT,
        ):
            return (
                f"{recovery.INITIAL_RECOVERY_GIT_COMMIT} "
                f"{recovery.OPERATOR_RECORDED_SOURCE_GIT_COMMIT}"
            )
        if arguments == (
            "status",
            "--porcelain",
            "--untracked-files=all",
        ):
            return ""
        if arguments == (
            "diff",
            "--name-only",
            (
                f"{recovery.OPERATOR_RECORDED_SOURCE_GIT_COMMIT}.."
                f"{recovery.INITIAL_RECOVERY_GIT_COMMIT}"
            ),
        ):
            return "\n".join(recovery.EXPECTED_RECOVERY_CHANGED_PATHS)
        if arguments == (
            "diff",
            "--name-only",
            f"{recovery.INITIAL_RECOVERY_GIT_COMMIT}..HEAD",
        ):
            return "\n".join(recovery.EXPECTED_RECOVERY_FIX_CHANGED_PATHS)
        if arguments == (
            "diff",
            "--name-only",
            f"{recovery.OPERATOR_RECORDED_SOURCE_GIT_COMMIT}..HEAD",
        ):
            return "\n".join(recovery.EXPECTED_RECOVERY_CHANGED_PATHS)
        raise AssertionError(arguments)

    monkeypatch.setattr(recovery, "_git_stdout", git_stdout)

    provenance = recovery.load_recovery_source_provenance(tmp_path)

    assert provenance == recovery_source_provenance()


def test_recovery_source_provenance_rejects_later_descendant(monkeypatch, tmp_path):
    recovery_commit = "b" * 40

    def git_stdout(_repository_path, *arguments):
        if arguments == ("rev-parse", "HEAD"):
            return recovery_commit
        return f"{recovery_commit} {'c' * 40}"

    monkeypatch.setattr(recovery, "_git_stdout", git_stdout)

    with pytest.raises(recovery.RecoveryValidationError, match="direct child"):
        recovery.load_recovery_source_provenance(tmp_path)


def test_run_rejects_structurally_forged_source_provenance(tmp_path):
    provenance = recovery_source_provenance()
    forged = recovery.RecoverySourceProvenance(
        git_commit=provenance.git_commit,
        parent_git_commit=recovery.OPERATOR_RECORDED_SOURCE_GIT_COMMIT,
        original_git_commit=provenance.original_git_commit,
        changed_paths_from_parent=provenance.changed_paths_from_parent,
        changed_paths_from_original=provenance.changed_paths_from_original,
    )

    with pytest.raises(
        recovery.RecoveryValidationError,
        match="recovery parent Git commit",
    ):
        asyncio.run(
            recovery.run_posthoc_lag_recovery(
                database_url="postgresql://writer@example/test",
                original=original_artifact(tmp_path),
                recovery_source=forged,
                parse_error_census=valid_parse_error_census(),
                excluded_session_evidence=valid_excluded_session_evidence(),
            )
        )


def test_exclusive_writer_never_clobbers_result(tmp_path):
    output = tmp_path / recovery.RECOVERY_RESULT_BASENAME
    recovery.write_recovery_result_exclusive(output, {"status": "first"})
    first_bytes = output.read_bytes()

    with pytest.raises(FileExistsError):
        recovery.write_recovery_result_exclusive(output, {"status": "second"})

    assert output.read_bytes() == first_bytes
    assert not list(tmp_path.glob("*.tmp"))


def test_standard_lag_cli_has_no_recovery_parse_policy_argument():
    from price_collector.shadow_signal_lag_test import build_argument_parser

    destinations = {action.dest for action in build_argument_parser()._actions}

    assert "allowed_chainlink_parse_error_totals" not in destinations
