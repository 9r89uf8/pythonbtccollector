import argparse
import asyncio
import json
from collections import deque
from dataclasses import replace
from decimal import Decimal, localcontext
from types import SimpleNamespace
from uuid import UUID

import pytest

import price_collector.shadow_signal_replay as replay_module
from price_collector.shadow_signal import ANCHOR_HISTORY_MISSING
from price_collector.shadow_signal_replay import (
    CHAINLINK_EVENT,
    CHAINLINK_SESSION_SOURCE,
    FUTURES_EVENT,
    FUTURES_SESSION_SOURCE,
    CHAINLINK_INTEGRITY_SQL,
    FUTURES_INTEGRITY_SQL,
    REPLAY_EVENTS_SQL,
    ReplayConfig,
    ReplayDataError,
    ReplayEvent,
    ReplaySession,
    V4CausalReplayConfig,
    V4_CAUSAL_REPLAY_MODE,
    build_argument_parser,
    config_from_arguments,
    encode_replay_report,
    iter_v4_causal_origins,
    replay_from_database,
    replay_shadow_signals,
    replay_v4_causal_signals,
    select_replay_sessions,
    write_replay_report,
)
from price_collector.shadow_signal_experiment import (
    ActiveIncumbentFreeze,
    ArtifactBinding,
    ForecastCodeManifest,
    ForecastConfig,
    V4ExperimentContract,
    V4ForecastSettings,
    V4_TIMING_CELLS,
    artifact_sha256,
)


FUTURES_CONNECTION_1 = UUID("11111111-1111-1111-1111-111111111111")
CHAINLINK_CONNECTION_1 = UUID("22222222-2222-2222-2222-222222222222")
FUTURES_CONNECTION_2 = UUID("33333333-3333-3333-3333-333333333333")
CHAINLINK_CONNECTION_2 = UUID("44444444-4444-4444-4444-444444444444")
ORIGIN_MS = 1_000_000


def replay_config(**overrides):
    arguments = {
        "start_ms": ORIGIN_MS + 2_000,
        "end_ms": ORIGIN_MS + 5_001,
        "lags_ms": (1_000,),
        "beta": Decimal("1"),
        "poll_ms": 100,
        "evaluation_interval_ms": 500,
        "futures_stale_ms": 1_000,
        "chainlink_stale_ms": 5_000,
        "reference_max_gap_ms": 100,
        "history_retention_ms": 6_100,
        "neutral_band_bps": Decimal("1"),
        "volatility_lookback_ms": 2_000,
    }
    arguments.update(overrides)
    return ReplayConfig(**arguments)


def event(
    kind,
    received_ms,
    value,
    *,
    connection_id=None,
    source_timestamp_ms=None,
    received_offset_ns=0,
    sequence=None,
    event_count=1,
):
    if connection_id is None:
        connection_id = (
            FUTURES_CONNECTION_1
            if kind == FUTURES_EVENT
            else CHAINLINK_CONNECTION_1
        )
    if source_timestamp_ms is None:
        source_timestamp_ms = received_ms
    if sequence is None:
        sequence = received_ms * 10 + received_offset_ns
    return ReplayEvent(
        kind=kind,
        received_wall_ns=received_ms * 1_000_000 + received_offset_ns,
        received_monotonic_ns=received_ms * 100 + received_offset_ns + 1,
        connection_id=connection_id,
        sequence=sequence,
        source_timestamp_ms=source_timestamp_ms,
        value=Decimal(value),
        event_count=event_count,
    )


def clean_session(
    source,
    connection_id,
    *,
    start_ms=ORIGIN_MS - 100,
    end_ms=ORIGIN_MS + 6_000,
    accepted=1,
    raw_rows=None,
    raw_accepted=None,
    parse_errors=0,
    dropped=0,
    ready=True,
    completed=True,
    integrity_checked=True,
):
    if raw_rows is None:
        raw_rows = accepted
    if raw_accepted is None:
        raw_accepted = accepted
    return ReplaySession(
        connection_id=connection_id,
        source=source,
        connected_wall_ns=(start_ms - 1) * 1_000_000,
        ready_wall_ns=start_ms * 1_000_000 if ready else None,
        disconnected_wall_ns=end_ms * 1_000_000 if completed else None,
        messages_accepted_total=accepted,
        parse_errors_total=parse_errors,
        records_dropped_total=dropped,
        raw_row_count=raw_rows if integrity_checked else None,
        raw_accepted_total=raw_accepted if integrity_checked else None,
        duplicate_key_count=0 if integrity_checked else None,
        monotonic_regression_count=0 if integrity_checked else None,
        integrity_checked=integrity_checked,
    )


def base_events(*, chainlink_catchup_offset_ns=0):
    events = [
        event(FUTURES_EVENT, ORIGIN_MS, "100"),
        event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
        event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
        event(FUTURES_EVENT, ORIGIN_MS + 1_000, "100"),
        event(FUTURES_EVENT, ORIGIN_MS + 1_500, "100"),
        event(CHAINLINK_EVENT, ORIGIN_MS + 1_500, "100"),
        event(FUTURES_EVENT, ORIGIN_MS + 2_000, "110"),
        event(FUTURES_EVENT, ORIGIN_MS + 2_500, "110"),
        event(FUTURES_EVENT, ORIGIN_MS + 3_000, "110"),
        event(
            CHAINLINK_EVENT,
            ORIGIN_MS + 3_000,
            "110",
            received_offset_ns=chainlink_catchup_offset_ns,
        ),
        event(FUTURES_EVENT, ORIGIN_MS + 3_500, "110"),
        event(FUTURES_EVENT, ORIGIN_MS + 4_000, "110"),
        event(FUTURES_EVENT, ORIGIN_MS + 4_500, "110"),
        event(FUTURES_EVENT, ORIGIN_MS + 5_000, "110"),
    ]
    return sorted(events, key=lambda item: item.sort_key)


def sessions_for(events, **overrides):
    futures_events = [item for item in events if item.kind == FUTURES_EVENT]
    chainlink_events = [item for item in events if item.kind == CHAINLINK_EVENT]
    return [
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_1,
            accepted=sum(item.event_count for item in futures_events),
            raw_rows=len(futures_events),
            raw_accepted=sum(item.event_count for item in futures_events),
            **overrides,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_1,
            accepted=len(chainlink_events),
            raw_rows=len(chainlink_events),
            raw_accepted=len(chainlink_events),
            **overrides,
        ),
    ]


def only_candidate(report):
    assert len(report.candidate_summaries) == 1
    return report.candidate_summaries[0]


def v4_code_manifest(prefix="a"):
    characters = {
        "a": ("a", "b", "c", "d"),
        "e": ("e", "f", "0", "1"),
    }[prefix]
    return ForecastCodeManifest(
        anchor_formation_sha256=characters[0] * 64,
        futures_reference_selection_sha256=characters[1] * 64,
        projection_sha256=characters[2] * 64,
        forecast_validity_sha256=characters[3] * 64,
    )


def v4_binding(artifact_type, character):
    return ArtifactBinding(
        artifact_type=artifact_type,
        schema_version=1,
        sha256=character * 64,
    )


def v4_settings(**overrides):
    arguments = {
        "futures_stale_ms": 1_000,
        "chainlink_stale_ms": 5_000,
        "history_retention_ms": 10_000,
    }
    arguments.update(overrides)
    return V4ForecastSettings(**arguments)


def v4_incumbent(*, config=None, code=None):
    frozen_code = code or v4_code_manifest()
    frozen_config = config or v4_settings().config_for_lag(3_000)
    return ActiveIncumbentFreeze(
        selection_sha256="2" * 64,
        replay_config_sha256="3" * 64,
        primary_model_version="catchup_ratio_l3000_b100",
        forecast_config=frozen_config,
        forecast_code=frozen_code,
        loaded_runtime_identity_sha256="7" * 64,
        installed_runtime_identity_sha256="7" * 64,
        invocation_start=v4_binding("active_invocation_start_record", "8"),
        selection_artifact=v4_binding("active_incumbent_selection", "2"),
        replay_config_artifact=v4_binding(
            "active_incumbent_replay_configuration",
            "3",
        ),
        forecast_code_manifest_artifact=ArtifactBinding(
            artifact_type="active_forecast_code_manifest",
            schema_version=1,
            sha256=artifact_sha256(
                frozen_code.to_artifact_dict("active_forecast_code_manifest")
            ),
        ),
        reconstruction_report=v4_binding(
            "active_forecast_reconstruction_report",
            "6",
        ),
    )


def v4_contract(*, settings_value=None, incumbent_value=None, code=None):
    frozen_code = code or v4_code_manifest()
    return V4ExperimentContract(
        forecast_settings=settings_value or v4_settings(),
        v4_forecast_code=frozen_code,
        v4_forecast_code_manifest_artifact=ArtifactBinding(
            artifact_type="v4_forecast_code_manifest",
            schema_version=1,
            sha256=artifact_sha256(
                frozen_code.to_artifact_dict("v4_forecast_code_manifest")
            ),
        ),
        active_incumbent=incumbent_value or v4_incumbent(code=frozen_code),
    )


V4_GENERATED_MS = ORIGIN_MS + 10_000


def v4_replay_config(**overrides):
    arguments = {
        "scoring_start_ms": V4_GENERATED_MS,
        "scoring_end_ms": V4_GENERATED_MS + 3_701,
        "contract": v4_contract(),
        "timing_cell": V4_TIMING_CELLS[0],
    }
    arguments.update(overrides)
    return V4CausalReplayConfig(**arguments)


def exact_event(
    kind,
    *,
    wall_ns,
    value,
    monotonic_ns,
    source_sequence,
    source_timestamp_ms=None,
    connection_id=None,
):
    if connection_id is None:
        connection_id = (
            FUTURES_CONNECTION_1
            if kind == FUTURES_EVENT
            else CHAINLINK_CONNECTION_1
        )
    if source_timestamp_ms is None:
        source_timestamp_ms = wall_ns // 1_000_000
    return ReplayEvent(
        kind=kind,
        received_wall_ns=wall_ns,
        received_monotonic_ns=monotonic_ns,
        connection_id=connection_id,
        sequence=source_sequence,
        source_timestamp_ms=source_timestamp_ms,
        value=Decimal(value),
    )


def canonical_v4_events(*, generated_ms=V4_GENERATED_MS):
    anchor_ms = generated_ms - 100
    values = []
    sequence = 1
    for lag_ms in reversed((1_500, 2_000, 2_500, 3_000, 3_500)):
        reference_ms = anchor_ms - lag_ms
        values.append(
            exact_event(
                FUTURES_EVENT,
                wall_ns=reference_ms * 1_000_000,
                value="100",
                monotonic_ns=sequence,
                source_sequence=sequence,
            )
        )
        sequence += 1
    values.append(
        exact_event(
            FUTURES_EVENT,
            wall_ns=(generated_ms - 200) * 1_000_000,
            value="100",
            monotonic_ns=sequence,
            source_sequence=sequence,
        )
    )
    sequence += 1
    values.extend(
        [
            exact_event(
                FUTURES_EVENT,
                wall_ns=anchor_ms * 1_000_000,
                value="110",
                monotonic_ns=sequence,
                source_sequence=sequence,
            ),
            exact_event(
                CHAINLINK_EVENT,
                wall_ns=anchor_ms * 1_000_000,
                value="100",
                monotonic_ns=sequence + 1,
                source_sequence=sequence + 1,
            ),
        ]
    )
    sequence += 2
    for lag_ms, actual in zip(
        (1_500, 2_000, 2_500, 3_000, 3_500),
        ("115", "120", "125", "130", "135"),
    ):
        actual_receipt_ms = generated_ms + lag_ms - 100
        values.append(
            exact_event(
                CHAINLINK_EVENT,
                wall_ns=actual_receipt_ms * 1_000_000,
                value=actual,
                monotonic_ns=sequence,
                source_sequence=sequence,
            )
        )
        sequence += 1
    return sorted(values, key=lambda item: item.sort_key)


def v4_sessions(events, config, *, segments=None):
    if segments is None:
        segments = (
            (
                config.archive_input_start_ms * 1_000_000,
                config.archive_input_end_ms * 1_000_000,
                FUTURES_CONNECTION_1,
                CHAINLINK_CONNECTION_1,
            ),
        )
    sessions = []
    for start_ns, end_ns, futures_id, chainlink_id in segments:
        for source, kind, connection_id in (
            (FUTURES_SESSION_SOURCE, FUTURES_EVENT, futures_id),
            (CHAINLINK_SESSION_SOURCE, CHAINLINK_EVENT, chainlink_id),
        ):
            source_events = [
                item
                for item in events
                if item.kind == kind
                and item.connection_id == connection_id
                and start_ns <= item.received_wall_ns < end_ns
            ]
            accepted = sum(item.event_count for item in source_events)
            sessions.append(
                ReplaySession(
                    connection_id=connection_id,
                    source=source,
                    connected_wall_ns=max(1, start_ns - 1),
                    ready_wall_ns=start_ns,
                    disconnected_wall_ns=end_ns,
                    messages_accepted_total=accepted,
                    parse_errors_total=0,
                    records_dropped_total=0,
                    raw_row_count=len(source_events),
                    raw_accepted_total=accepted,
                    duplicate_key_count=0,
                    monotonic_regression_count=0,
                    wall_regression_count=0,
                    out_of_session_count=0,
                    integrity_checked=True,
                )
            )
    return sessions


def test_v4_causal_replay_uses_the_frozen_family_and_horizon_actuals():
    config = v4_replay_config()
    events = canonical_v4_events()

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )

    assert result.to_dict()["mode"] == V4_CAUSAL_REPLAY_MODE
    assert result.to_dict()["losses_materialized"] is False
    assert result.scheduled_origin_vector == tuple(
        range(V4_GENERATED_MS, V4_GENERATED_MS + 3_701, 500)
    )
    assert result.target_eligible_mask == (
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
    )
    origin = result.origins[0]
    assert origin.generation_eligible is True
    assert origin.common_scored is True
    assert origin.decision_eligible is True
    assert [item.lag_ms for item in origin.candidate_attempts] == [
        1_500,
        2_000,
        2_500,
        3_000,
        3_500,
    ]
    assert [item.projected_chainlink for item in origin.candidate_attempts] == [
        Decimal("110")
    ] * 5
    assert [
        item.matched_no_change_prediction for item in origin.candidate_attempts
    ] == [Decimal("100")] * 5
    assert [
        item.actual_chainlink.value for item in origin.candidate_attempts
    ] == [
        Decimal("115"),
        Decimal("120"),
        Decimal("125"),
        Decimal("130"),
        Decimal("135"),
    ]
    assert origin.control_attempt.identity.model_role == (
        "offline_replay_replacement_control"
    )
    assert origin.control_attempt.identity != origin.candidate_attempts[3].identity
    assert origin.control_attempt.projected_chainlink == (
        origin.candidate_attempts[3].projected_chainlink
    )
    assert origin.control_attempt.actual_chainlink == (
        origin.candidate_attempts[3].actual_chainlink
    )


def test_v4_observation_preserves_delivery_identity_without_live_metadata():
    config = v4_replay_config()
    events = canonical_v4_events()
    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )

    attempt = result.origins[0].candidate_attempts[0]
    observations = (
        attempt.chainlink_anchor,
        attempt.futures_now,
        attempt.futures_reference,
        attempt.actual_chainlink,
    )
    assert all(item is not None for item in observations)
    for observation in observations:
        payload = observation.to_dict()
        assert isinstance(payload["value"], Decimal)
        assert payload["received_ms"] == (
            payload["received_wall_ns"] // 1_000_000
        )
        assert payload["publisher_epoch"] is None
        assert payload["accepted_event_sequence"] is None
        assert payload["publisher_epoch_capture"] == "not_captured"
        assert payload["accepted_event_sequence_capture"] == "not_captured"
        assert payload["source_sequence"] > 0


def test_legacy_replay_remains_schema_v3_and_does_not_infer_v4():
    events = base_events()
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(),
    )

    assert report.to_dict()["schema_version"] == 3
    assert report.to_dict()["mode"] == "shadow_raw_replay"
    assert "contract_digest" not in report.to_dict()


@pytest.mark.parametrize(
    ("offset_ns", "expected_projection"),
    [(0, Decimal("110")), (1, Decimal("100"))],
)
def test_v4_forecast_input_uses_exact_ceiling_visibility(
    offset_ns,
    expected_projection,
):
    config = v4_replay_config()
    events = canonical_v4_events()
    current_wall_ns = (V4_GENERATED_MS - 100) * 1_000_000
    current_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == FUTURES_EVENT
        and item.received_wall_ns == current_wall_ns
        and item.value == Decimal("110")
    )
    current = events[current_index]
    events[current_index] = replace(
        current,
        received_wall_ns=current.received_wall_ns + offset_ns,
    )
    events.sort(key=lambda item: item.sort_key)

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )

    first = result.origins[0]
    assert first.candidate_attempts[0].projected_chainlink == expected_projection
    if offset_ns:
        assert first.candidate_attempts[0].futures_now.received_ms == (
            V4_GENERATED_MS - 200
        )
    else:
        assert first.candidate_attempts[0].futures_now.received_ms == (
            V4_GENERATED_MS - 100
        )


@pytest.mark.parametrize(
    ("timing_cell", "expected_generated_ms", "expected_projection", "valid"),
    [
        (V4_TIMING_CELLS[0], V4_GENERATED_MS, Decimal("110"), True),
        (V4_TIMING_CELLS[1], V4_GENERATED_MS + 100, Decimal("110"), True),
        (V4_TIMING_CELLS[2], V4_GENERATED_MS + 200, Decimal("110"), True),
        (V4_TIMING_CELLS[3], V4_GENERATED_MS + 300, Decimal("110"), True),
        (V4_TIMING_CELLS[4], V4_GENERATED_MS + 400, Decimal("110"), True),
        (V4_TIMING_CELLS[5], V4_GENERATED_MS, Decimal("100"), True),
        (V4_TIMING_CELLS[6], V4_GENERATED_MS, None, False),
    ],
)
def test_v4_uses_every_frozen_phase_and_source_delay_cell(
    timing_cell,
    expected_generated_ms,
    expected_projection,
    valid,
):
    config = v4_replay_config(
        scoring_end_ms=V4_GENERATED_MS + 4_201,
        timing_cell=timing_cell,
    )
    events = canonical_v4_events()

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )

    first = result.origins[0]
    assert first.generated_ms == expected_generated_ms
    assert first.target_eligible is True
    assert first.generation_eligible is valid
    assert all(attempt.valid is valid for attempt in first.candidate_attempts)
    assert first.candidate_attempts[0].projected_chainlink == expected_projection


@pytest.mark.parametrize(
    ("offset_ns", "expected_actual"),
    [(0, Decimal("115")), (1, Decimal("100"))],
)
def test_v4_short_horizon_actual_cannot_leak_from_later_confirmation(
    offset_ns,
    expected_actual,
):
    config = v4_replay_config()
    events = canonical_v4_events()
    target_ms = V4_GENERATED_MS + 1_500
    actual_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == CHAINLINK_EVENT
        and item.value == Decimal("115")
    )
    actual = events[actual_index]
    events[actual_index] = replace(
        actual,
        received_wall_ns=actual.received_wall_ns + offset_ns,
    )
    events.sort(key=lambda item: item.sort_key)

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )

    selected = result.origins[0].candidate_attempts[0].actual_chainlink
    assert selected.value == expected_actual
    assert selected.visible_ms <= target_ms
    assert selected.received_wall_ns <= target_ms * 1_000_000


@pytest.mark.parametrize(
    ("reference_gap_ms", "expected_valid"),
    [(0, True), (250, True), (251, False)],
)
def test_v4_reference_gap_boundaries_are_inclusive(
    reference_gap_ms,
    expected_valid,
):
    config = v4_replay_config()
    events = canonical_v4_events()
    anchor_ms = V4_GENERATED_MS - 100
    reference_targets = {
        anchor_ms - lag_ms for lag_ms in (1_500, 2_000, 2_500, 3_000, 3_500)
    }
    for index, item in enumerate(events):
        if item.kind == FUTURES_EVENT and item.received_ms in reference_targets:
            events[index] = replace(
                item,
                received_wall_ns=(
                    item.received_ms - reference_gap_ms
                )
                * 1_000_000,
                source_timestamp_ms=item.source_timestamp_ms - reference_gap_ms,
            )
    events.sort(key=lambda item: item.sort_key)

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )
    attempts = result.origins[0].candidate_attempts

    assert [item.valid for item in attempts] == [expected_valid] * 5
    if expected_valid:
        assert [item.futures_reference_gap_ms for item in attempts] == [
            reference_gap_ms
        ] * 5
    else:
        assert {item.status for item in attempts} == {"anchor_reference_gap"}
        assert result.origins[0].generation_eligible is False


@pytest.mark.parametrize(
    ("received_age_ms", "expected_valid"),
    [(100, True), (101, False)],
)
def test_v4_staleness_uses_floored_receipt_ms_with_inclusive_threshold(
    received_age_ms,
    expected_valid,
):
    settings_value = v4_settings(futures_stale_ms=100)
    frozen_code = v4_code_manifest()
    contract = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(
            config=settings_value.config_for_lag(3_000),
            code=frozen_code,
        ),
        code=frozen_code,
    )
    config = v4_replay_config(contract=contract)
    events = canonical_v4_events()
    current_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == FUTURES_EVENT and item.value == Decimal("110")
    )
    current = events[current_index]
    receipt_offset_ns = 0 if received_age_ms == 100 else 999_999
    events[current_index] = replace(
        current,
        received_wall_ns=(
            V4_GENERATED_MS - received_age_ms
        )
        * 1_000_000
        + receipt_offset_ns,
        source_timestamp_ms=V4_GENERATED_MS - received_age_ms,
    )
    events.sort(key=lambda item: item.sort_key)

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )
    attempts = result.origins[0].candidate_attempts

    assert [item.valid for item in attempts] == [expected_valid] * 5
    if not expected_valid:
        assert {item.status for item in attempts} == {"futures_stale"}


def test_v4_stale_chainlink_event_clears_the_existing_anchor():
    settings_value = v4_settings(chainlink_stale_ms=100)
    contract = v4_contract(settings_value=settings_value)
    forecast_config = settings_value.config_for_lag(3_000)
    identity = contract.candidate_identity(3_000)
    machine = replay_module._V4ForecastMachine(((forecast_config, identity),))
    reference = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=V4_GENERATED_MS - 3_200,
        value="100",
        visible_ms=V4_GENERATED_MS - 3_200,
        sequence=1,
    )
    current = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=V4_GENERATED_MS - 200,
        value="110",
        visible_ms=V4_GENERATED_MS - 100,
        sequence=2,
    )
    fresh_anchor = v4_visible_observation(
        CHAINLINK_EVENT,
        received_ms=V4_GENERATED_MS - 200,
        value="100",
        visible_ms=V4_GENERATED_MS - 100,
        sequence=3,
    )
    stale_chainlink = v4_visible_observation(
        CHAINLINK_EVENT,
        received_ms=V4_GENERATED_MS - 101,
        value="101",
        visible_ms=V4_GENERATED_MS,
        sequence=4,
    )

    machine.apply_poll(
        futures_events=(reference,),
        chainlink_events=(),
        now_ms=V4_GENERATED_MS - 3_200,
    )
    machine.apply_poll(
        futures_events=(current,),
        chainlink_events=(fresh_anchor,),
        now_ms=V4_GENERATED_MS - 100,
    )
    (before,) = machine.forecast_attempts(
        latest_futures=current,
        latest_chainlink=fresh_anchor,
        generated_ms=V4_GENERATED_MS - 100,
    )
    machine.apply_poll(
        futures_events=(),
        chainlink_events=(stale_chainlink,),
        now_ms=V4_GENERATED_MS,
    )
    (after,) = machine.forecast_attempts(
        latest_futures=current,
        latest_chainlink=stale_chainlink,
        generated_ms=V4_GENERATED_MS,
    )

    assert before.valid is True
    assert after.valid is False
    assert after.status == "chainlink_stale"
    assert after.chainlink_anchor is None
    assert after.projected_chainlink is None


def test_v4_stale_futures_event_is_not_added_to_reference_history():
    settings_value = v4_settings(futures_stale_ms=100)
    contract = v4_contract(settings_value=settings_value)
    forecast_config = settings_value.config_for_lag(3_000)
    identity = contract.candidate_identity(3_000)
    machine = replay_module._V4ForecastMachine(((forecast_config, identity),))
    anchor_received_ms = V4_GENERATED_MS - 100
    reference_target_ms = anchor_received_ms - 3_000
    fallback = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_target_ms - 251,
        value="99",
        visible_ms=reference_target_ms - 200,
        sequence=1,
    )
    stale_exact_reference = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_target_ms,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=2,
    )
    anchor = v4_visible_observation(
        CHAINLINK_EVENT,
        received_ms=anchor_received_ms,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=3,
    )

    machine.apply_poll(
        futures_events=(fallback,),
        chainlink_events=(),
        now_ms=reference_target_ms - 200,
    )
    machine.apply_poll(
        futures_events=(stale_exact_reference,),
        chainlink_events=(anchor,),
        now_ms=V4_GENERATED_MS,
    )
    (attempt,) = machine.forecast_attempts(
        latest_futures=stale_exact_reference,
        latest_chainlink=anchor,
        generated_ms=V4_GENERATED_MS,
    )

    assert attempt.valid is False
    assert "futures_stale" in attempt.invalid_reasons
    assert "anchor_reference_gap" in attempt.invalid_reasons
    assert attempt.futures_reference is None
    assert attempt.futures_reference_gap_ms == 251


def test_v4_anchor_keeps_its_reference_across_later_futures_updates():
    config = v4_replay_config(scoring_end_ms=V4_GENERATED_MS + 4_201)
    events = canonical_v4_events()
    events.append(
        exact_event(
            FUTURES_EVENT,
            wall_ns=(V4_GENERATED_MS + 400) * 1_000_000,
            value="120",
            monotonic_ns=99_001,
            source_sequence=99_001,
        )
    )
    events.sort(key=lambda item: item.sort_key)

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )
    first, second = result.origins[:2]

    assert first.target_eligible is True
    assert second.target_eligible is True
    assert first.candidate_attempts[0].futures_reference.identity == (
        second.candidate_attempts[0].futures_reference.identity
    )
    assert second.candidate_attempts[0].projected_chainlink == Decimal("120")


@pytest.mark.parametrize("regressed_kind", [FUTURES_EVENT, CHAINLINK_EVENT])
def test_v4_source_timestamp_regression_invalidates_only_its_generation(
    regressed_kind,
):
    config = v4_replay_config(
        scoring_end_ms=V4_GENERATED_MS + 4_201,
    )
    events = canonical_v4_events()
    events.append(
        exact_event(
            regressed_kind,
            wall_ns=(V4_GENERATED_MS + 400) * 1_000_000,
            value="111",
            monotonic_ns=99_010,
            source_sequence=99_010,
            source_timestamp_ms=V4_GENERATED_MS - 101,
        )
    )
    events.sort(key=lambda item: item.sort_key)

    result = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    )

    pending_before_regression = result.origins[0]
    regression_origin = result.origins[1]
    assert pending_before_regression.generation_eligible is True
    assert pending_before_regression.common_scored is True
    assert pending_before_regression.integrity_reset_before_finalization is False
    assert regression_origin.target_eligible is True
    assert {item.status for item in regression_origin.candidate_attempts} == {
        "timestamp_regression"
    }
    assert regression_origin.generation_eligible is False
    assert regression_origin.common_scored is False


def test_v3_replay_retains_timestamp_regression_behavior():
    events = base_events()
    regression_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == FUTURES_EVENT
        and item.received_ms == ORIGIN_MS + 2_000
    )
    events[regression_index] = replace(
        events[regression_index],
        source_timestamp_ms=ORIGIN_MS + 1_499,
    )
    events.sort(key=lambda item: item.sort_key)

    candidate = only_candidate(
        replay_shadow_signals(
            events=events,
            sessions=sessions_for(events),
            config=replay_config(),
        )
    )

    assert candidate["invalid_statuses"]["timestamp_regression"] == 1
    assert "timestamp_regression" in candidate["invalid_reasons"]


def test_v4_session_reset_is_future_blind_then_invalidates_the_full_cohort():
    continuous_config = v4_replay_config()
    events = canonical_v4_events()
    continuous = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, continuous_config),
        config=continuous_config,
    ).origins[0]

    reset_ns = (V4_GENERATED_MS + 2_000) * 1_000_000
    reset_events = []
    for item in events:
        if item.received_wall_ns >= reset_ns:
            replacement_connection = (
                FUTURES_CONNECTION_2
                if item.kind == FUTURES_EVENT
                else CHAINLINK_CONNECTION_2
            )
            item = replace(item, connection_id=replacement_connection)
        reset_events.append(item)
    reset_events.sort(key=lambda item: item.sort_key)
    segments = (
        (
            continuous_config.archive_input_start_ms * 1_000_000,
            reset_ns,
            FUTURES_CONNECTION_1,
            CHAINLINK_CONNECTION_1,
        ),
        (
            reset_ns,
            continuous_config.archive_input_end_ms * 1_000_000,
            FUTURES_CONNECTION_2,
            CHAINLINK_CONNECTION_2,
        ),
    )
    reset = replay_v4_causal_signals(
        events=reset_events,
        sessions=v4_sessions(
            reset_events,
            continuous_config,
            segments=segments,
        ),
        config=continuous_config,
    ).origins[0]

    assert reset.target_eligible == continuous.target_eligible is True
    assert reset.generation_eligible == continuous.generation_eligible is True
    assert continuous.common_scored is True
    assert reset.common_scored is False
    assert reset.decision_eligible is False
    assert reset.integrity_reset_before_finalization is True
    assert reset.missing_reasons == ("integrity_reset_before_finalization",)
    assert all(item.actual_chainlink is None for item in reset.candidate_attempts)


@pytest.mark.parametrize("reset_kind", [FUTURES_EVENT, CHAINLINK_EVENT])
def test_v4_each_source_reconnect_invalidates_a_pending_full_cohort(reset_kind):
    config = v4_replay_config()
    reset_ms = V4_GENERATED_MS + 2_000
    start_ms = config.archive_input_start_ms
    end_ms = config.archive_input_end_ms
    replacement_connection = (
        FUTURES_CONNECTION_2
        if reset_kind == FUTURES_EVENT
        else CHAINLINK_CONNECTION_2
    )
    events = [
        replace(item, connection_id=replacement_connection)
        if item.kind == reset_kind and item.received_ms >= reset_ms
        else item
        for item in canonical_v4_events()
    ]
    events.sort(key=lambda item: item.sort_key)
    sessions = []
    for source, kind, original_connection in (
        (FUTURES_SESSION_SOURCE, FUTURES_EVENT, FUTURES_CONNECTION_1),
        (CHAINLINK_SESSION_SOURCE, CHAINLINK_EVENT, CHAINLINK_CONNECTION_1),
    ):
        session_ranges = (
            (
                (start_ms, reset_ms, original_connection),
                (reset_ms, end_ms, replacement_connection),
            )
            if kind == reset_kind
            else ((start_ms, end_ms, original_connection),)
        )
        for session_start_ms, session_end_ms, connection_id in session_ranges:
            session_events = [
                item
                for item in events
                if item.kind == kind
                and item.connection_id == connection_id
                and session_start_ms <= item.received_ms < session_end_ms
            ]
            accepted = sum(item.event_count for item in session_events)
            sessions.append(
                clean_session(
                    source,
                    connection_id,
                    start_ms=session_start_ms,
                    end_ms=session_end_ms,
                    accepted=accepted,
                    raw_rows=len(session_events),
                    raw_accepted=accepted,
                )
            )

    origin = replay_v4_causal_signals(
        events=events,
        sessions=sessions,
        config=config,
    ).origins[0]

    assert origin.target_eligible is True
    assert origin.generation_eligible is True
    assert origin.common_scored is False
    assert origin.decision_eligible is False
    assert origin.integrity_reset_before_finalization is True
    assert origin.missing_reasons == ("integrity_reset_before_finalization",)


@pytest.mark.parametrize(
    ("boundary_offset_ns", "expected_common_scored"),
    [(0, False), (1, True)],
)
def test_v4_session_reset_boundary_is_half_open_at_finalization(
    boundary_offset_ns,
    expected_common_scored,
):
    config = v4_replay_config()
    events = canonical_v4_events()
    finalization_ns = (V4_GENERATED_MS + 3_700) * 1_000_000
    boundary_ns = finalization_ns + boundary_offset_ns
    segments = (
        (
            config.archive_input_start_ms * 1_000_000,
            boundary_ns,
            FUTURES_CONNECTION_1,
            CHAINLINK_CONNECTION_1,
        ),
        (
            boundary_ns,
            config.archive_input_end_ms * 1_000_000,
            FUTURES_CONNECTION_2,
            CHAINLINK_CONNECTION_2,
        ),
    )

    origin = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config, segments=segments),
        config=config,
    ).origins[0]

    assert origin.common_scored is expected_common_scored
    assert origin.integrity_reset_before_finalization is (
        not expected_common_scored
    )


def test_v4_distinct_control_can_reject_without_changing_common_cohort():
    settings_value = v4_settings()
    frozen_code = v4_code_manifest()
    control_config = replace(
        settings_value.config_for_lag(3_000),
        futures_stale_ms=50,
    )
    contract = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(config=control_config, code=frozen_code),
        code=frozen_code,
    )
    config = v4_replay_config(contract=contract)
    events = canonical_v4_events()

    origin = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    ).origins[0]

    assert origin.generation_eligible is True
    assert origin.common_scored is True
    assert origin.control_attempt.valid is False
    assert origin.control_attempt.status == "futures_stale"
    assert origin.decision_eligible is False
    assert origin.missing_reasons == ("control_forecast_invalid:futures_stale",)


def test_v4_distinct_control_with_unavailable_code_fails_closed():
    settings_value = v4_settings()
    active_code = v4_code_manifest("e")
    contract = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(
            config=replace(
                settings_value.config_for_lag(3_000),
                futures_stale_ms=999,
            ),
            code=active_code,
        ),
        code=v4_code_manifest(),
    )
    config = v4_replay_config(contract=contract)

    with pytest.raises(
        ReplayDataError,
        match="manifest-verified reconstructed executor",
    ):
        replay_v4_causal_signals(events=(), sessions=(), config=config)


def test_v4_global_lattice_retains_origins_when_sessions_are_missing():
    config = v4_replay_config(
        scoring_start_ms=V4_GENERATED_MS + 1,
        scoring_end_ms=V4_GENERATED_MS + 5_001,
    )

    result = replay_v4_causal_signals(events=(), sessions=(), config=config)

    assert result.scheduled_origin_vector == tuple(
        range(V4_GENERATED_MS + 500, V4_GENERATED_MS + 5_001, 500)
    )
    assert V4_GENERATED_MS + 1 not in result.scheduled_origin_vector
    assert len(result.origins) == 10
    assert result.target_eligible_origin_vector == (
        V4_GENERATED_MS + 500,
        V4_GENERATED_MS + 1_000,
        V4_GENERATED_MS + 1_500,
    )
    assert result.generation_eligible_mask == (False, False, False)
    assert all(
        "session_unavailable_at_generation" in item.missing_reasons
        for item in result.origins
        if item.target_eligible
    )


def test_v4_same_source_raw_ties_use_sequence_for_latest_cache_value():
    config = v4_replay_config()
    events = canonical_v4_events()
    wall_ns = (V4_GENERATED_MS - 100) * 1_000_000
    original_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == FUTURES_EVENT
        and item.received_wall_ns == wall_ns
        and item.value == Decimal("110")
    )
    original = events[original_index]
    events[original_index] = replace(
        original,
        value=Decimal("110"),
        received_monotonic_ns=99_100,
        sequence=99_100,
    )
    events.append(
        replace(
            original,
            value=Decimal("120"),
            received_monotonic_ns=99_100,
            sequence=99_101,
        )
    )
    events.sort(key=lambda item: item.sort_key)

    origin = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    ).origins[0]

    assert [item.projected_chainlink for item in origin.candidate_attempts] == [
        Decimal("120")
    ] * 5
    assert {item.futures_now.source_sequence for item in origin.candidate_attempts} == {
        99_101
    }


def v4_visible_observation(
    kind,
    *,
    received_ms,
    value,
    visible_ms,
    sequence,
):
    return replay_module.V4VisibleObservation(
        kind=kind,
        value=Decimal(value),
        received_wall_ns=received_ms * 1_000_000,
        received_monotonic_ns=sequence,
        available_wall_ns=visible_ms * 1_000_000,
        visible_ms=visible_ms,
        source_timestamp_ms=received_ms,
        connection_id=(
            FUTURES_CONNECTION_1
            if kind == FUTURES_EVENT
            else CHAINLINK_CONNECTION_1
        ),
        source_sequence=sequence,
    )


def test_v4_same_poll_reference_rule_matches_the_existing_engine():
    settings_value = v4_settings(futures_stale_ms=3_100)
    frozen = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(
            config=settings_value.config_for_lag(3_000),
        ),
    )
    forecast_config = settings_value.config_for_lag(3_000)
    identity = frozen.candidate_identity(3_000)
    machine = replay_module._V4ForecastMachine(((forecast_config, identity),))
    anchor_received_ms = V4_GENERATED_MS - 100
    reference_received_ms = anchor_received_ms - 3_000
    prior = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_received_ms - 100,
        value="99",
        visible_ms=V4_GENERATED_MS - 100,
        sequence=1,
    )
    reference = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_received_ms,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=2,
    )
    anchor = v4_visible_observation(
        CHAINLINK_EVENT,
        received_ms=anchor_received_ms,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=3,
    )
    machine.apply_poll(
        futures_events=(prior,),
        chainlink_events=(),
        now_ms=V4_GENERATED_MS - 100,
    )
    machine.apply_poll(
        futures_events=(reference,),
        chainlink_events=(anchor,),
        now_ms=V4_GENERATED_MS,
    )
    (attempt,) = machine.forecast_attempts(
        latest_futures=reference,
        latest_chainlink=anchor,
        generated_ms=V4_GENERATED_MS,
    )

    legacy = replay_module.ShadowSignalEngine(
        models=(
            replay_module.CatchupModel(
                version=identity.model_version,
                lag_ms=3_000,
                beta=Decimal("1"),
            ),
        ),
        futures_stale_ms=3_100,
        chainlink_stale_ms=5_000,
        reference_max_gap_ms=250,
        history_retention_ms=10_000,
        max_future_skew_ms=0,
    )
    legacy.observe(
        futures=prior.observed_price,
        chainlink=None,
        now_ms=V4_GENERATED_MS - 100,
    )
    legacy_signal = legacy.observe(
        futures=reference.observed_price,
        chainlink=anchor.observed_price,
        now_ms=V4_GENERATED_MS,
    ).signals[0]

    assert attempt.valid == legacy_signal.valid is True
    assert attempt.futures_reference.identity == reference.identity
    assert attempt.futures_reference_gap_ms == (
        legacy_signal.futures_reference_gap_ms
    ) == 0
    assert attempt.projected_chainlink == (
        legacy_signal.projection.projected_chainlink
    )

    no_history = replay_module._V4ForecastMachine(
        ((forecast_config, identity),)
    )
    no_history.apply_poll(
        futures_events=(reference,),
        chainlink_events=(anchor,),
        now_ms=V4_GENERATED_MS,
    )
    (invalid_attempt,) = no_history.forecast_attempts(
        latest_futures=reference,
        latest_chainlink=anchor,
        generated_ms=V4_GENERATED_MS,
    )
    assert invalid_attempt.valid is False
    assert invalid_attempt.status == "anchor_history_missing"


def test_v4_event_complete_history_keeps_intermediate_causal_reference():
    settings_value = v4_settings(futures_stale_ms=4_000)
    frozen = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(
            config=settings_value.config_for_lag(3_000),
        ),
    )
    forecast_config = settings_value.config_for_lag(3_000)
    identity = frozen.candidate_identity(3_000)
    machine = replay_module._V4ForecastMachine(((forecast_config, identity),))
    anchor_received_ms = V4_GENERATED_MS - 100
    reference_target_ms = anchor_received_ms - 3_000
    fallback = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_target_ms - 251,
        value="99",
        visible_ms=V4_GENERATED_MS - 100,
        sequence=1,
    )
    overwritten = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_target_ms,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=2,
    )
    cache_winner = v4_visible_observation(
        FUTURES_EVENT,
        received_ms=reference_target_ms + 25,
        value="101",
        visible_ms=V4_GENERATED_MS,
        sequence=3,
    )
    anchor = v4_visible_observation(
        CHAINLINK_EVENT,
        received_ms=anchor_received_ms,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=4,
    )
    machine.apply_poll(
        futures_events=(fallback,),
        chainlink_events=(),
        now_ms=V4_GENERATED_MS - 100,
    )
    machine.apply_poll(
        futures_events=(overwritten, cache_winner),
        chainlink_events=(anchor,),
        now_ms=V4_GENERATED_MS,
    )
    (attempt,) = machine.forecast_attempts(
        latest_futures=cache_winner,
        latest_chainlink=anchor,
        generated_ms=V4_GENERATED_MS,
    )

    assert attempt.valid is True
    assert attempt.futures_reference.identity == overwritten.identity
    assert attempt.futures_reference_gap_ms == 0
    assert attempt.futures_now.identity == cache_winner.identity
    assert attempt.projected_chainlink == Decimal("101")


def test_v4_event_complete_poll_preserves_intermediate_timestamp_regression():
    config = v4_replay_config()
    models = tuple(
        (
            forecast_config,
            config.contract.candidate_identity(forecast_config.lag_ms),
        )
        for forecast_config in config.candidate_configs
    )
    machine = replay_module._V4ForecastMachine(models)
    prior = replace(
        v4_visible_observation(
            FUTURES_EVENT,
            received_ms=V4_GENERATED_MS - 4_000,
            value="99",
            visible_ms=V4_GENERATED_MS - 100,
            sequence=1,
        ),
        source_timestamp_ms=V4_GENERATED_MS,
    )
    regressed = replace(
        v4_visible_observation(
            FUTURES_EVENT,
            received_ms=V4_GENERATED_MS - 3_000,
            value="100",
            visible_ms=V4_GENERATED_MS,
            sequence=2,
        ),
        source_timestamp_ms=V4_GENERATED_MS - 1,
    )
    cache_winner = replace(
        v4_visible_observation(
            FUTURES_EVENT,
            received_ms=V4_GENERATED_MS - 2_900,
            value="101",
            visible_ms=V4_GENERATED_MS,
            sequence=3,
        ),
        source_timestamp_ms=V4_GENERATED_MS + 1,
    )
    chainlink = v4_visible_observation(
        CHAINLINK_EVENT,
        received_ms=V4_GENERATED_MS - 100,
        value="100",
        visible_ms=V4_GENERATED_MS,
        sequence=4,
    )

    machine.apply_poll(
        futures_events=(prior,),
        chainlink_events=(),
        now_ms=V4_GENERATED_MS - 100,
    )
    machine.apply_poll(
        futures_events=(regressed, cache_winner),
        chainlink_events=(chainlink,),
        now_ms=V4_GENERATED_MS,
    )
    attempts = machine.forecast_attempts(
        latest_futures=cache_winner,
        latest_chainlink=chainlink,
        generated_ms=V4_GENERATED_MS,
    )

    assert len(attempts) == 5
    assert {attempt.status for attempt in attempts} == {
        "timestamp_regression"
    }
    assert all(attempt.valid is False for attempt in attempts)


@pytest.mark.parametrize(
    ("scoring_end_offset_ms", "expected_target_eligible"),
    [(3_500, False), (3_501, True)],
)
def test_v4_maximum_horizon_tail_rule_is_strict(
    scoring_end_offset_ms,
    expected_target_eligible,
):
    config = v4_replay_config(
        scoring_end_ms=V4_GENERATED_MS + scoring_end_offset_ms
    )
    events = canonical_v4_events()

    origin = replay_v4_causal_signals(
        events=events,
        sessions=v4_sessions(events, config),
        config=config,
    ).origins[0]

    assert origin.target_eligible is expected_target_eligible


def test_v4_overlapping_parse_error_session_fails_loss_free_replay_closed():
    config = v4_replay_config()
    events = canonical_v4_events()
    sessions = v4_sessions(events, config)
    sessions[0] = replace(sessions[0], parse_errors_total=1)

    with pytest.raises(
        ReplayDataError,
        match="failed loss-free archive quality: parse_errors",
    ):
        replay_v4_causal_signals(
            events=events,
            sessions=sessions,
            config=config,
        )


def test_v4_streaming_and_collected_replay_are_identical():
    config = v4_replay_config()
    events = canonical_v4_events()
    sessions = v4_sessions(events, config)

    streamed = tuple(
        iter_v4_causal_origins(
            events=iter(events),
            sessions=sessions,
            config=config,
        )
    )
    collected = replay_v4_causal_signals(
        events=iter(events),
        sessions=sessions,
        config=config,
    )

    assert streamed == collected.origins


def test_v4_streaming_yields_before_scanning_a_sparse_full_day(monkeypatch):
    config = v4_replay_config(
        scoring_end_ms=V4_GENERATED_MS + 86_400_000,
    )
    original_poll = replay_module.V4CausalReplayRunner._poll
    poll_calls = 0

    def counted_poll(runner, tick_ms):
        nonlocal poll_calls
        poll_calls += 1
        if poll_calls > 200:
            raise AssertionError("streaming replay scanned beyond its first origin")
        return original_poll(runner, tick_ms)

    monkeypatch.setattr(
        replay_module.V4CausalReplayRunner,
        "_poll",
        counted_poll,
    )
    origins = iter(
        iter_v4_causal_origins(
            events=(),
            sessions=(),
            config=config,
        )
    )

    first = next(origins)
    origins.close()

    assert first.generated_ms == V4_GENERATED_MS
    assert poll_calls < 200


def test_v4_same_code_with_unsupported_control_rule_fails_closed():
    settings_value = v4_settings()
    frozen_code = v4_code_manifest()
    active_config = replace(
        settings_value.config_for_lag(3_000),
        anchor_rule="different_active_anchor_rule",
    )
    contract = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(
            config=active_config,
            code=frozen_code,
        ),
        code=frozen_code,
    )

    with pytest.raises(ReplayDataError, match="unsupported forecast code or rules"):
        replay_v4_causal_signals(
            events=(),
            sessions=(),
            config=v4_replay_config(contract=contract),
        )


def test_v4_forecast_machine_rejects_identity_config_digest_mismatch():
    config = v4_replay_config()
    forecast_config = config.candidate_configs[0]
    identity = replace(
        config.contract.candidate_identity(forecast_config.lag_ms),
        forecast_config_digest="f" * 64,
    )

    with pytest.raises(ReplayDataError, match="forecast-config digest"):
        replay_module._V4ForecastMachine(((forecast_config, identity),))


def test_v4_distinct_control_requires_sufficient_history_retention():
    settings_value = v4_settings()
    frozen_code = v4_code_manifest()
    active_config = replace(
        settings_value.config_for_lag(3_000),
        history_retention_ms=1,
    )
    contract = v4_contract(
        settings_value=settings_value,
        incumbent_value=v4_incumbent(
            config=active_config,
            code=frozen_code,
        ),
        code=frozen_code,
    )

    with pytest.raises(ReplayDataError, match="history_retention_ms"):
        replay_v4_causal_signals(
            events=(),
            sessions=(),
            config=v4_replay_config(contract=contract),
        )


@pytest.mark.parametrize(
    "config_overrides",
    [
        {"scoring_start_ms": -1},
        {"scoring_end_ms": V4_GENERATED_MS},
        {
            "timing_cell": replace(
                V4_TIMING_CELLS[0],
                futures_delay_ms=99,
            )
        },
    ],
)
def test_v4_replay_config_rejects_non_frozen_inputs(config_overrides):
    with pytest.raises((TypeError, ValueError)):
        v4_replay_config(**config_overrides)


def test_replay_uses_bucket_close_ratio_and_pairs_no_change_baseline():
    events = base_events()
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(),
    )
    candidate = only_candidate(report)
    metrics = candidate["metrics"]
    common_cohort = candidate["common_cohort"]

    assert candidate["model_version"] == "catchup_ratio_l1000_b100"
    assert candidate["scheduled"] == 7
    assert candidate["valid_generated"] == 7
    assert candidate["target_eligible"] == 5
    assert candidate["valid_target_eligible"] == 5
    assert candidate["target_censored"] == 2
    assert candidate["scored"] == 5
    assert candidate["generation_coverage"] == Decimal("1")
    assert candidate["maturation_coverage"] == Decimal("1")
    assert metrics["model_mean_absolute_error_usd"] == Decimal("0")
    assert metrics["baseline_mean_absolute_error_usd"] == Decimal("4")
    assert metrics["wins"] == 2
    assert metrics["ties"] == 3
    assert metrics["losses"] == 0
    directional = metrics["directional"]
    assert directional["confusion_matrix"] == {
        "actual_up": {
            "predicted_up": 2,
            "predicted_neutral": 0,
            "predicted_down": 0,
        },
        "actual_neutral": {
            "predicted_up": 0,
            "predicted_neutral": 3,
            "predicted_down": 0,
        },
        "actual_down": {
            "predicted_up": 0,
            "predicted_neutral": 0,
            "predicted_down": 0,
        },
    }
    assert directional["rates"]["three_class_accuracy"] == Decimal("1")
    assert directional["rates"]["action_precision"] == Decimal("1")
    assert directional["rates"]["move_recall"] == Decimal("1")
    assert directional["rates"]["predicted_action_frequency"] == (
        Decimal("2") / Decimal("5")
    )
    assert common_cohort["scored"] == 5
    assert common_cohort["metrics"][
        "model_mean_absolute_error_usd"
    ] == Decimal("0")
    assert common_cohort["metrics"][
        "baseline_mean_absolute_error_usd"
    ] == Decimal("4")


def test_directional_metrics_cover_all_confusion_cells_and_neutral_false_actions():
    aggregate = replay_module._MetricAggregate(
        keep_medians=False,
        sample_max=10,
        seed=1,
    )
    direction_value = {
        "up": Decimal("2"),
        "neutral": Decimal("0"),
        "down": Decimal("-2"),
    }
    for actual in ("up", "neutral", "down"):
        for predicted in ("up", "neutral", "down"):
            aggregate.add(
                SimpleNamespace(
                    model_error=Decimal("0"),
                    baseline_error=Decimal("0"),
                    model_error_bps=Decimal("0"),
                    baseline_error_bps=Decimal("0"),
                    actual_move_bps=direction_value[actual],
                    absolute_advantage=Decimal("0"),
                    forecast=SimpleNamespace(
                        predicted_move_bps=direction_value[predicted]
                    ),
                ),
                neutral_band_bps=Decimal("1"),
            )

    directional = aggregate.summary()["directional"]

    assert directional["confusion_matrix"] == {
        "actual_up": {
            "predicted_up": 1,
            "predicted_neutral": 1,
            "predicted_down": 1,
        },
        "actual_neutral": {
            "predicted_up": 1,
            "predicted_neutral": 1,
            "predicted_down": 1,
        },
        "actual_down": {
            "predicted_up": 1,
            "predicted_neutral": 1,
            "predicted_down": 1,
        },
    }
    assert directional["counts"] == {
        "three_class_correct": 3,
        "predicted_actions": 6,
        "actual_moves": 6,
        "actual_neutral": 3,
        "correct_actions": 2,
        "false_actions_on_neutral": 2,
        "opposite_direction_actions": 2,
    }
    with localcontext() as context:
        context.prec = 50
        assert directional["rates"] == {
            "three_class_accuracy": Decimal("1") / Decimal("3"),
            "action_precision": Decimal("1") / Decimal("3"),
            "move_recall": Decimal("1") / Decimal("3"),
            "false_action_rate_on_neutral": Decimal("2") / Decimal("3"),
            "opposite_direction_rate_on_actual_moves": (
                Decimal("1") / Decimal("3")
            ),
            "predicted_action_frequency": Decimal("2") / Decimal("3"),
        }


def test_directional_metrics_use_null_for_zero_denominators_and_band_is_inclusive():
    aggregate = replay_module._MetricAggregate(
        keep_medians=False,
        sample_max=10,
        seed=1,
    )
    aggregate.add(
        SimpleNamespace(
            model_error=Decimal("0"),
            baseline_error=Decimal("0"),
            model_error_bps=Decimal("0"),
            baseline_error_bps=Decimal("0"),
            actual_move_bps=Decimal("1"),
            absolute_advantage=Decimal("0"),
            forecast=SimpleNamespace(predicted_move_bps=Decimal("-1")),
        ),
        neutral_band_bps=Decimal("1"),
    )

    directional = aggregate.summary()["directional"]

    assert directional["confusion_matrix"]["actual_neutral"][
        "predicted_neutral"
    ] == 1
    assert directional["rates"] == {
        "three_class_accuracy": Decimal("1"),
        "action_precision": None,
        "move_recall": None,
        "false_action_rate_on_neutral": Decimal("0"),
        "opposite_direction_rate_on_actual_moves": None,
        "predicted_action_frequency": Decimal("0"),
    }


@pytest.mark.parametrize(
    ("offset_ns", "model_mae", "baseline_mae", "wins", "losses"),
    [
        (0, Decimal("0"), Decimal("10"), 1, 0),
        (1, Decimal("10"), Decimal("0"), 0, 1),
    ],
)
def test_actual_target_includes_exact_event_but_never_first_event_after_target(
    offset_ns,
    model_mae,
    baseline_mae,
    wins,
    losses,
):
    events = base_events(chainlink_catchup_offset_ns=offset_ns)
    config = replay_config(end_ms=ORIGIN_MS + 3_001)
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=config,
    )
    metrics = only_candidate(report)["metrics"]

    assert metrics["count"] == 1
    assert metrics["model_mean_absolute_error_usd"] == model_mae
    assert metrics["baseline_mean_absolute_error_usd"] == baseline_mae
    assert metrics["wins"] == wins
    assert metrics["losses"] == losses


@pytest.mark.parametrize(
    ("futures_offset_ns", "expected_model_error"),
    [
        (0, Decimal("0")),
        (1, Decimal("10")),
    ],
)
def test_poll_visibility_includes_exact_event_and_excludes_one_ns_after(
    futures_offset_ns,
    expected_model_error,
):
    events = [
        event(FUTURES_EVENT, ORIGIN_MS, "100"),
        event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
        event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
        event(FUTURES_EVENT, ORIGIN_MS + 1_500, "100"),
        event(CHAINLINK_EVENT, ORIGIN_MS + 1_500, "100"),
        event(
            FUTURES_EVENT,
            ORIGIN_MS + 2_000,
            "110",
            received_offset_ns=futures_offset_ns,
        ),
        event(CHAINLINK_EVENT, ORIGIN_MS + 3_000, "110"),
    ]
    events.sort(key=lambda item: item.sort_key)

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(end_ms=ORIGIN_MS + 3_001),
    )

    assert only_candidate(report)["metrics"][
        "model_mean_absolute_error_usd"
    ] == expected_model_error


@pytest.mark.parametrize(
    ("futures_offset_ns", "expected_model_error"),
    [
        (0, Decimal("0")),
        (1, Decimal("10")),
    ],
)
def test_delayed_visibility_includes_exact_boundary_but_not_one_ns_after(
    futures_offset_ns,
    expected_model_error,
):
    events = sorted(
        [
            event(FUTURES_EVENT, ORIGIN_MS, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 1_500, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 1_500, "100"),
            event(
                FUTURES_EVENT,
                ORIGIN_MS + 1_900,
                "110",
                received_offset_ns=futures_offset_ns,
            ),
            event(CHAINLINK_EVENT, ORIGIN_MS + 3_000, "110"),
        ],
        key=lambda item: item.sort_key,
    )

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            end_ms=ORIGIN_MS + 3_001,
            futures_availability_delay_ms=100,
        ),
    )

    assert only_candidate(report)["metrics"][
        "model_mean_absolute_error_usd"
    ] == expected_model_error


@pytest.mark.parametrize(
    ("futures_offset_ns", "expected_regime_counts"),
    [
        (0, {"high": 3, "low": 2}),
        (1, {"high": 2, "low": 3}),
    ],
)
def test_delayed_futures_jump_changes_volatility_slice_only_when_visible(
    futures_offset_ns,
    expected_regime_counts,
):
    events = base_events()
    jump_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == FUTURES_EVENT
        and item.received_ms == ORIGIN_MS + 2_000
    )
    events[jump_index] = event(
        FUTURES_EVENT,
        ORIGIN_MS + 2_000,
        "110",
        received_offset_ns=futures_offset_ns,
    )
    events.sort(key=lambda item: item.sort_key)

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            futures_availability_delay_ms=1_000,
            futures_stale_ms=5_000,
            reference_max_gap_ms=500,
            history_retention_ms=6_500,
        ),
    )
    candidate = only_candidate(report)

    assert candidate["scored"] == 5
    assert {
        regime: metrics["count"]
        for regime, metrics in candidate["slices"][
            "raw_bucket_return_rms_regime"
        ].items()
    } == expected_regime_counts


def test_delayed_futures_return_lookback_starts_at_worker_poll_visibility():
    events = base_events()
    jump_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == FUTURES_EVENT
        and item.received_ms == ORIGIN_MS + 2_000
    )
    events[jump_index] = event(
        FUTURES_EVENT,
        ORIGIN_MS + 2_000,
        "110",
        received_offset_ns=1,
    )
    events.sort(key=lambda item: item.sort_key)

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            futures_availability_delay_ms=1_000,
            futures_stale_ms=5_000,
            reference_max_gap_ms=500,
            history_retention_ms=6_500,
            volatility_lookback_ms=400,
        ),
    )
    candidate = only_candidate(report)

    # The jump is available one nanosecond after the 3,000 ms poll, so the
    # worker first sees it at 3,100 ms. It remains exactly on the 400 ms
    # lookback boundary for the 3,500 ms forecast.
    assert candidate["scored"] == 5
    assert {
        regime: metrics["count"]
        for regime, metrics in candidate["slices"][
            "raw_bucket_return_rms_regime"
        ].items()
    } == {"high": 1, "low": 3, "unknown": 1}


def test_initial_futures_seeds_volatility_only_after_becoming_visible():
    config = replay_config(futures_availability_delay_ms=100)
    initial_futures = event(
        FUTURES_EVENT,
        ORIGIN_MS + 900,
        "100",
    )
    segment = replay_module.ReplaySegment(
        start_wall_ns=(ORIGIN_MS + 1_000) * replay_module.NS_PER_MS,
        end_wall_ns=(ORIGIN_MS + 6_000) * replay_module.NS_PER_MS,
        futures_session_id=FUTURES_CONNECTION_1,
        chainlink_session_id=CHAINLINK_CONNECTION_1,
    )
    runtime = replay_module._SegmentRuntime(
        segment=segment,
        config=config,
        accumulators={
            model.version: replay_module._CandidateAccumulator(model, config)
            for model in config.models
        },
        diagnostics=replay_module._EventDiagnostics(
            sample_max=config.quantile_sample_max
        ),
        initial_futures=initial_futures,
    )

    assert runtime._last_futures_for_volatility is None
    runtime._apply_visible_events(ORIGIN_MS + 999)
    assert runtime._last_futures_for_volatility is None

    runtime._apply_visible_events(ORIGIN_MS + 1_000)
    assert runtime._last_futures_for_volatility == initial_futures
    assert runtime._volatility_returns == deque()


def test_source_delays_reorder_visibility_without_advancing_polls_early():
    events = sorted(
        [
            event(FUTURES_EVENT, ORIGIN_MS, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 1_500, "100"),
            # This raw event is encountered before the Chainlink event below,
            # but its assumed visibility time is later than the 2,000 ms poll.
            event(FUTURES_EVENT, ORIGIN_MS + 1_900, "110"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 1_950, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 3_000, "100"),
        ],
        key=lambda item: item.sort_key,
    )

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            end_ms=ORIGIN_MS + 3_001,
            reference_max_gap_ms=500,
            history_retention_ms=6_500,
            futures_availability_delay_ms=200,
            chainlink_availability_delay_ms=0,
        ),
    )
    candidate = only_candidate(report)

    assert candidate["scored"] == 1
    assert candidate["metrics"]["model_mean_absolute_error_usd"] == Decimal("0")
    assert candidate["metrics"]["baseline_mean_absolute_error_usd"] == Decimal(
        "0"
    )


def test_visibility_delay_preserves_raw_lag_but_gates_actual_history():
    events = base_events()

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            end_ms=ORIGIN_MS + 3_001,
            chainlink_availability_delay_ms=100,
        ),
    )
    candidate = only_candidate(report)

    # The 1,500 ms Chainlink anchor still targets the futures event received at
    # 500 ms, rather than treating its assumed 1,600 ms visibility as receipt.
    assert candidate["reference_gap_ms"]["max"] == 0
    # The raw Chainlink event received exactly at the 3,000 ms target is not an
    # evaluator-visible actual until 3,100 ms, so the prior visible value wins.
    assert candidate["scored"] == 1
    assert candidate["metrics"]["model_mean_absolute_error_usd"] == Decimal(
        "10"
    )
    assert candidate["metrics"]["baseline_mean_absolute_error_usd"] == Decimal(
        "0"
    )


@pytest.mark.parametrize(
    ("chainlink_offset_ns", "expected_model_error"),
    [
        (0, Decimal("0")),
        (1, Decimal("10")),
    ],
)
def test_delayed_actual_visibility_includes_exact_target_not_one_ns_after(
    chainlink_offset_ns,
    expected_model_error,
):
    events = sorted(
        [
            event(FUTURES_EVENT, ORIGIN_MS, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 1_500, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 1_500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 2_000, "110"),
            event(
                CHAINLINK_EVENT,
                ORIGIN_MS + 2_900,
                "110",
                received_offset_ns=chainlink_offset_ns,
            ),
        ],
        key=lambda item: item.sort_key,
    )

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            end_ms=ORIGIN_MS + 3_001,
            chainlink_availability_delay_ms=100,
        ),
    )

    assert only_candidate(report)["metrics"][
        "model_mean_absolute_error_usd"
    ] == expected_model_error


def test_evaluation_phase_generates_at_the_configured_poll_offset():
    events = sorted(
        [
            event(FUTURES_EVENT, ORIGIN_MS, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 1_500, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 1_500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 2_050, "110"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 3_000, "110"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 3_100, "110"),
        ],
        key=lambda item: item.sort_key,
    )

    exact_phase = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(end_ms=ORIGIN_MS + 3_101),
    )
    shifted_phase = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(
            end_ms=ORIGIN_MS + 3_101,
            evaluation_phase_offset_ms=100,
        ),
    )

    assert only_candidate(exact_phase)["metrics"][
        "model_mean_absolute_error_usd"
    ] == Decimal("10")
    assert only_candidate(shifted_phase)["metrics"][
        "model_mean_absolute_error_usd"
    ] == Decimal("0")


def test_error_metrics_are_decimal_and_match_hand_calculation():
    events = base_events()
    catchup_index = next(
        index
        for index, item in enumerate(events)
        if item.kind == CHAINLINK_EVENT
        and item.received_ms == ORIGIN_MS + 3_000
    )
    events[catchup_index] = event(
        CHAINLINK_EVENT,
        ORIGIN_MS + 3_000,
        "107",
    )
    events.sort(key=lambda item: item.sort_key)

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(end_ms=ORIGIN_MS + 3_001),
    )
    metrics = only_candidate(report)["metrics"]

    assert metrics["model_mean_absolute_error_usd"] == Decimal("3")
    assert metrics["baseline_mean_absolute_error_usd"] == Decimal("7")
    assert metrics["model_rmse_usd"] == Decimal("3")
    assert metrics["baseline_rmse_usd"] == Decimal("7")
    assert metrics["model_median_absolute_error_bps"] == Decimal("300")
    assert metrics["baseline_median_absolute_error_bps"] == Decimal("700")
    assert metrics["mean_absolute_advantage_usd"] == Decimal("4")
    assert metrics["mae_skill_vs_no_change"] == Decimal("4") / Decimal("7")
    assert metrics["sufficient_statistics"] == {
        "model_absolute_error_sum_usd": Decimal("3"),
        "baseline_absolute_error_sum_usd": Decimal("7"),
        "model_squared_error_sum_usd2": Decimal("9"),
        "baseline_squared_error_sum_usd2": Decimal("49"),
        "absolute_advantage_sum_usd": Decimal("4"),
    }


def test_advantage_statistic_is_canonical_under_decimal_rounding():
    aggregate = replay_module._MetricAggregate(
        keep_medians=True,
        sample_max=10,
        seed=1,
    )
    values = (
        (
            Decimal("726.5243569104000901000382311"),
            Decimal("548.0240037469730283658451020"),
        ),
        (
            Decimal("1650.041671915500515412773504"),
            Decimal("1851.461269683651937858055606"),
        ),
    )
    for model_error, baseline_error in values:
        aggregate.add(
            SimpleNamespace(
                model_error=model_error,
                baseline_error=baseline_error,
                model_error_bps=model_error,
                baseline_error_bps=baseline_error,
                actual_move_bps=Decimal("0"),
                absolute_advantage=(
                    abs(baseline_error) - abs(model_error)
                ),
                forecast=SimpleNamespace(predicted_move_bps=Decimal("0")),
            ),
            neutral_band_bps=Decimal("1"),
        )

    summary = aggregate.summary()
    statistics = summary["sufficient_statistics"]

    assert statistics["absolute_advantage_sum_usd"] == (
        statistics["baseline_absolute_error_sum_usd"]
        - statistics["model_absolute_error_sum_usd"]
    )
    assert summary["mean_absolute_advantage_usd"] == (
        statistics["absolute_advantage_sum_usd"] / Decimal("2")
    )


def test_non_poll_aligned_target_uses_latest_chainlink_at_exact_target():
    config = replay_config(
        end_ms=ORIGIN_MS + 3_051,
        lags_ms=(1_050,),
        history_retention_ms=6_200,
    )
    events = sorted(
        [
            event(FUTURES_EVENT, ORIGIN_MS, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 500, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 1_000, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 1_550, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS + 1_550, "100"),
            event(FUTURES_EVENT, ORIGIN_MS + 2_000, "110"),
            event(
                CHAINLINK_EVENT,
                ORIGIN_MS + 3_050,
                "110",
                received_offset_ns=1,
            ),
        ],
        key=lambda item: item.sort_key,
    )

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=config,
    )
    candidate = only_candidate(report)

    assert candidate["target_eligible"] == 1
    assert candidate["scored"] == 1
    assert candidate["missing_actual"] == 0
    assert candidate["metrics"][
        "model_mean_absolute_error_usd"
    ] == Decimal("10")
    assert candidate["metrics"][
        "baseline_mean_absolute_error_usd"
    ] == Decimal("0")


def test_replay_collapses_intermediate_cache_events_until_next_poll():
    events = base_events()
    events.extend(
        [
            event(
                CHAINLINK_EVENT,
                ORIGIN_MS + 1_450,
                "99",
                received_offset_ns=100,
                sequence=99_001,
            ),
            event(
                CHAINLINK_EVENT,
                ORIGIN_MS + 1_490,
                "100",
                received_offset_ns=100,
                sequence=99_002,
            ),
        ]
    )
    events.sort(key=lambda item: item.sort_key)

    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(),
    )

    assert only_candidate(report)["metrics"][
        "model_mean_absolute_error_usd"
    ] == Decimal("0")
    assert report.event_diagnostics[CHAINLINK_EVENT]["events"] == 5


def test_invalid_poll_attempts_are_counted_in_coverage():
    events = [
        event(FUTURES_EVENT, ORIGIN_MS, "100"),
        event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
    ]
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(),
    )
    candidate = only_candidate(report)

    assert candidate["scheduled"] == 7
    assert candidate["valid_generated"] == 0
    assert candidate["scored"] == 0
    assert candidate["generation_coverage"] == Decimal("0")
    assert sum(candidate["invalid_statuses"].values()) == 7
    assert ANCHOR_HISTORY_MISSING in candidate["invalid_reasons"]


def test_three_candidates_use_the_same_scheduled_population():
    events = base_events() + [
        event(FUTURES_EVENT, ORIGIN_MS + offset_ms, "110")
        for offset_ms in (5_500, 6_000, 6_500, 7_000)
    ]
    events.sort(key=lambda item: item.sort_key)
    config = replay_config(
        start_ms=ORIGIN_MS + 2_000,
        end_ms=ORIGIN_MS + 7_001,
        lags_ms=(500, 1_000, 1_500),
        history_retention_ms=7_000,
    )
    sessions = sessions_for(events)
    sessions = [
        replace_session_end(session, ORIGIN_MS + 8_000)
        for session in sessions
    ]
    report = replay_shadow_signals(
        events=events,
        sessions=sessions,
        config=config,
    )

    scheduled = {
        candidate["scheduled"] for candidate in report.candidate_summaries
    }
    assert scheduled == {11}
    assert [
        candidate["scored"] for candidate in report.candidate_summaries
    ] == [10, 9, 8]
    common_cohorts = [
        candidate["common_cohort"]
        for candidate in report.candidate_summaries
    ]
    assert {
        (
            cohort["target_eligible"],
            cohort["valid_generated"],
            cohort["scored"],
        )
        for cohort in common_cohorts
    } == {(8, 8, 8)}
    assert report.status == "ok"


def test_common_cohort_slices_only_include_common_scored_outcomes():
    events = base_events() + [
        event(FUTURES_EVENT, ORIGIN_MS + offset_ms, "110")
        for offset_ms in (5_500, 6_000, 6_500, 7_000)
    ]
    events.sort(key=lambda item: item.sort_key)
    config = replay_config(
        start_ms=ORIGIN_MS + 2_000,
        end_ms=ORIGIN_MS + 7_001,
        lags_ms=(500, 1_000, 1_500),
        history_retention_ms=7_000,
    )
    sessions = [
        replace_session_end(session, ORIGIN_MS + 8_000)
        for session in sessions_for(events)
    ]

    report = replay_shadow_signals(
        events=events,
        sessions=sessions,
        config=config,
    )

    for candidate in report.candidate_summaries:
        common_cohort = candidate["common_cohort"]
        assert common_cohort["scored"] == 8
        assert set(common_cohort["slices"]) == {
            "actual_direction",
            "actual_move_size",
            "raw_bucket_return_rms_regime",
            "market_expiry",
            "session_boundary_proximity",
        }
        for dimension in common_cohort["slices"].values():
            assert sum(
                category["count"] for category in dimension.values()
            ) == common_cohort["scored"]
        for dimension in candidate["slices"].values():
            assert sum(
                category["count"] for category in dimension.values()
            ) == candidate["scored"]

    assert [
        candidate["scored"] for candidate in report.candidate_summaries
    ] == [10, 9, 8]


def test_status_requires_evidence_for_every_candidate_and_common_cohort():
    events = base_events() + [
        event(FUTURES_EVENT, ORIGIN_MS + offset_ms, "110")
        for offset_ms in (5_500, 6_000, 6_500, 7_000)
    ]
    events.sort(key=lambda item: item.sort_key)
    config = replay_config(
        start_ms=ORIGIN_MS + 2_000,
        end_ms=ORIGIN_MS + 7_001,
        lags_ms=(500, 1_000, 1_500),
        history_retention_ms=7_000,
    )
    sessions = [
        replace_session_end(session, ORIGIN_MS + 8_000)
        for session in sessions_for(events)
    ]
    report = replay_shadow_signals(
        events=events,
        sessions=sessions,
        config=config,
    )

    missing_candidate = [dict(summary) for summary in report.candidate_summaries]
    missing_candidate[-1]["scored"] = 0
    assert replace(
        report,
        candidate_summaries=tuple(missing_candidate),
    ).status == "partial_candidate_evidence"

    missing_common = []
    for summary in report.candidate_summaries:
        updated = dict(summary)
        updated["common_cohort"] = dict(summary["common_cohort"], scored=0)
        missing_common.append(updated)
    assert replace(
        report,
        candidate_summaries=tuple(missing_common),
    ).status == "partial_candidate_evidence"


def test_poll_simulation_is_clipped_to_requested_end_inside_long_session():
    events = base_events()
    sessions = [
        replace_session_end(session, ORIGIN_MS + 60 * 60 * 1000)
        for session in sessions_for(events)
    ]

    report = replay_shadow_signals(
        events=events,
        sessions=sessions,
        config=replay_config(),
    )

    assert report.polls_processed == 52


def test_quantile_state_is_bounded_while_streaming_metrics_stay_exact():
    events = base_events()
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(quantile_sample_max=3),
    )
    candidate = only_candidate(report)
    sampling = candidate["metrics"]["quantile_sampling"]

    assert sampling == {
        "population_size": 5,
        "sample_size": 3,
        "bounded_reservoir": True,
    }
    assert candidate["metrics"][
        "model_mean_absolute_error_usd"
    ] == Decimal("0")


def test_volatility_recovers_from_decimal_subtraction_drift():
    runtime = object.__new__(replay_module._SegmentRuntime)
    runtime.config = replay_config(volatility_lookback_ms=2_000)
    runtime._volatility_returns = deque(
        [(ORIGIN_MS, Decimal("4"))]
    )
    runtime._volatility_square_sum = Decimal("-1E-27")

    assert runtime._current_volatility(ORIGIN_MS) == Decimal("2")
    assert runtime._volatility_square_sum == Decimal("4")

    runtime._volatility_returns = deque(
        [(ORIGIN_MS - 3_000, Decimal("4"))]
    )
    runtime._volatility_square_sum = Decimal("4.000000000000000000000000001")

    assert runtime._current_volatility(ORIGIN_MS) is None
    assert runtime._volatility_square_sum == Decimal("0")


def replace_session_end(session, end_ms):
    return ReplaySession(
        connection_id=session.connection_id,
        source=session.source,
        connected_wall_ns=session.connected_wall_ns,
        ready_wall_ns=session.ready_wall_ns,
        disconnected_wall_ns=end_ms * 1_000_000,
        messages_accepted_total=session.messages_accepted_total,
        parse_errors_total=session.parse_errors_total,
        records_dropped_total=session.records_dropped_total,
        raw_row_count=session.raw_row_count,
        raw_accepted_total=session.raw_accepted_total,
        duplicate_key_count=session.duplicate_key_count,
        monotonic_regression_count=session.monotonic_regression_count,
        integrity_checked=session.integrity_checked,
    )


@pytest.mark.parametrize(
    ("session_overrides", "expected_reason"),
    [
        ({"completed": False}, "open_unverified"),
        ({"ready": False}, "never_ready"),
        ({"dropped": 1}, "records_dropped"),
        ({"parse_errors": 1}, "parse_errors"),
        ({"integrity_checked": False}, "integrity_unverified"),
        (
            {"raw_accepted": 0},
            "accepted_count_mismatch_or_retention_truncated",
        ),
    ],
)
def test_session_quality_exclusions_are_reported(
    session_overrides,
    expected_reason,
):
    config = replay_config(
        exclude_parse_error_sessions=expected_reason == "parse_errors"
    )
    futures = clean_session(
        FUTURES_SESSION_SOURCE,
        FUTURES_CONNECTION_1,
        **session_overrides,
    )
    chainlink = clean_session(
        CHAINLINK_SESSION_SOURCE,
        CHAINLINK_CONNECTION_1,
    )

    selection = select_replay_sessions([futures, chainlink], config)

    assert selection.segments == ()
    assert selection.excluded_by_reason[expected_reason] == 1


def test_parse_error_sessions_can_be_included_explicitly():
    config = replay_config(exclude_parse_error_sessions=False)
    sessions = [
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_1,
            parse_errors=1,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_1,
        ),
    ]

    selection = select_replay_sessions(sessions, config)

    assert len(selection.segments) == 1
    assert selection.eligible_by_source[FUTURES_SESSION_SOURCE] == 1


def test_session_intersections_create_separate_reconnect_segments():
    sessions = [
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_1,
            start_ms=ORIGIN_MS,
            end_ms=ORIGIN_MS + 3_000,
        ),
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_2,
            start_ms=ORIGIN_MS + 3_100,
            end_ms=ORIGIN_MS + 6_000,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_1,
            start_ms=ORIGIN_MS - 100,
            end_ms=ORIGIN_MS + 4_000,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_2,
            start_ms=ORIGIN_MS + 4_100,
            end_ms=ORIGIN_MS + 6_000,
        ),
    ]

    selection = select_replay_sessions(sessions, replay_config())

    assert [
        (segment.start_wall_ns, segment.end_wall_ns)
        for segment in selection.segments
    ] == [
        (ORIGIN_MS * 1_000_000, (ORIGIN_MS + 3_000) * 1_000_000),
        (
            (ORIGIN_MS + 3_100) * 1_000_000,
            (ORIGIN_MS + 4_000) * 1_000_000,
        ),
        (
            (ORIGIN_MS + 4_100) * 1_000_000,
            (ORIGIN_MS + 6_000) * 1_000_000,
        ),
    ]


def test_overlapping_sessions_for_one_source_are_rejected():
    sessions = [
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_1,
            end_ms=ORIGIN_MS + 4_000,
        ),
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_2,
            start_ms=ORIGIN_MS + 3_000,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_1,
        ),
    ]

    with pytest.raises(ReplayDataError, match="overlap"):
        select_replay_sessions(sessions, replay_config())


def test_replay_requires_chronological_exact_ns_input():
    events = base_events()
    events[0], events[1] = events[1], events[0]

    with pytest.raises(ReplayDataError, match="chronological"):
        replay_shadow_signals(
            events=events,
            sessions=sessions_for(events),
            config=replay_config(),
        )


def test_replay_rejects_an_exact_duplicate_raw_event():
    events = base_events()
    events.insert(1, events[0])

    with pytest.raises(ReplayDataError, match="duplicated"):
        replay_shadow_signals(
            events=events,
            sessions=sessions_for(events),
            config=replay_config(),
        )


def test_same_millisecond_events_preserve_exact_ns_order():
    first = event(
        CHAINLINK_EVENT,
        ORIGIN_MS,
        "100",
        received_offset_ns=1,
        sequence=1,
    )
    second = event(
        CHAINLINK_EVENT,
        ORIGIN_MS,
        "101",
        received_offset_ns=2,
        sequence=2,
    )

    assert first.received_ms == second.received_ms
    assert first.sort_key < second.sort_key
    assert second.observed_price.value == Decimal("101")


def test_report_json_serializes_decimals_as_strings_and_does_not_select_model():
    events = base_events()
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(),
    )
    payload = json.loads(encode_replay_report(report))

    assert payload["schema_version"] == 3
    assert payload["mode"] == "shadow_raw_replay"
    assert payload["selection_performed"] is False
    assert payload["configuration"]["beta"] == "1"
    assert payload["configuration"]["max_future_skew_ms"] == 0
    assert payload["configuration"]["futures_availability_delay_ms"] == 0
    assert payload["configuration"]["chainlink_availability_delay_ms"] == 0
    assert payload["configuration"]["evaluation_phase_offset_ms"] == 0
    assert any(
        "not measured Redis publication-completion timing" in limitation
        for limitation in payload["data_quality"]["limitations"]
    )
    assert payload["candidates"][0]["metrics"][
        "baseline_mean_absolute_error_usd"
    ] == "4"
    assert payload["candidates"][0]["common_cohort"]["metrics"][
        "baseline_mean_absolute_error_usd"
    ] == "4"
    directional = payload["candidates"][0]["metrics"]["directional"]
    assert all(
        isinstance(value, int)
        for row in directional["confusion_matrix"].values()
        for value in row.values()
    )
    assert all(
        value is None or isinstance(value, str)
        for value in directional["rates"].values()
    )
    assert "directional_accuracy_when_action" not in payload["candidates"][0][
        "metrics"
    ]
    statistics = payload["candidates"][0]["metrics"][
        "sufficient_statistics"
    ]
    assert all(isinstance(value, str) for value in statistics.values())
    assert {
        key: Decimal(value) for key, value in statistics.items()
    } == {
        "absolute_advantage_sum_usd": Decimal("20"),
        "baseline_absolute_error_sum_usd": Decimal("20"),
        "baseline_squared_error_sum_usd2": Decimal("200"),
        "model_absolute_error_sum_usd": Decimal("0"),
        "model_squared_error_sum_usd2": Decimal("0"),
    }
    common_slices = payload["candidates"][0]["common_cohort"]["slices"]
    common_slice_statistic = common_slices["actual_direction"]["neutral"][
        "sufficient_statistics"
    ]["model_absolute_error_sum_usd"]
    assert isinstance(common_slice_statistic, str)
    assert Decimal(common_slice_statistic) == Decimal("0")
    assert not _contains_float(payload)


def test_report_file_write_is_atomic_and_leaves_no_temporary_file(tmp_path):
    events = base_events()
    report = replay_shadow_signals(
        events=events,
        sessions=sessions_for(events),
        config=replay_config(),
    )
    output = tmp_path / "replay.json"

    write_replay_report(output, report)

    assert json.loads(output.read_text(encoding="utf-8"))["schema_version"] == 3
    assert list(tmp_path.iterdir()) == [output]


def test_report_records_fixed_timing_sensitivity_assumptions():
    report = replay_shadow_signals(
        events=[],
        sessions=[],
        config=replay_config(
            futures_availability_delay_ms=25,
            chainlink_availability_delay_ms=50,
            evaluation_phase_offset_ms=100,
        ),
    )

    payload = report.to_dict()

    assert payload["configuration"]["futures_availability_delay_ms"] == 25
    assert payload["configuration"]["chainlink_availability_delay_ms"] == 50
    assert payload["configuration"]["evaluation_phase_offset_ms"] == 100
    assert payload["configuration"]["volatility_time_basis"] == (
        "worker_poll_visibility_ms"
    )
    assert any(
        "not measured Redis publication-completion timing" in limitation
        for limitation in payload["data_quality"]["limitations"]
    )


def test_report_status_distinguishes_missing_evidence():
    no_segments = replay_shadow_signals(
        events=[],
        sessions=[],
        config=replay_config(),
    )
    no_scores = replay_shadow_signals(
        events=[
            event(FUTURES_EVENT, ORIGIN_MS, "100"),
            event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
        ],
        sessions=sessions_for(
            [
                event(FUTURES_EVENT, ORIGIN_MS, "100"),
                event(CHAINLINK_EVENT, ORIGIN_MS, "100"),
            ]
        ),
        config=replay_config(),
    )

    assert no_segments.status == "no_eligible_segments"
    assert no_scores.status == "no_scored_forecasts"


def test_cli_writes_diagnostic_report_and_returns_nonzero_without_scores(
    monkeypatch,
    tmp_path,
):
    report = replay_shadow_signals(
        events=[],
        sessions=[],
        config=replay_config(),
    )

    async def fake_run_cli(_arguments):
        return report

    monkeypatch.setattr(replay_module, "_run_cli", fake_run_cli)
    output = tmp_path / "empty.json"

    exit_code = replay_module.main(
        [
            "--start-ms",
            str(ORIGIN_MS),
            "--end-ms",
            str(ORIGIN_MS + 20_000),
            "--output",
            str(output),
        ]
    )

    assert exit_code == 2
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == (
        "no_eligible_segments"
    )


def _contains_float(value):
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(_contains_float(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_float(item) for item in value)
    return False


@pytest.mark.parametrize(
    "overrides",
    [
        {"end_ms": ORIGIN_MS + 2_000},
        {"lags_ms": (1_000, 1_000)},
        {"lags_ms": ()},
        {"lags_ms": (100, 200, 300, 400, 500, 600)},
        {"poll_ms": 99},
        {"poll_ms": 150, "evaluation_interval_ms": 600},
        {"evaluation_interval_ms": 400},
        {"poll_ms": 300, "evaluation_interval_ms": 500},
        {"futures_availability_delay_ms": -1},
        {"chainlink_availability_delay_ms": -1},
        {"evaluation_phase_offset_ms": -1},
        {"evaluation_phase_offset_ms": 1},
        {"evaluation_phase_offset_ms": 500},
        {"quantile_sample_max": 50_001},
        {"history_retention_ms": 30_001},
        {"volatility_lookback_ms": 30_001},
        {
            "start_ms": ORIGIN_MS,
            "end_ms": ORIGIN_MS + 12 * 60 * 60 * 1000 + 1,
            "lags_ms": (12 * 60 * 60 * 1000,),
            "history_retention_ms": 12 * 60 * 60 * 1000 + 5_100,
        },
        {
            "lags_ms": (500, 1_000, 1_500, 2_000, 2_500),
            "history_retention_ms": 7_600,
            "quantile_sample_max": 30_001,
        },
        {"history_retention_ms": 6_099},
        {"neutral_band_bps": Decimal("-1")},
        {"move_size_thresholds_bps": (Decimal("3"), Decimal("1"))},
        {
            "start_ms": ORIGIN_MS,
            "end_ms": ORIGIN_MS + 24 * 60 * 60 * 1000 + 1,
        },
    ],
)
def test_replay_config_validation(overrides):
    with pytest.raises((TypeError, ValueError)):
        replay_config(**overrides)


def test_default_candidate_versions_match_the_plan():
    config = ReplayConfig(
        start_ms=ORIGIN_MS,
        end_ms=ORIGIN_MS + 20_000,
    )

    assert [model.version for model in config.models] == [
        "catchup_ratio_l3000_b100",
        "catchup_ratio_l3500_b100",
        "catchup_ratio_l4000_b100",
    ]


def test_replay_config_accepts_operational_bounds():
    config = replay_config(
        lags_ms=(500, 1_000, 1_500, 2_000, 2_500),
        history_retention_ms=7_600,
        quantile_sample_max=30_000,
        futures_availability_delay_ms=300,
        chainlink_availability_delay_ms=400,
        evaluation_phase_offset_ms=400,
    )
    single_candidate = replay_config(quantile_sample_max=50_000)

    assert len(config.models) == 5
    assert config.poll_ms == 100
    assert config.evaluation_interval_ms == 500
    assert config.max_future_skew_ms == 0
    assert config.futures_availability_delay_ms == 300
    assert config.chainlink_availability_delay_ms == 400
    assert config.evaluation_phase_offset_ms == 400
    assert single_candidate.quantile_sample_max == 50_000


def test_cli_requires_bounded_range_and_builds_decimal_config():
    parser = build_argument_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    arguments = parser.parse_args(
        [
            "--start-ms",
            str(ORIGIN_MS),
            "--end-ms",
            str(ORIGIN_MS + 20_000),
            "--lags-ms",
            "3000,3500,4000",
            "--beta",
            "1.00",
            "--neutral-band-bps",
            "0.75",
            "--futures-availability-delay-ms",
            "25",
            "--chainlink-availability-delay-ms",
            "50",
            "--evaluation-phase-offset-ms",
            "100",
        ]
    )

    config = config_from_arguments(arguments)

    assert config.lags_ms == (3_000, 3_500, 4_000)
    assert config.beta == Decimal("1.00")
    assert config.neutral_band_bps == Decimal("0.75")
    assert config.max_future_skew_ms == 0
    assert config.futures_availability_delay_ms == 25
    assert config.chainlink_availability_delay_ms == 50
    assert config.evaluation_phase_offset_ms == 100
    assert not hasattr(arguments, "database_url")


def test_raw_queries_are_partition_bounded_half_open_and_read_only_selects():
    normalized_events = " ".join(REPLAY_EVENTS_SQL.split()).lower()
    normalized_futures = " ".join(FUTURES_INTEGRITY_SQL.split()).lower()
    normalized_chainlink = " ".join(CHAINLINK_INTEGRITY_SQL.split()).lower()
    normalized_sessions = " ".join(
        replay_module.SESSION_SQL.split()
    ).lower()

    assert "bucket_start_ms >= $1" in normalized_events
    assert "bucket_start_ms < $2" in normalized_events
    assert "last_received_wall_ns >= $3" in normalized_events
    assert "last_received_wall_ns < $4" in normalized_events
    assert "received_wall_ns >= $3" in normalized_events
    assert "received_wall_ns < $4" in normalized_events
    assert "close_price as value" in normalized_events
    assert "last_trade_time_ms as source_timestamp_ms" in normalized_events
    assert "provider_event_ms as source_timestamp_ms" in normalized_events
    assert "sum(event_count)" in normalized_futures
    assert "count(distinct receive_sequence)" in normalized_chainlink
    assert "first_received_wall_ns >= sessions.ready_wall_ns" in (
        normalized_futures
    )
    assert "sessions.source = 'binance_futures_agg_trade'" in (
        normalized_futures
    )
    assert "sessions.source = 'polymarket_chainlink_rtds'" in (
        normalized_chainlink
    )
    assert "futures_sessions.source = 'binance_futures_agg_trade'" in (
        normalized_events
    )
    assert "chainlink_sessions.source = 'polymarket_chainlink_rtds'" in (
        normalized_events
    )
    assert "source in ( 'binance_futures_agg_trade', " in normalized_sessions
    assert "'polymarket_chainlink_rtds' )" in normalized_sessions
    for query in (
        normalized_events,
        normalized_futures,
        normalized_chainlink,
        " ".join(replay_module.PARTITION_MANIFEST_SQL.split()).lower(),
        " ".join(replay_module.ORPHAN_CONNECTIONS_SQL.split()).lower(),
        normalized_sessions,
    ):
        assert "insert " not in query
        assert "update " not in query
        assert "delete " not in query


def test_database_replay_uses_short_statements_and_closes_connection(
    monkeypatch,
):
    class FakeConnection:
        def __init__(self):
            self.fetch_calls = []
            self.closed = False

        async def fetch(self, query, *arguments):
            self.fetch_calls.append((query, arguments))
            if query == replay_module.PARTITION_MANIFEST_SQL:
                return [("parent", "partition", "bound")]
            if query == replay_module.SESSION_SQL:
                return []
            if query == replay_module.ORPHAN_CONNECTIONS_SQL:
                return []
            raise AssertionError("unexpected query")

        async def close(self):
            self.closed = True

    connection = FakeConnection()
    connect_calls = []

    async def fake_connect(**arguments):
        connect_calls.append(arguments)
        return connection

    monkeypatch.setattr(replay_module.asyncpg, "connect", fake_connect)

    report = asyncio.run(
        replay_from_database(
            database_url="postgresql://writer@127.0.0.1/db",
            config=replay_config(),
            chunk_ms=1_000,
        )
    )

    assert report.status == "no_eligible_segments"
    assert connection.closed is True
    assert not hasattr(connection, "transaction")
    assert connect_calls[0]["server_settings"] == {
        "application_name": "price_collector_shadow_signal_replay",
        "statement_timeout": "1500",
        "lock_timeout": "1000",
        "default_transaction_read_only": "on",
    }
    assert [query for query, _args in connection.fetch_calls].count(
        replay_module.PARTITION_MANIFEST_SQL
    ) == 2


def test_database_event_reader_uses_contiguous_short_time_chunks():
    class FakeConnection:
        def __init__(self):
            self.calls = []

        async def fetch(self, query, *arguments):
            self.calls.append((query, arguments))
            return []

    connection = FakeConnection()
    start_ns = ORIGIN_MS * 1_000_000
    end_ns = start_ns + 650 * 1_000_000

    async def run():
        return [
            item
            async for item in replay_module._iter_database_events(
                connection,
                start_ns=start_ns,
                end_ns=end_ns,
                connection_ids=(FUTURES_CONNECTION_1,),
                chunk_ms=300,
            )
        ]

    assert asyncio.run(run()) == []
    exact_ranges = [
        (arguments[2], arguments[3])
        for _query, arguments in connection.calls
    ]
    assert exact_ranges == [
        (start_ns, start_ns + 300 * 1_000_000),
        (start_ns + 300 * 1_000_000, start_ns + 600 * 1_000_000),
        (start_ns + 600 * 1_000_000, end_ns),
    ]
    assert all(query == REPLAY_EVENTS_SQL for query, _args in connection.calls)
