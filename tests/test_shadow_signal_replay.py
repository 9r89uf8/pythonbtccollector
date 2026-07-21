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
    build_argument_parser,
    config_from_arguments,
    encode_replay_report,
    replay_from_database,
    replay_shadow_signals,
    select_replay_sessions,
    write_replay_report,
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


def test_exact_chainlink_parse_error_allowlist_keeps_other_strict_gates():
    config = replay_config(
        exclude_parse_error_sessions=True,
        allowed_chainlink_parse_error_totals=(0, 2),
    )
    sessions = [
        clean_session(
            FUTURES_SESSION_SOURCE,
            FUTURES_CONNECTION_1,
            parse_errors=2,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_1,
            parse_errors=2,
        ),
        clean_session(
            CHAINLINK_SESSION_SOURCE,
            CHAINLINK_CONNECTION_2,
            parse_errors=1,
        ),
    ]

    selection = select_replay_sessions(sessions, config)

    assert selection.segments == ()
    assert selection.eligible_by_source[CHAINLINK_SESSION_SOURCE] == 1
    assert selection.excluded_by_reason["parse_errors"] == 2


def test_chainlink_parse_error_allowlist_is_explicit_in_report():
    report = replay_shadow_signals(
        events=[],
        sessions=[],
        config=replay_config(
            exclude_parse_error_sessions=True,
            allowed_chainlink_parse_error_totals=(0, 2),
        ),
    ).to_dict()

    assert report["configuration"][
        "allowed_chainlink_parse_error_totals"
    ] == [0, 2]
    assert report["data_quality"]["session_policy"] == (
        "completed_integrity_checked_with_exact_chainlink_parse_error_allowlist"
    )


@pytest.mark.parametrize(
    "allowed_totals",
    [(0, 1), (0, 2, 3), (2,), [0, 2]],
)
def test_chainlink_parse_error_allowlist_is_incident_specific(allowed_totals):
    with pytest.raises((TypeError, ValueError)):
        replay_config(
            exclude_parse_error_sessions=True,
            allowed_chainlink_parse_error_totals=allowed_totals,
        )


def test_chainlink_parse_exception_does_not_bypass_other_integrity_gates():
    config = replay_config(
        exclude_parse_error_sessions=True,
        allowed_chainlink_parse_error_totals=(0, 2),
    )
    chainlink = clean_session(
        CHAINLINK_SESSION_SOURCE,
        CHAINLINK_CONNECTION_1,
        parse_errors=2,
        dropped=1,
    )

    assert chainlink.exclusion_reasons(config) == ("records_dropped",)


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
