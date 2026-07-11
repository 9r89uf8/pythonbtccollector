from decimal import Decimal, Overflow, localcontext

import pytest

from price_collector.shadow_signal import (
    ANCHORED,
    ANCHOR_HISTORY_MISSING,
    ANCHOR_REFERENCE_GAP,
    CHAINLINK_STALE,
    CHAINLINK_UNAVAILABLE,
    FUTURES_STALE,
    FUTURES_UNAVAILABLE,
    MODEL_ERROR,
    TIMESTAMP_REGRESSION,
    VALID,
    WAITING_FOR_NEW_CHAINLINK_ANCHOR,
    WARMING_UP_FUTURES_HISTORY,
    CatchupModel,
    ModelAnchor,
    ObservedPrice,
    ShadowSignalEngine,
    no_change_projection,
    project_from_anchor,
)


MODEL_VERSION = "catchup_ratio_l3500_b100"
MODEL = CatchupModel(
    version=MODEL_VERSION,
    lag_ms=3_500,
    beta=Decimal("1"),
)


def observed(
    value,
    *,
    received_ms,
    source_timestamp_ms=None,
):
    return ObservedPrice(
        value=Decimal(value),
        source_timestamp_ms=source_timestamp_ms,
        received_ms=received_ms,
    )


def make_engine(*, models=(MODEL,), **overrides):
    arguments = {
        "models": models,
        "futures_stale_ms": 1_000,
        "chainlink_stale_ms": 5_000,
        "reference_max_gap_ms": 250,
        "history_retention_ms": 10_000,
    }
    arguments.update(overrides)
    return ShadowSignalEngine(**arguments)


def build_valid_engine():
    engine = make_engine()
    futures_reference = observed(
        "60000",
        received_ms=6_400,
        source_timestamp_ms=6_380,
    )
    initial_chainlink = observed(
        "50000",
        received_ms=6_400,
        source_timestamp_ms=6_390,
    )
    futures_now = observed(
        "60060",
        received_ms=9_900,
        source_timestamp_ms=9_880,
    )
    anchor_chainlink = observed(
        "50000",
        received_ms=10_000,
        source_timestamp_ms=9_980,
    )

    engine.observe(
        futures=futures_reference,
        chainlink=initial_chainlink,
        now_ms=6_400,
    )
    engine.observe(
        futures=futures_now,
        chainlink=initial_chainlink,
        now_ms=9_900,
    )
    result = engine.observe(
        futures=futures_now,
        chainlink=anchor_chainlink,
        now_ms=10_000,
    )
    return engine, futures_reference, futures_now, anchor_chainlink, result


@pytest.mark.parametrize(
    (
        "chainlink_value",
        "reference_value",
        "current_value",
        "beta",
        "projected",
        "pending",
        "pending_bps",
        "direction",
    ),
    [
        ("50000", "60010", "60010", "1", "50000", "0", "0", "flat"),
        ("50000", "60000", "60060", "1", "50050", "50", "10", "up"),
        ("50000", "60000", "59940", "1", "49950", "-50", "-10", "down"),
        ("50000", "60000", "60120", "0.5", "50050", "50", "10", "up"),
    ],
)
def test_project_from_anchor_transfers_ratio_not_dollar_delta(
    chainlink_value,
    reference_value,
    current_value,
    beta,
    projected,
    pending,
    pending_bps,
    direction,
):
    model = CatchupModel(version="candidate", lag_ms=3_500, beta=Decimal(beta))
    anchor = ModelAnchor(
        chainlink=observed(chainlink_value, received_ms=10_000),
        futures_reference=observed(reference_value, received_ms=6_500),
    )

    projection = project_from_anchor(
        model=model,
        anchor=anchor,
        futures_now=observed(current_value, received_ms=10_000),
    )

    assert projection.projected_chainlink == Decimal(projected)
    assert projection.pending_move == Decimal(pending)
    assert projection.pending_move_bps == Decimal(pending_bps)
    assert projection.direction == direction
    assert isinstance(projection.projected_chainlink, Decimal)
    assert isinstance(projection.pending_move, Decimal)
    assert isinstance(projection.pending_move_bps, Decimal)


def test_no_change_projection_is_paired_to_an_explicit_horizon():
    chainlink = observed("50000", received_ms=10_000)

    projection = no_change_projection(chainlink=chainlink, horizon_ms=3_500)

    assert projection.model_version == "baseline_no_change"
    assert projection.horizon_ms == 3_500
    assert projection.projected_chainlink == Decimal("50000")
    assert projection.pending_move == Decimal("0")
    assert projection.pending_move_bps == Decimal("0")
    assert projection.direction == "flat"


def test_startup_waits_for_a_new_chainlink_event_after_history_warms():
    engine = make_engine()
    futures_reference = observed(
        "60000",
        received_ms=6_400,
        source_timestamp_ms=6_380,
    )
    initial_chainlink = observed(
        "50000",
        received_ms=6_400,
        source_timestamp_ms=6_390,
    )
    futures_now = observed(
        "60060",
        received_ms=9_900,
        source_timestamp_ms=9_880,
    )

    first = engine.observe(
        futures=futures_reference,
        chainlink=initial_chainlink,
        now_ms=6_400,
    ).signal_for(MODEL_VERSION)
    warmed = engine.observe(
        futures=futures_now,
        chainlink=initial_chainlink,
        now_ms=9_900,
    ).signal_for(MODEL_VERSION)

    assert first.valid is False
    assert first.state == WARMING_UP_FUTURES_HISTORY
    assert first.projection is None
    assert warmed.valid is False
    assert warmed.state == WAITING_FOR_NEW_CHAINLINK_ANCHOR
    assert warmed.projection is None

    anchor_chainlink = observed(
        "50000",
        received_ms=10_000,
        source_timestamp_ms=9_980,
    )
    valid = engine.observe(
        futures=futures_now,
        chainlink=anchor_chainlink,
        now_ms=10_000,
    ).signal_for(MODEL_VERSION)

    assert valid.valid is True
    assert valid.status == VALID
    assert valid.state == ANCHORED
    assert valid.anchor.futures_reference == futures_reference
    assert valid.projection.projected_chainlink == Decimal("50050")
    assert valid.projection.pending_move == Decimal("50")
    assert valid.projection.pending_move_bps == Decimal("10")


def test_stale_cached_futures_value_cannot_seed_startup_history():
    engine = make_engine()
    stale_cached_futures = observed(
        "60000",
        received_ms=6_500,
        source_timestamp_ms=6_500,
    )
    cached_chainlink = observed(
        "50000",
        received_ms=10_000,
        source_timestamp_ms=10_000,
    )

    startup = engine.observe(
        futures=stale_cached_futures,
        chainlink=cached_chainlink,
        now_ms=10_000,
    )

    assert startup.futures_history_size == 0
    assert startup.signal_for(MODEL_VERSION).status == FUTURES_STALE
    assert startup.signal_for(MODEL_VERSION).anchor is None

    fresh_prices = (
        observed("60001", received_ms=10_001, source_timestamp_ms=10_001),
        observed("60002", received_ms=11_000, source_timestamp_ms=11_000),
        observed("60003", received_ms=12_000, source_timestamp_ms=12_000),
        observed("60004", received_ms=13_000, source_timestamp_ms=13_000),
        observed("60005", received_ms=13_501, source_timestamp_ms=13_501),
    )
    warmed = None
    for price in fresh_prices:
        warmed = engine.observe(
            futures=price,
            chainlink=cached_chainlink,
            now_ms=price.received_ms,
        )

    warmed_signal = warmed.signal_for(MODEL_VERSION)
    assert warmed_signal.state == WAITING_FOR_NEW_CHAINLINK_ANCHOR
    assert warmed_signal.anchor is None
    assert warmed_signal.projection is None

    new_chainlink = observed(
        "50000",
        received_ms=13_502,
        source_timestamp_ms=13_502,
    )
    valid = engine.observe(
        futures=fresh_prices[-1],
        chainlink=new_chainlink,
        now_ms=13_502,
    ).signal_for(MODEL_VERSION)

    assert valid.valid is True
    assert valid.anchor.futures_reference == fresh_prices[0]


def test_first_poll_never_anchors_even_when_model_lag_is_shorter_than_freshness():
    short_model = CatchupModel("short_lag", 500, Decimal("1"))
    engine = make_engine(
        models=(short_model,),
        history_retention_ms=6_000,
    )
    cached_futures = observed(
        "60000",
        received_ms=9_500,
        source_timestamp_ms=9_500,
    )
    cached_chainlink = observed(
        "50000",
        received_ms=10_000,
        source_timestamp_ms=10_000,
    )

    first = engine.observe(
        futures=cached_futures,
        chainlink=cached_chainlink,
        now_ms=10_000,
    ).signal_for("short_lag")

    assert first.status == ANCHOR_HISTORY_MISSING
    assert first.anchor is None
    assert first.projection is None


def test_receive_age_clamps_small_mget_race_to_zero():
    engine = make_engine()
    reference = observed("60000", received_ms=6_500, source_timestamp_ms=6_500)
    current = observed("60060", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=reference, chainlink=None, now_ms=6_499)
    engine.observe(futures=current, chainlink=None, now_ms=9_899)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=9_999,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is True
    assert signal.futures_received_age_ms == 99
    assert signal.chainlink_received_age_ms == 0


@pytest.mark.parametrize("source", ["futures", "chainlink"])
def test_excessive_future_receive_timestamp_is_invalid_without_poisoning_state(
    source,
):
    engine, _, futures_now, chainlink, _ = build_valid_engine()
    future_received_ms = 10_252
    futures = (
        observed(
            "90000",
            received_ms=future_received_ms,
            source_timestamp_ms=10_001,
        )
        if source == "futures"
        else futures_now
    )
    current_chainlink = (
        observed(
            "90000",
            received_ms=future_received_ms,
            source_timestamp_ms=10_001,
        )
        if source == "chainlink"
        else chainlink
    )

    invalid = engine.observe(
        futures=futures,
        chainlink=current_chainlink,
        now_ms=10_001,
    )
    recovered = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=10_002,
    ).signal_for(MODEL_VERSION)

    assert invalid.timestamp_regression_sources == (source,)
    assert invalid.signal_for(MODEL_VERSION).status == TIMESTAMP_REGRESSION
    assert invalid.signal_for(MODEL_VERSION).projection is None
    assert recovered.valid is True


def test_reference_lookup_never_uses_an_observation_after_the_target():
    engine = make_engine()
    eligible = observed("60000", received_ms=6_450, source_timestamp_ms=100)
    future_side = observed("90000", received_ms=6_550, source_timestamp_ms=200)
    current = observed("60060", received_ms=9_900, source_timestamp_ms=300)

    engine.observe(futures=eligible, chainlink=None, now_ms=6_450)
    engine.observe(futures=future_side, chainlink=None, now_ms=6_550)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)
    signal = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=20_000),
        now_ms=10_000,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is True
    assert signal.anchor.futures_reference == eligible
    assert signal.futures_reference_target_ms == 6_500
    assert signal.futures_reference_gap_ms == 50
    assert signal.projection.projected_chainlink == Decimal("50050")


@pytest.mark.parametrize(
    ("reference_received_ms", "expected_valid", "expected_status"),
    [
        (6_250, True, VALID),
        (6_249, False, ANCHOR_REFERENCE_GAP),
    ],
)
def test_reference_gap_boundary_is_inclusive(
    reference_received_ms,
    expected_valid,
    expected_status,
):
    engine = make_engine()
    reference = observed(
        "60000",
        received_ms=reference_received_ms,
        source_timestamp_ms=reference_received_ms,
    )
    current = observed("60060", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=reference, chainlink=None, now_ms=reference_received_ms)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is expected_valid
    assert signal.status == expected_status
    if expected_valid:
        assert signal.futures_reference_gap_ms == 250
        assert signal.projection is not None
    else:
        assert signal.futures_reference_gap_ms == 251
        assert signal.projection is None


def test_missing_reference_never_uses_the_future_side():
    engine = make_engine()
    future_side = observed("60000", received_ms=6_501, source_timestamp_ms=6_501)
    current = observed("60060", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=future_side, chainlink=None, now_ms=6_501)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is False
    assert signal.status == ANCHOR_HISTORY_MISSING
    assert signal.anchor is None
    assert signal.projection is None


def test_candidate_models_anchor_and_project_independently():
    models = (
        CatchupModel("l3000", 3_000, Decimal("1")),
        CatchupModel("l3500", 3_500, Decimal("1")),
        CatchupModel("l4000", 4_000, Decimal("1")),
    )
    engine = make_engine(models=models)
    history = (
        observed("48000", received_ms=6_000, source_timestamp_ms=6_000),
        observed("50000", received_ms=6_500, source_timestamp_ms=6_500),
        observed("60000", received_ms=7_000, source_timestamp_ms=7_000),
        observed("60000", received_ms=9_900, source_timestamp_ms=9_900),
    )
    for price in history:
        engine.observe(futures=price, chainlink=None, now_ms=price.received_ms)

    result = engine.observe(
        futures=history[-1],
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    )

    expected = {
        "l3000": ("60000", "50000", "0", "0"),
        "l3500": ("50000", "60000", "10000", "2000"),
        "l4000": ("48000", "62500", "12500", "2500"),
    }
    for version, values in expected.items():
        reference, projected, pending, bps = values
        signal = result.signal_for(version)
        assert signal.valid is True
        assert signal.anchor.futures_reference.value == Decimal(reference)
        assert signal.projection.projected_chainlink == Decimal(projected)
        assert signal.projection.pending_move == Decimal(pending)
        assert signal.projection.pending_move_bps == Decimal(bps)


def test_candidate_anchor_failure_does_not_invalidate_another_model():
    models = (
        CatchupModel("l3000", 3_000, Decimal("1")),
        CatchupModel("l4000", 4_000, Decimal("1")),
    )
    engine = make_engine(models=models)
    reference = observed("60000", received_ms=6_900, source_timestamp_ms=6_900)
    current = observed("60060", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=reference, chainlink=None, now_ms=6_900)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    result = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    )

    assert result.signal_for("l3000").valid is True
    assert result.signal_for("l4000").status == ANCHOR_HISTORY_MISSING
    assert result.signal_for("l4000").projection is None


def test_candidate_model_error_does_not_invalidate_another_model():
    models = (
        CatchupModel("beta_1", 3_500, Decimal("1")),
        CatchupModel("beta_2", 3_500, Decimal("2")),
    )
    engine = make_engine(models=models)
    reference = observed("100", received_ms=6_500, source_timestamp_ms=6_500)
    current = observed("40", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=reference, chainlink=None, now_ms=6_500)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    result = engine.observe(
        futures=current,
        chainlink=observed("50", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    )

    assert result.signal_for("beta_1").valid is True
    assert result.signal_for("beta_1").projection.projected_chainlink == Decimal("20")
    assert result.signal_for("beta_2").status == MODEL_ERROR
    assert result.signal_for("beta_2").projection is None


def test_same_price_chainlink_refresh_reanchors_and_removes_pending_move():
    engine, _, futures_now, _, initial = build_valid_engine()
    assert initial.signal_for(MODEL_VERSION).projection.pending_move == Decimal("50")
    current = observed("60060", received_ms=13_400, source_timestamp_ms=13_380)
    engine.observe(
        futures=current,
        chainlink=initial.signal_for(MODEL_VERSION).chainlink_now,
        now_ms=13_400,
    )
    refreshed_chainlink = observed(
        "50000",
        received_ms=13_500,
        source_timestamp_ms=13_480,
    )

    signal = engine.observe(
        futures=current,
        chainlink=refreshed_chainlink,
        now_ms=13_500,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is True
    assert signal.anchor.chainlink == refreshed_chainlink
    assert signal.anchor.futures_reference == futures_now
    assert signal.projection.projected_chainlink == Decimal("50000")
    assert signal.projection.pending_move == Decimal("0")
    assert signal.projection.pending_move_bps == Decimal("0")


def test_chainlink_catchup_is_not_counted_twice():
    engine, _, futures_now, anchor_chainlink, _ = build_valid_engine()
    current = observed("60060", received_ms=13_400, source_timestamp_ms=13_380)
    engine.observe(futures=current, chainlink=anchor_chainlink, now_ms=13_400)

    signal = engine.observe(
        futures=current,
        chainlink=observed(
            "50050",
            received_ms=13_500,
            source_timestamp_ms=13_480,
        ),
        now_ms=13_500,
    ).signal_for(MODEL_VERSION)

    assert signal.anchor.futures_reference == futures_now
    assert signal.projection.projected_chainlink == Decimal("50050")
    assert signal.projection.pending_move == Decimal("0")


def test_failed_chainlink_refresh_discards_the_previous_valid_anchor():
    engine, _, _, anchor_chainlink, valid_result = build_valid_engine()
    old_anchor = valid_result.signal_for(MODEL_VERSION).anchor
    current = observed("60070", received_ms=13_900, source_timestamp_ms=13_880)
    engine.observe(futures=current, chainlink=anchor_chainlink, now_ms=13_900)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50010", received_ms=14_000, source_timestamp_ms=13_980),
        now_ms=14_000,
    ).signal_for(MODEL_VERSION)

    assert signal.status == ANCHOR_REFERENCE_GAP
    assert signal.futures_reference_gap_ms == 600
    assert signal.anchor is None
    assert signal.projection is None
    assert old_anchor is not None


def test_freshness_uses_received_time_and_exact_threshold_is_fresh():
    engine, _, futures_now, chainlink, _ = build_valid_engine()

    exact_futures_threshold = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=10_900,
    ).signal_for(MODEL_VERSION)
    stale_futures = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=10_901,
    ).signal_for(MODEL_VERSION)

    assert exact_futures_threshold.valid is True
    assert stale_futures.status == FUTURES_STALE
    assert stale_futures.projection is None

    fresh_futures = observed(
        "60060",
        received_ms=15_000,
        source_timestamp_ms=14_980,
    )
    exact_chainlink_threshold = engine.observe(
        futures=fresh_futures,
        chainlink=chainlink,
        now_ms=15_000,
    ).signal_for(MODEL_VERSION)
    stale_chainlink = engine.observe(
        futures=fresh_futures,
        chainlink=chainlink,
        now_ms=15_001,
    ).signal_for(MODEL_VERSION)

    assert exact_chainlink_threshold.valid is True
    assert stale_chainlink.status == CHAINLINK_STALE
    assert stale_chainlink.projection is None


def test_stale_futures_still_advances_ordering_watermarks():
    engine, _, _, chainlink, _ = build_valid_engine()
    stale = observed(
        "60061",
        received_ms=10_001,
        source_timestamp_ms=11_000,
    )
    stale_result = engine.observe(
        futures=stale,
        chainlink=chainlink,
        now_ms=11_002,
    )
    regressed = engine.observe(
        futures=observed(
            "60062",
            received_ms=11_003,
            source_timestamp_ms=10_500,
        ),
        chainlink=chainlink,
        now_ms=11_003,
    )

    assert stale_result.signal_for(MODEL_VERSION).status == FUTURES_STALE
    assert regressed.timestamp_regression_sources == ("futures",)
    assert regressed.futures_history_size == 0
    assert regressed.signal_for(MODEL_VERSION).status == TIMESTAMP_REGRESSION
    assert regressed.signal_for(MODEL_VERSION).anchor is None


def test_source_timestamp_age_does_not_control_freshness():
    engine = make_engine()
    reference = observed("60000", received_ms=6_400, source_timestamp_ms=1)
    current = observed("60060", received_ms=9_900, source_timestamp_ms=2)
    engine.observe(futures=reference, chainlink=None, now_ms=6_400)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=3),
        now_ms=10_000,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is True
    assert signal.futures_received_age_ms == 100
    assert signal.chainlink_received_age_ms == 0


@pytest.mark.parametrize(
    ("missing_source", "expected_status"),
    [
        ("futures", FUTURES_UNAVAILABLE),
        ("chainlink", CHAINLINK_UNAVAILABLE),
    ],
)
def test_missing_current_input_immediately_removes_projection_and_recovers(
    missing_source,
    expected_status,
):
    engine, _, futures_now, chainlink, _ = build_valid_engine()
    futures = None if missing_source == "futures" else futures_now
    current_chainlink = None if missing_source == "chainlink" else chainlink

    invalid = engine.observe(
        futures=futures,
        chainlink=current_chainlink,
        now_ms=10_001,
    ).signal_for(MODEL_VERSION)
    recovered = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=10_002,
    ).signal_for(MODEL_VERSION)

    assert invalid.status == expected_status
    assert invalid.projection is None
    assert recovered.valid is True
    assert recovered.projection is not None


@pytest.mark.parametrize("source", ["futures", "chainlink"])
def test_received_timestamp_regression_resets_dependent_state(source):
    engine, _, futures_now, chainlink, valid_result = build_valid_engine()
    assert valid_result.signal_for(MODEL_VERSION).valid is True
    if source == "futures":
        regressing_futures = observed(
            "60061",
            received_ms=9_899,
            source_timestamp_ms=10_001,
        )
        regressing_chainlink = chainlink
    else:
        regressing_futures = futures_now
        regressing_chainlink = observed(
            "50001",
            received_ms=9_999,
            source_timestamp_ms=10_001,
        )

    regressed = engine.observe(
        futures=regressing_futures,
        chainlink=regressing_chainlink,
        now_ms=10_001,
    )
    repeated = engine.observe(
        futures=regressing_futures,
        chainlink=regressing_chainlink,
        now_ms=10_002,
    ).signal_for(MODEL_VERSION)

    signal = regressed.signal_for(MODEL_VERSION)
    assert signal.status == TIMESTAMP_REGRESSION
    assert signal.projection is None
    assert regressed.timestamp_regression_sources == (source,)
    assert repeated.valid is False
    assert repeated.anchor is None
    assert repeated.projection is None
    if source == "futures":
        assert regressed.futures_history_size == 0
    else:
        assert regressed.futures_history_size == 2


@pytest.mark.parametrize("source", ["futures", "chainlink"])
def test_source_timestamp_regression_resets_dependent_state(source):
    engine, _, futures_now, chainlink, _ = build_valid_engine()
    if source == "futures":
        futures = observed(
            "60061",
            received_ms=10_001,
            source_timestamp_ms=9_879,
        )
        current_chainlink = chainlink
    else:
        futures = futures_now
        current_chainlink = observed(
            "50001",
            received_ms=10_001,
            source_timestamp_ms=9_979,
        )

    result = engine.observe(
        futures=futures,
        chainlink=current_chainlink,
        now_ms=10_001,
    )
    repeated = engine.observe(
        futures=futures,
        chainlink=current_chainlink,
        now_ms=10_002,
    ).signal_for(MODEL_VERSION)

    assert result.timestamp_regression_sources == (source,)
    assert result.signal_for(MODEL_VERSION).status == TIMESTAMP_REGRESSION
    assert result.signal_for(MODEL_VERSION).projection is None
    assert repeated.anchor is None
    assert repeated.projection is None
    if source == "futures":
        assert result.futures_history_size == 0
    else:
        assert result.futures_history_size == 2


def test_simultaneous_source_regressions_are_both_reported_and_reset():
    engine, _, _, _, _ = build_valid_engine()

    result = engine.observe(
        futures=observed(
            "60061",
            received_ms=9_899,
            source_timestamp_ms=9_879,
        ),
        chainlink=observed(
            "50001",
            received_ms=9_999,
            source_timestamp_ms=9_979,
        ),
        now_ms=10_001,
    )

    assert result.timestamp_regression_sources == ("futures", "chainlink")
    assert result.futures_history_size == 0
    assert result.signal_for(MODEL_VERSION).status == TIMESTAMP_REGRESSION
    assert result.signal_for(MODEL_VERSION).anchor is None
    assert result.signal_for(MODEL_VERSION).projection is None


def test_futures_regression_does_not_erase_chainlink_source_watermark():
    engine, _, _, _, _ = build_valid_engine()
    regressing_futures = observed(
        "60061",
        received_ms=9_899,
        source_timestamp_ms=10_001,
    )
    chainlink_without_source_time = observed(
        "50001",
        received_ms=10_001,
        source_timestamp_ms=None,
    )
    engine.observe(
        futures=regressing_futures,
        chainlink=chainlink_without_source_time,
        now_ms=10_001,
    )

    result = engine.observe(
        futures=regressing_futures,
        chainlink=observed(
            "50002",
            received_ms=10_002,
            source_timestamp_ms=9_000,
        ),
        now_ms=10_002,
    )

    assert result.timestamp_regression_sources == ("chainlink",)
    assert result.signal_for(MODEL_VERSION).status == TIMESTAMP_REGRESSION


def test_engine_clock_regression_resets_all_state():
    engine, _, futures_now, chainlink, _ = build_valid_engine()

    regressed = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=9_999,
    )
    repeated = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=10_000,
    )

    assert regressed.timestamp_regression_sources == ("engine_clock",)
    assert regressed.futures_history_size == 0
    assert regressed.signal_for(MODEL_VERSION).status == TIMESTAMP_REGRESSION
    assert regressed.signal_for(MODEL_VERSION).anchor is None
    assert repeated.futures_history_size == 0
    assert repeated.signal_for(MODEL_VERSION).anchor is None
    assert repeated.signal_for(MODEL_VERSION).projection is None


def test_exact_duplicate_identities_do_not_append_or_reanchor():
    engine, _, futures_now, chainlink, result = build_valid_engine()
    original = result.signal_for(MODEL_VERSION)

    repeated_result = engine.observe(
        futures=futures_now,
        chainlink=chainlink,
        now_ms=10_001,
    )
    repeated = repeated_result.signal_for(MODEL_VERSION)

    assert repeated_result.futures_history_size == 2
    assert repeated.anchor is original.anchor
    assert repeated.projection == original.projection


def test_source_timestamp_change_alone_is_a_new_chainlink_identity():
    engine, _, futures_now, chainlink, result = build_valid_engine()
    old_anchor = result.signal_for(MODEL_VERSION).anchor
    refreshed = ObservedPrice(
        value=chainlink.value,
        source_timestamp_ms=chainlink.source_timestamp_ms + 1,
        received_ms=chainlink.received_ms,
    )

    signal = engine.observe(
        futures=futures_now,
        chainlink=refreshed,
        now_ms=10_001,
    ).signal_for(MODEL_VERSION)

    assert signal.valid is True
    assert signal.anchor is not old_anchor
    assert signal.anchor.chainlink == refreshed


def test_latest_ingested_futures_observation_wins_an_equal_receive_time_tie():
    engine = make_engine()
    first = observed("60000", received_ms=6_500, source_timestamp_ms=6_400)
    corrected = observed("75000", received_ms=6_500, source_timestamp_ms=6_401)
    current = observed("60000", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=first, chainlink=None, now_ms=6_500)
    engine.observe(futures=corrected, chainlink=None, now_ms=6_500)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50000", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    ).signal_for(MODEL_VERSION)

    assert signal.anchor.futures_reference == corrected
    assert signal.projection.projected_chainlink == Decimal("40000")


def test_futures_history_is_pruned_by_receive_time_and_keeps_cutoff_equality():
    engine = make_engine()
    oldest = observed("60000", received_ms=0, source_timestamp_ms=0)
    cutoff = observed("60001", received_ms=1, source_timestamp_ms=1)
    current = observed("60002", received_ms=10_001, source_timestamp_ms=10_001)
    engine.observe(futures=oldest, chainlink=None, now_ms=0)
    engine.observe(futures=cutoff, chainlink=None, now_ms=1)
    result = engine.observe(futures=current, chainlink=None, now_ms=10_001)

    assert result.futures_history_size == 2


def test_projection_model_error_does_not_escape_the_engine():
    model = CatchupModel("high_beta", 3_500, Decimal("2"))
    engine = make_engine(models=(model,))
    reference = observed("100", received_ms=6_500, source_timestamp_ms=6_500)
    current = observed("40", received_ms=9_900, source_timestamp_ms=9_900)
    engine.observe(futures=reference, chainlink=None, now_ms=6_500)
    engine.observe(futures=current, chainlink=None, now_ms=9_900)

    signal = engine.observe(
        futures=current,
        chainlink=observed("50", received_ms=10_000, source_timestamp_ms=10_000),
        now_ms=10_000,
    ).signal_for("high_beta")

    assert signal.status == MODEL_ERROR
    assert signal.projection is None


def test_projection_rejects_non_finite_pending_basis_points():
    model = CatchupModel("overflow", 3_500, Decimal("1"))
    anchor = ModelAnchor(
        chainlink=observed("1E-9", received_ms=10_000),
        futures_reference=observed("1", received_ms=6_500),
    )
    with localcontext() as context:
        context.Emax = 9
        context.Emin = -9
        context.traps[Overflow] = False
        with pytest.raises(ValueError, match="basis points"):
            project_from_anchor(
                model=model,
                anchor=anchor,
                futures_now=observed("1E+9", received_ms=10_000),
            )


def test_market_expiry_boundary_does_not_reset_model_state():
    market_end_ms = 1_783_459_500_000
    engine = make_engine(
        futures_stale_ms=10_000,
        chainlink_stale_ms=10_000,
        history_retention_ms=20_000,
    )
    reference = observed(
        "60000",
        received_ms=market_end_ms - 7_000,
        source_timestamp_ms=market_end_ms - 7_010,
    )
    current = observed(
        "60060",
        received_ms=market_end_ms - 3_600,
        source_timestamp_ms=market_end_ms - 3_610,
    )
    chainlink = observed(
        "50000",
        received_ms=market_end_ms - 3_500,
        source_timestamp_ms=market_end_ms - 3_510,
    )
    engine.observe(
        futures=reference,
        chainlink=None,
        now_ms=reference.received_ms,
    )
    engine.observe(
        futures=current,
        chainlink=None,
        now_ms=current.received_ms,
    )
    exact = engine.observe(
        futures=current,
        chainlink=chainlink,
        now_ms=market_end_ms - 3_500,
    )
    old_anchor = exact.signal_for(MODEL_VERSION).anchor
    crossing = engine.observe(
        futures=current,
        chainlink=chainlink,
        now_ms=market_end_ms - 3_499,
    )
    rolled = engine.observe(
        futures=current,
        chainlink=chainlink,
        now_ms=market_end_ms,
    )

    assert exact.signal_for(MODEL_VERSION).full_horizon_before_market_end is True
    assert crossing.signal_for(MODEL_VERSION).full_horizon_before_market_end is False
    assert exact.market.market_id == 5_944_864
    assert rolled.market.market_id == 5_944_865
    assert rolled.market.market_end_ms == 1_783_459_800_000
    assert rolled.signal_for(MODEL_VERSION).valid is True
    assert rolled.signal_for(MODEL_VERSION).anchor is old_anchor
    assert rolled.signal_for(MODEL_VERSION).full_horizon_before_market_end is True


@pytest.mark.parametrize(
    ("value", "error_type"),
    [
        (1.0, TypeError),
        (Decimal("0"), ValueError),
        (Decimal("-1"), ValueError),
        (Decimal("NaN"), ValueError),
        (Decimal("Infinity"), ValueError),
    ],
)
def test_observed_price_rejects_non_decimal_or_invalid_prices(value, error_type):
    with pytest.raises(error_type):
        ObservedPrice(value=value, source_timestamp_ms=None, received_ms=1)


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        ("received_ms", True, TypeError),
        ("received_ms", -1, ValueError),
        ("source_timestamp_ms", False, TypeError),
        ("source_timestamp_ms", -1, ValueError),
    ],
)
def test_observed_price_rejects_invalid_timestamps(field, value, error_type):
    arguments = {
        "value": Decimal("1"),
        "source_timestamp_ms": None,
        "received_ms": 1,
    }
    arguments[field] = value
    with pytest.raises(error_type):
        ObservedPrice(**arguments)


def test_engine_validates_models_and_history_retention():
    duplicate = CatchupModel(MODEL_VERSION, 3_000, Decimal("1"))
    with pytest.raises(ValueError, match="unique"):
        make_engine(models=(MODEL, duplicate))
    with pytest.raises(ValueError, match="history_retention_ms"):
        make_engine(history_retention_ms=8_749)
    with pytest.raises(ValueError, match="at least one"):
        make_engine(models=())


@pytest.mark.parametrize(
    "overrides",
    [
        {"futures_stale_ms": 0},
        {"chainlink_stale_ms": 0},
        {"reference_max_gap_ms": -1},
        {"max_future_skew_ms": -1},
    ],
)
def test_engine_rejects_invalid_timing_thresholds(overrides):
    with pytest.raises(ValueError):
        make_engine(**overrides)


@pytest.mark.parametrize(
    ("arguments", "error_type"),
    [
        ({"version": "", "lag_ms": 1, "beta": Decimal("1")}, ValueError),
        ({"version": "x", "lag_ms": 0, "beta": Decimal("1")}, ValueError),
        ({"version": "x", "lag_ms": 1, "beta": 1.0}, TypeError),
        ({"version": "x", "lag_ms": 1, "beta": Decimal("NaN")}, ValueError),
        ({"version": "x", "lag_ms": 1, "beta": Decimal("-1")}, ValueError),
    ],
)
def test_catchup_model_validation(arguments, error_type):
    with pytest.raises(error_type):
        CatchupModel(**arguments)
