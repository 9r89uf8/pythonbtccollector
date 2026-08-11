from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal, getcontext

import pytest

from price_collector.twap_shadow import (
    BASIS_WINDOW_MS,
    BPS_QUANTUM,
    FRACTION_QUANTUM,
    HORIZONS_SECONDS,
    MODEL_VERSION,
    PRICE_QUANTUM,
    SCHEMA_VERSION,
    SOURCE_BINANCE_SPOT,
    SOURCE_CHAINLINK_SPOT,
    SOURCE_ENDPOINT_DELAYS_MS,
    SOURCE_FUTURES,
    SOURCE_NAMES,
    SOURCE_OBSERVATION_RETENTION_MS,
    WINDOW_MS,
    ActualTwapObservation,
    SourceObservation,
    TwapShadowModel,
)


D = Decimal


def test_model_v1_definition_is_pinned() -> None:
    assert MODEL_VERSION == 1
    assert WINDOW_MS == 30_000
    assert BASIS_WINDOW_MS == 1_800_000
    assert HORIZONS_SECONDS == (1, 3, 5, 10)
    assert dict(SOURCE_ENDPOINT_DELAYS_MS) == {
        SOURCE_FUTURES: 2_800,
        SOURCE_CHAINLINK_SPOT: 1_700,
        SOURCE_BINANCE_SPOT: 2_500,
    }


def _observe_source(
    model: TwapShadowModel,
    source: str,
    price: str,
    source_ms: int,
    *,
    received_ms: int | None = None,
    sequence: int = 0,
) -> None:
    model.observe_source(
        SourceObservation(
            source=source,
            price=D(price),
            source_ms=source_ms,
            received_ms=source_ms if received_ms is None else received_ms,
            sequence=sequence,
        )
    )


def _observe_all_sources(
    model: TwapShadowModel,
    price: str,
    source_ms: int,
    *,
    received_ms: int | None = None,
) -> None:
    for source in SOURCE_NAMES:
        _observe_source(
            model,
            source,
            price,
            source_ms,
            received_ms=received_ms,
        )


def _seed_zero_basis(model: TwapShadowModel, provider_ms: int) -> None:
    _observe_all_sources(model, "100", provider_ms - 40_000)
    _observe_all_sources(model, "100", provider_ms, received_ms=provider_ms + 100)
    update = model.observe_actual(
        ActualTwapObservation(
            price=D("100"),
            provider_event_ms=provider_ms,
            received_ms=provider_ms + 500,
        )
    )
    assert all(item.residual_bps == D("0E-8") for item in update.source_residuals)


def _source_forecast(batch, horizon_seconds: int, source: str):
    horizon = next(
        item for item in batch.forecasts if item.horizon_seconds == horizon_seconds
    )
    return next(item for item in horizon.sources if item.source == source)


def test_time_weighted_average_is_exact_and_half_open() -> None:
    model = TwapShadowModel()
    origin_ms = 100_000
    target_ms = origin_ms + 1_000
    end_ms = target_ms - SOURCE_ENDPOINT_DELAYS_MS[SOURCE_FUTURES]
    start_ms = end_ms - 30_000

    _observe_source(model, SOURCE_FUTURES, "90", start_ms - 1_000)
    _observe_source(model, SOURCE_FUTURES, "100", start_ms, sequence=1)
    _observe_source(model, SOURCE_FUTURES, "108", start_ms + 15_000, sequence=1)
    _observe_source(model, SOURCE_FUTURES, "110", start_ms + 15_000, sequence=2)
    # The half-open interval excludes this price entirely.
    _observe_source(model, SOURCE_FUTURES, "999", end_ms)

    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    forecast = _source_forecast(batch, 1, SOURCE_FUTURES)

    assert forecast.window_start_ms == start_ms
    assert forecast.window_end_ms == end_ms
    assert forecast.raw_twap == D("105.000000000000000000")
    assert forecast.known_fraction == D("1.00000000")
    assert forecast.adjusted_twap is None
    assert "basis_warming_up" in forecast.quality_flags


def test_irregular_step_durations_are_weighted_in_milliseconds() -> None:
    model = TwapShadowModel()
    origin_ms = 200_000
    target_ms = origin_ms + 1_000
    end_ms = target_ms - SOURCE_ENDPOINT_DELAYS_MS[SOURCE_CHAINLINK_SPOT]
    start_ms = end_ms - 30_000

    _observe_source(model, SOURCE_CHAINLINK_SPOT, "100", start_ms)
    _observe_source(model, SOURCE_CHAINLINK_SPOT, "130", start_ms + 10_000)
    _observe_source(model, SOURCE_CHAINLINK_SPOT, "130", origin_ms)

    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 50)
    forecast = _source_forecast(batch, 1, SOURCE_CHAINLINK_SPOT)
    assert forecast.raw_twap == D("120.000000000000000000")


def test_basis_sign_same_second_replacement_and_exact_eviction() -> None:
    model = TwapShadowModel()
    provider_ms = 1_000_000
    _observe_all_sources(model, "100", provider_ms - 40_000)
    _observe_all_sources(model, "100", provider_ms, received_ms=provider_ms + 100)

    first = model.observe_actual(
        ActualTwapObservation(D("101"), provider_ms, provider_ms + 500)
    )
    second = model.observe_actual(
        ActualTwapObservation(D("102"), provider_ms, provider_ms + 600, sequence=1)
    )
    assert first.source_residuals[0].residual_bps == D("100.00000000")
    assert second.source_residuals[0].residual_bps == D("200.00000000")

    origin_ms = provider_ms + 1_000
    _observe_all_sources(model, "100", origin_ms, received_ms=origin_ms + 10)
    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    assert batch.basis_sample_counts[SOURCE_FUTURES] == 1
    assert batch.source_bias_bps[SOURCE_FUTURES] == D("200.00000000")

    later_ms = provider_ms + BASIS_WINDOW_MS
    _observe_all_sources(model, "100", later_ms - 40_000)
    _observe_all_sources(model, "100", later_ms, received_ms=later_ms + 10)
    model.observe_actual(
        ActualTwapObservation(D("100"), later_ms, later_ms + 500)
    )
    later_batch = model.forecast(
        origin_ms=later_ms + 1_000,
        issued_ms=later_ms + 1_100,
    )
    assert later_batch.basis_sample_counts[SOURCE_FUTURES] == 1
    assert later_batch.source_bias_bps[SOURCE_FUTURES] == D("0E-8")


def test_all_horizons_use_source_delays_and_flat_hold_known_fraction() -> None:
    model = TwapShadowModel()
    provider_ms = 2_000_000
    _seed_zero_basis(model, provider_ms)
    origin_ms = provider_ms + 1_000
    _observe_all_sources(model, "100", origin_ms, received_ms=origin_ms + 10)

    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    assert tuple(item.horizon_seconds for item in batch.forecasts) == HORIZONS_SECONDS
    assert all(item.value == D("100.000000000000000000") for item in batch.forecasts)
    assert "flat_hold_future" in batch.quality_flags

    expected_known = {
        1: D("1.00000000"),
        3: D("0.95666667"),
        5: D("0.89000000"),
        10: D("0.72333333"),
    }
    for forecast in batch.forecasts:
        assert forecast.target_second_ms == origin_ms + forecast.horizon_seconds * 1_000
        assert forecast.known_fraction == expected_known[forecast.horizon_seconds]
        for source in forecast.sources:
            expected_end = (
                forecast.target_second_ms
                - SOURCE_ENDPOINT_DELAYS_MS[source.source]
            )
            assert source.window_end_ms == expected_end
            assert source.window_start_ms == expected_end - 30_000


def test_cutoffs_exclude_late_receive_and_post_origin_source_events() -> None:
    model = TwapShadowModel()
    provider_ms = 3_000_000
    _seed_zero_basis(model, provider_ms)
    origin_ms = provider_ms + 1_000
    _observe_all_sources(model, "100", origin_ms, received_ms=origin_ms + 10)

    # Both prices would affect a future-held window if either leaked through.
    _observe_source(
        model,
        SOURCE_FUTURES,
        "1000",
        origin_ms - 500,
        received_ms=origin_ms + 500,
    )
    _observe_source(
        model,
        SOURCE_CHAINLINK_SPOT,
        "1000",
        origin_ms + 50,
        received_ms=origin_ms + 50,
    )

    first = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    assert _source_forecast(first, 10, SOURCE_FUTURES).raw_twap == D(
        "100.000000000000000000"
    )
    assert _source_forecast(first, 10, SOURCE_CHAINLINK_SPOT).raw_twap == D(
        "100.000000000000000000"
    )

    # A forecast second is immutable even after a formerly late event becomes visible.
    repeated = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 900)
    assert repeated is first


def test_stale_sources_do_not_contribute_to_consensus() -> None:
    model = TwapShadowModel(stale_after_ms=10_000)
    provider_ms = 4_000_000
    _seed_zero_basis(model, provider_ms)
    origin_ms = provider_ms + 11_000

    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    assert batch.status == "unavailable"
    assert all(item.value is None and item.source_count == 0 for item in batch.forecasts)
    assert all(
        "source_stale" in source.quality_flags
        for horizon in batch.forecasts
        for source in horizon.sources
    )


def test_two_source_consensus_uses_median_and_reports_spread() -> None:
    model = TwapShadowModel(stale_after_ms=10_000)
    provider_ms = 5_000_000
    _seed_zero_basis(model, provider_ms)
    origin_ms = provider_ms + 12_000

    _observe_source(
        model,
        SOURCE_FUTURES,
        "100",
        origin_ms - 5_000,
        received_ms=origin_ms - 4_900,
    )
    _observe_source(
        model,
        SOURCE_CHAINLINK_SPOT,
        "102",
        origin_ms - 5_000,
        received_ms=origin_ms - 4_900,
    )

    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    horizon = batch.forecasts[0]
    contributors = [
        item.adjusted_twap for item in horizon.sources if item.adjusted_twap is not None
    ]
    expected_value = ((contributors[0] + contributors[1]) / D(2)).quantize(
        PRICE_QUANTUM
    )
    expected_spread = (
        (max(contributors) - min(contributors))
        / expected_value
        * D("10000")
    ).quantize(BPS_QUANTUM)

    assert horizon.source_count == 2
    assert horizon.status == "degraded"
    assert horizon.value == expected_value
    assert horizon.source_spread_bps == expected_spread
    assert "partial_consensus" in horizon.quality_flags


def test_recent_ensemble_nowcast_error_uses_nearest_rank_p90() -> None:
    model = TwapShadowModel()
    provider_ms = 6_000_000
    _observe_all_sources(model, "100", provider_ms - 40_000)

    actual_prices = ("100", "110", "100", "125")
    updates = []
    for index, actual_price in enumerate(actual_prices):
        event_ms = provider_ms + index * 1_000
        _observe_all_sources(model, "100", event_ms, received_ms=event_ms + 50)
        updates.append(
            model.observe_actual(
                ActualTwapObservation(
                    D(actual_price),
                    event_ms,
                    event_ms + 500,
                )
            )
        )

    assert updates[0].abs_nowcast_error_bps is None
    assert updates[1].abs_nowcast_error_bps == D("909.09090909")
    assert updates[2].abs_nowcast_error_bps == D("500.00000000")
    assert updates[3].abs_nowcast_error_bps == D("2000.00000000")

    origin_ms = provider_ms + 4_000
    _observe_all_sources(model, "100", origin_ms, received_ms=origin_ms + 10)
    batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    assert batch.recent_error.sample_count == 3
    assert batch.recent_error.p90_abs_error_bps == D("2000.00000000")
    assert all(
        item.estimated_error_bps == D("2000.00000000")
        for item in batch.forecasts
    )


def test_outputs_are_frozen_decimal_quantized_and_live_payload_is_strict() -> None:
    original_precision = getcontext().prec
    getcontext().prec = 6
    try:
        model = TwapShadowModel()
        provider_ms = 7_000_000
        _seed_zero_basis(model, provider_ms)
        origin_ms = provider_ms + 1_000
        _observe_all_sources(
            model,
            "100.1234567890123456789",
            origin_ms,
            received_ms=origin_ms + 1,
        )
        batch = model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)
    finally:
        getcontext().prec = original_precision

    assert batch.schema_version == SCHEMA_VERSION
    assert batch.model_version == MODEL_VERSION
    assert all(
        forecast.value is None or forecast.value.as_tuple().exponent == -18
        for forecast in batch.forecasts
    )
    assert all(
        forecast.known_fraction is None
        or forecast.known_fraction.as_tuple().exponent == -8
        for forecast in batch.forecasts
    )
    assert all(
        item.median_bps is None or item.median_bps.as_tuple().exponent == -8
        for item in batch.source_basis
    )

    with pytest.raises(FrozenInstanceError):
        batch.status = "changed"  # type: ignore[misc]

    payload = batch.to_live_payload()
    assert set(payload) == {
        "schema_version",
        "model_version",
        "origin_second_ms",
        "generated_ms",
        "basis_window_seconds",
        "status",
        "quality_flags",
        "source_bias_bps",
        "basis_sample_counts",
        "recent_p90_abs_error_bps",
        "predictions",
    }
    assert set(payload["source_bias_bps"]) == set(SOURCE_NAMES)
    assert set(payload["basis_sample_counts"]) == set(SOURCE_NAMES)
    assert set(payload["predictions"]) == {"h1", "h3", "h5", "h10"}
    prediction = payload["predictions"]["h1"]
    assert set(prediction) == {
        "horizon_seconds",
        "target_second_ms",
        "target_market_id",
        "expected_actual_received_ms",
        "value",
        "known_fraction",
        "source_count",
        "source_spread_bps",
        "estimated_error_bps",
    }
    assert isinstance(prediction["value"], str)
    assert isinstance(prediction["known_fraction"], str)
    assert payload["quality_flags"] == sorted(set(payload["quality_flags"]))


@pytest.mark.parametrize(
    "factory",
    [
        lambda: SourceObservation(SOURCE_FUTURES, 100.0, 1, 1),
        lambda: ActualTwapObservation(100.0, 1_000, 1_001),
        lambda: SourceObservation(SOURCE_FUTURES, D("NaN"), 1, 1),
        lambda: SourceObservation(SOURCE_FUTURES, D("Infinity"), 1, 1),
        lambda: SourceObservation(SOURCE_FUTURES, D("0"), 1, 1),
        lambda: SourceObservation(SOURCE_FUTURES, D("100"), True, 1),
    ],
)
def test_float_nonfinite_and_invalid_observations_are_rejected(factory) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()


def test_forecast_requires_aligned_origin_and_generation_in_same_second() -> None:
    model = TwapShadowModel()
    with pytest.raises(ValueError, match="aligned"):
        model.forecast(origin_ms=10_001, issued_ms=10_100)
    with pytest.raises(ValueError, match="within"):
        model.forecast(origin_ms=10_000, issued_ms=11_000)


def test_source_history_is_bounded_and_ancient_late_arrivals_are_ignored() -> None:
    model = TwapShadowModel()
    origin_ms = 3_000_000
    model.forecast(origin_ms=origin_ms, issued_ms=origin_ms + 100)

    _observe_source(
        model,
        SOURCE_FUTURES,
        "999",
        origin_ms - SOURCE_OBSERVATION_RETENTION_MS - 1,
        received_ms=origin_ms + 200,
    )
    next_batch = model.forecast(
        origin_ms=origin_ms + 1_000,
        issued_ms=origin_ms + 1_100,
    )
    assert _source_forecast(next_batch, 1, SOURCE_FUTURES).raw_twap is None
