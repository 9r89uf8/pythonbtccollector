from dataclasses import FrozenInstanceError
from decimal import Decimal, localcontext
import json

import pytest

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, HORIZONS, PriceEvent

BASE = 1_800_000_000_000
NS = 1_000_000


def make_event(feed, second, price, sequence, *, receipt_second=None):
    stamp = BASE + second * 1000
    received = BASE + (second if receipt_second is None else receipt_second) * 1000
    return PriceEvent(feed, Decimal(price), stamp, received * NS,
                      (received - BASE) * NS, sequence, str(sequence), 60 if feed == "twap" else None)


def warmed(prices=None):
    engine = GhostTwapEngine("test", GhostPolicy(enabled=True))
    for second in range(39, 101):
        engine.accept(make_event("spot", second, str(prices(second) if prices else 100), second))
    engine.accept(make_event("twap", 100, "100", 101))
    return engine


def snapshot(engine, second=100):
    return engine.snapshot("decision", (BASE + second * 1000) * NS, second * 1_000_000_000)


def test_default_disabled_has_no_inputs_or_publishable_forecasts():
    engine = GhostTwapEngine("disabled")
    assert not engine.accept(make_event("spot", 100, "100", 1))
    decision = snapshot(engine)
    assert decision.included_sequence is None
    assert engine.history_size == 0
    assert len(decision.forecasts) == 6
    assert all(f.price is None and "disabled" in f.reasons for f in decision.forecasts)


def test_permanent_step_replaces_outgoing_prices_and_counts_every_slot():
    # A 60-dollar permanent step at s=100 adds exactly one dollar per entering slot.
    engine = warmed(lambda second: 160 if second == 100 else 100)
    result = snapshot(engine)
    assert len(result.slots) == 89
    assert [f.horizon_s for f in result.forecasts] == list(HORIZONS)
    for forecast, expected in zip(result.forecasts, [100, 100, 101, 103, 108, 128]):
        assert forecast.price == Decimal(expected)
        selected = result.slots[forecast.slot_start_index:forecast.slot_start_index + 60]
        assert len(selected) == 60
        assert selected[0].slot_timestamp_ms == forecast.target_source_timestamp_ms - 62_000
        assert selected[-1].slot_timestamp_ms == forecast.target_source_timestamp_ms - 3_000
        assert sum(vars(forecast.counts).values()) == 60
        assert forecast.counts.future == max(forecast.horizon_s - 3, 0)
        assert forecast.estimated_remaining_ns == forecast.horizon_s * 1_000_000_000
        assert not forecast.estimate_overdue


def test_nonflat_outgoing_history_and_reversal_use_actual_window():
    engine = warmed(lambda second: 120 if second < 60 else 80 if second < 100 else 100)
    result = snapshot(engine)
    for forecast in result.forecasts:
        slots = range(100 + forecast.horizon_s - 62, 100 + forecast.horizon_s - 2)
        expected_prices = [120 if s < 60 else 80 if s < 100 else 100 for s in slots]
        with localcontext() as context:
            context.prec = 80
            expected = (Decimal(sum(expected_prices)) / Decimal(60)).quantize(Decimal("1e-18"))
        assert forecast.price == expected


def test_e18_rounding_is_half_even_and_independent_of_ambient_context():
    for small, expected in [("0.000000000000000030", "100.000000000000000000"),
                            ("0.000000000000000090", "100.000000000000000002")]:
        with localcontext() as high:
            high.prec = 80
            step_price = Decimal(100) + Decimal(small)
        with localcontext() as low:
            low.prec = 3
            engine = warmed(lambda second: step_price if second == 100 else Decimal(100))
            assert snapshot(engine).forecasts[2].price == Decimal(expected)


@pytest.mark.parametrize("price", [100.0, "100", Decimal("NaN"), Decimal("Infinity"),
                                  Decimal(0), Decimal(-1), Decimal("1e20"), Decimal("1e-19")])
def test_prices_require_exact_positive_bounded_decimals(price):
    with pytest.raises(ValueError):
        PriceEvent("spot", price, BASE, BASE * NS, 0, 1, "event")


def test_unsupported_identity_and_clock_types_are_rejected():
    with pytest.raises(ValueError):
        PriceEvent("twap", Decimal(100), BASE, BASE * NS, 0, 1, "event", 30)
    with pytest.raises(ValueError):
        PriceEvent("spot", Decimal(100), BASE + 1, BASE * NS, 0, 1, "event")
    with pytest.raises(ValueError):
        PriceEvent("spot", Decimal(100), BASE, BASE * NS, True, 1, "event")


def test_snapshot_is_deeply_frozen_and_json_keeps_exact_prices_and_clocks():
    engine = warmed()
    result = snapshot(engine)
    before = result.to_audit_json()
    engine.accept(make_event("spot", 100, "120", 102))
    assert result.to_audit_json() == before
    with pytest.raises(FrozenInstanceError):
        result.slots[0].value = Decimal(1)
    with pytest.raises(FrozenInstanceError):
        result.forecasts[0].counts.observed = 0
    audit = json.loads(before)
    live = json.loads(result.to_live_json())
    assert len(audit["slots"]) == 89
    assert "slots" not in live and "slot_inputs" not in live
    assert live["publication_state"] == "not_published"
    assert live["decision_time_ms"] == BASE + 100_000
    assert live["decision_wall_ns"] == str((BASE + 100_000) * NS)
    assert all(f["price"] == "100.000000000000000000" for f in live["forecasts"])
    inputs = {x["sequence"]: x for x in audit["slot_inputs"]}
    for f in audit["forecasts"]:
        selected = audit["slots"][f["slot_start_index"]:f["slot_start_index"] + 60]
        with localcontext() as ctx:
            ctx.prec = 80
            reconstructed = sum((Decimal(inputs[s["input_sequence"]]["value"]) for s in selected), Decimal(0)) / 60
        assert reconstructed == Decimal(f["price"])


def test_market_boundary_uses_target_stamp():
    engine = GhostTwapEngine("boundary", GhostPolicy(enabled=True))
    for second in range(236, 298):
        engine.accept(make_event("spot", second, "100", second))
    engine.accept(make_event("twap", 297, "100", 298))
    decision = snapshot(engine, 297)
    earlier, boundary = decision.forecasts[1:3]
    assert earlier.market.market_id + 1 == boundary.market.market_id
    assert boundary.market.market_start_ms == BASE + 300_000
