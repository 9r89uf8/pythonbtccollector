from copy import deepcopy
from dataclasses import replace
from decimal import Decimal, localcontext
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from price_collector.dashboard_api import PRICE_ENDPOINT, TWAP_SOURCE
from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent
from price_collector.market import market_for_sample_second
from price_collector.market_conditions import build_observation
from price_collector.polymarket_evidence import price_request_params
from price_collector.settlement import make_context

START = 1789593000000
END = START + 300_000
NS = 1_000_000
MONO_ORIGIN = START - 300_000


def context(start=START, value="100"):
    market = SimpleNamespace(
        window=market_for_sample_second(start), start_ms=start, end_ms=start + 300_000,
        condition_id="condition", up_token_id="up", down_token_id="down",
        settlement_reference="chainlink_twap", settlement_window_s=60,
        settlement_source_url=TWAP_SOURCE, settlement_rule_version="btc-5m-twap-60",
    )
    return make_context(market, {
        "source_url": PRICE_ENDPOINT, "request_params": price_request_params(market),
        "price_to_beat": value, "completed": False, "incomplete": True,
    }, status="ok", http_status=200, requested_wall_ns=start * NS,
        received_wall_ns=start * NS, requested_monotonic_ns=(start - MONO_ORIGIN) * NS,
        received_monotonic_ns=(start - MONO_ORIGIN) * NS, identity_validated=True)


def decision(now=START + 100_000, *, spot="100.03", twap="99.99"):
    engine = GhostTwapEngine("market-condition-test", GhostPolicy(enabled=True))
    source = now // 1000 * 1000 - 1000
    for seq, (feed, value, age) in enumerate((("spot", spot, 500), ("twap", twap, 250)), 1):
        engine.accept(PriceEvent(feed, Decimal(value), source, (now - age) * NS,
            (now - age - MONO_ORIGIN) * NS, seq, str(seq), 60 if feed == "twap" else None))
    return engine.snapshot("1", now * NS, (now - MONO_ORIGIN) * NS)


def observe(snapshot=None, reference=None, **kwargs):
    return build_observation(snapshot or decision(), context() if reference is None else reference,
        kwargs.get("wall", START * NS), kwargs.get("mono", (START - MONO_ORIGIN) * NS))


def test_observation_covers_whole_market_and_needs_no_ghost_history():
    snapshot = decision()
    before = snapshot.to_audit_json()
    assert all(forecast.price is None and "missing_slots" in forecast.reasons
               for forecast in snapshot.forecasts)
    result = observe(snapshot)
    assert result["status"] == "available" and result["quality"] == "healthy"
    assert result["remaining_ms"] == 200_000
    assert result["schema_version"] == 4 and result["kind"] == "market_conditions"
    assert result["model_version"] == "market-conditions-v1"
    assert result["rule_version"] == "historical-market-conditions-v1"
    assert result["observation_window_s"] == 300 and result["sampling_interval_ms"] == 5000
    assert result["history_cohort"]
    assert result["target_source_timestamp_ms"] == END
    assert set(result["signals"]) == {"twap", "spot"}
    assert result["signals"]["spot"] == dict(price="100.030000000000000000", side="up",
        signed_lead_usd="0.030000000000000000", lead_bps="3.000000000000000000")
    assert result["signals"]["twap"]["side"] == "down"
    for key in ("projected_price", "slots", "slot_inputs", "counts", "source_horizon_s"):
        assert key not in result
    assert result["current_spot"]["event_id"] == snapshot.current_spot.event_id
    assert result["current_spot"]["source_timestamp_ms"] == snapshot.current_spot.source_timestamp_ms
    assert result["reference"]["available_wall_ns"] == str(START * NS)
    assert int(result["valid_until_wall_ns"]) == snapshot.decision_wall_ns + 2500 * NS
    assert snapshot.to_audit_json() == before
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("remaining", [300_000, 299_999, 240_000, 60_001, 60_000, 1])
def test_all_remaining_times_are_valid_with_current_market_reference(remaining):
    result = observe(decision(END - remaining))
    assert result["status"] == "available"
    assert result["remaining_ms"] == remaining and result["market_end_ms"] == END
    assert int(result["valid_until_wall_ns"]) <= END * NS


def test_exact_close_rolls_into_new_market_and_requires_new_reference():
    snapshot = decision(END)
    previous = observe(snapshot)
    assert previous["status"] == "unavailable"
    assert "reference_unavailable_or_noncausal" in previous["reasons"]
    assert previous["market_start_ms"] == END and previous["remaining_ms"] == 300_000
    current = observe(snapshot, context(END), wall=END * NS, mono=(END - MONO_ORIGIN) * NS)
    assert current["status"] == "available"
    # Fresh source events may predate this market; only the opening is tied to it.
    assert current["current_spot"]["source_timestamp_ms"] < current["market_start_ms"]


@pytest.mark.parametrize("fault", ["missing", "conflict", "wrong_market", "wrong_identity",
    "wrong_params", "completed", "future_wall", "future_mono", "receipt_after_availability"])
def test_reference_identity_and_availability_fail_closed(fault):
    snapshot, reference = decision(), context()
    kwargs = {}
    if fault == "missing": reference = {}
    elif fault == "conflict": reference["conflicted"] = True
    elif fault == "wrong_market": reference["market_id"] += 1
    elif fault == "wrong_identity": reference["identity_validated"] = False
    elif fault == "wrong_params": reference["request_params"]["twapLookbackSeconds"] = "30"
    elif fault == "completed": reference["completed"] = True
    elif fault == "future_wall": kwargs["wall"] = snapshot.decision_wall_ns + 1
    elif fault == "future_mono": kwargs["mono"] = snapshot.decision_monotonic_ns + 1
    else: reference["received_wall_ns"] = str(START * NS + 1)
    result = observe(snapshot, reference, **kwargs)
    assert result["status"] == "unavailable"
    assert "reference_unavailable_or_noncausal" in result["reasons"]


@pytest.mark.parametrize("feed", ["spot", "twap"])
@pytest.mark.parametrize("fault", ["missing", "stale_source", "stale_receipt_wall", "stale_receipt_mono",
    "future_source", "future_receipt_wall", "future_receipt_mono", "source_after_receipt", "wrong_feed"])
def test_both_inputs_require_canonical_identity_and_fresh_causal_clocks(feed, fault):
    snapshot = decision()
    event = getattr(snapshot, "current_" + feed)
    if fault == "missing": event = None
    elif fault == "stale_source": event = replace(event, source_timestamp_ms=event.source_timestamp_ms - 6000)
    elif fault == "stale_receipt_wall": event = replace(event, received_wall_ns=snapshot.decision_wall_ns - 3001 * NS)
    elif fault == "stale_receipt_mono": event = replace(event, received_monotonic_ns=snapshot.decision_monotonic_ns - 3001 * NS)
    elif fault == "future_source": event = replace(event, source_timestamp_ms=snapshot.decision_wall_ns // NS + 1000)
    elif fault == "future_receipt_wall": event = replace(event, received_wall_ns=snapshot.decision_wall_ns + 1)
    elif fault == "future_receipt_mono": event = replace(event, received_monotonic_ns=snapshot.decision_monotonic_ns + 1)
    elif fault == "source_after_receipt": event = replace(event, received_wall_ns=event.source_timestamp_ms * NS - 1)
    else: event = getattr(snapshot, "current_" + ("twap" if feed == "spot" else "spot"))
    result = observe(replace(snapshot, **{"current_" + feed: event}))
    assert result["status"] == "unavailable" and result["quality"] == "unavailable"
    assert any(feed in reason for reason in result["reasons"])


def test_actual_engine_faults_remain_binding_and_empty_history_does_not():
    snapshot = replace(decision(), slots=(), forecasts=(), valid_until_wall_ns=0)
    assert observe(snapshot)["status"] == "available"
    faulty = observe(replace(snapshot, reasons=("acceptance_order",)))
    assert faulty["status"] == "unavailable" and "acceptance_order" in faulty["reasons"]


def test_disconnect_is_unavailable_then_fresh_reconnect_needs_no_history_warmup():
    original = decision()
    engine = GhostTwapEngine(original.run_id, original.policy)
    engine.accept(original.current_spot)
    engine.accept(original.current_twap)
    wall, mono = original.decision_wall_ns, original.decision_monotonic_ns
    engine.record_gap("spot", "connection_end", observed_wall_ns=wall, observed_monotonic_ns=mono)
    waiting = engine.snapshot("2", wall, mono)
    assert waiting.spot_reconnect.status == "waiting"
    assert observe(waiting)["status"] == "unavailable"
    engine.accept(PriceEvent("spot", Decimal("100.04"), wall // NS,
        wall + NS, mono + NS, 3, "fresh-reconnect"))
    recovered = engine.snapshot("3", wall + NS, mono + NS)
    assert recovered.spot_reconnect.status == "retained"
    assert all(forecast.price is None for forecast in recovered.forecasts)
    assert observe(recovered)["status"] == "available"


@pytest.mark.parametrize("clock", ["source", "receipt_wall", "receipt_mono"])
def test_exact_freshness_expiry_is_unavailable_and_never_extended(clock):
    snapshot = decision()
    event = snapshot.current_spot
    if clock == "source":
        event = replace(event, source_timestamp_ms=snapshot.decision_wall_ns // NS - 5000)
    elif clock == "receipt_wall":
        event = replace(event, source_timestamp_ms=snapshot.decision_wall_ns // NS - 4000,
                        received_wall_ns=snapshot.decision_wall_ns - 3000 * NS)
    else:
        event = replace(event, received_monotonic_ns=snapshot.decision_monotonic_ns - 3000 * NS)
    result = observe(replace(snapshot, current_spot=event))
    assert result["status"] == "unavailable" and "inputs_expired" in result["reasons"]
    assert int(result["valid_until_wall_ns"]) == snapshot.decision_wall_ns


@pytest.mark.parametrize("changes", [{"enabled": False}, {"source_max_age_ms": 6000},
    {"receipt_max_age_ms": 4000}])
def test_disabled_or_relaxed_input_policies_are_unavailable(changes):
    snapshot = decision()
    result = observe(replace(snapshot, policy=replace(snapshot.policy, **changes)))
    assert result["status"] == "unavailable" and "unsupported_input_policy" in result["reasons"]


def test_exact_e18_margins_and_ambient_decimal_context_independence():
    snapshot = decision(spot="100.019999999999999999", twap="100")
    with localcontext() as arithmetic:
        arithmetic.prec = 8
        result = observe(snapshot)
    assert result["signals"]["spot"]["lead_bps"] == "1.999999999999999900"
    assert result["signals"]["twap"]["side"] == "tie"
    assert result["signals"]["twap"]["signed_lead_usd"] == "0.000000000000000000"


def test_reference_snapshot_is_frozen_and_missing_reference_has_no_substitute():
    reference = context()
    result = observe(reference=reference)
    before = deepcopy(result["reference"])
    reference["price_to_beat"] = "101"
    reference["request_params"]["twapLookbackSeconds"] = "30"
    assert result["reference"] == before
    missing = build_observation(decision(), None, 0, 0)
    assert missing["status"] == "unavailable"
    assert all(signal["side"] is None and signal["lead_bps"] is None
               for signal in missing["signals"].values())


def test_schema_replaces_existing_schedule_check_and_preserves_older_versions():
    sql = (Path(__file__).parents[1] / "schema.sql").read_text(encoding="utf-8")
    check = sql[sql.index("-- Replace the original anonymous final-30s"):]
    assert "DROP CONSTRAINT settlement_audit_observation_window_check" in check
    for version in ("historical-market-conditions-v1", "historical-settlement-v2"):
        assert version in check
    assert "'sampling_interval_ms'='5000'::jsonb THEN 300000" in check
    assert "'sampling_interval_ms'='2000'::jsonb THEN 60000" in check
    assert "THEN 30000" in check
    assert "AND created_ms = decision_wall_ns / 1000000) NOT VALID;" in check
