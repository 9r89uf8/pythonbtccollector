import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal, localcontext
import json
from types import SimpleNamespace

import httpx
import pytest

from price_collector.dashboard_api import PRICE_ENDPOINT, TWAP_SOURCE
from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent
from price_collector.market import market_for_sample_second
from price_collector.polymarket_evidence import (
    CollectionEvidenceRuntime, EvidenceSettings, parse_price_observation, price_request_params,
)
from price_collector.settlement import (
    CONTEXT_KEY, DAY_MS, SettlementSettings, build_projection, decode_context,
    latch_context, make_context, public_payload,
)

START = 1789593000000
END = START + 300_000
NS = 1_000_000


def market():
    return SimpleNamespace(
        window=market_for_sample_second(START), start_ms=START, end_ms=END,
        condition_id="condition", up_token_id="up", down_token_id="down",
        settlement_reference="chainlink_twap", settlement_window_s=60,
        settlement_source_url=TWAP_SOURCE, settlement_rule_version="btc-5m-twap-60",
        slug="test", raw_gamma={"market": {}},
    )


def context(value="100"):
    return make_context(market(), {
        "source_url": PRICE_ENDPOINT, "request_params": price_request_params(market()),
        "price_to_beat": value, "completed": False, "incomplete": True,
    }, status="ok", http_status=200, requested_wall_ns=(START + 9000) * NS,
        requested_monotonic_ns=9000 * NS, received_wall_ns=(START + 10000) * NS,
        received_monotonic_ns=10000 * NS, identity_validated=True)


def decision(*, horizon=32, remaining=30_000, omit=None, value="100.03", trending=False):
    engine = GhostTwapEngine("settlement-test", GhostPolicy(enabled=True))
    anchor = END - horizon * 1000
    now = END - remaining
    seq = 0
    for stamp in range(anchor - 61_000, anchor + 1, 1000):
        if stamp == omit:
            continue
        seq += 1
        receipt = max(stamp + 1000, now - 1000) if stamp == anchor else stamp + 1000
        price = Decimal(value) + (Decimal(stamp - anchor) / 1000 if trending else 0)
        engine.accept(PriceEvent("spot", price, stamp, receipt * NS,
            (receipt - START) * NS, seq, str(seq)))
    seq += 1
    engine.accept(PriceEvent("twap", Decimal("100.01"), anchor, (now - 500) * NS,
        (now - START - 500) * NS, seq, str(seq), 60))
    return engine.snapshot("1", now * NS, (now - START) * NS)


def project(snapshot=None, reference=None, **kwargs):
    return build_projection(snapshot or decision(), context() if reference is None else reference,
        kwargs.get("wall", (START + 11000) * NS), kwargs.get("mono", 11000 * NS))


def test_closing_projection_extends_future_tail_without_mutating_six_horizon_contract():
    snapshot = decision()
    original = snapshot.to_audit_json()
    result = project(snapshot)
    assert result["status"] == "available" and result["quality"] == "healthy"
    assert result["market_id"] == market().window.market_id
    assert result["target_source_timestamp_ms"] == END
    assert result["source_horizon_s"] == 32
    assert result["slots"][0]["slot_timestamp_ms"] == END - 62_000
    assert result["slots"][-1]["slot_timestamp_ms"] == END - 3000
    assert len(result["slots"]) == 60
    assert result["counts"] == dict(observed=31, carried=0, pending=2, future=27, missing=0)
    assert result["projected_price"] == "100.030000000000000000"
    assert result["schema_version"] == 3 and result["history_cohort"]
    assert result["observation_window_s"] == 60 and result["sampling_interval_ms"] == 2000
    assert all("qualifies" not in signal for signal in result["signals"].values())
    assert len(snapshot.slots) == 89 and len(snapshot.forecasts) == 6
    assert snapshot.to_audit_json() == original
    assert "slots" not in public_payload(result) and "slot_inputs" not in public_payload(result)
    assert isinstance(result["decision_wall_ns"], str)
    json.dumps(result, allow_nan=False)


def test_h35_is_bounded_but_source_expiry_prevents_publication_at_inclusive_age_limit():
    result = project(decision(horizon=35))
    assert result["source_horizon_s"] == 35 and len(result["slots"]) == 60
    assert result["counts"]["missing"] == 0
    assert result["status"] == "unavailable" and "inputs_expired" in result["reasons"]


def test_interior_carry_stays_available_and_degraded_with_frozen_constituents():
    result = project(decision(omit=END - 50_000))
    assert result["quality"] == "degraded" and result["status"] == "available"
    assert result["counts"]["carried"] == 1 and result["max_interior_carry_ms"] == 1000
    assert result["signals"]["ghost"]["side"] == "up"
    assert {s["input_sequence"] for s in result["slots"]} <= {e["sequence"] for e in result["slot_inputs"]}


@pytest.mark.parametrize("fault", ["different_market", "future_receipt", "late_availability", "mono_availability",
    "unvalidated_identity", "wrong_params", "completed", "conflicted", "unavailable", "after_end"])
def test_reference_and_availability_fail_closed(fault):
    snapshot, reference = decision(), context()
    kwargs = {}
    if fault == "different_market": reference["market_id"] += 1
    elif fault == "future_receipt": reference["received_wall_ns"] = str(snapshot.decision_wall_ns + 1)
    elif fault == "late_availability": kwargs["wall"] = snapshot.decision_wall_ns + 1
    elif fault == "mono_availability": kwargs["mono"] = snapshot.decision_monotonic_ns + 1
    elif fault == "unvalidated_identity": reference["identity_validated"] = False
    elif fault == "wrong_params": reference["request_params"]["twapLookbackSeconds"] = "30"
    elif fault == "completed": reference["completed"] = True
    elif fault == "conflicted": reference["conflicted"] = True
    elif fault == "unavailable": reference["status"] = "unavailable"
    else: snapshot = replace(snapshot, decision_wall_ns=END * NS, decision_monotonic_ns=300000 * NS)
    result = project(snapshot, reference, **kwargs)
    assert result["status"] == "unavailable" and result["projected_price"] is None
    assert all("qualifies" not in value for value in result["signals"].values())
    if fault == "after_end":
        assert result["market_id"] == market_for_sample_second(END).market_id
        assert result["market_end_ms"] == END + 300000


def test_only_final_60_seconds_and_exact_margins_have_no_candidate_threshold():
    assert "outside_final_60_seconds" in project(decision(horizon=62, remaining=60001))["reasons"]
    with localcontext() as ctx:
        ctx.prec = 80
        snapshot = decision(value="100.019999999999999999")
    result = project(snapshot)
    assert result["signals"]["ghost"]["lead_bps"] == "1.999999999999999900"
    exact = project(decision(value="100.02"))
    assert exact["signals"]["ghost"]["lead_bps"] == "2.000000000000000000"
    assert "threshold_bps" not in exact and "qualifies" not in exact["signals"]["ghost"]


@pytest.mark.parametrize("seconds", [30, 31, 45, 59, 60])
def test_earlier_projection_keeps_known_prices_and_fills_only_future_tail(seconds):
    snapshot = decision(horizon=seconds + 2, remaining=seconds * 1000, value="100", trending=True)
    before = snapshot.to_audit_json()
    result = project(snapshot)
    # Spot rises $1 each source second through the anchor, then is held at100.
    # There are (60-seconds) lower known prices: -1, -2, ... -(60-seconds).
    n = 60 - seconds
    with localcontext() as ctx:
        ctx.prec = 80
        expected = (Decimal(100) - Decimal(n * (n + 1)) / 120).quantize(Decimal('1e-18'))
    assert result['status'] == 'available'
    assert Decimal(result['projected_price']) == expected
    assert result['counts'] == dict(observed=n + 1, pending=2, future=seconds - 3, carried=0, missing=0)
    assert snapshot.to_audit_json() == before


def test_extended_projection_does_not_fill_a_missing_past_slot_with_current_spot():
    snapshot = decision(horizon=47, remaining=45_000)
    stamp = END - 55_000
    snapshot = replace(snapshot, slots=tuple(slot for slot in snapshot.slots if slot.slot_timestamp_ms != stamp))
    result = project(snapshot)
    assert result['status'] == 'unavailable' and result['counts']['missing'] == 1


def test_context_canonical_decimal_equality_and_conflict_latch():
    a = latch_context(None, context("100.00"))
    b = latch_context(a, context("100"))
    assert not b["conflicted"] and b["first_price_to_beat"] == "100"
    c = latch_context(b, context("100.000000000000000001"))
    assert c["conflicted"] and c["status"] == "unavailable"
    d = latch_context(c, context("100"))
    assert d["conflicted"] and d["price_to_beat"] is None
    assert decode_context(json.dumps(d))["conflicted"]
    with pytest.raises(ValueError): decode_context('{"schema_version":1,"price":1.0}')
    with pytest.raises(ValueError): decode_context(" " * 8193)


def test_settings_default_off_and_retired_campaign_environment_has_no_effect(monkeypatch):
    monkeypatch.delenv("SETTLEMENT_ENABLED", raising=False)
    monkeypatch.setenv("SETTLEMENT_EVALUATION_START_MS", "retired-setting")
    assert SettlementSettings().enabled is False
    assert SettlementSettings(enabled=True).model_dump() == {"enabled": True}


class Redis:
    def __init__(self): self.raw, self.calls = None, []
    async def get(self, key):
        assert key == CONTEXT_KEY
        return self.raw
    async def set(self, key, value, *, px):
        self.calls.append((key, px))
        self.raw = value


class Writer:
    def offer(self, record): return True


def runtime(redis, *, enabled=True, client=None):
    return CollectionEvidenceRuntime(pool=None, settings=EvidenceSettings(), writer=Writer(), client=client,
        collector_settings=SimpleNamespace(POLYMARKET_GAMMA_BASE_URL="https://gamma.example.test",
            POLYMARKET_CLOB_BASE_URL="https://clob.example.test"),
        parse_market=lambda *args, **kwargs: market(),
        wall_ns=lambda: (START + 200000) * NS, monotonic_ns=lambda: 200000 * NS,
        settlement_settings=SettlementSettings(enabled=enabled), settlement_redis=redis)


def test_bad_optional_history_setting_does_not_break_evidence_collector(monkeypatch):
    monkeypatch.setenv("SETTLEMENT_ENABLED", "invalid")
    instance = CollectionEvidenceRuntime(pool=None, settings=EvidenceSettings(), writer=Writer(),
        collector_settings=SimpleNamespace(POLYMARKET_GAMMA_BASE_URL="https://gamma.example.test",
            POLYMARKET_CLOB_BASE_URL="https://clob.example.test"),
        parse_market=lambda *args, **kwargs: market(), settlement_redis=Redis())
    assert instance.settlement_settings.enabled is False


def test_metadata_context_latch_survives_worker_recreation_and_default_off_does_nothing():
    async def run():
        redis = Redis()
        await runtime(redis, enabled=False)._publish_settlement_context(context())
        assert redis.raw is None
        await runtime(redis)._publish_settlement_context(context())
        await runtime(redis)._publish_settlement_context(context("101"))
        await runtime(redis)._publish_settlement_context(context())
        assert decode_context(redis.raw)["conflicted"] is True
        kept = runtime(redis)
        await kept._publish_settlement_context(context())
        redis.raw = None
        await kept._publish_settlement_context(context())
        assert decode_context(redis.raw)["conflicted"] is True
        assert all(px == 100000 for _, px in redis.calls)
    asyncio.run(run())


def test_existing_http_poll_supplies_context_without_extra_requests_and_requires_identity():
    async def run():
        urls = []
        def handle(request):
            urls.append(str(request.url))
            return httpx.Response(200, text='{"openPrice":100,"completed":false}')
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            redis = Redis()
            rt = runtime(redis, client=client)
            await rt.observe_http(market(), "price_to_beat", PRICE_ENDPOINT, parse_price_observation,
                params=price_request_params(market()), identity_validated=True)
            assert len(urls) == 1 and decode_context(redis.raw)["status"] == "available"
            await rt.observe_http(market(), "price_to_beat", PRICE_ENDPOINT, parse_price_observation,
                params=price_request_params(market()))
            assert len(urls) == 2 and decode_context(redis.raw)["status"] == "unavailable"
    asyncio.run(run())
