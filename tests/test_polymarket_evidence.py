import asyncio
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from price_collector import polymarket_evidence as evidence
from price_collector import polymarket_probability_collector as collector
from price_collector.market import market_for_sample_second


START = collector.TWAP_60S_CUTOVER_MS + 300_000


def market():
    return collector.CurrentPolymarketMarket(
        window=market_for_sample_second(START), slug=f"btc-updown-5m-{START // 1000}",
        gamma_event_id="event", gamma_market_id="market", condition_id="condition",
        question="BTC Up or Down", start_ms=START, end_ms=START + 300_000,
        up_token_id="up-token", down_token_id="down-token", up_outcome="Up", down_outcome="Down",
        active=True, closed=False, archived=False, settlement_reference="chainlink_twap",
        settlement_window_s=60, settlement_source_url=collector.CURRENT_TWAP_SOURCE_URL,
        settlement_rule_version="btc-5m-twap-60", raw_gamma={"market": {}},
    )


class Writer:
    def __init__(self):
        self.records = []
        self.quotes = []

    async def start(self):
        pass

    def offer(self, record):
        self.records.append(record)
        return True

    def offer_quote(self, quote):
        self.quotes.append(quote)
        return True

    async def close(self, **kwargs):
        pass


def runtime(*, writer=None, client=None, wall_ns=None, monotonic_ns=None, parse_market=None):
    return evidence.CollectionEvidenceRuntime(
        pool=None, settings=evidence.EvidenceSettings(),
        collector_settings=SimpleNamespace(
            POLYMARKET_GAMMA_BASE_URL="https://gamma.example.test",
            POLYMARKET_CLOB_BASE_URL="https://clob.example.test",
        ),
        parse_market=parse_market or collector.parse_current_market_from_gamma,
        writer=writer or Writer(), client=client,
        wall_ns=wall_ns or (lambda: (START + 190_000) * 1_000_000),
        monotonic_ns=monotonic_ns or (lambda: 1_000_000),
    )


def capture_state(rt, state=None):
    return evidence.QuoteCapture(
        runtime=rt, market=market(), state=state or collector.ProbabilityState("up-token", "down-token"),
        connected_wall_ns=START * 1_000_000, connected_monotonic_ns=10,
        subscribed_wall_ns=START * 1_000_000 + 1, subscribed_monotonic_ns=11,
    )


def test_exact_preclose_price_retains_long_decimal_and_incomplete_flag():
    response = httpx.Response(200, text=(
        '{"openPrice":78031.190816420330123456,"closePrice":null,'
        '"timestamp":1789004217930,"completed":false,"incomplete":true,"cached":true}'
    ))
    status, parsed = evidence.parse_price_observation(evidence.exact_response_json(response))
    assert status == "ok"
    assert parsed["price_to_beat"] == Decimal("78031.190816420330123456")
    assert parsed["incomplete"] is True
    assert parsed["close_price"] is None
    assert parsed["api_timestamp_ms"] == 1789004217930
    assert "provider_event_ms" not in parsed


@pytest.mark.parametrize("value", [True, 78.01, "NaN", "-1", "0", "Infinity"])
def test_invalid_or_float_strike_rejected(value):
    with pytest.raises(ValueError):
        evidence.parse_price_observation({"openPrice": value})


def test_missing_strike_is_missing_and_never_zero():
    status, parsed = evidence.parse_price_observation({"openPrice": None, "closePrice": None})
    assert status == "missing" and parsed["price_to_beat"] is None
    with pytest.raises(ValueError):
        evidence.exact_response_json(httpx.Response(200, text='{"openPrice":NaN}'))


def test_price_request_is_bound_to_exact_rule_and_window():
    params = evidence.price_request_params(market())
    assert params == {
        "symbol": "BTC", "eventStartTime": "2026-08-14T00:05:00Z", "variant": "fiveminute",
        "endDate": "2026-08-14T00:10:00Z", "twapEnabled": "true", "twapLookbackSeconds": "60",
    }
    with pytest.raises(ValueError, match="settlement"):
        evidence.price_request_params(replace(market(), settlement_window_s=30))
    with pytest.raises(ValueError, match="identity"):
        evidence.price_request_params(replace(market(), down_token_id="up-token"))


def clob_payload():
    return {
        "c": "condition", "t": [{"t": "up-token", "o": "Up"}, {"t": "down-token", "o": "Down"}],
        "mos": 5, "mts": Decimal("0.01"), "mbf": 1000, "tbf": 1000,
        "ao": True, "itode": True, "fd": {"r": Decimal("0.070000000000000001"), "e": 1, "to": True}, "v": "v1",
    }


def test_clob_fee_curve_and_delay_are_preserved_separately():
    status, parsed = evidence.parse_clob_observation(clob_payload(), market())
    assert status == "ok"
    assert parsed["data"]["fd"]["r"] == Decimal("0.070000000000000001")
    assert parsed["data"]["tbf"] == 1000
    assert parsed["data"]["itode"] is True
    assert "delay_ms" not in parsed["data"]


@pytest.mark.parametrize("field,value", [
    ("c", "different-condition"), ("t", [{"t": "up-token", "o": "Down"}]),
    ("ao", "true"), ("mts", 0.01), ("mos", False),
])
def test_wrong_clob_identity_or_invalid_fields_fail_closed(field, value):
    payload = clob_payload()
    payload[field] = value
    with pytest.raises(ValueError):
        evidence.parse_clob_observation(payload, market())


def test_absent_fee_or_delay_not_filled_from_defaults():
    payload = clob_payload()
    del payload["itode"]
    del payload["fd"]
    status, parsed = evidence.parse_clob_observation(payload, market())
    assert status == "missing"
    assert parsed["data"]["itode"] is None
    assert parsed["data"]["itode_present"] is False
    assert parsed["data"]["fd"] is None


def test_gamma_exact_selected_fields_and_identity_validation():
    source = {
        "feesEnabled": True,
        "feeSchedule": {"rate": Decimal("0.070000000000000001"), "exponent": 1, "takerOnly": True},
        "orderMinSize": Decimal("5"), "orderPriceMinTickSize": Decimal("0.01"),
        "liquidity": Decimal("999999"), "acceptingOrders": True,
    }
    observed = replace(market(), raw_gamma={"market": source})
    status, parsed = evidence.parse_gamma_observation({}, market(), lambda *args, **kwargs: observed)
    assert status == "ok"
    assert parsed["data"]["feeSchedule"]["rate"] == Decimal("0.070000000000000001")
    assert "liquidity" not in parsed["data"]
    with pytest.raises(ValueError, match="identity"):
        evidence.parse_gamma_observation({}, market(), lambda *args, **kwargs: replace(observed, up_token_id="other"))


def test_incomplete_fee_curves_are_not_reported_complete():
    observed = replace(market(), raw_gamma={"market": {
        "feesEnabled": True, "feeSchedule": {}, "orderMinSize": 5, "orderPriceMinTickSize": "0.01",
    }})
    status, data = evidence.parse_gamma_observation({}, market(), lambda *args, **kwargs: observed)
    assert status == "missing"
    assert data["data"]["feeSchedule"]["takerOnly"] is None
    payload = clob_payload()
    del payload["fd"]["to"]
    assert evidence.parse_clob_observation(payload, market())[0] == "missing"
    payload["fd"]["to"] = "true"
    with pytest.raises(ValueError, match="boolean"):
        evidence.parse_clob_observation(payload, market())
    observed.raw_gamma["market"]["feesEnabled"] = "false"
    with pytest.raises(ValueError, match="boolean"):
        evidence.parse_gamma_observation({}, market(), lambda *args, **kwargs: observed)


def test_order_rules_keep_separate_seconds_delay_and_validate_identity():
    payload = {
        "condition_id": "condition", "tokens": [{"token_id": "up-token", "outcome": "Up"}, {"token_id": "down-token", "outcome": "Down"}],
        "seconds_delay": 0, "minimum_order_size": "5", "minimum_tick_size": "0.001", "accepting_orders": True,
    }
    status, parsed = evidence.parse_clob_order_rules(payload, market())
    assert status == "ok"
    assert parsed["data"]["seconds_delay"] == 0
    assert "itode" not in parsed["data"]
    del payload["seconds_delay"]
    assert evidence.parse_clob_order_rules(payload, market())[0] == "missing"
    payload["condition_id"] = "other"
    with pytest.raises(ValueError, match="condition"):
        evidence.parse_clob_order_rules(payload, market())


def test_response_availability_uses_receipt_even_when_request_crosses_close():
    async def scenario():
        now = {"wall": (START + 299_990) * 1_000_000, "mono": 100}
        async def handler(request):
            now.update(wall=(START + 300_020) * 1_000_000, mono=30_000_100)
            return httpx.Response(200, text='{"openPrice":78000.123456789012345678,"closePrice":78001,"completed":true}', headers={"age": "10", "date": "Fri, 14 Aug 2026 00:10:00 GMT"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            rt = runtime(client=client, wall_ns=lambda: now["wall"], monotonic_ns=lambda: now["mono"])
            result = await rt.observe_http(market(), "price_to_beat", evidence.PRICE_ENDPOINT, evidence.parse_price_observation, params=evidence.price_request_params(market()))
        row = rt.writer.records[0]
        assert result == "ok"
        assert row.requested_wall_ns < market().window.market_end_ms * 1_000_000 < row.received_wall_ns
        assert row.received_monotonic_ns - row.requested_monotonic_ns == 30_000_000
        assert row.payload["price_to_beat"] == "78000.123456789012345678"
        assert row.response_age_seconds == 10
        assert row.response_date == "Fri, 14 Aug 2026 00:10:00 GMT"
        assert row.provider_event_ms is None
    asyncio.run(scenario())


@pytest.mark.parametrize("response_status,body,expected", [
    (404, "missing", "http_error"), (200, "not-json", "invalid"),
    (200, '{"openPrice":null}', "missing"),
    (200, '{"openPrice":78000,"completed":true}', "invalid"),
])
def test_http_observations_preserve_missing_invalid_and_errors(response_status, body, expected):
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(response_status, text=body))) as client:
            rt = runtime(client=client)
            status = await rt.observe_http(market(), "price_to_beat", evidence.PRICE_ENDPOINT, evidence.parse_price_observation)
        row = rt.writer.records[0]
        assert status == expected and row.status == expected
        assert row.http_status == response_status
        assert row.response_sha256
    asyncio.run(scenario())


def test_transport_failure_is_recorded_without_an_invented_http_status():
    async def scenario():
        def fail(request):
            raise httpx.ReadTimeout("timeout", request=request)
        async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
            rt = runtime(client=client)
            await rt.observe_http(market(), "price_to_beat", evidence.PRICE_ENDPOINT, evidence.parse_price_observation)
        row = rt.writer.records[0]
        assert row.status == "transport_error" and row.http_status is None
        assert row.payload["error_type"] == "ReadTimeout"
    asyncio.run(scenario())


def test_strike_requests_are_not_made_after_contradictory_gamma_identity():
    async def scenario():
        urls = []
        def handler(request):
            urls.append(str(request.url))
            return httpx.Response(200, json={})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            rt = runtime(client=client)
            await rt.poll_market_once(market())
        assert all("crypto-price" not in url for url in urls)
        row = next(row for row in rt.writer.records if row.kind == "price_to_beat")
        assert row.status == "invalid" and row.requested_wall_ns is None
    asyncio.run(scenario())


def test_repeated_prices_and_revisions_each_keep_their_receipt_record():
    async def scenario():
        values = iter(["78000.1", "78000.1", "78000.2"])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, text='{"openPrice":' + next(values) + '}'))) as client:
            rt = runtime(client=client)
            for _ in range(3):
                await rt.observe_http(market(), "price_to_beat", evidence.PRICE_ENDPOINT, evidence.parse_price_observation)
        rows = rt.writer.records
        assert [row.payload["price_to_beat"] for row in rows] == ["78000.1", "78000.1", "78000.2"]
        assert len({row.record_id for row in rows}) == 3
        assert rows[0].payload_hash == rows[1].payload_hash != rows[2].payload_hash
    asyncio.run(scenario())


def test_quote_half_open_window_and_actual_observation_time():
    rt = runtime()
    capture = capture_state(rt)
    capture_start = (START + 180_000) * 1_000_000
    assert not capture.sample_once(capture_start - 1, 20)
    assert capture.sample_once(capture_start + 7_123_456, 30)
    assert rt.writer.quotes[0].observed_wall_ns == capture_start + 7_123_456
    assert rt.writer.quotes[0].received_wall_ns is None
    assert rt.writer.quotes[0].up_ask is None
    assert not capture.sample_once((START + 300_000) * 1_000_000, 40)


def test_same_price_refresh_and_independent_components_survive_sampling():
    rt = runtime()
    state = collector.ProbabilityState("up-token", "down-token")
    capture = capture_state(rt, state)
    state.update_token("up-token", bid=Decimal("0.5"), ask=Decimal("0.51"), replace=True,
                       provider_event_ms=START + 179_980, received_ms=START + 179_990, event_type="book")
    state.update_token("down-token", bid=Decimal("0.48"), ask=Decimal("0.49"), replace=True,
                       provider_event_ms=START + 179_900, received_ms=START + 179_910, event_type="book")
    capture.frame_received()
    capture.accepted((START + 179_990) * 1_000_000, 20)
    capture.sample_once((START + 180_000) * 1_000_000, 30)
    # Refresh only Up ask at the same price; Down and Up bid must stay old.
    state.update_token("up-token", bid=None, ask=Decimal("0.51"), replace=False,
                       provider_event_ms=START + 180_050, received_ms=START + 180_060, event_type="price_change")
    capture.frame_received()
    capture.accepted((START + 180_060) * 1_000_000, 40)
    capture.sample_once((START + 180_100) * 1_000_000, 50)
    before, after = rt.writer.quotes
    assert before.up_ask == after.up_ask
    assert after.up_ask_received_ms == START + 180_060
    assert before.up_bid_received_ms == after.up_bid_received_ms == START + 179_990
    assert before.down_ask_received_ms == after.down_ask_received_ms == START + 179_910
    assert after.receive_sequence == 2
    assert before.up_ask_received_ms == START + 179_990


def test_tick_updates_are_independent_of_probability_updates():
    rt = runtime()
    capture = capture_state(rt)
    capture.tick_size_change({
        "event_type": "tick_size_change", "asset_id": "down-token", "market": "condition",
        "old_tick_size": "0.01", "new_tick_size": "0.001",
    }, (START + 200_000) * 1_000_000, 100, START + 199_999)
    row = rt.writer.records[-1]
    assert row.kind == "tick_size" and row.status == "ok"
    assert row.payload["outcome"] == "Down"
    assert row.payload["new_tick_size"] == "0.001"
    assert row.provider_event_ms == START + 199_999
    assert capture.state.down_ask is None


def test_invalid_tick_provider_clock_is_recorded_without_disrupting_reader():
    rt = runtime()
    capture = capture_state(rt)
    capture.tick_size_change({"event_type": "tick_size_change", "asset_id": "up-token", "new_tick_size": "0.001"},
                            (START + 200_000) * 1_000_000, 100, -1)
    row = rt.writer.records[-1]
    assert row.status == "invalid"
    assert row.provider_event_ms is None
    assert row.payload["reported_provider_event_ms"] == -1


def test_quote_sampler_recovers_after_invalid_state(monkeypatch):
    async def scenario():
        clock = {"wall": (START + 190_000) * 1_000_000, "samples": 0, "sleeps": 0}
        rt = runtime(wall_ns=lambda: clock["wall"])
        capture = capture_state(rt)
        def sample(wall, mono):
            clock["samples"] += 1
            if clock["samples"] == 1:
                raise ValueError("invalid quote component")
            return True
        async def sleep(delay):
            clock["wall"] += 100_000_000
            clock["sleeps"] += 1
            if clock["sleeps"] == 2:
                capture.closed = True
        capture.sample_once = sample
        monkeypatch.setattr(evidence.asyncio, "sleep", sleep)
        await capture.run()
        assert clock["samples"] == 2
        assert [row.payload["reason"] for row in rt.writer.records if row.kind == "gap"] == [
            "invalid_quote_state", "invalid_quote_state_recovered",
        ]
    asyncio.run(scenario())


def test_new_probability_raw_keeps_clocks_without_repeated_prices_or_tokens():
    state = collector.ProbabilityState("up-token", "down-token")
    state.up_ask_received_ms = START + 1
    raw = state.raw_snapshot()
    assert raw["up_ask_received_ms"] == START + 1
    assert set(raw) == {"event_type"} | {f"{component}_{clock}" for component in evidence.COMPONENTS for clock in ("provider_event_ms", "received_ms")}


def test_market_metadata_task_survives_a_quote_session_close():
    async def scenario():
        rt = runtime()
        started = asyncio.Event()
        async def metadata_loop(selected):
            started.set()
            await asyncio.Event().wait()
        rt._metadata_loop = metadata_loop
        rt.register_market(market())
        rt.register_market(market())
        await started.wait()
        assert len(rt._market_tasks) == 1
        capture = capture_state(rt)
        await capture.close("connection_error")
        assert not rt._market_tasks[market().window.market_id].done()
        await rt.close()
    asyncio.run(scenario())


def test_signal_stop_cancels_and_drains_collector(monkeypatch):
    async def scenario():
        loop = asyncio.get_running_loop()
        callbacks = {}
        drained = []
        monkeypatch.setattr(loop, "add_signal_handler", lambda signum, callback: callbacks.setdefault(signum, callback))
        monkeypatch.setattr(loop, "remove_signal_handler", lambda signum: True)
        async def collecting(settings):
            try:
                callbacks[collector.signal.SIGTERM]()
                await asyncio.Event().wait()
            finally:
                drained.append(True)
        monkeypatch.setattr(collector, "run_collector", collecting)
        await collector.run_with_signals(SimpleNamespace())
        assert drained == [True]
    asyncio.run(scenario())


def test_failed_probability_sampler_still_closes_evidence_session(monkeypatch):
    async def scenario():
        class Socket:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            async def send(self, message):
                pass
            async def recv(self):
                await asyncio.sleep(0)
                return "PONG"
        async def sampler(**kwargs):
            raise RuntimeError("historical sample write failed")
        async def idle(**kwargs):
            await asyncio.Event().wait()
        async def ping(*args, **kwargs):
            await asyncio.Event().wait()
        monkeypatch.setattr(collector.websockets, "connect", lambda *args, **kwargs: Socket())
        monkeypatch.setattr(collector, "probability_sampler_loop", sampler)
        monkeypatch.setattr(collector, "probability_rest_prime_loop", idle)
        monkeypatch.setattr(collector, "clob_ping_loop", ping)
        monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: START + 190_000)
        rt = runtime(monotonic_ns=lambda: 10**18)
        # Keep metadata networking out of this lifecycle regression.
        rt.register_market = lambda market: None
        with pytest.raises(RuntimeError, match="historical sample"):
            await collector.collect_current_market(
                settings=SimpleNamespace(POLYMARKET_CLOB_WS_URL="wss://example.test", POLYMARKET_CLOB_PING_SECONDS=10,
                    POLYMARKET_PROBABILITY_SOURCE="polymarket_clob", POLYMARKET_PROBABILITY_STALE_MS=15000,
                    POLYMARKET_RESOLUTION_WS_GRACE_SECONDS=30),
                pool=None, client=None, current_market=market(), evidence=rt,
            )
        assert not rt._sessions
        assert any(row.kind == "session_end" for row in rt.writer.records)
        assert any(row.kind == "gap" and row.payload["reason"] == "connection_ended_before_market_close" for row in rt.writer.records)
    asyncio.run(scenario())
