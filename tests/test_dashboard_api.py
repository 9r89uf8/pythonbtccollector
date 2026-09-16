import asyncio
from decimal import Decimal
import json

import pytest
from fastapi.testclient import TestClient

import price_collector.api as api
from price_collector.dashboard_api import (
    HISTORY_SQL, PRICE_ENDPOINT, REFERENCE_SQL, TWAP_SOURCE,
    fetch_dashboard_payload, opening_reference,
)
from price_collector.market import market_for_sample_second


START = 1789593000000
NOW = START + 90_000
WINDOW = market_for_sample_second(START)


def observation(price="76075.764015588400000001"):
    return {
        "settlement_reference": "chainlink_twap", "settlement_window_s": 60,
        "settlement_source_url": TWAP_SOURCE, "settlement_rule_version": "btc-5m-twap-60",
        "condition_id": "condition", "up_token_id": "up", "down_token_id": "down",
        "status": "ok", "http_status": 200, "received_wall_ns": (START + 5_000) * 1_000_000,
        "requested_wall_ns": (START + 4_000) * 1_000_000,
        "payload": {
            "source_url": PRICE_ENDPOINT, "price_to_beat": price,
            "completed": False, "incomplete": True, "close_price": None,
            "request_params": {
                "symbol": "BTC", "variant": "fiveminute", "twapEnabled": "true",
                "twapLookbackSeconds": "60", "eventStartTime": "2026-09-16T21:10:00Z",
                "endDate": "2026-09-16T21:15:00Z",
            },
        },
    }


def test_opening_uses_exact_observed_reference_even_when_close_is_incomplete():
    row = observation()
    row["payload"] = json.dumps(row["payload"])
    result = opening_reference([row], WINDOW, NOW)
    assert result["reference_status"] == "official"
    assert result["price_to_beat"] == "76075.764015588400000001"
    assert result["reference_received_ms"] == START + 5_000


@pytest.mark.parametrize("change", [
    "wrong_market", "wrong_identity", "future_receipt", "preopen_request",
    "completed", "invalid", "float_price", "missing_price",
])
def test_unusable_opening_never_gets_a_price_fallback(change):
    row = observation()
    if change == "wrong_market":
        row["payload"]["request_params"]["eventStartTime"] = "2026-09-16T21:05:00Z"
    elif change == "wrong_identity":
        row["settlement_window_s"] = 30
    elif change == "future_receipt":
        row["received_wall_ns"] = (NOW + 1) * 1_000_000
    elif change == "preopen_request":
        row["requested_wall_ns"] = (START - 1) * 1_000_000
    elif change == "completed":
        row["payload"]["completed"] = True
    elif change == "invalid":
        row["status"] = "invalid"
    elif change == "float_price":
        row["payload"]["price_to_beat"] = 76075.5
    else:
        row["payload"]["price_to_beat"] = None
    result = opening_reference([row], WINDOW, NOW)
    assert result["price_to_beat"] is None
    assert result["reference_status"] == "unavailable"


def test_conflicting_openings_and_boundary_do_not_reuse_old_market_reference():
    result = opening_reference([observation("10"), observation("11")], WINDOW, NOW)
    assert result["reference_reason"] == "conflicting_opening_references"
    next_window = market_for_sample_second(START + 300_000)
    result = opening_reference([observation()], next_window, START + 300_000)
    assert result["price_to_beat"] is None


class Connection:
    def __init__(self):
        self.calls = []

    async def fetch(self, sql, *args, timeout):
        self.calls.append((sql, args, timeout))
        if sql == HISTORY_SQL:
            return [
                {"symbol": "BTCUSD", "price": Decimal("76085.123456789012345678"),
                 "provider_event_ms": NOW - 1000, "received_ms": NOW - 50},
                {"symbol": "BTCUSD_TWAP_60S", "price": Decimal("76075.987654321098765432"),
                 "provider_event_ms": NOW - 2000, "received_ms": NOW - 100},
            ]
        assert sql == REFERENCE_SQL
        return [observation()]


class Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args):
        return False


class Pool:
    def __init__(self):
        self.connection = Connection()

    def acquire(self, *, timeout):
        assert timeout == 3.0
        return Acquire(self.connection)


def test_history_preserves_precision_missing_seconds_and_bounded_query_inputs():
    pool = Pool()
    payload = asyncio.run(fetch_dashboard_payload(pool, NOW))
    assert payload["history"]["chainlink"] == [{
        "source_timestamp_ms": NOW - 1000, "received_ms": NOW - 50,
        "value": "76085.123456789012345678",
    }]
    assert payload["history"]["twap"][0]["value"] == "76075.987654321098765432"
    assert len(payload["history"]["twap"]) == 1  # No filled seconds.
    assert payload["market"]["market_start_ms"] == START
    assert payload["market"]["market_end_ms"] == START + 300_000
    assert pool.connection.calls[0][1] == (NOW - 900_000, NOW, NOW)
    assert pool.connection.calls[1][1] == (WINDOW.market_id, START * 1_000_000, NOW * 1_000_000)


def test_route_is_separate_bounded_read_with_no_store_and_typed_failure(monkeypatch):
    captured = []

    async def fake_fetch(pool, now_ms):
        captured.append((pool, now_ms))
        return {"server_time_ms": now_ms, "history": {}, "market": {}}

    reader = object()
    monkeypatch.setattr(api, "get_pool", lambda request: reader)
    monkeypatch.setattr(api, "current_utc_epoch_ms", lambda: NOW)
    monkeypatch.setattr(api, "fetch_dashboard_payload", fake_fetch)
    client = TestClient(api.app)
    response = client.get("/markets/current/dashboard")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert captured == [(reader, NOW)]

    async def unavailable(*args):
        raise TimeoutError("private connection information")

    monkeypatch.setattr(api, "fetch_dashboard_payload", unavailable)
    response = client.get("/markets/current/dashboard")
    assert response.status_code == 503
    assert response.json() == {"state": "unavailable", "reason": "dashboard_history_unavailable"}
