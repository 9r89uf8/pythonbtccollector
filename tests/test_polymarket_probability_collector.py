import asyncio
from decimal import Decimal

import pytest

import price_collector.polymarket_probability_collector as collector
from price_collector.market import MarketWindow


def market_window():
    return MarketWindow(
        market_id=5_944_864,
        market_start_ms=1_783_459_200_000,
        market_end_ms=1_783_459_500_000,
    )


def current_market():
    return collector.CurrentPolymarketMarket(
        window=market_window(),
        slug="btc-updown-5m-1783459200",
        gamma_event_id="event-1",
        gamma_market_id="market-1",
        condition_id="condition-1",
        question="BTC Up or Down",
        start_ms=1_783_459_200_000,
        end_ms=1_783_459_500_000,
        up_token_id="up-token",
        down_token_id="down-token",
        up_outcome="Up",
        down_outcome="Down",
        active=True,
        closed=False,
        archived=False,
        raw_gamma={"market": {"id": "market-1"}},
    )


def test_slug_for_window_uses_market_start_unix_seconds():
    assert collector.slug_for_window(market_window(), "btc-updown-5m") == (
        "btc-updown-5m-1783459200"
    )


def test_parse_current_market_from_gamma_maps_up_down_tokens_from_json_strings():
    slug = "btc-updown-5m-1783459200"
    event = {
        "id": "event-1",
        "slug": slug,
        "title": "BTC Up or Down",
        "markets": [
            {
                "id": "market-1",
                "slug": slug,
                "conditionId": "condition-1",
                "question": "BTC Up or Down",
                "outcomes": '["Down","Up"]',
                "clobTokenIds": '["down-token","up-token"]',
                "active": False,
                "closed": False,
                "archived": False,
                "startDate": "2026-07-07T21:20:00Z",
                "endDate": "2026-07-07T21:25:00Z",
            }
        ],
    }

    market = collector.parse_current_market_from_gamma(
        event,
        window=market_window(),
        slug=slug,
    )

    assert market.gamma_event_id == "event-1"
    assert market.gamma_market_id == "market-1"
    assert market.condition_id == "condition-1"
    assert market.up_token_id == "up-token"
    assert market.down_token_id == "down-token"
    assert market.active is False
    assert market.closed is False
    assert market.start_ms == 1_783_459_200_000
    assert market.end_ms == 1_783_459_500_000


def test_build_clob_subscription_uses_only_up_and_down_token_ids():
    subscription = collector.build_clob_subscription(current_market())

    assert subscription == {
        "type": "market",
        "assets_ids": ["up-token", "down-token"],
        "custom_feature_enabled": True,
    }


def test_apply_clob_book_and_price_change_updates_best_bid_ask():
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
    )

    assert collector.apply_clob_message(
        state,
        {
            "event_type": "book",
            "asset_id": "up-token",
            "timestamp": "1783459200123",
            "bids": [{"price": "0.45"}, {"price": "0.47"}],
            "asks": [{"price": "0.50"}, {"price": "0.49"}],
        },
        received_ms=1_783_459_200_200,
    )
    assert collector.apply_clob_message(
        state,
        {
            "event_type": "price_change",
            "timestamp": "1783459200456",
            "price_changes": [
                {
                    "asset_id": "down-token",
                    "best_bid": "0.50",
                    "best_ask": "0.53",
                }
            ],
        },
        received_ms=1_783_459_200_500,
    )

    assert state.up_bid == Decimal("0.47")
    assert state.up_ask == Decimal("0.49")
    assert state.down_bid == Decimal("0.50")
    assert state.down_ask == Decimal("0.53")
    assert state.latest_provider_event_ms == 1_783_459_200_456
    assert state.latest_received_ms == 1_783_459_200_500


def test_midpoint_and_normalized_probabilities_use_decimal_math():
    up_mid = collector.midpoint(Decimal("0.39"), Decimal("0.41"))
    down_mid = collector.midpoint(Decimal("0.59"), Decimal("0.61"))

    up_norm, down_norm = collector.normalized_probs(up_mid, down_mid)

    assert up_mid == Decimal("0.40")
    assert down_mid == Decimal("0.60")
    assert up_norm == Decimal("0.4")
    assert down_norm == Decimal("0.6")


def test_build_probability_snapshot_skips_stale_and_boundary_samples():
    market = current_market()
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
        up_bid=Decimal("0.47"),
        up_ask=Decimal("0.49"),
        down_bid=Decimal("0.50"),
        down_ask=Decimal("0.53"),
        latest_provider_event_ms=1_783_459_200_500,
        latest_received_ms=1_783_459_200_500,
    )

    snapshot = collector.build_probability_snapshot(
        current_market=market,
        state=state,
        now_ms=1_783_459_201_123,
        stale_ms=15_000,
    )

    assert snapshot is not None
    assert snapshot.sample_second_ms == 1_783_459_201_000
    assert snapshot.up_mid == Decimal("0.48")
    assert snapshot.down_mid == Decimal("0.515")

    assert (
        collector.build_probability_snapshot(
            current_market=market,
            state=state,
            now_ms=1_783_459_216_000,
            stale_ms=15_000,
        )
        is None
    )
    assert (
        collector.build_probability_snapshot(
            current_market=market,
            state=state,
            now_ms=1_783_459_500_000,
            stale_ms=15_000,
        )
        is None
    )

    collector.apply_clob_message(
        state,
        {"event_type": "market_resolved", "timestamp": "1783459217000"},
        received_ms=1_783_459_217_000,
    )
    assert state.resolved is True
    assert (
        collector.build_probability_snapshot(
            current_market=market,
            state=state,
            now_ms=1_783_459_217_000,
            stale_ms=15_000,
        )
        is None
    )


def test_sample_probability_once_writes_one_snapshot_with_market_source_key(monkeypatch):
    calls = []

    async def fake_upsert_polymarket_probability_sample(pool, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        collector,
        "upsert_polymarket_probability_sample",
        fake_upsert_polymarket_probability_sample,
    )
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
        up_bid=Decimal("0.47"),
        up_ask=Decimal("0.49"),
        down_bid=Decimal("0.50"),
        down_ask=Decimal("0.53"),
        latest_provider_event_ms=1_783_459_200_500,
        latest_received_ms=1_783_459_200_500,
    )

    written = asyncio.run(
        collector.sample_probability_once(
            pool="pool",
            current_market=current_market(),
            state=state,
            source="polymarket_clob",
            stale_ms=15_000,
            now_ms=1_783_459_201_123,
        )
    )

    assert written is True
    assert calls[0]["source"] == "polymarket_clob"
    assert calls[0]["sample_second_ms"] == 1_783_459_201_000
    assert calls[0]["window"].market_id == 5_944_864
    assert calls[0]["up_mid"] == Decimal("0.48")
    assert calls[0]["down_mid"] == Decimal("0.515")


def test_clob_ping_loop_sends_text_ping_every_configured_interval(monkeypatch):
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 1:
            raise asyncio.CancelledError

    class FakeWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, message):
            self.sent.append(message)

    fake_websocket = FakeWebSocket()
    monkeypatch.setattr(collector.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(collector.clob_ping_loop(fake_websocket, ping_seconds=10))

    assert sleeps == [10, 10]
    assert fake_websocket.sent == ["PING"]
