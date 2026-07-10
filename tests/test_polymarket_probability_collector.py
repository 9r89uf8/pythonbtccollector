import asyncio
from decimal import Decimal
from types import SimpleNamespace

import httpx
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


def resolved_gamma_event(*, outcome_prices='["0","1"]'):
    slug = "btc-updown-5m-1783459200"
    return {
        "id": "event-1",
        "slug": slug,
        "eventMetadata": {
            "priceToBeat": Decimal("63337.115841440165"),
            "finalPrice": Decimal("63336.71900847139"),
        },
        "markets": [
            {
                "id": "market-1",
                "slug": slug,
                "conditionId": "condition-1",
                "outcomes": '["Up","Down"]',
                "outcomePrices": outcome_prices,
                "clobTokenIds": '["up-token","down-token"]',
                "closed": True,
                "closedTime": "2026-07-07 21:25:17+00",
                "umaResolutionStatus": "resolved",
            }
        ],
    }


def test_parse_polymarket_resolution_uses_official_prices_and_clob_winner():
    resolution = collector.parse_polymarket_resolution(
        resolved_gamma_event(),
        slug=current_market().slug,
        gamma_market_id="market-1",
        condition_id="condition-1",
        up_token_id="up-token",
        down_token_id="down-token",
        clob_data={
            "closed": True,
            "is_50_50_outcome": False,
            "tokens": [
                {
                    "token_id": "up-token",
                    "outcome": "Up",
                    "price": Decimal("0"),
                    "winner": False,
                },
                {
                    "token_id": "down-token",
                    "outcome": "Down",
                    "price": Decimal("1"),
                    "winner": True,
                },
            ],
        },
    )

    assert resolution.status == "resolved"
    assert resolution.resolution_type == "winner"
    assert resolution.winner == "Down"
    assert resolution.winning_token_id == "down-token"
    assert resolution.up_payout == Decimal("0")
    assert resolution.down_payout == Decimal("1")
    assert resolution.chainlink_open_price == Decimal("63337.115841440165")
    assert resolution.chainlink_close_price == Decimal("63336.71900847139")
    assert resolution.chainlink_source == "polymarket_gamma_event_metadata"
    assert resolution.resolution_source == "polymarket_clob_rest"
    assert resolution.resolved_at_ms == 1_783_459_517_000
    assert resolution.is_complete is True


def test_parse_polymarket_resolution_never_uses_nonterminal_probabilities_as_winner():
    resolution = collector.parse_polymarket_resolution(
        resolved_gamma_event(outcome_prices='["0.03","0.97"]'),
        slug=current_market().slug,
        gamma_market_id="market-1",
        condition_id="condition-1",
        up_token_id="up-token",
        down_token_id="down-token",
    )

    assert resolution.status == "pending"
    assert resolution.resolution_type is None
    assert resolution.winner is None
    assert resolution.winning_token_id is None
    assert resolution.up_payout is None
    assert resolution.down_payout is None
    assert resolution.resolution_source is None
    assert resolution.resolved_at_ms is None
    assert resolution.is_complete is False


def test_parse_polymarket_resolution_preserves_official_split_without_winner():
    resolution = collector.parse_polymarket_resolution(
        resolved_gamma_event(outcome_prices='["0.5","0.5"]'),
        slug=current_market().slug,
        gamma_market_id="market-1",
        condition_id="condition-1",
        up_token_id="up-token",
        down_token_id="down-token",
        clob_data={
            "closed": True,
            "is_50_50_outcome": True,
            "tokens": [],
        },
    )

    assert resolution.status == "resolved"
    assert resolution.resolution_type == "split"
    assert resolution.winner is None
    assert resolution.winning_token_id is None
    assert resolution.up_payout == Decimal("0.5")
    assert resolution.down_payout == Decimal("0.5")
    assert resolution.is_complete is True


def test_parse_polymarket_resolution_rejects_gamma_clob_disagreement():
    with pytest.raises(
        collector.ResolutionParseError,
        match="Gamma and CLOB official resolutions disagree",
    ):
        collector.parse_polymarket_resolution(
            resolved_gamma_event(),
            slug=current_market().slug,
            gamma_market_id="market-1",
            condition_id="condition-1",
            up_token_id="up-token",
            down_token_id="down-token",
            clob_data={
                "closed": True,
                "tokens": [
                    {
                        "token_id": "up-token",
                        "outcome": "Up",
                        "winner": True,
                    },
                    {
                        "token_id": "down-token",
                        "outcome": "Down",
                        "winner": False,
                    },
                ],
            },
        )


def test_parse_polymarket_resolution_requires_all_stored_market_ids_to_match():
    gamma_event = resolved_gamma_event()
    gamma_event["markets"][0]["conditionId"] = "different-condition"

    with pytest.raises(
        collector.ResolutionParseError,
        match="Gamma resolution identity mismatch",
    ):
        collector.parse_polymarket_resolution(
            gamma_event,
            slug=current_market().slug,
            gamma_market_id="market-1",
            condition_id="condition-1",
            up_token_id="up-token",
            down_token_id="down-token",
        )


def test_fetch_polymarket_resolution_parses_json_numbers_as_decimal():
    requests = []

    class FakeResponse:
        def __init__(self, text, status_code=200):
            self.text = text
            self.status_code = status_code

        def raise_for_status(self):
            return None

    class FakeClient:
        async def get(self, url):
            requests.append(url)
            if "/events/slug/" in url:
                return FakeResponse(
                    """{
                      "id":"event-1",
                      "slug":"btc-updown-5m-1783459200",
                      "eventMetadata":{
                        "priceToBeat":63337.115841440165,
                        "finalPrice":63336.71900847139
                      },
                      "markets":[{
                        "id":"market-1",
                        "slug":"btc-updown-5m-1783459200",
                        "conditionId":"condition-1",
                        "outcomes":"[\\"Up\\",\\"Down\\"]",
                        "outcomePrices":"[\\"0\\",\\"1\\"]",
                        "clobTokenIds":"[\\"up-token\\",\\"down-token\\"]",
                        "closed":true,
                        "closedTime":"2026-07-07 21:25:17+00",
                        "umaResolutionStatus":"resolved"
                      }]
                    }"""
                )
            return FakeResponse(
                """{
                  "closed":true,
                  "tokens":[
                    {"token_id":"up-token","outcome":"Up","winner":false},
                    {"token_id":"down-token","outcome":"Down","winner":true}
                  ]
                }"""
            )

    resolution = asyncio.run(
        collector.fetch_polymarket_resolution(
            FakeClient(),
            SimpleNamespace(
                POLYMARKET_GAMMA_BASE_URL="https://gamma-api.polymarket.com",
                POLYMARKET_CLOB_BASE_URL="https://clob.polymarket.com",
            ),
            {
                "market_id": 5_944_864,
                "slug": current_market().slug,
                "gamma_market_id": "market-1",
                "condition_id": "condition-1",
                "up_token_id": "up-token",
                "down_token_id": "down-token",
            },
        )
    )

    assert resolution.chainlink_open_price == Decimal("63337.115841440165")
    assert resolution.chainlink_close_price == Decimal("63336.71900847139")
    assert requests == [
        "https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1783459200",
        "https://clob.polymarket.com/markets/condition-1",
    ]


def test_fetch_polymarket_resolution_uses_gamma_when_optional_clob_json_is_invalid():
    class FakeResponse:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        async def get(self, url):
            if "/events/slug/" in url:
                return FakeResponse(
                    collector.json.dumps(resolved_gamma_event(), default=str)
                )
            return FakeResponse("{")

    resolution = asyncio.run(
        collector.fetch_polymarket_resolution(
            FakeClient(),
            SimpleNamespace(
                POLYMARKET_GAMMA_BASE_URL="https://gamma-api.polymarket.com",
                POLYMARKET_CLOB_BASE_URL="https://clob.polymarket.com",
            ),
            {
                "market_id": 5_944_864,
                "slug": current_market().slug,
                "gamma_market_id": "market-1",
                "condition_id": "condition-1",
                "up_token_id": "up-token",
                "down_token_id": "down-token",
            },
        )
    )

    assert resolution.status == "resolved"
    assert resolution.resolution_type == "winner"
    assert resolution.winner == "Down"
    assert resolution.resolution_source == "polymarket_gamma"
    assert resolution.is_complete is True


def test_reconcile_polymarket_resolution_persists_complete_official_result(
    monkeypatch,
):
    writes = []
    retries = []

    async def fake_fetch(client, settings, market):
        return collector.PolymarketResolution(
            status="resolved",
            resolution_type="winner",
            chainlink_open_price=Decimal("63337.115841440165"),
            chainlink_close_price=Decimal("63336.71900847139"),
            chainlink_source="polymarket_gamma_event_metadata",
            winner="Down",
            winning_token_id="down-token",
            up_payout=Decimal("0"),
            down_payout=Decimal("1"),
            resolved_at_ms=1_783_459_517_000,
            resolution_source="polymarket_clob_rest",
            raw_resolution={"gamma": {"id": "event-1"}},
        )

    async def fake_upsert(pool, **kwargs):
        writes.append(kwargs)

    async def fake_retry(pool, **kwargs):
        retries.append(kwargs)

    monkeypatch.setattr(collector, "fetch_polymarket_resolution", fake_fetch)
    monkeypatch.setattr(
        collector,
        "upsert_polymarket_btc_5m_resolution",
        fake_upsert,
    )
    monkeypatch.setattr(
        collector,
        "schedule_polymarket_resolution_retry",
        fake_retry,
    )

    complete = asyncio.run(
        collector.reconcile_polymarket_resolution_once(
            settings=SimpleNamespace(
                POLYMARKET_RESOLUTION_POLL_SECONDS=5,
                POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300,
            ),
            pool="pool",
            client="client",
            market={"market_id": 5_944_864, "resolution_attempts": 0},
            now_ms=1_783_459_520_000,
        )
    )

    assert complete is True
    assert retries == []
    assert writes[0]["resolution_status"] == "resolved"
    assert writes[0]["resolution_type"] == "winner"
    assert writes[0]["winner"] == "Down"
    assert writes[0]["chainlink_open_price"] == Decimal("63337.115841440165")
    assert writes[0]["next_check_ms"] is None
    assert writes[0]["resolution_attempts"] == 1


def test_reconcile_polymarket_resolution_schedules_durable_retry_on_failure(
    monkeypatch,
):
    retries = []

    async def fake_fetch(client, settings, market):
        raise httpx.ConnectError("Gamma unavailable")

    async def fake_retry(pool, **kwargs):
        retries.append(kwargs)

    monkeypatch.setattr(collector, "fetch_polymarket_resolution", fake_fetch)
    monkeypatch.setattr(
        collector,
        "schedule_polymarket_resolution_retry",
        fake_retry,
    )

    complete = asyncio.run(
        collector.reconcile_polymarket_resolution_once(
            settings=SimpleNamespace(
                POLYMARKET_RESOLUTION_POLL_SECONDS=5,
                POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300,
            ),
            pool="pool",
            client="client",
            market={"market_id": 5_944_864, "resolution_attempts": 2},
            now_ms=1_783_459_520_000,
        )
    )

    assert complete is False
    assert retries == [
        {
            "market_id": 5_944_864,
            "checked_ms": 1_783_459_520_000,
            "next_check_ms": 1_783_459_540_000,
            "resolution_attempts": 3,
        }
    ]


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


def test_market_resolved_message_captures_official_websocket_winner():
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
    )
    message = {
        "event_type": "market_resolved",
        "timestamp": "1783459517000",
        "winning_outcome": "Down",
        "winning_asset_id": "down-token",
    }

    assert collector.apply_clob_message(
        state,
        message,
        received_ms=1_783_459_517_100,
    )
    assert state.resolved is True
    assert state.winning_outcome == "Down"
    assert state.winning_asset_id == "down-token"
    assert state.resolution_event_ms == 1_783_459_517_000
    assert state.raw_resolution_event == message


def test_persist_websocket_resolution_writes_official_winner_for_reconciliation(
    monkeypatch,
):
    writes = []

    async def fake_upsert(pool, **kwargs):
        writes.append(kwargs)

    monkeypatch.setattr(
        collector,
        "upsert_polymarket_btc_5m_resolution",
        fake_upsert,
    )
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
        resolved=True,
        winning_outcome="Down",
        winning_asset_id="down-token",
        resolution_event_ms=1_783_459_517_000,
        raw_resolution_event={"event_type": "market_resolved"},
    )

    written = asyncio.run(
        collector.persist_websocket_resolution(
            settings=SimpleNamespace(POLYMARKET_RESOLUTION_POLL_SECONDS=5),
            pool="pool",
            current_market=current_market(),
            state=state,
            checked_ms=1_783_459_517_100,
        )
    )

    assert written is True
    assert writes[0]["resolution_status"] == "resolved"
    assert writes[0]["resolution_type"] == "winner"
    assert writes[0]["winner"] == "Down"
    assert writes[0]["winning_token_id"] == "down-token"
    assert writes[0]["chainlink_open_price"] is None
    assert writes[0]["next_check_ms"] == 1_783_459_522_100
    assert writes[0]["resolution_source"] == "polymarket_clob_ws"


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
            now_ms=1_783_459_199_999,
            stale_ms=15_000,
        )
        is None
    )
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


def test_build_probability_snapshot_requires_asks_not_bids():
    market = current_market()
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
        up_bid=None,
        up_ask=Decimal("0.56"),
        down_bid=None,
        down_ask=Decimal("0.45"),
        latest_provider_event_ms=1_783_459_200_000,
        latest_received_ms=1_783_459_200_000,
    )

    snapshot = collector.build_probability_snapshot(
        current_market=market,
        state=state,
        now_ms=1_783_459_200_123,
        stale_ms=15_000,
    )

    assert snapshot is not None
    assert snapshot.up_mid == Decimal("0.56")
    assert snapshot.down_mid == Decimal("0.45")

    state.down_ask = None
    assert (
        collector.build_probability_snapshot(
            current_market=market,
            state=state,
            now_ms=1_783_459_200_123,
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


def test_rest_prime_asks_before_t0_allow_t0_snapshot(monkeypatch):
    async def fake_fetch_best_asks_from_clob_prices(client, settings, current_market):
        return Decimal("0.56"), Decimal("0.45")

    calls = []

    async def fake_upsert_polymarket_probability_sample(pool, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        collector,
        "fetch_best_asks_from_clob_prices",
        fake_fetch_best_asks_from_clob_prices,
    )
    monkeypatch.setattr(
        collector,
        "upsert_polymarket_probability_sample",
        fake_upsert_polymarket_probability_sample,
    )
    monkeypatch.setattr(
        collector,
        "current_utc_epoch_ms",
        lambda: current_market().window.market_start_ms - 500,
    )
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
    )

    updated = asyncio.run(
        collector.prime_probability_state_from_rest(
            client="client",
            settings=SimpleNamespace(POLYMARKET_CLOB_BASE_URL="https://clob.polymarket.com"),
            current_market=current_market(),
            state=state,
        )
    )
    written = asyncio.run(
        collector.sample_probability_once(
            pool="pool",
            current_market=current_market(),
            state=state,
            source="polymarket_clob",
            stale_ms=15_000,
            now_ms=current_market().window.market_start_ms,
        )
    )

    assert updated is True
    assert written is True
    assert calls[0]["sample_second_ms"] == current_market().window.market_start_ms
    assert calls[0]["up_ask"] == Decimal("0.56")
    assert calls[0]["down_ask"] == Decimal("0.45")


def test_unavailable_asks_skip_snapshot_without_backfill(monkeypatch):
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
        latest_provider_event_ms=current_market().window.market_start_ms,
        latest_received_ms=current_market().window.market_start_ms,
    )

    written = asyncio.run(
        collector.sample_probability_once(
            pool="pool",
            current_market=current_market(),
            state=state,
            source="polymarket_clob",
            stale_ms=15_000,
            now_ms=current_market().window.market_start_ms,
        )
    )

    assert written is False
    assert calls == []


def test_fetch_best_asks_from_clob_prices_posts_sell_sides():
    requests = []

    class FakeResponse:
        text = '{"up-token": {"SELL": 0.56}, "down-token": {"SELL": "0.45"}}'

        def raise_for_status(self):
            return None

    class FakeClient:
        async def post(self, url, *, json):
            requests.append((url, json))
            return FakeResponse()

    up_ask, down_ask = asyncio.run(
        collector.fetch_best_asks_from_clob_prices(
            FakeClient(),
            SimpleNamespace(POLYMARKET_CLOB_BASE_URL="https://clob.polymarket.com/"),
            current_market(),
        )
    )

    assert up_ask == Decimal("0.56")
    assert down_ask == Decimal("0.45")
    assert requests == [
        (
            "https://clob.polymarket.com/prices",
            [
                {"token_id": "up-token", "side": "SELL"},
                {"token_id": "down-token", "side": "SELL"},
            ],
        )
    ]


def test_run_collector_preloads_and_starts_next_market_before_boundary(monkeypatch):
    start_ms = current_market().window.market_start_ms
    clock = {"now_ms": start_ms + 120_000}
    discover_calls = []
    collect_calls = []
    created_task_names = []

    class StopCollector(asyncio.CancelledError):
        pass

    class FakePool:
        async def close(self):
            return None

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeTask:
        def __init__(self, awaitable):
            self.cancelled = False
            self.done_value = False
            if asyncio.iscoroutine(awaitable):
                awaitable.close()

        def done(self):
            return self.done_value

        def cancel(self):
            self.cancelled = True
            self.done_value = True

        async def _wait(self):
            self.done_value = True
            if self.cancelled:
                raise asyncio.CancelledError
            return None

        def __await__(self):
            return self._wait().__await__()

    async def fake_create_pool(database_url):
        return FakePool()

    async def fake_sleep_until_ms_or_task_done(target_ms, task):
        clock["now_ms"] = target_ms

    async def fake_discover_current_polymarket_market(settings, pool, client, window):
        discover_calls.append((window.market_id, clock["now_ms"], window))
        return collector.CurrentPolymarketMarket(
            window=window,
            slug=f"btc-updown-5m-{window.market_start_ms // 1000}",
            gamma_event_id="event",
            gamma_market_id="market",
            condition_id="condition",
            question="BTC Up or Down",
            start_ms=window.market_start_ms,
            end_ms=window.market_end_ms,
            up_token_id=f"up-{window.market_id}",
            down_token_id=f"down-{window.market_id}",
            up_outcome="Up",
            down_outcome="Down",
            active=True,
            closed=False,
            archived=False,
            raw_gamma={"market": {"id": "market"}},
        )

    def fake_collect_current_market(*, settings, pool, client, current_market):
        collect_calls.append((current_market.window.market_id, clock["now_ms"], current_market))
        if len(collect_calls) == 2:
            raise StopCollector

        async def noop():
            return None

        return noop()

    def fake_create_task(awaitable):
        if asyncio.iscoroutine(awaitable):
            created_task_names.append(awaitable.cr_code.co_name)
        return FakeTask(awaitable)

    settings = SimpleNamespace(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        POLYMARKET_GAMMA_BASE_URL="https://gamma-api.polymarket.com",
        POLYMARKET_CLOB_WS_URL="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        POLYMARKET_PROBABILITY_SOURCE="polymarket_clob",
        POLYMARKET_PROBABILITY_STALE_MS=15_000,
        POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS=45,
        POLYMARKET_NEXT_MARKET_RETRY_MS=500,
        POLYMARKET_RESOLUTION_POLL_SECONDS=5,
        POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS=300,
        POLYMARKET_RESOLUTION_BATCH_SIZE=20,
        POLYMARKET_RESOLUTION_WS_GRACE_SECONDS=30,
    )

    monkeypatch.setattr(collector, "create_pool", fake_create_pool)
    monkeypatch.setattr(collector, "require_collector_database_url", lambda settings: "db")
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: clock["now_ms"])
    monkeypatch.setattr(
        collector,
        "sleep_until_ms_or_task_done",
        fake_sleep_until_ms_or_task_done,
    )
    monkeypatch.setattr(
        collector,
        "discover_current_polymarket_market",
        fake_discover_current_polymarket_market,
    )
    monkeypatch.setattr(collector, "collect_current_market", fake_collect_current_market)
    monkeypatch.setattr(collector.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(collector.httpx, "AsyncClient", FakeAsyncClient)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(collector.run_collector(settings))

    current_window = discover_calls[0][2]
    next_window = discover_calls[1][2]

    assert discover_calls[1][1] == current_window.market_end_ms - 45_000
    assert discover_calls[1][1] < current_window.market_end_ms
    assert collect_calls[1][0] == next_window.market_id
    assert collect_calls[1][1] < next_window.market_start_ms
    assert "resolution_reconciler_loop" in created_task_names


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


def test_collect_current_market_treats_post_end_socket_failure_as_rest_fallback(
    monkeypatch,
):
    class FailingWebSocket:
        def __init__(self):
            self.sent = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def send(self, message):
            self.sent.append(message)

        async def recv(self):
            raise ConnectionError("old market socket closed")

    async def wait_forever(*args, **kwargs):
        await asyncio.Event().wait()

    websocket = FailingWebSocket()
    monkeypatch.setattr(
        collector.websockets,
        "connect",
        lambda *args, **kwargs: websocket,
    )
    monkeypatch.setattr(collector, "clob_ping_loop", wait_forever)
    monkeypatch.setattr(collector, "probability_sampler_loop", wait_forever)
    monkeypatch.setattr(collector, "probability_rest_prime_loop", wait_forever)
    monkeypatch.setattr(
        collector,
        "current_utc_epoch_ms",
        lambda: current_market().window.market_end_ms + 1,
    )

    asyncio.run(
        collector.collect_current_market(
            settings=SimpleNamespace(
                POLYMARKET_CLOB_WS_URL="wss://example.test/ws",
                POLYMARKET_CLOB_PING_SECONDS=10,
                POLYMARKET_PROBABILITY_SOURCE="polymarket_clob",
                POLYMARKET_PROBABILITY_STALE_MS=15_000,
                POLYMARKET_RESOLUTION_WS_GRACE_SECONDS=30,
            ),
            pool=object(),
            client=object(),
            current_market=current_market(),
        )
    )

    assert len(websocket.sent) == 1
