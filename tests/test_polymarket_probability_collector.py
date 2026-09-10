import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

import price_collector.polymarket_probability_collector as collector
from price_collector.market import MarketWindow


LEGACY_MARKET_START_MS = 1_783_459_200_000
CURRENT_MARKET_START_MS = collector.TWAP_60S_CUTOVER_MS


def market_window(*, start_ms=LEGACY_MARKET_START_MS):
    return MarketWindow(
        market_id=start_ms // 300_000,
        market_start_ms=start_ms,
        market_end_ms=start_ms + 300_000,
    )


def current_market(*, start_ms=LEGACY_MARKET_START_MS, settlement_rule=None):
    window = market_window(start_ms=start_ms)
    if settlement_rule is None:
        settlement_rule = collector.expected_twap_settlement_rule(start_ms)
    (
        settlement_reference,
        settlement_window_s,
        settlement_source_url,
        settlement_rule_version,
    ) = settlement_rule
    return collector.CurrentPolymarketMarket(
        window=window,
        slug=f"btc-updown-5m-{start_ms // 1000}",
        gamma_event_id="event-1",
        gamma_market_id="market-1",
        condition_id="condition-1",
        question="BTC Up or Down",
        start_ms=start_ms,
        end_ms=start_ms + 300_000,
        up_token_id="up-token",
        down_token_id="down-token",
        up_outcome="Up",
        down_outcome="Down",
        active=True,
        closed=False,
        archived=False,
        settlement_reference=settlement_reference,
        settlement_window_s=settlement_window_s,
        settlement_source_url=settlement_source_url,
        settlement_rule_version=settlement_rule_version,
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
                "startDate": "2026-07-06T21:20:00Z",
                "eventStartTime": "2026-07-07T21:20:00Z",
                "endDate": "2026-07-07T21:25:00Z",
                "resolutionSource": collector.LEGACY_TWAP_SOURCE_URL,
                "cryptoMarketConfigId": collector.LEGACY_TWAP_RULE_VERSION,
                "cryptoMarketConfig": {
                    "id": collector.LEGACY_TWAP_RULE_VERSION,
                    "asset": "btc",
                    "duration": "5m",
                    "twapEnabled": True,
                    "twapLookbackSeconds": collector.LEGACY_TWAP_WINDOW_SECONDS,
                },
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
    assert market.settlement_reference == "chainlink_twap"
    assert market.settlement_window_s == 30
    assert market.settlement_source_url == collector.LEGACY_TWAP_SOURCE_URL
    assert market.settlement_rule_version == collector.LEGACY_TWAP_RULE_VERSION


def test_market_rule_parser_recognizes_legacy_and_fails_mixed_twap_closed():
    assert collector.parse_market_settlement_rule(
        {},
        {
            "resolutionSource": "https://data.chain.link/streams/btc-usd",
            "description": "Resolves from the Chainlink BTC/USD data stream.",
        },
    ) == (
        collector.SETTLEMENT_REFERENCE_CHAINLINK_SPOT,
        None,
        "https://data.chain.link/streams/btc-usd",
        collector.LEGACY_SPOT_RULE_VERSION,
    )

    assert collector.parse_market_settlement_rule(
        {},
        {
            "resolutionSource": collector.LEGACY_TWAP_SOURCE_URL,
            "cryptoMarketConfigId": collector.LEGACY_TWAP_RULE_VERSION,
            "cryptoMarketConfig": {
                "id": collector.LEGACY_TWAP_RULE_VERSION,
                "asset": "btc",
                "duration": "5m",
                "twapEnabled": True,
                "twapLookbackSeconds": collector.LEGACY_TWAP_WINDOW_SECONDS,
            },
        },
    ) == collector.LEGACY_TWAP_SETTLEMENT_RULE

    reference, window_s, source_url, rule_version = (
        collector.parse_market_settlement_rule(
            {},
            {
                "resolutionSource": collector.SUPPORTED_TWAP_SOURCE_URL,
                "cryptoMarketConfigId": collector.LEGACY_TWAP_RULE_VERSION,
                "cryptoMarketConfig": {
                    "id": collector.LEGACY_TWAP_RULE_VERSION,
                    "asset": "btc",
                    "duration": "5m",
                    "twapEnabled": True,
                    "twapLookbackSeconds": collector.SUPPORTED_TWAP_WINDOW_SECONDS,
                },
            },
        )
    )
    assert reference == collector.SETTLEMENT_REFERENCE_UNKNOWN
    assert window_s == 60
    assert source_url == collector.SUPPORTED_TWAP_SOURCE_URL
    assert rule_version == "btc-5m-twap-30"


def test_expected_twap_rule_changes_only_at_exact_sixty_second_cutover():
    assert collector.TWAP_60S_CUTOVER_MS == 1_786_665_600_000
    assert collector.expected_twap_settlement_rule(
        collector.TWAP_60S_CUTOVER_MS - 300_000
    ) == collector.LEGACY_TWAP_SETTLEMENT_RULE
    assert collector.expected_twap_settlement_rule(
        collector.TWAP_60S_CUTOVER_MS
    ) == collector.CURRENT_TWAP_SETTLEMENT_RULE
    assert collector.expected_twap_settlement_rule(
        collector.TWAP_60S_CUTOVER_MS + 300_000
    ) == collector.CURRENT_TWAP_SETTLEMENT_RULE


@pytest.mark.parametrize(
    ("start_ms", "settlement_rule"),
    (
        (
            collector.TWAP_60S_CUTOVER_MS - 300_000,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS + 300_000,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
    ),
)
def test_store_current_market_accepts_exact_rule_in_force(
    monkeypatch,
    start_ms,
    settlement_rule,
):
    writes = []

    async def fake_upsert(pool, **kwargs):
        writes.append((pool, kwargs))

    monkeypatch.setattr(
        collector,
        "upsert_polymarket_btc_5m_market",
        fake_upsert,
    )
    market = current_market(
        start_ms=start_ms,
        settlement_rule=settlement_rule,
    )

    asyncio.run(
        collector.store_current_market(
            "pool",
            market,
            seen_ms=start_ms,
        )
    )

    assert len(writes) == 1
    assert writes[0][0] == "pool"
    assert writes[0][1]["window"] == market.window
    assert (
        writes[0][1]["settlement_reference"],
        writes[0][1]["settlement_window_s"],
        writes[0][1]["settlement_source_url"],
        writes[0][1]["settlement_rule_version"],
    ) == settlement_rule


@pytest.mark.parametrize(
    ("start_ms", "settlement_rule"),
    (
        (
            collector.TWAP_60S_CUTOVER_MS - 300_000,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS + 300_000,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
    ),
)
def test_store_current_market_rejects_inverted_cutover_identity_before_database(
    start_ms,
    settlement_rule,
):
    inverted = current_market(
        start_ms=start_ms,
        settlement_rule=settlement_rule,
    )

    with pytest.raises(collector.GammaDiscoveryError, match="refusing to store"):
        asyncio.run(
            collector.store_current_market(
                object(),
                inverted,
                seen_ms=start_ms,
            )
        )


def test_store_current_market_refuses_unknown_rules_before_database():
    for reference in (
        collector.SETTLEMENT_REFERENCE_UNKNOWN,
        collector.SETTLEMENT_REFERENCE_CHAINLINK_SPOT,
    ):
        unsupported = replace(
            current_market(),
            settlement_reference=reference,
            settlement_window_s=None,
            settlement_source_url=None,
            settlement_rule_version=None,
        )
        with pytest.raises(
            collector.GammaDiscoveryError,
            match="refusing to store",
        ):
            asyncio.run(
                collector.store_current_market(
                    object(),
                    unsupported,
                    seen_ms=LEGACY_MARKET_START_MS,
                )
            )


@pytest.mark.parametrize(
    ("start_ms", "settlement_rule"),
    (
        (
            collector.TWAP_60S_CUTOVER_MS - 300_000,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS + 300_000,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
    ),
)
def test_discovery_accepts_exact_rule_in_force(
    monkeypatch,
    start_ms,
    settlement_rule,
):
    candidate = current_market(
        start_ms=start_ms,
        settlement_rule=settlement_rule,
    )
    stores = []

    class Response:
        status_code = 200
        text = "{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {}

    class Client:
        async def get(self, url, **kwargs):
            return Response()

    async def fake_store(pool, market, *, seen_ms):
        stores.append((pool, market, seen_ms))

    monkeypatch.setattr(
        collector,
        "parse_current_market_from_gamma",
        lambda payload, *, window, slug: candidate,
    )
    monkeypatch.setattr(collector, "store_current_market", fake_store)
    monkeypatch.setattr(collector, "current_utc_epoch_ms", lambda: start_ms)

    discovered = asyncio.run(
        collector.discover_current_polymarket_market(
            SimpleNamespace(
                POLYMARKET_BTC_5M_SLUG_PREFIX="btc-updown-5m",
                POLYMARKET_GAMMA_BASE_URL="https://gamma.example.test",
            ),
            "pool",
            Client(),
            candidate.window,
        )
    )

    assert discovered == candidate
    assert stores == [("pool", candidate, start_ms)]


@pytest.mark.parametrize(
    ("start_ms", "settlement_rule"),
    (
        (
            collector.TWAP_60S_CUTOVER_MS - 300_000,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS + 300_000,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
    ),
)
def test_discovery_rejects_inverted_cutover_identity(
    monkeypatch,
    start_ms,
    settlement_rule,
):
    candidate = current_market(
        start_ms=start_ms,
        settlement_rule=settlement_rule,
    )

    class Response:
        status_code = 200
        text = "{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {}

    class Client:
        async def get(self, url, **kwargs):
            return Response()

    async def unexpected_store(*args, **kwargs):
        raise AssertionError("inverted discovery must not store the market")

    monkeypatch.setattr(
        collector,
        "parse_current_market_from_gamma",
        lambda payload, *, window, slug: candidate,
    )
    monkeypatch.setattr(collector, "store_current_market", unexpected_store)

    with pytest.raises(collector.GammaDiscoveryError, match="expected BTC 5m"):
        asyncio.run(
            collector.discover_current_polymarket_market(
                SimpleNamespace(
                    POLYMARKET_BTC_5M_SLUG_PREFIX="btc-updown-5m",
                    POLYMARKET_GAMMA_BASE_URL="https://gamma.example.test",
                ),
                "pool",
                Client(),
                candidate.window,
            )
        )


def test_historical_discovery_fallback_does_not_require_active_open_market(
    monkeypatch,
):
    start_ms = collector.TWAP_60S_CUTOVER_MS
    candidate = current_market(start_ms=start_ms)
    requests = []
    stores = []

    class Response:
        text = "{}"

        def __init__(self, status_code):
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return {}

    class Client:
        async def get(self, url, **kwargs):
            requests.append((url, kwargs))
            return Response(404 if "/events/slug/" in url else 200)

    async def fake_store(pool, market, *, seen_ms):
        stores.append((pool, market, seen_ms))

    monkeypatch.setattr(
        collector,
        "parse_current_market_from_gamma",
        lambda payload, *, window, slug: candidate,
    )
    monkeypatch.setattr(collector, "store_current_market", fake_store)
    monkeypatch.setattr(
        collector,
        "current_utc_epoch_ms",
        lambda: candidate.window.market_end_ms + 1,
    )

    discovered = asyncio.run(
        collector.discover_current_polymarket_market(
            SimpleNamespace(
                POLYMARKET_BTC_5M_SLUG_PREFIX="btc-updown-5m",
                POLYMARKET_GAMMA_BASE_URL="https://gamma.example.test",
            ),
            "pool",
            Client(),
            candidate.window,
        )
    )

    assert discovered == candidate
    assert requests == [
        (
            f"https://gamma.example.test/events/slug/{candidate.slug}",
            {},
        ),
        (
            "https://gamma.example.test/markets",
            {"params": {"slug": candidate.slug}},
        ),
    ]
    assert stores == [
        ("pool", candidate, candidate.window.market_end_ms + 1)
    ]


def test_backfill_scan_advances_cursor_across_failures_and_stores_metadata_only(
    monkeypatch,
):
    backfill_start_ms = collector.TWAP_60S_CUTOVER_MS + 300_000
    settings = SimpleNamespace(
        POLYMARKET_MARKET_BACKFILL_START_MS=backfill_start_ms,
    )
    windows = [
        market_window(start_ms=collector.TWAP_60S_CUTOVER_MS),
        market_window(start_ms=collector.TWAP_60S_CUTOVER_MS + 300_000),
    ]
    scans = []
    discoveries = []

    async def fake_fetch(pool, **kwargs):
        scans.append((pool, kwargs))
        return windows

    async def fake_discover(settings, pool, client, window):
        discoveries.append((settings, pool, client, window))
        if window == windows[1]:
            raise collector.GammaDiscoveryError("missing historical metadata")
        return current_market(start_ms=window.market_start_ms)

    monkeypatch.setattr(
        collector,
        "fetch_missing_polymarket_market_windows",
        fake_fetch,
    )
    monkeypatch.setattr(
        collector,
        "discover_current_polymarket_market",
        fake_discover,
    )

    attempted, stored, last_scanned_ms = asyncio.run(
        collector.backfill_missing_polymarket_markets_once(
            settings=settings,
            pool="pool",
            client="client",
            now_ms=collector.TWAP_60S_CUTOVER_MS + 900_000,
            after_market_start_ms=collector.TWAP_60S_CUTOVER_MS - 300_000,
            limit=20,
        )
    )

    assert (attempted, stored) == (2, 1)
    assert last_scanned_ms == windows[-1].market_start_ms
    assert scans == [
        (
            "pool",
            {
                "first_market_start_ms": backfill_start_ms,
                "now_ms": collector.TWAP_60S_CUTOVER_MS + 900_000,
                "after_market_start_ms": (
                    collector.TWAP_60S_CUTOVER_MS - 300_000
                ),
                "limit": 20,
            },
        )
    ]
    assert collector.expected_twap_settlement_rule(
        collector.TWAP_60S_CUTOVER_MS - 300_000
    ) == collector.LEGACY_TWAP_SETTLEMENT_RULE
    assert collector.expected_twap_settlement_rule(
        collector.TWAP_60S_CUTOVER_MS
    ) == collector.CURRENT_TWAP_SETTLEMENT_RULE
    assert [call[3] for call in discoveries] == windows


def resolved_gamma_event(
    *,
    outcome_prices='["0","1"]',
    market_start_ms=LEGACY_MARKET_START_MS,
    settlement_rule=None,
):
    if settlement_rule is None:
        settlement_rule = collector.expected_twap_settlement_rule(
            market_start_ms
        )
    (
        _settlement_reference,
        settlement_window_s,
        settlement_source_url,
        settlement_rule_version,
    ) = settlement_rule
    slug = f"btc-updown-5m-{market_start_ms // 1000}"
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
                "resolutionSource": settlement_source_url,
                "cryptoMarketConfigId": settlement_rule_version,
                "cryptoMarketConfig": {
                    "id": settlement_rule_version,
                    "asset": "btc",
                    "duration": "5m",
                    "twapEnabled": True,
                    "twapLookbackSeconds": settlement_window_s,
                },
                "closed": True,
                "closedTime": datetime.fromtimestamp(
                    (market_start_ms + 317_000) / 1000,
                    tz=timezone.utc,
                ).isoformat(),
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


def test_resolution_revalidates_live_gamma_twap_rule_and_rejects_changes():
    changed = resolved_gamma_event(
        market_start_ms=CURRENT_MARKET_START_MS,
        settlement_rule=collector.CURRENT_TWAP_SETTLEMENT_RULE,
    )
    changed["markets"][0]["cryptoMarketConfig"][
        "twapLookbackSeconds"
    ] = collector.LEGACY_TWAP_WINDOW_SECONDS
    current = current_market(start_ms=CURRENT_MARKET_START_MS)

    with pytest.raises(
        collector.ResolutionParseError,
        match="canonical Gamma market no longer",
    ):
        collector.parse_polymarket_resolution(
            changed,
            slug=current.slug,
            gamma_market_id="market-1",
            condition_id="condition-1",
            up_token_id="up-token",
            down_token_id="down-token",
            expected_settlement_reference="chainlink_twap",
            expected_settlement_window_s=collector.CURRENT_TWAP_WINDOW_SECONDS,
            expected_settlement_source_url=collector.CURRENT_TWAP_SOURCE_URL,
            expected_settlement_rule_version=collector.CURRENT_TWAP_RULE_VERSION,
            expected_market_start_ms=CURRENT_MARKET_START_MS,
        )

    with pytest.raises(
        collector.ResolutionParseError,
        match="contradicts the discovered rule",
    ):
        collector.parse_polymarket_resolution(
            resolved_gamma_event(
                market_start_ms=CURRENT_MARKET_START_MS,
                settlement_rule=collector.CURRENT_TWAP_SETTLEMENT_RULE,
            ),
            slug=current.slug,
            gamma_market_id="market-1",
            condition_id="condition-1",
            up_token_id="up-token",
            down_token_id="down-token",
            expected_settlement_reference="chainlink_twap",
            expected_settlement_window_s=collector.LEGACY_TWAP_WINDOW_SECONDS,
            expected_settlement_source_url=collector.LEGACY_TWAP_SOURCE_URL,
            expected_settlement_rule_version=collector.LEGACY_TWAP_RULE_VERSION,
            expected_market_start_ms=CURRENT_MARKET_START_MS,
        )


@pytest.mark.parametrize(
    ("market_start_ms", "settlement_rule"),
    (
        (
            collector.TWAP_60S_CUTOVER_MS - 300_000,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS + 300_000,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
    ),
)
def test_resolution_reconciliation_accepts_rule_in_force_at_market_boundary(
    market_start_ms,
    settlement_rule,
):
    gamma = resolved_gamma_event(
        market_start_ms=market_start_ms,
        settlement_rule=settlement_rule,
    )
    market = current_market(
        start_ms=market_start_ms,
        settlement_rule=settlement_rule,
    )
    reference, window_s, source_url, rule_version = settlement_rule

    resolution = collector.parse_polymarket_resolution(
        gamma,
        slug=market.slug,
        gamma_market_id="market-1",
        condition_id="condition-1",
        up_token_id="up-token",
        down_token_id="down-token",
        expected_settlement_reference=reference,
        expected_settlement_window_s=window_s,
        expected_settlement_source_url=source_url,
        expected_settlement_rule_version=rule_version,
        expected_market_start_ms=market_start_ms,
    )

    assert resolution.winner == "Down"
    assert resolution.chainlink_open_price == Decimal("63337.115841440165")
    assert resolution.chainlink_close_price == Decimal("63336.71900847139")


@pytest.mark.parametrize(
    ("market_start_ms", "inverted_rule"),
    (
        (
            collector.TWAP_60S_CUTOVER_MS - 300_000,
            collector.CURRENT_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
        (
            collector.TWAP_60S_CUTOVER_MS + 300_000,
            collector.LEGACY_TWAP_SETTLEMENT_RULE,
        ),
    ),
)
def test_resolution_reconciliation_rejects_inverted_cutover_identity(
    market_start_ms,
    inverted_rule,
):
    gamma = resolved_gamma_event(
        market_start_ms=market_start_ms,
        settlement_rule=inverted_rule,
    )
    market = current_market(
        start_ms=market_start_ms,
        settlement_rule=inverted_rule,
    )
    reference, window_s, source_url, rule_version = inverted_rule

    with pytest.raises(
        collector.ResolutionParseError,
        match="rule in force at the market boundary",
    ):
        collector.parse_polymarket_resolution(
            gamma,
            slug=market.slug,
            gamma_market_id="market-1",
            condition_id="condition-1",
            up_token_id="up-token",
            down_token_id="down-token",
            expected_settlement_reference=reference,
            expected_settlement_window_s=window_s,
            expected_settlement_source_url=source_url,
            expected_settlement_rule_version=rule_version,
            expected_market_start_ms=market_start_ms,
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
                        "resolutionSource":"https://data.chain.link/streams/btc-usd-twap-30s-streams",
                        "cryptoMarketConfigId":"btc-5m-twap-30",
                        "cryptoMarketConfig":{
                          "id":"btc-5m-twap-30",
                          "asset":"btc",
                          "duration":"5m",
                          "twapEnabled":true,
                          "twapLookbackSeconds":30
                        },
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
            market={
                "market_id": 5_944_864,
                "resolution_attempts": 0,
                "settlement_rule_version": collector.LEGACY_TWAP_RULE_VERSION,
                "reconciled_settlement_rule_version": "stale-rule",
                "settlement_identity_refresh_required": True,
            },
            now_ms=1_783_459_520_000,
        )
    )

    assert complete is True
    assert retries == []
    assert writes[0]["resolution_status"] == "resolved"
    assert writes[0]["resolution_type"] == "winner"
    assert writes[0]["winner"] == "Down"
    assert writes[0]["chainlink_open_price"] == Decimal("63337.115841440165")
    assert (
        writes[0]["expected_settlement_rule_version"]
        == collector.LEGACY_TWAP_RULE_VERSION
    )
    assert writes[0]["settlement_identity_validated"] is True
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
            market={
                "market_id": 5_944_864,
                "resolution_attempts": 2,
                "settlement_rule_version": collector.LEGACY_TWAP_RULE_VERSION,
                "reconciled_settlement_rule_version": "stale-rule",
                "settlement_identity_refresh_required": True,
            },
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


def test_resolution_reconciler_pages_backfill_before_due_scan_and_wraps_cursor(
    monkeypatch,
):
    cutover_ms = collector.TWAP_60S_CUTOVER_MS
    clock = {"now_ms": cutover_ms + 900_000}
    order = []
    cursors = []
    sleep_calls = []
    backfill_results = [
        (2, 1, cutover_ms + 300_000),
        (1, 0, cutover_ms + 600_000),
        (0, 0, None),
        (0, 0, None),
    ]

    class StopSession(RuntimeError):
        pass

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            assert kwargs == {"timeout": 10.0}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    async def fake_backfill(**kwargs):
        order.append("backfill")
        assert kwargs["settings"] is settings
        assert kwargs["pool"] == "pool"
        assert isinstance(kwargs["client"], FakeAsyncClient)
        assert kwargs["now_ms"] == clock["now_ms"]
        assert kwargs["limit"] == 2
        cursors.append(kwargs["after_market_start_ms"])
        return backfill_results[len(cursors) - 1]

    async def fake_fetch_due(pool, **kwargs):
        order.append("due")
        assert pool == "pool"
        assert kwargs == {"now_ms": clock["now_ms"], "limit": 2}
        return [{"market_id": 5_955_552}]

    async def fake_reconcile(**kwargs):
        order.append("reconcile")
        assert kwargs["settings"] is settings
        assert kwargs["pool"] == "pool"
        assert isinstance(kwargs["client"], FakeAsyncClient)
        assert kwargs["market"] == {"market_id": 5_955_552}

    async def fake_sleep(seconds):
        order.append("sleep")
        sleep_calls.append(seconds)
        if len(sleep_calls) == 4:
            raise StopSession
        clock["now_ms"] += 60_000 if len(sleep_calls) == 3 else 5_000

    settings = SimpleNamespace(
        POLYMARKET_RESOLUTION_POLL_SECONDS=5,
        POLYMARKET_RESOLUTION_BATCH_SIZE=2,
    )
    monkeypatch.setattr(collector.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(
        collector,
        "current_utc_epoch_ms",
        lambda: clock["now_ms"],
    )
    monkeypatch.setattr(
        collector,
        "backfill_missing_polymarket_markets_once",
        fake_backfill,
    )
    monkeypatch.setattr(
        collector,
        "fetch_due_polymarket_resolutions",
        fake_fetch_due,
    )
    monkeypatch.setattr(
        collector,
        "reconcile_polymarket_resolution_once",
        fake_reconcile,
    )
    monkeypatch.setattr(collector.asyncio, "sleep", fake_sleep)

    with pytest.raises(StopSession):
        asyncio.run(collector._resolution_reconciler_session(settings, "pool"))

    assert cursors == [
        None,
        cutover_ms + 300_000,
        cutover_ms + 600_000,
        None,
    ]
    assert order == [
        "backfill",
        "due",
        "reconcile",
        "sleep",
    ] * 4
    assert sleep_calls == [5, 5, 5, 5]


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
    assert state.up_bid_provider_event_ms == 1_783_459_200_123
    assert state.up_ask_received_ms == 1_783_459_200_200
    assert state.down_bid_provider_event_ms == 1_783_459_200_456
    assert state.down_ask_received_ms == 1_783_459_200_500


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
    assert (
        writes[0]["expected_settlement_rule_version"]
        == collector.LEGACY_TWAP_RULE_VERSION
    )
    assert writes[0]["settlement_identity_validated"] is False
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
        up_bid_provider_event_ms=1_783_459_200_500,
        up_bid_received_ms=1_783_459_200_500,
        up_ask_provider_event_ms=1_783_459_200_500,
        up_ask_received_ms=1_783_459_200_500,
        down_bid_provider_event_ms=1_783_459_200_500,
        down_bid_received_ms=1_783_459_200_500,
        down_ask_provider_event_ms=1_783_459_200_500,
        down_ask_received_ms=1_783_459_200_500,
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
        up_ask_provider_event_ms=1_783_459_200_000,
        up_ask_received_ms=1_783_459_200_000,
        down_ask_provider_event_ms=1_783_459_200_000,
        down_ask_received_ms=1_783_459_200_000,
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


def test_build_probability_snapshot_rejects_one_stale_outcome():
    market = current_market()
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
    )
    assert state.update_token(
        "down-token",
        bid=Decimal("0.50"),
        ask=Decimal("0.53"),
        replace=True,
        provider_event_ms=1_783_459_200_000,
        received_ms=1_783_459_200_000,
        event_type="book",
    )
    assert state.update_token(
        "up-token",
        bid=Decimal("0.47"),
        ask=Decimal("0.49"),
        replace=True,
        provider_event_ms=1_783_459_216_000,
        received_ms=1_783_459_216_000,
        event_type="book",
    )

    assert (
        collector.build_probability_snapshot(
            current_market=market,
            state=state,
            now_ms=1_783_459_216_100,
            stale_ms=15_000,
        )
        is None
    )


def test_outcome_freshness_keeps_old_ask_age_after_bid_only_update():
    state = collector.ProbabilityState(
        up_token_id="up-token",
        down_token_id="down-token",
    )
    assert state.update_token(
        "up-token",
        bid=Decimal("0.47"),
        ask=Decimal("0.49"),
        replace=True,
        provider_event_ms=1_000,
        received_ms=1_100,
        event_type="book",
    )
    assert state.update_token(
        "up-token",
        bid=Decimal("0.48"),
        ask=None,
        replace=False,
        provider_event_ms=2_000,
        received_ms=2_100,
        event_type="price_change",
    )

    assert state.outcome_observation_ms("Up") == (1_000, 1_100)
    assert state.latest_received_ms == 2_100


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
        up_bid_provider_event_ms=1_783_459_200_500,
        up_bid_received_ms=1_783_459_200_500,
        up_ask_provider_event_ms=1_783_459_200_500,
        up_ask_received_ms=1_783_459_200_500,
        down_bid_provider_event_ms=1_783_459_200_500,
        down_bid_received_ms=1_783_459_200_500,
        down_ask_provider_event_ms=1_783_459_200_500,
        down_ask_received_ms=1_783_459_200_500,
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
    assert calls[0]["up_received_ms"] == 1_783_459_200_500
    assert calls[0]["down_received_ms"] == 1_783_459_200_500


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
        (
            settlement_reference,
            settlement_window_s,
            settlement_source_url,
            settlement_rule_version,
        ) = collector.expected_twap_settlement_rule(window.market_start_ms)
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
            settlement_reference=settlement_reference,
            settlement_window_s=settlement_window_s,
            settlement_source_url=settlement_source_url,
            settlement_rule_version=settlement_rule_version,
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
        POLYMARKET_MARKET_BACKFILL_START_MS=collector.TWAP_60S_CUTOVER_MS,
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
