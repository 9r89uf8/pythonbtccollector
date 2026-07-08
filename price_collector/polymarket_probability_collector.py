import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional

import httpx
import websockets

from price_collector.collector import (
    current_utc_epoch_ms,
    reconnect_delay_seconds,
    require_collector_database_url,
    sample_second_ms_for_now,
    seconds_until_next_utc_second,
    setup_logging,
)
from price_collector.config import Settings
from price_collector.db import (
    create_pool,
    upsert_polymarket_btc_5m_market,
    upsert_polymarket_probability_sample,
)
from price_collector.market import MarketWindow, market_for_sample_second


LOGGER = logging.getLogger("price_collector.polymarket_probability_collector")


class GammaDiscoveryError(ValueError):
    pass


class ClobMessageParseError(ValueError):
    pass


@dataclass(frozen=True)
class CurrentPolymarketMarket:
    window: MarketWindow
    slug: str
    gamma_event_id: Optional[str]
    gamma_market_id: Optional[str]
    condition_id: Optional[str]
    question: Optional[str]
    start_ms: Optional[int]
    end_ms: Optional[int]
    up_token_id: str
    down_token_id: str
    up_outcome: str
    down_outcome: str
    active: Optional[bool]
    closed: Optional[bool]
    archived: Optional[bool]
    raw_gamma: Mapping[str, Any]


@dataclass
class ProbabilityState:
    up_token_id: str
    down_token_id: str
    up_bid: Optional[Decimal] = None
    up_ask: Optional[Decimal] = None
    down_bid: Optional[Decimal] = None
    down_ask: Optional[Decimal] = None
    latest_provider_event_ms: Optional[int] = None
    latest_received_ms: Optional[int] = None
    latest_event_type: Optional[str] = None
    resolved: bool = False

    def update_token(
        self,
        asset_id: str,
        *,
        bid: Optional[Decimal],
        ask: Optional[Decimal],
        replace: bool,
        provider_event_ms: Optional[int],
        received_ms: int,
        event_type: str,
    ) -> bool:
        if asset_id == self.up_token_id:
            if replace or bid is not None:
                self.up_bid = bid
            if replace or ask is not None:
                self.up_ask = ask
        elif asset_id == self.down_token_id:
            if replace or bid is not None:
                self.down_bid = bid
            if replace or ask is not None:
                self.down_ask = ask
        else:
            return False

        self._mark_seen(
            provider_event_ms=provider_event_ms,
            received_ms=received_ms,
            event_type=event_type,
        )
        return True

    def mark_resolved(
        self,
        *,
        provider_event_ms: Optional[int],
        received_ms: int,
        event_type: str,
    ) -> None:
        self.resolved = True
        self._mark_seen(
            provider_event_ms=provider_event_ms,
            received_ms=received_ms,
            event_type=event_type,
        )

    def _mark_seen(
        self,
        *,
        provider_event_ms: Optional[int],
        received_ms: int,
        event_type: str,
    ) -> None:
        if provider_event_ms is not None:
            if (
                self.latest_provider_event_ms is None
                or provider_event_ms > self.latest_provider_event_ms
            ):
                self.latest_provider_event_ms = provider_event_ms
        self.latest_received_ms = received_ms
        self.latest_event_type = event_type

    def raw_snapshot(self) -> dict[str, Any]:
        return {
            "event_type": self.latest_event_type,
            "resolved": self.resolved,
            "up_token_id": self.up_token_id,
            "down_token_id": self.down_token_id,
            "up_bid": self.up_bid,
            "up_ask": self.up_ask,
            "down_bid": self.down_bid,
            "down_ask": self.down_ask,
        }


@dataclass(frozen=True)
class ProbabilitySnapshot:
    sample_second_ms: int
    window: MarketWindow
    up_bid: Optional[Decimal]
    up_ask: Optional[Decimal]
    up_mid: Optional[Decimal]
    down_bid: Optional[Decimal]
    down_ask: Optional[Decimal]
    down_mid: Optional[Decimal]
    up_prob_norm: Optional[Decimal]
    down_prob_norm: Optional[Decimal]
    provider_event_ms: Optional[int]
    received_ms: int
    raw: Mapping[str, Any]


def slug_for_window(window: MarketWindow, prefix: str) -> str:
    return f"{prefix}-{window.market_start_ms // 1000}"


def parse_jsonish(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return [
                item.strip().strip('"').strip("'")
                for item in stripped.strip("[]").split(",")
                if item.strip()
            ]
    return value


def _first_value(data: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = data.get(name)
        if value is not None:
            return value
    return None


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _string_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _bool_or_none(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    return None


def _parse_epoch_or_iso_ms(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value * 1000 if value < 100_000_000_000 else value

    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        int_value = int(value)
        return int_value * 1000 if int_value < 100_000_000_000 else int_value

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdecimal():
            int_value = int(stripped)
            return int_value * 1000 if int_value < 100_000_000_000 else int_value
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.astimezone(timezone.utc).timestamp() * 1000)

    return None


def _parse_first_time_ms(
    market: Mapping[str, Any],
    event: Mapping[str, Any],
    *names: str,
) -> Optional[int]:
    value = _first_value(market, *names)
    if value is None:
        value = _first_value(event, *names)
    return _parse_epoch_or_iso_ms(value)


def _outcome_label(value: Any) -> str:
    if isinstance(value, Mapping):
        for field in ("name", "label", "outcome", "title"):
            label = value.get(field)
            if label is not None:
                return str(label)
    return str(value)


def _token_id(value: Any) -> str:
    if isinstance(value, Mapping):
        for field in ("token_id", "tokenId", "id", "asset_id", "assetId"):
            token = value.get(field)
            if token is not None:
                return str(token)
    return str(value)


def extract_up_down_tokens(
    market: Mapping[str, Any],
) -> tuple[str, str, str, str]:
    outcomes = parse_jsonish(_first_value(market, "outcomes", "outcomePrices"))
    token_ids = parse_jsonish(_first_value(market, "clobTokenIds", "clob_token_ids"))

    if not token_ids:
        tokens = parse_jsonish(_first_value(market, "tokens"))
        if isinstance(tokens, list) and tokens:
            outcomes = [_outcome_label(token) for token in tokens]
            token_ids = [_token_id(token) for token in tokens]

    if not isinstance(outcomes, list) or not isinstance(token_ids, list):
        raise GammaDiscoveryError("Gamma market outcomes and CLOB token IDs must be arrays")

    if len(outcomes) != len(token_ids):
        raise GammaDiscoveryError("Gamma market outcomes and CLOB token IDs differ in length")

    up_token_id = None
    down_token_id = None
    up_outcome = "Up"
    down_outcome = "Down"

    for outcome, token_id in zip(outcomes, token_ids):
        label = _outcome_label(outcome)
        normalized = label.strip().lower()
        if normalized == "up":
            up_token_id = _token_id(token_id)
            up_outcome = label
        elif normalized == "down":
            down_token_id = _token_id(token_id)
            down_outcome = label

    if not up_token_id or not down_token_id:
        raise GammaDiscoveryError("Gamma market is missing Up or Down CLOB token IDs")

    return up_token_id, down_token_id, up_outcome, down_outcome


def _iter_event_markets(data: Any) -> Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    if isinstance(data, list):
        for market in data:
            if isinstance(market, Mapping):
                yield {}, market
        return

    if not isinstance(data, Mapping):
        return

    nested_data = data.get("data")
    if isinstance(nested_data, list):
        for market in nested_data:
            if isinstance(market, Mapping):
                yield {}, market

    markets = data.get("markets")
    if isinstance(markets, list):
        for market in markets:
            if isinstance(market, Mapping):
                yield data, market

    if _first_value(data, "clobTokenIds", "clob_token_ids", "tokens") is not None:
        yield {}, data


def parse_current_market_from_gamma(
    data: Any,
    *,
    window: MarketWindow,
    slug: str,
) -> CurrentPolymarketMarket:
    for event, market in _iter_event_markets(data):
        market_slug = _first_value(market, "slug")
        event_slug = _first_value(event, "slug")
        if market_slug is not None and market_slug != slug:
            continue
        if market_slug is None and event_slug is not None and event_slug != slug:
            continue

        try:
            up_token_id, down_token_id, up_outcome, down_outcome = extract_up_down_tokens(market)
        except GammaDiscoveryError:
            continue

        raw_gamma: Mapping[str, Any]
        if event:
            raw_gamma = {"event": event, "market": market}
        else:
            raw_gamma = {"market": market}

        return CurrentPolymarketMarket(
            window=window,
            slug=slug,
            gamma_event_id=_string_or_none(_first_value(event, "id", "event_id")),
            gamma_market_id=_string_or_none(_first_value(market, "id", "market_id")),
            condition_id=_string_or_none(
                _first_value(market, "conditionId", "condition_id", "conditionID")
            ),
            question=_string_or_none(
                _coalesce(
                    _first_value(market, "question", "title"),
                    _first_value(event, "question", "title"),
                )
            ),
            start_ms=_parse_first_time_ms(
                market,
                event,
                "startDate",
                "start_date",
                "startTime",
                "start_time",
                "start_ms",
            ),
            end_ms=_parse_first_time_ms(
                market,
                event,
                "endDate",
                "end_date",
                "endTime",
                "end_time",
                "end_ms",
            ),
            up_token_id=up_token_id,
            down_token_id=down_token_id,
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            active=_bool_or_none(
                _coalesce(_first_value(market, "active"), _first_value(event, "active"))
            ),
            closed=_bool_or_none(
                _coalesce(_first_value(market, "closed"), _first_value(event, "closed"))
            ),
            archived=_bool_or_none(
                _coalesce(_first_value(market, "archived"), _first_value(event, "archived"))
            ),
            raw_gamma=raw_gamma,
        )

    raise GammaDiscoveryError(f"no parseable BTC 5m Polymarket market found for slug={slug!r}")


async def discover_current_polymarket_market(
    settings: Settings,
    pool: Any,
    client: httpx.AsyncClient,
    window: MarketWindow,
) -> CurrentPolymarketMarket:
    slug = slug_for_window(window, settings.POLYMARKET_BTC_5M_SLUG_PREFIX)
    base_url = settings.POLYMARKET_GAMMA_BASE_URL.rstrip("/")
    seen_ms = current_utc_epoch_ms()

    response = await client.get(f"{base_url}/events/slug/{slug}")
    if response.status_code != 404:
        response.raise_for_status()
        try:
            current_market = parse_current_market_from_gamma(
                response.json(),
                window=window,
                slug=slug,
            )
        except GammaDiscoveryError:
            current_market = None
        else:
            await store_current_market(pool, current_market, seen_ms=seen_ms)
            return current_market

    response = await client.get(
        f"{base_url}/markets",
        params={"slug": slug, "active": "true", "closed": "false"},
    )
    response.raise_for_status()
    current_market = parse_current_market_from_gamma(
        response.json(),
        window=window,
        slug=slug,
    )
    await store_current_market(pool, current_market, seen_ms=seen_ms)
    return current_market


async def store_current_market(
    pool: Any,
    current_market: CurrentPolymarketMarket,
    *,
    seen_ms: int,
) -> None:
    await upsert_polymarket_btc_5m_market(
        pool,
        window=current_market.window,
        slug=current_market.slug,
        gamma_event_id=current_market.gamma_event_id,
        gamma_market_id=current_market.gamma_market_id,
        condition_id=current_market.condition_id,
        question=current_market.question,
        start_ms=current_market.start_ms,
        end_ms=current_market.end_ms,
        up_token_id=current_market.up_token_id,
        down_token_id=current_market.down_token_id,
        up_outcome=current_market.up_outcome,
        down_outcome=current_market.down_outcome,
        active=current_market.active,
        closed=current_market.closed,
        archived=current_market.archived,
        raw_gamma=current_market.raw_gamma,
        seen_ms=seen_ms,
    )


def build_clob_subscription(current_market: CurrentPolymarketMarket) -> dict[str, Any]:
    return {
        "type": "market",
        "assets_ids": [
            current_market.up_token_id,
            current_market.down_token_id,
        ],
        "custom_feature_enabled": True,
    }


def _probability_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ClobMessageParseError("probability price must be numeric")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ClobMessageParseError("probability price is invalid") from exc

    if not parsed.is_finite() or parsed < 0 or parsed > 1:
        raise ClobMessageParseError("probability price must be between 0 and 1")

    return parsed


def _best_bid(levels: Any) -> Optional[Decimal]:
    prices = []
    if not isinstance(levels, list):
        return None
    for level in levels:
        if not isinstance(level, Mapping):
            continue
        try:
            price = _probability_decimal(level.get("price"))
        except ClobMessageParseError:
            continue
        if price is not None:
            prices.append(price)
    return max(prices) if prices else None


def _best_ask(levels: Any) -> Optional[Decimal]:
    prices = []
    if not isinstance(levels, list):
        return None
    for level in levels:
        if not isinstance(level, Mapping):
            continue
        try:
            price = _probability_decimal(level.get("price"))
        except ClobMessageParseError:
            continue
        if price is not None:
            prices.append(price)
    return min(prices) if prices else None


def parse_clob_messages(raw_message: Any) -> list[Mapping[str, Any]]:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    if raw_message in ("PONG", "PING"):
        return []

    payload = json.loads(raw_message, parse_float=Decimal)
    if isinstance(payload, Mapping):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    return []


def _message_timestamp_ms(message: Mapping[str, Any]) -> Optional[int]:
    return _parse_epoch_or_iso_ms(
        _first_value(message, "timestamp", "event_timestamp", "created_at")
    )


def apply_clob_message(
    state: ProbabilityState,
    message: Mapping[str, Any],
    *,
    received_ms: int,
) -> bool:
    event_type = _first_value(message, "event_type", "type")
    if not isinstance(event_type, str):
        return False

    provider_event_ms = _message_timestamp_ms(message)

    if event_type == "book":
        asset_id = _string_or_none(_first_value(message, "asset_id", "assetId"))
        if asset_id is None:
            return False
        return state.update_token(
            asset_id,
            bid=_best_bid(message.get("bids")),
            ask=_best_ask(message.get("asks")),
            replace=True,
            provider_event_ms=provider_event_ms,
            received_ms=received_ms,
            event_type=event_type,
        )

    if event_type == "price_change":
        updated = False
        price_changes = message.get("price_changes")
        if not isinstance(price_changes, list):
            return False
        for change in price_changes:
            if not isinstance(change, Mapping):
                continue
            asset_id = _string_or_none(_first_value(change, "asset_id", "assetId"))
            if asset_id is None:
                continue
            change_event_ms = _message_timestamp_ms(change) or provider_event_ms
            updated = (
                state.update_token(
                    asset_id,
                    bid=_probability_decimal(_first_value(change, "best_bid", "bestBid")),
                    ask=_probability_decimal(_first_value(change, "best_ask", "bestAsk")),
                    replace=False,
                    provider_event_ms=change_event_ms,
                    received_ms=received_ms,
                    event_type=event_type,
                )
                or updated
            )
        return updated

    if event_type == "best_bid_ask":
        asset_id = _string_or_none(_first_value(message, "asset_id", "assetId"))
        if asset_id is None:
            return False
        return state.update_token(
            asset_id,
            bid=_probability_decimal(_first_value(message, "best_bid", "bestBid")),
            ask=_probability_decimal(_first_value(message, "best_ask", "bestAsk")),
            replace=True,
            provider_event_ms=provider_event_ms,
            received_ms=received_ms,
            event_type=event_type,
        )

    if event_type == "market_resolved":
        state.mark_resolved(
            provider_event_ms=provider_event_ms,
            received_ms=received_ms,
            event_type=event_type,
        )
        return True

    return False


def midpoint(bid: Optional[Decimal], ask: Optional[Decimal]) -> Optional[Decimal]:
    if bid is not None and ask is not None:
        return (bid + ask) / Decimal("2")
    if bid is not None:
        return bid
    if ask is not None:
        return ask
    return None


def normalized_probs(
    up_mid: Optional[Decimal],
    down_mid: Optional[Decimal],
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    if up_mid is None or down_mid is None:
        return None, None

    total = up_mid + down_mid
    if total <= 0:
        return None, None

    return up_mid / total, down_mid / total


def build_probability_snapshot(
    *,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
    now_ms: int,
    stale_ms: int,
) -> Optional[ProbabilitySnapshot]:
    sample_second_ms = sample_second_ms_for_now(now_ms)
    if sample_second_ms < current_market.window.market_start_ms:
        return None
    if sample_second_ms >= current_market.window.market_end_ms:
        return None
    if state.resolved:
        return None

    if state.latest_received_ms is None:
        return None
    if now_ms - state.latest_received_ms > stale_ms:
        return None

    up_mid = midpoint(state.up_bid, state.up_ask)
    down_mid = midpoint(state.down_bid, state.down_ask)
    if up_mid is None or down_mid is None:
        return None

    up_prob_norm, down_prob_norm = normalized_probs(up_mid, down_mid)

    return ProbabilitySnapshot(
        sample_second_ms=sample_second_ms,
        window=current_market.window,
        up_bid=state.up_bid,
        up_ask=state.up_ask,
        up_mid=up_mid,
        down_bid=state.down_bid,
        down_ask=state.down_ask,
        down_mid=down_mid,
        up_prob_norm=up_prob_norm,
        down_prob_norm=down_prob_norm,
        provider_event_ms=state.latest_provider_event_ms,
        received_ms=state.latest_received_ms,
        raw=state.raw_snapshot(),
    )


async def sample_probability_once(
    *,
    pool: Any,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
    source: str,
    stale_ms: int,
    now_ms: Optional[int] = None,
) -> bool:
    current_ms = current_utc_epoch_ms() if now_ms is None else now_ms
    snapshot = build_probability_snapshot(
        current_market=current_market,
        state=state,
        now_ms=current_ms,
        stale_ms=stale_ms,
    )
    if snapshot is None:
        LOGGER.debug(
            "polymarket_probability_sample_skipped",
            extra={"event": "polymarket_probability_sample_skipped", "now_ms": current_ms},
        )
        return False

    await upsert_polymarket_probability_sample(
        pool,
        window=snapshot.window,
        source=source,
        sample_second_ms=snapshot.sample_second_ms,
        up_token_id=current_market.up_token_id,
        down_token_id=current_market.down_token_id,
        up_bid=snapshot.up_bid,
        up_ask=snapshot.up_ask,
        up_mid=snapshot.up_mid,
        down_bid=snapshot.down_bid,
        down_ask=snapshot.down_ask,
        down_mid=snapshot.down_mid,
        up_prob_norm=snapshot.up_prob_norm,
        down_prob_norm=snapshot.down_prob_norm,
        provider_event_ms=snapshot.provider_event_ms,
        received_ms=snapshot.received_ms,
        raw=snapshot.raw,
    )

    LOGGER.info(
        "polymarket_probability_sample_written",
        extra={
            "event": "polymarket_probability_sample_written",
            "market_id": snapshot.window.market_id,
            "sample_second_ms": snapshot.sample_second_ms,
            "provider_event_ms": snapshot.provider_event_ms,
            "received_ms": snapshot.received_ms,
        },
    )
    return True


async def clob_ping_loop(websocket: Any, *, ping_seconds: int) -> None:
    while True:
        await asyncio.sleep(ping_seconds)
        await websocket.send("PING")


async def probability_sampler_loop(
    *,
    pool: Any,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
    source: str,
    stale_ms: int,
) -> None:
    last_sample_second_ms: Optional[int] = None

    while True:
        await asyncio.sleep(seconds_until_next_utc_second())
        now_ms = current_utc_epoch_ms()
        sample_second_ms = sample_second_ms_for_now(now_ms)
        if sample_second_ms >= current_market.window.market_end_ms:
            return
        if sample_second_ms == last_sample_second_ms:
            continue

        last_sample_second_ms = sample_second_ms
        await sample_probability_once(
            pool=pool,
            current_market=current_market,
            state=state,
            source=source,
            stale_ms=stale_ms,
            now_ms=now_ms,
        )


async def collect_current_market(
    *,
    settings: Settings,
    pool: Any,
    current_market: CurrentPolymarketMarket,
) -> None:
    state = ProbabilityState(
        up_token_id=current_market.up_token_id,
        down_token_id=current_market.down_token_id,
    )

    LOGGER.info(
        "polymarket_clob_connecting",
        extra={
            "event": "polymarket_clob_connecting",
            "url": settings.POLYMARKET_CLOB_WS_URL,
            "market_id": current_market.window.market_id,
            "slug": current_market.slug,
        },
    )
    async with websockets.connect(
        settings.POLYMARKET_CLOB_WS_URL,
        ping_interval=None,
        close_timeout=10,
    ) as websocket:
        await websocket.send(json.dumps(build_clob_subscription(current_market)))
        LOGGER.info(
            "polymarket_clob_subscribed",
            extra={
                "event": "polymarket_clob_subscribed",
                "market_id": current_market.window.market_id,
                "slug": current_market.slug,
                "up_token_id": current_market.up_token_id,
                "down_token_id": current_market.down_token_id,
            },
        )

        ping_task = asyncio.create_task(
            clob_ping_loop(
                websocket,
                ping_seconds=settings.POLYMARKET_CLOB_PING_SECONDS,
            )
        )
        sampler_task = asyncio.create_task(
            probability_sampler_loop(
                pool=pool,
                current_market=current_market,
                state=state,
                source=settings.POLYMARKET_PROBABILITY_SOURCE,
                stale_ms=settings.POLYMARKET_PROBABILITY_STALE_MS,
            )
        )

        try:
            while True:
                now_ms = current_utc_epoch_ms()
                if now_ms >= current_market.window.market_end_ms:
                    return
                if sampler_task.done():
                    await sampler_task
                    return

                seconds_until_end = max(
                    (current_market.window.market_end_ms - now_ms) / 1000,
                    0.001,
                )
                try:
                    raw_message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=min(30.0, seconds_until_end),
                    )
                except asyncio.TimeoutError:
                    continue

                received_ms = current_utc_epoch_ms()
                try:
                    messages = parse_clob_messages(raw_message)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    LOGGER.warning(
                        "polymarket_clob_message_skipped",
                        extra={
                            "event": "polymarket_clob_message_skipped",
                            "error": str(exc),
                        },
                    )
                    continue

                for message in messages:
                    try:
                        apply_clob_message(state, message, received_ms=received_ms)
                    except ClobMessageParseError as exc:
                        LOGGER.warning(
                            "polymarket_clob_message_skipped",
                            extra={
                                "event": "polymarket_clob_message_skipped",
                                "error": str(exc),
                            },
                        )
        finally:
            ping_task.cancel()
            sampler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ping_task
            with contextlib.suppress(asyncio.CancelledError):
                await sampler_task


async def run_collector(settings: Settings) -> None:
    setup_logging(settings.LOG_LEVEL)
    LOGGER.info(
        "polymarket_probability_collector_starting",
        extra={
            "event": "polymarket_probability_collector_starting",
            "app_env": settings.APP_ENV,
            "gamma_base_url": settings.POLYMARKET_GAMMA_BASE_URL,
            "clob_ws_url": settings.POLYMARKET_CLOB_WS_URL,
            "source": settings.POLYMARKET_PROBABILITY_SOURCE,
            "stale_ms": settings.POLYMARKET_PROBABILITY_STALE_MS,
        },
    )

    pool = await create_pool(require_collector_database_url(settings))
    try:
        attempt = 0
        async with httpx.AsyncClient(timeout=10.0) as client:
            while True:
                now_ms = current_utc_epoch_ms()
                window = market_for_sample_second(sample_second_ms_for_now(now_ms))
                try:
                    current_market = await discover_current_polymarket_market(
                        settings,
                        pool,
                        client,
                        window,
                    )
                    attempt = 0
                    LOGGER.info(
                        "polymarket_market_discovered",
                        extra={
                            "event": "polymarket_market_discovered",
                            "market_id": current_market.window.market_id,
                            "slug": current_market.slug,
                            "up_token_id": current_market.up_token_id,
                            "down_token_id": current_market.down_token_id,
                        },
                    )
                    await collect_current_market(
                        settings=settings,
                        pool=pool,
                        current_market=current_market,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    attempt += 1
                    delay = reconnect_delay_seconds(attempt)
                    LOGGER.warning(
                        "polymarket_probability_reconnect_scheduled",
                        extra={
                            "event": "polymarket_probability_reconnect_scheduled",
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                    )
                    await asyncio.sleep(delay)
    finally:
        await pool.close()


def main() -> None:
    settings = Settings()
    asyncio.run(run_collector(settings))


if __name__ == "__main__":
    main()
