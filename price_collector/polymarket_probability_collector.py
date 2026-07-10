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
    fetch_due_polymarket_resolutions,
    schedule_polymarket_resolution_retry,
    upsert_polymarket_btc_5m_market,
    upsert_polymarket_btc_5m_resolution,
    upsert_polymarket_probability_sample,
)
from price_collector.market import MarketWindow, market_for_sample_second


LOGGER = logging.getLogger("price_collector.polymarket_probability_collector")


class GammaDiscoveryError(ValueError):
    pass


class ClobMessageParseError(ValueError):
    pass


class ResolutionParseError(ValueError):
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


@dataclass(frozen=True)
class PolymarketResolution:
    status: str
    resolution_type: Optional[str]
    chainlink_open_price: Optional[Decimal]
    chainlink_close_price: Optional[Decimal]
    chainlink_source: Optional[str]
    winner: Optional[str]
    winning_token_id: Optional[str]
    up_payout: Optional[Decimal]
    down_payout: Optional[Decimal]
    resolved_at_ms: Optional[int]
    resolution_source: Optional[str]
    raw_resolution: Mapping[str, Any]

    @property
    def is_complete(self) -> bool:
        return (
            self.status == "resolved"
            and self.chainlink_open_price is not None
            and self.chainlink_close_price is not None
        )


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
    winning_outcome: Optional[str] = None
    winning_asset_id: Optional[str] = None
    resolution_event_ms: Optional[int] = None
    raw_resolution_event: Optional[Mapping[str, Any]] = None

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
        winning_outcome: Optional[str],
        winning_asset_id: Optional[str],
        raw_event: Mapping[str, Any],
    ) -> None:
        self.resolved = True
        self.winning_outcome = winning_outcome
        self.winning_asset_id = winning_asset_id
        self.resolution_event_ms = provider_event_ms
        self.raw_resolution_event = dict(raw_event)
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
            "winning_outcome": self.winning_outcome,
            "winning_asset_id": self.winning_asset_id,
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
        if (
            len(stripped) >= 3
            and stripped[-3] in {"+", "-"}
            and stripped[-2:].isdigit()
        ):
            stripped = f"{stripped}:00"
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


def _resolution_decimal(
    value: Any,
    *,
    minimum: Decimal,
    maximum: Optional[Decimal] = None,
) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ResolutionParseError("resolution value must be numeric")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ResolutionParseError("resolution value is invalid") from exc

    if not parsed.is_finite() or parsed < minimum:
        raise ResolutionParseError("resolution value is outside its valid range")
    if maximum is not None and parsed > maximum:
        raise ResolutionParseError("resolution value is outside its valid range")
    return parsed


def _find_gamma_resolution_market(
    data: Any,
    *,
    slug: str,
    gamma_market_id: Optional[str],
    condition_id: Optional[str],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    fallback: Optional[tuple[Mapping[str, Any], Mapping[str, Any]]] = None
    for event, market in _iter_event_markets(data):
        market_id = _string_or_none(_first_value(market, "id", "market_id"))
        market_condition_id = _string_or_none(
            _first_value(market, "conditionId", "condition_id", "conditionID")
        )
        market_slug = _string_or_none(_first_value(market, "slug"))
        event_slug = _string_or_none(_first_value(event, "slug"))

        has_stored_identity = gamma_market_id is not None or condition_id is not None
        if has_stored_identity:
            gamma_id_matches = (
                gamma_market_id is None or market_id == gamma_market_id
            )
            condition_matches = (
                condition_id is None or market_condition_id == condition_id
            )
            if gamma_id_matches and condition_matches:
                return event, market
        elif market_slug == slug or (market_slug is None and event_slug == slug):
            fallback = (event, market)

    if gamma_market_id is not None or condition_id is not None:
        raise ResolutionParseError(
            f"Gamma resolution identity mismatch for slug={slug!r}"
        )
    if fallback is not None:
        return fallback
    raise ResolutionParseError(f"Gamma resolution market not found for slug={slug!r}")


def _gamma_outcome_map(
    market: Mapping[str, Any],
    value_field: str,
) -> dict[str, Any]:
    outcomes = parse_jsonish(_first_value(market, "outcomes"))
    values = parse_jsonish(_first_value(market, value_field))
    if not isinstance(outcomes, list) or not isinstance(values, list):
        return {}
    if len(outcomes) != len(values):
        raise ResolutionParseError(
            f"Gamma outcomes and {value_field} differ in length"
        )

    mapped = {}
    for outcome, value in zip(outcomes, values):
        mapped[_outcome_label(outcome).strip().lower()] = value
    return mapped


def _parse_clob_resolution(
    data: Any,
    *,
    up_token_id: str,
    down_token_id: str,
) -> Optional[tuple[str, Optional[str], Optional[str], Decimal, Decimal]]:
    if not isinstance(data, Mapping) or _bool_or_none(data.get("closed")) is not True:
        return None

    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        return None

    winners = [
        token
        for token in tokens
        if isinstance(token, Mapping)
        and _bool_or_none(token.get("winner")) is True
    ]
    if len(winners) == 1:
        winner_token = winners[0]
        outcome = _outcome_label(winner_token.get("outcome")).strip().lower()
        token_id = _token_id(winner_token)
        if outcome == "up":
            if token_id != up_token_id:
                raise ResolutionParseError("CLOB Up winner token does not match Gamma")
            return "winner", "Up", token_id, Decimal("1"), Decimal("0")
        if outcome == "down":
            if token_id != down_token_id:
                raise ResolutionParseError("CLOB Down winner token does not match Gamma")
            return "winner", "Down", token_id, Decimal("0"), Decimal("1")
        raise ResolutionParseError("CLOB winner is not an Up or Down outcome")

    if _bool_or_none(data.get("is_50_50_outcome")) is True:
        return "split", None, None, Decimal("0.5"), Decimal("0.5")
    return None


def parse_polymarket_resolution(
    gamma_data: Any,
    *,
    slug: str,
    gamma_market_id: Optional[str],
    condition_id: Optional[str],
    up_token_id: str,
    down_token_id: str,
    clob_data: Any = None,
) -> PolymarketResolution:
    event, market = _find_gamma_resolution_market(
        gamma_data,
        slug=slug,
        gamma_market_id=gamma_market_id,
        condition_id=condition_id,
    )

    metadata = _first_value(event, "eventMetadata", "event_metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}
    chainlink_open_price = _resolution_decimal(
        _first_value(metadata, "priceToBeat", "price_to_beat"),
        minimum=Decimal("0.000000000000000001"),
    )
    chainlink_close_price = _resolution_decimal(
        _first_value(metadata, "finalPrice", "final_price"),
        minimum=Decimal("0.000000000000000001"),
    )
    chainlink_source = (
        "polymarket_gamma_event_metadata"
        if chainlink_open_price is not None or chainlink_close_price is not None
        else None
    )

    payout_values = _gamma_outcome_map(market, "outcomePrices")
    up_payout_candidate = _resolution_decimal(
        payout_values.get("up"),
        minimum=Decimal("0"),
        maximum=Decimal("1"),
    )
    down_payout_candidate = _resolution_decimal(
        payout_values.get("down"),
        minimum=Decimal("0"),
        maximum=Decimal("1"),
    )

    token_values = _gamma_outcome_map(market, "clobTokenIds")
    gamma_up_token = _string_or_none(token_values.get("up"))
    gamma_down_token = _string_or_none(token_values.get("down"))
    if gamma_up_token is not None and gamma_up_token != up_token_id:
        raise ResolutionParseError("Gamma Up token changed before resolution")
    if gamma_down_token is not None and gamma_down_token != down_token_id:
        raise ResolutionParseError("Gamma Down token changed before resolution")

    gamma_terminal = None
    gamma_status = _string_or_none(
        _first_value(market, "umaResolutionStatus", "uma_resolution_status")
    )
    if gamma_status is not None and gamma_status.strip().lower() == "resolved":
        if up_payout_candidate == 1 and down_payout_candidate == 0:
            gamma_terminal = (
                "winner",
                "Up",
                up_token_id,
                Decimal("1"),
                Decimal("0"),
            )
        elif up_payout_candidate == 0 and down_payout_candidate == 1:
            gamma_terminal = (
                "winner",
                "Down",
                down_token_id,
                Decimal("0"),
                Decimal("1"),
            )
        elif up_payout_candidate == Decimal("0.5") and down_payout_candidate == Decimal(
            "0.5"
        ):
            gamma_terminal = (
                "split",
                None,
                None,
                Decimal("0.5"),
                Decimal("0.5"),
            )

    clob_terminal = _parse_clob_resolution(
        clob_data,
        up_token_id=up_token_id,
        down_token_id=down_token_id,
    )
    if gamma_terminal is not None and clob_terminal is not None:
        if gamma_terminal != clob_terminal:
            raise ResolutionParseError("Gamma and CLOB official resolutions disagree")

    terminal = clob_terminal or gamma_terminal
    if terminal is None:
        status = "pending"
        resolution_type = None
        winner = None
        winning_token_id = None
        up_payout = None
        down_payout = None
        resolution_source = None
    else:
        status = "resolved"
        resolution_type, winner, winning_token_id, up_payout, down_payout = terminal
        resolution_source = (
            "polymarket_clob_rest"
            if clob_terminal is not None
            else "polymarket_gamma"
        )

    raw_resolution = {"gamma": gamma_data}
    if clob_data is not None:
        raw_resolution["clob"] = clob_data

    return PolymarketResolution(
        status=status,
        resolution_type=resolution_type,
        chainlink_open_price=chainlink_open_price,
        chainlink_close_price=chainlink_close_price,
        chainlink_source=chainlink_source,
        winner=winner,
        winning_token_id=winning_token_id,
        up_payout=up_payout,
        down_payout=down_payout,
        resolved_at_ms=(
            _parse_epoch_or_iso_ms(_first_value(market, "closedTime", "closed_time"))
            if terminal is not None
            else None
        ),
        resolution_source=resolution_source,
        raw_resolution=raw_resolution,
    )


async def fetch_polymarket_resolution(
    client: httpx.AsyncClient,
    settings: Settings,
    market: Mapping[str, Any],
) -> PolymarketResolution:
    gamma_base_url = settings.POLYMARKET_GAMMA_BASE_URL.rstrip("/")
    response = await client.get(f"{gamma_base_url}/events/slug/{market['slug']}")
    if response.status_code == 404 and market.get("gamma_market_id") is not None:
        response = await client.get(
            f"{gamma_base_url}/markets/{market['gamma_market_id']}"
        )
    response.raise_for_status()
    gamma_data = json.loads(response.text, parse_float=Decimal)

    clob_data = None
    condition_id = market.get("condition_id")
    if condition_id is not None:
        clob_base_url = settings.POLYMARKET_CLOB_BASE_URL.rstrip("/")
        try:
            clob_response = await client.get(f"{clob_base_url}/markets/{condition_id}")
            if clob_response.status_code != 404:
                clob_response.raise_for_status()
                clob_data = json.loads(clob_response.text, parse_float=Decimal)
        except (httpx.HTTPError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            LOGGER.warning(
                "polymarket_resolution_clob_check_failed",
                extra={
                    "event": "polymarket_resolution_clob_check_failed",
                    "market_id": market["market_id"],
                    "error": repr(exc),
                },
            )

    return parse_polymarket_resolution(
        gamma_data,
        slug=str(market["slug"]),
        gamma_market_id=market.get("gamma_market_id"),
        condition_id=condition_id,
        up_token_id=str(market["up_token_id"]),
        down_token_id=str(market["down_token_id"]),
        clob_data=clob_data,
    )


def resolution_retry_delay_ms(
    attempt: int,
    *,
    base_seconds: int,
    max_seconds: int,
) -> int:
    exponent = min(max(0, int(attempt) - 1), 16)
    delay_seconds = min(
        max(1, int(max_seconds)),
        max(1, int(base_seconds)) * (2**exponent),
    )
    return delay_seconds * 1000


async def persist_websocket_resolution(
    *,
    settings: Settings,
    pool: Any,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
    checked_ms: int,
) -> bool:
    if not state.resolved:
        return False
    if state.winning_outcome is None or state.winning_asset_id is None:
        return False

    normalized_outcome = state.winning_outcome.strip().lower()
    if normalized_outcome == "up":
        if state.winning_asset_id != current_market.up_token_id:
            raise ResolutionParseError("WebSocket Up winner token does not match Gamma")
        winner = "Up"
        up_payout = Decimal("1")
        down_payout = Decimal("0")
    elif normalized_outcome == "down":
        if state.winning_asset_id != current_market.down_token_id:
            raise ResolutionParseError("WebSocket Down winner token does not match Gamma")
        winner = "Down"
        up_payout = Decimal("0")
        down_payout = Decimal("1")
    else:
        raise ResolutionParseError("WebSocket winner is not an Up or Down outcome")

    await upsert_polymarket_btc_5m_resolution(
        pool,
        market_id=current_market.window.market_id,
        resolution_status="resolved",
        resolution_type="winner",
        chainlink_open_price=None,
        chainlink_close_price=None,
        chainlink_source=None,
        winner=winner,
        winning_token_id=state.winning_asset_id,
        up_payout=up_payout,
        down_payout=down_payout,
        resolved_at_ms=state.resolution_event_ms,
        resolution_source="polymarket_clob_ws",
        raw_resolution={"websocket": state.raw_resolution_event},
        checked_ms=checked_ms,
        next_check_ms=(
            checked_ms
            + max(1, int(settings.POLYMARKET_RESOLUTION_POLL_SECONDS)) * 1000
        ),
        resolution_attempts=1,
    )
    return True


async def reconcile_polymarket_resolution_once(
    *,
    settings: Settings,
    pool: Any,
    client: httpx.AsyncClient,
    market: Mapping[str, Any],
    now_ms: Optional[int] = None,
) -> bool:
    attempt = int(market.get("resolution_attempts") or 0) + 1
    retry_ms = resolution_retry_delay_ms(
        attempt,
        base_seconds=settings.POLYMARKET_RESOLUTION_POLL_SECONDS,
        max_seconds=settings.POLYMARKET_RESOLUTION_MAX_BACKOFF_SECONDS,
    )

    try:
        resolution = await fetch_polymarket_resolution(client, settings, market)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        checked_ms = current_utc_epoch_ms() if now_ms is None else now_ms
        await schedule_polymarket_resolution_retry(
            pool,
            market_id=int(market["market_id"]),
            checked_ms=checked_ms,
            next_check_ms=checked_ms + retry_ms,
            resolution_attempts=attempt,
        )
        LOGGER.warning(
            "polymarket_resolution_retry_scheduled",
            extra={
                "event": "polymarket_resolution_retry_scheduled",
                "market_id": market["market_id"],
                "attempt": attempt,
                "next_check_ms": checked_ms + retry_ms,
                "error": repr(exc),
            },
        )
        return False

    checked_ms = current_utc_epoch_ms() if now_ms is None else now_ms
    next_check_ms = None if resolution.is_complete else checked_ms + retry_ms
    await upsert_polymarket_btc_5m_resolution(
        pool,
        market_id=int(market["market_id"]),
        resolution_status=resolution.status,
        resolution_type=resolution.resolution_type,
        chainlink_open_price=resolution.chainlink_open_price,
        chainlink_close_price=resolution.chainlink_close_price,
        chainlink_source=resolution.chainlink_source,
        winner=resolution.winner,
        winning_token_id=resolution.winning_token_id,
        up_payout=resolution.up_payout,
        down_payout=resolution.down_payout,
        resolved_at_ms=resolution.resolved_at_ms,
        resolution_source=resolution.resolution_source,
        raw_resolution=resolution.raw_resolution,
        checked_ms=checked_ms,
        next_check_ms=next_check_ms,
        resolution_attempts=attempt,
    )
    LOGGER.info(
        "polymarket_resolution_checked",
        extra={
            "event": "polymarket_resolution_checked",
            "market_id": market["market_id"],
            "status": resolution.status,
            "winner": resolution.winner,
            "complete": resolution.is_complete,
            "next_check_ms": next_check_ms,
        },
    )
    return resolution.is_complete


async def _resolution_reconciler_session(settings: Settings, pool: Any) -> None:
    poll_seconds = max(1, int(settings.POLYMARKET_RESOLUTION_POLL_SECONDS))
    batch_size = max(1, int(settings.POLYMARKET_RESOLUTION_BATCH_SIZE))

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            now_ms = current_utc_epoch_ms()
            try:
                markets = await fetch_due_polymarket_resolutions(
                    pool,
                    now_ms=now_ms,
                    limit=batch_size,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning(
                    "polymarket_resolution_scan_failed",
                    extra={
                        "event": "polymarket_resolution_scan_failed",
                        "error": repr(exc),
                    },
                )
                await asyncio.sleep(poll_seconds)
                continue

            for market in markets:
                try:
                    await reconcile_polymarket_resolution_once(
                        settings=settings,
                        pool=pool,
                        client=client,
                        market=market,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.warning(
                        "polymarket_resolution_check_failed",
                        extra={
                            "event": "polymarket_resolution_check_failed",
                            "market_id": market["market_id"],
                            "error": repr(exc),
                        },
                    )

            await asyncio.sleep(poll_seconds)


async def resolution_reconciler_loop(settings: Settings, pool: Any) -> None:
    restart_seconds = max(1, int(settings.POLYMARKET_RESOLUTION_POLL_SECONDS))
    while True:
        try:
            await _resolution_reconciler_session(settings, pool)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.error(
                "polymarket_resolution_reconciler_restarting",
                extra={
                    "event": "polymarket_resolution_reconciler_restarting",
                    "delay_seconds": restart_seconds,
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(restart_seconds)


async def fetch_best_asks_from_clob_prices(
    client: httpx.AsyncClient,
    settings: Settings,
    current_market: CurrentPolymarketMarket,
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    base_url = settings.POLYMARKET_CLOB_BASE_URL.rstrip("/")
    request_body = [
        {
            "token_id": current_market.up_token_id,
            "side": "SELL",
        },
        {
            "token_id": current_market.down_token_id,
            "side": "SELL",
        },
    ]

    response = await client.post(
        f"{base_url}/prices",
        json=request_body,
    )
    response.raise_for_status()

    data = json.loads(response.text, parse_float=Decimal)
    if not isinstance(data, Mapping):
        return None, None

    def parse_price(token_id: str) -> Optional[Decimal]:
        token_data = data.get(token_id)
        if not isinstance(token_data, Mapping):
            return None

        raw_price = token_data.get("SELL")
        if raw_price is None:
            return None

        try:
            price = raw_price if isinstance(raw_price, Decimal) else Decimal(str(raw_price))
        except (InvalidOperation, ValueError):
            return None

        if not price.is_finite() or price < 0 or price > 1:
            return None

        return price

    return (
        parse_price(current_market.up_token_id),
        parse_price(current_market.down_token_id),
    )


async def prime_probability_state_from_rest(
    client: httpx.AsyncClient,
    settings: Settings,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
) -> bool:
    try:
        up_ask, down_ask = await fetch_best_asks_from_clob_prices(
            client,
            settings,
            current_market,
        )
    except Exception as exc:
        LOGGER.debug(
            "polymarket_probability_rest_prime_failed",
            extra={
                "event": "polymarket_probability_rest_prime_failed",
                "market_id": current_market.window.market_id,
                "error": repr(exc),
            },
        )
        return False

    received_ms = current_utc_epoch_ms()
    updated = False

    if up_ask is not None:
        updated = (
            state.update_token(
                current_market.up_token_id,
                bid=None,
                ask=up_ask,
                replace=False,
                provider_event_ms=received_ms,
                received_ms=received_ms,
                event_type="rest_prime_prices",
            )
            or updated
        )

    if down_ask is not None:
        updated = (
            state.update_token(
                current_market.down_token_id,
                bid=None,
                ask=down_ask,
                replace=False,
                provider_event_ms=received_ms,
                received_ms=received_ms,
                event_type="rest_prime_prices",
            )
            or updated
        )

    if updated:
        LOGGER.info(
            "polymarket_probability_rest_prime_updated",
            extra={
                "event": "polymarket_probability_rest_prime_updated",
                "market_id": current_market.window.market_id,
                "up_ask": str(up_ask) if up_ask is not None else None,
                "down_ask": str(down_ask) if down_ask is not None else None,
                "received_ms": received_ms,
            },
        )

    return updated


async def probability_rest_prime_loop(
    *,
    client: httpx.AsyncClient,
    settings: Settings,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
) -> None:
    while current_utc_epoch_ms() < current_market.window.market_start_ms:
        await asyncio.sleep(0.05)

    stop_ms = (
        current_market.window.market_start_ms
        + settings.POLYMARKET_REST_PRIME_SECONDS * 1000
    )

    while current_utc_epoch_ms() < stop_ms:
        await prime_probability_state_from_rest(
            client,
            settings,
            current_market,
            state,
        )
        await asyncio.sleep(seconds_until_next_utc_second())


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
            winning_outcome=_string_or_none(
                _first_value(message, "winning_outcome", "winningOutcome")
            ),
            winning_asset_id=_string_or_none(
                _first_value(
                    message,
                    "winning_asset_id",
                    "winningAssetId",
                    "winning_token_id",
                )
            ),
            raw_event=message,
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

    if state.up_ask is None or state.down_ask is None:
        return None

    up_mid = midpoint(state.up_bid, state.up_ask)
    down_mid = midpoint(state.down_bid, state.down_ask)
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
    client: httpx.AsyncClient,
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
        rest_prime_task = asyncio.create_task(
            probability_rest_prime_loop(
                client=client,
                settings=settings,
                current_market=current_market,
                state=state,
            )
        )

        try:
            resolution_deadline_ms = (
                current_market.window.market_end_ms
                + max(0, int(settings.POLYMARKET_RESOLUTION_WS_GRACE_SECONDS))
                * 1000
            )
            while True:
                now_ms = current_utc_epoch_ms()
                if now_ms >= resolution_deadline_ms:
                    return
                if sampler_task.done():
                    await sampler_task
                    if now_ms < current_market.window.market_end_ms:
                        return

                seconds_until_deadline = max(
                    (resolution_deadline_ms - now_ms) / 1000,
                    0.001,
                )
                try:
                    raw_message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=min(30.0, seconds_until_deadline),
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if (
                        current_utc_epoch_ms()
                        < current_market.window.market_end_ms
                    ):
                        raise
                    LOGGER.warning(
                        "polymarket_resolution_grace_socket_closed",
                        extra={
                            "event": "polymarket_resolution_grace_socket_closed",
                            "market_id": current_market.window.market_id,
                            "error": repr(exc),
                        },
                    )
                    return

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
                        continue

                    if state.resolved:
                        try:
                            await persist_websocket_resolution(
                                settings=settings,
                                pool=pool,
                                current_market=current_market,
                                state=state,
                                checked_ms=received_ms,
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            LOGGER.warning(
                                "polymarket_websocket_resolution_skipped",
                                extra={
                                    "event": "polymarket_websocket_resolution_skipped",
                                    "market_id": current_market.window.market_id,
                                    "error": str(exc),
                                },
                            )
                        return
        finally:
            ping_task.cancel()
            sampler_task.cancel()
            rest_prime_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ping_task
            with contextlib.suppress(asyncio.CancelledError):
                await sampler_task
            with contextlib.suppress(asyncio.CancelledError):
                await rest_prime_task


async def sleep_until_ms(target_ms: int) -> None:
    while True:
        now_ms = current_utc_epoch_ms()
        remaining_ms = target_ms - now_ms
        if remaining_ms <= 0:
            return
        await asyncio.sleep(min(remaining_ms / 1000, 1.0))


async def sleep_until_ms_or_task_done(target_ms: int, task: Any) -> None:
    if task.done():
        await task
        raise RuntimeError("Polymarket collection task ended before preload time")

    sleep_task = asyncio.create_task(sleep_until_ms(target_ms))
    try:
        done, _pending = await asyncio.wait(
            {sleep_task, task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            await task
            raise RuntimeError("Polymarket collection task ended before preload time")
    finally:
        if not sleep_task.done():
            sleep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sleep_task


async def cancel_and_drain_task(task: Any) -> None:
    task.cancel()
    with contextlib.suppress(Exception, asyncio.CancelledError):
        await task


async def discover_market_with_retries(
    *,
    settings: Settings,
    pool: Any,
    client: httpx.AsyncClient,
    window: MarketWindow,
    deadline_ms: int,
    retry_ms: int,
) -> CurrentPolymarketMarket:
    last_error: Optional[Exception] = None

    while current_utc_epoch_ms() < deadline_ms:
        try:
            return await discover_current_polymarket_market(
                settings,
                pool,
                client,
                window,
            )
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(retry_ms / 1000)

    if last_error is not None:
        raise last_error

    raise GammaDiscoveryError(
        f"could not discover Polymarket market for market_id={window.market_id}"
    )


async def start_market_collection(
    *,
    settings: Settings,
    pool: Any,
    client: httpx.AsyncClient,
    window: MarketWindow,
) -> tuple[CurrentPolymarketMarket, Any]:
    current_market = await discover_current_polymarket_market(
        settings,
        pool,
        client,
        window,
    )
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
    task = asyncio.create_task(
        collect_current_market(
            settings=settings,
            pool=pool,
            client=client,
            current_market=current_market,
        )
    )
    return current_market, task


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
            "resolution_poll_seconds": settings.POLYMARKET_RESOLUTION_POLL_SECONDS,
            "resolution_batch_size": settings.POLYMARKET_RESOLUTION_BATCH_SIZE,
            "resolution_ws_grace_seconds": (
                settings.POLYMARKET_RESOLUTION_WS_GRACE_SECONDS
            ),
        },
    )

    pool = await create_pool(require_collector_database_url(settings))
    current_task: Optional[asyncio.Task] = None
    next_task: Optional[asyncio.Task] = None
    resolution_task: Optional[asyncio.Task] = asyncio.create_task(
        resolution_reconciler_loop(settings, pool)
    )
    try:
        attempt = 0
        async with httpx.AsyncClient(timeout=10.0) as client:
            while current_task is None:
                now_ms = current_utc_epoch_ms()
                current_window = market_for_sample_second(sample_second_ms_for_now(now_ms))
                try:
                    current_market, current_task = await start_market_collection(
                        settings=settings,
                        pool=pool,
                        client=client,
                        window=current_window,
                    )
                    attempt = 0
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

            while True:
                try:
                    next_window = market_for_sample_second(current_market.window.market_end_ms)
                    preload_at_ms = (
                        current_market.window.market_end_ms
                        - settings.POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS * 1000
                    )

                    await sleep_until_ms_or_task_done(preload_at_ms, current_task)
                    next_market = await discover_market_with_retries(
                        settings=settings,
                        pool=pool,
                        client=client,
                        window=next_window,
                        deadline_ms=current_market.window.market_end_ms - 500,
                        retry_ms=settings.POLYMARKET_NEXT_MARKET_RETRY_MS,
                    )
                    LOGGER.info(
                        "polymarket_next_market_preloaded",
                        extra={
                            "event": "polymarket_next_market_preloaded",
                            "current_market_id": current_market.window.market_id,
                            "next_market_id": next_market.window.market_id,
                            "next_slug": next_market.slug,
                        },
                    )

                    next_task = asyncio.create_task(
                        collect_current_market(
                            settings=settings,
                            pool=pool,
                            client=client,
                            current_market=next_market,
                        )
                    )

                    try:
                        await current_task
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if (
                            current_utc_epoch_ms()
                            < current_market.window.market_end_ms
                        ):
                            raise
                        LOGGER.warning(
                            "polymarket_resolution_grace_ended_early",
                            extra={
                                "event": "polymarket_resolution_grace_ended_early",
                                "market_id": current_market.window.market_id,
                                "error": repr(exc),
                            },
                        )

                    current_market = next_market
                    current_task = next_task
                    next_task = None
                    attempt = 0

                except asyncio.CancelledError:
                    await cancel_and_drain_task(current_task)
                    if next_task is not None:
                        await cancel_and_drain_task(next_task)
                    raise

                except Exception as exc:
                    attempt += 1
                    delay = reconnect_delay_seconds(attempt)
                    LOGGER.warning(
                        "polymarket_probability_cycle_recovering",
                        extra={
                            "event": "polymarket_probability_cycle_recovering",
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                    )

                    await cancel_and_drain_task(current_task)
                    if next_task is not None:
                        await cancel_and_drain_task(next_task)
                    next_task = None

                    await asyncio.sleep(delay)

                    now_ms = current_utc_epoch_ms()
                    current_window = market_for_sample_second(sample_second_ms_for_now(now_ms))
                    current_market, current_task = await start_market_collection(
                        settings=settings,
                        pool=pool,
                        client=client,
                        window=current_window,
                    )
    finally:
        if resolution_task is not None:
            await cancel_and_drain_task(resolution_task)
        if current_task is not None:
            await cancel_and_drain_task(current_task)
        if next_task is not None:
            await cancel_and_drain_task(next_task)
        await pool.close()


def main() -> None:
    settings = Settings()
    asyncio.run(run_collector(settings))


if __name__ == "__main__":
    main()
