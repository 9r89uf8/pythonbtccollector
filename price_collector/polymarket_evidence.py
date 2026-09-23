"""Compact, causal Polymarket observations for a future study.

This module collects public information, never submits orders, and never runs a
model. Quote observations are explicitly sampled; they are not an event tape or
evidence of a fill. Neither this path nor its settings depend on raw_capture.
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Optional
from uuid import UUID, uuid4

import httpx
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from price_collector.config import TWAP_60S_CUTOVER_MS
from price_collector.market import market_for_sample_second
from price_collector.polymarket_evidence_store import (
    EvidenceRecord,
    EvidenceWriter,
    QuoteObservation,
)


LOGGER = logging.getLogger("price_collector.polymarket_evidence")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
PRICE_ENDPOINT = "https://polymarket.com/api/crypto/crypto-price"
COMPONENTS = ("up_bid", "up_ask", "down_bid", "down_ask")


class EvidenceSettings(BaseSettings):
    """Read only by the probability collector; no other service is opted in."""

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=True)

    POLYMARKET_EVIDENCE_ENABLED: bool = False
    POLYMARKET_EVIDENCE_POLL_SECONDS: float = Field(default=5.0, ge=1, le=60)
    POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS: int = Field(default=100, ge=50, le=1000)
    POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS: int = Field(default=120, ge=120, le=300)
    POLYMARKET_EVIDENCE_QUEUE_MAX_RECORDS: int = Field(default=5000, ge=20)
    POLYMARKET_EVIDENCE_QUOTE_QUEUE_MAX_RECORDS: int = Field(default=2000, ge=20)
    POLYMARKET_EVIDENCE_BATCH_MAX_ROWS: int = Field(default=250, ge=1)
    POLYMARKET_EVIDENCE_FLUSH_MS: int = Field(default=500, ge=50, le=5000)
    POLYMARKET_EVIDENCE_WARN_RELATION_MB: int = Field(default=4096, ge=1)
    POLYMARKET_EVIDENCE_MAX_RELATION_MB: int = Field(default=6144, ge=2)

    @model_validator(mode="after")
    def validate_limits(self) -> "EvidenceSettings":
        if self.POLYMARKET_EVIDENCE_WARN_RELATION_MB >= self.POLYMARKET_EVIDENCE_MAX_RELATION_MB:
            raise ValueError("evidence warning size must be below the quote pause size")
        if self.POLYMARKET_EVIDENCE_BATCH_MAX_ROWS > min(
            self.POLYMARKET_EVIDENCE_QUEUE_MAX_RECORDS,
            self.POLYMARKET_EVIDENCE_QUOTE_QUEUE_MAX_RECORDS,
        ):
            raise ValueError("evidence batch size must fit both queues")
        return self


def _iso_ms(value: int) -> str:
    return (EPOCH + timedelta(milliseconds=value)).isoformat().replace("+00:00", "Z")


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def exact_response_json(response: httpx.Response) -> Any:
    return json.loads(response.text, parse_float=Decimal, parse_constant=_reject_constant)


def _decimal(value: Any, field: str, *, zero_allowed: bool = False) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, (float, bool)) or not isinstance(value, (str, int, Decimal)):
        raise ValueError(f"{field} must be an exact decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} is not a decimal") from exc
    if not result.is_finite() or result < 0 or (not zero_allowed and result == 0):
        raise ValueError(f"{field} is outside its allowed range")
    return result


def _optional_int(value: Any, field: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if not isinstance(value, int) or value < 0 or value > 2**63 - 1:
        raise ValueError(f"{field} must be a nonnegative BIGINT")
    return value


def _optional_bool(value: Any, field: str) -> Optional[bool]:
    if value is not None and not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def validate_market_identity(market: Any) -> None:
    window = market.window
    if window != market_for_sample_second(window.market_start_ms):
        raise ValueError("invalid five-minute market window")
    expected_s = 60 if window.market_start_ms >= TWAP_60S_CUTOVER_MS else 30
    if (
        market.settlement_reference != "chainlink_twap"
        or market.settlement_window_s != expected_s
        or market.settlement_rule_version != f"btc-5m-twap-{expected_s}"
        or market.settlement_source_url
        != f"https://data.chain.link/streams/btc-usd-twap-{expected_s}s-streams"
    ):
        raise ValueError("unrecognized or contradictory settlement identity")
    if (
        not market.condition_id
        or not market.up_token_id
        or not market.down_token_id
        or market.up_token_id == market.down_token_id
        or market.start_ms != window.market_start_ms
        or market.end_ms != window.market_end_ms
    ):
        raise ValueError("incomplete market identity")


def price_request_params(market: Any) -> dict[str, str]:
    validate_market_identity(market)
    return {
        "symbol": "BTC",
        "eventStartTime": _iso_ms(market.window.market_start_ms),
        "variant": "fiveminute",
        "endDate": _iso_ms(market.window.market_end_ms),
        "twapEnabled": "true",
        "twapLookbackSeconds": str(market.settlement_window_s),
    }


def parse_price_observation(payload: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(payload, Mapping) or "openPrice" not in payload:
        raise ValueError("price response has no openPrice field")
    open_price = _decimal(payload.get("openPrice"), "openPrice")
    close_price = _decimal(payload.get("closePrice"), "closePrice")
    flags = {}
    for key in ("completed", "incomplete", "cached"):
        value = payload.get(key)
        if value is not None and not isinstance(value, bool):
            raise ValueError(f"{key} must be a boolean")
        flags[key] = value
    return ("ok" if open_price is not None else "missing"), {
        "price_to_beat": open_price,
        "close_price": close_price,
        # This is deliberately not provider_event_ms or a Chainlink source clock.
        "api_timestamp_ms": _optional_int(payload.get("timestamp"), "timestamp"),
        **flags,
    }


def parse_gamma_observation(
    payload: Any, market: Any, parse_market: Callable[..., Any]
) -> tuple[str, dict[str, Any]]:
    observed = parse_market(payload, window=market.window, slug=market.slug)
    validate_market_identity(observed)
    for field in (
        "condition_id", "up_token_id", "down_token_id", "settlement_reference",
        "settlement_window_s", "settlement_rule_version", "settlement_source_url",
    ):
        if getattr(observed, field) != getattr(market, field):
            raise ValueError(f"Gamma changed market identity: {field}")
    source = observed.raw_gamma["market"]
    fields = (
        "feesEnabled", "feeSchedule", "orderMinSize", "orderPriceMinTickSize",
        "secondsDelay", "acceptingOrders", "acceptingOrdersTimestamp",
        "active", "closed", "archived", "negRisk", "negRiskMarketID",
    )
    data = {key: source.get(key) for key in fields}
    for key in ("feesEnabled", "acceptingOrders", "active", "closed", "archived", "negRisk"):
        data[key] = _optional_bool(data[key], key)
    for key in ("orderMinSize", "orderPriceMinTickSize"):
        data[key] = _decimal(data[key], key)
    fee = data.get("feeSchedule")
    if fee is not None:
        if not isinstance(fee, Mapping):
            raise ValueError("feeSchedule must be an object")
        data["feeSchedule"] = dict(fee)
        for key in ("rate", "exponent", "rebateRate"):
            if key in fee:
                data["feeSchedule"][key] = _decimal(fee[key], key, zero_allowed=True)
        data["feeSchedule"]["takerOnly"] = _optional_bool(fee.get("takerOnly"), "takerOnly")
    event = observed.raw_gamma.get("event", {})
    event_metadata = event.get("eventMetadata") if isinstance(event, Mapping) else None
    if isinstance(event_metadata, Mapping):
        data["eventMetadata"] = {
            "priceToBeat": _decimal(event_metadata.get("priceToBeat"), "priceToBeat"),
            "finalPrice": _decimal(event_metadata.get("finalPrice"), "finalPrice"),
        }
    fee_complete = data["feesEnabled"] is False or (
        isinstance(data["feeSchedule"], Mapping)
        and all(data["feeSchedule"].get(key) is not None for key in ("rate", "exponent", "takerOnly"))
    )
    complete = fee_complete and all(data[key] is not None for key in (
        "feesEnabled", "orderMinSize", "orderPriceMinTickSize",
    ))
    return ("ok" if complete else "missing"), {"data": data}


def parse_clob_observation(payload: Any, market: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("c") != market.condition_id:
        raise ValueError("CLOB response condition does not match the requested market")
    tokens = payload.get("t")
    if not isinstance(tokens, list):
        raise ValueError("CLOB response has no token mapping")
    expected = {market.up_token_id: "up", market.down_token_id: "down"}
    observed = {
        item.get("t"): str(item.get("o", "")).lower()
        for item in tokens if isinstance(item, Mapping)
    }
    if observed != expected or len(tokens) != 2:
        raise ValueError("CLOB response tokens/outcomes do not match")
    fields = ("mos", "mts", "mbf", "tbf", "ao", "aot", "itode", "fd", "v", "oas")
    data = {key: payload.get(key) for key in fields}
    for key in ("mos", "mts"):
        data[key] = _decimal(data[key], key)
    fee = data.get("fd")
    if fee is not None:
        if not isinstance(fee, Mapping):
            raise ValueError("fd must be an object")
        data["fd"] = dict(fee)
        for key in ("r", "e"):
            data["fd"][key] = _decimal(fee.get(key), key, zero_allowed=True)
        data["fd"]["to"] = _optional_bool(fee.get("to"), "fd.to")
    for key in ("ao", "itode"):
        if data[key] is not None and not isinstance(data[key], bool):
            raise ValueError(f"{key} must be a boolean")
    # Preserve absence instead of silently converting it into a measured zero delay.
    data["itode_present"] = "itode" in payload
    complete = (
        data["mos"] is not None and data["mts"] is not None
        and data["ao"] is not None and isinstance(data["fd"], Mapping)
        and data["fd"].get("r") is not None and data["fd"].get("e") is not None
        and data["fd"].get("to") is not None
    )
    return ("ok" if complete else "missing"), {"data": data}


def parse_clob_order_rules(payload: Any, market: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("condition_id") != market.condition_id:
        raise ValueError("CLOB order rules condition does not match")
    tokens = payload.get("tokens")
    if not isinstance(tokens, list) or len(tokens) != 2:
        raise ValueError("CLOB order rules have no exact token mapping")
    observed = {item.get("token_id"): str(item.get("outcome", "")).lower()
                for item in tokens if isinstance(item, Mapping)}
    if observed != {market.up_token_id: "up", market.down_token_id: "down"}:
        raise ValueError("CLOB order rules tokens/outcomes do not match")
    data = {key: payload.get(key) for key in (
        "seconds_delay", "minimum_order_size", "minimum_tick_size", "maker_base_fee",
        "taker_base_fee", "accepting_orders", "active", "closed", "enable_order_book", "neg_risk",
    )}
    data["seconds_delay"] = _optional_int(data["seconds_delay"], "seconds_delay")
    for key in ("minimum_order_size", "minimum_tick_size"):
        data[key] = _decimal(data[key], key)
    for key in ("accepting_orders", "active", "closed", "enable_order_book", "neg_risk"):
        data[key] = _optional_bool(data[key], key)
    complete = all(data[key] is not None for key in (
        "seconds_delay", "minimum_order_size", "minimum_tick_size", "accepting_orders",
    ))
    # This older seconds_delay value is retained separately from compact itode.
    return ("ok" if complete else "missing"), {"data": data}


class CollectionEvidenceRuntime:
    def __init__(
        self, *, pool: Any, settings: EvidenceSettings, collector_settings: Any,
        parse_market: Callable[..., Any], writer: Optional[Any] = None,
        client: Optional[httpx.AsyncClient] = None,
        wall_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self.settings = settings
        self.collector_settings = collector_settings
        self.parse_market = parse_market
        self.wall_ns = wall_ns
        self.monotonic_ns = monotonic_ns
        self.writer = writer if writer is not None else EvidenceWriter(
            pool,
            queue_max=settings.POLYMARKET_EVIDENCE_QUEUE_MAX_RECORDS,
            quote_queue_max=settings.POLYMARKET_EVIDENCE_QUOTE_QUEUE_MAX_RECORDS,
            batch_size=settings.POLYMARKET_EVIDENCE_BATCH_MAX_ROWS,
            flush_seconds=settings.POLYMARKET_EVIDENCE_FLUSH_MS / 1000,
            max_relation_mb=settings.POLYMARKET_EVIDENCE_MAX_RELATION_MB,
            warn_relation_mb=settings.POLYMARKET_EVIDENCE_WARN_RELATION_MB,
        )
        self.client = client
        self._owns_client = client is None
        self._market_tasks: dict[int, asyncio.Task] = {}
        self._sessions: dict[UUID, QuoteCapture] = {}
        self._closing = False

    async def start(self) -> None:
        await self.writer.start()
        if self.client is None:
            self.client = httpx.AsyncClient(
                timeout=5.0,
                limits=httpx.Limits(max_connections=6, max_keepalive_connections=6),
                follow_redirects=False,
            )
        LOGGER.info("polymarket_evidence_started", extra={
            "event": "polymarket_evidence_started",
            "quote_interval_ms": self.settings.POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS,
            "quote_window_seconds": self.settings.POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS,
            "poll_seconds": self.settings.POLYMARKET_EVIDENCE_POLL_SECONDS,
        })

    def register_market(self, market: Any) -> None:
        if self._closing or market.window.market_id in self._market_tasks:
            return
        try:
            validate_market_identity(market)
        except ValueError as exc:
            LOGGER.error("polymarket_evidence_market_rejected", extra={
                "event": "polymarket_evidence_market_rejected",
                "market_id": market.window.market_id, "error": str(exc),
            })
            self._record(market, "gap", {"reason": "invalid_market_identity", "error": str(exc)})
            return
        now_ms = self.wall_ns() // 1_000_000
        if now_ms >= market.window.market_end_ms + 15_000:
            return
        for key, task in list(self._market_tasks.items()):
            if task.done():
                # The task reports its own exceptions; pruning bounds runtime memory.
                self._market_tasks.pop(key)
        self._market_tasks[market.window.market_id] = asyncio.create_task(
            self._metadata_loop(market)
        )

    def _record(self, market: Any, kind: str, payload: Mapping[str, Any], **kwargs: Any) -> bool:
        received_wall_ns = kwargs.pop("received_wall_ns", None)
        received_monotonic_ns = kwargs.pop("received_monotonic_ns", None)
        return self.writer.offer(EvidenceRecord(
            record_id=uuid4(), market_id=market.window.market_id, kind=kind,
            received_wall_ns=self.wall_ns() if received_wall_ns is None else received_wall_ns,
            received_monotonic_ns=self.monotonic_ns() if received_monotonic_ns is None else received_monotonic_ns,
            payload=payload, **kwargs,
        ))

    async def observe_http(
        self, market: Any, kind: str, url: str, parser: Callable[[Any], tuple[str, dict[str, Any]]],
        *, params: Optional[Mapping[str, str]] = None,
    ) -> str:
        requested_wall = self.wall_ns()
        requested_monotonic = self.monotonic_ns()
        response = None
        response_sha256 = None
        response_date = None
        response_age_seconds = None
        parsed: dict[str, Any] = {}
        provenance: dict[str, Any] = {"source_url": url, "request_params": dict(params or {})}
        try:
            assert self.client is not None
            response = await self.client.get(url, params=params)
            received_wall = self.wall_ns()
            received_monotonic = self.monotonic_ns()
            response_sha256 = hashlib.sha256(response.content).hexdigest()
            response_date = response.headers.get("date")
            try:
                response_age_seconds = _optional_int(response.headers.get("age"), "Age")
            except ValueError:
                # Keep malformed cache metadata in the selected payload for audit,
                # without mistaking it for a measured source-price freshness clock.
                provenance["invalid_response_age"] = response.headers.get("age")
            if response.status_code != 200:
                status = "http_error"
            else:
                try:
                    status, parsed = parser(exact_response_json(response))
                    if (
                        kind == "price_to_beat" and parsed.get("completed") is True
                        and received_wall // 1_000_000 < market.window.market_end_ms
                    ):
                        raise ValueError("price endpoint reports completion before market close")
                except (ValueError, TypeError, KeyError, InvalidOperation) as exc:
                    status = "invalid"
                    parsed = {"error": str(exc)[:240]}
        except asyncio.CancelledError:
            raise
        except (httpx.HTTPError, OSError) as exc:
            received_wall = self.wall_ns()
            received_monotonic = self.monotonic_ns()
            status = "transport_error"
            parsed = {"error_type": type(exc).__name__}
        # Neither clock is inferred from the requested window or a source cache timestamp.
        self._record(
            market, kind, {**provenance, **parsed}, status=status,
            received_wall_ns=received_wall, received_monotonic_ns=received_monotonic,
            requested_wall_ns=requested_wall, requested_monotonic_ns=requested_monotonic,
            http_status=response.status_code if response is not None else None,
            response_sha256=response_sha256, response_date=response_date,
            response_age_seconds=response_age_seconds,
        )
        return status

    async def poll_market_once(self, market: Any) -> None:
        gamma_url = self.collector_settings.POLYMARKET_GAMMA_BASE_URL.rstrip("/")
        clob_url = self.collector_settings.POLYMARKET_CLOB_BASE_URL.rstrip("/")
        gamma_status, _, _ = await asyncio.gather(
            self.observe_http(
                market, "gamma_market", f"{gamma_url}/events/slug/{market.slug}",
                lambda payload: parse_gamma_observation(payload, market, self.parse_market),
            ),
            self.observe_http(
                market, "clob_market", f"{clob_url}/clob-markets/{market.condition_id}",
                lambda payload: parse_clob_observation(payload, market),
            ),
            self.observe_http(
                market, "clob_order_rules", f"{clob_url}/markets/{market.condition_id}",
                lambda payload: parse_clob_order_rules(payload, market),
            ),
        )
        # Require a current successful identity check before accepting the website's
        # window-keyed response, which does not itself echo the condition/token IDs.
        if gamma_status in ("ok", "missing"):
            await self.observe_http(
                market, "price_to_beat", PRICE_ENDPOINT, parse_price_observation,
                params=price_request_params(market),
            )
        else:
            self._record(market, "price_to_beat", {
                "source_url": PRICE_ENDPOINT, "request_params": price_request_params(market),
                "reason": "not_requested_without_current_gamma_identity",
                "gamma_status": gamma_status,
            }, status="invalid")

    async def _metadata_loop(self, market: Any) -> None:
        while not self._closing and self.wall_ns() // 1_000_000 < market.window.market_end_ms + 15_000:
            try:
                await self.poll_market_once(market)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.exception("polymarket_evidence_poll_failed", extra={
                    "event": "polymarket_evidence_poll_failed", "market_id": market.window.market_id,
                })
                self._record(market, "gap", {"reason": "metadata_poll_failed", "error_type": type(exc).__name__})
            await asyncio.sleep(self.settings.POLYMARKET_EVIDENCE_POLL_SECONDS)

    def start_session(
        self, market: Any, state: Any, *, connected_wall_ns: int,
        connected_monotonic_ns: int, subscribed_wall_ns: int, subscribed_monotonic_ns: int,
    ) -> "QuoteCapture":
        capture = QuoteCapture(
            runtime=self, market=market, state=state,
            connected_wall_ns=connected_wall_ns, connected_monotonic_ns=connected_monotonic_ns,
            subscribed_wall_ns=subscribed_wall_ns, subscribed_monotonic_ns=subscribed_monotonic_ns,
        )
        self._sessions[capture.connection_id] = capture
        capture.task = asyncio.create_task(capture.run())
        return capture

    async def close(self) -> None:
        self._closing = True
        for task in self._market_tasks.values():
            task.cancel()
        for task in self._market_tasks.values():
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for capture in list(self._sessions.values()):
            await capture.close("shutdown")
        await self.writer.close(timeout_seconds=10)
        if self._owns_client and self.client is not None:
            await self.client.aclose()


class QuoteCapture:
    def __init__(
        self, *, runtime: CollectionEvidenceRuntime, market: Any, state: Any,
        connected_wall_ns: int, connected_monotonic_ns: int,
        subscribed_wall_ns: int, subscribed_monotonic_ns: int,
    ) -> None:
        self.runtime = runtime
        self.market = market
        self.state = state
        self.connection_id = uuid4()
        self.receive_sequence = 0
        self.last_accepted_wall_ns: Optional[int] = None
        self.last_accepted_monotonic_ns: Optional[int] = None
        self.last_accepted_sequence = 0
        self.task: Optional[asyncio.Task] = None
        self.closed = False
        self.invalid_sample_since_ns: Optional[int] = None
        runtime._record(market, "session_start", {
            "source": "polymarket_clob", "connected_wall_ns": connected_wall_ns,
            "connected_monotonic_ns": connected_monotonic_ns,
            "subscribed_wall_ns": subscribed_wall_ns,
            "subscribed_monotonic_ns": subscribed_monotonic_ns,
            "quote_interval_ms": runtime.settings.POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS,
            "quote_window_seconds": runtime.settings.POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS,
        }, connection_id=self.connection_id, received_wall_ns=subscribed_wall_ns,
            received_monotonic_ns=subscribed_monotonic_ns)

    def frame_received(self) -> int:
        self.receive_sequence += 1
        return self.receive_sequence

    def accepted(self, wall_ns: int, monotonic_ns: int) -> None:
        self.last_accepted_sequence = self.receive_sequence
        self.last_accepted_wall_ns = wall_ns
        self.last_accepted_monotonic_ns = monotonic_ns

    def parse_error(self, wall_ns: int, monotonic_ns: int, error: Exception) -> None:
        self.runtime._record(self.market, "gap", {
            "source": "quotes", "reason": "parse_error", "receive_sequence": self.receive_sequence,
            "error_type": type(error).__name__,
        }, connection_id=self.connection_id, received_wall_ns=wall_ns,
            received_monotonic_ns=monotonic_ns)

    def tick_size_change(
        self, message: Mapping[str, Any], wall_ns: int, monotonic_ns: int,
        provider_event_ms: Optional[int],
    ) -> None:
        if message.get("event_type", message.get("type")) != "tick_size_change":
            return
        token = message.get("asset_id", message.get("assetId"))
        if token not in (self.market.up_token_id, self.market.down_token_id):
            return
        status = "ok"
        try:
            provider_event_ms = _optional_int(provider_event_ms, "provider_event_ms")
            if message.get("market") not in (None, self.market.condition_id):
                raise ValueError("tick change condition does not match")
            new_tick = _decimal(message.get("new_tick_size"), "new_tick_size")
            old_tick = _decimal(message.get("old_tick_size"), "old_tick_size")
            if new_tick is None or new_tick > 1 or (old_tick is not None and old_tick > 1):
                raise ValueError("tick change has invalid increment")
            payload = {"outcome": "Up" if token == self.market.up_token_id else "Down",
                       "old_tick_size": old_tick, "new_tick_size": new_tick}
        except ValueError as exc:
            status = "invalid"
            payload = {"error": str(exc), "reported_provider_event_ms": provider_event_ms}
            provider_event_ms = None
        self.runtime._record(self.market, "tick_size", payload, status=status,
            connection_id=self.connection_id, provider_event_ms=provider_event_ms,
            received_wall_ns=wall_ns, received_monotonic_ns=monotonic_ns)

    def sample_once(self, observed_wall_ns: int, observed_monotonic_ns: int) -> bool:
        start_ms = self.market.window.market_end_ms - self.runtime.settings.POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS * 1000
        if not start_ms * 1_000_000 <= observed_wall_ns < self.market.window.market_end_ms * 1_000_000:
            return False
        fields = {name: getattr(self.state, name) for name in COMPONENTS}
        for name in COMPONENTS:
            for clock in ("provider_event_ms", "received_ms"):
                fields[f"{name}_{clock}"] = getattr(self.state, f"{name}_{clock}")
        return self.runtime.writer.offer_quote(QuoteObservation(
            connection_id=self.connection_id, market_id=self.market.window.market_id,
            observed_wall_ns=observed_wall_ns, observed_monotonic_ns=observed_monotonic_ns,
            receive_sequence=self.last_accepted_sequence,
            received_wall_ns=self.last_accepted_wall_ns,
            received_monotonic_ns=self.last_accepted_monotonic_ns,
            event_type=self.state.latest_event_type, resolved=self.state.resolved, **fields,
        ))

    async def run(self) -> None:
        interval_ns = self.runtime.settings.POLYMARKET_EVIDENCE_QUOTE_INTERVAL_MS * 1_000_000
        start_ns = (self.market.window.market_end_ms - self.runtime.settings.POLYMARKET_EVIDENCE_QUOTE_WINDOW_SECONDS * 1000) * 1_000_000
        end_ns = self.market.window.market_end_ms * 1_000_000
        try:
            while not self.closed:
                observed_wall = self.runtime.wall_ns()
                if observed_wall >= end_ns:
                    return
                if observed_wall < start_ns:
                    await asyncio.sleep(min(1.0, (start_ns - observed_wall) / 1_000_000_000))
                    continue
                # No await between taking the observation clock and copying state.
                try:
                    self.sample_once(observed_wall, self.runtime.monotonic_ns())
                except (ValueError, TypeError) as exc:
                    if self.invalid_sample_since_ns is None:
                        self.invalid_sample_since_ns = observed_wall
                        self.runtime._record(self.market, "gap", {
                            "source": "quotes", "reason": "invalid_quote_state",
                            "first_invalid_observed_wall_ns": observed_wall,
                            "error": str(exc)[:240],
                        }, connection_id=self.connection_id)
                else:
                    if self.invalid_sample_since_ns is not None:
                        self.runtime._record(self.market, "gap", {
                            "source": "quotes", "reason": "invalid_quote_state_recovered",
                            "first_invalid_observed_wall_ns": self.invalid_sample_since_ns,
                            "recovered_observed_wall_ns": observed_wall,
                        }, connection_id=self.connection_id)
                        self.invalid_sample_since_ns = None
                now = self.runtime.wall_ns()
                await asyncio.sleep(max(0.001, (interval_ns - now % interval_ns) / 1_000_000_000))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("polymarket_evidence_quote_sampler_failed", extra={
                "event": "polymarket_evidence_quote_sampler_failed", "market_id": self.market.window.market_id,
            })
            self.runtime._record(self.market, "gap", {
                "source": "quotes", "reason": "sampler_failed", "error_type": type(exc).__name__,
            }, connection_id=self.connection_id)

    async def close(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
        closed_wall = self.runtime.wall_ns()
        closed_monotonic = self.runtime.monotonic_ns()
        if closed_wall < self.market.window.market_end_ms * 1_000_000:
            self.runtime._record(self.market, "gap", {
                "source": "quotes", "reason": "connection_ended_before_market_close",
                "close_reason": reason, "last_receive_sequence": self.receive_sequence,
                "last_accepted_received_wall_ns": self.last_accepted_wall_ns,
                "disconnect_detected_wall_ns": closed_wall,
                "gap_end_unknown": True,
            }, connection_id=self.connection_id, received_wall_ns=closed_wall,
                received_monotonic_ns=closed_monotonic)
        self.runtime._record(self.market, "session_end", {
            "source": "polymarket_clob", "reason": reason,
            "last_receive_sequence": self.receive_sequence,
            "last_accepted_received_wall_ns": self.last_accepted_wall_ns,
            "last_accepted_received_monotonic_ns": self.last_accepted_monotonic_ns,
        }, connection_id=self.connection_id, received_wall_ns=closed_wall,
            received_monotonic_ns=closed_monotonic)
        self.runtime._sessions.pop(self.connection_id, None)
