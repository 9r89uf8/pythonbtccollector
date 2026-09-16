"""Bounded saved history and the observed opening reference for the local UI.

This is a historical read, separate from the Redis-only live and ghost routes.
Prices retain their database precision; absent seconds are never filled here.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from typing import Any, Mapping

from price_collector.market import market_for_sample_second


HISTORY_MS = 15 * 60 * 1000
QUERY_TIMEOUT_SECONDS = 3.0
REQUEST_TIMEOUT_SECONDS = 4.0
PRICE_ENDPOINT = "https://polymarket.com/api/crypto/crypto-price"
TWAP_SOURCE = "https://data.chain.link/streams/btc-usd-twap-60s-streams"

# The primary key (instrument_id, sample_second_ms) bounds each price scan.
# The two constant instrument identities each yield at most 901 sample seconds.
HISTORY_SQL = """
SELECT i.symbol, history.price, history.provider_event_ms, history.received_ms
FROM instruments i
JOIN providers p USING (provider_id)
CROSS JOIN LATERAL (
    SELECT price, provider_event_ms, received_ms
    FROM price_samples
    WHERE instrument_id = i.instrument_id
      AND sample_second_ms >= $1 AND sample_second_ms <= $2
      AND provider_event_ms >= $1 AND provider_event_ms <= $3 AND received_ms <= $3
    ORDER BY sample_second_ms
    LIMIT 901
) history
WHERE (p.provider_code = 'polymarket_chainlink_rtds' AND i.symbol = 'BTCUSD')
   OR (p.provider_code = 'polymarket_chainlink_twap_rtds'
       AND i.symbol = 'BTCUSD_TWAP_60S')
ORDER BY i.symbol, history.provider_event_ms
"""

# market_id is the market primary key; the observation lookup uses the existing
# (market_id, received_wall_ns) index. Only this five-minute window is visited.
REFERENCE_SQL = """
SELECT m.settlement_reference, m.settlement_window_s, m.settlement_source_url,
       m.settlement_rule_version, m.condition_id, m.up_token_id, m.down_token_id,
       o.status, o.http_status, o.received_wall_ns, o.requested_wall_ns, p.payload
FROM polymarket_btc_5m_markets m
LEFT JOIN LATERAL (
    SELECT status, http_status, received_wall_ns, requested_wall_ns, payload_hash
    FROM polymarket_market_observations
    WHERE market_id = m.market_id AND kind = 'price_to_beat'
      AND received_wall_ns >= $2 AND received_wall_ns <= $3
    ORDER BY received_wall_ns DESC
    LIMIT 512
) o ON TRUE
LEFT JOIN polymarket_evidence_payloads p USING (payload_hash)
WHERE m.market_id = $1
ORDER BY o.received_wall_ns DESC NULLS LAST
"""


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value // 1000, timezone.utc).isoformat().replace("+00:00", "Z")


def _price(value: Any) -> str:
    # asyncpg returns NUMERIC as Decimal. JSON evidence prices are strings.
    if not isinstance(value, (str, Decimal)):
        raise ValueError("price must be an exact decimal")
    number = Decimal(value)
    if not number.is_finite() or number <= 0:
        raise ValueError("price must be positive and finite")
    return format(number, "f")


def opening_reference(rows: list[Mapping[str, Any]], window: Any, now_ms: int) -> dict[str, Any]:
    result = {
        "price_to_beat": None, "reference_status": "unavailable",
        "reference_reason": "opening_reference_not_observed",
        "reference_source": None, "reference_received_ms": None,
    }
    if not rows:
        result["reference_reason"] = "market_not_discovered"
        return result
    identity = rows[0]
    if (
        identity.get("settlement_reference") != "chainlink_twap"
        or identity.get("settlement_window_s") != 60
        or identity.get("settlement_source_url") != TWAP_SOURCE
        or identity.get("settlement_rule_version") != "btc-5m-twap-60"
        or not identity.get("condition_id") or not identity.get("up_token_id")
        or not identity.get("down_token_id")
        or identity.get("up_token_id") == identity.get("down_token_id")
    ):
        result["reference_reason"] = "market_identity_invalid"
        return result
    params = {
        "symbol": "BTC", "variant": "fiveminute", "twapEnabled": "true",
        "twapLookbackSeconds": "60", "eventStartTime": _iso(window.market_start_ms),
        "endDate": _iso(window.market_end_ms),
    }
    accepted: list[tuple[Decimal, int]] = []
    for row in rows:
        if row.get("status") != "ok" or row.get("http_status") != 200:
            continue
        try:
            received_ns = int(row["received_wall_ns"])
            requested_ns = int(row["requested_wall_ns"])
            if not (window.market_start_ms * 1_000_000 <= requested_ns <= received_ns
                    <= now_ms * 1_000_000 < window.market_end_ms * 1_000_000):
                continue
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if (not isinstance(payload, Mapping) or payload.get("source_url") != PRICE_ENDPOINT
                    or payload.get("request_params") != params
                    or payload.get("completed") is True):
                continue
            # incomplete=true is normal before close; it does not invalidate an
            # already observed opening price. Never substitute a close or spot.
            accepted.append((Decimal(_price(payload.get("price_to_beat"))), received_ns // 1_000_000))
        except (ValueError, TypeError, KeyError, InvalidOperation):
            continue
    if not accepted:
        return result
    if len({price for price, _ in accepted}) != 1:
        result["reference_reason"] = "conflicting_opening_references"
        return result
    price, received_ms = max(accepted, key=lambda pair: pair[1])
    return {
        "price_to_beat": format(price, "f"), "reference_status": "official",
        "reference_reason": None, "reference_source": PRICE_ENDPOINT,
        "reference_received_ms": received_ms,
    }


async def fetch_dashboard_payload(pool: Any, now_ms: int) -> dict[str, Any]:
    """Finish all indexed reads within one bounded budget using the reader pool."""
    async def fetch() -> dict[str, Any]:
        end_second = (now_ms // 1000) * 1000
        start_ms = end_second - HISTORY_MS
        window = market_for_sample_second(end_second)
        async with pool.acquire(timeout=QUERY_TIMEOUT_SECONDS) as connection:
            history_rows = await connection.fetch(
                HISTORY_SQL, start_ms, end_second, now_ms, timeout=QUERY_TIMEOUT_SECONDS,
            )
            reference_rows = await connection.fetch(
                REFERENCE_SQL, window.market_id, window.market_start_ms * 1_000_000,
                now_ms * 1_000_000, timeout=QUERY_TIMEOUT_SECONDS,
            )
        history: dict[str, Any] = {
            "window_start_ms": start_ms, "window_end_ms": now_ms,
            "chainlink": [], "twap": [],
        }
        for row in history_rows:
            key = "twap" if row["symbol"] == "BTCUSD_TWAP_60S" else "chainlink"
            history[key].append({
                "source_timestamp_ms": int(row["provider_event_ms"]),
                "received_ms": int(row["received_ms"]), "value": _price(row["price"]),
            })
        return {
            "schema_version": 1, "server_time_ms": now_ms, "history": history,
            "market": {
                "market_id": window.market_id, "market_start_ms": window.market_start_ms,
                "market_end_ms": window.market_end_ms,
                **opening_reference(reference_rows, window, now_ms),
            },
        }
    return await asyncio.wait_for(fetch(), timeout=REQUEST_TIMEOUT_SECONDS)
