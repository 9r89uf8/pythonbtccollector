import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Optional

import asyncpg

from price_collector.market import MarketWindow


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def epoch_ms_to_utc_datetime(epoch_ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=epoch_ms)


def utc_datetime_to_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def decimal_or_none(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    return format(value, "f")


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )


async def create_read_pool(settings: Any) -> asyncpg.Pool:
    database_url = settings.READ_DATABASE_URL or settings.DATABASE_URL
    if not database_url:
        raise RuntimeError("READ_DATABASE_URL or DATABASE_URL must be set for the API")
    return await create_pool(database_url)


async def get_instrument_id(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
) -> int:
    async with pool.acquire() as connection:
        instrument_id = await connection.fetchval(
            """
            SELECT i.instrument_id
            FROM instruments i
            JOIN providers p ON p.provider_id = i.provider_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
            """,
            provider_code,
            symbol,
        )

    if instrument_id is None:
        raise LookupError(
            f"instrument not found for provider_code={provider_code!r}, symbol={symbol!r}"
        )

    return int(instrument_id)


async def _ensure_market_window(connection: asyncpg.Connection, window: MarketWindow) -> None:
    await connection.execute(
        """
        INSERT INTO market_windows (
            market_id,
            market_start_ms,
            market_end_ms,
            market_start_at,
            market_end_at
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (market_id) DO NOTHING
        """,
        window.market_id,
        window.market_start_ms,
        window.market_end_ms,
        epoch_ms_to_utc_datetime(window.market_start_ms),
        epoch_ms_to_utc_datetime(window.market_end_ms),
    )


async def ensure_market_window(pool: asyncpg.Pool, window: MarketWindow) -> None:
    async with pool.acquire() as connection:
        await _ensure_market_window(connection, window)


async def upsert_price_sample(
    pool: asyncpg.Pool,
    *,
    instrument_id: int,
    sample_second_ms: int,
    window: MarketWindow,
    price: Decimal,
    provider_event_ms: Optional[int],
    received_ms: int,
    source_price_field: str = "c",
    provider_message_ms: Optional[int] = None,
    source_topic: Optional[str] = None,
) -> None:
    if not isinstance(price, Decimal):
        raise TypeError("price must be Decimal")

    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO price_samples (
                    instrument_id,
                    sample_second_ms,
                    sample_second_at,
                    market_id,
                    price,
                    provider_event_ms,
                    received_ms,
                    source_price_field,
                    provider_message_ms,
                    source_topic
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (instrument_id, sample_second_ms)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    provider_event_ms = EXCLUDED.provider_event_ms,
                    received_ms = EXCLUDED.received_ms,
                    source_price_field = EXCLUDED.source_price_field,
                    provider_message_ms = EXCLUDED.provider_message_ms,
                    source_topic = EXCLUDED.source_topic
                """,
                instrument_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                window.market_id,
                price,
                provider_event_ms,
                received_ms,
                source_price_field,
                provider_message_ms,
                source_topic,
            )


async def upsert_polymarket_btc_5m_market(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    slug: str,
    gamma_event_id: Optional[str],
    gamma_market_id: Optional[str],
    condition_id: Optional[str],
    question: Optional[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    up_token_id: str,
    down_token_id: str,
    up_outcome: str,
    down_outcome: str,
    active: Optional[bool],
    closed: Optional[bool],
    archived: Optional[bool],
    raw_gamma: Mapping[str, Any],
    seen_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO polymarket_btc_5m_markets (
                    market_id,
                    slug,
                    gamma_event_id,
                    gamma_market_id,
                    condition_id,
                    question,
                    start_ms,
                    end_ms,
                    start_at,
                    end_at,
                    up_token_id,
                    down_token_id,
                    up_outcome,
                    down_outcome,
                    active,
                    closed,
                    archived,
                    raw_gamma,
                    first_seen_ms,
                    last_seen_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, $16, $17,
                    $18::jsonb, $19, $20
                )
                ON CONFLICT (market_id)
                DO UPDATE SET
                    slug = EXCLUDED.slug,
                    gamma_event_id = EXCLUDED.gamma_event_id,
                    gamma_market_id = EXCLUDED.gamma_market_id,
                    condition_id = EXCLUDED.condition_id,
                    question = EXCLUDED.question,
                    start_ms = EXCLUDED.start_ms,
                    end_ms = EXCLUDED.end_ms,
                    start_at = EXCLUDED.start_at,
                    end_at = EXCLUDED.end_at,
                    up_token_id = EXCLUDED.up_token_id,
                    down_token_id = EXCLUDED.down_token_id,
                    up_outcome = EXCLUDED.up_outcome,
                    down_outcome = EXCLUDED.down_outcome,
                    active = EXCLUDED.active,
                    closed = EXCLUDED.closed,
                    archived = EXCLUDED.archived,
                    raw_gamma = EXCLUDED.raw_gamma,
                    last_seen_ms = EXCLUDED.last_seen_ms,
                    updated_at = now()
                """,
                window.market_id,
                slug,
                gamma_event_id,
                gamma_market_id,
                condition_id,
                question,
                start_ms,
                end_ms,
                epoch_ms_to_utc_datetime(start_ms) if start_ms is not None else None,
                epoch_ms_to_utc_datetime(end_ms) if end_ms is not None else None,
                up_token_id,
                down_token_id,
                up_outcome,
                down_outcome,
                active,
                closed,
                archived,
                json.dumps(raw_gamma, default=str),
                seen_ms,
                seen_ms,
            )


async def upsert_polymarket_probability_sample(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    source: str,
    sample_second_ms: int,
    up_token_id: str,
    down_token_id: str,
    up_bid: Optional[Decimal],
    up_ask: Optional[Decimal],
    up_mid: Optional[Decimal],
    down_bid: Optional[Decimal],
    down_ask: Optional[Decimal],
    down_mid: Optional[Decimal],
    up_prob_norm: Optional[Decimal],
    down_prob_norm: Optional[Decimal],
    provider_event_ms: Optional[int],
    received_ms: int,
    raw: Optional[Mapping[str, Any]],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO polymarket_probability_samples (
                    market_id,
                    source,
                    sample_second_ms,
                    sample_second_at,
                    up_token_id,
                    down_token_id,
                    up_bid,
                    up_ask,
                    up_mid,
                    down_bid,
                    down_ask,
                    down_mid,
                    up_prob_norm,
                    down_prob_norm,
                    provider_event_ms,
                    received_ms,
                    raw
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14,
                    $15, $16,
                    $17::jsonb
                )
                ON CONFLICT (market_id, source, sample_second_ms)
                DO UPDATE SET
                    up_token_id = EXCLUDED.up_token_id,
                    down_token_id = EXCLUDED.down_token_id,
                    up_bid = EXCLUDED.up_bid,
                    up_ask = EXCLUDED.up_ask,
                    up_mid = EXCLUDED.up_mid,
                    down_bid = EXCLUDED.down_bid,
                    down_ask = EXCLUDED.down_ask,
                    down_mid = EXCLUDED.down_mid,
                    up_prob_norm = EXCLUDED.up_prob_norm,
                    down_prob_norm = EXCLUDED.down_prob_norm,
                    provider_event_ms = EXCLUDED.provider_event_ms,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                window.market_id,
                source,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                up_token_id,
                down_token_id,
                up_bid,
                up_ask,
                up_mid,
                down_bid,
                down_ask,
                down_mid,
                up_prob_norm,
                down_prob_norm,
                provider_event_ms,
                received_ms,
                json.dumps(raw, default=str) if raw is not None else None,
            )


async def health_check(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.fetchval("SELECT 1")


async def fetch_latest_price(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
) -> Optional[Mapping[str, Any]]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                ps.price AS price,
                ps.sample_second_ms AS sample_second_ms,
                ps.sample_second_at AS sample_second_at,
                ps.provider_event_ms AS provider_event_ms,
                ps.received_ms AS received_ms,
                ps.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
            ORDER BY ps.sample_second_ms DESC
            LIMIT 1
            """,
            provider_code,
            symbol,
        )

    return dict(row) if row is not None else None


async def fetch_latest_market_id(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
) -> Optional[int]:
    async with pool.acquire() as connection:
        market_id = await connection.fetchval(
            """
            SELECT max(ps.market_id)
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
            """,
            provider_code,
            symbol,
        )

    return int(market_id) if market_id is not None else None


async def fetch_market_summary(
    pool: asyncpg.Pool,
    provider_code: str,
    symbol: str,
    market_id: int,
) -> Optional[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                mw.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms,
                mw.market_start_at AS market_start_at,
                mw.market_end_at AS market_end_at,
                ps.sample_second_ms AS sample_second_ms,
                ps.sample_second_at AS sample_second_at,
                ps.price AS price
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE p.provider_code = $1
              AND i.symbol = $2
              AND ps.market_id = $3
            ORDER BY ps.sample_second_ms ASC
            """,
            provider_code,
            symbol,
            market_id,
        )

    if not rows:
        return None

    samples = [
        {
            "sample_second_ms": row["sample_second_ms"],
            "sample_second_at": row["sample_second_at"],
            "price": row["price"],
        }
        for row in rows
    ]
    prices = [sample["price"] for sample in samples]
    first = rows[0]

    return {
        "provider": first["provider"],
        "symbol": first["symbol"],
        "market_id": first["market_id"],
        "market_start_ms": first["market_start_ms"],
        "market_end_ms": first["market_end_ms"],
        "market_start_at": first["market_start_at"],
        "market_end_at": first["market_end_at"],
        "sample_count": len(samples),
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "samples": samples,
    }


def build_market_sources_summary(
    rows: list[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    if not rows:
        return None

    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = (row["provider"], row["symbol"])
        grouped.setdefault(key, []).append(row)

    first = rows[0]
    sources = []
    for source_rows in grouped.values():
        prices = [row["price"] for row in source_rows]
        source_first = source_rows[0]
        source_latest = source_rows[-1]
        sources.append(
            {
                "provider": source_first["provider"],
                "symbol": source_first["symbol"],
                "quote_asset": source_first["quote_asset"],
                "sample_count": len(source_rows),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "latest_sample_second_ms": source_latest["sample_second_ms"],
                "latest_provider_event_ms": source_latest["provider_event_ms"],
                "latest_received_ms": source_latest["received_ms"],
            }
        )

    return {
        "market_id": first["market_id"],
        "market_start_ms": first["market_start_ms"],
        "market_end_ms": first["market_end_ms"],
        "market_start_at": first["market_start_at"],
        "market_end_at": first["market_end_at"],
        "sources": sources,
    }


async def fetch_market_summaries_for_btc_sources(
    pool: asyncpg.Pool,
    market_id: int,
) -> Optional[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                i.quote_asset AS quote_asset,
                mw.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms,
                mw.market_start_at AS market_start_at,
                mw.market_end_at AS market_end_at,
                ps.sample_second_ms AS sample_second_ms,
                ps.price AS price,
                ps.provider_event_ms AS provider_event_ms,
                ps.received_ms AS received_ms
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE ps.market_id = $1
              AND (
                (p.provider_code = 'binance_spot' AND i.symbol = 'BTCUSDT')
                OR
                (p.provider_code = 'polymarket_chainlink_rtds' AND i.symbol = 'BTCUSD')
              )
            ORDER BY
                CASE p.provider_code
                    WHEN 'binance_spot' THEN 0
                    WHEN 'polymarket_chainlink_rtds' THEN 1
                    ELSE 2
                END,
                i.symbol ASC,
                ps.sample_second_ms ASC
            """,
            market_id,
        )

    return build_market_sources_summary([dict(row) for row in rows])


def build_market_download_payload(
    rows: list[Mapping[str, Any]],
    *,
    include_probabilities: bool,
) -> Optional[dict[str, Any]]:
    if not rows:
        return None

    first = rows[0]
    market_start_ms = int(first["market_start_ms"])
    market_end_ms = int(first["market_end_ms"])

    series = []
    for row in rows:
        sample_second_ms = int(row["sample_second_ms"])
        item = {
            "t": (sample_second_ms - market_start_ms) // 1000,
            "timestamp_ms": sample_second_ms,
            "timestamp_at": utc_datetime_to_z(epoch_ms_to_utc_datetime(sample_second_ms)),
            "prices": {
                "binance": decimal_or_none(row["binance_price"]),
                "chainlink": decimal_or_none(row["chainlink_price"]),
            },
        }

        if include_probabilities:
            item["probabilities"] = {
                "up": {
                    "bid": decimal_or_none(row["up_bid"]),
                    "ask": decimal_or_none(row["up_ask"]),
                    "mid": decimal_or_none(row["up_mid"]),
                    "normalized": decimal_or_none(row["up_prob_norm"]),
                },
                "down": {
                    "bid": decimal_or_none(row["down_bid"]),
                    "ask": decimal_or_none(row["down_ask"]),
                    "mid": decimal_or_none(row["down_mid"]),
                    "normalized": decimal_or_none(row["down_prob_norm"]),
                },
            }

        series.append(item)

    return {
        "schema_version": 1,
        "market": {
            "market_id": int(first["market_id"]),
            "market_start_ms": market_start_ms,
            "market_end_ms": market_end_ms,
            "market_start_at": utc_datetime_to_z(first["market_start_at"]),
            "market_end_at": utc_datetime_to_z(first["market_end_at"]),
            "seconds_expected": (market_end_ms - market_start_ms) // 1000,
        },
        "series": series,
    }


async def fetch_market_download_payload(
    pool: asyncpg.Pool,
    *,
    market_id: int,
    include_probabilities: bool,
) -> Optional[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            WITH mw AS (
                SELECT *
                FROM market_windows
                WHERE market_id = $1
            ),
            seconds AS (
                SELECT generate_series(
                    (SELECT market_start_ms FROM mw),
                    (SELECT market_end_ms FROM mw) - 1000,
                    1000::BIGINT
                )::BIGINT AS sample_second_ms
            ),
            binance AS (
                SELECT ps.sample_second_ms, ps.price
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'binance_spot'
                  AND i.symbol = 'BTCUSDT'
            ),
            chainlink AS (
                SELECT ps.sample_second_ms, ps.price
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'polymarket_chainlink_rtds'
                  AND i.symbol = 'BTCUSD'
            ),
            probs AS (
                SELECT *
                FROM polymarket_probability_samples
                WHERE market_id = $1
                  AND source = 'polymarket_clob'
            ),
            pm AS (
                SELECT *
                FROM polymarket_btc_5m_markets
                WHERE market_id = $1
            )
            SELECT
                mw.market_id,
                mw.market_start_ms,
                mw.market_end_ms,
                mw.market_start_at,
                mw.market_end_at,

                pm.slug,
                pm.question,
                pm.condition_id,
                pm.up_token_id,
                pm.down_token_id,

                s.sample_second_ms,

                b.price AS binance_price,
                c.price AS chainlink_price,

                probs.up_bid,
                probs.up_ask,
                probs.up_mid,
                probs.down_bid,
                probs.down_ask,
                probs.down_mid,
                probs.up_prob_norm,
                probs.down_prob_norm
            FROM seconds s
            CROSS JOIN mw
            LEFT JOIN pm ON pm.market_id = mw.market_id
            LEFT JOIN binance b ON b.sample_second_ms = s.sample_second_ms
            LEFT JOIN chainlink c ON c.sample_second_ms = s.sample_second_ms
            LEFT JOIN probs ON probs.sample_second_ms = s.sample_second_ms
            ORDER BY s.sample_second_ms ASC
            """,
            market_id,
        )

    return build_market_download_payload(
        [dict(row) for row in rows],
        include_probabilities=include_probabilities,
    )
