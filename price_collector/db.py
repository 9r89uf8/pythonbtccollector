import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
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


def decimal_fixed_or_none(
    value: Optional[Decimal],
    places: str,
) -> Optional[str]:
    if value is None:
        return None

    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    quantum = Decimal(places)
    return format(value.quantize(quantum, rounding=ROUND_HALF_UP), "f")


def decimal_2dp_or_none(value: Optional[Decimal]) -> Optional[str]:
    return decimal_fixed_or_none(value, "0.01")


def money_2dp(value: Optional[Decimal]) -> Optional[str]:
    return decimal_fixed_or_none(value, "0.01")


def oi_3dp(value: Optional[Decimal]) -> Optional[str]:
    return decimal_fixed_or_none(value, "0.001")


def age_ms(server_time_ms: int, timestamp_ms: Optional[int]) -> Optional[int]:
    if timestamp_ms is None:
        return None
    return max(0, server_time_ms - int(timestamp_ms))


def freshness_meta(
    *,
    server_time_ms: int,
    source_time_ms: Optional[int],
    received_ms: Optional[int],
) -> dict[str, Optional[int]]:
    return {
        "source_age_ms": age_ms(server_time_ms, source_time_ms),
        "received_age_ms": age_ms(server_time_ms, received_ms),
        "transport_lag_ms": (
            None
            if source_time_ms is None or received_ms is None
            else max(0, int(received_ms) - int(source_time_ms))
        ),
    }


def freshness_age_only_meta(
    *,
    server_time_ms: int,
    source_time_ms: Optional[int],
    received_ms: Optional[int],
) -> dict[str, Optional[int]]:
    return {
        "source_age_ms": age_ms(server_time_ms, source_time_ms),
        "received_age_ms": age_ms(server_time_ms, received_ms),
    }


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


async def upsert_binance_futures_snapshot(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    futures_last_price: Optional[Decimal],
    futures_last_price_time_ms: Optional[int],
    mark_price: Optional[Decimal],
    index_price: Optional[Decimal],
    last_funding_rate: Optional[Decimal],
    next_funding_time_ms: Optional[int],
    premium_index_time_ms: Optional[int],
    open_interest: Optional[Decimal],
    open_interest_time_ms: Optional[int],
    oi_notional_usdt: Optional[Decimal],
    premium_bps: Optional[Decimal],
    received_ms: int,
    raw: Optional[Mapping[str, Any]],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_futures_snapshots (
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    futures_last_price,
                    futures_last_price_time_ms,
                    mark_price,
                    index_price,
                    last_funding_rate,
                    next_funding_time_ms,
                    premium_index_time_ms,
                    open_interest,
                    open_interest_time_ms,
                    oi_notional_usdt,
                    premium_bps,
                    received_ms,
                    raw
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8, $9, $10, $11,
                    $12, $13,
                    $14, $15,
                    $16,
                    $17::jsonb
                )
                ON CONFLICT (symbol, sample_second_ms)
                DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    sample_second_at = EXCLUDED.sample_second_at,
                    futures_last_price = EXCLUDED.futures_last_price,
                    futures_last_price_time_ms = EXCLUDED.futures_last_price_time_ms,
                    mark_price = EXCLUDED.mark_price,
                    index_price = EXCLUDED.index_price,
                    last_funding_rate = EXCLUDED.last_funding_rate,
                    next_funding_time_ms = EXCLUDED.next_funding_time_ms,
                    premium_index_time_ms = EXCLUDED.premium_index_time_ms,
                    open_interest = EXCLUDED.open_interest,
                    open_interest_time_ms = EXCLUDED.open_interest_time_ms,
                    oi_notional_usdt = EXCLUDED.oi_notional_usdt,
                    premium_bps = EXCLUDED.premium_bps,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                futures_last_price,
                futures_last_price_time_ms,
                mark_price,
                index_price,
                last_funding_rate,
                next_funding_time_ms,
                premium_index_time_ms,
                open_interest,
                open_interest_time_ms,
                oi_notional_usdt,
                premium_bps,
                received_ms,
                json.dumps(raw, default=str) if raw is not None else None,
            )


async def upsert_binance_futures_oi_5m_summary(
    pool: asyncpg.Pool,
    *,
    symbol: str,
    source_window_start_ms: int,
    source_window_end_ms: int,
    effective_window: MarketWindow,
    binance_timestamp_ms: int,
    sum_open_interest: Optional[Decimal],
    sum_open_interest_value: Optional[Decimal],
    received_ms: int,
    raw: Optional[Mapping[str, Any]],
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, effective_window)
            await connection.execute(
                """
                INSERT INTO binance_futures_oi_5m_summaries (
                    symbol,
                    source_window_start_ms,
                    source_window_end_ms,
                    effective_market_id,
                    binance_timestamp_ms,
                    sum_open_interest,
                    sum_open_interest_value,
                    received_ms,
                    raw
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                ON CONFLICT (symbol, source_window_start_ms, source_window_end_ms)
                DO UPDATE SET
                    effective_market_id = EXCLUDED.effective_market_id,
                    binance_timestamp_ms = EXCLUDED.binance_timestamp_ms,
                    sum_open_interest = EXCLUDED.sum_open_interest,
                    sum_open_interest_value = EXCLUDED.sum_open_interest_value,
                    received_ms = EXCLUDED.received_ms,
                    raw = EXCLUDED.raw
                """,
                symbol,
                source_window_start_ms,
                source_window_end_ms,
                effective_window.market_id,
                binance_timestamp_ms,
                sum_open_interest,
                sum_open_interest_value,
                received_ms,
                json.dumps(raw, default=str) if raw is not None else None,
            )


async def upsert_binance_flow_1s(
    pool: asyncpg.Pool,
    *,
    venue: str,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    buy_base: Decimal,
    sell_base: Decimal,
    buy_quote: Decimal,
    sell_quote: Decimal,
    delta_quote: Decimal,
    total_quote: Decimal,
    taker_imbalance: Optional[Decimal],
    cvd_quote: Decimal,
    cvd_10s: Decimal,
    cvd_30s: Decimal,
    imbalance_10s: Optional[Decimal],
    imbalance_30s: Optional[Decimal],
    agg_trade_count: int,
    trade_count: int,
    max_trade_quote: Optional[Decimal],
    first_agg_trade_id: Optional[int],
    last_agg_trade_id: Optional[int],
    last_trade_time_ms: Optional[int],
    last_event_time_ms: Optional[int],
    received_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_flow_1s (
                    venue,
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    buy_base,
                    sell_base,
                    buy_quote,
                    sell_quote,
                    delta_quote,
                    total_quote,
                    taker_imbalance,
                    cvd_quote,
                    cvd_10s,
                    cvd_30s,
                    imbalance_10s,
                    imbalance_30s,
                    agg_trade_count,
                    trade_count,
                    max_trade_quote,
                    first_agg_trade_id,
                    last_agg_trade_id,
                    last_trade_time_ms,
                    last_event_time_ms,
                    received_ms
                )
                VALUES (
                    $1, $2,
                    $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12,
                    $13, $14, $15, $16, $17,
                    $18, $19, $20,
                    $21, $22, $23, $24,
                    $25
                )
                ON CONFLICT (venue, symbol, sample_second_ms)
                DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    sample_second_at = EXCLUDED.sample_second_at,
                    buy_base = EXCLUDED.buy_base,
                    sell_base = EXCLUDED.sell_base,
                    buy_quote = EXCLUDED.buy_quote,
                    sell_quote = EXCLUDED.sell_quote,
                    delta_quote = EXCLUDED.delta_quote,
                    total_quote = EXCLUDED.total_quote,
                    taker_imbalance = EXCLUDED.taker_imbalance,
                    cvd_quote = EXCLUDED.cvd_quote,
                    cvd_10s = EXCLUDED.cvd_10s,
                    cvd_30s = EXCLUDED.cvd_30s,
                    imbalance_10s = EXCLUDED.imbalance_10s,
                    imbalance_30s = EXCLUDED.imbalance_30s,
                    agg_trade_count = EXCLUDED.agg_trade_count,
                    trade_count = EXCLUDED.trade_count,
                    max_trade_quote = EXCLUDED.max_trade_quote,
                    first_agg_trade_id = EXCLUDED.first_agg_trade_id,
                    last_agg_trade_id = EXCLUDED.last_agg_trade_id,
                    last_trade_time_ms = EXCLUDED.last_trade_time_ms,
                    last_event_time_ms = EXCLUDED.last_event_time_ms,
                    received_ms = EXCLUDED.received_ms
                """,
                venue,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                buy_base,
                sell_base,
                buy_quote,
                sell_quote,
                delta_quote,
                total_quote,
                taker_imbalance,
                cvd_quote,
                cvd_10s,
                cvd_30s,
                imbalance_10s,
                imbalance_30s,
                agg_trade_count,
                trade_count,
                max_trade_quote,
                first_agg_trade_id,
                last_agg_trade_id,
                last_trade_time_ms,
                last_event_time_ms,
                received_ms,
            )


async def upsert_binance_book_1s(
    pool: asyncpg.Pool,
    *,
    venue: str,
    symbol: str,
    window: MarketWindow,
    sample_second_ms: int,
    bid: Decimal,
    ask: Decimal,
    bid_qty: Decimal,
    ask_qty: Decimal,
    mid: Decimal,
    spread: Decimal,
    spread_bps: Decimal,
    book_imbalance: Optional[Decimal],
    microprice: Optional[Decimal],
    update_id: Optional[int],
    event_time_ms: Optional[int],
    transaction_time_ms: Optional[int],
    received_ms: int,
) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await _ensure_market_window(connection, window)
            await connection.execute(
                """
                INSERT INTO binance_book_1s (
                    venue,
                    symbol,
                    market_id,
                    sample_second_ms,
                    sample_second_at,
                    bid,
                    ask,
                    bid_qty,
                    ask_qty,
                    mid,
                    spread,
                    spread_bps,
                    book_imbalance,
                    microprice,
                    update_id,
                    event_time_ms,
                    transaction_time_ms,
                    received_ms
                )
                VALUES (
                    $1, $2,
                    $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13, $14,
                    $15, $16, $17,
                    $18
                )
                ON CONFLICT (venue, symbol, sample_second_ms)
                DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    sample_second_at = EXCLUDED.sample_second_at,
                    bid = EXCLUDED.bid,
                    ask = EXCLUDED.ask,
                    bid_qty = EXCLUDED.bid_qty,
                    ask_qty = EXCLUDED.ask_qty,
                    mid = EXCLUDED.mid,
                    spread = EXCLUDED.spread,
                    spread_bps = EXCLUDED.spread_bps,
                    book_imbalance = EXCLUDED.book_imbalance,
                    microprice = EXCLUDED.microprice,
                    update_id = EXCLUDED.update_id,
                    event_time_ms = EXCLUDED.event_time_ms,
                    transaction_time_ms = EXCLUDED.transaction_time_ms,
                    received_ms = EXCLUDED.received_ms
                """,
                venue,
                symbol,
                window.market_id,
                sample_second_ms,
                epoch_ms_to_utc_datetime(sample_second_ms),
                bid,
                ask,
                bid_qty,
                ask_qty,
                mid,
                spread,
                spread_bps,
                book_imbalance,
                microprice,
                update_id,
                event_time_ms,
                transaction_time_ms,
                received_ms,
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
    server_time_ms: int,
    include_probabilities: bool,
    include_futures: bool,
    include_oi: bool,
) -> Optional[dict[str, Any]]:
    if not rows:
        return None

    first = rows[0]
    market_start_ms = int(first["market_start_ms"])
    market_end_ms = int(first["market_end_ms"])

    series = []
    for row in rows:
        sample_second_ms = int(row["sample_second_ms"])
        binance_source_ms = row.get("binance_provider_event_ms")
        binance_received_ms = row.get("binance_received_ms")
        chainlink_source_ms = row.get("chainlink_provider_event_ms")
        chainlink_message_ms = row.get("chainlink_provider_message_ms")
        chainlink_received_ms = row.get("chainlink_received_ms")
        chainlink_sample_second_ms = row.get("chainlink_sample_second_ms")
        futures_last_price_time_ms = row.get("futures_last_price_time_ms")
        premium_index_time_ms = row.get("premium_index_time_ms")
        open_interest_time_ms = row.get("open_interest_time_ms")
        futures_received_ms = row.get("futures_received_ms")
        item = {
            "t": (sample_second_ms - market_start_ms) // 1000,
            "timestamp_ms": sample_second_ms,
            "timestamp_at": utc_datetime_to_z(epoch_ms_to_utc_datetime(sample_second_ms)),
            "prices": {
                "binance": decimal_2dp_or_none(row["binance_price"]),
                "chainlink": decimal_2dp_or_none(row["chainlink_price"]),
            },
            "freshness": {
                "binance": {
                    "source_ms": binance_source_ms,
                    "received_ms": binance_received_ms,
                    **freshness_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=binance_source_ms,
                        received_ms=binance_received_ms,
                    ),
                },
                "chainlink": {
                    "source_ms": chainlink_source_ms,
                    "message_ms": chainlink_message_ms,
                    "received_ms": chainlink_received_ms,
                    "is_carried_forward": (
                        chainlink_sample_second_ms is not None
                        and int(chainlink_sample_second_ms) != sample_second_ms
                    ),
                    **freshness_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=chainlink_source_ms,
                        received_ms=chainlink_received_ms,
                    ),
                },
                "futures_last": {
                    "source_ms": futures_last_price_time_ms,
                    "received_ms": futures_received_ms,
                    **freshness_age_only_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=futures_last_price_time_ms,
                        received_ms=futures_received_ms,
                    ),
                },
                "open_interest": {
                    "source_ms": open_interest_time_ms,
                    "received_ms": futures_received_ms,
                    **freshness_age_only_meta(
                        server_time_ms=server_time_ms,
                        source_time_ms=open_interest_time_ms,
                        received_ms=futures_received_ms,
                    ),
                },
            },
        }

        if include_probabilities:
            item["probabilities"] = {
                "up": {
                    "ask": decimal_2dp_or_none(row["up_ask"]),
                },
                "down": {
                    "ask": decimal_2dp_or_none(row["down_ask"]),
                },
            }

        if include_futures:
            item["futures"] = {
                "last": money_2dp(row["futures_last_price"]),
                "mark": money_2dp(row["mark_price"]),
                "index": money_2dp(row["index_price"]),
                "premium_bps": money_2dp(row["premium_bps"]),
            }

        if include_oi:
            item["open_interest"] = {
                "contracts": oi_3dp(row["open_interest"]),
                "notional_usdt": money_2dp(row["oi_notional_usdt"]),
                "delta_30s": oi_3dp(row["oi_delta_30s"]),
                "delta_60s": oi_3dp(row["oi_delta_60s"]),
                "delta_300s": oi_3dp(row["oi_delta_300s"]),
            }

        series.append(item)

    payload = {
        "schema_version": 1,
        "server_time_ms": server_time_ms,
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

    if include_oi and first.get("prev_oi_source_window_start_ms") is not None:
        payload["previous_5m_oi_summary"] = {
            "source_window_start_ms": first["prev_oi_source_window_start_ms"],
            "source_window_end_ms": first["prev_oi_source_window_end_ms"],
            "effective_market_id": int(first["market_id"]),
            "sum_open_interest": oi_3dp(first["prev_oi_sum_open_interest"]),
            "sum_open_interest_value": money_2dp(
                first["prev_oi_sum_open_interest_value"]
            ),
        }

    return payload


async def fetch_market_download_payload(
    pool: asyncpg.Pool,
    *,
    market_id: int,
    server_time_ms: int,
    include_probabilities: bool,
    include_futures: bool,
    include_oi: bool,
    fill_display: bool = False,
    max_carry_forward_ms: int = 10_000,
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
                SELECT
                    ps.sample_second_ms,
                    ps.price,
                    ps.provider_event_ms AS binance_provider_event_ms,
                    ps.received_ms AS binance_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'binance_spot'
                  AND i.symbol = 'BTCUSDT'
            ),
            chainlink AS (
                SELECT
                    ps.sample_second_ms,
                    ps.price,
                    ps.provider_event_ms AS chainlink_provider_event_ms,
                    ps.provider_message_ms AS chainlink_provider_message_ms,
                    ps.received_ms AS chainlink_received_ms
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
            futures AS (
                SELECT *
                FROM binance_futures_snapshots
                WHERE market_id = $1
                  AND symbol = 'BTCUSDT'
            ),
            oi_prev AS (
                SELECT *
                FROM binance_futures_oi_5m_summaries
                WHERE effective_market_id = $1
                  AND symbol = 'BTCUSDT'
                ORDER BY source_window_end_ms DESC
                LIMIT 1
            ),
            oi_30 AS (
                SELECT
                    f.sample_second_ms,
                    prev.open_interest AS open_interest_30s_ago
                FROM futures f
                LEFT JOIN binance_futures_snapshots prev
                  ON prev.symbol = f.symbol
                 AND prev.sample_second_ms = f.sample_second_ms - 30000
            ),
            oi_60 AS (
                SELECT
                    f.sample_second_ms,
                    prev.open_interest AS open_interest_60s_ago
                FROM futures f
                LEFT JOIN binance_futures_snapshots prev
                  ON prev.symbol = f.symbol
                 AND prev.sample_second_ms = f.sample_second_ms - 60000
            ),
            oi_300 AS (
                SELECT
                    f.sample_second_ms,
                    prev.open_interest AS open_interest_300s_ago
                FROM futures f
                LEFT JOIN binance_futures_snapshots prev
                  ON prev.symbol = f.symbol
                 AND prev.sample_second_ms = f.sample_second_ms - 300000
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
                b.binance_provider_event_ms,
                b.binance_received_ms,

                c.sample_second_ms AS chainlink_sample_second_ms,
                c.price AS chainlink_price,
                c.chainlink_provider_event_ms,
                c.chainlink_provider_message_ms,
                c.chainlink_received_ms,

                probs.up_bid,
                probs.up_ask,
                probs.up_mid,
                probs.down_bid,
                probs.down_ask,
                probs.down_mid,
                probs.up_prob_norm,
                probs.down_prob_norm,

                f.futures_last_price,
                f.mark_price,
                f.index_price,
                f.last_funding_rate,
                f.next_funding_time_ms,
                f.futures_last_price_time_ms,
                f.premium_index_time_ms,
                f.open_interest,
                f.open_interest_time_ms,
                f.oi_notional_usdt,
                f.premium_bps,
                f.received_ms AS futures_received_ms,

                (f.open_interest - oi_30.open_interest_30s_ago) AS oi_delta_30s,
                (f.open_interest - oi_60.open_interest_60s_ago) AS oi_delta_60s,
                (f.open_interest - oi_300.open_interest_300s_ago) AS oi_delta_300s,

                oi_prev.source_window_start_ms AS prev_oi_source_window_start_ms,
                oi_prev.source_window_end_ms AS prev_oi_source_window_end_ms,
                oi_prev.sum_open_interest AS prev_oi_sum_open_interest,
                oi_prev.sum_open_interest_value AS prev_oi_sum_open_interest_value
            FROM seconds s
            CROSS JOIN mw
            LEFT JOIN pm ON pm.market_id = mw.market_id
            LEFT JOIN binance b ON b.sample_second_ms = s.sample_second_ms
            LEFT JOIN LATERAL (
                SELECT *
                FROM chainlink cl
                WHERE (
                    $2::BOOLEAN
                    AND cl.sample_second_ms <= s.sample_second_ms
                    AND cl.sample_second_ms >= s.sample_second_ms - $3::BIGINT
                )
                OR (
                    NOT $2::BOOLEAN
                    AND cl.sample_second_ms = s.sample_second_ms
                )
                ORDER BY cl.sample_second_ms DESC
                LIMIT 1
            ) c ON TRUE
            LEFT JOIN probs ON probs.sample_second_ms = s.sample_second_ms
            LEFT JOIN futures f ON f.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_30 ON oi_30.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_60 ON oi_60.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_300 ON oi_300.sample_second_ms = s.sample_second_ms
            LEFT JOIN oi_prev ON TRUE
            ORDER BY s.sample_second_ms ASC
            """,
            market_id,
            fill_display,
            max(0, max_carry_forward_ms),
        )

    return build_market_download_payload(
        [dict(row) for row in rows],
        server_time_ms=server_time_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
    )


async def fetch_current_live_payload(
    pool: asyncpg.Pool,
    *,
    window: MarketWindow,
    current_sample_second_ms: int,
    server_time_ms: int,
    max_chainlink_carry_forward_ms: int = 10_000,
) -> dict[str, Any]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            WITH binance AS (
                SELECT
                    ps.sample_second_ms AS binance_sample_second_ms,
                    ps.price AS binance_price,
                    ps.provider_event_ms AS binance_provider_event_ms,
                    ps.received_ms AS binance_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'binance_spot'
                  AND i.symbol = 'BTCUSDT'
                ORDER BY ps.sample_second_ms DESC
                LIMIT 1
            ),
            chainlink AS (
                SELECT
                    ps.sample_second_ms AS chainlink_sample_second_ms,
                    ps.price AS chainlink_price,
                    ps.provider_event_ms AS chainlink_provider_event_ms,
                    ps.provider_message_ms AS chainlink_provider_message_ms,
                    ps.received_ms AS chainlink_received_ms
                FROM price_samples ps
                JOIN instruments i ON i.instrument_id = ps.instrument_id
                JOIN providers p ON p.provider_id = i.provider_id
                WHERE ps.market_id = $1
                  AND p.provider_code = 'polymarket_chainlink_rtds'
                  AND i.symbol = 'BTCUSD'
                  AND ps.sample_second_ms <= $2
                  AND ps.sample_second_ms >= $2 - $3::BIGINT
                ORDER BY ps.sample_second_ms DESC
                LIMIT 1
            ),
            futures_price AS (
                SELECT
                    sample_second_ms AS futures_sample_second_ms,
                    futures_last_price,
                    futures_last_price_time_ms,
                    mark_price,
                    index_price,
                    premium_index_time_ms,
                    received_ms AS futures_received_ms
                FROM binance_futures_snapshots
                WHERE market_id = $1
                  AND symbol = 'BTCUSDT'
                  AND (
                    futures_last_price IS NOT NULL
                    OR mark_price IS NOT NULL
                    OR index_price IS NOT NULL
                  )
                ORDER BY COALESCE(
                    futures_last_price_time_ms,
                    premium_index_time_ms,
                    sample_second_ms
                ) DESC
                LIMIT 1
            ),
            oi AS (
                SELECT
                    sample_second_ms AS oi_sample_second_ms,
                    open_interest,
                    open_interest_time_ms,
                    received_ms AS oi_received_ms
                FROM binance_futures_snapshots
                WHERE market_id = $1
                  AND symbol = 'BTCUSDT'
                  AND open_interest IS NOT NULL
                ORDER BY COALESCE(open_interest_time_ms, sample_second_ms) DESC
                LIMIT 1
            )
            SELECT
                binance.binance_sample_second_ms,
                binance.binance_price,
                binance.binance_provider_event_ms,
                binance.binance_received_ms,

                chainlink.chainlink_sample_second_ms,
                chainlink.chainlink_price,
                chainlink.chainlink_provider_event_ms,
                chainlink.chainlink_provider_message_ms,
                chainlink.chainlink_received_ms,

                futures_price.futures_sample_second_ms,
                futures_price.futures_last_price,
                futures_price.futures_last_price_time_ms,
                futures_price.mark_price,
                futures_price.index_price,
                futures_price.premium_index_time_ms,
                futures_price.futures_received_ms,

                oi.oi_sample_second_ms,
                oi.open_interest,
                oi.open_interest_time_ms,
                oi.oi_received_ms
            FROM (SELECT 1) seed
            LEFT JOIN binance ON TRUE
            LEFT JOIN chainlink ON TRUE
            LEFT JOIN futures_price ON TRUE
            LEFT JOIN oi ON TRUE
            """,
            window.market_id,
            current_sample_second_ms,
            max(0, max_chainlink_carry_forward_ms),
        )

    data = dict(row) if row is not None else {}

    binance_source_ms = data.get("binance_provider_event_ms")
    binance_received_ms = data.get("binance_received_ms")
    chainlink_source_ms = data.get("chainlink_provider_event_ms")
    chainlink_received_ms = data.get("chainlink_received_ms")
    futures_last_time_ms = data.get("futures_last_price_time_ms")
    premium_index_time_ms = data.get("premium_index_time_ms")
    futures_received_ms = data.get("futures_received_ms")
    open_interest_time_ms = data.get("open_interest_time_ms")
    oi_received_ms = data.get("oi_received_ms")

    return {
        "server_time_ms": server_time_ms,
        "market_id": window.market_id,
        "market_start_ms": window.market_start_ms,
        "market_end_ms": window.market_end_ms,
        "prices": {
            "binance_spot": {
                "value": decimal_2dp_or_none(data.get("binance_price")),
                "sample_second_ms": data.get("binance_sample_second_ms"),
                "provider_event_ms": binance_source_ms,
                "received_ms": binance_received_ms,
                **freshness_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=binance_source_ms,
                    received_ms=binance_received_ms,
                ),
            },
            "chainlink": {
                "value": decimal_2dp_or_none(data.get("chainlink_price")),
                "sample_second_ms": data.get("chainlink_sample_second_ms"),
                "provider_event_ms": chainlink_source_ms,
                "provider_message_ms": data.get("chainlink_provider_message_ms"),
                "received_ms": chainlink_received_ms,
                "is_carried_forward_for_display": (
                    data.get("chainlink_sample_second_ms") is not None
                    and int(data["chainlink_sample_second_ms"]) < current_sample_second_ms
                ),
                **freshness_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=chainlink_source_ms,
                    received_ms=chainlink_received_ms,
                ),
            },
        },
        "futures": {
            "last": {
                "value": money_2dp(data.get("futures_last_price")),
                "time_ms": futures_last_time_ms,
                "received_ms": futures_received_ms,
                **freshness_age_only_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=futures_last_time_ms,
                    received_ms=futures_received_ms,
                ),
            },
            "mark": {
                "value": money_2dp(data.get("mark_price")),
                "time_ms": premium_index_time_ms,
                "received_ms": futures_received_ms,
                **freshness_age_only_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=premium_index_time_ms,
                    received_ms=futures_received_ms,
                ),
            },
            "index": {
                "value": money_2dp(data.get("index_price")),
                "time_ms": premium_index_time_ms,
                "received_ms": futures_received_ms,
                **freshness_age_only_meta(
                    server_time_ms=server_time_ms,
                    source_time_ms=premium_index_time_ms,
                    received_ms=futures_received_ms,
                ),
            },
        },
        "open_interest": {
            "contracts": oi_3dp(data.get("open_interest")),
            "time_ms": open_interest_time_ms,
            "received_ms": oi_received_ms,
            **freshness_age_only_meta(
                server_time_ms=server_time_ms,
                source_time_ms=open_interest_time_ms,
                received_ms=oi_received_ms,
            ),
        },
    }
