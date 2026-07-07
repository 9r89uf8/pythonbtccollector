from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from price_collector.collector import current_utc_epoch_ms
from price_collector.config import Settings
from price_collector.db import (
    create_read_pool,
    fetch_latest_market_id,
    fetch_latest_price,
    fetch_market_summary,
    health_check,
)


DEFAULT_PROVIDER = "binance_spot"
DEFAULT_SYMBOL = "BTCUSDT"
SERVICE_NAME = "price-api"


def utc_datetime_to_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def decimal_to_string(value: Decimal) -> str:
    return format(value, "f")


def serialize_latest_price(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "provider": row["provider"],
        "symbol": row["symbol"],
        "price": decimal_to_string(row["price"]),
        "sample_second_ms": row["sample_second_ms"],
        "sample_second_at": utc_datetime_to_z(row["sample_second_at"]),
        "provider_event_ms": row["provider_event_ms"],
        "received_ms": row["received_ms"],
        "market_id": row["market_id"],
        "market_start_ms": row["market_start_ms"],
        "market_end_ms": row["market_end_ms"],
    }


def serialize_market_summary(summary: Mapping[str, Any], *, now_ms: int) -> dict[str, Any]:
    return {
        "provider": summary["provider"],
        "symbol": summary["symbol"],
        "market_id": summary["market_id"],
        "market_start_ms": summary["market_start_ms"],
        "market_end_ms": summary["market_end_ms"],
        "market_start_at": utc_datetime_to_z(summary["market_start_at"]),
        "market_end_at": utc_datetime_to_z(summary["market_end_at"]),
        "is_complete": now_ms >= summary["market_end_ms"],
        "sample_count": summary["sample_count"],
        "open": decimal_to_string(summary["open"]),
        "high": decimal_to_string(summary["high"]),
        "low": decimal_to_string(summary["low"]),
        "close": decimal_to_string(summary["close"]),
        "samples": [
            {
                "sample_second_ms": sample["sample_second_ms"],
                "sample_second_at": utc_datetime_to_z(sample["sample_second_at"]),
                "price": decimal_to_string(sample["price"]),
            }
            for sample in summary["samples"]
        ],
    }


def get_pool(request: Request) -> Any:
    return request.app.state.pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    pool = await create_read_pool(settings)
    app.state.settings = settings
    app.state.pool = pool
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
async def healthz(request: Request) -> JSONResponse:
    try:
        await health_check(get_pool(request))
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "database": "error",
                "service": SERVICE_NAME,
                "error": str(exc),
            },
        )

    return JSONResponse(
        {
            "ok": True,
            "database": "ok",
            "service": SERVICE_NAME,
        }
    )


@app.get("/prices/latest")
async def prices_latest(
    request: Request,
    provider: str = Query(DEFAULT_PROVIDER),
    symbol: str = Query(DEFAULT_SYMBOL),
) -> dict[str, Any]:
    row = await fetch_latest_price(get_pool(request), provider, symbol)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no latest price found for provider={provider!r}, symbol={symbol!r}",
        )

    return serialize_latest_price(row)


@app.get("/markets/latest")
async def markets_latest(
    request: Request,
    provider: str = Query(DEFAULT_PROVIDER),
    symbol: str = Query(DEFAULT_SYMBOL),
) -> dict[str, Any]:
    market_id = await fetch_latest_market_id(get_pool(request), provider, symbol)
    if market_id is None:
        raise HTTPException(
            status_code=404,
            detail=f"no market found for provider={provider!r}, symbol={symbol!r}",
        )

    return await market_by_id_response(
        request,
        provider=provider,
        symbol=symbol,
        market_id=market_id,
    )


@app.get("/markets/{market_id}")
async def markets_by_id(
    request: Request,
    market_id: int,
    provider: str = Query(DEFAULT_PROVIDER),
    symbol: str = Query(DEFAULT_SYMBOL),
) -> dict[str, Any]:
    return await market_by_id_response(
        request,
        provider=provider,
        symbol=symbol,
        market_id=market_id,
    )


async def market_by_id_response(
    request: Request,
    *,
    provider: str,
    symbol: str,
    market_id: int,
) -> dict[str, Any]:
    summary = await fetch_market_summary(get_pool(request), provider, symbol, market_id)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no samples found for provider={provider!r}, "
                f"symbol={symbol!r}, market_id={market_id!r}"
            ),
        )

    return serialize_market_summary(summary, now_ms=current_utc_epoch_ms())

