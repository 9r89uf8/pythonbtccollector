from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
from typing import Any, Mapping, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from price_collector.collector import current_utc_epoch_ms
from price_collector.config import Settings
from price_collector.db import (
    create_read_pool,
    fetch_market_download_payload,
    fetch_latest_market_id,
    fetch_latest_price,
    fetch_market_summaries_for_btc_sources,
    fetch_market_summary,
    health_check,
)
from price_collector.live_cache import (
    LIVE_CACHE_READ_ERRORS,
    LiveCachePayloadError,
    build_current_live_payload,
    create_live_cache,
)
from price_collector.market import market_for_sample_second


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


def serialize_market_sources_summary(
    summary: Mapping[str, Any],
    *,
    now_ms: int,
) -> dict[str, Any]:
    return {
        "market_id": summary["market_id"],
        "market_start_ms": summary["market_start_ms"],
        "market_end_ms": summary["market_end_ms"],
        "market_start_at": utc_datetime_to_z(summary["market_start_at"]),
        "market_end_at": utc_datetime_to_z(summary["market_end_at"]),
        "is_complete": now_ms >= summary["market_end_ms"],
        "sources": [
            {
                "provider": source["provider"],
                "symbol": source["symbol"],
                "quote_asset": source["quote_asset"],
                "sample_count": source["sample_count"],
                "open": decimal_to_string(source["open"]),
                "high": decimal_to_string(source["high"]),
                "low": decimal_to_string(source["low"]),
                "close": decimal_to_string(source["close"]),
                "latest_sample_second_ms": source["latest_sample_second_ms"],
                "latest_provider_event_ms": source["latest_provider_event_ms"],
                "latest_received_ms": source["latest_received_ms"],
            }
            for source in summary["sources"]
        ],
    }


def serialize_download_series_item(item: Mapping[str, Any]) -> dict[str, Any]:
    exported = dict(item)
    exported.pop("freshness", None)

    prices = dict(exported.get("prices") or {})
    futures = exported.get("futures")
    if isinstance(futures, Mapping):
        prices["futures"] = futures.get("last")
    exported["prices"] = prices

    return exported


def serialize_download_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    exported = {}
    for key, value in payload.items():
        if key == "series":
            exported[key] = [
                serialize_download_series_item(item)
                for item in value
            ]
        else:
            exported[key] = value

    return exported


def get_pool(request: Request) -> Any:
    return request.app.state.pool


def get_live_cache(request: Request) -> Any:
    return request.app.state.live_cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    pool = await create_read_pool(settings)
    live_cache = create_live_cache(settings)
    app.state.settings = settings
    app.state.pool = pool
    app.state.live_cache = live_cache
    try:
        yield
    finally:
        await live_cache.close()
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


@app.get("/markets/current/sources")
async def markets_current_sources(request: Request) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    sample_second_ms = (now_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    return await market_sources_response(
        request,
        market_id=window.market_id,
        now_ms=now_ms,
    )


@app.get("/markets/{market_id}/sources")
async def markets_sources_by_id(
    request: Request,
    market_id: int,
) -> dict[str, Any]:
    return await market_sources_response(
        request,
        market_id=market_id,
        now_ms=current_utc_epoch_ms(),
    )


@app.get("/markets/current/data")
async def markets_current_data(
    request: Request,
    include_probabilities: bool = Query(False),
    include_futures: bool = Query(False),
    include_oi: bool = Query(False),
    fill_display: bool = Query(False),
    max_carry_forward_ms: int = Query(10_000),
) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    sample_second_ms = (now_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=window.market_id,
        server_time_ms=now_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
        fill_display=fill_display,
        max_carry_forward_ms=max_carry_forward_ms,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="no current market data found")
    return payload


@app.get("/markets/{market_id}/data")
async def markets_data_by_id(
    request: Request,
    market_id: int,
    include_probabilities: bool = Query(False),
    include_futures: bool = Query(False),
    include_oi: bool = Query(False),
    fill_display: bool = Query(False),
    max_carry_forward_ms: int = Query(10_000),
) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=market_id,
        server_time_ms=now_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
        fill_display=fill_display,
        max_carry_forward_ms=max_carry_forward_ms,
    )
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"no market data found for market_id={market_id}",
        )
    return payload


@app.get("/markets/current/download")
async def markets_current_download(
    request: Request,
    include_probabilities: bool = Query(False),
    include_futures: bool = Query(False),
    include_oi: bool = Query(False),
    fill_display: bool = Query(False),
    max_carry_forward_ms: int = Query(10_000),
) -> Response:
    now_ms = current_utc_epoch_ms()
    sample_second_ms = (now_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    return await market_download_response(
        request,
        market_id=window.market_id,
        server_time_ms=now_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
        fill_display=fill_display,
        max_carry_forward_ms=max_carry_forward_ms,
    )


@app.get("/markets/{market_id}/download")
async def markets_download_by_id(
    request: Request,
    market_id: int,
    include_probabilities: bool = Query(False),
    include_futures: bool = Query(False),
    include_oi: bool = Query(False),
    fill_display: bool = Query(False),
    max_carry_forward_ms: int = Query(10_000),
) -> Response:
    now_ms = current_utc_epoch_ms()
    return await market_download_response(
        request,
        market_id=market_id,
        server_time_ms=now_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
        fill_display=fill_display,
        max_carry_forward_ms=max_carry_forward_ms,
    )


@app.get("/markets/current/live")
async def markets_current_live(
    request: Request,
    max_chainlink_carry_forward_ms: int = Query(10_000),
) -> dict[str, Any]:
    _ = max_chainlink_carry_forward_ms
    now_ms = current_utc_epoch_ms()
    sample_second_ms = (now_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)

    try:
        return await build_current_live_payload(
            get_live_cache(request),
            window=window,
            server_time_ms=now_ms,
        )
    except LIVE_CACHE_READ_ERRORS as exc:
        raise HTTPException(status_code=503, detail="live cache unavailable") from exc
    except LiveCachePayloadError as exc:
        raise HTTPException(status_code=503, detail="live cache payload invalid") from exc


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


async def market_sources_response(
    request: Request,
    *,
    market_id: int,
    now_ms: int,
) -> dict[str, Any]:
    summary = await fetch_market_summaries_for_btc_sources(get_pool(request), market_id)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"no BTC source samples found for market_id={market_id!r}",
        )

    return serialize_market_sources_summary(summary, now_ms=now_ms)


async def market_download_response(
    request: Request,
    *,
    market_id: int,
    server_time_ms: int,
    include_probabilities: bool,
    include_futures: bool,
    include_oi: bool,
    fill_display: bool,
    max_carry_forward_ms: int,
) -> Response:
    payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=market_id,
        server_time_ms=server_time_ms,
        include_probabilities=include_probabilities,
        include_futures=include_futures,
        include_oi=include_oi,
        fill_display=fill_display,
        max_carry_forward_ms=max_carry_forward_ms,
    )
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"no market data found for market_id={market_id}",
        )

    parts = ["btc_5m_market", str(market_id)]
    if include_futures:
        parts.append("futures")
    if include_oi:
        parts.append("oi")
    if include_probabilities:
        parts.append("probabilities")
    filename = "_".join(parts) + ".json"
    download_payload = serialize_download_payload(payload)

    return Response(
        content=json.dumps(download_payload, default=str, separators=(",", ":")),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
