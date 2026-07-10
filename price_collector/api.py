from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
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
    fetch_recent_market_windows,
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
DOWNLOAD_FLOW_FIELDS = (
    "taker_imbalance",
    "cvd_10s",
    "cvd_30s",
    "imbalance_10s",
    "imbalance_30s",
)
DOWNLOAD_BOOK_FIELDS = (
    "book_imbalance",
    "microprice",
)


def utc_datetime_to_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def decimal_to_string(value: Decimal) -> str:
    return format(value, "f")


def _format_download_decimal_string(value: Any, places: str) -> Any:
    if value is None or not isinstance(value, str):
        return value
    quantized = Decimal(value).quantize(Decimal(places), rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def serialize_download_flow(flow: Mapping[str, Any]) -> dict[str, Any]:
    exported = {
        key: flow.get(key)
        for key in DOWNLOAD_FLOW_FIELDS
    }
    exported["taker_imbalance"] = _format_download_decimal_string(
        exported["taker_imbalance"],
        "0.0000",
    )
    exported["cvd_10s"] = _format_download_decimal_string(exported["cvd_10s"], "0.01")
    exported["cvd_30s"] = _format_download_decimal_string(exported["cvd_30s"], "0.01")
    exported["imbalance_10s"] = _format_download_decimal_string(
        exported["imbalance_10s"],
        "0.0000",
    )
    exported["imbalance_30s"] = _format_download_decimal_string(
        exported["imbalance_30s"],
        "0.0000",
    )
    return exported


def serialize_download_book(book: Mapping[str, Any]) -> dict[str, Any]:
    exported = {
        key: book.get(key)
        for key in DOWNLOAD_BOOK_FIELDS
    }
    exported["book_imbalance"] = _format_download_decimal_string(
        exported["book_imbalance"],
        "0.0000",
    )
    exported["microprice"] = _format_download_decimal_string(
        exported["microprice"],
        "0.01",
    )
    return exported


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


def serialize_market_index_item(
    row: Mapping[str, Any],
    *,
    now_ms: int,
) -> dict[str, Any]:
    return {
        "market_id": int(row["market_id"]),
        "market_start_ms": int(row["market_start_ms"]),
        "market_end_ms": int(row["market_end_ms"]),
        "market_start_at": utc_datetime_to_z(row["market_start_at"]),
        "market_end_at": utc_datetime_to_z(row["market_end_at"]),
        "is_complete": now_ms >= int(row["market_end_ms"]),
        "availability": {
            "binance": int(row.get("binance_sample_count") or 0),
            "chainlink": int(row.get("chainlink_sample_count") or 0),
            "futures": int(row.get("futures_sample_count") or 0),
            "open_interest": int(row.get("open_interest_sample_count") or 0),
            "flow": int(row.get("flow_sample_count") or 0),
            "book": int(row.get("book_sample_count") or 0),
            "probabilities": int(row.get("probability_sample_count") or 0),
        },
    }


def serialize_download_series_item(item: Mapping[str, Any]) -> dict[str, Any]:
    exported = dict(item)
    exported.pop("freshness", None)

    prices = dict(exported.get("prices") or {})
    futures = exported.get("futures")
    if isinstance(futures, Mapping):
        prices["futures"] = futures.get("last")
    exported["prices"] = prices
    exported.pop("futures", None)

    flow = exported.get("flow")
    if isinstance(flow, Mapping):
        exported["flow"] = serialize_download_flow(flow)

    book = exported.get("book")
    if isinstance(book, Mapping):
        exported["book"] = serialize_download_book(book)

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


@app.get("/markets")
async def markets_index(
    request: Request,
    limit: int = Query(3, ge=1, le=50),
    include_current: bool = Query(False),
    before_market_id: Optional[int] = Query(None, ge=0),
) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    rows = await fetch_recent_market_windows(
        get_pool(request),
        server_time_ms=now_ms,
        include_current=include_current,
        before_market_id=before_market_id,
        limit=limit + 1,
    )
    has_more = len(rows) > limit
    page_rows = rows[:limit]

    return {
        "schema_version": 1,
        "server_time_ms": now_ms,
        "markets": [
            serialize_market_index_item(row, now_ms=now_ms)
            for row in page_rows
        ],
        "next_before_market_id": (
            int(page_rows[-1]["market_id"])
            if has_more and page_rows
            else None
        ),
    }


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
    include_flow: bool = Query(False),
    include_book: bool = Query(False),
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
        include_flow=include_flow,
        include_book=include_book,
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
    include_flow: bool = Query(False),
    include_book: bool = Query(False),
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
        include_flow=include_flow,
        include_book=include_book,
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
    include_flow: bool = Query(False),
    include_book: bool = Query(False),
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
        include_flow=include_flow,
        include_book=include_book,
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
    include_flow: bool = Query(False),
    include_book: bool = Query(False),
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
        include_flow=include_flow,
        include_book=include_book,
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
    include_flow: bool,
    include_book: bool,
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
        include_flow=include_flow,
        include_book=include_book,
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
    if include_flow:
        parts.append("flow")
    if include_book:
        parts.append("book")
    if include_probabilities:
        parts.append("probabilities")
    filename = "_".join(parts) + ".json"
    download_payload = serialize_download_payload(payload)

    return Response(
        content=json.dumps(download_payload, default=str, separators=(",", ":")),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
