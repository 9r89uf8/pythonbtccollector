from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any, Literal, Mapping, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from starlette.middleware.gzip import GZipMiddleware

from price_collector.collector import current_utc_epoch_ms
from price_collector.config import Settings
from price_collector.db import (
    create_read_pool,
    fetch_market_download_payload,
    fetch_latest_market_id,
    fetch_latest_price,
    fetch_market_microstructure_rows,
    fetch_market_summaries_for_btc_sources,
    fetch_market_summary,
    fetch_recent_market_windows,
    health_check,
)
from price_collector.live_cache import (
    BINANCE_SPOT_LIVE_KEY,
    CHAINLINK_LIVE_KEY,
    FUTURES_LIVE_KEY,
    LIVE_CACHE_READ_ERRORS,
    MICROSTRUCTURE_LIVE_KEY,
    LiveCachePayloadError,
    build_current_live_payload,
    create_live_cache,
)
from price_collector.market import market_for_sample_second
from price_collector.microstructure_api import (
    MICROSTRUCTURE_GROUPS,
    merge_microstructure_history,
    parse_microstructure_groups,
    serialize_microstructure_row,
)
from price_collector.flip_research import (
    FLIP_DEFINITION_VERSION,
    fetch_flip_distribution,
    fetch_flip_markets,
    fetch_market_flip_analysis,
)


DEFAULT_PROVIDER = "binance_spot"
DEFAULT_SYMBOL = "BTCUSDT"
SERVICE_NAME = "price-api"
FlipKind = Literal["any_crossing", "decisive_flip", "cutoff_reversal"]
FlipDirection = Literal["up_to_down", "down_to_up"]
FlipWinner = Literal["Up", "Down"]
FlipDataView = Literal["event_window", "full"]
DEFAULT_FLIP_EVENT_WINDOW_BEFORE_SECONDS = 30
MAX_FLIP_EVENT_WINDOW_BEFORE_SECONDS = 120
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


def _mapping_value(
    row: Mapping[str, Any],
    *names: str,
    default: Any = None,
) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return default


def _decimal_string_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("flip research decimal values must not be binary floats")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("flip research decimal values must be finite")
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        parsed = Decimal(value)
        if not parsed.is_finite():
            raise ValueError("flip research decimal values must be finite")
        return format(parsed, "f")
    raise TypeError("flip research decimal values must be Decimal strings or null")


def _integer_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("flip research integer values must not be booleans")
    return int(value)


def _serialize_decisive_flip(row: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    direction = _mapping_value(
        row,
        "decisive_flip_direction",
        "decisive_direction",
    )
    observed_ms_before_end = _mapping_value(
        row,
        "decisive_flip_ms_before_end",
        "decisive_observed_ms_before_end",
        "decisive_ms_before_end",
    )
    if direction is None and observed_ms_before_end is None:
        return None
    return {
        "direction": direction,
        "observed_ms_before_end": _integer_or_none(observed_ms_before_end),
    }


def _serialize_flip_archive(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": _mapping_value(row, "archive_status"),
        "source_microstructure_rows": _integer_or_none(
            _mapping_value(
                row,
                "source_microstructure_rows",
                "source_microstructure_row_count",
                "archive_source_row_count",
            )
        ),
        "archived_microstructure_rows": _integer_or_none(
            _mapping_value(
                row,
                "archived_microstructure_rows",
                "archived_microstructure_row_count",
                "archive_row_count",
                "archive_archived_row_count",
            )
        ),
        "retention_safe": _mapping_value(row, "retention_safe"),
        "archived_at_ms": _integer_or_none(
            _mapping_value(row, "archived_at_ms", "archived_ms")
        ),
    }


def serialize_flip_market_item(row: Mapping[str, Any]) -> dict[str, Any]:
    market_id = int(row["market_id"])
    return {
        "market_id": market_id,
        "market_start_ms": int(row["market_start_ms"]),
        "market_end_ms": int(row["market_end_ms"]),
        "evaluation_status": _mapping_value(row, "evaluation_status"),
        "price_to_beat": _decimal_string_or_none(
            _mapping_value(
                row,
                "price_to_beat",
                "official_open_price",
                "chainlink_open_price",
            )
        ),
        "official_close": _decimal_string_or_none(
            _mapping_value(
                row,
                "official_close",
                "official_close_price",
                "chainlink_close_price",
            )
        ),
        "winner": _mapping_value(row, "winner", "official_winner"),
        "matching_crossing_count": int(
            _mapping_value(row, "matching_crossing_count", default=0) or 0
        ),
        "total_crossing_count_last_20s": int(
            _mapping_value(
                row,
                "total_crossing_count_last_20s",
                "crossing_count",
                "observed_crossing_count",
                default=0,
            )
            or 0
        ),
        "first_crossing_ms_before_end": _integer_or_none(
            _mapping_value(
                row,
                "first_crossing_ms_before_end",
                "first_crossing_observed_ms_before_end",
            )
        ),
        "last_crossing_ms_before_end": _integer_or_none(
            _mapping_value(
                row,
                "last_crossing_ms_before_end",
                "last_crossing_observed_ms_before_end",
            )
        ),
        "decisive_flip": _serialize_decisive_flip(row),
        "archive": _serialize_flip_archive(row),
        "flip_detail_url": f"/markets/{market_id}/flips",
        "data_url": f"/markets/{market_id}/data",
        "evidence_url": f"/markets/{market_id}/flips/data",
        "evidence_download_url": f"/markets/{market_id}/flips/download",
    }


def serialize_flip_event(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_sequence": int(row["event_sequence"]),
        "direction": row["direction"],
        "previous_side": _mapping_value(row, "previous_side", "from_side"),
        "new_side": _mapping_value(row, "new_side", "to_side"),
        "previous_chainlink_price": _decimal_string_or_none(
            _mapping_value(
                row,
                "previous_chainlink_price",
                "previous_price",
                "from_price",
            )
        ),
        "new_chainlink_price": _decimal_string_or_none(
            _mapping_value(
                row,
                "new_chainlink_price",
                "new_price",
                "to_price",
            )
        ),
        "previous_sample_second_ms": _integer_or_none(
            _mapping_value(row, "previous_sample_second_ms")
        ),
        "new_sample_second_ms": _integer_or_none(
            _mapping_value(
                row,
                "new_sample_second_ms",
                "sample_second_ms",
            )
        ),
        "previous_provider_event_ms": _integer_or_none(
            _mapping_value(row, "previous_provider_event_ms")
        ),
        "new_provider_event_ms": _integer_or_none(
            _mapping_value(
                row,
                "new_provider_event_ms",
                "provider_event_ms",
            )
        ),
        "previous_received_ms": _integer_or_none(
            _mapping_value(row, "previous_received_ms")
        ),
        "new_received_ms": _integer_or_none(
            _mapping_value(row, "new_received_ms", "received_ms")
        ),
        "observation_gap_ms": _integer_or_none(
            _mapping_value(row, "observation_gap_ms")
        ),
        "observed_ms_before_end": int(row["observed_ms_before_end"]),
        "is_decisive": bool(row["is_decisive"]),
        "observation_precision": _mapping_value(
            row,
            "observation_precision",
            default="one_second_summary",
        ),
    }


def serialize_flip_cutoff(
    row: Mapping[str, Any],
    *,
    microstructure_groups: tuple[str, ...] = MICROSTRUCTURE_GROUPS,
) -> dict[str, Any]:
    microstructure = _mapping_value(row, "microstructure")
    if microstructure is not None and not isinstance(microstructure, Mapping):
        raise TypeError("cutoff microstructure must be a mapping or null")

    return {
        "seconds_before_end": int(row["seconds_before_end"]),
        "cutoff_ms": _integer_or_none(_mapping_value(row, "cutoff_ms")),
        "chainlink": {
            "price": _decimal_string_or_none(
                _mapping_value(
                    row,
                    "chainlink_price",
                    "causal_chainlink_price",
                )
            ),
            "sample_second_ms": _integer_or_none(
                _mapping_value(row, "chainlink_sample_second_ms")
            ),
            "provider_event_ms": _integer_or_none(
                _mapping_value(row, "chainlink_provider_event_ms")
            ),
            "received_ms": _integer_or_none(
                _mapping_value(row, "chainlink_received_ms")
            ),
            "age_ms": _integer_or_none(
                _mapping_value(
                    row,
                    "chainlink_age_ms",
                    "chainlink_source_age_ms",
                    "price_age_ms",
                )
            ),
            "received_age_ms": _integer_or_none(
                _mapping_value(row, "chainlink_received_age_ms")
            ),
            "fresh": _mapping_value(row, "chainlink_fresh"),
        },
        "signed_distance": _decimal_string_or_none(
            _mapping_value(
                row,
                "signed_distance",
                "price_distance",
                "signed_distance_to_price_to_beat",
            )
        ),
        "absolute_distance": _decimal_string_or_none(
            _mapping_value(
                row,
                "absolute_distance",
                "absolute_price_distance",
                "absolute_distance_to_price_to_beat",
            )
        ),
        "apparent_side": _mapping_value(row, "apparent_side"),
        "probabilities": {
            "up": {
                "bid": _decimal_string_or_none(
                    _mapping_value(row, "up_bid")
                ),
                "ask": _decimal_string_or_none(
                    _mapping_value(
                        row,
                        "up_probability",
                        "up_ask",
                    )
                ),
                "mid": _decimal_string_or_none(
                    _mapping_value(row, "up_mid")
                ),
                "normalized": _decimal_string_or_none(
                    _mapping_value(row, "up_prob_norm")
                ),
                "provider_event_ms": _integer_or_none(
                    _mapping_value(
                        row,
                        "up_probability_provider_event_ms",
                    )
                ),
                "received_ms": _integer_or_none(
                    _mapping_value(row, "up_probability_received_ms")
                ),
                "source_age_ms": _integer_or_none(
                    _mapping_value(row, "up_probability_source_age_ms")
                ),
                "received_age_ms": _integer_or_none(
                    _mapping_value(row, "up_probability_received_age_ms")
                ),
            },
            "down": {
                "bid": _decimal_string_or_none(
                    _mapping_value(row, "down_bid")
                ),
                "ask": _decimal_string_or_none(
                    _mapping_value(
                        row,
                        "down_probability",
                        "down_ask",
                    )
                ),
                "mid": _decimal_string_or_none(
                    _mapping_value(row, "down_mid")
                ),
                "normalized": _decimal_string_or_none(
                    _mapping_value(row, "down_prob_norm")
                ),
                "provider_event_ms": _integer_or_none(
                    _mapping_value(
                        row,
                        "down_probability_provider_event_ms",
                    )
                ),
                "received_ms": _integer_or_none(
                    _mapping_value(row, "down_probability_received_ms")
                ),
                "source_age_ms": _integer_or_none(
                    _mapping_value(row, "down_probability_source_age_ms")
                ),
                "received_age_ms": _integer_or_none(
                    _mapping_value(row, "down_probability_received_age_ms")
                ),
            },
            "sample_second_ms": _integer_or_none(
                _mapping_value(row, "probability_sample_second_ms")
            ),
            "provider_event_ms": _integer_or_none(
                _mapping_value(row, "probability_provider_event_ms")
            ),
            "received_ms": _integer_or_none(
                _mapping_value(row, "probability_received_ms")
            ),
            "age_ms": _integer_or_none(
                _mapping_value(
                    row,
                    "probability_age_ms",
                    "probability_source_age_ms",
                )
            ),
            "received_age_ms": _integer_or_none(
                _mapping_value(row, "probability_received_age_ms")
            ),
            "fresh": _mapping_value(row, "probability_fresh"),
        },
        "official_winner": _mapping_value(
            row,
            "official_winner",
            "winner",
        ),
        "flipped_after_cutoff": _mapping_value(row, "flipped_after_cutoff"),
        "microstructure_sample_second_ms": _integer_or_none(
            _mapping_value(row, "microstructure_sample_second_ms")
        ),
        "microstructure_available": _mapping_value(
            row,
            "microstructure_available",
            default=microstructure is not None,
        ),
        "microstructure": (
            None
            if microstructure is None
            else serialize_microstructure_row(
                microstructure,
                groups=microstructure_groups,
            )
        ),
        "quality_flags": list(
            _mapping_value(row, "quality_flags", default=()) or ()
        ),
    }


def serialize_market_flip_analysis(
    analysis: Mapping[str, Any],
    *,
    now_ms: int,
    microstructure_groups: tuple[str, ...] = MICROSTRUCTURE_GROUPS,
) -> dict[str, Any]:
    evaluation = analysis["evaluation"]
    events = analysis.get("events") or []
    cutoffs = analysis.get("cutoffs") or []
    market_id = int(evaluation["market_id"])
    return {
        "schema_version": 1,
        "definition_version": int(
            _mapping_value(
                evaluation,
                "definition_version",
                default=FLIP_DEFINITION_VERSION,
            )
        ),
        "server_time_ms": now_ms,
        "market": {
            "market_id": market_id,
            "market_start_ms": int(evaluation["market_start_ms"]),
            "market_end_ms": int(evaluation["market_end_ms"]),
            "price_to_beat": _decimal_string_or_none(
                _mapping_value(
                    evaluation,
                    "price_to_beat",
                    "official_open_price",
                    "chainlink_open_price",
                )
            ),
            "official_close": _decimal_string_or_none(
                _mapping_value(
                    evaluation,
                    "official_close",
                    "official_close_price",
                    "chainlink_close_price",
                )
            ),
            "winner": _mapping_value(
                evaluation,
                "winner",
                "official_winner",
            ),
        },
        "evaluation": {
            "status": _mapping_value(evaluation, "evaluation_status"),
            "observation_precision": _mapping_value(
                evaluation,
                "observation_precision",
            ),
            "analysis_start_ms": _integer_or_none(
                _mapping_value(evaluation, "analysis_start_ms")
            ),
            "analysis_end_ms": _integer_or_none(
                _mapping_value(evaluation, "analysis_end_ms")
            ),
            "crossing_count": int(
                _mapping_value(
                    evaluation,
                    "crossing_count",
                    "observed_crossing_count",
                    default=0,
                )
                or 0
            ),
            "touch_count": int(
                _mapping_value(evaluation, "touch_count", default=0) or 0
            ),
            "first_crossing_ms_before_end": _integer_or_none(
                _mapping_value(
                    evaluation,
                    "first_crossing_ms_before_end",
                    "first_crossing_observed_ms_before_end",
                )
            ),
            "last_crossing_ms_before_end": _integer_or_none(
                _mapping_value(
                    evaluation,
                    "last_crossing_ms_before_end",
                    "last_crossing_observed_ms_before_end",
                )
            ),
            "decisive_flip": _serialize_decisive_flip(evaluation),
            "chainlink": {
                "observation_count": int(
                    _mapping_value(
                        evaluation,
                        "chainlink_observation_count",
                        default=0,
                    )
                    or 0
                ),
                "strict_observation_count": int(
                    _mapping_value(
                        evaluation,
                        "chainlink_strict_observation_count",
                        default=0,
                    )
                    or 0
                ),
                "first_provider_event_ms": _integer_or_none(
                    _mapping_value(
                        evaluation,
                        "chainlink_first_provider_event_ms",
                    )
                ),
                "last_provider_event_ms": _integer_or_none(
                    _mapping_value(
                        evaluation,
                        "chainlink_last_provider_event_ms",
                    )
                ),
                "max_gap_ms": _integer_or_none(
                    _mapping_value(
                        evaluation,
                        "chainlink_max_gap_ms",
                        "max_observation_gap_ms",
                    )
                ),
            },
            "cutoff_coverage": {
                "chainlink_count": int(
                    _mapping_value(
                        evaluation,
                        "chainlink_cutoff_count",
                        default=0,
                    )
                    or 0
                ),
                "fresh_chainlink_count": int(
                    _mapping_value(
                        evaluation,
                        "fresh_chainlink_cutoff_count",
                        default=0,
                    )
                    or 0
                ),
                "probability_count": int(
                    _mapping_value(
                        evaluation,
                        "probability_cutoff_count",
                        default=0,
                    )
                    or 0
                ),
                "fresh_probability_count": int(
                    _mapping_value(
                        evaluation,
                        "fresh_probability_cutoff_count",
                        default=0,
                    )
                    or 0
                ),
                "microstructure_count": int(
                    _mapping_value(
                        evaluation,
                        "microstructure_cutoff_count",
                        default=0,
                    )
                    or 0
                ),
            },
            "quality_flags": list(
                _mapping_value(
                    evaluation,
                    "quality_flags",
                    default=(),
                )
                or ()
            ),
            "evaluated_at_ms": _integer_or_none(
                _mapping_value(evaluation, "evaluated_at_ms", "evaluated_ms")
            ),
        },
        "events": [
            serialize_flip_event(event)
            for event in events
        ],
        "cutoffs": [
            serialize_flip_cutoff(
                cutoff,
                microstructure_groups=microstructure_groups,
            )
            for cutoff in cutoffs
        ],
        "archive": _serialize_flip_archive(evaluation),
        "data_url": f"/markets/{market_id}/data",
        "evidence_url": f"/markets/{market_id}/flips/data",
        "evidence_download_url": f"/markets/{market_id}/flips/download",
    }


def _contains_non_null_data(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_contains_non_null_data(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_non_null_data(item) for item in value)
    return value is not None


def summarize_flip_series_availability(
    series: list[Mapping[str, Any]],
) -> dict[str, int]:
    def count_mapping(name: str) -> int:
        return sum(
            _contains_non_null_data(item.get(name))
            for item in series
        )

    return {
        "series_rows": len(series),
        "binance_price_rows": sum(
            (item.get("prices") or {}).get("binance") is not None
            for item in series
        ),
        "chainlink_price_rows": sum(
            (item.get("prices") or {}).get("chainlink") is not None
            for item in series
        ),
        "probability_rows": count_mapping("probabilities"),
        "futures_rows": count_mapping("futures"),
        "open_interest_rows": count_mapping("open_interest"),
        "flow_rows": count_mapping("flow"),
        "book_rows": count_mapping("book"),
        "microstructure_rows": sum(
            item.get("microstructure") is not None
            for item in series
        ),
        "microstructure_healthy_rows": sum(
            isinstance(item.get("microstructure"), Mapping)
            and item["microstructure"].get("collector_healthy") is True
            for item in series
        ),
    }


def select_flip_anchor_event(
    events: list[Mapping[str, Any]],
    *,
    event_sequence: Optional[int],
) -> tuple[Optional[Mapping[str, Any]], str]:
    if event_sequence is not None:
        selected = next(
            (
                event
                for event in events
                if int(event["event_sequence"]) == event_sequence
            ),
            None,
        )
        if selected is None:
            raise LookupError(
                f"no flip event_sequence={event_sequence} found"
            )
        return selected, "requested_event_sequence"

    decisive = next(
        (event for event in events if event.get("is_decisive") is True),
        None,
    )
    if decisive is not None:
        return decisive, "decisive_event"
    if not events:
        return None, "market_end_fallback"
    return (
        max(events, key=lambda event: int(event["event_sequence"])),
        "latest_event_fallback",
    )


def build_flip_evidence_bundle(
    analysis: Mapping[str, Any],
    market_payload: Mapping[str, Any],
    *,
    anchor_event: Optional[Mapping[str, Any]],
    anchor_reason: str,
    view: FlipDataView,
    before_seconds: int,
    requested_event_sequence: Optional[int],
    now_ms: int,
    microstructure_groups: tuple[str, ...] = MICROSTRUCTURE_GROUPS,
) -> dict[str, Any]:
    serialized_flip = serialize_market_flip_analysis(
        analysis,
        now_ms=now_ms,
        microstructure_groups=microstructure_groups,
    )
    payload_market = market_payload.get("market")
    full_series_value = market_payload.get("series")
    if not isinstance(payload_market, Mapping):
        raise TypeError("market evidence payload must contain market metadata")
    if not isinstance(full_series_value, list):
        raise TypeError("market evidence payload series must be a list")

    full_series = list(full_series_value)
    market_start_ms = int(payload_market["market_start_ms"])
    market_end_ms = int(payload_market["market_end_ms"])
    anchor_sample_second_ms = (
        market_end_ms
        if anchor_event is None
        else int(anchor_event["sample_second_ms"])
    )

    if view == "event_window":
        requested_start_ms = (
            anchor_sample_second_ms - before_seconds * 1_000
        )
        window_start_ms = max(market_start_ms, requested_start_ms)
        clipped_at_market_start = window_start_ms != requested_start_ms
    else:
        window_start_ms = market_start_ms
        clipped_at_market_start = False

    selected_series = [
        item
        for item in full_series
        if window_start_ms
        <= int(item["timestamp_ms"])
        < market_end_ms
    ]
    selected_microstructure_seconds = {
        int(item["timestamp_ms"])
        for item in selected_series
        if item.get("microstructure") is not None
    }

    compact_cutoffs: list[dict[str, Any]] = []
    reused_cutoff_microstructure_rows = 0
    for cutoff in serialized_flip["cutoffs"]:
        compact_cutoff = dict(cutoff)
        microstructure_second_ms = compact_cutoff.get(
            "microstructure_sample_second_ms"
        )
        can_reuse_series_row = (
            compact_cutoff.get("microstructure") is not None
            and microstructure_second_ms is not None
            and int(microstructure_second_ms)
            in selected_microstructure_seconds
        )
        if can_reuse_series_row:
            compact_cutoff.pop("microstructure", None)
            compact_cutoff["microstructure_reused_from_series"] = True
            reused_cutoff_microstructure_rows += 1
        else:
            compact_cutoff["microstructure_reused_from_series"] = False
        compact_cutoffs.append(compact_cutoff)

    market = {
        **dict(payload_market),
        "price_to_beat": serialized_flip["market"]["price_to_beat"],
        "official_close": serialized_flip["market"]["official_close"],
        "winner": serialized_flip["market"]["winner"],
    }
    serialized_anchor = (
        None
        if anchor_event is None
        else serialize_flip_event(anchor_event)
    )
    selected_availability = summarize_flip_series_availability(
        selected_series
    )
    full_availability = summarize_flip_series_availability(full_series)
    market_id = int(market["market_id"])
    evidence_query_parts = [f"view={view}"]
    if view == "event_window":
        evidence_query_parts.append(f"before_seconds={before_seconds}")
    if requested_event_sequence is not None:
        evidence_query_parts.append(
            f"event_sequence={requested_event_sequence}"
        )
    if microstructure_groups != MICROSTRUCTURE_GROUPS:
        evidence_query_parts.append(
            "microstructure_groups=" + ",".join(microstructure_groups)
        )
    evidence_query = "?" + "&".join(evidence_query_parts)

    result: dict[str, Any] = {
        "schema_version": 1,
        "definition_version": serialized_flip["definition_version"],
        "market_data_schema_version": int(
            market_payload.get("schema_version") or 0
        ),
        "data_scope": "curated_public_api",
        "server_time_ms": now_ms,
        "market": market,
        "selection": {
            "view": view,
            "anchor": {
                "selection_reason": anchor_reason,
                "sample_second_ms": anchor_sample_second_ms,
                "event": serialized_anchor,
            },
            "window": {
                "before_seconds": (
                    before_seconds if view == "event_window" else None
                ),
                "start_ms": window_start_ms,
                "end_ms_exclusive": market_end_ms,
                "row_count": len(selected_series),
                "rows_before_anchor": sum(
                    int(item["timestamp_ms"]) < anchor_sample_second_ms
                    for item in selected_series
                ),
                "rows_at_or_after_anchor_second": sum(
                    int(item["timestamp_ms"]) >= anchor_sample_second_ms
                    for item in selected_series
                ),
                "clipped_at_market_start": clipped_at_market_start,
            },
        },
        "availability": {
            "selected_window": selected_availability,
            "full_market": full_availability,
        },
        "flip": {
            "evaluation": serialized_flip["evaluation"],
            "events": serialized_flip["events"],
            "cutoffs": compact_cutoffs,
            "archive": serialized_flip["archive"],
            "cutoff_microstructure_rows_reused_from_series": (
                reused_cutoff_microstructure_rows
            ),
        },
        "series": selected_series,
        "navigation": {
            "older_page_cursor": market_id,
            "list_parameter": "before_market_id",
            "preserve_list_filters": True,
        },
        "links": {
            "flip_list": "/markets/flips",
            "flip_detail": f"/markets/{market_id}/flips",
            "evidence": (
                f"/markets/{market_id}/flips/data{evidence_query}"
            ),
            "evidence_download": (
                f"/markets/{market_id}/flips/download{evidence_query}"
            ),
            "full_market_data": (
                f"/markets/{market_id}/data"
                "?include_probabilities=true"
                "&include_futures=true"
                "&include_oi=true"
                "&include_flow=true"
                "&include_book=true"
                "&include_microstructure=true"
            ),
        },
    }
    if "previous_5m_oi_summary" in market_payload:
        result["previous_5m_oi_summary"] = market_payload[
            "previous_5m_oi_summary"
        ]
    return result


def serialize_flip_distribution(
    distribution: Mapping[str, Any],
    *,
    max_seconds: int,
    now_ms: int,
) -> dict[str, Any]:
    population = distribution.get("population") or {}
    return {
        "schema_version": 1,
        "definition_version": FLIP_DEFINITION_VERSION,
        "server_time_ms": now_ms,
        "max_seconds": max_seconds,
        "population": {
            "resolved_markets": int(population.get("resolved_markets") or 0),
            "eligible_markets": int(population.get("eligible_markets") or 0),
            "ambiguous_markets": int(population.get("ambiguous_markets") or 0),
            "markets_with_any_crossing": int(
                population.get("markets_with_any_crossing") or 0
            ),
        },
        "crossings_by_time": [
            {
                "from_ms_before_end": int(row["from_ms_before_end"]),
                "to_ms_before_end": int(row["to_ms_before_end"]),
                "crossing_event_count": int(row["crossing_event_count"]),
                "unique_market_count": int(row["unique_market_count"]),
                "decisive_flip_market_count": int(
                    row["decisive_flip_market_count"]
                ),
                "to_up_count": int(row["to_up_count"]),
                "to_down_count": int(row["to_down_count"]),
                "cumulative_unique_markets_within_window": int(
                    row["cumulative_unique_markets_within_window"]
                ),
                "cumulative_market_rate": _decimal_string_or_none(
                    row.get("cumulative_market_rate")
                ),
            }
            for row in distribution.get("crossings_by_time") or []
        ],
        "cutoff_reversals": [
            {
                "seconds_before_end": int(row["seconds_before_end"]),
                "eligible_markets": int(row["eligible_markets"]),
                "markets_reversed_by_close": int(
                    row["markets_reversed_by_close"]
                ),
                "reversal_rate": _decimal_string_or_none(
                    row.get("reversal_rate")
                ),
            }
            for row in distribution.get("cutoff_reversals") or []
        ],
    }


def serialize_download_series_item(item: Mapping[str, Any]) -> dict[str, Any]:
    exported = dict(item)
    exported.pop("freshness", None)
    exported.pop("timestamp_ms", None)

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


def serialize_download_market(market: Mapping[str, Any]) -> dict[str, Any]:
    exported = dict(market)
    exported.pop("market_start_ms", None)
    exported.pop("market_end_ms", None)

    chainlink_resolution = exported.get("chainlink_resolution")
    if isinstance(chainlink_resolution, Mapping):
        formatted_resolution = dict(chainlink_resolution)
        formatted_resolution["open"] = _format_download_decimal_string(
            formatted_resolution.get("open"),
            "0.01",
        )
        formatted_resolution["close"] = _format_download_decimal_string(
            formatted_resolution.get("close"),
            "0.01",
        )
        exported["chainlink_resolution"] = formatted_resolution

    return exported


def serialize_download_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    exported = {}
    for key, value in payload.items():
        if key == "series":
            exported[key] = [
                serialize_download_series_item(item)
                for item in value
            ]
        elif key == "market" and isinstance(value, Mapping):
            exported[key] = serialize_download_market(value)
        else:
            exported[key] = value

    return exported


def get_pool(request: Request) -> Any:
    return request.app.state.pool


def get_live_cache(request: Request) -> Any:
    return request.app.state.live_cache


def requested_microstructure_groups(
    *,
    include_microstructure: bool,
    microstructure_groups: Optional[str],
) -> tuple[str, ...]:
    if not include_microstructure:
        if microstructure_groups is not None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "microstructure_groups requires include_microstructure=true"
                ),
            )
        return ()

    try:
        return parse_microstructure_groups(microstructure_groups)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def validate_flip_date_range(
    *,
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> None:
    if start_ms is not None and end_ms is not None and start_ms >= end_ms:
        raise HTTPException(
            status_code=422,
            detail="start_ms must be less than end_ms",
        )


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
app.add_middleware(GZipMiddleware, minimum_size=1_000)


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


@app.get("/markets/flips")
async def markets_flips(
    request: Request,
    within_seconds: int = Query(20, ge=1, le=20),
    kind: FlipKind = Query("any_crossing"),
    direction: Optional[FlipDirection] = Query(None),
    winner: Optional[FlipWinner] = Query(None),
    limit: int = Query(20, ge=1, le=50),
    before_market_id: Optional[int] = Query(None, ge=0),
    start_ms: Optional[int] = Query(None, ge=0),
    end_ms: Optional[int] = Query(None, ge=0),
) -> dict[str, Any]:
    validate_flip_date_range(start_ms=start_ms, end_ms=end_ms)
    now_ms = current_utc_epoch_ms()
    rows = await fetch_flip_markets(
        get_pool(request),
        definition_version=FLIP_DEFINITION_VERSION,
        within_seconds=within_seconds,
        kind=kind,
        direction=direction,
        winner=winner,
        start_ms=start_ms,
        end_ms=end_ms,
        before_market_id=before_market_id,
        limit=limit + 1,
    )
    has_more = len(rows) > limit
    page_rows = rows[:limit]
    filters: dict[str, Any] = {
        "within_seconds": within_seconds,
        "kind": kind,
    }
    for name, value in (
        ("direction", direction),
        ("winner", winner),
        ("start_ms", start_ms),
        ("end_ms", end_ms),
    ):
        if value is not None:
            filters[name] = value

    return {
        "schema_version": 1,
        "definition_version": FLIP_DEFINITION_VERSION,
        "server_time_ms": now_ms,
        "filters": filters,
        "markets": [
            serialize_flip_market_item(row)
            for row in page_rows
        ],
        "next_before_market_id": (
            int(page_rows[-1]["market_id"])
            if has_more and page_rows
            else None
        ),
    }


@app.get("/markets/flips/distribution")
async def markets_flips_distribution(
    request: Request,
    max_seconds: int = Query(20, ge=1, le=20),
    direction: Optional[FlipDirection] = Query(None),
    start_ms: Optional[int] = Query(None, ge=0),
    end_ms: Optional[int] = Query(None, ge=0),
) -> dict[str, Any]:
    validate_flip_date_range(start_ms=start_ms, end_ms=end_ms)
    now_ms = current_utc_epoch_ms()
    distribution = await fetch_flip_distribution(
        get_pool(request),
        definition_version=FLIP_DEFINITION_VERSION,
        max_seconds=max_seconds,
        direction=direction,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    return serialize_flip_distribution(
        distribution,
        max_seconds=max_seconds,
        now_ms=now_ms,
    )


@app.get("/markets/{market_id}/flips")
async def markets_flips_by_id(
    request: Request,
    market_id: int,
) -> dict[str, Any]:
    analysis = await fetch_market_flip_analysis(
        get_pool(request),
        market_id=market_id,
        definition_version=FLIP_DEFINITION_VERSION,
    )
    if analysis is None:
        raise HTTPException(
            status_code=404,
            detail=f"no flip analysis found for market_id={market_id}",
        )
    return serialize_market_flip_analysis(
        analysis,
        now_ms=current_utc_epoch_ms(),
    )


@app.get("/markets/{market_id}/flips/data")
async def markets_flip_data_by_id(
    request: Request,
    market_id: int,
    view: FlipDataView = Query("event_window"),
    before_seconds: int = Query(
        DEFAULT_FLIP_EVENT_WINDOW_BEFORE_SECONDS,
        ge=0,
        le=MAX_FLIP_EVENT_WINDOW_BEFORE_SECONDS,
        description=(
            "One-second rows before the selected anchor; applied only "
            "when view=event_window"
        ),
    ),
    event_sequence: Optional[int] = Query(
        None,
        ge=1,
        description=(
            "Crossing to anchor; defaults to the decisive crossing and "
            "then the latest crossing, or market end when no event exists"
        ),
    ),
    microstructure_groups: Optional[str] = Query(
        None,
        description=(
            "Comma-separated microstructure groups: "
            + ", ".join(MICROSTRUCTURE_GROUPS)
        ),
    ),
) -> dict[str, Any]:
    selected_microstructure_groups = requested_microstructure_groups(
        include_microstructure=True,
        microstructure_groups=microstructure_groups,
    )
    return await market_flip_evidence_payload(
        request,
        market_id=market_id,
        view=view,
        before_seconds=before_seconds,
        event_sequence=event_sequence,
        microstructure_groups=selected_microstructure_groups,
    )


@app.get("/markets/{market_id}/flips/download")
async def markets_flip_download_by_id(
    request: Request,
    market_id: int,
    view: FlipDataView = Query("event_window"),
    before_seconds: int = Query(
        DEFAULT_FLIP_EVENT_WINDOW_BEFORE_SECONDS,
        ge=0,
        le=MAX_FLIP_EVENT_WINDOW_BEFORE_SECONDS,
        description=(
            "One-second rows before the selected anchor; applied only "
            "when view=event_window"
        ),
    ),
    event_sequence: Optional[int] = Query(
        None,
        ge=1,
        description=(
            "Crossing to anchor; defaults to the decisive crossing and "
            "then the latest crossing, or market end when no event exists"
        ),
    ),
    microstructure_groups: Optional[str] = Query(
        None,
        description=(
            "Comma-separated microstructure groups: "
            + ", ".join(MICROSTRUCTURE_GROUPS)
        ),
    ),
) -> Response:
    selected_microstructure_groups = requested_microstructure_groups(
        include_microstructure=True,
        microstructure_groups=microstructure_groups,
    )
    payload = await market_flip_evidence_payload(
        request,
        market_id=market_id,
        view=view,
        before_seconds=before_seconds,
        event_sequence=event_sequence,
        microstructure_groups=selected_microstructure_groups,
    )
    filename = f"btc_5m_flip_{market_id}_{view}.json"
    return Response(
        content=json.dumps(payload, default=str, separators=(",", ":")),
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
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
    include_flow: bool = Query(False),
    include_book: bool = Query(False),
    include_microstructure: bool = Query(False),
    microstructure_groups: Optional[str] = Query(
        None,
        description=(
            "Comma-separated microstructure groups: "
            + ", ".join(MICROSTRUCTURE_GROUPS)
        ),
    ),
    fill_display: bool = Query(False),
    max_carry_forward_ms: int = Query(10_000),
) -> dict[str, Any]:
    selected_microstructure_groups = requested_microstructure_groups(
        include_microstructure=include_microstructure,
        microstructure_groups=microstructure_groups,
    )
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
    if include_microstructure:
        microstructure_rows = await fetch_market_microstructure_rows(
            get_pool(request),
            market_id=window.market_id,
        )
        merge_microstructure_history(
            payload,
            microstructure_rows,
            groups=selected_microstructure_groups,
        )
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
    include_microstructure: bool = Query(False),
    microstructure_groups: Optional[str] = Query(
        None,
        description=(
            "Comma-separated microstructure groups: "
            + ", ".join(MICROSTRUCTURE_GROUPS)
        ),
    ),
    fill_display: bool = Query(False),
    max_carry_forward_ms: int = Query(10_000),
) -> dict[str, Any]:
    selected_microstructure_groups = requested_microstructure_groups(
        include_microstructure=include_microstructure,
        microstructure_groups=microstructure_groups,
    )
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
    if include_microstructure:
        microstructure_rows = await fetch_market_microstructure_rows(
            get_pool(request),
            market_id=market_id,
        )
        merge_microstructure_history(
            payload,
            microstructure_rows,
            groups=selected_microstructure_groups,
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


@app.get("/markets/current/microstructure/live")
async def markets_current_microstructure_live(
    request: Request,
) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    current_sample_second_ms = (now_ms // 1_000) * 1_000
    current_window = market_for_sample_second(current_sample_second_ms)

    try:
        live_cache = get_live_cache(request)
        prices, snapshot = await live_cache.get_prices_with_microstructure(
            [
                BINANCE_SPOT_LIVE_KEY,
                CHAINLINK_LIVE_KEY,
                FUTURES_LIVE_KEY,
            ],
            microstructure_key=MICROSTRUCTURE_LIVE_KEY,
        )
        snapshot_sample_second_ms = (
            None
            if snapshot is None
            else int(snapshot["sample_second_ms"])
        )
        snapshot_window = (
            current_window
            if snapshot_sample_second_ms is None
            else market_for_sample_second(snapshot_sample_second_ms)
        )
        serialized_snapshot = (
            None
            if snapshot is None
            else serialize_microstructure_row(snapshot)
        )
    except LIVE_CACHE_READ_ERRORS as exc:
        raise HTTPException(status_code=503, detail="live cache unavailable") from exc
    except (LiveCachePayloadError, TypeError, ValueError, KeyError) as exc:
        raise HTTPException(status_code=503, detail="live cache payload invalid") from exc

    return {
        "schema_version": 1,
        "server_time_ms": now_ms,
        "market_id": snapshot_window.market_id,
        "sample_second_ms": snapshot_sample_second_ms,
        "served_from": "redis",
        "prices": {
            "binance_spot": (
                None
                if prices.get(BINANCE_SPOT_LIVE_KEY) is None
                else prices[BINANCE_SPOT_LIVE_KEY].value
            ),
            "chainlink": (
                None
                if prices.get(CHAINLINK_LIVE_KEY) is None
                else prices[CHAINLINK_LIVE_KEY].value
            ),
            "futures": (
                None
                if prices.get(FUTURES_LIVE_KEY) is None
                else prices[FUTURES_LIVE_KEY].value
            ),
        },
        "microstructure": serialized_snapshot,
    }


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


async def market_flip_evidence_payload(
    request: Request,
    *,
    market_id: int,
    view: FlipDataView,
    before_seconds: int,
    event_sequence: Optional[int],
    microstructure_groups: tuple[str, ...],
) -> dict[str, Any]:
    now_ms = current_utc_epoch_ms()
    analysis = await fetch_market_flip_analysis(
        get_pool(request),
        market_id=market_id,
        definition_version=FLIP_DEFINITION_VERSION,
    )
    if analysis is None:
        raise HTTPException(
            status_code=404,
            detail=f"no flip analysis found for market_id={market_id}",
        )

    events = list(analysis.get("events") or [])
    try:
        anchor_event, anchor_reason = select_flip_anchor_event(
            events,
            event_sequence=event_sequence,
        )
    except LookupError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"{exc} for market_id={market_id}",
        ) from exc

    market_payload = await fetch_market_download_payload(
        get_pool(request),
        market_id=market_id,
        server_time_ms=now_ms,
        include_probabilities=True,
        include_futures=True,
        include_oi=True,
        include_flow=True,
        include_book=True,
        fill_display=False,
        max_carry_forward_ms=10_000,
    )
    if market_payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"no market data found for market_id={market_id}",
        )

    microstructure_rows = await fetch_market_microstructure_rows(
        get_pool(request),
        market_id=market_id,
    )
    merge_microstructure_history(
        market_payload,
        microstructure_rows,
        groups=microstructure_groups,
    )
    return build_flip_evidence_bundle(
        analysis,
        market_payload,
        anchor_event=anchor_event,
        anchor_reason=anchor_reason,
        view=view,
        before_seconds=before_seconds,
        requested_event_sequence=event_sequence,
        now_ms=now_ms,
        microstructure_groups=microstructure_groups,
    )


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
