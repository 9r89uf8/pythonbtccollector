"""Shared API serialization for finalized Binance microstructure rows."""

from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional


MICROSTRUCTURE_GROUPS = (
    "books",
    "flow",
    "cross_market",
    "liquidations",
    "quality",
)

_BOOK_FIELDS = (
    ("bid", "bid"),
    ("ask", "ask"),
    ("mid", "mid"),
    ("spread_bps", "spread_bps"),
    ("imbalance_1", "imbalance_1"),
    ("imbalance_5", "imbalance_5"),
    ("imbalance_10", "imbalance_10"),
    ("bid_depth_usdt_10", "bid_depth_usdt_10"),
    ("ask_depth_usdt_10", "ask_depth_usdt_10"),
    ("weighted_mid_offset_bps", "weighted_mid_offset_bps"),
    ("bbo_ofi_usdt", "snapshot_bbo_ofi_usdt"),
)


def parse_microstructure_groups(value: Optional[str]) -> tuple[str, ...]:
    """Parse a comma-separated group selection into stable canonical order."""
    if value is None:
        return MICROSTRUCTURE_GROUPS

    requested = [part.strip() for part in value.split(",")]
    if not requested or any(not part for part in requested):
        raise ValueError(
            "microstructure_groups must be a comma-separated list of non-empty "
            "group names"
        )

    invalid = sorted(set(requested).difference(MICROSTRUCTURE_GROUPS))
    if invalid:
        allowed = ", ".join(MICROSTRUCTURE_GROUPS)
        raise ValueError(
            f"unknown microstructure_groups: {', '.join(invalid)}; "
            f"allowed groups: {allowed}"
        )

    requested_set = set(requested)
    return tuple(group for group in MICROSTRUCTURE_GROUPS if group in requested_set)


def _financial_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("microstructure financial values must not be binary floats")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("microstructure financial values must be finite")
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(
                "microstructure financial values must be decimal strings"
            ) from exc
        if not parsed.is_finite():
            raise ValueError("microstructure financial values must be finite")
        return format(parsed, "f")
    raise TypeError(
        "microstructure financial values must be Decimal, decimal strings, or null"
    )


def _integer_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("microstructure integer values must be integers or null")
    return value


def _serialize_book(row: Mapping[str, Any], *, prefix: str) -> dict[str, Any]:
    return {
        response_name: _financial_string(row.get(f"{prefix}_{column_suffix}"))
        for response_name, column_suffix in _BOOK_FIELDS
    }


def serialize_microstructure_row(
    row: Mapping[str, Any],
    *,
    groups: Iterable[str] = MICROSTRUCTURE_GROUPS,
) -> dict[str, Any]:
    """Convert a flat database/cache row to the nested dashboard contract."""
    selected = tuple(groups)
    invalid = sorted(set(selected).difference(MICROSTRUCTURE_GROUPS))
    if invalid:
        raise ValueError(f"unknown microstructure groups: {', '.join(invalid)}")

    collector_healthy = row.get("collector_healthy")
    if not isinstance(collector_healthy, bool):
        raise TypeError("collector_healthy must be a boolean")

    result: dict[str, Any] = {"collector_healthy": collector_healthy}

    if "books" in selected:
        result["books"] = {
            "spot": {
                **_serialize_book(row, prefix="spot"),
                "snapshot_count": _integer_or_none(
                    row.get("spot_book_snapshot_count")
                ),
            },
            "futures": {
                **_serialize_book(row, prefix="fut"),
                "snapshot_count": _integer_or_none(
                    row.get("fut_book_snapshot_count")
                ),
            },
        }

    if "flow" in selected:
        result["flow"] = {
            "spot_buy_usdt": _financial_string(row.get("spot_buy_usdt")),
            "spot_sell_usdt": _financial_string(row.get("spot_sell_usdt")),
            "futures_buy_usdt": _financial_string(row.get("fut_buy_usdt")),
            "futures_sell_usdt": _financial_string(row.get("fut_sell_usdt")),
            "futures_rpi_buy_usdt": _financial_string(
                row.get("fut_rpi_buy_usdt")
            ),
            "futures_rpi_sell_usdt": _financial_string(
                row.get("fut_rpi_sell_usdt")
            ),
            "spot_trade_id_span": _integer_or_none(row.get("spot_trade_id_span")),
            "spot_aggtrade_count": _integer_or_none(
                row.get("spot_aggtrade_count")
            ),
            "spot_max_aggtrade_usdt": _financial_string(
                row.get("spot_max_aggtrade_usdt")
            ),
            "spot_vwap": _financial_string(row.get("spot_vwap")),
            "spot_trade_high": _financial_string(row.get("spot_trade_high")),
            "spot_trade_low": _financial_string(row.get("spot_trade_low")),
            "spot_last_trade": _financial_string(row.get("spot_last_trade")),
            "futures_trade_id_span": _integer_or_none(
                row.get("fut_trade_id_span")
            ),
            "futures_aggtrade_count": _integer_or_none(
                row.get("fut_aggtrade_count")
            ),
            "futures_max_aggtrade_usdt": _financial_string(
                row.get("fut_max_aggtrade_usdt")
            ),
            "futures_vwap": _financial_string(row.get("fut_vwap")),
            "futures_trade_high": _financial_string(row.get("fut_trade_high")),
            "futures_trade_low": _financial_string(row.get("fut_trade_low")),
            "futures_last_trade": _financial_string(row.get("fut_last_trade")),
        }

    if "cross_market" in selected:
        result["cross_market"] = {
            "perp_spot_basis_bps": _financial_string(
                row.get("perp_spot_basis_bps")
            ),
            "spot_futures_book_skew_ms": _integer_or_none(
                row.get("spot_fut_book_skew_ms")
            ),
            "mark_price": _financial_string(row.get("mark_price")),
            "index_price": _financial_string(row.get("index_price")),
            "mark_index_basis_bps": _financial_string(
                row.get("mark_index_basis_bps")
            ),
            "funding_rate": _financial_string(row.get("funding_rate")),
            "seconds_to_funding": _integer_or_none(row.get("seconds_to_funding")),
            "open_interest_btc": _financial_string(
                row.get("open_interest_btc")
            ),
            "open_interest_usdt": _financial_string(
                row.get("open_interest_usdt")
            ),
        }

    if "liquidations" in selected:
        result["liquidations"] = {
            "observed_long_usdt": _financial_string(row.get("long_liq_usdt")),
            "observed_short_usdt": _financial_string(row.get("short_liq_usdt")),
            "snapshot_count": _integer_or_none(row.get("liq_snapshot_count")),
        }

    if "quality" in selected:
        result["quality"] = {
            "schema_version": _integer_or_none(row.get("schema_version")),
            "sample_span_ms": _integer_or_none(row.get("sample_span_ms")),
            "sample_jitter_ms": _integer_or_none(row.get("sample_jitter_ms")),
            "spot_book_age_ms": _integer_or_none(row.get("spot_book_age_ms")),
            "spot_book_lag_ms": _integer_or_none(row.get("spot_book_lag_ms")),
            "futures_book_age_ms": _integer_or_none(row.get("fut_book_age_ms")),
            "futures_book_lag_ms": _integer_or_none(row.get("fut_book_lag_ms")),
            "spot_trade_age_ms": _integer_or_none(row.get("spot_trade_age_ms")),
            "spot_trade_lag_mean_ms": _financial_string(
                row.get("spot_trade_lag_mean_ms")
            ),
            "spot_trade_lag_max_ms": _integer_or_none(
                row.get("spot_trade_lag_max_ms")
            ),
            "futures_trade_age_ms": _integer_or_none(row.get("fut_trade_age_ms")),
            "futures_trade_lag_mean_ms": _financial_string(
                row.get("fut_trade_lag_mean_ms")
            ),
            "futures_trade_lag_max_ms": _integer_or_none(
                row.get("fut_trade_lag_max_ms")
            ),
            "mark_age_ms": _integer_or_none(row.get("mark_age_ms")),
            "mark_lag_ms": _integer_or_none(row.get("mark_lag_ms")),
            "open_interest_age_ms": _integer_or_none(row.get("oi_age_ms")),
            "open_interest_exchange_age_ms": _integer_or_none(
                row.get("oi_exchange_age_ms")
            ),
            "open_interest_http_lag_ms": _integer_or_none(
                row.get("oi_http_lag_ms")
            ),
            "liquidation_lag_mean_ms": _financial_string(
                row.get("liq_lag_mean_ms")
            ),
            "connection_errors": _integer_or_none(row.get("connection_errors")),
            "received_ms": _integer_or_none(row.get("received_ms")),
        }

    return result


def merge_microstructure_history(
    payload: dict[str, Any],
    rows: Iterable[Mapping[str, Any]],
    *,
    groups: Iterable[str] = MICROSTRUCTURE_GROUPS,
) -> dict[str, Any]:
    """Attach finalized rows to an existing one-second market response grid."""
    selected = tuple(groups)
    rows_by_second = {
        int(row["sample_second_ms"]): row
        for row in rows
    }

    series = payload.get("series")
    if not isinstance(series, list):
        raise TypeError("market payload series must be a list")

    matched_seconds: set[int] = set()
    healthy_rows = 0
    for item in series:
        sample_second_ms = int(item["timestamp_ms"])
        row = rows_by_second.get(sample_second_ms)
        if row is None:
            item["microstructure"] = None
            continue

        item["microstructure"] = serialize_microstructure_row(
            row,
            groups=selected,
        )
        matched_seconds.add(sample_second_ms)
        if row.get("collector_healthy") is True:
            healthy_rows += 1

    market = payload.get("market")
    if not isinstance(market, Mapping):
        raise TypeError("market payload market metadata must be a mapping")
    seconds_expected = int(market["seconds_expected"])
    microstructure_rows = len(matched_seconds)

    availability = dict(payload.get("availability") or {})
    availability.update(
        {
            "microstructure_rows": microstructure_rows,
            "microstructure_healthy_rows": healthy_rows,
            "microstructure_missing_seconds": max(
                0,
                seconds_expected - microstructure_rows,
            ),
        }
    )
    payload["availability"] = availability
    payload["schema_version"] = max(3, int(payload.get("schema_version") or 0))
    return payload
