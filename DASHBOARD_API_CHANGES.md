# Dashboard API Changes: Prediction Removal

Effective with commit `362c91c` on July 22, 2026, both Chainlink prediction
engines and their stored evaluations were removed. This is the short migration
guide for dashboard consumers. The complete current API contract remains in
[`FRONTEND_API.md`](FRONTEND_API.md).

## What Is No Longer Available

These routes no longer exist:

| Method | Removed path | Former use |
| --- | --- | --- |
| `GET` | `/markets/current/live/challengers/chainlink-catchup-2s` | Live two-second projection |
| `GET` | `/markets/current/shadow-evaluations` | Current-market prediction evaluations |
| `GET` | `/markets/{market_id}/shadow-evaluations` | Historical-market prediction evaluations |
| `GET` | `/markets/current/shadow-evaluations/download` | Rounded current-market prediction download |
| `GET` | `/markets/{market_id}/shadow-evaluations/download` | Rounded historical prediction download |

Calling one of these paths now returns HTTP `404`:

```json
{
  "detail": "Not Found"
}
```

The following dashboard data is therefore unavailable:

- Two-second and three-second projected Chainlink prices
- The 2s/3s model selector and challenger status
- Predicted move, move in basis points, and predicted direction
- Model validity, status, state, invalid reasons, beta, lag, and horizon
- Forecast-time reference prices and model timing diagnostics
- Projected-versus-actual charts and forecast-error series
- MAE, RMSE, bias, baseline skill, and win/tie/loss metrics
- Prediction evaluation downloads and their model/version metadata

There is currently no replacement projection or forecast-evaluation endpoint.

## Changed Live Response

`GET /markets/current/live` remains available. It now returns actual live
source prices only. The top-level `signals` property has been removed entirely;
the server does not return `"signals": null` or an empty signals object.

Current response shape:

```json
{
  "server_time_ms": 1783988794075,
  "market_id": 5946629,
  "market_start_ms": 1783988700000,
  "market_end_ms": 1783989000000,
  "prices": {
    "binance_spot": {
      "value": "62310.12",
      "source_timestamp_ms": 1783988793900,
      "received_ms": 1783988793950,
      "source_age_ms": 175,
      "received_age_ms": 125,
      "provider_event_ms": 1783988793900
    },
    "chainlink": {
      "value": "62290.21096323273",
      "source_timestamp_ms": 1783988792000,
      "received_ms": 1783988793346,
      "source_age_ms": 2075,
      "received_age_ms": 729,
      "provider_event_ms": 1783988792000
    }
  },
  "futures": {
    "last": {
      "value": "62331.80",
      "source_timestamp_ms": 1783988793451,
      "received_ms": 1783988793638,
      "source_age_ms": 624,
      "received_age_ms": 437,
      "time_ms": 1783988793451
    }
  }
}
```

Important behavior:

- Prices are fixed-point JSON strings. Use a decimal library, not binary
  floating-point, for price or basis calculations.
- `provider_event_ms` and futures `time_ms` are compatibility aliases for
  `source_timestamp_ms`.
- A missing source still returns its price object, but every value, timestamp,
  age, and compatibility alias in that object is `null`.
- Stale cached values remain present while `source_age_ms` and
  `received_age_ms` grow. The dashboard should use those fields for freshness.
- The compatibility query parameter `max_chainlink_carry_forward_ms` is still
  accepted but does not filter or carry Redis values.

## Dashboard Field Migration

| Old dashboard input | Current source |
| --- | --- |
| `live.signals.chainlink_catchup.current_chainlink` | `live.prices.chainlink.value` |
| `live.signals.chainlink_catchup.futures_now` | `live.futures.last.value` |
| `live.signals.chainlink_catchup.projected_chainlink` | No replacement |
| `live.signals.chainlink_catchup.pending_move` | No replacement |
| `live.signals.chainlink_catchup.pending_move_bps` | No replacement |
| `live.signals.chainlink_catchup.direction` | No replacement |
| `live.signals.chainlink_catchup.valid/status/state` | No replacement |
| Challenger response `prediction` | No replacement |
| Shadow evaluation `points` and `performance` | No replacement |

The dashboard may calculate the current observed basis as:

```text
futures.last.value - prices.chainlink.value
```

Only calculate it when both values are non-null and sufficiently fresh. This
is an observed source-price difference, not a Chainlink projection.

## What the Dashboard Can Still Consume

- `GET /markets/current/live` for current Binance Spot, Polymarket Chainlink,
  and Binance futures last prices
- `GET /markets/current/data` and `GET /markets/{market_id}/data` for stored
  actual market series
- `GET /markets/current/download` and `GET /markets/{market_id}/download` for
  ordinary market-data JSON downloads
- The market discovery, source comparison, latest-price, market-summary, and
  health endpoints documented in [`FRONTEND_API.md`](FRONTEND_API.md)

The ordinary market-data response formats were not changed by prediction
removal.

## Live Error Responses

A Redis connection or read failure returns HTTP `503`:

```json
{
  "detail": "live cache unavailable"
}
```

A malformed actual source-price cache value also returns HTTP `503`:

```json
{
  "detail": "live cache payload invalid"
}
```

Prediction-specific null, malformed-payload, and model-validity behavior no
longer exists.
