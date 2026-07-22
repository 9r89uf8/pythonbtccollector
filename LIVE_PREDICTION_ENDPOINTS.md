# Live Chainlink Prediction Endpoints

This document covers only the two Redis-backed endpoints used to display the
live 3-second and 2-second Chainlink catch-up predictions:

| Lag choice | Endpoint | Prediction location | Role |
| --- | --- | --- | --- |
| 3 seconds | `GET /markets/current/live` | `signals.chainlink_catchup` | Accepted primary |
| 2 seconds | `GET /markets/current/live/challengers/chainlink-catchup-2s` | `prediction` | Prospective challenger |

Neither endpoint queries PostgreSQL. They return the newest short-lived Redis
state, not historical evaluation rows. The 2-second model is still an
unselected prospective challenger. Its active V2 model applies a calibrated
soft futures–Chainlink basis band; the accepted 3-second engine is unchanged.

## Access

The production API listens on the droplet at `127.0.0.1:9000`. With the SSH
tunnel already open, use:

```text
http://127.0.0.1:9000
```

For a browser dashboard, route `/api` through the same-origin backend or local
development proxy to the tunneled API. The API does not install CORS
middleware.

The examples below assume:

```bash
API_BASE_URL="http://127.0.0.1:9000"
```

All prices and financial values are JSON strings. Keep them as strings or use a
decimal-number library; do not parse them with JavaScript `Number` when exact
financial arithmetic matters. All fields ending in `_ms` are UTC epoch
milliseconds.

## Endpoint 1: Accepted 3-Second Prediction and Live Prices

```text
GET /markets/current/live
```

This endpoint reads the current Binance spot, Chainlink, Binance futures, and
accepted 3-second prediction together. Use it for the dashboard's actual-price
display regardless of which prediction lag is selected.

It accepts one optional compatibility parameter:

| Parameter | Default | Current behavior |
| --- | ---: | --- |
| `max_chainlink_carry_forward_ms` | `10000` | Accepted but does not currently filter or carry Redis values |

Request:

```bash
curl -fsS "${API_BASE_URL}/markets/current/live"
```

Representative valid response:

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
  },
  "signals": {
    "chainlink_catchup": {
      "schema_version": 1,
      "mode": "shadow",
      "selection_schema_version": 2,
      "selection_policy_version": "chronological_holdout_v2",
      "selection_fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
      "selection_artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699",
      "selection_evidence_end_ms": 1783983205028,
      "model_version": "catchup_ratio_l3000_b100",
      "beta": "1",
      "generated_ms": 1783988794005,
      "valid": true,
      "status": "valid",
      "invalid_reasons": [],
      "state": "anchored",
      "horizon_ms": 3000,
      "estimated_lag_ms": 3000,
      "current_chainlink": "62290.21096323273",
      "projected_chainlink": "62292.00981418305598931493660",
      "pending_move": "1.79885095032598931493660",
      "pending_move_bps": "0.2887854965506176800898399415",
      "direction": "up",
      "futures_now": "62331.80",
      "futures_reference": "62330.00",
      "chainlink_now_source_timestamp_ms": 1783988792000,
      "chainlink_now_received_ms": 1783988793346,
      "anchor_chainlink_source_timestamp_ms": 1783988792000,
      "anchor_chainlink_received_ms": 1783988793346,
      "futures_now_source_timestamp_ms": 1783988793451,
      "futures_now_received_ms": 1783988793638,
      "futures_reference_source_timestamp_ms": 1783988789826,
      "futures_reference_received_ms": 1783988790015,
      "futures_reference_target_ms": 1783988790346,
      "futures_reference_gap_ms": 331,
      "futures_received_age_ms": 367,
      "chainlink_received_age_ms": 659,
      "market_id": 5946629,
      "market_start_ms": 1783988700000,
      "market_end_ms": 1783989000000,
      "ms_to_market_end": 205995,
      "full_horizon_before_market_end": true,
      "signal_age_ms": 70
    }
  }
}
```

### Top-level and price fields

| Field | Meaning |
| --- | --- |
| `server_time_ms` | API time used to calculate ages and the top-level market |
| `market_id` | Current five-minute market ID at `server_time_ms` |
| `market_start_ms`, `market_end_ms` | Current half-open five-minute market window |
| `prices.binance_spot` | Latest Binance spot price object |
| `prices.chainlink` | Latest Chainlink RTDS price object |
| `futures.last` | Latest Binance USD-M `btcusdt@aggTrade.p` price object |
| `signals.chainlink_catchup` | Accepted 3-second prediction object, or `null` |

Every price object has the first five fields below. Spot and Chainlink add
`provider_event_ms`; futures adds `time_ms`.

| Field | Meaning |
| --- | --- |
| `value` | Decimal price string, or `null` when the Redis value is absent |
| `source_timestamp_ms` | Provider/source event time, or `null` |
| `received_ms` | Time the collector received the value locally, or `null` |
| `source_age_ms` | `max(0, server_time_ms - source_timestamp_ms)`, or `null` |
| `received_age_ms` | `max(0, server_time_ms - received_ms)`, or `null` |
| `provider_event_ms` | Spot/Chainlink-only compatibility alias for `source_timestamp_ms` |
| `time_ms` | Futures-only compatibility alias for `source_timestamp_ms` |

If a source-price key is missing, its object is still present and all of its
fields are `null`. The endpoint does not reject an old value; the dashboard
should use both age fields to mark it stale.

### Accepted-model-only fields

The 3-second prediction contains selection metadata proving which promoted
model artifact produced it:

| Field | Meaning |
| --- | --- |
| `selection_schema_version` | Accepted selection artifact schema |
| `selection_policy_version` | Selection policy identifier |
| `selection_fingerprint_sha256` | Frozen configuration fingerprint |
| `selection_artifact_sha256` | Full accepted artifact hash |
| `selection_evidence_end_ms` | End of evidence used before the model was promoted |
| `horizon_ms` | Forecast horizon; currently `3000` |
| `estimated_lag_ms` | Futures lookback/estimated lag; currently `3000` |

## Endpoint 2: Prospective 2-Second Challenger

```text
GET /markets/current/live/challengers/chainlink-catchup-2s
```

This endpoint reads only the separate 2-second challenger Redis value. It does
not return actual source prices, does not change the accepted prediction, and
has no query parameters.

Request:

```bash
curl -fsS \
  "${API_BASE_URL}/markets/current/live/challengers/chainlink-catchup-2s"
```

Representative valid response:

```json
{
  "schema_version": 1,
  "server_time_ms": 1783988794075,
  "market_id": 5946629,
  "market_start_ms": 1783988700000,
  "market_end_ms": 1783989000000,
  "publication_role": "challenger",
  "prediction": {
    "schema_version": 1,
    "mode": "shadow_candidate",
    "publication_role": "challenger",
    "experiment_version": "prospective_catchup_2s_basis_v2",
    "model_version": "catchup_v2_l2000_h2000_b100_basis5m",
    "beta": "1",
    "futures_lookback_ms": 2000,
    "forecast_horizon_ms": 2000,
    "generated_ms": 1783988794005,
    "target_ms": 1783988796005,
    "valid": true,
    "status": "valid",
    "invalid_reasons": [],
    "state": "basis_within_band",
    "current_chainlink": "62290.21096323273",
    "projected_chainlink": "62292.00981418305598931493660",
    "pending_move": "1.79885095032598931493660",
    "pending_move_bps": "0.2887854965506176800898399415",
    "direction": "up",
    "futures_now": "62331.80",
    "futures_reference": "62330.00",
    "chainlink_now_source_timestamp_ms": 1783988792000,
    "chainlink_now_received_ms": 1783988793346,
    "anchor_chainlink_source_timestamp_ms": 1783988792000,
    "anchor_chainlink_received_ms": 1783988793346,
    "futures_now_source_timestamp_ms": 1783988793451,
    "futures_now_received_ms": 1783988793638,
    "futures_reference_source_timestamp_ms": 1783988790826,
    "futures_reference_received_ms": 1783988791015,
    "futures_reference_target_ms": 1783988791346,
    "futures_reference_gap_ms": 331,
    "futures_received_age_ms": 367,
    "chainlink_received_age_ms": 659,
    "market_id": 5946629,
    "market_start_ms": 1783988700000,
    "market_end_ms": 1783989000000,
    "ms_to_market_end": 205995,
    "full_horizon_before_market_end": true,
    "signal_age_ms": 70
  }
}
```

The V2 challenger defines basis as `futures - Chainlink`. Its normal level is
the arithmetic mean of strictly prior 500 ms samples from the preceding five
minutes, with 600 samples required. The soft-band half-width is
`max($1, 0.75 * population standard deviation)`. The raw two-second projection
is unchanged inside the band; outside it, the final projection moves 50%
toward the nearest band edge. During warmup or if this basis calculation fails,
the worker publishes the raw lag projection.

The wire shape and nested schema version remain `1`. No basis diagnostics are
added to this endpoint. `projected_chainlink` is the final value after any soft
correction, and the move and direction fields are calculated from that value.
The exact calibration is frozen in
`price_collector/shadow_signal_2s_basis_calibration.json`.
Only V2 is published here. The worker keeps the raw
`catchup_v1_l2000_h2000_b100` model as a silent same-timestamp evaluation
comparator; V1 is available through historical reporting, not this live
endpoint.

When the challenger key is missing, expired, or malformed, the wrapper remains
HTTP `200` and `prediction` is `null`:

```json
{
  "schema_version": 1,
  "server_time_ms": 1783988794075,
  "market_id": 5946629,
  "market_start_ms": 1783988700000,
  "market_end_ms": 1783989000000,
  "publication_role": "challenger",
  "prediction": null
}
```

### Challenger-wrapper and challenger-only fields

| Field | Meaning |
| --- | --- |
| Top-level `schema_version` | Challenger endpoint wrapper schema; currently `1` |
| Top-level `publication_role` | Always `"challenger"` |
| `prediction` | Complete 2-second prediction object, or `null` |
| `prediction.publication_role` | Always `"challenger"` |
| `prediction.experiment_version` | Prospective experiment identifier |
| `prediction.futures_lookback_ms` | Futures lookback; fixed at `2000` |
| `prediction.forecast_horizon_ms` | Prediction horizon; fixed at `2000` |
| `prediction.target_ms` | `generated_ms + forecast_horizon_ms` |

## Fields Shared by Both Prediction Objects

The prediction objects use different timing-field names, but the fields below
have the same meaning and can be rendered by one component.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | integer | Signal payload schema |
| `mode` | string | `"shadow"` for the accepted model or `"shadow_candidate"` for the challenger |
| `model_version` | string | Exact model identity |
| `beta` | decimal string | Frozen move multiplier; currently `"1"` |
| `generated_ms` | integer | Time both inputs were locally available and the prediction was generated |
| `valid` | boolean | Whether the current prediction is usable |
| `status` | string | `"valid"` or the primary reason it is currently invalid |
| `invalid_reasons` | string array | All current invalidity reasons |
| `state` | string | Model state such as `basis_within_band`, `basis_adjusted_up`, `basis_adjusted_down`, `basis_warming_up`, `warming_up_futures_history`, or `waiting_for_new_chainlink_anchor` |
| `current_chainlink` | decimal string or `null` | Chainlink value used at forecast time |
| `projected_chainlink` | decimal string or `null` | Predicted Chainlink value at the model horizon; for V2 this is the final value after any basis-band correction |
| `pending_move` | decimal string or `null` | `projected_chainlink - current_chainlink` |
| `pending_move_bps` | decimal string or `null` | Pending move in basis points relative to current Chainlink |
| `direction` | `"up"`, `"down"`, `"flat"`, or `null` | Direction implied by `pending_move` |
| `futures_now` | decimal string or `null` | Latest futures price used by the model |
| `futures_reference` | decimal string or `null` | As-of futures price selected near the lookback target |
| `chainlink_now_source_timestamp_ms` | integer or `null` | Source time of the current Chainlink input |
| `chainlink_now_received_ms` | integer or `null` | Local receive time of the current Chainlink input |
| `anchor_chainlink_source_timestamp_ms` | integer or `null` | Source time of the Chainlink anchor |
| `anchor_chainlink_received_ms` | integer or `null` | Local receive time of the Chainlink anchor |
| `futures_now_source_timestamp_ms` | integer or `null` | Source time of the latest futures input |
| `futures_now_received_ms` | integer or `null` | Local receive time of the latest futures input |
| `futures_reference_source_timestamp_ms` | integer or `null` | Source time of the selected futures reference |
| `futures_reference_received_ms` | integer or `null` | Local receive time of the selected futures reference |
| `futures_reference_target_ms` | integer or `null` | Requested lookback boundary for the reference |
| `futures_reference_gap_ms` | integer or `null` | Distance between the selected reference and its requested boundary |
| `futures_received_age_ms` | integer or `null` | Futures receive age when the worker generated the signal |
| `chainlink_received_age_ms` | integer or `null` | Chainlink receive age when the worker generated the signal |
| `market_id` | integer | Generation-time five-minute market ID |
| `market_start_ms`, `market_end_ms` | integers | Generation-time market window |
| `ms_to_market_end` | integer | Time remaining in that window at generation |
| `full_horizon_before_market_end` | boolean | Whether the complete forecast horizon ends before or at the market boundary |
| `signal_age_ms` | integer | `max(0, server_time_ms - generated_ms)`, added by the API |

Typical invalid statuses include `warming_up`, `chainlink_unavailable`,
`futures_unavailable`, `chainlink_stale`, `futures_stale`,
`anchor_history_missing`, `anchor_reference_gap`, `timestamp_regression`, and
`model_error`.

## How to Implement the Dashboard Toggle

Always fetch `/markets/current/live` because it supplies the actual prices. If
the user selects 2 seconds, also fetch the challenger endpoint and replace only
the prediction selected for display.

```javascript
const SUPPORTED_LAGS_MS = new Set([2000, 3000]);

async function apiGet(path) {
  const response = await fetch(`/api${path}`, {
    headers: { Accept: "application/json" },
  });
  const contentType = response.headers.get("content-type") ?? "";
  const body = contentType.includes("application/json")
    ? await response.json()
    : await response.text();
  if (!response.ok) {
    const detail = typeof body === "object" && body !== null
      ? body.detail
      : body;
    throw new Error(detail || `HTTP ${response.status}`);
  }
  return body;
}

function normalizePrediction(rawPrediction) {
  if (rawPrediction == null) return null;

  const horizonMs =
    rawPrediction.horizon_ms ?? rawPrediction.forecast_horizon_ms;
  const lagMs =
    rawPrediction.estimated_lag_ms ?? rawPrediction.futures_lookback_ms;

  return {
    ...rawPrediction,
    horizon_ms: horizonMs,
    estimated_lag_ms: lagMs,
    target_ms:
      rawPrediction.target_ms ?? rawPrediction.generated_ms + horizonMs,
  };
}

async function loadLivePrediction(selectedLagMs) {
  if (!SUPPORTED_LAGS_MS.has(selectedLagMs)) {
    throw new Error(`Unsupported prediction lag: ${selectedLagMs}`);
  }

  const liveRequest = apiGet("/markets/current/live");
  const challengerRequest = selectedLagMs === 2000
    ? apiGet("/markets/current/live/challengers/chainlink-catchup-2s")
    : Promise.resolve(null);

  const [live, challenger] = await Promise.all([
    liveRequest,
    challengerRequest,
  ]);

  const rawPrediction = selectedLagMs === 2000
    ? challenger.prediction
    : live.signals.chainlink_catchup;

  return {
    server_time_ms: live.server_time_ms,
    market_id: live.market_id,
    market_start_ms: live.market_start_ms,
    market_end_ms: live.market_end_ms,
    prices: live.prices,
    futures: live.futures,
    selected_lag_ms: selectedLagMs,
    prediction: normalizePrediction(rawPrediction),
  };
}
```

Use the normalized result as follows:

```javascript
const snapshot = await loadLivePrediction(selectedLagMs);
const prediction = snapshot.prediction;

if (prediction == null) {
  // Worker disabled, key expired, or payload rejected.
  showPredictionUnavailable();
} else if (!prediction.valid) {
  // Never keep displaying an older valid projection.
  clearProjectedPrice();
  showPredictionStatus(prediction.status, prediction.invalid_reasons);
} else {
  showPrediction({
    modelVersion: prediction.model_version,
    lagMs: prediction.estimated_lag_ms,
    horizonMs: prediction.horizon_ms,
    currentChainlink: prediction.current_chainlink,
    projectedChainlink: prediction.projected_chainlink,
    pendingMove: prediction.pending_move,
    pendingMoveBps: prediction.pending_move_bps,
    direction: prediction.direction,
    signalAgeMs: prediction.signal_age_ms,
  });
}
```

Do not carry an earlier valid prediction forward when the newest value is
`null` or has `valid: false`.

## Market Boundaries

The response wrapper's market fields are calculated at `server_time_ms`. The
nested prediction's market fields were fixed at `generated_ms`. A prediction
can therefore briefly describe the preceding market immediately after a
five-minute boundary. Keep the nested generation-time context intact and use
`full_horizon_before_market_end` when deciding whether a prediction covers its
full horizon inside that market.

## HTTP and Availability Behavior

| Condition | `/markets/current/live` | 2-second challenger endpoint |
| --- | --- | --- |
| Prediction key absent or expired | HTTP `200`; `signals.chainlink_catchup: null` | HTTP `200`; `prediction: null` |
| Well-formed but invalid prediction | HTTP `200`; object with `valid: false` | HTTP `200`; object with `valid: false` |
| Malformed prediction payload | HTTP `200`; accepted prediction becomes `null` | HTTP `200`; challenger prediction becomes `null` |
| Missing source-price key | HTTP `200`; that price object's fields are `null` | Not applicable; source prices are not read |
| Malformed source-price payload | HTTP `503`; `{"detail":"live cache payload invalid"}` | Not applicable; source prices are not read |
| Redis connection/read failure | HTTP `503`; `{"detail":"live cache unavailable"}` | HTTP `503`; `{"detail":"live cache unavailable"}` |

Treat `null`, `valid: false`, and HTTP `503` as different states:

- `null` means no current decodable prediction is cached.
- `valid: false` means the worker is running and explicitly reports why the
  current prediction cannot be used.
- HTTP `503` means the API could not read the required live-cache path.
