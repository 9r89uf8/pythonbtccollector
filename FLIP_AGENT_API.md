# Flip Market Agent API

This guide is for agents that review one BTC five-minute flip market at a time,
then advance to the next matching market. It focuses on the compact
event-relative response, the complete five-minute response, and the matching
JSON download.

For the complete flip-research field reference, see
[`FLIP_RESEARCH_API.md`](FLIP_RESEARCH_API.md). For microstructure group fields,
see [`MICROSTRUCTURE_API.md`](MICROSTRUCTURE_API.md).

## Endpoints

| Method | Path | Use |
| --- | --- | --- |
| `GET` | `/markets/flips` | List matching flip markets newest first |
| `GET` | `/markets/{market_id}/flips/data` | Return one agent-ready evidence bundle |
| `GET` | `/markets/{market_id}/flips/download` | Download the identical bundle as an attachment |

All three routes are read-only and PostgreSQL-backed. They do not use Redis.

The API is private and binds to `127.0.0.1:9000` on the droplet. From another
machine, open an SSH tunnel:

```bash
ssh -N -L 19000:127.0.0.1:9000 root@152.42.247.86
```

Then use:

```bash
API_BASE_URL="http://127.0.0.1:19000"
```

Do not expose droplet port `9000` publicly.

## Recommended Agent Loop

Start with one matching market:

```bash
curl --get "${API_BASE_URL}/markets/flips" \
  --data-urlencode "within_seconds=20" \
  --data-urlencode "kind=any_crossing" \
  --data-urlencode "limit=1"
```

Take `markets[0].market_id` and request its compact evidence:

```bash
curl --compressed \
  "${API_BASE_URL}/markets/5944864/flips/data"
```

After reviewing it, use `navigation.older_page_cursor` as the exclusive
`before_market_id` cursor on another `/markets/flips?limit=1` request. Preserve
every other list filter unchanged:

```bash
curl --get "${API_BASE_URL}/markets/flips" \
  --data-urlencode "within_seconds=20" \
  --data-urlencode "kind=any_crossing" \
  --data-urlencode "limit=1" \
  --data-urlencode "before_market_id=5944864"
```

Do not calculate the next saved flip as `market_id - 1`. Most five-minute
markets are not necessarily members of the filtered flip result set. Stop when
the next list request returns `markets: []`; the list response can also report
a null `next_before_market_id` when its current page has no older match.

The default `any_crossing` search can return both `confirmed_flip` and
`ambiguous` evaluations. Always inspect `flip.evaluation.status`. An ambiguous
market contains useful crossing evidence, but it must not be presented as a
confirmed flip.

## Evidence Request

```http
GET /markets/{market_id}/flips/data
```

The download route accepts the same parameters:

```http
GET /markets/{market_id}/flips/download
```

### Query parameters

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `view` | `event_window` or `full` | `event_window` | Select a compact event-relative slice or all 300 market seconds |
| `before_seconds` | integer `0`-`120` | `30` | Number of complete one-second slots before the anchor; used only by `event_window` |
| `event_sequence` | integer `>= 1` or omitted | omitted | Anchor the window to one exact saved crossing |
| `microstructure_groups` | comma-separated string or omitted | all groups | Select microstructure groups included in `series` |

Valid microstructure groups are:

- `books`
- `flow`
- `cross_market`
- `liquidations`
- `quality`

Group names are case-sensitive. Group filtering changes the nested
microstructure fields, not the reported microstructure row counts.

`before_seconds` is ignored when `view=full`. It does not extend a response
outside the five-minute market.

## Compact Event Window

`view=event_window` is designed for an agent that needs the important part of a
market without reading the unrelated first minutes.

When `event_sequence` is omitted, the API anchors the response to:

1. the decisive crossing, when one exists; otherwise
2. the latest saved crossing; otherwise
3. the market end for an evaluation with no crossing events.

When `event_sequence` is supplied, that exact crossing is the anchor, whether
or not it is decisive.

For a crossing anchor, the anchor time is the crossing's new exact TWAP event.
It is an observed strict-side transition, not an interpolated instant at which
the price must have crossed the threshold. The complete crossing bracket
remains in `selection.anchor.event` and `flip.events`, including the previous
and new provider timestamps and exact E18 TWAP prices.

For a no-event evaluation, `selection.anchor.sample_second_ms` is
`market_end_ms`, `selection.anchor.event` is `null`, and the reason is
`market_end_fallback`. The default compact response then contains the final
`before_seconds` rows. This also lets a caller inspect a saved non-flip
evaluation directly. `view=full` works with or without crossing events.

The selected series uses the anchor's saved sample-second:

```text
window_start_ms =
  max(market_start_ms, anchor_sample_second_ms - before_seconds * 1000)

window_end_ms = market_end_ms
```

The interval is half-open:

```text
[window_start_ms, window_end_ms)
```

It contains the requested complete slots before the anchor, the anchor slot,
and every remaining slot through the end of the market. The anchor slot is
outside the interval when the no-event anchor is exactly `market_end_ms`. With
the default 30-second lookback and a crossing in the evaluated final 20
seconds, the result is normally 31 to 50 rows instead of 300. A no-event
fallback returns the final 30 rows.

Rows keep their original `t` offsets from the market start. They are not
renumbered to start at zero.

The cutter applies only to `series`. `flip.events` still contains every saved
crossing, and `flip.cutoffs` still contains all twenty T-20 through T-1
records.

Rows counted at or after the anchor are keyed one-second summaries at or after
the crossing observation's floored source second. They are not proof that every
event aggregated into the anchor-second row happened after the crossing.

Examples:

```bash
# Default: 30 slots before the decisive-or-latest crossing through market end.
curl --compressed \
  "${API_BASE_URL}/markets/5944864/flips/data"

# Ten prior slots around crossing event 2, with smaller microstructure objects.
curl --get --compressed \
  "${API_BASE_URL}/markets/5944864/flips/data" \
  --data-urlencode "view=event_window" \
  --data-urlencode "before_seconds=10" \
  --data-urlencode "event_sequence=2" \
  --data-urlencode "microstructure_groups=flow,cross_market,quality"
```

## Complete Five-Minute View

Use `view=full` when the compact response raises a question that requires the
whole market:

```bash
curl --get --compressed \
  "${API_BASE_URL}/markets/5944864/flips/data" \
  --data-urlencode "view=full"
```

The full view returns the normal 300 one-second slots in
`[market_start_ms, market_end_ms)`. It includes all API-visible historical
layers for each slot:

- Binance Spot, standard Chainlink context, and settlement TWAP prices and
  freshness
- Polymarket Up/Down probability quotes
- Binance futures last, mark, index, and premium
- open interest
- futures flow and book summaries
- Binance spot/futures microstructure, cross-market values, observed forced
  orders, and quality fields

Unavailable source observations remain `null`; the API does not fabricate or
carry values forward for this research response.

“Full” means the complete five-minute public API representation. It does not
include the isolated raw-capture schema, individual raw trades, raw depth
messages, every non-crossing raw source event, secrets, or collector-internal
JSON. Flip crossings retain their exact TWAP event brackets.

## Response Contract

Both data views return the same top-level shape:

```json
{
  "schema_version": 2,
  "definition_version": 2,
  "market_data_schema_version": 4,
  "data_scope": "curated_public_api",
  "server_time_ms": 1783459600123,
  "market": {
    "market_id": 5944864,
    "market_start_ms": 1783459200000,
    "market_end_ms": 1783459500000,
    "settlement": {
      "reference": "chainlink_twap",
      "window_s": 30,
      "source_url": "https://data.chain.link/streams/btc-usd-twap-30s-streams",
      "rule_version": "btc-5m-twap-30",
      "price_to_beat": "63337.115841440165000000",
      "official_final_price": "63336.719008471390000000"
    }
  },
  "selection": {
    "view": "event_window",
    "anchor": {
      "selection_reason": "decisive_event",
      "sample_second_ms": 1783459498000,
      "event": {
        "event_sequence": 2,
        "direction": "up_to_down",
        "previous_side": "Up",
        "new_side": "Down",
        "previous_twap_price": "63337.250000000000000000",
        "new_twap_price": "63336.990000000000000000",
        "previous_sample_second_ms": 1783459497000,
        "new_sample_second_ms": 1783459498000,
        "previous_provider_event_ms": 1783459497300,
        "new_provider_event_ms": 1783459498300,
        "observed_ms_before_end": 1700,
        "is_decisive": true,
        "observation_precision": "exact_twap_event"
      }
    },
    "window": {
      "before_seconds": 30,
      "start_ms": 1783459468000,
      "end_ms_exclusive": 1783459500000,
      "row_count": 32,
      "rows_before_anchor": 30,
      "rows_at_or_after_anchor_second": 2,
      "clipped_at_market_start": false
    }
  },
  "availability": {
    "selected_window": {
      "series_rows": 32,
      "binance_price_rows": 32,
      "chainlink_price_rows": 31,
      "twap_price_rows": 32,
      "probability_rows": 30,
      "futures_rows": 32,
      "open_interest_rows": 32,
      "flow_rows": 31,
      "book_rows": 32,
      "microstructure_rows": 31,
      "microstructure_healthy_rows": 30
    },
    "full_market": {
      "series_rows": 300,
      "binance_price_rows": 299,
      "chainlink_price_rows": 297,
      "twap_price_rows": 298,
      "probability_rows": 295,
      "futures_rows": 299,
      "open_interest_rows": 298,
      "flow_rows": 297,
      "book_rows": 298,
      "microstructure_rows": 299,
      "microstructure_healthy_rows": 296
    }
  },
  "flip": {
    "evaluation": {},
    "events": [],
    "cutoffs": [],
    "archive": {},
    "cutoff_microstructure_rows_reused_from_series": 20
  },
  "series": [],
  "navigation": {
    "older_page_cursor": 5944864,
    "list_parameter": "before_market_id",
    "preserve_list_filters": true
  },
  "links": {}
}
```

The example counts are illustrative.

### Versions and selection

- `schema_version` versions this agent-bundle envelope.
- `definition_version` versions the saved flip definition.
- `market_data_schema_version` versions the nested market-series contract.
- `data_scope="curated_public_api"` states that the bundle covers every public
  API layer, not every stored database or raw-capture column.
- `selection.view` records the applied view.
- `selection.anchor` identifies the selected crossing and whether it was
  requested, decisive, or the latest fallback.
- `selection.window` records the actual half-open time range after clamping it
  to the market.

`selection.anchor` has exactly three fields:

- `selection_reason`
- `sample_second_ms`
- `event`

`selection_reason` is `requested_event_sequence`, `decisive_event`,
`latest_event_fallback`, or `market_end_fallback`. For an event anchor, `event`
is the normal serialized crossing and retains the full bracket, including its
previous/new prices and receive timestamps. For the market-end fallback,
`event` is `null`. `selection.window.before_seconds` is `null` for `view=full`.
`selection.window.rows_at_or_after_anchor_second` counts selected one-second
rows by their keys; it does not claim that an anchor-second row is purely
post-crossing.

Use the returned selection metadata rather than reconstructing the anchor from
rounded display times.

### Availability

`availability.selected_window` describes only the returned `series` slice.
`availability.full_market` describes the complete market even when the compact
view was requested.

Each object contains:

- `series_rows`
- `binance_price_rows`
- `chainlink_price_rows`
- `twap_price_rows`
- `probability_rows`
- `futures_rows`
- `open_interest_rows`
- `flow_rows`
- `book_rows`
- `microstructure_rows`
- `microstructure_healthy_rows`

For a normal completed full grid, `full_market.series_rows` is `300`.
`selected_window.series_rows` is the slice length. The remaining counters
measure source coverage and can be lower. A row counted for a source can still
contain nullable fields when only part of that source observation was
available.

`chainlink_price_rows` measures standard Chainlink spot context.
`twap_price_rows` measures the independently collected 30-second settlement
reference. Never substitute one for the other. The top-level
`market.settlement` object records the exact rule identity for the evidence.

### Flip evidence and duplicate microstructure

`flip.evaluation`, `flip.events`, and `flip.archive` retain the existing flip
research semantics. `flip.cutoffs` retains the twenty causal T-20 through T-1
records. Changing `view` never filters either array; it slices only `series`.

A cutoff normally contains a typed microstructure snapshot. To avoid sending
the same large object twice, the bundle omits a cutoff's inline
`microstructure` only when the returned `series` contains a non-null canonical
microstructure object at that same `microstructure_sample_second_ms`. The
cutoff keeps its sample timestamp and other evidence, and sets
`microstructure_reused_from_series: true`. If that sample second is outside the
selected series or its series microstructure is `null`, the inline cutoff
microstructure remains and `microstructure_reused_from_series` is `false`.

`flip.cutoff_microstructure_rows_reused_from_series` reports how many cutoff
microstructure objects were deduplicated in this way.

`microstructure_groups` applies both to `series[].microstructure` and to any
inline cutoff microstructure that remains.

### Navigation and links

`navigation.older_page_cursor` is an exclusive cursor seed for the next
`/markets/flips` list request. It does not promise that another match exists;
an empty next list page is the normal end condition. Keep the original list
filters unchanged.
Server-provided `links` are relative API paths for the related list, data,
download, compact, or full resources. Resolve them against the same private API
base URL. The link names are `flip_list`, `flip_detail`, `evidence`,
`evidence_download`, and `full_market_data`. The two evidence links preserve the
applied view, event sequence, lookback, and non-default microstructure groups.

`previous_5m_oi_summary` is optional and is omitted when unavailable.

## Download

The download response has JSON content identical to the corresponding data
response:

```bash
curl -OJ --get \
  "${API_BASE_URL}/markets/5944864/flips/download" \
  --data-urlencode "view=event_window" \
  --data-urlencode "before_seconds=30"
```

It additionally sends `Content-Disposition: attachment` so browsers and
`curl -OJ` use this server-provided filename:

```text
btc_5m_flip_{market_id}_{view}.json
```

To download all 300 seconds:

```bash
curl -OJ --get \
  "${API_BASE_URL}/markets/5944864/flips/download" \
  --data-urlencode "view=full"
```

## Python Agent Example

This loop reads one compact market at a time and advances with the exclusive
cursor:

```python
from __future__ import annotations

from typing import Any, Iterator

import httpx


API_BASE_URL = "http://127.0.0.1:19000"


def get_json(
    client: httpx.Client,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = client.get(
        path,
        params=params,
    )
    response.raise_for_status()
    return response.json()


def iter_flip_markets() -> Iterator[dict[str, Any]]:
    list_filters: dict[str, Any] = {
        "within_seconds": 20,
        "kind": "any_crossing",
        "limit": 1,
    }
    cursor: int | None = None

    with httpx.Client(
        base_url=API_BASE_URL,
        headers={"Accept": "application/json"},
        timeout=30,
    ) as client:
        while True:
            list_params = dict(list_filters)
            if cursor is not None:
                list_params["before_market_id"] = cursor

            page = get_json(client, "/markets/flips", list_params)
            if not page["markets"]:
                return

            market_id = page["markets"][0]["market_id"]
            bundle = get_json(
                client,
                f"/markets/{market_id}/flips/data",
                {
                    "view": "event_window",
                    "before_seconds": 30,
                    "microstructure_groups": "flow,cross_market,quality",
                },
            )
            yield bundle

            cursor = bundle["navigation"]["older_page_cursor"]
```

If an agent needs more context after inspecting a compact bundle, request the
same market again with `view=full`; do not advance the cursor first.

## Interpretation Rules

- All fields ending in `_ms` are UTC epoch milliseconds.
- Financial values and rates are decimal strings. Do not convert them through a
  binary floating-point type.
- `null` is evidence of missing, stale, incomplete, or unavailable data. Do not
  replace it with zero or silently forward-fill it.
- Exact equality with the price-to-beat threshold is a touch, not an Up or Down
  side.
- Multiple crossings can occur. The decisive crossing is the last observed
  crossing into the official winning side.
- The official winner comes from Polymarket resolution data, never from the
  final probability quote.
- Only definition version `2` uses the exact 30-second TWAP event stream.
  Standard Chainlink spot is context and must not be used to reconstruct a
  missing TWAP crossing.
- `archive.status: "complete"` means every available source microstructure row
  was copied. It does not promise 300 collected rows.
- Treat long-lived cached evidence as stable only when
  `flip.archive.retention_safe` is `true`.

## Errors

- HTTP `200` with `markets: []` means the filtered list is exhausted.
- HTTP `404` means the requested market, flip evaluation, market data, or an
  explicitly requested `event_sequence` is unavailable. An evaluation with no
  events does not by itself produce a `404`; it uses the market-end fallback.
- HTTP `422` means a query parameter is invalid.
- HTTP `500`, `502`, or `503` indicates an API, database, tunnel, or proxy
  failure. Retry with bounded backoff and keep the current cursor until a
  request succeeds.

Do not reinterpret an empty list, a `404`, missing values, or an ambiguous
evaluation as a non-flip.
