# Flip Research Dashboard API

This is the focused dashboard integration guide for the permanent BTC
five-minute flip-research API. It covers the three flip endpoints and the
archive-aware historical data call used to load a selected market's complete
evidence.

The complete application contract remains in
[`FRONTEND_API.md`](FRONTEND_API.md). Microstructure field definitions are also
documented in [`MICROSTRUCTURE_API.md`](MICROSTRUCTURE_API.md).

## What the Dashboard Can Call

| Method | Path | Dashboard use |
| --- | --- | --- |
| `GET` | `/markets/flips` | Find matching markets and paginate newest to oldest |
| `GET` | `/markets/{market_id}/flips` | Load one market's evaluation, crossings, and T-20 through T-1 evidence |
| `GET` | `/markets/flips/distribution` | Build crossing-time and cutoff-reversal charts |
| `GET` | `/markets/{market_id}/data?include_microstructure=true` | Load the selected market's 300-second series, using the permanent archive when necessary |

All routes are read-only and PostgreSQL-backed. They are historical research
routes, not low-latency Redis routes.

## Access and Base URL

Production binds the API only to `127.0.0.1:9000` on the droplet. Do not expose
port `9000` publicly.

A browser dashboard should call a same-origin backend or reverse proxy:

```text
browser -> dashboard.example/api -> private/SSH hop -> droplet 127.0.0.1:9000
```

The API has no CORS middleware and no application-level authentication. The
proxy is therefore both the browser integration point and the security
boundary. If the dashboard backend is not on the droplet, connect it through a
private network or managed SSH tunnel. A browser cannot create the SSH tunnel
itself. When the dashboard proxy runs on the droplet, the private/SSH hop in
the diagram collapses to a loopback request.

For development, open a local tunnel:

```bash
ssh -N -L "${LOCAL_API_PORT}:127.0.0.1:9000" \
  "${DROPLET_USER}@${DROPLET_IP}"
```

Then use:

```bash
API_BASE_URL="http://127.0.0.1:${LOCAL_API_PORT}"
```

With a same-origin dashboard proxy, a reusable browser helper can be:

```javascript
const API_BASE_URL = "/api";

async function apiGet(path, query = {}) {
  const url = new URL(`${API_BASE_URL}${path}`, window.location.origin);

  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  const contentType = response.headers.get("content-type") ?? "";
  const body = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const detail = typeof body === "object" && body !== null
      ? body.detail ?? body.error
      : body;
    throw new Error(detail || `HTTP ${response.status}`);
  }

  return body;
}
```

## Definitions the UI Must Keep Separate

The threshold is Polymarket's official Chainlink `priceToBeat`. The result is
the official resolved winner; it is never inferred from the final probability
quote.

| API term | Meaning |
| --- | --- |
| `any_crossing` | Any observed strict move from below the threshold to above it, or above to below, during the final window |
| `decisive_flip` | The last observed crossing into the official winning side |
| `cutoff_reversal` | At exactly T-X, the apparent strict side differs from the official winner |

An exact equality with the threshold is a touch/tie, not Up or Down. Multiple
crossings are retained. Missing or stale Chainlink threshold evidence produces
an `ambiguous` evaluation instead of silently producing `non_flip`. Missing or
stale probability or microstructure evidence is retained through coverage and
quality fields but does not by itself change the evaluation status.

The dashboard must not label every `/markets/flips` result as a confirmed flip.
For example, an observed crossing across a stale observation gap remains
queryable evidence but has `evaluation_status: "ambiguous"`.

Evaluation states are:

- `confirmed_flip`
- `non_flip`
- `ambiguous`

Archive states are:

- `not_required`
- `pending`
- `complete`
- `failed`

`archive.status: "complete"` means every available source row was copied. It
does not promise exactly 300 rows. Missing collection seconds remain missing.
`archive.archived_at_ms` is nullable while archival is incomplete and remains
null when `archive.status` is `not_required`.

## Recommended Dashboard Flow

1. Load `/markets/flips` for the searchable results table.
2. When the user selects a market, load its flip detail and complete market data
   in parallel.
3. Load `/markets/flips/distribution` for aggregate charts.
4. Use the returned exclusive cursor for older result pages.

```javascript
async function loadFlipPage(filters = {}) {
  return apiGet("/markets/flips", {
    within_seconds: filters.withinSeconds ?? 20,
    kind: filters.kind ?? "any_crossing",
    direction: filters.direction,
    winner: filters.winner,
    start_ms: filters.startMs,
    end_ms: filters.endMs,
    before_market_id: filters.beforeMarketId,
    limit: filters.limit ?? 20,
  });
}

async function loadFlipMarket(marketId) {
  return Promise.all([
    apiGet(`/markets/${marketId}/flips`),
    apiGet(`/markets/${marketId}/data`, {
      include_probabilities: true,
      include_futures: true,
      include_oi: true,
      include_flow: true,
      include_book: true,
      include_microstructure: true,
    }),
  ]).then(([flip, data]) => ({ flip, data }));
}

async function loadFlipDistribution(filters = {}) {
  return apiGet("/markets/flips/distribution", {
    max_seconds: filters.maxSeconds ?? 20,
    direction: filters.direction,
    start_ms: filters.startMs,
    end_ms: filters.endMs,
  });
}
```

Leave `fill_display` at its default `false` on the research data call. The
dashboard should display missing evidence as missing rather than carry values
forward.

## Find Matching Markets

### Request

```http
GET /markets/flips
```

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `within_seconds` | integer `1`-`20` | `20` | Final-window bound for crossing queries, or the exact cutoff for `cutoff_reversal` |
| `kind` | enum | `any_crossing` | `any_crossing`, `decisive_flip`, or `cutoff_reversal` |
| `direction` | enum or omitted | omitted | `up_to_down` or `down_to_up` |
| `winner` | enum or omitted | omitted | Official `Up` or `Down`; capitalization is significant |
| `limit` | integer `1`-`50` | `20` | Markets per page |
| `before_market_id` | non-negative integer or omitted | omitted | Exclusive cursor for the next older page |
| `start_ms` | non-negative integer or omitted | omitted | Inclusive market-start timestamp |
| `end_ms` | non-negative integer or omitted | omitted | Exclusive market-start timestamp |

`start_ms` and `end_ms` filter the market start, not the crossing timestamp.
When both are present, `start_ms` must be less than `end_ms`.
The response's `filters` object echoes active search filters but intentionally
does not echo `limit`, `before_market_id`, or omitted filters.

For crossing kinds, `within_seconds=5` means an event was observed in the final
five seconds. For `cutoff_reversal`, it means the single T-5 observation.

```bash
curl --get "${API_BASE_URL}/markets/flips" \
  --data-urlencode "within_seconds=5" \
  --data-urlencode "kind=any_crossing" \
  --data-urlencode "limit=1"
```

### Response

Markets are ordered newest first and appear only once even when they crossed
multiple times.

```json
{
  "schema_version": 1,
  "definition_version": 1,
  "server_time_ms": 1783459600123,
  "filters": {
    "within_seconds": 5,
    "kind": "any_crossing"
  },
  "markets": [
    {
      "market_id": 5944864,
      "market_start_ms": 1783459200000,
      "market_end_ms": 1783459500000,
      "evaluation_status": "confirmed_flip",
      "price_to_beat": "63337.115841440165000000",
      "official_close": "63336.719008471390000000",
      "winner": "Down",
      "matching_crossing_count": 2,
      "total_crossing_count_last_20s": 3,
      "first_crossing_ms_before_end": 4800,
      "last_crossing_ms_before_end": 1700,
      "decisive_flip": {
        "direction": "up_to_down",
        "observed_ms_before_end": 1700
      },
      "archive": {
        "status": "complete",
        "source_microstructure_rows": 299,
        "archived_microstructure_rows": 299,
        "retention_safe": true,
        "archived_at_ms": 1783459525000
      },
      "flip_detail_url": "/markets/5944864/flips",
      "data_url": "/markets/5944864/data"
    }
  ],
  "next_before_market_id": 5944864
}
```

Field details:

- `matching_crossing_count` respects the request's kind, window, and direction.
  For a returned `cutoff_reversal` row, it is always `1` despite the historical
  crossing-oriented field name.
- `total_crossing_count_last_20s`, first/last crossing fields, and
  `decisive_flip` summarize the complete 20-second evaluation, not only the
  active filter.
- `price_to_beat`, `official_close`, and every other financial decimal are JSON
  strings.
- Use `flip_detail_url` and `data_url` as relative paths under the same API
  base.

An empty search returns HTTP `200`, `markets: []`, and
`next_before_market_id: null`.

### Pagination

The cursor is exclusive. Pass it back unchanged:

```javascript
const firstPage = await loadFlipPage({ limit: 20 });

const secondPage = firstPage.next_before_market_id === null
  ? null
  : await loadFlipPage({
      limit: 20,
      beforeMarketId: firstPage.next_before_market_id,
    });
```

When filters change, discard the cursor and start from the first page.
When requesting an older page, keep every other filter identical.

## Load One Market's Flip Evidence

### Request

```http
GET /markets/{market_id}/flips
```

```bash
curl --compressed "${API_BASE_URL}/markets/5944864/flips"
```

The response contains:

- `market`: threshold, official close, winner, and window timestamps.
- `evaluation`: classification, crossing summary, data quality, and coverage.
- `events`: every crossing ordered by `event_sequence`.
- `cutoffs`: exactly T-20 through T-1, ordered in that direction.
- `archive`: archive state and source/copied row counts.
- `data_url`: relative path for the full 300-second market series.

Neither `events` nor `cutoffs` is paginated.

Important response shape:

```json
{
  "schema_version": 1,
  "definition_version": 1,
  "server_time_ms": 1783459600123,
  "market": {
    "market_id": 5944864,
    "market_start_ms": 1783459200000,
    "market_end_ms": 1783459500000,
    "price_to_beat": "63337.115841440165000000",
    "official_close": "63336.719008471390000000",
    "winner": "Down"
  },
  "evaluation": {
    "status": "confirmed_flip",
    "observation_precision": "one_second_summary",
    "analysis_start_ms": 1783459480000,
    "analysis_end_ms": 1783459500000,
    "crossing_count": 3,
    "touch_count": 0,
    "first_crossing_ms_before_end": 4800,
    "last_crossing_ms_before_end": 1700,
    "decisive_flip": {
      "direction": "up_to_down",
      "observed_ms_before_end": 1700
    },
    "chainlink": {
      "observation_count": 20,
      "strict_observation_count": 20,
      "first_provider_event_ms": 1783459480000,
      "last_provider_event_ms": 1783459498300,
      "max_gap_ms": 1000
    },
    "cutoff_coverage": {
      "chainlink_count": 20,
      "fresh_chainlink_count": 20,
      "probability_count": 20,
      "fresh_probability_count": 20,
      "microstructure_count": 19
    },
    "quality_flags": ["missing_microstructure_cutoffs"],
    "evaluated_at_ms": 1783459524000
  },
  "events": [
    {
      "event_sequence": 1,
      "direction": "up_to_down",
      "previous_side": "Up",
      "new_side": "Down",
      "previous_chainlink_price": "63337.250000000000000000",
      "new_chainlink_price": "63336.990000000000000000",
      "previous_sample_second_ms": 1783459497000,
      "new_sample_second_ms": 1783459498000,
      "previous_provider_event_ms": 1783459497300,
      "new_provider_event_ms": 1783459498300,
      "previous_received_ms": 1783459497420,
      "new_received_ms": 1783459498420,
      "observation_gap_ms": 1000,
      "observed_ms_before_end": 1700,
      "is_decisive": true,
      "observation_precision": "one_second_summary"
    }
  ],
  "cutoffs": [
    {
      "seconds_before_end": 20,
      "cutoff_ms": 1783459480000,
      "chainlink": {
        "price": "63338.010000000000000000",
        "sample_second_ms": 1783459479000,
        "provider_event_ms": 1783459479000,
        "received_ms": 1783459479100,
        "age_ms": 1000,
        "received_age_ms": 900,
        "fresh": true
      },
      "signed_distance": "0.894158559835000000",
      "absolute_distance": "0.894158559835000000",
      "apparent_side": "Up",
      "probabilities": {
        "up": {
          "bid": "0.60000000",
          "ask": "0.61000000",
          "mid": "0.60500000",
          "normalized": "0.61000000",
          "provider_event_ms": 1783459479500,
          "received_ms": 1783459479600,
          "source_age_ms": 500,
          "received_age_ms": 400
        },
        "down": {
          "bid": "0.38000000",
          "ask": "0.39000000",
          "mid": "0.38500000",
          "normalized": "0.39000000",
          "provider_event_ms": 1783459479400,
          "received_ms": 1783459479550,
          "source_age_ms": 600,
          "received_age_ms": 450
        },
        "sample_second_ms": 1783459479000,
        "provider_event_ms": 1783459479500,
        "received_ms": 1783459479600,
        "age_ms": 600,
        "received_age_ms": 450,
        "fresh": true
      },
      "official_winner": "Down",
      "flipped_after_cutoff": true,
      "microstructure_sample_second_ms": 1783459479000,
      "microstructure_available": true,
      "microstructure": {
        "collector_healthy": true,
        "books": {},
        "flow": {},
        "cross_market": {},
        "liquidations": {},
        "quality": {}
      },
      "quality_flags": []
    }
  ],
  "archive": {
    "status": "complete",
    "source_microstructure_rows": 299,
    "archived_microstructure_rows": 299,
    "retention_safe": true,
    "archived_at_ms": 1783459525000
  },
  "data_url": "/markets/5944864/data"
}
```

Each cutoff is causal: it uses only evidence that was available at that cutoff.
Render `chainlink.fresh`, `probabilities.fresh`, `microstructure_available`, and
`quality_flags`; do not hide them.

`events[].observed_ms_before_end` is the new opposite-side observation's
provider timestamp, not an interpolated instant at which the price crossed the
threshold. For a timeline, treat `previous_provider_event_ms` through
`new_provider_event_ms` as the observed crossing bracket and expose
`observation_gap_ms`.

The Up and Down probability timestamps and ages are per outcome. A fresh Up
quote does not make an old Down quote fresh. Historical samples created before
per-outcome timestamps were added remain present but can report
`fresh: false`.
Each outcome's timestamps describe the oldest non-null bid/ask component used
for that outcome. The enclosing probability timestamps are the row's causal
availability bound, while its enclosing age fields report the worst component
age.

This endpoint returns HTTP `404` when the current definition has no completed
evaluation for that market. That can be temporary while official resolution or
an evaluator retry is pending.

## Load Distribution Charts

### Request

```http
GET /markets/flips/distribution
```

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `max_seconds` | integer `1`-`20` | `20` | Return one row per second from T-X through T-1 |
| `direction` | enum or omitted | omitted | `up_to_down` or `down_to_up` |
| `start_ms` | non-negative integer or omitted | omitted | Inclusive market-start timestamp |
| `end_ms` | non-negative integer or omitted | omitted | Exclusive market-start timestamp |

```bash
curl --get "${API_BASE_URL}/markets/flips/distribution" \
  --data-urlencode "max_seconds=20"
```

### Response

```json
{
  "schema_version": 1,
  "definition_version": 1,
  "server_time_ms": 1783459600123,
  "max_seconds": 20,
  "population": {
    "resolved_markets": 2050,
    "eligible_markets": 2012,
    "ambiguous_markets": 38,
    "markets_with_any_crossing": 214
  },
  "crossings_by_time": [
    {
      "from_ms_before_end": 5000,
      "to_ms_before_end": 4000,
      "crossing_event_count": 16,
      "unique_market_count": 15,
      "decisive_flip_market_count": 12,
      "to_up_count": 7,
      "to_down_count": 9,
      "cumulative_unique_markets_within_window": 33,
      "cumulative_market_rate": "0.01640159"
    }
  ],
  "cutoff_reversals": [
    {
      "seconds_before_end": 5,
      "eligible_markets": 2008,
      "markets_reversed_by_close": 151,
      "reversal_rate": "0.07519920"
    }
  ]
}
```

Chart semantics:

- A `5000` to `4000` crossing bucket is `(4000, 5000]` milliseconds before
  expiry.
- `crossing_event_count` can exceed `unique_market_count` because a market can
  cross more than once.
- Use `unique_market_count` or `cumulative_market_rate` for market incidence;
  do not use event count as the market denominator.
- Ambiguous markets are reported in `population.ambiguous_markets` and excluded
  from the top-level eligible population and crossing-time calculations.
- `population.markets_with_any_crossing` respects `max_seconds` and
  `direction`. The resolved, eligible, and ambiguous population counts remain
  direction-independent.
- Each cutoff-reversal denominator is evidence-based independently. It can
  include an otherwise ambiguous market when that exact cutoff has fresh,
  strict Chainlink evidence and an official winner.
- `cutoff_reversals[].eligible_markets` is calculated independently at each
  cutoff and excludes missing, tied, future-timestamped, or stale Chainlink
  evidence.
- With a direction filter, cutoff denominators are also restricted to the
  relevant apparent starting side. The top-level eligible population remains
  direction-independent.
- Both arrays contain zero-count rows, so the dashboard can render a stable
  T-X through T-1 axis.
- Rates are decimal strings with eight fractional digits, or `null` when the
  denominator is zero.

The API orders rows from T-`max_seconds` through T-1, so the returned order
already progresses toward expiry. Reverse the array only when the chart's
numeric “seconds before end” axis should run from `1` up to `max_seconds`.

## Load the Complete 300-Second Evidence

The flip-detail endpoint contains only the final twenty cutoff records. For the
complete five-minute series, call the existing data endpoint:

```bash
curl --compressed \
  "${API_BASE_URL}/markets/5944864/data?include_probabilities=true&include_futures=true&include_oi=true&include_flow=true&include_book=true&include_microstructure=true"
```

The bare `data_url` returned by the flip endpoints does not include query
parameters. Append `include_microstructure=true`; otherwise the response omits
microstructure.

When `include_microstructure=true`:

- `series` still contains the normal 300 one-second slots.
- Each slot has `microstructure`, either an object or `null`.
- The current `binance_microstructure_1s` row is preferred.
- If the current row has expired, a permanent flip-archive row is used.
- Missing source seconds remain `null`; no evidence is fabricated.
- `availability.microstructure_rows`,
  `availability.microstructure_healthy_rows`, and
  `availability.microstructure_missing_seconds` describe coverage.
- The response uses `schema_version: 3`.

Full five-minute microstructure is archived permanently only for
`confirmed_flip` and `ambiguous` evaluations. A `non_flip` evaluation uses
`archive.status: "not_required"`; its ordinary microstructure can disappear
after normal retention.

To reduce response size, select a subset:

```http
GET /markets/{market_id}/data
  ?include_microstructure=true
  &microstructure_groups=books,flow,quality
```

Valid groups are:

- `books`
- `flow`
- `cross_market`
- `liquidations`
- `quality`

Group names are case-sensitive. Sending `microstructure_groups` without
`include_microstructure=true`, an unknown group, or an empty group returns HTTP
`422`.

The response does not identify whether an individual second came from the live
table or permanent archive; both use the same schema.

The `/markets/{market_id}/download` endpoint does not include microstructure.
Use `/data?include_microstructure=true` for archived evidence.

## Errors and Empty States

| Status | Meaning | Dashboard behavior |
| --- | --- | --- |
| `200` | Successful response, including an empty list | Render the response normally |
| `404` | The requested market has no current-version flip evaluation, or no market data | Show “analysis not available yet”; retry later only for recently ended markets |
| `422` | Invalid enum/range, malformed parameter, or `start_ms >= end_ms` | Treat as a dashboard request bug and show the returned `detail` |
| `500` | Unhandled API or PostgreSQL failure | Preserve the current UI state and retry with bounded backoff |
| `502`/`503` | Dashboard proxy, connectivity, or health-check failure | Preserve the current UI state and retry with bounded backoff |

Do not turn an empty list, a `404`, a null cutoff value, or an ambiguous market
into a non-flip classification.

## Data and Cache Conventions

- Fields ending in `_ms` are UTC epoch milliseconds.
- Financial values and rates are decimal strings. Keep the original strings;
  convert only presentation copies for chart libraries.
- `schema_version` versions the JSON shape.
- `definition_version` versions the flip definition. Include it in cached
  analysis/distribution keys and invalidate old cached interpretations if it
  changes.
- Requests always use the server's current definition version. There is no
  `definition_version` query parameter for selecting older definitions.
- A completed evaluation is permanent, but `archive.status` can progress from
  `pending` or `failed` to `complete`. Cache long-term only after
  `archive.retention_safe` is true.
- These routes do not need sub-second polling. Refresh the list after market
  resolutions or on a moderate timer. Distribution results can be cached for
  several minutes.
- Browsers negotiate gzip automatically. Use `curl --compressed` for the larger
  detail and data responses.

## Suggested UI Mapping

| UI component | API fields |
| --- | --- |
| Results table | market end, winner, `evaluation_status`, matching/total crossings, last crossing time, archive coverage |
| Crossing timeline | `events[].observed_ms_before_end`, direction, prices, decisive marker |
| T-20 through T-1 table | cutoff apparent side, distance, probability, freshness, winner, `flipped_after_cutoff` |
| Crossing histogram | `crossings_by_time[].unique_market_count` and `crossing_event_count` |
| Cumulative incidence line | `crossings_by_time[].cumulative_market_rate` |
| Cutoff reversal line | `cutoff_reversals[].reversal_rate` |
| Full evidence chart | `/markets/{market_id}/data` `series[]`, including archived microstructure |

Always display the evaluation status and quality indicators near any flip label
or chart derived from the market.
