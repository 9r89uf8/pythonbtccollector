# Dashboard Instructions After Redis Live Cache Change

Use this for the separate dashboard repository.

The collector backend now separates fast live display values from historical research data:

```text
/markets/current/live = fastest live cards, backed by Redis through FastAPI
/markets/current/data = 300-row PostgreSQL research grid
```

The dashboard must keep that split.

## Scope

- Keep the dashboard local-only.
- Keep using the Vite proxy: `/price-api/* -> http://127.0.0.1:9000/*`.
- Do not call the droplet IP directly from browser code.
- Do not connect to Redis from the dashboard.
- Do not connect to PostgreSQL from the dashboard.
- Do not connect to Binance or Chainlink directly from the dashboard.
- Keep JavaScript only. Do not add TypeScript.

## Live Cards

Use this endpoint for live cards:

```text
GET /price-api/markets/current/live
```

This endpoint should drive the fastest displayed current values for:

- Binance Spot
- Polymarket Chainlink RTDS
- Binance USD-M Futures last price

Expected response shape:

```js
{
  server_time_ms: 1783459250123,
  prices: {
    binance_spot: {
      value: "62067.89",
      source_timestamp_ms: 1783459250000,
      received_ms: 1783459250050,
      source_age_ms: 123,
      received_age_ms: 73
    },
    chainlink: {
      value: "62066.12",
      source_timestamp_ms: 1783459249900,
      received_ms: 1783459250075,
      source_age_ms: 223,
      received_age_ms: 48
    }
  },
  futures: {
    last: {
      value: "62070.11",
      source_timestamp_ms: 1783459249950,
      received_ms: 1783459250090,
      source_age_ms: 173,
      received_age_ms: 33
    }
  }
}
```

Compatibility fields may also exist, such as `provider_event_ms` or `time_ms`. New dashboard code should prefer `source_timestamp_ms`.

Use the API-provided freshness fields directly:

- `source_age_ms`: age of the source event timestamp
- `received_age_ms`: age of the collector/API receive timestamp

Do not calculate live-card freshness from `/markets/current/data` rows. Grid row timestamps are market seconds, not source freshness timestamps.

## Live Polling

Poll `/price-api/markets/current/live` every 1 second for cards.

Use `setTimeout`-based polling, not bare `setInterval`, so requests cannot overlap.

Abort stale requests on refresh or unmount.

Pause polling when the page is hidden.

Handle these states without crashing:

- SSH tunnel down
- API down
- Redis/live cache not populated yet
- malformed live response
- stale source
- missing source

## Research Grid And Charts

Keep using `/data` endpoints for charts, 5-minute progress, research grids, and downloads:

```text
GET /price-api/markets/current/data
GET /price-api/markets/{market_id}/data
GET /price-api/markets/current/download
GET /price-api/markets/{market_id}/download
```

Download responses are intentionally cleaner than `/data` responses:

- Per-row `freshness` is omitted.
- When futures are requested, `row.prices.futures` contains the futures last price.
- The separate `row.futures` object is omitted from downloads.
- Futures mark, index, and premium are dashboard/data-grid fields, not download fields.

For dashboard chart display with futures/open interest, request:

```text
GET /price-api/markets/current/data?include_futures=true&include_oi=true&fill_display=true&max_carry_forward_ms=10000
```

Rules:

- Use `/markets/current/data` for the 300-second market chart.
- Use `/markets/current/live` for live card values and live freshness.
- Do not use `/markets/current/live` for historical chart data.
- Do not use `/markets/current/data` for fastest live cards.
- Leave chart gaps where values are `null`.
- Convert decimal strings to `Number` only for chart points and display math.
- Keep API decimal strings as strings at the data boundary.

Chart mappings:

| Chart line | Data-grid field |
| --- | --- |
| Binance Spot | `row.prices.binance` |
| Chainlink RTDS | `row.prices.chainlink` |
| Binance USD-M Futures | `row.futures.last` |
| Open Interest | `row.open_interest.contracts` |

Probability data remains opt-in:

```text
GET /price-api/markets/current/data?include_probabilities=true
```

Use:

```text
row.probabilities.up.ask
row.probabilities.down.ask
```

## Source Cards

Card mapping:

| Card | Live payload field | Value | Freshness |
| --- | --- | --- | --- |
| Binance Spot | `prices.binance_spot` | `value` | `source_age_ms`, `received_age_ms` |
| Chainlink RTDS | `prices.chainlink` | `value` | `source_age_ms`, `received_age_ms` |
| Binance USD-M Futures | `futures.last` | `value` | `source_age_ms`, `received_age_ms` |

Freshness interpretation:

| Source age | Received age | Meaning |
| --- | --- | --- |
| Low | Low | Source and collector are fresh |
| High | Low | Upstream source is stale or slow, collector is alive |
| Low | High | Collector or Redis live update is stale |
| High | High | Source or collector problem |

## Implementation Checklist

- Add or update a live API client function for `/price-api/markets/current/live`.
- Add or update a live payload guard.
- Add or update a `useCurrentMarketLive` hook.
- Drive top live cards from `/live`, not `/data`.
- Keep charts and downloads on `/data` and `/download`.
- Update fixtures for the new live payload shape.
- Add tests for live URL construction, live guard validation, freshness display, and missing live source handling.

## Verification

Before handing off dashboard changes, run:

```bash
npm run lint
npm run test
npm run build
```

Also verify through the SSH tunnel:

```bash
curl http://127.0.0.1:9000/markets/current/live
curl "http://127.0.0.1:9000/markets/current/data?include_futures=true&include_oi=true&fill_display=true&max_carry_forward_ms=10000"
```
